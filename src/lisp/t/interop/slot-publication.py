"""Check slot publication failures in an owned SQL server and control REPL."""

import argparse
import json
import os
from pathlib import Path
import re
import socket
import stat
import time

from parking_wire import RawConnection, WireError


def main(port, control_path, prefix, after_restart):
    parent = Path(control_path).parent.stat()
    assert parent.st_uid == os.getuid() and stat.S_IMODE(parent.st_mode) == 0o700
    assert stat.S_ISSOCK(Path(control_path).stat().st_mode)

    def control(code):
        with socket.socket(socket.AF_UNIX) as client:
            client.settimeout(10)
            client.connect(control_path)
            client.sendall((code + "\n(quit)\n").encode())
            chunks = []
            while chunk := client.recv(65536):
                chunks.append(chunk)
        reply = b"".join(chunks).decode()
        assert ";; error:" not in reply and ";; read error:" not in reply, reply
        return reply

    def query(sql):
        connection = RawConnection(port, "slot-publication-test")
        try:
            return connection.query(sql)
        finally:
            connection.close()

    hooks = Path(__file__).resolve().with_name("slot-publication-hooks.lisp")
    assert '"' not in str(hooks) and "\\" not in str(hooks)
    control(f'(load "{hooks}")')
    start = time.monotonic()
    manifest_path = Path(control_path).parent / "slot-publication-state.json"

    def row(name):
        return query("SELECT slot_name,restart_lsn,confirmed_flush_lsn "
                     f"FROM pg_replication_slots WHERE slot_name='{name}'")

    def state_hex(name):
        reply = control(f'(slot-publication-test-state-hex "{name}")')
        match = re.search(r'\n"([0-9A-F]+)"\n', reply)
        assert match, reply
        return match[1]

    if after_restart:
        manifest = json.loads(manifest_path.read_text())
        assert manifest["prefix"] == prefix, manifest
        for entry in manifest["slots"]:
            name = entry["name"]
            assert name in (prefix + "_after_root", prefix + "_after_child"), name
            assert row(name) == entry["row"], (name, row(name), entry["row"])
            assert state_hex(name) == entry["state"], name
            assert ":VERIFIED" in control(f'(slot-publication-test-check-restart-fence "{name}" nil)')
            query(f"SELECT pg_drop_replication_slot('{name}')")
            assert row(name) == []
        print(f"slot-publication: PASS (cold restore resolves failed creation; exact state retained) {time.monotonic() - start:.6f}s")
        return

    assert not manifest_path.exists(), manifest_path
    manifest = {"prefix": prefix, "slots": []}
    for kind in ("before-root", "after-root", "after-child"):
        name = f"{prefix}_{kind.replace('-', '_')}"
        assert row(name) == []
        try:
            control(f'(slot-publication-test-setup "{name}" :{kind})')
            try:
                query(f"SELECT pg_create_logical_replication_slot('{name}','pgoutput')")
            except WireError as condition:
                assert condition.sqlstate == "XX000", condition
                assert "injected slot publication namespace fault" in str(condition), condition
            else:
                raise AssertionError((kind, "expected publication failure"))
            assert ":VERIFIED" in control("(slot-publication-test-verify)")
        finally:
            control("(slot-publication-test-restore)")
        if kind == "before-root":
            assert row(name) == []
        else:
            before = state_hex(name)
            try:
                query(f"SELECT pg_drop_replication_slot('{name}')")
            except WireError as condition:
                assert condition.sqlstate == "55000" and "restart required" in str(condition), condition
            else:
                raise AssertionError((kind, "failed creation allowed a drop"))
            assert ":VERIFIED" in control(f'(slot-publication-test-check-restart-fence "{name}" t)')
            assert state_hex(name) == before
            manifest["slots"].append({"name": name, "row": row(name), "state": before})
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"slot-publication: PASS (real logical activation, namespace loss, retained pin, failed-create fence); restart server and run --after-restart {time.monotonic() - start:.6f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("port", type=int)
    parser.add_argument("control_path")
    parser.add_argument("--prefix", default="slot_pub_contract")
    parser.add_argument("--after-restart", action="store_true")
    args = parser.parse_args()
    assert 1 <= len(args.prefix) <= 40
    assert all(c in "abcdefghijklmnopqrstuvwxyz0123456789_" for c in args.prefix)
    main(args.port, args.control_path, args.prefix, args.after_restart)

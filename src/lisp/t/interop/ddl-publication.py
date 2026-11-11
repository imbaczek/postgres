"""Exercise DDL publication through a task-owned SQL server and control socket."""

import concurrent.futures
from contextlib import ExitStack
import os
from pathlib import Path
import socket
import stat
import sys
import time

from parking_wire import RawConnection, WireError


def main(port, control_path):
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

    def query(sql, application="ddl-publication-test"):
        connection = RawConnection(port, application)
        try:
            return connection.query(sql)
        finally:
            connection.close()

    def wait_flag(flag):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if "\nT\n" in control(f"(not (null (getf *ddl-publication-test-state* :{flag})))"):
                return
            time.sleep(0.01)
        raise AssertionError(f"DDL hook did not reach {flag}")

    def rejected(sql, text):
        try:
            query(sql, "ddl-publication-replay")
        except WireError as condition:
            assert text in str(condition), (sql, condition)
        else:
            raise AssertionError((sql, "expected rejection", text))

    hooks = Path(__file__).resolve().with_name("ddl-publication-hooks.lisp")
    assert '"' not in str(hooks) and "\\" not in str(hooks)
    control(f'(load "{hooks}")')
    prefix = f"ddlpub_{os.getpid()}"
    a, b, c, d = [prefix + suffix for suffix in ("_a", "_b", "_c", "_d")]
    ia, ib, ic, id_ = [name + "_idx" for name in (a, b, c, d)]
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    installed = False
    try:
        for table in (a, b, c, d):
            query(f"CREATE TABLE {table} (id integer)")
            query(f"INSERT INTO {table} VALUES (1)")
        control(f'(ddl-publication-test-setup "{a}")')
        installed = True
        build = pool.submit(query, f"CREATE INDEX {ia} ON {a} (id)")
        wait_flag("paused")
        drop = pool.submit(query, f"DROP TABLE {a}", "ddl-publication-waiter")
        wait_flag("waiting")
        query(f"CREATE INDEX {ib} ON {b} (id)")
        assert not build.done() and not drop.done()
        control("(ddl-publication-test-release)")
        build.result(timeout=10)
        drop.result(timeout=10)
        assert ":CHAIN-VALID" in control(f'(ddl-publication-test-chain \'("{ib}" "{ia}" "{a}"))')
        control("(ddl-publication-test-restore)")
        installed = False

        control(f'(ddl-publication-test-setup "{c}" t)')
        installed = True
        build = pool.submit(rejected, f"CREATE INDEX {ic} ON {c} (id)", "subscriber schema differs")
        wait_flag("paused")
        query(f"CREATE INDEX {id_} ON {d} (id)")
        control("(ddl-publication-test-release)")
        build.result(timeout=10)
        absent = control(f'(with-write-lock (null (gethash "{ic}" (database-indexes (gethash "postgres" (catalog-databases *catalog*))))))')
        assert "\nT\n" in absent, absent
        assert ":CHAIN-VALID" in control(f'(ddl-publication-test-chain \'("{id_}"))')
        for sql in (
            f"CREATE TABLE IF NOT EXISTS {c} (id integer)",
            f"CREATE INDEX IF NOT EXISTS {id_} ON {d} (id)",
            f"ALTER TABLE IF EXISTS {prefix}_missing ADD CONSTRAINT unused UNIQUE (id)",
            f"ALTER TABLE {c} DROP CONSTRAINT IF EXISTS unused",
        ):
            rejected(sql, "subscriber schema differs")
        control("(ddl-publication-test-restore)")
        installed = False

        control(f'(ddl-publication-test-setup "{c}" t)')
        installed = True
        rejected("DROP TABLE IF EXISTS pg_lisp_subscription_progress", "cannot target subscriber metadata")
        print("ddl-publication: PASS (gate ordering, interleaved schema chain, late drift, no-op guards)")
    finally:
        with ExitStack() as cleanup:
            for table in (a, b, c, d):
                cleanup.callback(query, f"DROP TABLE IF EXISTS {table}")
            if installed:
                cleanup.callback(control, "(ddl-publication-test-restore)")
            cleanup.callback(pool.shutdown, wait=True, cancel_futures=True)
            if installed:
                cleanup.callback(control, "(ddl-publication-test-release)")


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2])

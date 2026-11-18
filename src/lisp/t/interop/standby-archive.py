"""Exercise native standby WAL archiving and archive-to-stream recovery."""

import argparse
from contextlib import closing
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import tempfile


spec = importlib.util.spec_from_file_location(
    "standby_archive_follow", Path(__file__).with_name("patroni-follow.py"))
follow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(follow)


def advance_wal(port, start, endpoint):
    with closing(follow.psycopg2.connect(host="127.0.0.1", port=port,
                                        user="postgres", dbname="postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            for row in range(start, start + 6000):
                cursor.execute("INSERT INTO archive_probe VALUES (%s)", (row,))
                if row % 100 == 0:
                    cursor.execute("SELECT pg_current_wal_flush_lsn() >= %s::pg_lsn", (endpoint,))
                    if cursor.fetchone() == (True,):
                        return row
    raise AssertionError(f"source did not reach {endpoint}")


def check_primary_archive_handoff(node, port, data, archive, attempts, command, last):
    wal = data / "pg_wal"
    archive_status = wal / "archive_status"
    history = "00000002.history"
    follow.wait_for((archive_status / (history + ".done")).is_file)
    assert (archive / history).read_bytes() == (wal / history).read_bytes()

    (data / "postgresql.auto.conf").write_text(
        "archive_mode = on\narchive_command = '" + command.replace("'", "''") + "'\n")
    previous_attempts = attempts.read_text().splitlines()
    assert node.restart(timeout=90) is True
    assert follow.query(port, "SHOW archive_mode") == [("on",)]
    assert follow.query(port, "SELECT pg_is_in_recovery()") == [(False,)]
    last = advance_wal(port, last + 1, "0/3000000")
    assert follow.query(port, "SELECT max(id) FROM archive_probe") == [(last,)]
    child = "000000020000000000000002.wal"
    proof_name = child + ".proof"
    follow.wait_for((archive_status / (proof_name + ".done")).is_file)
    proof = (archive / proof_name).read_bytes()
    assert len(proof) == 52 and proof[:4] == b"PWAP"
    end = 2 * 16 * 1024 * 1024 + (archive / child).stat().st_size
    assert int.from_bytes(proof[32:40], "little") == end
    new_attempts = attempts.read_text().splitlines()[len(previous_attempts):]
    assert child in new_attempts and proof_name in new_attempts
    assert not any(name.startswith("00000001") for name in new_attempts)
    return last


def check_second_promotion(upstream, upstream_port, root, archive, command, last):
    root.mkdir(mode=0o700)
    data = root / "data"
    conninfo = f"host=127.0.0.1 port={upstream_port} user=postgres dbname=postgres"
    follow.query(upstream_port, "SELECT pg_create_physical_replication_slot('follow_one',true)")
    subprocess.run([str(follow.BASE / "bin/pg_basebackup"), "-D", str(data),
                    "-X", "stream", "-d", conninfo], check=True, timeout=90)
    (data / "postgresql.auto.conf").write_text(
        "archive_mode = on\narchive_command = '" + command.replace("'", "''") + "'\n")
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    node = follow.Postgresql(follow.configuration(data, port, root), follow.Null())
    member = follow.Member(0, "promoted", None,
                           {"conn_url": f"postgres://127.0.0.1:{upstream_port}/postgres"})
    try:
        assert node.follow(member, timeout=90) is True
        follow.query(upstream_port, f"INSERT INTO archive_probe VALUES ({last + 1})")
        follow.wait_for(lambda: follow.query(port, "SELECT max(id) FROM archive_probe") == [(last + 1,)])
        assert upstream.stop(mode="fast", checkpoint=False, stop_timeout=45)
        assert follow.query(port, "SELECT pg_promote(true,30)") == [(True,)]
        follow.query(port, f"INSERT INTO archive_probe VALUES ({last + 2})")
        assert follow.query(port, "SELECT max(id) FROM archive_probe") == [(last + 2,)]

        status = data / "pg_wal/archive_status"
        history = "00000003.history"
        follow.wait_for((status / (history + ".done")).is_file)
        previous = (archive / "00000002.history").read_bytes()
        current = (archive / history).read_bytes()
        assert current.startswith(previous)
        timeline, location, _ = current[len(previous):].split(maxsplit=2)
        assert timeline == b"2"
        high, low = (int(part, 16) for part in location.split(b"/"))
        fork = (high << 32) + low
        segment, offset = divmod(fork, 16 * 1024 * 1024)
        assert offset > 0
        partial = f"00000002{segment // 256:08X}{segment % 256:08X}.wal.partial"
        proof_name = partial + ".proof"
        follow.wait_for((status / (proof_name + ".done")).is_file)
        assert (archive / partial).stat().st_size == offset
        proof = (archive / proof_name).read_bytes()
        assert len(proof) == 52 and proof[:4] == b"PWAP"
        assert int.from_bytes(proof[32:40], "little") == fork
        assert node.stop(mode="fast", checkpoint=False, stop_timeout=45)
    finally:
        node.connection_pool.close()
        if node.is_running():
            node.stop(mode="immediate", checkpoint=False, stop_timeout=30)


def exercise(root, source_port, target_port):
    source_data, target_data = root / "source", root / "replica"
    source_socket = root / "source.sock"
    archive = root / "archive"
    primary_archive = root / "primary-archive"
    allow = root / "allow-archive"
    allow_proofs = root / "allow-archive-proofs"
    attempts = root / "archive-attempts"
    archive.mkdir()
    primary_archive.mkdir()
    primary_command = f'cp -- "%p" {shlex.quote(str(primary_archive))}/"%f"'
    pg_wal = target_data / "pg_wal"
    queue = pg_wal / "standby_archive"
    node = None
    with (root / "source.log").open("w") as log:
        source = subprocess.Popen(
            [str(follow.BASE / "pg-lisp.sh"), "-D", str(source_data), "-p", str(source_port),
             "-c", "archive_mode=on", "-c", f"archive_command={primary_command}",
             "--control-socket", str(source_socket)], stdout=log, stderr=log)
        try:
            follow.wait_for(lambda: follow.query(source_port, "SELECT 1") == [(1,)])
            follow.query(source_port, "CREATE TABLE archive_probe(id integer PRIMARY KEY)")
            follow.query(source_port, "SELECT pg_create_physical_replication_slot('follow_one',true)")
            target_data.mkdir(mode=0o700)
            follow.control(source, source_socket,
                           f'(physical-base-backup {json.dumps(str(target_data) + "/")} "standby_archive")')
            (target_data / "postgresql.conf").write_text("# Native base managed by Patroni.\n")
            command = (
                f'echo "%f" >> {shlex.quote(str(attempts))}; '
                f'case "%f" in *.proof) test -f {shlex.quote(str(allow_proofs))} || exit 1;; esac; '
                f'test -f {shlex.quote(str(allow))} && cp -- "%p" {shlex.quote(str(archive))}/"%f"'
            )
            (target_data / "postgresql.auto.conf").write_text(
                "archive_mode = always\narchive_command = '" + command.replace("'", "''") + "'\n")
            node = follow.Postgresql(follow.configuration(target_data, target_port, root), follow.Null())
            member = follow.Member(0, "source", None,
                                   {"conn_url": f"postgres://127.0.0.1:{source_port}/postgres"})
            assert node.follow(member, timeout=90) is True
            assert follow.query(target_port, "SHOW archive_mode") == [("always",)]
            follow.query(target_port, "SELECT pg_wal_replay_pause()")
            last = advance_wal(source_port, 1, "0/1000000")
            first = "000000010000000000000000.wal"
            second = "000000010000000000000001.wal"
            follow.wait_for(lambda: (queue / first).is_file())
            follow.wait_for(lambda: attempts.is_file() and first in attempts.read_text().splitlines())
            assert follow.query(target_port, "SELECT count(*) FROM archive_probe") == [(0,)]
            assert not (archive / first).exists()
            assert not (queue / "archive_status" / (first + ".done")).exists()
            assert (queue / first).read_bytes() == (source_data / "pg_wal/0000000000000000.wal").read_bytes()
            print("standby-archive: staged durable WAL with replay paused and command failing", flush=True)

            queue.chmod(0o755)
            last = advance_wal(source_port, last + 1, "0/2000000")
            follow.query(target_port, "SELECT pg_wal_replay_resume()")
            follow.wait_for(lambda: follow.query(target_port, "SELECT max(id) FROM archive_probe") == [(last,)])
            assert not (queue / second).exists()
            receive_control = target_data / "pg_physical_receive/receive.control"
            floor = int.from_bytes(receive_control.read_bytes()[48:56], "little")
            assert node.stop(mode="fast", checkpoint=False, stop_timeout=45)
            assert node.controldata()["Database cluster state"] == "shut down in recovery"
            assert int.from_bytes(receive_control.read_bytes()[48:56], "little") == floor
            pointer = (target_data / "physical-restartpoint.control").read_bytes()
            assert int.from_bytes(pointer[8:16], "little") > floor
            expected = {
                name: hashlib.sha256(
                    (source_data / "pg_wal" / f"{index:016X}.wal").read_bytes()).digest()
                for index, name in enumerate((first, second))
            }
            for index, name in enumerate((first, second)):
                raw_name = f"{index:016X}.wal"
                receipt = source_data / "pg_wal/archive_status" / (raw_name + ".proof.done")
                follow.wait_for(receipt.is_file)
                raw = (primary_archive / raw_name).read_bytes()
                assert hashlib.sha256(raw).digest() == expected[name]
                proof = (primary_archive / (raw_name + ".proof")).read_bytes()
                assert len(proof) == 52 and proof[:4] == b"PWAP"
                assert int.from_bytes(proof[32:40], "little") == index * 16 * 1024 * 1024 + len(raw)
            source.send_signal(signal.SIGINT)
            assert source.wait(timeout=45) == 0
            queue.chmod(0o700)
            assert node.follow(member, timeout=90) is True
            follow.wait_for(lambda: (queue / second).is_file())
            assert not any(archive.iterdir())
            assert follow.query(target_port, "SELECT max(id) FROM archive_probe") == [(last,)]
            print("standby-archive: clean shutdown kept unstaged WAL and cold restart staged it offline", flush=True)
            assert follow.query(target_port, "SELECT pg_promote(true,30)") == [(True,)]
            assert follow.query(target_port, "SELECT pg_is_in_recovery()") == [(False,)]
            follow.query(target_port, f"INSERT INTO archive_probe VALUES ({last + 1})")
            assert follow.query(target_port, "SELECT max(id) FROM archive_probe") == [(last + 1,)]
            allow.touch()
            for name in (first, second):
                follow.wait_for(lambda: (queue / "archive_status" / (name + ".done")).is_file())
                with (archive / name).open("rb") as stream:
                    assert hashlib.file_digest(stream, "sha256").digest() == expected[name]
                assert not (archive / (name + ".proof")).exists()
            allow_proofs.touch()
            for index, name in enumerate((first, second)):
                proof_name = name + ".proof"
                follow.wait_for(lambda: (queue / "archive_status" / (proof_name + ".done")).is_file())
                proof = (archive / proof_name).read_bytes()
                assert len(proof) == 52 and proof[:4] == b"PWAP"
                end = index * 16 * 1024 * 1024 + (archive / name).stat().st_size
                assert int.from_bytes(proof[32:40], "little") == end
            print("standby-archive: promoted writer archived receive WAL and its end proofs", flush=True)
            last = check_primary_archive_handoff(node, target_port, target_data,
                                                 archive, attempts, command, last + 1)
            print("standby-archive: restarted in archive_mode=on and archived writer WAL", flush=True)
            check_second_promotion(node, target_port, root / "second-promotion", archive, command, last)
            print("standby-archive: second promotion archived timeline history and fork proof", flush=True)
        finally:
            try:
                if node is not None:
                    node.connection_pool.close()
                    if node.is_running():
                        node.stop(mode="immediate", checkpoint=False, stop_timeout=30)
            finally:
                if source.poll() is None:
                    source.kill()
                    source.wait(timeout=10)


def reload_archive_only(node, port, root, config):
    process = node.is_running()
    command = config["recovery_conf"]["restore_command"]
    node.reload_config({**config, "recovery_conf": {"restore_command": ""}})
    assert node.follow(None, do_reload=True) is True
    follow.wait_for(lambda: follow.query(port, "SHOW primary_conninfo") == [("",)]
                    and follow.query(port, "SHOW restore_command") == [("",)])
    follow.wait_for(lambda: ":REPLAY-ONLY" in follow.control(
        process, root / "target.sock",
        "(when (and *physical-follow-runtime* "
        "(eq *physical-follow-desired* *physical-follow-desired-applied*) "
        "(physical-follow-receiver-stopped-p *physical-follow-runtime*)) :replay-only)"))
    node.reload_config(config)
    assert node.follow(None, do_reload=True) is True
    follow.wait_for(lambda: follow.query(port, "SHOW restore_command") == [(command,)])
    assert follow.query(port, "SHOW primary_conninfo") == [("",)]
    assert node.is_running().pid == process.pid


def restore_then_stream(root, source_port, target_port, reload_restore):
    source_data, target_data = root / "source", root / "replica"
    source_socket = root / "source.sock"
    archive = root / "archive"
    attempts = root / "restore-attempts"
    block = root / "block-restore"
    shell_pid = root / "restore-shell-pid"
    archive.mkdir()
    archive_command = f'cp -- "%p" {shlex.quote(str(archive))}/"%f"'
    marker = shlex.quote(str(shell_pid))
    pending_marker = shlex.quote(str(shell_pid.with_suffix(".tmp")))
    restore_command = (
        f'if test -f {shlex.quote(str(block))}; then '
        f'printf partial > "%p" && printf "%%s\\n" "$$" > {pending_marker} && '
        f'mv -- {pending_marker} {marker} && exec sleep 300; exit 1; fi; '
        f'printf "%%s %%s\\n" "%f" "%r" >> {shlex.quote(str(attempts))}; '
        f'test -f {shlex.quote(str(archive))}/"%f" && cp -- {shlex.quote(str(archive))}/"%f" "%p"'
    )
    args = [str(follow.BASE / "pg-lisp.sh"), "-D", str(source_data), "-p", str(source_port),
            "-c", "archive_mode=on", "-c", f"archive_command={archive_command}",
            "--control-socket", str(source_socket)]
    node = None
    with (root / "source.log").open("w") as log:
        source = subprocess.Popen(args, stdout=log, stderr=log)
        try:
            follow.wait_for(lambda: follow.query(source_port, "SELECT 1") == [(1,)])
            follow.query(source_port, "CREATE TABLE archive_probe(id integer PRIMARY KEY)")
            follow.query(source_port, "SELECT pg_create_physical_replication_slot('follow_one',true)")
            target_data.mkdir(mode=0o700)
            follow.control(source, source_socket,
                           f'(physical-base-backup {json.dumps(str(target_data) + "/")} "archive_restore")')
            (target_data / "postgresql.conf").write_text("# Native base managed by Patroni.\n")
            config = follow.configuration(target_data, target_port, root)
            config["recovery_conf"] = {"restore_command": restore_command}
            node = follow.Postgresql(config, follow.Null())
            member = follow.Member(0, "source", None,
                                   {"conn_url": f"postgres://127.0.0.1:{source_port}/postgres"})
            assert node.follow(member, timeout=90) is True
            assert follow.query(target_port, "SHOW restore_command") == [(restore_command,)]
            receiver = "SELECT status,slot_name FROM pg_stat_get_wal_receiver()"
            follow.wait_for(lambda: follow.query(target_port, receiver) == [("streaming", "follow_one")])
            assert follow.query(target_port, "SELECT count(*) FROM archive_probe") == [(0,)]
            assert node.stop(mode="fast", checkpoint=False, stop_timeout=45)

            last = advance_wal(source_port, 1, "0/2000000")
            for segment in (0, 1):
                name = f"{segment:016X}.wal"
                receipt = source_data / "pg_wal/archive_status" / (name + ".proof.done")
                follow.wait_for(receipt.is_file)
                assert (archive / name).read_bytes() == (source_data / "pg_wal" / name).read_bytes()
            source.send_signal(signal.SIGINT)
            assert source.wait(timeout=45) == 0

            proof_name = "0000000000000000.wal.proof"
            proof = archive / proof_name
            withheld = root / proof_name
            proof.rename(withheld)
            attempts.unlink(missing_ok=True)
            block.touch()
            assert node.start(timeout=90) is True
            follow.wait_for(shell_pid.is_file)
            shell = int(shell_pid.read_text())
            child = node.is_running()
            assert child is not None
            try:
                child.kill()
                child.wait(timeout=15)
            finally:
                try:
                    os.kill(shell, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.waitpid(shell, 0)
            block.unlink()
            stages = list((target_data / "pg_wal").glob(".pg-restore.*"))
            assert len(stages) == 1 and stages[0].is_dir(), stages
            staged, = stages[0].iterdir()
            assert staged.read_bytes() == b"partial"
            (root / "target.sock").unlink(missing_ok=True)
            assert node.start(timeout=90) is True
            assert not stages[0].exists()
            print("archive-restore: cold restart removed the interrupted restore stage", flush=True)
            rows = "SELECT count(*), max(id) FROM archive_probe"
            first_attempt = f"{proof_name} 0000000000000000.wal"
            follow.wait_for(lambda: attempts.is_file() and first_attempt in attempts.read_text().splitlines())
            assert follow.query(target_port, rows) == [(0, None)]
            if reload_restore:
                reload_archive_only(node, target_port, root, config)
            withheld.rename(proof)
            follow.wait_for(lambda: follow.query(
                target_port, "SELECT pg_last_wal_replay_lsn() > '0/1000000'::pg_lsn") == [(True,)])
            follow.wait_for(lambda: 0 < follow.query(target_port, rows)[0][0] < last)
            archived_count, archived_max = follow.query(target_port, rows)[0]
            assert archived_count == archived_max
            assert source.poll() == 0
            print(f"archive-restore: replayed {archived_count} rows while source was down", flush=True)

            source_socket.unlink(missing_ok=True)
            source = subprocess.Popen(args, stdout=log, stderr=log)
            follow.wait_for(lambda: follow.query(source_port, "SELECT 1") == [(1,)])
            if reload_restore:
                assert node.follow(member, do_reload=True) is True
            follow.query(source_port, f"INSERT INTO archive_probe VALUES ({last + 1})")
            follow.wait_for(lambda: follow.query(target_port, rows) == [(last + 1, last + 1)])
            follow.wait_for(lambda: follow.query(target_port, receiver) == [("streaming", "follow_one")])
            assert not list((target_data / "pg_wal").glob(".pg-restore.*"))
            print("archive-restore: streaming resumed on follow_one and replayed the remaining rows", flush=True)
            assert node.stop(mode="fast", checkpoint=False, stop_timeout=45)
        finally:
            try:
                if node is not None:
                    node.connection_pool.close()
                    if node.is_running():
                        node.stop(mode="immediate", checkpoint=False, stop_timeout=30)
            finally:
                if source.poll() is None:
                    source.kill()
                    source.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    cases = parser.add_mutually_exclusive_group()
    cases.add_argument("--restore-then-stream", action="store_true")
    cases.add_argument("--reload-restore", action="store_true")
    options = parser.parse_args()
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    root = Path(tempfile.mkdtemp(prefix="pg-standby-archive."))
    helper = root / "libpq-helper"
    subprocess.run([str(follow.BASE / "t/build-libpq-helper.sh"), str(helper)], check=True, timeout=60)
    os.environ["PG_LIBPQ_HELPER"] = str(helper)
    with socket.socket() as first, socket.socket() as second:
        first.bind(("127.0.0.1", 0))
        second.bind(("127.0.0.1", 0))
        ports = first.getsockname()[1], second.getsockname()[1]
    print(f"standby-archive: artifacts={root} ports={ports}", flush=True)
    if options.restore_then_stream or options.reload_restore:
        restore_then_stream(root, *ports, reload_restore=options.reload_restore)
    else:
        exercise(root, *ports)


if __name__ == "__main__":
    main()

"""Compact TRUNCATE storage redo respects standby readers and survives restart."""
import importlib.util
from pathlib import Path
import subprocess
import time

spec = importlib.util.spec_from_file_location("wire", Path(__file__).with_name("command-complete.py"))
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def run():
    source, target, clone = wire.OwnedLisp(), wire.OwnedLisp(), wire.OwnedLisp()
    connections = []
    try:
        source.start()
        writer = wire.Connection(source.port)
        connections.append(writer)
        writer.query("CREATE TABLE truncate_replay(id int PRIMARY KEY)")
        writer.query("INSERT INTO truncate_replay VALUES " + ",".join(f"({i})" for i in range(1000)))
        target.data.mkdir(mode=0o700)
        source.control(f'(physical-base-backup "{target.data}/" "truncate_replay_base")')
        target.start()
        hooks = wire.LISP / "t/physical-replay-native.lisp"
        source.control(f'(load "{hooks}")')
        target.control(f'(load "{hooks}")')
        target.control('(physical-recovery-enable-streaming)')
        reader = wire.Connection(target.port)
        connections.append(reader)
        reader.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        assert reader.query("SELECT count(*) FROM truncate_replay") == [["1000"]]
        writer.query("TRUNCATE truncate_replay")
        writer.query("INSERT INTO truncate_replay VALUES(42)")
        frames = source.root / "reset-frames.bin"
        cursor = reader.query("SELECT pg_last_wal_replay_lsn()::text")[0][0]
        source.control(f'(physical-test-export-wal (parse-replication-lsn "{cursor}") "{frames}")')
        target.control(f'(physical-test-receive-file "{frames}")')
        assert ":DEFERRED" in target.control('(physical-recovery-replay)')
        assert reader.query("SELECT count(*) FROM truncate_replay") == [["1000"]]
        target.control('(assert (not *physical-recovery-failed-p*))')

        # Kill after destructive redo but before applied progress publication.
        # Restart must repeat the same reset and page images from durable spool.
        target.control('(physical-test-crash-after-redo)')
        reader.close()
        connections.remove(reader)
        try:
            target.control('(physical-recovery-replay)')
        except (AssertionError, subprocess.SubprocessError, OSError):
            pass
        deadline = time.monotonic() + 10
        while target.process.poll() is None:
            assert time.monotonic() < deadline, "standby did not reach redo crash hook"
            time.sleep(0.02)
        assert target.process.returncode == -9, target.process.returncode
        target.stop()
        target.start()
        witness = wire.Connection(target.port)
        connections.append(witness)
        assert witness.query("SELECT id FROM truncate_replay") == [["42"]]
        assert witness.query("SELECT id FROM truncate_replay WHERE id=42") == [["42"]]
        witness.query("CHECKPOINT")
        target.stop(immediate=True)
        witness.close()
        connections.remove(witness)
        target.start()
        witness = wire.Connection(target.port)
        connections.append(witness)
        assert witness.query("SELECT id FROM truncate_replay") == [["42"]]
        assert witness.query("SELECT pg_promote(true,10)") == [["t"]]
        assert witness.query("SELECT pg_is_in_recovery()") == [["f"]]
        witness.query("INSERT INTO truncate_replay VALUES(43)")
        target.stop(immediate=True)
        witness.close()
        connections.remove(witness)
        target.start()
        witness = wire.Connection(target.port)
        connections.append(witness)
        assert witness.query("SELECT id FROM truncate_replay ORDER BY id") == [["42"], ["43"]]
        clone.data.mkdir(mode=0o700)
        target.control(f'(physical-base-backup "{clone.data}/" "truncate_replay_child")')
        clone.start()
        backup_reader = wire.Connection(clone.port)
        connections.append(backup_reader)
        assert backup_reader.query("SELECT id FROM truncate_replay ORDER BY id") == [["42"], ["43"]]
        target.control('(physical-source-pin-release "truncate_replay_child")')
        source.control('(physical-source-pin-release "truncate_replay_base")')
        print("truncate-replay: PASS (reader barrier, reset redo, crash, restartpoint and promotion)")
    finally:
        for connection in connections:
            connection.close()
        clone.stop()
        target.stop()
        source.stop()
        print(f"artifacts: source={source.root} target={target.root}")


if __name__ == "__main__":
    run()

"""TRUNCATE must not read, rewrite or log the old relation's pages."""
import importlib.util
from pathlib import Path
import time

spec = importlib.util.spec_from_file_location("wire", Path(__file__).with_name("command-complete.py"))
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def lsn(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def run():
    server = wire.OwnedLisp()
    connection = None
    try:
        server.start()
        connection = wire.Connection(server.port)
        for name, count in (("truncate_small", 2), ("truncate_large", 12000)):
            connection.query(f"CREATE TABLE {name}(id int PRIMARY KEY)")
            for first in range(1, count + 1, 500):
                connection.query(f"INSERT INTO {name} VALUES " +
                                 ",".join(f"({i})" for i in range(first, min(first + 500, count + 1))))
        connection.query("CHECKPOINT")
        server.control("(defvar pg::*truncate-size-images* 0)")
        server.control("(defvar pg::*truncate-size-original* (symbol-function 'pg::wal-primary-append-pages))")
        server.control("(setf (symbol-function 'pg::wal-primary-append-pages) "
                       "(lambda (keys spool) (incf pg::*truncate-size-images* (length keys)) "
                       "(funcall pg::*truncate-size-original* keys spool)))")
        measurements = []
        try:
            for name in ("truncate_small", "truncate_large"):
                server.control("(setf pg::*truncate-size-images* 0)")
                before = lsn(connection.query("SELECT pg_current_wal_lsn()::text")[0][0])
                start = time.monotonic()
                connection.query(f"TRUNCATE {name}")
                elapsed = time.monotonic() - start
                after = lsn(connection.query("SELECT pg_current_wal_lsn()::text")[0][0])
                marker = server.root / (name + ".images")
                server.control(f'(with-open-file (out "{marker}" :direction :output) '
                               '(format out "~d" pg::*truncate-size-images*))')
                images = int(marker.read_text())
                measurements.append((images, after - before, elapsed))
                assert connection.query(f"SELECT count(*) FROM {name}") == [["0"]]
                connection.query(f"INSERT INTO {name} VALUES(42)")
                assert connection.query(f"SELECT id FROM {name} WHERE id=42") == [["42"]]
        finally:
            server.control("(setf (symbol-function 'pg::wal-primary-append-pages) pg::*truncate-size-original*)")
        print("truncate-size measurements (page images, WAL bytes, seconds):", measurements, flush=True)
        assert measurements[1][0] <= measurements[0][0] + 2, measurements
        assert measurements[1][1] <= measurements[0][1] + 1024, measurements
        server.stop(immediate=True)
        connection.close()
        connection = None
        server.start()
        connection = wire.Connection(server.port)
        for name in ("truncate_small", "truncate_large"):
            assert connection.query(f"SELECT id FROM {name}") == [["42"]]
        connection.query("CHECKPOINT")
        print("truncate-size: PASS (bounded page images/WAL and crash recovery)")
    finally:
        if connection:
            connection.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()

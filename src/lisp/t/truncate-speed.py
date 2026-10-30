"""TRUNCATE work stays bounded even when an old fork has a huge sparse tail.

The tail deliberately has no tuples to inspect. TRUNCATE must discard storage
without reading it, just as C can truncate a relation containing new zero pages.
The real-row/index comparison lives in truncate-size.py. Wall times are reported
for diagnosis; page I/O and WAL volume, rather than timing, gate this regression.
"""
import importlib.util
import os
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
        connection.query("CREATE TABLE truncate_speed(id int)")
        identity = server.root / "truncate-speed-node"
        server.control(
            f'(let ((pg::*backend* (pg::make-backend))) '
            f'(with-open-file (out "{identity}" :direction :output) '
            '(format out "~d" (pg::pg-class-relfilenode '
            '(pg::resolve-table-or-error "truncate_speed")))))')
        node = int(identity.read_text())
        path = server.data / "base" / str(node)
        server.control("(defvar pg::*truncate-speed-reads* 0)")
        server.control("(defvar pg::*truncate-speed-writes* 0)")
        server.control("(defvar pg::*truncate-speed-read* (symbol-function 'pg::smgr-read))")
        server.control("(defvar pg::*truncate-speed-write* (symbol-function 'pg::smgr-write))")
        measurements, lengths = [], []
        for blocks in (4, 256, 1 << 20):
            connection.query("INSERT INTO truncate_speed VALUES(7)")
            connection.query("CHECKPOINT")
            # The server is idle and CHECKPOINT has drained its dirty pages.
            # Grow only this owned fork; ftruncate allocates no tail data blocks.
            with path.open("r+b") as stream:
                stream.truncate(blocks * 8192)
                stream.flush()
                os.fsync(stream.fileno())
            server.control(f'(assert (= {blocks} (pg::smgr-nblocks {node})))')
            before = lsn(connection.query("SELECT pg_current_wal_lsn()::text")[0][0])
            server.control(
                "(setf pg::*truncate-speed-reads* 0 pg::*truncate-speed-writes* 0 "
                "(symbol-function 'pg::smgr-read) "
                "(lambda (&rest args) (incf pg::*truncate-speed-reads*) "
                "(apply pg::*truncate-speed-read* args)) "
                "(symbol-function 'pg::smgr-write) "
                "(lambda (&rest args) (incf pg::*truncate-speed-writes*) "
                "(apply pg::*truncate-speed-write* args)))")
            try:
                start = time.monotonic()
                connection.query("TRUNCATE truncate_speed")
                elapsed = time.monotonic() - start
            finally:
                server.control(
                    "(setf (symbol-function 'pg::smgr-read) pg::*truncate-speed-read* "
                    "(symbol-function 'pg::smgr-write) pg::*truncate-speed-write*)")
            after = lsn(connection.query("SELECT pg_current_wal_lsn()::text")[0][0])
            counts = server.root / "truncate-speed-counts"
            server.control(
                f'(with-open-file (out "{counts}" :direction :output :if-exists :supersede) '
                '(format out "~d ~d" pg::*truncate-speed-reads* pg::*truncate-speed-writes*))')
            reads, writes = map(int, counts.read_text().split())
            measurement = (blocks, reads, writes, after - before, elapsed)
            measurements.append(measurement)
            print("truncate-speed (old blocks, reads, writes, WAL bytes, seconds):", measurement, flush=True)
            # Gate each size before trying the 8-GiB sparse case. A page-loop
            # regression therefore fails on the small fixture without huge work.
            assert reads <= 4 and writes <= 4 and after - before < 65536, measurement
            lengths.append(path.stat().st_size)
            assert connection.query("SELECT count(*) FROM truncate_speed") == [["0"]]
        assert max(row[3] for row in measurements) <= min(row[3] for row in measurements) + 1024
        assert lengths == [0] * len(measurements), lengths
        server.stop(immediate=True)
        connection.close()
        connection = None
        server.start()
        connection = wire.Connection(server.port)
        assert connection.query("SELECT count(*) FROM truncate_speed") == [["0"]]
        assert path.stat().st_size == 0
        print("truncate-speed: PASS (bounded work through an 8-GiB sparse fork, physical shrink and restart)")
    finally:
        if connection is not None:
            connection.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()

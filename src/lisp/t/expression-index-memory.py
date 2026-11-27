"""Compare sequential, full-expression and partial-expression index workloads."""

import argparse
import contextlib
import json
import random
import re
import runpy
import shutil
from pathlib import Path


HERE = Path(__file__).resolve().parent
memory = runpy.run_path(str(HERE / "workload-memory.py"))


def insert_rows(connection, start, count):
    for offset in range(start, start + count, 500):
        rows = ",".join(f"({i},{-i},{i % 100})" for i in range(offset, min(offset + 500, start + count)))
        connection.query("INSERT INTO index_workload VALUES " + rows)


def wal_position(connection):
    [[value]] = connection.query("SELECT pg_current_wal_lsn()")
    high, low = (int(word, 16) for word in value.split("/"))
    return (high << 32) + low


def index_bytes(server):
    result = server.control(
        "(let* ((pg::*backend* (pg::make-backend :database \"postgres\")) "
        "       (index (pg::lookup-index \"workload_index\"))) "
        "  (format t \"~&INDEX-BYTES=~d~%\" "
        "    (* pg::+blcksz+ (pg::buf-nblocks (pg::pg-index-relfilenode index)))))"
    )
    return int(re.search(r"INDEX-BYTES=(\d+)", result).group(1))


def measured_writes(server, connection, operation):
    before = wal_position(connection)
    result = memory["measure"](server, operation, None)
    return dict(wal_bytes=wal_position(connection) - before, **result)


def lookup_sql(key, percentage):
    return f"SELECT id FROM index_workload WHERE abs(key) = {key} AND bucket < {percentage}"


def lookup_counters(connection, query):
    [[raw]] = connection.query("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF) " + query)
    summary = json.loads(raw)[0]
    pending = [summary["Plan"]]
    scans = []
    filter_removals = 0
    while pending:
        node = pending.pop()
        pending.extend(node.get("Plans", ()))
        filter_removals += node.get("Rows Removed by Filter", 0)
        if node["Node Type"] in ("Seq Scan", "Index Scan"):
            scans.append(node)
    [scan] = scans
    return dict(
        scan=scan["Node Type"], rows_removed=filter_removals,
        relation_reads=summary["Native Relation Reads"],
        clean_cache_hits=summary["Native Clean Cache Hits"],
    )


def run_variant(server, connection, args, percentage, variant):
    connection.query("CREATE TABLE index_workload (id int, key int, bucket int)")
    try:
        insert_rows(connection, 0, args.rows)
        connection.query("ANALYZE index_workload")
        indexed = variant != "sequential"
        info = dict(variant=variant, percentage=percentage, rows=args.rows)
        if indexed:
            predicate = f" WHERE bucket < {percentage}" if variant == "partial" else ""
            create = "CREATE INDEX workload_index ON index_workload (abs(key))" + predicate
            built = measured_writes(server, connection, lambda: connection.query(create))
            print(json.dumps(dict(phase="build", index_bytes=index_bytes(server), **info, **built)), flush=True)
        query = lookup_sql(0, percentage)
        counters = lookup_counters(connection, query)

        def read_points():
            for i in range(args.lookups):
                key = 100 * (i % (args.rows // 100))
                assert connection.query(lookup_sql(key, percentage)) == [[str(key)]]

        read_points()
        lookups = memory["measure"](server, read_points, None)
        print(json.dumps(dict(phase="lookup", lookups=args.lookups, **info, **counters, **lookups)), flush=True)
        writes = measured_writes(server, connection, lambda: insert_rows(connection, args.rows, args.writes))
        for key in (args.rows, args.rows + args.writes - 100):
            assert connection.query(lookup_sql(key, percentage)) == [[str(key)]]
        print(json.dumps(dict(phase="insert", inserted=args.writes, **info, **writes)), flush=True)
    finally:
        connection.query("DROP TABLE index_workload")


def run_external_builds(server, connection, rows):
    connection.query("CREATE TABLE index_workload (id int, bucket int, payload text)")
    connection.query("ALTER TABLE index_workload ALTER COLUMN payload SET STORAGE EXTERNAL")
    payload = "".join(random.Random(19).choices("abcdefghijklmnopqrstuvwxyz0123456789", k=6400))
    for start in range(0, rows, 20):
        values = ",".join(f"({i},{i % 100},'{payload}')" for i in range(start, min(rows, start + 20)))
        connection.query("INSERT INTO index_workload VALUES " + values)
    connection.query("ANALYZE index_workload")
    server.control(
        "(progn (defparameter pg::*benchmark-toast-fetches* 0) "
        "  (let ((fetch (symbol-function 'pg::toast-fetch-value))) "
        "    (setf (symbol-function 'pg::toast-fetch-value) "
        "          (lambda (&rest args) "
        "            (incf pg::*benchmark-toast-fetches*) (apply fetch args)))))"
    )
    for concurrent in (False, True):
        create = ("CREATE INDEX " + ("CONCURRENTLY " if concurrent else "")
                  + "workload_index ON index_workload ((length(payload) + id)) WHERE bucket < 1")
        for sample in range(3):
            server.control("(setf pg::*benchmark-toast-fetches* 0)")
            built = measured_writes(server, connection, lambda: connection.query(create))
            response = server.control('(format t "~&FETCHES=~d~%" pg::*benchmark-toast-fetches*)')
            fetches = int(re.search(r"FETCHES=(\d+)", response).group(1))
            assert connection.query(
                "SELECT id FROM index_workload WHERE length(payload) + id = 6400 AND bucket < 1"
            ) == [["0"]]
            assert connection.query(
                "SELECT id FROM index_workload WHERE length(payload) + id = 6401 AND bucket < 1"
            ) == []
            print(json.dumps(dict(phase="external_build", concurrent=concurrent, rows=rows,
                                  sample=sample + 1, toast_fetches=fetches,
                                  index_bytes=index_bytes(server), **built)), flush=True)
            connection.query("DROP INDEX workload_index")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int)
    parser.add_argument("--writes", type=int, default=1000)
    parser.add_argument("--lookups", type=int, default=20)
    parser.add_argument("--percentage", type=int, choices=(1, 10, 90, 100), action="append")
    parser.add_argument("--external", action="store_true", help="measure builds on external text at 1% coverage")
    args = parser.parse_args()
    if args.rows is None:
        args.rows = 1000 if args.external else 10000
    if any(value <= 0 or value % 100 for value in (args.rows, args.writes)) or args.lookups <= 0:
        parser.error("rows and writes must be positive multiples of 100, and lookups must be positive")
    server = memory["OwnedLisp"]()
    try:
        server.start()
        with contextlib.closing(memory["connect"](server)) as connection:
            if args.external:
                run_external_builds(server, connection, args.rows)
            else:
                for percentage in args.percentage or (1, 10, 90, 100):
                    for variant in ("sequential", "full", "partial"):
                        run_variant(server, connection, args, percentage, variant)
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    main()

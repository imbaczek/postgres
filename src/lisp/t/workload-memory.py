"""Measure process allocation and memory for common pg-lisp SQL workloads.

Run with Python 3 on Linux. Each sample starts a disposable server, prepares
its data, and measures the workload window separately from setup and forced
full GC. --profiles collects SB-SPROF allocation stacks in separate runs.
"""

import argparse
import concurrent.futures
import json
import random
import re
import runpy
import shutil
import statistics
import sys
import threading
import time
from pathlib import Path


fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
OwnedLisp, Connection = fixture["OwnedLisp"], fixture["Connection"]

WORKLOADS = (
    "scan", "point_reads", "sort", "aggregate", "join", "copy_out",
    "heap_insert", "indexed_insert", "update", "delete", "mixed", "toast",
)


def connect(server):
    sql = Connection(server.port)
    sql.socket.settimeout(120)
    return sql


def insert(sql, table, start, end, width=80):
    payload = "x" * width
    for offset in range(start, end, 128):
        rows = ",".join(f"({i},{i % 64},'{payload}')" for i in range(offset, min(offset + 128, end)))
        sql.query(f"INSERT INTO {table} VALUES {rows}")


def check(condition, detail):
    if not condition:
        raise AssertionError(detail)


def prepare(sql, workload, rows):
    sql.query("CREATE TABLE items (id integer, grp integer, payload text)")
    if workload in {"point_reads", "indexed_insert", "update", "delete", "mixed"}:
        sql.query("CREATE UNIQUE INDEX items_id ON items(id)")
    if workload == "indexed_insert":
        sql.query("CREATE INDEX items_grp ON items(grp)")
    if workload not in {"heap_insert", "indexed_insert", "toast"}:
        insert(sql, "items", 0, rows)
    if workload == "join":
        sql.query("CREATE TABLE groups (id integer, grp integer, payload text)")
        insert(sql, "groups", 0, 64, 8)


def exercise(server, sql, workload, rows, repeat):
    def run_query(query, expected):
        for _ in range(repeat):
            check(sql.query(query) == expected, query)

    if workload == "scan":
        run_query("SELECT count(*), sum(id) FROM items", [[str(rows), str(rows * (rows - 1) // 2)]])
    elif workload == "point_reads":
        for i in range(repeat * 64):
            key = i * 241 % rows
            check(sql.query(f"SELECT id FROM items WHERE id={key}") == [[str(key)]], key)
    elif workload == "sort":
        for _ in range(repeat):
            result = sql.query("SELECT payload,id FROM items ORDER BY grp DESC,id")
            check(len(result) == rows, len(result))
    elif workload == "aggregate":
        for _ in range(repeat):
            result = sql.query("SELECT grp,count(*),sum(id) FROM items GROUP BY grp")
            check(sum(int(group[1]) for group in result) == rows, result)
    elif workload == "join":
        run_query("SELECT count(*) FROM items JOIN groups ON items.grp=groups.id", [[str(rows)]])
    elif workload == "copy_out":
        for _ in range(repeat):
            sql.send(b"Q", b"COPY (SELECT id,grp,payload FROM items) TO STDOUT\0")
            tags = []
            while True:
                kind, body = sql.receive()
                if kind == b"Z":
                    break
                if kind == b"C":
                    tags.append(body.rstrip(b"\0"))
                check(kind != b"E", body)
            check(tags == [f"COPY {rows}".encode()], tags)
    elif workload in {"heap_insert", "indexed_insert"}:
        insert(sql, "items", 0, rows)
    elif workload == "update":
        sql.query("UPDATE items SET grp=grp+1 WHERE id % 4=0")
    elif workload == "delete":
        sql.query("DELETE FROM items WHERE id % 4=0")
    elif workload == "mixed":
        def client(worker):
            connection = connect(server)
            try:
                for i in range(repeat * 16):
                    key = i * 16 + worker
                    connection.query("BEGIN")
                    check(connection.query(f"SELECT id FROM items WHERE id={key}") == [[str(key)]], key)
                    connection.query(f"UPDATE items SET grp=grp+1 WHERE id={key}")
                    connection.query("COMMIT")
            finally:
                connection.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(client, range(4)))
    else:
        # Force external storage with incompressible, repeatable ASCII payloads.
        rng = random.Random(42)
        for i in range(32):
            payload = rng.randbytes(4096).hex()
            sql.query(f"INSERT INTO items VALUES ({i},0,'{payload}')")
        run_query("SELECT sum(length(payload)) FROM items", [[str(32 * 8192)]])


def snapshot(server, collect=False):
    gc = "(sb-ext:gc :full t)" if collect else ""
    output = server.control(
        f'(progn {gc} (format t "~&MEMORY=~d ~d ~d~%" '
        '(sb-ext:get-bytes-consed) (sb-kernel:dynamic-usage) '
        '(round (* 1000 sb-ext:*gc-run-time*) internal-time-units-per-second)) (values))'
    )
    return tuple(int(value) for value in re.search(r"MEMORY=(\d+) (\d+) (\d+)", output).groups())


def rss(pid):
    with Path(f"/proc/{pid}/status").open() as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("VmRSS missing for benchmark server")


def measure(server, operation, profile_path):
    pid = server.process.pid
    if profile_path:
        server.control("(require :sb-sprof)")
    before, live_before, gc_before = snapshot(server, collect=True)
    rss_before = rss(pid)
    peak = rss_before
    stopped = threading.Event()

    def sample():
        nonlocal peak
        while not stopped.wait(0.005):
            peak = max(peak, rss(pid))

    if profile_path:
        server.control("(sb-sprof:start-profiling :mode :alloc :max-samples 20000)")
    sampler = threading.Thread(target=sample)
    sampler.start()
    start = time.monotonic()
    try:
        operation()
    finally:
        elapsed = time.monotonic() - start
        stopped.set()
        sampler.join()
        peak = max(peak, rss(pid))
        if profile_path:
            server.control("(sb-sprof:stop-profiling)")
    after, _, gc_after = snapshot(server)
    _, live_after, _ = snapshot(server, collect=True)
    if profile_path:
        profile_path.write_text(server.control(
            "(progn (sb-sprof:report :type :graph :max 40 :min-percent 0.5 "
            ":show-progress nil) (values))"
        ))
    return {
        "seconds": elapsed, "allocated_bytes": after - before,
        "peak_rss_bytes": peak, "rss_growth_bytes": peak - rss_before,
        "live_heap_before_bytes": live_before, "live_heap_after_bytes": live_after,
        "gc_ms": gc_after - gc_before,
    }


def run_case(workload, rows, repeat, profile):
    server = OwnedLisp()
    sql = None
    succeeded = False
    try:
        server.start()
        sql = connect(server)
        prepare(sql, workload, rows)
        result = measure(server, lambda: exercise(server, sql, workload, rows, repeat), profile)
        if workload in {"heap_insert", "indexed_insert", "delete", "update"}:
            expected = rows - (rows + 3) // 4 if workload == "delete" else rows
            check(sql.query("SELECT count(*) FROM items") == [[str(expected)]], workload)
            if workload == "update":
                check(sql.query("SELECT count(*) FROM items WHERE grp % 4=0") == [["0"]], workload)
        succeeded = True
        return result
    finally:
        if sql is not None:
            sql.close()
        server.stop()
        if succeeded:
            shutil.rmtree(server.root)
        else:
            print(f"Retained failed fixture at {server.root}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--repeat", type=int, default=4)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--case", choices=WORKLOADS, action="append")
    parser.add_argument("--profiles", type=Path, help="Separate SB-SPROF allocation sample runs")
    args = parser.parse_args()
    workloads = args.case or WORKLOADS
    if min(args.rows, args.repeat, args.runs) < 1:
        parser.error("use positive counts")
    if "mixed" in workloads and args.rows < args.repeat * 256:
        parser.error("mixed requires at least repeat * 256 rows")
    if args.profiles:
        args.profiles.mkdir(parents=True, exist_ok=True)
    results = {}
    for workload in workloads:
        runs = [run_case(workload, args.rows, args.repeat, None) for _ in range(args.runs)]
        results[workload] = {
            "samples": runs,
            "median": {key: statistics.median(run[key] for run in runs) for key in runs[0]},
        }
        if args.profiles:
            run_case(workload, args.rows, args.repeat, args.profiles / f"{workload}.txt")
    print(json.dumps({"rows": args.rows, "repeat": args.repeat, "results": results}, indent=2))


if __name__ == "__main__":
    main()

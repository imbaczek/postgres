"""Measure process allocation and memory for common pg-lisp SQL workloads.

Run with Python 3 on Linux. Each sample starts a disposable server, prepares
its data, and measures the workload window separately from setup and forced
full GC. --profiles collects SB-SPROF allocation stacks in separate runs.
"""

import argparse
import concurrent.futures
import json
import math
import os
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
CONCURRENT_READ_WORKLOADS = ("concurrent_point_reads", "concurrent_scan")
MIXED_WORKLOADS = {"mixed", "mixed_serial"}
EXPLAIN_WORKLOADS = {"scan", "point_reads", "sort", "join", *CONCURRENT_READ_WORKLOADS}
EXPLAIN_OPTIONS = {
    "rows": "BUFFERS OFF, TIMING OFF",
    "counters": "BUFFERS ON, TIMING OFF",
    "timing": "BUFFERS ON, TIMING ON",
}


class MeasuredConnection(Connection):
    def __init__(self, port, latencies):
        super().__init__(port)
        self.latencies = latencies

    def query(self, query):
        if self.latencies is None:
            return super().query(query)
        start = time.monotonic_ns()
        result = super().query(query)
        self.latencies.append(time.monotonic_ns() - start)
        return result


def connect(server, latencies=None):
    sql = MeasuredConnection(server.port, latencies)
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
    if workload in {
        "point_reads", "concurrent_point_reads", "indexed_insert", "update", "delete", *MIXED_WORKLOADS,
    }:
        sql.query("CREATE UNIQUE INDEX items_id ON items(id)")
    if workload == "indexed_insert":
        sql.query("CREATE INDEX items_grp ON items(grp)")
    if workload not in {"heap_insert", "indexed_insert", "toast"}:
        insert(sql, "items", 0, rows)
    if workload == "join":
        sql.query("CREATE TABLE groups (id integer, grp integer, payload text)")
        insert(sql, "groups", 0, 64, 8)


def exercise(server, sql, workload, rows, repeat, explain_mode=None):
    def select(query, expected_rows, connection=sql):
        if explain_mode is None:
            result = connection.query(query)
            check(len(result) == expected_rows, query)
            return result
        options = EXPLAIN_OPTIONS[explain_mode]
        result = connection.query(f"EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, {options}) {query}")
        root = json.loads(result[0][0])[0]["Plan"]
        check(root["Actual Rows"] == expected_rows, root)
        check(("Actual Total Time" in root) == (explain_mode == "timing"), root)
        check(("Native Relation Reads" in root) == (explain_mode != "rows"), root)
        return None

    def run_query(query, expected, connection=sql):
        for _ in range(repeat):
            result = select(query, len(expected), connection)
            if result is not None:
                check(result == expected, query)

    def scan(connection):
        expected = [[str(rows), str(rows * (rows - 1) // 2)]]
        run_query("SELECT count(*), sum(id) FROM items", expected, connection)

    def point_reads(connection, count, offset=0):
        for i in range(count):
            key = (i + offset) * 241 % rows
            result = select(f"SELECT id FROM items WHERE id={key}", 1, connection)
            if result is not None:
                check(result == [[str(key)]], key)

    def run_clients(operation):
        def client(worker):
            connection = connect(server, sql.latencies)
            try:
                operation(connection, worker)
            finally:
                connection.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(client, range(4)))

    if workload == "scan":
        scan(sql)
    elif workload == "concurrent_scan":
        run_clients(lambda connection, _: scan(connection))
    elif workload == "point_reads":
        point_reads(sql, repeat * 64)
    elif workload == "concurrent_point_reads":
        run_clients(lambda connection, worker: point_reads(connection, repeat * 16, worker * repeat * 16))
    elif workload == "sort":
        for _ in range(repeat):
            select("SELECT payload,id FROM items ORDER BY grp DESC,id", rows)
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
    elif workload in MIXED_WORKLOADS:
        def mixed(connection, worker):
            for i in range(repeat * 16):
                key = i * 16 + worker
                connection.query("BEGIN")
                check(connection.query(f"SELECT id FROM items WHERE id={key}") == [[str(key)]], key)
                connection.query(f"UPDATE items SET grp=grp+1 WHERE id={key}")
                connection.query("COMMIT")

        if workload == "mixed":
            run_clients(mixed)
        else:
            for worker in range(4):
                mixed(sql, worker)
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


def cpu_seconds(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


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
    cpu_before = cpu_seconds(pid)
    start = time.monotonic()
    try:
        operation()
    finally:
        elapsed = time.monotonic() - start
        cpu_elapsed = cpu_seconds(pid) - cpu_before
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
        "seconds": elapsed, "server_cpu_seconds": cpu_elapsed,
        "allocated_bytes": after - before,
        "peak_rss_bytes": peak, "rss_growth_bytes": peak - rss_before,
        "live_heap_before_bytes": live_before, "live_heap_after_bytes": live_after,
        "gc_ms": gc_after - gc_before,
    }


def run_case(workload, rows, repeat, profile, statement_statistics=False,
             explain_mode=None, warmup=False):
    parameters = (("pg_stat_statements.enabled", "on"),) if statement_statistics else ()
    server = OwnedLisp(parameters)
    sql = None
    succeeded = False
    try:
        server.start()
        sql = connect(server)
        prepare(sql, workload, rows)
        if warmup:
            exercise(server, sql, workload, rows, 1, explain_mode)
        if statement_statistics:
            sql.query("SET pg_stat_statements.track TO 'none'")
            sql.query("SELECT pg_stat_statements_reset()")
            sql.query("RESET pg_stat_statements.track")
        # COPY uses a raw frontend exchange and has no Connection.query samples.
        sql.latencies = []
        result = measure(server, lambda: exercise(server, sql, workload, rows, repeat, explain_mode),
                         profile)
        latencies = sorted(sql.latencies)
        sql.latencies = None
        if latencies:
            result.update(
                sql_queries=len(latencies),
                sql_queries_per_second=len(latencies) / result["seconds"],
                sql_median_ms=statistics.median(latencies) / 1e6,
                sql_p95_ms=latencies[math.ceil(0.95 * len(latencies)) - 1] / 1e6,
            )
        if statement_statistics:
            sql.query("SET pg_stat_statements.track TO 'none'")
            calls = sql.query("SELECT coalesce(sum(calls), 0) FROM pg_stat_statements")
            result["tracked_calls"] = int(calls[0][0])
            [[dealloc]] = sql.query("SELECT dealloc FROM pg_stat_statements_info")
            result["statistics_evictions"] = int(dealloc)
            # Mixed transactions track SELECT and UPDATE. BEGIN and COMMIT are utility commands.
            tracked_per_repeat = {
                "point_reads": 64, "concurrent_point_reads": 64,
                "scan": 1, "concurrent_scan": 4, "mixed": 128, "mixed_serial": 128,
            }.get(workload)
            if tracked_per_repeat is not None and explain_mode is None:
                check(result["tracked_calls"] == repeat * tracked_per_repeat, result)
        if workload in {"heap_insert", "indexed_insert", "delete", "update"}:
            expected = rows - (rows + 3) // 4 if workload == "delete" else rows
            check(sql.query("SELECT count(*) FROM items") == [[str(expected)]], workload)
            if workload == "update":
                check(sql.query("SELECT count(*) FROM items WHERE grp % 4=0") == [["0"]], workload)
        if workload in MIXED_WORKLOADS:
            groups, remainder = divmod(rows, 64)
            initial_sum = groups * (63 * 64 // 2) + remainder * (remainder - 1) // 2
            expected_sum = initial_sum + 64 * (repeat + int(warmup))
            check(sql.query("SELECT sum(grp) FROM items") == [[str(expected_sum)]], workload)
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
    parser.add_argument("--repeat", type=int, default=4,
                        help="Iterations per sample, per client for concurrent_scan")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--case", choices=(*WORKLOADS, *CONCURRENT_READ_WORKLOADS, "mixed_serial"),
                        action="append")
    parser.add_argument("--profiles", type=Path, help="Separate SB-SPROF allocation sample runs")
    parser.add_argument("--statement-statistics", action="store_true",
                        help="Enable pg_stat_statements and report observed calls per sample")
    parser.add_argument("--explain-mode", choices=EXPLAIN_OPTIONS,
                        help="Run SELECT through EXPLAIN ANALYZE with the selected instrumentation")
    parser.add_argument("--warmup", action="store_true",
                        help="Run one warmup iteration for SELECT and mixed read/write workloads")
    args = parser.parse_args()
    workloads = args.case or WORKLOADS
    if min(args.rows, args.repeat, args.runs) < 1:
        parser.error("use positive counts")
    if MIXED_WORKLOADS.intersection(workloads) and args.rows < args.repeat * 256:
        parser.error("mixed requires at least repeat * 256 rows")
    if args.explain_mode and not set(workloads) <= EXPLAIN_WORKLOADS:
        parser.error("EXPLAIN requires explicit SELECT cases: " + ", ".join(sorted(EXPLAIN_WORKLOADS)))
    if args.warmup and not set(workloads) <= EXPLAIN_WORKLOADS | MIXED_WORKLOADS:
        parser.error("warmup requires explicit SELECT or mixed read/write cases")
    if args.profiles:
        args.profiles.mkdir(parents=True, exist_ok=True)
    results = {}
    for workload in workloads:
        runs = [
            run_case(workload, args.rows, args.repeat, None, args.statement_statistics,
                     args.explain_mode, args.warmup)
            for _ in range(args.runs)
        ]
        results[workload] = {
            "samples": runs,
            "median": {key: statistics.median(run[key] for run in runs) for key in runs[0]},
        }
        if args.profiles:
            run_case(workload, args.rows, args.repeat, args.profiles / f"{workload}.txt",
                     args.statement_statistics, args.explain_mode, args.warmup)
    print(json.dumps({"rows": args.rows, "repeat": args.repeat,
                      "statement_statistics": args.statement_statistics,
                      "explain_mode": args.explain_mode, "warmup": args.warmup, "results": results},
                     indent=2))


if __name__ == "__main__":
    main()

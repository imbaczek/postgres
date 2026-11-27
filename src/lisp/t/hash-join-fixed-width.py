"""Compare warmed fixed-width count joins on pg-lisp and PostgreSQL C."""

import argparse
import contextlib
import json
import runpy
import shutil
import threading
import time
from pathlib import Path


HERE = Path(__file__).resolve().parent
MEMORY = runpy.run_path(str(HERE / "workload-memory.py"))
REFERENCE = runpy.run_path(str(HERE / "btree-range.py"))["reference_server"]
KEYS = {
    "int4": "k::int4",
    "int8": "k + 4611686018427387904::bigint",
    "float4": "((k - 500000)::float8 / 7::float8)::float4",
    "float8": "(k - 500000)::float8 / 7::float8",
    "date": "DATE '0001-01-01' + k::int4",
    "time": "TIME '00:00:00' + k * INTERVAL '86399 microseconds'",
    "timestamp": "TIMESTAMP '280000-01-01 00:00:00' + k * INTERVAL '17 microseconds'",
    "timestamptz": "TIMESTAMPTZ '280000-01-01 00:00:00+00' + k * INTERVAL '17 microseconds'",
    "interval": "INTERVAL '2000000000 months' + k * INTERVAL '{step} 17 microseconds'",
}
QUERY = "SELECT count(*) FROM fixed_probe p JOIN fixed_build b ON p.k = b.k"


@contextlib.contextmanager
def lisp_connection():
    server = MEMORY["OwnedLisp"]()
    try:
        server.start()
        with contextlib.closing(MEMORY["connect"](server)) as connection:
            connection.socket.settimeout(600)
            yield server, connection
    finally:
        server.stop()
        shutil.rmtree(server.root)


def seed(connection, type_name, rows, native):
    if native:
        connection.query("CREATE TABLE fixed_source (k bigint)")
        for start in range(0, rows, 1000):
            values = ",".join(f"({i})" for i in range(start, min(rows, start + 1000)))
            connection.query("INSERT INTO fixed_source VALUES " + values)
        source = "fixed_source"
    else:
        source = f"generate_series(0::bigint, {rows - 1}::bigint) AS source(k)"
    for table, step in (("fixed_probe", "1 month 2 days"), ("fixed_build", "32 days")):
        expression = KEYS[type_name].format(step=step)
        connection.query(f"CREATE TABLE {table} (k {type_name})")
        connection.query(f"INSERT INTO {table} SELECT {expression} FROM {source}")
        connection.query(f"ANALYZE {table}")
        assert connection.query(f"SELECT count(*) FROM {table}") == [[str(rows)]]


def explain(connection, rows):
    document = json.loads(connection.query(
        "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) " + QUERY
    )[0][0])[0]
    stack = [document["Plan"]]
    while stack:
        node = stack.pop()
        if node["Node Type"] == "Hash Join":
            assert node["Actual Rows"] == rows, node
            return node
        stack.extend(node.get("Plans", []))
    raise AssertionError(document)


def measure_c(pid, operation):
    rss = MEMORY["rss"]
    cpu = MEMORY["cpu_seconds"]
    peak = rss(pid)
    done = threading.Event()

    def sample():
        nonlocal peak
        while not done.wait(0.005):
            peak = max(peak, rss(pid))

    monitor = threading.Thread(target=sample)
    monitor.start()
    start_cpu = cpu(pid)
    start = time.perf_counter()
    try:
        operation()
        elapsed = time.perf_counter() - start
        cpu_elapsed = cpu(pid) - start_cpu
    finally:
        done.set()
        monitor.join()
    return dict(seconds=elapsed, server_cpu_seconds=cpu_elapsed, peak_rss_bytes=max(peak, rss(pid)))


def benchmark(connection, type_name, rows, samples, server=None):
    native = server is not None
    connection.query("SET work_mem = '64kB'")
    if not native:
        connection.query("SET max_parallel_workers_per_gather = 0")
        connection.query("SET jit = off")
        assert connection.query("SHOW hash_mem_multiplier") == [["2"]]
        pid = int(connection.query("SELECT pg_backend_pid()")[0][0])
    seed(connection, type_name, rows, native)
    plan = explain(connection, rows)
    if rows == 1_000_000:
        if native:
            build_kib = 40 if type_name in ("int8", "float8", "time", "timestamp", "timestamptz") else 48
            assert plan["Native Hash Key Probes"] == rows, plan
            assert plan["Native Hash Peak Build Bytes"] == build_kib * 1024, plan
        else:
            build = next(node for node in plan["Plans"] if node["Node Type"] == "Hash")
            assert build["Hash Batches"] > 1, plan

    def check():
        assert connection.query(QUERY) == [[str(rows)]], type_name

    check()
    for sample in range(samples):
        if native:
            measurement = MEMORY["measure"](server, check, None)
        else:
            measurement = measure_c(pid, check)
        print(json.dumps(dict(engine="lisp" if native else "c", key=type_name,
                              rows=rows, sample=sample, plan=plan, **measurement)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-bin", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--lisp-samples", type=int, default=3)
    parser.add_argument("--c-samples", type=int, default=5)
    parser.add_argument("keys", nargs="*", metavar="TYPE", help=", ".join(KEYS))
    args = parser.parse_args()
    if min(args.rows, args.lisp_samples, args.c_samples) <= 0:
        parser.error("row and sample counts must be positive")
    for key in args.keys or KEYS:
        if key not in KEYS:
            parser.error(f"unknown key type {key!r}")
        with REFERENCE(args.reference_bin) as connection:
            benchmark(connection, key, args.rows, args.c_samples)
        with lisp_connection() as (server, connection):
            benchmark(connection, key, args.rows, args.lisp_samples, server)


if __name__ == "__main__":
    main()

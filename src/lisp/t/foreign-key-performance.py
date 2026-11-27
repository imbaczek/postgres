"""Compare FK statement CPU, allocation and peak RSS on Linux.

Run with --postgres-bin pointing at the C reference installation's bin directory.
SBCL and SBCL_HOME select the Lisp runtime, as in the wire regressions.
One JSON object per statement records the process high-water mark before and after
the statement. C measurements are per connection backend, Lisp measurements are
for the multithreaded server. The harness runs both servers serially.
Use --skip-allocation to measure a server without opening its control REPL.
"""

import argparse
import contextlib
import json
from pathlib import Path
import re
import runpy
import shutil
import socket
import subprocess
import tempfile
import time


fixture = runpy.run_path(str(Path(__file__).with_name("foreign-key-concurrency.py")))
OwnedLisp, Connection, WireError = (
    fixture["OwnedLisp"], fixture["Connection"], fixture["WireError"]
)

PRIMARY_TYPES = (
    ("int2", "smallint", str),
    ("int4", "integer", str),
    ("int8", "bigint", str),
    ("uuid", "uuid", lambda n: f"'01234567-89ab-cdef-0123-{n:012x}'"),
    ("varchar127", "varchar(127)", lambda n: "'" + f"{n:08d}" + "a" * (52 + n % 68) + "'"),
)

WIDTH_TYPES = (
    ("oid", "oid", lambda n: f"'{n}'"),
    ("float4", "real", lambda n: f"'{n}.25'"),
    ("float8", "double precision", lambda n: f"'{n}.25'"),
    ("time", "time", lambda n: f"'00:{n // 60:02}:{n % 60:02}'"),
    ("timetz", "timetz", lambda n: f"'00:{n // 60:02}:{n % 60:02}+00'"),
    ("timestamp", "timestamp", lambda n: f"'2024-01-01 00:{n // 60:02}:{n % 60:02}'"),
    ("timestamptz", "timestamptz", lambda n: f"'2024-01-01 00:{n // 60:02}:{n % 60:02}+00'"),
    ("interval", "interval", lambda n: f"'{n} seconds'"),
    ("numeric18", "numeric(18,4)", lambda n: f"'{n}.2500'"),
)


def cases(suite):
    if suite in ("primary", "width"):
        for name, sqltype, literal in PRIMARY_TYPES if suite == "primary" else WIDTH_TYPES:
            rows = ",".join(f"({literal(n)})" for n in range(1000))
            yield name, f"id {sqltype}", "id", rows, rows, f"id={literal(0)}", 999
        return
    yield from (work_cases() if suite == "work" else holdout_cases())


def work_cases():
    boolean = ",".join(["(true)", "(false)"] * 500)
    yield "bool_reuse", "id boolean", "id", "(true),(false)", boolean, "id=true", 500
    def long_prefix(n):
        return "'" + "p" * (52 + n % 68) + f"{n:08d}'"

    parent = ",".join(f"({long_prefix(n)})" for n in range(1000))
    child = ",".join(f"({long_prefix(n)})" for n in range(999, -1, -1))
    yield "varchar_prefix_reverse", "id varchar(127)", "id", parent, child, f"id={long_prefix(0)}", 999
    def mixed_interval(n):
        return f"'{-n // 50} months {n % 50} days {n}.125 seconds'"

    parent = ",".join(f"({mixed_interval(n)})" for n in range(1000))
    child = ",".join(f"({mixed_interval(n)})" for n in range(999, -1, -1))
    yield "interval_mixed_reverse", "id interval", "id", parent, child, f"id={mixed_interval(0)}", 999
    for name, definition, columns, literal, predicate in (
        ("date_reverse", "id date", "id", lambda n: f"(date '2024-01-01' + {n})",
         "id < date '2024-01-11'"),
        ("composite_reverse", "id int, other bigint", "id,other", lambda n: f"({n},{n * 3})",
         "id < 10"),
    ):
        parent = ",".join(literal(n) for n in range(1000))
        child = ",".join(literal(n) for n in range(999, -1, -1))
        yield name, definition, columns, parent, child, predicate, 990


def holdout_cases():
    def permuted(count):
        return [(position * 137) % count for position in range(count)]

    order = permuted(1000)
    parent = ",".join(f"({n},{n * 3})" for n in range(1000))
    child = ",".join(f"({n},{'NULL' if n % 4 == 0 else n * 3})" for n in order)
    yield "composite_nullable", "id int, other bigint", "id,other", parent, child, "id < 40", 970

    def ascii_key(n):
        length = 60 if n % 2 else 127
        return "'" + ("Z" if n % 2 else "0") * (length - 8) + f"{n:08d}'"

    parent = ",".join(f"({ascii_key(n)})" for n in range(1000))
    child = ",".join(f"({ascii_key(n)})" for n in order)
    yield "varchar_alternating_lengths", "id varchar(127)", "id", parent, child, f"id={ascii_key(989)}", 999

    def timestamp(n):
        return f"(timestamp '2001-03-01 00:00:00' + interval '{n - 500}.125 seconds')"

    parent = ",".join(f"({timestamp(n)})" for n in range(1000))
    child = ",".join(f"({timestamp(n)})" for n in order)
    yield ("timestamp_fractional", "id timestamp", "id", parent, child,
           f"id >= {timestamp(979)}", 979)


def resources(pid):
    proc = Path(f"/proc/{pid}")
    thread_cpu = {}
    for thread in (proc / "task").glob("*/schedstat"):
        try:
            thread_cpu[thread.parent.name] = int(thread.read_text().split()[0])
        except FileNotFoundError:
            continue
    peak_kib = int(re.search(r"^VmHWM:\s+(\d+)", (proc / "status").read_text(), re.M)[1])
    return thread_cpu, peak_kib


def bytes_consed(server):
    output = server.control('(format t "~&FK-BYTES=~d~%" (sb-ext:get-bytes-consed))')
    return int(re.search(r"FK-BYTES=(\d+)", output)[1])


def measure(connection, pid, server, sample, name, phase, sql, capture_allocations):
    capture = server is not None and capture_allocations
    before_bytes = bytes_consed(server) if capture else None
    before_cpu, before_peak = resources(pid)
    started = time.monotonic_ns()
    connection.query(sql)
    wall_ns = time.monotonic_ns() - started
    after_cpu, after_peak = resources(pid)
    after_bytes = bytes_consed(server) if capture else None
    # Backend and WAL writer threads persist across these statements. A control
    # thread can exit between samples, and its earlier CPU must not be subtracted.
    cpu_ns = sum(value - before_cpu.get(tid, 0) for tid, value in after_cpu.items())
    print(json.dumps(dict(engine="lisp" if server else "c", sample=sample, case=name,
                          phase=phase, cpu_ns=cpu_ns, wall_ns=wall_ns,
                          peak_before_kib=before_peak, peak_after_kib=after_peak,
                          allocated_bytes=after_bytes - before_bytes if capture else None)), flush=True)


def run_suite(port, server, workload, samples, capture_allocations=True):
    for sample in range(1, samples + 1):
        with contextlib.closing(Connection(port)) as connection:
            pid = server.process.pid if server else int(connection.query("SELECT pg_backend_pid()")[0][0])
            for name, definition, columns, parent, child, predicate, remaining in workload:
                try:
                    connection.query(f"CREATE TABLE p ({definition}, PRIMARY KEY ({columns}))")
                except WireError as error:
                    if not server or name != "oid" or error.sqlstate != "0A000":
                        raise
                    print(json.dumps(dict(engine="lisp", sample=sample, case=name,
                                          unsupported=str(error))), flush=True)
                    continue
                connection.query(
                    f"CREATE TABLE ch ({definition}, FOREIGN KEY ({columns}) REFERENCES p ON DELETE CASCADE)"
                )
                connection.query(f"CREATE TABLE plain ({definition})")
                connection.query(f"INSERT INTO p VALUES {parent}")
                for phase, sql in (("plain", f"INSERT INTO plain VALUES {child}"),
                                   ("insert", f"INSERT INTO ch VALUES {child}"),
                                   ("cascade", f"DELETE FROM p WHERE {predicate}")):
                    measure(connection, pid, server, sample, name, phase, sql, capture_allocations)
                actual = connection.query("SELECT count(*) FROM ch")
                if actual != [[str(remaining)]]:
                    raise AssertionError((name, remaining, actual))
                connection.query("DROP TABLE plain, ch, p")


def run_c(binaries, workload, samples):
    with tempfile.TemporaryDirectory(prefix="pg-fk-reference-") as root:
        data = Path(root) / "data"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        subprocess.run([binaries / "initdb", "-A", "trust", "-U", "postgres", "-D", data],
                       check=True, stdout=subprocess.DEVNULL)
        subprocess.run([binaries / "pg_ctl", "-w", "-D", data, "-l", Path(root) / "postgres.log",
                        "-o", f"-p {port} -h 127.0.0.1", "start"],
                       check=True, stdout=subprocess.DEVNULL)
        try:
            run_suite(port, None, workload, samples)
        finally:
            subprocess.run([binaries / "pg_ctl", "-w", "-D", data, "stop", "-m", "fast"],
                           check=True, stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-bin", type=Path, required=True)
    parser.add_argument("--suite", choices=("primary", "width", "work", "holdout"), default="primary")
    parser.add_argument("--case", help="Run one case in fresh server processes for an isolated RSS comparison")
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--skip-allocation", action="store_true")
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be positive")
    workload = [case for case in cases(args.suite) if args.case is None or case[0] == args.case]
    if not workload:
        parser.error(f"{args.case!r} is not in the {args.suite} suite")
    run_c(args.postgres_bin.resolve(), workload, args.samples)
    server = OwnedLisp()
    try:
        server.start()
        run_suite(server.port, server, workload, args.samples, not args.skip_allocation)
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    main()

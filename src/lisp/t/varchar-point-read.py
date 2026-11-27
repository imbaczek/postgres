"""Compare warmed serial varchar primary-key lookups on pg-lisp and PostgreSQL C."""

import argparse
import base64
import hashlib
import json
import random
import runpy
import struct
from pathlib import Path


HERE = Path(__file__).resolve().parent
FIXTURE = runpy.run_path(str(HERE / "hash-join-fixed-width.py"))
MEMORY = FIXTURE["MEMORY"]
from parking_wire import WireError

SELECT = "SELECT payload FROM idempotency_keys WHERE k = "
STATEMENT = b"point_read\0"


def key_for(row, min_length=60, max_length=127):
    digest = hashlib.shake_256(b"pg-lisp-point-read-v1" + struct.pack("!Q", row)).digest(98)
    length = min_length + int.from_bytes(digest[:2], "big") % (max_length - min_length + 1)
    prefix = base64.urlsafe_b64encode(digest[2:]).decode("ascii")
    return prefix[:length - 16] + f"{row:016x}"


def frame(kind, body):
    return kind + struct.pack("!I", len(body) + 4) + body


def collect(connection, completion=None):
    rows = []
    error = None
    while True:
        kind, body = connection.receive()
        if kind == b"D":
            rows.append(connection.data_row(body))
        elif kind == b"E":
            error = connection.error_fields(body)
        elif completion is not None and (kind, body) == completion:
            completion = None
        elif kind == b"Z":
            if error is not None:
                raise RuntimeError(error)
            assert completion is None, completion
            return rows


def prepare(connection, key_type):
    query = (SELECT + "$1").encode("ascii") + b"\0"
    parameter_oid = 1043 if key_type == "varchar(127)" else 25
    connection.socket.sendall(frame(b"P", STATEMENT + query + struct.pack("!HI", 1, parameter_oid))
                              + frame(b"S", b""))
    assert collect(connection, (b"1", b"")) == []


def operation(key, prepared):
    if not prepared:
        return frame(b"Q", (SELECT + f"'{key}'").encode("ascii") + b"\0")
    value = key.encode("ascii")
    bind = b"\0" + STATEMENT + struct.pack("!HHI", 0, 1, len(value)) + value + bytes(2)
    return frame(b"B", bind) + frame(b"E", bytes(5)) + frame(b"S", b"")


def make_reads(args, misses=False, warmup=False):
    rng = random.Random(263821 + 2 * misses + warmup)
    for _ in range(args.reads):
        row = rng.randrange(args.rows) + (args.rows if misses else 0)
        key = key_for(row, args.min_length, args.max_length)
        yield operation(key, args.prepared), [] if misses else [[str(row)]]


def verify_reads(connection, commands, check_completion=False):
    for request, expected in commands:
        connection.socket.sendall(request)
        completion = (b"C", b"SELECT 1\0" if expected else b"SELECT 0\0") if check_completion else None
        actual = collect(connection, completion)
        assert actual == expected, (actual, expected, request)


def verify_prepared_index(connection, server, key):
    if server is None:
        plan = connection.query(f"EXPLAIN EXECUTE point_read('{key}')")
        assert any("Index Scan" in line for row in plan for line in row), plan
        return
    server.control(
        "(defparameter *point-probe-original* "
        "(symbol-function 'open-btree-equal-cursor-unlocked)) "
        "(defparameter *point-probe-observed* nil) "
        "(setf (symbol-function 'open-btree-equal-cursor-unlocked) "
        "(lambda (relation key) "
        "(setf *point-probe-observed* t) "
        "(funcall *point-probe-original* relation key)))")
    try:
        verify_reads(connection, [(operation(key, True), [["0"]])])
        assert "POINT-PROBE-OBSERVED=T" in server.control(
            '(format t "POINT-PROBE-OBSERVED=~a" *point-probe-observed*)')
    finally:
        server.control("(setf (symbol-function 'open-btree-equal-cursor-unlocked) "
                       "*point-probe-original*)")


def seed(connection, args):
    connection.query(f"CREATE TABLE idempotency_keys (k {args.key_type} PRIMARY KEY, payload integer NOT NULL)")
    for start in range(0, args.rows, 500):
        stop = min(args.rows, start + 500)
        values = ",".join(f"('{key_for(row, args.min_length, args.max_length)}',{row})"
                          for row in range(start, stop))
        connection.query("INSERT INTO idempotency_keys VALUES " + values)
    connection.query("ANALYZE idempotency_keys")
    assert connection.query("SELECT count(*) FROM idempotency_keys") == [[str(args.rows)]]


def verify_constraints(connection, key, key_type):
    failures = [(key, "23505")]
    if key_type == "varchar(127)":
        failures.append(("x" * 128, "22001"))
    for probe, expected in failures:
        try:
            connection.query(f"INSERT INTO idempotency_keys VALUES ('{probe}',-1)")
        except WireError as error:
            assert error.sqlstate == expected, error
        else:
            raise AssertionError(f"expected SQLSTATE {expected}")


def benchmark(connection, args, server=None):
    engine = "c" if server is None else "lisp"
    connection.socket.settimeout(600)
    connection.query("SET work_mem = '64kB'")
    if server is None:
        connection.query("SET max_parallel_workers_per_gather = 0")
        connection.query("SET jit = off")
        pid = int(connection.query("SELECT pg_backend_pid()")[0][0])
    print(json.dumps(dict(engine=engine, stage="seed", rows=args.rows)), flush=True)
    seed(connection, args)
    key = key_for(0, args.min_length, args.max_length)
    verify_constraints(connection, key, args.key_type)
    plan = connection.query("EXPLAIN " + SELECT + f"'{key}'")
    if args.rows == 1_000_000:
        assert any("Index Scan" in line for row in plan for line in row), plan
    if args.prepared:
        prepare(connection, args.key_type)
        if args.key_type == "varchar(127)":
            verify_reads(connection, [(operation("x" * 128, True), [])])
        if args.rows == 1_000_000:
            verify_prepared_index(connection, server, key)
    verify_reads(connection, make_reads(args, warmup=True))
    for misses in (False, True):
        commands = list(make_reads(args, misses=misses))
        verify_reads(connection, commands, check_completion=True)
        for sample in range(args.samples):
            def run():
                verify_reads(connection, commands)

            if server is None:
                result = FIXTURE["measure_c"](pid, run)
            else:
                profile = args.profile if not misses and sample == 0 else None
                result = MEMORY["measure"](server, run, profile)
            print(json.dumps(dict(engine=engine, key_type=args.key_type, prepared=args.prepared, misses=misses,
                                  rows=args.rows, reads=args.reads, sample=sample,
                                  lengths=[args.min_length, args.max_length], literal_plan=plan, **result)),
                  flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-bin", type=Path)
    parser.add_argument("--engine", choices=("both", "c", "lisp"), default="both")
    parser.add_argument("--engine-root", type=Path, default=HERE.parents[2])
    parser.add_argument("--key-type", choices=("varchar(127)", "text"), default="varchar(127)")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--reads", type=int, default=20_000)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--profile", type=Path, help="write an allocation profile for the first Lisp hit batch")
    parser.add_argument("--min-length", type=int, default=60)
    parser.add_argument("--max-length", type=int, default=127)
    parser.add_argument("--prepared", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    if min(args.rows, args.reads, args.samples) <= 0:
        parser.error("row, read, and sample counts must be positive")
    if not 16 <= args.min_length <= args.max_length <= 127:
        parser.error("lengths must satisfy 16 <= minimum <= maximum <= 127")
    if args.engine != "lisp":
        if args.reference_bin is None:
            parser.error("--reference-bin is required when running PostgreSQL C")
        with FIXTURE["REFERENCE"](args.reference_bin) as connection:
            benchmark(connection, args)
    if args.engine != "c":
        MEMORY["OwnedLisp"].start.__globals__["LISP"] = args.engine_root.resolve() / "src/lisp"
        with FIXTURE["lisp_connection"]() as (server, connection):
            benchmark(connection, args, server)


if __name__ == "__main__":
    main()

"""Measure holdout SQL workloads on a selected pg-lisp checkout."""

import argparse
import contextlib
import json
import runpy
import shutil
from collections import Counter
from pathlib import Path


HERE = Path(__file__).resolve().parent
MEMORY = runpy.run_path(str(HERE / "workload-memory.py"))
from parking_wire import WireError

CASES = (
    "int4_scan", "mixed_scan", "int4_projection", "int4_residual",
    "text_join", "wide_left", "typed_skew", "typed_sparse",
    "nested_cross", "nested_full", "lateral_index", "parameterized_index",
    "int4_right", "int4_full",
)


def load(connection, table, columns, rows, extra_ddl=None):
    connection.query(f"CREATE TABLE {table} ({columns})")
    if extra_ddl:
        connection.query(extra_ddl)
    for start in range(0, len(rows), 250):
        values = ",".join(
            "(" + ",".join("NULL" if value is None else str(value) for value in row) + ")"
            for row in rows[start:start + 250]
        )
        connection.query(f"INSERT INTO {table} VALUES {values}")
    connection.query(f"ANALYZE {table}")


def prepare_scan(connection, case):
    if case == "int4_scan":
        values = [i - 50000 for i in range(100000) if i % 11]
        rows = [(i - 50000 if i % 11 else None,) for i in range(100000)]
        load(connection, "hold_scan", "k int", rows)
        return "SELECT count(k), sum(k) FROM hold_scan", [len(values), sum(values)], 7
    rows = [(i, f"'key{(i * 13) % 1000:04}'") for i in range(40000)]
    load(connection, "hold_scan", "k int, t text", rows)
    values = [i for i in range(40000) if (i * 13) % 1000 >= 500]
    return ("SELECT count(*), sum(k) FROM hold_scan WHERE t >= 'key0500'",
            [len(values), sum(values)], 7)


def prepare_join(connection, case):
    left_rows = 850 if case == "wide_left" else 900
    right_rows = 170 if case == "wide_left" else 1300
    left = [(i, i * 13 % 211) for i in range(left_rows)]
    right = [(i, i * 7 % 197) for i in range(right_rows)]
    matches = [(i, j, key) for i, key in left for j, other in right if key == other]
    join = "FROM hold_l l JOIN hold_r r ON l.k = r.k"
    if case in ("int4_right", "int4_full"):
        load(connection, "hold_l", "id int, k int", left)
        load(connection, "hold_r", "id int, k int", right)
        missing_right = len(right) - len({j for _, j, _ in matches})
        missing_left = len(left) - len({i for i, _, _ in matches}) if case == "int4_full" else 0
        kind = "FULL" if case == "int4_full" else "RIGHT"
        return (f"SELECT count(*), count(l.id), count(r.id) "
                f"FROM hold_l l {kind} JOIN hold_r r ON l.k = r.k",
                [len(matches) + missing_left + missing_right,
                 len(matches) + missing_left, len(matches) + missing_right], 3)
    if case == "int4_projection":
        load(connection, "hold_l", "k int", [(key,) for _, key in left])
        load(connection, "hold_r", "k int", [(key,) for _, key in right])
        return (f"SELECT count(*), sum(l.k) {join}",
                [len(matches), sum(key for _, _, key in matches)], 3)
    if case == "text_join":
        load(connection, "hold_l", "k text", [(f"'key{key}'",) for _, key in left])
        load(connection, "hold_r", "k text", [(f"'key{key}'",) for _, key in right])
        return (f"SELECT count(*), sum(length(l.k)) {join}",
                [len(matches), sum(len(f"key{key}") for _, _, key in matches)], 3)
    load(connection, "hold_l", "id int, k int", left)
    if case == "int4_residual":
        load(connection, "hold_r", "id int, k int", right)
        return f"SELECT count(*) {join} AND l.id < r.id", [sum(i < j for i, j, _ in matches)], 3
    payloads = [(i, key, "'" + ("abcde" + str(i).zfill(3)) * 800 + "'") for i, key in right]
    load(connection, "hold_r", "id int, k int, t text", payloads,
         "ALTER TABLE hold_r ALTER COLUMN t SET STORAGE EXTERNAL")
    return ("SELECT count(*), count(r.id), max(length(r.t)) "
            "FROM hold_l l LEFT JOIN hold_r r ON l.k = r.k",
            [left_rows, len(matches), 6400], 3)


def prepare_typed_join(connection, case):
    if case == "typed_skew":
        left = [None if i % 23 == 0 else 0 if i % 7 == 0 else i * 17 % 371 for i in range(5200)]
        right = [None if i % 19 == 0 else i % 5 if i % 11 == 0 else i * 11 % 311 for i in range(4400)]
    else:
        left = [None if i % 23 == 0 else i * 4096 - 1000000 for i in range(14000)]
        right = [None if i % 19 == 0 else (i + 6700) * 4096 - 1000000 for i in range(12000)]
    load(connection, "hold_l", "k int", [(key,) for key in left])
    load(connection, "hold_r", "k int", [(key,) for key in right])
    frequencies = Counter(key for key in right if key is not None)
    expected = sum(frequencies[key] for key in left if key is not None)
    return "SELECT count(*) FROM hold_l l JOIN hold_r r ON l.k = r.k", [expected], 3


def prepare_loop_join(connection, case):
    if case == "nested_cross":
        load(connection, "hold_l", "k int", [(i,) for i in range(240)])
        load(connection, "hold_r", "k int", [(i,) for i in range(200)])
        return ("SELECT count(*), sum(l.k + r.k) FROM hold_l l CROSS JOIN hold_r r",
                [48000, 200 * sum(range(240)) + 240 * sum(range(200))], 5)
    if case == "nested_full":
        load(connection, "hold_l", "k int", [(i,) for i in range(121)])
        load(connection, "hold_r", "k int", [(i,) for i in range(121)])
        pairs = sum(range(121))
        return ("SELECT count(*), count(l.k), count(r.k) FROM hold_l l FULL JOIN hold_r r ON l.k < r.k",
                [pairs + 2, pairs + 1, pairs + 1], 5)
    load(connection, "hold_l", "k int", [(i * 11,) for i in range(800)])
    load(connection, "hold_r", "k int, v int", [(i, i + 7) for i in range(6000)])
    connection.query("CREATE INDEX hold_r_key ON hold_r(k)")
    source = ("LEFT JOIN LATERAL (SELECT v FROM hold_r WHERE k = l.k LIMIT 1) r ON true"
              if case == "lateral_index" else "LEFT JOIN hold_r r ON l.k = r.k")
    return (f"SELECT count(*), count(r.v), sum(r.v) FROM hold_l l {source}",
            [800, 546, sum(i * 11 + 7 for i in range(546))], 7)


def benchmark(case, args):
    server = MEMORY["OwnedLisp"]()
    try:
        server.start()
        with contextlib.closing(MEMORY["connect"](server)) as connection:
            connection.socket.settimeout(600)
            if case.endswith("_scan"):
                query, result, default_repeats = prepare_scan(connection, case)
            elif case.startswith("typed_"):
                query, result, default_repeats = prepare_typed_join(connection, case)
            elif case in ("nested_cross", "nested_full", "lateral_index", "parameterized_index"):
                query, result, default_repeats = prepare_loop_join(connection, case)
            else:
                query, result, default_repeats = prepare_join(connection, case)
            if args.control_prelude:
                server.control(args.control_prelude.read_text())
            try:
                connection.query("SET work_mem = '64kB'")
                work_mem = connection.query("SHOW work_mem")[0][0]
            except WireError as error:
                if error.sqlstate not in ("42704", "0A000"):
                    raise
                work_mem = None
            expected = [list(map(str, result))]
            repeats = args.repeats or default_repeats

            def run(count):
                for _ in range(count):
                    actual = connection.query(query)
                    assert actual == expected, (case, actual, expected)

            run(1)
            plan = connection.query("EXPLAIN " + query)
            for sample in range(args.samples):
                measurements = MEMORY["measure"](server, lambda: run(repeats), None)
                print(json.dumps(dict(
                    case=case, checkout=str(args.engine_root), sample=sample,
                    repeats=repeats, work_mem=work_mem, plan=plan, **measurements
                )), flush=True)
    finally:
        server.stop()
        shutil.rmtree(server.root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-root", type=Path, default=HERE.parents[2])
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--control-prelude", type=Path)
    parser.add_argument("cases", nargs="*", metavar="CASE", help=", ".join(CASES))
    args = parser.parse_args()
    for case in args.cases:
        if case not in CASES:
            parser.error(f"unknown case {case!r}")
    if args.samples <= 0 or (args.repeats is not None and args.repeats <= 0):
        parser.error("sample and repeat counts must be positive")
    # The shared fixture binds its launcher and control client to its module's
    # checkout. Select the engine for both paths before opening any server.
    MEMORY["OwnedLisp"].start.__globals__["LISP"] = args.engine_root.resolve() / "src/lisp"
    for case in args.cases or CASES[:6]:
        benchmark(case, args)


if __name__ == "__main__":
    main()

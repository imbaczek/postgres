"""Compare planner row estimates and results on controlled distributions."""

import argparse
import json
import runpy
from pathlib import Path


FIXTURE = runpy.run_path(str(Path(__file__).with_name("hash-join-fixed-width.py")))


def evaluated_plan(connection, query, expected):
    plan = json.loads(connection.query("EXPLAIN (ANALYZE, FORMAT JSON) " + query)[0][0])[0]["Plan"]
    while plan["Node Type"] == "Project":
        plan = plan["Plans"][0]
    assert plan["Actual Rows"] == expected, plan
    return plan


def plan_estimate(connection, query, expected, verify_cost=False):
    plan = evaluated_plan(connection, query, expected)
    if verify_cost:
        assert plan["Node Type"] == "Filter", plan
        child = plan["Plans"][0]
        assert abs(plan["Total Cost"] - child["Total Cost"] - 0.75) <= 0.02, plan
    node = plan
    while node["Node Type"] == "Filter":
        node = node["Plans"][0]
    return plan["Plan Rows"], node["Node Type"]


def verify_integer_distribution(connection, engine):
    connection.query("CREATE TABLE stats_skew (k integer, tag integer, padding text)")
    connection.query("ALTER TABLE stats_skew ALTER COLUMN padding SET STORAGE PLAIN")
    keys = ["0"] * 240 + ["NULL"] * 20 + [str(value) for value in range(1000, 1040)]
    pad = "x" * 1800
    for offset in range(0, len(keys), 50):
        rows = ",".join(f"({key},{i % 2},'{pad}')" for i, key in enumerate(keys[offset:offset + 50], offset))
        connection.query("INSERT INTO stats_skew VALUES " + rows)
    connection.query("ANALYZE stats_skew")
    for indexed in (False, True):
        if indexed:
            connection.query("CREATE INDEX stats_skew_idx ON stats_skew(k)")
            if engine == "c":
                connection.query("SET enable_seqscan = off")
        for predicate, expected in (("k > 500", 40), ("500 < k", 40), ("k <= 500", 240),
                                    ("k IS NULL", 20), ("k IS NOT NULL", 280),
                                    ("k < 1000", 240), ("k <= 1000", 241),
                                    ("k < 2147483648", 280),
                                    ("k >= 1005 AND k < 1025", 20),
                                    ("k >= 1005 AND k < 1025 AND k IS NOT NULL", 20),
                                    ("k >= 1005 AND k < 1025 AND tag = 1", 10),
                                    ("k > 1003 AND k > 1010", 29),
                                    ("k > 1038 AND k < 1040", 1),
                                    ("k > 1038 AND k < 1040 AND tag = 1", 1),
                                    ("k = 0 AND k > 500", 0),
                                    ("k = 0 AND k <= 500", 240),
                                    ("k > 1040 AND k < 1038", 0)):
            query = f"SELECT k FROM stats_skew WHERE {predicate}"
            estimate, node = plan_estimate(connection, query, expected,
                                           verify_cost=engine == "lisp" and not indexed and predicate == "k > 500")
            if predicate.startswith("k = 0 AND"):
                if engine == "lisp":
                    assert estimate == max(1, expected), (indexed, predicate, estimate)
            else:
                assert abs(estimate - expected) <= 2, (engine, indexed, predicate, estimate, expected)
            if indexed and predicate.startswith("k > 1038 AND k < 1040"):
                assert "Index" in node, (engine, node)
            print(json.dumps(dict(engine=engine, index_available=indexed, node=node, predicate=predicate,
                                  estimated=estimate, actual=expected)), flush=True)
    if engine == "c":
        connection.query("RESET enable_seqscan")


def seed_typed_join(connection, kind, copies=1, analyze_right=True):
    if kind == "numeric":
        left_values, right_values = ("1.0", "2.0", "3.0"), ("1.000", "2.000", "4.000")
    else:
        left_values, right_values = ("'red'", "'blue'", "'green'"), ("'red'", "'blue'", "'black'")
    for table, values, counts in (
        ("stats_left", left_values, (120, 80, 80)),
        ("stats_right", right_values, (30, 40, 210)),
    ):
        connection.query(f"CREATE TABLE {table} (k {kind})")
        rows = ["NULL"] * 20 + [value for value, count in zip(values, counts) for _ in range(count)]
        for _ in range(copies):
            connection.query(f"INSERT INTO {table} VALUES " + ",".join(f"({key})" for key in rows))
        if table == "stats_left" or analyze_right:
            connection.query(f"ANALYZE {table}")


JOIN_QUERY = "SELECT l.k FROM stats_left l JOIN stats_right r ON l.k = r.k"


def verify_join_distribution(connection, engine):
    for kind in ("varchar(16)", "numeric"):
        seed_typed_join(connection, kind)
        estimate, node = plan_estimate(connection, JOIN_QUERY, 6800)
        assert "Join" in node, (engine, kind, node)
        assert abs(estimate - 6800) <= 2, (engine, kind, estimate)
        print(json.dumps(dict(engine=engine, key_type=kind, node=node, estimated=estimate, actual=6800)),
              flush=True)
        connection.query("DROP TABLE stats_left, stats_right")
    seed_typed_join(connection, "varchar(16)", analyze_right=False)
    plan = evaluated_plan(connection, JOIN_QUERY, 6800)
    estimate, node = plan["Plan Rows"], plan["Node Type"]
    assert "Join" in node and 1 <= estimate < 6800, (engine, node, estimate)
    if engine == "lisp":
        left_rows, right_rows = (child["Plan Rows"] for child in plan["Plans"])
        assert estimate == round(left_rows * right_rows * (280 / 300) / 200), plan
    print(json.dumps(dict(engine=engine, stats="left only", node=node, estimated=estimate, actual=6800)),
          flush=True)
    connection.query("DROP TABLE stats_left, stats_right")


def verify_fractional_distribution(connection, engine):
    for kind in ("float4", "float8", "numeric"):
        connection.query(f"CREATE TABLE stats_float (k {kind}, padding text)")
        connection.query("ALTER TABLE stats_float ALTER COLUMN padding SET STORAGE PLAIN")
        keys = ["'0.0'"] * 122 + ["'-0.0'"] * 115 + ["'NaN'", "'Infinity'", "'-Infinity'"] + ["NULL"] * 20
        keys.extend(f"'{value}.25'" for value in range(1, 41))
        padding = "x" * 1800
        for offset in range(0, len(keys), 50):
            rows = ",".join(f"({key},'{padding}')" for key in keys[offset:offset + 50])
            connection.query("INSERT INTO stats_float VALUES " + rows)
        connection.query("ANALYZE stats_float")
        for predicate, expected in (("k > '11.5' AND k <= '21.5'", 10), ("k > '100.0'", 2),
                                    ("k < '1.5'", 239), ("k > 'NaN'", 0),
                                    ("k > 'Infinity'", 1), ("k < '-Infinity'", 0),
                                    *([("k > 100", 2)] if kind == "numeric" else [])):
            estimate, node = plan_estimate(connection, f"SELECT k FROM stats_float WHERE {predicate}", expected)
            assert abs(estimate - expected) <= 2, (engine, kind, predicate, estimate)
            print(json.dumps(dict(engine=engine, key_type=kind, node=node, predicate=predicate,
                                  estimated=estimate, actual=expected)), flush=True)
        connection.query("CREATE INDEX stats_float_idx ON stats_float(k)")
        predicate = "k > '39.5' AND k < '40.5'"
        estimate, node = plan_estimate(connection, f"SELECT k FROM stats_float WHERE {predicate}", 1)
        assert 1 <= estimate <= 2 and "Index" in node, (engine, kind, node, estimate)
        print(json.dumps(dict(engine=engine, key_type=kind, node=node, predicate=predicate,
                              estimated=estimate, actual=1)), flush=True)
        connection.query("DROP TABLE stats_float")


def measure_analyze(server, connection, sample_kind="integer"):
    key_types = {
        "integer": ("int", "int", "bigint", "int"),
        "float": ("float4", "float8", "float8", "float4"),
        "numeric": ("numeric",) * 4,
    }[sample_kind]
    connection.query(
        "CREATE TABLE measured_analyze (" +
        ",".join(f"{column} {kind}" for column, kind in zip(("k", "skew", "distinct_key", "nullable"), key_types))
        + ")"
    )
    for start in range(0, 36000, 500):
        rows = ",".join(
            f"({i % 47},{0 if i % 5 else 500 + i % 97},"
            f"{2**62 + i if sample_kind == 'integer' else i / 7},{'NULL' if i % 5 else i % 179})"
            for i in range(start, start + 500)
        )
        connection.query("INSERT INTO measured_analyze VALUES " + rows)

    def analyze_batch():
        for _ in range(15):
            connection.query("ANALYZE measured_analyze")

    analyze_batch()
    for _ in range(3):
        print(json.dumps(FIXTURE["MEMORY"]["measure"](server, analyze_batch, None)), flush=True)


def seed_measured_ranges(connection):
    connection.query("CREATE TABLE measured_range (k integer, payload integer)")
    for start in range(0, 4000, 500):
        rows = ",".join(f"({i if i % 9 else 0},{i})" for i in range(start, start + 500))
        connection.query("INSERT INTO measured_range VALUES " + rows)
    connection.query("ANALYZE measured_range")
    connection.query("CREATE INDEX measured_range_idx ON measured_range(k)")


def measure_range_planning(server, connection):
    seed_measured_ranges(connection)
    queries = (
        "EXPLAIN (FORMAT JSON) SELECT payload FROM measured_range WHERE k >= 1024 AND k < 1048",
        "EXPLAIN (FORMAT JSON) SELECT payload FROM measured_range WHERE k > 10 AND k > 100",
    )

    def batch():
        for _ in range(300):
            for query in queries:
                if not connection.query(query):
                    raise AssertionError(query)

    batch()
    for _ in range(3):
        print(json.dumps(FIXTURE["MEMORY"]["measure"](server, batch, None)), flush=True)


def measure_join_planning(server, connection):
    seed_typed_join(connection, "numeric", copies=12)

    def batch():
        for _ in range(600):
            if not connection.query("EXPLAIN (FORMAT JSON) " + JOIN_QUERY):
                raise AssertionError(JOIN_QUERY)

    batch()
    for _ in range(3):
        print(json.dumps(FIXTURE["MEMORY"]["measure"](server, batch, None)), flush=True)


def measure_execution(server, connection):
    seed_measured_ranges(connection)
    seed_typed_join(connection, "numeric")
    narrow = [i for i in range(1024, 1048) if i % 9]
    cases = (
        ("narrow_range", "SELECT count(*) FROM measured_range WHERE k >= 1024 AND k < 1048",
         len(narrow), 200),
        ("range_residual",
         "SELECT count(*) FROM measured_range WHERE k >= 1024 AND k < 1048 AND payload % 3 = 0",
         sum(i % 3 == 0 for i in narrow), 200),
        ("numeric_join", JOIN_QUERY.replace("SELECT l.k", "SELECT count(*)"), 6800, 30),
    )
    for case, query, expected, repeats in cases:
        plan = json.loads(connection.query("EXPLAIN (FORMAT JSON) " + query)[0][0])[0]["Plan"]
        nodes = []
        todo = [plan]
        while todo:
            node = todo.pop()
            nodes.append(node["Node Type"])
            todo.extend(reversed(node.get("Plans", [])))

        def batch():
            for _ in range(repeats):
                if connection.query(query) != [[str(expected)]]:
                    raise AssertionError(case)

        batch()
        for _ in range(3):
            result = FIXTURE["MEMORY"]["measure"](server, batch, None)
            print(json.dumps(dict(case=case, repeats=repeats, nodes=nodes, **result)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-bin", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--analyze-memory", action="store_true")
    parser.add_argument("--sample-kind", choices=("integer", "float", "numeric"), default="integer")
    mode.add_argument("--range-planning-memory", action="store_true")
    mode.add_argument("--join-planning-memory", action="store_true")
    mode.add_argument("--execution-memory", action="store_true")
    parser.add_argument("--engine-root", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    FIXTURE["MEMORY"]["OwnedLisp"].start.__globals__["LISP"] = args.engine_root.resolve() / "src/lisp"
    if args.analyze_memory or args.range_planning_memory or args.join_planning_memory or args.execution_memory:
        with FIXTURE["lisp_connection"]() as (server, connection):
            if args.analyze_memory:
                measure_analyze(server, connection, args.sample_kind)
            elif args.range_planning_memory:
                measure_range_planning(server, connection)
            elif args.join_planning_memory:
                measure_join_planning(server, connection)
            else:
                measure_execution(server, connection)
        return
    if args.reference_bin is None:
        parser.error("--reference-bin is required for C comparisons")
    with FIXTURE["REFERENCE"](args.reference_bin) as reference:
        verify_integer_distribution(reference, "c")
        verify_join_distribution(reference, "c")
        verify_fractional_distribution(reference, "c")
    with FIXTURE["lisp_connection"]() as (_, connection):
        verify_integer_distribution(connection, "lisp")
        verify_join_distribution(connection, "lisp")
        verify_fractional_distribution(connection, "lisp")


if __name__ == "__main__":
    main()

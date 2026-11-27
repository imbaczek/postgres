"""Measure a hash join over SQL wire input and verify the output independently."""

import argparse
import contextlib
import json
import random
import runpy
import shutil
from collections import Counter
from pathlib import Path


HERE = Path(__file__).resolve().parent
memory = runpy.run_path(str(HERE / "workload-memory.py"))
wire = runpy.run_path(str(HERE / "hash-join.py"))


def workload_key(row, distribution):
    if distribution == "uniform":
        return row
    if distribution == "single":
        return 1
    # Map evenly distributed buckets with a cubic transform to concentrate
    # rows at the low end while retaining a long tail.
    bucket = ((row * 2654435761) & 0xFFFFFFFF) * 1000 // 2**32
    return bucket**3 // 1000000


def load_input(sql, table, count, distribution, payload=None, matching_keys=None):
    columns = "k int, payload text" if payload is not None else "k int"
    sql.query(f"CREATE TABLE {table} ({columns})")
    if payload is not None:
        sql.query(f"ALTER TABLE {table} ALTER COLUMN payload SET STORAGE EXTERNAL")
    suffix = f",'{payload}'" if payload is not None else ""
    batch_size = 32 if payload is not None else 1000
    counts = Counter()
    for start in range(0, count, batch_size):
        keys = [workload_key(row, distribution) for row in range(start, min(start + batch_size, count))]
        counts.update(key for key in keys if matching_keys is None or key in matching_keys)
        values = ",".join(f"({key}{suffix})" for key in keys)
        sql.query(f"INSERT INTO {table} VALUES " + values)
    sql.query(f"ANALYZE {table}")
    return counts


def verify_external_payload(server, payload_bytes):
    response = server.control(
        "(let ((pg::*backend* (pg::make-backend :database \"postgres\"))) "
        "  (format t \"~&WIDTH=~d~%\" "
        "    (pg::pg-table-statistics-average-tuple-bytes "
        "      (pg::pg-class-statistics (pg::lookup-table \"hash_build\")))))"
    )
    physical_width = int(response.split("WIDTH=", 1)[1].split()[0])
    assert physical_width < payload_bytes, physical_width
    return physical_width


def join_plan_metrics(sql, query, algorithm, matches):
    nodes, temp = wire["explain_plan_nodes"](sql, query)
    if algorithm == "hash":
        node = next(node for node in nodes if node["Node Type"] == "Hash Join")
        assert node["Actual Rows"] == matches, node
        if "Native Hash Key Probes" in node:
            assert 0 < node["Native Hash Key Probes"] or matches == 0, node
        else:
            assert node["Native Hash Candidates"] >= matches, node
        counters = {key: value for key, value in node.items() if key.startswith("Native Hash ")}
        return temp, dict(build_side=node["Build Side"], spill_read_bytes=node["Native Spill Read Bytes"],
                          spill_written_bytes=node["Native Spill Written Bytes"], **counters)
    node = next(node for node in nodes if node["Node Type"] == "Nested Loop")
    outer, inner = node["Plans"]
    assert (inner["Node Type"] == "Index Scan") == (algorithm == "parameterized"), node
    assert node["Actual Rows"] == matches, node
    return temp, dict(
        outer_rows=outer["Actual Rows"],
        inner_rows_per_loop=inner["Actual Rows"],
        inner_loops=inner["Actual Loops"],
        rejected_per_loop=node["Rows Removed by Join Filter"],
        spill_read_bytes=node["Native Spill Read Bytes"],
        spill_written_bytes=node["Native Spill Written Bytes"],
    )


def algorithms(server, sql, compare_loops):
    yield "hash"
    if compare_loops:
        # Force only the hash path off in this disposable server. Both loop
        # runs execute the same SQL through the ordinary planner and wire path.
        server.control(
            "(setf (symbol-function 'pg::select-two-table-hash-input) "
            "      (constantly nil))"
        )
        yield "nested"
        sql.query("CREATE INDEX hash_probe_key ON hash_probe(k)")
        sql.query("CREATE INDEX hash_build_key ON hash_build(k)")
        # Prefer the existing index candidate for the inner side even for
        # a small fixture where the cost model chooses a sequential scan.
        server.control(
            "(let ((candidates (symbol-function 'pg::relation-path-candidates))) "
            "  (setf (symbol-function 'pg::relation-path-candidates) "
            "        (lambda (prepared index joined) "
            "          (let ((paths (funcall candidates prepared index joined))) "
            "            (if (and joined (rest paths)) (rest paths) paths)))))"
        )
        yield "parameterized"


def benchmark(server, sql, args):
    payload = None
    if args.payload == "external":
        payload = "".join(random.Random(19).choices("0123456789abcdefghijklmnopqrstuvwxyz", k=6400))
    build = load_input(sql, "hash_build", args.build_rows, args.distribution, payload)
    probe = load_input(sql, "hash_probe", args.probe_rows, args.distribution, matching_keys=build)
    physical_width = verify_external_payload(server, len(payload)) if payload is not None else None
    matches = sum(probe[key] * count for key, count in build.items())
    expected = [str(matches)]
    extra = ", max(length(b.payload))" if payload is not None else ""
    if payload is not None:
        expected.append(str(len(payload)))
    query = f"SELECT count(*){extra} FROM hash_probe p JOIN hash_build b ON p.k = b.k"

    def run():
        result = sql.query(query)
        assert result == [expected], (result, expected)

    for algorithm in algorithms(server, sql, args.compare_loops):
        for work_mem in args.work_mem or ("4MB", "64kB"):
            sql.query(f"SET work_mem = '{work_mem}'")
            temp, counters = join_plan_metrics(sql, query, algorithm, matches)
            for sample in range(args.samples):
                result = memory["measure"](server, run, None)
                print(json.dumps(dict(
                    algorithm=algorithm, distribution=args.distribution, payload=args.payload,
                    right_physical_width=physical_width, probe_rows=args.probe_rows,
                    build_rows=args.build_rows, work_mem=work_mem, matches=matches,
                    temp_bytes=temp, sample=sample + 1, **counters, **result,
                )), flush=True)


def positive_integer(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe-rows", default=10000, type=positive_integer)
    parser.add_argument("--build-rows", type=positive_integer)
    parser.add_argument("--distribution", choices=("uniform", "skew", "single"), default="uniform")
    parser.add_argument("--payload", choices=("narrow", "external"), default="narrow")
    parser.add_argument("--work-mem", action="append", metavar="SIZE")
    parser.add_argument("--samples", default=1, type=positive_integer)
    parser.add_argument("--compare-loops", action="store_true", help="also measure both nested loop paths")
    args = parser.parse_args()
    if args.build_rows is None:
        args.build_rows = 96 if args.payload == "external" else 512
    server = memory["OwnedLisp"]()
    try:
        server.start()
        with contextlib.closing(memory["connect"](server)) as sql:
            sql.socket.settimeout(600)
            benchmark(server, sql, args)
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    main()

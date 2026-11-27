"""Measure sequential, equality-only, and bounded composite scans over the same rows."""

import argparse
import concurrent.futures
import contextlib
import json
import re
import runpy
import shutil
import time
from pathlib import Path


HERE = Path(__file__).resolve().parent
memory = runpy.run_path(str(HERE / "workload-memory.py"))
wire = runpy.run_path(str(HERE / "hash-join.py"))


def seed(connection, rows):
    connection.query("CREATE TABLE range_workload (tenant int, id int)")
    for start in range(0, rows, 1000):
        # One tenant owns half the heap. The other rows span 63 smaller tenants.
        tuples = ",".join(
            f"({0 if i % 2 == 0 else 1 + i // 2 % 63},{i})" for i in range(start, min(start + 1000, rows))
        )
        connection.query("INSERT INTO range_workload VALUES " + tuples)
    connection.query("ANALYZE range_workload")


def install_tid_counter(server):
    server.control(
        "(progn (defparameter pg::*range-benchmark-candidates* nil) "
        "       (defparameter pg::*range-benchmark-heap-pages* (make-hash-table)) "
        "       (let ((next (symbol-function 'pg::select-index-cursor-next-tid))) "
        "         (setf (symbol-function 'pg::select-index-cursor-next-tid) "
        "               (lambda (cursor) "
        "                 (let ((tid (funcall next cursor))) "
        "                   (when (and tid pg::*range-benchmark-candidates*) "
        "                     (incf pg::*range-benchmark-candidates*) "
        "                     (setf (gethash (car tid) pg::*range-benchmark-heap-pages*) t)) "
        "                   tid)))))"
    )


def install_index_choice(server):
    # Measure the index path even when the normal planner chooses Seq Scan.
    server.control(
        "(let ((choose (symbol-function 'pg::choose-cheapest-select-path))) "
        "  (defparameter pg::*range-benchmark-force-index* nil) "
        "  (setf (symbol-function 'pg::choose-cheapest-select-path) "
        "        (lambda (table paths &optional outer-rows) "
        "          (or (and pg::*range-benchmark-force-index* "
        "                   (find :index-scan paths :key #'pg::select-scan-path-kind)) "
        "              (funcall choose table paths outer-rows)))))"
    )


def heap_pages(server):
    reply = server.control(
        "(let* ((pg::*backend* (pg::make-backend :database \"postgres\")) "
        "       (table (pg::lookup-table \"range_workload\"))) "
        '  (format t "~&PAGES=~d~%" (pg::buf-nblocks (pg::pg-class-relfilenode table))))'
    )
    return int(re.search(r"PAGES=(\d+)", reply).group(1))


def workload(rows, basis_points, limit):
    count = rows * basis_points // 10000
    # Center the range on the middle of the heap and align both ends to tenant 0.
    lower = 2 * ((rows // 2 - count) // 2)
    upper = lower + 2 * count
    query = (
        f"SELECT id FROM range_workload WHERE tenant = 0 AND id >= {lower} AND id < {upper} "
        f"ORDER BY id DESC LIMIT {limit}"
    )
    expected = [[str(i)] for i in range(upper - 2, max(lower - 2, upper - 2 * (limit + 1)), -2)]
    return query, expected


def collect_lookup_work(server, connection, query, variant, table_pages):
    server.control("(progn (setf pg::*range-benchmark-candidates* 0) (clrhash pg::*range-benchmark-heap-pages*))")
    try:
        nodes, _ = wire["explain_plan_nodes"](connection, query)
        response = server.control(
            '(format t "~&WORK=~d ~d~%" pg::*range-benchmark-candidates* '
            '         (hash-table-count pg::*range-benchmark-heap-pages*))'
        )
        candidates, visited = (int(n) for n in re.search(r"WORK=(\d+) (\d+)", response).groups())
    finally:
        server.control("(setf pg::*range-benchmark-candidates* nil)")
    [scan] = [node for node in nodes if node["Node Type"] in ("Seq Scan", "Index Scan")]
    if variant == "sequential":
        assert scan["Node Type"] == "Seq Scan" and candidates == 0, scan
        visited = table_pages
    else:
        assert scan["Node Type"] == "Index Scan" and candidates > 0, scan
        assert (scan["Scan Direction"] == "Backward") == (variant == "bounded"), scan
    return dict(
        candidate_tids=candidates, heap_pages=visited, scan=scan["Node Type"],
        rows_removed=sum(node.get("Rows Removed by Filter", 0) for node in nodes),
        scanned_rows=scan["Actual Rows"],
    )


def run_matrix(server, connection, args, variant, table_pages):
    for points in args.selectivity or (1, 10, 100, 1000, 5000):
        for limit in args.limit or (10, 100, 10000):
            query, expected = workload(args.rows, points, limit)
            server.control("(setf pg::*range-benchmark-force-index* nil)")
            [[plan]] = connection.query("EXPLAIN " + query)
            natural_path = ("sequential" if "Seq Scan" in plan else
                            "ordered_index" if plan.startswith("Limit") else "index_with_sort")
            if variant != "sequential":
                server.control("(setf pg::*range-benchmark-force-index* t)")

            def execute():
                assert connection.query(query) == expected, (variant, points, limit)

            counters = collect_lookup_work(server, connection, query, variant, table_pages)
            if variant == "bounded":
                assert counters["candidate_tids"] == len(expected), (query, counters)
            measured = memory["measure"](server, execute, None)
            print(json.dumps(dict(
                variant=variant, rows=args.rows, selectivity_basis_points=points, limit=limit,
                returned=len(expected), natural_path=natural_path, **counters, **measured,
            )), flush=True)


def install_writer_probe(server, variant, reader_ready, writer_ready, release):
    reader = ("pg::materialize-select-tuple" if variant == "sequential"
              else "pg::select-index-cursor-next-tid")
    server.control(
        "(let* ((pg::*backend* (pg::make-backend :database \"postgres\")) "
        f"       (reader (symbol-function '{reader})) "
        "       (writer (symbol-function 'pg::call-with-btree-relation-write-lock)) "
        "       (index (pg::lookup-index \"range_workload_ix\")) "
        "       (target (and index (pg::pg-index-relfilenode index))) "
        "       (reader-armed t) (writer-armed t)) "
        "  (defparameter pg::*range-probe-reader* reader) "
        "  (defparameter pg::*range-probe-writer* writer) "
        "  (defparameter pg::*range-probe-wait-ms* nil) "
        "  (defparameter pg::*range-probe-release-tick* nil) "
        "  (defparameter pg::*range-probe-after-release-ms* nil) "
        f"  (setf (symbol-function '{reader}) "
        "        (lambda (&rest args) "
        "          (declare (dynamic-extent args)) "
        "          (when reader-armed "
        "            (setf reader-armed nil) "
        f"            (with-open-file (marker {json.dumps(str(reader_ready))} :direction :output) "
        '              (write-line "ready" marker)) '
        f"            (loop until (probe-file {json.dumps(str(release))}) "
        "                  do (pg::check-for-interrupts) (sleep 0.001)) "
        "            (setf pg::*range-probe-release-tick* (get-internal-real-time))) "
        "          (apply reader args)) "
        "        (symbol-function 'pg::call-with-btree-relation-write-lock) "
        "        (lambda (node callback) "
        "          (if (and writer-armed target (= node target)) "
        "              (progn "
        "                (setf writer-armed nil) "
        f"                (with-open-file (marker {json.dumps(str(writer_ready))} :direction :output) "
        '                  (write-line "ready" marker)) '
        "                (let ((start (get-internal-real-time))) "
        "                  (funcall writer node "
        "                           (lambda () "
        "                             (let ((acquired (get-internal-real-time))) "
        "                               (setf pg::*range-probe-wait-ms* "
        "                                     (/ (* 1000 (- acquired start)) internal-time-units-per-second) "
        "                                     pg::*range-probe-after-release-ms* "
        "                                     (and pg::*range-probe-release-tick* "
        "                                          (/ (* 1000 (- acquired pg::*range-probe-release-tick*)) "
        "                                             internal-time-units-per-second)))) "
        "                             (funcall callback))))) "
        "              (funcall writer node callback)))))"
    )
    return reader


def writer_probe(server, connection, rows, variant):
    points, limit = 5000, 10000
    query, expected = workload(rows, points, limit)
    [[plan]] = connection.query("EXPLAIN " + query)
    assert ("Seq Scan" in plan) == (variant == "sequential"), plan
    assert plan.startswith("Limit") == (variant == "bounded"), plan
    with contextlib.closing(memory["connect"](server)) as writer:
        writer.query("BEGIN")
        markers = tuple(server.root / f"range-{variant}-{kind}"
                        for kind in ("reader-ready", "writer-ready", "reader-release"))
        reader_ready, writer_ready, release = markers
        reader_hook = install_writer_probe(server, variant, *markers)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                read = pool.submit(connection.query, query)
                try:
                    wait_for_file = memory["fixture"]["wait_for_file"]
                    wait_for_file(reader_ready, server.process)
                    start = time.monotonic()
                    write = pool.submit(writer.query, "INSERT INTO range_workload VALUES (0,-2)")
                    if variant == "sequential":
                        write.result(timeout=10)
                        write_ms = 1000 * (time.monotonic() - start)
                        assert not read.done()
                    else:
                        wait_for_file(writer_ready, server.process)
                        assert not write.done()
                finally:
                    release.touch()
                write.result(timeout=120)
                if variant != "sequential":
                    write_ms = 1000 * (time.monotonic() - start)
                assert read.result(timeout=120) == expected
            response = server.control(
                '(format t "~&WAIT=~,3f ~,3f~%ACQUIRED-AFTER=~a~%" (or pg::*range-probe-wait-ms* 0) '
                '         (or pg::*range-probe-after-release-ms* 0) '
                '         (not (null pg::*range-probe-after-release-ms*)))'
            )
            assert ("\nACQUIRED-AFTER=T\n" in response) == (variant != "sequential"), response
            wait_ms, after_release_ms = map(float, re.search(r"WAIT=([\d.]+) ([\d.]+)", response).groups())
            print(json.dumps(dict(phase="writer_probe", variant=variant, rows=rows,
                                  selectivity_basis_points=points, limit=limit,
                                  writer_btree_wait_ms=wait_ms, writer_after_release_ms=after_release_ms,
                                  writer_statement_ms=write_ms)), flush=True)
        finally:
            server.control(
                f"(setf (symbol-function '{reader_hook}) pg::*range-probe-reader* "
                "      (symbol-function 'pg::call-with-btree-relation-write-lock) pg::*range-probe-writer*)"
            )
        writer.query("ROLLBACK")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--selectivity", action="append", type=int, choices=(1, 10, 100, 1000, 5000),
                        help="basis points of the whole table")
    parser.add_argument("--limit", action="append", type=int, choices=(10, 100, 10000))
    parser.add_argument("--writer-only", action="store_true", help="measure an aligned same-table writer overlap")
    args = parser.parse_args()
    if args.rows < 10000 or args.rows % 10000:
        parser.error("rows must be a positive multiple of 10,000")
    server = memory["OwnedLisp"]()
    try:
        server.start()
        with contextlib.closing(memory["connect"](server)) as connection:
            seed(connection, args.rows)
            pages = heap_pages(server)
            install_tid_counter(server)
            install_index_choice(server)
            for variant, columns in (("sequential", None), ("equality", "tenant"), ("bounded", "tenant, id")):
                if columns:
                    connection.query(f"CREATE INDEX range_workload_ix ON range_workload({columns})")
                if args.writer_only:
                    if columns:
                        server.control("(setf pg::*range-benchmark-force-index* t)")
                    writer_probe(server, connection, args.rows, variant)
                else:
                    run_matrix(server, connection, args, variant, pages)
                if columns:
                    connection.query("DROP INDEX range_workload_ix")
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    main()

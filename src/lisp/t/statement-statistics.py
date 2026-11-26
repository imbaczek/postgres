"""Exercise pg_stat_statements over the simple and extended SQL protocols."""

import concurrent.futures
import json
import runpy
import shutil
import socket
import struct
import time
from pathlib import Path


fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
OwnedLisp, Connection = fixture["OwnedLisp"], fixture["Connection"]


def receive_until(connection, terminal):
    kinds = []
    while True:
        kind, body = connection.receive()
        if kind == b"E":
            raise AssertionError(body)
        kinds.append(kind)
        if kind == terminal:
            return kinds


def report(connection, query):
    connection.query("SET pg_stat_statements.track TO 'none'")
    try:
        return connection.query(query)
    finally:
        connection.query("RESET pg_stat_statements.track")


def statistics_wire_fields(connection, source):
    connection.send(b"Q", f"SELECT * FROM {source} LIMIT 0".encode() + bytes(1))
    kind, body = connection.receive()
    assert kind == b"T", (kind, body)
    fields = []
    offset = 2
    for _ in range(struct.unpack_from("!H", body)[0]):
        end = body.index(0, offset)
        _, _, oid, _, typmod, _ = struct.unpack_from("!IhIhih", body, end + 1)
        assert typmod == -1, (source, body[offset:end], typmod)
        fields.append((body[offset:end].decode(), oid))
        offset = end + 1 + struct.calcsize("!IhIhih")
    assert offset == len(body), (source, offset, len(body))
    receive_until(connection, b"Z")
    return fields


def verify_statistics_descriptors(connection):
    # RowDescription names and type OIDs captured with C pg_stat_statements 1.13.
    statement_fields = [
        ("userid", 26), ("dbid", 26), ("toplevel", 16),
        ("queryid", 20), ("query", 25), ("plans", 20),
        ("total_plan_time", 701), ("min_plan_time", 701), ("max_plan_time", 701),
        ("mean_plan_time", 701), ("stddev_plan_time", 701), ("calls", 20),
        ("total_exec_time", 701), ("min_exec_time", 701), ("max_exec_time", 701),
        ("mean_exec_time", 701), ("stddev_exec_time", 701), ("rows", 20),
        ("shared_blks_hit", 20), ("shared_blks_read", 20), ("shared_blks_dirtied", 20),
        ("shared_blks_written", 20), ("local_blks_hit", 20), ("local_blks_read", 20),
        ("local_blks_dirtied", 20), ("local_blks_written", 20), ("temp_blks_read", 20),
        ("temp_blks_written", 20), ("shared_blk_read_time", 701), ("shared_blk_write_time", 701),
        ("local_blk_read_time", 701), ("local_blk_write_time", 701), ("temp_blk_read_time", 701),
        ("temp_blk_write_time", 701), ("wal_records", 20), ("wal_fpi", 20),
        ("wal_bytes", 1700), ("wal_buffers_full", 20), ("jit_functions", 20),
        ("jit_generation_time", 701), ("jit_inlining_count", 20), ("jit_inlining_time", 701),
        ("jit_optimization_count", 20), ("jit_optimization_time", 701), ("jit_emission_count", 20),
        ("jit_emission_time", 701), ("jit_deform_count", 20), ("jit_deform_time", 701),
        ("parallel_workers_to_launch", 20), ("parallel_workers_launched", 20), ("generic_plan_calls", 20),
        ("custom_plan_calls", 20), ("stats_since", 1184), ("minmax_stats_since", 1184),
    ]
    for source in ("pg_stat_statements", "pg_stat_statements(false)"):
        assert statistics_wire_fields(connection, source) == statement_fields, source
    assert statistics_wire_fields(connection, "pg_stat_statements_info") == [
        ("dealloc", 20), ("stats_reset", 1184),
    ]


def verify_simple_select(connection):
    verify_statistics_descriptors(connection)
    report(connection, "SELECT pg_stat_statements_reset()")
    assert connection.query("SELECT 7") == [["7"]]
    assert connection.query("SELECT 9") == [["9"]]
    assert counters(connection) == [["2", "2", "2"]]
    [[queryid]] = report(connection, "SELECT queryid FROM pg_stat_statements")
    [[plan]] = connection.query("EXPLAIN (FORMAT JSON) SELECT 11")
    identity = json.loads(plan)[0]
    assert identity["Query Identifier"] == int(queryid)
    [[second]] = connection.query("EXPLAIN (FORMAT JSON) SELECT 12")
    assert json.loads(second)[0]["Plan Identifier"] == identity["Plan Identifier"]
    assert counters(connection) == [["2", "2", "2"]]
    assert report(connection, "SELECT query FROM pg_stat_statements ORDER BY query") == [["select $1"]]
    assert report(
        connection, "SELECT queryid IS NOT NULL, query IS NULL, calls FROM pg_stat_statements(false)"
    ) == [["t", "t", "2"]]
    assert report(connection, "SELECT query FROM pg_stat_statements(true)") == [["select $1"]]
    assert report(connection, "SELECT query FROM pg_stat_statements(NULL::boolean)") == []
    assert report(
        connection,
        "SELECT shared_blks_read IS NULL, wal_bytes IS NULL, "
        "stats_since = minmax_stats_since FROM pg_stat_statements",
    ) == [["t", "t", "t"]]
    assert report(connection,
        "SELECT relation_reads, spill_written_bytes, wal_records "
        "FROM pg_stat_statements_resources ORDER BY queryid"
    ) == [["0", "0", "0"]]


def verify_nested_perform(connection):
    source = "DO $$ BEGIN PERFORM 7; PERFORM 9; END $$"
    report(connection, "SELECT pg_stat_statements_reset()")
    assert connection.query(source) == []
    assert counters(connection) == []
    connection.query("SET pg_stat_statements.track TO 'all'")
    try:
        assert connection.query(source) == []
        connection.send(b"P", bytes(1) + source.encode() + bytes(3))
        connection.send(b"B", bytes(8))
        connection.send(b"E", bytes(5))
        connection.send(b"S", b"")
        assert receive_until(connection, b"Z").count(b"C") == 1
        assert connection.query("SELECT 11") == [["11"]]
        assert report(
            connection, "SELECT toplevel, plans, calls, rows FROM pg_stat_statements ORDER BY toplevel"
        ) == [["f", "4", "4", "4"], ["t", "1", "1", "1"]]
        assert report(
            connection, "SELECT lower(query) FROM pg_stat_statements WHERE NOT toplevel"
        ) == [["select $1"]]
    finally:
        connection.query("RESET pg_stat_statements.track")


def verify_planning_setting(connection):
    assert connection.query("SHOW pg_stat_statements.track_planning") == [["off"]]
    report(connection, "SELECT pg_stat_statements_reset()")
    assert connection.query("SELECT 7") == [["7"]]
    assert counters(connection) == [["0", "1", "1"]]
    connection.query("SET pg_stat_statements.track_planning = on")
    assert connection.query("SELECT 9") == [["9"]]
    assert counters(connection) == [["1", "2", "2"]]
    assert report(
        connection,
        "SELECT enumvals FROM pg_settings WHERE name = 'pg_stat_statements.track'",
    ) == [["{none,top,all}"]]


def counters(connection):
    return report(connection,
        "SELECT plans, calls, rows FROM pg_stat_statements ORDER BY plans, calls, rows"
    )


def verify_runtime_stats(connection, server):
    query = "SELECT gc_cpu_time_ms, allocated_bytes, dynamic_heap_bytes FROM pg_lisp_runtime_stats"
    before, = connection.query(query)
    server.control("(sb-ext:gc :full t)")
    after, = connection.query(query)
    assert float(after[0]) >= float(before[0]) >= 0
    assert int(after[1]) > int(before[1]) > 0
    assert int(before[2]) > 0 and int(after[2]) > 0
    assert connection.query("SELECT build_revision FROM pg_lisp_runtime_stats") == [
        ["observability-wire-fixture"]
    ]


def verify_statistics_role_visibility(server, owner):
    owner.query("CREATE ROLE observability_reader LOGIN")
    report(owner, "SELECT pg_stat_statements_reset()")
    owner.query("SELECT 1")
    # Ordinary non-superuser SQL admission is outside the supported scope.
    # Exercise the same catalog adapters with a backend owned by that role.
    server.control(f'''(let* ((pg::*backend* (pg::make-backend :startup-user "observability_reader"))
                                (owner (pg::lookup-backend {owner.backend_pid}))
                                (row (first (pg::sql-statement-statistics-rows)))
                                (counters (first (pg::sql-statement-statistics-rows nil)))
                                (activity (pg::sql-activity-view-row owner "observability_reader" nil)))
                           (assert (eq (aref row 3) pg::+null+))
                           (assert (string= (aref row 4) "<insufficient privilege>"))
                           (assert (eq (aref counters 3) pg::+null+))
                           (assert (eq (aref counters 4) pg::+null+))
                           (assert (eq (aref activity 11) pg::+null+))
                           (assert (string= (aref activity 12) "<insufficient privilege>"))
                           (assert (equal "42501"
                                          (handler-case (pg::sql-statement-statistics-reset)
                                            (pg::pg-error (e) (pg::pg-error-sqlstate e)))))
                           (assert (equal "42501"
                                          (handler-case
                                              (pg::session-set-extra-setting
                                               "pg_stat_statements.track" "none")
                                            (pg::pg-error (e) (pg::pg-error-sqlstate e))))))''')


def verify_index_recheck(connection):
    assert connection.observe("EXPLAIN (unknown_option) SELECT 1") == ("error", "42601")
    assert connection.observe("EXPLAIN (memory) SELECT 1") == ("error", "0A000")
    [[raw]] = connection.query("EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF) SELECT 1")
    timed = json.loads(raw)[0]["Plan"]
    assert "Actual Startup Time" in timed and "Actual Total Time" in timed, timed
    assert (timed["Actual Rows"], timed["Actual Loops"]) == (1, 1), timed
    assert all("cost=" not in line for [line] in connection.query("EXPLAIN SELECT 1"))
    rows = connection.query("EXPLAIN (ANALYZE, TIMING OFF, BUFFERS ON) SELECT 1")
    assert all("actual time=" not in line for [line] in rows), rows
    assert any("rows=1" in line and "loops=1" in line for [line] in rows), rows
    assert any("Native Buffers:" in line for [line] in rows), rows
    connection.query("CREATE TABLE observed_index (x int, y int, payload text)")
    payload = "x" * 128
    for start in range(0, 2048, 128):
        values = ", ".join(
            f"({value}, {value}, '{payload}')" for value in range(start, start + 128)
        )
        connection.query("INSERT INTO observed_index VALUES " + values)
    connection.query("ANALYZE observed_index")
    result = connection.query("EXPLAIN (FORMAT JSON) SELECT x FROM observed_index")
    scan = json.loads(result[0][0])[0]["Plan"]
    while scan["Node Type"] != "Seq Scan":
        scan, = scan["Plans"]
    assert scan["Plan Rows"] == 2048 and scan["Total Cost"] > 20, scan
    assert scan["Startup Cost"] == 0
    assert "Actual Rows" not in scan, scan
    result = connection.query("EXPLAIN (FORMAT JSON, COSTS OFF) SELECT x FROM observed_index")
    scan = json.loads(result[0][0])[0]["Plan"]
    while scan["Node Type"] != "Seq Scan":
        scan, = scan["Plans"]
    assert all(key not in scan for key in ("Plan Rows", "Startup Cost", "Total Cost")), scan
    [[plan]] = connection.query(
        "EXPLAIN (FORMAT JSON) SELECT x FROM observed_index ORDER BY y LIMIT 3"
    )
    top_n = json.loads(plan)[0]["Plan"]
    assert (top_n["Plan Rows"], top_n["Startup Cost"], top_n["Total Cost"]) == (3, 90.95, 90.96), top_n
    [[plan]] = connection.query(
        "EXPLAIN (FORMAT JSON) SELECT x FROM observed_index WHERE x = 15 ORDER BY y"
    )
    filtered_sort = json.loads(plan)[0]["Plan"]
    assert filtered_sort["Plan Rows"] == 1 and filtered_sort["Startup Cost"] >= 69.60, filtered_sort
    [[plan]] = connection.query("EXPLAIN (FORMAT JSON) SELECT 1 UNION ALL SELECT 2")
    append = json.loads(plan)[0]["Plan"]
    assert append["Plan Rows"] == 2 and append["Startup Cost"] == 0
    assert append["Total Cost"] == 0.03, append
    [[plan]] = connection.query(
        "EXPLAIN (FORMAT JSON) SELECT x FROM observed_index LIMIT 1024"
    )
    limit = json.loads(plan)[0]["Plan"]
    assert limit["Plan Rows"] == 1024 and limit["Startup Cost"] == 0, limit
    [child] = limit["Plans"]
    assert abs(limit["Total Cost"] * 2 - child["Total Cost"]) <= 0.02, limit
    [[plan]] = connection.query(
        "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) SELECT x FROM observed_index LIMIT 0"
    )
    empty = json.loads(plan)[0]["Plan"]
    assert (empty["Plan Rows"], empty["Actual Rows"], empty["Total Cost"]) == (1, 0, 0.03), empty
    assert empty["Plans"][0]["Actual Loops"] == 0, empty
    [[plan]] = connection.query(
        "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) "
        "SELECT x FROM observed_index ORDER BY y LIMIT 0"
    )
    empty = json.loads(plan)[0]["Plan"]
    assert (empty["Plan Rows"], empty["Actual Rows"]) == (1, 0), empty
    assert empty["Plans"][0]["Actual Loops"] == 0, empty
    [[plan]] = connection.query(
        "EXPLAIN (FORMAT JSON) (SELECT payload || '' FROM observed_index ORDER BY x LIMIT 1) "
        "UNION ALL SELECT 'tail'::text"
    )
    partial = json.loads(plan)[0]["Plan"]
    assert partial["Plan Rows"] == 2 and "Total Cost" not in partial, partial
    text = connection.query("EXPLAIN (COSTS ON) SELECT x FROM observed_index")
    assert "cost=0.00.." in text[0][0] and "rows=2048" in text[0][0], text
    statement = "SELECT x FROM observed_index WHERE x = 15 AND y = 16"
    [[unindexed]] = connection.query("EXPLAIN (FORMAT JSON) " + statement)
    unindexed_identity = json.loads(unindexed)[0]
    connection.query("CREATE UNIQUE INDEX observed_index_x ON observed_index(x)")
    result = connection.query("EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) " + statement)
    indexed_identity = json.loads(result[0][0])[0]
    assert indexed_identity["Query Identifier"] == unindexed_identity["Query Identifier"]
    assert indexed_identity["Plan Identifier"] != unindexed_identity["Plan Identifier"]
    nodes = [indexed_identity["Plan"]]
    rechecks = []
    filters = []
    while nodes:
        node = nodes.pop()
        nodes.extend(node.get("Plans", ()))
        if node["Node Type"] == "Index Scan":
            assert node["Plan Rows"] == 1, node
            assert 0 < node["Startup Cost"] < node["Total Cost"], node
            rechecks.append(node["Rows Removed by Index Recheck"])
        if "Rows Removed by Filter" in node:
            filters.append(node["Rows Removed by Filter"])
    assert rechecks == [0] and filters == [1], (rechecks, filters)


def verify_resource_totals(connection):
    for query in (
        "SELECT count(*) FROM observed_index",
        "SELECT sum(x) FROM observed_index",
        "SELECT sum(x + y) FROM observed_index",
        "SELECT count(*) FROM observed_index WHERE y > x",
    ):
        [[estimate]] = connection.query("EXPLAIN (FORMAT JSON) " + query)
        plan = json.loads(estimate)[0]["Plan"]
        assert plan["Plan Rows"] == 1, plan
        if query in ("SELECT count(*) FROM observed_index", "SELECT sum(x) FROM observed_index"):
            assert (plan["Startup Cost"], plan["Total Cost"]) == (69.60, 69.61), plan
        else:
            assert "Total Cost" not in plan, plan
    report(connection, "SELECT pg_stat_statements_reset()")
    statement = "SELECT count(*) FROM observed_index"
    assert connection.query(statement) == [["2048"]]
    [[raw]] = connection.query(
        "EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS ON, TIMING OFF, COSTS OFF) " + statement
    )
    explanation = json.loads(raw)[0]
    queryid = explanation["Query Identifier"]
    [[accesses, writes, spill, wal]] = report(
        connection,
        "SELECT relation_reads + clean_cache_hits, relation_writes, "
        "spill_written_bytes, wal_records FROM pg_stat_statements_resources "
        f"WHERE queryid = {queryid}",
    )
    explained = explanation["Native Relation Reads"] + explanation["Native Clean Cache Hits"]
    assert int(accesses) == explained > 0
    assert [writes, spill, wal] == ["0", "0", "0"]


def verify_nested_loop_rescans(connection):
    connection.query("CREATE TABLE cross_left (x int)")
    connection.query("CREATE TABLE cross_right (x int)")
    connection.query("INSERT INTO cross_left VALUES (1), (2), (3)")
    connection.query("INSERT INTO cross_right VALUES (4), (5)")
    connection.query("ANALYZE cross_left")
    connection.query("ANALYZE cross_right")
    [[estimate]] = connection.query(
        "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) SELECT * FROM cross_left CROSS JOIN cross_right"
    )
    cross = json.loads(estimate)[0]["Plan"]
    assert (cross["Plan Rows"], cross["Actual Rows"]) == (6, 6), cross
    [loop] = cross["Plans"]
    outer, inner = loop["Plans"]
    replay_rows = (outer["Plan Rows"] - 1) * inner["Plan Rows"]
    expected = outer["Total Cost"] + inner["Total Cost"] + 0.01 * (6 + replay_rows)
    assert cross["Startup Cost"] == 0 and abs(cross["Total Cost"] - expected) < 0.005, cross
    connection.query("UPDATE cross_right SET x = 2 WHERE x = 4")
    connection.query("UPDATE cross_right SET x = NULL WHERE x = 5")
    connection.query("ANALYZE cross_right")
    equality_sql = "SELECT cross_left.x FROM cross_left JOIN cross_right ON cross_left.x = cross_right.x"
    explain = "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) "
    [[estimate]] = connection.query(explain + equality_sql)
    equality = json.loads(estimate)[0]["Plan"]
    assert (equality["Plan Rows"], equality["Actual Rows"]) == (1, 1), equality
    assert equality["Total Cost"] > cross["Total Cost"], equality
    connection.query("DELETE FROM cross_left")
    connection.query("DELETE FROM cross_right")
    connection.query("INSERT INTO cross_left VALUES (2), (2), (2), (9), (9), (NULL)")
    connection.query("INSERT INTO cross_right VALUES (2), (2), (3), (3), (3), (NULL)")
    connection.query("ANALYZE cross_left")
    connection.query("ANALYZE cross_right")
    [[estimate]] = connection.query(explain + equality_sql)
    skewed = json.loads(estimate)[0]["Plan"]
    assert (skewed["Plan Rows"], skewed["Actual Rows"]) == (6, 6), skewed
    connection.query("INSERT INTO cross_left VALUES (13), (14)")
    connection.query("INSERT INTO cross_right VALUES (16), (18)")
    connection.query("ANALYZE cross_left")
    connection.query("ANALYZE cross_right")
    [[estimate]] = connection.query(explain + equality_sql)
    residual = json.loads(estimate)[0]["Plan"]
    assert (residual["Plan Rows"], residual["Actual Rows"]) == (11, 6), residual
    for aggregate in ("count(*)", "sum(x)"):
        [[estimate]] = connection.query(explain + f"SELECT x, {aggregate} FROM cross_left GROUP BY x")
        grouped = json.loads(estimate)[0]["Plan"]
        assert (grouped["Plan Rows"], grouped["Actual Rows"]) == (5, 5), grouped
        if aggregate == "count(*)":
            assert (grouped["Startup Cost"], grouped["Total Cost"]) == (1.12, 1.17), grouped
        else:
            assert (grouped["Startup Cost"], grouped["Total Cost"]) == (1.21, 1.31), grouped
    statement = (
        "SELECT b.x FROM (SELECT 15 AS k UNION ALL SELECT 100000) a "
        "CROSS JOIN LATERAL (SELECT x FROM observed_index WHERE x + 0 = a.k LIMIT 1) b"
    )
    assert connection.query(statement) == [["15"]]
    [[raw]] = connection.query(
        "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF, COSTS OFF) " + statement
    )
    root = json.loads(raw)[0]["Plan"]
    assert root["Actual Rows"] == 1
    nodes = [root]
    rescans = []
    filter_counts = []
    while nodes:
        node = nodes.pop()
        nodes.extend(node.get("Plans", ()))
        assert "Actual Total Time" not in node and "Total Cost" not in node, node
        if node["Node Type"] == "Subquery Scan" and node.get("Alias") == "b":
            rescans.append((node["Actual Loops"], node["Actual Rows"]))
        if node["Node Type"] == "Filter":
            filter_counts.append(
                (node["Actual Loops"], node["Actual Rows"], node["Rows Removed by Filter"])
            )
    assert rescans == [(2, 0.5)], root
    # The first scan rejects 15 before its match. The second rejects all 2048.
    assert filter_counts == [(2, 0.5, 1032)], root


def verify_spill_resource_totals(server, connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    statement = "SELECT payload, x FROM observed_index ORDER BY y DESC, x"
    group_count = "SELECT x, count(*) FROM observed_index GROUP BY x"
    [[estimate]] = connection.query("EXPLAIN (FORMAT JSON) " + group_count)
    memory_group = json.loads(estimate)[0]["Plan"]
    [[estimate]] = connection.query("EXPLAIN (FORMAT JSON) " + statement)
    memory_sort = json.loads(estimate)[0]["Plan"]
    memory_startup = memory_sort["Startup Cost"]
    assert (memory_startup, memory_sort["Total Cost"]) == (177.12, 182.24), memory_sort
    for sql, method in ((statement, "in-memory stable sort"), (statement + " LIMIT 3", "top-N heap")):
        [[raw]] = connection.query(
            "EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS ON, TIMING OFF, COSTS OFF) " + sql
        )
        document = json.loads(raw)[0]
        assert document["Native Peak Temp Bytes"] == 0, document
        sort = document["Plan"]
        assert sort["Sort Method"] == method and sort["Sort Space Type"] == "Memory", sort
    plain = connection.query("EXPLAIN (COSTS OFF) " + statement)
    assert all("Sort Method" not in line for [line] in plain)
    analyzed = connection.query("EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) " + statement)
    assert any("Sort Method: in-memory stable sort" in line for [line] in analyzed)
    connection.query("CREATE PUBLICATION sort_method_publication FOR TABLE observed_index")
    [[raw]] = connection.query(
        "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF, COSTS OFF) "
        "SELECT (pg_get_publication_tables('sort_method_publication')).relid "
        "FROM observed_index WHERE x < 2 ORDER BY x"
    )
    project_set_sort = json.loads(raw)[0]["Plan"]
    assert project_set_sort["Actual Rows"] == 2, project_set_sort
    assert project_set_sort["Sort Method"] == "in-memory stable sort", project_set_sort
    server.control("(setf pg::*query-work-memory-bytes* 65536)")
    try:
        [[estimate]] = connection.query(
            "EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS ON, TIMING OFF) " + group_count
        )
        document = json.loads(estimate)[0]
        grouped = document["Plan"]
        assert (grouped["Plan Rows"], grouped["Actual Rows"]) == (2048, 2048), grouped
        assert grouped["Startup Cost"] > memory_group["Startup Cost"], grouped
        assert grouped["Total Cost"] > memory_group["Total Cost"], grouped
        assert document["Native Spill Written Bytes"] > 0, document
        [[estimate]] = connection.query("EXPLAIN (FORMAT JSON) " + statement)
        disk_sort = json.loads(estimate)[0]["Plan"]
        assert disk_sort["Total Cost"] > disk_sort["Startup Cost"] > memory_startup, disk_sort
        rows = connection.query(statement)
        assert [key for _, key in rows] == [str(key) for key in range(2047, -1, -1)]
        [[raw]] = connection.query(
            "EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS ON, TIMING OFF, COSTS OFF) " + statement
        )
        server.control("(setf pg::*query-work-memory-bytes* 8192)")
        [[merged]] = connection.query(
            "EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS ON, TIMING OFF, COSTS OFF) "
            "SELECT payload, x FROM observed_index ORDER BY y % 17, x"
        )
        merge = json.loads(merged)[0]
        assert merge["Plan"]["Sort Method"] == "external merge", merge
        assert 0 < merge["Native Peak Temp Bytes"] < merge["Native Spill Written Bytes"], merge
        # The bounded heap fits. The estimated full input does not.
        server.control("(setf pg::*query-work-memory-bytes* 786432)")
        [[bounded]] = connection.query("EXPLAIN (FORMAT JSON) " + statement + " LIMIT 1536")
        bounded_sort = json.loads(bounded)[0]["Plan"]
        assert bounded_sort["Plan Rows"] == 1536, bounded_sort
        assert memory_startup < bounded_sort["Startup Cost"] < disk_sort["Startup Cost"], bounded_sort
    finally:
        server.control("(setf pg::*query-work-memory-bytes* (* 4 1024 1024))")
    explanation = json.loads(raw)[0]
    queryid = explanation["Query Identifier"]
    [[reads, writes]] = report(
        connection,
        "SELECT spill_read_bytes, spill_written_bytes FROM pg_stat_statements_resources "
        f"WHERE queryid = {queryid}",
    )
    assert int(reads) == explanation["Native Spill Read Bytes"] > 0, (reads, explanation)
    assert int(writes) == explanation["Native Spill Written Bytes"] > 0, (writes, explanation)
    assert explanation["Native Peak Temp Bytes"] == int(writes), explanation
    sort = explanation["Plan"]
    assert sort["Node Type"] == "Sort"
    assert sort["Sort Method"] == "external run concatenation"
    assert sort["Sort Space Type"] == "Disk"
    assert (sort["Actual Rows"], sort["Actual Loops"]) == (2048, 1)
    assert "Actual Total Time" not in sort and "Total Cost" not in sort
    assert sort["Native Spill Read Bytes"] == int(reads)
    assert sort["Native Spill Written Bytes"] == int(writes)
    assert all(child["Native Spill Written Bytes"] == 0 for child in sort["Plans"])


def verify_set_operation(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    assert connection.query("SELECT 1 UNION ALL SELECT 1") == [["1"], ["1"]]
    assert connection.query("SELECT 2 UNION ALL SELECT 2") == [["2"], ["2"]]
    assert connection.query("SELECT 2 UNION SELECT 2") == [["2"]]
    assert counters(connection) == [["1", "1", "1"], ["2", "2", "4"]]


def verify_values_identity(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    assert connection.query("VALUES (1)") == [["1"]]
    assert connection.query("VALUES (2)") == [["2"]]
    assert connection.query("SELECT 1") == [["1"]]
    assert counters(connection) == [["1", "1", "1"], ["2", "2", "2"]]


def verify_structural_query_text(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    connection.query("SELECT 1, 2 WHERE false IS FALSE ORDER BY 1")
    connection.query("SELECT 1, 2 WHERE false IS FALSE ORDER BY 2")
    connection.query("SELECT 1 WHERE NULL IS NULL")
    assert report(connection, "SELECT query FROM pg_stat_statements ORDER BY query") == [
        ["select $1 , $2 where $3 is false order by 1"],
        ["select $1 , $2 where $3 is false order by 2"],
        ["select $1 where $2 is null"],
    ]


def verify_resolved_relation_identity(connection, c_peer=False):
    relation = "fingerprint_relation"
    connection.query(f"CREATE TABLE {relation} (x int, y int)")
    connection.query(f"INSERT INTO {relation} VALUES (1, 1)")
    report(connection, "SELECT pg_stat_statements_reset()")
    first = f"SELECT x FROM {relation} WHERE y = 1"
    assert connection.query(first) == [["1"]]
    assert connection.query(f"SELECT alias.x FROM {relation} alias WHERE alias.y = 2") == []
    assert connection.query(f"SELECT y FROM {relation} WHERE x = 1") == [["1"]]
    counts = counters(connection)
    expected = [["1", "1", "0"], ["1", "1", "1"], ["1", "1", "1"]] if c_peer else [
        ["1", "1", "1"], ["2", "2", "1"],
    ]
    assert counts == expected, counts
    connection.query(f"DROP TABLE {relation}")
    connection.query(f"CREATE TABLE {relation} (x int, y int)")
    report(connection, f"INSERT INTO {relation} VALUES (1, 1)")
    assert connection.query(first) == [["1"]]
    counts = counters(connection)
    expected = [["1", "1", "0"], ["1", "1", "1"], ["2", "2", "2"]] if c_peer else [
        ["1", "1", "1"], ["1", "1", "1"], ["2", "2", "1"],
    ]
    assert counts == expected, counts


def verify_expression_subquery(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    assert connection.query("SELECT EXISTS (SELECT 1)") == [["t"]]
    assert connection.query("SELECT EXISTS (SELECT 2)") == [["t"]]
    assert connection.query("SELECT EXISTS (SELECT 2::bigint)") == [["t"]]
    assert counters(connection) == [["1", "1", "1"], ["2", "2", "2"]]


def verify_cte_statistics(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    for value in (1, 2):
        result = connection.query(f"WITH q AS (SELECT {value} AS x) SELECT x FROM q")
        assert result == [[str(value)]]
    assert connection.query("WITH q AS MATERIALIZED (SELECT 3 AS x) SELECT x FROM q") == [["3"]]
    recursive = (
        "WITH RECURSIVE q(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM q WHERE x < 3) "
        "SELECT x FROM q ORDER BY x"
    )
    assert connection.query(recursive) == [["1"], ["2"], ["3"]]
    [[raw]] = connection.query("EXPLAIN (FORMAT JSON, COSTS OFF) " + recursive)
    explanation = json.loads(raw)[0]
    assert explanation["Query Identifier"] > 0 and explanation["Plan Identifier"] > 0
    assert counters(connection) == [["1", "1", "1"], ["1", "1", "3"], ["2", "2", "2"]]
    connection.query("CREATE TABLE observed_cte (x int)")
    report(connection, "SELECT pg_stat_statements_reset()")
    for value in (1, 2):
        assert connection.query(
            f"WITH q AS (SELECT {value} AS x) INSERT INTO observed_cte SELECT x FROM q RETURNING x"
        ) == [[str(value)]]
    assert counters(connection) == [["2", "2", "2"]]


def verify_array_and_case_statistics(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    for first, second in ((1, 2), (3, 4)):
        statement = (
            f"SELECT CASE WHEN ({first} < {second}) IS TRUE "
            f"THEN (ARRAY[{first}, {second}])[2] ELSE coalesce(NULL, {first}) END"
        )
        assert connection.query(statement) == [[str(second)]]
    assert connection.query("SELECT (ARRAY[1, 2])[1:2]") == [["{1,2}"]]
    assert connection.query("SELECT (ARRAY[1, 2])[:2]") == [["{1,2}"]]
    assert counters(connection) == [["1", "1", "1"], ["1", "1", "1"], ["2", "2", "2"]]


def verify_lateral_statistics(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    correlated = (
        "SELECT b.y FROM (SELECT {value} AS x) a "
        "CROSS JOIN LATERAL (SELECT {operand} + 1 AS y) b"
    )
    for value in (1, 2):
        statement = correlated.format(value=value, operand="a.x")
        assert connection.query(statement) == [[str(value + 1)]]
    assert connection.query(correlated.format(value=3, operand="3")) == [["4"]]
    assert counters(connection) == [["1", "1", "1"], ["2", "2", "2"]]


def verify_in_list_statistics(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    assert connection.query("SELECT 1 IN (1, 2)") == [["t"]]
    assert connection.query("SELECT 3 IN (4, 5)") == [["f"]]
    assert connection.query("SELECT 3 IN (4, 5, 6)") == [["f"]]
    assert connection.query("SELECT 3 NOT IN (4, 5)") == [["t"]]
    assert connection.query("SELECT 3 IN (4::bigint, 5)") == [["f"]]
    assert counters(connection) == [["1", "1", "1"], ["1", "1", "1"], ["3", "3", "3"]]


def verify_window_statistics(connection):
    for query, costs in (
        ("SELECT row_number() OVER () FROM observed_index", (0, 90.08)),
        ("SELECT rank() OVER (ORDER BY y) FROM observed_index", (177.12, 212.96)),
        ("SELECT sum(x) OVER () FROM observed_index", (None, None)),
    ):
        [[estimate]] = connection.query("EXPLAIN (FORMAT JSON) " + query)
        plan = json.loads(estimate)[0]["Plan"]
        assert plan["Plan Rows"] == 2048, plan
        assert (plan.get("Startup Cost"), plan.get("Total Cost")) == costs, plan
    report(connection, "SELECT pg_stat_statements_reset()")
    statement = "SELECT row_number() OVER (ORDER BY x {order}) FROM (SELECT {value} AS x) q"
    for value in (1, 2):
        assert connection.query(statement.format(order="ASC", value=value)) == [["1"]]
    assert connection.query(statement.format(order="DESC", value=3)) == [["1"]]
    assert counters(connection) == [["1", "1", "1"], ["2", "2", "2"]]


def verify_set_function_statistics(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    statement = "SELECT pid FROM pg_stat_get_activity({})"
    assert connection.query(statement.format(connection.backend_pid)) == [[str(connection.backend_pid)]]
    assert connection.query(statement.format(-1)) == []
    assert counters(connection) == [["2", "2", "1"]]


def verify_parameterized_dml(connection):
    connection.query("CREATE TABLE parameter_items (x int)")
    report(connection, "SELECT pg_stat_statements_reset()")
    source = b"INSERT INTO parameter_items VALUES ($1)"
    connection.send(b"P", b"parameter_insert\0" + source + b"\0" + struct.pack("!HI", 1, 23))
    for value in (b"7", b"9"):
        binding = b"\0parameter_insert\0" + struct.pack("!HHI", 0, 1, len(value)) + value + bytes(2)
        connection.send(b"B", binding)
        connection.send(b"E", bytes(5))
        connection.send(b"S", b"")
        assert receive_until(connection, b"Z").count(b"C") == 1
    assert counters(connection) == [["2", "2", "2"]]


def verify_dml_subqueries(connection):
    connection.query("CREATE TABLE observed_dml (x int)")
    connection.query("INSERT INTO observed_dml VALUES (1), (2), (3), (4)")
    report(connection, "SELECT pg_stat_statements_reset()")
    for key in (1, 2):
        connection.query(
            f"UPDATE observed_dml SET x = x + 10 FROM (SELECT {key} AS k) s WHERE x = s.k"
        )
    for key in (3, 4):
        connection.query(f"DELETE FROM observed_dml WHERE x = (SELECT {key})")
    assert counters(connection) == [["2", "2", "2"], ["2", "2", "2"]]
    assert report(connection, "SELECT x FROM observed_dml ORDER BY x") == [["11"], ["12"]]


def verify_execution_error(connection):
    connection.query("CREATE TABLE observed_errors (x int PRIMARY KEY)")
    connection.query("INSERT INTO observed_errors VALUES (1)")
    report(connection, "SELECT pg_stat_statements_reset()")
    assert connection.observe("INSERT INTO observed_errors VALUES (1)") == ("error", "23505")
    assert counters(connection) == [["1", "0", "0"]]
    assert report(
        connection, "SELECT execution_errors, execution_cancellations FROM pg_stat_statements_resources"
    ) == [["1", "0"]]


def verify_conflict_statistics(connection):
    connection.query("CREATE TABLE observed_conflict (x int PRIMARY KEY, y int)")
    connection.query("INSERT INTO observed_conflict VALUES (1, 0)")
    report(connection, "SELECT pg_stat_statements_reset()")
    for value in (10, 20):
        assert connection.query(
            f"INSERT INTO observed_conflict VALUES (1, {value}) "
            "ON CONFLICT (x) DO UPDATE SET y = excluded.y RETURNING y"
        ) == [[str(value)]]
    assert connection.query(
        "INSERT INTO observed_conflict VALUES (1, 30) ON CONFLICT (x) DO NOTHING RETURNING y"
    ) == []
    assert counters(connection) == [["1", "1", "0"], ["2", "2", "2"]]


def verify_targeted_reset(connection):
    report(connection, "SELECT pg_stat_statements_reset()")
    connection.query("SELECT 7")
    connection.query("SELECT 9::bigint")
    fields = "SELECT queryid, calls, min_exec_time, max_exec_time FROM pg_stat_statements"
    groups = report(connection, fields + " ORDER BY queryid")
    assert len(groups) == 2, groups
    targeted = int(groups[0][0])
    report(connection, f"SELECT pg_stat_statements_reset(queryid => {targeted}, minmax_only => true)")
    extrema = report(connection, fields + f" WHERE queryid = {targeted}")[0]
    assert extrema[1] == "1" and all(float(value) == 0 for value in extrema[2:])
    assert report(
        connection,
        "SELECT stats_since < minmax_stats_since FROM pg_stat_statements "
        f"WHERE queryid = {targeted}",
    ) == [["t"]]
    report(connection, f"SELECT pg_stat_statements_reset(0, 0, {targeted})")
    assert report(connection, fields + " ORDER BY queryid") == [groups[1]]


def verify_suspended_returning(connection, observer):
    connection.query("CREATE TABLE items (x int PRIMARY KEY, payload text DEFAULT 'candidate payload')")
    report(connection, "SELECT pg_stat_statements_reset()")
    source = b"INSERT INTO items VALUES (1),(2),(3) RETURNING x"
    connection.send(b"P", b"statement\0" + source + b"\0\0\0")
    connection.send(b"B", b"portal\0statement\0" + bytes(6))
    for _ in range(2):
        connection.send(b"E", b"portal\0" + struct.pack("!I", 1))
        connection.send(b"H", b"")
        assert receive_until(connection, b"s").count(b"D") == 1
        assert counters(observer) == [["1", "1", "3"]]

    connection.send(b"C", b"Pportal\0")
    connection.send(b"S", b"")
    receive_until(connection, b"Z")
    assert counters(observer) == [["1", "1", "3"]]
    assert report(observer, "SELECT query FROM pg_stat_statements ORDER BY query") == [
        ["insert into items values ( $1 ) , ( $2 ) , ( $3 ) returning x"]
    ]


def verify_close_unexecuted_returning(connection, observer):
    report(observer, "SELECT pg_stat_statements_reset()")
    source = b"INSERT INTO items VALUES (10) RETURNING x"
    connection.send(b"P", b"unexecuted_stmt\0" + source + bytes(3))
    connection.send(b"B", b"unexecuted_portal\0unexecuted_stmt\0" + bytes(6))
    connection.send(b"C", b"Punexecuted_portal\0")
    connection.send(b"S", b"")
    kinds = receive_until(connection, b"Z")
    assert b"2" in kinds and b"3" in kinds and b"D" not in kinds
    assert counters(observer) == [["1", "0", "0"]]
    assert report(observer, "SELECT x FROM items WHERE x = 10") == []


def verify_failed_returning_execution(connection, observer):
    report(observer, "SELECT pg_stat_statements_reset()")
    source = b"INSERT INTO items VALUES (1) RETURNING x"
    connection.send(b"P", b"failed_stmt\0" + source + bytes(3))
    connection.send(b"B", b"failed_portal\0failed_stmt\0" + bytes(6))
    connection.send(b"E", b"failed_portal\0" + bytes(4))
    connection.send(b"S", b"")
    errors = []
    while True:
        kind, body = connection.receive()
        assert kind not in (b"D", b"C")
        if kind == b"E":
            errors.append(body)
        if kind == b"Z":
            break
    assert len(errors) == 1 and b"C23505\0" in errors[0], errors
    assert report(
        observer, "SELECT state, query, query_id IS NOT NULL, xact_start "
        f"FROM pg_stat_activity WHERE pid = {connection.backend_pid}"
    ) == [["idle", source.decode(), "t", None]]
    assert counters(observer) == [["1", "0", "0"]]
    assert report(
        observer, "SELECT execution_errors FROM pg_stat_statements_resources"
    ) == [["1"]]


def verify_transaction_wait(holder, blocked, observer, cancel=False, commit=False):
    pid = blocked.backend_pid
    activity = (
        "SELECT state, wait_event_type, wait_event, query, query_id IS NOT NULL FROM pg_stat_activity "
        f"WHERE pid = {pid}"
    )
    if cancel:
        blocked.query("SET pg_stat_statements.track_planning = on")
        report(observer, "SELECT pg_stat_statements_reset()")
    # The successful rollback-release case moved key 1 to 5.
    key = 5 if commit else 2 if cancel else 1
    holder.query("BEGIN")
    holder.query(f"UPDATE items SET x = 4 WHERE x = {key}")
    holder_activity = (
        "SELECT state, xact_start IS NOT NULL, query_start > xact_start "
        f"FROM pg_stat_activity WHERE pid = {holder.backend_pid}"
    )
    assert report(observer, holder_activity) == [["idle in transaction", "t", "t"]]
    source = f"UPDATE items SET x = 5 WHERE x = {key}"
    tail = "SHOW application_name"
    submitted = source if cancel else f"SELECT 0; {source} ; {tail}"
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(blocked.observe, submitted)
        try:
            deadline = time.monotonic() + 5
            while True:
                row = report(observer, activity)[0]
                if row == ["active", "Lock", "transactionid", source, "t"]:
                    assert not pending.done()
                    break
                if pending.done() or time.monotonic() >= deadline:
                    raise AssertionError(row)
                time.sleep(0.02)
            if cancel:
                assert report(observer, f"SELECT pg_cancel_backend({pid})") == [["t"]]
                assert pending.result(timeout=5) == ("error", "57014")
                counts = counters(observer)
                assert counts == [["2", "1", "1"]], counts
                assert report(
                    observer, "SELECT execution_errors, execution_cancellations "
                    "FROM pg_stat_statements_resources"
                ) == [["0", "1"]]
        finally:
            holder.query("COMMIT" if commit else "ROLLBACK")
        if not cancel:
            assert pending.result(timeout=5)[0] == "rows"
    row = observer.query(activity)[0]
    last = [source, "t"] if cancel else [tail, "f"]
    assert row in [["idle", None, None, *last], ["idle", "Client", "ClientRead", *last]], row
    assert report(observer, holder_activity) == [["idle", "f", None]]
    if commit:
        assert report(observer, "SELECT x FROM items ORDER BY x") == [["2"], ["3"], ["4"]]


def verify_bind_activity(connection, observer):
    pid = int(connection.query("SELECT pg_backend_pid()")[0][0])
    activity = f"SELECT query, query_id FROM pg_stat_activity WHERE pid = {pid}"
    connection.query("SELECT 1")
    old_text, old_id = observer.query(activity)[0]
    assert old_text == "SELECT 1" and old_id is not None
    source = b"SELECT 1::bigint"
    connection.send(b"P", b"activity_stmt\0" + source + b"\0\0\0")
    connection.send(b"H", b"")
    receive_until(connection, b"1")
    assert observer.query(activity) == [[source.decode(), None]]
    connection.send(b"B", b"activity_portal\0activity_stmt\0" + bytes(6))
    connection.send(b"H", b"")
    receive_until(connection, b"2")
    text, query_id = observer.query(activity)[0]
    assert text == source.decode() and query_id is not None and query_id != old_id
    connection.send(b"S", b"")
    receive_until(connection, b"Z")


def verify_tracking_disabled_before_completion(connection, observer):
    [[planning]] = connection.query("SHOW pg_stat_statements.track_planning")
    connection.query("SET pg_stat_statements.track_planning TO off")
    report(observer, "SELECT pg_stat_statements_reset()")
    connection.query("SELECT set_config('pg_stat_statements.track', 'none', false)")
    assert counters(observer) == []
    connection.query("RESET pg_stat_statements.track")

    for finish in (b"E", b"C"):
        report(observer, "SELECT pg_stat_statements_reset()")
        connection.query("BEGIN")
        connection.send(b"P", b"\0SELECT 1 UNION ALL SELECT 2\0\0\0")
        connection.send(b"B", b"tracking_portal\0\0" + bytes(6))
        connection.send(b"E", b"tracking_portal\0" + struct.pack("!I", 1))
        connection.send(b"S", b"")
        assert b"s" in receive_until(connection, b"Z")
        connection.query("SET pg_stat_statements.track TO 'none'")
        payload = (b"tracking_portal\0" + bytes(4) if finish == b"E"
                   else b"Ptracking_portal\0")
        connection.send(finish, payload)
        connection.send(b"S", b"")
        assert (b"C" if finish == b"E" else b"3") in receive_until(connection, b"Z")
        connection.query("COMMIT")
        assert counters(observer) == []
        connection.query("RESET pg_stat_statements.track")
    connection.query(f"SET pg_stat_statements.track_planning TO {planning}")


def verify_activity_without_counter_collection(connection, observer):
    connection.query("CREATE TABLE untracked_rows (x bigint)")
    connection.query("INSERT INTO untracked_rows VALUES (7)")
    report(observer, "SELECT pg_stat_statements_reset()")
    activity = f"SELECT query, query_id FROM pg_stat_activity WHERE pid = {connection.backend_pid}"
    connection.query("SET pg_stat_statements.track TO 'none'")
    try:
        for source in ("SELECT 7::bigint", "UPDATE untracked_rows SET x=x RETURNING x"):
            assert connection.query(source) == [["7"]]
            [[text, queryid]] = report(observer, activity)
            assert text == source and queryid is not None
            connection.send(b"P", bytes(1) + source.encode() + bytes(3))
            connection.send(b"B", bytes(8))
            connection.send(b"H", b"")
            receive_until(connection, b"2")
            assert report(observer, activity) == [[source, queryid]]
            connection.send(b"E", bytes(5))
            connection.send(b"S", b"")
            assert receive_until(connection, b"Z").count(b"D") == 1
            assert report(observer, activity) == [[source, queryid]]
        assert counters(observer) == []
    finally:
        connection.query("RESET pg_stat_statements.track")


def verify_rollback_discards_suspended_select(connection, observer):
    report(connection, "SELECT pg_stat_statements_reset()")
    connection.query("BEGIN")
    connection.send(b"P", b"rollback_stmt\0SELECT 1\0\0\0")
    connection.send(b"B", b"rollback_portal\0rollback_stmt\0" + bytes(6))
    connection.send(b"E", b"rollback_portal\0" + struct.pack("!I", 1))
    connection.send(b"S", b"")
    assert b"s" in receive_until(connection, b"Z")
    assert counters(observer) == [["1", "0", "0"]]
    connection.query("ROLLBACK")
    assert counters(observer) == [["1", "0", "0"]]
    connection.query("BEGIN")
    connection.send(b"B", b"close_portal\0rollback_stmt\0" + bytes(6))
    connection.send(b"E", b"close_portal\0" + struct.pack("!I", 1))
    connection.send(b"C", b"Pclose_portal\0")
    connection.send(b"S", b"")
    assert b"s" in receive_until(connection, b"Z")
    connection.query("COMMIT")
    assert counters(observer) == [["2", "1", "1"]]


def verify_completed_select_survives_rollback(connection, observer):
    report(connection, "SELECT pg_stat_statements_reset()")
    connection.query("BEGIN")
    connection.send(b"P", b"complete_stmt\0SELECT 1\0\0\0")
    connection.send(b"B", b"complete_portal\0complete_stmt\0" + bytes(6))
    connection.send(b"E", b"complete_portal\0" + bytes(4))
    assert receive_until(connection, b"C").count(b"D") == 1
    assert counters(observer) == [["1", "1", "1"]]
    connection.send(b"S", b"")
    receive_until(connection, b"Z")
    connection.query("ROLLBACK")
    assert counters(observer) == [["1", "1", "1"]]


def verify_reset_during_suspend(server, connection, observer):
    report(observer, "SELECT pg_stat_statements_reset()")
    connection.query("BEGIN")
    source = b"SELECT x FROM observed_dml ORDER BY x"
    connection.send(b"P", b"reset_stmt\0" + source + bytes(3))
    connection.send(b"B", b"reset_portal\0reset_stmt\0" + bytes(6))
    connection.send(b"E", b"reset_portal\0" + struct.pack("!I", 1))
    connection.send(b"S", b"")
    assert b"s" in receive_until(connection, b"Z")
    assert counters(observer) == [["1", "0", "0"]]
    activity = (
        "SELECT state, query, query_id, query_start, xact_start "
        f"FROM pg_stat_activity WHERE pid = {connection.backend_pid}"
    )
    [suspended] = report(observer, activity)
    assert suspended[:2] == ["idle in transaction", source.decode()]
    assert all(value is not None for value in suspended[2:]), suspended
    report(observer, "SELECT pg_stat_statements_reset()")
    assert counters(observer) == []
    run_concurrent_statements(server)
    assert counters(observer) == [["0", "128", "128"]]
    connection.send(b"E", b"reset_portal\0" + bytes(4))
    connection.send(b"S", b"")
    assert receive_until(connection, b"Z").count(b"C") == 1
    assert counters(observer) == [["0", "1", "2"], ["0", "128", "128"]]
    [resumed] = report(observer, activity)
    assert resumed[:3] == suspended[:3] and resumed[4] == suspended[4], resumed
    assert resumed[3] != suspended[3], resumed
    connection.query("COMMIT")
    assert counters(observer) == [["0", "1", "2"], ["0", "128", "128"]]
    [committed] = report(observer, activity)
    assert committed[:3] == ["idle", "COMMIT", None] and committed[4] is None, committed


def verify_partial_frame_wait(connection, observer):
    pid = int(connection.query("SELECT pg_backend_pid()")[0][0])
    activity = f"SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = {pid}"
    payload = b"SELECT 1\0"
    connection.socket.sendall(b"Q")
    for partial in (struct.pack("!I", 4 + len(payload)), payload):
        time.sleep(0.05)
        assert observer.query(activity) == [["Client", "ClientRead"]]
        connection.socket.sendall(partial)
    assert receive_until(connection, b"Z").count(b"D") == 1


def verify_client_write_wait(server, observer):
    consumer = Connection(server.port)
    try:
        pid = consumer.backend_pid
        consumer.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        statement = (
            "SELECT a.payload FROM observed_index a CROSS JOIN observed_index b LIMIT 65536"
        )
        consumer.send(b"Q", statement.encode() + bytes(1))
        activity = (
            "SELECT state, wait_event_type, wait_event, query FROM pg_stat_activity "
            f"WHERE pid = {pid}"
        )
        deadline = time.monotonic() + 5
        consecutive = 0
        while True:
            rows = report(observer, activity)
            waiting = rows == [["active", "Client", "ClientWrite", statement]]
            consecutive = consecutive + 1 if waiting else 0
            if consecutive == 2:
                break
            assert time.monotonic() < deadline, rows
            time.sleep(0.02)
    finally:
        consumer.close()
    deadline = time.monotonic() + 5
    while report(observer, activity):
        assert time.monotonic() < deadline
        time.sleep(0.02)


def verify_disconnect_discards_suspended_select(server, observer):
    report(observer, "SELECT pg_stat_statements_reset()")
    connection = Connection(server.port)
    try:
        pid = connection.backend_pid
        connection.query("SET pg_stat_statements.track_planning = on")
        connection.query("BEGIN")
        source = b"SELECT x FROM observed_dml ORDER BY x"
        connection.send(b"P", b"disconnect_stmt\0" + source + bytes(3))
        connection.send(b"B", b"disconnect_portal\0disconnect_stmt\0" + bytes(6))
        connection.send(b"E", b"disconnect_portal\0" + struct.pack("!I", 1))
        connection.send(b"S", b"")
        assert b"s" in receive_until(connection, b"Z")
        assert counters(observer) == [["1", "0", "0"]]
    finally:
        connection.close()
    query = f"SELECT pid FROM pg_stat_activity WHERE pid = {pid}"
    deadline = time.monotonic() + 5
    while report(observer, query):
        assert time.monotonic() < deadline
        time.sleep(0.02)
    assert counters(observer) == [["1", "0", "0"]]


def verify_restart_clears_statistics(server):
    statement = "SELECT x FROM observed_dml ORDER BY x"
    before = Connection(server.port)
    try:
        report(before, "SELECT pg_stat_statements_reset()")
        assert before.query(statement) == [["11"], ["12"]]
        [[queryid]] = report(before, "SELECT queryid FROM pg_stat_statements")
        [[reset_before]] = report(before, "SELECT stats_reset FROM pg_stat_statements_info")
        assert reset_before is not None
    finally:
        before.close()
    server.stop()
    server.start()
    after = Connection(server.port)
    try:
        assert report(after, f"SELECT calls FROM pg_stat_statements WHERE queryid = {int(queryid)}") == []
        [[dealloc, reset_after]] = report(
            after, "SELECT dealloc, stats_reset FROM pg_stat_statements_info"
        )
        assert dealloc == "0" and reset_after > reset_before
        assert report(after, statement) == [["11"], ["12"]]
    finally:
        after.close()


def verify_reload_preserves_statistics(server, connection, observer):
    report(observer, "SELECT pg_stat_statements_reset()")
    connection.query("BEGIN")
    source = b"SELECT x FROM observed_dml ORDER BY x"
    connection.send(b"P", b"reload_stmt\0" + source + bytes(3))
    connection.send(b"B", b"reload_portal\0reload_stmt\0" + bytes(6))
    connection.send(b"E", b"reload_portal\0" + struct.pack("!I", 1))
    connection.send(b"S", b"")
    assert b"s" in receive_until(connection, b"Z")
    assert counters(observer) == [["1", "0", "0"]]
    query = (
        "SELECT state, query, query_id FROM pg_stat_activity "
        f"WHERE pid = {connection.backend_pid}"
    )
    [[state, query_text, queryid]] = report(observer, query)
    assert state == "idle in transaction" and query_text == source.decode() and queryid is not None
    ready = server.root / "statement-drain-ready"
    drained = server.root / "statement-drained"
    server.control(
        "(sb-thread:make-thread (lambda () "
        "(let ((pg::*live-operation-before-open-hook* "
        f'(lambda () (with-open-file (s "{ready}" :direction :output :if-exists :supersede) '
        '(write-line "ready" s))))) '
        "(pg::call-with-live-operation-reload "
        f'(lambda () (with-open-file (s "{drained}" :direction :output) '
        '(write-line "drained" s)))))))'
    )
    deadline = time.monotonic() + 5
    while not ready.exists():
        assert time.monotonic() < deadline
        time.sleep(0.02)
    assert not drained.exists()
    assert report(observer, query) == [[state, query_text, queryid]]
    connection.query("ROLLBACK")
    deadline = time.monotonic() + 5
    while not drained.exists():
        assert time.monotonic() < deadline
        time.sleep(0.02)
    assert "reload complete" in server.control("(reload)", timeout=60)
    assert report(observer, query) == [["idle", "ROLLBACK", None]]
    assert counters(observer) == [["1", "0", "0"]]


def verify_statistics_eviction():
    server = OwnedLisp((
        ("pg_stat_statements.enabled", "on"),
        ("pg_stat_statements.max", "1"),
        ("pg_stat_statements.max_text_bytes", "64"),
    ))
    succeeded = False
    try:
        server.start()
        connection = Connection(server.port)
        try:
            assert report(
                connection, "SELECT build_revision IS NULL FROM pg_lisp_runtime_stats"
            ) == [["t"]]
            report(connection, "SELECT pg_stat_statements_reset()")
            assert connection.query("SELECT 1") == [["1"]]
            [[first]] = report(connection, "SELECT queryid FROM pg_stat_statements")
            assert connection.query("SELECT 1::bigint") == [["1"]]
            [[second, hidden, calls]] = report(
                connection, "SELECT queryid, query IS NULL, calls FROM pg_stat_statements"
            )
            assert second != first and hidden == "t" and calls == "1"
            assert connection.query("SELECT 2") == [["2"]]
            assert report(
                connection, "SELECT queryid, query, calls FROM pg_stat_statements"
            ) == [[first, "select $1", "1"]]
            assert report(connection, "SELECT dealloc FROM pg_stat_statements_info") == [["2"]]
            report(connection, "SELECT pg_stat_statements_reset()")
            assert report(connection, "SELECT dealloc FROM pg_stat_statements_info") == [["0"]]
        finally:
            connection.close()
        succeeded = True
    finally:
        server.stop()
        if succeeded:
            shutil.rmtree(server.root)
        else:
            print(f"Retained failed eviction fixture at {server.root}")


def run_concurrent_statements(server):
    def client(worker):
        connection = Connection(server.port)
        try:
            for offset in range(32):
                value = worker * 32 + offset
                assert connection.query(f"SELECT {value}::bigint") == [[str(value)]]
        finally:
            connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(client, range(4)))


def verify_disabled_statistics():
    server = OwnedLisp()
    try:
        server.start()
        connection = Connection(server.port)
        try:
            assert connection.query("SHOW pg_stat_statements.enabled") == [["off"]]
            assert connection.query("SELECT 1") == [["1"]]
            assert connection.query("SELECT calls FROM pg_stat_statements") == []
            assert connection.query("SELECT calls FROM pg_stat_statements(true)") == []
            assert connection.query("SELECT dealloc FROM pg_stat_statements_info") == []
            assert connection.query(
                "SELECT allocated_bytes > 0 FROM pg_lisp_runtime_stats"
            ) == [["t"]]
        finally:
            connection.close()
    finally:
        server.stop()
        shutil.rmtree(server.root)


def main():
    server = OwnedLisp((
        ("pg_stat_statements.enabled", "on"),
        ("pg_lisp.build_revision", "observability-wire-fixture"),
    ))
    succeeded = False
    try:
        server.start()
        connection = Connection(server.port)
        try:
            verify_runtime_stats(connection, server)
            verify_statistics_role_visibility(server, connection)
            verify_index_recheck(connection)
            verify_resource_totals(connection)
            verify_nested_loop_rescans(connection)
            verify_spill_resource_totals(server, connection)
            verify_planning_setting(connection)
            verify_simple_select(connection)
            verify_nested_perform(connection)
            verify_set_operation(connection)
            verify_values_identity(connection)
            verify_structural_query_text(connection)
            verify_resolved_relation_identity(connection)
            verify_expression_subquery(connection)
            verify_cte_statistics(connection)
            verify_array_and_case_statistics(connection)
            verify_lateral_statistics(connection)
            verify_in_list_statistics(connection)
            verify_window_statistics(connection)
            verify_set_function_statistics(connection)
            verify_parameterized_dml(connection)
            verify_dml_subqueries(connection)
            verify_execution_error(connection)
            verify_conflict_statistics(connection)
            verify_targeted_reset(connection)
            observer = Connection(server.port)
            try:
                verify_bind_activity(connection, observer)
                verify_tracking_disabled_before_completion(connection, observer)
                verify_activity_without_counter_collection(connection, observer)
                verify_partial_frame_wait(connection, observer)
                verify_client_write_wait(server, observer)
                verify_rollback_discards_suspended_select(connection, observer)
                verify_completed_select_survives_rollback(connection, observer)
                verify_reset_during_suspend(server, connection, observer)
                verify_reload_preserves_statistics(server, connection, observer)
                verify_suspended_returning(connection, observer)
                verify_close_unexecuted_returning(connection, observer)
                verify_failed_returning_execution(connection, observer)
                blocked = Connection(server.port)
                try:
                    verify_transaction_wait(connection, blocked, observer)
                    verify_transaction_wait(connection, blocked, observer, cancel=True)
                    verify_transaction_wait(connection, blocked, observer, commit=True)
                finally:
                    blocked.close()
                verify_disconnect_discards_suspended_select(server, observer)
            finally:
                observer.close()
        finally:
            connection.close()
        verify_restart_clears_statistics(server)
        succeeded = True
    finally:
        server.stop()
        if succeeded:
            shutil.rmtree(server.root)
        else:
            print(f"Retained failed fixture at {server.root}")
    verify_statistics_eviction()
    verify_disabled_statistics()


if __name__ == "__main__":
    main()

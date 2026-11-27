"""Compare pg-lisp partial and expression index semantics with PostgreSQL C."""

import argparse
import concurrent.futures
import contextlib
import json
import runpy
import shutil
import time
from pathlib import Path


range_fixture = runpy.run_path(str(Path(__file__).with_name("btree-range.py")))
OwnedLisp, Connection = (range_fixture[key] for key in ("OwnedLisp", "Connection"))
from parking_wire import WireError


def outcome(connection, statement):
    try:
        return connection.query(statement)
    except WireError as error:
        return error.sqlstate


def compare(peers, statement, expected):
    results = [outcome(peer, statement) for peer in peers]
    assert results == [expected, expected], (statement, results, expected)


def verify_definition_errors(peers):
    compare(peers, "CREATE TABLE expression_errors (id int, divisor int)", [])
    compare(peers, "INSERT INTO expression_errors VALUES (1,0),(2,2)", [])
    for statement, state in (
        ("CREATE INDEX invalid_expr ON expression_errors ((random()))", "42P17"),
        ("CREATE INDEX invalid_pred ON expression_errors (id) WHERE random() < 0.5", "42P17"),
        ("CREATE INDEX invalid_cast ON expression_errors ((CAST('invalid' AS integer)))", "22P02"),
        ("CREATE INDEX invalid_div ON expression_errors ((10 / divisor))", "22012"),
    ):
        compare(peers, statement, state)
    compare(peers, "SELECT relname FROM pg_class WHERE relname IN "
            "('invalid_expr','invalid_pred','invalid_cast','invalid_div') ORDER BY relname", [])
    compare(peers, "CREATE INDEX safe_div ON expression_errors ((10 / divisor)) WHERE divisor > 0", [])
    compare(peers, "UPDATE expression_errors SET divisor=0 WHERE id=2", [])
    compare(peers, "INSERT INTO expression_errors VALUES (3,0)", [])
    compare(peers, "SELECT id FROM expression_errors WHERE divisor > 0 AND 10/divisor = 5", [])


def verify_maintenance(peers):
    compare(peers, "CREATE TABLE expression_rows (id int, label text, flag int)", [])
    compare(peers, "INSERT INTO expression_rows VALUES "
            "(1,'Alpha',1),(2,'ALPHA',0),(3,'Beta',NULL),(4,NULL,1),(5,NULL,0)", [])
    ddl = "CREATE UNIQUE INDEX expression_lookup ON expression_rows (lower(label)) WHERE flag > 0"
    compare(peers, ddl, [])
    for statement, expected in (
        ("INSERT INTO expression_rows VALUES (6,'aLpHa',1)", "23505"),
        ("INSERT INTO expression_rows VALUES (6,'aLpHa',1) ON CONFLICT DO NOTHING", []),
        ("INSERT INTO expression_rows VALUES (6,'aLpHa',1) ON CONFLICT (lower(label)) DO NOTHING", "42P10"),
        ("UPDATE expression_rows SET flag=1 WHERE id=2", "23505"),
        ("UPDATE expression_rows SET label='Other' WHERE id=2", []),
        ("UPDATE expression_rows SET flag=0 WHERE id=1", []),
        ("UPDATE expression_rows SET flag=1 WHERE id=2", []),
        ("UPDATE expression_rows SET label='Gamma' WHERE id=2", []),
        ("UPDATE expression_rows SET flag=1 WHERE id=3", []),
        ("INSERT INTO expression_rows VALUES (6,'gAmMa',1)", "23505"),
        ("INSERT INTO expression_rows VALUES (6,NULL,1)", []),
        ("BEGIN", []),
        ("UPDATE expression_rows SET label='Temp' WHERE id=2", []),
        ("INSERT INTO expression_rows VALUES (7,'Gamma',1)", []),
        ("ROLLBACK", []),
        ("INSERT INTO expression_rows VALUES (7,'Gamma',1)", "23505"),
        ("DELETE FROM expression_rows WHERE id=3", []),
        ("VACUUM expression_rows", []),
        ("INSERT INTO expression_rows VALUES (8,'BETA',1)", []),
    ):
        compare(peers, statement, expected)
    for expression, expected in (
        ("lower(label) = 'gamma' AND flag > 0", [["2"]]),
        ("lower(label) = 'gamma' AND flag >= 1", [["2"]]),
        ("lower(label) = 'beta' AND (flag > 0 OR flag IS NULL)", [["8"]]),
        ("lower(label) IS NULL AND flag > 0", [["4"], ["6"]]),
        ("lower(label) = 'alpha'", [["1"]]),
        ("lower(label || '') = 'gamma' AND flag > 0", [["2"]]),
    ):
        compare(peers, "SELECT id FROM expression_rows WHERE " + expression + " ORDER BY id", expected)


def verify_copy_and_null_uniqueness(peers):
    compare(peers, "CREATE TABLE expression_copy (id int, label text, flag int)", [])
    compare(peers, "INSERT INTO expression_copy SELECT * FROM expression_rows WHERE id <> 6", [])
    compare(peers, "CREATE UNIQUE INDEX copied_expression ON expression_copy ((lower(CAST(label AS text)))) "
            "NULLS NOT DISTINCT WHERE flag > 0", [])
    compare(peers, "INSERT INTO expression_copy VALUES (12,NULL,1)", "23505")
    compare(peers, "INSERT INTO expression_copy VALUES (12,NULL,NULL)", [])
    compare(peers, "INSERT INTO expression_copy VALUES (13,'gamma',1)", "23505")
    compare(peers, "SELECT id FROM expression_copy WHERE lower(CAST(label AS text))='gamma' "
            "AND flag > 0 ORDER BY id", [["2"]])


def verify_unique_waits(peers):
    for observer in peers:
        port = observer.socket.getpeername()[1]
        with contextlib.closing(Connection(port)) as blocker, contextlib.closing(Connection(port)) as contender:
            contender.query("SET statement_timeout = '10s'")
            for action, expected in (("COMMIT", "23505"), ("ROLLBACK", [])):
                label = "conflict_" + action.lower()
                blocker.query("BEGIN")
                blocker.query(f"INSERT INTO expression_rows VALUES (20,'{label}',1)")
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(
                        outcome, contender, f"INSERT INTO expression_rows VALUES (21,'{label}',1)"
                    )
                    try:
                        deadline = time.monotonic() + 5
                        while observer.query(
                            f"SELECT wait_event_type FROM pg_stat_activity WHERE pid={contender.backend_pid}"
                        ) != [["Lock"]]:
                            assert not pending.done() and time.monotonic() < deadline, action
                            time.sleep(0.005)
                        # Predicate-false rows do not compete for the same arbiter.
                        observer.query(f"INSERT INTO expression_rows VALUES (22,'{label}',0)")
                        blocker.query(action)
                    finally:
                        blocker.query("ROLLBACK")
                    assert pending.result(timeout=10) == expected, action
    compare(peers, "SELECT id FROM expression_rows WHERE lower(label)='conflict_commit' ORDER BY id",
            [["20"], ["22"]])
    compare(peers, "SELECT id FROM expression_rows WHERE lower(label)='conflict_rollback' ORDER BY id",
            [["21"], ["22"]])


def verify_restored_indexes(peers):
    compare(peers, "SELECT indkey, indnkeyatts, indisvalid, indisunique FROM pg_index "
            "WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname='copied_expression')",
            [["0", "1", "t", "t"]])
    compare(peers, "SELECT relname FROM pg_class WHERE relname='aborted_expression'", [])
    compare(peers, "SELECT id FROM expression_copy WHERE lower(CAST(label AS text))='gamma' "
            "AND flag > 0 ORDER BY id", [["2"]])
    compare(peers, "INSERT INTO expression_copy VALUES (23,NULL,1)", "23505")
    compare(peers, "INSERT INTO expression_rows VALUES (23,'CONFLICT_COMMIT',1)", "23505")


def verify_temporal_acceptance(peers):
    compare(peers, "CREATE TABLE events (id int, tenant_id int, created_at timestamp, "
            "zoned_at timestamptz, status text)", [])
    for start in range(0, 1200, 200):
        rows = ",".join(
            f"({i},{42 if i == 42 else 17},'2026-11-26 15:00:00',"
            "'2026-11-26 15:00:00+00','active')" for i in range(start, start + 200)
        )
        compare(peers, "INSERT INTO events VALUES " + rows, [])
    compare(peers, "CREATE INDEX active_tenant_bucket ON events (tenant_id, (created_at::date)) "
            "WHERE status = 'active'", [])
    compare(peers, "CREATE INDEX zoned_bucket ON events ((zoned_at::date))", "42P17")
    compare(peers, "ANALYZE events", [])
    query = ("SELECT id FROM events WHERE status = 'active' AND tenant_id = 42 "
             "AND created_at::date = '2026-11-26'::date")
    plan = peers[0].query("EXPLAIN " + query)[0][0]
    assert "Index Scan using active_tenant_bucket" in plan, plan
    node = json.loads(peers[0].query("EXPLAIN (FORMAT JSON) " + query)[0][0])[0]["Plan"]
    while node["Node Type"] != "Index Scan":
        [node] = node["Plans"]
    assert node["Plan Rows"] <= 4, node
    compare(peers, query, [["42"]])
    compare(peers, "UPDATE events SET status='inactive' WHERE id=42", [])
    compare(peers, query, [])
    compare(peers, "UPDATE events SET status='active', created_at='2026-11-27 00:00:00' WHERE id=42", [])
    compare(peers, query, [])
    compare(peers, query.replace("'2026-11-26'::date", "'2026-11-27'::date"), [["42"]])


def main(reference_bin):
    server = OwnedLisp()
    try:
        server.start()
        with range_fixture["reference_server"](reference_bin) as c:
            with contextlib.closing(Connection(server.port)) as native:
                peers = (native, c)
                verify_definition_errors(peers)
                verify_maintenance(peers)
                verify_copy_and_null_uniqueness(peers)
                verify_unique_waits(peers)
                verify_temporal_acceptance(peers)
                compare(peers, "BEGIN", [])
                compare(peers, "CREATE INDEX aborted_expression ON expression_rows ((id + 1)) WHERE flag > 0", [])
                compare(peers, "ROLLBACK", [])
            server.stop(immediate=True)
            server.start()
            with contextlib.closing(Connection(server.port)) as native:
                verify_restored_indexes((native, c))
        print("P04 index DDL and DML matched PostgreSQL C")
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-bin", type=Path, required=True)
    main(parser.parse_args().reference_bin)

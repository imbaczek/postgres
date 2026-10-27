"""Observe executor continuations across real replay, promotion and live reload."""
import json
import os
import re
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2

from parking_wire import RawConnection, WireError

MATCH_LABEL = "match" + "x" * 80


def effect(value):
    before = "set_config('pg_lisp.resource_effects',current_setting('pg_lisp.resource_effects') || ':b',false)"
    after = "set_config('pg_lisp.resource_effects',current_setting('pg_lisp.resource_effects') || ':a',false)"
    hook = "current_setting('pg_lisp_test_resource_park')"
    if os.environ.get("RESOURCE_SQL_PROMOTION") == "1":
        hook = f"CASE WHEN {hook}='promote' THEN pg_promote(true,60) ELSE true END"
    return (f"CASE WHEN {before} IS NOT NULL THEN CASE WHEN {hook} IS NOT NULL "
            f"THEN CASE WHEN {after} IS NOT NULL THEN {value} ELSE {value} END "
            f"ELSE {value} END ELSE {value} END")


def cases():
    join = ("SELECT l.id,l.label,r.rid,r.tag,{marker} AS marker "
            "FROM resource_join_left l FULL JOIN resource_right r ON l.id=r.k")
    sorted_rows = ("SELECT s.id,s.label,{marker} AS marker FROM "
                   "(SELECT id,label FROM resource_left ORDER BY id) s ORDER BY s.id")
    result = {
        "seq_stream": dict(sql="SELECT id,label,{marker} AS marker FROM resource_left",
                           after=5, visits=96, raw=True, pages=3, plan="Seq Scan"),
        "join_load": dict(sql=join, after=2, visits=16, files=1, writers=1, io=1,
                          unordered=True, plan="Full", match_phase=":unfinished"),
        "join_replay": dict(sql=join, after=3, visits=16, files=1, readers=1, io=1,
                            unordered=True, plan="Full", match_phase=":unfinished"),
        "join_unmatched": dict(sql=join, after=15, visits=16, files=1, readers=1, io=1,
                               unordered=True, plan="Full", match_phase=":finished"),
        "aggregate_input": dict(sql="SELECT count(*),sum(id),array_agg({marker}) FROM resource_left",
                                value="label::text", after=3, visits=96, files=1, writers=1, plan="Aggregate"),
        "aggregate_output": dict(sql="SELECT count(*),sum(id),array_agg(label::text),{marker} AS marker FROM resource_left",
                                 after=1, visits=1, files=1, plan="Aggregate"),
        "hash_output": dict(sql="SELECT label,count(*),{marker} AS marker FROM resource_left GROUP BY label",
                            after=2, visits=4, files=1, unordered=True, plan="HashAggregate"),
        "sort_input": dict(sql="SELECT id,label FROM resource_left ORDER BY {marker}",
                           value="id", after=3, visits=96, files=2, plan="Sort"),
        "sort_output": dict(sql=sorted_rows, after=1, visits=96, files=1, readers=1, plan="Sort"),
        "arena_memory": dict(sql=sorted_rows, after=1, visits=96, budget=1048576,
                             arena=1048576, plan="Sort"),
        "arena_spill": dict(sql=sorted_rows, after=1, visits=96, files=1, readers=1,
                            arena=1048576, plan="Sort"),
    }
    result["cancel_sort"] = dict(result["sort_output"], action="cancel")
    result["disconnect_sort"] = dict(result["sort_output"], action="disconnect")
    result["quota_join"] = dict(result["join_load"], action="quota")
    result["nested_indexes"] = dict(
        sql=("SELECT o.id,o.label,i.id,i.tag,{marker} AS marker "
             "FROM resource_outer_indexed o JOIN resource_inner_indexed i ON i.owner_id=o.id "
             f"WHERE o.label='{MATCH_LABEL}'"), after=1, visits=192, files=2,
        unordered=True, plan="Nested Loop Inner Join", indexed=True, index_scans=2, repairs=95)
    for case in result.values():
        value = case.get("value", "1")
        case["before"] = case["sql"].format(marker=value)
        case["query"] = case["sql"].format(marker=effect(value))
    return result


def connect(port, name="executor-control"):
    connection = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  dbname="postgres", connect_timeout=3, application_name=name)
    connection.autocommit = True
    return connection


def query(connection, sql):
    if isinstance(connection, RawConnection):
        return connection.query(sql)
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else None


def scalar(connection, sql):
    return query(connection, sql)[0][0]


def wait_until(predicate):
    deadline = time.monotonic() + 25
    while not predicate():
        assert time.monotonic() < deadline, "executor parking barrier timed out"
        time.sleep(0.025)


def setup_indexed(connection):
    pads = ",".join(f"p{i} varchar(120)" for i in range(24))
    query(connection, f"CREATE TABLE resource_outer_indexed (id integer PRIMARY KEY,label varchar(120),{pads})")
    query(connection, f"CREATE TABLE resource_inner_indexed (id integer PRIMARY KEY,owner_id integer,tag varchar(120),{pads})")
    query(connection, "INSERT INTO resource_outer_indexed (id,label) VALUES " +
          ",".join(f"({i},'{MATCH_LABEL}')" for i in range(1, 97)))
    query(connection, "INSERT INTO resource_inner_indexed (id,owner_id,tag) VALUES " +
          ",".join(f"({i},{(i + 1) // 2},'inner{i}')" for i in range(1, 193)))
    padding = ",".join("'" + "x" * 120 + "'" for _ in range(24))
    for start in range(1000, 5096, 64):
        query(connection, "INSERT INTO resource_outer_indexed VALUES " +
              ",".join(f"({i},'other',{padding})" for i in range(start, start + 64)))
        query(connection, "INSERT INTO resource_inner_indexed VALUES " +
              ",".join(f"({i},-1,'other',{padding})" for i in range(start, start + 64)))
    query(connection, "CREATE INDEX resource_outer_label_idx ON resource_outer_indexed (label)")
    query(connection, "CREATE INDEX resource_inner_owner_idx ON resource_inner_indexed (owner_id)")
    query(connection, "ANALYZE resource_outer_indexed")
    query(connection, "ANALYZE resource_inner_indexed")


def setup(port, indexed=False):
    connection = connect(port)
    try:
        query(connection, "CREATE TABLE resource_left (id integer PRIMARY KEY,label varchar(120),p1 varchar(120),p2 varchar(120))")
        padding = "x" * 120
        query(connection, "INSERT INTO resource_left VALUES " +
              ",".join(f"({i},'g{i % 4}','{padding}','{padding}')" for i in range(1, 97)))
        query(connection, "CREATE TABLE resource_join_left (id integer PRIMARY KEY,label varchar(120))")
        query(connection, "INSERT INTO resource_join_left VALUES " +
              ",".join(f"({i},'left{i}')" for i in range(1, 9)))
        query(connection, "CREATE TABLE resource_right (rid integer PRIMARY KEY,k integer,tag varchar(120))")
        rows = [(i, (i + 1) // 2) for i in range(1, 13)] + [(13, 9), (14, 10)]
        query(connection, "INSERT INTO resource_right VALUES " +
              ",".join(f"({rid},{key},'right{rid}')" for rid, key in rows))
        if indexed:
            setup_indexed(connection)
    finally:
        connection.close()


def normalized(rows, case, raw=False):
    rows = json.loads(json.dumps(rows))
    if raw:
        rows = [[None if value is None else str(value) for value in row] for row in rows]
    return sorted(rows, key=repr) if case.get("unordered") else rows


def oracle(port, path):
    setup(port, indexed=True)
    connection = connect(port)
    try:
        expected = {name: query(connection, case["before"]) for name, case in cases().items()}
        Path(path).write_text(json.dumps(expected))
        wire = RawConnection(port, "executor-wire-oracle")
        try:
            assert wire.query("SELECT 1,'wire',NULL") == [["1", "wire", None]]
            try:
                wire.query("SELECT 1/0")
                raise AssertionError("raw oracle missed division by zero")
            except WireError as failure:
                assert failure.sqlstate == "22012"
            assert wire.query("SELECT 2") == [["2"]]
        finally:
            wire.close()
        print("executor-parking: pinned C result and raw-protocol oracles prepared", flush=True)
    finally:
        connection.close()


def control(base, root, expression):
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(base / "t/control-client.lisp"),
         "unix", str(root / "target.sock")], input=expression + "\n(quit)\n",
        text=True, capture_output=True, timeout=30, check=True)
    assert ";; error:" not in result.stdout + result.stderr, result.stdout + result.stderr
    return result.stdout


def index_blocks(base, root):
    output = control(base, root, '(format t "RESOURCE_INDEX_BLOCKS=~d~%" (resource-parking-index-blocks))')
    return int(re.search(r"RESOURCE_INDEX_BLOCKS=(\d+)", output)[1])


def mutate(connection, role, added, indexed=False):
    for table in ("resource_left", "resource_join_left"):
        query(connection, f"UPDATE {table} SET label='{role}' WHERE id={2 if role == 'parent-after' else 3}")
        query(connection, f"INSERT INTO {table} (id,label) VALUES ({added},'{role}')")
    query(connection, f"UPDATE resource_right SET tag='{role}' WHERE rid={2 if role == 'parent-after' else 3}")
    query(connection, f"INSERT INTO resource_right VALUES ({added},1,'{role}')")
    if indexed:
        changed = 3 if role == "parent-after" else 2
        query(connection, f"UPDATE resource_outer_indexed SET label='{role}' WHERE id={changed}")
        query(connection, f"INSERT INTO resource_outer_indexed (id,label) VALUES ({50000 + added},'{MATCH_LABEL}')")
        query(connection, f"UPDATE resource_inner_indexed SET tag='{role}' WHERE id={changed * 2}")
        query(connection, f"INSERT INTO resource_inner_indexed (id,owner_id,tag) VALUES ({50000 + added},{changed},'{role}')")


def verify_mutations(connection, role, added, indexed=False):
    changed = 2 if role == "parent-after" else 3
    for table in ("resource_left", "resource_join_left"):
        assert scalar(connection, f"SELECT label FROM {table} WHERE id={changed}") == role
        assert scalar(connection, f"SELECT label FROM {table} WHERE id={added}") == role
    assert scalar(connection, f"SELECT tag FROM resource_right WHERE rid={changed}") == role
    assert scalar(connection, f"SELECT tag FROM resource_right WHERE rid={added}") == role
    if indexed:
        changed = 3 if role == "parent-after" else 2
        assert scalar(connection, f"SELECT label FROM resource_outer_indexed WHERE id={changed}") == role
        assert scalar(connection, f"SELECT label FROM resource_outer_indexed WHERE id={50000 + added}") == MATCH_LABEL
        assert scalar(connection, f"SELECT tag FROM resource_inner_indexed WHERE id={changed * 2}") == role
        assert scalar(connection, f"SELECT tag FROM resource_inner_indexed WHERE id={50000 + added}") == role


def finish_failed_case(base, root, case, reader, target, future):
    action = case["action"]
    disconnected = action == "disconnect"
    if action == "cancel":
        reader.cancel()
    elif disconnected:
        with socket.fromfd(reader.fileno(), socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.shutdown(socket.SHUT_RDWR)
    else:
        control(base, root, "(resource-parking-exhaust-quota)")
    (root / "release").touch()
    try:
        future.result(timeout=30)
        raise AssertionError(f"expected {action} to abort the query")
    except psycopg2.Error as failure:
        if disconnected:
            assert isinstance(failure, psycopg2.OperationalError), failure
        else:
            assert failure.pgcode == ("57014" if action == "cancel" else "53400"), failure
    flag = "t" if disconnected else "nil"
    wait_until(lambda: ":DONE" in control(
        base, root, f"(if (resource-parking-abort-ready-p {flag}) :done :waiting)"))
    expected_visits = case["visits"] if disconnected else case["after"]
    control(base, root, f"(assert (resource-parking-verify-aborted {flag} {expected_visits}))")
    assert scalar(target, "SELECT pg_is_in_recovery()") is False
    verify_mutations(target, "parent-after", 1000)
    verify_mutations(target, "child-after", 2000)
    if not disconnected:
        query(reader, "ROLLBACK")
        assert scalar(reader, "SHOW transaction_read_only") == "off"
        verify_mutations(reader, "child-after", 2000)


def run(base, root, source_port, target_port, target_pid):
    name = os.environ["RESOURCE_PARKING_CASE"]
    case = cases()[name]
    source, target = connect(source_port), connect(target_port)
    reader = RawConnection(target_port) if case.get("raw") else connect(target_port, "executor-parking")
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        query(source, "INSERT INTO promotion_sql_items VALUES (2,'warm'),(3,'warm2')")
        query(source, "INSERT INTO promotion_sql_items VALUES (4,'warm3')")
        wait_until(lambda: scalar(target, "SELECT count(*) FROM promotion_sql_items") == 4)
        query(reader, "SET statement_timeout='120s'")
        query(reader, "BEGIN ISOLATION LEVEL REPEATABLE READ")
        before = query(reader, case["before"])
        expected = json.loads(Path(os.environ["RESOURCE_PARKING_ORACLE"]).read_text())[name]
        assert normalized(before, case) == normalized(expected, case, case.get("raw")), (name, before, expected)
        plan = query(target, "EXPLAIN " + case["query"])
        assert any(case["plan"] in row[0] for row in plan), (name, plan)
        assert sum(row[0].count("Index Scan") for row in plan) >= case.get("index_scans", 0), (name, plan)
        if case.get("indexed"):
            for index in ("resource_outer_label_idx", "resource_inner_owner_idx"):
                assert any(index in row[0] for row in plan), (name, index, plan)
        query(reader, "SELECT set_config('pg_lisp.resource_effects','trace',false)")
        backend_pid = int(scalar(reader, "SELECT pg_backend_pid()"))
        control(base, root, f'(load "{base}/t/live-operation-parking-native.lisp")')
        control(base, root, f'(load "{base}/t/executor-parking-native.lisp")')
        options = [case.get(key, 0) for key in ("files", "readers", "writers", "io")]
        arena = str(case["arena"]) if case.get("arena") else "nil"
        control(base, root, f'(resource-parking-install #p"{root}/" {case["after"]} '
                f'{case.get("budget", 1)} {arena} ' + " ".join(map(str, options)) +
                f' {case.get("match_phase", ":any")} {case.get("index_scans", 0)})')
        sql_promotion = os.environ.get("RESOURCE_SQL_PROMOTION") == "1"
        if sql_promotion:
            control(base, root, f'(load "{base}/t/promotion-sql-native.lisp")')
            control(base, root, f'(resource-parking-install-sql-promotion #p"{root}/" {backend_pid})')
        future = pool.submit(query, reader, case["query"])
        wait_until(lambda: (root / "parked").exists() or future.done())
        if future.done():
            raise AssertionError(f"{name} finished before parking: {future.result()}")
        assert int((root / "parked").read_text()) == target_pid
        print(f"executor-parking: {name} parked; " +
              (root / "resource-facts").read_text().strip().replace("\n", " "), flush=True)
        paths = [Path(line) for line in (root / "resource-paths").read_text().splitlines()]
        assert all(path.exists() and path.resolve().is_relative_to(root / "copy") for path in paths)
        if case.get("raw"):
            wait_until(lambda: len(reader.rows_seen) >= case["after"] - 1)
            assert len(reader.rows_seen) == case["after"] - 1
            assert reader.rows_seen == before[:case["after"] - 1]
        control(base, root, "(sb-ext:gc :full t)")
        subprocess.run([sys.executable, str(base / "t/interop/signal-owned-server.py"),
                        str(target_pid), str(root / "copy"), "HUP"], check=True, timeout=5)
        wait_until(lambda: (root / "reload-backed-off").exists())
        mutate(source, "parent-after", 1000, case.get("indexed"))
        position = scalar(source, "SELECT pg_current_wal_flush_lsn()")
        wait_until(lambda: scalar(target, f"SELECT pg_last_wal_replay_lsn() >= '{position}'::pg_lsn"))
        if sql_promotion:
            wait_until(lambda: (root / "sql-worker-held").exists())
            (root / "sql-worker-release").touch()
            wait_until(lambda: (root / "sql-wait-completed").exists())
        else:
            result = subprocess.run([str(base / "bin/pg_ctl"), "promote", "-D", str(root / "copy"),
                                     "-w", "-t", "30"], text=True, capture_output=True, timeout=40)
            assert result.returncode == 0, result.stdout + result.stderr
        assert scalar(target, "SELECT pg_is_in_recovery()") is False
        assert not future.done(), f"{name} resumed before release"
        verify_mutations(target, "parent-after", 1000, case.get("indexed"))
        blocks = index_blocks(base, root) if case.get("indexed") else None
        query(target, "SET statement_timeout='15s'")
        mutate(target, "child-after", 2000, case.get("indexed"))
        verify_mutations(target, "child-after", 2000, case.get("indexed"))
        if case.get("indexed"):
            query(target, "INSERT INTO resource_inner_indexed (id,owner_id,tag) VALUES " +
                  ",".join(f"({100000 + i},{i % 96 + 1},'growth')" for i in range(512)))
            assert index_blocks(base, root) > blocks, "child writer did not split the selected inner index"
        if case.get("action"):
            finish_failed_case(base, root, case, reader, target, future)
            print(f"executor-parking: {name} expected abort, cleanup, reload and primary state PASS", flush=True)
            return
        (root / "release").touch()
        actual = future.result(timeout=30)
        assert normalized(actual, case) == normalized(before, case), (name, actual, before)
        assert scalar(reader, "SELECT current_setting('pg_lisp.resource_effects')") == "trace" + ":b:a" * case["visits"]
        assert all(not path.exists() for path in paths), f"{name} retained spill files"
        wait_until(lambda: ":NEW" in control(base, root, "(if (parking-native-reloaded-p) :new :old)"))
        control(base, root, f'(assert (resource-parking-verify {case["visits"]} {case.get("pages", 0)} '
                f'{1 if case.get("arena") else 0} {case.get("repairs", 0)}))')
        assert normalized(query(reader, case["before"]), case) == normalized(before, case)
        assert scalar(reader, "SHOW transaction_read_only") == "on"
        query(reader, "COMMIT")
        assert scalar(reader, "SHOW transaction_read_only") == "off"
        verify_mutations(reader, "parent-after", 1000, case.get("indexed"))
        verify_mutations(reader, "child-after", 2000, case.get("indexed"))
        print(f"executor-parking: {name} replay, promotion, GC, exact continuation and reload PASS", flush=True)
    finally:
        (root / "release").touch()
        (root / "sql-worker-release").touch()
        reader.close()
        pool.shutdown(wait=True)
        source.close()
        target.close()


if __name__ == "__main__":
    if sys.argv[1] == "--setup":
        setup(int(sys.argv[2]), indexed=cases()[os.environ["RESOURCE_PARKING_CASE"]].get("indexed", False))
    elif sys.argv[1] == "--oracle":
        oracle(int(sys.argv[2]), sys.argv[3])
    elif sys.argv[1] == "--list-cases":
        print(" ".join(cases()))
    else:
        run(Path(sys.argv[1]), Path(sys.argv[2]), *map(int, sys.argv[3:]))

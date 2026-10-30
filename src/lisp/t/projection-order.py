"""Regression: Simple and Extended Query share post-sort volatile projection."""
import importlib.util
import os
from pathlib import Path
import re
import struct
import subprocess
import sys

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def extended(connection, sql):
    parameters = []
    for name in ("projection_seq", "projection_key", "projection_pub"):
        literal = "'" + name + "'"
        if literal in sql:
            parameters.append(name.encode())
            sql = sql.replace(literal, "$" + str(len(parameters)))
    connection.socket.sendall(wire.parse(sql, (25,) * len(parameters)) +
                              wire.bind(tuple(parameters)) +
                              wire.frame(b"E", b"\0" + struct.pack("!I", 0)) +
                              wire.frame(b"S", b""))
    rows = []
    while True:
        kind, body = connection.receive()
        assert kind != b"E", connection.error_fields(body)
        if kind == b"D":
            rows.append(connection.data_row(body))
        elif kind == b"Z":
            return rows


def check(connection):
    connection.query("CREATE TABLE projection_order(id int)")
    connection.query("INSERT INTO projection_order VALUES (2),(1)")
    connection.query("CREATE SEQUENCE projection_seq")
    connection.query("CREATE SEQUENCE projection_key")
    cases = (
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY id",
         [["1", "1"], ["2", "2"]], "2"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY id LIMIT 1",
         [["1", "1"]], "1"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY id DESC LIMIT 1",
         [["2", "1"]], "1"),
        ("SELECT id,nextval('projection_seq') AS n FROM projection_order ORDER BY n DESC LIMIT 1",
         [["1", "2"]], "2"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY 2 DESC LIMIT 1",
         [["1", "2"]], "2"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY nextval('projection_key') DESC LIMIT 1",
         [["1", "1"]], "1"),
        ("SELECT DISTINCT id,nextval('projection_seq') FROM projection_order ORDER BY id",
         [["1", "2"], ["2", "1"]], "2"),
        ("SELECT id,nextval('projection_seq') FROM projection_order GROUP BY id ORDER BY id LIMIT 1",
         [["1", "1"]], "1"),
        ("SELECT id,nextval('projection_seq') FROM (SELECT id FROM projection_order ORDER BY id) u ORDER BY id DESC LIMIT 1",
         [["2", "1"]], "1"),
        ("SELECT id,nextval('projection_seq') FROM projection_order ORDER BY id LIMIT 0",
         [], "100"),
    )
    for query in (connection.query, lambda sql: extended(connection, sql)):
        for sql, expected, current in cases:
            # Called=true makes LIMIT 0 observable without currval being undefined.
            connection.query("SELECT setval('projection_seq',100,true)")
            connection.query("SELECT setval('projection_key',1,false)")
            if current != "100":
                connection.query("SELECT setval('projection_seq',1,false)")
            actual = query(sql)
            assert actual == expected, (sql, actual, expected)
            assert connection.query("SELECT currval('projection_seq')") == [[current]], sql
            if "ORDER BY nextval(" in sql:
                assert connection.query("SELECT currval('projection_key')") == [["2"]], sql
    print("projection-order: Simple/Extended sort, top-N, aliases, ordinals and sequence effects PASS")


def check_dml(connection):
    connection.query("CREATE TABLE projection_dst(id int,n bigint)")
    connection.query("CREATE TABLE projection_set_dst(id int,n bigint,x text)")
    connection.query("CREATE TABLE projection_extra(id int)")
    connection.query("CREATE PUBLICATION projection_pub FOR TABLE projection_order,projection_extra")
    cases = (
        ("ORDER BY id", [["1", "1"], ["2", "2"]], "2"),
        ("ORDER BY id LIMIT 1", [["1", "1"]], "1"),
        ("ORDER BY id DESC LIMIT 1", [["2", "1"]], "1"),
        ("ORDER BY n DESC LIMIT 1", [["1", "2"]], "2"),
        ("ORDER BY id LIMIT 0", [], "100"),
    )
    for query in (connection.query, lambda sql: extended(connection, sql)):
        for returning in (False, True):
            for suffix, expected, current in cases:
                connection.query("DELETE FROM projection_dst")
                connection.query("SELECT setval('projection_seq',100,true)")
                if current != "100":
                    connection.query("SELECT setval('projection_seq',1,false)")
                sql = ("INSERT INTO projection_dst SELECT id,nextval('projection_seq') AS n "
                       f"FROM projection_order {suffix}")
                if returning:
                    sql += " RETURNING id,n"
                rows = query(sql)
                assert rows == (expected if returning else []), (sql, rows, expected)
                assert connection.query("SELECT * FROM projection_dst ORDER BY id") == expected, sql
                assert connection.query("SELECT currval('projection_seq')") == [[current]], sql
        connection.query("DELETE FROM projection_dst")
        connection.query("INSERT INTO projection_dst VALUES(0,0)")
        connection.query("SELECT setval('projection_seq',1,false)")
        sql = ("UPDATE projection_dst SET n=(SELECT nextval('projection_seq') "
               "FROM projection_order ORDER BY id LIMIT 1) RETURNING n")
        assert query(sql) == [["1"]], sql
        assert connection.query("SELECT currval('projection_seq')") == [["1"]], sql

        connection.query("DELETE FROM projection_set_dst")
        connection.query("SELECT setval('projection_seq',1,false)")
        sql = ("INSERT INTO projection_set_dst "
               "SELECT id,nextval('projection_seq'),pg_get_publication_tables('projection_pub') "
               "FROM projection_order ORDER BY id LIMIT 1 RETURNING id,n")
        assert query(sql) == [["1", "1"]], sql
        assert connection.query("SELECT id,n FROM projection_set_dst") == [["1", "1"]], sql
        assert connection.query("SELECT currval('projection_seq')") == [["1"]], sql

        # Keep a bare SRF in the sorted inner projection, then drop its record
        # output so the INSERT target can use ordinary scalar types.
        connection.query("DELETE FROM projection_dst")
        connection.query("SELECT setval('projection_seq',1,false)")
        sql = ("INSERT INTO projection_dst SELECT id,n FROM "
               "(SELECT id,nextval('projection_seq') AS n,"
               "pg_get_publication_tables('projection_pub') AS x "
               "FROM projection_order ORDER BY id LIMIT 1) q RETURNING id,n")
        assert query(sql) == [["1", "1"]], sql
        assert connection.query("SELECT currval('projection_seq')") == [["1"]], sql
    print("projection-order: INSERT, RETURNING, scalar UPDATE and sorted ProjectSet PASS")


def check_compressed_origins(connection, control=None):
    connection.query("CREATE TABLE projection_compressed(id int,v text)")
    connection.query("CREATE TABLE projection_plain(id int,v text,n bigint,x text)")
    connection.query("ALTER TABLE projection_plain ALTER COLUMN v SET STORAGE PLAIN")
    connection.query("INSERT INTO projection_compressed VALUES " +
                     ",".join(f"({i},'{chr(96 + i) * 20000}')" for i in range(8, 0, -1)))
    assert connection.query("SELECT count(*) FROM projection_compressed WHERE length(v)=20000") == [["8"]]
    cases = (
        ("id,v", "id,v", 1, False),
        ("id,v,n", "id,v,nextval('projection_seq')", 1, True),
        ("id,v", "id,CASE WHEN nextval('projection_seq')>0 THEN v ELSE v END", 1, True),
        ("id,v,n,x", "id,v,nextval('projection_seq'),pg_get_publication_tables('projection_pub')", 2, False),
        ("id,v,n,x", "id,v,nextval('projection_seq'),"
         "(pg_get_publication_tables('projection_pub')).relid IS NOT NULL", 2, False),
    )

    def control_integer(expression):
        output = control(f'(format t "PROJECTION_VALUE=~d~%" {expression})')
        return int(re.search(r"PROJECTION_VALUE=(\d+)", output)[1])

    old_memory = (control_integer("pg::*query-work-memory-bytes*") if control else
                  connection.query("SHOW work_mem")[0][0])
    try:
        # Decoded source values and cached projections exceed 64kB, forcing
        # Lisp's full sort and sorted ProjectSet through their spill paths.
        for memory in (4 * 1024 * 1024, 64 * 1024):
            if control:
                control(f"(setf pg::*query-work-memory-bytes* {memory})")
            else:
                connection.query(f"SET work_mem='{memory // 1024}kB'")
            spill_before = (control_integer("pg::*query-temp-file-sequence*")
                            if control and memory == 64 * 1024 else None)
            for query in (connection.query, lambda sql: extended(connection, sql)):
                for suffix in ("ORDER BY id", "ORDER BY id LIMIT 1"):
                    for columns, targets, expansion, scalar_sequence in cases:
                        connection.query("DELETE FROM projection_plain")
                        connection.query("SELECT setval('projection_seq',1,false)")
                        sql = (f"INSERT INTO projection_plain({columns}) SELECT {targets} "
                               f"FROM projection_compressed {suffix} RETURNING id")
                        ids = [1] if "LIMIT" in suffix else [i for i in range(1, 9) for _ in range(expansion)]
                        assert query(sql) == [[str(i)] for i in ids], sql
                        # A 20k PLAIN datum fits only if its existing inline
                        # compression survives; new compression is disallowed.
                        expected = [[str(i), "20000", chr(96 + i)] for i in ids]
                        actual = connection.query("SELECT id,length(v),substr(v,1,1) "
                                                  "FROM projection_plain ORDER BY id")
                        assert actual == expected, (sql, memory, actual, expected)
                        if scalar_sequence or "LIMIT" in suffix and "nextval" in targets:
                            assert connection.query("SELECT currval('projection_seq')") == [[str(len(ids))]], sql
            if spill_before is not None:
                assert control_integer("pg::*query-temp-file-sequence*") > spill_before
    finally:
        if control:
            control(f"(setf pg::*query-work-memory-bytes* {old_memory})")
        else:
            connection.query(f"SET work_mem='{old_memory}'")
    print("projection-order: compressed origins survive deferred projection, ProjectSet and spill PASS")


def run():
    server = None if len(sys.argv) > 1 else wire.OwnedLisp()
    connection = None
    try:
        if server:
            server.start()
        connection = wire.Connection(int(sys.argv[1]) if server is None else server.port)
        check(connection)
        check_dml(connection)
        control = server.control if server else None
        if not server and len(sys.argv) > 2:
            def control(expression):
                result = subprocess.run(
                    [os.environ.get("SBCL", "sbcl"), "--script",
                     str(Path(__file__).with_name("control-client.lisp")), "unix", sys.argv[2]],
                    input=expression + "\n(quit)\n", text=True,
                    capture_output=True, check=True, timeout=15)
                output = result.stdout + result.stderr
                assert ";; error:" not in output, output
                return output
        check_compressed_origins(connection, control)
    finally:
        if connection:
            connection.close()
        if server:
            server.stop()
            print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()

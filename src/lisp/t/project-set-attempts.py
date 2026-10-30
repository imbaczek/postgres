"""Regression: ProjectSet evaluates the full target list on every set attempt."""
import importlib.util
from pathlib import Path
import struct
import sys

spec = importlib.util.spec_from_file_location("projection_order", Path(__file__).with_name("projection-order.py"))
projection = importlib.util.module_from_spec(spec)
spec.loader.exec_module(projection)
wire = projection.wire
from returning_portals import bind, cycle, execute, expect_kinds


def extended(connection, sql):
    values = []
    for name in ("attempt_seq", "attempt_one", "attempt_empty", "attempt_two"):
        literal = "'" + name + "'"
        if literal in sql:
            values.append(name.encode())
            sql = sql.replace(literal, "$" + str(len(values)))
    connection.socket.sendall(wire.parse(sql, (25,) * len(values)) + wire.bind(tuple(values)) +
                              wire.frame(b"E", b"\0" + struct.pack("!I", 0)) + wire.frame(b"S", b""))
    rows = []
    while True:
        kind, body = connection.receive()
        assert kind != b"E", connection.error_fields(body)
        if kind == b"D":
            rows.append(connection.data_row(body))
        elif kind == b"Z":
            return rows


def check(connection):
    connection.query("CREATE TABLE attempt_rows(id int)")
    connection.query("INSERT INTO attempt_rows VALUES(2),(1)")
    connection.query("CREATE TABLE attempt_extra(id int)")
    connection.query("CREATE SEQUENCE attempt_seq")
    connection.query("CREATE PUBLICATION attempt_one FOR TABLE attempt_rows")
    connection.query("CREATE PUBLICATION attempt_two FOR TABLE attempt_rows,attempt_extra")
    connection.query("CREATE PUBLICATION attempt_empty FOR TABLE attempt_rows")
    connection.query("ALTER PUBLICATION attempt_empty DROP TABLE attempt_rows")
    scalar = "nextval('attempt_seq')"
    bare = "pg_get_publication_tables('attempt_one')"
    empty = "pg_get_publication_tables('attempt_empty')"
    args = "pg_get_publication_tables(CASE WHEN nextval('attempt_seq')>0 THEN 'attempt_one' ELSE 'attempt_one' END)"
    args_empty = args.replace("'attempt_one'", "'attempt_empty'")
    nested = "(pg_get_publication_tables('attempt_one')).relid IS NOT NULL"
    cases = (
        (f"{scalar},{bare}", ["1", "3"], "4", 0),
        (f"{bare},{scalar}", ["1", "3"], "4", 1),
        (f"{scalar},{empty}", [], "102", 0),
        (f"{scalar},{args}", ["1", "4"], "6", 0),
        (f"{args},{scalar}", ["2", "5"], "6", 1),
        (f"{scalar},{args_empty}", [], "104", 0),
        (f"{scalar},{nested}", ["1", "2"], "2", 0),
        (f"{scalar},{nested.replace('attempt_one', 'attempt_empty')}", [], "100", 0),
        (f"{scalar},({args}).relid IS NOT NULL", ["2", "4"], "4", 0),
        (f"{scalar},{bare},pg_get_publication_tables('attempt_two')", ["1", "2", "4", "5"], "6", 0),
    )
    for query in (connection.query, lambda sql: extended(connection, sql)):
        for targets, expected, current, column in cases:
            connection.query("SELECT setval('attempt_seq',100,true)")
            if expected:
                connection.query("SELECT setval('attempt_seq',1,false)")
            sql = f"SELECT {targets} FROM attempt_rows"
            rows = query(sql)
            assert [row[column] for row in rows] == expected, (sql, rows, expected)
            if "attempt_two" in targets:
                assert rows[1][1] is None and rows[3][1] is None, rows
            actual = connection.query("SELECT currval('attempt_seq')")
            assert actual == [[current]], (sql, actual, current)
        for suffix, expected, current in (
                ("ORDER BY x", ["1", "2"], "2"),
                ("ORDER BY 2 LIMIT 1", ["1"], "1"),
                ("ORDER BY id", ["1", "3"], "4"),
                ("ORDER BY id LIMIT 1", ["1"], "1")):
            connection.query("SELECT setval('attempt_seq',1,false)")
            sql = f"SELECT {scalar},{bare} AS x FROM attempt_rows {suffix}"
            rows = query(sql)
            assert [row[0] for row in rows] == expected, (sql, rows, expected)
            assert connection.query("SELECT currval('attempt_seq')") == [[current]], sql
    plan = "\n".join(row[0] for row in connection.query(
        f"EXPLAIN ANALYZE SELECT {scalar},{bare} FROM attempt_rows ORDER BY id"))
    scan = next(line for line in plan.splitlines() if "Seq Scan on attempt_rows" in line)
    assert "rows=2" in scan and "loops=1" in scan, plan
    # C calls the ordinary projection Result; Lisp calls it Project.
    projections = [line for line in plan.splitlines() if "ProjectSet" in line or "Project " in line]
    assert projections and all("rows=2" in line and "loops=1" in line for line in projections), plan
    # A suspended portal must run only the attempts needed by this Execute.
    connection.query("BEGIN")
    connection.query("SELECT setval('attempt_seq',1,false)")
    setup = [wire.parse("SELECT nextval($1),pg_get_publication_tables($2) FROM attempt_rows",
                        (25, 25), name=b"attempt_pull"),
             bind((), (b"attempt_seq", b"attempt_one"), portal=b"attempt_pull", statement=b"attempt_pull")]
    expect_kinds(cycle(connection, setup), [b"1", b"2", b"Z"])
    for expected in ("1", "3"):
        result = cycle(connection, [execute(1, b"attempt_pull")])
        expect_kinds(result, [b"D", b"s", b"Z"])
        assert connection.data_row(result[0][1])[0] == expected, result
        assert connection.query("SELECT currval('attempt_seq')") == [[expected]]
    expect_kinds(cycle(connection, [execute(1, b"attempt_pull")]), [b"C", b"Z"])
    assert connection.query("SELECT currval('attempt_seq')") == [["4"]]
    connection.query("ROLLBACK")
    print("project-set-attempts: Simple/Extended empty, bare, nested, volatile arguments and uneven sets PASS")


def run():
    server = None if len(sys.argv) > 1 else wire.OwnedLisp()
    connection = None
    try:
        if server:
            server.start()
        connection = wire.Connection(int(sys.argv[1]) if server is None else server.port)
        check(connection)
    finally:
        if connection:
            connection.close()
        if server:
            server.stop()
            print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()

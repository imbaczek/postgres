"""Verify composite B-tree paths through the SQL wire protocol."""

import argparse
import contextlib
import datetime
import itertools
import json
import runpy
import shutil
import socket
import struct
import subprocess
import tempfile
from pathlib import Path


fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
OwnedLisp, Connection = fixture["OwnedLisp"], fixture["Connection"]


def populate(connection):
    connection.query("CREATE TABLE bounds (tenant int, seq int, payload text)")
    payload = "x" * 250
    for start in range(0, 5000, 250):
        tuples = ",".join(
            f"({i // 100},{i % 100},'{payload}')" for i in range(start, start + 250)
        )
        connection.query("INSERT INTO bounds VALUES " + tuples)
    connection.query("INSERT INTO bounds VALUES (12, NULL, 'null key')")
    connection.query("CREATE INDEX bounds_ix ON bounds (tenant, seq)")
    connection.query("ANALYZE bounds")


def verify_bounds(connection):
    base = "SELECT seq FROM bounds WHERE tenant = 12"
    bounded = base + " AND 30 <= seq AND seq < 35"
    assert "Index Scan using bounds_ix" in connection.query("EXPLAIN " + bounded)[0][0]
    assert sorted(connection.query(bounded)) == [[str(n)] for n in range(30, 35)]
    assert connection.query(bounded + " AND seq <= 29") == []

    for direction, expected in (("ASC", [["30"], ["31"]]), ("DESC", [["34"], ["33"]])):
        query = bounded + f" ORDER BY seq {direction} LIMIT 2"
        plan = connection.query("EXPLAIN " + query)[0][0]
        assert "Index Scan using bounds_ix" in plan and plan.startswith("Limit"), plan
        assert connection.query(query) == expected
        scan = json.loads(connection.query("EXPLAIN (FORMAT JSON) " + query)[0][0])[0]["Plan"]
        while scan["Node Type"] != "Index Scan":
            [scan] = scan["Plans"]
        assert scan["Scan Direction"] == ("Forward" if direction == "ASC" else "Backward")

    nullable = base + " ORDER BY seq DESC LIMIT 2"
    assert connection.query(nullable) == [[None], ["99"]]
    assert not connection.query("EXPLAIN " + nullable)[0][0].startswith("Limit")

    for order, expected in (
        ("ASC NULLS FIRST", [[None], ["0"]]),
        ("DESC NULLS LAST", [["99"], ["98"]]),
    ):
        query = base + f" ORDER BY seq {order} LIMIT 2"
        plan = connection.query("EXPLAIN " + query)[0][0]
        assert "Index Scan using bounds_ix" in plan and plan.startswith("Limit"), plan
        assert connection.query(query) == expected


def verify_suspended_range(connection):
    query = b"SELECT seq FROM bounds WHERE tenant = 12 AND seq < 35 ORDER BY seq DESC LIMIT 3"
    connection.send(b"P", b"range_stmt\0" + query + bytes(3))
    connection.send(b"B", b"range_portal\0range_stmt\0" + bytes(6))
    for value in (34, 33, 32):
        connection.send(b"E", b"range_portal\0" + struct.pack("!I", 1))
        connection.send(b"H", b"")
        while True:
            kind, body = connection.receive()
            assert kind != b"E", body
            if kind == b"D":
                assert body == struct.pack("!HI", 1, 2) + str(value).encode()
                break
        kind, _ = connection.receive()
        assert kind in ((b"s", b"C") if value == 32 else (b"s",))
    connection.send(b"S", b"")
    assert connection.receive()[0] == b"Z"


def verify_parameter_ranges(connection):
    query = b"SELECT seq FROM bounds WHERE tenant = $1 AND seq >= $2 ORDER BY seq LIMIT 2"
    connection.send(b"P", b"parameter_range\0" + query + b"\0" + struct.pack("!HII", 2, 23, 23))
    for tenant, lower, expected in ((12, 30, [30, 31]), (38, 99, [99]), (12, None, [])):
        values = b""
        for value in (tenant, lower):
            encoded = None if value is None else str(value).encode()
            values += struct.pack("!i", -1 if encoded is None else len(encoded))
            values += encoded or b""
        connection.send(b"B", b"\0parameter_range\0" + struct.pack("!HH", 0, 2) + values + bytes(2))
        connection.send(b"E", bytes(5))
        connection.send(b"S", b"")
        rows = []
        while True:
            kind, body = connection.receive()
            assert kind != b"E", body
            if kind == b"D":
                rows.append(int(body[6:]))
            if kind == b"Z":
                break
        assert rows == expected


def split_fixture_rows():
    return [(i % 5, (i * 37) % 31, i) for i in range(3500)]


def populate_split_ranges(connection):
    connection.query("CREATE TABLE range_changes (tenant int, seq int, marker int)")
    connection.query("CREATE INDEX range_changes_ix ON range_changes (tenant, seq)")
    assert connection.query(
        "SELECT seq FROM range_changes WHERE tenant = 2 AND seq >= 4 ORDER BY seq LIMIT 7"
    ) == []
    rows = split_fixture_rows()
    for start in range(0, len(rows), 250):
        connection.query("INSERT INTO range_changes VALUES " + ",".join(
            f"({tenant},{seq},{marker})" for tenant, seq, marker in rows[start:start + 250]
        ))
    connection.query("UPDATE range_changes SET seq = seq + 100 WHERE tenant = 2 AND marker < 600")
    connection.query("DELETE FROM range_changes WHERE tenant = 2 AND marker >= 2500")
    connection.query("VACUUM range_changes")
    connection.query("ANALYZE range_changes")


def verify_split_ranges(connection):
    keys = [
        seq + (100 if marker < 600 else 0)
        for tenant, seq, marker in split_fixture_rows() if tenant == 2 and marker < 2500
    ]
    for direction in ("ASC", "DESC"):
        for lower, upper in ((4, 18), (26, 28), (30, 105), (130, 150)):
            expected = [[str(seq)] for seq in sorted(
                (seq for seq in keys if lower <= seq < upper), reverse=direction == "DESC"
            )[:7]]
            query = (f"SELECT seq FROM range_changes WHERE tenant = 2 AND seq >= {lower} AND seq < {upper} "
                     f"ORDER BY seq {direction} LIMIT 7")
            plan = connection.query("EXPLAIN " + query)[0][0]
            assert plan.startswith("Limit") and "Index Scan using range_changes_ix" in plan, plan
            heap_query = query.replace("tenant = 2", "tenant + 0 = 2").replace("ORDER BY seq", "ORDER BY seq + 0")
            assert "Seq Scan" in connection.query("EXPLAIN " + heap_query)[0][0]
            assert connection.query(query) == connection.query(heap_query) == expected, query


def verify_timestamp_acceptance(connection):
    connection.query("CREATE TABLE events (id int, tenant_id int, created_at timestamp)")
    epoch = datetime.datetime(2026, 11, 1)
    for start in range(0, 16000, 200):
        values = ",".join(
            f"({i},{42 if i % 8 == 0 else 43},'{epoch + datetime.timedelta(seconds=i - 1200)}')"
            for i in range(start, start + 200)
        )
        connection.query("INSERT INTO events VALUES " + values)
    connection.query("CREATE INDEX events_tenant_time ON events (tenant_id, created_at)")
    connection.query("ANALYZE events")
    query = ("SELECT id, created_at FROM events WHERE tenant_id = 42 "
             "AND created_at >= '2026-11-01'::timestamp ORDER BY created_at LIMIT 100")
    plan = connection.query("EXPLAIN " + query)[0][0]
    assert plan.startswith("Limit") and "Index Scan using events_tenant_time" in plan, plan
    expected = [[str(i), str(epoch + datetime.timedelta(seconds=i - 1200))] for i in range(1200, 2000, 8)]
    assert connection.query(query) == expected


@contextlib.contextmanager
def reference_server(binary):
    with tempfile.TemporaryDirectory(prefix="pg-btree-range-reference-") as directory:
        root = Path(directory)
        data = root / "data"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        subprocess.run([binary / "initdb", "-D", data, "-A", "trust", "-U", "postgres",
                        "--no-locale", "--encoding=UTF8"],
                       check=True, stdout=subprocess.DEVNULL, timeout=60)
        ctl = [binary / "pg_ctl", "-D", data, "-w"]
        subprocess.run(ctl + ["-l", root / "server.log", "-o",
                              f"-p {port} -h 127.0.0.1 -k {root}", "start"],
                       check=True, stdout=subprocess.DEVNULL, timeout=60)
        try:
            with contextlib.closing(Connection(port)) as connection:
                yield connection
        finally:
            subprocess.run(ctl + ["-m", "fast", "stop"],
                           check=True, stdout=subprocess.DEVNULL, timeout=60)


def verify_reference(connection, reference):
    populate(reference)
    for peer in (connection, reference):
        for endpoint in (20, 40):
            peer.query("INSERT INTO bounds VALUES " + ",".join(
                f"(12,{endpoint},'dup')" for _ in range(700)
            ))
        peer.query("ANALYZE bounds")
    predicates = ("", " AND seq > 20", " AND seq >= 20", " AND seq < 40",
                  " AND seq <= 40", " AND seq > 40 AND seq < 20",
                  " AND seq > 20 AND seq < 40", " AND seq >= 20 AND seq <= 40")
    cases = itertools.product(predicates, ("ASC", "DESC"),
                              ("", " NULLS FIRST", " NULLS LAST"), (1, 10, 1200))
    for predicate, direction, nulls, limit in cases:
        query = (f"SELECT seq FROM bounds WHERE tenant=12{predicate}"
                 f" ORDER BY seq {direction}{nulls} LIMIT {limit}")
        assert connection.query(query) == reference.query(query), query
    print("144 composite range queries matched C PostgreSQL")


def main(reference_bin=None):
    server = OwnedLisp()
    try:
        server.start()
        connection = Connection(server.port)
        try:
            populate(connection)
            verify_bounds(connection)
            verify_suspended_range(connection)
            verify_parameter_ranges(connection)
            populate_split_ranges(connection)
            verify_split_ranges(connection)
            verify_timestamp_acceptance(connection)
        finally:
            connection.close()
        server.stop(immediate=True)
        server.start()
        connection = Connection(server.port)
        try:
            verify_bounds(connection)
            verify_split_ranges(connection)
            if reference_bin:
                with reference_server(reference_bin) as reference:
                    verify_reference(connection, reference)
        finally:
            connection.close()
    finally:
        server.stop()
        shutil.rmtree(server.root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-bin", type=Path)
    main(parser.parse_args().reference_bin)

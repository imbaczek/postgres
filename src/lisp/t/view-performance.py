"""Measure view query CPU, RSS and Lisp allocations against C PostgreSQL."""

import argparse
import contextlib
from functools import partial
from pathlib import Path
import random
import runpy
import shutil
import socket
import subprocess
import tempfile


support = runpy.run_path(str(Path(__file__).with_name("foreign-key-performance.py")))
Connection = support["Connection"]
OwnedLisp = support["OwnedLisp"]
measure = support["measure"]

TYPES = {
    "int4": ("integer", str),
    "int8": ("bigint", str),
    "uuid": ("uuid", lambda value: f"'01234567-89ab-cdef-0123-{value:012x}'"),
    "varchar127": ("varchar(127)", lambda value: f"'{value:08d}" + "p" * 119 + "'"),
}


@contextlib.contextmanager
def c_server(binaries):
    with tempfile.TemporaryDirectory(prefix="pg-view-c-") as root:
        data = Path(root) / "data"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        subprocess.run([binaries / "initdb", "-A", "trust", "-U", "postgres", "-D", data],
                       check=True, stdout=subprocess.DEVNULL)
        command = [binaries / "pg_ctl", "-w", "-D", data]
        subprocess.run(command + ["-l", Path(root) / "postgres.log",
                                  "-o", f"-p {port} -h 127.0.0.1", "start"],
                       check=True, stdout=subprocess.DEVNULL)
        try:
            yield port
        finally:
            subprocess.run(command + ["stop", "-m", "fast"], check=True,
                           stdout=subprocess.DEVNULL)


def dispersed_uuid(value, rows):
    word = f"{value * ((1 << 128) // rows):032x}"
    return f"'{word[:8]}-{word[8:12]}-{word[12:16]}-{word[16:20]}-{word[20:]}'"


def install_workload(connection, kind, literal, rows, shuffled):
    connection.query(f"CREATE TABLE view_input (id int, payload {kind}, bucket int)")
    order = range(rows)
    if shuffled:
        order = list(order)
        random.Random(11).shuffle(order)
    for start in range(0, rows, 500):
        values = ",".join(f"({row}, {literal(row)}, {row % 11})"
                          for row in order[start:start + 500])
        connection.query(f"INSERT INTO view_input VALUES {values}")
    connection.query("ANALYZE view_input")
    connection.query("CREATE VIEW visible_payload AS SELECT id, payload, bucket FROM view_input")
    connection.query("CREATE VIEW nested_payload AS SELECT * FROM visible_payload")
    connection.query(
        "CREATE VIEW bounded_payload AS "
        "SELECT * FROM visible_payload ORDER BY payload DESC LIMIT 128"
    )
    connection.query("CREATE TABLE selected_buckets (bucket int)")
    connection.query("INSERT INTO selected_buckets VALUES (1), (3), (7)")


def queries(suite, lower):
    if suite == "primary":
        for phase, relation in (("direct", "view_input"), ("view", "visible_payload"),
                                ("nested", "nested_payload")):
            yield phase, f"SELECT count(*) FROM {relation} WHERE payload >= {lower}"
    else:
        yield "bounded", "SELECT count(*) FROM bounded_payload WHERE bucket = 7"
        yield "join", (
            "SELECT count(*) FROM nested_payload JOIN selected_buckets USING (bucket) "
            f"WHERE payload >= {lower}"
        )


def run_case(c_port, lisp_server, name, rows, samples, suite, allocations, shuffled):
    kind, literal = TYPES[name]
    if shuffled and name == "uuid":
        # Spread ordered values across the UUID range so comparisons also
        # exercise differences in the leading bytes.
        literal = partial(dispersed_uuid, rows=rows)
    start = rows // 2
    expected = {
        "direct": rows - start, "view": rows - start, "nested": rows - start,
        "bounded": sum(row % 11 == 7 for row in range(max(0, rows - 128), rows)),
        "join": sum(row % 11 in (1, 3, 7) for row in range(start, rows)),
    }
    workload = tuple(queries(suite, literal(start)))
    with contextlib.closing(Connection(c_port)) as c, \
            contextlib.closing(Connection(lisp_server.port)) as lisp:
        for connection in (c, lisp):
            install_workload(connection, kind, literal, rows, shuffled)
            for phase, sql in workload:
                assert connection.query(sql) == [[str(expected[phase])]], (name, phase)
        engines = ((c, int(c.query("SELECT pg_backend_pid()")[0][0]), None),
                   (lisp, lisp_server.process.pid, lisp_server))
        for sample in range(1, samples + 1):
            # Keep each engine's query order while alternating which runs first.
            for connection, pid, server in (engines if sample % 2 else reversed(engines)):
                for phase, sql in workload:
                    measure(connection, pid, server, sample, name, phase, sql, allocations)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-bin", type=Path, required=True)
    parser.add_argument("--suite", choices=("primary", "holdout"), default="primary")
    parser.add_argument("--case", choices=TYPES)
    parser.add_argument("--rows", type=int, default=12000)
    parser.add_argument("--samples", type=int, default=9)
    parser.add_argument("--shuffled", action="store_true",
                        help="shuffle insertion order and spread UUIDs across their range")
    parser.add_argument("--skip-allocation", action="store_true")
    args = parser.parse_args()
    if args.rows < 1 or args.samples < 1:
        parser.error("--rows and --samples must be positive")
    for name in (args.case,) if args.case else TYPES:
        with c_server(args.postgres_bin) as port:
            server = OwnedLisp()
            try:
                server.start()
                run_case(port, server, name, args.rows, args.samples,
                         args.suite, not args.skip_allocation, args.shuffled)
            finally:
                server.stop()
                shutil.rmtree(server.root)


if __name__ == "__main__":
    main()

"""Check C PostgreSQL's separate physical-slot feedback horizons."""

import argparse
import importlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import psycopg2


peer = importlib.import_module("backend-admission")
BASE = Path(__file__).resolve().parents[2]


def check_catalog_advancement(sql, slot, primary_port, standby_port):
    reader = psycopg2.connect(host="127.0.0.1", port=standby_port, user="postgres", dbname="postgres")
    try:
        reader.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with reader.cursor() as cursor:
            cursor.execute("SELECT oid FROM pg_class LIMIT 1")
            assert cursor.fetchone() is not None
        catalog = ("SELECT catalog_xmin::text FROM pg_replication_slots "
                   "WHERE slot_name='local_catalog'")
        before = int(sql(standby_port, catalog))
        data = slot("xmin")
        for _ in range(8):
            sql(primary_port, "SELECT pg_current_xact_id()")
        boundary = sql(primary_port, "SELECT pg_log_standby_snapshot()")
        peer.wait(lambda: sql(standby_port,
                              f"SELECT pg_last_wal_replay_lsn() >= '{boundary}'::pg_lsn") == "t", 20)
        sql(standby_port, f"SELECT pg_replication_slot_advance('local_catalog','{boundary}')")
        after = int(sql(standby_port, catalog))
        assert after > before, (before, after)
        peer.wait(lambda: slot("xmin, catalog_xmin") == f"{data}|{after}", 20)
        print("catalog-horizon-advanced-with-data-pinned", data, before, after, flush=True)
    finally:
        reader.close()


def exercise(root, binary, primary_port, standby_port, processes):
    primary, standby = root / "primary", root / "standby"
    primary.mkdir()
    standby.mkdir()

    def command(tool, *args, **options):
        return subprocess.run([binary / tool, *args], check=True, timeout=60, **options)

    def sql_command(port, query):
        return [binary / "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
                "-p", str(port), "-U", "postgres", "-d", "postgres", "-Atc", query]

    def sql(port, query):
        return subprocess.run(sql_command(port, query), check=True, timeout=5,
                              capture_output=True, text=True).stdout.strip()

    def start(node, port, label):
        return peer.start(BASE, node, binary / "postgres", port, label, False, processes)

    def slot(columns):
        return sql(primary_port, f"SELECT {columns} FROM pg_replication_slots WHERE slot_name = 'feedback_probe'")

    def check(expected, phase):
        columns = "active, xmin IS NOT NULL, catalog_xmin IS NOT NULL"
        peer.wait(lambda: slot(columns) == expected, 20)
        print(phase, expected, flush=True)

    def configure(value):
        sql(standby_port, f"ALTER SYSTEM SET hot_standby_feedback={value}")
        assert sql(standby_port, "SELECT pg_reload_conf()") == "t"

    command("initdb", "-D", primary / "data", "-A", "trust", "-U", "postgres",
            "--no-locale", stdout=subprocess.DEVNULL)
    with (primary / "data/postgresql.conf").open("a") as config:
        config.write("\nwal_level = logical\n")
    source = start(primary, primary_port, "source")
    command("pg_basebackup", "-h", "127.0.0.1", "-p", str(primary_port), "-U", "postgres",
            "-D", standby / "data", "-R", "-C", "-S", "feedback_probe")
    with (standby / "data/postgresql.auto.conf").open("a") as config:
        config.write("\nwal_receiver_status_interval = 1\nhot_standby_feedback = off\n")
    follower = start(standby, standby_port, "follower")

    check("t|f|f", "off")
    configure("on")
    check("t|t|f", "data-horizon")

    create = "SELECT pg_create_logical_replication_slot('local_catalog','pgoutput')"
    with (root / "create-slot.log").open("w") as log:
        process = subprocess.Popen(sql_command(standby_port, create), stdout=log, stderr=subprocess.STDOUT,
                                   env={**os.environ, "PGOPTIONS": "-c statement_timeout=15000"})
        try:
            deadline = time.monotonic() + 20
            while process.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError("standby slot creation did not finish")
                sql(primary_port, "SELECT pg_log_standby_snapshot()")
                time.sleep(0.2)
            assert process.returncode == 0, (root / "create-slot.log").read_text()
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    check("t|t|t", "data-and-catalog-horizons")
    check_catalog_advancement(sql, slot, primary_port, standby_port)
    peer.stop(BASE, standby, follower)
    check("f|t|t", "disconnected-slot-retains-horizons")
    retained = slot("xmin, catalog_xmin")
    sql(primary_port, "CHECKPOINT")
    peer.stop(BASE, primary, source)
    source = start(primary, primary_port, "source-restarted")
    check("f|t|t", "restarted-primary-retains-horizons")
    assert slot("xmin, catalog_xmin") == retained
    follower = start(standby, standby_port, "follower-restarted")
    check("t|t|t", "reconnected-slot-retains-horizons")
    configure("off")
    check("t|f|f", "both-horizons-cleared")
    configure("on")
    check("t|t|t", "both-horizons-restored")
    sql(standby_port, "SELECT pg_drop_replication_slot('local_catalog')")
    check("t|t|f", "catalog-horizon-cleared")
    configure("off")
    check("t|f|f", "data-horizon-cleared")
    peer.stop(BASE, standby, follower)
    peer.stop(BASE, primary, source)


def main():
    if not __debug__:
        raise RuntimeError("Run this assertion-based oracle without Python -O.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference_bin", type=Path)
    parser.add_argument("primary_port", type=int)
    parser.add_argument("standby_port", type=int)
    args = parser.parse_args()
    os.environ.pop("PGSERVICE", None)
    os.environ.pop("PGOPTIONS", None)
    os.environ.update(PGHOSTADDR="127.0.0.1", PGCONNECT_TIMEOUT="2")
    root = Path(tempfile.mkdtemp(prefix="pg-hs-feedback-"))
    print(f"hot-standby-feedback: artifacts={root}", flush=True)
    processes = []
    try:
        exercise(root, args.reference_bin.resolve(), args.primary_port, args.standby_port, processes)
    finally:
        for node in ("standby", "primary"):
            registry = root / node / "owned"
            if registry.is_dir():
                subprocess.run([sys.executable, BASE / "t/interop/owned-peer.py", "cleanup", registry],
                               check=True, timeout=30)
        for process in processes:
            process.wait(timeout=10)


if __name__ == "__main__":
    main()

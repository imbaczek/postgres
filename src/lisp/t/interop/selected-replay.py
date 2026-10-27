"""Manually replay a promoted sibling history without a network coordinator."""
import os
import runpy
import signal
import shutil
import stat
import subprocess
import sys
import time
import traceback
from pathlib import Path

import psutil

base, root = Path(sys.argv[1]), Path(sys.argv[2])
source_port, promoted_port, promoted_pid = map(int, sys.argv[3:])
replica_port = promoted_port + 1
data = root / "sibling"
contract = runpy.run_path(str(base / "t/interop/promotion-sql-contract.py"))


def control(socket, form, expect_error=False):
    result = subprocess.run([os.environ.get("SBCL", "sbcl"), "--script",
                             str(base / "t/control-client.lisp"), "unix", str(root / socket)],
                            input=form + "\n(quit)\n", capture_output=True, text=True,
                            timeout=30, check=True)
    text = result.stdout + result.stderr
    assert (";; error:" in text) == expect_error, text
    return text


def query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else None


def wait(predicate):
    deadline = time.monotonic() + 30
    while not predicate():
        assert time.monotonic() < deadline, "sibling follow barrier timed out"
        time.sleep(0.025)


def connect(port):
    connection = contract["connect"](port)
    connection.autocommit = True
    return connection


def identify_expected(system_id, timeline, lsn):
    connection = contract["psycopg2"].connect(host="127.0.0.1", port=replica_port,
        user="postgres", dbname="postgres", replication="database", connect_timeout=3)
    connection.autocommit = True
    try:
        row = query(connection, "IDENTIFY_SYSTEM")[0]
        high, low = str(row[2]).split("/")
        assert (str(row[0]), int(row[1]), (int(high, 16) << 32) + int(low, 16)) == (str(system_id), timeline, lsn), row
    finally:
        connection.close()


def interrupted(number, frame):
    raise SystemExit(128 + number)


signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)


def lisp_string(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def start_peer():
    registry = root / "extra-peers"
    registry.mkdir(mode=0o700, exist_ok=True)
    with (root / "sibling.log").open("a") as output:
        process = subprocess.Popen([sys.executable, str(base / "t/interop/owned-peer.py"),
                                    "start", str(registry), str(data), str(base / "pg-lisp.sh"),
                                    "-p", str(replica_port), "-D", str(data),
                                    "--control-socket", str(root / "sibling.sock")],
                                   stdout=output, stderr=subprocess.STDOUT)
    def ready():
        assert process.poll() is None, (root / "sibling.log").read_text()
        try:
            owner = psutil.Process(process.pid)
            return (any(c.status == psutil.CONN_LISTEN and c.laddr.port == replica_port
                        for c in owner.net_connections(kind="tcp"))
                    and any(c.laddr == str(root / "sibling.sock")
                            for c in owner.net_connections(kind="unix")))
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            return False
    wait(ready)
    assert int((data / "postmaster.pid").read_text().splitlines()[0]) == process.pid
    node = (root / "sibling.sock").lstat()
    assert stat.S_ISSOCK(node.st_mode)
    return process, (node.st_dev, node.st_ino)


def stop_peer(process, socket_identity, crash=False):
    if process.poll() is None:
        result = subprocess.run([sys.executable, str(base / "t/interop/signal-owned-server.py"),
                                 str(process.pid), str(data), "KILL" if crash else "INT",
                                 "5" if crash else "20"], timeout=25)
        if result.returncode:
            subprocess.run([sys.executable, str(base / "t/interop/signal-owned-server.py"),
                            str(process.pid), str(data), "KILL", "5"], check=True, timeout=10)
    status = process.wait(timeout=10)
    path = root / "sibling.sock"
    if path.exists():
        node = path.lstat()
        assert stat.S_ISSOCK(node.st_mode) and (node.st_dev, node.st_ino) == socket_identity
        path.unlink()
    return status


def load_helper(endpoint):
    control(endpoint, f'(load {lisp_string(base / "t/selected-replay-native.lisp")})')


peer, socket_identity = None, None
connections = []
try:
    data.mkdir(mode=0o700)
    control("source.sock", f'(physical-base-backup {lisp_string(str(data) + "/")} "sibling_seed")')
    peer, socket_identity = start_peer()
    source, promoted, sibling = [connect(p) for p in (source_port, promoted_port, replica_port)]
    connections.extend((source, promoted, sibling))
    control("sibling.sock", f'(physical-follow-start "127.0.0.1" {source_port} "sibling_seed")')
    query(source, "INSERT INTO promotion_sql_items VALUES (2,'shared-parent')")
    parent_end = query(source, "SELECT pg_current_wal_flush_lsn()")[0][0]
    for connection in (promoted, sibling):
        wait(lambda: query(connection, f"SELECT pg_last_wal_replay_lsn() >= '{parent_end}'::pg_lsn") == [(True,)])
    control("sibling.sock", '(physical-follow-stop)')
    query(sibling, "CHECKPOINT")
    original_base = {name: (data / name).read_bytes()
                     for name in ("global/pg_wal_control", "global/pg_control")}
    assert query(promoted, "SELECT pg_promote(true,30)") == [(True,)]
    query(promoted, "INSERT INTO promotion_sql_items VALUES (3,'selected-child')")
    bundle = root / "selected-frames"
    bundle.mkdir(mode=0o700)
    load_helper("target.sock")
    control("target.sock", f'(selected-replay-export #p{lisp_string(str(bundle) + "/")})')
    identity, timeline, fork, end = map(int, (bundle / "identity.txt").read_text().split())
    assert timeline == 2
    control("sibling.sock", f'(assert (= {identity} (catalog-system-identifier)))')
    control("sibling.sock", f'(assert (= {fork} *last-replay-lsn* (physical-store-flushed-lsn *physical-receive-store*)))')
    load_helper("sibling.sock")
    control("sibling.sock", f'(selected-replay-export-parent-prefix #p{lisp_string(str(bundle) + "/")})')
    assert (bundle / "source-prefix.bin").read_bytes() == (bundle / "receiver-prefix.bin").read_bytes()
    history = (bundle / "history.txt").read_text()
    eor = " ".join(map(str, (bundle / "eor.bin").read_bytes()))
    control("sibling.sock", f'(physical-recovery-select-history 2 {lisp_string(history)} ({eor}))')
    load_helper("sibling.sock")
    receive_race = os.environ.get("SELECTED_REPLAY_RECEIVE_RACE") == "1"
    if receive_race:
        for command in ("pg_wal_replay_pause", "pg_wal_replay_resume"):
            control("sibling.sock", f'(selected-replay-pause-interleaving "{command}")')
        query(sibling, "SELECT pg_wal_replay_pause()")
        control("sibling.sock", f'(selected-replay-inject-eor-after-worker-join #p{lisp_string(str(bundle) + "/")})')
        assert query(sibling, "SELECT pg_promote(true,1)") == [(False,)]
        control("sibling.sock", '(assert (and (not *physical-recovery-failed-p*) (not *physical-promotion-stopping-p*) *physical-restartpoint-worker* (sb-thread:thread-alive-p *physical-restartpoint-worker*)))')
        assert (data / "promote").is_file()
        assert query(sibling, "SELECT pg_is_wal_replay_paused()") == [(True,)]
        query(sibling, "SELECT pg_wal_replay_resume()")
        assert query(sibling, "SELECT pg_is_wal_replay_paused()") == [(False,)]
        print("selected-replay: concurrent pause gate and paused recovery restoration PASS", flush=True)
        print("selected-replay: EOR received during promotion worker stop resumes healthy recovery and retains request PASS", flush=True)
    control("sibling.sock", f'(selected-replay-inject #p{lisp_string(str(bundle) + "/")} t)')
    assert query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(2,)]
    control("sibling.sock", '(assert (= 1 (physical-recovery-replay-timeline)))')
    identify_expected(identity, 1, fork)
    assert query(sibling, "SELECT pid,status,received_tli FROM pg_catalog.pg_stat_get_wal_receiver()") == [(None, None, None)]
    sql_wait = receive_race or os.environ.get("SELECTED_REPLAY_SQL_WAIT") == "1"
    if sql_wait:
        if not receive_race:
            assert query(sibling, "SELECT pg_promote(true,1)") == [(False,)]
        control("sibling.sock", '(assert (and (not *physical-recovery-failed-p*) (not *physical-promotion-stopping-p*) (= 1 (physical-recovery-replay-timeline))))')
        assert (data / "promote").is_file()
    control("sibling.sock", '(physical-recovery-replay)')
    control("sibling.sock", '(assert (and (= 2 (physical-recovery-replay-timeline)) (not (getf (getf *physical-recovery-state* :source-reservation) :complete))))')
    assert query(sibling, "SELECT pg_is_in_recovery()") == [(True,)]
    identify_expected(identity, 2, fork + len((bundle / "eor.bin").read_bytes()))
    if os.environ.get("SELECTED_REPLAY_EOR_PROMOTION_PROBE") == "1":
        assert ':AWAITING-CHILD-INITIALIZATION' in control("sibling.sock", '(physical-recovery-promote)')
        control("sibling.sock", '(assert (and (not *physical-recovery-failed-p*) (= 2 (getf *physical-recovery-state* :phase)) (null (getf *physical-recovery-state* :promotion-child)) (not *physical-promotion-stopping-p*)))')
        print("selected-replay: promotion before child checkpoint waits without stopping recovery PASS", flush=True)
    if not sql_wait:
        sibling.close()
        assert stop_peer(peer, socket_identity, crash=True) == -signal.SIGKILL
        peer, socket_identity = start_peer()
        sibling = connect(replica_port)
        connections.append(sibling)
        control("sibling.sock", '(assert (and (= 2 (physical-recovery-replay-timeline)) (not (getf (getf *physical-recovery-state* :source-reservation) :complete))))')
        print("selected-replay: EOR-only SIGKILL restart preserves child pair, recovery exclusion and incomplete allocation PASS", flush=True)
    load_helper("sibling.sock")
    control("sibling.sock", f'(selected-replay-inject #p{lisp_string(str(bundle) + "/")})')
    retained = None
    if sql_wait:
        wait(lambda: query(sibling, "SELECT pg_is_in_recovery()") == [(False,)])
        assert query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(3,)]
        assert not (data / "promote").exists()
        print("selected-replay: same accepted SQL request survives timeout and EOR-only wait, then promotes after child checkpoint PASS", flush=True)
    else:
        control("sibling.sock", '(physical-recovery-replay)')
        assert query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(3,)]
        assert query(sibling, "SELECT pg_is_in_recovery()") == [(True,)]
        control("sibling.sock", f'(assert (= {end} *last-replay-lsn*))')
        identify_expected(identity, 2, end)
        query(sibling, "CHECKPOINT")
        sibling.close()
        assert stop_peer(peer, socket_identity, crash=True) == -signal.SIGKILL
        peer, socket_identity = start_peer()
        sibling = connect(replica_port)
        connections.append(sibling)
        assert query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(3,)]
        assert query(sibling, "SELECT pg_is_in_recovery()") == [(True,)]
        control("sibling.sock", '(assert (= 2 (physical-recovery-replay-timeline)))')
        assert all((data / name).read_bytes() == value for name, value in original_base.items())
        print("selected-replay: child checkpoint, replay, restartpoint and SIGKILL restart preserve original base PASS", flush=True)
        point = os.environ.get("SELECTED_REPLAY_PROMOTION_CRASH")
        retained = None
        if point:
            assert point in ("after-online-intent", "after-child-control")
            control("sibling.sock", f'(load {lisp_string(base / "t/child-bootstrap-native.lisp")})')
            hook = "after-child-control-directory-fsync" if point == "after-child-control" else point
            control("sibling.sock", f'(child-bootstrap-install-promotion-crash #p{lisp_string(str(root) + "/")} :{hook})')
            assert query(sibling, "SELECT pg_promote(false,30)") == [(True,)]
            marker = root / "promotion-crash-held"
            wait(lambda: marker.exists() and int(marker.read_text()) == peer.pid)
            sibling.close()
            assert stop_peer(peer, socket_identity, crash=True) == -signal.SIGKILL
            if os.environ.get("SELECTED_REPLAY_REMOVE_RETIRED") == "1":
                assert point == "after-child-control"
                for name in ("pg_physical_receive", "pg_physical_restartpoints"):
                    directory = data / name
                    assert directory.is_dir() and not directory.is_symlink()
                    shutil.rmtree(directory)
                (data / "physical-backup.manifest").unlink()
            peer, socket_identity = start_peer()
            sibling = connect(replica_port)
            connections.append(sibling)
            assert query(sibling, "SELECT pg_is_in_recovery()") == [(False,)]
            print(f"selected-replay: cold completion of base-1/parent-2 promotion after {point} PASS", flush=True)
        else:
            retained = connect(replica_port)
            retained.autocommit = False
            retained.set_session(isolation_level="REPEATABLE READ")
            connections.append(retained)
            assert query(retained, "SELECT count(*) FROM promotion_sql_items") == [(3,)]
            assert query(sibling, "SELECT pg_promote(true,30)") == [(True,)]
    assert query(sibling, "SELECT substr(pg_walfile_name(pg_current_wal_lsn()),1,8)") == [("00000003",)]
    query(sibling, "INSERT INTO promotion_sql_items VALUES (4,'own-child-three')")
    if retained:
        assert query(retained, "SELECT count(*) FROM promotion_sql_items") == [(3,)]
        assert query(retained, "SHOW transaction_read_only") == [("on",)]
        retained.commit()
        assert query(retained, "SELECT count(*) FROM promotion_sql_items") == [(4,)]
        retained.close()
        print("selected-replay: retained RR reader survives promotion from applied child 2 PASS", flush=True)
    sibling.close()
    assert stop_peer(peer, socket_identity, crash=True) == -signal.SIGKILL
    peer, socket_identity = start_peer()
    sibling = connect(replica_port)
    connections.append(sibling)
    assert query(sibling, "SELECT pg_is_in_recovery(),count(*) FROM promotion_sql_items") == [(False, 4)]
    query(sibling, "INSERT INTO promotion_sql_items VALUES (5,'own-restarted-three')")
    print("selected-replay: original base 1, applied child 2, promotion 3 and primary crash restart/write PASS", flush=True)
except BaseException:
    traceback.print_exc()
    if os.environ.get("PARKING_KEEP_ON_FAILURE") == "1" and peer is not None and peer.poll() is None:
        print(f"selected-replay: retained owned PID={peer.pid} PGDATA={data} control={root / 'sibling.sock'}; touch {root / 'debug-release'} to finish cleanup", flush=True)
        deadline = time.monotonic() + int(os.environ.get("PARKING_DEBUG_SECONDS", "600"))
        while not (root / "debug-release").exists() and time.monotonic() < deadline:
            time.sleep(0.1)
    raise
finally:
    for connection in connections:
        connection.close()
    if peer is not None:
        stop_peer(peer, socket_identity)

"""Bootstrap and restart a native standby from an already promoted primary."""
import os
import runpy
import signal
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import psutil
import psycopg2

base, root = Path(sys.argv[1]), Path(sys.argv[2])
source_port, parent_port, parent_pid = map(int, sys.argv[3:])
child_port, data = parent_port + 1, root / "child-copy"
socket_owners = {}
peer_parameters = {}
contract = runpy.run_path(str(base / "t/interop/promotion-sql-contract.py"))


def interrupted(number, frame):
    raise SystemExit(128 + number)


signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)


def query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else None


def scalar(connection, sql):
    return query(connection, sql)[0][0]


def connect(port):
    connection = contract["connect"](port)
    connection.autocommit = True
    return connection


def control(endpoint, expression, expect_error=False):
    result = subprocess.run([os.environ.get("SBCL", "sbcl"), "--script",
                             str(base / "t/control-client.lisp"), "unix", str(root / endpoint)],
                            input=expression + "\n(quit)\n", capture_output=True, text=True,
                            check=True, timeout=30)
    assert (";; error:" in result.stdout + result.stderr) == expect_error, result.stdout + result.stderr
    return result.stdout


def wait_until(predicate):
    deadline = time.monotonic() + 30
    while not predicate():
        assert time.monotonic() < deadline, "child bootstrap barrier timed out"
        time.sleep(0.025)


def start_peer(directory, port, socket_name, log_name):
    (root / "extra-peers").mkdir(mode=0o700, exist_ok=True)
    with (root / log_name).open("a") as output:
        process = subprocess.Popen([sys.executable, str(base / "t/interop/owned-peer.py"),
                                    "start", str(root / "extra-peers"), str(directory),
                                    str(base / "pg-lisp.sh"), "-p", str(port),
                                    "-D", str(directory), "--control-socket", str(root / socket_name)],
                                   stdout=output, stderr=subprocess.STDOUT)
    peer_parameters[process] = (directory, port, socket_name, log_name)
    return process


def start_child():
    process = start_peer(data, child_port, "child.sock", "child.log")
    (root / "child.pid").write_text(str(process.pid))
    return process


def ready_child(process):
    directory, port, socket_name, log_name = peer_parameters[process]
    def ready():
        assert process.poll() is None, (root / log_name).read_text()
        try:
            peer = psutil.Process(process.pid)
            return (any(item.status == psutil.CONN_LISTEN and item.laddr.port == port
                        for item in peer.net_connections(kind="tcp"))
                    and any(item.laddr == str(root / socket_name)
                            for item in peer.net_connections(kind="unix")))
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            return False
    wait_until(ready)
    assert int((directory / "postmaster.pid").read_text().splitlines()[0]) == process.pid
    node = (root / socket_name).lstat()
    assert stat.S_ISSOCK(node.st_mode)
    socket_owners[process] = (node.st_dev, node.st_ino)


def stop_child(process, crash=False):
    directory, port, socket_name, log_name = peer_parameters[process]
    if process.poll() is None:
        result = subprocess.run([sys.executable, str(base / "t/interop/signal-owned-server.py"),
                                 str(process.pid), str(directory), "KILL" if crash else "INT", "5" if crash else "20"], timeout=25)
        if result.returncode:
            subprocess.run([sys.executable, str(base / "t/interop/signal-owned-server.py"),
                            str(process.pid), str(directory), "KILL", "5"], check=True, timeout=10)
    result = process.wait(timeout=10)
    owned = socket_owners.pop(process, None)
    path = root / socket_name
    if owned and path.exists():
        node = path.lstat()
        assert stat.S_ISSOCK(node.st_mode) and (node.st_dev, node.st_ino) == owned
        path.unlink()
    return result


def fnv(bytes_):
    result = 14695981039346656037
    for byte in bytes_:
        result = ((result ^ byte) * 1099511628211) & ((1 << 64) - 1)
    return result


def backup_publication_faults():
    control("target.sock", f'(load "{base}/t/child-bootstrap-native.lisp")')
    for point in ("after-pin-binding", "after-manifest"):
        directory = root / ("fault-" + point)
        directory.mkdir(mode=0o700)
        pin = point.replace("-", "_")
        control("target.sock", f'(child-bootstrap-install-backup-fault :{point})')
        try:
            output = control("target.sock", f'(physical-base-backup "{directory}/" "{pin}")', expect_error=True)
            assert "injected child backup publication failure" in output
        finally:
            control("target.sock", '(setf *physical-bootstrap-test-hook* nil)')
        assert (directory / "recovery.required").is_file()
        fields = (root / "copy/pg_physical_pins" / (pin + ".pin")).read_text().split()
        assert len(fields) == 11 and fields[3] == "2"
        original_control = (directory / "physical-recovery.control").read_bytes()
        manifest = directory / "physical-backup.manifest"
        process = start_peer(directory, child_port + 1, "fault.sock", "fault-" + point + ".log")
        try:
            if point == "after-pin-binding":
                assert not manifest.exists()
                assert process.wait(timeout=15) != 0
                assert (directory / "physical-recovery.control").read_bytes() == original_control
            else:
                assert int(fields[-2]) == fnv(manifest.read_bytes())
                ready_child(process)
                connection = connect(child_port + 1)
                try:
                    assert scalar(connection, "SELECT pg_is_in_recovery()") is True
                    assert scalar(connection, "SELECT count(*) FROM promotion_sql_items") == 3
                finally:
                    connection.close()
        finally:
            stop_child(process)
    control("target.sock", '(physical-source-pin-load)')
    print("child-bootstrap: durable pin binding, incomplete target refusal and ambiguous completed backup PASS", flush=True)


def reject_wrong_manifest():
    control("child.sock", f'(load "{base}/t/child-bootstrap-native.lisp")')
    control("child.sock", '(child-bootstrap-install-wrong-manifest)')
    try:
        control("child.sock", f'(physical-follow-start "127.0.0.1" {parent_port} "child_seed")')
        wait_until(lambda: ":STOPPED" in control("child.sock", '(if (and *physical-follow-runtime* (physical-follow-runtime-stopped-p *physical-follow-runtime*)) :stopped :waiting)'))
        output = control("child.sock", '(physical-follow-runtime-last-error *physical-follow-runtime*)')
        assert "mismatch" in output, output
        control("child.sock", '(assert (and (not *physical-recovery-failed-p*) (= *last-replay-lsn* (getf *physical-recovery-state* :end) (physical-store-written-lsn *physical-receive-store*) (physical-store-flushed-lsn *physical-receive-store*))))')
        control("target.sock", '(assert (and (null (gethash "child_seed" *physical-source-senders*)) (null (physical-source-pin-confirmed-lsn (gethash "child_seed" *physical-source-pins*)))))')
    finally:
        control("child.sock", '(child-bootstrap-restore-query)')
        control("child.sock", '(physical-follow-stop)')
    print("child-bootstrap: wrong manifest binding rejected without receive/replay or source floor advance PASS", flush=True)


def source_restart(source_process, source_connection):
    directory = root / "source-restart-replica"
    directory.mkdir(mode=0o700)
    control("child.sock", f'(physical-base-backup "{directory}/" "source_restart")')
    replica = start_peer(directory, child_port + 1, "replica.sock", "replica.log")
    connection = None
    try:
        ready_child(replica)
        connection = connect(child_port + 1)
        control("replica.sock", f'(physical-follow-start "127.0.0.1" {child_port} "source_restart")')
        query(source_connection, "INSERT INTO promotion_sql_items VALUES (8,'source-before-crash')")
        wait_until(lambda: scalar(connection, "SELECT count(*) FROM promotion_sql_items") == 8)
        position = scalar(source_connection, "SELECT pg_current_wal_flush_lsn()")
        high, low = position.split("/")
        position = (int(high, 16) << 32) | int(low, 16)
        pin_path = data / "pg_physical_pins/source_restart.pin"
        wait_until(lambda: int(pin_path.read_text().split()[6]) >= position)
        before = pin_path.read_text().split()
        expected = fnv((directory / "physical-backup.manifest").read_bytes())
        assert before[3] == "3" and int(before[-2]) == expected
        source_connection.close()
        assert stop_child(source_process, crash=True) == -signal.SIGKILL
        source_process = start_child()
        ready_child(source_process)
        source_connection = connect(child_port)
        after = pin_path.read_text().split()
        assert after[3] == "3" and int(after[-2]) == expected
        assert int(after[6]) >= int(before[6])
        query(source_connection, "INSERT INTO promotion_sql_items VALUES (9,'source-after-crash')")
        wait_until(lambda: scalar(connection, "SELECT count(*) FROM promotion_sql_items") == 9)
        assert query(connection, "SELECT receive_start_tli,received_tli FROM pg_catalog.pg_stat_get_wal_receiver()") == [(3, 3)]
        print("child-bootstrap: promoted source crash preserves bound pin, confirmed floor and receiver reconnect PASS", flush=True)
        return source_process, source_connection
    except BaseException:
        if source_connection:
            source_connection.close()
        stop_child(source_process)
        raise
    finally:
        if connection:
            connection.close()
        stop_child(replica)


connections = [contract["connect"](port) for port in (source_port, parent_port, parent_port)]
child, child_connection = None, None
try:
    contract["check"](*connections)
    parent = connections[1]
    parent.autocommit = True
    query(parent, "INSERT INTO promotion_sql_items VALUES (3,'child2')")
    if os.environ.get("CHILD_BOOTSTRAP_BACKUP_FAULTS") == "1":
        backup_publication_faults()
    data.mkdir(mode=0o700)
    output = control("target.sock", f'(physical-base-backup "{data}/" "child_seed")')
    (root / "child-backup.log").write_text(output)
    assert ":TIMELINE 2" in output
    assert (data / "pg_wal/00000002.history").read_bytes() == (root / "copy/pg_wal/00000002.history").read_bytes()
    child = start_child()
    ready_child(child)
    child_connection = connect(child_port)
    assert scalar(child_connection, "SELECT pg_is_in_recovery()") is True
    assert scalar(child_connection, "SHOW transaction_read_only") == "on"
    assert query(child_connection, "SELECT id,label FROM promotion_sql_items ORDER BY id") == [(1, "after"), (2, "new"), (3, "child2")]
    if os.environ.get("CHILD_BOOTSTRAP_FORCE_TIMEOUT") == "1":
        (root / "timeout-ready").write_text(str(child.pid))
        print(f"child-bootstrap: forced timeout peer ready PID={child.pid}", flush=True)
        time.sleep(60)
    if os.environ.get("CHILD_BOOTSTRAP_WRONG_MANIFEST") == "1":
        reject_wrong_manifest()
    control("child.sock", f'(physical-follow-start "127.0.0.1" {parent_port} "child_seed")')
    query(parent, "INSERT INTO promotion_sql_items VALUES (4,'streamed2')")
    wait_until(lambda: scalar(child_connection, "SELECT count(*) FROM promotion_sql_items") == 4)
    assert query(child_connection, "SELECT receive_start_tli,received_tli FROM pg_catalog.pg_stat_get_wal_receiver()") == [(2, 2)]
    query(child_connection, "CHECKPOINT")
    child_connection.close()
    child_connection = None
    assert stop_child(child, crash=os.environ.get("CHILD_BOOTSTRAP_CRASH") == "1") == (-signal.SIGKILL if os.environ.get("CHILD_BOOTSTRAP_CRASH") == "1" else 0)
    child = None
    query(parent, "INSERT INTO promotion_sql_items VALUES (5,'restart2')")
    child = start_child()
    ready_child(child)
    child_connection = connect(child_port)
    wait_until(lambda: scalar(child_connection, "SELECT count(*) FROM promotion_sql_items") == 5)
    assert query(child_connection, "SELECT receive_start_tli,received_tli FROM pg_catalog.pg_stat_get_wal_receiver()") == [(2, 2)]
    assert scalar(child_connection, "SELECT pg_is_in_recovery()") is True
    print("child-bootstrap: timeline-2 backup, receive/replay, restartpoint and reconnect PASS", flush=True)
    if os.environ.get("CHILD_BOOTSTRAP_PROMOTE") == "1":
        point = os.environ.get("CHILD_BOOTSTRAP_PROMOTION_CRASH")
        if point:
            assert point in ("after-online-intent", "after-child-control")
            control("child.sock", f'(load "{base}/t/child-bootstrap-native.lisp")')
            hook = "after-child-control-directory-fsync" if point == "after-child-control" else point
            control("child.sock", f'(child-bootstrap-install-promotion-crash #p"{root}/" :{hook})')
            assert scalar(child_connection, "SELECT pg_promote(false,30)") is True
            marker = root / "promotion-crash-held"
            wait_until(lambda: marker.exists() and int(marker.read_text()) == child.pid)
            child_connection.close()
            child_connection = None
            assert stop_child(child, crash=True) == -signal.SIGKILL
            if os.environ.get("CHILD_BOOTSTRAP_REMOVE_RETIRED") == "1":
                assert point == "after-child-control"
                for name in ("pg_physical_receive", "pg_physical_restartpoints"):
                    directory = data / name
                    assert directory.is_dir() and not directory.is_symlink()
                    shutil.rmtree(directory)
                (data / "physical-backup.manifest").unlink()
                print("child-bootstrap: removed retired spool, generations and manifest after commit", flush=True)
            child = start_child()
            ready_child(child)
            child_connection = connect(child_port)
            assert scalar(child_connection, "SELECT pg_is_in_recovery()") is False
            print(f"child-bootstrap: cold timeline-3 completion after {point} PASS", flush=True)
        else:
            assert scalar(child_connection, "SELECT pg_promote(true,30)") is True
        assert scalar(child_connection, "SELECT substr(pg_walfile_name(pg_current_wal_lsn()),1,8)") == "00000003"
        query(child_connection, "INSERT INTO promotion_sql_items VALUES (6,'child3')")
        child_connection.close()
        child_connection = None
        assert stop_child(child, crash=os.environ.get("CHILD_BOOTSTRAP_CRASH") == "1") == (-signal.SIGKILL if os.environ.get("CHILD_BOOTSTRAP_CRASH") == "1" else 0)
        child = start_child()
        ready_child(child)
        child_connection = connect(child_port)
        assert scalar(child_connection, "SELECT pg_is_in_recovery()") is False
        assert scalar(child_connection, "SELECT substr(pg_walfile_name(pg_current_wal_lsn()),1,8)") == "00000003"
        assert scalar(child_connection, "SELECT count(*) FROM promotion_sql_items") == 6
        query(child_connection, "INSERT INTO promotion_sql_items VALUES (7,'restart3')")
        print("child-bootstrap: promotion to timeline 3, primary restart and child writes PASS", flush=True)
        if os.environ.get("CHILD_BOOTSTRAP_SOURCE_RESTART") == "1":
            child, child_connection = source_restart(child, child_connection)
except BaseException:
    if os.environ.get("CHILD_BOOTSTRAP_FORCE_TIMEOUT") == "1":
        time.sleep(30)
    if child is not None and child.poll() is None:
        try:
            output = control("child.sock", '(list :phase (getf *physical-recovery-state* :phase) :failed *physical-recovery-failed-p* :follow-error (and *physical-follow-runtime* (physical-follow-runtime-last-error *physical-follow-runtime*)))')
            print(output, flush=True)
            (root / "child-diagnostic.log").write_text(output)
        except Exception as error:
            print(f"child-bootstrap: diagnostic failed: {error}", flush=True)
    raise
finally:
    (root / "promotion-crash-release").touch()
    if child_connection:
        child_connection.close()
    if child:
        stop_child(child)
    for connection in connections:
        connection.close()

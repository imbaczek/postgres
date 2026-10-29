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

if not __debug__:
    raise RuntimeError('selected replay tests require Python assertions')

import psutil
import follow_stream_wire

base, root = Path(sys.argv[1]), Path(sys.argv[2])
source_port, promoted_port, promoted_pid = map(int, sys.argv[3:])
replica_port = promoted_port + 1
data = root / "sibling"
contract = runpy.run_path(str(base / "t/interop/promotion-sql-contract.py"))
peer_parameters = {}


def control(socket, form, expect_error=False):
    result = subprocess.run([os.environ.get("SBCL", "sbcl"), "--script",
                             str(base / "t/control-client.lisp"), "unix", str(root / socket)],
                            input=form + "\n(quit)\n", capture_output=True, text=True,
                            timeout=30, check=True)
    text = result.stdout + result.stderr
    assert ";; read error:" not in text, text
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


def start_peer(directory=None, port=None, socket_name="sibling.sock", log_name="sibling.log"):
    directory = directory or data
    port = replica_port if port is None else port
    registry = root / "extra-peers"
    registry.mkdir(mode=0o700, exist_ok=True)
    with (root / log_name).open("a") as output:
        process = subprocess.Popen([sys.executable, str(base / "t/interop/owned-peer.py"),
                                    "start", str(registry), str(directory), str(base / "pg-lisp.sh"),
                                    "-p", str(port), "-D", str(directory),
                                    "--control-socket", str(root / socket_name)],
                                   stdout=output, stderr=subprocess.STDOUT)
    peer_parameters[process] = (directory, socket_name)
    def ready():
        assert process.poll() is None, (root / log_name).read_text()
        try:
            owner = psutil.Process(process.pid)
            return (any(c.status == psutil.CONN_LISTEN and c.laddr.port == port
                        for c in owner.net_connections(kind="tcp"))
                    and any(c.laddr == str(root / socket_name)
                            for c in owner.net_connections(kind="unix")))
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            return False
    wait(ready)
    assert int((directory / "postmaster.pid").read_text().splitlines()[0]) == process.pid
    node = (root / socket_name).lstat()
    assert stat.S_ISSOCK(node.st_mode)
    return process, (node.st_dev, node.st_ino)


def stop_peer(process, socket_identity, crash=False):
    directory, socket_name = peer_parameters[process]
    if process.poll() is None:
        result = subprocess.run([sys.executable, str(base / "t/interop/signal-owned-server.py"),
                                 str(process.pid), str(directory), "KILL" if crash else "INT",
                                 "5" if crash else "20"], timeout=25)
        if result.returncode:
            subprocess.run([sys.executable, str(base / "t/interop/signal-owned-server.py"),
                            str(process.pid), str(directory), "KILL", "5"], check=True, timeout=10)
    status = process.wait(timeout=10)
    path = root / socket_name
    if path.exists():
        node = path.lstat()
        assert stat.S_ISSOCK(node.st_mode) and (node.st_dev, node.st_ino) == socket_identity
        path.unlink()
    return status


def load_helper(endpoint):
    control(endpoint, f'(load {lisp_string(base / "t/selected-replay-native.lisp")})')


def restart_promoted_source(connection, pid=None):
    pid = promoted_pid if pid is None else pid
    directory = root / "copy"
    path = root / "target.sock"
    owner = psutil.Process(pid)
    assert any(c.laddr == str(path) for c in owner.net_connections(kind="unix"))
    assert any(c.status == psutil.CONN_LISTEN and c.laddr.port == promoted_port
               for c in owner.net_connections(kind="tcp"))
    original = path.lstat()
    assert stat.S_ISSOCK(original.st_mode)
    connection.close()
    subprocess.run([sys.executable, str(base / "t/interop/signal-owned-server.py"),
                     str(pid), str(directory), "KILL", "5"], check=True, timeout=10)
    for process in peer_parameters:
        if process.pid == pid:
            assert process.wait(timeout=10) == -signal.SIGKILL
    if pid == promoted_pid:
        (root / "expected-target-sigkill").write_text(str(promoted_pid) + "\n")
    if path.exists():
        current = path.lstat()
        assert stat.S_ISSOCK(current.st_mode)
        assert (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino)
        path.unlink()
    process, identity = start_peer(directory, promoted_port, "target.sock", "target-restarted.log")
    return process, identity, connect(promoted_port)


def reject_bootstrap_for_anchor(system_id, fork, manifest, seed):
    connection = contract["psycopg2"].connect(host="127.0.0.1", port=promoted_port,
        user="postgres", dbname="postgres", replication="database", connect_timeout=3)
    connection.autocommit = True
    query_text = (f"START_REPLICATION PHYSICAL {fork >> 32:X}/{fork & 0xffffffff:08X} "
                  f"(native_format 'pg_lisp_wal_7', proof_pin 'selected_follow', system_id '{system_id}', timeline '2', "
                  f"base_lsn '{fork >> 32:X}/{fork & 0xffffffff:08X}', prefix_digest '{seed}', manifest_digest '{manifest}')")
    try:
        try:
            query(connection, query_text)
        except contract["psycopg2"].Error as error:
            assert error.pgcode == "55000", error
        else:
            raise AssertionError("follow anchor admitted a bootstrap stream")
    finally:
        connection.close()


def exercise_named_streams(promoted, port, system_id, base, fork, manifest, seed,
                           source_prefix, history, eor):
    public = "wire_public_slot"

    def state():
        active, restart = query(promoted, "SELECT active,restart_lsn FROM pg_replication_slots "
                                "WHERE slot_name='wire_public_slot'")[0]
        number = None if restart is None else sum(
            int(part, 16) << shift for part, shift in zip(restart.split("/"), (32, 0)))
        return active, number

    query(promoted, "SELECT * FROM pg_create_physical_replication_slot('wire_public_slot')")
    assert state() == (False, None)
    follow_stream_wire.at_fork(port, system_id, base, fork, manifest, seed,
                               slot="wire_follow", public_slot=public)
    assert state() == (False, None), "exact-fork completion reserved a public slot without feedback"
    follow_stream_wire.early_done(port, system_id, base, fork, fork, manifest, seed,
                                  2, "wire_follow", public_slot=public)
    assert state() == (False, None), "CopyDone before proof feedback reserved the public slot"
    follow_stream_wire.prepare(port, "named_behind", system_id, base, base, fork,
                               manifest, 14695981039346656037, source_prefix, history, eor)
    digest = follow_stream_wire.parent_suffix(port, system_id, base, fork, manifest,
                                               source_prefix, proof_pin="named_behind", public_slot=public)
    assert state() == (False, fork)
    query(promoted, "SELECT * FROM pg_replication_slot_advance('wire_public_slot','"
          + follow_stream_wire.lsn(fork + len(eor)) + "')")
    assert state() == (False, fork + len(eor))

    def observe(client):
        wait(lambda: state() == (True, fork))
        assert state() == (True, fork), "public retention followed write/apply instead of flush"
        follow_stream_wire.rejects_public_slot(port, system_id, base, fork, manifest, seed, public, "55006")
        follow_stream_wire.rejects_public_slot(port, system_id, base, fork, manifest, seed,
                                                "missing_public_slot", "42704")
        assert state() == (True, fork)
        control("target.sock", '(assert (and (not (gethash "wire_follow" *physical-source-senders*)) '
                '(= 1 (wal-replication-slot-restart-timeline (gethash "wire_public_slot" *wal-replication-slots*)))))')

    live_sender = follow_stream_wire.child_eor(port, system_id, base, fork, manifest, digest, eor,
                                               proof_pin="named_behind", public_slot=public,
                                               observe=observe, hold=True)
    try:
        print("named-stream: exact claims, feedback-only flush, backward reconnect, fork timeline and CopyDone cleanup PASS", flush=True)
        return public, live_sender
    except BaseException:
        live_sender.close()
        raise


def retarget_form(port, prefix):
    if os.environ.get('FOLLOW_LIBPQ') == '1':
        conninfo = (f'host=127.0.0.1 port={port} user=postgres dbname=postgres '
                    'sslmode=disable gssencmode=disable password=private-acceptance-marker')
        return (f'(physical-follow-retarget-conninfo "{conninfo}" '
                f':prefix "{prefix}" :fallback "controller-libpq")')
    return f'(physical-follow-retarget "127.0.0.1" {port} "{prefix}")'


def exercise_same_timeline(upstream, sibling, port, timeline, count, other=None):
    control("sibling.sock", '(defparameter *same-follow-selection* (copy-tree (physical-store-selection *physical-receive-store*)))')
    control("sibling.sock", retarget_form(port, f'same_{timeline}'))
    control("sibling.sock", f'(let* ((binding (fifth (physical-follow-read-config))) (anchor (getf binding :anchor))) (assert (and (eq :same-timeline (physical-follow-anchor-kind anchor)) (= {timeline} (getf anchor :source-timeline) (getf anchor :receive-timeline) (physical-store-selected-timeline *physical-receive-store*)) (equal (getf binding :before) (getf binding :after)) (equal *same-follow-selection* (physical-store-selection *physical-receive-store*)))))')
    query(upstream, "INSERT INTO promotion_sql_items VALUES (9,'same-timeline-probe')")
    readers = [sibling] + ([other] if other is not None else [])
    for reader in readers:
        wait(lambda: query(reader, "SELECT count(*) FROM promotion_sql_items") == [(count + 1,)])
    query(upstream, "DELETE FROM promotion_sql_items WHERE id=9")
    for reader in readers:
        wait(lambda: query(reader, "SELECT count(*) FROM promotion_sql_items") == [(count,)])
    wait(lambda: ':CLEANUP-COUNT 0' in control("sibling.sock", '(physical-follow-controller-status)'))
    print(f"follow-controller: same-timeline {timeline} retarget preserves selection and streams new WAL PASS", flush=True)


def exercise_controller(promoted, sibling):
    control("sibling.sock", '(defparameter *controller-original-runtime* *physical-follow-runtime*)')
    control("sibling.sock", retarget_form(1, 'unreachable'), expect_error=True)
    control("sibling.sock", '(assert (and (eq *controller-original-runtime* *physical-follow-runtime*) (not *physical-recovery-failed-p*)))')
    control("sibling.sock", retarget_form(promoted_port, 'manual'))
    wait(lambda: query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(3,)])
    control("sibling.sock", '(assert (and (not *physical-recovery-failed-p*) (= 2 (physical-recovery-replay-timeline)) (eq :active (getf (fifth (physical-follow-read-config)) :phase))))')
    if os.environ.get('FOLLOW_LIBPQ') == '1':
        control('sibling.sock', "(assert (typep (physical-follow-runtime-client *physical-follow-runtime*) 'physical-libpq-channel))")
        control('sibling.sock', '(assert (physical-follow-config-source (physical-follow-read-config)))')
        status = control('sibling.sock', '(physical-follow-controller-status)')
        assert 'private-acceptance-marker' not in status
        conninfo = query(sibling, 'SELECT * FROM pg_catalog.pg_stat_get_wal_receiver()')[0][-1]
        assert 'controller-libpq' in conninfo and 'private-acceptance-marker' not in conninfo, (
            'fallback visible', 'controller-libpq' in conninfo,
            'private fixture visible', 'private-acceptance-marker' in conninfo)
        assert stat.S_IMODE((data / 'physical-follow.control').stat().st_mode) == 0o600
    query(sibling, "CHECKPOINT")
    query(promoted, "INSERT INTO promotion_sql_items VALUES (6,'controller-live')")
    wait(lambda: query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(4,)])
    wait(lambda: ':CLEANUP-COUNT 0' in control("sibling.sock", '(physical-follow-controller-status)'))
    print("follow-controller: public retarget, child receipt/replay and asynchronous pin retirement PASS", flush=True)
    exercise_same_timeline(promoted, sibling, promoted_port, 2, 4)


def exercise_receiver_only_stop(promoted, sibling):
    load_helper("sibling.sock")
    control("sibling.sock", '(defparameter *receiver-only-replay-thread* (physical-follow-runtime-replay-thread *physical-follow-runtime*))')
    query(sibling, "SELECT pg_wal_replay_pause()")
    query(promoted, "DELETE FROM promotion_sql_items WHERE id=6")
    delete_flush = query(promoted, "SELECT pg_current_wal_flush_lsn()::text")[0][0]
    wait(lambda: query(sibling, "SELECT pg_last_wal_receive_lsn()>='" + delete_flush + "'::pg_lsn") == [(True,)])
    assert query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(4,)]
    control("sibling.sock", '(selected-replay-receiver-action nil)')
    wait(lambda: ':RUNNING' not in control("sibling.sock", '*selected-replay-receiver-result*'))
    control("sibling.sock", '(assert (eq :done *selected-replay-receiver-result*))')
    assert query(sibling, "SELECT pid,status,received_tli FROM pg_catalog.pg_stat_get_wal_receiver()") == [(None, None, None)]
    assert query(sibling, "SELECT pg_is_wal_replay_paused()") == [(True,)]
    query(sibling, "SELECT pg_wal_replay_resume()")
    wait(lambda: query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(3,)])
    control("sibling.sock", '(assert (and (eq *receiver-only-replay-thread* (physical-follow-runtime-replay-thread *physical-follow-runtime*)) (sb-thread:thread-alive-p *receiver-only-replay-thread*) (not *physical-recovery-failed-p*)))')
    control("sibling.sock", '(selected-replay-receiver-action t)')
    wait(lambda: ':RUNNING' not in control("sibling.sock", '*selected-replay-receiver-result*'))
    control("sibling.sock", '(assert (eq :done *selected-replay-receiver-result*))')
    query(promoted, "INSERT INTO promotion_sql_items VALUES (6,'receiver-resumed')")
    wait(lambda: query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(4,)])
    print("follow-controller: receiver-only stop preserves pause and replays buffered WAL, then resumes receipt PASS", flush=True)


def exercise_controller_parent_catchup():
    directory = root / "controller-lagging"
    process, identity = start_peer(directory, replica_port + 1, "lagging.sock", "lagging.log")
    connection = connect(replica_port + 1)
    try:
        control("lagging.sock", retarget_form(promoted_port, 'too_early'), expect_error=True)
        control("lagging.sock", '(assert (not *physical-recovery-failed-p*))')
        control("lagging.sock", '(physical-recovery-enable-streaming)')
        control("lagging.sock", f'(physical-follow-save-config (list 1 "127.0.0.1" {source_port} "controller_lag_seed"))')
        control("lagging.sock", '(defparameter *controller-attempts* nil)')
        control("lagging.sock", '(setf *physical-follow-test-hook* (lambda (point) (when (eq point :after-controller-proof) (let ((entry (getf (sixth (physical-follow-read-config)) :preparing))) (push (list (getf entry :pin) (getf (getf entry :request) :receive-lsn)) *controller-attempts*)))))')
        control("lagging.sock", retarget_form(promoted_port, 'catchup'))
        wait(lambda: query(connection, "SELECT count(*) FROM promotion_sql_items") == [(4,)])
        control("lagging.sock", '(assert (= 2 (length *controller-attempts*)))')
        control("lagging.sock", '(assert (not (equal (first (first *controller-attempts*)) (first (second *controller-attempts*)))))')
        control("lagging.sock", '(assert (< (second (second *controller-attempts*)) (second (first *controller-attempts*))))')
        control("lagging.sock", '(assert (and (= 2 (physical-recovery-replay-timeline)) (not *physical-recovery-failed-p*)))')
        wait(lambda: ':CLEANUP-COUNT 0' in control("lagging.sock", '(physical-follow-controller-status)'))
        print("follow-controller: lagging parent CopyDone and automatic fresh-pin child handoff PASS", flush=True)
    finally:
        connection.close()
        stop_peer(process, identity)


peer, socket_identity = None, None
source_peer, source_socket_identity = None, None
connections = []
try:
    data.mkdir(mode=0o700)
    control("source.sock", f'(physical-base-backup {lisp_string(str(data) + "/")} "sibling_seed")')
    if os.environ.get("SELECTED_REPLAY_CONTROLLER") == "1":
        lagging = root / "controller-lagging"
        lagging.mkdir(mode=0o700)
        control("source.sock", f'(physical-base-backup {lisp_string(str(lagging) + "/")} "controller_lag_seed")')
    peer, socket_identity = start_peer()
    source, promoted, sibling = [connect(p) for p in (source_port, promoted_port, replica_port)]
    connections.extend((source, promoted, sibling))
    control("sibling.sock", f'(physical-follow-start "127.0.0.1" {source_port} "sibling_seed")')
    query(source, "INSERT INTO promotion_sql_items VALUES (2,'shared-parent')")
    parent_end = query(source, "SELECT pg_current_wal_flush_lsn()")[0][0]
    for connection in (promoted, sibling):
        wait(lambda: query(connection, f"SELECT pg_last_wal_replay_lsn() >= '{parent_end}'::pg_lsn") == [(True,)])
    if os.environ.get("SELECTED_REPLAY_CONTROLLER") == "1":
        exercise_same_timeline(source, sibling, source_port, 1, 2, promoted)
    if os.environ.get("SELECTED_REPLAY_CONTROLLER") != "1":
        control("sibling.sock", '(physical-follow-stop)')
    query(sibling, "CHECKPOINT")
    original_base = {name: (data / name).read_bytes()
                     for name in ("global/pg_wal_control", "global/pg_control")}
    promotion_slot_restart = query(
        sibling, "SELECT lsn FROM pg_create_physical_replication_slot('promotion_reserved',true)")[0][0]
    query(sibling, "SELECT pg_create_physical_replication_slot('promotion_unreserved')")
    assert query(promoted, "SELECT pg_promote(true,30)") == [(True,)]
    query(promoted, "INSERT INTO promotion_sql_items VALUES (3,'selected-child')")
    if os.environ.get("SELECTED_REPLAY_CONTROLLER") == "1":
        exercise_controller(promoted, sibling)
        exercise_receiver_only_stop(promoted, sibling)
        exercise_controller_parent_catchup()
        sibling.close()
        assert stop_peer(peer, socket_identity, crash=True) == -signal.SIGKILL
        peer, socket_identity = start_peer()
        sibling = connect(replica_port)
        connections.append(sibling)
        assert query(sibling, "SELECT pg_is_in_recovery(),count(*) FROM promotion_sql_items") == [(True, 4)]
        query(promoted, "INSERT INTO promotion_sql_items VALUES (7,'controller-restarted')")
        wait(lambda: query(sibling, "SELECT count(*) FROM promotion_sql_items") == [(5,)])
        if os.environ.get("SELECTED_REPLAY_CONTROLLER_CUT") == "1":
            control("sibling.sock", f'(load {lisp_string(base / "t/child-bootstrap-native.lisp")})')
            control("sibling.sock", f'(child-bootstrap-install-promotion-crash #p{lisp_string(str(root) + "/")} :after-retire-follow-transfer)')
            assert query(sibling, "SELECT pg_promote(false,30)") == [(True,)]
            marker = root / "promotion-crash-held"
            wait(lambda: marker.exists() and int(marker.read_text()) == peer.pid)
            transferred = (data / "physical-follow.control").read_bytes()
            assert b":CLEANUP" in transferred and b":ANCHOR" not in transferred
            assert (data / "physical-backup.manifest").exists()
            assert (data / "physical-recovery.control").exists()
            sibling.close()
            assert stop_peer(peer, socket_identity, crash=True) == -signal.SIGKILL
            peer, socket_identity = start_peer()
            sibling = connect(replica_port)
            connections.append(sibling)
            assert query(sibling, "SELECT pg_is_in_recovery(),count(*) FROM promotion_sql_items") == [(False, 5)]
            print("follow-controller: SIGKILL after cleanup transfer, before recovery artifact deletion PASS", flush=True)
        else:
            assert query(sibling, "SELECT pg_promote(true,30)") == [(True,)]
        control("sibling.sock", '(assert (physical-follow-cleanup-only-p (physical-follow-read-config)))')
        wait(lambda: ':CLEANUP-COUNT 0' in control("sibling.sock", '(physical-follow-controller-status)'))
        wait(lambda: ':IDLE' in control("sibling.sock", '(if (sb-thread:thread-alive-p *physical-follow-controller-worker*) :running :idle)'))
        print("follow-controller: active SIGKILL restart and promotion cleanup transfer PASS", flush=True)
        sibling.close()
        assert stop_peer(peer, socket_identity, crash=True) == -signal.SIGKILL
        peer, socket_identity = start_peer()
        sibling = connect(replica_port)
        connections.append(sibling)
        assert query(sibling, "SELECT pg_is_in_recovery(),count(*) FROM promotion_sql_items") == [(False, 5)]
        query(sibling, "INSERT INTO promotion_sql_items VALUES (8,'controller-primary-restarted')")
        wait(lambda: ':IDLE' in control("sibling.sock", '(if (sb-thread:thread-alive-p *physical-follow-controller-worker*) :running :idle)'))
        print("follow-controller: cleanup-only primary SIGKILL restart/write PASS", flush=True)
        raise SystemExit(0)
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
    if os.environ.get("SELECTED_REPLAY_ANCHOR") == "1":
        control("sibling.sock", f'(selected-replay-export-follow-request #p{lisp_string(str(bundle) + "/")} 2)')
        system_id, original_end, manifest, seed = map(int, (bundle / "follow-summary.txt").read_text().split())
        assert system_id == identity and original_end < fork
        control("target.sock", f'(selected-replay-bad-follow-overlap #p{lisp_string(str(bundle) + "/")})')
        cut = os.environ.get("SELECTED_REPLAY_ANCHOR_CUT")
        if cut:
            assert cut in ("after-follow-retention", "after-follow-proof", "after-follow-ready")
            control("target.sock", f'(selected-replay-install-follow-cut #p{lisp_string(str(root) + "/")} :{cut})')
            control("target.sock", f'(selected-replay-start-follow-pin #p{lisp_string(str(bundle) + "/")})')
            marker = root / "follow-cut-held"
            wait(lambda: marker.exists() and int(marker.read_text()) == promoted_pid)
        else:
            control("target.sock", f'(selected-replay-follow-pin #p{lisp_string(str(bundle) + "/")})')
            reject_bootstrap_for_anchor(system_id, fork, manifest, seed)
        for name in ("pg_physical_receive", "pg_physical_restartpoints"):
            retired = root / "copy" / name
            assert retired.is_dir() and not retired.is_symlink()
            shutil.rmtree(retired)
        source_peer, source_socket_identity, promoted = restart_promoted_source(promoted)
        connections.append(promoted)
        load_helper("target.sock")
        expected_stage = "retaining" if cut in ("after-follow-retention", "after-follow-proof") else "ready"
        control("target.sock", f'(assert (eq :{expected_stage} (physical-follow-pin-stage (gethash "selected_follow" *physical-source-pins*))))')
        control("target.sock", f'(selected-replay-follow-pin #p{lisp_string(str(bundle) + "/")})')
        reject_bootstrap_for_anchor(system_id, fork, manifest, seed)
        control("target.sock", '(reload)')
        source_prefix = (bundle / "source-prefix.bin").read_bytes()
        source_history = (bundle / "history.txt").read_text()
        source_eor = (bundle / "eor.bin").read_bytes()
        follow_stream_wire.prepare_rejections(
            promoted_port, system_id, original_end, fork, manifest, seed, source_prefix)
        control("target.sock", '(assert (null (gethash "wire_rejected" *physical-source-pins*)))')
        wire_anchor = follow_stream_wire.prepare(
            promoted_port, "wire_follow", system_id, original_end, fork, fork,
            manifest, seed, source_prefix, source_history, source_eor)
        follow_stream_wire.prepare(
            promoted_port, "selected_behind", system_id, original_end, original_end, fork,
            manifest, 14695981039346656037, source_prefix, source_history, source_eor)
        follow_stream_wire.at_fork(promoted_port, system_id, original_end, fork, manifest, seed)
        follow_stream_wire.rejects_unbound_requests(
            promoted_port, system_id, original_end, fork, manifest, seed)
        follow_stream_wire.early_done(promoted_port, system_id, original_end, original_end,
                                      fork, manifest, 14695981039346656037, 1,
                                      "selected_behind", after_data=True)
        follow_stream_wire.early_done(promoted_port, system_id, original_end, fork,
                                      fork, manifest, seed, 2, "selected_follow")
        follow_stream_wire.malformed_copy_done(
            promoted_port, system_id, original_end, fork, manifest, seed)
        wait(lambda: ":DONE" in control("target.sock", '(if (gethash "selected_follow" *physical-source-senders*) :waiting :done)'))
        control("target.sock", f'(assert (= {original_end} (physical-source-pin-confirmed-lsn (gethash "selected_behind" *physical-source-pins*))))')
        behind_digest = follow_stream_wire.parent_suffix(
            promoted_port, system_id, original_end, fork, manifest,
            (bundle / "source-prefix.bin").read_bytes())
        named_public, named_sender = exercise_named_streams(
            promoted, promoted_port, system_id, original_end,
            fork, manifest, seed, source_prefix, source_history, source_eor)
        connections.append(named_sender)
        follow_stream_wire.child_eor(promoted_port, system_id, original_end, fork, manifest, seed,
                                     (bundle / "eor.bin").read_bytes())
        wait(lambda: ":DONE" in control("target.sock", '(if (gethash "selected_follow" *physical-source-senders*) :waiting :done)'))
        assert query(promoted, "SELECT active FROM pg_replication_slots "
                     "WHERE slot_name='wire_public_slot'") == [(True,)]
        source_peer, source_socket_identity, promoted = restart_promoted_source(promoted, source_peer.pid)
        named_sender.close()
        connections.append(promoted)
        assert query(promoted, "SELECT active,restart_lsn='" + follow_stream_wire.lsn(fork)
                     + "'::pg_lsn FROM pg_replication_slots WHERE slot_name='wire_public_slot'") == [(False, True)]
        control("target.sock", '(assert (= 1 (wal-replication-slot-restart-timeline '
                '(gethash "wire_public_slot" *wal-replication-slots*))))')
        query(promoted, "SELECT pg_drop_replication_slot('" + named_public + "')")
        print("named-stream: SIGKILL with an active sender preserves the public slot and clears runtime ownership PASS", flush=True)
        assert follow_stream_wire.prepare(
            promoted_port, "wire_follow", system_id, original_end, fork, fork,
            manifest, seed, source_prefix, source_history, source_eor) == wire_anchor
        follow_stream_wire.at_fork(promoted_port, system_id, original_end, fork, manifest,
                                   seed, slot="wire_follow")
        load_helper("target.sock")
        control("target.sock", f'(selected-replay-probe-native-client #p{lisp_string(str(bundle) + "/")} {promoted_port})')
        print("native-client: typed PREPARE/history, exact-fork START, fsynced CopyDone, checked pin retirement and canceled preparation PASS", flush=True)
        print("follow-prepare: source-owned proof, exact retry and durable READY after SIGKILL PASS", flush=True)
        follow_stream_wire.at_fork(promoted_port, system_id, original_end, fork, manifest,
                                   behind_digest, slot="selected_behind")
        print("follow-stream: bounded parent suffix, CopyDone, next timeline, child EOR and source crash/resume PASS", flush=True)
        assert query(promoted, "SELECT pg_is_in_recovery(),count(*) FROM promotion_sql_items") == [(False, 3)]
        print(f"follow-anchor: real sibling origin, independent overlap, refusal of bootstrap mode and source SIGKILL recovery ({cut or 'ready'}) PASS", flush=True)
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
        query(sibling, "SELECT pg_create_physical_replication_slot('promotion_temporary',true,true)")
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
    assert query(sibling, "SELECT slot_name,restart_lsn,active FROM pg_replication_slots "
                 "WHERE slot_name IN ('promotion_reserved','promotion_unreserved') ORDER BY slot_name") == [
                     ("promotion_reserved", promotion_slot_restart, False),
                     ("promotion_unreserved", None, False)]
    if not sql_wait:
        assert query(sibling, "SELECT active FROM pg_replication_slots "
                     "WHERE slot_name='promotion_temporary'") == ([] if point else [(True,)])
    print("selected-replay: physical slot restart floors and temporary ownership survive promotion PASS", flush=True)
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
    assert query(sibling, "SELECT slot_name,restart_lsn,active FROM pg_replication_slots "
                 "WHERE slot_name IN ('promotion_reserved','promotion_unreserved') ORDER BY slot_name") == [
                     ("promotion_reserved", promotion_slot_restart, False),
                     ("promotion_unreserved", None, False)]
    assert query(sibling, "SELECT slot_name FROM pg_replication_slots "
                 "WHERE slot_name='promotion_temporary'") == []
    query(sibling, "INSERT INTO promotion_sql_items VALUES (5,'own-restarted-three')")
    print("selected-replay: original base 1, applied child 2, promotion 3 and primary crash restart/write PASS", flush=True)
except BaseException as error:
    if isinstance(error, SystemExit) and error.code == 0:
        raise
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
    if source_peer is not None:
        stop_peer(source_peer, source_socket_identity)

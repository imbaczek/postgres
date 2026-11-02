"""Require a primary's final shutdown checkpoint to reach its live standby."""
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

import psycopg2


def lsn(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) + int(low, 16)


def query(connection, statement):
    with connection.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall() if cursor.description else None


def connect(port):
    connection = psycopg2.connect(host='127.0.0.1', port=port, user='postgres',
                                  dbname='postgres', connect_timeout=2)
    connection.autocommit = True
    return connection


def lisp_string(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def control_peer(base, root, name, pid, form):
    result = subprocess.run([os.environ.get('SBCL', 'sbcl'), '--script',
                             str(base / 't/control-client.lisp'), 'unix', str(root / f'{name}.sock')],
                            input=f'(assert (= (sb-posix:getpid) {pid}))\n{form}\n(quit)\n',
                            text=True, capture_output=True, check=True, timeout=30)
    assert ';; error:' not in result.stdout, result.stdout


def exercise(base, root, source_port, target_port, target_pid):
    scenario = os.environ.get('SHUTDOWN_STREAMING_SCENARIO', 'stream')
    assert scenario in ('stream', 'no-stream', 'quit', 'checkpoint-cut', 'second-disconnect')
    source_data = root / 'source'
    source_pid = int((source_data / 'postmaster.pid').read_text().splitlines()[0])
    assert int((root / 'copy/postmaster.pid').read_text().splitlines()[0]) == target_pid
    source = connect(source_port)
    target = connect(target_port)
    try:
        query(source, "INSERT INTO promotion_sql_items VALUES (2,'before-shutdown')")
        before = lsn(query(source, 'SELECT pg_current_wal_flush_lsn()::text')[0][0])
        deadline = time.monotonic() + 30
        while query(target, 'SELECT count(*) FROM promotion_sql_items') != [(2,)]:
            assert time.monotonic() < deadline, 'standby did not replay the final user transaction'
            time.sleep(0.02)
        if scenario == 'no-stream':
            control_peer(base, root, 'target', target_pid, '(physical-follow-stop)')
        else:
            control_peer(base, root, 'target', target_pid,
                         f'(progn (load {lisp_string(base / "t/shutdown-streaming-native.lisp")}) '
                         f'(shutdown-streaming-install-feedback-barrier #p{lisp_string(str(root) + "/")} {before}))')
        control_peer(base, root, 'source', source_pid,
                     f'(progn (load {lisp_string(base / "t/shutdown-streaming-native.lisp")}) '
                     f'(shutdown-streaming-install-parked-worker #p{lisp_string(str(root) + "/")}) '
                     f'(shutdown-streaming-record-outcomes #p{lisp_string(str(root) + "/")}))')
        if scenario == 'second-disconnect':
            control_peer(base, root, 'target', target_pid,
                         f'(shutdown-streaming-second-parameters #p{lisp_string(str(root) + "/")})')
            base_lsn, manifest, cursor = map(int, (root / 'second-parameters').read_text().split())
            assert cursor == before
            control_peer(base, root, 'source', source_pid,
                         f'(let ((pin (physical-source-pin-create "second_shutdown"))) '
                         f'(assert (= {base_lsn} (physical-source-pin-consistent-lsn pin))) '
                         f'(physical-source-pin-bind-manifest pin {manifest}) (values))')
            control_peer(base, root, 'target', target_pid,
                         f'(shutdown-streaming-start-second-client #p{lisp_string(str(root) + "/")} '
                         f'{source_port} {base_lsn} {before})')
            deadline = time.monotonic() + 10
            while not (root / 'second-ready').exists():
                assert not (root / 'second-error').exists(), (root / 'second-error').read_text()
                assert time.monotonic() < deadline, 'secondary stream did not start'
                time.sleep(0.02)
        if scenario == 'checkpoint-cut':
            control_peer(base, root, 'source', source_pid,
                         f'(shutdown-streaming-install-checkpoint-cut #p{lisp_string(str(root) + "/")})')
        deadline = time.monotonic() + 10
        while not (root / 'parked-ready').exists():
            assert not (root / 'parked-error').exists(), (root / 'parked-error').read_text()
            assert time.monotonic() < deadline, 'worker did not park before shutdown'
            time.sleep(0.02)
        source.close()
        shutdown_signal = os.environ.get('SHUTDOWN_STREAMING_SIGNAL', 'INT')
        assert shutdown_signal in ('INT', 'TERM')
        descriptor = os.pidfd_open(source_pid)
        stopping = subprocess.Popen([sys.executable, str(base / 't/interop/signal-owned-server.py'),
                                     str(source_pid), str(source_data), shutdown_signal, '30'])
        try:
            deadline = time.monotonic() + 10
            while not (root / 'parked-drain-gate').exists():
                assert stopping.poll() is None, 'primary exited before draining parked work'
                assert time.monotonic() < deadline, 'shutdown did not reopen admission for parked cleanup'
                time.sleep(0.02)
            pending_control = subprocess.check_output([str(base / 'bin/pg_controldata'), '-D', str(source_data)],
                                                      text=True, timeout=10)
            assert 'Database cluster state:               in production' in pending_control, pending_control
            assert not (root / 'flush-held').exists(), 'checkpoint advanced before the parked worker drained'
            assert not select.select([descriptor], [], [], 0.2)[0], 'primary exited with a parked operation'
            (root / 'release-parked').touch()
            if scenario != 'no-stream':
                barrier = 'checkpoint-cut' if scenario == 'checkpoint-cut' else 'flush-held'
                deadline = time.monotonic() + 15
                while not (root / barrier).exists():
                    assert stopping.poll() is None, f'primary exited before {barrier}'
                    assert time.monotonic() < deadline, f'barrier timed out: {barrier}'
                    time.sleep(0.02)
                if scenario != 'checkpoint-cut':
                    written, flushed = map(int, (root / 'flush-held').read_text().split())
                    endpoint = int((root / 'shutdown-endpoint').read_text())
                    assert written >= flushed >= endpoint > before
                pending_control = subprocess.check_output([str(base / 'bin/pg_controldata'), '-D', str(source_data)],
                                                          text=True, timeout=10)
                assert 'Database cluster state:               shutting down' in pending_control, pending_control
                assert not select.select([descriptor], [], [], 0.2)[0], 'primary exited without final flush acknowledgement'
                if scenario == 'second-disconnect':
                    deadline = time.monotonic() + 10
                    while not (root / 'second-final').exists():
                        assert not (root / 'second-error').exists(), (root / 'second-error').read_text()
                        assert time.monotonic() < deadline, 'secondary stream did not receive shutdown WAL'
                        time.sleep(0.02)
                    assert int((root / 'second-final').read_text()) >= endpoint > before
                    (root / 'release-second').touch()
                    while not (root / 'second-closed').exists():
                        assert time.monotonic() < deadline, 'secondary stream did not disconnect'
                        time.sleep(0.02)
                    assert not (root / 'second-error').exists(), (root / 'second-error').read_text()
                    assert not select.select([descriptor], [], [], 0.2)[0], 'one disconnect bypassed the remaining sender'
                if scenario in ('quit', 'checkpoint-cut'):
                    signal.pidfd_send_signal(descriptor, signal.SIGQUIT)
                    assert select.select([descriptor], [], [], 5)[0], 'SIGQUIT did not interrupt sender drain'
        finally:
            (root / 'release-parked').touch()
            (root / 'release-feedback').touch()
            os.close(descriptor)
            stopping.wait(timeout=35)
        assert stopping.returncode == 0, f'owned primary shutdown failed: {stopping.returncode}'
        (root / 'expected-source-shutdown').write_text(str(source_pid))
        control = subprocess.check_output([str(base / 'bin/pg_controldata'), '-D', str(source_data)],
                                          text=True, timeout=60)
        if scenario in ('quit', 'checkpoint-cut'):
            assert 'Database cluster state:               shutting down' in control, control
        else:
            assert 'Database cluster state:               shut down' in control, control
        if scenario == 'second-disconnect':
            assert sorted((root / 'sender-outcomes').read_text().split()) == ['ABANDONED', 'ACKNOWLEDGED']
        checkpoint_start = lsn(next(line.split(':', 1)[1].strip() for line in control.splitlines()
                                    if line.startswith('Latest checkpoint location:')))
        checkpoint = int((root / 'shutdown-endpoint').read_text())
        assert checkpoint_start < checkpoint, 'control checkpoint location is a start, not its durable end'
        assert checkpoint > before, 'shutdown did not publish a final checkpoint'
        positions = None
        deadline = time.monotonic() + 10
        while scenario not in ('no-stream', 'checkpoint-cut') and time.monotonic() < deadline:
            positions = query(target, 'SELECT pg_last_wal_receive_lsn()::text,pg_last_wal_replay_lsn()::text')[0]
            if all(value is not None and lsn(value) >= checkpoint for value in positions):
                break
            time.sleep(0.05)
        else:
            if scenario not in ('no-stream', 'checkpoint-cut'):
                raise AssertionError(f'shutdown checkpoint {checkpoint:X} was not delivered: '
                                     f'standby receive/replay={positions}, previous durable={before:X}')
        if scenario == 'checkpoint-cut':
            received = query(target, 'SELECT pg_last_wal_receive_lsn()::text')[0][0]
            assert lsn(received) < checkpoint, 'unpublished shutdown target was sent'
        assert query(target, 'SELECT pg_is_in_recovery(),count(*) FROM promotion_sql_items') == [(True, 2)]
        if scenario == 'no-stream':
            assert (root / 'parked-terminal').exists()
            assert not (root / 'parked-resumed').exists()
        else:
            assert (root / 'parked-resumed').exists()
        assert not (root / 'parked-error').exists(), (root / 'parked-error').read_text()
        if scenario != 'no-stream':
            control_peer(base, root, 'target', target_pid, '(shutdown-streaming-remove-feedback-barrier)')
        print('shutdown-streaming: parked backendless work drained before the final checkpoint PASS', flush=True)
        if scenario == 'no-stream':
            print('shutdown-streaming: parked work with an empty sender plan preserves clean shutdown PASS', flush=True)
        elif scenario in ('quit', 'checkpoint-cut'):
            print(f'shutdown-streaming: SIGQUIT during {scenario} exits promptly and preserves dirty control PASS', flush=True)
        else:
            print('shutdown-streaming: final flush feedback held primary alive and unclean until acknowledgement PASS', flush=True)
            print(f'shutdown-streaming: final checkpoint {checkpoint:X} received and replayed after clean primary shutdown PASS', flush=True)
            if scenario == 'second-disconnect':
                print('shutdown-streaming: one abandoned sender cannot bypass the remaining sender acknowledgement PASS', flush=True)
    finally:
        (root / 'release-parked').touch()
        (root / 'release-feedback').touch()
        (root / 'release-checkpoint-cut').touch()
        (root / 'release-second').touch()
        source.close()
        target.close()


if __name__ == '__main__':
    assert __debug__, 'shutdown streaming test requires Python assertions'
    exercise(Path(sys.argv[1]), Path(sys.argv[2]), *map(int, sys.argv[3:]))

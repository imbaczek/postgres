"""Check native FPW startup, reload, unchanged policy and cold reconciliation."""
from contextlib import ExitStack
import json
from pathlib import Path
import runpy
import signal
import time

import psycopg2


if not __debug__:
    raise RuntimeError('This assertion-based oracle requires Python without -O.')

HERE = Path(__file__).resolve().parent
cold = runpy.run_path(str(HERE / 'source-fpw-cold.py'))
api = cold['api']


def state(server):
    path = server.root / 'fpw-config-state.txt'
    server.control(f'''(let ((state (sb-thread:with-mutex (*wal-lock*)
                                     (wal-checked-fpw-state-under-lock))))
      (assert state)
      (with-open-file (out {cold['lisp_string'](path)} :direction :output :if-exists :supersede)
        (format out "~d ~d ~d ~a ~d ~d~%"
          (if (wal-fpw-state-enabled state) 1 0)
          (wal-fpw-state-start state) (wal-fpw-state-end state)
          (wal-fpw-state-witness-kind state)
          (primary-wal-writer-cycles *primary-wal-writer*)
          (if (eq *fpw-config-test-root* (startup-capture-configuration)) 0 1))))''')
    enabled, start, end, kind, cycles, changed = path.read_text().split()
    return dict(enabled=int(enabled), start=int(start), end=int(end), kind=kind,
                cycles=int(cycles), root_changed=int(changed))


def remember_root(server):
    server.control('(progn (defparameter *fpw-config-test-root* (startup-capture-configuration)) (values))')


def checkpoint_now(connection):
    with connection.cursor() as cursor:
        cursor.execute('CHECKPOINT')


def reload_to(server, connection, value):
    remember_root(server)
    before = state(server)
    (server.data / 'postgresql.auto.conf').write_text(f'full_page_writes={value}\n')
    server.process.send_signal(signal.SIGHUP)
    api['follow']['wait_for'](
        lambda: api['scalar'](connection, 'SHOW full_page_writes') == value)
    expected = int(value == 'on')
    def installed():
        current = state(server)
        return (current['enabled'] == expected and current['root_changed'] == 1
                and current['cycles'] > before['cycles'] + 1)
    api['follow']['wait_for'](installed)
    return before, state(server)


def check():
    started = time.monotonic()
    server = api['peers']['OwnedLisp']()
    server.data.mkdir(mode=0o700)
    (server.data / 'postgresql.conf').write_text('full_page_writes=off\n')
    with ExitStack() as cleanup:
        cleanup.callback(server.stop, immediate=True)
        server.start()
        connection = api['peers']['connect'](server.port, server.process)
        cleanup.callback(connection.close)
        remember_root(server)
        assert api['scalar'](connection, 'SHOW full_page_writes') == 'off'
        assert state(server)['enabled'] == 0
        for statement in ('SET full_page_writes=on', 'RESET full_page_writes'):
            try:
                with connection.cursor() as cursor:
                    cursor.execute(statement)
            except psycopg2.Error as error:
                assert error.pgcode == '55P02', error
            else:
                raise AssertionError(f'SIGHUP setting accepted session command: {statement}')
        checkpoint_now(connection)
        checkpoint, _ = cold['read_copy'](server.data)
        assert checkpoint['full_page_writes'] is False
        before, after = reload_to(server, connection, 'off')
        assert (before['start'], before['end']) == (after['start'], after['end'])
        print('startup OFF, session context and unchanged reload PASS', flush=True)
        for value in ('on', 'off'):
            before, after = reload_to(server, connection, value)
            assert after['start'] > before['start'] and after['kind'] == 'CHANGE'
            checkpoint_now(connection)
            checkpoint, _ = cold['read_copy'](server.data)
            assert checkpoint['full_page_writes'] is (value == 'on')
            print(f'SIGHUP {value} and independent checkpoint PASS', flush=True)
        remember_root(server)
        before = state(server)
        log_offset = (server.root / 'server.log').stat().st_size
        (server.data / 'postgresql.auto.conf').write_text('full_page_writes=invalid\n')
        server.process.send_signal(signal.SIGHUP)
        api['follow']['wait_for'](lambda: 'configuration reload failed:' in
                                (server.root / 'server.log').read_text()[log_offset:])
        assert api['scalar'](connection, 'SHOW full_page_writes') == 'off'
        after = state(server)
        assert after['root_changed'] == 0
        assert (before['start'], before['end']) == (after['start'], after['end'])
        previous = server.process.pid
        server.process.kill()
        assert server.process.wait(timeout=20) == -signal.SIGKILL
        server.stop(immediate=True)
        (server.data / 'postgresql.auto.conf').write_text('full_page_writes=on\n')
        server.start()
        assert server.process.pid != previous
        connection = api['peers']['connect'](server.port, server.process)
        cleanup.callback(connection.close)
        remember_root(server)
        assert api['scalar'](connection, 'SHOW full_page_writes') == 'on'
        current = state(server)
        assert current['enabled'] == 1 and current['kind'] == 'CHANGE'
        assert current['start'] > before['start']
        checkpoint_now(connection)
        checkpoint, _ = cold['read_copy'](server.data)
        assert checkpoint['full_page_writes'] is True
    result = dict(result='PASS', root=str(server.root),
                  elapsed_seconds=time.monotonic() - started)
    (server.root / 'fpw-config-result.json').write_text(json.dumps(result) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    check()

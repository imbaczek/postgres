"""Lifecycle checks for suspended SELECT and RETURNING portals on an owned server."""
import json
import re
import subprocess
import sys
import time
from pathlib import Path

from binary_values import frame, parse
from returning_portals import bind, close_portal, cycle, execute, expect_kinds
from subqueries import QueryConnection
from subqueries_concurrency import assert_no_spill_files, control, spill_counter


def wait_for(predicate, label, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f'timed out waiting for {label}')


def reload_while_suspended(port, base, root, pid, sql, name):
    socket = root / 'native.sock'
    marker = root / f'{name}-reload-wait'
    log = root / 'native.log'
    peer = QueryConnection(port, f'portal-reload-{name}')
    observer = QueryConnection(port, f'portal-reload-observer-{name}')
    try:
        peer.query('BEGIN')
        expect_kinds(cycle(peer, [parse(sql, name=b'p'), bind((), portal=b'p', statement=b'p'),
                                  execute(1, b'p')]), [b'1', b'2', b'D', b's', b'Z'])
        expression = ('(setf pg::*query-execution-owner-reload-wait-hook* '
                      f'(lambda (generation) (declare (ignore generation)) '
                      f'(with-open-file (s {json.dumps(str(marker))} :direction :output '
                      ':if-exists :supersede :if-does-not-exist :create) (write-line "waiting" s))))')
        control(base, socket, expression)
        count = log.read_text(errors='replace').count('reload complete')
        subprocess.run([sys.executable, str(base / 't/interop/signal-owned-server.py'),
                        str(pid), str(root / 'native'), 'HUP'], check=True, timeout=10)
        wait_for(marker.exists, f'{name} reload wait')
        assert log.read_text(errors='replace').count('reload complete') == count
        assert observer.query('SELECT 42') == [['42']]
        # A resumed portal remains usable while reload waits. Close retires it.
        expect_kinds(cycle(peer, [execute(1, b'p')]), [b'D', b's', b'Z'])
        expect_kinds(cycle(peer, [close_portal(b'p')]), [b'3', b'Z'])
        peer.query('COMMIT')
        wait_for(lambda: log.read_text(errors='replace').count('reload complete') > count,
                 f'{name} reload completion', timeout=40)
    finally:
        try:
            peer.query('ROLLBACK')
        except Exception:
            pass
        peer.close()
        observer.close()
        control(base, socket, '(setf pg::*query-execution-owner-reload-wait-hook* nil)')


def snapshot_vacuum(port):
    reader = QueryConnection(port, 'portal-vacuum-reader')
    writer = QueryConnection(port, 'portal-vacuum-writer')
    try:
        reader.query('BEGIN')
        first = cycle(reader, [parse('SELECT id,v FROM portal_vacuum', name=b'snap'),
                               bind((), portal=b'snap', statement=b'snap'), execute(1, b'snap')])
        expect_kinds(first, [b'1', b'2', b'D', b's', b'Z'])
        assert reader.data_row(first[2][1]) == ['1', 'old-1-' + 'x' * 192]
        writer.query('DELETE FROM portal_vacuum WHERE id=400')
        writer.query('VACUUM portal_vacuum')
        result = cycle(reader, [execute(0, b'snap')])
        expect_kinds(result, [b'D'] * 399 + [b'C', b'Z'])
        rows = [reader.data_row(body) for kind, body in result if kind == b'D']
        assert [int(row[0]) for row in rows] == list(range(2, 401))
        assert rows[-1] == ['400', 'old-400-' + 'x' * 192], rows[-1]
        reader.query('ROLLBACK')
    finally:
        reader.close()
        writer.close()


def resource_cleanup(port, base, root):
    socket = root / 'native.sock'
    output = control(base, socket, '(format t "MEM=~d LIMIT=~d~%" pg::*query-work-memory-bytes* '
                     '(or pg::*query-temp-file-limit-bytes* -1))')
    match = re.search(r'MEM=(\d+) LIMIT=(-?\d+)', output)
    old_memory, old_limit = map(int, match.groups())
    peer = QueryConnection(port, 'portal-resources')
    try:
        control(base, socket, '(setf pg::*query-work-memory-bytes* 1024 '
                'pg::*query-temp-file-limit-bytes* nil)')
        before = spill_counter(base, socket)
        peer.query('BEGIN')
        select = 'SELECT id FROM portal_big ORDER BY v'
        expect_kinds(cycle(peer, [parse(select, name=b'spill_select'),
                                  bind((), portal=b'spill_select', statement=b'spill_select'),
                                  execute(1, b'spill_select')]), [b'1', b'2', b'D', b's', b'Z'])
        assert spill_counter(base, socket) > before, 'SELECT sort did not spill'
        expect_kinds(cycle(peer, [close_portal(b'spill_select')]), [b'3', b'Z'])
        peer.query('COMMIT')
        assert_no_spill_files(base, socket)
        before = spill_counter(base, socket)
        peer.query('BEGIN')
        sql = 'INSERT INTO portal_sink SELECT id,v FROM portal_big RETURNING v'
        expect_kinds(cycle(peer, [parse(sql, name=b'r1'), bind((), portal=b'r1', statement=b'r1'),
                                  execute(1, b'r1')]), [b'1', b'2', b'D', b's', b'Z'])
        assert spill_counter(base, socket) > before, 'RETURNING spool did not spill'
        # A second portal must share the backend quota with the retained first.
        usage = control(base, socket,
                        f'(let* ((b (pg::lookup-backend {peer.backend_pid})) '
                        '(q (gethash b pg::*backend-query-temp-quotas*))) '
                        '(format t "USED=~d~%" (pg::query-temp-quota-used-bytes q)))')
        used = int(re.search(r'USED=(\d+)', usage)[1])
        assert used > 0, usage
        control(base, socket, f'(setf pg::*query-temp-file-limit-bytes* {used + used // 2})')
        result = cycle(peer, [parse(sql, name=b'r2'), bind((), portal=b'r2', statement=b'r2'),
                              execute(1, b'r2')])
        assert any(kind == b'E' and body[0] == '53400' for kind, body in result), result
        peer.query('ROLLBACK')
        assert peer.query('SELECT count(*) FROM portal_sink') == [['0']]
        assert_no_spill_files(base, socket)

        # Disconnect abandons a suspended spilled RETURNING portal.
        control(base, socket, '(setf pg::*query-temp-file-limit-bytes* nil)')
        before = spill_counter(base, socket)
        peer.query('BEGIN')
        expect_kinds(cycle(peer, [parse(sql, name=b'disc'), bind((), portal=b'disc', statement=b'disc'),
                                  execute(1, b'disc')]), [b'1', b'2', b'D', b's', b'Z'])
        assert spill_counter(base, socket) > before, 'disconnect RETURNING did not spill'
        assert any(p.is_file() for p in (root / 'native' / 'pgsql_tmp').rglob('*')), 'no retained spill file'
        backend_pid = peer.backend_pid
        assert backend_pid is not None, 'backend PID was not received at startup'
        peer.close()
        peer = None
        wait_for(lambda: ':GONE' in control(
            base, socket, f'(if (pg::lookup-backend {backend_pid}) :present :gone)'),
            'disconnected backend retirement')
        wait_for(lambda: not any(p.is_file() for p in (root / 'native' / 'pgsql_tmp').rglob('*')),
                 'disconnect cleanup')
        assert_no_spill_files(base, socket)
        observer = QueryConnection(port, 'portal-disconnect-observer')
        try:
            assert observer.query('SELECT count(*) FROM portal_sink') == [['0']]
        finally:
            observer.close()
    finally:
        if peer is not None:
            peer.close()
        control(base, socket, f'(setf pg::*query-work-memory-bytes* {old_memory} '
                f'pg::*query-temp-file-limit-bytes* {"nil" if old_limit == -1 else old_limit})')


def main(port, base, root, pid):
    admin = QueryConnection(port, 'portal-lifecycle-setup')
    try:
        admin.query('CREATE TABLE portal_life(id int PRIMARY KEY)')
        admin.query('INSERT INTO portal_life VALUES (1),(2),(3)')
        admin.query('CREATE TABLE portal_vacuum(id int PRIMARY KEY,v text)')
        admin.query('INSERT INTO portal_vacuum VALUES ' + ','.join(
            f"({i},'old-{i}-{'x' * 192}')" for i in range(1, 401)))
        admin.query('CREATE TABLE portal_big(id int,v text)')
        admin.query('CREATE TABLE portal_sink(id int,v text)')
        admin.query('INSERT INTO portal_big VALUES ' + ','.join(
            f"({i},'{str(i).zfill(4)}{'x' * 120}')" for i in range(300)))
        reload_while_suspended(port, base, root, pid, 'SELECT id FROM portal_life ORDER BY id', 'select')
        reload_while_suspended(port, base, root, pid,
                               'INSERT INTO portal_sink SELECT id,v FROM portal_big RETURNING id', 'returning')
        admin.query('TRUNCATE portal_sink')
        snapshot_vacuum(port)
        resource_cleanup(port, base, root)
        print('portal-lifecycle: reload, snapshot, shared quota and cleanup PASS', flush=True)
    finally:
        admin.close()


if __name__ == '__main__':
    main(int(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), int(sys.argv[4]))

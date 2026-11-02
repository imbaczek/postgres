"""Exercise unchanged Patroni follow across promotion and explicit native reseeding."""
import argparse
from contextlib import ExitStack
import hashlib
import importlib.metadata
import os
from pathlib import Path
import runpy
import shlex
import signal
import subprocess
import time
import traceback

import psycopg2
from psycopg2.extras import PhysicalReplicationConnection

from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.async_executor import CriticalTask
from patroni.postgresql.misc import PostgresqlRole
from patroni.postgresql.mpp import Null


api = runpy.run_path(str(Path(__file__).with_name('patroni-sync-monitor.py')))
follow = api['follow']
BASE = Path(__file__).resolve().parents[2]


def member(node, name):
    port = node.connection_pool.conn_kwargs['port']
    return Member(0, name, None, {'conn_url': f'postgres://127.0.0.1:{port}/postgres'})


def rows(node):
    return node.query('SELECT id FROM public.descendant_rows ORDER BY id')


def write_row(node, value):
    # Application traffic has its own normal public search path. Patroni's
    # heartbeat pool deliberately keeps search_path=pg_catalog.
    follow['query'](int(node.connection_pool.conn_kwargs['port']),
                    f'INSERT INTO descendant_rows VALUES ({value})')


def streaming(node, slot, timeline):
    return node.query('SELECT status,slot_name,received_tli FROM pg_stat_get_wal_receiver()') == [
        ('streaming', slot, timeline)]


def promoted_history(node):
    connection = psycopg2.connect(host='127.0.0.1', user='postgres', dbname='postgres',
                                  port=node.connection_pool.conn_kwargs['port'],
                                  connection_factory=PhysicalReplicationConnection)
    try:
        cursor = connection.cursor()
        cursor.execute('IDENTIFY_SYSTEM')
        identity = cursor.fetchone()
        assert int(identity[1]) == 2, identity
        cursor.execute('TIMELINE_HISTORY 2')
        filename, content = cursor.fetchone()
        assert isinstance(content, str), type(content)
        content = content.encode('utf-8')
        (Path(node.data_dir).parent / 'promoted.history').write_bytes(content)
        entries = [line.split() for line in content.decode().splitlines() if line and not line.startswith('#')]
        assert filename == '00000002.history' and len(entries) == 1, (filename, entries)
        assert entries[0][0] == '1', entries
        return api['lsn_number'](entries[0][1])
    finally:
        connection.close()


def cold_start(node, control_socket=None):
    process = node.is_running()
    assert process is not None
    node.connection_pool.close()
    process.kill()
    process.wait(timeout=20)
    control_socket = control_socket or Path(node.data_dir).parent / 'target.sock'
    control_socket.unlink(missing_ok=True)
    assert node.start(timeout=90, role=PostgresqlRole.REPLICA) is True
    assert node.is_running().pid != process.pid


def hash_base(data):
    return {str(path.relative_to(data)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted((data / 'base').rglob('*')) if path.is_file()}


def reseed_from_promoted(node, promoted, archive, backup_label, control_socket=None):
    assert node.is_running() is None, 'reseed requires a stopped, owned node'
    node.connection_pool.close()
    data = Path(node.data_dir)
    assert not archive.exists(), archive
    data.rename(archive)
    data.mkdir(mode=0o700)
    node_control(promoted, f'(physical-base-backup "{data}/" "{backup_label}")')
    (data / 'postgresql.conf').write_text('# Explicit native reseed managed by Patroni.\n')
    (control_socket or data.parent / 'target.sock').unlink(missing_ok=True)


def descendant(source, connection, cleanup):
    promoted = api['start_follower'](connection, source, 'descendant_b', cleanup)
    sibling = api['start_follower'](connection, source, 'descendant_c', cleanup)
    follow['wait_for'](lambda: rows(promoted) == [(1,)] and rows(sibling) == [(1,)])
    sibling_pid = sibling.is_running().pid
    assert sibling.follow(None, do_reload=True) is True
    follow['wait_for'](lambda: sibling.query('SELECT pid FROM pg_stat_get_wal_receiver()') == [(None,)])
    follow['wait_for'](lambda: api['scalar'](
        connection, "SELECT active FROM pg_replication_slots WHERE slot_name='descendant_c'") is False)
    sibling_endpoint = api['lsn_number'](sibling.query('SELECT pg_last_wal_receive_lsn()')[0][0])
    with connection.cursor() as cursor:
        cursor.execute('INSERT INTO descendant_rows VALUES (2)')
    follow['wait_for'](lambda: rows(promoted) == [(1,), (2,)])
    assert rows(sibling) == [(1,)]
    process = source.process
    source.stop(immediate=True)
    assert process.returncode == -signal.SIGKILL
    assert promoted.promote(30, CriticalTask()) is True
    assert promoted.query('SELECT pg_is_in_recovery()') == [(False,)]
    assert sibling_endpoint <= promoted_history(promoted)
    promoted.query("SELECT pg_create_physical_replication_slot('descendant_c',true)")
    write_row(promoted, 3)
    assert sibling.follow(member(promoted, 'descendant_b'), do_reload=True) is True
    follow['wait_for'](lambda: streaming(sibling, 'descendant_c', 2) and rows(sibling) == [(1,), (2,), (3,)])
    assert sibling.is_running().pid == sibling_pid
    assert promoted.query("SELECT active FROM pg_replication_slots WHERE slot_name='descendant_c'") == [(True,)]
    sender = promoted.query("SELECT w.pid,application_name FROM pg_stat_get_wal_senders() w,"
                            "pg_stat_get_activity(w.pid) WHERE w.state='streaming'")
    assert len(sender) == 1 and sender[0][0] > 0 and sender[0][1] == 'descendant_c', sender
    assert promoted.query("SELECT active_pid FROM pg_replication_slots WHERE slot_name='descendant_c'") == [
        (sender[0][0],)]
    assert f"port={promoted.connection_pool.conn_kwargs['port']}" in sibling.query('SHOW primary_conninfo')[0][0]
    cold_start(sibling)
    follow['wait_for'](lambda: streaming(sibling, 'descendant_c', 2) and rows(sibling) == [(1,), (2,), (3,)])
    print('patroni-descendant: different upstream, child timeline, same-PID reload and cold follow PASS',
          flush=True)
    return promoted, sibling


def reseed_former_primary(source, promoted, sibling, cleanup):
    # Make divergence explicit in this isolated fixture. The old primary has no
    # followers now; fence it again before attempting any recovery conversion.
    source.start()
    connection = api['peers']['connect'](source.port, source.process)
    try:
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO descendant_rows VALUES (999)')
            cursor.execute('SELECT id FROM descendant_rows ORDER BY id')
            assert cursor.fetchall() == [(1,), (2,), (999,)]
    finally:
        connection.close()
    source.stop(immediate=True)
    assert rows(promoted) == [(1,), (2,), (3,)]
    root = source.root / 'rejoin_a'
    root.mkdir(mode=0o700)
    # This original primary was launched directly, rather than initialized by
    # Patroni. Supply the configuration file its adoption path expects.
    (source.data / 'postgresql.conf').write_text('# Former primary managed by Patroni.\n')
    config = follow['configuration'](source.data, source.port, root)
    config['name'] = 'rejoin_a'
    wrapper = root / 'postgres'
    wrapper.write_text(wrapper.read_text().rstrip('\n') + ' >> '
                       + shlex.quote(str(root / 'server.log')) + ' 2>&1\n')
    node = Postgresql(config, Null())
    cleanup.callback(api['stop_follower'], node)
    before = hash_base(source.data)
    assert node.follow(member(promoted, 'descendant_b'), timeout=15) is not True
    follow['wait_for'](lambda: node.is_running() is None)
    assert 'recovery control is missing' in (root / 'server.log').read_text()
    assert hash_base(source.data) == before
    print('patroni-descendant: divergent former primary fails closed without a native recovery base PASS',
          flush=True)
    promoted.query("SELECT pg_create_physical_replication_slot('rejoin_a',true)")
    reseed_from_promoted(node, promoted, source.root / 'former-primary.saved',
                         'patroni_reseed_a', root / 'target.sock')
    assert node.follow(member(promoted, 'descendant_b'), timeout=90) is True
    follow['wait_for'](lambda: streaming(node, 'rejoin_a', 2) and rows(node) == [(1,), (2,), (3,)])
    write_row(promoted, 4)
    follow['wait_for'](lambda: rows(node) == [(1,), (2,), (3,), (4,)]
                       and rows(sibling) == [(1,), (2,), (3,), (4,)])
    cold_start(node, root / 'target.sock')
    follow['wait_for'](lambda: streaming(node, 'rejoin_a', 2) and rows(node) == [(1,), (2,), (3,), (4,)])
    assert hash_base(source.root / 'former-primary.saved') == before
    print('patroni-descendant: explicit verified reseed, unchanged follow, child writes and cold rejoin PASS',
          flush=True)


def node_control(node, form):
    return follow['control'](node.is_running(), Path(node.data_dir).parent / 'target.sock', form)


def applied_divergence(source, connection, cleanup):
    promoted = api['start_follower'](connection, source, 'divergent_b', cleanup)
    sibling = api['start_follower'](connection, source, 'divergent_c', cleanup)
    follow['wait_for'](lambda: rows(promoted) == [(1,)] and rows(sibling) == [(1,)])
    assert promoted.follow(None, do_reload=True) is True
    follow['wait_for'](lambda: promoted.query('SELECT pid FROM pg_stat_get_wal_receiver()') == [(None,)])
    follow['wait_for'](lambda: api['scalar'](
        connection, "SELECT active FROM pg_replication_slots WHERE slot_name='divergent_b'") is False)
    with connection.cursor() as cursor:
        cursor.execute('INSERT INTO descendant_rows VALUES (2)')
    follow['wait_for'](lambda: rows(sibling) == [(1,), (2,)])
    assert rows(promoted) == [(1,)]
    source.stop(immediate=True)
    assert promoted.promote(30, CriticalTask()) is True
    fork = promoted_history(promoted)
    applied = api['lsn_number'](sibling.query('SELECT pg_last_wal_replay_lsn()')[0][0])
    assert applied > fork, (applied, fork)
    follow['wait_for'](lambda: sibling.query(
        'SELECT pg_last_wal_receive_lsn()=pg_last_wal_replay_lsn()') == [(True,)])
    node_control(sibling,
                 '(defparameter *patroni-saved-accepted* '
                 '(copy-tree (physical-follow-effective-config (physical-follow-read-config)))) '
                 '(defparameter *patroni-saved-receive* '
                 '(physical-store-control-tuple *physical-receive-store*))')
    before_pid, before_data = sibling.is_running().pid, hash_base(Path(sibling.data_dir))
    promoted.query("SELECT pg_create_physical_replication_slot('divergent_c',true)")
    write_row(promoted, 3)
    assert sibling.follow(member(promoted, 'divergent_b'), do_reload=True) is True
    # Successful reload is not successful attachment. Require the ancestry
    # rejection from the controller that actually performed source discovery.
    follow['wait_for'](lambda: ':REJECTED T' in node_control(sibling,
        '(let ((message (getf (physical-follow-controller-status) :last-error))) '
        '(list :rejected (and message '
        '(not (null (search "retained common ancestry" message))))))'))
    assert sibling.is_running().pid == before_pid
    assert rows(sibling) == [(1,), (2,)] and rows(promoted) == [(1,), (3,)]
    assert sibling.query('SELECT pid FROM pg_stat_get_wal_receiver()') == [(None,)]
    assert f"port={promoted.connection_pool.conn_kwargs['port']}" in sibling.query('SHOW primary_conninfo')[0][0]
    assert promoted.query("SELECT active FROM pg_replication_slots WHERE slot_name='divergent_c'") == [(False,)]
    node_control(sibling,
                 '(assert (not *physical-recovery-failed-p*)) '
                 '(assert (not (physical-follow-controller-enabled-p (physical-follow-read-config)))) '
                 '(assert (= 1 (physical-recovery-replay-timeline))) '
                 '(assert (equal *patroni-saved-accepted* '
                 '(physical-follow-effective-config (physical-follow-read-config)))) '
                 '(assert (equal *patroni-saved-receive* '
                 '(physical-store-control-tuple *physical-receive-store*)))')
    assert hash_base(Path(sibling.data_dir)) == before_data
    print('patroni-descendant: applied prefix beyond child fork rejected; data and binding preserved PASS',
          flush=True)
    assert sibling.stop(mode='fast') is True
    data = Path(sibling.data_dir)
    archive = data.parent / 'divergent.saved'
    saved = hash_base(data)
    reseed_from_promoted(sibling, promoted, archive, 'patroni_reseed_c')
    assert sibling.follow(member(promoted, 'divergent_b'), timeout=90) is True
    follow['wait_for'](lambda: streaming(sibling, 'divergent_c', 2) and rows(sibling) == [(1,), (3,)])
    write_row(promoted, 4)
    follow['wait_for'](lambda: rows(sibling) == [(1,), (3,), (4,)])
    cold_start(sibling)
    follow['wait_for'](lambda: streaming(sibling, 'divergent_c', 2) and rows(sibling) == [(1,), (3,), (4,)])
    assert hash_base(archive) == saved
    print('patroni-descendant: divergent standby explicitly reseeded and cold-followed child PASS', flush=True)


def check(binary, scenario):
    assert importlib.metadata.version('patroni') == '4.1.5'
    os.environ['PG_CONFIG'] = str(binary / 'pg_config')
    os.environ['PATH'] = str(binary) + os.pathsep + os.environ['PATH']
    source = api['peers']['OwnedLisp']()
    helper = source.root / 'libpq-helper'
    subprocess.run([str(BASE / 't/build-libpq-helper.sh'), str(helper)], check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    connection = None
    try:
        source.start()
        connection = api['peers']['connect'](source.port, source.process)
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE descendant_rows (id int PRIMARY KEY)')
            cursor.execute('INSERT INTO descendant_rows VALUES (1)')
        with ExitStack() as cleanup:
            try:
                if scenario == 'descendant':
                    promoted, sibling = descendant(source, connection, cleanup)
                    connection.close()
                    connection = None
                    reseed_former_primary(source, promoted, sibling, cleanup)
                else:
                    applied_divergence(source, connection, cleanup)
            except Exception:
                if os.environ.get('PG_DESCENDANT_DEBUG') == '1':
                    traceback.print_exc()
                    release = source.root / 'debug-release'
                    print(f'owned peers retained for inspection; touch {release}', flush=True)
                    deadline = time.monotonic() + 300
                    while not release.exists() and time.monotonic() < deadline:
                        time.sleep(0.05)
                raise
    finally:
        try:
            try:
                if connection is not None:
                    connection.close()
            finally:
                source.stop(immediate=True)
        finally:
            print(f'artifacts: {source.root}', flush=True)


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-bin', type=Path, required=True)
    parser.add_argument('--case', choices=('descendant', 'divergence', 'all'), default='all')
    arguments = parser.parse_args()
    cases = ('descendant', 'divergence') if arguments.case == 'all' else (arguments.case,)
    for scenario in cases:
        check(arguments.reference_bin.resolve(), scenario)

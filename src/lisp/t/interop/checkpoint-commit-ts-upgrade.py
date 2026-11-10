"""Preserve unknown commit timestamp facts from an actual older native writer."""
import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import runpy
import signal
import subprocess
import tempfile


if not __debug__:
    raise RuntimeError('This assertion-based oracle requires Python without optimization.')

HERE = Path(__file__).resolve().parent
checkpoint_api = runpy.run_path(str(HERE / 'control-checkpoint.py'))
api = checkpoint_api['api']
checkpoint = checkpoint_api['checkpoint']
read_copy = checkpoint_api['read_copy']
Role = checkpoint_api['PostgresqlRole']


def check_pair(record, known):
    if known:
        assert tuple(map(int, record[16:18])) == (0, 0), record
    else:
        assert record[16:18] == (None, None), record


def kill_follower(node):
    node.connection_pool.close()
    process = node.is_running()
    assert process is not None
    process.send_signal(signal.SIGKILL)
    process.wait(timeout=20)
    assert not process.is_running() or process.status() == checkpoint_api['psutil'].STATUS_ZOMBIE
    (Path(node.data_dir).parent / 'target.sock').unlink(missing_ok=True)


def check(legacy_source):
    source = api['peers']['OwnedLisp']()
    globals_ = type(source).start.__globals__
    original = globals_['LISP']
    with ExitStack() as cleanup:
        cleanup.callback(source.stop, immediate=True)
        try:
            globals_['LISP'] = legacy_source
            source.start()
        finally:
            globals_['LISP'] = original
        connection = api['peers']['connect'](source.port, source.process)
        cleanup.callback(connection.close)
        with connection.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        _, old = checkpoint(connection)
        check_pair(old, False)
        facts, _ = read_copy(source.data)
        assert 'oldest_commit_ts_xid' not in facts and 'newest_commit_ts_xid' not in facts
        follower = api['start_follower'](connection, source, 'legacy_ts', cleanup)
        target = follower.connection_pool.get('heartbeat').get()
        _, base = checkpoint(target)
        check_pair(base, False)
        print('upgrade: actual old-writer checkpoint and new-reader base preserve unknown facts PASS',
              flush=True)
        with connection.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        _, streamed = checkpoint(connection)
        assert streamed[0] != base[0] and streamed[0] != old[0], (old, base, streamed)
        barrier = api['lsn_number'](api['scalar'](connection, 'SELECT pg_current_wal_flush_lsn()'))
        api['follow']['wait_for'](lambda: api['lsn_number'](api['scalar'](
            target, 'SELECT pg_last_wal_replay_lsn()')) >= barrier)
        with target.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        assert checkpoint(target)[1] == streamed
        check_pair(streamed, False)
        kill_follower(follower)
        source.process.kill()
        assert source.process.wait(timeout=20) == -signal.SIGKILL
        assert follower.start(timeout=90, role=Role.REPLICA) is True
        target = follower.connection_pool.get('heartbeat').get()
        assert checkpoint(target)[1] == streamed
        check_pair(checkpoint(target)[1], False)
        print('upgrade: streamed unknown facts survive source-offline follower cold restart PASS',
              flush=True)
        assert api['scalar'](target, 'SELECT pg_promote(true, 30)') is True
        _, promoted = checkpoint(target)
        assert promoted[3] > streamed[3], (streamed, promoted)
        check_pair(promoted, False)
        facts, _ = read_copy(Path(follower.data_dir))
        assert 'oldest_commit_ts_xid' not in facts and 'newest_commit_ts_xid' not in facts
        kill_follower(follower)
        assert follower.start(timeout=90, role=Role.PRIMARY) is True
        target = follower.connection_pool.get('heartbeat').get()
        assert checkpoint(target)[1] == promoted
        check_pair(checkpoint(target)[1], False)
        print('upgrade: promotion and child cold restart do not invent known facts PASS', flush=True)
        with target.cursor() as cursor:
            cursor.execute('CHECKPOINT')
        check_pair(checkpoint(target)[1], True)
        facts, _ = read_copy(Path(follower.data_dir))
        assert facts['oldest_commit_ts_xid'] == facts['newest_commit_ts_xid'] == 0
        print('upgrade: new child checkpoint captures its own installed off policy PASS', flush=True)
        print(f'artifacts: old-source={source.root} child={follower.data_dir}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--legacy-source', type=Path, required=True)
    arguments = parser.parse_args()
    helper = Path(tempfile.mkdtemp(prefix='pg-commit-ts-helper-')) / 'libpq-helper'
    subprocess.run([str(checkpoint_api['BASE'] / 't/build-libpq-helper.sh'), str(helper)],
                   check=True, timeout=60)
    os.environ['PG_LIBPQ_HELPER'] = str(helper)
    check(arguments.legacy_source.resolve())

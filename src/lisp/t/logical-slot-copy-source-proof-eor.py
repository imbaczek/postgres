"""Read permanent source proofs across real, manually promoted writer lineages."""
import argparse
from pathlib import Path
import re
import runpy
import time


harness = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-source-proof.py')))
OwnedLisp = harness['OwnedLisp']
standby = harness['standby']
catalog = harness['catalog']
string = harness['string']
native = Path(__file__).with_name('logical-slot-copy-source-proof-eor-native.lisp')


def query(peer, statement, replication=None):
    connection = standby['RawConnection'](peer.port, replication=replication)
    try:
        connection.socket.settimeout(65)
        return connection.query(statement)
    finally:
        connection.close()


def lsn_number(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) | int(low, 16)


def sync(source, replicas, rows, timeline):
    expected = [[str(number)] for number in range(1, rows + 1)]
    for peer in replicas:
        standby['wait_rows'](peer, expected)
    end = lsn_number(query(source, 'SELECT pg_current_wal_flush_lsn()::text')[0][0])
    for peer in replicas:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            assert peer.process.poll() is None
            applied = lsn_number(query(peer, 'SELECT pg_last_wal_replay_lsn()::text')[0][0])
            if applied == end:
                peer.control(f'(assert (= {timeline} (physical-recovery-replay-timeline)))')
                break
            assert applied < end, (applied, end, peer.root)
            time.sleep(0.05)
        else:
            raise TimeoutError(('exact EOR replay boundary', peer.root, end))
    return end


def promote(peer, system_id, parent, child, fork):
    assert query(peer, 'SELECT pg_promote(true,30)') == [['t']]
    assert query(peer, 'SELECT pg_is_in_recovery()') == [['f']]
    assert query(peer, "SELECT count(*) FROM pg_replication_slots WHERE slot_type='logical'") == [['0']]
    identity = query(peer, 'IDENTIFY_SYSTEM', replication='database')[0]
    assert identity[:2] == [system_id, str(child)], identity
    history = (peer.data / 'pg_wal' / f'{child:08X}.history').read_text().splitlines()
    prior, position, _ = history[-1].split('\t', 2)
    assert int(prior) == parent and lsn_number(position) == fork, history
    peer.control(f'(load {string(native)})')
    result = peer.control(f'(logical-source-proof-test-source-eor {parent} {child} {fork})')
    assert 'SOURCE-PROOF-SOURCE-EOR-PASS' in result, result
    print(f'SOURCE-PROOF-WRITER-PROMOTED parent={parent} child={child} fork={fork}', flush=True)


def retarget(peer, source, prefix, bootstrap_pin=None):
    old_pin = string(bootstrap_pin) if bootstrap_pin else '(fourth (physical-follow-read-config))'
    peer.control(f'(defparameter *logical-source-proof-eor-old-pin* {old_pin})')
    result = peer.control(f'(physical-follow-retarget "127.0.0.1" {source.port} "{prefix}")', timeout=120)
    assert ':STARTED' in result, result
    peer.control(f'''(let ((config (physical-follow-read-config)))
      (assert (= {source.port} (third config)))
      (assert *logical-source-proof-eor-old-pin*)
      (assert (not (equal *logical-source-proof-eor-old-pin* (fourth config)))))''')


def run(nested=False, optional=False):
    started = time.monotonic()
    source = OwnedLisp(parameters=(('wal_level', 'logical'),))
    branch = OwnedLisp(parameters=(('wal_level', 'logical'),))
    child = OwnedLisp(parameters=(('wal_level', 'replica'),))
    second = OwnedLisp(parameters=(('wal_level', 'logical'),)) if nested else None
    peers = [source, branch, child] + ([second] if second else [])
    try:
        source.start()
        query(source, 'CREATE TABLE copy_rows (id integer, body text)')
        for name in ('alpha', 'beta'):
            query(source, f"SELECT * FROM pg_create_logical_replication_slot('{name}', 'pgoutput')")
        query(source, "INSERT INTO copy_rows VALUES (1, 'lineage base')")
        identity = query(source, 'IDENTIFY_SYSTEM', replication='database')[0]
        assert identity[1] == '1', identity
        ordinary = [(branch, 'eor_branch_b')] + ([(second, 'eor_branch_d')] if second else [])
        for peer, pin in ordinary:
            peer.data.mkdir(mode=0o700)
            source.control(f'(physical-base-backup {string(peer.data)} "{pin}")', timeout=90)
            assert not (peer.data / 'logical-slot-copy.descriptor').exists()
            peer.start()
            assert query(peer, "SELECT count(*) FROM pg_replication_slots WHERE slot_type='logical'") == [['0']]
            peer.control(f'(physical-follow-start "127.0.0.1" {source.port} "{pin}")')
        child.data.mkdir(mode=0o700)
        source.control(f'(logical-slot-base-copy {string(child.data)} "eor_import_pin")', timeout=90)
        child.start()
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "eor_import_pin")')
        query(source, "INSERT INTO copy_rows VALUES (2, 'parent suffix')")
        fork = sync(source, [peer for peer, _ in ordinary] + [child], 2, 1)
        for peer, _ in ordinary:
            peer.control('(physical-follow-stop)')
        child.control('(physical-follow-stop)')
        source_process = source.process
        source.stop(immediate=True)
        assert source_process.poll() is not None
        promote(branch, identity[0], 1, 2, fork)
        retarget(child, branch, 'proof_eor2', 'eor_import_pin')
        if second:
            retarget(second, branch, 'proof_eor_sibling', 'eor_branch_d')
        query(branch, "INSERT INTO copy_rows VALUES (3, 'first promoted writer')")
        fork = sync(branch, [child] + ([second] if second else []), 3, 2)
        upstream, parent = branch, 2
        if second:
            child.control('(physical-follow-stop)')
            second.control('(physical-follow-stop)')
            branch.stop(immediate=True)
            promote(second, identity[0], 2, 3, fork)
            retarget(child, second, 'proof_eor3')
            query(second, "INSERT INTO copy_rows VALUES (4, 'second promoted writer')")
            fork = sync(second, [child], 4, 3)
            upstream, parent = second, 3
        assert query(child, 'SELECT pg_is_in_recovery()') == [['t']]
        assert query(child, "SELECT count(*) FROM pg_replication_slots WHERE slot_type='logical'") == [['0']]
        child.control('(physical-follow-stop)')
        child.control('(physical-recovery-restartpoint)')
        upstream.stop(immediate=True)
        harness['environment']['optional'] = optional
        catalog['prepare'](child, pending=True, disposition='eligible', timeout=120)
        child.stop(immediate=True)
        harness['base']['probe'](child, 'eor-cf')
        harness['probe'](child, 'eor-baseline', eor=(parent, fork))
        before = catalog['image'](child.data)
        for retry in range(2):
            harness['probe'](child, f'eor-retry-{retry}', eor=(parent, fork))
            assert before == catalog['image'](child.data), 'EOR cold retry changed files'
        print(f'SOURCE-PROOF-EOR-WORKFLOW-PASS nested={nested} optional={optional} '
              f'seconds={time.monotonic()-started:.6f}', flush=True)
    except BaseException:
        for peer in peers:
            if peer.process is not None and peer.process.poll() is None:
                print(f'SOURCE-PROOF-EOR-DIAGNOSTIC pid={peer.process.pid} root={peer.root}', flush=True)
                try:
                    print(peer.control('''(format t "EOR-STATE phase=~s applied=~s timeline=~s failed=~s~%"
                      (getf *physical-recovery-state* :phase) *last-replay-lsn*
                      (getf *physical-recovery-state* :replay-timeline) *physical-recovery-failed-p*)''', timeout=5), flush=True)
                except Exception as error:
                    print(type(error).__name__, flush=True)
        raise
    finally:
        for peer in reversed(peers):
            peer.stop(immediate=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--nested', action='store_true')
    parser.add_argument('--optional', action='store_true')
    parser.add_argument('--all', action='store_true')
    args = parser.parse_args()
    if args.all:
        for nested in (False, True):
            for optional in (False, True):
                run(nested, optional)
    else:
        run(args.nested, args.optional)

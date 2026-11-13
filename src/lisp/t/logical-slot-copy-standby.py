"""Follow and restart a slot-bearing copy without exposing its logical slots."""
from pathlib import Path
import os
import runpy
import time

copy = runpy.run_path(str(Path(__file__).with_name('logical-slot-base-copy.py')))
api = copy['api']
RawConnection = copy['RawConnection']
lisp_string = copy['lisp_string']


def rows(peer):
    connection = RawConnection(peer.port)
    try:
        assert connection.query('SELECT pg_is_in_recovery()') == [['t']]
        return connection.query('SELECT id FROM copy_rows ORDER BY id')
    finally:
        connection.close()


def wait_rows(peer, expected):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if rows(peer) == expected:
            return
        assert peer.process.poll() is None, peer.root / 'server.log'
        time.sleep(0.05)
    raise AssertionError(('standby did not replay expected rows', expected, peer.root))


def assert_detached(peer, descriptor):
    peer.control("(assert (zerop (loop for slot being the hash-values of *wal-replication-slots* "
                 "count (wal-logical-slot-p slot))))")
    assert descriptor == (peer.data / 'logical-slot-copy.descriptor').read_bytes()
    assert not (peer.data / 'pg_replslot/alpha').exists()
    assert (peer.data / 'logical-slot-copy.base-verified').is_file()


def startup_refused(peer, expected):
    log = peer.root / 'server.log'
    before = log.stat().st_size if log.exists() else 0
    try:
        peer.start()
    except RuntimeError:
        assert expected in log.read_text()[before:], peer.root
        assert peer.process.poll() is not None
    else:
        raise AssertionError(('invalid copied standby started', peer.root))
    finally:
        peer.stop(immediate=True)


def insert(peer, number):
    connection = RawConnection(peer.port)
    try:
        connection.query(f"INSERT INTO copy_rows VALUES ({number}, 'physical continuation')")
    finally:
        connection.close()


def run():
    started = time.monotonic()
    source = api['OwnedLisp'](parameters=(('wal_level', 'logical'),))
    child = api['OwnedLisp'](parameters=(('wal_level', 'logical'),))
    try:
        source.start()
        connection = RawConnection(source.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            connection.query("SELECT * FROM pg_create_logical_replication_slot('alpha', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (1, 'original base')")
        finally:
            connection.close()
        child.data.mkdir(mode=0o700)
        source.control(f'(logical-slot-base-copy {lisp_string(child.data)} "standby_copy_pin")', timeout=90)
        descriptor_path = child.data / 'logical-slot-copy.descriptor'
        descriptor = descriptor_path.read_bytes()
        original_files = {str(p.relative_to(child.data)): p.read_bytes()
                          for p in child.data.rglob('*') if p.is_file()}
        conflict = child.data / 'physical-reseed.retired'
        conflict.write_text('unclaimed conflicting receipt')
        snapshot = {str(p.relative_to(child.data)): p.read_bytes()
                    for p in child.data.rglob('*') if p.is_file()}
        startup_refused(child, 'pending offline reseed follow')
        assert snapshot == {str(p.relative_to(child.data)): p.read_bytes()
                            for p in child.data.rglob('*') if p.is_file()}
        conflict.unlink()
        child.start()
        wait_rows(child, [['1']])
        assert_detached(child, descriptor)
        child.stop()
        child.start()  # Dirty phase-1 recovery control still binds the original D image.
        wait_rows(child, [['1']])
        assert_detached(child, descriptor)
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "standby_copy_pin")')
        insert(source, 2)
        wait_rows(child, [['1'], ['2']])
        connection = RawConnection(child.port)
        try:
            connection.query("SELECT * FROM pg_create_physical_replication_slot('cascade', true)")
        finally:
            connection.close()

        # Actual source restarts emit OFF and a later ON; neither may resurrect
        # the source slot when the detached image is eventually imported.
        connection = RawConnection(source.port)
        try:
            connection.query("SELECT pg_drop_replication_slot('alpha')")
        finally:
            connection.close()
        source.stop()
        source.parameters = (('wal_level', 'replica'),)
        source.start()
        insert(source, 3)
        wait_rows(child, [['1'], ['2'], ['3']])
        source.stop()
        source.parameters = (('wal_level', 'logical'),)
        source.start()
        insert(source, 4)
        wait_rows(child, [['1'], ['2'], ['3'], ['4']])
        child.control('(physical-follow-stop)')
        child.control('(physical-recovery-restartpoint)')
        child.control('''(assert (and (> (physical-store-floor-lsn *physical-receive-store*)
                                      (physical-store-base-lsn *physical-receive-store*))
                                   (physical-store-retained-root *physical-receive-store*)
                                   (getf (getf *physical-recovery-state* :source-logical-fact)
                                         :last-disable-lsn)))''')
        assert_detached(child, descriptor)
        assert any((child.data / name).read_bytes() != data
                   for name, data in original_files.items() if name.startswith('base/')
                   and (child.data / name).is_file())
        child.stop(immediate=True)
        child.start()  # Phase 2 with a compacted receive floor and changed base files.
        wait_rows(child, [['1'], ['2'], ['3'], ['4']])
        child.control('(assert (wal-physical-slot-p (gethash "cascade" *wal-replication-slots*)))')
        assert_detached(child, descriptor)
        child.control('''(let* ((fact (getf *physical-recovery-state* :source-logical-fact))
                                    (history (physical-recovery-resource-history
                                              *physical-receive-store* *physical-recovery-state*))
                                    (descriptor (logical-slot-copy-descriptor-decode
                                                 (read-bounded-regular-file
                                                  (merge-pathnames "logical-slot-copy.descriptor" *pgdata*)
                                                  +logical-slot-copy-descriptor-maximum-bytes+)))
                                    (slot (logical-slot-state-decode
                                           (second (first (logical-slot-copy-descriptor-slots descriptor))) "alpha")))
          (assert (getf fact :last-disable-lsn))
          (assert (eq :insufficient
                      (logical-slot-on-coverage
                       (physical-logical-state-from-fact fact) history
                       (wal-replication-slot-consistent-lsn slot)
                       (logical-slot-provenance-birth-timeline (wal-replication-slot-logical-provenance slot))
                       :prefix-end *last-replay-lsn* (physical-recovery-replay-timeline)))))''')
        try:
            child.control('(physical-recovery-promote)')
        except AssertionError as error:
            assert 'child-owner import' in str(error), error
        else:
            raise AssertionError('copied standby promoted before child ownership import')
        wait_rows(child, [['1'], ['2'], ['3'], ['4']])
        child.stop(immediate=True)

        proof = child.data / 'logical-slot-copy.base-verified'
        saved = proof.read_bytes()
        proof.unlink()
        startup_refused(child, 'requires base verification receipt')
        proof.write_bytes(saved)
        proof.chmod(0o600)
        pieces = copy['decode_descriptor'](descriptor_path)[4]
        timeline, segment = pieces[0][:2]
        carrier = child.data / 'logical_slot_wal' / f'{timeline:08X}{segment // 256:08X}{segment % 256:08X}.wal'
        saved = carrier.read_bytes()
        carrier.write_bytes(bytes([saved[0] ^ 1]) + saved[1:])
        startup_refused(child, 'WAL piece differs')
        carrier.write_bytes(saved)
        child.start()
        wait_rows(child, [['1'], ['2'], ['3'], ['4']])
        assert_detached(child, descriptor)
        print(f'logical-slot-copy-standby: PASS {time.monotonic() - started:.6f}s; '
              f'source: {source.root}; standby: {child.root}', flush=True)
    except BaseException:
        if os.environ.get('PG_LISP_COPY_DEBUG') and child.process and child.process.poll() is None:
            print(f'STANDBY-DEBUG PID={child.process.pid} ROOT={child.root} '
                  f'CONTROL={child.control_socket}', flush=True)
            input()
        raise
    finally:
        child.stop(immediate=True)
        source.stop(immediate=True)


if __name__ == '__main__':
    run()

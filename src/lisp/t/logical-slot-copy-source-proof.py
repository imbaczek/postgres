"""Check permanent source facts from real cold imports with active P7 rows."""
import argparse
import json
import os
from pathlib import Path
import runpy
import re
import signal
import shutil
import subprocess
import time


base = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-cold-publish.py')))
catalog = base['catalog']
api = base['api']
environment = base['environment']
OwnedLisp = base['OwnedLisp']
standby = base['standby']
string = base['string']


def probe(peer, label, *args, failure=None, publication=None, cut='none', syscall=None,
          mount=None, all_lost=False, empty_interval=False, wal_removed=False, segment_boundary=False,
          catalog_spelling=False, catalog_size=None, binding=None, foreign_root=False,
          eor=None, rotation=False, **kwargs):
    assert failure in (None, 'error', 'throw', 'mutation')
    assert publication in (None, 'run', 'reject', 'error', 'throw', 'crash')
    assert binding in (None, 'prefix', 'inventory', 'extent')
    assert sum(bool(x) for x in (failure, publication, syscall, mount, all_lost, empty_interval, wal_removed, segment_boundary, catalog_spelling, binding, foreign_root, eor, rotation)) <= 1
    operation = (f'(logical-source-proof-test-preparation-failure :{failure})'
                 if failure else '(logical-source-proof-test-run)')
    if binding:
        operation = f'(logical-source-proof-test-binding :{binding})'
    if foreign_root:
        operation = '(logical-source-proof-test-foreign-root)'
    if eor:
        assert len(eor) == 2 and all(isinstance(x, int) and not isinstance(x, bool) for x in eor)
        operation = f'(logical-source-proof-test-eor {eor[0]} {eor[1]})'
    if segment_boundary:
        assert isinstance(segment_boundary, int) and not isinstance(segment_boundary, bool)
        assert isinstance(catalog_size, int) and catalog_size > 0
        operation = f'(logical-source-proof-test-segment-boundary {segment_boundary} {catalog_size})'
    if rotation:
        assert isinstance(rotation, int) and not isinstance(rotation, bool)
        assert isinstance(catalog_size, int) and catalog_size > 0
        operation = f'(logical-source-proof-test-rotation {rotation} {catalog_size})'
    if catalog_spelling:
        assert isinstance(catalog_spelling, int) and not isinstance(catalog_spelling, bool)
        assert isinstance(catalog_size, int) and catalog_size > 0
        operation = f'(logical-source-proof-test-catalog-spelling {catalog_spelling} {catalog_size})'
    if wal_removed:
        operation = '(logical-source-proof-test-wal-removed)'
    if empty_interval:
        operation = '(logical-source-proof-test-empty-interval)'
    if all_lost:
        operation = '(logical-source-proof-test-all-lost)'
    if publication:
        assert cut == 'none' or cut in PUBLICATION_CUTS
        operation = f'(logical-source-proof-test-publication :{publication} :{cut})'
    if syscall:
        assert syscall in PUBLICATION_SYSCALLS
        operation = f'(logical-source-proof-test-publication-syscall :{syscall})'
    if mount:
        assert mount in ('root', 'archive', 'canonical', 'temporary')
        operation = f'(logical-source-proof-test-publication-mount :{mount})'
    forms = [
        '(require :asdf)',
        f'(push (uiop:ensure-directory-pathname {string(catalog["LISP"])}) asdf:*central-registry*)',
        '(asdf:load-system "pg")', '(in-package :pg)',
        f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-native.lisp"))})',
        f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-publication-faults.lisp"))})',
        f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-binding-faults.lisp"))})',
        f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-eor-native.lisp"))})',
        f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-rotation-native.lisp"))})',
        f'(let ((*pgdata* (directory-pathname {string(peer.data)}))) '
        '(unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 '
        f':startup-reconcile (lambda () {operation})) '
        '(when *postmaster-pid-stream* (postmaster-remove-pid-file)) '
        '(when *postmaster-data-lock* (close *postmaster-data-lock*) '
        '(setf *postmaster-data-lock* nil))))',
    ]
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger',
               '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'source-proof-{label}.log'
    started = time.monotonic()
    with log.open('w') as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT,
                                timeout=600 if segment_boundary or rotation else 120)
    text = log.read_text()
    if publication == 'crash':
        assert result.returncode == -signal.SIGKILL, (result.returncode, log)
        assert f'SOURCE-PROOF-PUBLICATION-KILL :{cut.upper()}' in text, log
        print(f'SOURCE-PROOF-KILLED {label} seconds={time.monotonic()-started:.6f}', flush=True)
        return
    marker = ('SOURCE-PROOF-ROTATION-PASS' if rotation else
              'SOURCE-PROOF-EOR-PASS' if eor else
              'SOURCE-PROOF-READER-BINDING-PASS' if binding else
              'SOURCE-PROOF-FOREIGN-ROOT-PASS' if foreign_root else
              'SOURCE-PROOF-CATALOG-SPELLING-PASS' if catalog_spelling else
              'SOURCE-PROOF-SEGMENT-BOUNDARY-PASS' if segment_boundary else
              'SOURCE-PROOF-WAL-REMOVED-PASS' if wal_removed else
              'SOURCE-PROOF-EMPTY-INTERVAL-PASS' if empty_interval else
              'SOURCE-PROOF-ALL-LOST-PASS' if all_lost else
              'SOURCE-PROOF-MOUNT-PASS' if mount else
              'SOURCE-PROOF-SYSCALL-PASS' if syscall else
              'SOURCE-PROOF-PUBLICATION-PASS' if publication else
              'SOURCE-PROOF-PREPARATION-FAILURE-PASS' if failure
              else 'SOURCE-PROOF-READER-PASS')
    assert result.returncode == 0 and marker in text, (result.returncode, log)
    assert not (peer.data / 'postmaster.pid').exists()
    print(f'SOURCE-PROOF-CASE {label} PASS seconds={time.monotonic()-started:.6f}', flush=True)


PUBLICATION_CUTS = (
    'after-source-proof-seal', 'after-source-proof-temp-create',
    'after-source-proof-temp-write', 'after-source-proof-temp-sync',
    'after-source-proof-temp-close', 'before-source-proof-install',
    'after-source-proof-install', 'after-source-proof-file-sync',
    'after-source-proof-root-sync', 'after-source-proof-publication-close',
    'before-source-proof-orphan-unlink', 'after-source-proof-orphan-unlink',
)
PROOF = 'logical-slot-copy.source-proof'
TEMP = PROOF + '.tmp'
PUBLICATION_SYSCALLS = (
    'open', 'write', 'temp-sync', 'rename', 'rename-exists',
    'installed-sync', 'directory-sync', 'temp-close', 'body-close',
    'canonical-close', 'reader-proof-close', 'reader-archive-close',
    'outer-root-close', 'reuse-before-write', 'reuse-after-write',
)


def clear_publication(peer):
    for name in (PROOF, TEMP):
        (peer.data / name).unlink(missing_ok=True)
    fd = os.open(peer.data, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def make_temporary(peer, payload):
    fd = os.open(peer.data / TEMP, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as output:
        output.write(payload)


def publication_matrix(peer):
    expected = (peer.data / PROOF).read_bytes()
    cases = [(point, exit_mode) for point in PUBLICATION_CUTS
             for exit_mode in ('error', 'throw', 'crash')]
    print('SOURCE-PROOF-PUBLICATION-CUT-INVENTORY '+json.dumps(cases), flush=True)
    for point, exit_mode in cases:
        clear_publication(peer)
        if 'orphan-unlink' in point:
            make_temporary(peer, b'torn uncommitted temporary')
        before = catalog['image'](peer.data)
        label = exit_mode+'-'+point
        probe(peer, label, publication=exit_mode, cut=point)
        if point == 'after-source-proof-temp-write':
            assert (peer.data / TEMP).stat().st_size == 11, label
        probe(peer, 'recover-'+label, publication='run')
        recovered = catalog['image'](peer.data)
        assert {k: v for k, v in before.items() if k not in (PROOF, TEMP)} == {
            k: v for k, v in recovered.items() if k not in (PROOF, TEMP)}, label
        assert (peer.data / PROOF).read_bytes() == expected, label
        assert not (peer.data / TEMP).exists(), label
        probe(peer, 'retry-'+label, publication='run')
        assert recovered == catalog['image'](peer.data), (label, 'retry changed files or inodes')
    for canonical in (False, True):
        for index, payload in enumerate((b'', b'torn bytes', expected)):
            if canonical:
                prior_inode = (peer.data / PROOF).stat().st_ino
            else:
                clear_publication(peer)
            make_temporary(peer, payload)
            probe(peer, f'orphan-{canonical}-{index}', publication='run')
            assert not (peer.data / TEMP).exists()
            assert (peer.data / PROOF).read_bytes() == expected
            if canonical:
                assert prior_inode == (peer.data / PROOF).stat().st_ino
    print('SOURCE-PROOF-PUBLICATION-MATRIX-PASS', flush=True)


def publication_negative_matrix(peer):
    expected = (peer.data / PROOF).read_bytes()
    make_temporary(peer, b'preserve this uncommitted inode')
    (peer.data / PROOF).write_bytes(b'conflicting canonical file')
    before = catalog['image'](peer.data)
    probe(peer, 'canonical-conflict-with-orphan', publication='reject')
    assert before == catalog['image'](peer.data), 'conflict changed canonical or orphan inode/bytes'
    (peer.data / PROOF).write_bytes(expected)
    canonical_inode = (peer.data / PROOF).stat().st_ino
    probe(peer, 'canonical-conflict-recovery', publication='run')
    assert not (peer.data / TEMP).exists()
    assert (peer.data / PROOF).stat().st_ino == canonical_inode
    for kind in ('mode', 'hardlink', 'symlink', 'oversize'):
        source_before = catalog['image'](peer.data)
        outside = peer.root / ('source-proof-foreign-'+kind)
        outside.write_bytes(b'foreign witness')
        if kind == 'symlink':
            (peer.data / TEMP).symlink_to(outside)
        elif kind == 'hardlink':
            os.chmod(outside, 0o600)
            os.link(outside, peer.data / TEMP)
        else:
            make_temporary(peer, b'uncommitted temporary')
            if kind == 'mode':
                os.chmod(peer.data / TEMP, 0o644)
            else:
                os.truncate(peer.data / TEMP, 1024 * 1024 * 1024)
        before = (peer.data / TEMP).lstat()
        probe(peer, 'foreign-temp-'+kind, publication='reject')
        after = (peer.data / TEMP).lstat()
        assert (before.st_ino, before.st_mode, before.st_nlink, before.st_size,
                before.st_mtime_ns, before.st_ctime_ns) == (
                    after.st_ino, after.st_mode, after.st_nlink, after.st_size,
                    after.st_mtime_ns, after.st_ctime_ns), kind
        assert outside.read_bytes() == b'foreign witness', kind
        rejected = peer.root / ('source-proof-rejected-'+kind)
        (peer.data / TEMP).rename(rejected)
        assert source_before == catalog['image'](peer.data), kind
        rejected.unlink()
        outside.unlink()
        probe(peer, 'foreign-temp-recovery-'+kind, publication='run')
        assert (peer.data / PROOF).read_bytes() == expected
    print('SOURCE-PROOF-PUBLICATION-NEGATIVE-PASS cases=5', flush=True)


def publication_syscall_matrix(peer):
    expected = (peer.data / PROOF).read_bytes()
    print('SOURCE-PROOF-SYSCALL-INVENTORY '+json.dumps(PUBLICATION_SYSCALLS), flush=True)
    for mode in PUBLICATION_SYSCALLS:
        clear_publication(peer)
        before = catalog['image'](peer.data)
        probe(peer, 'syscall-'+mode, syscall=mode)
        if mode == 'rename-exists':
            assert (peer.data / PROOF).read_bytes() == b'FOREIGN'
            (peer.data / PROOF).unlink()
        probe(peer, 'syscall-recover-'+mode, publication='run')
        recovered = catalog['image'](peer.data)
        assert {k: v for k, v in before.items() if k not in (PROOF, TEMP)} == {
            k: v for k, v in recovered.items() if k not in (PROOF, TEMP)}, mode
        assert (peer.data / PROOF).read_bytes() == expected, mode
        assert not (peer.data / TEMP).exists(), mode
        probe(peer, 'syscall-retry-'+mode, publication='run')
        assert recovered == catalog['image'](peer.data), mode
    print('SOURCE-PROOF-SYSCALL-MATRIX-PASS', flush=True)


def publication_mount_matrix(peer):
    expected = (peer.data / PROOF).read_bytes()
    for kind in ('root', 'archive', 'canonical', 'temporary'):
        if kind == 'temporary':
            clear_publication(peer)
        before = catalog['image'](peer.data)
        probe(peer, 'mount-'+kind, mount=kind)
        after = catalog['image'](peer.data)
        assert {k: v for k, v in before.items() if k not in (PROOF, TEMP)} == {
            k: v for k, v in after.items() if k not in (PROOF, TEMP)}, kind
        if kind != 'temporary':
            assert before == after, kind
        probe(peer, 'mount-recover-'+kind, publication='run')
        assert (peer.data / PROOF).read_bytes() == expected, kind
    print('SOURCE-PROOF-MOUNT-MATRIX-PASS', flush=True)


def reader_binding_matrix(peer):
    for mode in ('prefix', 'inventory', 'extent'):
        before = catalog['image'](peer.data)
        probe(peer, 'binding-'+mode, binding=mode)
        assert before == catalog['image'](peer.data), (mode, 'binding probe changed original files')
    foreign = OwnedLisp()
    try:
        original = catalog['image'](peer.data)
        shutil.copytree(peer.data, foreign.data, symlinks=True)
        assert peer.data.stat().st_ino != foreign.data.stat().st_ino
        before = catalog['image'](foreign.data)
        probe(foreign, 'foreign-root', foreign_root=True)
        assert before == catalog['image'](foreign.data), 'foreign-root probe changed copied files'
        assert original == catalog['image'](peer.data), 'foreign-root probe changed source files'
        probe(peer, 'foreign-root-original-retry')
        assert original == catalog['image'](peer.data), 'original retry changed files'
    finally:
        foreign.stop(immediate=True)
    print('SOURCE-PROOF-READER-BINDING-MATRIX-PASS cases=4', flush=True)


def matrix(peer, disposition):
    assert disposition == 'eligible'
    assert peer.process is None
    source = catalog['peers'][0]
    assert source is not peer and source.process is not None
    source_process = source.process
    source.stop(immediate=True)
    assert source_process.poll() is not None
    base['probe'](peer, 'source-proof-cf')
    probe(peer, 'active-baseline')
    for failure in ('error', 'throw', 'mutation'):
        before = catalog['image'](peer.data)
        probe(peer, 'preparation-'+failure, failure=failure)
        assert before == catalog['image'](peer.data), 'preparation failure changed files'
    if arguments.reader_bindings:
        reader_binding_matrix(peer)
    if arguments.publication_cuts:
        publication_matrix(peer)
    if arguments.publication_negative:
        publication_negative_matrix(peer)
    if arguments.publication_syscalls:
        publication_syscall_matrix(peer)
    if arguments.publication_mounts:
        publication_mount_matrix(peer)
    print(f'SOURCE-PROOF-WORKFLOW-PASS optional={environment["optional"]}', flush=True)


def run_all_lost():
    # The existing standby workflow drops alpha, restarts the actual producer
    # with replica WAL, then restores logical WAL. ON cannot undo the OFF gap.
    peer = api['run_live'](pending=True)
    assert peer.process is None or peer.process.poll() is not None
    base['probe'](peer, 'all-lost-cf')
    probe(peer, 'all-lost-baseline', all_lost=True)
    before = catalog['image'](peer.data)
    for retry in range(2):
        probe(peer, f'all-lost-retry-{retry}', all_lost=True)
        assert before == catalog['image'](peer.data), 'all-lost retry changed bytes or inodes'
    print('SOURCE-PROOF-ALL-LOST-WORKFLOW-PASS', flush=True)


def run_boundary_case(optional, *, wal_removed=False, segment_boundary=False, catalog_spelling=False,
                      rotation=False):
    assert sum(bool(x) for x in (wal_removed, segment_boundary, catalog_spelling, rotation)) <= 1
    large = segment_boundary or rotation
    started = time.monotonic()
    source = OwnedLisp(parameters=(('wal_level', 'logical'),))
    child = OwnedLisp(parameters=(('wal_level', 'replica'),))
    try:
        source.start()
        connection = standby['RawConnection'](source.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            for name in (('alpha',) if wal_removed else ('alpha', 'beta')):
                connection.query(f"SELECT * FROM pg_create_logical_replication_slot('{name}', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (1, 'empty interval base')")
        finally:
            connection.close()
        if wal_removed:
            source.control(f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-native.lisp"))})')
            reply = source.control('(logical-source-proof-test-invalidate-alpha)')
            assert 'SOURCE-PROOF-SOURCE-WAL-REMOVED-PASS' in reply
        child.data.mkdir(mode=0o700)
        source.control(f'(logical-slot-base-copy {string(child.data)} "empty_interval_pin")', timeout=90)
        child.start()
        standby['wait_rows'](child, [['1']])
        # Initialize genuine phase-2 receive storage. Segment-boundary, rotation,
        # and catalog-spelling cases add complete source batches after D.
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "empty_interval_pin")')
        if segment_boundary or catalog_spelling or rotation:
            source.control(f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-native.lisp"))})')
            if rotation:
                source.control(f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-rotation-native.lisp"))})')
            reply = source.control(
                '(logical-source-proof-test-write-rotation)' if rotation else
                f'(logical-source-proof-test-write-catalog-batches {"t" if segment_boundary else "nil"})',
                timeout=120)
            match = re.search(r'SOURCE-PROOF-SOURCE-CATALOG-PASS start=([0-9]+) terminal=([0-9]+) catalog-bytes=([0-9]+)', reply)
            assert match, reply
            terminal = int(match.group(2))
            canonical_size = int(match.group(3))
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                reply = child.control('(format t "SOURCE-PROOF-APPLIED ~d~%" *last-replay-lsn*)')
                applied = int(re.search(r'SOURCE-PROOF-APPLIED ([0-9]+)', reply).group(1))
                if applied == terminal:
                    break
                assert applied < terminal, (applied, terminal)
                assert child.process.poll() is None
                time.sleep(0.05)
            else:
                raise TimeoutError(('segment-boundary replay', child.root))
            print(f'SOURCE-PROOF-BOUNDARY-REPLAY-PASS terminal={terminal}', flush=True)
        phase_timeout = 600 if large else 15
        child.control('(physical-follow-stop)', timeout=phase_timeout)
        child.control('(physical-recovery-restartpoint)', timeout=phase_timeout)
        if catalog_spelling:
            child.control(f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-native.lisp"))})')
            reply = child.control(f'(logical-source-proof-test-warm-catalog-spelling {canonical_size + 17})')
            assert 'SOURCE-PROOF-WARM-CATALOG-SPELLING-PASS' in reply
        environment['optional'] = optional
        catalog['prepare'](child, pending=True,
                           disposition='wal-removed' if wal_removed else 'eligible',
                            timeout=600 if large else None)
        child.stop(immediate=True)
        source.stop(immediate=True)
        label = ('rotation' if rotation else 'segment-boundary' if segment_boundary else 'catalog-spelling' if catalog_spelling
                 else 'wal-removed' if wal_removed else 'empty-interval')
        base['probe'](child, label+'-cf', timeout=600 if large else 90)
        probe_options = dict(
            empty_interval=not (wal_removed or segment_boundary or catalog_spelling or rotation),
            wal_removed=wal_removed,
            segment_boundary=terminal if segment_boundary else False,
            rotation=terminal if rotation else False,
            catalog_spelling=terminal if catalog_spelling else False,
            catalog_size=(canonical_size if large else canonical_size + 17)
            if large or catalog_spelling else None)
        probe(child, label+'-baseline', **probe_options)
        before = catalog['image'](child.data)
        for retry in range(2):
            probe(child, f'{label}-retry-{retry}', **probe_options)
            assert before == catalog['image'](child.data), f'{label} retry changed files'
        kind = label.upper()
        print(f'SOURCE-PROOF-{kind}-WORKFLOW-PASS optional={optional} '
              f'seconds={time.monotonic()-started:.6f}', flush=True)
    finally:
        child.stop(immediate=True)
        source.stop(immediate=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--smoke', action='store_true')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--all-lost', action='store_true')
    modes.add_argument('--empty-interval', action='store_true')
    modes.add_argument('--wal-removed', action='store_true')
    modes.add_argument('--segment-boundary', action='store_true')
    modes.add_argument('--rotation', action='store_true')
    modes.add_argument('--catalog-spelling', action='store_true')
    parser.add_argument('--reader-bindings', action='store_true')
    parser.add_argument('--publication-cuts', action='store_true')
    parser.add_argument('--publication-negative', action='store_true')
    parser.add_argument('--publication-syscalls', action='store_true')
    parser.add_argument('--publication-mounts', action='store_true')
    arguments = parser.parse_args()
    if arguments.all_lost or arguments.empty_interval or arguments.wal_removed or arguments.segment_boundary or arguments.catalog_spelling or arguments.rotation:
        if any((arguments.publication_cuts, arguments.publication_negative,
                arguments.publication_syscalls, arguments.publication_mounts, arguments.reader_bindings)):
            parser.error('semantic modes cannot be combined with publication fault matrices')
        if arguments.empty_interval or arguments.wal_removed or arguments.segment_boundary or arguments.catalog_spelling or arguments.rotation:
            for optional in ((False,) if arguments.smoke else (False, True)):
                run_boundary_case(optional, wal_removed=arguments.wal_removed,
                                  segment_boundary=arguments.segment_boundary,
                                  rotation=arguments.rotation,
                                  catalog_spelling=arguments.catalog_spelling)
        else:
            run_all_lost()
        raise SystemExit(0)
    OwnedLisp.start = catalog['start']
    standby['insert'] = catalog['insert']
    workflow = api['run_healthy'].__globals__
    workflow['probe'] = catalog['prepare']
    workflow['evidence_crash_matrix'] = lambda *args: None
    workflow['pending_crash_matrix'] = matrix
    workflow['cold_probe'] = probe
    try:
        for optional in ((False,) if arguments.smoke else (False, True)):
            environment['optional'] = optional
            catalog['peers'].clear()
            catalog['base_catalogs'].clear()
            catalog['dropped_nodes'].clear()
            api['run_healthy']()
    finally:
        OwnedLisp.start = catalog['original_start']
        standby['insert'] = catalog['original_insert']

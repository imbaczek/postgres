"""Check permanent source facts from real cold imports with active P7 rows."""
import argparse
import json
import os
from pathlib import Path
import runpy
import signal
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
          mount=None, all_lost=False, empty_interval=False, **kwargs):
    assert failure in (None, 'error', 'throw', 'mutation')
    assert publication in (None, 'run', 'reject', 'error', 'throw', 'crash')
    assert sum(bool(x) for x in (failure, publication, syscall, mount, all_lost, empty_interval)) <= 1
    operation = (f'(logical-source-proof-test-preparation-failure :{failure})'
                 if failure else '(logical-source-proof-test-run)')
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
                                timeout=120)
    text = log.read_text()
    if publication == 'crash':
        assert result.returncode == -signal.SIGKILL, (result.returncode, log)
        assert f'SOURCE-PROOF-PUBLICATION-KILL :{cut.upper()}' in text, log
        print(f'SOURCE-PROOF-KILLED {label} seconds={time.monotonic()-started:.6f}', flush=True)
        return
    marker = ('SOURCE-PROOF-EMPTY-INTERVAL-PASS' if empty_interval else
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


def run_empty_interval(optional):
    started = time.monotonic()
    source = OwnedLisp(parameters=(('wal_level', 'logical'),))
    child = OwnedLisp(parameters=(('wal_level', 'replica'),))
    try:
        source.start()
        connection = standby['RawConnection'](source.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            for name in ('alpha', 'beta'):
                connection.query(f"SELECT * FROM pg_create_logical_replication_slot('{name}', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (1, 'empty interval base')")
        finally:
            connection.close()
        child.data.mkdir(mode=0o700)
        source.control(f'(logical-slot-base-copy {string(child.data)} "empty_interval_pin")', timeout=90)
        child.start()
        standby['wait_rows'](child, [['1']])
        # Initialize phase-2 receive storage without post-D source writes.
        # The actual restartpoint must still prove F equals the copied D.
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "empty_interval_pin")')
        child.control('(physical-follow-stop)')
        child.control('(physical-recovery-restartpoint)')
        environment['optional'] = optional
        catalog['prepare'](child, pending=True, disposition='eligible')
        child.stop(immediate=True)
        source.stop(immediate=True)
        base['probe'](child, 'empty-interval-cf')
        probe(child, 'empty-interval-baseline', empty_interval=True)
        before = catalog['image'](child.data)
        for retry in range(2):
            probe(child, f'empty-interval-retry-{retry}', empty_interval=True)
            assert before == catalog['image'](child.data), 'empty interval retry changed files'
        print(f'SOURCE-PROOF-EMPTY-INTERVAL-WORKFLOW-PASS optional={optional} '
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
    parser.add_argument('--publication-cuts', action='store_true')
    parser.add_argument('--publication-negative', action='store_true')
    parser.add_argument('--publication-syscalls', action='store_true')
    parser.add_argument('--publication-mounts', action='store_true')
    arguments = parser.parse_args()
    if arguments.all_lost or arguments.empty_interval:
        if any((arguments.publication_cuts, arguments.publication_negative,
                arguments.publication_syscalls, arguments.publication_mounts)):
            parser.error('semantic modes cannot be combined with publication fault matrices')
        if arguments.empty_interval:
            for optional in ((False,) if arguments.smoke else (False, True)):
                run_empty_interval(optional)
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

"""Prove copied source WAL after child commit, without installing logical slots."""
import os
from pathlib import Path
import re
import runpy
import subprocess
import time

api = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-import-input.py')))
OwnedLisp, standby, lisp_string = api['OwnedLisp'], api['standby'], api['lisp_string']
LISP = standby['api']['LISP']
HOOK = Path(__file__).with_name('logical-slot-copy-import-commit-native.lisp')


def cold_probe(peer, label, operation, expected=None):
    before = api['tree_image'](peer.data)
    form = f'''(let ((*pgdata* (directory-pathname {lisp_string(peer.data)})) (done nil))
      (unwind-protect
           (postmaster-own-data-directory "127.0.0.1" 6543
             :startup-reconcile (lambda () {operation} (setf done t)))
        (when *postmaster-pid-stream* (postmaster-remove-pid-file))
        (when *postmaster-data-lock*
          (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil)))
      (assert done))'''
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {lisp_string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             f'(load {lisp_string(Path(__file__).with_name("logical-slot-copy-import-input-native.lisp"))})',
             f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(HOOK)}))',
             f'(setf *logical-import-commit-test-omit-control-fact* {"t" if label.startswith("optional-") else "nil"})',
             form]
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for code in forms:
        command.extend(['--eval', code])
    log = peer.root / f'committed-{label}.log'
    started = time.monotonic()
    with log.open('w') as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=90)
    text = log.read_text()
    assert result.returncode == (1 if expected else 0), (label, result.returncode, log)
    assert (expected or 'LOGICAL-IMPORT-COMMITTED-COLD-SOURCE-PASS') in text, (label, log)
    assert api['tree_image'](peer.data) == before, (label, 'cold proof changed disk bytes')
    assert not (peer.data / 'postmaster.pid').exists(), label
    print(f'COMMITTED-SOURCE-CASE {label} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def cold_matrix(peer, optional):
    prefix = 'optional-' if optional else 'normal-'
    healthy = '(logical-import-commit-test-cold-source)'
    cold_probe(peer, prefix + 'healthy', healthy)
    for mode in ('case', 'number'):
        cold_probe(peer, prefix + mode, f'(logical-import-commit-test-input-fault :{mode})',
                   'committed import callback modified its input binding')
    for mode in ('certificate', 'plan', 'fact', 'rewrite'):
        expected = ('logical copy input changed after verification' if mode == 'rewrite'
                    else 'committed import source binding was modified')
        cold_probe(peer, prefix + mode, f'(logical-import-commit-test-source-fault :{mode})', expected)
    segments = [path for path in (peer.data / 'pg_wal').iterdir()
                if re.fullmatch(r'[0-9A-F]{24}\.wal', path.name)]
    child = max(int(path.name[:8], 16) for path in segments)
    segment = next(path for path in segments if int(path.name[:8], 16) == child)
    extents = list((peer.data / 'pg_physical_retained').glob('extent-*.wal'))
    assert len(extents) == 1, extents
    for label, path, expected in (
            ('child-wal', segment, 'source-authenticated plan'),
            ('child-control', peer.data / 'global/pg_wal_control', 'exact child control'),
            ('source-extent', extents[0], 'retained WAL object differs from its immutable reference')):
        saved = path.read_bytes()
        changed = bytearray(saved)
        changed[0] ^= 1
        path.write_bytes(changed)
        try:
            cold_probe(peer, prefix + label, healthy, expected)
        finally:
            path.write_bytes(saved)
    cold_probe(peer, prefix + 'healthy-retry', healthy)


def run(optional=False):
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
            connection.query("INSERT INTO copy_rows VALUES (1, 'healthy base')")
        finally:
            connection.close()
        child.data.mkdir(mode=0o700)
        source.control(f'(logical-slot-base-copy {lisp_string(child.data)} "healthy_import_pin")', timeout=90)
        child.start()
        standby['wait_rows'](child, [['1']])
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "healthy_import_pin")')
        standby['insert'](source, 2)
        standby['wait_rows'](child, [['1'], ['2']])
        child.control('(physical-follow-stop)')
        child.control('(physical-recovery-restartpoint)')
        api['probe'](child, pending=True, disposition='eligible')
        child.control(f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(HOOK)}))')
        child.control(f'(setf *logical-import-commit-test-omit-control-fact* {"t" if optional else "nil"})')
        child.control('(logical-import-commit-test-start)')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert child.process.poll() is None, child.root / 'server.log'
            result = child.control('*logical-import-commit-test-result*')
            if ':PASSED' in result:
                child.control('(assert (not (eq :timeout (sb-thread:join-thread *logical-import-commit-test-thread* :timeout 5 :default :timeout))))')
                break
            if ':ERROR' in result:
                raise AssertionError((result, child.root / 'server.log'))
            time.sleep(0.05)
        else:
            raise TimeoutError(child.root)
        log = (child.root / 'server.log').read_text()
        assert 'LOGICAL-IMPORT-COMMITTED-SOURCE-PASS' in log
        if optional:
            assert 'LOGICAL-IMPORT-COMMITTED-OPTIONAL-C3-PREPARED' in log
        child.stop(immediate=True)
        source.stop(immediate=True)
        cold_matrix(child, optional)
        print(f'COMMITTED-SOURCE-PASS optional={optional} seconds={time.monotonic() - started:.6f}', flush=True)
    finally:
        child.stop(immediate=True)
        source.stop(immediate=True)


if __name__ == '__main__':
    run()
    run(optional=True)

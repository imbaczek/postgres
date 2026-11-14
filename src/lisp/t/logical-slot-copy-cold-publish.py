"""Publish cold child control from a real copied, compacted standby."""
import os
from pathlib import Path
import re
import runpy
import signal
import subprocess
import sys
import time

catalog = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-cold-catalog.py')))
environment = catalog['matrix'].__globals__
api = catalog['api']
standby = catalog['standby']
OwnedLisp = catalog['OwnedLisp']
string = catalog['string']


def probe(peer, label, *args, kill=None, late_source=False, cut=None,
          authority=None, syscall=None, **kwargs):
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {string(catalog["LISP"])}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             f'(load {string(Path(__file__).with_name("logical-slot-copy-cold-publish-native.lisp"))})',
             f'(let ((*pgdata* (directory-pathname {string(peer.data)}))) '
             '(unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 '
             ':startup-reconcile (lambda () (logical-cold-publish-test-run))) '
             '(when *postmaster-pid-stream* (postmaster-remove-pid-file)) '
             '(when *postmaster-data-lock* (close *postmaster-data-lock*) '
             '(setf *postmaster-data-lock* nil))))']
    if syscall:
        forms[-1] = forms[-1].replace('(logical-cold-publish-test-run)',
                                     f'(logical-cold-publish-test-syscall :{syscall})')
    if authority:
        forms[-1] = forms[-1].replace('(logical-cold-publish-test-run)',
                                     f'(logical-cold-publish-test-authority :{authority})')
    if cut:
        forms[-1] = forms[-1].replace('(logical-cold-publish-test-run)',
                                     f'(logical-cold-publish-test-cut :{cut[0]} :{cut[1]})')
    if late_source:
        forms[-1] = forms[-1].replace('(logical-cold-publish-test-run)', '(logical-cold-publish-test-late-source)')
    if kill:
        invocation = '(logical-cold-publish-test-run)'
        guarded = (f'(let ((*logical-slot-copy-verification-test-hook* '
                   f'(lambda (point context) (declare (ignore context)) '
                   f'(when (eq point :{kill}) '
                   f'(format t "~&COLD-PUBLISH-KILL {kill}~%") (finish-output) '
                   '(sb-posix:kill (sb-posix:getpid) sb-posix:sigkill))))) '
                   f'{invocation})')
        forms[-1] = forms[-1].replace(invocation, guarded)
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'cold-publish-{label}.log'
    started = time.monotonic()
    with log.open('w') as output:
        status = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=90).returncode
    text = log.read_text()
    if kill:
        assert status == -signal.SIGKILL and f'COLD-PUBLISH-KILL {kill}' in text, (status, log)
        print(f'COLD-PUBLISH-KILLED {label} seconds={time.monotonic()-started:.6f}', flush=True)
        return
    marker = ('COLD-PUBLISH-SYSCALL-PASS' if syscall else
              'COLD-PUBLISH-AUTHORITY-PASS' if authority else
              'COLD-PUBLISH-CUT-PASS' if cut else
              'COLD-PUBLISH-LATE-SOURCE-PASS' if late_source else
              'COLD-PUBLISH-CONTROL-PASS')
    assert status == 0 and marker in text, (status, log)
    assert not (peer.data / 'postmaster.pid').exists()
    if late_source or cut or authority or syscall:
        print(f'COLD-PUBLISH-FAULT {label} PASS seconds={time.monotonic()-started:.6f}', flush=True)
        return
    names = set(re.findall(r'^COLD-PUBLISH-CHILD-FILE (\S+)$', text, re.MULTILINE))
    assert names
    print(f'COLD-PUBLISH-CASE {label} PASS seconds={time.monotonic()-started:.6f}', flush=True)
    return names


def matrix(peer, disposition):
    assert disposition == 'eligible'
    target = peer.data / 'global/pg_catalog.dat'
    selected = target.read_bytes()
    base = catalog['base_catalogs'][peer]
    assert selected != base
    assert b'restored_since_base' in selected and b'dropped_since_base' not in selected
    assert not (peer.data / 'base' / str(catalog['dropped_nodes'][peer])).exists()
    parent_control = (peer.data / 'global/pg_wal_control').read_bytes()
    target.write_bytes(base)
    before = catalog['image'](peer.data)
    children = probe(peer, 'publish')
    after = catalog['image'](peer.data)
    mutable = children | {'global/pg_catalog.dat', 'global/pg_catalog.tmp',
                          'global/pg_wal_control', 'global/pg_wal_control.tmp'}
    assert {k: v for k, v in before.items() if k not in mutable} == {
        k: v for k, v in after.items() if k not in mutable}, 'publication changed unrelated files'
    assert target.read_bytes() == selected
    assert not (peer.data / 'global/pg_catalog.tmp').exists()
    assert not (peer.data / 'global/pg_wal_control.tmp').exists()
    control = (peer.data / 'global/pg_wal_control').read_bytes()
    assert control != before['global/pg_wal_control'][-1]
    probe(peer, 'cf-retry')
    assert after == catalog['image'](peer.data), 'CF retry replaced an inode or changed bytes'
    if '--syscalls' in sys.argv:
        for mode in ('open', 'write', 'old-sync', 'temp-sync', 'installed-sync',
                     'directory-sync', 'data-sync', 'rename', 'close'):
            for name in children:
                (peer.data / name).unlink(missing_ok=True)
            (peer.data / 'global/pg_wal_control').write_bytes(parent_control)
            target.write_bytes(base)
            probe(peer, 'syscall-'+mode, syscall=mode)
            probe(peer, 'syscall-recovery-'+mode)
    if '--authority' in sys.argv:
        for mode in ('copied', 'sticky'):
            for name in children:
                (peer.data / name).unlink(missing_ok=True)
            (peer.data / 'global/pg_wal_control').write_bytes(parent_control)
            target.write_bytes(base)
            probe(peer, 'authority-'+mode, authority=mode)
            probe(peer, 'authority-recovery-'+mode)
    if '--late-source' in sys.argv:
        for name in children:
            (peer.data / name).unlink(missing_ok=True)
        (peer.data / 'global/pg_wal_control').write_bytes(parent_control)
        target.write_bytes(base)
        probe(peer, 'late-source-d', late_source=True)
        probe(peer, 'late-source-recovery')
        probe(peer, 'late-source-cf', late_source=True)
        probe(peer, 'late-source-cf-recovery')
    if '--crashes' in sys.argv or '--cuts' in sys.argv:
        cases = []
        if '--crashes' in sys.argv:
            cases.extend((point, None) for point in CUTS)
        if '--cuts' in sys.argv:
            cases.extend((point, exit) for point in CUTS for exit in ('error', 'throw'))
        for point, exit in cases:
            for name in children:
                (peer.data / name).unlink(missing_ok=True)
            (peer.data / 'global/pg_wal_control').write_bytes(parent_control)
            target.write_bytes(base)
            prior = catalog['image'](peer.data)
            label = (exit or 'kill')+'-'+point
            if exit:
                probe(peer, label, cut=(point, exit))
            else:
                probe(peer, label, kill=point)
            probe(peer, 'recover-'+label)
            recovered = catalog['image'](peer.data)
            assert {k: v for k, v in prior.items() if k not in mutable} == {
                k: v for k, v in recovered.items() if k not in mutable}, (point, 'unrelated file changed')
            assert target.read_bytes() == selected
            assert not (peer.data / 'global/pg_catalog.tmp').exists()
            assert not (peer.data / 'global/pg_wal_control.tmp').exists()
            probe(peer, 'recovered-retry-'+label)
            assert recovered == catalog['image'](peer.data), (point, 'retry changed image')
    print(f'COLD-PUBLISH-WORKFLOW-PASS optional={environment["optional"]}', flush=True)


CUTS = ('before-cold-control-create', 'after-cold-control-create',
        'after-cold-control-write', 'after-cold-control-old-sync',
        'after-cold-control-temp-sync', 'before-cold-control-rename',
        'after-cold-control-rename', 'after-cold-control-file-sync',
        'after-cold-control-directory-sync', 'after-cold-control-close')


if __name__ == '__main__':
    OwnedLisp.start = catalog['start']
    standby['insert'] = catalog['insert']
    globals_for_run = api['run_healthy'].__globals__
    globals_for_run['probe'] = catalog['prepare']
    globals_for_run['evidence_crash_matrix'] = lambda *args: None
    globals_for_run['pending_crash_matrix'] = matrix
    globals_for_run['cold_probe'] = probe
    try:
        for optional in ((False,) if '--smoke' in sys.argv else (False, True)):
            environment['optional'] = optional
            catalog['peers'].clear()
            catalog['base_catalogs'].clear()
            catalog['dropped_nodes'].clear()
            api['run_healthy']()
    finally:
        OwnedLisp.start = catalog['original_start']
        standby['insert'] = catalog['original_insert']

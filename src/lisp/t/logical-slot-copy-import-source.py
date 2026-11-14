"""Check cold source callback leases against a real pending copied standby."""
import os
from pathlib import Path
import runpy
import subprocess
import time

api = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-import-input.py')))
LISP = api['standby']['api']['LISP']
MODES = ('healthy', 'case', 'number', 'cycle', 'caught', 'control', 'extent',
         'parent-rewrite', 'throw', 'close', 'optional', 'late-extent', 'post-check', 'post-error')


def probe(peer, mode):
    before = api['tree_image'](peer.data)
    string = api['lisp_string']
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             '(setf *print-circle* t *print-length* nil *print-level* nil)',
             '(handler-bind ((warning (lambda (c) (error "input fixture warning: ~a" c)))) '
             f'(load {string(Path(__file__).with_name("logical-slot-copy-import-input-native.lisp"))}))',
             '(handler-bind ((warning (lambda (c) (error "source fixture warning: ~a" c)))) '
             f'(load {string(Path(__file__).with_name("logical-slot-copy-import-source-native.lisp"))}))',
             f'(let ((*pgdata* (directory-pathname {string(peer.data)}))) '
             '(unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 '
             f':startup-reconcile (lambda () (logical-import-source-test-run :{mode}))) '
             '(when *postmaster-pid-stream* (postmaster-remove-pid-file)) '
             '(when *postmaster-data-lock* (close *postmaster-data-lock*) '
             '(setf *postmaster-data-lock* nil))))']
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'source-scope-{mode}.log'
    started = time.monotonic()
    with log.open('w') as out:
        result = subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, timeout=90)
    assert result.returncode == 0, (mode, result.returncode, log)
    assert f'LOGICAL-IMPORT-SOURCE-SCOPE-PASS :{mode.upper()}' in log.read_text(), (mode, log)
    assert before == api['tree_image'](peer.data), (mode, 'source scope changed durable bytes')
    assert not (peer.data / 'postmaster.pid').exists(), mode
    print(f'LOGICAL-IMPORT-SOURCE-CASE {mode} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def matrix(peer, disposition):
    assert disposition == 'eligible'
    for mode in MODES:
        probe(peer, mode)


if __name__ == '__main__':
    api['run_healthy'].__globals__['evidence_crash_matrix'] = lambda *args: None
    api['run_healthy'].__globals__['pending_crash_matrix'] = matrix
    api['run_healthy']()

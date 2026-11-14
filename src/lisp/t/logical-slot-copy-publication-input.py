"""Bind genuine pending-D and committed-CF inputs without changing storage."""
import os
from pathlib import Path
import runpy
import subprocess
import time

api = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-import-input.py')))
committed = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-committed-source.py')))
string = api['lisp_string']
LISP = api['standby']['api']['LISP']


def probe(peer, phase):
    before = api['tree_image'](peer.data)
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             f'(load {string(Path(__file__).with_name("logical-slot-copy-publication-input-native.lisp"))})',
             f'(let ((*pgdata* (directory-pathname {string(peer.data)}))) '
             '(unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 '
             f':startup-reconcile (lambda () (logical-publication-input-test-run :{phase}))) '
             '(when *postmaster-pid-stream* (postmaster-remove-pid-file)) '
             '(when *postmaster-data-lock* (close *postmaster-data-lock*) '
             '(setf *postmaster-data-lock* nil))))']
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'publication-input-{phase}.log'
    started = time.monotonic()
    with log.open('w') as output:
        status = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=90).returncode
    assert status == 0, (phase, status, log)
    assert f'PUBLICATION-INPUT-PHASE-PASS :{phase.upper()}' in log.read_text(), log
    assert before == api['tree_image'](peer.data), (phase, 'input proof changed durable bytes')
    assert not (peer.data / 'postmaster.pid').exists()
    print(f'PUBLICATION-INPUT-CASE {phase} PASS seconds={time.monotonic()-started:.6f}', flush=True)


if __name__ == '__main__':
    api['run_healthy'].__globals__['evidence_crash_matrix'] = lambda *args: None
    api['run_healthy'].__globals__['pending_crash_matrix'] = lambda peer, disposition: probe(peer, 'parent')
    api['run_healthy']()
    committed['run'].__globals__['cold_matrix'] = lambda peer, optional: probe(peer, 'child')
    committed['run']()
    committed['run'](optional=True)

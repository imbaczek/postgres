"""Check permanent source facts from real cold imports with active P7 rows."""
import argparse
import os
from pathlib import Path
import runpy
import subprocess
import time


base = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-cold-publish.py')))
catalog = base['catalog']
api = base['api']
environment = base['environment']
OwnedLisp = base['OwnedLisp']
standby = base['standby']
string = base['string']


def probe(peer, label, *args, **kwargs):
    forms = [
        '(require :asdf)',
        f'(push (uiop:ensure-directory-pathname {string(catalog["LISP"])}) asdf:*central-registry*)',
        '(asdf:load-system "pg")', '(in-package :pg)',
        f'(load {string(Path(__file__).with_name("logical-slot-copy-source-proof-native.lisp"))})',
        f'(let ((*pgdata* (directory-pathname {string(peer.data)}))) '
        '(unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 '
        ':startup-reconcile (lambda () (logical-source-proof-test-run))) '
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
    assert result.returncode == 0 and 'SOURCE-PROOF-BINDING-PASS' in text, (result.returncode, log)
    assert not (peer.data / 'postmaster.pid').exists()
    print(f'SOURCE-PROOF-CASE {label} PASS seconds={time.monotonic()-started:.6f}', flush=True)


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
    print(f'SOURCE-PROOF-WORKFLOW-PASS optional={environment["optional"]}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--smoke', action='store_true')
    arguments = parser.parse_args()
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

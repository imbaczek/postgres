"""Verify durable fixed journal publication and resync under real source proof."""
import os
from pathlib import Path
import re
import runpy
import subprocess
import time

base = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-install-handoff.py')))
api, image, root_fact = base['api'], base['image'], base['root_fact']
LISP, lisp_string, WORKSPACES = base['LISP'], base['lisp_string'], base['WORKSPACES']
HOOK = Path(__file__).with_name('logical-slot-copy-install-journal-native.lisp')
POINTS = ('before-journal-rename', 'after-journal-rename', 'after-journal-file-sync',
          'after-journal-active-sync', 'after-journal-container-sync',
          'after-journal-source-parent-sync', 'after-journal-target-parent-sync',
          'before-journal-final-proof', 'after-journal-final-proof')
MODES = ('fresh', 'fixed', 'fixed-one', 'fixed-all', 'fixed-active', 'physical', 'absent', 'orphan',
         'rename-collision', 'source-cut', 'proof-swap', 'close-cut', 'close-interrupt', 'close-phase', 'late-phase', 'caught', 'throw') + tuple(f'(:cut :{point})' for point in POINTS) + tuple(f'(:fsync {n})' for n in range(1, 6))

def mode_form(mode):
    return chr(39) + mode if mode.startswith('(') else ':' + mode



def snapshot(peer):
    return (image(peer.data), root_fact(peer.data),
            {name: image(peer.data / name, staging=True) for name in WORKSPACES})


def check(peer, saved):
    canonical, root, orphans = saved
    base['check_canonical'](peer, canonical, image(peer.data))
    assert root_fact(peer.data) == root
    assert not os.path.lexists(peer.data / 'logical-slot-copy.install')
    assert not (peer.data / 'pg_replslot').exists() or sorted(p.name for p in (peer.data / 'pg_replslot').iterdir()) in ([], ['.logical-slot-copy.work'])
    for name, old in orphans.items():
        now = image(peer.data / name, staging=True)
        assert all(now.get(key) == value for key, value in old.items()), (name, 'old orphan changed')


def fixtures():
    return [HOOK.with_name(name) for name in ('physical-directory-root-native.lisp', 'logical-slot-copy-install-stage-native.lisp',
            'logical-slot-copy-install-handoff-native.lisp', 'logical-slot-copy-install-input-native.lisp', 'logical-slot-copy-install-journal-native.lisp')]


def probe(peer, mode, label=None, saved=None):
    label = label or mode
    saved = saved or snapshot(peer)
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {lisp_string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             '(setf *print-circle* t *print-length* nil *print-level* nil)']
    for path in fixtures():
        forms.append(f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(path)}))')
    forms.append(f'(let ((*pgdata* (directory-pathname {lisp_string(peer.data)}))) (unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 :startup-reconcile (lambda () (logical-install-journal-test-run {mode_form(mode)}))) (when *postmaster-pid-stream* (postmaster-remove-pid-file)) (when *postmaster-data-lock* (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil))))')
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'journal-{label.replace(chr(32), chr(95))}-{time.monotonic_ns()}.log'
    started = time.monotonic()
    with log.open('w') as out:
        result = subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, timeout=90)
    if mode.startswith('(:kill '):
        assert result.returncode == -9, (mode, result.returncode, log)
        assert 'LOGICAL-INSTALL-JOURNAL-KILL ' + mode[7:-1].upper() in log.read_text(), log
        fixed = 'before-journal-rename' not in mode
        assert os.path.lexists(peer.data / 'logical-slot-copy.install') == fixed
        print(f'LOGICAL-INSTALL-JOURNAL-KILLED {mode} seconds={time.monotonic() - started:.6f}', flush=True)
        probe(peer, 'recover' if fixed else 'absent', label='restart-' + mode, saved=saved)
        return
    assert result.returncode == 0, (mode, result.returncode, log)
    assert f'LOGICAL-INSTALL-JOURNAL-PASS {(mode if mode.startswith('(') else ':' + mode).upper()}' in log.read_text(), (mode, log)
    check(peer, saved)
    assert not (peer.data / 'postmaster.pid').exists()
    print(f'LOGICAL-INSTALL-JOURNAL-CASE {label} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def live(peer, source, fault='throw'):
    del source
    for path in fixtures():
        peer.control(f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(path)}))')
    for mode in ('fresh', 'physical', fault):
        saved = snapshot(peer)
        started = time.monotonic()
        peer.control(f'(logical-install-journal-test-start {mode_form(mode)})')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert peer.process.poll() is None, peer.root / 'server.log'
            status = peer.control('*logical-install-journal-test-result*', timeout=90)
            if ':PASSED' in status:
                peer.control('(assert (not (eq :timeout (sb-thread:join-thread *logical-install-journal-test-thread* :timeout 5 :default :timeout))))')
                break
            assert ':ERROR' not in status, (mode, status, peer.root / 'server.log')
            time.sleep(0.05)
        else:
            raise TimeoutError((mode, peer.root))
        check(peer, saved)
        print(f'LOGICAL-INSTALL-JOURNAL-LIVE {mode} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def cold(peer, optional):
    del optional
    for mode in MODES:
        probe(peer, mode)
    for point in POINTS:
        probe(peer, f'(:kill :{point})')



if __name__ == '__main__':
    api['run'].__globals__['cold_matrix'] = cold
    for optional in (False, True):
        fault = '(:fsync 5)' if optional else 'throw'
        api['run'](optional, before_stop=lambda peer, source: live(peer, source, fault))

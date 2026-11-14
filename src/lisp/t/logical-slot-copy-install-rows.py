"""Verify ordered P7 row moves and prefix recovery under retained source proof."""
import os
from pathlib import Path
import re
import runpy
import subprocess
import time

base = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-install-handoff.py')))
api, image, root_fact = base['api'], base['image'], base['root_fact']
LISP, lisp_string, WORKSPACES = base['LISP'], base['lisp_string'], base['WORKSPACES']
HOOK = Path(__file__).with_name('logical-slot-copy-install-rows-native.lisp')
POINTS = ('row-open', 'before-row-rename', 'after-row-rename', 'after-row-state-sync',
          'after-row-directory-sync', 'after-row-source-parent-sync',
          'after-row-target-parent-sync', 'after-row-close')
MODES = ('fresh', 'resume-zero', 'resume-one', 'resume-all', 'physical', 'absent', 'orphan',
         'unspent', 'collision-alpha', 'collision-beta', 'source-cut', 'proof-swap',
         'close-cut', 'close-interrupt', 'nested-slot-mount', 'closed-row-change', 'caught', 'throw') + tuple(
             f'(:cut :{point} "{name}")' for name in ('alpha', 'beta') for point in POINTS) + tuple(
             f'(:fsync {n})' for n in range(1, 9))
QUICK = os.environ.get('PG_LISP_ROWS_QUICK') == '1'

def mode_form(mode):
    return chr(39) + mode if mode.startswith('(') else ':' + mode



def printed_mode(mode):
    text = mode if mode.startswith('(') else ':' + mode
    return re.sub(r'"[^"]*"|[^" ]+', lambda m: m[0] if m[0].startswith('"') else m[0].upper(), text)


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
            'logical-slot-copy-install-handoff-native.lisp', 'logical-slot-copy-install-input-native.lisp', 'logical-slot-copy-install-journal-native.lisp', 'logical-slot-copy-install-rows-native.lisp')]


def probe(peer, mode, label=None, saved=None):
    label = label or mode
    saved = saved or snapshot(peer)
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {lisp_string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             '(setf *print-circle* t *print-length* nil *print-level* nil)']
    for path in fixtures():
        forms.append(f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(path)}))')
    forms.append(f'(let ((*pgdata* (directory-pathname {lisp_string(peer.data)}))) (unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 :startup-reconcile (lambda () (logical-install-rows-test-run {mode_form(mode)}))) (when *postmaster-pid-stream* (postmaster-remove-pid-file)) (when *postmaster-data-lock* (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil))))')
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'rows-{label.replace(chr(32), chr(95))}-{time.monotonic_ns()}.log'
    started = time.monotonic()
    with log.open('w') as out:
        result = subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, timeout=90)
    if mode.startswith('(:kill '):
        assert result.returncode == -9, (mode, result.returncode, log)
        tokens = mode[1:-1].split(' ', 2)
        assert f'LOGICAL-INSTALL-ROWS-KILL {tokens[1].upper()} {tokens[2]}' in log.read_text(), log
        fixed = True
        assert os.path.lexists(peer.data / 'logical-slot-copy.install') == fixed
        print(f'LOGICAL-INSTALL-ROWS-KILLED {mode} seconds={time.monotonic() - started:.6f}', flush=True)
        probe(peer, 'recover' if fixed else 'absent', label='restart-' + mode, saved=saved)
        return
    assert result.returncode == 0, (mode, result.returncode, log)
    assert f'LOGICAL-INSTALL-ROWS-PASS {printed_mode(mode)}' in log.read_text(), (mode, log)
    check(peer, saved)
    assert not (peer.data / 'postmaster.pid').exists()
    print(f'LOGICAL-INSTALL-ROWS-CASE {label} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def live(peer, source, fault='throw'):
    del source
    for path in fixtures():
        peer.control(f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(path)}))')
    for mode in (('fresh', 'physical') if QUICK else ('fresh', 'physical', fault)):
        saved = snapshot(peer)
        started = time.monotonic()
        peer.control(f'(logical-install-rows-test-start {mode_form(mode)})')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert peer.process.poll() is None, peer.root / 'server.log'
            status = peer.control('*logical-install-rows-test-result*', timeout=90)
            if ':PASSED' in status:
                peer.control('(assert (not (eq :timeout (sb-thread:join-thread *logical-install-rows-test-thread* :timeout 5 :default :timeout))))')
                break
            assert ':ERROR' not in status, (mode, status, peer.root / 'server.log')
            time.sleep(0.05)
        else:
            raise TimeoutError((mode, peer.root))
        check(peer, saved)
        print(f'LOGICAL-INSTALL-ROWS-LIVE {mode} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def separate_slot_root(peer):
    saved = snapshot(peer)
    target = peer.data / 'pg_replslot'
    target.mkdir(mode=0o700, exist_ok=True)
    command = ['sudo', '-n', 'mount', '-t', 'tmpfs', '-o',
               f'size=16M,noswap,mode=0700,uid={os.geteuid()},gid={os.getegid()}',
               'pg-lisp-rows-fixture', str(target)]
    subprocess.run(command, check=True, capture_output=True, text=True)
    try:
        assert target.stat().st_dev != peer.data.stat().st_dev
        probe(peer, 'separate-slot-root')
    finally:
        subprocess.run(['sudo', '-n', 'umount', str(target)], check=True)
    check(peer, saved)


def cold(peer, optional):
    del optional
    try:
        for mode in (('fresh', 'resume-one', 'resume-all', 'unspent') if QUICK else MODES):
            probe(peer, mode)
        if not QUICK:
            separate_slot_root(peer)
            for name in ('alpha', 'beta'):
                for point in POINTS:
                    probe(peer, f'(:kill :{point} "{name}")')
    finally:
        target = peer.data / 'pg_replslot' / '.logical-slot-copy.work'
        if base['stage']['mounted'](target):
            subprocess.run(['sudo', '-n', 'umount', str(target)], check=True)



if __name__ == '__main__':
    api['run'].__globals__['cold_matrix'] = cold
    for optional in ((False,) if QUICK else (False, True)):
        fault = '(:fsync 8)' if optional else 'throw'
        api['run'](optional, before_stop=lambda peer, source: live(peer, source, fault))

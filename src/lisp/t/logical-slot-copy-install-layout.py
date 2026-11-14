"""Exercise installation layouts with owned manual moves, then restore the fork."""
import os
from pathlib import Path
import re
import runpy
import subprocess
import time

base = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-install-handoff.py')))
api, image, root_fact = base['api'], base['image'], base['root_fact']
LISP, lisp_string, WORKSPACES = base['LISP'], base['lisp_string'], base['WORKSPACES']
HOOK = Path(__file__).with_name('logical-slot-copy-install-layout-native.lisp')
MODES = ('positive', 'historical-mounts', 'closed-journal', 'cleanup-journal', 'inspection-close-error', 'body-close-error', 'caught-restore', 'journal', 'row-bytes', 'hardlink', 'fifo',
         'duplicate', 'capacity', 'bind-container', 'nonprefix', 'private-active', 'early-active', 'foreign-logical',
         'extra-journal', 'pending-inode', 'slot-inode', 'positive')


def snapshot(peer):
    return (image(peer.data), root_fact(peer.data),
            {name: image(peer.data / name, staging=True) for name in WORKSPACES})


def check(peer, saved):
    canonical, root, orphans = saved
    base['check_canonical'](peer, canonical, image(peer.data))
    assert root_fact(peer.data) == root
    assert not os.path.lexists(peer.data / 'logical-slot-copy.install')
    assert sorted(p.name for p in (peer.data / 'pg_replslot').iterdir()) == ['.logical-slot-copy.work']
    for name, old in orphans.items():
        now = image(peer.data / name, staging=True)
        assert all(now.get(key) == value for key, value in old.items()), (name, 'old orphan changed')


def fixtures():
    return [HOOK.with_name(name) for name in ('physical-directory-root-native.lisp', 'logical-slot-copy-install-stage-native.lisp',
            'logical-slot-copy-install-handoff-native.lisp', 'logical-slot-copy-install-layout-native.lisp')]


def probe(peer, mode, label=None):
    label = label or mode
    saved = snapshot(peer)
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {lisp_string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             '(setf *print-circle* t *print-length* nil *print-level* nil)']
    for path in fixtures():
        forms.append(f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(path)}))')
    forms.append(f'(let ((*pgdata* (directory-pathname {lisp_string(peer.data)}))) (unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 :startup-reconcile (lambda () (logical-install-layout-test-run :{mode}))) (when *postmaster-pid-stream* (postmaster-remove-pid-file)) (when *postmaster-data-lock* (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil))))')
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'layout-{mode}-{time.monotonic_ns()}.log'
    started = time.monotonic()
    with log.open('w') as out:
        result = subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, timeout=90)
    assert result.returncode == 0, (mode, result.returncode, log)
    assert f'LOGICAL-LAYOUT-PASS :{mode.upper()}' in log.read_text(), (mode, log)
    check(peer, saved)
    assert not (peer.data / 'postmaster.pid').exists()
    print(f'LOGICAL-LAYOUT-CASE {label} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def live(peer, source):
    del source
    for path in fixtures():
        peer.control(f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(path)}))')
    for mode in ('positive', 'historical-mounts'):
        saved = snapshot(peer)
        started = time.monotonic()
        peer.control(f'(logical-install-layout-test-start :{mode})')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert peer.process.poll() is None, peer.root / 'server.log'
            status = peer.control('*logical-install-handoff-test-result*', timeout=90)
            if ':PASSED' in status:
                peer.control('(assert (not (eq :timeout (sb-thread:join-thread *logical-install-handoff-test-thread* :timeout 5 :default :timeout))))')
                break
            assert ':ERROR' not in status, (mode, status, peer.root / 'server.log')
            time.sleep(0.05)
        else:
            raise TimeoutError((mode, peer.root))
        check(peer, saved)
        print(f'LOGICAL-LAYOUT-LIVE {mode} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def cold(peer, optional):
    assert not optional
    for mode in MODES:
        probe(peer, mode)
    path = peer.data / 'pg_replslot'
    original, saved = path.stat(), image(path, staging=True)
    try:
        options = f'size=16m,noswap,nodev,nosuid,noexec,uid={os.geteuid()},gid={os.getegid()},mode=700'
        subprocess.run(['sudo', '-n', 'mount', '-t', 'tmpfs', '-o', options,
                        'pg-lisp-layout-native', str(path)], check=True)
        assert path.stat().st_dev != peer.data.stat().st_dev
        probe(peer, 'positive', 'separate-filesystem')
        probe(peer, 'positive', 'separate-filesystem-retry')
    finally:
        if base['stage']['mounted'](path):
            subprocess.run(['sudo', '-n', 'umount', str(path)], check=True)
    assert (path.stat().st_dev, path.stat().st_ino) == (original.st_dev, original.st_ino)
    assert image(path, staging=True) == saved
    probe(peer, 'positive', 'after-mount-retry')


if __name__ == '__main__':
    api['run'].__globals__['cold_matrix'] = cold
    api['run'](before_stop=live)

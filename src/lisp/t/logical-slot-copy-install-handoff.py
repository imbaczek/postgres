"""Verify fresh handoffs across closed preparation scopes on a real child fork."""
import os
from pathlib import Path
import re
import runpy
import stat
import subprocess
import time

stage = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-install-stage.py')))
api, image, root_fact = stage['api'], stage['image'], stage['root_fact']
LISP, lisp_string, WORKSPACES = api['LISP'], api['lisp_string'], stage['WORKSPACES']
HOOK = Path(__file__).with_name('logical-slot-copy-install-handoff-native.lisp')
FAILURES = {
    'invalid-input': 'exact child control',
    'before-stage': 'handoff phase cut :BEFORE-STAGE',
    'after-stage': 'handoff phase cut :AFTER-STAGE',
    'after-preparation': 'handoff phase cut :AFTER-PREPARATION',
    'after-mint': 'handoff phase cut :AFTER-MINT',
    'after-preparation-throw': 'handoff nonlocal or revoked exit',
    'source-reproof': 'handoff final source cut',
    'callback-error': 'handoff callback cut',
    'callback-throw': 'handoff nonlocal or revoked exit',
    'root-bind': 'scope ended', 'mode-swap': 'scope ended',
    'lock-swap': 'scope ended', 'gate-swap': 'scope ended',
    'root-close': 'handoff outer root close cut',
    'final-ownership': 'handoff final ownership cut',
    'reuse-orphan': 'cannot exclusively create logical installation directory',
}


def check_canonical(peer, before, after):
    if 'pg_replslot' not in before and 'pg_replslot' in after:
        after = dict(after)
        device, inode, mode, uid, gid, contents = after.pop('pg_replslot')
        assert device == peer.data.stat().st_dev and inode > 0
        assert stat.S_ISDIR(mode) and stat.S_IMODE(mode) == 0o700
        assert (uid, gid, contents) == (os.geteuid(), os.getegid(), None)
        print('LOGICAL-HANDOFF-CREATED-SLOT-ROOT', flush=True)
    changed = sorted(name for name in before.keys() | after.keys()
                     if before.get(name) != after.get(name))
    assert not changed, ('canonical entries changed', changed)


def probe(peer, mode, old_token=None):
    before, root = image(peer.data), root_fact(peer.data)
    orphans = {name: image(peer.data / name, staging=True) for name in WORKSPACES}
    roots = {name: root_fact(peer.data / name) for name in WORKSPACES}
    form = f'''(let ((*pgdata* (directory-pathname {lisp_string(peer.data)})))
      (unwind-protect
           (postmaster-own-data-directory "127.0.0.1" 6543
             :startup-reconcile (lambda () (logical-install-handoff-test-cold :{mode})))
        (when *postmaster-pid-stream* (postmaster-remove-pid-file))
        (when *postmaster-data-lock*
          (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil))))'''
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {lisp_string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             '(setf *print-circle* t *print-length* nil *print-level* nil)',
             f'(load {lisp_string(HOOK.with_name("physical-directory-root-native.lisp"))})',
             f'(load {lisp_string(HOOK.with_name("logical-slot-copy-install-stage-native.lisp"))})',
             f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(HOOK)}))',
             form]
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for code in forms:
        command.extend(['--eval', code])
    environment = dict(os.environ)
    if old_token:
        environment['PG_LISP_HANDOFF_OLD_STAGE'] = old_token
    log = peer.root / f'handoff-{mode}.log'
    started = time.monotonic()
    try:
        with log.open('w') as output:
            result = subprocess.run(command, env=environment, stdout=output, stderr=subprocess.STDOUT, timeout=90)
    finally:
        if stage['mounted'](peer.data):
            subprocess.run(['sudo', '-n', 'umount', str(peer.data)], check=True)
    text = log.read_text()
    failure = mode in FAILURES
    assert result.returncode == (1 if failure else 0), (mode, result.returncode, log)
    if failure:
        report = re.search(r'LOGICAL-HANDOFF-EXPECTED-FAILURE :' + re.escape(mode.upper()) + r': ([^\n]+)', text)
        assert report and FAILURES[mode] in report.group(1), (mode, log)
    else:
        assert ('LOGICAL-INSTALL-ROOT-CLOSE-PASS' if mode.startswith('root-') else 'LOGICAL-HANDOFF-PASS') in text
    if mode == 'root-bind':
        assert 'LOGICAL-HANDOFF-BIND-REJECT' in text
    check_canonical(peer, before, image(peer.data))
    assert root_fact(peer.data) == root
    assert not (peer.data / 'postmaster.pid').exists()
    assert not os.path.lexists(peer.data / 'logical-slot-copy.install')
    assert sorted(p.name for p in (peer.data / 'pg_replslot').iterdir()) == ['.logical-slot-copy.work']
    for name, saved in orphans.items():
        after = image(peer.data / name, staging=True)
        assert all(after.get(key) == value for key, value in saved.items()), (mode, name, 'old orphan changed')
        assert root_fact(peer.data / name) == roots[name]
        if mode in ('invalid-input', 'before-stage', 'reuse-orphan') or mode.startswith('root-') and mode != 'root-bind' and mode != 'root-close':
            assert after == saved, (mode, name, 'unexpected stage mutation')
    print(f'LOGICAL-HANDOFF-CASE {mode} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def live(peer, source):
    del source
    peer.control(f'(load {lisp_string(HOOK.with_name("physical-directory-root-native.lisp"))})')
    peer.control(f'(load {lisp_string(HOOK.with_name("logical-slot-copy-install-stage-native.lisp"))})')
    peer.control(f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(HOOK)}))')
    for mode in ('healthy', 'cross-operation'):
        before, root = image(peer.data), root_fact(peer.data)
        orphans = {name: image(peer.data / name, staging=True) for name in WORKSPACES}
        roots = {name: root_fact(peer.data / name) for name in WORKSPACES}
        started = time.monotonic()
        peer.control(f'(logical-install-handoff-test-start :{mode})')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert peer.process.poll() is None, peer.root / 'server.log'
            result = peer.control('*logical-install-handoff-test-result*', timeout=90)
            if ':PASSED' in result:
                peer.control('(assert (not (eq :timeout (sb-thread:join-thread *logical-install-handoff-test-thread* :timeout 5 :default :timeout))))')
                break
            assert ':ERROR' not in result, (mode, result, peer.root / 'server.log')
            time.sleep(0.05)
        else:
            raise TimeoutError((mode, peer.root))
        check_canonical(peer, before, image(peer.data))
        assert root_fact(peer.data) == root
        for name, saved in orphans.items():
            after = image(peer.data / name, staging=True)
            assert all(after.get(key) == value for key, value in saved.items()), (mode, name, 'old orphan changed')
            if roots[name] is not None:
                assert root_fact(peer.data / name) == roots[name]
        print(f'LOGICAL-HANDOFF-LIVE {mode} PASS seconds={time.monotonic() - started:.6f}', flush=True)


def cold(peer, optional):
    assert not optional
    token = next(p.name for p in sorted((peer.data / WORKSPACES[0]).iterdir())
                 if re.fullmatch(r'[0-9a-f]{32}', p.name))
    for mode in ('root-error', 'root-throw', 'root-normal'):
        probe(peer, mode)
    path = peer.data / 'global/pg_wal_control'
    saved = path.read_bytes()
    changed = bytearray(saved)
    changed[0] ^= 1
    path.write_bytes(changed)
    try:
        probe(peer, 'invalid-input')
    finally:
        path.write_bytes(saved)
    for mode in ('healthy', 'cross-operation', *tuple(name for name in FAILURES if name != 'invalid-input')):
        probe(peer, mode, token)
    probe(peer, 'healthy-retry')


if __name__ == '__main__':
    api['run'].__globals__['cold_matrix'] = cold
    api['run'](before_stop=live)

"""Exercise unpublished logical-slot staging on a real committed child copy."""
import os
from pathlib import Path
import re
import runpy
import signal
import stat
import subprocess
import time

api = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-committed-source.py')))
LISP, lisp_string = api['LISP'], api['lisp_string']
WORKSPACES = ('logical-slot-copy.work', 'pg_replslot/.logical-slot-copy.work')
FAILURES = {
    'value': 'stage image changed', 'image': 'stage image changed',
    'extra': 'container inventory changed', 'mode': 'binding or privacy changed',
    'nonlocal': 'nonlocal staging exit', 'partial': 'logical staging partial write cut',
    'journal-rewrite': 'staged files or observations changed',
    'journal-replace': 'binding or privacy changed', 'journal-symlink': 'binding or privacy changed',
    'state-hardlink': 'binding or privacy changed', 'rename-container': 'binding or privacy changed',
    'parent-binding': 'binding or privacy changed', 'bind': 'binding or privacy changed',
    'close': 'logical staging close cut', 'body-close': 'logical staging original body cut',
    'mkdir-error': 'cannot exclusively create logical installation directory',
    'open-error': 'cannot open logical installation inode',
    'parent-sync': 'logical staging fsync cut :PARENT-SYNC',
    'file-sync': 'logical staging fsync cut :FILE-SYNC',
    'read-error': 'logical staging read cut', 'write-error': 'logical staging write cut',
    'pg-collision': 'cannot exclusively create logical installation directory',
    'slot-collision': 'cannot exclusively create logical installation directory',
    'pg-work-file': 'cannot open logical installation inode',
    'slot-work-symlink': 'cannot open logical installation inode',
}


def root_fact(root):
    if not os.path.lexists(root):
        return None
    facts = root.lstat()
    contents = (root.read_bytes() if stat.S_ISREG(facts.st_mode) else
                os.readlink(root) if stat.S_ISLNK(facts.st_mode) else None)
    return facts.st_dev, facts.st_ino, facts.st_mode, facts.st_uid, facts.st_gid, contents


def image(root, *, staging=False):
    result = {}
    if not os.path.lexists(root) or not stat.S_ISDIR(root.lstat().st_mode):
        return result
    for directory, children, files in os.walk(root, followlinks=False):
        parent = Path(directory)
        children[:] = sorted(name for name in children
                             if staging or str((parent / name).relative_to(root)) not in WORKSPACES)
        for name in sorted(children + files):
            path = parent / name
            relative = str(path.relative_to(root))
            if name == 'postmaster.pid' or (not staging and relative in WORKSPACES):
                continue
            facts = path.lstat()
            if staging:
                result[relative] = (facts.st_dev, facts.st_ino, facts.st_mode, facts.st_uid, facts.st_gid,
                                    facts.st_nlink, facts.st_mtime_ns, facts.st_ctime_ns,
                                    path.read_bytes() if stat.S_ISREG(facts.st_mode) else None)
            else:
                contents = (path.read_bytes() if stat.S_ISREG(facts.st_mode) else
                            os.readlink(path) if stat.S_ISLNK(facts.st_mode) else None)
                result[relative] = (facts.st_dev, facts.st_ino, facts.st_mode,
                                    facts.st_uid, facts.st_gid, contents)
    return result


def staged_probe(peer, label, mode=None, *, cut=None):
    before = image(peer.data)
    orphans = {name: image(peer.data / name, staging=True) for name in WORKSPACES}
    workspace_roots = {name: root_fact(peer.data / name) for name in WORKSPACES}
    root_identity = root_fact(peer.data)
    cut_form = f"'(:{cut[0]} {cut[1]} {cut[2]})" if cut else 'nil'
    form = f'''(let ((*pgdata* (directory-pathname {lisp_string(peer.data)})))
      (unwind-protect
           (postmaster-own-data-directory "127.0.0.1" 6543
             :startup-reconcile (lambda () (logical-install-stage-test-cold {':' + mode if mode else 'nil'} {cut_form})))
        (when *postmaster-pid-stream* (postmaster-remove-pid-file))
        (when *postmaster-data-lock*
          (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil))))'''
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {lisp_string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             f'(load {lisp_string(Path(__file__).with_name("logical-slot-copy-import-input-native.lisp"))})',
             f'(load {lisp_string(Path(__file__).with_name("logical-slot-copy-import-commit-native.lisp"))})',
             f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(Path(__file__).with_name("logical-slot-copy-install-stage-native.lisp"))}))',
             form]
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for code in forms:
        command.extend(['--eval', code])
    log = peer.root / f'staged-{label}.log'
    started = time.monotonic()
    outside = peer.root / 'outside-stage'
    outside.touch(exist_ok=True)
    outside_directory = peer.root / 'outside-stage-directory'
    outside_directory.mkdir(mode=0o700, exist_ok=True)
    environment = dict(os.environ, PG_LISP_STAGE_OUTSIDE=str(outside),
                       PG_LISP_STAGE_OUTSIDE_DIRECTORY=str(outside_directory))
    with log.open('w') as output:
        result = subprocess.run(command, env=environment, stdout=output, stderr=subprocess.STDOUT, timeout=90)
    text = log.read_text()
    if cut:
        assert result.returncode == -signal.SIGKILL and 'LOGICAL-STAGE-KILL' in text, (label, result.returncode, log)
        (peer.data / 'postmaster.pid').unlink()
    else:
        failure = mode not in (None, 'trace', 'authority')
        marker = 'LOGICAL-STAGE-EXPECTED-FAILURE' if failure else 'LOGICAL-INSTALL-STAGED-PASS'
        assert result.returncode == (1 if failure else 0) and marker in text, (label, result.returncode, log)
        if failure:
            report = re.search(r'LOGICAL-STAGE-EXPECTED-FAILURE :' + re.escape(mode.upper()) + r': ([^\n]+)', text)
            assert report and FAILURES[mode] in report.group(1), (label, log)
        if mode == 'bind':
            assert 'LOGICAL-STAGE-BIND-MOUNT-CHANGED' in text
        if mode == 'body-close':
            assert re.search(r'LOGICAL-STAGE-EXPECTED-FAILURE :BODY-CLOSE: logical staging original body cut', text)
    assert outside.read_bytes() == b''
    assert list(outside_directory.iterdir()) == []
    assert image(peer.data) == before, (label, 'canonical file changed')
    assert root_fact(peer.data) == root_identity
    assert not (peer.data / 'postmaster.pid').exists()
    assert not os.path.lexists(peer.data / 'logical-slot-copy.install')
    assert sorted(p.name for p in (peer.data / 'pg_replslot').iterdir()) == ['.logical-slot-copy.work']
    for name, saved in orphans.items():
        after = image(peer.data / name, staging=True)
        assert all(after.get(key) == value for key, value in saved.items()), (label, name, 'old orphan changed')
        if workspace_roots[name] is not None:
            assert root_fact(peer.data / name) == workspace_roots[name], (label, name, 'workspace root changed')
    print(f'LOGICAL-STAGED-CASE {label} PASS seconds={time.monotonic() - started:.6f}', flush=True)
    return text


def mounted(path):
    return any(line.split()[4] == str(path) for line in Path('/proc/self/mountinfo').read_text().splitlines())


def mount_matrix(peer):
    path = peer.data / 'pg_replslot'
    original = path.stat()
    saved = image(path, staging=True)
    try:
        options = f'size=16m,noswap,nodev,nosuid,noexec,uid={os.geteuid()},gid={os.getegid()},mode=700'
        subprocess.run(['sudo', '-n', 'mount', '-t', 'tmpfs', '-o', options,
                        'pg-lisp-stage-native', str(path)], check=True)
        assert path.stat().st_dev != peer.data.stat().st_dev
        staged_probe(peer, 'separate-filesystem')
        staged_probe(peer, 'separate-filesystem-retry')
    finally:
        if mounted(path):
            subprocess.run(['sudo', '-n', 'umount', str(path)], check=True)
    assert (path.stat().st_dev, path.stat().st_ino) == (original.st_dev, original.st_ino)
    assert image(path, staging=True) == saved
    try:
        staged_probe(peer, 'bind-mount', 'bind')
    finally:
        if mounted(path):
            subprocess.run(['sudo', '-n', 'umount', str(path)], check=True)
    staged_probe(peer, 'bind-mount-retry')


def staged_matrix(peer, optional):
    assert not optional
    for name in WORKSPACES:
        root = peer.data / name
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        opaque = root / 'unowned'
        opaque.mkdir(mode=0o700)
        (opaque / 'keep').write_bytes(b'opaque existing workspace')
        os.mkfifo(opaque / 'fifo', 0o600)
    staged_probe(peer, 'authority', 'authority')
    staged_probe(peer, 'healthy')
    for mode in ('value', 'image', 'extra', 'mode', 'nonlocal', 'partial', 'journal-rewrite',
                 'journal-replace', 'journal-symlink', 'state-hardlink', 'rename-container',
                 'parent-binding', 'close', 'body-close', 'mkdir-error', 'open-error',
                 'parent-sync', 'file-sync', 'write-error', 'read-error'):
        staged_probe(peer, mode, mode)
    for mode, root, token in (
            ('pg-collision', WORKSPACES[0], '00000000000000000000000000000001'),
            ('slot-collision', WORKSPACES[1], '00000000000000000000000000000002')):
        collision = peer.data / root / token
        collision.mkdir(mode=0o700)
        os.mkfifo(collision / 'opaque', 0o600)
        staged_probe(peer, mode, mode)
    for mode, name in (('pg-work-file', WORKSPACES[0]), ('slot-work-symlink', WORKSPACES[1])):
        workspace = peer.data / name
        saved = peer.root / ('saved-' + mode)
        before = image(workspace, staging=True)
        workspace.rename(saved)
        try:
            if mode == 'pg-work-file':
                workspace.write_bytes(b'opaque non-directory workspace')
            else:
                workspace.symlink_to(saved, target_is_directory=True)
            staged_probe(peer, mode, mode)
        finally:
            workspace.unlink()
            saved.rename(workspace)
        assert image(workspace, staging=True) == before
    staged_probe(peer, 'healthy-retry')
    trace = staged_probe(peer, 'trace', 'trace')
    cuts = [(point, name, int(ordinal)) for point, name, ordinal in
            re.findall(r'LOGICAL-STAGE-EVENT :([A-Z-]+) (NIL|:NONCE|"[^"]+") ([0-9]+)', trace)]
    assert cuts and {cut[0] for cut in cuts} == {
        'MKDIR', 'AFTER-INSTALL-INODE-OPEN', 'AFTER-INSTALL-PARENT-FSYNC',
        'AFTER-INSTALL-FILE-WRITE', 'AFTER-INSTALL-INODE-FSYNC'}
    assert len(cuts) == len(set(cuts)), cuts
    print(f'LOGICAL-STAGED-CRASH-CUTS {len(cuts)}', flush=True)
    for index, cut in enumerate(cuts):
        staged_probe(peer, f'kill-{index}', cut=cut)
        staged_probe(peer, f'kill-{index}-retry')
    mount_matrix(peer)


if __name__ == '__main__':
    api['run'].__globals__['cold_matrix'] = staged_matrix
    api['run']()

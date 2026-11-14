"""Restore actual selected-R catalog after DDL beyond the copied base."""
import os
from pathlib import Path
import runpy
import re
import subprocess
import signal
import stat
import sys
import time

api = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-import-input.py')))
standby = api['standby']
OwnedLisp = api['OwnedLisp']
string = api['lisp_string']
LISP = standby['api']['LISP']
base_catalogs = {}
dropped_nodes = {}
peers = []
original_start = OwnedLisp.start
original_insert = standby['insert']
original_probe = api['probe']
optional = False


def start(peer):
    catalog = peer.data / 'global/pg_catalog.dat'
    if catalog.exists():
        base_catalogs[peer] = catalog.read_bytes()
    result = original_start(peer)
    if not peers:
        connection = standby['RawConnection'](peer.port)
        try:
            connection.query('CREATE TABLE dropped_since_base (id integer)')
        finally:
            connection.close()
    peers.append(peer)
    return result


def insert(source, row):
    child = peers[-1]
    child.control(f'(load {string(Path(__file__).with_name("logical-slot-copy-cold-catalog-native.lisp"))})')
    reply = child.control('(logical-cold-catalog-dropped-node)')
    dropped_nodes[child] = int(re.search(r'COLD-CATALOG-BASE-NODE ([0-9]+)', reply).group(1))
    original_insert(source, row)
    connection = standby['RawConnection'](source.port)
    try:
        connection.query('DROP TABLE dropped_since_base')
        connection.query('CREATE TABLE restored_since_base (id integer)')
    finally:
        connection.close()
    child = peers[-1]
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        connection = standby['RawConnection'](child.port)
        try:
            connection.query('SELECT id FROM restored_since_base')
            return
        except Exception:
            time.sleep(0.05)
        finally:
            connection.close()
    raise TimeoutError('new catalog did not reach copied standby')


def prepare(peer, *args, **kwargs):
    original_probe(peer, *args, **kwargs)
    if optional:
        peer.control(f'(load {string(Path(__file__).with_name("logical-slot-copy-import-commit-native.lisp"))})')
        peer.control(f'(load {string(Path(__file__).with_name("logical-slot-copy-cold-catalog-native.lisp"))})')
        peer.control('(logical-cold-catalog-prepare-optional)')
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            result = peer.control('*logical-cold-catalog-optional-result*')
            if ':PASSED' in result:
                peer.control('(assert (not (eq :timeout (sb-thread:join-thread *logical-cold-catalog-optional-thread* :timeout 5 :default :timeout))))')
                return
            assert ':ERROR' not in result, (result, peer.root / 'server.log')
            time.sleep(0.05)
        raise TimeoutError('optional C3 preparation')


def image(root):
    result = {}
    for path in sorted(root.rglob('*')):
        relative = str(path.relative_to(root))
        if relative == 'postmaster.pid':
            continue
        info = path.lstat()
        payload = (path.read_bytes() if stat.S_ISREG(info.st_mode)
                   else os.readlink(path) if stat.S_ISLNK(info.st_mode) else None)
        result[relative] = (info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
                            info.st_uid, info.st_gid, payload)
    return result


def probe(peer, label, mode=':healthy', kill=None):
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             f'(load {string(Path(__file__).with_name("logical-slot-copy-cold-catalog-native.lisp"))})',
             f'(let ((*pgdata* (directory-pathname {string(peer.data)}))) '
             '(unwind-protect (postmaster-own-data-directory "127.0.0.1" 6543 '
             f':startup-reconcile (lambda () (logical-cold-catalog-test-run \'{mode}))) '
             '(when *postmaster-pid-stream* (postmaster-remove-pid-file)) '
             '(when *postmaster-data-lock* (close *postmaster-data-lock*) '
             '(setf *postmaster-data-lock* nil))))']
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'cold-catalog-{label}.log'
    started = time.monotonic()
    with log.open('w') as output:
        status = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=90).returncode
    text = log.read_text()
    if kill:
        assert status == -signal.SIGKILL and f'COLD-CATALOG-KILL :{kill.upper()}' in text, (status, log)
        print(f'COLD-CATALOG-KILLED {label} seconds={time.monotonic()-started:.6f}', flush=True)
        return
    assert status == 0 and 'COLD-CATALOG-RESTORE-PASS' in text, (status, log)
    assert not (peer.data / 'postmaster.pid').exists()
    print(f'COLD-CATALOG-CASE {label} PASS seconds={time.monotonic()-started:.6f}', flush=True)


FAULTS = ('source', 'restartpoint', 'pending', 'evidence', 'parent', 'target', 'extra',
          'binding', 'fd-field', 'seal-field', 'caught-source', 'caught-registry', 'caught-create',
          'caught-write', 'caught-sync', 'caught-rename', 'open', 'read', 'create',
          'write', 'temp-sync', 'old-sync', 'file-sync', 'directory-sync', 'rename',
          'rename-after', 'unlink', 'unlink-after', 'close', 'close-interrupt',
          'store-close', 'root-close', 'throw-before', 'throw-after',
          'late-source', 'late-restartpoint', 'late-target', 'late-extra')
CUTS = ('after-cold-catalog-open', 'before-cold-catalog-create', 'after-cold-catalog-create',
        'after-cold-catalog-write', 'after-cold-catalog-temp-sync', 'after-cold-catalog-old-sync',
        'before-cold-catalog-rename', 'after-cold-catalog-rename', 'after-cold-catalog-file-sync',
        'after-cold-catalog-directory-sync', 'before-cold-catalog-close', 'after-cold-catalog-close',
        'after-cold-catalog-execute', 'before-cold-catalog-unlink', 'after-cold-catalog-unlink',
        'after-cold-catalog-unlink-sync')


def matrix(peer, disposition):
    assert disposition == 'eligible'
    target = peer.data / 'global/pg_catalog.dat'
    temporary = peer.data / 'global/pg_catalog.tmp'
    selected = target.read_bytes()
    base = base_catalogs[peer]
    assert selected != base
    assert b'restored_since_base' in selected and b'restored_since_base' not in base
    assert b'dropped_since_base' in base and b'dropped_since_base' not in selected
    assert not (peer.data / 'base' / str(dropped_nodes[peer])).exists(), 'DROP did not remove the old base fork'
    structural = ('empty', 'partial', 'full', 'already-current', 'current-with-residue',
                  'foreign', 'oversize', 'symlink', 'hardlink', 'fifo', 'directory', 'mode',
                  'control-temp', 'foreign-global', 'target-missing', 'target-symlink',
                  'target-hardlink', 'target-fifo', 'target-directory', 'target-mode')
    cases = [('stale', ':healthy'), ('forged', ':forged'), ('copied-forged', ':copied-forged')]
    cases += [(name, ':healthy' if name in structural[:5] else ':reject') for name in structural]
    cases += [(name, ':'+name) for name in FAULTS]
    cases += [(name, ':'+name) for name in ('dirty-catalog', 'dirty-store', 'dirty-ready')]
    if '--smoke' in sys.argv:
        cases = [x for x in cases if x[0] in ('stale', 'forged', 'copied-forged', 'partial', 'fd-field', 'seal-field', 'caught-write', 'caught-registry')]
    for label, mode in cases:
        target.write_bytes(selected if label in ('already-current', 'current-with-residue') else base)
        backup = peer.root / 'catalog-backup'
        external = peer.root / 'catalog-external'
        control_temp = peer.data / 'global/pg_wal_control.tmp'
        foreign = peer.data / 'global/catalog-foreign-leaf'
        if label in ('empty', 'partial', 'full', 'current-with-residue', 'unlink', 'unlink-after'):
            temporary.write_bytes(b'' if label == 'empty' else selected if label == 'full' else selected[:len(selected)//2])
            temporary.chmod(0o600)
        elif label in ('foreign', 'oversize', 'mode'):
            temporary.write_bytes((bytes([selected[0]^1])+selected[1:]) if label == 'foreign'
                                  else selected+b'x' if label == 'oversize' else selected)
            temporary.chmod(0o644 if label == 'mode' else 0o600)
        elif label == 'symlink':
            external.write_bytes(selected);external.chmod(0o600);temporary.symlink_to(external)
        elif label == 'hardlink':
            external.write_bytes(selected);external.chmod(0o600);os.link(external, temporary)
        elif label == 'fifo':
            os.mkfifo(temporary, 0o600)
        elif label == 'directory':
            temporary.mkdir(mode=0o700)
        elif label == 'control-temp':
            control_temp.write_bytes(b'control');control_temp.chmod(0o600)
        elif label == 'foreign-global':
            foreign.write_bytes(b'foreign');foreign.chmod(0o600)
        elif label.startswith('target-'):
            target.rename(backup)
            if label == 'target-symlink':
                external.write_bytes(base);external.chmod(0o600);target.symlink_to(external)
            elif label == 'target-hardlink':
                external.write_bytes(base);external.chmod(0o600);os.link(external, target)
            elif label == 'target-fifo':
                os.mkfifo(target, 0o600)
            elif label == 'target-directory':
                target.mkdir(mode=0o700)
            elif label == 'target-mode':
                target.write_bytes(base);target.chmod(0o644)
        before = image(peer.data)
        external_before = external.read_bytes() if external.exists() else None
        try:
            probe(peer, label, mode)
            after = image(peer.data)
            if mode == ':reject' or label in ('forged', 'dirty-catalog', 'dirty-store', 'dirty-ready'):
                assert before == after, (label, 'rejected probe changed disk')
            else:
                allowed = {'global/pg_catalog.dat', 'global/pg_catalog.tmp'}
                assert {k:v for k,v in before.items() if k not in allowed} == {k:v for k,v in after.items() if k not in allowed}, (label, 'unrelated file changed')
                committed = (mode in (':healthy', ':copied-forged') or label in ('file-sync', 'directory-sync', 'rename-after',
                             'close', 'close-interrupt', 'store-close', 'root-close', 'throw-after')
                             or label.startswith('late-'))
                assert target.read_bytes() == (selected if committed else base), (label, 'catalog publication boundary')
                if label in ('already-current', 'current-with-residue'):
                    assert before['global/pg_catalog.dat'] == after['global/pg_catalog.dat'], (label, 'current catalog inode replaced')
            if external_before is not None:
                assert external.read_bytes() == external_before, (label, 'outside witness changed')
        finally:
            for path in (temporary, control_temp, foreign):
                if path.is_dir() and not path.is_symlink():path.rmdir()
                else:path.unlink(missing_ok=True)
            if backup.exists():
                if target.is_dir() and not target.is_symlink():target.rmdir()
                else:target.unlink(missing_ok=True)
                backup.rename(target)
            external.unlink(missing_ok=True)
            target.chmod(0o600)
    if '--smoke' not in sys.argv:
        for point in CUTS:
            target.write_bytes(base)
            if 'unlink' in point:
                temporary.write_bytes(selected[:len(selected)//2]);temporary.chmod(0o600)
            before = image(peer.data)
            probe(peer, 'kill-'+point, f'(:kill :{point})', kill=point)
            probe(peer, 'restart-'+point)
            after = image(peer.data)
            allowed = {'global/pg_catalog.dat', 'global/pg_catalog.tmp'}
            assert {k:v for k,v in before.items() if k not in allowed} == {k:v for k,v in after.items() if k not in allowed}, (point, 'restart changed unrelated files')
            assert target.read_bytes() == selected and not temporary.exists(), point
    target.write_bytes(selected)
    probe(peer, 'final-idempotent')
    print(f'COLD-CATALOG-WORKFLOW-PASS optional={optional}', flush=True)



def committed_rejection(peer, optional_control):
    target = peer.data / 'global/pg_catalog.dat'
    temporary = peer.data / 'global/pg_catalog.tmp'
    catalog = target.read_bytes()
    for label in ('committed', 'committed-temp', 'committed-stale'):
        if label == 'committed-temp':
            temporary.write_bytes(catalog[:len(catalog)//2]);temporary.chmod(0o600)
        if label == 'committed-stale':
            target.write_bytes(catalog+b'\n ')
        before = image(peer.data)
        try:
            probe(peer, label, ':reject')
            assert before == image(peer.data), (label, 'parent restoration changed committed child')
        finally:
            temporary.unlink(missing_ok=True)
            target.write_bytes(catalog)
    print(f'COLD-CATALOG-CF-REJECTION-PASS optional={optional_control}', flush=True)


if __name__ == '__main__':
    OwnedLisp.start = start
    standby['insert'] = insert
    api['run_healthy'].__globals__['probe'] = prepare
    api['run_healthy'].__globals__['evidence_crash_matrix'] = lambda *args: None
    api['run_healthy'].__globals__['pending_crash_matrix'] = matrix
    for optional in ((False,) if '--smoke' in sys.argv else (False, True)):
        peers.clear();base_catalogs.clear();dropped_nodes.clear()
        api['run_healthy']()
    if '--smoke' not in sys.argv:
        OwnedLisp.start = original_start
        standby['insert'] = original_insert
        committed = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-committed-source.py')))
        committed['run'].__globals__['cold_matrix'] = committed_rejection
        committed['run']()
        committed['run'](optional=True)

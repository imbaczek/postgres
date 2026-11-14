"""Reconcile real pre-rename child-control cuts without opening storage."""
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import time

api = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-import-input.py')))
OwnedLisp, string = api['OwnedLisp'], api['lisp_string']
standby = api['standby']
LISP = standby['api']['LISP']
CUTS = ('after-import-control-open', 'before-import-control-unlink',
        'after-import-control-unlink', 'before-import-control-directory-sync',
        'after-import-control-directory-sync', 'before-import-control-final-proof',
        'after-import-control-close', 'after-import-control-final-proof')
FAULTS = ('source', 'restartpoint', 'pending', 'evidence', 'child', 'parent', 'temp',
          'extra', 'caught', 'throw-before', 'throw-after', 'post-unlink-source',
          'read', 'open', 'unlink', 'fsync', 'absent-fsync', 'close', 'close-interrupt',
          'store-close', 'outer-root-close', 'final-check', 'global-swap', 'root-swap',
          'late-source', 'late-restartpoint', 'late-pending', 'late-evidence',
          'late-child', 'late-parent', 'late-temp', 'late-extra')


def image(peer):
    result = {}
    for path in peer.data.rglob('*'):
        relative = str(path.relative_to(peer.data))
        if relative in ('postmaster.pid', 'global/pg_wal_control.tmp'):
            continue
        stat = path.lstat()
        result[relative] = (stat.st_dev, stat.st_ino, stat.st_mode, stat.st_nlink,
                            stat.st_uid, path.read_bytes() if path.is_file() else None)
    return result


def probe(peer, mode, label=None):
    killed = isinstance(mode, tuple)
    label = label or ('kill-' + mode[1] if killed else mode)
    form_mode = f"'(:kill :{mode[1]})" if killed else ':' + mode
    before = image(peer)
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {string(LISP)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             '(setf *print-circle* t *print-length* nil *print-level* nil)']
    for name in ('logical-slot-copy-import-input-native.lisp',
                 'logical-slot-copy-import-source-native.lisp',
                 'logical-slot-copy-import-control-native.lisp'):
        forms.append('(handler-bind ((warning (lambda (c) (error "control fixture warning: ~a" c)))) '
                     f'(load {string(Path(__file__).with_name(name))}))')
    forms.append(f'''(let ((*pgdata* (directory-pathname {string(peer.data)})))
      (unwind-protect
       (postmaster-own-data-directory "127.0.0.1" 6543 :startup-reconcile
         (lambda () (logical-import-control-test-run {form_mode})))
       (when *postmaster-pid-stream* (postmaster-remove-pid-file))
       (when *postmaster-data-lock* (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil))))''')
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for form in forms:
        command.extend(['--eval', form])
    log = peer.root / f'control-temp-{label}.log'
    start = time.monotonic()
    with log.open('w') as out:
        result = subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, timeout=120)
    if killed:
        assert result.returncode == -9, (mode, result.returncode, log)
        assert f'CONTROL-TEMP-KILL :{mode[1].upper()}' in log.read_text(), log
        present = mode[1] in ('after-import-control-open', 'before-import-control-unlink')
        assert os.path.lexists(peer.data / 'global/pg_wal_control.tmp') == present
        assert before == image(peer), (mode, 'kill changed canonical files')
        print(f'CONTROL-TEMP-KILLED {mode[1]} seconds={time.monotonic() - start:.6f}', flush=True)
        probe(peer, 'full' if present else 'absent', label='restart-' + mode[1])
        return
    assert result.returncode == 0, (mode, result.returncode, log)
    assert f'CONTROL-TEMP-RESULT :{mode.upper()} ' in log.read_text(), log
    assert before == image(peer), (mode, 'changed canonical files')
    assert not (peer.data / 'postmaster.pid').exists()
    print(f'CONTROL-TEMP-CASE {label} PASS seconds={time.monotonic() - start:.6f}', flush=True)


def matrix(peer, expected):
    temporary = peer.data / 'global/pg_wal_control.tmp'
    witness = peer.root / 'control-witness'

    def reset(size=len(expected)):
        if temporary.is_dir() and not temporary.is_symlink():
            temporary.rmdir()
        else:
            temporary.unlink(missing_ok=True)
        witness.unlink(missing_ok=True)
        if size is not None:
            temporary.write_bytes(expected[:size])
            temporary.chmod(0o600)

    for mode, size in [('full', len(expected)), ('absent', None), ('empty', 0),
                       ('partial', len(expected) // 2)]:
        reset(size)
        probe(peer, mode)
        assert not os.path.lexists(temporary)
        probe(peer, 'absent', label=mode + '-idempotent')
    for mode in ('foreign', 'oversize', 'symlink', 'hardlink', 'fifo', 'directory',
                 'mode', 'foreign-global', 'catalog-temp', 'journal', 'child-control', 'orphan'):
        reset()
        extra = None
        canonical = peer.data / 'global/pg_wal_control'
        parent = canonical.read_bytes()
        if mode == 'foreign':
            temporary.write_bytes(bytes([expected[0] ^ 1]) + expected[1:])
        elif mode == 'oversize':
            temporary.write_bytes(expected + b'x')
        elif mode in ('symlink', 'hardlink'):
            temporary.unlink()
            witness.write_bytes(expected)
            witness.chmod(0o600)
            if mode == 'symlink':
                temporary.symlink_to(witness)
            else:
                os.link(witness, temporary)
        elif mode == 'fifo':
            temporary.unlink()
            os.mkfifo(temporary, 0o600)
        elif mode == 'directory':
            temporary.unlink()
            temporary.mkdir(mode=0o700)
        elif mode == 'mode':
            temporary.chmod(0o644)
        elif mode in ('foreign-global', 'catalog-temp', 'journal'):
            extra = peer.data / ('logical-slot-copy.install' if mode == 'journal' else
                                 'global/pg_catalog.tmp' if mode == 'catalog-temp' else 'global/foreign')
            extra.write_bytes(b'untouched')
            extra.chmod(0o600)
        elif mode == 'child-control':
            canonical.write_bytes(expected)
        elif mode == 'orphan':
            extra = peer.data / 'logical-slot-copy.work' / 'opaque'
            assert not extra.parent.exists()
            extra.mkdir(mode=0o700, parents=True)
            extra.parent.chmod(0o700)
            (extra / 'keep').write_bytes(b'untouched')
        candidate = temporary.read_bytes() if temporary.is_file() and not temporary.is_symlink() else None
        try:
            probe(peer, mode)
            if mode == 'orphan':
                assert not os.path.lexists(temporary)
                assert (extra / 'keep').read_bytes() == b'untouched'
            else:
                assert os.path.lexists(temporary), mode
                if candidate is not None:
                    assert temporary.read_bytes() == candidate, mode
            if witness.exists():
                assert witness.read_bytes() == expected
        finally:
            if mode == 'child-control':
                canonical.write_bytes(parent)
            if extra is not None:
                if extra.is_dir():
                    shutil.rmtree(extra.parent)
                else:
                    extra.unlink()
    for mode in FAULTS:
        reset(None if mode == 'absent-fsync' else len(expected))
        probe(peer, mode)
    for point in CUTS:
        reset()
        probe(peer, ('kill', point))


def run(optional=False):
    source = OwnedLisp(parameters=(('wal_level', 'logical'),))
    child = OwnedLisp(parameters=(('wal_level', 'replica'),))
    try:
        source.start()
        connection = standby['RawConnection'](source.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            for name in ('alpha', 'beta'):
                connection.query(f"SELECT * FROM pg_create_logical_replication_slot('{name}', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (1, 'healthy base')")
        finally:
            connection.close()
        child.data.mkdir(mode=0o700)
        source.control(f'(logical-slot-base-copy {string(child.data)} "healthy_import_pin")', timeout=90)
        child.start()
        standby['wait_rows'](child, [['1']])
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "healthy_import_pin")')
        standby['insert'](source, 2)
        standby['wait_rows'](child, [['1'], ['2']])
        child.control('(physical-follow-stop)')
        child.control('(physical-recovery-restartpoint)')
        api['probe'](child, pending=True, disposition='eligible')
        parent = (child.data / 'global/pg_wal_control').read_bytes()
        child.control(f'(load {string(Path(__file__).with_name("logical-slot-copy-import-commit-native.lisp"))})')
        child.control(f'(setf *logical-import-commit-test-omit-control-fact* {"t" if optional else "nil"} *logical-import-commit-test-cut* :before-child-control-rename)')
        child.control('(logical-import-commit-test-start)')
        assert child.process.wait(timeout=90) == -9
        assert 'LOGICAL-IMPORT-CHILD-SIGKILL :BEFORE-CHILD-CONTROL-RENAME' in (child.root / 'server.log').read_text()
        assert (child.data / 'global/pg_wal_control').read_bytes() == parent
        source.stop(immediate=True)
        temporary = child.data / 'global/pg_wal_control.tmp'
        expected = temporary.read_bytes()
        assert expected and expected != parent
        print('CONTROL-TEMP-GLOBAL-NAMES', sorted(p.name for p in temporary.parent.iterdir()), flush=True)
        matrix(child, expected)
        print(f'CONTROL-TEMP-WORKFLOW-PASS optional={optional}', flush=True)
    finally:
        child.stop(immediate=True)
        source.stop(immediate=True)


if __name__ == '__main__':
    run(False)
    run(True)

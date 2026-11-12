"""Publish base verification receipts under genuine cold postmaster ownership."""
from pathlib import Path
import os
import runpy
import shutil
import subprocess
import time

copy = runpy.run_path(str(Path(__file__).with_name('logical-slot-base-copy.py')))
api = copy['api']
RawConnection = copy['RawConnection']
lisp_string = copy['lisp_string']


def cold_verify(server, target, scratch, label, point=None, refusal=None, touch=False):
    injection = (f'(when (eq point {point}) (setf fired t) (error "base proof test cut"))'
                 if point else '(declare (ignore point))')
    if touch:
        injection = f'''(when (eq point :before-base-proof-publication)
          (setf fired t)
          (with-open-file (stream {lisp_string(target / 'PG_VERSION')}
                                  :direction :io :element-type '(unsigned-byte 8) :if-exists :overwrite)
            (let ((byte (read-byte stream)))
              (file-position stream 0) (write-byte byte stream) (fsync-stream stream))))'''
    checks = (f'(assert (and caught (search {lisp_string(refusal)} (princ-to-string caught))))'
              if refusal else '(when caught (error caught)) (assert succeeded)')
    form = f'''(let ((*pgdata* (directory-pathname {lisp_string(target)}))
                     (caught nil) (succeeded nil) (fired nil))
      (declare (ignorable fired))
      (let ((*logical-slot-copy-verification-test-hook*
              (lambda (point context) (declare (ignore context)) {injection})))
        (unwind-protect
             (handler-case
                 (postmaster-own-data-directory "127.0.0.1" 6543
                   :startup-reconcile
                   (lambda ()
                     (logical-slot-copy-record-base-verification {lisp_string(scratch)})
                     (setf succeeded t)))
               (error (condition) (setf caught condition)))
          (when *postmaster-pid-stream* (postmaster-remove-pid-file))
          (when *postmaster-data-lock* (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil))))
      {checks}
      {'(assert fired)' if point or touch else ''}
      (assert (and (not *wal-ready*) (not *catalog*) (not *physical-receive-store*)
                   (zerop (hash-table-count *wal-replication-slots*))))
      (format t "~&BASE-PROOF-COLD-PASS {label}~%"))'''
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for code in ['(require :asdf)',
                 f'(push (uiop:ensure-directory-pathname {lisp_string(api["LISP"])}) asdf:*central-registry*)',
                 '(asdf:load-system "pg")', '(in-package :pg)', form]:
        command.extend(['--eval', code])
    log = server.root / f'cold-{label}.log'
    with log.open('w') as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=90)
    assert result.returncode == 0 and f'BASE-PROOF-COLD-PASS {label}' in log.read_text(), log
    assert not (target / 'postmaster.pid').exists()
    assert not (target / 'pg_replslot').exists()
    assert sorted(p.name for p in scratch.iterdir()) == ['.pg_lisp.lock']


def run():
    start = time.monotonic()
    server = api['OwnedLisp'](parameters=(('wal_level', 'logical'),))
    try:
        server.start()
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            connection.query("SELECT * FROM pg_create_logical_replication_slot('alpha', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (1, 'pending source state')")
        finally:
            connection.close()
        original = server.root / 'original'
        original.mkdir(mode=0o700)
        server.control(f'(logical-slot-base-copy {lisp_string(original)} "base_proof_pin")', timeout=90)
        source_slot = (server.data / 'pg_replslot/alpha/state').read_bytes()

        def fixture(name):
            target = server.root / name
            shutil.copytree(original, target)
            scratch = server.root / f'{name}-scratch'
            scratch.mkdir(mode=0o700)
            return target, scratch

        target, scratch = fixture('success')
        before = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file()}
        cold_verify(server, target, scratch, 'initial')
        proof = target / 'logical-slot-copy.base-verified'
        image = proof.read_bytes()
        assert image.startswith(b'PGLOGICALBASE1 ')
        cold_verify(server, target, scratch, 'idempotent')
        assert image == proof.read_bytes()
        assert before == {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*')
                          if p.is_file() and p != proof}

        graft, work = fixture('grafted')
        (graft / proof.name).write_bytes(image)
        (graft / proof.name).chmod(0o600)
        cold_verify(server, graft, work, 'grafted', refusal='differs from verified input')
        assert image == (graft / proof.name).read_bytes()

        for label, point, filename in [
                ('file-cut', ':after-base-proof-file-fsync', 'logical-slot-copy.base-verified.tmp'),
                ('rename-cut', ':after-base-proof-rename', 'logical-slot-copy.base-verified'),
                ('root-cut', ':before-base-proof-root-fsync', 'logical-slot-copy.base-verified'),
                ('readback-cut', ':after-base-proof-root-fsync', 'logical-slot-copy.base-verified')]:
            cut, work = fixture(label)
            cold_verify(server, cut, work, label, point=point, refusal='base proof test cut')
            saved = (cut / filename).read_bytes()
            if filename.endswith('.tmp'):
                cold_verify(server, cut, work, label + '-fenced', refusal='root inventory')
                assert saved == (cut / filename).read_bytes()
                (cut / filename).unlink()  # Remove only this fixture's exact known fault output.
            cold_verify(server, cut, work, label + '-retry')

        changed, work = fixture('changed')
        cold_verify(server, changed, work, 'changed', refusal='input changed after verification', touch=True)
        assert not (changed / proof.name).exists()
        assert not (changed / (proof.name + '.tmp')).exists()
        assert source_slot == (server.data / 'pg_replslot/alpha/state').read_bytes()
        print(f'logical-slot-copy-base-proof: PASS {time.monotonic() - start:.6f}s; '
              f'artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


if __name__ == '__main__':
    run()

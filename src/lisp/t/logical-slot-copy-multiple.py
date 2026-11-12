"""Verify overlapping restart ranges from two real logical slots."""
from pathlib import Path
import runpy
import time

copy = runpy.run_path(str(Path(__file__).with_name('logical-slot-base-copy.py')))
api = copy['api']
RawConnection = copy['RawConnection']
lisp_string = copy['lisp_string']


def run():
    start = time.monotonic()
    server = api['OwnedLisp'](parameters=(('wal_level', 'logical'),))
    try:
        server.start()
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            connection.query("SELECT * FROM pg_create_logical_replication_slot('alpha', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (1, 'before second slot')")
            connection.query("SELECT * FROM pg_create_logical_replication_slot('beta', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (2, 'after second slot')")
            copy['fill_past_rotation'](connection)
        finally:
            connection.close()
        target = server.root / 'copy'
        scratch = server.root / 'scratch'
        target.mkdir(mode=0o700)
        scratch.mkdir(mode=0o700)
        expected = [(name, (server.data / f'pg_replslot/{name}/state').read_bytes())
                    for name in ['alpha', 'beta']]
        server.control(f'(logical-slot-base-copy {lisp_string(target)} "multi_pin")', timeout=90)
        descriptor = copy['decode_descriptor'](target / 'logical-slot-copy.descriptor')
        assert descriptor[3] == expected
        assert len(descriptor[4]) >= 2
        server.control(f'''(let ((expected (make-hash-table :test #'equal)) (seen nil) (restarts nil) (scanned 0))
          (sb-thread:with-mutex (*wal-lock*)
            (dolist (name '("alpha" "beta"))
              (let ((table (make-hash-table :test #'eql)))
                (maphash (lambda (xid value) (setf (gethash xid table) (copy-tree value)))
                         (wal-replication-slot-pending-xids (gethash name *wal-replication-slots*)))
                (setf (gethash name expected) table))))
          (let ((*logical-slot-copy-verification-test-hook*
                  (lambda (point context)
                    (when (eq point :after-record) (incf scanned))
                    (when (eq point :slot-verified)
                      (let* ((slot (first context)) (progress (second context))
                             (name (wal-replication-slot-name slot)))
                        (assert (eq :reserved (wal-replication-slot-wal-status slot)))
                        (assert (plusp scanned))
                        (push (cons name (wal-replication-slot-restart-lsn slot)) restarts)
                        (setf scanned 0)
                        (assert (equalp (gethash name expected)
                                        (logical-slot-startup-progress-pending progress)))
                        (push name seen))))))
            (let ((summary (logical-slot-copy-verify {lisp_string(target)} {lisp_string(scratch)})))
              (assert (= 2 (getf summary :slots)))
              (assert (= (getf summary :pending-xids)
                         (loop for table being the hash-values of expected sum (hash-table-count table))))))
          (assert (equal '("alpha" "beta") (sort seen #'string<)))
          ;; Both positive scans reach the same checkpoint, so these ranges overlap.
          (assert (< (cdr (assoc "alpha" restarts :test #'equal))
                     (cdr (assoc "beta" restarts :test #'equal)))) (values))''', timeout=90)
        assert expected == [(name, (server.data / f'pg_replslot/{name}/state').read_bytes())
                            for name in ['alpha', 'beta']]
        assert not (target / 'pg_replslot').exists()
        assert sorted(p.name for p in scratch.iterdir()) == ['.pg_lisp.lock']
        connection = RawConnection(server.port)
        try:
            assert connection.query('SELECT id FROM copy_rows WHERE id < 3 ORDER BY id') == [['1'], ['2']]
        finally:
            connection.close()
        print(f'logical-slot-copy-multiple: PASS {time.monotonic() - start:.6f}s; '
              f'2 slots, {len(descriptor[4])} pieces; artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


if __name__ == '__main__':
    run()

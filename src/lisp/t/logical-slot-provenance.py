"""Check SQL and wire slot identity across primary and promoted-server crashes.

The promoted case creates fresh slots after a checkpoint on the child timeline,
with the source's logical ON origin retained through promotion.
"""
from pathlib import Path
import re
import runpy
import signal
import time


if not __debug__:
    raise RuntimeError('This assertion-based oracle requires Python without optimization.')

api = runpy.run_path(str(Path(__file__).with_name('command-complete.py')))
from parking_wire import RawConnection


def slot_image(server, name, timeline=1, on_pair=None):
    raw = (server.data / 'pg_replslot' / name / 'state').read_bytes()
    assert raw.endswith(b'\n') and raw.count(b'\n') == 1, raw
    fields = raw.decode('ascii').split()
    assert len(fields) == 17 and fields[0] == 'PGSLOT6', fields
    assert fields[1] == name and fields[6] == 'pgoutput', fields
    assert len(fields[9]) == 32 and all(c in '0123456789abcdef' for c in fields[9]), fields
    assert fields[10] == fields[13] == fields[14] == str(timeline), fields
    if on_pair is None:
        assert int(fields[11]) > 0 and fields[12] == '1', fields
    else:
        assert tuple(fields[11:13]) == on_pair, (fields, on_pair)
    assert fields[15] == 'none', fields
    return raw, fields


def lsn_number(value):
    high, low = value.split('/')
    return int(high, 16) << 32 | int(low, 16)


def check_native_cursor(server, name, timeline):
    """Read the slot's current durable interval and check pin and optional root lease lifetimes.

    Archive payload substitution is covered by structural tests separately.
    """
    response = server.control(f'''(progn
      (call-with-logical-replication-slot "{name}"
        (lambda (slot)
          (let ((confirmed (wal-replication-slot-confirmed-flush-lsn slot)) (count 0)
                (before-pins (sb-thread:with-mutex (*wal-lock*)
                               (loop for key being the hash-keys of *wal-snapshot-retention-pins* collect key)))
                (before-roots (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                                (loop for key being the hash-keys of *physical-retained-root-references* collect key)))
                (scan-pin nil) (archive-reader nil))
            (call-with-logical-slot-wal-cursor "{name}" nil nil
              (wal-replication-slot-confirmed-timeline slot) :prefix-end
              (lambda (cursor)
                (sb-thread:with-mutex (*wal-lock*)
                  (let ((pins (set-difference
                               (loop for key being the hash-keys of *wal-snapshot-retention-pins* collect key)
                               before-pins :test #'eq))
                        (archive (logical-wal-cursor-archive cursor)))
                    (assert (= 1 (length pins)))
                    (setf scan-pin (first pins))
                    (assert (eql confirmed (gethash scan-pin *wal-snapshot-retention-pins*)))
                    (assert (eql (null archive) (= {timeline} 1)))
                    (when archive
                      (logical-wal-archive-check-cursor
                       archive (logical-wal-cursor-history cursor) (logical-wal-cursor-namespace cursor))
                      (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                        (setf archive-reader
                              (loop for key being the hash-keys of *physical-retained-root-references*
                                      using (hash-value reference)
                                    when (and (not (member key before-roots :test #'eq))
                                              (eql (physical-retained-root-reference-root-id reference)
                                                   (getf (logical-wal-archive-reference archive) :root-id)))
                                      return key))
                        (assert archive-reader)))))
                (loop
                  (multiple-value-bind (record bytes owner carrier kind) (logical-wal-cursor-next cursor)
                    (declare (ignore bytes))
                    (unless record (return))
                    (assert (= owner carrier {timeline}))
                    (assert (eq kind :ordinary))
                    (incf count)))
                (assert (= (logical-wal-cursor-position cursor) (logical-wal-cursor-end cursor)))
                (assert (eql (zerop count) (= confirmed (logical-wal-cursor-end cursor))))))
            (sb-thread:with-mutex (*wal-lock*)
              (assert (not (nth-value 1 (gethash scan-pin *wal-snapshot-retention-pins*)))))
            (when archive-reader
              (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                (assert (not (nth-value 1 (gethash archive-reader *physical-retained-root-references*))))))
            (assert (eql confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
            (format t "CURSOR-CHECKED=~d~%" count))))
      (values))''')
    counts = re.findall(r'CURSOR-CHECKED=(\d+)', response)
    assert len(counts) == 1, response
    print(f'logical-slot-cursor: {name} timeline={timeline} records={counts[0]}', flush=True)


def check_native_reader(server, name):
    """Exercise the real reader and an aligned continuation without acknowledging the slot."""
    response = server.control(f'''(progn
      (call-with-logical-replication-slot "{name}"
        (lambda (slot)
          (let ((confirmed (wal-replication-slot-confirmed-flush-lsn slot))
                (original (symbol-function 'call-with-logical-slot-wal-cursor-from-scan))
                (decoder (symbol-function 'wal-decode-record))
                (before-roots (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                                (loop for key being the hash-keys of *physical-retained-root-references* collect key)))
                (pins nil) (roots nil) (decoded 0) (calls 0))
            (unwind-protect
                (progn
                  (setf (symbol-function 'call-with-logical-slot-wal-cursor-from-scan)
                        (lambda (&rest arguments)
                          (let* ((scan (first arguments)) (callback (fourth arguments)))
                            (push (wal-retained-slot-scan-pin scan) pins)
                            (sb-thread:with-mutex (*wal-lock*)
                              (assert (eql (wal-retained-slot-scan-start scan)
                                           (gethash (first pins) *wal-snapshot-retention-pins*))))
                            (setf (fourth arguments)
                                  (lambda (cursor source)
                                    (when (logical-wal-cursor-archive cursor)
                                      (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                                        (let ((keys (set-difference
                                                     (loop for key being the hash-keys of *physical-retained-root-references*
                                                           collect key)
                                                     before-roots :test #'eq)))
                                          (assert (= 1 (length keys)))
                                          (push (first keys) roots))))
                                    (funcall callback cursor source))))
                          (multiple-value-prog1 (apply original arguments) (incf calls))))
                  (setf (symbol-function 'wal-decode-record)
                        (lambda (&rest arguments) (incf decoded) (apply decoder arguments)))
                  (call-with-logical-reader-spools
                    (lambda ()
                      (multiple-value-bind (transactions progress exhausted)
                          (wal-logical-slot-read-transactions "{name}")
                        (assert exhausted)
                        (assert (<= confirmed progress *wal-durable-lsn*))
                        (assert (eql progress *wal-logical-slot-read-progress*))
                        (assert (every (lambda (tx) (> (logical-transaction-commit-lsn tx) confirmed))
                                       transactions))
                        (when (> progress confirmed)
                          (let ((reuse (symbol-function 'logical-wal-reusable-continuation)) (checks 0))
                            (unwind-protect
                                (progn
                                  (setf (symbol-function 'logical-wal-reusable-continuation)
                                        (lambda (&rest args)
                                          (incf checks)
                                          (let ((proof (apply reuse args)))
                                            (if (= (second args) progress) (assert proof) (assert (null proof)))
                                            proof)))
                                  (sb-thread:with-mutex (*wal-lock*)
                                    (assert (wal-slot-terminal-ack-p slot progress)))
                                  (assert (= checks 1))
                                  (sb-thread:with-mutex (*wal-lock*)
                                    (assert (not (wal-slot-terminal-ack-p slot (1- progress)))))
                                  (assert (= checks 2)))
                              (setf (symbol-function 'logical-wal-reusable-continuation) reuse))))
                        (when (> progress confirmed)
                          (dolist (kind '(:terminate :cancel :timeout))
                           (let* ((source-check (symbol-function 'logical-wal-check-source))
                                 (persist (symbol-function 'wal-persist-replication-slot-ack-under-lock))
                                 (scanner (symbol-function 'wal-scan))
                                 (terminated (make-backend :termination-pending (eq kind :terminate)
                                                          :cancel-pending t
                                                          :statement-deadline (when (eq kind :timeout) 0)))
                                 (*backend* terminated)
                                 (injected nil) (fallbacks 0) (saves 0) (caught nil)
                                 (proof *wal-logical-slot-read-continuation*)
                                 (old-progress *wal-logical-slot-read-progress*))
                            (unwind-protect
                                (progn
                                  (setf (symbol-function 'logical-wal-check-source)
                                        (lambda (&rest args)
                                          (declare (ignore args))
                                          (setf injected t)
                                          (if (eq kind :terminate)
                                              (deliver-backend-termination terminated)
                                              (check-for-interrupts))
                                          (error "interrupt injection did not fire"))
                                        (symbol-function 'wal-persist-replication-slot-ack-under-lock)
                                        (lambda (&rest args)
                                          (declare (ignore args))
                                          (incf saves) (error "terminated ACK reached persistence"))
                                        (symbol-function 'wal-scan)
                                        (lambda (&rest args) (incf fallbacks) (apply scanner args)))
                                  (setf caught
                                        (handler-case
                                            (wal-acknowledge-replication-slot "{name}" progress *wal-logical-slot-owner*)
                                          (error (condition) condition)))
                                  (assert injected)
                                    (assert (and (typep caught 'pg-error)
                                                 (or (not (eq kind :terminate)) (fatal-error-p caught))
                                                 (string= (if (eq kind :terminate) "57P01" "57014")
                                                          (pg-error-sqlstate caught)))
                                            () "ACK lost interruption: ~a; fallback scans ~d; saves ~d"
                                          caught fallbacks saves)
                                  (assert (not (backend-termination-pending terminated)))
                                  (assert (not (backend-cancel-pending terminated)))
                                    (when (eq kind :timeout) (assert (null (backend-statement-deadline terminated))))
                                  (assert (zerop fallbacks)) (assert (zerop saves))
                                  (assert (= confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
                                  (assert (eq proof *wal-logical-slot-read-continuation*))
                                  (assert (eql old-progress *wal-logical-slot-read-progress*)))
                              (setf (symbol-function 'logical-wal-check-source) source-check
                                    (symbol-function 'wal-persist-replication-slot-ack-under-lock) persist
                                      (symbol-function 'wal-scan) scanner)))))
                        (let ((before decoded) (proof *wal-logical-slot-read-continuation*))
                          (when (> progress confirmed) (assert proof))
                          (multiple-value-bind (more continued done)
                              (wal-logical-slot-read-transactions "{name}" :from-lsn progress)
                            (assert (null more))
                            (assert (eql continued progress))
                            (assert done)
                            (assert (= before decoded))
                            (assert (eq proof *wal-logical-slot-read-continuation*)))))))
                  (assert (= calls 2))
                  (assert (= 2 (length (remove-duplicates pins :test #'eq))))
                  (sb-thread:with-mutex (*wal-lock*)
                    (dolist (pin pins) (assert (not (nth-value 1 (gethash pin *wal-snapshot-retention-pins*))))))
                  (when roots
                    (assert (= 2 (length (remove-duplicates roots :test #'eq))))
                    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                      (dolist (key roots) (assert (not (nth-value 1 (gethash key *physical-retained-root-references*)))))))
                  (assert (eql confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
                  (format t "READER-CHECKED=~d~%" calls))
              (setf (symbol-function 'call-with-logical-slot-wal-cursor-from-scan) original
                    (symbol-function 'wal-decode-record) decoder)))))
      (values))''')
    assert re.findall(r'READER-CHECKED=(\d+)', response) == ['2'], response


def check_unowned_terminal_advance(server, name, archive_expected=False):
    """Advance without inventing an owner; keep proof resources live through save."""
    check_terminal_owner_race(server, name)
    check_terminal_interruption_cleanup(server, name, archive_expected)
    response = server.control(f'''(progn
      (let* ((slot (sb-thread:with-mutex (*wal-lock*)
                     (wal-find-usable-replication-slot-under-lock "{name}")))
             (target (sb-thread:with-mutex (*wal-lock*) *wal-durable-lsn*))
             (archive-call (symbol-function 'call-with-optional-logical-wal-archive))
             (persist (symbol-function 'wal-persist-replication-slot-ack-under-lock))
             (scanner (symbol-function 'wal-scan))
             (before-pins (sb-thread:with-mutex (*wal-lock*)
                            (loop for key being the hash-keys of *wal-snapshot-retention-pins* collect key)))
             (before-roots (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                             (loop for key being the hash-keys of *physical-retained-root-references* collect key)))
             (pins nil) (roots nil) (archives 0) (saves 0))
        (assert (null *wal-logical-slot-owner*))
        (assert (null (wal-replication-slot-active-owner slot)))
        (assert (> target (wal-replication-slot-confirmed-flush-lsn slot)))
        (unwind-protect
            (progn
              (setf (symbol-function 'wal-scan)
                    (lambda (&rest args) (declare (ignore args))
                      (error "unowned terminal proof fell back to locked local scan"))
                    (symbol-function 'call-with-optional-logical-wal-archive)
                    (lambda (&rest args)
                      (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                      (incf archives) (apply archive-call args))
                    (symbol-function 'wal-persist-replication-slot-ack-under-lock)
                    (lambda (&rest args)
                      (assert (sb-thread:holding-mutex-p *wal-lock*))
                      (assert (null *wal-logical-slot-owner*))
                      (assert (null (wal-replication-slot-active-owner slot)))
                      (setf pins (set-difference
                                  (loop for key being the hash-keys of *wal-snapshot-retention-pins* collect key)
                                  before-pins :test #'eq))
                      (assert (= 1 (length pins)))
                      (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                        (setf roots (set-difference
                                     (loop for key being the hash-keys of *physical-retained-root-references* collect key)
                                     before-roots :test #'eq)))
                      (assert (= {1 if archive_expected else 0} (length roots)))
                      (multiple-value-prog1 (apply persist args) (incf saves))))
              (assert (eq slot (wal-acknowledge-replication-slot "{name}" target)))
              (assert (= 1 archives saves))
              (assert (= target (wal-replication-slot-confirmed-flush-lsn slot)))
              (sb-thread:with-mutex (*wal-lock*)
                (dolist (pin pins) (assert (not (nth-value 1 (gethash pin *wal-snapshot-retention-pins*))))))
              (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                (dolist (key roots) (assert (not (nth-value 1 (gethash key *physical-retained-root-references*))))))
              (assert (null (wal-replication-slot-active-owner slot)))
              (format t "UNOWNED-ADVANCED=~d~%" target))
          (setf (symbol-function 'wal-scan) scanner
                (symbol-function 'call-with-optional-logical-wal-archive) archive-call
                (symbol-function 'wal-persist-replication-slot-ack-under-lock) persist)))
      (values))''')
    assert len(re.findall(r'UNOWNED-ADVANCED=\d+', response)) == 1, response


def check_terminal_ack_overtake(server, name):
    """A newer durable ACK wins while an older proof is outside the WAL lock."""
    response = server.control(f'''(progn
      (let* ((classifier (symbol-function 'logical-wal-cursor-terminal-p))
             (persist (symbol-function 'wal-persist-replication-slot-ack-under-lock))
             (ready (sb-thread:make-semaphore)) (worker nil) (worker-error nil)
             (target nil) (end (sb-thread:with-mutex (*wal-lock*) *wal-durable-lsn*))
             (triggered nil) (caught nil) (saves 0)
             (before-pins (sb-thread:with-mutex (*wal-lock*)
                            (hash-table-count *wal-snapshot-retention-pins*))))
        (call-with-logical-replication-slot "{name}"
          (lambda (slot)
            (let ((confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
              (call-with-logical-slot-wal-cursor "{name}" nil end
                (wal-replication-slot-confirmed-timeline slot) :prefix-end
                (lambda (cursor)
                  (loop for record = (logical-wal-cursor-next cursor) while record
                        when (and (> (wal-record-end record) confirmed)
                                  (wal-terminal-record-type-p (wal-record-type record)))
                          do (setf target (wal-record-end record)) (return)))))))
        (assert (and target (< target end)))
        (unwind-protect
            (progn
              (setf (symbol-function 'wal-persist-replication-slot-ack-under-lock)
                    (lambda (&rest args)
                      (multiple-value-prog1 (apply persist args) (incf saves)))
                    (symbol-function 'logical-wal-cursor-terminal-p)
                    (lambda (&rest args)
                      (multiple-value-prog1 (apply classifier args)
                        (unless triggered
                          (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                          (setf triggered t
                                worker (sb-thread:make-thread
                                        (lambda ()
                                          (let ((*backend* nil))
                                            (unwind-protect
                                                 (handler-case
                                                     (wal-acknowledge-replication-slot "{name}" end)
                                                   (error (condition) (setf worker-error condition)))
                                              (sb-thread:signal-semaphore ready))))
                                        :name "terminal-proof-ack-overtake"))
                          (assert (sb-thread:wait-on-semaphore ready :timeout 10))
                          (assert (null worker-error))))))
              (handler-case (wal-acknowledge-replication-slot "{name}" target)
                (pg-error (condition) (setf caught condition)))
              (assert (and caught (string= "22023" (pg-error-sqlstate caught))
                           (search "backwards" (pg-error-message caught))))
              (assert (= 1 saves))
              (sb-thread:with-mutex (*wal-lock*)
                (let ((slot (wal-find-usable-replication-slot-under-lock "{name}")))
                  (assert (= end (wal-replication-slot-confirmed-flush-lsn slot)))
                  (assert (null (wal-replication-slot-active-owner slot))))
                (assert (= before-pins (hash-table-count *wal-snapshot-retention-pins*))))
              (format t "TERMINAL-ACK-OVERTAKE-REJECTED=~d~%" end))
          (when worker
            (assert (not (eq :timeout (sb-thread:join-thread worker :timeout 10 :default :timeout)))))
          (setf (symbol-function 'logical-wal-cursor-terminal-p) classifier
                (symbol-function 'wal-persist-replication-slot-ack-under-lock) persist)))
      (values))''')
    match = re.search(r'TERMINAL-ACK-OVERTAKE-REJECTED=(\d+)', response)
    assert match, response
    assert int(slot_image(server, name)[1][5]) == int(match[1])


def check_terminal_owner_race(server, name):
    """An actual reader acquiring the slot invalidates an unowned proof."""
    before = (server.data / 'pg_replslot' / name / 'state').read_bytes()
    response = server.control(f'''(progn
      (let* ((classifier (symbol-function 'logical-wal-cursor-terminal-p))
             (persist (symbol-function 'wal-persist-replication-slot-ack-under-lock))
             (ready (sb-thread:make-semaphore)) (release (sb-thread:make-semaphore))
             (worker nil) (worker-error nil) (caught nil) (saves 0)
             (before-pins (sb-thread:with-mutex (*wal-lock*)
                            (hash-table-count *wal-snapshot-retention-pins*)))
             (before-roots (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                             (hash-table-count *physical-retained-root-references*)))
             (target (sb-thread:with-mutex (*wal-lock*) *wal-durable-lsn*)))
        (unwind-protect
            (progn
              (setf (symbol-function 'wal-persist-replication-slot-ack-under-lock)
                    (lambda (&rest args) (declare (ignore args)) (incf saves)
                      (error "stale terminal proof attempted persistence"))
                    (symbol-function 'logical-wal-cursor-terminal-p)
                    (lambda (&rest args)
                      (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                      (multiple-value-prog1 (apply classifier args)
                        (setf worker
                              (sb-thread:make-thread
                               (lambda ()
                                 (let ((*backend* nil))
                                   (handler-case
                                       (call-with-logical-replication-slot "{name}"
                                         (lambda (slot)
                                           (declare (ignore slot))
                                           (sb-thread:signal-semaphore ready)
                                           (assert (sb-thread:wait-on-semaphore release :timeout 10))))
                                     (error (condition)
                                       (setf worker-error condition)
                                       (sb-thread:signal-semaphore ready)))))
                               :name "terminal-proof-owner-race"))
                        (assert (sb-thread:wait-on-semaphore ready :timeout 10))
                        (assert (null worker-error)))))
              (handler-case (wal-acknowledge-replication-slot "{name}" target)
                (error (condition) (setf caught condition)))
              (assert (and caught (search "changed" (princ-to-string caught))))
              (assert (zerop saves))
              (sb-thread:with-mutex (*wal-lock*)
                (assert (= before-pins (hash-table-count *wal-snapshot-retention-pins*))))
              (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                (assert (= before-roots (hash-table-count *physical-retained-root-references*))))
              (format t "TERMINAL-OWNER-RACE-REJECTED~%"))
          (setf (symbol-function 'logical-wal-cursor-terminal-p) classifier
                (symbol-function 'wal-persist-replication-slot-ack-under-lock) persist)
          (sb-thread:signal-semaphore release)
          (when worker
            (assert (not (eq :timeout (sb-thread:join-thread worker :timeout 10 :default :timeout)))))
          (assert (null worker-error))))
      (values))''')
    assert 'TERMINAL-OWNER-RACE-REJECTED' in response, response
    assert (server.data / 'pg_replslot' / name / 'state').read_bytes() == before


def check_terminal_interruption_cleanup(server, name, archive_expected):
    """Interrupt real source validation while the terminal proof owns its leases."""
    before = (server.data / 'pg_replslot' / name / 'state').read_bytes()
    response = server.control(f'''(progn
      (let* ((slot (sb-thread:with-mutex (*wal-lock*)
                     (wal-find-usable-replication-slot-under-lock "{name}")))
             (target (sb-thread:with-mutex (*wal-lock*) *wal-durable-lsn*))
             (image (wal-encode-replication-slot slot))
             (decoder (symbol-function 'wal-decode-control-line))
             (classifier (symbol-function 'logical-wal-cursor-terminal-p))
             (persist (symbol-function 'wal-persist-replication-slot-ack-under-lock))
             (scanner (symbol-function 'wal-scan))
             (before-pins (sb-thread:with-mutex (*wal-lock*)
                            (loop for key being the hash-keys of *wal-snapshot-retention-pins* collect key)))
             (before-roots (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                             (loop for key being the hash-keys of *physical-retained-root-references* collect key)))
             (armed nil) (queued 0) (delivered 0) (saves 0) (sentinel nil)
             (pins nil) (roots nil))
        (assert (> target (wal-replication-slot-confirmed-flush-lsn slot)))
        (unwind-protect
            (dolist (kind '(:termination :cancel :timeout))
              (let ((*backend* (make-backend)) (caught nil))
                (setf queued 0 delivered 0 sentinel nil pins nil roots nil)
                (setf (symbol-function 'wal-scan)
                      (lambda (&rest args) (declare (ignore args))
                        (error "interrupted terminal proof used legacy scan"))
                      (symbol-function 'wal-persist-replication-slot-ack-under-lock)
                      (lambda (&rest args) (declare (ignore args)) (incf saves)
                        (error "interrupted terminal proof attempted persistence"))
                      (symbol-function 'logical-wal-cursor-terminal-p)
                      (lambda (&rest args) (setf armed t) (apply classifier args))
                      (symbol-function 'wal-decode-control-line)
                      (lambda (line)
                        (when armed
                          (setf armed nil)
                          (assert (sb-thread:holding-mutex-p *wal-lock*))
                          (setf pins (set-difference
                                      (loop for key being the hash-keys of *wal-snapshot-retention-pins* collect key)
                                      before-pins :test #'eq))
                          (assert (= 1 (length pins)))
                          (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                            (setf roots (set-difference
                                         (loop for key being the hash-keys of *physical-retained-root-references* collect key)
                                         before-roots :test #'eq)))
                          (assert (= {1 if archive_expected else 0} (length roots)))
                          (incf queued)
                          (sb-thread:interrupt-thread sb-thread:*current-thread*
                            (lambda ()
                              (incf delivered)
                              (handler-bind ((pg-error (lambda (condition) (setf sentinel condition))))
                                (ecase kind
                                  (:termination (setf (backend-termination-pending *backend*) t
                                                      (backend-cancel-pending *backend*) t))
                                  (:cancel (setf (backend-cancel-pending *backend*) t))
                                  (:timeout (setf (backend-statement-deadline *backend*)
                                                 (1- (get-internal-real-time)))))
                                (check-for-interrupts)))))
                        (funcall decoder line)))
                (handler-case (wal-acknowledge-replication-slot "{name}" target)
                  (pg-error (condition) (setf caught condition)))
                (assert (and caught (eq caught sentinel)))
                (assert (string= (if (eq kind :termination) "57P01" "57014")
                                 (pg-error-sqlstate caught)))
                (assert (= 1 queued delivered))
                (assert (zerop saves))
                (assert (null (backend-termination-pending *backend*)))
                (assert (null (backend-cancel-pending *backend*)))
                (assert (null (backend-statement-deadline *backend*)))
                (assert (equalp image (wal-encode-replication-slot slot)))
                (assert (null (wal-replication-slot-active-owner slot)))
                (sb-thread:with-mutex (*wal-lock*)
                  (dolist (pin pins) (assert (not (nth-value 1 (gethash pin *wal-snapshot-retention-pins*))))))
                (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                  (dolist (key roots) (assert (not (nth-value 1 (gethash key *physical-retained-root-references*))))))
                (format t "TERMINAL-INTERRUPT-CLEANED=~a~%" kind)))
          (setf (symbol-function 'wal-decode-control-line) decoder
                (symbol-function 'logical-wal-cursor-terminal-p) classifier
                (symbol-function 'wal-persist-replication-slot-ack-under-lock) persist
                (symbol-function 'wal-scan) scanner)))
      (values))''')
    assert len(re.findall(r'TERMINAL-INTERRUPT-CLEANED=', response)) == 3, response
    assert (server.data / 'pg_replslot' / name / 'state').read_bytes() == before


def check_restored_slots(server, connection, before, timeline=1, on_pair=None):
    for name, (raw, fields) in before.items():
        assert slot_image(server, name, timeline, on_pair)[0] == raw, name
        rows = connection.query(
            'SELECT slot_type,active,restart_lsn,confirmed_flush_lsn '
            f"FROM pg_replication_slots WHERE slot_name='{name}'")
        assert len(rows) == 1 and rows[0][:2] == ['logical', 'f'], rows
        restart, confirmed = map(lsn_number, rows[0][2:])
        assert (restart, confirmed) == (int(fields[3]), int(fields[5])), (rows, fields)
        check_native_cursor(server, name, timeline)
        check_native_reader(server, name)
        assert slot_image(server, name, timeline, on_pair)[0] == raw, name


def crash(server):
    process = server.process
    server.stop(immediate=True)
    assert process.returncode == -signal.SIGKILL, process.returncode
    return process.pid


def append_unfinished_batch(server):
    """Sync complete records without a terminal, then let the caller SIGKILL."""
    response = server.control('''(progn
      (sb-thread:with-mutex (*wal-lock*)
        (wal-append-batch-start)
        (let* ((images (list (make-wal-page-image
                              :relfilenode 700 :block 0 :page (page-init (make-page))
                              :xids '(999) :provenance :row)))
               (end (+ *wal-end-lsn* +wal-header-size+ (wal-page-payload-length images))))
          (wal-append-record +wal-record-pages+ 0 (wal-encode-page-images images end)))
        (wal-flush-core)
        (format t "UNFINISHED-END=~d~%" *wal-durable-lsn*))
      (values))''')
    ends = re.findall(r'UNFINISHED-END=(\d+)', response)
    assert len(ends) == 1, response
    return int(ends[0])


def check():
    started = time.monotonic()
    server = api['OwnedLisp']()
    try:
        server.start()
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE provenance_rows (id integer)')
            connection.query('INSERT INTO provenance_rows VALUES (1)')
            connection.query("SELECT pg_create_logical_replication_slot('sql_identity','pgoutput')")
            replication = RawConnection(server.port, replication='database')
            try:
                replication.query('CREATE_REPLICATION_SLOT wire_identity LOGICAL pgoutput NOEXPORT_SNAPSHOT')
            finally:
                replication.close()
            before = {name: slot_image(server, name) for name in ('sql_identity', 'wire_identity')}
            assert before['sql_identity'][1][8] == before['wire_identity'][1][8]
            assert before['sql_identity'][1][9] != before['wire_identity'][1][9]
            connection.query('INSERT INTO provenance_rows VALUES (2)')
        finally:
            connection.close()
        first_pid = crash(server)
        server.start()
        assert server.process.pid != first_pid
        connection = RawConnection(server.port)
        try:
            assert connection.query('SELECT id FROM provenance_rows ORDER BY id') == [['1'], ['2']]
            check_restored_slots(server, connection, before)
            connection.query('INSERT INTO provenance_rows VALUES (3)')
            assert connection.query('SELECT count(*) FROM provenance_rows') == [['3']]
        finally:
            connection.close()
        print(f'logical-slot-provenance: PASS (SQL and wire slots, native crash/restart) '
              f'{time.monotonic() - started:.6f}s; artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


def check_unfinished_tail():
    """Startup discards an open batch without losing completed slot progress."""
    started = time.monotonic()
    server = api['OwnedLisp']()
    try:
        server.start()
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE tail_rows (id integer)')
            connection.query("SELECT pg_create_logical_replication_slot('tail_identity','pgoutput')")
            connection.query('INSERT INTO tail_rows VALUES (1)')
        finally:
            connection.close()
        before = slot_image(server, 'tail_identity')[0]
        unfinished_end = append_unfinished_batch(server)
        first_pid = crash(server)
        server.start()
        assert server.process.pid != first_pid
        assert slot_image(server, 'tail_identity')[0] == before
        server.control(f'''(progn
          (sb-thread:with-mutex (*wal-lock*)
            (assert (>= *wal-durable-lsn* {unfinished_end}))
            (let ((pending (wal-replication-slot-pending-xids
                             (gethash "tail_identity" *wal-replication-slots*))))
              (assert (= 1 (hash-table-count pending)))
              (assert (not (gethash 999 pending)))))
          (values))''')
        connection = RawConnection(server.port)
        try:
            assert connection.query('SELECT id FROM tail_rows') == [['1']]
            connection.query('INSERT INTO tail_rows VALUES (2)')
            assert connection.query('SELECT id FROM tail_rows ORDER BY id') == [['1'], ['2']]
        finally:
            connection.close()
        print(f'logical-slot-provenance: PASS (unfinished durable batch, native crash/restart) '
              f'{time.monotonic() - started:.6f}s; artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


def logical_on_pair(server):
    response = server.control(
        '(sb-thread:with-mutex (*wal-lock*) '
        '(let ((fields (wal-current-logical-decoding-fields))) '
        '(assert (getf fields :logical-decoding-enabled)) '
        '(format t "PROVENANCE-ON=~a ~a~%" '
        '(or (getf fields :logical-on-start-lsn) "-") '
        '(or (getf fields :logical-on-start-timeline) "-"))))')
    matches = re.findall(r'PROVENANCE-ON=(\d+|-) (\d+|-)', response)
    assert len(matches) == 1, response
    return matches[0]


def check_promoted():
    started = time.monotonic()
    source, child = api['OwnedLisp'](), None
    print(f'logical-slot-provenance: promoted case source={source.root}', flush=True)
    try:
        source.data.mkdir(mode=0o700)
        (source.data / 'postgresql.conf').write_text('wal_level=logical\n')
        source.start()
        child = api['OwnedLisp']()
        assert child.port != source.port
        print(f'logical-slot-provenance: promoted case child={child.root}', flush=True)
        connection = RawConnection(source.port)
        try:
            assert connection.query('SHOW wal_level') == [['logical']]
            connection.query('CREATE TABLE provenance_rows (id integer)')
            connection.query('INSERT INTO provenance_rows VALUES (1)')
            assert connection.query('SELECT count(*) FROM pg_replication_slots') == [['0']]
        finally:
            connection.close()
        replication = RawConnection(source.port, replication='database')
        try:
            identity = replication.query('IDENTIFY_SYSTEM')[0]
            assert identity[1] == '1', identity
            system_id = identity[0]
        finally:
            replication.close()
        on_pair = logical_on_pair(source)
        assert on_pair[0].isdigit() and int(on_pair[0]) > 0 and on_pair[1] == '1', on_pair
        child.data.mkdir(mode=0o700)
        source.control(f'(physical-base-backup "{child.data}/" "provenance_promotion")', timeout=60)
        (child.data / 'postgresql.conf').write_text('wal_level=logical\n')
        child.start()
        child.control('(physical-recovery-enable-streaming)')
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "provenance_promotion")')
        connection = RawConnection(source.port)
        try:
            connection.query('INSERT INTO provenance_rows VALUES (2)')
            durable = connection.query('SELECT pg_current_wal_flush_lsn()')[0][0]
        finally:
            connection.close()
        connection = RawConnection(child.port)
        try:
            assert connection.query('SHOW wal_level') == [['logical']]
            assert connection.query('SELECT pg_is_in_recovery()') == [['t']]
            deadline = time.monotonic() + 30
            while connection.query(f"SELECT pg_last_wal_replay_lsn() >= '{durable}'::pg_lsn") != [['t']]:
                assert source.process.poll() is None and child.process.poll() is None
                assert time.monotonic() < deadline, 'child did not replay source commit'
                time.sleep(0.05)
            assert connection.query('SELECT id FROM provenance_rows ORDER BY id') == [['1'], ['2']]
        finally:
            connection.close()
        crash(source)
        connection = RawConnection(child.port)
        try:
            assert connection.query('SELECT pg_promote(true,30)') == [['t']]
            assert connection.query('SELECT pg_is_in_recovery()') == [['f']]
            assert connection.query("SELECT count(*) FROM pg_replication_slots WHERE slot_type='logical'") == [['0']]
            assert logical_on_pair(child) == on_pair
            history = (child.data / 'pg_wal/00000002.history').read_text().splitlines()
            parent, fork, _ = history[-1].split('\t', 2)
            assert parent == '1', history
            fork = lsn_number(fork)
            connection.query('INSERT INTO provenance_rows VALUES (3)')
            connection.query('CHECKPOINT')
            connection.query("SELECT pg_create_logical_replication_slot('sql_child_identity','pgoutput')")
            replication = RawConnection(child.port, replication='database')
            try:
                assert replication.query('IDENTIFY_SYSTEM')[0][:2] == [system_id, '2']
                replication.query('CREATE_REPLICATION_SLOT wire_child_identity LOGICAL pgoutput NOEXPORT_SNAPSHOT')
            finally:
                replication.close()
            before = {name: slot_image(child, name, 2, on_pair)
                      for name in ('sql_child_identity', 'wire_child_identity')}
            assert len({fields[9] for _, fields in before.values()}) == 2
            for _, fields in before.values():
                assert fields[8] == system_id, fields
                assert fork < int(fields[3]) <= int(fields[4]) == int(fields[5]), (fork, fields)
            check_restored_slots(child, connection, before, 2, on_pair)
            connection.query('INSERT INTO provenance_rows VALUES (4)')
            tail = lsn_number(connection.query('SELECT pg_current_wal_flush_lsn()')[0][0])
            assert all(int(fields[5]) < tail for _, fields in before.values()), (before, tail)
        finally:
            connection.close()
        first_pid = crash(child)
        child.start()
        assert child.process.pid != first_pid
        connection = RawConnection(child.port)
        try:
            assert connection.query('SELECT pg_is_in_recovery()') == [['f']]
            assert connection.query('SELECT id FROM provenance_rows ORDER BY id') == [['1'], ['2'], ['3'], ['4']]
            check_restored_slots(child, connection, before, 2, on_pair)
            replication = RawConnection(child.port, replication='database')
            try:
                assert replication.query('IDENTIFY_SYSTEM')[0][:2] == [system_id, '2']
            finally:
                replication.close()
            connection.query("SELECT pg_create_logical_replication_slot('admin_child','pgoutput')")
            connection.query('INSERT INTO provenance_rows VALUES (5)')
            assert connection.query('SELECT count(*) FROM provenance_rows') == [['5']]
            check_unowned_terminal_advance(child, 'admin_child', archive_expected=True)
        finally:
            connection.close()
        print(f'logical-slot-provenance: PASS (new child-tail SQL/wire slots, promotion, crash/restart) '
              f'{time.monotonic() - started:.6f}s; artifacts: {child.root}', flush=True)
    finally:
        if child is not None:
            child.stop(immediate=True)
        source.stop(immediate=True)


def check_delayed_feedback_reader():
    """Prove linear decoding and fence continuation reuse across source/owner changes."""
    started = time.monotonic()
    server = api['OwnedLisp']()
    count = 64
    try:
        server.start()
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE continuation_rows (id integer)')
            connection.query("SELECT pg_create_logical_replication_slot('delayed_reader','pgoutput')")
            connection.query("SELECT pg_create_logical_replication_slot('admin_advance','pgoutput')")
            connection.query("SELECT pg_create_logical_replication_slot('admin_overtake','pgoutput')")
            before = slot_image(server, 'delayed_reader')[0]
            for value in range(1, count + 1):
                connection.query(f'INSERT INTO continuation_rows VALUES ({value})')
            response = server.control(f'''(progn
              (let ((saved nil) (saved-progress nil))
                (call-with-logical-replication-slot "delayed_reader"
                  (lambda (slot)
                    (let* ((confirmed (wal-replication-slot-confirmed-flush-lsn slot))
                           (restart (wal-replication-slot-restart-lsn slot))
                           (end (sb-thread:with-mutex (*wal-lock*) *wal-durable-lsn*))
                           (reference 0) (decoded 0) (rows 0) (fast 0) (progress confirmed) (first-progress nil)
                           (decoder (symbol-function 'wal-decode-record))
                           (original (symbol-function 'call-with-logical-slot-wal-cursor-from-scan)))
                      (call-with-logical-slot-wal-cursor "delayed_reader" nil end
                        (wal-replication-slot-confirmed-timeline slot) :prefix-end
                        (lambda (cursor)
                          (loop while (logical-wal-cursor-next cursor) do (incf reference))))
                      (unwind-protect
                          (progn
                            (setf (symbol-function 'wal-decode-record)
                                  (lambda (&rest args) (incf decoded) (apply decoder args))
                                  (symbol-function 'call-with-logical-slot-wal-cursor-from-scan)
                                  (lambda (&rest args)
                                    (when (wal-retained-slot-scan-continuation (first args)) (incf fast))
                                    (apply original args)))
                            (let ((start (get-internal-real-time)) (bytes (sb-ext:get-bytes-consed)))
                              (loop
                                (when (> rows {count}) (error "reader duplicated rows"))
                                (when
                                    (call-with-logical-reader-spools
                                      (lambda ()
                                        (multiple-value-bind (transactions next exhausted)
                                            (wal-logical-slot-read-transactions
                                             "delayed_reader" :from-lsn progress :upto-lsn end
                                             :max-transactions 1)
                                          (dolist (tx transactions)
                                            (logical-transaction-map-changes tx
                                              (lambda (change)
                                                (when (eq (logical-change-kind change) :insert)
                                                  (incf rows)
                                                  (assert (= rows (aref (logical-change-new-values change) 0)))))))
                                          (assert (<= progress next end))
                                          (unless first-progress (setf first-progress next))
                                          (setf progress next)
                                          exhausted)))
                                  (return)))
                              (assert (= rows {count}))
                              (assert (= decoded reference))
                              (assert (>= fast {count}))
                              (format t "CONTINUATION-LINEAR rows=~d decoded=~d reference=~d fast=~d seconds=~,6f allocated=~d~%"
                                      rows decoded reference fast
                                      (/ (- (get-internal-real-time) start)
                                         (float internal-time-units-per-second 1d0))
                                      (- (sb-ext:get-bytes-consed) bytes)))
                            (assert (= confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
                            (assert (= restart (wal-replication-slot-restart-lsn slot)))
                            (setf saved *wal-logical-slot-read-continuation* saved-progress progress)
                            (assert saved)
                            (sb-thread:with-mutex (*wal-lock*)
                              (assert (eq saved (logical-wal-reusable-continuation slot progress end)))
                              (flet ((invalid-source ()
                                       (let ((message
                                               (handler-case
                                                   (logical-wal-check-source (logical-wal-continuation-source saved) end)
                                                 (error (condition) (princ-to-string condition)))))
                                         (assert (and message (search "source changed" message)))
                                         (assert (null (logical-wal-reusable-continuation slot progress end))))))
                                (let ((*wal-ready* nil)) (invalid-source))
                                (let ((*wal-insert-timeline* (1+ *wal-insert-timeline*))) (invalid-source))
                                (let ((*wal-segment-namespace* (if (eq *wal-segment-namespace* :legacy) :timeline :legacy)))
                                  (invalid-source))
                                (let ((*pgdata* (uiop:pathname-parent-directory-pathname (truename *pgdata*))))
                                  (invalid-source))
                                (let ((*wal-logical-slot-owner* (gensym "other-owner"))) (invalid-source))
                                (let ((*catalog* (copy-catalog *catalog*))) (invalid-source))
                                (let ((profile (wal-slot-profile slot)))
                                  (unwind-protect
                                      (progn (setf (gethash slot *wal-slot-profiles*)
                                                   (if (eq profile :text) :binary-bytea :text))
                                             (invalid-source))
                                    (setf (gethash slot *wal-slot-profiles*) profile)))
                                (let* ((provenance (copy-structure (wal-replication-slot-logical-provenance slot)))
                                       (replacement
                                         (make-wal-replication-slot
                                          :name (wal-replication-slot-name slot)
                                          :database-oid (wal-replication-slot-database-oid slot)
                                          :restart-lsn (wal-replication-slot-restart-lsn slot)
                                          :restart-timeline (wal-replication-slot-restart-timeline slot)
                                          :consistent-lsn (wal-replication-slot-consistent-lsn slot)
                                          :confirmed-flush-lsn (wal-replication-slot-confirmed-flush-lsn slot)
                                          :confirmed-timeline (wal-replication-slot-confirmed-timeline slot)
                                          :plugin (wal-replication-slot-plugin slot)
                                          :active-owner *wal-logical-slot-owner*
                                          :pending-xids (wal-replication-slot-pending-xids slot)
                                          :logical-provenance provenance)))
                                  (unwind-protect
                                      (progn (setf (gethash "delayed_reader" *wal-replication-slots*) replacement)
                                             (assert (eq provenance (wal-replication-slot-logical-provenance
                                                                      (gethash "delayed_reader" *wal-replication-slots*))))
                                             (invalid-source))
                                    (setf (gethash "delayed_reader" *wal-replication-slots*) slot))))
                              (let ((*wal-durable-lsn* (+ *wal-durable-lsn* 128)))
                                (assert (eq saved (logical-wal-reusable-continuation slot progress end))))
                              (assert (eq saved (logical-wal-reusable-continuation slot progress end))))
                            (let* ((owner *wal-logical-slot-owner*)
                                   (thread (sb-thread:make-thread
                                            (lambda ()
                                              (let ((*wal-logical-slot-owner* owner)
                                                    (*wal-logical-slot-read-continuation* saved))
                                                (sb-thread:with-mutex (*wal-lock*)
                                                  (logical-wal-reusable-continuation slot progress end)))))))
                              (assert (null (sb-thread:join-thread thread))))
                            ;; A different valid continuation retains the replay fallback.
                            (let ((*wal-logical-slot-read-continuation* saved) (replayed nil) (ids nil))
                              (setf (symbol-function 'call-with-logical-slot-wal-cursor-from-scan)
                                    (lambda (&rest args)
                                      (assert (null (wal-retained-slot-scan-continuation (first args))))
                                      (multiple-value-prog1 (apply original args) (setf replayed t))))
                              (call-with-logical-reader-spools
                                (lambda ()
                                  (multiple-value-bind (transactions next exhausted)
                                      (wal-logical-slot-read-transactions "delayed_reader" :from-lsn first-progress
                                                                          :upto-lsn end :max-transactions 1)
                                    (assert (> next first-progress)) (assert (not exhausted))
                                    (dolist (tx transactions)
                                      (logical-transaction-map-changes tx
                                        (lambda (change)
                                          (when (eq (logical-change-kind change) :insert)
                                            (push (aref (logical-change-new-values change) 0) ids))))))))
                              (assert replayed) (assert (equal ids '(2))))
                            ;; Invalidate the snapshot after fast admission but before wrapper entry.
                            (let ((injected nil) (message nil) (old-progress *wal-logical-slot-read-progress*))
                              (setf (symbol-function 'call-with-logical-slot-wal-cursor-from-scan)
                                    (lambda (&rest args)
                                      (assert (wal-retained-slot-scan-continuation (first args)))
                                      (let ((*catalog* (copy-catalog *catalog*)))
                                        (setf injected t)
                                        (apply original args))))
                              (setf message
                                    (handler-case
                                        (wal-logical-slot-read-transactions "delayed_reader" :from-lsn progress :upto-lsn end)
                                      (error (condition) (princ-to-string condition))))
                              (assert injected)
                              (assert (and message (search "source changed" message)))
                              (assert (eq saved *wal-logical-slot-read-continuation*))
                              (assert (eql old-progress *wal-logical-slot-read-progress*)))
                            ;; A changed catalog at admission must miss the certificate and replay.
                            (let ((*catalog* (copy-catalog *catalog*))
                                  (*wal-logical-slot-read-continuation* saved)
                                  (replayed nil))
                              (setf (symbol-function 'call-with-logical-slot-wal-cursor-from-scan)
                                    (lambda (&rest args)
                                      (assert (null (wal-retained-slot-scan-continuation (first args))))
                                      (multiple-value-prog1 (apply original args) (setf replayed t))))
                              (call-with-logical-reader-spools
                                (lambda ()
                                  (multiple-value-bind (transactions next exhausted)
                                      (wal-logical-slot-read-transactions "delayed_reader" :from-lsn progress :upto-lsn end)
                                    (assert (null transactions)) (assert (= progress next)) (assert exhausted))))
                              (assert replayed)))
                        (setf (symbol-function 'wal-decode-record) decoder
                              (symbol-function 'call-with-logical-slot-wal-cursor-from-scan) original)))))
                ;; Ownership release clears the dynamic certificate; a stolen old token cannot be reused.
                (assert (null *wal-logical-slot-read-continuation*))
                (call-with-logical-replication-slot "delayed_reader"
                  (lambda (slot)
                    (let ((*wal-logical-slot-read-continuation* saved))
                      (sb-thread:with-mutex (*wal-lock*)
                        (assert (null (logical-wal-reusable-continuation slot saved-progress *wal-durable-lsn*))))))))
              (values))''', timeout=45)
            matches = re.findall(r'CONTINUATION-LINEAR rows=(\d+) decoded=(\d+) reference=(\d+) fast=(\d+) seconds=([0-9.]+) allocated=(\d+)', response)
            assert len(matches) == 1, response
            result = matches[0]
            assert int(result[0]) == count and result[1] == result[2] and int(result[3]) >= count, result
            assert slot_image(server, 'delayed_reader')[0] == before
            check_unowned_terminal_advance(server, 'admin_advance')
            check_terminal_ack_overtake(server, 'admin_overtake')
            print('logical-slot-continuation: ' + ' '.join(result), flush=True)
        finally:
            connection.close()
        print(f'logical-slot-provenance: PASS (linear delayed feedback and source/owner fences) '
              f'{time.monotonic() - started:.6f}s; artifacts: {server.root}', flush=True)
    finally:
        server.stop(immediate=True)


if __name__ == '__main__':
    check()
    check_unfinished_tail()
    check_promoted()
    check_delayed_feedback_reader()

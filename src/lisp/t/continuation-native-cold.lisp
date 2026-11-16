(in-package #:pg)

(defvar *native-cold-test-failure* nil)

(defun native-host-cold-append ()
  (with-wal-critical
    (wal-append-batch-start)
    (wal-append-record +wal-record-maintenance+ 0 (make-array 0 :element-type '(unsigned-byte 8)))
    (wal-flush-core))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*)))

(defun native-host-cold-file-image ()
  (loop for name in '("pg_logical_continuation/selector" "pg_logical_continuation/selector.tmp"
                      "pg_replslot/alpha/state" "pg_replslot/alpha/state.tmp"
                      "pg_replslot/beta/state" "pg_replslot/beta/state.tmp")
        for path = (merge-pathnames name *pgdata*)
        when (slot-state-lstat path)
          collect (list name (physical-image-file-facts -100 (namestring path))
                        (read-bounded-regular-file path (logical-slot-continuation-prepared-maximum-bytes)))))

(defun native-host-cold-reservation-probes (reservation slot)
  (let ((reads 0) (original (symbol-function 'physical-image-file-facts)))
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-image-file-facts)
                 (lambda (&rest arguments) (incf reads) (apply original arguments)))
           (assert (handler-case
                       (progn (logical-slot-continuation-cold-reservation-call
                               (copy-logical-slot-continuation-cold-reservation reservation) :check) nil)
                     (error () t)))
           (assert (zerop reads)))
      (setf (symbol-function 'physical-image-file-facts) original)))
  (let ((*logical-slot-continuation-cold-reservations* (make-hash-table :test #'equal)))
    (assert (handler-case (progn (logical-slot-continuation-check-slot slot) nil)
              (error (condition) (search "root registration" (princ-to-string condition))))))
  (let ((*logical-slot-continuation-cold-registries* (make-hash-table :test #'eq)))
    (assert (handler-case
                (progn (logical-slot-continuation-cold-reservation-call reservation :check) nil)
              (error () t))))
  (let ((*wal-replication-slots* (make-hash-table :test #'equal)))
    (assert (handler-case
                (progn (logical-slot-continuation-cold-reservation-call reservation :check) nil)
              (error () t))))
  (let ((*wal-snapshot-retention-pins* (make-hash-table :test #'eq)))
    (assert (handler-case
                (progn (logical-slot-continuation-cold-reservation-call reservation :check) nil)
              (error () t))))
  (assert (logical-slot-continuation-cold-reservation-call reservation :check))
  (dolist (create (list (lambda () (wal-check-replication-slot-capacity))
                        (lambda () (physical-slot-publish-create-under-lock "cold_probe" nil nil nil))
                        (lambda () (wal-create-replication-slot-file slot))))
    (assert (handler-case (progn (funcall create) nil)
              (error (condition)
                (search "cold reconciliation has not published" (princ-to-string condition)))))))

(defun native-host-cold-recheck-refused-before-io (check)
  (let ((reads 0) (original (symbol-function 'physical-image-file-facts)))
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-image-file-facts)
                 (lambda (&rest arguments) (incf reads) (apply original arguments)))
           (assert (handler-case (progn (funcall check) nil) (error () t)))
           (assert (zerop reads)))
      (setf (symbol-function 'physical-image-file-facts) original))))

(defun native-host-cold-classification (gate ownership)
  (let* ((case (uiop:getenv "PG_NATIVE_COLD_CASE"))
         (expected-generation (parse-integer (uiop:getenv "PG_NATIVE_COLD_GENERATION")))
         (expected-kind (intern (uiop:getenv "PG_NATIVE_COLD_KIND") :keyword))
         (expected-choice (intern (uiop:getenv "PG_NATIVE_COLD_CHOICE") :keyword))
         (registry *wal-replication-slots*) (mirrors (logical-slot-publication-mirrors))
         (pins (hash-table-count *wal-snapshot-retention-pins*))
         (fds (native-host-writer-open-fds)) (files (native-host-cold-file-image))
         (reservation nil) (visits 0) (escaped nil) (expired-checks 0)
         (*logical-slot-continuation-cold-test-hook*
           (lambda (point actual)
             (declare (ignore actual))
             (when (eq point :after-observation)
               (native-host-cold-recheck-refused-before-io escaped)
               (incf expired-checks)))))
    (assert (zerop (hash-table-count registry)))
    (dotimes (attempt 2)
      (call-with-logical-slot-continuation-cold-state
       gate ownership
       (lambda (observation actual check)
         (incf visits)
         (setf escaped check)
         (let ((result (sb-thread:join-thread
                        (sb-thread:make-thread
                         (lambda ()
                           (handler-case
                               (progn (native-host-cold-recheck-refused-before-io check) :passed)
                             (error (condition) condition)))))))
           (assert (eq :passed result)))
         (assert (eq expected-kind (getf observation :kind)))
         (assert (eq expected-choice (getf observation :choice)))
         (let ((snapshot (getf observation :snapshot)))
           (assert (= expected-generation (getf snapshot :generation)))
           (assert (= *wal-durable-lsn* (getf observation :horizon)))
           (when (uiop:getenv "PG_NATIVE_COLD_APPEND")
             (assert (< (first (getf snapshot :sealed-end)) (getf observation :horizon))))
           (sb-thread:with-mutex (*wal-lock*)
             (assert (logical-slot-continuation-cold-reservation-call actual :check))
             (assert (= 41268 (gethash actual *wal-snapshot-retention-pins*)))
             (assert (handler-case
                         (progn
                           (logical-slot-continuation-cold-reservation-call
                            (copy-logical-slot-continuation-cold-reservation actual) :check)
                           nil)
                       (error () t)))
             (let ((slot (logical-slot-state-decode (second (first (getf snapshot :slots))) "alpha")))
               (native-host-cold-reservation-probes actual slot)
               (assert (handler-case
                           (progn (logical-slot-continuation-check-slot slot) nil)
                         (error (condition)
                           (search "cold reconciliation has not published" (princ-to-string condition)))))))
           (incf (getf snapshot :generation))
           (setf (aref (getf snapshot :binding) 0) 0))
         (when reservation (assert (eq actual reservation)))
         (setf reservation actual)
         (funcall check)))
      (assert (eq registry *wal-replication-slots*))
      (assert (zerop (hash-table-count registry)))
      (assert (equal mirrors (logical-slot-publication-mirrors)))
      (assert (equalp files (native-host-cold-file-image)))
      (assert (= (1+ pins) (hash-table-count *wal-snapshot-retention-pins*))))
    (assert (= 2 visits expired-checks))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (sb-thread:with-mutex (*wal-lock*)
      (assert (logical-slot-continuation-cold-reservation-call reservation :check))
      (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*))))
    (format t "~&NATIVE-COLD-CLASSIFICATION PASS case=~a kind=~s choice=~s generation=~d horizon=~d persistent-F no-admission repeat-idempotent no-fd-leak~%"
            case expected-kind expected-choice expected-generation *wal-durable-lsn*)))

(defun native-host-cold-mutate (mode)
  (cond
    ((equal mode "corrupt-selector")
     (let* ((path (merge-pathnames "pg_logical_continuation/selector" *pgdata*))
            (bytes (read-bounded-regular-file path (logical-slot-continuation-prepared-maximum-bytes))))
       (setf (aref bytes (1- (length bytes))) (logxor 1 (aref bytes (1- (length bytes)))))
       (with-open-file (stream path :direction :output :if-exists :overwrite
                                    :element-type '(unsigned-byte 8))
         (write-sequence bytes stream))))
    ((equal mode "changed-beta")
     (let* ((path (wal-replication-slot-path "beta"))
            (slot (logical-slot-state-decode (read-bounded-regular-file path +slot-state-maximum-bytes+) "beta")))
       (incf (wal-replication-slot-confirmed-flush-lsn slot))
       (with-open-file (stream path :direction :output :if-exists :supersede
                                    :element-type '(unsigned-byte 8))
         (write-sequence (logical-slot-state-encode slot) stream))
       (sb-posix:chmod (namestring path) #o600)))
    ((member mode '("stable-state-temp" "unrelated-state-temp" "wrong-prefix") :test #'equal)
     (let* ((name (if (equal mode "unrelated-state-temp") "beta" "alpha"))
            (path (merge-pathnames (concatenate 'string "pg_replslot/" name "/state.tmp") *pgdata*)))
       (with-open-file (stream path :direction :output :if-exists :supersede
                                    :element-type '(unsigned-byte 8))
         (write-sequence (string->bytes "unselected wrong prefix") stream))
       (sb-posix:chmod (namestring path) #o600)))))

(defun native-host-cold-refusal (gate ownership mode)
  (let ((fds (native-host-writer-open-fds))
        (close (symbol-function 'sb-posix:close))
        (reached 0) (caught nil) (files nil)
        (expected (cond ((equal mode "corrupt-selector") "invalid continuation snapshot envelope")
                        ((equal mode "changed-beta") "exact saved inventory")
                        ((equal mode "wrong-prefix") "not an exact new-state prefix")
                        ((equal mode "caught-recheck-failure") "observation failed")
                        ((member mode '("stable-state-temp" "unrelated-state-temp") :test #'equal)
                         "unclassified slot entry")
                        (t "native cold injected failure"))))
    (native-host-cold-mutate mode)
    (setf files (native-host-cold-file-image))
    (flet ((stop () (incf reached) (error "native cold injected failure ~a" mode)))
      (let ((*logical-slot-continuation-cold-test-hook*
              (lambda (point reservation)
                (declare (ignore reservation))
                (cond
                  ((and (equal mode "protected") (eq point :protected)) (stop))
                  ((and (equal mode "reservation-returned") (eq point :reservation-returned)) (stop))
                  ((and (equal mode "after-observation") (eq point :after-observation)) (stop))
                  ((and (equal mode "after-proof-return") (eq point :after-proof-return)) (stop))
                  ((and (equal mode "source-close") (eq point :after-observation))
                   (setf (symbol-function 'sb-posix:close)
                         (lambda (fd)
                           (funcall close fd)
                           (setf (symbol-function 'sb-posix:close) close)
                           (stop))))))))
        (unwind-protect
             (setf caught
                   (handler-case
                       (progn
                         (call-with-logical-slot-continuation-cold-state
                          gate ownership
                          (lambda (observation reservation check)
                            (declare (ignore observation reservation))
                            (funcall check)
                            (when (equal mode "callback") (stop))
                            (when (equal mode "caught-recheck-failure")
                              (let ((original (symbol-function 'physical-image-file-facts)))
                                (unwind-protect
                                     (progn
                                       (setf (symbol-function 'physical-image-file-facts)
                                             (lambda (&rest ignored)
                                               (declare (ignore ignored))
                                               (setf (symbol-function 'physical-image-file-facts) original)
                                               (stop)))
                                       (assert (handler-case (progn (funcall check) nil) (error () t))))
                                  (setf (symbol-function 'physical-image-file-facts) original))))))
                         nil)
                     (error (condition) (princ-to-string condition))))
          (setf (symbol-function 'sb-posix:close) close))))
    (assert (and caught (search expected caught)))
    (unless (member mode '("corrupt-selector" "changed-beta" "wrong-prefix"
                           "stable-state-temp" "unrelated-state-temp") :test #'equal)
      (assert (= 1 reached)))
    (assert (= 1 (hash-table-count *logical-slot-continuation-cold-reservations*)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (sb-thread:with-mutex (*wal-lock*)
      (loop for reservation being the hash-values of *logical-slot-continuation-cold-reservations*
            do (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
               (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
               (assert (handler-case
                           (progn (logical-slot-continuation-cold-reservation-call reservation :check) nil)
                         (error () t)))))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (equalp files (native-host-cold-file-image)))
    (format t "~&NATIVE-COLD-CLASSIFICATION PASS case=~a refusal=~s persistent-F uncertain no-cleanup no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE") caught)))

(defun native-host-continuation-cold-state (gate ownership)
  (handler-case
      (progn
        (when (uiop:getenv "PG_NATIVE_COLD_APPEND") (native-host-cold-append))
        (if (uiop:getenv "PG_NATIVE_COLD_FAULT")
            (native-host-cold-refusal gate ownership (uiop:getenv "PG_NATIVE_COLD_FAULT"))
            (native-host-cold-classification gate ownership)))
    (error (condition)
      (setf *native-cold-test-failure* condition)
      (format t "~&NATIVE-COLD-CLASSIFICATION FAILURE ~a~%" condition)
      (finish-output))))

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

(defun native-host-cold-parent-boundaries (gate ownership)
  (let ((files (native-host-cold-file-image)) (fds (native-host-writer-open-fds))
                                              (proved nil))
    (call-with-logical-slot-continuation-cold-context
     gate ownership
     (lambda (storage reservation archive)
       (call-with-logical-slot-continuation-cold-observation
        storage reservation
        (lambda (observation actual check)
          (assert (eq actual reservation))
          (let* ((facts (logical-slot-continuation-storage-call storage :facts))
                 (snapshot (getf observation :snapshot))
                 (requests (logical-slot-continuation-saved-boundaries snapshot (getf facts :certificate))))
            (assert (member '(:confirmed 29061 1 :prefix-end) requests :test #'equal))
            (assert (member '(:restart 29061 1 :prefix-end) requests :test #'equal))
            (assert (null (logical-slot-continuation-prove-parent-boundaries
                           storage facts archive
                           (cons '(:confirmed 29285 1 :prefix-end) requests))))
            (funcall check)
            (setf proved t))))))
    (assert proved)
    (assert (equalp files (native-host-cold-file-image)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (format t "~&NATIVE-COLD-PARENT-BOUNDARIES PASS case=~a target=29061 D=29285 baseline=20430 no-admission no-cleanup~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-selected-boundaries (gate ownership)
  (let ((files (native-host-cold-file-image)) (fds (native-host-writer-open-fds))
                                              (proved nil))
    (call-with-logical-slot-continuation-cold-context
     gate ownership
     (lambda (storage reservation archive)
       (call-with-logical-slot-continuation-cold-observation
        storage reservation
        (lambda (observation actual check)
          (assert (eq actual reservation))
          (let* ((facts (logical-slot-continuation-storage-call storage :facts))
                 (snapshot (getf observation :snapshot))
                 (horizon (getf observation :horizon))
                 (sealed (first (getf snapshot :sealed-end))))
            (flet ((boundaries ()
                     (funcall check)
                     (logical-slot-continuation-runtime-tail-observer
                      (second (getf snapshot :origin)) sealed
                      (logical-slot-continuation-prove-parent-boundaries
                       storage facts archive
                       (logical-slot-continuation-saved-boundaries snapshot (getf facts :certificate))
                       :batch-policy :logical-reader)
                      :logical-reader)))
              (call-with-logical-slot-continuation-local-tail-source
               storage snapshot
               (lambda (prove check-tail reader guard)
                 (declare (ignore reader guard))
                 (funcall prove)
                 (funcall check-tail)
                 (funcall check)
                 (funcall prove))
               :boundaries #'boundaries :end horizon
               :batch-policy :logical-reader :require-closed nil))
            (funcall check)
            (setf proved t))))))
    (assert proved)
    (assert (equalp files (native-host-cold-file-image)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (format t "~&NATIVE-COLD-SELECTED-BOUNDARIES PASS case=~a historical-seal full-horizon reproof no-admission no-cleanup~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-source-probes (source check)
  (let* ((inventory (logical-slot-startup-source-inventory source))
         (copied (copy-logical-slot-startup-inventory inventory))
         (state (logical-slot-startup-inventory-state inventory)))
    (native-host-cold-recheck-refused-before-io
     (lambda () (logical-slot-startup-inventory-call
                 (copy-logical-slot-imported-startup-source source) :observe)))
    (native-host-cold-recheck-refused-before-io
     (lambda () (logical-slot-startup-inventory-dispatch copied :observe source)))
    (unwind-protect
         (progn
           (setf (gethash copied *logical-slot-startup-inventories*) state)
           (native-host-cold-recheck-refused-before-io
            (lambda () (logical-slot-startup-inventory-dispatch copied :observe source))))
      (remhash copied *logical-slot-startup-inventories*))
    (native-host-cold-recheck-refused-before-io
     (lambda () (logical-slot-startup-inventory-dispatch inventory :preflight)))
    (let ((refused nil))
      (sb-thread:join-thread
       (sb-thread:make-thread
        (lambda ()
          (native-host-cold-recheck-refused-before-io
           (lambda () (logical-slot-startup-inventory-call source :observe)))
          (setf refused t))
        :name "foreign-cold-inventory-probe"))
      (assert refused))
    (assert (logical-slot-startup-inventory-call source :check))
    (funcall check)))

(defun native-host-cold-reconstruct-pending (gate ownership)
  (let ((files (native-host-cold-file-image)) (fds (native-host-writer-open-fds))
                                              (saved-check nil) (saved-source nil) (count 0)
                                              (*logical-capture-memory-bytes* (if (uiop:getenv "PG_NATIVE_COLD_SPILL")
                                                                                  0 *logical-capture-memory-bytes*)))
    (funcall
     (symbol-function 'call-with-logical-slot-continuation-cold-reconstruction)
     gate ownership
     (lambda (source observation reservation check)
       (assert (logical-slot-imported-startup-source-p source))
       (native-host-cold-source-probes source check)
       (when (probe-file (merge-pathnames "pg_replslot/alpha/state.tmp" *pgdata*))
         (let ((entered nil))
           (assert (handler-case
                       (progn
                         (call-with-logical-slot-imported-startup-source
                          gate ownership (logical-slot-startup-source-entries source)
                          (lambda (ordinary) (declare (ignore ordinary)) (setf entered t)))
                         nil)
                     (error (condition)
                       (format t "~&NATIVE-COLD-ORDINARY-REFUSAL ~a~%" condition)
                       (search "unfinished replication slot state update" (princ-to-string condition)))))
           (assert (not entered))))
       (assert (= (logical-slot-startup-source-end source) (getf observation :horizon)))
       (dolist (entry (logical-slot-startup-source-entries source))
         (let* ((slot (logical-slot-startup-entry-slot entry))
                (confirmed (wal-replication-slot-confirmed-flush-lsn slot))
                (pending (wal-replication-slot-pending-xids slot))
                (expected (remove-if-not
                           (lambda (row) (> (third row) confirmed))
                           '((3 20598 29061) (4 29381 37853)))))
           (assert (member (wal-replication-slot-name slot) '("alpha" "beta") :test #'equal))
           (assert (= (length expected) (hash-table-count pending)))
           (dolist (row expected)
             (assert (equal (cons (second row) (third row)) (gethash (first row) pending))))
           (incf count)))
       (assert (= count 2))
       (assert (zerop (hash-table-count *wal-replication-slots*)))
       (funcall check)
       (sb-thread:with-mutex (*wal-lock*)
         (logical-slot-continuation-cold-reservation-call reservation :check))
       (setf saved-check check saved-source source))
     :policy (equal "policy" (uiop:getenv "PG_NATIVE_COLD_PROOF")))
    (assert (= count 2))
    (native-host-cold-recheck-refused-before-io saved-check)
    (native-host-cold-recheck-refused-before-io
     (lambda () (logical-slot-startup-inventory-call saved-source :observe)))
    (assert (equalp files (native-host-cold-file-image)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (format t "~&NATIVE-COLD-PENDING PASS case=~a both-members through-L exact-pending no-admission no-cleanup~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-pending-refusal (gate ownership mode)
  (let ((fds (native-host-writer-open-fds)) (files (native-host-cold-file-image))
                                            (reached nil) (saved-source nil) (caught nil))
    (setf caught
          (handler-case
              (progn
                (call-with-logical-slot-continuation-cold-reconstruction
                 gate ownership
                 (lambda (source observation reservation check)
                   (declare (ignore observation reservation))
                   (setf reached t saved-source source)
                   (cond
                     ((equal mode "pending-registration")
                      (let* ((inventory (logical-slot-startup-source-inventory source))
                             (state (gethash inventory *logical-slot-startup-inventories*)))
                        (unwind-protect
                             (progn
                               (remhash inventory *logical-slot-startup-inventories*)
                               (let ((sources *logical-slot-startup-sources*) (refused nil))
                                 (sb-thread:join-thread
                                  (sb-thread:make-thread
                                   (lambda ()
                                     (let ((*logical-slot-startup-sources* sources))
                                       (native-host-cold-recheck-refused-before-io
                                        (lambda () (logical-slot-startup-inventory-call source :check)))
                                       (setf refused t)))
                                   :name "missing-cold-inventory-foreign-probe"))
                                 (assert refused)
                                 (assert (logical-slot-startup-source-active-p source)))
                               (native-host-cold-recheck-refused-before-io check)
                               (assert (not (logical-slot-startup-source-active-p source))))
                          (setf (gethash inventory *logical-slot-startup-inventories*) state))))
                     ((equal mode "pending-candidate")
                      (let* ((slot (logical-slot-startup-entry-slot
                                    (first (logical-slot-startup-source-entries source))))
                             (confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
                        (unwind-protect
                             (progn
                               (incf (wal-replication-slot-confirmed-flush-lsn slot))
                               (native-host-cold-recheck-refused-before-io check))
                          (setf (wal-replication-slot-confirmed-flush-lsn slot) confirmed))))
                     ((equal mode "pending-temp-swap")
                      (let* ((path (merge-pathnames "pg_replslot/alpha/state.tmp" *pgdata*))
                             (replacement (merge-pathnames "pg_replslot/alpha/state.replacement" *pgdata*))
                             (bytes (read-bounded-regular-file path +slot-state-maximum-bytes+)))
                        (with-open-file (stream replacement :direction :output :if-exists :error
                                                            :element-type '(unsigned-byte 8))
                          (write-sequence bytes stream))
                        (sb-posix:chmod (namestring replacement) #o600)
                        (sb-posix:rename (namestring replacement) (namestring path))
                        (setf files (native-host-cold-file-image))
                        (assert (handler-case (progn (funcall check) nil) (error () t)))))
                     (t (error "unknown cold pending refusal ~s" mode)))
                   (native-host-cold-recheck-refused-before-io check)))
                nil)
            (error (condition) (princ-to-string condition))))
    (assert (and reached caught))
    (native-host-cold-recheck-refused-before-io
     (lambda () (logical-slot-startup-inventory-call saved-source :observe)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (sb-thread:with-mutex (*wal-lock*)
      (loop for reservation being the hash-values of *logical-slot-continuation-cold-reservations*
            do (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
               (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))))
    (assert (= 1 (hash-table-count *logical-slot-continuation-cold-reservations*)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (equalp files (native-host-cold-file-image)))
    (format t "~&NATIVE-COLD-PENDING-REFUSAL PASS case=~a mode=~a sticky no-fallback persistent-F no-cleanup~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE") mode)))

(defun native-host-cold-policy-refusal (gate ownership mode)
  (dolist (enabled (if (equal mode "policy-off-on") '(nil t) '(nil)))
    (with-wal-critical
      (wal-append-batch-start)
      (wal-append-record +wal-record-logical-decoding-status-change+ 0
                         (wal-encode-logical-decoding-status-change enabled))
      (wal-flush-core)))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*))
  (let ((files (native-host-cold-file-image)) (fds (native-host-writer-open-fds))
                                              (entered nil) (caught nil))
    (setf caught
          (handler-case
              (progn
                (call-with-logical-slot-continuation-cold-reconstruction
                 gate ownership
                 (lambda (&rest args) (declare (ignore args)) (setf entered t))
                 :policy t)
                nil)
            (error (condition) (princ-to-string condition))))
    (assert (and caught (search "slot birth inside the current ON interval" caught)))
    (assert (not entered))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (assert (equalp files (native-host-cold-file-image)))
    (assert (equal fds (native-host-writer-open-fds)))
    (sb-thread:with-mutex (*wal-lock*)
      (assert (= 1 (hash-table-count *logical-slot-continuation-cold-reservations*)))
      (loop for reservation being the hash-values of *logical-slot-continuation-cold-reservations*
            do (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
               (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*))))))
  (format t "~&NATIVE-COLD-POLICY-REFUSAL PASS case=~a mode=~a preserved-F no-admission no-cleanup~%"
          (uiop:getenv "PG_NATIVE_COLD_CASE") mode))

(defun native-host-cold-preparation (gate ownership)
  (let ((files (native-host-cold-file-image)) (fds (native-host-writer-open-fds))
                                              (saved nil) (count 0)
                                              (*logical-capture-memory-bytes* (if (uiop:getenv "PG_NATIVE_COLD_SPILL")
                                                                                  0 *logical-capture-memory-bytes*)))
    (flet ((call (token operation)
             (uiop:symbol-call :pg :logical-slot-continuation-cold-preparation-call token operation)))
      (funcall
       (symbol-function 'call-with-logical-slot-continuation-cold-preparation)
       gate ownership
       (lambda (token)
         (assert (null *logical-slot-startup-sources*))
         (assert (zerop (hash-table-count *logical-slot-startup-inventories*)))
         (assert (zerop (hash-table-count *wal-replication-slots*)))
         (assert (call token :check))
         (native-host-cold-recheck-refused-before-io
          (lambda () (call (copy-structure token) :check)))
         (let ((refused nil))
           (sb-thread:join-thread
            (sb-thread:make-thread
             (lambda ()
               (native-host-cold-recheck-refused-before-io (lambda () (call token :check)))
               (setf refused t))))
           (assert refused))
         (let ((registry (call token :registry)))
           (assert (not (eq registry *wal-replication-slots*)))
           (assert (= 2 (hash-table-count registry)))
           (maphash
            (lambda (name slot)
              (assert (equal name (wal-replication-slot-name slot)))
              (let* ((confirmed (wal-replication-slot-confirmed-flush-lsn slot))
                     (pending (wal-replication-slot-pending-xids slot))
                     (expected (remove-if-not (lambda (row) (> (third row) confirmed))
                                              '((3 20598 29061) (4 29381 37853)))))
                (assert (= (length expected) (hash-table-count pending)))
                (dolist (row expected)
                  (assert (equal (cons (second row) (third row)) (gethash (first row) pending)))))
              (incf count))
            registry))
         (sb-thread:with-mutex (*wal-lock*)
           (let ((*max-replication-slots* 0))
             (assert (handler-case (progn (wal-check-replication-slot-capacity) nil)
                       (pg-error (condition)
                         (search "has not published its group" (princ-to-string condition)))))))
         (assert (call token :claim))
         (setf saved token)))
      (assert (= count 2))
      (native-host-cold-recheck-refused-before-io (lambda () (call saved :check)))
      (assert (equalp files (native-host-cold-file-image)))
      (assert (equal fds (native-host-writer-open-fds)))
      (assert (zerop (hash-table-count *wal-replication-slots*)))
      (format t "~&NATIVE-COLD-PREPARATION PASS case=~a private-pending closed-source-scopes typed-one-shot no-admission~%"
              (uiop:getenv "PG_NATIVE_COLD_CASE")))))

(defun native-host-cold-reconcile-kill (point)
  (when (equal point (uiop:getenv "PG_NATIVE_COLD_RECONCILE_CUT"))
    (format t "~&NATIVE-COLD-RECONCILIATION-SIGKILL ~a~%" point)
    (finish-output)
    (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
    (error "reconciliation SIGKILL returned")))

(defun native-host-cold-reconcile (gate ownership)
  (let ((before (native-host-cold-file-image))
        (fds (native-host-writer-open-fds))
        (registry *wal-replication-slots*)
        (mirrors (logical-slot-publication-mirrors))
        (snapshot nil) (reservation nil)
        (*logical-slot-continuation-cold-reconciliation-test-hook*
          (lambda (point) (native-host-cold-reconcile-kill (string-downcase point))))
        (*logical-slot-continuation-retirement-test-hook*
          (lambda (point purpose)
            (native-host-cold-reconcile-kill
             (format nil "~(~a~)-~(~a~)" purpose point))))
        (*logical-slot-continuation-replacement-test-hook*
          (lambda (point)
            (native-host-cold-reconcile-kill (format nil "replacement-~(~a~)" point)))))
    (call-with-logical-slot-continuation-cold-state
     gate ownership
     (lambda (observation actual check)
       (setf snapshot (getf observation :snapshot) reservation actual)
       (funcall check)))
    (assert (= (getf snapshot :generation)
               (parse-integer (uiop:getenv "PG_NATIVE_COLD_GENERATION"))))
    (format t "~&NATIVE-COLD-RECONCILIATION-EXPECTED ~a~%"
            (with-output-to-string (stream)
              (loop for byte across (logical-slot-continuation-encode snapshot)
                    do (format stream "~2,'0x" byte))))
    (finish-output)
    (assert (equalp snapshot
                    (uiop:symbol-call :pg :logical-slot-continuation-cold-reconcile gate ownership)))
    (let* ((after (native-host-cold-file-image))
           (selector (assoc "pg_logical_continuation/selector" after :test #'string=)))
      (assert (equalp (third selector) (logical-slot-continuation-encode snapshot)))
      (dolist (row after)
        (assert (not (search ".tmp" (first row))))
        (unless (string= (first row) "pg_logical_continuation/selector")
          (assert (equalp row (assoc (first row) before :test #'string=)))))
      (dolist (row (getf snapshot :slots))
        (assert (equalp (second row)
                        (third (assoc (format nil "pg_replslot/~a/state" (first row))
                                      after :test #'string=)))))
      (when (string= (uiop:getenv "PG_NATIVE_COLD_KIND") "STABLE")
        (assert (equalp selector
                        (assoc "pg_logical_continuation/selector" before :test #'string=))))
      (assert (equalp snapshot
                      (uiop:symbol-call :pg :logical-slot-continuation-cold-reconcile gate ownership)))
      (assert (equalp after (native-host-cold-file-image))))
    (assert (eq registry *wal-replication-slots*))
    (assert (zerop (hash-table-count registry)))
    (assert (equal mirrors (logical-slot-publication-mirrors)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (sb-thread:with-mutex (*wal-lock*)
      (assert (logical-slot-continuation-cold-reservation-call reservation :check))
      (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*))))
    (format t "~&NATIVE-COLD-RECONCILIATION PASS case=~a exact-selected-generation-and-seal stable-state-inodes repeat-idempotent retained-F no-admission~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-reconcile-reenter (gate ownership)
  (let* ((files (native-host-cold-file-image))
         (fds (native-host-writer-open-fds))
         (armed t) (refused nil) (caught nil)
         (*logical-slot-continuation-cold-reconciliation-test-hook*
           (lambda (point)
             (when (and armed (eq point :before-effects))
               (setf armed nil)
               (let ((context (loop for key being the hash-keys
                                      of *logical-slot-continuation-cold-reconciliations*
                                    return key)))
                 (assert context)
                 (setf caught
                       (handler-case
                           (progn (logical-slot-continuation-cold-reconciliation-execute context) nil)
                         (error () t))))))))
    (setf refused
          (handler-case
              (progn (uiop:symbol-call :pg :logical-slot-continuation-cold-reconcile gate ownership) nil)
            (error () t)))
    (assert (and (not armed) caught refused))
    (assert (equalp files (native-host-cold-file-image)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-RECONCILIATION-REFUSAL PASS case=~a caught-reentry-sticky no-effects retained-F~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-reconcile-failure (gate ownership mode)
  (let* ((before (native-host-cold-file-image))
         (fds (native-host-writer-open-fds))
         (reservation nil) (snapshot nil) (hit nil)
         (point (cond ((equal mode "reconcile-after-unlink") :after-slot-root-sync)
                      ((equal mode "reconcile-after-rename") :stable-selected)
                      ((equal mode "reconcile-after-close") :stable-durable)
                      (t (error "unknown reconciliation failure mode"))))
         (*logical-slot-continuation-cold-reconciliation-test-hook*
           (lambda (actual)
             (when (eq point actual) (setf hit t) (error "reconciliation effect cut")))))
    (call-with-logical-slot-continuation-cold-state
     gate ownership
     (lambda (observation actual check)
       (setf reservation actual snapshot (getf observation :snapshot))
       (funcall check)))
    (assert (handler-case
                (progn (uiop:symbol-call :pg :logical-slot-continuation-cold-reconcile gate ownership) nil)
              (error () t)))
    (assert hit)
    (let ((after (native-host-cold-file-image)))
      (assert (not (equalp before after)))
      (dolist (row after)
        (unless (search "selector" (first row))
          (assert (not (search ".tmp" (first row))))
          (assert (equalp row (assoc (first row) before :test #'string=)))))
      (unless (eq point :after-slot-root-sync)
        (assert (equalp (logical-slot-continuation-encode snapshot)
                        (third (assoc "pg_logical_continuation/selector" after :test #'string=)))))
      (assert (handler-case
                  (progn (uiop:symbol-call :pg :logical-slot-continuation-cold-reconcile gate ownership) nil)
                (error () t)))
      (assert (equalp after (native-host-cold-file-image))))
    (sb-thread:with-mutex (*wal-lock*)
      (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
      (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
      (assert (handler-case
                  (progn (logical-slot-continuation-cold-reservation-call reservation :check) nil)
                (error () t))))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-RECONCILIATION-REFUSAL PASS case=~a mode=~a actual-effects uncertain-F retry-fenced no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE") mode)))

(defun native-host-cold-preparation-refusal (gate ownership mode)
  (let ((files (native-host-cold-file-image)) (fds (native-host-writer-open-fds))
                                              (saved nil) (entered nil) (caught nil) (reentered nil))
    (flet ((call (token operation)
             (logical-slot-continuation-cold-preparation-call token operation)))
      (setf caught
            (handler-case
                (let ((*max-replication-slots* (if (equal mode "prepare-capacity") 1 *max-replication-slots*))
                      (*logical-slot-continuation-cold-test-hook*
                        (when (equal mode "prepare-after-close")
                          (lambda (point value)
                            (declare (ignore value))
                            (when (eq point :after-proof-return) (error "injected closed-scope failure"))))))
                  (call-with-logical-slot-continuation-cold-preparation
                   gate ownership
                   (lambda (token)
                     (setf saved token entered t)
                     (cond
                       ((equal mode "prepare-registration")
                        (let ((state (gethash token *logical-slot-continuation-cold-preparations*))
                              (scopes *logical-slot-continuation-cold-preparation-scopes*))
                          (remhash token *logical-slot-continuation-cold-preparations*)
                          (let ((refused nil))
                            (sb-thread:join-thread
                             (sb-thread:make-thread
                              (lambda ()
                                (let ((*logical-slot-continuation-cold-preparation-scopes* scopes))
                                  (native-host-cold-recheck-refused-before-io (lambda () (call token :check)))
                                  (setf refused t)))))
                            (assert refused))
                          (setf (gethash token *logical-slot-continuation-cold-preparations*) state)
                          (assert (call token :check))
                          (unwind-protect
                               (progn
                                 (remhash token *logical-slot-continuation-cold-preparations*)
                                 (native-host-cold-recheck-refused-before-io (lambda () (call token :check))))
                            (setf (gethash token *logical-slot-continuation-cold-preparations*) state))))
                       ((member mode '("prepare-pending" "prepare-pending-swap") :test #'equal)
                        (let* ((slot (gethash "beta" (call token :registry)))
                               (pending (wal-replication-slot-pending-xids slot))
                               (positions (gethash 3 pending)))
                          (unwind-protect
                               (progn
                                 (if (equal mode "prepare-pending-swap")
                                     (setf (wal-replication-slot-pending-xids slot) (let ((copy (make-hash-table :test #'eql)))
                                                                                      (maphash (lambda (xid pair) (setf (gethash xid copy) pair)) pending)
                                                                                      copy))
                                     (setf (gethash 3 pending) (cons (1+ (car positions)) (cdr positions))))
                                 (assert (handler-case (progn (call token :check) nil) (error () t))))
                            (setf (wal-replication-slot-pending-xids slot) pending
                                  (gethash 3 pending) positions))))
                       ((equal mode "prepare-reuse")
                        (assert (call token :claim))
                        (native-host-cold-recheck-refused-before-io (lambda () (call token :claim))))
                       ((equal mode "prepare-reenter")
                        (let ((*logical-slot-continuation-cold-test-hook*
                                (lambda (point value)
                                  (declare (ignore value))
                                  (when (eq point :after-proof-return)
                                    (let ((masked nil))
                                      (native-host-cold-recheck-refused-before-io
                                       (lambda ()
                                         (handler-bind ((error (lambda (condition)
                                                                 (declare (ignore condition))
                                                                 (setf masked (not sb-sys:*interrupts-enabled*)))))
                                           (call token :registry))))
                                      (assert masked))
                                    (setf reentered t)))))
                          (assert (handler-case (progn (call token :claim) nil) (error () t)))
                          (assert reentered)))
                       (t (error "unexpected preparation refusal callback ~s" mode)))
                     (native-host-cold-recheck-refused-before-io (lambda () (call token :check)))))
                  nil)
              (error (condition) (princ-to-string condition))))
      (assert caught)
      (assert (eq (not (null entered))
                  (not (member mode '("prepare-capacity" "prepare-after-close") :test #'equal))))
      (when saved
        (native-host-cold-recheck-refused-before-io (lambda () (call saved :check))))
      (assert (zerop (hash-table-count *logical-slot-continuation-cold-preparations*)))
      (assert (zerop (hash-table-count *wal-replication-slots*)))
      (sb-thread:with-mutex (*wal-lock*)
        (assert (= 1 (hash-table-count *logical-slot-continuation-cold-reservations*)))
        (loop for reservation being the hash-values of *logical-slot-continuation-cold-reservations*
              do (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
                 (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))))
      (assert (equalp files (native-host-cold-file-image)))
      (assert (equal fds (native-host-writer-open-fds)))
      (format t "~&NATIVE-COLD-PREPARATION-REFUSAL PASS case=~a mode=~a sticky persistent-F no-admission~%"
              (uiop:getenv "PG_NATIVE_COLD_CASE") mode))))

(defun native-host-cold-preparation-wal-change (gate ownership)
  (let ((files (native-host-cold-file-image)) (fds (native-host-writer-open-fds))
                                              (entered nil) (proved nil) (refusal nil) (caught nil))
    (setf caught
          (handler-case
              (progn
                (call-with-logical-slot-continuation-cold-preparation
                 gate ownership
                 (lambda (token)
                   (setf entered t)
                   (let* ((observation (logical-slot-continuation-cold-preparation-call token :observation))
                          (proof (logical-slot-continuation-cold-preparation-call token :proof))
                          (registry (logical-slot-continuation-cold-preparation-call token :registry))
                          (image (logical-slot-continuation-cold-preparation-semantic-image
                                  (logical-slot-publication-registry-image registry)))
                          (start (first (getf (getf observation :snapshot) :sealed-end)))
                          (header (make-array +wal-header-size+ :element-type '(unsigned-byte 8))))
                     (assert (< start (getf observation :horizon)))
                     (with-open-file (stream (wal-segment-path (floor start +wal-segment-size+))
                                             :direction :io :if-exists :overwrite :if-does-not-exist :error
                                             :element-type '(unsigned-byte 8))
                       (file-position stream (mod start +wal-segment-size+))
                       (assert (= +wal-header-size+ (read-sequence header stream)))
                       (assert (= +wal-record-batch-start+ (wal-record-type (wal-decode-record header start))))
                       (wal-set-u16 header 4 +wal-version-v7+)
                       (wal-set-u32 header 28 (wal-record-crc32 header))
                       (assert (= +wal-version-v7+ (wal-record-version (wal-decode-record header start))))
                       (file-position stream (mod start +wal-segment-size+))
                       (write-sequence header stream)
                       (fsync-stream stream))
                     (call-with-logical-slot-continuation-cold-reconstruction
                      gate ownership
                      (lambda (source observed reservation check current)
                        (declare (ignore reservation))
                        (assert (equalp observation observed))
                        (multiple-value-bind (restored frozen)
                            (logical-slot-continuation-cold-prepare-registry source)
                          (declare (ignore restored))
                          (assert (equalp image (logical-slot-continuation-cold-preparation-semantic-image frozen))))
                        (let ((before (logical-slot-continuation-copy proof))
                              (after (logical-slot-continuation-copy current)))
                          (remf before :tail) (remf after :tail)
                          (assert (equalp before after)))
                        (assert (/= (getf (getf proof :tail) :digest) (getf (getf current :tail) :digest)))
                        (funcall check)
                        (setf proved t))
                      :policy t :receipt t)
                     (setf refusal
                           (handler-case
                               (progn (logical-slot-continuation-cold-preparation-call token :claim) nil)
                             (error (condition) (princ-to-string condition))))
                     (assert (and proved refusal
                                  (search "differs from its fresh closed-scope proof" refusal)))
                     (native-host-cold-recheck-refused-before-io
                      (lambda () (logical-slot-continuation-cold-preparation-call token :check))))))
                nil)
            (error (condition) (princ-to-string condition))))
    (assert (and entered proved refusal caught))
    (assert (equalp files (native-host-cold-file-image)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (sb-thread:with-mutex (*wal-lock*)
      (loop for reservation being the hash-values of *logical-slot-continuation-cold-reservations*
            do (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
               (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))))
    (format t "~&NATIVE-COLD-PREPARATION-WAL-REFUSAL PASS case=~a valid-CRC same-policy same-pending changed-transcript~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-continuation-cold-state (gate ownership)
  (handler-case
      (progn
        (when (uiop:getenv "PG_NATIVE_COLD_APPEND") (native-host-cold-append))
        (cond
          ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                   '("reconcile-after-unlink" "reconcile-after-rename" "reconcile-after-close") :test #'equal)
           (native-host-cold-reconcile-failure gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
          ((equal "reconcile-reenter" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
           (native-host-cold-reconcile-reenter gate ownership))
          ((equal "reconcile" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
           (native-host-cold-reconcile gate ownership))
          ((equal "prepare-wal" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
           (native-host-cold-preparation-wal-change gate ownership))
          ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                   '("prepare-registration" "prepare-pending" "prepare-pending-swap" "prepare-reuse"
                     "prepare-reenter" "prepare-capacity" "prepare-after-close") :test #'equal)
           (native-host-cold-preparation-refusal gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
          ((equal "prepare" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
           (native-host-cold-preparation gate ownership))
          ((member (uiop:getenv "PG_NATIVE_COLD_PROOF") '("policy-off" "policy-off-on") :test #'equal)
           (native-host-cold-policy-refusal gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
          ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                   '("pending-registration" "pending-candidate" "pending-temp-swap") :test #'equal)
           (native-host-cold-pending-refusal gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
          ((member (uiop:getenv "PG_NATIVE_COLD_PROOF") '("pending" "policy") :test #'equal)
           (native-host-cold-reconstruct-pending gate ownership))
          ((equal "selected-boundaries" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
           (native-host-cold-selected-boundaries gate ownership))
          ((equal "parent-boundaries" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
           (native-host-cold-parent-boundaries gate ownership))
          ((uiop:getenv "PG_NATIVE_COLD_FAULT")
           (native-host-cold-refusal gate ownership (uiop:getenv "PG_NATIVE_COLD_FAULT")))
          (t (native-host-cold-classification gate ownership))))
    (error (condition)
      (setf *native-cold-test-failure* condition)
      (format t "~&NATIVE-COLD-CLASSIFICATION FAILURE ~a~%" condition)
      (finish-output))))

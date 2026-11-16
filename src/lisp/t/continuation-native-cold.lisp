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
                                              (saved-check nil) (saved-source nil) (count 0))
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
       (setf saved-check check saved-source source)))
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

(defun native-host-continuation-cold-state (gate ownership)
  (handler-case
      (progn
        (when (uiop:getenv "PG_NATIVE_COLD_APPEND") (native-host-cold-append))
        (cond
          ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                   '("pending-registration" "pending-candidate" "pending-temp-swap") :test #'equal)
           (native-host-cold-pending-refusal gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
          ((equal "pending" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
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

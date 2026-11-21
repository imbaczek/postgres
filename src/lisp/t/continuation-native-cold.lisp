(in-package #:pg)

(defvar *native-cold-test-failure* nil)

(defmacro with-native-cold-spill-probe (&body body)
  (let ((flag (gensym "SPILL")) (mode (gensym "MODE")) (required (gensym "REQUIRED"))
                                (original (gensym "ORIGINAL")) (paths (gensym "PATHS")))
    `(let* ((,flag (not (null (uiop:getenv "PG_NATIVE_COLD_SPILL"))))
            (,mode (uiop:getenv "PG_NATIVE_COLD_PROOF"))
            (,required (and ,flag ,mode
                            (or (member ,mode '("prepare" "pending" "policy" "reconcile" "publish" "load")
                                        :test #'equal)
                                (eql 0 (search "publish-" ,mode))
                                (and (eql 0 (search "load-" ,mode))
                                     (not (member ,mode '("load-empty" "load-empty-late") :test #'equal)))
                                (eql 0 (search "reconcile-" ,mode)))))
            (*logical-capture-memory-bytes* (if ,flag 0 *logical-capture-memory-bytes*))
            (,original (symbol-function 'logical-spool-ensure-file))
            (,paths nil))
       (unwind-protect
            (progn
              (when ,flag
                (setf (symbol-function 'logical-spool-ensure-file)
                      (lambda (spool)
                        (multiple-value-prog1 (funcall ,original spool)
                          (let ((path (logical-spool-path spool)))
                            (when (and path (search "pg-continuation-proof." (namestring path)))
                              (assert (probe-file path))
                              (assert (not (uiop:subpathp path *pgdata*)))
                              (unless (member path ,paths :test #'equal)
                                (push path ,paths)
                                (format t "~&NATIVE-COLD-SPILL-USED transcript threshold=0~%")
                                (finish-output))))))))
              (multiple-value-prog1 (progn ,@body)
                (when (and ,flag (not *native-cold-test-failure*))
                  (when ,required (assert ,paths))
                  (when ,paths
                    (dolist (path ,paths)
                      (assert (not (probe-file path)))
                      (assert (not (probe-file (uiop:pathname-directory-pathname path)))))
                    (format t "~&NATIVE-COLD-SPILL PASS transcripts=~d threshold=0 cleanup-complete~%"
                            (length ,paths))))))
         (when ,flag (setf (symbol-function 'logical-spool-ensure-file) ,original))))))

(defun native-host-cold-append (&key commit)
  (with-wal-critical
    (wal-append-batch-start)
    (if commit
        (wal-append-record +wal-record-commit+ (getf *wal-checkpoint-snapshot* :xid-reserved-through)
                           (wal-encode-commit-timestamp 42))
        (wal-append-record +wal-record-maintenance+ 0 (make-array 0 :element-type '(unsigned-byte 8))))
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

(defun native-host-cold-with-wal (function)
  (if (sb-thread:holding-mutex-p *wal-lock*) (funcall function)
      (sb-thread:with-mutex (*wal-lock*) (funcall function))))

(defun native-host-cold-publication-kill (point)
  (when (equal point (uiop:getenv "PG_NATIVE_COLD_RECONCILE_CUT"))
    (format t "~&NATIVE-COLD-PUBLICATION-SIGKILL ~a~%" point)
    (finish-output)
    (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
    (error "cold publication SIGKILL returned")))

(defun native-host-cold-publish (gate ownership loader)
  (if loader
      (progn (wal-load-replication-slots :gate-owner gate :ownership ownership)
             *wal-replication-slots*)
      (uiop:symbol-call :pg :logical-slot-continuation-cold-publish-startup gate ownership)))

(defun native-host-cold-bootstrap-selector (snapshot prefix)
  (let ((selector (merge-pathnames "pg_logical_continuation/selector" *pgdata*)))
    (sb-posix:unlink (namestring selector))
    (when prefix
      (with-open-file (stream (merge-pathnames "pg_logical_continuation/selector.tmp" *pgdata*)
                              :direction :output :if-exists :error
                              :element-type '(unsigned-byte 8))
        (write-sequence (logical-slot-continuation-encode snapshot) stream :end 17)))))

(defun native-host-cold-publication (gate ownership &optional loader)
  (let* ((before (native-host-cold-file-image))
         (fds (native-host-writer-open-fds))
         (old-registry *wal-replication-slots*)
         (snapshot nil) (reservation nil)
         (completed nil) (candidate-identities nil) (saved-completion nil) (points nil)
         (*logical-slot-continuation-cold-publication-test-hook*
           (lambda (point completion group registry)
             (native-host-cold-publication-kill (string-downcase point))
             (push point points)
             (when (eq point :completion-ready)
               (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
               (native-host-cold-recheck-refused-before-io
                (lambda ()
                  (logical-slot-continuation-cold-completion-call (copy-structure completion) :check)))
               (let ((scopes *logical-slot-continuation-cold-completion-scopes*))
                 (assert
                  (sb-thread:join-thread
                   (sb-thread:make-thread
                    (lambda ()
                      (let ((*logical-slot-continuation-cold-completion-scopes* scopes))
                        (native-host-cold-recheck-refused-before-io
                         (lambda ()
                           (logical-slot-continuation-cold-completion-call completion :check)))
                        t)))))))
             (native-host-cold-with-wal
              (lambda ()
                (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
                (when (eq point :completion-ready)
                  (setf completed registry saved-completion completion
                        candidate-identities
                        (loop for name being the hash-keys of registry using (hash-value slot)
                              collect (list name slot (wal-replication-slot-pending-xids slot)))))
                (when group
                  (if (eq point :before-old-pin-retirement)
                      (assert (and (eq registry *wal-replication-slots*)
                                   (logical-slot-continuation-protection-call group :check)))
                      (progn
                        (assert (eq old-registry *wal-replication-slots*))
                        (assert (null (gethash registry
                                               *logical-slot-continuation-protection-registries*)))
                        (assert (handler-case
                                    (progn
                                      (funcall (logical-slot-continuation-protection-state group) :check group)
                                      nil)
                                  (error () t)))))
                  (unless (eq point :before-group-pin)
                    (assert (= 41268 (gethash group *wal-snapshot-retention-pins*)))))))))
         (*logical-slot-startup-publication-test-hook*
           (lambda (point owner token registry)
             (declare (ignore owner token))
             (native-host-cold-publication-kill (string-downcase point))
             (assert (eq completed registry))
             (native-host-cold-with-wal
              (lambda ()
                (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
                (if (member point '(:after-commit :before-owner-return))
                    (assert (eq completed *wal-replication-slots*))
                    (assert (eq old-registry *wal-replication-slots*))))))))
    (call-with-logical-slot-continuation-cold-state
     gate ownership
     (lambda (observation actual check)
       (setf snapshot (getf observation :snapshot) reservation actual)
       (funcall check)))
    (assert (= (getf snapshot :generation)
               (parse-integer (uiop:getenv "PG_NATIVE_COLD_GENERATION"))))
    (format t "~&NATIVE-COLD-PUBLICATION-EXPECTED ~a~%"
            (with-output-to-string (stream)
              (loop for byte across (logical-slot-continuation-encode snapshot)
                    do (format stream "~2,'0x" byte))))
    (finish-output)
    (when (member loader '(:bootstrap :bootstrap-prefix))
      (native-host-cold-bootstrap-selector snapshot (eq loader :bootstrap-prefix)))
    (let* ((restored (native-host-cold-publish gate ownership loader))
           (group (gethash restored *logical-slot-continuation-protection-registries*)))
      (native-host-cold-publication-kill "completed")
      (assert (and (hash-table-p restored) (eq restored *wal-replication-slots*)
                   (not (eq restored old-registry))))
      (assert (= 2 (hash-table-count restored) *logical-capture-live-slots*))
      (assert (eq completed restored))
      (dolist (row candidate-identities)
        (let ((slot (gethash (first row) restored)))
          (assert (eq slot (second row)))
          (assert (eq (third row) (wal-replication-slot-pending-xids slot)))))
      (assert (equal (reverse points)
                     '(:completion-ready :before-group-pin :after-group-pin :maps-ready
                       :before-old-pin-retirement)))
      (native-host-cold-recheck-refused-before-io
       (lambda () (logical-slot-continuation-cold-completion-call saved-completion :check)))
      (assert (zerop (hash-table-count old-registry)))
      (assert (logical-slot-continuation-protection-p group))
      (assert (null *logical-slot-continuation-cold-preparation-scopes*))
      (assert (null *logical-slot-startup-sources*))
      (assert (zerop (hash-table-count *logical-slot-startup-inventories*)))
      (sb-thread:with-mutex (*wal-lock*)
        (assert (logical-slot-continuation-protection-call group :check))
        (assert (equalp (logical-slot-continuation-encode snapshot)
                        (logical-slot-continuation-protection-call group :image)))
        (assert (= 41268 (gethash group *wal-snapshot-retention-pins*)))
        (assert (null (gethash reservation *wal-snapshot-retention-pins*)))
        (assert (logical-slot-continuation-cold-check-admission))
        (maphash
         (lambda (name slot)
           (assert (eq slot (logical-slot-continuation-check-slot slot)))
           (assert (not (wal-replication-slot-state-uncertain-p slot)))
           (assert (equalp (logical-slot-state-encode slot)
                           (second (assoc name (getf snapshot :slots) :test #'string=))))
           (let* ((confirmed (wal-replication-slot-confirmed-flush-lsn slot))
                  (pending (wal-replication-slot-pending-xids slot))
                  (expected (remove-if-not (lambda (row) (> (third row) confirmed))
                                           '((3 20598 29061) (4 29381 37853)))))
             (assert (= (length expected) (hash-table-count pending)))
             (dolist (row expected)
               (assert (equal (cons (second row) (third row)) (gethash (first row) pending))))))
         restored))
      (let ((after (native-host-cold-file-image)))
        (dolist (row after)
          (assert (not (search ".tmp" (first row))))
          (unless (search "selector" (first row))
            (assert (equalp row (assoc (first row) before :test #'string=)))))
        (assert (equalp (logical-slot-continuation-encode snapshot)
                        (third (assoc "pg_logical_continuation/selector" after :test #'string=))))))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-PUBLICATION PASS case=~a exact-selected-generation-and-pending registry-and-mirrors group-F gapless-reservation-handoff no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-publication-failure (gate ownership mode &optional loader)
  (let* ((files (native-host-cold-file-image)) (fds (native-host-writer-open-fds))
                                               (old-registry *wal-replication-slots*) (mirrors (logical-slot-publication-mirrors))
                                               (reservation nil) (snapshot nil) (group nil) (candidates nil)
                                               (hit nil) (post nil) (armed nil) (target-fd nil) (refused nil) (visits 0)
                                               (close (symbol-function 'sb-posix:close))
                                               (notify (symbol-function 'logical-slot-startup-publication-notify))
                                               (target (cdr (assoc mode
                                                                   '(("publish-after-group-pin" . :after-group-pin)
                                                                     ("publish-maps-ready" . :maps-ready)
                                                                     ("publish-derived" . :derived)
                                                                     ("publish-prepared" . :prepared)
                                                                     ("publish-before-commit" . :before-commit)
                                                                     ("publish-after-commit" . :after-commit)
                                                                     ("publish-owner-return" . :before-owner-return)
                                                                     ("publish-before-retirement" . :before-old-pin-retirement))
                                                                   :test #'equal)))
                                               (*logical-slot-continuation-cold-publication-test-hook*
                                                 (lambda (point completion actual prepared)
                                                   (declare (ignore completion))
                                                   (incf visits)
                                                   (setf candidates prepared)
                                                   (when actual (setf group actual))
                                                   (when (eq point target) (setf hit t) (error "cold staging cut ~a" mode))))
                                               (*logical-slot-startup-publication-test-hook*
                                                 (lambda (point owner token prepared)
                                                   (declare (ignore owner token))
                                                   (incf visits)
                                                   (assert (eq candidates prepared))
                                                   (when (eq point :after-commit)
                                                     (setf post t)
                                                     (cond
                                                       ((equal mode "publish-lost-registration")
                                                        (native-host-cold-with-wal
                                                         (lambda () (remhash group *logical-slot-continuation-protection-identities*)))
                                                        (setf hit t)
                                                        (error "cold lost registration after publication"))
                                                       ((equal mode "publish-close")
                                                        (let* ((expected (namestring (truename (merge-pathnames "pg_replslot/alpha/state" *pgdata*))))
                                                               (matches
                                                                 (loop for path in (native-host-writer-open-fds)
                                                                       for link = (ignore-errors (sb-posix:readlink (namestring path)))
                                                                       when (equal expected link)
                                                                         collect (parse-integer (file-namestring path)))))
                                                          (assert (= 1 (length matches)))
                                                          (setf target-fd (first matches) armed t)))))
                                                   (when (eq point target) (setf hit t) (error "cold publication cut ~a" mode)))))
    (unless (or target (member mode '("publish-close" "publish-notify" "publish-lost-registration")
                               :test #'equal))
      (error "unknown cold publication failure mode"))
    (call-with-logical-slot-continuation-cold-state
     gate ownership
     (lambda (observation actual check)
       (setf snapshot (getf observation :snapshot) reservation actual)
       (funcall check)))
    (unwind-protect
         (progn
           (when (equal mode "publish-close")
             (setf (symbol-function 'sb-posix:close)
                   (lambda (fd)
                     (let ((result (funcall close fd)))
                       (when (and armed (not hit) (eql target-fd fd))
                         (setf hit t)
                         (error "cold final close reported failure after closing its FD"))
                       result))))
           (when (equal mode "publish-notify")
             (setf (symbol-function 'logical-slot-startup-publication-notify)
                   (lambda (actual-gate actual-ownership restored)
                     (funcall notify actual-gate actual-ownership restored)
                     (setf hit t)
                     (error "cold notify cut"))))
           (setf refused
                 (handler-case
                     (progn
                       (native-host-cold-publish gate ownership loader)
                       nil)
                   (error () t))))
      (setf (symbol-function 'sb-posix:close) close
            (symbol-function 'logical-slot-startup-publication-notify) notify))
    (assert (and hit refused group candidates))
    (native-host-cold-with-wal
     (lambda ()
       (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
       (assert (= 41268 (gethash group *wal-snapshot-retention-pins*)))
       (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
       (assert (funcall (logical-slot-continuation-protection-state group) :uncertain group))
       (assert (loop for slot being the hash-values of candidates
                     always (wal-replication-slot-state-uncertain-p slot)))
       (if post
           (assert (eq candidates *wal-replication-slots*))
           (progn
             (assert (eq old-registry *wal-replication-slots*))
             (assert (zerop (hash-table-count old-registry)))
             (assert (equal mirrors (logical-slot-publication-mirrors)))))
       (assert (handler-case
                   (progn (logical-slot-continuation-protection-call group :check) nil)
                 (error () t)))))
    (let ((after (native-host-cold-file-image)) (before-retry visits))
      (dolist (row after)
        (assert (not (search ".tmp" (first row))))
        (unless (search "selector" (first row))
          (assert (equalp row (assoc (first row) files :test #'string=)))))
      (assert (equalp (logical-slot-continuation-encode snapshot)
                      (third (assoc "pg_logical_continuation/selector" after :test #'string=))))
      (assert (handler-case
                  (progn
                    (native-host-cold-publish gate ownership loader)
                    nil)
                (error () t)))
      (assert (= visits before-retry))
      (assert (equalp after (native-host-cold-file-image))))
    (assert (equal fds (native-host-writer-open-fds)))
    (format t "~&NATIVE-COLD-PUBLICATION-REFUSAL PASS case=~a mode=~a published=~s both-F-pins uncertain-group-and-members retry-fenced no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE") mode post)))

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
                                              (entered nil) (proved nil) (refusal nil) (rechecked nil) (caught nil))
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
                          (commit-start (+ start +wal-header-size+)))
                     (assert (< commit-start (getf observation :horizon)))
                     (multiple-value-bind (segment offset) (floor commit-start +wal-segment-size+)
                       (with-open-file (stream (wal-segment-path segment)
                                               :direction :io :if-exists :overwrite :if-does-not-exist :error
                                               :element-type '(unsigned-byte 8))
                         (let* ((record (wal-lineage-read-record stream segment offset (file-length stream)))
                                (timestamp (wal-decode-commit-timestamp (wal-record-payload record))))
                           (assert (= +wal-record-commit+ (wal-record-type record)))
                           (let ((bytes (wal-encode-record +wal-record-commit+ (wal-record-xid record) commit-start
                                                           (wal-encode-commit-timestamp (logxor 1 timestamp)))))
                             (assert (= (+ commit-start (length bytes)) (wal-record-end record)))
                             (file-position stream offset)
                             (write-sequence bytes stream)
                             (fsync-stream stream)))))
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
                      (lambda () (logical-slot-continuation-cold-preparation-call token :check)))
                     (setf rechecked t))))
                nil)
            (error (condition) (princ-to-string condition))))
    (assert (and entered proved rechecked caught) ()
            "WAL mutation proof did not reach claim refusal (entered ~s, proved ~s): ~a"
            entered proved (or refusal caught))
    (assert (equalp files (native-host-cold-file-image)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (sb-thread:with-mutex (*wal-lock*)
      (loop for reservation being the hash-values of *logical-slot-continuation-cold-reservations*
            do (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
               (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))))
    (format t "~&NATIVE-COLD-PREPARATION-WAL-REFUSAL PASS case=~a valid-CRC same-policy same-pending changed-transcript~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-add-extra (staging &optional reserved (system-id 5339102419085599071))
  (let ((image (wal-encode-replication-slot
                (make-wal-replication-slot
                 :name "extra" :kind :physical :system-id system-id
                 :database-oid nil :plugin nil :consistent-lsn nil
                 :confirmed-flush-lsn nil :restart-lsn (when reserved (1- *wal-durable-lsn*))
                 :restart-timeline (when reserved *wal-insert-timeline*) :inactive-since 0
                 :wal-status (if reserved :reserved :lost)))))
    (slot-state-create (wal-replication-slot-directory) "extra" image)
    (when staging
      (with-open-file (stream (merge-pathnames "pg_replslot/extra/state.tmp" *pgdata*)
                              :direction :output :if-exists :error
                              :element-type '(unsigned-byte 8))
        (write-sequence image stream :end 1)))))

(defun native-host-cold-extra-files ()
  (append (native-host-cold-file-image)
          (loop for name in '("pg_replslot/extra/state" "pg_replslot/extra/state.tmp")
                for path = (merge-pathnames name *pgdata*)
                when (slot-state-lstat path)
                  collect (list name (physical-image-file-facts -100 (namestring path))
                                (read-bounded-regular-file path +slot-state-maximum-bytes+)))))

(defun native-host-cold-mixed-seal ()
  (let ((selected (native-host-writer-selected))
        (physical (read-bounded-regular-file
                   (merge-pathnames "pg_replslot/extra/state" *pgdata*)
                   +slot-state-maximum-bytes+)))
    (format t "~&NATIVE-COLD-MIXED-PUBLICATION-EXPECTED ~a:~a~%"
            (with-output-to-string (stream)
              (loop for byte across (logical-slot-continuation-encode selected)
                    do (format stream "~2,'0x" byte)))
            (with-output-to-string (stream)
              (loop for byte across physical do (format stream "~2,'0x" byte))))
    (finish-output)))

(defun native-host-cold-mixed-restart (gate ownership)
  (let ((before (native-host-cold-extra-files))
        (fds (native-host-writer-open-fds)))
    (wal-load-replication-slots :gate-owner gate :ownership ownership)
    (catalog-init)
    (assert (= 3 (hash-table-count *wal-replication-slots*)))
    (assert (= 2 *logical-capture-live-slots*))
    (let* ((beta (gethash "beta" *wal-replication-slots*))
           (physical (gethash "extra" *wal-replication-slots*))
           (group (gethash *wal-replication-slots*
                           *logical-slot-continuation-protection-registries*)))
      (assert (= 37853 (wal-replication-slot-confirmed-flush-lsn beta)))
      (assert (zerop (hash-table-count (wal-replication-slot-pending-xids beta))))
      (assert (wal-physical-slot-p physical))
      (assert (= *wal-durable-lsn* (wal-replication-slot-restart-lsn physical)))
      (call-with-logical-replication-slot
       "beta" (lambda (owned) (assert (eq beta owned))))
      (native-host-cold-with-wal
       (lambda () (assert (logical-slot-continuation-protection-call group :check)))))
    (assert (equalp before (native-host-cold-extra-files)))
    (assert (equal fds (native-host-writer-open-fds)))
    (native-host-cold-mixed-seal)
    (format t "~&NATIVE-COLD-MIXED-PUBLICATION PASS case=~a fresh-restart exact-saved-inventory~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-logical-hex (bytes)
  (with-output-to-string (stream)
    (loop for byte across bytes do (format stream "~2,'0x" byte))))

(defun native-host-cold-logical-pending (slot)
  (with-output-to-string (stream)
    (dolist (xid (sort (loop for xid being the hash-keys of
                                                         (wal-replication-slot-pending-xids slot) collect xid) #'<))
      (let ((entry (gethash xid (wal-replication-slot-pending-xids slot))))
        (format stream "~d/~d/~d;" xid (car entry) (cdr entry))))))

(defun native-host-cold-logical-compatibility (empty)
  (let* ((old (gethash "ordinary" *wal-replication-slots*))
         (unbound (member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                          '("load-unbound-logical" "load-noncanonical-v4") :test #'equal))
         (slot (make-wal-replication-slot
                :name "ordinary" :kind :logical
                :database-oid (unless unbound (wal-replication-slot-database-oid old))
                :plugin (if unbound "pg_lisp" (wal-replication-slot-plugin old))
                :restart-lsn (if empty *wal-durable-lsn* (wal-replication-slot-restart-lsn old))
                :consistent-lsn (wal-replication-slot-consistent-lsn old)
                :confirmed-flush-lsn (unless unbound
                                       (if empty *wal-durable-lsn*
                                           (wal-replication-slot-confirmed-flush-lsn old)))
                :invalidation-cause :none :wal-status :reserved)))
    (slot-state-update
     (wal-replication-slot-directory) "ordinary"
     (if (equal "load-noncanonical-v4" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
         (let ((prefix (format nil "PGSLOT4 ordinary - 0~d ~d - reserved pg_lisp"
                               (wal-replication-slot-restart-lsn slot)
                               (wal-replication-slot-consistent-lsn slot))))
           (string->bytes (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix)))))
         (wal-encode-replication-slot slot)))
    (setf (gethash "ordinary" *wal-replication-slots*) slot)
    (format t "~&NATIVE-COLD-LOGICAL-COMPATIBILITY provenance-free-codec-fixture mode=~a empty=~s~%"
            (uiop:getenv "PG_NATIVE_COLD_PROOF") empty)))

(defun native-host-cold-logical-seed (gate ownership)
  (wal-load-replication-slots :gate-owner gate :ownership ownership)
  (catalog-init)
  (let ((*backend* (make-backend)))
    (register-backend *backend*)
    (unwind-protect
         (progn
           (uiop:with-temporary-file (:stream output :direction :io :element-type '(unsigned-byte 8))
             (assert (nth-value 1 (exec-simple-query output "CREATE TABLE continuation_ordinary (id int)"))))
           (create-standard-logical-slot "ordinary" :nothing)
           (when (member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                         '("load-legacy-logical" "load-unbound-logical" "load-noncanonical-v4") :test #'equal)
             (native-host-cold-logical-compatibility nil))
           (uiop:with-temporary-file (:stream output :direction :io :element-type '(unsigned-byte 8))
             (assert (nth-value 1 (exec-simple-query output "INSERT INTO continuation_ordinary VALUES (117)"))))
           (when (equal "load-legacy-empty" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
             (native-host-cold-logical-compatibility t))
           (let ((slot (gethash "ordinary" *wal-replication-slots*)))
             (assert (eq (not (null (member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                                            '("load-logical" "load-empty-logical") :test #'equal)))
                         (not (null (logical-slot-startup-known-p slot)))))
             (assert (null (wal-replication-slot-child-owner slot)))
             (assert (= (if (equal "load-legacy-empty" (uiop:getenv "PG_NATIVE_COLD_PROOF")) 0 1)
                        (hash-table-count (wal-replication-slot-pending-xids slot))))
             (format t "~&NATIVE-COLD-LOGICAL-SEED-EXPECTED ~a ~a~%"
                     (native-host-cold-logical-hex (wal-encode-replication-slot slot))
                     (native-host-cold-logical-pending slot))))
      (unwind-protect (end-backend-session) (unregister-backend *backend*))))
  (format t "~&NATIVE-COLD-LOGICAL-SEED PASS case=~a real-creation real-insert~%"
          (uiop:getenv "PG_NATIVE_COLD_CASE")))

(defun native-host-cold-logical-proof-probes (name target)
  (let ((saved nil) (rows nil))
    (call-with-logical-slot-continuation-native-proof
     name target
     (lambda (proof)
       (setf saved proof)
       (sb-thread:with-mutex (*wal-lock*)
         (setf rows
               (logical-slot-continuation-read-slot-inventory
                (logical-slot-continuation-native-proof-slot-root proof)
                (lambda () (logical-slot-continuation-native-proof-check-under-lock proof))))
         (assert (= 3 (length rows)))
         (assert (= 2 (length (logical-slot-continuation-native-proof-partition-under-lock proof rows))))
         (assert (handler-case
                     (progn
                       (logical-slot-continuation-native-proof-partition-under-lock
                        (copy-logical-slot-continuation-native-proof proof) rows)
                       nil)
                   (error () t)))
         (assert (eq proof (logical-slot-continuation-native-proof-check-under-lock proof))))))
    (native-host-cold-recheck-refused-before-io
     (lambda ()
       (sb-thread:with-mutex (*wal-lock*)
         (logical-slot-continuation-native-proof-partition-under-lock saved rows))))))

(defun native-host-cold-logical-noop-preflight (beta)
  (let* ((path (wal-replication-slot-path "ordinary"))
         (facts (physical-image-file-facts -100 (namestring path)))
         (bytes (read-bounded-regular-file path +slot-state-maximum-bytes+))
         (selected (native-host-cold-file-image)))
    (when (equal "load-noncanonical-v4" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
      (assert (string= "PGSLOT4" (bytes->string (subseq bytes 0 7))))
      (assert (not (equalp bytes
                           (wal-encode-replication-slot (gethash "ordinary" *wal-replication-slots*))))))
    (call-with-logical-slot-continuation-native-proof
     "beta" (wal-replication-slot-confirmed-flush-lsn beta)
     (lambda (proof)
       (sb-thread:with-mutex (*wal-lock*)
         (multiple-value-bind (record candidate ledger)
             (logical-slot-continuation-native-preflight-under-lock proof)
           (assert (null record))
           (assert (eq beta candidate))
           (assert (= 3 (length ledger)))))))
    (assert (equalp selected (native-host-cold-file-image)))
    (assert (equal facts (physical-image-file-facts -100 (namestring path))))
    (assert (equalp bytes (read-bounded-regular-file path +slot-state-maximum-bytes+)))))

(defun native-host-cold-logical-ordinary-operation (ordinary)
  (if (wal-replication-slot-database-oid ordinary)
      (call-with-logical-replication-slot
       "ordinary"
       (lambda (owned)
         (assert (eq ordinary owned))
         (unless (wal-replication-slot-confirmed-flush-lsn ordinary)
           (wal-finalize-replication-slot-copy
            "ordinary" (wal-replication-slot-consistent-lsn ordinary) *wal-logical-slot-owner*))
         (assert (eq ordinary
                     (wal-acknowledge-replication-slot
                      "ordinary" *wal-durable-lsn* *wal-logical-slot-owner*))))
       :allow-initial-copy t)
      (assert (handler-case
                  (progn (call-with-logical-replication-slot "ordinary" (lambda (slot)
                                                                          (declare (ignore slot)))) nil)
                (pg-error (condition)
                  (assert (string= "55000" (pg-error-sqlstate condition)))
                  t)))))

(defun native-host-cold-logical-advance (ordinary beta)
  (assert (string= (uiop:getenv "PG_NATIVE_COLD_LOGICAL_SEED_IMAGE")
                   (native-host-cold-logical-hex (wal-encode-replication-slot ordinary))))
  (assert (string= (uiop:getenv "PG_NATIVE_COLD_LOGICAL_SEED_PENDING")
                   (native-host-cold-logical-pending ordinary)))
  (assert (= (if (equal "load-legacy-empty" (uiop:getenv "PG_NATIVE_COLD_PROOF")) 0 1)
             (hash-table-count (wal-replication-slot-pending-xids ordinary))))
  (native-host-cold-logical-noop-preflight beta)
  (let ((before (native-host-cold-file-image)))
    (native-host-cold-logical-ordinary-operation ordinary)
    (assert (equalp before (native-host-cold-file-image))))
  (native-host-cold-logical-proof-probes "beta" 37853)
  (let* ((path (wal-replication-slot-path "ordinary"))
         (before (physical-image-file-facts -100 (namestring path)))
         (bytes (read-bounded-regular-file path +slot-state-maximum-bytes+))
         (generation (getf (native-host-writer-selected) :generation)))
    (call-with-logical-replication-slot
     "beta"
     (lambda (owned)
       (assert (eq beta owned))
       (logical-slot-continuation-native-acknowledge "beta" 37853 *wal-logical-slot-owner*)))
    (assert (equal before (physical-image-file-facts -100 (namestring path))))
    (assert (equalp bytes (read-bounded-regular-file path +slot-state-maximum-bytes+)))
    (assert (= (1+ generation) (getf (native-host-writer-selected) :generation)))))

(defun native-host-cold-logical-publication (gate ownership)
  (when (equal "seed" (uiop:getenv "PG_NATIVE_COLD_LOGICAL_PHASE"))
    (return-from native-host-cold-logical-publication
      (native-host-cold-logical-seed gate ownership)))
  (let ((fds (native-host-writer-open-fds)))
    (wal-load-replication-slots :gate-owner gate :ownership ownership)
    (catalog-init)
    (assert (= 3 (hash-table-count *wal-replication-slots*)))
    (assert (= 3 *logical-capture-live-slots*))
    (let* ((ordinary (gethash "ordinary" *wal-replication-slots*))
           (beta (gethash "beta" *wal-replication-slots*))
           (group (gethash *wal-replication-slots*
                           *logical-slot-continuation-protection-registries*)))
      (assert group)
      (assert (eq (equal "load-logical" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
                  (not (null (logical-slot-startup-known-p ordinary)))))
      (assert (null (wal-replication-slot-child-owner ordinary)))
      (when (equal "advance" (uiop:getenv "PG_NATIVE_COLD_LOGICAL_PHASE"))
        (native-host-cold-logical-advance ordinary beta))
      (if (wal-replication-slot-database-oid ordinary)
          (progn
            (assert (= *wal-durable-lsn* (wal-replication-slot-confirmed-flush-lsn ordinary)))
            (assert (zerop (hash-table-count (wal-replication-slot-pending-xids ordinary)))))
          (progn
            (assert (null (wal-replication-slot-confirmed-flush-lsn ordinary)))
            (assert (= 1 (hash-table-count (wal-replication-slot-pending-xids ordinary))))))
      (assert (= 37853 (wal-replication-slot-confirmed-flush-lsn beta)))
      (native-host-cold-with-wal
       (lambda () (assert (logical-slot-continuation-protection-call group :check))))
      (assert (eq ordinary (gethash "ordinary" *wal-replication-slots*)))
      (format t "~&NATIVE-COLD-LOGICAL-EXPECTED ~a:~a:~a:~a~%"
              (native-host-cold-logical-hex
               (logical-slot-continuation-encode (native-host-writer-selected)))
              (native-host-cold-logical-hex
               (read-bounded-regular-file (wal-replication-slot-path "ordinary")
                                          +slot-state-maximum-bytes+))
              (native-host-cold-logical-pending beta)
              (native-host-cold-logical-pending ordinary)))
    (assert (equal fds (native-host-writer-open-fds)))
    (format t "~&NATIVE-COLD-LOGICAL PASS case=~a ordinary-state child-ack exact-full-inventory~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-with-temporary-physical (function)
  (if (not (uiop:getenv "PG_NATIVE_COLD_TEMPORARY"))
      (funcall function)
      (let ((*backend* (make-backend
                        :pid (sb-thread:thread-os-tid sb-thread:*current-thread*)
                        :thread sb-thread:*current-thread*)))
        (physical-slot-check-live-admission)
        (register-backend *backend*)
        (unwind-protect
             (let* ((slot (wal-create-physical-replication-slot
                           "native_temporary" :temporary t :immediately-reserve t))
                    (capability (wal-replication-slot-temporary-lifetime slot)))
               (assert (physical-slot-lifetime-p capability))
               (wal-advance-physical-replication-slot "native_temporary" *wal-durable-lsn*)
               (let ((image (physical-slot-encode slot)))
                 (funcall function)
                 (native-host-cold-with-wal
                  (lambda ()
                    (assert (eq slot (physical-slot-lifetime-check-under-lock slot)))
                    (assert (eq capability (wal-replication-slot-temporary-lifetime slot)))
                    (assert (equalp image (physical-slot-encode slot)))))
                 (wal-drop-replication-slot "native_temporary" slot)
                 (assert (null (gethash "native_temporary" *wal-replication-slots*)))
                 (format t "~&NATIVE-COLD-TEMPORARY PASS issued-lifetime child-ack owner-drop~%")))
          (unregister-backend *backend*)))))

(defun native-host-cold-mixed-publication (gate ownership)
  (when (uiop:getenv "PG_NATIVE_COLD_MIXED_RESTART")
    (return-from native-host-cold-mixed-publication
      (native-host-cold-mixed-restart gate ownership)))
  (native-host-cold-add-extra nil t)
  (let* ((fds (native-host-writer-open-fds)) (snapshot nil) (reservation nil)
                                             (candidates nil)
                                             (*logical-slot-continuation-cold-publication-test-hook*
                                               (lambda (point completion group registry)
                                                 (declare (ignore completion group))
                                                 (when (eq point :completion-ready)
                                                   (setf candidates
                                                         (loop for name being the hash-keys of registry using (hash-value slot)
                                                               collect (list name slot (wal-replication-slot-pending-xids slot))))))))
    (call-with-logical-slot-continuation-cold-state
     gate ownership
     (lambda (observation actual check)
       (assert (= 3 (length (getf observation :rows))))
       (setf snapshot (getf observation :snapshot) reservation actual)
       (assert (= 2 (length (getf snapshot :slots))))
       (funcall check)))
    (let ((before (native-host-cold-extra-files)))
      (wal-load-replication-slots :gate-owner gate :ownership ownership)
      (catalog-init)
      (let* ((registry *wal-replication-slots*)
             (group (gethash registry *logical-slot-continuation-protection-registries*))
             (physical (gethash "extra" registry))
             (beta (gethash "beta" registry)))
        (assert (= 3 (hash-table-count registry)))
        (assert (= 2 *logical-capture-live-slots*))
        (assert (and group (wal-physical-slot-p physical)))
        (assert (plusp (wal-replication-slot-inactive-since physical)))
        (dolist (row candidates)
          (assert (eq (second row) (gethash (first row) registry)))
          (assert (eq (third row) (wal-replication-slot-pending-xids (second row)))))
        (assert (= 3 (length candidates)))
        (native-host-cold-with-wal
         (lambda ()
           (assert (logical-slot-continuation-protection-call group :check))
           (assert (eq physical (logical-slot-continuation-check-slot physical t)))
           (assert (= 41268 (gethash group *wal-snapshot-retention-pins*)))
           (assert (null (gethash reservation *wal-snapshot-retention-pins*)))))
        (assert (= *wal-durable-lsn*
                   (wal-advance-physical-replication-slot "extra" *wal-durable-lsn*)))
        (let ((after (assoc "pg_replslot/extra/state" (native-host-cold-extra-files)
                            :test #'string=)))
          (assert (not (equalp after (assoc "pg_replslot/extra/state" before :test #'string=))))
          (setf before (cons after (remove "pg_replslot/extra/state" before
                                           :key #'first :test #'string=))))
        (native-host-cold-with-temporary-physical
         (lambda ()
           (call-with-logical-replication-slot
            "beta"
            (lambda (owned)
              (assert (eq beta owned))
              (assert (< (wal-replication-slot-confirmed-flush-lsn owned) 37853))
              (assert (eq beta (logical-slot-continuation-native-acknowledge
                                "beta" 37853 *wal-logical-slot-owner*)))))))
        (assert (= 37853 (wal-replication-slot-confirmed-flush-lsn beta)))
        (assert (zerop (hash-table-count (wal-replication-slot-pending-xids beta))))
        (assert (eq physical (gethash "extra" registry)))
        (assert (equalp (assoc "pg_replslot/extra/state" before :test #'string=)
                        (assoc "pg_replslot/extra/state" (native-host-cold-extra-files)
                               :test #'string=)))
        (let ((selected (native-host-writer-selected)))
          (assert (= (1+ (getf snapshot :generation)) (getf selected :generation)))
          (assert (= 2 (length (getf selected :slots)))))
        (native-host-cold-with-wal
         (lambda () (assert (logical-slot-continuation-protection-call group :check))))))
    (assert (equal fds (native-host-writer-open-fds)))
    (native-host-cold-mixed-seal)
    (format t "~&NATIVE-COLD-MIXED-PUBLICATION PASS case=~a complete-registry child-ack physical-preserved no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-mixed-loader-refusal (gate ownership staging)
  (let ((fds (native-host-writer-open-fds)) (registry *wal-replication-slots*)
                                            (mirrors (logical-slot-publication-mirrors)) (reservation nil))
    (call-with-logical-slot-continuation-cold-state
     gate ownership
     (lambda (observation actual check)
       (declare (ignore observation))
       (setf reservation actual)
       (funcall check)))
    (native-host-cold-add-extra staging nil
                                (if staging 5339102419085599071 5339102419085599072))
    (let ((before (native-host-cold-extra-files)))
      (assert (handler-case
                  (progn (wal-load-replication-slots :gate-owner gate :ownership ownership) nil)
                (error () t)))
      (assert (equalp before (native-host-cold-extra-files)))
      (assert (eq registry *wal-replication-slots*))
      (assert (zerop (hash-table-count registry)))
      (assert (equal mirrors (logical-slot-publication-mirrors)))
      (native-host-cold-with-wal
       (lambda ()
         (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
         (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain)))))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-LOADER-REFUSAL PASS case=~a mixed-inventory-preserved staging=~s empty-registry retained-F no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE") staging)))

(defun native-host-cold-empty-late-loader (gate ownership)
  (let ((fds (native-host-writer-open-fds)) (registry *wal-replication-slots*)
                                            (mirrors (logical-slot-publication-mirrors)) (hit nil) (before nil) (refused nil)
                                            (restore (symbol-function 'logical-slot-continuation-restore-empty-startup)))
    (unwind-protect
         (progn
           (setf (symbol-function 'logical-slot-continuation-restore-empty-startup)
                 (lambda (actual-gate actual-ownership)
                   (let ((result (funcall restore actual-gate actual-ownership)))
                     (assert (= 1 (getf result :generation)))
                     (assert (null (getf result :slots)))
                     (native-host-cold-add-extra t)
                     (setf before (native-host-cold-extra-files) hit t)
                     result)))
           (setf refused
                 (handler-case
                     (progn (wal-load-replication-slots :gate-owner gate :ownership ownership) nil)
                   (error () t))))
      (setf (symbol-function 'logical-slot-continuation-restore-empty-startup) restore))
    (assert hit)
    (assert refused)
    (assert (equalp before (native-host-cold-extra-files)))
    (assert (eq registry *wal-replication-slots*))
    (assert (zerop (hash-table-count registry)))
    (assert (equal mirrors (logical-slot-publication-mirrors)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-EMPTY-LOADER-REFUSAL PASS case=~a late-physical-and-staging-preserved no-adoption no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-empty-token-probes (token)
  (assert (functionp (logical-slot-continuation-cold-empty-handoff-state token)))
  (assert (>= (length *logical-slot-imported-host-startup-owners*) 2))
  (let ((invoked nil))
    (native-host-cold-recheck-refused-before-io
     (lambda ()
       (logical-slot-continuation-cold-empty-call
        (%make-logical-slot-continuation-cold-empty-handoff
         :state (lambda (&rest arguments) (declare (ignore arguments)) (setf invoked t)))
        :check)))
    (assert (not invoked)))
  (sb-thread:with-mutex (*wal-lock*)
    (native-host-cold-recheck-refused-before-io
     (lambda ()
       (logical-slot-continuation-cold-empty-call
        (copy-logical-slot-continuation-cold-empty-handoff token) :check)))
    (let ((copy (copy-logical-slot-continuation-cold-empty-handoff token)))
      (native-host-cold-recheck-refused-before-io
       (lambda ()
         (funcall (logical-slot-continuation-cold-empty-handoff-state copy) :check copy nil)))))
  (assert
   (eq :passed
       (sb-thread:join-thread
        (sb-thread:make-thread
         (lambda ()
           (sb-thread:with-mutex (*wal-lock*)
             (native-host-cold-recheck-refused-before-io
              (lambda () (logical-slot-continuation-cold-empty-call token :check)))
             :passed))))))
  (sb-thread:with-mutex (*wal-lock*)
    (assert (logical-slot-continuation-cold-empty-call token :check)))
  t)

(defun native-host-cold-empty-ordinary-slot (&optional keep)
  (catalog-init)
  (let ((*backend* (make-backend :pid (sb-thread:thread-os-tid sb-thread:*current-thread*)
                                 :thread sb-thread:*current-thread*)))
    (physical-slot-check-live-admission)
    (register-backend *backend*)
    (unwind-protect
         (let ((slot (wal-create-physical-replication-slot "empty_ordinary" :immediately-reserve t)))
           (assert (eq slot (gethash "empty_ordinary" *wal-replication-slots*)))
           (assert (slot-state-lstat
                    (merge-pathnames "empty_ordinary/state" (wal-replication-slot-directory))))
           (wal-advance-physical-replication-slot "empty_ordinary" *wal-durable-lsn*)
           (unless keep
             (wal-drop-replication-slot "empty_ordinary" slot)
             (assert (zerop (hash-table-count *wal-replication-slots*)))))
      (unregister-backend *backend*)))
  (format t "~&NATIVE-COLD-EMPTY-ADMISSION PASS ordinary-slot-created ~a~%"
          (if keep "kept-for-restart" "and-dropped")))

(defun native-host-cold-empty-loader (gate ownership &optional late keep)
  (let* ((fds (native-host-writer-open-fds)) (workspace-facts nil)
                                             (selector (merge-pathnames "pg_logical_continuation/selector" *pgdata*))
                                             (workspace (merge-pathnames "pg_replslot/.logical-slot-copy.work" *pgdata*))
                                             (tokens nil)
                                             (outer-hook *logical-slot-continuation-cold-empty-test-hook*)
                                             (*logical-slot-continuation-cold-empty-test-hook*
                                               (lambda (point token reservation)
                                                 (format t "~&NATIVE-COLD-EMPTY-HANDOFF point=~s~%" point)
                                                 (finish-output)
                                                 (when (eq point :minted)
                                                   (push (cons token reservation) tokens)
                                                   (native-host-cold-empty-token-probes token))
                                                 (when outer-hook (funcall outer-hook point token reservation))))
                                             (*logical-slot-continuation-cold-publication-test-hook*
                                               (lambda (&rest ignored)
                                                 (declare (ignore ignored))
                                                 (error "empty loader entered cold publication"))))
    (dolist (name '("alpha/" "beta/"))
      (uiop:delete-directory-tree (merge-pathnames name (wal-replication-slot-directory))
                                  :validate t :if-does-not-exist :error))
    (sb-posix:unlink (namestring selector))
    (unless (slot-state-lstat workspace)
      (with-open-file (stream workspace :direction :output :if-exists :error)
        (write-string "opaque local workspace" stream)))
    (setf workspace-facts (physical-image-file-facts -100 (namestring workspace)))
    (when late
      (native-host-cold-empty-late-loader gate ownership)
      (assert (equal workspace-facts (physical-image-file-facts -100 (namestring workspace))))
      (return-from native-host-cold-empty-loader t))
    (dotimes (iteration 2)
      (let ((before (when (plusp iteration) (native-host-cold-file-image))))
        (let ((pins (hash-table-count *wal-snapshot-retention-pins*)))
          (wal-load-replication-slots :gate-owner gate :ownership ownership)
          (assert (= pins (hash-table-count *wal-snapshot-retention-pins*))))
        (sb-thread:with-mutex (*wal-lock*)
          (assert (logical-slot-continuation-cold-check-admission))
          (assert (zerop (hash-table-count *logical-slot-continuation-cold-reservations*)))
          (assert (zerop (hash-table-count *logical-slot-continuation-cold-registries*)))
          (wal-check-replication-slot-capacity)
          (dolist (binding tokens)
            (native-host-cold-recheck-refused-before-io
             (lambda () (logical-slot-continuation-cold-empty-call (car binding) :check)))
            (native-host-cold-recheck-refused-before-io
             (lambda () (logical-slot-continuation-cold-reservation-call (cdr binding) :check)))))
        (assert (zerop (hash-table-count *wal-replication-slots*)))
        (assert (zerop *logical-capture-live-slots*))
        (assert (null (gethash *wal-replication-slots*
                               *logical-slot-continuation-protection-registries*)))
        (assert (equal workspace-facts (physical-image-file-facts -100 (namestring workspace))))
        (when before (assert (equalp before (native-host-cold-file-image))))
        (let ((snapshot
                (logical-slot-continuation-decode
                 (read-bounded-regular-file
                  selector (logical-slot-continuation-prepared-maximum-bytes)))))
          (assert (= 1 (getf snapshot :generation)))
          (assert (null (getf snapshot :slots))))))
    (native-host-cold-empty-ordinary-slot keep)
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-EMPTY-LOADER PASS case=~a bootstrap restart exact-empty-selector opaque-workspace no-cold-publication no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-empty-ordinary-restart (gate ownership)
  (let ((fds (native-host-writer-open-fds))
        (pins (hash-table-count *wal-snapshot-retention-pins*)))
    (if (uiop:getenv "PG_NATIVE_COLD_EMPTY_RESTART")
        (wal-load-replication-slots :gate-owner gate :ownership ownership)
        (native-host-cold-empty-loader gate ownership nil t))
    (assert (= pins (hash-table-count *wal-snapshot-retention-pins*)))
    (assert (= 1 (hash-table-count *wal-replication-slots*)))
    (let ((slot (gethash "empty_ordinary" *wal-replication-slots*)))
      (assert slot)
      (assert (null (wal-replication-slot-database-oid slot)))
      (assert (null (wal-replication-slot-child-owner slot)))
      (let* ((bytes (read-bounded-regular-file
                     (wal-replication-slot-path "empty_ordinary") +slot-state-maximum-bytes+))
             (saved (physical-slot-decode bytes "empty_ordinary"))
             (copy (copy-wal-replication-slot slot)))
        (assert (plusp (wal-replication-slot-inactive-since slot)))
        (setf (wal-replication-slot-inactive-since copy)
              (wal-replication-slot-inactive-since saved))
        (assert (equalp bytes (physical-slot-encode copy)))
        (format t "~&NATIVE-COLD-EMPTY-ORDINARY-EXPECTED ~a~%"
                (native-host-cold-logical-hex bytes))))
    (native-host-cold-with-wal
     (lambda () (assert (logical-slot-continuation-cold-check-admission))))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-EMPTY-ORDINARY PASS case=~a persistent-slot no-cold-group no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-empty-logical-restart (gate ownership)
  (when (equal "seed" (uiop:getenv "PG_NATIVE_COLD_LOGICAL_PHASE"))
    (native-host-cold-empty-loader gate ownership)
    (return-from native-host-cold-empty-logical-restart
      (native-host-cold-logical-seed gate ownership)))
  (let ((fds (native-host-writer-open-fds))
        (pins (hash-table-count *wal-snapshot-retention-pins*)))
    (wal-load-replication-slots :gate-owner gate :ownership ownership)
    (catalog-init)
    (assert (= 1 (hash-table-count *wal-replication-slots*)))
    (assert (= 1 *logical-capture-live-slots*))
    (assert (= pins (hash-table-count *wal-snapshot-retention-pins*)))
    (let ((ordinary (gethash "ordinary" *wal-replication-slots*)))
      (assert (logical-slot-startup-known-p ordinary))
      (assert (null (wal-replication-slot-child-owner ordinary)))
      (when (equal "advance" (uiop:getenv "PG_NATIVE_COLD_LOGICAL_PHASE"))
        (assert (string= (uiop:getenv "PG_NATIVE_COLD_LOGICAL_SEED_IMAGE")
                         (native-host-cold-logical-hex (wal-encode-replication-slot ordinary))))
        (assert (string= (uiop:getenv "PG_NATIVE_COLD_LOGICAL_SEED_PENDING")
                         (native-host-cold-logical-pending ordinary)))
        (assert (= 1 (hash-table-count (wal-replication-slot-pending-xids ordinary))))
        (native-host-cold-logical-ordinary-operation ordinary))
      (assert (= *wal-durable-lsn* (wal-replication-slot-confirmed-flush-lsn ordinary)))
      (assert (zerop (hash-table-count (wal-replication-slot-pending-xids ordinary))))
      (assert (null (gethash *wal-replication-slots*
                             *logical-slot-continuation-protection-registries*)))
      (native-host-cold-with-wal
       (lambda () (assert (logical-slot-continuation-cold-check-admission))))
      (let* ((selector-path (merge-pathnames "pg_logical_continuation/selector" *pgdata*))
             (selector (read-bounded-regular-file
                        selector-path (logical-slot-continuation-prepared-maximum-bytes))))
        (assert (null (getf (logical-slot-continuation-decode selector) :slots)))
        (format t "~&NATIVE-COLD-LOGICAL-EXPECTED ~a:~a::~a~%"
                (native-host-cold-logical-hex selector)
                (native-host-cold-logical-hex
                 (read-bounded-regular-file
                  (wal-replication-slot-path "ordinary") +slot-state-maximum-bytes+))
                (native-host-cold-logical-pending ordinary))))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-LOGICAL PASS case=~a ordinary-only reconstructed-pending acknowledged no-cold-group no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-descendant-empty (gate ownership)
  (let ((fds (native-host-writer-open-fds))
        (pins (hash-table-count *wal-snapshot-retention-pins*))
        (selector (merge-pathnames "pg_logical_continuation/selector" *pgdata*)))
    (assert (= 3 *wal-insert-timeline*))
    (dotimes (iteration 2)
      (let ((before (when (probe-file selector)
                      (read-bounded-regular-file
                       selector (logical-slot-continuation-prepared-maximum-bytes)))))
        (wal-load-replication-slots :gate-owner gate :ownership ownership)
        (assert (zerop (hash-table-count *wal-replication-slots*)))
        (assert (zerop *logical-capture-live-slots*))
        (assert (= pins (hash-table-count *wal-snapshot-retention-pins*)))
        (assert (zerop (hash-table-count *logical-slot-continuation-cold-reservations*)))
        (assert (zerop (hash-table-count *logical-slot-continuation-cold-registries*)))
        (let* ((after (read-bounded-regular-file
                       selector (logical-slot-continuation-prepared-maximum-bytes)))
               (snapshot (logical-slot-continuation-decode after)))
          (when before (assert (equalp before after)))
          (assert (null (getf snapshot :slots)))
          (assert (= 2 (first (getf snapshot :origin)))))))
    (native-host-cold-with-wal
     (lambda () (assert (logical-slot-continuation-cold-check-admission))))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (assert (equal fds (native-host-writer-open-fds)))
    (format t "~&NATIVE-COLD-DESCENDANT PASS case=~a timeline=3 origin=2 empty-repeated F-released no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-descendant-refusal (gate ownership mode)
  (let* ((fds (native-host-writer-open-fds))
         (files (native-host-cold-file-image))
         (registry *wal-replication-slots*)
         (hit nil) (condition nil)
         (*logical-slot-continuation-cold-empty-test-hook*
           (lambda (point token reservation)
             (declare (ignore token))
             (when (and (eq point :minted) (equal mode "load-descendant-timeline"))
               (let ((timeline *wal-insert-timeline*) (rejected nil))
                 (unwind-protect
                      (progn
                        (setf *wal-insert-timeline* 4)
                        (native-host-cold-with-wal
                         (lambda ()
                           (setf rejected
                                 (handler-case
                                     (progn
                                       (logical-slot-continuation-cold-reservation-call reservation :check)
                                       nil)
                                   (error () t))))))
                   (setf *wal-insert-timeline* timeline))
                 (assert rejected)
                 (setf hit t)
                 (error "descendant timeline changed after its actual reservation"))))))
    (assert (= 3 *wal-insert-timeline*))
    (handler-case
        (wal-load-replication-slots :gate-owner gate :ownership ownership)
      (error (caught) (setf condition caught)))
    (assert condition)
    (if (equal mode "load-descendant-timeline")
        (assert hit)
        (assert (search "original child host" (princ-to-string condition))))
    (assert (= 3 *wal-insert-timeline*))
    (assert (eq registry *wal-replication-slots*))
    (assert (zerop (hash-table-count registry)))
    (native-host-cold-with-wal
     (lambda ()
       (let ((reservations (loop for reservation being the hash-values of
                                 *logical-slot-continuation-cold-reservations*
                                 collect reservation)))
         (assert (= 1 (length reservations)))
         (let ((reservation (first reservations)))
           (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
           (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))))
       (assert (handler-case
                   (progn (logical-slot-continuation-cold-check-admission) nil)
                 (pg-error (caught)
                   (assert (string= "55000" (pg-error-sqlstate caught)))
                   t)))))
    (assert (equalp files (native-host-cold-file-image)))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-DESCENDANT-REFUSAL PASS mode=~a F-pinned admission-closed files-preserved no-fd-leak~%"
            mode)))

(defun native-host-cold-empty-registration-loss (token mode)
  (let* ((table *logical-slot-continuation-cold-empty-handoffs*)
         (state (gethash token table))
         (replacement (make-hash-table :test #'eq)))
    (assert state)
    (maphash (lambda (key value) (setf (gethash key replacement) value)) table)
    (unwind-protect
         (progn
           (if (equal mode "empty-registration-map")
               (setf *logical-slot-continuation-cold-empty-handoffs* replacement)
               (remhash token table))
           (native-host-cold-recheck-refused-before-io
            (lambda ()
              (if (equal mode "empty-registration-raw")
                  (funcall (logical-slot-continuation-cold-empty-handoff-state token)
                           :check token nil)
                  (logical-slot-continuation-cold-empty-call token :check)))))
      (setf *logical-slot-continuation-cold-empty-handoffs* table
            (gethash token table) state))
    (native-host-cold-recheck-refused-before-io
     (lambda () (logical-slot-continuation-cold-empty-call token :check)))
    t))

(defun native-host-cold-empty-failure (gate ownership mode &optional ordinary)
  (let* ((fds (native-host-writer-open-fds))
         (old-registry *wal-replication-slots*)
         (mirrors (logical-slot-publication-mirrors))
         (token nil) (reservation nil) (hit nil) (post nil) (armed nil)
         (visits 0) (refused nil) (prospective nil)
         (close (symbol-function 'sb-posix:close))
         (notify (symbol-function 'logical-slot-startup-publication-notify))
         (publish (symbol-function 'logical-slot-startup-publish))
         (scoped (symbol-function 'logical-slot-continuation-cold-empty-publish-scoped))
         (target (cdr (assoc mode
                             '(("empty-minted" . :minted)
                               ("empty-prepared" . :prepared)
                               ("empty-before-commit" . :before-commit)
                               ("empty-after-commit" . :after-commit)
                               ("empty-before-retirement" . :before-retirement))
                             :test #'equal)))
         (*logical-slot-continuation-cold-empty-test-hook*
           (lambda (point actual owner)
             (incf visits)
             (setf token actual reservation owner)
             (when (eq point :after-commit)
               (setf post t)
               (when (equal mode "empty-close") (setf armed t)))
             (when (and (eq point :minted)
                        (member mode '("empty-registration-map" "empty-registration-restore"
                                       "empty-registration-raw") :test #'equal))
               (native-host-cold-with-wal
                (lambda () (native-host-cold-empty-registration-loss actual mode)))
               (setf hit t)
               (error "empty handoff observed registration loss ~a" mode))
             (when (and (eq point :before-commit) (equal mode "empty-owner-change"))
               (push (first *logical-slot-imported-host-startup-owners*)
                     *logical-slot-imported-host-startup-owners*)
               (setf hit t))
             (when (and (eq point :before-commit) (equal mode "empty-table-change"))
               (assert prospective)
               (setf (gethash "intruder" prospective) nil hit t))
             (when (eq point target)
               (setf hit t)
               (error "empty handoff cut ~a" mode)))))
    (assert (or target (member mode '("empty-close" "empty-notify" "empty-registration-map"
                                      "empty-registration-restore" "empty-registration-raw"
                                      "empty-premature-close" "empty-owner-change"
                                      "empty-table-change") :test #'equal)))
    (unwind-protect
         (progn
           (when (equal mode "empty-table-change")
             (setf (symbol-function 'logical-slot-startup-publish)
                   (lambda (source restored &optional archive handoff)
                     (setf prospective restored)
                     (funcall publish source restored archive handoff))))
           (when (equal mode "empty-premature-close")
             (setf (symbol-function 'logical-slot-continuation-cold-empty-publish-scoped)
                   (lambda (actual-gate actual-ownership actual-reservation)
                     (let* ((results (multiple-value-list
                                      (funcall scoped actual-gate actual-ownership actual-reservation)))
                            (closed (third results))
                            (rejected (handler-case (progn (funcall closed) nil) (error () t))))
                       (assert rejected)
                       (setf hit t)
                       (error "empty publication cannot record closed scopes while an owner is live")))))
           (when (equal mode "empty-close")
             (setf (symbol-function 'sb-posix:close)
                   (lambda (fd)
                     (let ((result (funcall close fd)))
                       (when (and armed (not hit))
                         (setf hit t armed nil)
                         (error "empty close reported failure after closing its FD"))
                       result))))
           (when (equal mode "empty-notify")
             (setf (symbol-function 'logical-slot-startup-publication-notify)
                   (lambda (actual-gate actual-ownership restored)
                     (funcall notify actual-gate actual-ownership restored)
                     (setf hit t)
                     (error "empty notification cut"))))
           (setf refused
                 (handler-case
                     (progn
                       (if ordinary
                           (wal-load-replication-slots :gate-owner gate :ownership ownership)
                           (native-host-cold-empty-loader gate ownership))
                       nil)
                   (error () t))))
      (setf (symbol-function 'sb-posix:close) close
            (symbol-function 'logical-slot-startup-publication-notify) notify
            (symbol-function 'logical-slot-startup-publish) publish
            (symbol-function 'logical-slot-continuation-cold-empty-publish-scoped) scoped))
    (assert (and hit refused token reservation))
    (assert (= (if (and ordinary post) 1 0) (hash-table-count *wal-replication-slots*)))
    (assert (null (gethash *wal-replication-slots*
                           *logical-slot-continuation-protection-registries*)))
    (if post
        (assert (not (eq old-registry *wal-replication-slots*)))
        (progn
          (assert (eq old-registry *wal-replication-slots*))
          (assert (equal mirrors (logical-slot-publication-mirrors)))))
    (native-host-cold-with-wal
     (lambda ()
       (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
       (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
       (native-host-cold-recheck-refused-before-io
        (lambda () (logical-slot-continuation-cold-empty-call token :check)))
       (assert (handler-case
                   (progn (logical-slot-continuation-cold-check-admission) nil)
                 (pg-error (condition)
                   (assert (string= "55000" (pg-error-sqlstate condition)))
                   t)))))
    (let ((files (native-host-cold-file-image)) (before visits))
      (assert (handler-case
                  (progn (wal-load-replication-slots :gate-owner gate :ownership ownership) nil)
                (error () t)))
      (assert (= before visits))
      (assert (equalp files (native-host-cold-file-image))))
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-EMPTY-REFUSAL PASS case=~a mode=~a published=~s F-pinned admission-closed retry-fenced no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE") mode post)))

(defun native-host-cold-cleanup-file-image ()
  (let ((directory (sb-posix:open (namestring (wal-replication-slot-directory))
                                  (logior sb-posix:o-rdonly sb-posix:o-directory
                                          sb-posix:o-nofollow +postmaster-o-cloexec+))))
    (unwind-protect
         (list (physical-image-file-facts directory)
               (logical-slot-continuation-cold-read-raw-inventory directory (lambda () t))
               (physical-image-file-facts -100 (namestring (merge-pathnames "pg_logical_continuation/" *pgdata*)))
               (native-host-cold-file-image))
      (sb-posix:close directory))))

(defun native-host-cold-cleanup-fixtures (gate ownership mode)
  (when (equal mode "plan-empty")
    (dolist (name '("alpha/" "beta/"))
      (uiop:delete-directory-tree (merge-pathnames name (wal-replication-slot-directory))
                                  :validate t :if-does-not-exist :error)))
  (when (member mode '("plan-bootstrap-prefix" "plan-empty") :test #'equal)
    (let ((snapshot nil))
      (call-with-logical-slot-continuation-cold-context
       gate ownership
       (lambda (storage reservation archive)
         (declare (ignore reservation archive))
         (let ((facts (logical-slot-continuation-storage-call storage :facts)))
           (setf snapshot
                 (if (equal mode "plan-empty")
                     (logical-slot-continuation-empty-snapshot
                      (logical-slot-continuation-storage-call storage :binding)
                      (getf facts :certificate)
                      (logical-slot-continuation-storage-call storage :host))
                     (logical-slot-continuation-active-startup-image storage facts))))))
      (native-host-cold-bootstrap-selector snapshot t)))
  (native-host-cold-add-extra t nil 998)
  (slot-state-create
   (wal-replication-slot-directory) "coldtemp"
   (physical-slot-encode
    (make-wal-replication-slot :name "coldtemp" :kind :physical :system-id 999
                               :database-oid nil :plugin nil :consistent-lsn nil
                               :restart-lsn nil :restart-timeline nil :inactive-since 0
                               :wal-status :lost :temporary-p t)))
  (let ((staging (merge-pathnames "orphan.tmp/" (wal-replication-slot-directory))))
    (sb-posix:mkdir (namestring staging) #o700)
    (with-open-file (stream (merge-pathnames "state.tmp" staging) :direction :output :if-exists :error
                                                                  :element-type '(unsigned-byte 8))
      (write-sequence #(1 2 3) stream))))

(defun native-host-cold-cleanup-plan (gate ownership)
  (let* ((mode (uiop:getenv "PG_NATIVE_COLD_PROOF"))
         (registry *wal-replication-slots*) (mirrors (logical-slot-publication-mirrors))
         (pins (hash-table-count *wal-snapshot-retention-pins*))
         (lifetimes (hash-table-count *physical-slot-lifetimes*))
         (fds (native-host-writer-open-fds)) (reservation nil) (escaped nil)
         (files nil) (first-plan nil) (visits 0))
    (native-host-cold-cleanup-fixtures gate ownership mode)
    (setf files (native-host-cold-cleanup-file-image))
    (dotimes (attempt 2)
      (call-with-logical-slot-continuation-cold-cleanup-plan
       gate ownership
       (lambda (plan actual check)
         (incf visits)
         (setf escaped check)
         (if first-plan (assert (equalp first-plan plan))
             (setf first-plan (logical-slot-continuation-copy plan)))
         (assert (equal '(:remove-staging :remove-scratch :retire-temporary)
                        (mapcar (lambda (action) (getf action :operation)) (getf plan :actions))))
         (assert (equal '("orphan.tmp" "extra" "coldtemp")
                        (mapcar (lambda (action) (getf action :name)) (getf plan :actions))))
         (assert (equal '("alpha" "beta") (getf plan :protected)))
         (if (member mode '("plan-bootstrap-prefix" "plan-empty") :test #'equal)
             (progn (assert (eq :absent (getf plan :kind)))
                    (assert (eq :bootstrap (getf plan :choice)))
                    (assert (= 1 (getf (getf plan :snapshot) :generation))))
             (progn
               (assert (eq (intern (uiop:getenv "PG_NATIVE_COLD_KIND") :keyword) (getf plan :kind)))
               (assert (eq (intern (uiop:getenv "PG_NATIVE_COLD_CHOICE") :keyword) (getf plan :choice)))))
         (if (equal mode "plan-empty")
             (assert (equal '("extra") (mapcar #'first (getf plan :retained))))
             (assert (equal '("alpha" "beta" "extra") (mapcar #'first (getf plan :retained)))))
         (assert
          (eq :passed
              (sb-thread:join-thread
               (sb-thread:make-thread
                (lambda ()
                  (handler-case
                      (progn (native-host-cold-recheck-refused-before-io check) :passed)
                    (error (condition) condition)))))))
         (when reservation (assert (eq reservation actual)))
         (setf reservation actual)
         (sb-thread:with-mutex (*wal-lock*)
           (assert (logical-slot-continuation-cold-reservation-call actual :check))
           (assert (= 41268 (gethash actual *wal-snapshot-retention-pins*))))
         (incf (getf (getf plan :snapshot) :generation))
         (setf (getf (first (getf plan :ledger)) :name) "changed-copy")
         (funcall check)))
      (native-host-cold-recheck-refused-before-io escaped)
      (assert (eq registry *wal-replication-slots*))
      (assert (zerop (hash-table-count registry)))
      (assert (equal mirrors (logical-slot-publication-mirrors)))
      (assert (null (gethash registry *logical-slot-continuation-protection-registries*)))
      (assert (equalp files (native-host-cold-cleanup-file-image))))
    (assert (= 2 visits))
    (assert (= (1+ pins) (hash-table-count *wal-snapshot-retention-pins*)))
    (assert (= lifetimes (hash-table-count *physical-slot-lifetimes*)))

    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))

    (assert (equal fds (native-host-writer-open-fds)))

    (format t "~&NATIVE-COLD-CLEANUP-PLAN PASS case=~a mode=~a complete-ledger no-effects persistent-F no-admission repeat-idempotent no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE") mode)))

(defun native-host-cold-cleanup-corrupt-scratch ()
  (with-open-file (stream (merge-pathnames "pg_replslot/extra/state.tmp" *pgdata*)
                          :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
    (write-byte 0 stream)))

(defun native-host-cold-cleanup-without-scratch (image)
  (let* ((copy (logical-slot-continuation-copy image))
         (extra (find "extra" (second copy) :key (lambda (row) (getf row :name)) :test #'string=)))
    (setf (getf extra :files) (remove "state.tmp" (getf extra :files) :key #'first :test #'string=))
    copy))

(defun native-host-cold-cleanup-step-cut (phase step index)
  (let ((cut (format nil "~(~a~)-~2,'0d-~(~a~)" phase index (first step))))
    (when (equal cut (uiop:getenv "PG_NATIVE_COLD_RECONCILE_CUT"))
      (format t "~&NATIVE-COLD-CLEANUP-EFFECTS-SIGKILL ~a~%" cut)
      (finish-output)
      (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill))))

(defun native-host-cold-cleanup-expected (files extra)
  (native-host-cold-logical-hex
   (string->bytes
    (with-standard-io-syntax
      (write-to-string (list files extra) :readably t :length nil :level nil)))))

(defun native-host-cold-cleanup-effects (gate ownership)
  (let* ((registry *wal-replication-slots*) (mirrors (logical-slot-publication-mirrors))
                                            (pin-table *wal-snapshot-retention-pins*)
                                            (pins (loop for key being the hash-keys of pin-table using (hash-value value)
                                                        collect (cons key value)))
                                            (lifetimes (hash-table-count *physical-slot-lifetimes*))
                                            (fds (native-host-writer-open-fds)) (reservation nil)
                                            (files nil) (extra nil) (steps 0)
                                            (restart (uiop:getenv "PG_NATIVE_COLD_CLEANUP_RESTART"))
                                            (*logical-slot-continuation-cold-cleanup-test-hook*
                                              (lambda (phase step)
                                                (native-host-cold-cleanup-step-cut phase step steps)
                                                (when (eq phase :after) (incf steps))))
                                            (*logical-slot-continuation-cold-test-hook*
                                              (lambda (point actual)
                                                (when (eq point :protected)
                                                  (when reservation (assert (eq reservation actual)))
                                                  (setf reservation actual)))))
    (unless restart
      (native-host-cold-cleanup-fixtures gate ownership "plan")
      (with-open-file (stream (merge-pathnames "pg_replslot/coldtemp/state.tmp" *pgdata*)
                              :direction :output :if-exists :error :element-type '(unsigned-byte 8))
        (write-sequence #(7 8) stream)))
    (setf files (native-host-cold-file-image)
          extra (let ((path (merge-pathnames "pg_replslot/extra/state" *pgdata*)))
                  (list (physical-image-file-facts -100 (namestring path))
                        (read-bounded-regular-file path +slot-state-maximum-bytes+))))
    (format t "~&NATIVE-COLD-CLEANUP-EFFECTS-EXPECTED ~a~%"
            (native-host-cold-cleanup-expected files extra))
    (finish-output)
    (let ((actions (logical-slot-continuation-cold-cleanup-startup gate ownership)))
      (if restart (assert (<= 0 actions 3)) (assert (= 3 actions))))
    (assert (zerop (logical-slot-continuation-cold-cleanup-startup gate ownership)))
    (assert (eq registry *wal-replication-slots*))
    (assert (zerop (hash-table-count registry)))
    (assert (equal mirrors (logical-slot-publication-mirrors)))
    (assert (equalp files (native-host-cold-file-image)))
    (let* ((image (native-host-cold-cleanup-file-image))
           (rows (second image))
           (ordinary (find "extra" rows :key (lambda (row) (getf row :name)) :test #'equal)))
      (assert (equal '("alpha" "beta" "extra")
                     (mapcar (lambda (row) (getf row :name)) rows)))
      (assert (equal '("state") (mapcar #'first (getf ordinary :files))))
      (assert (equalp extra (rest (first (getf ordinary :files))))))
    (assert (eq pin-table *wal-snapshot-retention-pins*))
    (assert (= (1+ (length pins)) (hash-table-count pin-table)))
    (dolist (entry pins) (assert (equal (cdr entry) (gethash (car entry) pin-table))))
    (assert (= lifetimes (hash-table-count *physical-slot-lifetimes*)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (assert reservation)
    (sb-thread:with-mutex (*wal-lock*)
      (assert (logical-slot-continuation-cold-reservation-call reservation :check))
      (assert (= 41268 (gethash reservation pin-table))))
    (assert (equal fds (native-host-writer-open-fds)))
    (format t "~&NATIVE-COLD-CLEANUP-EFFECTS PASS case=~a stages scratch temporary-retirement exact-retained-inodes persistent-F empty-registry no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-cold-cleanup-plan-refusal (gate ownership mode)
  (let ((close (symbol-function 'sb-posix:close))
        (registry *wal-replication-slots*) (mirrors (logical-slot-publication-mirrors))
        (fds (native-host-writer-open-fds)) (reservation nil) (escaped nil)
        (changed nil) (injected nil) (visits 0) (caught nil) (files nil))
    (native-host-cold-cleanup-fixtures gate ownership mode)
    (setf files (native-host-cold-cleanup-file-image))
    (let ((*logical-slot-continuation-cold-test-hook*
            (lambda (point actual)
              (when (eq point :protected) (setf reservation actual))
              (when (and (equal mode "plan-close") (eq point :after-observation))
                (setf (symbol-function 'sb-posix:close)
                      (lambda (fd)
                        (funcall close fd)
                        (setf (symbol-function 'sb-posix:close) close
                              injected t)
                        (error "native cleanup close failure"))))))
          (*logical-slot-continuation-inventory-test-hook*
            (lambda (point)
              (when (and (equal mode "plan-replaced") (eq point :between-passes) (not changed))
                (native-host-cold-cleanup-corrupt-scratch)
                (setf changed t)))))
      (unwind-protect
           (setf caught
                 (handler-case
                     (progn
                       (call-with-logical-slot-continuation-cold-cleanup-plan
                        gate ownership
                        (lambda (plan actual check)
                          (declare (ignore plan))
                          (incf visits)
                          (setf reservation actual escaped check)
                          (funcall check)
                          (when (equal mode "plan-caught")
                            (native-host-cold-cleanup-corrupt-scratch)
                            (setf changed t)
                            (assert (handler-case (progn (funcall check) nil) (error () t)))
                            (native-host-cold-recheck-refused-before-io check))))
                       nil)
                   (error (condition) (princ-to-string condition))))
        (setf (symbol-function 'sb-posix:close) close)))
    (assert caught)
    (cond
      ((equal mode "plan-replaced") (assert (and changed (zerop visits))))
      ((equal mode "plan-caught")
       (assert (and changed (= 1 visits) (search "cold cleanup plan failed" caught))))
      ((equal mode "plan-close")
       (assert (and injected (= 1 visits) (search "native cleanup close failure" caught)))))
    (when escaped (native-host-cold-recheck-refused-before-io escaped))
    (assert (eq registry *wal-replication-slots*))
    (assert (zerop (hash-table-count registry)))
    (assert (equal mirrors (logical-slot-publication-mirrors)))
    (assert (null (gethash registry *logical-slot-continuation-protection-registries*)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (assert reservation)
    (sb-thread:with-mutex (*wal-lock*)
      (assert (logical-slot-continuation-cold-reservation-call reservation :uncertain))
      (assert (= 41268 (gethash reservation *wal-snapshot-retention-pins*)))
      (assert (handler-case
                  (progn (logical-slot-continuation-cold-reservation-call reservation :check) nil)
                (error () t))))
    (assert (equal fds (native-host-writer-open-fds)))
    (let ((after (native-host-cold-cleanup-file-image)))
      (assert (equalp (if changed (native-host-cold-cleanup-without-scratch files) files)
                      (if changed (native-host-cold-cleanup-without-scratch after) after))))
    (format t "~&NATIVE-COLD-CLEANUP-PLAN PASS case=~a refusal=~a persistent-F uncertain empty-registry no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE") mode)))

(defun native-host-continuation-cold-state (gate ownership)
  (with-native-cold-spill-probe
      (handler-case
          (progn
            (when (uiop:getenv "PG_NATIVE_COLD_APPEND")
              (native-host-cold-append :commit (equal "prepare-wal" (uiop:getenv "PG_NATIVE_COLD_PROOF"))))
            (cond
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("empty-minted" "empty-prepared" "empty-before-commit" "empty-after-commit"
                         "empty-before-retirement" "empty-close" "empty-notify"
                         "empty-registration-map" "empty-registration-restore" "empty-registration-raw"
                         "empty-premature-close" "empty-owner-change" "empty-table-change") :test #'equal)
               (native-host-cold-empty-failure gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
              ((equal "cleanup" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
               (native-host-cold-cleanup-effects gate ownership))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("plan-replaced" "plan-caught" "plan-close") :test #'equal)
               (native-host-cold-cleanup-plan-refusal gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("plan" "plan-bootstrap-prefix" "plan-empty") :test #'equal)
               (native-host-cold-cleanup-plan gate ownership))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("publish-after-group-pin" "publish-maps-ready" "publish-derived" "publish-prepared"
                         "publish-before-commit" "publish-after-commit" "publish-owner-return" "publish-close"
                         "publish-notify" "publish-before-retirement" "publish-lost-registration") :test #'equal)
               (native-host-cold-publication-failure gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("load-logical" "load-legacy-logical" "load-unbound-logical" "load-legacy-empty" "load-noncanonical-v4")
                       :test #'equal)
               (native-host-cold-logical-publication gate ownership))
              ((equal "load-physical" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
               (native-host-cold-mixed-publication gate ownership))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("load-mixed-refusal" "load-mixed-staging-refusal") :test #'equal)
               (native-host-cold-mixed-loader-refusal
                gate ownership (equal "load-mixed-staging-refusal" (uiop:getenv "PG_NATIVE_COLD_PROOF"))))
              ((eql 0 (search "empty-ordinary-" (or (uiop:getenv "PG_NATIVE_COLD_PROOF") "")))
               (native-host-cold-empty-failure
                gate ownership
                (concatenate 'string "empty-" (subseq (uiop:getenv "PG_NATIVE_COLD_PROOF")
                                                      (length "empty-ordinary-")))
                t))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("load-descendant-prepared" "load-descendant-child" "load-descendant-timeline")
                       :test #'equal)
               (native-host-cold-descendant-refusal gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
              ((equal "load-descendant-empty" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
               (native-host-cold-descendant-empty gate ownership))
              ((equal "load-empty-logical" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
               (native-host-cold-empty-logical-restart gate ownership))
              ((equal "load-empty-ordinary" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
               (native-host-cold-empty-ordinary-restart gate ownership))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF") '("load-empty" "load-empty-late") :test #'equal)
               (native-host-cold-empty-loader
                gate ownership (equal "load-empty-late" (uiop:getenv "PG_NATIVE_COLD_PROOF"))))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("load-bootstrap" "load-bootstrap-prefix") :test #'equal)
               (native-host-cold-publication
                gate ownership
                (if (equal "load-bootstrap-prefix" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
                    :bootstrap-prefix :bootstrap)))
              ((eql 0 (search "load-" (or (uiop:getenv "PG_NATIVE_COLD_PROOF") "")))
               (native-host-cold-publication-failure
                gate ownership (concatenate 'string "publish-" (subseq (uiop:getenv "PG_NATIVE_COLD_PROOF") 5)) t))
              ((equal "load" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
               (native-host-cold-publication gate ownership t))
              ((equal "publish" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
               (native-host-cold-publication gate ownership))
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
          (finish-output)))))

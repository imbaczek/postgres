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
        (call-with-logical-replication-slot
         "beta"
         (lambda (owned)
           (assert (eq beta owned))
           (assert (< (wal-replication-slot-confirmed-flush-lsn owned) 37853))
           (assert (eq beta (logical-slot-continuation-native-acknowledge
                             "beta" 37853 *wal-logical-slot-owner*)))))
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
                                            (restore (symbol-function 'logical-slot-continuation-restore-startup)))
    (unwind-protect
         (progn
           (setf (symbol-function 'logical-slot-continuation-restore-startup)
                 (lambda (actual-gate actual-ownership)
                   (let ((result (funcall restore actual-gate actual-ownership)))
                     (assert (null result))
                     (native-host-cold-add-extra t)
                     (setf before (native-host-cold-extra-files) hit t)
                     result)))
           (setf refused
                 (handler-case
                     (progn (wal-load-replication-slots :gate-owner gate :ownership ownership) nil)
                   (error () t))))
      (setf (symbol-function 'logical-slot-continuation-restore-startup) restore))
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

(defun native-host-cold-empty-loader (gate ownership &optional late)
  (let ((fds (native-host-writer-open-fds)) (workspace-facts nil)
                                            (selector (merge-pathnames "pg_logical_continuation/selector" *pgdata*))
                                            (workspace (merge-pathnames "pg_replslot/.logical-slot-copy.work" *pgdata*))
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
        (wal-load-replication-slots :gate-owner gate :ownership ownership)
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
    (assert (equal fds (native-host-writer-open-fds)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-COLD-EMPTY-LOADER PASS case=~a bootstrap restart exact-empty-selector opaque-workspace no-cold-publication no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_COLD_CASE"))))

(defun native-host-continuation-cold-state (gate ownership)
  (with-native-cold-spill-probe
      (handler-case
          (progn
            (when (uiop:getenv "PG_NATIVE_COLD_APPEND") (native-host-cold-append))
            (cond
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("publish-after-group-pin" "publish-maps-ready" "publish-derived" "publish-prepared"
                         "publish-before-commit" "publish-after-commit" "publish-owner-return" "publish-close"
                         "publish-notify" "publish-before-retirement" "publish-lost-registration") :test #'equal)
               (native-host-cold-publication-failure gate ownership (uiop:getenv "PG_NATIVE_COLD_PROOF")))
              ((equal "load-physical" (uiop:getenv "PG_NATIVE_COLD_PROOF"))
               (native-host-cold-mixed-publication gate ownership))
              ((member (uiop:getenv "PG_NATIVE_COLD_PROOF")
                       '("load-mixed-refusal" "load-mixed-staging-refusal") :test #'equal)
               (native-host-cold-mixed-loader-refusal
                gate ownership (equal "load-mixed-staging-refusal" (uiop:getenv "PG_NATIVE_COLD_PROOF"))))
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

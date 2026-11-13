(in-package #:pg)

(defvar *logical-import-native-thread* nil)
(defvar *logical-import-native-result* nil)

(defvar *logical-import-native-expected-disposition* :wal-level-insufficient)

(defvar *logical-import-native-expected-names* '("alpha"))

(defun logical-import-native-rejects (function &optional message)
  (assert (handler-case (progn (funcall function) nil)
            (error (condition)
              (or (null message) (search message (princ-to-string condition)))))))

(defun logical-import-native-source-change (owner context)
  (let* ((path (merge-pathnames "logical-slot-copy.descriptor"
                                (physical-image-context-root-path context)))
         (bytes (read-bounded-regular-file path +logical-slot-copy-descriptor-maximum-bytes+))
         (injected nil) (changed nil)
         (*logical-slot-copy-verification-test-hook*
           (lambda (point observed)
             (declare (ignore observed))
             (when (and (eq point :after-import-source-scan) (not injected))
               (with-open-file (out path :direction :io :element-type '(unsigned-byte 8)
                                         :if-exists :overwrite)
                 (write-byte (logxor 1 (aref bytes 0)) out)
                 (force-output out))
               (setf changed (= (logxor 1 (aref bytes 0))
                                (aref (read-bounded-regular-file
                                       path +logical-slot-copy-descriptor-maximum-bytes+) 0))
                     injected t)))))
    (unwind-protect
         (logical-import-native-rejects
          (lambda () (logical-slot-copy-prepare-import-source owner context)))
      (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                                :if-exists :overwrite)
        (write-sequence bytes out)))
    (assert (and injected changed))
    (logical-slot-copy-check-import-receipt owner context)))

(defun logical-import-native-state-change (owner context)
  (let* ((state *physical-recovery-state*)
         (snapshot (getf state :source-checkpoint))
         (injected nil)
         (*logical-slot-copy-verification-test-hook*
           (lambda (point observed)
             (declare (ignore observed))
             (when (and (eq point :after-import-source-scan) (not injected))
               (setf (getf state :source-checkpoint) (copy-list snapshot))
               (incf (getf (getf state :source-checkpoint) :time))
               (setf injected t)))))
    (assert snapshot)
    (unwind-protect
         (logical-import-native-rejects
          (lambda () (logical-slot-copy-prepare-import-source owner context))
          "selected restartpoint changed")
      (setf (getf state :source-checkpoint) snapshot))
    (assert injected)
    (assert (eq state *physical-recovery-state*))
    (logical-slot-copy-check-input owner context)))

(defun logical-import-native-source (owner context descriptor control)
  (let* ((source (logical-slot-copy-prepare-import-source owner context))
         (rows (getf source :slots)))
    (assert (= (getf control :phase) (getf source :phase)))
    (assert (= (getf control :applied) (getf source :fork)))
    (assert (equal control (physical-recovery-decode-control (getf source :control-bytes))))
    (assert (eq (eq :eligible *logical-import-native-expected-disposition*)
                (getf source :eligible-interest)))
    (assert (equal *logical-import-native-expected-names* (mapcar #'first rows)))
    (assert (= (length rows) (length (logical-slot-copy-descriptor-slots descriptor))))
    (loop for row in rows
          for (name bytes) in (logical-slot-copy-descriptor-slots descriptor)
          do (assert (equal name (first row)))
             (assert (equalp bytes (string->bytes (fourth row))))
             (assert (= (third row) (physical-store-update-digest #xCBF29CE484222325 bytes)))
             (assert (eq *logical-import-native-expected-disposition* (fifth row)))
             (let ((slot (logical-slot-state-decode bytes name)))
               (assert (equal "pgoutput"
                              (wal-replication-slot-plugin slot)))
               (assert (eq :text (wal-slot-profile slot)))))
    (format t "~&LOGICAL-IMPORT-SOURCE-PASS ~D ~D~%" (getf source :phase) (length rows))
    (finish-output)
    source))

(defun logical-import-native-live-scope (phase)
  (let* ((store *physical-receive-store*) (state *physical-recovery-state*)
                                          (lock (physical-store-lock-stream store))
                                          (tuple (physical-store-control-tuple store))
                                          (escaped nil) (escaped-context nil))
    (assert (= phase (getf state :phase)))
    (call-with-logical-slot-copy-import-input
     (lambda (owner context)
       (setf escaped owner escaped-context context)
       (assert (and (eq :live (logical-slot-copy-import-owner-mode owner))
                    (not (logical-slot-copy-startup-owner-p owner))))
       (logical-slot-copy-check-input owner context)
       (logical-import-native-rejects
        (lambda () (logical-slot-copy-check-import-publication owner context))
        "publication owner")
       (multiple-value-bind (descriptor observations manifest control)
           (logical-slot-copy-check-import-receipt owner context)
         (assert (and descriptor observations manifest (= phase (getf control :phase))
                      (= *last-replay-lsn* (getf control :applied))))
         (logical-import-native-source owner context descriptor control)
         (when (= phase 2)
           (logical-import-native-source-change owner context)
           (logical-import-native-state-change owner context)))
       (logical-import-native-rejects
        (lambda () (logical-slot-copy-check-base-proof-input owner context nil)))
       (let ((*physical-recovery-state* (copy-list state)))
         (logical-import-native-rejects
          (lambda () (physical-adoption-check-owner-role owner))))
       (assert (sb-thread:join-thread
                (sb-thread:make-thread
                 (lambda ()
                   (handler-case (progn (postmaster-check-ownership
                                         (logical-slot-copy-import-owner-ownership owner)) nil)
                     (error () t)))
                 :name "logical import foreign owner probe")))
       (let ((*pgdata* (physical-image-context-root-path context))
             (*physical-image-context* nil))
         (physical-follow-call-with-receive-root
          store (lambda () (physical-slot-check-standby-receive
                            store state *last-replay-lsn*))))
       (logical-slot-copy-check-input owner context)))
    (assert (not (physical-adoption-owner-active-p escaped)))
    (logical-import-native-rejects
     (lambda () (logical-slot-copy-check-input escaped escaped-context)))
    (logical-import-native-rejects
     (lambda () (logical-slot-copy-prepare-import-source escaped escaped-context)))
    (unwind-protect
         (progn
           (setf (physical-adoption-owner-active-p escaped) t)
           (logical-import-native-rejects
            (lambda () (postmaster-check-ownership
                        (logical-slot-copy-import-owner-ownership escaped))))
           (logical-import-native-rejects
            (lambda () (physical-directory-check-owner escaped))))
      (setf (physical-adoption-owner-active-p escaped) nil))
    (assert (and (eq store *physical-receive-store*) (eq state *physical-recovery-state*)
                 (eq lock (physical-store-lock-stream store)) (open-stream-p lock)
                 (not (physical-store-closed-p store)) (not (physical-store-failed-p store))
                 (equal tuple (physical-store-control-tuple store))))
    (physical-store-check-held-lock store)
    (when (= phase 2)
      (logical-import-native-rejects #'logical-slot-copy-publish-import-pending "requires phase 3"))
    (format t "~&LOGICAL-IMPORT-LIVE-SCOPE-PASS ~D~%" phase)
    (finish-output)))

(defun logical-import-native-reject-copy-id-owner (path saved bytes)
  (let* ((bad (copy-tree saved))
         (owner (logical-slot-parse-generation (getf saved :copy-id))))
    (setf (getf bad :owner) (getf saved :copy-id)
          (getf bad :slots)
          (loop for row in (getf saved :slots)
                for slot = (logical-slot-state-decode (string->bytes (fourth row)) (first row))
                collect (list (first row) (second row) (third row)
                              (bytes->string
                               (logical-slot-copy-import-child-image
                                slot owner (logical-slot-effective-invalidation-cause slot)))
                              (fifth row))))
    (let ((forged (logical-slot-copy-import-image bad)))
      (assert (equal bad (logical-slot-copy-import-decode forged)))
      (assert (= (length forged) (length bytes)))
      (flet ((overwrite (image)
               (with-open-file (output path :direction :output :if-exists :overwrite
                                            :if-does-not-exist :error
                                            :element-type '(unsigned-byte 8))
                 (write-sequence image output))))
        (unwind-protect
             (progn
               (overwrite forged)
               (logical-import-native-rejects
                #'logical-slot-copy-publish-import-pending "aliases a copied identity"))
          (overwrite bytes))))))

(defun logical-import-native-pending-publication ()
  (let* ((path (merge-pathnames "logical-slot-copy.import" *pgdata*))
         (temporary (merge-pathnames "logical-slot-copy.import.tmp" *pgdata*)))
    (assert (not (probe-file path)))
    (assert (not (probe-file temporary)))
    (let ((*max-replication-slots* 0) (minted 0))
      (let ((*logical-slot-copy-verification-test-hook*
              (lambda (point context)
                (declare (ignore context))
                (when (eq point :before-import-owner-mint) (incf minted)))))
        (logical-import-native-rejects #'logical-slot-copy-publish-import-pending
                                       "max_replication_slots")
        (assert (zerop minted))))
    (let ((cut nil))
      (let ((*logical-slot-copy-verification-test-hook*
              (lambda (point context)
                (declare (ignore context))
                (when (eq point :before-import-pending-file-fsync)
                  (setf cut t)
                  (error "pending-before-fsync-cut")))))
        (logical-import-native-rejects #'logical-slot-copy-publish-import-pending
                                       "pending-before-fsync-cut"))
      (assert cut)
      (assert (and (probe-file temporary) (not (probe-file path))))
      (logical-import-native-rejects #'logical-slot-copy-publish-import-pending
                                     "unexpected pending import temporary")
      (delete-file temporary))
    (let ((cut nil))
      (let ((*logical-slot-copy-verification-test-hook*
              (lambda (point context)
                (declare (ignore context))
                (when (eq point :after-import-pending-rename)
                  (setf cut t)
                  (error "pending-after-rename-cut")))))
        (logical-import-native-rejects #'logical-slot-copy-publish-import-pending
                                       "pending-after-rename-cut"))
      (assert (and cut (probe-file path) (not (probe-file temporary)))))
    (let* ((bytes (read-bounded-regular-file path +logical-slot-copy-import-maximum-bytes+))
           (saved (logical-slot-copy-import-decode bytes))
           (inode (sb-posix:stat-ino (slot-state-lstat path)))
           (*logical-slot-copy-verification-test-hook*
             (lambda (point context)
               (declare (ignore context))
               (when (eq point :before-import-owner-mint)
                 (error "pending retry minted another owner")))))
      (dotimes (retry 2)
        (declare (ignorable retry))
        (assert (equal saved (logical-slot-copy-publish-import-pending)))
        (assert (= inode (sb-posix:stat-ino (slot-state-lstat path))))
        (assert (equalp bytes (read-bounded-regular-file path +logical-slot-copy-import-maximum-bytes+))))
      (assert (eq :pending (getf saved :phase)))
      (assert (equal *logical-import-native-expected-names* (mapcar #'first (getf saved :slots))))
      (assert (= (getf saved :pending-control-digest)
                 (logical-slot-copy-import-digest
                  (read-bounded-regular-file (physical-recovery-control-path)
                                             +physical-recovery-control-maximum-bytes+))))
      (dolist (row (getf saved :slots))
        (let ((slot (logical-slot-state-decode (string->bytes (fourth row)) (first row))))
          (assert (= (logical-slot-parse-generation (getf saved :owner))
                     (wal-replication-slot-child-owner slot)))
          (assert (eq (if (eq *logical-import-native-expected-disposition* :eligible) :none
                          *logical-import-native-expected-disposition*)
                      (logical-slot-effective-invalidation-cause slot)))))
      (logical-import-native-reject-copy-id-owner path saved bytes)
      (format t "~&LOGICAL-IMPORT-PENDING-PUBLICATION-PASS ~A~%" (getf saved :owner))
      (finish-output))))

(defun logical-import-native-make-pending ()
  ;; This fixture uses the durable intent producer, never the public promotion
  ;; entry point. The copied-slot fence must remain in place on both sides.
  (logical-import-native-rejects #'logical-slot-copy-check-promotion
                                 "child-owner import before promotion")
  (let ((policy (startup-promotion-policy-arguments))
        (base-control (read-bounded-regular-file
                       (merge-pathnames "global/pg_wal_control" *pgdata*)
                       (1+ +wal-control-maximum-bytes+))))
    (call-with-logical-slot-copy-import-input
     (lambda (owner context)
       (let ((source (logical-slot-copy-prepare-import-source owner context)))
         (when (getf source :eligible-interest)
           (setf (getf policy :promotion-logical-decoding-enabled) t)))))
    (check-physical-resource-promotion)
    (physical-follow-promotion-cleanup-config)
    (physical-recovery-publish-selected-history *physical-receive-store*)
    (let* ((state *physical-recovery-state*)
           (namespace (physical-promotion-check-base-control state))
           (floor (physical-promotion-timeline-floor state))
           (child (physical-promotion-select-child floor)))
      (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
        (assert (equalp catalog (catalog-image-bytes)))
        (multiple-value-bind (fork next) (physical-promotion-check-progress state metadata prefix generation)
          (physical-promotion-preflight-runtime
           next (getf (getf metadata :source-reservation) :bound) fork)
          (with-physical-follow-controller-publication (obsolete)
            (physical-promotion-freeze-timeline-floor floor)
            (let ((finished nil))
              (unwind-protect
                   (progn
                     (setf *physical-recovery-state*
                           (physical-promotion-publish-retained-intent
                            state metadata fork
                            (lambda (reference)
                              (apply #'physical-promotion-make-pending
                                     state metadata prefix generation namespace child
                                     (- (get-universal-time) 2208988800) reference policy))))
                     (setf finished t))
                (unless finished (setf *physical-recovery-failed-p* t))))))))
    (assert (equalp base-control
                    (read-bounded-regular-file
                     (merge-pathnames "global/pg_wal_control" *pgdata*)
                     (1+ +wal-control-maximum-bytes+)))))
  (logical-import-native-rejects #'logical-slot-copy-check-promotion
                                 "child-owner import before promotion")
  (logical-import-native-pending-publication)
  (logical-import-native-live-scope 3))

(defun logical-import-native-run-live (&optional pending)
  (physical-follow-call-with-controller-blocked
   (lambda ()
     (sb-thread:with-mutex (*physical-shutdown-lock*)
       (sb-thread:with-mutex (*physical-follow-command-lock*)
         (let ((stopping *physical-promotion-stopping-p*)
               (restartpoint (not (null *physical-restartpoint-worker*))))
           (unwind-protect
                (progn
                  (with-live-operation
                    (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
                      (setf *physical-promotion-stopping-p* t)))
                  (physical-restartpoint-join-for-shutdown)
                  (sb-thread:with-mutex (*reload-run-lock*)
                    (with-live-operation-quiescence
                      (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
                        (sb-thread:with-mutex (*timeline-history-lock*)
                          (logical-import-native-live-scope 2)
                          (when pending (logical-import-native-make-pending)))))))
             (unless pending
               (with-live-operation
                 (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
                   (setf *physical-promotion-stopping-p* stopping))
                 (when restartpoint (physical-restartpoint-start-worker)))))))))))

(defun logical-import-native-start (&optional pending)
  (assert (not (and *logical-import-native-thread*
                    (sb-thread:thread-alive-p *logical-import-native-thread*))))
  (setf *logical-import-native-result* nil
        *logical-import-native-thread*
        (sb-thread:make-thread
         (lambda ()
           (setf *logical-import-native-result*
                 (handler-case (progn (logical-import-native-run-live pending) (list :passed))
                   (error (condition) (list :error (princ-to-string condition))))))
         :name "logical import native input probe"))
  :started)

(defun logical-import-native-crash-pending (cut)
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
          (sb-thread:with-mutex (*timeline-history-lock*)
            (let ((*logical-slot-copy-verification-test-hook*
                    (lambda (point context)
                      (declare (ignore context))
                      (when (eq point cut)
                        (format t "~&LOGICAL-IMPORT-SIGKILL-CUT ~S~%" cut)
                        (finish-output)
                        (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)))))
              (logical-slot-copy-publish-import-pending)
              (error "pending import SIGKILL cut was not reached"))))))))

(defun logical-import-native-cold-probe ()
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
          (sb-thread:with-mutex (*timeline-history-lock*)
            (let ((bytes (read-bounded-regular-file
                          (merge-pathnames "logical-slot-copy.import" *pgdata*)
                          +logical-slot-copy-import-maximum-bytes+)))
              (assert (equal (logical-slot-copy-import-decode bytes)
                             (logical-slot-copy-publish-import-pending)))
              (assert (equalp bytes (read-bounded-regular-file
                                     (merge-pathnames "logical-slot-copy.import" *pgdata*)
                                     +logical-slot-copy-import-maximum-bytes+))))
            (let ((escaped nil))
              (call-with-logical-slot-copy-import-input
               (lambda (owner context)
                 (setf escaped owner)
                 (assert (and (eq :cold (logical-slot-copy-import-owner-mode owner))
                              (= 3 (logical-slot-copy-import-owner-phase owner))
                              (not (logical-slot-copy-startup-owner-p owner))))
                 (multiple-value-bind (descriptor observations manifest control)
                     (logical-slot-copy-check-import-receipt owner context)
                   (declare (ignore observations manifest))
                   (logical-import-native-source owner context descriptor control))
                 (let ((directory (physical-adoption-owner-directory owner))
                       (fd (physical-image-context-root-fd context))
                       (other (cdr (assoc "global" (physical-image-context-children context)
                                          :test #'equal))))
                   (dolist (parent (list (cons directory -1) (list directory fd)
                                         (cons #P"relative/" fd)))
                     (logical-import-native-rejects
                      (lambda () (physical-promotion-read-physical-slots :parent parent))
                      "promotion slot parent requires"))
                   (assert other)
                   (logical-import-native-rejects
                    (lambda ()
                      (physical-promotion-read-physical-slots :parent (cons directory other)))
                    "replication slot directory binding changed"))
                 (let ((*pgdata* (physical-image-context-root-path context))
                       (*physical-image-context* nil))
                   (multiple-value-bind (state metadata prefix generation)
                       (physical-promotion-read-pending-state)
                     (let ((store nil))
                       (unwind-protect
                            (progn
                              (physical-promotion-open-pending-store
                               state :on-open (lambda (opened) (setf store opened)))
                              (assert store)
                              (physical-promotion-validate-cold-state
                               state metadata prefix generation store #'physical-promotion-wal-plan
                               (cons (physical-adoption-owner-directory owner)
                                     (physical-image-context-root-fd context)))
                              (physical-follow-call-with-receive-root
                               store
                               (lambda ()
                                 (physical-slot-check-standby-receive
                                  store state (getf state :applied)))))
                         (when store (physical-store-close store))))))
                 (logical-slot-copy-check-input owner context)))
              (logical-import-native-rejects
               (lambda () (postmaster-check-ownership
                           (logical-slot-copy-import-owner-ownership escaped))))
              (assert (and (not *wal-ready*) (null *catalog*) (null *physical-receive-store*)
                           (null *physical-recovery-state*) (null *wal-output-stream*)
                           (null *clog-stream*)
                           (zerop (hash-table-count *wal-replication-slots*))))
              (format t "~&LOGICAL-IMPORT-COLD-SCOPE-PASS 3~%")
              (finish-output))))))))

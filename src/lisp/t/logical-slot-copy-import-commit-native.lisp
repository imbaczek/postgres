(in-package #:pg)

(defvar *logical-import-commit-test-omit-control-fact* nil)

(defvar *logical-import-commit-test-result* nil)
(defvar *logical-import-commit-test-thread* nil)

(defvar *logical-import-commit-test-cut* nil)

(defvar *logical-import-commit-test-injected* nil)

(defun logical-import-commit-test-point (point)
  (cond
    ((eq point *logical-import-commit-test-cut*)
     (format t "~&LOGICAL-IMPORT-CHILD-SIGKILL ~s~%" point)
     (finish-output)
     (sb-posix:kill (sb-posix:getpid) 9))
    ((and (eq *logical-import-commit-test-cut* :delay) (eq point :after-child-control-rename))
     (setf *logical-import-commit-test-injected* t)
     (sleep 1.2))
    ((and (eq *logical-import-commit-test-cut* :rewrite-certificate)
          (eq point :after-child-history-sync))
     (let* ((path (merge-pathnames "logical-slot-copy.import" *pgdata*))
            (bytes (read-bounded-regular-file path +logical-slot-copy-import-maximum-bytes+)))
       (with-open-file (stream path :direction :output :if-exists :supersede
                                    :element-type '(unsigned-byte 8))
         (write-sequence bytes stream)
         (fsync-stream stream))
       (sb-posix:chmod (namestring path) #o600)
       (setf *logical-import-commit-test-injected* t)))
    ((and (eq *logical-import-commit-test-cut* :hardlink-child)
          (eq point :after-child-history-sync))
     (sb-posix:link
      (namestring (wal-segment-path (floor (getf *physical-recovery-state* :applied) +wal-segment-size+)
                                    (getf *physical-recovery-state* :promotion-child) :timeline))
      (namestring (merge-pathnames "child-control-alias" *pgdata*)))
     (setf *logical-import-commit-test-injected* t))
    ((and (eq *logical-import-commit-test-cut* :symlink-control-temp)
          (eq point :after-child-data-sync))
     (let ((witness (merge-pathnames "child-control-witness" *pgdata*)))
       (with-open-file (stream witness :direction :output :if-exists :error)
         (write-string "unchanged" stream))
       (sb-posix:symlink (namestring witness)
                         (namestring (pgdata-resolve-path "global/pg_wal_control.tmp" *pgdata*)))
       (setf *logical-import-commit-test-injected* t)))
    ((and (eq *logical-import-commit-test-cut* :corrupt-child)
          (eq point :after-child-data-sync))
     (let ((path (wal-segment-path (floor (getf *physical-recovery-state* :applied) +wal-segment-size+)
                                   (getf *physical-recovery-state* :promotion-child) :timeline)))
       (with-open-file (stream path :direction :io :if-exists :overwrite
                                    :element-type '(unsigned-byte 8))
         (let ((byte (read-byte stream)))
           (file-position stream 0)
           (write-byte (logxor byte 1) stream)
           (fsync-stream stream))))
     (setf *logical-import-commit-test-injected* t))
    ((and (eq *logical-import-commit-test-cut* :replace-child-control)
          (eq point :after-child-control-rename))
     (let* ((path (wal-control-path))
            (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))
            (bytes (read-bounded-regular-file path (1+ +wal-control-maximum-bytes+))))
       (sb-posix:unlink (namestring path))
       (with-open-file (stream path :direction :output :if-exists :error
                                    :element-type '(unsigned-byte 8))
         (write-sequence bytes stream))
       (sb-posix:chmod (namestring path) #o600)
       (assert (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))))
     (setf *logical-import-commit-test-injected* t))
    ((and (member *logical-import-commit-test-cut* '(:extra-child :extra-child-after-rename))
          (eq point (if (eq *logical-import-commit-test-cut* :extra-child)
                        :after-child-data-sync :after-child-control-rename)))
     (let* ((certificate (logical-slot-copy-import-decode
                          (read-bounded-regular-file
                           (merge-pathnames "logical-slot-copy.import" *pgdata*)
                           +logical-slot-copy-import-maximum-bytes+)))
            (segment (1+ (floor (getf certificate :child-end) +wal-segment-size+)))
            (path (wal-segment-path segment (getf certificate :child-timeline) :timeline)))
       (with-open-file (stream path :direction :output :if-exists :error
                                    :element-type '(unsigned-byte 8))
         (write-sequence (wal-encode-record +wal-record-batch-start+ 0 (* segment +wal-segment-size+) #())
                         stream)
         (fsync-stream stream)))
     (setf *logical-import-commit-test-injected* t))
    ((and (eq *logical-import-commit-test-cut* :corrupt-source-extent)
          (eq point :before-child-control-rename))
     (let* ((store *physical-receive-store*)
            (tuple (physical-store-control-tuple store))
            (directory (physical-store-retained-directory store))
            (index (physical-retained-inventory-read-index
                    directory (physical-store-retained-root store)
                    (physical-store-retained-identity store) (physical-store-retained-history store)))
            (entry (first (getf index :entries)))
            (path (physical-retained-object-path directory :extent (first entry))))
       (assert entry)
       (with-open-file (stream path :direction :io :if-exists :overwrite
                                    :element-type '(unsigned-byte 8))
         (let ((byte (read-byte stream)))
           (file-position stream 0)
           (write-byte (logxor byte 1) stream)
           (fsync-stream stream)))
       (assert (equal tuple (physical-store-control-tuple store))))
     (setf *logical-import-commit-test-injected* t))
    ((and (eq *logical-import-commit-test-cut* :corrupt-control-temp)
          (eq point :after-child-control-file-fsync))
     (let ((path (pgdata-resolve-path "global/pg_wal_control.tmp" *pgdata*)))
       (with-open-file (stream path :direction :io :if-exists :overwrite
                                    :element-type '(unsigned-byte 8))
         (let ((byte (read-byte stream)))
           (file-position stream 0)
           (write-byte (logxor byte 1) stream)
           (fsync-stream stream))))
     (setf *logical-import-commit-test-injected* t))
    ((and (eq *logical-import-commit-test-cut* :owner-exit)
          (eq point :after-import-child-control))
     (setf *logical-import-commit-test-injected* t)
     (error "child commit owner exit cut"))))

(defun logical-import-commit-test-binding ()
  (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
    (sb-thread:with-mutex (*timeline-history-lock*)
      (let ((saved-owner nil))
        (call-with-logical-slot-copy-committed-input
         (lambda (owner context certificate evidence descriptor manifest control plan status)
           (declare (ignore context evidence descriptor manifest))
           (assert (logical-slot-copy-committed-owner-p owner))
           (assert (not (logical-slot-copy-import-owner-p owner)))
           (assert (eq :pending (getf certificate :phase)))
           (assert (= 3 (getf control :phase)))
           (assert (= (getf plan :end) (getf certificate :child-end)))
           (assert (eq status (if *logical-import-commit-test-omit-control-fact*
                                  :requires-source-replay :control-fact-checked)))
           (setf saved-owner owner)))
        (assert (not (physical-adoption-owner-active-p saved-owner)))
        (handler-case
            (progn (physical-directory-check-owner saved-owner)
                   (error "retired committed owner was accepted"))
          (error (condition)
            (assert (not (search "retired committed owner was accepted"
                                 (princ-to-string condition))))))))))

(defun logical-import-commit-test-source ()
  (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
    (sb-thread:with-mutex (*timeline-history-lock*)
      (call-with-logical-slot-copy-committed-source
       (lambda (owner context store certificate plan fact history)
         (declare (ignore context))
         (assert (logical-slot-copy-committed-owner-p owner))
         (assert (physical-store-p store))
         (assert (= (getf plan :fork) (getf certificate :fork)))
         (assert (< (getf certificate :base-end) (getf certificate :fork)))
         (assert (physical-logical-fact-at-p
                  fact (getf certificate :fork) (getf certificate :parent-timeline)))
         (assert history)))
      (format t "~&LOGICAL-IMPORT-COMMITTED-SOURCE-PASS~%")
      (finish-output))))

(defun logical-import-commit-test-cold-source ()
  (assert (and (not *wal-ready*) (null *physical-recovery-state*)
               (null *physical-receive-store*) (null *catalog*) (null *wal-output-stream*)))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (sb-thread:with-mutex (*physical-follow-generation-lock*)
          (logical-import-commit-test-binding)
          (logical-import-commit-test-source)))))
  (assert (and (not *wal-ready*) (null *physical-recovery-state*)
               (null *physical-receive-store*) (null *catalog*) (null *wal-output-stream*)
               (null *clog-stream*) (zerop (hash-table-count *wal-replication-slots*))))
  (format t "~&LOGICAL-IMPORT-COMMITTED-COLD-SOURCE-PASS~%")
  (finish-output))

(defun logical-import-commit-test-source-fault (mode)
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (sb-thread:with-mutex (*physical-follow-generation-lock*)
          (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
            (sb-thread:with-mutex (*timeline-history-lock*)
              (call-with-logical-slot-copy-committed-source
               (lambda (owner context store certificate plan fact history)
                 (declare (ignore owner store history))
                 (ecase mode
                   (:certificate (incf (getf certificate :child-end)))
                   (:plan (incf (getf plan :end)))
                   (:fact (setf (car fact) :mutated))
                   (:rewrite
                    (let* ((path (merge-pathnames "logical-slot-copy.import"
                                                  (physical-image-context-root-path context)))
                           (bytes (read-bounded-regular-file
                                   path +logical-slot-copy-import-maximum-bytes+)))
                      (with-open-file (out path :direction :output :if-exists :supersede
                                                :element-type '(unsigned-byte 8))
                        (write-sequence bytes out)
                        (fsync-stream out)))))))))))))
  (error "committed source fault was accepted"))

(defun logical-import-commit-test-input-fault (mode)
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (sb-thread:with-mutex (*physical-follow-generation-lock*)
          (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
            (sb-thread:with-mutex (*timeline-history-lock*)
              (call-with-logical-slot-copy-committed-input
               (lambda (owner context certificate evidence descriptor manifest control plan status)
                 (declare (ignore owner context certificate evidence descriptor control plan status))
                 (ecase mode
                   (:case
                       (let* ((text (getf manifest :cluster-identity))
                              (position (position-if #'alpha-char-p text))
                              (character (char text position)))
                         (setf (char text position)
                               (if (upper-case-p character)
                                   (char-downcase character) (char-upcase character)))))
                   (:number (setf (getf manifest :end) (float (getf manifest :end) 1d0))))))))))))
  (error "committed input fault was accepted"))

(defun logical-import-commit-test-omit-fact ()
  (let* ((path (physical-recovery-control-path))
         (control (physical-recovery-read-control *pgdata*))
         (compat
           (apply #'physical-recovery-control-image
                  (append
                   (loop for key in '(:phase :system-id :timeline :manifest-digest
                                      :checkpoint :applied :next-xid)
                         collect (getf control key))
                   (loop for key in '(:db-state :state-time :applied-timeline
                                      :history-target :history-digest :promotion-child
                                      :promotion-generation :promotion-source-namespace
                                      :retained-reference :resource-fact :fpw-fact
                                      :promotion-full-page-writes
                                      :promotion-logical-decoding-enabled :promotion-wal-level)
                         when (get-properties control (list key))
                           append (list key (getf control key)))))))
    (assert (getf control :logical-fact))
    (assert (null (getf (physical-recovery-decode-control compat) :logical-fact)))
    (with-open-file (out path :direction :output :if-exists :supersede
                              :element-type '(unsigned-byte 8))
      (write-sequence compat out)
      (fsync-stream out))
    (sb-posix:chmod (namestring path) #o600)
    (delete-file (merge-pathnames "logical-slot-copy.import" *pgdata*))
    (delete-file (merge-pathnames "logical-slot-copy.evidence" *pgdata*))
    (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
      (sb-thread:with-mutex (*timeline-history-lock*)
        (logical-slot-copy-publish-import-pending)))
    (format t "~&LOGICAL-IMPORT-COMMITTED-OPTIONAL-C3-PREPARED~%")
    (finish-output)))

(defun logical-import-commit-test-run ()
  (physical-follow-call-with-controller-blocked
   (lambda ()
     (sb-thread:with-mutex (*physical-shutdown-lock*)
       (sb-thread:with-mutex (*physical-follow-command-lock*)
         (sb-thread:with-mutex (*reload-run-lock*)
           (with-live-operation-quiescence
             (with-physical-follow-controller-publication (obsolete)
               (when *logical-import-commit-test-omit-control-fact*
                 (logical-import-commit-test-omit-fact))
               (let* ((certificate-path (merge-pathnames "logical-slot-copy.import" *pgdata*))
                      (certificate-bytes (read-bounded-regular-file
                                          certificate-path +logical-slot-copy-import-maximum-bytes+))
                      (certificate (logical-slot-copy-import-decode certificate-bytes))
                      (evidence-path (merge-pathnames "logical-slot-copy.evidence" *pgdata*))
                      (evidence (read-bounded-regular-file evidence-path +logical-slot-copy-evidence-maximum-bytes+))
                      (pending (read-bounded-regular-file (physical-recovery-control-path)
                                                          +physical-recovery-control-maximum-bytes+))
                      (end (logical-slot-copy-import-commit-child-control)))
                 (assert (= end (getf certificate :child-end)))
                 (logical-import-commit-test-binding)
                 (logical-import-commit-test-source)
                 (multiple-value-bind (checkpoint durable snapshot child checkpoint-child namespace)
                     (wal-read-control-fields)
                   (assert (= end checkpoint durable))
                   (assert (= child checkpoint-child (getf certificate :child-timeline)))
                   (assert (eq :timeline namespace))
                   (assert (= (getf snapshot :start)
                              (getf (getf (getf (logical-slot-copy-import-evidence-decode evidence) :plan)
                                          :snapshot) :start))))
                 (physical-promotion-check-child-control
                  (getf certificate :child-timeline) (getf certificate :child-timeline))
                 (assert (equalp certificate-bytes (read-bounded-regular-file
                                                    certificate-path +logical-slot-copy-import-maximum-bytes+)))
                 (assert (equalp evidence (read-bounded-regular-file
                                           evidence-path +logical-slot-copy-evidence-maximum-bytes+)))
                 (assert (equalp pending (read-bounded-regular-file
                                          (physical-recovery-control-path) +physical-recovery-control-maximum-bytes+)))
                 (assert (recovery-in-progress-p))
                 (assert (zerop (hash-table-count *wal-replication-slots*)))
                 (assert (null (slot-state-map (wal-replication-slot-directory) #'list :stabilize nil :local-workspace t)))
                 (logical-import-native-rejects #'physical-promotion-publish-child-control
                                                "child-owner import before promotion")
                 (format t "~&LOGICAL-IMPORT-LIVE-CHILD-CONTROL-PASS ~d~%" end)
                 (finish-output))))))))))

(defun logical-import-commit-test-start ()
  (when (and *logical-import-commit-test-thread*
             (sb-thread:thread-alive-p *logical-import-commit-test-thread*))
    (error "logical child commit test is already running"))
  (setf *logical-import-commit-test-result* :running
        *logical-import-commit-test-thread*
        (sb-thread:make-thread
         (lambda ()
           (let ((*physical-promotion-test-hook* #'logical-import-commit-test-point)
                 (*logical-slot-copy-verification-test-hook*
                   (lambda (point context)
                     (declare (ignore context))
                     (logical-import-commit-test-point point))))
             (handler-case
                 (progn (logical-import-commit-test-run)
                        (setf *logical-import-commit-test-result* :passed))
               (error (condition)
                 (format t "~&LOGICAL-IMPORT-LIVE-CHILD-ERROR ~a~%" condition)
                 (finish-output)
                 (setf *logical-import-commit-test-result* (list :error (princ-to-string condition)))))))
         :name "logical import child commit test"))
  :started)

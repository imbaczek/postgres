(in-package #:pg)

(defun cursor-unit-append-maintenance ()
  (with-wal-critical
    (wal-append-batch-start)
    (let ((end (wal-append-record +wal-record-maintenance+ 0
                                  (make-array 0 :element-type '(unsigned-byte 8)))))
      (wal-flush-core)
      end)))

(defun call-with-owned-cursor-test (function)
  "Use the real reset logger and production metadata for the fixture catalog.
The shared slot fixture mocks the logger and leaves control metadata unset."
  (let ((real-log-reset (symbol-function 'wal-log-clog-reset-under-lock)))
    (call-with-logical-slot-creation-test
     (lambda ()
       (let ((*wal-control-metadata*
               (make-wal-control-metadata (catalog-system-identifier) +db-in-production+))
             (mock-log-reset (symbol-function 'wal-log-clog-reset-under-lock)))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-log-clog-reset-under-lock) real-log-reset)
                (create-standard-logical-slot "cursor_source" :nothing)
                (let* ((slot (gethash "cursor_source" *wal-replication-slots*))
                       (start (wal-replication-slot-confirmed-flush-lsn slot))
                       (middle (cursor-unit-append-maintenance))
                       (end (cursor-unit-append-maintenance)))
                  (call-with-logical-replication-slot
                   "cursor_source" (lambda (owned) (funcall function owned start middle end)))))
           (setf (symbol-function 'wal-log-clog-reset-under-lock) mock-log-reset)))))))

(rove:deftest logical-wal-owned-cursor-keeps-horizon-across-real-clog-reset
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (declare (ignore middle))
       (let ((escaped nil) (before *wal-storage-incarnation*))
         (call-with-logical-slot-wal-cursor
          "cursor_source" start end (wal-replication-slot-confirmed-timeline slot) :prefix-end
          (lambda (cursor)
            (setf escaped cursor)
            (rove:ok (= 1 (hash-table-count *wal-snapshot-retention-pins*)))
            (clog-reset)
            (rove:ok (not (eq before *wal-storage-incarnation*)))
            (rove:ok (> *wal-durable-lsn* end))
            (let ((records (loop for record = (logical-wal-cursor-next cursor)
                                 while record collect record)))
              (rove:ok (equal (list +wal-record-batch-start+ +wal-record-maintenance+
                                    +wal-record-batch-start+ +wal-record-maintenance+)
                              (mapcar #'wal-record-type records)))
              (rove:ok (= end (wal-record-end (car (last records))))))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))
         (rove:ok (rove:signals (logical-wal-cursor-next escaped) 'error))))))

(rove:deftest logical-wal-owned-cursor-rejects-unproven-geometric-starts
    (dolist (failure '(:interior :other-boundary :owner :role))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore slot))
         (let ((entered nil))
           (rove:ok
            (rove:signals
             (call-with-logical-slot-wal-cursor
              "cursor_source" (case failure (:interior (1+ start)) (:other-boundary middle) (t start))
              end (if (eq failure :owner) 2 1) (if (eq failure :role) :record-start :prefix-end)
              (lambda (cursor) (declare (ignore cursor)) (setf entered t)))
             'error))
           (rove:ok (not entered))
           (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*))))))))

(rove:deftest logical-wal-owned-cursor-requires-exact-pending-restart-evidence
    (dolist (failure '(nil :missing :swapped :owner :role))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore start))
         (let ((entered nil))
           (sb-thread:with-mutex (*wal-lock*)
             (setf (wal-replication-slot-restart-lsn slot) middle
                   (wal-replication-slot-restart-timeline slot) 1
                   (wal-replication-slot-confirmed-flush-lsn slot) end)
             (unless (eq failure :missing)
               (setf (gethash 17 (wal-replication-slot-pending-xids slot))
                     (cons (if (eq failure :swapped) (1+ middle) middle) end)))
             (wal-save-replication-slot slot))
           (rove:ok
            (eq (null failure)
                (handler-case
                    (progn
                      (call-with-logical-slot-wal-cursor
                       "cursor_source" middle end (if (eq failure :owner) 2 1)
                       (if (eq failure :role) :prefix-end :record-start)
                       (lambda (cursor)
                         (setf entered t)
                         (rove:ok (= +wal-record-batch-start+
                                     (wal-record-type (logical-wal-cursor-next cursor))))
                         (rove:ok (= +wal-record-maintenance+
                                     (wal-record-type (logical-wal-cursor-next cursor))))
                         (rove:ok (null (logical-wal-cursor-next cursor)))))
                      t)
                  (error () nil))))
           (rove:ok (eq (null failure) entered))
           (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*))))))))

(rove:deftest logical-wal-owned-cursor-fences-source-and-slot-changes
    (dolist (change '(:registry :pin :owner :database :plugin :profile :catalog :birth
                      :timeline :namespace :root :control :horizon :role))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore middle))
         (call-with-logical-slot-wal-cursor
          "cursor_source" start end 1 :prefix-end
          (lambda (cursor)
            (flet ((reject () (rove:ok (rove:signals (logical-wal-cursor-next cursor) 'error))))
              (ecase change
                (:registry
                 (setf (gethash "cursor_source" *wal-replication-slots*)
                       (wal-copy-replication-slot-with-profile slot))
                 (reject))
                (:pin (clrhash *wal-snapshot-retention-pins*) (reject))
                (:owner (setf (wal-replication-slot-active-owner slot) (gensym)) (reject))
                (:database (incf (wal-replication-slot-database-oid slot)) (reject))
                (:plugin (setf (wal-replication-slot-plugin slot) "pg_lisp") (reject))
                (:profile (setf (gethash slot *wal-slot-profiles*) :binary-bytea) (reject))
                (:catalog (let ((*catalog* (copy-catalog *catalog*))) (reject)))
                (:birth (incf (wal-replication-slot-consistent-lsn slot)) (reject))
                (:timeline (let ((*wal-insert-timeline* 2)) (reject)))
                (:namespace (let ((*wal-segment-namespace* :timeline)) (reject)))
                (:root (let ((*pgdata* (uiop:pathname-parent-directory-pathname *pgdata*))) (reject)))
                (:control (let ((*wal-control-metadata* (list :system-id -1))) (reject)))
                (:horizon (let ((*wal-durable-lsn* (1- end))) (reject)))
                (:role (let ((*physical-recovery-state* '(:cursor-test-standby))) (reject)))))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))))))

(rove:deftest logical-wal-owned-cursor-cleans-pins-after-nonlocal-exit
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (declare (ignore slot middle))
       (let ((escaped nil))
         (rove:ok
          (eq :aborted
              (catch :cursor-abort
                (call-with-logical-slot-wal-cursor
                 "cursor_source" start end 1 :prefix-end
                 (lambda (cursor) (setf escaped cursor) (throw :cursor-abort :aborted))))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))
         (rove:ok (rove:signals (logical-wal-cursor-next escaped) 'error))))))

(rove:deftest logical-wal-owned-cursor-rejects-mismatched-persisted-control
    (dolist (failure '(:timeline :namespace :horizon :missing :corrupt))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore slot middle))
         (call-with-logical-slot-wal-cursor
          "cursor_source" start end 1 :prefix-end
          (lambda (cursor)
            (let* ((fields (multiple-value-list (wal-read-control-fields)))
                   (bytes (wal-encode-control-image
                           (first fields) (if (eq failure :horizon) (1- end) (second fields))
                           (third fields)
                           :timeline (if (eq failure :timeline) 2 (fourth fields))
                           :checkpoint-timeline (fifth fields)
                           :namespace (if (eq failure :namespace)
                                          :timeline (sixth fields))
                           :metadata (seventh fields))))
              (if (eq failure :missing)
                  (delete-file (wal-control-path))
                  (progn
                    (when (eq failure :corrupt)
                      (setf (aref bytes 0) (logxor 1 (aref bytes 0))))
                    (wal-atomic-write-bytes (wal-control-path) bytes)))
              (rove:ok (rove:signals (logical-wal-cursor-next cursor) 'error))
              (rove:ok (= start (logical-wal-cursor-position cursor))))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))))))

(defun call-with-cursor-provider-file (bytes function)
  (let* ((directory (uiop:ensure-directory-pathname
                     (sb-posix:mkdtemp
                      (namestring (merge-pathnames "cursor-provider-XXXXXX" (uiop:temporary-directory))))))
         (path (merge-pathnames "carrier" directory)))
    (unwind-protect
         (progn
           (with-open-file (stream path :direction :output :if-exists :error
                                        :element-type '(unsigned-byte 8))
             (write-sequence bytes stream))
           (funcall function path))
      (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(defun call-with-cursor-provider-input (path function)
  (let ((parent nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf parent (physical-backup-open-directory (uiop:pathname-directory-pathname path)))
             (sb-sys:with-local-interrupts
               (call-with-physical-image-input-at
                parent (file-namestring path)
                (lambda (stream)
                  (funcall function stream (sb-posix:stat-size (sb-posix:fstat (sb-sys:fd-stream-fd stream)))))
                :element-type '(unsigned-byte 8))))
        (when parent (sb-posix:close parent))))))

(cl-spec:defproperty logical-wal-provider-preserves-owned-record-bytes
    ((payload-size (range integer 1 512)))
  (:about call-with-logical-wal-cursor)
  (:trials (:normal 50))
  (let* ((raw (wal-encode-record +wal-record-maintenance+ 1 0
                                 (make-array payload-size :element-type '(unsigned-byte 8)
                                                          :initial-element 42)))
         (*wal-lock* (sb-thread:make-mutex :name "cursor provider property"))
         (history (timeline-parse-history 1 "")))
    (call-with-cursor-provider-file
     raw
     (lambda (path)
       (call-with-logical-wal-cursor
        history :timeline 0 (length raw) 1 :record-start (lambda () t)
        (lambda (cursor)
          (multiple-value-bind (record bytes owner carrier kind) (logical-wal-cursor-next cursor)
            (and record (equalp raw bytes) (= 1 owner carrier)
                 (eq :ordinary kind) (= (length raw) (wal-record-end record))
                 (null (logical-wal-cursor-next cursor)))))
        :segment-reader
        (lambda (segment carrier read)
          (assert (= 0 segment))
          (assert (= 1 carrier))
          (call-with-cursor-provider-input path read)))))))

(rove:deftest logical-wal-provider-generated-byte-preservation
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-wal-provider-preserves-owned-record-bytes :seed 2321))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 50 (getf data :trials)))))

(rove:deftest logical-wal-provider-never-falls-back-and-keeps-reader-scoped
    (let* ((*wal-lock* (sb-thread:make-mutex :name "cursor provider scope"))
           (history (timeline-parse-history 1 ""))
           (raw (wal-encode-record +wal-record-maintenance+ 1 0
                                   (make-array 1 :element-type '(unsigned-byte 8) :initial-element 42)))
           (escaped nil) (held nil) (calls 0))
      (call-with-cursor-provider-file
       raw
       (lambda (path)
         (call-with-logical-wal-cursor
          history :timeline 0 (length raw) 1 :record-start (lambda () t)
          (lambda (cursor)
            (rove:ok (logical-wal-cursor-next cursor))
            (rove:ok (not (open-stream-p held)))
            (rove:ok (handler-case (progn (funcall escaped nil 0) nil) (error () t))))
          :segment-reader
          (lambda (segment carrier read)
            (assert (= 0 segment)) (assert (= 1 carrier))
            (incf calls) (setf escaped read)
            (rove:ok
             (eq :rejected
                 (sb-thread:join-thread
                  (sb-thread:make-thread
                   (lambda () (handler-case (funcall read nil 0) (error () :rejected)))))))
            (rove:ok
             (handler-case
                 (let ((*wal-lock* (sb-thread:make-mutex)))
                   (sb-thread:with-mutex (*wal-lock*) (funcall read nil 0)) nil)
               (error () t)))
            (with-open-file (stream path :element-type '(unsigned-byte 8))
              (setf held stream)
              (funcall read stream (file-length stream)))))))
      (rove:ok (= 1 calls))
      (rove:ok (handler-case (progn (funcall escaped nil 0) nil) (error () t)))
      (call-with-logical-wal-cursor
       history :timeline 0 (length raw) 1 :record-start (lambda () t)
       (lambda (cursor)
         (rove:ok (handler-case (progn (logical-wal-cursor-next cursor) nil)
                    (error (condition) (search "provider-only failure" (princ-to-string condition)))))
         (rove:ok (zerop (logical-wal-cursor-position cursor))))
       :segment-reader (lambda (&rest ignored) (declare (ignore ignored)) (error "provider-only failure")))))

(rove:deftest logical-wal-provider-requires-one-read-within-valid-bounds
    (let* ((*wal-lock* (sb-thread:make-mutex :name "cursor provider bounds"))
           (history (timeline-parse-history 1 ""))
           (raw (wal-encode-record +wal-record-maintenance+ 1 0
                                   (make-array 1 :element-type '(unsigned-byte 8) :initial-element 42))))
      (call-with-cursor-provider-file
       raw
       (lambda (path)
         (dolist (mode '(:omit :twice :negative :oversized :short))
           (call-with-logical-wal-cursor
            history :timeline 0 (length raw) 1 :record-start (lambda () t)
            (lambda (cursor)
              (rove:ok (handler-case (progn (logical-wal-cursor-next cursor) nil) (error () t)))
              (rove:ok (zerop (logical-wal-cursor-position cursor))))
            :segment-reader
            (lambda (segment carrier read)
              (declare (ignore segment carrier))
              (unless (eq mode :omit)
                (with-open-file (stream path :element-type '(unsigned-byte 8))
                  (funcall read stream (case mode (:negative -1) (:oversized (1+ +wal-segment-size+))
                                             (:short (1- (length raw))) (t (length raw))))
                  (when (eq mode :twice) (funcall read stream (length raw))))))))))))

(rove:deftest logical-wal-provider-rejects-invalid-sources-before-reading
    (let ((history (timeline-parse-history 1 "")) (calls 0))
      (dolist (pair (list (list 42 nil)
                          (list (lambda (&rest args) (declare (ignore args)) (incf calls)) :archive)))
        (let ((caught nil))
          (handler-case
              (call-with-logical-wal-cursor
               history :timeline 0 0 1 :record-start (lambda () t) #'identity
               :segment-reader (first pair) :archive (second pair))
            (error (condition) (setf caught condition)))
          (rove:ok (and caught (search "invalid logical WAL cursor scope" (princ-to-string caught))))))
      (rove:ok (zerop calls))))

(rove:deftest logical-wal-provider-rechecks-source-before-progress
    (let* ((*wal-lock* (sb-thread:make-mutex :name "cursor provider source"))
           (allowed t)
           (raw (wal-encode-record +wal-record-maintenance+ 1 0
                                   (make-array 1 :element-type '(unsigned-byte 8) :initial-element 42))))
      (call-with-cursor-provider-file
       raw
       (lambda (path)
         (call-with-logical-wal-cursor
          (timeline-parse-history 1 "") :timeline 0 (length raw) 1 :record-start
          (lambda () allowed)
          (lambda (cursor)
            (rove:ok (handler-case (progn (logical-wal-cursor-next cursor) nil) (error () t)))
            (rove:ok (zerop (logical-wal-cursor-position cursor))))
          :segment-reader
          (lambda (segment carrier read)
            (declare (ignore segment carrier))
            (with-open-file (stream path :element-type '(unsigned-byte 8))
              (funcall read stream (file-length stream))
              (setf allowed nil))))))))

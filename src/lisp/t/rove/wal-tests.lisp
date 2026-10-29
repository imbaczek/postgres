;;;; wal-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-wal-catalog-codec-and-page-checksums
    (let* ((catalog (make-array 17 :element-type '(unsigned-byte 8)
                                   :initial-element 65))
           (payload (wal-encode-catalog-payload catalog '(123 456))))
      (multiple-value-bind (deletes decoded)
          (wal-decode-catalog-payload payload)
        (rove:ok (equal deletes '(123 456)) "WAL catalog delete list round-trips")
        (rove:ok (equalp decoded catalog) "WAL catalog image round-trips"))
      (let ((corrupt (copy-seq payload)))
        (decf (aref corrupt 12))
        (rove:ok (handler-case
                     (progn (wal-decode-catalog-payload corrupt) nil)
                   (error () t))
                 "WAL catalog image length mismatch is rejected")))
  (let ((page (page-init (make-page))))
    (rove:ok (wal-verify-page page 123 4) "unstamped page is accepted for staging")
    (wal-stamp-page page 99 123 4)
    (rove:ok (wal-verify-page page 123 4) "WAL page checksum verifies at its location")
    (rove:ok (not (wal-verify-page page 123 5))
             "WAL page checksum binds the block number")
    (setf (aref page 100) (logxor (aref page 100) #xFF))
    (rove:ok (not (wal-verify-page page 123 4))
             "WAL page checksum detects page damage")))

(rove:deftest test-wal-record-codec
    (let* ((payload (make-array 0 :element-type '(unsigned-byte 8)))
           (record (wal-encode-record +wal-record-commit+ 42 64 payload))
           (decoded (wal-decode-record record 64)))
      (rove:ok (= (wal-record-type decoded) +wal-record-commit+)
               "WAL record type round-trips")
      (rove:ok (= (wal-record-xid decoded) 42) "WAL record XID round-trips")
      (rove:ok (= (wal-record-start decoded) 64) "WAL record LSN round-trips")
      (rove:ok (= (wal-record-end decoded) (+ 64 +wal-header-size+))
               "WAL record end LSN is computed")
      (rove:ok (equalp (wal-record-payload decoded) payload)
               "WAL record payload round-trips")
      (dolist (case
                  (list
                   (cons "bad WAL record magic is rejected"
                         (lambda (bytes) (setf (aref bytes 0) 0)))
                   (cons "unsupported WAL record version is rejected"
                         (lambda (bytes) (wal-set-u16 bytes 4 1)))
                   (cons "unknown WAL record type is rejected"
                         (lambda (bytes) (wal-set-u16 bytes 6 #xFFFF)))
                   (cons "bad WAL record length is rejected"
                         (lambda (bytes) (wal-set-u32 bytes 8 31)))
                   (cons "WAL record at the wrong LSN is rejected"
                         (lambda (bytes) (wal-set-u64 bytes 12 65)))
                   (cons "bad WAL record checksum is rejected"
                         (lambda (bytes)
                           (setf (aref bytes 20) (logxor (aref bytes 20) #xFF))))))
        (let ((corrupt (copy-seq record)))
          (funcall (cdr case) corrupt)
          (rove:ok (handler-case
                       (progn (wal-decode-record corrupt 64) nil)
                     (error () t))
                   (car case))))))

(rove:deftest test-wal-page-image-xid-codec-compatibility
    (let* ((lsn 4096)
           (page (page-init (make-page)))
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page page
                                       :xids '(7201 7202)))
           (payload (wal-encode-page-images (list image) lsn))
           (decoded (first (wal-decode-page-images payload lsn +wal-version+)))
           (no-xid-image (make-wal-page-image
                          :relfilenode 7311 :block 4 :page (page-init (make-page))))
           (one-xid-image (make-wal-page-image
                           :relfilenode 7312 :block 5 :page (page-init (make-page))
                           :xids '(7203)))
           (multi-payload
             (wal-encode-page-images (list image no-xid-image one-xid-image) lsn))
           (multi-decoded
             (wal-decode-page-images multi-payload lsn +wal-version+))
           (legacy-v3 (make-array (+ 4 12 +blcksz+)
                                  :element-type '(unsigned-byte 8)
                                  :initial-element 0))
           (legacy-v2 (make-array (+ 4 8 +blcksz+)
                                  :element-type '(unsigned-byte 8)
                                  :initial-element 0)))
      (rove:ok (equal (wal-page-image-xids decoded) '(7201 7202))
               "WAL v4 page images retain transaction contributors")
      (rove:ok (= (length payload) (+ 4 12 +blcksz+ 4 (* 8 2)))
               "WAL v4 page-image length includes variable contributor data")
      (rove:ok (equal (mapcar #'wal-page-image-xids multi-decoded)
                      '((7201 7202) nil (7203)))
               "WAL v4 multi-page records retain each image's contributor list")
      (wal-set-u32 legacy-v3 0 1)
      (wal-set-u32 legacy-v3 4 7310)
      (wal-set-u32 legacy-v3 8 0)
      (wal-set-u32 legacy-v3 12 3)
      (replace legacy-v3 page :start1 16)
      (wal-set-u32 legacy-v2 0 1)
      (wal-set-u32 legacy-v2 4 7310)
      (wal-set-u32 legacy-v2 8 3)
      (replace legacy-v2 page :start1 12)
      (rove:ok (null (wal-page-image-xids
                      (first (wal-decode-page-images legacy-v3 lsn +wal-version-v3+))))
               "WAL v3 page images remain readable without contributor metadata")
      (rove:ok (null (wal-page-image-xids
                      (first (wal-decode-page-images legacy-v2 lsn +wal-version-v2+))))
               "WAL v2 page images remain readable without fork metadata")))

(rove:deftest test-wal-recovery-page-spool-preflight-and-order
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-spool-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-temp-file-limit-bytes* 0)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (original (symbol-function 'wal-apply-page-images))
           (writes nil)
           (terminal nil)
           (first (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (second (make-wal-page-image :relfilenode 7310 :block 3
                                        :page (let ((page (page-init (make-page))))
                                                (setf (aref page 100) 42) page)))
           (one (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                 :payload (wal-encode-page-images (list first) 100)))
           (two (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 200
                                 :payload (wal-encode-page-images (list second) 200)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore split-p fault-on-first-write))
                     (dolist (image images)
                       (push (list (wal-page-image-block image)
                                   (aref (wal-page-image-page image) 100)) writes))))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record one)
               (wal-replay-record two)
               (rove:ok (null *wal-recovery-page-images*))
               (rove:ok (= 1 (length (uiop:directory-files directory))))
               (wal-replay-finish-batch (lambda () (lambda () (setf terminal t))))
               (rove:ok (equal (nreverse writes) '((3 0) (3 42))))
               (rove:ok terminal)
               (rove:ok (null (uiop:directory-files directory))))
             (setf writes nil terminal nil)
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record one)
               (wal-replay-record two)
               (let* ((spool *wal-recovery-page-spool*)
                      (file (logical-spool-stream spool))
                      (offset (+ 8 12 (length (wal-record-payload one)) 8 12 20)))
                 (finish-output file)
                 (file-position file offset)
                 (write-byte 99 file)
                 (finish-output file))
               (rove:ok
                (handler-case
                    (progn
                      (wal-replay-finish-batch (lambda () (lambda () (setf terminal t))))
                      nil)
                  (error (condition) (search "checksum mismatch" (princ-to-string condition)))))
               (rove:ok (null writes))
               (rove:ok (null terminal)))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'wal-apply-page-images) original)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-failure-cleanup
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-fault-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (original (symbol-function 'wal-apply-page-images))
           (writes nil)
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (payload (wal-encode-page-images (list image) 100))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload payload))
           (bad (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 200
                                 :payload payload))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore images split-p fault-on-first-write))
                     (setf writes t)))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record page)
               (rove:ok (handler-case
                            (progn (wal-replay-finish-batch (lambda () (error "invalid terminal"))) nil)
                          (error (condition) (search "invalid terminal" (princ-to-string condition)))))
               (rove:ok (null writes)))
             (rove:ok (null (uiop:directory-files directory)))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record page)
               (rove:ok (handler-case (progn (wal-replay-record bad) nil) (error () t)))
               (rove:ok (null writes)))
             (rove:ok (null (uiop:directory-files directory)))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record page)
               (wal-replay-record start)
               (rove:ok (null (uiop:directory-files directory)))
               (wal-replay-finish-batch (lambda () #'values))
               (rove:ok (null writes)))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'wal-apply-page-images) original)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-replay-fault-cleanup
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-replay-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (old-map (symbol-function 'logical-spool-map-events))
           (old-apply (symbol-function 'wal-apply-page-images))
           (calls 0) (writes nil) (terminal nil)
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload (wal-encode-page-images (list image) 100)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-spool-map-events)
                   (lambda (spool function)
                     (incf calls)
                     (when (= calls 2) (error "injected replay failure"))
                     (funcall old-map spool function))
                   (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore images split-p fault-on-first-write))
                     (setf writes t)))
             (rove:ok
              (handler-case
                  (with-wal-replay-staging ()
                    (wal-replay-record start)
                    (wal-replay-record page)
                    (wal-replay-finish-batch (lambda () (lambda () (setf terminal t))))
                    nil)
                (error (condition) (search "injected replay failure" (princ-to-string condition)))))
             (rove:ok (= calls 2))
             (rove:ok (null writes))
             (rove:ok (null terminal))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'logical-spool-map-events) old-map
              (symbol-function 'wal-apply-page-images) old-apply)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-cleanup-failure
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-cleanup-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-temp-pending-cleanups* nil)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (unlink (symbol-function 'unlink-query-temp-file))
           (apply-pages (symbol-function 'wal-apply-page-images))
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload (wal-encode-page-images (list image) 100)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore images split-p fault-on-first-write))))
             (rove:ok
              (handler-case
                  (with-wal-replay-staging ()
                    (wal-replay-record start)
                    (wal-replay-record page)
                    (logical-spool-seal *wal-recovery-page-spool*)
                    (call-with-logical-spool-owner
                     *wal-recovery-page-spool*
                     (lambda ()
                       (finish-query-temp-stream
                        (logical-spool-stream *wal-recovery-page-spool*) t)))
                    (setf (symbol-function 'unlink-query-temp-file)
                          (lambda (path) (declare (ignore path)) (error "injected unlink failure")))
                    (wal-recovery-close-page-spool)
                    nil)
                (error (condition) (search "recovery page spool cleanup failed"
                                           (princ-to-string condition)))))
             (rove:ok (= 1 (getf (query-temp-pending-cleanup-status) :records)))
             (rove:ok (= 1 (length (uiop:directory-files directory))))
             (setf (symbol-function 'unlink-query-temp-file) unlink)
             (retry-pending-query-temp-cleanup)
             (rove:ok (zerop (getf (query-temp-pending-cleanup-status) :records)))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'unlink-query-temp-file) unlink
              (symbol-function 'wal-apply-page-images) apply-pages)
        (retry-pending-query-temp-cleanup)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-spool-private-namespace-cleanup
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-namespace-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory)
           (*query-temp-directory* nil)
           (*query-temp-instance-directory* nil)
           (*query-temp-root-directory* nil)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (apply-pages (symbol-function 'wal-apply-page-images))
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload (wal-encode-page-images (list image) 100)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore images split-p fault-on-first-write))))
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record page)
               (wal-replay-finish-batch (lambda () #'values))
               (rove:ok (null *wal-recovery-page-spool*)))
             (rove:ok (zerop (getf (query-temp-pending-cleanup-status) :records))))
        (setf (symbol-function 'wal-apply-page-images) apply-pages)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-cleanup-error-still-closes-logical-spool
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-mixed-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (original (symbol-function 'close-logical-spool))
           (page-spool nil)
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (page (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                  :payload (wal-encode-page-images (list image) 100)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10))
           (event (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "value"))))
           (part (make-wal-record :type +wal-record-logical-changes+ :xid 88 :end 150
                                  :payload (logical-encode-fragment
                                            0 2 (make-logical-fragment
                                                 :event event :start 0 :end (floor (length event) 2))))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (rove:ok
              (handler-case
                  (with-wal-replay-staging ()
                    (wal-replay-record start)
                    (wal-replay-record page)
                    (wal-replay-record part)
                    (setf page-spool *wal-recovery-page-spool*)
                    (rove:ok (= 2 (length (uiop:directory-files directory))))
                    (setf (symbol-function 'close-logical-spool)
                          (lambda (spool)
                            (if (eq spool page-spool)
                                (error "injected page close failure")
                                (funcall original spool))))
                    nil)
                (error (condition) (search "injected page close failure" (princ-to-string condition)))))
             (rove:ok (= 1 (length (uiop:directory-files directory)))))
        (setf (symbol-function 'close-logical-spool) original)
        (when page-spool (close-logical-spool page-spool))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-recovery-page-late-corruption-restarts-cleanly
    (let* ((directory (merge-pathnames
                       (format nil "pg-redo-retry-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-recovery-page-spool-enabled* t)
           (*wal-checkpoint-lsn* 0)
           (old-map (symbol-function 'logical-spool-map-events))
           (old-apply (symbol-function 'wal-apply-page-images))
           (calls 0) (writes nil) (terminal nil)
           (first (make-wal-page-image :relfilenode 7310 :block 3 :page (page-init (make-page))))
           (second (make-wal-page-image :relfilenode 7310 :block 4 :page (page-init (make-page))))
           (one (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 100
                                 :payload (wal-encode-page-images (list first) 100)))
           (two (make-wal-record :type +wal-record-pages+ :version +wal-version+ :end 200
                                 :payload (wal-encode-page-images (list second) 200)))
           (start (make-wal-record :type +wal-record-batch-start+ :xid 0 :payload #() :end 10)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-apply-page-images)
                   (lambda (images &key split-p fault-on-first-write)
                     (declare (ignore split-p fault-on-first-write))
                     (dolist (image images) (push (wal-page-image-block image) writes)))
                   (symbol-function 'logical-spool-map-events)
                   (lambda (spool function)
                     (incf calls)
                     (when (= calls 2)
                       (let ((file (logical-spool-stream spool)))
                         (file-position file (+ 8 12 (length (wal-record-payload one)) 8 12 20))
                         (write-byte 99 file)
                         (finish-output file)))
                     (funcall old-map spool function)))
             (rove:ok
              (handler-case
                  (with-wal-replay-staging ()
                    (wal-replay-record start)
                    (wal-replay-record one)
                    (wal-replay-record two)
                    (wal-replay-finish-batch (lambda () (lambda () (setf terminal t))))
                    nil)
                (error (condition) (search "checksum mismatch" (princ-to-string condition)))))
             (rove:ok (equal writes '(3)))
             (rove:ok (null terminal))
             (rove:ok (null (uiop:directory-files directory)))
             (setf (symbol-function 'logical-spool-map-events) old-map
                   writes nil)
             (with-wal-replay-staging ()
               (wal-replay-record start)
               (wal-replay-record one)
               (wal-replay-record two)
               (wal-replay-finish-batch (lambda () (lambda () (setf terminal t)))))
             (rove:ok (equal (nreverse writes) '(3 4)))
             (rove:ok terminal)
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'logical-spool-map-events) old-map
              (symbol-function 'wal-apply-page-images) old-apply)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-with-wal-critical-validation-errors-do-not-poison
    (let ((*wal-poisoned* nil))
      (rove:ok (handler-case
                   (progn
                     (with-wal-critical
                       (wal-invalid-input "invalid test input"))
                     nil)
                 (wal-input-validation-error () t))
               "WAL input validation errors propagate through the critical macro")
      (rove:ok (not *wal-poisoned*)
               "WAL input validation errors do not poison the critical section")
      (rove:ok (eq :healthy
                   (with-wal-critical :healthy))
               "the critical macro releases its lock after an input validation error")))

(rove:deftest test-wal-drop-missing-slot-does-not-poison-wal
    (let ((*wal-ready* t)
          (*wal-poisoned* nil)
          (*wal-replication-slots* (make-hash-table :test #'equal)))
      (rove:ok (handler-case
                   (progn (wal-drop-replication-slot "missing_slot") nil)
                 (error () t))
               "dropping a missing slot reports an error")
      (rove:ok (not *wal-poisoned*)
               "a missing slot does not poison WAL or request fail-stop shutdown")))

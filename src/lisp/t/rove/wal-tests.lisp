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

(rove:deftest test-wal-primary-chunked-pages-match-legacy
    (let* ((directory (merge-pathnames
                       (format nil "pg-primary-pages-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-temp-file-limit-bytes* 0)
           (*logical-capture-memory-bytes* 1)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (old-start (symbol-function 'wal-next-record-start))
           (old-append (symbol-function 'wal-append-record))
           (position 100) (records nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (loop for block from 512 downto 0
                   for key = (wal-page-key 7310 block)
                   do (setf (gethash key *wal-page-cache*) (page-init (make-page))
                            (gethash key *wal-page-cache-xids*)
                            (if (evenp block) '(1 44 45) '(0 1 55))))
             (let ((excluded (wal-page-key 7311 0)))
               (setf (gethash excluded *wal-page-cache*) (page-init (make-page))))
             (setf (symbol-function 'wal-next-record-start)
                   (lambda (length) (declare (ignore length)) position)
                   (symbol-function 'wal-append-record)
                   (lambda (type xid payload)
                     (declare (ignore xid))
                     (incf position (+ +wal-header-size+ (length payload)))
                     (push (list type position payload) records)
                     position))
             (let* ((legacy (wal-page-images '(7311)))
                    (keys (wal-primary-page-keys '(7311))))
               (wal-append-page-images legacy)
               (let ((expected (nreverse records)))
                 (setf records nil position 100)
                 (with-wal-primary-page-spool (spool)
                   (wal-primary-preflight-page-keys keys)
                   (wal-primary-append-pages keys spool)
                   (wal-primary-seal-page-spool spool)
                   (rove:ok (= 2 (length records)))
                   (rove:ok (equalp expected (nreverse records)))
                   (let ((count 0) (last-block -1))
                     (wal-primary-map-page-spool
                      spool
                      (lambda (images)
                        (dolist (image images)
                          (incf count)
                          (rove:ok (> (wal-page-image-block image) last-block))
                          (setf last-block (wal-page-image-block image)))))
                     (rove:ok (= 513 count)))))
               (rove:ok (null (uiop:directory-files directory)))))
        (setf (symbol-function 'wal-next-record-start) old-start
              (symbol-function 'wal-append-record) old-append)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-primary-split-pause-once-across-chunks
    (let* ((directory (merge-pathnames
                       (format nil "pg-primary-split-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 1)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-test-pause-relfilenode* 7311)
           (old-start (symbol-function 'wal-next-record-start))
           (old-append (symbol-function 'wal-append-record))
           (old-write (symbol-function 'smgr-write))
           (old-pause (symbol-function 'wal-test-pause))
           (position 100) (writes 0) (pauses 0))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (loop for block below 512
                   do (setf (gethash (wal-page-key 7310 block) *wal-page-cache*)
                            (page-init (make-page))))
             (loop for block from 1 to 514
                   do (setf (gethash (wal-page-key 7311 block) *wal-page-cache*)
                            (page-init (make-page))))
             (setf (symbol-function 'wal-next-record-start)
                   (lambda (length) (declare (ignore length)) position)
                   (symbol-function 'wal-append-record)
                   (lambda (type xid payload)
                     (declare (ignore type xid))
                     (incf position (+ +wal-header-size+ (length payload))))
                   (symbol-function 'smgr-write)
                   (lambda (node block page &optional fork)
                     (declare (ignore node block page fork))
                     (incf writes))
                   (symbol-function 'wal-test-pause)
                   (lambda (point)
                     (when (eq point :after-btree-split-page-write) (incf pauses))))
             (let ((keys (wal-primary-page-keys)))
               (with-wal-primary-page-spool (spool)
                 (wal-primary-preflight-page-keys keys)
                 (wal-primary-append-pages keys spool)
                 (wal-primary-seal-page-spool spool)
                 (wal-primary-apply-page-spool spool :split-p t :fault-on-first-write t)))
             (rove:ok (= 1026 writes))
             (rove:ok (= 1 pauses))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'wal-next-record-start) old-start
              (symbol-function 'wal-append-record) old-append
              (symbol-function 'smgr-write) old-write
              (symbol-function 'wal-test-pause) old-pause)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-primary-page-contributor-retention
    (let* ((*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (*wal-replication-slots* (make-hash-table :test #'equal))
           (*active-xids* '(44 55 66))
           (slot (make-wal-replication-slot :name "retained"))
           (one (wal-page-key 7310 0))
           (two (wal-page-key 7310 1)))
      (setf (gethash "retained" *wal-replication-slots*) slot
            (gethash one *wal-page-cache*) (page-init (make-page))
            (gethash two *wal-page-cache*) (page-init (make-page))
            (gethash one *wal-page-cache-xids*) '(1 44 55)
            (gethash two *wal-page-cache-xids*) '(0 1 55 77)
            (gethash 44 *wal-xid-first-lsn*) 10)
      (wal-primary-record-page-xids (wal-primary-page-keys) 100)
      (rove:ok (= 10 (gethash 44 *wal-xid-first-lsn*)))
      (rove:ok (= 100 (gethash 55 *wal-xid-first-lsn*)))
      (rove:ok (null (gethash 77 *wal-xid-first-lsn*)))
      (rove:ok (equal '(10) (gethash 44 (wal-replication-slot-pending-xids slot))))
      (rove:ok (equal '(100) (gethash 55 (wal-replication-slot-pending-xids slot))))
      (rove:ok (null (gethash 66 (wal-replication-slot-pending-xids slot))))))

(rove:deftest test-wal-primary-empty-page-spool
    (let* ((directory (merge-pathnames
                       (format nil "pg-primary-empty-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (visits 0))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (with-wal-primary-page-spool (spool)
               (let ((keys (wal-primary-page-keys)))
                 (wal-primary-preflight-page-keys keys)
                 (wal-primary-append-pages keys spool)
                 (wal-primary-seal-page-spool spool)
                 (wal-primary-map-page-spool spool
                                             (lambda (images) (declare (ignore images)) (incf visits)))))
             (rove:ok (zerop visits))
             (rove:ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-primary-spool-corruption-fails-stop-before-and-after-commit
    (let* ((directory (merge-pathnames
                       (format nil "pg-primary-corrupt-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*logical-capture-memory-bytes* 0)
           (*wal-ready* t)
           (*wal-output-stream* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-replication-slots* (make-hash-table :test #'equal))
           (*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (*active-xids* nil)
           (names '(wal-append-running-xacts-under-lock wal-append-batch-start
                    wal-next-record-start wal-append-record wal-flush
                    wal-slot-record-xid-commit-under-wal-lock logical-spool-map-events
                    smgr-write wal-fail-stop))
           (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
           (old-map (symbol-function 'logical-spool-map-events)))
      (ensure-directories-exist directory)
      (setf (gethash (wal-page-key 7310 0) *wal-page-cache*) (page-init (make-page)))
      (unwind-protect
           (dolist (corrupt-pass '(1 2))
             (let ((*wal-poisoned* nil) (position 100) (calls 0) (records nil)
                                        (flushed nil) (callback nil) (writes 0))
               (setf (symbol-function 'wal-append-running-xacts-under-lock)
                     (lambda (xid) (declare (ignore xid)))
                     (symbol-function 'wal-append-batch-start) (lambda () 100)
                     (symbol-function 'wal-next-record-start)
                     (lambda (length) (declare (ignore length)) position)
                     (symbol-function 'wal-append-record)
                     (lambda (type xid payload)
                       (declare (ignore xid))
                       (push type records)
                       (incf position (+ +wal-header-size+ (length payload))))
                     (symbol-function 'wal-flush) (lambda () (setf flushed t))
                     (symbol-function 'wal-slot-record-xid-commit-under-wal-lock)
                     (lambda (xid lsn) (declare (ignore xid lsn)))
                     (symbol-function 'smgr-write)
                     (lambda (node block page &optional fork)
                       (declare (ignore node block page fork)) (incf writes))
                     (symbol-function 'wal-fail-stop) (lambda (condition) (error condition))
                     (symbol-function 'logical-spool-map-events)
                     (lambda (spool function)
                       (incf calls)
                       (when (= calls corrupt-pass)
                         (let ((file (logical-spool-stream spool)))
                           (file-position file (+ 8 12 20))
                           (write-byte 99 file)
                           (finish-output file)))
                       (funcall old-map spool function)))
               (rove:ok (handler-case
                            (progn (wal-commit-transaction 44 (lambda () (setf callback t))) nil)
                          (error (condition) (search "checksum mismatch" (princ-to-string condition)))))
               (rove:ok *wal-poisoned*)
               (rove:ok (zerop writes))
               (rove:ok (= 1 (hash-table-count *wal-page-cache*)))
               (rove:ok (eql (= corrupt-pass 2) flushed))
               (rove:ok (eql (= corrupt-pass 2) callback))
               (rove:ok (eql (= corrupt-pass 2)
                             (not (null (member +wal-record-commit+ records)))))
               (rove:ok (null (uiop:directory-files directory)))))
        (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

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

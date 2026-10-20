;;;; wal-tests.lisp -- write-ahead log tests.

(in-package #:pg)

(defun test-wal-catalog-codec-and-page-checksums ()
  (let* ((catalog (make-array 17 :element-type '(unsigned-byte 8)
                                 :initial-element 65))
         (payload (wal-encode-catalog-payload catalog '(123 456))))
    (multiple-value-bind (deletes decoded)
        (wal-decode-catalog-payload payload)
      (check (equal deletes '(123 456)) "WAL catalog delete list round-trips")
      (check (equalp decoded catalog) "WAL catalog image round-trips"))
    (let ((corrupt (copy-seq payload)))
      (decf (aref corrupt 12))
      (check (handler-case
                 (progn (wal-decode-catalog-payload corrupt) nil)
               (error () t))
             "WAL catalog image length mismatch is rejected")))
  (let ((page (page-init (make-page))))
    (check (wal-verify-page page 123 4) "unstamped page is accepted for staging")
    (wal-stamp-page page 99 123 4)
    (check (wal-verify-page page 123 4) "WAL page checksum verifies at its location")
    (check (not (wal-verify-page page 123 5))
           "WAL page checksum binds the block number")
    (setf (aref page 100) (logxor (aref page 100) #xFF))
    (check (not (wal-verify-page page 123 4))
           "WAL page checksum detects page damage")))

(defun test-wal-record-codec ()
  (let* ((payload (make-array 0 :element-type '(unsigned-byte 8)))
         (record (wal-encode-record +wal-record-commit+ 42 64 payload))
         (decoded (wal-decode-record record 64)))
    (check (= (wal-record-type decoded) +wal-record-commit+)
           "WAL record type round-trips")
    (check (= (wal-record-xid decoded) 42) "WAL record XID round-trips")
    (check (= (wal-record-start decoded) 64) "WAL record LSN round-trips")
    (check (= (wal-record-end decoded) (+ 64 +wal-header-size+))
           "WAL record end LSN is computed")
    (check (equalp (wal-record-payload decoded) payload)
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
        (check (handler-case
                   (progn (wal-decode-record corrupt 64) nil)
                 (error () t))
               (car case))))))

(defun test-wal-chunked-catalog-record ()
  (let* ((test-pgdata (merge-pathnames "wal-catalog-chunks-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-ready* t)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-pending-deletes* '())
         (*wal-pending-btree-split* nil)
         (*wal-test-pause-at* nil)
         (catalog-bytes
           (make-array (+ (- +wal-segment-size+ +wal-header-size+) 1)
                       :element-type '(unsigned-byte 8)
                       :initial-element 65))
         (catalog-written nil)
         (record-types '()))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (unwind-protect
         (progn
           (wal-commit-catalog
            catalog-bytes
            (lambda (bytes)
              (setf catalog-written (equalp bytes catalog-bytes))))
           (check catalog-written
                  "oversized catalog image is published after WAL flush")
           (wal-scan (lambda (record)
                       (push (wal-record-type record) record-types)))
           (setf record-types (nreverse record-types))
           (check (and (= (count +wal-record-catalog-chunk+ record-types) 2)
                       (= (car (last record-types))
                          +wal-record-catalog-commit+))
                  "oversized catalog image uses chunks and a terminal record")
           (let ((*wal-recovery-group-active* nil)
                 (*wal-recovery-page-images* '())
                 (*wal-recovery-catalog-chunks* '())
                 (*wal-recovery-catalog-chunk-count* nil)
                 (*wal-recovery-catalog-next-chunk* 0))
             (wal-scan #'wal-replay-record))
           (with-open-file (stream (merge-pathnames "global/pg_catalog.dat"
                                                    *pgdata*)
                                   :direction :input
                                   :element-type '(unsigned-byte 8))
             (let ((recovered (make-array (file-length stream)
                                          :element-type '(unsigned-byte 8))))
               (read-sequence recovered stream)
               (check (equalp recovered catalog-bytes)
                      "chunked catalog WAL reassembles during recovery")))
           (let ((*wal-recovery-catalog-chunks*
                   (list (make-array 1 :element-type '(unsigned-byte 8))))
                 (*wal-recovery-catalog-chunk-count* 1)
                 (*wal-recovery-catalog-next-chunk* 1))
             (check (handler-case
                        (progn
                          (wal-reassemble-catalog-chunks 1 #xffffffffffffffff)
                          nil)
                      (error () t))
                    "catalog chunk length mismatch fails before allocation")))
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*))))))

(defun test-wal-catalog-terminal-validation-precedes-page-redo ()
  (let* ((test-pgdata
           (merge-pathnames "wal-invalid-catalog-terminal-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*relation-streams* (make-hash-table :test #'equal))
         (*wal-ready* t)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (relfilenode 424242)
         (page-before (page-init (make-page)))
         (page-after (let ((page (page-init (make-page))))
                       (setf (aref page 100) #x5A)
                       page))
         (images (list (make-wal-page-image :relfilenode relfilenode
                                            :block 0 :page page-after)))
         (catalog-chunk
           (make-array 1 :element-type '(unsigned-byte 8) :initial-element 65))
         (batch-bytes
           (wal-encode-record +wal-record-batch-start+ 0 0 #()))
         (batch-record (wal-decode-record batch-bytes 0))
         (pages-start (wal-record-end batch-record))
         (pages-end (+ pages-start +wal-header-size+
                       (wal-page-payload-length images)))
         (pages-payload (wal-encode-page-images images pages-end))
         (pages-bytes
           (wal-encode-record +wal-record-pages+ 0 pages-start pages-payload))
         (pages-record (wal-decode-record pages-bytes pages-start))
         (chunk-start (wal-record-end pages-record))
         (chunk-payload (wal-encode-catalog-chunk-payload
                         catalog-chunk 0 1))
         (chunk-bytes
           (wal-encode-record +wal-record-catalog-chunk+ 0 chunk-start
                              chunk-payload))
         (chunk-record (wal-decode-record chunk-bytes chunk-start))
         (terminal-start (wal-record-end chunk-record))
         (terminal-payload
           (wal-encode-catalog-commit-payload
            (make-array 2 :element-type '(unsigned-byte 8) :initial-element 65)
            1))
         (terminal-bytes
           (wal-encode-record +wal-record-catalog-commit+ 0 terminal-start
                              terminal-payload))
         (terminal-record (wal-decode-record terminal-bytes terminal-start))
         (end-lsn (wal-record-end terminal-record)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (merge-pathnames "base/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (ensure-directories-exist (relation-file-path relfilenode))
    (smgr-write relfilenode 0 page-before)
    (with-open-file (stream (wal-segment-path 0)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (dolist (bytes (list batch-bytes pages-bytes chunk-bytes terminal-bytes))
        (write-sequence bytes stream)))
    (setf *wal-durable-lsn* end-lsn)
    (wal-save-control-fields 0 end-lsn)
    (unwind-protect
         (progn
           ;; Each record was decoded after encoding, so its CRC is valid. The
           ;; terminal declares two catalog bytes although its only chunk has
           ;; one; recovery must reject it before writing the staged page.
           (let ((error-message
                   (handler-case
                       (progn (wal-recover) nil)
                     (error (condition) (princ-to-string condition)))))
             (check (and error-message
                         (search "catalog chunks do not match their declared length"
                                 error-message))
                    "malformed chunked catalog terminal fails recovery")
             (check (equalp (smgr-read relfilenode 0) page-before)
                    "invalid catalog terminal leaves relation pages unchanged")))
      (smgr-close-all)
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-segment-rotation ()
  (let* ((test-pgdata (merge-pathnames "wal-segment-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (directory-synced-on-rotation nil)
         (original-fsync-directory (symbol-function 'fsync-directory)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-directory)
                 (lambda (directory)
                   (when (eql *wal-output-segment* 0)
                     (setf directory-synced-on-rotation t))
                   (funcall original-fsync-directory directory)))
           (wal-append-record
            +wal-record-commit+ 42
            (make-array (- +wal-segment-size+ +wal-header-size+)
                        :element-type '(unsigned-byte 8)
                        :initial-element 0))
           (wal-append-record +wal-record-checkpoint+ 0 #())
           (check directory-synced-on-rotation
                  "WAL rotation syncs segment names before opening the next segment")
           (wal-flush)
           (let ((records 0))
             (multiple-value-bind (end-lsn segments)
                 (wal-scan (lambda (record)
                             (declare (ignore record))
                             (incf records)))
               (check (= records 2) "WAL scanner reads records across a segment boundary")
               (check (= end-lsn *wal-end-lsn*) "WAL scan ends at the last segment LSN")
               (check (equal segments '(0 1)) "WAL scan discovers contiguous segments")
               (wal-save-checkpoint-control end-lsn)
               (wal-remove-checkpointed-segments end-lsn)
               (check (not (probe-file (wal-segment-path 0)))
                      "checkpoint recycling removes older WAL segments")
               (check (probe-file (wal-segment-path 1))
                      "checkpoint recycling retains the checkpoint segment")
               (multiple-value-bind (retained-end retained-segments)
                   (wal-scan (lambda (record) (declare (ignore record))))
                 (check (= retained-end end-lsn)
                        "WAL recovery scans from the retained checkpoint segment")
                 (check (equal retained-segments '(1))
                        "WAL recovery accepts a retained checkpoint suffix"))))
           (setf (symbol-function 'fsync-directory) original-fsync-directory)
           (when *wal-output-stream*
             (ignore-errors (close *wal-output-stream*)))))))

(defun test-wal-checkpoint-skips-obsolete-prefix ()
  (let* ((test-pgdata (merge-pathnames "wal-checkpoint-scan-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-checkpoint-lsn* (+ (* 2 +wal-segment-size+)
                                  (* 2 +wal-header-size+)))
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+)
                               (* 3 +wal-header-size+)))
         (prefix (wal-encode-record +wal-record-commit+ 1 0 #()))
         (same-segment-prefix
           (wal-encode-record +wal-record-commit+ 3
                              (* 2 +wal-segment-size+) #()))
         (checkpoint-start (+ (* 2 +wal-segment-size+) +wal-header-size+))
         (checkpoint (wal-encode-record +wal-record-checkpoint+ 0
                                        checkpoint-start #()))
         (post-checkpoint (wal-encode-record
                           +wal-record-commit+ 2
                           (+ checkpoint-start +wal-header-size+) #()))
         (obsolete-bytes (copy-seq prefix))
         (same-segment-obsolete-bytes (copy-seq same-segment-prefix)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    ;; The gap at segment 1 and damage in segment 0 are both before the durable
    ;; checkpoint, so neither should prevent scanning the retained suffix.
    (setf (aref obsolete-bytes 28) (logxor (aref obsolete-bytes 28) #xFF))
    (setf (aref same-segment-obsolete-bytes 28)
          (logxor (aref same-segment-obsolete-bytes 28) #xFF))
    (with-open-file (stream (wal-segment-path 0)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-sequence obsolete-bytes stream))
    (with-open-file (stream (wal-segment-path 2)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-sequence same-segment-obsolete-bytes stream)
      (write-sequence checkpoint stream)
      (write-sequence post-checkpoint stream))
    (multiple-value-bind (end-lsn segments)
        (wal-scan (lambda (record) (declare (ignore record))))
      (check (= end-lsn *wal-durable-lsn*)
             "checkpoint scan advances through the retained WAL suffix")
      (check (equal segments '(2))
             "checkpoint scan ignores obsolete segments and gaps"))
    ;; The checkpoint itself and all records after it remain subject to normal
    ;; integrity checks.
    (with-open-file (stream (wal-segment-path 2)
                            :direction :io
                            :element-type '(unsigned-byte 8)
                            :if-exists :overwrite)
      (file-position stream (+ +wal-header-size+ 28))
      (write-byte (logxor (aref checkpoint 28) #xFF) stream))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "WAL checksum mismatch" (princ-to-string condition))))
           "checkpoint scan validates the checkpoint record")
    (with-open-file (stream (wal-segment-path 2)
                            :direction :io
                            :element-type '(unsigned-byte 8)
                            :if-exists :overwrite)
      (file-position stream +wal-header-size+)
      (write-sequence checkpoint stream)
      (file-position stream (+ (* 2 +wal-header-size+) 28))
      (write-byte (logxor (aref post-checkpoint 28) #xFF) stream))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "WAL checksum mismatch" (princ-to-string condition))))
           "checkpoint scan validates records after the checkpoint")))

(defun test-wal-missing-interior-segment ()
  (let* ((test-pgdata (merge-pathnames "wal-gap-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-checkpoint-lsn* +wal-header-size+)
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+) +wal-header-size+)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (dolist (segment '(0 1 2))
      (let* ((start (* segment +wal-segment-size+))
             (type (if (zerop segment)
                       +wal-record-checkpoint+
                       +wal-record-commit+))
             (record (wal-encode-record type 0 start #())))
        (with-open-file (stream (wal-segment-path segment)
                                :direction :output
                                :element-type '(unsigned-byte 8)
                                :if-exists :supersede
                                :if-does-not-exist :create)
          (write-sequence record stream))))
    (multiple-value-bind (end-lsn segments)
        (wal-scan (lambda (record) (declare (ignore record))))
      (check (= end-lsn *wal-durable-lsn*)
             "WAL scan reads a complete retained segment chain")
      (check (equal segments '(0 1 2))
             "WAL scan sees all segments before a missing-segment check"))
    (delete-file (wal-segment-path 1))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "missing WAL segment" (princ-to-string condition))))
           "WAL recovery rejects a missing interior segment")))

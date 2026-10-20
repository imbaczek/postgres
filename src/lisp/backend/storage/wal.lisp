;;;; wal.lisp -- small physical WAL with full-page after-images.

(in-package #:pg)

(defconstant +wal-segment-size+ (* 16 1024 1024))
(defconstant +wal-header-size+ 32)
(defconstant +wal-max-page-images-per-record+ 512)
(defconstant +wal-record-pages+ 1)
(defconstant +wal-record-commit+ 2)
(defconstant +wal-record-catalog+ 3)
(defconstant +wal-record-checkpoint+ 4)
(defconstant +wal-record-clog-reset+ 5)
(defconstant +wal-record-batch-start+ 6)
(defconstant +wal-record-catalog-chunk+ 7)
(defconstant +wal-record-catalog-commit+ 8)
(defconstant +wal-catalog-chunk-header-size+ 8)
(defconstant +wal-version+ 2)

(defstruct wal-record
  type xid start end payload)

(defstruct wal-page-image
  relfilenode block page)

(defvar *wal-lock* (sb-thread:make-mutex :name "pg wal and page cache lock"))
(defvar *wal-page-cache* (make-hash-table :test #'equal))
(defvar *wal-page-batch* nil)
(defvar *wal-page-batch-split* nil)
(defvar *wal-pending-btree-split* nil)
(defvar *wal-pending-deletes* '())
(defvar *wal-poisoned* nil)
(defvar *wal-end-lsn* 0)
(defvar *wal-checkpoint-lsn* 0)
(defvar *wal-durable-lsn* 0)
(defvar *wal-ready* nil)
(defvar *wal-output-stream* nil)
(defvar *wal-output-segment* nil)
(defvar *wal-new-segments* '())
(defvar *wal-test-pause-at* nil)
(defvar *wal-test-pause-relfilenode* nil)
(defvar *wal-recovery-group-active* nil)
(defvar *wal-recovery-page-images* '())
(defvar *wal-recovery-catalog-chunks* '())
(defvar *wal-recovery-catalog-chunk-count* nil)
(defvar *wal-recovery-catalog-next-chunk* 0)

(declaim (ftype function wal-test-pause
                wal-remove-checkpointed-segments wal-atomic-write-bytes
                clog-set clog-reset clog-fsync))
(declaim (notinline pg-class-relfilenode pg-index-relfilenode
                    database-tables database-indexes catalog-databases))

(defun wal-directory ()
  (merge-pathnames "pg_wal/" *pgdata*))

(defun wal-parent-directory (directory)
  (let* ((path (pathname directory))
         (parts (pathname-directory path))
         (parent-parts (if (and parts
                                (not (eq (car (last parts)) :absolute)))
                           (butlast parts)
                           parts)))
    (if (equal parts parent-parts)
        path
        (make-pathname :directory parent-parts
                       :name nil :type nil :version nil
                       :defaults path))))

(defun wal-ensure-directory-durable (directory)
  "Create DIRECTORY's path one level at a time and sync each parent entry."
  (let* ((path (pathname directory))
         (parent (wal-parent-directory path)))
    (unless (equal path parent)
      (wal-ensure-directory-durable parent)
      (ensure-directories-exist (merge-pathnames "wal-directory-dummy" path))
      (fsync-directory parent))
    path))

(defun wal-segment-path (segment)
  (merge-pathnames (format nil "~16,'0X.wal" segment) (wal-directory)))

(defun wal-control-path ()
  (merge-pathnames "global/pg_wal_control" *pgdata*))

(defun wal-u16 (bytes offset)
  (logior (aref bytes offset)
          (ash (aref bytes (+ offset 1)) 8)))

(defun wal-u32 (bytes offset)
  (logior (aref bytes offset)
          (ash (aref bytes (+ offset 1)) 8)
          (ash (aref bytes (+ offset 2)) 16)
          (ash (aref bytes (+ offset 3)) 24)))

(defun wal-u64 (bytes offset)
  (logior (wal-u32 bytes offset)
          (ash (wal-u32 bytes (+ offset 4)) 32)))

(defun wal-set-u16 (bytes offset value)
  (setf (aref bytes offset) (logand value #xFF)
        (aref bytes (+ offset 1)) (logand (ash value -8) #xFF))
  value)

(defun wal-set-u32 (bytes offset value)
  (setf (aref bytes offset) (logand value #xFF)
        (aref bytes (+ offset 1)) (logand (ash value -8) #xFF)
        (aref bytes (+ offset 2)) (logand (ash value -16) #xFF)
        (aref bytes (+ offset 3)) (logand (ash value -24) #xFF))
  value)

(defun wal-set-u64 (bytes offset value)
  (wal-set-u32 bytes offset (logand value #xFFFFFFFF))
  (wal-set-u32 bytes (+ offset 4) (logand (ash value -32) #xFFFFFFFF))
  value)

(defun make-wal-crc32-table ()
  (let ((table (make-array 256 :element-type '(unsigned-byte 32))))
    (dotimes (i 256 table)
      (let ((crc i))
        (loop repeat 8
              do (setf crc (if (oddp crc)
                               (logxor (ash crc -1) #xEDB88320)
                               (ash crc -1))))
        (setf (aref table i) (logand crc #xFFFFFFFF))))))

(defvar *wal-crc32-table* (make-wal-crc32-table))

(defun wal-crc32 (bytes &optional (start 0) (end (length bytes)))
  (let ((crc #xFFFFFFFF))
    (loop for i from start below end
          for slot = (logand (logxor crc (aref bytes i)) #xFF)
          do (setf crc (logxor (ash crc -8) (aref *wal-crc32-table* slot))))
    (logxor crc #xFFFFFFFF)))

(defun wal-page-checksum (page relfilenode block)
  "Return a location-bound nonzero CRC16 for PAGE, reserving legacy values."
  (let ((copy (make-array (+ 8 (length page))
                          :element-type '(unsigned-byte 8)
                          :initial-element 0)))
    (page-set-u32 copy 0 relfilenode)
    (page-set-u32 copy 4 block)
    (replace copy page :start1 8)
    (page-set-u16 copy (+ 8 +pd-checksum-off+) 0)
    (let ((checksum (logand (wal-crc32 copy) #xFFFF)))
      (if (<= checksum 3) (+ checksum 4) checksum))))

(defun wal-stamp-page (page lsn relfilenode block)
  (page-set-u64 page +pd-lsn-off+ lsn)
  (page-set-u16 page +pd-checksum-off+ 0)
  (page-set-u16 page +pd-checksum-off+
                (wal-page-checksum page relfilenode block))
  page)

(defun wal-verify-page (page relfilenode block)
  (let ((lsn (page-u64 page +pd-lsn-off+))
        (checksum (page-u16 page +pd-checksum-off+)))
    (if (zerop lsn)
        ;; New pages staged in memory and legacy pages both have no WAL LSN.
        ;; Persisted WAL pages always carry a nonzero LSN and must have a CRC.
        (zerop checksum)
        (= checksum (wal-page-checksum page relfilenode block)))))

(defun wal-page-key (relfilenode block)
  (cons relfilenode block))

(defun wal-copy-dirty-page (page)
  "Copy PAGE without its persisted LSN/checksum until it is WAL-logged."
  (let ((copy (copy-seq page)))
    (page-set-u64 copy +pd-lsn-off+ 0)
    (page-set-u16 copy +pd-checksum-off+ 0)
    copy))

(defun wal-visible-page-under-lock (relfilenode block)
  (let ((key (wal-page-key relfilenode block)))
    (or (and *wal-page-batch* (gethash key *wal-page-batch*))
        (gethash key *wal-page-cache*)
        (smgr-read relfilenode block))))

(defmacro with-wal-page-state-lock (&body body)
  "Use the current page batch, or lock the shared WAL page state."
  `(if *wal-page-batch*
       (progn ,@body)
       (sb-thread:with-mutex (*wal-lock*)
         ,@body)))

(defun wal-buffer-read (relfilenode block)
  (if (not *wal-ready*)
      (smgr-read relfilenode block)
      (let ((page
              (with-wal-page-state-lock
                (wal-visible-page-under-lock relfilenode block))))
        (when page (copy-seq page)))))

(defun wal-buffer-write (relfilenode block page)
  (unless *wal-ready*
    (return-from wal-buffer-write (smgr-write relfilenode block page)))
  (let ((key (wal-page-key relfilenode block))
        (copy (wal-copy-dirty-page page)))
    (with-wal-page-state-lock
      (setf (gethash key (if *wal-page-batch*
                             *wal-page-batch*
                             *wal-page-cache*))
            copy)))
  (wal-test-pause :after-page-stage)
  (values))

(defun wal-visible-nblocks-under-lock (relfilenode)
  (let ((nblocks (smgr-nblocks relfilenode)))
    (flet ((count-map (map)
             (when map
               (maphash (lambda (key page)
                          (declare (ignore page))
                          (when (eql (car key) relfilenode)
                            (setf nblocks (max nblocks (1+ (cdr key))))))
                        map))))
      (count-map *wal-page-cache*)
      (count-map *wal-page-batch*))
    nblocks))

(defun wal-buffer-nblocks (relfilenode)
  (if (not *wal-ready*)
      (smgr-nblocks relfilenode)
      (with-wal-page-state-lock
        (wal-visible-nblocks-under-lock relfilenode))))

(defun wal-buffer-extend (relfilenode page)
  (if (not *wal-ready*)
      (smgr-extend relfilenode page)
      (with-wal-page-state-lock
        (let ((block (wal-visible-nblocks-under-lock relfilenode)))
          (setf (gethash (wal-page-key relfilenode block)
                         (if *wal-page-batch*
                             *wal-page-batch*
                             *wal-page-cache*))
                (wal-copy-dirty-page page))
          (unless *wal-page-batch*
            (wal-test-pause :after-page-stage))
          block))))

(defmacro with-wal-page-batch (&body body)
  "Stage related page edits privately and publish them together on success."
  `(if *wal-page-batch*
       (progn ,@body)
       (sb-thread:with-mutex (*wal-lock*)
         (let ((*wal-page-batch* (make-hash-table :test #'equal))
               (*wal-page-batch-split* nil))
           (multiple-value-prog1
               (progn ,@body)
             (maphash (lambda (key page)
                        (setf (gethash key *wal-page-cache*) page))
                      *wal-page-batch*)
             (when *wal-page-batch-split*
               (setf *wal-pending-btree-split* t)))))))

(defun wal-mark-btree-split ()
  (when *wal-page-batch*
    (setf *wal-page-batch-split* t))
  (values))

(defun wal-forget-relation (relfilenode)
  (when *wal-ready*
    (sb-thread:with-mutex (*wal-lock*)
      (let ((keys '()))
        (maphash (lambda (key page)
                   (declare (ignore page))
                   (when (eql (car key) relfilenode)
                     (push key keys)))
                 *wal-page-cache*)
        (dolist (key keys) (remhash key *wal-page-cache*)))))
  (values))

(defun wal-page-images (&optional excluded-relfilenodes)
  (let ((images '()))
    (maphash (lambda (key page)
               (unless (member (car key) excluded-relfilenodes :test #'eql)
                 (push (make-wal-page-image :relfilenode (car key)
                                            :block (cdr key)
                                            :page (copy-seq page))
                       images)))
             *wal-page-cache*)
    (sort images
          (lambda (a b)
            (if (= (wal-page-image-relfilenode a)
                   (wal-page-image-relfilenode b))
                (< (wal-page-image-block a) (wal-page-image-block b))
                (< (wal-page-image-relfilenode a)
                   (wal-page-image-relfilenode b)))))))

(defun wal-page-payload-length (images)
  (+ 4 (* (length images) (+ 8 +blcksz+))))

(defun wal-encode-page-images (images lsn)
  (let ((payload (make-array (wal-page-payload-length images)
                             :element-type '(unsigned-byte 8)
                             :initial-element 0))
        (offset 4))
    (wal-set-u32 payload 0 (length images))
    (dolist (image images)
      (wal-stamp-page (wal-page-image-page image) lsn
                      (wal-page-image-relfilenode image)
                      (wal-page-image-block image))
      (wal-set-u32 payload offset (wal-page-image-relfilenode image))
      (wal-set-u32 payload (+ offset 4) (wal-page-image-block image))
      (replace payload (wal-page-image-page image) :start1 (+ offset 8))
      (incf offset (+ 8 +blcksz+)))
    payload))

(defun wal-decode-page-images (payload lsn)
  (when (< (length payload) 4)
    (error "WAL page-image record is too short"))
  (let* ((count (wal-u32 payload 0))
         (expected (+ 4 (* count (+ 8 +blcksz+))))
         (offset 4)
         (images '()))
    (unless (= expected (length payload))
      (error "WAL page-image record has an invalid length"))
    (loop repeat count
          do (let* ((relfilenode (wal-u32 payload offset))
                    (block (wal-u32 payload (+ offset 4)))
                    (page (subseq payload (+ offset 8) (+ offset 8 +blcksz+))))
               (unless (and (= (page-u64 page +pd-lsn-off+) lsn)
                            (wal-verify-page page relfilenode block))
                 (error "WAL page image has an invalid LSN or checksum"))
               (push (make-wal-page-image :relfilenode relfilenode
                                          :block block :page page)
                     images)
               (incf offset (+ 8 +blcksz+))))
    (nreverse images)))

(defun wal-segment-and-offset (lsn)
  (floor lsn +wal-segment-size+))

(defun wal-fail-stop (condition)
  (setf *wal-poisoned* t)
  (when (fboundp 'request-fast-shutdown)
    (ignore-errors (funcall (symbol-function 'request-fast-shutdown))))
  (error condition))

(defun wal-check-healthy ()
  (when *wal-poisoned*
    (error "WAL I/O previously failed; refusing further WAL operations"))
  (values))

(defmacro with-wal-critical (&body body)
  "Run BODY under the WAL lock and request fail-stop shutdown on errors."
  `(handler-case
       (sb-thread:with-mutex (*wal-lock*)
         (handler-case
             (progn
               (wal-check-healthy)
               ,@body)
           (error (condition)
             ;; Publish poison before another WAL operation can acquire the
             ;; mutex; request shutdown after WITH-MUTEX releases it.
             (setf *wal-poisoned* t)
             (error condition))))
     (error (condition)
       (wal-fail-stop condition))))

(defun wal-next-record-start (length)
  (when (> length +wal-segment-size+)
    (error "WAL record of ~a bytes exceeds the segment size" length))
  (multiple-value-bind (segment offset)
      (wal-segment-and-offset *wal-end-lsn*)
    (if (> (+ offset length) +wal-segment-size+)
        (* (1+ segment) +wal-segment-size+)
        *wal-end-lsn*)))

(defun wal-open-output (segment)
  (unless (eql segment *wal-output-segment*)
    (when *wal-output-stream*
      (finish-output *wal-output-stream*)
      ;; Rotation can happen in the middle of a commit batch. Make every
      ;; previous segment and its directory entry durable before opening a
      ;; later segment. The final segment is flushed once the batch's
      ;; commit/catalog record has been appended.
      (fsync-stream *wal-output-stream*)
      (when *wal-new-segments*
        (fsync-directory (wal-directory))
        (setf *wal-new-segments* '()))
      (close *wal-output-stream*)
      (setf *wal-output-stream* nil))
    (let* ((path (wal-segment-path segment))
           (newp (not (probe-file path))))
      (setf *wal-output-stream*
            (open path :direction :output
                       :element-type '(unsigned-byte 8)
                       :if-exists :append :if-does-not-exist :create)
            *wal-output-segment* segment)
      (when newp (pushnew segment *wal-new-segments* :test #'eql))))
  *wal-output-stream*)

(defun wal-encode-record (type xid start payload)
  (let* ((length (+ +wal-header-size+ (length payload)))
         (record (make-array length :element-type '(unsigned-byte 8)
                                    :initial-element 0)))
    (setf (aref record 0) (char-code #\P)
          (aref record 1) (char-code #\G)
          (aref record 2) (char-code #\W)
          (aref record 3) (char-code #\L))
    (wal-set-u16 record 4 +wal-version+)
    (wal-set-u16 record 6 type)
    (wal-set-u32 record 8 length)
    (wal-set-u64 record 12 start)
    (wal-set-u64 record 20 xid)
    (replace record payload :start1 +wal-header-size+)
    (wal-set-u32 record 28 (wal-crc32 record))
    record))

(defun wal-append-record (type xid payload)
  (wal-check-healthy)
  (handler-case
      (let* ((length (+ +wal-header-size+ (length payload)))
             (start (wal-next-record-start length))
             (segment (floor start +wal-segment-size+))
             (offset (mod start +wal-segment-size+))
             (record (wal-encode-record type xid start payload))
             (stream (wal-open-output segment)))
        (unless (= (or (file-position stream) offset) offset)
          (error "WAL segment ~a is not positioned at offset ~a" segment offset))
        (write-sequence record stream)
        (setf *wal-end-lsn* (+ start length))
        *wal-end-lsn*)
    (error (condition)
      (wal-fail-stop condition))))

(defun wal-flush ()
  (wal-check-healthy)
  (handler-case
      (progn
        (when *wal-output-stream*
          (finish-output *wal-output-stream*)
          (fsync-stream *wal-output-stream*))
        (when *wal-new-segments*
          (fsync-directory (wal-directory))
          (setf *wal-new-segments* '()))
        (wal-test-pause :after-wal-bytes-flush)
        ;; Record the durable end only after the WAL bytes and segment names
        ;; have reached stable storage. Recovery uses it to distinguish an
        ;; unflushed tail from damage inside acknowledged WAL.
        (wal-save-durable-lsn *wal-end-lsn*)
        (values))
    (error (condition)
      (wal-fail-stop condition))))

(defun wal-append-page-images (images)
  (loop for start from 0 below (length images)
        by +wal-max-page-images-per-record+
        for stop = (min (length images)
                        (+ start +wal-max-page-images-per-record+))
        for chunk = (subseq images start stop)
        for length = (+ +wal-header-size+ (wal-page-payload-length chunk))
        for lsn = (+ (wal-next-record-start length) length)
        for payload = (wal-encode-page-images chunk lsn)
        do (unless (= lsn (wal-append-record +wal-record-pages+ 0 payload))
             (error "WAL page-image LSN changed while appending")))
  images)

(defun wal-append-batch-start ()
  (wal-append-record +wal-record-batch-start+ 0
                     (make-array 0 :element-type '(unsigned-byte 8))))

(defun wal-apply-page-images (images &key split-p fault-on-first-write)
  (let ((paused nil))
    (loop for image in images
          do (smgr-write (wal-page-image-relfilenode image)
                         (wal-page-image-block image)
                         (wal-page-image-page image))
             (when (and (not paused)
                        fault-on-first-write split-p
                        (or (and *wal-test-pause-relfilenode*
                                 (eql *wal-test-pause-relfilenode*
                                      (wal-page-image-relfilenode image))
                                 (plusp (wal-page-image-block image)))
                            (null *wal-test-pause-relfilenode*)))
               (setf paused t)
               (wal-test-pause :after-btree-split-page-write))))
  (values))

(defun wal-clear-page-cache (&optional excluded-relfilenodes)
  (let ((keys '()))
    (maphash (lambda (key page)
               (declare (ignore page))
               (when (or (null excluded-relfilenodes)
                         (member (car key) excluded-relfilenodes :test #'eql))
                 (push key keys)))
             *wal-page-cache*)
    (dolist (key keys) (remhash key *wal-page-cache*)))
  (values))

(defun wal-commit-transaction (xid after-durable-function)
  "Flush every dirty after-image and XID's commit record as one WAL boundary."
  (unless *wal-ready*
    (funcall after-durable-function)
    (return-from wal-commit-transaction (values)))
  (with-wal-critical
    (let* ((split-p *wal-pending-btree-split*)
           (images (wal-page-images)))
      (wal-append-batch-start)
      (wal-append-page-images images)
      ;; This hook leaves complete page records in the file without a
      ;; terminal record, so recovery must discard this entire batch.
      (when *wal-output-stream* (finish-output *wal-output-stream*))
      (wal-test-pause :after-page-images)
      (wal-append-record +wal-record-commit+ xid
                         (make-array 0 :element-type '(unsigned-byte 8)))
      (wal-flush)
      (wal-test-pause :after-wal-flush)
      (funcall after-durable-function)
      (wal-apply-page-images images :split-p split-p
                                    :fault-on-first-write t)
      (wal-clear-page-cache)
      (setf *wal-pending-btree-split* nil)))
  (values))

;; TODO: Stream catalog serialization into a temporary file and write bounded
;; WAL chunks from it. Recovery should spool chunks, validate terminal metadata,
;; then publish from the spool; this path currently holds several full-image
;; byte arrays in memory.
(defun wal-encode-catalog-payload (bytes deletes)
  (let* ((payload (make-array (+ 8 (* 4 (length deletes)) (length bytes))
                              :element-type '(unsigned-byte 8)
                              :initial-element 0))
         (offset 0))
    (wal-set-u32 payload offset (length deletes))
    (incf offset 4)
    (dolist (relfilenode deletes)
      (wal-set-u32 payload offset relfilenode)
      (incf offset 4))
    (wal-set-u32 payload offset (length bytes))
    (incf offset 4)
    (replace payload bytes :start1 offset)
    payload))

(defun wal-decode-catalog-payload (payload)
  (when (< (length payload) 8)
    (error "WAL catalog record is too short"))
  (let* ((count (wal-u32 payload 0))
         (offset 4))
    (when (> (+ offset (* count 4) 4) (length payload))
      (error "WAL catalog record has an invalid delete list"))
    (let ((deletes '()))
      (loop repeat count
            do (push (wal-u32 payload offset) deletes)
               (incf offset 4))
      (setf deletes (nreverse deletes))
      (let ((catalog-length (wal-u32 payload offset)))
        (incf offset 4)
        (unless (= (+ offset catalog-length) (length payload))
          (error "WAL catalog record has an invalid catalog image length (offset ~a, image ~a, payload ~a)"
                 offset catalog-length (length payload)))
        (values deletes (subseq payload offset))))))

(defun wal-encode-catalog-chunk-payload (bytes index count)
  (let ((payload (make-array (+ +wal-catalog-chunk-header-size+
                                (length bytes))
                             :element-type '(unsigned-byte 8)
                             :initial-element 0)))
    (wal-set-u32 payload 0 index)
    (wal-set-u32 payload 4 count)
    (replace payload bytes :start1 +wal-catalog-chunk-header-size+)
    payload))

(defun wal-decode-catalog-chunk-payload (payload)
  (when (< (length payload) (1+ +wal-catalog-chunk-header-size+))
    (error "WAL catalog chunk is too short"))
  (let ((index (wal-u32 payload 0))
        (count (wal-u32 payload 4)))
    (unless (and (plusp count) (< index count))
      (error "WAL catalog chunk has an invalid index or count"))
    (values index count
            (subseq payload +wal-catalog-chunk-header-size+))))

(defun wal-catalog-chunk-count (payload)
  (let* ((chunk-size (- +wal-segment-size+ +wal-header-size+
                        +wal-catalog-chunk-header-size+))
         (count (ceiling (length payload) chunk-size)))
    (unless (<= count #xFFFFFFFF)
      (error "WAL catalog image requires too many chunks"))
    count))

(defun wal-encode-catalog-commit-payload (payload chunk-count)
  (let ((commit (make-array 12 :element-type '(unsigned-byte 8)
                               :initial-element 0)))
    (wal-set-u32 commit 0 chunk-count)
    (wal-set-u64 commit 4 (length payload))
    commit))

(defun wal-decode-catalog-commit-payload (payload)
  (unless (= (length payload) 12)
    (error "WAL catalog commit record has an invalid length"))
  (let ((count (wal-u32 payload 0))
        (length (wal-u64 payload 4)))
    (unless (plusp count)
      (error "WAL catalog commit record has an invalid chunk count"))
    (values count length)))

(defun wal-append-catalog-chunks (payload count)
  (let ((chunk-size (- +wal-segment-size+ +wal-header-size+
                       +wal-catalog-chunk-header-size+)))
    (loop for index from 0 below count
          for start = (* index chunk-size)
          for stop = (min (length payload) (+ start chunk-size))
          for chunk = (subseq payload start stop)
          do (wal-append-record
              +wal-record-catalog-chunk+ 0
              (wal-encode-catalog-chunk-payload chunk index count))))
  (values))

(defun wal-reassemble-catalog-chunks (expected-count expected-length)
  (unless (and (eql expected-count *wal-recovery-catalog-chunk-count*)
               (= expected-count *wal-recovery-catalog-next-chunk*)
               (= expected-count (length *wal-recovery-catalog-chunks*)))
    (error "WAL catalog chunk sequence is incomplete"))
  (let* ((chunks (nreverse *wal-recovery-catalog-chunks*))
         (actual-length (reduce #'+ chunks :key #'length :initial-value 0)))
    (unless (= actual-length expected-length)
      (error "WAL catalog chunks do not match their declared length"))
    (let ((payload (make-array actual-length
                               :element-type '(unsigned-byte 8)))
          (offset 0))
      (dolist (chunk chunks)
        (replace payload chunk :start1 offset)
        (incf offset (length chunk)))
      payload)))

(defun wal-defer-relation-delete (relfilenode)
  (when *wal-ready*
    (sb-thread:with-mutex (*wal-lock*)
      (pushnew relfilenode *wal-pending-deletes* :test #'eql)))
  (values))

(defun wal-commit-catalog (catalog-bytes write-function)
  "Durably publish a full catalog image and any deferred relation drops."
  (unless *wal-ready*
    (return-from wal-commit-catalog (funcall write-function catalog-bytes)))
  (with-wal-critical
    (let* ((deletes (sort (copy-list *wal-pending-deletes*) #'<))
           (images (wal-page-images deletes))
           (catalog-payload
             (wal-encode-catalog-payload catalog-bytes deletes))
           (chunked-p
             (> (+ +wal-header-size+ (length catalog-payload))
                +wal-segment-size+))
           (chunk-count (when chunked-p
                          (wal-catalog-chunk-count catalog-payload)))
           (commit-payload (when chunked-p
                             (wal-encode-catalog-commit-payload
                              catalog-payload chunk-count))))
      (wal-append-batch-start)
      (wal-append-page-images images)
      (if chunked-p
          (progn
            (wal-append-catalog-chunks catalog-payload chunk-count)
            (when *wal-output-stream*
              (finish-output *wal-output-stream*))
            (wal-test-pause :after-catalog-chunks)
            (wal-append-record +wal-record-catalog-commit+ 0
                               commit-payload))
          (wal-append-record +wal-record-catalog+ 0 catalog-payload))
      (wal-flush)
      (wal-test-pause :after-catalog-wal-flush)
      (wal-apply-page-images images)
      (funcall write-function catalog-bytes)
      (dolist (relfilenode deletes)
        (smgr-forget relfilenode :delete t))
      (setf *wal-pending-deletes*
            (set-difference *wal-pending-deletes* deletes :test #'eql))
      (when deletes (fsync-directory (merge-pathnames "base/" *pgdata*)))
      (wal-clear-page-cache)
      (setf *wal-pending-btree-split* nil)))
  (values))

(defun wal-discover-segments ()
  (sort
   (loop for path in (directory (merge-pathnames "*.wal" (wal-directory)))
         for name = (pathname-name path)
         when (and name (= (length name) 16)
                   (every (lambda (char)
                            (or (digit-char-p char 16)
                                (and (char>= char #\A) (char<= char #\F))))
                          name))
           collect (parse-integer name :radix 16))
   #'<))

(defun wal-valid-type-p (type)
  (member type (list +wal-record-pages+ +wal-record-commit+
                     +wal-record-catalog+ +wal-record-checkpoint+
                     +wal-record-clog-reset+ +wal-record-batch-start+
                     +wal-record-catalog-chunk+
                     +wal-record-catalog-commit+)))

(defun wal-decode-record (bytes expected-lsn)
  (unless (and (>= (length bytes) +wal-header-size+)
               (= (aref bytes 0) (char-code #\P))
               (= (aref bytes 1) (char-code #\G))
               (= (aref bytes 2) (char-code #\W))
               (= (aref bytes 3) (char-code #\L))
               (= (wal-u16 bytes 4) +wal-version+)
               (= (wal-u32 bytes 8) (length bytes))
               (= (wal-u64 bytes 12) expected-lsn)
               (wal-valid-type-p (wal-u16 bytes 6)))
    (error "invalid WAL record at LSN ~a" expected-lsn))
  (let ((saved-crc (wal-u32 bytes 28))
        (copy (copy-seq bytes)))
    (wal-set-u32 copy 28 0)
    (unless (= saved-crc (wal-crc32 copy))
      (error "WAL checksum mismatch at LSN ~a" expected-lsn)))
  (let* ((type (wal-u16 bytes 6))
         (xid (wal-u64 bytes 20))
         (payload (subseq bytes +wal-header-size+))
         (end (+ expected-lsn (length bytes))))
    (make-wal-record :type type :xid xid :start expected-lsn :end end
                     :payload payload)))

(defun wal-truncate-tail (path offset)
  (with-open-file (stream path :direction :io
                               :element-type '(unsigned-byte 8)
                               :if-exists :overwrite)
    (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) offset)
    (fsync-stream stream)))

(defun wal-truncate-unflushed-tail (segment last-segment offset lsn reason)
  (unless (and (eql segment last-segment)
               (>= lsn *wal-durable-lsn*))
    (error "~a at LSN ~a before durable LSN ~a"
           reason lsn *wal-durable-lsn*))
  (wal-truncate-tail (wal-segment-path segment) offset)
  (values))

(defun wal-scan (function)
  "Visit durable records; truncate only tail data beyond the durable LSN."
  (let* ((segments (wal-discover-segments))
         (last-segment (car (last segments)))
         (checkpoint-record-start
           (if (zerop *wal-checkpoint-lsn*)
               0
               (- *wal-checkpoint-lsn* +wal-header-size+)))
         (checkpoint-segment (floor checkpoint-record-start
                                    +wal-segment-size+))
         (checkpoint-offset (mod checkpoint-record-start +wal-segment-size+))
         (scan-segments
           (if (zerop *wal-checkpoint-lsn*)
               segments
               (member checkpoint-segment segments)))
         (end-lsn *wal-checkpoint-lsn*)
         (checkpoint-found (zerop *wal-checkpoint-lsn*)))
    (when (and (plusp *wal-checkpoint-lsn*)
               (< *wal-checkpoint-lsn* +wal-header-size+))
      (error "WAL checkpoint LSN ~a is invalid" *wal-checkpoint-lsn*))
    (when (and (zerop *wal-checkpoint-lsn*) segments
               (not (zerop (first segments))))
      (error "WAL segment 0 is missing before the first checkpoint"))
    (when (and (plusp *wal-checkpoint-lsn*)
               (null scan-segments))
      (error "WAL checkpoint segment ~a is missing"
             checkpoint-segment))
    ;; Once the checkpoint control file is durable, WAL before this segment and
    ;; before the checkpoint record within it is no longer needed for redo.
    (loop for (left right) on scan-segments
          while right
          unless (= right (1+ left))
            do (error "missing WAL segment between ~a and ~a" left right))
    (loop for segment in scan-segments
          do (with-open-file (stream (wal-segment-path segment)
                                     :direction :input
                                     :element-type '(unsigned-byte 8))
               (let ((size (file-length stream))
                     (offset (if (and (plusp *wal-checkpoint-lsn*)
                                      (= segment checkpoint-segment))
                                 checkpoint-offset
                                 0)))
                 (loop while (< offset size)
                       do (let* ((record-lsn (+ (* segment +wal-segment-size+)
                                                offset))
                                 (remaining (- size offset)))
                            (when (< remaining +wal-header-size+)
                              (wal-truncate-unflushed-tail
                               segment last-segment offset record-lsn
                               "incomplete WAL header")
                              (return))
                            (file-position stream offset)
                            (let ((header (make-array +wal-header-size+
                                                      :element-type '(unsigned-byte 8))))
                              (unless (= (read-sequence header stream) +wal-header-size+)
                                (error "could not read WAL record header"))
                              (let ((record-length (wal-u32 header 8)))
                                (unless (and (>= record-length +wal-header-size+)
                                             (<= record-length +wal-segment-size+)
                                             (<= (+ offset record-length)
                                                 +wal-segment-size+))
                                  (wal-truncate-unflushed-tail
                                   segment last-segment offset record-lsn
                                   "invalid WAL record length")
                                  (return))
                                (when (> record-length remaining)
                                  (wal-truncate-unflushed-tail
                                   segment last-segment offset record-lsn
                                   "incomplete durable WAL record")
                                  (return))
                                (let ((bytes (make-array record-length
                                                         :element-type '(unsigned-byte 8))))
                                  (file-position stream offset)
                                  (unless (= (read-sequence bytes stream) record-length)
                                    (error "could not read WAL record at LSN ~a"
                                           record-lsn))
                                  (let ((record
                                          (handler-case
                                              (wal-decode-record bytes record-lsn)
                                            (error (condition)
                                              (if (and (eql segment last-segment)
                                                       (>= record-lsn
                                                           *wal-durable-lsn*))
                                                  (progn
                                                    (wal-truncate-tail
                                                     (wal-segment-path segment)
                                                     offset)
                                                    nil)
                                                  (error condition))))))
                                    (unless record (return))
                                    (when (and (plusp *wal-checkpoint-lsn*)
                                               (= record-lsn
                                                  checkpoint-record-start))
                                      (unless (and (= (wal-record-end record)
                                                      *wal-checkpoint-lsn*)
                                                   (= (wal-record-type record)
                                                      +wal-record-checkpoint+)
                                                   (zerop (length
                                                           (wal-record-payload
                                                            record))))
                                        (error "WAL checkpoint LSN ~a does not point to a valid checkpoint record"
                                               *wal-checkpoint-lsn*))
                                      (setf checkpoint-found t))
                                    (when (= (wal-record-end record)
                                             *wal-checkpoint-lsn*)
                                      (unless (= (wal-record-type record)
                                                 +wal-record-checkpoint+)
                                        (error "WAL checkpoint LSN ~a points to a non-checkpoint record"
                                               *wal-checkpoint-lsn*))
                                      (setf checkpoint-found t))
                                    (funcall function record)
                                    (setf end-lsn (wal-record-end record))
                                    (incf offset record-length))))))))))
    (unless checkpoint-found
      (error "WAL checkpoint LSN ~a is not present in retained WAL"
             *wal-checkpoint-lsn*))
    (when (> *wal-checkpoint-lsn* end-lsn)
      (error "WAL checkpoint LSN ~a is past the end of WAL ~a"
             *wal-checkpoint-lsn* end-lsn))
    (when (> *wal-durable-lsn* end-lsn)
      (error "WAL durable LSN ~a is past the end of WAL ~a"
             *wal-durable-lsn* end-lsn))
    (values end-lsn scan-segments)))

(defun wal-read-u64-payload (payload)
  (unless (= (length payload) 8)
    (error "WAL XID reset record has an invalid length"))
  (wal-u64 payload 0))

(defun wal-apply-catalog-image (deletes catalog-bytes)
  (wal-atomic-write-bytes (merge-pathnames "global/pg_catalog.dat" *pgdata*)
                          catalog-bytes)
  (dolist (relfilenode deletes)
    (smgr-forget relfilenode :delete t))
  (when deletes (fsync-directory (merge-pathnames "base/" *pgdata*))))

(defun wal-prepare-catalog-record (payload)
  "Validate a catalog payload and return the operation that publishes it."
  (multiple-value-bind (deletes catalog-bytes)
      (wal-decode-catalog-payload payload)
    (lambda () (wal-apply-catalog-image deletes catalog-bytes))))

(defun wal-replay-finish-batch (terminal-preparer &optional catalog-chunks-p)
  (unless *wal-recovery-group-active*
    (error "WAL terminal record appears outside a batch"))
  (when (and *wal-recovery-catalog-chunk-count*
             (not catalog-chunks-p))
    (error "WAL catalog chunks lack a catalog commit record"))
  ;; Decode and validate the terminal operation before touching any relation
  ;; pages. Chunked catalog metadata must be reassembled and decoded first.
  (let ((terminal-function (funcall terminal-preparer)))
    (unless (functionp terminal-function)
      (error "WAL terminal validation did not produce an apply operation"))
    (wal-apply-page-images (nreverse *wal-recovery-page-images*))
    (funcall terminal-function))
  (setf *wal-recovery-group-active* nil
        *wal-recovery-page-images* '()
        *wal-recovery-catalog-chunks* '()
        *wal-recovery-catalog-chunk-count* nil
        *wal-recovery-catalog-next-chunk* 0)
  (values))

(defun wal-replay-record (record)
  (when (> (wal-record-end record) *wal-checkpoint-lsn*)
    (case (wal-record-type record)
      (#.+wal-record-batch-start+
       ;; A prior group without a terminal record is a crash tail. Ignore its
       ;; accumulated images and begin staging the next post-restart batch.
       (setf *wal-recovery-group-active* t
             *wal-recovery-page-images* '()
             *wal-recovery-catalog-chunks* '()
             *wal-recovery-catalog-chunk-count* nil
             *wal-recovery-catalog-next-chunk* 0))
      (#.+wal-record-pages+
       (unless *wal-recovery-group-active*
         (error "WAL page images appear outside a batch"))
       (when *wal-recovery-catalog-chunk-count*
         (error "WAL page images follow catalog chunks in a batch"))
       (setf *wal-recovery-page-images*
             (nconc (reverse
                     (wal-decode-page-images (wal-record-payload record)
                                             (wal-record-end record)))
                    *wal-recovery-page-images*)))
      (#.+wal-record-commit+
       (unless (zerop (length (wal-record-payload record)))
         (error "WAL commit record has an invalid payload"))
       (wal-replay-finish-batch
        (lambda ()
          (lambda ()
            (clog-set (wal-record-xid record) 1)))))
      (#.+wal-record-catalog+
       (wal-replay-finish-batch
        (lambda ()
          (wal-prepare-catalog-record (wal-record-payload record)))))
      (#.+wal-record-catalog-chunk+
       (unless *wal-recovery-group-active*
         (error "WAL catalog chunks appear outside a batch"))
       (multiple-value-bind (index count bytes)
           (wal-decode-catalog-chunk-payload (wal-record-payload record))
         (unless (= index *wal-recovery-catalog-next-chunk*)
           (error "WAL catalog chunk index ~a follows chunk ~a"
                  index *wal-recovery-catalog-next-chunk*))
         (when (and *wal-recovery-catalog-chunk-count*
                    (/= count *wal-recovery-catalog-chunk-count*))
           (error "WAL catalog chunk count changes within a batch"))
         (setf *wal-recovery-catalog-chunk-count* count)
         (push bytes *wal-recovery-catalog-chunks*)
         (incf *wal-recovery-catalog-next-chunk*)))
      (#.+wal-record-catalog-commit+
       (multiple-value-bind (count length)
           (wal-decode-catalog-commit-payload (wal-record-payload record))
         (wal-replay-finish-batch
          (lambda ()
            (wal-prepare-catalog-record
             (wal-reassemble-catalog-chunks count length)))
          t)))
      (#.+wal-record-checkpoint+
       (unless (zerop (length (wal-record-payload record)))
         (error "WAL checkpoint record has an invalid payload"))
       (wal-replay-finish-batch (lambda () #'values)))
      (#.+wal-record-clog-reset+
       (wal-read-u64-payload (wal-record-payload record))
       (wal-replay-finish-batch
        (lambda () (lambda () (clog-reset :wal-reset-p nil))))))))

(defun wal-recover ()
  "Redo durable records after the checkpoint before loading the catalog."
  (unless *wal-ready*
    (return-from wal-recover (values)))
  (let ((*wal-recovery-group-active* nil)
        (*wal-recovery-page-images* '())
        (*wal-recovery-catalog-chunks* '())
        (*wal-recovery-catalog-chunk-count* nil)
        (*wal-recovery-catalog-next-chunk* 0))
    (wal-scan #'wal-replay-record))
  (clog-fsync)
  (values))

(defun wal-atomic-write-bytes (path bytes)
  (with-atomic-output (stream
                       path
                       (make-pathname :type "tmp" :defaults path)
                       (make-pathname :name nil :type nil :defaults path)
                       :element-type '(unsigned-byte 8))
    (write-sequence bytes stream)))

(defun wal-read-control-fields ()
  (let ((path (wal-control-path)))
    (when (probe-file path)
      (with-open-file (stream path :direction :input)
        (let* ((line (read-line stream nil nil))
               (fields (and line
                            (loop with start = 0
                                  for end = (position #\Space line :start start)
                                  collect (subseq line start end)
                                  while end
                                  do (setf start (1+ end)))))
               (checkpoint-lsn (and (= (length fields) 4)
                                    (string= (first fields) "PGWAL3")
                                    (ignore-errors
                                     (parse-integer (second fields)))))
               (durable-lsn (and checkpoint-lsn
                                 (ignore-errors
                                  (parse-integer (third fields)))))
               (crc (and durable-lsn
                         (ignore-errors
                          (parse-integer (fourth fields) :radix 16))))
               (prefix (and checkpoint-lsn durable-lsn
                            (format nil "PGWAL3 ~a ~a"
                                    checkpoint-lsn durable-lsn))))
          (unless (and checkpoint-lsn durable-lsn crc
                       (= crc (wal-crc32 (string->bytes prefix)))
                       (<= 0 checkpoint-lsn durable-lsn #xFFFFFFFFFFFFFFFF)
                       (null (read-line stream nil nil)))
            (error "WAL control file ~a is corrupt" path))
          (values checkpoint-lsn durable-lsn))))))

(defun wal-save-control-fields (checkpoint-lsn durable-lsn)
  (let* ((prefix (format nil "PGWAL3 ~a ~a" checkpoint-lsn durable-lsn))
         (crc (wal-crc32 (string->bytes prefix)))
         (text (format nil "~a ~8,'0X~%" prefix crc)))
    (wal-atomic-write-bytes (wal-control-path) (string->bytes text)))
  (values))

(defun wal-save-durable-lsn (lsn)
  (when (< lsn *wal-durable-lsn*)
    (error "WAL durable LSN cannot move backwards from ~a to ~a"
           *wal-durable-lsn* lsn))
  (wal-save-control-fields *wal-checkpoint-lsn* lsn)
  (setf *wal-durable-lsn* lsn)
  (values))

(defun wal-save-checkpoint-control (lsn)
  (when (> lsn *wal-durable-lsn*)
    (error "WAL checkpoint LSN ~a is past durable LSN ~a"
           lsn *wal-durable-lsn*))
  (wal-save-control-fields lsn *wal-durable-lsn*)
  (setf *wal-checkpoint-lsn* lsn)
  (values))

(defun wal-sync-segments (segments)
  (dolist (segment segments)
    (with-open-file (stream (wal-segment-path segment)
                            :direction :io
                            :element-type '(unsigned-byte 8)
                            :if-exists :append)
      (fsync-stream stream)))
  (values))

(defun wal-init ()
  (wal-ensure-directory-durable *pgdata*)
  (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
  (wal-ensure-directory-durable (merge-pathnames "base/" *pgdata*))
  (wal-ensure-directory-durable (merge-pathnames "pg_wal/" *pgdata*))
  ;; Persist segment entries left visible by a process that exited before its
  ;; first successful WAL directory sync, before reusing any existing segment.
  (fsync-directory (wal-directory))
  (let ((segments (wal-discover-segments)))
    (multiple-value-bind (checkpoint-lsn durable-lsn)
        (wal-read-control-fields)
      (when (and (null checkpoint-lsn) segments)
        (error "WAL control file is missing while WAL segments exist"))
      (setf *wal-checkpoint-lsn* (or checkpoint-lsn 0)
            *wal-durable-lsn* (or durable-lsn 0)
            *wal-end-lsn* 0
            *wal-ready* t
            *wal-poisoned* nil
            *wal-page-cache* (make-hash-table :test #'equal)
            *wal-pending-deletes* '()
            *wal-pending-btree-split* nil
            *wal-output-stream* nil
            *wal-output-segment* nil
            *wal-new-segments* '()
            *wal-test-pause-at* nil
            *wal-test-pause-relfilenode* nil)
      (unless checkpoint-lsn
        (wal-save-control-fields 0 0))
      (multiple-value-bind (end-lsn found-segments)
          (wal-scan (lambda (record) (declare (ignore record))))
        (setf *wal-end-lsn* end-lsn)
        (when (> end-lsn *wal-durable-lsn*)
          ;; WAL can reach disk just before a process exits without updating
          ;; the control file. Make that complete tail durable before redo.
          (wal-sync-segments found-segments)
          (wal-save-durable-lsn end-lsn)))
      (setf *wal-test-pause-at*
            (let ((value (sb-ext:posix-getenv "PG_WAL_TEST_PAUSE_AT")))
              (and value (string-downcase value)))
            *wal-test-pause-relfilenode*
            (let ((value (sb-ext:posix-getenv
                          "PG_WAL_TEST_PAUSE_RELATION")))
              (and value (parse-integer value :junk-allowed t)))))
    (values)))

(defun wal-test-pause (point)
  (when (and *wal-test-pause-at*
             (string= *wal-test-pause-at* (string-downcase (symbol-name point))))
    (let ((marker (merge-pathnames
                   (format nil "test-paused-~a"
                           (string-downcase (symbol-name point)))
                   (wal-directory))))
      (with-open-file (stream marker :direction :output :if-exists :supersede
                                     :if-does-not-exist :create)
        (format stream "paused~%")
        (finish-output stream)
        (fsync-stream stream))
      (loop do (sleep 1))))
  (values))

(defun wal-relation-filennodes ()
  (let ((relfilenodes '()))
    (maphash (lambda (database-name database)
               (declare (ignore database-name))
               (maphash (lambda (table-name table)
                          (declare (ignore table-name))
                          (push (pg-class-relfilenode table) relfilenodes))
                        (database-tables database))
               (maphash (lambda (index-name index)
                          (declare (ignore index-name))
                          (push (pg-index-relfilenode index) relfilenodes))
                        (database-indexes database)))
             (catalog-databases (symbol-value '*catalog*)))
    relfilenodes))

(defun wal-remove-orphan-relations ()
  (let ((known (wal-relation-filennodes))
        (base (merge-pathnames "base/" *pgdata*))
        (removed nil))
    (dolist (path (directory (merge-pathnames "*" base)))
      (let ((relfilenode (ignore-errors
                          (and (null (pathname-type path))
                               (parse-integer (pathname-name path))))))
        (when (and relfilenode (not (member relfilenode known :test #'eql)))
          (smgr-forget relfilenode :delete t)
          (setf removed t))))
    (when removed (fsync-directory base)))
  (values))

(defun wal-checkpoint ()
  "Write all cached pages, sync durable state, and advance the redo pointer."
  ;; CHECKPOINT follows the global write-lock -> WAL-lock order.
  (with-write-lock
    (with-wal-critical
      (let ((images (wal-page-images)))
        (wal-append-batch-start)
        (wal-append-page-images images)
        (let ((checkpoint-end
                (wal-append-record
                 +wal-record-checkpoint+ 0
                 (make-array 0 :element-type '(unsigned-byte 8)))))
          (wal-flush)
          (wal-test-pause :after-checkpoint-wal-flush)
          (wal-apply-page-images images)
          (wal-clear-page-cache)
          (smgr-fsync-all)
          (clog-fsync)
          (fsync-directory *pgdata*)
          (fsync-directory (merge-pathnames "base/" *pgdata*))
          (fsync-directory (merge-pathnames "global/" *pgdata*))
          (wal-save-checkpoint-control checkpoint-end)
          (wal-test-pause :after-checkpoint-control)
          (wal-remove-checkpointed-segments checkpoint-end)))))
  (values))

(defun wal-remove-checkpointed-segments (checkpoint-lsn)
  (let* ((keep-segment (if (zerop checkpoint-lsn)
                           0
                           (floor (1- checkpoint-lsn) +wal-segment-size+)))
         (removed nil))
    (dolist (segment (wal-discover-segments))
      (when (< segment keep-segment)
        (when (and *wal-output-stream*
                   (eql segment *wal-output-segment*))
          (close *wal-output-stream*)
          (setf *wal-output-stream* nil *wal-output-segment* nil))
        ;; Delete only a contiguous prefix. Continuing after a failed unlink
        ;; could leave a hole before the checkpoint segment and block startup.
        (handler-case
            (progn
              (delete-file (wal-segment-path segment))
              (setf removed t))
          (file-error () (return)))))
    (when removed (fsync-directory (wal-directory))))
  (values))

(defun wal-log-clog-reset ()
  (when *wal-ready*
    (with-wal-critical
      (let* ((next-xid (symbol-value '*next-xid*))
             (images (wal-page-images))
             (payload (make-array 8 :element-type '(unsigned-byte 8)
                                    :initial-element 0)))
        (wal-append-batch-start)
        (wal-append-page-images images)
        (wal-set-u64 payload 0 next-xid)
        (wal-append-record +wal-record-clog-reset+ next-xid payload)
        (wal-flush)
        (wal-apply-page-images images)
        (wal-clear-page-cache))))
  (values))

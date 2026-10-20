;;;; unit-tests.lisp -- Lisp-level unit tests for the pg server.
;;;;
;;;; Run via t/run-unit-tests.sh, which loads the "pg" system first.

(in-package #:pg)

(defvar *test-failures* 0)
(defvar *test-checks* 0)

(defmacro check (form &optional (label nil))
  `(if ,form
       (incf *test-checks*)
       (progn
         (incf *test-failures*)
         (format t "~&FAIL: ~a~%" (or ,label ',form)))))

(defun test-page-roundtrip ()
  (let ((p (page-init (make-page))))
    (check (= (page-lp-count p) 0) "empty page has no line pointers")
    (check (= (page-free-space p) (- +blcksz+ +size-of-page-header+))
           "empty page free space")
    (let ((item (make-array 8 :element-type '(unsigned-byte 8)
                              :initial-element 7)))
      (check (= (page-add-item p item) 1) "first item gets LP 1")
      (multiple-value-bind (off flags len) (page-get-lp p 1)
        (check (= flags +lp-normal+) "LP is normal")
        (check (= len 8) "LP length")
        (check (equalp (subseq p off (+ off 8)) item) "LP payload"))
      (check (= (page-add-item p item) 2) "second item gets LP 2")
      (check (= (page-lp-count p) 2) "two line pointers")
      (page-mark-dead p 1)
      (check (= (page-lp-count p) 2) "dead slot keeps the LP")
      (check (= (page-add-item p item :allow-reuse t) 1)
             "dead slot is reused when ALLOW-REUSE"))))

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
         (*relation-streams* (make-hash-table :test #'eql))
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

(defun test-swank-arguments ()
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address)
      (parse-args '("--swank-port=4512" "--swank-bind-address=0.0.0.0"))
    (declare (ignore pgdata control-port control-socket))
    (check (string= host "127.0.0.1") "default SQL host is unchanged")
    (check (= port 5433) "default SQL port is unchanged")
    (check (= swank-port 4512) "Swank port accepts --option=value")
    (check (string= swank-bind-address "0.0.0.0")
           "Swank bind address accepts --option=value"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address)
      (parse-args '("--swank-bind-address" "0.0.0.0"))
    (declare (ignore host port pgdata control-port control-socket))
    (check (null swank-port) "Swank bind address alone does not enable listener")
    (check (string= swank-bind-address "0.0.0.0")
           "Swank bind address is preserved without a listener"))
  (check (handler-case
             (progn (parse-args '("--swank-port=")) nil)
           (error () t))
         "empty Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-port")) nil)
           (error () t))
         "missing Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-port" "65536")) nil)
           (error () t))
         "out-of-range Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-bind-address=")) nil)
           (error () t))
         "empty Swank bind address is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-bind-address")) nil)
           (error () t))
         "missing Swank bind address is rejected"))

(defun test-malformed-packet-p (path bytes reader &optional expected-message)
  (with-open-file (stream path
                          :direction :output
                          :if-exists :supersede
                          :if-does-not-exist :create
                          :element-type '(unsigned-byte 8))
    (write-sequence bytes stream))
  (handler-case
      (with-open-file (stream path
                              :direction :input
                              :element-type '(unsigned-byte 8))
        (funcall reader stream)
        nil)
    (pg-error (condition)
      (and expected-message
           (string= (pg-error-sqlstate condition) "08P01")
           (string= (pg-error-message condition) expected-message)))
    (error () (null expected-message))))

(defun test-concat-octets (&rest parts)
  (let* ((size (reduce #'+ parts :key #'length :initial-value 0))
         (bytes (make-array size :element-type '(unsigned-byte 8)))
         (offset 0))
    (dolist (part parts)
      (replace bytes part :start1 offset)
      (incf offset (length part)))
    bytes))

(defun test-startup-packet-size (path packet-length)
  (with-open-file (stream path
                          :direction :output
                          :if-exists :supersede
                          :if-does-not-exist :create
                          :element-type '(unsigned-byte 8))
    (write-sequence (u32-to-bytes packet-length) stream)
    (write-sequence
     (make-array (- packet-length 4)
                 :element-type '(unsigned-byte 8)
                 :initial-element 0)
     stream))
  (with-open-file (stream path
                          :direction :input
                          :element-type '(unsigned-byte 8))
    (let ((body (read-startup-packet stream)))
      (and body (= (length body) +max-startup-packet-length+)))))

(defun test-protocol-packet-limits ()
  (let ((path (merge-pathnames "malformed-packet.out" *pgdata*)))
    (check (test-startup-packet-size path +max-startup-message-size+)
           "startup packets accept PostgreSQL's maximum body length")
    (check (test-malformed-packet-p
            path (u32-to-bytes 7) #'read-startup-packet
            "startup packet length is too short")
           "undersized startup packets are rejected")
    (check (test-malformed-packet-p
            path (u32-to-bytes (1+ +max-startup-message-size+))
            #'read-startup-packet "startup packet exceeds the size limit")
           "oversized startup packets are rejected before allocation")
    (check (test-malformed-packet-p
            path (u32-to-bytes 8) #'read-startup-packet
            "incomplete startup packet body")
           "truncated startup packet bodies are rejected")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes 3))
            #'read-message "frontend message length is too short")
           "undersized query packets are rejected")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes
                                      (1+ +max-query-message-size+)))
            #'read-message "frontend message exceeds the size limit")
           "oversized query packets are rejected before allocation")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes 5))
            #'read-message "incomplete frontend message body")
           "truncated query packet bodies are rejected")))

(defun test-live-page-items-stop ()
  (let ((page (page-init (make-page)))
        (page-evaluations 0)
        (seen '()))
    (dotimes (i 3)
      (page-add-item page (make-array 8 :element-type '(unsigned-byte 8)
                                        :initial-element i)))
    (page-mark-dead page 2)
    (do-live-page-items ((progn (incf page-evaluations) page) offnum off len)
      (check (and (plusp off) (plusp len)) "live page item has storage")
      (push offnum seen))
    (check (equal (nreverse seen) '(1 3))
           "live page iteration skips dead slots")
    (check (= page-evaluations 1) "live page iterator evaluates PAGE once")
    (flet ((first-live-item ()
             (do-live-page-items (page offnum off len)
               (when (and (plusp off) (plusp len))
                 (return-from first-live-item offnum)))
             nil))
      (check (equal (first-live-item) 1)
             "live page iteration preserves nonlocal early return"))))

(defun test-live-page-item-guard ()
  (let ((page (page-init (make-page)))
        (page-evaluations 0)
        (offnum-evaluations 0))
    (page-add-item page #(1 2 3))
    (multiple-value-bind (off len)
        (with-live-page-item ((progn (incf page-evaluations) page)
                              (progn (incf offnum-evaluations) 1)
                              item-off item-len)
          (values item-off item-len))
      (check (and (plusp off) (plusp len))
             "single live page item binds its tuple location")
      (check (= page-evaluations 1)
             "single live page item evaluates PAGE once")
      (check (= offnum-evaluations 1)
             "single live page item evaluates OFFNUM once"))
    (page-mark-dead page 1)
    (let ((body-ran nil)
          (line-pointer-reads 0))
      (with-live-page-item (page 1 item-off item-len)
        (when (plusp item-off)
          (setf body-ran t)))
      (check (not body-ran)
             "single live page item skips dead line pointers")
      (flet ((page-get-lp (page offnum)
               (declare (ignore page offnum))
               (incf line-pointer-reads)
               (error "out-of-range line pointer was read")))
        (with-live-page-item (page 0 item-off item-len)
          (setf body-ran t))
        (with-live-page-item (page 2 item-off item-len)
          (setf body-ran t)))
      (check (and (not body-ran) (zerop line-pointer-reads))
             "single live page item skips offsets outside the page"))))

(defun test-page-compaction ()
  (let ((p (page-init (make-page))))
    (dotimes (i 64)
      (page-add-item p (make-array 32 :element-type '(unsigned-byte 8)
                                      :initial-element i)))
    (page-mark-dead p 10)
    (page-mark-dead p 20)
    (page-compact p)
    (check (= (page-lp-count p) 64) "compaction preserves LP count")
    (loop for i from 1 to 64
          unless (member i '(10 20))
            do (multiple-value-bind (off flags len) (page-get-lp p i)
                 (declare (ignore off len))
                 (check (= flags +lp-normal+) "compaction keeps live LPs")))))

(defun test-tuple-roundtrip ()
  (let* ((cols (coerce
                (list (make-pg-column :name "a" :type :int4 :typmod -1
                                      :attnum 1 :notnull nil)
                      (make-pg-column :name "b" :type :varchar :typmod 8
                                      :attnum 2 :notnull nil)
                      (make-pg-column :name "c" :type :int8 :typmod -1
                                      :attnum 3 :notnull nil))
                'vector))
         (vals (vector 42 "hello" 1234567890123))
         (bytes (form-heap-tuple vals cols 7 0 1 0 0)))
    (multiple-value-bind (got xmin xmax cid infomask)
        (deform-heap-tuple bytes cols)
      (declare (ignore infomask))
      (check (= xmin 7) "xmin round-trips")
      (check (= xmax 0) "xmax round-trips")
      (check (= cid 1) "cid round-trips")
      (check (= (aref got 0) 42) "int4 round-trips")
      (check (string= (aref got 1) "hello") "varchar round-trips")
      (check (= (aref got 2) 1234567890123) "int8 round-trips")))
  ;; NULLs must survive the round trip too.
  (let* ((cols (coerce
                (list (make-pg-column :name "a" :type :int4 :typmod -1
                                      :attnum 1 :notnull nil)
                      (make-pg-column :name "b" :type :varchar :typmod 8
                                      :attnum 2 :notnull nil))
                'vector))
         (vals (vector +null+ "x"))
         (bytes (form-heap-tuple vals cols 7 0 1 0 0)))
    (multiple-value-bind (got xmin xmax cid infomask)
        (deform-heap-tuple bytes cols)
      (declare (ignore xmin xmax cid infomask))
      (check (eq (aref got 0) +null+) "null int4 round-trips")
      (check (string= (aref got 1) "x") "varchar after null"))))

(defun test-full-xid-tuple-format ()
  (let* ((columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (full-xmin (+ +xid-cycle+ 41))
         (full-xmax (+ +xid-cycle+ 42))
         (bytes (form-heap-tuple (vector 7) columns full-xmin full-xmax 0 0 0)))
    (check (= +size-of-heap-tuple-header+ 23)
           "heap tuple header has PostgreSQL's 23-byte size")
    (check (equal (list +heap-tuple-xmin-off+ +heap-tuple-xmax-off+
                        +heap-tuple-cid-off+ +heap-tuple-ctid-block-off+
                        +heap-tuple-ctid-offnum-off+ +heap-tuple-natts-off+
                        +heap-tuple-infomask-off+ +heap-tuple-hoff-off+)
                  '(0 4 8 12 16 18 20 22))
           "heap tuple header offsets match HeapTupleHeaderData")
    (check (= (page-u32 bytes +heap-tuple-xmin-off+) 41)
           "tuple xmin stores the low 32-bit TransactionId")
    (check (= (page-u32 bytes +heap-tuple-xmax-off+) 42)
           "tuple xmax stores the low 32-bit TransactionId")
    (multiple-value-bind (values xmin xmax)
        (deform-heap-tuple bytes columns)
      (check (= (aref values 0) 7) "wide-XID tuple keeps its attributes")
      (check (= (full-xid-from-32 xmin (+ +xid-cycle+ 100)) full-xmin)
             "tuple xmin reconstructs in the current epoch")
      (check (= (full-xid-from-32 xmax (+ +xid-cycle+ 100)) full-xmax)
             "tuple xmax reconstructs in the current epoch")))
  (check (= (advance-full-xid (1- +xid-cycle+))
            (+ +xid-cycle+ +first-normal-xid+))
         "FullXID advance skips special low XIDs at wrap")
  (check (= (full-xid-from-32 #xFFFFFFFE (+ +xid-cycle+ 3))
            #xFFFFFFFE)
         "tuple XIDs below the new epoch resolve to the prior epoch"))

(defun test-xid-tuple-update-roundtrip ()
  ;; Exercise the heap-level xmin/xmax contract behind the update system-column
  ;; cases in src/test/regress/sql/update.sql, using a later 32-bit XID epoch.
  (let* ((*next-xid* (+ +xid-cycle+ 52))
         (relfilenode 918273646)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (insert-xid (+ +xid-cycle+ 50))
         (update-xid (+ +xid-cycle+ 51)))
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (multiple-value-bind (old-block old-offnum)
             (heapam-insert relfilenode columns (vector 10) insert-xid 0)
           (heapam-set-xmax relfilenode old-block old-offnum update-xid 1)
           (multiple-value-bind (new-block new-offnum)
               (heapam-insert relfilenode columns (vector 11) update-xid 1)
             (check (heapam-set-ctid relfilenode old-block old-offnum
                                     new-block new-offnum)
                    "heap update links the old tuple to its successor")
             (let ((old-tuple (heapam-get-tuple relfilenode old-block
                                                old-offnum columns))
                   (new-tuple (heapam-get-tuple relfilenode new-block
                                                new-offnum columns)))
               (check (and (= (htup-ctid-block old-tuple) new-block)
                           (= (htup-ctid-offnum old-tuple) new-offnum))
                      "heap read preserves the update-chain ctid")
               (check (= (htup-xmin old-tuple) insert-xid)
                      "heap read reconstructs the old row's FullXID xmin")
               (check (= (htup-xmax old-tuple) update-xid)
                      "heap read reconstructs the updating FullXID xmax")
               (check (= (htup-xmin new-tuple) update-xid)
                      "heap read reconstructs the new row's FullXID xmin")
               (check (zerop (htup-xmax new-tuple))
                      "new row has an invalid xmax"))))
      (smgr-forget relfilenode :delete t))))

(defun test-heap-update-chain-follow ()
  ;; PostgreSQL's heap_delete reports TM_Updated and ExecDelete runs
  ;; EvalPlanQual. Model the ctid walk and row-predicate recheck at heap level.
  (let* ((*next-xid* 7205)
         (*active-xids* '(7204))
         (*backend* (make-backend))
         (*current-snapshot* (make-snapshot-struct :xmin 7200 :xmax 7205
                                                   :xip '() :curcid 10))
         (relfilenode 918273647)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (tids '()))
    (dolist (xid '(7200 7201 7202 7203))
      (clog-set xid +clog-committed+))
    (clog-set 7204 +clog-in-progress+)
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (progn
           (let ((previous nil))
             (dolist (spec '((7200 10) (7201 20) (7202 30) (7203 40)))
               (destructuring-bind (xmin value) spec
                 (multiple-value-bind (block offnum)
                     (heapam-insert relfilenode columns (vector value) xmin 0)
                   (when previous
                     (heapam-set-xmax relfilenode (first previous)
                                      (second previous) xmin 0)
                     (heapam-set-ctid relfilenode (first previous)
                                      (second previous) block offnum))
                   (push (cons block offnum) tids)
                   (setf previous (list block offnum))))))
           (setf tids (nreverse tids))
           (multiple-value-bind (deleted tuple block offnum)
               (heapam-delete-tid relfilenode columns
                                  (caar tids) (cdar tids) 7204 1
                                  (lambda (candidate)
                                    (= (aref (htup-values candidate) 0) 40)))
             (check deleted "update chain reaches and matches latest tuple")
             (check (= (aref (htup-values tuple) 0) 40)
                    "update-chain recheck returns the latest row values")
             (check (equal (cons block offnum) (car (last tids)))
                    "update-chain delete marks the latest tuple TID")
             (check (= (htup-xmin tuple) 7203)
                    "update chain can span several committed XIDs")
             (check (= (htup-xmax (heapam-get-tuple
                                   relfilenode block offnum columns))
                       7204)
                    "only the latest tuple receives the new xmax")))
      (release-xid 7204)
      (smgr-forget relfilenode :delete t))))

(defun test-heap-free-space-map ()
  (let ((fsm (make-heap-fsm)))
    (heap-fsm-record-page fsm 4 300)
    (heap-fsm-record-page fsm 6 160)
    (check (= (heap-fsm-take-page fsm 150) 6)
           "heap FSM selects the smallest fitting space category")
    (heap-fsm-record-page fsm 6 64)
    (check (= (heap-fsm-take-page fsm 250) 4)
           "heap FSM refreshes a page's space category")
    (check (null (heap-fsm-take-page fsm 250))
           "heap FSM skips pages below the requested space")
    (check (not (gethash 4 (heap-fsm-page-space fsm)))
           "taking a page removes its old category entry")
    (heap-fsm-record-page fsm 8 200)
    (check (= (heap-fsm-take-page fsm 200) 8)
           "heap FSM preserves exact-fit pages at category boundaries"))
  ;; The heap path should reuse the current page rather than extending a page
  ;; for every INSERT.
  (let* ((relfilenode 918273648)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (*backend* (make-backend)))
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (progn
           (dotimes (i 100)
             (heapam-insert relfilenode columns (vector i) 7200 0))
           (check (= (buf-nblocks relfilenode) 1)
                  "heap inserts reuse a page found through the FSM")
           (heap-fsm-forget relfilenode)
           (heapam-insert relfilenode columns #(100) 7200 0)
           (check (= (buf-nblocks relfilenode) 1)
                  "first insert rebuilds the FSM from existing heap pages"))
      (smgr-forget relfilenode :delete t))))

(defun test-postgres-special-xid-statuses ()
  ;; Port the status cases from src/test/regress/sql/xid.sql. PostgreSQL's
  ;; TransactionLogFetch treats bootstrap/frozen as committed and invalid as
  ;; aborted. This Lisp check also uses the active-XID set to distinguish
  ;; in-progress XIDs from abandoned zero-status entries.
  (let* ((committed 7021)
         (aborted 7022)
         (active 7023)
         (abandoned 7024)
         (*active-xids* (list active)))
    (clog-set committed +clog-committed+)
    (clog-set aborted +clog-aborted+)
    (clog-set active +clog-in-progress+)
    (check (xid-committed-p committed) "committed XID status is preserved")
    (check (xid-aborted-p aborted) "aborted XID status is preserved")
    (check (xid-in-progress-p active) "active XID status is in progress")
    (check (xid-aborted-p abandoned)
           "inactive zero-status XID is treated as abandoned")
    (check (xid-committed-p +bootstrap-xid+)
           "bootstrap XID is always committed")
    (check (xid-committed-p +frozen-xid+)
           "frozen XID is always committed")
    (check (not (xid-aborted-p +bootstrap-xid+))
           "bootstrap XID is never aborted")
    (check (not (xid-aborted-p +frozen-xid+))
           "frozen XID is never aborted")
    (check (xid-aborted-p +invalid-xid+)
           "invalid XID is treated as aborted")))

(defun test-xid-control-state ()
  (let* ((*pgdata* (merge-pathnames "xid-state/" *pgdata*))
         (control (control-path))
         (temporary (pgdata-path "global/pg_control.tmp"))
         (clog (clog-path)))
    (check (= (load-next-xid) +first-normal-xid+)
           "missing XID control state initializes an empty cluster")
    (ensure-directories-exist clog)
    (with-open-file (stream clog
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (write-byte 0 stream))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "missing XID control state is rejected when persistent state exists")
    (ensure-directories-exist control)
    ;; An empty file models a crash that truncated pg_control in place.
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-string "" stream))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "truncated XID control state is rejected")
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (format stream "not-an-xid~%"))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "invalid XID control state is rejected")
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (format stream "~a~%" (+ +first-normal-xid+ 100)))
    (check (= (load-next-xid) (+ +first-normal-xid+ 100))
           "valid XID control state loads exactly")
    (let ((fsync-count 0)
          (*xid-batch-end* (+ +first-normal-xid+ 200)))
      (save-next-xid
       :fsync-function
       (lambda (fd)
         (incf fsync-count)
         (sb-posix:fsync fd))
       :fsync-directory-function
       (lambda (directory)
         (incf fsync-count)
         (fsync-directory directory)))
      (check (= (load-next-xid) *xid-batch-end*)
             "XID control save installs the new boundary")
      (check (= fsync-count 2)
             "XID control save fsyncs its file and parent directory")
      (check (not (probe-file temporary))
             "successful XID control save removes its temporary file")
      (let ((*xid-batch-end* (+ +first-normal-xid+ 300)))
        (check (handler-case
                   (progn
                     (save-next-xid
                      :rename-function
                      (lambda (from to)
                        (declare (ignore from to))
                        (error "injected XID control rename failure")))
                     nil)
                 (error () t))
               "XID control rename errors are reported")
        (check (= (load-next-xid) (+ +first-normal-xid+ 200))
               "failed XID control replacement preserves prior state")
        (check (not (probe-file temporary))
               "failed XID control save removes its temporary file")))))

(defun test-data-row-first-column (payload)
  (when (and (>= (length payload) 6)
             (plusp (bytes-to-u16 payload 0)))
    (let ((length (bytes-to-u32 payload 2)))
      (unless (= length #xFFFFFFFF)
        (when (<= (+ 6 length) (length payload))
          (bytes->string payload :start 6 :end (+ 6 length)))))))

(defun test-show-timezone-values (path)
  (with-open-file (stream path
                          :direction :input
                          :element-type '(unsigned-byte 8))
    (loop for message = (read-message stream)
          while message
          when (char= (frontend-message-type message) #\D)
            collect (test-data-row-first-column
                     (frontend-message-payload message)))))

(defun test-session-setting-rollback ()
  (let ((*backend* (make-backend))
        (path (merge-pathnames "session-settings.out" *pgdata*)))
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream
                         (concatenate 'string
                                      "SET TIME ZONE 'Europe/Warsaw'; "
                                      "SHOW timezone; "
                                      "BEGIN; "
                                      "SET TIME ZONE 'America/New_York'; "
                                      "SHOW timezone; "
                                      "ROLLBACK; "
                                      "SHOW timezone; "
                                      "RESET timezone; "
                                      "SHOW timezone;")))
    (check (equal (test-show-timezone-values path)
                  '("Europe/Warsaw" "America/New_York" "Europe/Warsaw" "UTC"))
           "SHOW reports SET values and the value restored by ROLLBACK")
    (check (not (nth-value 1 (gethash "timezone"
                                      (backend-settings *backend*))))
           "RESET TIME ZONE returns the backend to its default")
    (check (eq (backend-state *backend*) :idle)
           "ROLLBACK leaves the backend outside a transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :append
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "SET TIME ZONE 'Europe/Warsaw';"))
    (let ((caught nil))
      (handler-case
          (exec-set nil '(:name "timezone" :value "Invalid/NoSuchZone"))
        (pg-error (condition)
          (setf caught condition)))
      (check (and caught
                  (string= (pg-error-sqlstate caught) "22023"))
             "SET TIME ZONE rejects missing zoneinfo names")
      (check (string= (gethash "timezone" (backend-settings *backend*))
                      "Europe/Warsaw")
             "rejected time zone preserves the previous session setting"))))

(defun test-xid-wraparound-freezes-tuples ()
  ;; Deterministic one-cycle counterpart to xid_wraparound/t/003_wraparounds.pl.
  (let* ((*backend* (make-backend))
         (*catalog* (make-catalog))
         (*next-xid* (+ +xid-cycle+ +first-normal-xid+))
         (*xid-batch-end* *next-xid*)
         (*active-xids* '())
         (*active-snapshots* 0)
         (*xid-wrap-pending* nil)
         (*xid-wrap-completed-through* nil)
         (relfilenode 918273645)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (db (make-database :name "postgres" :oid 1))
         (table (make-pg-class :oid 2 :name "wrap_rows"
                               :relfilenode relfilenode
                               :columns columns :indexes nil))
         (page (page-init (make-page))))
    (setf (gethash "postgres" (catalog-databases *catalog*)) db
          (gethash "wrap_rows" (database-tables db)) table)
    (ensure-directories-exist (relation-file-path relfilenode))
    (dolist (spec '((7 0 :committed nil)
                    (7 8 :committed :committed)
                    (9 0 :aborted nil)
                    (7 10 :committed :aborted)))
      (destructuring-bind (xmin xmax xmin-status xmax-status) spec
        (when xmin-status
          (clog-set xmin (ecase xmin-status
                           (:committed +clog-committed+)
                           (:aborted +clog-aborted+))))
        (when xmax-status
          (clog-set xmax (ecase xmax-status
                           (:committed +clog-committed+)
                           (:aborted +clog-aborted+))))
        (page-add-item page
                       (form-heap-tuple (vector 1) columns xmin xmax 0 0 0))))
    (unwind-protect
         (progn
           (buf-extend relfilenode page)
           (complete-xid-wraparound)
           (let ((frozen-page (buf-read relfilenode 0)))
             (loop for expected in '((2 0) (2 2) (0 0) (2 0))
                   for offnum from 1
                   do (multiple-value-bind (off flags len)
                          (page-get-lp frozen-page offnum)
                        (declare (ignore flags len))
                        (check (= (page-u32 frozen-page
                                            (+ off +heap-tuple-xmin-off+))
                                  (first expected))
                               "wraparound freezes old xmin status")
                        (check (= (page-u32 frozen-page
                                            (+ off +heap-tuple-xmax-off+))
                                  (second expected))
                               "wraparound freezes old xmax status"))))
           (let ((snapshot (make-snapshot-struct :xmin +first-normal-xid+
                                                 :xmax *next-xid*
                                                 :xip '() :curcid 10)))
             (loop for expected-visible in '(t nil nil t)
                   for offnum from 1
                   for tuple = (heapam-get-tuple relfilenode 0 offnum columns)
                   do (check (eql (heap-tuple-visible-p
                                   (htup-xmin tuple) (htup-xmax tuple)
                                   (htup-cid tuple) snapshot nil)
                                  expected-visible)
                             "pre-wrap tuple visibility survives freezing")))
           (check (= (clog-get 7) +clog-in-progress+)
                  "wraparound resets the old CLOG cycle")
           (check (xid-aborted-p 7)
                  "cleared CLOG IDs are abandoned when no longer active")
           (check (not (xid-aborted-p +frozen-xid+))
                  "frozen XID is not aborted after CLOG reset")
           (check (not (xid-aborted-p +bootstrap-xid+))
                  "bootstrap XID is not aborted after CLOG reset")
           (check (eq (unique-conflict-status
                       (make-htup :xmin +frozen-xid+ :xmax 0) nil)
                      :violation)
                  "frozen live tuples still block duplicate unique keys")
           (check (eq (unique-conflict-status
                       (make-htup :xmin +frozen-xid+
                                  :xmax +frozen-xid+)
                       nil)
                      :free)
                  "frozen committed deletes free unique keys")
           (check (not (probe-file (xid-wrap-marker-path)))
                  "completed wraparound removes its recovery marker")
           ;; Replay after a crash that left the durable marker behind even
           ;; though freezing and the CLOG reset had already completed.
           (save-xid-wrap-marker *next-xid*)
           (setf *xid-wrap-pending* t
                 *xid-wrap-completed-through* nil)
           (complete-xid-wraparound)
           (check (not (probe-file (xid-wrap-marker-path)))
                  "wraparound marker replay completes idempotently")
           (check (null *xid-wrap-pending*)
                  "completed wraparound releases the XID allocation gate")
           ;; A cancellation delivered by the marker unlink must be reported
           ;; only after the completed state has opened the XID gate.
           (save-xid-wrap-marker *next-xid*)
           (setf *xid-wrap-pending* t
                 *xid-wrap-completed-through* nil)
           (let ((cancel-error nil)
                 (shutdown-count 0))
             (handler-case
                 (complete-xid-wraparound
                  :delete-marker-function
                  (lambda ()
                    (delete-xid-wrap-marker)
                    (setf (backend-cancel-pending *backend*) t))
                  :shutdown-function
                  (lambda () (incf shutdown-count)))
               (pg-error (condition)
                 (setf cancel-error condition)))
             (check (and cancel-error
                         (string= (pg-error-sqlstate cancel-error) "57014"))
                    "post-marker cancellation is an ordinary query cancel")
             (check (zerop shutdown-count)
                    "post-marker cancellation does not request server shutdown")
             (check (and (not *xid-wrap-pending*)
                         (eql *xid-wrap-completed-through* *next-xid*))
                    "post-marker cancellation publishes the completed transition")
             (check (not (probe-file (xid-wrap-marker-path)))
                    "post-marker cancellation leaves no recovery marker"))
           (let ((expected-xid *next-xid*))
             (check (= (assign-xid) expected-xid)
                    "new-epoch XIDs can be allocated after freezing")))
      (smgr-forget relfilenode :delete t))))

(defun test-wait-until (predicate)
  (loop repeat 1000
        when (funcall predicate) return t
          do (sleep 0.01)
        finally (return nil)))

(defun test-prepare-wraparound-fixture (relfilenode)
  (smgr-forget relfilenode :delete t)
  (ignore-errors (delete-file (xid-wrap-marker-path)))
  (ignore-errors (delete-file (pgdata-path "global/pg_xid_wrap_pending.tmp")))
  (let* ((catalog (make-catalog))
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (db (make-database :name "postgres" :oid 1))
         (table (make-pg-class :oid 2 :name "wrap_fault_rows"
                               :relfilenode relfilenode
                               :columns columns :indexes nil))
         (boundary (+ +xid-cycle+ +first-normal-xid+)))
    (setf (gethash "postgres" (catalog-databases catalog)) db
          (gethash "wrap_fault_rows" (database-tables db)) table
          *next-xid* boundary
          *xid-batch-end* boundary
          *active-xids* '()
          *active-snapshots* 0
          *xid-wrap-pending* nil
          *xid-wrap-failure* nil
          *xid-wrap-completed-through* nil)
    ;; XID allocation persisted this reserved batch boundary before wrap.
    (save-next-xid)
    (clog-reset)
    (clog-set 7 +clog-committed+)
    (ensure-directories-exist (relation-file-path relfilenode))
    (dotimes (block 2)
      (let ((page (page-init (make-page))))
        (page-add-item page (form-heap-tuple (vector (1+ block)) columns 7 0 0 0 0))
        (buf-extend relfilenode page)))
    catalog))

(defun test-restart-wraparound-fixture (relfilenode)
  ;; Reopen heap and CLOG files and rebuild XID state from durable storage.
  (smgr-forget relfilenode)
  (when *clog-stream*
    (close *clog-stream*)
    (setf *clog-stream* nil))
  (setf *active-xids* '()
        *active-snapshots* 0
        *xid-wrap-pending* nil
        *xid-wrap-failure* nil
        *xid-wrap-completed-through* nil)
  (xact-init)
  (complete-xid-wraparound))

(defun test-xid-wraparound-fault-recovery ()
  (let ((*backend* nil)
        (*catalog* nil)
        (*next-xid* *next-xid*)
        (*xid-batch-end* *xid-batch-end*)
        (*active-xids* *active-xids*)
        (*active-snapshots* *active-snapshots*)
        (*xid-wrap-pending* *xid-wrap-pending*)
        (*xid-wrap-failure* *xid-wrap-failure*)
        (*xid-wrap-completed-through* *xid-wrap-completed-through*)
        (relfilenode 918273646))
    (labels ((run-failure (name inject expected-marker-p
                           &optional (restart-marker-p expected-marker-p))
               (let ((catalog (test-prepare-wraparound-fixture relfilenode))
                     (boundary (+ +xid-cycle+ +first-normal-xid+))
                     (shutdown-count 0))
                 (setf *catalog* catalog)
                 (check (handler-case
                            (progn
                              (funcall inject
                                       (lambda () (incf shutdown-count)))
                              nil)
                          (error () t))
                        (format nil "~a failure is reported" name))
                 (check (and *xid-wrap-pending* *xid-wrap-failure*)
                        (format nil "~a keeps the XID gate closed" name))
                 (check (= shutdown-count 1)
                        (format nil "~a requests server shutdown" name))
                 (check (eql (not (null (probe-file (xid-wrap-marker-path))))
                             expected-marker-p)
                        (format nil "~a leaves the expected recovery marker state"
                                name))
                 (when (string= name "partial relation freeze")
                   (let* ((first-page (buf-read relfilenode 0))
                          (second-page (buf-read relfilenode 1))
                          (first-off (nth-value 0 (page-get-lp first-page 1)))
                          (second-off (nth-value 0 (page-get-lp second-page 1))))
                     (check (= (page-u32 first-page
                                         (+ first-off +heap-tuple-xmin-off+))
                               +frozen-xid+)
                            "freeze fault leaves the first page partially frozen")
                     (check (= (page-u32 second-page
                                         (+ second-off +heap-tuple-xmin-off+))
                               7)
                            "freeze fault leaves the next page untouched")))
                 (check (handler-case
                            (progn (wait-for-xid-wraparound (make-backend)) nil)
                          (pg-error (condition)
                            (string= (pg-error-severity condition) "FATAL")))
                        (format nil "~a rejects sessions at the failed gate" name))
                 ;; A directory-fsync failure can leave either rename
                 ;; outcome after a crash. Exercise both marker states using
                 ;; a startup reload of the durable XID boundary and CLOG.
                 (if restart-marker-p
                     (save-xid-wrap-marker boundary)
                     (ignore-errors (delete-file (xid-wrap-marker-path))))
                 (test-restart-wraparound-fixture relfilenode)
                 (check (= *next-xid* boundary)
                        (format nil "~a restart reloads the XID boundary" name))
                 (check (not *xid-wrap-pending*)
                        (format nil "~a recovery opens the gate" name))
                 (check (not (probe-file (xid-wrap-marker-path)))
                        (format nil "~a recovery removes the marker" name))
                 (dotimes (block 2)
                   (let* ((page (buf-read relfilenode block))
                          (off (nth-value 0 (page-get-lp page 1))))
                     (check (= (page-u32 page (+ off +heap-tuple-xmin-off+))
                               +frozen-xid+)
                            (format nil "~a recovery freezes page ~d" name block)))))))
      (run-failure
       "control file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :fsync-function (lambda (fd)
                               (declare (ignore fd))
                               (error "injected control file fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "control file rename"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :rename-function (lambda (from to)
                                (declare (ignore from to))
                                (error "injected control file rename failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "control file directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected control directory fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-function (lambda (fd)
                               (declare (ignore fd))
                               (error "injected marker file fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker rename"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :rename-function (lambda (from to)
                                (declare (ignore from to))
                                (error "injected marker rename failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker directory fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "marker directory fsync with rename lost"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker directory fsync failure"))))
          :shutdown-function shutdown))
       t nil)
      (run-failure
       "partial relation freeze"
       (lambda (shutdown)
         (complete-xid-wraparound
          :freeze-function
          (lambda (old-epoch)
            (freeze-all-heap-xids
             old-epoch
             :after-page-function
             (lambda (relation block)
               (declare (ignore relation))
               (when (zerop block)
                 (error "injected partial relation freeze failure")))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "relation fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :freeze-function
          (lambda (old-epoch)
            (freeze-all-heap-xids
             old-epoch
             :fsync-function (lambda (relation)
                               (declare (ignore relation))
                               (error "injected relation fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "CLOG file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :reset-clog-function
          (lambda (&key fsync-function fsync-directory-function)
            (declare (ignore fsync-function fsync-directory-function))
            (clog-reset
             :fsync-function (lambda (stream)
                               (declare (ignore stream))
                               (error "injected CLOG file fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "CLOG directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :reset-clog-function
          (lambda (&key fsync-function fsync-directory-function)
            (declare (ignore fsync-function fsync-directory-function))
            (clog-reset
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected CLOG directory fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "marker removal directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :delete-marker-function
          (lambda (&key fsync-directory-function)
            (declare (ignore fsync-directory-function))
            (delete-xid-wrap-marker
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker removal directory fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker removal directory fsync with unlink lost"
       (lambda (shutdown)
         (complete-xid-wraparound
          :delete-marker-function
          (lambda (&key fsync-directory-function)
            (declare (ignore fsync-directory-function))
            (delete-xid-wrap-marker
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker removal directory fsync failure"))))
          :shutdown-function shutdown))
       nil t))))

(defun test-xid-wraparound-coordinator-cancel ()
  (let* ((relfilenode 918273647)
         (*backend* nil)
         (*catalog* nil)
         (old-next-xid *next-xid*)
         (old-batch-end *xid-batch-end*)
         (old-active-xids *active-xids*)
         (old-active-snapshots *active-snapshots*)
         (old-wrap-pending *xid-wrap-pending*)
         (old-wrap-failure *xid-wrap-failure*)
         (old-completed-through *xid-wrap-completed-through*)
         (pgdata *pgdata*)
         (coordinator (make-backend))
         (waiter (make-backend))
         (coordinator-waiting nil)
         (coordinator-error nil)
         (waiter-error nil)
         (shutdown-count 0)
         (catalog nil)
         (coordinator-thread nil)
         (waiter-thread nil))
    (unwind-protect
         (progn
           (setf catalog (test-prepare-wraparound-fixture relfilenode))
           ;; Keep the active-XID state global so both SBCL threads observe it.
           (setf *active-xids* '(77))
           (setf coordinator-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* coordinator)
                          (*catalog* catalog)
                          (*pgdata* pgdata))
                      (handler-case
                          (complete-xid-wraparound
                           :wait-for-xids-function
                           (lambda ()
                             (setf coordinator-waiting t)
                             (wait-for-no-active-xids))
                           :shutdown-function
                           (lambda () (incf shutdown-count)))
                        (error (condition)
                          (setf coordinator-error condition)))))))
           (check (test-wait-until (lambda () coordinator-waiting))
                  "wraparound coordinator waits for active XIDs")
           (setf waiter-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* waiter)
                          (*catalog* catalog)
                          (*pgdata* pgdata))
                      (handler-case
                          (prepare-xid-wraparound-for-statement waiter)
                        (error (condition)
                          (setf waiter-error condition)))))))
           (check (test-wait-until
                   (lambda () (backend-wraparound-waiting waiter)))
                  "another backend waits on the wraparound gate")
           (setf (backend-cancel-pending coordinator) t)
           (check (test-wait-until
                   (lambda () (and coordinator-error waiter-error)))
                  "coordinator cancellation releases waiting backends")
           (check (and (typep coordinator-error 'pg-error)
                       (string= (pg-error-sqlstate coordinator-error) "XX001"))
                  "failed coordinator receives a fatal wraparound error")
           (check (and (typep waiter-error 'pg-error)
                       (string= (pg-error-sqlstate waiter-error) "XX001"))
                  "waiting backend receives a fatal wraparound error")
           (check (and (typep *xid-wrap-failure* 'pg-error)
                       (string= (pg-error-sqlstate *xid-wrap-failure*) "57014"))
                  "wraparound failure records the original cancellation")
           (check (= shutdown-count 1)
                  "wraparound cancellation requests server shutdown once"))
      (setf *active-xids* '())
      (broadcast-xact)
      (sb-thread:with-mutex (*xid-wrap-lock*)
        (sb-thread:condition-broadcast *xid-wrap-cv*))
      (dolist (thread (list coordinator-thread waiter-thread))
        (when (and thread (sb-thread:thread-alive-p thread))
          (ignore-errors (sb-thread:join-thread thread :timeout 2 :default nil))))
      (ignore-errors (delete-file (xid-wrap-marker-path)))
      (smgr-forget relfilenode :delete t)
      (setf *next-xid* old-next-xid
            *xid-batch-end* old-batch-end
            *active-xids* old-active-xids
            *active-snapshots* old-active-snapshots
            *xid-wrap-pending* old-wrap-pending
            *xid-wrap-failure* old-wrap-failure
            *xid-wrap-completed-through* old-completed-through))))

(defun test-key-order ()
  (flet ((k (v ty) (encode-index-key (list v) (list ty))))
    (check (minusp (key-compare (k -1 :int4) (k 0 :int4))) "int4 order")
    (check (minusp (key-compare (k -2147483648 :int4) (k 2147483647 :int4)))
           "int4 extremes")
    (check (minusp (key-compare (k -1 :int8) (k 0 :int8))) "int8 order")
    (check (minusp (key-compare (k +null+ :int4) (k -2147483648 :int4)))
           "null sorts first")
    (check (minusp (key-compare (k "abc" :varchar) (k "abd" :varchar)))
           "varchar order")
    ;; Index keys carry a length prefix, so ordering is length-first.  This
    ;; is internal only: SQL comparisons and ORDER BY use compare-3way.
    (check (minusp (key-compare (k "b" :varchar) (k "aa" :varchar)))
           "varchar key encoding is length-first")))

(defun test-index-catalog-roundtrip ()
  (let* ((idx (make-pg-index :oid 42 :name "nnd" :relfilenode 42
                             :table-oid 41 :columns '(1) :unique-p t
                             :constraint-p t
                             :nulls-not-distinct-p t))
         (sexp (index-to-sexp idx))
         (copy (index-from-sexp sexp)))
    (check (equal sexp
                  '(:oid 42 :name "nnd" :relfilenode 42 :table-oid 41
                    :columns (1) :unique t :primary nil :constraint t
                    :nulls-not-distinct t))
           "index catalog encoding is the compatible flat plist")
    (check (pg-index-nulls-not-distinct-p copy)
           "NULLS NOT DISTINCT survives catalog serialization")
    (check (pg-index-constraint-p copy)
           "constraint ownership survives catalog serialization"))
  (let* ((class (make-pg-class
                 :oid 50 :name "codec" :relfilenode 51
                 :columns (vector (make-pg-column :name "value" :type :int4
                                                  :typmod -1 :attnum 1
                                                  :notnull t))
                 :indexes '(52)))
         (sexp (class-to-sexp class))
         (copy (class-from-sexp sexp)))
    (check (equal sexp
                  '(:oid 50 :name "codec" :relfilenode 51
                    :columns ((:name "value" :type :int4 :typmod -1
                               :attnum 1 :notnull t))
                    :indexes (52)))
           "class and column codecs use compatible flat plists")
    (check (vectorp (pg-class-columns copy))
           "class codec restores columns as a vector")
    (check (string= (pg-column-name (aref (pg-class-columns copy) 0)) "value")
           "class codec round-trips column mappings"))
  (let ((legacy-pk (index-from-sexp '(:primary t))))
    (check (not (pg-index-constraint-p legacy-pk))
           "legacy catalog has no constraint ownership flag")
    (check (pg-index-constraint-owned-p legacy-pk)
           "legacy primary index remains constraint-owned")))

(defun test-sql-strict-let-evaluation ()
  (let ((evaluations '()))
    (check (eq +null+
               (sql-strict-let ((left (progn (push :left evaluations) +null+))
                                (right (progn (push :right evaluations) 7)))
                 :body-must-not-run))
           "strict let returns NULL when any binding is NULL")
    (check (equal (nreverse evaluations) '(:left :right))
           "strict let evaluates every binding once before NULL check"))
  (let ((evaluations 0))
    (check (= 9
              (sql-strict-let ((left (progn (incf evaluations) 4))
                               (right (progn (incf evaluations) 5)))
                (+ left right)))
           "strict let returns body result for non-NULL bindings")
    (check (= evaluations 2) "strict let bindings run once each")))

(defun test-error-cleanup-and-fatal-rethrow ()
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (cleanup-count 0)
         (caught nil))
    (handler-case
        (with-error-cleanup (condition (incf cleanup-count))
          (error fatal))
      (pg-error (condition)
        (setf caught condition)))
    (check (= cleanup-count 1) "error cleanup runs before rethrow")
    (check (eq caught fatal) "error cleanup re-signals the original condition"))
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (handler-ran nil)
         (caught nil))
    (handler-case
        (handler-case-nonfatal
         (error fatal)
         (condition
          (setf handler-ran (typep condition 'error))))
      (pg-error (condition)
        (setf caught condition)))
    (check (not handler-ran) "nonfatal handler skips FATAL errors")
    (check (eq caught fatal) "nonfatal handler re-signals the original FATAL"))
  (check (eq :handled
             (handler-case-nonfatal
              (error "test nonfatal")
              (condition
               (if (typep condition 'error) :handled :unexpected))))
         "nonfatal handler handles ordinary errors"))

(defun test-create-table-cleanup-after-fsync-error ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend))
         (db (make-database :name "postgres" :oid 1))
         (original-fsync-index (symbol-function 'fsync-index))
         (caught nil))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) db)
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-index)
                 (lambda (index)
                   (declare (ignore index))
                   (error "injected final index fsync failure")))
           (handler-case
               (exec-create-table
                nil
                '(:name "cleanup_test"
                  :columns ((:name "value" :type :int4 :typmod -1 :unique t))))
             (error (condition)
               (setf caught condition)))
           (check (typep caught 'error)
                  "CREATE TABLE re-signals final index fsync failure")
           (check (null (gethash "cleanup_test" (database-tables db)))
                  "CREATE TABLE cleanup removes table after final fsync failure"))
      (setf (symbol-function 'fsync-index) original-fsync-index))))

(defun test-relation-build-own-xid ()
  (let ((relfilenode 987653)
        (xid 987654))
    (setf (gethash relfilenode *relation-writers*) (list xid))
    (let ((token (acquire-relation-build-lock relfilenode xid)))
      (check (eq token (gethash relfilenode *relation-builders*))
             "relation build ignores its own writer XID")
      (release-relation-build-lock relfilenode token))
    (remhash relfilenode *relation-writers*)))

(defun test-visibility ()
  (let ((snap (make-snapshot-struct :xmin 10 :xmax 1000 :xip '() :curcid 5)))
    ;; Committed insert, never deleted -> visible.
    (clog-set 100 +clog-committed+)
    (check (heap-tuple-visible-p 100 0 0 snap nil) "committed insert visible")
    ;; Aborted insert -> invisible.
    (clog-set 101 +clog-aborted+)
    (check (not (heap-tuple-visible-p 101 0 0 snap nil)) "aborted insert hidden")
    ;; Committed insert, committed delete -> invisible.
    (clog-set 102 +clog-committed+)
    (clog-set 103 +clog-committed+)
    (check (not (heap-tuple-visible-p 102 103 0 snap nil))
           "committed delete hides tuple")
    ;; Committed insert, in-progress delete still in our snapshot -> visible.
    (clog-set 104 +clog-committed+)
    (clog-set 105 +clog-in-progress+)
    (let ((snap2 (make-snapshot-struct :xmin 10 :xmax 1000
                                       :xip '(105) :curcid 5)))
      (check (heap-tuple-visible-p 104 105 0 snap2 nil)
             "concurrent delete leaves tuple visible"))
    ;; Own-xid rule: visible only if inserted before the current command.
    (let ((snap3 (make-snapshot-struct :xmin 10 :xmax 1000 :xip '() :curcid 5)))
      (check (heap-tuple-visible-p 200 0 3 snap3 200)
             "own insert visible for later command")
      (check (not (heap-tuple-visible-p 200 0 5 snap3 200))
             "own insert invisible in same command"))))

(defun test-btree-duplicate-keys ()
  ;; Insert many entries with one key so the leaves holding them must split.
  ;; Repeatedly updating an indexed value produces exactly this shape: a long
  ;; run of index entries with equal keys spread over several leaves.
  (let* ((rel 987654)
         (key (encode-index-key (list 42) (list :int4)))
         (n 2000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel key 0 i))
    (let ((found (btree-search rel key)))
      (check (= (length found) n)
             "duplicate keys are found across leaf splits")
      (check (= (length (remove-duplicates found :test #'equal)) n)
             "every duplicate-key TID is distinct")
      (check (null (btree-search rel (encode-index-key (list 43) (list :int4))))
             "an absent key is not found"))))

(defun test-btree-mixed-duplicate-keys ()
  ;; Interleave two keys so splits produce equal separators for both, which
  ;; stresses the key-range order that search assumes.
  (let* ((rel 987655)
         (ka (encode-index-key (list 1) (list :int4)))
         (kb (encode-index-key (list 2) (list :int4)))
         (n 3000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel (if (evenp i) ka kb) 0 i))
    (let ((fa (btree-search rel ka))
          (fb (btree-search rel kb)))
      (check (= (length fa) (/ n 2)) "even entries found (mixed keys)")
      (check (= (length fb) (/ n 2)) "odd entries found (mixed keys)")
      (check (every (lambda (tid) (evenp (cdr tid))) fa)
             "key A search returned only key-A TIDs")
      (check (every (lambda (tid) (oddp (cdr tid))) fb)
             "key B search returned only key-B TIDs"))))

(defun test-parse-error-aborts-transaction ()
  (let ((path (merge-pathnames "parse-error.out" *pgdata*)))
    (setf (backend-state *backend*) :in-progress
          (backend-explicit *backend*) t
          (backend-command-id *backend*) 0)
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select from;"))
    (check (eq (backend-state *backend*) :aborted)
           "a parse error aborts an explicit transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select 1;"))
    (check (eq (backend-state *backend*) :aborted)
           "aborted transaction stays aborted")
    (abort-transaction)
    (check (eq (backend-state *backend*) :idle)
           "abort clears the transaction state")))

(defun run-unit-tests ()
  (setf *test-failures* 0
        *test-checks* 0)
  (let ((dir (merge-pathnames
              (format nil "pg-unit-~a/" (random 1000000))
              #p"/tmp/opencode/")))
    (ensure-directories-exist dir)
    (let ((*pgdata* dir)
          (*backend* (make-backend)))
      (clog-init)
      (test-swank-arguments)
      (test-protocol-packet-limits)
      (test-page-roundtrip)
      (test-wal-catalog-codec-and-page-checksums)
      (test-wal-record-codec)
      (test-wal-chunked-catalog-record)
      (test-wal-catalog-terminal-validation-precedes-page-redo)
      (test-wal-segment-rotation)
      (test-wal-checkpoint-skips-obsolete-prefix)
      (test-wal-missing-interior-segment)
      (test-live-page-items-stop)
      (test-live-page-item-guard)
      (test-page-compaction)
      (test-tuple-roundtrip)
      (test-full-xid-tuple-format)
      (test-xid-tuple-update-roundtrip)
      (test-heap-update-chain-follow)
      (test-heap-free-space-map)
      (test-postgres-special-xid-statuses)
      (test-xid-control-state)
      (test-key-order)
      (test-index-catalog-roundtrip)
      (test-sql-strict-let-evaluation)
      (test-error-cleanup-and-fatal-rethrow)
      (test-create-table-cleanup-after-fsync-error)
      (test-relation-build-own-xid)
      (test-visibility)
      (test-btree-duplicate-keys)
      (test-btree-mixed-duplicate-keys)
      (test-session-setting-rollback)
      (test-parse-error-aborts-transaction)
      (test-xid-wraparound-freezes-tuples)
      (test-xid-wraparound-fault-recovery)
      (test-xid-wraparound-coordinator-cancel))
    (ignore-errors
     (uiop:delete-directory-tree dir :validate t :if-does-not-exist :ignore)))
  (format t "~&unit-tests: ~a checks, ~a failures~%"
          *test-checks* *test-failures*)
  (when (plusp *test-failures*)
    (sb-ext:exit :code 1)))

(run-unit-tests)

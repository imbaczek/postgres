;;;; wal-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(defun call-with-wal-scan-test-files (segments durable-lsn function)
  "Run FUNCTION with isolated WAL files and a known durable boundary.
Each entry in SEGMENTS contains a segment number followed by its byte vectors."
  (let* ((*pgdata* (merge-pathnames
                    (format nil "pg-wal-scan-~d-~d/" (sb-posix:getpid) (random 1000000))
                    (uiop:temporary-directory)))
         (*wal-checkpoint-lsn* 0)
         (*wal-checkpoint-snapshot* nil)
         (*wal-durable-lsn* durable-lsn)
         (*wal-insert-timeline* 1)
         (*wal-segment-namespace* :legacy))
    (ensure-directories-exist (wal-directory))
    (unwind-protect
         (progn
           (dolist (entry segments)
             (with-open-file (stream (wal-segment-path (first entry))
                                     :direction :output
                                     :element-type '(unsigned-byte 8)
                                     :if-exists :error)
               (dolist (bytes (rest entry))
                 (write-sequence bytes stream))))
           (funcall function))
      (uiop:delete-directory-tree *pgdata* :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-scan-rejects-impossible-rotation-gap
    (let* ((size +wal-segment-size+)
           (end (+ size 64))
           (segments
             (list (list 0 (wal-encode-record +wal-record-batch-start+ 0 0 #())
                         (wal-encode-record +wal-record-maintenance+ 0 32 #()))
                   (list 1 (wal-encode-record +wal-record-batch-start+ 0 size #())
                         (wal-encode-record +wal-record-maintenance+ 0 (+ size 32) #())))))
      (call-with-wal-scan-test-files
       segments end
       (lambda ()
         (let ((seen nil))
           (rove:ok
            (handler-case
                (progn
                  (wal-scan (lambda (record) (push (wal-record-start record) seen)))
                  nil)
              (error (condition)
                (search "rotation gap" (princ-to-string condition))))
            "CRC-valid short segments cannot hide a missing durable suffix")
           (rove:ok (equal (reverse seen) '(0 32))
                    "the impossible successor never reaches the recovery callback"))
         (rove:ok
          (handler-case
              (progn
                (wal-scan-lineage
                 (lambda (&rest arguments) (declare (ignore arguments)))
                 (list (make-timeline-history-entry :timeline 1 :begin 0))
                 0 end :namespace :legacy)
                nil)
            (error (condition)
              (search "rotation gap" (princ-to-string condition))))
          "the primary and lineage readers reject the same impossible gap")
         (dolist (entry segments)
           (with-open-file (stream (wal-segment-path (first entry))
                                   :element-type '(unsigned-byte 8))
             (let ((expected (apply #'concatenate '(vector (unsigned-byte 8)) (rest entry)))
                   (actual (make-array (file-length stream) :element-type '(unsigned-byte 8))))
               (read-sequence actual stream)
               (rove:ok (equalp actual expected) "gap rejection leaves segment bytes intact"))))
         (rove:ok
          (handler-case
              (progn (wal-scan (constantly nil) 0 :end-lsn end) nil)
            (error (condition)
              (search "rotation gap" (princ-to-string condition))))
          "a bounded scan must reject an impossible gap before its requested end")
         (let ((*wal-durable-lsn* 64))
           (rove:ok
            (handler-case
                (progn (wal-scan (constantly nil)) nil)
              (error (condition)
                (search "rotation gap" (princ-to-string condition))))
            "a complete unflushed tail cannot be adopted across an impossible gap"))
         (rove:ok (= 64 (wal-scan (constantly nil) 0 :end-lsn 64))
                  "a bounded scan may stop before the damaged suffix")
         (rove:ok (= 64 (wal-scan (lambda (record)
                                    (when (= 64 (wal-record-end record)) :stop))))
                  "a callback may stop before the damaged suffix")
         (rove:ok (= end (wal-scan (constantly nil) size :end-lsn end))
                  "an exact retained scan does not inspect the preceding prefix")))))

(rove:deftest test-wal-scan-accepts-legal-rotation
    (let* ((size +wal-segment-size+)
           (prefix-end (- size 16))
           (end (+ size 64))
           (prefix (wal-encode-record
                    +wal-record-maintenance+ 0 0
                    (make-array (- prefix-end +wal-header-size+)
                                :element-type '(unsigned-byte 8) :initial-element 0))))
      (call-with-wal-scan-test-files
       (list (list 0 prefix)
             (list 1 (wal-encode-record +wal-record-batch-start+ 0 size #())
                   (wal-encode-record +wal-record-maintenance+ 0 (+ size 32) #())))
       end
       (lambda ()
         (let ((seen nil))
           (rove:ok (= end (wal-scan
                            (lambda (record) (push (wal-record-start record) seen)))))
           (rove:ok (equal (nreverse seen) (list 0 size (+ size 32)))
                    "a 32-byte successor may rotate past 16 bytes of unused capacity"))
         (rove:ok (= end (wal-scan (constantly nil) prefix-end :end-lsn end))
                  "an exact retained cursor may precede a legal rotation")
         (rove:ok
          (handler-case
              (progn (wal-scan (constantly nil) 0 :end-lsn (1- size)) nil)
            (error () t))
          "a bounded scan cannot end inside unused rotation capacity")))))

(rove:deftest test-wal-scan-exact-fit-and-full-segment-boundary
    (let* ((size +wal-segment-size+)
           (prefix-end (- size 32))
           (end (+ size 32))
           (prefix (wal-encode-record
                    +wal-record-maintenance+ 0 0
                    (make-array (- prefix-end +wal-header-size+)
                                :element-type '(unsigned-byte 8) :initial-element 0)))
           (fitting (wal-encode-record +wal-record-batch-start+ 0 prefix-end #()))
           (successor (wal-encode-record +wal-record-maintenance+ 0 size #())))
      (call-with-wal-scan-test-files
       (list (list 0 prefix fitting) (list 1 successor)) end
       (lambda ()
         (let ((seen nil))
           (rove:ok (= end (wal-scan
                            (lambda (record) (push (wal-record-start record) seen)))))
           (rove:ok (equal (nreverse seen) (list 0 prefix-end size))
                    "a fitting record fills its segment before the next record rotates"))
         (rove:ok (= end (wal-scan (constantly nil) prefix-end :end-lsn end)))
         (rove:ok (= end (wal-scan (constantly nil) size :end-lsn end)))
         (rove:ok (= size (wal-scan (constantly nil) 0 :end-lsn size)))))
      (call-with-wal-scan-test-files
       (list (list 0 prefix) (list 1 successor)) end
       (lambda ()
         (rove:ok
          (handler-case
              (progn (wal-scan (constantly nil)) nil)
            (error (condition)
              (search "rotation gap" (princ-to-string condition))))
          "a successor that exactly fits the remaining capacity cannot rotate")))))

(rove:deftest test-wal-scan-rejects-empty-intermediate-segment
    (let* ((size +wal-segment-size+)
           (prefix-end (- size 16))
           (prefix (wal-encode-record
                    +wal-record-maintenance+ 0 0
                    (make-array (- prefix-end +wal-header-size+)
                                :element-type '(unsigned-byte 8) :initial-element 0))))
      (call-with-wal-scan-test-files
       (list (list 0 prefix) (list 1)
             (list 2 (wal-encode-record +wal-record-maintenance+ 0 (* 2 size) #())))
       (+ (* 2 size) 32)
       (lambda ()
         (let ((seen nil))
           (rove:ok
            (handler-case
                (progn
                  (wal-scan (lambda (record) (push (wal-record-start record) seen)))
                  nil)
              (error (condition)
                (search "rotation gap" (princ-to-string condition))))
            "consecutive filenames cannot hide an empty intermediate segment")
           (rove:ok (equal seen '(0)) "records after the empty segment are not visited"))))))

(rove:deftest test-wal-scan-preserves-durable-tail-repair-boundary
    (let* ((start (wal-encode-record +wal-record-batch-start+ 0 0 #()))
           (terminal (wal-encode-record +wal-record-maintenance+ 0 32 #()))
           (tail (wal-encode-record
                  +wal-record-maintenance+ 0 64
                  (make-array 8 :element-type '(unsigned-byte 8) :initial-element 0)))
           (bad-crc (copy-seq tail)))
      (setf (aref bad-crc 28) (logxor 1 (aref bad-crc 28)))
      (dolist (damaged (list (subseq tail 0 7) (subseq tail 0 35) bad-crc))
        (call-with-wal-scan-test-files
         (list (list 0 start terminal damaged)) 64
         (lambda ()
           (rove:ok (= 64 (wal-scan (constantly nil)))
                    "an incomplete or corrupt unflushed tail is discarded")
           (with-open-file (stream (wal-segment-path 0) :element-type '(unsigned-byte 8))
             (rove:ok (= 64 (file-length stream))))))
        (call-with-wal-scan-test-files
         (list (list 0 start terminal damaged)) 104
         (lambda ()
           (rove:ok
            (handler-case
                (progn (wal-scan (constantly nil)) nil)
              (error () t))
            "the same damage before the durable boundary stops the scan")
           (with-open-file (stream (wal-segment-path 0) :element-type '(unsigned-byte 8))
             (rove:ok (= (+ 64 (length damaged)) (file-length stream))
                      "durable corruption is never truncated")))))))

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

(rove:deftest test-wal-dirty-scratch-shared-and-batch-lifecycle
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-store-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory)
           (*query-temp-directory* directory)
           (*query-temp-file-limit-bytes* 0)
           (*wal-dirty-resident-limit-pages* 0)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (*backend* (make-backend :xid 44))
           (*wal-row-page-write-p* t)
           (page (page-init (make-page))))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (aref page 100) 1)
             (wal-buffer-write 7310 0 page)
             (rove:ok (= 0 (wal-dirty-store-resident *wal-dirty-store*)))
             (rove:ok (= 1 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (rove:ok (= 1 (aref (wal-buffer-read 7310 0) 100)))
             (rove:ok (equal '(44 1) (gethash (wal-page-key 7310 0) *wal-page-cache-xids*)))
             (rove:ok (handler-case
                          (with-wal-page-batch
                            (setf (aref page 100) 2)
                            (wal-buffer-write 7310 0 page)
                            (wal-buffer-write 7310 1 page)
                            (error "discard batch"))
                        (error (condition) (search "discard batch" (princ-to-string condition)))))
             (rove:ok (= 1 (aref (wal-buffer-read 7310 0) 100)))
             (rove:ok (= 1 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (setf (backend-xid *backend*) 55)
             (with-wal-page-batch
               (wal-buffer-write 7310 0 page)
               (wal-buffer-write 7310 1 page))
             (rove:ok (= 2 (aref (wal-buffer-read 7310 0) 100)))
             (rove:ok (= 2 (wal-buffer-nblocks 7310)))
             (rove:ok (equal '(44 55) (sort (remove-if (lambda (xid) (< xid 3))
                                                       (copy-list (gethash (wal-page-key 7310 0)
                                                                           *wal-page-cache-xids*))) #'<)))
             (let ((next (wal-dirty-store-next-slot *wal-dirty-store*)))
               (dotimes (i 5) (wal-buffer-write 7310 0 page))
               (rove:ok (<= (wal-dirty-store-next-slot *wal-dirty-store*) (1+ next))))
             (wal-clear-page-cache)
             (rove:ok (null *wal-dirty-store*))
             (rove:ok (null (uiop:directory-files directory))))
        (wal-clear-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-seeded-batch-model
    ;; Fixed LCG seed makes every failed batch and overwritten slot reproducible.
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-model-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*pgdata* directory)
           (*query-temp-directory* directory)
           (*query-temp-file-limit-bytes* 0)
           (*wal-dirty-resident-limit-pages* 0)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (*backend* (make-backend :xid 44))
           (*wal-row-page-write-p* t)
           (model (make-array 12 :initial-element nil))
           (seed 1299709)
           (rolled-back 0))
      (ensure-directories-exist directory)
      (unwind-protect
           (labels ((draw (bound)
                      (setf seed (mod (+ (* seed 1664525) 1013904223) 4294967296))
                      (mod (ash seed -8) bound))
                    (page-with (value)
                      (let ((page (page-init (make-page))))
                        (setf (aref page 100) value)
                        page)))
             (dotimes (batch 300)
               (let ((updates nil)
                     (abort-p (zerop (draw 4))))
                 (dotimes (i (1+ (draw 5)))
                   (push (cons (draw 12) (draw 256)) updates))
                 (setf updates (nreverse updates))
                 (catch 'rollback
                   (with-wal-page-batch
                     (dolist (update updates)
                       (wal-buffer-write 7310 (car update) (page-with (cdr update))))
                     (when abort-p (throw 'rollback nil))))
                 (if abort-p
                     (incf rolled-back)
                     (dolist (update updates)
                       (setf (aref model (car update)) (cdr update))))
                 (dotimes (key 12)
                   (multiple-value-bind (reference present)
                       (gethash (wal-page-key 7310 key) *wal-page-cache*)
                     (declare (ignore reference))
                     (rove:ok (eql present (not (null (aref model key)))))
                     (when present
                       (rove:ok (= (aref model key) (aref (wal-buffer-read 7310 key) 100))))))))
             (rove:ok (> rolled-back 0))
             (rove:ok (= 0 (wal-dirty-store-resident *wal-dirty-store*)))
             (rove:ok (= (hash-table-count *wal-page-cache*) (wal-dirty-store-disk-refs *wal-dirty-store*)))
             ;; At most 12 shared and five private references coexist.
             (rove:ok (<= (wal-dirty-store-next-slot *wal-dirty-store*) 17))
             (wal-clear-page-cache)
             (rove:ok (null *wal-dirty-store*))
             (rove:ok (null (uiop:directory-files directory))))
        (wal-clear-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-resident-budget
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-budget-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-dirty-resident-limit-pages* 2)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (with-wal-page-batch
               (dotimes (block 6)
                 (let ((page (page-init (make-page))))
                   (setf (aref page 100) block)
                   (wal-buffer-write 7310 block page))))
             (rove:ok (= 2 (wal-dirty-store-resident *wal-dirty-store*)))
             (rove:ok (= 4 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (dotimes (block 6)
               (rove:ok (= block (aref (wal-buffer-read 7310 block) 100))))
             (rove:ok (= 2 (wal-dirty-store-resident *wal-dirty-store*)))
             (wal-clear-page-cache)
             (rove:ok (null *wal-dirty-store*))
             (rove:ok (null (uiop:directory-files directory))))
        (wal-clear-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-corruption-fails-stop
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-corrupt-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-dirty-resident-limit-pages* 0)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (old-fail (symbol-function 'wal-fail-stop))
           (old-read (symbol-function 'smgr-read))
           (fallback nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop) (lambda (condition) (error condition))
                   (symbol-function 'smgr-read)
                   (lambda (&rest args) (declare (ignore args)) (setf fallback t) nil))
             (wal-buffer-write 7310 0 (page-init (make-page)))
             (let ((file (logical-spool-stream (wal-dirty-store-spool *wal-dirty-store*))))
               (file-position file 100)
               (write-byte 99 file)
               (finish-output file))
             (rove:ok (handler-case
                          (progn (wal-buffer-read 7310 0) nil)
                        (wal-dirty-store-error (condition)
                          (search "checksum mismatch" (princ-to-string condition)))))
             (rove:ok *wal-poisoned*)
             (rove:ok (not fallback))
             (wal-clear-page-cache)
             (rove:ok (null *wal-dirty-store*))
             (rove:ok (null (uiop:directory-files directory))))
        (setf (symbol-function 'wal-fail-stop) old-fail
              (symbol-function 'smgr-read) old-read)
        (wal-clear-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-write-and-batch-failures-preserve-shared
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-failure-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-dirty-resident-limit-pages* 1)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (*backend* (make-backend :xid 44))
           (*wal-row-page-write-p* t)
           (old-fail (symbol-function 'wal-fail-stop))
           (old-ensure (symbol-function 'wal-dirty-ensure-spool))
           (old-merge (symbol-function 'wal-dirty-batch-contributors))
           (page (page-init (make-page)))
           (key (wal-page-key 7310 0)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop) (lambda (condition) (error condition)))
             (setf (aref page 100) 1)
             (wal-buffer-write 7310 0 page)
             (setf (aref page 100) 2
                   (symbol-function 'wal-dirty-ensure-spool)
                   (lambda (store) (declare (ignore store)) (error "injected scratch failure")))
             (rove:ok (handler-case (progn (wal-buffer-write 7310 0 page) nil)
                        (wal-dirty-store-error (condition)
                          (search "injected scratch failure" (princ-to-string condition)))))
             (rove:ok *wal-poisoned*)
             (rove:ok (= 1 (aref (wal-dirty-read-reference key (gethash key *wal-page-cache*)) 100)))
             (rove:ok (equal '(44 1) (gethash key *wal-page-cache-xids*)))
             (setf (symbol-function 'wal-dirty-ensure-spool) old-ensure
                   *wal-poisoned* nil)
             (setf (symbol-function 'wal-dirty-batch-contributors)
                   (lambda ()
                     (error "injected batch publication failure")))
             (rove:ok (handler-case
                          (with-wal-page-batch
                            (wal-buffer-write 7310 0 page)
                            (wal-buffer-write 7310 1 page))
                        (wal-dirty-store-error (condition)
                          (search "injected batch publication failure" (princ-to-string condition)))))
             (rove:ok *wal-poisoned*)
             (rove:ok (= 1 (hash-table-count *wal-page-cache*)))
             (rove:ok (= 1 (aref (wal-dirty-read-reference key (gethash key *wal-page-cache*)) 100)))
             (rove:ok (= 1 (wal-dirty-store-resident *wal-dirty-store*)))
             (rove:ok (zerop (wal-dirty-store-disk-refs *wal-dirty-store*))))
        (setf (symbol-function 'wal-fail-stop) old-fail
              (symbol-function 'wal-dirty-ensure-spool) old-ensure
              (symbol-function 'wal-dirty-batch-contributors) old-merge)
        (wal-clear-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-batch-publication-error-fails-stop-with-owned-pages
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-publish-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*wal-dirty-resident-limit-pages* 0)
           (*wal-dirty-store* nil)
           (*wal-page-cache* (make-hash-table :test #'equal))
           (*wal-page-cache-xids* (make-hash-table :test #'equal))
           (*wal-ready* t) (*wal-poisoned* nil)
           (old-fail (symbol-function 'wal-fail-stop))
           (old-release (symbol-function 'wal-dirty-release-reference))
           (page (page-init (make-page)))
           (key (wal-page-key 7310 0))
           (previous nil))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (setf (symbol-function 'wal-fail-stop) (lambda (condition) (error condition)))
             (setf (aref page 100) 1)
             (wal-buffer-write 7310 0 page)
             (setf previous (gethash key *wal-page-cache*)
                   (aref page 100) 2
                   (symbol-function 'wal-dirty-release-reference)
                   (lambda (reference)
                     (if (eq reference previous)
                         (error "injected published-reference failure")
                         (funcall old-release reference))))
             (rove:ok
              (handler-case
                  (with-wal-page-batch
                    (wal-buffer-write 7310 0 page)
                    (wal-buffer-write 7310 1 page))
                (wal-dirty-store-error (condition)
                  (search "injected published-reference failure" (princ-to-string condition)))))
             (rove:ok *wal-poisoned*)
             (rove:ok (= 2 (hash-table-count *wal-page-cache*)))
             (rove:ok (= 3 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (dolist (block '(0 1))
               (rove:ok (not (member (wal-dirty-reference-slot
                                      (gethash (wal-page-key 7310 block) *wal-page-cache*))
                                     (wal-dirty-store-free-slots *wal-dirty-store*)))))
             (rove:ok (= 2 (aref (wal-dirty-read-reference key (gethash key *wal-page-cache*)) 100)))
             (rove:ok (= 2 (aref (wal-dirty-read-reference
                                  (wal-page-key 7310 1)
                                  (gethash (wal-page-key 7310 1) *wal-page-cache*)) 100))))
        (setf (symbol-function 'wal-fail-stop) old-fail
              (symbol-function 'wal-dirty-release-reference) old-release)
        (when previous (wal-dirty-release-reference previous))
        (wal-clear-page-cache)
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest test-wal-dirty-scratch-cross-thread-contributors
    (let* ((directory (merge-pathnames
                       (format nil "pg-dirty-threads-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (names '(*query-temp-directory* *wal-dirty-resident-limit-pages* *wal-dirty-store*
                    *wal-page-cache* *wal-page-cache-xids* *wal-ready* *wal-poisoned*))
           (saved (mapcar (lambda (name) (cons name (symbol-value name))) names))
           (key (wal-page-key 7310 0)))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             ;; Child threads use the same global WAL cache and scratch owner.
             (setf *query-temp-directory* directory
                   *wal-dirty-resident-limit-pages* 0
                   *wal-dirty-store* nil
                   *wal-page-cache* (make-hash-table :test #'equal)
                   *wal-page-cache-xids* (make-hash-table :test #'equal)
                   *wal-ready* t *wal-poisoned* nil)
             (let* ((gate (sb-thread:make-semaphore :count 0))
                    (threads
                      (loop for xid in '(44 55)
                            collect
                            (let ((id xid))
                              (sb-thread:make-thread
                               (lambda ()
                                 (let ((*backend* (make-backend :xid id))
                                       (*wal-row-page-write-p* t)
                                       (page (page-init (make-page))))
                                   (setf (aref page 100) id)
                                   (sb-thread:wait-on-semaphore gate)
                                   (wal-buffer-write 7310 0 page))))))))
               (sb-thread:signal-semaphore gate 2)
               (dolist (thread threads) (sb-thread:join-thread thread)))
             (rove:ok (member (aref (wal-buffer-read 7310 0) 100) '(44 55)))
             (rove:ok (equal '(44 55) (sort (remove-if (lambda (xid) (< xid 3))
                                                       (copy-list (gethash key *wal-page-cache-xids*))) #'<)))
             (rove:ok (= 1 (wal-dirty-store-disk-refs *wal-dirty-store*)))
             (wal-clear-page-cache)
             (rove:ok (null (uiop:directory-files directory))))
        (wal-clear-page-cache)
        (dolist (entry saved) (setf (symbol-value (car entry)) (cdr entry)))
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

;;;; Durable receive progress must survive loss of the receiver process.
(in-package #:pg-physical-tests)

(defun call-with-physical-store-directory (function)
  (let ((root (merge-pathnames (format nil "pg-physical-store-~d-~a/" (sb-posix:getpid) (gensym))
                               (uiop:temporary-directory))))
    (ensure-directories-exist (merge-pathnames "dummy" root))
    (unwind-protect (funcall function root)
      (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore))))

(defmacro with-physical-store-directory ((root) &body body)
  `(call-with-physical-store-directory (lambda (,root) ,@body)))

(defun sample-physical-record (&optional (start 0))
  (pg::wal-encode-record pg::+wal-record-batch-start+ 0 start (empty-octets)))

(deftest physical-store-compaction-preserves-unflushed-input
    (with-physical-store-directory (root)
      (let* ((segment pg::+wal-segment-size+)
             (base (- segment 40))
             (store (pg::physical-store-open root 123 1 :create t :start-lsn base))
             (seed #xCBF29CE484222325)
             (expected seed)
             (anchor nil)
             (seen nil))
        (unwind-protect
             (progn
               (dolist (cursor (list base segment (+ segment 32)))
                 (let ((raw (sample-physical-record cursor)))
                   (pg::physical-store-input store cursor raw)
                   (setf expected (pg::physical-store-update-digest expected raw))
                   (when (= cursor segment) (setf anchor expected))))
               (pg::physical-store-flush store)
               (pg::physical-store-input store (+ segment 64) (sample-physical-record (+ segment 64)))
               (let ((partial (sample-physical-record (+ segment 96))))
                 (pg::physical-store-input store (+ segment 96) (subseq partial 0 10))
                 (pg::physical-store-compact
                  store (+ segment 32)
                  (lambda (digest)
                    (ok (= anchor digest))
                    (ok (probe-file (pg::physical-store-segment-path store 0)))))
                 (ok (not (probe-file (pg::physical-store-segment-path store 0))))
                 (ok (= base (pg::physical-store-base-lsn store)))
                 (ok (= (+ segment 32) (pg::physical-store-floor-lsn store)))
                 (ok (= (+ segment 64) (pg::physical-store-flushed-lsn store)))
                 (ok (= expected (pg::physical-store-flushed-digest store)))
                 (ok (= (+ segment 96) (pg::physical-store-written-lsn store)))
                 (ok (= 10 (pg::physical-wal-decoder-used (pg::physical-store-decoder store))))
                 (pg::physical-store-map-durable-records
                  store (lambda (record raw) (declare (ignore raw))
                          (push (pg::wal-record-start record) seen)))
                 (ok (equal seen (list (+ segment 32))))
                 ;; Neither complete unflushed writes nor a partial record were discarded.
                 (pg::physical-store-input store (+ segment 106) (subseq partial 10))
                 (pg::physical-store-flush store)
                 (ok (= (+ segment 128) (pg::physical-store-flushed-lsn store)))))
          (pg::physical-store-close store))
        (let ((reopened (pg::physical-store-open root 123 1)))
          (unwind-protect
               (progn
                 (ok (= (+ segment 32) (pg::physical-store-floor-lsn reopened)))
                 (ok (= anchor (pg::physical-store-prefix-digest reopened (+ segment 32))))
                 (ok (= (+ segment 128) (pg::physical-store-flushed-lsn reopened))))
            (pg::physical-store-close reopened))))))

(deftest physical-store-compaction-publication-crash-states
    (dolist (point '(:after-restartpoint-publication :after-floor-publication))
      (with-physical-store-directory (root)
        (let* ((segment pg::+wal-segment-size+)
               (base (- segment 40))
               (store (pg::physical-store-open root 123 1 :create t :start-lsn base))
               (published nil))
          (unwind-protect
               (progn
                 (pg::physical-store-input store base (sample-physical-record base))
                 (pg::physical-store-input store segment (sample-physical-record segment))
                 (pg::physical-store-flush store)
                 (let ((pg::*physical-store-test-hook*
                         (lambda (actual ignored)
                           (declare (ignore ignored))
                           (when (eq point actual) (error "publication interruption")))))
                   (ok (intake-fails-p
                        (lambda ()
                          (pg::physical-store-compact store (+ segment 32)
                                                      (lambda (digest)
                                                        (setf published digest)))))))
                 (ok published)
                 (ok (probe-file (pg::physical-store-segment-path store 0))))
            (pg::physical-store-close store))
          (let ((reopened (pg::physical-store-open root 123 1)))
            (unwind-protect
                 (progn
                   (ok (= (pg::physical-store-floor-lsn reopened)
                          (if (eq point :after-floor-publication) (+ segment 32) base))))
              (pg::physical-store-close reopened)))))))

(deftest physical-store-compaction-uses-flush-checksum-on-reopen
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-compact store 32 (lambda (digest) (declare (ignore digest)))))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (progn
               (ok (= 32 (pg::physical-store-floor-lsn store)))
               (ok (= 32 (pg::physical-store-written-lsn store)))
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-flush store)
               (ok (= 64 (pg::physical-store-flushed-lsn store))))
          (pg::physical-store-close store)))))

(deftest physical-store-prefix-digest-exact-boundaries
    (let* ((segment pg::+wal-segment-size+)
           (base (- segment 40))
           (first (sample-physical-record base))
           (second (sample-physical-record segment))
           (seed #xCBF29CE484222325)
           (first-digest (pg::physical-store-update-digest seed first))
           (final-digest (pg::physical-store-update-digest first-digest second)))
      (with-physical-store-directory (root)
        (let ((store (pg::physical-store-open root 123 1 :create t :start-lsn base)))
          (unwind-protect
               (progn
                 (pg::physical-store-input store base first)
                 (pg::physical-store-input store segment second)
                 (pg::physical-store-flush store)
                 (ok (= seed (pg::physical-store-prefix-digest store base)))
                 (ok (= first-digest
                        (pg::physical-store-prefix-digest store (+ base 32))))
                 (ok (= final-digest
                        (pg::physical-store-prefix-digest store (+ segment 32))))
                 (pg::physical-store-input store (+ segment 32)
                                           (sample-physical-record (+ segment 32)))
                 (ok (= final-digest
                        (pg::physical-store-prefix-digest store (+ segment 32)))))
            (pg::physical-store-close store)))
        (let ((store (pg::physical-store-open root 123 1)))
          (unwind-protect
               (ok (= first-digest
                      (pg::physical-store-prefix-digest store (+ base 32))))
            (pg::physical-store-close store))))))

(deftest physical-store-prefix-digest-rejects-unacknowledged-and-gap-cursors
    (let* ((segment pg::+wal-segment-size+)
           (base (- segment 40)))
      (dolist (cursor (list (1- base) (1+ base) (+ base 33) segment
                            (+ segment 64)))
        (with-physical-store-directory (root)
          (let ((store (pg::physical-store-open root 123 1 :create t :start-lsn base)))
            (unwind-protect
                 (progn
                   (pg::physical-store-input store base (sample-physical-record base))
                   (pg::physical-store-input store segment (sample-physical-record segment))
                   (pg::physical-store-flush store)
                   (pg::physical-store-input store (+ segment 32)
                                             (sample-physical-record (+ segment 32)))
                   (ok (intake-fails-p
                        (lambda () (pg::physical-store-prefix-digest store cursor))))
                   (ok (pg::physical-store-failed-p store)))
              (pg::physical-store-close store)))))))

(deftest physical-store-prefix-digest-validates-beyond-requested-anchor
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-flush store)
               (with-open-file (stream (merge-pathnames "0000000000000000.wal" root)
                                       :direction :io :if-exists :overwrite
                                       :element-type '(unsigned-byte 8))
                 (file-position stream 60)
                 (write-byte 0 stream)
                 (pg::fsync-stream stream))
               (ok (intake-fails-p
                    (lambda () (pg::physical-store-prefix-digest store 32))))
               (ok (pg::physical-store-failed-p store)))
          (pg::physical-store-close store)))))

(deftest physical-store-durable-reader-resumes-across-rotation
    (let* ((segment pg::+wal-segment-size+)
           (base (- segment 40))
           (previous-end (+ base 32)))
      (dolist (cursor (list base previous-end (+ previous-end 1) segment (+ segment 32)))
        (with-physical-store-directory (root)
          (let ((store (pg::physical-store-open root 123 1 :create t :start-lsn base))
                (seen '()))
            (unwind-protect
                 (progn
                   (pg::physical-store-input store base (sample-physical-record base))
                   (pg::physical-store-input store segment (sample-physical-record segment))
                   (pg::physical-store-flush store)
                   (flet ((visit (record raw)
                            (declare (ignore raw))
                            (push (pg::wal-record-start record) seen)))
                     (if (member cursor (list (+ previous-end 1) segment))
                         (progn
                           (ok (intake-fails-p
                                (lambda ()
                                  (pg::physical-store-map-durable-records
                                   store #'visit :start-lsn cursor))))
                           (ok (null seen)))
                         (progn
                           (ok (= (+ segment 32)
                                  (pg::physical-store-map-durable-records
                                   store #'visit :start-lsn cursor)))
                           (ok (equal (reverse seen)
                                      (cond ((= cursor base) (list base segment))
                                            ((= cursor previous-end) (list segment))
                                            (t nil))))))))
              (pg::physical-store-close store)))))))

(deftest physical-store-first-record-rotates-after-base
    (with-physical-store-directory (root)
      (let* ((segment pg::+wal-segment-size+)
             (base (- segment 8))
             (store (pg::physical-store-open root 123 1 :create t :start-lsn base)))
        (unwind-protect
             (progn
               (pg::physical-store-input store segment (sample-physical-record segment))
               (pg::physical-store-flush store))
          (pg::physical-store-close store))
        (let ((reopened (pg::physical-store-open root 123 1))
              (seen nil))
          (unwind-protect
               (progn
                 (ok (= (+ segment 32)
                        (pg::physical-store-map-durable-records
                         reopened (lambda (record raw)
                                    (declare (ignore raw))
                                    (push (pg::wal-record-start record) seen)))))
                 (ok (equal seen (list segment)))
                 (ok (= #xCBF29CE484222325
                        (pg::physical-store-prefix-digest reopened base)))
                 (ok (= (pg::physical-store-update-digest
                         #xCBF29CE484222325 (sample-physical-record segment))
                        (pg::physical-store-prefix-digest reopened (+ segment 32)))))
            (pg::physical-store-close reopened))))))

(deftest physical-store-durable-reader-callback-error-latches-store
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t))
            (calls 0))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-flush store)
               (ok (intake-fails-p
                    (lambda ()
                      (pg::physical-store-map-durable-records
                       store (lambda (record raw)
                               (declare (ignore record raw))
                               (when (= (incf calls) 2)
                                 (error "replay callback failed")))))))
               (ok (= calls 2))
               (ok (pg::physical-store-failed-p store))
               (ok (intake-fails-p (lambda () (pg::physical-store-flush store)))))
          (pg::physical-store-close store)))))

(deftest physical-store-durable-reader-excludes-unflushed-records
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t))
            (seen '()))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (ok (zerop (pg::physical-store-map-durable-records
                           store (lambda (record raw)
                                   (declare (ignore raw))
                                   (push (pg::wal-record-start record) seen)))))
               (ok (null seen))
               (pg::physical-store-flush store)
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (ok (= 32 (pg::physical-store-map-durable-records
                          store (lambda (record raw)
                                  (ok (equalp raw (sample-physical-record)))
                                  (push (pg::wal-record-start record) seen)))))
               (ok (equal seen '(0)))
               (setf seen nil)
               (pg::physical-store-flush store)
               (ok (= 64 (pg::physical-store-map-durable-records
                          store (lambda (record raw)
                                  (declare (ignore raw))
                                  (push (pg::wal-record-start record) seen))
                          :start-lsn 32)))
               (ok (equal seen '(32))))
          (pg::physical-store-close store)))))

(deftest physical-store-durable-reader-rejects-invalid-cursor-before-callback
    (dolist (cursor '(-1 1 33 65))
      (with-physical-store-directory (root)
        (let ((store (pg::physical-store-open root 123 1 :create t))
              (calls 0))
          (unwind-protect
               (progn
                 (pg::physical-store-input store 0 (sample-physical-record))
                 (pg::physical-store-input store 32 (sample-physical-record 32))
                 (pg::physical-store-flush store)
                 (ok (intake-fails-p
                      (lambda ()
                        (pg::physical-store-map-durable-records
                         store (lambda (record raw)
                                 (declare (ignore record raw))
                                 (incf calls))
                         :start-lsn cursor))))
                 (ok (zerop calls)))
            (pg::physical-store-close store))))))

(deftest physical-store-durable-reader-validates-prefix-before-callback
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t))
            (calls 0))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-flush store)
               (with-open-file (stream (merge-pathnames "0000000000000000.wal" root)
                                       :direction :io :if-exists :overwrite
                                       :element-type '(unsigned-byte 8))
                 (file-position stream 60)
                 (write-byte 0 stream)
                 (pg::fsync-stream stream))
               (ok (intake-fails-p
                    (lambda ()
                      (pg::physical-store-map-durable-records
                       store (lambda (record raw)
                               (declare (ignore record raw))
                               (incf calls))))))
               (ok (zerop calls))
               (ok (pg::physical-store-failed-p store)))
          (pg::physical-store-close store)))))

(deftest physical-store-network-reconnect-resets-to-flush
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-input store 64 (subseq (sample-physical-record 64) 0 7))
               (ok (= 32 (pg::physical-store-reset-unflushed store)))
               (ok (= 32 (pg::physical-store-written-lsn store)))
               (ok (zerop (pg::physical-wal-decoder-used (pg::physical-store-decoder store))))
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (ok (= 64 (pg::physical-store-flush store))))
          (pg::physical-store-close store)))))

(deftest physical-store-write-is-not-flush
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (subseq (sample-physical-record) 0 7))
               (ok (zerop (pg::physical-store-written-lsn store)))
               (ok (zerop (pg::physical-store-flushed-lsn store)))
               (pg::physical-store-input store 7 (subseq (sample-physical-record) 7))
               (ok (= 32 (pg::physical-store-written-lsn store)))
               (ok (zerop (pg::physical-store-flushed-lsn store)))
               (pg::physical-store-flush store)
               (ok (= 32 (pg::physical-store-flushed-lsn store))))
          (pg::physical-store-close store)))
      (let ((reopened (pg::physical-store-open root 123 1)))
        (unwind-protect (ok (= 32 (pg::physical-store-flushed-lsn reopened)))
          (pg::physical-store-close reopened)))))

(deftest physical-store-preserves-source-octets
    (with-physical-store-directory (root)
      (let ((bytes (sample-physical-record))
            (store (pg::physical-store-open root 123 1 :create t)))
        ;; Supported older records survive receipt verbatim.
        (pg::wal-set-u16 bytes 4 pg::+wal-version-v2+)
        (pg::wal-set-u32 bytes 28 0)
        (pg::wal-set-u32 bytes 28 (pg::wal-crc32 bytes))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 bytes)
               (pg::physical-store-flush store))
          (pg::physical-store-close store))
        (with-open-file (stream (merge-pathnames "0000000000000000.wal" root)
                                :element-type '(unsigned-byte 8))
          (let ((stored (make-array 32 :element-type '(unsigned-byte 8))))
            (ok (= 32 (read-sequence stored stream)))
            (ok (equalp bytes stored)))))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect (ok (= 32 (pg::physical-store-flushed-lsn store)))
          (pg::physical-store-close store)))))

(deftest physical-store-reopens-a-large-page-image-batch
    (with-physical-store-directory (root)
      (let* ((images (loop for block below 10
                           collect (pg::make-wal-page-image
                                    :relfilenode 100 :block block :fork :main :xids '(42)
                                    :page (pg::page-init (pg::make-page)))))
             (start (sample-physical-record))
             (page-start (length start))
             (page-end (+ page-start pg::+wal-header-size+ (pg::wal-page-payload-length images)))
             (pages (pg::wal-encode-record pg::+wal-record-pages+ 0 page-start
                                           (pg::wal-encode-page-images images page-end)))
             (terminal (pg::wal-encode-record pg::+wal-record-commit+ 42 page-end
                                              (pg::wal-encode-commit-timestamp 123)))
             (bytes (concatenate '(vector (unsigned-byte 8)) start pages terminal))
             (store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (loop for offset from 0 below (length bytes) by 333
                     do (pg::physical-store-input
                         store offset (subseq bytes offset (min (length bytes) (+ offset 333)))))
               (ok (= (length bytes) (pg::physical-store-written-lsn store)))
               (ok (zerop (pg::physical-store-flushed-lsn store)))
               (pg::physical-store-flush store))
          (pg::physical-store-close store))
        (let ((store (pg::physical-store-open root 123 1)))
          (unwind-protect
               (progn
                 (ok (= (length bytes) (pg::physical-store-flushed-lsn store)))
                 (ok (= 10 (length (pg::wal-decode-page-images
                                    (pg::wal-record-payload
                                     (pg::wal-decode-record pages page-start))
                                    page-end)))))
            (pg::physical-store-close store))))))

(deftest physical-store-discards-unacknowledged-tail
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (pg::physical-store-input store 0 (sample-physical-record))
        (pg::physical-store-flush store)
        (pg::physical-store-input store 32 (sample-physical-record 32))
        (pg::physical-store-close store))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (progn
               (ok (= 32 (pg::physical-store-written-lsn store)))
               (ok (= 32 (pg::physical-store-flushed-lsn store)))
               (pg::physical-store-input store 32 (sample-physical-record 32))
               (pg::physical-store-flush store)
               (ok (= 64 (pg::physical-store-flushed-lsn store))))
          (pg::physical-store-close store)))))

(deftest physical-store-rejects-foreign-history-and-owners
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1))))
          (pg::physical-store-close store)))
      (ok (intake-fails-p (lambda () (pg::physical-store-open root 124 1))))
      (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 2))))
      (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1 :create t))))))

(deftest physical-store-control-failure-latches-receiver
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (let ((pg::*physical-store-test-hook*
                       (lambda (point store)
                         (declare (ignore store))
                         (when (eq point :before-control-write) (error "injected disk failure")))))
                 (ok (intake-fails-p (lambda () (pg::physical-store-flush store)))))
               (ok (zerop (pg::physical-store-flushed-lsn store)))
               (ok (intake-fails-p (lambda () (pg::physical-store-flush store))))
               (ok (intake-fails-p
                    (lambda () (pg::physical-store-input store 32 (sample-physical-record 32))))))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect (ok (zerop (pg::physical-store-flushed-lsn store)))
          (pg::physical-store-close store)))))

(deftest physical-store-validates-before-tail-truncation
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (pg::physical-store-input store 0 (sample-physical-record))
        (pg::physical-store-flush store)
        (pg::physical-store-input store 32 (sample-physical-record 32))
        (pg::physical-store-close store))
      (let ((path (merge-pathnames "0000000000000000.wal" root)))
        (with-open-file (stream path :direction :io :if-exists :overwrite
                                     :element-type '(unsigned-byte 8))
          (file-position stream 28)
          (write-byte 0 stream)
          (pg::fsync-stream stream))
        (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1))))
        (with-open-file (stream path :element-type '(unsigned-byte 8))
          (ok (= 64 (file-length stream)))))))

(deftest physical-store-detects-omitted-acknowledged-record
    (with-physical-store-directory (root)
      (let* ((segment pg::+wal-segment-size+)
             (base (- segment 64))
             (store (pg::physical-store-open root 123 1 :create t :start-lsn base))
             (terminal (pg::wal-encode-record pg::+wal-record-commit+ 42 segment
                                              (pg::wal-encode-commit-timestamp 123))))
        (unwind-protect
             (progn
               (pg::physical-store-input store base (sample-physical-record base))
               (pg::physical-store-input store (- segment 32)
                                         (sample-physical-record (- segment 32)))
               (pg::physical-store-input store segment terminal)
               (pg::physical-store-flush store)
               (pg::physical-store-input store (+ segment 40)
                                         (sample-physical-record (+ segment 40))))
          (pg::physical-store-close store))
        ;; Losing the last complete record in the prior segment leaves a
        ;; plausible rotation gap for the next 40-byte record. Its local CRC
        ;; cannot establish that the preceding acknowledged record survived.
        (with-open-file (stream (merge-pathnames "0000000000000000.wal" root)
                                :direction :io :if-exists :overwrite
                                :element-type '(unsigned-byte 8))
          (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) (- segment 32))
          (pg::fsync-stream stream))
        (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1))))
        (with-open-file (stream (merge-pathnames "0000000000000001.wal" root)
                                :element-type '(unsigned-byte 8))
          (ok (= 72 (file-length stream)))))))

(deftest physical-store-rejects-missing-or-corrupt-manifest
    (dolist (damage '(:missing-manifest :corrupt-manifest :missing-segment))
      (with-physical-store-directory (root)
        (let ((store (pg::physical-store-open root 123 1 :create t)))
          (pg::physical-store-input store 0 (sample-physical-record))
          (pg::physical-store-flush store)
          (pg::physical-store-input store 32 (sample-physical-record 32))
          (pg::physical-store-close store))
        (let ((manifest (merge-pathnames "receive.control" root))
              (segment (merge-pathnames "0000000000000000.wal" root)))
          (ecase damage
            (:missing-manifest (delete-file manifest))
            (:corrupt-manifest
             (with-open-file (stream manifest :direction :io :if-exists :overwrite
                                              :element-type '(unsigned-byte 8))
               (write-byte 0 stream)
               (pg::fsync-stream stream)))
            (:missing-segment (delete-file segment)))
          (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1))))
          (unless (eq damage :missing-segment)
            (with-open-file (stream segment :element-type '(unsigned-byte 8))
              (ok (= 64 (file-length stream)))))))))

(deftest physical-store-control-publication-is-restart-authority
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (let ((pg::*physical-store-test-hook*
                       (lambda (point store)
                         (declare (ignore store))
                         (when (eq point :after-control-write)
                           (error "lost caller after durable rename")))))
                 (ok (intake-fails-p (lambda () (pg::physical-store-flush store)))))
               (ok (zerop (pg::physical-store-flushed-lsn store)))
               (ok (pg::physical-store-failed-p store)))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect (ok (= 32 (pg::physical-store-flushed-lsn store)))
          (pg::physical-store-close store)))))

(deftest physical-store-rotation-and-nonzero-base
    (with-physical-store-directory (root)
      (let* ((segment pg::+wal-segment-size+)
             (base (- segment 40))
             (store (pg::physical-store-open root 123 1 :create t :start-lsn base)))
        (unwind-protect
             (progn
               (pg::physical-store-input store base (sample-physical-record base))
               (pg::physical-store-input store segment (sample-physical-record segment))
               (pg::physical-store-flush store)
               (ok (= (+ segment 32) (pg::physical-store-flushed-lsn store))))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (ok (= (+ pg::+wal-segment-size+ 32) (pg::physical-store-flushed-lsn store)))
          (pg::physical-store-close store)))))

(deftest physical-store-corrupt-input-never-certifies-prefix
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t))
             (bad (sample-physical-record 32)))
        (unwind-protect
             (progn
               (setf (aref bad 31) (logxor 1 (aref bad 31)))
               (ok (intake-fails-p
                    (lambda () (pg::physical-store-input
                                store 0 (concatenate '(vector (unsigned-byte 8))
                                                     (sample-physical-record) bad)))))
               (ok (= 32 (pg::physical-store-written-lsn store)))
               (ok (zerop (pg::physical-store-flushed-lsn store)))
               (ok (intake-fails-p (lambda () (pg::physical-store-flush store)))))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect (ok (zerop (pg::physical-store-written-lsn store)))
          (pg::physical-store-close store)))))

(deftest physical-store-sigkill-durability-boundaries
    (dolist (point '(:after-segment-fsync :before-control-fsync :after-control-fsync
                     :before-control-rename :after-control-rename
                     :before-control-directory-fsync :after-control-directory-fsync
                     :after-control-write))
      (with-physical-store-directory (root)
        (pg::physical-store-close (pg::physical-store-open root 123 1 :create t))
        (multiple-value-bind (output errors status)
            (uiop:run-program
             (list (or (uiop:getenv "SBCL") "sbcl") "--noinform" "--no-sysinit"
                   "--no-userinit" "--script"
                   (namestring (asdf:system-relative-pathname
                                "pg" "t/physical-store-crash-child.lisp"))
                   (namestring root) (symbol-name point))
             :output :string :error-output :string :ignore-error-status t)
          (declare (ignore errors))
          (ok (not (zerop status)) (format nil "child unexpectedly succeeded: ~a" output))
          (ok (probe-file (merge-pathnames "crash.marker" root))
              (format nil "child never reached ~a: ~a" point output)))
        ;; SIGKILL releases the process lock; the new owner trusts only the
        ;; control image that survived, and validates its corresponding WAL.
        (let ((store (pg::physical-store-open root 123 1)))
          (unwind-protect
               (ok (= (if (member point '(:after-control-rename
                                          :before-control-directory-fsync
                                          :after-control-directory-fsync
                                          :after-control-write))
                          32 0)
                      (pg::physical-store-flushed-lsn store))
                   (format nil "wrong restart boundary at ~a" point))
            (pg::physical-store-close store))))))

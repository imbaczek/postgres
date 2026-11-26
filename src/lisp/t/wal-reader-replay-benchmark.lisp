(in-package #:pg-physical-tests)

;; Run in a dedicated SBCL process because the measurement wraps WAL decoding.
(defun measure-wal-reader-replay (count mode)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root)
           (empty (make-array 0 :element-type '(unsigned-byte 8)))
           (columns '((:attnum 1 :name "id" :type :int4 :typmod -1 :notnull t)))
           (position 0)
           (records
             (loop for xid from 1 to count append
                                           (let* ((marker (pg::wal-encode-record pg::+wal-record-batch-start+
                                                                                 0 position empty))
                                                  (event (pg::logical-encode-change
                                                          (pg::make-logical-change
                                                           :kind :insert :database-oid 7 :relation-oid 701
                                                           :relation-name "profile_rows" :columns columns
                                                           :key-attnums '(1) :new-values (vector xid))))
                                                  (chunk (pg::wal-encode-record pg::+wal-record-logical-changes+ xid
                                                                                (+ position (length marker))
                                                                                (pg::logical-encode-chunk 0 1 (list event))))
                                                  (commit (pg::wal-encode-record pg::+wal-record-commit+ xid
                                                                                 (+ position (length marker) (length chunk))
                                                                                 (pg::wal-encode-commit-timestamp xid))))
                                             (incf position (+ (length marker) (length chunk) (length commit)))
                                             (list marker chunk commit))))
           (history (pg::timeline-parse-history 1 ""))
           (decode (symbol-function 'pg::wal-decode-record))
           (decoded 0) (progress 0) (returned 0)
           (start nil) (allocated nil))
      (apply #'lineage-test-file 1 0 records)
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::wal-decode-record)
                   (lambda (&rest arguments) (incf decoded) (apply decode arguments))
                   start (get-internal-real-time)
                   allocated (sb-ext:get-bytes-consed))
             (dotimes (iteration count)
               (declare (ignorable iteration))
               (pg::call-with-logical-reader-spools
                (lambda ()
                  (let* ((anchor (if (eq mode :replay) 0 progress))
                         (scan (pg::make-wal-retained-slot-scan
                                :start anchor :end position :database 7)))
                    (pg::call-with-logical-wal-cursor
                     history :timeline anchor position 1 :prefix-end (lambda () t)
                     (lambda (cursor)
                       (multiple-value-bind (transactions next exhausted)
                           (pg::wal-logical-slot-read-transactions-from-scan
                            scan 1 1048576 1048576 :cursor cursor :output-start progress)
                         (declare (ignore exhausted))
                         (assert (= 1 (length transactions)))
                         (incf returned)
                         (assert (= returned (pg::logical-transaction-xid (first transactions))))
                         (setf progress next)))
                     :batch-policy :logical-reader)))))
             (assert (= returned count))
             (assert (= progress position))
             (format t "~&WAL-READER count=~d mode=~(~a~) decoded=~d rows=~d seconds=~,6f ~
                         allocated-bytes=~d~%"
                     count mode decoded returned
                     (/ (- (get-internal-real-time) start) (float internal-time-units-per-second 1d0))
                     (- (sb-ext:get-bytes-consed) allocated)))
        (setf (symbol-function 'pg::wal-decode-record) decode)))))

(dolist (count '(32 64 128))
  (measure-wal-reader-replay count :incremental)
  (measure-wal-reader-replay count :replay))

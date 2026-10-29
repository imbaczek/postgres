(in-package #:pg)

(defvar *row-replay-test-captured* nil)

(defvar *row-replay-test-terminals* 0)

(defun row-replay-test-install ()
  (setf *row-replay-test-terminals* 0
        *row-replay-test-captured*
        (list *catalog* *clog-stream*
              (sb-thread:with-mutex (*snapshot-count-lock*)
                (loop for snapshot being the hash-keys of *active-snapshot-set* collect snapshot))
              (loop for backend in (list-backends)
                    when (backend-virtual-reader-xid backend)
                      collect (cons backend (backend-virtual-reader-xid backend)))))
  (assert (= 2 (length (third *row-replay-test-captured*))))
  (assert (= 2 (length (fourth *row-replay-test-captured*))))
  (setf *physical-replay-test-hook*
        (lambda (point record)
          (when (and (eq point :after-redo) (= (wal-record-type record) +wal-record-commit+))
            (assert (= 1 (clog-get (wal-record-xid record))))
            (assert (member (wal-record-xid record)
                            (getf (getf *physical-recovery-state* :source-xids) :running))))
          (when (eq point :after-progress)
            (let ((source (getf *physical-recovery-state* :source-xids)))
              (running-xacts-check-state source)
              (assert (getf source :complete))
              (assert (= *next-xid* (getf source :next-xid)))
              (assert (= *last-replay-lsn* (getf source :through)))
              (when (= (wal-record-type record) +wal-record-commit+)
                (assert (not (member (wal-record-xid record) (getf source :running))))))
            (assert (sb-thread:holding-mutex-p *wal-lock*))
            (assert (eq *catalog* (first *row-replay-test-captured*)))
            (assert (eq *clog-stream* (second *row-replay-test-captured*)))
            (sb-thread:with-mutex (*snapshot-count-lock*)
              (assert (= 2 *active-snapshots*))
              (dolist (snapshot (third *row-replay-test-captured*))
                (assert (gethash snapshot *active-snapshot-set*))))
            (dolist (pair (fourth *row-replay-test-captured*))
              (assert (eql (cdr pair) (backend-virtual-reader-xid (car pair)))))
            (assert (= *last-replay-lsn* (wal-record-end record)))
            (assert (= *last-replay-lsn* (getf *physical-recovery-state* :required-applied)))
            (assert (zerop (hash-table-count *wal-page-cache*)))
            (incf *row-replay-test-terminals*))))
  t)

(defun row-replay-test-verify ()
  (assert (>= *row-replay-test-terminals* 2))
  (setf *physical-replay-test-hook* nil)
  (format t "row-replay-test: runtime and reader registrations preserved PASS~%")
  *row-replay-test-terminals*)

(defun row-replay-test-arm-crash (point marker)
  (setf *physical-replay-test-hook*
        (lambda (boundary record)
          (when (and (eq point boundary) (= (wal-record-type record) +wal-record-commit+))
            (with-open-file (out marker :direction :output :if-exists :error)
              (format out "~d~%" (sb-posix:getpid)))
            (loop repeat 600 do (sleep 0.05))
            (error "row replay crash barrier was not killed"))))
  t)

(defun row-replay-test-staged-dead-page-p (node block)
  (sb-thread:with-mutex (*wal-lock*)
    (let* ((key (wal-page-key node block))
           (reference (gethash key *wal-page-cache*))
           (page (and reference (wal-dirty-read-reference key reference))))
      (and page
           (loop for offnum from 1 to (page-lp-count page)
                   thereis (= +lp-dead+ (nth-value 1 (page-get-lp page offnum))))))))

(defun row-replay-test-pause-vacuum (marker release)
  (let ((original (symbol-function 'heap-vacuum-relation-locked)))
    (setf (symbol-function 'heap-vacuum-relation-locked)
          (lambda (node indexes oldest freeze &key after-page-function)
            (setf (symbol-function 'heap-vacuum-relation-locked) original)
            (let ((paused nil))
              (funcall original node indexes oldest freeze
                       :after-page-function
                       (lambda (&rest arguments)
                         (when after-page-function (apply after-page-function arguments))
                         (when (and (not paused)
                                    (apply #'row-replay-test-staged-dead-page-p arguments))
                           (setf paused t)
                           (with-open-file (out marker :direction :output :if-exists :error)
                             (format out "~d~%" (sb-posix:getpid)))
                           (loop repeat 600
                                 until (probe-file release) do (sleep 0.05))
                           (unless (probe-file release) (error "vacuum barrier timed out")))))))))
  t)

(defun row-replay-test-legacy-next-page-batch ()
  (let ((append-original (symbol-function 'wal-primary-append-pages))
        (image-original (symbol-function 'wal-primary-page-image)))
    (setf (symbol-function 'wal-primary-append-pages)
          (lambda (keys spool)
            (setf (symbol-function 'wal-primary-append-pages) append-original)
            (unwind-protect
                 (progn
                   (setf (symbol-function 'wal-primary-page-image)
                         (lambda (key)
                           (let ((image (funcall image-original key)))
                             (setf (wal-page-image-provenance image) :unknown)
                             image)))
                   (funcall append-original keys spool))
              (setf (symbol-function 'wal-primary-page-image) image-original)))))
  t)

(defun row-replay-test-incomplete-next-running-snapshot ()
  (let ((original (symbol-function 'wal-encode-running-xacts)))
    (setf (symbol-function 'wal-encode-running-xacts)
          (lambda (next running complete)
            (declare (ignore running complete))
            (setf (symbol-function 'wal-encode-running-xacts) original)
            (funcall original next nil nil))))
  t)

(defun row-replay-test-flush-running-snapshot ()
  (with-wal-critical
    (sb-thread:with-mutex (*xid-lock*)
      (wal-append-batch-start)
      (wal-append-record +wal-record-running-xacts+ 0
                         (wal-encode-running-xacts
                          *next-xid* (sort (copy-list *active-xids*) #'<) t))
      (wal-flush)))
  t)

(defun row-replay-test-coverage (covered incomplete &optional absent)
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (let ((flags (loop for snapshot being the hash-keys of *active-snapshot-set*
                       collect (snapshot-source-xids-complete-p snapshot))))
      (assert (= covered (count t flags)))
      (assert (= incomplete (count nil flags))))
    (when absent
      (maphash (lambda (snapshot present)
                 (when present
                   (assert (< absent (snapshot-xmax snapshot)))
                   (assert (not (member absent (snapshot-xip snapshot))))))
               *active-snapshot-set*)))
  (format t "snapshot coverage PASS~%"))

(defun row-replay-test-only-active-xid ()
  (sb-thread:with-mutex (*xid-lock*)
    (assert (= 1 (length *active-xids*)))
    (format t "~&ACTIVE-XID=~d~%" (first *active-xids*))))

(defun row-replay-test-has-contributor (xid)
  (sb-thread:with-mutex (*wal-lock*)
    (assert (some (lambda (image) (member xid (wal-page-image-xids image)))
                  (wal-page-images))))
  t)

(defun row-replay-test-index-split ()
  (let* ((database (gethash "postgres" (catalog-databases *catalog*)))
         (table (gethash "promotion_sql_items" (database-tables database)))
         (index (find (pg-class-oid table)
                      (loop for value being the hash-values of (database-indexes database)
                            collect value)
                      :key #'pg-index-table-oid)))
    (assert index)
    ;; An unsplit tree already has a metapage and one leaf.
    (assert (> (smgr-nblocks (pg-index-relfilenode index)) 2)))
  t)

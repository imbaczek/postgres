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
          (when (eq point :after-progress)
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
                         (unless paused
                           (setf paused t)
                           (with-open-file (out marker :direction :output :if-exists :error)
                             (format out "~d~%" (sb-posix:getpid)))
                           (loop repeat 600
                                 until (probe-file release) do (sleep 0.05))
                           (unless (probe-file release) (error "vacuum barrier timed out")))))))))
  t)

(defun row-replay-test-legacy-next-page-batch ()
  (let ((original (symbol-function 'wal-page-images)))
    (setf (symbol-function 'wal-page-images)
          (lambda (&optional excluded)
            (setf (symbol-function 'wal-page-images) original)
            (let ((images (funcall original excluded)))
              (dolist (image images) (setf (wal-page-image-provenance image) :unknown))
              images))))
  t)

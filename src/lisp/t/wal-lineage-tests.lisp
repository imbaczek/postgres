;;;; Native timeline-file selection and read-only fork validation.
(in-package #:pg-physical-tests)

(defun lineage-test-bytes (path)
  (with-open-file (stream path :element-type '(unsigned-byte 8))
    (let ((bytes (make-array (file-length stream) :element-type '(unsigned-byte 8))))
      (read-sequence bytes stream)
      bytes)))

(defun lineage-test-record (start &optional (length 33) (xid 1))
  (pg::wal-encode-record pg::+wal-record-maintenance+ xid start
                         (make-array (- length pg::+wal-header-size+)
                                     :element-type '(unsigned-byte 8) :initial-element 42)))

(defun lineage-test-file (timeline segment &rest records)
  (let ((path (pg::wal-segment-path segment timeline :timeline)))
    (pg::wal-atomic-write-bytes path (apply #'concatenate '(vector (unsigned-byte 8)) records))
    path))

(defun lineage-test-history (fork &optional (child 2))
  (list (pg::make-timeline-history-entry :timeline child :begin fork)
        (pg::make-timeline-history-entry :timeline 1 :begin 0 :end fork)))

(defun lineage-test-read (history start end &rest options)
  (let ((records nil))
    (multiple-value-bind (cursor files complete)
        (apply #'pg::wal-scan-lineage
               (lambda (record owner file)
                 (push (list (pg::wal-record-start record) (pg::wal-record-xid record) owner file) records))
               history start end options)
      (values (nreverse records) cursor files complete))))

(cl-spec:defproperty lineage-reader-preserves-generated-branch-ownership
    ((parent-length (range integer 32 256)) (child-length (range integer 32 256))
                                            (timeline (range integer 2 4294967295)))
  (:about pg::wal-scan-lineage)
  (:trials (:normal 100))
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root))
      (lineage-test-file timeline 0 (lineage-test-record 0 parent-length)
                         (lineage-test-record parent-length child-length 2))
      (multiple-value-bind (records cursor files complete)
          (lineage-test-read (lineage-test-history parent-length timeline)
                             0 (+ parent-length child-length))
        (and complete (= cursor (+ parent-length child-length))
             (equal files (list (list timeline 0)))
             (equal records (list (list 0 1 1 timeline)
                                  (list parent-length 2 timeline timeline))))))))

(deftest generated-lineage-reader-branch-ownership
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'lineage-reader-preserves-generated-branch-ownership :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest lineage-reader-uses-child-copied-prefix-without-parent-file
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (history (lineage-test-history 33)))
        (lineage-test-file 2 0 (lineage-test-record 0) (lineage-test-record 33 33 2))
        ;; A higher branch outside the chosen history must never be considered.
        (lineage-test-file 99 0 (make-array 1 :element-type '(unsigned-byte 8)))
        (multiple-value-bind (records cursor files complete) (lineage-test-read history 0 66)
          (ok (equal records '((0 1 1 2) (33 2 2 2))))
          (ok (= cursor 66)) (ok (equal files '((2 0)))) (ok complete))
        (multiple-value-bind (records cursor files complete) (lineage-test-read history 33 66)
          (declare (ignore cursor files complete))
          (ok (equal records '((33 2 2 2)))))
        (multiple-value-bind (cursor files complete)
            (pg::wal-scan-lineage (lambda (&rest args) (declare (ignore args)) :stop) history 0 66)
          (declare (ignore files))
          (ok (= cursor 33)) (ok (null complete))))))

(deftest lineage-reader-rejects-parent-tail-and-corrupt-child-without-modifying-files
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (history (lineage-test-history 33))
                                 (parent (lineage-test-file 1 0 (lineage-test-record 0) (lineage-test-record 33 33 8)))
                                 (parent-bytes (lineage-test-bytes parent)))
        (ok (intake-fails-p (lambda () (lineage-test-read history 0 66))))
        (ok (equalp parent-bytes (lineage-test-bytes parent)))
        (let* ((bad (lineage-test-record 0))
               (child (progn (setf (aref bad 28) (logxor 1 (aref bad 28)))
                             (lineage-test-file 2 0 bad (lineage-test-record 33 33 2))))
               (child-bytes (lineage-test-bytes child)))
          (ok (intake-fails-p (lambda () (lineage-test-read history 0 66))))
          (ok (equalp child-bytes (lineage-test-bytes child)))
          (ok (equalp parent-bytes (lineage-test-bytes parent)))))))

(deftest lineage-reader-rejects-split-records-and-inexact-bounds
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (lineage-test-file 2 0 (lineage-test-record 0) (lineage-test-record 33))
        (dolist (bounds '((0 32) (1 33) (0 67) (34 66) (-1 33) (66 33)))
          (ok (intake-fails-p (lambda () (lineage-test-read (lineage-test-history 33)
                                                            (first bounds) (second bounds))))))
        (ok (intake-fails-p (lambda () (lineage-test-read (lineage-test-history 32) 0 66))))
        (multiple-value-bind (records cursor files complete)
            (lineage-test-read (lineage-test-history 33) 66 66)
          (ok (null records)) (ok (= cursor 66)) (ok (null files)) (ok complete)))))

(deftest lineage-reader-validates-child-rotation-after-divergent-parent-tail
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (size pg::+wal-segment-size+) (fork (- size 64))
                                 (history (lineage-test-history fork))
                                 (parent (lineage-test-file 1 0 (lineage-test-record 0 fork)
                                                            (lineage-test-record fork 64 8)))
                                 (before (lineage-test-bytes parent)))
        (lineage-test-file 2 1 (lineage-test-record size 96 2))
        (multiple-value-bind (records cursor files complete) (lineage-test-read history 0 (+ size 96))
          (ok (equal records (list (list 0 1 1 1) (list size 2 2 2))))
          (ok (= cursor (+ size 96)))
          (ok (equal files '((1 0) (2 1))))
          (ok complete))
        (ok (equalp before (lineage-test-bytes parent)))
        (ok (= (nth-value 1 (lineage-test-read history fork (+ size 96))) (+ size 96)))
        (lineage-test-file 2 1 (lineage-test-record size 32 2))
        (ok (intake-fails-p (lambda () (lineage-test-read history 0 (+ size 32)))))
        (lineage-test-file 2 1 (lineage-test-record size 96 2))
        (ok (intake-fails-p (lambda () (lineage-test-read history 0 size)))))))

(deftest lineage-reader-never-falls-back-from-special-or-symlink-child
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (history (lineage-test-history 33))
                                 (parent (lineage-test-file 1 0 (lineage-test-record 0) (lineage-test-record 33)))
                                 (child (pg::wal-segment-path 0 2 :timeline)))
        (sb-posix:symlink (namestring parent) (namestring child))
        (ok (intake-fails-p (lambda () (lineage-test-read history 0 66))))
        (sb-posix:unlink (namestring child))
        (sb-posix:mkfifo (namestring child) #o600)
        (ok (intake-fails-p (lambda () (lineage-test-read history 0 66))))
        (sb-posix:unlink (namestring child)))))

(deftest lineage-reader-handles-segment-boundary-and-equal-forks
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (size pg::+wal-segment-size+)
                                 (pg::*wal-durable-lsn* 0)
                                 (history (list (pg::make-timeline-history-entry :timeline 3 :begin size)
                                                (pg::make-timeline-history-entry :timeline 2 :begin size :end size)
                                                (pg::make-timeline-history-entry :timeline 1 :begin 0 :end size))))
        (lineage-test-file 1 0 (lineage-test-record 0 size))
        (lineage-test-file 3 1 (lineage-test-record size 32 3))
        (multiple-value-bind (records cursor files complete) (lineage-test-read history 0 (+ size 32))
          (ok (equal records (list (list 0 1 1 1) (list size 3 3 3))))
          (ok (= cursor (+ size 32))) (ok (equal files '((1 0) (3 1)))) (ok complete))
        ;; No child-named previous segment is needed for an exact-boundary fork.
        (ok (not (probe-file (pg::wal-segment-path 0 3 :timeline)))))))

(deftest lineage-reader-rejects-backward-file-selection-and-forks-inside-gaps
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (size pg::+wal-segment-size+) (last-end (- size 64)))
        (lineage-test-file 2 0 (lineage-test-record 0 last-end))
        (lineage-test-file 1 1 (lineage-test-record size 96))
        (ok (intake-fails-p (lambda () (lineage-test-read (lineage-test-history 0) 0 (+ size 96)))))
        (delete-file (pg::wal-segment-path 0 2 :timeline))
        (lineage-test-file 1 0 (lineage-test-record 0 last-end))
        (lineage-test-file 2 1 (lineage-test-record size 96))
        ;; A switchpoint in skipped capacity is not a selected-branch record end.
        (ok (intake-fails-p
             (lambda () (lineage-test-read (lineage-test-history (1+ last-end)) 0 (+ size 96))))))))

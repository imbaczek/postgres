;;;; Physical intake must preserve native WAL invariants across network fragments.
(defpackage #:pg-physical-tests (:use #:cl #:rove))
(in-package #:pg-physical-tests)

(defvar *physical-test-plain-cache*
  ;; Use only a cache from a completed successful plain strict production
  ;; compile of the identical pinned source and runtime.
  nil)

(defun physical-test-child-command (command)
  ;; Pass environment entries as literal arguments and retain a fresh child.
  (unless (and (consp command) (list-length command)
               (every (lambda (argument)
                        (and (stringp argument) (not (find (code-char 0) argument)))) command)
               (plusp (length (first command))))
    (error "physical test children require an argument list, not a shell command"))
  (if *physical-test-plain-cache*
      (let ((cache (uiop:ensure-directory-pathname *physical-test-plain-cache*)))
        (unless (uiop:absolute-pathname-p cache) (error "physical child cache must be absolute"))
        (let ((*print-length* nil) (*print-level* nil) (*print-readably* t)
                                   (*package* (find-package :cl-user)))
          (list* "env"
                 (concatenate 'string "ASDF_OUTPUT_TRANSLATIONS="
                              (write-to-string
                               (list :output-translations (list t (namestring cache))
                                     :ignore-inherited-configuration)))
                 "PG_LISP_TEST_CHILD_CACHE_EXPECTED=1"
                 command)))
      command))

(defun empty-octets ()
  (make-array 0 :element-type '(unsigned-byte 8)))

(defun physical-test-checkpoint-record (start &key (timeline 1))
  "Return a native checkpoint record and its snapshot with no optional source facts."
  (let ((snapshot (list :version 2 :start start :end (+ start (pg::wal-checkpoint-record-size))
                        :redo start :next-xid 3 :xid-reserved-through 2 :next-oid nil
                        :time 0 :timeline timeline :prev-timeline timeline :oldest-active-xid 0)))
    (values (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                   (pg::wal-encode-checkpoint-snapshot snapshot))
            snapshot)))

(defun legacy-checkpoint-record (start)
  "Explicit version-5 fixture; new writers require the version-6 payload."
  (let ((bytes (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start (empty-octets))))
    (pg::wal-set-u16 bytes 4 5)
    (pg::wal-set-u32 bytes 28 0)
    (pg::wal-set-u32 bytes 28 (pg::wal-crc32 bytes))
    bytes))

(defun intake-fails-p (function)
  (handler-case (progn (funcall function) nil) (error () t)))

(defun commit-component-startup-installation (candidate)
  "Exercise publication and predecessor checks without requiring a running postmaster."
  (pg::startup-call-with-prepared-installation candidate nil nil nil))

(defun install-component-startup-settings (settings native overrides &optional (policy nil supplied-p))
  (commit-component-startup-installation
   (if supplied-p
       (pg::startup-prepare-installation settings native overrides policy)
       (pg::startup-prepare-installation settings native overrides)))
  settings)

(defun reload-component-startup-settings ()
  (multiple-value-bind (candidate settings) (pg::startup-prepare-reload-installation)
    (when candidate
      (commit-component-startup-installation candidate)
      settings)))

(deftest fragmented-wal-records
    (let* ((first (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 (empty-octets)))
           (last (pg::wal-encode-record pg::+wal-record-commit+ 42 (length first)
                                        (pg::wal-encode-commit-timestamp 123456)))
           (bytes (concatenate '(vector (unsigned-byte 8)) first last)))
      ;; Every possible split, including a one-byte header or a split CRC/body.
      (loop for split from 0 to (length bytes)
            for decoder = (pg::make-physical-wal-decoder)
            for records = (append
                           (pg::physical-wal-decode-input decoder 0 (subseq bytes 0 split))
                           (pg::physical-wal-decode-input decoder split (subseq bytes split)))
            do (ok (equal '(6 2) (mapcar #'pg::wal-record-type records)))
               (ok (equal '(0 32) (mapcar #'pg::wal-record-start records)))
               (ok (equal '(32 72) (mapcar #'pg::wal-record-end records))))
      (let ((decoder (pg::make-physical-wal-decoder)) (records nil)
                                                      (pg::*wal-durable-lsn* 11))
        (loop for index below (length bytes)
              do (setf records
                       (append records (pg::physical-wal-decode-input
                                        decoder index (subseq bytes index (1+ index))))))
        (ok (= 2 (length records)))
        (ok (= 11 pg::*wal-durable-lsn*))
        (ok (= 72 (pg::physical-wal-decoder-next-lsn decoder))))))

(deftest incomplete-and-corrupt-wal
    (let* ((bytes (pg::wal-encode-record pg::+wal-record-commit+ 42 0
                                         (pg::wal-encode-commit-timestamp 123456)))
           (decoder (pg::make-physical-wal-decoder)))
      (ok (null (pg::physical-wal-decode-input decoder 0 (subseq bytes 0 39))))
      (setf (aref bytes 39) (logxor 1 (aref bytes 39)))
      (ok (intake-fails-p
           (lambda () (pg::physical-wal-decode-input decoder 39 (subseq bytes 39)))))
      (ok (pg::physical-wal-decoder-failed-p decoder))
      (ok (intake-fails-p
           (lambda () (pg::physical-wal-decode-input decoder 40 (empty-octets)))))
      ;; A new connection discards the partial buffer and resumes at the owner's
      ;; actual durable boundary, not the previous decoder's intake cursor.
      (setf (aref bytes 39) (logxor 1 (aref bytes 39)))
      (ok (= 1 (length (pg::physical-wal-decode-input
                        (pg::make-physical-wal-decoder) 0 bytes))))))

(deftest physical-input-position-and-header-validation
    (let ((bytes (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 (empty-octets))))
      (dolist (position '(1 32 16777216))
        (ok (intake-fails-p
             (lambda () (pg::physical-wal-decode-input
                         (pg::make-physical-wal-decoder) position bytes)))))
      (dolist (offset '(0 4 6 12 28))
        (let ((bad (copy-seq bytes)))
          (setf (aref bad offset) (logxor (aref bad offset) 255))
          (ok (intake-fails-p
               (lambda () (pg::physical-wal-decode-input
                           (pg::make-physical-wal-decoder) 0 bad))))))
      (dolist (length '(0 31 16777217 4294967295))
        (let ((bad (copy-seq bytes)))
          (pg::wal-set-u32 bad 8 length)
          (ok (intake-fails-p
               (lambda () (pg::physical-wal-decode-input
                           (pg::make-physical-wal-decoder) 0 bad))))))
      (let ((decoder (pg::make-physical-wal-decoder)))
        (pg::physical-wal-decode-input decoder 0 (subseq bytes 0 1))
        (ok (intake-fails-p
             (lambda () (pg::physical-wal-decode-input decoder 0 bytes)))))))

(deftest physical-segment-rotation
    (let* ((end (- pg::+wal-segment-size+ 8))
           (next pg::+wal-segment-size+)
           (bytes (pg::wal-encode-record pg::+wal-record-batch-start+ 0 next (empty-octets)))
           (decoder (pg::make-physical-wal-decoder :start-lsn end)))
      (ok (null (pg::physical-wal-decode-input decoder next (subseq bytes 0 1))))
      (ok (= next (pg::wal-record-start
                   (first (pg::physical-wal-decode-input
                           decoder (1+ next) (subseq bytes 1))))))
      ;; Rotation is invalid when the next record still fits the prior segment.
      (ok (intake-fails-p
           (lambda () (pg::physical-wal-decode-input
                       (pg::make-physical-wal-decoder :start-lsn (- next 32))
                       next bytes))))
      (ok (intake-fails-p
           (lambda () (pg::physical-wal-decode-input
                       (pg::make-physical-wal-decoder :start-lsn end)
                       (* 2 next) bytes))))))

(deftest physical-boundary-and-empty-input
    (let* ((segment pg::+wal-segment-size+)
           (first-start (- segment 40))
           (first (pg::wal-encode-record pg::+wal-record-batch-start+ 0 first-start
                                         (empty-octets)))
           (next (pg::wal-encode-record pg::+wal-record-batch-start+ 0 segment
                                        (empty-octets)))
           (decoder (pg::make-physical-wal-decoder :start-lsn first-start)))
      (ok (= 1 (length (pg::physical-wal-decode-input decoder first-start first))))
      (ok (null (pg::physical-wal-decode-input decoder (- segment 8) (empty-octets))))
      (ok (= 1 (length (pg::physical-wal-decode-input decoder segment next))))
      (let* ((start (- segment 32))
             (exact (pg::wal-encode-record pg::+wal-record-batch-start+ 0 start
                                           (empty-octets)))
             (decoder (pg::make-physical-wal-decoder :start-lsn start)))
        ;; An exact boundary has no gap and may share one transport chunk.
        (ok (= 2 (length (pg::physical-wal-decode-input
                          decoder start
                          (concatenate '(vector (unsigned-byte 8)) exact next))))))
      (ok (intake-fails-p
           (lambda () (pg::physical-wal-decode-input
                       (pg::make-physical-wal-decoder :start-lsn (- segment 8))
                       segment (empty-octets)))))
      (let ((decoder (pg::make-physical-wal-decoder :start-lsn first-start)))
        (pg::physical-wal-decode-input decoder first-start (subseq first 0 1))
        (ok (intake-fails-p
             (lambda () (pg::physical-wal-decode-input decoder segment next)))))))

(deftest physical-input-cannot-overflow-lsn
    (let* ((start #xffffffffffffffff)
           (decoder (pg::make-physical-wal-decoder :start-lsn start))
           (byte (make-array 1 :element-type '(unsigned-byte 8) :initial-element 80)))
      (ok (intake-fails-p
           (lambda () (pg::physical-wal-decode-input decoder start byte))))
      (ok (= start (pg::physical-wal-decoder-next-lsn decoder)))
      (ok (pg::physical-wal-decoder-failed-p decoder))))

(cl-spec:defproperty physical-fragmentation-preserves-records
    ((timestamp (range integer -9223372036854775808 9223372036854775807))
     (width (range integer 1 80)))
  (:about pg::physical-wal-decode-input)
  (:trials (:normal 200))
  (let* ((bytes (pg::wal-encode-record pg::+wal-record-commit+ 42 0
                                       (pg::wal-encode-commit-timestamp timestamp)))
         (decoder (pg::make-physical-wal-decoder))
         (pg::*wal-durable-lsn* 0)
         (records
           (loop for offset from 0 below (length bytes) by width
                 append (pg::physical-wal-decode-input
                         decoder offset
                         (subseq bytes offset (min (length bytes) (+ offset width)))))))
    (and (= 1 (length records))
         (= timestamp (pg::wal-decode-commit-timestamp
                       (pg::wal-record-payload (first records))))
         (= 40 (pg::wal-record-end (first records)))
         (zerop pg::*wal-durable-lsn*))))

(deftest generated-physical-fragmentation
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-fragmentation-preserves-records :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

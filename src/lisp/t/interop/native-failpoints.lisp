(in-package #:pg)

(defun install-native-copy-test-hook (directory)
  "Pause two generations of each native COPY independently."
  (let ((counts (make-hash-table :test #'equal))
        (lock (sb-thread:make-mutex :name "native COPY test generations")))
    (setf *logical-test-failpoint-hook*
          (lambda (point &rest details)
            (when (eq point :standard-stream-start)
              (sb-thread:with-mutex (lock)
                (with-open-file (output (format nil "~a/native-protocol.txt" directory)
                                        :direction :output :if-exists :append :if-does-not-exist :create)
                  (format output "~a|~d|~{~a~^,~}~%" (first details) (second details) (third details))
                  (finish-output output)
                  (fsync-stream output))))
            (when (and (eq point :after-copy-out-response)
                       (member (first details) '("interop_rows" "interop_other") :test #'equal))
              (let* ((table (first details))
                     (generation (sb-thread:with-mutex (lock)
                                   (incf (gethash table counts 0))))
                     (base (format nil "~a/native-~a-~d" directory table generation)))
                (when (<= generation 2)
                  (logical-test-failpoint-mark-once (concatenate 'string base ".paused") point)
                  (loop until (probe-file (concatenate 'string base ".release"))
                        do (sleep 0.05))))))))
  :native-copy-hook-installed)

(defun install-native-feedback-test-hook (directory after)
  "Pause after native flush feedback arrives, before publisher slot persistence."
  (let ((fired nil)
        (lock (sb-thread:make-mutex :name "native feedback test")))
    (setf *logical-test-failpoint-hook*
          (lambda (point &rest details)
            (when (and (eq point :before-slot-acknowledgment)
                       (equal "interop_sub" (first details))
                       (> (second details) after)
                       (sb-thread:with-mutex (lock)
                         (unless fired (setf fired t))))
              (with-open-file (output (format nil "~a/native-feedback-position.txt" directory)
                                      :direction :output :if-exists :error :if-does-not-exist :create)
                (write-line (wal-format-lsn (second details)) output)
                (finish-output output)
                (fsync-stream output))
              (logical-test-failpoint-mark-once
               (format nil "~a/native-feedback.paused" directory) point)
              (loop until (probe-file (format nil "~a/native-feedback.release" directory))
                    do (sleep 0.05))))))
  :native-feedback-hook-installed)

(defun install-native-publication-race-hook (directory)
  "Pause outside the mutation lock, after immutable publication capture."
  (let ((fired nil) (lock (sb-thread:make-mutex :name "publication capture race")))
    (setf *logical-test-failpoint-hook*
          (lambda (point &rest details)
            (when (and (eq point :before-logical-row-encode)
                       (eq (first details) :insert) (equal (second details) "interop_rows")
                       (sb-thread:with-mutex (lock) (unless fired (setf fired t))))
              (logical-test-failpoint-mark-once
               (format nil "~a/native-publication.paused" directory) point)
              (loop until (probe-file (format nil "~a/native-publication.release" directory))
                    do (sleep 0.05))))))
  :native-publication-hook-installed)

(defun install-native-transaction-end-hook (directory ids)
  "Record exact published commit ends independently of later checkpoint WAL."
  (setf *logical-test-failpoint-hook*
        (lambda (point &rest details)
          (when (and (eq point :before-standard-commit) (equal "interop_sub" (first details)))
            (logical-transaction-map-changes
             (third details)
             (lambda (change)
               (when (and (eq :insert (logical-change-kind change))
                          (equal "interop_rows" (logical-change-relation-name change))
                          (member (aref (logical-change-new-values change) 0) ids))
                 (let* ((id (aref (logical-change-new-values change) 0))
                        (base (format nil "~a/native-transaction-~d" directory id)))
                   (unless (probe-file (concatenate 'string base ".ready"))
                     (with-open-file (output (concatenate 'string base ".end")
                                             :direction :output :if-exists :supersede
                                             :if-does-not-exist :create)
                       (write-line (wal-format-lsn (second details)) output)
                       (finish-output output)
                       (fsync-stream output))
                     (logical-test-failpoint-mark-once
                      (concatenate 'string base ".ready") point)))))))))
  :native-transaction-hook-installed)

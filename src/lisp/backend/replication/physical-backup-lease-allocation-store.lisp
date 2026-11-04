(in-package #:pg)

(defvar *physical-backup-lease-allocation-limit* 65536)
(defvar *physical-backup-lease-allocation-test-hook* nil)

(defun physical-backup-lease-allocation-directory ()
  (merge-pathnames "pg_physical_backup_allocations/" *pgdata*))

(defun physical-backup-lease-allocation-path (record &optional temporary)
  (let* ((checked (physical-backup-lease-allocation-copy record))
         (name (getf (getf checked :request) :name)))
    (merge-pathnames
     (format nil "~a.~2,'0d.~a" name (getf checked :generation) (if temporary "tmp" "alloc"))
     (physical-backup-lease-allocation-directory))))

(defun physical-backup-lease-allocation-read (path &key stabilize)
  (let ((directory (physical-backup-lease-allocation-directory))
        (maximum +physical-backup-lease-allocation-maximum-bytes+))
    (unless (equal (pathname path) (merge-pathnames (file-namestring path) directory))
      (error "backup lease allocation path is outside its inventory"))
    (physical-backup-lease-check-directory :directory directory)
    (multiple-value-bind (stream size) (open-bounded-regular-file path maximum stabilize)
      (unwind-protect
           (flet ((checked-identity (stat) (physical-backup-lease-file-identity stat nil maximum)))
             (let* ((before (checked-identity (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                    (bytes (make-array size :element-type '(unsigned-byte 8))))
               (unless (= size (sixth before))
                 (error "backup lease allocation changed before reading"))
               (when stabilize
                 (fsync-stream stream)
                 (fsync-directory directory))
               (unless (and (= size (read-sequence bytes stream))
                            (equal before (checked-identity (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                            (equal before (checked-identity (sb-posix:lstat (namestring path)))))
                 (error "backup lease allocation changed while reading"))
               (let ((record (handler-case (physical-backup-lease-allocation-decode bytes)
                               (wal-input-validation-error ()
                                 (error "invalid durable backup lease allocation coordinates")))))
                 (unless (equal (pathname path) (physical-backup-lease-allocation-path record))
                   (error "backup lease allocation pathname differs from its ownership"))
                 record)))
        (close stream)))))

(defun physical-backup-lease-allocation-write (record)
  (physical-backup-lease-require-locks)
  (let* ((checked (physical-backup-lease-allocation-copy record))
         (bytes (physical-backup-lease-allocation-image checked))
         (directory (physical-backup-lease-check-directory
                     :create t :directory (physical-backup-lease-allocation-directory)))
         (path (physical-backup-lease-allocation-path checked)))
    (physical-backup-lease-write-bytes
     path (physical-backup-lease-allocation-path checked t) bytes
     :directory directory :maximum +physical-backup-lease-allocation-maximum-bytes+
     :check-readback
     (lambda ()
       (unless (equal checked (physical-backup-lease-allocation-read path :stabilize t))
         (error "backup lease allocation readback differs from publication")))
     :test-point
     (lambda (point)
       (when *physical-backup-lease-allocation-test-hook*
         (funcall *physical-backup-lease-allocation-test-hook* point checked))))
    checked))

(defun physical-backup-lease-allocation-entry-p (entry)
  (let* ((stem (pathname-name entry))
         (type (pathname-type entry)))
    (and (stringp stem) (= 42 (length stem)) (char= #\. (char stem 39))
         (physical-backup-lease-name-p (subseq stem 0 39))
         (every #'digit-char-p (subseq stem 40))
         (let ((generation (parse-integer stem :start 40)))
           (and (<= 1 generation +physical-backup-lease-allocation-maximum-generation+)
                (member type '("alloc" "tmp") :test #'equal)
                (equal entry (format nil "~a.~2,'0d.~a" (subseq stem 0 39) generation type)))))))

(defun physical-backup-lease-allocation-inventory ()
  (unless (typep *physical-backup-lease-allocation-limit* '(integer 0 65536))
    (error "invalid backup lease allocation inventory limit"))
  (multiple-value-bind (directory stat)
      (physical-backup-lease-check-directory
       :directory (physical-backup-lease-allocation-directory))
    (unless stat
      (fsync-directory *pgdata*)
      (return-from physical-backup-lease-allocation-inventory (values nil nil)))
    (let ((records nil) (temporaries nil)
                        (limit *physical-backup-lease-allocation-limit*))
      (dolist (entry (directory-entry-names directory :maximum-entries (* 2 limit)
                                                      :maximum-name-bytes (* 2 limit 48)))
        (unless (physical-backup-lease-allocation-entry-p entry)
          (error "unexpected backup lease allocation inventory entry"))
        (let* ((temporary (equal "tmp" (pathname-type entry)))
               (path (merge-pathnames entry directory)))
          (physical-backup-lease-file-identity
           (sb-posix:lstat (namestring path)) temporary
           +physical-backup-lease-allocation-maximum-bytes+)
          (if temporary (push path temporaries) (push path records))))
      (when (or (> (length records) limit) (> (length temporaries) limit))
        (error "backup lease allocation inventory exceeds capacity"))
      (fsync-directory directory)
      (values (sort records #'string< :key #'namestring)
              (sort temporaries #'string< :key #'namestring)))))

(defvar *physical-backup-lease-allocations* (make-hash-table :test #'equal))

(defun physical-backup-lease-allocation-load-candidates (parents)
  (physical-backup-lease-require-locks)
  (multiple-value-bind (paths temporaries) (physical-backup-lease-allocation-inventory)
    (let ((allocations (make-hash-table :test #'equal)))
      (dolist (path paths)
        (let* ((row (physical-backup-lease-allocation-read path :stabilize t))
               (name (getf row :name)))
          (when (gethash name allocations)
            (error "backup lease allocation inventory shares a successor name"))
          (setf (gethash name allocations) row)))
      (physical-backup-lease-allocation-check-graph parents allocations)
      (values allocations temporaries))))

(defun physical-backup-lease-allocation-install-records (allocations)
  (physical-backup-lease-require-locks)
  (setf *physical-backup-lease-allocations* allocations)
  (values))

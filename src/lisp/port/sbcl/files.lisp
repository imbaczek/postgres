;;;; files.lisp -- durable page I/O over SBCL file streams.

(in-package #:pg)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-posix))

(defun open-relation-file (pathname)
  (ensure-directories-exist pathname)
  (open pathname
        :direction :io
        :element-type '(unsigned-byte 8)
        :if-exists :overwrite
        :if-does-not-exist :create))

(defun relation-nblocks (stream)
  (let ((len (file-length stream)))
    (floor len +blcksz+)))

(defun read-page (stream block)
  "Return the page vector for BLOCK, or NIL if the block does not exist."
  (let ((buf (make-array +blcksz+ :element-type '(unsigned-byte 8)
                                  :initial-element 0)))
    (file-position stream (* block +blcksz+))
    (let ((n (read-sequence buf stream)))
      (if (< n +blcksz+)
          nil
          buf))))

(defun write-page (stream block page)
  (file-position stream (* block +blcksz+))
  (write-sequence page stream)
  (finish-output stream)
  (values))

(defun fsync-stream (stream)
  (finish-output stream)
  (sb-posix:fsync (sb-sys:fd-stream-fd stream)))

(defun fsync-directory (dir)
  "Fsync DIR so that a rename inside it survives a crash."
  (let* ((name (namestring dir))
         (trimmed (if (and (> (length name) 1)
                           (char= (char name (1- (length name))) #\/))
                      (subseq name 0 (1- (length name)))
                      name))
         (fd (sb-posix:open trimmed sb-posix:o-rdonly)))
    (unwind-protect
         (sb-posix:fsync fd)
      (sb-posix:close fd))))

(defun atomic-replace-file (target-path temporary-path directory write-function
                            &key (element-type 'character)
                              (fsync-function #'sb-posix:fsync)
                              (rename-function #'sb-posix:rename)
                              (fsync-directory-function #'fsync-directory))
  "Write and fsync TEMPORARY-PATH, rename it to TARGET-PATH, and fsync DIRECTORY."
  (ensure-directories-exist target-path)
  (unwind-protect
       (progn
         (with-open-file (stream temporary-path
                                 :direction :output
                                 :if-exists :supersede
                                 :if-does-not-exist :create
                                 :element-type element-type)
           (funcall write-function stream)
           (finish-output stream)
           (funcall fsync-function (sb-sys:fd-stream-fd stream)))
         (funcall rename-function (namestring temporary-path)
                  (namestring target-path))
         (funcall fsync-directory-function directory))
    (ignore-errors (delete-file temporary-path)))
  (values))

(defmacro with-atomic-output ((stream target-path temporary-path directory
                               &rest options)
                              &body body)
  "Write BODY to a temporary file and atomically replace TARGET-PATH."
  `(atomic-replace-file ,target-path ,temporary-path ,directory
                        (lambda (,stream) ,@body)
                        ,@options))

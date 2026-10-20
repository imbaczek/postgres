;;;; lwlock.lisp -- coarse locks for the MVP.

(in-package #:pg)

(defvar *write-lock* (sb-thread:make-mutex :name "pg write lock"))
(defvar *catalog-lock* (sb-thread:make-mutex :name "pg catalog lock"))
(defvar *buffer-lock* (sb-thread:make-mutex :name "pg buffer lock"))

(defmacro with-write-lock (&body body)
  `(sb-thread:with-mutex (*write-lock*) ,@body))

(defmacro with-catalog-lock (&body body)
  `(sb-thread:with-mutex (*catalog-lock*) ,@body))

(defmacro with-write-catalog-locks (&body body)
  "Run BODY with the write lock acquired before the catalog lock."
  `(with-write-lock
     (with-catalog-lock ,@body)))

(defmacro with-buffer-lock (&body body)
  `(sb-thread:with-mutex (*buffer-lock*) ,@body))

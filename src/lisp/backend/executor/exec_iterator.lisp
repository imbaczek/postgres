;;;; exec_iterator.lisp -- same-thread resumable rows and appendable replay storage.
(in-package #:pg)

(defvar *sql-open-iterators* nil)

(defstruct (sql-iterator (:constructor %make-sql-iterator))
  next-function close-function closed-p)

(defun make-sql-iterator (next &optional close)
  (let ((iterator (%make-sql-iterator :next-function next :close-function close)))
    (when *sql-open-iterators* (setf (gethash iterator *sql-open-iterators*) t))
    iterator))

(defun sql-iterator-next (iterator)
  (check-for-interrupts)
  (unless (sql-iterator-closed-p iterator)
    (funcall (sql-iterator-next-function iterator))))

(defun close-sql-iterator (iterator)
  (sb-sys:without-interrupts
    (when (and iterator (not (sql-iterator-closed-p iterator)))
      (setf (sql-iterator-closed-p iterator) t)
      (unwind-protect
           (sb-sys:with-local-interrupts
             (when (sql-iterator-close-function iterator)
               (funcall (sql-iterator-close-function iterator))))
        (when *sql-open-iterators* (remhash iterator *sql-open-iterators*))
        (setf (sql-iterator-next-function iterator) nil
              (sql-iterator-close-function iterator) nil))))
  (values))

(defun map-sql-iterator (iterator callback)
  (loop
    (multiple-value-bind (row present) (sql-iterator-next iterator)
      (unless present (return))
      (when (eq (funcall callback row) :stop) (return :stop)))))

(defstruct (query-replay-store (:constructor %make-query-replay-store))
  producer spool eof-p readers closed-p
  (memory-rows (make-array 0 :adjustable t :fill-pointer 0)))

(defun make-query-replay-store (producer &optional (memory-limit *query-work-memory-bytes*))
  (%make-query-replay-store :producer producer :spool (make-query-row-spool memory-limit)))

(defun close-query-replay-store (store)
  (sb-sys:without-interrupts
    (unless (query-replay-store-closed-p store)
      (setf (query-replay-store-closed-p store) t)
      (let ((spool (query-replay-store-spool store)))
        (unwind-protect
             (sb-sys:with-local-interrupts
               (dolist (reader (copy-list (query-replay-store-readers store)))
                 (close-sql-iterator reader)))
          (unwind-protect
               (close-sql-iterator (query-replay-store-producer store))
            (unwind-protect
                 (progn
                   (close-query-row-spool spool)
                   (when (query-row-spool-pathname spool)
                     (delete-query-temp-file (query-row-spool-pathname spool))))
              (setf (query-row-spool-rows spool) nil
                    (query-replay-store-memory-rows store) nil
                    (query-replay-store-readers store) nil
                    (query-replay-store-producer store) nil)))))))
  (values))

(defun append-query-replay-row (store row)
  (let* ((spool (query-replay-store-spool store))
         (copy (copy-query-sort-value row)))
    (unless (query-row-spool-pathname spool)
      (incf (query-row-spool-memory-bytes spool) 16))
    (query-row-spool-append spool copy)
    (if (query-row-spool-pathname spool)
        (setf (query-replay-store-memory-rows store) nil)
        (vector-push-extend copy (query-replay-store-memory-rows store)))))

(defun make-query-replay-reader (store)
  (let ((index 0) (input nil) (offset nil) (reader nil))
    (setf reader
          (make-sql-iterator
           (lambda ()
             (when (query-replay-store-closed-p store)
               (error "Cannot read a closed query replay store"))
             (let ((spool (query-replay-store-spool store)))
               (when (and (= index (query-row-spool-count spool))
                          (not (query-replay-store-eof-p store)))
                 (multiple-value-bind (row present)
                     (sql-iterator-next (query-replay-store-producer store))
                   (if present
                       (append-query-replay-row store row)
                       (progn
                         (setf (query-replay-store-eof-p store) t)
                         (close-sql-iterator (query-replay-store-producer store))
                         (query-row-spool-seal spool)))))
               (when (< index (query-row-spool-count spool))
                 (let ((value
                         (if (query-row-spool-pathname spool)
                             (progn
                               (when (query-row-spool-output-stream spool)
                                 (finish-output (query-row-spool-output-stream spool)))
                               (unless input
                                 (setf input (open-query-temp-input (query-row-spool-pathname spool)))
                                 (dotimes (skip index) (query-spill-read-record input))
                                 (setf offset (file-position input)))
                               (file-position input offset)
                               (prog1 (query-spill-read-record input)
                                 (setf offset (file-position input))))
                             (aref (query-replay-store-memory-rows store) index))))
                   (incf index)
                   (values (copy-query-sort-value value) t)))))
           (lambda ()
             (when input (finish-query-temp-stream input nil))
             (setf (query-replay-store-readers store)
                   (delete reader (query-replay-store-readers store) :test #'eq)))))
    (push reader (query-replay-store-readers store))
    reader))

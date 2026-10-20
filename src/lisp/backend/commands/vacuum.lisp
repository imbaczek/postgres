;;;; vacuum.lisp -- table-level VACUUM horizons and catalog updates.

(in-package #:pg)

(defun current-table-index-relfilenodes (table)
  (mapcar #'pg-index-relfilenode (table-indexes table)))

(defun call-with-table-maintenance-locks
    (table index-provider verify-function function)
  "Hold the heap and current index maintenance locks around FUNCTION."
  (let ((heap-relfilenode (pg-class-relfilenode table)))
    (loop
      for indexes = (funcall index-provider)
      for requested = (cons heap-relfilenode indexes)
      do (multiple-value-bind (locked-relations token)
             (acquire-relation-maintenance-locks requested)
           (let ((current-indexes (funcall index-provider)))
             (if (every (lambda (relfilenode)
                          (member relfilenode locked-relations :test #'=))
                        current-indexes)
                 (return-from call-with-table-maintenance-locks
                   (unwind-protect
                        (progn
                          (when verify-function
                            (funcall verify-function))
                          (funcall function current-indexes))
                     (release-relation-maintenance-locks
                      locked-relations token)))
                 (release-relation-maintenance-locks locked-relations token)))))))

(defun vacuum-catalog-relation
    (table index-provider verify-function
     &key forced-oldest-xmin forced-freeze-cutoff after-page-function
       fsync-function)
  "Vacuum one catalog relation and durably advance its relfrozenxid."
  (call-with-table-maintenance-locks
   table index-provider verify-function
   (lambda (index-relfilenodes)
     (let* ((next-xid (next-full-xid))
            (oldest-xmin (or forced-oldest-xmin (oldest-xmin)))
            (old-relfrozenxid (or (pg-class-relfrozenxid table)
                                  +first-normal-xid+))
            (age (max 0 (- next-xid old-relfrozenxid)))
            (freeze-target
              (if forced-freeze-cutoff
                  forced-freeze-cutoff
                  (if (>= age +vacuum-aggressive-freeze-age+)
                      next-xid
                      (max +first-normal-xid+
                           (- next-xid +vacuum-freeze-min-age+)))))
            (freeze-cutoff (min oldest-xmin freeze-target)))
       (heap-vacuum-relation-locked
        (pg-class-relfilenode table) index-relfilenodes
        oldest-xmin freeze-cutoff
        :after-page-function after-page-function)
       (with-write-catalog-locks
         (when verify-function (funcall verify-function))
         (setf (pg-class-relfrozenxid table)
               (max old-relfrozenxid freeze-cutoff))
         (save-catalog))
       (when fsync-function
         (funcall fsync-function (pg-class-relfilenode table)))
       (pg-class-relfrozenxid table)))))

(defun vacuum-table (table)
  "Run a plain manual VACUUM for TABLE in the current database."
  (vacuum-catalog-relation
   table
   (lambda () (current-table-index-relfilenodes table))
   (lambda () (ensure-current-table table))))

(defun vacuum-all-relations-for-xid-wraparound
    (boundary &key fsync-function after-page-function)
  "Freeze each catalog relation to BOUNDARY, skipping all-frozen VM pages."
  (maphash
   (lambda (database-name database)
     (declare (ignore database-name))
     (maphash
      (lambda (table-name table)
        (declare (ignore table-name))
        (let ((index-provider
                (lambda ()
                  (loop for oid in (pg-class-indexes table)
                        for index = (gethash oid (database-indexes database))
                        when index collect (pg-index-relfilenode index))))
              (verify-function
                (lambda ()
                  (unless (eq table
                              (gethash (pg-class-name table)
                                       (database-tables database)))
                    (ereport "42P01" "relation \"~a\" does not exist"
                             (pg-class-name table))))))
          (vacuum-catalog-relation
           table index-provider verify-function
           :forced-oldest-xmin boundary
           :forced-freeze-cutoff boundary
           :after-page-function after-page-function
           :fsync-function fsync-function)))
      (database-tables database)))
   (catalog-databases *catalog*))
  (values))

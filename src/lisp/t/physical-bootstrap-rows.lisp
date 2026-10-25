;;;; Inspect copied pages and CLOG read-only; do not initialize or start the target.
(require :asdf)
(push (uiop:pathname-parent-directory-pathname
       (uiop:pathname-directory-pathname *load-truename*))
      asdf:*central-registry*)
(asdf:load-system "pg")

(defun write-bootstrap-copy-rows (target output)
  (let* ((pg::*pgdata* (uiop:ensure-directory-pathname target))
         (catalog (pg::load-catalog))
         (database (gethash "postgres" (pg::catalog-databases catalog)))
         (table (gethash "bootstrap_data" (pg::database-tables database)))
         (pg::*next-xid* (pg::load-next-xid))
         (snapshot (pg::make-snapshot-struct :xmin pg::*next-xid* :xmax pg::*next-xid*
                                             :xip nil :curcid 0))
         (rows nil))
    (with-open-file (clog (pg::clog-path) :element-type '(unsigned-byte 8))
      (let ((pg::*clog-stream* clog))
        (with-open-file (heap (pg::relation-file-path (pg::pg-class-relfilenode table))
                              :element-type '(unsigned-byte 8))
          (assert (zerop (mod (file-length heap) pg::+blcksz+)))
          (dotimes (block (/ (file-length heap) pg::+blcksz+))
            (let ((page (pg::read-page heap block)))
              (assert (pg::wal-verify-page page (pg::pg-class-relfilenode table) block))
              (pg::heapam-scan-page
               page block (pg::pg-class-columns table) snapshot
               (lambda (tuple) (push (pg::htup-values tuple) rows))))))))
    (with-open-file (stream output :direction :output :if-exists :supersede)
      (dolist (row (sort rows #'< :key (lambda (row) (aref row 0))))
        (format stream "~d|~a~%" (aref row 0) (aref row 1))))))

(apply #'write-bootstrap-copy-rows (uiop:command-line-arguments))

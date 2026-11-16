(in-package #:pg)

(defun logical-slot-continuation-cold-read-temporary (directory name image)
  (call-with-logical-slot-copy-source-proof-fd
   directory name nil
   (lambda (fd facts)
     (let ((bytes (logical-slot-copy-import-control-read-fd fd (length image))))
       (unless (and (<= (length bytes) (length image))
                    (null (mismatch bytes image :end2 (length bytes))))
         (error "cold continuation temporary is not an exact new-state prefix"))
       (list (copy-list facts) bytes)))))

(defun logical-slot-continuation-cold-read-row (directory name check target new-image)
  (funcall check)
  (multiple-value-prog1
      (call-with-logical-slot-copy-source-proof-fd
       directory name t
       (lambda (child child-facts)
         (let* ((names (logical-slot-copy-import-control-names child 3))
                (temporary (member "state.tmp" names :test #'string=)))
           (unless (or (equal names '("state"))
                       (and target (string= target name) temporary
                            (equal names '("state" "state.tmp"))))
             (error "cold continuation inventory has an unclassified slot entry"))
           (call-with-logical-slot-copy-source-proof-fd
            child "state" nil
            (lambda (fd facts)
              (let* ((bytes (logical-slot-copy-import-control-read-fd fd +slot-state-maximum-bytes+))
                     (staged (when temporary
                               (logical-slot-continuation-cold-read-temporary child "state.tmp" new-image))))
                (wal-decode-replication-slot bytes name)
                (unless (equal names (logical-slot-copy-import-control-names child 3))
                  (error "cold continuation slot entries changed"))
                (list name (copy-list child-facts) (copy-list facts) bytes staged)))))))
    (funcall check)))

(defun logical-slot-continuation-cold-read-inventory (directory guard record)
  (when record (logical-slot-continuation-prepared-check record))
  (let* ((target (when record (getf record :name)))
         (image (when record
                  (second (find target (getf (getf record :new) :slots) :key #'first :test #'string=)))))
    (logical-slot-continuation-observe-slot-inventory
     directory guard
     (lambda (directory name check)
       (logical-slot-continuation-cold-read-row directory name check target image)))))

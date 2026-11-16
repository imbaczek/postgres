(in-package #:pg)

(defvar *logical-slot-continuation-inventory-test-hook* nil)

(defun logical-slot-continuation-slot-inventory-names (directory)
  (let* ((maximum +logical-slot-continuation-obligation-limit+)
         (names (remove ".logical-slot-copy.work"
                        (logical-slot-copy-import-control-names directory (1+ maximum))
                        :test #'string=)))
    (unless (and (<= (length names) maximum) (every #'slot-state-valid-name-p names))
      (error "continuation slot inventory has an unclassified entry or exceeds its bound"))
    names))

(defun logical-slot-continuation-read-slot-inventory-row (directory name check)
  (funcall check)
  (multiple-value-prog1
      (call-with-logical-slot-copy-source-proof-fd
       directory name t
       (lambda (child child-facts)
         (unless (equal '("state") (logical-slot-copy-import-control-names child 3))
           (error "continuation slot inventory requires one stable state file"))
         (call-with-logical-slot-copy-source-proof-fd
          child "state" nil
          (lambda (fd facts)
            (let ((bytes (logical-slot-copy-import-control-read-fd fd +slot-state-maximum-bytes+)))
              (wal-decode-replication-slot bytes name)
              (unless (equal '("state") (logical-slot-copy-import-control-names child 3))
                (error "continuation slot inventory changed during its read"))
              (list name (copy-list child-facts) (copy-list facts) bytes))))))
    (funcall check)))

(defun logical-slot-continuation-read-slot-inventory (directory guard)
  (unless (functionp guard) (error "continuation slot inventory requires a directory guard"))
  (funcall guard)
  (let ((root-facts (physical-image-file-facts directory)))
    (unless (and (sb-posix:s-isdir (third root-facts))
                 (= #o700 (logand #o7777 (third root-facts)))
                 (= (fifth root-facts) (sb-posix:geteuid)))
      (error "continuation slot inventory requires a private directory"))
    (let ((names (logical-slot-continuation-slot-inventory-names directory)))
      (labels ((check ()
                 (funcall guard)
                 (unless (equal root-facts (physical-image-file-facts directory))
                   (error "continuation slot inventory root changed")))
               (read-pass ()
                 (check)
                 (unless (equal names (logical-slot-continuation-slot-inventory-names directory))
                   (error "continuation slot inventory names changed"))
                 (multiple-value-prog1
                     (mapcar (lambda (name)
                               (logical-slot-continuation-read-slot-inventory-row directory name #'check))
                             names)
                   (check))))
        (let ((ledger (read-pass)))
          (when *logical-slot-continuation-inventory-test-hook*
            (funcall *logical-slot-continuation-inventory-test-hook* :between-passes))
          (unless (equalp ledger (read-pass))
            (error "continuation slot inventory changed between passes"))
          (check)
          (values (mapcar (lambda (row) (list (first row) (copy-seq (fourth row)))) ledger)
                  ledger))))))

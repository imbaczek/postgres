(in-package #:pg)

;; Observe saved slot files without writing, syncing or cleaning them up.
;; P7 policy, admission and continuation model authority are checked elsewhere;
;; unlike startup cleanup, this observation rejects unexpected entries.

(defvar *logical-slot-continuation-inventory-test-hook* nil)

(defun logical-slot-continuation-slot-inventory-names (directory)
  ;; Bound slot names to 1024.  The fixed local work entry is opaque and is
  ;; excluded without opening it or reading its metadata; reject every other invalid name.
  (let* ((maximum +logical-slot-continuation-obligation-limit+)
         (names (remove ".logical-slot-copy.work"
                        (logical-slot-copy-import-control-names directory (1+ maximum))
                        :test #'string=)))
    (unless (and (<= (length names) maximum) (every #'slot-state-valid-name-p names))
      (error "continuation slot inventory has an unclassified entry or exceeds its bound"))
    names))

(defun logical-slot-continuation-read-slot-inventory-row (directory name check)
  ;; Open the private child and state without following links or crossing a
  ;; mount.  Require only state, rejecting state.tmp and unknown entries.
  ;; The WAL decoder checks supported formats, including grammar and CRC;
  ;; retain exact bytes, including legacy encodings, rather than re-encode.
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

(defun logical-slot-continuation-observe-inventory (directory guard read-names read-row)
  (unless (and (functionp guard) (functionp read-names) (functionp read-row))
    (error "continuation inventory requires a guard and observers"))
  (funcall guard)
  (let ((root-facts (physical-image-file-facts directory)))
    (unless (and (sb-posix:s-isdir (third root-facts))
                 (= #o700 (logand #o7777 (third root-facts)))
                 (= (fifth root-facts) (sb-posix:geteuid)))
      (error "continuation slot inventory requires a private directory"))
    (let ((names (funcall read-names directory)))
      (labels ((check ()
                 (funcall guard)
                 (unless (equal root-facts (physical-image-file-facts directory))
                   (error "continuation slot inventory root changed")))
               (read-pass ()
                 (check)
                 (unless (equal names (funcall read-names directory))
                   (error "continuation slot inventory names changed"))
                 (multiple-value-prog1
                     (mapcar (lambda (name) (funcall read-row directory name #'check)) names)
                   (check))))
        (let ((ledger (read-pass)))
          (when *logical-slot-continuation-inventory-test-hook*
            (funcall *logical-slot-continuation-inventory-test-hook* :between-passes))
          (unless (equalp ledger (read-pass))
            (error "continuation slot inventory changed between passes"))
          (check)
          ledger)))))

(defun logical-slot-continuation-observe-slot-inventory (directory guard read-row)
  (let ((ledger (logical-slot-continuation-observe-inventory
                 directory guard #'logical-slot-continuation-slot-inventory-names read-row)))
    (values (mapcar (lambda (row) (list (first row) (copy-seq (fourth row)))) ledger)
            ledger)))

(defun logical-slot-continuation-read-slot-inventory (directory guard)
  ;; Stable observation still rejects all temporaries. The shared kernel checks
  ;; full root facts, complete names and two independently opened row ledgers.
  (logical-slot-continuation-observe-slot-inventory
   directory guard #'logical-slot-continuation-read-slot-inventory-row))

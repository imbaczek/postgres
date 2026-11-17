(in-package #:pg)

(defun logical-slot-continuation-cold-entry-base (name)
  (unless (and (stringp name) (plusp (length name)))
    (error "cold cleanup requires a slot or staging name"))
  (let* ((staging (and (>= (length name) 4)
                       (string= ".tmp" name :start2 (- (length name) 4))))
         (base (if staging (subseq name 0 (- (length name) 4)) name)))
    (unless (slot-state-valid-name-p base)
      (error "cold cleanup inventory has an invalid slot or staging name"))
    (values base (not (null staging)))))

(defun logical-slot-continuation-cold-raw-names (directory)
  (let* ((maximum +logical-slot-continuation-obligation-limit+)
         (names (remove ".logical-slot-copy.work"
                        (logical-slot-copy-import-control-names directory (1+ (* 2 maximum)))
                        :test #'string=))
         (staging (loop for name in names
                        count (nth-value 1 (logical-slot-continuation-cold-entry-base name)))))
    (unless (and (<= staging maximum) (<= (- (length names) staging) maximum))
      (error "cold cleanup inventory exceeds its slot or staging bound"))
    names))

(defun logical-slot-continuation-cold-read-raw-file (directory name check)
  (funcall check)
  (multiple-value-prog1
      (call-with-logical-slot-copy-source-proof-fd
       directory name nil
       (lambda (fd facts)
         (list (copy-seq name) (copy-list facts)
               (logical-slot-copy-import-control-read-fd fd +slot-state-maximum-bytes+))))
    (funcall check)))

(defun logical-slot-continuation-cold-read-raw-row (directory name check)
  (funcall check)
  (multiple-value-prog1
      (call-with-logical-slot-copy-source-proof-fd
       directory name t
       (lambda (child facts)
         (let ((names (logical-slot-copy-import-control-names child 3)))
           (unless (every (lambda (entry) (member entry '("state" "state.tmp") :test #'string=)) names)
             (error "cold cleanup inventory has an unexpected slot file"))
           (let ((files (mapcar (lambda (entry)
                                  (logical-slot-continuation-cold-read-raw-file child entry check))
                                names)))
             (unless (equal names (logical-slot-copy-import-control-names child 3))
               (error "cold cleanup slot contents changed during observation"))
             (multiple-value-bind (base staging)
                 (logical-slot-continuation-cold-entry-base name)
               (list :name (copy-seq name) :base (copy-seq base) :staging staging
                     :directory (copy-list facts) :files files))))))
    (funcall check)))

(defun logical-slot-continuation-cold-read-raw-inventory (directory guard)
  (logical-slot-continuation-observe-inventory
   directory guard #'logical-slot-continuation-cold-raw-names
   #'logical-slot-continuation-cold-read-raw-row))

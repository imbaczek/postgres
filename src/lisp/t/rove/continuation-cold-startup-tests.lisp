(in-package #:pg)

(defun unit-continuation-cold-write-temp (directory name bytes)
  (let ((path (merge-pathnames (concatenate 'string name "/state.tmp")
                               (physical-image-fd-path directory))))
    (with-open-file (stream path :direction :output :if-exists :error
                                 :element-type '(unsigned-byte 8))
      (write-sequence bytes stream))
    (sb-posix:chmod (namestring path) #o600)
    path))

(rove:deftest continuation-cold-inventory-preserves-unselected-next-state
    (let* ((record (unit-continuation-prepared))
           (old (getf (getf record :old) :slots))
           (next (second (first (getf (getf record :new) :slots))))
           (*wal-durable-lsn* 192))
      (dolist (length (list 0 (floor (length next) 2) (length next)))
        (unit-continuation-inventory-fixture
         old
         (lambda (directory check)
           (let* ((prefix (subseq next 0 length))
                  (path (unit-continuation-cold-write-temp directory "alpha" prefix))
                  (facts (physical-image-file-facts -100 (namestring path))))
             (multiple-value-bind (rows ledger)
                 (logical-slot-continuation-cold-read-inventory directory check record)
               (rove:ok (logical-slot-continuation-slot-rows-equal-p old rows))
               (rove:ok (eq :old (logical-slot-continuation-prepared-classify record rows)))
               (rove:ok (equalp prefix (second (fifth (first ledger))))))
             (rove:ok (equal facts (physical-image-file-facts -100 (namestring path))))
             (rove:ok (equalp prefix (read-bounded-regular-file path (length next))))))))))

(cl-spec:defproperty continuation-cold-selection-preserves-historical-generation-and-seal
    ((generation (range integer 1 18446744073709551614))
     (identity (range integer 0 340282366920938463463374607431768211455))
     (advance (range integer 0 31))
     (side (range integer 0 1)))
  (:about logical-slot-continuation-cold-classify)
  (:trials (:normal 200))
  (multiple-value-bind (certificate host ignored) (unit-continuation-active-inputs identity)
    (declare (ignore ignored))
    (let* ((record (unit-continuation-prepared :generation generation :identity identity :advance advance))
           (which (if (zerop side) :old :new))
           (snapshot (getf record which))
           (rows (getf snapshot :slots)))
      (setf (sixth host) (+ 256 advance))
      (multiple-value-bind (selected choice)
          (logical-slot-continuation-cold-classify
           :prepared record rows (getf snapshot :binding) certificate host)
        (and (eq choice which)
             (equalp snapshot selected)
             (= (+ generation side) (getf selected :generation))
             (< (first (getf selected :sealed-end)) (sixth host))
             (multiple-value-bind (kind model)
                 (logical-slot-continuation-cold-decode-selected (logical-slot-continuation-encode selected))
               (and (eq kind :stable)
                    (equalp selected
                            (logical-slot-continuation-cold-classify
                             kind model rows (getf snapshot :binding) certificate host)))))))))

(rove:deftest continuation-cold-selected-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-cold-selection-preserves-historical-generation-and-seal
                                         :seed 11506 :profile :normal))))
      (format t "~&COLD-SELECTED-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-cold-inventory-rejects-unclassified-temporaries
    (let* ((record (unit-continuation-prepared))
           (old (getf (getf record :old) :slots))
           (next (second (first (getf (getf record :new) :slots))))
           (*wal-durable-lsn* 192))
      (dolist (mode '(:stable :unrelated :wrong-prefix))
        (unit-continuation-inventory-fixture
         old
         (lambda (directory check)
           (let* ((bytes (copy-seq next))
                  (name (if (eq mode :unrelated) "beta" "alpha")))
             (when (eq mode :wrong-prefix) (setf (aref bytes 0) 0))
             (let* ((path (unit-continuation-cold-write-temp directory name bytes))
                    (facts (physical-image-file-facts -100 (namestring path))))
               (rove:ok (handler-case
                            (progn (logical-slot-continuation-cold-read-inventory
                                    directory check (unless (eq mode :stable) record)) nil)
                          (error () t)))
               (rove:ok (equal facts (physical-image-file-facts -100 (namestring path))))
               (rove:ok (equalp bytes (read-bounded-regular-file path (length next)))))))))))

(rove:deftest continuation-cold-inventory-detects-byte-identical-temporary-substitution
    (let* ((record (unit-continuation-prepared))
           (old (getf (getf record :old) :slots))
           (next (second (first (getf (getf record :new) :slots))))
           (*wal-durable-lsn* 192))
      (unit-continuation-inventory-fixture
       old
       (lambda (directory check)
         (let* ((path (unit-continuation-cold-write-temp directory "alpha" next))
                (replacement (merge-pathnames "state.replacement" path))
                (changed nil)
                (*logical-slot-continuation-inventory-test-hook*
                  (lambda (point)
                    (when (eq point :between-passes)
                      (with-open-file (stream replacement :direction :output :if-exists :error
                                                          :element-type '(unsigned-byte 8))
                        (write-sequence next stream))
                      (sb-posix:chmod (namestring replacement) #o600)
                      (sb-posix:rename (namestring replacement) (namestring path))
                      (setf changed t)))))
           (rove:ok (handler-case
                        (progn (logical-slot-continuation-cold-read-inventory directory check record) nil)
                      (error () t)))
           (rove:ok changed)
           (rove:ok (equalp next (read-bounded-regular-file path (length next)))))))))

(rove:deftest continuation-cold-reservation-forgeries-refuse-before-io
    (let* ((calls 0) (reads 0) (original (symbol-function 'physical-image-file-facts))
                     (reservation (%make-logical-slot-continuation-cold-reservation
                                   :state (lambda (&rest ignored) (declare (ignore ignored)) (incf calls))))
                     (storage (%make-logical-slot-continuation-startup-storage
                               :state (lambda (&rest ignored) (declare (ignore ignored)) (incf calls)))))
      (unwind-protect
           (progn
             (setf (symbol-function 'physical-image-file-facts)
                   (lambda (&rest arguments) (incf reads) (apply original arguments)))
             (sb-thread:with-mutex (*wal-lock*)
               (dolist (action
                        (list (lambda () (logical-slot-continuation-cold-reservation-call reservation :check))
                              (lambda () (logical-slot-continuation-cold-reservation-call
                                          (copy-logical-slot-continuation-cold-reservation reservation) :failure-cleanup))
                              (lambda () (logical-slot-continuation-ensure-cold-reservation storage))
                              (lambda () (logical-slot-continuation-ensure-cold-reservation
                                          (%make-logical-slot-continuation-runtime-storage)))))
                 (rove:ok (handler-case (progn (funcall action) nil) (error () t)))))
             (rove:ok (zerop calls))
             (rove:ok (zerop reads)))
        (setf (symbol-function 'physical-image-file-facts) original))))

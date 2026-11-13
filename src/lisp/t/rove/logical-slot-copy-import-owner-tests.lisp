(in-package #:pg)

(rove:deftest logical-import-owner-cannot-use-cold-receipt-publisher
    (let ((owner (%make-logical-slot-copy-import-owner))
          (calls 0)
          (original (symbol-function 'logical-slot-copy-input-check-observations)))
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-slot-copy-input-check-observations)
                   (lambda (&rest ignored) (declare (ignore ignored)) (incf calls) t))
             (rove:ok (not (logical-slot-copy-startup-owner-p owner)))
             (rove:ok (rove:signals
                       (logical-slot-copy-check-base-proof-input owner nil nil) 'error))
             (rove:ok (zerop calls) "writer role is checked before observations"))
        (setf (symbol-function 'logical-slot-copy-input-check-observations) original))))

(rove:deftest logical-copy-startup-input-retains-cold-entry-guard
    (let ((*wal-ready* t) (calls 0)
                          (original (symbol-function 'call-with-logical-slot-copy-owned-input)))
      (unwind-protect
           (progn
             (setf (symbol-function 'call-with-logical-slot-copy-owned-input)
                   (lambda (&rest ignored) (declare (ignore ignored)) (incf calls)))
             (rove:ok (rove:signals
                       (call-with-logical-slot-copy-startup-input
                        (lambda (&rest ignored) (declare (ignore ignored)))) 'error))
             (rove:ok (zerop calls) "cold guard precedes ownership construction"))
        (setf (symbol-function 'call-with-logical-slot-copy-owned-input) original))))

(rove:deftest logical-import-input-rejects-unowned-exclusion-before-construction
    (dolist (depth '(0 1))
      (let ((*live-operation-depth* depth) (calls 0)
                                           (original (symbol-function 'call-with-logical-slot-copy-owned-input)))
        (unwind-protect
             (progn
               (setf (symbol-function 'call-with-logical-slot-copy-owned-input)
                     (lambda (&rest ignored) (declare (ignore ignored)) (incf calls)))
               (rove:ok (rove:signals
                         (call-with-logical-slot-copy-import-input
                          (lambda (&rest ignored) (declare (ignore ignored)))) 'error))
               (rove:ok (zerop calls) "exclusion guard precedes ownership construction"))
          (setf (symbol-function 'call-with-logical-slot-copy-owned-input) original)))))


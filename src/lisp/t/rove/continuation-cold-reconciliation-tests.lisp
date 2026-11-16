(in-package #:pg)

(defun unit-continuation-retirement-expected (directory name)
  (let ((path (merge-pathnames (concatenate 'string name ".tmp")
                               (physical-image-fd-path directory))))
    (when (slot-state-lstat path)
      (list (physical-image-file-facts directory (concatenate 'string name ".tmp"))
            (read-bounded-regular-file path 2048)))))

(rove:deftest continuation-retirement-refuses-a-nonprivate-parent-without-a-temporary
    (unit-continuation-replacement-fixture
     :selector
     (lambda (directory check name old new)
       (declare (ignore check name old new))
       (let ((path (namestring (physical-image-fd-path directory))) (entered nil))
         (unwind-protect
              (progn
                (sb-posix:chmod path #o755)
                (rove:ok
                 (handler-case
                     (progn
                       (call-with-logical-slot-continuation-temporary-retirement
                        directory :selector nil (lambda (removed) (declare (ignore removed)))
                        (lambda (operation) (declare (ignore operation)) (setf entered t)))
                       nil)
                   (error () t)))
                (rove:ok (not entered)))
           (sb-posix:chmod path #o700))))))

(cl-spec:defproperty continuation-retirement-preserves-the-canonical-inode
    ((kind (range integer 0 3)) (part (range integer 0 9)))
  (:about call-with-logical-slot-continuation-temporary-retirement)
  (:trials (:normal 80))
  (let ((purpose (if (oddp kind) :selector :slot-state))
        (prefix (when (>= kind 2) part)))
    (unit-continuation-replacement-fixture
     purpose
     (lambda (directory check name old new)
       (declare (ignore new))
       (let ((before (physical-image-file-facts directory name))
             (expected (unit-continuation-retirement-expected directory name))
             (receipt nil))
         (call-with-logical-slot-continuation-temporary-retirement
          directory purpose expected
          (lambda (removed) (declare (ignore removed)) (funcall check))
          (lambda (operation)
            (funcall operation :retire)
            (funcall operation :sync)
            (setf receipt (funcall operation :receipt))))
         (and (fourth receipt) (fifth receipt)
              (equal before (physical-image-file-facts directory name))
              (equalp old (read-bounded-regular-file
                           (merge-pathnames name (physical-image-fd-path directory)) 2048))
              (null (slot-state-lstat
                     (merge-pathnames (concatenate 'string name ".tmp")
                                      (physical-image-fd-path directory)))))))
     prefix)))

(rove:deftest continuation-retirement-generated-inode-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property
                    'continuation-retirement-preserves-the-canonical-inode
                    :seed 11531 :profile :normal))))
      (format t "~&RETIREMENT-INODE-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)))
      (rove:ok (= 80 (getf result :trials)))))

(rove:deftest continuation-retirement-rejects-foreign-and-expired-operations-before-io
    (unit-continuation-replacement-fixture
     :selector
     (lambda (directory check name old new)
       (declare (ignore old new))
       (let ((saved nil) (calls 0))
         (call-with-logical-slot-continuation-temporary-retirement
          directory :selector (unit-continuation-retirement-expected directory name)
          (lambda (removed) (declare (ignore removed)) (incf calls) (funcall check))
          (lambda (operation)
            (setf saved operation calls 0)
            (rove:ok
             (sb-thread:join-thread
              (sb-thread:make-thread
               (lambda ()
                 (handler-case (progn (funcall operation :retire) nil)
                   (error () t))))))
            (rove:ok (zerop calls))
            (funcall operation :retire)
            (funcall operation :sync)))
         (setf calls 0)
         (rove:ok (handler-case (progn (funcall saved :check) nil) (error () t)))
         (rove:ok (zerop calls))))
     4))

(rove:deftest continuation-retirement-caught-failure-is-sticky-after-unlink
    (dolist (purpose '(:selector :slot-state))
      (unit-continuation-replacement-fixture
       purpose
       (lambda (directory check name old new)
         (declare (ignore new))
         (let ((caught nil) (sticky nil)
                            (*logical-slot-continuation-retirement-test-hook*
                              (lambda (point actual)
                                (declare (ignore actual))
                                (when (eq point :after-unlink) (error "retirement cut")))))
           (rove:ok
            (handler-case
                (progn
                  (call-with-logical-slot-continuation-temporary-retirement
                   directory purpose (unit-continuation-retirement-expected directory name)
                   (lambda (removed) (declare (ignore removed)) (funcall check))
                   (lambda (operation)
                     (setf caught (handler-case (progn (funcall operation :retire) nil) (error () t))
                           sticky (handler-case (progn (funcall operation :sync) nil) (error () t)))))
                  nil)
              (error () t)))
           (rove:ok (and caught sticky))
           (rove:ok (equalp old (read-bounded-regular-file
                                 (merge-pathnames name (physical-image-fd-path directory)) 2048)))
           (rove:ok (null (slot-state-lstat
                           (merge-pathnames (concatenate 'string name ".tmp")
                                            (physical-image-fd-path directory)))))))
       0)))

(rove:deftest continuation-retirement-refuses-a-substituted-temporary-before-unlink
    (unit-continuation-replacement-fixture
     :slot-state
     (lambda (directory check name old new)
       (declare (ignore old new))
       (let* ((expected (unit-continuation-retirement-expected directory name))
              (path (merge-pathnames "state.tmp" (physical-image-fd-path directory)))
              (backup (merge-pathnames "held-original" (physical-image-fd-path directory)))
              (entered nil))
         (sb-posix:rename (namestring path) (namestring backup))
         (with-open-file (stream path :direction :output :if-exists :error
                                      :element-type '(unsigned-byte 8))
           (write-sequence (second expected) stream))
         (sb-posix:chmod (namestring path) #o600)
         (rove:ok
          (handler-case
              (progn
                (call-with-logical-slot-continuation-temporary-retirement
                 directory :slot-state expected
                 (lambda (removed) (declare (ignore removed)) (funcall check))
                 (lambda (operation) (setf entered t) (funcall operation :retire)))
                nil)
            (error () t)))
         (rove:ok (not entered))
         (rove:ok (slot-state-lstat path))))
     4))

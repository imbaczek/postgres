(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-cancellation-publication-admits-only-preconfiguration
    ((shape (range integer 0 19)) (selected boolean))
  (:about pg::physical-reseed-follow-cancellation-preconfiguration-record)
  (:trials (:normal 100))
  (let ((record (reseed-cancellation-codec-fixture shape selected)))
    (eq (< shape 3)
        (handler-case
            (equal (pg::physical-reseed-follow-copy record)
                   (pg::physical-reseed-follow-cancellation-preconfiguration-record record))
          (error () nil)))))

(deftest generated-reseed-cancellation-publication-domain
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-cancellation-publication-admits-only-preconfiguration
                                         :seed 186 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-cancellation-publication-refuses-every-frozen-configuration
    (dotimes (shape 20)
      (dolist (selected '(nil t))
        (let ((record (reseed-cancellation-codec-fixture shape selected)))
          (ok (eq (< shape 3)
                  (handler-case
                      (equal (pg::physical-reseed-follow-copy record)
                             (pg::physical-reseed-follow-cancellation-preconfiguration-record record))
                    (error () nil))))))))

(deftest reseed-cancellation-begin-publishes-pending-and-preserves-retry-inode
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let* ((expected (pg::physical-reseed-follow-cancellation-begin-record record))
              (original-connect (symbol-function 'pg::physical-libpq-connect-frozen))
              (connections 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                      (lambda (&rest arguments) (declare (ignore arguments))
                        (incf connections) (error "BeginCancel attempted a source connection")))
                (ok (equal expected (pg::physical-reseed-follow-begin-cancellation owner)))
                (let ((identity (pg::physical-reseed-follow-file-identity
                                 (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.follow" root))))))
                  (ok (equal expected (pg::physical-reseed-follow-begin-cancellation owner)))
                  (ok (equal expected (pg::physical-reseed-follow-recover-cancellation owner)))
                  (ok (equal identity
                             (pg::physical-reseed-follow-file-identity
                              (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.follow" root)))))))
                (ok (equalp (pg::physical-reseed-follow-image expected)
                            (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root))))
                (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                (ok (zerop connections)))
           (setf (symbol-function 'pg::physical-libpq-connect-frozen) original-connect))))))

(deftest reseed-cancellation-recovery-promotes-only-the-exact-pending-temporary
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let* ((expected (pg::physical-reseed-follow-cancellation-begin-record record))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
         (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image expected))
         (let ((identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring temporary)))))
           (ok (signals (pg::physical-reseed-follow-begin-cancellation owner) 'error))
           (ok (equal expected (pg::physical-reseed-follow-recover-cancellation owner)))
           (ok (equal (subseq identity 0 7)
                      (subseq (pg::physical-reseed-follow-file-identity
                               (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.follow" root))))
                              0 7))))
         (ok (not (probe-file temporary)))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-cancellation-recovery-keeps-partial-temporary-fenced
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (bytes (subseq (pg::physical-reseed-follow-image
                              (pg::physical-reseed-follow-cancellation-begin-record record)) 0 17)))
         (reseed-follow-owner-test-write temporary bytes)
         (ok (signals (pg::physical-reseed-follow-recover-cancellation owner) 'error))
         (ok (equalp bytes (pg::recovery-read-bytes temporary)))
         (ok (equalp (pg::physical-reseed-follow-image record)
                     (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))))

(deftest reseed-cancellation-stable-pending-survives-actual-attempt-abort
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
         (reseed-follow-prepare-test-abort root record)
         (ok (equal pending (pg::physical-reseed-follow-recover-cancellation owner)))
         (ok (equal pending (pg::physical-reseed-follow-begin-cancellation owner)))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-cancellation-temporary-cannot-promote-after-actual-attempt-abort
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (bytes (pg::physical-reseed-follow-image
                      (pg::physical-reseed-follow-cancellation-begin-record record))))
         (reseed-follow-owner-test-write temporary bytes)
         (reseed-follow-prepare-test-abort root record)
         (ok (signals (pg::physical-reseed-follow-recover-cancellation owner) 'error))
         (ok (equalp bytes (pg::recovery-read-bytes temporary)))
         (ok (equalp (pg::physical-reseed-follow-image record)
                     (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-cancellation-rejects-journal-mutation-after-final-root-close
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let ((original (symbol-function 'pg::call-with-physical-directory-root))
             (injected 0) (depth 0)
             (path (merge-pathnames "physical-reseed.follow" root)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::call-with-physical-directory-root)
                      (lambda (actual-owner function)
                        (incf depth)
                        (unwind-protect
                             (multiple-value-prog1
                                 (funcall original actual-owner function)
                               (when (and (= depth 1) (eq actual-owner owner) (zerop injected)
                                          (probe-file path)
                                          (getf (pg::physical-reseed-follow-decode
                                                 (pg::recovery-read-bytes path)) :cancellation))
                                 (incf injected)
                                 (with-open-file (stream path :direction :io :if-exists :overwrite
                                                              :element-type '(unsigned-byte 8))
                                   (let ((byte (read-byte stream)))
                                     (file-position stream 0)
                                     (write-byte byte stream)
                                     (finish-output stream)))))
                          (decf depth))))
                (ok (signals (pg::physical-reseed-follow-begin-cancellation owner) 'error))
                (ok (= 1 injected))
                (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))
           (setf (symbol-function 'pg::call-with-physical-directory-root) original))))))

(deftest reseed-cancellation-authority-refuses-configured-histories
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let ((calls 0))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (pg::call-with-physical-directory-root
               owner
               (lambda (root-fd directory)
                 (declare (ignore directory))
                 (loop for shape from 3 below 20 do
                   (dolist (selected '(nil t))
                     (let* ((previous (reseed-cancellation-codec-fixture shape selected))
                            (candidate (pg::physical-reseed-follow-cancellation-begin-record previous)))
                       (ok (equal "cancellation publication requires an unconfigured follow predecessor"
                                  (handler-case
                                      (pg::call-with-physical-reseed-follow-cancellation-authority
                                       owner root-fd candidate (list :record previous)
                                       (pg::physical-reseed-follow-image candidate) :write #'values
                                       (lambda (check) (declare (ignore check)) (incf calls)))
                                    (error (condition) (princ-to-string condition))))))))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))
         (ok (zerop calls))))))

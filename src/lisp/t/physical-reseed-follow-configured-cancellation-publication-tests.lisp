(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-cancellation-publication-dispatch-keeps-owner-boundaries
    ((shape (range integer 0 19)) (selected boolean))
  (:about pg::physical-reseed-follow-cancellation-publication-record)
  (:trials (:normal 100))
  (let ((record (reseed-cancellation-codec-fixture shape selected))
        (ordinary (pg::make-physical-reseed-follow-owner))
        (forged (pg::make-physical-reseed-follow-cancellation-owner)))
    (and (eq (< shape 3)
             (handler-case
                 (equal record
                        (pg::physical-reseed-follow-cancellation-publication-record ordinary record))
               (error () nil)))
         (handler-case
             (progn
               (pg::physical-reseed-follow-cancellation-publication-record forged record)
               nil)
           (error () t)))))

(deftest generated-reseed-cancellation-publication-owner-dispatch-contract
    (let ((result
            (cl-spec:result-data
             (cl-spec:run-property 'reseed-cancellation-publication-dispatch-keeps-owner-boundaries
                                   :seed 207 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(defun check-configured-cancellation-publication-geometry (kind)
  (call-with-reseed-inert-settlement-fixture
   kind
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (pg::call-with-physical-reseed-follow-cancellation-owner
      root
      (lambda (owner)
        (let* ((before (pg::physical-reseed-follow-read-configured-cancellation owner))
               (expected (pg::physical-reseed-follow-cancellation-begin-record before))
               (connect (symbol-function 'pg::physical-libpq-connect-frozen))
               (point (symbol-function 'pg::physical-reseed-follow-test-point))
               (published-checks 0)
               (connections 0) (record nil) (snapshot nil) (failure nil))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                       (lambda (&rest arguments)
                         (declare (ignore arguments))
                         (incf connections)
                         (error "local configured cancellation contacted its source")))
                 (setf (symbol-function 'pg::physical-reseed-follow-test-point)
                       (lambda (event path)
                         (funcall point event path)
                         (when (eq event :after-follow-rename)
                           (incf published-checks)
                           (let ((context (pg::physical-reseed-follow-installation-context owner)))
                             (ok (equal expected
                                        (pg::physical-reseed-follow-installation-context-record context)))
                             (ok (equal expected
                                        (getf (pg::physical-reseed-follow-installation-context-snapshot context)
                                              :record)))
                             (ok (signals (pg::physical-reseed-follow-begin-cancellation owner) 'error))
                             (ok (sb-thread:join-thread
                                  (sb-thread:make-thread
                                   (lambda ()
                                     (handler-case
                                         (progn
                                           (pg::physical-reseed-follow-cancellation-publication-record owner expected)
                                           nil)
                                       (error () t))))))))))
                 (handler-case
                     (setf record (pg::physical-reseed-follow-begin-cancellation owner))
                   (error (condition) (setf failure (princ-to-string condition))))
                 (ng failure (or failure "configured cancellation publishes without error"))
                 (ok (equal expected record))
                 (setf snapshot
                       (pg::call-with-physical-directory-root
                        owner
                        (lambda (fd directory)
                          (declare (ignore directory))
                          (pg::physical-reseed-follow-snapshot-at fd))))
                 (ok (equal expected (getf snapshot :record)))
                 (ok (equalp (pg::physical-reseed-follow-image expected)
                             (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                 (ng (pg::physical-adoption-path-present-p
                      (merge-pathnames "physical-reseed.follow.tmp" root)))
                 (ng (pg::physical-reseed-follow-owner-busy-p owner))
                 (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
                 (ok (zerop connections))
                 (ok (= 1 published-checks))
                 (ok (equal expected
                            (pg::physical-reseed-follow-read-configured-cancellation owner))))
            (setf (symbol-function 'pg::physical-libpq-connect-frozen) connect
                  (symbol-function 'pg::physical-reseed-follow-test-point) point))))))))

(deftest reseed-configured-cancellation-begin-publishes-pending-and-adopts-the-journal
    (check-configured-cancellation-publication-geometry :same-timeline))

(deftest reseed-configured-cancellation-publishes-retained-timeline-history
    (check-configured-cancellation-publication-geometry :retained-timeline))

(deftest reseed-configured-cancellation-publishes-before-fork-history
    (check-configured-cancellation-publication-geometry :before-fork))

(deftest reseed-configured-cancellation-publishes-at-fork-history
    (check-configured-cancellation-publication-geometry :at-fork))

(deftest reseed-configured-cancellation-publishes-later-writer-history
    (check-configured-cancellation-publication-geometry :later-writer))

(deftest reseed-configured-cancellation-recovers-a-published-marker-after-rename-error
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (let ((expected nil) (escaped nil) (fired 0)
                            (point (symbol-function 'pg::physical-reseed-follow-test-point)))
         (ok (signals
              (pg::call-with-physical-reseed-follow-cancellation-owner
               root
               (lambda (owner)
                 (setf escaped owner
                       expected (pg::physical-reseed-follow-cancellation-begin-record
                                 (pg::physical-reseed-follow-read-configured-cancellation owner)))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::physical-reseed-follow-test-point)
                              (lambda (event path)
                                (funcall point event path)
                                (when (eq event :after-follow-rename)
                                  (incf fired)
                                  (error "injected configured cancellation rename exit"))))
                        (pg::physical-reseed-follow-begin-cancellation owner))
                   (setf (symbol-function 'pg::physical-reseed-follow-test-point) point))))
              'error))
         (ok (= fired 1))
         (ok (and escaped (pg::physical-reseed-follow-owner-uncertain-p escaped)))
         (ng (pg::physical-reseed-follow-owner-busy-p escaped))
         (ok (equalp (pg::physical-reseed-follow-image expected)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ng (pg::physical-adoption-path-present-p
              (merge-pathnames "physical-reseed.follow.tmp" root)))
         (let ((identity (pg::physical-reseed-follow-file-identity
                          (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.follow" root))))))
           (pg::call-with-physical-reseed-follow-cancellation-owner
            root
            (lambda (owner)
              (ok (equal expected (pg::physical-reseed-follow-recover-cancellation owner)))
              (ok (equal expected (pg::physical-reseed-follow-begin-cancellation owner)))
              (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
              (ng (pg::physical-reseed-follow-owner-uncertain-p owner))))
           (ok (equal identity
                      (pg::physical-reseed-follow-file-identity
                       (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.follow" root)))))))))))

(deftest reseed-configured-cancellation-explicit-acquisition-recovers-the-exact-temporary
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (let* ((path (merge-pathnames "physical-reseed.follow" root))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (before (lineage-test-bytes path))
              (expected nil) (escaped nil) (fired 0) (admitted nil)
              (point (symbol-function 'pg::physical-reseed-follow-test-point)))
         (ok (signals
              (pg::call-with-physical-reseed-follow-cancellation-owner
               root
               (lambda (owner)
                 (setf escaped owner
                       expected (pg::physical-reseed-follow-cancellation-begin-record
                                 (pg::physical-reseed-follow-read-configured-cancellation owner)))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::physical-reseed-follow-test-point)
                              (lambda (event actual-path)
                                (funcall point event actual-path)
                                (when (eq event :before-follow-rename)
                                  (incf fired)
                                  (error "injected configured cancellation temporary exit"))))
                        (pg::physical-reseed-follow-begin-cancellation owner))
                   (setf (symbol-function 'pg::physical-reseed-follow-test-point) point))))
              'error))
         (ok (= fired 1))
         (ok (pg::physical-reseed-follow-owner-uncertain-p escaped))
         (ng (pg::physical-reseed-follow-owner-busy-p escaped))
         (ok (equalp before (lineage-test-bytes path)))
         (ok (equalp (pg::physical-reseed-follow-image expected) (lineage-test-bytes temporary)))
         (ok (signals
              (pg::call-with-physical-reseed-follow-cancellation-owner
               root (lambda (owner) (declare (ignore owner)) (setf admitted t)))
              'error))
         (ng admitted)
         (ok (equalp before (lineage-test-bytes path)))
         (let ((identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring temporary)))))
           (pg::call-with-physical-reseed-follow-cancellation-owner
            root
            (lambda (owner)
              (setf admitted t)
              (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
              (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
              (ng (pg::physical-reseed-follow-owner-busy-p owner)))
            :recover t)
           (ok admitted)
           (ng (pg::physical-adoption-path-present-p temporary))
           (ok (equalp (pg::physical-reseed-follow-image expected) (lineage-test-bytes path)))
           (ok (equal (subseq identity 0 7)
                      (subseq (pg::physical-reseed-follow-file-identity
                               (sb-posix:lstat (namestring path))) 0 7))))))))

(deftest reseed-configured-cancellation-explicit-acquisition-preserves-invalid-temporaries
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (let* ((path (merge-pathnames "physical-reseed.follow" root))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (before (lineage-test-bytes path))
              (record (pg::physical-reseed-follow-decode before))
              (expected (pg::physical-reseed-follow-cancellation-begin-record record))
              (bytes (pg::physical-reseed-follow-image expected))
              (connect (symbol-function 'pg::physical-libpq-connect-frozen))
              (connections 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                      (lambda (&rest arguments)
                        (declare (ignore arguments))
                        (incf connections)
                        (error "invalid configured cancellation recovery contacted its source")))
                (dolist (invalid (list (subseq bytes 0 (floor (length bytes) 2))
                                       before))
                  (testing (if (= (length invalid) (length before)) "predecessor" "partial")
                           (reseed-follow-owner-test-write temporary invalid)
                           (let ((identity (pg::physical-reseed-follow-file-identity
                                            (sb-posix:lstat (namestring temporary))))
                                 (admitted nil))
                             (ok (signals
                                  (pg::call-with-physical-reseed-follow-cancellation-owner
                                   root (lambda (owner) (declare (ignore owner)) (setf admitted t))
                                   :recover t)
                                  'error))
                             (ng admitted)
                             (ok (equalp before (lineage-test-bytes path)))
                             (ok (equalp invalid (lineage-test-bytes temporary)))
                             (ok (equal identity
                                        (pg::physical-reseed-follow-file-identity
                                         (sb-posix:lstat (namestring temporary))))))
                           (delete-file temporary)))
                (ok (zerop connections))
                (pg::call-with-physical-reseed-follow-cancellation-owner
                 root
                 (lambda (owner)
                   (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner))))))
           (setf (symbol-function 'pg::physical-libpq-connect-frozen) connect))))))

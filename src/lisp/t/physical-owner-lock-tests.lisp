(in-package #:pg-physical-tests)

(defun owner-lock-test-fdinfo (shape inode)
  (let* ((row (format nil "lock:~c1: FLOCK ADVISORY WRITE 123 08:30:~d 0 EOF~%"
                      #\Tab (if (= shape 6) (1+ inode) inode)))
         (header (format nil "pos:~c0~%flags:~c02100002~%mnt_id:~c88~%ino:~c~d~%"
                         #\Tab #\Tab #\Tab #\Tab inode)))
    (concatenate
     'string header
     (case shape
       (0 row)
       (1 (format nil "lock: 1: FLOCK ADVISORY READ 123 08:30:~d 0 EOF~%" inode))
       (2 "")
       (3 (format nil "lock: 1: POSIX ADVISORY WRITE 123 08:30:~d 0 EOF~%" inode))
       (4 (format nil "lock: 1: FLOCK MANDATORY WRITE 123 08:30:~d 0 EOF~%" inode))
       (5 (format nil "lock: 1: -> FLOCK ADVISORY WRITE 123 08:30:~d 0 EOF~%" inode))
       (6 row)
       (7 (concatenate 'string row row))
       (8 "lock: 1: FLOCK ADVISORY WRITE 123 invalid 0 EOF")
       (9 (format nil "lock: 1: FLOCK ADVISORY WRITE 123 08:30:~d 0~%" inode))
       (10 (concatenate 'string row (make-string 4097 :initial-element #\x)))
       (11 (format nil "lock: 1: FLOCK ADVISORY WRITE -1 08:30:~d 0 EOF~%" inode))
       (otherwise (error "unknown fdinfo fixture"))))))

(cl-spec:defproperty offline-owner-flock-evidence-is-exact
    ((shape (range integer 0 11)) (inode (range integer 1 1000000)))
  (:about pg::physical-file-exclusive-flock-p)
  (:trials (:normal 100))
  (let* ((text (owner-lock-test-fdinfo shape inode))
         (before (copy-seq text)))
    (and (eql (zerop shape)
              (not (null (pg::physical-file-exclusive-flock-p text inode))))
         (equal before text))))

(deftest generated-offline-owner-flock-evidence-contract
    (let ((result
            (cl-spec:result-data
             (cl-spec:run-property 'offline-owner-flock-evidence-is-exact
                                   :seed 201 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(cl-spec:defproperty fdinfo-parser-preserves-generic-reference
    ((shape (range integer 0 11))
     (inode (range integer 1 18446744073709551615))
     (offset (range integer 0 4096))
     (code (range integer 0 1114111)))
  (:about pg::physical-file-exclusive-flock-p)
  (:trials (:normal 1000))
  (let* ((text (owner-lock-test-fdinfo shape inode))
         (mutated (make-array (length text) :element-type 'character :initial-contents text))
         (character (code-char code)))
    (when (and character (plusp (length mutated)))
      (setf (char mutated (mod offset (length mutated))) character))
    (and (eql (not (null (pg::physical-file-exclusive-flock-generic-p text inode)))
              (not (null (pg::physical-file-exclusive-flock-p text inode))))
         (eql (not (null (pg::physical-file-exclusive-flock-generic-p mutated inode)))
              (not (null (pg::physical-file-exclusive-flock-p mutated inode)))))))

(deftest generated-fdinfo-parser-reference-equivalence
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'fdinfo-parser-preserves-generic-reference :seed 202))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 1000 (getf data :trials)))))

(deftest fdinfo-decoding-error-keeps-owned-octets-after-scratch-expires
    (let* ((expected (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(65 255 66)))
           (condition
             (let ((scratch (make-array 3 :element-type '(unsigned-byte 8) :initial-contents expected)))
               (declare (dynamic-extent scratch))
               (handler-case (pg::physical-file-fdinfo-text scratch 3)
                 (error (condition)
                   (let ((owned (slot-value condition 'array)))
                     (ok (not (eq scratch owned)))
                     condition))))))
      (ok (typep condition 'sb-impl::malformed-ascii))
      (sb-ext:gc :full t)
      (ok (equalp expected (slot-value condition 'array))))
  (let ((octets (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(65 66 67))))
    (ok (string= "ABC" (pg::physical-file-fdinfo-text octets 3)))
    (ok (string= "" (pg::physical-file-fdinfo-text octets 0)))))

(deftest fdinfo-parser-preserves-legacy-number-and-string-inputs
    (dolist (inode '(1 4294967295 4294967296 9223372036854775808 18446744073709551615
                     18446744073709551616 99999999999999999999))
      (let ((text (owner-lock-test-fdinfo 0 inode)))
        (ok (pg::physical-file-exclusive-flock-p text inode))
        (ok (eql (pg::physical-file-exclusive-flock-generic-p text inode)
                 (pg::physical-file-exclusive-flock-p text inode)))))
  (let* ((text (owner-lock-test-fdinfo 0 1))
         (wide (make-array (length text) :element-type 'character :initial-contents text))
         (adjustable (make-array (+ 4 (length text)) :element-type 'character
                                                     :fill-pointer (length text) :initial-element #\x)))
    (replace adjustable text)
    (dolist (value (list text wide adjustable
                         (concatenate 'string (string (code-char 955)) (string #\Newline) text)))
      (ok (pg::physical-file-exclusive-flock-p value 1))
      (ok (eql (pg::physical-file-exclusive-flock-generic-p value 1)
               (pg::physical-file-exclusive-flock-p value 1))))
    (dolist (inode '(1.0 1.0d0 -1 0))
      (ok (eql (pg::physical-file-exclusive-flock-generic-p text inode)
               (pg::physical-file-exclusive-flock-p text inode))))
    (ok (signals (pg::physical-file-exclusive-flock-p text "1") 'type-error))))

(deftest fdinfo-parser-preserves-unicode-zero-spelling
    (dolist (code '(1776 65296 120792))
      (let* ((character (code-char code))
             (text (format nil "ino: ~c~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:~c 0 EOF~%"
                           character character)))
        (ok (eql (pg::physical-file-exclusive-flock-generic-p text 0)
                 (pg::physical-file-exclusive-flock-p text 0))))))

(deftest offline-owner-flock-evidence-rejects-every-invalid-shape
    (loop for shape from 0 to 11
          for text = (owner-lock-test-fdinfo shape 12345)
          do (ok (eql (zerop shape)
                      (not (null (pg::physical-file-exclusive-flock-p text 12345)))))))

(deftest offline-owner-flock-evidence-binds-header-and-whole-file-range
    (let ((row (format nil "lock: 1: FLOCK ADVISORY WRITE 123 08:30:12345 0 EOF~%")))
      (dolist (header (list ""
                            (format nil "ino: 12346~%")
                            (format nil "ino: invalid~%")
                            (format nil "ino: 12345~%ino: 12345~%")))
        (ng (pg::physical-file-exclusive-flock-p (concatenate 'string header row) 12345)))
      (dolist (range '("1 EOF" "0 99"))
        (ng (pg::physical-file-exclusive-flock-p
             (format nil "ino: 12345~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:12345 ~a~%" range)
             12345)))
      (let* ((text (concatenate 'string (format nil "ino: 12345~%") row))
             (padded (concatenate 'string text (make-string (- 4096 (length text)) :initial-element #\x))))
        (ok (pg::physical-file-exclusive-flock-p padded 12345))
        (ng (pg::physical-file-exclusive-flock-p (concatenate 'string padded "x") 12345)))))

(deftest offline-owner-flock-inspection-distinguishes-open-descriptions
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (let ((path (merge-pathnames ".pg_lisp.lock" root)))
        (pg::wal-atomic-write-bytes path (make-array 0 :element-type '(unsigned-byte 8)))
        (pg::call-with-physical-adoption-owner
         root
         (lambda (owner)
           (let ((fd (sb-sys:fd-stream-fd (pg::physical-adoption-owner-lock owner)))
                 (duplicate nil) (separate nil))
             (unwind-protect
                  (progn
                    (sb-sys:without-interrupts
                      (setf duplicate (sb-posix:dup fd)
                            separate (open path :direction :input)))
                    (ok (pg::physical-file-holds-exclusive-flock-p fd))
                    (ok (pg::physical-file-holds-exclusive-flock-p duplicate))
                    (ng (pg::physical-file-holds-exclusive-flock-p (sb-sys:fd-stream-fd separate)))
                    (ok (minusp (pg::physical-store-flock (sb-sys:fd-stream-fd separate) 6)))
                    (ok (pg::physical-file-holds-exclusive-flock-p fd)))
               (when separate (close separate))
               (when duplicate (sb-posix:close duplicate)))
             (ok (eq owner (pg::physical-directory-check-owner-common owner)))))))))

(deftest offline-owner-flock-inspection-failure-permanently-revokes-ownership
    (dolist (mode '(:missing :error))
      (with-physical-store-directory (root)
        (sb-posix:chmod (namestring root) #o700)
        (pg::wal-atomic-write-bytes (merge-pathnames ".pg_lisp.lock" root)
                                    (make-array 0 :element-type '(unsigned-byte 8)))
        (pg::call-with-physical-adoption-owner
         root
         (lambda (owner)
           (let ((original (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
                 (copy (copy-structure owner))
                 (calls 0) (message nil))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                          (lambda (fd)
                            (declare (ignore fd))
                            (incf calls)
                            (when (eq mode :error) (error "injected fdinfo inspection error"))
                            nil))
                    (setf message
                          (handler-case
                              (progn (pg::physical-directory-check-owner-common owner) nil)
                            (error (condition) (princ-to-string condition)))))
               (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) original))
             (ok (and message (search "offline adoption lost its exclusive directory lock" message)))
             (ok (= 1 calls))
             (ng (pg::physical-adoption-owner-active-p owner))
             (ok (signals (pg::physical-directory-check-owner-common owner) 'error))
             (setf (pg::physical-adoption-owner-active-p owner) t)
             (ok (signals (pg::physical-directory-check-owner-common owner) 'error))
             (ng (pg::physical-adoption-owner-active-p owner))
             (ok (signals (pg::physical-directory-check-owner-common copy) 'error))
             (ng (pg::physical-adoption-owner-active-p copy))
             (let ((fd (sb-sys:fd-stream-fd (pg::physical-adoption-owner-lock owner))))
               (unwind-protect
                    (progn
                      (unless (zerop (pg::physical-store-flock fd 8))
                        (error "cannot release the test-owned root lock"))
                      (with-open-file (replacement (merge-pathnames ".pg_lisp.lock" root) :direction :input)
                        (unless (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd replacement) 6))
                          (error "cannot acquire the test-owned replacement lock"))
                        (ok (pg::physical-file-holds-exclusive-flock-p (sb-sys:fd-stream-fd replacement)))
                        (setf (pg::physical-adoption-owner-lock copy) replacement
                              (pg::physical-adoption-owner-active-p copy) t)
                        (ok (signals (pg::physical-directory-check-owner-common copy) 'error))
                        (ng (pg::physical-adoption-owner-active-p copy))))
                 (unless (zerop (pg::physical-store-flock fd 6))
                   (error "cannot restore the test-owned root lock")))))))
        (pg::call-with-physical-adoption-owner
         root (lambda (owner) (ok (eq owner (pg::physical-directory-check-owner-common owner))))))))

(deftest offline-owner-flock-downgrade-requires-a-new-owner
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (pg::wal-atomic-write-bytes (merge-pathnames ".pg_lisp.lock" root)
                                  (make-array 0 :element-type '(unsigned-byte 8)))
      (pg::call-with-physical-adoption-owner
       root
       (lambda (owner)
         (let ((fd (sb-sys:fd-stream-fd (pg::physical-adoption-owner-lock owner)))
               (message nil))
           (unwind-protect
                (progn
                  (ok (zerop (pg::physical-store-flock fd 5)))
                  (ng (pg::physical-file-holds-exclusive-flock-p fd))
                  (setf message
                        (handler-case
                            (progn (pg::physical-directory-check-owner-common owner) nil)
                          (error (condition) (princ-to-string condition))))
                  (ok (and message (search "offline adoption lost its exclusive directory lock" message)))
                  (ng (pg::physical-adoption-owner-active-p owner)))
             (unless (zerop (pg::physical-store-flock fd 6))
               (error "cannot restore the test-owned exclusive lock")))
           (setf (pg::physical-adoption-owner-active-p owner) t)
           (ok (signals (pg::physical-directory-check-owner-common owner) 'error))
           (ng (pg::physical-adoption-owner-active-p owner)))))
      (pg::call-with-physical-adoption-owner
       root (lambda (owner) (ok (eq owner (pg::physical-directory-check-owner-common owner)))))))

(deftest offline-stream-flock-scope-survives-rearm-and-copy
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (let ((path (merge-pathnames "scope.lock" root))
            (owner (list :owner)) (copy (list :copy)))
        (with-open-file (stream path :direction :io :if-exists :error :if-does-not-exist :create
                                     :element-type '(unsigned-byte 8))
          (let ((fd (sb-sys:fd-stream-fd stream)) (contender nil))
            (ok (zerop (pg::physical-store-flock fd 6)))
            (ok (pg::physical-file-check-exclusive-flock stream owner))
            (unwind-protect
                 (progn
                   (ok (zerop (pg::physical-store-flock fd 8)))
                   (sb-sys:without-interrupts (setf contender (open path :direction :input)))
                   (ok (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd contender) 6)))
                   (ok (signals (pg::physical-file-check-exclusive-flock stream owner) 'error)))
              (when contender (close contender))
              (unless (zerop (pg::physical-store-flock fd 6))
                (error "cannot restore the test-owned stream lock")))
            (ok (pg::physical-file-holds-exclusive-flock-p fd))
            (ok (signals (pg::physical-file-check-exclusive-flock stream owner) 'error))
            (ok (signals (pg::physical-file-check-exclusive-flock stream copy) 'error))
            (unwind-protect
                 (progn
                   (unless (zerop (pg::physical-store-flock fd 8))
                     (error "cannot release the test-owned stream lock"))
                   (with-open-file (replacement path :direction :input)
                     (unless (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd replacement) 6))
                       (error "cannot acquire the replacement stream lock"))
                     (ok (pg::physical-file-holds-exclusive-flock-p (sb-sys:fd-stream-fd replacement)))
                     (ok (signals (pg::physical-file-check-exclusive-flock replacement copy) 'error))
                     (ok (pg::physical-file-check-exclusive-flock replacement (list :fresh-owner)))))
              (unless (zerop (pg::physical-store-flock fd 6))
                (error "cannot restore the original stream lock"))))))))

(deftest offline-stream-flock-rechecks-revocation-after-inspection
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames "nested.lock" root))
            (owner (list :owner))
            (original (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
            (calls 0)
            (nested-refused nil))
        (with-open-file (stream path :direction :io :if-exists :error :if-does-not-exist :create
                                     :element-type '(unsigned-byte 8))
          (let ((fd (sb-sys:fd-stream-fd stream)))
            (ok (zerop (pg::physical-store-flock fd 6)))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                         (lambda (borrowed-fd)
                           (declare (ignore borrowed-fd))
                           (incf calls)
                           (when (= calls 1)
                             (handler-case
                                 (pg::physical-file-check-exclusive-flock stream owner)
                               (error () (setf nested-refused t)))
                             t)))
                   (ok (signals (pg::physical-file-check-exclusive-flock stream owner) 'error))
                   (ok nested-refused)
                   (ok (= calls 2)))
              (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) original))
            (ok (funcall original fd))
            (ok (signals (pg::physical-file-check-exclusive-flock stream owner) 'error)))))))

(defun owner-lock-test-call-proof (kind function)
  (with-physical-store-directory (root)
    (ecase kind
      (:directory
       (sb-posix:chmod (namestring root) #o700)
       (pg::wal-atomic-write-bytes (merge-pathnames ".pg_lisp.lock" root)
                                   (make-array 0 :element-type '(unsigned-byte 8)))
       (pg::call-with-physical-adoption-owner
        root
        (lambda (owner)
          (flet ((check () (pg::physical-directory-check-owner-common owner)))
            (funcall function #'check
                     (lambda () (check) (pg::physical-adoption-owner-active-p owner))
                     (sb-sys:fd-stream-fd (pg::physical-adoption-owner-lock owner)))))))
      (:store
       (let ((store (pg::physical-store-open root 123 1 :create t)))
         (unwind-protect
              (flet ((check () (pg::physical-store-check-held-lock store)))
                (funcall function #'check
                         (lambda () (check) (not (pg::physical-store-failed-p store)))
                         (sb-sys:fd-stream-fd (pg::physical-store-lock-stream store))))
           (pg::physical-store-close store))))
      ((:stream :scope)
       (with-open-file (stream (merge-pathnames "interrupted.lock" root)
                               :direction :io :if-exists :error :if-does-not-exist :create
                               :element-type '(unsigned-byte 8))
         (let ((fd (sb-sys:fd-stream-fd stream)) (owner (list :owner)) (active t))
           (unless (zerop (pg::physical-store-flock fd 6))
             (error "cannot acquire the test-owned lock"))
           (flet ((check ()
                    (ecase kind
                      (:stream (pg::physical-file-check-exclusive-flock stream owner))
                      (:scope (pg::physical-reseed-check-directory-flock fd active "test lock lost")))))
             (funcall function #'check (lambda () (check) active) fd))))))))

(defun owner-lock-test-interruption (check healthy fd delivery kind)
  (let ((original (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
        (pg::*backend* (pg::make-backend))
        (sentinel nil) (caught nil) (calls 0) (delivered 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                 (lambda (borrowed-fd)
                   (incf calls)
                   (flet ((deliver ()
                            (incf delivered)
                            (handler-bind ((pg::pg-error (lambda (condition) (setf sentinel condition))))
                              (ecase kind
                                (:termination (setf (pg::backend-termination-pending pg::*backend*) t))
                                (:cancel (setf (pg::backend-cancel-pending pg::*backend*) t))
                                (:timeout (setf (pg::backend-statement-deadline pg::*backend*)
                                                (1- (get-internal-real-time))))
                                (:wal-failure
                                 (setf sentinel (make-condition 'simple-error :format-control "latched WAL failure")
                                       (pg::backend-wal-failure pg::*backend*) sentinel)
                                 (error sentinel)))
                              (pg::check-for-interrupts))))
                     (if (eq delivery :synchronous)
                         (deliver)
                         (sb-thread:interrupt-thread sb-thread:*current-thread* #'deliver)))
                   (funcall original borrowed-fd)))
           (handler-case (funcall check)
             (error (condition) (setf caught condition))))
      (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) original))
    (ok (and sentinel (eq sentinel caught)) "the original backend interruption escapes")
    (unless (eq kind :wal-failure)
      (ok (and (typep caught 'pg::pg-error)
               (string= (if (eq kind :termination) "57P01" "57014") (pg::pg-error-sqlstate caught))))
      (ok (and (typep caught 'pg::pg-error)
               (string= (if (eq kind :termination) "FATAL" "ERROR") (pg::pg-error-severity caught)))))
    (ok (= 1 calls delivered))
    (ok (null (pg::backend-termination-pending pg::*backend*)))
    (ok (null (pg::backend-cancel-pending pg::*backend*)))
    (ok (null (pg::backend-statement-deadline pg::*backend*)))
    (ok (funcall original fd) "the descriptor remains exclusively locked")
    (ok (handler-case (funcall healthy) (error () nil))
        "an interrupted successful inspection does not revoke ownership")))

(deftest offline-stream-flock-preserves-backend-interruptions-without-revocation
    (dolist (proof '(:stream :directory :store :scope))
      (dolist (delivery '(:synchronous :asynchronous))
        (dolist (kind '(:termination :cancel :timeout :wal-failure))
          (owner-lock-test-call-proof
           proof (lambda (check healthy fd)
                   (owner-lock-test-interruption check healthy fd delivery kind)))))))

(deftest offline-directory-owner-rechecks-revocation-after-inspection
    (dolist (nested-copy '(nil t))
      (with-physical-store-directory (root)
        (sb-posix:chmod (namestring root) #o700)
        (pg::wal-atomic-write-bytes (merge-pathnames ".pg_lisp.lock" root)
                                    (make-array 0 :element-type '(unsigned-byte 8)))
        (pg::call-with-physical-adoption-owner
         root
         (lambda (owner)
           (let ((original (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
                 (nested (if nested-copy (copy-structure owner) owner))
                 (calls 0) (refused nil) (message nil))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                          (lambda (fd)
                            (declare (ignore fd))
                            (incf calls)
                            (if (= 1 calls)
                                (progn
                                  (handler-case (pg::physical-directory-check-owner-common nested)
                                    (error () (setf refused t)))
                                  t)
                                nil)))
                    (setf message
                          (handler-case (progn (pg::physical-directory-check-owner-common owner) nil)
                            (error (condition) (princ-to-string condition)))))
               (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) original))
             (ok refused)
             (ok (= 2 calls))
             (ok (and message (search "lost its exclusive directory lock" message)))
             (ok (not (pg::physical-adoption-owner-active-p owner)))
             (ok (signals (pg::physical-directory-check-owner-common owner) 'error))))))))

(deftest offline-lock-loss-is-latched-before-a-queued-interruption
    (dolist (proof '(:stream :directory :store :scope))
      (dolist (loss '(:missing :error :unlocked))
        (owner-lock-test-call-proof
         proof
         (lambda (check healthy fd)
           (let ((original (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
                 (pg::*backend* (pg::make-backend))
                 (sentinel nil) (caught nil) (queued 0) (delivered 0) (calls 0))
             (unwind-protect
                  (handler-case
                      (sb-sys:without-interrupts
                        (when (eq loss :unlocked)
                          (unless (zerop (pg::physical-store-flock fd 8))
                            (error "cannot release the test-owned lock")))
                        (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                              (lambda (borrowed-fd)
                                (incf calls) (incf queued)
                                (sb-thread:interrupt-thread
                                 sb-thread:*current-thread*
                                 (lambda ()
                                   (incf delivered)
                                   (handler-bind ((pg::pg-error
                                                    (lambda (condition) (setf sentinel condition))))
                                     (setf (pg::backend-termination-pending pg::*backend*) t)
                                     (pg::check-for-interrupts))))
                                (ecase loss
                                  (:missing nil)
                                  (:error (error "injected fdinfo read failure"))
                                  (:unlocked (funcall original borrowed-fd)))))
                        (funcall check))
                    (error (condition) (setf caught condition)))
               (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) original)
               (when (eq loss :unlocked)
                 (unless (zerop (pg::physical-store-flock fd 6))
                   (error "cannot restore the test-owned lock"))))
             (ok (and sentinel (eq sentinel caught)))
             (ok (and (typep caught 'pg::pg-error) (string= "57P01" (pg::pg-error-sqlstate caught))))
             (ok (= 1 calls queued delivered))
             (ok (null (pg::backend-termination-pending pg::*backend*)))
             (ok (funcall original fd))
             (ok (not (handler-case (funcall healthy) (error () nil)))
                 "a completed failed observation stays revoked after rearming the lock")))))))

(deftest physical-store-owned-lock-check-permanently-revokes-missing-stream
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t))
             (stream (pg::physical-store-lock-stream store)))
        (unwind-protect
             (progn
               (ok (pg::physical-store-check-held-lock store))
               (let ((candidate (pg::physical-follow-copy-replay-candidate store)))
                 (ok (eq (pg::physical-store-directory store)
                         (pg::physical-store-directory candidate)))
                 (ok (null (pg::physical-store-lock-stream candidate)))
                 (ok (progn (pg::physical-store-check-live candidate) t))
                 (ok (signals (pg::physical-store-check-held-lock candidate) 'error))
                 (ok (pg::physical-store-check-held-lock store)))
               (setf (pg::physical-store-lock-stream store) nil)
               (ok (signals (pg::physical-store-check-held-lock store) 'error))
               (ok (pg::physical-store-failed-p store))
               (setf (pg::physical-store-lock-stream store) stream
                     (pg::physical-store-failed-p store) nil)
               (ok (pg::physical-file-holds-exclusive-flock-p (sb-sys:fd-stream-fd stream)))
               (ok (signals (pg::physical-store-check-held-lock store) 'error))
               (ok (pg::physical-store-failed-p store)))
          (setf (pg::physical-store-lock-stream store) stream)
          (pg::physical-store-close store)))))

(deftest offline-stream-flock-loss-revokes-stream-after-owner-revocation
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames "revoked-owner.lock" root))
            (owner (list :owner))
            (original (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
            (calls 0))
        (with-open-file (stream path :direction :io :if-exists :error :if-does-not-exist :create
                                     :element-type '(unsigned-byte 8))
          (let ((fd (sb-sys:fd-stream-fd stream)))
            (ok (zerop (pg::physical-store-flock fd 6)))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                         (lambda (borrowed-fd)
                           (declare (ignore borrowed-fd))
                           (incf calls)
                           (pg::physical-file-revoke-flock nil owner)
                           nil))
                   (ok (signals (pg::physical-file-check-exclusive-flock stream owner) 'error)))
              (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) original))
            (ok (= calls 1))
            (ok (funcall original fd))
            (ok (signals (pg::physical-file-check-exclusive-flock stream (list :fresh-owner)) 'error)))))))

(deftest physical-store-owned-lock-check-revokes-every-inspection-failure
    (dolist (kind '(:non-stream :closed-stream :named-inode :stream-swap :failed-entry :closed-entry))
      (testing (symbol-name kind)
               (with-physical-store-directory (root)
                 (let* ((store (pg::physical-store-open root 123 1 :create t))
                        (stream (pg::physical-store-lock-stream store))
                        (path (merge-pathnames "receive.lock" root))
                        (saved (merge-pathnames "receive.lock.saved" root))
                        (replacement (open path :direction :input))
                        (probe (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
                        (moved nil) (calls 0) (failure nil))
                   (unwind-protect
                        (progn
                          (case kind
                            (:non-stream (setf (pg::physical-store-lock-stream store) :invalid))
                            (:closed-stream
                             (close stream)
                             (unless (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd replacement) 6))
                               (error "cannot acquire the test replacement lock")))
                            (:named-inode
                             (sb-sys:without-interrupts
                               (sb-posix:rename (namestring path) (namestring saved))
                               (setf moved t))
                             (with-open-file (out path :direction :output :if-exists :error
                                                       :if-does-not-exist :create)
                               (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)))
                            (:stream-swap
                             (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                                   (lambda (fd)
                                     (incf calls)
                                     (setf (pg::physical-store-lock-stream store) replacement)
                                     (funcall probe fd))))
                            (:failed-entry (setf (pg::physical-store-failed-p store) t))
                            (:closed-entry (setf (pg::physical-store-closed-p store) t)))
                          (handler-case (pg::physical-store-check-held-lock store)
                            (error (condition) (setf failure condition)))
                          (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) probe)
                          (when moved
                            (delete-file path)
                            (sb-posix:rename (namestring saved) (namestring path))
                            (setf moved nil))
                          (ok failure)
                          (ok (pg::physical-store-failed-p store))
                          (when (eq kind :stream-swap)
                            (ok (= calls 1))
                            (ok (search "ownership changed during lock validation" (princ-to-string failure))))
                          (when (eq kind :named-inode)
                            (ok (search "ownership inode changed" (princ-to-string failure))))
                          (setf (pg::physical-store-lock-stream store)
                                (if (open-stream-p stream) stream replacement)
                                (pg::physical-store-failed-p store) nil
                                (pg::physical-store-closed-p store) nil)
                          (ok (funcall probe
                                       (sb-sys:fd-stream-fd (pg::physical-store-lock-stream store))))
                          (ok (signals (pg::physical-store-check-held-lock store) 'error))
                          (ok (pg::physical-store-failed-p store)))
                     (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) probe)
                     (when moved
                       (when (probe-file path) (delete-file path))
                       (sb-posix:rename (namestring saved) (namestring path)))
                     (setf (pg::physical-store-lock-stream store)
                           (if (open-stream-p stream) stream replacement)
                           (pg::physical-store-closed-p store) nil)
                     (pg::physical-store-close store)
                     (when (open-stream-p replacement) (close replacement))))))))

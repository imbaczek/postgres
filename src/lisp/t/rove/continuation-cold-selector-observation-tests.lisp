(in-package #:pg)

(declaim (ftype function call-with-logical-slot-continuation-cold-selector-files))

(defun unit-continuation-selector-observation-fixture (function)
  (unit-continuation-directory-fixture
   (lambda (root pathname)
     (sb-posix:mkdir (namestring (merge-pathnames "selection/" pathname)) #o700)
     (call-with-logical-slot-copy-source-proof-fd
      root "selection" t
      (lambda (directory facts)
        (declare (ignore facts))
        (funcall function directory))
      :mutable-directory t))))

(cl-spec:defproperty continuation-selector-absence-keeps-bootstrap-temporary-unselected
    ((cut (range integer 0 1024)))
  (:about call-with-logical-slot-continuation-cold-selector-files)
  (:trials (:normal 40))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
    (let* ((snapshot (logical-slot-continuation-active-child-snapshot
                      (string->bytes "binding") certificate host rows))
           (bytes (logical-slot-continuation-encode snapshot))
           (prefix (subseq bytes 0 (min cut (length bytes)))))
      (unit-continuation-selector-observation-fixture
       (lambda (directory)
         (let ((path (unit-continuation-cleanup-write-file directory "selector.tmp" prefix)))
           (and
            (call-with-logical-slot-continuation-cold-selector-files
             directory (lambda () t)
             (lambda (kind model selected temporary check)
               (and (eq :absent kind) (null model) (null selected)
                    (equalp prefix (third temporary)) (funcall check))))
            (equalp prefix (read-bounded-regular-file path (length bytes))))))))))

(rove:deftest continuation-selector-absence-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-selector-absence-keeps-bootstrap-temporary-unselected
                                         :seed 11915 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 40 (getf result :trials)))))

(rove:deftest continuation-selector-observation-keeps-stable-and-prepared-files
    (dolist (prepared '(nil t))
      (unit-continuation-selector-observation-fixture
       (lambda (directory)
         (let* ((record (unit-continuation-prepared))
                (model (if prepared record (getf record :old)))
                (image (if prepared (logical-slot-continuation-prepared-encode model)
                           (logical-slot-continuation-encode model)))
                (selector (unit-continuation-cleanup-write-file directory "selector" image))
                (temporary (unit-continuation-cleanup-write-file directory "selector.tmp" #(1 2)))
                (fds (length (directory #p"/proc/self/fd/*"))))
           (call-with-logical-slot-continuation-cold-selector-files
            directory (lambda () t)
            (lambda (kind observed selected scratch check)
              (rove:ok (eq (if prepared :prepared :stable) kind))
              (rove:ok (equalp model observed))
              (rove:ok (equalp image (third selected)))
              (rove:ok (equalp #(1 2) (third scratch)))
              (setf (aref (third selected) 0) 0
                    (aref (third scratch) 0) 0)
              (rove:ok (funcall check))))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok (equalp image (read-bounded-regular-file selector (length image))))
           (rove:ok (equalp #(1 2) (read-bounded-regular-file temporary 2))))))))

(rove:deftest continuation-selector-observation-rejects-expired-and-cross-thread-checks
    (unit-continuation-selector-observation-fixture
     (lambda (directory)
       (let ((escaped nil) (guards 0))
         (call-with-logical-slot-continuation-cold-selector-files
          directory (lambda () (incf guards))
          (lambda (kind model selected temporary check)
            (declare (ignore model selected temporary))
            (rove:ok (eq :absent kind))
            (setf escaped check)
            (let* ((before guards)
                   (thread (sb-thread:make-thread
                            (lambda ()
                              (handler-case (progn (funcall check) nil) (error () t))))))
              (rove:ok (sb-thread:join-thread thread))
              (rove:ok (= before guards)))
            (rove:ok (funcall check))))
         (let ((before guards))
           (rove:ok (handler-case (progn (funcall escaped) nil) (error () t)))
           (rove:ok (= before guards)))))))

(rove:deftest continuation-selector-observation-latches-failed-absence
    (unit-continuation-selector-observation-fixture
     (lambda (directory)
       (let ((guards 0) (escaped nil))
         (rove:ok
          (handler-case
              (progn
                (call-with-logical-slot-continuation-cold-selector-files
                 directory (lambda () (incf guards))
                 (lambda (kind model selected temporary check)
                   (declare (ignore kind model selected temporary))
                   (setf escaped check)
                   (let ((path (unit-continuation-cleanup-write-file directory "selector" #(0))))
                     (rove:ok (handler-case (progn (funcall check) nil) (error () t)))
                     (sb-posix:unlink (namestring path)))
                   (let ((before guards))
                     (rove:ok (handler-case (progn (funcall check) nil) (error () t)))
                     (rove:ok (= before guards)))))
                nil)
            (error () t)))
         (let ((before guards))
           (rove:ok (handler-case (progn (funcall escaped) nil) (error () t)))
           (rove:ok (= before guards)))))))

(rove:deftest continuation-selector-observation-refuses-replaced-and-changed-files
    (dolist (mode '(:selected-bytes :selected-inode :temporary-bytes :temporary-inode))
      (unit-continuation-selector-observation-fixture
       (lambda (directory)
         (let* ((record (unit-continuation-prepared))
                (image (logical-slot-continuation-encode (getf record :old)))
                (selected (unit-continuation-cleanup-write-file directory "selector" image))
                (temporary (unit-continuation-cleanup-write-file directory "selector.tmp" #(1 2)))
                (fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok
            (handler-case
                (progn
                  (call-with-logical-slot-continuation-cold-selector-files
                   directory (lambda () t)
                   (lambda (kind model selector scratch check)
                     (declare (ignore kind model selector scratch))
                     (let ((path (if (member mode '(:selected-bytes :selected-inode)) selected temporary)))
                       (if (member mode '(:selected-inode :temporary-inode))
                           (let ((replacement
                                   (unit-continuation-cleanup-write-file
                                    directory "replacement"
                                    (if (eq mode :selected-inode) image #(1 2)))))
                             (sb-posix:rename (namestring replacement) (namestring path)))
                           (with-open-file (stream path :direction :output :if-exists :overwrite
                                                        :element-type '(unsigned-byte 8))
                             (write-byte 0 stream))))
                     (funcall check)))
                  nil)
              (error () t))
            (format nil "reject change ~s" mode))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*")))))))))

(rove:deftest continuation-selector-observation-refuses-links-and-unclassified-names
    (dolist (mode '(:symlink :hardlink :extra))
      (unit-continuation-selector-observation-fixture
       (lambda (directory)
         (let* ((root (physical-image-fd-path directory))
                (target (unit-continuation-cleanup-write-file directory
                                                              (if (eq mode :extra) "extra" "selector.tmp") #(1 2)))
                (selector (merge-pathnames "selector" root))
                (called nil) (fds (length (directory #p"/proc/self/fd/*"))))
           (ecase mode
             (:symlink (sb-posix:symlink (namestring target) (namestring selector)))
             (:hardlink (sb-posix:link (namestring target) (namestring selector)))
             (:extra nil))
           (rove:ok (handler-case
                        (progn (call-with-logical-slot-continuation-cold-selector-files
                                directory (lambda () t) (lambda (&rest values)
                                                          (declare (ignore values)) (setf called t)))
                               nil)
                      (error () t)))
           (rove:ok (not called))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*")))))))))

(rove:deftest continuation-selector-observation-expires-after-nonlocal-exit
    (unit-continuation-selector-observation-fixture
     (lambda (directory)
       (let ((escaped nil) (guards 0) (fds (length (directory #p"/proc/self/fd/*"))))
         (rove:ok
          (eq :gone
              (catch 'selection-done
                (call-with-logical-slot-continuation-cold-selector-files
                 directory (lambda () (incf guards))
                 (lambda (kind model selected temporary check)
                   (declare (ignore kind model selected temporary))
                   (setf escaped check)
                   (throw 'selection-done :gone))))))
         (let ((before guards))
           (rove:ok (handler-case (progn (funcall escaped) nil) (error () t)))
           (rove:ok (= before guards)))
         (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))))))

(rove:deftest continuation-selector-observation-can-require-a-canonical-selector
    (unit-continuation-selector-observation-fixture
     (lambda (directory)
       (let ((called nil))
         (unit-continuation-cleanup-write-file directory "selector.tmp" #(1 2))
         (rove:ok (handler-case
                      (progn (call-with-logical-slot-continuation-cold-selector-files
                              directory (lambda () t) (lambda (&rest values)
                                                        (declare (ignore values)) (setf called t))
                              :require-selected t)
                             nil)
                    (error () t)))
         (rove:ok (not called))))))

(rove:deftest continuation-selector-observation-latches-a-nonlocal-guard-exit
    (unit-continuation-selector-observation-fixture
     (lambda (directory)
       (let ((throwing nil) (guards 0))
         (rove:ok
          (handler-case
              (progn
                (call-with-logical-slot-continuation-cold-selector-files
                 directory
                 (lambda () (incf guards) (when throwing (throw 'guard-exit :left)))
                 (lambda (kind model selected temporary check)
                   (declare (ignore kind model selected temporary))
                   (setf throwing t)
                   (rove:ok (eq :left (catch 'guard-exit (funcall check))))
                   (setf throwing nil)
                   (let ((before guards))
                     (rove:ok (handler-case (progn (funcall check) nil) (error () t)))
                     (rove:ok (= before guards)))))
                nil)
            (error () t)))))))

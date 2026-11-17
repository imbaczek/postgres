(require :asdf)
(let ((source (truename (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*)))))
  (pushnew source asdf:*central-registry* :test #'equal)
  (asdf:load-asd (merge-pathnames "pg.asd" source))
  (asdf:load-system "pg")
  (asdf:load-system "rove"))

(in-package #:pg)

(defun native-imported-main-with-root (function)
  (call-with-physical-adoption-owner
   *pgdata*
   (lambda (owner)
     (call-with-physical-directory-root
      owner
      (lambda (root path)
        (declare (ignore path))
        (let ((mount (physical-adoption-install-mount-id nil :directory-fd root)))
          (flet ((check ()
                   (physical-directory-check-owner owner)
                   (physical-image-check-target *pgdata* root)
                   (assert (= mount
                              (physical-adoption-install-mount-id *pgdata*)
                              (physical-adoption-install-mount-id nil :directory-fd root)
                              (physical-adoption-install-mount-id
                               nil :directory-fd
                               (sb-sys:fd-stream-fd (physical-adoption-owner-lock owner)))))
                   t))
            (check)
            (multiple-value-prog1 (funcall function root #'check)
              (check)))))))))

(rove:deftest imported-main-pair-observer-is-read-only-and-scoped-before-wal
    (let* ((started (get-internal-real-time))
           (*pgdata* (uiop:ensure-directory-pathname
                      (or (uiop:getenv "PG_NATIVE_PROOF_DATA")
                          (error "Set PG_NATIVE_PROOF_DATA to the owned certified fixture"))))
           (fds (length (directory #p"/proc/self/fd/*")))
           (saved-check nil)
           (called 0))
      (when (boundp 'cl-user::*native-imported-main-bootstrap-start*)
        (format t "~&IMPORTED-MAIN-OBSERVATION-PHASE setup wall=~,6f~%"
                (/ (- started (symbol-value 'cl-user::*native-imported-main-bootstrap-start*))
                   internal-time-units-per-second)))
      (rove:ok (not *wal-ready*))
      (rove:ok (zerop (hash-table-count *wal-replication-slots*)))
      (native-imported-main-with-root
       (lambda (root root-check)
         (rove:ok
          (equal
           '(:observed 17)
           (multiple-value-list
            (call-with-logical-slot-imported-startup-pair
             root root-check
             (lambda (image facts check)
               (incf called)
               (setf saved-check check)
               (rove:ok (equalp image
                                (read-bounded-regular-file
                                 (merge-pathnames "logical-slot-copy.source-proof" *pgdata*)
                                 (logical-slot-copy-source-proof-maximum-bytes))))
               (rove:ok (eql (sb-posix:stat-ino (sb-posix:fstat root))
                             (getf (getf facts :certificate) :inode)))
               (setf (getf (getf facts :certificate) :inode) 0
                     (aref image 0) (logxor #xff (aref image 0)))
               (rove:ok (funcall check))
               (let ((rejected nil))
                 (sb-thread:join-thread
                  (sb-thread:make-thread
                   (lambda ()
                     (setf rejected
                           (handler-case (progn (funcall check) nil) (error () t))))))
                 (rove:ok rejected))
               (values :observed 17))))))
         (rove:ok (rove:signals (funcall saved-check) 'error))
         (rove:ok
          (rove:signals
           (call-with-logical-slot-imported-startup-pair
            root root-check
            (lambda (image facts check)
              (declare (ignore image facts))
              (incf called)
              (setf saved-check check)
              (error "deliberate imported observation callback failure")))
           'error))
         (rove:ok (rove:signals (funcall saved-check) 'error))
         (let ((reject nil))
           (rove:ok
            (rove:signals
             (call-with-logical-slot-imported-startup-pair
              root
              (lambda ()
                (funcall root-check)
                (when reject (error "deliberate root-check failure")))
              (lambda (image facts check)
                (declare (ignore image facts))
                (incf called)
                (setf reject t)
                (rove:ok (rove:signals (funcall check) 'error))
                (setf reject nil)
                (rove:ok (rove:signals (funcall check) 'error))))
             'error)))
         (let ((directory *pgdata*))
           (rove:ok
            (rove:signals
             (call-with-logical-slot-imported-startup-pair
              root
              (lambda () (let ((*pgdata* directory)) (funcall root-check)))
              (lambda (image facts check)
                (declare (ignore image facts))
                (incf called)
                (let ((*pgdata* (merge-pathnames "missing-startup-root/" directory)))
                  (rove:ok (rove:signals (funcall check) 'error)))
                (rove:ok (rove:signals (funcall check) 'error))))
             'error)))))
      (rove:ok (= called 4))
      (rove:ok (not *wal-ready*))
      (rove:ok (zerop (hash-table-count *wal-replication-slots*)))
      (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
      (format t "~&IMPORTED-MAIN-OBSERVATION-PHASE execution wall=~,6f~%"
              (/ (- (get-internal-real-time) started) internal-time-units-per-second))))

(unless (rove:run-test 'imported-main-pair-observer-is-read-only-and-scoped-before-wal)
  (error "imported main observation tests failed"))
(format t "~&IMPORTED-MAIN-OBSERVATION PASS~%")

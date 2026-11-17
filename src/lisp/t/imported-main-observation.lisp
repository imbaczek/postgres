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

(defun native-imported-main-with-history-read-fault (name enabled function)
  (let ((original (symbol-function 'logical-slot-copy-import-control-read-fd))
        (injected 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'logical-slot-copy-import-control-read-fd)
                 (lambda (fd maximum)
                   (let ((bytes (funcall original fd maximum)))
                     (if (and (funcall enabled)
                              (equal name
                                     (file-namestring
                                      (sb-posix:readlink (format nil "/proc/self/fd/~d" fd)))))
                         (let ((changed (copy-seq bytes)))
                           (incf injected)
                           (setf (aref changed 0) (logxor #xff (aref changed 0)))
                           changed)
                         bytes))))
           (funcall function (lambda () injected)))
      (setf (symbol-function 'logical-slot-copy-import-control-read-fd) original))))

(defun native-imported-main-history-contract (root root-check)
  (let ((saved-check nil)
        (expected (or (uiop:getenv "PG_NATIVE_IMPORTED_HISTORY_PROFILE") "original")))
    (unless (member expected '("original" "retired") :test #'equal)
      (error "unknown native imported history profile"))
    (call-with-logical-slot-imported-startup-pair
     root root-check
     (lambda (image facts pair-check)
       (declare (ignore facts))
       (let ((names
               (append '("logical-slot-copy.descriptor" "logical-slot-copy.base-verified"
                         "logical-slot-copy.evidence")
                       (when (equal expected "original")
                         '("physical-backup.manifest" "physical-recovery.control" "recovery.required")))))
         (rove:ok
          (eq :history
              (call-with-logical-slot-imported-startup-history
               root image pair-check
               (lambda (profile snapshot check)
                 (rove:ok (eq profile (if (equal expected "original") :original :retired)))
                 (rove:ok (equal names (mapcar #'first snapshot)))
                 (setf saved-check check)
                 (let* ((name (caar snapshot)) (initial (char name 0)))
                   (unwind-protect
                        (progn
                          (setf (char name 0) #\x)
                          (rove:ok (funcall check)))
                     (setf (char name 0) initial)))
                 (setf (first (second (first snapshot))) 0
                       (aref (third (first snapshot)) 0)
                       (logxor #xff (aref (third (first snapshot)) 0)))
                 (rove:ok (funcall check))
                 (let ((rejected nil))
                   (sb-thread:join-thread
                    (sb-thread:make-thread
                     (lambda ()
                       (setf rejected
                             (handler-case (progn (funcall check) nil) (error () t))))))
                   (rove:ok rejected))
                 :history))))
         (rove:ok (rove:signals (funcall saved-check) 'error))
         (rove:ok
          (rove:signals
           (call-with-logical-slot-imported-startup-history
            root image pair-check
            (lambda (profile snapshot check)
              (declare (ignore profile snapshot))
              (setf saved-check check)
              (error "deliberate historical observation callback failure")))
           'error))
         (rove:ok (rove:signals (funcall saved-check) 'error))
         (dolist (name names)
           (native-imported-main-with-history-read-fault
            name (constantly t)
            (lambda (injected)
              (let ((called nil))
                (rove:ok
                 (rove:signals
                  (call-with-logical-slot-imported-startup-history
                   root image pair-check
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (setf called t)))
                  'error))
                (rove:ok (not called))
                (rove:ok (= 1 (funcall injected)))))))
         (let ((enabled nil))
           (native-imported-main-with-history-read-fault
            (first names) (lambda () enabled)
            (lambda (injected)
              (rove:ok
               (rove:signals
                (call-with-logical-slot-imported-startup-history
                 root image pair-check
                 (lambda (profile snapshot check)
                   (declare (ignore profile snapshot))
                   (setf enabled t)
                   (rove:ok (rove:signals (funcall check) 'error))
                   (setf enabled nil)
                   (rove:ok (rove:signals (funcall check) 'error))))
                'error))
              (rove:ok (= 1 (funcall injected)))))))))))

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
         (native-imported-main-history-contract root root-check)
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

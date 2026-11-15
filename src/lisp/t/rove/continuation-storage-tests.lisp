(in-package #:pg)

(rove:deftest continuation-directory-requires-real-startup-authority
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (let ((path (merge-pathnames "pg_logical_continuation/" *pgdata*)))
         (rove:ok (rove:signals
                   (logical-slot-continuation-prepare-startup-directory nil nil) 'error))
         (rove:ok (not (probe-file path)))))))

(rove:deftest continuation-directory-is-private-durable-and-reusable
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (call-with-logical-slot-owned-startup-test
        (lambda (gate ownership)
          (let* ((path (merge-pathnames "pg_logical_continuation/" *pgdata*))
                 (before (length (directory #p"/proc/self/fd/*"))))
            (rove:ok (logical-slot-continuation-prepare-startup-directory gate ownership))
            (let ((facts (physical-image-file-facts -100 (namestring path))))
              (rove:ok (= #o700 (logand #o7777 (third facts))))
              (rove:ok (= (sb-posix:geteuid) (fifth facts)))
              (with-open-file (stream (merge-pathnames "unclassified" path)
                                      :direction :output :if-exists :error)
                (write-line "preserve until selector recovery classifies this" stream))
              (rove:ok (logical-slot-continuation-prepare-startup-directory gate ownership))
              (rove:ok (probe-file (merge-pathnames "unclassified" path)))
              (rove:ok
               (equal (logical-slot-copy-source-proof-directory-identity facts)
                      (logical-slot-copy-source-proof-directory-identity
                       (physical-image-file-facts -100 (namestring path))))))
            (rove:ok (= before (length (directory #p"/proc/self/fd/*"))))))))))

(rove:deftest continuation-directory-rejects-unsafe-existing-entry
    (dolist (kind '(:file :symlink :mode))
      (call-with-logical-slot-real-startup-test
       (lambda ()
         (call-with-logical-slot-owned-startup-test
          (lambda (gate ownership)
            (let* ((path (merge-pathnames "pg_logical_continuation" *pgdata*))
                   (name (namestring path))
                   (before (length (directory #p"/proc/self/fd/*"))))
              (ecase kind
                (:file (with-open-file (stream path :direction :output) (write-line "keep" stream)))
                (:symlink (sb-posix:symlink "missing-continuation-directory" name))
                (:mode (sb-posix:mkdir name #o755) (sb-posix:chmod name #o755)))
              (rove:ok (rove:signals
                        (logical-slot-continuation-prepare-startup-directory gate ownership) 'error))
              (rove:ok (slot-state-lstat path))
              (rove:ok (= before (length (directory #p"/proc/self/fd/*")))))))))))

(rove:deftest continuation-storage-refuses-forged-and-expired-publishers
    (rove:ok (rove:signals (logical-slot-continuation-storage-check nil) 'error))
  (let* ((called nil)
         (fake (%make-logical-slot-continuation-startup-storage
                :state (lambda (&rest arguments)
                         (declare (ignore arguments))
                         (setf called t)))))
    (rove:ok (rove:signals (logical-slot-continuation-storage-check fake) 'error))
    (rove:ok (not called))))

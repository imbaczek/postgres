(in-package #:pg-physical-tests)

(defun backup-lease-allocation-store-test-write (record)
  (pg::with-write-lock
    (pg::with-wal-publication-fence (begin-publication)
      (begin-publication)
      (pg::physical-backup-lease-allocation-write record))))

(deftest backup-lease-allocation-store-roundtrip-is-private-and-separate
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((record (backup-lease-allocation-test-record))
              (path (pg::physical-backup-lease-allocation-path record)))
         (ok (equal record (backup-lease-allocation-store-test-write record)))
         (ok (equal record (pg::physical-backup-lease-allocation-read path :stabilize t)))
         (ok (= #o600 (logand #o777 (sb-posix:stat-mode (sb-posix:lstat (namestring path))))))
         (ok (= #o700 (logand #o777 (sb-posix:stat-mode
                                     (sb-posix:lstat
                                      (namestring (pg::physical-backup-lease-allocation-directory)))))))
         (multiple-value-bind (paths temporaries) (pg::physical-backup-lease-allocation-inventory)
           (ok (equal (list path) paths))
           (ok (null temporaries)))
         (ok (not (probe-file (pg::physical-backup-lease-directory))))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-records*)))
         (let ((canceled (pg::physical-backup-lease-allocation-copy record)))
           (setf (getf canceled :state) :canceled)
           (backup-lease-allocation-store-test-write canceled)
           (ok (equal canceled (pg::physical-backup-lease-allocation-read path))))))))

(deftest backup-lease-allocation-store-rejects-rebound-paths
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((record (backup-lease-allocation-test-record))
              (path (pg::physical-backup-lease-allocation-path record))
              (other (backup-lease-allocation-test-record :live 2)))
         (backup-lease-allocation-store-test-write record)
         (ok (equal record (pg::physical-backup-lease-allocation-read path)))
         (sb-posix:rename (namestring path)
                          (namestring (pg::physical-backup-lease-allocation-path other)))
         (ok (intake-fails-p
              (lambda () (pg::physical-backup-lease-allocation-read
                          (pg::physical-backup-lease-allocation-path other)))))))))

(deftest backup-lease-allocation-store-rejects-a-matching-basename-outside-its-directory
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (let* ((record (backup-lease-allocation-test-record))
              (path (pg::physical-backup-lease-allocation-path record))
              (foreign (merge-pathnames (file-namestring path) root)))
         (backup-lease-allocation-store-test-write record)
         (ok (equal record (pg::physical-backup-lease-allocation-read path)))
         (sb-posix:rename (namestring path) (namestring foreign))
         (ok (intake-fails-p
              (lambda () (pg::physical-backup-lease-allocation-read foreign :stabilize t))))
         (ok (equalp (pg::physical-backup-lease-allocation-image record)
                     (lineage-test-bytes foreign)))))))

(deftest backup-lease-allocation-store-rejects-unsafe-records
    (dolist (kind '(:symlink :hardlink :fifo :oversized :public :directory))
      (call-with-backup-lease-store-fixture
       (lambda (root)
         (let* ((record (backup-lease-allocation-test-record))
                (path (pg::physical-backup-lease-allocation-path record))
                (saved (merge-pathnames "saved-allocation" root)))
           (backup-lease-allocation-store-test-write record)
           (ok (equal record (pg::physical-backup-lease-allocation-read path)))
           (sb-posix:rename (namestring path) (namestring saved))
           (let ((before (lineage-test-bytes saved)))
             (ecase kind
               (:symlink (sb-posix:symlink (namestring saved) (namestring path)))
               (:hardlink (sb-posix:link (namestring saved) (namestring path)))
               (:fifo (sb-posix:mkfifo (namestring path) #o600))
               (:oversized
                (with-open-file (stream path :direction :output :if-exists :error)
                  (write-string (make-string 8193 :initial-element #\x) stream)))
               (:public
                (with-open-file (stream path :direction :output :if-exists :error
                                             :element-type '(unsigned-byte 8))
                  (write-sequence before stream))
                (sb-posix:chmod (namestring path) #o644))
               (:directory (sb-posix:mkdir (namestring path) #o700)))
             (ok (intake-fails-p (lambda () (pg::physical-backup-lease-allocation-read path)))
                 (format nil "~s" kind))
             (ok (equalp before (lineage-test-bytes saved)))))))))

(deftest backup-lease-allocation-store-inventory-is-canonical-and-bounded
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((record (backup-lease-allocation-test-record))
              (temporary (pg::physical-backup-lease-allocation-path record t))
              (directory (pg::physical-backup-lease-allocation-directory)))
         (backup-lease-allocation-store-test-write record)
         (with-open-file (stream temporary :direction :output :if-exists :error)
           (declare (ignore stream)))
         (sb-posix:chmod (namestring temporary) #o600)
         (multiple-value-bind (paths temporaries) (pg::physical-backup-lease-allocation-inventory)
           (ok (= 1 (length paths)))
           (ok (equal (list temporary) temporaries)))
         (let ((pg::*physical-backup-lease-allocation-limit* 0))
           (ok (intake-fails-p #'pg::physical-backup-lease-allocation-inventory)))
         (dolist (bad '(".hidden" "unknown.alloc" "reseed_00000000000000000000000000000000.1.alloc"
                        "reseed_00000000000000000000000000000000.00.alloc"
                        "reseed_00000000000000000000000000000000.17.alloc"
                        "reseed_00000000000000000000000000000000.01.extra"))
           (let ((path (merge-pathnames bad directory)))
             (with-open-file (stream path :direction :output :if-exists :error)
               (declare (ignore stream)))
             (ok (intake-fails-p #'pg::physical-backup-lease-allocation-inventory) bad)
             (delete-file path)))
         (ok (probe-file temporary))))))

(deftest backup-lease-allocation-store-faults-preserve-complete-records
    (dolist (point '(:before-write :after-write :after-file-fsync :after-rename :after-directory-fsync))
      (call-with-backup-lease-store-fixture
       (lambda (root)
         (declare (ignore root))
         (let* ((record (backup-lease-allocation-test-record))
                (path (pg::physical-backup-lease-allocation-path record))
                (seen nil)
                (pg::*physical-backup-lease-allocation-test-hook*
                  (lambda (actual value)
                    (declare (ignore value))
                    (when (eq point actual)
                      (setf seen t)
                      (error "allocation publication cut")))))
           (ok (intake-fails-p (lambda () (backup-lease-allocation-store-test-write record))))
           (ok seen)
           (ok pg::*wal-poisoned*)
           (ok (not (probe-file (pg::physical-backup-lease-allocation-path record t))))
           (when (probe-file path)
             (ok (equal record (pg::physical-backup-lease-allocation-read path)))))))))

(deftest backup-lease-allocation-startup-validates-before-rewriting-parent-or-cleaning-temp
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((parent (backup-lease-test-record))
              (record (backup-lease-allocation-test-record))
              (name (getf (getf parent :request) :name))
              (path (pg::physical-backup-lease-path (getf parent :request)))
              (temporary (pg::physical-backup-lease-allocation-path record t))
              (before-parents nil)
              (before-allocations (make-hash-table :test #'equal)))
         (pg::physical-backup-lease-publish nil parent)
         (setf before-parents pg::*physical-backup-lease-records*)
         (backup-lease-allocation-store-test-write record)
         (with-open-file (stream temporary :direction :output :if-exists :error)
           (declare (ignore stream)))
         (sb-posix:chmod (namestring temporary) #o600)
         (let ((before-bytes (lineage-test-bytes path))
               (pg::*physical-backup-lease-allocations* before-allocations))
           (ok (intake-fails-p #'pg::physical-backup-lease-load-records))
           (ok (equalp before-bytes (lineage-test-bytes path)))
           (ok (probe-file temporary))
           (ok (eq before-parents pg::*physical-backup-lease-records*))
           (ok (eq before-allocations pg::*physical-backup-lease-allocations*))
           (ok (eq :requested (getf (gethash name pg::*physical-backup-lease-records*) :phase)))
           (ok pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-startup-loads-one-validated-graph
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((parent (backup-lease-test-record :ready))
              (record (backup-lease-allocation-test-record :canceled))
              (name (getf (getf parent :request) :name))
              (temporary (pg::physical-backup-lease-allocation-path record t))
              (pg::*physical-backup-lease-allocations* (make-hash-table :test #'equal)))
         (pg::with-write-lock
           (pg::with-wal-publication-fence (begin-publication)
             (begin-publication)
             (pg::physical-backup-lease-write parent)))
         (backup-lease-allocation-store-test-write record)
         (with-open-file (stream temporary :direction :output :if-exists :error)
           (declare (ignore stream)))
         (sb-posix:chmod (namestring temporary) #o600)
         (pg::physical-backup-lease-load-records)
         (ok (equal parent (gethash name pg::*physical-backup-lease-records*)))
         (ok (equal record (gethash (getf record :name) pg::*physical-backup-lease-allocations*)))
         (ok (not (probe-file temporary)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-store-recovers-real-sigkill-before-and-after-cancellation
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "allocation-cache.XXXXXX" cache-root))))))
        (dolist (point '(:before-write :after-write :after-file-fsync :after-rename :after-directory-fsync))
          (dolist (state '(:live :canceled))
            (call-with-backup-lease-store-fixture
             (lambda (root)
               (let* ((parent (backup-lease-test-record :ready))
                      (row (backup-lease-allocation-test-record))
                      (log (merge-pathnames "allocation-child.log" root))
                      (expected (if (member point '(:after-rename :after-directory-fsync))
                                    state (when (eq state :canceled) :live))))
                 (pg::with-write-lock
                   (pg::with-wal-publication-fence (begin-publication)
                     (begin-publication)
                     (pg::physical-backup-lease-write parent)))
                 (when (eq state :canceled) (backup-lease-allocation-store-test-write row))
                 (setf (getf row :state) state)
                 (let ((result
                         (backup-lease-store-run-child
                          root log
                          (format nil
                                  "(setf pg::*physical-backup-lease-allocation-test-hook*
                                   (lambda (point value)
                                     (declare (ignore value))
                                     (when (eq point ~s) (sb-posix:kill (sb-posix:getpid) 9))))
                                 (pg::with-write-lock
                                   (pg::with-wal-publication-fence (begin-publication)
                                     (begin-publication)
                                     (pg::physical-backup-lease-allocation-write '~a)))"
                                  point (pg::physical-bootstrap-manifest-text row))
                          cache)))
                   (ok (equal '(:signaled 9) result) (format nil "~s/~s; ~a" state point log)))
                 (let ((result
                         (backup-lease-store-run-child
                          root log
                          (format nil
                                  "(pg::physical-backup-lease-load-records)
                                 (assert (equal '~a
                                   (gethash ~s pg::*physical-backup-lease-records*)))
                                 (let ((row (gethash ~s pg::*physical-backup-lease-allocations*)))
                                   (assert (eq ~s (getf row :state))))
                                 (multiple-value-bind (paths temporaries)
                                     (pg::physical-backup-lease-allocation-inventory)
                                   (declare (ignore paths))
                                   (assert (null temporaries)))"
                                  (pg::physical-bootstrap-manifest-text parent)
                                  (getf (getf parent :request) :name) (getf row :name) expected)
                          cache)))
                   (ok (equal '(:exited 0) result) (format nil "fresh reopen ~s/~s; ~a" state point log)))
                 (pg::physical-backup-lease-load-records)
                 (ok (eq expected
                         (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))))))))))

(deftest backup-lease-allocation-startup-fail-stops-on-invalid-durable-coordinates
    (call-with-backup-lease-store-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((parent (backup-lease-test-record :ready))
              (row (backup-lease-allocation-test-record))
              (path (pg::physical-backup-lease-allocation-path row))
              (parents pg::*physical-backup-lease-records*)
              (allocations pg::*physical-backup-lease-allocations*))
         (pg::with-write-lock
           (pg::with-wal-publication-fence (begin-publication)
             (begin-publication)
             (pg::physical-backup-lease-write parent)))
         (backup-lease-allocation-store-test-write row)
         (ok (equal row (pg::physical-backup-lease-allocation-read path)))
         (setf (getf row :coordinates) (append '(:kind :bogus) (getf row :coordinates)))
         (let ((bytes (pg::physical-backup-lease-encode-literal "PGBACKUPALLOC1" row 8192)))
           (with-open-file (stream path :direction :output :if-exists :supersede
                                        :element-type '(unsigned-byte 8))
             (write-sequence bytes stream))
           (sb-posix:chmod (namestring path) #o600)
           (ok (intake-fails-p #'pg::physical-backup-lease-load-records))
           (ok pg::*wal-poisoned*)
           (ok (eq parents pg::*physical-backup-lease-records*))
           (ok (eq allocations pg::*physical-backup-lease-allocations*))
           (ok (equalp bytes (lineage-test-bytes path))))))))

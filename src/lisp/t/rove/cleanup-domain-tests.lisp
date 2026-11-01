(in-package #:pg)

(defun cleanup-domain-adoption-result (limit)
  (let* ((*query-temp-pending-cleanups* nil)
         (*query-temp-directory* nil)
         (lock (sb-thread:make-mutex :name "cleanup domain model"))
         (identity (gensym "DOMAIN-"))
         (domain (make-query-temp-cleanup-domain lock identity))
         (owner (make-private-query-temp-scope-owner limit))
         (quota (query-temp-cleanup-owner-quota owner))
         (creator (query-temp-cleanup-owner-thread owner))
         (link (query-temp-cleanup-owner-link owner)))
    (sb-thread:with-mutex (lock)
      (call-with-query-temp-cleanup-domain
       domain identity
       (lambda ()
         (adopt-query-temp-owner-by-domain owner domain)
         (unless (and (eq creator (query-temp-cleanup-owner-thread owner))
                      (eq quota (query-temp-cleanup-owner-quota owner))
                      (eq link (query-temp-cleanup-owner-link owner))
                      (eq domain (query-temp-cleanup-owner-domain owner))
                      (= limit (query-temp-quota-limit quota))
                      (not (query-temp-cleanup-domain-empty-p domain)))
           (return-from cleanup-domain-adoption-result nil))
         (let ((*query-temp-file-limit-bytes* 0))
           (close-query-temp-owner owner))
         (and (query-temp-cleanup-domain-empty-p domain)
              (eq :resolved (query-temp-cleanup-owner-state owner))
              (= limit (query-temp-quota-limit quota))
              (zerop (query-temp-quota-used-bytes quota))
              (null *query-temp-pending-cleanups*)))))))

(cl-spec:defproperty cleanup-domain-adoption-preserves-private-owner-and-quota
    ((limit (range integer 0 1000000)))
  (:about adopt-query-temp-owner-by-domain)
  (:trials (:normal 100))
  (cleanup-domain-adoption-result limit))

(rove:deftest cleanup-domain-adoption-contract
    (unit-check-property 'cleanup-domain-adoption-preserves-private-owner-and-quota))

(rove:deftest cleanup-domain-rejects-wrong-authority-before-directory-effects
    (let* ((root (merge-pathnames
                  (format nil "pg-cleanup-domain-~d-~d/" (sb-posix:getpid) (random 1000000))
                  (uiop:temporary-directory)))
           (*query-temp-directory* nil) (*query-temp-instance-directory* root)
           (*query-temp-pending-cleanups* nil)
           (lock (sb-thread:make-mutex :name "cleanup domain authority"))
           (identity (gensym "DOMAIN-"))
           (domain (make-query-temp-cleanup-domain lock identity))
           (owner (make-private-query-temp-scope-owner nil)))
      (ensure-directories-exist root)
      (unwind-protect
           (progn
             (rove:ok
              (handler-case
                  (progn (call-with-query-temp-cleanup-domain domain identity (lambda () t)) nil)
                (error () t)))
             (sb-thread:with-mutex (lock)
               (rove:ok
                (handler-case
                    (progn (call-with-query-temp-cleanup-domain domain (gensym) (lambda () t)) nil)
                  (error () t)))
               (call-with-query-temp-cleanup-domain
                domain identity (lambda () (adopt-query-temp-owner-by-domain owner domain))))
             (rove:ok
              (handler-case
                  (progn (call-with-query-temp-owner owner #'open-query-temp-io) nil)
                (error () t)))
             (let ((*query-temp-cleanup-owner* owner)
                   (*query-temp-quota* (query-temp-cleanup-owner-quota owner))
                   (*query-temp-file-scope-active-p* t)
                   (*query-temp-scope-directory* nil))
               (rove:ok (handler-case (progn (open-query-temp-io) nil) (error () t)))
               (rove:ok (handler-case (progn (finish-query-temp-scope) nil) (error () t))))
             (rove:ok (eq :active (query-temp-cleanup-owner-state owner)))
             (rove:ok (null (query-temp-cleanup-owner-entries owner)))
             (rove:ok (null (query-temp-cleanup-owner-directory owner)))
             (rove:ok (zerop (query-temp-quota-used-bytes (query-temp-cleanup-owner-quota owner))))
             (rove:ok (null (uiop:subdirectories root)))
             (rove:ok (null (uiop:directory-files root))))
        (sb-thread:with-mutex (lock)
          (call-with-query-temp-cleanup-domain domain identity
                                               (lambda () (close-query-temp-owner owner))))
        (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore))))

(defun call-with-cleanup-domain-file-test (function)
  (let* ((root (merge-pathnames
                (format nil "pg-cleanup-domain-files-~d-~d/" (sb-posix:getpid) (random 1000000))
                (uiop:temporary-directory)))
         (*query-temp-directory* root)
         (*error-output* (make-string-output-stream))
         (lock (sb-thread:make-mutex :name "cleanup domain files"))
         (identity (gensym "DOMAIN-"))
         (domain (make-query-temp-cleanup-domain lock identity)))
    (ensure-directories-exist root)
    (unwind-protect
         (funcall function root lock identity domain)
      (sb-thread:with-mutex (lock)
        (call-with-query-temp-cleanup-domain
         domain identity
         (lambda ()
           (dolist (owner (copy-list (query-temp-cleanup-domain-owners domain)))
             (close-query-temp-owner owner)))))
      (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore))))

(rove:deftest cleanup-domain-survives-creator-and-ordinary-retry
    (call-with-cleanup-domain-file-test
     (lambda (root lock identity domain)
       (let ((unlink (symbol-function 'unlink-query-temp-file))
             (owner nil) (path nil) (stream nil) (failure nil) (attempts 0) (thread nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'unlink-query-temp-file)
                      (lambda (candidate)
                        (if (equal candidate path)
                            (progn (incf attempts) (error "retained domain file"))
                            (funcall unlink candidate))))
                (setf thread
                      (sb-thread:make-thread
                       (lambda ()
                         (handler-case
                             (let ((*query-temp-directory* root) (*query-temp-file-limit-bytes* nil)
                                                                 (*query-temp-file-scope-active-p* nil) (*query-temp-quota* nil)
                                                                 (*query-temp-cleanup-owner* nil) (*query-temp-cleanup-domain* nil)
                                                                 (*backend* nil) (*error-output* (make-string-output-stream)))
                               (setf owner (make-private-query-temp-scope-owner 1024))
                               (sb-thread:with-mutex (lock)
                                 (call-with-query-temp-cleanup-domain
                                  domain identity
                                  (lambda ()
                                    (adopt-query-temp-owner-by-domain owner domain)
                                    (call-with-query-temp-owner
                                     owner
                                     (lambda ()
                                       (multiple-value-setq (path stream) (open-query-temp-output))
                                       (query-spill-write-record stream "domain debt")
                                       (finish-query-temp-stream stream t)))
                                    (close-query-temp-owner owner))))
                               (let ((before attempts))
                                 (retry-pending-query-temp-cleanup)
                                 (unless (= before attempts) (error "creator retried domain cleanup")))
                               (with-query-temp-files
                                 (multiple-value-bind (ordinary-path ordinary-stream) (open-query-temp-output)
                                   (declare (ignore ordinary-path))
                                   (query-spill-write-record ordinary-stream "ordinary query")))
                               t)
                           (error (condition) (setf failure (princ-to-string condition)) nil)))
                       :name "cleanup domain creator"))
                (rove:ok (sb-thread:join-thread thread))
                (rove:ok (null failure) (or failure "creator succeeded"))
                (rove:ok (not (sb-thread:thread-alive-p thread)))
                (rove:ok (eq thread (query-temp-cleanup-owner-thread owner)))
                (rove:ok (eq :pending (query-temp-cleanup-owner-state owner)))
                (rove:ok (probe-file path))
                (rove:ok (not (open-stream-p stream)))
                (rove:ok (plusp (query-temp-quota-used-bytes (query-temp-cleanup-owner-quota owner))))
                (let ((before attempts))
                  (rove:ok
                   (handler-case (progn (retry-pending-query-temp-cleanup domain) nil)
                     (error () t)))
                  (rove:ok (= before attempts)))
                (setf (symbol-function 'unlink-query-temp-file) unlink)
                (sb-thread:with-mutex (lock)
                  (call-with-query-temp-cleanup-domain
                   domain identity
                   (lambda ()
                     (retry-pending-query-temp-cleanup domain)
                     (rove:ok (query-temp-cleanup-domain-empty-p domain))
                     (close-query-temp-owner owner))))
                (rove:ok (eq :resolved (query-temp-cleanup-owner-state owner)))
                (rove:ok (not (probe-file path)))
                (rove:ok (zerop (query-temp-quota-used-bytes (query-temp-cleanup-owner-quota owner))))
                (rove:ok (not (member owner *query-temp-pending-cleanups* :test #'eq))))
           (setf (symbol-function 'unlink-query-temp-file) unlink))))))

(rove:deftest cleanup-domain-retries-only-the-requested-owner-without-relinking
    (call-with-cleanup-domain-file-test
     (lambda (root lock identity domain)
       (declare (ignore root))
       (let ((unlink (symbol-function 'unlink-query-temp-file))
             (attempts (make-hash-table :test #'equal))
             (owners nil) (paths nil))
         (unwind-protect
              (sb-thread:with-mutex (lock)
                (call-with-query-temp-cleanup-domain
                 domain identity
                 (lambda ()
                   (loop repeat 2 do
                     (let ((owner (make-private-query-temp-scope-owner 1024)))
                       (adopt-query-temp-owner-by-domain owner domain)
                       (call-with-query-temp-owner
                        owner
                        (lambda ()
                          (multiple-value-bind (path stream) (open-query-temp-output)
                            (push path paths)
                            (query-spill-write-record stream "pending")
                            (finish-query-temp-stream stream t))))
                       (push owner owners)))
                   (setf (symbol-function 'unlink-query-temp-file)
                         (lambda (path) (incf (gethash path attempts 0)) (error "retained file")))
                   (dolist (owner owners) (close-query-temp-owner owner))
                   (let* ((owner (first owners))
                          (other-before (gethash (second paths) attempts))
                          (link (query-temp-cleanup-owner-link owner))
                          (tail (cdr link)))
                     (close-query-temp-owner owner)
                     (rove:ok (= other-before (gethash (second paths) attempts)))
                     (rove:ok (eq link (query-temp-cleanup-owner-link owner)))
                     (rove:ok (eq tail (cdr link)))
                     (rove:ok (eq :pending (query-temp-cleanup-owner-state owner))))
                   (setf (symbol-function 'unlink-query-temp-file) unlink)
                   (retry-pending-query-temp-cleanup domain)
                   (rove:ok (query-temp-cleanup-domain-empty-p domain))
                   (rove:ok (every (lambda (owner)
                                     (zerop (query-temp-quota-used-bytes
                                             (query-temp-cleanup-owner-quota owner))))
                                   owners)))))
           (setf (symbol-function 'unlink-query-temp-file) unlink))))))

(rove:deftest cleanup-domain-pending-debt-blocks-only-its-own-domain
    (call-with-cleanup-domain-file-test
     (lambda (root lock identity domain)
       (declare (ignore root))
       (let ((unlink (symbol-function 'unlink-query-temp-file))
             (path nil) (attempts 0)
             (other-identity (gensym "OTHER-")))
         (unwind-protect
              (sb-thread:with-mutex (lock)
                (let ((other (make-query-temp-cleanup-domain lock other-identity))
                      (debtor (make-private-query-temp-scope-owner 1024))
                      (fresh (make-private-query-temp-scope-owner 1024)))
                  (call-with-query-temp-cleanup-domain
                   domain identity
                   (lambda ()
                     (adopt-query-temp-owner-by-domain debtor domain)
                     (adopt-query-temp-owner-by-domain fresh domain)
                     (call-with-query-temp-owner
                      debtor
                      (lambda ()
                        (multiple-value-bind (file stream) (open-query-temp-output)
                          (setf path file)
                          (query-spill-write-record stream "debt")
                          (finish-query-temp-stream stream t))))
                     (setf (symbol-function 'unlink-query-temp-file)
                           (lambda (file)
                             (if (equal file path)
                                 (progn (incf attempts) (error "retained file"))
                                 (funcall unlink file))))
                     (close-query-temp-owner debtor)
                     (rove:ok (eq :pending (query-temp-cleanup-owner-state debtor)))
                     (rove:ok
                      (handler-case
                          (progn (call-with-query-temp-owner fresh #'open-query-temp-io nil) nil)
                        (pg-error () t)))
                     (rove:ok (null (query-temp-cleanup-owner-entries fresh)))
                     (rove:ok (null (query-temp-cleanup-owner-directory fresh)))
                     (rove:ok (zerop (query-temp-quota-used-bytes
                                      (query-temp-cleanup-owner-quota fresh))))))
                  (call-with-query-temp-cleanup-domain
                   other other-identity
                   (lambda ()
                     (let ((before attempts)
                           (owner (make-private-query-temp-scope-owner 1024)))
                       (adopt-query-temp-owner-by-domain owner other)
                       (unwind-protect
                            (progn
                              (rove:ok
                               (handler-case (progn (close-query-temp-owner debtor) nil)
                                 (error () t)))
                              (retry-pending-query-temp-cleanup other)
                              (rove:ok (= before attempts))
                              (call-with-query-temp-owner
                               owner
                               (lambda ()
                                 (multiple-value-bind (file stream) (open-query-temp-output)
                                   (declare (ignore file))
                                   (query-spill-write-record stream "independent")))))
                         (close-query-temp-owner owner))
                       (rove:ok (query-temp-cleanup-domain-empty-p other)))))
                  (setf (symbol-function 'unlink-query-temp-file) unlink)
                  (call-with-query-temp-cleanup-domain
                   domain identity
                   (lambda ()
                     (retry-pending-query-temp-cleanup domain)
                     (rove:ok (eq :resolved (query-temp-cleanup-owner-state debtor)))
                     (rove:ok (not (query-temp-cleanup-domain-empty-p domain)))
                     (close-query-temp-owner fresh)
                     (rove:ok (query-temp-cleanup-domain-empty-p domain))))))
           (setf (symbol-function 'unlink-query-temp-file) unlink))))))

(rove:deftest cleanup-domain-retains-empty-private-directory-debt
    (call-with-cleanup-domain-file-test
     (lambda (root lock identity domain)
       (let* ((*query-temp-directory* nil)
              (*query-temp-instance-directory* root)
              (owner (make-private-query-temp-scope-owner 1024))
              (delete-tree (symbol-function 'uiop:delete-directory-tree))
              (directory nil) (path nil) (attempts 0))
         (sb-posix:chmod root #o700)
         (unwind-protect
              (progn
                (sb-thread:with-mutex (lock)
                  (call-with-query-temp-cleanup-domain
                   domain identity
                   (lambda ()
                     (adopt-query-temp-owner-by-domain owner domain)
                     (call-with-query-temp-owner
                      owner
                      (lambda ()
                        (multiple-value-bind (file stream) (open-query-temp-output)
                          (setf path file)
                          (query-spill-write-record stream "private scope")
                          (finish-query-temp-stream stream t))))
                     (setf directory (query-temp-cleanup-owner-directory owner)
                           (symbol-function 'uiop:delete-directory-tree)
                           (lambda (candidate &rest arguments)
                             (if (equal candidate directory)
                                 (progn (incf attempts) (error "retained private directory"))
                                 (apply delete-tree candidate arguments))))
                     (close-query-temp-owner owner)
                     (rove:ok (eq :pending (query-temp-cleanup-owner-state owner)))
                     (rove:ok (not (query-temp-cleanup-domain-empty-p domain))))))
                (rove:ok (not (probe-file path)))
                (rove:ok (probe-file directory))
                (rove:ok (zerop (query-temp-quota-used-bytes
                                 (query-temp-cleanup-owner-quota owner))))
                (let ((before attempts))
                  (retry-pending-query-temp-cleanup)
                  (rove:ok (= before attempts)))
                (setf (symbol-function 'uiop:delete-directory-tree) delete-tree)
                (let ((thread
                        (sb-thread:make-thread
                         (lambda ()
                           (sb-thread:with-mutex (lock)
                             (call-with-query-temp-cleanup-domain
                              domain identity
                              (lambda ()
                                (retry-pending-query-temp-cleanup domain)
                                (query-temp-cleanup-domain-empty-p domain)))))
                         :name "private directory cleanup successor")))
                  (rove:ok (sb-thread:join-thread thread)))
                (rove:ok (eq :resolved (query-temp-cleanup-owner-state owner)))
                (rove:ok (not (probe-file directory)))
                (rove:ok (probe-file root)))
           (setf (symbol-function 'uiop:delete-directory-tree) delete-tree))))))

(defun cleanup-domain-test-octets (&rest bytes)
  (make-array (length bytes) :element-type '(unsigned-byte 8) :initial-contents bytes))

(rove:deftest cleanup-domain-rejects-used-and-shared-private-owners
    (call-with-cleanup-domain-file-test
     (lambda (root lock identity domain)
       (declare (ignore root))
       (sb-thread:with-mutex (lock)
         (call-with-query-temp-cleanup-domain
          domain identity
          (lambda ()
            (let* ((owner (make-private-query-temp-scope-owner nil))
                   (quota (query-temp-cleanup-owner-quota owner))
                   (alias (make-query-temp-scope-owner quota)))
              (rove:ok
               (handler-case (progn (adopt-query-temp-owner-by-domain owner domain) nil)
                 (error () t)))
              (rove:ok (null (query-temp-quota-private-owner quota)))
              (close-query-temp-owner alias)
              (close-query-temp-owner owner))
            (let ((owner (make-private-query-temp-scope-owner nil)))
              (call-with-query-temp-owner owner (lambda () nil) nil)
              (rove:ok
               (handler-case (progn (adopt-query-temp-owner-by-domain owner domain) nil)
                 (error () t)))
              (close-query-temp-owner owner))
            (let* ((owner (make-private-query-temp-scope-owner nil))
                   (quota (query-temp-cleanup-owner-quota owner)))
              (let ((*query-temp-cleanup-owner* owner))
                (rove:ok
                 (handler-case (progn (adopt-query-temp-owner-by-domain owner domain) nil)
                   (error () t))))
              (adopt-query-temp-owner-by-domain owner domain)
              (rove:ok
               (handler-case (progn (adopt-query-temp-owner-by-domain owner domain) nil)
                 (error () t)))
              (rove:ok
               (handler-case (progn (make-query-temp-scope-owner quota) nil) (error () t)))
              (rove:ok (eq owner (query-temp-quota-private-owner quota)))
              (rove:ok (eq :active (query-temp-cleanup-owner-state owner)))
              (close-query-temp-owner owner))
            (rove:ok (query-temp-cleanup-domain-empty-p domain))))))))

(rove:deftest cleanup-domain-checks-memory-spool-authority-before-mutation
    (call-with-cleanup-domain-file-test
     (lambda (root lock identity domain)
       (declare (ignore root))
       (let ((spool (make-logical-spool)))
         (sb-thread:with-mutex (lock)
           (call-with-query-temp-cleanup-domain
            domain identity
            (lambda () (adopt-query-temp-owner-by-domain (logical-spool-owner spool) domain))))
         (dolist (operation
                  (list (lambda () (logical-spool-append spool (cleanup-domain-test-octets 1 2)))
                        (lambda () (logical-spool-seal spool))
                        (lambda () (call-with-logical-spool-visit spool (lambda () nil)))
                        (lambda () (close-logical-spool spool))))
           (rove:ok (handler-case (progn (funcall operation) nil) (error () t)))
           (rove:ok (not (logical-spool-failed-p spool)))
           (rove:ok (not (logical-spool-sealed-p spool)))
           (rove:ok (not (logical-spool-closed-p spool)))
           (rove:ok (not (logical-spool-visiting-p spool)))
           (rove:ok (zerop (logical-spool-count spool))))
         (sb-thread:with-mutex (lock)
           (call-with-query-temp-cleanup-domain
            domain identity
            (lambda ()
              (logical-spool-append spool (cleanup-domain-test-octets 1 2))
              (logical-spool-seal spool)
              (rove:ok (equalp '(#(1 2)) (logical-spool-events-in-order spool)))
              (close-logical-spool spool)
              (rove:ok (query-temp-cleanup-domain-empty-p domain)))))))))

(rove:deftest cleanup-domain-adoption-and-foreign-use-have-a-single-winner
    (dolist (adopt-first-p '(nil t))
      (call-with-cleanup-domain-file-test
       (lambda (root lock identity domain)
         (let ((ready (sb-thread:make-semaphore)) (resume (sb-thread:make-semaphore))
                                                  (spool nil) (failure nil) (adopt-rejected nil) (thread nil))
           (unwind-protect
                (progn
                  (setf thread
                        (sb-thread:make-thread
                         (lambda ()
                           (handler-case
                               (let ((*query-temp-directory* root) (*logical-capture-memory-bytes* 1024))
                                 (setf spool (make-logical-spool))
                                 (when adopt-first-p
                                   (sb-thread:with-mutex (lock)
                                     (call-with-query-temp-cleanup-domain
                                      domain identity
                                      (lambda ()
                                        (adopt-query-temp-owner-by-domain (logical-spool-owner spool) domain)))))
                                 (sb-thread:signal-semaphore ready)
                                 (unless (sb-thread:wait-on-semaphore resume :timeout 10)
                                   (error "cleanup adoption actor timed out"))
                                 (if adopt-first-p
                                     (sb-thread:with-mutex (lock)
                                       (call-with-query-temp-cleanup-domain
                                        domain identity (lambda () (close-logical-spool spool))))
                                     (progn
                                       (sb-thread:with-mutex (lock)
                                         (call-with-query-temp-cleanup-domain
                                          domain identity
                                          (lambda ()
                                            (setf adopt-rejected
                                                  (handler-case
                                                      (progn (adopt-query-temp-owner-by-domain
                                                              (logical-spool-owner spool) domain) nil)
                                                    (error () t))))))
                                       (close-logical-spool spool)))
                                 t)
                             (error (condition) (setf failure (princ-to-string condition)) nil)))
                         :name "cleanup adoption owner"))
                  (rove:ok (sb-thread:wait-on-semaphore ready :timeout 10))
                  (if adopt-first-p
                      (progn
                        (rove:ok
                         (handler-case (progn (logical-spool-append spool (cleanup-domain-test-octets 1)) nil) (error () t)))
                        (rove:ok (zerop (logical-spool-count spool)))
                        (rove:ok (not (logical-spool-failed-p spool))))
                      (progn
                        (logical-spool-append spool (cleanup-domain-test-octets 1))
                        (rove:ok (= 1 (logical-spool-count spool)))))
                  (sb-thread:signal-semaphore resume)
                  (rove:ok (sb-thread:join-thread thread))
                  (rove:ok (null failure) (or failure "actor succeeded"))
                  (rove:ok (eql adopt-first-p (not adopt-rejected)))
                  (sb-thread:with-mutex (lock)
                    (rove:ok (query-temp-cleanup-domain-empty-p domain))))
             (sb-thread:signal-semaphore resume)
             (when thread (sb-thread:join-thread thread))))))))

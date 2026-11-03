(in-package #:pg-physical-tests)

(defun reseed-image-test-model (request)
  (list :manifest
        (list :version 3 :source-pin (getf request :name) :cluster-identity (getf request :cluster-identity)
              :system-id (getf request :system-id) :timeline (getf request :timeline)
              :checkpoint-start 32 :end 152)
        :manifest-digest 991 :files nil))

(deftest reseed-image-preparation-requires-ordinary-stopped-ownership
    (call-with-reseed-owner-directory
     (lambda (root)
       (pg::call-with-physical-adoption-owner
        root
        (lambda (owner)
          (let ((message
                  (handler-case (progn (pg::physical-reseed-prepare-image owner root) "accepted")
                    (error (condition) (princ-to-string condition)))))
            (ok (search "ordinary stopped-reseed ownership" message))
            (ok (not (probe-file (pg::physical-reseed-attempt-path root))))))))))

(deftest reseed-image-preparation-refuses-cancellation-before-stage-or-source-access
    (dolist (phase '(:closing :closed))
      (call-with-reseed-remote-fixture
       (lambda (root stage owner request profile state)
         (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")
         (setf (getf state :abort-phases) (list phase))
         (pg::physical-reseed-cancel owner :helper "/unused/helper")
         (uiop:delete-directory-tree stage :validate t)
         (with-physical-store-directory (incoming)
           (sb-posix:chmod (namestring incoming) #o700)
           (let* ((before (lineage-test-bytes (pg::physical-reseed-attempt-path root)))
                  (frozen (getf state :frozen))
                  (message
                    (handler-case
                        (progn (pg::physical-reseed-prepare-image owner incoming :helper "/unused/helper") "accepted")
                      (error (condition) (princ-to-string condition)))))
             (ok (search "original active attempt and frozen stage" message))
             (ok (= frozen (getf state :frozen)))
             (ok (= 1 (getf state :captures)))
             (ok (null (pg::directory-entry-names incoming)))
             (ok (equalp before (lineage-test-bytes (pg::physical-reseed-attempt-path root))))))))))

(deftest reseed-image-preparation-keeps-legacy-attempts-cleanup-only
    (call-with-reseed-remote-fixture
     (lambda (root stage owner request profile state)
       (declare (ignore root stage request profile))
       (pg::physical-reseed-attempt-publish owner (reseed-attempt-test-record owner))
       (with-physical-store-directory (incoming)
         (sb-posix:chmod (namestring incoming) #o700)
         (let ((message
                 (handler-case
                     (progn (pg::physical-reseed-prepare-image owner incoming :helper "/unused/helper") "accepted")
                   (error (condition) (princ-to-string condition)))))
           (ok (search "original active attempt and frozen stage" message))
           (ok (zerop (getf state :frozen)))
           (ok (zerop (getf state :captures)))
           (ok (null (pg::directory-entry-names incoming))))))))

(deftest reseed-image-preparation-refuses-unsafe-workspaces-before-reconnecting
    (dolist (kind '(:old-child :stage-child :same :public :special-mode :nonempty :mount))
      (call-with-reseed-remote-fixture
       (lambda (root stage owner request profile state)
         (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")
         (with-physical-store-directory (separate)
           (let* ((incoming (case kind
                              (:old-child (merge-pathnames "incoming/" root))
                              (:stage-child (merge-pathnames "incoming/" stage))
                              (:same stage)
                              (otherwise separate)))
                  (frozen (getf state :frozen))
                  (mount-calls 0)
                  (old-mount (symbol-function 'pg::physical-adoption-install-mount-id)))
             (when (member kind '(:old-child :stage-child))
               (sb-posix:mkdir (namestring incoming) #o700))
             (sb-posix:chmod (namestring separate) #o700)
             (case kind
               (:public (sb-posix:chmod (namestring incoming) #o755))
               (:special-mode (sb-posix:chmod (namestring incoming) #o2700))
               (:nonempty (with-open-file (out (merge-pathnames "preserve" incoming) :direction :output)
                            (write-string "do not overwrite" out))))
             (unwind-protect
                  (progn
                    (when (eq kind :mount)
                      (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                            (lambda (path &key directory-fd)
                              (declare (ignore path directory-fd))
                              (incf mount-calls)
                              (if (= mount-calls 3) 2 1))))
                    (ok (intake-fails-p
                         (lambda () (pg::physical-reseed-prepare-image owner incoming :helper "/unused/helper"))))
                    (ok (= frozen (getf state :frozen)))
                    (ok (= 1 (getf state :captures)))
                    (when (eq kind :mount) (ok (= 3 mount-calls)))
                    (cond
                      ((eq kind :same) (ok (probe-file (merge-pathnames "received.witness" incoming))))
                      ((eq kind :nonempty)
                       (ok (equalp (pg::string->bytes "do not overwrite")
                                   (lineage-test-bytes (merge-pathnames "preserve" incoming)))))
                      (t (ok (null (pg::directory-entry-names incoming))))))
               (setf (symbol-function 'pg::physical-adoption-install-mount-id) old-mount))))))))

(deftest reseed-image-preparation-needs-ready-status-before-write-and-after-clone
    (dolist (kind '(:unknown :lost-after-clone))
      (call-with-reseed-remote-fixture
       (lambda (root stage owner request profile state)
         (declare (ignore root))
         (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")
         (dolist (name '("base/" "global/" "pg_wal/"))
           (sb-posix:mkdir (namestring (merge-pathnames name stage)) #o700))
         (with-physical-store-directory (incoming)
           (sb-posix:chmod (namestring incoming) #o700)
           (let* ((checks 0) (mkdirs 0)
                             (image (reseed-image-test-model request))
                             (old-check (symbol-function 'pg::physical-reseed-image-check))
                             (pg::*physical-image-test-hook*
                               (lambda (point name)
                                 (declare (ignore name))
                                 (when (eq point :after-mkdir) (incf mkdirs)))))
             (when (eq kind :unknown) (setf (getf state :status-result) nil))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-reseed-image-check)
                          (lambda (directory &optional expected)
                            (declare (ignore directory expected))
                            (incf checks)
                            (when (and (eq kind :lost-after-clone) (= checks 3))
                              (setf (getf state :status-result) nil))
                            image))
                    (let ((message
                            (handler-case
                                (progn (pg::physical-reseed-prepare-image owner incoming :helper "/unused/helper") nil)
                              (error (condition) (princ-to-string condition)))))
                      (ok message)
                      (ok (search "source backup lease is no longer ready" message) message))
                    (ok (= (if (eq kind :unknown) 1 2) (getf state :statuses)))
                    (ok (= (if (eq kind :unknown) 1 3) checks))
                    (ok (= (if (eq kind :unknown) 0 3) mkdirs))
                    (ok (= 1 (getf state :captures)))
                    (ok (eq :premanifest (getf (pg::physical-reseed-attempt-read owner) :phase)))
                    (ok (equal (if (eq kind :unknown) nil '("base" "global" "pg_wal"))
                               (sort (pg::directory-entry-names incoming) #'string<))))
               (setf (symbol-function 'pg::physical-reseed-image-check) old-check))))))))
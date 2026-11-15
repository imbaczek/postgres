;; Destructive test for a disposable prepared child only.
;; Drops canonical slot files, then checks that the permanent historical proof
;; can be observed without resurrecting slots.

(require :asdf)
(let ((source (truename (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*)))))
  (pushnew source asdf:*central-registry* :test #'equal)
  (asdf:load-asd (merge-pathnames "pg.asd" source))
  (asdf:load-system "pg"))
(in-package #:pg)

(defun native-imported-host-reject-short-control (owner context facts)
  (let ((original (symbol-function 'logical-slot-imported-owner-source-under-lock))
        (fork (getf (getf facts :certificate) :fork)))
    (assert (plusp fork))
    (unwind-protect
         (progn
           (setf (symbol-function 'logical-slot-imported-owner-source-under-lock)
                 (lambda (candidate)
                   (multiple-value-bind (slot system-id history end) (funcall original candidate)
                     (declare (ignore end))
                     (values slot system-id history (1- fork)))))
           (assert (handler-case
                       (progn (logical-slot-imported-check-certificate-source owner context facts) nil)
                     (error () t))))
      (setf (symbol-function 'logical-slot-imported-owner-source-under-lock) original))))

(defun native-imported-host-observation ()
  (unless (equal "1" (uiop:getenv "PG_NATIVE_HOST_DISPOSABLE"))
    (error "This test drops slot files; set PG_NATIVE_HOST_DISPOSABLE=1 only for its disposable fixture"))
  (let* ((*pgdata* (uiop:ensure-directory-pathname
                    (or (uiop:getenv "PG_NATIVE_PROOF_DATA") (error "Set PG_NATIVE_PROOF_DATA"))))
         (*print-circle* t) (*print-level* 4) (*print-length* 8)
         (*wal-level* :logical) (*startup-installed-configuration* nil)
         (exit-hooks sb-ext:*exit-hooks*) (saved-owner nil) (saved-check nil))
    (let ((settings (startup-default-settings (namestring *pgdata*))))
      (setf (getf (gethash "wal_level" settings) :value) "logical"
            *startup-installed-configuration*
            (startup-prepare-installation settings (make-hash-table :test #'equal)
                                          (make-hash-table :test #'equal) nil)))
    (unwind-protect
         (progn
           (postmaster-own-data-directory "127.0.0.1" 5439)
           (call-with-live-operation-startup-quiescence
            (lambda (gate)
              (call-with-postmaster-ownership
               (lambda (ownership)
                 (check-primary-storage-startup-context gate ownership)
                 (wal-mark-primary-startup)
                 (wal-init)
                 (xact-init)
                 (wal-recover)
                 (note-primary-startup-replay)
                 (physical-promotion-restore-primary-retained)
                 (let ((registry *wal-replication-slots*)
                       (fds (length (directory #p"/proc/self/fd/*")))
                       (dropped 0))
                   (assert (zerop (hash-table-count registry)))
                   (assert (null *catalog*))
                   (dolist (entry (logical-slot-startup-read-inventory :local-workspace t))
                     (let* ((name (wal-replication-slot-name (logical-slot-startup-entry-slot entry)))
                            (root (wal-replication-slot-directory))
                            (claim (slot-state-claim-drop root name)))
                       (slot-state-drop root name claim)
                       (incf dropped)))
                   (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
                   (call-with-logical-slot-imported-host-startup-owner
                    gate ownership
                    (lambda (owner)
                      (setf saved-owner owner)
                      (call-with-physical-directory-root
                       owner
                       (lambda (root path)
                         (declare (ignore path))
                         (call-with-physical-image-owned-context
                          root t
                          (lambda (context)
                            (call-with-logical-slot-imported-proof-files
                             owner context
                             (lambda (facts check under-lock)
                               (setf saved-check check)
                               (assert (plusp (length (getf (getf facts :certificate) :slots))))
                               (native-imported-host-reject-short-control owner context facts)
                               (funcall check)
                               (sb-thread:with-mutex (*wal-lock*) (funcall under-lock))
                               (format t "~&NATIVE-HOST proof bound with zero slot files; dropped=~d historical-rows=~d~%"
                                       dropped (length (getf (getf facts :certificate) :slots)))))))))))
                   (assert (not (physical-adoption-owner-active-p saved-owner)))
                   (assert (handler-case (progn (funcall saved-check) nil) (error () t)))
                   (assert (eq registry *wal-replication-slots*))
                   (assert (zerop (hash-table-count registry)))
                   (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
                   (assert (null *catalog*))
                   (assert (= fds (length (directory #p"/proc/self/fd/*"))))
                   (assert (null *primary-wal-writer*))
                   (format t "~&NATIVE-HOST PASS canonical permanent proof checked independently of current slots; no publication~%")))))))
      (postmaster-remove-pid-file)
      (when *postmaster-data-lock* (close *postmaster-data-lock*))
      (setf sb-ext:*exit-hooks* exit-hooks))))

(native-imported-host-observation)

(in-package #:pg)

(defstruct (logical-slot-copy-startup-owner
            (:include physical-adoption-owner)
            (:constructor %make-logical-slot-copy-startup-owner))
  (ownership nil :read-only t)
  existing-proof)

(defun logical-slot-copy-check-postmaster-owner (owner ownership)
  (physical-directory-check-reseed-intent (physical-adoption-owner-directory owner))
  (postmaster-check-ownership ownership)
  (unless (and (equal (physical-adoption-owner-directory owner)
                      (postmaster-ownership-directory ownership))
               (= (physical-adoption-owner-device owner) (postmaster-ownership-device ownership))
               (= (physical-adoption-owner-inode owner) (postmaster-ownership-inode ownership))
               (eq (physical-adoption-owner-lock owner) (postmaster-ownership-lock ownership)))
    (error "logical copy verification owner differs from its postmaster"))
  owner)

(defun logical-slot-copy-owner-arguments (ownership)
  (list :directory (postmaster-ownership-directory ownership)
        :device (postmaster-ownership-device ownership)
        :inode (postmaster-ownership-inode ownership)
        :lock (postmaster-ownership-lock ownership)
        :lock-device (postmaster-ownership-lock-device ownership)
        :lock-inode (postmaster-ownership-lock-inode ownership)
        :thread sb-thread:*current-thread* :active-p t :ownership ownership))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-copy-startup-owner))
  (physical-adoption-check-startup-quiescence)
  (logical-slot-copy-check-postmaster-owner
   owner (logical-slot-copy-startup-owner-ownership owner)))

(defmethod physical-adoption-owner-extra-root-names ((owner logical-slot-copy-startup-owner))
  (physical-directory-check-owner owner)
  (append (list "postmaster.pid")
          (when (logical-slot-copy-startup-owner-existing-proof owner)
            (list "logical-slot-copy.base-verified"))))

(defun call-with-logical-slot-copy-owned-input (constructor function)
  (call-with-postmaster-ownership
   (lambda (ownership)
     (let ((owner (apply constructor (logical-slot-copy-owner-arguments ownership))))
       (unwind-protect
            (progn
              (physical-directory-check-owner owner)
              (call-with-physical-directory-root
               owner
               (lambda (borrowed root)
                 (declare (ignore root))
                 (call-with-physical-backup-image-directory
                  borrowed
                  (lambda (path)
                    (declare (ignore path))
                    (let ((context *physical-image-context*))
                      (logical-slot-copy-open-input-sidecar owner context)
                      (let ((proof
                              (when (member "logical-slot-copy.base-verified"
                                            (directory-entry-names
                                             (physical-image-context-root-path context)
                                             :follow-symlinks t :maximum-entries 128)
                                            :test #'equal)
                                (let ((bytes (logical-slot-copy-read-file
                                              context "logical-slot-copy.base-verified"
                                              +logical-slot-copy-base-proof-maximum-bytes+)))
                                  (logical-slot-copy-base-proof-decode bytes)
                                  bytes))))
                        (multiple-value-prog1 (funcall function owner context proof)
                          (logical-slot-copy-check-input owner context)))))))))
         (setf (physical-adoption-owner-active-p owner) nil))))))

(defun call-with-logical-slot-copy-startup-input (function)
  (physical-adoption-check-startup-quiescence)
  (call-with-logical-slot-copy-owned-input
   #'%make-logical-slot-copy-startup-owner
   (lambda (owner context proof)
     (setf (logical-slot-copy-startup-owner-existing-proof owner) proof)
     (funcall function owner context))))

(defun logical-slot-copy-base-proof-value (context manifest descriptor)
  (logical-slot-copy-check-binding descriptor manifest
                                   (logical-slot-copy-descriptor-manifest-digest descriptor))
  (let ((root (physical-image-file-facts (physical-image-context-root-fd context))))
    (list :phase :base-verified :copy-id (copy-seq (logical-slot-copy-descriptor-copy-id descriptor))
          :device (first root) :inode (second root)
          :manifest-digest (logical-slot-copy-descriptor-manifest-digest descriptor)
          :descriptor-digest (physical-store-update-digest
                              #xCBF29CE484222325 (logical-slot-copy-descriptor-encode descriptor))
          :system-id (getf manifest :system-id) :end (getf manifest :end)
          :timeline (getf manifest :timeline)
          :history-digest (physical-store-update-digest
                           #xCBF29CE484222325 (logical-slot-copy-descriptor-history descriptor))
          :slots (length (logical-slot-copy-descriptor-slots descriptor))
          :pieces (length (logical-slot-copy-descriptor-wal-pieces descriptor)))))

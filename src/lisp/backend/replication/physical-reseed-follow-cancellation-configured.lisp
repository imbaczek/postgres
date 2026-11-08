(in-package #:pg)

(defun physical-reseed-follow-cancellation-configured-record (value)
  (let* ((record (physical-reseed-follow-copy value))
         (configuration (getf record :configuration)))
    (unless (and (eq :inert (getf configuration :phase))
                 (null (getf record :activation))
                 (null (getf record :retirement)))
      (error "configured cancellation ownership requires inert history before activation"))
    record))

(defun physical-reseed-follow-cancellation-configured-history (value)
  ;; Freeze the inert configured predecessor in a private copy, leaving the
  ;; cancellation field out of the history bound to the cleanup owner.
  (let ((record (physical-reseed-follow-cancellation-configured-record value)))
    (remf record :cancellation)
    record))

;; This owner admits read-only observations of inert configured history.
;; Its forward-owner method rejects forward follow operations.
(defstruct (physical-reseed-follow-cancellation-owner
            (:include physical-reseed-follow-owner)
            (:print-function print-physical-reseed-install-owner)))

(defmethod physical-reseed-follow-check-configuration-role
    ((owner physical-reseed-follow-cancellation-owner) record)
  (physical-reseed-follow-cancellation-check-owner-history owner record))

(defmethod physical-reseed-follow-check-forward-owner
    ((owner physical-reseed-follow-cancellation-owner))
  (error "cancellation ownership cannot perform forward follow operations"))

(defun physical-reseed-follow-cancellation-initial-arguments (directory)
  (let ((fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
           (let* ((snapshot (physical-reseed-follow-snapshot-at fd))
                  (record (physical-reseed-follow-cancellation-configured-record (getf snapshot :record)))
                  (arguments (physical-reseed-follow-configuration-initial-arguments directory)))
             (unless (equal (getf arguments :binding) (physical-reseed-follow-core-projection record))
               (error "cancellation seed changed its configured predecessor"))
             (physical-reseed-follow-check-snapshot fd snapshot)
             (setf (getf arguments :binding)
                   (physical-reseed-follow-cancellation-configured-history record))
             arguments))
      (when fd (sb-posix:close fd)))))

(defun call-with-physical-reseed-follow-cancellation-configured-image (owner function)
  (unless (physical-reseed-follow-cancellation-owner-p owner)
    (error "configured cancellation image requires dedicated cleanup ownership"))
  (call-with-physical-directory-root
   owner
   (lambda (fd directory)
     (declare (ignore directory))
     (physical-reseed-follow-cancellation-configured-record
      (physical-reseed-follow-read-at fd "physical-reseed.follow"))))
  (%call-with-physical-reseed-follow-mapped-image owner function))

(defmethod physical-reseed-follow-image-dispatch
    ((owner physical-reseed-follow-cancellation-owner) function)
  (call-with-physical-reseed-follow-cancellation-configured-image owner function))

(defmethod physical-reseed-follow-installation-layout-state
    ((owner physical-reseed-follow-cancellation-owner) descriptors value binding images prefix check)
  (physical-reseed-follow-cancellation-configured-record value)
  (%physical-reseed-follow-configuration-layout-state owner descriptors value binding images prefix check))

;; The private registry binds each cleanup owner to its thread and copied
;; history for the owner's lifetime.  Observe before the callback and again
;; after it returns normally, then remove the registry entry on exit.
(let ((owners (make-hash-table :test #'eq))
      (lock (sb-thread:make-mutex :name "configured cancellation history")))
  (defun physical-reseed-follow-cancellation-check-owner-history (owner record)
    (physical-directory-check-owner-common owner)
    (let* ((copy (physical-reseed-follow-cancellation-configured-record record))
           (history (physical-reseed-follow-cancellation-configured-history copy)))
      (sb-thread:with-mutex (lock)
        (let ((entry (gethash owner owners)))
          (unless (and entry
                       (physical-adoption-owner-active-p owner)
                       (eq (first entry) sb-thread:*current-thread*)
                       (equal (second entry) (physical-reseed-follow-owner-binding owner))
                       (equal (second entry) history))
            (error "configured cancellation owner changed its frozen history"))))
      copy))

  (defun call-with-physical-reseed-follow-cancellation-owner (directory function)
    (let ((owned nil))
      (unwind-protect
           (call-with-physical-adoption-directory-owner
            directory
            (lambda (owner)
              (flet ((verify ()
                       (physical-reseed-follow-observe-configured-cancellation owner)))
                (verify)
                (multiple-value-prog1 (funcall function owner) (verify))))
            :constructor
            (lambda (&rest arguments)
              (sb-sys:without-interrupts
                (setf owned (apply #'make-physical-reseed-follow-cancellation-owner arguments))
                (sb-thread:with-mutex (lock)
                  (setf (gethash owned owners)
                        (list sb-thread:*current-thread*
                              (physical-adoption-attempt-value-copy
                               (physical-reseed-follow-owner-binding owned))))))
              owned)
            :initial-arguments #'physical-reseed-follow-cancellation-initial-arguments)
        (when owned
          (sb-sys:without-interrupts
            (sb-thread:with-mutex (lock)
              (remhash owned owners))))))))

(defstruct (physical-reseed-follow-cancellation-observation
            (:constructor %make-physical-reseed-follow-cancellation-observation (data))
            (:copier nil)
            (:print-function physical-libpq-print-private-object))
  data)

(defun physical-reseed-follow-cancellation-configured-observation-at (owner fd check)
  (let* ((context (physical-reseed-follow-installation-context owner))
         (descriptors (physical-reseed-follow-installation-context-descriptors context))
         (record (physical-reseed-follow-installation-context-record context))
         (plan (physical-reseed-follow-installation-context-plan context))
         (binding (physical-reseed-follow-installation-context-binding context))
         (images (physical-reseed-follow-installation-context-images context))
         (prefix (physical-reseed-follow-installation-context-prefix context))
         (mount (physical-adoption-install-mount-id nil :directory-fd fd)))
    (funcall check)
    (when (physical-reseed-follow-entry fd "physical-reseed.follow.tmp" t)
      (error "configured cancellation temporary requires explicit reconciliation"))
    (let* ((snapshot (physical-reseed-follow-snapshot-at fd))
           (layout (physical-reseed-follow-installation-layout-state
                    owner descriptors record binding images prefix check))
           (stamp (physical-reseed-follow-retirement-role-stamp descriptors (getf layout :names))))
      (setf (getf layout :images)
            (loop for (logical parent leaf row) in (getf layout :images)
                  for role = (car (rassoc parent descriptors))
                  do (unless role (error "configured observation has an unknown image parent"))
                  collect (list logical role leaf row)))
      (let ((observation
              (physical-adoption-attempt-value-copy
               (list :record (physical-reseed-follow-cancellation-configured-record (getf snapshot :record))
                     :snapshot snapshot
                     :attempt (physical-reseed-follow-attempt-snapshot-at fd)
                     :install (list :record (physical-reseed-install-read-at fd "physical-reseed.install")
                                    :row (physical-reseed-install-old-observe fd "physical-reseed.install" mount))
                     :plan plan :prefix prefix :binding binding :layout layout :stamp stamp
                     :control (physical-store-control-tuple
                               (physical-reseed-follow-installation-context-store context))))))
        (funcall check)
        (physical-reseed-follow-check-snapshot fd snapshot)
        (physical-reseed-follow-retirement-check-stamp stamp)
        observation))))

(defun physical-reseed-follow-observe-configured-cancellation (owner)
  ;; Collect detached observation data while the mapped resources are held.
  ;; After they close, audit the ten directory roles' metadata and return the
  ;; data in an opaque observation.
  (unless (physical-reseed-follow-cancellation-owner-p owner)
    (error "configured cancellation observation requires dedicated cleanup ownership"))
  (physical-reseed-follow-check-idle-state owner)
  (unwind-protect
       (progn
         (setf (physical-reseed-follow-owner-busy-p owner) t)
         (let ((observation
                 (call-with-physical-reseed-follow-image
                  owner
                  (lambda (fd check)
                    (physical-reseed-follow-cancellation-configured-observation-at owner fd check)))))
           (physical-reseed-follow-retirement-audit-stamp
            (getf observation :stamp)
            (lambda () (physical-reseed-check-owner owner)))
           (%make-physical-reseed-follow-cancellation-observation observation)))
    (setf (physical-reseed-follow-owner-busy-p owner) nil)))

(defun physical-reseed-follow-read-configured-cancellation (owner)
  (getf (physical-reseed-follow-cancellation-observation-data
         (physical-reseed-follow-observe-configured-cancellation owner))
        :record))

(in-package #:pg)

;;; Publication has typed, scoped authority over a retained input proof.
;;; A failed check revokes that authority even if the error is caught.
(defstruct (logical-slot-copy-install-publisher
            (:constructor %make-logical-slot-copy-install-publisher)
            (:print-function physical-libpq-print-private-object))
  "Scoped publication authority tied to a proved input and held root."
  (owner nil :read-only t) (context nil :read-only t) (input nil :read-only t)
  (record nil :read-only t) (operation nil :read-only t) (image nil :read-only t)
  (root nil :read-only t) nodes descriptors files work container journal active phase failed)

(defvar *logical-slot-copy-install-publishers* nil)
(defvar *logical-slot-copy-install-publication-test-hook* nil)

(defgeneric logical-slot-copy-install-publisher-check-role (publisher)
  (:documentation "Check role-specific authority while descriptors are held."))

(defmethod logical-slot-copy-install-publisher-check-role ((publisher logical-slot-copy-install-publisher))
  publisher)

(defgeneric logical-slot-copy-install-publisher-check-closed-role (publisher)
  (:documentation "Check role-specific observations after descriptor cleanup."))

(defmethod logical-slot-copy-install-publisher-check-closed-role ((publisher logical-slot-copy-install-publisher))
  publisher)

(defun logical-slot-copy-install-publisher-registration (publisher)
  "Require the exact registered publisher and an active scope."
  (unless (and (logical-slot-copy-install-publisher-p publisher)
               (member publisher *logical-slot-copy-install-publishers* :test #'eq))
    (error "logical installation requires its registered publisher"))
  (when (logical-slot-copy-install-publisher-failed publisher)
    (error "logical installation publisher scope ended"))
  publisher)

(defun logical-slot-copy-install-revoke-publisher (publisher)
  "Fail the publisher and revoke its input and outer operation."
  (setf (logical-slot-copy-install-publisher-failed publisher) t)
  (logical-slot-copy-install-revoke-input (logical-slot-copy-install-publisher-record publisher)))

(defun logical-slot-copy-install-check-publisher (publisher)
  "Recheck the input seal and held inode bindings; revoke on failure."
  (logical-slot-copy-install-publisher-registration publisher)
  (let ((completed nil))
    (unwind-protect
         (progn
           (logical-slot-copy-install-check-input-seal
            (logical-slot-copy-install-publisher-owner publisher)
            (logical-slot-copy-install-publisher-context publisher)
            (logical-slot-copy-install-publisher-input publisher)
            (logical-slot-copy-install-publisher-record publisher))
           (unless (and
                    (eq (logical-slot-copy-install-publisher-record publisher)
                        (logical-slot-copy-install-input-record
                         (logical-slot-copy-install-publisher-owner publisher)
                         (logical-slot-copy-install-publisher-context publisher)
                         (logical-slot-copy-install-publisher-input publisher)))
                    (equalp (logical-slot-copy-install-publisher-image publisher)
                            (logical-slot-copy-install-input-state-image
                             (logical-slot-copy-install-publisher-record publisher))))
             (error "logical installation publisher input binding changed"))
           (logical-slot-copy-install-check-node (logical-slot-copy-install-publisher-root publisher))
           (dolist (node (logical-slot-copy-install-publisher-nodes publisher))
             (logical-slot-copy-install-check-node node))
           (dolist (row (logical-slot-copy-install-publisher-files publisher))
             (unless (equal (second row) (physical-image-file-facts (first row)))
               (error "logical installation publisher file changed")))
           (logical-slot-copy-install-publisher-check-role publisher)
           (setf completed t)
           publisher)
      (unless completed (logical-slot-copy-install-revoke-publisher publisher)))))

(defun logical-slot-copy-install-publisher-point (publisher point)
  (when *logical-slot-copy-install-publication-test-hook*
    (funcall *logical-slot-copy-install-publication-test-hook* point publisher))
  (logical-slot-copy-install-check-publisher publisher))

(defun logical-slot-copy-install-publisher-proof (publisher)
  "Recheck the retained input proof and publisher bindings."
  (logical-slot-copy-install-check-publisher publisher)
  (logical-slot-copy-install-check-input
   (logical-slot-copy-install-publisher-owner publisher)
   (logical-slot-copy-install-publisher-context publisher)
   (logical-slot-copy-install-publisher-input publisher))
  (logical-slot-copy-install-check-publisher publisher))

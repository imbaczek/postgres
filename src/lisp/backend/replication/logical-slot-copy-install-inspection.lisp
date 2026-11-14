(in-package #:pg)

(defstruct (logical-slot-copy-install-inspection
            (:constructor %make-logical-slot-copy-install-inspection)
            (:print-function physical-libpq-print-private-object))
  operation root nodes active (facts (make-hash-table :test #'eq)) closed)

(defvar *logical-slot-copy-install-inspection* nil)

(defun logical-slot-copy-install-check-inspection (&key closed)
  (let ((scope *logical-slot-copy-install-inspection*))
    (unless (and (logical-slot-copy-install-inspection-p scope)
                 (logical-slot-copy-install-inspection-active scope))
      (error "logical installation inspection scope ended"))
    (logical-slot-copy-install-check-operation
     (logical-slot-copy-install-inspection-operation scope))
    (logical-slot-copy-install-check-node (logical-slot-copy-install-inspection-root scope))
    (dolist (node (logical-slot-copy-install-inspection-nodes scope))
      (logical-slot-copy-install-check-node node)
      (unless (equal (gethash node (logical-slot-copy-install-inspection-facts scope))
                     (physical-image-file-facts (logical-slot-copy-install-node-fd node)))
        (error "logical installation inspection observation changed")))
    (when closed
      (dolist (entry (logical-slot-copy-install-inspection-closed scope))
        (unless (equal (second entry)
                       (physical-image-file-facts
                        (logical-slot-copy-install-node-fd
                         (logical-slot-copy-install-inspection-root scope))
                        (first entry)))
          (error "logical installation inspection observation changed"))))
    scope))

(defun logical-slot-copy-install-inspection-node (node)
  (let ((scope (logical-slot-copy-install-check-inspection)))
    (unless (or (eq node (logical-slot-copy-install-inspection-root scope))
                (member node (logical-slot-copy-install-inspection-nodes scope) :test #'eq))
      (error "logical installation inspection inode is not registered"))
    node))

(defun logical-slot-copy-install-inspection-open (parent name kind group &key missing)
  (declare (notinline physical-image-openat sb-posix:close))
  (let ((scope (logical-slot-copy-install-check-inspection)) (fd nil) (registered nil))
    (logical-slot-copy-install-inspection-node parent)
    (when group (logical-slot-copy-install-inspection-node group))
    (unless (and (not (eq :file (logical-slot-copy-install-node-kind parent)))
                 (physical-adoption-install-relative-p name) (not (find #\/ name))
                 (member kind '(:file :directory :slot-root)))
      (error "invalid logical installation inspection path"))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       (logical-slot-copy-install-node-fd parent) name
                       (logior sb-posix:o-rdonly sb-posix:o-nofollow +postmaster-o-cloexec+
                               (if (eq kind :file) sb-posix:o-nonblock sb-posix:o-directory)) 0))
             (when (minusp fd)
               (setf fd nil)
               (if (and missing (= sb-posix:enoent (sb-alien:get-errno)))
                   (return-from logical-slot-copy-install-inspection-open
                     (progn (logical-slot-copy-install-check-inspection) nil))
                   (error "cannot open logical installation inspection inode")))
             (let ((node (%make-logical-slot-copy-install-node
                          :fd fd :parent parent :name (copy-seq name) :kind kind
                          :identity (logical-slot-copy-install-file-identity (physical-image-file-facts fd)))))
               (push node (logical-slot-copy-install-inspection-nodes scope))
               (setf registered t
                     (gethash node (logical-slot-copy-install-inspection-facts scope))
                     (physical-image-file-facts fd))
               (logical-slot-copy-install-check-inspection)
               (when group
                 (unless (and (= (first (logical-slot-copy-install-node-identity node))
                                 (first (logical-slot-copy-install-node-identity group)))
                              (= (third (logical-slot-copy-install-node-identity node))
                                 (third (logical-slot-copy-install-node-identity group))))
                   (error "logical installation inspection crosses its current mount group")))
               node))
        (when (and fd (not registered))
          (handler-case (sb-posix:close fd)
            (error (condition)
              (ignore-errors (report-query-temp-cleanup-failure "installation inspection open" condition)))))))))

(defun logical-slot-copy-install-inspection-path (node)
  (let ((parts nil))
    (loop for current = node then (logical-slot-copy-install-node-parent current)
          while (logical-slot-copy-install-node-parent current)
          do (push (logical-slot-copy-install-node-name current) parts))
    (format nil "~{~a~^/~}" parts)))

(defun logical-slot-copy-install-inspection-close (node)
  (declare (notinline sb-posix:close))
  (let ((scope *logical-slot-copy-install-inspection*))
    (unless (and scope (member node (logical-slot-copy-install-inspection-nodes scope) :test #'eq))
      (error "logical installation inspection cannot close a borrowed inode"))
    (sb-sys:without-interrupts
      (push (list (logical-slot-copy-install-inspection-path node)
                  (gethash node (logical-slot-copy-install-inspection-facts scope)))
            (logical-slot-copy-install-inspection-closed scope))
      (remhash node (logical-slot-copy-install-inspection-facts scope))
      (setf (logical-slot-copy-install-inspection-nodes scope)
            (delete node (logical-slot-copy-install-inspection-nodes scope) :test #'eq))
      (sb-posix:close (logical-slot-copy-install-node-fd node)))))

(defun logical-slot-copy-install-inspection-read (node maximum)
  (declare (notinline sb-posix:read))
  (logical-slot-copy-install-inspection-node node)
  (unless (and (eq :file (logical-slot-copy-install-node-kind node))
               (integerp maximum) (<= 0 maximum +logical-slot-copy-install-maximum-bytes+))
    (error "invalid logical installation inspection read"))
  (let* ((fd (logical-slot-copy-install-node-fd node))
         (before (physical-image-file-facts fd)) (size (sixth before)))
    (unless (and (<= 0 size maximum) (zerop (sb-posix:lseek fd 0 sb-posix:seek-set)))
      (error "logical installation inspection file exceeds its bound"))
    (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
      (sb-sys:with-pinned-objects (bytes)
        (loop with position = 0 while (< position size)
              for count = (sb-posix:read fd (sb-sys:sap+ (sb-sys:vector-sap bytes) position) (- size position))
              do (unless (plusp count) (error "short logical installation inspection read"))
                 (incf position count)))
      (logical-slot-copy-install-check-inspection)
      (unless (equal before (physical-image-file-facts fd))
        (error "logical installation inspection file changed during reading"))
      bytes)))

(defun logical-slot-copy-install-inspection-names (node maximum)
  (logical-slot-copy-install-inspection-node node)
  (when (eq :file (logical-slot-copy-install-node-kind node))
    (error "logical installation inventory requires a directory"))
  (prog1 (sort (directory-entry-names
                (physical-image-fd-path (logical-slot-copy-install-node-fd node))
                :follow-symlinks t :maximum-entries maximum) #'string<)
    (logical-slot-copy-install-check-inspection)))

(defun logical-slot-copy-install-inspection-identity (node expected)
  (logical-slot-copy-install-inspection-node node)
  (unless (equal (subseq expected 0 2)
                 (subseq (logical-slot-copy-install-node-identity node) 0 2))
    (error "logical installation inspection differs from its recorded inode"))
  node)

(defun call-with-logical-slot-copy-install-inspection (operation function)
  (logical-slot-copy-install-check-operation operation)
  (let* ((fd (logical-slot-copy-install-operation-state-root-fd
              (logical-slot-copy-install-operation-record operation)))
         (root (%make-logical-slot-copy-install-node
                :fd fd :kind :root
                :identity (logical-slot-copy-install-file-identity (physical-image-file-facts fd))))
         (scope (%make-logical-slot-copy-install-inspection
                 :operation operation :root root :active t))
         (*logical-slot-copy-install-inspection* scope) (completed nil) (close-error nil))
    (unwind-protect
         (multiple-value-prog1
             (progn (logical-slot-copy-install-check-inspection) (funcall function root))
           (logical-slot-copy-install-check-inspection :closed t) (setf completed t))
      (dolist (node (copy-list (logical-slot-copy-install-inspection-nodes scope)))
        (handler-case (logical-slot-copy-install-inspection-close node)
          (error (condition) (unless close-error (setf close-error condition)))))
      (when (and completed (not close-error))
        (handler-case (logical-slot-copy-install-check-inspection :closed t)
          (error (condition) (setf close-error condition))))
      (setf (logical-slot-copy-install-inspection-active scope) nil)
      (when close-error
        (if completed (error close-error)
            (ignore-errors (report-query-temp-cleanup-failure "installation inspection descriptors" close-error)))))))

(in-package #:pg)

;;; Hold and check publication descriptors until the scoped ledger is drained.
(defun logical-slot-copy-install-publisher-open (publisher parent name kind &key missing (group (logical-slot-copy-install-publisher-root publisher)))
  "Open a child under a held parent and register its descriptor before checks.
Use the current root mount group by default; pg_replslot may establish its own
group.  Retain file facts for rechecking."
  (declare (notinline physical-image-openat sb-posix:close))
  (logical-slot-copy-install-check-publisher publisher)
  (unless (and (or (eq parent (logical-slot-copy-install-publisher-root publisher))
                   (member parent (logical-slot-copy-install-publisher-nodes publisher) :test #'eq))
               (not (eq :file (logical-slot-copy-install-node-kind parent)))
               (physical-adoption-install-relative-p name) (not (find #\/ name))
               (member kind '(:file :directory :slot-root))
               (if (eq kind :slot-root)
                   (and (null group) (eq parent (logical-slot-copy-install-publisher-root publisher))
                        (equal name "pg_replslot"))
                   (and group
                        (or (eq group (logical-slot-copy-install-publisher-root publisher))
                            (member group (logical-slot-copy-install-publisher-nodes publisher) :test #'eq)))))
    (error "invalid logical installation publisher leaf"))
  (let ((fd nil) (registered nil))
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
                   (return-from logical-slot-copy-install-publisher-open nil)
                   (error "cannot open logical installation publisher inode")))
             (push fd (logical-slot-copy-install-publisher-descriptors publisher))
             (setf registered t)
             (let* ((identity (logical-slot-copy-install-file-identity (physical-image-file-facts fd)))
                    (group-identity (when group (logical-slot-copy-install-node-identity group)))
                    (node (%make-logical-slot-copy-install-node
                           :fd fd :parent parent :name (copy-seq name) :kind kind :identity identity)))
               (push node (logical-slot-copy-install-publisher-nodes publisher))
               (when (eq kind :file)
                 (push (list fd (physical-image-file-facts fd)) (logical-slot-copy-install-publisher-files publisher)))
               (unless (or (null group-identity)
                           (and (= (first identity) (first group-identity))
                                (= (third identity) (third group-identity))))
                 (error "logical installation publisher crosses its current mount group"))
               (logical-slot-copy-install-check-publisher publisher)
               node))
        (when (and fd (not registered)) (sb-posix:close fd))))))

(defun logical-slot-copy-install-publisher-sync (publisher node point)
  "Sync a registered inode and recheck publication bindings."
  (declare (notinline sb-posix:fsync))
  (logical-slot-copy-install-check-publisher publisher)
  (unless (or (eq node (logical-slot-copy-install-publisher-root publisher))
              (member node (logical-slot-copy-install-publisher-nodes publisher) :test #'eq))
    (error "logical installation publisher cannot sync an unregistered inode"))
  (sb-posix:fsync (logical-slot-copy-install-node-fd node))
  (logical-slot-copy-install-publisher-point publisher point))

(defun logical-slot-copy-install-publisher-close (publisher node)
  "Remove an owned leaf from the ledger and close it once with interrupts masked.
On Linux, do not retry a failed close: the descriptor number may be reused."
  (declare (notinline sb-posix:close))
  (logical-slot-copy-install-check-publisher publisher)
  (unless (and (member node (logical-slot-copy-install-publisher-nodes publisher) :test #'eq)
               (not (find node (logical-slot-copy-install-publisher-nodes publisher)
                          :key #'logical-slot-copy-install-node-parent :test #'eq)))
    (error "logical installation publisher can close only an owned leaf"))
  (sb-sys:without-interrupts
    (let ((fd (logical-slot-copy-install-node-fd node)))
      (setf (logical-slot-copy-install-publisher-nodes publisher)
            (delete node (logical-slot-copy-install-publisher-nodes publisher) :test #'eq)
            (logical-slot-copy-install-publisher-files publisher)
            (delete fd (logical-slot-copy-install-publisher-files publisher) :key #'first)
            (logical-slot-copy-install-publisher-descriptors publisher)
            (delete fd (logical-slot-copy-install-publisher-descriptors publisher)))
      (sb-posix:close fd))))

(defun logical-slot-copy-install-publisher-path (node)
  (let ((parts nil))
    (loop for current = node then (logical-slot-copy-install-node-parent current)
          while (logical-slot-copy-install-node-parent current)
          do (push (logical-slot-copy-install-node-name current) parts))
    (format nil "~{~a~^/~}" parts)))

(defun logical-slot-copy-install-publisher-observations (publisher)
  "Capture root-relative paths and inode facts before closing descriptors."
  (logical-slot-copy-install-check-publisher publisher)
  (mapcar (lambda (node)
            (list (logical-slot-copy-install-publisher-path node)
                  (physical-image-file-facts (logical-slot-copy-install-node-fd node))))
          (logical-slot-copy-install-publisher-nodes publisher)))

(defun logical-slot-copy-install-publisher-check-closed (publisher observations phase prefix)
  "Recheck the input, phase, prefix, and path bindings after descriptor cleanup."
  (let ((root (logical-slot-copy-install-publisher-root publisher)))
    (logical-slot-copy-install-check-input
     (logical-slot-copy-install-publisher-owner publisher)
     (logical-slot-copy-install-publisher-context publisher)
     (logical-slot-copy-install-publisher-input publisher))
    (let ((record (logical-slot-copy-install-publisher-record publisher)))
      (unless (and (eq phase (logical-slot-copy-install-input-state-phase record))
                   (= prefix (logical-slot-copy-install-input-state-prefix record)))
        (error "logical installation journal phase or prefix changed during cleanup")))
    (logical-slot-copy-install-check-node root)
    (dolist (row observations)
      (unless (equal (second row) (physical-image-file-facts
                                   (logical-slot-copy-install-node-fd root) (first row)))
        (error "logical installation publication changed after descriptor cleanup")))
    (logical-slot-copy-install-publisher-check-closed-role publisher)))

(defun call-with-logical-slot-copy-install-publisher (owner context input function &key (constructor #'%make-logical-slot-copy-install-publisher))
  "Call FUNCTION with scoped publication authority backed by the retained input proof.
Drain every owned descriptor once with interrupts masked, then recheck its path
facts.  CONSTRUCTOR selects the publisher role while reusing this scope.
Preserve an original error if close also fails.  Failure, including a caught
check failure or cleanup failure, revokes the input and fences the operation.
A constructor failure after input authentication also revokes that input; a
forged input is rejected before entering this failure envelope."
  (declare (notinline sb-posix:close))
  (let ((record (logical-slot-copy-install-input-record owner context input)))
    (when *logical-slot-copy-install-publishers*
      (error "logical installation publisher is already active"))
    (let ((returned nil))
      (unwind-protect
           (multiple-value-prog1
               (let* ((operation (logical-slot-copy-install-input-state-operation record))
                      (fd (logical-slot-copy-install-operation-state-root-fd
                           (logical-slot-copy-install-operation-record operation)))
                      (publisher (funcall constructor
                                          :owner owner :context context :input input :record record :operation operation
                                          :image (copy-seq (logical-slot-copy-install-input-state-image record))
                                          :root (%make-logical-slot-copy-install-node
                                                 :fd fd :kind :root
                                                 :identity (logical-slot-copy-install-file-identity (physical-image-file-facts fd)))))
                      (*logical-slot-copy-install-publishers* (list publisher))
                      (completed nil) (body-completed nil) (close-error nil) (observations nil) (phase nil) (prefix nil))
                 (unwind-protect
                      (multiple-value-prog1
                          (unwind-protect
                               (multiple-value-prog1
                                   (progn (logical-slot-copy-install-publisher-proof publisher) (funcall function publisher))
                                 (setf observations (logical-slot-copy-install-publisher-observations publisher)
                                       phase (logical-slot-copy-install-input-state-phase record)
                                       prefix (logical-slot-copy-install-input-state-prefix record)
                                       body-completed t))
                            (sb-sys:without-interrupts
                              (loop while (logical-slot-copy-install-publisher-descriptors publisher)
                                    for owned = (pop (logical-slot-copy-install-publisher-descriptors publisher))
                                    do (handler-case (sb-posix:close owned)
                                         (error (condition) (unless close-error (setf close-error condition))))))

                            (when close-error
                              (if body-completed (error close-error)
                                  (ignore-errors (report-query-temp-cleanup-failure "installation publisher descriptors" close-error)))))
                        (logical-slot-copy-install-publisher-check-closed publisher observations phase prefix)
                        (setf completed t))
                   (unless completed (logical-slot-copy-install-revoke-publisher publisher))
                   (setf (logical-slot-copy-install-publisher-failed publisher) t)))
             (setf returned t))
        (unless returned (logical-slot-copy-install-revoke-input record))))))

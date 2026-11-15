(in-package #:pg)

(defun logical-slot-imported-check-install-directory (fd)
  (unless (equal '("active" "journal") (logical-slot-copy-import-control-names fd 3))
    (error "imported logical source installation is not a terminal pair"))
  t)

(defun logical-slot-imported-check-held-file (parent name fd facts bytes)
  (unless (and (equal facts (physical-image-file-facts fd))
               (equal facts (physical-image-file-facts parent name))
               (equalp bytes (logical-slot-copy-import-control-read-fd fd (length bytes))))
    (error "imported logical source lost a canonical file binding"))
  t)

(defun logical-slot-imported-check-file-anchors
    (root named-root root-facts held &key canonical mutable-root)
  (let ((observed (physical-image-file-facts root))
        (named (physical-image-file-facts -100 named-root)))
    (unless (if mutable-root
                (let ((identity (logical-slot-copy-source-proof-directory-identity root-facts)))
                  (and (equal identity (logical-slot-copy-source-proof-directory-identity observed))
                       (equal identity (logical-slot-copy-source-proof-directory-identity named))))
                (and (equal root-facts observed) (equal root-facts named)))
      (error "imported logical source root descriptor changed")))
  (dolist (entry held)
    (destructuring-bind (parent name fd facts bytes) entry
      (unless (and (equal facts (physical-image-file-facts fd))
                   (equal facts (physical-image-file-facts parent name)))
        (error "imported logical source lost a named file binding"))
      (cond ((and bytes canonical)
             (logical-slot-imported-check-held-file parent name fd facts bytes))
            ((null bytes) (logical-slot-imported-check-install-directory fd)))))
  (dolist (name '("logical-slot-copy.import.tmp" "logical-slot-copy.source-proof.tmp"))
    (when (slot-state-lstat (slot-state-fd-path root name))
      (error "imported logical source has unfinished metadata")))
  t)

(defun logical-slot-imported-check-certificate-source (owner context facts &key runtime)
  (let* ((root (physical-image-file-facts (physical-image-context-root-fd context)))
         (certificate (getf facts :certificate)))
    (unless (and (eql (first root) (getf certificate :device))
                 (eql (second root) (getf certificate :inode)))
      (error "imported logical source differs from the permanent certificate root"))
    (sb-thread:with-mutex (*wal-lock*)
      (multiple-value-bind (slot system-id history end)
          (logical-slot-imported-owner-source-under-lock owner)
        (let ((proof-end (if runtime (logical-slot-imported-scan-proof-horizon-under-lock owner) end)))
          (unless (and (eql system-id (getf certificate :system-id))
                       (<= (getf certificate :fork) proof-end))
            (error "imported logical source differs from the permanent certificate identity"))
          (logical-slot-imported-check-origin-history
           (getf certificate :child-timeline) (getf certificate :parent-timeline)
           (getf certificate :fork) (getf (getf facts :evidence) :child-history) history)
          (when slot
            (let ((row (find (wal-replication-slot-name slot) (getf certificate :slots)
                             :key #'first :test #'equal)))
              (unless (and row (fourth row))
                (error "imported logical source differs from the permanent certificate"))
              (logical-slot-imported-check-state
               slot (logical-slot-state-decode (string->bytes (fourth row)) (first row))
               history proof-end))))))
    t))

(defun call-with-logical-slot-imported-proof-file-scope (owner context function &key runtime)
  "Observe the canonical permanent pair through an explicit read-only context.
Keep *PGDATA* and *PHYSICAL-IMAGE-CONTEXT* bindings intact for startup
authority checks. The named PGDATA path must still bind to this held context.
The private callback scope supplies a full check outside WAL and a lighter
named-FD anchor check under WAL. It does not acquire a retained-root lease,
replay D through F, provide WAL records, publish a slot or establish readiness."
  (when (and runtime (not (logical-slot-imported-scan-owner-p owner)))
    (error "mutable imported proof roots require a runtime scan owner"))
  (call-with-logical-slot-imported-owner-check
   owner
   (lambda ()
     (logical-slot-imported-check-owner owner)
     (physical-image-check-context context)
     (unless (physical-image-read-context-p context)
       (error "imported logical source requires a read-only root context"))
     (let* ((root (physical-image-context-root-fd context))
            (root-facts (physical-image-file-facts root))
            (named-root (namestring (physical-adoption-owner-directory owner)))
            (held nil) (private-facts nil) (private-image nil) (active nil)
            (thread sb-thread:*current-thread*))
       (unless (and (= (first root-facts) (physical-adoption-owner-device owner))
                    (= (second root-facts) (physical-adoption-owner-inode owner))
                    (equal root-facts (physical-image-file-facts -100 named-root)))
         (error "imported logical source context differs from its owned PGDATA"))
       (labels ((hold (parent name directory-p bytes next)
                  (call-with-logical-slot-copy-source-proof-fd
                   parent name directory-p
                   (lambda (fd facts)
                     (let ((record (list parent name fd facts (and bytes (copy-seq bytes)))))
                       (sb-sys:without-interrupts
                         (unwind-protect
                              (progn
                                (push record held)
                                (sb-sys:with-local-interrupts
                                  (when bytes (logical-slot-imported-check-held-file parent name fd facts bytes))
                                  (funcall next fd)))
                           (when (eq record (first held)) (pop held))))))))
                (require-scope ()
                  (unless (and active (eq thread sb-thread:*current-thread*))
                    (error "imported logical proof file scope ended or changed thread")))
                (check-anchors-under-lock ()
                  (assert (sb-thread:holding-mutex-p *wal-lock*))
                  (call-with-logical-slot-imported-owner-check
                   owner
                   (lambda ()
                     (require-scope)
                     (logical-slot-imported-owner-under-lock owner)
                     (when runtime (logical-slot-imported-scan-proof-horizon-under-lock owner))
                     (logical-slot-imported-check-file-anchors root named-root root-facts held :mutable-root runtime))))
                ;; CHECK with T returns a copy of the exact proof image and
                ;; the names and copied facts of its held FDs for binding.
                (check (&optional binding)
                  (when (sb-thread:holding-mutex-p *wal-lock*)
                    (error "full imported proof check requires the WAL mutex to be released"))
                  (call-with-logical-slot-imported-owner-check
                   owner
                   (lambda ()
                     (require-scope)
                     (logical-slot-imported-check-owner owner)
                     (physical-image-check-context context)
                     (logical-slot-imported-check-file-anchors
                      root named-root root-facts held :canonical t :mutable-root runtime)
                     (logical-slot-imported-check-certificate-source owner context private-facts :runtime runtime)
                     (if binding
                         (list (copy-seq private-image)
                               (mapcar (lambda (entry)
                                         (list (second entry) (copy-tree (fourth entry)))) held))
                         t)))))
         (hold root "logical-slot-copy.source-proof" nil nil
               (lambda (proof-fd)
                 (let* ((image (logical-slot-copy-import-control-read-fd
                                proof-fd (logical-slot-copy-source-proof-maximum-bytes)))
                        (parts (logical-slot-copy-source-proof-decode image))
                        (facts (logical-slot-copy-check-source-proof image))
                        (certificate (getf facts :certificate))
                        (journal (getf parts :installation))
                        (pending (getf (getf facts :journal) :certificate))
                        (active-image (logical-slot-copy-install-active-image certificate)))
                   (setf (fifth (first held)) (copy-seq image)
                         private-facts facts private-image (copy-seq image))
                   (logical-slot-imported-check-certificate-source owner context facts :runtime runtime)
                   (hold root "logical-slot-copy.import" nil active-image
                         (lambda (active-fd)
                           (declare (ignore active-fd))
                           (hold root "logical-slot-copy.install" t nil
                                 (lambda (installation)
                                   (logical-slot-imported-check-install-directory installation)
                                   (hold installation "journal" nil journal
                                         (lambda (journal-fd)
                                           (declare (ignore journal-fd))
                                           (hold installation "active" nil pending
                                                 (lambda (pending-fd)
                                                   (declare (ignore pending-fd))
                                                   (unwind-protect
                                                        (progn
                                                          (setf active t)
                                                          (check)
                                                          (multiple-value-prog1
                                                              (funcall function
                                                                       (logical-slot-copy-check-source-proof (copy-seq image))
                                                                       #'check #'check-anchors-under-lock)
                                                            (check)))
                                                     (setf active nil))))))))))))))))))

(defun call-with-logical-slot-imported-proof-files (owner context function)
  (call-with-logical-slot-imported-proof-file-scope owner context function))

(defun call-with-logical-slot-imported-runtime-proof-files (owner context function)
  (unless (logical-slot-imported-scan-owner-p owner)
    (error "runtime proof files require their registered scan owner"))
  (call-with-logical-slot-imported-proof-file-scope owner context function :runtime t))

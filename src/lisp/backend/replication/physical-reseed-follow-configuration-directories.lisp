(in-package #:pg)

;; Hold the installed base, global and WAL directories by descriptor while
;; checking their bindings to the root and the original installation plan.
(defun call-with-physical-reseed-follow-base-directories (root-fd value-plan function parent-check)
  (let* ((plan (physical-reseed-install-copy value-plan))
         (local (getf (getf plan :attempt) :local))
         (mount (physical-adoption-install-mount-id nil :directory-fd root-fd))
         (children nil) (active nil))
    (labels ((check ()
               (unless active (error "follow base directory scope has ended"))
               (funcall parent-check)
               (let ((root (physical-image-check-directory root-fd)))
                 (unless (and (= (getf local :device) (sb-posix:stat-dev root))
                              (= (getf local :inode) (sb-posix:stat-ino root)))
                   (error "follow base root differs from the install plan")))
               (dolist (entry children)
                 (destructuring-bind (role name fd) entry
                   (declare (ignore role))
                   (let ((expected (assoc name (getf plan :new) :test #'equal))
                         (current (physical-reseed-install-old-observe root-fd name mount)))
                     (unless (and (equal (subseq expected 0 6) (subseq current 0 6))
                                  (equal (physical-reseed-install-row-file-facts current)
                                         (physical-reseed-file-facts (physical-image-check-directory fd)))
                                  (= mount (physical-adoption-install-mount-id nil :directory-fd fd)))
                       (error "follow base child differs from its held named directory")))))))
      (unwind-protect
           (progn
             (dolist (entry '((:base "base") (:global "global") (:wal "pg_wal")))
               (funcall parent-check)
               (destructuring-bind (role name) entry
                 (sb-sys:without-interrupts
                   (let ((fd nil))
                     (unwind-protect
                          (progn
                            (setf fd (physical-image-openat
                                      root-fd name
                                      (logior sb-posix:o-rdonly sb-posix:o-directory
                                              sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
                            (when (minusp fd) (setf fd nil) (error "cannot open follow base child directory"))
                            (push (list role name fd) children)
                            (setf fd nil))
                       (when fd (sb-posix:close fd)))))))
             (setf children (nreverse children) active t)
             (check)
             (multiple-value-prog1
                 (funcall function (mapcar (lambda (entry) (cons (first entry) (third entry))) children) #'check)
               (check)))
        (setf active nil)
        (dolist (entry children) (sb-posix:close (third entry)))))))

;; Duplicate the borrowed root and child descriptors for a read-only image
;; context.  Close only those duplicates when the callback exits.
(defun call-with-physical-reseed-follow-held-base-directory
    (root-fd base-fd global-fd wal-fd function parent-check)
  (call-with-physical-image-owned-context
   root-fd t
   (lambda (context)
     (let ((borrowed (list (cons "base" base-fd) (cons "global" global-fd) (cons "pg_wal" wal-fd)))
           (mount (physical-adoption-install-mount-id nil :directory-fd root-fd)))
       (labels ((check ()
                  (physical-image-check-context context)
                  (funcall parent-check)
                  (let ((root (physical-image-context-root-fd context)))
                    (unless (and (equal (physical-reseed-file-facts (physical-image-check-directory root))
                                        (physical-reseed-file-facts (physical-image-check-directory root-fd)))
                                 (= mount (physical-adoption-install-mount-id nil :directory-fd root)
                                    (physical-adoption-install-mount-id nil :directory-fd root-fd)))
                      (error "held base read root changed its descriptor binding"))
                    (physical-image-check-children context)
                    (dolist (entry (physical-image-context-children context))
                      (let* ((name (car entry)) (fd (cdr entry))
                                                (source (cdr (assoc name borrowed :test #'equal)))
                                                (held (physical-reseed-file-facts (physical-image-check-directory fd)))
                                                (named (physical-reseed-install-old-observe root name mount)))
                        (unless (and (equal held (physical-reseed-file-facts (physical-image-check-directory source)))
                                     (equal held (physical-reseed-install-row-file-facts named))
                                     (= mount (physical-adoption-install-mount-id nil :directory-fd fd)
                                        (physical-adoption-install-mount-id nil :directory-fd source)))
                          (error "held base read child changed its named descriptor binding")))))
                  (funcall parent-check)))
         (dolist (entry borrowed)
           (sb-sys:without-interrupts
             (let ((fd nil))
               (unwind-protect
                    (progn
                      (setf fd (sb-posix:fcntl (cdr entry) 1030 0))
                      (physical-image-register-child context (car entry) fd)
                      (setf fd nil))
                 (when fd (sb-posix:close fd))))))
         (let ((*physical-image-context* context))
           (check)
           (multiple-value-prog1 (funcall function (physical-image-context-root-path context))
             (check))))))))

(defun call-with-physical-reseed-follow-fixed-configuration-directories
    (parent value function parent-check)
  (let* ((record (physical-reseed-follow-copy value))
         (configuration (getf record :configuration))
         (expected (getf configuration :directories))
         (workspace (getf configuration :workspace))
         (children nil) (active nil))
    (unless (member (getf configuration :phase) '(:staged :installing :inert))
      (error "fixed configuration directories require staged intent"))
    (labels ((check ()
               (unless active (error "fixed configuration directory scope has ended"))
               (funcall parent-check)
               (let ((stat (physical-image-check-directory parent)))
                 (unless (and (= (getf workspace :device) (sb-posix:stat-dev stat))
                              (= (getf workspace :inode) (sb-posix:stat-ino stat)))
                   (error "fixed configuration workspace differs from its saved identity")))
               (dolist (entry children)
                 (destructuring-bind (role name fd) entry
                   (unless (equal (getf expected role)
                                  (physical-reseed-follow-configuration-child-fact parent name fd))
                     (error "fixed configuration child differs from its saved identity"))))))
      (unwind-protect
           (progn
             (dolist (entry '((:staged "staged") (:saved "saved")))
               (funcall parent-check)
               (destructuring-bind (role name) entry
                 (sb-sys:without-interrupts
                   (let ((fd nil))
                     (unwind-protect
                          (progn
                            (setf fd (physical-image-openat
                                      parent name
                                      (logior sb-posix:o-rdonly sb-posix:o-directory
                                              sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
                            (when (minusp fd) (setf fd nil) (error "cannot open fixed configuration directory"))
                            (push (list role name fd) children)
                            (setf fd nil))
                       (when fd (sb-posix:close fd)))))))
             (setf active t)
             (check)
             (multiple-value-prog1
                 (funcall function (third (assoc :staged children)) (third (assoc :saved children)) #'check)
               (check)))
        (setf active nil)
        (dolist (entry children) (sb-posix:close (third entry)))))))

;; Staged permits prefix zero; Installing permits the journal step or one
;; move ahead; Inert requires the complete prefix of four or five moves.
(defun physical-reseed-follow-configuration-initial-prefix
    (root-fd workspace-fd staged-fd saved-fd value-plan value extra)
  (multiple-value-bind (plan record) (physical-reseed-follow-configuration-install-plan value-plan value)
    (let* ((configuration (getf record :configuration))
           (maximum (if (physical-reseed-follow-configuration-selected-p
                         (physical-reseed-follow-core-projection record)) 5 4))
           (step (getf configuration :step))
           (allowed (ecase (getf configuration :phase)
                      (:staged (list 0))
                      (:installing (if (< step maximum) (list step (1+ step)) (list step)))
                      (:inert (list maximum)))))
      (flet ((names ()
               (list :root (physical-reseed-install-directory-names root-fd)
                     :workspace (physical-reseed-install-directory-names workspace-fd)
                     :staged (physical-reseed-install-directory-names staged-fd)
                     :saved (physical-reseed-install-directory-names saved-fd))))
        (let* ((actual (names))
               (matches (remove-if-not
                         (lambda (prefix)
                           (equal actual (physical-reseed-follow-configuration-prefix-signature
                                          plan record prefix extra)))
                         allowed)))
          (unless (and (= 1 (length matches)) (equal actual (names)))
            (error "configuration names disagree with their unique allowed initial prefix"))
          (first matches))))))

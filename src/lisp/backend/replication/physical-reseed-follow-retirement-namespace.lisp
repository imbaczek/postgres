(in-package #:pg)

(defun physical-reseed-follow-retirement-final-extra-at (root snapshot &optional pid-evidence)
  (physical-reseed-follow-check-snapshot root snapshot)
  (if pid-evidence
      (funcall (getf pid-evidence :verify))
      (when (physical-adoption-path-present-p (merge-pathnames "postmaster.pid" (physical-image-fd-path root)))
        (error "retirement read-only layout refuses temporary follow or PID entries")))
  (list (cons (copy-seq "physical-reseed.follow")
              (nth-value 1 (physical-reseed-follow-entry root "physical-reseed.follow")))))

(defun physical-reseed-follow-retirement-extra-at (root snapshot)
  ;; The strict entry admits the final journal alone and refuses a temporary.
  (let ((extra (physical-reseed-follow-retirement-final-extra-at root snapshot)))
    (when (physical-adoption-path-present-p
           (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root)))
      (error "retirement read-only layout refuses temporary follow or PID entries"))
    extra))

(defun physical-reseed-follow-retirement-initial-prefix (descriptors plan record binding extra)
  ;; Compare all ten directory inventories twice and require one matching
  ;; archive prefix consistent with the recorded retirement progress.
  (flet ((names ()
           (loop for role in '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive)
                 append (list role (physical-reseed-install-directory-names
                                    (physical-reseed-follow-configuration-role-fd descriptors role))))))
    (let* ((actual (names))
           (matches (loop for prefix from 0 to 2
                          when (equal actual (physical-reseed-follow-retirement-inventories
                                              plan record binding prefix extra))
                            collect prefix)))
      (unless (and (= 1 (length matches)) (equal actual (names)))
        (error "retirement layout lacks a unique complete archive prefix"))
      (physical-reseed-follow-retirement-check-prefix record (first matches)))))

(defun physical-reseed-follow-retirement-layout-state-at (descriptors snapshot check extra-reader candidate &optional pid-evidence)
  ;; The final snapshot remains the layout authority.  The extra reader and
  ;; checks keep admitted evidence stable.
  ;; Use the provisional install read only to obtain a plan.  Prove the saved
  ;; role, complete images and exact prefix, then recheck both journals around
  ;; the layout proof against the actual follow snapshot.
  (let* ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (record (physical-reseed-follow-copy (getf snapshot :record)))
         (pid-entry (getf pid-evidence :entry)))
    (funcall check)
    (when pid-evidence (funcall (getf pid-evidence :verify)))
    (physical-reseed-follow-check-snapshot root snapshot)
    (let* ((plan (physical-reseed-follow-retirement-provisional-plan-at root saved record))
           (base-fds (loop for role in '(:base :global :wal)
                           collect (cons role (physical-reseed-follow-configuration-role-fd descriptors role)))))
      (flet ((initial-check ()
               (funcall check)
               (funcall extra-reader root)))
        (multiple-value-bind (base control binding model images row)
            (physical-reseed-follow-configuration-base-proof
             root saved base-fds plan record (getf (getf record :configuration) :step) #'initial-check)
          (declare (ignore base control model row))
          (let* ((extra (funcall extra-reader root))
                 (prefix (physical-reseed-follow-retirement-initial-prefix descriptors plan record binding extra))
                 (install (physical-reseed-follow-retirement-install-at descriptors record prefix))
                 (attempt (physical-reseed-follow-retirement-attempt-at root (if (= prefix 2) saved root) record prefix))
                 (positions (physical-reseed-follow-retirement-positions record binding prefix))
                 (expected (physical-reseed-follow-retirement-inventories plan record binding prefix extra))
                 (mount (physical-adoption-install-mount-id nil :directory-fd root)))
            (unless (and (equal (mapcar #'car extra)
                                (append '("physical-reseed.follow")
                                        (when candidate '("physical-reseed.follow.tmp"))
                                        (when pid-entry '("postmaster.pid"))))
                         (equal pid-entry (assoc "postmaster.pid" extra :test #'equal)))
              (error "retirement observation has unexpected root extras"))
            (flet ((verify ()
                     (funcall check)
                     (when pid-evidence (funcall (getf pid-evidence :verify)))
                     (unless (and (equal extra (funcall extra-reader root))
                                  (equal install (physical-reseed-follow-retirement-install-at descriptors record prefix))
                                  (equal attempt (physical-reseed-follow-retirement-attempt-at
                                                  root (if (= prefix 2) saved root) record prefix)))
                       (error "retirement journals changed during layout verification"))))
              (verify)
              (let ((layout
                      (physical-reseed-follow-layout-proof-at
                       descriptors plan record positions images extra expected mount #'verify check
                       extra-reader
                       (lambda () (physical-reseed-follow-activation-active-row
                                   descriptors record binding positions mount)))))
                (when candidate
                  (physical-reseed-follow-retirement-check-temporary-layout
                   descriptors record candidate prefix install attempt))
                (verify)
                (append (list :prefix prefix :layout layout :install install :attempt attempt
                              :snapshot (physical-adoption-attempt-value-copy snapshot))
                        (when candidate
                          (list :temporary (physical-reseed-follow-copy candidate))))))))))))

(defun physical-reseed-follow-retirement-layout-state (descriptors snapshot check)
  ;; Keep the strict layout policy for callers without a held temporary.
  (physical-reseed-follow-retirement-layout-state-at
   descriptors snapshot check
   (lambda (root) (physical-reseed-follow-retirement-extra-at root snapshot))
   nil))

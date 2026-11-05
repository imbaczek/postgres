(in-package #:pg)

(defun physical-reseed-follow-retirement-extra-at (root snapshot)
  ;; Recheck the follow snapshot and fence the follow temporary and PID.
  ;; The complete layout also refuses unexplained temporary entries.
  (physical-reseed-follow-check-snapshot root snapshot)
  (dolist (name '("physical-reseed.follow.tmp" "postmaster.pid"))
    (when (physical-adoption-path-present-p (merge-pathnames name (physical-image-fd-path root)))
      (error "retirement read-only layout refuses temporary follow or PID entries")))
  (list (cons (copy-seq "physical-reseed.follow")
              (nth-value 1 (physical-reseed-follow-entry root "physical-reseed.follow")))))

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

(defun physical-reseed-follow-retirement-layout-state (descriptors snapshot check)
  ;; Use the provisional install read only to obtain a plan.  Prove the saved
  ;; role, complete images and exact prefix, then recheck both journals around
  ;; the layout proof against the actual follow snapshot.
  ;; The descriptors and check belong to the caller's scope.  This first entry
  ;; refuses temporaries and PID entries, and does not acquire guards or prove
  ;; receive-store semantics or startup authority.
  (let* ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (record (physical-reseed-follow-copy (getf snapshot :record)))
         (present (loop for fd in (list root saved)
                        when (physical-adoption-path-present-p
                              (merge-pathnames "physical-reseed.install" (physical-image-fd-path fd)))
                          collect fd)))
    (funcall check)
    (physical-reseed-follow-check-snapshot root snapshot)
    (unless (= 1 (length present)) (error "retirement install requires one root or saved location"))
    (let* ((provisional (if (= root (first present))
                            (physical-reseed-install-read-at root "physical-reseed.install")
                            (physical-reseed-install-read-archive-at root saved)))
           (plan (getf provisional :plan))
           (base-fds (loop for role in '(:base :global :wal)
                           collect (cons role (physical-reseed-follow-configuration-role-fd descriptors role)))))
      (physical-reseed-follow-configuration-install-plan plan record)
      (flet ((initial-check ()
               (funcall check)
               (physical-reseed-follow-retirement-extra-at root snapshot)))
        (multiple-value-bind (base control binding model images row)
            (physical-reseed-follow-configuration-base-proof
             root saved base-fds plan record (getf (getf record :configuration) :step) #'initial-check)
          (declare (ignore base control model row))
          (let* ((extra (physical-reseed-follow-retirement-extra-at root snapshot))
                 (prefix (physical-reseed-follow-retirement-initial-prefix descriptors plan record binding extra))
                 (install (physical-reseed-follow-retirement-install-at descriptors record prefix))
                 (attempt (physical-reseed-follow-retirement-attempt-at root (if (= prefix 2) saved root) record prefix))
                 (positions (physical-reseed-follow-retirement-positions record binding prefix))
                 (expected (physical-reseed-follow-retirement-inventories plan record binding prefix extra))
                 (mount (physical-adoption-install-mount-id nil :directory-fd root)))
            (flet ((verify ()
                     (funcall check)
                     (unless (and (equal extra (physical-reseed-follow-retirement-extra-at root snapshot))
                                  (equal install (physical-reseed-follow-retirement-install-at descriptors record prefix))
                                  (equal attempt (physical-reseed-follow-retirement-attempt-at
                                                  root (if (= prefix 2) saved root) record prefix)))
                       (error "retirement journals changed during layout verification"))))
              (verify)
              (let ((layout
                      (physical-reseed-follow-layout-proof-at
                       descriptors plan record positions images extra expected mount #'verify check
                       (lambda (fd) (physical-reseed-follow-retirement-extra-at fd snapshot))
                       (lambda () (physical-reseed-follow-activation-active-row
                                   descriptors record binding positions mount)))))
                (verify)
                (list :prefix prefix :layout layout :install install :attempt attempt
                      :snapshot (physical-adoption-attempt-value-copy snapshot))))))))))
(in-package #:pg)

(defun physical-reseed-follow-retirement-final-extra-at (root snapshot &optional pid-evidence)
  ;; Verify supplied PID evidence.  Without it, keep the strict PID fence.
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

(defun physical-reseed-follow-retirement-inventories-at (plan record binding prefix extra alias)
  ;; An alias adds the saved name only for the pending journal at the final
  ;; recorded step.  The prefix still counts completed moves.
  (let ((inventories (physical-reseed-follow-retirement-inventories plan record binding prefix extra)))
    (when alias
      (let ((kind (physical-reseed-follow-retirement-pending-journal record)))
        (unless (and kind (= prefix (getf (getf record :retirement) :step)))
          (error "retirement alias requires the recorded pending move"))
        (push (if (eq kind :install) "physical-reseed.install" "physical-reseed.attempt")
              (getf inventories :saved))))
    (physical-reseed-follow-configuration-sort-inventories inventories)))

(defun physical-reseed-follow-retirement-journals-at (descriptors record prefix alias check)
  ;; Use alias evidence only for the recorded pending move.  Read the other
  ;; journal through its ordinary single-link observer.
  (funcall check)
  (let* ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (kind (when alias (physical-reseed-follow-retirement-pending-journal record)))
         (state (when alias
                  (unless (and kind (= prefix (getf (getf record :retirement) :step)))
                    (error "retirement alias requires the recorded pending move"))
                  (physical-reseed-follow-retirement-alias-at root saved record check))))
    (values
     (if (eq kind :install) state
         (physical-reseed-follow-retirement-install-at descriptors record prefix))
     (if (eq kind :attempt) state
         (physical-reseed-follow-retirement-attempt-at root (if (= prefix 2) saved root) record prefix)))))

(defun physical-reseed-follow-retirement-initial-prefix (descriptors plan record binding extra)
  ;; Match the complete inventories, including the one permitted alias shape.
  ;; A next temporary does not select or authorize another move.
  (flet ((names ()
           (loop for role in '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive)
                 append (list role (physical-reseed-install-directory-names
                                    (physical-reseed-follow-configuration-role-fd descriptors role))))))
    (let* ((actual (names))
           (pending (physical-reseed-follow-retirement-pending-journal record))
           (step (getf (getf record :retirement) :step))
           (matches
             (append
              (loop for prefix from 0 to 2
                    when (equal actual (physical-reseed-follow-retirement-inventories
                                        plan record binding prefix extra))
                      collect (list prefix nil))
              (when (and pending
                         (equal actual (physical-reseed-follow-retirement-inventories-at
                                        plan record binding step extra t)))
                (list (list step t))))))
      (unless (and (= 1 (length matches)) (equal actual (names)))
        (error "retirement layout lacks a unique complete archive prefix"))
      (physical-reseed-follow-retirement-check-prefix record (caar matches))
      (values (caar matches) (second (first matches))))))

(defun physical-reseed-follow-retirement-layout-state-at (descriptors snapshot check extra-reader candidate &optional pid-evidence)
  ;; When PID evidence is supplied, verify it and admit only its matching root
  ;; entry.  The strict path supplies no PID evidence.
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
    (let* ((plan (physical-reseed-follow-retirement-provisional-plan-at root saved record check))
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
                 (geometry (multiple-value-list
                            (physical-reseed-follow-retirement-initial-prefix descriptors plan record binding extra)))
                 (prefix (first geometry))
                 (alias (second geometry))
                 (journals (multiple-value-list
                            (physical-reseed-follow-retirement-journals-at descriptors record prefix alias check)))
                 (install (first journals))
                 (attempt (second journals))
                 (positions (physical-reseed-follow-retirement-positions record binding prefix))
                 (expected (physical-reseed-follow-retirement-inventories-at plan record binding prefix extra alias))
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
                                  (equal (list install attempt)
                                         (multiple-value-list
                                          (physical-reseed-follow-retirement-journals-at
                                           descriptors record prefix alias check))))
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
                (append (when alias (list :location :alias))
                        (list :prefix prefix :layout layout :install install :attempt attempt
                              :snapshot (physical-adoption-attempt-value-copy snapshot))
                        (when candidate
                          (list :temporary (physical-reseed-follow-copy candidate))))))))))))

(defun physical-reseed-follow-retirement-layout-state (descriptors snapshot check)
  ;; Keep the strict layout policy for callers without a held temporary.
  (physical-reseed-follow-retirement-layout-state-at
   descriptors snapshot check
   (lambda (root) (physical-reseed-follow-retirement-extra-at root snapshot))
   nil))

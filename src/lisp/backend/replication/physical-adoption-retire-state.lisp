;; Validation for crash-resumable retirement of terminal adoption authority.
;;
;; The complete retirement marker permits only an ordered missing prefix of
;; handoff, install, and attempt files.  The follow plan retains the facts needed
;; to validate the remaining namespace, preserved trees, workspaces, WAL, and
;; active recovery state after those files have gone.
(in-package #:pg)

(defun physical-adoption-retire-check-owner (owner)
  (unless (physical-adoption-startup-owner-p owner)
    (error "adoption retirement requires real postmaster startup ownership"))
  (physical-adoption-check-owner owner))

(defun physical-adoption-retire-files (value)
  (let ((handoff (getf value :handoff)))
    (list (list "physical-adoption.handoff" (getf value :handoff-file)
                +physical-adoption-handoff-maximum-bytes+)
          (list "physical-adoption.install" (getf handoff :journal-file)
                +physical-adoption-install-maximum-bytes+)
          (list "physical-adoption.attempt" (getf handoff :attempt-file)
                +physical-adoption-attempt-maximum-bytes+))))

(defun physical-adoption-retire-presence-prefix (presence)
  "Return the length of the missing authority prefix, rejecting any gap."
  (unless (and (listp presence) (= 3 (length presence))
               (every (lambda (value) (member value '(nil t))) presence))
    (error "invalid adoption retirement authority presence"))
  (let ((prefix (or (position t presence) 3)))
    (unless (every #'identity (nthcdr prefix presence))
      (error "adoption retirement authority has a missing non-prefix entry"))
    prefix))

(defun physical-adoption-retire-authority-prefix (owner value mount)
  (let ((root (physical-adoption-owner-directory owner)))
    (physical-adoption-retire-presence-prefix
     (loop for (name expected maximum) in (physical-adoption-retire-files value)
           for path = (merge-pathnames name root)
           for present = (physical-adoption-path-present-p path)
           collect (when present
                     (unless (and (= mount (physical-adoption-install-mount-id path))
                                  (equal expected (physical-adoption-handoff-file-fact path maximum)))
                       (error "adoption retirement authority file changed"))
                     t)))))

(defun physical-adoption-retire-check-context (owner value)
  (let* ((plan (getf (getf value :handoff) :installation))
         (mount (physical-adoption-install-check-directories owner plan)))
    (physical-adoption-check-preserved-trees plan)
    (physical-adoption-follow-check-workspaces owner value mount)
    (unless (= mount (physical-adoption-install-mount-id
                      (physical-adoption-follow-path (physical-adoption-owner-directory owner))))
      (error "adoption retirement journal crosses a mount"))
    mount))

(defun physical-adoption-retire-namespace-p (owner state prefix mount)
  (let* ((value (getf state :plan))
         (positions (physical-adoption-follow-positions value (getf state :step)))
         (removed (mapcar #'first (subseq (physical-adoption-retire-files value) 0 prefix)))
         (names (remove-if (lambda (name) (member name removed :test #'equal))
                           (physical-adoption-follow-root-names value positions owner))))
    (and (physical-adoption-install-names-p (physical-adoption-owner-directory owner) names
                                            :local-workspace t)
         (physical-adoption-follow-position-namespace-p value positions mount))))

(defun physical-adoption-retire-validate (owner state)
  "Revalidate a complete marker, its cleanup prefix, and retained follow state.
Return the number of old authority files already removed.  Recheck the context
and journal after validating content so changes during inspection are rejected."
  (physical-adoption-retire-check-owner owner)
  (let* ((value (getf state :plan))
         (last (length (physical-adoption-follow-moves value))))
    (unless (and (getf state :retiring) (not (getf state :retirement-partial))
                 (not (getf state :partial)) (= last (getf state :step))
                 (equal state (physical-adoption-follow-read owner)))
      (error "adoption retirement requires an unchanged complete terminal marker"))
    (let* ((mount (physical-adoption-retire-check-context owner value))
           (prefix (physical-adoption-retire-authority-prefix owner value mount)))
      (unless (physical-adoption-retire-namespace-p owner state prefix mount)
        (error "adoption retirement namespace differs from its exact cleanup prefix"))
      (physical-adoption-follow-check-content owner value last mount)
      (physical-adoption-follow-check-active owner value)
      (unless (and (= mount (physical-adoption-retire-check-context owner value))
                   (= prefix (physical-adoption-retire-authority-prefix owner value mount))
                   (physical-adoption-retire-namespace-p owner state prefix mount)
                   (equal state (physical-adoption-follow-read owner)))
        (error "adoption retirement context changed during validation"))
      prefix)))

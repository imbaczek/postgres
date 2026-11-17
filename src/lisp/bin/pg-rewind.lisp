(require :asdf)
(asdf:load-asd (merge-pathnames "../pg.asd" *load-truename*))
(asdf:load-system "pg")

(in-package #:pg)

(defun native-rewind-options (arguments)
  "Parse the target and exactly one of source, resume, or finish."
  (let ((target nil) (source nil) (offline-mode nil))
    (labels ((accept (key value)
               (unless (and value (plusp (length value)))
                 (error "pg_rewind option requires a value"))
               (ecase key
                 (:target
                  (when target (error "duplicate pg_rewind target"))
                  (setf target value))
                 (:source
                  (when source (error "duplicate pg_rewind source"))
                  (setf source value)))))
      (loop while arguments
            for argument = (pop arguments)
            do (cond
                 ((member argument '("-D" "--target-pgdata") :test #'equal)
                  (accept :target (pop arguments)))
                 ((member argument '("--finish" "--resume") :test #'equal)
                  (when offline-mode (error "multiple pg_rewind offline modes"))
                  (setf offline-mode (if (equal argument "--finish") :finish :resume)))
                 ((equal argument "--source-server")
                  (accept :source (pop arguments)))
                 ((uiop:string-prefix-p "--target-pgdata=" argument)
                  (accept :target (subseq argument (length "--target-pgdata="))))
                 ((uiop:string-prefix-p "--source-server=" argument)
                  (accept :source (subseq argument (length "--source-server="))))
                 (t (error "unsupported native pg_rewind option")))))
    (unless (and target (if offline-mode (null source) source))
      (error "pg_rewind requires -D and exactly one of --source-server, --resume, or --finish"))
    (values (truename (uiop:ensure-directory-pathname target)) source offline-mode)))

(defun native-rewind-workspace (target)
  (let* ((parent (uiop:pathname-parent-directory-pathname target))
         (directory (sb-posix:mkdtemp (namestring (merge-pathnames ".pg-rewind.XXXXXX" parent)))))
    (uiop:ensure-directory-pathname directory)))

(defun native-rewind-workspace-p (target workspace)
  (and (equal (uiop:pathname-parent-directory-pathname workspace)
              (uiop:pathname-parent-directory-pathname target))
       (stringp (car (last (pathname-directory workspace))))
       (uiop:string-prefix-p ".pg-rewind." (car (last (pathname-directory workspace))))))

(defun native-rewind-stage-workspace (target attempt)
  (let* ((stage (pathname (getf (getf attempt :stage) :directory)))
         (workspace (uiop:pathname-parent-directory-pathname stage)))
    (unless (and (getf attempt :rewind-origin)
                 (native-rewind-workspace-p target workspace)
                 (equal stage (merge-pathnames "stage/" workspace)))
      (error "attempt is not a native rewind CLI capture"))
    workspace))

(defun native-rewind-workspace-from-plan (target plan)
  "Derive the CLI workspace from the validated install plan."
  (let* ((attempt (getf plan :attempt))
         (incoming (pathname (getf (getf plan :incoming) :directory)))
         (workspace (uiop:pathname-parent-directory-pathname incoming)))
    (native-rewind-stage-workspace target attempt)
    (unless (and (native-rewind-workspace-p target workspace)
                 (equal incoming (merge-pathnames "incoming/" workspace))
                 (equal (getf (getf plan :quarantine) :directory)
                        (namestring (merge-pathnames "quarantine/" workspace))))
      (error "installation is not a native rewind CLI workspace"))
    workspace))

(defun native-rewind-complete-install (target helper)
  "Resume installation from the validated owner's current step to its terminal step.
Require each advance to publish the next step and return the plan's workspace."
  (call-with-physical-reseed-install-owner
   target
   (lambda (owner)
     (let* ((plan (physical-reseed-install-owner-plan owner))
            (terminal (physical-reseed-install-last-step plan))
            (workspace (native-rewind-workspace-from-plan target plan)))
       (loop for step from (physical-reseed-install-owner-step owner) below terminal
             do (let ((progress (physical-reseed-install-advance owner :helper helper)))
                  (unless (= (getf progress :step) (1+ step))
                    (error "rewind installation did not publish its next step"))))
       workspace))))

(defun native-rewind-install (target profile helper)
  "Capture and install a source image while holding the required target owners.
Publish the install journal before advancing the installation steps."
  (let ((workspace nil))
    (call-with-physical-reseed-owner
     target
     (lambda (owner)
       (physical-rewind-inspect-primary owner)
       (let ((*pgdata* target))
         (physical-reseed-check-before-pid))
       (setf workspace (native-rewind-workspace target))
       (format t "~&pg_rewind workspace: ~a~%" workspace)
       (finish-output)
       (destructuring-bind (stage incoming quarantine follow-workspace)
           (loop for name in '("stage/" "incoming/" "quarantine/" "follow/")
                 for path = (merge-pathnames name workspace)
                 do (sb-posix:mkdir (string-right-trim "/" (namestring path)) #o700)
                 collect path)
         (declare (ignore follow-workspace))
         (physical-rewind-start-discovered-remote owner stage profile :helper helper)
         (physical-reseed-prepare-image owner incoming :helper helper)
         (physical-reseed-publish-install owner incoming quarantine :helper helper))))
    (native-rewind-complete-install target helper)
    workspace))

(defun native-rewind-proved-follow-p (target)
  "Check the follow journal under configuration ownership for a proved successor."
  (call-with-physical-reseed-follow-configuration-owner
   target
   (lambda (owner)
     (call-with-physical-directory-root
      owner
      (lambda (fd directory)
        (declare (ignore directory))
        (let ((record (getf (physical-reseed-follow-snapshot-at fd) :record)))
          (eq :proved (getf (first (getf record :generations)) :phase))))))))

(defun native-rewind-follow (target workspace helper)
  "Prepare a successor unless already proved, then resume configuration and activation.
Retire the journals and finalize the installed standby."
  (unless (native-rewind-proved-follow-p target)
    (let ((name (string-downcase (format nil "rewind_~32,'0x" (logical-slot-new-generation)))))
      (physical-rewind-select-installed-follow target name))
    (let ((*pgdata* target))
      (call-with-physical-reseed-follow-owner
       target
       (lambda (owner)
         (let ((record (physical-reseed-follow-prepare owner :helper helper)))
           (unless (eq :proved (getf (first (getf record :generations)) :phase))
             (error "native pg_rewind successor did not reach its proved state")))))))
  (physical-reseed-follow-configure-and-activate
   target (merge-pathnames "follow/" workspace) (postgres-current-timestamp) :helper helper)
  (physical-reseed-follow-retire-offline target 5432)
  (physical-reseed-follow-finalize-offline target 5432))

(defun native-rewind-resume-before-install (target helper)
  (call-with-physical-reseed-owner
   target
   (lambda (owner)
     (let ((attempt (physical-rewind-recover-attempt
                     owner (lambda (record) (native-rewind-stage-workspace target record)))))
       (physical-rewind-resume-capture owner attempt :helper helper)
       (let* ((workspace (native-rewind-workspace target))
              (incoming (merge-pathnames "incoming/" workspace))
              (quarantine (merge-pathnames "quarantine/" workspace)))
         (format t "~&pg_rewind workspace: ~a~%" workspace)
         (finish-output)
         (dolist (path (list incoming quarantine (merge-pathnames "follow/" workspace)))
           (sb-posix:mkdir (string-right-trim "/" (namestring path)) #o700))
         (physical-reseed-prepare-image owner incoming :helper helper)
         (physical-reseed-publish-install owner incoming quarantine :helper helper)))))
  (native-rewind-complete-install target helper))

(defun native-rewind-resume-workspace (target helper)
  "Use the validated install plan when follow state exists.
Otherwise complete the published installation and return its workspace."
  (if (or (physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow" target))
          (physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow.tmp" target)))
      (call-with-physical-reseed-follow-configuration-owner
       target
       (lambda (owner)
         (native-rewind-workspace-from-plan target (physical-reseed-install-owner-plan owner))))
      (if (physical-adoption-path-present-p (merge-pathnames "physical-reseed.install" target))
          (native-rewind-complete-install target helper)
          (native-rewind-resume-before-install target helper))))

(defun native-rewind-retirement-present-p (target)
  "Route a named retired receipt or active follow record to the retirement owner.
That owner checks authority before any update."
  (or (physical-adoption-path-present-p (merge-pathnames "physical-reseed.retired" target))
      (let ((fd (physical-backup-open-directory target)))
        (unwind-protect
             (when (physical-reseed-follow-entry fd "physical-reseed.follow")
               (let ((record (physical-reseed-follow-read-at fd "physical-reseed.follow")))
                 (or (getf record :retirement)
                     (eq :active (getf (getf record :activation) :phase)))))
          (sb-posix:close fd)))))

(defun native-rewind-main (arguments)
  "Run a new rewind or resume a published install or follow phase.
Use --finish for offline configuration handback, including after receipt consumption."
  (handler-case
      (multiple-value-bind (target conninfo offline-mode) (native-rewind-options arguments)
        (ecase offline-mode
          (:finish (physical-reseed-follow-finalize-offline target 5432))
          (:resume
           (if (native-rewind-retirement-present-p target)
               (progn
                 (physical-reseed-follow-retire-offline target 5432 :rewind-only t)
                 (physical-reseed-follow-finalize-offline target 5432))
               (let* ((helper (physical-libpq-helper-path))
                      (workspace (native-rewind-resume-workspace target helper)))
                 (native-rewind-follow target workspace helper))))
          ((nil)
           (let* ((helper (physical-libpq-helper-path))
                  (profile (make-physical-libpq-profile conninfo :mode :physical
                                                                 :fallback "native-rewind"))
                  (workspace (native-rewind-install target profile helper)))
             (native-rewind-follow target workspace helper))))
        (write-line "pg_rewind: target is ready to follow the source")
        0)
    (error (condition)
      (format *error-output* "~&pg_rewind: native rewind failed (~a)~%" (type-of condition))
      1)))

(sb-ext:exit :code (native-rewind-main (uiop:command-line-arguments)))

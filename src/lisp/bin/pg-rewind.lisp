(require :asdf)
(asdf:load-asd (merge-pathnames "../pg.asd" *load-truename*))
(asdf:load-system "pg")

(in-package #:pg)

(defun native-rewind-options (arguments)
  "Parse the supported target, source, and offline finish options."
  (let ((target nil) (source nil) (finish nil))
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
                 ((equal argument "--finish")
                  (when finish (error "duplicate pg_rewind finish option"))
                  (setf finish t))
                 ((equal argument "--source-server")
                  (accept :source (pop arguments)))
                 ((uiop:string-prefix-p "--target-pgdata=" argument)
                  (accept :target (subseq argument (length "--target-pgdata="))))
                 ((uiop:string-prefix-p "--source-server=" argument)
                  (accept :source (subseq argument (length "--source-server="))))
                 (t (error "unsupported native pg_rewind option")))))
    (unless (and target (if finish (null source) source))
      (error "pg_rewind requires -D and either --source-server or --finish"))
    (values (truename (uiop:ensure-directory-pathname target)) source finish)))

(defun native-rewind-workspace (target)
  (let* ((parent (uiop:pathname-parent-directory-pathname target))
         (directory (sb-posix:mkdtemp (namestring (merge-pathnames ".pg-rewind.XXXXXX" parent)))))
    (uiop:ensure-directory-pathname directory)))

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
    (let ((plan (getf (physical-reseed-install-initial-arguments target) :plan)))
      (call-with-physical-reseed-install-owner
       target
       (lambda (owner)
         (loop repeat (physical-reseed-install-last-step plan)
               do (physical-reseed-install-advance owner :helper helper)))))
    workspace))

(defun native-rewind-follow (target workspace helper)
  "Select and prove a successor, activate it, and archive the install and attempt journals.
Finalize the stopped streaming standby after retirement."
  (let ((name (string-downcase (format nil "rewind_~32,'0x" (logical-slot-new-generation)))))
    (physical-rewind-select-installed-follow target name)
    (let ((*pgdata* target))
      (call-with-physical-reseed-follow-owner
       target
       (lambda (owner)
         (let ((record (physical-reseed-follow-prepare owner :helper helper)))
           (unless (eq :proved (getf (first (getf record :generations)) :phase))
             (error "native pg_rewind successor did not reach its proved state"))))))
    (physical-reseed-follow-configure-and-activate
     target (merge-pathnames "follow/" workspace) (postgres-current-timestamp) :helper helper)
    ;; The port is used only while holding offline PID-file ownership.
    (physical-reseed-follow-retire-offline target 5432)
    (physical-reseed-follow-finalize-offline target 5432)))

(defun native-rewind-main (arguments)
  "Run native rewind or finish configuration handoff after terminal retirement."
  (handler-case
      (multiple-value-bind (target conninfo finish) (native-rewind-options arguments)
        (if finish
            (physical-reseed-follow-finalize-offline target 5432)
            (let* ((helper (physical-libpq-helper-path))
                   (profile (make-physical-libpq-profile conninfo :mode :physical
                                                                  :fallback "native-rewind"))
                   (workspace (native-rewind-install target profile helper)))
              (native-rewind-follow target workspace helper)))
        (write-line "pg_rewind: target is ready to follow the source")
        0)
    (error (condition)
      (format *error-output* "~&pg_rewind: native rewind failed (~a)~%" (type-of condition))
      1)))

(sb-ext:exit :code (native-rewind-main (uiop:command-line-arguments)))

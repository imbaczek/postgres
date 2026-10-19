;;; format.el --- Batch formatter for pg-lisp -*- lexical-binding: t; -*-

(require 'package)
(package-initialize)

(unless (require 'slime nil t)
  (error "SLIME is not installed; install the `slime' package from MELPA"))
(require 'slime-cl-indent)
(slime-setup '(slime-cl-indent))

(setq-default indent-tabs-mode nil)
(setq make-backup-files nil
      create-lockfiles nil
      slime-net-coding-system 'utf-8-unix)

(defun pg-format-wait-for-connection (timeout)
  (let ((deadline (+ (float-time) timeout)))
    (while (and (not (slime-connected-p)) (< (float-time) deadline))
      (accept-process-output nil 0.1))
    (unless (slime-connected-p)
      (error "Timed out waiting for the SLIME connection"))))

(defun pg-format-wait-for-indentation (timeout)
  (let ((deadline (+ (float-time) timeout))
        (updated nil))
    (let ((hook (lambda (&rest _)
                  (setq updated t))))
      (add-hook 'slime-indentation-update-hooks hook)
      (unwind-protect
          (progn
            (slime-update-indentation)
            (while (and (not updated) (< (float-time) deadline))
              (accept-process-output nil 0.1))
            (unless updated
              (error "Timed out waiting for SLIME indentation information")))
        (remove-hook 'slime-indentation-update-hooks hook)))))

(defun pg-format--lisp-command (root program)
  (let ((registry (format "(push #p%s asdf:*central-registry*)"
                          (prin1-to-string (file-name-as-directory root)))))
    (list program
          "--noinform"
          "--disable-debugger"
          "--eval" "(require :asdf)"
          "--eval" registry
          "--eval"
          "(setf uiop:*compile-file-warnings-behaviour* :error uiop:*compile-file-failure-behaviour* :error)"
          "--eval" "(asdf:load-system \"pg\" :force t)")))

(defun pg-format-files (files check-only)
  (let (changed)
    (dolist (file files)
      (let* ((path (expand-file-name file))
             (buffer (progn
                       (unless (file-regular-p path)
                         (error "format: not a regular file: %s" path))
                       (find-file-noselect path))))
        (with-current-buffer buffer
          (unless (derived-mode-p 'lisp-mode)
            (lisp-mode))
          (let ((original (buffer-substring-no-properties (point-min) (point-max))))
            (indent-region (point-min) (point-max))
            (if (equal original
                       (buffer-substring-no-properties (point-min) (point-max)))
                (message "format: %s is already formatted" path)
              (if check-only
                  (progn
                    (push path changed)
                    (message "format: %s needs formatting" path))
                (save-buffer)
                (message "format: formatted %s" path)))))))
    (when changed
      (message "format: check failed for %s"
               (mapconcat #'identity (nreverse changed) ", "))
      (kill-emacs 1))))

(let* ((root (or (getenv "PG_LISP_ROOT")
                 (error "PG_LISP_ROOT is not set")))
       (program (or (getenv "PG_LISP_INFERIOR")
                    (error "PG_LISP_INFERIOR is not set")))
       (check-only (equal (getenv "PG_LISP_FORMAT_CHECK") "1"))
       (files (if (equal (car command-line-args-left) "--")
                  (cdr command-line-args-left)
                command-line-args-left)))
  (unless files
    (error "Pass one or more Lisp source files to format"))
  (setq command-line-args-left nil)
  (setq slime-lisp-implementations
        `((pg-lisp (,@(pg-format--lisp-command root program))
                   :coding-system utf-8-unix))
        slime-default-lisp 'pg-lisp)
  (slime)
  (pg-format-wait-for-connection 60)
  (pg-format-wait-for-indentation 60)
  (pg-format-files files check-only))

;;; format.el ends here

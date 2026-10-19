;;;; dest.lisp -- destination receiver naming shim.

(in-package #:pg)

(defun dest-command-complete (stream tag)
  (send-command-complete stream tag))

(defun dest-row-description (stream columns)
  (send-row-description stream columns))

(defun dest-data-row (stream values)
  (send-data-row-text stream values))

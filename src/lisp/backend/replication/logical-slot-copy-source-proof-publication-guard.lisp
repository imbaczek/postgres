(in-package #:pg)

(defvar *logical-slot-copy-source-proof-publication-test-hook* nil)
(defvar *logical-slot-copy-source-proof-publication-write-limit* nil)

(defun call-with-logical-slot-copy-source-proof-action
    (owner context input attempt function &key source)
  ;; Check the preparation on both sides of an action; failure is sticky.
  (let ((record (logical-slot-copy-source-proof-preparation-record owner context input attempt))
        (complete nil))
    (unwind-protect
         (multiple-value-prog1
             (progn
               (logical-slot-copy-source-proof-check-preparation
                owner context input attempt :source source)
               (funcall function))
           (logical-slot-copy-source-proof-check-preparation
            owner context input attempt :source source)
           (setf complete t))
      (unless complete (funcall (fourth record) :fail)))))

(defun logical-slot-copy-source-proof-publication-point
    (owner context input attempt point &key (source t))
  (call-with-logical-slot-copy-source-proof-action
   owner context input attempt
   (lambda ()
     (when *logical-slot-copy-source-proof-publication-test-hook*
       (funcall *logical-slot-copy-source-proof-publication-test-hook* point)))
   :source source))

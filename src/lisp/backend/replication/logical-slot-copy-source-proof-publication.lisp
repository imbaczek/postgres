(in-package #:pg)

(defun logical-slot-copy-source-proof-prove-canonical (owner context input attempt canonical)
  ;; Run the anchored reader while the original input and source lease remain
  ;; held.  Retry still requires actual R to match the preparation.
  (call-with-logical-slot-copy-source-proof-action
   owner context input attempt
   (lambda ()
     (let ((root (physical-image-context-root-fd context)))
       (logical-slot-copy-source-proof-check-file context "logical-slot-copy.source-proof.tmp" :absent t)
       (call-with-logical-slot-copy-source-proof-fd
        root "logical-slot-copy.source-proof" nil
        (lambda (fd facts)
          (unless (equal canonical facts) (error "source proof changed before independent validation"))
          (logical-slot-copy-source-proof-check-image-fd owner context input attempt fd)
          (let ((*logical-slot-copy-verification-test-hook* nil))
            (logical-slot-copy-read-source-proof-under-locks))
          (logical-slot-copy-source-proof-check-open-file
           context "logical-slot-copy.source-proof" fd (logical-slot-copy-install-file-identity canonical))
          (logical-slot-copy-source-proof-check-image-fd owner context input attempt fd)))))
   :source t))

(defun logical-slot-copy-source-proof-publish-prepared (owner context input attempt)
  ;; Check any canonical file before discarding an orphan, then sync and prove
  ;; the result.  After the output close hook, run a quiet terminal proof.
  (logical-slot-copy-source-proof-check-preparation owner context input attempt)
  (logical-slot-copy-source-proof-publication-point
   owner context input attempt :after-source-proof-seal)
  (let ((canonical (logical-slot-copy-source-proof-read-canonical owner context input attempt)))
    (logical-slot-copy-source-proof-remove-temporary owner context input attempt canonical)
    (unless canonical
      (let ((temporary (logical-slot-copy-source-proof-write-temporary owner context input attempt)))
        (logical-slot-copy-source-proof-publication-point
         owner context input attempt :after-source-proof-temp-close)
        (setf canonical
              (logical-slot-copy-source-proof-install-temporary owner context input attempt temporary))))
    (logical-slot-copy-source-proof-sync-canonical owner context input attempt canonical)
    (logical-slot-copy-source-proof-prove-canonical owner context input attempt canonical)
    (logical-slot-copy-source-proof-publication-point
     owner context input attempt :after-source-proof-publication-close)
    (let ((*logical-slot-copy-source-proof-publication-test-hook* nil)
          (*logical-slot-copy-verification-test-hook* nil))
      (logical-slot-copy-source-proof-prove-canonical owner context input attempt canonical))
    :published))

(defun logical-slot-copy-publish-source-proof ()
  ;; Publish or resync from the active cold installation and actual R.
  ;; A matching canonical file alone does not authorize retry.  Return success
  ;; only after the outer input and preparation scopes have cleaned up.
  (let ((result
          (call-with-logical-slot-copy-install-cold-input
           (lambda (owner context input)
             (call-with-logical-slot-copy-source-proof-preparation
              owner context input
              (lambda (attempt)
                (logical-slot-copy-source-proof-publish-prepared owner context input attempt)))))))
    (unless (eq :published result)
      (error "source proof publication requires an existing active installation"))
    result))

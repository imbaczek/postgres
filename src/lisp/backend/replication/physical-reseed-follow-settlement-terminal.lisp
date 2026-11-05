(in-package #:pg)

;;; Record the closed source outcome against the exact pending predecessor.

;; Accept a terminal candidate only as the exact successor of pending intent.
(defun physical-reseed-follow-terminal-settlement-candidate (context value)
  (let ((before (physical-reseed-follow-installation-context-record context))
        (candidate (physical-reseed-follow-inert-settlement-candidate context value)))
    (unless (and (eq :handoff-pending (getf (getf before :settlement) :phase))
                 (member (getf (getf candidate :settlement) :phase) '(:handed-off :source-aborted)))
      (error "terminal settlement requires its exact pending inert predecessor"))
    candidate))

;; Compare all predecessors observed around network I/O before publication.
(defun physical-reseed-follow-check-settlement-result-snapshot (context expected)
  (unless (equal expected
                 (list :follow (physical-reseed-follow-installation-context-snapshot context)
                       :attempt (physical-reseed-follow-installation-context-attempt context)
                       :install (physical-reseed-follow-installation-context-install-state context)))
    (error "settlement predecessors changed before terminal publication"))
  t)

;; Publish the closed receipt with an exact predecessor comparison.  The
;; candidate and its readback remain subject to the guarded local write.
;; Historical settlement after local abort grants no activation or cleanup rights.
(defun physical-reseed-follow-publish-settlement-result (owner receipt expected)
  (call-with-physical-reseed-follow-installation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-installation-context owner))
            (before (physical-reseed-follow-installation-context-record context))
            (candidate (physical-reseed-follow-copy before)))
       (funcall check)
       (physical-reseed-follow-check-settlement-result-snapshot context expected)
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
        "terminal settlement temporary requires explicit recovery")
       (multiple-value-bind (action source)
           (physical-reseed-follow-settlement-source-action before receipt nil)
         (unless (eq :terminal action) (error "terminal publication requires an exact closed source receipt"))
         (setf (getf candidate :settlement)
               (list :phase (ecase (getf source :outcome)
                              (:handed-off :handed-off) (:aborted :source-aborted))
                     :successor (getf (getf before :settlement) :successor) :receipt source)))
       (let* ((candidate (physical-reseed-follow-terminal-settlement-candidate context candidate))
              (bytes (physical-reseed-follow-image candidate))
              (*physical-reseed-follow-terminal-settlement-candidate* candidate))
         (funcall check)
         (physical-reseed-follow-check-settlement-result-snapshot context expected)
         (physical-reseed-follow-write-installed-intent context candidate bytes check))))))

;; Settle the source outcome while leaving the configuration inert and startup
;; fenced.  Activation and startup retirement are separate future work.
(defun physical-reseed-follow-settle (owner &key helper)
  (let ((record (physical-reseed-follow-read owner)))
    (unless (getf record :settlement)
      (setf record (physical-reseed-follow-begin-settlement owner)))
    (unless (eq :handoff-pending (getf (getf record :settlement) :phase))
      (return-from physical-reseed-follow-settle record))
    (multiple-value-bind (receipt snapshot)
        (physical-reseed-follow-settle-remote owner :helper helper)
      (if receipt
          (physical-reseed-follow-publish-settlement-result owner receipt snapshot)
          (physical-reseed-follow-copy (getf (getf snapshot :follow) :record))))))

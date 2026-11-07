(in-package #:pg)

(defstruct (physical-reseed-follow-cancellation-ack-authority
            (:include physical-reseed-follow-cancellation-authority)
            (:constructor %make-physical-reseed-follow-cancellation-ack-authority)
            (:copier nil)
            (:print-function physical-libpq-print-private-object)))

(defun physical-reseed-follow-publish-cancellation-cleaned-held
    (owner operation expected evidence begin-publication evidence-check)
  (declare (notinline sb-posix:close))
  (unless (eq owner (physical-reseed-follow-cancellation-operation-owner operation))
    (error "cleanup publication changed its operation owner"))
  (let* ((snapshot (physical-adoption-attempt-value-copy expected))
         (plan (physical-reseed-follow-claim-cleanup-operation-evidence evidence operation snapshot))
         (previous (getf snapshot :follow))
         (candidate (physical-reseed-follow-cancellation-cleaned-candidate (getf previous :record) plan))
         (bytes (physical-reseed-follow-image candidate))
         (attempt (getf snapshot :attempt))
         (temporary (getf snapshot :temporary))
         (mode (if temporary :recover :write))
         (thread sb-thread:*current-thread*)
         (published nil))
    (when (and temporary (not (equal candidate (getf temporary :record))))
      (error "acknowledged cleanup differs from its captured recovery candidate"))
    (physical-reseed-follow-cancellation-check-cleanup-capacity candidate)
    (funcall begin-publication candidate)
    (let ((result
            (call-with-physical-reseed-follow-cancellation-finalization
             owner
             (lambda (root-fd store-check)
               (let ((authority nil) (live t) (fd nil))
                 (labels
                     ((scope (value actual-owner actual-root actual-candidate actual-previous actual-bytes)
                        (unless (and live (eq value authority)
                                     (physical-reseed-follow-cancellation-ack-authority-p value)
                                     (eq owner actual-owner) (eql root-fd actual-root)
                                     (eq thread sb-thread:*current-thread*)
                                     (eq thread (physical-reseed-follow-cancellation-authority-thread value))
                                     (eq mode (physical-reseed-follow-cancellation-authority-mode value))
                                     (eq owner (physical-reseed-follow-cancellation-authority-owner value))
                                     (eql root-fd (physical-reseed-follow-cancellation-authority-root-fd value))
                                     (physical-reseed-follow-cancellation-authority-active-p value)
                                     (equal candidate actual-candidate) (equal previous actual-previous)
                                     (or (null actual-bytes) (equalp bytes actual-bytes))
                                     (equal candidate (physical-reseed-follow-cancellation-authority-candidate value))
                                     (equal previous (physical-reseed-follow-cancellation-authority-previous value))
                                     (equalp bytes (physical-reseed-follow-cancellation-authority-bytes value)))
                          (error "cleanup acknowledgment authority changed its publication scope")))
                      (verify ()
                        (scope authority owner root-fd candidate previous bytes)
                        (physical-reseed-cleanup-check-binding owner (getf attempt :record))
                        (physical-reseed-follow-check-attempt-snapshot-at root-fd attempt)
                        (funcall store-check)
                        (funcall evidence-check root-fd)
                        (physical-reseed-follow-cancellation-check-publication-state
                         authority root-fd (getf temporary :identity))
                        (when (and temporary
                                   (not (physical-reseed-follow-cancellation-authority-published authority)))
                          (physical-reseed-follow-check-cleaned-recovery-temporary-at root-fd temporary candidate))))
                   (when *physical-reseed-follow-cancellation-authority*
                     (error "cleanup acknowledgment authority cannot reenter publication"))
                   (physical-reseed-follow-check-snapshot root-fd previous)
                   (unwind-protect
                        (progn
                          (if temporary
                              (progn
                                (setf (physical-reseed-follow-owner-uncertain-p owner) t)
                                (sb-sys:without-interrupts
                                  (setf fd (physical-reseed-follow-open-cleaned-recovery-temporary
                                            root-fd temporary candidate))))
                              (physical-adoption-require-absent
                               (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root-fd))
                               "cleanup acknowledgment temporary requires explicit recovery"))
                          (physical-reseed-follow-cancellation-local-proof owner root-fd candidate)
                          (setf authority
                                (%make-physical-reseed-follow-cancellation-ack-authority
                                 :owner owner :root-fd root-fd :thread thread :mode mode :fd fd
                                 :candidate (physical-reseed-follow-copy candidate)
                                 :previous (physical-adoption-attempt-value-copy previous) :bytes (copy-seq bytes)
                                 :scope #'scope :check #'verify :phase :acquiring :active-p t))
                          (let ((*physical-reseed-follow-cancellation-authority* authority))
                            (unwind-protect
                                 (progn
                                   (verify)
                                   (setf (physical-reseed-follow-cancellation-authority-phase authority) :ready)
                                   (multiple-value-bind (record final)
                                       (if temporary
                                           (physical-reseed-follow-promote-intent
                                            owner root-fd fd (subseq (getf temporary :identity) 0 5)
                                            candidate previous #'verify)
                                           (physical-reseed-follow-write-intent
                                            owner root-fd candidate bytes previous #'verify))
                                     (setf published (physical-adoption-attempt-value-copy final))
                                     (values record final)))
                              (setf live nil
                                    (physical-reseed-follow-cancellation-authority-active-p authority) nil))))
                     (when fd (sb-posix:close fd)))))))))
      (values result published))))

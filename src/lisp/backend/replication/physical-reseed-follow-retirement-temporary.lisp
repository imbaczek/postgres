(in-package #:pg)

(defun physical-reseed-follow-retirement-next-reference (root record)
  ;; Use the frozen attempt reference when present.  The initial transition
  ;; obtains it from the actual root attempt observation.
  (or (getf (getf record :retirement) :attempt-file)
      (let ((attempt (physical-reseed-follow-retirement-attempt-at root root record 0)))
        (list :identity (getf attempt :identity) :digest (getf attempt :digest)))))

(defun physical-reseed-follow-retirement-temporary-state-at (root stream)
  ;; Require the held temporary bytes to be the complete canonical image.
  (multiple-value-bind (state bytes)
      (physical-reseed-follow-retirement-journal-state-at root stream "physical-reseed.follow.tmp")
    (unless (equalp bytes (physical-reseed-follow-image (getf state :record)))
      (error "retirement temporary is not a complete canonical image"))
    state))

(defun call-with-physical-reseed-follow-retirement-next-temporary (root snapshot parent-check function)
  ;; Hold a read-only temporary descriptor or pin its absence during the callback.
  ;; PARENT-CHECK stays independent of layout checks to avoid recursion.  The
  ;; callback gets copied state and a thread-bound check revoked on exit.
  (let ((snapshot (physical-adoption-attempt-value-copy snapshot))
        (thread sb-thread:*current-thread*) (active nil)
        (fd nil) (stream nil) (state nil))
    (unwind-protect
         (progn
           (funcall parent-check)
           (physical-reseed-follow-check-snapshot root snapshot)
           (when (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)
             (sb-sys:without-interrupts
               (setf fd (physical-image-openat
                         root "physical-reseed.follow.tmp"
                         (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) 0)))
             (when (minusp fd) (setf fd nil) (error "cannot open retirement temporary"))
             (sb-sys:without-interrupts
               (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8))))
             (setf state (physical-reseed-follow-retirement-temporary-state-at root stream))
             (physical-reseed-follow-test-point
              :after-retirement-temporary-open
              (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))))
           (flet ((verify ()
                    (unless (and active (eq thread sb-thread:*current-thread*))
                      (error "retirement temporary scope ended or changed thread"))
                    (funcall parent-check)
                    (physical-reseed-follow-check-snapshot root snapshot)
                    (if state
                        (let ((record (getf snapshot :record)))
                          (unless (and (equal state (physical-reseed-follow-retirement-temporary-state-at root stream))
                                       (equal (getf state :record)
                                              (physical-reseed-follow-retirement-next-record
                                               record (physical-reseed-follow-retirement-next-reference root record))))
                            (error "retirement temporary differs from its exact next record or held facts")))
                        (when (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)
                          (error "retirement temporary appeared during an absent observation")))
                    (funcall parent-check)
                    (physical-reseed-follow-check-snapshot root snapshot)
                    t))
             (setf active t)
             (verify)
             (multiple-value-prog1
                 (funcall function (physical-adoption-attempt-value-copy state) #'verify)
               (verify))))
      (setf active nil)
      (if stream (close stream) (when fd (sb-posix:close fd))))))

(defun physical-reseed-follow-retirement-check-temporary-layout
    (descriptors record candidate prefix install attempt)
  ;; Keep the final record as the layout basis.  Require the candidate to be
  ;; the exact next record at the observed prefix and reprove both files.
  (let* ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (expected (physical-reseed-follow-retirement-next-record
                    record (physical-reseed-follow-retirement-next-reference root record))))
    (unless (and (equal expected candidate)
                 (= prefix (getf (getf candidate :retirement) :step)))
      (error "retirement temporary requires its exact next observed archive prefix"))
    (physical-reseed-follow-retirement-check-prefix candidate prefix)
    (unless (and (equal install (physical-reseed-follow-retirement-install-at descriptors candidate prefix))
                 (equal attempt (physical-reseed-follow-retirement-attempt-at
                                 root (if (= prefix 2) saved root) candidate prefix)))
      (error "retirement temporary differs from the proved install or attempt references"))
    t))

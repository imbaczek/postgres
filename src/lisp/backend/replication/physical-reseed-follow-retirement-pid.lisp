(in-package #:pg)

(defun physical-reseed-follow-retirement-pid-state-at (root fd)
  ;; Compare the named PID file with the held descriptor before and after
  ;; reading its contents and check that it remains on the root mount.
  (let* ((mount (physical-adoption-install-mount-id nil :directory-fd root))
         (row (physical-reseed-install-old-observe root "postmaster.pid" mount))
         (held (physical-reseed-file-facts (sb-posix:fstat fd))))
    (unless (and (equal held (physical-reseed-install-row-file-facts row))
                 (= mount (physical-adoption-install-mount-id nil :directory-fd fd)))
      (error "retirement PID differs from its held descriptor"))
    (let ((state (physical-reseed-read-pid (physical-image-fd-path root))))
      (unless (and state (equal held (getf state :identity))
                   (equal held (physical-reseed-file-facts (sb-posix:fstat fd)))
                   (equal row (physical-reseed-install-old-observe root "postmaster.pid" mount))
                   (= mount (physical-adoption-install-mount-id nil :directory-fd fd)))
        (error "retirement PID changed during its held observation"))
      (append state (list :row row)))))

(defun physical-reseed-follow-retirement-pid-state-copy (state)
  ;; Copy the private PID evidence and its byte vector for the callback.
  (let ((copy (physical-adoption-attempt-value-copy state)))
    (when copy
      (setf (getf copy :bytes) (copy-seq (getf state :bytes))))
    copy))

(defun call-with-physical-reseed-follow-retirement-pid (root parent-check policy function)
  ;; Hold a read-only PID descriptor or observe its absence around the callback.
  ;; VERIFY rechecks the file and policy on this thread and is revoked on exit.
  ;; PARENT-CHECK stays independent of the layout proof.  Close the held
  ;; descriptor on every exit.
  (let ((fd nil) (state nil) (active nil) (thread sb-thread:*current-thread*))
    (unwind-protect
         (progn
           (funcall parent-check)
           (when (physical-adoption-path-present-p (merge-pathnames "postmaster.pid" (physical-image-fd-path root)))
             (sb-sys:without-interrupts
               (setf fd (physical-image-openat
                         root "postmaster.pid"
                         (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) 0))
               (when (minusp fd) (setf fd nil) (error "cannot open retirement PID")))
             (setf state (physical-reseed-follow-retirement-pid-state-at root fd))
             (physical-reseed-follow-test-point
              :after-retirement-pid-open (merge-pathnames "postmaster.pid" (physical-image-fd-path root))))
           (labels ((check-file ()
                      (if state
                          (let ((current (physical-reseed-follow-retirement-pid-state-at root fd)))
                            (unless (and (= (getf state :pid) (getf current :pid))
                                         (equal (getf state :identity) (getf current :identity))
                                         (equal (getf state :row) (getf current :row))
                                         (equalp (getf state :bytes) (getf current :bytes)))
                              (error "retirement PID changed its held bytes or metadata")))
                          (when (physical-adoption-path-present-p
                                 (merge-pathnames "postmaster.pid" (physical-image-fd-path root)))
                            (error "retirement PID appeared during an absent observation")))
                      t)
                    (verify ()
                      (unless (and active (eq thread sb-thread:*current-thread*))
                        (error "retirement PID scope ended or changed thread"))
                      (funcall parent-check)
                      (check-file)
                      (funcall policy state)
                      (check-file)
                      (funcall parent-check)
                      t))
             (setf active t)
             (verify)
             (multiple-value-prog1
                 (funcall function (physical-reseed-follow-retirement-pid-state-copy state) #'verify)
               (verify))))
      (setf active nil)
      (when fd (sb-posix:close fd)))))

(defun call-with-physical-reseed-follow-retirement-before-pid (root parent-check function)
  ;; Before PID ownership, apply the postmaster stale-PID and ancestor policy.
  (call-with-physical-reseed-follow-retirement-pid
   root parent-check
   (lambda (state)
     (when (and state (postmaster-pid-is-live-p (getf state :pid)))
       (error "retirement startup PID refers to a live process")))
   function))

(defun call-with-physical-reseed-follow-retirement-owned-pid (root parent-check function)
  ;; Check the held PID against the current process and the borrowed postmaster
  ;; stream.  The PID scope closes its own descriptor and leaves that stream held.
  (let ((stream *postmaster-pid-stream*) (inode *postmaster-pid-inode*)
                                         (directory (when *postmaster-pid-directory* (copy-seq (namestring *postmaster-pid-directory*)))))
    (call-with-physical-reseed-follow-retirement-pid
     root parent-check
     (lambda (state)
       (unless (and state stream inode directory
                    (eq stream *postmaster-pid-stream*) (open-stream-p stream)
                    (eql inode *postmaster-pid-inode*)
                    *postmaster-pid-directory*
                    (equal directory (namestring *postmaster-pid-directory*))
                    (postmaster-pid-owned-p)
                    (= (getf state :pid) (sb-posix:getpid))
                    (equal (getf state :identity)
                           (physical-reseed-file-facts (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                    (= (physical-adoption-install-mount-id nil :directory-fd root)
                       (physical-adoption-install-mount-id nil :directory-fd (sb-sys:fd-stream-fd stream))))
         (error "retirement observation no longer owns the postmaster PID context"))
       (physical-image-check-target (pathname directory) root))
     function)))

(defun call-with-physical-reseed-follow-retirement-pid-observation (root snapshot parent-check function pid-scope)
  ;; Compose PID and temporary checks with the layout proof.  PID verification
  ;; is a parent check, so it does not recurse through the layout.  Pass copied
  ;; private PID and temporary evidence to the callback.
  (let ((snapshot (physical-adoption-attempt-value-copy snapshot)))
    (funcall
     pid-scope root parent-check
     (lambda (pid verify-pid)
       (let* ((entry (when pid (cons (copy-seq "postmaster.pid")
                                     (physical-adoption-attempt-value-copy (getf pid :row)))))
              (evidence (list :entry entry :verify verify-pid)))
         (call-with-physical-reseed-follow-retirement-next-temporary
          root snapshot verify-pid
          (lambda (temporary verify-temporary)
            (flet ((extra (fd)
                     (funcall verify-temporary)
                     (append (physical-reseed-follow-retirement-final-extra-at fd snapshot evidence)
                             (when temporary
                               (list (cons (copy-seq "physical-reseed.follow.tmp")
                                           (physical-adoption-attempt-value-copy (getf temporary :row)))))
                             (when entry (list (physical-adoption-attempt-value-copy entry))))))
              (call-with-physical-reseed-follow-retirement-observation-at
               root snapshot verify-pid
               (lambda (descriptors state verify)
                 (funcall function descriptors
                          (append state (list :pid-file (physical-reseed-follow-retirement-pid-state-copy pid)
                                              :temporary-file (physical-adoption-attempt-value-copy temporary)))
                          verify))
               #'extra (getf temporary :record) evidence)))))))))

(defun call-with-physical-reseed-follow-retirement-before-pid-observation (root snapshot parent-check function)
  ;; Use the pre-ownership PID policy for the retirement observation.
  (call-with-physical-reseed-follow-retirement-pid-observation
   root snapshot parent-check function #'call-with-physical-reseed-follow-retirement-before-pid))

(defun call-with-physical-reseed-follow-retirement-owned-pid-observation (root snapshot parent-check function)
  ;; Use the current postmaster ownership policy for the retirement observation.
  (call-with-physical-reseed-follow-retirement-pid-observation
   root snapshot parent-check function #'call-with-physical-reseed-follow-retirement-owned-pid))

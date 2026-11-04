(in-package #:pg)

;; Tests can limit STEP write chunks to exercise a partially written tail.
(defvar *physical-reseed-install-progress-write-limit* nil)

;; Each transition requires the original active attempt and a fresh source READY
;; check, with the local attempt journal checked on both sides.
(defun physical-reseed-install-require-ready (owner client record plan)
  (unless (eq :premanifest (getf record :phase)) (error "reseed movement requires an active original attempt"))
  (physical-reseed-require-journal owner record)
  (physical-reseed-image-check-source client record plan)
  (physical-reseed-require-journal owner record))

;; Keep the writable journal descriptor bound to the recorded named file and
;; its current identity and length.
(defun physical-reseed-install-progress-fd-check (root-fd fd state)
  (let ((identity (getf (getf state :plan) :journal)))
    (physical-reseed-install-check-held-journal
     root-fd fd "physical-reseed.install" (list (getf identity :device) (getf identity :inode))))
  (let ((facts (physical-reseed-install-file-identity (sb-posix:fstat fd))))
    (unless (and (equal facts (getf state :identity)) (= (sixth facts) (getf state :file-size)))
      (error "reseed progress journal descriptor changed"))))

;; Own the journal descriptor and transition closures for FUNCTION's lifetime.
;; The saved record is the raw attempt snapshot for this advancement. Each check
;; revalidates it along with the namespace and journal descriptor.
(defun call-with-physical-reseed-install-progress
    (owner root-fd initial-state initial-prefix namespace-check client record function)
  (let ((fd nil) (active nil) (state initial-state) (prefix initial-prefix)
                 (path (merge-pathnames "physical-reseed.install" (physical-image-fd-path root-fd))))
    (labels ((live ()
               (unless active (error "reseed progress scope has ended")))
             (current ()
               (live)
               (values state prefix))
             (check ()
               (live)
               (multiple-value-bind (actual observed) (funcall namespace-check)
                 (unless (and (equal state actual) (= prefix observed)
                              (equal record (physical-reseed-cleanup-owner-attempt owner)))
                   (error "reseed progress or attempt changed outside this transition")))
               (physical-reseed-install-progress-fd-check root-fd fd state))
             (ready ()
               (live)
               (physical-reseed-install-require-ready owner client record (getf state :plan))
               (check))
             (point (event &optional (where path))
               (live)
               ;; Decide before invocation so a hook that clears itself still
               ;; gets the post-hook check.
               (when *physical-reseed-install-test-hook*
                 (physical-reseed-install-test-point event where)
                 (check)))
             (refresh (step observed)
               (live)
               (let* ((previous (getf state :step))
                      (valid-length (getf state :valid-length)))
                 (unless (or (= step previous) (= step (1+ previous)))
                   (error "reseed progress skipped a journal step"))
                 (when (> step previous)
                   (incf valid-length (length (physical-reseed-install-step-image (getf state :body-crc) step))))
                 (multiple-value-bind (actual actual-prefix) (funcall namespace-check)
                   (unless (and (equal (getf state :plan) (getf actual :plan))
                                (= step (getf actual :step)) (= observed actual-prefix)
                                (not (getf actual :partial))
                                (= valid-length (getf actual :valid-length) (getf actual :file-size))
                                (equal record (physical-reseed-cleanup-owner-attempt owner)))
                     (error "reseed progress readback differs from the intended transition"))
                   (physical-reseed-install-progress-fd-check root-fd fd actual)
                   (setf state actual prefix observed)))))
      (unwind-protect
           (progn
             (sb-sys:without-interrupts
               (setf fd (physical-image-openat
                         root-fd "physical-reseed.install"
                         (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) 0)))
             (when (minusp fd) (setf fd nil) (error "cannot open retained reseed progress journal"))
             (physical-reseed-install-progress-fd-check root-fd fd state)
             (setf active t)
             (point :after-install-progress-open)
             (ready)
             (multiple-value-prog1 (funcall function fd #'current #'check #'ready #'point #'refresh)
               (check)))
        (setf active nil)
        (when fd (sb-posix:close fd))))))

;; Map a zero-based move to its held source and target parents and leaf name.
(defun physical-reseed-install-move-arguments (plan index root-fd incoming-fd quarantine-fd)
  (let* ((old (getf plan :old)) (old-count (length old))
                                (top (physical-reseed-install-top-rows plan)))
    (unless (typep index `(integer 0 ,(+ old-count (length top) -1)))
      (error "reseed move index is outside the plan"))
    (if (< index old-count)
        (values root-fd quarantine-fd (first (nth index old)))
        (values incoming-fd root-fd (first (nth (- index old-count) top))))))

;; Sync both parents after a rename before recording its step. Keep fsync
;; uninlined here so tests can inject a failure at the actual syscall boundary.
(defun physical-reseed-install-sync-parents (source-fd target-fd point)
  (declare (notinline sb-posix:fsync))
  (funcall point :before-install-source-parent-fsync (physical-image-fd-path source-fd))
  (sb-posix:fsync source-fd)
  (funcall point :after-install-source-parent-fsync (physical-image-fd-path source-fd))
  (funcall point :before-install-target-parent-fsync (physical-image-fd-path target-fd))
  (sb-posix:fsync target-fd)
  (funcall point :after-install-target-parent-fsync (physical-image-fd-path target-fd)))

;; At the completed move prefix, verify the immutable base and exact root
;; inventory. The only extra names are .pg_lisp.lock, physical-reseed.attempt
;; and physical-reseed.install.
(defun physical-reseed-install-check-image (root-fd plan check)
  (funcall check)
  (physical-reseed-install-test-point :before-install-image-validation (physical-image-fd-path root-fd))
  (funcall check)
  (call-with-physical-backup-image-directory
   root-fd
   (lambda (directory)
     (multiple-value-bind (manifest digest) (physical-reseed-image-base-state directory)
       (unless (and (equal manifest (getf plan :manifest))
                    (= digest (getf plan :manifest-digest)))
         (error "installed reseed base differs from its immutable plan"))
       (physical-image-check-inventory
        directory
        (append (mapcar #'first (physical-backup-metadata-bounds))
                (mapcar #'first (getf manifest :files))
                '(".pg_lisp.lock" "physical-reseed.attempt" "physical-reseed.install"))))))
  (physical-reseed-install-test-point :after-install-image-validation (physical-image-fd-path root-fd))
  (funcall check)
  t)

;; Advance at most one move, or publish the terminal step after validating the
;; installed image. A visible journal prefix is synced with its root directory
;; before another move. A partial STEP tail is truncated and synced before the
;; next complete STEP is written. A rename observed one step ahead is synced in
;; both parents before its STEP is recorded. Terminal M+1 records validation
;; after all M moves, and source cleanup remains a separate operation.
(defun physical-reseed-install-advance (owner &key helper)
  (declare (notinline sb-posix:fsync))
  (unless (physical-reseed-install-owner-p owner) (error "reseed advancement requires an install owner"))
  (physical-reseed-check-owner owner)
  (let ((record (physical-reseed-attempt-copy (physical-reseed-cleanup-owner-attempt owner))))
    (unless (eq :premanifest (getf record :phase)) (error "reseed cancellation prevents advancement"))
    (call-with-physical-reseed-install-namespace
     owner
     (lambda (root-fd incoming-fd quarantine-fd state prefix namespace-check)
       (call-with-physical-reseed-client
        owner record
        (lambda (client)
          (call-with-physical-reseed-install-progress
           owner root-fd state prefix namespace-check client record
           (lambda (fd current check ready point refresh)
             (let ((path (merge-pathnames "physical-reseed.install" (physical-image-fd-path root-fd))))
               (flet ((truncate-tail ()
                        (multiple-value-bind (before observed) (funcall current)
                          (when (getf before :partial)
                            (funcall point :before-install-progress-truncate)
                            (funcall ready)
                            (sb-posix:ftruncate fd (getf before :valid-length))
                            (funcall refresh (getf before :step) observed)
                            (funcall point :after-install-progress-truncate)
                            (sb-posix:fsync fd)
                            (funcall point :after-install-progress-truncate-fsync))))
                      (append-step ()
                        (multiple-value-bind (before observed) (funcall current)
                          (let ((step (1+ (getf before :step))))
                            (funcall point :before-install-step-write)
                            (funcall ready)
                            (let ((*physical-adoption-install-test-hook* #'physical-reseed-install-test-point))
                              (physical-adoption-install-write-bytes
                               fd (physical-reseed-install-step-image (getf before :body-crc) step)
                               (getf before :valid-length) path :limit *physical-reseed-install-progress-write-limit*
                               :point :after-install-step-write-chunk))
                            (funcall refresh step observed)
                            (funcall point :before-install-step-fsync)
                            (sb-posix:fsync fd)
                            (funcall point :after-install-step-fsync)
                            (funcall point :before-install-step-readback)
                            (funcall refresh step observed)
                            (funcall point :after-install-step-readback)))))
                 (funcall point :before-install-prefix-fsync)
                 (funcall ready)
                 (sb-posix:fsync fd)
                 (funcall point :after-install-prefix-fsync)
                 (funcall point :before-install-prefix-directory-fsync)
                 (sb-posix:fsync root-fd)
                 (funcall point :after-install-prefix-directory-fsync)
                 (multiple-value-bind (before observed) (funcall current)
                   (let* ((plan (getf before :plan)) (step (getf before :step))
                                                     (maximum (1- (physical-reseed-install-last-step plan))))
                     (when (>= step maximum)
                       (physical-reseed-install-check-image root-fd plan check)
                       (when (= step maximum)
                         (truncate-tail)
                         (append-step)))
                     (when (< step maximum)
                       (multiple-value-bind (source target name)
                           (physical-reseed-install-move-arguments plan step root-fd incoming-fd quarantine-fd)
                         (when (= step observed)
                           (truncate-tail)
                           (funcall point :before-install-move)
                           (funcall ready)
                           (unless (zerop (physical-adoption-renameat2
                                           source name target name +physical-adoption-rename-noreplace+))
                             (error "reseed no-replace move failed, errno ~d" (sb-alien:get-errno)))
                           (funcall refresh step (1+ observed))
                           (funcall point :after-install-move))
                         (physical-reseed-install-sync-parents source target point)
                         (truncate-tail)
                         (append-step)))))
                 (funcall check)
                 (multiple-value-bind (after observed) (funcall current)
                   (list :step (getf after :step) :observed observed :partial (getf after :partial))))))))
        :helper helper)))))
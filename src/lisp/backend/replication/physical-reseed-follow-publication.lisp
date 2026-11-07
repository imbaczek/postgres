(in-package #:pg)

;;; Publish and recover the local follow record.  The shared promotion path
;;; provides durable file mechanics; its caller supplies the phase-specific proof.

;; Sync the held temporary, verify canonical readback and the exact predecessor,
;; rename, sync the root, then read back and recheck the final image.  The caller
;; retains the descriptor and keeps the owner busy and uncertain throughout.

;; Promote only the checked temporary and retain its descriptor through the
;; publication checks.  Adopt the canonical post-rename snapshot before hooks
;; can observe the new journal generation.
(defun physical-reseed-follow-promote-intent (owner root-fd fd fixed candidate previous check)
  ;; A cancellation promotion claims the held temporary before mutation.
  (declare (notinline sb-posix:fsync))
  (unless (and (physical-reseed-follow-owner-busy-p owner)
               (physical-reseed-follow-owner-uncertain-p owner))
    (error "target follow promotion requires an active uncertain store scope"))
  (physical-reseed-follow-cancellation-claim owner root-fd candidate previous :promote :fd fd)
  (let ((name "physical-reseed.follow.tmp")
        (path (merge-pathnames "physical-reseed.follow" (physical-image-fd-path root-fd)))
        (renamed nil))
    (labels ((guard ()
               (funcall check)
               (unless renamed (physical-reseed-follow-check-snapshot root-fd previous))
               (physical-reseed-follow-store-fd-check root-fd fd name fixed))
             (point (event)
               (guard)
               (physical-reseed-follow-test-point event path)
               (guard)))
      (point :before-follow-temporary-fsync)
      (sb-posix:fsync fd)
      (point :after-follow-temporary-fsync)
      (point :before-follow-temporary-readback)
      (multiple-value-bind (actual identity) (physical-reseed-follow-read-at root-fd name)
        (unless (equal candidate actual) (error "target follow temporary readback differs"))
        (let ((row (nth-value 1 (physical-reseed-follow-entry root-fd name))))
          (point :after-follow-temporary-readback)
          (point :before-follow-rename)
          (unless (and (equal identity (guard))
                       (equal row (nth-value 1 (physical-reseed-follow-entry root-fd name))))
            (error "target follow temporary changed before promotion"))
          (sb-sys:without-interrupts
            (unless (zerop (if previous
                               (port-renameat root-fd name root-fd "physical-reseed.follow")
                               (physical-adoption-renameat2
                                root-fd name root-fd "physical-reseed.follow" +physical-adoption-rename-noreplace+)))
              (error "cannot promote target follow intent: errno ~d" (sb-alien:get-errno)))
            (setf renamed t name "physical-reseed.follow"))
          (let ((published (physical-reseed-follow-snapshot-at root-fd)))
            (unless (and (equal candidate (getf published :record))
                         (equal (subseq identity 0 7) (subseq (getf published :identity) 0 7))
                         (equal (subseq row 1 8) (subseq (getf published :row) 1 8)))
              (error "target follow final changed at the publication boundary"))
            (physical-reseed-follow-published-snapshot owner root-fd candidate published previous))
          (point :after-follow-rename)
          (sb-posix:fsync root-fd)
          (point :after-follow-directory-fsync)
          (point :before-follow-readback)
          (let ((snapshot (physical-reseed-follow-snapshot-at root-fd)))
            (unless (and (equal candidate (getf snapshot :record))
                         (equal (subseq identity 0 7) (subseq (getf snapshot :identity) 0 7))
                         (equal (subseq row 1 8) (subseq (getf snapshot :row) 1 8)))
              (error "target follow final readback differs"))
            (point :after-follow-readback)
            (physical-reseed-follow-check-snapshot root-fd snapshot)
            (funcall check)
            (values (physical-reseed-follow-copy candidate) snapshot)))))))

;; Create the private temporary exclusively and retain its descriptor through
;; writing and promotion.  Latch uncertainty before the first possible mutation;
;; an incomplete exit preserves the temporary or final entry for a new owner.

(defun physical-reseed-follow-write-intent (owner root-fd candidate bytes previous check)
  ;; A cancellation write claims its typed authority before creating the file.
  (declare (notinline sb-posix:close))
  (physical-reseed-follow-cancellation-claim owner root-fd candidate previous :write :bytes bytes)
  (let ((fd nil) (fixed nil)
                 (path (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root-fd))))
    (unwind-protect
         (progn
           (funcall check)
           (physical-reseed-follow-check-snapshot root-fd previous)
           (physical-reseed-follow-test-point :before-follow-temporary-create path)
           (funcall check)
           (physical-reseed-follow-check-snapshot root-fd previous)
           (physical-adoption-require-absent path "target follow temporary requires reconciliation")
           (setf (physical-reseed-follow-owner-uncertain-p owner) t)
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       root-fd "physical-reseed.follow.tmp"
                       (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                               sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) #o600)))
           (when (minusp fd) (setf fd nil) (error "cannot exclusively create target follow temporary"))
           (sb-posix:fchmod fd #o600)
           (setf fixed (subseq (physical-reseed-follow-file-identity (sb-posix:fstat fd) t) 0 5))
           (flet ((held ()
                    (physical-directory-check-owner-common owner)
                    (physical-reseed-follow-store-fd-check root-fd fd "physical-reseed.follow.tmp" fixed))
                  (proof () (funcall check) (physical-reseed-follow-check-snapshot root-fd previous)))
             (held)
             (physical-reseed-follow-test-point :after-follow-temporary-create path)
             (proof) (held)
             (let ((*physical-adoption-install-test-hook*
                     (lambda (event where)
                       (held)
                       (when *physical-reseed-follow-test-hook*
                         (physical-reseed-follow-test-point event where)
                         (proof) (held)))))
               (physical-adoption-install-write-bytes
                fd bytes 0 path :limit *physical-reseed-follow-write-limit* :point :after-follow-write-chunk))
             (unless (= (length bytes) (sixth (held)))
               (error "target follow temporary length differs from its frozen image"))
             (physical-reseed-follow-cancellation-write-complete owner root-fd fd fixed candidate previous)
             (physical-reseed-follow-promote-intent owner root-fd fd fixed candidate previous check)))
      (when fd (sb-posix:close fd)))))

;; Freeze the candidate, predecessor and encoded bytes before file I/O.  Check
;; the exact final snapshot and local image evidence before publishing the intent.

(defun physical-reseed-follow-publish-intent (owner candidate &key previous)
  (let* ((candidate (physical-reseed-follow-intent-copy candidate previous))
         (previous (and previous (physical-reseed-follow-intent-copy previous)))
         (bytes (physical-reseed-follow-image candidate)))
    (physical-reseed-follow-check-idle owner)
    (physical-reseed-follow-check-binding owner candidate)
    (call-with-physical-reseed-follow-store-proof
     owner
     (lambda (root-fd check)
       (let ((snapshot (physical-reseed-follow-snapshot-at root-fd)))
         (unless (equal previous (getf snapshot :record))
           (error "target follow publication requires its exact previous record"))
         (physical-reseed-follow-check-local-intent owner root-fd candidate)
         (physical-reseed-follow-write-intent owner root-fd candidate bytes snapshot check))))))

;; Inspect a temporary before mutation.  Promotion requires an active attempt,
;; the exact predecessor and phase-specific local evidence.  A stable PROVED
;; final can be recovered after abort, but a temporary cannot.  Malformed or
;; unsupported temporaries remain unchanged and fenced.

(defun physical-reseed-follow-recover-intent (owner)
  (physical-reseed-follow-check-idle owner)
  (call-with-physical-reseed-follow-attempt-proof
   owner
   (lambda (root-fd check)
     (block settled
       (let* ((previous (physical-reseed-follow-snapshot-at root-fd))
              (temporary (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp" t))
              (fd nil))
         (if previous
             (physical-reseed-follow-check-stable-record-at owner root-fd (getf previous :record))
             (physical-reseed-follow-check-active-attempt owner))
         (unless temporary
           (when previous
             (setf (physical-reseed-follow-owner-uncertain-p owner) t)
             (physical-reseed-follow-read-at root-fd "physical-reseed.follow" :stabilize t))
           (physical-reseed-follow-check-snapshot root-fd previous)
           (funcall check)
           (return-from settled
             (values (and previous (physical-reseed-follow-copy (getf previous :record))) previous)))
         (let* ((candidate (physical-reseed-follow-recovery-candidate-at
                            owner root-fd (physical-reseed-follow-read-at root-fd "physical-reseed.follow.tmp")
                            (getf previous :record)))
                (identity (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp")))
           (physical-reseed-follow-check-local-intent owner root-fd candidate)
           (funcall check)
           (physical-reseed-follow-check-snapshot root-fd previous)
           (setf (physical-reseed-follow-owner-uncertain-p owner) t)
           (unwind-protect
                (progn
                  (sb-sys:without-interrupts
                    (setf fd (physical-image-openat
                              root-fd "physical-reseed.follow.tmp"
                              (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) 0)))
                  (when (minusp fd) (setf fd nil) (error "cannot open target follow recovery temporary"))
                  (unless (equal identity (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                    (error "target follow recovery temporary changed"))
                  (physical-reseed-follow-promote-intent
                   owner root-fd fd (subseq identity 0 5) candidate previous
                   (lambda ()
                     (funcall check)
                     (physical-reseed-follow-check-active-attempt owner))))
             (when fd (sb-posix:close fd)))))))))

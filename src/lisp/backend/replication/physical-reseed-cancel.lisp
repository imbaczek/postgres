(in-package #:pg)

;; Tests can limit repair writes to exercise partial replacement images.
(defvar *physical-reseed-cancel-write-limit* nil)

;; Read the bounded temporary image through its held descriptor. Check the
;; descriptor, mount and named entry before and after reading the exact bytes.
(defun physical-reseed-cancel-temporary-bytes (root-fd fd stream mount)
  (let* ((name "physical-reseed.attempt.tmp")
         (before (physical-reseed-install-old-observe root-fd name mount))
         (stat (sb-posix:fstat fd))
         (identity (physical-reseed-attempt-file-identity stat t))
         (bytes (make-array (sixth identity) :element-type '(unsigned-byte 8))))
    (unless (and (equal (physical-reseed-install-row-file-facts before)
                        (physical-reseed-file-facts stat))
                 (= mount (physical-adoption-install-mount-id nil :directory-fd fd)))
      (error "cancellation temporary lost its descriptor binding"))
    (file-position stream 0)
    (unless (and (= (length bytes) (read-sequence bytes stream))
                 (eq :eof (read-byte stream nil :eof))
                 (equal identity (physical-reseed-attempt-file-identity (sb-posix:fstat fd) t))
                 (equal before (physical-reseed-install-old-observe root-fd name mount)))
      (error "cancellation temporary changed while reading"))
    (values bytes before)))

;; Derive canonical same-phase and next-phase images from the trusted private
;; predecessor. The transition check supplies the allowed phase boundaries.
(defun physical-reseed-cancel-candidates (previous)
  (loop for phase in (ecase (getf previous :phase)
                       (:premanifest '(:premanifest :aborting))
                       (:aborting '(:aborting :aborted))
                       (:aborted '(:aborted)))
        collect (let ((candidate (physical-reseed-attempt-copy previous)))
                  (setf (getf candidate :phase) phase)
                  (physical-reseed-attempt-check-transition previous candidate)
                  (physical-reseed-attempt-image candidate))))

;; Keep the root and temporary descriptors open while FUNCTION examines or
;; repairs the candidate. The temporary must be a private regular file with
;; mode 0600 and one link. CHECK and REFRESH belong to this scope and recheck
;; the predecessor and nanosecond entry metadata around their observations.
;; A successful promotion changes the named entries, so the caller performs its
;; checks before promotion instead of applying a generic check after FUNCTION.
(defun call-with-physical-reseed-cancel-temporary (owner function)
  (physical-reseed-check-attempt-authority owner)
  (call-with-physical-directory-root
   owner
   (lambda (root-fd directory)
     (declare (ignore directory))
     (let* ((root (physical-image-fd-path root-fd))
            (path (merge-pathnames "physical-reseed.attempt.tmp" root))
            (previous (physical-reseed-attempt-read-at root-fd "physical-reseed.attempt" :stabilize t))
            (mount (physical-adoption-install-mount-id nil :directory-fd root-fd))
            (previous-row (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount))
            (fd nil) (stream nil) (row nil) (bytes nil) (active nil))
       (labels ((live ()
                  (unless active (error "cancellation temporary scope has ended")))
                (check-root ()
                  (live)
                  (physical-reseed-attempt-check-owner owner previous)
                  (unless (and (equal previous
                                      (physical-reseed-attempt-read-at root-fd "physical-reseed.attempt"))
                               (equal previous-row
                                      (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount)))
                    (error "cancellation predecessor changed")))
                (check ()
                  (check-root)
                  (multiple-value-bind (actual actual-row)
                      (physical-reseed-cancel-temporary-bytes root-fd fd stream mount)
                    (unless (and (equal row actual-row) (equalp bytes actual))
                      (error "cancellation temporary changed outside normalization"))))
                (refresh (expected)
                  (check-root)
                  (multiple-value-bind (actual actual-row)
                      (physical-reseed-cancel-temporary-bytes root-fd fd stream mount)
                    (unless (and (equal (subseq row 0 6) (subseq actual-row 0 6))
                                 (equalp expected actual))
                      (error "cancellation normalization readback differs"))
                    (setf row actual-row bytes actual))))
         (unwind-protect
              (progn
                (sb-sys:without-interrupts
                  (setf fd (physical-image-openat
                            root-fd "physical-reseed.attempt.tmp"
                            (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                    +postmaster-o-cloexec+) 0)))
                (when (minusp fd) (setf fd nil) (error "cannot open cancellation temporary"))
                (physical-reseed-attempt-file-identity (sb-posix:fstat fd) t)
                (sb-sys:without-interrupts
                  (setf stream (sb-sys:make-fd-stream fd :input t :output t
                                                         :element-type '(unsigned-byte 8) :buffering :none)))
                (multiple-value-setq (bytes row)
                  (physical-reseed-cancel-temporary-bytes root-fd fd stream mount))
                (setf active t)
                (check)
                (funcall function root-fd fd path previous bytes #'check #'refresh))
           (setf active nil)
           (if stream (close stream) (when fd (sb-posix:close fd)))))))))

;; Replace an authorized partial image with ABORTING copied from the trusted
;; predecessor. Sync the empty file after truncation before writing any new bytes,
;; so a crash cannot leave a new prefix followed by an old tail. Read back the
;; complete image and sync it before using the existing checked promotion.
(defun physical-reseed-cancel-normalize-temporary (owner root-fd fd path previous check refresh)
  ;; Keep calls visible to tests that inject syscall failures.
  (declare (notinline sb-posix:fsync))
  (physical-reseed-check-attempt-authority owner)
  (let* ((candidate (physical-reseed-attempt-copy previous))
         (empty (make-array 0 :element-type '(unsigned-byte 8))))
    (setf (getf candidate :phase) :aborting)
    (physical-reseed-attempt-check-transition previous candidate)
    (let ((image (physical-reseed-attempt-image candidate)))
      (flet ((point (event)
               (physical-reseed-attempt-test-point event path)
               (funcall check)))
        (point :before-cancel-repair-truncate)
        (sb-posix:ftruncate fd 0)
        (funcall refresh empty)
        (point :after-cancel-repair-truncate)
        (sb-posix:fsync fd)
        (point :after-cancel-repair-empty-fsync)
        (let ((*physical-adoption-install-test-hook* #'physical-reseed-attempt-test-point))
          (physical-adoption-install-write-bytes
           fd image 0 path :limit *physical-reseed-cancel-write-limit*
           :point :after-cancel-repair-write-chunk))
        (funcall refresh image)
        (point :after-cancel-repair-write)
        (sb-posix:fsync fd)
        (point :after-cancel-repair-file-fsync)
        (let ((temporary-identity (physical-reseed-attempt-file-identity (sb-posix:fstat fd)))
              (previous-identity
                (physical-reseed-attempt-file-identity
                 (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.attempt"
                                                              (physical-image-fd-path root-fd)))))))
          (funcall check)
          (physical-reseed-attempt-promote-temporary
           owner candidate temporary-identity previous previous-identity))))))

;; Full canonical candidates go through the ordinary recovery checks. This also
;; covers a complete ABORTED candidate whose trusted publisher checked the source
;; receipt before writing it. The receipt itself is not stored in the journal.
;;
;; With a predecessor, only a strict prefix of an allowed same-phase or next-phase
;; image can be repaired, and the repair writes the predecessor as ABORTING.
;; Missing final or temporary entries go through ordinary recovery, which can
;; recover a complete initial premanifest temporary and fences a partial one
;; without a final predecessor. This path needs no stage or incoming directory.
(defun physical-reseed-attempt-recover-for-cancel (owner)
  (physical-reseed-check-attempt-authority owner)
  (physical-reseed-check-owner owner)
  (let* ((directory (physical-adoption-owner-directory owner))
         (temporary (physical-reseed-attempt-temporary-path directory))
         (final (physical-reseed-attempt-path directory)))
    (unless (and (physical-adoption-path-present-p temporary)
                 (physical-adoption-path-present-p final))
      (return-from physical-reseed-attempt-recover-for-cancel (physical-reseed-attempt-recover owner))))
  (call-with-physical-reseed-cancel-temporary
   owner
   (lambda (root-fd fd path previous bytes check refresh)
     (let ((images (physical-reseed-cancel-candidates previous)))
       (when (find bytes images :test #'equalp)
         (return-from physical-reseed-attempt-recover-for-cancel (physical-reseed-attempt-recover owner)))
       (unless (and (member (getf previous :phase) '(:premanifest :aborting))
                    (some (lambda (image)
                            (and (< (length bytes) (length image))
                                 (equalp bytes (subseq image 0 (length bytes)))))
                          images))
         (error "cancellation temporary is not an authorized canonical prefix"))
       (funcall check)
       (physical-reseed-cancel-normalize-temporary owner root-fd fd path previous check refresh)))))

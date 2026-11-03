(in-package #:pg)

;; The final and temporary files are startup fences in every phase, including
;; ABORTED.  Their contents are private: they include a bearer token and
;; reversible connection credentials.  Do not log records or their bytes.

(defvar *physical-reseed-attempt-test-hook* nil)

(defun physical-reseed-attempt-test-point (point path)
  (when *physical-reseed-attempt-test-hook*
    (funcall *physical-reseed-attempt-test-hook* point path)))

(defun physical-reseed-attempt-check-owner (owner record)
  (physical-reseed-check-owner owner)
  (let ((*pgdata* (physical-adoption-owner-directory owner)))
    (physical-adoption-check-startup))
  (unless (equal (getf record :local) (physical-reseed-attempt-local-facts owner))
    (error "reseed attempt belongs to another stopped directory or PID generation"))
  owner)

(defun physical-reseed-attempt-file-identity (stat &optional allow-empty)
  (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
               (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
               (= #o600 (logand #o7777 (sb-posix:stat-mode stat)))
               (= 1 (sb-posix:stat-nlink stat))
               (<= (if allow-empty 0 1) (sb-posix:stat-size stat) +physical-reseed-attempt-maximum-bytes+))
    (error "reseed metadata is not a bounded private single-link regular file"))
  (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
        (sb-posix:stat-mode stat) (sb-posix:stat-uid stat) (sb-posix:stat-nlink stat)
        (sb-posix:stat-size stat) (sb-posix:stat-mtime stat) (sb-posix:stat-ctime stat)))

;; Check the bounded, owned, mode 0600 single-link file against both its open
;; descriptor and pathname.  Stabilization syncs the file and directory before
;; returning the checked record.

(defun physical-reseed-attempt-read-file (owner path &key (stabilize t))
  (physical-reseed-check-owner owner)
  (let ((directory (physical-adoption-owner-directory owner)))
    (unless (member path (list (physical-reseed-attempt-path directory)
                               (physical-reseed-attempt-temporary-path directory)) :test #'equal)
      (error "unexpected reseed metadata pathname"))
    (multiple-value-bind (stream size)
        (open-bounded-regular-file path +physical-reseed-attempt-maximum-bytes+ stabilize)
      (unwind-protect
           (let* ((before (physical-reseed-attempt-file-identity
                           (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                  (bytes (make-array size :element-type '(unsigned-byte 8))))
             (unless (= size (sixth before)) (error "reseed metadata changed before reading"))
             (when stabilize (fsync-stream stream) (fsync-directory directory))
             (unless (and (= size (read-sequence bytes stream))
                          (equal before (physical-reseed-attempt-file-identity
                                         (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                          (equal before (physical-reseed-attempt-file-identity
                                         (sb-posix:lstat (namestring path)))))
               (error "reseed metadata changed while reading"))
             (let ((record (physical-reseed-attempt-decode bytes)))
               (physical-reseed-attempt-check-owner owner record)
               record))
        (close stream)))))

;; Ordinary reads require the temporary name to be absent; recovery must
;; reconcile it before the final record can be used.

(defun physical-reseed-attempt-read (owner)
  (physical-reseed-check-owner owner)
  (let ((directory (physical-adoption-owner-directory owner)))
    (physical-adoption-require-absent
     (physical-reseed-attempt-temporary-path directory)
     "reseed temporary intent requires reconciliation before remote operations")
    (physical-reseed-attempt-read-file owner (physical-reseed-attempt-path directory))))

;; An ABORTED intent requires a caller-supplied typed CLOSED/ABORTED receipt
;; matching the original canonical request.  The future RPC driver must observe
;; the source.  This structural check runs before any I/O, including direct
;; calls to the temporary writer.

(defun physical-reseed-attempt-check-abort-receipt (candidate receipt)
  (when (eq :aborted (getf candidate :phase))
    (let ((receipt (when receipt (physical-backup-lease-copy receipt))))
      (unless (and receipt (eq :closed (getf receipt :phase))
                   (eq :aborted (getf receipt :outcome))
                   (equal (getf candidate :request) (getf receipt :request)))
        (error "aborted reseed intent requires its original closed source receipt"))
      receipt)))

;; Exclusive creation retains this owned temporary inode on failure.  The
;; generic atomic-output helper replaces or cleans temporary files and cannot
;; preserve this fence.

(defun physical-reseed-attempt-write-temporary (owner record &key receipt)
  (physical-reseed-attempt-check-abort-receipt record receipt)
  (physical-reseed-attempt-check-owner owner record)
  (let* ((path (physical-reseed-attempt-temporary-path (physical-adoption-owner-directory owner)))
         (bytes (physical-reseed-attempt-image record))
         (fd nil) (stream nil))
    (unwind-protect
         (progn
           (setf fd (sb-posix:open (namestring path)
                                   (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl
                                           sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
           (sb-posix:fchmod fd #o600)
           (physical-reseed-attempt-file-identity (sb-posix:fstat fd) t)
           (physical-reseed-attempt-test-point :after-create path)
           (setf stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8))
                 fd nil)
           (write-sequence bytes stream)
           (finish-output stream)
           (physical-reseed-attempt-test-point :after-write path)
           (fsync-stream stream)
           (physical-reseed-attempt-test-point :after-file-fsync path)
           (physical-reseed-attempt-file-identity (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
      (unwind-protect
           (when stream (close stream))
        (when fd (sb-posix:close fd))))))

(defun physical-reseed-attempt-check-previous (owner previous identity)
  (let ((path (physical-reseed-attempt-path (physical-adoption-owner-directory owner))))
    (if previous
        (unless (and (equal identity
                            (physical-reseed-attempt-file-identity (sb-posix:lstat (namestring path))))
                     (equal previous (physical-reseed-attempt-read-file owner path)))
          (error "reseed attempt changed before publication"))
        (physical-adoption-require-absent path "reseed attempt already exists"))))

;; Promote the complete temporary inode after checking it and its predecessor.
;; Like slot state publication, sync before rename and sync the directory after.
;; Success also requires exact readback.  After temporary creation, a failure
;; before rename leaves that inode as a fence; after rename, the final name
;; remains the fence even if directory sync or readback fails.

(defun physical-reseed-attempt-promote-temporary (owner candidate temporary-identity previous previous-identity)
  ;; The caller has already written or recovered the complete temporary inode.
  (physical-reseed-attempt-check-owner owner candidate)
  (let* ((directory (physical-adoption-owner-directory owner))
         (path (physical-reseed-attempt-path directory))
         (temporary (physical-reseed-attempt-temporary-path directory)))
    (physical-reseed-attempt-test-point :before-rename path)
    (physical-reseed-attempt-check-owner owner candidate)
    (unless (and (equal temporary-identity
                        (physical-reseed-attempt-file-identity (sb-posix:lstat (namestring temporary))))
                 (equal candidate (physical-reseed-attempt-read-file owner temporary)))
      (error "reseed temporary intent changed before publication"))
    (physical-reseed-attempt-check-previous owner previous previous-identity)
    (sb-posix:rename (namestring temporary) (namestring path))
    (physical-reseed-attempt-test-point :after-rename path)
    (fsync-directory directory)
    (physical-reseed-attempt-test-point :after-directory-fsync path)
    (physical-reseed-attempt-test-point :before-readback path)
    (let ((actual (physical-reseed-attempt-read owner)))
      (unless (equal candidate actual) (error "reseed attempt readback differs"))
      (physical-reseed-attempt-test-point :after-readback path)
      actual)))

(defun physical-reseed-attempt-publish (owner candidate &key previous receipt)
  (let* ((candidate (physical-reseed-attempt-copy candidate))
         (receipt (physical-reseed-attempt-check-abort-receipt candidate receipt))
         (previous (when previous (physical-reseed-attempt-copy previous)))
         (owner (physical-reseed-attempt-check-owner owner candidate))
         (directory (physical-adoption-owner-directory owner))
         (path (physical-reseed-attempt-path directory))
         (temporary (physical-reseed-attempt-temporary-path directory))
         (previous-identity nil))
    (physical-adoption-require-absent temporary "reseed temporary intent requires reconciliation")
    (if previous
        (progn
          (unless (equal previous (physical-reseed-attempt-read owner))
            (error "reseed attempt changed before publication"))
          (physical-reseed-attempt-check-transition previous candidate)
          (setf previous-identity
                (physical-reseed-attempt-file-identity (sb-posix:lstat (namestring path)))))
        (progn
          (physical-adoption-require-absent path "reseed attempt already exists")
          (unless (eq :premanifest (getf candidate :phase))
            (error "a new reseed attempt must start before manifest receipt"))))
    (let ((temporary-identity (physical-reseed-attempt-write-temporary owner candidate :receipt receipt)))
      (physical-reseed-attempt-promote-temporary owner candidate temporary-identity previous previous-identity))))

;; Reconcile only a complete canonical temporary record for this owner and a
;; valid transition.  A complete ABORTED temporary record was receipt-checked
;; before it was written, so recovery does not need another source RPC.  Partial
;; or malformed files remain fences; do not recapture facts, choose a new ID,
;; or unlink them through this interface.

(defun physical-reseed-attempt-recover (owner)
  (physical-reseed-check-owner owner)
  (let* ((directory (physical-adoption-owner-directory owner))
         (path (physical-reseed-attempt-path directory))
         (temporary (physical-reseed-attempt-temporary-path directory)))
    (unless (physical-adoption-path-present-p temporary)
      (return-from physical-reseed-attempt-recover (physical-reseed-attempt-read owner)))
    (let* ((candidate (physical-reseed-attempt-read-file owner temporary))
           (temporary-identity (physical-reseed-attempt-file-identity (sb-posix:lstat (namestring temporary))))
           (previous (when (physical-adoption-path-present-p path)
                       (physical-reseed-attempt-read-file owner path)))
           (previous-identity (when previous
                                (physical-reseed-attempt-file-identity (sb-posix:lstat (namestring path))))))
      (if previous
          (physical-reseed-attempt-check-transition previous candidate)
          (unless (eq :premanifest (getf candidate :phase))
            (error "temporary reseed intent has no predecessor")))
      (physical-reseed-attempt-promote-temporary owner candidate temporary-identity previous previous-identity))))

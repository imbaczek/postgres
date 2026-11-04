(in-package #:pg)

;; Installation image inspection.  Hold the original frozen stage and its lock
;; for the callback, checking its identity before and after use.  The check
;; closure is valid only within this scope.
(defun call-with-physical-reseed-install-stage (owner root-fd record function)
  (physical-reseed-require-journal owner record)
  (let* ((expected (physical-reseed-attempt-stage-copy (getf record :stage) (getf record :local)))
         (directory (pathname (getf expected :directory))) (fd nil) (active nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
           (unless (zerop (physical-store-flock fd 6)) (error "reseed install stage is already owned"))
           (setf active t)
           (flet ((check ()
                    (unless active (error "reseed install stage scope has ended"))
                    (unless (equal expected (physical-reseed-install-auxiliary-base-facts owner root-fd directory fd))
                      (error "reseed install stage identity changed"))))
             (check)
             (multiple-value-prog1 (funcall function directory fd #'check)
               (check))))
      (setf active nil)
      (when fd (sb-posix:close fd)))))

(defun physical-reseed-install-file-row-at (parent leaf name size expected-digest mount synchronize)
  (let ((before (physical-reseed-install-old-observe parent leaf mount)))
    (call-with-physical-image-input-at
     parent leaf
     (lambda (input)
       (let* ((fd (sb-sys:fd-stream-fd input))
              (facts (physical-reseed-install-row-file-facts before))
              (digest #xCBF29CE484222325)
              (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
         (unless (and (= size (seventh before))
                      (equal facts (physical-reseed-file-facts (sb-posix:fstat fd)))
                      (= mount (physical-adoption-install-mount-id nil :directory-fd fd)))
           (error "reseed image descriptor differs from its named file"))
         (loop with remaining = size
               while (plusp remaining)
               for count = (read-sequence buffer input :end (min remaining (length buffer)))
               do (when (zerop count) (error "reseed image file was truncated"))
                  (setf digest (physical-store-update-digest digest (subseq buffer 0 count)))
                  (decf remaining count))
         (unless (and (eq :eof (read-byte input nil :eof)) (= digest expected-digest))
           (error "reseed image file differs from its recorded digest"))
         (when synchronize (sb-posix:fsync fd))
         (unless (and (equal facts (physical-reseed-file-facts (sb-posix:fstat fd)))
                      (equal before (physical-reseed-install-old-observe parent leaf mount)))
           (error "reseed image file changed while capturing its identity"))
         (append (list (copy-seq name)) (rest before) (list digest))))
     :element-type '(unsigned-byte 8))))

;; Bind each image entry to the named statx observation and held descriptor.
;; Read exactly the expected size and digest, optionally sync the file, and
;; recheck both observations before returning its inventory row.
(defun physical-reseed-install-image-file-row (context entry mount synchronize)
  (destructuring-bind (name size expected-digest) entry
    (multiple-value-bind (parent leaf) (physical-image-parent context name)
      (physical-reseed-install-file-row-at parent leaf name size expected-digest mount synchronize))))

;; Bind a named image directory to its held descriptor and optionally sync it.
;; A changed name or descriptor invalidates the captured row.
(defun physical-reseed-install-image-directory-row (context name mount synchronize)
  (let* ((fd (cdr (assoc name (physical-image-context-children context) :test #'equal)))
         (root (physical-image-context-root-fd context))
         (before (physical-reseed-install-old-observe root name mount)))
    (unless (and fd (equal (physical-reseed-install-row-file-facts before)
                           (physical-reseed-file-facts (physical-image-check-directory fd))))
      (error "reseed incoming directory descriptor changed"))
    (when synchronize (sb-posix:fsync fd))
    (unless (and (equal before (physical-reseed-install-old-observe root name mount))
                 (equal (physical-reseed-install-row-file-facts before)
                        (physical-reseed-file-facts (sb-posix:fstat fd))))
      (error "reseed incoming directory changed while capturing its identity"))
    (append (list (copy-seq name)) (rest before) (list nil))))

;; Capture sorted file and directory rows within a read scope.  Synchronization
;; flushes each file and child directory, then the image root directory.
(defun physical-reseed-install-image-rows (context image &key synchronize)
  (unless (physical-image-read-context-p context) (error "reseed install inventory requires a read scope"))
  (physical-image-check-children context)
  (unless (<= (length (getf image :files)) 99997) (error "reseed incoming image exceeds the install entry bound"))
  (let* ((fd (physical-image-context-root-fd context))
         (mount (physical-adoption-install-mount-id nil :directory-fd fd))
         (files (loop for entry in (getf image :files)
                      collect (physical-reseed-install-image-file-row context entry mount synchronize)))
         (directories (loop for name in '("base" "global" "pg_wal")
                            collect (physical-reseed-install-image-directory-row context name mount synchronize))))
    (when synchronize (sb-posix:fsync fd))
    (physical-image-check-children context)
    (sort (append files directories) #'string< :key #'first)))

;; Only the auxiliary lock and install fence may occupy a new quarantine.
(defun physical-reseed-install-check-empty-quarantine (fd)
  (unless (equal '(".pg_lisp.lock" "physical-reseed.install")
                 (sort (directory-entry-names (physical-image-fd-path fd)
                                              :follow-symlinks t :maximum-entries 2 :maximum-name-bytes 128) #'string<))
    (error "reseed quarantine has unexpected entries")))

;; Verify the incoming image before preparing its auxiliary ownership and return
;; the directory facts that the later scope must match.
(defun physical-reseed-install-preflight-incoming (owner incoming image)
  (call-with-physical-reseed-stage
   owner incoming
   (lambda (directory facts fd)
     (declare (ignore directory))
     (call-with-physical-backup-image-directory fd (lambda (root) (physical-reseed-image-check root image)))
     facts)
   :empty nil))

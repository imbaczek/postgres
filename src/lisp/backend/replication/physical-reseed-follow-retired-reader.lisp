(in-package #:pg)

(defun physical-reseed-follow-retired-geometry (follow receipt)
  ;; Observe the three retirement layouts: follow only, two names for the
  ;; same inode, or receipt only.  This does not admit the full layout,
  ;; archive, or startup state.
  (let ((links (if (and follow receipt) 2 1)))
    (unless (and (or follow receipt)
                 (or (null follow)
                     (and (listp follow) (= 9 (length follow))
                          (equal "physical-reseed.follow" (first follow))
                          (eql links (fifth follow))))
                 (or (null receipt)
                     (and (listp receipt) (= 9 (length receipt))
                          (equal "physical-reseed.retired" (first receipt))
                          (eql links (fifth receipt))))
                 (or (not (and follow receipt)) (equal (rest follow) (rest receipt))))
      (error "retired journal requires exact follow and receipt link geometry")))
  (cond ((null receipt) :follow) ((null follow) :receipt) (t :alias)))

(defun physical-reseed-follow-retired-rows (root check)
  ;; ROOT is borrowed from the caller.  CHECK guards its parent scope;
  ;; retain exact file facts and mount identity for later comparisons.
  (funcall check)
  (let* ((stat (physical-image-check-directory root))
         (mount (physical-adoption-install-mount-id nil :directory-fd root))
         (rows
           (loop for name in '("physical-reseed.follow" "physical-reseed.retired")
                 collect
                 (when (physical-adoption-path-present-p
                        (merge-pathnames name (physical-image-fd-path root)))
                   (let ((row (physical-reseed-install-old-observe root name mount)))
                     (unless (and (= (sb-posix:stat-dev stat) (second row))
                                  (= #o100600 (fourth row))
                                  (= (sb-posix:geteuid) (sixth row))
                                  (<= 1 (seventh row) +physical-reseed-follow-maximum-bytes+))
                       (error "retired journal requires a bounded private regular file"))
                     row)))))
    (physical-adoption-require-absent
     (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
     "retired journal refuses a follow temporary")
    (let ((geometry (physical-reseed-follow-retired-geometry (first rows) (second rows))))
      (funcall check)
      (list :geometry geometry :follow (first rows) :receipt (second rows) :mount mount))))

(defun physical-reseed-follow-retired-check-root (root record check)
  (funcall check)
  (let* ((local (getf (getf record :attempt) :local))
         (directory (pathname (getf local :directory)))
         (stat (physical-image-check-directory root)))
    (unless (and (= (getf local :device) (sb-posix:stat-dev stat))
                 (= (getf local :inode) (sb-posix:stat-ino stat))
                 (equal (namestring directory) (namestring (truename directory)))
                 (= (physical-adoption-install-mount-id directory)
                    (physical-adoption-install-mount-id nil :directory-fd root)))
      (error "retired journal differs from its canonical target root"))
    (physical-image-check-target directory root))
  (funcall check)
  t)

(defun physical-reseed-follow-retired-check-held (root fd observation check)
  (funcall check)
  (unless (and (equal observation (physical-reseed-follow-retired-rows root check))
               (equal (rest (or (getf observation :follow) (getf observation :receipt)))
                      (physical-reseed-follow-retirement-held-file-facts fd (getf observation :mount))))
    (error "retired journal changed its named or held facts"))
  t)

(defun call-with-physical-reseed-follow-retired-journal (root function check)
  (declare (notinline physical-image-openat))
  (let* ((observation (physical-reseed-follow-retired-rows root check))
         (row (or (getf observation :follow) (getf observation :receipt)))
         (name (first row))
         (path (merge-pathnames name (physical-image-fd-path root)))
         (bytes (make-array (seventh row) :element-type '(unsigned-byte 8)))
         (current (make-array (length bytes) :element-type '(unsigned-byte 8)))
         (record nil) (fd nil) (stream nil) (active nil) (thread sb-thread:*current-thread*))
    ;; Own the opened descriptor until cleanup.  Read a bounded canonical
    ;; Retired2 image and compare both its named and held facts.
    (multiple-value-prog1
        (unwind-protect
             (progn
               (sb-sys:without-interrupts
                 (setf fd (physical-image-openat
                           root name (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                             +postmaster-o-cloexec+) 0))
                 (when (minusp fd) (setf fd nil) (error "cannot open retired journal")))
               (sb-sys:without-interrupts
                 (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8))))
               (physical-reseed-follow-test-point :after-retired-journal-open path)
               (flet ((verify-row ()
                        (unless (and active (eq thread sb-thread:*current-thread*))
                          (error "retired journal scope ended or changed thread"))
                        (when record (physical-reseed-follow-retired-check-root root record check))
                        (physical-reseed-follow-retired-check-held root fd observation check)))
                 (setf active t)
                 (verify-row)
                 (unless (and (= (length bytes) (read-sequence bytes stream))
                              (eq :eof (read-byte stream nil :eof)))
                   (error "retired journal changed while reading"))
                 (verify-row)
                 (setf record (physical-reseed-follow-decode bytes))
                 (unless (and (eq :retired (getf (getf record :retirement) :phase))
                              (eql 2 (getf (getf record :retirement) :step)))
                   (error "retired journal requires the canonical Retired2 image"))
                 (verify-row)
                 ;; The generic tree copier preserves vectors.  Copy the bytes
                 ;; explicitly so the returned state cannot change our baseline.
                 (let ((state (append observation
                                      (list :snapshot
                                            (list :record record
                                                  :identity (physical-reseed-install-row-file-facts row)
                                                  :row row)
                                            :bytes (copy-seq bytes)))))
                   ;; The callback may verify only on this thread and while the
                   ;; descriptor is open.  Cleanup revokes it before close.
                   (flet ((verify ()
                            (verify-row)
                            (file-position stream 0)
                            (unless (and (= (length current) (read-sequence current stream))
                                         (eq :eof (read-byte stream nil :eof))
                                         (equalp bytes current))
                              (error "retired journal changed its held bytes"))
                            (verify-row)
                            t))
                     (verify)
                     (multiple-value-prog1
                         (funcall function stream (physical-adoption-attempt-value-copy state) #'verify)
                       (verify))))))
          (setf active nil)
          (if stream (close stream) (when fd (sb-posix:close fd))))
      ;; Check the parent and named files once more after descriptor cleanup.
      (funcall check)
      (physical-reseed-follow-test-point :after-retired-journal-close path)
      (physical-reseed-follow-retired-check-root root record check)
      (unless (equal observation (physical-reseed-follow-retired-rows root check))
        (error "retired journal changed during descriptor cleanup")))))

(defun physical-reseed-follow-retired-journal-at (root check)
  (call-with-physical-reseed-follow-retired-journal
   root
   (lambda (stream state verify)
     (declare (ignore stream))
     (funcall verify)
     state)
   check))

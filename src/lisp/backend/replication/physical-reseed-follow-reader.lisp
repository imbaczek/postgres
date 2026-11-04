(in-package #:pg)

(defvar *physical-reseed-follow-test-hook* nil)

(defun physical-reseed-follow-test-point (point path)
  (when *physical-reseed-follow-test-hook*
    (funcall *physical-reseed-follow-test-hook* point path)))

(defun physical-reseed-follow-file-identity (stat &optional allow-empty)
  (unless (and (= #o100600 (sb-posix:stat-mode stat))
               (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
               (= 1 (sb-posix:stat-nlink stat))
               (<= (if allow-empty 0 1) (sb-posix:stat-size stat) +physical-reseed-follow-maximum-bytes+))
    (error "target follow journal is not a bounded private single-link regular file"))
  (physical-reseed-file-facts stat))

(defun physical-reseed-follow-entry (root-fd name &optional allow-empty)
  (unless (member name '("physical-reseed.follow" "physical-reseed.follow.tmp") :test #'equal)
    (error "unexpected target follow journal name"))
  (let ((path (merge-pathnames name (physical-image-fd-path root-fd))))
    (when (physical-adoption-path-present-p path)
      (let* ((mount (physical-adoption-install-mount-id nil :directory-fd root-fd))
             (row (physical-reseed-install-old-observe root-fd name mount))
             (facts (physical-reseed-follow-file-identity (sb-posix:lstat (namestring path)) allow-empty)))
        (unless (and (equal facts (physical-reseed-install-row-file-facts row))
                     (equal row (physical-reseed-install-old-observe root-fd name mount)))
          (error "target follow journal metadata changed"))
        (values facts row)))))

(defun physical-reseed-follow-read-at (root-fd name &key stabilize)
  (let ((before (physical-reseed-follow-entry root-fd name))
        (fd nil) (stream nil))
    (unless before (error "target follow journal is absent"))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       root-fd name (logior (if stabilize sb-posix:o-rdwr sb-posix:o-rdonly)
                                            sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) 0)))
           (when (minusp fd) (setf fd nil) (error "cannot open target follow journal"))
           (unless (and (equal before (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                        (= (physical-adoption-install-mount-id nil :directory-fd root-fd)
                           (physical-adoption-install-mount-id nil :directory-fd fd)))
             (error "target follow journal descriptor differs from its name"))
           (sb-sys:without-interrupts
             (setf stream (sb-sys:make-fd-stream fd :input t :output stabilize
                                                    :element-type '(unsigned-byte 8))))
           (when stabilize (fsync-stream stream) (sb-posix:fsync root-fd))
           (physical-reseed-follow-test-point
            :after-follow-journal-open (merge-pathnames name (physical-image-fd-path root-fd)))
           (let ((bytes (make-array (sixth before) :element-type '(unsigned-byte 8))))
             (unless (and (= (length bytes) (read-sequence bytes stream))
                          (eq :eof (read-byte stream nil :eof))
                          (equal before (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                          (equal before (physical-reseed-follow-entry root-fd name)))
               (error "target follow journal changed while reading"))
             (values (physical-reseed-follow-decode bytes) before)))
      (if stream (close stream) (when fd (sb-posix:close fd))))))

(defun physical-reseed-follow-terminal-reference (state)
  (let ((step (getf state :step)))
    (unless (and (= step (physical-reseed-install-last-step (getf state :plan)))
                 (not (getf state :partial))
                 (= (getf state :valid-length) (getf state :file-size)
                    (sixth (getf state :identity))))
      (error "target follow requires a complete terminal install journal"))
    (list :identity (copy-list (getf state :identity)) :digest (getf state :digest)
          :body-crc (getf state :body-crc) :step step)))

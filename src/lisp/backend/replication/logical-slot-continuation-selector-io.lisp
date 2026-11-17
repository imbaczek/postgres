(in-package #:pg)

(defvar *logical-slot-continuation-selector-test-hook* nil)
(defvar *logical-slot-continuation-selector-write-limit* nil)

(defun logical-slot-continuation-selector-point (storage point)
  (logical-slot-continuation-storage-check storage)
  (when *logical-slot-continuation-selector-test-hook*
    (funcall *logical-slot-continuation-selector-test-hook* point storage))
  (logical-slot-continuation-storage-check storage))

;; Validate the names under a held selector directory for both direct and
;; storage-backed observers.  The storage wrapper supplies its directory.
(defun logical-slot-continuation-selector-file-names (directory)
  (let ((names (logical-slot-copy-import-control-names directory 3)))
    (unless (every (lambda (name) (member name '("selector" "selector.tmp") :test #'equal)) names)
      (error "continuation directory has unclassified entries"))
    names))

(defun logical-slot-continuation-selector-names (storage)
  (logical-slot-continuation-selector-file-names
   (logical-slot-continuation-storage-call storage :directory)))

(defun logical-slot-continuation-selector-check-open (storage name fd identity)
  (let* ((directory (logical-slot-continuation-storage-call storage :directory))
         (held (physical-image-file-facts fd))
         (named (physical-image-file-facts directory name)))
    (unless (and (equal identity (logical-slot-copy-install-file-identity held))
                 (equal held named)
                 (logical-slot-copy-import-control-private-facts-p
                  held (physical-image-file-facts directory)))
      (error "continuation selector lost its private named inode"))
    held))

(defun logical-slot-continuation-selector-write-initial (storage image function)
  ;; Raw selector I/O callers must hold the actual origin proof and its
  ;; root-reader lease.  Resume only an exact temporary prefix.  A new file
  ;; is opened exclusively, without following links, and its private named FD
  ;; remains held through installation.
  (declare (notinline sb-posix:write sb-posix:fsync sb-posix:close))
  (let* ((directory (logical-slot-continuation-storage-call storage :directory))
         (limit (or *logical-slot-continuation-selector-write-limit* 65536))
         (before (when (slot-state-lstat (slot-state-fd-path directory "selector.tmp"))
                   (physical-image-file-facts directory "selector.tmp")))
         (fd nil) (identity nil) (complete nil))
    (unless (functionp function) (error "initial selector writer requires an installation callback"))
    (unless (typep limit '(integer 1 65536)) (error "invalid continuation selector write limit"))
    (when (slot-state-lstat (slot-state-fd-path directory "selector"))
      (error "initial continuation selector already exists"))
    (sb-sys:without-interrupts
      (unwind-protect
           (multiple-value-prog1
               (sb-sys:with-local-interrupts
                 (progn
                   (sb-sys:without-interrupts
                     (setf fd (physical-image-openat
                               directory "selector.tmp"
                               (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                       +postmaster-o-cloexec+
                                       (if before 0 (logior sb-posix:o-creat sb-posix:o-excl))) #o600))
                     (when (minusp fd) (setf fd nil) (error "cannot open initial continuation selector temporary"))
                     (let ((facts (physical-image-file-facts fd)))
                       (setf identity (logical-slot-copy-install-file-identity facts))
                       (when (and before (not (equal before facts)))
                         (error "continuation temporary changed before open"))))
                   (logical-slot-continuation-selector-check-open storage "selector.tmp" fd identity)
                   (let* ((prefix (logical-slot-copy-import-control-read-fd fd (length image)))
                          (position (length prefix)))
                     (unless (and (<= position (length image))
                                  (null (mismatch prefix image :end2 position)))
                       (error "unfinished initial selector is not an exact prefix"))
                     (sb-posix:lseek fd position sb-posix:seek-set)
                     (sb-sys:with-pinned-objects (image)
                       (loop while (< position (length image))
                             for count = (min limit (- (length image) position))
                             do (logical-slot-continuation-selector-check-open storage "selector.tmp" fd identity)
                                (let ((written (sb-posix:write
                                                fd (sb-sys:sap+ (sb-sys:vector-sap image) position) count)))
                                  (unless (<= 1 written count) (error "short continuation selector write"))
                                  (incf position written))
                                (logical-slot-continuation-selector-point storage :after-selector-temp-write))))
                   (logical-slot-continuation-selector-check-open storage "selector.tmp" fd identity)
                   (unless (equalp image (logical-slot-copy-import-control-read-fd fd (length image)))
                     (error "continuation selector temporary differs after write"))
                   (sb-posix:fsync fd)
                   (logical-slot-continuation-selector-point storage :after-selector-temp-sync)
                   (logical-slot-continuation-selector-check-open storage "selector.tmp" fd identity)
                   (funcall function fd identity)))
             (setf complete t))
        (when fd
          (let ((owned fd))
            (sb-sys:without-interrupts
              (setf fd nil)
              (handler-case
                  (progn
                    (unless (or (null identity)
                                (equal identity (logical-slot-copy-install-file-identity
                                                 (physical-image-file-facts owned))))
                      (error "continuation selector descriptor ownership was lost"))
                    (sb-posix:close owned))
                (error (condition)
                  (if complete (error condition)
                      (report-query-temp-cleanup-failure "continuation selector temporary" condition)))))))))))

(defun call-with-logical-slot-continuation-selector (storage function)
  ;; The selector is a self-contained canonical snapshot, not a pointer chain.
  ;; The caller must keep the origin proof and root-reader lease through this
  ;; read; decoding model claims alone does not prove the source.
  (let ((directory (logical-slot-continuation-storage-call storage :directory)))
    (call-with-logical-slot-copy-source-proof-fd
     directory "selector" nil
     (lambda (fd facts)
       (let* ((image (logical-slot-copy-import-control-read-fd
                      fd (logical-slot-continuation-maximum-bytes)))
              (snapshot (logical-slot-continuation-decode image)))
         (logical-slot-imported-check-held-file directory "selector" fd facts image)
         (multiple-value-prog1 (funcall function snapshot fd (copy-list facts))
           (logical-slot-continuation-storage-check storage)
           (logical-slot-imported-check-held-file directory "selector" fd facts image)))))))

(defun logical-slot-continuation-install-initial-selector (storage image)
  ;; Publish with NOREPLACE, sync file and directory, and read back the exact
  ;; selected image.  A canonical selector with a temporary is ambiguous and
  ;; is refused without removing either file.
  (declare (notinline sb-posix:fsync))
  (let ((directory (logical-slot-continuation-storage-call storage :directory)))
    (logical-slot-continuation-selector-write-initial
     storage image
     (lambda (fd identity)
       (logical-slot-continuation-selector-point storage :before-selector-install)
       (logical-slot-continuation-selector-check-open storage "selector.tmp" fd identity)
       (unless (and (equalp image (logical-slot-copy-import-control-read-fd fd (length image)))
                    (not (slot-state-lstat (slot-state-fd-path directory "selector"))))
         (error "initial selector changed before installation"))
       (logical-slot-continuation-selector-check-open storage "selector.tmp" fd identity)
       (unless (zerop (physical-adoption-renameat2
                       directory "selector.tmp" directory "selector" +physical-adoption-rename-noreplace+))
         (error "cannot install initial continuation selector exclusively"))
       (logical-slot-continuation-selector-point storage :after-selector-install)
       (logical-slot-continuation-selector-check-open storage "selector" fd identity)
       (call-with-logical-slot-continuation-selector
        storage
        (lambda (snapshot selected selected-facts)
          (declare (ignore snapshot))
          (unless (and (equal identity (logical-slot-copy-install-file-identity selected-facts))
                       (equalp image (logical-slot-copy-import-control-read-fd selected (length image))))
            (error "initial selector installation selected different bytes"))
          (sb-posix:fsync fd)
          (sb-posix:fsync directory)
          (logical-slot-continuation-selector-point storage :after-selector-directory-sync)
          (unless (equal '("selector") (logical-slot-continuation-selector-names storage))
            (error "initial selector publication left ambiguous entries"))))
       t))))

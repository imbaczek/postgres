(in-package #:pg)

(defun logical-slot-copy-source-proof-directory-identity (facts)
  "Select device, inode, full mode, owner and mount identity for a directory."
  (list (first facts) (second facts) (third facts) (fifth facts) (ninth facts)))

(defun call-with-logical-slot-copy-source-proof-fd (parent name directory-p function &key mutable-directory)
  ;; Hold one private, named input for FUNCTION and recheck its identity.
  ;; Full facts are required by default. Mutable directories may change size,
  ;; link count and timestamps, but retain their private named identity.
  ;; Leave a reused descriptor open.  Close owned input once, without retrying
  ;; a reported close failure; preserve an original body failure during cleanup.
  (declare (notinline sb-posix:close))
  (unless (and (stringp name) (plusp (length name))
               (not (find #\/ name)) (not (find (code-char 0) name))
               (not (member name '("." "..") :test #'equal)))
    (error "source proof input requires one fixed file name"))
  (when (and mutable-directory (not directory-p))
    (error "mutable source proof input must be a directory"))
  (let ((fd nil) (identity nil) (complete nil) (before (physical-image-file-facts parent name))
                 (parent-facts (physical-image-file-facts parent)))
    (flet ((matches (left right)
             (if mutable-directory
                 (equal (logical-slot-copy-source-proof-directory-identity left)
                        (logical-slot-copy-source-proof-directory-identity right))
                 (equal left right))))
      (unwind-protect
           (progn
             (sb-sys:without-interrupts
               (setf fd (physical-image-openat
                         parent name (logior sb-posix:o-rdonly sb-posix:o-nofollow
                                             sb-posix:o-nonblock +postmaster-o-cloexec+
                                             (if directory-p sb-posix:o-directory 0)) 0))
               (when (minusp fd) (setf fd nil) (error "cannot open source proof input")))
             (let ((held (physical-image-file-facts fd)))
               (setf identity held)
               (unless (and (matches before held)
                            (matches held (physical-image-file-facts parent name))
                            (= (first held) (first parent-facts))
                            (= (ninth held) (ninth parent-facts))
                            (= (fifth held) (sb-posix:geteuid))
                            (if directory-p
                                (and (sb-posix:s-isdir (third held))
                                     (= #o700 (logand #o7777 (third held))))
                                (logical-slot-copy-import-control-private-facts-p held parent-facts)))
                 (error "source proof input is not private, bound and on the current mount"))
               (multiple-value-prog1 (funcall function fd held)
                 (unless (and (matches held (physical-image-file-facts fd))
                              (matches held (physical-image-file-facts parent name)))
                   (error "source proof input changed during its scope"))
                 (setf complete t))))
        (when fd
          (let ((owned fd))
            (sb-sys:without-interrupts
              (setf fd nil)
              (handler-case
                  (progn
                    (when identity
                      (let ((current (handler-case (physical-image-file-facts owned) (error () nil))))
                        (unless (and current (= (first identity) (first current))
                                     (= (second identity) (second current))
                                     (= (ninth identity) (ninth current)))
                          (error "source proof descriptor ownership was lost before close"))))
                    (sb-posix:close owned))
                (error (condition)
                  (if complete (error condition)
                      (report-query-temp-cleanup-failure "source proof input" condition)))))))))))

(defun call-with-logical-slot-imported-archive-directory (parent function)
  "Hold the retained directory while its contents may change.
Before reading selected objects, protect them with a retained-root lease and
check their contents. This scope alone grants no logical authority."
  (call-with-logical-slot-copy-source-proof-fd
   parent "pg_physical_retained" t function :mutable-directory t))

(in-package #:pg)

(defun logical-slot-continuation-cold-final-close (records complete)
  (let ((failure nil))
    (dolist (record records)
      (let ((fd (shiftf (third record) nil)))
        (when fd
          (handler-case
              (logical-slot-continuation-retirement-close fd (eighth record) t)
            (error (condition) (unless failure (setf failure condition)))))))
    (when failure
      (if complete (error failure)
          (report-query-temp-cleanup-failure "cold publication final files" failure)))))

(defun logical-slot-continuation-cold-final-check-record (record)
  (destructuring-bind (parent name fd facts image directory-p parent-facts opened-facts) record
    (declare (ignore opened-facts))
    (unless (and fd (equal facts (physical-image-file-facts fd))
                 (equal facts (physical-image-file-facts parent name))
                 (equal parent-facts (physical-image-file-facts parent)))
      (error "cold publication final input lost its exact named inode"))
    (if directory-p
        (unless (equal '("state") (logical-slot-copy-import-control-names fd 3))
          (error "cold publication final slot contains an unclassified entry"))
        (logical-slot-imported-check-held-file parent name fd facts image))
    t))

(defun call-with-logical-slot-continuation-cold-publication-files (storage completion function)
  (unless (functionp function) (error "cold publication final files require a callback"))
  (let* ((data (logical-slot-continuation-cold-completion-call completion :data))
         (observation (getf data :observation))
         (selected (getf observation :selector))
         (ledger (getf observation :ledger))
         (directory (logical-slot-continuation-storage-call storage :directory))
         (records nil) (active t) (thread sb-thread:*current-thread*) (complete nil))
    (logical-slot-continuation-storage-call
     storage :with-cold-slot-root
     (lambda (slot-root check-root)
       (labels ((check ()
                  (unless (and active (eq thread sb-thread:*current-thread*))
                    (error "cold publication final files ended or changed thread"))
                  (assert (sb-thread:holding-mutex-p *wal-lock*))
                  (logical-slot-continuation-storage-call storage :check-under-lock)
                  (funcall check-root)
                  (unless (and
                           (equal (getf observation :directory-facts)
                                  (physical-image-file-facts directory))
                           (equal '("selector") (logical-slot-copy-import-control-names directory 3))
                           (equal (getf observation :slot-root-facts)
                                  (physical-image-file-facts slot-root))
                           (equal (mapcar #'first ledger)
                                  (logical-slot-continuation-slot-inventory-names slot-root)))
                    (error "cold publication final selector or slot inventory changed"))
                  (dolist (record records)
                    (logical-slot-continuation-cold-final-check-record record))
                  (logical-slot-continuation-storage-call storage :check-under-lock)
                  t)
                (open-one (parent name directory-p facts image)
                  (let* ((parent-facts (physical-image-file-facts parent))
                         (record (list parent name nil (copy-list facts) image directory-p
                                       parent-facts nil)))
                    (sb-sys:without-interrupts
                      (push record records)
                      (setf (third record)
                            (physical-image-openat
                             parent name
                             (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                     +postmaster-o-cloexec+
                                     (if directory-p sb-posix:o-directory 0)) 0))
                      (when (minusp (third record))
                        (setf (third record) nil)
                        (error "cannot open cold publication final input"))
                      (let ((held (physical-image-file-facts (third record))))
                        (setf (eighth record) held)
                        (unless (and (equal facts held)
                                     (equal held (physical-image-file-facts parent name))
                                     (= (first held) (first parent-facts))
                                     (= (ninth held) (ninth parent-facts))
                                     (= (fifth held) (sb-posix:geteuid))
                                     (if directory-p
                                         (and (sb-posix:s-isdir (third held))
                                              (= #o700 (logand #o7777 (third held))))
                                         (logical-slot-copy-import-control-private-facts-p
                                          held parent-facts)))
                          (error "cold publication final input is not private, bound and on this mount"))))
                    (logical-slot-continuation-cold-final-check-record record)
                    (third record))))
         (sb-sys:without-interrupts
           (unwind-protect
                (sb-sys:with-local-interrupts
                  (sb-thread:with-mutex (*wal-lock*) (check))
                  (open-one directory "selector" nil (second selected) (third selected))
                  (dolist (row ledger)
                    (let ((child (open-one slot-root (first row) t (second row) nil)))
                      (open-one child "state" nil (third row) (fourth row))))
                  (sb-thread:with-mutex (*wal-lock*) (check))
                  (multiple-value-prog1 (funcall function #'check)
                    (sb-thread:with-mutex (*wal-lock*) (check))
                    (setf complete t)))
             (setf active nil)
             (logical-slot-continuation-cold-final-close records complete))))))))

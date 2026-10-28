;;;; Owned subprocess for receive rewind crash cuts.
(require :asdf)
(push (uiop:pathname-parent-directory-pathname
       (uiop:pathname-directory-pathname *load-truename*))
      asdf:*central-registry*)
(asdf:load-system "pg")

(defun physical-store-rewind-crash-main ()
  (let* ((args (uiop:command-line-arguments))
         (root (pathname (first args)))
         (rooted (equal (third args) "rooted"))
         (verify (equal (second args) "VERIFY"))
         (fork (if rooted 128 32))
         (point (find (second args)
                      '(:after-history-publication :before-control-rename :after-control-rename
                        :after-control-directory-fsync :after-follow-control-readback
                        :before-follow-tail-cleanup :after-follow-tail-cleanup)
                      :key #'symbol-name :test #'string-equal)))
    (unless (or point (and rooted verify)) (error "unknown receive rewind crash point"))
    (let ((store (pg::physical-store-open
                  root 123 1 :discard-tail (not verify)
                  :retained-identity (when rooted
                                       '(:system-id 123 :base-timeline 1 :base 32 :manifest-digest 77)))))
      (unwind-protect
           (if verify
               (let ((id (parse-integer (fourth args))))
                 (assert (= id (pg::physical-store-retained-root store)))
                 (assert (= 96 (pg::physical-store-floor-lsn store)))
                 (assert (= 128 (pg::physical-store-flushed-lsn store)))
                 (assert (= 2 (pg::physical-store-selected-timeline store)))
                 (with-open-file (in (pg::physical-store-segment-path store 0 1)
                                     :element-type '(unsigned-byte 8))
                   (assert (= 160 (file-length in))))
                 (assert (not (pg::physical-retained-root-delete-unprotected
                               (pg::physical-store-retained-directory store) :inventory id)))
                 (pg::physical-store-reset-unflushed store)
                 (with-open-file (in (pg::physical-store-segment-path store 0 1)
                                     :element-type '(unsigned-byte 8))
                   (assert (= 128 (file-length in))))
                 (format t "rooted rewind cold verification PASS~%"))
               (let ((pg::*physical-store-test-hook*
                       (lambda (where owner)
                         (declare (ignore owner))
                         (when (eq where point)
                           (with-open-file (out (merge-pathnames "rewind-crash.marker" root)
                                                :direction :output :if-exists :supersede)
                             (format out "~d ~a~%" (sb-posix:getpid) point)
                             (pg::fsync-stream out))
                           (pg::fsync-directory root)
                           (sb-posix:kill (sb-posix:getpid) 9)))))
                 (pg::physical-store-rewind-select-history
                  store (pg::physical-store-control-tuple store)
                  (if rooted '(96 1) '(32 1)) (if rooted '(64 1) '(32 1))
                  2 (format nil "1~c~X/~8,'0X~creceive rewind crash~%" #\Tab
                            (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab)
                  (pg::wal-encode-end-of-recovery-record fork 42 2 1 1))
                 (error "receive rewind crash boundary was not reached")))
        (pg::physical-store-close store)))))

(physical-store-rewind-crash-main)

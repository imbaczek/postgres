;;;; Task-owned subprocess for receive-store SIGKILL durability boundaries.
(require :asdf)
(push (uiop:pathname-parent-directory-pathname
       (uiop:pathname-directory-pathname *load-truename*))
      asdf:*central-registry*)
(asdf:load-system "pg")

(let* ((args (uiop:command-line-arguments))
       (root (pathname (first args)))
       (point (find (second args)
                    '(:after-segment-fsync :before-control-fsync :after-control-fsync
                      :before-control-rename :after-control-rename
                      :before-control-directory-fsync :after-control-directory-fsync
                      :after-control-write)
                    :key #'symbol-name :test #'string-equal))
       (store (pg::physical-store-open root 123 1)))
  (unless point (error "unknown physical receive crash point"))
  (pg::physical-store-input
   store 0 (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0
                                  (make-array 0 :element-type '(unsigned-byte 8))))
  (let ((pg::*physical-store-test-hook*
          (lambda (phase receiver)
            (declare (ignore receiver))
            (when (eq phase point)
              (with-open-file (stream (merge-pathnames "crash.marker" root)
                                      :direction :output :if-exists :supersede)
                (format stream "~a~%" point)
                (pg::fsync-stream stream))
              (pg::fsync-directory root)
              (sb-posix:kill (sb-posix:getpid) 9)))))
    (pg::physical-store-flush store))
  (error "physical receive crash failpoint was not reached"))

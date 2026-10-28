;;;; Owned subprocess for follow-pin deletion and cold retry.
(require :asdf)
(push (uiop:pathname-parent-directory-pathname
       (uiop:pathname-directory-pathname *load-truename*))
      asdf:*central-registry*)
(asdf:load-system "pg")

(defun follow-release-crash-main ()
  (let* ((args (uiop:command-line-arguments))
         (pg::*pgdata* (pathname (first args)))
         (point (second args))
         (digest (parse-integer (third args)))
         (path (pg::physical-source-pin-path "release_follow"))
         (pin (when (probe-file path)
                (pg::physical-follow-pin-decode
                 (pg::read-bounded-regular-file path (pg::physical-follow-pin-image-limit))))))
    (when pin
      (setf (gethash "release_follow" pg::*physical-source-pins*) pin
            (gethash pin pg::*wal-snapshot-retention-pins*) (pg::physical-source-pin-restart-lsn pin)))
    (if (string-equal point "RETRY")
        (progn
          (assert (member (pg::physical-source-follow-release "release_follow" digest) '(:released :absent)))
          (assert (not (probe-file path)))
          (assert (not (gethash "release_follow" pg::*physical-source-pins*)))
          (assert (not (and pin (gethash pin pg::*wal-snapshot-retention-pins*))))
          (assert (eq :absent (pg::physical-source-follow-release "release_follow" digest)))
          (format t "follow release cold retry PASS~%"))
        (let ((pg::*physical-source-test-hook*
                (lambda (where selected)
                  (declare (ignore selected))
                  (when (string-equal point (symbol-name where))
                    (with-open-file (out (merge-pathnames "release-crash.marker" pg::*pgdata*)
                                         :direction :output :if-exists :supersede)
                      (format out "~d ~a~%" (sb-posix:getpid) where)
                      (pg::fsync-stream out))
                    (pg::fsync-directory pg::*pgdata*)
                    (sb-posix:kill (sb-posix:getpid) 9)))))
          (pg::physical-source-follow-release "release_follow" digest)
          (error "follow release crash point was not reached")))))

(follow-release-crash-main)

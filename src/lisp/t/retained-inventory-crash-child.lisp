(require :asdf)
(push (uiop:pathname-parent-directory-pathname
       (uiop:pathname-directory-pathname *load-truename*))
      asdf:*central-registry*)
(asdf:load-system "pg")

(defun retained-inventory-crash-main ()
  (let* ((args (uiop:command-line-arguments))
         (root (pathname (first args)))
         (token (string-upcase (second args)))
         (point
           (or (find token '(:after-extent-publication :after-inventory-publication)
                     :key #'symbol-name :test #'string=)
               (loop for kind in '(:extent :inventory)
                       thereis (loop for phase in '(:before-file-fsync :after-file-fsync :before-rename
                                                    :after-rename :before-directory-fsync :after-directory-fsync)
                                     when (string= token (format nil "~a-~a" kind phase))
                                       return (list kind phase)))))
         (images (loop for name in '("input-0.image" "input-1.image")
                       collect (pg::read-bounded-regular-file
                                (merge-pathnames name root) pg::+physical-retained-image-limit+)))
         (metadata (pg::physical-retained-extent-decode (first images)))
         (identity (loop for key in '(:system-id :base-timeline :base :manifest-digest)
                         append (list key (getf metadata key)))))
    (unless point (error "unknown retained inventory crash point"))
    (let ((pg::*physical-retained-inventory-test-hook*
            (lambda (where id)
              (when (equal where point)
                (with-open-file (stream (merge-pathnames "inventory-crash.marker" root)
                                        :direction :output :if-exists :supersede)
                  (format stream "~d ~d ~a~%" (sb-posix:getpid) id token)
                  (pg::fsync-stream stream))
                (pg::fsync-directory root)
                (sb-posix:kill (sb-posix:getpid) 9)))))
      (pg::physical-retained-inventory-publish
       (merge-pathnames "archive/" root) identity
       (getf metadata :history-target) (getf metadata :history-text) images))
    (error "retained inventory crash point was not reached")))

(retained-inventory-crash-main)

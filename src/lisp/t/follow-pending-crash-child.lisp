;;;; Owned subprocess for interrupted local follow reconciliation.
(require :asdf)
(push (uiop:pathname-parent-directory-pathname
       (uiop:pathname-directory-pathname *load-truename*))
      asdf:*central-registry*)
(asdf:load-system "pg")

(defun follow-pending-crash-main ()
  (let* ((args (uiop:command-line-arguments))
         (pg::*pgdata* (pathname (first args)))
         (directory (pathname (second args)))
         (point (third args))
         (base (pg::recovery-parse-manifest-text
                (pg::bytes->string (pg::recovery-read-bytes
                                    (merge-pathnames "pending-base.fixture" pg::*pgdata*)))))
         (store (pg::physical-store-open
                 directory (getf base :system-id) (getf base :timeline)
                 :discard-tail nil :create-lock nil
                 :retained-identity (pg::physical-recovery-retained-identity base)
                 :retained-directory (when (equal (fourth args) "rooted")
                                       (pg::physical-recovery-retained-directory)))))
    (labels ((crash (where)
               (when (string-equal point (symbol-name where))
                 (with-open-file (out (merge-pathnames "pending-crash.marker" pg::*pgdata*)
                                      :direction :output :if-exists :supersede)
                   (format out "~d ~a~%" (sb-posix:getpid) where)
                   (pg::fsync-stream out))
                 (pg::fsync-directory pg::*pgdata*)
                 (sb-posix:kill (sb-posix:getpid) 9))))
      (let ((pg::*physical-follow-test-hook* #'crash)
            (pg::*physical-store-test-hook*
              (lambda (where owner) (declare (ignore owner)) (crash where)))
            (pg::*physical-recovery-control-test-hook*
              (lambda (where state) (declare (ignore state)) (crash where))))
        (unwind-protect
             (if (string-equal point "VERIFY")
                 (let* ((config (pg::physical-follow-read-config))
                        (binding (fifth config)) (root-id (pg::physical-store-retained-root store)))
                   (assert (eql root-id (nth 10 (getf binding :before))))
                   (assert (not (pg::physical-retained-root-delete-unprotected
                                 (pg::physical-store-retained-directory store) :inventory root-id)))
                   (let ((active (pg::physical-follow-reconcile-pending-store store base config))
                         (control (pg::physical-recovery-read-control pg::*pgdata*)))
                     (assert (eq :active (getf (fifth active) :phase)))
                     (assert (eql root-id (pg::physical-store-retained-root store)))
                     (assert (= (getf control :applied) (first (getf binding :required))))
                     (assert (= (getf control :next-xid)
                                (getf (getf binding :recovery) :observed-next-xid))))
                   (format t "pending rooted cold verification PASS~%"))
                 (progn
                   (pg::physical-follow-reconcile-pending-store store base (pg::physical-follow-read-config))
                   (error "pending follow crash point was not reached")))
          (pg::physical-store-close store))))))

(follow-pending-crash-main)

(require :asdf)
(asdf:load-asd (merge-pathnames "../pg.asd" *load-truename*))
(asdf:load-system "pg")

(in-package #:pg)

(defun adoption-stage-native-prove (stage state proof port expected-root)
  (let* ((profile (make-physical-libpq-profile
                   (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres connect_timeout=3" port)
                   :mode :physical))
         (client (physical-libpq-connect (physical-libpq-helper-path) profile))
         (name nil)
         (coordinates nil)
         (anchor nil))
    (unwind-protect
         (let* ((record (physical-adoption-attempt-create state stage client profile))
                (identity (getf proof :identity))
                (request (list :system-id (first identity) :base-timeline (second identity)
                               :base (third identity) :manifest-digest (fourth identity)
                               :end (getf proof :end))))
           (assert (physical-adoption-attempt-validate-context (getf state :owner) record))
           (assert (not (probe-file (physical-adoption-attempt-path
                                     (physical-adoption-owner-directory (getf state :owner))))))
           (setf name (getf (getf record :stage) :source-pin)
                 coordinates (getf record :request)
                 anchor (pg-client-native-prepare client name coordinates))
           (assert (equal (getf record :source-history) (getf anchor :history-text)))
           (let ((remote (pg-client-native-prove-retained client name anchor request)))
             (assert (equal (getf record :source-history) (getf remote :history-text)))
             (assert (= (getf proof :end-digest) (getf remote :end-digest)))
             (assert (= expected-root (getf remote :root-id)))
             (format t "~&adoption-context: fresh stage, frozen accepted route and retained proof agree PASS~%")))
      (unwind-protect
           (when coordinates
             (if anchor
                 (pg-client-native-release client name (physical-follow-anchor-digest anchor))
                 (pg-client-native-abort client name coordinates)))
        (physical-client-close client)))))

(destructuring-bind (directory target port system timeline base manifest endpoint digest root)
    (uiop:command-line-arguments)
  (let ((identity (mapcar #'parse-integer (list system timeline base manifest))))
    (call-with-physical-adoption-owner
     directory
     (lambda (owner)
       (let* ((state (physical-adoption-inspect-primary owner))
              (proof (physical-adoption-prove-primary-origin state identity))
              (stage (physical-adoption-stage-base
                      state proof target (format nil "adopted_base_~d" (sb-posix:getpid)))))
         (assert (= (parse-integer endpoint) (getf proof :end)))
         (assert (= (parse-integer digest) (getf proof :end-digest)))
         (assert (= (getf state :next-xid) (getf (getf stage :base-state) :next-xid)))
         (adoption-stage-native-prove stage state proof (parse-integer port) (parse-integer root)))))))

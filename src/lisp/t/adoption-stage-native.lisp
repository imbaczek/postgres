(require :asdf)
(asdf:load-asd (merge-pathnames "../pg.asd" *load-truename*))
(asdf:load-system "pg")

(in-package #:pg)

(defun adoption-stage-native-prove (stage state proof port expected-root)
  (let* ((manifest (getf stage :base-state))
         (end (getf manifest :end))
         (timeline (getf manifest :timeline))
         (segment (floor (1- end) +wal-segment-size+))
         (start (* segment +wal-segment-size+))
         (overlap
           (let ((*pgdata* (getf stage :directory)))
             (read-bounded-regular-file
              (wal-segment-path segment timeline (getf manifest :wal-namespace))
              +wal-segment-size+)))
         (client (physical-libpq-connect
                  (physical-libpq-helper-path)
                  (make-physical-libpq-profile
                   (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres connect_timeout=3" port)
                   :mode :physical)))
         (name (format nil "verified_stage_~d" (sb-posix:getpid)))
         (coordinates nil)
         (anchor nil))
    (unwind-protect
         (let* ((source (physical-client-identity-row client))
                (source-timeline (pg-client-native-u64 (aref source 1) t))
                (identity (getf proof :identity))
                (request (list :system-id (first identity) :base-timeline (second identity)
                               :base (third identity) :manifest-digest (fourth identity) :end end)))
           (assert (= (getf state :system-id) (pg-client-native-u64 (aref source 0))))
           (assert (= end (+ start (length overlap))))
           (setf coordinates
                 (list :version 1 :system-id (getf state :system-id)
                       :base-timeline timeline :base end :manifest-digest (getf stage :manifest-digest)
                       :source-timeline source-timeline :receive-timeline timeline
                       :receive-lsn end :receive-digest #xCBF29CE484222325 :overlap-start start
                       :overlap-digest (physical-store-update-digest #xCBF29CE484222325 overlap))
                 anchor (pg-client-native-prepare client name coordinates))
           (let ((remote (pg-client-native-prove-retained client name anchor request)))
             (assert (= (getf proof :end-digest) (getf remote :end-digest)))
             (assert (= expected-root (getf remote :root-id)))
             (format t "~&adoption-stage: verified canonical E/Mnew and independent retained proof PASS~%")))
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
              (stage (physical-adoption-stage-base state proof target "adopted_base")))
         (assert (= (parse-integer endpoint) (getf proof :end)))
         (assert (= (parse-integer digest) (getf proof :end-digest)))
         (assert (= (getf state :next-xid) (getf (getf stage :base-state) :next-xid)))
         (adoption-stage-native-prove stage state proof (parse-integer port) (parse-integer root)))))))

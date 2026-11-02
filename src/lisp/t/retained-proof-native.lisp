(require :asdf)
(asdf:load-asd (merge-pathnames "../pg.asd" *load-truename*))
(asdf:load-system "pg")

(in-package #:pg)

(defun retained-proof-native-client (mode port)
  (ecase mode
    (:native (pg-client-connect "127.0.0.1" port "postgres" :replication-p t))
    (:libpq (physical-libpq-connect
             (physical-libpq-helper-path)
             (make-physical-libpq-profile
              (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres connect_timeout=3" port)
              :mode :physical)))))

(defun retained-proof-native-check (mode port state identity expected-digest expected-root
                                    &optional prepare-name query-path)
  (destructuring-bind (system timeline base manifest) identity
    (let* ((*pgdata* (physical-adoption-owner-directory (getf state :owner)))
           (end (getf state :end))
           (segment (floor (1- end) +wal-segment-size+))
           (start (* segment +wal-segment-size+))
           (bytes (read-bounded-regular-file
                   (wal-segment-path segment timeline (getf state :namespace)) +wal-segment-size+))
           (overlap (subseq bytes 0 (- end start)))
           (client (retained-proof-native-client mode port))
           (name (or prepare-name (if (eq mode :native) "origin_proof_native" "origin_proof_libpq")))
           (coordinates nil)
           (anchor nil)
           (commands-published-p nil))
      (unwind-protect
           (let* ((source (physical-client-identity-row client))
                  (source-timeline (pg-client-native-u64 (aref source 1) t))
                  (request (list :system-id system :base-timeline timeline :base base
                                 :manifest-digest manifest :end end)))
             (assert (= system (pg-client-native-u64 (aref source 0))))
             (assert (> source-timeline timeline))
             ;; This labels synthetic receiver coordinates, not an installed base.
             (setf coordinates
                   (list :version 1 :system-id system :base-timeline timeline :base end
                         :manifest-digest (logxor manifest 1) :source-timeline source-timeline
                         :receive-timeline timeline :receive-lsn end
                         :receive-digest #xCBF29CE484222325 :overlap-start start
                         :overlap-digest (physical-store-update-digest #xCBF29CE484222325 overlap))
                   anchor (pg-client-native-prepare client name coordinates))
             (if prepare-name
                 (progn
                   (with-open-file (output query-path :direction :output :if-exists :supersede)
                     (format output "~a~%RELEASE_FOLLOW SLOT ~a (native_format '~a', anchor_digest '~d')~%"
                             (physical-origin-proof-query name (physical-follow-anchor-digest anchor) request)
                             name +physical-native-format+ (physical-follow-anchor-digest anchor)))
                   (setf commands-published-p t))
                 (let ((proof (pg-client-native-prove-retained client name anchor request)))
                   (assert (= end (getf proof :end)))
                   (assert (= expected-digest (getf proof :end-digest)))
                   (assert (= expected-root (getf proof :root-id)))
                   (format t "~&retained-proof: ~a transport independently proves original history at ~d PASS~%"
                           mode end))))
        (unwind-protect
             (when (and coordinates (not commands-published-p))
               (if anchor
                   (pg-client-native-release client name (physical-follow-anchor-digest anchor))
                   (pg-client-native-abort client name coordinates)))
          (physical-client-close client))))))

(destructuring-bind (directory port system timeline base manifest endpoint digest root &optional prepare-name query-path)
    (uiop:command-line-arguments)
  (let* ((identity (mapcar #'parse-integer (list system timeline base manifest)))
         (expected-end (parse-integer endpoint))
         (expected-digest (parse-integer digest)))
    (call-with-physical-adoption-owner
     directory
     (lambda (owner)
       (let* ((state (physical-adoption-inspect-primary owner))
              (proof (physical-adoption-prove-primary-origin state identity)))
         (assert (= expected-end (getf state :end)))
         (assert (= expected-digest (getf proof :end-digest)))
         (dolist (mode (if prepare-name '(:libpq) '(:native :libpq)))
           (retained-proof-native-check mode (parse-integer port) state identity expected-digest
                                        (parse-integer root) prepare-name query-path)))))))

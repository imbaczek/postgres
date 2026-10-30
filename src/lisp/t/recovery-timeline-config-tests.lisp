(in-package #:pg-physical-tests)

(deftest recovery-timeline-setting-matches-c-unsigned-base-zero-parsing
    (dolist (entry '(("current" :current) ("latest" :latest)
                     ("1" 1) ("020" 16) ("0x10" 16) ("0XfF" 255)
                     ("+0x10" 16)
                     ("+2" 2) (" 2" 2) ("4294967295" 4294967295)
                     ("-18446744073709551615" 1)
                     ("-18446744069414584321" 4294967295)))
      (destructuring-bind (text expected) entry
        (ok (eql expected (pg::startup-parse-recovery-timeline text)))
        (ok (string= text (pg::startup-normalize-setting "recovery_target_timeline" text)))))
  (dolist (text '("" "CURRENT" "Latest" " current" "latest " "0" "-1" "08"
                  "0x" "2 " "2x" "1.0" "1e2" "2MB" "4294967296"
                  "18446744073709551616" "-18446744073709551616"))
    (ok (signals (pg::startup-normalize-setting "recovery_target_timeline" text) 'error))))

(deftest recovery-timeline-binary-prefix-follows-c-library-support
    (dolist (entry '(("0b10" 2) ("0B11" 3)))
      (destructuring-bind (text expected) entry
        (if (sb-sys:find-foreign-symbol-address "__isoc23_strtoull")
            (progn
              (ok (= expected (pg::startup-parse-recovery-timeline text)))
              (ok (string= text (pg::startup-normalize-setting "recovery_target_timeline" text))))
            (ok (signals (pg::startup-parse-recovery-timeline text) 'error))))))

(cl-spec:defproperty recovery-timeline-numeric-spelling-preserves-its-goal
    ((timeline (range integer 1 4294967295))
     (style (range integer 0 4)))
  (:about pg::startup-normalize-setting)
  (:trials (:normal 200))
  (let ((text (ecase style
                (0 (format nil "~d" timeline))
                (1 (format nil "0~o" timeline))
                (2 (format nil "0x~x" timeline))
                (3 (format nil "+~d" timeline))
                (4 (format nil "~d" (- timeline (ash 1 64)))))))
    (and (= timeline (pg::startup-parse-recovery-timeline text))
         (string= text (pg::startup-normalize-setting "recovery_target_timeline" text)))))

(deftest generated-recovery-timeline-numeric-spelling
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'recovery-timeline-numeric-spelling-preserves-its-goal :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest bootstrap-uses-active-timeline-setting-before-cached-child-admission
    (dolist (word '("current" "latest" "0x1"))
      (call-with-cached-child-follow-config
       (lambda (root store active fork)
         (declare (ignore active))
         (let ((control (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
           (pg::physical-store-close store)
           (sb-posix:rename (namestring (pg::physical-store-directory store))
                            (namestring (pg::physical-recovery-spool-directory)))
           (call-with-current-promotion-owner
            (lambda ()
              (let ((pg::*wal-ready* nil) (pg::*physical-receive-store* nil)
                                          (pg::*physical-recovery-state* nil) (pg::*physical-follow-runtime* nil)
                                          (pg::*physical-restartpoint-worker* nil)
                                          (pg::*wal-checkpoint-snapshot* pg::*wal-checkpoint-snapshot*)
                                          (pg::*wal-poisoned* pg::*wal-poisoned*)
                                          (pg::*active-xids* pg::*active-xids*)
                                          (pg::*startup-server-settings* (pg::startup-default-settings (namestring root))))
                (setf (gethash "recovery_target_timeline" pg::*startup-server-settings*)
                      (list :value word :source :file :pending-restart t
                            :desired-value (if (string= word "current") "latest" "current")))
                (unwind-protect
                     (if (string= word "latest")
                         (progn
                           (ok (signals (pg::physical-bootstrap-check-startup) 'error))
                           (ok (equalp control (pg::recovery-read-bytes
                                                (pg::physical-recovery-control-path root)))))
                         (progn
                           (pg::physical-bootstrap-check-startup)
                           (ok (= 1 (getf (getf pg::*physical-recovery-state* :timeline-policy) :target)))
                           (ok (eq (if (string= word "current") :current :numeric)
                                   (getf (getf pg::*physical-recovery-state* :timeline-policy) :goal)))
                           (pg::physical-recovery-open-storage)
                           (ok (= fork pg::*last-replay-lsn*))
                           (ok (= 1 (pg::physical-recovery-replay-timeline)))
                           (ok (not pg::*physical-recovery-failed-p*))))
                  (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil))
                  (when pg::*physical-receive-store*
                    (pg::physical-store-close pg::*physical-receive-store*)))))))))))

(defun call-with-recovery-timeline-settings (root function)
  (let ((pg::*pgdata* root)
        (pg::*startup-server-settings* nil)
        (pg::*startup-native-inputs* nil) (pg::*startup-command-overrides* nil)
        (pg::*enable-hot-standby* t)
        (pg::*wal-keep-size-mb* 0)
        (pg::*physical-restartpoint-timeout* 300)
        (pg::*physical-restartpoint-wal-bytes* 1073741824)
        (pg::*max-slot-wal-keep-bytes* 1073741824)
        (pg::*wal-slot-retention-policy* nil)
        (pg::*max-replication-slots* 10)
        (pg::*max-connections* 100) (pg::*max-wal-senders* 10))
    (funcall function)))

(deftest timeline-setting-reload-preserves-active-spelling-and-policy
    (dolist (values '(("current" "latest" :current) ("020" "0x10" 16)))
      (destructuring-bind (active desired goal) values
        (with-physical-store-directory (root)
          (call-with-recovery-timeline-settings
           root
           (lambda ()
             (let* ((path (merge-pathnames "postgresql.conf" root))
                    (policy (list :target 1))
                    (pg::*physical-recovery-state* (list :timeline-policy policy)))
               (write-startup-config path (format nil "recovery_target_timeline='~a'~%" active))
               (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
                 (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
               (ok (eql goal (pg::startup-recovery-timeline-goal)))
               (ok (string= active (pg::startup-setting-show-value "recovery_target_timeline")))
               (write-startup-config path (format nil "recovery_target_timeline='~a'~%" desired))
               (pg::startup-reload-settings)
               (let* ((entry (gethash "recovery_target_timeline" pg::*startup-server-settings*))
                      (row (pg::startup-settings-row "recovery_target_timeline" entry)))
                 (ok (equal desired (getf entry :desired-value)))
                 (ok (string= active (aref row 1)))
                 (ok (string= "postmaster" (aref row 3)))
                 (ok (string= "string" (aref row 4)))
                 (ok (aref row 9))
                 (ok (eql goal (pg::startup-recovery-timeline-goal)))
                 (ok (eq policy (getf pg::*physical-recovery-state* :timeline-policy)))
                 (write-startup-config path "recovery_target_timeline=0")
                 (ok (signals (pg::startup-reload-settings) 'error))
                 (ok (eq entry (gethash "recovery_target_timeline" pg::*startup-server-settings*))))
               (write-startup-config path (format nil "recovery_target_timeline='~a'~%" active))
               (pg::startup-reload-settings)
               (ok (not (getf (gethash "recovery_target_timeline" pg::*startup-server-settings*)
                              :pending-restart)))
               (ok (signals (pg::startup-check-session-setting "recovery_target_timeline") 'error)))))))))

(deftest live-reload-installs-the-default-for-a-newly-added-startup-setting
    (with-physical-store-directory (root)
      (call-with-recovery-timeline-settings
       root
       (lambda ()
         (multiple-value-bind (native overrides) (pg::startup-cli-inputs (list "-D" (namestring root)))
           (pg::startup-install-settings (pg::startup-load-settings native overrides) native overrides))
         (remhash "recovery_target_timeline" pg::*startup-server-settings*)
         (pg::startup-reload-settings)
         (let* ((entry (gethash "recovery_target_timeline" pg::*startup-server-settings*))
                (row (pg::startup-settings-row "recovery_target_timeline" entry)))
           (ok (string= "latest" (aref row 1)))
           (ok (string= "default" (aref row 5)))
           (ok (not (aref row 9)))
           (ok (eq :latest (pg::startup-recovery-timeline-goal))))))))

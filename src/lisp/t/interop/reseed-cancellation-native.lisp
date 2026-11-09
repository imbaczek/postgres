(in-package #:pg)

(defun native-cancellation-read-journal (owner)
  (call-with-physical-directory-root
   owner
   (lambda (fd directory)
     (declare (ignore directory))
     (physical-reseed-follow-read-at fd "physical-reseed.follow"))))

(defun native-cancellation-env (name &optional default)
  (or (sb-posix:getenv (concatenate 'string "NATIVE_CANCELLATION_" name))
      default (error "missing native cancellation test setting ~a" name)))

(defun call-with-native-cancellation-owner (target action function)
  (if (and (equal "yes" (native-cancellation-env "CONFIGURED" "no"))
           (not (member action '("prove" "configure" "settle") :test #'equal)))
      (call-with-physical-reseed-follow-cancellation-owner target function)
      (call-with-physical-reseed-follow-configuration-owner target function)))

(defvar *native-cancellation-source-events* nil)

(defvar *native-cancellation-source-originals* nil)

(defvar *native-cancellation-source-events-lock* (sb-thread:make-mutex :name "native cancellation source events"))

(defun native-cancellation-reset-source-events ()
  (unless *native-cancellation-source-originals*
    (setf *native-cancellation-source-originals*
          (loop for symbol in '(physical-backup-lease-status physical-backup-lease-abort
                                physical-backup-lease-cleanup-selected physical-source-follow-release)
                for event in '(:status :abort :cleanup :release)
                collect (let ((original (symbol-function symbol)) (event event))
                          (setf (symbol-function symbol)
                                (lambda (&rest arguments)
                                  (multiple-value-prog1 (apply original arguments)
                                    (sb-thread:with-mutex (*native-cancellation-source-events-lock*)
                                      (push event *native-cancellation-source-events*)))))
                          (cons symbol original)))))
  (sb-thread:with-mutex (*native-cancellation-source-events-lock*)
    (setf *native-cancellation-source-events* nil))
  (values))

(defun native-cancellation-check-source-events (expected)
  (sb-thread:with-mutex (*native-cancellation-source-events-lock*)
    (assert (equal expected (reverse *native-cancellation-source-events*))))
  (values))

(defun call-with-native-cancellation-client (record function)
  (let* ((source (physical-follow-source-copy (getf (getf record :attempt) :source)))
         (request (physical-backup-lease-request-copy (getf (getf record :attempt) :request)))
         (client nil))
    (unwind-protect
         (progn
           (setf client
                 (physical-libpq-connect-frozen
                  (native-cancellation-env "HELPER")
                  (nth-value 1 (physical-follow-source-profile source))
                  :on-spawn (lambda (actor) (setf client actor))))
           (physical-adoption-attempt-check-client client source)
           (physical-reseed-source-identity client request :current-timeline nil)
           (funcall function client request))
      (when client (physical-client-close client)))))

(defun native-cancellation-handoff (owner legacy)
  (let* ((record (physical-reseed-follow-read owner))
         (generation (first (getf record :generations))))
    (assert (eq (if legacy :preparing :proved) (getf generation :phase)))
    (assert (eq legacy (null (physical-reseed-follow-generation-anchor generation))))
    (call-with-native-cancellation-client
     record
     (lambda (client request)
       (let* ((anchor (if legacy
                          (pg-client-native-prepare client (getf generation :name)
                                                    (getf generation :coordinates))
                          (physical-reseed-follow-generation-anchor generation)))
              (successor (list :name (getf generation :name)
                               :anchor-digest (physical-follow-anchor-digest anchor))))
         (assert anchor)
         (assert
          (handler-case
              (progn (pg-client-native-backup-lease-handoff client request successor) nil)
            (pg-client-error () t))))))
    (assert (equal record (physical-reseed-follow-read owner)))
    record))

(defun native-cancellation-reuse-refused (owner)
  (let ((record
          (native-cancellation-read-journal owner)))
    (assert (eq :cleaned (getf (getf record :cancellation) :phase)))
    (call-with-native-cancellation-client
     record
     (lambda (client request)
       (declare (ignore request))
       (let ((generation (first (getf record :generations))))
         (assert generation)
         (assert
          (handler-case
              (progn
                (pg-client-native-prepare client (getf generation :name)
                                          (getf generation :coordinates))
                nil)
            (pg-client-error (condition)
              (assert (equal "55000" (pg-client-error-sqlstate condition)))
              (assert (equal "native source command failed"
                             (pg-client-error-message condition)))
              (format t "~%NATIVE-CANCELLATION-REUSE-REFUSED ~d~%" (sb-posix:getpid))
              t))))))
    record))

(defun native-cancellation-cleanup-unavailable (owner)
  (assert
   (handler-case
       (progn
         (physical-reseed-follow-cleanup-cancellation
          owner :helper (native-cancellation-env "HELPER"))
         nil)
     (pg-client-error (condition)
       (assert (equal "08001" (pg-client-error-sqlstate condition)))
       (assert (equal "libpq connection failed" (pg-client-error-message condition)))
       t)))
  (sb-thread:with-mutex (*physical-libpq-channels-lock*)
    (assert (zerop (hash-table-count *physical-libpq-channels*))))
  (native-cancellation-read-journal owner))

(define-condition native-cancellation-discarded-reply (error) ())

(defun native-cancellation-lose-reply (owner)
  (let* ((kind (native-cancellation-env "LOSE_REPLY"))
         (symbol (cond ((equal kind "abort") 'pg-client-native-backup-lease-abort)
                       ((member kind '("observer" "issuer") :test #'equal)
                        'pg-client-native-backup-lease-cleanup-selected)
                       ((equal kind "release") 'pg-client-native-release)
                       ((equal kind "finalization") 'physical-reseed-follow-cancellation-check-cleanup-stamp)
                       (t (error "unknown native lost reply case"))))
         (ordinal (if (equal kind "issuer") 2 1))
         (original (symbol-function symbol))
         (calls 0)
         (discarded 0))
    (unwind-protect
         (progn
           (setf (symbol-function symbol)
                 (lambda (&rest arguments)
                   (let ((results (multiple-value-list (apply original arguments))))
                     (incf calls)
                     (when (= calls ordinal)
                       (incf discarded)
                       (format t "~%NATIVE-CANCELLATION-DISCARDED ~d ~a ~d~%"
                               (sb-posix:getpid) kind calls)
                       (finish-output)
                       (when (equal "yes" (native-cancellation-env "KILL_EFFECT" "no"))
                         (sb-posix:kill (sb-posix:getpid) 9))
                       (error 'native-cancellation-discarded-reply))
                     (values-list results))))
           (assert
            (handler-case
                (progn
                  (physical-reseed-follow-cleanup-cancellation
                   owner :helper (native-cancellation-env "HELPER"))
                  nil)
              (native-cancellation-discarded-reply () t)))
           (assert (= 1 discarded))
           (assert (= ordinal calls)))
      (setf (symbol-function symbol) original)))
  (sb-thread:with-mutex (*physical-libpq-channels-lock*)
    (assert (zerop (hash-table-count *physical-libpq-channels*))))
  (native-cancellation-read-journal owner))

(defun native-cancellation-run-actions ()
  (let* ((*print-circle* t) (*print-level* 5) (*print-length* 10)
                            (target (uiop:ensure-directory-pathname (native-cancellation-env "TARGET")))
                            (*pgdata* target)
                            (action (native-cancellation-env "ACTION"))
                            (cut-name (native-cancellation-env "CUT" "none"))
                            (cut (cdr (assoc cut-name
                                             '(("create" . :after-follow-temporary-create)
                                               ("write" . :after-follow-write-chunk)
                                               ("fsync" . :after-follow-temporary-fsync)
                                               ("rename" . :after-follow-rename)
                                               ("dirsync" . :after-follow-directory-fsync)
                                               ("readback" . :after-follow-readback))
                                             :test #'equal)))
                            (*physical-reseed-follow-write-limit*
                              (when (equal "yes" (native-cancellation-env "PARTIAL" "no")) 257))
                            (*physical-reseed-follow-test-hook*
                              (when cut
                                (lambda (event path)
                                  (declare (ignore path))
                                  (when (and (eq event cut)
                                             (physical-reseed-follow-cancellation-ack-authority-p
                                              *physical-reseed-follow-cancellation-authority*))
                                    (format t "~%NATIVE-CANCELLATION-CUT ~d ~s ~a~%"
                                            (sb-posix:getpid) event (namestring target))
                                    (finish-output)
                                    (sb-posix:kill (sb-posix:getpid) 9))))))
    (unless (or cut (equal cut-name "none")) (error "unknown native cancellation cut"))
    (call-with-native-cancellation-owner
     target action
     (lambda (owner)
       (let ((record
               (cond
                 ((equal action "prove")
                  (let* ((record (physical-reseed-follow-prepare
                                  owner :helper (native-cancellation-env "HELPER")))
                         (generations (getf record :generations)))
                    (assert (= 1 (length generations)))
                    (assert (eq :proved (getf (first generations) :phase)))
                    (assert (physical-reseed-follow-generation-anchor (first generations)))
                    (assert (equal record (physical-reseed-follow-read owner)))
                    record))
                 ((equal action "configure")
                  (physical-reseed-follow-begin-configuration
                   owner (uiop:ensure-directory-pathname (native-cancellation-env "WORKSPACE")) 12345)
                  (let ((record (physical-reseed-follow-stage-configuration owner)))
                    (loop repeat 8 until (eq :inert (getf (getf record :configuration) :phase))
                          do (setf record (physical-reseed-follow-advance-configuration owner)))
                    (assert (eq :inert (getf (getf record :configuration) :phase)))
                    record))
                 ((equal action "settle")
                  (physical-reseed-follow-settle owner :helper (native-cancellation-env "HELPER")))
                 ((equal action "handoff-allocated") (native-cancellation-handoff owner nil))
                 ((equal action "handoff-legacy") (native-cancellation-handoff owner t))
                 ((equal action "reuse-refused") (native-cancellation-reuse-refused owner))
                 ((equal action "cleanup-unavailable") (native-cancellation-cleanup-unavailable owner))
                 ((equal action "lose-reply") (native-cancellation-lose-reply owner))
                 ((equal action "begin") (physical-reseed-follow-begin-cancellation owner))
                 ((equal action "cleanup")
                  (multiple-value-bind (result outcome)
                      (physical-reseed-follow-cleanup-cancellation owner :helper (native-cancellation-env "HELPER"))
                    (assert (equal (native-cancellation-env "OUTCOME" "complete")
                                   (string-downcase (symbol-name outcome))))
                    result))
                 ((equal action "recover-refused")
                  (assert
                   (handler-case
                       (progn
                         (physical-reseed-follow-recover-cleaned-cancellation
                          owner :helper (native-cancellation-env "HELPER"))
                         nil)
                     (error (condition)
                       (assert (search (native-cancellation-env "REFUSAL_CAUSE")
                                       (princ-to-string condition)))
                       (format t "~%NATIVE-CANCELLATION-REFUSED ~d ~a~%"
                               (sb-posix:getpid) condition)
                       t)))
                  (native-cancellation-read-journal owner))
                 ((equal action "recover")
                  (multiple-value-bind (result outcome)
                      (physical-reseed-follow-recover-cleaned-cancellation owner :helper (native-cancellation-env "HELPER"))
                    (assert (eq :complete outcome))
                    result))
                 (t (error "unknown native cancellation action")))))
         (when (equal "yes" (native-cancellation-env "CONFIGURED" "no"))
           (assert (eq :inert (getf (getf record :configuration) :phase)))
           (assert (= 1 (length (getf record :generations))))
           (let ((generation (first (getf record :generations))))
             (assert (eq :proved (getf generation :phase)))
             (assert (physical-reseed-follow-generation-anchor generation))))
         (let ((phase (getf (getf record :cancellation) :phase)))
           (assert (equal (native-cancellation-env "PHASE" "none")
                          (if phase (string-downcase (symbol-name phase)) "none"))))
         (assert (not (physical-reseed-follow-owner-busy-p owner)))
         (assert (not (physical-reseed-follow-owner-uncertain-p owner))))))
    (sb-thread:with-mutex (*physical-libpq-channels-lock*)
      (assert (zerop (hash-table-count *physical-libpq-channels*))))
    (when cut (error "native cancellation cut was not reached"))
    (format t "~%NATIVE-CANCELLATION-CHECKED ~d ~a~%" (sb-posix:getpid) action)
    (finish-output)
    (values)))

(defun native-cancellation-run ()
  (let ((setting (native-cancellation-env "EXPECT_CONNECTIONS" "any"))
        (original (symbol-function 'physical-libpq-connect-frozen))
        (connections 0))
    (if (equal setting "any")
        (native-cancellation-run-actions)
        (unwind-protect
             (progn
               (setf (symbol-function 'physical-libpq-connect-frozen)
                     (lambda (&rest arguments)
                       (incf connections)
                       (apply original arguments)))
               (native-cancellation-run-actions)
               (assert (= (parse-integer setting) connections))
               (format t "~%NATIVE-CANCELLATION-CONNECTIONS ~d ~d~%"
                       (sb-posix:getpid) connections)
               (finish-output))
          (setf (symbol-function 'physical-libpq-connect-frozen) original)))))

(defun native-cancellation-pause-closing ()
  (let ((original (symbol-function 'physical-backup-lease-finish-closing-under-lock)))
    (setf (symbol-function 'physical-backup-lease-finish-closing-under-lock)
          (lambda (request begin-publication &optional authority)
            (declare (ignore begin-publication authority))
            (setf (symbol-function 'physical-backup-lease-finish-closing-under-lock) original)
            (physical-backup-lease-copy (physical-backup-lease-current-record request)))))
  (values))

(defun native-cancellation-lose-selection (lease-name name absent)
  (with-write-lock
    (with-wal-critical
      (let ((record (gethash lease-name *physical-backup-lease-records*))
            (pin (gethash name *physical-source-pins*)))
        (assert record)
        (assert (eq :closing (getf record :phase)))
        (assert (eq :handed-off (getf record :outcome)))
        (assert (equal name (getf (getf record :successor) :name)))
        (assert (gethash lease-name *physical-source-pins*))
        (assert pin)
        (if absent
            (progn
              (delete-file (physical-source-pin-path name))
              (fsync-directory (physical-source-pin-directory))
              (remhash name *physical-source-pins*))
            (progn
              (setf (physical-source-pin-lost-p pin) t)
              (physical-source-pin-save pin)))
        (remhash pin *wal-snapshot-retention-pins*))))
  (values))

(defun native-cancellation-check-source (lease-name follow-name &optional selected-present)
  (let ((valid
          (with-write-lock
            (sb-thread:with-mutex (*wal-lock*)
              (let ((record (gethash lease-name *physical-backup-lease-records*))
                    (pin (gethash follow-name *physical-source-pins*)))
                (and record
                     (equal record (physical-backup-lease-current-record (getf record :request)))
                     (eq :closed (getf record :phase))
                     (eq :handed-off (getf record :outcome))
                     (equal follow-name (getf (getf record :successor) :name))
                     (null (gethash lease-name *physical-source-pins*))
                     (not (physical-backup-lease-lstat (physical-source-pin-path lease-name)))
                     (if selected-present
                         (and pin (physical-source-pin-lost-p pin)
                              (physical-backup-lease-lstat (physical-source-pin-path follow-name))
                              (not (gethash pin *wal-snapshot-retention-pins*)))
                         (and (null pin)
                              (not (physical-backup-lease-lstat (physical-source-pin-path follow-name)))))
                     (not *wal-poisoned*)))))))
    (assert valid))
  (sb-thread:with-mutex (*physical-libpq-channels-lock*)
    (assert (zerop (hash-table-count *physical-libpq-channels*))))
  (values))

(defvar *native-cancellation-reuse-witness* nil)

(defun native-cancellation-observe-reuse ()
  (let ((original (symbol-function 'physical-source-follow-existing-anchor)))
    (setf *native-cancellation-reuse-witness* nil)
    (setf (symbol-function 'physical-source-follow-existing-anchor)
          (lambda (name request)
            (setf (symbol-function 'physical-source-follow-existing-anchor) original)
            (handler-case (funcall original name request)
              (wal-input-validation-error (condition)
                (setf *native-cancellation-reuse-witness*
                      (list (copy-seq name) (princ-to-string condition)))
                (error condition))))))
  (values))

(defun native-cancellation-check-reuse-witness (name allocated)
  (multiple-value-bind (witness historical)
      (with-write-lock
        (sb-thread:with-mutex (*wal-lock*)
          (values
           (copy-tree *native-cancellation-reuse-witness*)
           (handler-case
               (progn (physical-backup-lease-check-successor-create name) nil)
             (wal-input-validation-error (condition)
               (princ-to-string condition))))))
    (assert (equal name (first witness)))
    (assert (if allocated
                (search "backup lease allocation lacks its current operation owner"
                        (second witness))
                (or (equal "physical follow attempt was canceled; use a new pin name"
                           (second witness))
                    (search "historical successor permanently reserves" (second witness)))))
    (assert (search "historical successor permanently reserves" historical)))
  (values))

(defun native-cancellation-publish-selection (target name)
  (let ((*pgdata* target))
    (call-with-physical-reseed-follow-owner
     target
     (lambda (owner)
       (let* ((initial (physical-reseed-follow-read owner))
              (record (physical-reseed-follow-copy initial))
              (base (physical-recovery-verify-base-state)))
         (assert (null (getf initial :generations)))
         (multiple-value-bind (bytes entry)
             (physical-recovery-read-base-overlap base target)
           (setf (getf record :generations)
                 (list (list :generation 1 :name name
                             :coordinates
                             (list :version 1 :system-id (getf base :system-id)
                                   :base-timeline (getf base :timeline) :base (getf base :end)
                                   :manifest-digest (getf base :manifest-digest)
                                   :source-timeline (getf (getf (getf record :attempt) :request) :timeline)
                                   :receive-timeline (getf base :timeline) :receive-lsn (getf base :end)
                                   :receive-digest #xCBF29CE484222325
                                   :overlap-start (- (getf base :end) (length bytes))
                                   :overlap-digest (third entry))
                             :source-history "" :phase :preparing :anchor nil))))
         (physical-reseed-follow-publish-intent owner record :previous initial))))))

(defun native-cancellation-install-initial (target stage incoming quarantine request profile helper)
  (call-with-physical-reseed-owner
   target
   (lambda (owner)
     (physical-reseed-start-remote owner stage request profile :helper helper)
     (physical-reseed-prepare-image owner incoming :helper helper)
     (physical-reseed-publish-install owner incoming quarantine :helper helper)))
  (let ((plan (getf (physical-reseed-install-initial-arguments target) :plan)))
    (call-with-physical-reseed-install-owner
     target
     (lambda (owner)
       (loop repeat (physical-reseed-install-last-step plan)
             do (physical-reseed-install-advance owner :helper helper)))))
  (call-with-physical-reseed-follow-owner
   target
   (lambda (owner)
     (let ((record (physical-reseed-follow-publish-intent
                    owner (physical-reseed-follow-initial-record owner))))
       (assert (null (getf record :generations)))))))

(defvar *native-cancellation-install-worker* nil)

(defun native-cancellation-start-initial-install (target stage incoming quarantine index port helper go done &optional follow-name)
  (assert (null *native-cancellation-install-worker*))
  (sb-sys:without-interrupts
    (setf *native-cancellation-install-worker*
          (sb-thread:make-thread
           (lambda ()
             (loop until (probe-file go) do (sleep 0.01))
             (let ((result
                     (handler-case
                         (progn
                           (native-cancellation-install-initial
                            target stage incoming quarantine
                            (list :version 1 :name (string-downcase (format nil "reseed_~32,'0x" index))
                                  :token (string-downcase (format nil "~64,'0x" (+ index 1000)))
                                  :cluster-identity (catalog-cluster-identity *catalog*)
                                  :system-id (catalog-system-identifier) :timeline *wal-insert-timeline*)
                            (make-physical-libpq-profile
                             (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres" port)
                             :mode :physical :fallback "native-target-follow")
                            helper)
                           (when follow-name (native-cancellation-publish-selection target follow-name))
                           "PASS")
                       (error (condition) (format nil "ERROR ~a" condition)))))
               (with-open-file (out done :direction :output :if-exists :error)
                 (sb-posix:chmod done #o600)
                 (write-line result out))))
           :name "owned-native-initial-install")))
  (values))

(defun native-cancellation-join-initial-install ()
  (assert *native-cancellation-install-worker*)
  (sb-thread:join-thread *native-cancellation-install-worker*)
  (setf *native-cancellation-install-worker* nil)
  (values))

(defun native-cancellation-check-aborted-source (lease-name)
  (let ((valid
          (with-write-lock
            (sb-thread:with-mutex (*wal-lock*)
              (let ((record (gethash lease-name *physical-backup-lease-records*)))
                (and record
                     (equal record (physical-backup-lease-current-record (getf record :request)))
                     (eq :closed (getf record :phase))
                     (eq :aborted (getf record :outcome))
                     (null (getf record :successor))
                     (null (gethash lease-name *physical-source-pins*))
                     (not (physical-backup-lease-lstat (physical-source-pin-path lease-name)))
                     (not *wal-poisoned*)))))))
    (assert valid))
  (values))

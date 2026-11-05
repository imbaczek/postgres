(in-package #:pg)

(defun native-settlement-test-env (name &optional default)
  (or (sb-ext:posix-getenv (concatenate 'string "NATIVE_SETTLEMENT_" name))
      default (error "missing native settlement test setting ~a" name)))

(defun call-with-native-settlement-test-offline (function)
  (let ((original (symbol-function 'physical-libpq-connect-frozen)) (connections 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-libpq-connect-frozen)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (incf connections) (error "offline action attempted source I/O")))
           (multiple-value-prog1 (funcall function) (assert (zerop connections))))
      (setf (symbol-function 'physical-libpq-connect-frozen) original))))

(defun native-settlement-test-check (record)
  ;; Derive the observed prefix from the saved or root control file and check
  ;; it against the journal and the active enablement bit.
  (let* ((phase (getf (getf record :settlement) :phase))
         (expected (native-settlement-test-env "EXPECTED" "none"))
         (present (probe-file (physical-follow-config-path)))
         (config (if present
                     (physical-follow-read-config)
                     (physical-follow-decode-config
                      (recovery-read-bytes
                       (merge-pathnames "saved/follow.pending"
                                        (uiop:ensure-directory-pathname
                                         (native-settlement-test-env "WORKSPACE")))
                       :maximum +physical-follow-config-maximum-bytes+))))
         (prefix (cond ((not present) 1)
                       ((eq :active (getf (fifth config) :phase)) 2)
                       (t 0))))
    (assert (eq :inert (getf (getf record :configuration) :phase)))
    (assert (equal expected
                   (ecase phase
                     ((nil) "none") (:handoff-pending "pending")
                     (:handed-off "handed-off") (:source-aborted "source-aborted"))))
    (physical-reseed-follow-activation-check-prefix record prefix)
    (assert (eq (if (= prefix 2) :active :pending) (getf (fifth config) :phase)))
    (assert (eq (= prefix 2) (not (null (getf (sixth config) :enabled)))))))

;; Drive target settlement and activation actions, including offline local
;; progress and native source retries.
(defun native-settlement-test-action (owner action)
  ;; Stage the local active image and exercise source retries and rejected
  ;; allocations through native test connections, alongside settlement actions.
  (cond
    ((equal action "install")
     (physical-reseed-follow-begin-configuration
      owner (uiop:ensure-directory-pathname (native-settlement-test-env "WORKSPACE")) 12345)
     (let ((record (physical-reseed-follow-stage-configuration owner)))
       (loop repeat 8 until (eq :inert (getf (getf record :configuration) :phase))
             do (setf record (physical-reseed-follow-advance-configuration owner)))
       record))
    ;; Exercise local progress and recovery offline.  Initial intent and its
    ;; recovery exercise native source retries, with committed retry offline.
    ((equal action "advance-activation")
     (let* ((before (physical-reseed-follow-read owner))
            (record (physical-reseed-follow-advance-activation owner)))
       (assert (equal record (physical-reseed-follow-activation-progress-record before)))
       record))
    ;; Read the bounded final journal directly while a temporary may exist.
    ((equal action "recover-activation-progress")
     (let* ((before (call-with-physical-directory-root
                     owner (lambda (fd directory)
                             (declare (ignore directory))
                             (physical-reseed-follow-read-at fd "physical-reseed.follow"))))
            (expected (if (probe-file (merge-pathnames "physical-reseed.follow.tmp" *pgdata*))
                          (physical-reseed-follow-activation-progress-record before)
                          before))
            (record (physical-reseed-follow-recover-activation owner)))
       (assert (equal expected record))
       record))
    ((equal action "active-activation")
     (let ((before (physical-reseed-follow-read owner)))
       (assert (eq :active (getf (getf before :activation) :phase)))
       (assert (equal before (physical-reseed-follow-advance-activation owner)))
       before))
    ((equal action "start-activation")
     (let ((record (physical-reseed-follow-start-activation owner :helper (native-settlement-test-env "HELPER"))))
       (assert (eq :activating (getf (getf record :activation) :phase)))
       (assert (zerop (getf (getf record :activation) :step)))
       record))
    ((equal action "start-activation-reject")
     (assert (handler-case
                 (progn
                   (physical-reseed-follow-start-activation owner :helper (native-settlement-test-env "HELPER"))
                   nil)
               (pg-client-error () t)))
     (assert (not (physical-reseed-follow-owner-busy-p owner)))
     (assert (not (physical-reseed-follow-owner-uncertain-p owner)))
     (physical-reseed-follow-read owner))
    ((equal action "existing-activation")
     (call-with-native-settlement-test-offline
      (lambda ()
        (let ((record (physical-reseed-follow-start-activation owner :helper (native-settlement-test-env "HELPER"))))
          (assert (eq :activating (getf (getf record :activation) :phase)))
          (assert (zerop (getf (getf record :activation) :step)))
          record))))
    ((equal action "recover-activation")
     (let ((record (physical-reseed-follow-recover-activation owner :helper (native-settlement-test-env "HELPER"))))
       (assert (eq :activating (getf (getf record :activation) :phase)))
       (assert (zerop (getf (getf record :activation) :step)))
       record))
    ((equal action "recover-activation-reject")
     (assert (handler-case
                 (progn
                   (physical-reseed-follow-recover-activation owner :helper (native-settlement-test-env "HELPER"))
                   nil)
               (pg-client-error () t)))
     (assert (not (physical-reseed-follow-owner-busy-p owner)))
     (assert (physical-reseed-follow-owner-uncertain-p owner))
     nil)
    ((equal action "stage-activation")
     (physical-reseed-follow-begin-activation owner)
     (physical-reseed-follow-stage-activation owner))
    ((equal action "probe-activation")
     (multiple-value-bind (anchor snapshot)
         (physical-reseed-follow-probe-activation-source owner :helper (native-settlement-test-env "HELPER"))
       (let ((record (getf (getf snapshot :follow) :record)))
         (assert (equal anchor (physical-reseed-follow-generation-anchor (first (getf record :generations)))))
         (assert (eq :staged (getf (getf record :activation) :phase)))
         (assert (not (physical-reseed-follow-owner-uncertain-p owner)))
         record)))
    ((equal action "probe-activation-reject")
     (assert (handler-case
                 (progn
                   (physical-reseed-follow-probe-activation-source owner :helper (native-settlement-test-env "HELPER"))
                   nil)
               (pg-client-error () t)))
     (assert (not (physical-reseed-follow-owner-busy-p owner)))
     (assert (not (physical-reseed-follow-owner-uncertain-p owner)))
     (physical-reseed-follow-read owner))
    ((equal action "probe-wrong-allocation")
     (let* ((record (physical-reseed-follow-read owner))
            (attempt (getf record :attempt))
            (source (physical-follow-source-copy (getf attempt :source)))
            (generation (first (getf record :generations)))
            (frozen (nth-value 1 (physical-follow-source-profile source)))
            (client nil))
       (unwind-protect
            (progn
              (setf client
                    (physical-libpq-connect-frozen
                     (native-settlement-test-env "HELPER") frozen
                     :on-spawn (lambda (actor) (setf client actor))))
              (physical-adoption-attempt-check-client client source)
              (assert
               (handler-case
                   (progn
                     (pg-client-native-backup-lease-follow-prepare
                      client (physical-backup-lease-request-copy (getf attempt :request))
                      (if (equal "generation" (native-settlement-test-env "WRONG"))
                          (1+ (getf generation :generation))
                          (getf generation :generation))
                      (if (equal "name" (native-settlement-test-env "WRONG"))
                          (concatenate 'string (getf generation :name) "_wrong")
                          (copy-seq (getf generation :name)))
                      (physical-follow-request-copy (getf generation :coordinates)))
                     nil)
                 (pg-client-error () t))))
         (when client (physical-client-close client)))
       record))
    ((equal action "pending") (physical-reseed-follow-begin-settlement owner))
    ((equal action "settle")
     (physical-reseed-follow-settle owner :helper (native-settlement-test-env "HELPER")))
    ((equal action "settle-error")
     (assert (handler-case
                 (progn (physical-reseed-follow-settle owner :helper (native-settlement-test-env "HELPER")) nil)
               (pg-client-error () t)))
     (assert (not (physical-reseed-follow-owner-busy-p owner)))
     (physical-reseed-follow-read owner))
    ((equal action "settle-reject")
     (assert (handler-case
                 (progn (physical-reseed-follow-settle owner :helper (native-settlement-test-env "HELPER")) nil)
               (error () t)))
     (assert (not (physical-reseed-follow-owner-busy-p owner)))
     (physical-reseed-follow-read owner))
    ((equal action "recover")
     (if (equal "yes" (native-settlement-test-env "REFUSE" "no"))
         (progn
           (assert (handler-case
                       (progn (physical-reseed-follow-recover-configuration owner) nil)
                     (error () t)))
           (assert (physical-reseed-follow-owner-uncertain-p owner))
           nil)
         (physical-reseed-follow-recover-configuration owner)))
    ((equal action "check") (physical-reseed-follow-read owner))
    (t (error "unknown native settlement action"))))

;; Install crash hooks at named target publication points. Check the resulting
;; configuration against the journal and verify startup remains fenced.
(defun native-settlement-test-run ()
  (let* ((*print-circle* t) (*print-level* 5) (*print-length* 10)
                            (target (uiop:ensure-directory-pathname (native-settlement-test-env "TARGET")))
                            (*pgdata* target)
                            (action (native-settlement-test-env "ACTION"))
                            (cut-name (native-settlement-test-env "CUT" "none"))
                            (cut (cdr (assoc cut-name
                                             '(("create" . :after-follow-temporary-create)
                                               ("write" . :after-follow-write-chunk)
                                               ("fsync" . :after-follow-temporary-fsync)
                                               ("rename" . :after-follow-rename)
                                               ("dirsync" . :after-follow-directory-fsync)
                                               ("before-create" . :before-follow-temporary-create)
                                               ("move" . :after-activation-move)
                                               ("move-file-fsync" . :after-activation-moved-file-fsync)
                                               ("move-source-fsync" . :after-activation-source-directory-fsync)
                                               ("move-destination-fsync" . :after-activation-destination-directory-fsync))
                                             :test #'equal)))
                            (*physical-reseed-follow-write-limit*
                              (when (equal "yes" (native-settlement-test-env "PARTIAL" "no")) 257))
                            (*physical-reseed-follow-test-hook*
                              (when cut
                                (lambda (event path)
                                  (declare (ignore path))
                                  (when (eq event cut)
                                    (write-line "NATIVE-SETTLEMENT-CUT-REACHED")
                                    (finish-output)
                                    (sb-posix:kill (sb-posix:getpid) 9))))))
    (unless (or cut (equal cut-name "none")) (error "unknown native settlement cut"))
    (if (equal action "prove")
        (call-with-physical-reseed-follow-owner
         target
         (lambda (owner)
           (let ((record (physical-reseed-follow-prepare owner :helper (native-settlement-test-env "HELPER"))))
             (assert (eq :proved (getf (first (getf record :generations)) :phase))))))
        (flet ((perform ()
                 (call-with-physical-reseed-follow-configuration-owner
                  target
                  (lambda (owner)
                    (let ((record (native-settlement-test-action owner action)))
                      (when record (native-settlement-test-check record))
                      (assert (not (physical-reseed-follow-owner-busy-p owner))))))))
          (if (member action '("install" "pending" "recover" "check" "stage-activation"
                               "advance-activation" "recover-activation-progress" "active-activation") :test #'equal)
              (call-with-native-settlement-test-offline #'perform)
              (perform))
          (assert (handler-case (progn (physical-reseed-check-before-pid) nil) (error () t)))))
    (when cut (error "native settlement cut was not reached"))
    (write-line "NATIVE-SETTLEMENT-CHECKED")
    (values)))

;; Inspect the actual source lease and pins under the source locks.
(defun native-settlement-test-source-audit (lease-name follow-name expected)
  (with-write-lock
    (with-wal-critical
      (let ((record (gethash lease-name *physical-backup-lease-records*))
            (pin (gethash follow-name *physical-source-pins*)))
        (assert record)
        (assert (equal record (physical-backup-lease-current-record (getf record :request))))
        (if (equal expected "source-aborted")
            (progn
              (assert (eq :closed (getf record :phase)))
              (assert (eq :aborted (getf record :outcome)))
              (assert (null (getf record :successor)))
              (assert (null pin))
              (assert (not (gethash lease-name *physical-source-pins*)))
              (assert (not (physical-backup-lease-lstat (physical-source-pin-path lease-name))))
              (assert (not (physical-backup-lease-lstat (physical-source-pin-path follow-name)))))
            (progn
              (assert (physical-follow-pin-p pin))
              (assert (physical-follow-pin-ready-p pin))
              (assert (not (physical-source-pin-lost-p pin)))
              (physical-follow-pin-check-wal pin)
              (cond
                ((equal expected "ready")
                 (assert (eq :ready (getf record :phase)))
                 (assert (gethash lease-name *physical-source-pins*)))
                ((equal expected "closing")
                 (assert (eq :closing (getf record :phase)))
                 (assert (eq :handed-off (getf record :outcome)))
                 (assert (gethash lease-name *physical-source-pins*))
                 (physical-backup-lease-check-successor-pin record pin))
                ((equal expected "handed-off")
                 (assert (eq :closed (getf record :phase)))
                 (assert (eq :handed-off (getf record :outcome)))
                 (assert (equal follow-name (getf (getf record :successor) :name)))
                 (assert (not (gethash lease-name *physical-source-pins*)))
                 (assert (not (physical-backup-lease-lstat (physical-source-pin-path lease-name))))
                 (physical-backup-lease-check-successor-pin record pin))
                (t (error "unknown source audit outcome"))))))
      (assert (not *wal-poisoned*))))
  (values))

(defvar *native-settlement-test-row-sender* nil)

(defvar *native-settlement-test-lose-reply* nil)

(defvar *native-settlement-test-handoffs* 0)

(defvar *native-settlement-test-statuses* 0)

(defvar *native-activation-test-saved-pin* nil)

(defun native-activation-test-source-pin-fault (name mode)
  ;; Inject runtime source pin faults under the write and WAL locks.  Restore
  ;; the saved pin and cancellation state under the same locks after each case.
  (with-write-lock
    (with-wal-critical
      (if (eq mode :restore)
          (destructuring-bind (saved-name pin canceled-p canceled) *native-activation-test-saved-pin*
            (assert (equal name saved-name))
            (setf (gethash name *physical-source-pins*) pin)
            (if canceled-p
                (setf (gethash name *physical-follow-aborted-attempts*) canceled)
                (remhash name *physical-follow-aborted-attempts*))
            (setf *native-activation-test-saved-pin* nil)
            (physical-follow-pin-check-wal pin))
          (let ((pin (gethash name *physical-source-pins*)))
            (assert (null *native-activation-test-saved-pin*))
            (assert (physical-follow-pin-ready-p pin))
            (multiple-value-bind (canceled canceled-p)
                (gethash name *physical-follow-aborted-attempts*)
              (setf *native-activation-test-saved-pin* (list name pin canceled-p canceled)))
            (ecase mode
              (:canceled (setf (gethash name *physical-follow-aborted-attempts*) t))
              ((:unready :replaced)
               (let* ((copy (copy-physical-follow-pin pin))
                      (anchor (physical-follow-anchor-copy (physical-follow-pin-anchor pin))))
                 (ecase mode
                   (:unready
                    (setf (physical-follow-pin-stage copy) :retaining
                          (physical-source-pin-confirmed-lsn copy) nil
                          (physical-source-pin-restart-lsn copy) (getf anchor :overlap-start)
                          (physical-source-pin-prefix-digest copy) (getf anchor :receive-digest)))
                   (:replaced
                    (setf (getf anchor :overlap-digest) (logxor 1 (getf anchor :overlap-digest))
                          (physical-follow-pin-anchor copy) anchor)))
                 (physical-follow-pin-check-fields copy)
                 (setf (gethash name *physical-source-pins*) copy))))))))
  (values))

(defvar *native-settlement-test-reply-mode* "normal")

(defvar *native-settlement-test-finish-closing* nil)

;; Inject lost or altered replies separately from source state changes.  The
;; closing cut kills the source after its closing state is durable; closing
;; reply leaves that actual state unfinished for a later retry.
(defun native-settlement-test-source-hook (&key lose-reply closing-cut closing-reply (reply-mode "normal"))
  (unless *native-settlement-test-row-sender*
    (setf *native-settlement-test-row-sender* (symbol-function 'send-native-backup-lease-row)))
  (unless *native-settlement-test-finish-closing*
    (setf *native-settlement-test-finish-closing*
          (symbol-function 'physical-backup-lease-finish-closing-under-lock)))
  (setf *native-settlement-test-lose-reply* lose-reply
        *native-settlement-test-reply-mode* reply-mode
        *native-settlement-test-handoffs* 0
        *native-settlement-test-statuses* 0
        (symbol-function 'physical-backup-lease-finish-closing-under-lock)
        (if closing-reply
            (lambda (request begin-publication)
              (declare (ignore begin-publication))
              (setf (symbol-function 'physical-backup-lease-finish-closing-under-lock)
                    *native-settlement-test-finish-closing*)
              (let ((record (physical-backup-lease-current-record request)))
                (assert (eq :closing (getf record :phase)))
                (assert (eq :handed-off (getf record :outcome)))
                (physical-backup-lease-copy record)))
            *native-settlement-test-finish-closing*)
        (symbol-function 'send-native-backup-lease-row)
        (lambda (stream row schema tag)
          (cond
            ((equal tag "BACKUP_LEASE_HANDOFF")
             (incf *native-settlement-test-handoffs*)
             (when *native-settlement-test-lose-reply*
               (setf *native-settlement-test-lose-reply* nil)
               (close stream :abort t)
               (error "native test lost committed handoff reply")))
            ((equal tag "BACKUP_LEASE_STATUS")
             (incf *native-settlement-test-statuses*)))
          (let ((reply (copy-list row)))
            (cond
              ((and (equal tag "BACKUP_LEASE_STATUS")
                    (equal *native-settlement-test-reply-mode* "unknown"))
               (setf reply (list (first row) "unknown" nil nil nil nil nil nil)))
              ((and (equal tag "BACKUP_LEASE_STATUS")
                    (equal *native-settlement-test-reply-mode* "wrong-base"))
               (setf (nth 4 reply) (wal-format-lsn (+ 8 (pg-client-native-lsn (nth 4 reply))))))
              ((and (equal tag "BACKUP_LEASE_HANDOFF")
                    (equal *native-settlement-test-reply-mode* "wrong-successor"))
               (setf (nth 6 reply) "native_wrong_successor")))
            (funcall *native-settlement-test-row-sender* stream reply schema tag)))
        *physical-backup-lease-test-hook*
        (when closing-cut
          (lambda (event record)
            (when (and (eq event :after-directory-fsync)
                       (eq :closing (getf record :phase))
                       (eq :handed-off (getf record :outcome)))
              (write-line "NATIVE-SOURCE-CLOSING-CUT-REACHED")
              (finish-output)
              (sb-posix:kill (sb-posix:getpid) 9)))))
  (values))

(defun native-settlement-test-source-counts (handoffs statuses)
  (assert (= handoffs *native-settlement-test-handoffs*))
  (assert (= statuses *native-settlement-test-statuses*))
  (values))

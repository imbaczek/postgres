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
  (let* ((phase (getf (getf record :settlement) :phase))
         (expected (native-settlement-test-env "EXPECTED" "none"))
         (config (physical-follow-read-config)))
    (assert (eq :inert (getf (getf record :configuration) :phase)))
    (assert (equal expected
                   (ecase phase
                     ((nil) "none") (:handoff-pending "pending")
                     (:handed-off "handed-off") (:source-aborted "source-aborted"))))
    (assert (eq :pending (getf (fifth config) :phase)))
    (assert (not (getf (sixth config) :enabled)))))

;; Drive one target action.  Recovery and local checks are exercised offline;
;; the remote actions use the native source helper.
(defun native-settlement-test-action (owner action)
  (cond
    ((equal action "install")
     (physical-reseed-follow-begin-configuration
      owner (uiop:ensure-directory-pathname (native-settlement-test-env "WORKSPACE")) 12345)
     (let ((record (physical-reseed-follow-stage-configuration owner)))
       (loop repeat 8 until (eq :inert (getf (getf record :configuration) :phase))
             do (setf record (physical-reseed-follow-advance-configuration owner)))
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

;; Install crash hooks at named target publication points and check that the
;; resulting configuration remains disabled and startup remains fenced.
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
                                               ("before-create" . :before-follow-temporary-create))
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
          (if (member action '("install" "pending" "recover" "check") :test #'equal)
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

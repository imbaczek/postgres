(in-package #:pg)

;;; Client primitives for native lease commands and bundle receipt.  Before
;;; the first RPC, the caller must durably save the original lease ID, token,
;;; source context, frozen route and credential owner.  The caller owns the
;;; channel, empty target directory, and cleanup after errors.  These functions
;;; do not journal the request, install the target, retry, or create a new ID.
;;;
;;; The helper closes its channel on any query error.  Reconnect to the
;;; original frozen route to ask STATUS or ABORT after an uncertain result.

;;; Here, helper refers to the libpq transport. A raw pg-client connection
;;; remains open after an ordinary SQL error.
(defun pg-client-native-backup-lease-literal (value)
  (unless (and (stringp value) (not (find (code-char 0) value)))
    (error "invalid native backup lease query value"))
  (with-output-to-string (output)
    (write-char #\' output)
    (loop for character across value
          do (write-char character output)
             (when (char= character #\') (write-char character output)))
    (write-char #\' output)))

(defun pg-client-native-backup-lease-query (verb request)
  (unless (assoc verb *physical-backup-lease-wire-verbs* :test #'equal)
    (error "invalid native backup lease query command"))
  (let* ((request (physical-backup-lease-request-copy request))
         (options (list (cons "native_format" +physical-native-format+)
                        (cons "lease_name" (getf request :name))
                        (cons "lease_token" (getf request :token))
                        (cons "cluster_identity" (getf request :cluster-identity))
                        (cons "system_id" (format nil "~d" (getf request :system-id)))
                        (cons "source_timeline" (format nil "~d" (getf request :timeline))))))
    (with-output-to-string (output)
      (format output "~a (" verb)
      (loop for (name . value) in options for first-p = t then nil
            do (unless first-p (write-string ", " output))
               (format output "~a ~a" name (pg-client-native-backup-lease-literal value)))
      (write-char #\) output))))

;; Validate the typed row and its owner.  UNKNOWN returns NIL; base and
;; successor fields must each be entirely present or entirely null.
(defun pg-client-native-backup-lease-record (row request)
  (let* ((request (physical-backup-lease-request-copy request))
         (row (physical-client-check-row row *physical-backup-lease-result-schema*))
         (phase (cdr (assoc (aref row 1)
                            '(("requested" . :requested) ("capturing" . :capturing) ("ready" . :ready)
                              ("interrupted" . :interrupted) ("closing" . :closing) ("closed" . :closed))
                            :test #'equal))))
    (unless (equal (getf request :name) (aref row 0))
      (pg-client-native-reject "native backup lease result has a different owner"))
    (when (equal "unknown" (aref row 1))
      (unless (every #'null (subseq row 2))
        (pg-client-native-reject "unknown backup lease has unexpected state"))
      (return-from pg-client-native-backup-lease-record nil))
    (unless phase (pg-client-native-reject "native backup lease result has an invalid phase"))
    (handler-case
        (let ((base-fields (subseq row 3 6)) (successor-fields (subseq row 6 8))
                                             (outcome (cdr (assoc (aref row 2) '(("aborted" . :aborted) ("handed-off" . :handed-off))
                                                                  :test #'equal))))
          (when (and (aref row 2) (null outcome)) (error "invalid outcome"))
          (unless (or (every #'null base-fields) (every #'stringp base-fields))
            (error "partial base identity"))
          (unless (or (every #'null successor-fields) (every #'stringp successor-fields))
            (error "partial successor identity"))
          (physical-backup-lease-copy
           (list :version 1 :request request :phase phase :outcome outcome
                 :base (when (aref row 3)
                         (list :checkpoint-start (pg-client-native-lsn (aref row 3))
                               :base (pg-client-native-lsn (aref row 4))
                               :manifest-digest (pg-client-native-u64 (aref row 5))))
                 :successor (when (aref row 6)
                              (list :name (aref row 6) :anchor-digest (pg-client-native-u64 (aref row 7)))))))
      (pg-client-error (condition) (error condition))
      (error () (pg-client-native-reject "native backup lease result has inconsistent state")))))

(defun pg-client-native-backup-lease-command (client verb request)
  (unless (member verb '("BACKUP_LEASE_STATUS" "BACKUP_LEASE_ABORT") :test #'equal)
    (error "native backup lease command requires a status result"))
  (let* ((request (physical-backup-lease-request-copy request))
         (row (physical-client-query client (pg-client-native-backup-lease-query verb request)
                                     *physical-backup-lease-result-schema* verb :maximum 4096))
         (record (pg-client-native-backup-lease-record row request)))
    (when (and (equal verb "BACKUP_LEASE_ABORT")
               (not (and (member (getf record :phase) '(:closing :closed))
                         (eq :aborted (getf record :outcome)))))
      (pg-client-native-reject "native backup lease abort did not confirm cancellation"))
    record))

(defun pg-client-native-backup-lease-status (client request)
  (pg-client-native-backup-lease-command client "BACKUP_LEASE_STATUS" request))

(defun pg-client-native-backup-lease-abort (client request)
  (pg-client-native-backup-lease-command client "BACKUP_LEASE_ABORT" request))

(defun physical-backup-lease-check-received-status (manifest digest request record)
  (setf request (physical-backup-lease-request-copy request))
  (unless (eq :ready (getf record :phase))
    (error 'pg-client-error :sqlstate "55000" :message "source backup lease is no longer ready"))
  (let ((base (getf record :base)))
    (unless (and (equal request (getf record :request))
                 (equal (getf request :name) (getf manifest :source-pin))
                 (equal (getf request :cluster-identity) (getf manifest :cluster-identity))
                 (eql (getf request :system-id) (getf manifest :system-id))
                 (eql (getf request :timeline) (getf manifest :timeline))
                 (eql (getf base :checkpoint-start) (physical-manifest-checkpoint-start manifest))
                 (eql (getf base :base) (getf manifest :end))
                 (eql (getf base :manifest-digest) digest))
      (pg-client-native-reject "native backup lease differs from the received image")))
  (physical-backup-lease-copy record))

;; Finish the stream, reread the manifest, and bind it to a READY status for
;; the original request before accepting the received inventory.

(defun physical-backup-lease-receive-remote (target channel request)
  (physical-libpq-start-copy-out channel
                                 (pg-client-native-backup-lease-query "CAPTURE_BACKUP_NATIVE" request))
  (let ((manifest (physical-backup-receive-bundle
                   target (lambda () (physical-backup-next-copy-frame channel "CAPTURE_BACKUP_NATIVE")))))
    (physical-backup-finish-copy channel "CAPTURE_BACKUP_NATIVE")
    (multiple-value-bind (actual digest) (recovery-read-manifest target)
      (unless (equal manifest actual) (pg-client-native-reject "received backup manifest changed"))
      (let ((record (physical-backup-lease-check-received-status
                     actual digest request (pg-client-native-backup-lease-status channel request))))
        (physical-backup-check-received-inventory target manifest)
        (values manifest record)))))

;; A supplied descriptor selects scoped descriptor-relative receipt.  Without
;; it, preserve the existing pathname-based receiver.

;; Receive into the caller's empty directory, verify the manifest, then check
;; STATUS against the received image.  READY describes source retention at
;; that check, not a permanent guarantee; follow or handoff must revalidate.
(defun physical-backup-lease-capture-remote (directory channel request &key directory-fd)
  (let ((request (physical-backup-lease-request-copy request))
        (target (physical-bootstrap-target directory)))
    (if directory-fd
        (progn
          (physical-image-check-target target directory-fd)
          (call-with-physical-backup-receive-directory
           directory-fd (lambda (anchored) (physical-backup-lease-receive-remote anchored channel request))))
        (physical-backup-lease-receive-remote target channel request))))

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

(defun pg-client-native-backup-lease-options (request)
  (let ((request (physical-backup-lease-request-copy request)))
    (list (cons "native_format" +physical-native-format+)
          (cons "lease_name" (getf request :name))
          (cons "lease_token" (getf request :token))
          (cons "cluster_identity" (getf request :cluster-identity))
          (cons "system_id" (format nil "~d" (getf request :system-id)))
          (cons "source_timeline" (format nil "~d" (getf request :timeline))))))

(defun pg-client-native-backup-lease-option-query (verb options)
  (with-output-to-string (output)
    (format output "~a (" verb)
    (loop for (name . value) in options for first-p = t then nil
          do (unless first-p (write-string ", " output))
             (format output "~a ~a" name (pg-client-native-backup-lease-literal value)))
    (write-char #\) output)))

(defun pg-client-native-backup-lease-follow-query (verb request generation name coordinates)
  (unless (member verb '("BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW") :test #'equal)
    (error "invalid native backup lease allocation command"))
  (let* ((row (physical-backup-lease-follow-intent request generation name coordinates :live))
         (request (getf row :request))
         (coordinates (getf row :coordinates)))
    (pg-client-native-backup-lease-option-query
     verb
     (append
      (pg-client-native-backup-lease-options request)
      (list (cons "allocation_generation" (format nil "~d" (getf row :generation)))
            (cons "allocation_name" (getf row :name)))
      (loop for (field key kind) in *physical-follow-wire-fields*
            for value = (getf coordinates key)
            collect (cons (concatenate 'string "follow_" field)
                          (if (eq kind :lsn) (wal-format-lsn value) (format nil "~d" value))))
      (when (eq :retained-timeline (physical-follow-request-kind coordinates))
        (list (cons "follow_proof_kind" "retained_timeline")))))))

(defun pg-client-native-backup-lease-query (verb request &optional successor)
  (unless (member verb '("CAPTURE_BACKUP_NATIVE" "BACKUP_LEASE_STATUS" "BACKUP_LEASE_ABORT"
                         "BACKUP_LEASE_HANDOFF" "BACKUP_LEASE_CLEANUP_SELECTED") :test #'equal)
    (error "invalid native backup lease query command"))
  (let* ((request (physical-backup-lease-request-copy request))
         (successor (if (member verb '("BACKUP_LEASE_HANDOFF" "BACKUP_LEASE_CLEANUP_SELECTED") :test #'equal)
                        (physical-backup-lease-successor-copy successor request)
                        (when successor (error "unexpected backup lease successor")))))
    (pg-client-native-backup-lease-option-query
     verb
     (append (pg-client-native-backup-lease-options request)
             (when successor
               (list (cons "successor_name" (getf successor :name))
                     (cons "successor_anchor_digest"
                           (format nil "~d" (getf successor :anchor-digest)))))))))

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

;; Freeze the complete intent before I/O and validate the returned owner and
;; anchor against it.  The proof frame has the same bound as ordinary follow
;; preparation.  These primitives do not retry, journal, or activate a receiver.

(defun pg-client-native-backup-lease-follow-prepare (client request generation name coordinates)
  (let* ((intent (physical-backup-lease-follow-intent request generation name coordinates :live))
         (request (getf intent :request))
         (generation (getf intent :generation))
         (name (getf intent :name))
         (coordinates (getf intent :coordinates))
         (row (physical-client-query
               client (pg-client-native-backup-lease-follow-query
                       "BACKUP_LEASE_PREPARE_FOLLOW" request generation name coordinates)
               (physical-backup-lease-follow-prepare-schema coordinates) "BACKUP_LEASE_PREPARE_FOLLOW"
               :maximum +physical-client-proof-frame-maximum+)))
    (unless (and (equal (getf request :name) (aref row 0))
                 (= generation (pg-client-native-u64 (aref row 1))))
      (pg-client-native-reject "native allocation preparation returned a different owner"))
    (pg-client-native-prepare-anchor (subseq row 2) name coordinates)))

;; Check the bounded reply against the frozen intent and return its parent.
;; The echoed generation and name acknowledge the request, not a child state.

(defun pg-client-native-backup-lease-follow-cancel (client request generation name coordinates)
  (let* ((intent (physical-backup-lease-follow-intent request generation name coordinates :live))
         (request (getf intent :request))
         (generation (getf intent :generation))
         (name (getf intent :name))
         (coordinates (getf intent :coordinates))
         (row (physical-client-query
               client (pg-client-native-backup-lease-follow-query
                       "BACKUP_LEASE_CANCEL_FOLLOW" request generation name coordinates)
               *physical-backup-lease-follow-cancel-schema* "BACKUP_LEASE_CANCEL_FOLLOW"
               :maximum 4096))
         (parent (pg-client-native-backup-lease-record (subseq row 0 8) request)))
    (unless (and parent (= generation (pg-client-native-u64 (aref row 8)))
                 (equal name (aref row 9)))
      (pg-client-native-reject "native allocation cancellation returned a different owner"))
    parent))

;; Copy the request and successor before I/O so the reply is checked against
;; the same identity sent in the query. HANDOFF accepts only CLOSED with the
;; exact successor. The caller must settle an uncertain result after reconnecting
;; to the original frozen route.
(defun pg-client-native-backup-lease-command (client verb request &optional successor)
  (unless (member verb '("BACKUP_LEASE_STATUS" "BACKUP_LEASE_ABORT" "BACKUP_LEASE_HANDOFF"
                         "BACKUP_LEASE_CLEANUP_SELECTED") :test #'equal)
    (error "native backup lease command requires a status result"))
  (let* ((request (physical-backup-lease-request-copy request))
         (successor (when successor (physical-backup-lease-successor-copy successor request)))
         (query (pg-client-native-backup-lease-query verb request successor))
         (row (physical-client-query client query *physical-backup-lease-result-schema* verb :maximum 4096))
         (record (pg-client-native-backup-lease-record row request)))
    (when (and (equal verb "BACKUP_LEASE_ABORT")
               (not (and (member (getf record :phase) '(:closing :closed))
                         (eq :aborted (getf record :outcome)))))
      (pg-client-native-reject "native backup lease abort did not confirm cancellation"))
    (when (and (member verb '("BACKUP_LEASE_HANDOFF" "BACKUP_LEASE_CLEANUP_SELECTED") :test #'equal)
               (not (and (eq :closed (getf record :phase))
                         (eq :handed-off (getf record :outcome))
                         (equal successor (getf record :successor)))))
      (pg-client-native-reject "native backup lease handoff did not confirm the successor"))
    record))

(defun pg-client-native-backup-lease-status (client request)
  (pg-client-native-backup-lease-command client "BACKUP_LEASE_STATUS" request))

(defun pg-client-native-backup-lease-abort (client request)
  (pg-client-native-backup-lease-command client "BACKUP_LEASE_ABORT" request))

(defun pg-client-native-backup-lease-handoff (client request successor)
  (pg-client-native-backup-lease-command client "BACKUP_LEASE_HANDOFF" request successor))

(defun pg-client-native-backup-lease-cleanup-selected (client request successor)
  (pg-client-native-backup-lease-command client "BACKUP_LEASE_CLEANUP_SELECTED" request successor))

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

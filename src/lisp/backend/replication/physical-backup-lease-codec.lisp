(in-package #:pg)

;;; Immutable request and record format for source-side physical backup leases.
;;; This layer records intent and state; READY and successor fields do not prove
;;; that a source pin, backup, or handoff exists.  Live operations must establish
;;; those facts separately.
;;;
;;; Requests bind a reseed_ name and token to a UTF-8 cluster identity, its FNV
;;; system identifier, and a timeline.  Copies validate and detach mutable input.
;;; The bounded, checksummed image has one canonical encoding; decoding rejects
;;; malformed or alternate encodings rather than accepting ambiguous metadata.
(defconstant +physical-backup-lease-maximum-bytes+ 8192)

(defun physical-backup-lease-hex-p (value length)
  (and (stringp value) (= length (length value))
       (every (lambda (character) (find character "0123456789abcdef")) value)))

(defun physical-backup-lease-name-p (name)
  (and (stringp name) (= 39 (length name)) (uiop:string-prefix-p "reseed_" name)
       (physical-backup-lease-hex-p (subseq name 7) 32)))

(defun physical-backup-lease-request-copy (request)
  (physical-follow-check-plist request '(:version :name :token :system-id :cluster-identity :timeline))
  (let ((name (getf request :name))
        (token (getf request :token))
        (identity (getf request :cluster-identity)))
    (unless (and (eql 1 (getf request :version))
                 (physical-backup-lease-name-p name)
                 (physical-backup-lease-hex-p token 64)
                 (stringp identity) (<= 1 (length identity) 1024)
                 (typep (getf request :system-id) '(unsigned-byte 64))
                 (typep (getf request :timeline) '(integer 1 4294967295)))
      (error "invalid physical backup lease request"))
    (let ((bytes (string->bytes identity)))
      (unless (and (<= (length bytes) 1024)
                   (= (getf request :system-id)
                      (physical-store-update-digest #xCBF29CE484222325 bytes)))
        (error "invalid physical backup lease cluster identity")))
    (list :version 1 :name (copy-seq name) :token (copy-seq token)
          :system-id (getf request :system-id) :cluster-identity (copy-seq identity)
          :timeline (getf request :timeline))))

(defun physical-backup-lease-base-copy (base)
  (physical-follow-check-plist base '(:checkpoint-start :base :manifest-digest))
  (unless (and (every (lambda (key) (typep (getf base key) '(unsigned-byte 64)))
                      '(:checkpoint-start :base :manifest-digest))
               (wal-checkpoint-record-length-p (- (getf base :base) (getf base :checkpoint-start))))
    (error "invalid physical backup lease base"))
  (list :checkpoint-start (getf base :checkpoint-start) :base (getf base :base)
        :manifest-digest (getf base :manifest-digest)))

(defun physical-backup-lease-successor-copy (successor request)
  (physical-follow-check-plist successor '(:name :anchor-digest))
  (unless (and (wal-valid-replication-slot-name-p (getf successor :name))
               (not (equal (getf successor :name) (getf request :name)))
               (typep (getf successor :anchor-digest) '(unsigned-byte 64)))
    (error "invalid physical backup lease successor"))
  (list :name (copy-seq (getf successor :name)) :anchor-digest (getf successor :anchor-digest)))

(defun physical-backup-lease-copy (record)
  (let ((keys '(:version :request :phase :base :outcome :successor)))
    (when (get-properties record '(:rewind-seal-digest))
      (setf keys (cons :rewind-seal-digest keys)))
    (physical-follow-check-plist record keys))
  (let* ((request (physical-backup-lease-request-copy (getf record :request)))
         (phase (getf record :phase))
         (base (when (getf record :base) (physical-backup-lease-base-copy (getf record :base))))
         (seal-digest (getf record :rewind-seal-digest))
         (outcome (getf record :outcome))
         (successor (when (getf record :successor)
                      (physical-backup-lease-successor-copy (getf record :successor) request))))
    (unless (and (eql 1 (getf record :version))
                 (member phase '(:requested :capturing :ready :interrupted :closing :closed))
                 (case phase ((:requested :capturing) (null base)) (:ready base) (t t))
                 (or (not (get-properties record '(:rewind-seal-digest)))
                     (and base (typep seal-digest '(unsigned-byte 64))))
                 (if (member phase '(:closing :closed))
                     (case outcome
                       (:aborted (null successor))
                       (:handed-off (and base successor))
                       (t nil))
                     (and (null outcome) (null successor))))
      (error "incoherent physical backup lease state"))
    (append (list :version 1 :request request :phase phase :base base :outcome outcome :successor successor)
            (when seal-digest (list :rewind-seal-digest seal-digest)))))

;; The base restriction applies to transitions from an existing record; an initial
;; aborted closing record may also carry a structurally valid base.

;; The request never changes.  Only capturing -> ready can introduce a base;
;; interrupted records cannot resume capture.  Closing freezes its outcome and
;; successor, and closed records remain tombstones rather than being evicted.
;; An initial aborted closing record is permitted, including an abort before capture.
(defun physical-backup-lease-transition (previous candidate)
  ;; A completed base survives interruption and remains bound to any handoff.
  (let* ((before (when previous (physical-backup-lease-copy previous)))
         (after (physical-backup-lease-copy candidate))
         (from (getf before :phase))
         (to (getf after :phase)))
    (unless
        (if (null before)
            (or (eq to :requested)
                (and (eq to :closing) (eq :aborted (getf after :outcome))
                     (null (getf after :rewind-seal-digest))))
            (and (equal (getf before :request) (getf after :request))
                 (if (eq from to)
                     (equal before after)
                     (and (member to (case from
                                       (:requested '(:capturing :interrupted :closing))
                                       (:capturing '(:ready :interrupted :closing))
                                       (:ready '(:interrupted :closing))
                                       (:interrupted '(:closing))
                                       (:closing '(:closed))))
                          (or (and (eq from :capturing) (eq to :ready))
                              (and (equal (getf before :base) (getf after :base))
                                   (eql (getf before :rewind-seal-digest)
                                        (getf after :rewind-seal-digest))))
                          (if (eq from :closing)
                              (and (eq (getf before :outcome) (getf after :outcome))
                                   (equal (getf before :successor) (getf after :successor)))
                              (or (not (eq to :closing))
                                  (eq :aborted (getf after :outcome))
                                  (and (member from '(:ready :interrupted))
                                       (getf before :base))))))))
      (error "physical backup lease cannot rebind or repeat capture"))
    after))

;; Lease and allocation files use a bounded canonical literal with a magic
;; header and checksum so recovery can reject damaged or ambiguous records.
(defun physical-backup-lease-encode-literal (magic value maximum)
  (metadata-literal-envelope-image magic value maximum "physical backup lease"))

;; Normalize the parsed value and re-encode it to require the canonical bytes.
(defun physical-backup-lease-decode-literal (magic bytes maximum normalize)
  (metadata-literal-envelope-decode magic bytes maximum normalize "physical backup lease"))

(defun physical-backup-lease-image (record)
  (physical-backup-lease-encode-literal
   "PGBACKUPLEASE1" (physical-backup-lease-copy record) +physical-backup-lease-maximum-bytes+))

(defun physical-backup-lease-decode (bytes)
  (physical-backup-lease-decode-literal
   "PGBACKUPLEASE1" bytes +physical-backup-lease-maximum-bytes+ #'physical-backup-lease-copy))

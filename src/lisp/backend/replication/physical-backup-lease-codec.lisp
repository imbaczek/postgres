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
  (physical-follow-check-plist record '(:version :request :phase :base :outcome :successor))
  (let* ((request (physical-backup-lease-request-copy (getf record :request)))
         (phase (getf record :phase))
         (base (when (getf record :base) (physical-backup-lease-base-copy (getf record :base))))
         (outcome (getf record :outcome))
         (successor (when (getf record :successor)
                      (physical-backup-lease-successor-copy (getf record :successor) request))))
    (unless (and (eql 1 (getf record :version))
                 (member phase '(:requested :capturing :ready :interrupted :closing :closed))
                 (case phase ((:requested :capturing) (null base)) (:ready base) (t t))
                 (if (member phase '(:closing :closed))
                     (case outcome
                       (:aborted (null successor))
                       (:handed-off (and base successor))
                       (t nil))
                     (and (null outcome) (null successor))))
      (error "incoherent physical backup lease state"))
    (list :version 1 :request request :phase phase :base base :outcome outcome :successor successor)))

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
                (and (eq to :closing) (eq :aborted (getf after :outcome))))
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
                              (equal (getf before :base) (getf after :base)))
                          (if (eq from :closing)
                              (and (eq (getf before :outcome) (getf after :outcome))
                                   (equal (getf before :successor) (getf after :successor)))
                              (or (not (eq to :closing))
                                  (eq :aborted (getf after :outcome))
                                  (and (member from '(:ready :interrupted))
                                       (getf before :base))))))))
      (error "physical backup lease cannot rebind or repeat capture"))
    after))

(defun physical-backup-lease-image (record)
  (let* ((text (physical-bootstrap-manifest-text (physical-backup-lease-copy record)))
         (bytes (string->bytes text))
         (image (string->bytes (format nil "PGBACKUPLEASE1 ~8,'0X~%~a~%" (wal-crc32 bytes) text))))
    (unless (<= (length image) +physical-backup-lease-maximum-bytes+)
      (error "physical backup lease exceeds its record bound"))
    image))

(defun physical-backup-lease-decode (bytes)
  (unless (typep bytes '(vector (unsigned-byte 8)))
    (error "physical backup lease requires an octet vector"))
  (unless (<= (length bytes) +physical-backup-lease-maximum-bytes+)
    (error "physical backup lease exceeds its record bound"))
  (let* ((text (bytes->string bytes))
         (newline (position #\Newline text))
         (prefix "PGBACKUPLEASE1 "))
    (unless (and newline (= newline (+ (length prefix) 8)) (uiop:string-prefix-p prefix text))
      (error "invalid physical backup lease envelope"))
    (let* ((body (subseq text (1+ newline)))
           (crc (parse-integer text :start (length prefix) :end newline :radix 16)))
      (unless (and (plusp (length body)) (char= #\Newline (char body (1- (length body))))
                   (= crc (wal-crc32 (string->bytes (subseq body 0 (1- (length body)))))))
        (error "physical backup lease checksum mismatch"))
      (let ((record (physical-backup-lease-copy (recovery-parse-manifest-text body))))
        (unless (equalp bytes (physical-backup-lease-image record))
          (error "noncanonical physical backup lease record"))
        record))))

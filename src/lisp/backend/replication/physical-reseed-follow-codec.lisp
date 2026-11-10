(in-package #:pg)

(defconstant +physical-reseed-follow-maximum-bytes+ (* 64 1024 1024))
(defconstant +physical-reseed-follow-proof-reserve+ 8192)

(defconstant +physical-reseed-follow-configuration-reserve+ 8192)

;;; Bounded structural codec for a private target follow record.  The original
;;; attempt includes credentials and must not be printed.  This module neither
;;; publishes a journal nor grants ownership, performs RPC, or activates follow.

;; The target image is bounded separately from the larger install predecessor.
;; This reference describes the predecessor; an owner must verify the actual
;; install journal and its terminal state before relying on it.

(defun physical-reseed-follow-install-copy (value local stage)
  (physical-follow-check-plist value '(:identity :digest :body-crc :step))
  (let ((identity (getf value :identity)))
    (unless (and (listp identity) (= 8 (length identity))
                 (every (lambda (item) (typep item '(unsigned-byte 64))) (subseq identity 0 6))
                 (every (lambda (item) (typep item '(signed-byte 64))) (subseq identity 6))
                 (= (first identity) (getf local :device))
                 (plusp (second identity)) (= (third identity) #o100600) (= (fourth identity) 1)
                 (<= 1 (sixth identity) +physical-reseed-install-maximum-bytes+)
                 (not (member (second identity)
                              (list (getf local :inode) (getf local :lock-inode) (getf stage :inode))))
                 (typep (getf value :digest) '(unsigned-byte 64))
                 (typep (getf value :body-crc) '(unsigned-byte 32))
                 (typep (getf value :step) '(integer 1 100012)))
      (error "invalid target follow predecessor reference"))
    (list :identity (copy-list identity) :digest (getf value :digest)
          :body-crc (getf value :body-crc) :step (getf value :step))))

;; Keep bounded exact control and authentication evidence tied to the request.
;; These structural checks do not prove current disk contents or authentication
;; policy.  The phase-zero control's manifest digest is intentionally zero;
;; its exhausted next XID is the actual value, not an allocation reservation.

(defun physical-reseed-follow-base-copy (value request)
  (physical-follow-check-plist
   value '(:manifest-digest :checkpoint-start :recovery-control :wal-control :auth-files))
  (let* ((recovery (physical-libpq-datum-octets (getf value :recovery-control) +physical-recovery-control-maximum-bytes+))
         (wal (physical-libpq-datum-octets (getf value :wal-control) (1+ +wal-control-maximum-bytes+)))
         (control (physical-recovery-decode-control recovery))
         (auth (physical-adoption-attempt-auth-copy (getf value :auth-files))))
    (unless (and (typep (getf value :manifest-digest) '(unsigned-byte 64))
                 (typep (getf value :checkpoint-start) '(unsigned-byte 64))
                 (zerop (getf control :phase))
                 (= (getf control :system-id) (getf request :system-id))
                 (= (getf control :timeline) (getf request :timeline)))
      (error "invalid target follow base identity"))
    (multiple-value-bind (checkpoint durable snapshot timeline checkpoint-timeline namespace metadata)
        (wal-decode-control-line (bytes->string wal))
      (declare (ignore namespace))
      (unless (and (= checkpoint durable (getf control :checkpoint) (getf control :applied))
                   (= timeline checkpoint-timeline (getf request :timeline))
                   (= (getf value :checkpoint-start)
                      (if snapshot (getf snapshot :start) (- checkpoint +wal-header-size+)))
                   (or (null metadata) (= (getf metadata :system-id) (getf request :system-id))))
        (error "target follow control images disagree")))
    (dolist (entry auth)
      (unless (<= (second entry) (second (assoc (first entry) (physical-backup-metadata-bounds) :test #'equal)))
        (error "target follow authentication commitment exceeds its file bound")))
    (values
     (list :manifest-digest (getf value :manifest-digest) :checkpoint-start (getf value :checkpoint-start)
           :recovery-control (physical-libpq-octets-datum recovery)
           :wal-control (physical-libpq-octets-datum wal) :auth-files auth)
     control)))

;; Generations are contiguous from 1, names are unique, and every older entry
;; is canceled.  Coordinates bind each entry to the same base and receive point.

(defun physical-reseed-follow-generations-copy (entries request base control)
  (unless (and (listp entries) (<= (length entries) +physical-backup-lease-allocation-maximum-generation+))
    (error "target follow history exceeds its generation bound"))
  (let ((names nil))
    (loop for value in entries for generation from 1
          for row = (physical-reseed-follow-generation-copy value request)
          for coordinates = (getf row :coordinates)
          unless (and (= generation (getf row :generation))
                      (not (member (getf row :name) names :test #'equal))
                      (or (= generation (length entries)) (eq :canceled (getf row :phase)))
                      (= (getf coordinates :base) (getf coordinates :receive-lsn) (getf control :applied))
                      (= (getf coordinates :receive-timeline) (getf request :timeline))
                      (= (getf coordinates :manifest-digest) (getf base :manifest-digest))
                      (= (getf coordinates :receive-digest) #xCBF29CE484222325))
            do (error "target follow generation differs from its immutable base or history")
          do (push (getf row :name) names)
          collect row)))

;; Validate and copy the immutable attempt, install and phase-zero base evidence
;; with its generation history.  Structural validity does not prove disk state.
(defun physical-reseed-follow-core-copy (record)
  (physical-reseed-install-check-literal record)
  (physical-follow-check-plist record '(:version :attempt :install :base :generations))
  (let* ((attempt (physical-reseed-attempt-copy (getf record :attempt)))
         (stage (getf attempt :stage)) (request (getf attempt :request)))
    (unless (and (eql 1 (getf record :version)) (eq :premanifest (getf attempt :phase)) stage)
      (error "target follow journal requires its original installed attempt"))
    (let ((installation (physical-reseed-follow-install-copy
                         (getf record :install) (getf attempt :local) stage)))
      (multiple-value-bind (base control) (physical-reseed-follow-base-copy (getf record :base) request)
        (list :version 1 :attempt attempt :install installation :base base
              :generations (physical-reseed-follow-generations-copy
                            (getf record :generations) request base control))))))

;; Extract the core keys for validation without the optional configuration group.
(defun physical-reseed-follow-core-projection (record)
  (loop for key in '(:version :attempt :install :base :generations)
        append (list key (getf record key))))

;; Return a detached checked record.  Optional configuration and settlement
;; groups are checked against their predecessors.  Missing keys differ from NIL
;; groups; records without settlement keep their original canonical encoding.
(defun physical-reseed-follow-copy (record)
  ;; Private copies reuse this call's validated predecessors.  Missing groups
  ;; use public copies to validate the full required prefix.  Validation here
  ;; grants no filesystem authority and creates no persistent trust or cache.
  ;; Check optional groups in predecessor order, ending with cancellation.
  ;; Each receives the canonical core and any already checked groups, so a
  ;; cancellation receipt is checked against its frozen predecessors.
  ;; Presence of a key is distinct from NIL.
  (physical-reseed-install-check-literal record)
  (let* ((configuration-p (loop for key in record by #'cddr thereis (eq key :configuration)))
         (settlement-p (loop for key in record by #'cddr thereis (eq key :settlement)))
         (activation-p (loop for key in record by #'cddr thereis (eq key :activation)))
         (retirement-p (loop for key in record by #'cddr thereis (eq key :retirement)))
         (cancellation-p (loop for key in record by #'cddr thereis (eq key :cancellation)))
         (keys '(:version :attempt :install :base :generations)))
    (physical-follow-check-plist
     record (append keys (when configuration-p '(:configuration))
                    (when settlement-p '(:settlement)) (when activation-p '(:activation))
                    (when retirement-p '(:retirement)) (when cancellation-p '(:cancellation))))
    (let ((core (physical-reseed-follow-core-copy (physical-reseed-follow-core-projection record))))
      (when configuration-p
        (setf core (append core (list :configuration
                                      (%physical-reseed-follow-configuration-copy
                                       (getf record :configuration) core)))))
      (when settlement-p
        (setf core (append core (list :settlement
                                      (funcall (if configuration-p
                                                   #'%physical-reseed-follow-settlement-copy
                                                   #'physical-reseed-follow-settlement-copy)
                                               (getf record :settlement) core)))))
      (when activation-p
        (setf core (append core (list :activation
                                      (funcall (if (and configuration-p settlement-p)
                                                   #'%physical-reseed-follow-activation-copy
                                                   #'physical-reseed-follow-activation-copy)
                                               (getf record :activation) core)))))
      (when retirement-p
        (setf core (append core (list :retirement
                                      (funcall (if (and configuration-p settlement-p activation-p)
                                                   #'%physical-reseed-follow-retirement-copy
                                                   #'physical-reseed-follow-retirement-copy)
                                               (getf record :retirement) core)))))
      (when cancellation-p
        (setf core (append core (list :cancellation
                                      (physical-reseed-follow-cancellation-copy
                                       (getf record :cancellation) core)))))
      core)))

;; The original attempt, install reference and base remain immutable.  Append
;; only a preparing generation after canceled history, or advance the last
;; generation without changing older entries.

(defun physical-reseed-follow-check-transition (previous candidate)
  (let* ((before (physical-reseed-follow-copy previous))
         (after (physical-reseed-follow-copy candidate))
         (old (getf before :generations)) (new (getf after :generations))
         (count (length old)) (request (getf (getf before :attempt) :request)))
    (unless (every (lambda (key) (equal (getf before key) (getf after key)))
                   '(:version :attempt :install :base))
      (error "target follow journal cannot rebind its predecessors"))
    (when (or (getf before :cancellation) (getf after :cancellation))
      (physical-reseed-follow-cancellation-check-transition before after))
    (when (or (getf before :retirement) (getf after :retirement))
      (physical-reseed-follow-retirement-check-transition before after))
    (when (or (getf before :activation) (getf after :activation))
      (physical-reseed-follow-activation-check-transition before after))
    (when (or (getf before :settlement) (getf after :settlement))
      (physical-reseed-follow-settlement-check-transition before after))
    (when (or (getf before :configuration) (getf after :configuration))
      (unless (equal old new) (error "configuration intent freezes its generation history"))
      (physical-reseed-follow-configuration-check-transition
       (getf before :configuration) (getf after :configuration)
       (physical-reseed-follow-core-projection after)))
    (cond
      ((= (length new) (1+ count))
       (unless (and (equal old (butlast new))
                    (or (null old) (eq :canceled (getf (car (last old)) :phase)))
                    (eq :preparing (getf (car (last new)) :phase)))
         (error "target follow append requires canceled history and durable preparation")))
      ((= count (length new))
       (unless (equal (butlast old) (butlast new)) (error "target follow history is immutable"))
       (when old
         (physical-reseed-follow-generation-check-transition
          (car (last old)) (car (last new)) request)))
      (t (error "target follow generation history cannot be shortened or skipped")))
    t))

;; Check the bounded image and the reserve for remaining forward work.
;; Pending settlement without cancellation also needs room for the larger
;; terminal receipt.  Cancellation cleanup uses its separate capacity preflight.
(defun physical-reseed-follow-image (record)
  (let* ((record (physical-reseed-follow-copy record))
         (bytes (physical-backup-lease-encode-literal
                 "PGRESEEDFOLLOW1" record +physical-reseed-follow-maximum-bytes+)))
    (physical-reseed-follow-check-image-capacity
     record (length bytes) +physical-reseed-follow-maximum-bytes+)
    bytes))

(defun physical-reseed-follow-decode (bytes)
  (let ((record (physical-backup-lease-decode-literal
                 "PGRESEEDFOLLOW1" bytes +physical-reseed-follow-maximum-bytes+
                 #'physical-reseed-follow-copy)))
    (physical-reseed-follow-image record)
    record))

(in-package #:pg)

;;; Enforce ownership of private source pins reserved by backup leases.  The
;;; durable lease inventory is authoritative even after a lease is closed, so
;;; ordinary pin operations must not reuse those names.  These checks also
;;; protect the origin receipt until the lease reaches CLOSED.
;;;
;;; This module supplies the ownership checks and pin creation primitive; the
;;; capture owner is managed separately from any backup command interface.

;; The caller holds WAL exclusion while using the record or making a decision
;; based on it.  Publication also requires write exclusion.
(defun physical-backup-lease-record-under-lock (name)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "backup lease lookup requires WAL exclusion"))
  (gethash name *physical-backup-lease-records*))

;; Index every recorded successor, including closed leases, so an assignment
;; cannot be reused by another lease. The cache belongs to this registry object
;; and is rebuilt when the registry changes or publication invalidates it.
(defun physical-backup-lease-successor-records ()
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "backup lease successor lookup requires WAL exclusion"))
  (unless (eq *physical-backup-lease-records*
              (car *physical-backup-lease-successor-cache*))
    (let ((owners (make-hash-table :test #'equal)))
      (maphash
       (lambda (name record)
         (declare (ignore name))
         (let ((successor (getf (getf record :successor) :name)))
           (when successor
             (when (gethash successor owners)
               (wal-invalid-input "backup leases share a historical successor"))
             (setf (gethash successor owners) record))))
       *physical-backup-lease-records*)
      (setf *physical-backup-lease-successor-cache*
            (cons *physical-backup-lease-records* owners))))
  (cdr *physical-backup-lease-successor-cache*))

;; A historical assignment remains reserved, while CLOSING also protects the
;; successor pin from mutation until CLOSED has been published.
(defun physical-backup-lease-closing-successor (name)
  (let ((record (gethash name (physical-backup-lease-successor-records))))
    (when (eq :closing (getf record :phase)) record)))

(defun physical-backup-lease-check-successor-mutation (name)
  (when (physical-backup-lease-closing-successor name)
    (wal-invalid-input "backup lease handoff still owns successor ~a" name)))

(defun physical-backup-lease-check-successor-create (name)
  (when (gethash name (physical-backup-lease-successor-records))
    (wal-invalid-input "backup lease historical successor permanently reserves this name")))

(defun physical-backup-lease-check-ordinary-pin (name)
  (physical-backup-lease-check-successor-mutation name)
  (when (physical-backup-lease-record-under-lock name)
    (wal-invalid-input "backup lease owns private source pin ~a" name)))

(declaim (ftype function physical-backup-lease-check-capture-owner))

(declaim (ftype function physical-backup-lease-allocation-check-unowned-name))

;; A lease may create its pin only for the exact CAPTURING request and owner,
;; while the caller owns quiescence and the matching record is still on disk.
;; Ordinary creation may not take any name in the lease inventory.
(defun physical-backup-lease-check-pin-create (name request)
  (physical-backup-lease-allocation-check-unowned-name name)
  (physical-backup-lease-check-successor-create name)
  (let ((record (physical-backup-lease-record-under-lock name)))
    (cond
      (request
       (physical-backup-lease-require-locks)
       (unless (and record (eq :capturing (getf record :phase))
                    (equal request (getf record :request))
                    (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
         (wal-invalid-input "backup lease creation lacks its capturing owner"))
       (unless (eq record (physical-backup-lease-check-capture-owner
                           *physical-backup-lease-capture-owner*))
         (wal-invalid-input "backup lease pin has a different capture owner")))
      (record (wal-invalid-input "backup lease reserves private source pin ~a" name)))))

(defun physical-backup-lease-create-pin (request)
  (let ((request (physical-backup-lease-request-copy request)))
    (unless (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
      (error "backup lease pin creation requires operation quiescence"))
    (physical-source-pin-create (getf request :name) request)))

(defun physical-backup-lease-pin-directory-stat ()
  (let ((stat (physical-backup-lease-lstat (physical-source-pin-directory))))
    (when stat
      (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                   (= (sb-posix:geteuid) (sb-posix:stat-uid stat)))
        (error "backup lease pin directory is not an owned ordinary directory")))
    stat))

(defun physical-backup-lease-check-pin-record (pin record)
  (let ((request (getf record :request)) (base (getf record :base)))
    (unless (and (physical-source-pin-p pin) (not (physical-follow-pin-p pin))
                 (equal (physical-source-pin-name pin) (getf request :name))
                 (= (physical-source-pin-system-id pin) (getf request :system-id))
                 (= (physical-source-pin-timeline pin) (getf request :timeline))
                 (null (physical-source-pin-confirmed-lsn pin))
                 (= #xCBF29CE484222325 (physical-source-pin-prefix-digest pin))
                 (or (null base)
                     (and (= (getf base :base) (physical-source-pin-consistent-lsn pin))
                          (= (getf base :checkpoint-start) (physical-source-pin-restart-lsn pin))
                          (eql (getf base :manifest-digest) (physical-source-pin-manifest-digest pin)))))
      (error "backup lease pin differs from its recorded ownership")))
  pin)

;; Recovery may retire a disk-only pin; a primary requires the registered object.

;; Retirement requires the matching durable CLOSING intent and no sender.
;; Check both the registered generation and the disk image before unlinking.
(defun physical-backup-lease-check-pin-unlink (pin request)
  (physical-backup-lease-require-locks)
  (physical-backup-lease-check-successor-mutation (physical-source-pin-name pin))
  (let* ((name (physical-source-pin-name pin))
         (record (physical-backup-lease-record-under-lock name)))
    (cond
      (record
       (unless (and request (eq :closing (getf record :phase))
                    (equal request (getf record :request))
                    (null (gethash name *physical-source-senders*))
                    (null (gethash name *physical-backup-lease-capture-owners*)))
         (wal-invalid-input "backup lease retirement lacks its closing owner"))
       (physical-backup-lease-pin-directory-stat)
       (physical-backup-lease-check-pin-record pin record)
       (let ((registered (gethash name *physical-source-pins*)))
         (unless (or (eq registered pin)
                     (and (null registered) (recovery-in-progress-p)))
           (wal-invalid-input "backup lease pin generation changed")))
       (unless (equal record (physical-backup-lease-read (physical-backup-lease-path request)))
         (error "backup lease closing intent changed on disk"))
       (unless (equalp (physical-source-pin-encode pin)
                       (physical-source-pin-encode (physical-source-pin-read-retirement name)))
         (error "backup lease retirement pin changed on disk")))
      (request (wal-invalid-input "backup lease retirement has no recorded owner"))))
  pin)

;; A receipt remains owned through interruption and closing; only CLOSED
;; releases it.  The closed lease name itself remains reserved.
(defun physical-backup-lease-origin-in-use-p (identity)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "backup lease origin lookup requires WAL exclusion"))
  (loop for record being the hash-values of *physical-backup-lease-records*
        for request = (getf record :request)
        for base = (getf record :base)
          thereis (and base (not (eq :closed (getf record :phase)))
                       (equal identity (list (getf request :system-id) (getf request :timeline)
                                             (getf base :base) (getf base :manifest-digest))))))

;; Immutable successor identity remains meaningful after CLOSED, when the pin
;; may have advanced.  CLOSING also requires the stricter unused-pin check below.

(defun physical-backup-lease-successor-pin-identity-p (record pin)
  (let* ((request (getf record :request))
         (base (getf record :base))
         (successor (getf record :successor))
         (anchor (and (physical-follow-pin-p pin) (physical-follow-pin-anchor pin))))
    (and base successor anchor (physical-follow-pin-ready-p pin)
         (equal (getf successor :name) (physical-source-pin-name pin))
         (= (getf successor :anchor-digest) (physical-follow-anchor-digest anchor))
         (= (getf request :system-id) (getf anchor :system-id))
         (= (getf request :timeline) (getf anchor :base-timeline))
         (= (getf base :base) (getf anchor :base))
         (= (getf base :manifest-digest) (getf anchor :manifest-digest)))))

;; Require the exact unused READY follow pin for this backup's original system,
;; timeline, base and manifest. Its saved anchor digest and receive position
;; bind the successor to the handoff before the old retention can be released.
(defun physical-backup-lease-check-successor-pin (record pin)
  (let* ((successor (getf record :successor))
         (anchor (and (physical-follow-pin-p pin) (physical-follow-pin-anchor pin))))
    (unless (and (physical-backup-lease-successor-pin-identity-p record pin)
                 (not (physical-source-pin-lost-p pin))
                 (eql (physical-source-pin-confirmed-lsn pin) (getf anchor :receive-lsn))
                 (eql (physical-source-pin-restart-lsn pin) (getf anchor :receive-lsn))
                 (null (gethash (getf successor :name) *physical-source-senders*)))
      (wal-invalid-input "backup lease handoff lacks its unused ready successor"))
    (physical-follow-pin-check-fields pin)
    pin))

(declaim (ftype function physical-backup-lease-check-successor-assignment))

(defun physical-backup-lease-check-successor-inventory ()
  (physical-backup-lease-require-locks)
  (physical-backup-lease-successor-records)
  (values))

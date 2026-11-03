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

(defun physical-backup-lease-check-ordinary-pin (name)
  (when (physical-backup-lease-record-under-lock name)
    (wal-invalid-input "backup lease owns private source pin ~a" name)))

(declaim (ftype function physical-backup-lease-check-capture-owner))

;; A lease may create its pin only for the exact CAPTURING request and owner,
;; while the caller owns quiescence and the matching record is still on disk.
;; Ordinary creation may not take any name in the lease inventory.
(defun physical-backup-lease-check-pin-create (name request)
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

;; Decode and match ownership before deciding whether WAL must be checked.
;; An already lost pin needs no old WAL.  Standby, CLOSING, and a changed
;; timeline mark a pin lost for the caller to persist before publishing it.
(defun physical-backup-lease-startup-pin (path &optional standby)
  (physical-backup-lease-require-locks)
  (let* ((name (pathname-name path))
         (record (physical-backup-lease-record-under-lock name)))
    (unless record (return-from physical-backup-lease-startup-pin (values nil nil)))
    (physical-backup-lease-pin-directory-stat)
    (let ((pin (physical-source-bootstrap-pin-decode
                path (read-bounded-regular-file path (physical-follow-pin-image-limit) :stabilize t))))
      (when (eq :closed (getf record :phase))
        (error "closed backup lease still has a source pin"))
      (physical-backup-lease-check-pin-record pin record)
      (cond
        ((physical-source-pin-lost-p pin) (values pin nil))
        ((or standby (eq :closing (getf record :phase))
             (/= (physical-source-pin-timeline pin) *wal-insert-timeline*))
         (setf (physical-source-pin-lost-p pin) t)
         (values pin t))
        (t (physical-source-pin-check-wal pin) (values pin nil))))))

;; A READY lease without a pin is inconsistent metadata, not ordinary loss.
(defun physical-backup-lease-check-startup-inventory (candidates)
  (physical-backup-lease-require-locks)
  (maphash (lambda (name record)
             (when (and (eq :ready (getf record :phase)) (null (gethash name candidates)))
               (error "ready backup lease has no source pin")))
           *physical-backup-lease-records*))

(in-package #:pg)

;; An allocation records one follow preparation under a backup lease. LIVE
;; means it has not been canceled. CANCELED permanently revokes preparation,
;; even when pin cleanup is unfinished. The parent lease still determines
;; whether preparation or handoff is allowed. The follow pin stores the
;; prepared anchor; this record stores the allocation identity.

(defconstant +physical-backup-lease-allocation-maximum-bytes+ 8192)
(defconstant +physical-backup-lease-allocation-maximum-generation+ 16)

;; Validate and copy the immutable parent request, generation, successor name
;; and follow coordinates. Each parent has at most sixteen generations.
(defun physical-backup-lease-allocation-copy (record)
  (physical-follow-check-plist record '(:version :request :generation :name :coordinates :state))
  (let* ((request (physical-backup-lease-request-copy (getf record :request)))
         (generation (getf record :generation))
         (name (getf record :name))
         (coordinates (physical-follow-request-copy (getf record :coordinates)))
         (state (getf record :state))
         (base (getf coordinates :base))
         (cursor (getf coordinates :receive-lsn)))
    (unless (and (eql 1 (getf record :version))
                 (integerp generation)
                 (<= 1 generation +physical-backup-lease-allocation-maximum-generation+)
                 (member state '(:live :canceled))
                 (wal-valid-replication-slot-name-p name)
                 (not (equal name (getf request :name)))
                 (= (getf request :system-id) (getf coordinates :system-id))
                 (= (getf request :timeline) (getf coordinates :base-timeline))
                 (<= (getf coordinates :base-timeline) (getf coordinates :receive-timeline)
                     (getf coordinates :source-timeline))
                 (plusp base) (<= base cursor)
                 (= (getf coordinates :overlap-start)
                    (* (floor (1- cursor) +wal-segment-size+) +wal-segment-size+)))
      (error "invalid physical backup lease follow allocation"))
    (list :version 1 :request request :generation generation :name (copy-seq name)
          :coordinates coordinates :state state)))

;; An allocation can move from LIVE to CANCELED. Its identity and coordinates
;; remain fixed and a canceled allocation cannot be revived.
(defun physical-backup-lease-allocation-transition (previous candidate)
  (let* ((before (when previous (physical-backup-lease-allocation-copy previous)))
         (after (physical-backup-lease-allocation-copy candidate)))
    (when before
      (let ((expected (copy-list before)))
        (setf (getf expected :state) (getf after :state))
        (unless (and (equal expected after)
                     (or (eq :live (getf before :state)) (eq :canceled (getf after :state))))
          (error "physical backup lease allocation cannot rebind or revive"))))
    after))

(defun physical-backup-lease-allocation-image (record)
  (physical-backup-lease-encode-literal
   "PGBACKUPALLOC1" (physical-backup-lease-allocation-copy record)
   +physical-backup-lease-allocation-maximum-bytes+))

(defun physical-backup-lease-allocation-decode (bytes)
  (physical-backup-lease-decode-literal
   "PGBACKUPALLOC1" bytes +physical-backup-lease-allocation-maximum-bytes+
   #'physical-backup-lease-allocation-copy))

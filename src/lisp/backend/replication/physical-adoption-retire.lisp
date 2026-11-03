;; Retire a completed physical adoption during owned postmaster startup.
;;
;; Durable terminal follow publication and intact old authority precede the
;; retirement marker.  Once the marker is durable, remove each older authority
;; file in order and sync the root after each removal; remove follow last.
;; Retirement itself does not contact the source, run redo, or grant write
;; authority.  Normal startup continues in standby recovery afterwards.
(in-package #:pg)

(defvar *physical-adoption-retire-write-limit* nil)

(defun physical-adoption-retire-check-unmarked (owner state)
  "Require terminal follow publication and all old authority before marking."
  (let ((last (length (physical-adoption-follow-moves (getf state :plan)))))
    (unless (and (not (getf state :retiring)) (not (getf state :partial))
                 (= last (getf state :step))
                 (= last (physical-adoption-follow-validate owner state)))
      (error "adoption retirement requires complete follow publication"))))

(defun physical-adoption-retire-mark (owner state)
  "Publish or verify the CRC-bound retirement marker on the follow journal.
A torn marker may be repaired only while the old authority remains intact.  Sync
the terminal journal before writing and syncing the complete marker."
  (physical-adoption-retire-check-owner owner)
  (if (getf state :retiring)
      (progn
        (physical-adoption-retire-validate owner state)
        (physical-adoption-follow-sync-journal owner state)
        (physical-adoption-retire-validate owner state)
        state)
      (progn
        (physical-adoption-retire-check-unmarked owner state)
        (physical-adoption-follow-sync-journal owner state)
        (physical-adoption-install-test-point
         :before-retire-marker-write (physical-adoption-follow-path (physical-adoption-owner-directory owner)))
        (physical-adoption-retire-check-unmarked owner state)
        (call-with-physical-adoption-follow-fd
         owner state
         (lambda (fd path)
           (when (getf state :retirement-partial)
             (sb-posix:ftruncate fd (getf state :valid-length))
             (sb-posix:fsync fd)
             (physical-adoption-install-test-point :after-retire-marker-truncate path))
           (physical-adoption-install-write-bytes
            fd (physical-adoption-retire-marker-image (getf state :body-crc))
            (getf state :valid-length) path :limit *physical-adoption-retire-write-limit*
            :point :after-retire-marker-write-chunk)
           (sb-posix:fsync fd)
           (physical-adoption-install-test-point :after-retire-marker-fsync path)))
        (let ((actual (physical-adoption-follow-read owner)))
          (unless (zerop (physical-adoption-retire-validate owner actual))
            (error "adoption authority disappeared before its retirement marker"))
          actual))))

(defun physical-adoption-retire-unlink-authority (owner state prefix)
  "Recheck and remove the next authority file, then sync the root directory."
  (let* ((root (physical-adoption-owner-directory owner))
         (name (first (nth prefix (physical-adoption-retire-files (getf state :plan)))))
         (path (merge-pathnames name root)))
    (physical-adoption-install-test-point :before-retire-authority-unlink path)
    (unless (= prefix (physical-adoption-retire-validate owner state))
      (error "adoption authority advanced before its checked removal"))
    (delete-file path)
    (physical-adoption-install-test-point :after-retire-authority-unlink path)
    (fsync-directory root)
    (physical-adoption-install-test-point :after-retire-authority-fsync root)))

(defun physical-adoption-retire-finish (owner state)
  "Remove the follow fence last, sync the root, and recheck startup ownership."
  (let* ((root (physical-adoption-owner-directory owner))
         (path (physical-adoption-follow-path root)))
    (physical-adoption-install-test-point :before-retire-follow-unlink path)
    (unless (= 3 (physical-adoption-retire-validate owner state))
      (error "adoption follow fence cannot precede earlier authority removal"))
    (delete-file path)
    (physical-adoption-install-test-point :after-retire-follow-unlink path)
    (fsync-directory root)
    (physical-adoption-install-test-point :after-retire-follow-fsync root)
    (physical-adoption-retire-check-owner owner)
    (physical-adoption-check-startup)
    :retired))

(defun physical-adoption-retire-resume (owner)
  "Mark an unmarked terminal follow, or resume from its durable marker and exact authority prefix."
  (physical-adoption-retire-check-owner owner)
  (let ((state (physical-adoption-retire-mark owner (physical-adoption-follow-read owner))))
    (loop for prefix = (physical-adoption-retire-validate owner state)
          while (< prefix 3)
          do (physical-adoption-retire-unlink-authority owner state prefix))
    (physical-adoption-retire-finish owner state)))

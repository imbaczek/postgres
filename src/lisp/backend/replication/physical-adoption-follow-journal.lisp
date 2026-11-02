;; Make follow authority and move progress durable.
;; Each rename's source and destination directories are synced before recording
;; its STEP.  A torn progress record is truncated and synced before replacement;
;; validation must first show the corresponding move already completed.
(in-package #:pg)

(defvar *physical-adoption-follow-write-limit* nil)
(defvar *physical-adoption-follow-stage-write-limit* nil)
(defvar *physical-adoption-follow-progress-write-limit* nil)

(defun call-with-physical-adoption-follow-fd (owner state function)
  (unless (equal state (physical-adoption-follow-read owner))
    (error "adoption follow journal changed before opening for sync"))
  (let* ((path (physical-adoption-follow-path (physical-adoption-owner-directory owner)))
         (identity (getf (getf state :plan) :journal))
         (row (list "physical-adoption.follow" :file #o600 (getf identity :device)
                    (getf identity :inode) 0 0))
         (fd (sb-posix:open (namestring path)
                            (logior sb-posix:o-rdwr sb-posix:o-nofollow +postmaster-o-cloexec+))))
    (unwind-protect
         (let ((stat (sb-posix:fstat fd)))
           (unless (and (physical-adoption-install-row-identity-p row stat)
                        (= (getf state :file-size) (sb-posix:stat-size stat))
                        (equal (physical-adoption-stage-stat-identity stat)
                               (physical-adoption-stage-stat-identity (sb-posix:lstat (namestring path)))))
             (error "adoption follow journal inode changed while opening"))
           (funcall function fd path))
      (sb-posix:close fd))))

(defun physical-adoption-follow-sync-authority (owner value)
  (dolist (pair (list (cons :after-follow-authority-source-fsync
                            (physical-adoption-follow-directory value :workspace))
                      (cons :after-follow-authority-target-fsync
                            (physical-adoption-owner-directory owner))))
    (fsync-directory (cdr pair))
    (physical-adoption-install-test-point (car pair) (cdr pair))))

(defun physical-adoption-follow-sync-journal (owner state)
  (call-with-physical-adoption-follow-fd
   owner state (lambda (fd path)
                 (sb-posix:fsync fd)
                 (physical-adoption-install-test-point :after-follow-journal-fsync path)))
  (physical-adoption-follow-sync-authority owner (getf state :plan))
  (unless (equal state (physical-adoption-follow-read owner))
    (error "adoption follow journal changed during durability sync"))
  state)

(defun physical-adoption-follow-sync-move (value move)
  (dolist (pair (list (cons :after-follow-source-fsync (first move))
                      (cons :after-follow-target-fsync (third move))))
    (let ((directory (physical-adoption-follow-directory value (cdr pair))))
      (fsync-directory directory)
      (physical-adoption-install-test-point (car pair) directory))))

;; Sync the completed move before appending and syncing its progress record.
(defun physical-adoption-follow-append-step (owner state)
  (let* ((value (getf state :plan)) (step (getf state :step))
                                    (moves (physical-adoption-follow-moves value))
                                    (next (1+ step)))
    (unless (and (< step (length moves)) (= next (physical-adoption-follow-validate owner state)))
      (error "adoption follow progress requires exactly one verified move"))
    (physical-adoption-follow-sync-move value (nth step moves))
    (unless (= next (physical-adoption-follow-validate owner state))
      (error "adoption follow moved while syncing its parents"))
    (call-with-physical-adoption-follow-fd
     owner state
     (lambda (fd path)
       (when (getf state :partial)
         (sb-posix:ftruncate fd (getf state :valid-length))
         (sb-posix:fsync fd)
         (physical-adoption-install-test-point :after-follow-progress-truncate path))
       (physical-adoption-install-write-bytes
        fd (physical-adoption-install-step-image (getf state :body-crc) next)
        (getf state :valid-length) path
        :limit *physical-adoption-follow-progress-write-limit* :point :after-follow-progress-write-chunk)
       (sb-posix:fsync fd)
       (physical-adoption-install-test-point :after-follow-progress-fsync path)))
    (let ((actual (physical-adoption-follow-read owner)))
      (unless (and (= next (getf actual :step)) (not (getf actual :partial)))
        (error "adoption follow progress readback differs"))
      (physical-adoption-follow-validate owner actual)
      actual)))

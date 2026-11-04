(in-package #:pg)

(defvar *physical-backup-lease-allocation-history-cache* nil)

;; Histories are indexed by parent lease and ordered by generation.  Rebuild
;; and validate the index under WAL exclusion when the registry is replaced;
;; publication invalidates it before changing the registered records.

(defun physical-backup-lease-allocation-histories ()
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "backup lease allocation lookup requires WAL exclusion"))
  (unless (eq *physical-backup-lease-allocations*
              (car *physical-backup-lease-allocation-history-cache*))
    (when (> (hash-table-count *physical-backup-lease-allocations*) 65536)
      (error "backup lease allocation registry exceeds capacity"))
    (let ((histories (make-hash-table :test #'equal)))
      (maphash
       (lambda (name record)
         (let ((row (handler-case (physical-backup-lease-allocation-copy record)
                      (wal-input-validation-error ()
                        (error "invalid registered backup lease allocation")))))
           (unless (equal name (getf row :name))
             (error "backup lease allocation registry key changed"))
           (push row (gethash (getf (getf row :request) :name) histories))))
       *physical-backup-lease-allocations*)
      (maphash
       (lambda (name rows)
         (physical-backup-lease-allocation-check-history nil rows)
         (setf (gethash name histories)
               (sort rows #'< :key (lambda (row) (getf row :generation)))))
       histories)
      (setf *physical-backup-lease-allocation-history-cache*
            (cons *physical-backup-lease-allocations* histories))))
  (cdr *physical-backup-lease-allocation-history-cache*))

(defun physical-backup-lease-allocation-check-disk (record)
  (unless (equal record (physical-backup-lease-allocation-read
                         (physical-backup-lease-allocation-path record)))
    (error "backup lease allocation registry and disk disagree"))
  record)

(defun physical-backup-lease-allocation-next-history (before after history parent)
  (let ((next
          (if before
              (mapcar (lambda (row)
                        (if (= (getf row :generation) (getf after :generation)) after row))
                      history)
              (append history (list after)))))
    (unless (if before (member before history :test #'equal)
                (= (getf after :generation) (1+ (length history))))
      (wal-invalid-input "backup lease allocation generation is stale"))
    (handler-case (physical-backup-lease-allocation-check-history parent next)
      (error () (wal-invalid-input "backup lease allocation cannot replace active history")))
    next))

(defun physical-backup-lease-allocation-check-new-name (after)
  (let* ((name (getf after :name))
         (assigned (gethash name (physical-backup-lease-successor-records))))
    (when (or (gethash name *physical-backup-lease-records*) assigned
              (gethash name *physical-source-pins*) (gethash name *physical-source-senders*)
              (physical-backup-lease-lstat (physical-source-pin-path name)))
      (wal-invalid-input "backup lease allocation name belongs to another owner"))
    (physical-follow-check-attempt-name name)))

(defun physical-backup-lease-allocation-check-predecessors (history)
  (dolist (row history)
    (physical-backup-lease-allocation-check-disk row)
    (unless (and (eq :canceled (getf row :state))
                 (null (gethash (getf row :name) *physical-source-pins*))
                 (null (gethash (getf row :name) *physical-source-senders*))
                 (null (physical-backup-lease-lstat (physical-source-pin-path (getf row :name)))))
      (wal-invalid-input "backup lease allocation predecessor cleanup is incomplete"))))

(defun physical-backup-lease-allocation-check-publication (before after parent history)
  (handler-case
      (physical-backup-lease-allocation-check-parent after *physical-backup-lease-records*)
    (error () (wal-invalid-input "backup lease allocation differs from its completed parent")))
  (unless (equal before (gethash (getf after :name) *physical-backup-lease-allocations*))
    (wal-invalid-input "backup lease allocation caller has stale state"))
  (if before
      (physical-backup-lease-allocation-check-disk before)
      (progn
        (unless (member (getf parent :phase) '(:ready :interrupted))
          (wal-invalid-input "backup lease allocation requires an eligible completed parent"))
        (unless (and (typep *physical-backup-lease-allocation-limit* '(integer 0 65536))
                     (< (hash-table-count *physical-backup-lease-allocations*)
                        *physical-backup-lease-allocation-limit*))
          (wal-invalid-input "backup lease allocation inventory is at capacity"))
        (physical-backup-lease-allocation-check-new-name after)
        (when (physical-backup-lease-lstat (physical-backup-lease-allocation-path after))
          (error "unregistered backup lease allocation file"))
        (physical-backup-lease-allocation-check-predecessors history)
        (when (eq :live (getf after :state))
          (when (or (recovery-in-progress-p)
                    (/= (getf (getf after :coordinates) :source-timeline) *wal-insert-timeline*)
                    (gethash (getf (getf parent :request) :name) *physical-source-senders*)
                    (gethash (getf (getf parent :request) :name) *physical-backup-lease-capture-owners*))
            (wal-invalid-input "backup lease allocation source is not available for preparation"))
          (physical-backup-lease-ready-action parent))))
  (values))

;; AFTER is a validated transition from BEFORE.  With write and WAL exclusion,
;; recheck the parent, history, registry and disk before starting publication.
;; Write the durable record before exposing it in memory, and return a copy.

(defun physical-backup-lease-allocation-publish-checked-under-lock (before after begin-publication)
  (physical-backup-lease-require-locks)
  (let* ((request (getf after :request))
         (parent (physical-backup-lease-current-record request))
         (histories (physical-backup-lease-allocation-histories))
         (history (gethash (getf request :name) histories))
         (next (physical-backup-lease-allocation-next-history before after history parent)))
    (physical-backup-lease-allocation-check-publication before after parent history)
    (unless (equal before after)
      (funcall begin-publication)
      (when (null before)
        (fsync-directory (if (physical-backup-lease-pin-directory-stat)
                             (physical-source-pin-directory) *pgdata*)))
      (physical-backup-lease-allocation-write after)
      (when *physical-backup-lease-allocation-test-hook*
        (funcall *physical-backup-lease-allocation-test-hook* :before-memory after))
      (setf *physical-backup-lease-allocation-history-cache* nil
            (gethash (copy-seq (getf after :name)) *physical-backup-lease-allocations*) after
            (gethash (copy-seq (getf request :name)) histories) next
            *physical-backup-lease-allocation-history-cache*
            (cons *physical-backup-lease-allocations* histories)))
    (physical-backup-lease-allocation-copy after)))

(defun physical-backup-lease-allocation-publish (previous candidate)
  (let* ((before (when previous (physical-backup-lease-allocation-copy previous)))
         (after (physical-backup-lease-allocation-transition before candidate)))
    (with-live-operation
      (with-write-lock
        (with-wal-publication-fence (begin-publication)
          (physical-backup-lease-allocation-publish-checked-under-lock
           before after #'begin-publication))))))

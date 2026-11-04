(in-package #:pg)

(defun physical-backup-lease-follow-intent (request generation name coordinates state)
  (physical-backup-lease-allocation-copy
   (list :version 1 :request request :generation generation :name name
         :coordinates coordinates :state state)))

(defun physical-backup-lease-allocation-current-intent (candidate)
  (physical-backup-lease-require-locks)
  (let* ((request (getf candidate :request))
         (parent (physical-backup-lease-current-record request))
         (name (getf candidate :name))
         (before (gethash name *physical-backup-lease-allocations*))
         (history (gethash (getf request :name) (physical-backup-lease-allocation-histories))))
    (cond
      (before
       (let ((expected (copy-list before)))
         (setf (getf expected :state) (getf candidate :state))
         (unless (equal expected candidate)
           (wal-invalid-input "backup lease allocation intent is already bound differently")))
       (physical-backup-lease-allocation-check-disk before))
      (t
       (when (find (getf candidate :generation) history :key (lambda (row) (getf row :generation)))
         (wal-invalid-input "backup lease allocation generation belongs to another intent"))
       (when (physical-backup-lease-lstat (physical-backup-lease-allocation-path candidate))
         (error "unregistered backup lease allocation intent file"))))
    (values parent before)))

(defun physical-backup-lease-follow-prepare (request generation name coordinates)
  (let ((candidate (physical-backup-lease-follow-intent request generation name coordinates :live)))
    (with-live-operation
      (let ((row
              (with-write-lock
                (with-wal-publication-fence (begin-publication)
                  (multiple-value-bind (parent before)
                      (physical-backup-lease-allocation-current-intent candidate)
                    (when (or (eq :aborted (getf parent :outcome))
                              (and before (eq :canceled (getf before :state))))
                      (wal-invalid-input "backup lease allocation preparation is permanently revoked"))
                    (physical-backup-lease-allocation-publish-checked-under-lock
                     before (physical-backup-lease-allocation-transition before candidate)
                     #'begin-publication))))))
        (call-with-physical-backup-lease-allocation-context
         :prepare row
         (lambda () (physical-source-follow-prepare (getf row :name) (getf row :coordinates))))))))

(defun physical-backup-lease-allocation-cleanup-pin (row)
  (handler-case
      (multiple-value-bind (pin disk) (physical-source-retirement-pins (getf row :name))
        (declare (ignore disk))
        (when pin (physical-backup-lease-allocation-check-retirement-pin row pin))
        pin)
    (wal-input-validation-error ()
      (error "backup lease allocation cleanup metadata disagree"))))

(defun physical-backup-lease-allocation-settle-parent (parent begin-publication)
  (if (and (eq :closing (getf parent :phase))
           (null (gethash (getf (getf parent :request) :name) *physical-backup-lease-capture-owners*)))
      (physical-backup-lease-finish-closing-under-lock (getf parent :request) begin-publication)
      (physical-backup-lease-copy parent)))

(defun physical-backup-lease-allocation-revoke-under-lock (before candidate begin-publication)
  (physical-backup-lease-require-locks)
  (let ((name (getf candidate :name)))
    (when (gethash name *physical-source-senders*)
      (return-from physical-backup-lease-allocation-revoke-under-lock :busy))
    (let* ((pin (when before (physical-backup-lease-allocation-cleanup-pin before)))
           (row (physical-backup-lease-allocation-publish-checked-under-lock
                 before (physical-backup-lease-allocation-transition before candidate)
                 begin-publication)))
      (funcall begin-publication)
      (call-with-physical-backup-lease-allocation-context
       :retire row
       (lambda ()
         (if pin (physical-source-pin-unlink pin)
             (progn
               (physical-backup-lease-allocation-check-retirement name)
               (fsync-directory (if (physical-backup-lease-pin-directory-stat)
                                    (physical-source-pin-directory) *pgdata*))))))
      row)))

(defun physical-backup-lease-follow-cancel-under-lock (candidate begin-publication)
  (multiple-value-bind (parent before) (physical-backup-lease-allocation-current-intent candidate)
    (let ((name (getf candidate :name)))
      (when (and (member (getf parent :phase) '(:closing :closed))
                 (or (null before) (equal name (getf (getf parent :successor) :name))))
        (when (and before (not (eq :live (getf before :state))))
          (error "handed-off backup lease has a canceled selected allocation"))
        (return-from physical-backup-lease-follow-cancel-under-lock
          (physical-backup-lease-allocation-settle-parent parent begin-publication)))
      (if (eq :busy (physical-backup-lease-allocation-revoke-under-lock before candidate begin-publication))
          :busy
          (physical-backup-lease-allocation-settle-parent parent begin-publication)))))

(defun physical-backup-lease-follow-cancel (request generation name coordinates)
  (let* ((candidate (physical-backup-lease-follow-intent request generation name coordinates :canceled))
         (result
           (with-live-operation
             (with-write-lock
               (with-wal-publication-fence (begin-publication)
                 (physical-backup-lease-follow-cancel-under-lock candidate #'begin-publication))))))
    (when (eq result :busy) (ereport "55006" "backup lease allocation has an active sender"))
    result))
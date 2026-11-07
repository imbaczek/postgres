(in-package #:pg)

(defstruct (physical-backup-lease-selected-cleanup-authority
            (:constructor %make-physical-backup-lease-selected-cleanup-authority)
            (:copier nil)
            (:print-object
             (lambda (value stream)
               (print-unreadable-object (value stream :type t :identity t)))))
  (check nil :read-only t) (phase :acquiring) active-p)

(defvar *physical-backup-lease-selected-cleanup-authority* nil)

(defun physical-backup-lease-selected-cleanup-observation (record retired)
  (physical-backup-lease-require-locks)
  (let* ((request (getf record :request))
         (name (getf request :name))
         (selected-name (getf (getf record :successor) :name)))
    (when (or (gethash name *physical-backup-lease-capture-owners*)
              (gethash name *physical-source-senders*)
              (gethash selected-name *physical-source-senders*))
      (wal-invalid-input "selected cleanup still has an active source owner"))
    (multiple-value-bind (presence pin coordinates row)
        (physical-backup-lease-selected-observation-under-lock record)
      (let* ((bootstrap
               (if (eq :closing (getf record :phase))
                   (physical-backup-lease-closing-bootstrap-pin record)
                   (progn
                     (physical-backup-lease-pin-directory-stat)
                     (nth-value 0 (physical-source-retirement-pins name)))))
             (history (gethash name (physical-backup-lease-allocation-histories))))
        (when (and retired bootstrap)
          (wal-invalid-input "selected cleanup retained its bootstrap pin"))
        (values
         (list presence (gethash selected-name *physical-source-pins*)
               (when pin (coerce (physical-source-pin-encode pin) 'list))
               coordinates row
               (mapcar #'physical-backup-lease-allocation-copy history))
         bootstrap
         (list (gethash name *physical-source-pins*)
               (when bootstrap (coerce (physical-source-pin-encode bootstrap) 'list))))))))

(defun physical-backup-lease-selected-cleanup-check-entry (authority)
  (when (or authority *physical-backup-lease-selected-cleanup-authority*)
    (unless (and (physical-backup-lease-selected-cleanup-authority-p authority)
                 (eq authority *physical-backup-lease-selected-cleanup-authority*)
                 (physical-backup-lease-selected-cleanup-authority-active-p authority)
                 (functionp (physical-backup-lease-selected-cleanup-authority-check authority))
                 (not (eq :acquiring (physical-backup-lease-selected-cleanup-authority-phase authority))))
      (wal-invalid-input "selected cleanup entry requires its explicit active authority")))
  (values))

(defun physical-backup-lease-selected-cleanup-claim (authority record operation &optional candidate)
  (unless authority
    (wal-invalid-input "selected cleanup lacks its active typed authority"))
  (physical-backup-lease-selected-cleanup-check-entry authority)
  (funcall (physical-backup-lease-selected-cleanup-authority-check authority)
           authority record operation candidate))

(defun call-with-physical-backup-lease-selected-cleanup (record function)
  (when *physical-backup-lease-selected-cleanup-authority*
    (wal-invalid-input "selected cleanup authority cannot reenter"))
  (let* ((before (physical-backup-lease-copy record))
         (after (physical-backup-lease-copy before))
         (thread sb-thread:*current-thread*)
         (backend *backend*)
         (incarnation *wal-storage-incarnation*)
         (root (copy-seq (namestring (uiop:ensure-directory-pathname *pgdata*))))
         (phase :acquiring)
         (claiming nil)
         (active t)
         (selected nil)
         (bootstrap-image nil)
         (authority nil))
    (unless (and (eq :closing (getf before :phase))
                 (eq :handed-off (getf before :outcome)))
      (wal-invalid-input "selected cleanup requires a closing handoff"))
    (setf (getf after :phase) :closed)
    (labels ((scope (value actual operation candidate)
               (unless (and active (not claiming) (eq value authority)
                            (eq value *physical-backup-lease-selected-cleanup-authority*)
                            (physical-backup-lease-selected-cleanup-authority-active-p value)
                            (eq phase (physical-backup-lease-selected-cleanup-authority-phase value))
                            (eq thread sb-thread:*current-thread*) (eq backend *backend*)
                            (eq incarnation *wal-storage-incarnation*)
                            (string= root (namestring (uiop:ensure-directory-pathname *pgdata*)))
                            (equal actual (if (eq operation :done) after before))
                            (or (null candidate) (equal candidate after)))
                 (wal-invalid-input "selected cleanup authority scope changed")))
             (advance (next)
               (sb-sys:without-interrupts
                 (setf phase next
                       (physical-backup-lease-selected-cleanup-authority-phase authority) next)))
             (claim (value actual operation candidate)
               (scope value actual operation candidate)
               (let ((expected
                       (ecase operation
                         (:retire :ready) (:retired :retiring)
                         (:publish :retired) (:done :publishing))))
                 (unless (and (eq phase expected)
                              (if (eq operation :publish) candidate (null candidate)))
                   (wal-invalid-input "selected cleanup authority phase was reused"))
                 (setf claiming t)
                 (when (member operation '(:retire :publish))
                   (advance (if (eq operation :retire) :retiring :publishing)))
                 (multiple-value-bind (observed current-bootstrap current-bootstrap-image)
                     (physical-backup-lease-selected-cleanup-observation
                      actual (not (eq operation :retire)))
                   (unless (and (equal selected observed)
                                (or (not (eq operation :retire))
                                    (equal bootstrap-image current-bootstrap-image)))
                     (wal-invalid-input "selected cleanup ownership observation changed"))
                   (prog1 (case operation
                            (:retire current-bootstrap)
                            (:retired (advance :retired))
                            (:done (advance :done)))
                     (setf claiming nil))))))
      (setf authority (%make-physical-backup-lease-selected-cleanup-authority
                       :check #'claim :active-p t))
      (let ((*physical-backup-lease-selected-cleanup-authority* authority))
        (unwind-protect
             (progn
               (multiple-value-bind (observed bootstrap image)
                   (physical-backup-lease-selected-cleanup-observation before nil)
                 (declare (ignore bootstrap))
                 (setf selected observed bootstrap-image image))
               (advance :ready)
               (prog1 (funcall function authority)
                 (unless (eq phase :done)
                   (error "selected cleanup authority was not completely consumed"))))
          (sb-sys:without-interrupts
            (setf active nil
                  (physical-backup-lease-selected-cleanup-authority-active-p authority) nil)))))))

(defun physical-backup-lease-cleanup-selected (request successor)
  (when *physical-backup-lease-selected-cleanup-authority*
    (wal-invalid-input "selected cleanup cannot recursively acquire authority"))
  (let* ((request (physical-backup-lease-request-copy request))
         (successor (physical-backup-lease-successor-copy successor request)))
    (with-live-operation
      (with-write-lock
        (with-wal-publication-fence (begin-publication)
          (let ((record (physical-backup-lease-current-record request)))
            (unless (and (member (getf record :phase) '(:closing :closed))
                         (eq :handed-off (getf record :outcome))
                         (equal successor (getf record :successor)))
              (wal-invalid-input "selected cleanup has no matching committed handoff"))
            (if (eq :closed (getf record :phase))
                (progn
                  (physical-backup-lease-selected-cleanup-observation record t)
                  (physical-backup-lease-copy record))
                (call-with-physical-backup-lease-selected-cleanup
                 record
                 (lambda (authority)
                   (physical-backup-lease-finish-closing-under-lock
                    request #'begin-publication authority))))))))))

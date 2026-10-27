(in-package #:pg)

(defstruct (physical-retained-root-reference (:constructor %make-physical-retained-root-reference))
  directory identity root-id objects)

(defstruct (physical-retained-root-hold (:constructor %make-physical-retained-root-hold))
  owner directory identity expected-root objects candidates reference (state :preparing))

(defvar *physical-retained-root-references* (make-hash-table :test #'eq))

(defvar *physical-retained-root-holds* (make-hash-table :test #'eq))

(defvar *physical-retained-root-updates* (make-hash-table :test #'eq))

(defvar *physical-retained-root-test-hook* nil)

(defun physical-retained-root-test-point (point hold)
  (when *physical-retained-root-test-hook* (funcall *physical-retained-root-test-hook* point hold)))

(defun physical-retained-root-require-lock ()
  (unless (sb-thread:holding-mutex-p *physical-retained-inventory-publish-lock*)
    (error "retained WAL ownership requires archive exclusion")))

(defun physical-retained-root-directory (directory)
  (unless (uiop:directory-pathname-p directory) (error "retained WAL directory is not a directory pathname"))
  (let ((path (truename directory)))
    (unless (uiop:directory-exists-p path) (error "retained WAL directory is absent"))
    path))

(defun physical-retained-root-copy-identity (identity)
  (let ((keys '(:system-id :base-timeline :base :manifest-digest)))
    (physical-follow-check-plist identity keys)
    (unless (and (every (lambda (key) (typep (getf identity key) '(unsigned-byte 64)))
                        '(:system-id :base :manifest-digest))
                 (typep (getf identity :base-timeline) '(integer 1 4294967295)))
      (error "invalid retained WAL owner identity"))
    (loop for key in keys append (list key (getf identity key)))))

(defun physical-retained-root-add-objects (destination source)
  (when source (maphash (lambda (object value) (declare (ignore value))
                          (setf (gethash object destination) t)) source))
  destination)

(defun physical-retained-root-make-reference (directory identity id entries)
  (let ((objects (make-hash-table :test #'equal)))
    (setf (gethash (cons :inventory id) objects) t)
    (dolist (entry entries) (setf (gethash (cons :extent (first entry)) objects) t))
    (%make-physical-retained-root-reference
     :directory directory :identity (physical-retained-root-copy-identity identity)
     :root-id id :objects objects)))

(defun physical-retained-root-load-reference (directory identity id history)
  (physical-retained-root-require-lock)
  (when id
    (let ((payload (physical-retained-inventory-load directory id identity history)))
      (physical-retained-root-make-reference directory identity id (getf payload :entries)))))

(defun physical-retained-root-current (owner)
  (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
    (let ((reference (gethash owner *physical-retained-root-references*)))
      (and reference (physical-retained-root-reference-root-id reference)))))

(defun physical-retained-root-check-context (reference directory identity)
  (when reference
    (unless (and (equal directory (physical-retained-root-reference-directory reference))
                 (equal identity (physical-retained-root-reference-identity reference)))
      (error "retained WAL control owner belongs to another archive or base"))))

(defun physical-retained-root-install (owner reference)
  (physical-retained-root-require-lock)
  (if reference (setf (gethash owner *physical-retained-root-references*) reference)
      (remhash owner *physical-retained-root-references*)))

(defun physical-retained-root-register (owner directory id identity history)
  (unless owner (error "retained WAL control owner is absent"))
  (let ((directory (physical-retained-root-directory directory))
        (identity (physical-retained-root-copy-identity identity)))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (when (gethash owner *physical-retained-root-updates*)
        (error "retained WAL control owner has an unresolved update"))
      (let ((current (gethash owner *physical-retained-root-references*)))
        (physical-retained-root-check-context current directory identity)
        (when (and current (not (eql id (physical-retained-root-reference-root-id current))))
          (error "retained WAL control changed; reconcile its durable reference")))
      (let ((reference (physical-retained-root-load-reference directory identity id history)))
        (sb-sys:without-interrupts (physical-retained-root-install owner reference)))
      id)))

(defun physical-retained-root-drop-hold (hold state)
  (physical-retained-root-require-lock)
  (setf (physical-retained-root-hold-state hold) state)
  (remhash hold *physical-retained-root-holds*)
  (when (eq hold (gethash (physical-retained-root-hold-owner hold) *physical-retained-root-updates*))
    (remhash (physical-retained-root-hold-owner hold) *physical-retained-root-updates*)))

(defun physical-retained-root-control-snapshot (owner directory identity)
  (physical-retained-root-require-lock)
  (let ((current (gethash owner *physical-retained-root-references*))
        (hold (gethash owner *physical-retained-root-updates*)))
    (physical-retained-root-check-context current directory identity)
    (when hold
      (unless (and (eq :uncertain (physical-retained-root-hold-state hold))
                   (equal directory (physical-retained-root-hold-directory hold))
                   (equal identity (physical-retained-root-hold-identity hold)))
        (error "retained WAL control update is still active or has another context")))
    (values current hold)))

(defun physical-retained-root-check-control-snapshot (owner current hold)
  (physical-retained-root-require-lock)
  (unless (and (eq current (gethash owner *physical-retained-root-references*))
               (eq hold (gethash owner *physical-retained-root-updates*))
               (or (null hold) (eq :uncertain (physical-retained-root-hold-state hold))))
    (error "retained WAL control owner changed during control readback")))

(defun physical-retained-root-reconcile (owner directory identity history read-selected)
  (unless (and owner (functionp read-selected))
    (error "retained WAL reconciliation requires an owner and durable control reader"))
  (let ((directory (physical-retained-root-directory directory))
        (identity (physical-retained-root-copy-identity identity)))
    (multiple-value-bind (current hold)
        (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
          (physical-retained-root-control-snapshot owner directory identity))
      (let ((id (funcall read-selected)))
        (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
          (physical-retained-root-check-control-snapshot owner current hold)
          (when hold
            (unless (or (eql id (physical-retained-root-hold-expected-root hold))
                        (and id (gethash id (physical-retained-root-hold-candidates hold))))
              (error "retained WAL control selected an unexpected inventory")))
          (when (and (not hold) current
                     (not (eql id (physical-retained-root-reference-root-id current))))
            (error "retained WAL control changed without a reserved update"))
          (let ((reference (physical-retained-root-load-reference directory identity id history)))
            (sb-sys:without-interrupts
              (physical-retained-root-install owner reference)
              (when hold (physical-retained-root-drop-hold hold :reconciled))))
          id)))))

(defun physical-retained-root-retire (owner directory expected identity read-selected)
  (unless (and owner (functionp read-selected))
    (error "retained WAL retirement requires an owner and durable control reader"))
  (let ((directory (physical-retained-root-directory directory))
        (identity (physical-retained-root-copy-identity identity)))
    (multiple-value-bind (current hold)
        (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
          (physical-retained-root-control-snapshot owner directory identity))
      (unless (null (funcall read-selected))
        (error "retained WAL control still selects an inventory"))
      (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
        (physical-retained-root-check-control-snapshot owner current hold)
        (unless (or (and (null current) (null hold))
                    (and current (eql expected (physical-retained-root-reference-root-id current)))
                    (and hold
                         (or (eql expected (physical-retained-root-hold-expected-root hold))
                             (gethash expected (physical-retained-root-hold-candidates hold)))))
          (error "retained WAL retired control reference is stale"))
        (sb-sys:without-interrupts
          (physical-retained-root-install owner nil)
          (when hold (physical-retained-root-drop-hold hold :retired))))
      t)))

(defun physical-retained-root-prepare-update (owner directory identity expected)
  (physical-retained-root-require-lock)
  (unless owner (error "retained WAL control owner is absent"))
  (when (gethash owner *physical-retained-root-updates*)
    (error "retained WAL control owner already has an active or uncertain update"))
  (let* ((current (gethash owner *physical-retained-root-references*))
         (objects (make-hash-table :test #'equal)))
    (physical-retained-root-check-context current directory identity)
    (unless (eql expected (and current (physical-retained-root-reference-root-id current)))
      (error "retained WAL expected control root is stale"))
    (when current (physical-retained-root-add-objects objects (physical-retained-root-reference-objects current)))
    (%make-physical-retained-root-hold
     :owner owner :directory directory :identity identity :expected-root expected
     :objects objects :candidates (make-hash-table :test #'eql))))

(defun physical-retained-root-check-hold (hold)
  (physical-retained-root-require-lock)
  (unless (and (eq hold (gethash hold *physical-retained-root-holds*))
               (eq hold (gethash (physical-retained-root-hold-owner hold) *physical-retained-root-updates*))
               (member (physical-retained-root-hold-state hold) '(:preparing :selecting)))
    (error "retained WAL publication hold is not active")))

(defun physical-retained-root-update-publish-under-lock (hold target text images previous keep-ids keep-supplied-p)
  (physical-retained-root-check-hold hold)
  (unless (or (null previous) (typep previous '(unsigned-byte 64)))
    (error "invalid retained WAL previous inventory"))
  (let* ((directory (physical-retained-root-hold-directory hold))
         (identity (physical-retained-root-hold-identity hold))
         (history (timeline-check-lineage (timeline-parse-history target text)))
         (prepared nil))
    (when previous
      (setf (gethash (cons :inventory previous) (physical-retained-root-hold-objects hold)) t)
      (let* ((index (physical-retained-inventory-read-index directory previous identity history))
             (reference (physical-retained-root-make-reference directory identity previous (getf index :entries))))
        (physical-retained-root-add-objects (physical-retained-root-hold-objects hold)
                                            (physical-retained-root-reference-objects reference))))
    (let ((id (apply #'physical-retained-inventory-publish-under-lock
                     directory identity target text images :previous previous
                     :protect (lambda (id entries)
                                (setf prepared (physical-retained-root-make-reference directory identity id entries))
                                (physical-retained-root-add-objects
                                 (physical-retained-root-hold-objects hold)
                                 (physical-retained-root-reference-objects prepared)))
                     (when keep-supplied-p (list :keep-ids keep-ids)))))
      (unless (and prepared (= id (physical-retained-root-reference-root-id prepared)))
        (error "retained WAL publication did not produce its prepared reference"))
      (sb-sys:without-interrupts
        (setf (gethash id (physical-retained-root-hold-candidates hold)) t
              (physical-retained-root-hold-reference hold) prepared))
      id)))

(defun physical-retained-root-update-publish (hold target text images &key previous (keep-ids nil keep-supplied-p))
  (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
    (physical-retained-root-update-publish-under-lock hold target text images previous keep-ids keep-supplied-p)))

(defun call-with-physical-retained-root-update
    (function owner directory identity target text images
     &key previous (expected-root previous) (keep-ids nil keep-supplied-p) read-selected)
  (unless (and (functionp function) (functionp read-selected))
    (error "retained WAL update requires selection and durable control reader functions"))
  (let ((directory (physical-retained-root-directory directory))
        (identity (physical-retained-root-copy-identity identity))
        (hold nil) (callback-started nil) (completed nil))
    (unwind-protect
         (let ((id
                 (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                   (let ((prepared (physical-retained-root-prepare-update owner directory identity expected-root)))
                     (sb-sys:without-interrupts
                       (setf hold prepared
                             (gethash hold *physical-retained-root-holds*) hold
                             (gethash owner *physical-retained-root-updates*) hold)
                       (physical-retained-root-test-point :after-hold-registration hold)))
                   (physical-retained-root-update-publish-under-lock
                    hold target text images previous keep-ids keep-supplied-p))))
           (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
             (sb-sys:without-interrupts
               (setf callback-started t (physical-retained-root-hold-state hold) :selecting)))
           (let* ((acknowledged (funcall function id hold))
                  (selected (funcall read-selected)))
             (physical-retained-root-test-point :after-control-readback hold)
             (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
               (physical-retained-root-check-hold hold)
               (let ((current (gethash owner *physical-retained-root-references*))
                     (reference (physical-retained-root-hold-reference hold)))
                 (physical-retained-root-check-context current directory identity)
                 (unless (and (eql expected-root (and current (physical-retained-root-reference-root-id current)))
                              (eql acknowledged selected)
                              (eql selected (physical-retained-root-reference-root-id reference)))
                   (error "retained WAL control readback or acknowledgement does not name its latest candidate"))
                 (sb-sys:without-interrupts
                   (physical-retained-root-install owner reference)
                   (physical-retained-root-test-point :after-reference-install hold)
                   (physical-retained-root-drop-hold hold :selected)
                   (setf completed t))))
             selected))
      (sb-sys:without-interrupts
        (when (and hold (not completed))
          (physical-retained-root-test-point :before-cleanup-lock hold)
          (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
            (if callback-started (setf (physical-retained-root-hold-state hold) :uncertain)
                (physical-retained-root-drop-hold hold :released))))))))

(defun physical-retained-root-protected-under-lock (directory kind id)
  (physical-retained-root-require-lock)
  (unless (and (member kind '(:extent :inventory)) (typep id '(unsigned-byte 64)))
    (error "invalid retained WAL protection lookup"))
  (let ((object (cons kind id)))
    (or (loop for reference being the hash-values of *physical-retained-root-references*
                thereis (and (equal directory (physical-retained-root-reference-directory reference))
                             (gethash object (physical-retained-root-reference-objects reference))))
        (loop for hold being the hash-values of *physical-retained-root-holds*
                thereis (and (equal directory (physical-retained-root-hold-directory hold))
                             (gethash object (physical-retained-root-hold-objects hold)))))))

(defun physical-retained-root-protected-p (directory kind id)
  (let ((directory (physical-retained-root-directory directory)))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (physical-retained-root-protected-under-lock directory kind id))))

(defun physical-retained-root-delete-unprotected (directory kind id)
  (let ((directory (physical-retained-root-directory directory)))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (unless (physical-retained-root-protected-under-lock directory kind id)
        (let ((path (physical-retained-object-path directory kind id)))
          (when (wal-namespace-file-stat path t)
            (delete-file path)
            (fsync-directory directory)
            t))))))

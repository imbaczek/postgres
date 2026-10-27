;;;; Process-local ownership of immutable retained WAL objects.
;;;;
;;;; Selected references protect complete inventories. Update holds protect old
;;;; and candidate objects until durable control selection is resolved. Startup
;;;; must restore every durable reference before deletion starts. Callers own
;;;; control serialization; callbacks run outside the archive mutex. Never take
;;;; store or WAL locks while holding that mutex.

(in-package #:pg)

(defstruct (physical-retained-root-reference (:constructor %make-physical-retained-root-reference))
  "A process-local owner's selected root and the objects it protects."
  directory identity root-id objects)

(defstruct (physical-retained-root-hold (:constructor %make-physical-retained-root-hold))
  "Protect an update's old and candidate objects until selection is resolved."
  owner directory identity expected-root objects candidates reference (state :preparing))

(defvar *physical-retained-root-references* (make-hash-table :test #'eq))

(defvar *physical-retained-root-holds* (make-hash-table :test #'eq))

(defvar *physical-retained-root-updates* (make-hash-table :test #'eq))

(defvar *physical-retained-root-test-hook* nil)

(defun physical-retained-root-test-point (point hold)
  "Call the private root-ownership test hook, if installed."
  (when *physical-retained-root-test-hook* (funcall *physical-retained-root-test-hook* point hold)))

(defun physical-retained-root-require-lock ()
  "Require the archive mutex for registry and object protection changes."
  (unless (sb-thread:holding-mutex-p *physical-retained-inventory-publish-lock*)
    (error "retained WAL ownership requires archive exclusion")))

(defun physical-retained-root-directory (directory)
  "Return the existing archive directory's canonical pathname."
  (unless (uiop:directory-pathname-p directory) (error "retained WAL directory is not a directory pathname"))
  (let ((path (truename directory)))
    (unless (uiop:directory-exists-p path) (error "retained WAL directory is absent"))
    path))

(defun physical-retained-root-copy-identity (identity)
  "Validate and copy an owner's base identity."
  (let ((keys '(:system-id :base-timeline :base :manifest-digest)))
    (physical-follow-check-plist identity keys)
    (unless (and (every (lambda (key) (typep (getf identity key) '(unsigned-byte 64)))
                        '(:system-id :base :manifest-digest))
                 (typep (getf identity :base-timeline) '(integer 1 4294967295)))
      (error "invalid retained WAL owner identity"))
    (loop for key in keys append (list key (getf identity key)))))

(defun physical-retained-root-add-objects (destination source)
  "Add SOURCE's protected object keys to DESTINATION."
  (when source (maphash (lambda (object value) (declare (ignore value))
                          (setf (gethash object destination) t)) source))
  destination)

(defun physical-retained-root-make-reference (directory identity id entries &optional overlap)
  "Build a reference protecting the inventory, extents and optional overlap.

Keep the overlap descriptor as the value of its object key."

  (let ((objects (make-hash-table :test #'equal)))
    (setf (gethash (cons :inventory id) objects) t)
    (when overlap (setf (gethash (cons :overlap (first overlap)) objects) (copy-tree overlap)))
    (dolist (entry entries) (setf (gethash (cons :extent (first entry)) objects) t))
    (%make-physical-retained-root-reference
     :directory directory :identity (physical-retained-root-copy-identity identity)
     :root-id id :objects objects)))

(defun physical-retained-root-reference-overlap (reference)
  "Return the overlap descriptor protected by REFERENCE, if present."
  (loop for object being the hash-keys of (physical-retained-root-reference-objects reference)
          using (hash-value descriptor)
        when (eq (car object) :overlap) return descriptor))

(defun physical-retained-root-load-reference (directory identity id history)
  "Validate the complete selected root under the archive mutex and build its reference."
  (physical-retained-root-require-lock)
  (when id
    (let ((payload (physical-retained-inventory-load directory id identity history)))
      (physical-retained-root-make-reference directory identity id (getf payload :entries)
                                             (getf payload :base-overlap)))))

(defun physical-retained-root-current (owner)
  "Return OWNER's process-local selected root ID, if any."
  (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
    (let ((reference (gethash owner *physical-retained-root-references*)))
      (and reference (physical-retained-root-reference-root-id reference)))))

(defun physical-retained-root-check-context (reference directory identity)
  "Reject a reference belonging to another archive or base."
  (when reference
    (unless (and (equal directory (physical-retained-root-reference-directory reference))
                 (equal identity (physical-retained-root-reference-identity reference)))
      (error "retained WAL control owner belongs to another archive or base"))))

(defun physical-retained-root-install (owner reference)
  "Install or remove OWNER's reference under the archive mutex."
  (physical-retained-root-require-lock)
  (if reference (setf (gethash owner *physical-retained-root-references*) reference)
      (remhash owner *physical-retained-root-references*)))

(defun physical-retained-root-register (owner directory id identity history)
  "Validate and register an owner's durable root before deletion starts.

The registry is process-local.  Startup must restore every durable reference;
this function refuses a silent change to an already registered root."
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
  "Retire HOLD and its owner update under the archive mutex."
  (physical-retained-root-require-lock)
  (setf (physical-retained-root-hold-state hold) state)
  (remhash hold *physical-retained-root-holds*)
  (when (eq hold (gethash (physical-retained-root-hold-owner hold) *physical-retained-root-updates*))
    (remhash (physical-retained-root-hold-owner hold) *physical-retained-root-updates*)))

(defun physical-retained-root-control-snapshot (owner directory identity)
  "Capture OWNER's reference and any uncertain hold before control readback."
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
  "Require the captured owner state to be unchanged after control readback."
  (physical-retained-root-require-lock)
  (unless (and (eq current (gethash owner *physical-retained-root-references*))
               (eq hold (gethash owner *physical-retained-root-updates*))
               (or (null hold) (eq :uncertain (physical-retained-root-hold-state hold))))
    (error "retained WAL control owner changed during control readback")))

(defun physical-retained-root-reconcile (owner directory identity history read-selected)
  "Reconcile an uncertain update with the root read from durable control.

Read control outside the archive mutex, then recheck the captured owner state
before validating and installing its reference."
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
  "Retire an owner's protection after control no longer selects a root.

Read control outside the archive mutex and recheck the captured owner state."
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
  "Prepare a hold only if EXPECTED is the owner's current control root."
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
  "Require an active hold registered for its owner."
  (physical-retained-root-require-lock)
  (unless (and (eq hold (gethash hold *physical-retained-root-holds*))
               (eq hold (gethash (physical-retained-root-hold-owner hold) *physical-retained-root-updates*))
               (member (physical-retained-root-hold-state hold) '(:preparing :selecting)))
    (error "retained WAL publication hold is not active")))

(defun physical-retained-root-record-candidate (hold id prepared)
  "Record a successfully published candidate and its prepared reference under archive exclusion."
  (physical-retained-root-require-lock)
  (unless (and prepared (= id (physical-retained-root-reference-root-id prepared)))
    (error "retained WAL publication did not produce its prepared reference"))
  (sb-sys:without-interrupts
    (setf (gethash id (physical-retained-root-hold-candidates hold)) t
          (physical-retained-root-hold-reference hold) prepared))
  id)

(defun physical-retained-root-merge-overlap (old new)
  "Preserve the existing overlap or require an identical replacement."
  (when (and old new (not (equal old new)))
    (error "retained base overlap cannot change within one base identity"))
  (or old new))

(defun physical-retained-root-held-overlap (hold)
  (let ((reference (or (physical-retained-root-hold-reference hold)
                       (gethash (physical-retained-root-hold-owner hold)
                                *physical-retained-root-references*))))
    (when reference (physical-retained-root-reference-overlap reference))))

(defun physical-retained-root-update-publish-under-lock (hold target text images previous keep-ids keep-supplied-p)
  "Publish a candidate while HOLD protects its inputs and objects.

PREVIOUS supplies inventory entries and can differ from the expected control
root. Successful intermediate candidates remain protected; the latest is the
candidate for selection."
  (physical-retained-root-check-hold hold)
  (unless (or (null previous) (typep previous '(unsigned-byte 64)))
    (error "invalid retained WAL previous inventory"))
  (let* ((directory (physical-retained-root-hold-directory hold))
         (identity (physical-retained-root-hold-identity hold))
         (history (timeline-check-lineage (timeline-parse-history target text)))
         (prepared nil) (overlap (physical-retained-root-held-overlap hold)))
    (when previous
      (setf (gethash (cons :inventory previous) (physical-retained-root-hold-objects hold)) t)
      (let* ((index (physical-retained-inventory-read-index directory previous identity history))
             (reference (physical-retained-root-make-reference directory identity previous (getf index :entries)
                                                               (getf index :base-overlap))))
        (setf overlap (physical-retained-root-merge-overlap overlap (getf index :base-overlap)))
        (physical-retained-root-add-objects (physical-retained-root-hold-objects hold)
                                            (physical-retained-root-reference-objects reference))))
    (let ((id (apply #'physical-retained-inventory-publish-under-lock
                     directory identity target text images :previous previous :base-overlap overlap
                     :protect (lambda (id entries)
                                (setf prepared (physical-retained-root-make-reference directory identity id entries overlap))
                                (physical-retained-root-add-objects
                                 (physical-retained-root-hold-objects hold)
                                 (physical-retained-root-reference-objects prepared)))
                     (when keep-supplied-p (list :keep-ids keep-ids)))))
      (physical-retained-root-record-candidate hold id prepared))))

(defun physical-retained-root-stage-image (hold target text image)
  "Copy and validate an extent, then protect its ID before writing it.

Publication and hold changes use archive exclusion. Return a fresh entry."
  (physical-retained-wal-check-budget (list image))
  (let* ((image (copy-seq image))
         (id (physical-store-update-digest #xCBF29CE484222325 image))
         (metadata (physical-retained-extent-decode image))
         (entry (list id (length image) metadata)))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (physical-retained-root-check-hold hold)
      (let ((history (physical-retained-inventory-check
                      (list :version 1 :identity (physical-retained-root-hold-identity hold)
                            :history-target target :history-text text :entries (list entry)))))
        (multiple-value-bind (checked bytes) (physical-retained-extent-decode image)
          (physical-retained-extent-scan checked bytes history)))
      (setf (gethash (cons :extent id) (physical-retained-root-hold-objects hold)) t)
      (physical-retained-object-publish (physical-retained-root-hold-directory hold)
                                        :extent image +physical-retained-image-limit+)
      (physical-retained-inventory-test-point :after-extent-publication id))
    entry))

(defun physical-retained-root-stage-overlap (hold target text image)
  "Validate an overlap and reserve its object ID under HOLD before writing it."
  (physical-retained-wal-check-budget (list image))
  (let* ((image (copy-seq image))
         (id (physical-store-update-digest #xCBF29CE484222325 image))
         (metadata (physical-retained-overlap-decode image))
         (entry (list id (length image) metadata)))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (physical-retained-root-check-hold hold)
      (physical-retained-inventory-check
       (physical-retained-inventory-payload
        (physical-retained-root-hold-identity hold) target text nil entry))
      (setf (gethash (cons :overlap id) (physical-retained-root-hold-objects hold)) t)
      (physical-retained-object-publish (physical-retained-root-hold-directory hold)
                                        :overlap image +physical-retained-image-limit+)
      (physical-retained-inventory-test-point :after-overlap-publication id))
    entry))

(defun physical-retained-root-publish-staged-entries (hold target text entries &key base-overlap)
  "Publish a checked root from entries reserved by HOLD.

Require every extent ID to be protected before loading it. Protect the root
before writing, and record it as a candidate only after successful publication."
  (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
    (physical-retained-root-check-hold hold)
    (setf base-overlap (physical-retained-root-merge-overlap
                        (physical-retained-root-held-overlap hold) base-overlap))
    (when (and base-overlap
               (not (gethash (cons :overlap (first base-overlap)) (physical-retained-root-hold-objects hold))))
      (error "retained base overlap was not reserved before reading"))
    (dolist (entry entries)
      (unless (gethash (cons :extent (first entry)) (physical-retained-root-hold-objects hold))
        (error "retained inventory entry was not reserved before reading")))
    (let ((prepared nil))
      (multiple-value-bind (id payload)
          (physical-retained-inventory-publish-entries-under-lock
           (physical-retained-root-hold-directory hold) (physical-retained-root-hold-identity hold)
           target text entries
           (lambda (id entries)
             (setf prepared (physical-retained-root-make-reference
                             (physical-retained-root-hold-directory hold)
                             (physical-retained-root-hold-identity hold) id entries base-overlap))
             (physical-retained-root-add-objects (physical-retained-root-hold-objects hold)
                                                 (physical-retained-root-reference-objects prepared)))
           :base-overlap base-overlap)
        (physical-retained-root-record-candidate hold id prepared)
        (values id payload)))))

(defun physical-retained-root-update-publish (hold target text images &key previous (keep-ids nil keep-supplied-p))
  "Publish another candidate under an active hold."
  (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
    (physical-retained-root-update-publish-under-lock hold target text images previous keep-ids keep-supplied-p)))

(defun call-with-physical-retained-root-update
    (function owner directory identity target text images
     &key previous (expected-root previous) (keep-ids nil keep-supplied-p) read-selected)
  "Publish candidates and select the latest through the caller's control owner.

The caller serializes its control. FUNCTION must fsync that control and return
the selected latest candidate ID; READ-SELECTED reads it back. Both run outside
the archive mutex. Agreement is not independent proof that control was synced.
An exit before FUNCTION releases the hold; an uncertain exit after it begins
retains protection until readback reconciliation or durable retirement."
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
  "Check references and holds for this directory, object kind and ID."
  (physical-retained-root-require-lock)
  (unless (and (member kind '(:extent :inventory :overlap)) (typep id '(unsigned-byte 64)))
    (error "invalid retained WAL protection lookup"))
  (let ((object (cons kind id)))
    (or (loop for reference being the hash-values of *physical-retained-root-references*
                thereis (and (equal directory (physical-retained-root-reference-directory reference))
                             (gethash object (physical-retained-root-reference-objects reference))))
        (loop for hold being the hash-values of *physical-retained-root-holds*
                thereis (and (equal directory (physical-retained-root-hold-directory hold))
                             (gethash object (physical-retained-root-hold-objects hold)))))))

(defun physical-retained-root-protected-p (directory kind id)
  "Return whether an object is protected by a local reference or hold."
  (let ((directory (physical-retained-root-directory directory)))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (physical-retained-root-protected-under-lock directory kind id))))

(defun physical-retained-root-delete-unprotected (directory kind id)
  "Delete an unprotected object under the archive mutex and sync its directory.

Restore all durable references before deletion begins.  Do not acquire store or
WAL locks while holding the archive mutex."
  (let ((directory (physical-retained-root-directory directory)))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (unless (physical-retained-root-protected-under-lock directory kind id)
        (let ((path (physical-retained-object-path directory kind id)))
          (when (wal-namespace-file-stat path t)
            (delete-file path)
            (fsync-directory directory)
            t))))))

(in-package #:pg)

;; Reconcile the observed selected generation under the startup ownership scope.
;; This context borrows descriptors and check callbacks and owns private copies
;; of the expected ledgers.  Only the active row or resource may change during
;; an effect; the context expires when its enclosing scope exits.

(defstruct (logical-slot-continuation-cold-reconciliation
            (:constructor %make-logical-slot-continuation-cold-reconciliation)
            (:print-function physical-libpq-print-private-object))
  storage preparation observation ledger selector temporary registry image
  slot-root root-check tail-check sidecar-check
  active-slot slot-removed selector-resource selector-removed
  (active t) (failed nil) (phase :ready) (thread sb-thread:*current-thread*))

(defvar *logical-slot-continuation-cold-reconciliations*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defvar *logical-slot-continuation-cold-reconciliation-test-hook* nil)

(defun logical-slot-continuation-cold-reconciliation-point (context point)
  (when *logical-slot-continuation-cold-reconciliation-test-hook*
    (funcall *logical-slot-continuation-cold-reconciliation-test-hook* point))
  (logical-slot-continuation-cold-reconciliation-check context))

(defmacro with-logical-slot-continuation-cold-reconciliation-check (context &body body)
  (let ((actual (gensym "CONTEXT")) (done (gensym "DONE")))
    `(let ((,actual ,context))
       (unless (and (logical-slot-continuation-cold-reconciliation-p ,actual)
                    (eq ,actual (gethash ,actual *logical-slot-continuation-cold-reconciliations*))
                    (logical-slot-continuation-cold-reconciliation-active ,actual)
                    (eq sb-thread:*current-thread*
                        (logical-slot-continuation-cold-reconciliation-thread ,actual)))
         (error "cold reconciliation ended or changed its registered identity or thread"))
       (sb-sys:without-interrupts
         (when (logical-slot-continuation-cold-reconciliation-failed ,actual)
           (error "cold reconciliation failed"))
         (let ((,done nil))
           (unwind-protect
                (multiple-value-prog1 (sb-sys:with-local-interrupts (progn ,@body))
                  (setf ,done t))
             (unless ,done
               (setf (logical-slot-continuation-cold-reconciliation-failed ,actual) t))))))))

(defun logical-slot-continuation-cold-reconciliation-base-check (context)
  (with-logical-slot-continuation-cold-reconciliation-check context
    (logical-slot-continuation-cold-preparation-call
     (logical-slot-continuation-cold-reconciliation-preparation context) :check)
    (logical-slot-continuation-storage-check
     (logical-slot-continuation-cold-reconciliation-storage context))
    (funcall (logical-slot-continuation-cold-reconciliation-root-check context))
    (funcall (logical-slot-continuation-cold-reconciliation-tail-check context))
    (funcall (logical-slot-continuation-cold-reconciliation-sidecar-check context))
    (logical-slot-publication-check-registry
     (logical-slot-continuation-cold-reconciliation-registry context)
     (logical-slot-continuation-cold-reconciliation-image context))))

(defun logical-slot-continuation-cold-reconciliation-check-selector (context)
  (let* ((storage (logical-slot-continuation-cold-reconciliation-storage context))
         (directory (logical-slot-continuation-storage-call storage :directory))
         (selected (logical-slot-continuation-cold-reconciliation-selector context))
         (temporary (logical-slot-continuation-cold-reconciliation-temporary context))
         (removed (logical-slot-continuation-cold-reconciliation-selector-removed context))
         (names (if (and temporary (not removed)) '("selector" "selector.tmp") '("selector"))))
    (unless (equal names (logical-slot-copy-import-control-names directory 3))
      (error "cold reconciliation selector names changed"))
    (dolist (row (if removed (list selected) (remove nil (list selected temporary))))
      (call-with-logical-slot-copy-source-proof-fd
       directory (first row) nil
       (lambda (fd facts)
         (unless (and (equal facts (second row))
                      (equalp (third row)
                              (logical-slot-copy-import-control-read-fd
                               fd (logical-slot-continuation-prepared-maximum-bytes))))
           (error "cold reconciliation selector inode or bytes changed")))))))

(defun logical-slot-continuation-cold-reconciliation-check (context)
  (with-logical-slot-continuation-cold-reconciliation-check context
    (logical-slot-continuation-cold-reconciliation-base-check context)
    (let* ((observation (logical-slot-continuation-cold-reconciliation-observation context))
           (snapshot (getf observation :snapshot))
           (prepared (when (eq (getf observation :kind) :prepared)
                       (nth-value 1 (logical-slot-continuation-cold-decode-selected
                                     (third (getf observation :selector))))))
           (directory (logical-slot-continuation-cold-reconciliation-slot-root context))
           (ledger (logical-slot-continuation-cold-reconciliation-ledger context))
           (active (logical-slot-continuation-cold-reconciliation-active-slot context)))
      (declare (ignore snapshot))
      (unless (equal (mapcar #'first ledger) (logical-slot-continuation-slot-inventory-names directory))
        (error "cold reconciliation complete slot inventory changed"))
      (dolist (row ledger)
        (let* ((name (first row))
               (target (when prepared (getf prepared :name)))
               (new (when prepared
                      (second (find target (getf (getf prepared :new) :slots)
                                    :key #'first :test #'string=))))
               (observed (logical-slot-continuation-cold-read-row
                          directory name
                          (lambda () (logical-slot-continuation-cold-reconciliation-base-check context))
                          target new)))
          (unless (if (and active (string= active name))
                      (and (equal name (first observed))
                           (equal (logical-slot-copy-source-proof-directory-identity (second row))
                                  (logical-slot-copy-source-proof-directory-identity (second observed)))
                           (equalp (subseq row 2 4) (subseq observed 2 4))
                           (equalp (if (logical-slot-continuation-cold-reconciliation-slot-removed context)
                                       nil (fifth row))
                                   (fifth observed)))
                      (equalp row observed))
            (error "cold reconciliation slot inode, bytes or temporary changed"))))
      (unless (eq :replacement
                  (logical-slot-continuation-cold-reconciliation-selector-resource context))
        (logical-slot-continuation-cold-reconciliation-check-selector context)))
    (logical-slot-continuation-cold-reconciliation-base-check context)
    t))

(defun logical-slot-continuation-cold-reconciliation-with-tail (storage observation archive proof function &key commit-guard)
  (let* ((snapshot (getf observation :snapshot))
         (horizon (getf observation :horizon))
         (facts (logical-slot-continuation-storage-call storage :facts)))
    (flet ((boundaries ()
             (logical-slot-continuation-runtime-tail-observer
              (second (getf snapshot :origin)) (first (getf snapshot :sealed-end))
              (logical-slot-continuation-prove-parent-boundaries
               storage facts archive
               (logical-slot-continuation-saved-boundaries snapshot (getf facts :certificate))
               :batch-policy :logical-reader)
              :logical-reader)))
      (call-with-logical-slot-continuation-local-tail-source
       storage snapshot
       (lambda (prove check-tail reader guard &optional map-records check-commit)
         (declare (ignore reader guard))
         (labels ((reprove ()
                    (unless (equalp proof
                                    (list* :tail (nth-value 1 (funcall prove))
                                           (logical-slot-continuation-cold-policy
                                            facts snapshot horizon archive map-records)))
                      (error "cold reconciliation changed its completed WAL proof"))))
           (reprove)
           (logical-slot-continuation-storage-call
            storage :with-sidecar
            (lambda (sidecar check-sidecar)
              (declare (ignore sidecar))
              (multiple-value-prog1 (apply function check-tail check-sidecar
                                           (when commit-guard (list check-commit)))
                (funcall check-sidecar)
                (reprove))))))
       :boundaries #'boundaries :end horizon :batch-policy :logical-reader
       :require-closed nil :transcript t :witness t :commit-guard commit-guard))))

(defun logical-slot-continuation-cold-reconciliation-with-context
    (storage preparation observation registry image check-tail check-sidecar)
  ;; The context owns the checked inventory only within this thread and scope.
  ;; Recheck the selected bytes, WAL proof and empty registry around effects.
  (logical-slot-continuation-storage-call
   storage :with-cold-slot-root
   (lambda (directory check-directory)
     (let ((context
             (%make-logical-slot-continuation-cold-reconciliation
              :storage storage :preparation preparation :observation observation
              :ledger (logical-slot-continuation-copy (getf observation :ledger))
              :selector (logical-slot-continuation-copy (getf observation :selector))
              :temporary (logical-slot-continuation-copy (getf observation :temporary))
              :registry registry :image image :slot-root directory
              :root-check check-directory :tail-check check-tail :sidecar-check check-sidecar)))
       (sb-sys:without-interrupts
         (unwind-protect
              (progn
                (setf (gethash context *logical-slot-continuation-cold-reconciliations*) context)
                (sb-sys:with-local-interrupts
                  (logical-slot-continuation-cold-reconciliation-check context)
                  (logical-slot-continuation-cold-reconciliation-execute context)
                  (logical-slot-continuation-cold-reconciliation-check context)
                  (logical-slot-continuation-copy
                   (list :kind :stable :choice :stable
                         :snapshot (getf observation :snapshot)
                         :selector (logical-slot-continuation-cold-reconciliation-selector context)
                         :temporary nil
                         :directory-facts
                         (physical-image-file-facts
                          (logical-slot-continuation-storage-call storage :directory))
                         :slot-root-facts (physical-image-file-facts directory)
                         :rows (mapcar (lambda (row) (list (first row) (fourth row)))
                                       (logical-slot-continuation-cold-reconciliation-ledger context))
                         :ledger (logical-slot-continuation-cold-reconciliation-ledger context)
                         :horizon (getf observation :horizon)))))
           (setf (logical-slot-continuation-cold-reconciliation-active context) nil)
           (remhash context *logical-slot-continuation-cold-reconciliations*)))))))

(defun logical-slot-continuation-cold-reconcile-storage
    (storage preparation observation proof registry image)
  (logical-slot-continuation-storage-call
   storage :with-origin
   (lambda (archive)
     (let ((reservation (logical-slot-continuation-cold-preparation-call preparation :reservation)))
       (call-with-logical-slot-continuation-cold-observation
        storage reservation
        (lambda (fresh actual check)
          (unless (and (eq actual reservation) (equalp fresh observation))
            (error "cold reconciliation changed its claimed inventory"))
          (funcall check))))
     (logical-slot-continuation-cold-reconciliation-with-tail
      storage observation archive proof
      (lambda (check-tail check-sidecar)
        (logical-slot-continuation-cold-reconciliation-with-context
         storage preparation observation registry image check-tail check-sidecar))))))

(defun logical-slot-continuation-cold-reconcile-prepared (owner preparation)
  (let* ((observation (logical-slot-continuation-cold-preparation-call preparation :observation))
         (proof (logical-slot-continuation-cold-preparation-call preparation :proof))
         (registry (logical-slot-continuation-cold-preparation-call preparation :registry))
         (reservation (logical-slot-continuation-cold-preparation-call preparation :reservation))
         (image (logical-slot-publication-registry-image registry)))
    (logical-slot-continuation-cold-preparation-call preparation :claim)
    (let ((final
            (call-with-logical-slot-startup-publication-storage
             owner
             (lambda (storage facts)
               (declare (ignore facts))
               (logical-slot-continuation-cold-reconcile-storage
                storage preparation observation proof registry image)))))
      (sb-thread:with-mutex (*wal-lock*)
        (values
         (logical-slot-continuation-copy (getf observation :snapshot))
         (list :registry registry :image image :observation final :proof proof
               :reservation reservation
               :binding (logical-slot-continuation-cold-reservation-call reservation :binding)
               :origin (logical-slot-continuation-cold-reservation-call reservation :origin)
               :root (let ((facts (physical-image-file-facts
                                   -100 (namestring (uiop:ensure-directory-pathname *pgdata*)))))
                       (list (first facts) (second facts) (ninth facts)))
               :host (logical-slot-imported-host-startup-image)
               :mirrors (logical-slot-publication-mirrors)
               :incarnation *wal-storage-incarnation* :namespace *wal-segment-namespace*
               :maximum *max-replication-slots* :wal-level *wal-level*
               :thread sb-thread:*current-thread* :mutex *wal-lock*))))))

(defun logical-slot-continuation-cold-reconcile (gate ownership)
  ;; This entry reconciles disk only: it retains F and leaves the registry empty.
  ;; Preparation expires before the completed observation is exposed.
  (call-with-logical-slot-startup-publication-owner
   gate ownership
   (lambda (owner)
     (call-with-logical-slot-continuation-cold-completion
      owner gate ownership
      (lambda (completion)
        (logical-slot-continuation-copy
         (getf (getf (logical-slot-continuation-cold-completion-call completion :data)
                     :observation)
               :snapshot)))))))

(in-package #:pg)

(defstruct (logical-slot-startup-publication-owner
            (:include logical-slot-imported-host-startup-owner)
            (:constructor %make-logical-slot-startup-publication-owner)
            (:print-function physical-libpq-print-private-object))
  (publication-state nil :read-only t))

(defstruct (logical-slot-startup-publication-token
            (:constructor %make-logical-slot-startup-publication-token)
            (:print-function physical-libpq-print-private-object)))

(defvar *logical-slot-startup-publication-owners* nil)
(defvar *logical-slot-startup-publication-tokens*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defun logical-slot-startup-publication-token-binding (owner token)
  (let ((binding (gethash token *logical-slot-startup-publication-tokens*)))
    (unless (and binding (eq owner (svref binding 0)) (eq :derived (svref binding 3)))
      (error "startup publication requires its unconsumed derived token"))
    binding))

(defun logical-slot-startup-publication-make-state (gate ownership image baseline registry-image control receipt)
  (let ((incarnation *wal-storage-incarnation*) (namespace *wal-segment-namespace*)
                                                (maximum *max-replication-slots*) (level *wal-level*)
                                                (thread sb-thread:*current-thread*) (mutex *wal-lock*) (owner nil))
    (labels ((check-identity (candidate)
               (unless (and (eq candidate owner) (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*))
                 (error "startup publication owner changed identity, thread or WAL mutex")))
             (check ()
               (unless (and (svref control 1) (not (svref control 2))
                            (member owner *logical-slot-startup-publication-owners* :test #'eq))
                 (error "startup publication owner ended or failed")))
             (under-lock ()
               (assert (sb-thread:holding-mutex-p *wal-lock*))
               (let ((expected (if (eq :published (svref control 0)) (svref control 4) baseline))
                     (rows (if (eq :published (svref control 0)) (svref control 5) registry-image)))
                 (unless (and (eq incarnation *wal-storage-incarnation*) (eq namespace *wal-segment-namespace*)
                              (= maximum *max-replication-slots*) (eq level *wal-level*)
                              (equalp image (logical-slot-imported-host-startup-image))
                              (equal expected (logical-slot-publication-mirrors)))
                   (error "startup publication host or mirror state changed"))
                 (logical-slot-publication-check-registry *wal-replication-slots* rows)))
             (state (operation candidate)
               (check-identity candidate)
               (ecase operation
                 (:identity t)
                 (:check (check))
                 (:authority
                  (check-primary-storage-startup-context gate ownership)
                  (logical-slot-copy-check-postmaster-owner owner ownership))
                 (:under-lock (under-lock))
                 (:facts
                  (values (first image)
                          (timeline-check-lineage
                           (timeline-parse-history (fifth image)
                                                   (if (fourth image) (bytes->string (fourth image)) "")))
                          (sixth image)))
                 (:fail
                  (setf (svref control 2) t)
                  (when (eq :published (svref control 0))
                    (logical-slot-publication-fence (first (svref control 4)))))
                 (:release (setf (svref control 1) nil))))
             (publish (candidate operation token)
               (check-identity candidate)
               (check)
               (let* ((binding (logical-slot-startup-publication-token-binding owner token))
                      (registry (svref binding 1))
                      (rows (svref binding 2)))
                 (ecase operation
                   (:prepare
                    (unless (eq :old (svref control 0))
                      (error "startup publication owner already prepared a replacement"))
                    (sb-thread:with-mutex (*wal-lock*)
                      (under-lock)
                      (logical-slot-publication-check-registry registry rows)
                      (unless (and (svref binding 4) (<= (hash-table-count registry) maximum)
                                   (every (lambda (row)
                                            (notany #'identity (subseq (eighth row) 0 6)))
                                          rows))
                        (error "startup publication candidates are active, failed or over capacity"))
                      (multiple-value-bind (count policy disable)
                          (logical-slot-startup-prepare-mirrors registry)
                        (setf (svref control 3) token
                              (svref control 4) (list registry count policy disable)
                              (svref control 5) rows
                              (svref control 6) (logical-slot-continuation-copy (svref binding 4))
                              (svref control 0) :prepared))))
                   (:commit
                    (assert (sb-thread:holding-mutex-p *wal-lock*))
                    (unless (and (eq :prepared (svref control 0)) (eq token (svref control 3))
                                 (equalp (svref binding 4) (svref control 6)))
                      (error "startup publication token does not match its prepared owner"))
                    (under-lock)
                    (logical-slot-publication-check-registry registry rows)
                    (let* ((expected (svref control 4))
                           (count (second expected)) (policy (third expected)) (disable (fourth expected)))
                      (sb-thread:with-mutex (*xid-lock*)
                        (sb-sys:without-interrupts
                          (setf *wal-replication-slots* registry
                                *logical-capture-live-slots* count
                                *wal-logical-policy* policy
                                *wal-logical-info-disable-pending* disable
                                (svref binding 3) :published
                                (svref control 0) :published
                                (svref receipt 0) registry)))))
                   ))))
      (values #'state #'publish (lambda (candidate) (setf owner candidate))))))

(defun logical-slot-startup-publication-call (owner operation token)
  (unless (and (logical-slot-startup-publication-owner-p owner)
               (member owner *logical-slot-startup-publication-owners* :test #'eq))
    (error "startup publication requires its registered owner"))
  (logical-slot-startup-publication-token-binding owner token)
  (call-with-logical-slot-imported-host-startup-check
   owner
   (lambda ()
     (funcall (logical-slot-startup-publication-owner-publication-state owner)
              owner operation token))))

(defun call-with-logical-slot-startup-publication-owner (gate ownership function &optional (receipt (vector nil)))
  (when (sb-thread:holding-mutex-p *wal-lock*)
    (error "startup publication acquisition requires the WAL mutex to be released"))
  (unless (and (typep receipt '(simple-vector 1)) (null (svref receipt 0)))
    (error "startup publication requires a fresh completion receipt"))
  (check-primary-storage-startup-context gate ownership)
  (let ((control (vector :old t nil nil nil nil nil)) (owner nil) (complete nil))
    (multiple-value-bind (state publish install)
        (sb-thread:with-mutex (*wal-lock*)
          (logical-slot-startup-publication-make-state
           gate ownership (logical-slot-imported-host-startup-image)
           (logical-slot-publication-mirrors)
           (logical-slot-publication-registry-image *wal-replication-slots*) control receipt))
      (setf owner (apply #'%make-logical-slot-startup-publication-owner
                         :state state :publication-state publish
                         (logical-slot-copy-owner-arguments ownership)))
      (funcall install owner)
      (let ((*logical-slot-imported-host-startup-owners*
              (cons owner *logical-slot-imported-host-startup-owners*))
            (*logical-slot-startup-publication-owners*
              (cons owner *logical-slot-startup-publication-owners*)))
        (unwind-protect
             (progn
               (logical-slot-imported-host-startup-check-owner owner)
               (multiple-value-prog1 (funcall function owner)
                 (logical-slot-imported-host-startup-check-owner owner)
                 (setf complete t)))
          (sb-sys:without-interrupts
            (unless complete (funcall state :fail owner))
            (funcall state :release owner)
            (setf (physical-adoption-owner-active-p owner) nil)))))))

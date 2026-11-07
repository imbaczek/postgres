(in-package #:pg)

(defstruct (physical-reseed-follow-cancellation-operation
            (:constructor %make-physical-reseed-follow-cancellation-operation)
            (:copier nil)
            (:print-function physical-libpq-print-private-object))
  (owner nil :read-only t) (thread nil :read-only t) (root nil :read-only t)
  (dispatch nil :read-only t))

(defvar *physical-reseed-follow-cancellation-operation* nil)

(defun physical-reseed-follow-cancellation-owner-root (owner)
  (call-with-physical-directory-root
   owner
   (lambda (fd directory)
     (declare (ignore directory))
     (let ((stat (sb-posix:fstat fd)))
       (list (copy-seq (namestring (physical-adoption-owner-directory owner)))
             (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
             (physical-adoption-install-mount-id nil :directory-fd fd))))))

(defun physical-reseed-follow-cancellation-operation-call (operation action &optional expected value)
  (unless (physical-reseed-follow-cancellation-operation-p operation)
    (error "cancellation work requires its typed operation"))
  (funcall (physical-reseed-follow-cancellation-operation-dispatch operation)
           operation action expected value))

(declaim (ftype function physical-reseed-follow-publish-cancellation-source-closed-held
                physical-reseed-follow-publish-cancellation-cleaned-held))

(defun physical-reseed-follow-cancellation-check-operation-scope (actual operation owner thread root live)
  (unless (and live (eq actual operation)
               (eq *physical-reseed-follow-cancellation-operation* operation)
               (eq thread sb-thread:*current-thread*)
               (physical-reseed-follow-owner-busy-p owner)
               (eq owner (physical-reseed-follow-cancellation-operation-owner operation))
               (eq thread (physical-reseed-follow-cancellation-operation-thread operation))
               (equal root (physical-reseed-follow-cancellation-operation-root operation))
               (equal root (physical-reseed-follow-cancellation-owner-root owner)))
    (error "cancellation operation changed owner, thread, root or lifetime")))

(defun physical-reseed-follow-cancellation-check-held-state (owner root expected fd)
  (let* ((stat (sb-posix:fstat fd))
         (actual (physical-reseed-follow-cancellation-snapshot-at fd)))
    (unless (and (= (second root) (sb-posix:stat-dev stat))
                 (= (third root) (sb-posix:stat-ino stat))
                 (= (fourth root) (physical-adoption-install-mount-id nil :directory-fd fd))
                 (equal root (physical-reseed-follow-cancellation-owner-root owner))
                 (equal (getf expected :attempt) (getf actual :attempt))
                 (equal (getf expected :install) (getf actual :install)))
      (error "cancellation publication changed actual attempt or install evidence"))))

(defun physical-reseed-follow-cancellation-check-adopted-state (expected candidate record final after)
  (unless (and (equal candidate record) (equal final (getf after :follow))
               (equal candidate (getf (getf after :follow) :record))
               (equal (getf expected :attempt) (getf after :attempt))
               (equal (getf expected :install) (getf after :install)))
    (error "cancellation publication differs from its frozen operation")))

(defun physical-reseed-follow-cancellation-operation-snapshot (owner recovery expected)
  (if recovery
      (physical-reseed-follow-cleaned-recovery-network-snapshot owner recovery expected)
      (physical-reseed-follow-cancellation-network-snapshot owner expected)))

(defun physical-reseed-follow-cancellation-acquire-state (owner recovery-request)
  (let ((recovery
          (ecase recovery-request
            ((nil) nil)
            (:cleaned
             (call-with-physical-directory-root
              owner
              (lambda (fd directory)
                (declare (ignore directory))
                (when (physical-reseed-follow-entry fd "physical-reseed.follow.tmp" t)
                  (physical-reseed-follow-capture-cleaned-recovery owner))))))))
    (values (physical-reseed-follow-cancellation-owner-root owner)
            (physical-reseed-follow-cancellation-operation-snapshot owner recovery nil)
            recovery)))

(defun physical-reseed-follow-cancellation-snapshot-ack-plan (snapshot)
  (let* ((record (getf (getf snapshot :follow) :record))
         (temporary (getf (getf snapshot :temporary) :record))
         (receipt (or (getf (getf record :cancellation) :receipt)
                      (getf (getf temporary :cancellation) :receipt))))
    (physical-reseed-follow-cancellation-ack-plan record receipt)))

(defun physical-reseed-follow-cancellation-publication-candidate (snapshot action observation)
  (let ((before (getf (getf snapshot :follow) :record)))
    (ecase action
      (:source-closed
       (when (getf snapshot :temporary)
         (error "cleanup recovery cannot publish a source observation over its temporary"))
       (physical-reseed-follow-cancellation-source-closed-candidate before observation))
      (:cleaned
       (physical-reseed-follow-cancellation-cleaned-candidate
        before (physical-reseed-follow-cancellation-snapshot-ack-plan snapshot))))))

(defun call-with-physical-reseed-follow-cancellation-operation (owner function)
  (%call-with-physical-reseed-follow-cancellation-operation owner function nil))

(defun %call-with-physical-reseed-follow-cancellation-operation (owner function recovery-request)
  (when *physical-reseed-follow-cancellation-operation*
    (error "cancellation operation cannot reenter acquisition or work"))
  (physical-reseed-follow-check-idle-state owner)
  (let ((*physical-reseed-follow-cancellation-operation* :acquiring)
        (operation nil) (current nil) (root nil) (recovery nil)
        (published nil) (complete nil) (body-finished nil)
        (thread sb-thread:*current-thread*) (live t) (checking nil) (phase :acquiring))
    (unwind-protect
         (progn
           (setf (physical-reseed-follow-owner-busy-p owner) t)
           (multiple-value-setq (root current recovery)
             (physical-reseed-follow-cancellation-acquire-state owner recovery-request))
           (labels
               ((snapshot (expected)
                  (physical-reseed-follow-cancellation-operation-snapshot owner recovery expected))
                (scope (actual)
                  (physical-reseed-follow-cancellation-check-operation-scope
                   actual operation owner thread root live))
                (publication-check (fd)
                  (scope operation)
                  (unless (eq phase :publishing) (error "cancellation publication has no active operation"))
                  (physical-reseed-follow-cancellation-check-held-state owner root current fd))
                (evidence-guard ()
                  (let ((frozen (physical-adoption-attempt-value-copy current)) (guarding nil))
                    (lambda ()
                      (when guarding (error "cleanup evidence cannot reenter its readiness guard"))
                      (let ((previous-checking checking))
                        (unwind-protect
                             (progn
                               (setf guarding t checking t)
                               (scope operation)
                               (unless (and (eq phase :ready) (equal frozen current)
                                            (not (physical-reseed-follow-owner-uncertain-p owner)))
                                 (error "cleanup evidence changed its ready operation epoch"))
                               (snapshot frozen))
                          (setf guarding nil checking previous-checking))))))
                (dispatch (actual action expected value)
                  (when checking (error "cancellation operation cannot reenter verification"))
                  (unwind-protect
                       (progn
                         (setf checking t)
                         (scope actual)
                         (when (and (physical-reseed-follow-owner-uncertain-p owner) (not (eq action :snapshot)))
                           (error "uncertain cancellation publication requires recovery"))
                         (unless (or (eq action :snapshot) (equal expected current))
                           (error "cancellation operation changed its expected predecessor"))
                         (ecase action
                           (:snapshot (physical-adoption-attempt-value-copy current))
                           (:check
                            (unless (member phase '(:ready :network))
                              (error "cancellation operation is publishing"))
                            (snapshot current))
                           (:evidence-checker (evidence-guard))
                           (:evidence-check
                            (unless (eq phase :ready)
                              (error "cleanup evidence requires a completed network phase"))
                            (snapshot current))
                           (:network-begin
                            (unless (eq phase :ready) (error "cancellation operation already has active work"))
                            (physical-reseed-follow-cancellation-check-cleanup-capacity
                             (getf (getf current :follow) :record))
                            (snapshot current)
                            (setf phase :network))
                           (:network-end
                            (unless (eq phase :network) (error "cancellation operation has no active network work"))
                            (snapshot current)
                            (setf phase :ready))
                           ((:source-closed :cleaned)
                            (unless (eq phase :ready) (error "cancellation operation cannot publish during network work"))
                            (let* ((candidate (physical-reseed-follow-cancellation-publication-candidate
                                               current action value))
                                   (finalized nil) (adopted nil) (begun nil))
                              (physical-reseed-follow-cancellation-check-cleanup-capacity candidate)
                              (unwind-protect
                                   (multiple-value-bind (record final)
                                       (flet ((begin (actual-candidate)
                                                (when begun (error "cancellation publication already began"))
                                                (setf begun t)
                                                (scope operation)
                                                (unless (and (eq phase :ready) (equal expected current)
                                                             (equal candidate actual-candidate))
                                                  (error "cancellation publication changed its ready predecessor"))
                                                (snapshot current)
                                                (setf phase :publishing)))
                                         (if (eq action :source-closed)
                                             (progn
                                               (begin candidate)
                                               (physical-reseed-follow-publish-cancellation-source-closed-held
                                                owner current candidate #'publication-check))
                                             (physical-reseed-follow-publish-cancellation-cleaned-held
                                              owner operation current value #'begin #'publication-check)))
                                     (setf finalized t)
                                     (let ((after (physical-reseed-follow-cancellation-network-snapshot owner)))
                                       (physical-reseed-follow-cancellation-check-adopted-state
                                        current candidate record final after)
                                       (setf current (physical-adoption-attempt-value-copy after)
                                             recovery nil phase :ready published t adopted t)
                                       (values (physical-reseed-follow-copy record)
                                               (physical-adoption-attempt-value-copy current))))
                                (when (and finalized (not adopted))
                                  (setf (physical-reseed-follow-owner-uncertain-p owner) t)))))))
                    (setf checking nil))))
             (setf operation
                   (%make-physical-reseed-follow-cancellation-operation
                    :owner owner :thread thread :root (physical-adoption-attempt-value-copy root) :dispatch #'dispatch))
             (let ((*physical-reseed-follow-cancellation-operation* operation))
               (setf phase :ready)
               (multiple-value-prog1 (funcall function operation)
                 (setf body-finished t)
                 (unless (eq phase :ready)
                   (error "cancellation operation returned with unfinished work"))
                 (dispatch operation :check current nil)
                 (setf complete t)))))
      (sb-sys:without-interrupts
        (when (and body-finished published (not complete))
          (setf (physical-reseed-follow-owner-uncertain-p owner) t))
        (setf live nil (physical-reseed-follow-owner-busy-p owner) nil)))))

(defun call-with-physical-reseed-follow-cancellation-source
    (operation expected function &key helper)
  (let* ((snapshot (physical-adoption-attempt-value-copy expected))
         (record (getf (getf snapshot :follow) :record))
         (source (physical-follow-source-copy (getf (getf record :attempt) :source)))
         (request (physical-backup-lease-request-copy (getf (getf record :attempt) :request)))
         (client nil) (result nil))
    (labels ((check ()
               (physical-reseed-follow-cancellation-operation-call operation :check snapshot)
               (physical-adoption-attempt-check-client client (physical-follow-source-copy source)))
             (raw-rpc (function)
               (check)
               (multiple-value-prog1 (physical-adoption-attempt-value-copy (funcall function))
                 (check)))
             (authenticate ()
               (raw-rpc (lambda ()
                          (physical-reseed-source-identity
                           client (physical-backup-lease-request-copy request) :current-timeline nil))))
             (rpc (function)
               (authenticate)
               (multiple-value-prog1 (raw-rpc function)
                 (authenticate))))
      (physical-reseed-follow-cancellation-operation-call operation :network-begin snapshot)
      (unwind-protect
           (let ((frozen (nth-value 1 (physical-follow-source-profile source))))
             (setf client
                   (physical-libpq-connect-frozen
                    (or helper (physical-libpq-helper-path :on-spawn (lambda (actor) (setf client actor))))
                    frozen :on-spawn (lambda (actor) (setf client actor))))
             (authenticate)
             (setf result (physical-adoption-attempt-value-copy (funcall function client #'rpc)))
             (authenticate))
        (when client (physical-client-close client)))
      (physical-reseed-follow-cancellation-operation-call operation :network-end snapshot)
      result)))

(defstruct (physical-reseed-follow-cleanup-operation-evidence
            (:constructor %make-physical-reseed-follow-cleanup-operation-evidence)
            (:copier nil)
            (:print-function physical-libpq-print-private-object))
  (claim nil :read-only t))

(let ((issued (make-hash-table :test #'eq))
      (active (make-hash-table :test #'eq))
      (lock (sb-thread:make-mutex :name "cleanup operation evidence")))
  (defun physical-reseed-follow-claim-cleanup-operation-evidence (evidence operation expected)
    (let ((claim (sb-thread:with-mutex (lock) (gethash evidence issued))))
      (unless (and (physical-reseed-follow-cleanup-operation-evidence-p evidence)
                   claim
                   (eq claim (physical-reseed-follow-cleanup-operation-evidence-claim evidence)))
        (error "cleanup acknowledgment requires live issuer-owned evidence"))
      (funcall claim evidence operation expected)))

  (defun call-with-physical-reseed-follow-cleanup-operation-evidence
      (operation expected function &key helper)
    (let* ((snapshot (physical-adoption-attempt-value-copy expected))
           (record (getf (getf snapshot :follow) :record))
           (plan (physical-reseed-follow-cancellation-snapshot-ack-plan snapshot))
           (request (physical-backup-lease-request-copy (getf (getf record :attempt) :request)))
           (thread sb-thread:*current-thread*)
           (readiness (physical-reseed-follow-cancellation-operation-call operation :evidence-checker snapshot))
           (confirmed nil) (evidence nil) (live nil) (claimed nil))
      (unless plan (error "cancellation history retains unresolved generation debt"))
      (let ((acquired nil))
        (unwind-protect
             (progn
               (sb-thread:with-mutex (lock)
                 (when (gethash operation active)
                   (error "cleanup evidence issuer cannot reenter its operation"))
                 (sb-sys:without-interrupts
                   (setf (gethash operation active) t acquired t)))
               (labels (
                        (claim (actual actual-operation actual-snapshot)
                          (unless (and live (not claimed) (eq actual evidence)
                                       (eq actual-operation operation)
                                       (eq thread sb-thread:*current-thread*)
                                       (eq *physical-reseed-follow-cancellation-operation* operation)
                                       (equal actual-snapshot snapshot))
                            (error "cleanup evidence changed operation, predecessor, thread or lifetime"))
                          (setf claimed t)
                          (funcall readiness)
                          (physical-adoption-attempt-value-copy confirmed)))
                 (setf confirmed
                       (call-with-physical-reseed-follow-cancellation-source
                        operation snapshot
                        (lambda (client rpc)
                          (let ((result
                                  (physical-reseed-follow-cancellation-confirm-operation-result
                                   record plan
                                   (funcall rpc
                                            (lambda ()
                                              (ecase (getf plan :operation)
                                                (:abort
                                                 (pg-client-native-backup-lease-abort
                                                  client (physical-backup-lease-request-copy request)))
                                                (:release
                                                 (pg-client-native-backup-lease-cleanup-selected
                                                  client (physical-backup-lease-request-copy request)
                                                  (physical-backup-lease-successor-copy
                                                   (getf plan :successor) request)))))))))
                            (when (eq :release (getf result :operation))
                              (let ((successor (getf result :successor)))
                                (unless (eq t (funcall rpc
                                                       (lambda ()
                                                         (pg-client-native-release
                                                          client (copy-seq (getf successor :name))
                                                          (getf successor :anchor-digest)))))
                                  (error "selected cleanup did not acknowledge digest release"))))
                            result))
                        :helper helper))
                 (setf evidence (%make-physical-reseed-follow-cleanup-operation-evidence :claim #'claim))
                 (sb-thread:with-mutex (lock)
                   (setf (gethash evidence issued) #'claim))
                 (unwind-protect
                      (progn
                        (setf live t)
                        (funcall function evidence))
                   (setf live nil))))
          (sb-thread:with-mutex (lock)
            (when evidence (remhash evidence issued))
            (when acquired (remhash operation active)))))))
  )

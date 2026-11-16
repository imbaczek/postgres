(in-package #:pg)

(defstruct (logical-slot-continuation-native-proof
            (:constructor %make-logical-slot-continuation-native-proof)
            (:print-function physical-libpq-print-private-object))
  ;; A completed, scoped observation, bound to its thread and live resources.
  ;; Its detached images and seal do not themselves authorize a slot write.
  (storage nil :read-only t) (scan nil :read-only t) (source nil :read-only t)
  (directory nil :read-only t) (slot-root nil :read-only t) (target-directory nil :read-only t)
  (binding nil :read-only t) (old-image nil :read-only t) (seal nil :read-only t)
  (horizon nil :read-only t) (target-lsn nil :read-only t) (kind nil :read-only t)
  (state nil :read-only t))

(defvar *logical-slot-continuation-native-proofs* nil)
(defvar *logical-slot-continuation-native-proof-identities*
  (make-hash-table :test #'eq :weakness :key :synchronized t))
(defvar *logical-slot-continuation-native-proof-test-hook* nil)

(defun logical-slot-continuation-native-proof-point (point &optional value)
  (when *logical-slot-continuation-native-proof-test-hook*
    (funcall *logical-slot-continuation-native-proof-test-hook* point value)))

(defun logical-slot-continuation-native-proof-result (snapshot name target found)
  ;; Require an established saved confirmation.  An unchanged target needs no
  ;; rediscovery; an advance must be a terminal batch found by the scan.
  ;; The proof seal is H = max(S, target), later bounded by captured horizon L.
  (logical-slot-continuation-check snapshot)
  (let* ((row (find name (getf snapshot :slots) :key #'first :test #'string=))
         (slot (and row (logical-slot-continuation-prepared-slot (second row) name)))
         (confirmed (and slot (wal-replication-slot-confirmed-flush-lsn slot)))
         (sealed (getf snapshot :sealed-end)))
    (unless (and confirmed (eq :reserved (wal-replication-slot-wal-status slot))
                 (typep target '(unsigned-byte 64)) (>= target confirmed)
                 (member found '(nil t)))
      (error "native proof requires its saved established confirmation"))
    (unless (or (= target confirmed) found)
      (ereport "22023" "replication slot acknowledgment must name a terminal WAL batch LSN"))
    (values (if (= target confirmed) :unchanged :advance)
            (if (<= target (first sealed))
                (copy-list sealed)
                (list target
                      (timeline-at-point
                       (1- target) (logical-slot-continuation-history
                                    (getf snapshot :history-target) (getf snapshot :history)))
                      :prefix-end)))))

(defun logical-slot-continuation-read-native-proof
    (storage facts scan source owner role target)
  ;; The callback builds a draft after draining and checking its cursor.
  ;; The observation wrapper finishes all postchecks before returning it.
  (call-with-logical-slot-continuation-runtime-observation
   storage facts scan source owner role
   (lambda (cursor actual-source snapshot image)
     (unless (eq source actual-source) (error "native proof changed its actual source"))
     (let ((found (logical-wal-cursor-terminal-p cursor target)))
       (loop while (logical-wal-cursor-next cursor))
       (sb-thread:with-mutex (*wal-lock*)
         (logical-wal-cursor-check-scope cursor)
         (unless (= (wal-retained-slot-scan-end scan) (logical-wal-cursor-position cursor))
           (error "native terminal observation did not finish its exact interval")))
       (multiple-value-bind (kind seal)
           (logical-slot-continuation-native-proof-result
            snapshot (wal-retained-slot-scan-name scan) target found)
         (logical-slot-continuation-native-proof-point :cursor-finished cursor)
         (list :kind kind :seal seal :image (copy-seq image)))))
   :batch-policy :logical-reader))

(defun call-with-logical-slot-continuation-native-resources (scan source function)
  ;; Hold the real terminal source, origin, storage, slot root, target directory
  ;; and independent fork pin throughout observation and receipt use.
  (unless (eq :terminal-check (logical-wal-source-purpose source))
    (error "native proof requires its terminal source"))
  (call-with-logical-slot-continuation-runtime-storage
   scan source
   (lambda (storage facts)
     (let ((directory (logical-slot-continuation-storage-call storage :directory))
           (binding (logical-slot-continuation-storage-call storage :binding))
           (horizon (logical-slot-continuation-storage-call storage :horizon)))
       (logical-slot-continuation-storage-call
        storage :with-slot-root
        (lambda (root root-check)
          (call-with-logical-slot-continuation-target-directory
           root (wal-retained-slot-scan-name scan) root-check
           (lambda (target target-check)
             (call-with-logical-slot-continuation-runtime-pin
              storage (getf (getf facts :certificate) :fork)
              (lambda (pin-check)
                (funcall function
                         (list :storage storage :facts facts :directory directory :binding binding :horizon horizon
                               :slot-root root :root-check root-check :target-directory target
                               :target-check target-check :pin-check pin-check))))))))))))

(defun call-with-logical-slot-continuation-native-proof-check (proof function)
  ;; Recheck the registered receipt and its live resources under WAL exclusion.
  ;; A nonlocal exit from its checked operation poisons the registered receipt.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (logical-slot-continuation-native-proof-p proof)
    (error "native terminal proof requires its completed receipt"))
  (let ((state (logical-slot-continuation-native-proof-state proof)))
    (unless (and (functionp state)
                 (eq state (gethash proof *logical-slot-continuation-native-proof-identities*))
                 (member proof *logical-slot-continuation-native-proofs* :test #'eq))
      (error "native terminal proof has no registered identity"))
    (funcall state :identity proof)
    (sb-sys:without-interrupts
      (let ((complete nil))
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (funcall state :check proof)
                   (funcall function))
               (setf complete t))
          (unless complete (funcall state :fail proof)))))))

(defun logical-slot-continuation-native-proof-check-under-lock (proof)
  (call-with-logical-slot-continuation-native-proof-check proof (lambda () proof)))

(defun logical-slot-continuation-native-proof-claim-under-lock (proof)
  ;; Claim once; this does not grant writer authority or replace fresh preflight.
  (call-with-logical-slot-continuation-native-proof-check
   proof (lambda () (funcall (logical-slot-continuation-native-proof-state proof) :claim proof))))

(defun call-with-logical-slot-continuation-native-proof (name target function &optional expected-owner)
  ;; Mint a point-in-time receipt only after the inner observation and all its
  ;; postchecks finish, while the outer resources remain live.  This does not
  ;; bypass ACK, admission or GC fences; a writer still needs fresh preflight.
  (unless (and (typep target '(unsigned-byte 64)) (functionp function))
    (error "native terminal proof requires a target and a callback"))
  (call-with-logical-slot-terminal-scan
   name target
   (lambda (slot)
     (wal-check-replication-slot-ack-under-lock slot name target expected-owner)
     (unless (and (wal-replication-slot-child-owner slot) (wal-replication-slot-logical-provenance slot))
       (error "native terminal proof requires its imported slot"))
     (values nil nil))
   (lambda (scan source owner role)
     (sb-thread:with-mutex (*wal-lock*)
       (unless (logical-slot-wal-start-proven-p
                (wal-retained-slot-scan-slot scan) (wal-retained-slot-scan-start scan) owner role)
         (error "native terminal start lacks its saved slot boundary")))
     (call-with-logical-slot-continuation-native-resources
      scan source
      (lambda (resources)
        (let* ((storage (getf resources :storage))
               (draft (logical-slot-continuation-read-native-proof
                       storage (getf resources :facts) scan source owner role target))
               (image (getf draft :image)) (seal (getf draft :seal))
               (binding (copy-seq (getf resources :binding)))
               (proof nil) (active t) (failed nil) (claimed nil)
               (thread sb-thread:*current-thread*) (mutex *wal-lock*))
          (unless (<= (first seal) (getf resources :horizon))
            (error "native terminal proof exceeds its captured durable horizon"))
          (logical-slot-continuation-native-proof-point :after-observation)
          (labels ((state (operation candidate)
                     (unless (and (eq candidate proof) (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*))
                       (error "native terminal proof changed identity, thread or WAL mutex"))
                     (ecase operation
                       (:identity t)
                       (:check
                        (unless (and active (not failed)
                                     (equalp image (logical-slot-continuation-native-proof-old-image proof))
                                     (equal seal (logical-slot-continuation-native-proof-seal proof))
                                     (equalp binding (logical-slot-continuation-native-proof-binding proof)))
                          (error "native terminal proof ended, changed or failed"))
                        (logical-slot-continuation-storage-call storage :check-under-lock)
                        (funcall (getf resources :pin-check))
                        (funcall (getf resources :root-check))
                        (funcall (getf resources :target-check))
                        (wal-retained-slot-scan-check-scope-under-lock scan)
                        (logical-wal-check-source source (wal-retained-slot-scan-end scan))
                        t)
                       (:claim
                        (unless (and active (not failed) (not claimed))
                          (error "native terminal proof was already claimed"))
                        (setf claimed t)
                        :claimed)
                       (:fail (setf failed t)))))
            (setf proof (%make-logical-slot-continuation-native-proof
                         :storage storage :scan scan :source source
                         :directory (getf resources :directory) :slot-root (getf resources :slot-root)
                         :target-directory (getf resources :target-directory)
                         :binding (copy-seq binding) :old-image (copy-seq image) :seal (copy-list seal)
                         :horizon (getf resources :horizon) :target-lsn target :kind (getf draft :kind)
                         :state #'state))
            (let ((*logical-slot-continuation-native-proofs*
                    (cons proof *logical-slot-continuation-native-proofs*)))
              (sb-sys:without-interrupts
                (unwind-protect
                     (progn
                       (setf (gethash proof *logical-slot-continuation-native-proof-identities*) #'state)
                       (sb-sys:with-local-interrupts
                         (logical-slot-continuation-storage-check storage)
                         (sb-thread:with-mutex (*wal-lock*)
                           (logical-slot-continuation-native-proof-check-under-lock proof))
                         (logical-slot-continuation-native-proof-point :receipt-registered proof)
                         (multiple-value-prog1 (funcall function proof)
                           (logical-slot-continuation-storage-check storage)
                           (sb-thread:with-mutex (*wal-lock*)
                             (logical-slot-continuation-native-proof-check-under-lock proof)))))
                  (setf active nil)
                  (remhash proof *logical-slot-continuation-native-proof-identities*)))))))))
   :end target))

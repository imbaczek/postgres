(in-package #:pg)

(defun logical-slot-imported-startup-inert-follow-p (config)
  ;; Parsed, source-free follow metadata may remain inert on this primary.
  ;; It is not historically certified and authorizes no follow or remote cleanup.
  ;; Leave passive unowned bootstrap debt in place without contacting its source.
  (let ((state (sixth config)))
    (and (physical-follow-no-source-p config)
         (or (physical-follow-cleanup-only-p config)
             (and (physical-follow-unattached-p config) (not (getf state :enabled))))
         (not (getf state :preparing)) (not (getf state :cleanup))
         (not (getf state :source)) (not (getf state :public-slot)))))

(defun logical-slot-imported-startup-check-policy ()
  ;; Historical recovery provenance is evidence, not a new recovery request.
  (physical-directory-check-reseed-intent *pgdata*)
  (physical-adoption-check-startup)
  (when (or (recovery-in-progress-p)
            (getf *physical-recovery-state* :standard-follow-managed-p)
            *physical-promotion-requested-p*)
    (error "imported primary startup conflicts with live recovery or promotion"))
  (dolist (name '("promote" "standby.signal" "recovery.signal" "physical-follow-managed.control"))
    (when (slot-state-lstat (merge-pathnames name *pgdata*))
      (error "imported primary startup conflicts with request ~a" name)))
  (when (slot-state-lstat (physical-follow-config-path))
    (unless (logical-slot-imported-startup-inert-follow-p (physical-follow-read-config))
      (error "imported primary startup requires inactive source-free follow metadata")))
  t)

(defun logical-slot-imported-startup-check-owned (expected)
  ;; Reobserve under published ownership before storage mutation.  The sticky
  ;; continuation fence prevents missing proof from selecting ordinary slot loading.
  (unless (and (eq :owned-pid (getf expected :phase)) (getf expected :inputs))
    (error "imported owned startup requires checked PID-boundary evidence"))
  (let ((expected (logical-slot-copy-clone-facts (getf expected :inputs))))
    (call-with-postmaster-ownership
     (lambda (ownership)
       (call-with-physical-adoption-startup-owner
        (lambda (owner)
          (call-with-physical-directory-root
           owner
           (lambda (root path)
             (declare (ignore path))
             (let ((mount (physical-adoption-install-mount-id nil :directory-fd root)))
               (flet ((check ()
                        (postmaster-check-ownership ownership)
                        (physical-adoption-check-owner owner)
                        (logical-slot-imported-startup-check-policy)
                        (physical-image-check-target *pgdata* root)
                        (unless (= mount
                                   (physical-adoption-install-mount-id *pgdata*)
                                   (physical-adoption-install-mount-id nil :directory-fd root)
                                   (physical-adoption-install-mount-id
                                    nil :directory-fd
                                    (sb-sys:fd-stream-fd *postmaster-data-lock*))
                                   (physical-adoption-install-mount-id
                                    nil :directory-fd
                                    (sb-sys:fd-stream-fd *postmaster-pid-stream*)))
                          (error "imported startup ownership crossed mounts"))
                        t))
                 (check)
                 (let ((inputs (logical-slot-imported-startup-inputs root #'check expected)))
                   (unless (physical-retained-fence-host-archive root)
                     (error "imported startup lost its continuation fence"))
                   (check)
                   inputs)))))))))))

(defun logical-slot-imported-own-startup (host port)
  ;; Compare observations across real PID publication, then reconcile again
  ;; under published ownership.  Return evidence for the pre-WAL recheck.
  (let ((before nil) (after nil))
    (logical-slot-imported-startup-check-policy)
    (postmaster-own-data-directory
     host port
     :startup-observer
     (lambda (lock phase)
       (logical-slot-imported-startup-check-policy)
       (ecase phase
         (:before-pid
          (wal-check-serving-namespace-before-pid)
          (setf before (logical-slot-imported-startup-observation lock phase)))
         (:owned-pid
          (setf after (logical-slot-imported-startup-observation lock phase before))))
       (logical-slot-imported-startup-check-policy))
     :startup-reconcile
     (lambda () (logical-slot-imported-startup-check-owned after)))
    after))

(defun logical-slot-imported-restore-primary-retained (root)
  "Restore the current primary control's root through the held host."
  (when (getf *wal-control-metadata* :retained-reference)
    (call-with-logical-slot-imported-archive-directory
     root
     (lambda (archive-fd facts)
       (declare (ignore facts))
       (let* ((fields (physical-promotion-restore-primary-retained
                       :expected-archive-key (physical-retained-archive-key archive-fd)))
              (id (getf (getf (seventh fields) :retained-reference) :root-id)))
         (when id (list (physical-promotion-retained-owner :primary) id)))))))

(defun call-with-logical-slot-imported-receive-retained (root proof function)
  "Restore the receive selection and call FUNCTION while holding spool ownership."
  (let ((path (slot-state-fd-path root "pg_physical_receive")))
    (with-slot-state-directory (receive-fd path t)
      (if (and receive-fd
               (slot-state-lstat (slot-state-fd-path receive-fd "receive.control")))
          (let* ((certificate (getf (logical-slot-copy-check-source-proof proof) :certificate))
                 (system-id (getf certificate :system-id))
                 (identity (physical-promotion-retained-identity
                            system-id (getf certificate :retained-reference))))
            (call-with-logical-slot-imported-archive-directory
             root
             (lambda (archive-fd facts)
               (declare (ignore facts))
               (let ((store nil))
                 (unwind-protect
                      (progn
                        (physical-store-open
                         (physical-image-fd-path receive-fd) system-id (getf identity :base-timeline)
                         :discard-tail nil :create-lock nil :retained-identity identity
                         :retained-directory (physical-image-fd-path archive-fd)
                         :expected-archive-key (physical-retained-archive-key archive-fd)
                         :on-open (lambda (opened) (setf store opened)))
                        (let ((snapshot (physical-store-control-snapshot store)))
                          (flet ((check ()
                                   (physical-retained-check-archive-binding
                                    (merge-pathnames "pg_physical_receive/" (physical-image-fd-path root))
                                    receive-fd)
                                   (physical-store-check-held-lock store)
                                   (physical-store-read-retained-control store snapshot)))
                            (check)
                            (let* ((id (physical-store-retained-root store))
                                   (selection (when id (list (physical-store-retained-owner store) id))))
                              (multiple-value-prog1 (funcall function selection #'check)
                                (check))))))
                   (when store (physical-store-close store)))))))
          (funcall function nil
                   (lambda ()
                     (when (slot-state-lstat (slot-state-fd-path root "pg_physical_receive/receive.control"))
                       (error "receive control appeared after observing its absence"))))))))

(defun logical-slot-imported-restore-startup-retained (gate ownership proof)
  "Restore current primary and surviving receive roots before slot loading."
  (call-with-logical-slot-imported-host-startup-owner
   gate ownership
   (lambda (owner)
     (call-with-physical-directory-root
      owner
      (lambda (root path)
        (declare (ignore path))
        (list (logical-slot-imported-restore-primary-retained root)
              (call-with-logical-slot-imported-receive-retained
               root proof (lambda (selection check) (funcall check) selection))))))))

(defun logical-slot-imported-check-unlink-controls (root proof primary origin-reference)
  "Require the durable primary and historical promotion selections at unlink."
  (let* ((metadata (let ((*pgdata* (physical-image-fd-path root)))
                     (nth-value 6 (wal-read-control-fields))))
         (id (getf (getf metadata :retained-reference) :root-id))
         (profile (logical-slot-imported-startup-recovery-profile root)))
    (unless (and (equalp metadata *wal-control-metadata*) (eql id (second primary)))
      (error "primary retained root changed during imported startup"))
    (when (eq profile :original)
      (let* ((facts (logical-slot-copy-check-source-proof proof))
             (expected (getf (getf facts :evidence) :control)))
        (unless (equal (getf (getf facts :certificate) :retained-reference) origin-reference)
          (error "historical promotion root differs from the restored origin"))
        (call-with-logical-slot-copy-source-proof-fd
         root "physical-recovery.control" nil
         (lambda (fd file-facts)
           (declare (ignore file-facts))
           (unless (equalp expected (logical-slot-copy-import-control-read-fd fd (length expected)))
             (error "historical promotion control changed before archive unlink"))))))
    (unless (eq profile (logical-slot-imported-startup-recovery-profile root))
      (error "historical promotion profile changed before archive unlink"))))

(defun logical-slot-imported-unlink-candidates (root archive-fd archive-key ownership proof primary reference deletion)
  "Collect only unprotected, canonical objects while holding the archive mutex."
  (let* ((directory (merge-pathnames "pg_physical_retained/" (physical-image-fd-path root)))
         (candidates (if (eq deletion :collect)
                         (physical-retained-root-collection-candidates directory archive-fd)
                         (list deletion))))
    (loop for (kind id) in candidates
          unless (physical-retained-root-protected-under-lock directory kind id archive-key)
            count (progn
                    (postmaster-check-ownership ownership)
                    (physical-image-check-target (postmaster-ownership-directory ownership) root)
                    (logical-slot-imported-check-unlink-controls root proof primary reference)
                    (physical-retained-root-unlink-under-lock directory archive-fd kind id))
              into removed
          finally (return (plusp removed)))))

(defun logical-slot-imported-check-startup-retained (gate ownership proof selections &optional deletion)
  "Verify the restored selections and optionally unlink one unprotected (kind id).
Keep postmaster and receive ownership through the archive operation."
  (destructuring-bind (primary expected-receive) selections
    (call-with-logical-slot-imported-host-startup-owner
     gate ownership
     (lambda (owner)
       (call-with-physical-directory-root
        owner
        (lambda (root path)
          (declare (ignore path))
          (call-with-logical-slot-imported-receive-retained
           root proof
           (lambda (receive check-receive)
             (unless (equal receive expected-receive)
               (error "receive root changed during imported startup"))
             (call-with-physical-image-owned-context
              root t
              (lambda (context)
                (let ((origin (logical-slot-continuation-storage-startup-origin owner context)))
                  (call-with-logical-slot-imported-archive-directory
                   root
                   (lambda (archive-fd facts)
                     (declare (ignore facts))
                     (let ((archive-key (physical-retained-archive-key archive-fd)))
                       (sb-thread:with-mutex (*wal-lock*)
                         (logical-slot-imported-host-startup-under-lock owner)
                         (funcall check-receive)
                         (multiple-value-bind (token reference)
                             (logical-slot-imported-host-origin-call origin :selection)
                           (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                             (physical-retained-check-required-roots-under-lock
                              archive-key (remove nil (list (list token (getf reference :root-id))
                                                            primary receive)))
                             (when deletion
                               (logical-slot-imported-unlink-candidates
                                root archive-fd archive-key ownership proof primary reference deletion)))))))))))))))))))

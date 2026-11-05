(in-package #:pg)

;;; Contact the source only after local pending intent is durable.

;; Capture the mapped follow image and actual attempt and install state.  Check
;; them again after network I/O, with no temporary awaiting local recovery.
(defun physical-reseed-follow-settlement-network-snapshot (owner &optional expected)
  (unless (physical-reseed-follow-owner-busy-p owner)
    (error "settlement network observation requires an active owner operation"))
  (call-with-physical-reseed-follow-image
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-installation-context owner))
            (record (physical-reseed-follow-installation-context-record context))
            (current (list :follow (physical-reseed-follow-installation-context-snapshot context)
                           :attempt (physical-reseed-follow-installation-context-attempt context)
                           :install (physical-reseed-follow-installation-context-install-state context))))
       (unless (and (eq :inert (getf (getf record :configuration) :phase))
                    (eq :handoff-pending (getf (getf record :settlement) :phase)))
         (error "network settlement requires durable pending intent on an inert installation"))
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
        "settlement temporary requires local recovery before network work")
       (physical-reseed-follow-read-at root "physical-reseed.follow" :stabilize t)
       (when (and expected (not (equal expected current)))
         (error "pending settlement or its actual attempt/install predecessor changed during network work"))
       (funcall check)
       current))))

;; Ask STATUS first using the frozen route and credentials.  A new HANDOFF also
;; requires an active attempt and a fresh source observation.  Keep the owner
;; busy through client close; no borrowed store or descriptor spans the RPC.
;; Copy the terminal receipt before client cleanup can change its input objects.
(defun physical-reseed-follow-settle-remote (owner &key helper)
  ;; STATUS can discover a historical terminal outcome even when future
  ;; capacity is short.  Check capacity only before a new HANDOFF can have an
  ;; effect on the source.
  (physical-reseed-follow-check-idle owner)
  (setf (physical-reseed-follow-owner-busy-p owner) t)
  (unwind-protect
       (let* ((snapshot (physical-reseed-follow-settlement-network-snapshot owner))
              (record (getf (getf snapshot :follow) :record))
              (attempt (getf record :attempt))
              (source (physical-follow-source-copy (getf attempt :source)))
              (generation (first (getf record :generations)))
              (active (eq :premanifest (getf (getf (getf snapshot :attempt) :record) :phase)))
              (request (physical-backup-lease-request-copy (getf attempt :request)))
              (successor (physical-backup-lease-successor-copy
                          (getf (getf record :settlement) :successor) request))
              (client nil) (receipt nil))
         (labels ((check ()
                    (physical-reseed-follow-settlement-network-snapshot owner snapshot))
                  (route ()
                    (physical-adoption-attempt-check-client client source)))
           (unwind-protect
                (let ((frozen (nth-value 1 (physical-follow-source-profile source))))
                  (setf client
                        (physical-libpq-connect-frozen
                         (or helper (physical-libpq-helper-path :on-spawn (lambda (actor) (setf client actor))))
                         frozen :on-spawn (lambda (actor) (setf client actor))))
                  (route)
                  (check)
                  (multiple-value-bind (action status)
                      (physical-reseed-follow-settlement-source-action
                       record (pg-client-native-backup-lease-status
                               client (physical-backup-lease-request-copy request)) active)
                    (check)
                    (route)
                    (case action
                      (:terminal (setf receipt status))
                      (:handoff
                       (progn
                         (physical-reseed-follow-check-future-capacity record)
                         (physical-reseed-follow-observe-source client source generation)
                         (check)
                         (multiple-value-bind (result confirmed)
                             (physical-reseed-follow-settlement-source-action
                              record (pg-client-native-backup-lease-handoff
                                      client (physical-backup-lease-request-copy request)
                                      (physical-backup-lease-successor-copy successor request))
                              active)
                           (unless (eq :terminal result)
                             (error "source handoff did not return the exact closed outcome"))
                           (setf receipt confirmed))
                         (check)
                         (route))))))
             (when client (physical-client-close client)))
           (check)
           (values receipt snapshot)))
    (setf (physical-reseed-follow-owner-busy-p owner) nil)))

(in-package #:pg)

;;; Durable controller configuration and bounded source retirement queue.
;;; Cleanup entries preserve the direct endpoint and exact pin identity across
;;; restart; they do not authenticate the remote server.

(defconstant +physical-follow-cleanup-capacity+ 64)

(defun physical-follow-endpoint-check (host port pin)
  (unless (and (stringp host) (<= 1 (length host) 255)
               (notany (lambda (char) (< (char-code char) 32)) host)
               (typep port '(integer 1 65535))
               (stringp pin) (wal-valid-replication-slot-name-p pin))
    (error "invalid native physical follow endpoint")))

(defun physical-follow-source-copy (source)
  (physical-follow-check-plist source '(:conninfo :fallback :owner))
  (let* ((profile (make-physical-libpq-profile (getf source :conninfo)
                                               :fallback (getf source :fallback)))
         (owner (physical-libpq-frozen-profile-from-datum (getf source :owner))))
    (list :conninfo (physical-libpq-profile-conninfo profile)
          :fallback (physical-libpq-profile-fallback profile)
          :owner (physical-libpq-frozen-profile-datum owner))))

(defun physical-follow-cleanup-copy (entry)
  "Check and copy a cleanup endpoint and its immutable request.
The tuple identifies a pin; it does not authenticate the remote endpoint."
  (physical-follow-check-plist
   entry (if (getf entry :connection) '(:kind :host :port :pin :request :connection)
             '(:kind :host :port :pin :request)))
  (physical-follow-endpoint-check (getf entry :host) (getf entry :port) (getf entry :pin))
  (let ((kind (getf entry :kind)))
    (unless (member kind '(:follow :bootstrap)) (error "invalid native cleanup kind"))
    (let ((result
            (list :kind kind
                  :host (copy-seq (getf entry :host))
                  :port (getf entry :port)
                  :pin (copy-seq (getf entry :pin))
                  :request (ecase kind
                             (:follow (physical-follow-request-copy (getf entry :request)))
                             (:bootstrap (physical-follow-bootstrap-request-copy
                                          (getf entry :request)))))))
      (when (getf entry :connection)
        (let ((profile (physical-libpq-frozen-profile-from-datum (getf entry :connection))))
          (multiple-value-bind (host port) (physical-libpq-frozen-display-endpoint profile)
            (setf (getf result :host) host (getf result :port) port))
          (setf result (append result (list :connection
                                            (physical-libpq-frozen-profile-datum profile))))))
      result)))

(defun physical-follow-cleanup-with-owner (entry source)
  "Attach the saved owner to a retirement entry, preserving the old credential."
  (physical-follow-cleanup-copy
   (append entry
           (when source
             (list :connection (getf (physical-follow-source-copy source) :owner))))))

(defun physical-follow-unowned-bootstrap-copy (record)
  "Check and copy the bounded reference to a bootstrap pin of unknown ownership.
It records debt but grants no authority to contact or retire a remote pin."
  (physical-follow-check-plist record '(:pin :request))
  (unless (and (stringp (getf record :pin))
               (wal-valid-replication-slot-name-p (getf record :pin)))
    (error "invalid unowned bootstrap pin"))
  (list :pin (copy-seq (getf record :pin))
        :request (physical-follow-bootstrap-request-copy (getf record :request))))

(defun physical-follow-unowned-bootstrap-from-base (base)
  "Build the unowned reference from verified base provenance."
  (physical-follow-unowned-bootstrap-copy
   (list :pin (getf base :source-pin)
         :request (list :version 1 :system-id (getf base :system-id)
                        :source-timeline (getf base :timeline) :base (getf base :end)
                        :manifest-digest (getf base :manifest-digest)))))

(defun physical-follow-public-slot-copy (slot)
  "Validate and copy a public slot name, or return NIL for no slot.
Names contain 1 to 63 lowercase ASCII letters, digits or underscores."
  (when slot
    (unless (and (stringp slot) (wal-valid-replication-slot-name-p slot))
      (error "invalid public physical replication slot"))
    (copy-seq slot)))

(defun physical-follow-controller-copy (state)
  "Check and copy durable controller state. A preparation reserves one queue slot.
KNOWN-TIMELINE is an optional nonzero 32-bit namespace bound, not replay state."
  (let ((keys '(:version :enabled :preparing :cleanup)))
    (dolist (key '(:source :mode :unowned-bootstrap :public-slot :known-timeline))
      (when (getf state key) (setf keys (append keys (list key)))))
    (physical-follow-check-plist state keys))
  (unless (and (eql 1 (getf state :version)) (member (getf state :enabled) '(nil t))
               (listp (getf state :cleanup))
               (member (getf state :mode) '(nil :standby-unattached))
               (or (null (getf state :known-timeline))
                   (typep (getf state :known-timeline) '(integer 1 4294967295))))
    (error "invalid native follow controller state"))
  (let* ((preparing (when (getf state :preparing)
                      (physical-follow-cleanup-copy (getf state :preparing))))
         (queue (mapcar #'physical-follow-cleanup-copy (getf state :cleanup))))
    (unless (and (or (null preparing) (eq :follow (getf preparing :kind)))
                 (<= (+ (length queue) (if preparing 1 0)) +physical-follow-cleanup-capacity+)
                 (= (length queue) (length (remove-duplicates queue :test #'equal))))
      (error "native follow cleanup capacity, preparation or duplicate entry is invalid"))
    (append (list :version 1 :enabled (getf state :enabled) :preparing preparing :cleanup queue)
            (when (getf state :known-timeline)
              (list :known-timeline (getf state :known-timeline)))
            (when (getf state :public-slot)
              (list :public-slot (physical-follow-public-slot-copy (getf state :public-slot))))
            (when (getf state :source)
              (list :source (physical-follow-source-copy (getf state :source))))
            (when (getf state :mode) (list :mode (getf state :mode)))
            (when (getf state :unowned-bootstrap)
              (list :unowned-bootstrap
                    (physical-follow-unowned-bootstrap-copy (getf state :unowned-bootstrap)))))))

(defun physical-follow-remember-timeline (state timeline)
  "Return a checked copy with the greatest known timeline ID preserved.
This is a namespace bound; it does not select ancestry or a replay position."
  (unless (typep timeline '(integer 1 4294967295))
    (error "invalid known follow timeline"))
  (let ((copy (physical-follow-controller-copy state)))
    (setf (getf copy :known-timeline) (max timeline (or (getf copy :known-timeline) 1)))
    copy))

(defun physical-follow-no-source-p (config)
  (and (listp config) (= 6 (length config)) (eql 1 (first config))
       (every #'null (subseq config 1 5))))

(defun physical-follow-unattached-p (config)
  "Recognize a standby with replay but no accepted source."
  (and (physical-follow-no-source-p config)
       (eq :standby-unattached (getf (sixth config) :mode))))

(defun physical-follow-cleanup-only-p (config)
  "Recognize primary cleanup state, distinct from an unattached standby."
  (and (physical-follow-no-source-p config)
       (not (getf (sixth config) :mode))))

(defun physical-follow-effective-config (config)
  (when (physical-follow-unattached-p config)
    (return-from physical-follow-effective-config
      (list :standby-unattached
            (physical-follow-unowned-bootstrap-copy (getf (sixth config) :unowned-bootstrap)))))
  (unless (physical-follow-no-source-p config)
    (let ((core (subseq config 0 (if (fifth config) 5 4)))
          (source (when (= 6 (length config)) (getf (sixth config) :source))))
      (append core
              (when source (list :source (physical-follow-source-copy source)))
              (when (getf (sixth config) :public-slot)
                (list :public-slot
                      (physical-follow-public-slot-copy (getf (sixth config) :public-slot))))))))

(defun physical-follow-controller-state (config)
  (if (= 6 (length config))
      (physical-follow-controller-copy (sixth config))
      (list :version 1 :enabled t :preparing nil :cleanup nil)))

(defun physical-follow-remember-config-timeline (config)
  "Preserve timeline IDs in the accepted anchor and checked saved requests.
Do this before removing a binding, preparation or cleanup entry.  Unaccepted
source discovery alone does not add a known timeline."
  (let* ((state (physical-follow-controller-state config))
         (anchor (getf (fifth config) :anchor))
         (known (getf state :known-timeline)))
    (when anchor
      (physical-follow-anchor-check anchor)
      (setf known (max (or known 1) (getf anchor :source-timeline))))
    (dolist (entry (append (when (getf state :preparing) (list (getf state :preparing)))
                           (getf state :cleanup)))
      (setf known (max (or known 1) (getf (getf entry :request) :source-timeline))))
    (if known (physical-follow-remember-timeline state known) state)))

(defun physical-follow-check-unowned-bootstrap-base (config base)
  "Require any unowned reference and preparation to agree with the verified base."
  (let* ((state (physical-follow-controller-state config))
         (record (getf state :unowned-bootstrap))
         (preparing (getf state :preparing)))
    (when record
      (unless (equal record (physical-follow-unowned-bootstrap-from-base base))
        (error "unowned bootstrap reference differs from the verified base"))
      (when preparing
        (let ((request (getf preparing :request)))
          (unless (and (= (getf request :system-id) (getf base :system-id))
                       (= (getf request :base-timeline) (getf base :timeline))
                       (= (getf request :base) (getf base :end))
                       (= (getf request :manifest-digest) (getf base :manifest-digest)))
            (error "unattached preparation differs from the verified base"))))))
  config)

(defun physical-follow-config-public-slot (config)
  "Return a validated copy of CONFIG's accepted public slot, if any."
  (physical-follow-public-slot-copy (getf (sixth config) :public-slot)))

(defun physical-follow-config-source (config)
  (when (= 6 (length config))
    (let ((source (getf (sixth config) :source)))
      (when source (physical-follow-source-copy source)))))

(defun physical-follow-controller-enabled-p (config)
  (and (not (physical-follow-no-source-p config))
       (getf (physical-follow-controller-state config) :enabled)))

(defun physical-follow-with-controller (config state)
  (append (if (physical-follow-no-source-p config) '(1 nil nil nil nil)
              (list 1 (second config) (third config) (fourth config) (copy-tree (fifth config))))
          (list (physical-follow-controller-copy state))))

(defun physical-follow-enqueue-cleanup (state entry)
  "Add a checked cleanup entry once, rejecting a queue above its fixed capacity."
  (let* ((copy (physical-follow-controller-copy state))
         (entry (physical-follow-cleanup-copy entry)))
    (unless (member entry (getf copy :cleanup) :test #'equal)
      (setf (getf copy :cleanup) (append (getf copy :cleanup) (list entry))))
    (physical-follow-controller-copy copy)))

(defun physical-follow-abandon-preparation-config (config)
  "Move an unfinished preparation into cleanup without losing its exact request."
  (let* ((state (physical-follow-controller-state config))
         (preparing (getf state :preparing)))
    (unless preparing (return-from physical-follow-abandon-preparation-config (copy-tree config)))
    (setf (getf state :preparing) nil)
    (physical-follow-with-controller config (physical-follow-enqueue-cleanup state preparing))))

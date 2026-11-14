(in-package #:pg)

;;; Observe a layout selected by detached journal bytes and validate its phase.
;;; These observations do not authorize publication.
(defun logical-slot-copy-install-name-index (names maximum)
  (logical-slot-copy-descriptor-list-count names maximum)
  (let ((index (make-hash-table :test #'equal)))
    (dolist (name names index)
      (unless (and (slot-state-valid-name-p name) (not (gethash name index)))
        (error "invalid logical installation inventory names"))
      (setf (gethash name index) t))))

(defun logical-slot-copy-install-layout-prefix (names source canonical)
  "Require each recorded row exactly once and return the installed prefix length."
  (unless (plusp (logical-slot-copy-descriptor-list-count names +logical-slot-copy-descriptor-maximum-slots+))
    (error "empty logical installation inventory"))
  (let ((expected (logical-slot-copy-install-name-index names +logical-slot-copy-descriptor-maximum-slots+))
        (staged (logical-slot-copy-install-name-index source +logical-slot-copy-descriptor-maximum-slots+))
        (installed (logical-slot-copy-install-name-index
                    canonical (+ +logical-slot-copy-descriptor-maximum-slots+ *max-replication-slots*)))
        (prefix 0) (suffix nil))
    (dolist (name source)
      (unless (gethash name expected) (error "foreign logical installation staging inventory")))
    (dolist (name names prefix)
      (let ((from (gethash name staged)) (to (gethash name installed)))
        (unless (and (or from to) (not (and from to)))
          (error "logical installation inventory must contain each row exactly once"))
        (if from (setf suffix t)
            (if suffix (error "logical installation rows do not form an installed prefix")
                (incf prefix)))))))

(defun logical-slot-copy-install-layout-pair (root container value active)
  "Determine certificate orientation from exact bytes and recorded inode identities."
  (let* ((canonical (logical-slot-copy-install-inspection-open root "logical-slot-copy.import" :file root))
         (other (logical-slot-copy-install-inspection-open container "active" :file root))
         (pending (getf value :certificate))
         (left (logical-slot-copy-install-inspection-read canonical +logical-slot-copy-import-maximum-bytes+))
         (right (logical-slot-copy-install-inspection-read other +logical-slot-copy-import-maximum-bytes+)))
    (prog1
        (cond
          ((and (equalp left pending) (equalp right active))
           (logical-slot-copy-install-inspection-identity canonical (getf value :pending-file))
           (logical-slot-copy-install-inspection-identity other (getf value :active-file))
           :pending)
          ((and (equalp left active) (equalp right pending))
           (logical-slot-copy-install-inspection-identity canonical (getf value :active-file))
           (logical-slot-copy-install-inspection-identity other (getf value :pending-file))
           :active)
          (t (error "logical installation certificate pair differs from its journal")))
      (logical-slot-copy-install-inspection-close other)
      (logical-slot-copy-install-inspection-close canonical))))

(defun logical-slot-copy-install-layout-row (parent slot-root row expected)
  "Check one P7 slot's recorded identities, inventory, and exact state bytes."
  (let* ((directory (logical-slot-copy-install-inspection-open parent (first row) :directory slot-root))
         (state (logical-slot-copy-install-inspection-open directory "state" :file slot-root)))
    (logical-slot-copy-install-inspection-identity directory (second expected))
    (logical-slot-copy-install-inspection-identity state (third expected))
    (unless (and (equal '("state") (logical-slot-copy-install-inspection-names directory 2))
                 (equalp (string->bytes (fourth row))
                         (logical-slot-copy-install-inspection-read state +slot-state-maximum-bytes+)))
      (error "logical installation P7 inventory or bytes differ from its journal"))
    (let ((facts (list (copy-seq (first row))
                       (physical-image-file-facts (logical-slot-copy-install-node-fd directory))
                       (physical-image-file-facts (logical-slot-copy-install-node-fd state)))))
      (logical-slot-copy-install-inspection-close state)
      (logical-slot-copy-install-inspection-close directory)
      facts)))

(defun logical-slot-copy-install-layout-physical (slot-root name)
  "Check and observe an extra physical slot and return its decoded disk image."
  (let* ((directory (logical-slot-copy-install-inspection-open slot-root name :directory slot-root))
         (state (logical-slot-copy-install-inspection-open directory "state" :file slot-root)))
    (unless (equal '("state") (logical-slot-copy-install-inspection-names directory 2))
      (error "logical installation physical slot has unresolved files"))
    (let* ((bytes (logical-slot-copy-install-inspection-read state +slot-state-maximum-bytes+))
           (slot (physical-slot-decode bytes name))
           (facts (list (copy-seq name) (length bytes) (logical-slot-copy-import-digest bytes)
                        (physical-image-file-facts (logical-slot-copy-install-node-fd directory))
                        (physical-image-file-facts (logical-slot-copy-install-node-fd state)))))
      (logical-slot-copy-install-inspection-close state)
      (logical-slot-copy-install-inspection-close directory)
      (values facts slot))))

(defun logical-slot-copy-install-layout-slots (root value certificate)
  "Check the P7 prefix and physical extras, counting all rows against capacity.
Recorded device and inode identities are checked with current mount groups;
historical mount IDs are not used as authority.  Return decoded physical slots
along with the prefix and observations."
  (let* ((slot-root (logical-slot-copy-install-inspection-open root "pg_replslot" :slot-root nil))
         (work (logical-slot-copy-install-inspection-open slot-root ".logical-slot-copy.work" :directory slot-root))
         (container (logical-slot-copy-install-inspection-open work (getf value :stage-id) :directory slot-root))
         (rows (getf certificate :slots)) (names (mapcar #'first rows))
         (source (logical-slot-copy-install-inspection-names container (1+ +logical-slot-copy-descriptor-maximum-slots+)))
         (canonical (remove ".logical-slot-copy.work"
                            (logical-slot-copy-install-inspection-names slot-root (+ 2 *max-replication-slots*))
                            :test #'equal))
         (prefix (logical-slot-copy-install-layout-prefix names source canonical))
         (physical (set-difference canonical names :test #'equal))
         (observations nil) (physical-slots nil))
    (logical-slot-copy-install-inspection-identity slot-root (getf value :slot-root))
    (logical-slot-copy-install-inspection-identity work (getf value :slot-work))
    (logical-slot-copy-install-inspection-identity container (getf value :slot-container))
    (when (> (+ (length physical) (length rows)) *max-replication-slots*)
      (error "logical installation exceeds max_replication_slots"))
    (loop for row in rows for expected in (getf value :entries) for position from 0
          do (push (logical-slot-copy-install-layout-row
                    (if (< position prefix) slot-root container) slot-root row expected)
                   observations))
    (dolist (name (sort physical #'string<))
      (multiple-value-bind (facts slot) (logical-slot-copy-install-layout-physical slot-root name)
        (push facts observations) (push slot physical-slots)))
    (values prefix (nreverse observations) (nreverse physical-slots))))

(defun logical-slot-copy-install-observe-layout (operation image)
  "Observe the layout selected by detached journal bytes under this operation.
Require the selected private or fixed container, exact journal and certificate
pair, and phase-compatible slot prefix.  A private stage must be pending with
no installed rows; an active certificate requires the full prefix.  The work
parent is checked as it exists now; the journal records no identity for it.  Other
workspace orphans remain opaque.  Reuse the certificate and active image from
this invocation's canonical decode; each observation still performs its own I/O
and checks, with no cache shared across checks."
  (logical-slot-copy-install-check-operation operation)
  (unless (and (typep image '(vector (unsigned-byte 8)))
               (<= 240 (length image) +logical-slot-copy-install-maximum-bytes+))
    (error "invalid logical installation layout image"))
  (let ((bytes (copy-seq image)))
    (multiple-value-bind (value certificate active) (logical-slot-copy-install-decode bytes)
      (call-with-logical-slot-copy-install-inspection
       operation
       (lambda (root)
         (let ((identity (logical-slot-copy-install-node-identity root)))
           (unless (and (= (first identity) (getf certificate :device))
                        (= (second identity) (getf certificate :inode)))
             (error "logical installation journal belongs to a different root")))
         (let* ((fixed (logical-slot-copy-install-inspection-open
                        root "logical-slot-copy.install" :directory root :missing t))
                (work (logical-slot-copy-install-inspection-open
                       root "logical-slot-copy.work" :directory root :missing t))
                (private (when work
                           (logical-slot-copy-install-inspection-open
                            work (getf value :stage-id) :directory root :missing t)))
                (container (or fixed private)))
           (unless (and container (not (and fixed private)))
             (error "logical installation container must have exactly one location"))
           (logical-slot-copy-install-inspection-identity container (getf value :container))
           (unless (equal '("active" "journal") (logical-slot-copy-install-inspection-names container 3))
             (error "logical installation journal container inventory changed"))
           (let ((journal (logical-slot-copy-install-inspection-open container "journal" :file root)))
             (logical-slot-copy-install-inspection-identity journal (getf value :journal))
             (unless (equalp bytes (logical-slot-copy-install-inspection-read
                                    journal +logical-slot-copy-install-maximum-bytes+))
               (error "logical installation journal differs from its selected image"))
             (logical-slot-copy-install-inspection-close journal))
           (let ((phase (logical-slot-copy-install-layout-pair root container value active)))
             (multiple-value-bind (prefix observations physical)
                 (logical-slot-copy-install-layout-slots root value certificate)
               (unless (and (or fixed (and (eq phase :pending) (zerop prefix)))
                            (or (eq phase :pending) (= prefix (length (getf value :entries)))))
                 (error "logical installation certificate phase differs from its installed prefix"))
               (list :phase (if fixed phase :private) :prefix prefix
                     :image bytes :observations observations :physical physical)))))))))

(defun logical-slot-copy-install-read-layout (operation image)
  "Read a selected layout without granting publication, source, or admission authority.
Failure revokes the operation even if caught and the layout is restored.  A
future typed publisher must establish its live capability or cold journal
authority, source proof, and physical slot eligibility before writing."
  (let ((record (logical-slot-copy-install-operation-record operation)) (completed nil))
    (unwind-protect
         (multiple-value-prog1 (logical-slot-copy-install-observe-layout operation image)
           (setf completed t))
      (unless completed
        (setf (logical-slot-copy-install-operation-state-failed record) t
              (physical-adoption-owner-active-p operation) nil)))))

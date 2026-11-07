(in-package #:pg)

;;; Classify structurally validated source pins and apply startup role and WAL
;;; policy before publishing loss or replacing the source pin registry.

;; A parent pin is bootstrap retention.  An allocated pin is cleanup debt,
;; a selected successor, or an unselected preparation.  A historical successor
;; without an allocation row is also selected; remaining pins are ordinary.
;; Classification alone does not establish usability.  The caller holds WAL
;; exclusion while using the returned owners.

(declaim (ftype (function (t t t t t) (values (sb-alien:alien sb-alien:int) &optional))
                physical-adoption-statx))

(defun physical-backup-lease-startup-wal-directory-state ()
  (let* ((context (and *physical-image-context*
                       (equal (pathname *pgdata*)
                              (physical-image-context-root-path *physical-image-context*))
                       *physical-image-context*))
         (fd (when context
               (physical-image-check-children context)
               (or (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal))
                   (error "startup image has no held WAL directory"))))
         (path (namestring (wal-directory))))
    (sb-alien:with-alien ((buffer (sb-alien:array sb-alien:unsigned-char 256)))
      (unless (zerop (physical-adoption-statx
                      (or fd -100) (if fd "" (string-right-trim "/" path))
                      (if fd #x1000 #x100) #x17ff
                      (sb-alien:cast (sb-alien:addr buffer) (* sb-alien:unsigned-char))))
        (error "cannot inspect startup WAL directory: errno ~d" (sb-alien:get-errno)))
      (let* ((sap (sb-alien:alien-sap (sb-alien:addr buffer)))
             (mode (sb-sys:sap-ref-16 sap #x1c))
             (uid (sb-sys:sap-ref-32 sap #x14)))
        (unless (and (= #x13cf (logand #x13cf (sb-sys:sap-ref-32 sap 0)))
                     (sb-posix:s-isdir mode) (= uid (sb-posix:geteuid))
                     (< (sb-sys:sap-ref-32 sap #x78) 1000000000)
                     (< (sb-sys:sap-ref-32 sap #x68) 1000000000))
          (error "startup WAL directory lacks owned ordinary directory facts"))
        (list (copy-seq path) (sb-sys:sap-ref-32 sap #x88) (sb-sys:sap-ref-32 sap #x8c)
              (sb-sys:sap-ref-64 sap #x20) mode uid
              (sb-sys:signed-sap-ref-64 sap #x70) (sb-sys:sap-ref-32 sap #x78)
              (sb-sys:signed-sap-ref-64 sap #x60) (sb-sys:sap-ref-32 sap #x68)
              (sb-sys:sap-ref-64 sap #x90))))))

(defun physical-backup-lease-startup-source-context ()
  ;; Capture the source facts observed during this startup plan.  Recheck them
  ;; before publishing loss and installing the replacement pin registry.
  (physical-backup-lease-require-locks)
  (list (physical-backup-lease-startup-wal-directory-state)
        (multiple-value-list (wal-read-control-fields))
        *wal-storage-incarnation* *wal-insert-timeline* *wal-segment-namespace*
        *wal-durable-lsn* *wal-end-lsn* *wal-checkpoint-lsn*
        (catalog-system-identifier) (copy-seq (catalog-cluster-identity *catalog*))))

(defun physical-backup-lease-check-startup-source-context (context)
  (when (and context (not (equal context (physical-backup-lease-startup-source-context))))
    (error "startup WAL namespace or source control context changed"))
  (values))

(defun physical-backup-lease-startup-selected-state (record pin standby context)
  ;; Classify availability after ownership has been checked.  A lost pin needs
  ;; no on-disk history for its old writer.  CLOSED standby pins need only saved fields.
  ;; A missing carrier or changed writer can later cause durable loss.
  (unless context (error "selected startup availability lacks its source context"))
  (cond
    ((null pin) :absent)
    ((physical-source-pin-lost-p pin) :lost)
    ((and standby (eq :closed (getf record :phase))) :closed-standby)
    ((/= (physical-source-pin-timeline pin) *wal-insert-timeline*) :writer-changed)
    (t
     (physical-follow-pin-check-source pin)
     (if (and standby
              (< (physical-source-pin-confirmed-lsn pin) (physical-follow-pin-end-lsn pin)))
         :deferred-role
         (handler-case
             (progn (physical-follow-pin-check-retained-wal pin) :usable)
           (wal-lineage-segment-unavailable (condition)
             (let ((segment (wal-lineage-unavailable-segment condition))
                   (reason (wal-lineage-unavailable-reason condition))
                   (end (physical-follow-pin-end-lsn pin))
                   (cursor (physical-source-pin-confirmed-lsn pin)))
               (unless (and (eq (wal-lineage-unavailable-namespace condition) *wal-segment-namespace*)
                            (member reason '(:missing :current-carrier))
                            (or (eq reason :missing)
                                (not (eq :retained-timeline
                                         (physical-follow-anchor-kind (physical-follow-pin-anchor pin)))))
                            (typep segment '(integer 0 *)) (< cursor end)
                            (<= (floor cursor +wal-segment-size+) segment
                                (floor (1- end) +wal-segment-size+)))
                 (error condition)))
             (physical-backup-lease-check-startup-source-context context)
             :missing-carrier))))))

(defun physical-backup-lease-check-startup-metadata (parents allocations)
  (physical-backup-lease-require-locks)
  (unless (and (= (hash-table-count parents) (hash-table-count *physical-backup-lease-records*))
               (= (hash-table-count allocations) (hash-table-count *physical-backup-lease-allocations*)))
    (error "backup lease startup owner inventory changed"))
  (maphash
   (lambda (name record)
     (declare (ignore name))
     (unless (equal record (physical-backup-lease-current-record (getf record :request)))
       (error "backup lease startup parent changed")))
   parents)
  (maphash
   (lambda (name row)
     (unless (equal row (gethash name *physical-backup-lease-allocations*))
       (error "backup lease startup allocation changed"))
     (physical-backup-lease-allocation-check-disk row))
   allocations)
  (values))

(defun physical-backup-lease-check-startup-loss (pin parent history)
  ;; Recheck the durable owner, allocation history and pin before saving loss.
  (unless (equal parent (physical-backup-lease-current-record (getf parent :request)))
    (error "backup lease pin loss parent changed"))
  (dolist (row history)
    (unless (equal row (gethash (getf row :name) *physical-backup-lease-allocations*))
      (error "backup lease pin loss allocation changed"))
    (physical-backup-lease-allocation-check-disk row))
  (unless (equalp (physical-source-pin-encode pin)
                  (physical-source-pin-encode
                   (physical-source-pin-read-retirement (physical-source-pin-name pin))))
    (error "backup lease startup pin changed before loss publication"))
  (values))

(defun physical-backup-lease-check-startup-pin-images (expected)
  (let ((current (physical-backup-lease-pin-inventory)))
    (unless (= (hash-table-count expected) (hash-table-count current))
      (error "backup lease startup pin inventory changed"))
    (maphash
     (lambda (name pin)
       (unless (and (gethash name current)
                    (equalp (physical-source-pin-encode pin)
                            (physical-source-pin-encode (gethash name current))))
         (error "backup lease startup pin image changed")))
     expected))
  (values))

(defun physical-backup-lease-startup-pin-class (name &optional (parents *physical-backup-lease-records*)
                                                       (allocations *physical-backup-lease-allocations*) successors)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "backup lease startup classification requires WAL exclusion"))
  (let ((parent (gethash name parents))
        (row (gethash name allocations)))
    (cond
      (parent (values :bootstrap parent parent))
      (row
       (let ((owner (gethash (getf (getf row :request) :name) parents)))
         (cond
           ((or (eq :canceled (getf row :state)) (eq :aborted (getf owner :outcome)))
            (values :cleanup row owner))
           ((and (eq :handed-off (getf owner :outcome))
                 (equal name (getf (getf owner :successor) :name)))
            (values :selected row owner))
           (t (values :preparation row owner)))))
      (t
       ;; Legacy handoffs have a selected successor but no allocation row.
       ;; Classify from the durable parent without inventing a row; WAL and
       ;; role policy still decide whether the pin is usable.
       (let ((successor-owner (gethash name (or successors (physical-backup-lease-successor-records)))))
         (if successor-owner
             (values :selected nil successor-owner)
             (values :ordinary nil nil)))))))

;; Apply role and WAL policy after structural graph validation.  A selected pin
;; is marked lost when its writer changes or a required carrier is unavailable.

(defun physical-backup-lease-startup-pin-candidate
    (pin standby states parents allocations successors)
  (multiple-value-bind (kind owner parent)
      (physical-backup-lease-startup-pin-class
       (physical-source-pin-name pin) parents allocations successors)
    (declare (ignore owner))
    (case kind
      ((:bootstrap :preparation)
       (cond
         ((and (eq kind :bootstrap) (eq :closing (getf parent :phase))
               (eq :handed-off (getf parent :outcome)))
          (unless (gethash (getf (getf parent :request) :name) states)
            (error "closing bootstrap lacks its selected startup decision"))
          (values pin nil t))
         ((physical-source-pin-lost-p pin) (values pin nil t))
         ((or standby (/= (physical-source-pin-timeline pin) *wal-insert-timeline*)
              (and (eq kind :bootstrap) (eq :closing (getf parent :phase))))
          (let ((lost (copy-structure pin)))
            (setf (physical-source-pin-lost-p lost) t)
            (values lost t t)))
         (t (physical-source-pin-check-wal pin) (values pin nil t))))
      (:cleanup (values pin nil t))
      (:selected
       (ecase (gethash (getf (getf parent :request) :name) states)
         ((:writer-changed :missing-carrier)
          (let ((lost (copy-structure pin)))
            (setf (physical-source-pin-lost-p lost) t)
            (values lost t (not standby))))
         ((:usable :lost :deferred-role :closed-standby)
          (values pin nil (not standby)))))
      (:ordinary
       (unless standby (physical-source-pin-check-wal pin))
       (values pin nil (not standby)))
      (t (error "unknown backup lease startup pin class")))))

(defun physical-backup-lease-check-startup-selected-sources
    (pins parents allocations histories successors standby context)
  (let ((states (make-hash-table :test #'equal)))
    (maphash
     (lambda (name record)
       (when (eq :handed-off (getf record :outcome))
         (let ((selected-name (getf (getf record :successor) :name)))
           (multiple-value-bind (presence pin coordinates row)
               (physical-backup-lease-selected-pin-observation
                record (gethash selected-name pins) parents allocations
                (gethash name histories) (gethash selected-name successors))
             (declare (ignore presence coordinates row))
             (setf (gethash name states)
                   (physical-backup-lease-startup-selected-state record pin standby context))))))
     parents)
    states))

(defun physical-backup-lease-startup-managed-pin-p (name)
  ;; Historical successors are managed even without an allocation row, so
  ;; standby startup removes any old registered retention for them.
  (or (gethash name *physical-backup-lease-records*)
      (gethash name *physical-backup-lease-allocations*)
      (gethash name (physical-backup-lease-successor-records))))

(defun physical-backup-lease-check-standby-registered-pins ()
  (maphash
   (lambda (name record)
     (let ((old (gethash name *physical-source-pins*)))
       (when old
         (when (eq :closed (getf record :phase))
           (error "closed backup lease still has a registered source pin"))
         (physical-backup-lease-check-pin-record old record))))
   *physical-backup-lease-records*)
  (values))

;; Build the complete replacement and loss-write lists before publication.
;; Standby keeps unmanaged registered pins and does not install ordinary or
;; selected disk pins as new primary retention.

(defun physical-backup-lease-startup-pin-plan (raw standby states parents allocations successors)
  (let ((candidates (make-hash-table :test #'equal))
        (invalidated nil))
    (when standby
      (maphash (lambda (name pin)
                 (unless (physical-backup-lease-startup-managed-pin-p name)
                   (setf (gethash name candidates) pin)))
               *physical-source-pins*))
    (maphash
     (lambda (name pin)
       (multiple-value-bind (candidate invalidate install)
           (physical-backup-lease-startup-pin-candidate
            pin standby states parents allocations successors)
         (when invalidate
           (multiple-value-bind (kind owner parent)
               (physical-backup-lease-startup-pin-class name parents allocations successors)
             (declare (ignore kind owner))
             (push (list pin candidate parent) invalidated)))
         (when install (setf (gethash name candidates) candidate))))
     raw)
    (values candidates invalidated)))

;; Replace the registry after loss writes.  On a primary, install retention for
;; each non-lost candidate; on a standby, preserve unmanaged retention entries.
;; The caller holds write and WAL exclusion and has started the publication fence.

(defun physical-backup-lease-install-startup-pins (candidates standby)
  (maphash
   (lambda (name pin)
     (when (or (not standby) (physical-backup-lease-startup-managed-pin-p name))
       (remhash pin *wal-snapshot-retention-pins*)))
   *physical-source-pins*)
  (unless standby
    (maphash (lambda (name pin)
               (declare (ignore name))
               (unless (physical-source-pin-lost-p pin)
                 (setf (gethash pin *wal-snapshot-retention-pins*)
                       (physical-source-pin-restart-lsn pin))))
             candidates))
  (setf *physical-source-pins* candidates)
  (values))

;; Validate the raw inventory and all selected closing sources before saving
;; any pin loss or replacing the registry.  On a standby, selected pins remain
;; disk-only and existing unmanaged entries are preserved without adding primary
;; retention.  The publication fence covers the disk and memory changes.

(defun physical-backup-lease-load-startup-pins (&optional standby)
  ;; Preflight every candidate and recheck the source context and disk images
  ;; before loss writes.  Install the replacement registry only after those writes.
  (when (and standby (not (recovery-in-progress-p)))
    (error "backup lease standby pin load requires recovery"))
  (with-write-lock
    (with-wal-publication-fence (begin-publication)
      (unless (and (zerop (hash-table-count *physical-source-senders*))
                   (zerop (hash-table-count *physical-backup-lease-capture-owners*)))
        (wal-invalid-input "cannot reload backup lease pins with active source owners"))
      (let ((raw (physical-backup-lease-pin-inventory)))
        (multiple-value-bind (histories successors parents allocations)
            (physical-backup-lease-allocation-check-pin-inventory
             *physical-backup-lease-records* *physical-backup-lease-allocations* raw)
          (when standby (physical-backup-lease-check-standby-registered-pins))
          (let* ((context (when (plusp (hash-table-count successors))
                            (physical-backup-lease-startup-source-context)))
                 (states (physical-backup-lease-check-startup-selected-sources
                          raw parents allocations histories successors standby context))
                 (expected (make-hash-table :test #'equal)))
            (maphash (lambda (name pin) (setf (gethash name expected) pin)) raw)
            (multiple-value-bind (candidates invalidated)
                (physical-backup-lease-startup-pin-plan
                 raw standby states parents allocations successors)
              (physical-backup-lease-check-startup-source-context context)
              (physical-backup-lease-check-startup-metadata parents allocations)
              (physical-backup-lease-check-startup-pin-images expected)
              (dolist (item invalidated)
                (destructuring-bind (before after parent) item
                  (declare (ignore after))
                  (physical-backup-lease-check-startup-loss
                   before parent (gethash (getf (getf parent :request) :name) histories))))
              (when (or (not standby) (plusp (hash-table-count parents))
                        (plusp (hash-table-count allocations)))
                (begin-publication)
                (dolist (item invalidated)
                  (destructuring-bind (before after parent) item
                    (physical-backup-lease-check-startup-source-context context)
                    (physical-backup-lease-check-startup-loss
                     before parent (gethash (getf (getf parent :request) :name) histories))
                    (physical-source-pin-save after)
                    (setf (gethash (physical-source-pin-name after) expected) after)))
                (physical-backup-lease-test-point :before-startup-pin-install nil)
                (physical-backup-lease-check-startup-source-context context)
                (physical-backup-lease-check-startup-metadata parents allocations)
                (physical-backup-lease-check-startup-pin-images expected)
                (physical-backup-lease-install-startup-pins candidates standby)
                (physical-backup-lease-test-point :after-startup-pin-install nil)
                (physical-backup-lease-check-startup-source-context context))))))))
  (values))

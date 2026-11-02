(in-package #:pg)

(defstruct physical-export-origin
  system-id timeline base manifest-digest proven-lsn prefix-digest)

(defconstant +physical-export-origin-maximum-bytes+ 1024)

(defun physical-export-origin-check (origin)
  (unless (and (physical-export-origin-p origin)
               (every (lambda (value) (typep value '(unsigned-byte 64)))
                      (list (physical-export-origin-system-id origin)
                            (physical-export-origin-manifest-digest origin)
                            (physical-export-origin-prefix-digest origin)))
               (typep (physical-export-origin-timeline origin) '(integer 1 4294967295))
               (typep (physical-export-origin-base origin) '(integer 32 18446744073709551615))
               (typep (physical-export-origin-proven-lsn origin) '(unsigned-byte 64))
               (<= (physical-export-origin-base origin) (physical-export-origin-proven-lsn origin))
               (or (< (physical-export-origin-base origin) (physical-export-origin-proven-lsn origin))
                   (= #xCBF29CE484222325 (physical-export-origin-prefix-digest origin))))
    (error "invalid physical export origin"))
  origin)

(defun physical-export-origin-identity (origin)
  (physical-export-origin-check origin)
  (list (physical-export-origin-system-id origin) (physical-export-origin-timeline origin)
        (physical-export-origin-base origin) (physical-export-origin-manifest-digest origin)))

(defun physical-export-origin-encode (origin)
  (physical-export-origin-check origin)
  (let* ((text (format nil "PGPHYSORIGIN1 ~d ~d ~d ~d ~d ~d"
                       (physical-export-origin-system-id origin)
                       (physical-export-origin-timeline origin)
                       (physical-export-origin-base origin)
                       (physical-export-origin-manifest-digest origin)
                       (physical-export-origin-proven-lsn origin)
                       (physical-export-origin-prefix-digest origin)))
         (crc (wal-crc32 (string->bytes text))))
    (string->bytes (format nil "~a ~8,'0X~%" text crc))))

(defun physical-export-origin-decode (bytes)
  (unless (<= 1 (length bytes) +physical-export-origin-maximum-bytes+)
    (error "invalid physical export origin size"))
  (with-input-from-string (input (bytes->string bytes))
    (let* ((line (read-line input nil nil))
           (fields (and line (wal-split-fields line))))
      (unless (and (= 8 (length fields)) (equal "PGPHYSORIGIN1" (first fields))
                   (null (read-line input nil nil)))
        (error "invalid physical export origin fields"))
      (destructuring-bind (system-id timeline base manifest proven digest)
          (mapcar #'parse-integer (subseq fields 1 7))
        (let ((origin (make-physical-export-origin
                       :system-id system-id :timeline timeline :base base
                       :manifest-digest manifest :proven-lsn proven :prefix-digest digest)))
          (unless (equalp bytes (physical-export-origin-encode origin))
            (error "noncanonical or corrupt physical export origin"))
          origin)))))

(defstruct physical-export-origin-entry origin)

(defstruct physical-export-origin-reservation
  backend thread system-id timeline namespace incarnation identity (state :reserved))

(defvar *physical-export-origins* (make-hash-table :test #'equal))

(defvar *physical-export-origin-reservations* (make-hash-table :test #'eq))

(defvar *physical-export-origin-limit* 1024)

(defvar *physical-export-origin-test-hook* nil)

(defun physical-export-origin-test-point (point origin)
  (when *physical-export-origin-test-hook*
    (funcall *physical-export-origin-test-hook* point origin)))

(defun physical-export-origin-directory ()
  (merge-pathnames "pg_physical_origins/" *pgdata*))

(defun physical-export-origin-path (origin)
  (physical-export-origin-check origin)
  (merge-pathnames
   (format nil "~16,'0X-~8,'0X-~16,'0X-~16,'0X.origin"
           (physical-export-origin-system-id origin) (physical-export-origin-timeline origin)
           (physical-export-origin-base origin) (physical-export-origin-manifest-digest origin))
   (physical-export-origin-directory)))

(defun physical-export-origin-save (origin)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "export origin publication requires WAL exclusion"))
  (physical-export-origin-test-point :before-save origin)
  (wal-ensure-directory-durable (physical-export-origin-directory))
  (wal-atomic-write-bytes (physical-export-origin-path origin)
                          (physical-export-origin-encode origin))
  (physical-export-origin-test-point :after-save origin))

(defun physical-export-origin-read (path &key stabilize)
  (let ((origin (physical-export-origin-decode
                 (read-bounded-regular-file path +physical-export-origin-maximum-bytes+
                                            :minimum 1 :stabilize stabilize))))
    (unless (equal (file-namestring path) (file-namestring (physical-export-origin-path origin)))
      (error "export origin pathname differs from its identity"))
    origin))

(defun physical-export-origin-load ()
  (with-wal-critical
    (unless (and (zerop (hash-table-count *physical-export-origin-reservations*))
                 (zerop (hash-table-count *physical-source-senders*)))
      (error "cannot load export origins with active export reservations"))
    (let ((paths (durable-state-file-paths (physical-export-origin-directory) "origin"))
          (candidates (make-hash-table :test #'equal)))
      (unless (and (typep *physical-export-origin-limit* '(integer 1))
                   (<= (length paths) *physical-export-origin-limit*))
        (error "export origin inventory exceeds its capacity"))
      (dolist (path paths)
        (let* ((origin (physical-export-origin-read path :stabilize t))
               (identity (physical-export-origin-identity origin)))
          (unless (= (physical-export-origin-system-id origin) (catalog-system-identifier))
            (error "export origin belongs to another system"))
          (when (and (not (recovery-in-progress-p))
                     (= *wal-insert-timeline* (physical-export-origin-timeline origin))
                     (> (physical-export-origin-proven-lsn origin) *wal-durable-lsn*))
            (error "export origin exceeds the primary durable endpoint"))
          (when (gethash identity candidates) (error "duplicate export origin"))
          (setf (gethash identity candidates) (make-physical-export-origin-entry :origin origin))))
      (setf *physical-export-origins* candidates)))
  (values))

(defun physical-export-origin-reserve ()
  (with-live-operation
    (prevent-recovery-primary-operation "native export origin reservation")
    (with-wal-critical
      (unless *backend* (wal-invalid-input "export origin reservation requires a backend"))
      (unless (typep *physical-export-origin-limit* '(integer 1))
        (wal-invalid-input "invalid export origin capacity"))
      (let ((pending (loop for token being the hash-keys of *physical-export-origin-reservations*
                           count (member (physical-export-origin-reservation-state token) '(:reserved :produced)))))
        (when (>= (+ pending (hash-table-count *physical-export-origins*)) *physical-export-origin-limit*)
          (wal-invalid-input "export origin capacity is exhausted; forget unused origins before exporting")))
      (let ((token (make-physical-export-origin-reservation
                    :backend *backend* :thread sb-thread:*current-thread*
                    :system-id (catalog-system-identifier) :timeline *wal-insert-timeline*
                    :namespace *wal-segment-namespace* :incarnation *wal-storage-incarnation*)))
        (sb-sys:without-interrupts
          (setf (gethash token *physical-export-origin-reservations*) t))
        (physical-export-origin-test-point :after-reserve token)
        token))))

(defun physical-export-origin-check-reservation (token)
  (unless (and (physical-export-origin-reservation-p token)
               (gethash token *physical-export-origin-reservations*)
               (eq *backend* (physical-export-origin-reservation-backend token))
               (eq sb-thread:*current-thread* (physical-export-origin-reservation-thread token))
               (= (catalog-system-identifier) (physical-export-origin-reservation-system-id token))
               (= *wal-insert-timeline* (physical-export-origin-reservation-timeline token))
               (eq *wal-segment-namespace* (physical-export-origin-reservation-namespace token))
               (eq *wal-storage-incarnation* (physical-export-origin-reservation-incarnation token))
               (not (recovery-in-progress-p)))
    (wal-invalid-input "export origin reservation no longer owns this source"))
  token)

(defun physical-export-origin-note-image (token manifest digest)
  (unless (and (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               *live-operation-quiescence-owner* (not *live-operation-admission-open*))
    (error "export origin capture requires owned quiescence"))
  (with-wal-critical
    (physical-export-origin-check-reservation token)
    (let ((identity (list (getf manifest :system-id) (getf manifest :timeline)
                          (getf manifest :end) digest)))
      (unless (and (eq :reserved (physical-export-origin-reservation-state token))
                   (null (physical-export-origin-reservation-identity token))
                   (typep digest '(unsigned-byte 64))
                   (eql (getf manifest :system-id) (catalog-system-identifier))
                   (eql (getf manifest :timeline) *wal-insert-timeline*)
                   (eql (getf manifest :checkpoint) *wal-checkpoint-lsn*)
                   (= *wal-checkpoint-lsn* *wal-durable-lsn*)
                   (eql (getf manifest :end) *wal-durable-lsn*))
        (wal-invalid-input "export origin capture differs from its produced image"))
      (sb-sys:without-interrupts
        (setf (physical-export-origin-reservation-identity token) identity
              (physical-export-origin-reservation-state token) :produced))))
  (values))

(defun physical-export-origin-release-reservation (token)
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*wal-lock*)
      (when (gethash token *physical-export-origin-reservations*)
        (unless (and (eq *backend* (physical-export-origin-reservation-backend token))
                     (eq sb-thread:*current-thread* (physical-export-origin-reservation-thread token)))
          (error "cannot release another export's reservation"))
        (setf (physical-export-origin-reservation-state token) :released)
        (remhash token *physical-export-origin-reservations*))))
  (values))

(defun call-with-physical-export-origin-reservation (function)
  (let ((token nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf token (physical-export-origin-reserve))
             (sb-sys:with-local-interrupts (funcall function token)))
        (when token (physical-export-origin-release-reservation token))))))

(defun physical-export-origin-install-under-lock (token origin begin-publication)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "export origin installation requires WAL exclusion"))
  (physical-export-origin-check-reservation token)
  (physical-export-origin-check origin)
  (let* ((identity (physical-export-origin-identity origin))
         (existing (gethash identity *physical-export-origins*)))
    (unless (and (= (physical-export-origin-system-id origin)
                    (physical-export-origin-reservation-system-id token))
                 (= (physical-export-origin-timeline origin)
                    (physical-export-origin-reservation-timeline token))
                 (= (physical-export-origin-base origin) (physical-export-origin-proven-lsn origin))
                 (member (physical-export-origin-reservation-state token) '(:produced :published))
                 (equal identity (physical-export-origin-reservation-identity token)))
      (wal-invalid-input "export origin does not match its completed export"))
    (unless existing
      (when (probe-file (physical-export-origin-path origin))
        (error "unregistered export origin exists on disk"))
      (funcall begin-publication)
      (physical-export-origin-save origin))
    (sb-sys:without-interrupts
      (unless existing
        (setf existing (make-physical-export-origin-entry :origin (copy-physical-export-origin origin))
              (gethash identity *physical-export-origins*) existing))
      (setf (physical-export-origin-reservation-identity token) identity
            (physical-export-origin-reservation-state token) :published))
    (copy-physical-export-origin (physical-export-origin-entry-origin existing))))

(defun physical-export-origin-initialize (token origin &optional preflight)
  (physical-export-origin-check origin)
  (with-live-operation
    (with-wal-publication-fence (begin-publication)
      (when preflight (funcall preflight))
      (physical-export-origin-install-under-lock token origin #'begin-publication))))

(defun physical-export-origin-capture-backup (directory manifest token)
  (multiple-value-bind (actual digest) (recovery-read-manifest directory)
    (unless (equal manifest actual) (error "produced export manifest changed"))
    (physical-export-origin-note-image token manifest digest))
  manifest)

(defun physical-export-origin-record-backup (directory manifest slot token)
  (multiple-value-bind (actual digest) (recovery-read-manifest directory)
    (unless (and (equal manifest actual)
                 (equal (list (getf actual :system-id) (getf actual :timeline)
                              (getf actual :end) digest)
                        (physical-export-origin-reservation-identity token)))
      (error "export origin differs from the produced manifest"))
    (let ((*pgdata* directory))
      (multiple-value-bind (state control) (physical-recovery-verify-base-state)
        (unless (and (zerop (getf control :phase))
                     (= (getf state :manifest-digest) digest))
          (error "export origin requires a fully verified immutable base")))
      (authentication-check-backup-policy directory))
    (physical-export-origin-initialize
     token (make-physical-export-origin
            :system-id (getf actual :system-id) :timeline (getf actual :timeline)
            :base (getf actual :end) :manifest-digest digest
            :proven-lsn (getf actual :end) :prefix-digest #xCBF29CE484222325)
     (lambda ()
       (physical-bootstrap-check-export-slot-under-wal-lock
        slot (wal-replication-slot-name slot) (physical-manifest-checkpoint-start actual))))))

(defun physical-export-origin-pin-identity (pin)
  (if (physical-follow-pin-p pin)
      (let ((anchor (physical-follow-pin-anchor pin)))
        (list (physical-source-pin-system-id pin) (getf anchor :base-timeline)
              (getf anchor :base) (getf anchor :manifest-digest)))
      (when (physical-source-pin-manifest-digest pin)
        (list (physical-source-pin-system-id pin) (physical-source-pin-timeline pin)
              (physical-source-pin-consistent-lsn pin) (physical-source-pin-manifest-digest pin)))))

(defun physical-export-origin-in-use-p (identity)
  (or (loop for token being the hash-keys of *physical-export-origin-reservations*
              thereis (equal identity (physical-export-origin-reservation-identity token)))
      (loop for owner being the hash-values of *physical-source-senders*
              thereis (and (physical-sender-p owner)
                           (equal identity (physical-export-origin-pin-identity (physical-sender-pin owner)))))))

(defun physical-export-origin-forget (identity)
  (with-live-operation
    (prevent-recovery-primary-operation "native export origin removal")
    (with-wal-publication-fence (begin-publication)
      (let ((entry (gethash identity *physical-export-origins*)))
        (unless entry (wal-invalid-input "export origin does not exist"))
        (when (physical-export-origin-in-use-p identity)
          (wal-invalid-input "export origin is in use"))
        (let ((origin (physical-export-origin-entry-origin entry)))
          (unless (equalp origin (physical-export-origin-read (physical-export-origin-path origin)))
            (error "export origin changed on disk"))
          (begin-publication)
          (delete-file (physical-export-origin-path origin))
          (fsync-directory (physical-export-origin-directory))
          (sb-sys:without-interrupts (remhash identity *physical-export-origins*))))))
  (values))

(defun physical-export-origin-list ()
  (sb-thread:with-mutex (*wal-lock*)
    (sort (loop for entry being the hash-values of *physical-export-origins*
                collect (copy-physical-export-origin (physical-export-origin-entry-origin entry)))
          #'string< :key (lambda (origin) (file-namestring (physical-export-origin-path origin))))))

(defstruct physical-export-origin-proof
  entry origin owner pin backend thread namespace incarnation requested verified-end digest)

(defun physical-export-origin-start-proof (name owner cursor requested)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "export origin proof capture requires WAL exclusion"))
  (when (physical-sender-p owner)
    (let* ((pin (physical-source-check-sender-under-lock name owner))
           (identity (physical-export-origin-pin-identity pin))
           (entry (gethash identity *physical-export-origins*))
           (origin (when entry (physical-export-origin-entry-origin entry))))
      (when (and origin
                 (= (physical-export-origin-timeline origin) *wal-insert-timeline*)
                 (= (physical-export-origin-system-id origin) (catalog-system-identifier))
                 (<= cursor (physical-export-origin-proven-lsn origin) requested))
        (make-physical-export-origin-proof
         :entry entry :origin (copy-physical-export-origin origin)
         :owner owner :pin pin :backend *backend* :thread sb-thread:*current-thread*
         :namespace *wal-segment-namespace* :incarnation *wal-storage-incarnation*
         :requested requested :verified-end (physical-export-origin-proven-lsn origin)
         :digest (physical-export-origin-prefix-digest origin))))))

(defun physical-export-origin-proof-record (proof cursor record raw)
  (when proof
    (let ((start (physical-export-origin-proven-lsn (physical-export-origin-proof-origin proof)))
          (end (wal-record-end record)))
      (when (< cursor start end)
        (wal-invalid-input "export origin floor is inside a source record or rotation gap"))
      (when (>= cursor start)
        (unless (= cursor (physical-export-origin-proof-verified-end proof))
          (wal-invalid-input "export origin proof omitted source records"))
        (let ((digest (physical-store-update-digest (physical-export-origin-proof-digest proof) raw)))
          (setf (physical-export-origin-proof-digest proof) digest
                (physical-export-origin-proof-verified-end proof) end)))))
  (values))

(defun physical-export-origin-publish-proof-under-lock (proof name owner endpoint begin-publication)
  (when proof
    (unless (sb-thread:holding-mutex-p *wal-lock*)
      (error "export origin proof publication requires WAL exclusion"))
    (let* ((pin (physical-source-check-sender-under-lock name owner))
           (origin (physical-export-origin-proof-origin proof))
           (identity (physical-export-origin-identity origin))
           (entry (gethash identity *physical-export-origins*)))
      (unless (and (eq entry (physical-export-origin-proof-entry proof))
                   (eq owner (physical-export-origin-proof-owner proof))
                   (eq pin (physical-export-origin-proof-pin proof))
                   (eq *backend* (physical-export-origin-proof-backend proof))
                   (eq sb-thread:*current-thread* (physical-export-origin-proof-thread proof))
                   (eq *wal-storage-incarnation* (physical-export-origin-proof-incarnation proof))
                   (eq *wal-segment-namespace* (physical-export-origin-proof-namespace proof))
                   (= *wal-insert-timeline* (physical-export-origin-timeline origin))
                   (= endpoint (physical-export-origin-proof-requested proof)
                      (physical-export-origin-proof-verified-end proof))
                   (<= endpoint *wal-durable-lsn*))
        (wal-invalid-input "export origin proof no longer owns its complete source range"))
      (let* ((current (physical-export-origin-entry-origin entry))
             (old (physical-export-origin-proven-lsn current))
             (start (physical-export-origin-proven-lsn origin))
             (digest (physical-export-origin-proof-digest proof)))
        (unless (and (equal identity (physical-export-origin-identity current))
                     (<= start old)
                     (or (> old start)
                         (= (physical-export-origin-prefix-digest current)
                            (physical-export-origin-prefix-digest origin))))
          (error "export origin generation changed its historical commitment"))
        (when (= old endpoint)
          (unless (= digest (physical-export-origin-prefix-digest current))
            (error "export origin proofs disagree at the same endpoint")))
        (when (< old endpoint)
          (let ((candidate (copy-physical-export-origin current)))
            (setf (physical-export-origin-proven-lsn candidate) endpoint
                  (physical-export-origin-prefix-digest candidate) digest)
            (funcall begin-publication)
            (physical-export-origin-save candidate)
            (setf (physical-export-origin-entry-origin entry) candidate)
            (physical-export-origin-test-point :after-progress-publication candidate))))))
  (values))

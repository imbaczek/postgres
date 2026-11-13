;;; The import model checks bounded canonical fields and internal consistency.
;;; It grants no file identity, digest authority or admission. A future owned
;;; phase 3 preparer must verify source bytes and regenerate PGSLOT7 images
;;; before publication.
(in-package #:pg)

;;; Maximum framed import control size in bytes.
(defconstant +logical-slot-copy-import-maximum-bytes+ (* 16 1024 1024))

(defun logical-slot-copy-import-child-image (source owner cause)
  "Build a child image from SOURCE with the supplied distinct OWNER and CAUSE.
Preserve provenance, profile, positions and any first terminal cause. OWNER
must differ from the source owner and generation; this does not mint authority."
  (unless (and (logical-slot-state-valid-p source)
               (wal-replication-slot-logical-provenance source)
               (typep owner '(unsigned-byte 128))
               (not (eql owner (wal-replication-slot-child-owner source)))
               (not (eql owner (logical-slot-provenance-generation
                                (wal-replication-slot-logical-provenance source))))
               (member cause '(:none :wal-removed :wal-level-insufficient))
               (or (eq :none (logical-slot-effective-invalidation-cause source))
                   (eq cause (logical-slot-effective-invalidation-cause source))))
    (error "invalid logical import child image identity or cause"))
  (let ((child (make-wal-replication-slot
                :name (wal-replication-slot-name source)
                :database-oid (wal-replication-slot-database-oid source)
                :restart-lsn (wal-replication-slot-restart-lsn source)
                :consistent-lsn (wal-replication-slot-consistent-lsn source)
                :confirmed-flush-lsn (wal-replication-slot-confirmed-flush-lsn source)
                :plugin (wal-replication-slot-plugin source)
                :logical-provenance (wal-replication-slot-logical-provenance source)
                :restart-timeline (wal-replication-slot-restart-timeline source)
                :confirmed-timeline (wal-replication-slot-confirmed-timeline source)
                :child-owner owner :invalidation-cause cause
                :wal-status (if (eq cause :none) :reserved :lost))))
    (wal-slot-set-initial-profile child (wal-slot-profile source))
    (logical-slot-state-encode child)))

(defun logical-slot-copy-import-check-row (row owner system-id fork phase)
  "Check a planned row against its owner, source identity and fork position.
A deferred row has a NIL image and is allowed only while pending."
  (unless (= 5 (logical-slot-copy-descriptor-list-count row 5))
    (error "invalid logical import slot row"))
  (destructuring-bind (name generation digest image disposition) row
    (let ((source-generation (logical-slot-parse-generation generation)))
      (unless (and (slot-state-valid-name-p name)
                   (typep digest '(unsigned-byte 64))
                   (not (eql source-generation owner))
                   (member disposition '(:eligible :wal-removed :wal-level-insufficient :deferred)))
        (error "invalid logical import slot identity"))
      (if (eq disposition :deferred)
          (unless (and (null image) (eq phase :pending))
            (error "deferred logical import slot cannot be active or installed"))
          (progn
            (unless (and (stringp image) (<= 1 (length image) +slot-state-maximum-bytes+)
                         (every (lambda (char) (< (char-code char) 128)) image))
              (error "invalid logical import planned image text"))
            (let* ((bytes (string->bytes image))
                   (slot (logical-slot-state-decode bytes name))
                   (provenance (wal-replication-slot-logical-provenance slot))
                   (cause (if (eq disposition :eligible) :none disposition)))
              (unless (and provenance
                           (eql owner (wal-replication-slot-child-owner slot))
                           (eql source-generation (logical-slot-provenance-generation provenance))
                           (eql system-id (logical-slot-provenance-system-id provenance))
                           (eq cause (logical-slot-effective-invalidation-cause slot))
                           (<= (wal-replication-slot-restart-lsn slot) fork)
                           (<= (wal-replication-slot-consistent-lsn slot) fork)
                           (or (null (wal-replication-slot-confirmed-flush-lsn slot))
                               (<= (wal-replication-slot-confirmed-flush-lsn slot) fork))
                           (equal image (bytes->string bytes)))
                (error "logical import planned image differs from its row")))))))
  row)

(defun logical-slot-copy-import-check-reference (value)
  "Check retained reference fields and range against VALUE without reading the archive."
  (let ((reference (getf value :retained-reference)))
    (if reference
        (unless (and (= 12 (logical-slot-copy-descriptor-list-count reference 12))
                     (equal (loop for (key data) on reference by #'cddr collect key)
                            '(:root-id :base-timeline :base :manifest-digest :end :end-digest))
                     (wal-retained-reference-valid-p reference)
                     (= (getf reference :base-timeline) (getf value :base-timeline))
                     (= (getf reference :manifest-digest) (getf value :manifest-digest))
                     (<= (getf reference :base) (getf value :base-end))
                     (= (getf reference :end) (getf value :fork)))
          (error "logical import retained reference differs from its frozen range"))
        (unless (= (getf value :base-end) (getf value :fork))
          (error "logical import requires its committed receive reference"))))
  value)

(defun logical-slot-copy-import-check (value)
  "Check bounded control fields and internal consistency, without granting admission.
The publisher must separately bind the actual descriptor and source slot bytes.
D is the copy base end, F the fork and H the initial child checkpoint end;
require D <= F < H. Active controls cannot contain deferred rows."
  (unless (and (= 44 (logical-slot-copy-descriptor-list-count value 44))
               (equal (loop for (key data) on value by #'cddr collect key)
                      '(:phase :copy-id :device :inode :manifest-digest :descriptor-digest
                        :base-proof-digest :system-id :base-end :base-timeline
                        :source-history-digest :selected-history-digest :promotion-generation
                        :parent-timeline :child-timeline :fork :child-end
                        :pending-control-digest :child-plan-digest :owner :retained-reference :slots))
               (member (getf value :phase) '(:pending :active))
               (every (lambda (key) (typep (getf value key) '(unsigned-byte 64)))
                      '(:device :inode :manifest-digest :descriptor-digest :base-proof-digest
                        :system-id :base-end :source-history-digest :selected-history-digest
                        :promotion-generation :fork :child-end :pending-control-digest :child-plan-digest))
               (every (lambda (key) (typep (getf value key) '(integer 1 4294967295)))
                      '(:base-timeline :parent-timeline :child-timeline))
               (<= (getf value :base-timeline) (getf value :parent-timeline))
               (< (getf value :parent-timeline) (getf value :child-timeline))
               (<= (getf value :base-end) (getf value :fork))
               (< (getf value :fork) (getf value :child-end)))
    (error "invalid logical import control fields"))
  (logical-slot-parse-generation (getf value :copy-id))
  (logical-slot-copy-import-check-reference value)
  (let ((owner (logical-slot-parse-generation (getf value :owner)))
        (previous nil))
    (unless (<= 1 (logical-slot-copy-descriptor-list-count
                   (getf value :slots) +logical-slot-copy-descriptor-maximum-slots+))
      (error "logical import control has no slots"))
    (dolist (row (getf value :slots))
      (logical-slot-copy-import-check-row
       row owner (getf value :system-id) (getf value :fork) (getf value :phase))
      (unless (or (null previous) (string< previous (first row)))
        (error "logical import slots are not strictly ordered"))
      (setf previous (first row))))
  value)

(defun logical-slot-copy-import-image (value)
  "Validate and frame a canonical bounded import control image."
  (logical-slot-copy-import-check value)
  (logical-slot-copy-literal-image
   "PGLOGICALIMPORT1" +logical-slot-copy-import-maximum-bytes+ value
   "logical import control"))

(defun logical-slot-copy-import-decode (bytes)
  "Decode a canonical bounded import control image and check its schema."
  (logical-slot-copy-literal-decode
   bytes "PGLOGICALIMPORT1" +logical-slot-copy-import-maximum-bytes+
   #'logical-slot-copy-import-image "logical import control"))

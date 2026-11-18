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
                :catalog-xmin (wal-replication-slot-catalog-xmin source)
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

(defun logical-slot-promotion-check-row (row system-id fork)
  "Validate an exact copied image and return its immutable generation."
  (unless (and (= 2 (logical-slot-copy-descriptor-list-count row 2))
               (slot-state-valid-name-p (first row))
               (stringp (second row))
               (<= 1 (length (second row)) +slot-state-maximum-bytes+))
    (error "invalid standby logical promotion row"))
  (let* ((bytes (string->bytes (second row)))
         (slot (wal-decode-replication-slot-image bytes (first row)))
         (provenance (wal-replication-slot-logical-provenance slot)))
    (unless (and (wal-logical-slot-p slot) provenance
                 (eql system-id (logical-slot-provenance-system-id provenance))
                 (null (wal-replication-slot-child-owner slot))
                 (eq :reserved (wal-replication-slot-wal-status slot))
                 (wal-replication-slot-confirmed-flush-lsn slot)
                 (running-xacts-normal-xid-p (wal-replication-slot-catalog-xmin slot))
                 (wal-logical-slot-covered-p slot fork)
                 (equalp bytes (wal-encode-replication-slot slot)))
      (error "standby logical promotion row differs from its fork or cluster"))
    (logical-slot-provenance-generation provenance)))

(defun logical-slot-promotion-evidence-image (value)
  "Frame slot images bound to the retained parent fork and assigned child timeline."
  (unless (and (= 12 (logical-slot-copy-descriptor-list-count value 12))
               (equal (loop for (key data) on value by #'cddr collect key)
                      '(:system-id :parent-timeline :child-timeline :fork :retained-reference :slots)))
    (error "invalid standby logical promotion evidence fields"))
  (destructuring-bind (&key system-id parent-timeline child-timeline fork retained-reference slots) value
    (unless (and (typep system-id '(unsigned-byte 64))
                 (typep fork '(unsigned-byte 64))
                 (typep parent-timeline '(integer 1 4294967295))
                 (typep child-timeline '(integer 1 4294967295))
                 (< parent-timeline child-timeline)
                 (= 12 (logical-slot-copy-descriptor-list-count retained-reference 12))
                 (equal (loop for (key data) on retained-reference by #'cddr collect key)
                        '(:root-id :base-timeline :base :manifest-digest :end :end-digest))
                 (wal-retained-reference-valid-p retained-reference)
                 (<= (getf retained-reference :base-timeline) parent-timeline)
                 (= (getf retained-reference :end) fork)
                 (<= 1 (logical-slot-copy-descriptor-list-count
                        slots +logical-slot-copy-descriptor-maximum-slots+)))
      (error "invalid standby logical promotion fork or inventory"))
    (let ((generations (make-hash-table :test #'eql))
          (previous nil))
      (dolist (row slots)
        (let ((generation (logical-slot-promotion-check-row row system-id fork)))
          (unless (and (or (null previous) (string< previous (first row)))
                       (not (gethash generation generations)))
            (error "duplicate or unordered standby logical promotion slot"))
          (setf (gethash generation generations) t
                previous (first row))))))
  (logical-slot-copy-literal-image "PGSTANDBYSLOTFORK" +logical-slot-copy-import-maximum-bytes+
                                   value "standby logical promotion evidence"))

(defun logical-slot-promotion-evidence-decode (bytes)
  "Decode a bounded canonical handoff image without granting slot authority."
  (logical-slot-copy-literal-decode bytes "PGSTANDBYSLOTFORK"
                                    +logical-slot-copy-import-maximum-bytes+
                                    #'logical-slot-promotion-evidence-image
                                    "standby logical promotion evidence"))

(defun logical-slot-promotion-evidence-path (digest)
  "Name a promotion evidence image by its digest in the owned data directory."
  (check-type digest (unsigned-byte 64))
  (merge-pathnames (format nil "global/standby-slot-fork-~16,'0X" digest) *pgdata*))

(defun logical-slot-promotion-publish-evidence (value)
  "Sync an evidence image before its digest can be committed in control."
  (let* ((bytes (logical-slot-promotion-evidence-image value))
         (digest (physical-store-update-digest #xCBF29CE484222325 bytes))
         (path (logical-slot-promotion-evidence-path digest)))
    (if (slot-state-lstat path)
        (progn
          (unless (equalp bytes (read-bounded-regular-file
                                 path +logical-slot-copy-import-maximum-bytes+ :stabilize t))
            (error "existing standby slot promotion evidence differs"))
          (fsync-directory (uiop:pathname-directory-pathname path)))
        (wal-atomic-write-bytes path bytes :mode #o600))
    digest))

(defun logical-slot-promotion-read-evidence (control)
  "Verify the evidence file named by pending control and return the bound image."
  (let* ((digest (getf control :logical-slot-evidence-digest))
         (bytes (read-bounded-regular-file
                 (logical-slot-promotion-evidence-path digest)
                 +logical-slot-copy-import-maximum-bytes+ :minimum 1)))
    (unless (= digest (physical-store-update-digest #xCBF29CE484222325 bytes))
      (error "standby slot promotion evidence digest differs from control"))
    (let ((value (logical-slot-promotion-evidence-decode bytes)))
      (unless (and (eql (getf value :system-id) (getf control :system-id))
                   (eql (getf value :parent-timeline)
                        (getf control :applied-timeline (getf control :timeline)))
                   (eql (getf value :child-timeline) (getf control :promotion-child))
                   (eql (getf value :fork) (getf control :applied))
                   (equal (getf value :retained-reference) (getf control :retained-reference)))
        (error "standby slot promotion evidence differs from its pending fork"))
      value)))

(defun logical-slot-promotion-read-committed-evidence (metadata timeline history)
  "Bind committed evidence to the child history and retained parent fork."
  (let* ((digest (getf metadata :logical-slot-evidence-digest))
         (reference (getf metadata :retained-reference))
         (fork (getf reference :end))
         (child (first history)))
    (unless (and digest reference (plusp fork)
                 (eql timeline (timeline-history-entry-timeline child))
                 (= fork (timeline-history-entry-begin child)))
      (error "committed logical slot evidence differs from the child history"))
    (logical-slot-promotion-read-evidence
     (list :logical-slot-evidence-digest digest :system-id (getf metadata :system-id)
           :timeline (logical-slot-prefix-timeline fork history) :promotion-child timeline
           :applied fork :retained-reference reference))))

(defun logical-slot-promotion-evidence-covers-slot-p (evidence slot)
  "Match immutable slot identity while allowing persisted progress to advance."
  (let* ((name (wal-replication-slot-name slot))
         (row (assoc name (getf evidence :slots) :test #'string=)))
    (when row
      (let ((initial (wal-decode-replication-slot-image (string->bytes (second row)) name))
            (confirmed (wal-replication-slot-confirmed-flush-lsn slot))
            (pin (wal-replication-slot-catalog-xmin slot)))
        (and (null (wal-replication-slot-child-owner slot))
             (logical-slot-provenance-fields-equal-p
              (wal-replication-slot-logical-provenance initial)
              (wal-replication-slot-logical-provenance slot))
             (eql (wal-replication-slot-consistent-lsn initial)
                  (wal-replication-slot-consistent-lsn slot))
             (eql (wal-replication-slot-database-oid initial) (wal-replication-slot-database-oid slot))
             (equal (wal-replication-slot-plugin initial) (wal-replication-slot-plugin slot))
             (eq (wal-slot-profile initial) (wal-slot-profile slot))
             (<= (wal-replication-slot-restart-lsn initial) (wal-replication-slot-restart-lsn slot))
             confirmed (<= (wal-replication-slot-confirmed-flush-lsn initial) confirmed)
             pin (<= (wal-replication-slot-catalog-xmin initial) pin))))))

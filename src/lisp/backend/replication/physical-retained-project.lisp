;;;; Project retained WAL onto a verified applied parent and fork.
;;;; The resulting archive root grants no replay, role or serving authority.
(in-package #:pg)

(defun physical-retained-project-history (source identity parent text fork)
  "Check the applied fork and project SOURCE's ancestry onto PARENT.

The parent history retains older intervals and leaves PARENT open-ended. The
caller supplies the independently verified fork."
  (timeline-check-lineage source)
  (let* ((history (timeline-check-lineage (timeline-parse-history parent text)))
         (suffix (member parent source :key #'timeline-history-entry-timeline))
         (base (getf identity :base)) (base-timeline (getf identity :base-timeline)))
    (unless (and (typep fork '(unsigned-byte 64)) (<= base fork)
                 suffix (= (length suffix) (length history))
                 (physical-store-history-extends-p history source)
                 (timeline-prefix-boundary-p base-timeline base source)
                 (timeline-prefix-boundary-p parent fork source)
                 (if (= fork base) (= parent base-timeline)
                     (= parent (timeline-at-point (1- fork) history))))
      (error "retained projection differs from applied parent ancestry or fork"))
    history))

(defun physical-retained-project-extent (image parent text history-digest end &optional checked-history)
  "Re-encode an extent through an exact record boundary on the parent history.

Choose the newest eligible segment carrier and trim bytes after END. This makes
an archive image; it does not assert that its bytes came from that carrier path.
CHECKED-HISTORY, when supplied, must already match PARENT and TEXT. Encoding
still validates the resulting metadata and records."
  (multiple-value-bind (metadata bytes) (physical-retained-extent-decode image)
    (unless (and (typep end '(unsigned-byte 64))
                 (< (getf metadata :start) end) (<= end (getf metadata :end)))
      (error "invalid retained extent projection endpoint"))
    (let* ((digest (nth-value 1 (physical-retained-extent-check metadata bytes :digest-cursor end)))
           (origin (* (getf metadata :segment) +wal-segment-size+))
           (history (or checked-history (timeline-check-lineage (timeline-parse-history parent text))))
           (carrier (first (timeline-segment-candidates (getf metadata :segment) history))))
      (unless digest (error "retained projection ends outside an exact record boundary"))
      (setf bytes (subseq bytes 0 (- end origin))
            (getf metadata :end) end (getf metadata :end-digest) digest
            (getf metadata :history-target) parent (getf metadata :history-text) text
            (getf metadata :history-digest) history-digest (getf metadata :file-timeline) carrier
            (getf metadata :prefix-digest) (physical-store-update-digest #xCBF29CE484222325 bytes))
      (values (physical-retained-extent-encode metadata bytes) metadata))))

(defun physical-retained-root-project-source (hold id history owner fork)
  "Return a fresh source index containing entries before FORK.

For a non-NIL ID, require its registered owner and copy all object protection
under the archive mutex before I/O. Validate the index and overlap and reread
selected images in full, including a crossing image's tail. Discarded images
rely on prior registration and immutable files. The source inventory is unchanged."
  (unless (or (null id) (typep id '(unsigned-byte 64)))
    (error "invalid retained projection source root"))
  (when id
    (let ((directory (physical-retained-root-hold-directory hold))
          (identity (physical-retained-root-hold-identity hold))
          (reference nil))
      (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
        (physical-retained-root-check-hold hold)
        (setf reference (gethash owner *physical-retained-root-references*))
        (unless (and owner reference (= id (physical-retained-root-reference-root-id reference))
                     (or (null (gethash owner *physical-retained-root-updates*))
                         (eq hold (gethash owner *physical-retained-root-updates*))))
          (error "retained projection requires its registered source owner and root"))
        (physical-retained-root-check-context reference directory identity)
        (physical-retained-root-add-objects (physical-retained-root-hold-objects hold)
                                            (physical-retained-root-reference-objects reference)))
      (physical-retained-root-test-point :after-project-source-reference hold)
      (let* ((index (physical-retained-inventory-read-index directory id identity history))
             (objects (physical-retained-root-reference-objects reference))
             (overlap (getf index :base-overlap))
             (entries (remove-if-not (lambda (entry) (< (getf (third entry) :start) fork))
                                     (getf index :entries))))
        (unless (and (gethash (cons :inventory id) objects)
                     (every (lambda (entry) (gethash (cons :extent (first entry)) objects))
                            (getf index :entries))
                     (or (null overlap) (gethash (cons :overlap (first overlap)) objects)))
          (error "retained projection index differs from its registered source reference"))
        (physical-retained-inventory-verify-entries
         entries history
         (lambda (image-id)
           (physical-retained-object-read directory :extent image-id +physical-retained-image-limit+))
         (physical-retained-inventory-read-overlap directory index))
        (setf (getf index :entries) entries)
        index))))

(defun physical-retained-root-project-overlap (hold source supplied)
  "Return a copy of a protected base overlap descriptor, rejecting conflicts."
  (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
    (physical-retained-root-check-hold hold)
    (let ((entry (physical-retained-root-merge-overlap
                  (physical-retained-root-held-overlap hold)
                  (physical-retained-root-merge-overlap (getf source :base-overlap) supplied))))
      (unless entry (error "retained projection lacks base overlap"))
      (physical-retained-inventory-check-overlap entry (physical-retained-root-hold-identity hold))
      (unless (gethash (cons :overlap (first entry)) (physical-retained-root-hold-objects hold))
        (error "retained projection base overlap was not reserved before reading"))
      (copy-tree entry))))

(defun physical-retained-root-project-prefix
    (hold source-id source-history parent parent-text parent-digest fork digest &key base-overlap source-owner)
  "Project a retained prefix to the independently verified applied FORK.

The caller must check durable source control against SOURCE-OWNER and SOURCE-ID,
and independently prove the applied fork, receive digest and parent selection.
The parent selection digest is NIL for the base timeline. Re-encode kept extents and
check metadata growth before staging. At the base, require the protected
overlap and fixed receive seed without reading extents. HOLD protects source
and candidate objects through later durable control selection and readback.
The returned root grants no replay, role or serving authority."
  (unless (and (stringp parent-text) (<= (length parent-text) +physical-retained-metadata-limit+))
    (error "retained projection parent history exceeds its bound"))
  (let* ((directory (physical-retained-root-hold-directory hold))
         (identity (physical-retained-root-hold-identity hold))
         (text (copy-seq parent-text))
         (history (physical-retained-project-history source-history identity parent text fork))
         (source (physical-retained-root-project-source hold source-id source-history source-owner fork))
         (overlap (physical-retained-root-project-overlap hold source base-overlap))
         (cursor (getf identity :base)) (current #xCBF29CE484222325)
         (entries nil) (count 0) (entry-bytes 0)
         (header (- (physical-retained-inventory-check-text-budget
                     (physical-retained-inventory-payload identity parent text nil overlap)) 3)))
    (unless (and overlap (typep digest '(unsigned-byte 64))
                 (if (= parent (getf identity :base-timeline)) (null parent-digest)
                     (typep parent-digest '(unsigned-byte 64))))
      (error "retained projection lacks base overlap or parent selection proof"))
    (dolist (entry (getf source :entries))
      (when (= cursor fork) (return))
      (let ((metadata (third entry)))
        (unless (and (= cursor (getf metadata :start))
                     (= current (getf metadata :start-digest)))
          (error "retained projection has a hole or receive digest discontinuity"))
        (multiple-value-bind (image projected)
            (physical-retained-project-extent
             (physical-retained-object-read directory :extent (first entry) +physical-retained-image-limit+)
             parent text parent-digest (min fork (getf metadata :end)) history)
          (let* ((id (physical-store-update-digest #xCBF29CE484222325 image))
                 (candidate (list id (length image) projected)))
            (incf count)
            (incf entry-bytes (physical-retained-inventory-check-text-budget candidate))
            (unless (and (<= count +physical-retained-inventory-count+)
                         (<= (+ header entry-bytes count 1) (- +physical-retained-inventory-limit+ 16)))
              (error "retained projection metadata exceeds its bound"))
            (push (physical-retained-root-stage-image hold parent text image) entries)
            (setf cursor (getf projected :end) current (getf projected :end-digest))))))
    (unless (and (= cursor fork) (= current digest))
      (error "retained projection lacks its exact applied endpoint or digest"))
    (setf entries (nreverse entries))
    (physical-store-check-retained-coverage
     (%make-physical-store :base-lsn (getf identity :base) :floor-lsn fork :floor-digest digest) entries)
    (physical-retained-root-publish-staged-entries hold parent text entries :base-overlap overlap)))

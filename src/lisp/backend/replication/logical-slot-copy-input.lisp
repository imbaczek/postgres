(in-package #:pg)

(defun logical-slot-copy-check-input (owner context)
  (physical-directory-check-owner owner)
  (physical-image-check-context context)
  (let ((stat (physical-image-check-directory (physical-image-context-root-fd context))))
    (unless (and (physical-image-read-context-p context)
                 (= (physical-adoption-owner-device owner) (sb-posix:stat-dev stat))
                 (= (physical-adoption-owner-inode owner) (sb-posix:stat-ino stat)))
      (error "logical copy input differs from its held offline root")))
  (physical-image-check-children context)
  t)

(defun logical-slot-copy-open-input-sidecar (owner context)
  (logical-slot-copy-check-input owner context)
  (when (assoc "logical_slot_wal" (physical-image-context-children context) :test #'equal)
    (error "logical copy input sidecar is already open"))
  (let ((fd nil) (root (physical-image-context-root-fd context)))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       root "logical_slot_wal"
                       (logior sb-posix:o-rdonly sb-posix:o-directory sb-posix:o-nofollow
                               +postmaster-o-cloexec+) 0))
             (when (minusp fd) (setf fd nil) (error "cannot open logical copy input sidecar"))
             (physical-image-check-directory fd (sb-posix:stat-dev (sb-posix:fstat root)))
             (push (cons "logical_slot_wal" fd) (physical-image-context-children context))
             (setf fd nil))
        (when fd (sb-posix:close fd)))))
  (logical-slot-copy-check-input owner context))

(defun logical-slot-copy-input-sidecar-inventory (context descriptor)
  (physical-image-check-children context)
  (let ((sidecar (cdr (assoc "logical_slot_wal" (physical-image-context-children context) :test #'equal))))
    (unless (and sidecar
                 (equal (sort (directory-entry-names (physical-image-fd-path sidecar)
                                                     :follow-symlinks t) #'string<)
                        (sort (loop for (timeline segment) in (logical-slot-copy-descriptor-wal-pieces descriptor)
                                    collect (wal-segment-filename segment timeline :timeline)) #'string<)))
      (error "logical copy input sidecar inventory differs from its descriptor")))
  t)

(defun logical-slot-copy-observe-pieces (context descriptor observations index)
  (dolist (piece (logical-slot-copy-descriptor-wal-pieces descriptor))
    (destructuring-bind (timeline segment size digest start end) piece
      (declare (ignore start end))
      (let ((actual (logical-slot-copy-file-row context (logical-slot-copy-piece-path timeline segment))))
        (unless (and (= size (second actual)) (= digest (third actual)))
          (error "logical copy WAL piece differs from its descriptor"))
        (setf (gethash (list timeline segment) index) (list piece actual))
        (push actual observations))))
  (values observations index))

(defun logical-slot-copy-input-empty-slot-root (context names directory)
  (when (member "pg_replslot" names :test #'equal)
    (slot-state-map
     (merge-pathnames "pg_replslot/" directory)
     (lambda (bytes name)
       (declare (ignore bytes name))
       (error "logical copy base input requires source slots to remain descriptor-only"))
     :parent-fd (physical-image-context-root-fd context)
     :cleanup nil :stabilize nil :local-workspace t :require-root t)
    (list "pg_replslot")))

(defun logical-slot-copy-input-inventory (owner context manifest descriptor)
  (logical-slot-copy-check-input owner context)
  (let* ((root (physical-image-context-root-path context))
         (files (getf manifest :files))
         (expected (append
                    (physical-adoption-owner-extra-root-names owner)
                    (logical-slot-copy-input-empty-slot-root
                     context (directory-entry-names root :follow-symlinks t)
                     (physical-adoption-owner-directory owner))
                    (list ".pg_lisp.lock" "base" "global" "pg_wal" "logical_slot_wal"
                          "recovery.required" "physical-recovery.control" "physical-backup.manifest"
                          "logical-slot-copy.descriptor" "pg_hba.conf" "pg_ident.conf"
                          "authentication.initialized")
                    (loop for (name) in files unless (find #\/ name) collect name))))
    (unless (equal (sort (physical-adoption-install-inventory-names
                          (directory-entry-names root :follow-symlinks t) t) #'string<)
                   (sort expected #'string<))
      (error "logical copy input root inventory differs from its completed image"))
    (logical-slot-copy-input-sidecar-inventory context descriptor)
    (let ((*physical-image-context* context))
      (recovery-check-image-directories manifest root)))
  t)

(defun logical-slot-copy-input-observe (owner context manifest descriptor)
  (logical-slot-copy-input-inventory owner context manifest descriptor)
  (let ((observations nil) (index (make-hash-table :test #'equal)))
    (dolist (row (getf manifest :files))
      (let ((actual (logical-slot-copy-file-row context (first row))))
        (unless (equal row (subseq actual 0 3))
          (error "logical copy immutable base file differs from its manifest"))
        (push actual observations)))
    (dolist (name '("physical-backup.manifest" "logical-slot-copy.descriptor" "recovery.required"
                    "physical-recovery.control" "pg_hba.conf" "pg_ident.conf" "authentication.initialized"))
      (push (logical-slot-copy-file-row context name) observations))
    (logical-slot-copy-observe-pieces context descriptor observations index)))

(defun logical-slot-copy-input-check-observations (owner context observations)
  (logical-slot-copy-check-input owner context)
  (dolist (row observations)
    (multiple-value-bind (parent leaf) (physical-image-parent context (first row))
      (unless (equal (fourth row) (physical-image-file-facts parent leaf))
        (error "logical copy input changed after verification"))))
  t)

(defun logical-slot-copy-input-segment-reader (owner context index)
  (lambda (segment carrier consume)
    (logical-slot-copy-check-input owner context)
    (let* ((entry (gethash (list carrier segment) index))
           (piece (first entry))
           (observation (second entry))
           (parent (cdr (assoc "logical_slot_wal" (physical-image-context-children context) :test #'equal)))
           (leaf (wal-segment-filename segment carrier :timeline)))
      (unless entry (error "logical copy is missing its selected WAL piece"))
      (call-with-physical-image-input-at
       parent leaf
       (lambda (stream)
         (let ((fd (sb-sys:fd-stream-fd stream)) (expected (fourth observation)))
           (unless (and (equal expected (physical-image-file-facts fd))
                        (equal expected (physical-image-file-facts parent leaf)))
             (error "logical copy selected WAL piece changed before reading"))
           (multiple-value-prog1 (funcall consume stream (third piece))
             (unless (and (equal expected (physical-image-file-facts fd))
                          (equal expected (physical-image-file-facts parent leaf)))
               (error "logical copy selected WAL piece changed during reading"))
             (logical-slot-copy-check-input owner context))))
       :element-type '(unsigned-byte 8)))))

(defun logical-slot-copy-check-checkpoint-bytes (bytes manifest descriptor)
  "Check the D record against manifest coordinates and copied control.
Require canonical snapshot payload when present; accept the legacy NIL snapshot.
This checks bytes without establishing file ownership."
  (let* ((fields (logical-slot-copy-control-fields
                  (logical-slot-copy-descriptor-base-control descriptor)))
         (record (wal-decode-record bytes (physical-manifest-checkpoint-start manifest)))
         (snapshot (wal-checkpoint-record-snapshot record)))
    (unless (and (= +wal-record-checkpoint+ (wal-record-type record))
                 (zerop (wal-record-xid record))
                 (= (getf manifest :end) (wal-record-end record))
                 (equalp (third fields) snapshot)
                 (or (null snapshot)
                     (equalp (wal-encode-checkpoint-snapshot snapshot) (wal-record-payload record))))
      (error "logical copy checkpoint record differs from its bound control"))
    bytes))

(defun logical-slot-copy-input-checkpoint (context manifest descriptor)
  (let* ((fields (logical-slot-copy-control-fields (logical-slot-copy-descriptor-base-control descriptor)))
         (start (physical-manifest-checkpoint-start manifest))
         (end (getf manifest :end))
         (leaf (wal-segment-filename (floor start +wal-segment-size+) (getf manifest :timeline) (sixth fields)))
         (parent (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal))))
    (call-with-physical-image-input-at
     parent leaf
     (lambda (stream)
       (let* ((size (sb-posix:stat-size (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
              (bytes (logical-wal-copy-record stream size start end)))
         (logical-slot-copy-check-checkpoint-bytes bytes manifest descriptor)))
     :element-type '(unsigned-byte 8))))

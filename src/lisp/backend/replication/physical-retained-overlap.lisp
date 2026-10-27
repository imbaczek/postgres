;;;; Retain the physical segment prefix ending at the immutable base.
;;;; At a segment boundary, this is the full preceding segment.
;;;; These bytes grant no logical replay coverage. Verified base provenance and
;;;; a durable inventory and control reference are required for their use.
(in-package #:pg)

(defun physical-retained-overlap-check-metadata (metadata)
  "Check the base identity, namespace, file entry and checkpoint boundary."
  (physical-follow-check-plist metadata
                               '(:version :system-id :base-timeline :base :manifest-digest
                                 :checkpoint-start :namespace :file))
  (let ((base (getf metadata :base)) (timeline (getf metadata :base-timeline))
                                     (namespace (getf metadata :namespace)) (entry (getf metadata :file))
                                     (start (getf metadata :checkpoint-start)))
    (unless (and (eql 1 (getf metadata :version))
                 (every (lambda (key) (typep (getf metadata key) '(unsigned-byte 64)))
                        '(:system-id :base :manifest-digest :checkpoint-start))
                 (<= 32 base) (typep timeline '(integer 1 4294967295))
                 (member namespace '(:legacy :timeline))
                 (or (= timeline 1) (eq namespace :timeline))
                 (listp entry) (= 3 (length entry))
                 (typep (second entry) '(unsigned-byte 64))
                 (typep (third entry) '(unsigned-byte 64)))
      (error "invalid retained base overlap metadata"))
    (let* ((segment (floor (1- base) +wal-segment-size+))
           (origin (* segment +wal-segment-size+))
           (name (concatenate 'string "pg_wal/" (wal-segment-filename segment timeline namespace))))
      (unless (and (<= origin start) (< start base)
                   (member (- base start) (list +wal-header-size+ (wal-checkpoint-record-size)))
                   (equal name (first entry)) (= (- base origin) (second entry)))
        (error "retained base overlap differs from its file evidence"))))
  metadata)

(defun physical-retained-overlap-check (metadata bytes)
  "Check overlap bytes against their file entry and checkpoint record.

Checksums alone do not authenticate provenance. Overlap supplies physical
prefix bytes, not logical replay coverage or a new receive seed."
  (physical-retained-overlap-check-metadata metadata)
  (let* ((entry (getf metadata :file)) (base (getf metadata :base))
                                       (origin (* (floor (1- base) +wal-segment-size+) +wal-segment-size+))
                                       (start (getf metadata :checkpoint-start)))
    (unless (and (typep bytes '(vector (unsigned-byte 8)))
                 (= (second entry) (length bytes))
                 (= (third entry) (physical-store-update-digest #xCBF29CE484222325 bytes)))
      (error "retained base overlap bytes differ from its file evidence"))
    (let* ((record (wal-decode-record (subseq bytes (- start origin)) start))
           (snapshot (wal-checkpoint-record-snapshot record)))
      (when (and snapshot (/= (getf metadata :base-timeline) (getf snapshot :timeline)))
        (error "retained base overlap checkpoint names another timeline"))))
  metadata)

(defun physical-retained-overlap-encode (metadata bytes)
  "Validate and encode a base overlap in a bounded envelope."
  (physical-retained-overlap-check metadata bytes)
  (physical-retained-envelope-encode #(80 71 82 79 86 76 48 49) metadata bytes))

(defun physical-retained-overlap-decode (image)
  "Decode an overlap envelope and validate its metadata and checkpoint."
  (multiple-value-bind (metadata bytes)
      (physical-retained-envelope-decode #(80 71 82 79 86 76 48 49) image)
    (physical-retained-overlap-check metadata bytes)
    (values metadata bytes)))

(defun physical-retained-overlap-capture (state directory)
  "Capture the physical segment prefix ending at the verified base checkpoint.

Recheck the exact manifest file and checkpoint. A selected durable inventory
and control reference must provide authority for later use of this object."
  (unless (eql (getf state :end) (getf state :checkpoint))
    (error "immutable base overlap requires its checkpoint end"))
  (multiple-value-bind (bytes entry) (physical-recovery-read-base-overlap state directory)
    (let ((metadata (list :version 1 :system-id (getf state :system-id)
                          :base-timeline (getf state :timeline) :base (getf state :end)
                          :manifest-digest (getf state :manifest-digest)
                          :checkpoint-start (physical-manifest-checkpoint-start state)
                          :namespace (getf state :wal-namespace :legacy) :file entry)))
      (physical-retained-overlap-check metadata bytes)
      (values metadata bytes))))

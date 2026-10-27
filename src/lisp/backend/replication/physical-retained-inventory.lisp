;;;; Immutable extent objects and inventory generations.
;;;; A durable control reference must select a generation before source bytes are removed.

(in-package #:pg)

(defconstant +physical-retained-inventory-limit+ (* 16 1024 1024))
(defconstant +physical-retained-inventory-count+ 65536)
(defvar *physical-retained-inventory-publish-lock* (sb-thread:make-mutex :name "retained WAL publication"))
(defvar *physical-retained-inventory-test-hook* nil)

(defun physical-retained-inventory-test-point (point id)
  "Call the private publication test hook, if installed."
  (when *physical-retained-inventory-test-hook*
    (funcall *physical-retained-inventory-test-hook* point id)))

(defun physical-retained-object-path (directory kind id)
  "Return the path for an immutable extent or inventory object."
  (unless (and (uiop:directory-pathname-p directory) (typep id '(unsigned-byte 64)))
    (error "invalid retained WAL object location"))
  (merge-pathnames (format nil "~a-~16,'0X.~a"
                           (ecase kind (:extent "extent") (:inventory "inventory") (:overlap "overlap"))
                           id (ecase kind (:extent "wal") (:inventory "index") (:overlap "wal")))
                   directory))

(defun physical-retained-object-read (directory kind id maximum)
  "Read a bounded object and check its content against ID.

The digest detects accidental corruption; it does not authenticate the object."
  (let ((bytes (read-bounded-regular-file (physical-retained-object-path directory kind id) maximum)))
    (unless (= id (physical-store-update-digest #xCBF29CE484222325 bytes))
      (error "retained WAL object differs from its immutable reference"))
    bytes))

(defun physical-retained-object-write (directory kind id path bytes)
  "Write and publish an object with file and directory sync test points."
  (flet ((mark (phase)
           (physical-retained-inventory-test-point (list kind phase) id)))
    (atomic-replace-file
     path (make-pathname :type "tmp" :defaults path) directory
     (lambda (stream) (write-sequence bytes stream))
     :element-type '(unsigned-byte 8)
     :fsync-function (lambda (fd)
                       (mark :before-file-fsync) (sb-posix:fsync fd) (mark :after-file-fsync))
     :rename-function (lambda (from to)
                        (mark :before-rename) (sb-posix:rename from to) (mark :after-rename))
     :fsync-directory-function (lambda (where)
                                 (mark :before-directory-fsync)
                                 (fsync-directory where)
                                 (mark :after-directory-fsync)))))

(defun physical-retained-object-publish (directory kind bytes maximum)
  "Publish immutable BYTES and return their durable content ID.

The caller has exclusive publication access.  A retry compares existing bytes and
syncs them; a collision is an error and leaves the existing file intact."
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (<= (length bytes) maximum))
    (error "invalid retained WAL object size"))
  (let* ((id (physical-store-update-digest #xCBF29CE484222325 bytes))
         (path (physical-retained-object-path directory kind id)))
    (wal-ensure-directory-durable directory)
    (if (wal-namespace-file-stat path t)
        (multiple-value-bind (stream size) (open-bounded-regular-file path maximum t)
          (unwind-protect
               (let ((existing (make-array size :element-type '(unsigned-byte 8))))
                 (unless (and (= size (read-sequence existing stream)) (equalp bytes existing))
                   (error "retained WAL immutable object collision"))
                 (fsync-stream stream))
            (close stream)))
        (physical-retained-object-write directory kind id path bytes))
    (fsync-directory directory)
    id))

(defun physical-retained-inventory-check-overlap (entry identity)
  "Check an overlap descriptor against the inventory's base identity."
  (unless (and (listp entry) (= 3 (length entry))
               (typep (first entry) '(unsigned-byte 64))
               (typep (second entry) '(unsigned-byte 64))
               (<= 20 (second entry) +physical-retained-image-limit+))
    (error "invalid retained base overlap reference"))
  (let ((metadata (physical-retained-overlap-check-metadata (third entry))))
    (unless (every (lambda (key) (eql (getf identity key) (getf metadata key)))
                   '(:system-id :base-timeline :base :manifest-digest))
      (error "retained base overlap has foreign identity")))
  entry)

(defun physical-retained-inventory-read-overlap (directory payload)
  "Read and validate the overlap named by an inventory, if present."
  (let ((entry (getf payload :base-overlap)))
    (when entry
      (physical-retained-inventory-check-overlap entry (getf payload :identity))
      (let ((image (physical-retained-object-read directory :overlap (first entry)
                                                  +physical-retained-image-limit+)))
        (unless (= (length image) (second entry))
          (error "retained base overlap reference length differs"))
        (multiple-value-bind (metadata bytes) (physical-retained-overlap-decode image)
          (unless (equal metadata (third entry))
            (error "retained base overlap differs from indexed metadata"))
          (list metadata bytes))))))

(defun physical-retained-inventory-payload (identity target text entries overlap)
  (append (list :version 1 :identity identity :history-target target :history-text text :entries entries)
          (when overlap (list :base-overlap overlap))))

(defun physical-retained-inventory-check (payload)
  "Check inventory identity, lineage and ordered extent metadata.

The supplied anchors still require independent base, receive or restartpoint
proof from the producer."
  (physical-follow-check-plist
   payload (append '(:version :identity :history-target :history-text :entries)
                   (when (member :base-overlap payload) '(:base-overlap))))
  (let* ((identity (getf payload :identity)) (target (getf payload :history-target))
                                             (text (getf payload :history-text)) (entries (getf payload :entries))
                                             (previous nil) (seen (make-hash-table :test #'eql)))
    (physical-follow-check-plist identity '(:system-id :base-timeline :base :manifest-digest))
    (unless (and (eql 1 (getf payload :version))
                 (every (lambda (key) (typep (getf identity key) '(unsigned-byte 64)))
                        '(:system-id :base :manifest-digest))
                 (typep (getf identity :base-timeline) '(integer 1 4294967295))
                 (typep target '(integer 1 4294967295)) (stringp text)
                 (<= (length text) +physical-retained-metadata-limit+)
                 (listp entries) (<= (length entries) +physical-retained-inventory-count+))
      (error "invalid retained WAL inventory fields"))
    (when (member :base-overlap payload)
      (physical-retained-inventory-check-overlap (getf payload :base-overlap) identity))
    (let ((history (timeline-check-lineage (timeline-parse-history target text))))
      (unless (timeline-prefix-boundary-p (getf identity :base-timeline) (getf identity :base) history)
        (error "retained WAL inventory has foreign base ancestry"))
      (dolist (entry entries)
        (unless (and (listp entry) (= 3 (length entry))
                     (typep (first entry) '(unsigned-byte 64))
                     (typep (second entry) '(unsigned-byte 64))
                     (<= 20 (second entry) +physical-retained-image-limit+)
                     (not (gethash (first entry) seen)))
          (error "invalid or duplicate retained WAL inventory entry"))
        (setf (gethash (first entry) seen) t)
        (let ((metadata (third entry)))
          (unless (and (every (lambda (key) (eql (getf identity key) (getf metadata key)))
                              '(:system-id :base-timeline :base :manifest-digest))
                       (physical-store-history-extends-p (physical-retained-extent-history metadata) history))
            (error "retained WAL inventory entry has foreign provenance"))
          (when previous
            (unless (<= (getf previous :end) (getf metadata :start))
              (error "retained WAL inventory entries overlap or are unordered"))
            (when (= (getf previous :end) (getf metadata :start))
              (unless (= (getf previous :end-digest) (getf metadata :start-digest))
                (error "retained WAL inventory digest chain disagrees"))))
          (setf previous metadata)))
      history)))

(defun physical-retained-inventory-check-text-budget
    (payload &optional (maximum (- +physical-retained-inventory-limit+ 16)))
  "Bound the encoded UTF-8 literal before semantic checks and printing.
Reject oversized integers and excessive nesting before rendering them."
  (let ((count 0))
    (labels ((add (size)
               (incf count size)
               (when (> count maximum) (error "retained WAL inventory text budget exceeded")))
             (visit (value depth)
               (when (> depth 16) (error "retained WAL literal nesting exceeds its bound"))
               (typecase value
                 (string
                  (add 2)
                  (loop for char across value for code = (char-code char)
                        do (add (cond ((or (char= char #\") (char= char #\\)) 2)
                                      ((< code 128) 1) ((< code 2048) 2)
                                      ((< code 65536) 3) (t 4)))))
                 (cons
                  (add 2)
                  (loop for item in value for first = t then nil
                        do (unless first (add 1)) (visit item (1+ depth))))
                 (integer
                  (unless (typep value '(unsigned-byte 64))
                    (error "retained WAL literal integer exceeds its bound"))
                  (add (length (format nil "~d" value))))
                 (null (add 3))
                 (symbol
                  (unless (keywordp value) (error "unsupported retained WAL literal symbol"))
                  (add (1+ (length (symbol-name value)))))
                 (t (error "unsupported retained WAL literal value")))))
      (visit payload 0))
    count))

(defun physical-retained-inventory-image (payload)
  "Validate and encode a bounded inventory with a CRC envelope."
  (physical-retained-inventory-check-text-budget payload)
  (physical-retained-inventory-check payload)
  (let* ((text (string->bytes (physical-bootstrap-manifest-text payload)))
         (size (+ 16 (length text))))
    (unless (<= size +physical-retained-inventory-limit+)
      (error "retained WAL inventory exceeds its encoded bound"))
    (let ((image (make-array size :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace image #(80 71 82 73 78 86 48 49))
      (wal-set-u32 image 8 (length text))
      (replace image text :start1 12)
      (wal-set-u32 image (- size 4) (wal-crc32 image))
      image)))

(defun physical-retained-inventory-decode (image)
  "Decode and validate a fresh inventory payload.

The CRC detects accidental corruption; it does not authenticate the payload."
  (unless (and (typep image '(vector (unsigned-byte 8)))
               (<= 16 (length image) +physical-retained-inventory-limit+)
               (equalp (subseq image 0 8) #(80 71 82 73 78 86 48 49))
               (= (wal-u32 image 8) (- (length image) 16)))
    (error "invalid retained WAL inventory envelope"))
  (let ((copy (copy-seq image)) (offset (- (length image) 4)))
    (wal-set-u32 copy offset 0)
    (unless (= (wal-u32 image offset) (wal-crc32 copy))
      (error "retained WAL inventory checksum mismatch"))
    (let ((payload (recovery-parse-manifest-text (bytes->string (subseq image 12 offset)))))
      (physical-retained-inventory-check payload)
      payload)))

(defun physical-retained-inventory-read-index (directory id identity history)
  "Read exactly ID and check identity and lineage, without opening extent files."
  (physical-follow-check-plist identity '(:system-id :base-timeline :base :manifest-digest))
  (timeline-check-lineage history)
  (let ((payload (physical-retained-inventory-decode
                  (physical-retained-object-read directory :inventory id +physical-retained-inventory-limit+))))
    (unless (and (every (lambda (key) (eql (getf identity key) (getf (getf payload :identity) key)))
                        '(:system-id :base-timeline :base :manifest-digest))
                 (physical-store-history-extends-p (physical-retained-inventory-check payload) history))
      (error "retained WAL inventory differs from required identity or history"))
    payload))

(defun physical-retained-inventory-verify-entries (entries history loader &optional overlap)
  "Validate checked, ordered entries using bounded LOADER reads.
Keep only the current segment's shared prefix between images."
  (let ((segment (when overlap (floor (1- (getf (first overlap) :base)) +wal-segment-size+)))
        (prefix (second overlap)))
    (dolist (entry entries)
      (let ((image (funcall loader (first entry))))
        (unless (and (= (length image) (second entry))
                     (= (first entry) (physical-store-update-digest #xCBF29CE484222325 image)))
          (error "retained WAL inventory image reference differs"))
        (multiple-value-bind (metadata bytes) (physical-retained-extent-decode image)
          (unless (equal metadata (third entry))
            (error "retained WAL image differs from indexed metadata"))
          (physical-retained-extent-scan metadata bytes history)
          (unless (eql segment (getf metadata :segment))
            (setf segment (getf metadata :segment) prefix nil))
          (when (and prefix
                     (let ((size (min (length prefix) (length bytes))))
                       (mismatch prefix bytes :end1 size :end2 size)))
            (error "retained WAL inventory has conflicting segment prefixes"))
          (when (or (null prefix) (> (length bytes) (length prefix)))
            (setf prefix bytes))))))
  t)

(defun physical-retained-inventory-load (directory id identity history)
  "Read an inventory and validate all referenced extents sequentially.

Return the freshly decoded payload."
  (let ((payload (physical-retained-inventory-read-index directory id identity history)))
    (physical-retained-inventory-verify-entries
     (getf payload :entries) history
     (lambda (image-id)
       (physical-retained-object-read directory :extent image-id +physical-retained-image-limit+))
     (physical-retained-inventory-read-overlap directory payload))
    payload))

(defun physical-retained-inventory-keep-entries (entries ids)
  "Select unique IDs from the previous inventory, preserving entry order."
  (unless (listp ids) (error "retained WAL kept IDs must be a list"))
  (let ((available (make-hash-table :test #'eql)) (selected (make-hash-table :test #'eql)))
    (dolist (entry entries) (setf (gethash (first entry) available) t))
    (loop for id in ids for count from 1
          do (unless (and (<= count +physical-retained-inventory-count+)
                          (typep id '(unsigned-byte 64))
                          (gethash id available) (not (gethash id selected)))
               (error "retained WAL kept ID is foreign, duplicate or exceeds its bound"))
             (setf (gethash id selected) t))
    (loop for entry in entries when (gethash (first entry) selected) collect entry)))

(defun physical-retained-inventory-publish-entries-under-lock (directory identity target text entries protect &key base-overlap)
  "Validate staged entries and publish one inventory under archive exclusion.

Check identity, history, images, overlap and shared prefixes before calling
PROTECT for the root and entries. Return the synced ID and freshly decoded
payload. The overlap does not add logical coverage."
  (unless (sb-thread:holding-mutex-p *physical-retained-inventory-publish-lock*)
    (error "retained WAL publication requires archive exclusion"))
  (let* ((image (physical-retained-inventory-image
                 (physical-retained-inventory-payload identity target text entries base-overlap)))
         (payload (physical-retained-inventory-decode image))
         (history (physical-retained-inventory-check payload)))
    (physical-retained-inventory-verify-entries
     (getf payload :entries) history
     (lambda (id) (physical-retained-object-read directory :extent id +physical-retained-image-limit+))
     (physical-retained-inventory-read-overlap directory payload))
    (funcall protect (physical-store-update-digest #xCBF29CE484222325 image) (getf payload :entries))
    (let ((id (physical-retained-object-publish directory :inventory image +physical-retained-inventory-limit+)))
      (physical-retained-inventory-test-point :after-inventory-publication id)
      (values id payload))))

(defun physical-retained-inventory-publish-under-lock
    (directory identity target text images &key previous (keep-ids nil keep-supplied-p) protect base-overlap)
  "Publish an inventory under the archive mutex.

When KEEP-IDS is supplied, retain only those previous entries; an explicit NIL
keeps none. Otherwise retain all previous entries. Call PROTECT with the candidate
ID and entries before writing objects. The caller owns control selection."
  (unless (sb-thread:holding-mutex-p *physical-retained-inventory-publish-lock*)
    (error "retained WAL publication requires archive exclusion"))
  (physical-retained-wal-check-budget images)
  (let* ((images (mapcar #'copy-seq images))
         (history (timeline-check-lineage (timeline-parse-history target text)))
         (old (when previous (physical-retained-inventory-load directory previous identity history)))
         (new (make-hash-table :test #'eql))
         (entries
           (append (if keep-supplied-p
                       (physical-retained-inventory-keep-entries (getf old :entries) keep-ids)
                       (copy-list (getf old :entries)))
                   (loop for image in images
                         for id = (physical-store-update-digest #xCBF29CE484222325 image)
                         collect (let ((metadata (physical-retained-extent-decode image)))
                                   (setf (gethash id new) image)
                                   (list id (length image) metadata)))))
         (payload (physical-retained-inventory-payload
                   identity target text
                   (sort entries #'< :key (lambda (entry) (getf (third entry) :start)))
                   (or (getf old :base-overlap) base-overlap)))
         (root-image (physical-retained-inventory-image payload)))
    (when (and (getf old :base-overlap) base-overlap
               (not (equal (getf old :base-overlap) base-overlap)))
      (error "retained base overlap cannot change within one base identity"))
    (physical-retained-inventory-verify-entries
     (getf payload :entries) history
     (lambda (id)
       (or (gethash id new)
           (physical-retained-object-read directory :extent id +physical-retained-image-limit+)))
     (physical-retained-inventory-read-overlap directory payload))
    (when protect
      (funcall protect (physical-store-update-digest #xCBF29CE484222325 root-image) (getf payload :entries)))
    (dolist (image images)
      (let ((id (physical-retained-object-publish directory :extent image +physical-retained-image-limit+)))
        (physical-retained-inventory-test-point :after-extent-publication id)))
    (let ((id (physical-retained-object-publish directory :inventory root-image +physical-retained-inventory-limit+)))
      (physical-retained-inventory-test-point :after-inventory-publication id)
      id)))

(defun physical-retained-inventory-publish
    (directory identity target text images &key previous (keep-ids nil keep-supplied-p))
  "Publish immutable extents and an inventory generation; return its synced ID.

Copy new images after checking the candidate budget.  If KEEP-IDS is supplied,
retain only those entries from PREVIOUS; an explicit NIL retains none.  The
caller must durably reference the returned ID in control before deletion can be
considered.  Cleanup must protect selected and scoped references and in-flight
publishers through control selection; this mutex alone does not cover that
interval.  Supplied base and receive anchors are not independently established."
  (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
    (apply #'physical-retained-inventory-publish-under-lock directory identity target text images
           :previous previous (when keep-supplied-p (list :keep-ids keep-ids)))))

(defun physical-retained-inventory-plan (directory id identity history start end)
  "Plan coverage from candidates selected by the index before image I/O.

Enforce the candidate budget before loading images.  Return freshly decoded
plan data; the plan does not grant serving authority."
  (unless (and (typep start '(unsigned-byte 64)) (typep end '(unsigned-byte 64)) (< start end))
    (error "invalid retained WAL inventory request"))
  (let* ((payload (physical-retained-inventory-read-index directory id identity history))
         (entries (remove-if-not (lambda (entry)
                                   (let ((metadata (third entry)))
                                     (and (< (getf metadata :start) end) (< start (getf metadata :end)))))
                                 (getf payload :entries))))
    (unless (and (<= (length entries) +physical-retained-plan-maximum-images+)
                 (<= (loop for entry in entries sum (second entry)) +physical-retained-plan-maximum-bytes+))
      (error "retained WAL candidate batch exceeds its budget"))
    (physical-retained-wal-plan
     (loop for entry in entries
           collect (let ((image (physical-retained-object-read
                                 directory :extent (first entry) +physical-retained-image-limit+)))
                     (unless (= (length image) (second entry))
                       (error "retained WAL candidate length differs from inventory"))
                     (unless (equal (physical-retained-extent-decode image) (third entry))
                       (error "retained WAL candidate metadata differs from inventory"))
                     image))
     identity history start end)))

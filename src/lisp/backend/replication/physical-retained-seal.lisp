;;;; Seal receive history before moving its durable floor.
;;;; Store ownership pins WAL and history while a local hold protects staged files.
;;;; Publish the inventory and restartpoint before selecting receive control.

(in-package #:pg)

(defstruct (physical-retained-seal (:constructor %make-physical-retained-seal))
  "State for sealing a receive prefix while the caller owns the store mutex.

ENTRIES are in reverse order, with the latest at the head for append and
coalescing. The scan keeps a constant number of extent buffers and a bounded
index; it validates the final inventory before selection."
  store hold history target text entries latest cursor digest prefix prefix-segment
  segment timeline start start-digest replacement bytes
  (entry-count 0) (entry-bytes 0) header-bytes)

(defun physical-retained-seal-initial-prefix (store entries)
  "Obtain the floor's physical prefix from the selected root or verified base.

Do not reconstruct it from raw bytes below the receive floor. The caller rejects unrooted
floors beyond the base, including empty segment boundaries."
  (let ((floor (physical-store-floor-lsn store)))
    (cond
      ((zerop (mod floor +wal-segment-size+)) (make-array 0 :element-type '(unsigned-byte 8)))
      ((physical-store-retained-root store)
       (let ((entry (car (last entries))))
         (unless (and entry (= floor (getf (third entry) :end)))
           (error "selected retained root lacks its exact floor prefix"))
         (nth-value 1
                    (physical-retained-extent-decode
                     (physical-retained-object-read (physical-store-retained-directory store)
                                                    :extent (first entry) +physical-retained-image-limit+)))))
      ((= floor (physical-store-base-lsn store))
       (unless (equal (physical-store-retained-identity store) (physical-recovery-retained-identity))
         (error "initial retained prefix requires verified recovery base provenance"))
       (physical-recovery-base-wal-prefix))
      (t (error "compacted receive history lacks retained provenance; reseed required")))))

(defun physical-retained-seal-append (seal raw)
  "Append a record to the current bounded segment image."
  (let* ((bytes (physical-retained-seal-bytes seal))
         (used (length bytes)) (needed (+ used (length raw))))
    (unless (<= needed +wal-segment-size+) (error "retained segment exceeds its bound"))
    (when (> needed (array-dimension bytes 0))
      (setf bytes (adjust-array bytes (min +wal-segment-size+ (max needed (* 2 (array-dimension bytes 0)))))
            (physical-retained-seal-bytes seal) bytes))
    (setf (fill-pointer bytes) needed)
    (replace bytes raw :start1 used)))

(defun physical-store-archive-base-prefix (store)
  "Read the verified base prefix when the recovery identity matches STORE.
Return NIL when it is unavailable or cannot be verified."
  (handler-case
      (when (and *physical-recovery-state*
                 (equal (physical-store-retained-identity store)
                        (physical-recovery-retained-identity)))
        (physical-recovery-base-wal-prefix))
    (error () nil)))

(defun physical-store-live-archive-segment (store timeline segment)
  "Build a closed segment from verified retained and live receive data under the store lock.
Require coverage from the segment start or matching base prefix and the requested
timeline. Return the image and end only after closure is proved."
  (with-physical-store (store)
    (let* ((floor (physical-store-floor-lsn store))
           (first (floor floor +wal-segment-size+))
           (root (physical-store-retained-root store))
           (directory (physical-store-retained-directory store))
           (identity (physical-store-retained-identity store))
           (history (physical-store-retained-history store))
           (base (physical-store-base-lsn store))
           (interior-base (and (= segment (floor base +wal-segment-size+))
                               (plusp (mod base +wal-segment-size+)))))
      (when root
        (physical-store-read-retained-control store (physical-store-control-tuple store)))
      (when (< segment first)
        (return-from physical-store-live-archive-segment
          (when root (physical-retained-archive-segment
                      directory root identity history timeline segment
                      :base-prefix-reader (lambda () (physical-store-archive-base-prefix store))))))
      (let* ((inventory
               (when (and (= segment first) root)
                 (let ((verified (physical-retained-inventory-load directory root identity history)))
                   (physical-store-check-retained-coverage store (getf verified :entries))
                   verified)))
             (base-prefix (when (and interior-base (not (getf inventory :base-overlap)))
                            (physical-store-archive-base-prefix store)))
             (prefix
               (when (= segment first)
                 (if (and interior-base (not root))
                     (or base-prefix (return-from physical-store-live-archive-segment nil))
                     (physical-retained-seal-initial-prefix store (getf inventory :entries)))))
             (bytes (make-array (max 4096 (length prefix)) :element-type '(unsigned-byte 8)
                                                           :adjustable t :fill-pointer (length prefix)))
             (seal (%make-physical-retained-seal :bytes bytes))
             (cursor floor) (owner (when (plusp (length prefix))
                                     (physical-store-prefix-timeline store floor)))
             (end nil) (complete nil))
        (when (and (= segment first) (/= (length prefix) (mod floor +wal-segment-size+)))
          (error "archive candidate lacks the verified receive floor prefix"))
        (when (and interior-base (not (getf inventory :base-overlap))
                   (not (physical-retained-archive-base-prefix-matches-p base-prefix prefix base)))
          (return-from physical-store-live-archive-segment nil))
        (replace bytes prefix)
        (when owner (setf end floor))
        (physical-store-validate-prefix
         store :on-record
         (lambda (record raw)
           (let* ((start (wal-record-start record))
                  (number (floor start +wal-segment-size+))
                  (carrier (physical-store-history-check-record store record raw cursor)))
             (cond ((= number segment)
                    (unless (= (mod start +wal-segment-size+)
                               (length (physical-retained-seal-bytes seal)))
                      (error "archive candidate lacks its exact receive prefix"))
                    (physical-retained-seal-append seal raw)
                    (setf owner carrier end (wal-record-end record)
                          complete (= (length (physical-retained-seal-bytes seal)) +wal-segment-size+)))
                   ((and end (eql owner carrier) (= cursor end) (= number (1+ segment)))
                    (setf complete t)))
             (setf cursor (wal-record-end record)))))
        (when (and complete (eql owner timeline))
          (values (physical-retained-seal-bytes seal) end))))))

(defun physical-store-stage-live-archive-segment (store timeline segment directory-fd check-directory)
  "Publish a closed receive segment in a held directory separate from the receive spool.
Return the staged name and proved end, or NIL until the segment is closed."
  (multiple-value-bind (bytes end) (physical-store-live-archive-segment store timeline segment)
    (when bytes
      (let ((name (wal-segment-filename segment timeline :timeline)))
        (values (wal-archive-stage-source directory-fd name bytes check-directory) end)))))

(defun physical-retained-seal-start-group (seal record timeline)
  "Start a segment group using the verified prefix, coalescing the latest entry when possible."
  (let* ((segment (floor (wal-record-start record) +wal-segment-size+))
         (offset (mod (wal-record-start record) +wal-segment-size+))
         (prefix (if (= segment (physical-retained-seal-prefix-segment seal))
                     (physical-retained-seal-prefix seal)
                     (make-array 0 :element-type '(unsigned-byte 8))))
         (latest (first (physical-retained-seal-entries seal)))
         (entry (when latest
                  (let ((metadata (third latest)))
                    (when (and (= segment (getf metadata :segment))
                               (= timeline (getf metadata :file-timeline))
                               (= (physical-retained-seal-cursor seal) (getf metadata :end)))
                      latest)))))
    (unless (= offset (length prefix))
      (error "retained segment lacks its authenticated physical prefix"))
    (when entry
      (multiple-value-bind (metadata old)
          (physical-retained-extent-decode
           (physical-retained-object-read (physical-store-retained-directory (physical-retained-seal-store seal))
                                          :extent (first entry) +physical-retained-image-limit+))
        (unless (and (equalp old prefix)
                     (= (getf metadata :end-digest) (physical-retained-seal-digest seal)))
          (error "coalesced retained input differs from its authenticated prefix"))))
    (setf (physical-retained-seal-segment seal) segment
          (physical-retained-seal-timeline seal) timeline
          (physical-retained-seal-start seal) (if entry (getf (third entry) :start) (physical-retained-seal-cursor seal))
          (physical-retained-seal-start-digest seal)
          (if entry (getf (third entry) :start-digest) (physical-retained-seal-digest seal))
          (physical-retained-seal-replacement seal) (and entry (first entry))
          (physical-retained-seal-bytes seal)
          (make-array (max 4096 (length prefix)) :element-type '(unsigned-byte 8)
                                                 :adjustable t :fill-pointer (length prefix) :initial-element 0))
    (replace (physical-retained-seal-bytes seal) prefix)))

(defun physical-retained-seal-metadata (seal)
  "Build metadata for the current extent from the store and scan state."
  (let ((identity (physical-store-retained-identity (physical-retained-seal-store seal)))
        (selection (physical-store-selection (physical-retained-seal-store seal))))
    (list :version 1 :system-id (getf identity :system-id) :base-timeline (getf identity :base-timeline)
          :base (getf identity :base) :manifest-digest (getf identity :manifest-digest)
          :history-target (physical-retained-seal-target seal)
          :history-digest (getf selection :digest) :history-text (physical-retained-seal-text seal)
          :start (physical-retained-seal-start seal) :end (physical-retained-seal-cursor seal)
          :start-digest (physical-retained-seal-start-digest seal) :end-digest (physical-retained-seal-digest seal)
          :segment (physical-retained-seal-segment seal) :file-timeline (physical-retained-seal-timeline seal)
          :prefix-digest (physical-store-update-digest #xCBF29CE484222325 (physical-retained-seal-bytes seal)))))

(defun physical-retained-seal-entry-cost (entry)
  "Return the encoded literal cost of an inventory entry."
  (physical-retained-inventory-check-text-budget entry))

(defun physical-retained-seal-initialize-budget (seal)
  "Initialize the entry count and text estimate for incremental checks."
  (let* ((hold (physical-retained-seal-hold seal))
         (reference (when hold (physical-retained-root-hold-reference hold))))
    (setf (physical-retained-seal-entry-count seal) (length (physical-retained-seal-entries seal))
          (physical-retained-seal-entry-bytes seal)
          (loop for entry in (physical-retained-seal-entries seal) sum (physical-retained-seal-entry-cost entry))
          (physical-retained-seal-header-bytes seal)
          (- (physical-retained-inventory-check-text-budget
              (physical-retained-inventory-payload
               (physical-store-retained-identity (physical-retained-seal-store seal))
               (physical-retained-seal-target seal) (physical-retained-seal-text seal) nil
               (when reference (physical-retained-root-reference-overlap reference))))
             3))))

(defun physical-retained-seal-entry-budget (seal entry replacement)
  "Check the budget after adding ENTRY and removing REPLACEMENT, if present.
Return the prospective entry count and literal-byte total."
  (let* ((count (+ (physical-retained-seal-entry-count seal) (if replacement 0 1)))
         (bytes (+ (physical-retained-seal-entry-bytes seal) (physical-retained-seal-entry-cost entry)
                   (- (if replacement (physical-retained-seal-entry-cost replacement) 0)))))
    (unless (and (<= count +physical-retained-inventory-count+)
                 (<= (+ (physical-retained-seal-header-bytes seal) bytes count 1)
                     (- +physical-retained-inventory-limit+ 16)))
      (error "retained sealing metadata exceeds its bound"))
    (values count bytes)))

(defun physical-retained-seal-publish-group (seal)
  "Stage the validated extent under the hold and update the reverse-ordered index."
  (when (physical-retained-seal-bytes seal)
    (let* ((metadata (physical-retained-seal-metadata seal))
           (image (physical-retained-extent-encode metadata (physical-retained-seal-bytes seal)))
           (previous (physical-retained-seal-entries seal))
           (replacement (when (physical-retained-seal-replacement seal) (first previous)))
           (entry (list (physical-store-update-digest #xCBF29CE484222325 image) (length image) metadata)))
      (when (and (physical-retained-seal-replacement seal)
                 (not (eql (physical-retained-seal-replacement seal) (first replacement))))
        (error "retained sealing replacement is not its latest entry"))
      (multiple-value-bind (count bytes) (physical-retained-seal-entry-budget seal entry replacement)
        (physical-retained-root-stage-image
         (physical-retained-seal-hold seal) (physical-retained-seal-target seal) (physical-retained-seal-text seal) image)
        (setf (physical-retained-seal-entries seal) (cons entry (if replacement (rest previous) previous))
              (physical-retained-seal-entry-count seal) count
              (physical-retained-seal-entry-bytes seal) bytes
              (physical-retained-seal-prefix seal) (copy-seq (physical-retained-seal-bytes seal))
              (physical-retained-seal-prefix-segment seal) (physical-retained-seal-segment seal)
              (physical-retained-seal-bytes seal) nil)))))

(defun physical-retained-seal-record (seal record raw)
  "Add a validated receive record, closing a group at a segment or timeline change."
  (let* ((store (physical-retained-seal-store seal))
         (timeline (physical-store-history-check-record store record raw (physical-retained-seal-cursor seal)))
         (segment (floor (wal-record-start record) +wal-segment-size+)))
    (when (and (physical-retained-seal-bytes seal)
               (or (/= segment (physical-retained-seal-segment seal))
                   (/= timeline (physical-retained-seal-timeline seal))))
      (physical-retained-seal-publish-group seal))
    (unless (physical-retained-seal-bytes seal)
      (physical-retained-seal-start-group seal record timeline))
    (physical-retained-seal-append seal raw)
    (setf (physical-retained-seal-cursor seal) (wal-record-end record)
          (physical-retained-seal-digest seal) (physical-store-update-digest (physical-retained-seal-digest seal) raw))))

(defun physical-retained-seal-scan (seal cursor digest)
  "Seal records through the exact receive boundary and publish one checked root.

Validate through durable flush even when CURSOR is earlier. Incremental budget
checks bound the index; final publication validates the full inventory and images."
  (physical-retained-seal-initialize-budget seal)
  (physical-store-validate-prefix
   (physical-retained-seal-store seal)
   :on-record (lambda (record raw)
                (when (<= (wal-record-end record) cursor)
                  (physical-retained-seal-record seal record raw))))
  (physical-retained-seal-publish-group seal)
  (unless (and (= cursor (physical-retained-seal-cursor seal))
               (= digest (physical-retained-seal-digest seal)))
    (error "retained sealing lacks its exact receive boundary"))
  (multiple-value-bind (id index)
      (physical-retained-root-publish-staged-entries
       (physical-retained-seal-hold seal) (physical-retained-seal-target seal)
       (physical-retained-seal-text seal) (reverse (physical-retained-seal-entries seal)))
    (let ((probe (%make-physical-store
                  :base-lsn (physical-store-base-lsn (physical-retained-seal-store seal))
                  :floor-lsn cursor :floor-digest digest)))
      (physical-store-check-retained-coverage probe (getf index :entries)))
    (setf (physical-retained-seal-latest seal) id)))

(defun physical-store-seal-and-select (store cursor digest publish-restartpoint)
  "Seal the prefix and select its root while the caller owns STORE's mutex.

Publish the restartpoint before receive control, then read back the complete
control tuple before handing off local root ownership. The restartpoint callback
must durably publish data; control readback does not prove that it did so."
  (physical-store-check-retained-identity store)
  (when (and (null (physical-store-retained-root store))
             (/= (physical-store-floor-lsn store) (physical-store-base-lsn store)))
    (error "compacted receive history lacks retained provenance; reseed required"))
  (wal-ensure-directory-durable (physical-store-retained-directory store))
  (physical-store-restore-retained-reference store)
  (multiple-value-bind (history target text) (physical-store-retained-history store)
    (let* ((entries (when (physical-store-retained-root store)
                      (getf (physical-retained-inventory-read-index
                             (physical-store-retained-directory store) (physical-store-retained-root store)
                             (physical-store-retained-identity store) history) :entries)))
           (prefix (physical-retained-seal-initial-prefix store entries))
           (expected nil) (selected nil))
      (call-with-physical-retained-root-update
       (lambda (id hold)
         (let* ((seal (%make-physical-retained-seal
                       :store store :hold hold :history history :target target :text text :entries (reverse entries) :latest id
                       :cursor (physical-store-floor-lsn store) :digest (physical-store-floor-digest store)
                       :prefix prefix :prefix-segment (floor (physical-store-floor-lsn store) +wal-segment-size+)))
                (root (physical-retained-seal-scan seal cursor digest)))
           (physical-store-test-point :after-retained-inventory-publication store)
           (funcall publish-restartpoint digest)
           (physical-store-test-point :after-restartpoint-publication store)
           (setf expected (physical-store-control-tuple store :floor cursor :floor-digest digest :root root)
                 selected root)
           (physical-store-save-control store (physical-store-flushed-lsn store)
                                        :digest (physical-store-flushed-digest store)
                                        :floor-lsn cursor :floor-digest digest :retained-root root)
           root))
       (physical-store-retained-owner store) (physical-store-retained-directory store)
       (physical-store-retained-identity store) target text nil :previous (physical-store-retained-root store)
       :read-selected (lambda () (physical-store-read-retained-control store expected)))
      (sb-sys:without-interrupts
        (setf (physical-store-retained-root store) selected
              (physical-store-floor-lsn store) cursor (physical-store-floor-digest store) digest))
      (physical-store-test-point :after-retained-root-handoff store)
      selected)))

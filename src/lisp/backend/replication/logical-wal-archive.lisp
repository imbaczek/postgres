;; Prepare bytes from a committed primary's retained WAL root in a private scope.
;; Supply checked archive bytes when the exact selected local carrier is absent.

(in-package #:pg)

(defstruct (logical-wal-archive (:constructor %make-logical-wal-archive)
                                (:print-object
                                 (lambda (object stream)
                                   (print-unreadable-object (object stream :type t :identity t)))))
  "Private retained-root selection and one-segment cache for one reader scope.
CACHE-BYTES holds a checked physical prefix; CACHE-START and CACHE-END bound
logical record coverage.  Equal bounds can witness EOF at the base.  Accessors
do not grant authority to produce or consume logical WAL."
  (directory nil :read-only t) (identity nil :read-only t)
  (reference nil :read-only t) (history nil :read-only t)
  (parent-history nil :read-only t) (guard nil :read-only t)
  (minimum-end 0 :read-only t)
  (index nil) (cache-segment nil) (cache-bytes nil) (cache-start 0) (cache-end 0))

(defun logical-wal-archive-clear-cache (archive)
  "Clear the private cache bytes and bounds without interruption."
  (sb-sys:without-interrupts
    (setf (logical-wal-archive-cache-segment archive) nil
          (logical-wal-archive-cache-bytes archive) nil
          (logical-wal-archive-cache-start archive) 0
          (logical-wal-archive-cache-end archive) 0)))

(defun logical-wal-archive-check-cursor (archive history namespace)
  "Check the archive scope and its match to the cursor history and namespace."
  (logical-wal-archive-check archive)
  (unless (and (eq namespace :timeline)
               (equalp history (logical-wal-archive-history archive)))
    (error "logical WAL cursor differs from its archive history or namespace"))
  t)

(defun logical-wal-archive-base-gap-p (archive segment carrier)
  "Recognize an overlap-only EOF witness at the base, anchored by the next
indexed extent and the exact carrier in the timeline namespace."
  (let* ((reference (logical-wal-archive-reference archive))
         (index (logical-wal-archive-index archive))
         (base (getf reference :base))
         (overlap (third (getf index :base-overlap)))
         (first (third (first (getf index :entries)))))
    (and (< base (getf reference :end))
         (plusp (mod base +wal-segment-size+))
         (= segment (floor base +wal-segment-size+))
         (= carrier (getf overlap :base-timeline))
         (eq :timeline (getf overlap :namespace))
         (eql (1+ segment) (getf first :segment))
         (eql base (getf first :start)))))

(defun logical-wal-archive-copy-at (archive position end carrier)
  "Copy a record within indexed logical coverage using the shared length check.
Return (bytes, size, T), (NIL, size, T) for EOF, or (NIL, NIL, NIL) for a
cache miss.  The returned bytes are fresh.  The checked physical prefix may
extend before the base, but cannot supply a record there."
  (logical-wal-archive-check archive)
  (let* ((reference (logical-wal-archive-reference archive))
         (segment (floor position +wal-segment-size+))
         (offset (mod position +wal-segment-size+)))
    (unless (and (<= (getf reference :base) position)
                 (< position (getf reference :end))
                 (eql carrier (logical-wal-segment-carrier
                               segment (logical-wal-archive-history archive))))
      (error "logical WAL position is outside certified archive coverage"))
    (unless (eql segment (logical-wal-archive-cache-segment archive))
      (return-from logical-wal-archive-copy-at (values nil nil nil)))
    (unless (<= (logical-wal-archive-cache-start archive) position
                (logical-wal-archive-cache-end archive))
      (error "logical WAL position is outside its checked segment coverage"))
    (let* ((bytes (logical-wal-archive-cache-bytes archive))
           (size (length bytes)))
      (when (= offset size)
        (return-from logical-wal-archive-copy-at (values nil size t)))
      (let ((length (logical-wal-check-record-length
                     bytes offset position size (min end (logical-wal-archive-cache-end archive)))))
        (values (subseq bytes offset (+ offset length)) size t)))))

(defun logical-wal-archive-certified-end (archive segment carrier)
  "Return the indexed EOF position for a segment wholly before the fork, or NIL.
Its entries must name CARRIER. A base gap witness can also supply the EOF
position. This check does not verify the live file's contents."
  (unless (<= (* (1+ segment) +wal-segment-size+)
              (getf (logical-wal-archive-reference archive) :end))
    (return-from logical-wal-archive-certified-end nil))
  (let ((end nil) (eligible t))
    (dolist (entry (getf (logical-wal-archive-index archive) :entries))
      (let ((metadata (third entry)))
        (when (= segment (getf metadata :segment))
          (unless (and (typep (getf metadata :end) '(unsigned-byte 64))
                       (< (* segment +wal-segment-size+) (getf metadata :end))
                       (<= (getf metadata :end) (* (1+ segment) +wal-segment-size+)))
            (error "invalid certified logical WAL segment geometry"))
          (unless (= carrier (getf metadata :file-timeline)) (setf eligible nil))
          (setf end (getf metadata :end)))))
    (cond ((and eligible end) end)
          ((and (null end) (logical-wal-archive-base-gap-p archive segment carrier))
           (getf (logical-wal-archive-reference archive) :base)))))

(defun logical-wal-archive-compare-live (archive position carrier size bytes)
  "Check live EOF geometry from index metadata, even before cache preparation.
Return whether a matching cache is present.  NIL bytes requests geometry only;
otherwise compare the full raw record span with the checked physical prefix,
including any header before the base.  This only rejects inconsistency; it does
not certify bytes of the live file that were not read."
  (logical-wal-archive-check archive)
  (let* ((segment (floor position +wal-segment-size+))
         (origin (* segment +wal-segment-size+))
         (end (logical-wal-archive-certified-end archive segment carrier)))
    (when (and end (/= size (- end origin)))
      (error "live logical WAL geometry differs from certified archive"))
    (when (eql segment (logical-wal-archive-cache-segment archive))
      (unless (and end
                   (eql carrier (logical-wal-segment-carrier
                                 segment (logical-wal-archive-history archive)))
                   (= size (length (logical-wal-archive-cache-bytes archive))))
        (error "live logical WAL geometry differs from certified archive"))
      (when bytes
        (let* ((offset (- position origin))
               (stop (+ offset (length bytes)))
               (prefix (logical-wal-archive-cache-bytes archive)))
          (unless (<= 0 offset stop (length prefix))
            (error "live logical WAL record exceeds its checked physical prefix"))
          (when (mismatch bytes prefix :start2 offset :end2 stop)
            (error "live logical WAL record differs from certified archive")))))
    (eql segment (logical-wal-archive-cache-segment archive))))

(defun call-with-optional-logical-wal-archive (system-id history namespace guard function)
  "Call FUNCTION with the committed archive when present, otherwise NIL."
  (let ((present
          (sb-thread:with-mutex (*wal-lock*)
            (unless (funcall guard) (error "logical WAL archive source is unavailable"))
            (not (null (getf (seventh (multiple-value-list (wal-read-control-fields)))
                             :retained-reference))))))
    (if present
        (call-with-logical-wal-archive system-id history namespace guard function)
        (funcall function nil))))

(defun logical-wal-archive-check (archive)
  "Recheck the caller's source proof and this archive's scope under the WAL mutex."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (funcall (logical-wal-archive-guard archive))
    (error "logical WAL archive is outside its selected source scope"))
  t)

(defun logical-wal-archive-check-selection (archive owner)
  "Check the committed primary control and retained-root owner under the WAL mutex.
The captured minimum end permits durable growth, but not regression."
  (let* ((reference (logical-wal-archive-reference archive))
         (fields (multiple-value-list (wal-read-control-fields)))
         (metadata (seventh fields))
         (history (logical-wal-archive-history archive))
         (tip (timeline-history-entry-timeline (first history))))
    (unless (and *wal-ready* (not (recovery-in-progress-p))
                 (= tip *wal-insert-timeline*)
                 (eq :timeline *wal-segment-namespace*)
                 (eql tip (fourth fields)) (eq :timeline (sixth fields))
                 (eql (getf (logical-wal-archive-identity archive) :system-id)
                      (getf metadata :system-id))
                 (equal reference (getf metadata :retained-reference))
                 (eql (getf (third fields) :prev-timeline)
                      (timeline-history-entry-timeline
                       (first (logical-wal-archive-parent-history archive))))
                 (<= (getf reference :end) (logical-wal-archive-minimum-end archive)
                     (second fields) *wal-durable-lsn*))
      (error "logical WAL archive differs from committed primary control"))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (let ((current (gethash owner *physical-retained-root-references*)))
        (physical-retained-root-check-context
         current (logical-wal-archive-directory archive)
         (logical-wal-archive-identity archive))
        (unless (and current
                     (eql (getf reference :root-id)
                          (physical-retained-root-reference-root-id current))
                     (not (gethash owner *physical-retained-root-updates*)))
          (error "logical WAL archive owner changed or has an unresolved update")))))
  t)

(defun logical-wal-archive-read-index (archive)
  "Read the selected index and check its identity, parent ancestry and chain.
The required base overlap does not itself establish logical coverage."
  (let* ((reference (logical-wal-archive-reference archive))
         (history (logical-wal-archive-parent-history archive))
         (index (physical-retained-inventory-read-index
                 (logical-wal-archive-directory archive) (getf reference :root-id)
                 (logical-wal-archive-identity archive) history)))
    (unless (and (getf index :base-overlap)
                 (eql (getf index :history-target)
                      (timeline-history-entry-timeline (first history)))
                 (= (length history) (length (physical-retained-inventory-check index))))
      (error "logical WAL archive lacks exact parent ancestry or base overlap"))
    (physical-store-check-retained-coverage
     (%make-physical-store :base-lsn (getf reference :base)
                           :floor-lsn (getf reference :end)
                           :floor-digest (getf reference :end-digest))
     (getf index :entries))
    index))

(defun call-with-logical-wal-archive-core (system-id history namespace guard identity-function function)
  "Call FUNCTION with a private archive selected from committed primary control.
GUARD supplies the caller's source proof; this scope also checks persisted
control, history and root.  It rechecks both under the captured WAL mutex on
the owning thread.  A retained-root reader lease spans the callback.  The
index and cache are cleared on every exit, and the archive then expires."
  ;; Startup supplies a pre-catalog identity only after the retained root has
  ;; been restored in committed control.  The callback can publish while the
  ;; reader lease is held; the runtime wrapper supplies the live catalog ID.
  (unless (and (functionp guard) (functionp identity-function) (functionp function) (eq namespace :timeline))
    (error "logical WAL archive requires a timeline source scope"))
  (timeline-check-lineage history)
  (let ((archive nil) (owner nil) (active t)
                      (thread sb-thread:*current-thread*) (mutex *wal-lock*)
                      (root (truename *pgdata*)) (history-bytes nil))
    (sb-thread:with-mutex (*wal-lock*)
      (unless (funcall guard) (error "logical WAL archive source is unavailable"))
      (let* ((fields (multiple-value-list (wal-read-control-fields)))
             (metadata (seventh fields))
             (reference (copy-tree (getf metadata :retained-reference)))
             (identity (physical-promotion-retained-identity system-id reference))
             (tip (timeline-history-entry-timeline (first history))))
        (multiple-value-bind (parent fork projected)
            (physical-promotion-retained-parent-history history)
          (unless (and (= fork (getf reference :end))
                       (eql parent (getf (third fields) :prev-timeline))
                       (eql system-id (funcall identity-function)))
            (error "logical WAL archive differs from its writer or fixed fork"))
          (setf history-bytes (timeline-history-bytes tip))
          (unless (equalp history (timeline-parse-history tip (bytes->string history-bytes)))
            (error "logical WAL archive history differs from the committed child"))
          (setf owner (physical-promotion-retained-owner :primary)
                archive
                (%make-logical-wal-archive
                 :directory (physical-retained-root-directory (physical-recovery-retained-directory))
                 :identity identity :reference reference
                 :history (mapcar #'copy-timeline-history-entry history)
                 :parent-history projected :minimum-end (second fields)
                 :guard (lambda ()
                          (and active (eq thread sb-thread:*current-thread*)
                               (eq mutex *wal-lock*) (equal root (truename *pgdata*))
                               (eql system-id (funcall identity-function))
                               (equalp history-bytes (timeline-history-bytes tip))
                               (funcall guard)
                               (logical-wal-archive-check-selection archive owner))))))))
    (unwind-protect
         (call-with-physical-retained-root-reader
          (lambda ()
            (sb-thread:with-mutex (*wal-lock*) (logical-wal-archive-check archive))
            (let ((index (logical-wal-archive-read-index archive)))
              (sb-thread:with-mutex (*wal-lock*)
                (logical-wal-archive-check archive)
                (setf (logical-wal-archive-index archive) index)))
            (funcall function archive))
          owner (logical-wal-archive-directory archive) (logical-wal-archive-identity archive)
          (getf (logical-wal-archive-reference archive) :root-id))
      (sb-sys:without-interrupts
        (setf active nil
              (logical-wal-archive-index archive) nil)
        (logical-wal-archive-clear-cache archive)))))

(defun call-with-logical-wal-archive (system-id history namespace guard function)
  (call-with-logical-wal-archive-core
   system-id history namespace guard #'catalog-system-identifier function))

(defun logical-wal-archive-segment-entries (archive segment carrier)
  "Select entries for the exact last-byte carrier in the child history.
An overlap-only base gap is a zero-width EOF witness, not record coverage."
  (unless (and (typep segment '(integer 0 1099511627775))
               (eql carrier (logical-wal-segment-carrier
                             segment (logical-wal-archive-history archive))))
    (error "logical WAL archive request differs from the selected carrier"))
  (let* ((reference (logical-wal-archive-reference archive))
         (origin (* segment +wal-segment-size+))
         (entries (remove-if-not
                   (lambda (entry) (= segment (getf (third entry) :segment)))
                   (getf (logical-wal-archive-index archive) :entries))))
    (unless (or (and (null entries) (logical-wal-archive-base-gap-p archive segment carrier))
                (and entries
                     (< origin (getf reference :end))
                     (< (getf reference :base) (+ origin +wal-segment-size+))
                     (every (lambda (entry)
                              (= carrier (getf (third entry) :file-timeline))) entries)))
      (error "logical WAL archive has no certified coverage for the selected carrier"))
    entries))

(defun logical-wal-archive-prepare (archive segment carrier)
  "Validate all selected extent versions outside WAL exclusion, then install
their physical prefix and separate logical bounds under a renewed source check."
  (when (sb-thread:holding-mutex-p *wal-lock*)
    (error "logical WAL archive preparation must run outside WAL exclusion"))
  (let ((entries nil))
    (sb-thread:with-mutex (*wal-lock*)
      (logical-wal-archive-check archive)
      (when (eql segment (logical-wal-archive-cache-segment archive))
        (unless (eql carrier (logical-wal-segment-carrier
                              segment (logical-wal-archive-history archive)))
          (error "logical WAL archive request differs from the selected carrier"))
        (return-from logical-wal-archive-prepare t))
      (setf entries (logical-wal-archive-segment-entries archive segment carrier))
      (logical-wal-archive-clear-cache archive))
    (let* ((directory (logical-wal-archive-directory archive))
           (reference (logical-wal-archive-reference archive))
           (base (getf reference :base))
           (origin (* segment +wal-segment-size+))
           (start (if entries (max base origin (getf (third (first entries)) :start)) base))
           (end (if entries (min (getf reference :end) (getf (third (car (last entries))) :end)) base))
           (overlap (when (= segment (floor (1- base) +wal-segment-size+))
                      (physical-retained-inventory-read-overlap
                       directory (logical-wal-archive-index archive))))
           (bytes (nth-value
                   1 (physical-retained-inventory-verify-entries
                      entries (logical-wal-archive-parent-history archive)
                      (lambda (id)
                        (physical-retained-object-read directory :extent id
                                                       +physical-retained-image-limit+))
                      overlap))))
      (sb-thread:with-mutex (*wal-lock*)
        (logical-wal-archive-check archive)
        (sb-sys:without-interrupts
          (setf (logical-wal-archive-cache-start archive) start
                (logical-wal-archive-cache-end archive) end
                (logical-wal-archive-cache-bytes archive) bytes
                (logical-wal-archive-cache-segment archive) segment))))
    t))

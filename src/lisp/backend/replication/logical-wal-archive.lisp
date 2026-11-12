;; Prepare bytes from a committed primary's retained WAL root in a private scope.
;; This is not yet a cursor fallback or a slot import path.

(in-package #:pg)

(defstruct (logical-wal-archive (:constructor %make-logical-wal-archive)
                                (:print-object
                                 (lambda (object stream)
                                   (print-unreadable-object (object stream :type t :identity t)))))
  "Private retained-root selection and one-segment cache for one reader scope.
Its accessors do not grant authority to produce or consume logical WAL."
  (directory nil :read-only t) (identity nil :read-only t)
  (reference nil :read-only t) (history nil :read-only t)
  (parent-history nil :read-only t) (guard nil :read-only t)
  (minimum-end 0 :read-only t)
  (index nil) (cache-segment nil) (cache-bytes nil))

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

(defun call-with-logical-wal-archive (system-id history namespace guard function)
  "Call FUNCTION with a private archive selected from committed primary control.
GUARD supplies the caller's source proof; this scope also checks persisted
control, history and root.  It rechecks both under the captured WAL mutex on
the owning thread.  A retained-root reader lease spans the callback.  The
index and cache are cleared on every exit, and the archive then expires."
  (unless (and (functionp guard) (functionp function) (eq namespace :timeline))
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
                       (eql system-id (catalog-system-identifier)))
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
                               (eql system-id (catalog-system-identifier))
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
              (logical-wal-archive-index archive) nil
              (logical-wal-archive-cache-segment archive) nil
              (logical-wal-archive-cache-bytes archive) nil)))))

(defun logical-wal-archive-segment-entries (archive segment carrier)
  "Select entries for the exact carrier chosen by the child history.
The carrier is the timeline at the segment's last byte, so a parent archive
file for the fork segment is rejected when the child carries that segment."
  (unless (and (typep segment '(integer 0 1099511627775))
               (eql carrier (logical-wal-segment-carrier
                             segment (logical-wal-archive-history archive))))
    (error "logical WAL archive request differs from the selected carrier"))
  (let* ((reference (logical-wal-archive-reference archive))
         (origin (* segment +wal-segment-size+))
         (entries (remove-if-not
                   (lambda (entry) (= segment (getf (third entry) :segment)))
                   (getf (logical-wal-archive-index archive) :entries))))
    (unless (and entries
                 (< origin (getf reference :end))
                 (< (getf reference :base) (+ origin +wal-segment-size+))
                 (every (lambda (entry)
                          (= carrier (getf (third entry) :file-timeline))) entries))
      (error "logical WAL archive has no certified coverage for the selected carrier"))
    entries))

(defun logical-wal-archive-prepare (archive segment carrier)
  "Prepare one segment's bytes outside WAL exclusion.
Validate every extent for this segment before installing the cache under
a renewed source check.  These bytes do not grant logical coverage or any
cursor, import, acknowledgement, replay or coordination authority."
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
      (setf (logical-wal-archive-cache-segment archive) nil
            (logical-wal-archive-cache-bytes archive) nil))
    (let* ((directory (logical-wal-archive-directory archive))
           (base (getf (logical-wal-archive-reference archive) :base))
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
        (setf (logical-wal-archive-cache-segment archive) segment
              (logical-wal-archive-cache-bytes archive) bytes)))
    t))

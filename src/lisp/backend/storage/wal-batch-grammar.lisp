;;;; Native WAL batch ordering and record validation.
(in-package #:pg.wal-batch-grammar)

(defvar *wal-recovery-group-active* nil)

(defvar *wal-recovery-reservation-batch-clean-p* nil)

(defvar *wal-recovery-page-images* '())

(defvar *wal-recovery-relation-resets* nil)

(defvar *wal-recovery-page-records-seen-p* nil)

(defvar *wal-recovery-catalog-chunk-count* nil)
(defvar *wal-recovery-catalog-next-chunk* 0)
(defvar *wal-recovery-logical-xid* nil)
(defvar *wal-recovery-logical-chunk-count* nil)
(defvar *wal-recovery-logical-next-chunk* 0)
(defvar *wal-recovery-logical-bytes* 0)
(defvar *wal-recovery-logical-catalog-event-p* nil)

(defun wal-batch-add-relation-reset (record active pages-seen catalog-count logical-count logical-catalog-event resets)
  "Return the staged reset, checking its batch position and uniqueness."
  (unless (and active (not pages-seen) (null catalog-count)
               (null logical-count) (not logical-catalog-event))
    (error "relation reset must precede page images in a WAL batch"))
  (let ((reset (wal-decode-relation-reset record)))
    (when (find reset resets :key #'cdr :test #'equal)
      (error "duplicate relation reset in a WAL batch"))
    (acons (wal-record-xid record) reset resets)))

(defun wal-recovery-stage-relation-reset (record)
  (setf *wal-recovery-relation-resets*
        (wal-batch-add-relation-reset record *wal-recovery-group-active*
                                      *wal-recovery-page-records-seen-p*
                                      *wal-recovery-catalog-chunk-count*
                                      *wal-recovery-logical-chunk-count*
                                      *wal-recovery-logical-catalog-event-p*
                                      *wal-recovery-relation-resets*))
  (values))

(defun wal-empty-metadata-terminal-type-p (type)
  "Recognize terminals that must be the only record after a batch marker."
  (member type (list +wal-record-running-xacts+ +wal-record-xid-reservation+
                     +wal-record-parameter-change+ +wal-record-full-page-writes-change+
                     +wal-record-logical-decoding-status-change+) :test #'=))

(defun wal-batch-next-marker-clean-p (record clean-p)
  "Return whether a metadata terminal can follow this record and its prefix."
  (let ((type (wal-record-type record)))
    (wal-require-ordinary-record-type type)
    (cond
      ((= type +wal-record-batch-start+)
       (unless (and (zerop (wal-record-xid record))
                    (zerop (length (wal-record-payload record))))
         (error "invalid WAL batch-start marker"))
       t)
      ((wal-empty-metadata-terminal-type-p type) clean-p)
      (t nil))))

(defun wal-note-recovery-batch-record (record)
  "Track marker-only prefixes for the current recovery or logical scan."
  (setf *wal-recovery-reservation-batch-clean-p*
        (wal-batch-next-marker-clean-p record *wal-recovery-reservation-batch-clean-p*)))

(defun wal-validate-empty-metadata-terminal (record clean-p)
  "Decode a metadata terminal without publishing it.
CLEAN-P must certify that only a valid batch marker precedes the terminal."
  (unless clean-p
    (error "metadata terminal must be the sole record after its batch marker"))
  (let ((type (wal-record-type record)))
    (cond
      ((= type +wal-record-running-xacts+) (wal-running-xacts-record-info record))
      ((= type +wal-record-parameter-change+) (wal-parameter-change-record-info record))
      ((= type +wal-record-full-page-writes-change+) (wal-full-page-writes-change-record-info record))
      ((= type +wal-record-logical-decoding-status-change+) (wal-logical-decoding-status-record-info record))
      ((= type +wal-record-xid-reservation+) (wal-xid-reservation-record-bound record))
      (t (error "unsupported empty metadata terminal")))))

(defun wal-check-batch-logical-terminal (kind xid resets logical-catalog-event logical-count)
  "Validate logical and relation-reset ordering at a terminal."
  (when resets
    (unless (and (eq kind :commit) (every (lambda (reset) (eql xid (car reset))) resets))
      (error "relation resets require their transaction's commit terminal")))
  (ecase kind
    (:commit (when logical-catalog-event
               (error "logical catalog events are attached to a row commit")))
    (:catalog (when logical-count
                (error "logical row changes are attached to a catalog WAL batch")))
    (:other (when (or logical-catalog-event logical-count)
              (error "logical changes are attached to an unsupported WAL terminal"))))
  (values))

(defun wal-decode-batch-terminal (record active clean catalog-count resets logical-catalog-event logical-count)
  "Validate framing and ordering. A catalog terminal returns its declared chunk count and size."
  (unless active (error "WAL terminal record appears outside a batch"))
  (let ((type (wal-record-type record))
        (payload (wal-record-payload record)))
    (when (and catalog-count (/= type +wal-record-catalog-commit+))
      (error "WAL catalog chunks lack a catalog commit record"))
    (flet ((check-logical (kind)
             (wal-check-batch-logical-terminal kind (wal-record-xid record)
                                               resets logical-catalog-event logical-count)))
      (cond
        ((= type +wal-record-commit+)
         (wal-decode-commit-timestamp payload)
         (check-logical :commit)
         nil)
        ((= type +wal-record-catalog+)
         (check-logical :catalog)
         payload)
        ((= type +wal-record-catalog-commit+)
         (check-logical :catalog)
         (wal-decode-catalog-commit-payload payload))
        (t
         (check-logical :other)
         (cond
           ((wal-empty-metadata-terminal-type-p type) (wal-validate-empty-metadata-terminal record clean))
           ((= type +wal-record-clog-reset+) (wal-read-u64-payload payload))
           ((= type +wal-record-checkpoint+) (wal-checkpoint-record-snapshot record))
           ((= type +wal-record-maintenance+)
            (unless (zerop (wal-record-xid record))
              (error "WAL maintenance record must not carry a transaction ID"))
            (unless (zerop (length payload))
              (error "WAL maintenance record has an invalid payload"))
            nil)
           (t (error "unsupported WAL terminal"))))))))

(defun wal-check-batch-page-record (active catalog-count)
  (unless active (error "WAL page images appear outside a batch"))
  (when catalog-count (error "WAL page images follow catalog chunks in a batch")))

(defun wal-check-batch-logical-changes (logical-catalog-event catalog-count)
  (when logical-catalog-event
    (error "logical row changes follow a catalog event in the same batch"))
  (when catalog-count
    (error "logical WAL changes follow catalog chunks in a batch")))

(defun wal-check-batch-logical-catalog-event (record active logical-catalog-event logical-count)
  (unless active (error "logical catalog event appears outside a WAL batch"))
  (unless (zerop (wal-record-xid record))
    (error "logical catalog event must not carry a transaction ID"))
  (when (or logical-catalog-event logical-count)
    (error "logical catalog event is mixed with other changes in a WAL batch"))
  (let ((payload (wal-record-payload record)))
    (unless (and (>= (length payload) 8)
                 (plusp (wal-u32 payload 0))
                 (= (+ 8 (wal-u32 payload 4)) (length payload)))
      (error "logical catalog event has an invalid payload"))))

(defun wal-decode-next-catalog-chunk (record active logical-catalog-event logical-count count next)
  "Validate the catalog chunk against the batch's grammar and return its count and bytes."
  (unless active
    (error "WAL catalog chunks appear outside a batch"))
  (when (or logical-catalog-event logical-count)
    (error "WAL catalog chunks are mixed with row or catalog events"))
  (multiple-value-bind (chunk-index chunk-count bytes)
      (wal-decode-catalog-chunk-payload (wal-record-payload record))
    (unless (= chunk-index next)
      (error "WAL catalog chunk index ~a follows chunk ~a" chunk-index next))
    (when (and count (/= chunk-count count))
      (error "WAL catalog chunk count changes within a batch"))
    (values chunk-count bytes)))

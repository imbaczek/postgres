;;;; exec_cursor.lisp -- resumable physical plans for demand-driven producers.
(in-package #:pg)

(defun make-mapping-sql-iterator (input function &key preserve-toast-context)
  ;; FUNCTION returns the output row, whether to keep it, and its origins.
  ;; The fourth iterator value is a TOAST slice context for the current row.
  ;; Bind it only while evaluating that row; pass it on only when requested.
  (make-sql-iterator
   (lambda ()
     (loop
       (multiple-value-bind (row present origins toast-context presence) (sql-iterator-next input)
         (unless present (return (values nil nil nil nil)))
         ;; A scan's saved pointer belongs only to this input row. Nested
         ;; subqueries must compute their own demand and slice plans.
         (let ((*query-row-origins* (and *datum-origin-capture* origins))
               (*toast-slice-current* toast-context)
               (*query-row-presence* presence)
               (*select-toast-demand* nil)
               (*select-toast-slice-plan* nil))
           (multiple-value-bind (result keep result-origins) (funcall function row)
             (when keep
               (return (values result t result-origins
                               (and preserve-toast-context toast-context) presence))))))))
   (lambda () (close-sql-iterator input))))

(defun make-buffered-sql-iterator (producer &optional input)
  (let ((store nil) (reader nil) (capture-p *datum-origin-capture*))
    (make-sql-iterator
     (lambda ()
       (let ((*datum-origin-capture* capture-p))
         (unless store
           (setf store (make-query-replay-store nil))
           (funcall producer (lambda (row) (append-query-replay-row store row)))
           (query-row-spool-seal (query-replay-store-spool store))
           (setf (query-replay-store-eof-p store) t
                 reader (make-query-replay-reader store)))
         (sql-iterator-next reader)))
     (lambda ()
       (when reader (close-sql-iterator reader))
       (when store (close-query-replay-store store))
       (when input (close-sql-iterator input))))))

(defvar *lateral-outer-environment* nil)

(defun postponable-sort-project-set (node child)
  "Return the ProjectSet, input width and calls when sort keys can run first."
  (when (and (member (first node) '(:sort :top-n))
             (eq (first child) :project) (eq (first (third child)) :project-set))
    (let* ((set-node (third child))
           (width (fourth set-node))
           (calls (second set-node))
           (targets (second child))
           (call-count (length calls)))
      (when (and (integerp width) (not (minusp width)) calls
                 (notany (lambda (entry)
                           (project-set-expression-dependent-p
                            (car entry) width call-count targets))
                         (second node)))
        (values set-node width calls)))))

(declaim (inline heap-page-item-visibility))

(defun heap-page-item-visibility (page offset snapshot my-xid committed-xid-cache)
  (let ((xmin (full-xid-from-32 (page-u32 page (+ offset +heap-tuple-xmin-off+))))
        (xmax (full-xid-from-32 (page-u32 page (+ offset +heap-tuple-xmax-off+))))
        (cid (page-u32 page (+ offset +heap-tuple-cid-off+)))
        (infomask (page-u16 page (+ offset +heap-tuple-infomask-off+))))
    (values (heap-tuple-visible-p xmin xmax cid snapshot my-xid infomask committed-xid-cache)
            xmin xmax cid infomask)))

(defmacro do-visible-fixed-page-key-batches ((page snapshot my-xid cache keys consume)
                                             (start index width alignment words) &body decode)
  "Feed visible non-NULL fixed heap keys into full and partial batches.
DECODE writes WORDS signed 32-bit values beginning at INDEX. The result
counts all visible rows, including NULLs excluded from the batches."
  (let ((visible-count (gensym "VISIBLE-COUNT"))
        (capacity (gensym "CAPACITY"))
        (offnum (gensym "OFFNUM"))
        (offset (gensym "OFFSET"))
        (length (gensym "LENGTH")))
    `(let ((,index 0) (,visible-count 0) (,capacity (length ,keys)))
       (declare (type fixnum ,index ,visible-count))
       (assert (and (plusp ,capacity) (zerop (mod ,capacity ,words))))
       (do-live-page-items (,page ,offnum ,offset ,length)
         (multiple-value-bind (visible xmin xmax cid infomask)
             (heap-page-item-visibility ,page ,offset ,snapshot ,my-xid ,cache)
           (declare (ignore xmin xmax cid))
           (when visible
             (incf ,visible-count)
             (let ((,start (single-fixed-heap-tuple-offset
                            ,page ,offset (+ ,offset ,length) infomask ,width ,alignment)))
               (when ,start
                 ,@decode
                 (incf ,index ,words)
                 (when (= ,index ,capacity)
                   (funcall ,consume ,keys ,index)
                   (setf ,index 0)))))))
       (when (plusp ,index) (funcall ,consume ,keys ,index))
       ,visible-count)))

(defun map-visible-int4-page-key-batches (page snapshot my-xid committed-xid-cache keys consume &optional float-p)
  "Send blocks of visible non-NULL four-byte keys through caller-owned KEYS."
  (declare (type (simple-array (signed-byte 32) (*)) keys)
           (dynamic-extent consume))
  (do-visible-fixed-page-key-batches (page snapshot my-xid committed-xid-cache keys consume)
      (start index 4 4 1)
    (let ((key (heap-read-int4 page start)))
      (setf (aref keys index)
            (if float-p
                (let ((magnitude (logand key #x7fffffff)))
                  (cond ((zerop magnitude) 0)
                        ((> magnitude #x7f800000) #x7fc00000)
                        (t key)))
                key)))))

(defun map-visible-int8-page-key-batches (page snapshot my-xid committed-xid-cache keys consume &optional float-p)
  "Visit eight-byte keys as pairs of signed 32-bit words."
  (declare (type (simple-array (signed-byte 32) (*)) keys)
           (dynamic-extent consume))
  (do-visible-fixed-page-key-batches (page snapshot my-xid committed-xid-cache keys consume)
      (start index 8 8 2)
    (let ((low (heap-read-int4 page start))
          (high (heap-read-int4 page (+ start 4))))
      (when float-p
        (let ((magnitude (logand high #x7fffffff)))
          (cond ((and (zerop magnitude) (zerop low))
                 (setf high 0))
                ((or (> magnitude #x7ff00000)
                     (and (= magnitude #x7ff00000) (not (zerop low))))
                 (setf low 0 high #x7ff80000)))))
      (setf (aref keys index) low
            (aref keys (1+ index)) high))))

(defun read-interval-hash-key (page start)
  "Encode the PostgreSQL comparison span as whole days and subday microseconds.
The 13-bit factor of a day lets us normalize the signed time using fixnums
even when its full 64-bit heap value exceeds the fixnum range."
  (declare (optimize (speed 3) (safety 1))
           (type (simple-array (unsigned-byte 8) (*)) page)
           (type fixnum start))
  (let* ((low (ldb (byte 32 0) (heap-read-int4 page start)))
         (high (heap-read-int4 page (+ start 4)))
         (scaled (+ (ash high 19) (ash low -13))))
    (multiple-value-bind (carry remainder) (floor scaled (ash +sql-microseconds-per-day+ -13))
      (let ((days (+ (* 30 (heap-read-int4 page (+ start 12)))
                     (heap-read-int4 page (+ start 8)) carry))
            (micros (logior (ash remainder 13) (logand low #x1fff))))
        (values (dpb (ldb (byte 31 0) micros) (byte 31 0)
                     (- (ash (ldb (byte 1 31) micros) 31)))
                (dpb (ldb (byte 31 0) days) (byte 31 0)
                     (- (ash (ldb (byte 1 31) days) 31)))
                (logior (ash (ash days -32) 5) (ash micros -32)))))))

(defun map-visible-interval-page-key-batches (page snapshot my-xid committed-xid-cache keys consume &optional float-p)
  "Visit canonical interval keys as three signed 32-bit words."
  (declare (type (simple-array (signed-byte 32) (*)) keys)
           (dynamic-extent consume)
           (ignore float-p))
  (do-visible-fixed-page-key-batches (page snapshot my-xid committed-xid-cache keys consume)
      (start index 16 8 3)
    (multiple-value-bind (low days high) (read-interval-hash-key page start)
      (setf (aref keys index) low
            (aref keys (+ index 1)) days
            (aref keys (+ index 2)) high))))

(defun map-visible-fixed-heap-key-batches (table snapshot keys consume map-page &optional float-p)
  "Visit a one-column fixed-width heap within the caller's relation and snapshot domain.
KEYS is writable, nonempty workspace. CONSUME may read but must not retain it."
  (declare (type (simple-array (signed-byte 32) (*)) keys)
           (dynamic-extent consume))
  (assert (plusp (length keys)))
  (let* ((relfilenode (pg-class-relfilenode table))
         (nblocks (buf-nblocks relfilenode))
         (my-xid (and *backend* (backend-xid *backend*)))
         (committed-xid-cache (list nil))
         (handle (make-buf-page-handle))
         (scratch (make-page))
         (visible-count 0))
    (declare (dynamic-extent committed-xid-cache))
    (dotimes (block nblocks visible-count)
      (check-for-interrupts)
      (unwind-protect
           (let ((page (buf-acquire-read-page handle relfilenode block :main scratch)))
             (when page
               (incf visible-count
                     (funcall map-page page snapshot my-xid committed-xid-cache keys consume float-p))))
        (buf-release-page handle)))))

(defun map-visible-int4-heap-key-batches (table snapshot keys consume)
  (declare (dynamic-extent consume))
  (map-visible-fixed-heap-key-batches table snapshot keys consume #'map-visible-int4-page-key-batches
                                      (eq (pg-column-type (aref (pg-class-columns table) 0)) :float4)))

(defun map-visible-int8-heap-key-batches (table snapshot keys consume)
  (declare (dynamic-extent consume))
  (map-visible-fixed-heap-key-batches table snapshot keys consume #'map-visible-int8-page-key-batches
                                      (eq (pg-column-type (aref (pg-class-columns table) 0)) :float8)))

(defun heap-page-xid-fields (page offset)
  "Read the adjacent raw xmin and xmax fields without boxing a combined XID word."
  #+x86-64
  (return-x86-page-word heap-page-xid-fields page offset 8
                        (lambda (sap start)
                          (let ((word (sb-sys:sap-ref-64 sap start)))
                            (values (ldb (byte 32 0) word) (ldb (byte 32 32) word)))))
  (values (page-u32 page offset) (page-u32 page (+ offset 4))))

(defun make-heap-page-visibility-reader (page snapshot)
  "Cache visibility of undeleted, committed insertions for the page's fixed snapshot."
  (let ((committed-xid-cache (list nil))
        (cached-xmin nil)
        (cached-reference nil))
    (lambda (offset my-xid)
      (multiple-value-bind (raw-xmin raw-xmax)
          (heap-page-xid-fields page (+ offset +heap-tuple-xmin-off+))
        (let ((infomask (page-u16 page (+ offset +heap-tuple-infomask-off+)))
              (reference *next-xid*))
          (cond ((logtest +heap-xmin-invalid+ infomask)
                 (values nil infomask))
                ((and (null my-xid) (zerop raw-xmax)
                      (eql raw-xmin cached-xmin) (eql reference cached-reference))
                 (values t infomask))
                (t
                 (let* ((xmin (full-xid-from-32 raw-xmin reference))
                        (xmax (full-xid-from-32 raw-xmax reference))
                        (cid (page-u32 page (+ offset +heap-tuple-cid-off+)))
                        (visible (heap-tuple-visible-p xmin xmax cid snapshot my-xid
                                                       infomask committed-xid-cache)))
                   (when (and visible (null my-xid) (zerop raw-xmax))
                     (setf cached-xmin raw-xmin cached-reference reference))
                   (values visible infomask)))))))))

(declaim (inline inline-text-field-cursor))

(defun inline-text-field-cursor (page start limit prefix alignment)
  "Locate an inline text header, skipping padding when present."
  (let ((cursor (+ start prefix)))
    (if (and (< cursor limit) (zerop (aref page cursor)))
        (+ start (align-up prefix alignment))
        cursor)))

(defun make-inline-text-key-reader (columns text-slot key-slot)
  "Prepare an integer projection for tuples accepted by the inline text predicate."
  (when key-slot
    (let* ((column (aref columns key-slot))
           (reader (case (pg-column-type column)
                     (:int4 #'heap-read-int4)
                     (:int8 #'heap-read-int8)))
           (text-prefix (heap-fixed-prefix-end columns text-slot)))
      (when (and reader text-prefix)
        (let ((alignment (column-align column))
              (width (column-fixed-size column))
              (text-alignment (column-align (aref columns text-slot)))
              (max-alignment (maxalign 1))
              (natts (length columns)))
          (flet ((key-offset (first cursor)
                   (let ((prefix (heap-fixed-prefix-end columns key-slot first cursor)))
                     (when prefix (align-up prefix alignment)))))
            (let* ((fixed-offset (when (< key-slot text-slot)
                                   (key-offset 0 (maxalign +size-of-heap-tuple-header+))))
                   (offsets (when (> key-slot text-slot)
                              (coerce
                               (loop for remainder below max-alignment
                                     for offset = (key-offset (1+ text-slot) remainder)
                                     unless offset return nil
                                       collect (- offset remainder))
                               'simple-vector))))
              (when (or fixed-offset (plusp (length offsets)))
                (lambda (page start limit)
                  ;; Skip layout discovery for every field except the text header.
                  (let ((offset
                          (if fixed-offset
                              (+ start fixed-offset)
                              (let* ((cursor (inline-text-field-cursor
                                              page start limit text-prefix text-alignment))
                                     (end (nth-value 1 (heap-inline-varlena-range page cursor limit))))
                                (+ end (svref offsets (mod (- end start) max-alignment)))))))
                    (unless (<= (+ offset width) limit)
                      (ereport "XX001" "truncated heap attribute"))
                    (let ((row (make-array natts :initial-element +null+)))
                      (setf (aref row key-slot) (funcall reader page offset))
                      row)))))))))))

(defun make-inline-text-page-predicate (columns slot constant-compare)
  "Locate an inline text predicate after a fixed-width prefix in canonical rows."
  (let ((prefix (heap-fixed-prefix-end columns slot))
        (natts (length columns))
        (alignment (column-align (aref columns slot)))
        (header (maxalign +size-of-heap-tuple-header+)))
    (when prefix
      (lambda (page start limit infomask)
        (when (and (not (logtest +heap-hasnull+ infomask))
                   (<= (+ start header) limit (length page))
                   (= (aref page (+ start +heap-tuple-hoff-off+)) header)
                   (= (page-u16 page (+ start +heap-tuple-natts-off+)) natts))
          (let ((cursor (inline-text-field-cursor page start limit prefix alignment)))
            (when (< cursor limit)
              (let ((tag (aref page cursor)))
                (when (and (/= tag 1) (/= (logand tag 3) 2))
                  (multiple-value-bind (first end)
                      (heap-inline-varlena-range page cursor limit)
                    (when (%octet-span-ascii-p page first end)
                      (values (funcall constant-compare page first end) t))))))))))))

(defun make-top-n-text-rejector (columns cutoff)
  "Reject canonical ASCII rows whose single sort key cannot enter the full heap."
  (when cutoff
    (let ((slot (top-n-key-cutoff-slot cutoff))
          (descending-p (top-n-key-cutoff-descending-p cutoff)))
      (when (and (< slot (length columns))
                 (member (pg-column-type (aref columns slot)) '(:text :varchar)))
        (let ((predicate
                (make-inline-text-page-predicate
                 columns slot
                 (lambda (page start end)
                   (declare (type (simple-array (unsigned-byte 8) (*)) page)
                            (type fixnum start end)
                            (optimize (speed 3)))
                   (let* ((key (the simple-base-string (top-n-key-cutoff-value cutoff)))
                          (width (the fixnum (- end start)))
                          (key-width (length key))
                          (comparison
                            (loop for i of-type fixnum below (min width key-width)
                                  for difference = (- (aref page (the fixnum (+ start i)))
                                                      (char-code (schar key i)))
                                  unless (zerop difference) return difference
                                    finally (return (- width key-width)))))
                     ;; An equal key would lose the arrival-order tie breaker.
                     (if descending-p (<= comparison 0) (>= comparison 0)))))))
          (when predicate
            (lambda (page start limit infomask)
              (and (typep (top-n-key-cutoff-value cutoff) 'simple-base-string)
                   (funcall predicate page start limit infomask)))))))))

(defun decode-inline-select-row (page columns start limit)
  (let ((row (deform-heap-tuple page columns :start start :limit limit :metadata :none)))
    (when (some #'heap-external-varlena-p row)
      (ereport "XX001" "external value has no owning TOAST relation"))
    row))

(defun make-fixed-uuid-page-predicate (layout slot span-compare)
  "Compare an already bounded UUID field without allocating a SQL UUID."
  (let ((offset (car (aref (cdr layout) slot))))
    (lambda (page start limit infomask)
      (when (canonical-fixed-heap-row-p page start limit infomask layout)
        (let ((first (+ start offset)))
          (values (funcall span-compare page first (+ first +sql-uuid-size+)) t))))))

(defun make-top-n-uuid-rejector (columns fixed-layout cutoff)
  "Reject canonical UUID rows whose key cannot enter the full heap.
A later row with an equal key loses the arrival-order tie breaker."
  (when (and cutoff fixed-layout)
    (let ((slot (top-n-key-cutoff-slot cutoff))
          (descending-p (top-n-key-cutoff-descending-p cutoff)))
      (when (and (< slot (length columns))
                 (eq (pg-column-type (aref columns slot)) :uuid))
        (let ((predicate
                (make-fixed-uuid-page-predicate
                 fixed-layout slot
                 (lambda (page start end)
                   (declare (ignore end)
                            (type (simple-array (unsigned-byte 8) (*)) page)
                            (type fixnum start)
                            (optimize (speed 3)))
                   (let* ((key (sql-uuid-bytes (top-n-key-cutoff-value cutoff)))
                          (comparison
                            (loop for i of-type fixnum below +sql-uuid-size+
                                  for difference = (- (aref page (the fixnum (+ start i)))
                                                      (aref key i))
                                  unless (zerop difference) return difference
                                    finally (return 0))))
                     (if descending-p (<= comparison 0) (>= comparison 0)))))))
          (lambda (page start limit infomask)
            (and (sql-uuid-p (top-n-key-cutoff-value cutoff))
                 (funcall predicate page start limit infomask))))))))

(defun make-adaptive-top-n-page-predicate (reject-row-p)
  "Pause cutoff comparisons when consecutive comparable rows pass the cutoff."
  (let ((misses 0)
        (skip 0))
    (declare (type fixnum misses skip))
    (lambda (page start limit infomask)
      (if (plusp skip)
          (progn (decf skip) (values nil nil))
          (multiple-value-bind (reject comparable)
              (funcall reject-row-p page start limit infomask)
            (cond (reject (setf misses 0))
                  ((and comparable (= (incf misses) 8))
                   (setf misses 0 skip 64)))
            (values nil reject))))))

(defun decode-page-filter-match (page columns start limit fixed-layout key-slot)
  "Decode an accepted tuple, skipping unused payload values for a count join.
KEY-SLOT may be a decoder specialized for the accepted inline tuple."
  (cond ((functionp key-slot) (funcall key-slot page start limit))
        ((and fixed-layout key-slot)
         (let ((row (make-array (length columns) :initial-element +null+)))
           (fill-fixed-heap-row row page start (cdr fixed-layout) key-slot (1+ key-slot))
           row))
        (fixed-layout
         (let ((row (make-array (length columns))))
           ;; The page predicate already validated the fixed layout.
           (fill-fixed-heap-row row page start (cdr fixed-layout) 0 (length row))
           row))
        (key-slot
         ;; The span comparison already tested the predicate. A demand for an
         ;; integer slot lets the general decoder step over all varlena data.
         (deform-heap-tuple page columns :start start :limit limit
                                         :metadata :none :demand key-slot))
        (t (decode-inline-select-row page columns start limit))))

(defun make-visible-heap-page-reader (page columns snapshot
                                      &optional values-mode fixed-layout filter-slot accept-value count-only-p page-predicate key-slot scratch)
  "Visit visible heap tuples from PAGE one at a time.

Check tuple visibility before deforming it, and leave later tuples untouched
until their next pull. The enclosing scan owns the page handle and snapshot.
PAGE-PREDICATE can accept or discard a visible row before decoding; its second
return value reports whether it handled that tuple layout.
If ACCEPT-VALUE is supplied, read FILTER-SLOT before decoding a canonical row.
COUNT-ONLY-P tallies matches for the page and returns their count in one pull.
When KEY-SLOT or a specialized decoder is supplied, a match may yield only its key.
Return the row, a presence flag, and optional compression provenance."
  (declare (inline page-get-lp))
  (let ((offnum 0)
        (count (page-lp-count page))
        (visible-row (make-heap-page-visibility-reader page snapshot)))
    (declare (type fixnum offnum count))
    (lambda ()
      (loop with matches of-type fixnum = 0
            while (< offnum count)
            do (check-for-interrupts)
               (incf offnum)
               (with-live-page-item (page offnum offset length count)
                 (multiple-value-bind (visible infomask)
                     (funcall visible-row offset (and *backend* (backend-xid *backend*)))
                   (when (and visible
                              (or (not page-predicate)
                                  (multiple-value-bind (matched handled)
                                      (funcall page-predicate page offset (+ offset length) infomask)
                                    (when (and handled matched)
                                      (if count-only-p
                                          (incf matches)
                                          (return
                                            (values
                                             (decode-page-filter-match page columns offset (+ offset length)
                                                                       fixed-layout key-slot)
                                             t))))
                                    (not handled))))
                     (if accept-value
                         (let* ((limit (+ offset length))
                                (canonical (and fixed-layout
                                                (canonical-fixed-heap-row-p
                                                 page offset limit infomask fixed-layout)))
                                (attribute (when canonical (aref (cdr fixed-layout) filter-slot)))
                                (row (unless canonical
                                       (deform-heap-tuple page columns :start offset
                                         :limit limit :metadata :none)))
                                (value (if canonical
                                           (funcall (cdr attribute) page (+ offset (car attribute)))
                                           (aref row filter-slot))))
                           (when (and (eq values-mode :inline)
                                      (some #'heap-external-varlena-p row))
                             (ereport "XX001" "external value has no owning TOAST relation"))
                           (when (funcall accept-value value)
                             (if count-only-p
                                 (incf matches)
                                 (return
                                   (values (or row
                                               (if key-slot
                                                   (decode-page-filter-match
                                                    page columns offset limit fixed-layout key-slot)
                                                   (decode-canonical-fixed-heap-row
                                                    page offset fixed-layout filter-slot value)))
                                           t)))))
                         (progn
                           (case values-mode
                             (:int4
                              (return (values (vector (single-int4-heap-tuple-value
                                                       page offset (+ offset length) infomask))
                                              t)))
                             ((:fixed :inline)
                              (let* ((limit (+ offset length))
                                     (decoded
                                       (and fixed-layout
                                            (decode-fixed-heap-row page offset limit infomask fixed-layout (car scratch)))))
                                (when decoded (return (values decoded t)))
                                (let ((row (deform-heap-tuple page columns :start offset
                                                                           :limit limit :metadata :none :destination (car scratch))))
                                  (when (and (eq values-mode :inline)
                                             (some #'heap-external-varlena-p row))
                                    (ereport "XX001" "external value has no owning TOAST relation"))
                                  (return (values row t))))))
                           (multiple-value-bind (values physical-compressed)
                               (deform-heap-tuple page columns :start offset :limit (+ offset length)
                                 :metadata :compression)
                             (return (values values t physical-compressed))))))))
            finally (return (if (plusp matches) (values matches t) (values nil nil)))))))

(defun make-buffered-select-index-tid-iterator (table path environment &optional (key nil key-p))
  ;; Buffer matching TIDs under the index lease, then release the lease before
  ;; fetching heap tuples. The buffer may spill and is owned by the iterator.
  (let ((*datum-origin-capture* nil))
    (make-buffered-sql-iterator
     (lambda (emit)
       (let* ((index (select-scan-path-index path))
              (key (if key-p key (select-scan-index-key table path environment))))
         (when key
           (call-with-current-select-index-domain
            (pg-index-relfilenode index)
            (lambda ()
              (call-with-select-btree-cursor
               (pg-index-relfilenode index) key
               (lambda (cursor lease)
                 (call-with-select-index-cursor
                  cursor lease
                  (lambda (tid)
                    (check-for-interrupts)
                    (funcall emit (vector (car tid) (cdr tid)))))))))))
       (detach-select-index-scans)))))

(defun make-select-index-tid-iterator (table path environment)
  "Pull TIDs under the active index group, or read a detached scan's sealed tail.
The group owns the borrowed lease. A pause copies unread TIDs before the group
releases it; closing an active scan discards its unread cursor without a copy."
  (unless *iterator-index-scan-domain-p*
    (return-from make-select-index-tid-iterator
      (make-buffered-select-index-tid-iterator table path environment)))
  (let ((node (pg-index-relfilenode (select-scan-path-index path)))
        (scan nil)
        (buffered nil)
        (store nil)
        (reader nil)
        (started-p nil))
    (make-sql-iterator
     (lambda ()
       (block next
         (unless started-p
           (setf started-p t)
           (let ((key (select-scan-index-key table path environment)))
             (when key
               (if (select-index-domain-covered-p *select-index-lease-domain*)
                   (call-with-current-select-index-domain
                    node
                    (lambda ()
                      (let ((lease
                              (loop for group in *select-index-lease-groups*
                                    for lease = (cdr (assoc node (select-index-lease-group-leases group)))
                                    when (and lease (btree-read-lease-active-p lease)) return lease)))
                        (unless lease (error "iterator index scan requires an active group lease"))
                        (setf scan (make-select-index-scan
                                    :cursor (open-select-index-cursor-unlocked node key)
                                    :lease lease :borrowed-lease-p t))
                        (push scan *select-index-scans*))))
                   ;; A prior park may have released the surrounding group.
                   ;; A temporary repair group cannot lend a cursor across pulls.
                   (setf buffered (make-buffered-select-index-tid-iterator table path environment key))))))
         (when buffered (return-from next (sql-iterator-next buffered)))
         (when scan
           (let ((spool (select-index-scan-spool scan)))
             (if spool
                 (progn
                   (unless reader
                     (setf store (adopt-sealed-query-row-spool spool)
                           reader (make-query-replay-reader store)))
                   (sql-iterator-next reader))
                 (progn
                   (unless (btree-read-lease-active-p (select-index-scan-lease scan))
                     (error "iterator index scan outlived its group lease"))
                   (let ((tid (select-index-cursor-next-tid (select-index-scan-cursor scan))))
                     (if tid
                         (values (vector (car tid) (cdr tid)) t)
                         (progn
                           (setf *select-index-scans* (remove scan *select-index-scans* :test #'eq)
                                 scan nil)
                           (values nil nil))))))))))
     (lambda ()
       (unwind-protect (close-sql-iterator buffered)
         (when scan
           (if store
               (close-query-replay-store store)
               (when (select-index-scan-spool scan)
                 (close-query-row-spool (select-index-scan-spool scan)))))))
     (lambda ()
       (setf *select-index-scans* (remove scan *select-index-scans* :test #'eq))))))

(defun make-select-index-scan-iterator (table path snapshot relation relations environment capture-p demand)
  ;; Fetch and check one TID at a time against the scan snapshot. Closing this
  ;; iterator releases its cursor or detached TID store.
  (let ((tids (make-select-index-tid-iterator table path environment))
        (presence (vector (ensure-select-source-identity relation)))
        (placement (make-select-scan-row-placement relations relation)))
    (make-sql-iterator
     (lambda ()
       (loop
         (multiple-value-bind (tid present) (sql-iterator-next tids)
           (unless present (return (values nil nil nil)))
           (let* ((*datum-origin-capture* capture-p)
                  (tuple (heapam-get-tuple (pg-class-relfilenode table)
                                           (aref tid 0) (aref tid 1)
                                           (pg-class-columns table))))
             (when (and tuple (htup-visible-p tuple snapshot))
               (let ((row (htup-values (materialize-select-tuple table tuple snapshot :demand demand)))
                     (origins (and capture-p (htup-origins tuple))))
                 (multiple-value-bind (projected-row projected-origins)
                     (if placement (funcall placement row origins) (values row origins))
                   (return (values projected-row t projected-origins nil presence)))))))))
     (lambda () (close-sql-iterator tids)))))

(defun single-int4-scan-layout-p (columns)
  (and (= (length columns) 1)
       (eq (pg-column-type (aref columns 0)) :int4)
       (= (column-align (aref columns 0)) 4)))

(defun make-scan-span-page-predicate (columns filter-slot span-compare values-mode fixed-layout)
  (when (and span-compare (< filter-slot (length columns)))
    (case (pg-column-type (aref columns filter-slot))
      ((:text :varchar)
       (when (eq values-mode :inline)
         (make-inline-text-page-predicate columns filter-slot span-compare)))
      (:uuid
       (when fixed-layout
         (make-fixed-uuid-page-predicate fixed-layout filter-slot span-compare))))))

(defun make-select-scan-iterator (node relations &optional filter-slot accept-value count-only-p span-compare key-slot scratch)
  ;; Capture the parameter environment and statement snapshot for this scan's
  ;; lifetime; later outer rows must not change its index key or visibility.
  ;; The page handle keeps one immutable version across pulls.  Release it when
  ;; the page reader is exhausted, or via iterator cleanup on close or error;
  ;; returned tuples and TOAST context must not depend on the handle's lifetime.
  (let* ((relation (physical-scan-relation node relations))
         (table (getf relation :table))
         (presence (vector (ensure-select-source-identity relation)))
         (path (physical-scan-path node relation))
         (environment (copy-query-sort-value *lateral-outer-environment*))
         (source (getf relation :row-source))
         (placement (make-select-scan-row-placement relations relation))
         (block 0) (nblocks nil) (rows nil) (page-reader nil) (initialized nil)
         (page-handle (make-buf-page-handle))
         (snapshot *current-snapshot*)
         (capture-p *datum-origin-capture*)
         (demand *select-toast-demand*)
         (slice-plan *select-toast-slice-plan*)
         (columns (and table (pg-class-columns table)))
         (key-slot (when (and key-slot (< key-slot (length columns))
                              (member (pg-column-type (aref columns key-slot))
                                      '(:int2 :int4 :int8)))
                     key-slot))
         ;; A TOAST demand limits varlena materialization only.
         (values-mode (and (not capture-p) (not slice-plan)
                           (cond ((single-int4-scan-layout-p columns) :int4)
                                 ((and (not (pg-class-toast table))
                                       (notany #'pg-column-dropped-p columns))
                                  (if (some (lambda (column)
                                              (heap-varlena-type-p (pg-column-type column)))
                                            columns)
                                      (when (or (not demand) count-only-p key-slot) :inline)
                                      :fixed)))))
         (fixed-layout (when (eq values-mode :fixed)
                         (make-fixed-heap-row-layout columns)))
         (span-page-predicate
           (make-scan-span-page-predicate columns filter-slot span-compare values-mode fixed-layout))
         (scratch (and (not placement) (not source) (member values-mode '(:fixed :inline))
                       (eql (length (car scratch)) (length columns)) scratch))
         (page-predicate
           (or span-page-predicate
               (let ((reject-row-p (case values-mode
                                     (:inline (make-top-n-text-rejector columns (cdr scratch)))
                                     (:fixed (make-top-n-uuid-rejector columns fixed-layout (cdr scratch))))))
                 (when reject-row-p
                   (make-adaptive-top-n-page-predicate reject-row-p)))))
         (key-projection
           (or (when (and page-predicate (eq values-mode :inline))
                 (make-inline-text-key-reader columns filter-slot key-slot))
               key-slot)))
    (when (and accept-value
               (not (and (or fixed-layout page-predicate (and count-only-p (eq values-mode :inline)))
                         (not placement) (not source)
                         (eq (select-scan-path-kind path) :seq-scan)
                         (< filter-slot (length columns)))))
      (return-from make-select-scan-iterator nil))
    (when (eq (select-scan-path-kind path) :index-scan)
      (return-from make-select-scan-iterator
        (make-select-index-scan-iterator table path snapshot relation relations
                                         environment capture-p demand)))
    (unless (eq (select-scan-path-kind path) :seq-scan)
      (error "A resumable producer requires a SeqScan plan"))
    (when (and fixed-layout (cdr scratch))
      (setf (top-n-key-cutoff-item-size-bound (cdr scratch))
            (top-n-fixed-item-size-bound columns (top-n-key-cutoff-slot (cdr scratch)))))
    (make-sql-iterator
     (lambda ()
       (unless initialized
         (setf initialized t)
         (if source (setf rows (funcall source))
             (setf nblocks (buf-nblocks (pg-class-relfilenode table)))))
       (loop
         (when rows
           (check-for-interrupts)
           (let ((row (pop rows)))
             (return (values (if placement (funcall placement row nil) row)
                             t nil nil presence))))
         (when page-reader
           (multiple-value-bind (values present physical-compressed) (funcall page-reader)
             (when present
               (when values-mode
                 (return (values (if placement (funcall placement values nil) values)
                                 t nil nil presence)))
               (let* ((*datum-origin-capture* capture-p)
                      (toast-context
                        (when (and slice-plan (eq table (car slice-plan)))
                          (loop for (expression . slot) in (cdr slice-plan)
                                for value = (aref values (1- slot))
                                when (heap-external-varlena-p value)
                                  collect (cons expression
                                                (list (pg-class-toast table) value snapshot values))))))
                 (multiple-value-bind (row origins)
                     (materialize-select-row table values snapshot physical-compressed :demand demand)
                   (multiple-value-bind (projected-row projected-origins)
                       (if placement (funcall placement row origins) (values row origins))
                     (return (values projected-row t projected-origins toast-context presence))))))
             (setf page-reader nil)
             (buf-release-page page-handle)))
         ;; The page reader checks each line pointer before decoding it.
         ;; Check again only when leaving that reader or fetching another page.
         (check-for-interrupts)
         (when (or source (>= block nblocks)) (return (values nil nil)))
         (let* ((current block)
                (page (buf-acquire-read-page page-handle
                                             (pg-class-relfilenode table) current)))
           (incf block)
           (when page
             (setf page-reader (make-visible-heap-page-reader
                                page columns snapshot values-mode fixed-layout filter-slot accept-value
                                count-only-p page-predicate key-projection scratch))))))
     nil
     (lambda ()
       (setf rows nil page-reader nil)
       (buf-release-page page-handle)))))

(defun make-append-sql-iterator (arms relations)
  (let ((remaining arms) (input nil) (coercions nil))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless input
           (unless remaining (return (values nil nil)))
           (let ((arm (pop remaining)))
             (setf coercions (first arm)
                   input (make-physical-plan-iterator (second arm) relations))))
         (multiple-value-bind (row present origins toast presence) (sql-iterator-next input)
           (declare (ignore toast))
           (let ((*query-row-presence* presence))
             (if present
                 (multiple-value-bind (projected result-origins)
                     (project-append-row coercions (car row) origins)
                   (return (values projected t result-origins nil presence)))
                 (progn (close-sql-iterator input) (setf input nil)))))))
     (lambda () (close-sql-iterator input)))))

(defun make-project-set-iterator (input calls &key attempt-function)
  "Keep the current input row until all of its sets are exhausted."
  (let ((width (length calls))
        (environment nil) (input-origins nil) (input-presence nil) (sets nil))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless sets
           (multiple-value-bind (row present origins toast presence) (sql-iterator-next input)
             (declare (ignore toast))
             (unless present (return (values nil nil)))
             (setf environment row input-origins origins input-presence presence
                   sets (map 'simple-vector #'identity calls))))
         (let* ((*query-row-origins* input-origins)
                (*query-row-presence* input-presence)
                (values (project-set-next-values environment sets attempt-function)))
           (if values
               (return (values (concatenate 'vector environment values) t
                               (project-set-row-origins input-origins (length environment) width)
                               nil input-presence))
               (setf sets nil)))))
     (lambda ()
       (setf sets nil environment nil input-origins nil)
       (close-sql-iterator input)))))

(defun make-project-set-projection-iterator (input calls targets width)
  "Project targets in order on each attempt to expand an input row.

Step each bare set target when reached, creating its iterator and evaluating
its arguments lazily.  Scalar targets also run on the final attempt, even when
all sets are empty and no row is emitted.  Closing the result closes INPUT."
  (let ((attempt-values nil) (attempt-origins nil)
                             (capture-p *datum-origin-capture*))
    (let ((expanded
            (make-project-set-iterator
             input calls
             :attempt-function
             (lambda (environment set-values step-set)
               (setf attempt-values (make-array (length targets))
                     attempt-origins
                     (and capture-p (make-array (length targets) :initial-element nil)))
               (loop for target in targets for index from 0
                     for expression = (getf target :expr)
                     do (if (and (eq (first expression) :col)
                                 (< width (second expression))
                                 (<= (second expression) (+ width (length calls))))
                            (let ((slot (- (second expression) width 1)))
                              (funcall step-set slot)
                              (setf (aref attempt-values index) (aref set-values slot)))
                            (multiple-value-bind (value origin)
                                (if capture-p
                                    (eval-expr-with-origin expression environment *query-row-origins*)
                                    (values (eval-expr expression environment) nil))
                              (setf (aref attempt-values index) value)
                              (when capture-p
                                (setf (aref attempt-origins index) origin)))))))))
      (make-mapping-sql-iterator
       expanded
       (lambda (environment)
         (values (cons attempt-values environment) t attempt-origins))))))

(defmacro make-limit-sql-iterator (count input &key lazy)
  "Validate COUNT on pull and forward the input row and its metadata.
LAZY defers INPUT until the first allowed pull. Expand at the call site so an
existing input needs no lazy-child captures or branch."
  (let ((remaining (gensym "LIMIT-"))
        (iterator (gensym "INPUT-")))
    `(let ((,remaining ,count)
           (,iterator ,(unless lazy input)))
       (make-sql-iterator
        (lambda ()
          (check-select-limit ,remaining)
          (when (plusp ,remaining)
            ,@(when lazy `((unless ,iterator (setf ,iterator ,input))))
            (decf ,remaining)
            (sql-iterator-next ,iterator)))
        (lambda () (close-sql-iterator ,iterator))))))

(defun sorted-project-set-keys (order deferred target-count)
  "Return normalized sort keys and their extra source expressions in SQL order."
  (let ((extra nil) (slot target-count))
    (values
     (loop for entry in order
           for expression = (car entry)
           collect (if (eq (first expression) :sort-output)
                       (progn
                         (setf (aref deferred (1- (second expression))) nil)
                         entry)
                       (progn
                         (push expression extra)
                         (cons (list :sort-output (incf slot)
                                     :type (expr-type expression) :typmod -1)
                               (cdr entry)))))
     (nreverse extra))))

(defun make-sorted-project-set-iterator (node project set-node width calls relations metadata)
  "Sort source rows before expansion when no sort key depends on a set result.

Cache ordinary targets and their origins per source row.  Source origins travel
through the sort after the target origins, then accompany each expanded input.
Bare set targets use target-list attempt order; wrapped set results remain a
separate projection. Closing the result closes the sort and its source."
  (let* ((targets (second project))
         (target-count (length targets))
         (capture-p *datum-origin-capture*)
         (deferred
           (map 'vector
                (lambda (target)
                  (or (project-set-expression-dependent-p
                       (getf target :expr) width (length calls) targets)
                      (sort-projection-volatile-p (getf target :expr))))
                targets))
         (bare-p (project-set-projection-targets-p targets width (length calls))))
    (multiple-value-bind (order extra) (sorted-project-set-keys (second node) deferred target-count)
      (let* ((cache nil)
             (cache-origins nil)
             (attempt-values nil)
             (attempt-origins nil)
             (transform
               (lambda (environment)
                 (multiple-value-bind (row origins)
                     (make-sort-project-input-row environment targets deferred)
                   (let ((keys (map 'vector
                                    (lambda (expression) (eval-expr expression environment))
                                    extra)))
                     (setf (car row) (concatenate 'vector (car row) keys))
                     (values row t origins)))))
             (source (make-mapping-sql-iterator
                      (make-physical-plan-iterator (third set-node) relations) transform))
             (sorted (make-select-sort-iterator source order :metadata metadata))
             (input
               (make-mapping-sql-iterator
                sorted
                (lambda (row)
                  (unless (= (length (cdr row)) width)
                    (error "ProjectSet input width differs from its plan"))
                  (setf cache (subseq (car row) 0 target-count)
                        cache-origins (and capture-p
                                           (subseq *query-row-origins* 0 target-count)))
                  (values (cdr row) t
                          (and capture-p (subseq *query-row-origins* target-count))))))
             (expanded
               (instrument-physical-plan-iterator
                set-node
                (make-project-set-iterator
                 input calls
                 :attempt-function
                 (when bare-p
                   (lambda (environment set-values step-set)
                     (setf attempt-values (copy-seq cache)
                           attempt-origins (and capture-p (copy-seq cache-origins)))
                     (loop for target in targets for index from 0
                           for expression = (getf target :expr)
                           do (cond
                                ((and (eq (first expression) :col)
                                      (< width (second expression))
                                      (<= (second expression) (+ width (length calls))))
                                 (let ((slot (- (second expression) width 1)))
                                   (funcall step-set slot)
                                   (setf (aref attempt-values index) (aref set-values slot))))
                                ((aref deferred index)
                                 (multiple-value-bind (value origin)
                                     (if capture-p
                                         (eval-expr-with-origin expression environment *query-row-origins*)
                                         (values (eval-expr expression environment) nil))
                                   (setf (aref attempt-values index) value)
                                   (when attempt-origins
                                     (setf (aref attempt-origins index) origin)))))))))))
             (output
               (instrument-physical-plan-iterator
                project
                (make-mapping-sql-iterator
                 expanded
                 (lambda (environment)
                   (if bare-p
                       (values (cons attempt-values environment) t attempt-origins)
                       (let ((*query-row-origins*
                               (and capture-p
                                    (concatenate 'vector cache-origins *query-row-origins*))))
                         (multiple-value-bind (row origins)
                             (finish-sort-project-row (cons cache environment) targets deferred)
                           (values row t origins)))))))))
        (if (eq (first node) :top-n)
            (make-limit-sql-iterator (third node) output)
            output)))))

(defun make-lateral-join-iterator (join left right-factory &optional metadata)
  ;; Each left row owns a fresh right iterator and parameter epoch.
  (let ((qual (getf join :qual))
        (right nil) (current-left nil) (left-origins nil) (left-presence nil) (left-present nil) (matched nil)
        (presence-cache (list nil))
        (null-right (make-array (getf join :right-width) :initial-element +null+)))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless left-present
           (multiple-value-bind (row present origins toast presence) (sql-iterator-next left)
             (declare (ignore toast))
             (setf current-left row left-present present
                   left-origins origins left-presence presence))
           (unless left-present (return (values nil nil)))
           (setf matched nil right (funcall right-factory current-left left-origins left-presence)))
         (multiple-value-bind (row present right-origins toast right-presence) (sql-iterator-next right)
           (declare (ignore toast))
           (if present
               (multiple-value-bind (joined origins presence)
                   (qualify-select-join-pair join qual current-left row left-origins right-origins
                                             left-presence right-presence *datum-origin-capture* presence-cache)
                 (cond (joined
                        (setf matched t)
                        (return (values joined t origins nil presence)))
                       (metadata
                        (incf (plan-node-metadata-filtered-row-count metadata)))))
               (progn
                 (close-sql-iterator right)
                 (setf right nil left-present nil)
                 (when (and (not matched) (eq (getf join :type) :left))
                   (multiple-value-bind (values origins)
                       (null-extend-select-join-row current-left left-origins null-right nil *datum-origin-capture*)
                     (return (values values t origins nil left-presence)))))))))
     (lambda ()
       (close-sql-iterator right)
       (close-sql-iterator left)
       (setf current-left nil left-origins nil)))))

(defun integer-join-candidate-p (left right offsets)
  "Reject nonmatches without copying a replayed row or combining environments."
  (let ((a (aref left (car offsets)))
        (b (aref right (cdr offsets))))
    (cond ((or (eq a +null+) (eq b +null+)) nil)
          ((and (integerp a) (integerp b)) (values (= a b) t))
          (t t))))

(defun cached-integer-join-counts (store right-offset)
  "Index a small, sealed inner replay by non-NULL integer key."
  (let ((rows (query-replay-store-memory-rows store))
        (metadata-p (or (query-replay-store-capture-origins-p store)
                        (query-replay-store-capture-presence-p store))))
    (when (<= (length rows) 32)
      (let ((counts (make-hash-table :test #'eql)))
        (loop for value across rows
              for right = (if metadata-p (first value) value)
              for key = (aref right right-offset)
              do (cond ((integerp key) (incf (gethash key counts 0)))
                       ((not (eq key +null+))
                        (return-from cached-integer-join-counts nil))))
        counts))))

;; The join owns LEFT and a replay store owning RIGHT.  A fresh reader scans
;; RIGHT for each left row; right/full joins make one final pass for unmatched
;; rows.  Closing the join closes the active reader, both inputs and match map.
(defun make-nested-loop-iterator (join left right &optional metadata count-only-p)
  (let* ((qual (getf join :qual))
         (outer-p (member (getf join :type) '(:right :full)))
         (spool-memory (nested-loop-spool-memory outer-p))
         (match-memory (nested-loop-match-memory spool-memory))
         (store (make-query-replay-store right spool-memory))
         (reader nil) (rewind-reader nil) (current-left nil) (left-origins nil) (left-presence nil) (left-present nil)
         (key-offsets (integer-join-key-offsets join qual))
         (batch-count-p (and count-only-p key-offsets (eq (getf join :type) :inner)))
         (inner-counts :unbuilt)
         (candidate-proven-p nil)
         (candidate-p (when key-offsets
                        (lambda (row)
                          (multiple-value-bind (candidate proven)
                              (integer-join-candidate-p current-left row key-offsets)
                            (setf candidate-proven-p proven)
                            (if (and candidate proven count-only-p) :count-match candidate)))))
         (left-done nil) (left-seen nil) (matched nil) (right-phase nil)
         (right-index 0)
         (matches nil)
         (presence-cache (list nil))
         (null-left (make-array (getf join :left-width) :initial-element +null+))
         (null-right (make-array (getf join :right-width) :initial-element +null+)))
    (make-sql-iterator
     (lambda ()
       (loop
         (unless (or left-present left-done)
           (multiple-value-bind (row present origins toast presence) (sql-iterator-next left)
             (declare (ignore toast))
             (setf current-left row left-present present
                   left-origins origins left-presence presence))
           (unless left-present (setf left-done t))
           (when left-present
             (when (and outer-p (not left-seen))
               (setf matches (make-query-match-map match-memory)))
             (setf left-seen t matched nil right-index 0)
             ;; A retained reader was rewound after exhausting a memory replay.
             (when (and batch-count-p reader)
               (when (eq inner-counts :unbuilt)
                 (setf inner-counts (cached-integer-join-counts store (cdr key-offsets))))
               (when inner-counts
                 (let ((key (aref current-left (car key-offsets))))
                   (when (or (integerp key) (eq key +null+))
                     (setf left-present nil)
                     (return (values (gethash key inner-counts 0) t))))))
             (unless reader
               (multiple-value-setq (reader rewind-reader)
                 (make-query-replay-reader store :candidate-p candidate-p :rewind-memory-p t)))))
         (when (and left-done (not outer-p)) (return (values nil nil)))
         (when (and left-done (not right-phase))
           (setf right-phase t right-index 0)
           (when left-seen
             (close-sql-iterator reader)
             (setf reader (make-query-replay-reader store)
                   rewind-reader nil)
             (query-match-map-finish matches)))
         ;; With no left rows, nothing will replay the right input or consult the match map.
         (multiple-value-bind (row present right-origins toast right-presence)
             (sql-iterator-next (if left-seen reader right))
           (declare (ignore toast))
           (cond
             (right-phase
              (unless present
                (unless left-seen (close-sql-iterator right))
                (return (values nil nil)))
              (let ((index right-index))
                (incf right-index)
                (unless (and left-seen (query-match-map-matched-p matches index))
                  (multiple-value-bind (values origins)
                      (null-extend-select-join-row row right-origins null-left t *datum-origin-capture*)
                    (return (values values t origins nil right-presence))))))
             (present
              (when (and matches (= (query-match-map-count matches) right-index))
                (query-match-map-append matches))
              (let ((index right-index))
                (incf right-index)
                ;; The reader still counts a rejected candidate as a right row.
                (multiple-value-bind (joined origins presence)
                    (when row
                      (if (and count-only-p candidate-proven-p)
                          #()
                          (qualify-select-join-pair
                           join qual current-left row left-origins right-origins
                           left-presence right-presence *datum-origin-capture* presence-cache
                           candidate-proven-p)))
                  (cond (joined
                         (setf matched t)
                         (when matches (query-match-map-mark matches index))
                         (return (values joined t origins nil presence)))
                        (metadata
                         (incf (plan-node-metadata-filtered-row-count metadata)))))))
             (t
              (unless (and rewind-reader (funcall rewind-reader))
                (close-sql-iterator reader)
                (setf reader nil rewind-reader nil))
              (setf left-present nil)
              (when (and (not matched) (member (getf join :type) '(:left :full)))
                (multiple-value-bind (values origins)
                    (null-extend-select-join-row current-left left-origins null-right nil *datum-origin-capture*)
                  (return (values values t origins nil left-presence)))))))))
     (lambda ()
       (close-sql-iterator reader)
       (close-sql-iterator left)
       (close-query-replay-store store)
       (when matches
         (close-query-match-map matches)
         (when (query-match-map-pathname matches)
           (delete-query-temp-file (query-match-map-pathname matches))))
       (setf current-left nil left-origins nil)))))

(defun accumulate-plan-resource-deltas (metadata usage reads hits writes spill-reads spill-writes wal-records wal-bytes)
  "Add one inclusive pull's delta without changing the query-wide counters."
  (let ((totals (or (plan-node-metadata-resources metadata)
                    (setf (plan-node-metadata-resources metadata)
                          (make-statement-resource-usage)))))
    (incf (statement-resource-usage-relation-reads totals)
          (- (statement-resource-usage-relation-reads usage) reads))
    (incf (statement-resource-usage-clean-cache-hits totals)
          (- (statement-resource-usage-clean-cache-hits usage) hits))
    (incf (statement-resource-usage-relation-writes totals)
          (- (statement-resource-usage-relation-writes usage) writes))
    (incf (statement-resource-usage-spill-read-bytes totals)
          (- (statement-resource-usage-spill-read-bytes usage) spill-reads))
    (incf (statement-resource-usage-spill-written-bytes totals)
          (- (statement-resource-usage-spill-written-bytes usage) spill-writes))
    (incf (statement-resource-usage-wal-records totals)
          (- (statement-resource-usage-wal-records usage) wal-records))
    (incf (statement-resource-usage-wal-bytes totals)
          (- (statement-resource-usage-wal-bytes usage) wal-bytes))))

(defun instrument-physical-plan-iterator (node input)
  "Count and time pulls from INPUT when execution instrumentation is enabled."
  (unless *query-plan-instrumentation-p*
    (return-from instrument-physical-plan-iterator input))
  (let* ((metadata (and *cte-active-plan* (gethash node (query-plan-metadata *cte-active-plan*))))
         (started-p nil))
    (if (null metadata) input
        (make-sql-iterator
         (lambda ()
           (let* ((started (when (eq *query-plan-instrumentation-p* t)
                             (funcall *statement-observation-clock*)))
                  (first-p (not started-p))
                  (usage (and *query-plan-instrumentation-p* *statement-resource-usage*))
                  (reads (when usage (statement-resource-usage-relation-reads usage)))
                  (hits (when usage (statement-resource-usage-clean-cache-hits usage)))
                  (writes (when usage (statement-resource-usage-relation-writes usage)))
                  (spill-reads (when usage (statement-resource-usage-spill-read-bytes usage)))
                  (spill-writes (when usage (statement-resource-usage-spill-written-bytes usage)))
                  (wal-records (when usage (statement-resource-usage-wal-records usage)))
                  (wal-bytes (when usage (statement-resource-usage-wal-bytes usage))))
             (when *query-plan-instrumentation-p*
               (unless started-p
                 (incf (plan-node-metadata-actual-loops metadata))
                 (setf started-p t)))
             (unwind-protect
                  (multiple-value-bind (row present origins toast-context presence) (sql-iterator-next input)
                    (when (and *query-plan-instrumentation-p* present)
                      (incf (plan-node-metadata-actual-row-count metadata)))
                    (values row present origins toast-context presence))
               (when usage
                 (accumulate-plan-resource-deltas metadata usage reads hits writes
                                                  spill-reads spill-writes wal-records wal-bytes))
               (when started
                 (let ((elapsed (- (funcall *statement-observation-clock*) started)))
                   (incf (plan-node-metadata-actual-total-time-ticks metadata) elapsed)
                   (when first-p
                     (incf (plan-node-metadata-actual-startup-time-ticks metadata) elapsed)))))))
         (lambda () (close-sql-iterator input))))))

(defun make-physical-plan-iterator (node relations)
  (instrument-physical-plan-iterator node (%make-physical-plan-iterator node relations)))

(defun make-resumable-query-plan-iterator (plan relations &key projected-only-p)
  ;; Restore the plan's TOAST demand and instrumentation context for each pull;
  ;; the caller owns the returned iterator and closes it when the plan ends.
  (multiple-value-bind (demand slice-plan)
      (select-plan-toast-access (query-plan-physical plan) relations)
    (let* ((*select-toast-demand* demand)
           (*select-toast-slice-plan* slice-plan)
           (*toast-slice-current* nil)
           (*cte-active-plan* plan)
           (*select-terminal-sort-node* (and projected-only-p (query-plan-physical plan)))
           (input (make-physical-plan-iterator (query-plan-physical plan) relations)))
      (make-sql-iterator
       (lambda ()
         (let ((*select-toast-demand* demand)
               (*select-toast-slice-plan* slice-plan)
               (*toast-slice-current* nil)
               (*cte-active-plan* plan))
           (multiple-value-bind (row present origins toast-context presence) (sql-iterator-next input)
             (declare (ignore toast-context))
             (values row present origins nil presence))))
       (lambda () (close-sql-iterator input))))))

(defun map-query-plan-rows (plan relations function &key projected-only-p)
  "Consume a plan in the current statement domain, closing it even on early stop."
  (let* ((root-p (not *subquery-execution-active-p*))
         (*subquery-result-cache* (if root-p (make-hash-table :test #'eq) *subquery-result-cache*))
         (*subquery-registered-relations* (if root-p relations *subquery-registered-relations*))
         (*subquery-outer-frames* (if root-p nil *subquery-outer-frames*))
         (*subquery-execution-active-p* t)
         (*subquery-instrumentation-depth* (1+ *subquery-instrumentation-depth*))
         (new-scan-scope-p (not *iterator-index-scan-domain-p*))
         (*iterator-index-scan-domain-p* t))
    (when (and *query-plan-instrumentation-p* (= *subquery-instrumentation-depth* 1))
      (reset-query-plan-tree-instrumentation plan))
    ;; Nested consumers can open a shared CTE producer that outlives their
    ;; reader. Register those cursors in the enclosing execution's scan scope.
    (progv (when new-scan-scope-p '(*select-index-scans*))
        (when new-scan-scope-p (list *select-index-scans*))
      (call-with-query-cte-execution
       root-p
       (lambda ()
         (call-with-select-index-lease-domain
          (query-plan-index-relfilenodes plan relations)
          (lambda ()
            (let ((iterator (if projected-only-p
                                (make-resumable-query-plan-iterator
                                 plan relations :projected-only-p t)
                                (make-resumable-query-plan-iterator plan relations))))
              (unwind-protect
                   (map-sql-iterator
                    iterator
                    (lambda (row)
                      (let ((*select-toast-demand* nil)
                            (*select-toast-slice-plan* nil)
                            (*toast-slice-current* nil))
                        (funcall function row))))
                (close-sql-iterator iterator))))))))))

(defun identity-projection-input (project relations)
  "Return the input node when PROJECT exposes exactly its scan's slots in order."
  (when (eq (first project) :project)
    (let* ((input (third project))
           (scan (loop for step = input then (third step)
                       while (eq (first step) :filter)
                       finally (return step)))
           (relation (when (member (first scan) '(:seq-scan :index-scan :subquery-scan))
                       (physical-scan-relation scan relations)))
           (targets (second project)))
      (when (and relation (not (getf relation :join-order-base))
                 (eql (length targets) (getf relation :width))
                 (loop for target in targets for slot from 1
                       for expr = (getf target :expr)
                       always (and (eq (first expr) :col) (eql (second expr) slot))))
        input))))

(defun view-identity-projection-input (node relations)
  "Return the row-vector producer when a view projects every slot unchanged."
  (when (and (not *datum-origin-capture*) (not *query-plan-instrumentation-p*)
             (getf (getf (physical-scan-relation node relations) :subquery-prepared)
                   :stored-view-expansion-p))
    (identity-projection-input (fourth node) relations)))

(defun make-subquery-scan-iterator (node relations &optional count-only-p key-slot scratch)
  (multiple-value-bind (frame presence)
      (make-physical-subquery-scan-frame node relations *lateral-outer-environment*)
    (let ((frames (cons frame *subquery-outer-frames*))
          (raw-input (view-identity-projection-input node relations))
          (input nil)
          (iterator nil))
      (flet ((close-input (owned)
               (unwind-protect (close-sql-iterator owned)
                 (close-cte-frame-cache (cdr frame)))))
        (unwind-protect
             (let ((*subquery-outer-frames* frames))
               (setf input (cond ((and raw-input count-only-p)
                                  (make-count-input-iterator raw-input relations))
                                 ((and raw-input key-slot)
                                  (make-count-join-probe-iterator raw-input relations key-slot))
                                 ((and raw-input scratch)
                                  (make-top-n-identity-scan-iterator raw-input relations scratch))
                                 (t (make-physical-plan-iterator
                                     (or raw-input (fourth node)) relations))))
               (let ((owned input))
                 (setf iterator
                       (make-sql-iterator
                        (lambda ()
                          (let ((*subquery-outer-frames* frames))
                            (multiple-value-bind (row present origins)
                                (sql-iterator-next-after-poll owned)
                              (values (if raw-input row (car row))
                                      present origins nil (and present presence)))))
                        (lambda () (close-input owned))))))
          (unless iterator (close-input input)))))))

(defun make-top-n-identity-scan-iterator (node relations scratch)
  "Pass SCRATCH through row-preserving scans only. The sorter owns retained rows."
  (case (first node)
    (:seq-scan (make-select-scan-iterator node relations nil nil nil nil nil scratch))
    (:subquery-scan (make-subquery-scan-iterator node relations nil nil scratch))
    (otherwise (make-physical-plan-iterator node relations))))

(defun make-physical-hash-join-iterator (node relations)
  (let* ((join (second node))
         (qual (unless (getf join :hash-qual-covered-p) (getf join :qual)))
         (build-left-p (getf join :build-left-p))
         (probe-width (when (member (getf join :type) '(:left :full)) (getf join :right-width)))
         (build-width (when (member (getf join :type) '(:right :full)) (getf join :left-width)))
         (metadata (and *query-plan-instrumentation-p* *cte-active-plan*
                        (gethash node (query-plan-metadata *cte-active-plan*))))
         ;; A greedy nested-loop base pads its row to query width. Hash join
         ;; consumes independent scan rows and assembles the source slots itself.
         (relations (mapcar (lambda (relation)
                              (if (getf relation :join-order-base)
                                  (let ((copy (copy-list relation)))
                                    (setf (getf copy :join-order-base) nil)
                                    copy)
                                  relation))
                            relations))
         (probe (make-physical-plan-iterator (if build-left-p (fourth node) (third node)) relations))
         (build nil)
         (iterator nil))
    (unwind-protect
         (progn
           (setf build (make-physical-plan-iterator (if build-left-p (third node) (fourth node)) relations))
           (destructuring-bind (probe-slots build-slots types) (getf join :hash-keys)
             (when build-left-p
               (rotatef probe-slots build-slots)
               (rotatef probe-width build-width))
             (setf iterator
                   (make-hash-join-iterator
                    probe build probe-slots build-slots types
                    :preserve-probe-width probe-width :preserve-build-width build-width :build-left-p build-left-p
                    :stats (when metadata
                             (or (plan-node-metadata-hash-join-stats metadata)
                                 (setf (plan-node-metadata-hash-join-stats metadata) (make-hash-join-stats))))
                    :residual (when qual
                                (lambda (row)
                                  (let ((matched (true-p (eval-expr qual row))))
                                    (when (and metadata (not matched))
                                      (incf (plan-node-metadata-filtered-row-count metadata)))
                                    matched)))))))
      (unless iterator
        (unwind-protect (close-sql-iterator probe)
          (close-sql-iterator build))))))

(defun make-count-join-probe-iterator (node relations key-slot)
  "Carry a count-only equality join's key demand through transparent view projections."
  (case (first node)
    (:subquery-scan (make-subquery-scan-iterator node relations nil key-slot))
    (:filter (make-physical-filter-iterator node relations nil key-slot))
    (otherwise (make-physical-plan-iterator node relations))))

(defun make-physical-join-iterator (node relations &optional count-only-p)
  ;; A parameterized index scan needs a new key and iterator for each left
  ;; row, just as a LATERAL input does (cf. ExecReScanIndexScan).
  (let ((metadata (and *query-plan-instrumentation-p* *cte-active-plan*
                       (gethash node (query-plan-metadata *cte-active-plan*)))))
    (if (physical-join-rescans-inner-p (second node) (fourth node))
        (make-lateral-join-iterator
         (second node) (make-physical-plan-iterator (third node) relations)
         (lambda (environment origins presence)
           (let ((*lateral-outer-environment* environment)
                 (*query-row-origins* origins)
                 (*query-row-presence* presence))
             (make-physical-plan-iterator (fourth node) relations)))
         metadata)
        (let* ((join (second node))
               (left-node (third node))
               (offsets (when (and count-only-p (eq (getf join :type) :inner)
                                   (not *datum-origin-capture*))
                          (integer-join-key-offsets join (getf join :qual)))))
          (make-nested-loop-iterator
           join (if offsets
                    (make-count-join-probe-iterator left-node relations (car offsets))
                    (make-physical-plan-iterator left-node relations))
           (make-physical-plan-iterator (fourth node) relations)
           metadata count-only-p)))))

(defun make-select-sort-iterator (input order &key limit projected-only-p metadata scratch)
  "Prepare the sort on the first pull and return one sorted row per call.
The reader retains its runs until close. If there is no enclosing temporary
owner, the iterator creates one and closes it after the reader and input."
  (let ((reader nil)
        (owner nil)
        (private-owner-p nil)
        (remaining limit)
        (limit-checked nil)
        (capture-p *datum-origin-capture*))
    (make-sql-iterator
     (lambda ()
       (unless limit-checked
         (when limit (check-select-limit limit))
         (setf limit-checked t))
       (unless (eql remaining 0)
         (unless owner
           (setf owner *query-temp-cleanup-owner*)
           (unless owner
             (setf owner (make-query-temp-scope-owner
                          (or *query-temp-quota* (backend-query-temp-quota *backend*)))
                   private-owner-p t
                   (query-temp-cleanup-owner-temporary-directory owner) *query-temp-directory*)))
         (call-with-query-temp-owner
          owner
          (lambda ()
            (let ((*datum-origin-capture* capture-p))
              (unless reader
                (multiple-value-bind (sorted method)
                    (call-with-query-sort-arena
                     (lambda (arena)
                       (prepare-select-sort input order arena limit projected-only-p scratch)))
                  (setf reader sorted)
                  (when metadata
                    (pushnew method (plan-node-metadata-sort-methods metadata))))
                (close-sql-iterator input))
              (multiple-value-bind (item present) (sql-iterator-next reader)
                (when present
                  (when remaining (decf remaining))
                  (values (if projected-only-p
                              (cons (query-sort-item-row item) nil)
                              (query-sort-item-row item))
                          t (and capture-p (not projected-only-p) (datum-origins-from-spill
                                                                   (query-sort-item-with-metadata-origins item)))
                          nil (unless projected-only-p
                                (query-sort-item-with-metadata-presence item))))))))))
     (lambda ()
       (unwind-protect
            (when owner
              (unwind-protect
                   (call-with-query-temp-owner owner (lambda () (close-sql-iterator reader)) nil)
                (when private-owner-p (close-query-temp-owner owner))))
         (close-sql-iterator input))))))

(defun make-sorted-distinct-iterator (input keys)
  "Yield the first row of each group from a sorted input.
Copy the previous projected values before the input advances again."
  (let ((previous nil))
    (make-mapping-sql-iterator
     input
     (lambda (row)
       (let ((values (car row)))
         (unless (and previous (grouping-rows-equal-p keys previous values))
           (setf previous (copy-query-sort-value values))
           (values (cons values values) t *query-row-origins*)))))))

(defun make-sorted-aggregate-iterator (input group-keys descriptors)
  "Finalize one group per pull, preserving the first row of the next group.
The current group's transition states are closed before the row is returned,
including when finalization or input evaluation fails."
  (let* ((width (length group-keys))
         (keys (loop for key in group-keys for position from 1
                     collect (list :col position :type (expr-type key) :typmod (expr-typmod key))))
         (evaluated (make-mapping-sql-iterator
                     input
                     (lambda (row)
                       (multiple-value-bind (values origins)
                           (aggregate-input-row row group-keys descriptors)
                         (values (cons nil values) t origins)))))
         (ordered (if keys
                      (make-select-sort-iterator evaluated (mapcar (lambda (key) (cons key nil)) keys))
                      evaluated))
         (pending nil)
         (pending-origins nil)
         (done nil))
    (make-sql-iterator
     (lambda ()
       (unless done
         (unless pending
           (multiple-value-bind (row present origins) (sql-iterator-next ordered)
             (when present (setf pending (cdr row) pending-origins origins))))
         (if (and keys (null pending))
             (progn (setf done t) (values nil nil))
             (with-query-temp-files ()
                                    (let ((states nil)
                                          (group (if pending (subseq pending 0 width) #()))
                                          (group-origins (and pending-origins (subseq pending-origins 0 width)))
                                          (transition-origins (make-array (length descriptors) :initial-element nil)))
                                      (unwind-protect
                                           (progn
                                             (setf states (make-aggregate-transition-states descriptors))
                                             (loop while pending
                                                   do (step-aggregate-transition-origins
                                                       states descriptors pending pending-origins transition-origins width)
                                                      (step-aggregate-transition-states states descriptors pending width)
                                                      (multiple-value-bind (row present origins) (sql-iterator-next ordered)
                                                        (setf pending (and present (cdr row)) pending-origins origins))
                                                   until (and pending keys (not (grouping-rows-equal-p keys group pending))))
                                             (setf done (null pending))
                                             (values (concatenate 'vector group (aggregate-transition-results states descriptors))
                                                     t (and *datum-origin-capture*
                                                            (concatenate 'vector
                                                                         (or group-origins (make-array width :initial-element nil))
                                                                         transition-origins))))
                                        (close-aggregate-transition-states states)))))))
     (lambda () (close-sql-iterator ordered)))))

(defun make-hash-aggregate-sort-fallback-iterator (path keys count)
  "Reduce adjacent canonical keys from one non-shrinking overflow batch.
The parent owns PATH. This iterator closes its sort and input stream on exit."
  (let* ((stream nil)
         (capture-p *datum-origin-capture*)
         (input
           (make-sql-iterator
            (lambda ()
              (unless stream
                (sb-sys:without-interrupts
                  (setf stream (open-query-temp-input path))))
              (let ((record (query-spill-read-record stream)))
                (unless (eq record +query-spill-eof+)
                  (if capture-p
                      (values (cons nil (first record)) t (datum-origins-from-spill (second record)))
                      (values (cons nil record) t)))))
            (lambda () (finish-query-temp-stream stream nil))))
         (sorted (make-select-sort-iterator input (mapcar (lambda (key) (cons key nil)) keys)))
         (pending nil)
         (pending-key nil)
         (pending-origins nil)
         (done nil))
    (labels ((advance ()
               (multiple-value-bind (row present origins) (sql-iterator-next sorted)
                 (if present
                     (let ((*query-row-origins* origins))
                       (multiple-value-setq (pending pending-key pending-origins)
                         (hash-aggregate-group keys (cdr row))))
                     (setf pending nil done t)))))
      (make-sql-iterator
       (lambda ()
         (unless done
           (unless pending (advance))
           (unless done
             (let ((key pending-key)
                   (group pending)
                   (origins pending-origins)
                   (rows 0))
               (loop do (incf rows) (advance)
                     until (or done (not (equal key pending-key))))
               (values (concatenate 'vector group (make-array count :initial-element rows))
                       t (and capture-p
                              (concatenate 'vector (or origins #())
                                           (make-array count :initial-element nil))))))))
       (lambda () (close-sql-iterator sorted))))))

(defun make-hash-aggregate-batch-iterator (source keys count memory-limit depth &key input)
  "Yield in-memory states before processing overflow partitions one at a time.
The iterator owns the overflow and pending partition files. Closing it early
also closes the active child and input, then removes its remaining files."
  (let ((groups nil)
        (origins nil)
        (spill nil)
        (spill-count 0)
        (partitions nil)
        (child-path nil)
        (child nil)
        (started-p nil))
    (labels ((close-child ()
               (unwind-protect (close-sql-iterator child)
                 (setf child nil)
                 (when child-path
                   (delete-query-temp-file child-path)
                   (setf child-path nil)))))
      (make-sql-iterator
       (lambda ()
         (block next-group
           (unless started-p
             (multiple-value-setq (groups origins spill spill-count)
               (prepare-hash-aggregate-batch source keys count memory-limit))
             (setf started-p t)
             (close-sql-iterator input))
           (when groups
             (let* ((state (pop groups))
                    (group-origins (gethash state origins)))
               (remhash state origins)
               (return-from next-group
                 (values (hash-aggregate-state-row state) t
                         (and *datum-origin-capture*
                              (concatenate 'vector (or group-origins #())
                                           (make-array count :initial-element nil)))))))
           (when spill
             (multiple-value-bind (batch-paths sort-p)
                 (partition-hash-aggregate-overflow spill spill-count keys depth)
               (if sort-p
                   (setf child (make-hash-aggregate-sort-fallback-iterator spill keys count)
                         child-path spill)
                   (progn
                     (setf partitions batch-paths)
                     (delete-query-temp-file spill)))
               (setf spill nil)))
           (loop
             (unless child
               (unless partitions (return))
               (let ((path (cdr (pop partitions))))
                 (setf child-path path
                       child (make-hash-aggregate-batch-iterator
                              (lambda (emit) (hash-aggregate-spill-file path emit))
                              keys count memory-limit (1+ depth)))))
             (multiple-value-bind (row present row-origins) (sql-iterator-next child)
               (when present (return (values row t row-origins))))
             (close-child))))
       (lambda ()
         (unwind-protect
              (unwind-protect (close-child)
                (when spill (delete-query-temp-file spill))
                (hash-aggregate-delete-partitions partitions))
           (close-sql-iterator input)))))))

(defun make-ungrouped-count-iterator (input aggregate-count)
  "Consume INPUT into one COUNT(*) state, including when the input is empty."
  (let ((done-p nil))
    (make-sql-iterator
     (lambda ()
       (unless done-p
         (setf done-p t)
         (let ((rows 0))
           (unwind-protect
                (loop
                  (multiple-value-bind (row present) (sql-iterator-next input)
                    (unless present (return))
                    ;; Eligible inner joins can return a whole match count at once.
                    (incf rows (if (integerp row) row 1))))
             (close-sql-iterator input))
           (values (make-array aggregate-count :initial-element rows)
                   t (when *datum-origin-capture*
                       (make-array aggregate-count :initial-element nil))))))
     (lambda () (close-sql-iterator input)))))

(defun make-hash-aggregate-iterator (input group-keys aggregate-count &key distinct-p)
  "Consume INPUT into hash batches and return one aggregate group per pull."
  (let* ((keys (loop for key in group-keys for slot from 1
                     collect (list :col slot :type (expr-type key) :typmod (expr-typmod key))))
         (source
           (lambda (emit)
             (map-sql-iterator
              input
              (lambda (row)
                (multiple-value-bind (values key origins)
                    (hash-aggregate-group group-keys (if distinct-p (car row) row))
                  (declare (ignore key))
                  (let ((*query-row-origins* origins))
                    (funcall emit (coerce values 'vector))))))))
         (batch (make-hash-aggregate-batch-iterator
                 source keys aggregate-count (max 1 (effective-query-work-memory-bytes)) 0 :input input)))
    (if distinct-p
        (make-mapping-sql-iterator batch (lambda (row) (values (cons row row) t *query-row-origins*)))
        batch)))

(defun make-hash-distinct-iterator (input keys)
  (make-hash-aggregate-iterator input keys 0 :distinct-p t))

(defun fixed-hash-count-scan-table (node relations type width)
  (when (eq (first node) :seq-scan)
    (let* ((relation (physical-scan-relation node relations))
           (table (getf relation :table))
           (columns (and table (pg-class-columns table))))
      (when (and (= (length columns) 1)
                 (eq (pg-column-type (aref columns 0)) type)
                 (= (column-fixed-size (aref columns 0)) width)
                 (= (column-align (aref columns 0)) (min width 8))
                 (not (getf relation :row-source))
                 (not (getf relation :virtual-p)))
        table))))

(defun make-plan-node-cardinality-observer (node)
  "Instrument a fused operator that consumes or produces rows without yielding them."
  (let ((metadata (and *query-plan-instrumentation-p* *cte-active-plan*
                       (gethash node (query-plan-metadata *cte-active-plan*)))))
    (when metadata
      (lambda (operation)
        (let* ((result nil)
               (rows 0)
               (iterator
                 (instrument-physical-plan-iterator
                  node (make-sql-iterator
                        (lambda ()
                          (multiple-value-setq (result rows) (funcall operation))
                          (values nil nil))))))
          (unwind-protect (sql-iterator-next iterator)
            (close-sql-iterator iterator))
          (incf (plan-node-metadata-actual-row-count metadata) rows)
          (values result rows))))))

(defun execute-fixed-hash-count (node build-node probe-node build probe snapshot count-join)
  (let* ((observer (make-plan-node-cardinality-observer node))
         (metadata (when observer (gethash node (query-plan-metadata *cte-active-plan*))))
         (stats (when metadata
                  (or (plan-node-metadata-hash-join-stats metadata)
                      (setf (plan-node-metadata-hash-join-stats metadata) (make-hash-join-stats))))))
    (flet ((run ()
             (let ((count (funcall count-join
                                   build probe snapshot :stats stats
                                   :observe-build (make-plan-node-cardinality-observer build-node)
                                   :observe-probe (make-plan-node-cardinality-observer probe-node))))
               (values count count))))
      (declare (dynamic-extent #'run))
      (if observer (funcall observer #'run) (run)))))

(defun make-fixed-hash-count-iterator (aggregate relations)
  "Reduce a pure fixed-width scalar equality join directly to COUNT(*) when its build spills."
  (let* ((node (fourth aggregate))
         (join (second node)))
    (when (and (null (second aggregate))
               ;; The planner only uses HashAggregate for COUNT(*) transitions.
               (eql (third aggregate) 1)
               (eq (first node) :hash-join) (eq (getf join :type) :inner)
               (getf join :hash-spill-pages)
               (not *datum-origin-capture*)
               (>= (effective-query-work-memory-bytes) (* 64 1024))
               (not *query-temp-file-limit-bytes*)
               (not (and *query-temp-quota* (query-temp-quota-limit *query-temp-quota*))))
      (multiple-value-bind (left right equality-p) (equality-column-references (getf join :qual))
        (when (and equality-p (eq (expr-type left) (expr-type right))
                   (member (expr-type left) '(:int4 :oid :float4 :date :int8 :float8 :time :timestamp :timestamptz :interval))
                   (or (and (= (second left) 1) (= (second right) 2))
                       (and (= (second left) 2) (= (second right) 1))))
          (let* ((type (expr-type left))
                 (width (case type
                          ((:int4 :oid :float4 :date) 4)
                          (:interval 16)
                          (otherwise 8)))
                 (count-join (case width
                               (4 #'count-int4-heap-hash-join)
                               (8 #'count-int8-heap-hash-join)
                               (16 #'count-interval-heap-hash-join)))
                 (left-node (third node))
                 (right-node (fourth node))
                 (left-table (fixed-hash-count-scan-table left-node relations type width))
                 (right-table (fixed-hash-count-scan-table right-node relations type width))
                 (snapshot *current-snapshot*)
                 (done nil))
            (when (and left-table right-table)
              (when (getf join :build-left-p)
                (rotatef left-table right-table)
                (rotatef left-node right-node))
              (make-sql-iterator
               (lambda ()
                 (unless done
                   (setf done t)
                   (let ((count (execute-fixed-hash-count
                                 node right-node left-node right-table left-table snapshot count-join)))
                     (unless (typep count '(signed-byte 64))
                       (ereport "22003" "bigint out of range"))
                     (values (vector count) t))))))))))))

(defun scalar-scan-comparator (type)
  "Select a comparator for values whose SQL type is established by the scan."
  (case type
    ((:int2 :int4 :int8 :oid :oid8 :date :time :timestamp :timestamptz)
     (lambda (left right)
       (declare (type integer left right))
       (cond ((< left right) -1) ((> left right) 1) (t 0))))
    ((:text :varchar :name) #'sql-string-compare)
    ((:float4 :float8 :numeric :uuid :timetz :interval) #'compare-3way)))

(defun make-octet-span-comparison (literal accept)
  (declare (type (simple-array (unsigned-byte 8) (*)) literal))
  (let ((length (length literal)))
    (declare (type fixnum length))
    (lambda (page start end)
      (declare (type (simple-array (unsigned-byte 8) (*)) page)
               (type fixnum start end))
      (funcall accept
               (let ((span (the fixnum (- end start))))
                 (loop for index fixnum from start below end
                       for position fixnum from 0 below length
                       for difference fixnum = (- (aref page index) (aref literal position))
                       unless (zerop difference) return difference
                         finally (return (- span length))))))))

(defun make-uuid-span-comparison (literal accept)
  "Compare a validated sixteen-byte heap span with a UUID's owned octets.
Check the first byte before loading words, which skip shared UUID prefixes."
  (declare (type (simple-array (unsigned-byte 8) (16)) literal))
  #+x86-64
  (let ((head (aref literal 0)))
    (sb-sys:with-pinned-objects (literal)
      (let* ((sap (sb-sys:vector-sap literal))
             (key-first (sb-sys:sap-ref-64 sap 0))
             (key-last (sb-sys:sap-ref-64 sap 8)))
        (declare (type (unsigned-byte 64) key-first key-last)
                 (type (unsigned-byte 8) head)
                 (optimize (speed 3) (safety 1)))
        (flet ((compare-word (a b)
                 (declare (type (unsigned-byte 64) a b))
                 (let ((difference (logxor a b)))
                   (if (zerop difference)
                       0
                       ;; XOR with the predecessor marks the first differing bit
                       ;; and every lower bit. Round its position down to a byte.
                       (let* ((bits (logxor difference (1- difference)))
                              (offset (logand (1- (logcount bits)) -8)))
                         (- (ldb (byte 8 offset) a) (ldb (byte 8 offset) b)))))))
          (declare (inline compare-word))
          (lambda (page start end)
            (declare (ignore end)
                     (type (simple-array (unsigned-byte 8) (*)) page)
                     (type fixnum start))
            (let ((lead (- (aref page start) head)))
              (funcall accept
                       (if (zerop lead)
                           (sb-sys:with-pinned-objects (page)
                             (let* ((sap (sb-sys:vector-sap page))
                                    (first (compare-word (sb-sys:sap-ref-64 sap start) key-first)))
                               (if (zerop first)
                                   (compare-word (sb-sys:sap-ref-64 sap (the fixnum (+ start 8))) key-last)
                                   first)))
                           lead))))))))
  #-x86-64
  (make-octet-span-comparison literal accept))

(defun make-ascii-span-comparison (constant accept)
  (when (and (stringp constant)
             (every (lambda (character) (< (char-code character) 128)) constant))
    (make-octet-span-comparison
     (map '(simple-array (unsigned-byte 8) (*)) #'char-code constant) accept)))

(defun signed-integer-filter-operator (operator reversed-p)
  (cond ((string= operator "=") #'=)
        ((string= operator "<>") #'/=)
        ((string= operator "<") (if reversed-p #'> #'<))
        ((string= operator ">") (if reversed-p #'< #'>))
        ((string= operator "<=") (if reversed-p #'>= #'<=))
        ((string= operator ">=") (if reversed-p #'<= #'>=))))

(defun make-scalar-column-comparison-filter (expression &optional heap-filter-p)
  "Resolve slots and operator spelling once for a scalar column-to-literal filter."
  (when (eq (first expression) :binop)
    (let* ((operator (second expression))
           (left (third expression))
           (right (fourth expression))
           (reversed-p (eq (first right) :col))
           (column (if reversed-p right left))
           (literal (if reversed-p left right))
           (compare (scalar-scan-comparator (expr-type column)))
           (accept
             (cond ((string= operator "=") #'zerop)
                   ((string= operator "<>") (complement #'zerop))
                   ((string= operator "<") (if reversed-p #'plusp #'minusp))
                   ((string= operator ">") (if reversed-p #'minusp #'plusp))
                   ((string= operator "<=") (complement (if reversed-p #'minusp #'plusp)))
                   ((string= operator ">=") (complement (if reversed-p #'plusp #'minusp))))))
      (when (and compare accept (eq (first column) :col) (eq (first literal) :const)
                 (or (eq (expr-type column) (expr-type literal))
                     (and (member (expr-type column) '(:int2 :int4 :int8))
                          (member (expr-type literal) '(:int2 :int4 :int8)))))
        (let* ((slot (1- (second column)))
               (constant (second literal))
               (integer-operator (when (member (expr-type column) '(:int2 :int4 :int8))
                                   (signed-integer-filter-operator operator reversed-p)))
               (accept-value
                 (if integer-operator
                     (lambda (value)
                       (and (not (eq value +null+)) (not (eq constant +null+))
                            (funcall integer-operator value constant)))
                     (lambda (value)
                       (and (not (eq value +null+)) (not (eq constant +null+))
                            (funcall accept (funcall compare value constant)))))))
          (values (lambda (row) (funcall accept-value (aref row slot)))
                  slot accept-value
                  (when heap-filter-p
                    (case (expr-type column)
                      ((:text :varchar) (make-ascii-span-comparison constant accept))
                      (:uuid (when (sql-uuid-p constant)
                               (make-uuid-span-comparison
                                (sql-uuid-bytes constant) accept)))))))))))

(defun make-physical-filter-iterator (node relations &optional count-only-p key-slot)
  (let* ((qual (second node))
         (child (third node))
         (metadata (and *query-plan-instrumentation-p* *cte-active-plan*
                        (gethash node (query-plan-metadata *cte-active-plan*)))))
    (multiple-value-bind (fast-filter slot accept-value span-compare)
        (make-scalar-column-comparison-filter
         qual (and (not *query-plan-instrumentation-p*) (eq (first child) :seq-scan)))
      (or (and accept-value (not *query-plan-instrumentation-p*)
               (eq (first child) :seq-scan)
               (make-select-scan-iterator child relations slot accept-value count-only-p span-compare key-slot))
          (make-mapping-sql-iterator
           (make-physical-plan-iterator child relations)
           (lambda (row)
             (let ((accepted-p (if fast-filter (funcall fast-filter row)
                                   (true-p (eval-expr qual row)))))
               (when (and metadata (not accepted-p))
                 (incf (plan-node-metadata-filtered-row-count metadata)))
               (values row accepted-p *query-row-origins*)))
           :preserve-toast-context t)))))

(defun make-count-input-iterator (node relations)
  "Request match markers from eligible filters, identity views and joins."
  (if *query-plan-instrumentation-p*
      (make-physical-plan-iterator node relations)
      (case (first node)
        (:filter (make-physical-filter-iterator node relations t))
        (:subquery-scan (make-subquery-scan-iterator node relations t))
        (:nested-loop-join (make-physical-join-iterator node relations t))
        (otherwise (make-physical-plan-iterator node relations)))))

(defun make-top-n-input-iterator (project relations order)
  "Return the input iterator and, for identity scans, its mutable scan state.
The state's CAR holds a reusable row and its CDR holds an optional key cutoff.
The sorter takes ownership of retained vectors before the scanner can overwrite them."
  (let ((input (and (not *datum-origin-capture*) (not *query-plan-instrumentation-p*)
                    (not *query-sort-project*)
                    (identity-projection-input project relations))))
    (if input
        (let* ((key (and (null (cdr order)) (caar order)))
               (cutoff (when (and (member (first key) '(:col :sort-output))
                                  (member (expr-type key) '(:text :varchar :uuid :int2 :int4 :int8)))
                         (make-top-n-key-cutoff :slot (1- (second key))
                                                :descending-p (order-descending-p (cdar order)))))
               (scratch (cons (make-array (length (second project))) cutoff)))
          (values (make-top-n-identity-scan-iterator input relations scratch) scratch))
        (make-physical-plan-iterator project relations))))

(defun %make-physical-plan-iterator (node relations)
  (case (first node)
    (:result
     (let ((done nil))
       (make-sql-iterator (lambda () (unless done (setf done t) (values #() t nil nil #()))))))
    ((:seq-scan :index-scan) (make-select-scan-iterator node relations))
    (:cte-scan (make-cte-scan-iterator (physical-scan-relation node relations)))
    (:worktable-scan (make-cte-worktable-iterator (physical-scan-relation node relations)))
    (:recursive-union (make-recursive-union-iterator node relations))
    (:with-ctes (make-with-ctes-iterator (second node) (third node) relations))
    (:append (make-append-sql-iterator (second node) relations))
    (:subquery-scan (make-subquery-scan-iterator node relations))
    (:filter (make-physical-filter-iterator node relations))
    (:project
     (let* ((targets (second node))
            (deferred (and *query-sort-project*
                           (eq node (car *query-sort-project*))
                           (cdr *query-sort-project*)))
            (set-node (and (null deferred) (project-set-projection-node node)))
            (*query-project-set* (and set-node (cons set-node targets))))
       (make-mapping-sql-iterator
        (make-physical-plan-iterator (third node) relations)
        (lambda (row)
          (multiple-value-bind (projected origins)
              (project-physical-row row targets deferred set-node)
            (values projected t origins))))))
    (:window-input
     (make-mapping-sql-iterator
      (make-physical-plan-iterator (third node) relations)
      (lambda (row)
        (multiple-value-bind (values origins)
            (window-input-with-origins row (second node) *query-row-origins*)
          (values values t origins)))))
    (:window
     (make-window-plan-iterator (make-physical-plan-iterator (third node) relations) (second node)))
    (:project-set
     (let ((input (make-physical-plan-iterator (third node) relations)))
       (if (and *query-project-set* (eq node (car *query-project-set*)))
           (make-project-set-projection-iterator
            input (second node) (cdr *query-project-set*) (fourth node))
           (make-project-set-iterator input (second node)))))
    (:limit
     (make-limit-sql-iterator (second node)
                              (make-physical-plan-iterator (third node) relations) :lazy t))
    (:nested-loop-join (make-physical-join-iterator node relations))
    (:hash-join (make-physical-hash-join-iterator node relations))
    (:sorted-distinct
     (let* ((keys (second node))
            (input (make-physical-plan-iterator (third node) relations))
            (order (mapcar (lambda (key) (list (list :sort-output (second key)))) keys))
            (sorted (make-select-sort-iterator input order)))
       (make-sorted-distinct-iterator sorted keys)))
    (:sorted-aggregate
     (make-sorted-aggregate-iterator (make-physical-plan-iterator (fourth node) relations)
                                     (second node) (third node)))
    (:hash-distinct
     (make-hash-distinct-iterator (make-physical-plan-iterator (third node) relations) (second node)))
    (:hash-aggregate
     (or (make-fixed-hash-count-iterator node relations)
         (let* ((child (fourth node))
                (groups (second node))
                (input (if groups
                           (make-physical-plan-iterator child relations)
                           (make-count-input-iterator child relations))))
           (if groups
               (make-hash-aggregate-iterator input groups (third node))
               (make-ungrouped-count-iterator input (third node))))))
    ((:sort :top-n)
     (let ((child (physical-buffered-node-child node))
           (metadata (when (and *query-plan-instrumentation-p* *cte-active-plan*)
                       (gethash node (query-plan-metadata *cte-active-plan*)))))
       ;; If a key needs an SRF value, keep expansion below sort instead of
       ;; moving it above the sort and changing the key's evaluation point.
       (multiple-value-bind (set-node width calls) (postponable-sort-project-set node child)
         (when set-node
           (return-from %make-physical-plan-iterator
             (make-sorted-project-set-iterator node child set-node width calls relations metadata))))
       (let* ((deferred (sort-projection-deferred-slots node child))
              (*query-sort-project* (and deferred (cons child deferred)))
              (bounded-p (eq (first node) :top-n)))
         (multiple-value-bind (input scratch)
             (if bounded-p
                 (make-top-n-input-iterator child relations (second node))
                 (make-physical-plan-iterator child relations))
           (let ((buffer (make-select-sort-iterator
                          input (second node) :scratch scratch
                          :limit (and bounded-p (third node))
                          :metadata metadata
                          :projected-only-p (and (eq node *select-terminal-sort-node*)
                                                 (eq (first node) :sort)
                                                 (null deferred)
                                                 (not *datum-origin-capture*)))))
             (if deferred
                 (make-mapping-sql-iterator
                  buffer
                  (lambda (row)
                    (multiple-value-bind (projected origins)
                        (finish-sort-project-row row (second child) deferred)
                      (values projected t origins))))
                 buffer))))))
    (otherwise (ereport "0A000" "unsupported resumable producer node ~a" (first node)))))

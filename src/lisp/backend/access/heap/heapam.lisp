;;;; heapam.lisp -- heap tuples on pages: form, deform, insert, delete-mark,
;;;; scan, lazy pruning.

(in-package #:pg)

;; pg-column is defined by the later catalog component in pg.asd.
(declaim (notinline pg-column-type))

(defstruct htup
  block
  offnum
  ctid-block
  ctid-offnum
  xmin
  xmax
  cid
  infomask
  values)

;;; The PostgreSQL FSM is persisted in a separate relation fork. Keep the MVP's
;;; page-space candidates in memory and rebuild them once per relation after a
;;; restart. Buckets are lower bounds, so a candidate is never reported as
;;; having more space than it actually does.
(defconstant +heap-fsm-category-count+ 256)
(defconstant +heap-fsm-category-size+ (/ +blcksz+ +heap-fsm-category-count+))

(defstruct heap-fsm
  (buckets (make-hash-table :test #'eql))
  (page-space (make-hash-table :test #'eql))
  (nonempty-categories (make-array +heap-fsm-category-count+
                                   :initial-element 0)))

(defvar *heap-fsms* (make-hash-table :test #'eql))

(defun heap-fsm-category (space)
  (min (1- +heap-fsm-category-count+)
       (floor (max 0 space) +heap-fsm-category-size+)))

(defun heap-fsm-forget (relfilenode)
  (remhash relfilenode *heap-fsms*)
  (values))

(defun heap-fsm-record-page (fsm block space)
  "Record BLOCK's current free space, removing its previous category first."
  (multiple-value-bind (old-space present-p)
      (gethash block (heap-fsm-page-space fsm))
    (when present-p
      (let* ((pages (gethash old-space (heap-fsm-buckets fsm)))
             (remaining (delete block pages :test #'eql)))
        (if remaining
            (setf (gethash old-space (heap-fsm-buckets fsm)) remaining)
            (progn
              (remhash old-space (heap-fsm-buckets fsm))
              (decf (aref (heap-fsm-nonempty-categories fsm)
                          (heap-fsm-category old-space))))))
      (remhash block (heap-fsm-page-space fsm))))
  (when (plusp space)
    (multiple-value-bind (pages present-p)
        (gethash space (heap-fsm-buckets fsm))
      (unless present-p
        (incf (aref (heap-fsm-nonempty-categories fsm)
                    (heap-fsm-category space))))
      (push block pages)
      (setf (gethash space (heap-fsm-buckets fsm)) pages
            (gethash block (heap-fsm-page-space fsm)) space)))
  (values))

(defun heap-fsm-take-page (fsm space-needed)
  "Return and remove a candidate page with at least SPACE-NEEDED bytes."
  (let* ((minimum-space (max 0 space-needed))
         (minimum-category (floor minimum-space +heap-fsm-category-size+)))
    (loop for category from minimum-category below +heap-fsm-category-count+
          when (plusp (aref (heap-fsm-nonempty-categories fsm) category))
            do (loop for space from (if (= category minimum-category)
                                        minimum-space
                                        (* category +heap-fsm-category-size+))
                       to (min +blcksz+
                               (1- (* (1+ category) +heap-fsm-category-size+)))
                     for candidates = (gethash space (heap-fsm-buckets fsm))
                     when candidates
                       do (let ((block (pop candidates)))
                            (if candidates
                                (setf (gethash space (heap-fsm-buckets fsm))
                                      candidates)
                                (progn
                                  (remhash space (heap-fsm-buckets fsm))
                                  (decf (aref (heap-fsm-nonempty-categories fsm)
                                              category))))
                            (remhash block (heap-fsm-page-space fsm))
                            (return-from heap-fsm-take-page block))))))

(defun heap-fsm-for-relation (relfilenode)
  (or (gethash relfilenode *heap-fsms*)
      (let ((fsm (make-heap-fsm)))
        ;; This one-time rebuild replaces heapam-insert's repeated full scan.
        (dotimes (block (buf-nblocks relfilenode))
          (let ((page (buf-read relfilenode block)))
            (when page
              (heap-fsm-record-page fsm block (page-free-space page)))))
        (setf (gethash relfilenode *heap-fsms*) fsm))))

(defun column-align (col)
  (case (pg-column-type col)
    (:int8 8)
    (:int4 4)
    (t 1)))

(defun column-fixed-size (col)
  (case (pg-column-type col)
    (:int4 4)
    (:int8 8)
    (t nil)))

(defun varchar-byte-length (string)
  (length (string->bytes string)))

(defun form-heap-tuple (values columns xmin xmax cid block off)
  "Build a HeapTupleHeaderData + null bitmap + attribute data byte vector."
  (let* ((natts (length columns))
         (bitmap-len (ceiling natts 8))
         (hoff (maxalign (+ +size-of-heap-tuple-header+ bitmap-len)))
         (offsets (make-array natts :initial-element nil))
         (cursor hoff)
         (infomask 0)
         (has-null nil))
    (loop for i from 0 below natts
          for v = (aref values i)
          for col = (aref columns i)
          do (cond
               ((eq v +null+)
                (setf has-null t))
               (t
                (let ((al (column-align col)))
                  (setf cursor (align-up cursor al))
                  (setf (aref offsets i) cursor)
                  (incf cursor
                        (if (eq (pg-column-type col) :varchar)
                            (1+ (varchar-byte-length v))
                            (column-fixed-size col)))))))
    (when has-null (setf infomask (logior infomask +heap-hasnull+)))
    (when (some (lambda (c) (eq (pg-column-type c) :varchar)) columns)
      (setf infomask (logior infomask +heap-hasvarwidth+)))
    (let ((bytes (make-array cursor :element-type '(unsigned-byte 8)
                                    :initial-element 0)))
      ;; HeapTupleHeaderData stores 32-bit TransactionIds. The allocator keeps
      ;; full XIDs and advances the epoch when these low 32 bits wrap.
      (page-set-u32 bytes +heap-tuple-xmin-off+ (xid32-of xmin))
      (page-set-u32 bytes +heap-tuple-xmax-off+ (xid32-of xmax))
      (page-set-u32 bytes +heap-tuple-cid-off+ cid)
      (page-set-u32 bytes +heap-tuple-ctid-block-off+ block)
      (page-set-u16 bytes +heap-tuple-ctid-offnum-off+ off)
      (page-set-u16 bytes +heap-tuple-natts-off+ natts)
      (page-set-u16 bytes +heap-tuple-infomask-off+ infomask)
      (setf (aref bytes +heap-tuple-hoff-off+) hoff)
      ;; Null bitmap.
      (loop for i from 0 below natts
            when (eq (aref values i) +null+)
              do (let ((byte-index (+ +size-of-heap-tuple-header+
                                      (floor i 8)))
                       (bit (mod i 8)))
                   (setf (aref bytes byte-index)
                         (logior (aref bytes byte-index) (ash 1 bit)))))
      ;; Attribute data.
      (loop for i from 0 below natts
            for v = (aref values i)
            for off = (aref offsets i)
            for col = (aref columns i)
            do (unless (eq v +null+)
                 (case (pg-column-type col)
                   (:int4
                    (page-set-u32 bytes off (logand v #xFFFFFFFF)))
                   (:int8
                    (page-set-u64 bytes off (logand v #xFFFFFFFFFFFFFFFF)))
                   (:varchar
                    (let ((data (string->bytes v)))
                      (when (> (length data) 126)
                        (ereport "22001"
                                 "value too long for type character varying(126)"))
                      (setf (aref bytes off)
                            (logior (ash (1+ (length data)) 1) 1))
                      (replace bytes data :start1 (1+ off)))))))
      bytes)))

(defun deform-heap-tuple (bytes columns)
  "Return values, XIDs, command ID, infomask, and successor TID."
  (let* ((natts (length columns))
         (xmin (page-u32 bytes +heap-tuple-xmin-off+))
         (xmax (page-u32 bytes +heap-tuple-xmax-off+))
         (cid (page-u32 bytes +heap-tuple-cid-off+))
         (ctid-block (page-u32 bytes +heap-tuple-ctid-block-off+))
         (ctid-offnum (page-u16 bytes +heap-tuple-ctid-offnum-off+))
         (infomask (page-u16 bytes +heap-tuple-infomask-off+))
         (hoff (aref bytes +heap-tuple-hoff-off+))
         (values (make-array natts :initial-element +null+))
         (cursor hoff))
    (loop for i from 0 below natts
          for col = (aref columns i)
          do (let ((is-null
                     (and (logtest infomask +heap-hasnull+)
                          (logbitp (mod i 8)
                                   (aref bytes (+ +size-of-heap-tuple-header+
                                                  (floor i 8)))))))
               (unless is-null
                 (let ((al (column-align col)))
                   (setf cursor (align-up cursor al))
                   (case (pg-column-type col)
                     (:int4
                      (let ((v (page-u32 bytes cursor)))
                        (when (>= v #x80000000)
                          (decf v #x100000000))
                        (setf (aref values i) v))
                      (incf cursor 4))
                     (:int8
                      (let ((v (page-u64 bytes cursor)))
                        (when (>= v #x8000000000000000)
                          (decf v #x10000000000000000))
                        (setf (aref values i) v))
                      (incf cursor 8))
                     (:varchar
                      (let* ((header (aref bytes cursor))
                             (shortp (oddp header))
                             (total (if shortp
                                        (ash header -1)
                                        (ash (page-u32 bytes cursor) -2)))
                             (data-start (if shortp (1+ cursor) (+ cursor 4)))
                             (data-len (- total (if shortp 1 4))))
                        (setf (aref values i)
                              (bytes->string bytes
                                             :start data-start
                                             :end (+ data-start data-len)))
                        (incf cursor total))))))))
    (values values xmin xmax cid infomask ctid-block ctid-offnum)))

(defun heapam-get-tuple (relfilenode block offnum columns)
  (let ((page (buf-read relfilenode block)))
    (when page
      (with-live-page-item (page offnum off len)
        (let ((bytes (subseq page off (+ off len))))
          (multiple-value-bind (values xmin xmax cid infomask
                                ctid-block ctid-offnum)
              (deform-heap-tuple bytes columns)
            (make-htup :block block :offnum offnum
                       :ctid-block ctid-block :ctid-offnum ctid-offnum
                       :xmin (full-xid-from-32 xmin)
                       :xmax (full-xid-from-32 xmax) :cid cid
                       :infomask infomask :values values)))))))

(defun heapam-insert (relfilenode columns values xmin cid)
  "Insert a tuple; return (values block offnum)."
  (let* ((tuple (form-heap-tuple values columns xmin 0 cid 0 0))
         (fsm (heap-fsm-for-relation relfilenode))
         (required (+ +size-of-item-id+ (maxalign (length tuple)))))
    (labels ((add-to-page (page block)
               (let ((offnum (page-add-item page tuple :allow-reuse nil)))
                 (when offnum
                   (multiple-value-bind (toff tflags tlen)
                       (page-get-lp page offnum)
                     (declare (ignore tflags tlen))
                     (page-set-u32 page (+ toff +heap-tuple-ctid-block-off+)
                                   block)
                     (page-set-u16 page (+ toff +heap-tuple-ctid-offnum-off+)
                                   offnum))
                   (buf-write relfilenode block page)
                   (mark-dirty relfilenode)
                   (heap-fsm-record-page fsm block (page-free-space page)))
                 offnum)))
      (loop for block = (heap-fsm-take-page fsm required)
            while block
            for page = (buf-read relfilenode block)
            when page
              do (let ((offnum (add-to-page page block)))
                   (if offnum
                       (return-from heapam-insert (values block offnum))
                       ;; FSM entries are advisory, like PostgreSQL's. Refresh
                       ;; one if an intervening page change made it stale.
                       (heap-fsm-record-page fsm block (page-free-space page)))))
      ;; Tuple TIDs remain stable for concurrent update-chain rechecks. Without
      ;; VACUUM to establish a safe removal horizon, extend rather than prune.
      (let* ((block (buf-nblocks relfilenode))
             (page (page-init (make-page)))
             (offnum (page-add-item page tuple :allow-reuse nil)))
        (unless offnum
          (ereport "54000" "row is too large for a heap page"))
        (multiple-value-bind (toff tflags tlen) (page-get-lp page offnum)
          (declare (ignore tflags tlen))
          (page-set-u32 page (+ toff +heap-tuple-ctid-block-off+) block)
          (page-set-u16 page (+ toff +heap-tuple-ctid-offnum-off+) offnum))
        (buf-write relfilenode block page)
        (mark-dirty relfilenode)
        (heap-fsm-record-page fsm block (page-free-space page))
        (values block offnum)))))

(defun heapam-set-xmax (relfilenode block offnum xmax cid)
  (let ((page (buf-read relfilenode block)))
    (when page
      (with-live-page-item (page offnum off len)
        (page-set-u32 page (+ off +heap-tuple-xmax-off+)
                      (xid32-of xmax))
        (page-set-u32 page (+ off +heap-tuple-cid-off+) cid)
        (buf-write relfilenode block page)
        (mark-dirty relfilenode)
        t))))

(defun heapam-set-ctid (relfilenode block offnum ctid-block ctid-offnum)
  (with-write-lock
    (let ((page (buf-read relfilenode block)))
      (when page
        (with-live-page-item (page offnum off len)
          (page-set-u32 page (+ off +heap-tuple-ctid-block-off+) ctid-block)
          (page-set-u16 page (+ off +heap-tuple-ctid-offnum-off+) ctid-offnum)
          (buf-write relfilenode block page)
          (mark-dirty relfilenode)
          t)))))

(defun fresh-delete-snapshot (fallback)
  (if *backend*
      (make-snapshot (backend-command-id *backend*))
      fallback))

(defun heapam-delete-tid (relfilenode columns block offnum xid cid
                          &optional (matches-p (constantly t)))
  "Delete the latest qualifying version of TID, waiting for a writer.

After a conflicting update commits, follow its ctid chain and evaluate
MATCHES-P against the refreshed row before claiming it. Return T, the tuple
whose xmax was set, and that tuple's physical block and offset."
  (register-relation-writer relfilenode xid)
  (let ((target-block block)
        (target-offnum offnum)
        (snapshot *current-snapshot*)
        (expected-xmin nil)
        (visited (make-hash-table :test #'equal)))
    (loop
      (let ((key (cons target-block target-offnum)))
        (when (gethash key visited)
          (ereport "XX001" "heap tuple update chain contains a cycle"))
        (setf (gethash key visited) t))
      (let ((action
              (with-write-lock
                (let ((tuple (heapam-get-tuple relfilenode target-block
                                               target-offnum columns)))
                  (cond
                    ((null tuple) :skip)
                    ((and expected-xmin
                          (/= (htup-xmin tuple) expected-xmin))
                     :skip)
                    ((= (htup-xmax tuple) xid) (list :done tuple))
                    ((and (not (zerop (htup-xmax tuple)))
                          (xid-active-p (htup-xmax tuple))
                          (/= (htup-xmax tuple) xid))
                     (list :wait (htup-xmax tuple)))
                    ((and (not (zerop (htup-xmax tuple)))
                          (xid-committed-p (htup-xmax tuple)))
                     (if (or (/= (htup-ctid-block tuple) target-block)
                             (/= (htup-ctid-offnum tuple) target-offnum))
                         (list :follow (htup-ctid-block tuple)
                               (htup-ctid-offnum tuple)
                               (htup-xmax tuple))
                         :skip))
                    ((not (heap-tuple-visible-p
                           (htup-xmin tuple) (htup-xmax tuple) (htup-cid tuple)
                           snapshot
                           (and *backend* (backend-xid *backend*))))
                     :skip)
                    ((or (zerop (htup-xmax tuple))
                         (xid-aborted-p (htup-xmax tuple)))
                     (if (funcall matches-p tuple)
                         (if (heapam-set-xmax relfilenode target-block
                                              target-offnum xid cid)
                             (list :done tuple)
                             :skip)
                         :skip))
                    (t :skip))))))
        (case (if (consp action) (car action) action)
          (:done (return (values t (second action)
                                 target-block target-offnum)))
          (:skip (return (values nil nil nil nil)))
          (:wait
           (wait-for-xid (second action))
           ;; Revisit this same TID after the writer ends; it is not a chain loop.
           (remhash (cons target-block target-offnum) visited)
           ;; READ COMMITTED rechecks a row after its conflicting writer ends.
           ;; Keep the original statement snapshot for every other scanned TID.
           (setf snapshot (fresh-delete-snapshot snapshot))
           (when *backend*
             (setf (backend-snapshot *backend*) snapshot)))
          (:follow
           (setf target-block (second action)
                 target-offnum (third action)
                 expected-xmin (fourth action)
                 snapshot (fresh-delete-snapshot snapshot))
           (when *backend*
             (setf (backend-snapshot *backend*) snapshot))))))))

(defun heapam-prune-page (relfilenode block)
  "Keep heap TIDs stable until VACUUM can establish a removal horizon."
  (declare (ignore relfilenode block))
  0)

(defun heapam-freeze-relation-xids
    (relfilenode columns old-epoch
     &key (fsync-function #'smgr-fsync) after-page-function)
  "Freeze old-cycle tuple XIDs after all transactions and snapshots drain."
  (declare (ignore columns))
  (let ((changed nil)
        (epoch-base (* old-epoch +xid-cycle+)))
    (dotimes (block (buf-nblocks relfilenode))
      (check-for-interrupts)
      (let ((page (buf-read relfilenode block))
            (page-changed nil))
        (when page
          (do-live-page-items (page offnum off len)
            (let* ((xmin (page-u32 page (+ off +heap-tuple-xmin-off+)))
                   (xmax (page-u32 page (+ off +heap-tuple-xmax-off+)))
                   (full-xmin (and (>= xmin +first-normal-xid+)
                                   (+ epoch-base xmin)))
                   (full-xmax (and (>= xmax +first-normal-xid+)
                                   (+ epoch-base xmax))))
              (when full-xmin
                (page-set-u32
                 page (+ off +heap-tuple-xmin-off+)
                 (if (= (clog-get full-xmin) +clog-committed+)
                     +frozen-xid+
                     +invalid-xid+))
                (setf page-changed t))
              (when full-xmax
                (page-set-u32
                 page (+ off +heap-tuple-xmax-off+)
                 (if (= (clog-get full-xmax) +clog-committed+)
                     +frozen-xid+
                     +invalid-xid+))
                (setf page-changed t))))
          (when page-changed
            (buf-write relfilenode block page)
            (setf changed t)
            (when after-page-function
              (funcall after-page-function relfilenode block))))))
    (when changed
      (funcall fsync-function relfilenode)))
  (values))

(defun htup-visible-p (tuple snapshot)
  (heap-tuple-visible-p (htup-xmin tuple)
                        (htup-xmax tuple)
                        (htup-cid tuple)
                        snapshot
                        (and *backend* (backend-xid *backend*))))

(defun heapam-scan-page (page block columns snapshot callback)
  (do-live-page-items (page offnum off len)
    (let ((bytes (subseq page off (+ off len))))
      (multiple-value-bind (values raw-xmin raw-xmax cid infomask
                            ctid-block ctid-offnum)
          (deform-heap-tuple bytes columns)
        (let ((xmin (full-xid-from-32 raw-xmin))
              (xmax (full-xid-from-32 raw-xmax)))
          (when (heap-tuple-visible-p
                 xmin xmax cid snapshot
                 (and *backend* (backend-xid *backend*)))
            (let ((tuple (make-htup :block block :offnum offnum
                                    :ctid-block ctid-block
                                    :ctid-offnum ctid-offnum
                                    :xmin xmin :xmax xmax
                                    :cid cid :infomask infomask
                                    :values values)))
              (when (eq (funcall callback tuple) :stop)
                (return-from heapam-scan-page :stop)))))))))

(defun heapam-scan (relfilenode columns snapshot callback)
  "Call CALLBACK with each visible HTUP. CALLBACK may return :stop."
  (let ((nblocks (buf-nblocks relfilenode)))
    (dotimes (block nblocks)
      (let ((page (buf-read relfilenode block)))
        (when page
          (when (eq (heapam-scan-page page block columns snapshot callback)
                    :stop)
            (return))))))
  (values))

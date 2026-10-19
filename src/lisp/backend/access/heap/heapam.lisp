;;;; heapam.lisp -- heap tuples on pages: form, deform, insert, delete-mark,
;;;; scan, lazy pruning.

(in-package #:pg)

;; pg-column is defined by the later catalog component in pg.asd.
(declaim (notinline pg-column-type))

(defstruct htup
  block
  offnum
  xmin
  xmax
  cid
  infomask
  values)

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
      ;; HeapTupleHeaderData
      (page-set-u32 bytes 0 xmin)
      (page-set-u32 bytes 4 xmax)
      (page-set-u32 bytes 8 cid)
      (page-set-u32 bytes 12 block)
      (page-set-u16 bytes 16 off)
      (page-set-u16 bytes 18 natts)
      (page-set-u16 bytes 20 infomask)
      (setf (aref bytes 22) hoff)
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
  "Return (values values xmin xmax cid infomask)."
  (let* ((natts (length columns))
         (xmin (page-u32 bytes 0))
         (xmax (page-u32 bytes 4))
         (cid (page-u32 bytes 8))
         (infomask (page-u16 bytes 20))
         (hoff (aref bytes 22))
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
    (values values xmin xmax cid infomask)))

(defun heapam-get-tuple (relfilenode block offnum columns)
  (let ((page (buf-read relfilenode block)))
    (when page
      (multiple-value-bind (off flags len) (page-get-lp page offnum)
        (when (= flags +lp-normal+)
          (let ((bytes (subseq page off (+ off len))))
            (multiple-value-bind (values xmin xmax cid infomask)
                (deform-heap-tuple bytes columns)
              (make-htup :block block :offnum offnum
                         :xmin xmin :xmax xmax :cid cid
                         :infomask infomask :values values))))))))

(defun heapam-insert (relfilenode columns values xmin cid &key allow-reuse)
  "Insert a tuple; return (values block offnum)."
  (let ((tuple (form-heap-tuple values columns xmin 0 cid 0 0))
        (nblocks (buf-nblocks relfilenode)))
    (flet ((try-page (block)
             (let ((page (buf-read relfilenode block)))
               (when page
                 (let ((offnum (page-add-item page tuple :allow-reuse allow-reuse)))
                   (when offnum
                     (multiple-value-bind (toff tflags tlen)
                         (page-get-lp page offnum)
                       (declare (ignore tflags tlen))
                       (page-set-u32 page (+ toff 12) block)
                       (page-set-u16 page (+ toff 16) offnum))
                     (buf-write relfilenode block page)
                     (mark-dirty relfilenode)
                     (return-from try-page (values block offnum))))))))
      (loop for block from 0 below nblocks
            do (multiple-value-bind (b o) (try-page block)
                 (when b (return-from heapam-insert (values b o)))))
      ;; No page had space: prune and retry once.
      (loop for block from 0 below nblocks
            do (when (plusp (heapam-prune-page relfilenode block))
                 (multiple-value-bind (b o) (try-page block)
                   (when b (return-from heapam-insert (values b o))))))
      ;; Extend.
      (let ((page (page-init (make-page))))
        (let ((offnum (page-add-item page tuple :allow-reuse nil)))
          (unless offnum
            (ereport "54000" "row is too large for a heap page"))
          (multiple-value-bind (toff tflags tlen) (page-get-lp page offnum)
            (declare (ignore tflags tlen))
            (page-set-u32 page (+ toff 12) nblocks)
            (page-set-u16 page (+ toff 16) offnum))
          (buf-write relfilenode nblocks page)
          (mark-dirty relfilenode)
          (values nblocks offnum))))))

(defun heapam-set-xmax (relfilenode block offnum xmax cid)
  (let ((page (buf-read relfilenode block)))
    (when page
      (multiple-value-bind (off flags len) (page-get-lp page offnum)
        (declare (ignore len))
        (when (= flags +lp-normal+)
          (page-set-u32 page (+ off 4) xmax)
          (page-set-u32 page (+ off 8) cid)
          (buf-write relfilenode block page)
          (mark-dirty relfilenode)
          t)))))

(defun heapam-delete-tid (relfilenode columns block offnum xid cid)
  "Mark TID deleted by XID, waiting for a concurrent deleter if needed.
Return T if this transaction actually deleted it."
  (register-relation-writer relfilenode xid)
  (loop
    (let ((action
            (with-write-lock
              (let ((tuple (heapam-get-tuple relfilenode block offnum columns)))
                (cond
                  ((null tuple) :skip)
                  ((= (htup-xmax tuple) xid) :done)
                  ((and (not (zerop (htup-xmax tuple)))
                        (xid-in-progress-p (htup-xmax tuple))
                        (/= (htup-xmax tuple) xid))
                   (cons :wait (htup-xmax tuple)))
                  ((not (heap-tuple-visible-p
                         (htup-xmin tuple) (htup-xmax tuple) (htup-cid tuple)
                         *current-snapshot*
                         (and *backend* (backend-xid *backend*))))
                   :skip)
                  ((zerop (htup-xmax tuple))
                   (heapam-set-xmax relfilenode block offnum xid cid)
                   :done)
                  ((xid-aborted-p (htup-xmax tuple))
                   (heapam-set-xmax relfilenode block offnum xid cid)
                   :done)
                  (t :skip))))))
      (cond
        ((eq action :done) (return t))
        ((eq action :skip) (return nil))
        (t (wait-for-xid (cdr action)))))))

(defun heapam-prune-page (relfilenode block)
  "Mark aborted / committed-dead tuples LP_DEAD and compact the page.
Return the number of tuples pruned."
  (let ((page (buf-read relfilenode block)))
    (unless page
      (return-from heapam-prune-page 0))
    (let ((pruned 0))
      (do-live-page-items (page i off len)
        (let* ((xmin (page-u32 page off))
               (xmax (page-u32 page (+ off 4))))
          (when (tuple-prunable-p xmin xmax)
            (page-mark-dead page i)
            (incf pruned))))
      (when (plusp pruned)
        (page-compact page)
        (buf-write relfilenode block page)
        (mark-dirty relfilenode))
      pruned)))

(defun htup-visible-p (tuple snapshot)
  (heap-tuple-visible-p (htup-xmin tuple)
                        (htup-xmax tuple)
                        (htup-cid tuple)
                        snapshot
                        (and *backend* (backend-xid *backend*))))

(defun heapam-scan-page (page block columns snapshot callback)
  (do-live-page-items (page offnum off len)
    (let ((bytes (subseq page off (+ off len))))
      (multiple-value-bind (values xmin xmax cid infomask)
          (deform-heap-tuple bytes columns)
        (when (heap-tuple-visible-p
               xmin xmax cid snapshot
               (and *backend* (backend-xid *backend*)))
          (let ((tuple (make-htup :block block :offnum offnum
                                  :xmin xmin :xmax xmax
                                  :cid cid :infomask infomask
                                  :values values)))
            (when (eq (funcall callback tuple) :stop)
              (return-from heapam-scan-page :stop))))))))

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

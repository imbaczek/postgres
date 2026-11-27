;;;; bufpage.lisp -- PostgreSQL page layout: PageHeaderData + ItemIdData.

(in-package #:pg)

;;; PageHeaderData field offsets.
(defconstant +pd-lsn-off+ 0)
(defconstant +pd-checksum-off+ 8)
(defconstant +pd-flags-off+ 10)
(defconstant +pd-lower-off+ 12)
(defconstant +pd-upper-off+ 14)
(defconstant +pd-special-off+ 16)
(defconstant +pd-pagesize-version-off+ 18)
(defconstant +pd-prune-xid-off+ 20)

;;; On-disk integers are native-endian in PostgreSQL; this box is x86_64
;;; (little-endian), so page accessors use little-endian.

(defun page-u16 (page off)
  (flet ((load-word (page off)
           (logior (aref page off)
                   (ash (aref page (1+ off)) 8))))
    (declare (inline load-word))
    (if (and (typep page '(simple-array (unsigned-byte 8) (*))) (typep off 'fixnum))
        (locally (declare (optimize (speed 3))) (load-word page off))
        (load-word page off))))

(defun page-u32 (page off)
  (flet ((load-word (page off)
           (logior (aref page off)
                   (ash (aref page (+ off 1)) 8)
                   (ash (aref page (+ off 2)) 16)
                   (ash (aref page (+ off 3)) 24))))
    (declare (inline load-word))
    (if (and (typep page '(simple-array (unsigned-byte 8) (*))) (typep off 'fixnum))
        (locally (declare (optimize (speed 3))) (load-word page off))
        (load-word page off))))

(defun page-u64 (page off)
  (logior (page-u32 page off)
          (ash (page-u32 page (+ off 4)) 32)))

#+(or x86-64 arm64)
(defun equal-octet-word-prefix (left right right-start limit)
  "Skip equal words in bounded octet spans, including unaligned page entries.
The caller bounds both spans. Word equality is independent of byte order."
  (declare (type (simple-array (unsigned-byte 8) (*)) left right)
           (type fixnum right-start limit)
           (optimize (speed 3) (safety 1)))
  (sb-sys:with-pinned-objects (left right)
    (let ((left-sap (sb-sys:vector-sap left))
          (right-sap (sb-sys:vector-sap right)))
      #+arm64
      (unless (zerop (logand 7 (logior (sb-sys:sap-int left-sap)
                                       (+ (sb-sys:sap-int right-sap) right-start))))
        (return-from equal-octet-word-prefix 0))
      (loop for index fixnum from 0 below (- limit 7) by 8
            while (= (sb-sys:sap-ref-64 left-sap index)
                     (sb-sys:sap-ref-64 right-sap (the fixnum (+ right-start index))))
            finally (return index)))))
(defun compare-octet-vector-to-span (left right right-start right-length)
  "Compare a complete octet vector with a slice of another in unsigned byte order."
  (flet ((compare-slice (left right right-start right-length start)
           (let ((left-length (length left)))
             (loop for i from start below (min left-length right-length)
                   for j from (+ right-start start)
                   for a = (aref left i)
                   for b = (aref right j)
                   do (cond ((< a b) (return -1))
                            ((> a b) (return 1)))
                   finally (return (signum (- left-length right-length)))))))
    (declare (inline compare-slice))
    (if (and (typep left '(simple-array (unsigned-byte 8) (*)))
             (typep right '(simple-array (unsigned-byte 8) (*)))
             (typep right-start 'fixnum) (typep right-length 'fixnum))
        (locally (declare (optimize (speed 3)))
          (let ((start
                  #+(or x86-64 arm64)
                  (let ((common (min (length left) right-length)))
                    (if (and (>= common 32) (>= right-start 0)
                             (<= common (- (length right) right-start)))
                        (equal-octet-word-prefix left right right-start common)
                        0))
                  #-(or x86-64 arm64) 0))
            (compare-slice left right right-start right-length start)))
        (compare-slice left right right-start right-length 0))))

(defun page-set-u16 (page off v)
  (setf (aref page off) (logand v #xFF)
        (aref page (1+ off)) (logand (ash v -8) #xFF))
  v)

(defun page-set-u32 (page off v)
  (setf (aref page off) (logand v #xFF)
        (aref page (+ off 1)) (logand (ash v -8) #xFF)
        (aref page (+ off 2)) (logand (ash v -16) #xFF)
        (aref page (+ off 3)) (logand (ash v -24) #xFF))
  v)

(defun page-set-u64 (page off v)
  (page-set-u32 page off (logand v #xFFFFFFFF))
  (page-set-u32 page (+ off 4) (logand (ash v -32) #xFFFFFFFF))
  v)

(defun make-page ()
  (make-array +blcksz+ :element-type '(unsigned-byte 8) :initial-element 0))

(defun page-init (page)
  (fill page 0)
  (page-set-u16 page +pd-lower-off+ +size-of-page-header+)
  (page-set-u16 page +pd-upper-off+ +blcksz+)
  (page-set-u16 page +pd-special-off+ +blcksz+)
  (page-set-u16 page +pd-pagesize-version-off+
                (logior +blcksz+ +pg-page-layout-version+))
  (page-set-u32 page +pd-prune-xid-off+ 0)
  page)

(defun page-lower (page) (page-u16 page +pd-lower-off+))
(defun page-upper (page) (page-u16 page +pd-upper-off+))
(defun (setf page-lower) (v page) (page-set-u16 page +pd-lower-off+ v))
(defun (setf page-upper) (v page) (page-set-u16 page +pd-upper-off+ v))

(defun page-lp-count (page)
  (floor (- (page-lower page) +size-of-page-header+) +size-of-item-id+))

(defun page-get-lp (page offnum)
  "Return (values lp_off lp_flags lp_len) for 1-based OFFNUM."
  (let ((v (page-u32 page (+ +size-of-page-header+
                             (* (1- offnum) +size-of-item-id+)))))
    (values (logand v #x7FFF)
            (logand (ash v -15) #x3)
            (logand (ash v -17) #x3FFF))))

(defmacro with-live-page-item ((page offnum off len) &body body)
  "Run BODY when OFFNUM on PAGE has a normal line pointer.

PAGE and OFFNUM are each evaluated once. OFF and LEN are bound to the line
pointer's tuple offset and length; invalid, unused, and dead slots are skipped."
  (let ((page-value (gensym "PAGE-"))
        (offnum-value (gensym "OFFNUM-"))
        (flags (gensym "FLAGS-")))
    `(let ((,page-value ,page)
           (,offnum-value ,offnum))
       (when (<= 1 ,offnum-value (page-lp-count ,page-value))
         (multiple-value-bind (,off ,flags ,len)
             (page-get-lp ,page-value ,offnum-value)
           (declare (ignorable ,len))
           (when (= ,flags +lp-normal+)
             ,@body))))))

(defmacro do-live-page-items ((page offnum off len) &body body)
  "Run BODY for each normal line pointer on PAGE.

PAGE and its line-pointer count are each evaluated once. OFFNUM, OFF, and LEN
are bound for each live item; dead and unused slots are skipped."
  (let ((page-value (gensym "PAGE-"))
        (line-pointer-count (gensym "LINE-POINTER-COUNT-"))
        (flags (gensym "FLAGS-")))
    `(let* ((,page-value ,page)
            (,line-pointer-count (page-lp-count ,page-value)))
       (loop for ,offnum from 1 to ,line-pointer-count
             do (multiple-value-bind (,off ,flags ,len)
                    (page-get-lp ,page-value ,offnum)
                  (declare (ignorable ,off ,len))
                  (when (= ,flags +lp-normal+)
                    ,@body))))))

(defun page-set-lp (page offnum off flags len)
  (page-set-u32 page (+ +size-of-page-header+
                        (* (1- offnum) +size-of-item-id+))
                (logior (logand off #x7FFF)
                        (ash (logand flags #x3) 15)
                        (ash (logand len #x3FFF) 17))))

(defun page-free-space (page)
  (- (page-upper page) (page-lower page)))

(defun page-compact (page)
  "Move live tuple data to the top of the page, preserving line pointers."
  (let ((new (make-page))
        (n (page-lp-count page)))
    (replace new page :end2 +size-of-page-header+)
    (setf (page-lower new) (+ +size-of-page-header+
                              (* +size-of-item-id+ n)))
    (setf (page-upper new) +blcksz+)
    (loop for i from 1 to n
          do (multiple-value-bind (off flags len) (page-get-lp page i)
               (if (= flags +lp-normal+)
                   (let ((newoff (- (page-upper new) (maxalign len))))
                     (setf (page-upper new) newoff)
                     (replace new page
                              :start1 newoff
                              :start2 off
                              :end2 (+ off len))
                     (page-set-lp new i newoff flags len))
                   (page-set-lp new i 0 flags 0))))
    (replace page new)
    page))

(defun page-ensure-space (page extra aligned)
  (when (>= (page-free-space page) (+ extra aligned))
    (return-from page-ensure-space t))
  (page-compact page)
  (>= (page-free-space page) (+ extra aligned)))

(defun page-first-unused-slot (page)
  "Return the first reusable line pointer, or NIL when there is none."
  (let ((end (- (the fixnum (page-lower page)) +size-of-item-id+)))
    (flet ((scan (page)
             ;; The two ItemIdData flag bits straddle bytes 1 and 2.
             (loop for offset fixnum from +size-of-page-header+ to end by +size-of-item-id+
                   for slot fixnum from 1
                   when (and (zerop (logand (aref page (+ offset 1)) #x80))
                             (zerop (logand (aref page (+ offset 2)) #x01)))
                     return slot)))
      (declare (inline scan))
      (if (typep page '(simple-array (unsigned-byte 8) (*)))
          (locally (declare (optimize (speed 3))) (scan page))
          (scan page)))))

(defun page-add-item (page data &key allow-reuse offset-number)
  "Allocate DATA as a new tuple on PAGE and return its 1-based line pointer.
Return NIL if there is no room or OFFSET-NUMBER is outside the insertion range.
An explicit OFFSET-NUMBER inserts a pointer and shifts subsequent pointers,
as PageAddItemExtended does; callers must allow existing pointer numbers to change.
When the offset is omitted and ALLOW-REUSE is true, LP_UNUSED slots may be
recycled. LP_DEAD slots remain unavailable until VACUUM has removed every
index entry that can reference them and changed them to LP_UNUSED. The second
value indicates whether the returned slot was recycled."
  (let* ((len (length data))
         (aligned (maxalign len))
         (slot nil)
         (reused-p nil))
    (cond
      (offset-number
       (unless (<= 1 offset-number (1+ (page-lp-count page)))
         (return-from page-add-item nil)))
      (allow-reuse
       (setf slot (page-first-unused-slot page)
             reused-p (not (null slot)))))
    (unless (page-ensure-space page (if slot 0 +size-of-item-id+) aligned)
      (return-from page-add-item nil))
    (unless slot
      (setf slot (or offset-number (1+ (page-lp-count page))))
      (when offset-number
        (let ((start (+ +size-of-page-header+ (* (1- slot) +size-of-item-id+)))
              (lower (page-lower page)))
          (when (< start lower)
            (replace page page :start1 (+ start +size-of-item-id+) :start2 start :end2 lower))))
      (incf (page-lower page) +size-of-item-id+))
    (let ((newoff (- (page-upper page) aligned)))
      (setf (page-upper page) newoff)
      (replace page data :start1 newoff)
      (page-set-lp page slot newoff +lp-normal+ len)
      (values slot reused-p))))

(defun page-item-data (page offnum)
  "Return a copy of the tuple bytes at 1-based OFFNUM, or NIL if not live."
  (multiple-value-bind (off flags len) (page-get-lp page offnum)
    (when (= flags +lp-normal+)
      (subseq page off (+ off len)))))

(defun page-mark-dead (page offnum)
  (page-set-lp page offnum 0 +lp-dead+ 0)
  page)

(defun page-mark-unused (page offnum)
  (page-set-lp page offnum 0 +lp-unused+ 0)
  page)

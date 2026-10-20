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
  (logior (aref page off)
          (ash (aref page (1+ off)) 8)))

(defun page-u32 (page off)
  (logior (aref page off)
          (ash (aref page (+ off 1)) 8)
          (ash (aref page (+ off 2)) 16)
          (ash (aref page (+ off 3)) 24)))

(defun page-u64 (page off)
  (logior (page-u32 page off)
          (ash (page-u32 page (+ off 4)) 32)))

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
                  (declare (ignorable ,len))
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

(defun page-add-item (page data &key allow-reuse)
  "Allocate DATA as a new tuple on PAGE. Return the 1-based line pointer
number, or NIL if the page has no room. With ALLOW-REUSE, LP_UNUSED/LP_DEAD
slots may be recycled; callers must only do this when no index entry can
reference the slot (see plan section 5)."
  (let* ((len (length data))
         (aligned (maxalign len))
         (slot nil))
    (when allow-reuse
      (loop for i from 1 to (page-lp-count page)
            do (multiple-value-bind (off flags l) (page-get-lp page i)
                 (declare (ignore off l))
                 (when (or (= flags +lp-unused+) (= flags +lp-dead+))
                   (setf slot i)
                   (return)))))
    (unless (page-ensure-space page (if slot 0 +size-of-item-id+) aligned)
      (return-from page-add-item nil))
    (unless slot
      (setf slot (1+ (page-lp-count page)))
      (setf (page-lower page)
            (+ +size-of-page-header+ (* +size-of-item-id+ slot))))
    (let ((newoff (- (page-upper page) aligned)))
      (setf (page-upper page) newoff)
      (replace page data :start1 newoff)
      (page-set-lp page slot newoff +lp-normal+ len)
      slot)))

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

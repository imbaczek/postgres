;;;; btree.lisp -- file-backed unique/duplicate B+tree index.
;;;;
;;;; Page 0 is the meta page, page 1 the initial root (also a leaf).
;;;; Internal pages carry (separator, child) entries where the separator is
;;;; the smallest key in the child subtree; the first entry may have an empty
;;;; separator (minus infinity). Leaf pages carry (key, heap TID) entries.
;;;; Stale entries are never removed: visibility of the heap tuple decides
;;;; whether they still matter.

(in-package #:pg)

(defconstant +btree-meta-magic+ #x42545245)   ; "BTRE"
(defconstant +btree-page-meta+ 1)
(defconstant +btree-page-leaf+ 2)
(defconstant +btree-page-internal+ 3)

(defun btree-page-type (page) (aref page +pd-flags-off+))
(defun set-btree-page-type (page type)
  (setf (aref page +pd-flags-off+) type)
  (page-set-u16 page +pd-checksum-off+ 0)
  page)

(defun empty-key ()
  (make-array 0 :element-type '(unsigned-byte 8)))

(defun key-compare (a b)
  (loop for i from 0 below (min (length a) (length b))
        do (cond ((< (aref a i) (aref b i)) (return -1))
                 ((> (aref a i) (aref b i)) (return 1)))
        finally (return (cond ((< (length a) (length b)) -1)
                              ((> (length a) (length b)) 1)
                              (t 0)))))

(defun key< (a b) (minusp (key-compare a b)))
(defun key= (a b) (zerop (key-compare a b)))
(defun key<= (a b) (not (plusp (key-compare a b))))

;;; ---- entry encoding ----

(defun encode-leaf-entry (key block off)
  (let* ((klen (length key))
         (v (make-array (+ 2 klen 6) :element-type '(unsigned-byte 8)
                                     :initial-element 0)))
    (page-set-u16 v 0 klen)
    (replace v key :start1 2)
    (page-set-u32 v (+ 2 klen) block)
    (page-set-u16 v (+ 6 klen) off)
    v))

(defun decode-leaf-entry (v)
  (let* ((klen (page-u16 v 0))
         (key (subseq v 2 (+ 2 klen)))
         (block (page-u32 v (+ 2 klen)))
         (off (page-u16 v (+ 6 klen))))
    (list key block off)))

(defun encode-internal-entry (key child)
  (let* ((klen (length key))
         (v (make-array (+ 2 klen 4) :element-type '(unsigned-byte 8)
                                     :initial-element 0)))
    (page-set-u16 v 0 klen)
    (replace v key :start1 2)
    (page-set-u32 v (+ 2 klen) child)
    v))

(defun decode-internal-entry (v)
  (let* ((klen (page-u16 v 0))
         (key (subseq v 2 (+ 2 klen)))
         (child (page-u32 v (+ 2 klen))))
    (cons key child)))

(defun btree-page-entries (page internal-p)
  (let ((entries '()))
    (do-live-page-items (page offnum off len)
      (let ((raw (subseq page off (+ off len))))
        (push (if internal-p
                  (decode-internal-entry raw)
                  (decode-leaf-entry raw))
              entries)))
    (nreverse entries)))

(defun btree-page-build (entries internal-p)
  (let ((page (make-page)))
    (page-init page)
    (set-btree-page-type page (if internal-p
                                  +btree-page-internal+
                                  +btree-page-leaf+))
    (dolist (e entries)
      (let ((item (if internal-p
                      (encode-internal-entry (car e) (cdr e))
                      (encode-leaf-entry (first e) (second e) (third e)))))
        (unless (page-add-item page item :allow-reuse nil)
          (return-from btree-page-build nil))))
    page))

(defun leaf-entry-less-p (a b)
  (let ((c (key-compare (first a) (first b))))
    (if (zerop c)
        (or (< (second a) (second b))
            (and (= (second a) (second b))
                 (< (third a) (third b))))
        (minusp c))))

;;; Internal-page insertion places a split's separator immediately after the
;;; child it came from (see btree-insert-rec) instead of re-sorting, so equal
;;; separators keep their key-range order even though KEY< is not a total order.

;;; ---- meta ----

(defun btree-init (relfilenode)
  (with-wal-page-batch
    ;; Recheck while holding the page-batch lock: two initializers can both
    ;; observe an empty relation before either enters this batch.
    (unless (zerop (buf-nblocks relfilenode))
      (return-from btree-init (values)))
    (let ((meta (make-page)))
      (page-init meta)
      (set-btree-page-type meta +btree-page-meta+)
      (page-set-u32 meta 24 +btree-meta-magic+)
      (page-set-u16 meta 28 0)
      (page-set-u32 meta 30 1)
      (buf-write relfilenode 0 meta)
      (let ((leaf (make-page)))
        (page-init leaf)
        (set-btree-page-type leaf +btree-page-leaf+)
        (buf-write relfilenode 1 leaf)))
    (mark-dirty relfilenode))
  (values))

(defun btree-ensure-init (relfilenode)
  (when (zerop (buf-nblocks relfilenode))
    (btree-init relfilenode)))

(defun btree-read-meta (relfilenode)
  (btree-ensure-init relfilenode)
  (let ((meta (buf-read relfilenode 0)))
    (unless (and meta (= (page-u32 meta 24) +btree-meta-magic+))
      (ereport "XX000" "index meta page is corrupt"))
    (values (page-u16 meta 28) (page-u32 meta 30))))

(defun btree-write-meta (relfilenode level root)
  (let ((meta (buf-read relfilenode 0)))
    (page-set-u16 meta 28 level)
    (page-set-u32 meta 30 root)
    (buf-write relfilenode 0 meta)
    (mark-dirty relfilenode))
  (values))

;;; ---- search ----

(defun btree-collect-equal (relfilenode level block key acc)
  "Push all TIDs whose key equals KEY in the subtree at BLOCK onto ACC.

Separators store the minimum key of each child.  Duplicate keys can
straddle a leaf split, so the same key may occur in several sibling
subtrees; visit every child that can contain KEY instead of descending
to a single leaf."
  (let ((page (buf-read relfilenode block)))
    (unless page
      (ereport "XX000" "index page ~a is missing" block))
    (if (zerop level)
        (progn
          (dolist (e (btree-page-entries page nil))
            (when (key= (first e) key)
              (push (cons (second e) (third e)) acc)))
          acc)
        (let ((entries (btree-page-entries page t)))
          (when (null entries)
            (return-from btree-collect-equal acc))
          ;; J is the last child whose minimum key is <= KEY.
          (let ((j 0))
            (loop for e in entries
                  for i from 0
                  when (key<= (car e) key)
                    do (setf j i))
            ;; If separators equal KEY, the run of such separators plus the
            ;; child immediately to its left can all contain KEY.
            (let ((start j))
              (when (key= (car (nth j entries)) key)
                (loop while (and (> start 0)
                                 (key= (car (nth (1- start) entries)) key))
                      do (decf start))
                (when (> start 0)
                  (decf start)))
              (loop for i from start to j
                    do (setf acc
                             (btree-collect-equal
                              relfilenode (1- level)
                              (cdr (nth i entries))
                              key acc))))
            acc)))))

(defun btree-search (relfilenode key)
  "Return a list of (block . off) heap TIDs for entries whose key equals KEY."
  (btree-ensure-init relfilenode)
  (multiple-value-bind (level root) (btree-read-meta relfilenode)
    (nreverse (btree-collect-equal relfilenode level root key '()))))

;;; ---- insert ----

(defun btree-split-entries (entries)
  "Split ENTRIES in half; return (values first-half second-half)."
  (let* ((n (length entries))
         (k (max 1 (floor n 2))))
    (when (>= k n) (setf k (1- n)))
    (values (subseq entries 0 k) (subseq entries k))))

(defun btree-insert-rec (relfilenode level block key tid-block tid-off)
  "Return (values split-p separator new-block)."
  (let ((page (buf-read relfilenode block)))
    (unless page
      (ereport "XX000" "index page ~a is missing" block))
    (if (zerop level)
        (let* ((entries (btree-page-entries page nil))
               (new-entry (list key tid-block tid-off))
               (all (sort (cons new-entry entries) #'leaf-entry-less-p))
               (built (btree-page-build all nil)))
          (if built
              (progn
                (buf-write relfilenode block built)
                (mark-dirty relfilenode)
                (values nil nil nil))
              (multiple-value-bind (old-entries new-entries)
                  (btree-split-entries all)
                (let ((old-page (btree-page-build old-entries nil))
                      (new-page (btree-page-build new-entries nil)))
                  (unless (and old-page new-page)
                    (ereport "54000" "index entry is too large"))
                  (buf-write relfilenode block old-page)
                  (let ((new-block (buf-nblocks relfilenode)))
                    (buf-write relfilenode new-block new-page)
                    (mark-dirty relfilenode)
                    (values t (first (first new-entries)) new-block))))))
        (let* ((entries (btree-page-entries page t))
               (chosen-index
                 (let ((idx 0))
                   (loop for e in entries
                         for i from 0
                         when (key<= (car e) key)
                           do (setf idx i))
                   idx))
               (child (cdr (nth chosen-index entries))))
          (multiple-value-bind (split-p sep new-block)
              (btree-insert-rec relfilenode (1- level) child
                                key tid-block tid-off)
            (if (not split-p)
                (values nil nil nil)
                (let* ((all (append (subseq entries 0 (1+ chosen-index))
                                    (list (cons sep new-block))
                                    (subseq entries (1+ chosen-index))))
                       (built (btree-page-build all t)))
                  (if built
                      (progn
                        (buf-write relfilenode block built)
                        (mark-dirty relfilenode)
                        (values nil nil nil))
                      (multiple-value-bind (old-entries new-entries)
                          (btree-split-entries all)
                        (let ((old-page (btree-page-build old-entries t))
                              (new-page (btree-page-build new-entries t)))
                          (unless (and old-page new-page)
                            (ereport "54000" "index internal entry is too large"))
                          (buf-write relfilenode block old-page)
                          (let ((new-block (buf-nblocks relfilenode)))
                            (buf-write relfilenode new-block new-page)
                            (mark-dirty relfilenode)
                            (values t (car (first new-entries)) new-block))))))))))))

(defun btree-insert (relfilenode key tid-block tid-off)
  (with-wal-page-batch
    (btree-ensure-init relfilenode)
    (multiple-value-bind (level root) (btree-read-meta relfilenode)
      (multiple-value-bind (split-p sep new-block)
          (btree-insert-rec relfilenode level root key tid-block tid-off)
        (when split-p
          (wal-mark-btree-split)
          (let ((old-root root))
            (setf root (buf-nblocks relfilenode))
            (let ((root-page (btree-page-build
                              (list (cons (empty-key) old-root)
                                    (cons sep new-block))
                              t)))
              (unless root-page
                (ereport "54000" "index root is too large"))
              (buf-write relfilenode root root-page)
              (btree-write-meta relfilenode (1+ level) root)))))))
  (values))

;;; ---- key encoding ----

(defun encode-index-key (values types)
  "Encode index column values so that byte-wise order matches SQL order for
the MVP types. Null sorts before any non-null value; ints are sign-flipped."
  (let ((out (make-array 0 :adjustable t :fill-pointer 0
                           :element-type '(unsigned-byte 8))))
    (labels ((push-byte (b) (vector-push-extend b out))
             (push-int-be (v bytes)
               (loop for shift from (* 8 (1- bytes)) downto 0 by 8
                     do (push-byte (ldb (byte 8 shift) v)))))
      (loop for v in values
            for ty in types
            do (cond
                 ((eq v +null+)
                  (push-byte 0))
                 ((eq ty :int4)
                  (push-byte 1)
                  (push-int-be (logxor (logand v #xFFFFFFFF) #x80000000) 4))
                 ((eq ty :int8)
                  (push-byte 2)
                  (push-int-be (logxor (logand v #xFFFFFFFFFFFFFFFF)
                                       #x8000000000000000)
                               8))
                 ((eq ty :varchar)
                  (push-byte 3)
                  (let ((data (string->bytes v)))
                    (push-byte (ldb (byte 8 8) (length data)))
                    (push-byte (ldb (byte 8 0) (length data)))
                    (loop for b across data do (push-byte b))))
                 (t
                  (ereport "XX000" "unsupported index key type ~a" ty)))))
    (coerce out '(simple-array (unsigned-byte 8) (*)))))

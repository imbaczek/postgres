;;;; btree.lisp -- file-backed unique/duplicate B+tree index.
;;;;
;;;; Page 0 is the meta page, page 1 the initial root (also a leaf).
;;;; Internal pages carry (separator, child) entries where the separator is
;;;; the smallest key in the child subtree; the first entry may have an empty
;;;; separator (minus infinity). Leaf pages carry (key, heap TID) entries.
;;;; Heap visibility guards stale entries until VACUUM removes their TIDs.

(in-package #:pg)

(defconstant +btree-meta-magic+ #x42545245)   ; "BTRE"
(defconstant +btree-page-meta+ 1)
(defconstant +btree-page-leaf+ 2)
(defconstant +btree-page-internal+ 3)
(defconstant +btree-vacuum-max-split-retries+ 2)
(defvar *btree-split-generations* (make-hash-table :test #'eql))
(defvar *btree-split-lock* (sb-thread:make-mutex :name "pg btree split generation"))
(defvar *btree-relation-locks* (make-hash-table :test #'eql))
(defvar *btree-relation-lock-map-lock*
  (sb-thread:make-mutex :name "pg btree relation lock map"))

(defstruct btree-relation-lock-state
  mutex
  waitqueue
  (reader-count 0)
  (reader-owners (make-hash-table :test #'eq))
  (waiting-writers 0)
  writer-owner
  (writer-depth 0))

(defun btree-relation-lock (relfilenode)
  (sb-thread:with-mutex (*btree-relation-lock-map-lock*)
    (or (gethash relfilenode *btree-relation-locks*)
        (setf (gethash relfilenode *btree-relation-locks*)
              (make-btree-relation-lock-state
               :mutex (sb-thread:make-mutex
                       :name (format nil "pg btree relation ~a" relfilenode))
               :waitqueue (sb-thread:make-waitqueue
                           :name (format nil "pg btree relation ~a waiters"
                                         relfilenode)))))))

(defmacro with-btree-relation-lock ((relfilenode) &body body)
  (let ((relation (gensym "BTREE-RELATION-")))
    `(let ((,relation ,relfilenode))
       (call-with-btree-relation-write-lock
        ,relation (lambda () ,@body)))))

(declaim (notinline btree-relation-condition-wait))

(defun btree-relation-condition-wait (state)
  (check-for-interrupts)
  (condition-wait-with-timeout (btree-relation-lock-state-waitqueue state)
                               (btree-relation-lock-state-mutex state))
  (check-for-interrupts))

(defstruct btree-read-lease
  lock-state thread (status :new) writer-reentrant-p)

(defun btree-read-lease-active-p (lease)
  (eq (btree-read-lease-status lease) :held))

(defvar *btree-read-leases* nil)

(defun call-with-btree-read-lease-lock (state function)
  (let ((mutex (btree-relation-lock-state-mutex state)))
    (sb-sys:without-interrupts
      (sb-thread:grab-mutex mutex)
      (unwind-protect
           (sb-sys:allow-with-interrupts (funcall function))
        (when (eq (sb-thread:mutex-owner mutex) sb-thread:*current-thread*)
          (sb-thread:release-mutex mutex))))))

(defun acquire-btree-read-lease (lease)
  (unless (and (eq (btree-read-lease-thread lease) sb-thread:*current-thread*)
               (eq (btree-read-lease-status lease) :new))
    (error "B-tree read lease must be new and owned by this thread"))
  (let ((state (btree-read-lease-lock-state lease))
        (thread (btree-read-lease-thread lease)))
    (call-with-btree-read-lease-lock
     state
     (lambda ()
       (if (eq (btree-relation-lock-state-writer-owner state) thread)
           (sb-sys:without-interrupts
             (setf (btree-read-lease-writer-reentrant-p lease) t
                   (btree-read-lease-status lease) :held))
           (progn
             (loop while (or (btree-relation-lock-state-writer-owner state)
                             (and (zerop (gethash thread
                                                  (btree-relation-lock-state-reader-owners state) 0))
                                  (plusp (btree-relation-lock-state-waiting-writers state))))
                   do (btree-relation-condition-wait state))
             (sb-sys:without-interrupts
               (let ((next-count (1+ (btree-relation-lock-state-reader-count state)))
                     (next-depth (1+ (gethash thread
                                              (btree-relation-lock-state-reader-owners state) 0))))
                 (setf (gethash thread (btree-relation-lock-state-reader-owners state)) next-depth
                       (btree-relation-lock-state-reader-count state) next-count
                       (btree-read-lease-status lease) :held))))))))
  lease)

(defun release-btree-read-lease (lease)
  (sb-sys:without-interrupts
    (unless (eq (btree-read-lease-thread lease) sb-thread:*current-thread*)
      (error "B-tree read lease belongs to another thread"))
    (unless (btree-read-lease-active-p lease)
      (setf (btree-read-lease-status lease) :released)
      (return-from release-btree-read-lease nil))
    (let ((state (btree-read-lease-lock-state lease))
          (thread (btree-read-lease-thread lease))
          (pending nil))
      (loop
        (handler-case
            (return
              (call-with-btree-read-lease-lock
               state
               (lambda ()
                 (unless (btree-read-lease-writer-reentrant-p lease)
                   (let ((depth (gethash thread (btree-relation-lock-state-reader-owners state) 0))
                         (count (btree-relation-lock-state-reader-count state)))
                     (unless (and (plusp depth) (plusp count))
                       (error "B-tree read lease ownership mismatch"))
                     (if (= depth 1)
                         (remhash thread (btree-relation-lock-state-reader-owners state))
                         (setf (gethash thread (btree-relation-lock-state-reader-owners state))
                               (1- depth)))
                     (setf (btree-relation-lock-state-reader-count state) (1- count))))
                 (setf (btree-read-lease-status lease) :released)
                 (when (zerop (btree-relation-lock-state-reader-count state))
                   (sb-thread:condition-broadcast (btree-relation-lock-state-waitqueue state))))))
          (sb-ext:timeout (condition) (setf pending condition))))
      (when pending (error pending)))
    t))

(defun call-with-btree-relation-read-lease (relfilenode function)
  (let ((lease (make-btree-read-lease :lock-state (btree-relation-lock relfilenode)
                                      :thread sb-thread:*current-thread*)))
    (unwind-protect
         (progn
           (acquire-btree-read-lease lease)
           (let ((*btree-read-leases* (cons lease *btree-read-leases*)))
             (funcall function lease)))
      (sb-sys:without-interrupts (release-btree-read-lease lease)))))

(defun call-with-btree-relation-read-lock (relfilenode function)
  (call-with-btree-relation-read-lease
   relfilenode
   (lambda (lease)
     (declare (ignore lease))
     (funcall function))))

(defun call-with-btree-relation-write-lock (relfilenode function)
  (let* ((state (btree-relation-lock relfilenode))
         (thread sb-thread:*current-thread*)
         (registered-waiter nil)
         (writer-held nil))
    ;; Install release cleanup before acquiring ownership, so an interrupt at
    ;; the acquisition boundary cannot strand the relation lock.
    (unwind-protect
         (progn
           (sb-thread:with-mutex ((btree-relation-lock-state-mutex state))
             (if (eq (btree-relation-lock-state-writer-owner state) thread)
                 (sb-sys:without-interrupts
                   (incf (btree-relation-lock-state-writer-depth state))
                   (setf writer-held t))
                 (progn
                   (when (plusp (gethash thread
                                         (btree-relation-lock-state-reader-owners state)
                                         0))
                     (error "cannot upgrade a B-tree scan lock to a writer lock"))
                   (unwind-protect
                        (progn
                          ;; Like LWLockDequeueSelf() in
                          ;; src/backend/storage/lmgr/lwlock.c, every interrupted
                          ;; wait must remove its waiter registration.
                          (sb-sys:without-interrupts
                            (incf (btree-relation-lock-state-waiting-writers state))
                            (setf registered-waiter t))
                          (loop while (or (btree-relation-lock-state-writer-owner state)
                                          (plusp (btree-relation-lock-state-reader-count state)))
                                do (btree-relation-condition-wait state))
                          (sb-sys:without-interrupts
                            (decf (btree-relation-lock-state-waiting-writers state))
                            (setf registered-waiter nil
                                  (btree-relation-lock-state-writer-owner state) thread
                                  (btree-relation-lock-state-writer-depth state) 1
                                  writer-held t)))
                     (when registered-waiter
                       (sb-sys:without-interrupts
                         (decf (btree-relation-lock-state-waiting-writers state))
                         (setf registered-waiter nil)
                         (sb-thread:condition-broadcast
                          (btree-relation-lock-state-waitqueue state))))))))
           (funcall function))
      (when writer-held
        (sb-thread:with-mutex ((btree-relation-lock-state-mutex state))
          (decf (btree-relation-lock-state-writer-depth state))
          (when (zerop (btree-relation-lock-state-writer-depth state))
            (setf (btree-relation-lock-state-writer-owner state) nil)
            (sb-thread:condition-broadcast
             (btree-relation-lock-state-waitqueue state))))))))

(defun call-with-btree-relation-locks (relfilenodes function)
  (let ((relfilenodes (sort (remove-duplicates (copy-list relfilenodes)
                                               :test #'eql)
                            #'<)))
    (labels ((lock-next (remaining)
               (if (null remaining)
                   (funcall function)
                   (call-with-btree-relation-write-lock
                    (car remaining)
                    (lambda () (lock-next (cdr remaining)))))))
      (lock-next relfilenodes))))

(defmacro with-btree-relation-locks ((relfilenodes) &body body)
  `(call-with-btree-relation-locks ,relfilenodes (lambda () ,@body)))

(defun call-with-btree-relation-read-locks (relfilenodes function)
  "Hold a shared lease on each relation in writer-compatible lock order."
  (let ((relfilenodes (sort (remove-duplicates (copy-list relfilenodes)
                                               :test #'eql)
                            #'<)))
    (labels ((lock-next (remaining)
               (if (null remaining)
                   (funcall function)
                   (call-with-btree-relation-read-lock
                    (car remaining)
                    (lambda () (lock-next (cdr remaining)))))))
      (lock-next relfilenodes))))

(defmacro with-btree-relation-read-locks ((relfilenodes) &body body)
  `(call-with-btree-relation-read-locks ,relfilenodes (lambda () ,@body)))

(defun btree-ensure-init-before-read (relfilenode)
  "Initialize an empty index before entering any shared read-lease set."
  (unless (plusp (buf-nblocks relfilenode))
    (with-btree-relation-lock (relfilenode)
      (btree-ensure-init relfilenode)))
  (values))

(defun btree-split-generation (relfilenode)
  (sb-thread:with-mutex (*btree-split-lock*)
    (gethash relfilenode *btree-split-generations* 0)))

(defun btree-record-split (relfilenode)
  "Compatibility name for the per-relation mutation generation bump."
  (btree-record-mutation relfilenode))

(defun btree-record-mutation (relfilenode)
  (sb-thread:with-mutex (*btree-split-lock*)
    (incf (gethash relfilenode *btree-split-generations* 0))))

(defun btree-forget (relfilenode)
  "Discard split-generation state after RELFILENODE is permanently deleted."
  (let ((lock-state (btree-relation-lock relfilenode)))
    (call-with-btree-relation-write-lock
     relfilenode
     (lambda ()
       ;; Callers hold the relation DDL gate, so no operation can have fetched
       ;; this state and still be waiting to enter it.
       (sb-thread:with-mutex (*btree-relation-lock-map-lock*)
         (when (eq (gethash relfilenode *btree-relation-locks*) lock-state)
           (remhash relfilenode *btree-relation-locks*)))
       (sb-thread:with-mutex (*btree-split-lock*)
         (remhash relfilenode *btree-split-generations*)))))
  (values))

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

(defun btree-init-unlocked (relfilenode)
  (with-wal-page-batch
    ;; Recheck while holding the page-batch lock: two initializers can both
    ;; observe an empty relation before either enters this batch.
    (unless (zerop (buf-nblocks relfilenode))
      (return-from btree-init-unlocked (values)))
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
        (buf-write relfilenode 1 leaf))
      (btree-record-mutation relfilenode))
    (mark-dirty relfilenode))
  (values))

(defun btree-init (relfilenode)
  (with-btree-relation-lock (relfilenode)
    (btree-init-unlocked relfilenode)))

(defun btree-ensure-init (relfilenode)
  (when (zerop (buf-nblocks relfilenode))
    (btree-init-unlocked relfilenode)))

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

(defun btree-search-unlocked (relfilenode key)
  "Return a list of (block . off) heap TIDs for entries whose key equals KEY."
  (btree-ensure-init relfilenode)
  (multiple-value-bind (level root) (btree-read-meta relfilenode)
    (nreverse (btree-collect-equal relfilenode level root key '()))))

(defun btree-search (relfilenode key)
  "Search one index while excluding its own concurrent page rewrites."
  (btree-ensure-init-before-read relfilenode)
  (call-with-btree-relation-read-lock
   relfilenode (lambda () (btree-search-unlocked relfilenode key))))

(defstruct btree-equal-cursor
  relfilenode key stack)

(defstruct btree-equal-cursor-frame
  level entries (next 0) (end -1))

(defun btree-equal-child-range (entries key)
  "Return the first and last child that can contain KEY."
  (let ((last-lower-bound 0))
    (loop for entry in entries
          for index from 0
          when (key<= (car entry) key)
            do (setf last-lower-bound index))
    (let ((first-candidate last-lower-bound))
      (when (key= (car (nth last-lower-bound entries)) key)
        (loop while (and (> first-candidate 0)
                         (key= (car (nth (1- first-candidate) entries)) key))
              do (decf first-candidate))
        (when (plusp first-candidate)
          (decf first-candidate)))
      (values first-candidate last-lower-bound))))

(defun btree-cursor-push-node (cursor level block)
  (let ((page (buf-read (btree-equal-cursor-relfilenode cursor) block)))
    (unless page
      (ereport "XX000" "index page ~a is missing" block))
    (if (zerop level)
        (let ((entries (btree-page-entries page nil)))
          (push (make-btree-equal-cursor-frame
                 :level 0 :entries entries :next 0 :end (1- (length entries)))
                (btree-equal-cursor-stack cursor)))
        (let ((entries (btree-page-entries page t)))
          (unless (null entries)
            (multiple-value-bind (first last)
                (btree-equal-child-range entries (btree-equal-cursor-key cursor))
              (push (make-btree-equal-cursor-frame
                     :level level :entries entries :next first :end last)
                    (btree-equal-cursor-stack cursor))))))))

(defun btree-cursor-next-tid (cursor)
  (loop while (btree-equal-cursor-stack cursor)
        for frame = (first (btree-equal-cursor-stack cursor))
        for next = (btree-equal-cursor-frame-next frame)
        do (if (> next (btree-equal-cursor-frame-end frame))
               (pop (btree-equal-cursor-stack cursor))
               (if (zerop (btree-equal-cursor-frame-level frame))
                   (let* ((entry (nth next
                                      (btree-equal-cursor-frame-entries frame)))
                          (key-order
                            (key-compare (first entry)
                                         (btree-equal-cursor-key cursor)))
                          (tid (cons (second entry) (third entry))))
                     (incf (btree-equal-cursor-frame-next frame))
                     (cond
                       ((minusp key-order))
                       ((plusp key-order)
                        (pop (btree-equal-cursor-stack cursor)))
                       (t (return tid))))
                   (let* ((entries (btree-equal-cursor-frame-entries frame))
                          (index next)
                          (child (cdr (nth index entries)))
                          (child-level (1- (btree-equal-cursor-frame-level frame))))
                     (incf (btree-equal-cursor-frame-next frame))
                     (btree-cursor-push-node cursor child-level child))))))

(defun btree-scan-equal (relfilenode key callback)
  "Stream matching TIDs to CALLBACK until it returns :STOP.

Like PostgreSQL's nbtree.c:btgettuple loop over _bt_first/_bt_next in
nbtsearch.c, this retains only a scan position and one TID at a time. PostgreSQL
uses page pins, page locks, and sibling links to survive concurrent splits; this
tree has key-only separators and no sibling links, so hold its relation-scoped
read lease for the scan to preserve the actual leaf traversal order. Readers can
nest and share this lease; relation writers wait until all scans finish."
  (btree-ensure-init-before-read relfilenode)
  (call-with-btree-relation-read-lock
   relfilenode
   (lambda ()
     (multiple-value-bind (level root) (btree-read-meta relfilenode)
       (let ((cursor (make-btree-equal-cursor :relfilenode relfilenode :key key)))
         (btree-cursor-push-node cursor level root)
         (loop for tid = (btree-cursor-next-tid cursor)
               while tid
               do (when (eq (funcall callback tid) :stop)
                    (return-from btree-scan-equal :stop))))))))

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
                    (btree-record-split relfilenode)
                    (wal-mark-btree-split)
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
                            (btree-record-split relfilenode)
                            (wal-mark-btree-split)
                            (mark-dirty relfilenode)
                            (values t (car (first new-entries)) new-block))))))))))))

(defun btree-insert-unlocked (relfilenode key tid-block tid-off)
  (with-wal-page-batch
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
              (btree-write-meta relfilenode (1+ level) root))))))
    ;; Track non-splitting inserts too, like PostgreSQL's scan-position checks
    ;; around concurrent leaf changes in nbtsearch.c.
    (btree-record-mutation relfilenode))
  (values))

(defun btree-insert (relfilenode key tid-block tid-off)
  (with-btree-relation-lock (relfilenode)
    (btree-insert-unlocked relfilenode key tid-block tid-off)))

(defun btree-vacuum-delete-page-tids (relfilenode block dead)
  (let ((page (buf-read relfilenode block)))
    (when (and page (= (btree-page-type page) +btree-page-leaf+))
      (let* ((entries (btree-page-entries page nil))
             (remaining
               (remove-if (lambda (entry)
                            (gethash (cons (second entry) (third entry)) dead))
                          entries)))
        (when (< (length remaining) (length entries))
          (let ((new-page (btree-page-build remaining nil)))
            (unless new-page
              (ereport "XX000" "could not rebuild B-tree leaf page"))
            (with-wal-page-batch
              (buf-write relfilenode block new-page)
              (btree-record-mutation relfilenode))))))))

(defun btree-vacuum-scan-tids (relfilenode dead nblocks
                               &key exclusive after-page-function)
  (dotimes (block nblocks)
    (check-for-interrupts)
    (if exclusive
        (btree-vacuum-delete-page-tids relfilenode block dead)
        (with-btree-relation-lock (relfilenode)
          (btree-vacuum-delete-page-tids relfilenode block dead)))
    ;; Tests use this hook to force splits between page visits. The final pass
    ;; deliberately omits it while holding the split barrier.
    (when (and after-page-function (not exclusive))
      (funcall after-page-function relfilenode block))))

(defun btree-vacuum-delete-tids (relfilenode tids &key after-page-function)
  "Remove TIDS, retrying boundedly before a write-locked final scan."
  (when tids
    (let ((dead (make-hash-table :test #'equal))
          (retries 0))
      (dolist (tid tids)
        (setf (gethash (cons (car tid) (cdr tid)) dead) t))
      (loop
        for generation = (btree-split-generation relfilenode)
        for nblocks = (buf-nblocks relfilenode)
        do (btree-vacuum-scan-tids relfilenode dead nblocks
                                   :after-page-function after-page-function)
           (wal-commit-maintenance)
           (when (= generation (btree-split-generation relfilenode))
             (return))
           (if (< retries +btree-vacuum-max-split-retries+)
               (incf retries)
               (progn
                 ;; Once bounded retries are exhausted, one relation-exclusive
                 ;; pass gives cleanup a stable page set without stalling writes
                 ;; to unrelated indexes.
                 (with-btree-relation-lock (relfilenode)
                   (btree-vacuum-scan-tids relfilenode dead
                                           (buf-nblocks relfilenode)
                                           :exclusive t)
                   (wal-commit-maintenance))
                 (return))))))
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

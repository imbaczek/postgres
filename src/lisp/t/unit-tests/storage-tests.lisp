;;;; storage-tests.lisp -- page, tuple, and free-space tests.

(in-package #:pg)

(defun test-xid-tuple-update-roundtrip ()
  ;; Exercise the heap-level xmin/xmax contract behind the update system-column
  ;; cases in src/test/regress/sql/update.sql, using a later 32-bit XID epoch.
  (let* ((*next-xid* (+ +xid-cycle+ 52))
         (relfilenode 918273646)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (insert-xid (+ +xid-cycle+ 50))
         (update-xid (+ +xid-cycle+ 51)))
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (multiple-value-bind (old-block old-offnum)
             (heapam-insert relfilenode columns (vector 10) insert-xid 0)
           (heapam-set-xmax relfilenode old-block old-offnum update-xid 1)
           (multiple-value-bind (new-block new-offnum)
               (heapam-insert relfilenode columns (vector 11) update-xid 1)
             (check (heapam-set-ctid relfilenode old-block old-offnum
                                     new-block new-offnum)
                    "heap update links the old tuple to its successor")
             (let ((old-tuple (heapam-get-tuple relfilenode old-block
                                                old-offnum columns))
                   (new-tuple (heapam-get-tuple relfilenode new-block
                                                new-offnum columns)))
               (check (and (= (htup-ctid-block old-tuple) new-block)
                           (= (htup-ctid-offnum old-tuple) new-offnum))
                      "heap read preserves the update-chain ctid")
               (check (= (htup-xmin old-tuple) insert-xid)
                      "heap read reconstructs the old row's FullXID xmin")
               (check (= (htup-xmax old-tuple) update-xid)
                      "heap read reconstructs the updating FullXID xmax")
               (check (= (htup-xmin new-tuple) update-xid)
                      "heap read reconstructs the new row's FullXID xmin")
               (check (zerop (htup-xmax new-tuple))
                      "new row has an invalid xmax"))))
      (smgr-forget relfilenode :delete t))))

(defun test-heap-update-chain-follow ()
  ;; PostgreSQL's heap_delete reports TM_Updated and ExecDelete runs
  ;; EvalPlanQual. Model the ctid walk and row-predicate recheck at heap level.
  (let* ((*next-xid* 7205)
         (*active-xids* '(7204))
         (*backend* (make-backend))
         (*current-snapshot* (make-snapshot-struct :xmin 7200 :xmax 7205
                                                   :xip '() :curcid 10))
         (relfilenode 918273647)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (tids '()))
    (dolist (xid '(7200 7201 7202 7203))
      (clog-set xid +clog-committed+))
    (clog-set 7204 +clog-in-progress+)
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (progn
           (let ((previous nil))
             (dolist (spec '((7200 10) (7201 20) (7202 30) (7203 40)))
               (destructuring-bind (xmin value) spec
                 (multiple-value-bind (block offnum)
                     (heapam-insert relfilenode columns (vector value) xmin 0)
                   (when previous
                     (heapam-set-xmax relfilenode (first previous)
                                      (second previous) xmin 0)
                     (heapam-set-ctid relfilenode (first previous)
                                      (second previous) block offnum))
                   (push (cons block offnum) tids)
                   (setf previous (list block offnum))))))
           (setf tids (nreverse tids))
           (multiple-value-bind (deleted tuple block offnum)
               (heapam-delete-tid relfilenode columns
                                  (caar tids) (cdar tids) 7204 1
                                  (lambda (candidate)
                                    (= (aref (htup-values candidate) 0) 40)))
             (check deleted "update chain reaches and matches latest tuple")
             (check (= (aref (htup-values tuple) 0) 40)
                    "update-chain recheck returns the latest row values")
             (check (equal (cons block offnum) (car (last tids)))
                    "update-chain delete marks the latest tuple TID")
             (check (= (htup-xmin tuple) 7203)
                    "update chain can span several committed XIDs")
             (check (= (htup-xmax (heapam-get-tuple
                                   relfilenode block offnum columns))
                       7204)
                    "only the latest tuple receives the new xmax")))
      (release-xid 7204)
      (smgr-forget relfilenode :delete t))))

(defun test-heap-free-space-map ()
  (let ((fsm (make-heap-fsm)))
    (heap-fsm-record-page fsm 4 300)
    (heap-fsm-record-page fsm 6 160)
    (check (= (heap-fsm-take-page fsm 150) 6)
           "heap FSM selects the smallest fitting space category")
    (heap-fsm-record-page fsm 6 64)
    (check (= (heap-fsm-take-page fsm 250) 4)
           "heap FSM refreshes a page's space category")
    (check (null (heap-fsm-take-page fsm 250))
           "heap FSM skips pages below the requested space")
    (check (not (gethash 4 (heap-fsm-page-space fsm)))
           "taking a page removes its old category entry")
    (heap-fsm-record-page fsm 8 200)
    (check (= (heap-fsm-take-page fsm 200) 8)
           "heap FSM preserves exact-fit pages at category boundaries"))
  ;; The heap path should reuse the current page rather than extending a page
  ;; for every INSERT.
  (let* ((relfilenode 918273648)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (*backend* (make-backend)))
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (progn
           (dotimes (i 100)
             (heapam-insert relfilenode columns (vector i) 7200 0))
           (check (= (buf-nblocks relfilenode) 1)
                  "heap inserts reuse a page found through the FSM")
           (heap-fsm-forget relfilenode)
           (heapam-insert relfilenode columns #(100) 7200 0)
           (check (= (buf-nblocks relfilenode) 1)
                  "first insert rebuilds the FSM from existing heap pages"))
      (smgr-forget relfilenode :delete t))))

;;;; vacuum-tests.lisp -- VACUUM and B-tree tests.

(in-package #:pg)

(defun test-vacuum-page-publication-is-atomic ()
  (let* ((*wal-ready* nil)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-page-batch* nil)
         (*wal-test-pause-at* nil)
         (relfilenode 918273657)
         (heap-block 0)
         (heap-page (page-init (make-page)))
         (changed-page nil)
         (original-visibilitymap-clear
           (symbol-function 'visibilitymap-clear))
         (staged-heap-write-seen nil)
         (injected-error nil))
    (smgr-forget relfilenode :delete t)
    (ensure-directories-exist (relation-file-path relfilenode))
    (setf (aref heap-page 100) #x11
          changed-page (copy-seq heap-page))
    (unwind-protect
         (progn
           (smgr-write relfilenode heap-block heap-page)
           (visibilitymap-set-status
            relfilenode heap-block
            (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
           (let ((*wal-ready* t))
             (setf (aref changed-page 100) #x22
                   (symbol-function 'visibilitymap-clear)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (let ((staged-page
                             (and *wal-page-batch*
                                  (gethash
                                   (wal-page-key relfilenode heap-block)
                                   *wal-page-batch*))))
                       (setf staged-heap-write-seen
                             (and staged-page
                                  (= (aref staged-page 100) #x22))))
                     (error "injected failure before visibility-map clear")))
             (unwind-protect
                  (progn
                    (setf injected-error
                          (handler-case
                              (progn
                                (try-vacuum-page-publication
                                 relfilenode heap-block heap-page
                                 :page changed-page :write-page-p t)
                                nil)
                            (error (condition)
                              (search "injected failure before visibility-map clear"
                                      (princ-to-string condition)))))
                    (check injected-error
                           "VACUUM page publication propagates a VM-clear failure")
                    (check staged-heap-write-seen
                           "fault is injected after the heap page is staged")
                    (check (not (nth-value
                                 1 (gethash (wal-page-key relfilenode heap-block)
                                            *wal-page-cache*)))
                           "failed VACUUM publication exposes no staged heap page")
                    (check (not (nth-value
                                 1 (gethash (wal-page-key relfilenode 0 :visibility)
                                            *wal-page-cache*)))
                           "failed VACUUM publication exposes no staged VM page")
                    (check (equalp (buf-read relfilenode heap-block) heap-page)
                           "failed VACUUM publication preserves the original heap page")
                    (check (= (visibilitymap-status relfilenode heap-block)
                              (logior +vm-all-visible-bit+
                                      +vm-all-frozen-bit+))
                           "failed VACUUM publication preserves visibility bits"))
               (setf (symbol-function 'visibilitymap-clear)
                     original-visibilitymap-clear))
             (check (try-vacuum-page-publication
                     relfilenode heap-block heap-page
                     :page changed-page :write-page-p t)
                    "stable heap page accepts a VACUUM publication")
             (check (and (nth-value
                          1 (gethash (wal-page-key relfilenode heap-block)
                                     *wal-page-cache*))
                         (nth-value
                          1 (gethash (wal-page-key relfilenode 0 :visibility)
                                     *wal-page-cache*)))
                    "successful VACUUM publication exposes both pages together")
             (check (= (aref (buf-read relfilenode heap-block) 100) #x22)
                    "successful VACUUM publication exposes the heap edit")
             (check (zerop (visibilitymap-status relfilenode heap-block))
                    "successful VACUUM publication clears visibility bits")))
      (setf (symbol-function 'visibilitymap-clear)
            original-visibilitymap-clear)
      (smgr-forget relfilenode :delete t))))

(defun test-vacuum-page-read-releases-global-write-lock ()
  (let* ((*wal-ready* nil)
         (*heap-fsms* (make-hash-table :test #'eql))
         (heap-relfilenode 918273658)
         (other-relfilenode 918273659)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (test-pgdata *pgdata*)
         (test-fsms *heap-fsms*)
         (vacuum-backend (make-backend))
         (writer-backend (make-backend))
         (test-lock (sb-thread:make-mutex :name "vacuum copied-page test"))
         (test-cv (sb-thread:make-waitqueue :name "vacuum copied-page cv"))
         (page-read-paused nil)
         (resume-scan nil)
         (writer-done nil)
         (vacuum-done nil)
         (writer-error nil)
         (vacuum-error nil)
         (threads '())
         (original-buf-read (symbol-function 'buf-read)))
    (smgr-forget heap-relfilenode :delete t)
    (smgr-forget other-relfilenode :delete t)
    (clog-set 8210 +clog-committed+)
    (clog-set 8211 +clog-committed+)
    (unwind-protect
         (progn
           (heapam-insert heap-relfilenode columns #(1) 8210 0)
           ;; Keep FSM initialization out of the paused scan read.
           (heap-fsm-for-relation heap-relfilenode)
           (setf (symbol-function 'buf-read)
                 (lambda (relfilenode block &optional (fork :main))
                   (let ((page (funcall original-buf-read relfilenode block fork)))
                     (when (and page
                                (eq fork :main)
                                (= relfilenode heap-relfilenode)
                                (zerop block)
                                (sb-thread:with-mutex (test-lock)
                                  (unless page-read-paused
                                    (setf page-read-paused t)
                                    (sb-thread:condition-broadcast test-cv)
                                    t)))
                       (sb-thread:with-mutex (test-lock)
                         (loop until resume-scan
                               do (sb-thread:condition-wait test-cv test-lock))))
                     page)))
           (push
            (sb-thread:make-thread
             (lambda ()
               (let ((*pgdata* test-pgdata)
                     (*heap-fsms* test-fsms)
                     (*wal-ready* nil)
                     (*backend* vacuum-backend))
                 (handler-case
                     (heap-vacuum-relation heap-relfilenode nil 8212 8100)
                   (error (condition) (setf vacuum-error condition))))
               (sb-thread:with-mutex (test-lock)
                 (setf vacuum-done t)
                 (sb-thread:condition-broadcast test-cv)))
             :name "vacuum copied-page scan")
            threads)
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) page-read-paused)))
                  "VACUUM reaches a private copied-page read")
           (push
            (sb-thread:make-thread
             (lambda ()
               (let ((*pgdata* test-pgdata)
                     (*heap-fsms* test-fsms)
                     (*wal-ready* nil)
                     (*backend* writer-backend))
                 (handler-case
                     (with-write-lock
                       (heapam-insert other-relfilenode columns #(2) 8211 0))
                   (error (condition) (setf writer-error condition))))
               (sb-thread:with-mutex (test-lock)
                 (setf writer-done t)
                 (sb-thread:condition-broadcast test-cv)))
             :name "unrelated heap writer during vacuum")
            threads)
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) writer-done))
                   1.0)
                  "an unrelated heap INSERT progresses during VACUUM page work")
           (sb-thread:with-mutex (test-lock)
             (setf resume-scan t)
             (sb-thread:condition-broadcast test-cv))
           (dolist (thread threads)
             (sb-thread:join-thread thread :timeout 3))
           (check (null writer-error)
                  (format nil "the unrelated heap INSERT finishes without an error: ~a"
                          writer-error))
           (check (null vacuum-error)
                  (format nil "VACUUM completes after its copied page is released: ~a"
                          vacuum-error))
           (check vacuum-done "VACUUM worker terminates"))
      (sb-thread:with-mutex (test-lock)
        (setf resume-scan t)
        (sb-thread:condition-broadcast test-cv))
      (dolist (thread threads)
        (ignore-errors (sb-thread:join-thread thread :timeout 3)))
      (setf (symbol-function 'buf-read) original-buf-read)
      (smgr-forget heap-relfilenode :delete t)
      (smgr-forget other-relfilenode :delete t))))

(defun test-vacuum-reuses-tid-clears-index-and-vm ()
  (let* ((*heap-fsms* (make-hash-table :test #'eql))
         (*wal-ready* nil)
         (heap-relfilenode 918273655)
         (index-relfilenode 918273656)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (old-key (encode-index-key '(1) '(:int4)))
         (new-key (encode-index-key '(2) '(:int4)))
         (old-tid nil))
    (smgr-forget heap-relfilenode :delete t)
    (smgr-forget index-relfilenode :delete t)
    (clog-set 8200 +clog-committed+)
    (clog-set 8201 +clog-committed+)
    (unwind-protect
         (progn
           (btree-init index-relfilenode)
           (setf old-tid
                 (multiple-value-list
                  (heapam-insert heap-relfilenode columns #(1) 8200 0)))
           (btree-insert index-relfilenode old-key (first old-tid)
                         (second old-tid))
           (with-write-lock
             (let ((page (buf-read heap-relfilenode (first old-tid))))
               (multiple-value-bind (tuple-off flags len)
                   (page-get-lp page (second old-tid))
                 (declare (ignore flags len))
                 (page-set-u32 page (+ tuple-off +heap-tuple-xmax-off+) 8201))
               (buf-write heap-relfilenode (first old-tid) page)))
           (heap-vacuum-relation heap-relfilenode (list index-relfilenode)
                                 8202 8100)
           (check (= (visibilitymap-status heap-relfilenode (first old-tid))
                     (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
                  "VACUUM marks the reclaimed empty page all-visible and all-frozen")
           (multiple-value-bind (off flags len)
               (page-get-lp (buf-read heap-relfilenode (first old-tid))
                            (second old-tid))
             (declare (ignore off len))
             (check (= flags +lp-unused+)
                    "VACUUM makes a deleted tuple TID reusable"))
           (visibilitymap-set-status
            heap-relfilenode (first old-tid)
            (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
           (check (= (visibilitymap-status heap-relfilenode (first old-tid))
                     (logior +vm-all-visible-bit+ +vm-all-frozen-bit+))
                  "fixture marks the reusable page all-visible and all-frozen")
           (multiple-value-bind (new-block new-off)
               (heapam-insert heap-relfilenode columns #(2) 8202 0)
             (btree-insert index-relfilenode new-key new-block new-off)
             (check (equal (list new-block new-off) old-tid)
                    "INSERT reuses the vacuumed heap TID")
             (check (null (btree-search index-relfilenode old-key))
                    "reused TID has no stale entry under its old index key")
             (check (equal (btree-search index-relfilenode new-key)
                           (list (cons new-block new-off)))
                    "new index key points to the reused TID")
             (check (zerop (visibilitymap-status heap-relfilenode new-block))
                    "INSERT clears all-visible and all-frozen VM bits")))
      (smgr-forget heap-relfilenode :delete t)
      (smgr-forget index-relfilenode :delete t))))

(defun test-vacuum-snapshot-horizon ()
  ;; Port the permanent-table horizon case from
  ;; src/test/isolation/specs/horizons.spec. The Lisp executor currently keeps
  ;; snapshots per statement, so register the older snapshot directly here.
  (let* ((*backend* (make-backend))
         (*next-xid* 7303)
         (*active-xids* '())
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*wal-ready* nil)
         (heap-relfilenode 918273651)
         (index-relfilenode 918273652)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (key (encode-index-key '(1) '(:int4)))
         (snapshot nil)
         (registration (make-snapshot-registration))
         (page (page-init (make-page)))
         (offset (page-add-item
                  page (form-heap-tuple #(1) columns 7301 0 0 0 1)))
         (snapshot-active nil)
         (index-cleanup-before-heap-reuse nil))
    ;; The snapshot starts after the insert but before the committed delete.
    (smgr-forget heap-relfilenode :delete t)
    (smgr-forget index-relfilenode :delete t)
    (clog-set 7301 +clog-committed+)
    (unwind-protect
         (progn
           (ensure-directories-exist (relation-file-path heap-relfilenode))
           (check (= (buf-extend heap-relfilenode page) 0)
                  "vacuum horizon fixture creates heap block zero")
           (btree-init index-relfilenode)
           (btree-insert index-relfilenode key 0 offset)
           (setf snapshot-active t
                 snapshot (make-snapshot 1 :register registration)
                 *next-xid* 7304)
           (with-write-lock
             (let ((heap-page (buf-read heap-relfilenode 0)))
               (multiple-value-bind (tuple-off flags len)
                   (page-get-lp heap-page offset)
                 (declare (ignore flags len))
                 (page-set-u32 heap-page
                               (+ tuple-off +heap-tuple-xmax-off+) 7303)
                 (buf-write heap-relfilenode 0 heap-page))))
           (clog-set 7303 +clog-committed+)
           (check (= (snapshot-xmax snapshot) 7303)
                  "snapshot treats the later delete XID as in the future")
           (check (= (oldest-xmin) 7303)
                  "registered old snapshot holds the vacuum removal horizon")
           (heap-vacuum-relation heap-relfilenode (list index-relfilenode)
                                 (oldest-xmin) 7300)
           (multiple-value-bind (off flags len)
               (page-get-lp (buf-read heap-relfilenode 0) offset)
             (declare (ignore off len))
             (check (= flags +lp-normal+)
                    "vacuum preserves a deleted tuple visible to the old snapshot"))
           (let ((tuple (heapam-get-tuple heap-relfilenode 0 offset columns)))
             (check (and tuple
                         (heap-tuple-visible-p
                          (htup-xmin tuple) (htup-xmax tuple) (htup-cid tuple)
                          snapshot nil))
                    "old snapshot can still see the tuple after vacuum")
             (check (equal (btree-search index-relfilenode key)
                           (list (cons 0 offset)))
                    "vacuum retains the tuple's index TID behind the old horizon"))
           (release-snapshot-registration registration)
           (setf snapshot-active nil)
           (check (= (oldest-xmin) 7304)
                  "releasing the old snapshot advances the removal horizon")
           (let ((original-btree-vacuum-delete-tids
                   (symbol-function 'btree-vacuum-delete-tids)))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'btree-vacuum-delete-tids)
                          (lambda (index-relfilenode tids)
                            (funcall original-btree-vacuum-delete-tids
                                     index-relfilenode tids)
                            (multiple-value-bind (off flags len)
                                (page-get-lp (buf-read heap-relfilenode 0) offset)
                              (declare (ignore off len))
                              (setf index-cleanup-before-heap-reuse
                                    (and (null (btree-search
                                                index-relfilenode key))
                                         (= flags +lp-dead+))))))
                    (heap-vacuum-relation heap-relfilenode
                                          (list index-relfilenode)
                                          (oldest-xmin) 7300))
               (setf (symbol-function 'btree-vacuum-delete-tids)
                     original-btree-vacuum-delete-tids)))
           (check index-cleanup-before-heap-reuse
                  "vacuum removes the index TID while its heap slot is still dead")
           (multiple-value-bind (off flags len)
               (page-get-lp (buf-read heap-relfilenode 0) offset)
             (declare (ignore off len))
             (check (= flags +lp-unused+)
                    "vacuum makes the dead heap slot reusable after horizon advance"))
           (check (null (btree-search index-relfilenode key))
                  "vacuum removes dead index TIDs before heap-slot reuse")
           (let* ((fsm (gethash heap-relfilenode *heap-fsms*))
                  (recorded-space (and fsm
                                       (gethash 0 (heap-fsm-page-space fsm))))
                  (page-space
                    (heap-page-insert-capacity (buf-read heap-relfilenode 0))))
             (check (and recorded-space (= recorded-space page-space))
                    "vacuum records reclaimed heap space in the FSM")))
      (when snapshot-active
        (release-snapshot-registration registration))
      (smgr-forget heap-relfilenode :delete t)
      (smgr-forget index-relfilenode :delete t))))

(defun test-btree-duplicate-keys ()
  ;; Insert many entries with one key so the leaves holding them must split.
  ;; Repeatedly updating an indexed value produces exactly this shape: a long
  ;; run of index entries with equal keys spread over several leaves.
  (let* ((rel 987654)
         (key (encode-index-key (list 42) (list :int4)))
         (n 2000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel key 0 i))
    (let ((found (btree-search rel key)))
      (check (= (length found) n)
             "duplicate keys are found across leaf splits")
      (check (= (length (remove-duplicates found :test #'equal)) n)
             "every duplicate-key TID is distinct")
      (check (null (btree-search rel (encode-index-key (list 43) (list :int4))))
             "an absent key is not found")
      (btree-vacuum-delete-tids
       rel (loop for i below (/ n 2) collect (cons 0 i)))
      (let ((remaining (btree-search rel key)))
        (check (= (length remaining) (/ n 2))
               "B-tree VACUUM removes dead TIDs across duplicate-key leaves")
        (check (every (lambda (tid) (>= (cdr tid) (/ n 2))) remaining)
               "B-tree VACUUM preserves every remaining duplicate-key TID")))))

(defun test-btree-mixed-duplicate-keys ()
  ;; Interleave two keys so splits produce equal separators for both, which
  ;; stresses the key-range order that search assumes.
  (let* ((rel 987655)
         (ka (encode-index-key (list 1) (list :int4)))
         (kb (encode-index-key (list 2) (list :int4)))
         (n 3000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel (if (evenp i) ka kb) 0 i))
    (let ((fa (btree-search rel ka))
          (fb (btree-search rel kb)))
      (check (= (length fa) (/ n 2)) "even entries found (mixed keys)")
      (check (= (length fb) (/ n 2)) "odd entries found (mixed keys)")
      (check (every (lambda (tid) (evenp (cdr tid))) fa)
             "key A search returned only key-A TIDs")
      (check (every (lambda (tid) (oddp (cdr tid))) fb)
             "key B search returned only key-B TIDs"))))

(defun test-btree-vacuum-bounded-split-retries ()
  (let* ((rel 918273654)
         (dead-key (encode-index-key '(4242) '(:int4)))
         (pass-count 0)
         (split-pass-count 0)
         (inserted 0))
    (smgr-forget rel :delete t)
    (unwind-protect
         (progn
           (btree-init rel)
           (dotimes (i 600)
             (with-write-lock
               (btree-insert rel (encode-index-key (list i) '(:int4)) 0 i)))
           (with-write-lock
             (btree-insert rel dead-key 90000 1))
           (let ((initial-generation (btree-split-generation rel)))
             (btree-vacuum-delete-tids
              rel '((90000 . 1))
              :after-page-function
              (lambda (index-relfilenode block)
                (declare (ignore index-relfilenode))
                (when (zerop block)
                  (incf pass-count)
                  (let ((generation (btree-split-generation rel)))
                    (dotimes (i 700)
                      (with-write-lock
                        (btree-insert
                         rel (encode-index-key (list (+ 100000 inserted i))
                                               '(:int4))
                         91000 i)))
                    (incf inserted 700)
                    (when (> (btree-split-generation rel) generation)
                      (incf split-pass-count))))))
             (check (= pass-count (1+ +btree-vacuum-max-split-retries+))
                    "B-tree VACUUM bounds retries before its exclusive final pass")
             (check (= split-pass-count pass-count)
                    "each optimistic cleanup pass overlaps a real B-tree split")
             (check (> (btree-split-generation rel) initial-generation)
                    "interleaved inserts advance the split generation")
             (check (null (btree-search rel dead-key))
                    "the final stable pass removes the dead TID")))
      (smgr-forget rel :delete t))
    (check (not (nth-value 1 (gethash rel *btree-split-generations*)))
           "permanently forgetting an index clears its split generation")))

;;;; Resource-vector pressure at the WAL publication boundary.
(in-package #:pg)

(declaim (special *wal-publication-pressure-limits* *wal-publication-pressure-keys*))

(defun wal-pressure-projection-result (pages contributors)
  (let* ((*wal-lock* (sb-thread:make-mutex :name "WAL pressure projection"))
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-page-cache-xids* (make-hash-table :test #'equal))
         (xids (append '(0 1) (loop for xid from 3 below (+ 3 contributors) collect xid)))
         (keys nil)
         (size (+ 12 +blcksz+ 4 (* 8 contributors)))
         (per-record (min +wal-max-page-images-per-record+
                          (floor (- +wal-segment-size+ +wal-header-size+ 4) size)))
         (records (ceiling pages per-record)))
    (dotimes (block pages)
      (let ((key (list 7310 :main block)))
        (push key keys)
        (setf (gethash key *wal-page-cache*)
              (make-wal-dirty-reference :page (unless (evenp block) #()))
              (gethash key *wal-page-cache-xids*) xids)))
    (sb-thread:with-mutex (*wal-lock*)
      (let ((ticket (wal-project-publication-pressure (nreverse keys) nil)))
        (and (= pages (wal-pressure-ticket-page-entries ticket))
             (= pages (wal-pressure-ticket-contributor-entries ticket))
             (= (* pages (+ 2 contributors)) (wal-pressure-ticket-contributor-cells ticket))
             (= records (wal-pressure-ticket-spool-records ticket))
             (= (+ (* pages size) (* 16 records)) (wal-pressure-ticket-spool-bytes ticket))
             (= (ceiling pages 2) (wal-pressure-ticket-disk-references ticket)))))))

(cl-spec:defproperty projected-page-spool-has-independent-format-cost
    ((pages (range integer 0 600)) (contributors (range integer 0 128)))
  (:about wal-project-publication-pressure)
  (:trials (:normal 100))
  (wal-pressure-projection-result pages contributors))

(rove:deftest wal-pressure-projection-matches-chunk-and-segment-boundaries
    (dolist (case '((0 0) (1 0) (512 2) (513 2) (500 4096)))
      (rove:ok (apply #'wal-pressure-projection-result case)))
  (unit-check-property 'projected-page-spool-has-independent-format-cost))

(cl-spec:defproperty each-wal-pressure-component-independently-limits-admission
    ((value (range integer 0 100)) (limit (range integer 0 100)))
  (:about wal-publication-pressure-exceeded-p)
  (:trials (:normal 100))
  (every (lambda (key)
           (let ((limits (copy-list *wal-publication-pressure-limits*)))
             (setf (getf limits key) limit)
             (eql (> value limit)
                  (not (null (wal-publication-pressure-exceeded-p (list key value) limits))))))
         *wal-publication-pressure-keys*))

(rove:deftest wal-pressure-all-components-are-independent
    (unit-check-property 'each-wal-pressure-component-independently-limits-admission))

(defun wal-pressure-test-limits (key limit)
  (let ((limits (copy-list *wal-publication-pressure-limits*)))
    (setf (getf limits key) limit)
    limits))

(rove:deftest wal-pressure-settles-before-the-incoming-batch-starts
    (call-with-queued-clog-test
     (lambda ()
       (let* ((*wal-publication-pressure-limits* (wal-pressure-test-limits :owners 1))
              (flush (symbol-function 'wal-flush-core))
              (first nil) (second nil) (old-end nil) (calls 0))
         (unwind-protect
              (progn
                (sb-thread:with-mutex (*wal-lock*)
                  (setf first (wal-admit-transaction-under-lock 44 nil)
                        old-end *wal-end-lsn*))
                (setf (symbol-function 'wal-flush-core)
                      (lambda ()
                        (assert (= old-end *wal-end-lsn*))
                        (assert (not (gethash 45 *clog-pending-commits*)))
                        (incf calls)
                        (funcall flush)))
                (sb-thread:with-mutex (*wal-lock*)
                  (multiple-value-bind (owner end force-flush-p)
                      (wal-admit-transaction-under-lock 45 nil)
                    (declare (ignore end))
                    (setf second owner)
                    (rove:ok (not force-flush-p))))
                (rove:ok (= 1 calls))
                (rove:ok (eq :released (wal-page-publication-state first)))
                (rove:ok (and (eq second *wal-pending-page-publication*)
                              (eq second *wal-pending-page-publication-tail*))))
           (setf (symbol-function 'wal-flush-core) flush))))))

(rove:deftest wal-pressure-oversized-incoming-owner-requires-synchronous-completion
    (call-with-queued-clog-test
     (lambda ()
       (let ((*wal-publication-pressure-limits* (wal-pressure-test-limits :owners 0)))
         (sb-thread:with-mutex (*wal-lock*)
           (multiple-value-bind (owner end force-flush-p)
               (wal-admit-transaction-under-lock 44 nil)
             (rove:ok force-flush-p)
             (rove:ok (< *wal-durable-lsn* end))
             (rove:ok (wal-pressure-ticket-p (wal-page-publication-pressure-ticket owner)))
             (wal-settle-page-publications-under-lock)
             (rove:ok (eq :released (wal-page-publication-state owner)))))))))

(rove:deftest wal-pressure-observes-unresolved-zero-byte-directory-debt
    (call-with-cleanup-domain-file-test
     (lambda (root lock identity domain)
       (let* ((*query-temp-directory* nil) (*query-temp-instance-directory* root)
                                           (owner (make-private-query-temp-scope-owner nil))
                                           (delete-tree (symbol-function 'uiop:delete-directory-tree))
                                           (directory nil))
         (sb-posix:chmod root #o700)
         (unwind-protect
              (sb-thread:with-mutex (lock)
                (call-with-query-temp-cleanup-domain
                 domain identity
                 (lambda ()
                   (adopt-query-temp-owner-by-domain owner domain)
                   (call-with-query-temp-owner
                    owner
                    (lambda ()
                      (multiple-value-bind (path stream) (open-query-temp-output)
                        (declare (ignore path))
                        (finish-query-temp-stream stream t))))
                   (setf directory (query-temp-cleanup-owner-directory owner)
                         (symbol-function 'uiop:delete-directory-tree)
                         (lambda (path &rest arguments)
                           (if (equal path directory) (error "retained pressure-test directory")
                               (apply delete-tree path arguments))))
                   (close-query-temp-owner owner)
                   (let ((cost (query-temp-cleanup-domain-resource-counts domain)))
                     (rove:ok (= 1 (getf cost :cleanup-owners)))
                     (rove:ok (= 1 (getf cost :private-directories)))
                     (rove:ok (zerop (getf cost :temporary-bytes)))
                     (rove:ok (zerop (getf cost :temporary-paths)))
                     (rove:ok (zerop (getf cost :open-streams)))))))
           (setf (symbol-function 'uiop:delete-directory-tree) delete-tree))))))

(rove:deftest wal-pressure-projection-agrees-with-a-real-two-record-spool
    (call-with-queued-clog-test
     (lambda ()
       (let ((projection nil) (owner nil) (forced nil))
         (dotimes (block 513)
           (let ((page (page-init (make-page))) (*wal-row-page-write-p* t))
             (setf (aref page 100) (mod block 256))
             (wal-buffer-write 7310 block page)))
         (sb-thread:with-mutex (*wal-lock*)
           (setf projection (wal-project-publication-pressure (wal-primary-page-keys) nil))
           (multiple-value-bind (publication end force-flush-p)
               (wal-admit-transaction-under-lock 44 nil)
             (declare (ignore end))
             (setf owner publication forced force-flush-p))
           (rove:ok (not forced))
           (rove:ok (= 2 (logical-spool-count (wal-page-publication-spool owner))))
           (rove:ok (equal (wal-pressure-ticket-footprint projection)
                           (wal-pressure-ticket-footprint (wal-page-publication-pressure-ticket owner))))
           (rove:ok (= 513 (getf (wal-publication-pressure-footprint) :scratch-disk-references)))
           (wal-settle-page-publications-under-lock)
           (rove:ok (every #'zerop (loop for (key value) on (wal-publication-pressure-footprint)
                                         by #'cddr collect value))))
         (rove:ok (= 0 (aref (smgr-read 7310 512) 100)))))))

(rove:deftest wal-pressure-keeps-ticket-charges-until-head-retirement
    (call-with-queued-clog-test
     (lambda ()
       (let ((close (symbol-function 'wal-close-primary-page-spool))
             (failure (make-condition 'simple-error :format-control "pressure retained closed spool"))
             (owner nil))
         (let ((*wal-row-page-write-p* t)) (wal-buffer-write 7310 0 (page-init (make-page))))
         (unwind-protect
              (sb-thread:with-mutex (*wal-lock*)
                (setf owner (wal-admit-transaction-under-lock 44 nil))
                (setf (symbol-function 'wal-close-primary-page-spool)
                      (lambda (spool) (funcall close spool) (error failure)))
                (rove:ok (eq failure (handler-case (wal-settle-page-publications-under-lock)
                                       (error (condition) condition))))
                (rove:ok (logical-spool-closed-p (wal-page-publication-spool owner)))
                (rove:ok (eq :applied (wal-page-publication-state owner)))
                (let ((cost (wal-publication-pressure-footprint)))
                  (rove:ok (= 1 (getf cost :owners)))
                  (rove:ok (= 1 (getf cost :page-map-entries)))
                  (rove:ok (plusp (getf cost :spool-bytes)))))
           (setf (symbol-function 'wal-close-primary-page-spool) close))))))

(rove:deftest wal-pressure-unexpected-file-growth-forces-sync-without-midbatch-drain
    (call-with-queued-clog-test
     (lambda ()
       (let* ((*logical-capture-memory-bytes* (* 4 1024 1024))
              (*wal-publication-pressure-limits* (wal-pressure-test-limits :open-streams 1))
              (append (symbol-function 'wal-primary-append-pages))
              (flush (symbol-function 'wal-flush-core))
              (calls 0) (first nil) (second nil))
         (sb-thread:with-mutex (*wal-lock*)
           (setf first (wal-admit-transaction-under-lock 44 nil)))
         (let ((*wal-row-page-write-p* t))
           (setf (backend-xid *backend*) 45)
           (wal-buffer-write 7310 0 (page-init (make-page))))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-primary-append-pages)
                      (lambda (keys spool)
                        (setf *logical-capture-memory-bytes* 0)
                        (funcall append keys spool))
                      (symbol-function 'wal-flush-core)
                      (lambda () (incf calls) (funcall flush)))
                (sb-thread:with-mutex (*wal-lock*)
                  (multiple-value-bind (owner end force-flush-p)
                      (wal-admit-transaction-under-lock 45 nil)
                    (declare (ignore end))
                    (setf second owner)
                    (rove:ok force-flush-p))
                  (rove:ok (zerop calls))
                  (rove:ok (eq first *wal-pending-page-publication*))
                  (rove:ok (eq second *wal-pending-page-publication-tail*))
                  (wal-settle-page-publications-under-lock)
                  (rove:ok (= 1 calls))))
           (setf (symbol-function 'wal-primary-append-pages) append
                 (symbol-function 'wal-flush-core) flush))))))

(rove:deftest wal-pressure-counts-retained-hash-capacity-with-no-live-entries
    (call-with-queued-clog-test
     (lambda ()
       (let ((*wal-page-cache* (make-hash-table :test #'equal :size 16384)))
         (sb-thread:with-mutex (*wal-lock*)
           (multiple-value-bind (owner end force-flush-p)
               (wal-admit-transaction-under-lock 44 nil)
             (declare (ignore end))
             (rove:ok force-flush-p)
             (rove:ok (zerop (wal-pressure-ticket-page-entries
                              (wal-page-publication-pressure-ticket owner))))
             (rove:ok (>= (wal-pressure-ticket-page-capacity
                           (wal-page-publication-pressure-ticket owner)) 16384))
             (wal-settle-page-publications-under-lock)))))))

(rove:deftest wal-pressure-rejects-invalid-vectors-before-comparison
    (dolist (cost '((:owners -1) (:typo 1) (:owners 1 :owners 2) (:owners)))
      (rove:ok (handler-case (progn (wal-publication-pressure-exceeded-p cost) nil)
                 (wal-input-validation-error () t))))
  (dolist (limits (list nil (list :owners 1) (wal-pressure-test-limits :owners -1)))
    (rove:ok (handler-case (progn (wal-publication-pressure-exceeded-p nil limits) nil)
               (wal-input-validation-error () t)))))

(rove:deftest wal-pressure-observes-a-retained-zero-byte-file
    (call-with-cleanup-domain-file-test
     (lambda (root lock identity domain)
       (declare (ignore root))
       (let ((owner (make-private-query-temp-scope-owner nil))
             (unlink (symbol-function 'unlink-query-temp-file))
             (path nil))
         (unwind-protect
              (sb-thread:with-mutex (lock)
                (call-with-query-temp-cleanup-domain
                 domain identity
                 (lambda ()
                   (adopt-query-temp-owner-by-domain owner domain)
                   (call-with-query-temp-owner
                    owner
                    (lambda ()
                      (multiple-value-bind (file stream) (open-query-temp-output)
                        (setf path file)
                        (finish-query-temp-stream stream t))))
                   (setf (symbol-function 'unlink-query-temp-file)
                         (lambda (file)
                           (if (equal path file) (error "retained zero-byte pressure file")
                               (funcall unlink file))))
                   (close-query-temp-owner owner)
                   (let ((cost (query-temp-cleanup-domain-resource-counts domain)))
                     (rove:ok (= 1 (getf cost :cleanup-owners)))
                     (rove:ok (= 1 (getf cost :temporary-paths)))
                     (rove:ok (zerop (getf cost :temporary-bytes)))
                     (rove:ok (zerop (getf cost :open-streams)))))))
           (setf (symbol-function 'unlink-query-temp-file) unlink))))))

(rove:deftest wal-pressure-predecessor-flush-failure-does-not-start-an-incoming-batch
    (call-with-queued-clog-test
     (lambda ()
       (let* ((*wal-publication-pressure-limits* (wal-pressure-test-limits :owners 1))
              (flush (symbol-function 'wal-flush-core))
              (failure (make-condition 'simple-error :format-control "pressure prefix flush failed"))
              (first nil) (old-end nil))
         (sb-thread:with-mutex (*wal-lock*)
           (setf first (wal-admit-transaction-under-lock 44 nil)
                 old-end *wal-end-lsn*))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-flush-core) (lambda () (error failure)))
                (sb-thread:with-mutex (*wal-lock*)
                  (rove:ok (eq failure (handler-case (wal-admit-transaction-under-lock 45 nil)
                                         (error (condition) condition))))
                  (rove:ok *wal-poisoned*)
                  (rove:ok (= old-end *wal-end-lsn*))
                  (rove:ok (eq first *wal-pending-page-publication*))
                  (rove:ok (eq first *wal-pending-page-publication-tail*))
                  (rove:ok (not (gethash 45 *clog-pending-commits*)))))
           (setf (symbol-function 'wal-flush-core) flush))))))

(rove:deftest wal-pressure-counts-shared-scratch-high-water-separately-from-live-references
    (call-with-queued-clog-test
     (lambda ()
       (let ((*wal-publication-pressure-limits* (wal-pressure-test-limits :temporary-bytes (* 20 1024))))
         (let ((*wal-row-page-write-p* t))
           (wal-buffer-write 7310 0 (page-init (make-page))))
         (let ((first (sb-thread:with-mutex (*wal-lock*) (wal-admit-transaction-under-lock 44 nil))))
           (let ((*wal-row-page-write-p* t))
             (setf (backend-xid *backend*) 45)
             (wal-buffer-write 7310 1 (page-init (make-page))))
           (sb-thread:with-mutex (*wal-lock*)
             (multiple-value-bind (owner end force-flush-p)
                 (wal-admit-transaction-under-lock 45 nil)
               (declare (ignore owner end))
               (rove:ok force-flush-p))
             (rove:ok (eq :released (wal-page-publication-state first)))
             (let ((cost (wal-publication-pressure-footprint)))
               (rove:ok (= 1 (getf cost :scratch-disk-references)))
               (rove:ok (= +blcksz+ (getf cost :scratch-bytes)))
               (rove:ok (> (getf cost :temporary-bytes) (* 20 1024))))
             (wal-settle-page-publications-under-lock)
             (rove:ok (zerop (getf (wal-publication-pressure-footprint) :temporary-bytes)))))))))

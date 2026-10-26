(in-package #:pg-physical-tests)

(defun row-replay-test-record (type &optional (xid 0))
  (pg::make-wal-record :version 7 :type type :xid xid :start 0 :end 32
                       :payload (empty-octets)))

(deftest row-replay-guards-record-types-and-full-xid-boundaries
    (dolist (horizon '(0 3 42 4294967301 18446744073709551615 18446744073709551616))
      (dolist (xid '(3 41 42 43 4294967301 18446744073709551615))
        (ok (eql (>= xid horizon)
                 (not (null (pg::physical-row-replay-allowed-p
                             (row-replay-test-record pg::+wal-record-commit+ xid) horizon)))))))
  (dolist (xid '(0 1 2 4294967296 4294967297 4294967298))
    (ok (not (pg::physical-row-replay-allowed-p
              (row-replay-test-record pg::+wal-record-commit+ xid) 0))))
  (dolist (type (list pg::+wal-record-checkpoint+ pg::+wal-record-catalog+
                      pg::+wal-record-catalog-chunk+ pg::+wal-record-catalog-commit+
                      pg::+wal-record-logical-catalog-event+ pg::+wal-record-maintenance+
                      pg::+wal-record-clog-reset+ pg::+wal-record-end-of-recovery+))
    (ok (not (pg::physical-row-replay-allowed-p (row-replay-test-record type 100) 3))))
  (dolist (type (list pg::+wal-record-batch-start+ pg::+wal-record-pages+
                      pg::+wal-record-logical-changes+ pg::+wal-record-xid-reservation+))
    (ok (pg::physical-row-replay-allowed-p (row-replay-test-record type) 100))))

(deftest row-replay-checks-every-page-contributor-before-apply
    (flet ((image (xids) (pg::make-wal-page-image :xids xids :provenance :row)))
      (ok (pg::physical-row-replay-images-allowed-p (list (image '(60 80)) (image '(100))) 60))
      (dolist (xids '(nil (59 100) (0 100) (4294967296 4294967301)))
        (ok (not (pg::physical-row-replay-images-allowed-p (list (image '(100)) (image xids)) 60))))
      (ok (pg::physical-row-replay-images-allowed-p nil 60))))

(deftest row-replay-preflight-never-synthesizes-missing-or-unknown-storage
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*catalog* (pg::make-catalog))
                                 (database (pg::make-database :name "postgres"))
                                 (table (pg::make-pg-class :oid 900 :relfilenode 900 :name "items"))
                                 (pg::*relation-streams* (make-hash-table :test #'equal)))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
              (gethash "items" (pg::database-tables database)) table)
        (ensure-directories-exist (pg::relation-file-path 900))
        (flet ((image (node block &optional (fork :main))
                 (pg::make-wal-page-image :relfilenode node :fork fork :block block
                                          :page (pg::make-page) :xids '(100))))
          (ok (intake-fails-p (lambda () (pg::physical-row-replay-preflight-images (list (image 900 0))))))
          (ok (not (probe-file (pg::relation-file-path 900))))
          (with-open-file (out (pg::relation-file-path 900)
                               :direction :output :element-type '(unsigned-byte 8))
            (write-sequence (pg::make-page) out))
          (ok (not (intake-fails-p
                    (lambda () (pg::physical-row-replay-preflight-images
                                (list (image 900 0) (image 900 1) (image 900 2)))))))
          (dolist (images (list (list (image 901 0)) (list (image 900 2))
                                (list (image 900 0) (image 900 0))
                                (list (image 900 1 :visibility))))
            (ok (intake-fails-p (lambda () (pg::physical-row-replay-preflight-images images)))))
          (ok (not (probe-file (pg::relation-file-path 901))))
          (ok (not (probe-file (pg::relation-file-path 900 :visibility))))
          (with-open-file (in (pg::relation-file-path 900) :element-type '(unsigned-byte 8))
            (ok (= pg::+blcksz+ (file-length in))))))))

(deftest streaming-clog-replacement-keeps-the-readable-stream-on-open-error
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*clog-stream* nil)
                                 (path (pg::clog-path)) (saved (merge-pathnames "clog-saved" root))
                                 (original nil))
        (unwind-protect
             (progn
               (pg::clog-init)
               (pg::clog-set 3 pg::+clog-committed+ :fsync t)
               (close pg::*clog-stream*)
               (setf original (open path :element-type '(unsigned-byte 8))
                     pg::*clog-stream* original)
               (sb-posix:rename (namestring path) (namestring saved))
               (ok (intake-fails-p #'pg::physical-recovery-open-clog-redo))
               (ok (eq original pg::*clog-stream*))
               (ok (open-stream-p original))
               (ok (= pg::+clog-committed+ (pg::clog-get 3)))
               (ok (not (probe-file path)))
               (sb-posix:rename (namestring saved) (namestring path))
               (pg::physical-recovery-open-clog-redo)
               (ok (not (open-stream-p original)))
               (ok (output-stream-p pg::*clog-stream*))
               (ok (= pg::+clog-committed+ (pg::clog-get 3))))
          (when pg::*clog-stream* (close pg::*clog-stream*))))))

(deftest row-replay-requires-explicit-page-provenance
    (dolist (provenance '(:unknown :row :maintenance))
      (let* ((image (pg::make-wal-page-image :relfilenode 900 :block 0
                                             :page (pg::page-init (pg::make-page))
                                             :xids '(42) :provenance provenance))
             (payload (pg::wal-encode-page-images (list image) 4096))
             (decoded (first (pg::wal-decode-page-images payload 4096 7))))
        (ok (eq provenance (pg::wal-page-image-provenance decoded)))
        (ok (equal '(42) (pg::wal-page-image-xids decoded)))
        (ok (eql (eq provenance :row)
                 (not (null (pg::physical-row-replay-images-allowed-p (list decoded) 3)))))
        (unless (eq provenance :unknown)
          (ok (intake-fails-p (lambda () (pg::wal-decode-page-images payload 4096 4)))))
        (pg::wal-set-u32 payload (+ 4 12 pg::+blcksz+) #x40000001)
        (ok (intake-fails-p (lambda () (pg::wal-decode-page-images payload 4096 7)))))))

(deftest row-page-provenance-never-loses-maintenance-or-legacy-taint
    (let* ((pg::*wal-ready* t)
           (pg::*backend* (pg::make-backend :xid 42))
           (pg::*wal-page-cache* (make-hash-table :test #'equal))
           (pg::*wal-page-cache-xids* (make-hash-table :test #'equal))
           (pg::*wal-page-batch* nil) (pg::*wal-page-batch-xids* nil)
           (key (pg::wal-page-key 900 0 :main)))
      (flet ((stage (xid row-p)
               (setf (pg::backend-xid pg::*backend*) xid)
               (let ((pg::*wal-row-page-write-p* row-p))
                 (pg::wal-buffer-write 900 0 (pg::page-init (pg::make-page)))))
             (image () (first (pg::wal-page-images))))
        (stage 42 t)
        (ok (eq :row (pg::wal-page-image-provenance (image))))
        (stage nil nil)
        (stage 43 t)
        (ok (eq :maintenance (pg::wal-page-image-provenance (image))))
        (ok (equal '(42 43) (sort (pg::wal-page-image-xids (image)) #'<)))
        (pg::with-wal-page-batch (stage 44 t))
        (ok (eq :maintenance (pg::wal-page-image-provenance (image))))
        (clrhash pg::*wal-page-cache*)
        (clrhash pg::*wal-page-cache-xids*)
        (pg::with-wal-page-batch (stage 45 t))
        (ok (eq :row (pg::wal-page-image-provenance (image))))
        (pg::with-wal-page-batch (stage 46 t))
        (ok (eq :row (pg::wal-page-image-provenance (image))))
        (ok (equal '(45 46) (sort (pg::wal-page-image-xids (image)) #'<)))
        (setf (gethash key pg::*wal-page-cache-xids*) '(47))
        (pg::with-wal-page-batch (stage 48 t))
        (ok (eq :maintenance (pg::wal-page-image-provenance (image))))
        (remhash key pg::*wal-page-cache-xids*)
        (stage 49 t)
        (ok (eq :maintenance (pg::wal-page-image-provenance (image)))))))

(cl-spec:defproperty row-page-provenance-survives-native-codec
    ((tag (range integer 0 2)) (xid (range integer 3 18446744073709551615)))
  (:about pg::wal-encode-page-images)
  (:trials (:normal 100))
  (let* ((provenance (nth tag '(:unknown :row :maintenance)))
         (image (pg::make-wal-page-image :relfilenode 900 :block 0
                                         :page (pg::page-init (pg::make-page))
                                         :xids (list xid) :provenance provenance))
         (decoded (first (pg::wal-decode-page-images
                          (pg::wal-encode-page-images (list image) 4096) 4096 7))))
    (and (eq provenance (pg::wal-page-image-provenance decoded))
         (equal (list xid) (pg::wal-page-image-xids decoded)))))

(deftest generated-row-page-provenance-codec
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'row-page-provenance-survives-native-codec :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest row-replay-horizon-retains-all-registered-snapshots
    (let* ((pg::*active-snapshots* 0)
           (pg::*active-snapshot-set* (make-hash-table :test #'eq))
           (first (pg::make-snapshot-registration))
           (second (pg::make-snapshot-registration))
           (low (pg::make-snapshot-struct :xmin 3 :xmax 31))
           (high (pg::make-snapshot-struct :xmin 3 :xmax 67)))
      (unwind-protect
           (progn
             (ok (= 0 (pg::physical-row-replay-reader-horizon)))
             (sb-thread:with-mutex (pg::*xid-lock*)
               (pg::register-active-snapshot low first)
               (pg::register-active-snapshot high second))
             (ok (= 67 (pg::physical-row-replay-reader-horizon)))
             (ok (= 2 pg::*active-snapshots*))
             (ok (eq low (pg::snapshot-registration-snapshot first)))
             (ok (eq high (pg::snapshot-registration-snapshot second)))
             (pg::release-snapshot-registration second)
             (ok (= 31 (pg::physical-row-replay-reader-horizon)))
             (pg::release-snapshot-registration first)
             (ok (= 0 (pg::physical-row-replay-reader-horizon))))
        (pg::release-snapshot-registration first)
        (pg::release-snapshot-registration second)))
  (let ((pg::*active-snapshots* 1)
        (pg::*active-snapshot-set* (make-hash-table :test #'eq)))
    (ok (intake-fails-p #'pg::physical-row-replay-reader-horizon))))

(cl-spec:defproperty row-replay-requires-every-old-snapshot-to-exclude-the-commit
    ((xid (range integer 3 1000000))
     (left (range integer 0 1000000))
     (right (range integer 0 1000000)))
  (:about pg::physical-row-replay-allowed-p)
  (:trials (:normal 200))
  (eql (and (>= xid left) (>= xid right))
       (not (null (pg::physical-row-replay-allowed-p
                   (row-replay-test-record pg::+wal-record-commit+ xid)
                   (max left right))))))

(deftest generated-row-replay-snapshot-exclusion
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property
                  'row-replay-requires-every-old-snapshot-to-exclude-the-commit :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

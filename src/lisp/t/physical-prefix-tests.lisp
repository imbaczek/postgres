;;;; Restartpoint generations preserve authenticated segment prefixes across compaction.
(in-package #:pg-physical-tests)

(defun prefix-test-append-terminal (store start)
  (let ((batch (sample-physical-record start)) (terminal (legacy-checkpoint-record (+ start 32))))
    (pg::physical-store-input store start batch)
    (pg::physical-store-input store (+ start 32) terminal)
    (pg::physical-store-flush store)
    (concatenate '(vector (unsigned-byte 8)) batch terminal)))

(deftest restartpoint-prefix-survives-same-segment-compaction-with-corrupt-pre-floor-spool
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (multiple-value-bind (metadata catalog prefix) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
        (declare (ignore catalog))
        (ok (= 4 (getf metadata :version))) (ok (= 96 (length prefix)))
        (let* ((base-before (lineage-test-bytes (pg::wal-segment-path 0 1 :legacy)))
               (suffix (prefix-test-append-terminal store 96))
               (path (pg::physical-store-segment-path store 0)))
          ;; These bytes are now below receive.control's floor and its validation.
          (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8) :if-exists :overwrite)
            (file-position stream 40) (write-byte 255 stream))
          (setf pg::*last-replay-lsn* 160)
          (pg::physical-store-compact store 160 #'pg::physical-restartpoint-publish)
          (multiple-value-bind (next ignored captured) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
            (declare (ignore ignored))
            (ok (= 160 (getf next :lsn)))
            (ok (equalp captured (concatenate '(vector (unsigned-byte 8)) prefix suffix))))
          (ok (equalp base-before (lineage-test-bytes (pg::wal-segment-path 0 1 :legacy))))))))

(deftest restartpoint-prefix-pointer-ahead-of-floor-remains-a-valid-anchor
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-store-test-hook*
              (lambda (point ignored)
                (declare (ignore ignored))
                (when (eq point :after-restartpoint-publication) (error "stop before floor")))))
        (ok (intake-fails-p
             (lambda () (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)))))
      (pg::physical-store-close store)
      (remf pg::*physical-recovery-state* :restartpoint-lsn)
      (remf pg::*physical-recovery-state* :wal-prefix-lsn)
      (remf pg::*physical-recovery-state* :wal-prefix-bytes)
      (let* ((reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1))
             (pg::*physical-receive-store* reopened))
        (unwind-protect
             (progn
               (ok (= 32 (pg::physical-store-floor-lsn reopened)))
               (ok (= 96 (pg::physical-restartpoint-prepare-storage reopened)))
               (ok (= 96 (getf pg::*physical-recovery-state* :restartpoint-lsn)))
               (let* ((prefix (getf pg::*physical-recovery-state* :wal-prefix-bytes))
                      (suffix (prefix-test-append-terminal reopened 96)))
                 (setf pg::*last-replay-lsn* 160)
                 (pg::physical-store-compact reopened 160 #'pg::physical-restartpoint-publish)
                 (ok (equalp (getf pg::*physical-recovery-state* :wal-prefix-bytes)
                             (concatenate '(vector (unsigned-byte 8)) prefix suffix)))))
          (pg::physical-store-close reopened)))))

(deftest restartpoint-prefix-corruption-is-rejected-before-restoring-catalog
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
      (let* ((pointer (lineage-test-bytes (pg::physical-restartpoint-pointer-path)))
             (path (pg::physical-restartpoint-generation-path 96 (pg::wal-u64 pointer 16)))
             (bytes (lineage-test-bytes path)))
        ;; Keep envelope CRC and pointer hash valid; the bound prefix digest must fail.
        (setf (aref bytes (1- (length bytes))) (logxor 1 (aref bytes (1- (length bytes)))))
        (pg::wal-set-u32 bytes 24 0)
        (pg::wal-set-u32 bytes 24 (pg::wal-crc32 bytes))
        (let ((digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
          (pg::wal-atomic-write-bytes (pg::physical-restartpoint-generation-path 96 digest) bytes)
          (pg::wal-set-u64 pointer 16 digest)
          (pg::wal-set-u32 pointer 24 0)
          (pg::wal-set-u32 pointer 24 (pg::wal-crc32 pointer))
          (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer))
        (pg::wal-atomic-write-bytes (pg::catalog-path) #(17))
        (ok (intake-fails-p (lambda () (pg::physical-restartpoint-prepare-storage store))))
        (ok (equalp #(17) (lineage-test-bytes (pg::catalog-path)))))))

(deftest restartpoint-prefix-legacy-unknowns-recover-at-boundaries-and-later-segments
    (with-physical-store-directory (root)
      (let* ((size pg::+wal-segment-size+) (base (- size 128))
                                           (store (pg::physical-store-open root 123 1 :create t :start-lsn base))
                                           (pg::*physical-recovery-state* (list :system-id 123 :timeline 1 :end base)))
        (unwind-protect
             (progn
               (prefix-test-append-terminal store base)
               (pg::physical-store-compact store (- size 64) (lambda (digest) (declare (ignore digest))))
               (pg::with-physical-store (store)
                 (ok (null (pg::physical-recovery-wal-prefix-under-lock store (- size 64)))))
               (prefix-test-append-terminal store (- size 64))
               (pg::with-physical-store (store)
                 (let ((prefix (pg::physical-recovery-wal-prefix-under-lock store size)))
                   (ok prefix) (ok (zerop (length prefix)))))
               (let ((suffix (prefix-test-append-terminal store size)))
                 (pg::with-physical-store (store)
                   (ok (equalp suffix (pg::physical-recovery-wal-prefix-under-lock store (+ size 64)))))))
          (pg::physical-store-close store)))))

(deftest restartpoint-prefix-codec-distinguishes-known-empty-unavailable-and-legacy
    (with-restartpoint-fixture (root store)
      (declare (ignore root store))
      (let* ((catalog (lineage-test-bytes (pg::catalog-path)))
             (metadata (pg::physical-restartpoint-metadata 77 catalog)))
        (setf (getf metadata :lsn) pg::+wal-segment-size+
              (getf metadata :wal-prefix-digest) #xCBF29CE484222325)
        (let ((bytes (pg::physical-restartpoint-encode metadata catalog (empty-octets))))
          (ok (equalp (empty-octets) (nth-value 2 (pg::physical-restartpoint-decode bytes)))))
        (setf (getf metadata :wal-prefix-digest) nil)
        (ok (null (nth-value 2 (pg::physical-restartpoint-decode
                                (pg::physical-restartpoint-encode metadata catalog)))))
        (setf (getf metadata :version) 3)
        (remf metadata :wal-prefix-digest)
        (multiple-value-bind (decoded ignored prefix)
            (pg::physical-restartpoint-decode (pg::physical-restartpoint-encode metadata catalog))
          (declare (ignore ignored))
          (ok (= 3 (getf decoded :version))) (ok (null prefix))))))

(cl-spec:defproperty restartpoint-embedded-prefix-roundtrips
    ((length (range integer 0 512)) (value (range integer 0 255)))
  (:about pg::physical-restartpoint-encode)
  (:trials (:normal 100))
  (let* ((prefix (make-array length :element-type '(unsigned-byte 8) :initial-element value))
         (metadata (list :version 4 :lsn length
                         :wal-prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 prefix))))
    (multiple-value-bind (decoded catalog restored)
        (pg::physical-restartpoint-decode (pg::physical-restartpoint-encode metadata #(1) prefix))
      (and (equal metadata decoded) (equalp catalog #(1)) (equalp restored prefix)))))

(deftest generated-restartpoint-embedded-prefix-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'restartpoint-embedded-prefix-roundtrips :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

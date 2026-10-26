;;;; History and EOR publication keep pending recovery closed.
(in-package #:pg-physical-tests)

(defun prepare-promotion-child ()
  (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent)
  (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix))

(deftest promotion-history-eor-publication-is-idempotent-and-keeps-parent-control
    (dolist (namespace '(:legacy :timeline))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root store))
         (prepare-promotion-child)
         (let ((control (lineage-test-bytes (pg::wal-control-path)))
               (intent (lineage-test-bytes (pg::physical-recovery-control-path))))
           (loop repeat 2 do
             (ok (= 272 (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))
           (let* ((bytes (lineage-test-bytes (pg::wal-segment-path 0 2 :timeline)))
                  (record (pg::wal-decode-record (subseq bytes 216) 216))
                  (history (pg::timeline-read-history 2))
                  (info (pg::wal-end-of-recovery-record-info record)))
             (ok (= 272 (length bytes)))
             (ok (= 2 (pg::timeline-check-end-of-recovery record 1 history)))
             (ok (= 2 (getf info :wal-level)))
             (ok (= (* 1000000 (- (getf pg::*physical-recovery-state* :state-time) 946684800))
                    (getf info :end-time))))
           (ok (equalp control (lineage-test-bytes (pg::wal-control-path))))
           (ok (equalp intent (lineage-test-bytes (pg::physical-recovery-control-path))))
           (ok (= 1 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
           (ok (equal "57P03" (recovery-state-of #'pg::check-recovery-available)))
           (ok (null pg::*physical-recovery-failed-p*))))
       :namespace namespace)))

(deftest promotion-history-eor-rejects-foreign-history-prefix-and-valid-tail
    (dolist (change '(:history :prefix :valid-tail :valid-unsupported :long-tail :symlink))
      (with-promotion-fixture (root store)
        (declare (ignore root store))
        (prepare-promotion-child)
        (let ((child (pg::wal-segment-path 0 2 :timeline))
              (history (pg::timeline-history-path 2 pg::*pgdata*)))
          (when (member change '(:valid-tail :valid-unsupported))
            (pg::timeline-publish-history-bytes history
                                                (pg::physical-promotion-history-image pg::*physical-recovery-state*)))
          (case change
            (:history (pg::wal-atomic-write-bytes history #(49 9 48 47 68 56 9 102 111 114 101 105 103 110 10)))
            (:prefix (pg::wal-atomic-write-bytes child (make-array 216 :element-type '(unsigned-byte 8) :initial-element 7)))
            (:valid-tail
             (pg::wal-atomic-write-bytes
              child (concatenate '(vector (unsigned-byte 8))
                                 (getf pg::*physical-recovery-state* :wal-prefix-bytes)
                                 (pg::wal-encode-end-of-recovery-record 216 1 3 1 2))))
            (:valid-unsupported
             (let ((record (pg::wal-encode-end-of-recovery-record 216 1 2 1 2)))
               (pg::wal-set-u16 record 4 99)
               (pg::wal-set-u16 record 6 99)
               (pg::wal-set-u32 record 28 0)
               (pg::wal-set-u32 record 28 (pg::wal-crc32 record))
               (pg::wal-atomic-write-bytes
                child (concatenate '(vector (unsigned-byte 8))
                                   (getf pg::*physical-recovery-state* :wal-prefix-bytes) record))))
            (:long-tail
             (pg::wal-atomic-write-bytes
              child (concatenate '(vector (unsigned-byte 8))
                                 (getf pg::*physical-recovery-state* :wal-prefix-bytes)
                                 (make-array 57 :element-type '(unsigned-byte 8) :initial-element 0))))
            (:symlink (delete-file child) (sb-posix:symlink "/missing-eor-child" (namestring child))))
          (let ((before (unless (eq change :symlink) (lineage-test-bytes child))))
            (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))
            (ok (null pg::*physical-recovery-failed-p*))
            (when before (ok (equalp before (lineage-test-bytes child)))))))))

(deftest promotion-eor-rotates-after-a-real-complete-maintenance-prefix
    (with-promotion-fixture (root store)
      (declare (ignore root))
      (let* ((start 216) (count (floor (- pg::+wal-segment-size+ start) 64))
                         (bytes (make-array (* count 64) :element-type '(unsigned-byte 8)))
                         (fork (+ start (length bytes))))
        (dotimes (index count)
          (let ((lsn (+ start (* index 64))) (offset (* index 64)))
            (replace bytes (sample-physical-record lsn) :start1 offset)
            (replace bytes (pg::wal-encode-record pg::+wal-record-maintenance+ 0 (+ lsn 32) (empty-octets))
                     :start1 (+ offset 32))))
        (pg::physical-store-input store start bytes)
        (pg::physical-store-flush store)
        (setf pg::*last-replay-lsn* fork
              (getf pg::*physical-recovery-state* :required-applied) fork
              (getf (getf pg::*physical-recovery-state* :source-reservation) :through) fork)
        (setf pg::*physical-recovery-state*
              (pg::physical-recovery-write-control pg::*physical-recovery-state* 2 fork 50))
        (pg::physical-store-compact store fork #'pg::physical-restartpoint-publish)
        (prepare-promotion-child)
        (let ((target (pg::wal-segment-path 1 2 :timeline)))
          ;; The selected pending child may have unclaimed orphan bytes in its next file.
          (pg::wal-atomic-write-bytes target #(9 8 7))
          (let ((pg::*physical-promotion-test-hook*
                  (lambda (point) (when (eq point :after-history) (throw 'rotated-history :paused)))))
            (ok (eq :paused (catch 'rotated-history
                              (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal)))))
          (ok pg::*physical-recovery-failed-p*)
          (ok (zerop (length (lineage-test-bytes target))))
          ;; An isolated recovery binding models a fresh owning process.
          (let ((pg::*physical-recovery-failed-p* nil))
            (loop repeat 2 do
              (ok (= (+ pg::+wal-segment-size+ 56)
                     (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))
            (ok (equalp (getf pg::*physical-recovery-state* :wal-prefix-bytes)
                        (lineage-test-bytes (pg::wal-segment-path 0 2 :timeline))))
            (ok (= 56 (length (lineage-test-bytes target))))
            (ok (= pg::+wal-segment-size+
                   (pg::wal-record-start
                    (pg::wal-decode-record (lineage-test-bytes target) pg::+wal-segment-size+)))))))))

(deftest promotion-history-eor-errors-and-nonlocal-exits-fence
    (dolist (point '(:before-history :after-history :before-eor-write :after-eor-write
                     :after-eor-fsync :after-eor-directory-fsync))
      (dolist (nonlocal '(nil t))
        (with-promotion-fixture (root store)
          (declare (ignore root store))
          (prepare-promotion-child)
          (let ((pg::*physical-promotion-test-hook*
                  (lambda (seen) (when (eq point seen)
                                   (if nonlocal (throw 'abort-eor :aborted)
                                       (error "injected EOR publication failure"))))))
            (if nonlocal
                (ok (eq :aborted (catch 'abort-eor
                                   (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))
                (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))))
          (ok pg::*physical-recovery-failed-p*)
          (ok (= 3 (getf (pg::physical-recovery-read-control pg::*pgdata*) :phase)))
          (let ((pg::*physical-recovery-failed-p* nil))
            (ok (= 272 (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))))))

(deftest promotion-history-install-failures-remain-fenced-and-resync-on-recovery
    (dolist (point '(:before-file-fsync :after-file-fsync :after-install :after-directory-fsync))
      (dolist (nonlocal '(nil t))
        (with-promotion-fixture (root store)
          (declare (ignore root store))
          (prepare-promotion-child)
          (let ((pg::*timeline-history-test-hook*
                  (lambda (seen) (when (eq point seen)
                                   (if nonlocal (throw 'abort-history :aborted)
                                       (error "injected history failure"))))))
            (if nonlocal
                (ok (eq :aborted (catch 'abort-history
                                   (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))
                (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))))
          (ok pg::*physical-recovery-failed-p*)
          (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))
          ;; A new fixture binding models recovery; production cannot clear this fence.
          (let ((pg::*physical-recovery-failed-p* nil))
            (ok (= 272 (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))))))))

(cl-spec:defproperty owned-incomplete-eor-is-reconstructed-from-persisted-intent
    ((length (range integer 0 56)) (byte (range integer 0 255)))
  (:about pg::physical-promotion-publish-wal)
  (:trials (:normal 50))
  (with-promotion-fixture (root store)
    (declare (ignore root store))
    (prepare-promotion-child)
    (let ((pg::*physical-promotion-test-hook*
            (lambda (point) (when (eq point :after-history) (throw 'pause-eor nil)))))
      (catch 'pause-eor (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal)))
    (let ((child (pg::wal-segment-path 0 2 :timeline))
          (pg::*physical-recovery-failed-p* nil))
      (pg::wal-atomic-write-bytes
       child (concatenate '(vector (unsigned-byte 8))
                          (getf pg::*physical-recovery-state* :wal-prefix-bytes)
                          (make-array length :element-type '(unsigned-byte 8) :initial-element byte)))
      (and (= 272 (call-with-promotion-exclusion #'pg::physical-promotion-publish-wal))
           (let ((info (pg::wal-end-of-recovery-record-info
                        (pg::wal-decode-record (subseq (lineage-test-bytes child) 216) 216))))
             (and (= 2 (getf info :timeline)) (= 1 (getf info :prev-timeline))))
           (= 1 pg::*wal-insert-timeline*) (not pg::*physical-recovery-failed-p*)))))

(deftest generated-owned-eor-reconstruction
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'owned-incomplete-eor-is-reconstructed-from-persisted-intent :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

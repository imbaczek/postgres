(in-package #:pg-physical-tests)

(deftest receive-compaction-seals-and-coalesces-authenticated-prefixes
    (with-retained-root-registry
        (with-restartpoint-fixture (root store)
          (declare (ignore root))
          (setf (pg::physical-store-retained-identity store) (pg::physical-recovery-retained-identity))
          (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
          (let ((old (pg::physical-store-retained-root store))
                (identity (pg::physical-store-retained-identity store)))
            (ok (typep old '(unsigned-byte 64)))
            (prefix-test-append-terminal store 96)
            (with-open-file (stream (pg::physical-store-segment-path store 0) :direction :io
                                                                              :element-type '(unsigned-byte 8) :if-exists :overwrite)
              (file-position stream 40) (write-byte 255 stream))
            (setf pg::*last-replay-lsn* 160)
            (pg::physical-store-compact store 160 #'pg::physical-restartpoint-publish)
            (let* ((id (pg::physical-store-retained-root store))
                   (archive (pg::physical-store-retained-directory store))
                   (history (pg::timeline-parse-history 1 ""))
                   (index (pg::physical-retained-inventory-load archive id identity history))
                   (entries (getf index :entries)))
              (ng (eql old id))
              (ok (= 1 (length entries)))
              (ok (= 32 (getf (third (first entries)) :start)))
              (ok (= 160 (getf (third (first entries)) :end)))
              (multiple-value-bind (metadata bytes)
                  (pg::physical-retained-extent-decode
                   (pg::physical-retained-object-read archive :extent (first (first entries))
                                                      pg::+physical-retained-image-limit+))
                (declare (ignore metadata))
                (ok (equalp bytes (getf pg::*physical-recovery-state* :wal-prefix-bytes))))
              (ok (pg::physical-retained-root-protected-p archive :inventory id))
              (ok (pg::physical-retained-root-protected-p archive :extent (first (first entries)))))
            (pg::physical-store-close store)
            (let ((reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1
                                                     :retained-identity identity)))
              (unwind-protect
                   (ok (= 160 (pg::physical-store-floor-lsn reopened)))
                (pg::physical-store-close reopened)))))))

(deftest receive-sealing-publication-exits-reconcile-the-durable-control
    (dolist (cut '(:after-retained-inventory-publication :after-restartpoint-publication
                   :before-control-rename :after-control-rename :after-control-directory-fsync
                   :after-retained-root-handoff :after-floor-publication))
      (with-retained-root-registry
          (with-restartpoint-fixture (root store)
            (declare (ignore root))
            (let* ((identity (pg::physical-recovery-retained-identity))
                   (selected (member cut '(:after-control-rename :after-control-directory-fsync :after-retained-root-handoff :after-floor-publication)))
                   (pg::*physical-store-test-hook*
                     (lambda (point ignored)
                       (declare (ignore ignored))
                       (when (eq cut point) (error "stop at ~s" cut)))))
              (setf (pg::physical-store-retained-identity store) identity)
              (ok (signals (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish) 'error))
              (ok (pg::physical-store-failed-p store))
              (pg::physical-store-close store)
              (let* ((pg::*physical-store-test-hook* nil)
                     (reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1
                                                        :retained-identity identity)))
                (unwind-protect
                     (progn
                       (ok (= (if selected 96 32) (pg::physical-store-floor-lsn reopened)))
                       (ok (if selected (typep (pg::physical-store-retained-root reopened) '(unsigned-byte 64))
                               (null (pg::physical-store-retained-root reopened))))
                       (ok (zerop (hash-table-count pg::*physical-retained-root-holds*)))
                       (ok (zerop (hash-table-count pg::*physical-retained-root-updates*)))
                       (let ((pg::*physical-receive-store* reopened))
                         (pg::physical-store-compact reopened 96 #'pg::physical-restartpoint-publish))
                       (ok (= 96 (pg::physical-store-floor-lsn reopened))))
                  (pg::physical-store-close reopened))))))))

(deftest receive-sealing-rejects-a-wrong-control-tuple-with-the-correct-root
    (with-retained-root-registry
        (with-restartpoint-fixture (root store)
          (declare (ignore root))
          (let* ((identity (pg::physical-recovery-retained-identity))
                 (original nil) (candidate nil)
                 (pg::*physical-store-test-hook*
                   (lambda (point active)
                     (when (eq point :after-control-write)
                       (let ((bytes (pg::recovery-read-bytes (pg::physical-store-path active "receive.control"))))
                         (setf original (copy-seq bytes) candidate (pg::wal-u64 bytes 76))
                         (pg::wal-set-u64 bytes 56 (logxor 1 (pg::wal-u64 bytes 56)))
                         (pg::wal-set-u64 bytes 40 (pg::wal-u64 bytes 56))
                         (pg::wal-set-u32 bytes 92 0)
                         (pg::wal-set-u32 bytes 92 (pg::wal-crc32 bytes))
                         (pg::wal-atomic-write-bytes (pg::physical-store-path active "receive.control") bytes))))))
            (setf (pg::physical-store-retained-identity store) identity)
            (ok (signals (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish) 'error))
            (ok (pg::physical-store-failed-p store))
            (ok (pg::physical-retained-root-protected-p (pg::physical-store-retained-directory store)
                                                        :inventory candidate))
            (pg::wal-atomic-write-bytes (pg::physical-store-path store "receive.control") original)
            (pg::physical-store-close store)
            (let ((pg::*physical-store-test-hook* nil)
                  (reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1
                                                     :retained-identity identity)))
              (unwind-protect
                   (progn
                     (ok (= candidate (pg::physical-store-retained-root reopened)))
                     (ok (= 96 (pg::physical-store-floor-lsn reopened)))
                     (ok (zerop (hash-table-count pg::*physical-retained-root-holds*))))
                (pg::physical-store-close reopened)))))))

(deftest receive-sealing-fences-a-nonlocal-exit-before-control-selection
    (with-retained-root-registry
        (with-restartpoint-fixture (root store)
          (declare (ignore root))
          (setf (pg::physical-store-retained-identity store) (pg::physical-recovery-retained-identity))
          (ok (eq :left
                  (catch 'leave
                    (pg::physical-store-compact store 96
                                                (lambda (digest) (declare (ignore digest)) (throw 'leave :left))))))
          (ok (pg::physical-store-failed-p store))
          (ok (signals (pg::physical-store-flush store) 'error))
          (pg::physical-store-close store)
          (let ((reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1
                                                   :retained-identity (pg::physical-recovery-retained-identity))))
            (unwind-protect
                 (progn
                   (ok (= 32 (pg::physical-store-floor-lsn reopened)))
                   (ok (zerop (hash-table-count pg::*physical-retained-root-updates*))))
              (pg::physical-store-close reopened))))))

(deftest receive-sealing-keeps-unflushed-input-out-of-its-root
    (with-retained-root-registry
        (with-restartpoint-fixture (root store)
          (declare (ignore root))
          (let ((identity (pg::physical-recovery-retained-identity))
                (partial (sample-physical-record 128)))
            (setf (pg::physical-store-retained-identity store) identity)
            (pg::physical-store-input store 96 (sample-physical-record 96))
            (pg::physical-store-input store 128 (subseq partial 0 10))
            (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
            (ok (= 96 (pg::physical-store-flushed-lsn store)))
            (ok (= 128 (pg::physical-store-written-lsn store)))
            (ok (= 10 (pg::physical-wal-decoder-used (pg::physical-store-decoder store))))
            (let* ((index (pg::physical-retained-inventory-load
                           (pg::physical-store-retained-directory store) (pg::physical-store-retained-root store)
                           identity (pg::timeline-parse-history 1 "")))
                   (last (third (car (last (getf index :entries))))))
              (ok (= 96 (getf last :end))))
            (pg::physical-store-input store 138 (subseq partial 10))
            (pg::physical-store-flush store)
            (ok (= 160 (pg::physical-store-flushed-lsn store)))))))

(deftest receive-sealing-preserves-rotation-and-end-of-recovery-proof
    (with-retained-root-registry
        (with-physical-store-directory (root)
          (let* ((segment pg::+wal-segment-size+) (fork (- segment 40))
                                                  (identity (list :system-id 123 :base-timeline 1 :base 0 :manifest-digest 77))
                                                  (store (pg::physical-store-open root 123 1 :create t :retained-identity identity))
                                                  (eor (store-history-eor fork 2 1))
                                                  (parent (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0
                                                                                 (make-array (- fork pg::+wal-header-size+) :element-type '(unsigned-byte 8)
                                                                                                                            :initial-element 0))))
            (unwind-protect
                 (progn
                   (pg::physical-store-input store 0 parent)
                   (pg::physical-store-flush store)
                   (pg::physical-store-compact store fork (lambda (digest) (declare (ignore digest))))
                   (pg::physical-store-select-history store 2 (store-history-text 1 fork) eor)
                   (pg::physical-store-input store segment eor)
                   (pg::physical-store-input store (+ segment 56) (sample-physical-record (+ segment 56)))
                   (pg::physical-store-flush store)
                   (pg::physical-store-compact store (+ segment 56) (lambda (digest) (declare (ignore digest))))
                   (ng (probe-file (pg::physical-store-segment-path store 0 1)))
                   (let* ((history (getf (pg::physical-store-selection store) :history))
                          (index (pg::physical-retained-inventory-load
                                  (pg::physical-store-retained-directory store) (pg::physical-store-retained-root store)
                                  identity history))
                          (last (third (car (last (getf index :entries))))))
                     (ok (= 2 (length (getf index :entries))))
                     (ok (= fork (getf last :start)))
                     (ok (= (+ segment 56) (getf last :end)))
                     (ok (= 2 (getf last :file-timeline)))
                     (ok (= 1 (length (pg::physical-retained-inventory-plan
                                       (pg::physical-store-retained-directory store) (pg::physical-store-retained-root store)
                                       identity history fork (+ segment 56)))))))
              (pg::physical-store-close store))
            (let ((reopened (pg::physical-store-open root 123 1 :retained-identity identity)))
              (unwind-protect
                   (progn
                     (ok (= (+ segment 56) (pg::physical-store-floor-lsn reopened)))
                     (ok (= (+ segment 88) (pg::physical-store-flushed-lsn reopened))))
                (pg::physical-store-close reopened)))))))

(defun seal-test-append-segments (store count)
  (let ((start (pg::physical-store-flushed-lsn store)))
    (loop repeat count do
      (let* ((end (* (1+ (floor start pg::+wal-segment-size+)) pg::+wal-segment-size+))
             (raw (pg::wal-encode-record
                   pg::+wal-record-batch-start+ 0 start
                   (make-array (- end start pg::+wal-header-size+) :element-type '(unsigned-byte 8)
                                                                   :initial-element 0))))
        (pg::physical-store-input store start raw)
        (setf start end)))
    (pg::physical-store-input store start (legacy-checkpoint-record start))
    (pg::physical-store-flush store)
    (+ start 32)))

(deftest receive-sealing-retries-restartpoints-ahead-in-the-same-and-later-segment
    (dolist (segments '(0 1))
      (with-retained-root-registry
          (with-restartpoint-fixture (root store)
            (declare (ignore root))
            (let ((identity (pg::physical-recovery-retained-identity)))
              (setf (pg::physical-store-retained-identity store) identity)
              (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
              (let ((cursor (seal-test-append-segments store segments)))
                (setf pg::*last-replay-lsn* cursor)
                (let ((pg::*physical-store-test-hook*
                        (lambda (point ignored)
                          (declare (ignore ignored))
                          (when (eq point :after-restartpoint-publication) (error "RP ahead")))))
                  (ok (signals (pg::physical-store-compact store cursor #'pg::physical-restartpoint-publish) 'error)))
                (with-open-file (stream (pg::physical-store-segment-path store 0) :direction :io
                                                                                  :element-type '(unsigned-byte 8) :if-exists :overwrite)
                  (file-position stream 40) (write-byte 255 stream))
                (pg::physical-store-close store)
                (remf pg::*physical-recovery-state* :wal-prefix-lsn)
                (remf pg::*physical-recovery-state* :wal-prefix-bytes)
                (remf pg::*physical-recovery-state* :restartpoint-lsn)
                (let* ((reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1
                                                          :retained-identity identity))
                       (pg::*physical-receive-store* reopened))
                  (unwind-protect
                       (progn
                         (ok (= 96 (pg::physical-store-floor-lsn reopened)))
                         (ok (= cursor (pg::physical-restartpoint-prepare-storage reopened)))
                         (ok (= (mod cursor pg::+wal-segment-size+)
                                (length (getf pg::*physical-recovery-state* :wal-prefix-bytes))))
                         (pg::physical-store-compact reopened cursor #'pg::physical-restartpoint-publish)
                         (ok (= cursor (pg::physical-store-floor-lsn reopened)))
                         (if (plusp segments)
                             (ng (probe-file (pg::physical-store-segment-path reopened 0)))
                             (ok (probe-file (pg::physical-store-segment-path reopened 0)))))
                    (pg::physical-store-close reopened)))))))))

(defun physical-seal-crash-at (marker token)
  (multiple-value-bind (output errors status)
      (uiop:run-program
       (list (or (uiop:getenv "SBCL") "sbcl") "--noinform" "--no-sysinit" "--no-userinit" "--script"
             (namestring (asdf:system-relative-pathname "pg" "t/physical-seal-crash-child.lisp"))
             (namestring marker) token (namestring (asdf:system-source-directory "cl-spec")))
       :output :string :error-output :string :ignore-error-status t)
    (ok (= 137 status) (format nil "~a status ~a ~a ~a" token status output errors)))
  (let* ((payload (pg::recovery-parse-manifest-text
                   (pg::bytes->string (pg::read-bounded-regular-file marker 4096))))
         (root (pathname (getf payload :directory)))
         (selected (member token '("AFTER-CONTROL-RENAME" "AFTER-CONTROL-DIRECTORY-FSYNC" "AFTER-RETAINED-ROOT-HANDOFF" "AFTER-FLOOR-PUBLICATION" "AFTER-OBSOLETE-SEGMENT-DELETE-EACH") :test #'string=))
         (identity (list :system-id 123 :base-timeline 1 :base 32 :manifest-digest 77))
         (partial (string= token "AFTER-OBSOLETE-SEGMENT-DELETE-EACH"))
         (cursor (if partial (+ (* 2 pg::+wal-segment-size+) 32) 96)))
    (unwind-protect
         (progn
           (ok (equal token (getf payload :point)))
           (ok (plusp (getf payload :pid)))
           (ok (= (if (string= token "STAGE-AFTER-EXTENT-PUBLICATION") 1 2) (getf payload :root-writes)))
           (when partial
             (ok (= 1 (count-if #'probe-file
                                (loop for number below 2
                                      collect (merge-pathnames (format nil "spool/~16,'0X.wal" number) root))))))
           (let ((store (pg::physical-store-open (merge-pathnames "spool/" root) 123 1 :retained-identity identity)))
             (unwind-protect
                  (progn
                    (ok (= (if selected cursor 32) (pg::physical-store-floor-lsn store)))
                    (ok (= cursor (pg::physical-store-flushed-lsn store)))
                    (ok (if selected (typep (pg::physical-store-retained-root store) '(unsigned-byte 64))
                            (null (pg::physical-store-retained-root store))))
                    (when selected
                      (let ((archive (pg::physical-store-retained-directory store))
                            (id (pg::physical-store-retained-root store)))
                        (ok (typep id '(unsigned-byte 64)))
                        (ok (pg::physical-retained-root-protected-p archive :inventory id))
                        (ok (= (if partial 3 1) (length (pg::physical-retained-inventory-plan
                                                         archive id identity (pg::timeline-parse-history 1 "") 32 cursor)))))))
               (pg::physical-store-close store))))
      (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore))))

(deftest receive-sealing-sigkill-durability-boundaries
    (dolist (token '("AFTER-RETAINED-INVENTORY-PUBLICATION" "RP-AFTER-GENERATION" "RP-AFTER-POINTER"
                     "AFTER-RESTARTPOINT-PUBLICATION" "BEFORE-CONTROL-RENAME" "AFTER-CONTROL-RENAME"
                     "AFTER-CONTROL-DIRECTORY-FSYNC" "AFTER-RETAINED-ROOT-HANDOFF" "AFTER-FLOOR-PUBLICATION"
                     "STAGE-AFTER-EXTENT-PUBLICATION" "AFTER-OBSOLETE-SEGMENT-DELETE-EACH"))
      (with-retained-root-registry
          (with-physical-store-directory (root)
            (physical-seal-crash-at (merge-pathnames "seal-crash.marker" root) token)))))

(deftest retained-staging-reads-extent-objects-linearly
    (dolist (count '(16 64))
      (with-retained-root-registry
          (with-physical-store-directory (root)
            (let* ((identity (list :system-id 123 :base-timeline 1 :base 0 :manifest-digest 77))
                   (digest #xCBF29CE484222325)
                   (prefix (make-array 0 :element-type '(unsigned-byte 8)))
                   (owner (gensym "STAGING"))
                   (original (symbol-function 'pg::physical-retained-object-read))
                   (reads 0) (root-writes 0)
                   (pg::*physical-retained-inventory-test-hook*
                     (lambda (point id)
                       (when (equal point '(:extent :before-file-fsync))
                         (ok (pg::physical-retained-root-protected-under-lock root :extent id)))
                       (when (eq point :after-inventory-publication) (incf root-writes)))))
              (unwind-protect
                   (progn
                     (setf (symbol-function 'pg::physical-retained-object-read)
                           (lambda (directory kind id maximum)
                             (when (eq kind :extent) (incf reads))
                             (funcall original directory kind id maximum)))
                     (pg::call-with-physical-retained-root-update
                      (lambda (id hold)
                        (declare (ignore id))
                        (let ((entries nil))
                          (dotimes (number count)
                            (let* ((start (* number 32)) (raw (sample-physical-record start))
                                                         (before digest))
                              (setf digest (pg::physical-store-update-digest digest raw)
                                    prefix (concatenate '(vector (unsigned-byte 8)) prefix raw))
                              (push (pg::physical-retained-root-stage-image
                                     hold 1 ""
                                     (pg::physical-retained-extent-encode
                                      (list :version 1 :system-id 123 :base-timeline 1 :base 0 :manifest-digest 77
                                            :history-target 1 :history-digest nil :history-text ""
                                            :start start :end (+ start 32) :start-digest before :end-digest digest
                                            :segment 0 :file-timeline 1
                                            :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 prefix))
                                      prefix))
                                    entries)))
                          (retained-root-save-control root
                                                      (pg::physical-retained-root-publish-staged-entries
                                                       hold 1 "" (nreverse entries)))))
                      owner root identity 1 "" nil :read-selected (lambda () (retained-root-read-control root)))
                     (ok (<= reads (* 2 count)) (format nil "~d extents required ~d file reads" count reads))
                     (ok (= 2 root-writes)))
                (setf (symbol-function 'pg::physical-retained-object-read) original)))))))

(deftest retained-staged-root-requires-reservation-and-rechecks-image-metadata
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let* ((foreign (pg::physical-retained-inventory-publish root identity 2 text images))
                  (entries (getf (pg::physical-retained-inventory-read-index root foreign identity history) :entries))
                  (owner (gensym "STAGING")))
             (pg::call-with-physical-retained-root-update
              (lambda (id hold)
                (declare (ignore id))
                (ok (signals (pg::physical-retained-root-publish-staged-entries hold 2 text entries) 'error))
                (let* ((entry (pg::physical-retained-root-stage-image hold 2 text (first images)))
                       (bad (copy-tree entry)))
                  (setf (getf (third bad) :end-digest) (logxor 1 (getf (third bad) :end-digest)))
                  (ok (signals (pg::physical-retained-root-publish-staged-entries hold 2 text (list bad)) 'error))
                  (retained-root-save-control
                   root (pg::physical-retained-root-publish-staged-entries hold 2 text (list entry)))))
              owner root identity 2 text nil :read-selected (lambda () (retained-root-read-control root))))))))

(deftest receive-compaction-cleanup-fences-before-a-second-nonlocal-interrupt
    (with-retained-root-registry
        (with-restartpoint-fixture (root store)
          (declare (ignore root))
          (let* ((identity (pg::physical-recovery-retained-identity))
                 (first-tag (gensym "FIRST")) (second-tag (gensym "SECOND")) (masked nil)
                 (pg::*physical-store-test-hook*
                   (lambda (point ignored)
                     (declare (ignore ignored))
                     (case point
                       (:after-control-rename
                        (sb-thread:interrupt-thread sb-thread:*current-thread*
                                                    (lambda () (throw first-tag :first))))
                       (:before-compaction-cleanup
                        (setf masked (not sb-sys:*interrupts-enabled*))
                        (sb-thread:interrupt-thread sb-thread:*current-thread*
                                                    (lambda () (throw second-tag :second))))))))
            (setf (pg::physical-store-retained-identity store) identity)
            (ok (eq :second (catch second-tag
                              (catch first-tag (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)))))
            (ok masked)
            (ok (pg::physical-store-failed-p store))
            (ok (signals (pg::physical-store-check-live store) 'error))
            (pg::physical-store-close store)
            (let* ((pg::*physical-store-test-hook* nil)
                   (reopened (pg::physical-store-open (pg::physical-store-directory store) 123 1
                                                      :retained-identity identity)))
              (unwind-protect
                   (progn
                     (ok (= 96 (pg::physical-store-floor-lsn reopened)))
                     (ok (zerop (hash-table-count pg::*physical-retained-root-holds*))))
                (pg::physical-store-close reopened)))))))

(defun seal-test-metadata-entry (number)
  (list number 64
        (list :version 1 :system-id 123 :base-timeline 1 :base 0 :manifest-digest 77
              :history-target 1 :history-digest nil :history-text (format nil "# λ ~a~%" (code-char 92))
              :start (* number 32) :end (* (1+ number) 32)
              :start-digest #xCBF29CE484222325 :end-digest #xCBF29CE484222325
              :segment 0 :file-timeline 1 :prefix-digest 0)))

(deftest retained-sealing-metadata-accounting-is-exact-and-linear
    (let ((visits nil))
      (dolist (count '(16 64))
        (let* ((identity (list :system-id 123 :base-timeline 1 :base 0 :manifest-digest 77))
               (store (pg::%make-physical-store :retained-identity identity))
               (seal (pg::%make-physical-retained-seal :store store :target 1 :text ""))
               (original (symbol-function 'pg::physical-retained-inventory-check-text-budget))
               (nodes 0))
          (labels ((size (value)
                     (cond ((consp value) (1+ (loop for item in value sum (size item))))
                           ((stringp value) (1+ (length value)))
                           (t 1)))
                   (check ()
                     (let ((payload (list :version 1 :identity identity :history-target 1 :history-text ""
                                          :entries (reverse (pg::physical-retained-seal-entries seal)))))
                       (ok (= (+ (pg::physical-retained-seal-header-bytes seal)
                                 (pg::physical-retained-seal-entry-bytes seal)
                                 (pg::physical-retained-seal-entry-count seal) 1)
                              (pg::physical-retained-inventory-check-text-budget payload))))))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-retained-inventory-check-text-budget)
                         (lambda (payload &optional (maximum (- pg::+physical-retained-inventory-limit+ 16)))
                           (incf nodes (size payload))
                           (funcall original payload maximum)))
                   (pg::physical-retained-seal-initialize-budget seal)
                   (dotimes (number count)
                     (let ((entry (seal-test-metadata-entry number)))
                       (multiple-value-bind (new-count bytes) (pg::physical-retained-seal-entry-budget seal entry nil)
                         (setf (pg::physical-retained-seal-entry-count seal) new-count
                               (pg::physical-retained-seal-entry-bytes seal) bytes)
                         (push entry (pg::physical-retained-seal-entries seal)))))
                   (check)
                   (let* ((old (first (pg::physical-retained-seal-entries seal)))
                          (replacement (copy-tree old)))
                     (setf (first replacement) (+ 1000 count)
                           (getf (third replacement) :history-text) (format nil "# replacement λ~%"))
                     (multiple-value-bind (new-count bytes) (pg::physical-retained-seal-entry-budget seal replacement old)
                       (setf (pg::physical-retained-seal-entry-count seal) new-count
                             (pg::physical-retained-seal-entry-bytes seal) bytes
                             (first (pg::physical-retained-seal-entries seal)) replacement)))
                   (check)
                   (push nodes visits))
              (setf (symbol-function 'pg::physical-retained-inventory-check-text-budget) original)))))
      (ok (<= (first visits) (* 5 (second visits))) (format nil "64/16 entry visit counts: ~s" visits))))

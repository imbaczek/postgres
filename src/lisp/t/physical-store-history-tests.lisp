(in-package #:pg-physical-tests)

(defun store-history-text (parent fork &optional prefix)
  (concatenate 'string prefix
               (format nil "~d~c~X/~8,'0X~creceive probe~%" parent #\Tab
                       (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab)))

(defun store-history-eor (fork child parent)
  (pg::wal-encode-end-of-recovery-record
   (if (<= (+ (mod fork pg::+wal-segment-size+) pg::+wal-end-of-recovery-record-size+)
           pg::+wal-segment-size+)
       fork (* (1+ (floor fork pg::+wal-segment-size+)) pg::+wal-segment-size+))
   42 child parent 1))

(deftest receive-history-selection-preserves-base-and-flush-on-reopen
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (let ((digest (pg::physical-store-flushed-digest store)))
                 (ok (= 2 (pg::physical-store-select-history store 2 (store-history-text 1 32) (store-history-eor 32 2 1))))
                 (ok (= 1 (pg::physical-store-timeline store)))
                 (ok (= 32 (pg::physical-store-flushed-lsn store)))
                 (ok (= digest (pg::physical-store-flushed-digest store)))))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (progn
               (ok (= 2 (pg::physical-store-selected-timeline store)))
               (ok (= 32 (pg::physical-store-flushed-lsn store)))
               (ok (= 1 (pg::physical-store-prefix-timeline store 32))))
          (pg::physical-store-close store)))))

(deftest receive-history-eor-and-child-records-survive-reopen
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t))
            (eor (store-history-eor 32 2 1)) (seen nil))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (pg::physical-store-select-history store 2 (store-history-text 1 32) eor)
               (pg::physical-store-input store 32 eor)
               (pg::physical-store-input store 88 (sample-physical-record 88))
               (ok (= 32 (pg::physical-store-flushed-lsn store)))
               (ok (= 120 (pg::physical-store-flush store)))
               (pg::physical-store-map-durable-records store
                                                       (lambda (record raw) (declare (ignore raw)) (push (pg::wal-record-start record) seen)))
               (ok (equal '(88 32 0) seen))
               (ok (probe-file (pg::physical-store-segment-path store 0 1)))
               (ok (probe-file (pg::physical-store-segment-path store 0 2))))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (progn
               (ok (= 120 (pg::physical-store-flushed-lsn store)))
               (ok (= 2 (pg::physical-store-prefix-timeline store 120))))
          (pg::physical-store-close store)))))

(deftest receive-history-rejects-child-data-without-exact-eor
    (dolist (bad '(:ordinary :wrong-eor))
      (with-physical-store-directory (root)
        (let ((store (pg::physical-store-open root 123 1 :create t)))
          (unwind-protect
               (progn
                 (pg::physical-store-input store 0 (sample-physical-record))
                 (pg::physical-store-flush store)
                 (pg::physical-store-select-history store 2 (store-history-text 1 32) (store-history-eor 32 2 1))
                 (ok (signals
                      (pg::physical-store-input store 32
                                                (if (eq bad :ordinary) (sample-physical-record 32)
                                                    (pg::wal-encode-end-of-recovery-record 32 43 2 1 1))) 'error))
                 (ok (= 32 (pg::physical-store-flushed-lsn store))))
            (pg::physical-store-close store))))))

(deftest receive-history-selection-publication-authority
    (dolist (point '(:after-history-publication :after-control-rename :after-history-selection))
      (with-physical-store-directory (root)
        (let ((store (pg::physical-store-open root 123 1 :create t)))
          (unwind-protect
               (progn
                 (pg::physical-store-input store 0 (sample-physical-record))
                 (pg::physical-store-flush store)
                 (let ((pg::*physical-store-test-hook*
                         (lambda (where owner) (declare (ignore owner))
                           (when (eq point where) (error "injected history publication loss")))))
                   (ok (signals (pg::physical-store-select-history
                                 store 2 (store-history-text 1 32) (store-history-eor 32 2 1)) 'error))))
            (pg::physical-store-close store)))
        (let ((store (pg::physical-store-open root 123 1)))
          (unwind-protect
               (progn
                 (ok (= (if (eq point :after-history-publication) 1 2)
                        (pg::physical-store-selected-timeline store)))
                 (ok (= 32 (pg::physical-store-flushed-lsn store))))
            (pg::physical-store-close store))))))

(deftest receive-history-missing-proof-does-not-truncate-tail
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)) (path nil))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (pg::physical-store-select-history store 2 (store-history-text 1 32) (store-history-eor 32 2 1))
               (setf path (pg::physical-store-history-path store 2 (getf (pg::physical-store-selection store) :digest)))
               (pg::physical-store-input store 32 (store-history-eor 32 2 1)))
          (pg::physical-store-close store))
        (delete-file path)
        (ok (signals (pg::physical-store-open root 123 1) 'error))
        (with-open-file (input (pg::physical-store-segment-path store 0 2) :element-type '(unsigned-byte 8))
          (ok (= 88 (file-length input)))))))

(deftest receive-history-rotation-compaction-keeps-transition-proof
    (with-physical-store-directory (root)
      (let* ((segment pg::+wal-segment-size+) (base (- segment 72)) (fork (- segment 40))
                                              (store (pg::physical-store-open root 123 1 :create t :start-lsn base))
                                              (eor (store-history-eor fork 2 1)) (digest nil))
        (unwind-protect
             (progn
               (pg::physical-store-input store base (sample-physical-record base))
               (pg::physical-store-flush store)
               (pg::physical-store-select-history store 2 (store-history-text 1 fork) eor)
               (pg::physical-store-input store segment eor)
               (pg::physical-store-input store (+ segment 56) (sample-physical-record (+ segment 56)))
               (pg::physical-store-flush store)
               (setf digest (pg::physical-store-prefix-digest store (+ segment 56)))
               (pg::physical-store-compact store (+ segment 56)
                                           (lambda (value) (ok (= value digest))))
               (ng (probe-file (pg::physical-store-segment-path store 0 1))))
          (pg::physical-store-close store))
        (let ((reopened (pg::physical-store-open root 123 1)))
          (unwind-protect
               (progn
                 (ok (= (+ segment 56) (pg::physical-store-floor-lsn reopened)))
                 (ok (= digest (pg::physical-store-floor-digest reopened)))
                 (ok (= (+ segment 88) (pg::physical-store-flushed-lsn reopened)))
                 (ok (= 2 (pg::physical-store-prefix-timeline reopened (+ segment 56)))))
            (pg::physical-store-close reopened))))))

(deftest receive-history-successive-selections-preserve-all-ancestry
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t))
            (first-history (store-history-text 1 32)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (pg::physical-store-select-history store 2 first-history (store-history-eor 32 2 1))
               (pg::physical-store-input store 32 (store-history-eor 32 2 1))
               (pg::physical-store-input store 88 (sample-physical-record 88))
               (pg::physical-store-flush store)
               (pg::physical-store-select-history store 5 (store-history-text 2 120 first-history)
                                                  (store-history-eor 120 5 2))
               (pg::physical-store-input store 120 (store-history-eor 120 5 2))
               (pg::physical-store-flush store))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (progn
               (ok (= 176 (pg::physical-store-flushed-lsn store)))
               (ok (= 5 (pg::physical-store-selected-timeline store)))
               (ok (= 1 (pg::physical-store-prefix-timeline store 32)))
               (ok (= 2 (pg::physical-store-prefix-timeline store 120)))
               (ok (= 5 (pg::physical-store-prefix-timeline store 176))))
          (pg::physical-store-close store)))))

(deftest receive-history-selection-rejects-absent-or-unflushed-parent-prefix
    (dolist (mode '(:behind :beyond :complete-tail :partial-tail))
      (with-physical-store-directory (root)
        (let* ((store (pg::physical-store-open root 123 1 :create t))
               (fork (case mode (:behind 64) (:beyond 0) (t 32))))
          (unwind-protect
               (progn
                 (pg::physical-store-input store 0 (sample-physical-record))
                 (pg::physical-store-flush store)
                 (when (member mode '(:complete-tail :partial-tail))
                   (pg::physical-store-input store 32
                                             (if (eq mode :complete-tail) (sample-physical-record 32)
                                                 (subseq (sample-physical-record 32) 0 7))))
                 (let ((before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
                   (ok (signals (pg::physical-store-select-history
                                 store 2 (store-history-text 1 fork) (store-history-eor fork 2 1)) 'error))
                   (ok (equalp before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))))
            (pg::physical-store-close store))))))

(deftest receive-history-selection-rejects-rewritten-previous-fork
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (pg::physical-store-select-history store 2 (store-history-text 1 32) (store-history-eor 32 2 1))
               (pg::physical-store-input store 32 (store-history-eor 32 2 1))
               (pg::physical-store-flush store)
               (ok (signals (pg::physical-store-select-history
                             store 3 (store-history-text 2 88 (store-history-text 1 31))
                             (store-history-eor 88 3 2)) 'error))
               (ok (= 88 (pg::physical-store-flushed-lsn store))))
          (pg::physical-store-close store)))))

(deftest receive-history-unflushed-eor-is-discarded-without-forgetting-selection
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (pg::physical-store-select-history store 2 (store-history-text 1 32) (store-history-eor 32 2 1))
               (pg::physical-store-input store 32 (store-history-eor 32 2 1)))
          (pg::physical-store-close store)))
      (let ((store (pg::physical-store-open root 123 1)))
        (unwind-protect
             (progn
               (ok (= 2 (pg::physical-store-selected-timeline store)))
               (ok (= 32 (pg::physical-store-written-lsn store) (pg::physical-store-flushed-lsn store)))
               (ng (probe-file (pg::physical-store-segment-path store 0 2)))
               (pg::physical-store-input store 32 (store-history-eor 32 2 1))
               (ok (= 88 (pg::physical-store-flush store))))
          (pg::physical-store-close store)))))

(cl-spec:defproperty receive-history-selection-codec-preserves-fork-provenance
    ((fork (range integer 0 18446744073692774400))
     (identity (range integer 0 18446744073709551615)))
  (:about pg::physical-store-check-history-selection)
  (:trials (:normal 100))
  (let* ((store (pg::%make-physical-store :system-id identity :timeline 1 :base-lsn fork))
         (eor (store-history-eor fork 2 1))
         (payload (list :version 1 :system-id identity :base-timeline 1 :base fork
                        :target 2 :history-text (store-history-text 1 fork)
                        :fork-digest #xCBF29CE484222325 :previous-digest nil :eor-octets (coerce eor 'list)))
         (bytes (pg::physical-store-history-image payload))
         (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
         (selection (pg::physical-store-check-history-selection
                     store (pg::physical-store-history-payload bytes) digest nil)))
    (and (= fork (getf selection :fork))
         (= identity (getf selection :system-id))
         (equalp eor (getf selection :eor-bytes)))))

(deftest generated-receive-history-fork-provenance
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'receive-history-selection-codec-preserves-fork-provenance :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest receive-history-sigkill-selection-and-eor-durability
    (dolist (scenario '(("select" :after-history-publication 1 32)
                        ("select" :after-control-rename 2 32)
                        ("select" :after-history-selection 2 32)
                        ("flush" :after-segment-fsync 2 32)
                        ("flush" :after-control-rename 2 88)
                        ("flush" :after-control-directory-fsync 2 88)))
      (destructuring-bind (mode point target flush) scenario
        (with-physical-store-directory (root)
          (let ((store (pg::physical-store-open root 123 1 :create t)))
            (unwind-protect
                 (progn
                   (pg::physical-store-input store 0 (sample-physical-record))
                   (pg::physical-store-flush store))
              (pg::physical-store-close store)))
          (multiple-value-bind (output errors status)
              (uiop:run-program
               (list (or (uiop:getenv "SBCL") "sbcl") "--noinform" "--no-sysinit" "--no-userinit" "--script"
                     (namestring (asdf:system-relative-pathname "pg" "t/physical-store-history-crash-child.lisp"))
                     (namestring root) mode (symbol-name point))
               :output :string :error-output :string :ignore-error-status t)
            (ok (not (zerop status)) (format nil "~a ~a: unexpectedly succeeded" mode point))
            (ok (probe-file (merge-pathnames "history-crash.marker" root))
                (format nil "~a ~a: ~a ~a" mode point output errors)))
          (let ((store (pg::physical-store-open root 123 1)))
            (unwind-protect
                 (progn
                   (ok (= target (pg::physical-store-selected-timeline store)))
                   (ok (= flush (pg::physical-store-flushed-lsn store))))
              (pg::physical-store-close store)))))))

(deftest receive-history-publication-obeys-the-reopen-chain-bound
    (let ((store (pg::%make-physical-store :system-id 123 :timeline 1 :base-lsn 0))
          (previous nil) (history "") (fork 0))
      (loop for child from 2 to (1+ pg::+physical-store-history-chain-limit+)
            for parent = (1- child)
            do (setf history (store-history-text parent fork history)
                     previous
                     (pg::physical-store-check-history-selection
                      store (list :version 1 :system-id 123 :base-timeline 1 :base 0
                                  :target child :history-text history :fork-digest 0
                                  :previous-digest (getf previous :digest)
                                  :eor-octets (coerce (store-history-eor fork child parent) 'list))
                      child previous))
               (incf fork pg::+wal-end-of-recovery-record-size+))
      (ok (= 257 (getf previous :target)))
      (ok (signals
           (pg::physical-store-check-history-selection
            store (list :version 1 :system-id 123 :base-timeline 1 :base 0
                        :target 258 :history-text (store-history-text 257 fork history)
                        :fork-digest 0 :previous-digest (getf previous :digest)
                        :eor-octets (coerce (store-history-eor fork 258 257) 'list))
            258 previous) 'error))))

(deftest receive-history-cannot-select-past-an-unreceived-eor
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (pg::physical-store-select-history store 2 (store-history-text 1 32) (store-history-eor 32 2 1))
               (let ((before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
                 (ok (signals
                      (pg::physical-store-select-history store 3
                                                         (store-history-text 2 32 (store-history-text 1 32))
                                                         (store-history-eor 32 3 2)) 'error))
                 (ok (equalp before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))))
          (pg::physical-store-close store)))))

(deftest receive-history-oversized-publication-leaves-control-unchanged
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (unwind-protect
             (progn
               (pg::physical-store-input store 0 (sample-physical-record))
               (pg::physical-store-flush store)
               (let ((before (lineage-test-bytes (pg::physical-store-path store "receive.control")))
                     (text (concatenate 'string (store-history-text 1 32) "#"
                                        (make-string (* 16 1024 1024) :initial-element #\x))))
                 (let ((rejected (handler-case
                                     (progn (pg::physical-store-select-history store 2 text (store-history-eor 32 2 1)) nil)
                                   (error () t))))
                   (ok rejected))
                 (let ((unchanged (equalp before (lineage-test-bytes (pg::physical-store-path store "receive.control")))))
                   (ok unchanged))))
          (pg::physical-store-close store)))))

(deftest receive-history-envelope-roundtrips-at-the-reader-size-limit
    (let* ((store (pg::%make-physical-store :system-id 123 :timeline 1 :base-lsn 0))
           (payload (list :version 1 :system-id 123 :base-timeline 1 :base 0
                          :target 2 :history-text (concatenate 'string (store-history-text 1 32) "#")
                          :fork-digest 0 :previous-digest nil
                          :eor-octets (coerce (store-history-eor 32 2 1) 'list)))
           (overhead (length (pg::physical-store-history-image payload)))
           (padding (- pg::+physical-store-history-maximum-bytes+ overhead)))
      (setf (getf payload :history-text)
            (concatenate 'string (getf payload :history-text) (make-string padding :initial-element #\x)))
      (let* ((bytes (pg::physical-store-history-image payload))
             (size (length bytes))
             (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
             (decoded (pg::physical-store-history-payload bytes))
             (same (equal payload decoded))
             (selection (pg::physical-store-check-history-selection store decoded digest nil))
             (valid-fork (= 32 (getf selection :fork))))
        (ok (= size pg::+physical-store-history-maximum-bytes+))
        (ok same)
        (ok valid-fork))))

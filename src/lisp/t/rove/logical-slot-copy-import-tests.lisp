(in-package #:pg)

(defun unit-logical-import-value (&key (generation 0) (end 80) (phase :pending)
                                    (profile :text) (cause :none))
  (let* ((owner (mod (1+ generation) (ash 1 128)))
         (source (unit-logical-provenance-slot
                  :generation generation :end end :plugin "pg_lisp" :profile profile))
         (source-image (wal-encode-replication-slot source))
         (planned (logical-slot-copy-import-child-image source owner cause)))
    (list :phase phase :copy-id (logical-slot-generation-token 17)
          :device 1 :inode 2 :manifest-digest 3 :descriptor-digest 4 :base-proof-digest 5
          :system-id 42 :base-end end :base-timeline 1
          :source-history-digest 6 :selected-history-digest 7
          :promotion-generation 8 :parent-timeline 1 :child-timeline 2
          :fork end :child-end (1+ end) :pending-control-digest 9 :child-plan-digest 10
          :owner (logical-slot-generation-token owner) :retained-reference nil
          :slots (list (list "fixture" (logical-slot-generation-token generation)
                             (physical-store-update-digest #xCBF29CE484222325 source-image)
                             (bytes->string planned)
                             (if (eq cause :none) :eligible cause))))))

(cl-spec:defproperty logical-import-control-preserves-full-width
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (end (range integer 80 18446744073709551614)))
  (:about logical-slot-copy-import-image logical-slot-copy-import-decode)
  (:trials (:normal 100))
  (let* ((value (unit-logical-import-value
                 :generation generation :end end
                 :phase (if (oddp generation) :pending :active)
                 :profile (if (oddp end) :text :binary-bytea)
                 :cause (nth (mod generation 3) '(:none :wal-removed :wal-level-insufficient))))
         (image (logical-slot-copy-import-image value))
         (decoded (logical-slot-copy-import-decode image)))
    (and (equal value decoded)
         (equalp image (logical-slot-copy-import-image decoded))
         (= end (getf decoded :fork)))))

(rove:deftest logical-import-control-generated-contract
    (dolist (generation '(0 340282366920938463463374607431768211455))
      (let* ((value (unit-logical-import-value :generation generation))
             (image (logical-slot-copy-import-image value)))
        (rove:ok (equal value (logical-slot-copy-import-decode image)))))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'logical-import-control-preserves-full-width :seed 732))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
    (rove:ok (= 100 (getf data :trials)))))

(rove:deftest logical-import-control-rejects-corruption
    (let ((image (logical-slot-copy-import-image (unit-logical-import-value))))
      (dotimes (i (length image))
        (let ((bad (copy-seq image)))
          (setf (aref bad i) (logxor 1 (aref bad i)))
          (rove:ok (rove:signals (logical-slot-copy-import-decode bad) 'error))))
      (rove:ok (rove:signals
                (logical-slot-copy-import-decode (subseq image 0 (1- (length image)))) 'error))
      (rove:ok (rove:signals
                (logical-slot-copy-import-decode
                 (concatenate '(vector (unsigned-byte 8)) image #(10))) 'error)))
  (rove:ok (rove:signals (logical-slot-copy-import-decode #()) 'error))
  (rove:ok (rove:signals
            (logical-slot-copy-import-decode
             (make-array (1+ +logical-slot-copy-import-maximum-bytes+)
                         :element-type '(unsigned-byte 8) :initial-element 0)) 'error)))

(rove:deftest logical-import-control-rejects-shape-and-transition-changes
    (dolist (change '((:phase :unknown) (:device -1) (:inode nil)
                      (:manifest-digest 18446744073709551616)
                      (:promotion-generation -1) (:base-timeline 0) (:parent-timeline 0)
                      (:child-timeline 1) (:base-end 81) (:fork 79) (:child-end 80)
                      (:pending-control-digest nil) (:child-plan-digest -1)
                      (:owner "00000000000000000000000000000000")
                      (:copy-id "1")))
      (let ((value (unit-logical-import-value)))
        (setf (getf value (first change)) (second change))
        (rove:ok (rove:signals (logical-slot-copy-import-image value) 'error)
                 (format nil "~s" change))))
  (let ((value (unit-logical-import-value)))
    (rove:ok (rove:signals
              (logical-slot-copy-import-image (append value '(:extra 1))) 'error))
    (rove:ok (rove:signals
              (logical-slot-copy-import-image (append '(:phase :pending) value)) 'error))
    (setf (cdr (last value)) value)
    (rove:ok (rove:signals (logical-slot-copy-import-image value) 'error)))
  (let ((value (unit-logical-import-value)))
    (setf (getf value :slots)
          (list (first (getf value :slots)) (copy-list (first (getf value :slots)))))
    (rove:ok (rove:signals (logical-slot-copy-import-image value) 'error))))

(rove:deftest logical-import-control-deferred-rows-cannot-be-active
    (let* ((value (unit-logical-import-value))
           (row (first (getf value :slots))))
      (setf (fourth row) nil (fifth row) :deferred)
      (rove:ok (equal value (logical-slot-copy-import-decode (logical-slot-copy-import-image value))))
      (setf (getf value :phase) :active)
      (rove:ok (rove:signals (logical-slot-copy-import-image value) 'error))
      (setf (getf value :phase) :pending (fifth row) :eligible)
      (rove:ok (rove:signals (logical-slot-copy-import-image value) 'error))))

(rove:deftest logical-import-control-binds-planned-owner-generation-and-cause
    (dolist (mode '(:owner :generation :cause :system :ahead :nonascii))
      (let* ((value (unit-logical-import-value))
             (row (first (getf value :slots))))
        (ecase mode
          (:owner (setf (getf value :owner) (logical-slot-generation-token 2)))
          (:generation (setf (second row) (logical-slot-generation-token 3)))
          (:cause (setf (fifth row) :wal-removed))
          (:system (setf (getf value :system-id) 43))
          (:ahead (setf (getf value :base-end) 79 (getf value :fork) 79))
          (:nonascii (setf (fourth row) (concatenate 'string (fourth row) (string (code-char 256))))))
        (rove:ok (rove:signals (logical-slot-copy-import-image value) 'error)
                 (format nil "~s" mode)))))

(rove:deftest logical-import-child-image-preserves-source-and-first-cause
    (dolist (cause '(:none :wal-removed :wal-level-insufficient))
      (let* ((source (unit-logical-provenance-slot :child-owner 2 :plugin "pg_lisp"
                                                   :profile :binary-bytea :cause cause))
             (before (wal-encode-replication-slot source))
             (image (logical-slot-copy-import-child-image source 3 cause))
             (child (wal-decode-replication-slot-image image "fixture")))
        (rove:ok (equalp before (wal-encode-replication-slot source)))
        (rove:ok (eql 3 (wal-replication-slot-child-owner child)))
        (rove:ok (eq :binary-bytea (wal-slot-profile child)))
        (rove:ok (= 32 (wal-replication-slot-restart-lsn child)))
        (rove:ok (= 65 (wal-replication-slot-consistent-lsn child)))
        (rove:ok (= 80 (wal-replication-slot-confirmed-flush-lsn child)))
        (rove:ok (= 0 (logical-slot-provenance-generation
                       (wal-replication-slot-logical-provenance child))))
        (rove:ok (eq cause (logical-slot-effective-invalidation-cause child)))
        (when (not (eq cause :none))
          (rove:ok (rove:signals (logical-slot-copy-import-child-image source 3 :none) 'error)))))
  (dolist (owner '(0 2 -1 340282366920938463463374607431768211456))
    (rove:ok (rove:signals
              (logical-slot-copy-import-child-image
               (unit-logical-provenance-slot :child-owner 2) owner :none) 'error))))

(defvar *unit-logical-import-reader-fired* nil)

(defun unit-logical-import-text-image (text)
  (let ((body (string->bytes text)))
    (concatenate '(vector (unsigned-byte 8))
                 (string->bytes (format nil "PGLOGICALIMPORT1 ~d ~8,'0X~%"
                                        (length body) (wal-crc32 body)))
                 body #(10))))

(rove:deftest logical-import-control-rejects-reader-and-canonical-spelling
    (let ((*unit-logical-import-reader-fired* nil))
      (rove:ok (rove:signals
                (logical-slot-copy-import-decode
                 (unit-logical-import-text-image
                  "#.(progn (setf pg::*unit-logical-import-reader-fired* t) nil)")) 'error))
      (rove:ok (null *unit-logical-import-reader-fired*)
               "rejection must precede reader evaluation"))
  (dolist (text '("#()" "(:PHASE :PENDING)"))
    (rove:ok (rove:signals
              (logical-slot-copy-import-decode (unit-logical-import-text-image text)) 'error)))
  (let* ((value (unit-logical-import-value))
         (text (physical-bootstrap-manifest-text value)))
    (rove:ok (rove:signals
              (logical-slot-copy-import-decode
               (unit-logical-import-text-image (concatenate 'string " " text))) 'error))))

(rove:deftest logical-import-control-binds-retained-reference
    (let ((value (unit-logical-import-value)))
      (setf (getf value :fork) 100 (getf value :child-end) 101)
      (rove:ok (rove:signals (logical-slot-copy-import-image value) 'error))
      (setf (getf value :retained-reference)
            (list :root-id 12 :base-timeline 1 :base 32 :manifest-digest 3 :end 100 :end-digest 13))
      (rove:ok (equal value (logical-slot-copy-import-decode (logical-slot-copy-import-image value))))
      (setf (getf (getf value :retained-reference) :end) 99)
      (rove:ok (rove:signals (logical-slot-copy-import-image value) 'error))))

(cl-spec:defproperty logical-import-disposition-keeps-first-cause
    ((cause (member :none :wal-removed :wal-level-insufficient))
     (disable (member nil t)) (offset (range integer 0 100000))
     (profile (member :text :binary-bytea)))
  (:about logical-slot-copy-import-disposition)
  (:trials (:normal 150))
  (let* ((history (timeline-parse-history 1 ""))
         (slot (unit-logical-provenance-slot :cause cause :end (+ 80 offset)
                                             :plugin "pg_lisp" :profile profile))
         (before (wal-encode-replication-slot slot))
         (fact (physical-logical-fact-next
                nil (unit-logical-status-record t :start (+ 32 offset)) 1 history)))
    (when disable
      (setf fact (physical-logical-fact-next
                  fact (unit-logical-status-record nil :start (+ 128 offset)) 1 history)))
    (setf fact (physical-logical-fact-next
                fact (unit-logical-status-record t :start (+ 256 offset)) 1 history))
    (and (eq (if (not (eq :none cause)) cause
                 (if disable :wal-level-insufficient :eligible))
             (logical-slot-copy-import-disposition slot fact history (+ 289 offset) 1))
         (equalp before (wal-encode-replication-slot slot)))))

(rove:deftest logical-import-source-disposition-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-import-disposition-keeps-first-cause
                                       :profile :normal :seed 7411))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~S" data))))

(rove:deftest logical-import-source-unknown-and-ahead-stay-deferred
    (let* ((history (timeline-parse-history 1 ""))
           (slot (unit-logical-provenance-slot))
           (fact (physical-logical-fact-next
                  nil (unit-logical-status-record t :start 32) 1 history)))
      (setf fact (physical-logical-fact-next fact (unit-logical-status-record t :start 256) 1 history))
      (rove:ok (eq :deferred (logical-slot-copy-import-disposition slot nil history 289 1)))
      (rove:ok (eq :deferred (logical-slot-copy-import-disposition
                              (unit-logical-provenance-slot :provenance nil) fact history 289 1)))
      (rove:ok (eq :deferred (logical-slot-copy-import-disposition
                              (unit-logical-provenance-slot :known-on nil) fact history 289 1)))
      (rove:ok (eq :deferred (logical-slot-copy-import-disposition
                              (unit-logical-provenance-slot :end 300) fact history 289 1)))
      (rove:ok (rove:signals
                (logical-slot-copy-import-disposition slot fact history 288 1) 'error))))

(cl-spec:defproperty logical-import-owner-excludes-copied-identities
    ((generation (range integer 0 340282366920938463463374607431768211455)))
  (:about logical-slot-copy-import-new-owner)
  (:trials (:normal 150))
  (let* ((source (unit-logical-import-value :generation generation))
         (prior (mod (1+ generation) (ash 1 128)))
         (fresh (mod (+ generation 19) (ash 1 128)))
         (original (symbol-function 'logical-slot-new-generation))
         (calls 0))
    (when (= fresh 17) (setf fresh 18))
    (let ((choices (list generation prior 17 fresh)))
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-slot-new-generation)
                   (lambda () (incf calls) (pop choices)))
             (and (= fresh (logical-slot-copy-import-new-owner source)) (= calls 4)))
        (setf (symbol-function 'logical-slot-new-generation) original)))))

(rove:deftest logical-import-owner-selection-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-import-owner-excludes-copied-identities
                                       :profile :normal :seed 7511))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~S" data))))

(rove:deftest logical-import-owner-selection-bounds-collision-retries
    (let ((original (symbol-function 'logical-slot-new-generation)) (calls 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-slot-new-generation)
                   (lambda () (incf calls) 17))
             (rove:ok (rove:signals
                       (logical-slot-copy-import-new-owner (unit-logical-import-value)) 'error))
             (rove:ok (= 32 calls)))
        (setf (symbol-function 'logical-slot-new-generation) original))))

(cl-spec:defproperty logical-import-interest-preserves-configured-policy
    ((level (member :replica :logical)) (fpw (member nil t)) (interest (member nil t)))
  (:about logical-slot-copy-import-add-interest)
  (:trials (:normal 150))
  (let* ((policy (list :promotion-full-page-writes fpw
                       :promotion-logical-decoding-enabled (eq level :logical)
                       :promotion-wal-level level))
         (saved (copy-list policy))
         (result (logical-slot-copy-import-add-interest policy interest)))
    (and (equal policy saved)
         (eql fpw (getf result :promotion-full-page-writes))
         (eq level (getf result :promotion-wal-level))
         (eql (or (eq level :logical) interest)
              (getf result :promotion-logical-decoding-enabled)))))

(rove:deftest logical-import-interest-policy-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-import-interest-preserves-configured-policy
                                       :profile :normal :seed 7514))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~S" data))))

(rove:deftest logical-import-interest-keeps-absent-and-known-off-policies
    (rove:ok (null (logical-slot-copy-import-add-interest nil nil)))
  (rove:ok (equal '(:promotion-logical-decoding-enabled t)
                  (logical-slot-copy-import-add-interest nil t)))
  (let* ((policy (list :promotion-full-page-writes nil
                       :promotion-logical-decoding-enabled nil :promotion-wal-level :replica))
         (result (logical-slot-copy-import-add-interest policy nil)))
    (rove:ok (equal policy result))
    (rove:ok (not (eq policy result)))
    (setf (getf result :promotion-logical-decoding-enabled) t)
    (rove:ok (null (getf policy :promotion-logical-decoding-enabled)))))

(rove:deftest logical-import-intent-runtime-waits-for-owner-exit-and-fences-uncertainty
    (let* ((before (list :phase 2))
           (*physical-recovery-state* before)
           (*physical-recovery-failed-p* nil)
           (*physical-recovery-control-test-hook* nil))
      (rove:ok
       (rove:signals
        (call-with-physical-promotion-intent-publication
         (lambda (publication)
           (declare (ignore publication))
           (error "preflight rejected")))
        'error))
      (rove:ok (and (eq before *physical-recovery-state*) (not *physical-recovery-failed-p*)))
      (rove:ok
       (rove:signals
        (call-with-physical-promotion-intent-publication
         (lambda (publication)
           (setf (physical-promotion-intent-publication-started-p publication) t)
           (unwind-protect (list :phase 3)
             (assert (eq before *physical-recovery-state*))
             (error "owned scope final check rejected"))))
        'error))
      (rove:ok (and (eq before *physical-recovery-state*) *physical-recovery-failed-p*)))
  (let* ((before (list :phase 2)) (pending (list :phase 3))
                                  (*physical-recovery-state* before) (*physical-recovery-failed-p* nil)
                                  (*physical-recovery-control-test-hook* nil))
    (rove:ok (eq pending
                 (call-with-physical-promotion-intent-publication
                  (lambda (publication)
                    (assert (eq before *physical-recovery-state*))
                    (setf (physical-promotion-intent-publication-started-p publication) t)
                    pending))))
    (rove:ok (eq pending *physical-recovery-state*))
    (rove:ok (not *physical-recovery-failed-p*))))

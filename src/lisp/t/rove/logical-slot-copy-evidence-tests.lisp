(in-package #:pg)

(defun unit-logical-import-evidence (fork salt)
  (let* ((state (list :phase 3 :applied fork :timeline 1 :promotion-child 2
                      :next-xid 50 :state-time salt
                      :source-reservation (list :version 1 :through fork :bound 1002 :complete t)))
         (plan (progn
                 (when (plusp (mod salt 3))
                   (setf (getf state :promotion-full-page-writes) (oddp salt)))
                 (when (= 2 (mod salt 3))
                   (setf (getf state :promotion-logical-decoding-enabled) (oddp salt)
                         (getf state :promotion-wal-level) (if (and (oddp salt) (logbitp 3 salt)) :logical :replica)))
                 (physical-promotion-checkpoint-plan state 20000 1003))))
    (list :manifest (make-array 3 :element-type '(unsigned-byte 8)
                                  :initial-contents (list 0 255 (mod salt 256)))
          :checkpoint (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 0 255)) :control (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(2 0 255))
          :selected-history (make-array 0 :element-type '(unsigned-byte 8))
          :child-history (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(255 0 3)) :plan plan)))

(cl-spec:defproperty logical-import-evidence-keeps-exact-bytes
    ((fork (range integer 256 50000000)) (salt (range integer 0 2147483647)))
  (:about logical-slot-copy-import-evidence-encode logical-slot-copy-import-evidence-decode)
  (:trials (:normal 200))
  (let* ((value (unit-logical-import-evidence fork salt))
         (image (logical-slot-copy-import-evidence-encode value))
         (before (copy-seq image))
         (decoded (logical-slot-copy-import-evidence-decode image)))
    (and (equalp value decoded)
         (equalp before image)
         (equalp image (logical-slot-copy-import-evidence-encode decoded))
         (not (eq (getf value :manifest) (getf decoded :manifest))))))

(rove:deftest logical-import-evidence-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-import-evidence-keeps-exact-bytes
                                       :seed 7516 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest logical-import-evidence-covers-policy-and-rotation-geometries
    (dolist (offset '(0 1 31 55 56 57 119 120 121 191 192 193))
      (dotimes (salt 6)
        (let* ((value (unit-logical-import-evidence (- +wal-segment-size+ offset) salt))
               (image (logical-slot-copy-import-evidence-encode value)))
          (rove:ok (equalp value (logical-slot-copy-import-evidence-decode image)))))))

(cl-spec:defproperty recovery-manifest-byte-decoder-preserves-envelope
    ((number (range integer 0 18446744073709551615)))
  (:about recovery-decode-manifest)
  (:trials (:normal 100))
  (let* ((value (list :number number :name "λ\\\"🙂"))
         (image (physical-bootstrap-manifest-image value)))
    (multiple-value-bind (decoded digest) (recovery-decode-manifest image)
      (and (equal value decoded) (= digest (physical-store-update-digest #xCBF29CE484222325 image))))))

(rove:deftest recovery-manifest-byte-decoder-generated-contract
    (unit-check-property 'recovery-manifest-byte-decoder-preserves-envelope))

(rove:deftest logical-import-artifacts-retain-startup-and-promotion-fences
    (let* ((base (uiop:ensure-directory-pathname
                  (sb-posix:mkdtemp
                   (namestring (merge-pathnames "import-artifact-XXXXXX" (uiop:temporary-directory))))))
           (*pgdata* base) (*physical-recovery-state* nil))
      (unwind-protect
           (dolist (name '("logical-slot-copy.import" "logical-slot-copy.import.tmp"
                           "logical-slot-copy.evidence" "logical-slot-copy.evidence.tmp"))
             (let ((path (merge-pathnames name base)))
               (with-open-file (out path :direction :output :if-exists :error)
                 (write-char #\x out))
               (rove:ok (rove:signals (logical-slot-copy-preflight-startup) 'error))
               (rove:ok (rove:signals (logical-slot-copy-check-promotion) 'error))
               (delete-file path)))
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))
  (rove:ok (rove:signals (logical-slot-copy-import-file-spec :active) 'error)))

(rove:deftest logical-import-evidence-history-keeps-the-applied-parent-before-a-newer-selection
    (let* ((parent (timeline-encode-history 2 1 100 "first fork" nil))
           (selected (timeline-encode-history 3 2 200 "received future" parent))
           (descriptor (make-logical-slot-copy-descriptor
                        :history (make-array 0 :element-type '(unsigned-byte 8))))
           (control (list :timeline 1 :history-target 3
                          :history-digest (logical-slot-copy-import-digest selected))))
      (dolist (fork '(100 150 200))
        (let* ((certificate (list :base-timeline 1 :parent-timeline 2 :child-timeline 4 :fork fork))
               (evidence (list :selected-history selected
                               :child-history (timeline-encode-history 4 2 fork
                                                                       "pg-lisp promotion" parent)))
               (history (logical-slot-copy-evidence-check-history
                         evidence certificate descriptor control)))
          (rove:ok (= 3 (timeline-history-entry-timeline (first history))))
          (rove:ok (timeline-prefix-boundary-p 2 fork history))
          (setf (getf evidence :child-history)
                (timeline-encode-history 4 2 (1+ fork) "pg-lisp promotion" parent))
          (rove:ok (rove:signals
                    (logical-slot-copy-evidence-check-history
                     evidence certificate descriptor control) 'error))))
      (let* ((certificate (list :base-timeline 1 :parent-timeline 2 :child-timeline 4 :fork 150))
             (changed-parent (timeline-encode-history 2 1 99 "changed ancestry" nil))
             (evidence (list :selected-history selected
                             :child-history (timeline-encode-history 4 2 150
                                                                     "pg-lisp promotion" changed-parent))))
        (rove:ok (rove:signals
                  (logical-slot-copy-evidence-check-history evidence certificate descriptor control)
                  'error)))))

(rove:deftest logical-import-evidence-retains-all-three-large-raw-blobs
    (let ((value (unit-logical-import-evidence 256 0))
          (size (* 16 1024 1024)))
      (dolist (key '(:manifest :selected-history :child-history))
        (setf (getf value key) (make-array size :element-type '(unsigned-byte 8)
                                                :initial-element 255)))
      (let* ((image (logical-slot-copy-import-evidence-encode value))
             (decoded (logical-slot-copy-import-evidence-decode image)))
        (rove:ok (> (length image) (* 3 size)))
        (rove:ok (<= (length image) +logical-slot-copy-evidence-maximum-bytes+))
        (dolist (key '(:manifest :selected-history :child-history))
          (rove:ok (= size (length (getf decoded key))))
          (rove:ok (equalp (getf value key) (getf decoded key)))))))

(rove:deftest logical-import-evidence-rejects-envelope-and-bounds
    (let ((image (logical-slot-copy-import-evidence-encode
                  (unit-logical-import-evidence 256 1))))
      (dotimes (index (length image))
        (let ((bad (copy-seq image)))
          (setf (aref bad index) (logxor 1 (aref bad index)))
          (rove:ok (rove:signals (logical-slot-copy-import-evidence-decode bad) 'error))))
      (rove:ok (rove:signals
                (logical-slot-copy-import-evidence-decode (subseq image 0 (1- (length image))))
                'error))
      (rove:ok (rove:signals
                (logical-slot-copy-import-evidence-decode
                 (concatenate '(vector (unsigned-byte 8)) image #(0))) 'error))
      (let ((bad (copy-seq image)))
        (wal-set-u32 bad 20 #xffffffff)
        (wal-set-u32 bad (- (length bad) 4) (wal-crc32 bad 0 (- (length bad) 4)))
        (rove:ok (rove:signals (logical-slot-copy-import-evidence-decode bad) 'error))))
  (dolist (change '((:extra 1) (:manifest nil) (:plan nil)))
    (let ((value (unit-logical-import-evidence 256 1)))
      (setf (getf value (first change)) (second change))
      (rove:ok (rove:signals (logical-slot-copy-import-evidence-encode value) 'error)))))

(rove:deftest logical-import-evidence-rejects-valid-crc-length-and-count-forgeries
    (let* ((image (logical-slot-copy-import-evidence-encode (unit-logical-import-evidence 256 0)))
           (position 20))
      (labels ((reject-at (offset number)
                 (let ((bad (copy-seq image)))
                   (wal-set-u32 bad offset number)
                   (wal-set-u32 bad (- (length bad) 4) (wal-crc32 bad 0 (- (length bad) 4)))
                   (rove:ok (rove:signals (logical-slot-copy-import-evidence-decode bad) 'error)))))
        (dotimes (index 5)
          (declare (ignorable index))
          (reject-at position #xffffffff)
          (incf position (+ 4 (wal-u32 image position))))
        (incf position 20)
        (reject-at position 16385)
        (incf position (+ 4 (wal-u32 image position)))
        (dolist (count '(0 1 2 4 6 8 4294967295))
          (reject-at position count)))))

(rove:deftest logical-import-evidence-rejects-child-plan-substitution
    (dolist (mode '(:fork :child :end :frame-order :frame-coordinate :snapshot-extra))
      (let* ((value (unit-logical-import-evidence 256 1))
             (plan (getf value :plan)))
        (ecase mode
          (:fork (incf (getf plan :fork)))
          (:child (incf (getf plan :child)))
          (:end (incf (getf plan :end)))
          (:frame-order (rotatef (first (getf plan :frames)) (second (getf plan :frames))))
          (:frame-coordinate (incf (first (first (getf plan :frames)))))
          (:snapshot-extra (setf (getf (getf plan :snapshot) :extra) 1)))
        (rove:ok (rove:signals (logical-slot-copy-import-evidence-encode value) 'error)
                 (format nil "~s" mode)))))

(rove:deftest logical-import-evidence-missing-control-fact-requires-source-replay
    (let* ((certificate (unit-logical-import-value))
           (source (unit-logical-provenance-slot :generation 0 :end 80 :plugin "pg_lisp" :profile :text))
           (bytes (logical-slot-state-encode source))
           (descriptor (make-logical-slot-copy-descriptor :slots (list (list "fixture" bytes))))
           (history (timeline-parse-history 1 "")))
      (rove:ok (eq :requires-source-replay
                   (logical-slot-copy-evidence-check-rows certificate descriptor nil history)))
      (rove:ok (rove:signals
                (logical-slot-copy-evidence-check-rows certificate descriptor nil nil) 'error))
      (let ((bad (copy-tree certificate)))
        (setf (second (first (getf bad :slots))) (logical-slot-generation-token 3))
        (rove:ok (rove:signals
                  (logical-slot-copy-evidence-check-rows bad descriptor nil history) 'error)))
      (setf source (unit-logical-provenance-slot
                    :generation 0 :end 80 :plugin "pg_lisp" :profile :text :child-owner 2)
            bytes (logical-slot-state-encode source)
            (logical-slot-copy-descriptor-slots descriptor) (list (list "fixture" bytes)))
      (let* ((bad (copy-tree certificate))
             (row (first (getf bad :slots)))
             (child (unit-logical-provenance-slot
                     :generation 0 :end 80 :plugin "pg_lisp" :profile :text :child-owner 2)))
        (setf (getf bad :owner) (logical-slot-generation-token 2)
              (third row) (logical-slot-copy-import-digest bytes)
              (fourth row) (bytes->string (logical-slot-state-encode child)))
        (rove:ok (rove:signals
                  (logical-slot-copy-evidence-check-rows bad descriptor nil history) 'error)))))

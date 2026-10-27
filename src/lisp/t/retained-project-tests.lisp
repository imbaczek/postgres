(in-package #:pg-physical-tests)

(defun project-test-source (target)
  (let* ((base (overlap-test-bytes 96))
         (raw (concatenate '(vector (unsigned-byte 8))
                           (legacy-checkpoint-record 96) (legacy-checkpoint-record 128)
                           (pg::wal-encode-end-of-recovery-record 160 42 2 1 1)
                           (legacy-checkpoint-record 216)
                           (when (= target 3)
                             (concatenate '(vector (unsigned-byte 8))
                                          (legacy-checkpoint-record 248)
                                          (pg::wal-encode-end-of-recovery-record 280 42 3 2 1)
                                          (legacy-checkpoint-record 336)))))
         (bytes (concatenate '(vector (unsigned-byte 8)) base raw))
         (text (if (= target 2) (store-history-text 1 160)
                   (concatenate 'string (store-history-text 1 160) (store-history-text 2 280))))
         (metadata (list :version 1 :system-id 123 :base-timeline 1 :base 96 :manifest-digest 77
                         :history-target target :history-digest (if (= target 2) 99 123)
                         :history-text text :start 96 :end (length bytes)
                         :start-digest #xCBF29CE484222325
                         :end-digest (pg::physical-store-update-digest #xCBF29CE484222325 raw)
                         :segment 0 :file-timeline target
                         :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
    (values (pg::physical-retained-extent-encode metadata bytes)
            (pg::physical-retained-overlap-encode (overlap-test-metadata 96 base) base)
            (pg::timeline-parse-history target text)
            '(:system-id 123 :base-timeline 1 :base 96 :manifest-digest 77)
            text bytes)))

(defun project-test-publish-source (root image overlap identity target text)
  (let ((owner (gensym)) (selected nil))
    (pg::call-with-physical-retained-root-update
     (lambda (initial hold) (declare (ignore initial))
       (let ((entries (mapcar (lambda (item) (pg::physical-retained-root-stage-image hold target text item))
                              (if (listp image) image (list image))))
             (auxiliary (pg::physical-retained-root-stage-overlap hold target text overlap)))
         (setf selected (pg::physical-retained-root-publish-staged-entries
                         hold target text entries :base-overlap auxiliary))))
     owner root identity target text nil :read-selected (lambda () selected))
    (values selected owner)))

(defun project-test-project-prefix (hold source history &rest options)
  (let ((owner (when source
                 (loop for owner being the hash-keys of pg::*physical-retained-root-references*
                         using (hash-value reference)
                       when (and (= source (pg::physical-retained-root-reference-root-id reference))
                                 (equal (pg::physical-retained-root-hold-directory hold)
                                        (pg::physical-retained-root-reference-directory reference)))
                         return owner))))
    (apply #'pg::physical-retained-root-project-prefix hold source history
           (append options (list :source-owner owner)))))

(deftest retained-projection-clips-copied-parent-prefix-before-future-eor
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text bytes) (project-test-source 2)
            (let* ((source (project-test-publish-source root image overlap identity 2 text))
                   (original (lineage-test-bytes (pg::physical-retained-object-path root :inventory source))))
              (dolist (fork '(96 128 160))
                (let ((selected nil) (owner (gensym))
                                     (digest (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96 fork))))
                  (pg::call-with-physical-retained-root-update
                   (lambda (initial hold) (declare (ignore initial))
                     (setf selected (project-test-project-prefix
                                     hold source history 1 "" nil fork digest)))
                   owner root identity 1 "" nil :read-selected (lambda () selected))
                  (let* ((payload (pg::physical-retained-inventory-load
                                   root selected identity (pg::timeline-parse-history 1 "")))
                         (entries (getf payload :entries)))
                    (ok (getf payload :base-overlap))
                    (if (= fork 96)
                        (ok (null entries))
                        (multiple-value-bind (metadata captured)
                            (pg::physical-retained-extent-decode
                             (pg::physical-retained-object-read root :extent (caar entries)
                                                                pg::+physical-retained-image-limit+))
                          (ok (= fork (length captured) (getf metadata :end)))
                          (ok (= 1 (getf metadata :file-timeline) (getf metadata :history-target)))
                          (ok (null (getf metadata :history-digest)))
                          (ok (= digest (getf metadata :end-digest)))
                          (ok (equalp captured (subseq bytes 0 fork))))))))
              (ok (equalp original (lineage-test-bytes
                                    (pg::physical-retained-object-path root :inventory source)))))))))

(deftest retained-projection-preserves-applied-eor-and-parent-selection-digest
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text bytes) (project-test-source 3)
            (let* ((source (project-test-publish-source root image overlap identity 3 text))
                   (parent-text (store-history-text 1 160))
                   (fork 248) (selected nil)
                   (digest (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96 fork))))
              (pg::call-with-physical-retained-root-update
               (lambda (initial hold) (declare (ignore initial))
                 (setf selected (project-test-project-prefix
                                 hold source history 2 parent-text 99 fork digest)))
               (gensym) root identity 2 parent-text nil :read-selected (lambda () selected))
              (let* ((payload (pg::physical-retained-inventory-load
                               root selected identity (pg::timeline-parse-history 2 parent-text)))
                     (entry (first (getf payload :entries))))
                (multiple-value-bind (metadata captured)
                    (pg::physical-retained-extent-decode
                     (pg::physical-retained-object-read root :extent (first entry) pg::+physical-retained-image-limit+))
                  (ok (= 2 (getf metadata :history-target) (getf metadata :file-timeline)))
                  (ok (= 99 (getf metadata :history-digest)))
                  (ok (= 248 (length captured)))
                  (ok (= pg::+wal-record-end-of-recovery+
                         (pg::wal-record-type (pg::wal-decode-record (subseq captured 160 216) 160))))
                  (ok (equalp captured (subseq bytes 0 fork))))))))))

(deftest retained-projection-rejects-inexact-endpoints-and-divergent-ancestry
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text bytes) (project-test-source 3)
            (let ((source (project-test-publish-source root image overlap identity 3 text))
                  (parent-text (store-history-text 1 160)))
              (dolist (case (list (list 1 "" nil 144 0)
                                  (list 1 "" nil 128 0)
                                  (list 1 "" nil 248 0)
                                  (list 2 parent-text nil 248 0)
                                  (list 2 (store-history-text 1 128) 99 248
                                        (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96 248)))))
                (let ((owner (gensym)))
                  (ok (intake-fails-p
                       (lambda () (pg::call-with-physical-retained-root-update
                                   (lambda (initial hold) (declare (ignore initial))
                                     (apply #'project-test-project-prefix hold source history case))
                                   owner root identity 1 "" nil :read-selected (lambda () nil)))))
                  (ok (null (pg::physical-retained-root-current owner))))))))))

(deftest retained-projection-cannot-fill-a-logical-hole-from-overlap
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text bytes) (project-test-source 2)
            (multiple-value-bind (metadata data) (pg::physical-retained-extent-decode image)
              (setf (getf metadata :start) 128
                    (getf metadata :start-digest)
                    (pg::physical-store-update-digest #xCBF29CE484222325 (subseq data 96 128)))
              (setf image (pg::physical-retained-extent-encode metadata data)))
            (let ((source (project-test-publish-source root image overlap identity 2 text)))
              (ok (intake-fails-p
                   (lambda () (pg::call-with-physical-retained-root-update
                               (lambda (initial hold) (declare (ignore initial))
                                 (project-test-project-prefix
                                  hold source history 1 "" nil 160
                                  (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96 160))))
                               (gensym) root identity 1 "" nil :read-selected (lambda () nil))))))))))

(deftest retained-projection-empty-root-requires-base-overlap-and-fixed-seed
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (dolist (base (list 32 96 pg::+wal-segment-size+))
            (let* ((bytes (overlap-test-bytes base))
                   (image (pg::physical-retained-overlap-encode (overlap-test-metadata base bytes) bytes))
                   (identity (list :system-id 123 :base-timeline 1 :base base :manifest-digest 77))
                   (history (pg::timeline-parse-history 1 ""))
                   (selected nil))
              (pg::call-with-physical-retained-root-update
               (lambda (initial hold) (declare (ignore initial))
                 (let ((entry (pg::physical-retained-root-stage-overlap hold 1 "" image)))
                   (setf selected (project-test-project-prefix
                                   hold nil history 1 "" nil base #xCBF29CE484222325 :base-overlap entry))))
               (gensym) root identity 1 "" nil :read-selected (lambda () selected))
              (let ((payload (pg::physical-retained-inventory-load root selected identity history)))
                (ok (null (getf payload :entries)))
                (ok (getf payload :base-overlap)))
              (dolist (bad '(nil t))
                (ok (intake-fails-p
                     (lambda () (pg::call-with-physical-retained-root-update
                                 (lambda (initial hold) (declare (ignore initial))
                                   (let ((entry (when bad (pg::physical-retained-root-stage-overlap hold 1 "" image))))
                                     (project-test-project-prefix
                                      hold nil history 1 "" nil base (if bad 0 #xCBF29CE484222325)
                                      :base-overlap entry)))
                                 (gensym) root identity 1 "" nil :read-selected (lambda () nil)))))))))))

(cl-spec:defproperty retained-parent-projection-preserves-prefix-boundaries
    ((count (range integer 1 16)) (kept (range integer 1 16))
                                  (system-id (range integer 0 18446744073709551615)))
  (:about pg::physical-retained-project-extent)
  (:trials (:normal 200))
  (let* ((base 96) (fork (+ base (* count 32))) (end (+ base (* (1+ (mod (1- kept) count)) 32)))
                   (raw (apply #'concatenate '(vector (unsigned-byte 8))
                               (append (loop for index below count collect (legacy-checkpoint-record (+ base (* index 32))))
                                       (list (pg::wal-encode-end-of-recovery-record fork 42 2 1 1)))))
                   (bytes (concatenate '(vector (unsigned-byte 8)) (overlap-test-bytes base) raw))
                   (metadata (list :version 1 :system-id system-id :base-timeline 1 :base base :manifest-digest 77
                                   :history-target 2 :history-digest 99 :history-text (store-history-text 1 fork)
                                   :start base :end (length bytes) :start-digest #xCBF29CE484222325
                                   :end-digest (pg::physical-store-update-digest #xCBF29CE484222325 raw)
                                   :segment 0 :file-timeline 2
                                   :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
                   (image (pg::physical-retained-extent-encode metadata bytes))
                   (projected (pg::physical-retained-project-extent image 1 "" nil end)))
    (multiple-value-bind (loaded prefix) (pg::physical-retained-extent-decode projected)
      (and (= end (length prefix) (getf loaded :end))
           (= system-id (getf loaded :system-id))
           (= 1 (getf loaded :history-target) (getf loaded :file-timeline))
           (null (getf loaded :history-digest))
           (= (getf loaded :end-digest)
              (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes base end)))
           (equalp prefix (subseq bytes 0 end))))))

(deftest generated-retained-parent-projection-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'retained-parent-projection-preserves-prefix-boundaries :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest retained-projection-rewrites-uncut-future-history-and-omits-fork-start-entry
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text bytes) (project-test-source 2)
            (multiple-value-bind (metadata ignored) (pg::physical-retained-extent-decode image)
              (declare (ignore ignored))
              (let* ((digest (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96 160)))
                     (left (copy-list metadata)) (right (copy-list metadata))
                     (prefix (subseq bytes 0 160)))
                (setf (getf left :end) 160 (getf left :end-digest) digest
                      (getf left :prefix-digest) (pg::physical-store-update-digest #xCBF29CE484222325 prefix)
                      (getf right :start) 160 (getf right :start-digest) digest)
                (let* ((first-image (pg::physical-retained-extent-encode left prefix))
                       (source (project-test-publish-source
                                root (list first-image (pg::physical-retained-extent-encode right bytes))
                                overlap identity 2 text))
                       (selected nil))
                  (pg::call-with-physical-retained-root-update
                   (lambda (initial hold) (declare (ignore initial))
                     (setf selected (project-test-project-prefix
                                     hold source history 1 "" nil 160 digest)))
                   (gensym) root identity 1 "" nil :read-selected (lambda () selected))
                  (let* ((payload (pg::physical-retained-inventory-load
                                   root selected identity (pg::timeline-parse-history 1 "")))
                         (entries (getf payload :entries)))
                    (ok (= 1 (length entries)))
                    (ok (/= (caar entries) (pg::physical-store-update-digest #xCBF29CE484222325 first-image)))
                    (ok (= 1 (getf (third (first entries)) :history-target)))))))))))

(deftest retained-projection-preserves-rotation-gap-without-granting-gap-boundaries
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (let* ((segment pg::+wal-segment-size+) (base (- segment 16))
                                                  (prefix (overlap-test-bytes base))
                                                  (raw (pg::wal-encode-end-of-recovery-record segment 42 2 1 1))
                                                  (digest (pg::physical-store-update-digest #xCBF29CE484222325 raw))
                                                  (text (store-history-text 1 base)) (history (pg::timeline-parse-history 2 text))
                                                  (identity (list :system-id 123 :base-timeline 1 :base base :manifest-digest 77))
                                                  (metadata (append (list :version 1) identity
                                                                    (list :history-target 2 :history-digest 99 :history-text text
                                                                          :start base :end (+ segment 56) :start-digest #xCBF29CE484222325
                                                                          :end-digest digest :segment 1 :file-timeline 2
                                                                          :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 raw))))
                                                  (image (pg::physical-retained-extent-encode metadata raw))
                                                  (auxiliary (pg::physical-retained-overlap-encode (overlap-test-metadata base prefix) prefix))
                                                  (source (project-test-publish-source root image auxiliary identity 2 text)))
            (dolist (case (list (list 1 "" nil base #xCBF29CE484222325)
                                (list 2 text 99 (+ segment 56) digest)))
              (let ((selected nil))
                (pg::call-with-physical-retained-root-update
                 (lambda (initial hold) (declare (ignore initial))
                   (setf selected (apply #'project-test-project-prefix hold source history case)))
                 (gensym) root identity (first case) (second case) nil :read-selected (lambda () selected))
                (let ((payload (pg::physical-retained-inventory-load
                                root selected identity (pg::timeline-parse-history (first case) (second case)))))
                  (if (= 1 (first case)) (ok (null (getf payload :entries)))
                      (ok (= digest (getf (third (first (getf payload :entries))) :end-digest)))))))
            (ok (intake-fails-p
                 (lambda () (pg::call-with-physical-retained-root-update
                             (lambda (initial hold) (declare (ignore initial))
                               (project-test-project-prefix hold source history 2 text 99 (1+ base) digest))
                             (gensym) root identity 2 text nil :read-selected (lambda () nil)))))))))

(deftest retained-projection-reserves-source-objects-before-releasing-index-lock
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text ignored) (project-test-source 2)
            (declare (ignore ignored))
            (let* ((source (pg::physical-retained-inventory-publish root identity 2 text (list image)))
                   (source-owner (gensym))
                   (image-id (pg::physical-store-update-digest #xCBF29CE484222325 image))
                   (done (sb-thread:make-semaphore)) (deleted :pending)
                   (thread nil) (selected nil)
                   (pg::*physical-retained-root-test-hook*
                     (lambda (point hold)
                       (declare (ignore hold))
                       (when (eq point :after-project-source-reference)
                         (ok (not (sb-thread:holding-mutex-p pg::*physical-retained-inventory-publish-lock*)))
                         (pg::physical-retained-root-retire source-owner root source identity (lambda () nil))
                         (setf thread (retained-root-test-thread
                                       (lambda ()
                                         (setf deleted (pg::physical-retained-root-delete-unprotected root :extent image-id))
                                         (sb-thread:signal-semaphore done))))
                         (ok (sb-thread:wait-on-semaphore done :timeout 5))
                         (ok (null deleted))))))
              (pg::physical-retained-root-register source-owner root source identity history)
              (unwind-protect
                   (pg::call-with-physical-retained-root-update
                    (lambda (initial hold) (declare (ignore initial))
                      (let ((entry (pg::physical-retained-root-stage-overlap hold 1 "" overlap)))
                        (setf selected (pg::physical-retained-root-project-prefix
                                        hold source history 1 "" nil 96 #xCBF29CE484222325
                                        :base-overlap entry :source-owner source-owner))))
                    (gensym) root identity 1 "" nil :read-selected (lambda () selected))
                (when thread (sb-thread:join-thread thread :timeout 5 :default :timeout)))
              (ok (probe-file (pg::physical-retained-object-path root :extent image-id))))))))

(deftest retained-projection-child-base-uses-base-history-without-selection-digest
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image ignored old-history ignored-identity ignored-text bytes) (project-test-source 2)
            (declare (ignore ignored old-history ignored-identity ignored-text))
            (multiple-value-bind (metadata ignored) (pg::physical-retained-extent-decode image)
              (declare (ignore ignored))
              (replace bytes (pg::wal-encode-end-of-recovery-record 160 42 3 2 1) :start1 160)
              (let* ((parent-text (store-history-text 1 64))
                     (text (concatenate 'string parent-text (store-history-text 2 160)))
                     (history (pg::timeline-parse-history 3 text))
                     (identity '(:system-id 123 :base-timeline 2 :base 96 :manifest-digest 77))
                     (base-bytes (subseq bytes 0 96))
                     (overlap (pg::physical-retained-overlap-encode
                               (overlap-test-metadata 96 base-bytes :timeline 2 :namespace :timeline) base-bytes))
                     (digest (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96 128)))
                     (selected nil))
                (setf (getf metadata :base-timeline) 2 (getf metadata :history-target) 3
                      (getf metadata :history-digest) 123 (getf metadata :history-text) text
                      (getf metadata :file-timeline) 3
                      (getf metadata :end-digest)
                      (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96))
                      (getf metadata :prefix-digest) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
                (let ((source (project-test-publish-source
                               root (pg::physical-retained-extent-encode metadata bytes) overlap identity 3 text)))
                  (pg::call-with-physical-retained-root-update
                   (lambda (initial hold) (declare (ignore initial))
                     (setf selected (project-test-project-prefix
                                     hold source history 2 parent-text nil 128 digest)))
                   (gensym) root identity 2 parent-text nil :read-selected (lambda () selected))
                  (let* ((payload (pg::physical-retained-inventory-load
                                   root selected identity (pg::timeline-parse-history 2 parent-text)))
                         (entry (first (getf payload :entries))))
                    (ok (= 2 (getf (third entry) :history-target) (getf (third entry) :base-timeline)))
                    (ok (null (getf (third entry) :history-digest)))))))))))

(deftest retained-projection-rejects-corrupt-source-tail-before-selecting-prefix
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text bytes) (project-test-source 2)
            (multiple-value-bind (source source-owner) (project-test-publish-source root image overlap identity 2 text)
              (let* ((path (pg::physical-retained-object-path
                            root :extent (pg::physical-store-update-digest #xCBF29CE484222325 image)))
                     (corrupt (copy-seq image)) (owner (gensym)))
                (setf (aref corrupt (- (length corrupt) 5)) (logxor 1 (aref corrupt (- (length corrupt) 5))))
                (pg::wal-atomic-write-bytes path corrupt)
                (ok (intake-fails-p
                     (lambda () (pg::call-with-physical-retained-root-update
                                 (lambda (initial hold) (declare (ignore initial))
                                   (project-test-project-prefix
                                    hold source history 1 "" nil 128
                                    (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96 128))))
                                 owner root identity 1 "" nil :read-selected (lambda () nil)))))
                (ok (null (pg::physical-retained-root-current owner)))
                (ok (= source (pg::physical-retained-root-current source-owner)))))))))

(deftest retained-projection-bounds-expanded-history-metadata-before-staging
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (let* ((count 32) (fork (+ 96 (* count 32))) (bytes (overlap-test-bytes 96))
                            (text (store-history-text 1 fork)) (history (pg::timeline-parse-history 2 text))
                            (identity '(:system-id 123 :base-timeline 1 :base 96 :manifest-digest 77))
                            (digest #xCBF29CE484222325) (images nil))
            (dotimes (index count)
              (let* ((start (+ 96 (* index 32))) (raw (legacy-checkpoint-record start))
                                                 (next (pg::physical-store-update-digest digest raw)))
                (setf bytes (concatenate '(vector (unsigned-byte 8)) bytes raw))
                (push (pg::physical-retained-extent-encode
                       (append (list :version 1) identity
                               (list :history-target 2 :history-digest 99 :history-text text
                                     :start start :end (+ start 32) :start-digest digest :end-digest next
                                     :segment 0 :file-timeline 2
                                     :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
                       bytes) images)
                (setf digest next)))
            (let* ((base (overlap-test-bytes 96))
                   (overlap (pg::physical-retained-overlap-encode (overlap-test-metadata 96 base) base))
                   (source (project-test-publish-source root (nreverse images) overlap identity 2 text))
                   (parent-text (with-output-to-string (stream)
                                  (dotimes (index 700) (write-char #\# stream)
                                    (write-string (make-string 999 :initial-element #\x) stream) (terpri stream))))
                   (staged 0) (owner (gensym))
                   (pg::*physical-retained-inventory-test-hook*
                     (lambda (point ignored) (declare (ignore ignored))
                       (when (eq point :after-extent-publication) (incf staged)))))
              (ok (handler-case
                      (progn (pg::call-with-physical-retained-root-update
                              (lambda (initial hold) (declare (ignore initial))
                                (project-test-project-prefix
                                 hold source history 1 parent-text nil fork digest))
                              owner root identity 1 parent-text nil :read-selected (lambda () nil))
                             nil)
                    (error (condition) (search "projection metadata exceeds" (princ-to-string condition)))))
              (ok (< 0 staged count))
              (ok (null (pg::physical-retained-root-current owner))))))))

(deftest retained-projection-uses-eligible-carriers-across-segments
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (let* ((segment pg::+wal-segment-size+) (base (overlap-test-bytes 96))
                                                  (filler (pg::wal-encode-record
                                                           pg::+wal-record-batch-start+ 0 96
                                                           (make-array (- segment 96 pg::+wal-header-size+)
                                                                       :element-type '(unsigned-byte 8) :initial-element 0)))
                                                  (first-bytes (concatenate '(vector (unsigned-byte 8)) base filler))
                                                  (eor (pg::wal-encode-end-of-recovery-record segment 42 2 1 1))
                                                  (checkpoint (legacy-checkpoint-record (+ segment 56)))
                                                  (fork (+ segment 88))
                                                  (future (pg::wal-encode-end-of-recovery-record fork 42 3 2 1))
                                                  (second-bytes (concatenate '(vector (unsigned-byte 8)) eor checkpoint future))
                                                  (parent-text (store-history-text 1 segment))
                                                  (text (concatenate 'string parent-text (store-history-text 2 fork)))
                                                  (history (pg::timeline-parse-history 3 text))
                                                  (identity '(:system-id 123 :base-timeline 1 :base 96 :manifest-digest 77))
                                                  (middle (pg::physical-store-update-digest #xCBF29CE484222325 filler))
                                                  (digest (pg::physical-store-update-digest middle (concatenate '(vector (unsigned-byte 8)) eor checkpoint)))
                                                  (images (loop for data in (list first-bytes second-bytes)
                                                                for start in (list 96 segment) for end in (list segment (+ fork 56))
                                                                for initial in (list #xCBF29CE484222325 middle)
                                                                for carrier in '(1 3) for number from 0
                                                                collect (pg::physical-retained-extent-encode
                                                                         (append (list :version 1) identity
                                                                                 (list :history-target 3 :history-digest 123 :history-text text
                                                                                       :start start :end end :start-digest initial
                                                                                       :end-digest (pg::physical-store-update-digest
                                                                                                    initial (if (zerop number) filler second-bytes))
                                                                                       :segment number :file-timeline carrier
                                                                                       :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 data)))
                                                                         data)))
                                                  (overlap (pg::physical-retained-overlap-encode (overlap-test-metadata 96 base) base))
                                                  (source (project-test-publish-source root images overlap identity 3 text))
                                                  (selected nil))
            (pg::call-with-physical-retained-root-update
             (lambda (initial hold) (declare (ignore initial))
               (setf selected (project-test-project-prefix
                               hold source history 2 parent-text 99 fork digest)))
             (gensym) root identity 2 parent-text nil :read-selected (lambda () selected))
            (let* ((payload (pg::physical-retained-inventory-load
                             root selected identity (pg::timeline-parse-history 2 parent-text)))
                   (entries (getf payload :entries)))
              (ok (equal '(1 2) (mapcar (lambda (entry) (getf (third entry) :file-timeline)) entries)))
              (ok (= fork (getf (third (second entries)) :end)))
              (ok (= digest (getf (third (second entries)) :end-digest))))))))

(deftest retained-projection-skips-wholly-discarded-images-after-registered-validation
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text bytes) (project-test-source 2)
            (multiple-value-bind (metadata ignored) (pg::physical-retained-extent-decode image)
              (declare (ignore ignored))
              (let* ((middle (pg::physical-store-update-digest #xCBF29CE484222325 (subseq bytes 96 160)))
                     (left (copy-list metadata)) (right (copy-list metadata))
                     (prefix (subseq bytes 0 160)))
                (setf (getf left :end) 160 (getf left :end-digest) middle
                      (getf left :prefix-digest) (pg::physical-store-update-digest #xCBF29CE484222325 prefix)
                      (getf right :start) 160 (getf right :start-digest) middle)
                (let* ((images (list (pg::physical-retained-extent-encode left prefix)
                                     (pg::physical-retained-extent-encode right bytes)))
                       (source (project-test-publish-source root images overlap identity 2 text))
                       (discarded (pg::physical-store-update-digest #xCBF29CE484222325 (second images)))
                       (reader (symbol-function 'pg::physical-retained-object-read))
                       (reads nil))
                  (delete-file (pg::physical-retained-object-path root :extent discarded))
                  (unwind-protect
                       (progn
                         (setf (symbol-function 'pg::physical-retained-object-read)
                               (lambda (directory kind id maximum)
                                 (when (eq kind :extent) (push id reads))
                                 (funcall reader directory kind id maximum)))
                         (dolist (fork '(96 128))
                           (setf reads nil)
                           (let ((selected nil))
                             (pg::call-with-physical-retained-root-update
                              (lambda (initial hold) (declare (ignore initial))
                                (setf selected (project-test-project-prefix
                                                hold source history 1 "" nil fork
                                                (pg::physical-store-update-digest #xCBF29CE484222325
                                                                                  (subseq bytes 96 fork)))))
                              (gensym) root identity 1 "" nil :read-selected (lambda () selected))
                             (if (= fork 96) (ok (null reads)) (ok reads))
                             (ok (not (member discarded reads))))))
                    (setf (symbol-function 'pg::physical-retained-object-read) reader)))))))))

(deftest retained-projection-rejects-unregistered-or-mismatched-source-before-extent-io
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (multiple-value-bind (image overlap history identity text ignored) (project-test-source 2)
            (declare (ignore ignored))
            (multiple-value-bind (source source-owner) (project-test-publish-source root image overlap identity 2 text)
              (let ((reader (symbol-function 'pg::physical-retained-object-read)) (reads 0))
                (unwind-protect
                     (progn
                       (setf (symbol-function 'pg::physical-retained-object-read)
                             (lambda (directory kind id maximum)
                               (when (eq kind :extent) (incf reads))
                               (funcall reader directory kind id maximum)))
                       (dolist (case (list (list source nil identity)
                                           (list source (gensym) identity)
                                           (list (logxor 1 source) source-owner identity)
                                           (list source source-owner
                                                 '(:system-id 123 :base-timeline 1 :base 96 :manifest-digest 78))))
                         (ok (intake-fails-p
                              (lambda () (pg::call-with-physical-retained-root-update
                                          (lambda (initial hold) (declare (ignore initial))
                                            (pg::physical-retained-root-project-prefix
                                             hold (first case) history 1 "" nil 96 #xCBF29CE484222325
                                             :source-owner (second case)))
                                          (gensym) root (third case) 1 "" nil :read-selected (lambda () nil))))))
                       (ok (zerop reads)))
                  (setf (symbol-function 'pg::physical-retained-object-read) reader))))))))

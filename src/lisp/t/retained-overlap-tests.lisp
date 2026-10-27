(in-package #:pg-physical-tests)

(defun overlap-test-metadata (base bytes &key (timeline 1) (namespace :legacy))
  (list :version 1 :system-id 123 :base-timeline timeline :base base :manifest-digest 77
        :checkpoint-start (- base 32) :namespace namespace
        :file (list (concatenate 'string "pg_wal/"
                                 (pg::wal-segment-filename (floor (1- base) pg::+wal-segment-size+)
                                                           timeline namespace))
                    (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))

(defun overlap-test-bytes (base)
  (let* ((size (1+ (mod (1- base) pg::+wal-segment-size+)))
         (bytes (make-array size :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes (legacy-checkpoint-record (- base 32)) :start1 (- size 32))
    bytes))

(deftest retained-base-overlap-includes-preceding-segment-at-boundary
    (dolist (base (list 96 pg::+wal-segment-size+ (+ pg::+wal-segment-size+ 96)))
      (let* ((bytes (overlap-test-bytes base))
             (metadata (overlap-test-metadata base bytes))
             (image (pg::physical-retained-overlap-encode metadata bytes)))
        (multiple-value-bind (loaded restored) (pg::physical-retained-overlap-decode image)
          (ok (equal metadata loaded))
          (ok (equalp bytes restored))
          (ok (= (length bytes) (1+ (mod (1- base) pg::+wal-segment-size+)))))
        (ok (intake-fails-p (lambda () (pg::physical-retained-extent-decode image)))))))

(deftest retained-base-overlap-capture-needs-exact-manifest-file
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (base 96) (bytes (overlap-test-bytes base))
             (metadata (overlap-test-metadata base bytes :timeline 2 :namespace :timeline))
             (entry (getf metadata :file))
             (state (list :version 3 :system-id 123 :timeline 2 :checkpoint base :end base :manifest-digest 77
                          :checkpoint-start 64 :wal-namespace :timeline :files (list entry)))
             (path (merge-pathnames (first entry) root)))
        (pg::wal-atomic-write-bytes path bytes)
        (multiple-value-bind (loaded restored) (pg::physical-retained-overlap-capture state root)
          (ok (equal metadata loaded))
          (ok (equalp bytes restored)))
        (delete-file path)
        (pg::wal-atomic-write-bytes (pg::wal-segment-path 0 1 :timeline) bytes)
        (ok (intake-fails-p (lambda () (pg::physical-retained-overlap-capture state root))))
        (sb-posix:symlink (namestring (pg::wal-segment-path 0 1 :timeline)) (namestring path))
        (ok (intake-fails-p (lambda () (pg::physical-retained-overlap-capture state root)))))))

(deftest retained-base-overlap-rejects-invalid-metadata-and-checkpoints
    (let* ((bytes (overlap-test-bytes 96))
           (metadata (overlap-test-metadata 96 bytes)))
      (dolist (change '((:version 2) (:system-id -1) (:base-timeline 0) (:base 95)
                        (:manifest-digest -1) (:checkpoint-start 65) (:namespace :guessed)))
        (let ((bad (copy-tree metadata)))
          (setf (getf bad (first change)) (second change))
          (ok (intake-fails-p (lambda () (pg::physical-retained-overlap-encode bad bytes))))))
      (dolist (key '(:base :unknown))
        (let ((bad (copy-tree metadata)))
          (setf (first bad) key)
          (ok (intake-fails-p (lambda () (pg::physical-retained-overlap-encode bad bytes))))))
      (let* ((bad (copy-seq bytes)))
        (replace bad (sample-physical-record 64) :start1 64)
        (ok (intake-fails-p
             (lambda () (pg::physical-retained-overlap-encode (overlap-test-metadata 96 bad) bad)))))
      (let ((image (pg::physical-retained-overlap-encode metadata bytes)))
        (dolist (offset '(0 8 12 16))
          (let ((bad (copy-seq image)))
            (setf (aref bad offset) (logxor 1 (aref bad offset)))
            (ok (intake-fails-p (lambda () (pg::physical-retained-overlap-decode bad))))))
        (ok (intake-fails-p (lambda () (pg::physical-retained-overlap-decode (subseq image 0 (1- (length image))))))))))

(deftest retained-base-overlap-capture-at-boundary-survives-base-removal
    (with-physical-store-directory (root)
      (let* ((base pg::+wal-segment-size+) (bytes (overlap-test-bytes base))
                                           (metadata (overlap-test-metadata base bytes))
                                           (entry (getf metadata :file))
                                           (state (list :version 1 :system-id 123 :timeline 1 :checkpoint base :end base
                                                        :manifest-digest 77 :files (list entry)))
                                           (path (merge-pathnames (first entry) root)))
        (pg::wal-atomic-write-bytes path bytes)
        (multiple-value-bind (loaded captured) (pg::physical-retained-overlap-capture state root)
          (let ((image (pg::physical-retained-overlap-encode loaded captured)))
            (delete-file path)
            (multiple-value-bind (restored restored-bytes) (pg::physical-retained-overlap-decode image)
              (ok (equal metadata restored))
              (ok (equalp bytes restored-bytes)))))
        (ok (intake-fails-p (lambda () (pg::physical-retained-overlap-capture state root)))))))

(deftest retained-base-overlap-checks-modern-checkpoint-timeline
    (let* ((snapshot (sample-checkpoint-snapshot :timeline 2))
           (record (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 32
                                          (pg::wal-encode-checkpoint-snapshot snapshot)))
           (bytes (make-array 152 :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace bytes record :start1 32)
      (let ((metadata (overlap-test-metadata 152 bytes :timeline 2 :namespace :timeline)))
        (setf (getf metadata :checkpoint-start) 32)
        (ok (eq metadata (pg::physical-retained-overlap-check metadata bytes)))
        (setf (getf metadata :base-timeline) 1
              (first (getf metadata :file)) "pg_wal/000000010000000000000000.wal")
        (ok (intake-fails-p (lambda () (pg::physical-retained-overlap-check metadata bytes)))))))

(cl-spec:defproperty retained-base-overlap-roundtrips
    ((base (range integer 32 1024)) (value (range integer 0 255))
                                    (system-id (range integer 0 18446744073709551615)))
  (:about pg::physical-retained-overlap-encode)
  (:trials (:normal 200))
  (let ((bytes (overlap-test-bytes base)))
    (fill bytes value :end (- base 32))
    (let ((metadata (overlap-test-metadata base bytes)))
      (setf (getf metadata :system-id) system-id)
      (multiple-value-bind (loaded restored)
          (pg::physical-retained-overlap-decode (pg::physical-retained-overlap-encode metadata bytes))
        (and (equal metadata loaded) (equalp bytes restored))))))

(deftest generated-retained-base-overlap-roundtrip
    (let ((data (cl-spec:result-data (cl-spec:run-property 'retained-base-overlap-roundtrips :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest retained-base-overlap-root-protects-object-and-grants-no-logical-coverage
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (let* ((bytes (overlap-test-bytes 96)) (metadata (overlap-test-metadata 96 bytes))
                                                 (image (pg::physical-retained-overlap-encode metadata bytes))
                                                 (identity '(:system-id 123 :base-timeline 1 :base 96 :manifest-digest 77))
                                                 (history (pg::timeline-parse-history 1 ""))
                                                 (owner (gensym)) (selected nil) (entry nil))
            (pg::call-with-physical-retained-root-update
             (lambda (initial hold)
               (declare (ignore initial))
               (setf entry (pg::physical-retained-root-stage-overlap hold 1 "" image))
               (ok (pg::physical-retained-root-protected-p root :overlap (first entry)))
               (ok (null (pg::physical-retained-root-delete-unprotected root :overlap (first entry))))
               (setf selected (pg::physical-retained-root-publish-staged-entries
                               hold 1 "" nil :base-overlap entry))
               (setf selected (pg::physical-retained-root-publish-staged-entries hold 1 "" nil))
               (setf selected (pg::physical-retained-root-update-publish hold 1 "" nil))
               (let* ((store (pg::%make-physical-store :retained-identity identity))
                      (seal (pg::%make-physical-retained-seal :store store :hold hold :target 1 :text "")))
                 (pg::physical-retained-seal-initialize-budget seal)
                 (ok (= (pg::physical-retained-seal-header-bytes seal)
                        (- (pg::physical-retained-inventory-check-text-budget
                            (pg::physical-retained-inventory-payload identity 1 "" nil entry)) 3))))
               selected)
             owner root identity 1 "" nil :read-selected (lambda () selected))
            (ok (equal entry (getf (pg::physical-retained-inventory-load root selected identity history)
                                   :base-overlap)))
            (ok (intake-fails-p
                 (lambda () (pg::physical-retained-inventory-plan root selected identity history 96 97))))
            (ok (intake-fails-p
                 (lambda () (pg::physical-retained-inventory-plan root selected identity history 64 96))))
            (with-retained-root-registry
                (pg::physical-retained-root-register owner root selected identity history)
              (ok (pg::physical-retained-root-protected-p root :overlap (first entry))))
            (let ((replacement (pg::physical-retained-inventory-publish root identity 1 "" nil
                                                                        :previous selected :keep-ids nil)))
              (ok (equal entry (getf (pg::physical-retained-inventory-load root replacement identity history)
                                     :base-overlap))))
            (pg::call-with-physical-retained-root-update
             (lambda (initial hold)
               (declare (ignore hold))
               (setf selected initial))
             owner root identity 1 "" nil :expected-root selected :read-selected (lambda () selected))
            (ok (equal entry (getf (pg::physical-retained-inventory-load root selected identity history)
                                   :base-overlap)))
            (let ((expected selected))
              (setf selected nil)
              (pg::physical-retained-root-retire owner root expected identity (lambda () selected)))
            (ok (pg::physical-retained-root-delete-unprotected root :overlap (first entry)))
            (ok (intake-fails-p (lambda () (pg::physical-retained-inventory-load
                                            root (pg::physical-store-update-digest #xCBF29CE484222325
                                                                                   (pg::physical-retained-inventory-image
                                                                                    (list :version 1 :identity identity :history-target 1
                                                                                          :history-text "" :entries nil :base-overlap entry)))
                                            identity history))))))))

(defun overlap-test-extent (prefix)
  (let* ((raw (legacy-checkpoint-record 96))
         (bytes (concatenate '(vector (unsigned-byte 8)) prefix raw))
         (metadata (list :version 1 :system-id 123 :base-timeline 1 :base 96 :manifest-digest 77
                         :history-target 1 :history-digest nil :history-text ""
                         :start 96 :end 128 :start-digest #xCBF29CE484222325
                         :end-digest (pg::physical-store-update-digest #xCBF29CE484222325 raw)
                         :segment 0 :file-timeline 1
                         :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
    (pg::physical-retained-extent-encode metadata bytes)))

(deftest retained-base-overlap-must-match-logical-extent-prefix
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (let* ((bytes (overlap-test-bytes 96))
                 (image (pg::physical-retained-overlap-encode (overlap-test-metadata 96 bytes) bytes))
                 (identity '(:system-id 123 :base-timeline 1 :base 96 :manifest-digest 77))
                 (owner (gensym)) (selected nil) (bad (copy-seq bytes)))
            (setf (aref bad 0) 1)
            (pg::call-with-physical-retained-root-update
             (lambda (initial hold)
               (declare (ignore initial))
               (let ((overlap (pg::physical-retained-root-stage-overlap hold 1 "" image))
                     (conflicting (pg::physical-retained-root-stage-image hold 1 "" (overlap-test-extent bad)))
                     (matching (pg::physical-retained-root-stage-image hold 1 "" (overlap-test-extent bytes))))
                 (ok (intake-fails-p
                      (lambda () (pg::physical-retained-root-publish-staged-entries
                                  hold 1 "" (list conflicting) :base-overlap overlap))))
                 (let* ((root-image (pg::physical-retained-inventory-image
                                     (pg::physical-retained-inventory-payload
                                      identity 1 "" (list conflicting) overlap)))
                        (id (sb-thread:with-mutex (pg::*physical-retained-inventory-publish-lock*)
                              (pg::physical-retained-object-publish root :inventory root-image
                                                                    pg::+physical-retained-inventory-limit+))))
                   (ok (intake-fails-p (lambda () (pg::physical-retained-inventory-load
                                                   root id identity (pg::timeline-parse-history 1 ""))))))
                 (setf selected (pg::physical-retained-root-publish-staged-entries
                                 hold 1 "" (list matching) :base-overlap overlap))))
             owner root identity 1 "" nil :read-selected (lambda () selected))
            (let ((plan (pg::physical-retained-inventory-plan
                         root selected identity (pg::timeline-parse-history 1 "") 96 128)))
              (ok (= 1 (length plan)))
              (ok (= 96 (getf (first plan) :start)))
              (ok (= #xCBF29CE484222325 (getf (first plan) :start-digest))))))))

(deftest retained-base-overlap-publication-failure-keeps-uncertain-protection
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (let* ((bytes (overlap-test-bytes 96))
                 (image (pg::physical-retained-overlap-encode (overlap-test-metadata 96 bytes) bytes))
                 (id (pg::physical-store-update-digest #xCBF29CE484222325 image))
                 (identity '(:system-id 123 :base-timeline 1 :base 96 :manifest-digest 77))
                 (owner (gensym))
                 (pg::*physical-retained-inventory-test-hook*
                   (lambda (point ignored) (declare (ignore ignored))
                     (when (eq point :after-overlap-publication) (throw 'overlap-cut :cut)))))
            (ok (eq :cut
                    (catch 'overlap-cut
                      (pg::call-with-physical-retained-root-update
                       (lambda (initial hold) (declare (ignore initial))
                         (pg::physical-retained-root-stage-overlap hold 1 "" image))
                       owner root identity 1 "" nil :read-selected (lambda () nil)))))
            (ok (pg::physical-retained-root-protected-p root :overlap id))
            (ok (null (pg::physical-retained-root-delete-unprotected root :overlap id)))
            (pg::physical-retained-root-reconcile owner root identity (pg::timeline-parse-history 1 "")
                                                  (lambda () nil))
            (ok (pg::physical-retained-root-delete-unprotected root :overlap id))))))

(deftest retained-boundary-overlap-root-reopens-without-base-and-rejects-corruption
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (let* ((base pg::+wal-segment-size+) (bytes (overlap-test-bytes base))
                                               (metadata (overlap-test-metadata base bytes)) (entry (getf metadata :file))
                                               (state (list :version 1 :system-id 123 :timeline 1 :checkpoint base :end base
                                                            :manifest-digest 77 :files (list entry)))
                                               (identity (list :system-id 123 :base-timeline 1 :base base :manifest-digest 77))
                                               (owner (gensym)) (selected nil) (overlap nil)
                                               (path (merge-pathnames (first entry) root))
                                               (history (pg::timeline-parse-history 1 "")))
            (pg::wal-atomic-write-bytes path bytes)
            (multiple-value-bind (captured captured-bytes) (pg::physical-retained-overlap-capture state root)
              (let ((image (pg::physical-retained-overlap-encode captured captured-bytes)))
                (pg::call-with-physical-retained-root-update
                 (lambda (initial hold) (declare (ignore initial))
                   (setf overlap (pg::physical-retained-root-stage-overlap hold 1 "" image)
                         selected (pg::physical-retained-root-publish-staged-entries
                                   hold 1 "" nil :base-overlap overlap)))
                 owner root identity 1 "" nil :read-selected (lambda () selected))))
            (delete-file path)
            (with-retained-root-registry
                (pg::physical-retained-root-register owner root selected identity history)
              (ok (pg::physical-retained-root-protected-p root :overlap (first overlap)))
              (let ((payload (pg::physical-retained-inventory-load root selected identity history)))
                (ok (null (getf payload :entries)))
                (ok (= base (second (getf (third (getf payload :base-overlap)) :file)))))
              (ok (intake-fails-p
                   (lambda () (pg::physical-retained-inventory-plan root selected identity history base (1+ base))))))
            (let* ((object (pg::physical-retained-object-path root :overlap (first overlap)))
                   (image (lineage-test-bytes object)))
              (setf (aref image 16) (logxor 1 (aref image 16)))
              (pg::wal-atomic-write-bytes object image)
              (with-retained-root-registry
                  (ok (intake-fails-p (lambda () (pg::physical-retained-root-register
                                                  owner root selected identity history))))
                (ok (null (pg::physical-retained-root-current owner)))))))))

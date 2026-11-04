(in-package #:pg-physical-tests)

(defun reseed-follow-configuration-codec-fixture (phase &optional selected)
  (let* ((core (reseed-follow-codec-fixture (if selected :descendant :same-timeline) :proved))
         (request (getf (getf core :attempt) :request))
         (base (getf core :base)) (generation (first (getf core :generations)))
         (coordinates (getf generation :coordinates)) (anchor (getf generation :anchor))
         (device (getf (getf (getf core :attempt) :local) :device)))
    (when selected
      (setf (getf coordinates :base) 192 (getf coordinates :receive-lsn) 192
            (getf anchor :base) 192 (getf anchor :receive-lsn) 192
            (getf base :checkpoint-start) 160
            (getf base :recovery-control)
            (pg::physical-libpq-octets-datum
             (pg::physical-recovery-control-image 0 (getf request :system-id) 2 0 192 192 42 :state-time 0))
            (getf base :wal-control)
            (pg::physical-libpq-octets-datum
             (pg::wal-encode-control-image
              192 192 nil :timeline 2 :checkpoint-timeline 2 :namespace :timeline
              :metadata (pg::make-wal-control-metadata (getf request :system-id) pg::+db-in-production+ 0)))))
    (setf core (pg::physical-reseed-follow-copy core))
    (let* ((images (sort (append (list (list "follow.pending" 1 17) (list "receive.initial" 68 17)
                                       (list "receive.lock" 0 #xCBF29CE484222325) (list "recovery.initial" 72 17))
                                 (when selected
                                   (list (list "history.receive" 16 17) (list "history.wal" 1 17)
                                         (list "receive.selected" 80 17) (list "recovery.selected" 88 17))))
                         #'string< :key #'first))
           (directories (unless (eq phase :building)
                          (list :lock-inode 100001 :fence-inode 100002
                                :staged (list :device device :inode 100003)
                                :saved (list :device device :inode 100004)
                                :receive (list :device device :inode 100005))))
           (entries (unless (eq phase :building)
                      (loop for (name size digest) in images for inode from 100006
                            collect (list (copy-seq name)
                                          (list (list (copy-seq name) :file #o600 device inode size digest)))))))
      (values
       (list :phase phase :generation 1
             :anchor-digest (pg::physical-follow-anchor-digest
                             (pg::physical-reseed-follow-generation-anchor (first (getf core :generations))))
             :time 12345 :workspace (list :directory (copy-seq "/tmp/configuration-workspace/") :device device :inode 100000)
             :directories directories :images images :entries entries
             :step (if (eq phase :inert) (if selected 5 4) 0))
       core))))

(cl-spec:defproperty reseed-follow-configuration-copy-preserves-its-selected-generation
    ((phase-choice (range integer 0 3)) (selected boolean))
  (:about pg::physical-reseed-follow-configuration-copy)
  (:trials (:normal 80))
  (multiple-value-bind (configuration core)
      (reseed-follow-configuration-codec-fixture (nth phase-choice '(:building :staged :installing :inert)) selected)
    (let* ((copy (pg::physical-reseed-follow-configuration-copy configuration core))
           (directory (copy-seq (getf (getf copy :workspace) :directory)))
           (same (equal configuration copy)))
      (setf (char (getf (getf configuration :workspace) :directory) 1) #\x)
      (and same (string= directory (getf (getf copy :workspace) :directory))))))

(deftest generated-reseed-follow-configuration-copy
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-configuration-copy-preserves-its-selected-generation
                                       :seed 120 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-configuration-copy-rejects-rebinding-and-unsafe-shapes
    (dolist (change '(:generation :anchor :workspace-alias :overlap :lock-alias :image-name :image-size
                      :empty-digest :entry-alias :entry-bytes :building-entries :staged-step :inert-step))
      (multiple-value-bind (value core) (reseed-follow-configuration-codec-fixture :staged t)
        (let ((valid (equal value (pg::physical-reseed-follow-configuration-copy value core))))
          (ok valid))
        (ecase change
          (:generation (setf (getf value :generation) 2))
          (:anchor (incf (getf value :anchor-digest)))
          (:workspace-alias
           (setf (getf (getf value :workspace) :inode) (getf (getf (getf core :attempt) :local) :inode)))
          (:overlap (setf (getf (getf value :workspace) :directory)
                          (getf (getf (getf core :attempt) :local) :directory)))
          (:lock-alias (setf (getf (getf value :directories) :lock-inode)
                             (getf (getf value :workspace) :inode)))
          (:image-name (setf (first (first (getf value :images))) "follow.active"))
          (:image-size (setf (second (assoc "receive.selected" (getf value :images) :test #'equal)) 96))
          (:empty-digest (setf (third (assoc "receive.lock" (getf value :images) :test #'equal)) 0))
          (:entry-alias (setf (fifth (first (second (first (getf value :entries))))) 100003))
          (:entry-bytes (incf (seventh (first (second (first (getf value :entries)))))))
          (:building-entries (setf (getf value :phase) :building))
          (:staged-step (setf (getf value :step) 1))
          (:inert-step (setf (getf value :phase) :inert (getf value :step) 4)))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-configuration-copy value core)))
            (format nil "Reject unsafe configuration ~A" change)))))

(deftest reseed-follow-configuration-transitions-preserve-intent-and-advance-one-prefix
    (dolist (selected '(nil t))
      (multiple-value-bind (building core) (reseed-follow-configuration-codec-fixture :building selected)
        (let* ((staged (reseed-follow-configuration-codec-fixture :staged selected))
               (installing (pg::physical-reseed-follow-configuration-copy staged core))
               (maximum (if selected 5 4)))
          (ok (pg::physical-reseed-follow-configuration-check-transition nil building core))
          (ok (pg::physical-reseed-follow-configuration-check-transition building building core))
          (ok (pg::physical-reseed-follow-configuration-check-transition building staged core))
          (setf (getf installing :phase) :installing)
          (ok (pg::physical-reseed-follow-configuration-check-transition staged installing core))
          (loop for step from 1 to maximum
                do (let ((next (pg::physical-reseed-follow-configuration-copy installing core)))
                     (setf (getf next :step) step)
                     (ok (pg::physical-reseed-follow-configuration-check-transition installing next core))
                     (setf installing next)))
          (let ((inert (pg::physical-reseed-follow-configuration-copy installing core)))
            (setf (getf inert :phase) :inert)
            (ok (pg::physical-reseed-follow-configuration-check-transition installing inert core))
            (ok (pg::physical-reseed-follow-configuration-check-transition inert inert core)))))))

(deftest reseed-follow-configuration-transitions-refuse-intent-rebinding-and-skips
    (multiple-value-bind (building core) (reseed-follow-configuration-codec-fixture :building t)
      (let ((staged (reseed-follow-configuration-codec-fixture :staged t)))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-configuration-check-transition building nil core))))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-configuration-check-transition nil staged core))))
        (dolist (change '(:workspace :images :time :identity :entry :skip :early-inert :direct-inert))
          (let* ((before (pg::physical-reseed-follow-configuration-copy
                          (if (member change '(:workspace :images :time)) building staged) core))
                 (after (pg::physical-reseed-follow-configuration-copy before core)))
            (ecase change
              (:workspace (setf (getf (getf after :workspace) :directory) "/tmp/configuration-other/"))
              (:images (incf (third (first (getf after :images)))))
              (:time (incf (getf after :time)))
              (:identity (setf (getf after :phase) :installing
                               (getf (getf after :directories) :lock-inode) 100020))
              (:entry (setf (getf after :phase) :installing
                            (fifth (first (second (first (getf after :entries))))) 100030))
              (:skip (setf (getf before :phase) :installing (getf after :phase) :installing
                           (getf after :step) 2))
              (:early-inert (setf (getf before :phase) :installing (getf after :phase) :inert
                                  (getf after :step) 5))
              (:direct-inert (setf (getf after :phase) :inert (getf after :step) 5)))
            (ok (intake-fails-p
                 (lambda () (pg::physical-reseed-follow-configuration-check-transition before after core)))
                (format nil "Reject configuration transition ~A" change)))))))

(deftest reseed-follow-configuration-journal-roundtrips-and-freezes-generations
    (dolist (selected '(nil t))
      (multiple-value-bind (configuration core) (reseed-follow-configuration-codec-fixture :building selected)
        (let* ((record (append core (list :configuration configuration)))
               (roundtrip (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image record))))
          (let ((same (equal record roundtrip))) (ok same))
          (ok (pg::physical-reseed-follow-check-transition core record))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-check-transition record core))))
          (let ((preparing (pg::physical-reseed-follow-copy core)))
            (setf (getf (first (getf preparing :generations)) :phase) :preparing
                  (getf (first (getf preparing :generations)) :anchor) nil)
            (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-check-transition preparing record)))))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-copy (append core (list :configuration nil))))))))))

(deftest reseed-follow-configuration-journal-is-fenced-from-the-legacy-follow-owner
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (with-physical-store-directory (workspace)
         (let ((core nil) (configured nil) (entered nil))
           (pg::call-with-physical-reseed-follow-owner
            root
            (lambda (owner)
              (setf core (reseed-follow-proof-test-record
                          (pg::physical-reseed-follow-initial-record owner) root :same-timeline))
              (pg::call-with-physical-reseed-follow-image
               owner
               (lambda (fd check)
                 (declare (ignore check))
                 (multiple-value-bind (images binding)
                     (pg::physical-reseed-follow-configuration-images-at owner fd core 12345)
                   (let ((stat (sb-posix:lstat (namestring workspace))))
                     (setf configured
                           (append core
                                   (list :configuration
                                         (list :phase :building :generation 1
                                               :anchor-digest (pg::physical-follow-anchor-digest (getf binding :anchor))
                                               :time 12345 :workspace
                                               (list :directory (namestring workspace)
                                                     :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat))
                                               :directories nil :entries nil :step 0
                                               :images (pg::physical-reseed-follow-configuration-image-specifications
                                                        images core)))))))))))
           (setf configured (pg::physical-reseed-follow-copy configured))
           (let ((path (merge-pathnames "physical-reseed.follow" root))
                 (temporary (merge-pathnames "replacement-configuration" root)))
             (reseed-follow-owner-test-write path (pg::physical-reseed-follow-image core))
             (pg::call-with-physical-reseed-follow-owner
              root (lambda (owner)
                     (let ((same (equal core (pg::physical-reseed-follow-read owner)))) (ok same))))
             (let* ((pending (merge-pathnames "physical-reseed.follow.tmp" root))
                    (bytes (pg::physical-reseed-follow-image configured)))
               (reseed-follow-owner-test-write pending bytes)
               (let ((facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring pending)))))
                 (pg::call-with-physical-reseed-follow-owner
                  root (lambda (owner)
                         (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-intent owner))))
                         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))
                 (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring pending))))))
               (ok (equalp bytes (lineage-test-bytes pending)))
               (delete-file pending))
             (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image configured))
             (sb-posix:rename (namestring temporary) (namestring path))
             (pg::fsync-directory root)
             (ok (intake-fails-p
                  (lambda () (pg::call-with-physical-reseed-follow-owner
                              root (lambda (owner) (declare (ignore owner)) (setf entered t))))))
             (ok (not entered))
             (let ((same (equal configured (pg::physical-reseed-follow-decode (lineage-test-bytes path)))))
               (ok same))))))))

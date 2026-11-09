(in-package #:pg-physical-tests)

(defun reseed-follow-retirement-codec-fixture (selected)
  (multiple-value-bind (record activation) (reseed-follow-activation-codec-fixture selected)
    (let* ((record (reseed-follow-with-activation record (car (last activation))))
           (bytes (pg::physical-reseed-attempt-image (getf record :attempt)))
           (device (getf (getf (getf record :attempt) :local) :device))
           (reference (list :identity (list device 300000 #o100600 1000 1 (length bytes) 0 0)
                            :digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
      (values record
              (loop for (phase step) in '((:retiring 0) (:retiring 1) (:retiring 2) (:retired 2))
                    collect (list :phase phase :step step
                                  :attempt-file (pg::physical-adoption-attempt-value-copy reference)))))))

(defun reseed-follow-with-retirement (record group)
  (append (pg::physical-reseed-follow-copy record)
          (when group (list :retirement (pg::physical-adoption-attempt-value-copy group)))))

(cl-spec:defproperty reseed-follow-retirement-next-preserves-predecessors
    ((selected boolean) (position (range integer 0 3)))
  (:about pg::physical-reseed-follow-retirement-next-record)
  (:trials (:normal 100))
  (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
    (let* ((expected (reseed-follow-with-retirement record (nth position groups)))
           (previous (reseed-follow-with-retirement record (when (plusp position) (nth (1- position) groups))))
           (reference (getf (nth position groups) :attempt-file))
           (before (pg::physical-adoption-attempt-value-copy (list previous reference)))
           (candidate (funcall (symbol-function 'pg::physical-reseed-follow-retirement-next-record)
                               previous reference)))
      (and (equal expected candidate)
           (not (eq previous candidate))
           (pg::physical-reseed-follow-check-transition previous candidate)
           (equal candidate (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image candidate)))
           (progn
             (incf (seventh (getf (getf (getf candidate :retirement) :attempt-file) :identity)))
             (equal before (list previous reference)))))))

(deftest generated-reseed-follow-retirement-next-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-retirement-next-preserves-predecessors
                                       :seed 144 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-retirement-next-refuses-terminal-inactive-and-rebound-inputs
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
        (dolist (kind '(:retired :missing :digest :rebound :inactive :incomplete))
          (let* ((previous (reseed-follow-with-retirement
                            record (case kind (:retired (fourth groups)) (:rebound (first groups)))))
                 (reference (pg::physical-adoption-attempt-value-copy (getf (first groups) :attempt-file))))
            (ecase kind
              (:retired)
              (:missing (setf reference nil))
              (:digest (incf (getf reference :digest)))
              (:rebound (incf (seventh (getf reference :identity))))
              (:inactive (setf (getf (getf previous :activation) :phase) :activating))
              (:incomplete (setf (getf (getf previous :activation) :phase) :activating
                                 (getf (getf previous :activation) :step) 1)))
            (let ((before (pg::physical-adoption-attempt-value-copy (list previous reference))))
              (ok (handler-case
                      (progn
                        (funcall (symbol-function 'pg::physical-reseed-follow-retirement-next-record)
                                 previous reference)
                        nil)
                    (error () t)))
              (ok (equal before (list previous reference)))))))))

(deftest reseed-follow-retirement-next-copies-an-aliased-attempt-reference
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
        (dolist (group (butlast groups))
          (let* ((previous (reseed-follow-with-retirement record group))
                 (reference (getf (getf previous :retirement) :attempt-file))
                 (before (pg::physical-adoption-attempt-value-copy (list previous reference)))
                 (candidate (pg::physical-reseed-follow-retirement-next-record previous reference))
                 (copied (getf (getf candidate :retirement) :attempt-file)))
            (ok (not (eq reference copied)))
            (incf (seventh (getf copied :identity)))
            (ok (equal before (list previous reference))))))))

(cl-spec:defproperty reseed-follow-retirement-copies-preserve-authority
    ((selected boolean) (position (range integer 0 3)))
  (:about pg::physical-reseed-follow-retirement-copy)
  (:trials (:normal 100))
  (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
    (let* ((group (nth position groups))
           (before (pg::physical-adoption-attempt-value-copy (list record group)))
           (copy (funcall (symbol-function 'pg::physical-reseed-follow-retirement-copy) group record))
           (journal (reseed-follow-with-retirement record copy)))
      (and (equal group copy)
           (equal journal (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image journal)))
           (progn
             (incf (seventh (getf (getf copy :attempt-file) :identity)))
             (equal before (list record group)))))))

(deftest generated-reseed-follow-retirement-codec-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-retirement-copies-preserve-authority
                                       :seed 129 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-retirement-keeps-absent-encoding-and-exact-phases
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
        (ok (equalp (pg::physical-reseed-follow-image record)
                    (pg::physical-backup-lease-encode-literal
                     "PGRESEEDFOLLOW1" record pg::+physical-reseed-follow-maximum-bytes+)))
        (dolist (group groups)
          (let ((journal (reseed-follow-with-retirement record group)))
            (ok (equal journal (pg::physical-reseed-follow-decode
                                (pg::physical-reseed-follow-image journal))))))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-copy (append record (list :retirement nil))) nil)
              (error () t)))
        (dolist (phase '(:retiring :retired))
          (dotimes (step 4)
            (let ((group (pg::physical-adoption-attempt-value-copy (first groups))))
              (setf (getf group :phase) phase (getf group :step) step)
              (ok (eq (not (null (if (eq phase :retiring) (<= step 2) (= step 2))))
                      (handler-case
                          (progn (funcall (symbol-function 'pg::physical-reseed-follow-retirement-copy)
                                          group record) t)
                        (error () nil))))))))))

(deftest reseed-follow-retirement-transitions-advance-one-archive
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
        (loop for old from -1 to 3
              do (loop for new from -1 to 3
                       for expected = (or (= old new) (= new (1+ old)))
                       for accepted = (handler-case
                                          (progn
                                            (pg::physical-reseed-follow-check-transition
                                             (reseed-follow-with-retirement record (when (>= old 0) (nth old groups)))
                                             (reseed-follow-with-retirement record (when (>= new 0) (nth new groups))))
                                            t)
                                        (error () nil))
                       do (ok (eq expected accepted)))))))

(deftest reseed-follow-retirement-binds-the-original-attempt-bytes
    (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture nil)
      (dolist (kind '(:size :digest :shape))
        (let* ((group (pg::physical-adoption-attempt-value-copy (first groups)))
               (reference (getf group :attempt-file)))
          (ecase kind
            (:size (incf (sixth (getf reference :identity))))
            (:digest (setf (getf reference :digest) (logxor 1 (getf reference :digest))))
            (:shape (setf (getf reference :identity) (list 1 2 3))))
          (ok (handler-case
                  (progn (funcall (symbol-function 'pg::physical-reseed-follow-retirement-copy)
                                  group record) nil)
                (error () t)))))))

(deftest reseed-follow-retirement-checks-identity-bounds-and-visible-aliases
    (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture t)
      (let* ((local (getf (getf record :attempt) :local))
             (config (getf record :configuration)) (directories (getf config :directories))
             (active (getf (getf record :activation) :entry))
             (inodes (append (list (getf local :inode) (getf local :lock-inode)
                                   (getf (getf (getf record :attempt) :stage) :inode)
                                   (second (getf (getf record :install) :identity))
                                   (getf (getf config :workspace) :inode)
                                   (getf directories :lock-inode) (getf directories :fence-inode)
                                   (fifth active))
                             (loop for role in '(:staged :saved :receive)
                                   collect (getf (getf directories role) :inode))
                             (loop for entry in (getf config :entries)
                                   collect (fifth (first (second entry)))))))
        (dolist (case (append (loop for inode in inodes collect (list 1 inode))
                              (list (list 0 -1) (list 0 (1+ (getf local :device)))
                                    (list 1 0) (list 1 (ash 1 64))
                                    (list 2 #o100644) (list 4 2) (list 3 -1)
                                    (list 3 (ash 1 64)) (list 5 0)
                                    (list 6 (1- (- (ash 1 63)))) (list 6 (ash 1 63))
                                    (list 7 (1- (- (ash 1 63)))) (list 7 (ash 1 63)))))
          (let* ((group (pg::physical-adoption-attempt-value-copy (first groups)))
                 (identity (getf (getf group :attempt-file) :identity)))
            (setf (nth (first case) identity) (second case))
            (ok (handler-case
                    (progn (funcall (symbol-function 'pg::physical-reseed-follow-retirement-copy)
                                    group record) nil)
                  (error () t)))))
        (let* ((group (pg::physical-adoption-attempt-value-copy (first groups)))
               (identity (getf (getf group :attempt-file) :identity)))
          (setf (second identity) (1- (ash 1 64))
                (fourth identity) (1- (ash 1 64))
                (seventh identity) (- (ash 1 63)) (eighth identity) (1- (ash 1 63)))
          (ok (funcall (symbol-function 'pg::physical-reseed-follow-retirement-copy) group record))))))

(deftest reseed-follow-retirement-freezes-attempt-reference-and-activation
    (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture nil)
      (let* ((old (reseed-follow-with-retirement record (first groups)))
             (new (reseed-follow-with-retirement record (second groups))))
        (incf (seventh (getf (getf (getf new :retirement) :attempt-file) :identity)))
        (ok (pg::physical-reseed-follow-copy new))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-check-transition old new) nil)
              (error () t))))
      (let ((before (pg::physical-adoption-attempt-value-copy record))
            (after (reseed-follow-with-retirement record (first groups))))
        (setf (getf (getf before :activation) :phase) :activating)
        (ok (pg::physical-reseed-follow-copy before))
        (ok (pg::physical-reseed-follow-copy after))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-check-transition before after) nil)
              (error () t))))))

(deftest reseed-follow-retirement-keeps-uid-and-link-count-distinct
    (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture nil)
      (dolist (uid (list 0 1 1000 (1- (ash 1 64))))
        (let* ((group (pg::physical-adoption-attempt-value-copy (first groups)))
               (identity (getf (getf group :attempt-file) :identity)))
          (setf (fourth identity) uid (fifth identity) 1)
          (ok (pg::physical-reseed-follow-retirement-copy group record))))
      (let* ((group (pg::physical-adoption-attempt-value-copy (first groups)))
             (identity (getf (getf group :attempt-file) :identity)))
        (setf (fourth identity) 1 (fifth identity) 1000)
        (ok (handler-case
                (progn (pg::physical-reseed-follow-retirement-copy group record) nil)
              (error () t))))))

(deftest reseed-follow-retirement-matches-the-real-attempt-identity-producer
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let* ((record (pg::physical-reseed-follow-read owner))
              (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
              (successor (pg::physical-reseed-follow-settlement-successor record))
              (device (getf (getf (getf record :attempt) :local) :device)))
         (setf (getf record :settlement)
               (list :phase :handed-off :successor successor
                     :receipt (list :version 1 :request (getf (getf record :attempt) :request)
                                    :phase :closed :base (pg::physical-reseed-follow-settlement-base record)
                                    :outcome :handed-off :successor successor))
               (getf record :activation)
               (list :phase :active :step 2 :image (list 1 17)
                     :entry (list "follow.active" :file #o600 device (1- (ash 1 64)) 1 17)))
         (setf record (pg::physical-reseed-follow-copy record))
         (with-open-file (input (merge-pathnames "physical-reseed.attempt" root)
                                :direction :input :element-type '(unsigned-byte 8))
           (let* ((stat (sb-posix:fstat (sb-sys:fd-stream-fd input)))
                  (identity (pg::physical-reseed-attempt-file-identity stat))
                  (bytes (make-array (sb-posix:stat-size stat) :element-type '(unsigned-byte 8))))
             (ok (= (length bytes) (read-sequence bytes input)))
             (ok (= (sb-posix:stat-uid stat) (fourth identity)))
             (ok (= 1 (fifth identity)))
             (let ((group (list :phase :retiring :step 0
                                :attempt-file (list :identity identity
                                                    :digest (pg::physical-store-update-digest
                                                             #xCBF29CE484222325 bytes)))))
               (ok (equal group (pg::physical-reseed-follow-retirement-copy group record))))
             (ok (equal identity (pg::physical-reseed-attempt-file-identity
                                  (sb-posix:fstat (sb-sys:fd-stream-fd input)))))))
         (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-follow-retirement-refuses-incomplete-activation
    (multiple-value-bind (active groups) (reseed-follow-retirement-codec-fixture nil)
      (multiple-value-bind (record activation) (reseed-follow-activation-codec-fixture nil)
        (dolist (state (butlast activation))
          (let ((other (reseed-follow-with-activation record state)))
            (ok (pg::physical-reseed-follow-copy other))
            (ok (handler-case
                    (progn (pg::physical-reseed-follow-retirement-copy (first groups) other) nil)
                  (error () t)))))
        (ok (equal active (reseed-follow-with-activation record (car (last activation))))))
      (remf active :activation)
      (ok (pg::physical-reseed-follow-copy active))
      (ok (handler-case
              (progn (pg::physical-reseed-follow-retirement-copy (first groups) active) nil)
            (error () t)))))

(cl-spec:defproperty reseed-follow-copy-preserves-mutable-predecessor-snapshots
    ((selected boolean) (position (range integer 0 3))
                        (time (range integer -9223372036854775808 9223372036854775807)))
  (:about pg::physical-reseed-follow-copy)
  (:trials (:normal 80))
  (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
    (let ((journal (reseed-follow-with-retirement record (nth position groups))))
      (setf (getf (getf journal :configuration) :time) time)
      (let* ((before (pg::physical-adoption-attempt-value-copy journal))
             (first-copy (pg::physical-reseed-follow-copy journal))
             (second-copy (pg::physical-reseed-follow-copy journal)))
        (and (equal before first-copy)
             (equal before second-copy)
             (progn
               (setf (char (getf (getf (getf first-copy :configuration) :workspace) :directory) 0) #\X)
               (and (equal before journal) (equal before second-copy))))))))

(deftest generated-reseed-follow-copy-predecessor-snapshots
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-copy-preserves-mutable-predecessor-snapshots
                                       :seed 232 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-copy-bounds-retirement-prefix-validation
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
        (dolist (group groups)
          (let* ((journal (reseed-follow-with-retirement record group))
                 (original (symbol-function 'pg::physical-reseed-follow-core-copy))
                 (calls 0)
                 (copy nil))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-reseed-follow-core-copy)
                         (lambda (value) (incf calls) (funcall original value)))
                   (setf copy (pg::physical-reseed-follow-copy journal)))
              (setf (symbol-function 'pg::physical-reseed-follow-core-copy) original))
            (ok (equal journal copy))
            (ok (and (plusp calls) (<= calls 2))
                (format nil "canonical predecessor core copied ~d times" calls)))))))

(deftest reseed-follow-public-group-copiers-keep-predecessor-validation
    (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture nil)
      (let ((journal (reseed-follow-with-retirement record (car (last groups)))))
        (flet ((rejected (function group predecessor)
                 (handler-case (progn (funcall function group predecessor) nil)
                   (error () t))))
          (dolist (entry '((:configuration . pg::physical-reseed-follow-configuration-copy)
                           (:settlement . pg::physical-reseed-follow-settlement-copy)
                           (:activation . pg::physical-reseed-follow-activation-copy)
                           (:retirement . pg::physical-reseed-follow-retirement-copy)))
            (let* ((key (car entry))
                   (function (symbol-function (cdr entry)))
                   (group (getf journal key))
                   (predecessor (if (eq key :configuration)
                                    (pg::physical-reseed-follow-core-projection journal)
                                    journal))
                   (changed (copy-list predecessor))
                   (missing (copy-list predecessor))
                   (cycle (copy-list predecessor)))
              (setf (getf changed :version) 2
                    (cdr (last cycle)) cycle)
              (remf missing (ecase key
                              (:configuration :attempt) (:settlement :configuration)
                              (:activation :settlement) (:retirement :activation)))
              (ok (rejected function group changed))
              (ok (rejected function group missing))
              (ok (rejected function group cycle))
              (ok (rejected function (append group (list :unknown t)) predecessor))
              (ok (handler-case (progn (pg::physical-reseed-follow-copy missing) nil)
                    (error () t)))))))))

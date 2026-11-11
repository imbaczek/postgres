(in-package #:pg-physical-tests)

(defun reseed-install-codec-fixture (&optional (old-count 2))
  (let* ((attempt (reseed-attempt-test-record))
         (request (getf attempt :request))
         (manifest (list :version 3 :system-id (getf request :system-id)
                         :cluster-identity (copy-seq (getf request :cluster-identity))
                         :timeline (getf request :timeline) :checkpoint 152 :checkpoint-start 32 :end 152
                         :next-xid 3 :source-pin (getf request :name)
                         :files (append (list (list (copy-seq "PG_VERSION") 3 111)
                                              (list (copy-seq "global/pg_control") 196 222))
                                        (when (> (getf request :timeline) 1)
                                          (list (list (concatenate 'string "pg_wal/"
                                                                   (pg::timeline-history-filename (getf request :timeline)))
                                                      1 333))))))
         (entries (append '(("base" nil nil) ("global" nil nil) ("pg_wal" nil nil))
                          (loop for spec in (pg::physical-backup-metadata-bounds)
                                for name = (first spec)
                                collect (list name (if (equal name "physical-backup.manifest") 100 1)
                                              (if (equal name "physical-backup.manifest") 991 33)))
                          (copy-list (getf manifest :files)))))
    (setf (getf attempt :stage) (list :directory (copy-seq "/tmp/reseed-codec-stage/") :device 1 :inode 4)
          attempt (pg::physical-reseed-attempt-copy attempt))
    (list :version 1 :attempt attempt :attempt-file (list :device 1 :inode 12)
          :incoming (list :directory (copy-seq "/tmp/reseed-codec-incoming/") :device 1 :inode 5 :lock-inode 8 :fence-inode 9)
          :quarantine (list :directory (copy-seq "/tmp/reseed-codec-quarantine/") :device 1 :inode 6 :lock-inode 10 :fence-inode 11)
          :journal (list :device 1 :inode 7)
          :manifest manifest :manifest-digest 991
          :old (loop for index below old-count
                     collect (list (format nil "old-~4,'0d" index) 1 (+ 1000 index) #o100600 1 1000 1 20 30))
          :new (loop for (name size digest) in (sort entries #'string< :key #'first)
                     for inode from 100
                     collect (list (copy-seq name) 1 inode (if size #o100600 #o040700) (if size 1 2)
                                   1000 (or size 4096) 20 30 digest)))))

(cl-spec:defproperty reseed-install-storage-shapes-preserve-unicode-progress
    ((shape (member :simple :adjustable :fill-pointer :displaced))
     (unicode (member nil t)) (old-count (range integer 0 4))
     (index (range integer 0 200)) (cut (range integer 0 50)))
  (:about pg::physical-reseed-install-decode)
  (:trials (:normal 100))
  (let ((plan (reseed-install-codec-fixture old-count)))
    (when unicode
      (setf (getf (getf (getf plan :attempt) :local) :directory)
            "/tmp/reseed-attempt-λ-🦉/"))
    (let* ((last (pg::physical-reseed-install-last-step plan))
           (step (mod index last))
           (before (pg::physical-reseed-install-image plan step))
           (after (pg::physical-reseed-install-image plan (1+ step)))
           (prefix (mod cut (- (length after) (length before))))
           (image (subseq after 0 (+ (length before) prefix)))
           (bytes (metadata-codec-test-storage image shape))
           (state (pg::physical-reseed-install-decode bytes)))
      (fill bytes 0)
      (and (equal plan (getf state :plan))
           (= step (getf state :step))
           (= (length before) (getf state :valid-length))
           (eq (plusp prefix) (not (null (getf state :partial))))))))

(deftest reseed-install-generated-storage-and-unicode-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-install-storage-shapes-preserve-unicode-progress
                                       :seed 315 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty reseed-install-codec-preserves-plan-and-next-step-prefix
    ((old-count (range integer 0 5)) (index (range integer 0 200)) (cut (range integer 0 25)))
  (:about pg::physical-reseed-install-image pg::physical-reseed-install-decode)
  (:trials (:normal 100))
  (let* ((plan (reseed-install-codec-fixture old-count))
         (last (pg::physical-reseed-install-last-step plan))
         (step (mod index last))
         (before (pg::physical-reseed-install-image plan step))
         (after (pg::physical-reseed-install-image plan (1+ step)))
         (prefix (mod cut (- (length after) (length before))))
         (partial (subseq after 0 (+ (length before) prefix)))
         (state (pg::physical-reseed-install-decode partial)))
    (and (equal plan (getf state :plan)) (= step (getf state :step))
         (= (length before) (getf state :valid-length))
         (eq (plusp prefix) (not (null (getf state :partial)))))))

(deftest generated-reseed-install-plan-and-progress-prefixes
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-install-codec-preserves-plan-and-next-step-prefix
                                       :seed 98 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest reseed-install-codec-roundtrips-every-progress-step-and-owns-copies
    (let* ((plan (reseed-install-codec-fixture))
           (checked (pg::physical-reseed-install-copy plan)))
      (loop for step from 0 to (pg::physical-reseed-install-last-step checked)
            for state = (pg::physical-reseed-install-decode (pg::physical-reseed-install-image plan step))
            do (ok (equal checked (getf state :plan)))
               (ok (= step (getf state :step)))
               (ok (not (getf state :partial))))
      (setf (char (getf (getf plan :incoming) :directory) 5) #\X
            (char (first (first (getf plan :new))) 0) #\X
            (char (getf (getf plan :manifest) :cluster-identity) 0) #\X)
      (ok (not (equal plan checked)))
      (ok (equal checked (pg::physical-reseed-install-copy (reseed-install-codec-fixture))))))

(deftest reseed-install-codec-binds-workspaces-source-inventory-and-private-identities
    (dolist (kind '(:phase :legacy :overlap :device :alias-root :alias-guard :alias-journal
                    :source :manifest-digest :missing :extra :unordered :file-alias :mode
                    :link-count :content-size :content-digest :directory-digest :old-management-alias :old-attempt-alias))
      (let ((plan (reseed-install-codec-fixture)))
        (ecase kind
          (:phase (setf (getf (getf plan :attempt) :phase) :aborting))
          (:legacy (remf (getf plan :attempt) :stage))
          (:overlap (setf (getf (getf plan :quarantine) :directory) "/tmp/reseed-codec-incoming/quarantine/"))
          (:device (incf (getf (getf plan :incoming) :device)))
          (:alias-root (setf (getf (getf plan :incoming) :inode) 2))
          (:alias-guard (setf (getf (getf plan :incoming) :lock-inode) 9))
          (:alias-journal (setf (getf (getf plan :journal) :inode) 8))
          (:source (setf (getf (getf plan :manifest) :source-pin) "another_source"))
          (:manifest-digest (incf (getf plan :manifest-digest)))
          (:missing (pop (getf plan :new)))
          (:extra (push '("unexpected" 1 9999 33152 1 1000 1 20 30 33) (getf plan :new)))
          (:unordered (setf (getf plan :new) (reverse (getf plan :new))))
          (:file-alias (setf (third (second (getf plan :new))) (third (first (getf plan :new)))))
          (:mode (setf (fourth (first (getf plan :new))) #o100644))
          (:link-count (setf (fifth (first (getf plan :new))) 2))
          (:content-size (incf (seventh (assoc "PG_VERSION" (getf plan :new) :test #'equal))))
          (:content-digest (incf (tenth (assoc "PG_VERSION" (getf plan :new) :test #'equal))))
          (:directory-digest (setf (tenth (assoc "base" (getf plan :new) :test #'equal)) 1))
          (:old-management-alias (setf (third (first (getf plan :old))) 8))
          (:old-attempt-alias (setf (third (first (getf plan :old))) 12)))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-install-image plan 0))) (format nil "~s" kind)))))

(deftest reseed-install-codec-allows-consistent-old-hardlinks-and-checks-original-pid
    (let ((plan (reseed-install-codec-fixture)))
      (setf (third (second (getf plan :old))) (third (first (getf plan :old)))
            (fifth (first (getf plan :old))) 2
            (fifth (second (getf plan :old))) 2)
      (ok (equal plan (pg::physical-reseed-install-copy plan)))
      (incf (seventh (second (getf plan :old))))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-install-copy plan)))))
  (let* ((plan (reseed-install-codec-fixture))
         (bytes (pg::string->bytes (format nil "-12345~%opaque PID evidence~%")))
         (identity (list 1 999 #o100600 1 1000 (length bytes) 0 0)))
    (setf (getf (getf (getf plan :attempt) :local) :pid)
          (list :pid 12345 :identity identity :bytes (pg::physical-libpq-octets-datum bytes))
          (getf plan :old) (append (getf plan :old) (list (append (list "postmaster.pid") identity))))
    (ok (equal plan (pg::physical-reseed-install-copy plan)))
    (setf (getf plan :old) (butlast (getf plan :old)))
    (ok (intake-fails-p (lambda () (pg::physical-reseed-install-copy plan)))))
  (let ((plan (reseed-install-codec-fixture)))
    (setf (getf plan :old)
          (append (getf plan :old) '(("postmaster.pid" 1 999 33152 1 1000 1 0 0))))
    (ok (intake-fails-p (lambda () (pg::physical-reseed-install-copy plan))))))

(deftest reseed-install-codec-refuses-cycles-corruption-and-noncanonical-body
    (let ((plan (reseed-install-codec-fixture)))
      (setf (getf plan :old) plan)
      (ok (intake-fails-p (lambda () (pg::physical-reseed-install-copy plan)))))
  (let* ((plan (reseed-install-codec-fixture))
         (image (pg::physical-reseed-install-image plan 0))
         (bad (copy-seq image)))
    (setf (aref bad (1- (length bad))) 0)
    (ok (intake-fails-p (lambda () (pg::physical-reseed-install-decode bad))))
    (dolist (suffix '("garbage" "STEP 2 00000000" "STEP 1 FFFFFFFF"))
      (ok (intake-fails-p
           (lambda () (pg::physical-reseed-install-decode
                       (concatenate '(vector (unsigned-byte 8)) image (pg::string->bytes suffix)))))))
    (let* ((text (concatenate 'string " " (pg::physical-bootstrap-manifest-text plan)))
           (body (pg::string->bytes text))
           (header (pg::string->bytes (format nil "PGRESEEDINSTALL1 ~d ~8,'0X~%" (length body) (pg::wal-crc32 body)))))
      (ok (intake-fails-p
           (lambda () (pg::physical-reseed-install-decode
                       (concatenate '(vector (unsigned-byte 8)) header body #(10)))))))
    (ok (intake-fails-p
         (lambda () (pg::physical-reseed-install-decode
                     (concatenate '(vector (unsigned-byte 8))
                                  (pg::physical-reseed-install-image plan (pg::physical-reseed-install-last-step plan))
                                  (pg::string->bytes "S"))))))))

(deftest reseed-install-decoder-bounds-progress-limit-traversals
    (let* ((plan (reseed-install-codec-fixture 32))
           (last (pg::physical-reseed-install-last-step plan))
           (image (pg::physical-reseed-install-image plan last))
           (original (symbol-function 'pg::physical-reseed-install-last-step))
           (calls 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-install-last-step)
                   (lambda (value) (incf calls) (funcall original value)))
             (ok (= last (getf (pg::physical-reseed-install-decode image) :step)))
             (ok (<= calls 2)))
        (setf (symbol-function 'pg::physical-reseed-install-last-step) original))))

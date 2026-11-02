(in-package #:pg-physical-tests)

(defun adoption-install-codec-fixture (&optional (old-count 2))
  (let* ((attempt (pg::physical-adoption-attempt-copy (adoption-attempt-codec-fixture :phase :proved)))
         (names '("PG_VERSION" "authentication.initialized" "base" "global" "pg_hba.conf" "pg_ident.conf"
                  "pg_wal" "pg_xact" "physical-backup.manifest" "physical-recovery.control" "recovery.required")))
    (flet ((tree (start)
             (loop for name in names for inode from start
                   collect (list name (list (if (member name '("base" "global" "pg_wal") :test #'equal)
                                                (list name :directory #o700 1 inode 0 0)
                                                (list name :file #o600 1 inode 0 #xCBF29CE484222325)))))))
      (list :version 1 :attempt attempt
            :workspace (list :directory "/tmp/adoption-codec-work/" :device 1 :inode 5 :lock-inode 15 :fence-inode 16)
            :incoming (list :device 1 :inode 6 :lock-inode 17 :fence-inode 18) :quarantine (list :device 1 :inode 7 :lock-inode 19 :fence-inode 20)
            :journal (list :device 1 :inode 8)
            :old (loop for index below old-count for name = (format nil "old-~4,'0d" index)
                       collect (list name (list (list name :file #o600 1 (+ 1000 index) 1 42))))
            :stage-tree (tree 2000) :new (tree 3000)))))

(deftest adoption-install-codec-roundtrips-the-immutable-plan-and-progress
    (let* ((plan (adoption-install-codec-fixture))
           (last (+ 1 (length (getf plan :old)) (length (getf plan :new)))))
      (loop for step from 0 to last
            for state = (pg::physical-adoption-install-decode (pg::physical-adoption-install-image plan step))
            do (ok (equal plan (getf state :plan)))
               (ok (= step (getf state :step)))
               (ok (not (getf state :partial))))))

(deftest adoption-install-codec-rejects-mount-caches-aliases-and-escaping-plans
    (dolist (mode '(:mount :alias :escape :root-reserved :incoming-shape :phase :workspace-inside-source))
      (let ((plan (adoption-install-codec-fixture)))
        (ecase mode
          (:mount (setf (getf (getf plan :workspace) :mount) 88))
          (:alias (setf (getf (getf plan :incoming) :inode) (getf (getf plan :quarantine) :inode)))
          (:escape (setf (first (first (second (first (getf plan :old))))) "../outside"))
          (:root-reserved (setf (first (first (getf plan :old))) ".pg_lisp.lock"))
          (:incoming-shape (incf (sixth (first (second (first (getf plan :new)))))))
          (:phase (setf (getf (getf plan :attempt) :phase) :installing))
          (:workspace-inside-source
           (setf (getf (getf plan :workspace) :directory) "/tmp/adoption-codec-source/work/")))
        (ok (intake-fails-p (lambda () (pg::physical-adoption-install-image plan 0))) (format nil "~a" mode)))))

(deftest adoption-install-codec-rejects-corruption-and-unexpected-progress
    (let* ((plan (adoption-install-codec-fixture))
           (image (pg::physical-adoption-install-image plan 0))
           (last (+ 1 (length (getf plan :old)) (length (getf plan :new)))))
      (let ((bad (copy-seq image)))
        (setf (aref bad (1- (length bad))) 0)
        (ok (intake-fails-p (lambda () (pg::physical-adoption-install-decode bad)))))
      (ok (intake-fails-p
           (lambda () (pg::physical-adoption-install-decode
                       (concatenate '(vector (unsigned-byte 8)) image (pg::string->bytes "garbage"))))))
      (ok (intake-fails-p
           (lambda () (pg::physical-adoption-install-decode
                       (concatenate '(vector (unsigned-byte 8))
                                    (pg::physical-adoption-install-image plan last)
                                    (pg::string->bytes "S"))))))))

(cl-spec:defproperty adoption-install-only-accepts-the-next-progress-prefix
    ((old-count (range integer 1 4)) (index (range integer 0 200)) (cut (range integer 0 25)))
  (:about pg::physical-adoption-install-decode)
  (:trials (:normal 100))
  (let* ((plan (adoption-install-codec-fixture old-count))
         (step (mod index (+ 1 old-count (length (getf plan :new)))))
         (before (pg::physical-adoption-install-image plan step))
         (after (pg::physical-adoption-install-image plan (1+ step)))
         (prefix (mod cut (- (length after) (length before))))
         (partial (subseq after 0 (+ (length before) prefix)))
         (decoded (pg::physical-adoption-install-decode partial)))
    (and (equal plan (getf decoded :plan))
         (= step (getf decoded :step))
         (= (length before) (getf decoded :valid-length))
         (eq (plusp prefix) (not (null (getf decoded :partial)))))))

(deftest generated-adoption-install-progress-prefixes
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-install-only-accepts-the-next-progress-prefix :seed 81))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

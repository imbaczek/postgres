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

(defun metadata-codec-test-storage (image shape)
  (let ((size (length image)))
    (ecase shape
      (:simple image)
      (:adjustable
       (make-array size :element-type '(unsigned-byte 8) :initial-contents image :adjustable t))
      (:fill-pointer
       (let ((result (make-array (+ size 7) :element-type '(unsigned-byte 8)
                                            :initial-element 255 :adjustable t :fill-pointer size)))
         (replace result image)
         result))
      (:displaced
       (let ((backing (make-array (+ size 6) :element-type '(unsigned-byte 8) :initial-element 255)))
         (replace backing image :start1 3)
         (make-array size :element-type '(unsigned-byte 8)
                          :displaced-to backing :displaced-index-offset 3))))))

(cl-spec:defproperty adoption-install-storage-shapes-preserve-progress
    ((shape (member :simple :adjustable :fill-pointer :displaced))
     (old-count (range integer 1 4)) (index (range integer 0 200)) (cut (range integer 0 50)))
  (:about pg::physical-adoption-install-decode)
  (:trials (:normal 100))
  (let* ((plan (adoption-install-codec-fixture old-count))
         (last (pg::physical-adoption-install-last-step plan))
         (step (mod index last))
         (before (pg::physical-adoption-install-image plan step))
         (after (pg::physical-adoption-install-image plan (1+ step)))
         (prefix (mod cut (- (length after) (length before))))
         (image (subseq after 0 (+ (length before) prefix)))
         (bytes (metadata-codec-test-storage image shape))
         (state (pg::physical-adoption-install-decode bytes)))
    (fill bytes 0)
    (and (equal plan (getf state :plan))
         (= step (getf state :step))
         (= (length before) (getf state :valid-length))
         (eq (plusp prefix) (not (null (getf state :partial)))))))

(deftest adoption-install-generated-storage-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-install-storage-shapes-preserve-progress
                                       :seed 316 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest adoption-install-rejects-checksummed-noncanonical-bodies
    (let* ((plan (adoption-install-codec-fixture))
           (canonical (pg::physical-bootstrap-manifest-text plan))
           (last-step (pg::physical-adoption-install-last-step plan)))
      (dolist (text (list (concatenate 'string canonical " ")
                          (concatenate 'string " " canonical)
                          (format nil "~a~%" canonical)))
        (let* ((body (pg::string->bytes text))
               (crc (pg::wal-crc32 body))
               (header (pg::string->bytes
                        (format nil "PGADOPTIONINSTALL1 ~d ~8,'0X~%" (length body) crc)))
               (base (concatenate '(vector (unsigned-byte 8)) header body
                                  (make-array 1 :element-type '(unsigned-byte 8)
                                                :initial-element 10)))
               (complete (apply #'concatenate '(vector (unsigned-byte 8)) base
                                (loop for step from 1 to last-step
                                      collect (pg::physical-adoption-install-step-image crc step))))
               (partial (concatenate '(vector (unsigned-byte 8)) base
                                     (pg::physical-adoption-install-step-image crc 1)
                                     (subseq (pg::physical-adoption-install-step-image crc 2) 0 3))))
          (dolist (image (list base complete partial))
            (dolist (shape '(:simple :adjustable :fill-pointer :displaced))
              (let ((bytes (metadata-codec-test-storage image shape)))
                (ok (intake-fails-p
                     (lambda () (pg::physical-adoption-install-decode bytes)))))))))))

(deftest adoption-install-rejects-long-headers-before-decoding
    (let ((bytes (make-array 1024 :element-type '(unsigned-byte 8) :initial-element 65))
          (original (symbol-function 'pg::bytes->string))
          (calls 0))
      (setf (aref bytes 1000) 10)
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::bytes->string)
                   (lambda (&rest arguments) (incf calls) (apply original arguments)))
             (ok (intake-fails-p (lambda () (pg::physical-adoption-install-decode bytes))))
             (ok (zerop calls)))
        (setf (symbol-function 'pg::bytes->string) original))))

(deftest adoption-install-decoder-bounds-progress-limit-traversals
    (let* ((plan (adoption-install-codec-fixture 32))
           (last (pg::physical-adoption-install-last-step plan))
           (image (pg::physical-adoption-install-image plan last))
           (original (symbol-function 'pg::physical-adoption-install-last-step))
           (calls 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-adoption-install-last-step)
                   (lambda (value) (incf calls) (funcall original value)))
             (ok (= last (getf (pg::physical-adoption-install-decode image) :step)))
             (ok (<= 1 calls 2) (format nil "progress-limit helper called ~d times" calls)))
        (setf (symbol-function 'pg::physical-adoption-install-last-step) original))))

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

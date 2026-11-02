(in-package #:pg-physical-tests)

(defun adoption-attempt-codec-fixture (&key (phase :preparing) (manifest 991) (digest 123)
                                         (root-id 456) (next 42) state)
  (let* ((owner (getf state :owner))
         (system (if state (getf state :system-id) 42))
         (snapshot (if state (getf state :snapshot)
                       (sample-checkpoint-snapshot :next-xid next :oldest-active-xid next)))
         (metadata (pg::make-wal-control-metadata system pg::+db-shutdowned+ 100))
         (wal (if state (first (getf state :capture))
                  (pg::wal-encode-control-image 152 152 snapshot :metadata metadata)))
         (capture (or (getf state :capture) (list wal nil nil nil nil nil nil)))
         (cluster (if state (getf state :cluster-identity) "attempt-codec"))
         (history (format nil "1~c0/000000D8~cattempt history~%" #\Tab #\Tab))
         (anchor
           (list :version 1 :system-id system :base-timeline 1 :base 152 :manifest-digest manifest
                 :source-timeline 2 :receive-timeline 1 :receive-lsn 152 :receive-digest #xCBF29CE484222325
                 :overlap-start 0 :overlap-digest 88 :history-text history
                 :eor-octets (coerce (pg::wal-encode-end-of-recovery-record 216 42 2 1 1) 'list)))
         (request (pg::physical-follow-request-copy
                   (loop for (key value) on anchor by #'cddr
                         unless (member key '(:history-text :eor-octets)) append (list key value))))
         (proof-p (eq phase :proved))
         (name "adoption_attempt"))
    (list
     :version 1 :phase phase
     :local (list :directory (if owner (namestring (pg::physical-adoption-owner-directory owner))
                                 "/tmp/adoption-codec-source/")
                  :device (if owner (pg::physical-adoption-owner-device owner) 1)
                  :inode (if owner (pg::physical-adoption-owner-inode owner) 2)
                  :lock-device (if owner (pg::physical-adoption-owner-lock-device owner) 1)
                  :lock-inode (if owner (pg::physical-adoption-owner-lock-inode owner) 3)
                  :system-id system :cluster-identity cluster :timeline 1 :namespace :legacy
                  :end 152 :checkpoint-start 32 :next-xid (getf snapshot :next-xid)
                  :allocation-next-xid 1003 :wal-control (pg::physical-libpq-octets-datum wal)
                  :capture (mapcar (lambda (bytes)
                                     (when bytes
                                       (list (length bytes)
                                             (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
                                   capture))
     :stage (list :directory "/tmp/adoption-codec-stage/"
                  :device (if owner (pg::physical-adoption-owner-device owner) 1) :inode 4
                  :system-id system :cluster-identity cluster :timeline 1
                  :checkpoint 152 :checkpoint-start 32 :end 152 :next-xid (getf snapshot :next-xid)
                  :manifest-digest manifest :source-pin name
                  :auth-files (list (list "authentication.initialized" 8 11)
                                    (list "pg_hba.conf" 12 22)
                                    (list "pg_ident.conf" 0 #xCBF29CE484222325)))
     :origin-proof (list :identity (list system 1 32 77) :proven-lsn 32
                         :prefix-digest #xCBF29CE484222325 :end 152 :end-digest digest)
     :source (libpq-test-source "codec-fixture" 5439)
     :request request :source-history history
     :anchor (when proof-p
               (loop for (key value) on anchor by #'cddr
                     unless (eq key :history-text) append (list key value)))
     :proof (when proof-p
              (list :slot-name name :anchor-digest (pg::physical-follow-anchor-digest anchor)
                    :system-id system :base-timeline 1 :base 32 :manifest-digest 77 :end 152
                    :end-digest digest :root-id root-id :fork 216 :source-timeline 2)))))

(cl-spec:defproperty adoption-attempt-codec-preserves-unsigned-witnesses
    ((manifest (range integer 0 18446744073709551615))
     (digest (range integer 0 18446744073709551615))
     (root (range integer 0 18446744073709551615))
     (next (range integer 3 1002))
     (phase (range integer 0 3)))
  (:about pg::physical-adoption-attempt-image)
  (:trials (:normal 100))
  (let* ((record (adoption-attempt-codec-fixture
                  :phase (nth phase '(:preparing :proved :aborting :aborted))
                  :manifest manifest :digest digest :root-id root :next next))
         (copy (pg::physical-adoption-attempt-copy record))
         (decoded (pg::physical-adoption-attempt-decode
                   (pg::physical-adoption-attempt-image record))))
    (and (equal copy decoded)
         (= next (getf (getf decoded :local) :next-xid))
         (= 1003 (getf (getf decoded :local) :allocation-next-xid)))))

(deftest generated-adoption-attempt-codec-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-attempt-codec-preserves-unsigned-witnesses :seed 76))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest adoption-attempt-codec-rejects-incoherent-private-records
    (dolist (mode '(:unknown :duplicate :version :phase :wal :capture :stage :request
                    :history :origin :partial-proof :wrong-proof :wrong-anchor :nested-stage :stage-parent :auth-name :auth-limit :auth-empty))
      (let ((record (adoption-attempt-codec-fixture :phase :proved)))
        (ecase mode
          (:unknown (setf record (append record (list :unknown 1))))
          (:duplicate (setf record (append record (list :phase :proved))))
          (:version (setf (getf record :version) 2))
          (:phase (setf (getf record :phase) :unknown-phase))
          (:wal (setf (getf (getf record :local) :wal-control) '(:bytes "00")))
          (:capture (setf (third (getf (getf record :local) :capture)) '(0 99)))
          (:stage (setf (getf (getf record :stage) :next-xid) 1003))
          (:nested-stage (setf (getf (getf record :stage) :directory)
                               (concatenate 'string (getf (getf record :local) :directory) "nested/")))
          (:stage-parent (setf (getf (getf record :stage) :directory) "/tmp/"))
          (:auth-name (setf (first (first (getf (getf record :stage) :auth-files))) "wrong"))
          (:auth-limit (setf (second (first (getf (getf record :stage) :auth-files))) 1048577))
          (:auth-empty (setf (third (third (getf (getf record :stage) :auth-files))) 0))
          (:request (setf (getf (getf record :request) :manifest-digest) 111))
          (:history (setf (getf record :source-history) "not a history"))
          (:origin (setf (getf (getf record :origin-proof) :end) 151))
          (:partial-proof (setf (getf record :proof) nil))
          (:wrong-proof (incf (getf (getf record :proof) :end-digest)))
          (:wrong-anchor (incf (getf (getf record :anchor) :manifest-digest))))
        (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-image record)))))))

(deftest adoption-attempt-envelope-refuses-damage-extra-forms-and-oversize
    (let* ((record (adoption-attempt-codec-fixture))
           (image (pg::physical-adoption-attempt-image record)))
      (setf (aref image (1- (length image))) 1)
      (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-decode image))))
      (dolist (body (list
                     (format nil "~a~%(:extra form)~%" (pg::physical-bootstrap-manifest-text record))
                     "#.(error \"reader evaluation must stay disabled\")"))
        (let ((bytes (pg::string->bytes
                      (format nil "PGADOPTIONATTEMPT1 ~8,'0X~%~a~%"
                              (pg::wal-crc32 (pg::string->bytes body)) body))))
          (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-decode bytes)))))))
  (ok (intake-fails-p
       (lambda () (pg::physical-adoption-attempt-decode
                   (make-array (1+ pg::+physical-adoption-attempt-maximum-bytes+)
                               :element-type '(unsigned-byte 8)))))))

(deftest adoption-attempt-transitions-cannot-rebind-or-restart-cancelled-work
    (let* ((preparing (adoption-attempt-codec-fixture))
           (proved (adoption-attempt-codec-fixture :phase :proved))
           (aborting (copy-tree proved))
           (aborted nil))
      (setf (getf aborting :phase) :aborting
            aborted (copy-tree aborting)
            (getf aborted :phase) :aborted)
      (ok (pg::physical-adoption-attempt-check-transition preparing proved))
      (ok (pg::physical-adoption-attempt-check-transition proved aborting))
      (ok (pg::physical-adoption-attempt-check-transition aborting aborted))
      (dolist (pair (list (list proved preparing) (list preparing aborted)
                          (list aborted preparing) (list aborting proved)))
        (ok (intake-fails-p
             (lambda () (apply #'pg::physical-adoption-attempt-check-transition pair)))))
      (let ((changed (copy-tree proved)))
        (incf (getf (getf changed :proof) :root-id))
        (ok (intake-fails-p
             (lambda () (pg::physical-adoption-attempt-check-transition proved changed)))))
      (let ((changed (copy-tree preparing)))
        (setf (getf (getf changed :source) :fallback) "rebound")
        (ok (intake-fails-p
             (lambda () (pg::physical-adoption-attempt-check-transition preparing changed)))))))

(deftest adoption-attempt-persists-private-monotone-phases-under-the-captured-owner
    (call-with-adoption-clean-primary-fixture
     (lambda (root snapshot metadata)
       (declare (ignore snapshot metadata))
       (pg::call-with-physical-adoption-owner
        root
        (lambda (owner)
          (let* ((state (pg::physical-adoption-inspect-primary owner))
                 (record (adoption-attempt-codec-fixture :state state))
                 (pg::*pgdata* (merge-pathnames "unrelated/" root))
                 (saved (pg::physical-adoption-attempt-publish owner record))
                 (path (pg::physical-adoption-attempt-path root))
                 (proved (adoption-attempt-codec-fixture :phase :proved :state state)))
            (ok (equal saved (pg::physical-adoption-attempt-read owner)))
            (ok (= #o600 (logand #o7777 (sb-posix:stat-mode (sb-posix:lstat (namestring path))))))
            (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-publish owner record))))
            (let ((wrong (copy-tree saved)))
              (incf (getf (getf wrong :origin-proof) :end-digest))
              (ok (intake-fails-p
                   (lambda () (pg::physical-adoption-attempt-publish owner proved :previous wrong)))))
            (dolist (phase '(:proved :aborting :aborted))
              (let ((candidate (if (eq phase :proved) proved (copy-tree saved))))
                (setf (getf candidate :phase) phase
                      saved (pg::physical-adoption-attempt-publish owner candidate :previous saved))
                (ok (eq phase (getf (pg::physical-adoption-attempt-read owner) :phase)))))
            (ok (not (probe-file (merge-pathnames "physical-adoption.attempt" pg::*pgdata*))))
            (ok (probe-file path))))))))

(deftest adoption-attempt-publication-cuts-retain-only-complete-intents
    (dolist (cut '(:before-file-fsync :after-file-fsync :before-rename :after-rename
                   :after-directory-fsync :before-readback :after-readback))
      (call-with-adoption-clean-primary-fixture
       (lambda (root snapshot metadata)
         (declare (ignore snapshot metadata))
         (pg::call-with-physical-adoption-owner
          root
          (lambda (owner)
            (let* ((state (pg::physical-adoption-inspect-primary owner))
                   (before (lineage-test-bytes (pg::wal-control-path)))
                   (record (adoption-attempt-codec-fixture :state state))
                   (pg::*physical-adoption-attempt-test-hook*
                     (lambda (point path)
                       (when (eq point cut)
                         (when (eq point :before-file-fsync)
                           (ok (= #o600 (logand #o7777
                                                (sb-posix:stat-mode
                                                 (sb-posix:lstat
                                                  (namestring (make-pathname :type "tmp" :defaults path))))))))
                         (error "attempt publication cut")))))
              (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-publish owner record))))
              (let ((path (pg::physical-adoption-attempt-path root)))
                (if (member cut '(:before-file-fsync :after-file-fsync :before-rename))
                    (ok (not (probe-file path)))
                    (progn
                      (ok (equal (pg::physical-adoption-attempt-copy record)
                                 (pg::physical-adoption-attempt-read owner)))
                      (ok (intake-fails-p #'pg::physical-adoption-check-startup)))))
              (ok (equalp before (lineage-test-bytes (pg::wal-control-path))))
              (ok (not (probe-file (merge-pathnames "physical-adoption.tmp" root)))))))))))

(deftest adoption-attempt-reader-refuses-nonprivate-or-shared-records
    (call-with-adoption-clean-primary-fixture
     (lambda (root snapshot metadata)
       (declare (ignore snapshot metadata))
       (pg::call-with-physical-adoption-owner
        root
        (lambda (owner)
          (let* ((state (pg::physical-adoption-inspect-primary owner))
                 (record (adoption-attempt-codec-fixture :state state))
                 (path (pg::physical-adoption-attempt-path root))
                 (alias (merge-pathnames "shared-attempt" root)))
            (pg::physical-adoption-attempt-publish owner record)
            (sb-posix:chmod (namestring path) #o644)
            (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-read owner))))
            (sb-posix:chmod (namestring path) #o600)
            (sb-posix:link (namestring path) (namestring alias))
            (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-read owner))))
            (delete-file alias)
            (ok (pg::physical-adoption-attempt-read owner))
            (delete-file path)
            (sb-posix:symlink "missing-intent" (namestring path))
            (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-read owner))))
            (ok (intake-fails-p #'pg::physical-adoption-check-startup))))))))

(defun adoption-attempt-test-history (size)
  (let* ((prefix (format nil "1~c0/000000D8~cattempt history~%" #\Tab #\Tab))
         (history (make-string size :initial-element #\x)))
    (replace history prefix)
    (loop for start from (length prefix) below size by 1000
          for end = (min size (+ start 1000))
          do (setf (char history start) #\#)
             (when (> (- end start) 1) (setf (char history (1- end)) #\Newline)))
    history))

(deftest adoption-attempt-stores-a-maximum-history-only-once
    (let* ((record (adoption-attempt-codec-fixture :phase :proved))
           (history (adoption-attempt-test-history pg::+physical-store-history-maximum-bytes+)))
      (setf (getf record :source-history) history
            (getf (getf record :proof) :anchor-digest)
            (pg::physical-follow-anchor-digest
             (append (getf record :anchor) (list :history-text history))))
      (let* ((image (pg::physical-adoption-attempt-image record))
             (text (pg::bytes->string image))
             (start (search history text))
             (decoded (pg::physical-adoption-attempt-decode image)))
        (ok start)
        (ok (null (search history text :start2 (1+ start))))
        (ok (equal history (getf decoded :source-history)))
        (ok (not (getf (getf decoded :anchor) :history-text)))
        (ok (not (getf (getf decoded :proof) :history-text)))
        (ok (< (length image) (+ pg::+physical-store-history-maximum-bytes+ 65536))))))

(deftest adoption-attempt-publication-copies-the-callers-previous-phase-before-reading
    (call-with-adoption-clean-primary-fixture
     (lambda (root snapshot metadata)
       (declare (ignore snapshot metadata))
       (pg::call-with-physical-adoption-owner
        root
        (lambda (owner)
          (let* ((state (pg::physical-adoption-inspect-primary owner))
                 (saved (pg::physical-adoption-attempt-publish
                         owner (adoption-attempt-codec-fixture :state state)))
                 (proved (adoption-attempt-codec-fixture :phase :proved :state state))
                 (original (symbol-function 'pg::physical-adoption-attempt-read)))
            (dolist (phase '(:aborting :aborted))
              (let ((next (copy-tree saved)))
                (setf (getf next :phase) phase
                      saved (pg::physical-adoption-attempt-publish owner next :previous saved))))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-adoption-attempt-read)
                         (lambda (reader-owner)
                           (let ((actual (funcall original reader-owner)))
                             (setf (getf saved :phase) :preparing)
                             actual)))
                   (ok (intake-fails-p
                        (lambda () (pg::physical-adoption-attempt-publish owner proved :previous saved)))))
              (setf (symbol-function 'pg::physical-adoption-attempt-read) original))
            (ok (eq :aborted (getf (pg::physical-adoption-attempt-read owner) :phase)))))))))

(deftest adoption-attempt-record-order-is-canonical-and-proof-numbers-stay-typed
    (let* ((record (adoption-attempt-codec-fixture :phase :proved))
           (canonical (pg::physical-adoption-attempt-copy record)))
      (dolist (key '(:local :stage :origin-proof :anchor :proof))
        (setf (getf record key)
              (loop for pair in (reverse (loop for (name value) on (getf record key) by #'cddr
                                               collect (list name value)))
                    append pair)))
      (ok (equal canonical (pg::physical-adoption-attempt-copy record)))
      (setf (getf (getf record :proof) :root-id) "456")
      (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-image record))))))

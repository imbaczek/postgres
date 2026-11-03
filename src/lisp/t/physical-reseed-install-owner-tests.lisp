(in-package #:pg-physical-tests)

(defun reseed-install-owner-test-write (root plan step &optional partial)
  (let* ((bytes (pg::physical-reseed-install-image plan step))
         (path (pg::physical-reseed-install-path root)))
    (when partial
      (let ((next (pg::physical-reseed-install-image plan (1+ step))))
        (setf bytes (subseq next 0 (1+ (length bytes))))))
    (with-open-file (out path :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
      (write-sequence bytes out)
      (finish-output out)
      (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (length bytes))
      (pg::fsync-stream out))
    (pg::fsync-directory root)))

(defun reseed-install-owner-test-plan (root owner attempt incoming quarantine)
  (let* ((plan (reseed-install-codec-fixture 0))
         (device (pg::physical-adoption-owner-device owner))
         (path (pg::physical-reseed-install-path root))
         (attempt-stat (sb-posix:lstat (namestring (pg::physical-reseed-attempt-path root))))
         (old (pg::physical-reseed-install-old-snapshot owner attempt)))
    (setf (getf plan :attempt) attempt
          (getf plan :attempt-file) (list :device device :inode (sb-posix:stat-ino attempt-stat))
          (getf plan :incoming) incoming (getf plan :quarantine) quarantine
          (getf plan :old) old)
    (loop for row in (getf plan :new) for inode from (+ (ash 1 48) 100)
          do (setf (second row) device (third row) inode))
    (with-open-file (out path :direction :output :if-exists :error :element-type '(unsigned-byte 8))
      (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
      (let ((stat (sb-posix:fstat (sb-sys:fd-stream-fd out))))
        (setf (getf plan :journal) (list :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat))))
      (write-sequence (pg::physical-reseed-install-image plan 0) out)
      (pg::fsync-stream out))
    (pg::fsync-directory root)
    plan))

(defun call-with-reseed-install-owner-fixture (function &key (pid t) (before-count 1) (after-count 0))
  (call-with-reaped-reseed-pid
   (lambda (dead)
     (call-with-reseed-owner-directory
      (lambda (root)
        (with-physical-store-directory (stage)
          (with-physical-store-directory (incoming)
            (with-physical-store-directory (quarantine)
              (dolist (directory (list stage incoming quarantine))
                (sb-posix:chmod (namestring directory) #o700))
              (when pid (reseed-test-write-pid root (- dead)))
              (dolist (name (append (loop for index below before-count collect (format nil "old-~4,'0d" index))
                                    (loop for index below after-count collect (format nil "zz-old-~4,'0d" index))))
                (with-open-file (out (merge-pathnames name root) :direction :output)
                  (write-string "opaque old data" out)))
              (let ((plan nil))
                (pg::call-with-physical-reseed-owner
                 root
                 (lambda (owner)
                   (let* ((record (reseed-attempt-test-record owner))
                          (stat (sb-posix:lstat (namestring stage))))
                     (setf (getf record :stage)
                           (list :directory (namestring stage) :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat)))
                     (setf record (pg::physical-reseed-attempt-publish owner record))
                     (pg::call-with-physical-reseed-install-auxiliary
                      owner incoming
                      (lambda (directory incoming-facts fd check)
                        (declare (ignore directory fd check))
                        (pg::call-with-physical-reseed-install-auxiliary
                         owner quarantine
                         (lambda (directory quarantine-facts fd check)
                           (declare (ignore directory fd check))
                           (setf plan (reseed-install-owner-test-plan root owner record incoming-facts quarantine-facts)))
                         :prepare t :empty t))
                      :prepare t :empty t))))
                (funcall function root stage incoming quarantine plan))))))))))

(defun reseed-install-owner-test-move (root quarantine plan count)
  (dolist (row (subseq (getf plan :old) 0 count))
    (sb-posix:rename (namestring (merge-pathnames (first row) root))
                     (namestring (merge-pathnames (first row) quarantine)))))

(cl-spec:defproperty reseed-install-pid-proof-accepts-recorded-and-single-ahead-prefixes
    ((before-count (range integer 0 3)) (after-count (range integer 0 3))
                                        (index (range integer 0 200)) (ahead boolean) (partial boolean))
  (:about pg::physical-reseed-install-check-pid pg::call-with-physical-reseed-install-owner)
  (:trials (:normal 100))
  (let ((accepted nil))
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming))
       (let* ((count (length (getf plan :old)))
              (step (mod index (1+ count)))
              (moves (min count (+ step (if ahead 1 0)))))
         (reseed-install-owner-test-move root quarantine plan moves)
         (reseed-install-owner-test-write root plan step partial)
         (pg::call-with-physical-reseed-install-owner
          root (lambda (owner) (setf accepted (eq owner (pg::physical-reseed-check-owner owner)))))))
     :before-count before-count :after-count after-count)
    accepted))

(deftest generated-reseed-install-pid-prefix-proof
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-install-pid-proof-accepts-recorded-and-single-ahead-prefixes
                                       :seed 100 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest reseed-install-owner-reopens-private-journal-without-image-health
    (dolist (pid '(nil t))
      (call-with-reseed-install-owner-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine))
         (let ((escaped nil))
           (pg::call-with-physical-reseed-install-owner
            root
            (lambda (owner)
              (setf escaped owner)
              (ok (eq owner (pg::physical-reseed-check-owner owner)))
              (ok (equal (getf plan :attempt) (pg::physical-reseed-attempt-read owner)))
              (ok (not (search "journal-secret" (format nil "~s" owner))))
              (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner owner))))
              (ok (intake-fails-p (lambda () (pg::physical-reseed-prepare-image owner #p"/unused/"))))
              (ok (intake-fails-p (lambda () (pg::physical-reseed-install-old-snapshot owner (getf plan :attempt)))))
              (ok (sb-thread:join-thread
                   (sb-thread:make-thread (lambda () (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner)))))))))
           (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner escaped))))))
       :pid pid)))

(deftest reseed-install-owner-keeps-cancellation-authority-across-attempt-inode-replacement
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (pg::call-with-physical-reseed-install-owner
        root
        (lambda (owner)
          (let* ((before (pg::physical-reseed-attempt-read owner))
                 (next (pg::physical-reseed-attempt-copy before))
                 (path (pg::physical-reseed-attempt-path root))
                 (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
            (setf (getf next :phase) :aborting)
            (setf next (pg::physical-reseed-attempt-publish owner next :previous before))
            (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
            (ok (eq owner (pg::physical-reseed-check-owner owner)))
            (let ((aborted (pg::physical-reseed-attempt-copy next)))
              (setf (getf aborted :phase) :aborted)
              (pg::physical-reseed-attempt-publish owner aborted :previous next :receipt (reseed-attempt-test-abort-receipt next))
              (ok (eq :aborted (getf (pg::physical-reseed-attempt-read owner) :phase))))
            (ok (= inode (getf (getf plan :attempt-file) :inode)))))))))

(deftest reseed-install-owner-proves-pid-at-recorded-or-single-ahead-location
    (dolist (case '(:root-before :root-at-boundary :moved-before-step :moved-torn-step :moved-recorded :moved-terminal
                    :moved-too-early :root-after-step))
      (call-with-reseed-install-owner-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming))
         (let* ((ordinal (1+ (position "postmaster.pid" (getf plan :old) :key #'first :test #'equal)))
                (moved (member case '(:moved-before-step :moved-torn-step :moved-recorded :moved-terminal :moved-too-early)))
                (step (case case
                        ((:root-before :moved-too-early) 0)
                        ((:root-at-boundary :moved-before-step :moved-torn-step) (1- ordinal))
                        (:moved-terminal (pg::physical-reseed-install-last-step plan))
                        (t ordinal)))
                (moves (if moved ordinal (if (eq case :root-before) 0 (1- ordinal)))))
           (reseed-install-owner-test-move root quarantine plan moves)
           (reseed-install-owner-test-write root plan step (eq case :moved-torn-step))
           (if (member case '(:moved-too-early :root-after-step))
               (ok (intake-fails-p
                    (lambda () (pg::call-with-physical-reseed-install-owner root (lambda (owner) (declare (ignore owner)))))))
               (pg::call-with-physical-reseed-install-owner
                root
                (lambda (owner)
                  (ok (eq owner (pg::physical-reseed-check-owner owner)))
                  (when moved
                    (pg::call-with-physical-reseed-install-auxiliary
                     owner quarantine
                     (lambda (directory facts fd check)
                       (declare (ignore directory facts fd))
                       (ok (funcall check))
                       (ok (eq owner (pg::physical-reseed-check-owner owner))))
                     :expected (getf plan :quarantine)))))))))))

(deftest reseed-install-owner-allows-only-original-ctime-to-change-on-pid-rename
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming))
       (let* ((original (getf (getf (getf plan :attempt) :local) :pid))
              (ordinal (1+ (position "postmaster.pid" (getf plan :old) :key #'first :test #'equal))))
         (sleep 1.1)
         (reseed-install-owner-test-move root quarantine plan ordinal)
         (reseed-install-owner-test-write root plan ordinal)
         (let ((current (pg::physical-reseed-read-pid quarantine)))
           (ok (/= (eighth (getf original :identity)) (eighth (getf current :identity))))
           (ok (equal (subseq (getf original :identity) 0 7) (subseq (getf current :identity) 0 7)))
           (ok (equalp (pg::physical-libpq-datum-octets (getf original :bytes)) (getf current :bytes))))
         (pg::call-with-physical-reseed-install-owner root (lambda (owner) (ok (pg::physical-reseed-check-owner owner))))))))

(deftest reseed-install-owner-root-pid-cleanup-survives-all-missing-workspaces
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore plan))
       (dolist (directory (list stage incoming quarantine))
         (uiop:delete-directory-tree directory :validate t))
       (pg::call-with-physical-reseed-operation-owner
        root
        (lambda (owner)
          (ok (pg::physical-reseed-install-owner-p owner))
          (let* ((record (pg::physical-reseed-attempt-read owner))
                 (next (pg::physical-reseed-attempt-copy record)))
            (setf (getf next :phase) :aborting)
            (pg::physical-reseed-attempt-publish owner next :previous record)
            (ok (eq :aborting (getf (pg::physical-reseed-attempt-read owner) :phase)))))))))

(deftest reseed-install-owner-refuses-damaged-moved-pid-and-quarantine-evidence
    (dolist (kind '(:bytes :replacement :hardlink :guard :quarantine-mode :root-pid :unknown-process))
      (call-with-reseed-install-owner-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming))
         (let* ((ordinal (1+ (position "postmaster.pid" (getf plan :old) :key #'first :test #'equal)))
                (path (merge-pathnames "postmaster.pid" quarantine))
                (original-probe (symbol-function 'pg::physical-reseed-process-state))
                (called nil))
           (reseed-install-owner-test-move root quarantine plan ordinal)
           (reseed-install-owner-test-write root plan ordinal)
           (unwind-protect
                (progn
                  (ecase kind
                    (:bytes (with-open-file (out path :direction :output :if-exists :overwrite) (write-char #\X out)))
                    (:replacement
                     (let ((bytes (lineage-test-bytes path)) (saved (merge-pathnames "saved-pid" quarantine)))
                       (sb-posix:rename (namestring path) (namestring saved))
                       (with-open-file (out path :direction :output :element-type '(unsigned-byte 8))
                         (write-sequence bytes out))
                       (sb-posix:chmod (namestring path) #o600)))
                    (:hardlink (sb-posix:link (namestring path) (namestring (merge-pathnames "pid-alias" quarantine))))
                    (:guard
                     (with-open-file (out (pg::physical-reseed-install-path quarantine) :direction :output :if-exists :overwrite)
                       (write-char #\X out)))
                    (:quarantine-mode (sb-posix:chmod (namestring quarantine) #o755))
                    (:root-pid (reseed-test-write-pid root (getf (getf (getf (getf plan :attempt) :local) :pid) :pid)))
                    (:unknown-process
                     (setf (symbol-function 'pg::physical-reseed-process-state)
                           (lambda (&rest values) (declare (ignore values)) :unknown))))
                  (ok (intake-fails-p
                       (lambda ()
                         (pg::call-with-physical-reseed-install-owner
                          root (lambda (owner) (declare (ignore owner)) (setf called t))))))
                  (ok (not called)))
             (setf (symbol-function 'pg::physical-reseed-process-state) original-probe)))))))

(deftest reseed-install-owner-refuses-observed-step-and-cancellation-regression
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming))
       (pg::call-with-physical-reseed-install-owner
        root
        (lambda (owner)
          (reseed-install-owner-test-move root quarantine plan 1)
          (reseed-install-owner-test-write root plan 1)
          (ok (pg::physical-reseed-check-owner owner))
          (reseed-install-owner-test-write root plan 0)
          (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))
          (reseed-install-owner-test-write root plan 1)
          (let* ((record (pg::physical-reseed-attempt-read owner))
                 (next (pg::physical-reseed-attempt-copy record)))
            (setf (getf next :phase) :aborting)
            (pg::physical-reseed-attempt-publish owner next :previous record)
            (let ((bytes (pg::physical-reseed-attempt-image record)))
              (with-open-file (out (pg::physical-reseed-attempt-path root)
                                   :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
                (write-sequence bytes out)
                (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (length bytes))
                (pg::fsync-stream out)))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))))))))

(deftest reseed-install-owner-raw-reader-keeps-root-and-journal-generation-bound
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let ((fd (pg::physical-backup-open-directory root)))
         (unwind-protect
              (progn
                (dolist (stabilize '(nil t))
                  (let ((state (pg::physical-reseed-install-read-at fd "physical-reseed.install" :stabilize stabilize)))
                    (ok (equal plan (getf state :plan)))
                    (ok (zerop (getf state :step)))))
                (dolist (name '("../physical-reseed.install" "physical-reseed.attempt" "postmaster.pid" ""))
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-install-read-at fd name)))))
                (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fd)))))
           (sb-posix:close fd))))))

(deftest reseed-install-owner-raw-reader-refuses-unsafe-and-rebound-records
    (dolist (kind '(:symlink :fifo :directory :public :hardlink :empty :oversized :generation :root-binding))
      (call-with-reseed-install-owner-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine))
         (let* ((path (pg::physical-reseed-install-path root))
                (saved (merge-pathnames "saved-install" root))
                (bytes (lineage-test-bytes path))
                (fd (pg::physical-backup-open-directory root)))
           (sb-posix:rename (namestring path) (namestring saved))
           (unwind-protect
                (progn
                  (ecase kind
                    (:symlink (sb-posix:symlink (namestring saved) (namestring path)))
                    (:fifo (sb-posix:mkfifo (namestring path) #o600))
                    (:directory (sb-posix:mkdir (namestring path) #o700))
                    (:hardlink (sb-posix:link (namestring saved) (namestring path)))
                    ((:public :empty :oversized :generation :root-binding)
                     (with-open-file (out path :direction :output :element-type '(unsigned-byte 8))
                       (sb-posix:fchmod (sb-sys:fd-stream-fd out) (if (eq kind :public) #o644 #o600))
                       (case kind
                         (:oversized (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (1+ pg::+physical-reseed-install-maximum-bytes+)))
                         (:root-binding
                          (let ((changed (pg::physical-reseed-install-copy plan)))
                            (setf (getf (getf changed :journal) :inode) (sb-posix:stat-ino (sb-posix:fstat (sb-sys:fd-stream-fd out))))
                            (incf (getf (getf (getf changed :attempt) :local) :inode) (ash 1 44))
                            (write-sequence (pg::physical-reseed-install-image changed 0) out)))
                         (:empty nil)
                         (t (write-sequence bytes out))))))
                  (ok (intake-fails-p (lambda () (pg::physical-reseed-install-read-at fd "physical-reseed.install"))))
                  (ok (equalp bytes (lineage-test-bytes saved)))
                  (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fd)))))
             (sb-posix:close fd)))))))

(deftest reseed-install-owner-refuses-root-replacement-without-touching-foreign-journal
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (let ((moved (pathname (concatenate 'string (string-right-trim "/" (namestring root)) ".saved/")))
             (fired nil) (called nil))
         (unwind-protect
              (let ((pg::*physical-reseed-install-test-hook*
                      (lambda (point path)
                        (declare (ignore path))
                        (when (and (eq point :after-journal-open) (not fired))
                          (setf fired t)
                          (sb-posix:rename (namestring root) (namestring moved))
                          (sb-posix:mkdir (namestring root) #o700)
                          (with-open-file (out (pg::physical-reseed-install-path root) :direction :output)
                            (write-string "foreign journal" out))))))
                (ok (intake-fails-p
                     (lambda ()
                       (pg::call-with-physical-reseed-install-owner
                        root (lambda (owner) (declare (ignore owner)) (setf called t))))))
                (ok fired) (ok (not called))
                (ok (equalp (pg::string->bytes "foreign journal") (lineage-test-bytes (pg::physical-reseed-install-path root))))
                (ok (equal '("physical-reseed.install") (pg::directory-entry-names root))))
           (when fired
             (uiop:delete-directory-tree root :validate t)
             (sb-posix:rename (namestring moved) (namestring root)))))
       (pg::call-with-physical-reseed-install-owner root (lambda (owner) (ok (pg::physical-reseed-check-owner owner)))))))

(deftest reseed-operation-owner-selector-keeps-install-fences-authoritative
    (call-with-reseed-inventory-fixture
     (lambda (root owner record)
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (close (pg::physical-adoption-owner-lock owner))
       (pg::call-with-physical-reseed-operation-owner
        root (lambda (selected)
               (ok (eq (type-of selected) 'pg::physical-reseed-owner))
               (ok (equal record (pg::physical-reseed-attempt-read selected)))))
       (with-open-file (out (pg::physical-reseed-install-temporary-path root) :direction :output)
         (write-string "partial initial install" out))
       (pg::call-with-physical-reseed-operation-owner
        root (lambda (selected) (ok (eq (type-of selected) 'pg::physical-reseed-cleanup-owner))))
       (with-open-file (out (pg::physical-reseed-install-path root) :direction :output)
         (write-string "invalid final install" out))
       (let ((called nil))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-reseed-operation-owner
                 root (lambda (selected) (declare (ignore selected)) (setf called t))))))
         (ok (not called))))))

(deftest reseed-install-owner-refuses-immutable-plan-and-live-request-rebinding
    (dolist (kind '(:plan :request))
      (call-with-reseed-install-owner-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine))
         (pg::call-with-physical-reseed-install-owner
          root
          (lambda (owner)
            (ecase kind
              (:plan
               (let ((changed (pg::physical-reseed-install-copy plan)))
                 (incf (getf changed :manifest-digest))
                 (incf (tenth (assoc "physical-backup.manifest" (getf changed :new) :test #'equal)))
                 (reseed-install-owner-test-write root changed 0)))
              (:request
               (let ((changed (pg::physical-reseed-attempt-read owner)))
                 (setf (getf (getf changed :request) :token) (make-string 64 :initial-element #\b))
                 (let ((bytes (pg::physical-reseed-attempt-image changed)))
                   (with-open-file (out (pg::physical-reseed-attempt-path root)
                                        :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
                     (write-sequence bytes out)
                     (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (length bytes))
                     (pg::fsync-stream out))))))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))))))))

(deftest reseed-install-owner-factory-binds-the-actual-root-lock-generation
    (call-with-reseed-install-owner-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let* ((changed (pg::physical-reseed-install-copy plan))
              (attempt (getf changed :attempt))
              (lock (merge-pathnames ".pg_lisp.lock" root))
              (inode (sb-posix:stat-ino (sb-posix:lstat (namestring lock))))
              (called nil))
         (incf (getf (getf attempt :local) :lock-inode) (ash 1 44))
         (let ((bytes (pg::physical-reseed-attempt-image attempt)))
           (with-open-file (out (pg::physical-reseed-attempt-path root)
                                :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
             (write-sequence bytes out)
             (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (length bytes))
             (pg::fsync-stream out)))
         (reseed-install-owner-test-write root changed 0)
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-reseed-install-owner
                 root (lambda (owner) (declare (ignore owner)) (setf called t))))))
         (ok (not called))
         (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring lock)))))))))

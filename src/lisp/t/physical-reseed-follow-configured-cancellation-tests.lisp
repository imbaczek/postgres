(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-configured-cancellation-admission-preserves-history
    ((shape (range integer 0 19)) (selected (range integer 0 1)) (pending (range integer 0 1)))
  (:about pg::physical-reseed-follow-cancellation-configured-record)
  (:trials (:normal 100))
  (let* ((record (pg::physical-reseed-follow-copy
                  (reseed-cancellation-codec-fixture shape (plusp selected))))
         (record (if (plusp pending)
                     (pg::physical-reseed-follow-cancellation-begin-record record) record))
         (before (pg::physical-bootstrap-manifest-text record)))
    (if (<= 6 shape 9)
        (let ((copy (pg::physical-reseed-follow-cancellation-configured-record record)))
          (and (equal record copy) (not (eq record copy))
               (equal before (pg::physical-bootstrap-manifest-text record))))
        (handler-case
            (progn (pg::physical-reseed-follow-cancellation-configured-record record) nil)
          (error () (equal before (pg::physical-bootstrap-manifest-text record)))))))

(deftest generated-reseed-configured-cancellation-admission-contract
    (let ((result
            (cl-spec:result-data
             (cl-spec:run-property 'reseed-configured-cancellation-admission-preserves-history
                                   :seed 200 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(deftest reseed-configured-cancellation-admission-bounds-inert-settlements
    (dotimes (shape 20)
      (dolist (selected '(nil t))
        (dolist (pending '(nil t))
          (let* ((record (pg::physical-reseed-follow-copy
                          (reseed-cancellation-codec-fixture shape selected)))
                 (record (if pending (pg::physical-reseed-follow-cancellation-begin-record record) record))
                 (before (pg::physical-bootstrap-manifest-text record)))
            (if (<= 6 shape 9)
                (let ((copy (pg::physical-reseed-follow-cancellation-configured-record record)))
                  (ok (equal record copy))
                  (ok (not (eq record copy)))
                  (ok (signals (pg::physical-reseed-follow-cancellation-preconfiguration-record record) 'error)))
                (ok (signals (pg::physical-reseed-follow-cancellation-configured-record record) 'error)))
            (ok (equal before (pg::physical-bootstrap-manifest-text record))))))))

(deftest reseed-configured-cancellation-admission-keeps-terminal-receipts
    (loop for shape from 6 to 9
          do (dolist (selected '(nil t))
               (let* ((record (pg::physical-reseed-follow-cancellation-begin-record
                               (reseed-cancellation-codec-fixture shape selected)))
                      (receipt (or (getf (getf record :settlement) :receipt)
                                   (reseed-cancellation-source-parent
                                    record :closed (if (= shape 9) :aborted :handed-off) t)))
                      (closed (pg::physical-reseed-follow-cancellation-source-closed-record record receipt))
                      (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record
                                closed receipt (if (= shape 9) :parent-abort :selected-release))))
                 (dolist (value (list closed cleaned))
                   (let* ((before (pg::physical-bootstrap-manifest-text value))
                          (copy (pg::physical-reseed-follow-cancellation-configured-record value))
                          (changed (pg::physical-reseed-follow-copy value)))
                     (ok (equal value copy))
                     (ok (equal (getf value :settlement) (getf copy :settlement)))
                     (setf (getf (getf (getf (getf changed :cancellation) :receipt) :request) :token)
                           (make-string 64 :initial-element #\d))
                     (ok (signals (pg::physical-reseed-follow-cancellation-configured-record changed) 'error))
                     (ok (equal before (pg::physical-bootstrap-manifest-text value)))))))))

(deftest reseed-configured-cancellation-owner-reads-inert-and-refuses-forward-work
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (let ((expected nil) (escaped nil))
        (call-with-reseed-inert-settlement-fixture
         kind
         (lambda (owner root workspace)
           (declare (ignore root workspace))
           (setf expected (pg::physical-reseed-follow-read owner)))
         (lambda (root workspace)
           (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                 (original (symbol-function 'pg::physical-libpq-connect-frozen))
                 (connections 0))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                          (lambda (&rest arguments)
                            (declare (ignore arguments))
                            (incf connections)
                            (error "read-only cancellation owner cannot connect")))
                    (pg::call-with-physical-reseed-follow-cancellation-owner
                     root
                     (lambda (owner)
                       (setf escaped owner)
                       (ok (pg::physical-reseed-follow-cancellation-owner-p owner))
                       (ok (not (pg::physical-reseed-follow-configuration-owner-p owner)))
                       (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
                       (ok (signals (pg::physical-reseed-follow-read owner) 'error))
                       (ok (signals (pg::physical-reseed-follow-prepare owner :helper "/unused/helper") 'error))
                       (ok (signals (pg::physical-reseed-follow-begin-configuration owner workspace 12345) 'error))
                       (ok (signals (pg::physical-reseed-follow-begin-settlement owner) 'error))
                       (ok (signals (pg::physical-reseed-follow-begin-activation owner) 'error))
                       (ok (signals (pg::physical-reseed-follow-publish-intent owner expected :previous expected) 'error))
                       (ok (signals (pg::physical-reseed-follow-recover-intent owner) 'error))
                       (ok (signals (pg::physical-reseed-follow-begin-cancellation owner) 'error))
                       (ok (signals (pg::physical-reseed-follow-recover-cancellation owner) 'error))
                       (ok (signals (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper") 'error))
                       (ok (not (pg::physical-backup-lease-lstat (merge-pathnames "physical-reseed.follow.tmp" root))))
                       (ok (signals
                            (pg::call-with-physical-reseed-follow-mapped-image
                             owner (lambda (&rest arguments) (declare (ignore arguments)) t)) 'error))
                       (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))
                    (ok (zerop connections))
                    (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                    (ok (signals (pg::physical-reseed-follow-read-configured-cancellation escaped) 'error)))
               (setf (symbol-function 'pg::physical-libpq-connect-frozen) original))))))))

(deftest reseed-configured-cancellation-owner-freezes-configured-history
    (let ((record nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore root workspace))
         (setf record (pg::physical-reseed-follow-read owner)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-cancellation-owner
          root
          (lambda (owner)
            (let ((original (pg::physical-adoption-attempt-value-copy
                             (pg::physical-reseed-follow-owner-binding owner)))
                  (changed (pg::physical-reseed-follow-copy record)))
              (incf (getf (getf changed :configuration) :time))
              (ok (equal record (pg::physical-reseed-follow-check-binding owner record)))
              (ok (equal changed (pg::physical-reseed-follow-cancellation-configured-record changed)))
              (dolist (replace '(t nil))
                (unwind-protect
                     (progn
                       (if replace
                           (setf (pg::physical-reseed-follow-owner-binding owner)
                                 (pg::physical-reseed-follow-cancellation-configured-history changed))
                           (incf (getf (getf (pg::physical-reseed-follow-owner-binding owner)
                                             :configuration) :time)))
                       (ok (signals (pg::physical-reseed-follow-check-binding owner changed) 'error)))
                  (setf (pg::physical-reseed-follow-owner-binding owner)
                        (pg::physical-adoption-attempt-value-copy original))))
              (ok (equal record (pg::physical-reseed-follow-check-binding owner record)))
              (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner))))))))))

(deftest reseed-configured-cancellation-observation-is-detached
    (let ((record nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore root workspace))
         (setf record (pg::physical-reseed-follow-read owner)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-cancellation-owner
          root
          (lambda (owner)
            (let* ((opaque (pg::physical-reseed-follow-observe-configured-cancellation owner))
                   (observation (pg::physical-reseed-follow-cancellation-observation-data opaque))
                   (expected (pg::physical-adoption-attempt-value-copy observation))
                   (layout (getf observation :layout))
                   (stamp (getf observation :stamp)))
              (ok (pg::physical-reseed-follow-cancellation-observation-p opaque))
              (ok (< (length (write-to-string opaque)) 200))
              (ok (equal record (getf observation :record)))
              (ok (= 10 (length (getf layout :directories))))
              (ok (= 10 (length (getf stamp :roles))))
              (ok (every (lambda (image) (keywordp (second image))) (getf layout :images)))
              (ok (every (lambda (role) (not (search "/proc/" (second role)))) (getf stamp :roles)))
              (ok (equal observation (pg::physical-reseed-follow-cancellation-observation-data
                                      (pg::physical-reseed-follow-observe-configured-cancellation owner))))
              (incf (getf (getf (getf observation :record) :configuration) :time))
              (setf (car (getf layout :directories)) nil)
              (ok (equal expected (pg::physical-reseed-follow-cancellation-observation-data
                                   (pg::physical-reseed-follow-observe-configured-cancellation owner))))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))))))))

(deftest reseed-configured-cancellation-observation-checks-resource-close
    (dolist (cut '(:store :image))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace) (declare (ignore owner root workspace)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (let* ((path (merge-pathnames "physical-reseed.follow" root))
                (temporary (merge-pathnames ".configured-observation-swap" root))
                (bytes (lineage-test-bytes path))
                (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))
                (close-store (symbol-function 'pg::physical-store-close))
                (image (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image))
                (fired nil) (returned nil) (escaped nil) (message nil))
           (flet ((replace-file ()
                    (unless fired
                      (with-open-file (output temporary :direction :output :if-exists :error
                                                        :element-type '(unsigned-byte 8))
                        (write-sequence bytes output))
                      (sb-posix:chmod (namestring temporary) #o600)
                      (sb-posix:rename (namestring temporary) (namestring path))
                      (setf fired t))))
             (setf message
                   (handler-case
                       (pg::call-with-physical-reseed-follow-cancellation-owner
                        root
                        (lambda (owner)
                          (setf escaped owner)
                          (unwind-protect
                               (progn
                                 (setf (symbol-function 'pg::physical-store-close)
                                       (lambda (store)
                                         (multiple-value-prog1 (funcall close-store store)
                                           (when (eq cut :store) (replace-file))))
                                       (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image)
                                       (lambda (held function)
                                         (multiple-value-prog1 (funcall image held function)
                                           (when (eq cut :image) (replace-file)))))
                                 (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                 (setf returned t))
                            (setf (symbol-function 'pg::physical-store-close) close-store
                                  (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image) image))))
                     (error (condition) (princ-to-string condition))))
             (ok fired)
             (ok (equalp bytes (lineage-test-bytes path)))
             (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
             (ok message)
             (ng returned)
             (ok (not (pg::physical-reseed-follow-owner-busy-p escaped)))
             (when (eq cut :image)
               (ok (search "retirement observation changed during resource cleanup" message)))))))))

(defun configured-cancellation-test-history (record settlement-phase cancellation-phase)
  (let ((candidate (pg::physical-reseed-follow-copy record)))
    (when settlement-phase
      (setf (getf candidate :settlement)
            (list :phase :handoff-pending
                  :successor (pg::physical-reseed-follow-settlement-successor candidate)
                  :receipt nil))
      (unless (eq settlement-phase :handoff-pending)
        (setf candidate (pg::physical-reseed-follow-completion-terminal candidate settlement-phase))))
    (if (null cancellation-phase)
        (pg::physical-reseed-follow-copy candidate)
        (let ((pending (pg::physical-reseed-follow-cancellation-begin-record candidate)))
          (if (eq cancellation-phase :pending)
              pending
              (let* ((receipt (or (getf (getf candidate :settlement) :receipt)
                                  (reseed-cancellation-source-parent candidate :closed :handed-off t)))
                     (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt)))
                (ecase cancellation-phase
                  (:source-closed closed)
                  (:cleaned
                   (pg::physical-reseed-follow-cancellation-cleaned-record
                    closed receipt (if (eq :aborted (getf receipt :outcome))
                                       :parent-abort :selected-release))))))))))

(deftest reseed-configured-cancellation-owner-opens-every-inert-settlement
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (let ((seed nil))
        (call-with-reseed-inert-settlement-fixture
         kind
         (lambda (owner root workspace)
           (declare (ignore root workspace))
           (setf seed (pg::physical-reseed-follow-read owner)))
         (lambda (root workspace)
           (declare (ignore workspace))
           (let* ((path (merge-pathnames "physical-reseed.follow" root))
                  (install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root)))
                  (control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root)))
                  (connect (symbol-function 'pg::physical-libpq-connect-frozen))
                  (connections 0))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                          (lambda (&rest arguments)
                            (declare (ignore arguments))
                            (incf connections)
                            (error "configured observation must not contact source")))
                    (dolist (phase '(nil :handoff-pending :handed-off :source-aborted))
                      (dolist (cancellation-phase '(nil :pending :source-closed :cleaned))
                        (testing (format nil "~a/~a/~a" kind phase cancellation-phase)
                                 (let* ((expected (configured-cancellation-test-history seed phase cancellation-phase))
                                        (bytes (pg::physical-reseed-follow-image expected)))
                                   (delete-file path)
                                   (reseed-follow-owner-test-write path bytes)
                                   (pg::call-with-physical-reseed-follow-cancellation-owner
                                    root
                                    (lambda (owner)
                                      (let* ((observed (pg::physical-reseed-follow-observe-configured-cancellation owner))
                                             (data (pg::physical-reseed-follow-cancellation-observation-data observed)))
                                        (ok (equal expected (getf data :record)))
                                        (ok (equal (getf expected :settlement)
                                                   (getf (getf data :record) :settlement)))
                                        (ok (= 10 (length (getf (getf data :stamp) :roles))))
                                        (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))))
                                   (ok (equalp bytes (lineage-test-bytes path)))
                                   (ok (equalp install (lineage-test-bytes
                                                        (merge-pathnames "physical-reseed.install" root))))
                                   (ok (equalp control (lineage-test-bytes
                                                        (merge-pathnames "physical-recovery.control" root))))))))
                    (ok (zerop connections)))
               (setf (symbol-function 'pg::physical-libpq-connect-frozen) connect))))))))

(deftest reseed-configured-cancellation-owner-fences-lifetime-and-reentry
    (let ((record nil) (escaped nil) (thrown-owner nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore root workspace))
         (setf record (pg::physical-reseed-follow-read owner)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-cancellation-owner
          root
          (lambda (owner)
            (setf escaped owner)
            (let ((thread nil))
              (unwind-protect
                   (progn
                     (sb-sys:without-interrupts
                       (setf thread
                             (sb-thread:make-thread
                              (lambda ()
                                (handler-case
                                    (progn
                                      (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                      nil)
                                  (error (condition) (princ-to-string condition)))))))
                     (ok (equal "offline adoption no longer owns its data directory"
                                (sb-thread:join-thread thread))))
                (when thread (sb-thread:join-thread thread))))
            (ok (signals (pg::physical-reseed-follow-observe-configured-cancellation
                          (copy-structure owner)) 'error))
            (let ((entered nil))
              (ok (signals
                   (pg::call-with-physical-reseed-follow-cancellation-owner
                    root (lambda (nested) (declare (ignore nested)) (setf entered t))) 'error))
              (ng entered))
            (let ((original (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
                  (reentries 0))
              (unwind-protect
                   (progn
                     (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                           (lambda (held fd check)
                             (incf reentries)
                             (ok (pg::physical-reseed-follow-owner-busy-p held))
                             (ok (signals (pg::physical-reseed-follow-observe-configured-cancellation held) 'error))
                             (funcall original held fd check)))
                     (pg::physical-reseed-follow-observe-configured-cancellation owner)
                     (ok (= 1 reentries)))
                (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                      original)))
            (let ((original (symbol-function 'pg::physical-store-close))
                  (observe-at (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
                  (target nil) (close-count 0)
                  (closed nil) (returned nil) (message nil))
              (unwind-protect
                   (progn
                     (setf (symbol-function 'pg::physical-store-close)
                           (lambda (store)
                             (funcall original store)
                             (when (eq store target)
                               (incf close-count)
                               (setf closed store)
                               (error "configured observation close failure")))
                           (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                           (lambda (held fd check)
                             (setf target (pg::physical-reseed-follow-installation-context-store
                                           (pg::physical-reseed-follow-installation-context held)))
                             (funcall observe-at held fd check)))
                     (setf message
                           (handler-case
                               (progn
                                 (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                 (setf returned t)
                                 nil)
                             (error (condition) (princ-to-string condition)))))
                (setf (symbol-function 'pg::physical-store-close) original
                      (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                      observe-at))
              (ok (and message (search "configured observation close failure" message)))
              (ng returned)
              (ok (= 1 close-count))
              (ok (and target (eq target closed) (pg::physical-store-closed-p closed)))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))
            (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner)))))
         (ok (signals (pg::physical-reseed-follow-check-binding escaped record) 'error))
         (let ((tag (gensym "CONFIGURED-OWNER-EXIT")))
           (ok (eq :done
                   (catch tag
                     (pg::call-with-physical-reseed-follow-cancellation-owner
                      root (lambda (owner) (setf thrown-owner owner) (throw tag :done)))))))
         (ok (signals (pg::physical-reseed-follow-check-binding thrown-owner record) 'error))))))

(deftest reseed-configured-cancellation-owner-keeps-original-history-through-target-abort
    (let ((record nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore root workspace))
         (setf record (pg::physical-reseed-follow-read owner)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (let* ((path (pg::physical-reseed-attempt-path root))
                (temporary (merge-pathnames ".configured-attempt-replacement" root))
                (follow (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                (install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
           (dolist (phase '(:premanifest :aborting :aborted))
             (let ((actual (pg::physical-reseed-attempt-copy (getf record :attempt))))
               (setf (getf actual :phase) phase)
               (reseed-follow-owner-test-write temporary (pg::physical-reseed-attempt-image actual))
               (sb-posix:rename (namestring temporary) (namestring path))
               (pg::fsync-directory root)
               (pg::call-with-physical-reseed-follow-cancellation-owner
                root
                (lambda (owner)
                  (let ((data (pg::physical-reseed-follow-cancellation-observation-data
                               (pg::physical-reseed-follow-observe-configured-cancellation owner))))
                    (ok (equal actual (getf (getf data :attempt) :record)))
                    (ok (equal record (getf data :record)))
                    (ok (eq :premanifest (getf (getf (getf data :record) :attempt) :phase)))
                    (ok (eq phase (getf (pg::physical-reseed-cleanup-owner-attempt owner) :phase)))
                    (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))))
               (ok (equalp follow (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
               (ok (equalp install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root)))))))))))

(defun check-configured-cancellation-owner-refusal (root &optional expected-message)
  (let ((entered nil) (message nil) (connections 0)
                      (connect (symbol-function 'pg::physical-libpq-connect-frozen)))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (incf connections)
                   (error "refused configured owner must not contact source")))
           (setf message
                 (handler-case
                     (pg::call-with-physical-reseed-follow-cancellation-owner
                      root (lambda (owner) (declare (ignore owner)) (setf entered t)))
                   (error (condition) (princ-to-string condition)))))
      (setf (symbol-function 'pg::physical-libpq-connect-frozen) connect))
    (ok message)
    (ng entered)
    (ok (zerop connections))
    (when expected-message (ok (search expected-message (or message ""))))))

(deftest reseed-configured-cancellation-owner-rejects-invalid-finals-without-fallback
    (let ((record nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore root workspace))
         (setf record (pg::physical-reseed-follow-read owner)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (let* ((path (merge-pathnames "physical-reseed.follow" root))
                (bytes (lineage-test-bytes path))
                (fallback-function (symbol-function 'pg::physical-reseed-follow-initial-arguments))
                (connect (symbol-function 'pg::physical-libpq-connect-frozen))
                (fallback nil) (connections 0))
           (labels ((remove-final ()
                      (let ((stat (pg::physical-backup-lease-lstat path)))
                        (when stat
                          (if (sb-posix:s-isdir (sb-posix:stat-mode stat))
                              (sb-posix:rmdir (namestring path))
                              (delete-file path))))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-reseed-follow-initial-arguments)
                          (lambda (&rest arguments)
                            (declare (ignore arguments))
                            (setf fallback t)
                            (error "unexpected preconfiguration fallback"))
                          (symbol-function 'pg::physical-libpq-connect-frozen)
                          (lambda (&rest arguments)
                            (declare (ignore arguments))
                            (incf connections)
                            (error "invalid configured final must not contact source")))
                    (dolist (kind '(:absent :malformed :torn :preconfiguration :building :staged
                                    :installing :activation :retirement :base :install :public
                                    :hardlink :symlink :fifo :directory))
                      (testing (symbol-name kind)
                               (let ((candidate (pg::physical-reseed-follow-copy record)))
                                 (setf fallback nil)
                                 (remove-final)
                                 (case kind
                                   (:preconfiguration
                                    (setf candidate (pg::physical-reseed-follow-core-projection candidate)))
                                   ((:building :staged :installing)
                                    (setf (getf (getf candidate :configuration) :phase) kind
                                          (getf (getf candidate :configuration) :step) 0)
                                    (when (eq kind :building)
                                      (setf (getf (getf candidate :configuration) :directories) nil
                                            (getf (getf candidate :configuration) :entries) nil)))
                                   (:activation (setf candidate (reseed-cancellation-codec-fixture 10 nil)))
                                   (:retirement (setf candidate (reseed-cancellation-codec-fixture 16 nil)))
                                   (:base
                                    (let ((digest (1+ (getf (getf candidate :base) :manifest-digest))))
                                      (setf (getf (getf candidate :base) :manifest-digest) digest)
                                      (dolist (generation (getf candidate :generations))
                                        (setf (getf (getf generation :coordinates) :manifest-digest) digest
                                              (getf (getf generation :anchor) :manifest-digest) digest))
                                      (setf (getf (getf candidate :configuration) :anchor-digest)
                                            (pg::physical-follow-anchor-digest
                                             (pg::physical-reseed-follow-generation-anchor
                                              (car (last (getf candidate :generations))))))))
                                   (:install (incf (getf (getf candidate :install) :digest))))
                                 (case kind
                                   (:absent)
                                   (:malformed (reseed-follow-owner-test-write path (pg::string->bytes "invalid follow")))
                                   (:torn (reseed-follow-owner-test-write
                                           path (concatenate '(vector (unsigned-byte 8)) bytes #(83))))
                                   (:hardlink
                                    (sb-posix:link (namestring (pg::physical-reseed-attempt-path root)) (namestring path)))
                                   (:symlink (sb-posix:symlink "physical-reseed.attempt" (namestring path)))
                                   (:fifo (sb-posix:mkfifo (namestring path) #o600))
                                   (:directory (sb-posix:mkdir (namestring path) #o700))
                                   (otherwise
                                    (ok (equal candidate (pg::physical-reseed-follow-copy candidate)))
                                    (reseed-follow-owner-test-write path (pg::physical-reseed-follow-image candidate))
                                    (when (eq kind :public) (sb-posix:chmod (namestring path) #o644))))
                                 (let* ((stat (pg::physical-backup-lease-lstat path))
                                        (before (when stat (pg::physical-reseed-file-facts stat))))
                                   (check-configured-cancellation-owner-refusal
                                    root
                                    (when (member kind '(:preconfiguration :building :staged :installing
                                                         :activation :retirement))
                                      "configured cancellation ownership requires inert history before activation"))
                                   (ng fallback)
                                   (ok (equal before
                                              (let ((after (pg::physical-backup-lease-lstat path)))
                                                (when after (pg::physical-reseed-file-facts after)))))))
                               (remove-final)
                               (reseed-follow-owner-test-write path bytes)))
                    (ok (zerop connections)))
               (setf (symbol-function 'pg::physical-reseed-follow-initial-arguments) fallback-function
                     (symbol-function 'pg::physical-libpq-connect-frozen) connect)
               (remove-final)
               (reseed-follow-owner-test-write path bytes)))
           (pg::call-with-physical-reseed-follow-cancellation-owner
            root (lambda (owner)
                   (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner))))))))))

(deftest reseed-configured-cancellation-owner-preserves-every-temporary
    (let ((record nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore root workspace))
         (setf record (pg::physical-reseed-follow-read owner)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (let* ((path (merge-pathnames "physical-reseed.follow.tmp" root))
                (final (merge-pathnames "physical-reseed.follow" root))
                (bytes (lineage-test-bytes final))
                (conflict (pg::physical-reseed-follow-copy record)))
           (incf (getf (getf conflict :configuration) :time))
           (dolist (kind '(:empty :partial :canonical :conflicting :public :symlink :fifo :directory))
             (testing (symbol-name kind)
                      (unwind-protect
                           (progn
                             (case kind
                               (:symlink (sb-posix:symlink "physical-reseed.follow" (namestring path)))
                               (:fifo (sb-posix:mkfifo (namestring path) #o600))
                               (:directory (sb-posix:mkdir (namestring path) #o700))
                               (otherwise
                                (reseed-follow-owner-test-write
                                 path (case kind
                                        (:empty (make-array 0 :element-type '(unsigned-byte 8)))
                                        (:partial (subseq bytes 0 (1- (length bytes))))
                                        (:conflicting (pg::physical-reseed-follow-image conflict))
                                        (otherwise bytes)))
                                (when (eq kind :public) (sb-posix:chmod (namestring path) #o644))))
                             (let* ((stat (sb-posix:lstat (namestring path)))
                                    (before (pg::physical-reseed-file-facts stat))
                                    (contents (when (sb-posix:s-isreg (sb-posix:stat-mode stat))
                                                (lineage-test-bytes path))))
                               (check-configured-cancellation-owner-refusal root)
                               (ok (equal before (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
                               (when contents (ok (equalp contents (lineage-test-bytes path))))
                               (ok (equalp bytes (lineage-test-bytes final)))))
                        (if (eq kind :directory) (sb-posix:rmdir (namestring path)) (delete-file path)))))
           (pg::call-with-physical-reseed-follow-cancellation-owner
            root (lambda (owner)
                   (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner))))))))))

(defun check-configured-cancellation-unexpected-entries
    (phase &optional
             (roles '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive))
             (fixture #'call-with-reseed-inert-settlement-fixture))
  (dolist (role roles)
    (testing (symbol-name role)
             (funcall fixture
                      :same-timeline
                      (lambda (owner root workspace) (declare (ignore owner root workspace)))
                      (lambda (root workspace)
                        (declare (ignore workspace))
                        (catch 'unexpected-entry-checked
                          (pg::call-with-physical-reseed-follow-cancellation-owner
                           root
                           (lambda (owner)
                             (let ((observe (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
                                   (image (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image))
                                   (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                                   (directory nil) (foreign nil) (facts nil) (target-fd nil)
                                   (fired 0) (returned nil) (message nil) (originals nil)
                                   (bytes (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(17 23 41))))
                               (flet ((insert-entry ()
                                        (incf fired)
                                        (setf foreign (merge-pathnames ".unexpected-configured-child" directory))
                                        (with-open-file (output foreign :direction :output :if-exists :error
                                                                        :element-type '(unsigned-byte 8))
                                          (write-sequence bytes output))
                                        (sb-posix:chmod (namestring foreign) #o600)
                                        (setf facts (pg::physical-reseed-file-facts
                                                     (sb-posix:lstat (namestring foreign))))))
                                 (unwind-protect
                                      (progn
                                        (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                                              (lambda (held fd check)
                                                (let* ((context (pg::physical-reseed-follow-installation-context held))
                                                       (descriptors (pg::physical-reseed-follow-installation-context-descriptors context)))
                                                  (setf target-fd (pg::physical-reseed-follow-configuration-role-fd descriptors role)
                                                        directory (truename (pg::physical-image-fd-path target-fd)))
                                                  (setf originals
                                                        (loop for protected in '(:root :incoming :quarantine :workspace)
                                                              for path = (truename
                                                                          (pg::physical-image-fd-path
                                                                           (pg::physical-reseed-follow-configuration-role-fd
                                                                            descriptors protected)))
                                                              collect (cons path (remove-if-not
                                                                                  (lambda (row) (eq :file (first row)))
                                                                                  (reseed-mapped-test-tree-facts path)))))
                                                  (when (eq phase :mapped) (insert-entry))
                                                  (funcall observe held fd check)))
                                              (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image)
                                              (lambda (held function)
                                                (multiple-value-prog1 (funcall image held function)
                                                  (when (eq phase :closed)
                                                    (ok (signals (sb-posix:fstat target-fd) 'sb-posix:syscall-error))
                                                    (insert-entry)))))
                                        (setf message
                                              (handler-case
                                                  (progn
                                                    (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                                    (setf returned t)
                                                    nil)
                                                (error (condition) (princ-to-string condition))))
                                        (ok (= fired 1))
                                        (ok message)
                                        (ng returned)
                                        (ng (pg::physical-reseed-follow-owner-busy-p owner))
                                        (ok (equalp journal (lineage-test-bytes
                                                             (merge-pathnames "physical-reseed.follow" root))))
                                        (dolist (tree originals)
                                          (let ((current (remove-if-not
                                                          (lambda (row) (eq :file (first row)))
                                                          (reseed-mapped-test-tree-facts (car tree)))))
                                            (dolist (row (cdr tree))
                                              (ok (equalp row (find (first (second row)) current
                                                                    :key (lambda (entry) (first (second entry)))
                                                                    :test #'equal))))))
                                        (ok (equalp bytes (lineage-test-bytes foreign)))
                                        (ok (equal facts (pg::physical-reseed-file-facts
                                                          (sb-posix:lstat (namestring foreign))))))
                                   (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at) observe
                                         (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image) image)
                                   (when foreign (delete-file foreign)))
                                 (throw 'unexpected-entry-checked t)))))))))))

(deftest reseed-configured-cancellation-refuses-unexpected-mapped-entries
    (check-configured-cancellation-unexpected-entries :mapped))

(deftest reseed-configured-cancellation-refuses-unexpected-entries-after-close
    (check-configured-cancellation-unexpected-entries :closed))

(deftest reseed-configured-cancellation-owner-rechecks-pid-and-workspace-ownership
    (let ((record nil) (quarantine nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore root workspace))
         (setf record (pg::physical-reseed-follow-read owner)
               quarantine (pathname (getf (getf (pg::physical-reseed-install-owner-plan owner)
                                                :quarantine) :directory))))
       (lambda (root workspace)
         (let* ((pid-path (merge-pathnames "postmaster.pid" root))
                (old-pid-path (merge-pathnames "postmaster.pid" quarantine))
                (old-bytes (lineage-test-bytes old-pid-path))
                (old-inode (sb-posix:stat-ino (sb-posix:lstat (namestring old-pid-path))))
                (follow (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                (install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
           (unwind-protect
                (progn
                  (ok (eq :alive (pg::physical-reseed-process-state (sb-posix:getpid))))
                  (reseed-follow-owner-test-write pid-path
                                                  (pg::string->bytes (format nil "~d~%" (sb-posix:getpid))))
                  (check-configured-cancellation-owner-refusal root "PID"))
             (when (pg::physical-backup-lease-lstat pid-path) (delete-file pid-path)))
           (with-physical-store-directory (scratch)
             (let ((saved (merge-pathnames "original.pid" scratch)))
               (unwind-protect
                    (progn
                      (sb-posix:rename (namestring old-pid-path) (namestring saved))
                      (reseed-follow-owner-test-write old-pid-path old-bytes)
                      (ok (/= old-inode (sb-posix:stat-ino (sb-posix:lstat (namestring old-pid-path)))))
                      (check-configured-cancellation-owner-refusal root "PID")
                      (ok (equalp old-bytes (lineage-test-bytes old-pid-path))))
                 (when (pg::physical-backup-lease-lstat saved)
                   (when (pg::physical-backup-lease-lstat old-pid-path) (delete-file old-pid-path))
                   (sb-posix:rename (namestring saved) (namestring old-pid-path))
                   (pg::fsync-directory quarantine)))))
           (ok (= old-inode (sb-posix:stat-ino (sb-posix:lstat (namestring old-pid-path)))))
           (let ((fd nil))
             (unwind-protect
                  (progn
                    (sb-sys:without-interrupts (setf fd (pg::physical-backup-open-directory workspace)))
                    (ok (zerop (pg::physical-store-flock fd 6)))
                    (check-configured-cancellation-owner-refusal root "configuration workspace is already owned"))
               (when fd (sb-posix:close fd))))
           (ok (equalp follow (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
           (ok (equalp install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
           (pg::call-with-physical-reseed-follow-cancellation-owner
            root (lambda (owner)
                   (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner))))))))))

(deftest reseed-configured-cancellation-owner-refuses-a-lost-root-lock
    (let ((record nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore root workspace))
         (setf record (pg::physical-reseed-follow-read owner)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (let ((escaped nil) (returned nil) (message nil))
           (setf message
                 (handler-case
                     (progn
                       (pg::call-with-physical-reseed-follow-cancellation-owner
                        root
                        (lambda (owner)
                          (setf escaped owner)
                          (let ((fd (sb-sys:fd-stream-fd (pg::physical-adoption-owner-lock owner)))
                                (contender nil) (refusal nil))
                            (unwind-protect
                                 (progn
                                   (ok (zerop (pg::physical-store-flock fd 8)))
                                   (sb-sys:without-interrupts
                                     (setf contender
                                           (open (merge-pathnames ".pg_lisp.lock" root) :direction :input)))
                                   (ok (= (sb-posix:stat-ino (sb-posix:fstat fd))
                                          (sb-posix:stat-ino
                                           (sb-posix:fstat (sb-sys:fd-stream-fd contender)))))
                                   (ok (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd contender) 6)))
                                   (setf refusal
                                         (handler-case
                                             (progn
                                               (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                               nil)
                                           (error (condition) (princ-to-string condition))))
                                   (ok (and refusal
                                            (search "offline adoption lost its exclusive directory lock" refusal)))
                                   (ng (pg::physical-adoption-owner-active-p owner)))
                              (when contender (close contender))
                              (unless (zerop (pg::physical-store-flock fd 6))
                                (error "cannot restore the test-owned root lock")))
                            (setf (pg::physical-adoption-owner-active-p owner) t)
                            (ok (signals (pg::physical-reseed-follow-check-binding owner record) 'error))
                            (ng (pg::physical-adoption-owner-active-p owner))
                            (setf (pg::physical-adoption-owner-active-p owner) t)
                            (ok (signals (pg::physical-reseed-follow-observe-configured-cancellation owner) 'error))
                            (ng (pg::physical-adoption-owner-active-p owner)))))
                       (setf returned t)
                       nil)
                   (error (condition) (princ-to-string condition))))
           (ok (and message (search "offline adoption" message)))
           (ng returned)
           (ng (pg::physical-adoption-owner-active-p escaped)))
         (pg::call-with-physical-reseed-follow-cancellation-owner
          root (lambda (owner)
                 (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner)))))))))

(deftest reseed-configured-cancellation-observation-refuses-lost-mapped-locks
    (dolist (role '(:workspace :guard :receive))
      (let ((record nil))
        (call-with-reseed-inert-settlement-fixture
         :same-timeline
         (lambda (owner root workspace)
           (declare (ignore root workspace))
           (setf record (pg::physical-reseed-follow-read owner)))
         (lambda (root workspace)
           (let* ((workspace-stat (sb-posix:lstat (namestring workspace)))
                  (guard-function (symbol-function 'pg::call-with-physical-reseed-install-guard))
                  (observe-at (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
                  (guard nil) (fired 0) (returned nil) (message nil)
                  (follow (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                  (install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
             (pg::call-with-physical-reseed-follow-cancellation-owner
              root
              (lambda (owner)
                (unwind-protect
                     (progn
                       (setf (symbol-function 'pg::call-with-physical-reseed-install-guard)
                             (lambda (parent name prepare function)
                               (funcall guard-function parent name prepare
                                        (lambda (stream)
                                          (let ((stat (sb-posix:fstat parent)))
                                            (when (and (equal name ".pg_lisp.lock")
                                                       (= (sb-posix:stat-dev workspace-stat) (sb-posix:stat-dev stat))
                                                       (= (sb-posix:stat-ino workspace-stat) (sb-posix:stat-ino stat)))
                                              (setf guard stream)))
                                          (funcall function stream))))
                             (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                             (lambda (held fd check)
                               (let* ((context (pg::physical-reseed-follow-installation-context held))
                                      (store (pg::physical-reseed-follow-installation-context-store context))
                                      (target (ecase role
                                                (:workspace
                                                 (pg::physical-reseed-follow-configuration-role-fd
                                                  (pg::physical-reseed-follow-installation-context-descriptors context)
                                                  :workspace))
                                                (:guard (sb-sys:fd-stream-fd guard))
                                                (:receive (sb-sys:fd-stream-fd (pg::physical-store-lock-stream store)))))
                                      (path (ecase role
                                              (:workspace workspace)
                                              (:guard (merge-pathnames ".pg_lisp.lock" workspace))
                                              (:receive (pg::physical-store-path store "receive.lock"))))
                                      (contender nil))
                                 (incf fired)
                                 (unwind-protect
                                      (progn
                                        (ok (pg::physical-file-holds-exclusive-flock-p target))
                                        (ok (zerop (pg::physical-store-flock target 8)))
                                        (sb-sys:without-interrupts
                                          (setf contender
                                                (if (eq role :workspace)
                                                    (pg::physical-backup-open-directory path)
                                                    (sb-posix:open (namestring path)
                                                                   (logior sb-posix:o-rdonly sb-posix:o-nofollow
                                                                           sb-posix:o-nonblock pg::+postmaster-o-cloexec+)))))
                                        (ok (= (sb-posix:stat-ino (sb-posix:fstat target))
                                               (sb-posix:stat-ino (sb-posix:fstat contender))))
                                        (ok (zerop (pg::physical-store-flock contender 6)))
                                        (ng (pg::physical-file-holds-exclusive-flock-p target))
                                        (funcall observe-at held fd check))
                                   (when contender (sb-posix:close contender))
                                   (unless (zerop (pg::physical-store-flock target 6))
                                     (error "cannot restore the test-owned mapped lock"))))))
                       (setf message
                             (handler-case
                                 (progn
                                   (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                   (setf returned t)
                                   nil)
                               (error (condition) (princ-to-string condition)))))
                  (setf (symbol-function 'pg::call-with-physical-reseed-install-guard) guard-function
                        (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at) observe-at))
                (ok (= 1 fired))
                (ok (and message (search "exclusive lock" message)))
                (ng returned)
                (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner)))))
             (ok (equalp follow (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
             (ok (equalp install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))))))))

(deftest reseed-configured-cancellation-closes-store-on-interrupted-open-handoff
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root
        (lambda (owner)
          (dolist (kind '(:error :throw :interrupt))
            (testing (symbol-name kind)
                     (let ((original (symbol-function 'pg::physical-store-open))
                           (opened nil) (directory nil) (calls 0)
                           (tag (gensym "CONFIGURED-STORE-HANDOFF"))
                           (before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                       (unwind-protect
                            (progn
                              (setf (symbol-function 'pg::physical-store-open)
                                    (lambda (&rest arguments)
                                      (setf directory (truename (first arguments))
                                            opened (apply original arguments))
                                      (incf calls)
                                      (ecase kind
                                        (:error (error "injected configured store handoff"))
                                        (:throw (throw tag :cut))
                                        (:interrupt
                                         (sb-thread:interrupt-thread
                                          sb-thread:*current-thread*
                                          (lambda () (error "injected configured store handoff")))))
                                      (error "configured handoff injection did not exit")))
                              (ok (eq :cut
                                      (catch tag
                                        (handler-case
                                            (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                          (error (condition)
                                            (when (search "injected configured store handoff"
                                                          (princ-to-string condition))
                                              :cut))))))
                              (setf (symbol-function 'pg::physical-store-open) original)
                              (ok (= calls 1))
                              (ok (and opened (pg::physical-store-closed-p opened)))
                              (ok (and opened (not (open-stream-p (pg::physical-store-lock-stream opened)))))
                              (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                              (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                              (ok (handler-case
                                      (let ((fresh
                                              (funcall original directory
                                                       (pg::physical-store-system-id opened)
                                                       (pg::physical-store-timeline opened)
                                                       :start-lsn (pg::physical-store-base-lsn opened)
                                                       :discard-tail nil :create-lock nil
                                                       :retained-identity (pg::physical-store-retained-identity opened))))
                                        (unwind-protect (pg::physical-store-check-held-lock fresh)
                                          (pg::physical-store-close fresh)))
                                    (error () nil))))
                         (setf (symbol-function 'pg::physical-store-open) original)
                         (when opened (pg::physical-store-close opened))))
                     (ok (pg::physical-reseed-follow-observe-configured-cancellation owner)))))))))

(deftest reseed-configured-cancellation-closes-directories-after-store-close-exits
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root
        (lambda (owner)
          (dolist (kind '(:error :throw :interrupt))
            (testing (symbol-name kind)
                     (let ((observe (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
                           (close-store (symbol-function 'pg::physical-store-close))
                           (open-store (symbol-function 'pg::physical-store-open))
                           (opened nil) (descriptors nil) (directory nil) (fired 0)
                           (tag (gensym "CONFIGURED-STORE-CLOSE"))
                           (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                       (unwind-protect
                            (progn
                              (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                                    (lambda (held fd check)
                                      (let ((context (pg::physical-reseed-follow-installation-context held)))
                                        (setf opened (pg::physical-reseed-follow-installation-context-store context)
                                              descriptors (copy-tree
                                                           (pg::physical-reseed-follow-installation-context-descriptors context))
                                              directory (truename
                                                         (pg::physical-image-fd-path
                                                          (pg::physical-reseed-follow-configuration-role-fd
                                                           descriptors :receive)))))
                                      (funcall observe held fd check))
                                    (symbol-function 'pg::physical-store-close)
                                    (lambda (store)
                                      (multiple-value-prog1 (funcall close-store store)
                                        (when (and (eq store opened) (zerop fired))
                                          (incf fired)
                                          (ecase kind
                                            (:error (error "injected configured store close"))
                                            (:throw (throw tag :cut))
                                            (:interrupt
                                             (sb-thread:interrupt-thread
                                              sb-thread:*current-thread*
                                              (lambda () (throw tag :cut)))))))))
                              (ok (eq :cut
                                      (catch tag
                                        (handler-case
                                            (progn
                                              (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                              :returned)
                                          (error (condition)
                                            (when (search "injected configured store close"
                                                          (princ-to-string condition))
                                              :cut))))))
                              (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at) observe
                                    (symbol-function 'pg::physical-store-close) close-store)
                              (ok (= fired 1))
                              (ok (= (length descriptors) 10))
                              (dolist (entry descriptors)
                                (ok (handler-case
                                        (progn (sb-posix:fstat (cdr entry)) nil)
                                      (sb-posix:syscall-error (condition)
                                        (= sb-posix:ebadf (sb-posix:syscall-errno condition))))))
                              (ok (and opened (pg::physical-store-closed-p opened)))
                              (ok (and opened (not (open-stream-p (pg::physical-store-lock-stream opened)))))
                              (ng (pg::physical-reseed-follow-owner-busy-p owner))
                              (ok (equalp journal (lineage-test-bytes
                                                   (merge-pathnames "physical-reseed.follow" root))))
                              (ok (let ((fresh
                                          (funcall open-store directory
                                                   (pg::physical-store-system-id opened)
                                                   (pg::physical-store-timeline opened)
                                                   :start-lsn (pg::physical-store-base-lsn opened)
                                                   :discard-tail nil :create-lock nil
                                                   :retained-identity (pg::physical-store-retained-identity opened))))
                                    (unwind-protect (pg::physical-store-check-held-lock fresh)
                                      (funcall close-store fresh)))))
                         (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at) observe
                               (symbol-function 'pg::physical-store-close) close-store)
                         (when opened (funcall close-store opened))))
                     (ok (pg::physical-reseed-follow-observe-configured-cancellation owner)))))))))

(defun configured-cancellation-receive-close-child-form (root marker kind)
  `(let ((pg::*pgdata* ,root)
         (evidence nil))
     (flet ((check (value)
              (assert value () "receive-close ~s check ~d failed" ,kind (1+ (length evidence)))
              (push t evidence))
            (journal-bytes ()
              (with-open-file (input (merge-pathnames "physical-reseed.follow" ,root)
                                     :element-type '(unsigned-byte 8))
                (let ((bytes (make-array (file-length input) :element-type '(unsigned-byte 8))))
                  (assert (= (length bytes) (read-sequence bytes input)))
                  bytes))))
       (pg::call-with-physical-reseed-follow-cancellation-owner
        ,root
        (lambda (owner)
          (let ((observe (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
                (close-fd (symbol-function 'sb-posix:close))
                (open-store (symbol-function 'pg::physical-store-open))
                (store nil) (descriptors nil) (receive nil) (directory nil)
                (fired 0) (returned nil) (tag (gensym "RECEIVE-CLOSE"))
                (journal (journal-bytes)))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                         (lambda (held fd verify)
                           (let ((context (pg::physical-reseed-follow-installation-context held)))
                             (setf store (pg::physical-reseed-follow-installation-context-store context)
                                   descriptors (copy-tree
                                                (pg::physical-reseed-follow-installation-context-descriptors context))
                                   receive (pg::physical-reseed-follow-configuration-role-fd descriptors :receive)
                                   directory (truename (pg::physical-image-fd-path receive))))
                           (funcall observe held fd verify)))
                   (sb-ext:without-package-locks
                     (setf (symbol-function 'sb-posix:close)
                           (lambda (fd)
                             (multiple-value-prog1 (funcall close-fd fd)
                               (when (and receive (= receive fd) (zerop fired))
                                 (incf fired)
                                 (ecase ,kind
                                   (:error (error "injected configured receive close"))
                                   (:throw (throw tag :cut))
                                   (:interrupt
                                    (sb-thread:interrupt-thread
                                     sb-thread:*current-thread*
                                     (lambda () (throw tag :cut))))))))))
                   (check (eq :cut
                              (catch tag
                                (handler-case
                                    (progn
                                      (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                      (setf returned t)
                                      :returned)
                                  (error (condition)
                                    (if (search "injected configured receive close"
                                                (princ-to-string condition))
                                        :cut
                                        (error "receive-close escaped before expected exit: ~a" condition))))))))
              (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at) observe)
              (sb-ext:without-package-locks
                (setf (symbol-function 'sb-posix:close) close-fd)))
            (check (= fired 1))
            (check (not returned))
            (check (= 10 (length descriptors)))
            (check (every (lambda (entry)
                            (handler-case (progn (sb-posix:fstat (cdr entry)) nil)
                              (sb-posix:syscall-error (condition)
                                (= sb-posix:ebadf (sb-posix:syscall-errno condition)))))
                          descriptors))
            (check (and store (pg::physical-store-closed-p store)))
            (check (not (open-stream-p (pg::physical-store-lock-stream store))))
            (check (not (pg::physical-reseed-follow-owner-busy-p owner)))
            (check (equalp journal
                           (journal-bytes)))
            (check
             (let ((fresh
                     (funcall open-store directory
                              (pg::physical-store-system-id store)
                              (pg::physical-store-timeline store)
                              :start-lsn (pg::physical-store-base-lsn store)
                              :discard-tail nil :create-lock nil
                              :retained-identity (pg::physical-store-retained-identity store))))
               (unwind-protect (pg::physical-store-check-held-lock fresh)
                 (pg::physical-store-close fresh))))
            (check (pg::physical-reseed-follow-observe-configured-cancellation owner)))))
       (with-open-file (output ,marker :direction :output :if-exists :error)
         (with-standard-io-syntax
           (write (list (sb-posix:getpid) ,kind (nreverse evidence)) :stream output)))
       (format t "~&CONFIGURED-RECEIVE-CLOSE-PASS~%"))))

(defun run-configured-cancellation-receive-close-child (root driver kind cache)
  (let* ((marker (merge-pathnames (format nil "~(~a~).result" kind) driver))
         (log (merge-pathnames (format nil "~(~a~).log" kind) driver))
         (directories
           (remove-duplicates
            (remove nil (mapcar #'asdf:system-source-directory (asdf:already-loaded-systems)))
            :test #'equal))
         (registry `(:source-registry
                     ,@(mapcar (lambda (directory) `(:directory ,directory)) directories)
                     :ignore-inherited-configuration))
         (forms (list '(require :asdf)
                      `(asdf:initialize-source-registry ',registry)
                      `(asdf:initialize-output-translations
                        '(:output-translations (t ,(namestring cache)) :ignore-inherited-configuration))
                      '(require :sb-posix)
                      '(declaim (notinline sb-posix:close))
                      '(asdf:load-system "pg")
                      '(make-package "PG-PHYSICAL-TESTS" :use '("CL"))
                      (configured-cancellation-receive-close-child-form root marker kind)))
         (arguments
           (let ((*package* (find-package :cl-user))
                 (*print-length* nil) (*print-level* nil) (*print-readably* t))
             (append '("--noinform" "--no-sysinit" "--no-userinit" "--non-interactive")
                     (loop for form in forms append (list "--eval" (write-to-string form))))))
         (process nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf process
                   (sb-ext:run-program (namestring sb-ext:*runtime-pathname*) arguments
                                       :wait nil :output log :error :output :if-output-exists :error)))
           (loop with deadline = (+ (get-internal-real-time) (* 180 internal-time-units-per-second))
                 while (member (sb-ext:process-status process) '(:running :stopped))
                 do (when (>= (get-internal-real-time) deadline)
                      (error "configured receive-close child exceeded deadline"))
                    (sleep 0.05))
           (sb-ext:process-wait process)
           (unless (and (eq :exited (sb-ext:process-status process))
                        (zerop (sb-ext:process-exit-code process))
                        (search "CONFIGURED-RECEIVE-CLOSE-PASS" (uiop:read-file-string log)))
             (error "configured receive-close child failed: ~a" (uiop:read-file-string log)))
           (with-open-file (input marker)
             (let* ((*read-eval* nil) (*package* (find-package :cl-user))
                                      (record (read input)))
               (assert (equal (list (sb-ext:process-pid process) kind (make-list 11 :initial-element t)) record))
               (assert (eq :eof (read input nil :eof)))
               t)))
      (when process
        (when (member (sb-ext:process-status process) '(:running :stopped))
          (sb-ext:process-kill process sb-posix:sigkill))
        (sb-ext:process-wait process)
        (sb-ext:process-close process)))))

(deftest reseed-configured-cancellation-closes-outer-directories-after-receive-close-exits
    (with-physical-store-directory (driver)
      (let ((cache (merge-pathnames "cache/" driver)))
        (call-with-reseed-inert-settlement-fixture
         :same-timeline
         (lambda (owner root workspace) (declare (ignore owner root workspace)))
         (lambda (root workspace)
           (declare (ignore workspace))
           (dolist (kind '(:error :throw :interrupt))
             (testing (symbol-name kind)
                      (ok (run-configured-cancellation-receive-close-child root driver kind cache)))))))))

(defun check-configured-cancellation-directory-swaps (phase)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (pg::call-with-physical-reseed-follow-cancellation-owner
      root
      (lambda (owner)
        (let ((record (pg::physical-reseed-follow-read-configured-cancellation owner))
              (before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
          (dolist (role '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive))
            (testing (symbol-name role)
                     (let ((observe (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
                           (image (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image))
                           (directory nil) (backup nil) (target-fd nil) (inode nil)
                           (moved nil) (created nil) (fired 0)
                           (foreign-before nil) (message nil) (failure nil) (returned nil))
                       (flet ((swap-directory ()
                                (incf fired)
                                (setf backup
                                      (pathname (concatenate 'string
                                                             (string-right-trim "/" (namestring directory))
                                                             ".configured-original/")))
                                (sb-sys:without-interrupts
                                  (sb-posix:rename (namestring directory) (namestring backup))
                                  (setf moved t)
                                  (sb-posix:mkdir (namestring directory) #o700)
                                  (setf created t))
                                (setf foreign-before (reseed-mapped-test-tree-facts directory))
                                (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring directory)))))))
                         (unwind-protect
                              (progn
                                (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                                      (lambda (held fd check)
                                        (let* ((context (pg::physical-reseed-follow-installation-context held))
                                               (descriptors (pg::physical-reseed-follow-installation-context-descriptors context)))
                                          (setf target-fd (pg::physical-reseed-follow-configuration-role-fd descriptors role)
                                                directory (truename (pg::physical-image-fd-path target-fd))
                                                inode (sb-posix:stat-ino (sb-posix:fstat target-fd)))
                                          (when (eq phase :mapped) (swap-directory))
                                          (funcall observe held fd check)))
                                      (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image)
                                      (lambda (held function)
                                        (let ((result (funcall image held function)))
                                          (when (eq phase :closed)
                                            (ok (signals (sb-posix:fstat target-fd) 'sb-posix:syscall-error))
                                            (swap-directory))
                                          result)))
                                (setf message
                                      (handler-case
                                          (progn
                                            (pg::physical-reseed-follow-observe-configured-cancellation owner)
                                            (setf returned t)
                                            nil)
                                        (error (condition)
                                          (setf failure condition)
                                          (princ-to-string condition))))
                                (ok (= fired 1))
                                (ok moved)
                                (ok created)
                                (ok message)
                                (ok (not returned))
                                (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                                (ok (reseed-activation-layout-facts-equal-p
                                     foreign-before (reseed-mapped-test-tree-facts directory)))
                                (when (and (eq phase :closed) (not (eq role :root)))
                                  (if (eq role :quarantine)
                                      (ok (and (typep failure 'sb-posix:syscall-error)
                                               (= sb-posix:enoent (sb-posix:syscall-errno failure))))
                                      (ok (search "retirement observation changed during resource cleanup" message)))))
                           (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at) observe
                                 (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image) image)
                           (when moved
                             (when created (sb-posix:rmdir (namestring directory)))
                             (sb-posix:rename (namestring backup) (namestring directory)))))
                       (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
                     (ok (equal record (pg::physical-reseed-follow-read-configured-cancellation owner)))))))))))

(deftest reseed-configured-cancellation-refuses-every-held-directory-replacement
    (check-configured-cancellation-directory-swaps :mapped))

(deftest reseed-configured-cancellation-audits-every-directory-through-close
    (check-configured-cancellation-directory-swaps :closed))

(defun restore-configured-cancellation-test-bytes (path bytes mtime)
  (with-open-file (output path :direction :io :if-exists :overwrite
                               :if-does-not-exist :error :element-type '(unsigned-byte 8))
    (write-sequence bytes output)
    (finish-output output)
    (sb-posix:ftruncate (sb-sys:fd-stream-fd output) (length bytes))
    (multiple-value-bind (seconds nanos) (floor mtime 1000000000)
      (sb-alien:with-alien ((times (sb-alien:array sb-alien:long 4)))
        (setf (sb-alien:deref times 0) 0
              (sb-alien:deref times 1) #x3ffffffe
              (sb-alien:deref times 2) seconds
              (sb-alien:deref times 3) nanos)
        (unless (zerop
                 (sb-alien:alien-funcall
                  (sb-alien:extern-alien
                   "futimens" (sb-alien:function sb-alien:int sb-alien:int (* sb-alien:long)))
                  (sb-sys:fd-stream-fd output)
                  (sb-alien:cast (sb-alien:addr times) (* sb-alien:long))))
          (error "cannot restore the fixture's nanosecond modification time"))))))

(defun check-configured-cancellation-one-file-change (owner path mount mtime phase kind)
  (with-physical-store-directory (scratch)
    (let* ((journal (merge-pathnames "physical-reseed.follow" (pg::physical-adoption-owner-directory owner)))
           (journal-bytes (lineage-test-bytes journal))
           (bytes (lineage-test-bytes path))
           (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path))))
           (backup (merge-pathnames "original" scratch))
           (observe (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at))
           (image (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image))
           (moved nil) (created nil) (fired 0)
           (changed-row nil) (changed-bytes nil) (message nil) (returned nil))
      (flet ((mutate ()
               (incf fired)
               (ecase kind
                 (:content (reseed-mapped-test-change-first-byte path))
                 (:replacement
                  (sb-sys:without-interrupts
                    (sb-posix:rename (namestring path) (namestring backup))
                    (setf moved t))
                  (reseed-follow-owner-test-write path bytes)
                  (setf created t)
                  (ok (equalp bytes (lineage-test-bytes path)))
                  (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))))
               (setf changed-row (pg::physical-reseed-install-old-observe
                                  pg::+physical-adoption-at-fdcwd+ (namestring path) mount)
                     changed-bytes (lineage-test-bytes path))))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at)
                     (lambda (held fd check)
                       (let ((result (funcall observe held fd check)))
                         (when (eq phase :mapped) (mutate))
                         result))
                     (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image)
                     (lambda (held function)
                       (let ((result (funcall image held function)))
                         (when (eq phase :closed) (mutate))
                         result)))
               (setf message
                     (handler-case
                         (progn
                           (pg::physical-reseed-follow-observe-configured-cancellation owner)
                           (setf returned t)
                           nil)
                       (error (condition) (princ-to-string condition))))
               (ok (= fired 1))
               (ok message)
               (ok (not returned))
               (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
               (ok (equal changed-row
                          (pg::physical-reseed-install-old-observe
                           pg::+physical-adoption-at-fdcwd+ (namestring path) mount)))
               (ok (equalp changed-bytes (lineage-test-bytes path)))
               (ok (equalp (if (equal path journal) changed-bytes journal-bytes)
                           (lineage-test-bytes journal))))
          (setf (symbol-function 'pg::physical-reseed-follow-cancellation-configured-observation-at) observe
                (symbol-function 'pg::%call-with-physical-reseed-follow-mapped-image) image)
          (ecase kind
            (:content (when (plusp fired)
                        (restore-configured-cancellation-test-bytes path bytes mtime)))
            (:replacement
             (when moved
               (when created (delete-file path))
               (sb-posix:rename (namestring backup) (namestring path))))))
        (ok (equalp bytes (lineage-test-bytes path)))
        (ok (equalp journal-bytes (lineage-test-bytes journal)))))))

(defun check-configured-cancellation-file-changes (phase kind)
  (let ((permanent '((:global "pg_catalog.dat") (:global "pg_control")
                     (:global "pg_wal_control") (:wal "0000000000000000.wal")
                     (:root "physical-reseed.install"))))
    (dolist (selection (cons :restorable permanent))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace) (declare (ignore owner root workspace)))
       (lambda (root workspace)
         (declare (ignore workspace))
         (let ((record nil) (roles nil))
           (pg::call-with-physical-reseed-follow-cancellation-owner
            root
            (lambda (owner)
              (setf record (pg::physical-reseed-follow-read-configured-cancellation owner))
              (let ((data (pg::physical-reseed-follow-cancellation-observation-data
                           (pg::physical-reseed-follow-observe-configured-cancellation owner))))
                (setf roles (getf (getf data :stamp) :roles)))))
           (let* ((files
                    (loop for (role directory mount row entries) in roles
                          append (loop for entry in entries
                                       when (sb-posix:s-isreg (fourth entry))
                                         collect (list role (merge-pathnames (first entry) directory)
                                                       mount (eighth entry)))))
                  (chosen
                    (remove-if-not
                     (lambda (entry)
                       (let ((key (list (first entry) (file-namestring (second entry)))))
                         (if (eq selection :restorable)
                             (not (member key permanent :test #'equal))
                             (equal key selection))))
                     files)))
             (ok (= 27 (length files)))
             (ok (equal '(:base :staged)
                        (loop for (role directory mount row entries) in roles
                              unless (some (lambda (entry) (sb-posix:s-isreg (fourth entry))) entries)
                                collect role)))
             (ok (= (length chosen) (if (eq selection :restorable) 22 1)))
             (dolist (entry chosen)
               (destructuring-bind (role path mount mtime) entry
                 (testing (format nil "~a/~a" role (file-namestring path))
                          (let ((tag (gensym "CONFIGURED-FILE-CHANGE"))
                                (before (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
                            (unless (eq selection :restorable)
                              (loop with deadline = (+ (get-internal-real-time) (* 5 internal-time-units-per-second))
                                    while (<= (- (get-universal-time) 2208988800) (1+ (eighth before)))
                                    do (when (>= (get-internal-real-time) deadline)
                                         (error "fixture clock did not advance past saved file ctime"))
                                       (sleep 0.05)))
                            (ok (eq :checked
                                    (catch tag
                                      (pg::call-with-physical-reseed-follow-cancellation-owner
                                       root
                                       (lambda (owner)
                                         (check-configured-cancellation-one-file-change
                                          owner path mount mtime phase kind)
                                         (throw tag :checked))))))
                            (if (eq selection :restorable)
                                (pg::call-with-physical-reseed-follow-cancellation-owner
                                 root
                                 (lambda (owner)
                                   (ok (equal record
                                              (pg::physical-reseed-follow-read-configured-cancellation owner)))))
                                (let ((after (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
                                  (ok (equal (butlast before) (butlast after)))
                                  (ok (/= (eighth before) (eighth after)))
                                  (when (eq role :root)
                                    (let ((fd (pg::physical-backup-open-directory root)))
                                      (unwind-protect
                                           (let* ((actual
                                                    (pg::physical-reseed-follow-terminal-reference
                                                     (pg::physical-reseed-install-read-at fd "physical-reseed.install")))
                                                  (expected (copy-tree (getf record :install))))
                                             (setf (eighth (getf expected :identity))
                                                   (eighth (getf actual :identity)))
                                             (ok (equal expected actual)))
                                        (sb-posix:close fd))))
                                  (check-configured-cancellation-owner-refusal
                                   root
                                   (if (eq role :root)
                                       "configuration seed differs from the actual terminal installation"
                                       "original image metadata differs at its prescribed configuration location")))))))))))))))

(deftest reseed-configured-cancellation-refuses-mapped-file-content-changes
    (check-configured-cancellation-file-changes :mapped :content))

(deftest reseed-configured-cancellation-refuses-file-content-changes-after-close
    (check-configured-cancellation-file-changes :closed :content))

(deftest reseed-configured-cancellation-refuses-mapped-same-byte-file-replacements
    (check-configured-cancellation-file-changes :mapped :replacement))

(deftest reseed-configured-cancellation-refuses-same-byte-file-replacements-after-close
    (check-configured-cancellation-file-changes :closed :replacement))

(defun call-with-populated-configured-cancellation-fixture (kind function &optional after)
  (let ((original (symbol-function 'call-with-adoption-clean-primary-fixture))
        (prepared 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'call-with-adoption-clean-primary-fixture)
                 (lambda (callback)
                   (funcall original
                            (lambda (root snapshot metadata)
                              (let ((database (gethash "postgres" (pg::catalog-databases pg::*catalog*))))
                                (setf (gethash "populated" (pg::database-tables database))
                                      (pg::make-pg-class
                                       :oid 900 :name "populated" :relfilenode 900 :relfrozenxid 42
                                       :columns (vector (pg::make-pg-column :name "id" :type :int4
                                                                            :typmod -1 :attnum 1))))
                                (pg::wal-atomic-write-bytes
                                 (pg::relation-file-path 900) (pg::page-init (pg::make-page)))
                                (pg::wal-atomic-write-bytes (pg::catalog-path) (pg::catalog-image-bytes)))
                              (incf prepared)
                              (funcall callback root snapshot metadata)))))
           (call-with-reseed-inert-settlement-fixture kind function after)
           (ok (= prepared 1)))
      (setf (symbol-function 'call-with-adoption-clean-primary-fixture) original))))

(deftest reseed-configured-cancellation-proves-a-populated-base
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (testing (symbol-name kind)
               (call-with-populated-configured-cancellation-fixture
                kind
                (lambda (owner root workspace)
                  (declare (ignore owner workspace))
                  (ok (= pg::+blcksz+ (length (lineage-test-bytes (merge-pathnames "base/900" root))))))
                (lambda (root workspace)
                  (declare (ignore workspace))
                  (pg::call-with-physical-reseed-follow-cancellation-owner
                   root
                   (lambda (owner)
                     (let* ((observation (pg::physical-reseed-follow-observe-configured-cancellation owner))
                            (data (pg::physical-reseed-follow-cancellation-observation-data observation))
                            (base (assoc :base (getf (getf data :stamp) :roles)))
                            (file (assoc "900" (fifth base) :test #'equal)))
                       (ok file)
                       (ok (= pg::+blcksz+ (seventh file)))
                       (ok (assoc "base/900" (getf (getf (getf data :plan) :manifest) :files) :test #'equal))))))))))

(defun check-populated-configured-cancellation-base-change (phase kind)
  (call-with-populated-configured-cancellation-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (let ((tag (gensym "POPULATED-BASE-CHANGE")))
       (ok (eq :checked
               (catch tag
                 (pg::call-with-physical-reseed-follow-cancellation-owner
                  root
                  (lambda (owner)
                    (let* ((data (pg::physical-reseed-follow-cancellation-observation-data
                                  (pg::physical-reseed-follow-observe-configured-cancellation owner)))
                           (base (assoc :base (getf (getf data :stamp) :roles)))
                           (file (assoc "900" (fifth base) :test #'equal))
                           (path (merge-pathnames "base/900" root)))
                      (ok file)
                      (ok (= pg::+blcksz+ (seventh file)))
                      (check-configured-cancellation-one-file-change
                       owner path (third base) (eighth file) phase kind)
                      (throw tag :checked)))))))
       (check-configured-cancellation-owner-refusal
        root "original image metadata differs at its prescribed configuration location")))))

(deftest reseed-configured-cancellation-refuses-populated-base-content-while-mapped
    (check-populated-configured-cancellation-base-change :mapped :content))

(deftest reseed-configured-cancellation-refuses-populated-base-content-after-close
    (check-populated-configured-cancellation-base-change :closed :content))

(deftest reseed-configured-cancellation-refuses-populated-base-replacement-while-mapped
    (check-populated-configured-cancellation-base-change :mapped :replacement))

(deftest reseed-configured-cancellation-refuses-populated-base-replacement-after-close
    (check-populated-configured-cancellation-base-change :closed :replacement))

(deftest reseed-configured-cancellation-refuses-unexpected-populated-base-entries
    (dolist (phase '(:mapped :closed))
      (testing (symbol-name phase)
               (check-configured-cancellation-unexpected-entries
                phase '(:base) #'call-with-populated-configured-cancellation-fixture))))

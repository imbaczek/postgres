(in-package #:pg-physical-tests)

(defun reseed-activation-layout-facts-equal-p (left right)
  (cond ((and (typep left '(vector (unsigned-byte 8)))
              (typep right '(vector (unsigned-byte 8))))
         (equalp left right))
        ((and (consp left) (consp right))
         (and (reseed-activation-layout-facts-equal-p (car left) (car right))
              (reseed-activation-layout-facts-equal-p (cdr left) (cdr right))))
        (t (equal left right))))

(defun call-with-reseed-activation-layout-fixture (kind function)
  ;; Construct proposed layouts under owned fixture descriptors.  This does not
  ;; publish an activation journal or exercise activation operation authority.
  (catch 'activation-layout-checked
    (call-with-reseed-configuration-input-fixture
     (lambda (owner root workspace record plan root-fd saved-fd base-fds check &optional descriptors)
       (multiple-value-bind (base control binding model images row)
           (pg::physical-reseed-follow-configuration-base-proof
            root-fd saved-fd base-fds plan record 0 check)
         (declare (ignore base control model row))
         (let ((moves (pg::physical-reseed-follow-configuration-moves record binding)))
           (dolist (move moves)
             (destructuring-bind (logical from-role from-leaf to-role to-leaf) move
               (declare (ignore logical))
               (unless (zerop (pg::physical-adoption-renameat2
                               (cdr (assoc from-role descriptors)) from-leaf
                               (cdr (assoc to-role descriptors)) to-leaf
                               pg::+physical-adoption-rename-noreplace+))
                 (error "activation fixture installation move failed"))))
           (let* ((candidate (pg::physical-adoption-attempt-value-copy record))
                  (configuration (getf candidate :configuration))
                  (successor (pg::physical-reseed-follow-settlement-successor candidate)))
             (setf (getf configuration :phase) :inert
                   (getf configuration :step) (length moves)
                   (getf candidate :settlement)
                   (list :phase :handed-off :successor successor
                         :receipt (list :version 1 :request (getf (getf candidate :attempt) :request)
                                        :phase :closed
                                        :base (pg::physical-reseed-follow-settlement-base candidate)
                                        :outcome :handed-off :successor successor)))
             (let* ((bytes (pg::physical-follow-config-image
                            (pg::physical-reseed-follow-activation-config candidate binding)))
                    (entry (pg::physical-reseed-follow-configuration-write-file
                            owner (cdr (assoc :staged descriptors)) "follow.active" "follow.active"
                            bytes check :prepare t)))
               (setf (getf candidate :activation)
                     (list :phase :staged :step 0
                           :image (list (length bytes)
                                        (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
                           :entry entry))
               (funcall function owner root workspace
                        (pg::physical-reseed-follow-copy candidate) binding images descriptors check)
               (throw 'activation-layout-checked :checked))))))
     kind)))

(deftest reseed-retirement-attempt-observes-root-and-one-ahead-archive
    (call-with-reseed-activation-layout-fixture
     :same-timeline
     (lambda (owner root workspace record binding images descriptors check)
       (declare (ignore owner root workspace binding images check))
       (setf (getf (getf record :activation) :phase) :active
             (getf (getf record :activation) :step) 2)
       (let* ((root-fd (cdr (assoc :root descriptors)))
              (saved-fd (cdr (assoc :saved descriptors)))
              (state (funcall (symbol-function 'pg::physical-reseed-follow-retirement-attempt-at)
                              root-fd root-fd record 0))
              (reference (list :identity (copy-list (getf state :identity))
                               :digest (getf state :digest))))
         (ok (equal (getf record :attempt) (getf state :record)))
         (ok (= (sb-posix:geteuid) (fourth (getf state :identity))))
         (ok (= 1 (fifth (getf state :identity))))
         (ok (handler-case
                 (progn (pg::physical-reseed-follow-retirement-attempt-at saved-fd saved-fd record 0) nil)
               (error (condition) (search "PGDATA binding" (princ-to-string condition)))))
         (setf (getf record :retirement)
               (list :phase :retiring :step 0 :attempt-file reference))
         (ok (equal state (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 1)))
         (decf (eighth (getf reference :identity)))
         (ok (handler-case
                 (progn (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 0) nil)
               (error (condition) (search "frozen attempt file reference" (princ-to-string condition)))))
         (incf (eighth (getf reference :identity)))
         (unless (zerop (pg::physical-adoption-renameat2
                         root-fd "physical-reseed.attempt" saved-fd "physical-reseed.attempt"
                         pg::+physical-adoption-rename-noreplace+))
           (error "attempt archive fixture move failed"))
         (setf (getf (getf record :retirement) :step) 1)
         (let ((archived (pg::physical-reseed-follow-retirement-attempt-at root-fd saved-fd record 2)))
           (ok (equal (getf state :record) (getf archived :record)))
           (ok (= (getf state :digest) (getf archived :digest)))
           (ok (equal (butlast (getf state :identity)) (butlast (getf archived :identity))))
           (decf (eighth (getf reference :identity)))
           (ok (pg::physical-reseed-follow-retirement-attempt-at root-fd saved-fd record 2))
           (incf (seventh (getf reference :identity)))
           (ok (handler-case
                   (progn (pg::physical-reseed-follow-retirement-attempt-at root-fd saved-fd record 2) nil)
                 (error (condition) (search "frozen attempt file reference" (princ-to-string condition)))))
           (decf (seventh (getf reference :identity))))
         (ok (handler-case
                 (progn (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 2) nil)
               (error (condition) (search "attempt container disagrees" (princ-to-string condition)))))))))

(deftest reseed-retirement-attempt-refuses-abort-replacement-and-same-byte-write
    (dolist (kind '(:aborting :replacement :same-byte-write))
      (call-with-reseed-activation-layout-fixture
       :same-timeline
       (lambda (owner root workspace record binding images descriptors check)
         (declare (ignore owner workspace binding images check))
         (setf (getf (getf record :activation) :phase) :active
               (getf (getf record :activation) :step) 2)
         (let* ((root-fd (cdr (assoc :root descriptors)))
                (path (merge-pathnames "physical-reseed.attempt" root))
                (saved (merge-pathnames "attempt.original" root))
                (original (lineage-test-bytes path))
                (bytes original)
                (fired nil))
           (when (eq kind :aborting)
             (let ((aborted (pg::physical-reseed-attempt-copy (getf record :attempt))))
               (setf (getf aborted :phase) :aborting
                     bytes (pg::physical-reseed-attempt-image aborted))
               (with-open-file (out path :direction :output :if-exists :overwrite
                                         :element-type '(unsigned-byte 8))
                 (write-sequence bytes out)
                 (finish-output out)
                 (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (length bytes)))))
           (let ((pg::*physical-reseed-attempt-test-hook*
                   (lambda (point observed)
                     (declare (ignore observed))
                     (when (and (not fired) (eq point :after-attempt-open)
                                (not (eq kind :aborting)))
                       (setf fired t)
                       (when (eq kind :replacement)
                         (sb-posix:rename (namestring path) (namestring saved)))
                       (with-open-file (out path :direction :output :if-exists :overwrite
                                                 :if-does-not-exist :create :element-type '(unsigned-byte 8))
                         (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                         (write-sequence bytes out))))))
             (ok (handler-case
                     (progn (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 0) nil)
                   (error (condition)
                     (search (if (eq kind :aborting) "exact active original attempt" "changed")
                             (princ-to-string condition))))))
           (ok (eq fired (not (eq kind :aborting))))
           (ok (equalp bytes (lineage-test-bytes path)))
           (when (eq kind :replacement)
             (ok (equalp original (lineage-test-bytes saved)))
             (ok (/= (sb-posix:stat-ino (sb-posix:lstat (namestring path)))
                     (sb-posix:stat-ino (sb-posix:lstat (namestring saved)))))))))))

(defun reseed-retirement-layout-write (root record)
  (let ((bytes (pg::physical-reseed-follow-image record)))
    (with-open-file (out (merge-pathnames "physical-reseed.follow" root)
                         :direction :output :if-exists :overwrite
                         :element-type '(unsigned-byte 8))
      (write-sequence bytes out)
      (finish-output out)
      (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (length bytes)))))

(defun call-with-reseed-retirement-layout-fixture (kind function)
  (call-with-reseed-activation-layout-fixture
   kind
   (lambda (owner root workspace record binding images descriptors check)
     (declare (ignore check))
     (dolist (move (pg::physical-reseed-follow-activation-moves record binding))
       (destructuring-bind (logical from-role from-leaf to-role to-leaf) move
         (declare (ignore logical))
         (unless (zerop (pg::physical-adoption-renameat2
                         (cdr (assoc from-role descriptors)) from-leaf
                         (cdr (assoc to-role descriptors)) to-leaf
                         pg::+physical-adoption-rename-noreplace+))
           (error "retirement fixture activation move failed"))))
     (setf (getf (getf record :activation) :phase) :active
           (getf (getf record :activation) :step) 2)
     (reseed-retirement-layout-write root record)
     (flet ((root-check () (pg::physical-directory-check-owner-common owner)))
       (funcall function owner root workspace record binding images descriptors #'root-check)))))

(deftest reseed-retirement-layout-proves-every-archive-prefix-read-only
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (ok
       (eq :checked
           (call-with-reseed-retirement-layout-fixture
            kind
            (lambda (owner root workspace record binding images descriptors check)
              (declare (ignore binding images))
              (let* ((root-fd (cdr (assoc :root descriptors)))
                     (saved-fd (cdr (assoc :saved descriptors)))
                     (attempt (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 0))
                     (reference (list :identity (getf attempt :identity) :digest (getf attempt :digest)))
                     (plan (pg::physical-reseed-install-owner-plan owner))
                     (directories (list root workspace
                                        (pathname (getf (getf plan :incoming) :directory))
                                        (pathname (getf (getf plan :quarantine) :directory)))))
                (loop for prefix from 0 to 2
                      do (reseed-retirement-layout-write root record)
                         (let* ((snapshot (pg::physical-reseed-follow-snapshot-at root-fd))
                                (before (mapcar #'reseed-mapped-test-tree-facts directories))
                                (state (funcall (symbol-function 'pg::physical-reseed-follow-retirement-layout-state)
                                                descriptors snapshot check)))
                           (ok (= prefix (getf state :prefix)))
                           (ok (= 10 (length (getf (getf state :layout) :directories))))
                           (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))
                           (ok (reseed-activation-layout-facts-equal-p
                                before (mapcar #'reseed-mapped-test-tree-facts directories))))
                         (when (< prefix 2)
                           (setf (getf record :retirement)
                                 (list :phase :retiring :step prefix :attempt-file reference))
                           (let ((name (if (zerop prefix) "physical-reseed.install" "physical-reseed.attempt")))
                             (unless (zerop (pg::physical-adoption-renameat2
                                             root-fd name saved-fd name pg::+physical-adoption-rename-noreplace+))
                               (error "retirement fixture archive move failed")))))
                (setf (getf record :retirement)
                      (list :phase :retired :step 2 :attempt-file reference))
                (reseed-retirement-layout-write root record)
                (ok (= 2 (getf (pg::physical-reseed-follow-retirement-layout-state
                                descriptors (pg::physical-reseed-follow-snapshot-at root-fd) check)
                               :prefix))))))))))

(deftest reseed-retirement-layout-refuses-fences-gaps-and-wrong-saved-role
    (dolist (fault '(:temporary :pid :snapshot-replacement :fake-saved :attempt-gap
                     :duplicate-install :missing-install))
      (call-with-reseed-retirement-layout-fixture
       :same-timeline
       (lambda (owner root workspace record binding images descriptors check)
         (declare (ignore owner record binding images))
         (let* ((root-fd (cdr (assoc :root descriptors)))
                (saved-fd (cdr (assoc :saved descriptors)))
                (staged-fd (cdr (assoc :staged descriptors)))
                (snapshot (pg::physical-reseed-follow-snapshot-at root-fd))
                (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                (message
                  (ecase fault
                    ((:temporary :pid) "refuses temporary follow or PID")
                    (:snapshot-replacement "final generation changed")
                    (:fake-saved "saved original control directory")
                    (:attempt-gap "unique complete archive prefix")
                    ((:duplicate-install :missing-install) "one root or saved location"))))
           (ecase fault
             ((:temporary :pid)
              (reseed-follow-owner-test-write
               (merge-pathnames (if (eq fault :temporary) "physical-reseed.follow.tmp" "postmaster.pid") root)
               #(1 2 3)))
             (:snapshot-replacement
              (sb-posix:rename (namestring (merge-pathnames "physical-reseed.follow" root))
                               (namestring (merge-pathnames "follow.original" root)))
              (reseed-follow-owner-test-write (merge-pathnames "physical-reseed.follow" root) journal))
             (:fake-saved
              (unless (zerop (pg::physical-adoption-renameat2
                              root-fd "physical-reseed.attempt" staged-fd "physical-reseed.attempt"
                              pg::+physical-adoption-rename-noreplace+))
                (error "fake archive fixture move failed"))
              (setf (cdr (assoc :saved descriptors)) staged-fd))
             (:attempt-gap
              (unless (zerop (pg::physical-adoption-renameat2
                              root-fd "physical-reseed.attempt" saved-fd "physical-reseed.attempt"
                              pg::+physical-adoption-rename-noreplace+))
                (error "attempt gap fixture move failed")))
             (:duplicate-install
              (sb-posix:link (namestring (merge-pathnames "physical-reseed.install" root))
                             (namestring (merge-pathnames "saved/physical-reseed.install" workspace))))
             (:missing-install
              (sb-posix:rename (namestring (merge-pathnames "physical-reseed.install" root))
                               (namestring (merge-pathnames "staged/physical-reseed.install" workspace)))))
           (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
             (ok (handler-case
                     (progn (pg::physical-reseed-follow-retirement-layout-state descriptors snapshot check) nil)
                   (error (condition) (search message (princ-to-string condition)))))
             (ok (reseed-activation-layout-facts-equal-p
                  before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))))))))

(deftest reseed-retirement-layout-rechecks-each-journal-after-content-proof
    (dolist (kind '(:follow :install :attempt :temporary :pid))
      (call-with-reseed-retirement-layout-fixture
       :same-timeline
       (lambda (owner root workspace record binding images descriptors check)
         (declare (ignore owner workspace record binding images))
         (let* ((root-fd (cdr (assoc :root descriptors)))
                (snapshot (pg::physical-reseed-follow-snapshot-at root-fd))
                (path (merge-pathnames
                       (ecase kind (:follow "physical-reseed.follow")
                              (:install "physical-reseed.install")
                              (:attempt "physical-reseed.attempt")
                              (:temporary "physical-reseed.follow.tmp")
                              (:pid "postmaster.pid")) root))
                (bytes (if (member kind '(:temporary :pid)) #(1 2 3) (lineage-test-bytes path)))
                (original (symbol-function 'pg::physical-reseed-follow-activation-active-row))
                (armed nil) (fired nil) (message nil))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-reseed-follow-activation-active-row)
                        (lambda (&rest arguments)
                          (multiple-value-prog1 (apply original arguments)
                            (setf armed t))))
                  (handler-case
                      (pg::physical-reseed-follow-retirement-layout-state
                       descriptors snapshot
                       (lambda ()
                         (funcall check)
                         (when (and armed (not fired))
                           (setf fired t)
                           (with-open-file (out path :direction :output :if-exists :overwrite
                                                     :if-does-not-exist :create :element-type '(unsigned-byte 8))
                             (write-sequence bytes out)))))
                    (error (condition) (setf message (princ-to-string condition)))))
             (setf (symbol-function 'pg::physical-reseed-follow-activation-active-row) original))
           (ok fired)
           (ok (and message (search (case kind
                                      (:follow "final generation changed")
                                      ((:temporary :pid) "refuses temporary follow or PID")
                                      (t "retirement")) message)))
           (ok (equalp bytes (lineage-test-bytes path))))))))

(deftest reseed-activation-layout-proves-every-actual-move-prefix-read-only
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (ok
       (eq :checked
           (call-with-reseed-activation-layout-fixture
            kind
            (lambda (owner root workspace record binding images descriptors check)
              (let* ((journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                     (control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root)))
                     (receive (cdr (assoc :receive descriptors)))
                     (inode (sb-posix:stat-ino (sb-posix:fstat receive)))
                     (moves (pg::physical-reseed-follow-activation-moves record binding)))
                (loop for prefix from 0 to 2
                      do (let* ((before (list (reseed-mapped-test-tree-facts root)
                                              (reseed-mapped-test-tree-facts workspace)))
                                (state (funcall
                                        (symbol-function 'pg::physical-reseed-follow-activation-layout-state)
                                        owner descriptors record binding images prefix check)))
                           (ok (reseed-activation-layout-facts-equal-p
                                before (list (reseed-mapped-test-tree-facts root)
                                             (reseed-mapped-test-tree-facts workspace))))
                           (ok (= 10 (length (getf state :directories))))
                           (ok (= (length images) (length (getf state :images))))
                           (ok (equal "follow.active" (first (getf state :active))))
                           (ok (equal (pg::physical-reseed-follow-activation-inventories
                                       (pg::physical-reseed-install-owner-plan owner)
                                       record binding prefix
                                       (pg::physical-reseed-install-extra-entries
                                        owner (cdr (assoc :root descriptors))))
                                      (getf state :names))))
                         (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                         (ok (equalp control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
                         (ok (= inode (sb-posix:stat-ino (sb-posix:fstat receive))))
                         (when (< prefix 2)
                           (destructuring-bind (logical from-role from-leaf to-role to-leaf) (nth prefix moves)
                             (declare (ignore logical))
                             (unless (zerop (pg::physical-adoption-renameat2
                                             (cdr (assoc from-role descriptors)) from-leaf
                                             (cdr (assoc to-role descriptors)) to-leaf
                                             pg::+physical-adoption-rename-noreplace+))
                               (error "activation fixture config move failed"))))))))))))

(deftest reseed-activation-layout-rejects-wrong-prefix-and-preserves-extra-file
    (call-with-reseed-activation-layout-fixture
     :same-timeline
     (lambda (owner root workspace record binding images descriptors check)
       (declare (ignore root workspace))
       (dolist (prefix '(1 2))
         (ok (intake-fails-p
              (lambda () (pg::physical-reseed-follow-activation-layout-state
                          owner descriptors record binding images prefix check)))))
       (let* ((saved (cdr (assoc :saved descriptors)))
              (bytes (pg::string->bytes "unrecognized saved entry"))
              (path (merge-pathnames "unexpected" (pg::physical-image-fd-path saved))))
         (pg::physical-reseed-follow-configuration-write-file
          owner saved "unexpected" "unexpected" bytes check :prepare t)
         (let ((inode (sb-posix:stat-ino (sb-posix:lstat path))))
           (ok (intake-fails-p
                (lambda () (pg::physical-reseed-follow-activation-layout-state
                            owner descriptors record binding images 0 check))))
           (ok (= inode (sb-posix:stat-ino (sb-posix:lstat path))))
           (ok (equalp bytes (lineage-test-bytes path))))))))

(deftest reseed-activation-layout-rejects-a-byte-identical-active-replacement
    (call-with-reseed-activation-layout-fixture
     :later-writer
     (lambda (owner root workspace record binding images descriptors check)
       (declare (ignore root workspace))
       (let* ((staged (cdr (assoc :staged descriptors)))
              (path (merge-pathnames "follow.active" (pg::physical-image-fd-path staged)))
              (bytes (lineage-test-bytes path))
              (inode (fifth (getf (getf record :activation) :entry))))
         (with-open-file (held path :element-type '(unsigned-byte 8))
           (ok (= inode (sb-posix:stat-ino (sb-posix:fstat (sb-sys:fd-stream-fd held)))))
           (sb-posix:unlink path)
           (let ((replacement (pg::physical-reseed-follow-configuration-write-file
                               owner staged "follow.active" "follow.active" bytes check :prepare t)))
             (ok (/= inode (fifth replacement)))
             (ok (equalp bytes (lineage-test-bytes path)))
             (ok (intake-fails-p
                  (lambda () (pg::physical-reseed-follow-activation-layout-state
                              owner descriptors record binding images 0 check))))
             (ok (equalp bytes (lineage-test-bytes path)))))))))

(deftest reseed-activation-layout-rechecks-active-bytes-after-the-proof
    (call-with-reseed-activation-layout-fixture
     :same-timeline
     (lambda (owner root workspace record binding images descriptors check)
       (declare (ignore root workspace))
       (let* ((path (merge-pathnames "follow.active"
                                     (pg::physical-image-fd-path (cdr (assoc :staged descriptors)))))
              (before (lineage-test-bytes path))
              (calls 0))
         (ok (intake-fails-p
              (lambda ()
                (pg::physical-reseed-follow-activation-layout-state
                 owner descriptors record binding images 0
                 (lambda ()
                   (funcall check)
                   (when (= 2 (incf calls))
                     (reseed-mapped-test-change-first-byte path)))))))
         (ok (= 2 calls))
         (ok (not (equalp before (lineage-test-bytes path))))))))

(deftest reseed-activation-output-proves-only-absent-or-complete-building-files
    (call-with-reseed-activation-layout-fixture
     :same-timeline
     (lambda (owner root workspace record binding images descriptors check)
       (declare (ignore workspace))
       (let* ((building (pg::physical-adoption-attempt-value-copy record))
              (path (merge-pathnames "follow.active"
                                     (pg::physical-image-fd-path (cdr (assoc :staged descriptors)))))
              (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (setf (getf (getf building :activation) :phase) :building
               (getf (getf building :activation) :entry) nil)
         (let* ((before (pg::physical-adoption-attempt-value-copy building))
                (state (funcall (symbol-function 'pg::physical-reseed-follow-activation-output-state)
                                owner descriptors building binding images check)))
           (ok (equal (getf (getf record :activation) :entry) (getf state :entry)))
           (ok (equal before building)))
         (ok (intake-fails-p
              (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-output-state)
                                  owner descriptors record binding images check))))
         (let ((unselected (pg::physical-adoption-attempt-value-copy building)))
           (remf unselected :activation)
           (ok (intake-fails-p
                (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-output-state)
                                    owner descriptors unselected binding images check)))))
         (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8)
                                      :if-exists :overwrite)
           (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) 1))
         (let ((partial (lineage-test-bytes path)))
           (ok (= 1 (length partial)))
           (ok (intake-fails-p
                (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-output-state)
                                    owner descriptors building binding images check))))
           (ok (equalp partial (lineage-test-bytes path))))
         (sb-posix:unlink path)
         (let ((state (funcall (symbol-function 'pg::physical-reseed-follow-activation-output-state)
                               owner descriptors building binding images check)))
           (ok (null (getf state :entry)))
           (ok (not (probe-file path))))
         (let ((unselected (pg::physical-adoption-attempt-value-copy building)))
           (remf unselected :activation)
           (ok (null (getf (funcall
                            (symbol-function 'pg::physical-reseed-follow-activation-output-state)
                            owner descriptors unselected binding images check) :entry))))
         (let ((bad (pg::physical-adoption-attempt-value-copy building)))
           (incf (second (getf (getf bad :activation) :image)))
           (ok (pg::physical-reseed-follow-copy bad))
           (ok (intake-fails-p
                (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-output-state)
                                    owner descriptors bad binding images check)))))
         (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-activation-output-preserves-directories-symlinks-and-empty-files
    (call-with-reseed-activation-layout-fixture
     :same-timeline
     (lambda (owner root workspace record binding images descriptors check)
       (declare (ignore root workspace))
       (let* ((building (pg::physical-adoption-attempt-value-copy record))
              (path (merge-pathnames "follow.active"
                                     (pg::physical-image-fd-path (cdr (assoc :staged descriptors))))))
         (setf (getf (getf building :activation) :phase) :building
               (getf (getf building :activation) :entry) nil)
         (sb-posix:unlink path)
         (dolist (kind '(:directory :symlink :empty))
           (ecase kind
             (:directory (sb-posix:mkdir path #o700))
             (:symlink (sb-posix:symlink "missing-active-target" path))
             (:empty (with-open-file (stream path :direction :output :if-exists :error)
                       (finish-output stream))
              (sb-posix:chmod path #o600)))
           (let ((inode (sb-posix:stat-ino (sb-posix:lstat path))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-reseed-follow-activation-output-state
                              owner descriptors building binding images check))))
             (ok (= inode (sb-posix:stat-ino (sb-posix:lstat path)))))
           (if (eq kind :directory) (sb-posix:rmdir path) (sb-posix:unlink path)))))))

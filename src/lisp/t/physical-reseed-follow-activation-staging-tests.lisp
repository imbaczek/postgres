(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-activation-staged-record-preserves-building
    ((selected boolean))
  (:about pg::physical-reseed-follow-activation-staged-record)
  (:trials (:normal 100))
  (multiple-value-bind (base groups) (reseed-follow-activation-codec-fixture selected)
    (let* ((before (reseed-follow-with-activation base (first groups)))
           (entry (getf (second groups) :entry))
           (saved (pg::physical-adoption-attempt-value-copy (list before entry)))
           (actual (funcall (symbol-function 'pg::physical-reseed-follow-activation-staged-record)
                            before entry))
           (expected (reseed-follow-with-activation base (second groups))))
      (and (equal actual expected)
           (equal saved (list before entry))))))

(cl-spec:defproperty reseed-activation-initial-intent-preserves-staged
    ((selected boolean))
  (:about pg::physical-reseed-follow-activation-intent-record)
  (:trials (:normal 100))
  (multiple-value-bind (base groups) (reseed-follow-activation-codec-fixture selected)
    (let* ((before (reseed-follow-with-activation base (second groups)))
           (saved (pg::physical-adoption-attempt-value-copy before))
           (actual (funcall (symbol-function 'pg::physical-reseed-follow-activation-intent-record) before))
           (expected (reseed-follow-with-activation base (third groups))))
      (and (equal actual expected)
           (equal saved before)
           (not (eq (getf actual :activation) (getf before :activation)))
           (not (eq (getf (getf actual :activation) :entry)
                    (getf (getf before :activation) :entry)))))))

(deftest generated-reseed-activation-initial-intent-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-activation-initial-intent-preserves-staged
                                       :seed 136 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-activation-initial-intent-refuses-every-other-phase
    (multiple-value-bind (base groups) (reseed-follow-activation-codec-fixture t)
      (dolist (group (cons nil (cons (first groups) (cddr groups))))
        (let* ((record (reseed-follow-with-activation base group))
               (saved (pg::physical-adoption-attempt-value-copy record)))
          (ok (signals (activation-test-call 'pg::physical-reseed-follow-activation-intent-record record)))
          (ok (equal saved record))))))

(cl-spec:defproperty reseed-activation-progress-preserves-intent
    ((selected boolean))
  (:about pg::physical-reseed-follow-activation-progress-record)
  (:trials (:normal 100))
  (multiple-value-bind (base groups) (reseed-follow-activation-codec-fixture selected)
    (loop for (old new) on (cddr groups) while new
          always
          (let* ((before (reseed-follow-with-activation base old))
                 (saved (pg::physical-adoption-attempt-value-copy before))
                 (actual (funcall
                          (symbol-function 'pg::physical-reseed-follow-activation-progress-record)
                          before))
                 (expected (reseed-follow-with-activation base new)))
            (and (equal actual expected)
                 (equal saved before)
                 (not (eq (getf actual :activation) (getf before :activation)))
                 (not (eq (getf (getf actual :activation) :entry)
                          (getf (getf before :activation) :entry))))))))

(deftest generated-reseed-activation-progress-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-activation-progress-preserves-intent
                                       :seed 138 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-activation-progress-refuses-other-phases
    (multiple-value-bind (base groups) (reseed-follow-activation-codec-fixture t)
      (dolist (group (list nil (first groups) (second groups) (sixth groups)))
        (let* ((record (reseed-follow-with-activation base group))
               (saved (pg::physical-adoption-attempt-value-copy record)))
          (ok (signals
               (activation-test-call 'pg::physical-reseed-follow-activation-progress-record record)))
          (ok (equal saved record))))))

(defun call-with-reseed-activation-source (owner root record function)
  (call-with-reseed-settlement-source
   owner root record
   (lambda (transport)
     (let* ((generation (first (getf record :generations)))
            (state (append (list :connects 0 :prepares 0
                                 :anchor (pg::physical-reseed-follow-generation-anchor generation))
                           transport))
            (symbols '(pg::pg-client-native-backup-lease-follow-prepare pg::physical-libpq-connect-frozen))
            (originals (mapcar #'symbol-function symbols)))
       (unwind-protect
            (progn
              (setf (symbol-function (first symbols))
                    (lambda (client request number name coordinates)
                      (pg::physical-adoption-attempt-check-client client (getf (getf record :attempt) :source))
                      (assert (null pg::*physical-reseed-follow-activation-scope*))
                      (assert (null pg::*physical-reseed-follow-installation-scope*))
                      (assert (equal request (getf (getf record :attempt) :request)))
                      (assert (= number (getf generation :generation)))
                      (assert (equal name (getf generation :name)))
                      (assert (equal coordinates (getf generation :coordinates)))
                      (incf (getf state :prepares))
                      (pg::physical-follow-anchor-copy (getf state :anchor)))
                    (symbol-function (second symbols))
                    (lambda (&rest arguments)
                      (incf (getf state :connects))
                      (apply (second originals) arguments)))
              (funcall function state))
         (loop for symbol in symbols for original in originals
               do (setf (symbol-function symbol) original)))))))

(deftest reseed-activation-start-publishes-one-source-authorized-intent
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-activation-fixture
       kind
       (lambda (owner root workspace settled)
         (declare (ignore settled))
         (pg::physical-reseed-follow-begin-activation owner)
         (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
                (before (activation-test-unrelated-tree root workspace))
                (previous (activation-test-snapshot owner))
                (captured nil))
           (call-with-reseed-activation-source
            owner root staged
            (lambda (state)
              (let* ((pg::*physical-reseed-follow-test-hook*
                       (lambda (event path)
                         (declare (ignore path))
                         (when (eq event :after-follow-rename)
                           (setf captured pg::*physical-reseed-follow-activation-authority*)
                           (ok (eq captured
                                   (pg::physical-reseed-follow-activation-authority
                                    (pg::physical-reseed-follow-activation-authority-context captured)))))))
                     (actual (activation-test-call 'pg::physical-reseed-follow-start-activation
                                                   owner :helper "/unused/helper"))
                     (snapshot (activation-test-snapshot owner)))
                (ok (equal actual (pg::physical-reseed-follow-activation-intent-record staged)))
                (ok (equal actual (pg::physical-reseed-follow-read owner)))
                (ok (not (equal previous snapshot)))
                (ok (equalp before (activation-test-unrelated-tree root workspace)))
                (ok (equal actual (activation-test-call 'pg::physical-reseed-follow-start-activation
                                                        owner :helper "/unused/helper")))
                (ok (equal snapshot (activation-test-snapshot owner)))
                (ok (= 1 (getf state :prepares) (getf state :closes)))
                (ok (= 4 (getf state :identities)))
                (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                (ok captured)
                (ok (not (pg::physical-reseed-follow-activation-authority-active-p captured)))
                (let ((pg::*physical-reseed-follow-activation-authority* captured))
                  (ok (signals
                       (pg::physical-reseed-follow-activation-authority
                        (pg::physical-reseed-follow-activation-authority-context captured)))))
                (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
                (unwind-protect
                     (let* ((local (pg::call-with-physical-reseed-follow-image
                                    owner
                                    (lambda (fd check)
                                      (declare (ignore fd check))
                                      (pg::physical-reseed-follow-activation-local-snapshot
                                       (pg::physical-reseed-follow-activation-context owner)))))
                            (activation (getf (getf (getf local :follow) :record) :activation)))
                       (ok (eq :activating (getf activation :phase)))
                       (ok (signals (pg::physical-reseed-follow-activation-network-snapshot owner))))
                  (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))))))))))

(deftest reseed-activation-initial-intent-recovery-reprobes-before-promoting
    ;; A complete temporary needs a fresh source observation before promotion.
    (let ((staged nil))
      (call-with-reseed-activation-fixture
       :same-timeline
       (lambda (owner root workspace settled)
         (declare (ignore workspace settled))
         (pg::physical-reseed-follow-begin-activation owner)
         (setf staged (pg::physical-reseed-follow-stage-activation owner))
         (call-with-reseed-activation-source
          owner root staged
          (lambda (state)
            (let ((pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event :after-follow-temporary-fsync)
                        (error "cut initial activation before rename")))))
              (ok (intake-fails-p
                   (lambda () (activation-test-call 'pg::physical-reseed-follow-start-activation
                                                    owner :helper "/unused/helper")))))
            (ok (= 1 (getf state :prepares) (getf state :closes)))
            (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
            (ok (equal (pg::physical-reseed-follow-activation-intent-record staged)
                       (pg::physical-reseed-follow-decode
                        (lineage-test-bytes (merge-pathnames "physical-reseed.follow.tmp" root))))))))
       (lambda (root workspace)
         (let ((before (list (reseed-mapped-test-tree-facts root)
                             (reseed-mapped-test-tree-facts workspace))))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let* ((symbol 'pg::physical-libpq-connect-frozen)
                     (original (symbol-function symbol))
                     (connects 0))
                (unwind-protect
                     (progn
                       (setf (symbol-function symbol)
                             (lambda (&rest arguments)
                               (declare (ignore arguments))
                               (incf connects)
                               (error "source offline during initial activation recovery")))
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-follow-recover-activation owner :helper "/unused/helper"))))
                       (ok (= 1 connects))
                       (ok (pg::physical-reseed-follow-owner-uncertain-p owner)))
                  (setf (symbol-function symbol) original)))))
           (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                    (reseed-mapped-test-tree-facts workspace)))))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root
          (lambda (owner)
            (call-with-reseed-activation-source
             owner root staged
             (lambda (state)
               (let ((record (pg::physical-reseed-follow-recover-activation owner :helper "/unused/helper")))
                 (ok (equal record (pg::physical-reseed-follow-activation-intent-record staged)))
                 (ok (= 1 (getf state :prepares) (getf state :closes)))
                 (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                 (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))))))))

(deftest reseed-activation-initial-intent-reconciles-every-journal-cut
    (dolist (cut '(:after-follow-temporary-create :after-follow-temporary-fsync
                   :before-follow-rename :after-follow-rename :after-follow-directory-fsync))
      (let ((staged nil))
        (call-with-reseed-activation-fixture
         :same-timeline
         (lambda (owner root workspace settled)
           (declare (ignore workspace settled))
           (pg::physical-reseed-follow-begin-activation owner)
           (setf staged (pg::physical-reseed-follow-stage-activation owner))
           (call-with-reseed-activation-source
            owner root staged
            (lambda (state)
              (let* ((hit nil)
                     (pg::*physical-reseed-follow-test-hook*
                       (lambda (event path)
                         (declare (ignore path))
                         (when (eq event cut) (setf hit t) (error "initial activation journal cut")))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-follow-start-activation owner :helper "/unused/helper"))))
                (ok hit)
                (ok (= 1 (getf state :connects) (getf state :prepares) (getf state :closes)))
                (ok (pg::physical-reseed-follow-owner-uncertain-p owner))))))
         (lambda (root workspace)
           (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                  (before (list (reseed-mapped-test-tree-facts root)
                                (reseed-mapped-test-tree-facts workspace)))
                  (preserved (activation-test-unrelated-tree root workspace)))
             (pg::call-with-physical-reseed-follow-configuration-owner
              root
              (lambda (owner)
                (call-with-reseed-activation-source
                 owner root staged
                 (lambda (state)
                   (if (eq cut :after-follow-temporary-create)
                       (progn
                         (ok (intake-fails-p
                              (lambda () (pg::physical-reseed-follow-recover-activation owner :helper "/unused/helper"))))
                         (ok (zerop (getf state :connects)))
                         (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                         (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                                  (reseed-mapped-test-tree-facts workspace)))))
                       (let* ((record (pg::physical-reseed-follow-recover-activation owner :helper "/unused/helper"))
                              (calls (if (member cut '(:after-follow-rename :after-follow-directory-fsync)) 0 1)))
                         (ok (equal record (pg::physical-reseed-follow-activation-intent-record staged)))
                         (ok (= calls (getf state :connects) (getf state :prepares) (getf state :closes)))
                         (ok (not (probe-file temporary)))
                         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))))
             (ok (reseed-activation-layout-facts-equal-p
                  preserved (activation-test-unrelated-tree root workspace)))))))))

(deftest reseed-activation-start-refuses-source-failures-without-publishing
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
              (before (list (reseed-mapped-test-tree-facts root)
                            (reseed-mapped-test-tree-facts workspace))))
         (dolist (mode '(:writer :anchor :lost-reply))
           (call-with-reseed-activation-source
            owner root staged
            (lambda (state)
              (let* ((symbol 'pg::pg-client-native-backup-lease-follow-prepare)
                     (original (symbol-function symbol)))
                (unwind-protect
                     (progn
                       (when (eq mode :writer) (setf (getf state :identity-timeline) 99))
                       (when (eq mode :anchor)
                         (setf (getf (getf state :anchor) :overlap-digest)
                               (logxor 1 (getf (getf state :anchor) :overlap-digest))))
                       (setf (symbol-function symbol)
                             (lambda (&rest arguments)
                               (prog1 (apply original arguments)
                                 (when (eq mode :lost-reply) (error "lost initial intent reply")))))
                       (let ((message
                               (handler-case
                                   (progn (pg::physical-reseed-follow-start-activation owner :helper "/unused/helper") nil)
                                 (error (condition) (princ-to-string condition))))
                             (expected (ecase mode
                                         (:writer "saved writer or receive frontier")
                                         (:anchor "exact recorded anchor")
                                         (:lost-reply "lost initial intent reply"))))
                         (ok (and message (search expected message))))
                       (ok (= 1 (getf state :connects) (getf state :closes)))
                       (ok (= (if (eq mode :writer) 0 1) (getf state :prepares)))
                       (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                       (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                       (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                                (reseed-mapped-test-tree-facts workspace)))))
                  (setf (symbol-function symbol) original))))))))))

(deftest reseed-activation-recovery-probe-rejects-bad-temporaries-before-connect
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
         (dolist (mode '(:partial :identical :wrong-entry))
           (let ((candidate (pg::physical-reseed-follow-activation-intent-record staged)))
             (when (eq mode :wrong-entry)
               (incf (fifth (getf (getf candidate :activation) :entry)) 1000000))
             (unwind-protect
                  (progn
                    (reseed-follow-owner-test-write
                     temporary
                     (ecase mode
                       (:partial #())
                       (:identical (pg::physical-reseed-follow-image staged))
                       (:wrong-entry (pg::physical-reseed-follow-image candidate))))
                    (let ((before (list (reseed-mapped-test-tree-facts root)
                                        (reseed-mapped-test-tree-facts workspace))))
                      (call-with-reseed-activation-source
                       owner root staged
                       (lambda (state)
                         (ok (intake-fails-p
                              (lambda ()
                                (pg::physical-reseed-follow-probe-activation-source
                                 owner :helper "/unused/helper" :recovery t))))
                         (ok (zerop (getf state :connects)))
                         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                         (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                                  (reseed-mapped-test-tree-facts workspace))))))))
               (when (probe-file temporary) (delete-file temporary)))))))))

(deftest reseed-activation-start-rechecks-local-facts-after-the-probe
    (dolist (mode '(:journal :active))
      (call-with-reseed-activation-fixture
       :same-timeline
       (lambda (owner root workspace settled)
         (declare (ignore settled))
         (pg::physical-reseed-follow-begin-activation owner)
         (let ((staged (pg::physical-reseed-follow-stage-activation owner)))
           (call-with-reseed-activation-source
            owner root staged
            (lambda (state)
              (let* ((symbol 'pg::physical-reseed-follow-probe-activation-source)
                     (original (symbol-function symbol))
                     (hit nil) (after-change nil))
                (unwind-protect
                     (progn
                       (setf (symbol-function symbol)
                             (lambda (&rest arguments)
                               (multiple-value-prog1 (apply original arguments)
                                 (assert (null pg::*physical-reseed-follow-activation-scope*))
                                 (assert (not (pg::physical-reseed-follow-owner-busy-p owner)))
                                 (ecase mode
                                   (:journal (reseed-test-replace-follow-record root staged))
                                   (:active
                                    (let* ((path (merge-pathnames "staged/follow.active" workspace))
                                           (bytes (lineage-test-bytes path))
                                           (inode (sb-posix:stat-ino (sb-posix:lstat path))))
                                      (sleep 1.1)
                                      (with-open-file (stream path :direction :io :if-exists :overwrite
                                                                   :element-type '(unsigned-byte 8))
                                        (write-sequence bytes stream)
                                        (finish-output stream))
                                      (assert (= inode (sb-posix:stat-ino (sb-posix:lstat path))))
                                      (assert (equalp bytes (lineage-test-bytes path))))))
                                 (setf hit t
                                       after-change (list (reseed-mapped-test-tree-facts root)
                                                          (reseed-mapped-test-tree-facts workspace))))))
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-follow-start-activation owner :helper "/unused/helper"))))
                       (ok hit)
                       (ok (= 1 (getf state :connects) (getf state :prepares) (getf state :closes)))
                       (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                       (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                       (ok (equalp after-change (list (reseed-mapped-test-tree-facts root)
                                                      (reseed-mapped-test-tree-facts workspace)))))
                  (setf (symbol-function symbol) original))))))))))

(deftest reseed-activation-authority-refuses-anchor-and-candidate-tampering
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
              (before (list (reseed-mapped-test-tree-facts root)
                            (reseed-mapped-test-tree-facts workspace))))
         (call-with-reseed-activation-source
          owner root staged
          (lambda (state)
            (multiple-value-bind (anchor snapshot)
                (pg::physical-reseed-follow-probe-activation-source owner :helper "/unused/helper")
              (pg::call-with-physical-reseed-follow-activation-operation
               owner
               (lambda (root check)
                 (declare (ignore root))
                 (let ((context (pg::physical-reseed-follow-activation-context owner)))
                   (pg::call-with-physical-reseed-follow-activation-authority
                    context anchor snapshot
                    (lambda (authority)
                      (let* ((candidate (pg::physical-reseed-follow-copy
                                         (pg::physical-reseed-follow-activation-authority-candidate authority)))
                             (pg::*physical-reseed-follow-activation-candidate*
                               (pg::physical-reseed-follow-copy candidate)))
                        (ok (eq authority (pg::physical-reseed-follow-activation-authority context)))
                        (dolist (field '(:anchor :candidate))
                          (let ((saved-anchor (pg::physical-follow-anchor-copy
                                               (pg::physical-reseed-follow-activation-authority-anchor authority)))
                                (saved-candidate (pg::physical-reseed-follow-copy
                                                  (pg::physical-reseed-follow-activation-authority-candidate authority))))
                            (unwind-protect
                                 (progn
                                   (ecase field
                                     (:anchor
                                      (setf (getf (pg::physical-reseed-follow-activation-authority-anchor authority)
                                                  :overlap-digest)
                                            (logxor 1 (getf saved-anchor :overlap-digest))))
                                     (:candidate
                                      (setf (getf (getf (pg::physical-reseed-follow-activation-authority-candidate authority)
                                                        :activation) :step) 1)))
                                   (ok (signals
                                        (pg::physical-reseed-follow-configuration-publication-candidate
                                         owner context candidate))))
                              (setf (pg::physical-reseed-follow-activation-authority-anchor authority) saved-anchor
                                    (pg::physical-reseed-follow-activation-authority-candidate authority) saved-candidate))
                            (ok (equal candidate (pg::physical-reseed-follow-configuration-publication-candidate
                                                  owner context candidate)))))
                        (funcall check)
                        (values (pg::physical-reseed-follow-activation-context-record context)
                                (pg::physical-reseed-follow-activation-context-snapshot context)))))))))
            (ok (= 1 (getf state :connects) (getf state :prepares) (getf state :closes)))
            (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
            (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                     (reseed-mapped-test-tree-facts workspace))))))))))

(defun call-with-reseed-building-activation-fixture (kind function &optional after)
  (call-with-reseed-activation-fixture
   kind
   (lambda (owner root workspace settled)
     (declare (ignore settled))
     (funcall function owner root workspace (pg::physical-reseed-follow-begin-activation owner)))
   after))

(defun check-reseed-activation-output-blocks-owner (root)
  (let* ((symbol 'pg::physical-reseed-follow-activation-output-state)
         (original (symbol-function symbol)) (proofs 0) (admitted nil))
    (unwind-protect
         (progn
           (setf (symbol-function symbol)
                 (lambda (&rest arguments) (incf proofs) (apply original arguments)))
           (ok (intake-fails-p
                (lambda ()
                  (pg::call-with-physical-reseed-follow-configuration-owner
                   root (lambda (owner) (declare (ignore owner)) (setf admitted t))))))
           (ok (not admitted))
           (ok (plusp proofs)))
      (setf (symbol-function symbol) original))))

(deftest generated-reseed-activation-staged-record-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-activation-staged-record-preserves-building
                                       :seed 134 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-activation-staging-is-durable-and-idempotent-in-every-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-activation-fixture
       kind
       (lambda (owner root workspace settled)
         (declare (ignore settled))
         (let* ((building (pg::physical-reseed-follow-begin-activation owner))
                (pending (lineage-test-bytes (merge-pathnames "physical-follow.control" root)))
                (recovery (lineage-test-bytes (merge-pathnames "physical-recovery.control" root)))
                (staged (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))
                (activation (getf staged :activation))
                (path (merge-pathnames "staged/follow.active" workspace))
                (bytes (lineage-test-bytes path))
                (snapshot (activation-test-snapshot owner)))
           (ok (eq :staged (getf activation :phase)))
           (ok (zerop (getf activation :step)))
           (ok (equal (getf (getf building :activation) :image) (getf activation :image)))
           (ok (equal (getf activation :image)
                      (list (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
           (ok (equal staged (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))
           (ok (equal snapshot (activation-test-snapshot owner)))
           (ok (equal staged (pg::physical-reseed-follow-read owner)))
           (ok (equalp pending (lineage-test-bytes (merge-pathnames "physical-follow.control" root))))
           (ok (equalp recovery (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
           (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (eq :staged (getf (getf (pg::physical-reseed-follow-read owner) :activation) :phase)))))))))

(deftest reseed-activation-source-snapshot-is-detached-and-read-only
    ;; Check that repeated snapshots detach their facts and leave local state
    ;; unchanged.  The following probe cases exercise retries and changed facts.
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (pg::physical-reseed-follow-stage-activation owner)
       (let ((before (list (reseed-mapped-test-tree-facts root)
                           (reseed-mapped-test-tree-facts workspace))))
         (ok (signals (activation-test-call 'pg::physical-reseed-follow-activation-network-snapshot owner)))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (let* ((snapshot (activation-test-call 'pg::physical-reseed-follow-activation-network-snapshot owner))
                     (again (activation-test-call 'pg::physical-reseed-follow-activation-network-snapshot owner snapshot)))
                (ok (equal snapshot again))
                (ok (equal (getf snapshot :active-entry)
                           (getf (getf (getf (getf snapshot :follow) :record) :activation) :entry)))
                (ok (consp (getf snapshot :active-row)))
                (let ((borrowed nil))
                  (ok (equal snapshot
                             (pg::call-with-physical-reseed-follow-image
                              owner
                              (lambda (root check)
                                (declare (ignore root check))
                                (setf borrowed (pg::physical-reseed-follow-activation-context owner))
                                (pg::physical-reseed-follow-activation-local-snapshot borrowed snapshot)))))
                  (ok (signals (pg::physical-reseed-follow-activation-local-snapshot borrowed))))
                (ok (not (eq (getf snapshot :active-row) (getf again :active-row))))
                (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                         (reseed-mapped-test-tree-facts workspace))))
                (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                (ok (probe-file (merge-pathnames "staged/follow.active" workspace))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))))))

(deftest reseed-activation-source-probe-retries-exact-allocation-without-local-effects
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-activation-fixture
       kind
       (lambda (owner root workspace settled)
         (declare (ignore settled))
         (pg::physical-reseed-follow-begin-activation owner)
         (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
                (generation (first (getf staged :generations)))
                (expected (pg::physical-reseed-follow-generation-anchor generation))
                (before (list (reseed-mapped-test-tree-facts root)
                              (reseed-mapped-test-tree-facts workspace))))
           (call-with-reseed-settlement-source
            owner root staged
            (lambda (state)
              (let* ((symbol 'pg::pg-client-native-backup-lease-follow-prepare)
                     (original (symbol-function symbol))
                     (calls 0) (lose-reply t))
                (unwind-protect
                     (progn
                       (setf (symbol-function symbol)
                             (lambda (client request number name coordinates)
                               (declare (ignore client))
                               (assert (pg::physical-reseed-follow-owner-busy-p owner))
                               (assert (null pg::*physical-reseed-follow-activation-scope*))
                               (assert (null pg::*physical-reseed-follow-installation-scope*))
                               (assert (equal request (getf (getf staged :attempt) :request)))
                               (assert (= number (getf generation :generation)))
                               (assert (equal name (getf generation :name)))
                               (assert (equal coordinates (getf generation :coordinates)))
                               (incf calls)
                               (when lose-reply
                                 (setf lose-reply nil)
                                 (error "injected lost selected-allocation reply"))
                               (pg::physical-follow-anchor-copy expected)))
                       (ok (intake-fails-p
                            (lambda () (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                             owner :helper "/unused/helper"))))
                       (ok (= 1 calls (getf state :closes)))
                       (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                       (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                       (multiple-value-bind (anchor snapshot)
                           (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                 owner :helper "/unused/helper")
                         (ok (equal expected anchor))
                         (ok (equal staged (getf (getf snapshot :follow) :record))))
                       (ok (= 2 calls (getf state :closes)))
                       (ok (= 6 (getf state :identities)))
                       (ok (zerop (getf state :handoffs)))
                       (ok (zerop (getf state :statuses)))
                       (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                       (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                       (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                                (reseed-mapped-test-tree-facts workspace)))))
                  (setf (symbol-function symbol) original))))))))))

(deftest reseed-activation-source-probe-refuses-changed-writer-and-anchor
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
              (generation (first (getf staged :generations)))
              (expected (pg::physical-reseed-follow-generation-anchor generation))
              (before (list (reseed-mapped-test-tree-facts root)
                            (reseed-mapped-test-tree-facts workspace))))
         (call-with-reseed-settlement-source
          owner root staged
          (lambda (state)
            (let* ((symbol 'pg::pg-client-native-backup-lease-follow-prepare)
                   (original (symbol-function symbol))
                   (calls 0))
              (unwind-protect
                   (progn
                     (setf (symbol-function symbol)
                           (lambda (&rest arguments)
                             (declare (ignore arguments))
                             (incf calls)
                             (let ((reply (pg::physical-follow-anchor-copy expected)))
                               (setf (getf reply :overlap-digest)
                                     (logxor 1 (getf reply :overlap-digest)))
                               (pg::physical-follow-anchor-copy reply))))
                     (setf (getf state :identity-timeline) 99)
                     (ok (intake-fails-p
                          (lambda () (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                           owner :helper "/unused/helper"))))
                     (ok (zerop calls))
                     (ok (= 1 (getf state :closes)))
                     (setf (getf state :identity-timeline) nil)
                     (let ((message
                             (handler-case
                                 (progn (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                              owner :helper "/unused/helper") nil)
                               (error (condition) (princ-to-string condition)))))
                       (ok (and message (search "exact recorded anchor" message))))
                     (ok (= 1 calls))
                     (ok (= 2 (getf state :closes)))
                     (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                     (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                     (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                              (reseed-mapped-test-tree-facts workspace)))))
                (setf (symbol-function symbol) original)))))))))

(deftest reseed-activation-source-probe-detaches-anchor-and-rechecks-client-close
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
              (expected (pg::physical-reseed-follow-generation-anchor (first (getf staged :generations)))))
         (call-with-reseed-settlement-source
          owner root staged
          (lambda (state)
            (let* ((symbol 'pg::pg-client-native-backup-lease-follow-prepare)
                   (original (symbol-function symbol))
                   (reply nil) (after-abort nil))
              (unwind-protect
                   (progn
                     (setf (symbol-function symbol)
                           (lambda (&rest arguments)
                             (declare (ignore arguments))
                             (setf reply (pg::physical-follow-anchor-copy expected)))
                           (getf state :on-close)
                           (lambda ()
                             (ok (pg::physical-reseed-follow-owner-busy-p owner))
                             (ok (intake-fails-p
                                  (lambda () (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                                   owner :helper "/unused/helper"))))
                             (incf (getf reply :overlap-digest))))
                     (ok (equal expected
                                (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                      owner :helper "/unused/helper")))
                     (setf (getf state :on-close)
                           (lambda ()
                             (ok (pg::physical-reseed-follow-owner-busy-p owner))
                             (reseed-settlement-test-abort-attempt root staged :aborting)
                             (setf after-abort (list (reseed-mapped-test-tree-facts root)
                                                     (reseed-mapped-test-tree-facts workspace)))))
                     (ok (intake-fails-p
                          (lambda () (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                           owner :helper "/unused/helper"))))
                     (ok (= 2 (getf state :closes)))
                     (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                     (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                     (ok (equalp after-abort (list (reseed-mapped-test-tree-facts root)
                                                   (reseed-mapped-test-tree-facts workspace)))))
                (setf (symbol-function symbol) original)))))))))

(deftest reseed-activation-source-probe-rechecks-writer-and-flush-after-prepare
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
              (expected (pg::physical-reseed-follow-generation-anchor (first (getf staged :generations))))
              (before (list (reseed-mapped-test-tree-facts root)
                            (reseed-mapped-test-tree-facts workspace))))
         (dolist (mode '(:promotion :flush-regression))
           (call-with-reseed-settlement-source
            owner root staged
            (lambda (state)
              (let* ((prepare 'pg::pg-client-native-backup-lease-follow-prepare)
                     (identity 'pg::physical-client-identity-row)
                     (old-prepare (symbol-function prepare))
                     (old-identity (symbol-function identity))
                     (armed nil) (calls 0) (late-identities 0))
                (unwind-protect
                     (progn
                       (setf (symbol-function prepare)
                             (lambda (&rest arguments)
                               (declare (ignore arguments))
                               (incf calls)
                               (setf armed t)
                               (pg::physical-follow-anchor-copy expected))
                             (symbol-function identity)
                             (lambda (client)
                               (let ((row (funcall old-identity client)))
                                 (when armed
                                   (incf late-identities)
                                   (ecase mode
                                     (:promotion (setf (aref row 1) "99"))
                                     (:flush-regression (setf (aref row 2) "0/0"))))
                                 row)))
                       (let ((message
                               (handler-case
                                   (progn (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                                owner :helper "/unused/helper") nil)
                                 (error (condition) (princ-to-string condition)))))
                         (ok (and message (search "saved writer or receive frontier" message))))
                       (ok (= 1 calls late-identities (getf state :closes)))
                       (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                       (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                       (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                                (reseed-mapped-test-tree-facts workspace)))))
                  (setf (symbol-function prepare) old-prepare
                        (symbol-function identity) old-identity))))))))))

(deftest reseed-activation-source-probe-rejects-incomplete-or-fenced-intent-before-connect
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (let* ((symbol 'pg::physical-libpq-connect-frozen)
              (original (symbol-function symbol))
              (connects 0))
         (labels ((refuses ()
                    (let ((before (list (reseed-mapped-test-tree-facts root)
                                        (reseed-mapped-test-tree-facts workspace))))
                      (ok (intake-fails-p
                           (lambda () (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                            owner :helper "/unused/helper"))))
                      (ok (zerop connects))
                      (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                      (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                      (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                               (reseed-mapped-test-tree-facts workspace)))))))
           (unwind-protect
                (progn
                  (setf (symbol-function symbol)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (incf connects)
                          (error "unexpected connection before local activation admission")))
                  (refuses)
                  (pg::physical-reseed-follow-begin-activation owner)
                  (refuses)
                  (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
                         (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
                    (unwind-protect
                         (progn
                           (reseed-follow-owner-test-write temporary #())
                           (refuses))
                      (when (probe-file temporary) (delete-file temporary)))
                    (reseed-settlement-test-abort-attempt root staged :aborting)
                    (refuses)))
             (setf (symbol-function symbol) original)))))))

(deftest reseed-activation-source-probe-detects-same-byte-output-writes-around-network
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (let* ((staged (pg::physical-reseed-follow-stage-activation owner))
              (anchor (pg::physical-reseed-follow-generation-anchor (first (getf staged :generations))))
              (path (merge-pathnames "staged/follow.active" workspace))
              (bytes (lineage-test-bytes path)))
         (dolist (boundary '(pg::physical-libpq-connect-frozen pg::physical-client-identity-row
                             pg::pg-client-native-backup-lease-follow-prepare pg::physical-client-close))
           (call-with-reseed-settlement-source
            owner root staged
            (lambda (state)
              (let* ((prepare 'pg::pg-client-native-backup-lease-follow-prepare)
                     (old-prepare (symbol-function prepare))
                     (original nil) (hit nil) (after-write nil))
                (unwind-protect
                     (progn
                       (setf (symbol-function prepare)
                             (lambda (&rest arguments)
                               (declare (ignore arguments))
                               (pg::physical-follow-anchor-copy anchor))
                             original (symbol-function boundary)
                             (symbol-function boundary)
                             (lambda (&rest arguments)
                               (multiple-value-prog1 (apply original arguments)
                                 (unless hit
                                   (assert (null pg::*physical-reseed-follow-activation-scope*))
                                   (let ((before (sb-posix:lstat path)))
                                     (sleep 1.1)
                                     (with-open-file (stream path :direction :io :if-exists :overwrite
                                                                  :element-type '(unsigned-byte 8))
                                       (write-sequence bytes stream)
                                       (finish-output stream))
                                     (let ((after (sb-posix:lstat path)))
                                       (assert (= (sb-posix:stat-ino before) (sb-posix:stat-ino after)))
                                       (assert (/= (sb-posix:stat-mtime before) (sb-posix:stat-mtime after)))))
                                   (assert (equalp bytes (lineage-test-bytes path)))
                                   (setf hit t
                                         after-write (list (reseed-mapped-test-tree-facts root)
                                                           (reseed-mapped-test-tree-facts workspace)))))))
                       (let ((message
                               (handler-case
                                   (progn (activation-test-call 'pg::physical-reseed-follow-probe-activation-source
                                                                owner :helper "/unused/helper") nil)
                                 (error (condition) (princ-to-string condition)))))
                         (ok (and message (search "changed during source observation" message))))
                       (ok hit)
                       (ok (= 1 (getf state :closes)))
                       (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                       (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                       (ok (equalp after-write (list (reseed-mapped-test-tree-facts root)
                                                     (reseed-mapped-test-tree-facts workspace)))))
                  (when original (setf (symbol-function boundary) original))
                  (setf (symbol-function prepare) old-prepare))))))))))

(deftest reseed-activation-staging-requires-building-intent
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore settled))
       (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (intake-fails-p
              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
         (ok (not (probe-file (merge-pathnames "staged/follow.active" workspace))))
         (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-activation-staging-retries-complete-output-after-every-sync-cut
    (dolist (cut '(:after-configuration-file-fsync
                   :before-activation-output-fsync :after-activation-output-fsync
                   :before-activation-staged-fsync :after-activation-staged-fsync
                   :before-activation-workspace-fsync :after-activation-workspace-fsync
                   :before-activation-ancestors-fsync :after-activation-ancestors-fsync))
      (call-with-reseed-building-activation-fixture
       :same-timeline
       (lambda (owner root workspace building)
         (declare (ignore workspace))
         (let* ((hit nil)
                (pg::*physical-reseed-follow-test-hook*
                  (lambda (event path)
                    (declare (ignore path))
                    (when (eq event cut) (setf hit t) (error "active output sync cut")))))
           (ok (intake-fails-p
                (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
           (ok hit)
           (ok (equalp (pg::physical-reseed-follow-image building)
                       (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
       (lambda (root workspace)
         (let* ((path (merge-pathnames "staged/follow.active" workspace))
                (facts (pg::physical-reseed-file-facts (sb-posix:lstat path)))
                (preserved (activation-test-unrelated-tree root workspace)))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let ((record (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))
                (ok (eq :staged (getf (getf record :activation) :phase))))))
           (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat path))))
           (ok (reseed-activation-layout-facts-equal-p
                preserved (activation-test-unrelated-tree root workspace)))
           (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))))

(deftest reseed-activation-staging-recovers-or-preserves-every-journal-cut
    (dolist (cut '(:after-follow-temporary-create :after-follow-temporary-fsync
                   :before-follow-rename :after-follow-rename :after-follow-directory-fsync))
      (call-with-reseed-building-activation-fixture
       :same-timeline
       (lambda (owner root workspace building)
         (declare (ignore root workspace building))
         (let* ((hit nil)
                (pg::*physical-reseed-follow-test-hook*
                  (lambda (event path)
                    (declare (ignore path))
                    (when (eq event cut) (setf hit t) (error "Staged journal cut")))))
           (ok (intake-fails-p
                (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
           (ok hit)
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))))
       (lambda (root workspace)
         (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                (final (merge-pathnames "physical-reseed.follow" root))
                (before (lineage-test-bytes final))
                (preserved (activation-test-unrelated-tree root workspace)))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (if (eq cut :after-follow-temporary-create)
                  (let ((partial (lineage-test-bytes temporary)))
                    (ok (intake-fails-p
                         (lambda () (pg::physical-reseed-follow-recover-activation owner))))
                    (ok (equalp partial (lineage-test-bytes temporary)))
                    (ok (equalp before (lineage-test-bytes final))))
                  (let ((record (pg::physical-reseed-follow-recover-activation owner)))
                    (ok (eq :staged (getf (getf record :activation) :phase)))
                    (ok (not (probe-file temporary)))
                    (ok (equal record (pg::physical-reseed-follow-read owner)))))))
           (ok (reseed-activation-layout-facts-equal-p
                preserved (activation-test-unrelated-tree root workspace))))))))

(deftest reseed-activation-staging-preserves-partial-output-and-refuses-reopen
    (dolist (cut '(:after-configuration-file-create :after-configuration-write-chunk))
      (let ((preserved nil) (journal nil))
        (call-with-reseed-building-activation-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore building))
           (setf journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
           (let* ((hit nil)
                  (pg::*physical-reseed-follow-write-limit* 1)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event cut) (setf hit t) (error "partial active output cut")))))
             (ok (intake-fails-p
                  (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
             (ok hit))
           (setf preserved (list (reseed-mapped-test-tree-facts root)
                                 (reseed-mapped-test-tree-facts workspace)))
           (ok (<= (length (lineage-test-bytes (merge-pathnames "staged/follow.active" workspace))) 1))
           (throw *reseed-follow-fixture-owner-exit* :partial))
         (lambda (root workspace)
           (check-reseed-activation-output-blocks-owner root)
           (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
           (ok (reseed-activation-layout-facts-equal-p
                preserved (list (reseed-mapped-test-tree-facts root)
                                (reseed-mapped-test-tree-facts workspace)))))))))

(defun activation-staging-test-image (root)
  (let* ((pg::*pgdata* root) (config (pg::physical-follow-read-config)))
    (setf (getf (fifth config) :phase) :active
          (getf (sixth config) :enabled) t)
    (pg::physical-follow-config-image config)))

(deftest reseed-activation-staging-requires-a-durability-proof-and-uses-reserved-capacity
    (call-with-reseed-building-activation-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (let* ((path (merge-pathnames "staged/follow.active" workspace))
              (bytes (activation-staging-test-image root)))
         (with-open-file (out path :direction :output :if-exists :error :element-type '(unsigned-byte 8))
           (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
           (write-sequence bytes out))
         (let ((snapshot (activation-test-snapshot owner)))
           (pg::call-with-physical-reseed-follow-activation-operation
            owner
            (lambda (fd check)
              (declare (ignore fd))
              (let* ((context (pg::physical-reseed-follow-activation-context owner))
                     (candidate (pg::physical-reseed-follow-activation-staged-record
                                 building (pg::physical-reseed-follow-activation-context-output-entry context)))
                     (pg::*physical-reseed-follow-activation-candidate* (pg::physical-reseed-follow-copy candidate)))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-follow-configuration-publication-candidate owner context candidate))))
                (ok (funcall check))
                (values building (pg::physical-reseed-follow-activation-context-snapshot context)))))
           (ok (equal snapshot (activation-test-snapshot owner))))
         (call-with-reseed-insufficient-capacity
          (lambda (calls)
            (let ((record (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))
              (ok (eq :staged (getf (getf record :activation) :phase))))
            (ok (zerop (funcall calls)))))))))

(deftest reseed-activation-staging-keeps-invalid-output-entries-fenced
    (dolist (kind '(:corrupt :directory :symlink :hardlink))
      (let ((preserved nil))
        (call-with-reseed-building-activation-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore building))
           (let ((path (merge-pathnames "staged/follow.active" workspace)))
             (ecase kind
               (:corrupt
                (let ((bytes (activation-staging-test-image root)))
                  (setf (aref bytes 0) (logxor 1 (aref bytes 0)))
                  (reseed-follow-owner-test-write path bytes)))
               (:directory (sb-posix:mkdir path #o700))
               (:symlink (sb-posix:symlink (merge-pathnames "physical-follow.control" root) path))
               (:hardlink (sb-posix:link (merge-pathnames "physical-follow.control" root) path)))
             (setf preserved (list (reseed-mapped-test-tree-facts root)
                                   (reseed-mapped-test-tree-facts workspace)))
             (ok (intake-fails-p
                  (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
             (ok (reseed-activation-layout-facts-equal-p
                  preserved (list (reseed-mapped-test-tree-facts root)
                                  (reseed-mapped-test-tree-facts workspace))))
             (throw *reseed-follow-fixture-owner-exit* :invalid)))
         (lambda (root workspace)
           (check-reseed-activation-output-blocks-owner root)
           (ok (reseed-activation-layout-facts-equal-p
                preserved (list (reseed-mapped-test-tree-facts root)
                                (reseed-mapped-test-tree-facts workspace)))))))))

(deftest reseed-activation-staging-refuses-a-byte-identical-file-replacement-before-sync
    (call-with-reseed-building-activation-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (let* ((path (merge-pathnames "staged/follow.active" workspace))
              (hit nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event where)
                  (declare (ignore where))
                  (when (eq event :before-activation-output-fsync)
                    (let ((replacement (merge-pathnames "staged/replacement-active" workspace)))
                      (reseed-follow-owner-test-write replacement (lineage-test-bytes path))
                      (sb-posix:rename replacement path)
                      (setf hit t))))))
         (ok (intake-fails-p
              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
         (ok hit)
         (ok (equalp (pg::physical-reseed-follow-image building)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))
     (lambda (root workspace)
       (let ((inode (sb-posix:stat-ino (sb-posix:lstat (merge-pathnames "staged/follow.active" workspace)))))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (let ((record (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))
                   (ok (eq :staged (getf (getf record :activation) :phase)))
                   (ok (= inode (fifth (getf (getf record :activation) :entry)))))))))))

(deftest reseed-activation-staging-rejects-a-byte-identical-write-after-durability-proof
    (call-with-reseed-building-activation-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (let* ((path (merge-pathnames "staged/follow.active" workspace))
              (hit nil) (changed nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event where)
                  (declare (ignore where))
                  (when (eq event :before-follow-rename)
                    (let* ((context (pg::physical-reseed-follow-activation-context owner))
                           (parent (pg::physical-reseed-follow-configuration-role-fd
                                    (pg::physical-reseed-follow-activation-context-descriptors context) :staged))
                           (mount (pg::physical-adoption-install-mount-id nil :directory-fd parent))
                           (before (pg::physical-reseed-install-old-observe parent "follow.active" mount)))
                      (with-open-file (out path :direction :io :if-exists :overwrite :if-does-not-exist :error
                                                :element-type '(unsigned-byte 8))
                        (let ((byte (read-byte out)))
                          (file-position out 0) (write-byte byte out)))
                      (setf hit t changed
                            (not (equal before (pg::physical-reseed-install-old-observe parent "follow.active" mount)))))))))
         (ok (intake-fails-p
              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
         (ok hit)
         (ok changed)
         (ok (equalp (pg::physical-reseed-follow-image building)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root (lambda (owner)
               (let ((record (pg::physical-reseed-follow-recover-activation owner)))
                 (ok (eq :staged (getf (getf record :activation) :phase)))
                 (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))))

(deftest reseed-activation-staging-rechecks-abort-before-output-sync
    (call-with-reseed-building-activation-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (declare (ignore workspace))
       (let* ((hit nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event path)
                  (declare (ignore path))
                  (when (eq event :before-activation-output-fsync)
                    (reseed-settlement-test-abort-attempt root building)
                    (setf hit t)))))
         (ok (intake-fails-p
              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))
         (ok hit)
         (ok (equalp (pg::physical-reseed-follow-image building)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
     (lambda (root workspace)
       (let* ((path (merge-pathnames "staged/follow.active" workspace))
              (facts (pg::physical-reseed-file-facts (sb-posix:lstat path))))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (eq :building (getf (getf (pg::physical-reseed-follow-read owner) :activation) :phase)))
                 (ok (intake-fails-p
                      (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner))))))
         (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat path))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))

(deftest reseed-activation-staging-recovers-from-real-fsync-failures
    (loop for fail-at from 1 to 3
          for leaf in '("staged/follow.active" "staged/" "")
          do (call-with-reseed-building-activation-fixture
              :same-timeline
              (lambda (owner root workspace building)
                (let* ((symbol 'sb-posix:fsync) (original (symbol-function symbol))
                                                (armed nil) (calls 0) (injected nil) (failed-inode nil)
                                                (pg::*physical-reseed-follow-test-hook*
                                                  (lambda (event path)
                                                    (declare (ignore path))
                                                    (when (eq event :before-activation-output-fsync) (setf armed t)))))
                  (unwind-protect
                       (progn
                         (setf (symbol-function symbol)
                               (lambda (fd)
                                 (when armed
                                   (incf calls)
                                   (when (= calls fail-at)
                                     (setf injected t failed-inode (sb-posix:stat-ino (sb-posix:fstat fd)))
                                     (error "injected active output fsync failure")))
                                 (funcall original fd)))
                         (ok (intake-fails-p
                              (lambda () (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)))))
                    (setf (symbol-function symbol) original))
                  (ok injected)
                  (ok (= failed-inode (sb-posix:stat-ino (sb-posix:lstat (merge-pathnames leaf workspace)))))
                  (ok (equalp (pg::physical-reseed-follow-image building)
                              (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
              (lambda (root workspace)
                (declare (ignore workspace))
                (pg::call-with-physical-reseed-follow-configuration-owner
                 root (lambda (owner)
                        (ok (eq :staged
                                (getf (getf (activation-test-call 'pg::physical-reseed-follow-stage-activation owner)
                                            :activation) :phase)))))))))

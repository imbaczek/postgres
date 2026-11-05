(in-package #:pg-physical-tests)

(defun call-with-reseed-inert-settlement-fixture (kind function &optional after)
  (call-with-reseed-follow-building-fixture
   kind
   (lambda (owner root workspace building)
     (declare (ignore building))
     (let* ((staged (pg::physical-reseed-follow-stage-configuration owner))
            (maximum (if (pg::physical-reseed-follow-configuration-selected-p
                          (pg::physical-reseed-follow-core-projection staged)) 5 4)))
       (loop repeat (+ maximum 2) do (pg::physical-reseed-follow-advance-configuration owner))
       (funcall function owner root workspace)))
   after))

(deftest reseed-follow-settlement-begin-is-durable-idempotent-and-disabled
    (dolist (kind '(:same-timeline :later-writer))
      (call-with-reseed-inert-settlement-fixture
       kind
       (lambda (owner root workspace)
         (declare (ignore workspace))
         (let* ((before (pg::physical-reseed-follow-read owner))
                (install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root)))
                (control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root)))
                (record (pg::physical-reseed-follow-begin-settlement owner))
                (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
           (ok (eq :handoff-pending (getf (getf record :settlement) :phase)))
           (ok (equal (getf before :configuration) (getf record :configuration)))
           (ok (equal (getf before :generations) (getf record :generations)))
           (ok (equal record (pg::physical-reseed-follow-begin-settlement owner)))
           (ok (equal record (pg::physical-reseed-follow-read owner)))
           (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
           (ok (equalp install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
           (ok (equalp control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
           (let* ((pg::*pgdata* root) (config (pg::physical-follow-read-config)))
             (ok (eq :pending (getf (fifth config) :phase)))
             (ok (not (getf (sixth config) :enabled))))
           (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (ok (eq :handoff-pending
                         (getf (getf (pg::physical-reseed-follow-read owner) :settlement) :phase)))))))))

(deftest reseed-follow-settlement-recovers-complete-pending-temporary
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let* ((injected nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event path)
                  (declare (ignore path))
                  (when (eq event :after-follow-temporary-readback)
                    (setf injected t)
                    (error "settlement pending temporary cut")))))
         (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-begin-settlement owner))))
         (ok injected)
         (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
         (ok (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root (lambda (owner)
               (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
                 (ok (eq :handoff-pending (getf (getf record :settlement) :phase)))
                 (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                 (ok (equal record (pg::physical-reseed-follow-recover-configuration owner)))
                 (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))))

(deftest reseed-follow-settlement-reopens-after-pending-rename-cut
    (let ((expected nil))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore workspace))
         (let* ((injected nil)
                (pg::*physical-reseed-follow-test-hook*
                  (lambda (event path)
                    (declare (ignore path))
                    (when (eq event :after-follow-rename)
                      (setf injected t) (error "pending rename cut")))))
           (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-begin-settlement owner))))
           (ok injected)
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
           (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
           (setf expected (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
       (lambda (root workspace)
         (declare (ignore workspace))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root (lambda (owner)
                 (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
                   (ok (eq :handoff-pending (getf (getf record :settlement) :phase)))
                   (ok (equal record (pg::physical-reseed-follow-decode expected)))
                   (ok (equalp expected (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                   (ok (probe-file (merge-pathnames "physical-reseed.install" root)))
                   (ok (probe-file (merge-pathnames "physical-reseed.attempt" root)))
                   (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))))

(deftest reseed-follow-settlement-preserves-partial-and-conflicting-temporaries
    (dolist (damage '(:partial :terminal-skip))
      (let ((replacement-bytes nil))
        (call-with-reseed-inert-settlement-fixture
         :same-timeline
         (lambda (owner root workspace)
           (declare (ignore workspace))
           (let* ((injected nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event :after-follow-temporary-readback)
                        (setf injected t) (error "pending temporary cut")))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-begin-settlement owner))))
             (ok injected))
           (let* ((bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow.tmp" root)))
                  (candidate (pg::physical-reseed-follow-decode bytes)))
             (if (eq damage :partial)
                 (setf replacement-bytes (subseq bytes 0 (floor (length bytes) 2)))
                 (let* ((successor (getf (getf candidate :settlement) :successor))
                        (receipt (list :version 1 :request (getf (getf candidate :attempt) :request)
                                       :phase :closed :base (pg::physical-reseed-follow-settlement-base candidate)
                                       :outcome :handed-off :successor successor)))
                   (setf (getf candidate :settlement)
                         (list :phase :handed-off :successor successor :receipt receipt)
                         replacement-bytes (pg::physical-reseed-follow-image candidate))))))
         (lambda (root workspace)
           (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                  (replacement (merge-pathnames "fixture.follow" root))
                  (effects 0) (before nil))
             (reseed-follow-owner-test-write replacement replacement-bytes)
             (sb-posix:rename replacement temporary)
             (setf before (list (reseed-mapped-test-tree-facts root)
                                (reseed-mapped-test-tree-facts workspace)))
             (let ((pg::*physical-reseed-follow-test-hook*
                     (lambda (event path)
                       (declare (ignore path))
                       (when (member event '(:before-follow-temporary-fsync :before-follow-rename))
                         (incf effects)))))
               (pg::call-with-physical-reseed-follow-configuration-owner
                root (lambda (owner)
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-follow-recover-configuration owner))))
                       (ok (pg::physical-reseed-follow-owner-uncertain-p owner)))))
             (ok (= 0 effects))
             (ok (equalp replacement-bytes (lineage-test-bytes temporary)))
             (ok (equalp before (list (reseed-mapped-test-tree-facts root)
                                      (reseed-mapped-test-tree-facts workspace))))))))))

(deftest reseed-follow-settlement-authenticates-operation-candidates
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore root workspace))
       (pg::call-with-physical-reseed-follow-installation-operation
        owner
        (lambda (fd check)
          (declare (ignore fd))
          (let* ((context (pg::physical-reseed-follow-installation-context owner))
                 (record (pg::physical-reseed-follow-copy
                          (pg::physical-reseed-follow-installation-context-record context)))
                 (snapshot (pg::physical-reseed-follow-installation-context-snapshot context)))
            (setf (getf record :settlement)
                  (list :phase :handoff-pending
                        :successor (pg::physical-reseed-follow-settlement-successor record) :receipt nil))
            (setf record (pg::physical-reseed-follow-copy record))
            (ok (intake-fails-p
                 (lambda () (pg::physical-reseed-follow-configuration-publication-candidate owner context record))))
            (let ((pg::*physical-reseed-follow-settlement-candidate* record))
              (ok (equal record (pg::physical-reseed-follow-configuration-publication-candidate owner context record)))
              (let ((pg::*physical-reseed-follow-installation-candidate* record))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-follow-configuration-publication-candidate owner context record)))))
              (ok (intake-fails-p
                   (lambda ()
                     (pg::physical-reseed-follow-configuration-publication-candidate
                      (pg::copy-physical-reseed-follow-configuration-owner owner) context record))))
              (let ((wrong (pg::physical-reseed-follow-copy record)))
                (incf (getf (getf wrong :configuration) :time))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-follow-configuration-publication-candidate owner context wrong))))))
            (funcall check)
            (ok (equal snapshot (pg::physical-reseed-follow-installation-context-snapshot context)))
            (values (getf snapshot :record) snapshot)))))))

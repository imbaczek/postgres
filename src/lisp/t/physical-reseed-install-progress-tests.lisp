(in-package #:pg-physical-tests)

(defun call-with-reseed-step-source (plan function)
  (let* ((request (getf (getf plan :attempt) :request))
         (token (gensym "RESEED-STEP-CLIENT"))
         (ready (backup-lease-test-record :ready))
         (state (list :ready t :aborted nil :connections 0 :closed-count 0 :statuses 0 :aborts 0
                      :owner nil :on-status nil :requests nil))
         (symbols '(pg::call-with-physical-reseed-client pg::pg-client-native-backup-lease-status
                    pg::pg-client-native-backup-lease-abort))
         (originals (mapcar #'symbol-function symbols)))
    (setf (getf ready :request) (pg::physical-backup-lease-request-copy request)
          (getf (getf ready :base) :checkpoint-start) (pg::physical-manifest-checkpoint-start (getf plan :manifest))
          (getf (getf ready :base) :base) (getf (getf plan :manifest) :end)
          (getf (getf ready :base) :manifest-digest) (getf plan :manifest-digest))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::call-with-physical-reseed-client)
                 (lambda (owner record callback &key helper capture)
                   (declare (ignore helper))
                   (assert (not capture))
                   (pg::physical-reseed-require-journal owner record)
                   (assert (every (lambda (key) (equal (getf record key) (getf (getf plan :attempt) key)))
                                  pg::*physical-reseed-attempt-identity-keys*))
                   (setf (getf state :owner) owner)
                   (incf (getf state :connections))
                   (unwind-protect (funcall callback token) (incf (getf state :closed-count))))
                 (symbol-function 'pg::pg-client-native-backup-lease-status)
                 (lambda (client supplied)
                   (assert (eq token client))
                   (assert (equal request supplied))
                   (push (pg::physical-backup-lease-request-copy supplied) (getf state :requests))
                   (incf (getf state :statuses))
                   (when (getf state :on-status) (funcall (getf state :on-status) state supplied))
                   (cond ((getf state :aborted) (reseed-attempt-test-abort-receipt (getf plan :attempt)))
                         ((getf state :ready) (pg::physical-backup-lease-copy ready))))
                 (symbol-function 'pg::pg-client-native-backup-lease-abort)
                 (lambda (client supplied)
                   (assert (eq token client))
                   (assert (equal request supplied))
                   (incf (getf state :aborts))
                   (setf (getf state :aborted) t)
                   (reseed-attempt-test-abort-receipt (getf plan :attempt))))
           (funcall function state))
      (loop for symbol in symbols for original in originals
            do (setf (symbol-function symbol) original)))))

(deftest reseed-install-progress-avoids-uninstalled-hook-proofs
    (let ((counts nil))
      (dolist (hooked '(nil t))
        (call-with-reseed-namespace-fixture
         (lambda (root stage incoming quarantine plan)
           (declare (ignore stage incoming quarantine))
           (let ((original (symbol-function 'pg::physical-reseed-install-namespace-state))
                 (count 0))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-reseed-install-namespace-state)
                          (lambda (&rest arguments)
                            (incf count)
                            (apply original arguments)))
                    (call-with-reseed-step-source
                     plan
                     (lambda (source)
                       (declare (ignore source))
                       (let ((pg::*physical-reseed-install-test-hook*
                               (when hooked (lambda (point directory)
                                              (declare (ignore point directory))))))
                         (pg::call-with-physical-reseed-install-owner
                          root
                          (lambda (owner)
                            (ok (equal '(:step 1 :observed 1 :partial nil)
                                       (pg::physical-reseed-install-advance owner :helper "/unused/helper")))))))))
               (setf (symbol-function 'pg::physical-reseed-install-namespace-state) original))
             (push count counts)))))
      (ok (> (first counts) (second counts)))
      (ok (plusp (second counts)))))

(deftest reseed-install-progress-rechecks-a-hook-that-clears-itself
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage quarantine))
       (let ((fired nil)
             (before (lineage-test-bytes (pg::physical-reseed-install-path root))))
         (call-with-reseed-step-source
          plan
          (lambda (source)
            (let ((pg::*physical-reseed-install-test-hook*
                    (lambda (point directory)
                      (declare (ignore directory))
                      (when (eq point :before-install-move)
                        (setf fired t pg::*physical-reseed-install-test-hook* nil)
                        (with-open-file (out (merge-pathnames "base/42" incoming)
                                             :direction :io :element-type '(unsigned-byte 8))
                          (write-byte 23 out))))))
              (pg::call-with-physical-reseed-install-owner
               root
               (lambda (owner)
                 (ok (intake-fails-p
                      (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper")))))))
            (ok (= (getf source :connections) (getf source :closed-count)))))
         (ok fired)
         (ok (equalp before (lineage-test-bytes (pg::physical-reseed-install-path root))))
         (dolist (row (getf plan :old))
           (ok (sb-posix:lstat (namestring (merge-pathnames (first row) root)))))))))

(deftest reseed-install-progress-rechecks-byte-changes-without-a-hook
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage quarantine))
       (let ((original (symbol-function 'sb-posix:fsync))
             (fired nil)
             (before (lineage-test-bytes (pg::physical-reseed-install-path root))))
         (unwind-protect
              (progn
                (setf (symbol-function 'sb-posix:fsync)
                      (lambda (fd)
                        (unless fired
                          (setf fired t)
                          (with-open-file (out (merge-pathnames "base/42" incoming)
                                               :direction :io :element-type '(unsigned-byte 8))
                            (write-byte 23 out)))
                        (funcall original fd)))
                (call-with-reseed-step-source
                 plan
                 (lambda (source)
                   (let ((pg::*physical-reseed-install-test-hook* nil))
                     (pg::call-with-physical-reseed-install-owner
                      root
                      (lambda (owner)
                        (ok (intake-fails-p
                             (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper")))))))
                   (ok (= (getf source :connections) (getf source :closed-count))))))
           (setf (symbol-function 'sb-posix:fsync) original))
         (ok fired)
         (ok (equalp before (lineage-test-bytes (pg::physical-reseed-install-path root))))
         (dolist (row (getf plan :old))
           (ok (sb-posix:lstat (namestring (merge-pathnames (first row) root)))))))))

(deftest reseed-install-terminal-refuses-a-byte-valid-invalid-base
    (dolist (partial '(nil t))
      (call-with-reseed-namespace-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage))
         (let ((maximum (1- (pg::physical-reseed-install-last-step plan))))
           (dotimes (index maximum)
             (reseed-namespace-test-move root incoming quarantine plan index))
           (reseed-install-owner-test-write root plan maximum partial)
           (let ((before (lineage-test-bytes (pg::physical-reseed-install-path root))))
             (call-with-reseed-step-source
              plan
              (lambda (source)
                (pg::call-with-physical-reseed-install-owner
                 root
                 (lambda (owner)
                   (ok (intake-fails-p
                        (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))))
                (ok (= (getf source :connections) (getf source :closed-count)))))
             (ok (equalp before (lineage-test-bytes (pg::physical-reseed-install-path root))))))))))

(deftest reseed-install-terminal-revalidates-before-recording-and-retrying
    (dolist (cut '(nil :after-install-image-validation :after-install-progress-truncate
                   :after-install-step-write-chunk :before-install-step-fsync :after-install-step-fsync))
      (call-with-reseed-namespace-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage))
         (let* ((maximum (1- (pg::physical-reseed-install-last-step plan)))
                (original (symbol-function 'pg::physical-reseed-image-base-state))
                (validations 0)
                (fired nil))
           (dotimes (index maximum)
             (reseed-namespace-test-move root incoming quarantine plan index))
           (reseed-install-owner-test-write root plan maximum t)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-reseed-image-base-state)
                        (lambda (directory)
                          (incf validations)
                          (multiple-value-bind (manifest digest) (pg::recovery-read-manifest directory)
                            (pg::recovery-check-manifest-schema manifest)
                            (pg::recovery-check-manifest-files manifest directory)
                            (values manifest digest))))
                  (call-with-reseed-step-source
                   plan
                   (lambda (source)
                     (let ((pg::*physical-reseed-install-progress-write-limit* 1)
                           (pg::*physical-reseed-install-test-hook*
                             (lambda (point directory)
                               (declare (ignore directory))
                               (when (and cut (eq point cut) (not fired))
                                 (setf fired t)
                                 (error "terminal cut")))))
                       (pg::call-with-physical-reseed-install-owner
                        root
                        (lambda (owner)
                          (if cut
                              (ok (intake-fails-p
                                   (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))
                              (ok (equal (list :step (1+ maximum) :observed maximum :partial nil)
                                         (pg::physical-reseed-install-advance owner :helper "/unused/helper")))))))
                     (ok (eq (not (null cut)) fired))
                     (dotimes (repeat 2)
                       (let ((before validations))
                         (pg::call-with-physical-reseed-install-owner
                          root
                          (lambda (owner)
                            (ok (equal (list :step (1+ maximum) :observed maximum :partial nil)
                                       (pg::physical-reseed-install-advance owner :helper "/unused/helper")))))
                         (ok (> validations before))))
                     (ok (= (getf source :connections) (getf source :closed-count))))))
             (setf (symbol-function 'pg::physical-reseed-image-base-state) original)))))))

(deftest reseed-install-advance-moves-and-records-exactly-one-entry
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (call-with-reseed-step-source
        plan
        (lambda (source)
          (loop for step from 1 below (pg::physical-reseed-install-last-step plan)
                do (pg::call-with-physical-reseed-install-owner
                    root
                    (lambda (owner)
                      (ok (equal (list :step step :observed step :partial nil)
                                 (pg::physical-reseed-install-advance owner :helper "/unused/helper")))
                      (ok (equal (list :step step :observed step :partial nil)
                                 (pg::physical-reseed-install-observe owner))))))
          (ok (plusp (getf source :statuses)))
          (ok (= (getf source :connections) (getf source :closed-count))))))))

(deftest reseed-install-advance-refuses-replaced-journal-before-mutation
    (dolist (cut '(:before-install-move :before-install-step-write))
      (call-with-reseed-namespace-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore quarantine))
         (with-physical-store-directory (holder)
           (let* ((path (pg::physical-reseed-install-path root))
                  (held (merge-pathnames "original-journal" holder))
                  (foreign (merge-pathnames "foreign-journal" holder))
                  (before (lineage-test-bytes path)) (foreign-bytes nil))
             (call-with-reseed-step-source
              plan
              (lambda (source)
                (let* ((fired nil)
                       (pg::*physical-reseed-install-test-hook*
                         (lambda (point where)
                           (declare (ignore where))
                           (when (and (eq point cut) (not fired))
                             (setf fired t)
                             (sb-posix:rename (namestring path) (namestring held))
                             (with-open-file (out path :direction :output :if-exists :error :element-type '(unsigned-byte 8))
                               (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                               (let* ((replacement (pg::physical-reseed-install-copy plan))
                                      (stat (sb-posix:fstat (sb-sys:fd-stream-fd out))))
                                 (setf (getf replacement :journal)
                                       (list :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat))
                                       foreign-bytes (pg::physical-reseed-install-image replacement 0))
                                 (write-sequence foreign-bytes out)))))))
                  (pg::call-with-physical-reseed-install-owner
                   root
                   (lambda (owner)
                     (ok (intake-fails-p (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))))
                  (ok fired))
                (ok (equalp before (lineage-test-bytes held)))
                (ok (equalp foreign-bytes (lineage-test-bytes path)))
                (sb-posix:rename (namestring path) (namestring foreign))
                (sb-posix:rename (namestring held) (namestring path))
                (uiop:delete-directory-tree stage :validate t)
                (uiop:delete-directory-tree incoming :validate t)
                (pg::call-with-physical-reseed-operation-owner
                 root
                 (lambda (owner)
                   (ok (eq :ready (getf (pg::physical-reseed-status owner :helper "/unused/helper") :phase)))
                   (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))))
                (ok (= (getf source :connections) (getf source :closed-count)))))))))))

(deftest reseed-install-advance-supports-empty-old-tree-and-missing-pid
    (dolist (opaque '(nil t))
      (call-with-reseed-namespace-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine))
         (when (not opaque) (ok (null (getf plan :old))))
         (call-with-reseed-step-source
          plan
          (lambda (source)
            (loop for step from 1 below (pg::physical-reseed-install-last-step plan)
                  do (pg::call-with-physical-reseed-install-owner
                      root
                      (lambda (owner)
                        (ok (equal (list :step step :observed step :partial nil)
                                   (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))))
            (ok (= (getf source :connections) (getf source :closed-count))))))
       :pid nil :opaque opaque :before-count 0 :after-count 0)))

(deftest reseed-install-advance-recovers-every-partial-step-byte
    (let ((step-length (length (pg::physical-reseed-install-step-image 0 1))))
      (loop for cut from 1 to step-length
            do (call-with-reseed-namespace-fixture
                (lambda (root stage incoming quarantine plan)
                  (declare (ignore stage incoming quarantine))
                  (call-with-reseed-step-source
                   plan
                   (lambda (source)
                     (let* ((chunks 0) (fired nil)
                                       (pg::*physical-reseed-install-progress-write-limit* 1)
                                       (pg::*physical-reseed-install-test-hook*
                                         (lambda (point path)
                                           (declare (ignore path))
                                           (when (eq point :after-install-step-write-chunk)
                                             (incf chunks)
                                             (when (= chunks cut) (setf fired t) (error "step byte cut"))))))
                       (pg::call-with-physical-reseed-install-owner
                        root
                        (lambda (owner)
                          (ok (intake-fails-p
                               (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))))
                       (ok fired)
                       (ok (= chunks cut)))
                     (pg::call-with-physical-reseed-install-owner
                      root
                      (lambda (owner)
                        (ok (equal (list :step (if (= cut step-length) 1 0) :observed 1 :partial (< cut step-length))
                                   (pg::physical-reseed-install-observe owner)))
                        (let ((next (if (= cut step-length) 2 1)))
                          (ok (equal (list :step next :observed next :partial nil)
                                     (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))))
                     (ok (= (getf source :connections) (getf source :closed-count))))))))))

(deftest reseed-install-advance-cuts-preserve-old-objects-and-source-cleanup
    (dolist (position '(:old :pid :new-directory))
      (dolist (cut '(:after-install-progress-open :before-install-prefix-fsync :after-install-prefix-fsync
                     :before-install-prefix-directory-fsync :after-install-prefix-directory-fsync
                     :before-install-move :after-install-move
                     :before-install-source-parent-fsync :after-install-source-parent-fsync
                     :before-install-target-parent-fsync :after-install-target-parent-fsync
                     :before-install-step-write :after-install-step-write-chunk
                     :before-install-step-fsync :after-install-step-fsync
                     :before-install-step-readback :after-install-step-readback
                     :before-install-progress-truncate :after-install-progress-truncate
                     :after-install-progress-truncate-fsync))
        (call-with-reseed-namespace-fixture
         (lambda (root stage incoming quarantine plan)
           (let* ((old (getf plan :old))
                  (step (ecase position
                          (:old 0)
                          (:pid (position "postmaster.pid" old :key #'first :test #'equal))
                          (:new-directory (1+ (length old)))))
                  (original-bytes
                    (loop for row in old when (sb-posix:s-isreg (fourth row))
                          collect (cons (first row) (lineage-test-bytes (merge-pathnames (first row) root)))))
                  (partial (member cut '(:before-install-progress-truncate :after-install-progress-truncate
                                         :after-install-progress-truncate-fsync))))
             (dotimes (move step) (reseed-namespace-test-move root incoming quarantine plan move))
             (reseed-install-owner-test-write root plan step partial)
             (call-with-reseed-step-source
              plan
              (lambda (source)
                (let* ((fired nil)
                       (pg::*physical-reseed-install-test-hook*
                         (lambda (point path)
                           (declare (ignore path))
                           (when (eq cut point) (setf fired t) (error "reseed progress cut")))))
                  (pg::call-with-physical-reseed-install-owner
                   root
                   (lambda (owner)
                     (ok (intake-fails-p (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))))
                  (ok fired))
                (uiop:delete-directory-tree stage :validate t)
                (uiop:delete-directory-tree incoming :validate t)
                (pg::call-with-physical-reseed-operation-owner
                 root
                 (lambda (owner)
                   (ok (eq :ready (getf (pg::physical-reseed-status owner :helper "/unused/helper") :phase)))
                   (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))
                   (ok (eq :closed (getf (pg::physical-reseed-status owner :helper "/unused/helper") :phase)))))
                (dolist (row old)
                  (let ((matches
                          (loop for directory in (list root quarantine)
                                for path = (merge-pathnames (first row) directory)
                                for stat = (handler-case (sb-posix:lstat (namestring path))
                                             (sb-posix:syscall-error (condition)
                                               (unless (= sb-posix:enoent (sb-posix:syscall-errno condition)) (error condition))
                                               nil))
                                when (and stat (= (second row) (sb-posix:stat-dev stat))
                                          (= (third row) (sb-posix:stat-ino stat)))
                                  collect path)))
                    (ok (= 1 (length matches)))
                    (let ((saved (assoc (first row) original-bytes :test #'equal)))
                      (when saved (ok (equalp (cdr saved) (lineage-test-bytes (first matches))))))))
                (ok (= 1 (getf source :aborts)))
                (ok (= (getf source :connections) (getf source :closed-count)))))))))))

(deftest reseed-install-advance-stabilizes-visible-progress-before-the-next-rename
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (call-with-reseed-step-source
        plan
        (lambda (source)
          (declare (ignore source))
          (let* ((fired nil)
                 (pg::*physical-reseed-install-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (eq point :before-install-step-fsync) (setf fired t) (error "unsynced complete step")))))
            (pg::call-with-physical-reseed-install-owner
             root (lambda (owner)
                    (ok (intake-fails-p (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))
                    (ok (equal '(:step 1 :observed 1 :partial nil) (pg::physical-reseed-install-observe owner)))))
            (ok fired))
          (pg::call-with-physical-reseed-install-owner
           root
           (lambda (owner)
             (let ((original (symbol-function 'sb-posix:fsync)) (events nil))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'sb-posix:fsync)
                            (lambda (fd)
                              (let ((stat (sb-posix:fstat fd)))
                                (push (list :sync (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)) events))
                              (funcall original fd)))
                      (let ((pg::*physical-reseed-install-test-hook*
                              (lambda (point path)
                                (declare (ignore path))
                                (when (eq point :before-install-move) (push (list :move) events)))))
                        (ok (equal '(:step 2 :observed 2 :partial nil)
                                   (pg::physical-reseed-install-advance owner :helper "/unused/helper")))))
                 (setf (symbol-function 'sb-posix:fsync) original))
               (let ((ordered (reverse events)) (journal (getf plan :journal)))
                 (ok (equal (list :sync (getf journal :device) (getf journal :inode)) (first ordered)))
                 (ok (>= (position :move ordered :key #'first) 2)))))))))))

(deftest reseed-install-advance-refuses-cancellation-during-ready
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let ((before (lineage-test-bytes (pg::physical-reseed-install-path root))))
         (call-with-reseed-step-source
          plan
          (lambda (source)
            (setf (getf source :on-status)
                  (lambda (state request)
                    (declare (ignore request))
                    (setf (getf state :on-status) nil)
                    (let* ((owner (getf state :owner))
                           (record (pg::physical-reseed-attempt-copy (pg::physical-reseed-cleanup-owner-attempt owner)))
                           (candidate (pg::physical-reseed-attempt-copy record)))
                      (setf (getf candidate :phase) :aborting)
                      (pg::physical-reseed-attempt-publish owner candidate :previous record))))
            (pg::call-with-physical-reseed-install-owner
             root
             (lambda (owner)
               (ok (intake-fails-p (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))
               (ok (eq :aborting (getf (pg::physical-reseed-cleanup-owner-attempt owner) :phase)))
               (ok (equal '(:step 0 :observed 0 :partial nil) (pg::physical-reseed-install-observe owner)))
               (ok (eq :aborted (getf (pg::physical-reseed-cancel owner :helper "/unused/helper") :phase)))))
            (ok (equalp before (lineage-test-bytes (pg::physical-reseed-install-path root))))
            (ok (= 1 (getf source :aborts)))
            (ok (= (getf source :connections) (getf source :closed-count)))))))))

(deftest reseed-install-advance-recovers-a-target-parent-sync-error
    (dolist (position '(:old :new))
      (call-with-reseed-namespace-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage))
         (let ((step (if (eq position :old) 0 (length (getf plan :old)))))
           (dotimes (move step) (reseed-namespace-test-move root incoming quarantine plan move))
           (reseed-install-owner-test-write root plan step)
           (call-with-reseed-step-source
            plan
            (lambda (source)
              (let ((original (symbol-function 'sb-posix:fsync)) (armed nil) (fired nil))
                (unwind-protect
                     (progn
                       (setf (symbol-function 'sb-posix:fsync)
                             (lambda (fd)
                               (when armed (setf armed nil fired t) (error "target parent sync failed"))
                               (funcall original fd)))
                       (let ((pg::*physical-reseed-install-test-hook*
                               (lambda (point path)
                                 (declare (ignore path))
                                 (when (eq point :before-install-target-parent-fsync) (setf armed t)))))
                         (pg::call-with-physical-reseed-install-owner
                          root
                          (lambda (owner)
                            (ok (intake-fails-p
                                 (lambda () (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))))))
                  (setf (symbol-function 'sb-posix:fsync) original))
                (ok fired))
              (pg::call-with-physical-reseed-install-owner
               root
               (lambda (owner)
                 (ok (equal (list :step step :observed (1+ step) :partial nil)
                            (pg::physical-reseed-install-observe owner)))
                 (ok (equal (list :step (1+ step) :observed (1+ step) :partial nil)
                            (pg::physical-reseed-install-advance owner :helper "/unused/helper")))))
              (ok (= (getf source :connections) (getf source :closed-count))))))))))

(deftest reseed-install-advance-repairs-single-ahead-without-another-move
    (dolist (position '(:old-first :pid :new-first :new-last))
      (call-with-reseed-namespace-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage))
         (let* ((old-count (length (getf plan :old)))
                (step (ecase position
                        (:old-first 0)
                        (:pid (position "postmaster.pid" (getf plan :old) :key #'first :test #'equal))
                        (:new-first old-count)
                        (:new-last (- (pg::physical-reseed-install-last-step plan) 2)))))
           (dotimes (move (1+ step)) (reseed-namespace-test-move root incoming quarantine plan move))
           (reseed-install-owner-test-write root plan step t)
           (call-with-reseed-step-source
            plan
            (lambda (source)
              (pg::call-with-physical-reseed-install-owner
               root
               (lambda (owner)
                 (ok (equal (list :step (1+ step) :observed (1+ step) :partial nil)
                            (pg::physical-reseed-install-advance owner :helper "/unused/helper")))
                 (ok (equal (list :step (1+ step) :observed (1+ step) :partial nil)
                            (pg::physical-reseed-install-observe owner)))))
              (ok (= (getf source :connections) (getf source :closed-count))))))))))

(deftest reseed-install-advance-requires-ready-before-progress-or-namespace-mutation
    (call-with-reseed-namespace-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let ((before (lineage-test-bytes (pg::physical-reseed-install-path root)))
             (mutation-point nil))
         (call-with-reseed-step-source
          plan
          (lambda (source)
            (setf (getf source :ready) nil)
            (let ((pg::*physical-reseed-install-test-hook*
                    (lambda (point path)
                      (declare (ignore path))
                      (when (member point '(:before-install-prefix-fsync :before-install-progress-truncate
                                            :before-install-move :before-install-step-write))
                        (setf mutation-point point)))))
              (pg::call-with-physical-reseed-install-owner
               root
               (lambda (owner)
                 (ok (handler-case
                         (progn (pg::physical-reseed-install-advance owner :helper "/unused/helper") nil)
                       (pg::pg-client-error (condition) (equal "55000" (pg::pg-client-error-sqlstate condition)))))
                 (ok (equal '(:step 0 :observed 0 :partial nil) (pg::physical-reseed-install-observe owner))))))
            (ok (not mutation-point))
            (ok (equalp before (lineage-test-bytes (pg::physical-reseed-install-path root))))
            (ok (= (getf source :connections) (getf source :closed-count)))))))))
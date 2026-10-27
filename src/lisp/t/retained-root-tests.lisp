(in-package #:pg-physical-tests)

(defmacro with-retained-root-registry (&body body)
  `(let ((pg::*physical-retained-root-references* (make-hash-table :test #'eq))
         (pg::*physical-retained-root-holds* (make-hash-table :test #'eq))
         (pg::*physical-retained-root-updates* (make-hash-table :test #'eq)))
     (declare (special pg::*physical-retained-root-references* pg::*physical-retained-root-holds*
                       pg::*physical-retained-root-updates*))
     ,@body))

(defun retained-root-save-control (root id &optional (name "selected.test-control"))
  (let ((bytes (if id (make-array 8 :element-type '(unsigned-byte 8))
                   (make-array 0 :element-type '(unsigned-byte 8)))))
    (when id (pg::wal-set-u64 bytes 0 id))
    (pg::wal-atomic-write-bytes (merge-pathnames name root) bytes)
    id))

(defun retained-root-read-control (root &optional (name "selected.test-control"))
  (when (sb-thread:holding-mutex-p pg::*physical-retained-inventory-publish-lock*)
    (error "control read must run outside archive exclusion"))
  (let ((path (merge-pathnames name root)))
    (when (pg::wal-namespace-file-stat path t)
      (let ((bytes (pg::read-bounded-regular-file path 8)))
        (unless (member (length bytes) '(0 8)) (error "invalid test control length"))
        (unless (zerop (length bytes)) (pg::wal-u64 bytes 0))))))

(deftest retained-root-hold-transfers-to-selected-reference
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let* ((owner (gensym "CONTROL")) (seen nil)
                                             (id (pg::call-with-physical-retained-root-update
                                                  (lambda (id hold)
                                                    (setf seen hold)
                                                    (ng (sb-thread:holding-mutex-p pg::*physical-retained-inventory-publish-lock*))
                                                    (ok (pg::physical-retained-root-protected-p root :inventory id))
                                                    (ng (pg::physical-retained-root-delete-unprotected root :inventory id))
                                                    (retained-root-save-control root id))
                                                  owner root identity 2 text images :read-selected (lambda () (retained-root-read-control root)))))
             (ok (= id (retained-root-read-control root)))
             (ok (= id (pg::physical-retained-root-current owner)))
             (ok (eq :selected (pg::physical-retained-root-hold-state seen)))
             (ok (zerop (hash-table-count pg::*physical-retained-root-holds*)))
             (ng (pg::physical-retained-root-delete-unprotected root :inventory id))
             (ok (= 2 (length (getf (pg::physical-retained-inventory-load root id identity history) :entries)))))))))

(deftest retained-root-ambiguous-selection-remains-protected-until-reconciliation
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let ((owner (gensym "CONTROL")) (seen nil) (published nil))
             (ok (signals
                  (pg::call-with-physical-retained-root-update
                   (lambda (id hold)
                     (setf seen hold published id)
                     (retained-root-save-control root id)
                     (error "failure after control synchronization"))
                   owner root identity 2 text images :read-selected (lambda () (retained-root-read-control root))) 'error))
             (ok (eq :uncertain (pg::physical-retained-root-hold-state seen)))
             (ok (null (pg::physical-retained-root-current owner)))
             (ng (pg::physical-retained-root-delete-unprotected root :inventory published))
             (ok (signals (pg::call-with-physical-retained-root-update
                           (lambda (id hold) (declare (ignore hold)) id)
                           owner root identity 2 text images :read-selected (lambda () (retained-root-read-control root))) 'error))
             (pg::physical-retained-root-reconcile owner root identity history (lambda () (retained-root-read-control root)))
             (ok (= published (pg::physical-retained-root-current owner)))
             (ok (zerop (hash-table-count pg::*physical-retained-root-holds*)))
             (ok (eq :reconciled (pg::physical-retained-root-hold-state seen))))))))

(deftest retained-root-nonlocal-selection-exit-keeps-an-uncertain-hold
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let ((owner (gensym "CONTROL")) (published nil))
             (retained-root-save-control root nil)
             (ok (eq :left
                     (catch 'leave
                       (pg::call-with-physical-retained-root-update
                        (lambda (id hold)
                          (declare (ignore hold)) (setf published id) (throw 'leave :left))
                        owner root identity 2 text images :read-selected (lambda () (retained-root-read-control root))))))
             (ng (pg::physical-retained-root-delete-unprotected root :inventory published))
             (pg::physical-retained-root-reconcile owner root identity history (lambda () (retained-root-read-control root)))
             (ok (pg::physical-retained-root-delete-unprotected root :inventory published)))))))

(deftest retained-root-false-acknowledgement-does-not-release-protection
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let ((owner (gensym "CONTROL")) (published nil))
             (ok (signals
                  (pg::call-with-physical-retained-root-update
                   (lambda (id hold) (declare (ignore hold)) (setf published id) id)
                   owner root identity 2 text images
                   :read-selected (lambda () (retained-root-read-control root))) 'error))
             (ng (pg::physical-retained-root-delete-unprotected root :inventory published))
             (pg::physical-retained-root-reconcile owner root identity history
                                                   (lambda () (retained-root-read-control root)))
             (ok (pg::physical-retained-root-delete-unprotected root :inventory published)))))))

(deftest retained-root-preselection-failure-releases-its-owner-reservation
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (declare (ignore history))
           (let ((owner (gensym "CONTROL")) (calls 0))
             (ok (signals
                  (pg::call-with-physical-retained-root-update
                   (lambda (id hold) (declare (ignore hold)) (incf calls) (retained-root-save-control root id))
                   owner root identity 2 text images :keep-ids '(0)
                   :read-selected (lambda () (retained-root-read-control root))) 'error))
             (ok (zerop calls))
             (ok (zerop (hash-table-count pg::*physical-retained-root-holds*)))
             (ok (zerop (hash-table-count pg::*physical-retained-root-updates*)))
             (ok (null (pg::directory-entry-names root)))
             (ok (integerp
                  (pg::call-with-physical-retained-root-update
                   (lambda (id hold) (declare (ignore hold)) (retained-root-save-control root id))
                   owner root identity 2 text images
                   :read-selected (lambda () (retained-root-read-control root))))))))))

(deftest retained-root-stale-and-simultaneous-owner-updates-are-rejected-before-selection
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let* ((owner (gensym "CONTROL")) (inner-calls 0)
                                             (old (pg::physical-retained-inventory-publish root identity 2 text images)))
             (retained-root-save-control root old)
             (pg::physical-retained-root-register owner root old identity history)
             (let ((new
                     (pg::call-with-physical-retained-root-update
                      (lambda (id hold)
                        (declare (ignore hold))
                        (ok (signals
                             (pg::call-with-physical-retained-root-update
                              (lambda (id hold) (declare (ignore hold)) (incf inner-calls) id)
                              owner root identity 2 text nil :previous old
                              :read-selected (lambda () (retained-root-read-control root))) 'error))
                        (retained-root-save-control root id))
                      owner root identity 2 text nil :previous old :keep-ids nil
                      :read-selected (lambda () (retained-root-read-control root)))))
               (ok (zerop inner-calls))
               (ok (= new (pg::physical-retained-root-current owner)))
               (let ((before (sort (pg::directory-entry-names root) #'string<)))
                 (ok (signals
                      (pg::call-with-physical-retained-root-update
                       (lambda (id hold) (declare (ignore hold)) (incf inner-calls) id)
                       owner root identity 2 text nil :previous old
                       :read-selected (lambda () (retained-root-read-control root))) 'error))
                 (ok (equal before (sort (pg::directory-entry-names root) #'string<))))))))))

(deftest retained-root-multiple-publication-batches-remain-protected
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let ((owner (gensym "CONTROL")) (first-id nil))
             (let ((selected
                     (pg::call-with-physical-retained-root-update
                      (lambda (id hold)
                        (setf first-id id)
                        (let ((last-id (pg::physical-retained-root-update-publish
                                        hold 2 text (rest images) :previous id)))
                          (ng (pg::physical-retained-root-delete-unprotected root :inventory id))
                          (ng (pg::physical-retained-root-delete-unprotected root :inventory last-id))
                          (retained-root-save-control root last-id)))
                      owner root identity 2 text (list (first images))
                      :read-selected (lambda () (retained-root-read-control root)))))
               (ok (/= selected first-id))
               (ok (pg::physical-retained-root-delete-unprotected root :inventory first-id))
               (ng (pg::physical-retained-root-delete-unprotected root :inventory selected))
               (ok (= 2 (length (getf (pg::physical-retained-inventory-load root selected identity history) :entries))))))))))

(deftest retained-root-reconciliation-rejects-unexpected-control-generation
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let* ((owner (gensym "CONTROL"))
                  (other (pg::physical-retained-inventory-publish root identity 2 text (list (first images))))
                  (published nil))
             (ok (signals
                  (pg::call-with-physical-retained-root-update
                   (lambda (id hold) (declare (ignore hold)) (setf published id) (error "ambiguous control"))
                   owner root identity 2 text images
                   :read-selected (lambda () (retained-root-read-control root))) 'error))
             (retained-root-save-control root other)
             (ok (signals (pg::physical-retained-root-reconcile
                           owner root identity history (lambda () (retained-root-read-control root))) 'error))
             (ng (pg::physical-retained-root-delete-unprotected root :inventory published))
             (ok (= 1 (hash-table-count pg::*physical-retained-root-holds*)))
             (retained-root-save-control root nil)
             (pg::physical-retained-root-reconcile owner root identity history
                                                   (lambda () (retained-root-read-control root))))))))

(deftest retained-root-old-reader-survives-coalescing-until-durable-retirement
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let* ((writer (gensym "WRITER")) (reader (gensym "READER"))
                                             (old (pg::physical-retained-inventory-publish root identity 2 text images))
                                             (entries (getf (pg::physical-retained-inventory-load root old identity history) :entries)))
             (retained-root-save-control root old)
             (retained-root-save-control root old "reader.test-control")
             (pg::physical-retained-root-register writer root old identity history)
             (pg::physical-retained-root-register reader root old identity history)
             (multiple-value-bind (metadata bytes) (retained-extent-fixture)
               (let ((new (pg::call-with-physical-retained-root-update
                           (lambda (id hold) (declare (ignore hold)) (retained-root-save-control root id))
                           writer root identity 2 text (list (pg::physical-retained-extent-encode metadata bytes))
                           :previous old :keep-ids nil
                           :read-selected (lambda () (retained-root-read-control root)))))
                 (ng (pg::physical-retained-root-delete-unprotected root :inventory old))
                 (dolist (entry entries)
                   (ng (pg::physical-retained-root-delete-unprotected root :extent (first entry))))
                 (retained-root-save-control root nil "reader.test-control")
                 (pg::physical-retained-root-retire
                  reader root old identity (lambda () (retained-root-read-control root "reader.test-control")))
                 (ok (pg::physical-retained-root-delete-unprotected root :inventory old))
                 (dolist (entry entries)
                   (ok (pg::physical-retained-root-delete-unprotected root :extent (first entry))))
                 (ok (= 1 (length (getf (pg::physical-retained-inventory-load root new identity history) :entries)))))))))))

(defun retained-root-test-thread (function)
  (let ((references pg::*physical-retained-root-references*)
        (holds pg::*physical-retained-root-holds*) (updates pg::*physical-retained-root-updates*))
    (sb-thread:make-thread
     (lambda ()
       (let ((pg::*physical-retained-root-references* references)
             (pg::*physical-retained-root-holds* holds) (pg::*physical-retained-root-updates* updates))
         (funcall function))))))

(deftest retained-root-gc-races-selection-without-losing-the-candidate
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (declare (ignore history))
           (let ((owner (gensym "CONTROL")) (ready (sb-thread:make-semaphore))
                                            (finished (sb-thread:make-semaphore)) (candidate nil) (outcome :unreached) (thread nil))
             (unwind-protect
                  (progn
                    (setf thread
                          (retained-root-test-thread
                           (lambda ()
                             (unwind-protect
                                  (handler-case
                                      (when (sb-thread:wait-on-semaphore ready :timeout 5)
                                        (setf outcome (pg::physical-retained-root-delete-unprotected root :inventory candidate)))
                                    (error (condition) (setf outcome condition)))
                               (sb-thread:signal-semaphore finished)))))
                    (let ((selected
                            (pg::call-with-physical-retained-root-update
                             (lambda (id hold)
                               (declare (ignore hold))
                               (setf candidate id)
                               (sb-thread:signal-semaphore ready)
                               (ok (sb-thread:wait-on-semaphore finished :timeout 5))
                               (ok (null outcome))
                               (retained-root-save-control root id))
                             owner root identity 2 text images
                             :read-selected (lambda () (retained-root-read-control root)))))
                      (ng (pg::physical-retained-root-delete-unprotected root :inventory selected))))
               (sb-thread:signal-semaphore ready)
               (when thread (sb-thread:join-thread thread :timeout 5 :default :timed-out))))))))

(deftest retained-root-protection-is-specific-to-the-archive-directory
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let* ((owner (gensym "CONTROL"))
                  (id (pg::physical-retained-inventory-publish root identity 2 text images)))
             (pg::physical-retained-root-register owner root id identity history)
             (with-physical-store-directory (other)
               (let ((other-id (pg::physical-retained-inventory-publish other identity 2 text images)))
                 (ok (= id other-id))
                 (ok (pg::physical-retained-root-protected-p root :inventory id))
                 (ng (pg::physical-retained-root-protected-p other :inventory other-id))
                 (ok (pg::physical-retained-root-delete-unprotected other :inventory other-id))
                 (ng (pg::physical-retained-root-delete-unprotected root :inventory id)))))))))

(deftest retained-root-interrupts-preserve-registration-and-handoff-atomicity
    (dolist (point '(:after-hold-registration :after-control-readback :after-reference-install))
      (with-retained-root-registry
          (call-with-retained-inventory
           (lambda (root images identity history text)
             (let ((owner (gensym "CONTROL")) (seen nil) (calls 0))
               (let ((pg::*physical-retained-root-test-hook*
                       (lambda (where hold)
                         (when (eq point where)
                           (setf seen hold)
                           (sb-thread:interrupt-thread
                            sb-thread:*current-thread* (lambda () (throw 'interrupted point)))))))
                 (ok (eq point
                         (catch 'interrupted
                           (pg::call-with-physical-retained-root-update
                            (lambda (id hold) (declare (ignore hold)) (incf calls) (retained-root-save-control root id))
                            owner root identity 2 text images
                            :read-selected (lambda () (retained-root-read-control root)))))))
               (cond
                 ((eq point :after-hold-registration)
                  (ok (zerop calls))
                  (ok (eq :released (pg::physical-retained-root-hold-state seen)))
                  (ok (zerop (hash-table-count pg::*physical-retained-root-holds*))))
                 ((eq point :after-control-readback)
                  (ok (= 1 calls))
                  (ok (eq :uncertain (pg::physical-retained-root-hold-state seen)))
                  (pg::physical-retained-root-reconcile owner root identity history
                                                        (lambda () (retained-root-read-control root)))
                  (ok (= (retained-root-read-control root) (pg::physical-retained-root-current owner))))
                 (t
                  (ok (= 1 calls))
                  (ok (eq :selected (pg::physical-retained-root-hold-state seen)))
                  (ok (= (retained-root-read-control root) (pg::physical-retained-root-current owner)))))
               (ok (zerop (hash-table-count pg::*physical-retained-root-updates*)))))))))

(deftest retained-root-reconciliation-keeps-protection-when-a-selected-image-is-missing
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let ((owner (gensym "CONTROL")) (candidate nil))
             (ok (signals
                  (pg::call-with-physical-retained-root-update
                   (lambda (id hold)
                     (declare (ignore hold)) (setf candidate id)
                     (retained-root-save-control root id) (error "ambiguous control response"))
                   owner root identity 2 text images
                   :read-selected (lambda () (retained-root-read-control root))) 'error))
             (let* ((index (pg::physical-retained-inventory-load root candidate identity history))
                    (image-id (first (first (getf index :entries)))))
               (delete-file (pg::physical-retained-object-path root :extent image-id))
               (ok (signals (pg::physical-retained-root-reconcile
                             owner root identity history (lambda () (retained-root-read-control root))) 'error))
               (ok (= 1 (hash-table-count pg::*physical-retained-root-holds*)))
               (ng (pg::physical-retained-root-delete-unprotected root :inventory candidate))))))))

(deftest retained-root-active-update-rejects-control-rechecks-before-reading
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let ((owner (gensym "CONTROL")) (reads 0))
             (pg::call-with-physical-retained-root-update
              (lambda (id hold)
                (declare (ignore hold))
                (flet ((reader () (incf reads) (retained-root-read-control root)))
                  (ok (signals (pg::physical-retained-root-reconcile owner root identity history #'reader) 'error))
                  (ok (signals (pg::physical-retained-root-retire owner root nil identity #'reader) 'error)))
                (ok (zerop reads))
                (retained-root-save-control root id))
              owner root identity 2 text images
              :read-selected (lambda () (retained-root-read-control root))))))))

(deftest retained-root-control-readback-rejects-a-changed-owner-snapshot
    (dolist (operation '(:reconcile :retire))
      (with-retained-root-registry
          (call-with-retained-inventory
           (lambda (root images identity history text)
             (let ((owner (gensym "CONTROL")) (published nil))
               (retained-root-save-control root nil)
               (catch 'leave
                 (pg::call-with-physical-retained-root-update
                  (lambda (id hold)
                    (declare (ignore hold))
                    (setf published id)
                    (throw 'leave nil))
                  owner root identity 2 text images
                  :read-selected (lambda () (retained-root-read-control root))))
               (flet ((reader ()
                        (pg::physical-retained-root-reconcile
                         owner root identity history (lambda () (retained-root-read-control root)))
                        nil))
                 (ok (signals
                      (ecase operation
                        (:reconcile (pg::physical-retained-root-reconcile owner root identity history #'reader))
                        (:retire (pg::physical-retained-root-retire owner root nil identity #'reader)))
                      'error)))
               (ok (zerop (hash-table-count pg::*physical-retained-root-holds*)))
               (ok (pg::physical-retained-root-delete-unprotected root :inventory published))))))))

(deftest retained-root-cleanup-survives-an-interrupt-while-waiting-for-exclusion
    (with-retained-root-registry
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (declare (ignore history))
           (let* ((owner (gensym "CONTROL"))
                  (parent sb-thread:*current-thread*)
                  (acquired (sb-thread:make-semaphore))
                  (release (sb-thread:make-semaphore))
                  (blocker nil)
                  (masked nil)
                  (pg::*physical-retained-root-test-hook*
                    (lambda (point hold)
                      (declare (ignore hold))
                      (case point
                        (:after-hold-registration (error "first interrupt"))
                        (:before-cleanup-lock
                         (setf masked (not sb-sys:*interrupts-enabled*)
                               blocker
                               (sb-thread:make-thread
                                (lambda ()
                                  (sb-thread:with-mutex (pg::*physical-retained-inventory-publish-lock*)
                                    (sb-thread:signal-semaphore acquired)
                                    (sb-thread:wait-on-semaphore release)
                                    (sleep 0.02)
                                    (sb-thread:interrupt-thread parent (lambda () (error "second interrupt")))))))
                         (sb-thread:wait-on-semaphore acquired)
                         (sb-thread:signal-semaphore release))))))
             (unwind-protect
                  (progn
                    (ok (signals
                         (pg::call-with-physical-retained-root-update
                          (lambda (id hold) (declare (ignore id hold)) (error "unreachable"))
                          owner root identity 2 text images :read-selected (lambda () nil))
                         'error))
                    (ok masked)
                    (ok (zerop (hash-table-count pg::*physical-retained-root-holds*)))
                    (ok (zerop (hash-table-count pg::*physical-retained-root-updates*))))
               (sb-thread:signal-semaphore release)
               (when blocker (sb-thread:join-thread blocker :timeout 5))))))))

(defun call-with-receive-retained-fixture (function)
  (with-physical-store-directory (root)
    (let* ((identity (list :system-id 123 :base-timeline 1 :base 32 :manifest-digest 77))
           (store (pg::physical-store-open root 123 1 :create t :start-lsn 32 :retained-identity identity))
           (archive (pg::physical-store-retained-directory store))
           (prefix (concatenate '(vector (unsigned-byte 8))
                                (sample-physical-record 0) (sample-physical-record 32)
                                (sample-physical-record 64)))
           (id nil) (image-id nil))
      (unwind-protect
           (progn
             (pg::physical-store-input store 32 (subseq prefix 32))
             (pg::physical-store-flush store)
             (pg::wal-ensure-directory-durable archive)
             (let* ((metadata (list :version 1 :system-id 123 :base-timeline 1 :base 32
                                    :manifest-digest 77 :history-target 1 :history-digest nil :history-text ""
                                    :start 32 :end 96 :start-digest #xCBF29CE484222325
                                    :end-digest (pg::physical-store-flushed-digest store)
                                    :segment 0 :file-timeline 1
                                    :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 prefix)))
                    (image (pg::physical-retained-extent-encode metadata prefix)))
               (setf id (pg::physical-retained-inventory-publish archive identity 1 "" (list image))
                     image-id (pg::physical-store-update-digest #xCBF29CE484222325 image)))
             (pg::physical-store-save-control store 96 :digest (pg::physical-store-flushed-digest store)
                                                       :floor-lsn 96 :floor-digest (pg::physical-store-flushed-digest store)
                                                       :retained-root id))
        (pg::physical-store-close store))
      (funcall function root identity id image-id prefix))))

(deftest receive-control-retained-root-is-validated-before-tail-cleanup
    (call-with-receive-retained-fixture
     (lambda (root identity id image-id prefix)
       (let ((reopened (pg::physical-store-open root 123 1 :retained-identity identity)))
         (unwind-protect
              (progn
                (ok (= id (pg::physical-store-retained-root reopened)))
                (ok (= 96 (pg::physical-store-floor-lsn reopened)))
                (pg::physical-store-input reopened 96 (sample-physical-record 96))
                (pg::physical-store-flush reopened))
           (pg::physical-store-close reopened)))
       (let ((reopened (pg::physical-store-open root 123 1 :retained-identity identity)))
         (unwind-protect
              (progn
                (ok (= id (pg::physical-store-retained-root reopened)))
                (ok (= 128 (pg::physical-store-flushed-lsn reopened))))
           (pg::physical-store-close reopened)))
       (let* ((path (merge-pathnames "0000000000000000.wal" root))
              (tail (concatenate '(vector (unsigned-byte 8)) prefix
                                 (sample-physical-record 96) (sample-physical-record 128))))
         (pg::wal-atomic-write-bytes path tail)
         (ok (signals (pg::physical-store-open root 123 1) 'error))
         (ok (signals (pg::physical-store-open root 123 1
                                               :retained-identity (list :system-id 123 :base-timeline 1
                                                                        :base 32 :manifest-digest 78))
                      'error))
         (ok (equalp tail (pg::recovery-read-bytes path)))
         (delete-file (pg::physical-retained-object-path (merge-pathnames "retained/" root) :extent image-id))
         (ok (signals (pg::physical-store-open root 123 1 :retained-identity identity) 'error))
         (ok (equalp tail (pg::recovery-read-bytes path)))))))

(deftest receive-control-retained-root-must-match-the-floor-and-digest
    (call-with-receive-retained-fixture
     (lambda (root identity id image-id prefix)
       (declare (ignore id image-id))
       (let* ((path (merge-pathnames "receive.control" root))
              (original (pg::recovery-read-bytes path))
              (wal (merge-pathnames "0000000000000000.wal" root))
              (tail (concatenate '(vector (unsigned-byte 8)) prefix (sample-physical-record 96))))
         (pg::wal-atomic-write-bytes wal tail)
         (dolist (kind '(:floor :digest :missing-root :reserved-history))
           (let ((bytes (copy-seq original)))
             (ecase kind
               (:floor
                (pg::wal-set-u64 bytes 48 64)
                (pg::wal-set-u64 bytes 56 (pg::physical-store-update-digest #xCBF29CE484222325
                                                                            (subseq prefix 32 64))))
               (:digest
                (let ((wrong (logxor 1 (pg::wal-u64 bytes 40))))
                  (pg::wal-set-u64 bytes 40 wrong)
                  (pg::wal-set-u64 bytes 56 wrong)))
               (:missing-root (pg::wal-set-u64 bytes 76 (logxor 1 (pg::wal-u64 bytes 76))))
               (:reserved-history (pg::wal-set-u64 bytes 68 1)))
             (pg::wal-set-u32 bytes 92 0)
             (pg::wal-set-u32 bytes 92 (pg::wal-crc32 bytes))
             (pg::wal-atomic-write-bytes path bytes)
             (ok (signals (pg::physical-store-open root 123 1 :retained-identity identity) 'error))
             (ok (equalp tail (pg::recovery-read-bytes wal)))))
         (pg::wal-atomic-write-bytes path original)))))

(deftest receive-control-preserves-retained-root-through-history-selection
    (call-with-receive-retained-fixture
     (lambda (root identity id image-id prefix)
       (declare (ignore image-id prefix))
       (let ((store (pg::physical-store-open root 123 1 :retained-identity identity)))
         (unwind-protect
              (progn
                (pg::physical-store-select-history store 2 (store-history-text 1 96) (store-history-eor 96 2 1))
                (pg::physical-store-input store 96 (store-history-eor 96 2 1))
                (pg::physical-store-flush store))
           (pg::physical-store-close store)))
       (let ((store (pg::physical-store-open root 123 1 :retained-identity identity)))
         (unwind-protect
              (progn
                (ok (= id (pg::physical-store-retained-root store)))
                (ok (= 2 (pg::physical-store-selected-timeline store)))
                (ok (= 96 (pg::physical-store-floor-lsn store)))
                (ok (= 152 (pg::physical-store-flushed-lsn store))))
           (pg::physical-store-close store))))))

(deftest receive-control-rejects-a-hole-before-a-matching-retained-endpoint
    (call-with-receive-retained-fixture
     (lambda (root identity id image-id prefix)
       (declare (ignore id image-id))
       (let* ((control (merge-pathnames "receive.control" root))
              (bytes (pg::recovery-read-bytes control))
              (metadata (list :version 1 :system-id 123 :base-timeline 1 :base 32
                              :manifest-digest 77 :history-target 1 :history-digest nil :history-text ""
                              :start 64 :end 96
                              :start-digest (pg::physical-store-update-digest #xCBF29CE484222325
                                                                              (subseq prefix 32 64))
                              :end-digest (pg::wal-u64 bytes 56) :segment 0 :file-timeline 1
                              :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 prefix)))
              (hole (pg::physical-retained-inventory-publish
                     (merge-pathnames "retained/" root) identity 1 ""
                     (list (pg::physical-retained-extent-encode metadata prefix)))))
         (pg::wal-set-u64 bytes 76 hole)
         (pg::wal-set-u32 bytes 92 0)
         (pg::wal-set-u32 bytes 92 (pg::wal-crc32 bytes))
         (pg::wal-atomic-write-bytes control bytes)
         (ok (signals (pg::physical-store-open root 123 1 :retained-identity identity) 'error))))))

(deftest rooted-receive-compaction-selects-a-covering-replacement
    (call-with-receive-retained-fixture
     (lambda (root identity id image-id prefix)
       (declare (ignore image-id prefix))
       (let ((store (pg::physical-store-open root 123 1 :retained-identity identity))
             (publications 0))
         (unwind-protect
              (progn
                (pg::physical-store-input store 96 (sample-physical-record 96))
                (pg::physical-store-flush store)
                (pg::physical-store-compact store 128
                                            (lambda (digest)
                                              (declare (ignore digest))
                                              (ok (= 96 (pg::physical-store-floor-lsn store)))
                                              (incf publications)))
                (ok (= 1 publications))
                (ok (= 128 (pg::physical-store-floor-lsn store)))
                (ng (eql id (pg::physical-store-retained-root store))))
           (pg::physical-store-close store)))
       (let ((store (pg::physical-store-open root 123 1 :retained-identity identity)))
         (unwind-protect
              (progn
                (ok (typep (pg::physical-store-retained-root store) '(unsigned-byte 64)))
                (ok (= 128 (pg::physical-store-floor-lsn store)))
                (ok (= 128 (pg::physical-store-flushed-lsn store))))
           (pg::physical-store-close store))))))

(deftest rooted-receive-store-opens-through-recovery-and-pending-promotion
    (call-with-receive-retained-fixture
     (lambda (root identity id image-id prefix)
       (declare (ignore identity image-id))
       (with-physical-store-directory (pgdata)
         (let* ((pg::*pgdata* pgdata)
                (pg::*physical-recovery-state* (list :system-id 123 :timeline 1 :end 32 :manifest-digest 77))
                (spool (pg::physical-recovery-spool-directory))
                (tail (concatenate '(vector (unsigned-byte 8)) prefix (sample-physical-record 96))))
           (sb-posix:rename (namestring root) (namestring spool))
           (sb-posix:rename (namestring (merge-pathnames "retained/" spool))
                            (namestring (pg::physical-recovery-retained-directory)))
           (pg::wal-atomic-write-bytes (merge-pathnames "0000000000000000.wal" spool) tail)
           (let ((store (pg::physical-promotion-open-pending-store pg::*physical-recovery-state*)))
             (unwind-protect
                  (progn
                    (ok (= id (pg::physical-store-retained-root store)))
                    (ok (equalp tail (pg::recovery-read-bytes (pg::physical-store-segment-path store 0)))))
               (pg::physical-store-close store)))
           (let ((store (pg::physical-recovery-open-receive-store)))
             (unwind-protect
                  (progn
                    (ok (= id (pg::physical-store-retained-root store)))
                    (ok (= 96 (pg::physical-store-flushed-lsn store))))
               (pg::physical-store-close store))))))))

(cl-spec:defproperty receive-control-codec-preserves-full-width-retained-references
    ((root-id (range integer 0 18446744073709551615))
     (manifest (range integer 0 18446744073709551615)))
  (:about pg::physical-store-encode-control)
  (:trials (:normal 200))
  (with-physical-store-directory (directory)
    (let* ((identity (list :system-id 123 :base-timeline 1 :base 32 :manifest-digest manifest))
           (store (pg::%make-physical-store :directory directory :system-id 123 :timeline 1
                                            :base-lsn 32 :floor-lsn 32 :retained-identity identity
                                            :retained-root root-id))
           (bytes (pg::physical-store-encode-control store 32 :digest #xCBF29CE484222325)))
      (pg::wal-atomic-write-bytes (merge-pathnames "receive.control" directory) bytes)
      (setf (pg::physical-store-retained-root store) nil)
      (pg::physical-store-load-control store)
      (and (= 96 (length bytes)) (= root-id (pg::physical-store-retained-root store))
           (= manifest (getf (pg::physical-store-retained-identity store) :manifest-digest))
           (= 32 (pg::physical-store-floor-lsn store))))))

(deftest generated-receive-control-retained-reference-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'receive-control-codec-preserves-full-width-retained-references :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(in-package #:pg)

(rove:deftest canonical-slot-state-create-update
    (let* ((base (merge-pathnames (format nil "canonical-slot-state-~a/" (gensym))
                                  (uiop:temporary-directory)))
           (root (merge-pathnames "pg_replslot/" base))
           (old (make-array 3 :element-type '(unsigned-byte 8)
                              :initial-contents '(1 2 3)))
           (new (make-array 2 :element-type '(unsigned-byte 8)
                              :initial-contents '(4 5))))
      (unwind-protect
           (progn
             (ensure-directories-exist root)
             (rove:ok (handler-case (progn (slot-state-update root "absent" old) nil)
                        (error () t)))
             (rove:ok (not (probe-file (slot-state-directory root "absent"))))
             (slot-state-create root "test_slot" old)
             (rove:ok (probe-file (slot-state-path root "test_slot")))
             (rove:ok (slot-state-read root "test_slot"
                                       (lambda (bytes name)
                                         (and (equal name "test_slot") (equalp bytes old)))
                                       :stabilize t))
             (rove:ok (handler-case (progn (slot-state-create root "test_slot" new) nil)
                        (error () t)))
             (slot-state-update root "test_slot" new)
             (rove:ok (slot-state-read root "test_slot"
                                       (lambda (bytes name)
                                         (declare (ignore name)) (equalp bytes new))))
             (rove:ok (not (probe-file
                            (merge-pathnames "state.tmp"
                                             (slot-state-directory root "test_slot"))))))
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))

(rove:deftest canonical-slot-state-exclusive-temporary
    (let* ((base (merge-pathnames (format nil "canonical-slot-exclusive-~a/" (gensym))
                                  (uiop:temporary-directory)))
           (root (merge-pathnames "pg_replslot/" base))
           (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7)))
      (unwind-protect
           (progn
             (ensure-directories-exist root)
             (slot-state-create root "test_slot" bytes)
             (let* ((outside (merge-pathnames "outside" base))
                    (temporary (merge-pathnames "state.tmp"
                                                (slot-state-directory root "test_slot"))))
               (with-open-file (output outside :direction :output :if-exists :error)
                 (write-string "untouched" output))
               (sb-posix:symlink (namestring outside) (namestring temporary))
               (rove:ok (handler-case (progn (slot-state-update root "test_slot" bytes) nil)
                          (error () t)))
               (rove:ok (equal (uiop:read-file-string outside) "untouched"))
               (rove:ok (sb-posix:s-islnk (sb-posix:stat-mode (sb-posix:lstat temporary))))
               (sb-posix:unlink temporary)
               (rove:ok (slot-state-read root "test_slot"
                                         (lambda (actual name)
                                           (declare (ignore name)) (equalp actual bytes))))))
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))

(rove:deftest canonical-slot-state-publication-faults
    (dolist (create-p '(t nil))
      (dolist (point (append '(:slot-file-before-write :slot-file-after-write
                               :slot-file-before-fsync :slot-file-after-fsync)
                             (when create-p
                               '(:slot-state-before-initial-rename
                                 :slot-state-after-initial-rename
                                 :slot-state-before-staging-sync))
                             '(:slot-file-before-rename :slot-file-after-rename
                               :slot-file-before-directory-fsync
                               :slot-file-after-directory-fsync-before-memory-publication)))
        (let* ((base (merge-pathnames (format nil "canonical-slot-fault-~a/" (gensym))
                                      (uiop:temporary-directory)))
               (root (merge-pathnames "pg_replslot/" base))
               (old (make-array 1 :element-type '(unsigned-byte 8) :initial-element 1))
               (new (make-array 1 :element-type '(unsigned-byte 8) :initial-element 2))
               (fired nil)
               (marker (make-condition 'simple-error :format-control "slot test fault"))
               (observed nil))
          (unwind-protect
               (progn
                 (ensure-directories-exist root)
                 (unless create-p (slot-state-create root "fault_slot" old))
                 (let ((*logical-test-failpoint-hook*
                         (lambda (actual &rest details)
                           (declare (ignore details))
                           (when (eq actual point)
                             (setf fired t)
                             (error marker)))))
                   (handler-case
                       (if create-p (slot-state-create root "fault_slot" new)
                           (slot-state-update root "fault_slot" new))
                     (error (condition) (setf observed condition))))
                 (rove:ok fired (format nil "~a ~a hook fired" create-p point))
                 (rove:ok (eq observed marker) "the intended condition escaped")
                 (let ((published (member point
                                          '(:slot-file-after-rename
                                            :slot-file-before-directory-fsync
                                            :slot-file-after-directory-fsync-before-memory-publication))))
                   (if (and create-p (not published))
                       (rove:ok (not (probe-file (slot-state-directory root "fault_slot")))
                                "create has not published the canonical directory")
                       (rove:ok
                        (slot-state-read root "fault_slot"
                                         (lambda (bytes name)
                                           (declare (ignore name))
                                           (equalp bytes (if published new old))))
                        "visible state matches the rename boundary"))))
            (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))))

(rove:deftest canonical-slot-state-replaced-directory
    (dolist (replace-root-p '(t nil))
      (let* ((base (merge-pathnames (format nil "canonical-slot-binding-~a/" (gensym))
                                    (uiop:temporary-directory)))
             (root (merge-pathnames "pg_replslot/" base))
             (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
             (fired nil)
             (observed nil))
        (unwind-protect
             (progn
               (ensure-directories-exist root)
               (slot-state-create root "test_slot" bytes)
               (let ((*logical-test-failpoint-hook*
                       (lambda (point &rest details)
                         (declare (ignore details))
                         (when (eq point :slot-file-before-write)
                           (setf fired t)
                           (let ((victim (if replace-root-p root
                                             (slot-state-directory root "test_slot")))
                                 (moved (merge-pathnames "moved/" base)))
                             (sb-posix:rename
                              (string-right-trim "/" (namestring victim))
                              (string-right-trim "/" (namestring moved)))
                             (ensure-directories-exist victim))))))
                 (handler-case (slot-state-update root "test_slot" bytes)
                   (error (condition) (setf observed condition))))
               (rove:ok fired "replacement hook ran")
               (rove:ok (and observed
                             (search "binding changed" (princ-to-string observed)))
                        "replacement rejected at the binding check")
               (rove:ok (null (directory-entry-names
                               (if replace-root-p root
                                   (slot-state-directory root "test_slot"))))
                        "replacement directory remained untouched")
               (rove:ok (not (probe-file (merge-pathnames (if replace-root-p "moved/test_slot/state.tmp"
                                                              "moved/state.tmp") base)))
                        "the owned temporary file was removed"))
          (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))))

(rove:deftest canonical-slot-state-startup-inventory
    (let* ((base (merge-pathnames (format nil "canonical-slot-inventory-~a/" (gensym))
                                  (uiop:temporary-directory)))
           (root (merge-pathnames "pg_replslot/" base))
           (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
           (staging (slot-state-directory root "unfinished" t)))
      (unwind-protect
           (progn
             (ensure-directories-exist root)
             (rove:ok (null (slot-state-map root (lambda (&rest ignored)
                                                   (declare (ignore ignored))
                                                   (error "empty inventory callback")))))
             (slot-state-create root "z_slot" bytes)
             (slot-state-create root "a_slot" bytes)
             (ensure-directories-exist staging)
             (with-open-file (output (merge-pathnames "state.tmp" staging)
                                     :direction :output)
               (write-string "partial" output))
             (with-open-file (output (merge-pathnames "state.tmp"
                                                      (slot-state-directory root "a_slot"))
                                     :direction :output)
               (write-string "partial" output))
             (rove:ok (handler-case (progn (slot-state-map root #'list) nil)
                        (error () t))
                      "promotion inventory rejects unfinished storage")
             (rove:ok (probe-file staging) "rejection leaves staging untouched")
             (rove:ok (equal
                       (slot-state-map root
                                       (lambda (actual name)
                                         (assert (equalp actual bytes)) name)
                                       :cleanup t)
                       '("a_slot" "z_slot"))
                      "startup returns canonical slots in name order")
             (rove:ok (not (probe-file staging)) "startup removes known staging")
             (rove:ok (not (probe-file
                            (merge-pathnames "state.tmp"
                                             (slot-state-directory root "a_slot"))))
                      "startup removes unfinished state update"))
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))

(rove:deftest canonical-slot-state-inventory-rejects-before-cleanup
    (let* ((base (merge-pathnames (format nil "canonical-slot-invalid-~a/" (gensym))
                                  (uiop:temporary-directory)))
           (root (merge-pathnames "pg_replslot/" base))
           (staging (slot-state-directory root "aaa" t))
           (invalid (merge-pathnames "zzz.tmp/" root)))
      (unwind-protect
           (progn
             (ensure-directories-exist staging)
             (ensure-directories-exist invalid)
             (with-open-file (output (merge-pathnames "unrelated" invalid)
                                     :direction :output)
               (write-string "preserve" output))
             (rove:ok (handler-case (progn (slot-state-map root #'list :cleanup t) nil)
                        (error () t)))
             (rove:ok (probe-file staging)
                      "an invalid later entry prevents earlier staging cleanup")
             (rove:ok (equal (uiop:read-file-string (merge-pathnames "unrelated" invalid))
                             "preserve")
                      "unknown contents remain untouched"))
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))

(rove:deftest canonical-slot-state-inventory-rejects-nonregular
    (dolist (kind '(:missing :fifo :symlink :unknown))
      (let* ((base (merge-pathnames (format nil "canonical-slot-shape-~a/" (gensym))
                                    (uiop:temporary-directory)))
             (root (merge-pathnames "pg_replslot/" base))
             (slot (slot-state-directory root "test_slot"))
             (state (merge-pathnames "state" slot)))
        (unwind-protect
             (progn
               (ensure-directories-exist slot)
               (ecase kind
                 (:missing nil)
                 (:fifo (sb-posix:mkfifo (namestring state) #o600))
                 (:symlink (sb-posix:symlink (namestring base) (namestring state)))
                 (:unknown
                  (with-open-file (output (merge-pathnames ".unexpected" slot)
                                          :direction :output)
                    (write-string "preserve" output))))
               (let ((called nil))
                 (rove:ok (handler-case
                              (progn (slot-state-map
                                      root (lambda (&rest ignored)
                                             (declare (ignore ignored))
                                             (setf called t))
                                      :cleanup t)
                                     nil)
                            (error () t))
                          (format nil "reject ~a state without opening it" kind))
                 (rove:ok (not called) "invalid state never reaches decoder")))
          (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))))

(rove:deftest canonical-slot-state-drop-retry
    (dolist (point '(:slot-drop-before-rename :slot-drop-after-rename
                     :slot-drop-before-directory-fsync :slot-drop-after-directory-fsync
                     :slot-drop-after-cleanup))
      (let* ((base (merge-pathnames (format nil "canonical-slot-drop-~a/" (gensym))
                                    (uiop:temporary-directory)))
             (root (merge-pathnames "pg_replslot/" base))
             (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
             (fired nil)
             (observed nil)
             (marker (make-condition 'simple-error :format-control "drop test fault")))
        (unwind-protect
             (progn
               (ensure-directories-exist root)
               (slot-state-create root "test_slot" bytes)
               (let ((claim (slot-state-claim-drop root "test_slot")))
                 (let ((*logical-test-failpoint-hook*
                         (lambda (actual &rest details)
                           (declare (ignore details))
                           (when (eq actual point) (setf fired t) (error marker)))))
                   (handler-case (slot-state-drop root "test_slot" claim)
                     (error (condition) (setf observed condition))))
                 (rove:ok fired "drop fault hook ran")
                 (rove:ok (eq marker observed) "the intended drop fault escaped")
                 (rove:ok (slot-state-drop root "test_slot" claim)
                          "same claim completes final, tombstone or absent retry")
                 (rove:ok (null (directory-entry-names root)) "drop is complete")))
          (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))))

(rove:deftest canonical-slot-state-drop-rejects-unowned-staging
    (let* ((base (merge-pathnames (format nil "canonical-slot-drop-owner-~a/" (gensym))
                                  (uiop:temporary-directory)))
           (root (merge-pathnames "pg_replslot/" base))
           (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7)))
      (unwind-protect
           (progn
             (ensure-directories-exist root)
             (slot-state-create root "test_slot" bytes)
             (let ((claim (slot-state-claim-drop root "test_slot"))
                   (staging (slot-state-directory root "test_slot" t)))
               (ensure-directories-exist staging)
               (rove:ok (handler-case
                            (progn (slot-state-claim-drop root "test_slot") nil)
                          (error () t))
                        "preexisting staging prevents a new claim")
               (rove:ok (handler-case (progn (slot-state-drop root "test_slot" claim) nil)
                          (error () t))
                        "simultaneous entries reject drop")
               (rove:ok (probe-file (slot-state-path root "test_slot"))
                        "unowned staging cannot retire canonical state")))
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))

(rove:deftest canonical-slot-state-drop-rejects-replacement
    (dolist (kind '(:root :final :tombstone))
      (let* ((base (merge-pathnames (format nil "canonical-slot-drop-change-~a/" (gensym))
                                    (uiop:temporary-directory)))
             (root (merge-pathnames "pg_replslot/" base))
             (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
             (moved (merge-pathnames "moved/" base)))
        (unwind-protect
             (progn
               (ensure-directories-exist root)
               (slot-state-create root "test_slot" bytes)
               (let ((claim (slot-state-claim-drop root "test_slot")))
                 (when (eq kind :tombstone)
                   (let ((marker (make-condition 'simple-error :format-control "retire fault"))
                         (observed nil))
                     (let ((*logical-test-failpoint-hook*
                             (lambda (point &rest details)
                               (declare (ignore details))
                               (when (eq point :slot-drop-after-rename) (error marker)))))
                       (handler-case (slot-state-drop root "test_slot" claim)
                         (error (condition) (setf observed condition))))
                     (rove:ok (eq marker observed) "the original drop retired its name")))
                 (let ((victim (ecase kind
                                 (:root root)
                                 (:final (slot-state-directory root "test_slot"))
                                 (:tombstone (slot-state-directory root "test_slot" t)))))
                   (sb-posix:rename (string-right-trim "/" (namestring victim))
                                    (string-right-trim "/" (namestring moved)))
                   (ensure-directories-exist victim)
                   (rove:ok (handler-case (progn (slot-state-drop root "test_slot" claim) nil)
                              (error () t))
                            "a replacement inode cannot satisfy the old claim")
                   (rove:ok (null (directory-entry-names victim))
                            "replacement was not modified")
                   (rove:ok (probe-file
                             (merge-pathnames (if (eq kind :root) "test_slot/state" "state")
                                              moved))
                            "claimed original state was preserved"))))
          (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))))

(rove:deftest canonical-slot-state-drop-sync-failure
    (let* ((base (merge-pathnames (format nil "canonical-slot-drop-sync-~a/" (gensym))
                                  (uiop:temporary-directory)))
           (root (merge-pathnames "pg_replslot/" base))
           (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
           (original (symbol-function 'fsync-directory))
           (marker (make-condition 'simple-error :format-control "root sync fault"))
           (attempts 0)
           (observed nil))
      (unwind-protect
           (progn
             (ensure-directories-exist root)
             (slot-state-create root "test_slot" bytes)
             (let ((claim (slot-state-claim-drop root "test_slot"))
                   (root-stat (sb-posix:stat root)))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'fsync-directory)
                            (lambda (path)
                              (let ((actual (sb-posix:stat path)))
                                (when (and (= (sb-posix:stat-dev actual)
                                              (sb-posix:stat-dev root-stat))
                                           (= (sb-posix:stat-ino actual)
                                              (sb-posix:stat-ino root-stat)))
                                  (incf attempts)
                                  (error marker)))
                              (funcall original path)))
                      (handler-case (slot-state-drop root "test_slot" claim)
                        (error (condition) (setf observed condition))))
                 (setf (symbol-function 'fsync-directory) original))
               (rove:ok (= attempts 1) "the root fsync was attempted exactly once")
               (rove:ok (eq observed marker) "the root sync failure escaped")
               (rove:ok (probe-file (merge-pathnames "state"
                                                     (slot-state-directory root "test_slot" t)))
                        "failed root sync retains the tombstone")
               (rove:ok (slot-state-drop root "test_slot" claim) "same claim retries")
               (rove:ok (null (directory-entry-names root)) "retry completes cleanup")))
        (setf (symbol-function 'fsync-directory) original)
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))

(rove:deftest canonical-slot-state-rejects-unowned-mutation
    (dolist (kind '(:directory :state :hardlink))
      (let* ((base (merge-pathnames (format nil "canonical-slot-permissions-~a/" (gensym))
                                    (uiop:temporary-directory)))
             (root (merge-pathnames "pg_replslot/" base))
             (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7)))
        (unwind-protect
             (progn
               (ensure-directories-exist root)
               (slot-state-create root "test_slot" bytes)
               (ecase kind
                 (:directory (sb-posix:chmod (namestring root) #o777))
                 (:state (sb-posix:chmod (namestring (slot-state-path root "test_slot")) #o666))
                 (:hardlink
                  (sb-posix:link (namestring (slot-state-path root "test_slot"))
                                 (namestring (merge-pathnames "outside" base)))))
               (rove:ok (handler-case (progn (slot-state-map root #'list :cleanup t) nil)
                          (error () t))
                        "inventory rejects unsafe ownership or links")
               (rove:ok (handler-case (progn (slot-state-update root "test_slot" bytes) nil)
                          (error () t))
                        "update rejects unsafe ownership or links")
               (rove:ok (handler-case (progn (slot-state-read root "test_slot" #'list) nil)
                          (error () t))
                        "read rejects unsafe ownership or links"))
          (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))))

(rove:deftest canonical-slot-state-rename-preserves-late-collision
    (dolist (create-p '(t nil))
      (let* ((base (merge-pathnames (format nil "canonical-slot-collision-~a/" (gensym))
                                    (uiop:temporary-directory)))
             (root (merge-pathnames "pg_replslot/" base))
             (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
             (collision (slot-state-directory root "test_slot" (not create-p)))
             (fired nil)
             (inode nil)
             (observed nil))
        (unwind-protect
             (progn
               (ensure-directories-exist root)
               (unless create-p (slot-state-create root "test_slot" bytes))
               (let ((claim (unless create-p (slot-state-claim-drop root "test_slot"))))
                 (let ((*logical-test-failpoint-hook*
                         (lambda (point &rest details)
                           (declare (ignore details))
                           (when (eq point (if create-p :slot-file-before-rename
                                               :slot-drop-before-rename))
                             (ensure-directories-exist collision)
                             (setf fired t inode (sb-posix:stat-ino (sb-posix:lstat collision)))))))
                   (handler-case
                       (if create-p (slot-state-create root "test_slot" bytes)
                           (slot-state-drop root "test_slot" claim))
                     (error (condition) (setf observed condition)))))
               (rove:ok fired "the late collision was installed")
               (rove:ok (and (typep observed 'sb-posix:syscall-error)
                             (= (sb-posix:syscall-errno observed) sb-posix:eexist))
                        "no-replace rename reports the intended collision")
               (rove:ok (= inode (sb-posix:stat-ino (sb-posix:lstat collision)))
                        "the collision directory was not replaced")
               (rove:ok (null (directory-entry-names collision))
                        "the collision directory was not modified"))
          (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))))

(rove:deftest canonical-slot-state-callback-replacement
    (dolist (map-p '(t nil))
      (dolist (replace-root-p '(t nil))
        (let* ((base (merge-pathnames (format nil "canonical-slot-callback-~a/" (gensym))
                                      (uiop:temporary-directory)))
               (root (merge-pathnames "pg_replslot/" base))
               (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
               (moved (merge-pathnames "moved/" base))
               (victim (if replace-root-p root (slot-state-directory root "test_slot")))
               (called 0)
               (observed nil))
          (unwind-protect
               (progn
                 (ensure-directories-exist root)
                 (slot-state-create root "test_slot" bytes)
                 (flet ((decode (actual name)
                          (assert (and (equalp actual bytes) (equal name "test_slot")))
                          (incf called)
                          (sb-posix:rename (string-right-trim "/" (namestring victim))
                                           (string-right-trim "/" (namestring moved)))
                          (ensure-directories-exist victim)
                          :decoded))
                   (handler-case
                       (if map-p (slot-state-map root #'decode)
                           (slot-state-read root "test_slot" #'decode))
                     (error (condition) (setf observed condition))))
                 (rove:ok (= called 1) "the decoder saw the original bytes once")
                 (rove:ok (and observed
                               (search "binding changed" (princ-to-string observed)))
                          "replacement prevents successful callback return")
                 (rove:ok (null (directory-entry-names victim))
                          "replacement remains untouched")
                 (rove:ok
                  (equalp bytes
                          (read-bounded-regular-file
                           (merge-pathnames (if replace-root-p "test_slot/state" "state")
                                            moved)
                           +slot-state-maximum-bytes+))
                  "the original state bytes remain intact"))
            (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))))

(defun call-with-canonical-wal-slot-fixture (function)
  (let* ((base (merge-pathnames (format nil "canonical-wal-slot-~a/" (gensym))
                                (uiop:temporary-directory)))
         (*pgdata* base)
         (*wal-lock* (sb-thread:make-mutex :name "canonical WAL slot fixture"))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*wal-poisoned* nil) (*wal-ready* nil)
         (*wal-durable-lsn* 100) (*wal-end-lsn* 100)
         (*wal-insert-timeline* 1) (*wal-segment-namespace* :legacy)
         (*physical-recovery-state* nil) (*physical-recovery-failed-p* nil)
         (*logical-capture-live-slots* 0) (*wal-logical-policy* nil)
         (*logical-capture-force* nil) (*backend* nil)
         (*exported-logical-snapshots* (make-hash-table :test #'equal))
         (*expired-exported-logical-snapshots* (make-hash-table :test #'equal)))
    (unwind-protect
         (progn
           (ensure-directories-exist (wal-replication-slot-directory))
           (funcall function))
      (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))

(cl-spec:defproperty logical-slot-image-preserves-source-frontiers
    ((confirmed (range integer 1 18446744073709551615))
     (frontier (range integer 0 18446744073709551615))
     (binary (range integer 0 1)))
  (:about wal-decode-replication-slot-image)
  (:trials (:normal 200))
  (let* ((*wal-durable-lsn* frontier)
         (slot (make-wal-replication-slot
                :name "source_frontier" :database-oid 42 :restart-lsn 0
                :consistent-lsn 0 :confirmed-flush-lsn confirmed
                :plugin (if (plusp binary) "pg_lisp" "pgoutput"))))
    (when (plusp binary) (wal-slot-set-initial-profile slot :binary-bytea))
    (let* ((bytes (wal-encode-replication-slot slot))
           (decoded (funcall (symbol-function 'wal-decode-replication-slot-image)
                             bytes "source_frontier"))
           (local (handler-case (wal-decode-replication-slot bytes "source_frontier")
                    (error () nil))))
      (and (= confirmed (wal-replication-slot-confirmed-flush-lsn decoded))
           (equalp bytes (wal-encode-replication-slot decoded))
           (eq (not (null local)) (<= confirmed frontier))))))

(rove:deftest generated-logical-slot-source-frontiers
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-slot-image-preserves-source-frontiers :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials))))
  (let* ((*wal-durable-lsn* nil)
         (maximum #xffffffffffffffff)
         (slot (make-wal-replication-slot
                :name "maximum_image" :database-oid 42 :restart-lsn maximum
                :consistent-lsn maximum :confirmed-flush-lsn maximum :plugin "pgoutput"))
         (bytes (wal-encode-replication-slot slot))
         (decoded (wal-decode-replication-slot-image bytes "maximum_image")))
    (rove:ok (= maximum (wal-replication-slot-restart-lsn decoded)
                (wal-replication-slot-consistent-lsn decoded)
                (wal-replication-slot-confirmed-flush-lsn decoded)))
    (let ((*wal-durable-lsn* maximum))
      (rove:ok (equalp bytes (wal-encode-replication-slot
                              (wal-decode-replication-slot bytes "maximum_image")))))))

(rove:deftest logical-slot-image-does-not-admit-ahead-of-local-wal
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((registry *wal-replication-slots*)
              (slot (make-wal-replication-slot
                     :name "ahead_image" :database-oid 42 :restart-lsn 400
                     :consistent-lsn 600 :confirmed-flush-lsn 1000 :plugin "pgoutput"))
              (bytes (wal-encode-replication-slot slot))
              (decoded (funcall (symbol-function 'wal-decode-replication-slot-image)
                                bytes "ahead_image")))
         (rove:ok (= 400 (wal-replication-slot-restart-lsn decoded)))
         (rove:ok (= 600 (wal-replication-slot-consistent-lsn decoded)))
         (rove:ok (= 1000 (wal-replication-slot-confirmed-flush-lsn decoded)))
         (rove:ok (not (eq slot decoded)))
         (rove:ok (null (wal-replication-slot-active-owner decoded)))
         (rove:ok (null (wal-replication-slot-drop-claim decoded)))
         (rove:ok (not (wal-replication-slot-creation-failed-p decoded)))
         (rove:ok (handler-case
                      (progn (wal-decode-replication-slot bytes "ahead_image") nil)
                    (error (condition) (search "outside durable WAL" (princ-to-string condition)))))
         (wal-create-replication-slot-file slot)
         (rove:ok (handler-case
                      (progn (wal-read-replication-slot (wal-replication-slot-path "ahead_image")) nil)
                    (error (condition) (search "outside durable WAL" (princ-to-string condition)))))
         (rove:ok (handler-case (progn (wal-load-replication-slots) nil) (error () t)))
         (rove:ok (eq registry *wal-replication-slots*))
         (rove:ok (zerop (hash-table-count registry)))
         (let ((*wal-durable-lsn* 1000))
           (rove:ok (= 1000 (wal-replication-slot-confirmed-flush-lsn
                             (wal-decode-replication-slot bytes "ahead_image")))))))))

(rove:deftest logical-slot-image-rejects-corrupt-and-inconsistent-source-state
    (dolist (prefix
             '("PGSLOT4 source_image 42 -1 0 1 reserved pgoutput"
               "PGSLOT4 source_image 42 0 18446744073709551616 18446744073709551616 reserved pgoutput"
               "PGSLOT4 source_image 42 20 10 15 reserved pgoutput"
               "PGSLOT4 source_image 42 0 20 10 reserved pgoutput"
               "PGSLOT4 source_image 42 0 0 1 unknown pgoutput"
               "PGSLOT5 source_image 42 0 0 1 reserved pgoutput binary-bytea"))
      (let ((bytes (string->bytes
                    (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix))))))
        (rove:ok (handler-case
                     (progn (funcall (symbol-function 'wal-decode-replication-slot-image)
                                     bytes "source_image") nil)
                   (error () t)))))
  (let* ((slot (make-wal-replication-slot
                :name "source_image" :database-oid 42 :restart-lsn 0
                :consistent-lsn 0 :confirmed-flush-lsn 1000 :plugin "pgoutput"))
         (bytes (wal-encode-replication-slot slot)))
    (rove:ok (handler-case
                 (progn (funcall (symbol-function 'wal-decode-replication-slot-image)
                                 bytes "other_name") nil)
               (error () t)))
    (rove:ok (wal-decode-replication-slot-image bytes "source_image"))
    (let* ((corrupt (copy-seq bytes))
           (index (1+ (position (char-code #\Space) corrupt :from-end t))))
      (setf (aref corrupt index)
            (if (= (aref corrupt index) (char-code #\0))
                (char-code #\1) (char-code #\0)))
      (rove:ok (handler-case
                   (progn (wal-decode-replication-slot-image corrupt "source_image") nil)
                 (error () t))))
    (setf (aref bytes 0) (char-code #\X))
    (rove:ok (handler-case
                 (progn (funcall (symbol-function 'wal-decode-replication-slot-image)
                                 bytes "source_image") nil)
               (error () t)))))

(rove:deftest canonical-wal-slot-roundtrip-and-strict-update
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let ((slot (make-wal-replication-slot :name "roundtrip" :restart-lsn 0
                                              :consistent-lsn 0 :wal-status :lost)))
         (rove:ok (handler-case (progn (wal-save-replication-slot slot) nil) (error () t))
                  "save cannot create an absent slot")
         (wal-create-replication-slot-file slot)
         (rove:ok (probe-file (merge-pathnames "roundtrip/state"
                                               (wal-replication-slot-directory))))
         (rove:ok (not (probe-file (merge-pathnames "roundtrip.slot"
                                                    (wal-replication-slot-directory)))))
         (rove:ok (equalp (wal-encode-replication-slot slot)
                          (wal-encode-replication-slot
                           (wal-read-replication-slot (wal-replication-slot-path "roundtrip")))))
         (wal-save-replication-slot slot)
         (let* ((old-registry *wal-replication-slots*)
                (sentinel (make-wal-replication-slot :name "old_registry")))
           (setf (gethash "old_registry" old-registry) sentinel)
           (wal-load-replication-slots)
           (rove:ok (not (eq old-registry *wal-replication-slots*))))
         (rove:ok (= 1 (hash-table-count *wal-replication-slots*)))
         (rove:ok (gethash "roundtrip" *wal-replication-slots*))))))

(rove:deftest canonical-wal-slot-startup-failure-is-atomic
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let ((original *wal-replication-slots*)
             (sentinel (make-wal-replication-slot :name "existing")))
         (setf (gethash "existing" original) sentinel)
         (dolist (name '("a_good" "z_bad"))
           (wal-create-replication-slot-file
            (make-wal-replication-slot :name name :restart-lsn 0
                                       :consistent-lsn 0 :wal-status :lost)))
         (with-open-file (stream (wal-replication-slot-path "z_bad")
                                 :direction :output :if-exists :overwrite)
           (write-string "CORRUPT" stream))
         (rove:ok (handler-case (progn (wal-load-replication-slots) nil) (error () t)))
         (rove:ok (eq original *wal-replication-slots*) "failed restore preserves registry identity")
         (rove:ok (= 1 (hash-table-count original)))
         (rove:ok (eq sentinel (gethash "existing" original)))
         (rove:ok (null (gethash "a_good" original)) "no partial slot was published")))))

(rove:deftest canonical-wal-slot-failed-drop-keeps-claim-and-pin
    (dolist (point '(:slot-drop-before-rename :slot-drop-after-rename
                     :slot-drop-before-directory-fsync :slot-drop-after-directory-fsync
                     :slot-drop-after-cleanup))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((slot (make-wal-replication-slot :name "pending" :restart-lsn 40
                                                 :consistent-lsn 40))
                (marker (make-condition 'simple-error :format-control "WAL drop fault"))
                (observed nil) (fired nil))
           (wal-create-replication-slot-file slot)
           (setf (gethash "pending" *wal-replication-slots*) slot)
           (let ((*logical-test-failpoint-hook*
                   (lambda (actual &rest details)
                     (declare (ignore details))
                     (when (eq actual point) (setf fired t) (error marker)))))
             (handler-case (wal-drop-replication-slot "pending")
               (error (condition) (setf observed condition))))
           (rove:ok fired "intended drop boundary was reached")
           (rove:ok (eq marker observed))
           (rove:ok (eq slot (gethash "pending" *wal-replication-slots*)))
           (rove:ok (wal-replication-slot-drop-claim slot))
           (rove:ok (= 40 (wal-slot-reserved-p slot)) "failed drop retains the WAL pin")
           (sb-thread:with-mutex (*wal-lock*)
             (rove:ok (handler-case
                          (progn (wal-find-usable-replication-slot-under-lock "pending") nil)
                        (pg-error (condition) (equal "55000" (pg-error-sqlstate condition))))))
           (rove:ok (handler-case (progn (wal-save-replication-slot slot) nil)
                      (pg-error (condition) (equal "55000" (pg-error-sqlstate condition)))))
           (wal-drop-replication-slot "pending")
           (rove:ok (null (gethash "pending" *wal-replication-slots*)))
           (rove:ok (null (directory-entry-names (wal-replication-slot-directory)))))))))

(rove:deftest canonical-wal-slot-delete-rejects-a-stale-generation
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((slot (make-wal-replication-slot :name "current" :restart-lsn 0
                                               :consistent-lsn 0 :wal-status :lost))
              (stale (copy-wal-replication-slot slot)))
         (wal-create-replication-slot-file slot)
         (setf (gethash "current" *wal-replication-slots*) slot)
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok (handler-case
                        (progn (wal-delete-replication-slot-file "current" stale) nil)
                      (error () t))))
         (rove:ok (null (wal-replication-slot-drop-claim slot)))
         (rove:ok (probe-file (wal-replication-slot-path "current")))))))

(rove:deftest canonical-wal-slot-pending-drop-fences-normal-operations
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let ((slot (make-wal-replication-slot :name "pending" :restart-lsn 40
                                              :consistent-lsn 40 :confirmed-flush-lsn 40
                                              :database-oid 16384))
             (*wal-logical-slot-owner* (gensym "DROP-GUARD-OWNER")))
         (wal-create-replication-slot-file slot)
         (setf (gethash "pending" *wal-replication-slots*) slot)
         (sb-thread:with-mutex (*wal-lock*)
           (setf (wal-replication-slot-drop-claim slot)
                 (slot-state-claim-drop (wal-replication-slot-directory) "pending"))
           (wal-invalidate-replication-slots-matching-under-lock
            (lambda (candidate) (declare (ignore candidate))
              (error "a pending drop must not be invalidated"))))
         (dolist (operation
                  (list (lambda () (wal-acknowledge-replication-slot "pending" 40))
                        (lambda () (wal-acknowledge-standard-replication-slot "pending" 40 40 40))
                        (lambda () (wal-finalize-replication-slot-copy "pending" 40 nil))
                        (lambda () (wal-logical-slot-validate-start-position "pending" 40))
                        (lambda () (wal-logical-slot-accept-resume-position "pending" 40))
                        (lambda () (call-with-logical-replication-slot
                                    "pending" (lambda (ignored) (declare (ignore ignored)))))
                        (lambda () (call-with-wal-retained-slot-scan
                                    "pending" 40 40 (lambda (ignored) (declare (ignore ignored)))))
                        (lambda () (validate-stream-status
                                    "pending" 40 '(:write 40 :flush 40 :apply 40)))
                        (lambda () (start-logical-replication
                                    nil "pending" 40 (make-logical-replication-command)))
                        (lambda () (wal-advance-physical-replication-slot "pending" 40))
                        (lambda () (sb-thread:with-mutex (*wal-lock*)
                                     (physical-source-public-slot-under-lock
                                      (make-logical-replication-command :slot-name "pending"))))))
           (rove:ok (handler-case (progn (funcall operation) nil)
                      (pg-error (condition)
                        (and (equal "55000" (pg-error-sqlstate condition))
                             (search "pending drop" (pg-error-message condition)))))
                    "normal operations reject the pending drop before other work"))
         (rove:ok (= 40 (wal-slot-reserved-p slot)))
         (rove:ok (eq :reserved (wal-replication-slot-wal-status slot)))))))

(rove:deftest canonical-slot-state-publication-receipt-survives-namespace-loss
    (dolist (kind '(:before-root :after-root :after-child :collision))
      (let* ((base (merge-pathnames (format nil "slot-receipt-~a/" (gensym))
                                    (uiop:temporary-directory)))
             (root (merge-pathnames "pg_replslot/" base))
             (moved (merge-pathnames "moved/" base))
             (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
             (receipt (list :unpublished))
             (marker (make-condition 'simple-error :format-control "receipt namespace fault"))
             (observed nil) (fired nil))
        (unwind-protect
             (progn
               (ensure-directories-exist root)
               (let ((*logical-test-failpoint-hook*
                       (lambda (point &rest details)
                         (declare (ignore details))
                         (when (eq point (if (member kind '(:before-root :collision))
                                             :slot-file-before-rename :slot-file-after-rename))
                           (setf fired t)
                           (if (eq kind :collision)
                               (ensure-directories-exist (slot-state-directory root "receipt_slot"))
                               (let ((victim (if (eq kind :after-child)
                                                 (slot-state-directory root "receipt_slot") root)))
                                 (sb-posix:rename (string-right-trim "/" (namestring victim))
                                                  (string-right-trim "/" (namestring moved)))
                                 (unless (eq kind :after-child) (ensure-directories-exist root))
                                 (error marker)))))))
                 (handler-case
                     (slot-state-create root "receipt_slot" bytes :publication-receipt receipt)
                   (error (condition) (setf observed condition))))
               (rove:ok fired "the intended namespace mutation happened")
               (rove:ok (eq (case kind (:before-root :unpublished) (:collision :uncertain)
                                  (otherwise :published))
                            (car receipt))
                        "receipt records only successful final namespace publication")
               (rove:ok (if (eq kind :collision)
                            (and (typep observed 'sb-posix:syscall-error)
                                 (= sb-posix:eexist (sb-posix:syscall-errno observed)))
                            (eq marker observed))
                        "publication bookkeeping preserves the original condition"))
          (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))))

(rove:deftest canonical-wal-slot-create-retains-published-pin-after-namespace-loss
    (dolist (kind '(:before-root :after-root :after-child))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*catalog* (make-catalog :cluster-identity "slot-receipt"))
                (root (wal-replication-slot-directory))
                (moved (merge-pathnames "moved/" *pgdata*))
                (marker (make-condition 'simple-error :format-control "WAL create namespace fault"))
                (observed nil) (fired nil))
           (let ((*logical-test-failpoint-hook*
                   (lambda (point &rest details)
                     (declare (ignore details))
                     (when (eq point (if (eq kind :before-root)
                                         :slot-file-before-rename :slot-file-after-rename))
                       (setf fired t)
                       (let ((victim (if (eq kind :after-child)
                                         (slot-state-directory root "receipt_slot") root)))
                         (sb-posix:rename (string-right-trim "/" (namestring victim))
                                          (string-right-trim "/" (namestring moved)))
                         (unless (eq kind :after-child) (ensure-directories-exist root))
                         (error marker))))))
             (handler-case
                 (sb-thread:with-mutex (*wal-lock*)
                   (physical-slot-publish-create-under-lock "receipt_slot" 40 1 nil))
               (error (condition) (setf observed condition))))
           (rove:ok fired)
           (rove:ok (eq marker observed) "caller propagates the exact namespace fault")
           (rove:ok (null (slot-state-lstat
                           (slot-state-directory (wal-replication-slot-directory) "receipt_slot")))
                    "the public pathname cannot witness the earlier rename")
           (let ((slot (gethash "receipt_slot" *wal-replication-slots*)))
             (if (eq kind :before-root)
                 (rove:ok (null slot) "unpublished attempt releases its registry claim")
                 (progn
                   (rove:ok (wal-physical-slot-p slot) "published attempt retains its exact name")
                   (rove:ok (= 40 (wal-slot-reserved-p slot)) "published attempt retains its WAL pin")
                   (rove:ok (wal-replication-slot-creation-failed-p slot))
                   (rove:ok (handler-case (progn (wal-save-replication-slot slot) nil)
                              (pg-error (condition) (search "restart required" (pg-error-message condition)))))
                   (rove:ok (handler-case (progn (wal-drop-replication-slot "receipt_slot") nil)
                              (pg-error (condition) (search "restart required" (pg-error-message condition)))))
                   (rove:ok (null (wal-replication-slot-drop-claim slot)))))))))))

(rove:deftest canonical-wal-slot-startup-resolves-failed-creation
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let ((slot (make-wal-replication-slot :name "restart_slot" :restart-lsn 0
                                              :consistent-lsn 0 :wal-status :lost)))
         (wal-create-replication-slot-file slot)
         (setf (wal-replication-slot-creation-failed-p slot) t
               (gethash "restart_slot" *wal-replication-slots*) slot)
         (rove:ok (handler-case (progn (wal-drop-replication-slot "restart_slot") nil)
                    (pg-error (condition) (search "restart required" (pg-error-message condition)))))
         (wal-load-replication-slots)
         (let ((restored (gethash "restart_slot" *wal-replication-slots*)))
           (rove:ok (not (eq restored slot)) "startup creates a freshly validated generation")
           (rove:ok (not (wal-replication-slot-creation-failed-p restored)))
           (wal-save-replication-slot restored))
         (wal-drop-replication-slot "restart_slot")
         (rove:ok (null (gethash "restart_slot" *wal-replication-slots*)))))))

(rove:deftest canonical-wal-slot-create-cannot-adopt-a-foreign-collision
    (dolist (late-p '(nil t))
      (call-with-canonical-wal-slot-fixture
       (lambda ()
         (let* ((*catalog* (make-catalog :cluster-identity "slot-collision"))
                (name "foreign_collision")
                (foreign (make-wal-replication-slot
                          :name name :kind :physical :system-id (catalog-system-identifier)
                          :restart-lsn 10 :restart-timeline 1
                          :plugin nil :consistent-lsn nil :confirmed-flush-lsn nil))
                (bytes (wal-encode-replication-slot foreign))
                (fired nil) (observed nil) (inode nil))
           (labels ((install-foreign ()
                      (ensure-directories-exist
                       (slot-state-directory (wal-replication-slot-directory) name))
                      (with-open-file (stream (wal-replication-slot-path name)
                                              :direction :output :if-exists :error
                                              :element-type '(unsigned-byte 8))
                        (write-sequence bytes stream)
                        (fsync-stream stream))
                      (setf inode (sb-posix:stat-ino
                                   (sb-posix:lstat (wal-replication-slot-path name))))))
             (unless late-p (install-foreign))
             (let ((*logical-test-failpoint-hook*
                     (lambda (point &rest details)
                       (declare (ignore details))
                       (when (and late-p (eq point :slot-file-before-rename))
                         (setf fired t)
                         (install-foreign)))))
               (handler-case
                   (sb-thread:with-mutex (*wal-lock*)
                     (physical-slot-publish-create-under-lock name 40 1 nil))
                 (error (condition) (setf observed condition)))))
           (rove:ok (eq late-p fired) "only a late collision reaches the publication syscall")
           (rove:ok (if late-p
                        (and (typep observed 'sb-posix:syscall-error)
                             (= sb-posix:eexist (sb-posix:syscall-errno observed)))
                        (and observed (search "already exists" (princ-to-string observed))))
                    "the intended collision rejected creation")
           (let ((candidate (gethash name *wal-replication-slots*)))
             (if late-p
                 (progn
                   (rove:ok (wal-replication-slot-creation-failed-p candidate))
                   (rove:ok (= 40 (wal-slot-reserved-p candidate)))
                   (rove:ok (handler-case (progn (wal-save-replication-slot candidate) nil)
                              (pg-error (condition)
                                (search "restart required" (pg-error-message condition)))))
                   (rove:ok (handler-case (progn (wal-drop-replication-slot name) nil)
                              (pg-error (condition)
                                (search "restart required" (pg-error-message condition)))))
                   (rove:ok (null (wal-replication-slot-drop-claim candidate))))
                 (rove:ok (null candidate) "certified unpublished candidate is removed")))
           (rove:ok (= inode (sb-posix:stat-ino (sb-posix:lstat (wal-replication-slot-path name))))
                    "the foreign inode is preserved")
           (rove:ok (equalp bytes (read-bounded-regular-file (wal-replication-slot-path name) 2048))
                    "no later use or drop altered foreign state"))))))

(rove:deftest canonical-wal-slot-backend-cleanup-skips-failed-temporary-creation
    (call-with-canonical-wal-slot-fixture
     (lambda ()
       (let* ((*catalog* (make-catalog :cluster-identity "temporary-slot-fence"))
              (*backend* (make-backend))
              (marker (make-condition 'simple-error :format-control "temporary slot publication fault"))
              (observed nil))
         (let ((*logical-test-failpoint-hook*
                 (lambda (point &rest details)
                   (declare (ignore details))
                   (when (eq point :slot-file-after-rename) (error marker)))))
           (handler-case
               (sb-thread:with-mutex (*wal-lock*)
                 (physical-slot-publish-create-under-lock "failed_temporary" 40 1 t))
             (error (condition) (setf observed condition))))
         (rove:ok (eq marker observed))
         (let ((failed (gethash "failed_temporary" *wal-replication-slots*)))
           (rove:ok (wal-replication-slot-creation-failed-p failed))
           (rove:ok (eq *backend* (wal-replication-slot-temporary-owner failed)))
           (rove:ok (null (wal-replication-slot-active-owner failed)))
           (sb-thread:with-mutex (*wal-lock*)
             (wal-invalidate-replication-slots-matching-under-lock
              (lambda (slot) (declare (ignore slot))
                (error "failed creation must not reach invalidation"))))
           (sb-thread:with-mutex (*wal-lock*)
             (physical-slot-publish-create-under-lock "ordinary_temporary" nil nil t))
           (wal-cleanup-backend-physical-slots *backend*)
           (rove:ok (eq failed (gethash "failed_temporary" *wal-replication-slots*)))
           (rove:ok (= 40 (wal-slot-reserved-p failed)))
           (rove:ok (probe-file (wal-replication-slot-path "failed_temporary")))
           (rove:ok (null (gethash "ordinary_temporary" *wal-replication-slots*)))
           (rove:ok (not (probe-file (wal-replication-slot-path "ordinary_temporary")))))))))

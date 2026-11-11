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

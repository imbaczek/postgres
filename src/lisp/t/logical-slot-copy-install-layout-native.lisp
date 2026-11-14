(in-package #:pg)

(defun logical-install-layout-test-read (operation image phase prefix)
  (let* ((before (logical-install-stage-test-fd-count))
         (layout (logical-slot-copy-install-read-layout operation image)))
    (assert (eq phase (getf layout :phase)))
    (assert (= prefix (getf layout :prefix)))
    (assert (equalp image (getf layout :image)))
    (when (not *wal-ready*) (assert (= before (logical-install-stage-test-fd-count))))
    (format t "~&LAYOUT-ORIENTATION ~s ~d~%" phase prefix)
    layout))

(defun logical-install-layout-test-body (operation handoff mode)
  (let* ((image (logical-slot-copy-install-handoff-image operation handoff))
         (value (logical-slot-copy-install-decode image))
         (certificate (logical-slot-copy-import-decode (getf value :certificate)))
         (rows (getf certificate :slots))
         (root (logical-slot-copy-install-operation-state-root-fd
                (logical-slot-copy-install-operation-record operation)))
         (fds nil) (cleanup nil))
    (labels ((open-directory (parent name)
               (let ((fd (physical-image-openat parent name
                                                (logior sb-posix:o-rdonly sb-posix:o-directory
                                                        sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
                 (assert (not (minusp fd))) (push fd fds) fd))
             (sync-pair (a b) (sb-posix:fsync a) (sb-posix:fsync b))
             (move (parent name target leaf)
               (assert (zerop (slot-state-renameat2 parent name target leaf 1)))
               (push (lambda () (assert (zerop (slot-state-renameat2 target leaf parent name 1)))
                       (sync-pair parent target)) cleanup)
               (sync-pair parent target))
             (exchange (container)
               (assert (zerop (slot-state-renameat2 root "logical-slot-copy.import" container "active" 2)))
               (push (lambda ()
                       (assert (zerop (slot-state-renameat2 root "logical-slot-copy.import" container "active" 2)))
                       (sync-pair root container)) cleanup)
               (sync-pair root container))
             (file (parent name bytes &optional original)
               (let ((fd (physical-image-openat
                          parent name (logior sb-posix:o-rdwr sb-posix:o-nofollow +postmaster-o-cloexec+
                                              (if original 0 (logior sb-posix:o-creat sb-posix:o-excl))) #o600)))
                 (assert (not (minusp fd))) (push fd fds)
                 (push (if original (lambda () (logical-install-stage-test-write-bytes fd original))
                           (lambda () (sb-posix:unlink (namestring (slot-state-fd-path parent name)))))
                       cleanup)
                 (logical-install-stage-test-write-bytes fd bytes)
                 (assert (equalp bytes (read-bounded-regular-file (slot-state-fd-path parent name)
                                                                  +logical-slot-copy-install-maximum-bytes+
                                                                  :stabilize nil)))))
             (add-slot (slot-root name bytes)
               (assert (zerop (physical-image-mkdirat slot-root name #o700)))
               (push (lambda () (sb-posix:rmdir (namestring (slot-state-fd-path slot-root name)))) cleanup)
               (file (open-directory slot-root name) "state" bytes)))
      (unwind-protect
           (let* ((work (open-directory root "logical-slot-copy.work"))
                  (token (getf value :stage-id)) (container (open-directory work token))
                  (slot-root (open-directory root "pg_replslot"))
                  (slot-work (open-directory slot-root ".logical-slot-copy.work"))
                  (slots (open-directory slot-work token)))
             (logical-install-layout-test-read operation image :private 0)
             (case mode
               (:positive
                (let ((physical (make-wal-replication-slot
                                 :name "local_physical" :kind :physical :system-id (getf certificate :system-id)
                                 :restart-lsn nil :restart-timeline nil :consistent-lsn nil
                                 :confirmed-flush-lsn nil :plugin nil :wal-status :reserved :inactive-since 0)))
                  (add-slot slot-root "local_physical" (physical-slot-encode physical)))
                (let ((layout (logical-install-layout-test-read operation image :private 0)))
                  (assert (= (1+ (length rows)) (length (getf layout :observations))))
                  (setf (aref (getf layout :image) 0) 0))
                (move work token root "logical-slot-copy.install")
                (logical-install-layout-test-read operation image :pending 0)
                (loop for row in rows for prefix from 1
                      do (move slots (first row) slot-root (first row))
                         (logical-install-layout-test-read operation image :pending prefix))
                (exchange container)
                (logical-install-layout-test-read operation image :active (length rows)))
               (:historical-mounts
                (let ((changed (logical-slot-copy-install-decode image)))
                  (dolist (key (logical-slot-copy-install-identity-keys)) (incf (third (getf changed key)) 100000))
                  (dolist (row (getf changed :entries))
                    (dolist (identity (rest row)) (incf (third identity) 100000)))
                  (let ((bytes (logical-slot-copy-install-encode changed)))
                    (file container "journal" bytes image)
                    (logical-install-layout-test-read operation bytes :private 0))))
               (t
                (case mode
                  ((:closed-journal :cleanup-journal :inspection-close-error :body-close-error)
                   (when (eq mode :body-close-error)
                     (let ((bytes (copy-seq image))) (setf (aref bytes 0) 0)
                       (file container "journal" bytes image)))
                   (let ((original (symbol-function 'logical-slot-copy-install-inspection-close))
                         (fired nil))
                     (push (lambda () (setf (symbol-function 'logical-slot-copy-install-inspection-close) original)) cleanup)
                     (push (lambda () (assert fired)) cleanup)
                     (setf (symbol-function 'logical-slot-copy-install-inspection-close)
                           (lambda (node)
                             (prog1 (funcall original node)
                               (when (and (not fired)
                                          (equal (if (eq mode :closed-journal) "journal" "logical-slot-copy.work")
                                                 (logical-slot-copy-install-node-name node)))
                                 (setf fired t)
                                 (when (member mode '(:inspection-close-error :body-close-error))
                                   (error "inspection close cut"))
                                 (let ((bytes (copy-seq image)))
                                   (setf (aref bytes 0) 0)
                                   (file container "journal" bytes image))))))))
                  (:caught-restore
                   (let ((bytes (copy-seq image)))
                     (setf (aref bytes 0) 0)
                     (file container "journal" bytes image))
                   (handler-case
                       (progn (logical-slot-copy-install-read-layout operation image)
                              (error "layout accepted bad journal"))
                     (error (condition)
                       (assert (search "journal differs" (princ-to-string condition)))))
                   (file container "journal" image image)
                   (logical-install-handoff-test-denied
                    (lambda () (logical-slot-copy-install-read-layout operation image)) "scope ended"))
                  (:journal (let ((bytes (copy-seq image))) (setf (aref bytes 0) 0)
                              (file container "journal" bytes image)))
                  (:row-bytes
                   (let* ((bytes (string->bytes (fourth (first rows)))) (changed (copy-seq bytes)))
                     (setf (aref changed 0) 0)
                     (file (open-directory slots (first (first rows))) "state" changed bytes)))
                  (:hardlink
                   (sb-posix:link (namestring (slot-state-fd-path container "journal"))
                                  (namestring (slot-state-fd-path work "layout-alias")))
                   (push (lambda () (sb-posix:unlink (namestring (slot-state-fd-path work "layout-alias")))) cleanup)
                   (assert (= 2 (fourth (physical-image-file-facts container "journal")))))
                  (:fifo
                   (move root "logical-slot-copy.import" work "layout-pending-save")
                   (sb-posix:mkfifo (namestring (slot-state-fd-path root "logical-slot-copy.import")) #o600)
                   (push (lambda () (sb-posix:unlink (namestring (slot-state-fd-path root "logical-slot-copy.import")))) cleanup)
                   (assert (sb-posix:s-isfifo (third (physical-image-file-facts root "logical-slot-copy.import")))))
                  (:duplicate (add-slot slot-root (first (first rows)) (string->bytes (fourth (first rows)))))
                  (:bind-container
                   (let* ((path (namestring
                                 (merge-pathnames
                                  (format nil "logical-slot-copy.work/~a/" token)
                                  (physical-adoption-owner-directory operation))))
                          (before (physical-image-file-facts work token)))
                     (assert (zerop (sb-ext:process-exit-code
                                     (sb-ext:run-program "/usr/bin/sudo"
                                                         (list "-n" "mount" "--bind" path path)
                                                         :output *standard-output* :error *error-output*))))
                     (push (lambda ()
                             (assert (zerop (sb-ext:process-exit-code
                                             (sb-ext:run-program "/usr/bin/sudo" (list "-n" "umount" path)
                                                                 :output *standard-output* :error *error-output*)))))
                           cleanup)
                     (let ((after (physical-image-file-facts work token)))
                       (assert (equal (subseq before 0 2) (subseq after 0 2)))
                       (assert (/= (ninth before) (ninth after))))))
                  (:capacity
                   (add-slot slot-root "local_physical"
                             (physical-slot-encode
                              (make-wal-replication-slot
                               :name "local_physical" :kind :physical :system-id (getf certificate :system-id)
                               :restart-lsn nil :restart-timeline nil :consistent-lsn nil
                               :confirmed-flush-lsn nil :plugin nil :wal-status :reserved :inactive-since 0)))
                   (let ((*max-replication-slots* (length rows)))
                     (logical-slot-copy-install-read-layout operation image))
                   (error "layout accepted capacity fault"))
                  (:nonprefix
                   (move work token root "logical-slot-copy.install")
                   (move slots (first (second rows)) slot-root (first (second rows))))
                  (:private-active (exchange container))
                  (:early-active (move work token root "logical-slot-copy.install") (exchange container))
                  (:foreign-logical
                   (let ((slot (logical-slot-state-decode (string->bytes (fourth (first rows))) (first (first rows)))))
                     (setf (wal-replication-slot-name slot) "foreign")
                     (add-slot slot-root "foreign" (wal-encode-replication-slot slot))))
                  (:extra-journal (file container "unexpected" (make-array 0 :element-type '(unsigned-byte 8))))
                  (:pending-inode
                   (move root "logical-slot-copy.import" work "layout-pending-save")
                   (file root "logical-slot-copy.import" (getf value :certificate)))
                  (:slot-inode
                   (let ((row-fd (open-directory slots (first (first rows)))))
                     (move row-fd "state" work "layout-state-save")
                     (file row-fd "state" (string->bytes (fourth (first rows))))))
                  (t (error "unknown layout fault mode")))
                (logical-slot-copy-install-read-layout operation image)
                (error "layout accepted injected fault"))))
        (dolist (action cleanup) (funcall action))
        (dolist (fd fds) (sb-posix:close fd))))))

(defun logical-install-layout-test-run (mode)
  (let ((before (logical-install-stage-test-fd-count)) (cold (not *wal-ready*))
                                                       (saved nil) (caught nil)
                                                       (expected (cdr (assoc mode '((:caught-restore . "scope ended")
                                                                                    (:inspection-close-error . "inspection close cut")
                                                                                    (:body-close-error . "journal differs")
                                                                                    (:capacity . "exceeds max_replication_slots")
                                                                                    (:bind-container . "current mount group")
                                                                                    (:cleanup-journal . "inspection observation changed")
                                                                                    (:closed-journal . "inspection observation changed")
                                                                                    (:journal . "journal differs")
                                                                                    (:row-bytes . "P7 inventory or bytes")
                                                                                    (:hardlink . "binding or privacy")
                                                                                    (:fifo . "binding or privacy")
                                                                                    (:duplicate . "exactly once")
                                                                                    (:nonprefix . "installed prefix")
                                                                                    (:private-active . "phase differs")
                                                                                    (:early-active . "phase differs")
                                                                                    (:foreign-logical . "physical replication slot")
                                                                                    (:extra-journal . "container inventory")
                                                                                    (:pending-inode . "recorded inode")
                                                                                    (:slot-inode . "recorded inode"))))))
    (handler-case
        (physical-follow-call-with-controller-blocked
         (lambda ()
           (sb-thread:with-mutex (*physical-shutdown-lock*)
             (call-with-logical-slot-copy-install-handoff
              (lambda (operation handoff)
                (setf saved (list operation (logical-slot-copy-install-handoff-image operation handoff)))
                (logical-install-layout-test-body operation handoff mode))))))
      (error (condition)
        (assert (and expected (search expected (princ-to-string condition))) () "unexpected layout failure: ~a" condition)
        (setf caught t)
        (assert *physical-recovery-failed-p*)))
    (when saved
      (logical-install-handoff-test-denied
       (lambda () (apply #'logical-slot-copy-install-read-layout saved)) "scope ended"))
    (assert (eq (not (null expected)) caught))
    (assert (eq caught (not (null *physical-recovery-failed-p*))))
    (when cold
      (assert (logical-install-handoff-test-unopened-p))
      (assert (= before (logical-install-stage-test-fd-count)))))
  (format t "~&LOGICAL-LAYOUT-PASS ~s~%" mode) (finish-output))

(defun logical-install-layout-test-start (mode)
  (setf *logical-install-handoff-test-result* :running
        *logical-install-handoff-test-thread*
        (sb-thread:make-thread
         (lambda ()
           (handler-case (progn (logical-install-layout-test-run mode)
                                (setf *logical-install-handoff-test-result* :passed))
             (error (condition) (setf *logical-install-handoff-test-result* (list :error (princ-to-string condition))))))
         :name "owned layout inspection")))

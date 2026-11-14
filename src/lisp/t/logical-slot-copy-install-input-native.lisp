(in-package #:pg)

(defvar *logical-install-input-test-result* nil)
(defvar *logical-install-input-test-thread* nil)
(defvar *logical-install-input-test-saved* nil)

(defun logical-install-input-test-move-layout (operation handoff prefix active &optional observer)
  (let* ((borrowed (logical-slot-copy-install-operation-state-root-fd
                    (logical-slot-copy-install-operation-record operation)))
         (root (sb-posix:fcntl borrowed 1030 0))
         (value (logical-slot-copy-install-decode
                 (logical-slot-copy-install-handoff-image operation handoff)))
         (fds (list root)) (undo nil) (transferred nil) (cleaned nil))
    (labels ((open-directory (parent name)
               (let ((fd (physical-image-openat parent name
                                                (logior sb-posix:o-rdonly sb-posix:o-directory
                                                        sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
                 (assert (not (minusp fd))) (push fd fds) fd))
             (sync (a b) (sb-posix:fsync a) (sb-posix:fsync b))
             (move (from name to target)
               (assert (zerop (slot-state-renameat2 from name to target 1)))
               (push (lambda () (assert (zerop (slot-state-renameat2 to target from name 1)))
                       (sync from to)) undo)
               (sync from to)
               (when observer (funcall observer)))
             (cleanup ()
               (unless cleaned
                 (setf cleaned t)
                 (dolist (action undo) (funcall action))
                 (dolist (fd fds) (sb-posix:close fd)))))
      (unwind-protect
           (let* ((work (open-directory root "logical-slot-copy.work"))
                  (token (getf value :stage-id))
                  (container (open-directory work token))
                  (slot-root (open-directory root "pg_replslot"))
                  (slot-work (open-directory slot-root ".logical-slot-copy.work"))
                  (slots (open-directory slot-work token)))
             (move work token root "logical-slot-copy.install")
             (loop for row in (getf value :entries) for count below prefix
                   do (move slots (first row) slot-root (first row)))
             (when active
               (assert (= prefix (length (getf value :entries))))
               (assert (zerop (slot-state-renameat2 root "logical-slot-copy.import" container "active" 2)))
               (push (lambda ()
                       (assert (zerop (slot-state-renameat2 root "logical-slot-copy.import" container "active" 2)))
                       (sync root container)) undo)
               (sync root container)
               (when observer (funcall observer)))
             (setf transferred t)
             #'cleanup)
        (unless transferred (cleanup))))))

(defun logical-install-input-test-physical (mode)
  (let* ((root (physical-backup-open-directory *pgdata*))
         (slot-root nil) (directory nil) (state-fd nil) (made nil) (created nil) (registered nil) (complete nil) (cleaned nil)
         (name "kept_physical")
         (certificate (logical-slot-copy-import-decode
                       (read-bounded-regular-file
                        (slot-state-fd-path root "logical-slot-copy.import")
                        +logical-slot-copy-import-maximum-bytes+ :stabilize nil)))
         (slot (make-wal-replication-slot
                :name name :kind :physical :plugin nil :consistent-lsn nil :confirmed-flush-lsn nil
                :system-id (if (eq mode :physical-system) (logxor 1 (getf certificate :system-id))
                               (getf certificate :system-id))
                :restart-lsn (if (eq mode :physical-future) (1+ (getf certificate :fork))
                                 (getf certificate :base-end))
                :restart-timeline (getf certificate :base-timeline)
                :wal-status (if (eq mode :physical-lost) :lost :reserved)
                :temporary-p (member mode '(:physical-temporary :physical-live-temp))
                :inactive-since 0)))
    (setf (wal-replication-slot-temporary-p slot) (not (null (wal-replication-slot-temporary-p slot))))
    (labels ((cleanup ()
               (unless cleaned
                 (setf cleaned t)
                 (when registered
                   (assert (eq slot (gethash name *wal-replication-slots*)))
                   (remhash name *wal-replication-slots*))
                 (when state-fd (sb-posix:close state-fd) (setf state-fd nil))
                 (when created
                   (sb-posix:unlink (namestring (slot-state-fd-path directory "state"))))
                 (when made (sb-posix:rmdir (namestring (slot-state-fd-path slot-root name))))
                 (when directory (sb-posix:close directory))
                 (when slot-root (sb-posix:close slot-root))
                 (sb-posix:close root))))
      (unwind-protect
           (progn
             (unless (slot-state-lstat (slot-state-fd-path root "pg_replslot"))
               (assert (zerop (physical-image-mkdirat root "pg_replslot" #o700))))
             (setf slot-root (physical-image-openat
                              root "pg_replslot" (logior sb-posix:o-rdonly sb-posix:o-directory
                                                         sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
             (when (minusp slot-root) (setf slot-root nil) (error "fixture slot root open failed"))
             (assert (zerop (physical-image-mkdirat slot-root name #o700)))
             (setf made t)
             (setf directory (physical-image-openat
                              slot-root name (logior sb-posix:o-rdonly sb-posix:o-directory
                                                     sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
             (when (minusp directory) (setf directory nil) (error "fixture slot directory open failed"))
             (setf state-fd (physical-image-openat
                             directory "state" (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                                                       sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
             (when (minusp state-fd) (setf state-fd nil) (error "fixture state open failed"))
             (setf created t)
             (logical-install-stage-test-write-bytes state-fd (physical-slot-encode slot))
             (sb-posix:fsync directory) (sb-posix:fsync slot-root)
             (when *wal-ready*
               (setf (wal-replication-slot-inactive-since slot) 123456
                     (wal-replication-slot-active-owner slot) (gensym "physical owner")
                     (wal-replication-slot-temporary-owner slot)
                     (when (wal-replication-slot-temporary-p slot) (gensym "temporary owner"))
                     (gethash name *wal-replication-slots*) slot
                     registered t))
             (setf complete t)
             (values #'cleanup slot))
        (unless complete (cleanup))))))

(defun logical-install-input-test-rewrite (root name bytes)
  (let ((fd (physical-image-openat root name
                                   (logior sb-posix:o-rdwr sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
    (assert (not (minusp fd)))
    (unwind-protect (logical-install-stage-test-write-bytes fd bytes)
      (sb-posix:close fd))))

(defun logical-install-input-test-inspect (owner context input)
  (assert (logical-slot-copy-install-input-owner-p owner))
  (assert (physical-image-read-context-p context))
  (assert (null *logical-slot-copy-install-preparations*))
  (assert (null *logical-slot-copy-install-stages*))
  (assert (null *logical-slot-copy-install-file-scope*))
  (assert (eq :pending (getf (logical-slot-copy-install-input-certificate input) :phase)))
  (logical-slot-copy-install-check-input owner context input)
  (logical-install-handoff-test-denied
   (lambda () (physical-adoption-check-owner owner)) "does not authorize offline adoption")
  (logical-install-handoff-test-denied
   (lambda () (logical-slot-copy-install-check-input (copy-structure owner) context input)) "registered")
  (logical-install-handoff-test-denied
   (lambda () (logical-slot-copy-install-check-input owner (copy-structure context) input)) "registered")
  (logical-install-handoff-test-denied
   (lambda () (logical-slot-copy-install-check-input owner context (copy-structure input))) "registered")
  (let* ((inputs *logical-slot-copy-install-inputs*) (operations *logical-slot-copy-install-operations*)
                                                     (thread
                                                       (sb-thread:make-thread
                                                        (lambda ()
                                                          (let ((*logical-slot-copy-install-inputs* inputs)
                                                                (*logical-slot-copy-install-operations* operations))
                                                            (handler-case
                                                                (progn (logical-slot-copy-install-check-input owner context input) nil)
                                                              (error (condition) (not (null (search "changed thread" (princ-to-string condition))))))))
                                                        :name "foreign installation input observer")))
    (assert (eq t (sb-thread:join-thread thread :timeout 5 :default :timeout))))
  (setf *logical-install-input-test-saved* (list owner context input))
  t)

(defun logical-install-input-test-fault (mode owner context input add-cleanup mark-fired)
  (labels ((check () (logical-slot-copy-install-check-input owner context input)))
    (case mode
      ((:proof-seal :caught-proof)
       (let* ((image (logical-slot-copy-install-input-image input)) (before (aref image 0)))
         (funcall add-cleanup (lambda () (setf (aref image 0) before)))
         (setf (aref image 0) 0) (funcall mark-fired)
         (if (eq mode :caught-proof)
             (progn
               (handler-case (progn (check) (error "proof mutation was accepted"))
                 (error (condition) (assert (search "proof image" (princ-to-string condition)))))
               (setf (aref image 0) before)
               (logical-install-handoff-test-denied #'check "scope ended"))
             (check))))
      (:proof-cycle
       (let* ((state (logical-slot-copy-committed-proof-state (logical-slot-copy-install-input-proof input)))
              (tail (cddr state)))
         (funcall add-cleanup (lambda () (setf (cddr state) tail)))
         (setf (cddr state) state) (funcall mark-fired) (check)))
      (:source-cut
       (let ((original (symbol-function 'logical-slot-copy-check-committed-source)))
         (funcall add-cleanup (lambda () (setf (symbol-function 'logical-slot-copy-check-committed-source) original)))
         (setf (symbol-function 'logical-slot-copy-check-committed-source)
               (lambda (&rest values) (declare (ignore values))
                 (funcall mark-fired) (error "installation input source cut")))
         (check)))
      (:callback-error (funcall mark-fired) (error "installation input callback cut"))
      (:callback-throw (funcall mark-fired) (throw 'installation-input-exit :escaped))
      (:backwards
       (let* ((record (logical-slot-copy-install-input-owner-record owner))
              (cleanup (logical-install-input-test-move-layout
                        (logical-slot-copy-install-input-owner-operation owner)
                        (logical-slot-copy-install-input-state-handoff record)
                        2 t #'check)))
         (funcall add-cleanup cleanup)
         (funcall cleanup) (funcall mark-fired) (check)))
      (:physical-owner-cut
       (assert *wal-ready*)
       (let* ((slot (gethash "kept_physical" *wal-replication-slots*))
              (original (wal-replication-slot-active-owner slot)))
         (funcall add-cleanup (lambda () (setf (wal-replication-slot-active-owner slot) original)))
         (setf (wal-replication-slot-active-owner slot) (gensym "substitute physical owner"))
         (funcall mark-fired) (check)))
      (:controller-cut
       (assert *wal-ready*)
       (let ((*physical-follow-controller-blocked-p* nil))
         (handler-case (progn (check) (error "controller mutation was accepted"))
           (error (condition)
             (assert (search "controller and shutdown" (princ-to-string condition)))
             (funcall mark-fired))))
       (logical-install-handoff-test-denied #'check "scope ended"))
      ((:store-close-error :late-child :late-phase)
       (assert (not *wal-ready*))
       (let* ((original (symbol-function 'physical-store-close))
              (target (logical-slot-copy-install-input-store input))
              (record (logical-slot-copy-install-input-owner-record owner))
              (operation (logical-slot-copy-install-input-owner-operation owner))
              (handoff (logical-slot-copy-install-input-state-handoff record))
              (changed nil) (root nil) (name nil) (bytes nil))
         (when (eq mode :late-child)
           (setf root (sb-posix:fcntl (physical-image-context-root-fd context) 1030 0)
                 name (first (find-if
                              (lambda (row)
                                (let ((path (first row)))
                                  (and (>= (length path) 11)
                                       (string= "pg_wal/" path :end2 7)
                                       (string= ".wal" path :start2 (- (length path) 4)))))
                              (logical-slot-copy-committed-proof-observations
                               (logical-slot-copy-install-input-proof input))))
                 bytes (read-bounded-regular-file (slot-state-fd-path root name)
                                                  +logical-slot-copy-install-maximum-bytes+ :stabilize nil))
           (assert (plusp (length bytes)))
           (funcall add-cleanup
                    (lambda () (unwind-protect (logical-install-input-test-rewrite root name bytes)
                                 (sb-posix:close root)))))
         (funcall add-cleanup (lambda () (setf (symbol-function 'physical-store-close) original)))
         (setf (symbol-function 'physical-store-close)
               (lambda (store)
                 (prog1 (funcall original store)
                   (when (and (eq store target) (not changed))
                     (setf changed t) (funcall mark-fired)
                     (ecase mode
                       (:store-close-error (error "installation input store close cut"))
                       (:late-child
                        (let ((copy (copy-seq bytes)))
                          (setf (aref copy 0) (logxor 1 (aref copy 0)))
                          (logical-install-input-test-rewrite root name copy)))
                       (:late-phase
                        (funcall add-cleanup
                                 (logical-install-input-test-move-layout operation handoff 0 nil))))))))))
      (t (error "unknown installation input fault")))))

(defun logical-install-input-test-run (mode)
  (let* ((before (logical-install-stage-test-fd-count)) (cold (not *wal-ready*))
                                                        (cleanup nil) (auxiliary nil) (physical-cleanup nil) (physical nil)
                                                        (called nil) (caught nil) (fired nil)
                                                        (expected (cdr (assoc mode
                                                                              '((:proof-seal . "proof image") (:proof-cycle . "proof image")
                                                                                (:caught-proof . "scope ended") (:source-cut . "installation input source cut")
                                                                                (:callback-error . "installation input callback cut")
                                                                                (:callback-throw . :nonlocal) (:backwards . "moved backwards")
                                                                                (:controller-cut . "scope ended") (:physical-owner-cut . "registry ownership changed") (:store-close-error . "store close cut")
                                                                                (:late-child . "input changed after verification")
                                                                                (:late-phase . "layout changed after its source scope")
                                                                                (:physical-system . "another cluster") (:physical-future . "outside durable source history")
                                                                                (:physical-temporary . "temporary physical") (:cold-registry . "unopened storage")
                                                                                (:bad-file . "cannot open logical installation inspection inode")
                                                                                (:bad-symlink . "cannot open logical installation inspection inode")
                                                                                (:bad-fifo . "cannot open logical installation inspection inode")
                                                                                (:bad-journal . "invalid logical slot installation envelope"))))))
    (setf *logical-install-input-test-saved* nil)
    (labels ((add-cleanup (function) (push function auxiliary))
             (mark-fired () (setf fired t))
             (inspect-input (owner context input)
               (setf called t)
               (logical-install-input-test-inspect owner context input)
               (when (and physical *wal-ready*)
                 (assert (eq physical
                             (first (first (logical-slot-copy-install-input-state-physical-witnesses
                                            (logical-slot-copy-install-input-owner-record owner)))))))
               (cond
                 ((eq mode :fresh-active)
                  (let ((record (logical-slot-copy-install-input-owner-record owner)))
                    (setf cleanup
                          (logical-install-input-test-move-layout
                           (logical-slot-copy-install-input-owner-operation owner)
                           (logical-slot-copy-install-input-state-handoff record)
                           2 t (lambda () (logical-slot-copy-install-check-input owner context input)))))
                  (assert (eq :active (logical-slot-copy-install-input-state-phase
                                       (logical-slot-copy-install-input-owner-record owner)))))
                 ((and expected (not (member mode '(:physical-system :physical-future :physical-temporary :cold-registry))))
                  (logical-install-input-test-fault mode owner context input #'add-cleanup #'mark-fired))
                 ((not (member mode '(:fresh-private :fresh-private-retry :physical :physical-lost :physical-live-temp
                                      :physical-system :physical-future :physical-temporary :cold-registry)))
                  (error "unknown installation input callback mode"))))
             (run ()
               (cond
                 ((eq mode :absent)
                  (assert cold)
                  (assert (null (call-with-logical-slot-copy-install-cold-input
                                 (lambda (&rest values) (declare (ignore values)) (setf called t))))))
                 ((eq mode :orphan)
                  (assert cold)
                  (call-with-logical-slot-copy-install-handoff
                   (lambda (operation handoff) (declare (ignore operation handoff)) nil))
                  (assert (null (call-with-logical-slot-copy-install-cold-input
                                 (lambda (&rest values) (declare (ignore values)) (setf called t))))))
                 ((eq mode :cold-denied)
                  (assert (not cold))
                  (logical-install-handoff-test-denied
                   (lambda () (call-with-logical-slot-copy-install-cold-input #'inspect-input)) "cold startup"))
                 ((eq mode :unfenced)
                  (assert (not cold))
                  (call-with-logical-slot-copy-install-handoff
                   (lambda (operation handoff)
                     (logical-install-handoff-test-denied
                      (lambda () (call-with-logical-slot-copy-install-input operation handoff #'inspect-input))
                      "controller and shutdown")
                     (assert (logical-slot-copy-install-handoff-image operation handoff)))))
                 ((member mode '(:cold-zero :cold-one :cold-all :cold-active))
                  (assert cold)
                  (let ((prefix (case mode (:cold-zero 0) (:cold-one 1) (t 2))))
                    (call-with-logical-slot-copy-install-handoff
                     (lambda (operation handoff)
                       (setf cleanup (logical-install-input-test-move-layout
                                      operation handoff prefix (eq mode :cold-active)))))
                    (let ((names (sort (directory-entry-names
                                        (merge-pathnames "logical-slot-copy.work/" *pgdata*) :follow-symlinks t)
                                       #'string<)))
                      (call-with-logical-slot-copy-install-cold-input
                       (lambda (owner context input)
                         (setf called t)
                         (logical-install-input-test-inspect owner context input)
                         (let* ((record (logical-slot-copy-install-input-owner-record owner))
                                (operation (logical-slot-copy-install-input-owner-operation owner)))
                           (assert (eq :cold (logical-slot-copy-install-input-state-origin record)))
                           (assert (null (logical-slot-copy-install-operation-state-handoffs
                                          (logical-slot-copy-install-operation-record operation))))
                           (assert (= prefix (logical-slot-copy-install-input-state-prefix record)))
                           (assert (eq (if (eq mode :cold-active) :active :pending)
                                       (logical-slot-copy-install-input-state-phase record))))))
                      (assert (equal names (sort (directory-entry-names
                                                  (merge-pathnames "logical-slot-copy.work/" *pgdata*)
                                                  :follow-symlinks t) #'string<))))))
                 ((member mode '(:bad-file :bad-symlink :bad-fifo :bad-journal))
                  (assert cold)
                  (let ((path (merge-pathnames "logical-slot-copy.install" *pgdata*)))
                    (ecase mode
                      (:bad-file
                       (with-open-file (out path :direction :output :if-exists :error) (write-line "bad" out))
                       (add-cleanup (lambda () (sb-posix:unlink (namestring path)))))
                      (:bad-symlink
                       (sb-posix:symlink "no-such-installation" (namestring path))
                       (add-cleanup (lambda () (sb-posix:unlink (namestring path)))))
                      (:bad-fifo
                       (sb-posix:mkfifo (namestring path) #o600)
                       (add-cleanup (lambda () (sb-posix:unlink (namestring path)))))
                      (:bad-journal
                       (sb-posix:mkdir (namestring path) #o700)
                       (let ((journal (merge-pathnames "journal" (uiop:ensure-directory-pathname path))))
                         (with-open-file (out journal :direction :output :if-exists :error) (write-line "bad" out))
                         (sb-posix:chmod (namestring journal) #o600)
                         (add-cleanup (lambda () (sb-posix:unlink (namestring journal)) (sb-posix:rmdir (namestring path)))))))
                    (mark-fired)
                    (call-with-logical-slot-copy-install-cold-input #'inspect-input)))
                 (t (call-with-logical-slot-copy-install-fresh-input #'inspect-input)))))
      (unwind-protect
           (progn
             (when (member mode '(:physical :physical-lost :physical-live-temp
                                  :physical-system :physical-future :physical-temporary :physical-owner-cut))
               (multiple-value-setq (physical-cleanup physical) (logical-install-input-test-physical mode))
               (when expected (mark-fired)))
             (when (eq mode :cold-registry)
               (assert cold)
               (setf (gethash "unrelated" *wal-replication-slots*) :occupied)
               (add-cleanup (lambda () (remhash "unrelated" *wal-replication-slots*)))
               (mark-fired))
             (handler-case
                 (let ((result (catch 'installation-input-exit (run))))
                   (when (eq result :escaped)
                     (assert (eq mode :callback-throw))
                     (setf caught t)))
               (error (condition)
                 (assert (and (stringp expected) (search expected (princ-to-string condition)))
                         () "unexpected installation input failure ~s: ~a" mode condition)
                 (setf caught t))))
        (dolist (function auxiliary) (funcall function))
        (when cleanup (funcall cleanup))
        (when physical-cleanup (funcall physical-cleanup)))
      (assert (eq (not (null expected)) caught))
      (assert (or (not expected) fired))
      (assert (eq caught (not (null *physical-recovery-failed-p*))))
      (when (member mode '(:absent :orphan :unfenced :cold-denied :bad-file :bad-symlink :bad-fifo :bad-journal
                           :physical-system :physical-future :physical-temporary :cold-registry))
        (assert (not called)))
      (when cold
        (assert (logical-install-handoff-test-unopened-p))
        (assert (zerop (hash-table-count *wal-replication-slots*)))
        (assert (= before (logical-install-stage-test-fd-count))))
      (when *logical-install-input-test-saved*
        (logical-install-handoff-test-denied
         (lambda () (apply #'logical-slot-copy-install-check-input *logical-install-input-test-saved*)) "registered"))))
  (format t "~&LOGICAL-INSTALL-INPUT-PASS ~s~%" mode) (finish-output))

(defun logical-install-input-test-start (mode)
  (setf *logical-install-input-test-result* :running
        *logical-install-input-test-thread*
        (sb-thread:make-thread
         (lambda ()
           (handler-case (progn (logical-install-input-test-run mode)
                                (setf *logical-install-input-test-result* :passed))
             (error (condition) (setf *logical-install-input-test-result* (list :error (princ-to-string condition))))))
         :name "owned installation input")))

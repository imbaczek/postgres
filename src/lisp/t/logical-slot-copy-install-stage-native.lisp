(in-package #:pg)

(defvar *logical-install-stage-test-injected* nil)

(defun logical-install-stage-test-fd-count ()
  (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))

(defvar *logical-install-stage-test-saved* nil)

(defvar *logical-install-stage-test-close-target* nil)

(defvar *logical-install-stage-test-close-fds* nil)

(defun logical-install-stage-test-write-bytes (fd bytes)
  (assert (zerop (sb-posix:lseek fd 0 sb-posix:seek-set)))
  (sb-sys:with-pinned-objects (bytes)
    (loop with position = 0 while (< position (length bytes))
          for count = (sb-posix:write fd (sb-sys:sap+ (sb-sys:vector-sap bytes) position)
                                      (- (length bytes) position))
          do (assert (plusp count)) (incf position count)))
  (sb-posix:fsync fd))

(defun logical-install-stage-test-denied (function)
  (assert (handler-case (progn (funcall function) nil)
            (error (condition) (search "not registered" (princ-to-string condition))))))

(defun logical-install-stage-test-node-authority (parent)
  (let* ((path (sb-ext:posix-getenv "PG_LISP_STAGE_OUTSIDE"))
         (directory (sb-ext:posix-getenv "PG_LISP_STAGE_OUTSIDE_DIRECTORY"))
         (outside (sb-posix:open path (logior sb-posix:o-rdwr sb-posix:o-nofollow)))
         (foreign-directory nil)
         (bytes (make-array 1 :element-type '(unsigned-byte 8) :initial-element 42)))
    (unwind-protect
         (progn
           (setf foreign-directory (sb-posix:open directory (logior sb-posix:o-rdonly
                                                                    sb-posix:o-directory sb-posix:o-nofollow)))
           (let ((foreign (%make-logical-slot-copy-install-node
                           :fd outside :kind :file
                           :identity (logical-slot-copy-install-file-identity (physical-image-file-facts outside))))
                 (foreign-parent (%make-logical-slot-copy-install-node
                                  :fd foreign-directory :kind :directory
                                  :identity (logical-slot-copy-install-file-identity
                                             (physical-image-file-facts foreign-directory)))))
             (logical-install-stage-test-denied
              (lambda () (logical-slot-copy-install-write-node foreign bytes)))
             (logical-install-stage-test-denied
              (lambda () (logical-slot-copy-install-read-node foreign 0)))
             (logical-install-stage-test-denied
              (lambda () (logical-slot-copy-install-sync-node foreign)))
             (logical-install-stage-test-denied
              (lambda () (logical-slot-copy-install-open-node foreign-parent "forbidden" :directory nil t t)))
             (logical-install-stage-test-denied
              (lambda () (logical-slot-copy-install-open-node parent "forbidden" :directory foreign-parent t t))))
           (let* ((stale (logical-slot-copy-install-open-node parent "scratch" :file parent t t))
                  (fd (logical-slot-copy-install-node-fd stale)))
             (logical-slot-copy-install-close-node stale)
             (sb-posix:unlink (slot-state-fd-path (logical-slot-copy-install-node-fd parent) "scratch"))
             (assert (= fd (sb-posix:dup2 outside fd)))
             (unwind-protect
                  (progn
                    (assert (= (sb-posix:stat-ino (sb-posix:fstat outside))
                               (sb-posix:stat-ino (sb-posix:fstat fd))))
                    (logical-install-stage-test-denied
                     (lambda () (logical-slot-copy-install-write-node stale bytes)))
                    (logical-install-stage-test-denied
                     (lambda () (logical-slot-copy-install-read-node stale 0)))
                    (logical-install-stage-test-denied
                     (lambda () (logical-slot-copy-install-sync-node stale)))
                    (logical-install-stage-test-denied
                     (lambda () (logical-slot-copy-install-open-node stale "forbidden" :directory nil t t))))
               (sb-posix:close fd)))
           (assert (zerop (sb-posix:stat-size (sb-posix:fstat outside)))))
      (when foreign-directory (sb-posix:close foreign-directory))
      (sb-posix:close outside))))

(defun logical-install-stage-test-replace-directory (node)
  (let* ((parent (logical-slot-copy-install-node-fd (logical-slot-copy-install-node-parent node)))
         (name (logical-slot-copy-install-node-name node)))
    (assert (zerop (port-renameat parent name parent (concatenate 'string name ".moved"))))
    (assert (zerop (physical-image-mkdirat parent name #o700)))
    (setf *logical-install-stage-test-injected* t)))

(defun logical-install-stage-test-mutate (stage directory mode)
  (let* ((nodes (logical-slot-copy-install-stage-nodes stage))
         (value (logical-slot-copy-install-stage-value stage))
         (container (getf nodes :container))
         (journal (getf nodes :journal))
         (parent (logical-slot-copy-install-node-fd container))
         (path (slot-state-fd-path parent "journal")))
    (ecase mode
      ((nil :trace :authority :pg-collision :slot-collision) nil)
      (:value
       (setf (getf value :evidence-digest) (logxor 1 (getf value :evidence-digest))))
      (:image
       (let ((image (logical-slot-copy-install-stage-image stage)))
         (setf (aref image 0) (logxor 1 (aref image 0)))))
      (:extra
       (sb-posix:mkfifo (namestring (merge-pathnames "extra" directory)) #o600))
      (:mode
       (sb-posix:chmod (namestring (merge-pathnames
                                    (format nil "~a/state" (first (first (getf value :entries))))
                                    directory)) #o644))
      (:journal-rewrite
       (sleep 0.001)
       (logical-install-stage-test-write-bytes
        (logical-slot-copy-install-node-fd journal) (logical-slot-copy-install-stage-image stage)))
      (:journal-replace
       (let ((old (logical-slot-copy-install-node-identity journal))
             (bytes (logical-slot-copy-install-stage-image stage)) (fd nil))
         (sb-posix:unlink path)
         (unwind-protect
              (progn
                (setf fd (sb-posix:open path (logior sb-posix:o-rdwr sb-posix:o-creat
                                                     sb-posix:o-excl sb-posix:o-nofollow) #o600))
                (logical-install-stage-test-write-bytes fd bytes)
                (assert (/= (second old) (sb-posix:stat-ino (sb-posix:fstat fd))))
                (assert (equalp bytes (read-bounded-regular-file path (length bytes) :stabilize nil))))
           (when fd (sb-posix:close fd)))))
      (:journal-symlink
       (sb-posix:unlink path)
       (sb-posix:symlink "active" path))
      (:state-hardlink
       (sb-posix:link
        (namestring (merge-pathnames
                     (format nil "~a/state" (first (first (getf value :entries)))) directory))
        (slot-state-fd-path (logical-slot-copy-install-node-fd (getf nodes :work))
                            (concatenate 'string "linked-" (getf value :stage-id)))))
      (:rename-container
       (logical-install-stage-test-replace-directory container))
      ((:close :body-close)
       (setf *logical-install-stage-test-close-target* (logical-slot-copy-install-node-fd (getf nodes :pending-file))
             *logical-install-stage-test-close-fds* (loop for (key node) on nodes by #'cddr
                                                          unless (eq key :root)
                                                            collect (logical-slot-copy-install-node-fd node)))
       (when (eq mode :body-close)
         (setf *logical-install-stage-test-injected* t)
         (error "logical staging original body cut")))
      (:nonlocal
       (setf *logical-install-stage-test-injected* t)
       (throw 'stage-test-exit :escaped)))
    (unless (member mode '(nil :trace :authority :close :pg-collision :slot-collision))
      (setf *logical-install-stage-test-injected* t))))

(defun logical-install-stage-test-event (point name cut counts)
  (let* ((name (if (and (stringp name) (physical-backup-lease-hex-p name 32)) :nonce name))
         (key (list point name)) (entry (assoc key (car counts) :test #'equal))
         (ordinal (if entry (incf (cdr entry))
                      (progn (push (cons key 1) (car counts)) 1))))
    (format t "~&LOGICAL-STAGE-EVENT ~s ~s ~d~%" point name ordinal)
    (when (and cut (eq point (first cut)) (equal name (second cut)) (= ordinal (third cut)))
      (format t "~&LOGICAL-STAGE-KILL ~s~%" cut)
      (finish-output)
      (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill))))

(defun logical-install-stage-test-inspect (owner context attempt stage mode)
  (logical-slot-copy-install-check-stage owner context attempt stage)
  (let* ((nodes (logical-slot-copy-install-stage-nodes stage))
         (value (logical-slot-copy-install-stage-value stage))
         (certificate (logical-slot-copy-install-preparation-certificate attempt))
         (root (physical-adoption-owner-directory owner))
         (directory (directory-pathname
                     (format nil "~apg_replslot/.logical-slot-copy.work/~a/"
                             root (getf value :stage-id))))
         (seen nil))
    (slot-state-map
     directory
     (lambda (bytes name)
       (let ((row (find name (getf certificate :slots) :key #'first :test #'string=)))
         (assert row)
         (assert (equalp bytes (string->bytes (fourth row))))
         (assert (wal-decode-replication-slot-image bytes name))
         (push name seen)))
     :parent-fd (logical-slot-copy-install-node-fd (getf nodes :slot-work))
     :stabilize nil :require-root t)
    (assert (equal (sort seen #'string<) (mapcar #'first (getf certificate :slots))))
    (assert (not (slot-state-lstat
                  (slot-state-fd-path (physical-image-context-root-fd context)
                                      "logical-slot-copy.install"))))
    (logical-install-stage-test-mutate stage directory mode)))

(defun logical-install-stage-test-bind-mount ()
  (let* ((root (fifth *logical-slot-copy-install-file-scope*))
         (resources (car (fourth *logical-slot-copy-install-file-scope*)))
         (node (find "pg_replslot" resources :key #'logical-slot-copy-install-node-name :test #'equal))
         (path (namestring (merge-pathnames
                            "pg_replslot/"
                            (physical-adoption-owner-directory (first *logical-slot-copy-install-file-scope*)))))
         (process (sb-ext:run-program "sudo" (list "-n" "mount" "--bind" path path)
                                      :search t :wait t :input nil :output *standard-output* :error *error-output*)))
    (assert (zerop (sb-ext:process-exit-code process)))
    (let ((held (physical-image-file-facts (logical-slot-copy-install-node-fd node)))
          (named (physical-image-file-facts (logical-slot-copy-install-node-fd root) "pg_replslot")))
      (assert (equal (subseq held 0 2) (subseq named 0 2)))
      (assert (/= (ninth held) (ninth named)))
      (format t "~&LOGICAL-STAGE-BIND-MOUNT-CHANGED ~d ~d~%" (ninth held) (ninth named))
      (setf *logical-install-stage-test-injected* t))))

(defun logical-install-stage-test-owned-state-fd-p (fd)
  (when *logical-slot-copy-install-file-scope*
    (let ((node (find fd (car (fourth *logical-slot-copy-install-file-scope*))
                      :key #'logical-slot-copy-install-node-fd :test #'eql)))
      (and node (equal "state" (logical-slot-copy-install-node-name node))))))

(defun logical-install-stage-test-run (mode cut)
  (let* ((mkdir (symbol-function 'physical-image-mkdirat))
         (open (symbol-function 'physical-image-openat))
         (close (symbol-function 'sb-posix:close))
         (sync (symbol-function 'sb-posix:fsync))
         (read (symbol-function 'sb-posix:read))
         (write (symbol-function 'sb-posix:write))
         (counts (list nil))
         (*logical-install-stage-test-close-target* nil)
         (*logical-install-stage-test-close-fds* nil)
         (*logical-slot-copy-install-file-test-hook*
           (lambda (point name)
             (when (or cut (eq mode :trace))
               (logical-install-stage-test-event point name cut counts))
             (when (and (eq mode :bind) (eq point :after-install-inode-open) (equal name "pg_replslot"))
               (logical-install-stage-test-bind-mount))
             (when (and (eq mode :parent-binding) (eq point :after-install-parent-fsync)
                        (physical-backup-lease-hex-p name 32))
               (logical-install-stage-test-replace-directory
                (find name (car (fourth *logical-slot-copy-install-file-scope*))
                      :key #'logical-slot-copy-install-node-name :test #'equal)))
             (when (and (eq mode :partial) (eq point :after-install-file-write) (equal name "state"))
               (setf *logical-install-stage-test-injected* t)
               (error "logical staging partial write cut")))))
    (unwind-protect
         (progn
           (when (or cut (member mode '(:trace :mkdir-error)))
             (setf (symbol-function 'physical-image-mkdirat)
                   (lambda (fd name permissions)
                     (if (and *logical-slot-copy-install-file-scope* (eq mode :mkdir-error)
                              (physical-backup-lease-hex-p name 32))
                         (progn (setf *logical-install-stage-test-injected* t) -1)
                         (let ((result (funcall mkdir fd name permissions)))
                           (when (zerop result)
                             (logical-install-stage-test-event :mkdir name cut counts))
                           result)))))
           (when (eq mode :open-error)
             (setf (symbol-function 'physical-image-openat)
                   (lambda (fd name flags permissions)
                     (if (and *logical-slot-copy-install-file-scope* (equal name "state"))
                         (progn (setf *logical-install-stage-test-injected* t) -1)
                         (funcall open fd name flags permissions)))))
           (when (member mode '(:parent-sync :file-sync))
             (setf (symbol-function 'sb-posix:fsync)
                   (lambda (fd)
                     (let ((file (sb-posix:s-isreg (sb-posix:stat-mode (sb-posix:fstat fd)))))
                       (when (and *logical-slot-copy-install-file-scope*
                                  (eq (not (null file)) (eq mode :file-sync)))
                         (setf *logical-install-stage-test-injected* t)
                         (error "logical staging fsync cut ~s" mode)))
                     (funcall sync fd))))
           (when (eq mode :write-error)
             (setf (symbol-function 'sb-posix:write)
                   (lambda (fd pointer size)
                     (if (logical-install-stage-test-owned-state-fd-p fd)
                         (progn (setf *logical-install-stage-test-injected* t)
                                (error "logical staging write cut"))
                         (funcall write fd pointer size)))))
           (when (eq mode :read-error)
             (setf (symbol-function 'sb-posix:read)
                   (lambda (fd pointer size)
                     (if (logical-install-stage-test-owned-state-fd-p fd)
                         (progn (setf *logical-install-stage-test-injected* t)
                                (error "logical staging read cut"))
                         (funcall read fd pointer size)))))
           (when (member mode '(:close :body-close))
             (setf (symbol-function 'sb-posix:close)
                   (lambda (fd)
                     (prog1 (funcall close fd)
                       (setf *logical-install-stage-test-close-fds*
                             (delete fd *logical-install-stage-test-close-fds*))
                       (when (eql fd *logical-install-stage-test-close-target*)
                         (setf *logical-install-stage-test-injected* t)
                         (error "logical staging close cut"))))))
           (catch 'stage-test-exit
             (call-with-logical-slot-copy-install-preparation
              (lambda (owner context attempt)
                (when (eq mode :authority)
                  (call-with-logical-slot-copy-install-file-scope
                   owner context attempt #'logical-install-stage-test-node-authority))
                (when (member mode '(:pg-collision :slot-collision :pg-work-file :slot-work-symlink))
                  (setf *logical-install-stage-test-injected* t))
                (call-with-logical-slot-copy-install-staged
                 owner context attempt
                 (lambda (stage)
                   (setf *logical-install-stage-test-saved* (list owner context attempt stage))
                   (logical-install-stage-test-inspect owner context attempt stage mode))
                 :stage-id (case mode
                             (:pg-collision "00000000000000000000000000000001")
                             (:slot-collision "00000000000000000000000000000002")))))))
      (setf (symbol-function 'physical-image-mkdirat) mkdir
            (symbol-function 'physical-image-openat) open
            (symbol-function 'sb-posix:close) close
            (symbol-function 'sb-posix:fsync) sync
            (symbol-function 'sb-posix:read) read
            (symbol-function 'sb-posix:write) write)
      (when (member mode '(:close :body-close))
        (assert (null *logical-install-stage-test-close-fds*))))))

(defun logical-install-stage-test-preparation (mode cut)
  (let ((fds (logical-install-stage-test-fd-count))
        (*logical-install-stage-test-saved* nil)
        (*logical-install-stage-test-injected* nil)
        (*logical-slot-copy-install-write-limit* (when (eq mode :partial) 1)))
    (handler-case
        (progn
          (logical-install-stage-test-run mode cut)
          (unless (member mode '(nil :trace :authority)) (error "nonlocal staging exit")))
      (error (condition)
        (assert (not (member mode '(nil :trace :authority))))
        (assert *logical-install-stage-test-injected* ()
                "staging fault was not injected; original condition: ~a" condition)
        (assert *physical-recovery-failed-p*)
        (assert (= fds (logical-install-stage-test-fd-count)))
        (error "LOGICAL-STAGE-EXPECTED-FAILURE ~s: ~a" mode condition)))
    (assert (not *physical-recovery-failed-p*))
    (assert (= fds (logical-install-stage-test-fd-count)))
    (assert (handler-case
                (progn (apply #'logical-slot-copy-install-check-stage *logical-install-stage-test-saved*) nil)
              (error () t)))
    (format t "~&LOGICAL-INSTALL-STAGED-PASS~%")
    (finish-output)))

(defun logical-install-stage-test-cold (&optional mode cut)
  (assert (and (not *wal-ready*) (null *physical-recovery-state*)
               (null *physical-receive-store*) (null *catalog*) (null *wal-output-stream*)))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (sb-thread:with-mutex (*physical-follow-generation-lock*)
          (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
            (sb-thread:with-mutex (*timeline-history-lock*)
              (logical-install-stage-test-preparation mode cut))))))))

;;;; Native WAL archive selection, publication, and retirement.

(in-package #:pg)

(defun wal-archive-ready-segment-p (segment)
  "Check the durable frontier and exclude the open WAL output."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (and (not *wal-poisoned*)
       (< segment (floor *wal-durable-lsn* +wal-segment-size+))
       (not (and *wal-output-stream* (eql segment *wal-output-segment*)))
       (let ((stat (query-temp-lstat (wal-segment-path segment))))
         (and stat
              (= sb-posix:s-ifreg (logand (sb-posix:stat-mode stat) sb-posix:s-ifmt))
              (<= 1 (sb-posix:stat-size stat) +wal-segment-size+)))))

(declaim (ftype function timeline-history-filename))

(declaim (ftype function timeline-archive-history-items))

(declaim (ftype function timeline-archive-partial-coordinates
                wal-archive-promotion-partial-name wal-archive-partial-items
                wal-archive-partial-matches-p))

(defun wal-archive-partial-item-p (item)
  (and (consp item) (eq (car item) :partial)))

(declaim (ftype function wal-archive-segment-proof-name wal-archive-primary-proof-matches-p))

(defun wal-archive-proof-item-p (item)
  (and (consp item) (eq (car item) :proof)))

(defun wal-archive-item-name (item)
  "Name a selected history, partial, or current timeline segment."
  (cond ((wal-archive-partial-item-p item) (second item))
        ((wal-archive-proof-item-p item)
         (wal-archive-segment-proof-name (wal-archive-item-name (cdr item))))
        ((consp item) (timeline-history-filename (cdr item)))
        (t (wal-segment-filename item *wal-insert-timeline* *wal-segment-namespace*))))

(defun wal-archive-item-ready-p (item)
  "Admit selected committed histories and partials when the primary is ready.
Apply the durable, closed segment checks to WAL segments."
  (cond ((wal-archive-partial-item-p item)
         (and (wal-archive-primary-ready-p)
              (member item (wal-archive-partial-items) :test #'equal)))
        ((wal-archive-proof-item-p item)
         (and (wal-archive-primary-ready-p) (wal-archive-item-ready-p (cdr item))))
        ((consp item)
         (and (wal-archive-primary-ready-p)
              (member item (wal-archive-history-items) :test #'equal)))
        (t (wal-archive-ready-segment-p item))))

(defun wal-archive-create-ready (directory-fd item)
  "Create or validate an empty regular ready marker and sync it.
The caller holds the WAL lock and syncs the status directory."
  (let* ((filename (wal-archive-item-name item))
         (marker (query-temp-fd-child-path directory-fd (concatenate 'string filename ".ready")))
         (flags (logior sb-posix:o-wronly sb-posix:o-creat
                        sb-posix:o-nofollow sb-posix:o-nonblock)))
    (sb-sys:without-interrupts
      (let ((fd (sb-posix:open marker flags #o600)))
        (unwind-protect
             (let ((stat (sb-posix:fstat fd)))
               (unless (and (= sb-posix:s-ifreg
                               (logand (sb-posix:stat-mode stat) sb-posix:s-ifmt))
                            (= 1 (sb-posix:stat-nlink stat))
                            (zerop (sb-posix:stat-size stat)))
                 (error "invalid WAL archive ready marker for ~a" filename))
               (sb-posix:fsync fd))
          (sb-posix:close fd))))))

(defun wal-archive-open-wal-directory ()
  (sb-posix:open (string-right-trim "/" (namestring (wal-directory)))
                 (logior sb-posix:o-rdonly sb-posix:o-directory
                         sb-posix:o-nofollow +postmaster-o-cloexec+)))

(defun wal-archive-open-status-directory (wal-fd)
  (let ((fd (physical-image-openat
             wal-fd "archive_status"
             (logior sb-posix:o-rdonly sb-posix:o-directory
                     sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
    (when (minusp fd) (error "cannot open WAL archive status directory"))
    fd))

(defun wal-archive-open-source (wal-fd name)
  (let ((fd (physical-image-openat
             wal-fd name (logior sb-posix:o-rdonly sb-posix:o-nofollow
                                 sb-posix:o-nonblock +postmaster-o-cloexec+) 0)))
    (when (minusp fd) (error "cannot open WAL archive source: ~a" name))
    fd))

(defun wal-archive-directory-binding-p (held named)
  "Compare directory identity and permissions without comparing mutable timestamps."
  (and (= (first held) (first named))
       (= (second held) (second named))
       (= (third held) (third named))
       (= (fifth held) (fifth named))
       (= (ninth held) (ninth named))))

(defun wal-archive-check-wal-directory (wal-fd)
  (physical-image-check-directory wal-fd nil t)
  (unless (wal-archive-directory-binding-p (physical-image-file-facts wal-fd)
                                           (physical-image-file-facts
                                            wal-fd (string-right-trim "/" (namestring (wal-directory)))))
    (error "WAL directory no longer names the held archive source directory")))

(defun wal-archive-check-source (wal-fd name source-fd)
  "Check that the named regular archive source still matches the held file facts."
  (let ((facts (physical-image-file-facts source-fd)))
    (unless (and (= sb-posix:s-ifreg (logand (third facts) sb-posix:s-ifmt))
                 (= 1 (fourth facts))
                 (= (sb-posix:geteuid) (fifth facts))
                 (<= 1 (sixth facts) +wal-segment-size+)
                 (= (ninth facts) (ninth (physical-image-file-facts wal-fd)))
                 (equal facts (physical-image-file-facts wal-fd name)))
      (error "WAL archive source no longer names the held segment: ~a" name))
    facts))

(declaim (ftype function timeline-read-history-stream))

(defun wal-archive-read-source-bytes (wal-fd name &key sync)
  (let* ((fd (wal-archive-open-source wal-fd name))
         (stream nil))
    (unwind-protect
         (let ((facts (wal-archive-check-source wal-fd name fd)))
           (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8)))
           (let ((bytes (timeline-read-history-stream stream)))
             (when sync
               (sb-posix:fsync fd)
               (sb-posix:fsync wal-fd))
             (unless (equal facts (wal-archive-check-source wal-fd name fd))
               (error "WAL archive source changed during read: ~a" name))
             (values bytes facts)))
      (if stream (close stream) (sb-posix:close fd)))))

(defun wal-archive-history-items (&optional source-name source-facts)
  "Select committed history and eligible ancestor prefixes from the held WAL directory.
When SOURCE-NAME is selected, require its facts to match the held archive source."
  (when (> *wal-insert-timeline* 1)
    (sb-sys:without-interrupts
      (let ((wal-fd (wal-archive-open-wal-directory)))
        (unwind-protect
             (progn
               (wal-archive-check-wal-directory wal-fd)
               (timeline-archive-history-items
                *wal-insert-timeline*
                (lambda (timeline)
                  (multiple-value-bind (bytes facts) (wal-archive-read-source-bytes wal-fd (timeline-history-filename timeline))
                    (when (and source-name (string= source-name (timeline-history-filename timeline))
                               (not (equal source-facts facts)))
                      (error "WAL archive history changed before command: ~a" source-name))
                    bytes))))
          (sb-posix:close wal-fd))))))

(defun wal-archive-partial-items ()
  "Select existing partial files at durable forks in the committed history.
Skip absent files and files with unsuitable type or length."
  (when (> *wal-insert-timeline* 1)
    (sb-sys:without-interrupts
      (let ((wal-fd (wal-archive-open-wal-directory)))
        (unwind-protect
             (progn
               (wal-archive-check-wal-directory wal-fd)
               (loop for (parent child fork) in
                                             (timeline-archive-partial-coordinates
                                              *wal-insert-timeline*
                                              (wal-archive-read-source-bytes
                                               wal-fd (timeline-history-filename *wal-insert-timeline*)))
                     for (segment offset) = (multiple-value-list (wal-segment-and-offset fork))
                     for name = (wal-archive-promotion-partial-name parent fork)
                     for stat = (and name (<= fork *wal-durable-lsn*)
                                     (query-temp-lstat (merge-pathnames name (wal-directory))))
                     when (and stat (sb-posix:s-isreg (sb-posix:stat-mode stat))
                               (= offset (sb-posix:stat-size stat)))
                       collect (list :partial name
                                     (wal-segment-filename segment child :timeline)
                                     offset)))
          (sb-posix:close wal-fd))))))

(defun wal-archive-partial-matches-p (wal-fd item &optional source-facts)
  "Check that the partial matches the child segment prefix and any held source facts.
Return NIL when either file cannot be safely checked."
  (destructuring-bind (kind name child length) item
    (declare (ignore kind))
    (handler-case
        (multiple-value-bind (bytes facts) (wal-archive-read-source-bytes wal-fd name)
          (and (= (length bytes) length)
               (or (null source-facts) (equal source-facts facts))
               (let ((child-bytes (wal-archive-read-source-bytes wal-fd child)))
                 (and (<= length (length child-bytes))
                      (null (mismatch bytes child-bytes :end2 length))))))
      (error () nil))))

(defun wal-archive-check-status-directory (wal-fd status-fd)
  (let ((facts (physical-image-file-facts status-fd)))
    (unless (and (= sb-posix:s-ifdir (logand (third facts) sb-posix:s-ifmt))
                 (zerop (logand (third facts) #o022))
                 (= (sb-posix:geteuid) (fifth facts))
                 (= (ninth facts) (ninth (physical-image-file-facts wal-fd)))
                 (wal-archive-directory-binding-p facts (physical-image-file-facts wal-fd "archive_status")))
      (error "invalid WAL archive status directory"))))

(defun wal-archive-check-private-child (parent-fd name fd)
  (physical-image-check-directory fd)
  (let ((facts (physical-image-file-facts fd)))
    (unless (and (= (ninth facts) (ninth (physical-image-file-facts parent-fd)))
                 (wal-archive-directory-binding-p facts (physical-image-file-facts parent-fd name)))
      (error "invalid standby archive directory: ~a" name))))

(defun call-with-wal-archive-private-child (parent-fd name function &key existing-only)
  (sb-sys:without-interrupts
    (unless (or existing-only (zerop (physical-image-mkdirat parent-fd name #o700))
                (= (sb-alien:get-errno) sb-posix:eexist))
      (error "cannot create standby archive directory: ~a" name))
    (let ((fd (physical-image-openat parent-fd name
                                     (logior sb-posix:o-rdonly sb-posix:o-directory
                                             sb-posix:o-nofollow +postmaster-o-cloexec+) 0)))
      (when (minusp fd) (error "cannot open standby archive directory: ~a" name))
      (unwind-protect
           (progn
             (wal-archive-check-private-child parent-fd name fd)
             (unless existing-only (sb-posix:fsync parent-fd))
             (sb-sys:with-local-interrupts
               (funcall function fd)))
        (sb-posix:close fd)))))

(defun call-with-standby-wal-archive-directory (function)
  "Open the durable staging area and pass its descriptor and binding check."
  (sb-sys:without-interrupts
    (let ((wal-fd (wal-archive-open-wal-directory)))
      (unwind-protect
           (progn
             (wal-archive-check-wal-directory wal-fd)
             (sb-sys:with-local-interrupts
               (call-with-wal-archive-private-child
                wal-fd "standby_archive"
                (lambda (stage-fd)
                  (call-with-wal-archive-private-child stage-fd "archive_status" #'identity)
                  (flet ((check-directory (fd)
                           (wal-archive-check-wal-directory wal-fd)
                           (wal-archive-check-private-child wal-fd "standby_archive" fd)))
                    (check-directory stage-fd)
                    (funcall function stage-fd #'check-directory))))))
        (sb-posix:close wal-fd)))))

(defun wal-archive-run-shell (template name wal-fd directory)
  "Run the archive command and wait for it to exit.
The caller keeps WAL-FD open for the command and holds no WAL lock."
  (let* ((path (format nil "/proc/~d/fd/~d/~a" (sb-posix:getpid) wal-fd name))
         (command (wal-shell-file-command template name path "archive_command"))
         (process (sb-ext:run-program "/bin/sh" (list "-c" command)
                                      :directory directory :wait t :input nil
                                      :output t :error t)))
    (unwind-protect
         (and (eq :exited (sb-ext:process-status process))
              (eql 0 (sb-ext:process-exit-code process)))
      (sb-ext:process-close process))))

(defun wal-archive-done-text (name facts)
  (format nil "WAL-ARCHIVE ~a~{ ~d~}~%" name (subseq facts 0 8)))

(defun wal-archive-done-matches-p (wal-fd status-fd name source-fd &key (sync t))
  "Check the exact completion receipt for the held source.
When SYNC is true, sync the receipt and directory. Recheck both named files."
  (let* ((flags (logior sb-posix:o-rdonly sb-posix:o-nonblock
                        sb-posix:o-nofollow +postmaster-o-cloexec+))
         (marker (concatenate 'string name ".done"))
         (fd (physical-image-openat status-fd marker flags 0))
         (stream nil))
    (when (minusp fd) (return-from wal-archive-done-matches-p nil))
    (unwind-protect
         (let* ((source (wal-archive-check-source wal-fd name source-fd))
                (facts (physical-image-file-facts fd))
                (expected (sb-ext:string-to-octets
                           (wal-archive-done-text name source) :external-format :ascii)))
           (when (and (= sb-posix:s-ifreg (logand (third facts) sb-posix:s-ifmt))
                      (= #o600 (logand (third facts) #o7777))
                      (= 1 (fourth facts))
                      (= (sb-posix:geteuid) (fifth facts))
                      (= (length expected) (sixth facts))
                      (= (ninth facts) (ninth (physical-image-file-facts status-fd))))
             (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8)))
             (let ((bytes (make-array (length expected) :element-type '(unsigned-byte 8))))
               (when (and (= (length bytes) (read-sequence bytes stream))
                          (equalp expected bytes))
                 (when sync
                   (sb-posix:fsync fd)
                   (sb-posix:fsync status-fd))
                 (and (equal facts (physical-image-file-facts status-fd marker))
                      (equal source (wal-archive-check-source wal-fd name source-fd)))))))
      (if stream (close stream) (sb-posix:close fd)))))

(declaim (ftype function wal-archive-staged-segment-done-p))

(defun wal-archive-proof-present-p (wal-fd name)
  (wal-namespace-file-stat
   (merge-pathnames name (physical-image-fd-path wal-fd)) t))

(defun wal-archive-remove-retired-markers (wal-fd status-fd names)
  "Remove status entries after their WAL sources have been durably unlinked."
  (handler-case
      (let ((directory (physical-image-fd-path status-fd))
            (changed nil))
        (wal-archive-check-status-directory wal-fd status-fd)
        (dolist (name names)
          (dolist (suffix '(".ready" ".done"))
            (let ((marker (concatenate 'string name suffix)))
              (handler-case
                  (when (wal-namespace-file-stat (merge-pathnames marker directory) t)
                    (if (zerop (port-unlinkat status-fd marker 0))
                        (setf changed t)
                        (warn "could not remove retired WAL archive marker: ~a" marker)))
                (error (condition)
                  (warn "could not inspect retired WAL archive marker ~a: ~a" marker condition))))))
        (when changed (sb-posix:fsync status-fd)))
    (error (condition)
      (warn "could not clean retired WAL archive markers: ~a" condition)))
  (values))

(defun wal-archive-recycle-segment (segment)
  "Under the WAL lock, unlink a segment only after its completion receipt matches.
Keep the descriptors open through unlink and sync the WAL directory."
  (when (wal-archive-ready-segment-p segment)
    (let* ((name (file-namestring (wal-segment-path segment)))
           (proof-name (wal-archive-segment-proof-name name))
           (proof-fd nil) (proof-facts nil) (wal-fd nil) (status-fd nil) (source-fd nil))
      (unwind-protect
           (when (and (handler-case
                          (progn
                            (setf wal-fd (wal-archive-open-wal-directory)
                                  status-fd (wal-archive-open-status-directory wal-fd)
                                  source-fd (wal-archive-open-source wal-fd name))
                            (wal-archive-check-wal-directory wal-fd)
                            (wal-archive-check-status-directory wal-fd status-fd)
                            (when (wal-archive-proof-present-p wal-fd proof-name)
                              (setf proof-fd (wal-archive-open-source wal-fd proof-name)
                                    proof-facts (wal-archive-check-source wal-fd proof-name proof-fd)))
                            (let ((facts (wal-archive-check-source wal-fd name source-fd)))
                              (when (and (or (not proof-fd)
                                             (and (wal-archive-done-matches-p wal-fd status-fd proof-name proof-fd)
                                                  (wal-archive-primary-proof-matches-p
                                                   wal-fd status-fd segment proof-facts)))
                                         (wal-archive-done-matches-p wal-fd status-fd name source-fd)
                                         (equal facts (wal-archive-check-source wal-fd name source-fd))
                                         (or (not proof-fd)
                                             (equal proof-facts
                                                    (wal-archive-check-source wal-fd proof-name proof-fd))))
                                (wal-archive-check-wal-directory wal-fd)
                                (wal-archive-check-status-directory wal-fd status-fd)
                                t)))
                        (error () nil))
                      (zerop (port-unlinkat wal-fd name 0)))
             (let ((proof-retired
                     (or (not proof-fd)
                         (handler-case
                             (and (equal proof-facts (wal-archive-check-source wal-fd proof-name proof-fd))
                                  (zerop (port-unlinkat wal-fd proof-name 0)))
                           (error () nil)))))
               (unless proof-retired
                 (warn "could not remove retired archive proof: ~a" proof-name))
               (sb-posix:fsync wal-fd)
               (wal-archive-remove-retired-markers
                wal-fd status-fd (if proof-retired (list name proof-name) (list name))))
             t)
        (unwind-protect
             (when proof-fd (sb-posix:close proof-fd))
          (unwind-protect
               (when source-fd (sb-posix:close source-fd))
            (unwind-protect
                 (when status-fd (sb-posix:close status-fd))
              (when wal-fd (sb-posix:close wal-fd)))))))))

(defun wal-archive-publish-done (status-fd name facts)
  "Write and sync the completion file, rename it, then sync the status directory."
  (let ((temporary (merge-pathnames ".archive-done-XXXXXX"
                                    (physical-image-fd-path status-fd)))
        (target (concatenate 'string name ".done"))
        (fd nil) (leaf nil) (stream nil) (renamed nil))
    (unwind-protect
         (progn
           (multiple-value-bind (descriptor path) (open-exclusive-temporary-file temporary)
             (setf fd descriptor leaf (file-namestring path)))
           (setf stream (sb-sys:make-fd-stream fd :output t :element-type 'character))
           (write-string (wal-archive-done-text name facts) stream)
           (finish-output stream)
           (sb-posix:fsync fd)
           (logical-test-failpoint :archive-done-after-file-sync name)
           (unless (equal (physical-image-file-facts fd)
                          (physical-image-file-facts status-fd leaf))
             (error "WAL archive completion temporary changed"))
           (unless (zerop (port-renameat status-fd leaf status-fd target))
             (error "cannot publish WAL archive completion for ~a" name))
           (setf renamed t)
           (logical-test-failpoint :archive-done-after-rename name)
           (sb-posix:fsync status-fd))
      (unwind-protect
           (when (and leaf (not renamed)
                      (ignore-errors (equal (physical-image-file-facts fd)
                                            (physical-image-file-facts status-fd leaf))))
             (port-unlinkat status-fd leaf 0))
        (cond (stream (close stream :abort t))
              (fd (sb-posix:close fd)))))))

(defun wal-archive-primary-ready-p ()
  "Check whether the primary WAL state permits archiver work."
  (and *wal-ready*
       (not *wal-poisoned*)
       (not (recovery-in-progress-p))
       (not *physical-promotion-stopping-p*)
       (member (getf *wal-control-metadata* :db-state)
               (list +db-in-production+ +db-shutdowning+ +db-shutdowned+))))

(declaim (ftype function startup-standby-wal-archive-enabled-p))

(defun wal-archive-staged-history-state (wal-fd name primary-facts)
  "Recognize a matching history already owned by the standby archive queue."
  (handler-case
      (call-with-wal-archive-private-child
       wal-fd "standby_archive"
       (lambda (stage-fd)
         (let ((path (merge-pathnames name (physical-image-fd-path stage-fd))))
           (when (wal-namespace-file-stat path t)
             (sb-sys:without-interrupts
               (let ((source-fd (wal-archive-open-source stage-fd name)))
                 (unwind-protect
                      (multiple-value-bind (bytes facts) (wal-archive-read-source-bytes stage-fd name)
                        (when (and (equal facts (wal-archive-check-source stage-fd name source-fd))
                                   (multiple-value-bind (primary-bytes facts) (wal-archive-read-source-bytes wal-fd name)
                                     (and (equal primary-facts facts) (equalp bytes primary-bytes))))
                          (call-with-wal-archive-private-child
                           stage-fd "archive_status"
                           (lambda (status-fd)
                             (let ((done (wal-archive-done-matches-p stage-fd status-fd name source-fd)))
                               (wal-archive-check-wal-directory wal-fd)
                               (wal-archive-check-private-child wal-fd "standby_archive" stage-fd)
                               (wal-archive-check-private-child stage-fd "archive_status" status-fd)
                               (unless (equal facts (wal-archive-check-source stage-fd name source-fd))
                                 (error "staged history changed during archive handoff"))
                               (if done :done :pending)))
                           :existing-only t)))
                   (sb-posix:close source-fd)))))))
       :existing-only t)
    (error () nil)))

(defun wal-archive-attempt (item template &key epoch)
  "Archive one durable WAL file with the command outside the WAL lock.
Keep the source and directory descriptors open through the command, recheck them
under the lock, and publish completion only after success."
  (when (or (zerop (length template)) (sb-thread:holding-mutex-p *wal-lock*))
    (error "WAL archive attempt requires a command and an unlocked WAL mutex"))
  (let ((directory *pgdata*) (wal-fd nil) (status-fd nil) (source-fd nil)
                             (name nil) (facts nil) (incarnation nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (sb-thread:with-mutex (*wal-lock*)
               (unless (and (startup-primary-wal-archive-enabled-p)
                            (or (null epoch)
                                (and (eq (first epoch) *wal-storage-incarnation*)
                                     (eql (second epoch) *wal-insert-timeline*)
                                     (eq (third epoch) *wal-segment-namespace*)
                                     (wal-archive-primary-ready-p)))
                            (if (consp item)
                                (wal-archive-primary-ready-p)
                                (wal-archive-ready-segment-p item)))
                 (return-from wal-archive-attempt nil))
               (setf incarnation *wal-storage-incarnation*
                     name (wal-archive-item-name item)
                     wal-fd (wal-archive-open-wal-directory)
                     status-fd (wal-archive-open-status-directory wal-fd)
                     source-fd (wal-archive-open-source wal-fd name))
               (wal-archive-check-wal-directory wal-fd)
               (wal-archive-check-status-directory wal-fd status-fd)
               (setf facts (wal-archive-check-source wal-fd name source-fd))
               (cond ((wal-archive-partial-item-p item)
                      (unless (and (wal-archive-item-ready-p item)
                                   (wal-archive-partial-matches-p wal-fd item facts))
                        (return-from wal-archive-attempt nil)))
                     ((wal-archive-proof-item-p item)
                      (unless (and (wal-archive-item-ready-p item)
                                   (wal-archive-primary-proof-matches-p wal-fd status-fd (cdr item) facts))
                        (return-from wal-archive-attempt nil)))
                     ((consp item)
                      (unless (member item (wal-archive-history-items name facts) :test #'equal)
                        (return-from wal-archive-attempt nil)))))
             ;; Reuse the standby receipt only for identical committed history.
             (let ((staged (when (and (consp item) (not (or (wal-archive-partial-item-p item) (wal-archive-proof-item-p item))))
                             (wal-archive-staged-history-state wal-fd name facts))))
               (when (and (eq staged :pending) (startup-standby-wal-archive-enabled-p))
                 (return-from wal-archive-attempt (values nil :staged-pending)))
               (when (or (eq staged :done) (wal-archive-run-shell template name wal-fd directory))
                 (sb-thread:with-mutex (*wal-lock*)
                   (unless (and (or (null epoch) (wal-archive-primary-ready-p))
                                (eq incarnation *wal-storage-incarnation*)
                                (string= name (wal-archive-item-name item))
                                (wal-archive-item-ready-p item)
                                (or (not (wal-archive-partial-item-p item))
                                    (wal-archive-partial-matches-p wal-fd item facts))
                                (or (not (wal-archive-proof-item-p item))
                                    (wal-archive-primary-proof-matches-p wal-fd status-fd (cdr item) facts))
                                (equal facts (wal-archive-check-source wal-fd name source-fd)))
                     (error "WAL archive source changed during command: ~a" name))
                   (wal-archive-check-wal-directory wal-fd)
                   (wal-archive-check-status-directory wal-fd status-fd)
                   (wal-archive-publish-done status-fd name facts))
                 t)))
        (unwind-protect
             (when source-fd (sb-posix:close source-fd))
          (unwind-protect
               (when status-fd (sb-posix:close status-fd))
            (when wal-fd (sb-posix:close wal-fd))))))))

(defun wal-archive-segment-proof-name (name)
  (concatenate 'string name ".proof"))

(declaim (ftype function wal-encode-archive-segment-proof wal-archive-stage-source))

(declaim (ftype function wal-verify-archive-segment-proof))

(defun wal-archive-primary-proof-matches-p (wal-fd status-fd item proof-facts)
  (let* ((name (wal-archive-item-name item))
         (proof-name (wal-archive-segment-proof-name name))
         (source-fd (wal-archive-open-source wal-fd name)))
    (unwind-protect
         (when (wal-archive-done-matches-p wal-fd status-fd name source-fd)
           (multiple-value-bind (bytes facts) (wal-archive-read-source-bytes wal-fd name)
             (multiple-value-bind (proof read-facts) (wal-archive-read-source-bytes wal-fd proof-name)
               (when (and (equal facts (wal-archive-check-source wal-fd name source-fd))
                          (or (null proof-facts) (equal proof-facts read-facts)))
                 (if (wal-archive-partial-item-p item)
                     (let ((stem (subseq name 0 (- (length name) (length ".wal.partial")))))
                       (multiple-value-bind (timeline segment) (wal-parse-segment-name stem :timeline)
                         (and timeline
                              ;; The data receipt binds bytes checked against the committed child.
                              (wal-verify-archive-segment-proof
                               proof (catalog-system-identifier) timeline segment :timeline bytes))))
                     (wal-verify-archive-segment-proof
                      proof (catalog-system-identifier) *wal-insert-timeline* item *wal-segment-namespace* bytes))))))
      (sb-posix:close source-fd))))

(defun wal-record-primary-archive-rotation (segment end)
  "Persist the writer's end before the first record enters the next segment."
  (sb-sys:without-interrupts
    (let ((name (wal-segment-filename segment *wal-insert-timeline* *wal-segment-namespace*))
          (fd (wal-archive-open-wal-directory)))
      (unwind-protect
           (progn
             (wal-archive-check-wal-directory fd)
             (let* ((bytes (wal-archive-read-source-bytes fd name :sync t))
                    (proof (wal-encode-archive-segment-proof
                            (catalog-system-identifier) *wal-insert-timeline* segment
                            *wal-segment-namespace* end bytes))
                    (leaf (wal-archive-segment-proof-name name))
                    (path (merge-pathnames leaf (physical-image-fd-path fd))))
               ;; A crash can reopen and extend this segment before rotation is durable.
               (when (and (wal-namespace-file-stat path t)
                          (not (equalp proof (wal-archive-read-source-bytes fd leaf))))
                 (wal-archive-check-wal-directory fd)
                 (unless (zerop (port-unlinkat fd leaf 0))
                   (error "cannot replace provisional WAL archive proof: ~a" leaf)))
               (wal-archive-stage-source fd leaf proof #'wal-archive-check-wal-directory)))
        (sb-posix:close fd)))))

(defun wal-archive-proof-segment-name (name)
  (when (uiop:string-suffix-p name ".wal.proof")
    (subseq name 0 (- (length name) (length ".proof")))))

(defun wal-archive-staged-segment-done-p (directory-fd status-fd name)
  (let ((source-fd (wal-archive-open-source directory-fd name)))
    (unwind-protect
         (wal-archive-done-matches-p directory-fd status-fd name source-fd)
      (sb-posix:close source-fd))))

(defun wal-archive-attempt-staged (directory-fd name template check-directory)
  "Archive one immutable staged standby file and record success.
The caller holds DIRECTORY-FD and serializes attempts for this staging area.
Return :ARCHIVED for new success, :ALREADY-DONE for a matching receipt, or NIL
when the command fails or its WAL segment has no completion receipt."
  (when (or (zerop (length template)) (sb-thread:holding-mutex-p *wal-lock*))
    (error "staged WAL archive attempt requires a command and an unlocked WAL mutex"))
  (let ((directory *pgdata*)
        (status-fd nil)
        (source-fd nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (funcall check-directory directory-fd)
             (setf status-fd (wal-archive-open-status-directory directory-fd)
                   source-fd (wal-archive-open-source directory-fd name))
             (wal-archive-check-status-directory directory-fd status-fd)
             (let ((facts (wal-archive-check-source directory-fd name source-fd)))
               (flet ((check-directories ()
                        (funcall check-directory directory-fd)
                        (wal-archive-check-status-directory directory-fd status-fd)))
                 (cond
                   ((wal-archive-done-matches-p directory-fd status-fd name source-fd)
                    (check-directories)
                    :already-done)
                   ((let ((segment-name (wal-archive-proof-segment-name name)))
                      (when (or (not segment-name)
                                (wal-archive-staged-segment-done-p directory-fd status-fd segment-name))
                        (sb-posix:fsync directory-fd)
                        (wal-archive-run-shell template name directory-fd directory)))
                    (check-directories)
                    (unless (equal facts (wal-archive-check-source directory-fd name source-fd))
                      (error "staged WAL archive source changed during command: ~a" name))
                    (wal-archive-publish-done status-fd name facts)
                    (check-directories)
                    :archived)))))
        (unwind-protect
             (when source-fd (sb-posix:close source-fd))
          (when status-fd (sb-posix:close status-fd)))))))

(defun wal-archive-reconcile-ready-under-lock ()
  "Create ready markers for completed durable segments under the WAL lock.
Exclude the open output segment and sync the status directory."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (and (startup-primary-wal-archive-enabled-p) (not *wal-poisoned*))
    (let ((segments (remove-if-not #'wal-archive-ready-segment-p (wal-discover-segments))))
      (when segments
        (let ((directory (merge-pathnames "archive_status/" (wal-directory))))
          (wal-ensure-directory-durable directory)
          (sb-sys:without-interrupts
            (let ((fd (or (query-temp-open-directory directory)
                          (error "cannot open the WAL archive status directory"))))
              (unwind-protect
                   (sb-sys:with-local-interrupts
                     (dolist (segment segments)
                       (wal-archive-create-ready fd segment))
                     (sb-posix:fsync fd))
                (sb-posix:close fd))))))))
  (values))

(defun wal-archive-try-reconcile-ready-under-lock ()
  "Report a failed archive notification without giving up WAL retention."
  (handler-case (wal-archive-reconcile-ready-under-lock)
    (error (condition)
      (warn "WAL archive notification failed: ~a" condition)))
  (values))

(declaim (ftype function startup-archive-command))

(defvar *wal-archiver-thread* nil)

(defvar *wal-archiver-stop* nil)

(defun wal-archive-item-pending-p (wal-fd status-fd item)
  "Check for a matching receipt and create a ready marker for pending work.
Skip an optional partial when it cannot be checked or does not match."
  (let ((partial (wal-archive-partial-item-p item))
        (proof (wal-archive-proof-item-p item)))
    (flet ((pending ()
             (let* ((name (wal-archive-item-name item))
                    (fd (wal-archive-open-source wal-fd name)))
               (unwind-protect
                    (and (not (wal-archive-done-matches-p wal-fd status-fd name fd :sync nil))
                         (or (not partial) (wal-archive-partial-matches-p wal-fd item))
                         (or (not proof) (wal-archive-primary-proof-matches-p wal-fd status-fd (cdr item) nil))
                         (progn (wal-archive-create-ready status-fd item) t))
                 (sb-posix:close fd)))))
      (if (or partial proof) (handler-case (pending) (error () nil)) (pending)))))

(defstruct wal-archive-scan epoch items)

(defvar *wal-archive-primary-scan* nil)

(defun wal-archive-reset-scan ()
  (when *wal-archive-primary-scan*
    (setf (wal-archive-scan-items *wal-archive-primary-scan*) nil)))

(defun wal-archive-primary-items (segments)
  (append (wal-archive-history-items)
          (loop for partial in (wal-archive-partial-items)
                collect partial
                collect (cons :proof partial))
          segments))

(defconstant +wal-archive-check-batch-size+ 32)

(defun wal-archive-select-pending-item (scan skip-proofs)
  "Check at most 32 candidates before releasing the WAL lock."
  (wal-ensure-directory-durable (merge-pathnames "archive_status/" (wal-directory)))
  (sb-sys:without-interrupts
    (let ((wal-fd (wal-archive-open-wal-directory))
          (status-fd nil))
      (unwind-protect
           (progn
             (setf status-fd (wal-archive-open-status-directory wal-fd))
             (wal-archive-check-wal-directory wal-fd)
             (wal-archive-check-status-directory wal-fd status-fd)
             (loop repeat +wal-archive-check-batch-size+
                   while (wal-archive-scan-items scan)
                   do (let ((item (pop (wal-archive-scan-items scan))))
                        (when (and (not (and (wal-archive-proof-item-p item)
                                             (member (cdr item) skip-proofs :test #'equal)))
                                   (wal-archive-item-ready-p item)
                                   (wal-archive-item-pending-p wal-fd status-fd item))
                          (sb-posix:fsync status-fd)
                          (return item)))))
        (unwind-protect
             (when status-fd (sb-posix:close status-fd))
          (sb-posix:close wal-fd))))))

(defun wal-archive-primary-epoch ()
  (when (wal-archive-primary-ready-p)
    (list *wal-storage-incarnation* *wal-insert-timeline* *wal-segment-namespace*)))

(defun wal-archive-refresh-primary-scan (scan epoch)
  ;; The directory walk only supplies candidates. Under the WAL lock, check the
  ;; epoch and take the history snapshot before testing candidates for readiness.
  (destructuring-bind (incarnation timeline namespace) epoch
    (declare (ignore incarnation))
    (let ((segments (loop for segment in (wal-discover-segments timeline namespace)
                          collect segment
                          collect (cons :proof segment))))
      (sb-thread:with-mutex (*wal-lock*)
        (when (equal epoch (wal-archive-primary-epoch))
          (setf (wal-archive-scan-epoch scan) epoch
                (wal-archive-scan-items scan) (wal-archive-primary-items segments)))))))

(defun wal-archive-next-primary-item (skip-proofs)
  "Enumerate new candidates outside the WAL lock and check them in batches."
  (let ((scan *wal-archive-primary-scan*)
        (refreshed nil))
    (loop
      (let ((epoch
              (sb-thread:with-mutex (*wal-lock*)
                (let ((epoch (wal-archive-primary-epoch)))
                  (unless epoch (return-from wal-archive-next-primary-item nil))
                  (unless (equal epoch (wal-archive-scan-epoch scan))
                    (wal-archive-reset-scan))
                  (when (wal-archive-scan-items scan)
                    (let ((item (wal-archive-select-pending-item scan skip-proofs)))
                      (when item
                        (return-from wal-archive-next-primary-item (values item epoch)))))
                  epoch))))
        (unless (wal-archive-scan-items scan)
          (when refreshed (return nil))
          (setf refreshed t)
          (wal-archive-refresh-primary-scan scan epoch))))))

(defun wal-archive-staged-name-p (name)
  (let ((segment-name (or (wal-archive-proof-segment-name name) name)))
    (cond ((and (= (length segment-name) 28) (uiop:string-suffix-p segment-name ".wal"))
           (wal-parse-segment-name (subseq segment-name 0 24) :timeline))
          ((and (= (length name) 16) (uiop:string-suffix-p name ".history")
                (every (lambda (char) (find char "0123456789ABCDEF")) (subseq name 0 8)))
           (> (parse-integer name :end 8 :radix 16) 1)))))

(defun wal-archiver-step-staged (template)
  "Archive the earliest pending staged file, retaining failed work for retry."
  (call-with-standby-wal-archive-directory
   (lambda (fd check-directory)
     (loop for name in (sort (directory-entry-names (physical-image-fd-path fd)
                                                    :follow-symlinks t)
                             (lambda (left right)
                               (let ((left-history (uiop:string-suffix-p left ".history"))
                                     (right-history (uiop:string-suffix-p right ".history")))
                                 (if (eq left-history right-history)
                                     (string< left right)
                                     left-history))))
           when (wal-archive-staged-name-p name)
             do (case (wal-archive-attempt-staged fd name template check-directory)
                  (:archived (return t))
                  (:already-done nil)
                  (otherwise
                   (warn "archive_command failed for staged WAL ~a; will retry" name)
                   (unless (wal-archive-proof-segment-name name)
                     (return nil))))))))

(declaim (ftype function startup-standby-wal-archive-enabled-p))

(declaim (ftype function physical-recovery-archive-receive-step))

(defun wal-archiver-step-primary (template)
  (let ((*wal-archive-primary-scan* (or *wal-archive-primary-scan* (make-wal-archive-scan)))
        (failed-proofs nil)
        (keep-scan nil))
    (unwind-protect
         (loop
           (multiple-value-bind (item epoch)
               (wal-archive-next-primary-item failed-proofs)
             (unless item (return nil))
             (if (wal-archive-proof-item-p item)
                 (progn
                   (handler-case
                       (if (wal-archive-attempt item template :epoch epoch)
                           (progn
                             (setf keep-scan (null failed-proofs))
                             (return t))
                           (warn "archive_command failed for WAL proof ~a; will retry" (cdr item)))
                     (error (condition)
                       (warn "could not archive WAL proof ~a: ~a" (cdr item) condition)))
                   (push (cdr item) failed-proofs))
                 (multiple-value-bind (archived status) (wal-archive-attempt item template :epoch epoch)
                   (unless (or archived (eq status :staged-pending))
                     (warn "archive_command failed for WAL item ~a; will retry" item))
                   (setf keep-scan (and archived (null failed-proofs)))
                   (return archived))))
           (unless keep-scan (wal-archive-reset-scan))))))

(defun wal-archiver-step ()
  (let ((template (startup-archive-command)))
    (when (and template (plusp (length template)))
      (let ((standby-archive-p (startup-standby-wal-archive-enabled-p)))
        (when standby-archive-p
          (physical-recovery-archive-receive-step))
        (or (and standby-archive-p
                 (handler-case (wal-archiver-step-staged template)
                   (error (condition)
                     (warn "staged WAL archiver will retry: ~a" condition)
                     nil)))
            (wal-archiver-step-primary template))))))

(defun wal-archiver-try-step ()
  (handler-case (wal-archiver-step)
    (error (condition)
      (warn "WAL archiver will retry: ~a" condition)
      nil)))

(defun wal-archive-retired-segment-name (marker)
  "Return the full segment name for a canonical segment or proof marker."
  (when (or (uiop:string-suffix-p marker ".ready")
            (uiop:string-suffix-p marker ".done"))
    (let* ((name (subseq marker 0 (position #\. marker :from-end t)))
           (segment (or (wal-archive-proof-segment-name name) name))
           (namespace (case (length segment) (20 :legacy) (28 :timeline))))
      (when (and namespace (uiop:string-suffix-p segment ".wal"))
        (multiple-value-bind (timeline number)
            (wal-parse-segment-name (subseq segment 0 (- (length segment) 4)) namespace)
          (when (and timeline (< number (floor *wal-durable-lsn* +wal-segment-size+)))
            segment))))))

(defun wal-archive-entry-absent-p (directory name)
  (handler-case (progn (sb-posix:lstat (merge-pathnames name directory)) nil)
    (sb-posix:syscall-error (condition)
      (if (= (sb-posix:syscall-errno condition) sb-posix:enoent)
          t
          (error condition)))))

(defun wal-archive-retire-orphan-proof (wal-fd status-fd name)
  "Remove an orphan full-segment proof only with a matching completion receipt."
  (handler-case
      (let ((fd (wal-archive-open-source wal-fd name)))
        (unwind-protect
             (and (wal-archive-done-matches-p wal-fd status-fd name fd)
                  (zerop (port-unlinkat wal-fd name 0)))
          (sb-posix:close fd)))
    (error (condition)
      (warn "could not reclaim orphaned archive proof ~a: ~a" name condition)
      nil)))

(defun wal-archive-clean-retired-batch (wal-fd status-fd markers seen)
  (wal-archive-check-wal-directory wal-fd)
  (wal-archive-check-status-directory wal-fd status-fd)
  (let ((directory (physical-image-fd-path wal-fd))
        (retired nil))
    (loop repeat +wal-archive-check-batch-size+
          while markers
          do (let ((name (wal-archive-retired-segment-name (pop markers))))
               (when (and name (not (gethash name seen)))
                 (setf (gethash name seen) t)
                 (let ((proof (wal-archive-segment-proof-name name)))
                   (when (and (wal-archive-entry-absent-p directory name)
                              (or (wal-archive-entry-absent-p directory proof)
                                  (wal-archive-retire-orphan-proof wal-fd status-fd proof)))
                     (push name retired)
                     (push proof retired))))))
    (when retired
      (sb-posix:fsync wal-fd)
      (wal-archive-remove-retired-markers wal-fd status-fd retired)))
  markers)

(defun wal-archive-clean-retired-status ()
  "Recover full-segment status cleanup interrupted after WAL retirement."
  (handler-case
      (sb-sys:without-interrupts
        (let ((wal-fd nil)
              (status-fd nil)
              (epoch nil))
          (unwind-protect
               (progn
                 (sb-thread:with-mutex (*wal-lock*)
                   (setf epoch (wal-archive-primary-epoch))
                   (unless epoch (return-from wal-archive-clean-retired-status nil))
                   (setf wal-fd (wal-archive-open-wal-directory))
                   (wal-archive-check-wal-directory wal-fd)
                   (when (wal-archive-entry-absent-p (physical-image-fd-path wal-fd) "archive_status")
                     (return-from wal-archive-clean-retired-status nil))
                   (setf status-fd (wal-archive-open-status-directory wal-fd))
                   (wal-archive-check-status-directory wal-fd status-fd))
                 (sb-sys:with-local-interrupts
                   (let ((markers (directory-entry-names (physical-image-fd-path status-fd)
                                                         :follow-symlinks t))
                         (seen (make-hash-table :test #'equal)))
                     (loop while markers
                           do (sb-thread:with-mutex (*wal-lock*)
                                (unless (equal epoch (wal-archive-primary-epoch))
                                  (return-from wal-archive-clean-retired-status nil))
                                (setf markers (wal-archive-clean-retired-batch wal-fd status-fd markers seen)))))))
            (unwind-protect
                 (when status-fd (sb-posix:close status-fd))
              (when wal-fd (sb-posix:close wal-fd))))))
    (error (condition)
      (warn "could not recover retired WAL archive status: ~a" condition)))
  (values))

(defun start-wal-archiver ()
  "Start the archiver when enabled. Retry pending work and drain it on shutdown."
  (when (startup-primary-wal-archive-enabled-p)
    (when *wal-archiver-thread* (error "WAL archiver has already started"))
    (let ((stop (sb-thread:make-semaphore))
          (directory *pgdata*))
      (setf *wal-archiver-stop* stop
            *wal-archiver-thread*
            (sb-thread:make-thread
             (lambda ()
               (let ((*pgdata* directory)
                     (*wal-archive-primary-scan* (make-wal-archive-scan)))
                 (wal-archive-clean-retired-status)
                 (loop
                   for archived = (wal-archiver-try-step)
                   until (if archived
                             (sb-thread:try-semaphore stop)
                             (sb-thread:wait-on-semaphore stop :timeout 1)))
                 ;; The shutdown checkpoint may have made another segment ready.
                 (loop while (wal-archiver-try-step))))
             :name "pg WAL archiver"))))
  (values))

(defun stop-wal-archiver ()
  "Signal the archiver and wait for its final drain, with a bounded join."
  (when *wal-archiver-thread*
    (sb-thread:signal-semaphore *wal-archiver-stop*)
    (when (eq :timeout (sb-thread:join-thread *wal-archiver-thread*
                                              :timeout 10 :default :timeout))
      (error "WAL archiver did not stop within 10 seconds"))
    (setf *wal-archiver-thread* nil *wal-archiver-stop* nil))
  (values))

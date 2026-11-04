(in-package #:pg)

;;; Private durable inventory of physical backup lease records.
;;; Publication and cold loading update this index only after durable metadata
;;; operations.  This is a record layer: it does not retain WAL, validate a
;;; READY record against a live pin or origin, run a backup, or release pins.
;;;
;;; Load this index before the primary pin reader and, on both roles, before
;;; listeners.  The authority layer gates streaming, unlink, and name creation
;;; using the loaded records and reconciles them with pins and origins.
(defvar *physical-backup-lease-records* (make-hash-table :test #'equal))

;; Reverse successor assignments for the current lease registry, under WAL
;; exclusion. Each successful record publication invalidates this cache.
(defvar *physical-backup-lease-successor-cache* nil)

;; Active owners are process-local and protected by write and WAL exclusion.
;; The durable record, rather than this registry, reserves a name at restart.
(defvar *physical-backup-lease-capture-owners* (make-hash-table :test #'equal))

(defvar *physical-backup-lease-capture-owner* nil)

(defvar *physical-backup-lease-limit* 65536)
(defvar *physical-backup-lease-test-hook* nil)

(defun physical-backup-lease-test-point (point record)
  (when *physical-backup-lease-test-hook*
    (funcall *physical-backup-lease-test-hook* point record)))

(defun physical-backup-lease-directory ()
  (merge-pathnames "pg_physical_backup_leases/" *pgdata*))

(defun physical-backup-lease-path (request &optional temporary)
  (let ((name (getf (physical-backup-lease-request-copy request) :name)))
    (merge-pathnames (format nil "~a.~a" name (if temporary "tmp" "lease"))
                     (physical-backup-lease-directory))))

(defun physical-backup-lease-lstat (path)
  (handler-case (sb-posix:lstat (string-right-trim "/" (namestring path)))
    (sb-posix:syscall-error (condition)
      (unless (= sb-posix:enoent (sb-posix:syscall-errno condition)) (error condition)))))

(defun physical-backup-lease-check-directory
    (&key create (directory (physical-backup-lease-directory)))
  (let ((stat (physical-backup-lease-lstat directory)))
    (when (and create (null stat))
      (sb-posix:mkdir (namestring directory) #o700)
      (fsync-directory *pgdata*)
      (setf stat (physical-backup-lease-lstat directory)))
    (when stat
      (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                   (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                   (zerop (logand #o077 (sb-posix:stat-mode stat))))
        (error "backup lease inventory is not an owned private directory")))
    (values directory stat)))

(defun physical-backup-lease-file-identity
    (stat &optional allow-empty (maximum +physical-backup-lease-maximum-bytes+))
  (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
               (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
               (= #o600 (logand #o777 (sb-posix:stat-mode stat)))
               (= 1 (sb-posix:stat-nlink stat))
               (<= (if allow-empty 0 1) (sb-posix:stat-size stat) maximum))
    (error "backup lease metadata is not a bounded private single-link regular file"))
  (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
        (sb-posix:stat-mode stat) (sb-posix:stat-uid stat) (sb-posix:stat-nlink stat)
        (sb-posix:stat-size stat) (sb-posix:stat-mtime stat) (sb-posix:stat-ctime stat)))

(defun physical-backup-lease-read (path &key stabilize)
  (physical-backup-lease-check-directory)
  (multiple-value-bind (stream size)
      (open-bounded-regular-file path +physical-backup-lease-maximum-bytes+ stabilize)
    (unwind-protect
         (let* ((before (physical-backup-lease-file-identity
                         (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                (bytes (make-array size :element-type '(unsigned-byte 8))))
           (unless (= size (sixth before))
             (error "backup lease metadata changed before reading"))
           (when stabilize
             (fsync-stream stream)
             (fsync-directory (physical-backup-lease-directory)))
           (unless (and (= size (read-sequence bytes stream))
                        (equal before (physical-backup-lease-file-identity
                                       (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                        (equal before (physical-backup-lease-file-identity
                                       (sb-posix:lstat (namestring path)))))
             (error "backup lease metadata changed while reading"))
           (let ((record (physical-backup-lease-decode bytes)))
             (unless (equal (file-namestring path)
                            (file-namestring (physical-backup-lease-path (getf record :request))))
               (error "backup lease pathname differs from its request"))
             record))
      (close stream))))

(defun physical-backup-lease-require-locks ()
  (unless (and (sb-thread:holding-mutex-p *write-lock*) (sb-thread:holding-mutex-p *wal-lock*))
    (error "backup lease publication requires write and WAL exclusion")))

;; Publish bounded bytes through an exclusive private temporary file. Sync the
;; file before rename, sync the directory and check the published readback.
;; Cleanup removes a temporary only while it still names the created inode.
(defun physical-backup-lease-write-bytes
    (path temporary bytes &key (directory (physical-backup-lease-directory))
                            (maximum +physical-backup-lease-maximum-bytes+)
                            check-readback (test-point (constantly nil)))
  (physical-backup-lease-require-locks)
  (unless (and (functionp check-readback) (functionp test-point))
    (wal-invalid-input "backup lease publication requires valid callbacks"))
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (<= (length bytes) maximum))
    (wal-invalid-input "backup lease publication exceeds its byte bound"))
  (let ((fd nil) (stream nil) (identity nil))
    (unwind-protect
         (progn
           (setf fd (sb-posix:open (namestring temporary)
                                   (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl
                                           sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
           (let ((stat (sb-posix:fstat fd)))
             (setf identity (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)))
             (physical-backup-lease-file-identity stat t maximum))
           (setf stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8))
                 fd nil)
           (funcall test-point :before-write)
           (write-sequence bytes stream)
           (finish-output stream)
           (funcall test-point :after-write)
           (fsync-stream stream)
           (funcall test-point :after-file-fsync)
           (close stream)
           (setf stream nil)
           (sb-posix:rename (namestring temporary) (namestring path))
           (setf identity nil)
           (funcall test-point :after-rename)
           (fsync-directory directory)
           (funcall check-readback)
           (funcall test-point :after-directory-fsync))
      (unwind-protect
           (when stream (close stream))
        (unwind-protect
             (when fd (sb-posix:close fd))
          (when identity
            (let ((stat (physical-backup-lease-lstat temporary)))
              (when stat
                (unless (equal (subseq identity 0 2)
                               (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)))
                  (error "backup lease temporary inode changed"))
                (sb-posix:unlink (namestring temporary)))))))))
  (values))

;; Caller owns write and WAL exclusion and an active publication fence.  This
;; low-level writer checks the locks but does not establish the fence itself.
;; Capture the exclusive temporary inode before validating it, so cleanup can
;; unlink only the inode created here.  Sync, rename, sync the directory, and
;; stabilize and read back the final file before the caller publishes memory.
(defun physical-backup-lease-write (record)
  (physical-backup-lease-require-locks)
  (let* ((request (getf record :request))
         (directory (physical-backup-lease-check-directory :create t))
         (path (physical-backup-lease-path request))
         (temporary (physical-backup-lease-path request t))
         (bytes (physical-backup-lease-image record)))
    (physical-backup-lease-write-bytes
     path temporary bytes :directory directory
     :check-readback
     (lambda ()
       (unless (equal record (physical-backup-lease-read path :stabilize t))
         (error "backup lease readback differs from publication")))
     :test-point (lambda (point) (physical-backup-lease-test-point point record)))
    record))

(defun physical-backup-lease-check-source (request &key current-timeline)
  (unless (and (= (getf request :system-id) (catalog-system-identifier))
               (equal (getf request :cluster-identity) (catalog-cluster-identity *catalog*))
               (or (not current-timeline)
                   (and (not (recovery-in-progress-p))
                        (= (getf request :timeline) *wal-insert-timeline*))))
    (wal-invalid-input "backup lease belongs to a different source context")))

(defun physical-backup-lease-check-capacity (count)
  (unless (and (typep *physical-backup-lease-limit* '(integer 1))
               (<= count *physical-backup-lease-limit*))
    (wal-invalid-input "backup lease inventory is at capacity")))

(declaim (ftype function physical-backup-lease-check-handoff-publication))

(declaim (ftype function physical-backup-lease-allocation-check-unowned-name))

(declaim (ftype function physical-backup-lease-allocation-check-handoff))

;; AFTER must already be the result of a validated transition from BEFORE.
;; The caller holds write and WAL exclusion and supplies the callback for an
;; active publication fence.  Recheck memory and disk under those locks, then
;; begin publication before writing disk and installing the new memory record.
(defun physical-backup-lease-publish-checked-under-lock (before after begin-publication)
  (physical-backup-lease-require-locks)
  (let* ((request (getf after :request)) (name (getf request :name)))
    (physical-backup-lease-check-source
     request :current-timeline (member (getf after :phase) '(:requested :capturing :ready)))
    (when (null before) (physical-backup-lease-allocation-check-unowned-name name))
    (unless (equal before (gethash name *physical-backup-lease-records*))
      (wal-invalid-input "backup lease caller has stale state"))
    (when (and (null before)
               (or (gethash name *physical-source-pins*)
                   (physical-backup-lease-lstat (physical-source-pin-path name))))
      (wal-invalid-input "backup lease name belongs to an existing source pin"))
    (physical-backup-lease-check-directory)
    (let* ((path (physical-backup-lease-path request))
           (present (physical-backup-lease-lstat path)))
      (unless (if before (and present (equal before (physical-backup-lease-read path)))
                  (null present))
        (error "backup lease disk and registered state disagree")))
    (physical-backup-lease-check-capacity
     (+ (hash-table-count *physical-backup-lease-records*) (if before 0 1)))
    (physical-backup-lease-allocation-check-handoff after)
    (physical-backup-lease-check-handoff-publication before after)
    (unless (equal before after)
      (funcall begin-publication)
      (physical-backup-lease-write after)
      (physical-backup-lease-test-point :before-memory after)
      ;; Invalidate before exposing the newly published record to later lookups.
      (setf *physical-backup-lease-successor-cache* nil
            (gethash (copy-seq name) *physical-backup-lease-records*) after))
    (physical-backup-lease-copy after)))

(defun physical-backup-lease-publish (previous candidate)
  (let* ((before (when previous (physical-backup-lease-copy previous)))
         (after (physical-backup-lease-transition before candidate)))
    (with-write-lock
      (with-wal-publication-fence (begin-publication)
        (physical-backup-lease-publish-checked-under-lock before after #'begin-publication)))))

(defun physical-backup-lease-inventory ()
  (multiple-value-bind (directory stat) (physical-backup-lease-check-directory)
    (unless stat
      (fsync-directory *pgdata*)
      (return-from physical-backup-lease-inventory (values nil nil)))
    (let ((records nil) (temporaries nil))
      (dolist (entry (directory-entry-names directory))
        (let ((name (pathname-name entry)) (type (pathname-type entry)))
          (unless (and (physical-backup-lease-name-p name) (member type '("lease" "tmp") :test #'equal)
                       (equal entry (format nil "~a.~a" name type)))
            (error "unexpected backup lease inventory entry"))
          (let ((path (merge-pathnames entry directory)))
            (if (equal type "lease") (push path records)
                (progn
                  (physical-backup-lease-file-identity (sb-posix:lstat (namestring path)) t)
                  (push path temporaries))))))
      (physical-backup-lease-check-capacity (length records))
      (fsync-directory directory)
      (values (sort records #'string< :key #'namestring) temporaries))))

(declaim (ftype function physical-backup-lease-allocation-load-candidates
                physical-backup-lease-allocation-check-graph
                physical-backup-lease-allocation-directory
                physical-backup-lease-allocation-install-records))

;; Validate the entire inventory before changing disk or memory.  Checked private
;; temporary files are discarded and incomplete captures are durably interrupted;
;; source pins are preserved.  A failure leaves the new index unpublished.
(defun physical-backup-lease-load-records ()
  (with-write-lock
    (with-wal-publication-fence (begin-publication)
      (unless (and (zerop (hash-table-count *physical-source-senders*))
                   (zerop (hash-table-count *physical-backup-lease-capture-owners*)))
        (wal-invalid-input "cannot reload backup lease records with active source owners"))
      (multiple-value-bind (paths temporaries) (physical-backup-lease-inventory)
        (let ((candidates (make-hash-table :test #'equal))
              (interrupted nil))
          (dolist (path paths)
            (let* ((record (physical-backup-lease-read path :stabilize t))
                   (request (getf record :request))
                   (name (getf request :name)))
              (physical-backup-lease-check-source request)
              (when (gethash name candidates) (error "duplicate backup lease request"))
              (setf (gethash name candidates) record)))
          ;; Check the original parent and allocation graph before constructing
          ;; interrupted parents. Check again before cleanup or record rewrites.
          (multiple-value-bind (allocations allocation-temporaries)
              (physical-backup-lease-allocation-load-candidates candidates)
            (maphash
             (lambda (name record)
               (when (member (getf record :phase) '(:requested :capturing))
                 (let ((next (copy-list record)))
                   (setf (getf next :phase) :interrupted
                         next (physical-backup-lease-transition record next))
                   (push next interrupted)
                   (setf (gethash name candidates) next))))
             candidates)
            (physical-backup-lease-allocation-check-graph candidates allocations)
            (begin-publication)
            (when (or interrupted temporaries)
              (dolist (path temporaries) (sb-posix:unlink (namestring path)))
              (dolist (record interrupted) (physical-backup-lease-write record))
              (fsync-directory (physical-backup-lease-directory)))
            (when allocation-temporaries
              (dolist (path allocation-temporaries) (sb-posix:unlink (namestring path)))
              (fsync-directory (physical-backup-lease-allocation-directory)))
            (physical-backup-lease-allocation-install-records allocations)
            (setf *physical-backup-lease-successor-cache* nil
                  *physical-backup-lease-records* candidates))))))
  (values))

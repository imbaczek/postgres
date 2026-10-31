;;;; files.lisp -- durable page I/O over SBCL file streams.

(in-package #:pg)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-posix))

(defconstant +postmaster-o-cloexec+ #o2000000
  "Linux bits/fcntl-linux.h O_CLOEXEC, shared by storage and postmaster descriptors.
SB-POSIX does not export this flag in the supported SBCL.")

(sb-alien:define-alien-routine ("mkostemp" port-mkostemp) sb-alien:int
  (template (* (sb-alien:unsigned 8))) (flags sb-alien:int))

(defun open-exclusive-temporary-file (template)
  "Create a private temporary inode from TEMPLATE with close-on-exec set atomically.
The caller must establish cleanup before calling and mask interrupts until the
returned descriptor and path are recorded. Ownership then passes to the caller;
an incomplete return closes the descriptor and removes any known temporary path."
  (let* ((bytes (concatenate '(vector (unsigned-byte 8))
                             (sb-ext:string-to-octets (namestring template) :external-format :utf-8)
                             #(0)))
         (fd nil) (path nil) (complete nil))
    (when (find 0 bytes :end (1- (length bytes))) (error "NUL in temporary file template"))
    (unwind-protect
         (sb-sys:with-pinned-objects (bytes)
           (sb-sys:without-interrupts
             (setf fd (port-mkostemp
                       (sb-alien:sap-alien (sb-sys:vector-sap bytes) (* (sb-alien:unsigned 8)))
                       +postmaster-o-cloexec+))
             (when (minusp fd)
               (let ((errno (sb-alien:get-errno)))
                 (error 'sb-posix:syscall-error :name 'mkostemp :errno errno)))
             (setf path (pathname (sb-ext:octets-to-string
                                   bytes :end (1- (length bytes)) :external-format :utf-8))
                   complete t)
             (values fd path)))
      (unless complete
        (when (and fd (not (minusp fd))) (ignore-errors (sb-posix:close fd)))
        (when path (ignore-errors (sb-posix:unlink (namestring path))))))))

(defun open-bounded-regular-file (path maximum &optional writable)
  "Open PATH as a bounded regular file and return its stream and byte size.
Check the opened descriptor before allocating a buffer. Do not follow a final
symlink or block opening a FIFO. The caller owns and must close the stream."
  (let ((fd (sb-posix:open (namestring path)
                           (logior (if writable sb-posix:o-rdwr sb-posix:o-rdonly)
                                   sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+)))
        (stream nil))
    (unwind-protect
         (let* ((stat (sb-posix:fstat fd)) (size (sb-posix:stat-size stat)))
           (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat)) (<= 0 size maximum))
             (error "invalid bounded regular file: ~a" path))
           (setf stream (sb-sys:make-fd-stream fd :input t :output writable
                                                  :element-type '(unsigned-byte 8)))
           (values stream size))
      (unless stream (sb-posix:close fd)))))

(defun read-bounded-regular-file (path maximum &key (minimum 0) stabilize)
  "Read the descriptor's checked byte count, enforcing MINIMUM and MAXIMUM.
With STABILIZE, open read-write and sync the checked file then its parent before
reading from the same descriptor. The caller must exclude concurrent renames.
Otherwise inspection reads do not sync. Reject short reads and close on every exit;
callers needing a coherent view across publications must compare control images."
  (multiple-value-bind (stream size) (open-bounded-regular-file path maximum stabilize)
    (unwind-protect
         (progn
           (unless (<= minimum size) (error "invalid regular metadata length: ~a" path))
           (when stabilize
             (fsync-stream stream)
             (fsync-directory (uiop:pathname-directory-pathname path)))
           (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
             (unless (= size (read-sequence bytes stream)) (error "short regular file read: ~a" path))
             bytes))
      (close stream))))

(defun open-relation-file (pathname)
  (ensure-directories-exist pathname)
  (open pathname
        :direction :io
        :element-type '(unsigned-byte 8)
        :if-exists :overwrite
        :if-does-not-exist :create))

(defun relation-nblocks (stream)
  (let ((len (file-length stream)))
    (floor len +blcksz+)))

(defun read-page (stream block)
  "Return the page vector for BLOCK, or NIL if the block does not exist."
  (let ((buf (make-array +blcksz+ :element-type '(unsigned-byte 8)
                                  :initial-element 0)))
    (file-position stream (* block +blcksz+))
    (let ((n (read-sequence buf stream)))
      (if (< n +blcksz+)
          nil
          buf))))

(defun write-page (stream block page)
  (file-position stream (* block +blcksz+))
  (write-sequence page stream)
  (finish-output stream)
  (values))

(defun fsync-stream (stream)
  (finish-output stream)
  (sb-posix:fsync (sb-sys:fd-stream-fd stream)))

(defun fsync-directory (dir)
  "Fsync DIR so that a rename inside it survives a crash."
  (let* ((name (namestring dir))
         (trimmed (if (and (> (length name) 1)
                           (char= (char name (1- (length name))) #\/))
                      (subseq name 0 (1- (length name)))
                      name))
         (fd (sb-posix:open trimmed sb-posix:o-rdonly)))
    (unwind-protect
         (sb-posix:fsync fd)
      (sb-posix:close fd))))

(defun directory-entry-names (path &key follow-symlinks)
  "Return the raw entry names in PATH, or NIL if PATH is initially absent.

Do not resolve child names; hidden entries and dangling links are included.
FOLLOW-SYMLINKS permits the directory itself to be a symlink.  An enumeration
error is reported rather than returning a partial inventory."
  (let* ((trimmed (string-right-trim "/" (namestring path)))
         (name (if (zerop (length trimmed)) "/" trimmed))
         (stat (handler-case (funcall (if follow-symlinks #'sb-posix:stat #'sb-posix:lstat) name)
                 (sb-posix:syscall-error (condition)
                   (unless (= (sb-posix:syscall-errno condition) sb-posix:enoent)
                     (error condition))))))
    (when stat
      (unless (= #o040000 (logand #o170000 (sb-posix:stat-mode stat)))
        (error "directory inventory requires an ordinary directory: ~a" path))
      (let ((directory (sb-posix:opendir name)))
        (unwind-protect
             (loop for entry = (progn (sb-alien:set-errno 0) (sb-posix:readdir directory))
                   for errno = (sb-alien:get-errno)
                   for item = (unless (sb-alien:null-alien entry) (sb-posix:dirent-name entry))
                   do (when (and (sb-alien:null-alien entry) (not (zerop errno)))
                        (error "could not read directory ~a: errno ~d" path errno))
                   until (sb-alien:null-alien entry)
                   unless (member item '("." "..") :test #'string=)
                     collect item)
          (sb-posix:closedir directory))))))

(defun durable-state-file-paths (directory type)
  "Sync the inventory directory, including when empty, then return sorted raw
entry names of TYPE as paths. If absent, sync its parent without creating it.
The caller excludes concurrent changes; the reader validates selected entries."
  (let ((entries (directory-entry-names directory)))
    (handler-case (fsync-directory directory)
      (sb-posix:syscall-error (condition)
        (if (and (= (sb-posix:syscall-errno condition) sb-posix:enoent) (null entries))
            (fsync-directory (uiop:pathname-parent-directory-pathname directory))
            (error condition))))
    (loop for name in (sort entries #'string<)
          when (equal (pathname-type name) type)
            collect (merge-pathnames name directory))))

(defvar *logical-test-failpoint-hook* nil)

(defvar *logical-test-failpoint-lock*
  (sb-thread:make-mutex :name "logical replication test failpoint"))

(defvar *logical-test-failpoint-count* 0)

(defvar *logical-test-failpoint-fired-p* nil)

(defun logical-test-failpoint-lsn-value (text)
  (let ((slash (and (stringp text) (position #\/ text))))
    (unless (and slash (plusp slash) (< slash (1- (length text))))
      (error "invalid logical test failpoint LSN ~s" text))
    (+ (ash (parse-integer text :end slash :radix 16) 32)
       (parse-integer text :start (1+ slash) :radix 16))))

(defun logical-test-failpoint-position-after-p (details threshold-text)
  (or (null threshold-text)
      (let ((position (second details)))
        (and (integerp position)
             (> position (logical-test-failpoint-lsn-value threshold-text))))))

(defun logical-test-failpoint-guard-open-p (guard-file)
  (or (null guard-file) (probe-file guard-file)))

(defun logical-test-failpoint-selected-p (point &optional details)
  (let ((selected (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT")))
    (when (and selected
               (string-equal selected (string-downcase (symbol-name point))))
      (let ((object (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_OBJECT")))
        (when (and object (not (equal object (first details))))
          (return-from logical-test-failpoint-selected-p nil)))
      (let ((guard-file
              (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_GUARD_FILE")))
        (unless (logical-test-failpoint-guard-open-p guard-file)
          (return-from logical-test-failpoint-selected-p nil)))
      (let ((after-text
              (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_LSN_AFTER")))
        ;; Boundary and acknowledgement hooks may repeat for keepalives. An
        ;; optional LSN threshold targets the first durable progress beyond a
        ;; known marker instead of counting unrelated replies.
        (unless (logical-test-failpoint-position-after-p details after-text)
          (return-from logical-test-failpoint-selected-p nil))
        (let* ((hit-text
                 (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_HIT"))
               (hit (if hit-text
                        (parse-integer hit-text :junk-allowed nil)
                        1)))
          (unless (plusp hit)
            (error "PG_LISP_TEST_FAILPOINT_HIT must be positive"))
          (sb-thread:with-mutex (*logical-test-failpoint-lock*)
            (unless *logical-test-failpoint-fired-p*
              (incf *logical-test-failpoint-count*)
              (when (= *logical-test-failpoint-count* hit)
                (setf *logical-test-failpoint-fired-p* t)
                t))))))))

(defun logical-test-failpoint-mark-once (path point)
  (ensure-directories-exist path)
  (if (probe-file path)
      nil
      (handler-case
          (with-open-file (stream path
                                  :direction :output
                                  :if-exists :error
                                  :if-does-not-exist :create)
            (format stream "~a ~a~%" point (sb-posix:getpid))
            (finish-output stream)
            (fsync-stream stream)
            (fsync-directory
             (make-pathname :name nil :type nil :defaults path))
            t)
        (file-error (condition)
          (if (probe-file path) nil (error condition))))))

(defun logical-test-failpoint (point &rest details)
  "Run a private, disabled-by-default replication failpoint."
  (when *logical-test-failpoint-hook*
    (apply *logical-test-failpoint-hook* point details))
  (when (logical-test-failpoint-selected-p point details)
    (let* ((action (or (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_ACTION")
                       "error"))
           (once-file
             (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_ONCE_FILE")))
      (when (and (string-equal action "crash") (null once-file))
        (error "crash failpoints require PG_LISP_TEST_FAILPOINT_ONCE_FILE"))
      (when (or (null once-file)
                (logical-test-failpoint-mark-once once-file point))
        (cond
          ((string-equal action "crash")
           (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
           (error "SIGKILL returned at logical failpoint ~a" point))
          ((string-equal action "error")
           (error "injected error at logical failpoint ~a" point))
          ((string-equal action "pause")
           (let ((release-file
                   (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_RELEASE_FILE")))
             (unless release-file
               (error "pause failpoints require PG_LISP_TEST_FAILPOINT_RELEASE_FILE"))
             (loop until (probe-file release-file) do (sleep 0.05))))
          (t
           (error "unknown logical failpoint action ~s" action))))))
  (values))

(defun atomic-replace-slot-failpoint (point target-path)
  (when (equal (pathname-type target-path) "slot")
    (logical-test-failpoint point target-path)))

(defun atomic-replace-file (target-path temporary-path directory write-function
                            &key (element-type 'character) mode
                              (fsync-function #'sb-posix:fsync)
                              (rename-function #'sb-posix:rename)
                              (fsync-directory-function #'fsync-directory))
  "Write and sync TEMPORARY-PATH before atomically replacing TARGET-PATH."
  (ensure-directories-exist target-path)
  (unwind-protect
       (progn
         (with-open-file (stream temporary-path
                                 :direction :output
                                 :if-exists :supersede
                                 :if-does-not-exist :create
                                 :element-type element-type)
           (when mode (sb-posix:fchmod (sb-sys:fd-stream-fd stream) mode))
           (atomic-replace-slot-failpoint :slot-file-before-write target-path)
           (funcall write-function stream)
           (finish-output stream)
           (atomic-replace-slot-failpoint :slot-file-after-write target-path)
           (atomic-replace-slot-failpoint :slot-file-before-fsync target-path)
           (funcall fsync-function (sb-sys:fd-stream-fd stream))
           (atomic-replace-slot-failpoint :slot-file-after-fsync target-path))
         (atomic-replace-slot-failpoint :slot-file-before-rename target-path)
         (funcall rename-function (namestring temporary-path)
                  (namestring target-path))
         (atomic-replace-slot-failpoint :slot-file-after-rename target-path)
         (atomic-replace-slot-failpoint
          :slot-file-before-directory-fsync target-path)
         (funcall fsync-directory-function directory)
         ;; This runs before WAL publishes the candidate restart/confirmed LSN
         ;; in memory, matching ReplicationSlotPersist's file-before-horizon order.
         (atomic-replace-slot-failpoint
          :slot-file-after-directory-fsync-before-memory-publication target-path))
    (ignore-errors (delete-file temporary-path)))
  (values))

(defmacro with-atomic-output ((stream target-path temporary-path directory
                               &rest options)
                              &body body)
  "Write BODY to a temporary file and atomically replace TARGET-PATH."
  `(atomic-replace-file ,target-path ,temporary-path ,directory
                        (lambda (,stream) ,@body)
                        ,@options))

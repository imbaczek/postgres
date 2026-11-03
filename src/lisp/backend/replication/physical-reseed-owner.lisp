;; Stopped-directory ownership for offline physical reseed.
;; This capability preserves the old data and PID evidence; it does not replay
;; WAL, produce a clean endpoint, or authorize an installation.

(in-package #:pg)

(defun physical-reseed-attempt-path (&optional (directory *pgdata*))
  "Return the fixed startup fence path for an interrupted reseed."
  (merge-pathnames "physical-reseed.attempt" directory))

(defun physical-reseed-attempt-temporary-path (&optional (directory *pgdata*))
  (merge-pathnames "physical-reseed.attempt.tmp" directory))

(defun physical-reseed-install-path (&optional (directory *pgdata*))
  (merge-pathnames "physical-reseed.install" directory))

(defun physical-reseed-install-temporary-path (&optional (directory *pgdata*))
  (merge-pathnames "physical-reseed.install.tmp" directory))

;; Plain stopped ownership rejects installation entries but permits its own
;; attempt journal, so STATUS and ABORT can reconcile an interrupted capture.

(defun physical-reseed-check-no-install (&optional (directory *pgdata*))
  (physical-directory-check-reseed-intent directory :install-only t))

;; Check all four reserved names before any PID mutation.  Even a completed
;; abort remains a fence until a later reconciliation protocol resolves it.

(defun physical-reseed-check-before-pid ()
  "Refuse startup on reseed intent or install entries before changing PID metadata."
  (physical-directory-check-reseed-intent *pgdata*))

(defstruct (physical-reseed-owner (:include physical-adoption-owner))
  ;; NIL means absent; otherwise retain the original PID, identity, and bytes.
  old-pid)

(defmethod physical-adoption-check-owner ((owner physical-reseed-owner))
  ;; Storage inheritance does not grant the clean-adoption capability.
  (declare (ignore owner))
  (error "stopped reseed ownership does not authorize clean adoption"))

;; The role check distinguishes ordinary stopped ownership from cleanup
;; ownership.  Operations such as capture, preparation, inventory and clean
;; adoption retain their own authority gates.

(defun physical-reseed-check-owner (owner)
  "Recheck directory ownership and the stopped target's PID evidence."
  (unless (physical-reseed-owner-p owner)
    (error "stopped reseed ownership is required"))
  (physical-directory-check-owner owner))

(defun physical-reseed-process-state (pid &optional (probe #'sb-posix:kill))
  "Return :ALIVE on success, :DEAD for ESRCH, and :UNKNOWN for other syscall errors.
Unlike C stale PID reclamation, do not exempt this process or its ancestors,
or reclaim a PID file on EPERM."
  (unless (typep pid '(integer 1 2147483647))
    (error "invalid process ID for stopped reseed ownership"))
  (handler-case
      (progn (funcall probe pid 0) :alive)
    (sb-posix:syscall-error (condition)
      (if (= sb-posix:esrch (sb-posix:syscall-errno condition)) :dead :unknown))))

;; Capture identity and change-detection fields shared by PID evidence and
;; image copy checks.  These observations do not lock the file against writes.

(defun physical-reseed-file-facts (stat)
  (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
        (sb-posix:stat-mode stat) (sb-posix:stat-nlink stat)
        (sb-posix:stat-uid stat) (sb-posix:stat-size stat)
        (sb-posix:stat-mtime stat) (sb-posix:stat-ctime stat)))

(defun physical-reseed-pid-identity (stat)
  "Select the metadata used to detect changes to the PID file."
  (physical-reseed-file-facts stat))

(defun physical-reseed-read-pid (directory)
  "Capture the PID file's bytes and identity, or NIL when absent.
Reject unsafe metadata and changes across the pathname and descriptor reads.
Normalize a negative single-user PID for probing while preserving its bytes."
  (let ((path (merge-pathnames "postmaster.pid" directory)))
    (when (physical-adoption-path-present-p path)
      (let* ((stat (sb-posix:lstat (namestring path)))
             (before (physical-reseed-pid-identity stat)))
        (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                     (= 1 (sb-posix:stat-nlink stat))
                     (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                     (<= 1 (sb-posix:stat-size stat) 16384))
          (error "reseed PID metadata is not a bounded owned single-link regular file"))
        ;; Defer interrupts until cleanup owns the stream.  Allow them again
        ;; while reading under that cleanup scope.
        (sb-sys:without-interrupts
          (multiple-value-bind (stream size) (open-bounded-regular-file path 16384)
            (unwind-protect
                 (sb-sys:with-local-interrupts
                   (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
                     (flet ((descriptor-identity ()
                              (physical-reseed-pid-identity
                               (sb-posix:fstat (sb-sys:fd-stream-fd stream)))))
                       (unless (and (= size (sixth before)) (equal before (descriptor-identity)))
                         (error "reseed PID metadata changed before reading"))
                       (unless (and (= size (read-sequence bytes stream))
                                    (eq :eof (read-byte stream nil :eof))
                                    (equal before (descriptor-identity))
                                    (equal before (physical-reseed-pid-identity
                                                   (sb-posix:lstat (namestring path)))))
                         (error "reseed PID metadata changed while reading")))
                     (let* ((line (bytes->string (subseq bytes 0 (position 10 bytes))))
                            (pid (ignore-errors (parse-integer line))))
                       (unless (and pid (not (zerop pid)) (<= (abs pid) 2147483647))
                         (error "reseed PID metadata has an invalid process ID"))
                       (list :pid (abs pid) :identity before :bytes bytes))))
              (close stream))))))))

;; Both stopped-owner roles retain the original PID bytes and inode evidence.
;; A recorded process must still be known dead before either role proceeds.

(defun physical-reseed-check-original-pid (owner)
  ;; Recheck both absence and exact contents, then require a dead recorded PID.
  (let* ((old (physical-reseed-owner-old-pid owner))
         (current (physical-reseed-read-pid (physical-adoption-owner-directory owner))))
    (unless (equalp old current)
      (error "reseed target PID metadata changed"))
    (when (and old (not (eq :dead (physical-reseed-process-state (getf old :pid)))))
      (error "reseed target process is live or its state is unknown")))
  owner)

(defmethod physical-adoption-check-owner-role ((owner physical-reseed-owner))
  (physical-reseed-check-no-install (physical-adoption-owner-directory owner))
  (physical-reseed-check-original-pid owner))

;; Check installation fences before reading PID evidence, and again whenever
;; the constructed owner is checked.  Neither check grants install authority.

(defun call-with-physical-reseed-owner (directory function)
  "Call FUNCTION with a thread-bound stopped-directory capability.
Keep the original PID file and dirty data intact; this does not perform reseed."
  (call-with-physical-adoption-directory-owner
   directory function :constructor #'make-physical-reseed-owner
   :initial-arguments (lambda (root)
                        (physical-reseed-check-no-install root)
                        (list :old-pid (physical-reseed-read-pid root)))))

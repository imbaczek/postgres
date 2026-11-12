(in-package #:pg)

(sb-alien:define-alien-routine ("unlinkat" logical-slot-copy-unlinkat) sb-alien:int
  (directory sb-alien:int) (name sb-alien:c-string) (flags sb-alien:int))

(defun logical-slot-copy-write-bytes (context name bytes)
  (let ((*physical-image-context* context))
    (call-with-physical-image-output
     (physical-image-context-root-path context) name
     (lambda (stream) (write-sequence bytes stream))))
  (list name (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes)))

(defun logical-slot-copy-read-file (context name maximum)
  (let* ((*physical-image-context* context)
         (path (pgdata-resolve-path name (physical-image-context-root-path context))))
    (multiple-value-bind (parent leaf) (physical-image-parent context name)
      (with-physical-image-input (stream path :element-type '(unsigned-byte 8))
        (let* ((fd (sb-sys:fd-stream-fd stream))
               (before (physical-image-file-facts fd))
               (size (sixth before)))
          (unless (<= size maximum) (error "logical copy file exceeds its read bound"))
          (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
            (unless (and (= size (read-sequence bytes stream))
                         (eq :eof (read-byte stream nil :eof))
                         (equal before (physical-image-file-facts fd))
                         (equal before (physical-image-file-facts parent leaf)))
              (error "logical copy file changed during readback"))
            (physical-image-check-children context)
            (values bytes before)))))))

(defun logical-slot-copy-open-sidecar (owner context)
  (logical-slot-copy-check-target owner context)
  (let* ((name "logical_slot_wal")
         (root (physical-image-context-root-fd context))
         (fd nil))
    (when (assoc name (physical-image-context-children context) :test #'equal)
      (error "logical copy WAL sidecar is already open"))
    (unless (zerop (physical-image-mkdirat root name #o700))
      (error "cannot create exclusive logical copy WAL sidecar"))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       root name (logior sb-posix:o-rdonly sb-posix:o-directory
                                         sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
             (when (minusp fd) (setf fd nil) (error "cannot open logical copy WAL sidecar"))
             (physical-image-check-directory fd (sb-posix:stat-dev (sb-posix:fstat root)))
             (unless (null (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t))
               (error "new logical copy WAL sidecar is not empty"))
             (push (cons name fd) (physical-image-context-children context))
             (setf fd nil))
        (when fd (sb-posix:close fd))))
    (logical-slot-copy-check-target owner context)))

(defun logical-slot-copy-piece-path (timeline segment)
  (concatenate 'string "logical_slot_wal/" (wal-segment-filename segment timeline :timeline)))

(defun logical-slot-copy-target-inventory (owner context files pieces descriptor-p)
  (logical-slot-copy-check-target owner context)
  (let* ((root (physical-image-context-root-path context))
         (actual (sort (directory-entry-names root :follow-symlinks t) #'string<))
         (expected (append '(".pg_lisp.lock" "base" "global" "pg_wal" "logical_slot_wal"
                             "logical-slot-copy.in-progress" "recovery.required"
                             "physical-recovery.control" "physical-backup.manifest"
                             "pg_hba.conf" "pg_ident.conf" "authentication.initialized")
                           (loop for (name) in files unless (find #\/ name) collect name)
                           (when descriptor-p (list "logical-slot-copy.descriptor")))))
    (unless (equal actual (sort expected #'string<))
      (error "logical copy target root inventory changed"))
    (let* ((sidecar (cdr (assoc "logical_slot_wal" (physical-image-context-children context)
                                :test #'equal)))
           (actual (sort (directory-entry-names (physical-image-fd-path sidecar)
                                                :follow-symlinks t) #'string<))
           (expected (sort (loop for (timeline segment) in pieces
                                 collect (wal-segment-filename segment timeline :timeline)) #'string<)))
      (unless (equal actual expected) (error "logical copy WAL sidecar inventory changed")))
    (let ((*physical-image-context* context))
      (recovery-check-image-directories (list :files files) root))
    t))

(defun logical-slot-copy-retire-marker (owner context copy-id)
  (logical-slot-copy-check-target owner context)
  (let ((bytes (logical-slot-copy-read-file context "logical-slot-copy.in-progress" 1024)))
    (unless (equalp bytes (string->bytes (format nil "PGLOGICALCOPY ~a~%" copy-id)))
      (error "logical copy in-progress identity changed")))
  (let ((fd (physical-image-context-root-fd context)))
    (unless (zerop (logical-slot-copy-unlinkat fd "logical-slot-copy.in-progress" 0))
      (error "cannot retire logical copy in-progress marker"))
    (sb-posix:fsync fd))
  t)

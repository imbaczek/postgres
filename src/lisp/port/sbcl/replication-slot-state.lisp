;;;; Replication slot state file I/O.
;;;;
;;;; The caller holds lifecycle, WAL, or startup exclusion for the whole
;;;; operation, including claim capture, storage, and drop retries. Root and
;;;; slot directories must be owned by the server user and not group- or
;;;; world-writable; state files must be owned by that user and have one link.
;;;; Held descriptors are confined to their callback scopes. This canonical I/O
;;;; does not imply compatibility with C binary state or logical standby import.

(in-package #:pg)

(defconstant +slot-state-maximum-bytes+ 4096)

(defvar *slot-state-root-scope* nil)

(defvar *slot-state-child-scope* nil)

(defun slot-state-valid-name-p (name)
  (and (stringp name) (<= 1 (length name) 63)
       (every (lambda (character)
                (or (char<= #\a character #\z)
                    (char<= #\0 character #\9)
                    (char= character #\_)))
              name)))

(defun slot-state-directory (root name &optional temporary)
  (unless (slot-state-valid-name-p name) (error "invalid replication slot name"))
  (merge-pathnames (format nil "~a~a/" name (if temporary ".tmp" ""))
                   (uiop:ensure-directory-pathname root)))

(defun slot-state-path (root name)
  (merge-pathnames "state" (slot-state-directory root name)))

(defun slot-state-fd-path (fd &optional leaf)
  (merge-pathnames (or leaf "") (physical-image-fd-path fd)))

(defun slot-state-lstat (path)
  (handler-case (sb-posix:lstat (string-right-trim "/" (namestring path)))
    (sb-posix:syscall-error (condition)
      (unless (= (sb-posix:syscall-errno condition) sb-posix:enoent)
        (error condition)))))

(defun slot-state-check-binding (path fd)
  (let ((held (sb-posix:fstat fd)) (named (slot-state-lstat path)))
    (unless (and named (sb-posix:s-isdir (sb-posix:stat-mode named))
                 (= (sb-posix:stat-dev held) (sb-posix:stat-dev named))
                 (= (sb-posix:stat-ino held) (sb-posix:stat-ino named)))
      (error "replication slot directory binding changed"))))

(defun slot-state-check-scope ()
  (unless (and *slot-state-root-scope*
               (eq (first *slot-state-root-scope*) sb-thread:*current-thread*))
    (error "replication slot descriptor scope is not active"))
  (destructuring-bind (owner parent parent-fd root root-fd) *slot-state-root-scope*
    (declare (ignore owner))
    (slot-state-check-binding parent parent-fd)
    (if root-fd
        (slot-state-check-binding root root-fd)
        (when (slot-state-lstat root)
          (error "replication slot root appeared during inspection"))))
  (when *slot-state-child-scope*
    (slot-state-check-binding (car *slot-state-child-scope*)
                              (cdr *slot-state-child-scope*))))

(defun slot-state-check-private-owner (stat)
  (unless (and (= (sb-posix:stat-uid stat) (sb-posix:geteuid))
               (zerop (logand (sb-posix:stat-mode stat) #o022)))
    (error "replication slot storage permits unowned mutation"))
  stat)

(defun slot-state-open-directory (path &optional missing-ok)
  (handler-case
      (let ((fd nil) (complete nil))
        (unwind-protect
             (progn
               (setf fd (sb-posix:open
                         (string-right-trim "/" (namestring path))
                         (logior sb-posix:o-rdonly sb-posix:o-directory
                                 sb-posix:o-nofollow +postmaster-o-cloexec+)))
               (slot-state-check-private-owner (sb-posix:fstat fd))
               (setf complete t)
               fd)
          (when (and fd (not complete)) (sb-posix:close fd))))
    (sb-posix:syscall-error (condition)
      (unless (and missing-ok (= (sb-posix:syscall-errno condition) sb-posix:enoent))
        (error condition)))))

(defmacro with-slot-state-directory ((fd path &optional missing-ok) &body body)
  `(let ((,fd nil))
     (unwind-protect
          (progn
            (sb-sys:without-interrupts
              (setf ,fd (slot-state-open-directory ,path ,missing-ok)))
            ,@body)
       (when ,fd (sb-posix:close ,fd)))))

(defun call-with-slot-state-root (root function &optional missing-ok)
  (let* ((root (uiop:ensure-directory-pathname root))
         (parent (uiop:pathname-parent-directory-pathname root))
         (leaf (car (last (pathname-directory root)))))
    (unless (and (stringp leaf) (not (member leaf '("" "." "..") :test #'equal)))
      (error "invalid replication slot root"))
    (with-slot-state-directory (parent-fd parent)
      (let ((anchored (slot-state-fd-path parent-fd leaf)))
        (with-slot-state-directory (root-fd anchored missing-ok)
          (let ((*slot-state-root-scope*
                  (list sb-thread:*current-thread* parent parent-fd anchored root-fd))
                (*slot-state-child-scope* nil))
            (unless root-fd (fsync-directory (physical-image-fd-path parent-fd)))
            (slot-state-check-scope)
            (multiple-value-prog1 (funcall function root-fd)
              (slot-state-check-scope))))))))

(defun slot-state-point (point root name)
  (slot-state-check-scope)
  (logical-test-failpoint point (slot-state-path root name))
  (slot-state-check-scope))

(defun slot-state-unlink (fd name)
  (unless (zerop (port-unlinkat fd name 0))
    (let ((errno (sb-alien:get-errno)))
      (unless (= errno sb-posix:enoent)
        (error 'sb-posix:syscall-error :name 'unlinkat :errno errno)))))

(sb-alien:define-alien-routine ("renameat2" slot-state-renameat2) sb-alien:int
  (from-fd sb-alien:int) (from sb-alien:c-string)
  (to-fd sb-alien:int) (to sb-alien:c-string) (flags sb-alien:unsigned-int))

(defun slot-state-rename (from-fd from to-fd to &optional no-replace)
  (slot-state-check-scope)
  (unless (zerop (if no-replace
                     (slot-state-renameat2 from-fd from to-fd to 1)
                     (port-renameat from-fd from to-fd to)))
    (error 'sb-posix:syscall-error :name 'renameat :errno (sb-alien:get-errno))))

(defun slot-state-check-bytes (bytes)
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (<= (length bytes) +slot-state-maximum-bytes+))
    (error "invalid replication slot state bytes"))
  bytes)

(defun slot-state-write (fd root name bytes publication-events)
  (slot-state-check-bytes bytes)
  (let ((output-fd nil) (stream nil) (created nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf output-fd
                   (physical-image-openat
                    fd "state.tmp"
                    (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl
                            sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
             (when (minusp output-fd)
               (setf output-fd nil)
               (error 'sb-posix:syscall-error :name 'openat :errno (sb-alien:get-errno)))
             (setf created t
                   stream (sb-sys:make-fd-stream output-fd :output t
                                                           :element-type '(unsigned-byte 8))
                   output-fd nil))
           (slot-state-point :slot-file-before-write root name)
           (write-sequence bytes stream)
           (finish-output stream)
           (slot-state-point :slot-file-after-write root name)
           (slot-state-point :slot-file-before-fsync root name)
           (fsync-stream stream)
           (slot-state-point :slot-file-after-fsync root name)
           (close stream)
           (setf stream nil)
           (slot-state-point (if publication-events :slot-file-before-rename
                                 :slot-state-before-initial-rename) root name)
           (slot-state-rename fd "state.tmp" fd "state")
           (setf created nil)
           (slot-state-point (if publication-events :slot-file-after-rename
                                 :slot-state-after-initial-rename) root name))
      (when stream (ignore-errors (close stream)))
      (when output-fd (ignore-errors (sb-posix:close output-fd)))
      (when created (ignore-errors (slot-state-unlink fd "state.tmp"))))))

(defun slot-state-sync-publication (root-fd child-fd root name)
  (slot-state-point :slot-file-before-directory-fsync root name)
  (fsync-directory (physical-image-fd-path child-fd))
  (fsync-directory (physical-image-fd-path root-fd))
  (slot-state-point :slot-file-after-directory-fsync-before-memory-publication root name))

(defun slot-state-create (root name bytes)
  (slot-state-directory root name)
  (slot-state-check-bytes bytes)
  (call-with-slot-state-root
   root
   (lambda (root-fd)
     (let* ((temporary (concatenate 'string name ".tmp"))
            (final-path (slot-state-fd-path root-fd name))
            (temporary-path (slot-state-fd-path root-fd temporary)))
       (when (or (slot-state-lstat final-path) (slot-state-lstat temporary-path))
         (error "replication slot directory already exists"))
       (unless (zerop (physical-image-mkdirat root-fd temporary #o700))
         (error 'sb-posix:syscall-error :name 'mkdirat :errno (sb-alien:get-errno)))
       (with-slot-state-directory (child-fd temporary-path)
         (let ((*slot-state-child-scope* (cons temporary-path child-fd)))
           (slot-state-write child-fd root name bytes nil)
           (slot-state-point :slot-state-before-staging-sync root name)
           (fsync-directory (physical-image-fd-path child-fd))
           (slot-state-check-binding temporary-path child-fd)
           (slot-state-point :slot-file-before-rename root name)
           (slot-state-rename root-fd temporary root-fd name t)
           (setf (car *slot-state-child-scope*) final-path)
           (slot-state-point :slot-file-after-rename root name)
           (slot-state-check-binding final-path child-fd)
           (slot-state-sync-publication root-fd child-fd root name)
           (slot-state-check-binding final-path child-fd))))))
  (values))

(defun slot-state-update (root name bytes)
  (slot-state-directory root name)
  (slot-state-check-bytes bytes)
  (call-with-slot-state-root
   root
   (lambda (root-fd)
     (let ((path (slot-state-fd-path root-fd name)))
       (with-slot-state-directory (child-fd path)
         (let ((*slot-state-child-scope* (cons path child-fd)))
           (slot-state-check-contents child-fd)
           (slot-state-check-binding path child-fd)
           (slot-state-write child-fd root name bytes t)
           (slot-state-check-binding path child-fd)
           (slot-state-sync-publication root-fd child-fd root name)
           (slot-state-check-binding path child-fd))))))
  (values))

(defun slot-state-read (root name function &key stabilize)
  (slot-state-directory root name)
  (call-with-slot-state-root
   root
   (lambda (root-fd)
     (let ((path (slot-state-fd-path root-fd name)))
       (with-slot-state-directory (child-fd path)
         (let ((*slot-state-child-scope* (cons path child-fd)))
           (slot-state-check-contents child-fd)
           (multiple-value-prog1
               (funcall function
                        (read-bounded-regular-file
                         (slot-state-fd-path child-fd "state") +slot-state-maximum-bytes+
                         :stabilize stabilize)
                        name)
             (slot-state-check-binding path child-fd))))))))

(defun slot-state-directory-entries (fd)
  (sort (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t
                                                           :maximum-entries 65536 :maximum-name-bytes 4456448)
        #'string<))

(defun slot-state-check-contents (fd &optional temporary)
  (let ((entries (slot-state-directory-entries fd)))
    (dolist (entry entries)
      (unless (member entry '("state" "state.tmp") :test #'string=)
        (error "unexpected replication slot directory entry"))
      (let ((stat (slot-state-lstat (slot-state-fd-path fd entry))))
        (when stat (slot-state-check-private-owner stat))
        (unless (and stat (= (sb-posix:stat-nlink stat) 1)
                     (sb-posix:s-isreg (sb-posix:stat-mode stat))
                     (<= (sb-posix:stat-size stat) +slot-state-maximum-bytes+))
          (error "replication slot entry is not bounded regular state"))))
    (unless (or temporary (member "state" entries :test #'string=))
      (error "replication slot directory has no state"))
    entries))

(defun slot-state-inventory-entry (root-fd entry cleanup)
  (let* ((temporary (uiop:string-suffix-p entry ".tmp"))
         (name (if temporary (subseq entry 0 (- (length entry) 4)) entry))
         (path (slot-state-fd-path root-fd entry)))
    (unless (and (slot-state-valid-name-p name) (or cleanup (not temporary)))
      (error "unexpected replication slot root entry"))
    (with-slot-state-directory (child-fd path)
      (let* ((*slot-state-child-scope* (cons path child-fd))
             (contents (slot-state-check-contents child-fd temporary))
             (stat (sb-posix:fstat child-fd)))
        (unless (or cleanup (not (member "state.tmp" contents :test #'string=)))
          (error "unfinished replication slot state update"))
        (slot-state-check-scope)
        (list entry name temporary (sb-posix:stat-dev stat)
              (sb-posix:stat-ino stat) contents)))))

(defun slot-state-check-inventory-binding (item fd)
  (let ((stat (sb-posix:fstat fd)))
    (unless (and (= (fourth item) (sb-posix:stat-dev stat))
                 (= (fifth item) (sb-posix:stat-ino stat))
                 (equal (sixth item)
                        (slot-state-check-contents fd (third item))))
      (error "replication slot inventory changed"))))

(defun slot-state-remove-staging (root-fd item)
  (let* ((entry (first item))
         (path (slot-state-fd-path root-fd entry)))
    (with-slot-state-directory (child-fd path)
      (let ((*slot-state-child-scope* (cons path child-fd)))
        (slot-state-check-inventory-binding item child-fd)
        (slot-state-check-scope)
        (dolist (file (sixth item)) (slot-state-unlink child-fd file))
        (fsync-directory (physical-image-fd-path child-fd))
        (slot-state-check-scope)
        (unless (zerop (port-unlinkat root-fd entry #x200))
          (error 'sb-posix:syscall-error :name 'unlinkat :errno (sb-alien:get-errno)))))
    (slot-state-check-scope)
    (fsync-directory (physical-image-fd-path root-fd))))

(defun slot-state-map (root function &key cleanup)
  (call-with-slot-state-root
   root
   (lambda (root-fd)
     (when root-fd
       (fsync-directory (physical-image-fd-path root-fd))
       (let ((items (mapcar (lambda (entry)
                              (slot-state-inventory-entry root-fd entry cleanup))
                            (slot-state-directory-entries root-fd))))
         (dolist (item items)
           (when (third item) (slot-state-remove-staging root-fd item)))
         (loop for item in items
               unless (third item)
                 collect
                 (let ((path (slot-state-fd-path root-fd (first item))))
                   (with-slot-state-directory (child-fd path)
                     (let ((*slot-state-child-scope* (cons path child-fd)))
                       (slot-state-check-inventory-binding item child-fd)
                       (when (member "state.tmp" (sixth item) :test #'string=)
                         (slot-state-unlink child-fd "state.tmp")
                         (fsync-directory (physical-image-fd-path child-fd)))
                       (multiple-value-prog1
                           (funcall function
                                    (read-bounded-regular-file
                                     (slot-state-fd-path child-fd "state")
                                     +slot-state-maximum-bytes+ :stabilize t)
                                    (second item))
                         (slot-state-check-scope)))))))))
   t))

(defun slot-state-claim-drop (root name)
  (slot-state-directory root name)
  (call-with-slot-state-root
   root
   (lambda (root-fd)
     (when (slot-state-lstat
            (slot-state-fd-path root-fd (concatenate 'string name ".tmp")))
       (error "replication slot staging directory already exists"))
     (let ((path (slot-state-fd-path root-fd name)))
       (with-slot-state-directory (child-fd path)
         (let ((*slot-state-child-scope* (cons path child-fd))
               (root-stat (sb-posix:fstat root-fd))
               (child-stat (sb-posix:fstat child-fd)))
           (slot-state-check-contents child-fd)
           (slot-state-check-scope)
           (list (copy-seq name)
                 (sb-posix:stat-dev root-stat) (sb-posix:stat-ino root-stat)
                 (sb-posix:stat-dev child-stat) (sb-posix:stat-ino child-stat))))))))

(defun slot-state-check-drop-root (root-fd name claim)
  (unless (and (listp claim) (= (length claim) 5)
               (equal (first claim) name)
               (every (lambda (value) (typep value '(integer 0 *))) (rest claim)))
    (error "invalid replication slot drop claim"))
  (let ((stat (sb-posix:fstat root-fd)))
    (unless (and (= (second claim) (sb-posix:stat-dev stat))
                 (= (third claim) (sb-posix:stat-ino stat)))
      (error "replication slot drop root changed"))))

(defun slot-state-retire-claimed (root-fd root name claim final-p)
  (let* ((temporary (concatenate 'string name ".tmp"))
         (entry (if final-p name temporary))
         (path (slot-state-fd-path root-fd entry))
         (temporary-path (slot-state-fd-path root-fd temporary)))
    (with-slot-state-directory (child-fd path)
      (let* ((*slot-state-child-scope* (cons path child-fd))
             (stat (sb-posix:fstat child-fd)))
        (unless (and (= (fourth claim) (sb-posix:stat-dev stat))
                     (= (fifth claim) (sb-posix:stat-ino stat)))
          (error "replication slot drop directory changed"))
        (slot-state-check-contents child-fd (not final-p))
        (when final-p
          (slot-state-point :slot-drop-before-rename root name)
          (slot-state-rename root-fd name root-fd temporary t)
          (setf (car *slot-state-child-scope*) temporary-path)
          (slot-state-point :slot-drop-after-rename root name))
        (slot-state-point :slot-drop-before-directory-fsync root name)
        (fsync-directory (physical-image-fd-path child-fd))
        (fsync-directory (physical-image-fd-path root-fd))
        (slot-state-point :slot-drop-after-directory-fsync root name)
        (list temporary name t (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
              (slot-state-check-contents child-fd t))))))

(defun slot-state-drop (root name claim)
  (slot-state-directory root name)
  (call-with-slot-state-root
   root
   (lambda (root-fd)
     (slot-state-check-drop-root root-fd name claim)
     (let ((final (slot-state-lstat (slot-state-fd-path root-fd name)))
           (temporary (slot-state-lstat
                       (slot-state-fd-path root-fd (concatenate 'string name ".tmp")))))
       (when (and final temporary)
         (error "replication slot drop has ambiguous directories"))
       (if (or final temporary)
           (slot-state-remove-staging
            root-fd (slot-state-retire-claimed root-fd root name claim (not (null final))))
           (fsync-directory (physical-image-fd-path root-fd)))
       (slot-state-point :slot-drop-after-cleanup root name)
       t))))

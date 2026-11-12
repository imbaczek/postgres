(in-package #:pg)

(sb-alien:define-alien-routine ("openat" physical-image-openat) sb-alien:int
  (directory sb-alien:int) (name sb-alien:c-string) (flags sb-alien:int) (mode sb-alien:unsigned-int))

(sb-alien:define-alien-routine ("mkdirat" physical-image-mkdirat) sb-alien:int
  (directory sb-alien:int) (name sb-alien:c-string) (mode sb-alien:unsigned-int))

;; An image scope owns a duplicate of the caller's root descriptor and the
;; cached child descriptors.  The caller retains ownership of its descriptor.
;; The scope and its /proc/self/fd paths are valid only in the owning thread
;; until cleanup closes the descriptors, including on a nonlocal exit.

(sb-alien:define-alien-routine ("statx" physical-image-statx) sb-alien:int
  (directory sb-alien:int) (name sb-alien:c-string) (flags sb-alien:int)
  (mask sb-alien:unsigned-int) (buffer (* sb-alien:unsigned-char)))

(defun physical-image-file-facts (descriptor &optional (name ""))
  (unless (and (typep descriptor '(signed-byte 32)) (stringp name)
               (not (find (code-char 0) name)))
    (error "invalid physical image file observation"))
  (sb-alien:with-alien ((buffer (sb-alien:array sb-alien:unsigned-char 256)))
    (unless (zerop (physical-image-statx
                    descriptor name (if (zerop (length name)) #x1100 #x100) #x17ff
                    (sb-alien:cast (sb-alien:addr buffer) (* sb-alien:unsigned-char))))
      (error "cannot inspect physical image file: errno ~d" (sb-alien:get-errno)))
    (let ((sap (sb-alien:alien-sap (sb-alien:addr buffer))))
      (unless (= #x13cf (logand #x13cf (sb-sys:sap-ref-32 sap 0)))
        (error "physical image file requires complete statx facts"))
      (flet ((timestamp (offset)
               (let ((seconds (sb-sys:signed-sap-ref-64 sap offset))
                     (nanos (sb-sys:sap-ref-32 sap (+ offset 8))))
                 (unless (< nanos 1000000000)
                   (error "invalid physical image statx timestamp"))
                 (+ (* seconds 1000000000) nanos))))
        (let* ((major (sb-sys:sap-ref-32 sap #x88))
               (minor (sb-sys:sap-ref-32 sap #x8c))
               (device (logior (logand minor #xff) (ash (logand major #xfff) 8)
                               (ash (logand minor #xffffff00) 12)
                               (ash (logand major #xfffff000) 32))))
          (list device (sb-sys:sap-ref-64 sap #x20) (sb-sys:sap-ref-16 sap #x1c)
                (sb-sys:sap-ref-32 sap #x10) (sb-sys:sap-ref-32 sap #x14)
                (sb-sys:sap-ref-64 sap #x28) (timestamp #x70) (timestamp #x60)
                (sb-sys:sap-ref-64 sap #x90)))))))

(defstruct (physical-image-context (:constructor %make-physical-image-context))
  root-fd root-path children (owner sb-thread:*current-thread*) (active t))

;; This subtype marks an existing image scope as read-only for these image APIs;
;; it does not make the underlying filesystem immutable.

(defstruct (physical-image-read-context
            (:include physical-image-context)
            (:constructor %make-physical-image-read-context)))

(defvar *physical-image-context* nil)

(defvar *physical-image-test-hook* nil)

(defun physical-image-check-context (context)
  (unless (and (physical-image-context-active context)
               (eq sb-thread:*current-thread* (physical-image-context-owner context)))
    (error "physical image descriptor scope is not active in this thread"))
  context)

(defun physical-image-fd-path (fd)
  (pathname (format nil "/proc/self/fd/~d/" fd)))

(defun physical-image-check-directory (fd &optional device)
  (let ((stat (sb-posix:fstat fd)))
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode stat))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                 (= #o700 (logand #o7777 (sb-posix:stat-mode stat)))
                 (or (null device) (= device (sb-posix:stat-dev stat))))
      (error "physical image directory is not private and owned"))
    stat))

;; Open and retain each private child without following symlinks.  A receive
;; scope creates it exclusively and requires it empty; that does not prove
;; the opened inode is the one created by mkdirat.  A read scope opens existing
;; children.

(defun physical-image-open-child (context name &key existing)
  (physical-image-check-context context)
  (unless (member name '("base" "global" "pg_wal") :test #'equal)
    (error "invalid physical image child directory"))
  (when (and (physical-image-read-context-p context) (not existing))
    (error "physical image read scope cannot create directories"))
  (let ((root (physical-image-context-root-fd context))
        (fd nil))
    (unless existing
      (unless (zerop (physical-image-mkdirat root name #o700))
        (error "cannot create exclusive physical image child directory"))
      (when *physical-image-test-hook* (funcall *physical-image-test-hook* :after-mkdir name)))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       root name
                       (logior sb-posix:o-rdonly sb-posix:o-directory
                               sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
             (when (minusp fd) (setf fd nil) (error "cannot open physical image child directory"))
             (physical-image-check-directory fd (sb-posix:stat-dev (sb-posix:fstat root)))
             (unless (or existing (null (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t)))
               (error "new physical image child directory is not empty"))
             (push (cons name fd) (physical-image-context-children context))
             (setf fd nil))
        (when fd (sb-posix:close fd))))))

(defun physical-image-parent (context name)
  (physical-image-check-context context)
  (unless (and (stringp name) (plusp (length name))
               (not (find (code-char 0) name)))
    (error "invalid physical image relative path"))
  (let ((slash (position #\/ name)))
    (if slash
        (let* ((child (subseq name 0 slash))
               (leaf (subseq name (1+ slash)))
               (fd (cdr (assoc child (physical-image-context-children context) :test #'equal))))
          (unless (and fd (not (find #\/ leaf))
                       (not (member leaf '("" "." "..") :test #'equal)))
            (error "invalid physical image child path"))
          (values fd leaf))
        (progn
          (when (member name '("." "..") :test #'equal) (error "invalid physical image file name"))
          (values (physical-image-context-root-fd context) name)))))

(defun pgdata-resolve-path (relative directory)
  (let ((context *physical-image-context*))
    (if (and context (equal (pathname directory) (physical-image-context-root-path context)))
        (let* ((name (namestring relative))
               (slash (position #\/ name)))
          (if (and slash (= slash (1- (length name))))
              (let ((fd (cdr (assoc (subseq name 0 slash) (physical-image-context-children context)
                                    :test #'equal))))
                (physical-image-check-context context)
                (unless fd (error "invalid physical image directory path"))
                (physical-image-fd-path fd))
              (multiple-value-bind (fd leaf) (physical-image-parent context name)
                (merge-pathnames leaf (physical-image-fd-path fd)))))
        (merge-pathnames relative directory))))

(defun physical-image-check-children (context)
  (physical-image-check-context context)
  (let ((root (physical-image-context-root-fd context)))
    (physical-image-check-directory root)
    (dolist (entry (physical-image-context-children context))
      (let* ((held (physical-image-check-directory (cdr entry)))
             (path (merge-pathnames (car entry) (physical-image-fd-path root)))
             (named (sb-posix:lstat (namestring path))))
        (unless (and (sb-posix:s-isdir (sb-posix:stat-mode named))
                     (= (sb-posix:stat-dev held) (sb-posix:stat-dev named))
                     (= (sb-posix:stat-ino held) (sb-posix:stat-ino named)))
          (error "physical image child directory was replaced"))))))

;; Duplicate the borrowed root and hold the child descriptors for the scope.
;; Receive scopes create children and sync them and the root on success; read
;; scopes open existing children.  Cleanup closes only descriptors owned here.

(defun call-with-physical-image-directory (borrowed-fd function read-only)
  (let ((context (if read-only (%make-physical-image-read-context) (%make-physical-image-context))))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             ;; 1030 is Linux F_DUPFD_CLOEXEC; the duplicate belongs to this scope.
             (setf (physical-image-context-root-fd context) (sb-posix:fcntl borrowed-fd 1030 0))
             (let ((fd (physical-image-context-root-fd context)))
               (physical-image-check-directory fd)
               (setf (physical-image-context-root-path context) (physical-image-fd-path fd))
               (unless (or read-only
                           (null (directory-entry-names (physical-image-context-root-path context)
                                                        :follow-symlinks t)))
                 (error "physical image receive directory is not empty")))
             (sb-sys:with-local-interrupts
               (dolist (name '("base" "global" "pg_wal"))
                 (physical-image-open-child context name :existing read-only))
               (let ((*physical-image-context* context))
                 (multiple-value-prog1
                     (funcall function (physical-image-context-root-path context))
                   (physical-image-check-children context)
                   (unless read-only
                     (dolist (entry (physical-image-context-children context)) (sb-posix:fsync (cdr entry)))
                     (sb-posix:fsync (physical-image-context-root-fd context)))))))
        (setf (physical-image-context-active context) nil)
        (dolist (entry (physical-image-context-children context)) (sb-posix:close (cdr entry)))
        (when (physical-image-context-root-fd context)
          (sb-posix:close (physical-image-context-root-fd context)))))))

(defun call-with-physical-backup-receive-directory (borrowed-fd function)
  (call-with-physical-image-directory borrowed-fd function nil))

(defun call-with-physical-backup-image-directory (borrowed-fd function)
  (call-with-physical-image-directory borrowed-fd function t))

;; Create a private 0600 file exclusively relative to a held directory, without
;; following symlinks.  Sync the file and parent before reporting success.

(defun call-with-physical-image-output (directory name function)
  (let ((context *physical-image-context*))
    (unless (and context (not (physical-image-read-context-p context))
                 (equal (pathname directory) (physical-image-context-root-path context)))
      (error "physical image output requires its descriptor scope"))
    (multiple-value-bind (parent leaf) (physical-image-parent context name)
      (let ((fd nil) (stream nil))
        (sb-sys:without-interrupts
          (unwind-protect
               (progn
                 (setf fd (physical-image-openat
                           parent leaf (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl
                                               sb-posix:o-nofollow +postmaster-o-cloexec+) #o600))
                 (when (minusp fd) (setf fd nil) (error "cannot create exclusive physical image file"))
                 (sb-posix:fchmod fd #o600)
                 (let ((stat (sb-posix:fstat fd)))
                   (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                                (= 1 (sb-posix:stat-nlink stat))
                                (= (sb-posix:geteuid) (sb-posix:stat-uid stat)))
                     (error "invalid physical image output file")))
                 (setf stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8)))
                 (sb-sys:with-local-interrupts
                   (multiple-value-prog1 (funcall function stream)
                     (fsync-stream stream)
                     (sb-posix:fsync parent))))
            (if stream (close stream) (when fd (sb-posix:close fd)))))))))

(defun physical-image-input-parent (context path)
  (physical-image-check-context context)
  (let ((directory (uiop:pathname-directory-pathname path))
        (leaf (file-namestring path)))
    (unless (and (plusp (length leaf)) (not (find (code-char 0) leaf))
                 (not (member leaf '("." "..") :test #'equal)))
      (error "invalid physical image input file"))
    (let ((fd (find directory
                    (cons (physical-image-context-root-fd context)
                          (mapcar #'cdr (physical-image-context-children context)))
                    :key #'physical-image-fd-path :test #'equal)))
      (unless fd (error "physical image input escaped its descriptor scope"))
      (values fd leaf))))

;; Open a private owned single-link regular file relative to the held parent
;; without following symlinks. A permitted missing file calls FUNCTION with NIL.
;; The stream and descriptor belong to this call and close on every exit.
(defun call-with-physical-image-input-at (parent leaf function
                                          &key (element-type 'character)
                                            (if-does-not-exist :error) (external-format :default))
  (unless (and (stringp leaf) (plusp (length leaf))
               (not (find #\/ leaf)) (not (find (code-char 0) leaf))
               (not (member leaf '("." "..") :test #'equal)))
    (error "physical image input requires a single file name"))
  (let ((fd nil) (stream nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       parent leaf (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                           +postmaster-o-cloexec+) 0))
             (when (minusp fd)
               (setf fd nil)
               (if (and (null if-does-not-exist) (= (sb-alien:get-errno) sb-posix:enoent))
                   (return-from call-with-physical-image-input-at (funcall function nil))
                   (error "cannot open physical image input file")))
             (let ((stat (sb-posix:fstat fd)))
               (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                            (= 1 (sb-posix:stat-nlink stat))
                            (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                            (= #o600 (logand #o7777 (sb-posix:stat-mode stat))))
                 (error "physical image input is not a private regular file")))
             (setf stream (sb-sys:make-fd-stream fd :input t :element-type element-type
                                                    :external-format external-format))
             (sb-sys:with-local-interrupts (funcall function stream)))
        (if stream (close stream) (when fd (sb-posix:close fd)))))))

;; Resolve PATH within the active image descriptor scope before opening it.
(defun call-with-physical-image-input (path function
                                       &key (direction :input) (element-type 'character)
                                         (if-does-not-exist :error) (external-format :default))
  (unless (eq direction :input) (error "physical image reader requires input direction"))
  (multiple-value-bind (parent leaf) (physical-image-input-parent *physical-image-context* path)
    (call-with-physical-image-input-at parent leaf function :element-type element-type
                                                            :if-does-not-exist if-does-not-exist :external-format external-format)))

;; Outside an image scope, retain ordinary pathname-based input behavior.

(defmacro with-physical-image-input ((stream path &rest options) &body body)
  `(if *physical-image-context*
       (call-with-physical-image-input ,path (lambda (,stream) ,@body) ,@options)
       (with-open-file (,stream ,path ,@options) ,@body)))

(defun physical-image-stream-length (stream)
  (if *physical-image-context*
      (sb-posix:stat-size (sb-posix:fstat (sb-sys:fd-stream-fd stream)))
      (file-length stream)))

(defun physical-image-check-target (directory fd)
  (let ((held (physical-image-check-directory fd))
        (named (sb-posix:lstat (string-right-trim "/" (namestring directory)))))
    (unless (and (sb-posix:s-isdir (sb-posix:stat-mode named))
                 (= (sb-posix:stat-dev held) (sb-posix:stat-dev named))
                 (= (sb-posix:stat-ino held) (sb-posix:stat-ino named)))
      (error "physical image target path does not name its held directory"))))

;; Compare exact root and child inventories through held descriptors, then open
;; each expected file under the same scope.  This is a snapshot; installation
;; must recheck the image before using it.

(defun physical-image-check-inventory (directory names)
  (let ((context *physical-image-context*))
    (unless (and context (equal (pathname directory) (physical-image-context-root-path context)))
      (error "physical image inventory requires its descriptor scope"))
    (physical-image-check-children context)
    (flet ((check-names (fd expected)
             (unless (equal (sort (copy-list expected) #'string<)
                            (sort (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t)
                                  #'string<))
               (error "physical image directory inventory changed"))))
      (check-names (physical-image-context-root-fd context)
                   (append (mapcar #'car (physical-image-context-children context))
                           (remove-if (lambda (name) (find #\/ name)) names)))
      (dolist (entry (physical-image-context-children context))
        (let ((prefix (concatenate 'string (car entry) "/")))
          (check-names (cdr entry)
                       (loop for name in names
                             when (uiop:string-prefix-p prefix name)
                               collect (subseq name (length prefix)))))))
    (dolist (name names)
      (with-physical-image-input (input (pgdata-resolve-path name directory)
                                        :element-type '(unsigned-byte 8))
        (declare (ignorable input))))
    t))

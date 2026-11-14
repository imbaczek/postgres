;;; Staging namespaces require exclusive control by cooperating local writers.
;;; mkdirat does not return an inode handle. The first captured descriptor is
;;; the baseline for later binding checks, as in physical-image.lisp.
(in-package #:pg)

(defstruct (logical-slot-copy-install-node (:constructor %make-logical-slot-copy-install-node))
  "A held inode and its binding to a parent and name."
  (fd nil :read-only t) (parent nil :read-only t) (name nil :read-only t)
  (kind nil :read-only t) (identity nil :read-only t))

(defvar *logical-slot-copy-install-file-scope* nil)
(defvar *logical-slot-copy-install-file-test-hook* nil)
(defvar *logical-slot-copy-install-write-limit* nil)

(defun logical-slot-copy-install-file-point (point name)
  (when *logical-slot-copy-install-file-test-hook*
    (funcall *logical-slot-copy-install-file-test-hook* point name)))

(defun logical-slot-copy-install-file-identity (facts)
  (list (first facts) (second facts) (ninth facts)))

(defun logical-slot-copy-install-check-node (node)
  "Check inode identity, privacy, and its binding to the held parent."
  (let* ((facts (physical-image-file-facts (logical-slot-copy-install-node-fd node)))
         (kind (logical-slot-copy-install-node-kind node))
         (mode (third facts)) (parent (logical-slot-copy-install-node-parent node)))
    (unless (and (equal (logical-slot-copy-install-node-identity node)
                        (logical-slot-copy-install-file-identity facts))
                 (= (fifth facts) (sb-posix:geteuid))
                 (if (eq kind :file)
                     (and (sb-posix:s-isreg mode) (= 1 (fourth facts))
                          (= #o600 (logand #o7777 mode)))
                     (and (sb-posix:s-isdir mode)
                          (if (eq kind :slot-root) (zerop (logand #o022 mode))
                              (= #o700 (logand #o7777 mode)))))
                 (or (null parent)
                     (equal (logical-slot-copy-install-node-identity node)
                            (logical-slot-copy-install-file-identity
                             (physical-image-file-facts
                              (logical-slot-copy-install-node-fd parent)
                              (logical-slot-copy-install-node-name node))))))
      (error "logical installation staged inode binding or privacy changed"))
    node))

(defun logical-slot-copy-install-check-file-scope ()
  "Recheck the preparation and all inode bindings in the active scope."
  (unless *logical-slot-copy-install-file-scope*
    (error "logical installation file operation requires its preparation scope"))
  (destructuring-bind (owner context attempt resources root) *logical-slot-copy-install-file-scope*
    (logical-slot-copy-install-check-preparation owner context attempt)
    (logical-slot-copy-install-check-node root)
    (dolist (node (car resources)) (logical-slot-copy-install-check-node node))
    resources))

(defun logical-slot-copy-install-check-node-argument (node &optional allow-root)
  "Require the actual registered node, or the borrowed root when allowed."
  (let ((resources (logical-slot-copy-install-check-file-scope)))
    (unless (or (member node (car resources) :test #'eq)
                (and allow-root (eq node (fifth *logical-slot-copy-install-file-scope*))))
      (error "logical installation node is not registered in this file scope"))
    (logical-slot-copy-install-check-node node)))

(defun logical-slot-copy-install-open-node (parent name kind group create exclusive &optional expected)
  "Open and register a child under a held parent, checking its filesystem group.
The first descriptor opened after mkdir is the identity baseline.  This requires
exclusive control of staging namespaces by cooperating local writers."
  (declare (notinline physical-image-mkdirat physical-image-openat sb-posix:fsync))
  (let ((resources (logical-slot-copy-install-check-file-scope))
        (fd nil) (registered nil) (created nil) (node nil))
    (logical-slot-copy-install-check-node-argument parent t)
    (when (eq :file (logical-slot-copy-install-node-kind parent))
      (error "logical installation parent must be a registered directory"))
    (when group (logical-slot-copy-install-check-node-argument group t))
    (unless (and (physical-adoption-install-relative-p name) (not (find #\/ name)))
      (error "invalid logical installation leaf name"))
    (when (and create (not (eq kind :file)))
      (if (zerop (physical-image-mkdirat (logical-slot-copy-install-node-fd parent) name #o700))
          (setf created t)
          (unless (and (not exclusive) (= sb-posix:eexist (sb-alien:get-errno)))
            (error "cannot exclusively create logical installation directory"))))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       (logical-slot-copy-install-node-fd parent) name
                       (logior +postmaster-o-cloexec+ sb-posix:o-nofollow
                               (if (eq kind :file)
                                   (if create (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl)
                                       sb-posix:o-rdonly)
                                   (logior sb-posix:o-rdonly sb-posix:o-directory)))
                       #o600))
             (when (minusp fd) (setf fd nil) (error "cannot open logical installation inode"))
             (setf node (%make-logical-slot-copy-install-node
                         :fd fd :parent parent :name (copy-seq name) :kind kind
                         :identity (logical-slot-copy-install-file-identity (physical-image-file-facts fd))))
             (push node (car resources))
             (setf registered t))
        (when (and fd (not registered)) (sb-posix:close fd))))
    (logical-slot-copy-install-check-node node)
    (when group
      (unless (and (= (first (logical-slot-copy-install-node-identity node))
                      (first (logical-slot-copy-install-node-identity group)))
                   (= (third (logical-slot-copy-install-node-identity node))
                      (third (logical-slot-copy-install-node-identity group))))
        (error "logical installation staging crosses its filesystem group")))
    (when created
      (unless (null (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t
                                                                       :maximum-entries 1))
        (error "new logical installation directory is not empty")))
    (when expected
      (unless (equal expected (logical-slot-copy-install-node-identity node))
        (error "logical installation staged inode differs from its recorded identity")))
    (logical-slot-copy-install-file-point :after-install-inode-open name)
    (logical-slot-copy-install-check-file-scope)
    (when created
      (sb-posix:fsync (logical-slot-copy-install-node-fd parent))
      (logical-slot-copy-install-file-point :after-install-parent-fsync name)
      (logical-slot-copy-install-check-file-scope))
    node))

(defun logical-slot-copy-install-close-node (node)
  "Close an owned node and remove it from the scope; the root is borrowed."
  (declare (notinline sb-posix:close))
  (destructuring-bind (owner context attempt resources root) *logical-slot-copy-install-file-scope*
    (declare (ignore owner context attempt root))
    (unless (member node (car resources) :test #'eq)
      (error "logical installation cannot close a borrowed descriptor"))
    (sb-sys:without-interrupts
      (setf (car resources) (delete node (car resources) :test #'eq))
      (sb-posix:close (logical-slot-copy-install-node-fd node)))))

(defun logical-slot-copy-install-sync-node (node)
  "Sync a registered inode and recheck the scope."
  (declare (notinline sb-posix:fsync))
  (logical-slot-copy-install-check-node-argument node t)
  (sb-posix:fsync (logical-slot-copy-install-node-fd node))
  (logical-slot-copy-install-file-point :after-install-inode-fsync
                                        (logical-slot-copy-install-node-name node))
  (logical-slot-copy-install-check-file-scope))

(defun logical-slot-copy-install-write-node (node bytes)
  "Write bounded bytes to a fresh owned file, then sync it."
  (declare (notinline sb-posix:write))
  (logical-slot-copy-install-check-node-argument node)
  (unless (and (eq :file (logical-slot-copy-install-node-kind node))
               (typep bytes '(simple-array (unsigned-byte 8) (*)))
               (<= (length bytes) +logical-slot-copy-install-maximum-bytes+)
               (zerop (sb-posix:stat-size (sb-posix:fstat (logical-slot-copy-install-node-fd node)))))
    (error "logical installation writes require a fresh bounded file"))
  (let ((fd (logical-slot-copy-install-node-fd node)))
    (unless (zerop (sb-posix:lseek fd 0 sb-posix:seek-set))
      (error "cannot seek logical installation file"))
    (sb-sys:with-pinned-objects (bytes)
      (loop with position = 0 while (< position (length bytes))
            for count = (min 65536 (- (length bytes) position)
                             (or *logical-slot-copy-install-write-limit* 65536))
            do (unless (plusp count) (error "invalid logical installation write bound"))
               (let ((written (sb-posix:write fd (sb-sys:sap+ (sb-sys:vector-sap bytes) position) count)))
                 (unless (plusp written) (error "short logical installation write"))
                 (incf position written))
               (logical-slot-copy-install-file-point :after-install-file-write
                                                     (logical-slot-copy-install-node-name node))
               (logical-slot-copy-install-check-file-scope))))
  (logical-slot-copy-install-sync-node node))

(defun logical-slot-copy-install-read-node (node size)
  "Read exactly SIZE bytes from an owned file and recheck its length."
  (declare (notinline sb-posix:read))
  (unless (and (integerp size) (<= 0 size +logical-slot-copy-install-maximum-bytes+))
    (error "invalid logical installation read bound"))
  (logical-slot-copy-install-check-node-argument node)
  (unless (eq :file (logical-slot-copy-install-node-kind node))
    (error "logical installation reads require an owned file"))
  (let ((fd (logical-slot-copy-install-node-fd node))
        (bytes (make-array size :element-type '(unsigned-byte 8))))
    (unless (and (= size (sb-posix:stat-size (sb-posix:fstat fd)))
                 (zerop (sb-posix:lseek fd 0 sb-posix:seek-set)))
      (error "logical installation file length changed"))
    (sb-sys:with-pinned-objects (bytes)
      (loop with position = 0 while (< position size)
            for count = (sb-posix:read fd (sb-sys:sap+ (sb-sys:vector-sap bytes) position) (- size position))
            do (unless (plusp count) (error "short logical installation read"))
               (incf position count)))
    (logical-slot-copy-install-check-file-scope)
    (unless (= size (sb-posix:stat-size (sb-posix:fstat fd)))
      (error "logical installation file grew while reading"))
    bytes))

(defun call-with-logical-slot-copy-install-file-scope (owner context attempt function)
  "Call FUNCTION with the borrowed root and close every owned descriptor on exit.
Preserve an original failure if closing also fails; the enclosing write scope
fences a failed preparation.  Nodes have authority only within this scope."
  (logical-slot-copy-install-call-with-write
   owner context attempt
   (lambda ()
     (let* ((resources (list nil))
            (root (%make-logical-slot-copy-install-node
                   :fd (physical-image-context-root-fd context) :kind :root
                   :identity (logical-slot-copy-install-file-identity
                              (physical-image-file-facts (physical-image-context-root-fd context)))))
            (*logical-slot-copy-install-file-scope* (list owner context attempt resources root))
            (completed nil) (close-error nil))
       (unwind-protect
            (multiple-value-prog1 (progn (logical-slot-copy-install-check-file-scope)
                                         (funcall function root))
              (logical-slot-copy-install-check-file-scope)
              (setf completed t))
         (dolist (node (copy-list (car resources)))
           (handler-case (logical-slot-copy-install-close-node node)
             (error (condition) (unless close-error (setf close-error condition)))))
         (when close-error
           (if completed (error close-error)
               (ignore-errors (report-query-temp-cleanup-failure "logical installation descriptors" close-error)))))))))

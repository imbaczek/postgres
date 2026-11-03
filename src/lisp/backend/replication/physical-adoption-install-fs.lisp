;;; Filesystem primitives and inventories for offline installation.
;;;
;;; Workspace, incoming and quarantine have private startup fences.  They and
;;; PGDATA and stage must share a filesystem and mount; traversals reject
;;; unsupported or aliased objects.  Mount IDs are checked again at runtime,
;;; not persisted in the plan.  File and directory syncs establish the
;;; durability ordering on which journal recovery relies.

(in-package #:pg)

(defconstant +physical-adoption-at-fdcwd+ -100)
(defconstant +physical-adoption-at-symlink-nofollow+ #x100)

(defconstant +physical-adoption-at-empty-path+ 4096)

(defconstant +physical-adoption-statx-mount+ #x1000)
(defconstant +physical-adoption-rename-noreplace+ 1)

(defparameter *physical-adoption-install-retained-names*
  '(".pg_lisp.lock" "physical-adoption.attempt" "physical-adoption.install"))

(defparameter *physical-adoption-install-guard-text* "offline adoption auxiliary directory")

(defun physical-adoption-install-auxiliary-fact (directory)
  (let* ((fact (physical-adoption-install-directory-fact directory))
         (lock (merge-pathnames ".pg_lisp.lock" directory))
         (guard (physical-adoption-install-path directory))
         (identities nil))
    (dolist (path (list lock guard))
      (let ((stat (sb-posix:lstat (namestring path))))
        (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                     (= #o600 (logand #o7777 (sb-posix:stat-mode stat)))
                     (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                     (= 1 (sb-posix:stat-nlink stat))
                     (= (getf fact :device) (sb-posix:stat-dev stat))
                     (= (getf fact :mount) (physical-adoption-install-mount-id path)))
          (error "adoption auxiliary ownership or fence entry changed"))
        (push (sb-posix:stat-ino stat) identities)))
    (unless (and (zerop (sb-posix:stat-size (sb-posix:lstat (namestring lock))))
                 (equalp (string->bytes *physical-adoption-install-guard-text*)
                         (read-bounded-regular-file guard 128)))
      (error "adoption auxiliary ownership or fence bytes changed"))
    (append fact (list :lock-inode (second identities) :fence-inode (first identities)))))

(defun physical-adoption-install-prepare-auxiliary (directory)
  (physical-adoption-install-directory-fact directory)
  (unless (null (physical-adoption-install-entry-names directory))
    (error "adoption auxiliary directory must start empty"))
  (dolist (name '(".pg_lisp.lock" "physical-adoption.install"))
    (with-open-file (stream (merge-pathnames name directory)
                            :direction :output :if-exists :error :element-type '(unsigned-byte 8))
      (when (equal name "physical-adoption.install")
        (write-sequence (string->bytes *physical-adoption-install-guard-text*) stream))
      (finish-output stream)
      (sb-posix:fchmod (sb-sys:fd-stream-fd stream) #o600)
      (sb-posix:fsync (sb-sys:fd-stream-fd stream))))
  (fsync-directory directory)
  (physical-adoption-install-auxiliary-fact directory))

(defvar *physical-adoption-install-test-hook* nil)

(defun physical-adoption-install-test-point (point path)
  (when *physical-adoption-install-test-hook*
    (funcall *physical-adoption-install-test-hook* point path)))

(sb-alien:define-alien-routine ("statx" physical-adoption-statx) sb-alien:int
  (directory sb-alien:int) (path sb-alien:c-string) (flags sb-alien:int)
  (mask sb-alien:unsigned-int) (buffer (* sb-alien:unsigned-char)))

(sb-alien:define-alien-routine ("renameat2" physical-adoption-renameat2) sb-alien:int
  (from-directory sb-alien:int) (from sb-alien:c-string)
  (to-directory sb-alien:int) (to sb-alien:c-string) (flags sb-alien:unsigned-int))

;; With a descriptor, statx AT_EMPTY_PATH identifies the held directory's
;; mount without resolving its pathname again.  Refuse a missing mount ID.

(defun physical-adoption-install-mount-id (path &key directory-fd)
  (sb-alien:with-alien ((buffer (sb-alien:array sb-alien:unsigned-char 256)))
    (unless (zerop (physical-adoption-statx
                    (or directory-fd +physical-adoption-at-fdcwd+)
                    (if directory-fd "" (string-right-trim "/" (namestring path)))
                    (if directory-fd +physical-adoption-at-empty-path+ +physical-adoption-at-symlink-nofollow+)
                    +physical-adoption-statx-mount+
                    (sb-alien:cast (sb-alien:addr buffer) (* sb-alien:unsigned-char))))
      (error "cannot inspect adoption mount identity: errno ~d" (sb-alien:get-errno)))
    (let ((sap (sb-alien:alien-sap (sb-alien:addr buffer))))
      (unless (logtest +physical-adoption-statx-mount+ (sb-sys:sap-ref-32 sap 0))
        (error "adoption installation requires statx mount identities"))
      (sb-sys:sap-ref-64 sap #x90))))

(defun physical-adoption-install-rename (from to)
  (unless (zerop (physical-adoption-renameat2
                  +physical-adoption-at-fdcwd+ (string-right-trim "/" (namestring from))
                  +physical-adoption-at-fdcwd+ (string-right-trim "/" (namestring to))
                  +physical-adoption-rename-noreplace+))
    (error "adoption no-replace rename failed: errno ~d" (sb-alien:get-errno)))
  (values))

(defun physical-adoption-install-relative-p (name)
  (and (stringp name) (<= 1 (length name) 4096)
       (not (find (code-char 0) name))
       (not (uiop:absolute-pathname-p name))
       (not (handler-case (wild-pathname-p name) (error () t)))
       (every (lambda (part) (and (plusp (length part)) (not (member part '("." "..") :test #'equal))))
              (uiop:split-string name :separator "/"))))

(defun physical-adoption-install-entry-path (root name)
  (unless (physical-adoption-install-relative-p name)
    (error "invalid adoption installation relative path"))
  (merge-pathnames name root))

(defun physical-adoption-install-entry-names (root)
  (let ((directory (sb-posix:opendir (namestring root))) (names nil))
    (unwind-protect
         (loop for entry = (sb-posix:readdir directory)
               until (zerop (sb-sys:sap-int (sb-alien:alien-sap entry)))
               for name = (sb-posix:dirent-name entry)
               unless (member name '("." "..") :test #'equal)
                 do (unless (and (physical-adoption-install-relative-p name) (not (find #\/ name)))
                      (error "unsupported adoption installation entry name"))
                    (push name names))
      (sb-posix:closedir directory))
    (sort names #'string<)))

(defun physical-adoption-install-directory-fact (directory)
  (let* ((requested (uiop:ensure-directory-pathname directory))
         (identity (physical-adoption-stage-directory-identity requested))
         (canonical (truename requested)))
    (unless (equal requested canonical)
      (error "adoption installation directory is not canonical"))
    (list :directory (namestring canonical) :device (first identity) :inode (second identity)
          :mount (physical-adoption-install-mount-id canonical))))

(defun physical-adoption-install-tree (root name device mount &optional (budget (list 100000 16777216)))
  (labels ((visit (relative)
             (when (or (minusp (decf (first budget)))
                       (minusp (decf (second budget) (+ 128 (* 2 (length (string->bytes relative)))))))
               (error "adoption installation inventory exceeds its bound"))
             (let* ((path (physical-adoption-install-entry-path root relative))
                    (stat (sb-posix:lstat (namestring path)))
                    (mode (sb-posix:stat-mode stat))
                    (permissions (logand #o7777 mode))
                    (inode (sb-posix:stat-ino stat)))
               (unless (and (= device (sb-posix:stat-dev stat))
                            (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                            (= mount (physical-adoption-install-mount-id path)))
                 (error "adoption installation entry changed owner, device or mount"))
               (cond
                 ((sb-posix:s-isreg mode)
                  (unless (= 1 (sb-posix:stat-nlink stat))
                    (error "adoption installation refuses hardlinked files"))
                  (let* ((*pgdata* root)
                         (state (physical-adoption-stage-file-state relative)))
                    (unless (and (equal (physical-adoption-stage-stat-identity stat) (fourth state))
                                 (= 1 (sb-posix:stat-nlink (sb-posix:lstat (namestring path)))))
                      (error "adoption installation file changed during inventory"))
                    (list (list relative :file permissions device inode (second state) (third state)))))
                 ((sb-posix:s-isdir mode)
                  (let* ((directory (uiop:ensure-directory-pathname path))
                         (names (physical-adoption-install-entry-names directory))
                         (children (mapcan (lambda (child) (visit (concatenate 'string relative "/" child))) names))
                         (after (sb-posix:lstat (namestring path))))
                    (unless (and (equal names (physical-adoption-install-entry-names directory))
                                 (= inode (sb-posix:stat-ino after))
                                 (= mode (sb-posix:stat-mode after))
                                 (= device (sb-posix:stat-dev after))
                                 (= (sb-posix:geteuid) (sb-posix:stat-uid after))
                                 (= mount (physical-adoption-install-mount-id path)))
                      (error "adoption installation directory changed during inventory"))
                    (cons (list relative :directory permissions device inode 0 0) children)))
                 (t (error "adoption installation refuses symlinks and special files"))))))
    (sort (visit name) #'string< :key #'first)))

(defun physical-adoption-install-snapshot (root &key exclude)
  (unless (and (listp exclude)
               (every (lambda (name) (member name *physical-adoption-install-retained-names* :test #'equal)) exclude))
    (error "adoption inventory can exclude only fixed management entries"))
  (let* ((fact (physical-adoption-install-directory-fact root))
         (names (physical-adoption-install-entry-names root))
         (budget (list 100000 16777216))
         (entries
           (loop for name in names
                 unless (member name exclude :test #'equal)
                   collect (list name (physical-adoption-install-tree
                                       root name (getf fact :device) (getf fact :mount) budget)))))
    (physical-adoption-install-test-point :before-snapshot-recheck root)
    (unless (and (equal fact (physical-adoption-install-directory-fact root))
                 (equal names (physical-adoption-install-entry-names root)))
      (error "adoption inventory root changed during traversal"))
    entries))

(defun physical-adoption-install-sync-ancestors (directory)
  (loop for current = (uiop:ensure-directory-pathname directory) then parent
        for parent = (uiop:pathname-parent-directory-pathname current)
        do (fsync-directory current)
           (physical-adoption-install-test-point :after-directory-name-fsync current)
        while (and parent (not (equal current parent))))
  (values))

(defun physical-adoption-install-row-identity-p (row stat)
  (destructuring-bind (relative kind mode device inode size digest) row
    (declare (ignore relative size digest))
    (and (= device (sb-posix:stat-dev stat))
         (= inode (sb-posix:stat-ino stat))
         (= mode (logand #o7777 (sb-posix:stat-mode stat)))
         (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
         (if (eq kind :directory)
             (sb-posix:s-isdir (sb-posix:stat-mode stat))
             (and (eq kind :file)
                  (sb-posix:s-isreg (sb-posix:stat-mode stat))
                  (= 1 (sb-posix:stat-nlink stat)))))))

(defun physical-adoption-install-sync-tree (root entries)
  (let ((mount (physical-adoption-install-mount-id root)))
    (dolist (entry entries)
      (dolist (row (reverse (second entry)))
        (destructuring-bind (relative kind mode device inode size digest) row
          (declare (ignore mode device inode digest))
          (let* ((path (physical-adoption-install-entry-path root relative))
                 (stat (sb-posix:lstat (namestring path))))
            (unless (and (physical-adoption-install-row-identity-p row stat)
                         (= mount (physical-adoption-install-mount-id path))
                         (or (eq kind :directory)
                             (= size (sb-posix:stat-size stat))))
              (error "adoption object changed before durability preparation"))
            (if (eq kind :directory)
                (fsync-directory (uiop:ensure-directory-pathname path))
                (multiple-value-bind (stream actual-size) (open-bounded-regular-file path size)
                  (unwind-protect
                       (let ((opened (sb-posix:fstat (sb-sys:fd-stream-fd stream))))
                         (unless (and (= size actual-size)
                                      (equal (physical-adoption-stage-stat-identity stat)
                                             (physical-adoption-stage-stat-identity opened)))
                           (error "adoption object changed while opening for sync"))
                         (sb-posix:fsync (sb-sys:fd-stream-fd stream)))
                    (close stream))))
            (physical-adoption-install-test-point :after-snapshot-object-fsync path)))))
    (fsync-directory root))
  (values))

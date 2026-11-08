(in-package #:pg)

;;; Check and prepare the private workspace files for a configuration.
;;; Check names, descriptors and mounts before and after file operations.

;; Map a logical image name to its workspace directory and leaf.  Selection
;; changes which receive control is installed and names the selected history.
(defun physical-reseed-follow-configuration-layout-name (name binding)
  (let ((after (getf binding :after)) (selected (not (equal (getf binding :before) (getf binding :after)))))
    (values
     (cond ((or (equal name "receive.lock") (equal name "receive.selected")
                (equal name "history.receive") (and (equal name "receive.initial") (not selected))) "receive")
           (t "staged"))
     (cond ((or (equal name "receive.selected") (and (equal name "receive.initial") (not selected))) "receive.control")
           ((equal name "history.receive") (physical-store-history-filename (eighth after) (ninth after)))
           (t name)))))

;; Return (logical-name directory-name leaf octets) rows.  The logical image
;; name can differ from the physical directory and leaf used for staging.
(defun physical-reseed-follow-configuration-layout (images binding)
  (loop for (name . bytes) in (sort (copy-list images) #'string< :key #'car)
        collect (multiple-value-bind (directory leaf) (physical-reseed-follow-configuration-layout-name name binding)
                  (list name directory leaf bytes))))

;; Read and verify one planned file, returning its checked identity and digest.
;; The named file and mount must still match the held descriptor after reading.
(defun physical-reseed-follow-configuration-file-row (parent leaf logical bytes)
  ;; Return the checked file row and, as a second value, fresh octets read
  ;; from the file after validating its bytes and identity.
  (call-with-physical-image-input-at
   parent leaf
   (lambda (stream)
     (let* ((fd (sb-sys:fd-stream-fd stream))
            (before (physical-reseed-file-facts (sb-posix:fstat fd)))
            (mount (physical-adoption-install-mount-id nil :directory-fd parent))
            (actual (make-array (length bytes) :element-type '(unsigned-byte 8))))
       (unless (and (= (first before) (sb-posix:stat-dev (sb-posix:fstat parent)))
                    (= mount (physical-adoption-install-mount-id nil :directory-fd fd))
                    (= mount (physical-adoption-install-mount-id
                              (merge-pathnames leaf (physical-image-fd-path parent))))
                    (= (length bytes) (sixth before))
                    (= (length bytes) (read-sequence actual stream))
                    (eq :eof (read-byte stream nil :eof))
                    (equalp bytes actual)
                    (equal before (physical-reseed-file-facts (sb-posix:fstat fd)))
                    (equal before (physical-reseed-file-facts
                                   (sb-posix:lstat (namestring (merge-pathnames leaf (physical-image-fd-path parent))))))
                    (= mount (physical-adoption-install-mount-id
                              (merge-pathnames leaf (physical-image-fd-path parent)))))
         (error "configuration file differs from its derived image or identity"))
       (values (list logical :file #o600 (first before) (second before) (length bytes)
                     (physical-store-update-digest #xCBF29CE484222325 actual))
               actual)))
   :element-type '(unsigned-byte 8)))

(defun physical-reseed-follow-configuration-output-check (parent fd path fixed)
  (let ((actual (physical-reseed-follow-file-identity (sb-posix:fstat fd) t)))
    (unless (and (equal fixed (subseq actual 0 5))
                 (equal actual (physical-reseed-follow-file-identity (sb-posix:lstat (namestring path)) t))
                 (= (first actual) (sb-posix:stat-dev (sb-posix:fstat parent)))
                 (= (physical-adoption-install-mount-id nil :directory-fd parent)
                    (physical-adoption-install-mount-id nil :directory-fd fd)
                    (physical-adoption-install-mount-id path)))
      (error "configuration output descriptor lost its identity or name"))
    actual))

;; Create only a planned leaf when PREPARE permits it; otherwise require the
;; existing complete file to match its bytes.  Torn files remain fenced.
(defun physical-reseed-follow-configuration-write-file (owner parent leaf logical bytes check &key prepare)
  (let ((path (merge-pathnames leaf (physical-image-fd-path parent))) (fd nil) (fixed nil))
    (funcall check)
    (if (physical-adoption-path-present-p path)
        (physical-reseed-follow-configuration-file-row parent leaf logical bytes)
        (unwind-protect
             (progn
               (unless prepare (error "staged configuration file is absent"))
               (physical-reseed-follow-test-point :before-configuration-file-create path)
               (funcall check)
               (setf (physical-reseed-follow-owner-uncertain-p owner) t)
               (sb-sys:without-interrupts
                 (setf fd (physical-image-openat
                           parent leaf (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                                               sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) #o600)))
               (when (minusp fd) (setf fd nil) (error "cannot create exclusive configuration file"))
               (sb-posix:fchmod fd #o600)
               (setf fixed (subseq (physical-reseed-follow-file-identity (sb-posix:fstat fd) t) 0 5))
               (flet ((held ()
                        (funcall check)
                        (physical-reseed-follow-configuration-output-check parent fd path fixed)))
                 (held)
                 (physical-reseed-follow-test-point :after-configuration-file-create path)
                 (held)
                 (let ((*physical-adoption-install-test-hook*
                         (lambda (event where)
                           (held)
                           (physical-reseed-follow-test-point event where)
                           (held))))
                   (physical-adoption-install-write-bytes
                    fd bytes 0 path :limit *physical-reseed-follow-write-limit* :point :after-configuration-write-chunk))
                 (held)
                 (sb-posix:fsync fd)
                 (physical-reseed-follow-test-point :after-configuration-file-fsync path)
                 (held)
                 (sb-posix:fsync parent)
                 (physical-reseed-follow-configuration-file-row parent leaf logical bytes)))
          (when fd (sb-posix:close fd))))))

(defun physical-reseed-follow-configuration-child-fact (parent name fd)
  (let* ((path (merge-pathnames (concatenate 'string name "/") (physical-image-fd-path parent)))
         (stat (physical-image-check-directory fd (sb-posix:stat-dev (sb-posix:fstat parent))))
         (mount (physical-adoption-install-mount-id nil :directory-fd parent)))
    (physical-image-check-target path fd)
    (unless (and (= mount (physical-adoption-install-mount-id nil :directory-fd fd))
                 (= mount (physical-adoption-install-mount-id path)))
      (error "configuration child directory changed its mount"))
    (list :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat))))

;; Hold the staged, saved and receive directories and recheck their names and
;; mounts around FUNCTION.  PREPARE permits creation only during building.
(defun call-with-physical-reseed-follow-configuration-children (owner parent function check &key prepare)
  (let ((children nil) (facts nil) (active nil))
    (unwind-protect
         (progn
           (dolist (name '("staged" "saved" "receive"))
             (funcall check)
             (let ((path (merge-pathnames name (physical-image-fd-path parent))) (fd nil))
               (unless (physical-adoption-path-present-p path)
                 (unless prepare (error "configuration child is absent"))
                 (setf (physical-reseed-follow-owner-uncertain-p owner) t)
                 (unless (zerop (physical-image-mkdirat parent name #o700))
                   (error "cannot create exclusive configuration child"))
                 (physical-reseed-follow-test-point :after-configuration-directory-create path)
                 (funcall check))
               (unwind-protect
                    (progn
                      (sb-sys:without-interrupts
                        (setf fd (physical-image-openat parent name
                                                        (logior sb-posix:o-rdonly sb-posix:o-directory sb-posix:o-nofollow
                                                                +postmaster-o-cloexec+) 0)))
                      (when (minusp fd) (setf fd nil) (error "cannot open configuration child"))
                      (push (cons name (physical-reseed-follow-configuration-child-fact parent name fd)) facts)
                      (push (cons name fd) children)
                      (setf fd nil))
                 (when fd (sb-posix:close fd)))))
           (setf active t)
           (flet ((verify ()
                    (unless active (error "configuration children scope has ended"))
                    (funcall check)
                    (dolist (entry children)
                      (unless (equal (cdr (assoc (car entry) facts :test #'equal))
                                     (physical-reseed-follow-configuration-child-fact parent (car entry) (cdr entry)))
                        (error "configuration child identity changed")))))
             (verify)
             (multiple-value-prog1 (funcall function children facts #'verify)
               (verify))))
      (setf active nil)
      (dolist (entry children) (sb-posix:close (cdr entry))))))

;; Accept only the ordered workspace and output names in LAYOUT.  The caller
;; separately checks the contents and identities of each existing file.
(defun physical-reseed-follow-configuration-prefix (parent layout)
  (let* ((order '(".pg_lisp.lock" "physical-reseed.install" "staged" "saved" "receive"))
         (names (sort (directory-entry-names (physical-image-fd-path parent) :follow-symlinks t :maximum-entries 6)
                      #'string<))
         (prefix (loop for n from 0 to 5
                       when (equal names (sort (subseq order 0 n) #'string<)) return n))
         (present (make-hash-table :test #'equal)))
    (unless prefix (error "configuration workspace has an unexplained root prefix"))
    (dolist (name '("staged" "saved" "receive"))
      (when (member name names :test #'equal)
        (let ((fd nil))
          (unwind-protect
               (progn
                 (sb-sys:without-interrupts
                   (setf fd (physical-image-openat parent name
                                                   (logior sb-posix:o-rdonly sb-posix:o-directory sb-posix:o-nofollow
                                                           +postmaster-o-cloexec+) 0)))
                 (when (minusp fd) (setf fd nil) (error "cannot inspect configuration child prefix"))
                 (physical-reseed-follow-configuration-child-fact parent name fd)
                 (let ((files (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t :maximum-entries 9)))
                   (dolist (leaf files)
                     (unless (and (= 5 prefix)
                                  (find-if (lambda (entry) (and (equal name (second entry))
                                                                (equal leaf (third entry)))) layout))
                       (error "configuration workspace contains an unexplained file"))
                     (setf (gethash (list name leaf) present) t))))
            (when fd (sb-posix:close fd))))))
    (let ((missing nil) (count 0))
      (dolist (entry layout)
        (if (gethash (list (second entry) (third entry)) present)
            (if missing (error "configuration files skip their staged prefix") (incf count))
            (setf missing t)))
      (values prefix count))))

;; Return (logical-name (file-row)) entries for the first COUNT layout items;
;; the default includes the whole layout.  Each file-row records its physical
;; identity, size and digest after checking the named file.
(defun physical-reseed-follow-configuration-current-entries (layout children check &optional (count (length layout)))
  (unless (typep count `(integer 0 ,(length layout))) (error "invalid configuration entry prefix"))
  (loop for (logical directory leaf bytes) in (subseq layout 0 count)
        collect
        (progn
          (funcall check)
          (list logical
                (list (physical-reseed-follow-configuration-file-row
                       (cdr (assoc directory children :test #'equal)) leaf logical bytes))))))

(defun physical-reseed-follow-configuration-prefix-entries (owner parent layout check)
  (multiple-value-bind (prefix count) (physical-reseed-follow-configuration-prefix parent layout)
    (if (= 5 prefix)
        (call-with-physical-reseed-follow-configuration-children
         owner parent
         (lambda (children facts child-check)
           (declare (ignore facts))
           (physical-reseed-follow-configuration-current-entries layout children child-check count))
         check)
        nil)))

(defun physical-reseed-follow-configuration-guard-facts (parent stream name)
  (let ((fd (sb-sys:fd-stream-fd stream))
        (mount (physical-adoption-install-mount-id nil :directory-fd parent))
        (path (merge-pathnames name (physical-image-fd-path parent))))
    (flet ((check-mount ()
             (unless (= mount (physical-adoption-install-mount-id nil :directory-fd fd)
                        (physical-adoption-install-mount-id path))
               (error "configuration guard path changed its mount"))))
      (check-mount)
      (multiple-value-prog1
          (physical-reseed-install-guard-facts
           parent stream name (sb-posix:stat-dev (sb-posix:fstat parent)) mount)
        (check-mount)))))

;; Hold a private guard with its named descriptor and mount checked.  PREPARE
;; permits creation. Pass the inode and a check function to FUNCTION; the check
;; function is valid only while this guard is held.
(defun call-with-physical-reseed-follow-configuration-guard (owner parent name function check &key prepare)
  (let* ((path (merge-pathnames name (physical-image-fd-path parent)))
         (synchronize prepare)
         (create (not (physical-adoption-path-present-p path))))
    (when (and create (not prepare)) (error "configuration guard is absent"))
    (setf prepare create)
    (funcall check)
    (when prepare
      (physical-reseed-follow-test-point :before-configuration-guard-create path)
      (funcall check)
      (setf (physical-reseed-follow-owner-uncertain-p owner) t))
    (call-with-physical-reseed-install-guard
     parent name prepare
     (lambda (stream)
       (let ((inode (physical-reseed-follow-configuration-guard-facts parent stream name)))
         (flet ((verify ()
                  (funcall check)
                  (unless (= inode (physical-reseed-follow-configuration-guard-facts parent stream name))
                    (error "configuration guard identity changed"))
                  (when (equal name ".pg_lisp.lock")
                    (physical-file-check-exclusive-flock stream))))
           (verify)
           (when synchronize
             (sb-posix:fsync (sb-sys:fd-stream-fd stream))
             (sb-posix:fsync parent)
             (when prepare (physical-reseed-follow-test-point :after-configuration-guard-sync path))
             (verify))
           (multiple-value-prog1 (funcall function inode #'verify)
             (verify))))))))

(in-package #:pg)

;; Observe the old root namespace for a future installation plan.  This does
;; not publish an install journal, move files, hand off, or retire startup
;; fences.  Later work must revalidate these observations before acting.

(defparameter *physical-reseed-install-retained-names*
  '(".pg_lisp.lock" "physical-reseed.attempt"))

(defparameter *physical-reseed-install-unresolved-names*
  '("physical-reseed.attempt.tmp" "physical-reseed.install" "physical-reseed.install.tmp"))

(defvar *physical-reseed-install-inventory-test-hook* nil)

(defun physical-reseed-install-old-name-p (name)
  (and (stringp name) (<= 1 (length (string->bytes name)) 255)
       (not (find #\/ name)) (not (find (code-char 0) name))
       (not (member name '("." "..") :test #'equal))
       (not (member name *physical-reseed-install-retained-names* :test #'equal))
       (not (member name *physical-reseed-install-unresolved-names* :test #'equal))))

;; A row is (name device inode mode nlink uid size mtime-ns ctime-ns).
;; Distinct hard-link names are allowed, but aliases of retained management
;; objects are not.  Copy the name and row so callers own the result.

;; Check the common stat fields carried by old and incoming inventory rows.
;; Names and any additional fields are checked by their respective callers.
(defun physical-reseed-install-row-facts-p (row device)
  (and (listp row) (= 9 (length row))
       (typep (second row) '(unsigned-byte 64)) (= device (second row))
       (typep (third row) '(integer 1 18446744073709551615))
       (typep (fourth row) '(unsigned-byte 16))
       (member (logand #o170000 (fourth row))
               '(#o100000 #o040000 #o120000 #o010000 #o140000 #o020000 #o060000))
       (typep (fifth row) '(integer 1 4294967295))
       (typep (sixth row) '(unsigned-byte 32))
       (typep (seventh row) '(signed-byte 64))
       (every (lambda (time) (typep time '(signed-byte 96))) (nthcdr 7 row))))

(defun physical-reseed-install-row-file-facts (row)
  (append (subseq row 1 7)
          (list (floor (eighth row) 1000000000) (floor (ninth row) 1000000000))))

(defun physical-reseed-install-old-row-copy (row device forbidden)
  (unless (and (physical-reseed-install-row-facts-p row device)
               (physical-reseed-install-old-name-p (first row))
               (not (member (subseq row 1 3) forbidden :test #'equal)))
    (error "invalid or aliased reseed old inventory row"))
  (cons (copy-seq (first row)) (copy-list (rest row))))

;; Require a strictly sorted, bounded name set, including a bound on encoded
;; UTF-8 bytes.  Names that cannot be represented as UTF-8 are unsupported.

(defun physical-reseed-install-old-copy (rows device &key forbidden)
  (unless (and (listp rows) (<= (length rows) 100000))
    (error "reseed old inventory exceeds its entry bound"))
  (let ((previous nil) (bytes 0))
    (loop for row in rows
          for checked = (physical-reseed-install-old-row-copy row device forbidden)
          do (unless (and (or (null previous) (string< previous (first checked)))
                          (<= (incf bytes (length (string->bytes (first checked)))) 16777216))
               (error "reseed old inventory is unordered, duplicate or oversized"))
             (setf previous (first checked))
          collect checked)))

;; Across later steps, ctime may change after rename or hard-link operations.
;; Directories also permit link count, size, and mtime changes.  The initial
;; two-pass snapshot below compares complete rows, including ctime; PID evidence
;; retains its separate, stronger ownership checks.

(defun physical-reseed-install-old-row-matches-p (before current)
  (and (equal (subseq before 0 4) (subseq current 0 4))
       (= (sixth before) (sixth current))
       (or (sb-posix:s-isdir (fourth before))
           (and (= (fifth before) (fifth current))
                (= (seventh before) (seventh current))
                (= (eighth before) (eighth current))))))

;; Pack statx major and minor using the Linux GNU makedev layout, matching
;; the device identity used by the surrounding stat-based ownership checks.

(defun physical-reseed-install-statx-device (sap)
  (let ((major (sb-sys:sap-ref-32 sap #x88)) (minor (sb-sys:sap-ref-32 sap #x8c)))
    (logior (logand minor #xff) (ash (logand major #xfff) 8)
            (ash (logand minor #xffffff00) 12) (ash (logand major #xfffff000) 32))))

;; Keep statx timestamps at nanosecond precision, including negative seconds.

(defun physical-reseed-install-statx-time (sap offset)
  (let ((seconds (sb-sys:signed-sap-ref-64 sap offset))
        (nanos (sb-sys:sap-ref-32 sap (+ offset 8))))
    (unless (< nanos 1000000000) (error "invalid reseed inventory statx timestamp"))
    (+ (* seconds 1000000000) nanos)))

;; Obtain one combined statx observation relative to the held root, without
;; following the entry.  Require the needed fields and the root's mount ID.
;; Do not open old contents, traverse directories, or read symlink referents;
;; nested mount descendants are therefore not inspected.

(defun physical-reseed-install-old-observe (fd name mount)
  (sb-alien:with-alien ((buffer (sb-alien:array sb-alien:unsigned-char 256)))
    (unless (zerop (physical-adoption-statx
                    fd name +physical-adoption-at-symlink-nofollow+ #x17ff
                    (sb-alien:cast (sb-alien:addr buffer) (* sb-alien:unsigned-char))))
      (error "cannot inspect reseed old inventory entry: errno ~d" (sb-alien:get-errno)))
    (let ((sap (sb-alien:alien-sap (sb-alien:addr buffer))))
      (unless (and (= #x13cf (logand #x13cf (sb-sys:sap-ref-32 sap 0)))
                   (= mount (sb-sys:sap-ref-64 sap #x90)))
        (error "reseed old inventory requires complete facts on the target mount"))
      (list name (physical-reseed-install-statx-device sap)
            (sb-sys:sap-ref-64 sap #x20) (sb-sys:sap-ref-16 sap #x1c)
            (sb-sys:sap-ref-32 sap #x10) (sb-sys:sap-ref-32 sap #x14)
            (sb-sys:sap-ref-64 sap #x28)
            (physical-reseed-install-statx-time sap #x70)
            (physical-reseed-install-statx-time sap #x60)))))

(defun physical-reseed-install-old-scan (fd mount device forbidden &key install-entry)
  (unless (member install-entry '(nil "physical-reseed.install.tmp" "physical-reseed.install") :test #'equal)
    (error "invalid reseed install inventory exclusion"))
  (let ((names (sort (directory-entry-names
                      (physical-image-fd-path fd) :follow-symlinks t
                      :maximum-entries (if install-entry 100003 100002)
                      :maximum-name-bytes (+ 16777216 256))
                     #'string<)))
    (when (intersection (remove install-entry names :test #'equal)
                        *physical-reseed-install-unresolved-names* :test #'equal)
      (error "reseed old inventory has unresolved management intent"))
    (physical-reseed-install-old-copy
     (loop for name in names
           unless (or (equal name install-entry)
                      (member name *physical-reseed-install-retained-names* :test #'equal))
             collect (physical-reseed-install-old-observe fd name mount))
     device :forbidden forbidden)))

;; Require the ordinary stopped owner and its exact original active journal.
;; Pair the root descriptor with its pathname and recheck both ownership and
;; journal after two identical scans.  These checks detect cooperative races,
;; not malicious same-UID ABA changes; the rows remain observations, not proof
;; that a later installation is safe.

(defun physical-reseed-install-old-snapshot (owner record)
  (unless (eq (type-of owner) 'physical-reseed-owner)
    (error "reseed old inventory requires ordinary stopped ownership"))
  (physical-reseed-require-journal owner record)
  (unless (eq :premanifest (getf record :phase))
    (error "reseed old inventory requires an active original attempt"))
  (let ((root (physical-adoption-owner-directory owner)) (fd nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts (setf fd (physical-backup-open-directory root)))
           (physical-image-check-target root fd)
           (let* ((device (physical-adoption-owner-device owner))
                  (mount (physical-adoption-install-mount-id nil :directory-fd fd))
                  (attempt (physical-reseed-install-old-observe fd "physical-reseed.attempt" mount))
                  (forbidden (list (list device (physical-adoption-owner-inode owner))
                                   (list (physical-adoption-owner-lock-device owner)
                                         (physical-adoption-owner-lock-inode owner))
                                   (subseq attempt 1 3)))
                  (rows (physical-reseed-install-old-scan fd mount device forbidden)))
             (when *physical-reseed-install-inventory-test-hook*
               (funcall *physical-reseed-install-inventory-test-hook* :after-inventory root))
             (unless (equal rows
                            (handler-case (physical-reseed-install-old-scan fd mount device forbidden)
                              (error (condition) (error "reseed old inventory changed: ~a" condition))))
               (error "reseed old inventory changed during verification"))
             (physical-image-check-target root fd)
             (physical-reseed-require-journal owner record)
             rows))
      (when fd (sb-posix:close fd)))))

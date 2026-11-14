(in-package #:pg)

(defun logical-slot-copy-cold-scratch-kind (name)
  (dolist (entry '((".timeline-" . :timeline) (".history-" . :history)))
    (when (uiop:string-prefix-p (car entry) name)
      (let ((start (length (car entry))))
        (unless (and (= (length name) (+ start 6))
                     (every (lambda (character)
                              (find character "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))
                            (subseq name start)))
          (error "invalid reserved WAL scratch name"))
        (return-from logical-slot-copy-cold-scratch-kind (cdr entry))))))

(defun logical-slot-copy-cold-scratch-same-inode-p (left right)
  (and (= (first left) (first right)) (= (second left) (second right))
       (= (ninth left) (ninth right))))

(defun logical-slot-copy-cold-scratch-preflight (rows directory-facts)
  ;; A canonical history inode may have extra links only to known .history-
  ;; scratch names in this directory.  The link count must equal the known
  ;; aliases, leaving no outside links; cleanup leaves canonical history with
  ;; one link.
  (dolist (row rows)
    (when (logical-slot-copy-cold-scratch-kind (first row))
      (let* ((facts (second row))
             (aliases (remove-if-not
                       (lambda (entry)
                         (logical-slot-copy-cold-scratch-same-inode-p facts (second entry))) rows))
             (canonical (remove-if
                         (lambda (entry) (logical-slot-copy-cold-scratch-kind (first entry))) aliases)))
        (unless (and (sb-posix:s-isreg (third facts))
                     (= #o600 (logand #o7777 (third facts)))
                     (= (sb-posix:geteuid) (fifth facts))
                     (= (first facts) (first directory-facts))
                     (= (ninth facts) (ninth directory-facts))
                     (= (fourth facts) (length aliases))
                     (every (lambda (entry) (equal facts (second entry))) aliases)
                     (or (null canonical)
                         (and (= 1 (length canonical))
                              (recovery-history-file-timeline
                               (concatenate 'string "pg_wal/" (caar canonical)))
                              (every (lambda (entry)
                                       (or (eq entry (first canonical))
                                           (eq :history (logical-slot-copy-cold-scratch-kind (first entry)))))
                                     aliases))))
          (error "WAL scratch is not private or has unexplained inode aliases")))))
  t)

(defun logical-slot-copy-cold-scratch-check-namespace (context directory before rows maximum changed)
  (physical-image-check-children context)
  (let ((now (physical-image-file-facts directory)))
    (unless (and (equal now (physical-image-file-facts
                             (physical-image-context-root-fd context) "pg_wal"))
                 (loop for old in before for actual in now for index from 0
                       always (or (and changed (member index '(5 6 7))) (eql old actual)))
                 (equal (mapcar #'first rows) (logical-slot-copy-import-control-names directory maximum)))
      (error "WAL scratch directory or namespace changed")))
  (dolist (row rows)
    (unless (equal (second row) (physical-image-file-facts directory (first row)))
      (error "WAL scratch protected inode changed")))
  t)

(defun logical-slot-copy-cold-scratch-after-unlink (rows row fd)
  ;; The held descriptor witnesses the unlinked inode.  Update link count and
  ;; ctime only for its remaining aliases; never refresh unrelated ledger rows.
  (let* ((before (second row)) (after (physical-image-file-facts fd)))
    (unless (and (= (1- (fourth before)) (fourth after))
                 (loop for old in before for actual in after for index from 0
                       always (or (member index '(3 7)) (eql old actual))))
      (error "WAL scratch unlink changed more than its link count and ctime"))
    (setf rows (delete row rows :test #'eq))
    (dolist (entry rows)
      (when (logical-slot-copy-cold-scratch-same-inode-p before (second entry))
        (setf (second entry) (copy-list after))))
    rows))

(defun logical-slot-copy-cold-clean-wal-scratch (context check mark-started maximum)
  (declare (notinline physical-image-openat port-unlinkat sb-posix:fsync))
  ;; Remove private WAL scratch before strict child preflight.  As with
  ;; RemoveTempXlogFiles in xlog.c, this needs a period without WAL writers;
  ;; the inode and alias checks here impose a stronger private-file policy.
  ;; The caller owns exclusive cold root and store access, a retained R lease,
  ;; and the incomplete-exit fence.  Interrupt masking does not exclude outside
  ;; OS writers.  CHECK repeatedly verifies the caller's owner and retained-source
  ;; guards.  MARK-STARTED must be idempotent and precedes unlink and directory fsync.
  ;;
  ;; The caller ensures MAXIMUM bounds the directory scan.  Return the number
  ;; removed; do not read or adopt scratch bytes.  Source proof follows later.
  (unless (and (functionp check) (functionp mark-started) (typep maximum '(integer 1 *)))
    (error "invalid cold WAL scratch cleanup scope"))
  (funcall check)
  (physical-image-check-children context)
  (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
         (before (physical-image-file-facts directory))
         (rows (mapcar (lambda (name) (list name (physical-image-file-facts directory name)))
                       (logical-slot-copy-import-control-names directory maximum)))
         (scratch (remove-if-not (lambda (row) (logical-slot-copy-cold-scratch-kind (first row))) rows))
         (changed nil) (removed 0))
    (logical-slot-copy-cold-scratch-check-namespace context directory before rows maximum changed)
    (logical-slot-copy-cold-scratch-preflight rows before)
    (dolist (selected scratch)
      (let ((fd nil) (complete nil) (row (assoc (first selected) rows :test #'equal)))
        (sb-sys:without-interrupts
          (unwind-protect
               (progn
                 (setf fd (physical-image-openat
                           directory (first row)
                           (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                   +postmaster-o-cloexec+) 0))
                 (when (minusp fd) (setf fd nil) (error "cannot open WAL scratch inode"))
                 (sb-sys:with-local-interrupts
                   (funcall check)
                   (logical-slot-copy-cold-scratch-check-namespace
                    context directory before rows maximum changed)
                   (unless (equal (second row) (physical-image-file-facts fd))
                     (error "WAL scratch name differs from held inode"))
                   (sb-sys:without-interrupts
                     (funcall mark-started)
                     (logical-slot-copy-cold-scratch-check-namespace
                      context directory before rows maximum changed)
                     (unless (zerop (port-unlinkat directory (first row) 0))
                       (error "cannot unlink owned WAL scratch"))
                     (setf changed t
                           rows (logical-slot-copy-cold-scratch-after-unlink rows row fd))
                     (incf removed))
                   (logical-slot-copy-cold-scratch-check-namespace
                    context directory before rows maximum changed)
                   (setf complete t)))
            (when fd
              (let ((failure (physical-image-drain-descriptors (list (cons nil fd)) nil)))
                (when (and complete failure) (error failure))))))))
    (funcall check)
    (logical-slot-copy-cold-scratch-check-namespace context directory before rows maximum changed)
    (sb-sys:without-interrupts
      (funcall mark-started)
      (logical-slot-copy-cold-scratch-check-namespace context directory before rows maximum changed)
      (sb-posix:fsync directory))
    (funcall check)
    (logical-slot-copy-cold-scratch-check-namespace context directory before rows maximum changed)
    removed))

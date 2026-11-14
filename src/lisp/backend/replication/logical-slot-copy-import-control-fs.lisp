(in-package #:pg)

(defun logical-slot-copy-import-control-names (global maximum)
  (sort (directory-entry-names (physical-image-fd-path global) :follow-symlinks t
                                                               :maximum-entries maximum :maximum-name-bytes (* 256 maximum))
        #'string<))

(defun logical-slot-copy-import-control-private-facts-p (facts global-facts)
  (and (sb-posix:s-isreg (third facts)) (= 1 (fourth facts))
       (= (sb-posix:geteuid) (fifth facts)) (= #o600 (logand #o7777 (third facts)))
       (= (first facts) (first global-facts)) (= (ninth facts) (ninth global-facts))))

(defun logical-slot-copy-import-control-global-image (context manifest)
  ;; Bind the private global directory and every manifest child.  Only the
  ;; control temporary is optional; another name is rejected and left untouched.
  (let* ((global (cdr (assoc "global" (physical-image-context-children context) :test #'equal)))
         (facts (physical-image-file-facts global))
         (names (sort (remove-duplicates
                       (cons "pg_wal_control"
                             (loop for row in (getf manifest :files) for name = (first row)
                                   when (uiop:string-prefix-p "global/" name)
                                     collect (subseq name 7))) :test #'equal) #'string<))
         (actual (logical-slot-copy-import-control-names global (+ 2 (length names))))
         (children nil))
    (unless (and (every (lambda (name) (and (plusp (length name)) (not (find #\/ name)))) names)
                 (equal names (remove "pg_wal_control.tmp" actual :test #'equal)))
      (error "control temporary global inventory differs from its base manifest"))
    (dolist (name names)
      (let ((child (physical-image-file-facts global name)))
        (unless (logical-slot-copy-import-control-private-facts-p child facts)
          (error "control temporary global child is not a private bound inode"))
        (push (list name child) children)))
    (unless (and (equal facts (physical-image-file-facts global))
                 (equal facts (physical-image-file-facts (physical-image-context-root-fd context) "global")))
      (error "control temporary global directory changed during observation"))
    (values global facts (nreverse children) (if (member "pg_wal_control.tmp" actual :test #'equal)
                                                 :present :absent))))

(defun logical-slot-copy-import-control-read-fd (fd maximum)
  (declare (notinline sb-posix:read))
  (let* ((facts (physical-image-file-facts fd)) (size (sixth facts)))
    (unless (and (<= 0 size maximum) (zerop (sb-posix:lseek fd 0 sb-posix:seek-set)))
      (error "control temporary exceeds its deterministic image bound"))
    (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
      (sb-sys:with-pinned-objects (bytes)
        (loop with offset = 0 while (< offset size)
              for count = (sb-posix:read fd (sb-sys:sap+ (sb-sys:vector-sap bytes) offset)
                                         (- size offset))
              do (unless (plusp count) (error "short control temporary read"))
                 (incf offset count)))
      (unless (equal facts (physical-image-file-facts fd))
        (error "control temporary changed during reading"))
      (values bytes facts))))

(defun logical-slot-copy-import-control-open-temp (record)
  ;; Keep the no-follow descriptor open across unlink.  The name and descriptor
  ;; must identify the same private, single-link inode and exact image prefix.
  (declare (notinline physical-image-openat))
  (logical-slot-copy-import-control-record record)
  (when (eq :present (logical-slot-copy-import-control-state-phase record))
    (sb-sys:without-interrupts
      (let ((fd (physical-image-openat
                 (logical-slot-copy-import-control-state-global record) "pg_wal_control.tmp"
                 (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                         +postmaster-o-cloexec+) 0)))
        (when (minusp fd) (error "cannot open bound control temporary"))
        (setf (logical-slot-copy-import-control-state-fd record) fd)))
    (let* ((fd (logical-slot-copy-import-control-state-fd record))
           (facts (physical-image-file-facts fd)))
      (unless (and (logical-slot-copy-import-control-private-facts-p
                    facts (logical-slot-copy-import-control-state-global-facts record))
                   (equal facts (physical-image-file-facts
                                 (logical-slot-copy-import-control-state-global record)
                                 "pg_wal_control.tmp")))
        (error "control temporary is not its private bound inode"))
      (multiple-value-bind (bytes read-facts)
          (logical-slot-copy-import-control-read-fd
           fd (length (logical-slot-copy-import-control-state-expected record)))
        (unless (and (equal facts read-facts)
                     (logical-slot-copy-import-control-prefix-p
                      bytes (logical-slot-copy-import-control-state-expected record)))
          (error "control temporary differs from its deterministic child image"))
        (setf (logical-slot-copy-import-control-state-facts record) facts
              (logical-slot-copy-import-control-state-bytes record) bytes)))))

(defun logical-slot-copy-import-control-check-namespace (record)
  (logical-slot-copy-import-control-record record)
  (let* ((global (logical-slot-copy-import-control-state-global record))
         (before (logical-slot-copy-import-control-state-global-facts record))
         (now (physical-image-file-facts global))
         (children (logical-slot-copy-import-control-state-global-children record))
         (present (eq :present (logical-slot-copy-import-control-state-phase record)))
         (expected (sort (append (mapcar #'first children) (when present (list "pg_wal_control.tmp")))
                         #'string<)))
    (unless (and (equal expected (logical-slot-copy-import-control-names global (1+ (length expected))))
                 (if (eq :removed (logical-slot-copy-import-control-state-phase record))
                     (loop for a in before for b in now for index from 0
                           always (or (member index '(5 6 7)) (eql a b)))
                     (equal before now))
                 (equal now (physical-image-file-facts
                             (logical-slot-copy-import-control-state-root record) "global")))
      (error "control temporary global namespace changed"))
    (dolist (child children)
      (unless (equal (second child) (physical-image-file-facts global (first child)))
        (error "control temporary other global child changed"))))
  t)

(defun logical-slot-copy-import-control-check-temp (record)
  ;; After unlink the held inode must have no links; its bytes remain unchanged.
  (logical-slot-copy-import-control-check-namespace record)
  (let ((fd (logical-slot-copy-import-control-state-fd record))
        (phase (logical-slot-copy-import-control-state-phase record)))
    (when fd
      (let* ((before (logical-slot-copy-import-control-state-facts record))
             (now (physical-image-file-facts fd)))
        (unless (if (eq phase :present)
                    (and (equal before now)
                         (equal before (physical-image-file-facts
                                        (logical-slot-copy-import-control-state-global record)
                                        "pg_wal_control.tmp")))
                    (and (eq phase :removed) (zerop (fourth now))
                         (loop for a in before for b in now for index from 0
                               always (or (member index '(3 7)) (eql a b)))))
          (error "control temporary inode changed"))
        (unless (equalp (logical-slot-copy-import-control-state-bytes record)
                        (logical-slot-copy-import-control-read-fd
                         fd (length (logical-slot-copy-import-control-state-expected record))))
          (error "control temporary bytes changed"))))
    (when (and (eq phase :present) (null fd))
      (error "control temporary lost its held descriptor")))
  t)

(defun logical-slot-copy-import-control-close-temp (record)
  (declare (notinline sb-posix:close))
  (sb-sys:without-interrupts
    (let ((fd (logical-slot-copy-import-control-state-fd record)))
      (when fd
        (setf (logical-slot-copy-import-control-state-fd record) nil)
        (sb-posix:close fd)))))

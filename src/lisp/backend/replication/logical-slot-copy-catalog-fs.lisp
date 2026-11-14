(in-package #:pg)

(defun logical-slot-copy-catalog-global-image (context manifest)
  (multiple-value-bind (global facts children present)
      (logical-slot-copy-import-bound-global-image context manifest :catalog)
    (values global facts children (if present :residue :absent))))

(defun logical-slot-copy-catalog-file-operations (attempt-reader)
  (declare (notinline physical-image-openat sb-posix:close))
  (let ((old-fd nil) (temp-fd nil))
    (lambda (operation subject)
      (let ((attempt (funcall attempt-reader)) (complete nil))
        (unless (eq attempt subject) (error "catalog descriptor scope differs"))
        (unwind-protect
             (multiple-value-prog1
                 (case operation
                   ((:close-old :close-temp)
                    (let* ((temporary (eq operation :close-temp))
                           (fd (if temporary temp-fd old-fd)))
                      (when fd
                        (unwind-protect
                             (unless (logical-slot-copy-committed-image-equal-p
                                      (logical-slot-copy-catalog-attempt-operation-seal attempt)
                                      (logical-slot-copy-catalog-operation-image attempt))
                               (error "catalog operation binding changed during cleanup"))
                          (if temporary
                              (setf temp-fd nil (logical-slot-copy-catalog-attempt-temp-fd attempt) nil)
                              (setf old-fd nil (logical-slot-copy-catalog-attempt-old-fd attempt) nil))
                          (unwind-protect (logical-slot-copy-catalog-seal-operation attempt)
                            (sb-posix:close fd))))))
                   ((:open-old :open-temp :create-temp)
                    (if (eq operation :create-temp)
                        (logical-slot-copy-catalog-check attempt)
                        (logical-slot-copy-catalog-record attempt))
                    (let* ((temporary (not (eq operation :open-old)))
                           (create (eq operation :create-temp))
                           (phase (logical-slot-copy-catalog-attempt-phase attempt)))
                      (unless (and
                               (if temporary
                                   (and (null temp-fd) (null (logical-slot-copy-catalog-attempt-temp-fd attempt))
                                        old-fd
                                        (if create (member phase '(:absent :unlinked)) (eq phase :residue)))
                                   (and (null old-fd) (null (logical-slot-copy-catalog-attempt-old-fd attempt))
                                        (member phase '(:absent :residue)))))
                        (error "catalog descriptor operation has the wrong phase"))
                      (sb-sys:without-interrupts
                        (when create (funcall (logical-slot-copy-catalog-attempt-mark-started attempt)))
                        (let ((fd (physical-image-openat
                                   (logical-slot-copy-catalog-attempt-global attempt)
                                   (if temporary "pg_catalog.tmp" "pg_catalog.dat")
                                   (logior (if create
                                               (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl)
                                               sb-posix:o-rdonly)
                                           sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+)
                                   (if create #o600 0))))
                          (when (minusp fd) (error "cannot open owned catalog descriptor"))
                          (if temporary
                              (setf temp-fd fd (logical-slot-copy-catalog-attempt-temp-fd attempt) fd)
                              (setf old-fd fd (logical-slot-copy-catalog-attempt-old-fd attempt) fd))
                          (when create
                            (setf (logical-slot-copy-catalog-attempt-phase attempt) :writing
                                  (logical-slot-copy-catalog-attempt-global-changed attempt) t
                                  (logical-slot-copy-catalog-attempt-temp-facts attempt) nil))
                          (logical-slot-copy-catalog-seal-operation attempt)
                          fd))))
                   (otherwise (error "unknown catalog descriptor operation")))
               (setf complete t))
          (unless complete (logical-slot-copy-catalog-revoke attempt)))))))

(defun logical-slot-copy-catalog-open-one (attempt temporary)
  (with-logical-slot-copy-catalog-guard (attempt :files nil)
    (let* ((fd (funcall (logical-slot-copy-catalog-attempt-file-operation attempt)
                        (if temporary :open-temp :open-old) attempt))
           (global (logical-slot-copy-catalog-attempt-global attempt))
           (facts (physical-image-file-facts fd))
           (b (logical-slot-copy-catalog-attempt-binding attempt)))
      (unless (and (logical-slot-copy-import-control-private-facts-p facts (getf b :global-facts))
                   (equal facts (physical-image-file-facts global (if temporary "pg_catalog.tmp" "pg_catalog.dat"))))
        (error "catalog restoration inode is not private or bound"))
      (when temporary
        (unless (logical-slot-copy-import-control-prefix-p
                 (logical-slot-copy-import-control-read-fd fd (length (getf b :catalog)))
                 (getf b :catalog))
          (error "catalog temporary differs from selected restartpoint"))
        (setf (logical-slot-copy-catalog-attempt-temp-facts attempt) facts)
        (logical-slot-copy-catalog-seal-operation attempt))))
  t)

(defun logical-slot-copy-catalog-check-namespace (attempt)
  (let* ((b (logical-slot-copy-catalog-attempt-binding attempt))
         (global (logical-slot-copy-catalog-attempt-global attempt))
         (before (getf b :global-facts)) (now (physical-image-file-facts global))
         (present (member (logical-slot-copy-catalog-attempt-phase attempt) '(:residue :writing :ready)))
         (names (sort (append (mapcar #'first (getf b :global-children))
                              (when present (list "pg_catalog.tmp"))) #'string<)))
    (unless (and (equal names (logical-slot-copy-import-control-names global (1+ (length names))))
                 (equal now (physical-image-file-facts (logical-slot-copy-catalog-attempt-root attempt) "global"))
                 (if (logical-slot-copy-catalog-attempt-global-changed attempt)
                     (loop for a in before for c in now for index from 0
                           always (or (member index '(5 6 7)) (eql a c)))
                     (equal before now)))
      (error "catalog restoration global namespace changed"))
    (dolist (child (getf b :global-children))
      (unless (equal (if (equal "pg_catalog.dat" (first child))
                         (logical-slot-copy-catalog-attempt-target-facts attempt)
                         (second child))
                     (physical-image-file-facts global (first child)))
        (error "catalog restoration protected global inode changed"))))
  t)

(defun logical-slot-copy-catalog-check-old (attempt)
  (let* ((b (logical-slot-copy-catalog-attempt-binding attempt))
         (fd (logical-slot-copy-catalog-attempt-old-fd attempt))
         (before (getf b :old-facts)))
    (when fd
      (let ((now (physical-image-file-facts fd)))
        (unless (if (eq :installed (logical-slot-copy-catalog-attempt-phase attempt))
                    (and (zerop (fourth now))
                         (loop for a in before for c in now for index from 0
                               always (or (member index '(3 7)) (eql a c))))
                    (equal before now))
          (error "catalog restoration original inode changed")))
      (unless (equalp (getf b :old-bytes)
                      (logical-slot-copy-import-control-read-fd fd (length (getf b :old-bytes))))
        (error "catalog restoration original bytes changed")))))

(defun logical-slot-copy-catalog-check-temp (attempt)
  (let* ((b (logical-slot-copy-catalog-attempt-binding attempt))
         (phase (logical-slot-copy-catalog-attempt-phase attempt))
         (fd (logical-slot-copy-catalog-attempt-temp-fd attempt))
         (before (logical-slot-copy-catalog-attempt-temp-facts attempt))
         (present (member phase '(:residue :writing :ready))))
    (when (and present (null fd)) (error "catalog restoration lost its temporary descriptor"))
    (when fd
      (let ((now (physical-image-file-facts fd)))
        (unless (or (eq phase :unlinked)
                    (logical-slot-copy-import-control-private-facts-p now (getf b :global-facts)))
          (error "catalog restoration temporary is not private"))
        (unless (if (eq phase :unlinked)
                    (and (zerop (fourth now))
                         (loop for a in before for c in now for index from 0
                               always (or (member index '(3 7)) (eql a c))))
                    (and (equal before now)
                         (equal now (physical-image-file-facts
                                     (logical-slot-copy-catalog-attempt-global attempt)
                                     (if (eq phase :installed) "pg_catalog.dat" "pg_catalog.tmp")))))
          (error "catalog restoration temporary inode changed"))
        (let ((bytes (logical-slot-copy-import-control-read-fd fd (length (getf b :catalog)))))
          (unless (and (logical-slot-copy-import-control-prefix-p bytes (getf b :catalog))
                       (or (not (member phase '(:ready :installed)))
                           (equalp bytes (getf b :catalog))))
            (error "catalog restoration temporary bytes changed")))))))

(defun logical-slot-copy-catalog-check-files (attempt)
  (logical-slot-copy-catalog-check-namespace attempt)
  (logical-slot-copy-catalog-check-old attempt)
  (logical-slot-copy-catalog-check-temp attempt)
  (let* ((b (logical-slot-copy-catalog-attempt-binding attempt))
         (expected (if (eq :installed (logical-slot-copy-catalog-attempt-phase attempt))
                       (getf b :catalog) (getf b :old-bytes))))
    (unless (equalp expected (logical-slot-copy-read-file
                              (logical-slot-copy-catalog-attempt-context attempt)
                              "global/pg_catalog.dat" (length expected)))
      (error "catalog restoration target bytes changed")))
  t)

(defun logical-slot-copy-catalog-close-one (attempt temporary)
  (unless (and (logical-slot-copy-catalog-attempt-p attempt)
               (functionp (logical-slot-copy-catalog-attempt-file-operation attempt)))
    (error "catalog cleanup requires its owned descriptor scope"))
  (sb-sys:without-interrupts
    (funcall (logical-slot-copy-catalog-attempt-file-operation attempt)
             (if temporary :close-temp :close-old) attempt)))

(defun logical-slot-copy-catalog-close (attempt)
  (unwind-protect (logical-slot-copy-catalog-close-one attempt t)
    (logical-slot-copy-catalog-close-one attempt nil)))

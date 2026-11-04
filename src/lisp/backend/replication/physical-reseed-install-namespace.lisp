(in-package #:pg)

(defun physical-reseed-install-top-rows (plan)
  (remove-if (lambda (row) (find #\/ (first row))) (getf plan :new)))

(defun physical-reseed-install-prefix-names (plan prefix)
  (let* ((old (getf plan :old)) (top (physical-reseed-install-top-rows plan))
                                (old-count (length old)) (old-moves (min prefix old-count))
                                (new-moves (max 0 (- prefix old-count))))
    (unless (typep prefix `(integer 0 ,(+ old-count (length top))))
      (error "invalid reseed namespace prefix"))
    (flet ((names (rows) (mapcar #'first rows)))
      (list :root
            (sort (append (list ".pg_lisp.lock" "physical-reseed.attempt" "physical-reseed.install")
                          (names (nthcdr old-moves old)) (names (subseq top 0 new-moves))) #'string<)
            :incoming
            (sort (append (list ".pg_lisp.lock" "physical-reseed.install") (names (nthcdr new-moves top))) #'string<)
            :quarantine
            (sort (append (list ".pg_lisp.lock" "physical-reseed.install") (names (subseq old 0 old-moves))) #'string<)))))

(defun physical-reseed-install-directory-names (fd)
  (sort (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t
                                                           :maximum-entries 100003 :maximum-name-bytes (+ 16777216 256)) #'string<))

(defun physical-reseed-install-namespace-names (root-fd incoming-fd quarantine-fd)
  (list :root (physical-reseed-install-directory-names root-fd)
        :incoming (physical-reseed-install-directory-names incoming-fd)
        :quarantine (physical-reseed-install-directory-names quarantine-fd)))

(defun physical-reseed-install-namespace-row-matches-p (expected current renamed)
  (and (equal (subseq expected 0 8) (subseq current 0 8))
       (or renamed (= (ninth expected) (ninth current)))))

(defun physical-reseed-install-namespace-old-rows (plan prefix root-fd quarantine-fd mount)
  (let ((moved (make-hash-table :test #'equal)) (old (getf plan :old)))
    (loop for row in old for ordinal from 1 while (<= ordinal prefix)
          do (setf (gethash (subseq row 1 3) moved) t))
    (loop for row in old for ordinal from 1
          for current = (physical-reseed-install-old-observe
                         (if (<= ordinal prefix) quarantine-fd root-fd) (first row) mount)
          do (unless (physical-reseed-install-namespace-row-matches-p
                      row current (gethash (subseq row 1 3) moved))
               (error "reseed old namespace metadata differs from its move prefix"))
          collect current)))

(defun call-with-physical-reseed-install-new-directories (root-fd incoming-fd plan prefix mount function)
  (let ((ordinals (make-hash-table :test #'equal)) (children nil) (active nil))
    (loop for row in (physical-reseed-install-top-rows plan)
          for ordinal from (1+ (length (getf plan :old)))
          do (setf (gethash (first row) ordinals) ordinal))
    (labels ((top-parent (name)
               (let ((ordinal (gethash name ordinals)))
                 (unless ordinal (error "unknown reseed incoming top-level name"))
                 (values (if (<= ordinal prefix) root-fd incoming-fd) (<= ordinal prefix))))
             (resolve (name)
               (unless active (error "reseed new-directory scope has ended"))
               (let ((slash (position #\/ name)))
                 (if slash
                     (let ((child (assoc (subseq name 0 slash) children :test #'equal)))
                       (unless child (error "unknown reseed incoming child directory"))
                       (values (cdr child) (subseq name (1+ slash)) nil))
                     (multiple-value-bind (parent renamed) (top-parent name)
                       (values parent name renamed)))))
             (check ()
               (unless active (error "reseed new-directory scope has ended"))
               (dolist (entry children)
                 (let* ((name (car entry)) (fd (cdr entry))
                                           (expected (assoc name (getf plan :new) :test #'equal)))
                   (multiple-value-bind (parent renamed) (top-parent name)
                     (let ((current (physical-reseed-install-old-observe parent name mount)))
                       (unless (and (physical-reseed-install-namespace-row-matches-p expected current renamed)
                                    (equal (physical-reseed-install-row-file-facts current)
                                           (physical-reseed-file-facts (physical-image-check-directory fd)))
                                    (= mount (physical-adoption-install-mount-id nil :directory-fd fd)))
                         (error "reseed new directory lost its named descriptor binding"))))
                   (let ((expected-names
                           (loop for row in (getf plan :new) for path = (first row)
                                 for slash = (position #\/ path)
                                 when (and slash (string= name path :end2 slash))
                                   collect (subseq path (1+ slash)))))
                     (unless (equal (sort expected-names #'string<) (physical-reseed-install-directory-names fd))
                       (error "reseed new child directory inventory differs")))))))
      (unwind-protect
           (progn
             (dolist (name '("base" "global" "pg_wal"))
               (sb-sys:without-interrupts
                 (let ((fd nil))
                   (unwind-protect
                        (progn
                          (setf fd (physical-image-openat
                                    (top-parent name) name
                                    (logior sb-posix:o-rdonly sb-posix:o-directory
                                            sb-posix:o-nofollow +postmaster-o-cloexec+) 0))
                          (when (minusp fd) (setf fd nil) (error "cannot open reseed new child directory"))
                          (push (cons name fd) children)
                          (setf fd nil))
                     (when fd (sb-posix:close fd))))))
             (setf active t)
             (check)
             (multiple-value-prog1 (funcall function #'resolve #'check)
               (check)))
        (setf active nil)
        (dolist (entry children) (sb-posix:close (cdr entry)))))))

(defun physical-reseed-install-namespace-new-rows (plan mount resolve)
  (loop for row in (getf plan :new)
        collect
        (multiple-value-bind (parent leaf renamed) (funcall resolve (first row))
          (let ((current
                  (if (sb-posix:s-isdir (fourth row))
                      (append (physical-reseed-install-old-observe parent leaf mount) (list nil))
                      (physical-reseed-install-file-row-at
                       parent leaf (first row) (seventh row) (tenth row) mount nil))))
            (unless (physical-reseed-install-namespace-row-matches-p row current renamed)
              (error "reseed new image metadata differs from its move prefix"))
            current))))

(defun physical-reseed-install-namespace-check-new-rows (rows mount resolve)
  (dolist (row rows)
    (multiple-value-bind (parent leaf) (funcall resolve (first row))
      (let ((current (physical-reseed-install-old-observe parent leaf mount)))
        (unless (equal (subseq row 0 9) (cons (first row) (rest current)))
          (error "reseed new image changed during namespace verification"))))))

(defun physical-reseed-install-namespace-state (owner root-fd incoming-fd quarantine-fd plan guards-check)
  (physical-reseed-check-owner owner)
  (funcall guards-check)
  (let* ((state (physical-reseed-install-read-at root-fd "physical-reseed.install"))
         (mount (physical-adoption-install-mount-id nil :directory-fd root-fd))
         (attempt-row (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount))
         (attempt (physical-reseed-attempt-read-at root-fd "physical-reseed.attempt"))
         (step (getf state :step)) (maximum (1- (physical-reseed-install-last-step plan)))
         (recorded (min step maximum))
         (names (physical-reseed-install-namespace-names root-fd incoming-fd quarantine-fd))
         (prefix (cond ((equal names (physical-reseed-install-prefix-names plan recorded)) recorded)
                       ((and (< step maximum)
                             (equal names (physical-reseed-install-prefix-names plan (1+ step))))
                        (1+ step))
                       (t (error "reseed namespaces disagree with recorded or single-ahead progress"))))
         (old (physical-reseed-install-namespace-old-rows plan prefix root-fd quarantine-fd mount)))
    (unless (and (equal plan (getf state :plan))
                 (equal attempt (physical-reseed-cleanup-owner-attempt owner)))
      (error "reseed namespace journal plan or current attempt changed"))
    (call-with-physical-reseed-install-new-directories
     root-fd incoming-fd plan prefix mount
     (lambda (resolve check-directories)
       (let ((new (physical-reseed-install-namespace-new-rows plan mount resolve)))
         (physical-reseed-install-test-point :after-install-namespace-files (physical-image-fd-path root-fd))
         (physical-reseed-check-owner owner)
         (unless (equal state (physical-reseed-install-read-at root-fd "physical-reseed.install"))
           (error "reseed journal changed during namespace verification"))
         (funcall guards-check)
         (unless (and (equal attempt (physical-reseed-attempt-read-at root-fd "physical-reseed.attempt"))
                      (equal attempt-row (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount)))
           (error "reseed attempt changed during namespace verification"))
         (physical-reseed-install-namespace-check-new-rows new mount resolve)
         (funcall check-directories)
         (unless (and (equal old (physical-reseed-install-namespace-old-rows plan prefix root-fd quarantine-fd mount))
                      (equal names (physical-reseed-install-namespace-names root-fd incoming-fd quarantine-fd)))
           (error "reseed namespace changed during verification"))
         (values state prefix))))))

(defun call-with-physical-reseed-install-namespace (owner function)
  (unless (physical-reseed-install-owner-p owner) (error "reseed namespace verification requires an install owner"))
  (physical-reseed-check-owner owner)
  (let ((plan (physical-reseed-install-copy (physical-reseed-install-owner-plan owner))))
    (call-with-physical-directory-root
     owner
     (lambda (root-fd root)
       (declare (ignore root))
       (call-with-physical-reseed-install-auxiliary
        owner (getf (getf plan :incoming) :directory)
        (lambda (incoming incoming-facts incoming-fd incoming-check)
          (declare (ignore incoming incoming-facts))
          (call-with-physical-reseed-install-auxiliary
           owner (getf (getf plan :quarantine) :directory)
           (lambda (quarantine quarantine-facts quarantine-fd quarantine-check)
             (declare (ignore quarantine quarantine-facts))
             (flet ((guards-check () (funcall incoming-check) (funcall quarantine-check)))
               (flet ((check ()
                        (physical-reseed-install-namespace-state
                         owner root-fd incoming-fd quarantine-fd plan #'guards-check)))
                 (multiple-value-bind (state prefix) (check)
                   (multiple-value-prog1 (funcall function root-fd incoming-fd quarantine-fd state prefix #'check)
                     (check))))))
           :expected (getf plan :quarantine)))
        :expected (getf plan :incoming))))))

(defun physical-reseed-install-observe (owner)
  (call-with-physical-reseed-install-namespace
   owner
   (lambda (root-fd incoming-fd quarantine-fd state prefix check)
     (declare (ignore root-fd incoming-fd quarantine-fd check))
     (list :step (getf state :step) :observed prefix :partial (getf state :partial)))))

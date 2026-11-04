(in-package #:pg)

(defun physical-reseed-follow-configuration-staged-record (record directories entries)
  (let* ((copy (physical-reseed-follow-copy record)) (configuration (getf copy :configuration)))
    (setf (getf configuration :phase) :staged
          (getf configuration :directories) directories
          (getf configuration :entries) entries)
    (physical-reseed-follow-copy copy)))

(defun call-with-physical-reseed-follow-staging (owner root-fd record function check)
  (let* ((configuration (getf record :configuration)) (phase (getf configuration :phase))
                                                      (prepare (eq phase :building)))
    (unless (member phase '(:building :staged)) (error "configuration cannot enter staging"))
    (multiple-value-bind (images binding)
        (physical-reseed-follow-configuration-images-at
         owner root-fd (physical-reseed-follow-core-projection record) (getf configuration :time))
      (let ((layout (physical-reseed-follow-configuration-layout images binding)))
        (call-with-physical-reseed-follow-workspace
         owner root-fd (pathname (getf (getf configuration :workspace) :directory))
         (lambda (facts fd workspace-check)
           (let ((expected (getf (physical-reseed-follow-configuration-building-record
                                  record facts images binding (getf configuration :time)) :configuration)))
             (unless (every (lambda (key) (equal (getf expected key) (getf configuration key)))
                            '(:generation :anchor-digest :time :workspace :images))
               (error "configuration staging differs from its derived intent")))
           (flet ((namespace-check ()
                    (funcall workspace-check)
                    (physical-reseed-follow-configuration-prefix fd layout)))
             (namespace-check)
             (let ((partial (physical-reseed-follow-configuration-prefix-entries owner fd layout #'namespace-check)))
               (when (and (not prepare) (/= (length partial) (length layout)))
                 (error "staged configuration has incomplete files")))
             (call-with-physical-reseed-follow-configuration-guard
              owner fd ".pg_lisp.lock"
              (lambda (lock-inode lock-check)
                (call-with-physical-reseed-follow-configuration-guard
                 owner fd "physical-reseed.install"
                 (lambda (fence-inode guard-check)
                   (call-with-physical-reseed-follow-configuration-children
                    owner fd
                    (lambda (children child-facts child-check)
                      (let ((directories (list :lock-inode lock-inode :fence-inode fence-inode
                                               :staged (cdr (assoc "staged" child-facts :test #'equal))
                                               :saved (cdr (assoc "saved" child-facts :test #'equal))
                                               :receive (cdr (assoc "receive" child-facts :test #'equal)))))
                        (unless prepare
                          (let ((actual (physical-reseed-follow-configuration-staged-record
                                         record directories
                                         (physical-reseed-follow-configuration-current-entries layout children child-check))))
                            (unless (equal record actual) (error "staged configuration identities changed"))))
                        (funcall function fd directories layout children child-check)))
                    guard-check :prepare prepare))
                 lock-check :prepare prepare))
              #'namespace-check :prepare prepare)))
         check)))))

(defun physical-reseed-follow-stage-configuration (owner)
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "configuration staging requires dedicated ownership"))
  (physical-reseed-follow-check-idle owner)
  (call-with-physical-reseed-follow-store-proof
   owner
   (lambda (root-fd check)
     (let* ((previous (physical-reseed-follow-snapshot-at root-fd))
            (record (getf previous :record)) (publishing nil))
       (unless record (error "configuration staging requires durable building intent"))
       (when (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp" t)
         (error "configuration staging requires a settled follow journal"))
       (physical-reseed-follow-check-binding owner record)
       (flet ((before-check ()
                (funcall check)
                (unless publishing (physical-reseed-follow-check-snapshot root-fd previous))))
         (call-with-physical-reseed-follow-staging
          owner root-fd record
          (lambda (workspace-fd directories layout children stage-check)
            (dolist (entry layout)
              (destructuring-bind (logical directory leaf bytes) entry
                (physical-reseed-follow-configuration-write-file
                 owner (cdr (assoc directory children :test #'equal)) leaf logical bytes stage-check
                 :prepare (eq :building (getf (getf record :configuration) :phase)))))
            (let* ((entries (physical-reseed-follow-configuration-current-entries layout children stage-check))
                   (candidate (physical-reseed-follow-configuration-staged-record record directories entries)))
              (physical-reseed-follow-check-transition record candidate)
              (flet ((verify ()
                       (funcall stage-check)
                       (unless (equal entries
                                      (physical-reseed-follow-configuration-current-entries layout children stage-check))
                         (error "configuration staged files changed during publication"))))
                (verify)
                (if (eq :staged (getf (getf record :configuration) :phase))
                    (values (physical-reseed-follow-copy record) previous)
                    (progn
                      (dolist (entry layout)
                        (call-with-physical-image-input-at
                         (cdr (assoc (second entry) children :test #'equal)) (third entry)
                         (lambda (stream) (sb-posix:fsync (sb-sys:fd-stream-fd stream)))
                         :element-type '(unsigned-byte 8)))
                      (dolist (entry children) (sb-posix:fsync (cdr entry)))
                      (sb-posix:fsync workspace-fd)
                      (physical-adoption-install-sync-ancestors
                       (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
                      (verify)
                      (setf publishing t)
                      (physical-reseed-follow-write-intent
                       owner root-fd candidate (physical-reseed-follow-image candidate) previous #'verify))))))
          #'before-check))))))

(defun physical-reseed-follow-configuration-workspace-observation (fd)
  (let* ((names (sort (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t :maximum-entries 6)
                      #'string<))
         (mount (physical-adoption-install-mount-id nil :directory-fd fd)))
    (loop for name in names collect (physical-reseed-install-old-observe fd name mount))))

(defun call-with-physical-reseed-follow-building-prefix-evidence (owner root-fd record function check)
  (let* ((configuration (getf record :configuration)) (time (getf configuration :time)))
    (unless (eq :building (getf configuration :phase)) (error "building prefix evidence requires building intent"))
    (multiple-value-bind (images binding)
        (physical-reseed-follow-configuration-images-at
         owner root-fd (physical-reseed-follow-core-projection record) time)
      (let ((layout (physical-reseed-follow-configuration-layout images binding)))
        (call-with-physical-reseed-follow-workspace
         owner root-fd (pathname (getf (getf configuration :workspace) :directory))
         (lambda (facts fd workspace-check)
           (unless (equal record (physical-reseed-follow-configuration-building-record record facts images binding time))
             (error "configuration prefix differs from its durable intent"))
           (multiple-value-bind (prefix count) (physical-reseed-follow-configuration-prefix fd layout)
             (declare (ignore count))
             (let ((observed (physical-reseed-follow-configuration-workspace-observation fd))
                   (entries (physical-reseed-follow-configuration-prefix-entries owner fd layout workspace-check)))
               (flet ((verify ()
                        (funcall workspace-check)
                        (physical-reseed-follow-configuration-prefix fd layout)
                        (unless (and (equal observed (physical-reseed-follow-configuration-workspace-observation fd))
                                     (equal entries
                                            (physical-reseed-follow-configuration-prefix-entries
                                             owner fd layout workspace-check)))
                          (error "configuration building prefix changed during recovery"))))
                 (labels ((with-fence (inner-check)
                            (if (>= prefix 2)
                                (call-with-physical-reseed-follow-configuration-guard
                                 owner fd "physical-reseed.install"
                                 (lambda (inode guard-check)
                                   (declare (ignore inode))
                                   (multiple-value-prog1 (funcall function fd guard-check) (funcall guard-check)))
                                 inner-check)
                                (multiple-value-prog1 (funcall function fd inner-check) (funcall inner-check)))))
                   (verify)
                   (if (plusp prefix)
                       (call-with-physical-reseed-follow-configuration-guard
                        owner fd ".pg_lisp.lock"
                        (lambda (inode lock-check) (declare (ignore inode)) (with-fence lock-check))
                        #'verify)
                       (with-fence #'verify)))))))
         check)))))

(defun call-with-physical-reseed-follow-staged-evidence (owner root-fd record function check)
  (unless (eq :staged (getf (getf record :configuration) :phase))
    (error "staged evidence requires staged configuration"))
  (call-with-physical-reseed-follow-staging
   owner root-fd record
   (lambda (fd directories layout children stage-check)
     (declare (ignore directories))
     (flet ((verify ()
              (funcall stage-check)
              (unless (equal (getf (getf record :configuration) :entries)
                             (physical-reseed-follow-configuration-current-entries layout children stage-check))
                (error "staged configuration changed during recovery"))))
       (verify)
       (multiple-value-prog1 (funcall function fd #'verify) (verify))))
   check))

(defun call-with-physical-reseed-follow-configuration-recovery-evidence (owner root-fd record function check)
  (case (getf (getf record :configuration) :phase)
    (:building (call-with-physical-reseed-follow-building-evidence owner root-fd record function check))
    (:staged (call-with-physical-reseed-follow-staged-evidence owner root-fd record function check))
    (otherwise (error "unsupported configuration recovery candidate"))))

(defun call-with-physical-reseed-follow-configuration-stable-evidence (owner root-fd record function check)
  (case (getf (getf record :configuration) :phase)
    (:building (call-with-physical-reseed-follow-building-prefix-evidence owner root-fd record function check))
    (:staged (call-with-physical-reseed-follow-staged-evidence owner root-fd record function check))
    (otherwise (error "unsupported stable configuration"))))

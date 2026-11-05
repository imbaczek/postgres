(in-package #:pg)

(defun physical-reseed-follow-retirement-directory-check (root expected fd)
  (let* ((directory (pathname (getf expected :directory)))
         (parent (physical-image-check-directory root))
         (stat (physical-image-check-directory fd (sb-posix:stat-dev parent)))
         (mount (physical-adoption-install-mount-id nil :directory-fd root)))
    (physical-image-check-target directory fd)
    (unless (and (equal (namestring directory) (namestring (truename directory)))
                 (= (getf expected :device) (sb-posix:stat-dev stat))
                 (= (getf expected :inode) (sb-posix:stat-ino stat))
                 (not (= (sb-posix:stat-ino parent) (sb-posix:stat-ino stat)))
                 (= mount (physical-adoption-install-mount-id nil :directory-fd fd)
                    (physical-adoption-install-mount-id directory)))
      (error "retirement directory lost its canonical recorded binding")))
  t)

(defun physical-reseed-follow-retirement-guard-observation (fd expected lock fence)
  (let ((mount (physical-adoption-install-mount-id nil :directory-fd fd)))
    (loop for (name stream key) in (list (list ".pg_lisp.lock" lock :lock-inode)
                                         (list "physical-reseed.install" fence :fence-inode))
          collect
          (let ((row (physical-reseed-install-old-observe fd (copy-seq name) mount))
                (inode (physical-reseed-follow-configuration-guard-facts fd stream name)))
            (unless (and (= inode (getf expected key))
                         (equal row (physical-reseed-install-old-observe fd name mount)))
              (error "retirement guard differs from its recorded inode or stable facts"))
            row))))

(defun call-with-physical-reseed-follow-retirement-directory (root value function parent-check)
  (let ((expected (physical-adoption-attempt-value-copy value))
        (fd nil) (active nil) (thread sb-thread:*current-thread*))
    (unwind-protect
         (progn
           (funcall parent-check)
           (sb-sys:without-interrupts
             (setf fd (physical-backup-open-directory (pathname (getf expected :directory)))))
           (physical-reseed-follow-retirement-directory-check root expected fd)
           (unless (zerop (physical-store-flock fd 6)) (error "retirement directory is already owned"))
           (call-with-physical-reseed-install-guard
            fd ".pg_lisp.lock" nil
            (lambda (lock)
              (call-with-physical-reseed-install-guard
               fd "physical-reseed.install" nil
               (lambda (fence)
                 (let ((guards (physical-reseed-follow-retirement-guard-observation fd expected lock fence)))
                   (flet ((verify ()
                            (unless (and active (eq thread sb-thread:*current-thread*))
                              (error "retirement directory scope ended or changed thread"))
                            (funcall parent-check)
                            (physical-reseed-follow-retirement-directory-check root expected fd)
                            (unless (equal guards (physical-reseed-follow-retirement-guard-observation fd expected lock fence))
                              (error "retirement guards changed during their held scope"))))
                     (unwind-protect
                          (progn
                            (setf active t)
                            (verify)
                            (multiple-value-prog1 (funcall function fd #'verify) (verify)))
                       (setf active nil)))))))))
      (setf active nil)
      (when fd (sb-posix:close fd)))))

(defun physical-reseed-follow-retirement-check-workspace (record plan)
  (let* ((workspace (getf (getf record :configuration) :workspace))
         (directory (pathname (getf workspace :directory))))
    (dolist (other (list (getf plan :incoming) (getf plan :quarantine)))
      (let ((path (pathname (getf other :directory))))
        (when (or (uiop:subpathp directory path) (uiop:subpathp path directory)
                  (and (= (getf workspace :device) (getf other :device))
                       (= (getf workspace :inode) (getf other :inode))))
          (error "retirement workspace overlaps an installation auxiliary")))))
  t)

(defun call-with-physical-reseed-follow-retirement-auxiliaries (root plan function check)
  (call-with-physical-reseed-follow-retirement-directory
   root (getf plan :incoming)
   (lambda (incoming incoming-check)
     (call-with-physical-reseed-follow-retirement-directory
      root (getf plan :quarantine)
      (lambda (quarantine quarantine-check)
        (funcall function incoming quarantine quarantine-check))
      incoming-check))
   check))

(defun physical-reseed-follow-retirement-check-receive (descriptors store record base binding check)
  (funcall check)
  (physical-store-check-live store)
  (physical-store-check-complete-input store)
  (unless (equal (getf binding :after) (physical-store-control-tuple store))
    (error "retirement receive tuple differs from the exact activated selection"))
  (physical-store-read-retained-control store (getf binding :after))
  (physical-store-validate-prefix store)
  (call-with-physical-reseed-follow-held-base-directory
   (physical-reseed-follow-configuration-role-fd descriptors :root)
   (physical-reseed-follow-configuration-role-fd descriptors :base)
   (physical-reseed-follow-configuration-role-fd descriptors :global)
   (physical-reseed-follow-configuration-role-fd descriptors :wal)
   (lambda (directory)
     (let ((*pgdata* directory))
       (physical-follow-check-active-state
        store base (physical-reseed-follow-activation-config record binding))))
   check)
  (funcall check)
  t)

(defun call-with-physical-reseed-follow-retirement-receive
    (descriptors snapshot base binding function check)
  (let* ((record (physical-reseed-follow-copy (getf snapshot :record)))
         (thread sb-thread:*current-thread*) (active nil)
         (store nil) (state nil))
    (unwind-protect
         (progn
           (funcall check)
           (setf state (physical-reseed-follow-retirement-layout-state descriptors snapshot check))
           (physical-store-open
            (physical-image-fd-path (physical-reseed-follow-configuration-role-fd descriptors :receive))
            (getf base :system-id) (getf base :timeline) :start-lsn (getf base :end)
            :discard-tail nil :create-lock nil
            :retained-identity (physical-recovery-retained-identity base)
            :retained-directory (merge-pathnames
                                 "pg_physical_retained/"
                                 (physical-image-fd-path (physical-reseed-follow-configuration-role-fd descriptors :root)))
            :on-open (lambda (opened) (setf store opened)))
           (setf active t)
           (flet ((verify ()
                    (unless (and active (eq thread sb-thread:*current-thread*))
                      (error "retirement observation scope ended or changed thread"))
                    (funcall check)
                    (unless (equal state (physical-reseed-follow-retirement-layout-state descriptors snapshot check))
                      (error "retirement layout changed during its observation scope"))
                    (physical-reseed-follow-retirement-check-receive descriptors store record base binding check)
                    (unless (equal state (physical-reseed-follow-retirement-layout-state descriptors snapshot check))
                      (error "retirement layout changed around active receive validation"))
                    t))
             (verify)
             (multiple-value-prog1
                 (funcall function (copy-tree descriptors) (physical-adoption-attempt-value-copy state) #'verify)
               (verify))))
      (setf active nil)
      (when store (physical-store-close store)))))

(defun call-with-physical-reseed-follow-retirement-observation (root snapshot parent-check function)
  (let* ((snapshot (physical-adoption-attempt-value-copy snapshot))
         (record (physical-reseed-follow-copy (getf snapshot :record)))
         (configuration (getf record :configuration))
         (directories (getf configuration :directories))
         (workspace (append (getf configuration :workspace)
                            (list :lock-inode (getf directories :lock-inode)
                                  :fence-inode (getf directories :fence-inode)))))
    (flet ((root-check ()
             (physical-adoption-check-startup-quiescence)
             (funcall parent-check)
             (let ((local (getf (getf record :attempt) :local))
                   (stat (physical-image-check-directory root)))
               (physical-image-check-target (pathname (getf local :directory)) root)
               (unless (and (= (getf local :device) (sb-posix:stat-dev stat))
                            (= (getf local :inode) (sb-posix:stat-ino stat))
                            (= (physical-adoption-install-mount-id (pathname (getf local :directory)))
                               (physical-adoption-install-mount-id nil :directory-fd root)))
                 (error "retirement root differs from its recorded named descriptor")))
             (physical-reseed-follow-retirement-extra-at root snapshot)))
      (root-check)
      (call-with-physical-reseed-follow-retirement-directory
       root workspace
       (lambda (workspace-fd workspace-check)
         (call-with-physical-reseed-follow-fixed-configuration-directories
          workspace-fd record
          (lambda (staged saved fixed-check)
            (let ((plan (physical-reseed-follow-retirement-provisional-plan-at root saved record)))
              (physical-reseed-follow-retirement-check-workspace record plan)
              (call-with-physical-reseed-follow-retirement-auxiliaries
               root plan
               (lambda (incoming quarantine auxiliary-check)
                 (call-with-physical-reseed-follow-base-directories
                  root plan
                  (lambda (base-fds base-check)
                    (let ((descriptors (append (list (cons :root root) (cons :workspace workspace-fd)
                                                     (cons :staged staged) (cons :saved saved)
                                                     (cons :incoming incoming) (cons :quarantine quarantine))
                                               base-fds)))
                      (multiple-value-bind (base control binding model images row)
                          (physical-reseed-follow-configuration-base-proof
                           root saved base-fds plan record (getf configuration :step) base-check)
                        (declare (ignore control model images row))
                        (call-with-physical-reseed-follow-installation-receive
                         descriptors record binding (getf configuration :step)
                         (lambda (held)
                           (call-with-physical-reseed-follow-retirement-receive
                            held snapshot base binding function base-check))
                         base-check))))
                  auxiliary-check))
               fixed-check)))
          workspace-check))
       #'root-check))))

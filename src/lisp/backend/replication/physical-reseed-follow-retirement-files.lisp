(in-package #:pg)

(defun physical-reseed-follow-retirement-attempt-at (root-fd container-fd value prefix)
  ;; Observe the exact original premanifest attempt at the prefix location.
  ;; Check the root binding, all three mounts, and the attempt file identity
  ;; and nanosecond facts.
  ;; The row maps UID before link count in the attempt identity.  A frozen
  ;; reference must match, allowing ctime to change after the move to saved.
  ;; This observation alone does not establish the saved role, the complete
  ;; namespace, or authority for an operation or startup.
  (let* ((record (physical-reseed-follow-copy value))
         (expected (getf record :attempt))
         (retirement (getf record :retirement))
         (reference (getf retirement :attempt-file))
         (root (sb-posix:fstat root-fd))
         (container (sb-posix:fstat container-fd))
         (local (getf expected :local))
         (same (and (= (sb-posix:stat-dev root) (sb-posix:stat-dev container))
                    (= (sb-posix:stat-ino root) (sb-posix:stat-ino container)))))
    (physical-reseed-follow-retirement-check-prefix record prefix)
    (unless (and (eq same (< prefix 2))
                 (= (getf local :device) (sb-posix:stat-dev root))
                 (= (getf local :inode) (sb-posix:stat-ino root)))
      (error "retirement attempt container disagrees with its prefix or PGDATA binding"))
    (let* ((mount (physical-adoption-install-mount-id nil :directory-fd root-fd))
           (name (copy-seq "physical-reseed.attempt"))
           (row (physical-reseed-install-old-observe container-fd name mount))
           (identity (list (second row) (third row) (fourth row)
                           (sixth row) (fifth row) (seventh row)
                           (floor (eighth row) 1000000000)
                           (floor (ninth row) 1000000000))))
      (unless (= mount (physical-adoption-install-mount-id nil :directory-fd container-fd))
        (error "retirement attempt container crosses mounts"))
      (let ((state
              (physical-reseed-attempt-read-state-at
               container-fd name
               :check-fd (lambda (fd)
                           (unless (and (= mount (physical-adoption-install-mount-id nil :directory-fd fd))
                                        (equal row (physical-reseed-install-old-observe container-fd name mount))
                                        (equal identity (physical-reseed-attempt-file-identity (sb-posix:fstat fd))))
                             (error "retirement attempt changed its held identity or nanosecond facts"))))))
        (unless (and (eq :premanifest (getf (getf state :record) :phase))
                     (equal expected (getf state :record)))
          (error "retirement requires the exact active original attempt"))
        (when reference
          (unless (and (= (getf reference :digest) (getf state :digest))
                       (if same
                           (equal (getf reference :identity) (getf state :identity))
                           (equal (butlast (getf reference :identity)) (butlast (getf state :identity)))))
            (error "retirement differs from its frozen attempt file reference")))
        (append state (list :row row))))))

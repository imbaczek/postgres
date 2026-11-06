(in-package #:pg)

(defun physical-reseed-follow-retirement-held-file-facts (fd mount)
  ;; Read complete identity and nanosecond facts from the held descriptor on
  ;; the expected mount.
  (sb-alien:with-alien ((buffer (sb-alien:array sb-alien:unsigned-char 256)))
    (unless (zerop (physical-adoption-statx
                    fd "" +physical-adoption-at-empty-path+ #x17ff
                    (sb-alien:cast (sb-alien:addr buffer) (* sb-alien:unsigned-char))))
      (error "cannot inspect held retirement file: errno ~d" (sb-alien:get-errno)))
    (let ((sap (sb-alien:alien-sap (sb-alien:addr buffer))))
      (unless (and (= #x13cf (logand #x13cf (sb-sys:sap-ref-32 sap 0)))
                   (= mount (sb-sys:sap-ref-64 sap #x90)))
        (error "held retirement file requires complete facts on the target mount"))
      (list (physical-reseed-install-statx-device sap)
            (sb-sys:sap-ref-64 sap #x20) (sb-sys:sap-ref-16 sap #x1c)
            (sb-sys:sap-ref-32 sap #x10) (sb-sys:sap-ref-32 sap #x14)
            (sb-sys:sap-ref-64 sap #x28)
            (physical-reseed-install-statx-time sap #x70)
            (physical-reseed-install-statx-time sap #x60)))))

(defun physical-reseed-follow-retirement-journal-state-at (root stream name)
  ;; Bind the held descriptor to the named journal while reading its exact
  ;; bytes.  Return the decoded state, stable identity and named facts.
  (let* ((fd (sb-sys:fd-stream-fd stream))
         (mount (physical-adoption-install-mount-id nil :directory-fd root))
         (noun (if (equal name "physical-reseed.follow.tmp") "temporary" "final journal")))
    (multiple-value-bind (identity row) (physical-reseed-follow-entry root name)
      (unless (and identity
                   (equal identity (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                   (= mount (physical-adoption-install-mount-id nil :directory-fd fd)))
        (error "retirement ~A differs from its held descriptor" noun))
      (file-position stream 0)
      (let ((bytes (make-array (sixth identity) :element-type '(unsigned-byte 8))))
        (unless (and (= (length bytes) (read-sequence bytes stream))
                     (eq :eof (read-byte stream nil :eof))
                     (equal identity (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                     (= mount (physical-adoption-install-mount-id nil :directory-fd fd))
                     (equal (list identity row)
                            (multiple-value-list (physical-reseed-follow-entry root name))))
          (error "retirement ~A changed during its held read" noun))
        (values (list :record (physical-reseed-follow-decode bytes) :identity identity :row row)
                bytes)))))

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

(defun physical-reseed-follow-retirement-install-at (descriptors value prefix)
  ;; Observe the install journal at the prefix location and compare it with
  ;; the frozen terminal reference.  Saved permits the rename ctime change.
  (let* ((record (physical-reseed-follow-copy value))
         (root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (parent (if (zerop prefix) root saved))
         (name (copy-seq "physical-reseed.install"))
         (mount (physical-adoption-install-mount-id nil :directory-fd root))
         (row (physical-reseed-install-old-observe parent name mount)))
    (physical-reseed-follow-retirement-check-prefix record prefix)
    (let* ((state (if (zerop prefix)
                      (physical-reseed-install-read-at root name)
                      (physical-reseed-install-read-archive-at root saved)))
           (actual (physical-reseed-follow-terminal-reference state))
           (expected (getf record :install)))
      (physical-reseed-follow-configuration-install-plan (getf state :plan) record)
      (unless (and (equal row (physical-reseed-install-old-observe parent name mount))
                   (equal (physical-reseed-install-row-file-facts row) (getf state :identity))
                   (every (lambda (key) (eql (getf expected key) (getf actual key)))
                          '(:digest :body-crc :step))
                   (if (zerop prefix)
                       (equal (getf expected :identity) (getf actual :identity))
                       (equal (butlast (getf expected :identity)) (butlast (getf actual :identity)))))
        (error "retirement install differs from its frozen reference or stable file facts"))
      (append state (list :row row)))))

(defun physical-reseed-follow-retirement-provisional-plan-at (root saved value)
  ;; Read one root or saved install journal to obtain a provisional plan.
  ;; The complete layout proof later establishes the location and saved role.
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation))
         (present (loop for fd in (list root saved)
                        when (physical-adoption-path-present-p
                              (merge-pathnames "physical-reseed.install" (physical-image-fd-path fd)))
                          collect fd)))
    (unless (and (eq :active (getf activation :phase)) (= 2 (getf activation :step)))
      (error "retirement requires completed activation"))
    (unless (= 1 (length present)) (error "retirement install requires one root or saved location"))
    (let* ((state (if (= root (first present))
                      (physical-reseed-install-read-at root "physical-reseed.install")
                      (physical-reseed-install-read-archive-at root saved)))
           (plan (getf state :plan)))
      (physical-reseed-follow-configuration-install-plan plan record)
      plan)))

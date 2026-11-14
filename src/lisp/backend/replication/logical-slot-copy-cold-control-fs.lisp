(in-package #:pg)

;; Descriptor-relative control-file operations for a single cold attempt.
;; Check the held directory and bound files at each phase.  Sync the old and
;; temporary files before rename, then sync the installed file and directory.
;; Owned descriptor ledgers remove an entry before close, so a close failure
;; cannot cause a later cleanup to close a reused descriptor.
(defun logical-slot-copy-cold-control-file-check (attempt runtime old temporary)
  (logical-slot-copy-cold-control-check-metadata attempt runtime)
  (let* ((b (logical-slot-copy-cold-control-attempt-binding attempt))
         (global (logical-slot-copy-cold-control-runtime-global runtime)))
    (when old
      (let* ((before (getf b :canonical-control-facts))
             (now (physical-image-file-facts old))
             (replaced (and (eq :parent (getf b :control-phase))
                            (eq :child (logical-slot-copy-cold-control-runtime-phase runtime)))))
        (unless (and (if replaced
                         (and (zerop (fourth now))
                              (logical-slot-copy-cold-facts-match-p before now '(3 7)))
                         (equal before now))
                     (equalp (getf b :canonical-control)
                             (logical-slot-copy-import-control-read-fd old (length (getf b :canonical-control)))))
          (error "cold publication original control witness changed"))))
    (when temporary
      (let ((name (if (eq :installed (logical-slot-copy-cold-control-runtime-temporary-kind runtime))
                      "pg_wal_control" (logical-slot-copy-cold-control-temporary-name runtime)))
            (facts (logical-slot-copy-cold-control-runtime-temporary-facts runtime))
            (bytes (logical-slot-copy-cold-control-runtime-temporary-bytes runtime)))
        (unless (and (equal facts (physical-image-file-facts temporary))
                     (if name
                         (and (logical-slot-copy-import-control-private-facts-p
                               facts (physical-image-file-facts global))
                              (equal facts (physical-image-file-facts global name)))
                         (zerop (fourth facts)))
                     (equalp bytes (logical-slot-copy-import-control-read-fd temporary (length bytes))))
          (error "cold publication temporary witness changed")))))
  t)

(defun logical-slot-copy-cold-control-create-file (attempt runtime acquire)
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (let* ((b (logical-slot-copy-cold-control-attempt-binding attempt))
         (global (logical-slot-copy-cold-control-runtime-global runtime)))
    (unless (and (eq :parent (logical-slot-copy-cold-control-runtime-phase runtime))
                 (eq :absent (logical-slot-copy-cold-control-runtime-temporary-kind runtime))
                 (logical-slot-copy-cold-control-runtime-child-complete runtime)
                 (logical-slot-copy-cold-control-runtime-data-synced runtime)
                 (equalp (getf b :catalog) (logical-slot-copy-cold-control-runtime-catalog-bytes runtime)))
      (error "cold control creation requires complete synced child data and catalog"))
    (sb-sys:without-interrupts
      (funcall (logical-slot-copy-cold-control-attempt-mark-started attempt))
      (let ((fd (funcall acquire :temp "pg_wal_control.tmp"
                         (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl) #o600)))
        (setf (logical-slot-copy-cold-control-runtime-temporary-kind runtime) :writing
              (logical-slot-copy-cold-control-runtime-temporary-bytes runtime)
              (make-array 0 :element-type '(unsigned-byte 8))
              (logical-slot-copy-cold-control-runtime-temporary-facts runtime) (physical-image-file-facts fd))
        (unless (and (zerop (sixth (physical-image-file-facts fd)))
                     (logical-slot-copy-import-control-private-facts-p
                      (physical-image-file-facts fd) (physical-image-file-facts global)))
          (error "cold control creation did not return an empty private inode"))))
    (logical-slot-copy-cold-control-advance-directory attempt runtime)))

(defun logical-slot-copy-cold-control-write-file (attempt runtime fd)
  (declare (notinline sb-posix:write))
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (unless (and fd (eq :writing (logical-slot-copy-cold-control-runtime-temporary-kind runtime)))
    (error "cold control write has no owned empty temporary"))
  (let ((bytes (getf (logical-slot-copy-cold-control-attempt-binding attempt) :child-control))
        (before (logical-slot-copy-cold-control-runtime-temporary-facts runtime)))
    (funcall (logical-slot-copy-cold-control-attempt-mark-started attempt))
    (unless (zerop (sb-posix:lseek fd 0 sb-posix:seek-set)) (error "cannot position cold control temporary"))
    (sb-sys:with-pinned-objects (bytes)
      (loop with offset = 0 while (< offset (length bytes))
            for count = (sb-posix:write fd (sb-sys:sap+ (sb-sys:vector-sap bytes) offset)
                                        (- (length bytes) offset))
            do (unless (<= 1 count (- (length bytes) offset)) (error "short cold control write"))
               (incf offset count)))
    (sb-sys:without-interrupts
      (let ((after (physical-image-file-facts fd)))
        (unless (and (= (sixth after) (length bytes))
                     (logical-slot-copy-cold-facts-match-p before after '(5 6 7)))
          (error "cold control write changed its inode"))
        (setf (logical-slot-copy-cold-control-runtime-temporary-facts runtime) after
              (logical-slot-copy-cold-control-runtime-temporary-bytes runtime) (copy-seq bytes)
              (logical-slot-copy-cold-control-runtime-temporary-kind runtime) :ready
              (logical-slot-copy-cold-control-runtime-temp-synced runtime) nil
              (logical-slot-copy-cold-control-runtime-old-synced runtime) nil)))))

(defun logical-slot-copy-cold-control-remove-file (attempt runtime fd)
  (declare (notinline port-unlinkat))
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (unless (and fd (eq :parent (logical-slot-copy-cold-control-runtime-phase runtime))
               (eq :control (logical-slot-copy-cold-control-runtime-temporary-kind runtime))
               (logical-slot-copy-cold-control-runtime-child-complete runtime)
               (equalp (getf (logical-slot-copy-cold-control-attempt-binding attempt) :catalog)
                       (logical-slot-copy-cold-control-runtime-catalog-bytes runtime)))
    (error "cold control residue removal requires exact R catalog and complete child"))
  (let ((before (physical-image-file-facts fd)))
    (sb-sys:without-interrupts
      (funcall (logical-slot-copy-cold-control-attempt-mark-started attempt))
      (unless (zerop (port-unlinkat (logical-slot-copy-cold-control-runtime-global runtime)
                                    "pg_wal_control.tmp" 0))
        (error "cannot remove authenticated cold control residue"))
      (setf (logical-slot-copy-cold-control-runtime-temporary-kind runtime) :absent)
      (let ((after (physical-image-file-facts fd)))
        (unless (and (zerop (fourth after))
                     (logical-slot-copy-cold-facts-match-p before after '(3 7)))
          (error "cold control residue inode changed during unlink"))
        (setf (logical-slot-copy-cold-control-runtime-temporary-facts runtime) after)))
    (logical-slot-copy-cold-control-advance-directory attempt runtime)))

(defun logical-slot-copy-cold-control-sync-file (attempt runtime operation old temporary)
  (declare (notinline sb-posix:fsync))
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (let* ((phase (logical-slot-copy-cold-control-runtime-phase runtime))
         (kind (logical-slot-copy-cold-control-runtime-temporary-kind runtime))
         (fd (ecase operation
               (:sync-old
                (unless (and (eq phase :parent) (eq kind :ready)
                             (logical-slot-copy-cold-control-runtime-data-synced runtime))
                  (error "cold old-control sync has the wrong phase"))
                old)
               (:sync-temp
                (unless (and (eq phase :parent) (eq kind :ready)
                             (logical-slot-copy-cold-control-runtime-data-synced runtime))
                  (error "cold temporary sync has the wrong phase"))
                temporary)
               (:sync-installed
                (unless (eq phase :child) (error "cold installed sync requires child control"))
                (if (eq kind :installed) temporary old))
               (:sync-directory
                (when (and (eq phase :child)
                           (not (logical-slot-copy-cold-control-runtime-file-synced runtime)))
                  (error "cold directory sync requires synced installed control"))
                (logical-slot-copy-cold-control-runtime-global runtime)))))
    (unless fd (error "cold control sync lost its owned descriptor"))
    (sb-sys:without-interrupts
      (funcall (logical-slot-copy-cold-control-attempt-mark-started attempt))
      (sb-posix:fsync fd)
      (ecase operation
        (:sync-old (setf (logical-slot-copy-cold-control-runtime-old-synced runtime) t))
        (:sync-temp (setf (logical-slot-copy-cold-control-runtime-temp-synced runtime) t))
        (:sync-installed (setf (logical-slot-copy-cold-control-runtime-file-synced runtime) t))
        (:sync-directory
         (when (eq phase :child)
           (setf (logical-slot-copy-cold-control-runtime-directory-synced runtime) t)))))))

(defun logical-slot-copy-cold-control-rename-file (attempt runtime old temporary)
  (declare (notinline port-renameat))
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (unless (and old temporary (eq :parent (logical-slot-copy-cold-control-runtime-phase runtime))
               (eq :ready (logical-slot-copy-cold-control-runtime-temporary-kind runtime))
               (logical-slot-copy-cold-control-runtime-old-synced runtime)
               (logical-slot-copy-cold-control-runtime-temp-synced runtime)
               (logical-slot-copy-cold-control-runtime-data-synced runtime)
               (logical-slot-copy-cold-control-runtime-child-complete runtime))
    (error "cold control rename requires complete pre-synced publication"))
  (let ((before (physical-image-file-facts temporary))
        (global (logical-slot-copy-cold-control-runtime-global runtime)))
    (sb-sys:without-interrupts
      (funcall (logical-slot-copy-cold-control-attempt-mark-started attempt))
      (unless (zerop (port-renameat global "pg_wal_control.tmp" global "pg_wal_control"))
        (error "cannot install cold child control"))
      (setf (logical-slot-copy-cold-control-runtime-phase runtime) :child
            (logical-slot-copy-cold-control-runtime-temporary-kind runtime) :installed
            (logical-slot-copy-cold-control-runtime-control-bytes runtime)
            (getf (logical-slot-copy-cold-control-attempt-binding attempt) :child-control)
            (logical-slot-copy-cold-control-runtime-file-synced runtime) nil
            (logical-slot-copy-cold-control-runtime-directory-synced runtime) nil)
      (let ((after (physical-image-file-facts temporary)))
        (unless (logical-slot-copy-cold-facts-match-p before after '(7))
          (error "cold child control installation changed its owned inode"))
        (setf (logical-slot-copy-cold-control-runtime-control-facts runtime) after
              (logical-slot-copy-cold-control-runtime-temporary-facts runtime) after))
      (logical-slot-copy-cold-control-advance-directory attempt runtime))))

;; Create the private dispatcher and its close function for this attempt.
;; Only the registered subject can use the fixed operations, and mutations
;; require an active source check.  The lexical ledger takes ownership as each
;; descriptor is opened and removes ownership before closing it.
(defun logical-slot-copy-cold-control-file-operations (attempt-reader runtime)
  (declare (notinline physical-image-openat sb-posix:close))
  (let ((owned nil) (active t))
    (labels ((descriptor (key) (cdr (assoc key owned)))
             (acquire (key name flags mode)
               (when (assoc key owned) (error "cold control descriptor is already owned"))
               (let* ((entry (cons key nil)) (link (list entry)) (fd nil))
                 (sb-sys:without-interrupts
                   (setf fd (physical-image-openat
                             (logical-slot-copy-cold-control-runtime-global runtime) name
                             (logior flags sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) mode))
                   (when (minusp fd) (error "cannot acquire cold control descriptor"))
                   (setf (cdr entry) fd (cdr link) owned owned link))
                 fd))
             (close-temp ()
               (let ((entry (assoc :temp owned)))
                 (when entry
                   (setf owned (delete entry owned :test #'eq))
                   (sb-posix:close (cdr entry)))))
             (close-all ()
               (let ((closing owned))
                 (setf active nil owned nil)
                 (physical-image-drain-descriptors closing nil)))
             (operate (operation subject)
               (let ((attempt (funcall attempt-reader)))
                 (unless (and active (eq subject attempt)) (error "cold control descriptor scope ended or differs"))
                 (with-logical-slot-copy-cold-control-guard (attempt)
                   (logical-slot-copy-cold-control-runtime-record attempt runtime)
                   (logical-slot-copy-cold-control-file-check attempt runtime (descriptor :old) (descriptor :temp))
                   (when (member operation '(:create :write :remove-residue :sync-old :sync-temp
                                             :rename :sync-installed :sync-directory))
                     (unless (logical-slot-copy-cold-control-runtime-source-active runtime)
                       (error "cold control mutation source expired"))
                     (funcall (logical-slot-copy-cold-control-attempt-source-check attempt)))
                   (case operation
                     (:check nil)
                     (:open-control (acquire :old "pg_wal_control" sb-posix:o-rdwr 0))
                     (:open-residue
                      (unless (eq :control (logical-slot-copy-cold-control-runtime-temporary-kind runtime))
                        (error "cold residue open has the wrong phase"))
                      (acquire :temp "pg_wal_control.tmp" sb-posix:o-rdonly 0))
                     (:create (logical-slot-copy-cold-control-create-file attempt runtime #'acquire))
                     (:write (logical-slot-copy-cold-control-write-file attempt runtime (descriptor :temp)))
                     (:remove-residue
                      (logical-slot-copy-cold-control-remove-file attempt runtime (descriptor :temp)))
                     (:close-temp
                      (unless (and (eq :parent (logical-slot-copy-cold-control-runtime-phase runtime))
                                   (eq :absent (logical-slot-copy-cold-control-runtime-temporary-kind runtime)))
                        (error "cold temporary close requires its unlinked residue"))
                      (close-temp))
                     ((:sync-old :sync-temp :sync-installed :sync-directory)
                      (logical-slot-copy-cold-control-sync-file
                       attempt runtime operation (descriptor :old) (descriptor :temp)))
                     (:rename
                      (logical-slot-copy-cold-control-rename-file
                       attempt runtime (descriptor :old) (descriptor :temp)))
                     (otherwise (error "unknown cold control descriptor operation")))
                   (logical-slot-copy-cold-control-file-check attempt runtime (descriptor :old) (descriptor :temp))))))
      (values #'operate #'close-all))))

(in-package #:pg)

(defun physical-reseed-follow-configuration-role-fd (descriptors role)
  (let ((entries (remove-if-not (lambda (entry) (eq role (car entry))) descriptors)))
    (unless (and (= 1 (length entries)) (typep (cdar entries) '(integer 0 *)))
      (error "configuration requires one held directory for role ~a" role))
    (cdar entries)))

(defun physical-reseed-follow-configuration-directory-binding (descriptors plan record positions role)
  (let* ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (configuration (getf record :configuration))
         (directories (getf configuration :directories)))
    (cond
      ((member role '(:root :incoming :quarantine :workspace))
       (let ((identity (ecase role
                         (:root (getf (getf plan :attempt) :local))
                         (:incoming (getf plan :incoming)) (:quarantine (getf plan :quarantine))
                         (:workspace (getf configuration :workspace)))))
         (values root (string-right-trim "/" (getf identity :directory))
                 (getf identity :device) (getf identity :inode))))
      ((member role '(:base :global :wal))
       (let* ((name (ecase role (:base "base") (:global "global") (:wal "pg_wal")))
              (row (assoc name (getf plan :new) :test #'equal)))
         (values root name (second row) (third row))))
      ((member role '(:staged :saved))
       (let ((identity (getf directories role)))
         (values (physical-reseed-follow-configuration-role-fd descriptors :workspace)
                 (ecase role (:staged "staged") (:saved "saved"))
                 (getf identity :device) (getf identity :inode))))
      ((eq role :receive)
       (let ((position (assoc :receive-directory positions)) (identity (getf directories :receive)))
         (values (physical-reseed-follow-configuration-role-fd descriptors (second position))
                 (third position) (getf identity :device) (getf identity :inode))))
      (t (error "unknown configuration directory role")))))

(defun physical-reseed-follow-configuration-directory-rows (descriptors plan record positions mount)
  (loop for role in '(:root :incoming :quarantine :base :global :wal :workspace :staged :saved :receive)
        for fd = (physical-reseed-follow-configuration-role-fd descriptors role)
        collect
        (multiple-value-bind (parent leaf device inode)
            (physical-reseed-follow-configuration-directory-binding descriptors plan record positions role)
          (let ((stat (physical-image-check-directory fd))
                (self (physical-reseed-install-old-observe fd "." mount))
                (named (physical-reseed-install-old-observe parent leaf mount)))
            (unless (and (= device (sb-posix:stat-dev stat)) (= inode (sb-posix:stat-ino stat))
                         (= mount (physical-adoption-install-mount-id nil :directory-fd fd))
                         (equal (rest self) (rest named))
                         (equal (physical-reseed-install-row-file-facts self) (physical-reseed-file-facts stat)))
              (error "configuration directory lost its recorded named binding: ~a" role))
            (cons role self)))))

(defun physical-reseed-follow-configuration-original-location (descriptors positions name)
  (cond
    ((equal name "physical-recovery.control")
     (let ((position (assoc :original-recovery positions)))
       (values (physical-reseed-follow-configuration-role-fd descriptors (second position)) (third position) t)))
    ((position #\/ name)
     (let* ((slash (position #\/ name)) (directory (subseq name 0 slash))
                                        (role (cond ((equal directory "base") :base) ((equal directory "global") :global)
                                                    ((equal directory "pg_wal") :wal) (t (error "unknown original image directory")))))
       (values (physical-reseed-follow-configuration-role-fd descriptors role) (subseq name (1+ slash)) nil)))
    (t (values (physical-reseed-follow-configuration-role-fd descriptors :root) name t))))

;; Top-level incoming rows have already moved to root, so their original ctime
;; comparison uses the inherited rename allowance; nested rows remain strict.
;; Inserting selected history can also change pg_wal's size and times.  Exact
;; children, bytes, identities and stable current rows are checked separately.
(defun physical-reseed-follow-configuration-original-rows (descriptors plan positions mount)
  (let ((history (assoc "history.wal" positions :test #'equal)))
    (loop for expected in (getf plan :new)
          for name = (first expected)
          collect
          (multiple-value-bind (parent leaf renamed)
              (physical-reseed-follow-configuration-original-location descriptors positions name)
            (let* ((directory (sb-posix:s-isdir (fourth expected)))
                   (current (if directory
                                (append (physical-reseed-install-old-observe parent leaf mount) (list nil))
                                (physical-reseed-install-file-row-at
                                 parent leaf name (seventh expected) (tenth expected) mount nil)))
                   (selected-wal (and directory (equal name "pg_wal") (eq :wal (second history)))))
              (unless (if selected-wal
                          (equal (subseq expected 0 6) (subseq current 0 6))
                          (physical-reseed-install-namespace-row-matches-p expected current renamed))
                (error "original image metadata differs at its prescribed configuration location"))
              current)))))

(defun physical-reseed-follow-configuration-image-rows (descriptors record positions images mount)
  (loop for (logical . bytes) in images
        for position = (assoc logical positions :test #'equal)
        for parent = (physical-reseed-follow-configuration-role-fd descriptors (second position))
        for leaf = (third position)
        for before = (physical-reseed-install-old-observe parent leaf mount)
        for row = (physical-reseed-follow-configuration-file-row parent leaf logical bytes)
        for expected = (first (second (assoc logical (getf (getf record :configuration) :entries) :test #'equal)))
        do (unless (and (equal row expected)
                        (equal before (physical-reseed-install-old-observe parent leaf mount)))
             (error "derived configuration file lost its recorded identity or stable metadata"))
        collect (list logical parent leaf before)))

(defun physical-reseed-follow-layout-proof
    (owner descriptors plan record positions images extra expected mount check &optional active-row)
  ;; Callers admit the owner, run the first check, and supply fixed positions
  ;; and inventories.  Capture the namespace, run the second check, and then
  ;; recheck the captured facts.  When supplied, the read-only active observer
  ;; runs twice and adds :active to the result; the old wrapper omits it.
  ;; Rows borrow parent directory descriptors.  These observations can become
  ;; stale after return and do not by themselves authorize filesystem changes.
  (let ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
        (quarantine (physical-reseed-follow-configuration-role-fd descriptors :quarantine)))
    (unless (= (length descriptors) 10) (error "mapped layout requires exactly ten directory roles"))
    (unless (equal (physical-reseed-follow-configuration-image-specifications
                    images (physical-reseed-follow-core-projection record))
                   (getf (getf record :configuration) :images))
      (error "mapped layout images differ from their committed specifications"))
    (flet ((names ()
             (loop for (role entries) on expected by #'cddr
                   append (list role
                                (physical-reseed-install-directory-names
                                 (physical-reseed-follow-configuration-role-fd descriptors role))))))
      (let* ((directories (physical-reseed-follow-configuration-directory-rows descriptors plan record positions mount))
             (old (physical-reseed-install-namespace-old-rows
                   plan (1- (physical-reseed-install-last-step plan)) root quarantine mount)))
        (unless (equal expected (names)) (error "mapped configuration directory inventory differs"))
        (let ((new (physical-reseed-follow-configuration-original-rows descriptors plan positions mount))
              (derived (physical-reseed-follow-configuration-image-rows descriptors record positions images mount))
              (active (when active-row (funcall active-row))))
          (funcall check)
          (physical-reseed-check-owner owner)
          (physical-reseed-install-namespace-check-new-rows
           new mount (lambda (name)
                       (physical-reseed-follow-configuration-original-location descriptors positions name)))
          (dolist (entry (if active-row (append derived (list active)) derived))
            (unless (equal (fourth entry) (physical-reseed-install-old-observe (second entry) (third entry) mount))
              (error "derived configuration file changed during the layout proof")))
          (unless (and (or (null active-row) (equal active (funcall active-row)))
                       (equal extra (physical-reseed-install-extra-entries owner root))
                       (equal old (physical-reseed-install-namespace-old-rows
                                   plan (1- (physical-reseed-install-last-step plan)) root quarantine mount))
                       (equal directories (physical-reseed-follow-configuration-directory-rows
                                           descriptors plan record positions mount))
                       (equal expected (names)))
            (error "mapped configuration layout changed during verification"))
          (append (list :new new :images derived)
                  (when active-row (list :active active))
                  (list :old old :directories directories :names expected)))))))

;; Prove the complete mapped namespace at one prefix, including unused images.
;; Check identities, contents and exact child names again around the proof.
(defun physical-reseed-follow-configuration-layout-state (owner descriptors value binding images prefix check)
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "mapped layout requires configuration ownership"))
  (physical-reseed-check-owner owner)
  (funcall check)
  (multiple-value-bind (plan record)
      (physical-reseed-follow-configuration-install-plan (physical-reseed-install-owner-plan owner) value)
    (let* ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
           (mount (physical-adoption-install-mount-id nil :directory-fd root))
           (positions (physical-reseed-follow-configuration-positions record binding prefix))
           (extra (physical-reseed-install-extra-entries owner root))
           (expected (physical-reseed-follow-configuration-inventories plan record binding prefix extra)))
      (physical-reseed-follow-layout-proof
       owner descriptors plan record positions images extra expected mount check))))

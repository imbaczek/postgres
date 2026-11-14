(in-package #:pg)

(defstruct (logical-slot-copy-install-stage
            (:constructor %make-logical-slot-copy-install-stage)
            (:print-function physical-libpq-print-private-object))
  "Private evidence for a staged installation, valid only within its scope."
  (owner nil :read-only t) (context nil :read-only t) (attempt nil :read-only t)
  (nodes nil :read-only t) (value nil :read-only t) (image nil :read-only t))

(defvar *logical-slot-copy-install-stages* nil)

(defun logical-slot-copy-install-stage-node-image (nodes)
  "Capture node, parent, name, and identity metadata."
  (unless (and (= 18 (logical-slot-copy-descriptor-list-count nodes 18))
               (equal (loop for key in nodes by #'cddr collect key)
                      '(:root :work :slot-root :slot-work :container :slot-container
                        :journal :pending-file :active-file)))
    (error "invalid logical installation stage nodes"))
  (loop for (key node) on nodes by #'cddr
        collect (list key (logical-slot-copy-install-node-fd node)
                      (when (logical-slot-copy-install-node-parent node)
                        (logical-slot-copy-install-node-fd (logical-slot-copy-install-node-parent node)))
                      (copy-seq (logical-slot-copy-install-node-name node))
                      (logical-slot-copy-install-node-kind node)
                      (copy-list (logical-slot-copy-install-node-identity node)))))

(defun logical-slot-copy-install-create-row (container root row)
  "Write, verify, and sync one certified slot, then close its descriptors."
  (let* ((name (first row)) (bytes (string->bytes (fourth row))))
    (slot-state-check-bytes bytes)
    (let* ((directory (logical-slot-copy-install-open-node container name :directory root t t))
           (state (logical-slot-copy-install-open-node directory "state" :file root t t))
           (identity (list (copy-seq name) (copy-list (logical-slot-copy-install-node-identity directory))
                           (copy-list (logical-slot-copy-install-node-identity state)))))
      (logical-slot-copy-install-write-node state bytes)
      (unless (equalp bytes (logical-slot-copy-install-read-node state (length bytes)))
        (error "logical installation staged slot differs from its certificate"))
      (logical-slot-copy-install-sync-node directory)
      (logical-slot-copy-install-sync-node container)
      (logical-slot-copy-install-close-node state)
      (logical-slot-copy-install-close-node directory)
      identity)))

(defun logical-slot-copy-install-observe-row (container root row expected)
  "Reopen one slot by its recorded identities and check its inventory and bytes."
  (let* ((directory (logical-slot-copy-install-open-node
                     container (first row) :directory root nil nil (second expected)))
         (state (logical-slot-copy-install-open-node directory "state" :file root nil nil (third expected)))
         (bytes (string->bytes (fourth row))))
    (unless (and (equal '("state") (directory-entry-names
                                    (physical-image-fd-path (logical-slot-copy-install-node-fd directory))
                                    :follow-symlinks t :maximum-entries 2))
                 (equalp bytes (logical-slot-copy-install-read-node state (length bytes))))
      (error "logical installation slot inventory or bytes changed"))
    (let ((facts (list (copy-seq (first row))
                       (physical-image-file-facts (logical-slot-copy-install-node-fd directory))
                       (physical-image-file-facts (logical-slot-copy-install-node-fd state)))))
      (logical-slot-copy-install-close-node state)
      (logical-slot-copy-install-close-node directory)
      facts)))

(defun logical-slot-copy-install-stage-observations (nodes value certificate)
  "Capture full inode facts and check the staged inventories and slot rows."
  (let ((container (getf nodes :container)) (slots (getf nodes :slot-container)))
    (unless (and (equal '("active" "journal")
                        (sort (directory-entry-names
                               (physical-image-fd-path (logical-slot-copy-install-node-fd container))
                               :follow-symlinks t :maximum-entries 3) #'string<))
                 (equal (mapcar #'first (getf certificate :slots))
                        (sort (directory-entry-names
                               (physical-image-fd-path (logical-slot-copy-install-node-fd slots))
                               :follow-symlinks t :maximum-entries (1+ +logical-slot-copy-descriptor-maximum-slots+))
                              #'string<)))
      (error "logical installation container inventory changed"))
    (list
     (loop for (key node) on nodes by #'cddr
           collect (list key (physical-image-file-facts (logical-slot-copy-install-node-fd node))))
     (loop for row in (getf certificate :slots) for expected in (getf value :entries)
           collect (logical-slot-copy-install-observe-row slots (getf nodes :slot-root) row expected)))))

(defun logical-slot-copy-install-check-stage (owner context attempt stage)
  "Recheck stage authority, exact images, bindings, inventories, and inode facts."
  (let ((record (assoc stage *logical-slot-copy-install-stages* :test #'eq)))
    (unless (and record (logical-slot-copy-install-stage-p stage)
                 (eq owner (logical-slot-copy-install-stage-owner stage))
                 (eq context (logical-slot-copy-install-stage-context stage))
                 (eq attempt (logical-slot-copy-install-stage-attempt stage)))
      (error "logical installation stage scope ended or changed owner"))
    (logical-slot-copy-install-check-preparation owner context attempt)
    (let* ((nodes (logical-slot-copy-install-stage-nodes stage))
           (expected (second record))
           (value (logical-slot-copy-install-decode expected))
           (certificate (logical-slot-copy-install-preparation-certificate attempt))
           (active (logical-slot-copy-install-active-image certificate)))
      (unless (and (equalp expected (logical-slot-copy-install-stage-image stage))
                   (equalp expected (logical-slot-copy-install-encode (logical-slot-copy-install-stage-value stage)))
                   (logical-slot-copy-committed-image-equal-p
                    (third record) (logical-slot-copy-install-stage-node-image nodes)))
        (error "logical installation stage image changed"))
      (logical-slot-copy-install-check-file-scope)
      (unless (and (equalp expected (logical-slot-copy-install-read-node (getf nodes :journal) (length expected)))
                   (equalp (getf value :certificate)
                           (logical-slot-copy-install-read-node (getf nodes :pending-file)
                                                                (length (getf value :certificate))))
                   (equalp active (logical-slot-copy-install-read-node (getf nodes :active-file) (length active)))
                   (logical-slot-copy-committed-image-equal-p
                    (fourth record) (logical-slot-copy-install-stage-observations nodes value certificate)))
        (error "logical installation staged files or observations changed")))
    t))

(defun logical-slot-copy-install-stage-id (attempt requested)
  "Choose a canonical stage ID that does not alias a source owner."
  (let* ((certificate (logical-slot-copy-install-preparation-certificate attempt))
         (excluded (logical-slot-copy-import-excluded-owners certificate))
         (number (if requested (logical-slot-parse-generation requested)
                     (logical-slot-copy-import-new-owner certificate)))
         (token (logical-slot-generation-token number)))
    (unless (and (or (null requested) (equal requested token))
                 (not (member number excluded :test #'eql)))
      (error "logical installation stage ID aliases its source"))
    token))

(defun logical-slot-copy-install-build-stage-value (attempt nodes token rows)
  "Build the installation journal value from certified evidence and staged identities."
  (list :certificate (logical-slot-copy-import-image
                      (logical-slot-copy-install-preparation-certificate attempt))
        :evidence-digest (logical-slot-copy-import-digest
                          (logical-slot-copy-import-evidence-encode
                           (logical-slot-copy-committed-proof-evidence
                            (logical-slot-copy-install-preparation-proof attempt))))
        :stage-id token
        :container (copy-list (logical-slot-copy-install-node-identity (getf nodes :container)))
        :slot-root (copy-list (logical-slot-copy-install-node-identity (getf nodes :slot-root)))
        :slot-work (copy-list (logical-slot-copy-install-node-identity (getf nodes :slot-work)))
        :slot-container (copy-list (logical-slot-copy-install-node-identity (getf nodes :slot-container)))
        :journal (copy-list (logical-slot-copy-install-node-identity (getf nodes :journal)))
        :pending-file (copy-list (logical-slot-copy-install-node-identity (getf nodes :pending-file)))
        :active-file (copy-list (logical-slot-copy-install-node-identity (getf nodes :active-file)))
        :entries rows))

(defun logical-slot-copy-install-create-stage-nodes (root token)
  "Create private containers in the root and slot-root filesystem groups."
  (let* ((work (logical-slot-copy-install-open-node root "logical-slot-copy.work" :directory root t nil))
         (slot-root (logical-slot-copy-install-open-node root "pg_replslot" :slot-root nil t nil))
         (slot-work (logical-slot-copy-install-open-node slot-root ".logical-slot-copy.work" :directory slot-root t nil))
         (container (logical-slot-copy-install-open-node work token :directory root t t))
         (slots (logical-slot-copy-install-open-node slot-work token :directory slot-root t t))
         (pending (logical-slot-copy-install-open-node root "logical-slot-copy.import" :file root nil nil))
         (active (logical-slot-copy-install-open-node container "active" :file root t t))
         (journal (logical-slot-copy-install-open-node container "journal" :file root t t)))
    (list :root root :work work :slot-root slot-root :slot-work slot-work
          :container container :slot-container slots :journal journal :pending-file pending :active-file active)))

(defun call-with-logical-slot-copy-install-staged (owner context attempt function &key stage-id)
  "Create and sync the installation journal and P7 slot images, then call FUNCTION
privately.  The callback must only observe the stage; checks before and after it
reject changes to bindings, names, contents, and full inode facts.  Unpublished orphans are left
in place and are never enumerated, adopted, or deleted here.  This does not
publish the fixed journal, move canonical slots, or exchange active state;
a later publisher needs its own authority and checks."
  (logical-slot-copy-install-check-preparation owner context attempt)
  (when (slot-state-lstat (slot-state-fd-path
                           (physical-image-context-root-fd context) "logical-slot-copy.install"))
    (error "logical installation journal already exists"))
  (let ((token (logical-slot-copy-install-stage-id attempt stage-id)))
    (call-with-logical-slot-copy-install-file-scope
     owner context attempt
     (lambda (root)
       (let* ((nodes (logical-slot-copy-install-create-stage-nodes root token))
              (certificate (logical-slot-copy-install-preparation-certificate attempt))
              (rows (loop for row in (getf certificate :slots)
                          collect (logical-slot-copy-install-create-row
                                   (getf nodes :slot-container) (getf nodes :slot-root) row)))
              (value (logical-slot-copy-install-build-stage-value attempt nodes token rows))
              (bytes (logical-slot-copy-install-encode value)))
         (logical-slot-copy-install-write-node
          (getf nodes :active-file) (logical-slot-copy-install-active-image certificate))
         (logical-slot-copy-install-write-node (getf nodes :journal) bytes)
         (dolist (key '(:slot-container :slot-work :slot-root :container :work :root))
           (logical-slot-copy-install-sync-node (getf nodes key)))
         (let* ((stage (%make-logical-slot-copy-install-stage
                        :owner owner :context context :attempt attempt :nodes nodes
                        :value (logical-slot-copy-install-decode bytes) :image (copy-seq bytes)))
                (record (list stage (copy-seq bytes)
                              (logical-slot-copy-import-state-image (logical-slot-copy-install-stage-node-image nodes))
                              (logical-slot-copy-install-stage-observations nodes value certificate)))
                (*logical-slot-copy-install-stages* (cons record *logical-slot-copy-install-stages*)))
           (logical-slot-copy-install-check-stage owner context attempt stage)
           (multiple-value-prog1 (funcall function stage)
             (logical-slot-copy-install-check-stage owner context attempt stage))))))))

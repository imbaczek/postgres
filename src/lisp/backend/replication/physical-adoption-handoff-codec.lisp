;;; Encoding for the immutable offline adoption handoff intent.
;;;
;;; The intent binds the terminal installation plan, attempt and journal file
;;; facts, and the new workspace, spool and intent identities.  It describes
;;; exactly an empty receive control and lock at the proved stage end.  Its
;;; checksum detects corruption; the validator checks the live objects.

(in-package #:pg)

(defconstant +physical-adoption-handoff-maximum-bytes+
  (+ +physical-adoption-install-maximum-bytes+ (* 4 1024 1024)))

(defun physical-adoption-handoff-empty-control (record)
  (let* ((stage (getf record :stage))
         (end (getf stage :end))
         (store (%make-physical-store :system-id (getf stage :system-id)
                                      :timeline (getf stage :timeline)
                                      :base-lsn end :floor-lsn end
                                      :written-lsn end :flushed-lsn end)))
    (physical-store-encode-control store end)))

(defun physical-adoption-handoff-file-copy (value device maximum)
  (physical-follow-check-plist value '(:device :inode :size :digest))
  (unless (and (eql device (getf value :device))
               (typep (getf value :inode) '(integer 1 18446744073709551615))
               (typep (getf value :size) `(integer 1 ,maximum))
               (typep (getf value :digest) '(unsigned-byte 64)))
    (error "invalid adoption handoff file identity"))
  (physical-adoption-attempt-fields-copy value '(:device :inode :size :digest)))

(defun physical-adoption-handoff-copy (value)
  (physical-follow-check-plist
   value '(:version :installation :attempt-file :journal-file :workspace :spool :spool-entries :intent-file))
  (unless (eql 1 (getf value :version)) (error "unsupported adoption handoff intent"))
  (let* ((plan (physical-adoption-install-copy (getf value :installation)))
         (record (getf plan :attempt))
         (device (getf (getf record :local) :device))
         (identities (make-hash-table :test #'equal)))
    (dolist (inode (physical-adoption-install-plan-inodes plan))
      (physical-adoption-install-register-identity device inode device identities))
    (let* ((attempt (physical-adoption-handoff-file-copy
                     (getf value :attempt-file) device +physical-adoption-attempt-maximum-bytes+))
           (journal (physical-adoption-handoff-file-copy
                     (getf value :journal-file) device +physical-adoption-install-maximum-bytes+)))
      (physical-adoption-install-register-identity device (getf attempt :inode) device identities)
      (unless (eql (getf journal :inode) (getf (getf plan :journal) :inode))
        (error "handoff journal identity differs from installation"))
      (let* ((workspace (physical-adoption-install-fact-copy
                         (getf value :workspace) device identities :directory t :guarded t))
             (spool (physical-adoption-install-fact-copy (getf value :spool) device identities))
             (entries (physical-adoption-install-trees-copy (getf value :spool-entries) device identities))
             (intent (physical-adoption-install-fact-copy (getf value :intent-file) device identities))
             (work (pathname (getf workspace :directory)))
             (control (physical-adoption-handoff-empty-control record))
             (digest (physical-store-update-digest #xCBF29CE484222325 control)))
        (dolist (other (list (getf (getf record :local) :directory)
                             (getf (getf record :stage) :directory)
                             (getf (getf plan :workspace) :directory)))
          (when (or (uiop:subpathp work (pathname other)) (uiop:subpathp (pathname other) work))
            (error "adoption handoff workspace overlaps protected installation data")))
        (unless (and (equal '("receive.control" "receive.lock") (mapcar #'first entries))
                     (loop for entry in entries
                           for size in '(68 0)
                           for hash in (list digest #xCBF29CE484222325)
                           always (let ((rows (second entry)))
                                    (and (= 1 (length rows))
                                         (equal (first entry) (first (first rows)))
                                         (eq :file (second (first rows)))
                                         (= #o600 (third (first rows)))
                                         (= size (sixth (first rows)))
                                         (= hash (seventh (first rows)))))))
          (error "adoption handoff requires exactly an empty receive control and lock"))
        (list :version 1 :installation plan :attempt-file attempt :journal-file journal
              :workspace workspace :spool spool :spool-entries entries :intent-file intent)))))

(defun physical-adoption-handoff-image (value)
  (metadata-sized-literal-image "PGADOPTIONHANDOFF1" +physical-adoption-handoff-maximum-bytes+
                                (physical-adoption-handoff-copy value) "adoption handoff intent"))

(defun physical-adoption-handoff-decode (bytes)
  (multiple-value-bind (start end)
      (metadata-sized-literal-span bytes "PGADOPTIONHANDOFF1"
                                   +physical-adoption-handoff-maximum-bytes+
                                   +physical-adoption-handoff-maximum-bytes+
                                   "adoption handoff intent" :maximum-header 64)
    (unless (= (1+ end) (length bytes))
      (error "unexpected data after adoption handoff intent"))
    (physical-adoption-handoff-copy
     (recovery-parse-manifest-text (bytes->string bytes :start start :end end)))))

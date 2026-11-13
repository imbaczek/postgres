(in-package #:pg)

(defconstant +logical-slot-copy-base-proof-maximum-bytes+ 4096)

(defun logical-slot-copy-check-base-proof-fence ()
  (dolist (name '("logical-slot-copy.base-verified" "logical-slot-copy.base-verified.tmp"))
    (when (slot-state-lstat (merge-pathnames name *pgdata*))
      (error "logical copy base verification receipt requires child-owner import before startup")))
  (values))

(defun logical-slot-copy-base-proof-valid-p (value)
  (and (= 24 (logical-slot-copy-descriptor-list-count value 24))
       (equal (loop for (key data) on value by #'cddr collect key)
              '(:phase :copy-id :device :inode :manifest-digest
                :descriptor-digest :system-id :end :timeline :history-digest :slots :pieces))
       (eq :base-verified (getf value :phase))
       (stringp (getf value :copy-id))
       (= 32 (length (getf value :copy-id)))
       (every (lambda (char) (find char "0123456789abcdef")) (getf value :copy-id))
       (every (lambda (key) (typep (getf value key) '(unsigned-byte 64)))
              '(:device :inode :manifest-digest :descriptor-digest
                :system-id :end :history-digest))
       (typep (getf value :timeline) '(integer 1 4294967295))
       (typep (getf value :slots) '(integer 1 1024))
       (typep (getf value :pieces) '(integer 0 65536))))

(defun logical-slot-copy-base-proof-image (value)
  (unless (logical-slot-copy-base-proof-valid-p value)
    (error "invalid logical copy base verification proof"))
  (logical-slot-copy-literal-image
   "PGLOGICALBASE1" +logical-slot-copy-base-proof-maximum-bytes+ value
   "logical copy base verification proof"))

(defun logical-slot-copy-base-proof-decode (bytes)
  (logical-slot-copy-literal-decode
   bytes "PGLOGICALBASE1" +logical-slot-copy-base-proof-maximum-bytes+
   #'logical-slot-copy-base-proof-image "logical copy base verification proof"))

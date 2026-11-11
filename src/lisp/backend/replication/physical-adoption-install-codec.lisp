;;; Immutable installation plan and append-only progress encoding.
;;;
;;; The plan records the proved attempt, directory identities and inventories
;;; of the old, stage and incoming trees.  Incoming content must match the
;;; stage, but its objects have distinct identities.  A bounded, checksummed
;;; body is followed by ordered checksummed STEP records.  Decode accepts a
;;; torn suffix only when it is a prefix of the next expected STEP; namespace
;;; validation must still prove that the corresponding operation happened.

(in-package #:pg)

(defconstant +physical-adoption-install-body-maximum+
  (+ +physical-adoption-attempt-maximum-bytes+ (* 64 1024 1024)))
(defconstant +physical-adoption-install-maximum-bytes+
  (+ +physical-adoption-install-body-maximum+ (* 8 1024 1024)))

(defparameter *physical-adoption-install-root-names*
  '("PG_VERSION" "authentication.initialized" "base" "global" "pg_hba.conf" "pg_ident.conf"
    "pg_wal" "pg_xact" "physical-backup.manifest" "physical-recovery.control" "recovery.required"))

(defun physical-adoption-install-register-identity (device inode expected identities)
  (unless (and (typep device '(unsigned-byte 64)) (= device expected)
               (typep inode '(integer 1 18446744073709551615))
               (not (gethash (list device inode) identities)))
    (error "invalid or aliased adoption installation object identity"))
  (setf (gethash (list device inode) identities) t))

(defun physical-adoption-install-fact-copy (value device identities &key directory guarded)
  (let ((keys (append (when directory '(:directory)) '(:device :inode)
                      (when guarded '(:lock-inode :fence-inode)))))
    (physical-follow-check-plist value keys)
    (when directory
      (let ((name (getf value :directory)))
        (unless (and (physical-adoption-attempt-directory-p name)
                     (physical-adoption-install-relative-p (string-trim "/" name)))
          (error "invalid adoption installation directory name"))))
    (physical-adoption-install-register-identity
     (getf value :device) (getf value :inode) device identities)
    (when guarded
      (dolist (key '(:lock-inode :fence-inode))
        (physical-adoption-install-register-identity device (getf value key) device identities)))
    (physical-adoption-attempt-fields-copy value keys)))

(defun physical-adoption-install-tree-copy (rows top device identities budget)
  (unless (and (listp rows) (plusp (length rows)))
    (error "empty adoption installation object inventory"))
  (let ((parents (make-hash-table :test #'equal)) (previous nil))
    (loop for row in rows
          collect
          (progn
            (unless (and (listp row) (= 7 (length row)))
              (error "invalid adoption installation inventory row"))
            (destructuring-bind (name kind mode row-device inode size digest) row
              (unless (and (physical-adoption-install-relative-p name)
                           (or (equal name top) (uiop:string-prefix-p (concatenate 'string top "/") name))
                           (or (null previous) (string< previous name))
                           (if previous
                               (let ((slash (position #\/ name :from-end t)))
                                 (and slash (eq :directory (gethash (subseq name 0 slash) parents))))
                               (equal name top))
                           (member kind '(:file :directory))
                           (typep mode '(integer 0 4095))
                           (typep size '(unsigned-byte 64)) (typep digest '(unsigned-byte 64))
                           (if (eq kind :directory)
                               (and (zerop size) (zerop digest))
                               (or (plusp size) (= #xCBF29CE484222325 digest))))
                (error "invalid adoption installation inventory fields"))
              (when (or (minusp (decf (first budget)))
                        (minusp (decf (second budget) (+ 128 (* 2 (length (string->bytes name)))))))
                (error "adoption installation inventory exceeds its bound"))
              (physical-adoption-install-register-identity row-device inode device identities)
              (setf previous name (gethash name parents) kind)
              (list (copy-seq name) kind mode row-device inode size digest))))))

(defun physical-adoption-install-trees-copy (entries device identities)
  (unless (and (listp entries) (<= 1 (length entries) 100000))
    (error "invalid adoption installation top-level inventory"))
  (let ((previous nil) (budget (list 100000 16777216)))
    (loop for entry in entries
          collect
          (progn
            (unless (and (listp entry) (= 2 (length entry))
                         (physical-adoption-install-relative-p (first entry))
                         (not (find #\/ (first entry)))
                         (not (member (first entry) *physical-adoption-install-retained-names* :test #'equal))
                         (or (null previous) (string< previous (first entry))))
              (error "invalid adoption installation top-level entry"))
            (setf previous (first entry))
            (list (copy-seq (first entry))
                  (physical-adoption-install-tree-copy (second entry) (first entry) device identities budget))))))

(defun physical-adoption-install-tree-content (entries)
  (loop for (name rows) in entries
        collect (list name (loop for row in rows collect (append (subseq row 0 3) (subseq row 5))))))

(defun physical-adoption-install-copy (plan)
  (physical-follow-check-plist
   plan '(:version :attempt :workspace :incoming :quarantine :journal :old :stage-tree :new))
  (unless (eql 1 (getf plan :version)) (error "unsupported adoption installation plan"))
  (let* ((attempt (physical-adoption-attempt-copy (getf plan :attempt)))
         (local (getf attempt :local))
         (stage (getf attempt :stage))
         (device (getf local :device))
         (identities (make-hash-table :test #'equal)))
    (unless (eq :proved (getf attempt :phase))
      (error "adoption installation requires a proved attempt"))
    (dolist (pair (list (list device (getf local :inode))
                        (list (getf local :lock-device) (getf local :lock-inode))
                        (list (getf stage :device) (getf stage :inode))))
      (physical-adoption-install-register-identity (first pair) (second pair) device identities))
    (let* ((workspace (physical-adoption-install-fact-copy
                       (getf plan :workspace) device identities :directory t :guarded t))
           (incoming (physical-adoption-install-fact-copy (getf plan :incoming) device identities :guarded t))
           (quarantine (physical-adoption-install-fact-copy (getf plan :quarantine) device identities :guarded t))
           (journal (physical-adoption-install-fact-copy (getf plan :journal) device identities))
           (old (physical-adoption-install-trees-copy (getf plan :old) device identities))
           (stage-tree (physical-adoption-install-trees-copy (getf plan :stage-tree) device identities))
           (new (physical-adoption-install-trees-copy (getf plan :new) device identities))
           (work (pathname (getf workspace :directory))))
      (dolist (other (list (getf local :directory) (getf stage :directory)))
        (when (or (uiop:subpathp work (pathname other)) (uiop:subpathp (pathname other) work))
          (error "adoption installation workspace overlaps its source or stage")))
      (unless (and (equal *physical-adoption-install-root-names* (mapcar #'first stage-tree))
                   (equal (physical-adoption-install-tree-content stage-tree)
                          (physical-adoption-install-tree-content new)))
        (error "adoption incoming image differs from the canonical stage inventory"))
      (list :version 1 :attempt attempt :workspace workspace :incoming incoming :quarantine quarantine
            :journal journal :old old :stage-tree stage-tree :new new))))

(defun physical-adoption-install-last-step (plan)
  (+ 1 (length (getf plan :old)) (length (getf plan :new))))

(defun physical-adoption-install-step-image (crc step)
  (string->bytes
   (format nil "STEP ~d ~8,'0X~%" step
           (wal-crc32 (string->bytes (format nil "~8,'0X ~d" crc step))))))

(defun physical-adoption-install-image (plan step)
  (let* ((plan (physical-adoption-install-copy plan))
         (body (string->bytes (physical-bootstrap-manifest-text plan)))
         (crc (wal-crc32 body))
         (header (string->bytes (format nil "PGADOPTIONINSTALL1 ~d ~8,'0X~%" (length body) crc)))
         (image (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0)))
    (unless (and (typep step '(integer 0 200001)) (<= step (physical-adoption-install-last-step plan))
                 (<= (length body) +physical-adoption-install-body-maximum+))
      (error "adoption installation image or progress exceeds its bound"))
    (flet ((emit (bytes)
             (unless (<= (+ (length image) (length bytes)) +physical-adoption-install-maximum-bytes+)
               (error "adoption installation journal exceeds its bound"))
             (loop for byte across bytes do (vector-push-extend byte image))))
      (emit header) (emit body) (emit #(10))
      (loop for index from 1 to step do (emit (physical-adoption-install-step-image crc index))))
    image))

(defun physical-adoption-install-decode (bytes)
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (<= (length bytes) +physical-adoption-install-maximum-bytes+))
    (error "invalid or oversized adoption installation journal"))
  (let* ((newline (position 10 bytes :end (min 64 (length bytes))))
         (parts (and newline (uiop:split-string (bytes->string bytes :end newline) :separator " "))))
    (unless (and (= 3 (length parts)) (equal "PGADOPTIONINSTALL1" (first parts))
                 (plusp (length (second parts))) (every #'digit-char-p (second parts))
                 (= 8 (length (third parts))))
      (error "invalid adoption installation header"))
    (let* ((size (parse-integer (second parts)))
           (crc (parse-integer (third parts) :radix 16))
           (start (1+ newline))
           (end (+ start size)))
      (unless (and (<= 1 size +physical-adoption-install-body-maximum+)
                   (< end (length bytes)) (= 10 (aref bytes end))
                   (= crc (wal-crc32 bytes start end))
                   (equal (bytes->string bytes :end start)
                          (format nil "PGADOPTIONINSTALL1 ~d ~8,'0X~%" size crc)))
        (error "adoption installation header or checksum mismatch"))
      (let* ((plan (physical-adoption-install-copy
                    (recovery-parse-manifest-text (bytes->string bytes :start start :end end))))
             (last-step (physical-adoption-install-last-step plan))
             (position (1+ end)) (step 0) (partial nil)
             (scratch (make-array 32 :element-type '(unsigned-byte 8))))
        (unless (null (mismatch bytes (string->bytes (physical-bootstrap-manifest-text plan))
                                :start1 start :end1 end))
          (error "noncanonical adoption installation body"))
        (loop while (< position (length bytes))
              do (when (= step last-step)
                   (error "unexpected data after final installation progress"))
                 (let* ((next-size (%physical-install-write-progress crc (1+ step) scratch))
                        (count (min next-size (- (length bytes) position))))
                   (unless (null (mismatch bytes scratch :start1 position :end1 (+ position count) :end2 count))
                     (error "invalid adoption installation progress"))
                   (when (< count next-size) (setf partial t) (return))
                   (incf position count)
                   (incf step)))
        (list :plan plan :step step :valid-length position :file-size (length bytes)
              :header-length (1+ end) :body-crc crc :partial partial)))))

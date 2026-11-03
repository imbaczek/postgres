;;;-------------------------------------------------------------------------
;;;
;;; physical-reseed-install-codec.lisp
;;;   Encode and check the durable physical reseed installation record.
;;;
;;; The record describes the original attempt, its directories and control
;;; files, and the old and incoming inventories.  The codec checks their
;;; structure and relationships and returns copies of the supplied facts.
;;; It does not open files or establish that those facts still hold.
;;;
;;; A future publisher must prove the held descriptors, guards, incoming image
;;; and source READY state before publishing.  An executor must validate the
;;; actual namespace before recovering a torn tail or moving entries.  The
;;; attempt-file identity records the initial alias-exclusion observation.
;;; Cancellation may replace the actual attempt inode.
;;;
;;;-------------------------------------------------------------------------
(in-package #:pg)

(defconstant +physical-reseed-install-body-maximum+ (* 64 1024 1024))
(defconstant +physical-reseed-install-maximum-bytes+
  (+ +physical-reseed-install-body-maximum+ (* 4 1024 1024)))

;; Bound the work and allocation needed to inspect an untrusted plan.  Shared
;; lists are allowed, but cycles and values outside the literal format are not.
(defun physical-reseed-install-check-literal (value)
  (let ((todo (list (cons :enter value))) (active (make-hash-table :test #'eq))
                                          (cells 0) (weight 0))
    (loop while todo
          do (let* ((frame (pop todo)) (node (cdr frame)))
               (if (eq :leave (car frame))
                   (remhash node active)
                   (cond
                     ((consp node)
                      (when (gethash node active) (error "cyclic reseed install plan"))
                      (setf (gethash node active) t)
                      (incf cells)
                      (incf weight 2)
                      (push (cons :leave node) todo)
                      (push (cons :enter (cdr node)) todo)
                      (push (cons :enter (car node)) todo))
                     ((stringp node)
                      (when (> (length node) +physical-reseed-install-body-maximum+)
                        (error "oversized reseed install string"))
                      (incf weight (+ 2 (* 2 (length (string->bytes node))))))
                     ((integerp node) (incf weight (+ 2 (max 1 (ceiling (integer-length node) 3)))))
                     ((null node) (incf weight 3))
                     ((keywordp node) (incf weight (+ 1 (length (symbol-name node)))))
                     (t (error "unsupported reseed install literal")))))
             (when (or (> cells 4000000) (> weight (* 2 +physical-reseed-install-body-maximum+)))
               (error "reseed install plan exceeds its literal bound")))
    t))

(defun physical-reseed-install-register (device inode expected identities)
  (let ((key (list device inode)))
    (unless (and (typep device '(unsigned-byte 64)) (= device expected)
                 (typep inode '(integer 1 18446744073709551615))
                 (not (gethash key identities)))
      (error "invalid or aliased reseed installation identity"))
    (setf (gethash key identities) t)))

(defun physical-reseed-install-control-copy (value device identities)
  (physical-follow-check-plist value '(:device :inode))
  (physical-reseed-install-register (getf value :device) (getf value :inode) device identities)
  (list :device (getf value :device) :inode (getf value :inode)))

(defun physical-reseed-install-auxiliary-copy (value local identities)
  (physical-follow-check-plist value '(:directory :device :inode :lock-inode :fence-inode))
  (let ((fact (physical-reseed-attempt-stage-copy
               (list :directory (getf value :directory) :device (getf value :device) :inode (getf value :inode))
               local)))
    (dolist (key '(:inode :lock-inode :fence-inode))
      (physical-reseed-install-register (getf value :device) (getf value key) (getf local :device) identities))
    (append fact (list :lock-inode (getf value :lock-inode) :fence-inode (getf value :fence-inode)))))

(defun physical-reseed-install-check-directories (directories)
  (loop for tail on directories
        do (dolist (other (rest tail))
             (when (or (uiop:subpathp (pathname (first tail)) (pathname other))
                       (uiop:subpathp (pathname other) (pathname (first tail))))
               (error "reseed installation directories overlap")))))

(defun physical-reseed-install-manifest-copy (manifest request)
  (unless (and (listp (getf manifest :files)) (<= (length (getf manifest :files)) 99991))
    (error "reseed install manifest exceeds its file bound"))
  (recovery-check-manifest-schema manifest)
  (unless (and (equal (getf manifest :source-pin) (getf request :name))
               (equal (getf manifest :cluster-identity) (getf request :cluster-identity))
               (eql (getf manifest :system-id) (getf request :system-id))
               (eql (getf manifest :timeline) (getf request :timeline)))
    (error "reseed install manifest belongs to another source request"))
  (loop for (key value) on manifest by #'cddr
        append (list key (cond
                           ((eq key :files)
                            (loop for (name size digest) in value collect (list (copy-seq name) size digest)))
                           ((stringp value) (copy-seq value))
                           (t value)))))

;; Check the private incoming inventory against the manifest and the fixed
;; metadata and directory entries.  Each incoming object has its own identity.
(defun physical-reseed-install-new-copy (rows manifest manifest-digest device identities)
  (let ((expected (make-hash-table :test #'equal)) (previous nil) (name-bytes 0))
    (dolist (name '("base" "global" "pg_wal")) (setf (gethash name expected) '(:directory)))
    (dolist (entry (physical-backup-metadata-bounds))
      (setf (gethash (first entry) expected) (cons :metadata (rest entry))))
    (dolist (entry (getf manifest :files))
      (setf (gethash (first entry) expected) (cons :file (rest entry))))
    (unless (and (listp rows) (= (length rows) (hash-table-count expected)) (<= (length rows) 100000))
      (error "reseed incoming inventory has missing or extra entries"))
    (loop for row in rows
          collect
          (progn
            (unless (and (listp row) (= 10 (length row))
                         (physical-reseed-install-row-facts-p (subseq row 0 9) device)
                         (stringp (first row))
                         (<= (- (length (first row))
                                (let ((slash (position #\/ (first row) :from-end t)))
                                  (if slash (1+ slash) 0))) 255)
                         (or (null previous) (string< previous (first row)))
                         (<= (incf name-bytes (length (string->bytes (first row)))) 16777216))
              (error "invalid or unordered reseed incoming inventory"))
            (let* ((name (first row)) (spec (gethash name expected))
                                      (directory (eq :directory (first spec))) (size (seventh row)) (digest (tenth row)))
              (unless (and spec (>= size 0)
                           (= (fourth row) (if directory #o040700 #o100600))
                           (if directory (null digest)
                               (and (= 1 (fifth row)) (typep digest '(unsigned-byte 64))
                                    (or (plusp size) (= digest #xCBF29CE484222325)))))
                (error "invalid private reseed incoming object"))
              (case (first spec)
                (:file
                 (unless (and (= size (second spec)) (= digest (third spec)))
                   (error "reseed incoming file differs from its manifest")))
                (:metadata
                 (unless (and (<= size (second spec))
                              (or (not (equal name "physical-backup.manifest")) (= digest manifest-digest)))
                   (error "reseed incoming metadata exceeds its bound or differs"))))
              (physical-reseed-install-register (second row) (third row) device identities)
              (setf previous name)
              (cons (copy-seq name) (copy-list (rest row))))))))

;; Old non-directory entries may be hardlinked.  All rows for an inode must
;; carry consistent facts, and their count must fit the recorded link count.
;; No old entry may alias an installation object.
(defun physical-reseed-install-check-old-aliases (rows identities)
  (let ((aliases (make-hash-table :test #'equal)))
    (dolist (row rows)
      (let* ((key (subseq row 1 3)) (previous (gethash key aliases)))
        (when (gethash key identities) (error "reseed old entry aliases an installation object"))
        (if previous
            (progn
              (unless (and (not (sb-posix:s-isdir (fourth row))) (equal (cdr row) (cdr (second previous))))
                (error "inconsistent reseed old hardlink facts"))
              (incf (first previous)))
            (setf (gethash key aliases) (list 1 row)))))
    (maphash (lambda (key value)
               (declare (ignore key))
               (unless (<= (first value) (fifth (second value)))
                 (error "reseed old hardlink count is inconsistent")))
             aliases)))

(defun physical-reseed-install-check-pid-row (attempt rows)
  (let ((pid (getf (getf attempt :local) :pid))
        (row (assoc "postmaster.pid" rows :test #'equal)))
    (unless (if pid
                (and row (sb-posix:s-isreg (fourth row)) (= 1 (fifth row))
                     (equal (getf pid :identity)
                            (physical-reseed-install-row-file-facts row)))
                (null row))
      (error "reseed old PID entry differs from its original evidence"))))

;; Check the plan and make a private copy of its mutable components.  Identity
;; checks compare recorded device and inode values, so callers must still
;; establish ownership and verify the objects through held descriptors.
(defun physical-reseed-install-copy (plan)
  (physical-reseed-install-check-literal plan)
  (physical-follow-check-plist
   plan '(:version :attempt :attempt-file :incoming :quarantine :journal :manifest :manifest-digest :old :new))
  (unless (and (eql 1 (getf plan :version)) (typep (getf plan :manifest-digest) '(unsigned-byte 64)))
    (error "invalid reseed install version or manifest digest"))
  (let* ((attempt (physical-reseed-attempt-copy (getf plan :attempt)))
         (local (getf attempt :local)) (stage (getf attempt :stage))
         (device (getf local :device)) (identities (make-hash-table :test #'equal)))
    (unless (and (eq :premanifest (getf attempt :phase)) stage)
      (error "reseed install requires the original active attempt and stage"))
    (dolist (pair (list (list device (getf local :inode))
                        (list (getf local :lock-device) (getf local :lock-inode))
                        (list (getf stage :device) (getf stage :inode))))
      (physical-reseed-install-register (first pair) (second pair) device identities))
    (let* ((attempt-file (physical-reseed-install-control-copy (getf plan :attempt-file) device identities))
           (incoming (physical-reseed-install-auxiliary-copy (getf plan :incoming) local identities))
           (quarantine (physical-reseed-install-auxiliary-copy (getf plan :quarantine) local identities))
           (journal (physical-reseed-install-control-copy (getf plan :journal) device identities))
           (manifest (physical-reseed-install-manifest-copy (getf plan :manifest) (getf attempt :request))))
      (physical-reseed-install-check-directories
       (mapcar (lambda (fact) (getf fact :directory)) (list local stage incoming quarantine)))
      (let* ((new (physical-reseed-install-new-copy
                   (getf plan :new) manifest (getf plan :manifest-digest) device identities))
             (old (physical-reseed-install-old-copy (getf plan :old) device)))
        (physical-reseed-install-check-old-aliases old identities)
        (physical-reseed-install-check-pid-row attempt old)
        (list :version 1 :attempt attempt :attempt-file attempt-file :incoming incoming :quarantine quarantine
              :journal journal
              :manifest manifest :manifest-digest (getf plan :manifest-digest) :old old :new new)))))

;; Progress covers the sorted old entries, then the top-level incoming entries,
;; followed by a terminal step.  The executor must verify the full image before
;; recording that final step.
(defun physical-reseed-install-last-step (plan)
  (+ 1 (length (getf plan :old))
     (count-if (lambda (row) (not (find #\/ (first row)))) (getf plan :new))))

(defun physical-reseed-install-step-image (crc step)
  (string->bytes
   (format nil "STEP ~d ~8,'0X~%" step
           (wal-crc32 (string->bytes (format nil "~8,'0X ~d" crc step))))))

;; Write a canonical body with its length and CRC, followed by consecutive
;; checksummed progress lines.  The initial image has no progress lines.
(defun physical-reseed-install-image (plan step)
  (let* ((plan (physical-reseed-install-copy plan))
         (body (string->bytes (physical-bootstrap-manifest-text plan)))
         (crc (wal-crc32 body))
         (header (string->bytes (format nil "PGRESEEDINSTALL1 ~d ~8,'0X~%" (length body) crc))))
    (unless (and (typep step '(integer 0 100012)) (<= step (physical-reseed-install-last-step plan))
                 (<= (length body) +physical-reseed-install-body-maximum+))
      (error "reseed install image or progress exceeds its bound"))
    (let ((parts (loop for index from 1 to step collect (physical-reseed-install-step-image crc index))))
      (let ((size (+ (length header) (length body) 1 (reduce #'+ parts :key #'length :initial-value 0))))
        (unless (<= size +physical-reseed-install-maximum-bytes+) (error "oversized reseed install journal"))
        (let ((image (make-array size :element-type '(unsigned-byte 8))) (position 0))
          (dolist (part (list* header body #(10) parts))
            (replace image part :start1 position)
            (incf position (length part)))
          image)))))

;; Accept only the canonical body and consecutive progress lines.  A trailing
;; prefix of the next line is reported as partial, with valid-length marking
;; the end of the last complete line.  The caller must validate the namespace
;; before using that boundary for recovery.
(defun physical-reseed-install-decode (bytes)
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (<= (length bytes) +physical-reseed-install-maximum-bytes+))
    (error "invalid or oversized reseed install journal"))
  (let* ((newline (position 10 bytes :end (min 64 (length bytes))))
         (parts (and newline (uiop:split-string (bytes->string (subseq bytes 0 newline)) :separator " "))))
    (unless (and (= 3 (length parts)) (equal "PGRESEEDINSTALL1" (first parts))
                 (<= 1 (length (second parts)) 8) (every #'digit-char-p (second parts))
                 (= 8 (length (third parts))))
      (error "invalid reseed install header"))
    (let* ((size (parse-integer (second parts))) (crc (parse-integer (third parts) :radix 16))
                                                 (start (1+ newline)) (end (+ start size)))
      (unless (and (<= 1 size +physical-reseed-install-body-maximum+)
                   (< end (length bytes)) (= 10 (aref bytes end))
                   (= crc (wal-crc32 (subseq bytes start end)))
                   (equal (bytes->string (subseq bytes 0 start))
                          (format nil "PGRESEEDINSTALL1 ~d ~8,'0X~%" size crc)))
        (error "reseed install header or checksum mismatch"))
      (let* ((plan (physical-reseed-install-copy
                    (recovery-parse-manifest-text (bytes->string (subseq bytes start end)))))
             (last-step (physical-reseed-install-last-step plan))
             (position (1+ end)) (step 0) (partial nil))
        (unless (equalp (subseq bytes start end) (string->bytes (physical-bootstrap-manifest-text plan)))
          (error "noncanonical reseed install body"))
        (loop while (< position (length bytes))
              do (when (= step last-step)
                   (error "unexpected data after final reseed install progress"))
                 (let* ((next (physical-reseed-install-step-image crc (1+ step)))
                        (count (min (length next) (- (length bytes) position))))
                   (unless (equalp (subseq bytes position (+ position count)) (subseq next 0 count))
                     (error "invalid reseed install progress"))
                   (when (< count (length next)) (setf partial t) (return))
                   (incf position count)
                   (incf step)))
        (list :plan plan :step step :valid-length position :file-size (length bytes)
              :header-length (1+ end) :body-crc crc :partial partial)))))

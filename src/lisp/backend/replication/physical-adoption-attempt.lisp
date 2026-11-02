;;;; Private, structurally checked offline adoption attempt records.
;;;; Publication records intent; it does not validate current context or authorize adoption.
(in-package #:pg)

(defconstant +physical-adoption-attempt-maximum-bytes+ +physical-follow-config-maximum-bytes+)
(defvar *physical-adoption-attempt-test-hook* nil)

(defparameter *physical-adoption-attempt-local-keys*
  '(:directory :device :inode :lock-device :lock-inode :system-id :cluster-identity
    :timeline :namespace :end :checkpoint-start :next-xid :allocation-next-xid :wal-control :capture))
(defparameter *physical-adoption-attempt-stage-keys*
  '(:directory :device :inode :system-id :cluster-identity :timeline :checkpoint
    :checkpoint-start :end :next-xid :manifest-digest :source-pin :auth-files))
(defparameter *physical-adoption-attempt-keys*
  '(:version :phase :local :stage :origin-proof :source :request :source-history :anchor :proof))

(defun physical-adoption-attempt-test-point (point path)
  (when *physical-adoption-attempt-test-hook*
    (funcall *physical-adoption-attempt-test-hook* point path)))

(defun physical-adoption-attempt-value-copy (value)
  (typecase value
    (string (copy-seq value))
    (cons (cons (physical-adoption-attempt-value-copy (car value))
                (physical-adoption-attempt-value-copy (cdr value))))
    (t value)))

(defun physical-adoption-attempt-fields-copy (value keys)
  (loop for key in keys
        append (list key (physical-adoption-attempt-value-copy (getf value key)))))

(defun physical-adoption-attempt-u64-fields-p (value keys)
  (every (lambda (key) (typep (getf value key) '(unsigned-byte 64))) keys))

(defun physical-adoption-attempt-directory-p (value)
  (and (stringp value) (plusp (length value)) (<= (length value) 4096)
       (not (position (code-char 0) value))
       (uiop:absolute-pathname-p value)
       (equal value (namestring (uiop:ensure-directory-pathname value)))))

(defun physical-adoption-attempt-capture-copy (capture wal)
  "Copy seven ordered control capture commitments, preserving absent entries.
Each present entry holds length and FNV; the first must match raw WAL control."
  (unless (and (listp capture) (= 7 (length capture)) (null (second capture)))
    (error "invalid adoption control capture"))
  (let ((copy
          (mapcar (lambda (entry)
                    (when entry
                      (unless (and (listp entry) (= 2 (length entry))
                                   (typep (first entry) '(unsigned-byte 64))
                                   (<= (first entry) +wal-segment-size+)
                                   (typep (second entry) '(unsigned-byte 64))
                                   (or (plusp (first entry)) (= #xCBF29CE484222325 (second entry))))
                        (error "invalid adoption capture commitment"))
                      (copy-list entry)))
                  capture)))
    (unless (equal (first copy)
                   (list (length wal) (physical-store-update-digest #xCBF29CE484222325 wal)))
      (error "adoption WAL control differs from its capture commitment"))
    copy))

(defun physical-adoption-attempt-local-copy (value)
  "Check saved clean control facts, keeping actual next XID distinct from allocation.
WAL control is stored as bounded hex; these checks do not reinspect disk."
  (physical-follow-check-plist value *physical-adoption-attempt-local-keys*)
  (unless (and (physical-adoption-attempt-directory-p (getf value :directory))
               (physical-adoption-attempt-u64-fields-p
                value '(:device :inode :lock-device :lock-inode :system-id :end :checkpoint-start))
               (stringp (getf value :cluster-identity)) (plusp (length (getf value :cluster-identity)))
               (typep (getf value :timeline) '(integer 1 4294967295))
               (member (getf value :namespace) '(:legacy :timeline))
               (or (= 1 (getf value :timeline)) (eq :timeline (getf value :namespace)))
               (typep (getf value :next-xid) '(integer 3 18446744073709551616))
               (typep (getf value :allocation-next-xid) '(integer 3 18446744073709551616))
               (<= (getf value :next-xid) (getf value :allocation-next-xid)))
    (error "invalid adoption local facts"))
  (let* ((wal (physical-libpq-datum-octets (getf value :wal-control) (1+ +wal-control-maximum-bytes+)))
         (fields (multiple-value-list (wal-decode-control-line (bytes->string wal))))
         (snapshot (third fields))
         (metadata (seventh fields))
         (copy (physical-adoption-attempt-fields-copy value *physical-adoption-attempt-local-keys*)))
    (unless (and snapshot (wal-checkpoint-snapshot-valid-p snapshot)
                 (= (first fields) (second fields) (getf value :end) (getf snapshot :end))
                 (= (fourth fields) (fifth fields) (getf value :timeline) (getf snapshot :timeline))
                 (eq (sixth fields) (getf value :namespace))
                 (= +db-shutdowned+ (getf metadata :db-state -1))
                 (= (getf value :system-id) (getf metadata :system-id))
                 (= (getf value :checkpoint-start) (getf snapshot :start))
                 (= (getf value :next-xid) (getf snapshot :next-xid))
                 (= (getf value :allocation-next-xid) (1+ (getf snapshot :xid-reserved-through)))
                 (or (zerop (getf snapshot :oldest-active-xid))
                     (= (getf snapshot :oldest-active-xid) (getf value :next-xid))))
      (error "adoption local facts differ from clean WAL control"))
    (setf (getf copy :wal-control) (physical-libpq-octets-datum wal)
          (getf copy :capture) (physical-adoption-attempt-capture-copy (getf value :capture) wal))
    copy))

(defun physical-adoption-attempt-auth-copy (entries)
  "Check and copy the three ordered authentication commitments outside Mnew."
  (unless (and (listp entries) (= 3 (length entries)))
    (error "invalid adoption authentication commitments"))
  (loop for entry in entries
        for name in '("authentication.initialized" "pg_hba.conf" "pg_ident.conf")
        unless (and (listp entry) (= 3 (length entry)) (equal name (first entry))
                    (typep (second entry) '(integer 0 1048576))
                    (typep (third entry) '(unsigned-byte 64))
                    (or (plusp (second entry)) (= #xCBF29CE484222325 (third entry))))
          do (error "invalid adoption authentication commitment")
        collect (physical-adoption-attempt-value-copy entry)))

(defun physical-adoption-attempt-stage-copy (value local)
  (physical-follow-check-plist value *physical-adoption-attempt-stage-keys*)
  (unless (and (physical-adoption-attempt-directory-p (getf value :directory))
               (not (or (uiop:subpathp (pathname (getf value :directory)) (pathname (getf local :directory)))
                        (uiop:subpathp (pathname (getf local :directory)) (pathname (getf value :directory)))))
               (physical-adoption-attempt-u64-fields-p value '(:device :inode :manifest-digest))
               (= (getf value :device) (getf local :device))
               (wal-valid-replication-slot-name-p (getf value :source-pin))
               (every (lambda (key) (equal (getf value key) (getf local key)))
                      '(:system-id :cluster-identity :timeline :checkpoint-start :end :next-xid))
               (= (getf value :checkpoint) (getf local :end)))
    (error "adoption stage facts differ from the local clean endpoint"))
  (let ((copy (physical-adoption-attempt-fields-copy value *physical-adoption-attempt-stage-keys*)))
    (setf (getf copy :auth-files) (physical-adoption-attempt-auth-copy (getf value :auth-files)))
    copy))

(defun physical-adoption-attempt-origin-copy (value local)
  (physical-follow-check-plist value '(:identity :proven-lsn :prefix-digest :end :end-digest))
  (let ((identity (getf value :identity)))
    (unless (and (listp identity) (= 4 (length identity))
                 (every (lambda (number) (typep number '(unsigned-byte 64))) identity)
                 (= (first identity) (getf local :system-id))
                 (= (second identity) (getf local :timeline))
                 (physical-adoption-attempt-u64-fields-p value '(:proven-lsn :prefix-digest :end :end-digest))
                 (= (getf value :end) (getf local :end))
                 (<= (third identity) (getf value :proven-lsn) (getf value :end))
                 (or (/= (third identity) (getf value :proven-lsn))
                     (= #xCBF29CE484222325 (getf value :prefix-digest)))
                 (or (/= (getf value :proven-lsn) (getf value :end))
                     (= (getf value :prefix-digest) (getf value :end-digest))))
      (error "invalid adoption origin witness")))
  (physical-adoption-attempt-fields-copy value '(:identity :proven-lsn :prefix-digest :end :end-digest)))

(defun physical-adoption-attempt-request-copy (value local stage history)
  (let* ((request (physical-follow-request-copy value))
         (source (getf request :source-timeline))
         (timeline (getf local :timeline))
         (end (getf local :end)))
    (unless (and (not (getf request :kind))
                 (stringp history) (<= (length history) +physical-store-history-maximum-bytes+)
                 (<= (length (string->bytes history)) +physical-store-history-maximum-bytes+)
                 (= (getf request :system-id) (getf local :system-id))
                 (= (getf request :base-timeline) (getf request :receive-timeline) timeline)
                 (= (getf request :base) (getf request :receive-lsn) end)
                 (= (getf request :manifest-digest) (getf stage :manifest-digest))
                 (= (getf request :receive-digest) #xCBF29CE484222325)
                 (= (getf request :overlap-start) (* (floor (1- end) +wal-segment-size+) +wal-segment-size+))
                 (> source timeline))
      (error "invalid canonical adoption PREPARE coordinates"))
    (let ((parsed (timeline-check-lineage (timeline-parse-history source history))))
      (unless (and (timeline-prefix-boundary-p timeline end parsed)
                   (= timeline (timeline-at-point (1- end) parsed)))
        (error "adoption source history does not include the local endpoint")))
    request))

(defun physical-adoption-attempt-with-history (value keys history)
  (physical-follow-check-plist value (remove :history-text keys))
  (append (physical-adoption-attempt-fields-copy value (remove :history-text keys))
          (list :history-text (copy-seq history))))

(defun physical-adoption-attempt-origin-request (origin)
  (let ((identity (getf origin :identity)))
    (list :system-id (first identity) :base-timeline (second identity)
          :base (third identity) :manifest-digest (fourth identity) :end (getf origin :end))))

(defun physical-adoption-attempt-proof-copy (anchor proof request origin stage history)
  "Reattach shared history to detached anchor and proof for existing validators.
Check typed proof fields and equality with the saved local history digest."
  (let* ((full-anchor (physical-follow-anchor-copy
                       (physical-adoption-attempt-with-history anchor *physical-follow-anchor-keys* history)))
         (full-proof (physical-adoption-attempt-with-history
                      proof (mapcar #'second *physical-origin-proof-result-fields*) history))
         (original (physical-adoption-attempt-origin-request origin)))
    (unless (equal request (physical-follow-request-copy
                            (loop for (key value) on full-anchor by #'cddr
                                  unless (member key '(:history-text :eor-octets))
                                    append (list key value))))
      (error "adoption PREPARE anchor differs from the saved request"))
    (dolist (field *physical-origin-proof-result-fields*)
      (let ((value (getf full-proof (second field))))
        (unless (case (third field)
                  (:text (stringp value))
                  (:timeline (typep value '(integer 1 4294967295)))
                  (otherwise (typep value '(unsigned-byte 64))))
          (error "invalid typed adoption source proof field"))))
    (physical-origin-proof-decode-result
     (coerce (physical-origin-proof-result-row full-proof) 'vector)
     (getf stage :source-pin) full-anchor original)
    (unless (= (getf origin :end-digest) (getf full-proof :end-digest))
      (error "adoption remote digest differs from its local history witness"))
    (values (physical-adoption-attempt-fields-copy full-anchor (remove :history-text *physical-follow-anchor-keys*))
            (physical-adoption-attempt-fields-copy full-proof
                                                   (remove :history-text (mapcar #'second *physical-origin-proof-result-fields*))))))

(defun physical-adoption-attempt-copy (record)
  "Validate and copy a structurally coherent attempt in canonical key order.
The source history is stored once, bounded to 16 MiB. This does not reinspect
source, stage or control files or authorize PREPARE or installation."
  (physical-follow-check-plist record *physical-adoption-attempt-keys*)
  (unless (and (eql 1 (getf record :version))
               (member (getf record :phase) '(:preparing :proved :aborting :aborted)))
    (error "unsupported adoption attempt state"))
  (let* ((local (physical-adoption-attempt-local-copy (getf record :local)))
         (stage (physical-adoption-attempt-stage-copy (getf record :stage) local))
         (origin (physical-adoption-attempt-origin-copy (getf record :origin-proof) local))
         (history (getf record :source-history))
         (request (physical-adoption-attempt-request-copy (getf record :request) local stage history))
         (source (physical-follow-source-copy (getf record :source)))
         (anchor (getf record :anchor))
         (proof (getf record :proof)))
    (unless (and (eq (null anchor) (null proof))
                 (case (getf record :phase)
                   (:preparing (null anchor))
                   (:proved anchor)
                   (otherwise t)))
      (error "incomplete adoption attempt proof state"))
    (when anchor
      (multiple-value-setq (anchor proof)
        (physical-adoption-attempt-proof-copy anchor proof request origin stage history)))
    (list :version 1 :phase (getf record :phase) :local local :stage stage
          :origin-proof origin :source source :request request :source-history (copy-seq history)
          :anchor anchor :proof proof)))

(defun physical-adoption-attempt-image (record)
  "Encode a checked attempt as a bounded CRC-protected literal.
The record contains private credentials; hex is not encryption. Do not log it."
  (let* ((checked (physical-adoption-attempt-copy record))
         (text (physical-bootstrap-manifest-text checked))
         (bytes (string->bytes text))
         (image (string->bytes (format nil "PGADOPTIONATTEMPT1 ~8,'0X~%~a~%" (wal-crc32 bytes) text))))
    (unless (<= (length image) +physical-adoption-attempt-maximum-bytes+)
      (error "adoption attempt exceeds its private record bound"))
    image))

(defun physical-adoption-attempt-decode (bytes)
  "Decode a bounded CRC envelope and safe literal into a checked owned copy.
CRC and FNV detect inconsistencies under cooperative ownership; they do not
authenticate the record or establish fresh source or stage facts."
  (unless (<= (length bytes) +physical-adoption-attempt-maximum-bytes+)
    (error "adoption attempt exceeds its private record bound"))
  (let* ((text (bytes->string bytes))
         (newline (position #\Newline text))
         (prefix "PGADOPTIONATTEMPT1 "))
    (unless (and newline (= newline (+ (length prefix) 8)) (uiop:string-prefix-p prefix text))
      (error "invalid adoption attempt envelope"))
    (let* ((body (subseq text (1+ newline)))
           (crc (parse-integer text :start (length prefix) :end newline :radix 16)))
      (unless (and (plusp (length body)) (char= #\Newline (char body (1- (length body))))
                   (= crc (wal-crc32 (string->bytes (subseq body 0 (1- (length body)))))))
        (error "adoption attempt checksum mismatch"))
      (physical-adoption-attempt-copy (recovery-parse-manifest-text body)))))

(defun physical-adoption-attempt-check-transition (previous candidate)
  "Allow idempotent phases and forward transitions without rebinding saved facts.
Once saved, anchor and proof cannot change; cancellation phases encode state
only and do not perform network ABORT."
  (let* ((previous (physical-adoption-attempt-copy previous))
         (candidate (physical-adoption-attempt-copy candidate))
         (old (getf previous :phase))
         (new (getf candidate :phase)))
    (unless (and (every (lambda (key) (equal (getf previous key) (getf candidate key)))
                        (remove-if (lambda (key) (member key '(:phase :anchor :proof)))
                                   *physical-adoption-attempt-keys*))
                 (or (eq old new)
                     (member (list old new)
                             '((:preparing :proved) (:preparing :aborting)
                               (:proved :aborting) (:aborting :aborted))
                             :test #'equal))
                 (or (and (eq old :preparing) (eq new :proved))
                     (and (equal (getf previous :anchor) (getf candidate :anchor))
                          (equal (getf previous :proof) (getf candidate :proof)))))
      (error "adoption attempt cannot rebind or reverse its saved operation"))
    t))

(defun physical-adoption-attempt-check-owner (owner record)
  (physical-adoption-check-owner owner)
  (let ((local (getf record :local)))
    (unless (and (equal (getf local :directory) (namestring (physical-adoption-owner-directory owner)))
                 (= (getf local :device) (physical-adoption-owner-device owner))
                 (= (getf local :inode) (physical-adoption-owner-inode owner))
                 (= (getf local :lock-device) (physical-adoption-owner-lock-device owner))
                 (= (getf local :lock-inode) (physical-adoption-owner-lock-inode owner)))
      (error "adoption attempt belongs to another offline owner")))
  owner)

(defun physical-adoption-attempt-read (owner)
  "Read a private single-link 0600 attempt belonging to the active offline owner.
Validation establishes structural coherence, not current external facts."
  (physical-adoption-check-owner owner)
  (let* ((path (physical-adoption-attempt-path (physical-adoption-owner-directory owner)))
         (stat (sb-posix:lstat (namestring path))))
    (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                 (= 1 (sb-posix:stat-nlink stat))
                 (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                 (= #o600 (logand #o7777 (sb-posix:stat-mode stat))))
      (error "adoption attempt is not an owned private regular file"))
    (let ((record (physical-adoption-attempt-decode
                   (read-bounded-regular-file path +physical-adoption-attempt-maximum-bytes+))))
      (physical-adoption-attempt-check-owner owner record)
      record)))

(defun physical-adoption-attempt-publish (owner candidate &key previous)
  "Publish owned copies under the active offline owner in its PGDATA.
Check the previous record and transition, then fsync, rename, sync the directory
and read back. This does not revalidate source or stage or grant operation authority."
  (let* ((candidate (physical-adoption-attempt-copy candidate))
         (previous (when previous (physical-adoption-attempt-copy previous)))
         (owner (physical-adoption-attempt-check-owner owner candidate))
         (directory (physical-adoption-owner-directory owner))
         (*pgdata* directory)
         (path (physical-adoption-attempt-path directory))
         (image (physical-adoption-attempt-image candidate)))
    (if previous
        (progn
          (unless (equal previous (physical-adoption-attempt-read owner))
            (error "adoption attempt changed before publication"))
          (physical-adoption-attempt-check-transition previous candidate))
        (progn
          (physical-adoption-check-startup)
          (unless (eq :preparing (getf candidate :phase))
            (error "a new adoption attempt must start preparing"))))
    (with-atomic-output
        (stream path (make-pathname :type "tmp" :defaults path) directory
                :element-type '(unsigned-byte 8) :mode #o600
                :fsync-function (lambda (fd)
                                  (physical-adoption-attempt-test-point :before-file-fsync path)
                                  (sb-posix:fsync fd)
                                  (physical-adoption-attempt-test-point :after-file-fsync path))
                :rename-function (lambda (from to)
                                   (physical-adoption-attempt-test-point :before-rename path)
                                   (sb-posix:rename from to)
                                   (physical-adoption-attempt-test-point :after-rename path))
                :fsync-directory-function
                (lambda (target)
                  (fsync-directory target)
                  (physical-adoption-attempt-test-point :after-directory-fsync path)))
      (write-sequence image stream))
    (physical-adoption-attempt-test-point :before-readback path)
    (physical-adoption-check-owner owner)
    (let ((actual (physical-adoption-attempt-read owner)))
      (unless (equal candidate actual) (error "adoption attempt readback differs"))
      (physical-adoption-attempt-test-point :after-readback path)
      actual)))

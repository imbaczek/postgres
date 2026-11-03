(in-package #:pg)

;; The attempt freezes the original lease request, physical source connection,
;; and stopped local directory, lock, and PID evidence before a future lease RPC.
;; It is a foundation for reconciliation, not a driver or installation authority.
;; The source contains reversible credentials and the request contains a bearer
;; token.  Never log the record or its encoded bytes.

(defconstant +physical-reseed-attempt-maximum-bytes+ (* 3 1024 1024))

(defparameter *physical-reseed-attempt-keys* '(:version :phase :local :request :source))
(defparameter *physical-reseed-local-keys* '(:directory :device :inode :lock-device :lock-inode :pid))

;; Preserve the complete PID file as opaque evidence.  A negative single-user
;; PID is normalized only for liveness checks, never in the saved bytes.

(defun physical-reseed-attempt-pid-copy (pid)
  (when pid
    (physical-follow-check-plist pid '(:pid :identity :bytes))
    (let* ((identity (getf pid :identity))
           (bytes (physical-libpq-datum-octets (getf pid :bytes) 16384)))
      (unless (and (typep (getf pid :pid) '(integer 1 2147483647))
                   (listp identity) (= 8 (length identity))
                   (every (lambda (value) (typep value '(unsigned-byte 64))) (subseq identity 0 6))
                   (every (lambda (value) (typep value '(signed-byte 64))) (subseq identity 6))
                   (= 1 (fourth identity)) (<= 1 (sixth identity) 16384)
                   (= (sixth identity) (length bytes))
                   (let ((actual (ignore-errors
                                  (parse-integer (bytes->string (subseq bytes 0 (position 10 bytes)))))))
                     (and actual (= (abs actual) (getf pid :pid)))))
        (error "invalid saved reseed PID evidence"))
      (list :pid (getf pid :pid) :identity (copy-list identity)
            :bytes (physical-libpq-octets-datum bytes)))))

(defun physical-reseed-attempt-local-copy (local)
  (physical-follow-check-plist local *physical-reseed-local-keys*)
  (unless (and (physical-adoption-attempt-directory-p (getf local :directory))
               (physical-adoption-attempt-u64-fields-p local '(:device :inode :lock-device :lock-inode)))
    (error "invalid stopped reseed directory identity"))
  (list :directory (copy-seq (getf local :directory))
        :device (getf local :device) :inode (getf local :inode)
        :lock-device (getf local :lock-device) :lock-inode (getf local :lock-inode)
        :pid (physical-reseed-attempt-pid-copy (getf local :pid))))

(defun physical-reseed-attempt-stage-copy (stage local)
  (physical-follow-check-plist stage '(:directory :device :inode))
  (unless (and (physical-adoption-attempt-directory-p (getf stage :directory))
               (notany (lambda (part) (member part '(:up :back)))
                       (pathname-directory (pathname (getf stage :directory))))
               (physical-adoption-attempt-u64-fields-p stage '(:device :inode))
               (= (getf stage :device) (getf local :device))
               (not (uiop:subpathp (pathname (getf stage :directory)) (pathname (getf local :directory))))
               (not (uiop:subpathp (pathname (getf local :directory)) (pathname (getf stage :directory)))))
    (error "reseed receive stage must be separate and on the target filesystem"))
  (list :directory (copy-seq (getf stage :directory))
        :device (getf stage :device) :inode (getf stage :inode)))

(defun physical-reseed-attempt-copy (record)
  (let ((stage-p (loop for key in record by #'cddr thereis (eq key :stage))))
    (physical-follow-check-plist record
                                 (if stage-p (append *physical-reseed-attempt-keys* '(:stage))
                                     *physical-reseed-attempt-keys*))
    (unless (and (eql 1 (getf record :version))
                 (member (getf record :phase) '(:premanifest :aborting :aborted)))
      (error "invalid reseed attempt version or phase"))
    (let ((source (physical-follow-source-copy (getf record :source)))
          (local (physical-reseed-attempt-local-copy (getf record :local))))
      (unless (eq :physical
                  (physical-libpq-frozen-profile-mode (nth-value 1 (physical-follow-source-profile source))))
        (error "reseed requires a saved physical replication connection"))
      (append
       (list :version 1 :phase (getf record :phase) :local local
             :request (physical-backup-lease-request-copy (getf record :request)) :source source)
       (when stage-p (list :stage (physical-reseed-attempt-stage-copy (getf record :stage) local)))))))

(defun physical-reseed-attempt-image (record)
  (let* ((text (physical-bootstrap-manifest-text (physical-reseed-attempt-copy record)))
         (image (string->bytes (format nil "PGRESEEDATTEMPT1 ~8,'0X~%~a~%"
                                       (wal-crc32 (string->bytes text)) text))))
    (unless (<= (length image) +physical-reseed-attempt-maximum-bytes+)
      (error "reseed attempt exceeds its private record bound"))
    image))

;; The checksum, literal parser, and canonical re-encoding validate structure.
;; They do not establish that the source is live or the control file is clean.

(defun physical-reseed-attempt-decode (bytes)
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (<= (length bytes) +physical-reseed-attempt-maximum-bytes+))
    (error "invalid reseed attempt byte vector"))
  (let* ((text (bytes->string bytes))
         (newline (position #\Newline text))
         (prefix "PGRESEEDATTEMPT1 "))
    (unless (and newline (= newline (+ (length prefix) 8)) (uiop:string-prefix-p prefix text))
      (error "invalid reseed attempt envelope"))
    (let* ((body (subseq text (1+ newline)))
           (crc (parse-integer text :start (length prefix) :end newline :radix 16)))
      (unless (and (plusp (length body)) (char= #\Newline (char body (1- (length body))))
                   (= crc (wal-crc32 (string->bytes (subseq body 0 (1- (length body)))))))
        (error "reseed attempt checksum mismatch"))
      (let ((record (physical-reseed-attempt-copy (recovery-parse-manifest-text body))))
        (unless (equalp bytes (physical-reseed-attempt-image record))
          (error "noncanonical reseed attempt record"))
        record))))

;; Phases describe durable intent.  Keep all other fields fixed so a retry
;; cannot silently select another request, source, or local PID generation.

(defun physical-reseed-attempt-check-transition (previous candidate)
  (let* ((previous (physical-reseed-attempt-copy previous))
         (candidate (physical-reseed-attempt-copy candidate))
         (old (getf previous :phase))
         (new (getf candidate :phase)))
    (unless (and (every (lambda (key) (equal (getf previous key) (getf candidate key)))
                        '(:version :local :request :source :stage))
                 (or (eq old new)
                     (member (list old new) '((:premanifest :aborting) (:aborting :aborted)) :test #'equal)))
      (error "reseed attempt cannot rebind or reverse its saved intent"))
    t))

;; Bind the attempt to the stopped root, its held lock, and exact PID evidence.
;; This ownership does not confer the clean-adoption capability.

(defun physical-reseed-attempt-local-facts (owner)
  (physical-reseed-check-owner owner)
  (let ((pid (physical-reseed-owner-old-pid owner)))
    (physical-reseed-attempt-local-copy
     (list :directory (namestring (physical-adoption-owner-directory owner))
           :device (physical-adoption-owner-device owner) :inode (physical-adoption-owner-inode owner)
           :lock-device (physical-adoption-owner-lock-device owner)
           :lock-inode (physical-adoption-owner-lock-inode owner)
           :pid (when pid
                  (list :pid (getf pid :pid) :identity (getf pid :identity)
                        :bytes (physical-libpq-octets-datum (getf pid :bytes))))))))

(in-package #:pg-physical-tests)

(defun backup-lease-test-request ()
  (let ((identity (copy-seq "backup-lease-codec")))
    (list :version 1 :name (copy-seq "reseed_0123456789abcdef0123456789abcdef")
          :token (make-string 64 :initial-element #\a)
          :system-id (pg::physical-store-update-digest #xCBF29CE484222325
                                                       (pg::string->bytes identity))
          :cluster-identity identity :timeline 2)))

(defun backup-lease-test-record (&optional (phase :requested))
  (list :version 1 :request (backup-lease-test-request) :phase phase
        :base (when (eq phase :ready)
                (list :checkpoint-start 32 :base 152 :manifest-digest 991))
        :outcome nil :successor nil))

(cl-spec:defproperty backup-lease-envelope-preserves-base-and-token
    ((digest (range integer 0 18446744073709551615))
     (nibble (range integer 0 15)))
  (:about pg::physical-backup-lease-image)
  (:trials (:normal 100))
  (let ((record (backup-lease-test-record :ready)))
    (setf (getf (getf record :base) :manifest-digest) digest
          (getf (getf record :request) :token)
          (make-string 64 :initial-element (char "0123456789abcdef" nibble)))
    (equal record (pg::physical-backup-lease-decode
                   (pg::physical-backup-lease-image record)))))

(deftest generated-backup-lease-envelope-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-envelope-preserves-base-and-token
                                         :seed 89 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(cl-spec:defproperty backup-lease-terminal-attempts-cannot-capture-again
    ((digest (range integer 0 18446744073709551615))
     (outcome (range integer 0 1))
     (phase (range integer 0 3)))
  (:about pg::physical-backup-lease-transition)
  (:trials (:normal 100))
  (let ((closed (backup-lease-test-record :ready)))
    (setf (getf (getf closed :base) :manifest-digest) digest
          (getf closed :phase) :closed
          (getf closed :outcome) (if (zerop outcome) :aborted :handed-off)
          (getf closed :successor)
          (unless (zerop outcome) (list :name "follow_ready" :anchor-digest digest)))
    (let* ((candidate (backup-lease-test-record (nth phase '(:requested :capturing :ready :interrupted))))
           (before (pg::physical-bootstrap-manifest-text closed)))
      (and (equal closed (pg::physical-backup-lease-transition closed closed))
           (handler-case
               (progn (pg::physical-backup-lease-transition closed candidate) nil)
             (error () t))
           (equal before (pg::physical-bootstrap-manifest-text closed))))))

(deftest generated-backup-lease-terminal-fence
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-terminal-attempts-cannot-capture-again
                                         :seed 90 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-copies-all-mutable-request-and-base-values
    (let* ((record (backup-lease-test-record :ready))
           (before (pg::physical-bootstrap-manifest-text record))
           (copy (pg::physical-backup-lease-copy record)))
      (setf (char (getf (getf copy :request) :name) 7) #\f
            (char (getf (getf copy :request) :token) 0) #\b
            (char (getf (getf copy :request) :cluster-identity) 0) #\B
            (getf (getf copy :base) :manifest-digest) 992)
      (ok (equal before (pg::physical-bootstrap-manifest-text record)))
      (ok (= 991 (getf (getf record :base) :manifest-digest)))))

(deftest backup-lease-request-rejects-foreign-and-unbounded-identities
    (dolist (mode '(:extra :duplicate :version :name :token :identity :system :timeline))
      (let ((record (backup-lease-test-record)))
        (ecase mode
          (:extra (setf (getf record :request)
                        (append (getf record :request) (list :extra 1))))
          (:duplicate (setf (getf record :request)
                            (append (getf record :request) (list :timeline 2))))
          (:version (setf (getf (getf record :request) :version) 2))
          (:name (setf (getf (getf record :request) :name) "../other"))
          (:token (setf (getf (getf record :request) :token) (make-string 64 :initial-element #\A)))
          (:identity
           (let ((identity (make-string 1025 :initial-element #\x)))
             (setf (getf (getf record :request) :cluster-identity) identity
                   (getf (getf record :request) :system-id)
                   (pg::physical-store-update-digest #xCBF29CE484222325 (pg::string->bytes identity)))))
          (:system (incf (getf (getf record :request) :system-id)))
          (:timeline (setf (getf (getf record :request) :timeline) 0)))
        (ok (intake-fails-p (lambda () (pg::physical-backup-lease-image record)))
            (format nil "~s" mode)))))

(deftest backup-lease-rejects-oversized-identity-before-utf8-allocation
    (let* ((request (backup-lease-test-request))
           (identity (make-string 1025 :initial-element #\x))
           (original (symbol-function 'pg::string->bytes))
           (encoded nil))
      (setf (getf request :cluster-identity) identity)
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::string->bytes)
                   (lambda (value)
                     (when (eq value identity) (setf encoded t))
                     (funcall original value)))
             (ok (intake-fails-p (lambda () (pg::physical-backup-lease-request-copy request))))
             (ok (not encoded)))
        (setf (symbol-function 'pg::string->bytes) original))))

(deftest backup-lease-identity-bound-counts-utf8-bytes
    (dolist (pair (list (list 512 t) (list 513 nil)))
      (let* ((identity (make-string (first pair) :initial-element (code-char #xe9)))
             (record (backup-lease-test-record))
             (request (getf record :request)))
        (setf (getf request :cluster-identity) identity
              (getf request :system-id)
              (pg::physical-store-update-digest #xCBF29CE484222325 (pg::string->bytes identity)))
        (if (second pair)
            (ok (equal record (pg::physical-backup-lease-decode (pg::physical-backup-lease-image record))))
            (ok (intake-fails-p (lambda () (pg::physical-backup-lease-image record))))))))

(deftest backup-lease-decoder-requires-an-octet-vector
    (dolist (value (list nil (list 0 1) "PGBACKUPLEASE1" (vector 0 1) 8192))
      (let ((message (handler-case (progn (pg::physical-backup-lease-decode value) nil)
                       (error (condition) (princ-to-string condition)))))
        (ok (and message (search "requires an octet vector" message))))))

(deftest backup-lease-record-refuses-incoherent-phases-and-coordinates
    (dolist (mode '(:missing-base :early-base :bad-length :negative :unknown
                    :outcome :successor :handoff-without-base :self-successor))
      (let ((record (backup-lease-test-record :ready)))
        (ecase mode
          (:missing-base (setf (getf record :base) nil))
          (:early-base (setf (getf record :phase) :capturing))
          (:bad-length (incf (getf (getf record :base) :base)))
          (:negative (setf (getf (getf record :base) :manifest-digest) -1))
          (:unknown (setf (getf record :phase) :recapture))
          (:outcome (setf (getf record :outcome) :aborted))
          (:successor (setf (getf record :successor) (list :name "follow" :anchor-digest 1)))
          (:handoff-without-base
           (setf (getf record :phase) :closing (getf record :outcome) :handed-off
                 (getf record :base) nil
                 (getf record :successor) (list :name "follow" :anchor-digest 1)))
          (:self-successor
           (setf (getf record :phase) :closing (getf record :outcome) :handed-off
                 (getf record :successor)
                 (list :name (getf (getf record :request) :name) :anchor-digest 1))))
        (ok (intake-fails-p (lambda () (pg::physical-backup-lease-image record)))
            (format nil "~s" mode)))))

(deftest backup-lease-transition-freezes-request-base-and-terminal-intent
    (let* ((requested (backup-lease-test-record))
           (capturing (backup-lease-test-record :capturing))
           (ready (backup-lease-test-record :ready))
           (interrupted (copy-tree ready))
           (closing (copy-tree ready))
           (closed nil))
      (setf (getf interrupted :phase) :interrupted
            (getf closing :phase) :closing (getf closing :outcome) :handed-off
            (getf closing :successor) (list :name "follow_ready" :anchor-digest 123)
            closed (copy-tree closing)
            (getf closed :phase) :closed)
      (dolist (pair (list (list nil requested) (list requested capturing)
                          (list capturing ready) (list ready interrupted)
                          (list ready closing) (list closing closed) (list closed closed)))
        (ok (equal (second pair) (pg::physical-backup-lease-transition (first pair) (second pair)))))
      (dolist (pair (list (list nil ready) (list requested ready)
                          (list interrupted ready) (list closed capturing)
                          (list closed closing) (list ready capturing)))
        (ok (intake-fails-p
             (lambda () (pg::physical-backup-lease-transition (first pair) (second pair))))))
      (let ((changed (copy-tree closing)))
        (setf (getf (getf changed :base) :manifest-digest) 992)
        (ok (intake-fails-p (lambda () (pg::physical-backup-lease-transition ready changed)))))
      (let ((changed (copy-tree capturing)))
        (setf (getf (getf changed :request) :token) (make-string 64 :initial-element #\b))
        (ok (intake-fails-p (lambda () (pg::physical-backup-lease-transition requested changed)))))))

(deftest backup-lease-abort-before-capture-is-a-terminal-request-tombstone
    (let ((closing (backup-lease-test-record)))
      (setf (getf closing :phase) :closing (getf closing :outcome) :aborted)
      (ok (equal closing (pg::physical-backup-lease-transition nil closing)))
      (let ((closed (copy-tree closing)))
        (setf (getf closed :phase) :closed)
        (ok (equal closed (pg::physical-backup-lease-transition closing closed)))
        (ok (intake-fails-p
             (lambda () (pg::physical-backup-lease-transition closed (backup-lease-test-record))))))))

(deftest backup-lease-envelope-refuses-damage-extra-forms-and-oversize
    (let* ((record (backup-lease-test-record :ready))
           (bytes (pg::physical-backup-lease-image record))
           (damaged (copy-seq bytes)))
      (setf (aref damaged (1- (length damaged))) 0)
      (ok (intake-fails-p (lambda () (pg::physical-backup-lease-decode damaged))))
      (ok (intake-fails-p
           (lambda () (pg::physical-backup-lease-decode
                       (make-array 8193 :element-type '(unsigned-byte 8) :initial-element 0)))))
      (dolist (suffix '(" NIL" " #.(error \"reader evaluation\")"))
        (let* ((text (concatenate 'string (pg::physical-bootstrap-manifest-text record) suffix))
               (image (pg::string->bytes
                       (format nil "PGBACKUPLEASE1 ~8,'0X~%~a~%"
                               (pg::wal-crc32 (pg::string->bytes text)) text))))
          (ok (intake-fails-p (lambda () (pg::physical-backup-lease-decode image))))))))

(deftest backup-lease-envelope-rejects-body-corruption-and-noncanonical-whitespace
    (let* ((record (backup-lease-test-record :ready))
           (image (pg::physical-backup-lease-image record))
           (damaged (copy-seq image))
           (body (1+ (position 10 image))))
      (setf (aref damaged (1+ body)) (char-code #\Space))
      (let ((message (handler-case (progn (pg::physical-backup-lease-decode damaged) nil)
                       (error (condition) (princ-to-string condition)))))
        (ok (and message (search "checksum mismatch" message))))
      (let* ((text (concatenate 'string " " (pg::physical-bootstrap-manifest-text record)))
             (noncanonical (pg::string->bytes
                            (format nil "PGBACKUPLEASE1 ~8,'0X~%~a~%"
                                    (pg::wal-crc32 (pg::string->bytes text)) text))))
        (ok (intake-fails-p (lambda () (pg::physical-backup-lease-decode noncanonical)))))))

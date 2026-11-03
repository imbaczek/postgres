(in-package #:pg-physical-tests)

(defun reseed-attempt-test-record (&optional owner)
  (let* ((info (pg::physical-libpq-decode-connection-info
                (libpq-test-connection-body
                 '(("replication" "true") ("password" "journal-secret")) "replication")))
         (pid (when owner (pg::physical-reseed-owner-old-pid owner))))
    (list :version 1 :phase :premanifest
          :local (list :directory (if owner (namestring (pg::physical-adoption-owner-directory owner))
                                      "/tmp/reseed-attempt/")
                       :device (if owner (pg::physical-adoption-owner-device owner) 1)
                       :inode (if owner (pg::physical-adoption-owner-inode owner) 2)
                       :lock-device (if owner (pg::physical-adoption-owner-lock-device owner) 1)
                       :lock-inode (if owner (pg::physical-adoption-owner-lock-inode owner) 3)
                       :pid (when pid
                              (list :pid (getf pid :pid) :identity (copy-list (getf pid :identity))
                                    :bytes (pg::physical-libpq-octets-datum (getf pid :bytes)))))
          :request (backup-lease-test-request)
          :source (list :conninfo "host=localhost password=journal-secret" :fallback "reseed"
                        :owner (pg::physical-libpq-frozen-profile-datum
                                (pg::make-physical-libpq-frozen-profile info))))))

(cl-spec:defproperty reseed-attempt-codec-preserves-intent-and-private-ownership
    ((number (range integer 0 18446744073709551615)) (phase (range integer 0 2)))
  (:about pg::physical-reseed-attempt-copy pg::physical-reseed-attempt-decode)
  (:trials (:normal 100))
  (let ((record (reseed-attempt-test-record)))
    (setf (getf record :phase) (nth phase '(:premanifest :aborting :aborted))
          (getf (getf record :local) :lock-inode) number)
    (let ((checked (pg::physical-reseed-attempt-copy record)))
      (equal checked (pg::physical-reseed-attempt-decode
                      (pg::physical-reseed-attempt-image checked))))))

(deftest generated-reseed-attempt-codec-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-attempt-codec-preserves-intent-and-private-ownership
                                         :seed 94 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-attempt-codec-refuses-rebinding-and-reversing-cancellation
    (let* ((record (reseed-attempt-test-record))
           (aborting (copy-tree record))
           (aborted (copy-tree record)))
      (setf (getf aborting :phase) :aborting (getf aborted :phase) :aborted)
      (ok (pg::physical-reseed-attempt-check-transition record record))
      (ok (pg::physical-reseed-attempt-check-transition record aborting))
      (ok (pg::physical-reseed-attempt-check-transition aborting aborted))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-check-transition record aborted))))
      (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-check-transition aborted record))))
      (dolist (key '(:local :source :request))
        (let ((changed (copy-tree record)))
          (ecase key
            (:local (incf (getf (getf changed :local) :lock-inode)))
            (:source (setf (getf (getf changed :source) :fallback) "different"))
            (:request (setf (getf (getf changed :request) :token) (make-string 64 :initial-element #\b))))
          (ok (intake-fails-p
               (lambda () (pg::physical-reseed-attempt-check-transition record changed))))))))

(deftest reseed-attempt-codec-rejects-damage-noncanonical-and-unsafe-fields
    (let* ((record (reseed-attempt-test-record))
           (bytes (pg::physical-reseed-attempt-image record))
           (bad (copy-seq bytes)))
      (setf (aref bad (1- (length bad))) 0)
      (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-decode bad))))
      (ok (intake-fails-p
           (lambda () (pg::physical-reseed-attempt-decode
                       (make-array (1+ pg::+physical-reseed-attempt-maximum-bytes+)
                                   :element-type '(unsigned-byte 8))))))
      (dolist (key '(:version :phase :local :request :source))
        (let ((bad (copy-tree record)))
          (setf (getf bad key) :invalid)
          (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy bad))))))
      (let ((bad (copy-tree record)))
        (setf (getf bad :source) (libpq-test-source "database-mode" 5439))
        (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy bad)))))
      (let ((bad (copy-tree record)))
        (setf (getf (getf bad :local) :directory) "../relative/")
        (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy bad)))))))

(deftest reseed-attempt-pid-codec-preserves-opaque-bytes-and-copies-input
    (let* ((record (reseed-attempt-test-record))
           (bytes (make-array 8 :element-type '(unsigned-byte 8)
                                :initial-contents '(45 49 50 10 0 255 13 10)))
           (pid (list :pid 12 :identity '(1 2 33152 1 1000 8 -1 -1)
                      :bytes (pg::physical-libpq-octets-datum bytes))))
      (setf (getf (getf record :local) :pid) pid)
      (let* ((copy (pg::physical-reseed-attempt-copy record))
             (copied-pid (getf (getf copy :local) :pid)))
        (ok (equal copy (pg::physical-reseed-attempt-decode (pg::physical-reseed-attempt-image record))))
        (ok (equalp bytes (pg::physical-libpq-datum-octets (getf copied-pid :bytes))))
        (setf (first (getf pid :identity)) 9)
        (ok (= 1 (first (getf copied-pid :identity))))
        (setf (getf pid :pid) 13)
        (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy record)))))))

(deftest reseed-attempt-envelope-refuses-valid-crc-noncanonical-and-reader-forms
    (let* ((record (reseed-attempt-test-record))
           (reordered (loop for key in '(:source :request :local :phase :version)
                            append (list key (getf record key))))
           (text (pg::physical-bootstrap-manifest-text reordered)))
      (dolist (body (list text (concatenate 'string text " nil") "#.(error \"reader evaluation\")"))
        (let ((image (pg::string->bytes
                      (format nil "PGRESEEDATTEMPT1 ~8,'0X~%~a~%"
                              (pg::wal-crc32 (pg::string->bytes body)) body))))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-decode image))))))))

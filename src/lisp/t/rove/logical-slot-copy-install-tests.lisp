(in-package #:pg)

(cl-spec:defproperty installation-handoffs-reject-unregistered-objects
    ((word (range integer 0 18446744073709551615)))
  (:about logical-slot-copy-install-handoff-image logical-slot-copy-install-consume-handoff)
  (:trials (:normal 200))
  (let* ((operation (%make-logical-slot-copy-install-operation
                     :device word :inode word :lock-device word :lock-inode word
                     :active-p t :thread sb-thread:*current-thread*))
         (handoff (%make-logical-slot-copy-install-handoff :operation operation)))
    (every (lambda (function)
             (handler-case (progn (funcall function operation handoff) nil)
               (error (condition) (search "scope ended" (princ-to-string condition)))))
           (list #'logical-slot-copy-install-handoff-image
                 #'logical-slot-copy-install-consume-handoff))))

(rove:deftest installation-handoffs-generated-registry-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'installation-handoffs-reject-unregistered-objects
                                       :seed 7529 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest installation-handoff-reentry-is-rejected-before-locks
    (let ((*logical-slot-copy-install-operations* (list :occupied)) (called nil))
      (dolist (function (list #'call-with-logical-slot-copy-install-handoff
                              #'call-with-logical-slot-copy-install-operation-under-locks))
        (rove:ok (handler-case
                     (progn (funcall function (lambda (&rest values)
                                                (declare (ignore values)) (setf called t))) nil)
                   (error (condition) (search "already active" (princ-to-string condition))))))
      (rove:ok (not called))))

(cl-spec:defproperty staged-node-images-do-not-alias-node-metadata
    ((number (range integer 0 18446744073709551615)))
  (:about logical-slot-copy-install-stage-node-image)
  (:trials (:normal 200))
  (let* ((nodes
           (loop for key in '(:root :work :slot-root :slot-work :container :slot-container
                              :journal :pending-file :active-file)
                 for fd from 10
                 append (list key (%make-logical-slot-copy-install-node
                                   :fd fd :name (copy-seq "Name") :kind :file
                                   :identity (list number fd 42)))))
         (image (logical-slot-copy-install-stage-node-image nodes))
         (before (logical-slot-copy-import-state-image image))
         (node (getf nodes :container)))
    (setf (char (logical-slot-copy-install-node-name node) 0) #\n
          (first (logical-slot-copy-install-node-identity node)) (logxor number 1))
    (and (logical-slot-copy-committed-image-equal-p image before)
         (not (logical-slot-copy-committed-image-equal-p
               image (logical-slot-copy-install-stage-node-image nodes))))))

(rove:deftest staged-node-images-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'staged-node-images-do-not-alias-node-metadata
                                       :seed 7527 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest staged-files-reject-unowned-operations-and-invalid-read-bounds
    (let ((*logical-slot-copy-install-file-scope* nil) (called nil))
      (rove:ok (handler-case
                   (progn (logical-slot-copy-install-open-node nil "unused" :directory nil t t)
                          (setf called t) nil)
                 (error () t)))
      (rove:ok (not called))
      (dolist (size (list -1 (1+ +logical-slot-copy-install-maximum-bytes+) :invalid))
        (rove:ok (handler-case (progn (logical-slot-copy-install-read-node nil size) nil)
                   (error (condition) (search "read bound" (princ-to-string condition)))))))
  (rove:ok (handler-case
               (progn (logical-slot-copy-install-check-stage
                       nil nil nil (%make-logical-slot-copy-install-stage)) nil)
             (error () t))))

(rove:deftest staged-node-images-reject-extra-and-cyclic-shapes
    (dolist (nodes (list nil (make-list 20 :initial-element :foreign)
                         (let ((cycle (list :root nil))) (setf (cddr cycle) cycle) cycle)))
      (rove:ok (handler-case (progn (logical-slot-copy-install-stage-node-image nodes) nil)
                 (error () t)))))

(defun unit-logical-slot-install-value (word generation &optional same-mount)
  (let* ((certificate (unit-logical-import-value
                       :profile (if (oddp word) :text :binary-bytea)
                       :cause (nth (mod word 3) '(:none :wal-removed :wal-level-insufficient))))
         (mount (1+ (mod word 18446744073709551615)))
         (slot-device (if same-mount word (logxor word #x8000000000000000)))
         (slot-mount (if same-mount mount (1+ (mod (logxor word 255) 18446744073709551615)))))
    (setf (getf certificate :device) word)
    (list :certificate (logical-slot-copy-import-image certificate)
          :evidence-digest word
          :stage-id (logical-slot-generation-token (logior generation (ash 1 127)))
          :container (list word 10 mount)
          :slot-root (list slot-device 20 slot-mount)
          :slot-work (list slot-device 21 slot-mount)
          :slot-container (list slot-device 22 slot-mount)
          :journal (list word 11 mount)
          :pending-file (list word 12 mount)
          :active-file (list word 13 mount)
          :entries (list (list "fixture" (list slot-device 30 slot-mount)
                               (list slot-device 31 slot-mount))))))

(cl-spec:defproperty slot-installation-journal-preserves-identities
    ((word (range integer 0 18446744073709551615))
     (generation (range integer 0 340282366920938463463374607431768211455))
     (same-mount (member nil t)))
  (:about logical-slot-copy-install-encode logical-slot-copy-install-decode)
  (:trials (:normal 200))
  (let* ((value (unit-logical-slot-install-value word generation same-mount))
         (image (logical-slot-copy-install-encode value))
         (before (copy-seq image))
         (decoded (logical-slot-copy-install-decode image)))
    (and (logical-slot-copy-committed-image-equal-p value decoded)
         (equalp before image)
         (equalp image (logical-slot-copy-install-encode decoded))
         (not (eq (getf value :certificate) (getf decoded :certificate))))))

(rove:deftest slot-installation-journal-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'slot-installation-journal-preserves-identities
                                       :seed 7523 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest slot-installation-active-certificate-changes-only-phase
    (let* ((value (unit-logical-slot-install-value 18446744073709551615 42))
           (pending (logical-slot-copy-import-decode (getf value :certificate)))
           (before (logical-slot-copy-import-image pending))
           (active (logical-slot-copy-import-decode (logical-slot-copy-install-active-image pending))))
      (rove:ok (eq :active (getf active :phase)))
      (rove:ok (eq :pending (getf pending :phase)))
      (setf (getf active :phase) :pending)
      (rove:ok (equalp before (logical-slot-copy-import-image active)))
      (rove:ok (equalp before (logical-slot-copy-import-image pending)))
      (rove:ok (handler-case
                   (progn
                     (setf (getf pending :phase) :active)
                     (logical-slot-copy-install-active-image pending) nil)
                 (error () t)))))

(rove:deftest slot-installation-journal-preserves-two-slot-order
    (let* ((value (unit-logical-slot-install-value 1 42))
           (certificate (logical-slot-copy-import-decode (getf value :certificate)))
           (source (unit-logical-provenance-slot :generation 2))
           (parent (getf value :slot-root)))
      (setf (wal-replication-slot-name source) "fixture_2")
      (let* ((bytes (wal-encode-replication-slot source))
             (row (list "fixture_2" (logical-slot-generation-token 2)
                        (logical-slot-copy-import-digest bytes)
                        (bytes->string
                         (logical-slot-copy-import-child-image
                          source (logical-slot-parse-generation (getf certificate :owner)) :none))
                        :eligible)))
        (setf (getf certificate :slots) (append (getf certificate :slots) (list row))
              (getf value :certificate) (logical-slot-copy-import-image certificate)
              (getf value :entries)
              (append (getf value :entries)
                      (list (list "fixture_2" (list (first parent) 32 (third parent))
                                  (list (first parent) 33 (third parent)))))))
      (let ((decoded (logical-slot-copy-install-decode (logical-slot-copy-install-encode value))))
        (rove:ok (equal '("fixture" "fixture_2") (mapcar #'first (getf decoded :entries))))
        (rove:ok (logical-slot-copy-committed-image-equal-p value decoded)))
      (setf (getf value :entries) (reverse (getf value :entries)))
      (rove:ok (handler-case (progn (logical-slot-copy-install-encode value) nil) (error () t)))))

(rove:deftest slot-installation-journal-rejects-aliases-mounts-and-uninstallable-rows
    (dolist (change
             (list
              (lambda (value) (setf (getf value :stage-id)
                                    (getf (logical-slot-copy-import-decode
                                           (getf value :certificate)) :owner)))
              (lambda (value) (setf (getf value :journal) (copy-list (getf value :active-file))))
              (lambda (value) (setf (second (getf value :container)) 2))
              (lambda (value) (setf (third (getf value :slot-work)) 0))
              (lambda (value) (incf (third (getf value :slot-container))))
              (lambda (value) (incf (first (getf value :pending-file))))
              (lambda (value) (setf (second (first (getf value :entries)))
                                    (copy-list (getf value :slot-root))))
              (lambda (value) (setf (first (first (getf value :entries))) "foreign"))
              (lambda (value) (setf (getf value :entries) nil))
              (lambda (value)
                (let* ((certificate (logical-slot-copy-import-decode (getf value :certificate)))
                       (row (first (getf certificate :slots))))
                  (setf (fourth row) nil (fifth row) :deferred
                        (getf value :certificate) (logical-slot-copy-import-image certificate))))))
      (let ((value (unit-logical-slot-install-value 1 42)))
        (funcall change value)
        (rove:ok (handler-case (progn (logical-slot-copy-install-encode value) nil) (error () t))))))

(defun unit-logical-slot-install-reseal (bytes)
  (wal-set-u32 bytes 16 (- (length bytes) 24))
  (wal-set-u32 bytes (- (length bytes) 4) (wal-crc32 bytes 0 (- (length bytes) 4)))
  bytes)

(rove:deftest slot-installation-journal-rejects-corrupt-and-noncanonical-frames
    (let* ((value (unit-logical-slot-install-value 1 42))
           (image (logical-slot-copy-install-encode value))
           (count-offset (+ 232 (length (getf value :certificate)))))
      (dolist (length (list 0 19 239 (1- (length image))))
        (rove:ok (handler-case
                     (progn (logical-slot-copy-install-decode (subseq image 0 length)) nil)
                   (error () t))))
      (dolist (change
               (list (lambda (bytes) (setf (aref bytes 0) 0))
                     (lambda (bytes) (setf (aref bytes (1- (length bytes)))
                                           (logxor 1 (aref bytes (1- (length bytes))))))
                     (lambda (bytes)
                       (wal-set-u32 bytes 20 (1+ +logical-slot-copy-import-maximum-bytes+))
                       (unit-logical-slot-install-reseal bytes))
                     (lambda (bytes)
                       (wal-set-u32 bytes count-offset
                                    (1+ +logical-slot-copy-descriptor-maximum-slots+))
                       (unit-logical-slot-install-reseal bytes))
                     (lambda (bytes)
                       (setf (aref bytes (+ count-offset 8)) (char-code #\A))
                       (unit-logical-slot-install-reseal bytes))))
        (let ((bytes (copy-seq image)))
          (funcall change bytes)
          (rove:ok (handler-case (progn (logical-slot-copy-install-decode bytes) nil) (error () t)))))
      (let ((bytes (make-array (1+ (length image)) :element-type '(unsigned-byte 8)
                                                   :initial-element 0)))
        (replace bytes image)
        (unit-logical-slot-install-reseal bytes)
        (rove:ok (handler-case (progn (logical-slot-copy-install-decode bytes) nil) (error () t))))))

(rove:deftest slot-installation-journal-fences-startup-before-slot-cleanup
    (call-with-unit-promotion-directory
     (lambda (context root)
       (declare (ignore root))
       (let* ((fd (physical-image-context-root-fd context))
              (path (merge-pathnames "logical-slot-copy.install" *pgdata*))
              (*physical-recovery-state* nil))
         (dolist (kind '(:directory :regular :symlink :fifo))
           (unwind-protect
                (progn
                  (ecase kind
                    (:directory (sb-posix:mkdir (namestring path) #o700))
                    (:regular (with-open-file (out path :direction :output :if-exists :error)
                                (write-line "not a journal" out)))
                    (:symlink (sb-posix:symlink "missing-install-target" (namestring path)))
                    (:fifo (sb-posix:mkfifo (namestring path) #o600)))
                  (let ((before (physical-image-file-facts fd "logical-slot-copy.install")))
                    (dolist (function (list #'logical-slot-copy-preflight-startup
                                            #'logical-slot-copy-check-promotion))
                      (rove:ok (handler-case (progn (funcall function) nil)
                                 (error (condition)
                                   (search "child-owner import" (princ-to-string condition))))))
                    (rove:ok (equal before (physical-image-file-facts fd "logical-slot-copy.install")))))
             (if (eq kind :directory) (sb-posix:rmdir (namestring path))
                 (sb-posix:unlink (namestring path)))))))))

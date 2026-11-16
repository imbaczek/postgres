(in-package #:pg)

(defun unit-continuation-inventory-fixture (rows function)
  (unit-continuation-directory-fixture
   (lambda (parent root)
     (declare (ignore root))
     (call-with-logical-slot-continuation-owned-directory
      parent :slots
      (lambda (directory check)
        (dolist (row rows)
          (sb-posix:mkdir (namestring (merge-pathnames (concatenate 'string (first row) "/")
                                                       (physical-image-fd-path directory))) #o700)
          (call-with-logical-slot-copy-source-proof-fd
           directory (first row) t
           (lambda (child facts)
             (declare (ignore facts))
             (let ((path (merge-pathnames "state" (physical-image-fd-path child))))
               (with-open-file (stream path :direction :output :if-exists :error
                                            :element-type '(unsigned-byte 8))
                 (write-sequence (second row) stream))
               (sb-posix:chmod (namestring path) #o600)))
           :mutable-directory t))
        (funcall function directory check))
      t))))

(defun unit-continuation-inventory-mixed-rows ()
  (let* ((p7 (second (third (multiple-value-list (unit-continuation-active-inputs)))))
         (prefix "PGSLOT1 legacy 0000 0000")
         (legacy (string->bytes (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix)))))
         (physical (wal-encode-replication-slot
                    (make-wal-replication-slot :name "physical" :kind :physical :system-id 42
                                               :database-oid nil :plugin nil :consistent-lsn nil
                                               :restart-lsn 17 :restart-timeline 1 :inactive-since 0))))
    (list (list "physical" physical) (list "legacy" legacy) p7)))

(rove:deftest continuation-slot-inventory-keeps-all-formats-and-exact-bytes
    (let* ((*wal-durable-lsn* 192)
           (rows (unit-continuation-inventory-mixed-rows)))
      (unit-continuation-inventory-fixture
       rows
       (lambda (directory check)
         (let ((fds (length (directory #p"/proc/self/fd/*"))))
           (multiple-value-bind (observed ledger)
               (logical-slot-continuation-read-slot-inventory directory check)
             (rove:ok (logical-slot-continuation-slot-rows-equal-p
                       (sort (copy-list rows) #'string< :key #'first) observed))
             (rove:ok (= (length rows) (length ledger)))
             (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
             (setf (aref (second (first observed)) 0) 0)
             (rove:ok (logical-slot-continuation-slot-rows-equal-p
                       (sort (copy-list rows) #'string< :key #'first)
                       (logical-slot-continuation-read-slot-inventory directory check)))))))))

(rove:deftest continuation-slot-inventory-rejects-temporary-and-unknown-entries
    (let ((*wal-durable-lsn* 192))
      (dolist (kind '(:root-temp :state-temp :extra-child :unknown-root))
        (unit-continuation-inventory-fixture
         (unit-continuation-inventory-mixed-rows)
         (lambda (directory check)
           (let* ((leaf (ecase kind (:root-temp "alpha.tmp/") (:state-temp "alpha/state.tmp")
                               (:extra-child "alpha/extra") (:unknown-root "BAD/")))
                  (path (merge-pathnames leaf (physical-image-fd-path directory)))
                  (fds (length (directory #p"/proc/self/fd/*"))))
             (if (member kind '(:root-temp :unknown-root))
                 (sb-posix:mkdir (namestring path) #o700)
                 (with-open-file (stream path :direction :output :if-exists :error
                                              :element-type '(unsigned-byte 8))
                   (write-byte 9 stream)))
             (rove:ok
              (handler-case (progn (logical-slot-continuation-read-slot-inventory directory check) nil)
                (error () t)))
             (rove:ok (probe-file path))
             (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))))))))

(rove:deftest continuation-target-directory-keeps-binding-and-callback-lifetime
    (let ((*wal-durable-lsn* 192))
      (unit-continuation-inventory-fixture
       (unit-continuation-inventory-mixed-rows)
       (lambda (directory root-check)
         (let ((escaped nil))
           (rove:ok
            (equal '(:target 73)
                   (multiple-value-list
                    (call-with-logical-slot-continuation-target-directory
                     directory "alpha" root-check
                     (lambda (target check)
                       (setf escaped check)
                       (funcall check)
                       (rove:ok
                        (sb-thread:join-thread
                         (sb-thread:make-thread
                          (lambda () (handler-case (progn (funcall check) nil) (error () t))))))
                       (with-open-file (stream (merge-pathnames "scope-probe" (physical-image-fd-path target))
                                               :direction :output :if-exists :error)
                         (write-string "metadata changes are permitted" stream))
                       (funcall check)
                       (values :target 73))))))
           (rove:ok (handler-case (progn (funcall escaped) nil) (error () t)))
           (funcall root-check))))))

(rove:deftest continuation-target-directory-rejects-invalid-name-before-open
    (let ((calls 0) (opens 0)
                    (original (symbol-function 'call-with-logical-slot-continuation-directory-binding)))
      (unwind-protect
           (progn
             (setf (symbol-function 'call-with-logical-slot-continuation-directory-binding)
                   (lambda (&rest arguments) (incf opens) (apply original arguments)))
             (dolist (name '("" "." ".." "alpha.tmp" "alpha/state" "UPPER" ".logical-slot-copy.work"))
               (rove:ok
                (handler-case
                    (progn
                      (call-with-logical-slot-continuation-target-directory
                       -1 name (lambda () (incf calls))
                       (lambda (&rest ignored) (declare (ignore ignored)) (incf calls)))
                      nil)
                  (error () t))))
             (rove:ok (zerop calls))
             (rove:ok (zerop opens)))
        (setf (symbol-function 'call-with-logical-slot-continuation-directory-binding) original))))

(rove:deftest continuation-target-directory-rejects-named-replacement
    (let ((*wal-durable-lsn* 192))
      (unit-continuation-inventory-fixture
       (unit-continuation-inventory-mixed-rows)
       (lambda (directory check)
         (let ((entered nil) (fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok
            (handler-case
                (progn
                  (call-with-logical-slot-continuation-target-directory
                   directory "alpha" check
                   (lambda (target target-check)
                     (declare (ignore target target-check))
                     (setf entered t)
                     (sb-posix:rename (namestring (merge-pathnames "alpha/" (physical-image-fd-path directory)))
                                      (namestring (merge-pathnames "saved/" (physical-image-fd-path directory))))
                     (sb-posix:mkdir (namestring (merge-pathnames "alpha/" (physical-image-fd-path directory))) #o700)))
                  nil)
              (error () t)))
           (rove:ok entered)
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*")))))))))

(rove:deftest continuation-slot-inventory-is-observational
    (let ((*wal-durable-lsn* 192))
      (unit-continuation-inventory-fixture
       (unit-continuation-inventory-mixed-rows)
       (lambda (directory check)
         (let ((opens 0) (syncs 0)
                         (open (symbol-function 'physical-image-openat))
                         (sync (symbol-function 'fsync-directory)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'physical-image-openat)
                        (lambda (parent name flags mode)
                          (incf opens)
                          (assert (zerop (logand flags (logior sb-posix:o-wronly sb-posix:o-rdwr
                                                               sb-posix:o-creat sb-posix:o-trunc sb-posix:o-append))))
                          (funcall open parent name flags mode))
                        (symbol-function 'fsync-directory)
                        (lambda (&rest arguments) (incf syncs) (apply sync arguments)))
                  (funcall (symbol-function 'fsync-directory) (physical-image-fd-path directory))
                  (assert (= 1 syncs))
                  (setf syncs 0)
                  (logical-slot-continuation-read-slot-inventory directory check)
                  (rove:ok (plusp opens))
                  (rove:ok (zerop syncs)))
             (setf (symbol-function 'physical-image-openat) open
                   (symbol-function 'fsync-directory) sync)))))))

(rove:deftest continuation-slot-inventory-rechecks-closed-rows-and-whole-root
    (let ((*wal-durable-lsn* 192))
      (dolist (kind '(:state-inode :directory-inode :valid-rewrite :root-add :row-remove))
        (unit-continuation-inventory-fixture
         (unit-continuation-inventory-mixed-rows)
         (lambda (directory check)
           (let* ((fds (length (directory #p"/proc/self/fd/*")))
                  (hits 0)
                  (*logical-slot-continuation-inventory-test-hook*
                    (lambda (point)
                      (assert (eq point :between-passes))
                      (incf hits)
                      (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
                      (let ((state (merge-pathnames "legacy/state" (physical-image-fd-path directory)))
                            (row (merge-pathnames "legacy/" (physical-image-fd-path directory))))
                        (ecase kind
                          (:state-inode
                           (let ((bytes (read-bounded-regular-file state 2048)))
                             (delete-file state)
                             (with-open-file (stream state :direction :output :if-exists :error
                                                           :element-type '(unsigned-byte 8))
                               (write-sequence bytes stream))
                             (sb-posix:chmod (namestring state) #o600)))
                          ((:directory-inode :row-remove)
                           (sb-posix:rename (namestring row)
                                            (namestring (merge-pathnames "../inventory-saved"
                                                                         (physical-image-fd-path directory)))))
                          (:valid-rewrite
                           (let* ((prefix "PGSLOT1 legacy 0000 0001")
                                  (bytes (string->bytes (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix))))))
                             (wal-decode-replication-slot bytes "legacy")
                             (with-open-file (stream state :direction :output :if-exists :overwrite
                                                           :element-type '(unsigned-byte 8))
                               (write-sequence bytes stream))
                             (sb-posix:chmod (namestring state) #o600)))
                          (:root-add
                           (sb-posix:mkdir (namestring (merge-pathnames "extra/" (physical-image-fd-path directory))) #o700)))
                        (when (eq kind :directory-inode)
                          (sb-posix:mkdir (namestring row) #o700)
                          (with-open-file (stream state :direction :output :if-exists :error
                                                        :element-type '(unsigned-byte 8))
                            (let ((prefix "PGSLOT1 legacy 0000 0000"))
                              (write-sequence (string->bytes (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix)))) stream)))
                          (sb-posix:chmod (namestring state) #o600))))))
             (rove:ok
              (handler-case (progn (logical-slot-continuation-read-slot-inventory directory check) nil)
                (error () t)))
             (rove:ok (= 1 hits))
             (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))))))))

(rove:deftest continuation-slot-inventory-skips-only-the-fixed-workspace
    (let ((*wal-durable-lsn* 192))
      (dolist (kind '(:file :directory :dangling-link))
        (unit-continuation-inventory-fixture
         (unit-continuation-inventory-mixed-rows)
         (lambda (directory check)
           (let* ((name ".logical-slot-copy.work")
                  (path (merge-pathnames name (physical-image-fd-path directory)))
                  (facts (symbol-function 'physical-image-file-facts))
                  (opens (symbol-function 'physical-image-openat))
                  (inspections 0))
             (ecase kind
               (:file (with-open-file (stream path :direction :output :if-exists :error) (write-string "opaque" stream)))
               (:directory (sb-posix:mkdir (namestring path) #o700))
               (:dangling-link (sb-posix:symlink "missing-workspace-target" (namestring path))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'physical-image-file-facts)
                          (lambda (fd &optional (child ""))
                            (when (equal child name) (incf inspections) (error "workspace was inspected"))
                            (funcall facts fd child))
                          (symbol-function 'physical-image-openat)
                          (lambda (fd child flags mode)
                            (when (equal child name) (incf inspections) (error "workspace was opened"))
                            (funcall opens fd child flags mode)))
                    (rove:ok (= 3 (length (logical-slot-continuation-read-slot-inventory directory check))))
                    (rove:ok (zerop inspections)))
               (setf (symbol-function 'physical-image-file-facts) facts
                     (symbol-function 'physical-image-openat) opens))
             (rove:ok (slot-state-lstat path))))))))

(rove:deftest continuation-slot-inventory-enforces-the-obligation-bound
    (let* ((*wal-durable-lsn* 192)
           (rows (loop for i below +logical-slot-continuation-obligation-limit+
                       for name = (format nil "slot_~4,'0d" i)
                       for prefix = (format nil "PGSLOT1 ~a 0 0" name)
                       collect (list name (string->bytes (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix))))))))
      (unit-continuation-inventory-fixture
       rows
       (lambda (directory check)
         (rove:ok (= 1024 (length (logical-slot-continuation-read-slot-inventory directory check))))
         (let ((path (merge-pathnames ".logical-slot-copy.work" (physical-image-fd-path directory))))
           (with-open-file (stream path :direction :output :if-exists :error) (write-string "opaque" stream)))
         (rove:ok (= 1024 (length (logical-slot-continuation-read-slot-inventory directory check))))
         (sb-posix:mkdir (namestring (merge-pathnames "extra/" (physical-image-fd-path directory))) #o700)
         (let ((calls 0) (original (symbol-function 'logical-slot-continuation-read-slot-inventory-row)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'logical-slot-continuation-read-slot-inventory-row)
                        (lambda (&rest arguments) (incf calls) (apply original arguments)))
                  (rove:ok
                   (handler-case (progn (logical-slot-continuation-read-slot-inventory directory check) nil)
                     (error () t)))
                  (rove:ok (zerop calls)))
             (setf (symbol-function 'logical-slot-continuation-read-slot-inventory-row) original)))))))

(rove:deftest continuation-slot-inventory-rejects-links-without-leaking
    (let ((*wal-durable-lsn* 192))
      (dolist (kind '(:child-link :state-link :state-hardlink))
        (unit-continuation-inventory-fixture
         (unit-continuation-inventory-mixed-rows)
         (lambda (directory check)
           (let ((fds (length (directory #p"/proc/self/fd/*")))
                 (row (merge-pathnames "legacy/" (physical-image-fd-path directory)))
                 (state (merge-pathnames "legacy/state" (physical-image-fd-path directory)))
                 (saved (merge-pathnames "../linked-row/" (physical-image-fd-path directory))))
             (if (eq kind :child-link)
                 (progn
                   (sb-posix:rename (namestring row) (namestring saved))
                   (sb-posix:symlink (namestring saved) (string-right-trim "/" (namestring row))))
                 (let ((copy (merge-pathnames "../linked-state" (physical-image-fd-path directory))))
                   (sb-posix:rename (namestring state) (namestring copy))
                   (if (eq kind :state-link)
                       (sb-posix:symlink (namestring copy) (namestring state))
                       (sb-posix:link (namestring copy) (namestring state)))))
             (rove:ok
              (handler-case (progn (logical-slot-continuation-read-slot-inventory directory check) nil)
                (error () t)))
             (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))))))))

(cl-spec:defproperty continuation-slot-inventory-preserves-generated-order-and-formats
    ((count (range integer 1 12)) (seed (range integer 0 255)))
  (:about logical-slot-continuation-read-slot-inventory)
  (:trials (:normal 200))
  (let* ((*wal-durable-lsn* 192)
         (sample (logical-slot-state-decode (second (second (third (multiple-value-list (unit-continuation-active-inputs))))) "alpha"))
         (rows
           (loop for i below count
                 for name = (format nil "slot_~3,'0d" i)
                 collect
                 (list name
                       (case (mod (+ i seed) 3)
                         (0 (let ((prefix (format nil "PGSLOT1 ~a ~a ~a" name
                                                  (if (oddp seed) "0000" "0") (if (oddp i) "0000" "0"))))
                              (string->bytes (format nil "~a ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix))))))
                         (1 (let ((slot (wal-copy-replication-slot-with-profile sample)))
                              (setf (wal-replication-slot-name slot) name)
                              (logical-slot-state-encode slot)))
                         (2 (physical-slot-encode
                             (make-wal-replication-slot :name name :kind :physical :system-id 42
                                                        :database-oid nil :plugin nil :consistent-lsn nil
                                                        :restart-lsn 17 :restart-timeline 1 :inactive-since 0))))))))
    (unit-continuation-inventory-fixture
     (reverse rows)
     (lambda (directory check)
       (multiple-value-bind (observed ledger)
           (logical-slot-continuation-read-slot-inventory directory check)
         (and (= count (length ledger))
              (logical-slot-continuation-slot-rows-equal-p rows observed)))))))

(rove:deftest continuation-slot-inventory-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property
                    'continuation-slot-inventory-preserves-generated-order-and-formats
                    :seed 11509 :profile :normal))))
      (format t "~&SLOT-INVENTORY-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)))
      (rove:ok (= 200 (getf result :trials)))))

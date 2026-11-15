(in-package #:pg)

(defun unit-continuation-active-inputs (&optional (generation 0) (restart 32))
  (let* ((alpha (unit-imported-state-baseline :generation generation))
         (beta (unit-imported-state-baseline :generation (logxor generation 1) :cause :wal-removed))
         (history (timeline-encode-history 2 1 96 "origin" nil)))
    (setf (wal-replication-slot-name alpha) "alpha"
          (wal-replication-slot-name beta) "beta")
    (let* ((baseline (logical-slot-state-encode alpha))
           (lost (logical-slot-state-encode beta)))
      (setf (wal-replication-slot-restart-lsn alpha) restart)
      (values (list :system-id 42 :child-timeline 2 :fork 96
                    :slots (list (list "alpha" nil nil (bytes->string baseline))
                                 (list "beta" nil nil (bytes->string lost))))
              (list 42 nil nil history 2 192)
              (list (list "beta" lost) (list "alpha" (logical-slot-state-encode alpha)))))))

(cl-spec:defproperty continuation-initial-active-keeps-exact-saved-obligations
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (restart (range integer 32 64)))
  (:about logical-slot-continuation-active-snapshot)
  (:trials (:normal 200))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs generation restart)
    (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (snapshot (logical-slot-continuation-active-snapshot binding certificate host rows))
           (plan (logical-slot-continuation-check snapshot))
           (encoded (logical-slot-continuation-encode snapshot)))
      (and (equal '("alpha" "beta") (mapcar #'first (getf snapshot :slots)))
           (equalp (reverse rows) (getf snapshot :slots))
           (not (eq (second (first rows)) (second (second (getf snapshot :slots)))))
           (= 96 (logical-slot-continuation-plan-floor plan))
           (equal '((96 2 :record-start) 2) (logical-slot-continuation-plan-tail-request plan))
           (null (logical-slot-continuation-plan-root-requests plan))
           (null (logical-slot-continuation-plan-run-requests plan))
           (equalp encoded (logical-slot-continuation-encode
                            (logical-slot-continuation-decode encoded)))))))

(rove:deftest continuation-initial-active-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-initial-active-keeps-exact-saved-obligations
                                         :seed 11401 :profile :normal))))
      (format t "~&ACTIVE-CONTRACT status=~s trials=~s seed=11401 digest=~s failure=~s shrunk=~s~%"
              (getf result :status) (getf result :trials) (getf result :definition-digest)
              (getf result :failure) (getf result :shrunk-failure))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-initial-active-rejects-unbound-saved-images
    (dolist (mode '(:empty :duplicate :generation :unknown-name :foreign-system :later-child))
      (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
        (ecase mode
          (:empty (setf rows nil))
          (:duplicate (setf rows (append rows (list (first rows)))))
          (:generation
           (let ((slot (unit-imported-state-baseline :generation 100)))
             (setf (wal-replication-slot-name slot) "alpha"
                   (second (second rows)) (logical-slot-state-encode slot))))
          (:unknown-name (setf (getf certificate :slots) nil))
          (:foreign-system (setf (first host) 100))
          (:later-child (setf (fifth host) 3)))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-active-snapshot
                             (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)) certificate host rows) nil)
                   (error () t))))))

(rove:deftest continuation-initial-active-snapshot-detaches-source-images
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
             (snapshot (logical-slot-continuation-active-snapshot binding certificate host rows))
             (saved (logical-slot-continuation-encode snapshot)))
        (setf (aref binding 0) 99
              (aref (fourth host) 0) 0
              (aref (second (first rows)) 0) 0)
        (rove:ok (equalp saved (logical-slot-continuation-encode snapshot))))))

(rove:deftest continuation-initial-active-does-not-resurrect-historical-slots
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (let* ((current (remove "beta" rows :key #'first :test #'equal))
             (snapshot (logical-slot-continuation-active-snapshot
                        (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3))
                        certificate host current)))
        (rove:ok (= 2 (length (getf certificate :slots))))
        (rove:ok (equal '("alpha") (mapcar #'first (getf snapshot :slots))))
        (rove:ok (logical-slot-continuation-plan-tail-request
                  (logical-slot-continuation-check snapshot))))))

(rove:deftest continuation-tail-files-reject-real-held-and-named-changes
    (dolist (mode '(:replace :truncate :append :symlink))
      (call-with-logical-slot-real-startup-test
       (lambda ()
         (let* ((path (merge-pathnames "continuation-tail-probe" *pgdata*))
                (bytes (make-array 5 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3 4 5))))
           (wal-atomic-write-bytes path bytes)
           (with-slot-state-directory (directory (namestring *pgdata*))
             (multiple-value-bind (stream size) (open-bounded-regular-file path 64)
               (unwind-protect
                    (let* ((facts (physical-image-file-facts (sb-sys:fd-stream-fd stream)))
                           (held (list (list "continuation-tail-probe" stream facts size))))
                      (rove:ok (logical-slot-continuation-check-tail-files directory held))
                      (ecase mode
                        (:replace (wal-atomic-write-bytes path bytes))
                        (:truncate
                         (with-open-file (output path :direction :io :element-type '(unsigned-byte 8)
                                                      :if-exists :overwrite)
                           (sb-posix:ftruncate (sb-sys:fd-stream-fd output) 2)))
                        (:append
                         (with-open-file (output path :direction :output :element-type '(unsigned-byte 8)
                                                      :if-exists :append)
                           (write-byte 6 output)))
                        (:symlink
                         (let ((other (merge-pathnames "continuation-tail-other" *pgdata*)))
                           (wal-atomic-write-bytes other bytes)
                           (delete-file path)
                           (sb-posix:symlink (namestring other) (namestring path)))))
                      (rove:ok (handler-case
                                   (progn (logical-slot-continuation-check-tail-files directory held) nil)
                                 (error () t))))
                 (close stream)))))))))

(rove:deftest continuation-initial-active-validates-row-shapes-before-copying
    (let ((original (symbol-function 'logical-slot-continuation-copy)))
      (unwind-protect
           (dolist (mode '(:cycle :dotted :name :blob))
             (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
               (let ((called nil) (row (first rows)))
                 (ecase mode
                   (:cycle (setf (cddr row) row))
                   (:dotted (setf (cddr row) 1))
                   (:name (setf (first row) (list "beta")))
                   (:blob (setf (second row) (list 1 2 3))))
                 (setf (symbol-function 'logical-slot-continuation-copy)
                       (lambda (value) (declare (ignore value)) (setf called t)
                               (error "malformed row reached copier")))
                 (rove:ok (handler-case
                              (progn
                                (logical-slot-continuation-active-snapshot
                                 (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3))
                                 certificate host rows)
                                nil)
                            (error () t)))
                 (rove:ok (not called))
                 (setf (symbol-function 'logical-slot-continuation-copy) original))))
        (setf (symbol-function 'logical-slot-continuation-copy) original))))

(cl-spec:defproperty continuation-tail-transcript-keeps-coordinates-and-exact-bytes
    ((size (range integer 1 256)) (byte (range integer 0 255))
                                  (carrier (range integer 1 4294967295)))
  (:about logical-slot-continuation-tail-frame)
  (:trials (:normal 200))
  (let* ((raw (make-array size :element-type '(unsigned-byte 8) :initial-element byte))
         (record (make-wal-record :start 96 :end (+ 96 size)))
         (frame (logical-slot-continuation-tail-frame record raw 2 carrier :ordinary 80)))
    (and (= 80 (wal-u64 frame 0)) (= 96 (wal-u64 frame 8))
         (= (+ 96 size) (wal-u64 frame 16))
         (= 2 (wal-u32 frame 24)) (= carrier (wal-u32 frame 28))
         (= 0 (aref frame 32)) (= size (wal-u32 frame 36))
         (= (+ 40 size) (length frame))
         (equalp raw (subseq frame 40)))))

(rove:deftest continuation-tail-transcript-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-tail-transcript-keeps-coordinates-and-exact-bytes
                                         :seed 11402 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-tail-transcript-spills-outside-pgdata-and-unwinds
    (dolist (exit '(:normal :error :throw))
      (call-with-logical-slot-real-startup-test
       (lambda ()
         (let ((path nil) (entered nil)
                          (facts (physical-image-file-facts -100 (namestring *pgdata*)))
                          (fds (length (directory #p"/proc/self/fd/*")))
                          (*logical-capture-memory-bytes* 0))
           (rove:ok
            (eq exit
                (handler-case
                    (catch 'transcript-exit
                      (call-with-logical-slot-continuation-transcript
                       (lambda (spool)
                         (let ((bytes (make-array 128 :element-type '(unsigned-byte 8) :initial-element 9)))
                           (logical-spool-append spool bytes)
                           (logical-spool-seal spool)
                           (setf path (logical-spool-path spool) entered t)
                           (rove:ok path)
                           (rove:ok (not (uiop:subpathp path *pgdata*)))
                           (logical-spool-map-events spool (lambda (observed) (rove:ok (equalp bytes observed))))
                           (sb-thread:with-mutex (*wal-lock*) (wal-require-no-temp-cleanup-under-lock))
                           (ecase exit
                             (:normal :normal)
                             (:error (error "transcript probe"))
                             (:throw (throw 'transcript-exit :throw)))))))
                  (error (condition) (if (eq exit :error) :error (error condition))))))
           (rove:ok entered)
           (rove:ok (not (probe-file path)))
           (rove:ok (not (probe-file (uiop:pathname-directory-pathname path))))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok (equal facts (physical-image-file-facts -100 (namestring *pgdata*)))))))))

(defun unit-continuation-boundary-positions ()
  (let* ((fork 80)
         (eor-end (+ fork +wal-end-of-recovery-record-size+))
         (batch-end (+ eor-end +wal-header-size+))
         (commit-end (+ batch-end +wal-header-size+ 8)))
    (values fork eor-end batch-end commit-end)))

(defun unit-continuation-boundary-records (observe)
  (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
    (declare (ignore commit-end))
    (let* ((eor (wal-encode-end-of-recovery-record fork 0 2 1 2))
           (batch (wal-encode-record +wal-record-batch-start+ 0 eor-end
                                     (make-array 0 :element-type '(unsigned-byte 8))))
           (commit (wal-encode-record +wal-record-commit+ 10 batch-end (wal-encode-commit-timestamp 0))))
      (funcall observe (wal-decode-record eor fork) eor 2 2 :end-of-recovery fork)
      (funcall observe (wal-decode-record batch eor-end) batch 2 2 :ordinary eor-end)
      (funcall observe (wal-decode-record commit batch-end) commit 2 2 :ordinary batch-end))))

(cl-spec:defproperty continuation-boundary-classification-keeps-restart-and-ack-distinct
    ((mode (range integer 0 4)))
  (:about logical-slot-continuation-boundary-observer)
  (:trials (:normal 200))
  (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
    (let ((result
            (handler-case
                (multiple-value-bind (observe finish)
                    (logical-slot-continuation-boundary-observer
                     (nth mode
                          (list (list (list :restart fork 2 :record-start)
                                      (list :restart eor-end 2 :record-start)
                                      (list :restart commit-end 2 :prefix-end)
                                      (list :confirmed commit-end 2 :prefix-end))
                                (list (list :restart (1+ fork) 2 :prefix-end))
                                (list (list :confirmed eor-end 2 :prefix-end))
                                (list (list :restart batch-end 3 :prefix-end))
                                (list (list :confirmed batch-end 2 :prefix-end))))
                     :fork fork)
                  (unit-continuation-boundary-records observe)
                  (funcall finish)
                  :accepted)
              (error () :rejected))))
      (eq result (if (zerop mode) :accepted :rejected)))))

(rove:deftest continuation-boundary-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-boundary-classification-keeps-restart-and-ack-distinct
                                         :seed 11403 :profile :normal))))
      (format t "~&BOUNDARY-CONTRACT status=~s trials=~s seed=11403 digest=~s~%"
              (getf result :status) (getf result :trials) (getf result :definition-digest))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-boundary-requires-records-and-completed-ordinary-acks
    (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
      (multiple-value-bind (observe finish)
          (logical-slot-continuation-boundary-observer
           (list (list :restart fork 2 :record-start) (list :restart eor-end 2 :record-start)
                 (list :restart commit-end 2 :prefix-end)
                 (list :confirmed commit-end 2 :prefix-end))
           :fork fork)
        (unit-continuation-boundary-records observe)
        (rove:ok (funcall finish)))
      (dolist (request (list (list :restart (1+ fork) 2 :prefix-end)
                             (list :confirmed eor-end 2 :prefix-end)
                             (list :restart batch-end 3 :prefix-end)
                             (list :confirmed batch-end 2 :prefix-end)))
        (multiple-value-bind (observe finish)
            (logical-slot-continuation-boundary-observer (list request) :fork fork)
          (unit-continuation-boundary-records observe)
          (rove:ok (handler-case (progn (funcall finish) nil) (error () t))))))
  (multiple-value-bind (observe finish) (logical-slot-continuation-boundary-observer nil)
    (let ((raw (wal-encode-record +wal-record-batch-start+ 0 96
                                  (make-array 0 :element-type '(unsigned-byte 8)))))
      (funcall observe (wal-decode-record raw 96) raw 2 2 :ordinary 96)
      (rove:ok (handler-case (progn (funcall finish) nil) (error () t))))))

(rove:deftest continuation-boundary-rejects-interior-batch-restarts
    (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
      (declare (ignore eor-end commit-end))
      (dolist (role '(:record-start :prefix-end))
        (multiple-value-bind (observe finish)
            (logical-slot-continuation-boundary-observer
             (list (list :restart batch-end 2 role)) :fork fork)
          (unit-continuation-boundary-records observe)
          (rove:ok (handler-case (progn (funcall finish) nil) (error () t))
                   "A commit start and preceding BATCH end cannot advance a slot restart")))))

(rove:deftest continuation-boundary-checkpoint-does-not-certify-an-ack
    (let* ((start 80)
           (raw (wal-encode-record +wal-record-checkpoint+ 0 start
                                   (make-array 0 :element-type '(unsigned-byte 8))))
           (record (wal-decode-record raw start)))
      (dolist (kind '(:confirmed :restart))
        (multiple-value-bind (observe finish)
            (logical-slot-continuation-boundary-observer
             (list (list kind (wal-record-end record) 1 :prefix-end))
             :checkpoint (list start raw))
          (funcall observe record raw 1 1 :ordinary start)
          (rove:ok (handler-case (progn (funcall finish) nil) (error () t))
                   "An exact standalone checkpoint is traversal evidence, not a completed ACK batch")))))

(rove:deftest continuation-boundary-keeps-persisted-pending-batch-start-role
    (multiple-value-bind (fork eor-end batch-end commit-end) (unit-continuation-boundary-positions)
      (declare (ignore batch-end commit-end))
      (multiple-value-bind (observe finish)
          (logical-slot-continuation-boundary-observer
           (list (list :restart eor-end 2 :prefix-end)) :fork fork)
        (unit-continuation-boundary-records observe)
        (rove:ok (handler-case (funcall finish) (error () nil))
                 "A persisted pending restart can use prefix-end geometry at an exact BATCH start"))
      (multiple-value-bind (observe finish)
          (logical-slot-continuation-boundary-observer
           (list (list :restart fork 2 :prefix-end)) :fork fork)
        (unit-continuation-boundary-records observe)
        (rove:ok (handler-case (progn (funcall finish) nil) (error () t))
                 "The child EOR never certifies a prefix-end restart"))))

(cl-spec:defproperty startup-publication-image-detects-derived-state-mutation
    ((xid (range integer 3 10000)) (start (range integer 32 128)) (span (range integer 1 128)))
  (:about logical-slot-publication-check-registry)
  (:trials (:normal 200))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
    (declare (ignore certificate host))
    (let* ((slot (logical-slot-state-decode (second (find "alpha" rows :key #'first :test #'equal)) "alpha"))
           (pending (wal-replication-slot-pending-xids slot))
           (registry (make-hash-table :test #'equal)))
      (setf (gethash xid pending) (cons start (+ start span))
            (gethash "alpha" registry) slot)
      (let ((image (logical-slot-publication-registry-image registry)))
        (flet ((rejected ()
                 (handler-case
                     (progn (logical-slot-publication-check-registry registry image) nil)
                   (error () t))))
          (and (logical-slot-publication-check-registry registry image)
               (progn (incf (cdr (gethash xid pending))) (rejected))
               (progn (decf (cdr (gethash xid pending)))
                      (logical-slot-publication-check-registry registry image))
               (progn (setf (gethash "alpha" registry) (copy-wal-replication-slot slot)) (rejected))
               (progn (setf (gethash "alpha" registry) slot
                            (wal-replication-slot-state-uncertain-p slot) t)
                      (rejected))))))))

(rove:deftest startup-publication-image-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'startup-publication-image-detects-derived-state-mutation
                                         :seed 11502 :profile :normal))))
      (format t "~&STARTUP-PUBLICATION-IMAGE status=~s trials=~s seed=11502 digest=~s~%"
              (getf result :status) (getf result :trials) (getf result :definition-digest))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

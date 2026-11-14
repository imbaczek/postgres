(in-package #:pg)

(cl-spec:defproperty observed-promotion-prefix-keeps-owned-inode-and-bytes
    ((offset (range integer 0 256)) (salt (range integer 0 255)))
  (:about wal-install-timeline-prefix)
  (:trials (:normal 100))
  (call-with-unit-promotion-directory
   (lambda (context root)
     (declare (ignore root))
     (let ((prefix (make-array offset :element-type '(unsigned-byte 8)))
           (witness nil) (installed nil) (events nil))
       (dotimes (i offset) (setf (aref prefix i) (mod (+ i salt) 256)))
       (unwind-protect
            (progn
              (wal-install-timeline-prefix
               1 2 offset prefix :timeline
               (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal))
               (lambda (event operation name fd &optional destination)
                 (declare (ignore name))
                 (assert (eq operation :prefix))
                 (push event events)
                 (case event
                   (:created
                    (assert (logtest 1 (sb-posix:fcntl fd sb-posix:f-getfd)))
                    (setf witness (sb-posix:fcntl fd 1030 0)))
                   (:installed
                    (let ((held (physical-image-file-facts witness))
                          (named (physical-image-file-facts
                                  (cdr (assoc "pg_wal" (physical-image-context-children context)
                                              :test #'equal))
                                  (file-namestring destination))))
                      (setf installed (and (= (first held) (first named))
                                           (= (second held) (second named)))))))))
              (and installed
                   (every (lambda (event) (member event events))
                          '(:created :written :before-install :installed))
                   (equalp prefix (read-bounded-regular-file (wal-segment-path 0 2 :timeline)
                                                             +wal-segment-size+))))
         (when witness (sb-posix:close witness)))))))

(rove:deftest staging-observer-prefix-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'observed-promotion-prefix-keeps-owned-inode-and-bytes
                                       :seed 315933))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest staging-observer-history-retains-install-inode-through-alias-removal
    (call-with-unit-promotion-directory
     (lambda (context root)
       (declare (ignore root))
       (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
              (path (timeline-history-path 2 *pgdata*))
              (bytes (timeline-encode-history 2 1 64 "observer" nil))
              (witness nil) (events nil) (links nil))
         (unwind-protect
              (progn
                (timeline-publish-history-bytes
                 path bytes :directory-fd directory
                 :observer
                 (lambda (event operation name fd &optional destination)
                   (declare (ignore name destination))
                   (rove:ok (eq operation :history))
                   (push event events)
                   (case event
                     (:created
                      (rove:ok (logtest 1 (sb-posix:fcntl fd sb-posix:f-getfd)))
                      (setf witness (sb-posix:fcntl fd 1030 0)))
                     (:installed
                      (push (fourth (physical-image-file-facts witness)) links)
                      (rove:ok (= (second (physical-image-file-facts witness))
                                  (second (physical-image-file-facts directory (file-namestring path))))))
                     (:unlinked (push (fourth (physical-image-file-facts witness)) links)))))
                (rove:ok (equal '(1 2) links))
                (rove:ok (equalp bytes (read-bounded-regular-file path 4096)))
                (rove:ok (every (lambda (event) (member event events))
                                '(:created :written :before-install :installed :before-unlink :unlinked)))
                (rove:ok (equal (list (timeline-history-filename 2))
                                (directory-entry-names (physical-image-fd-path directory) :follow-symlinks t))))
           (when witness (sb-posix:close witness)))))))

(rove:deftest staging-observer-errors-release-writer-descriptors
    (dolist (kind '(:prefix :history :frame))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (declare (ignore root))
         (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                (prefix (make-array 8 :element-type '(unsigned-byte 8) :initial-element 17))
                (history (timeline-encode-history 2 1 8 "observer" nil))
                (target (if (eq kind :history) (timeline-history-path 2 *pgdata*)
                            (wal-segment-path 0 2 :timeline)))
                (owned nil) (fired nil)
                (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
           (flet ((observe (event operation name fd &optional destination)
                    (declare (ignore operation name destination))
                    (when (eq event :created)
                      (setf owned fd fired t)
                      (error "observer creation cut"))))
             (rove:ok
              (handler-case
                  (progn
                    (ecase kind
                      (:prefix (wal-install-timeline-prefix 1 2 8 prefix :timeline directory #'observe))
                      (:history (timeline-publish-history-bytes target history
                                                                :directory-fd directory :observer #'observe))
                      (:frame (physical-promotion-stage-frame
                               2 (list 0 (wal-encode-record +wal-record-batch-start+ 0 0 #())) #'observe)))
                    nil)
                (error () t)))
             (rove:ok fired)
             (rove:ok (and owned
                           (handler-case (progn (sb-posix:fstat owned) nil) (error () t))))
             (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (rove:ok (if (eq kind :frame)
                          (and (probe-file target) (zerop (length (read-bounded-regular-file target 64))))
                          (not (probe-file target))))))))))

(rove:deftest staging-observer-sees-frame-rewrites-and-orphan-removal
    (call-with-unit-promotion-directory
     (lambda (context root)
       (declare (ignore root))
       (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
              (frame (wal-encode-record +wal-record-batch-start+ 0 0 #()))
              (events nil))
         (flet ((observe (event operation name fd &optional destination)
                  (declare (ignore name destination))
                  (push (list event operation) events)
                  (when fd (rove:ok (sb-posix:s-isreg (third (physical-image-file-facts fd)))))))
           (physical-promotion-stage-frame 2 (list 0 frame) #'observe)
           (physical-promotion-stage-frame 2 (list 0 frame) #'observe)
           (rove:ok (= 1 (count '(:created :frame) events :test #'equal)))
           (rove:ok (= 1 (count '(:opened-existing :frame) events :test #'equal)))
           (rove:ok (= 2 (count '(:written :frame) events :test #'equal)))
           (rove:ok (equalp frame (read-bounded-regular-file (wal-segment-path 0 2 :timeline) 1024)))
           (physical-promotion-remove-child-orphans '(:promotion-child 2) nil #'observe)
           (rove:ok (member '(:before-unlink :orphan) events :test #'equal))
           (rove:ok (member '(:unlinked :orphan) events :test #'equal))
           (rove:ok (null (directory-entry-names (physical-image-fd-path directory) :follow-symlinks t))))))))

(rove:deftest staging-observer-covers-complete-checkpoint-staging-and-retry
    (call-with-unit-promotion-directory
     (lambda (context root)
       (declare (ignore root))
       (let* ((state (list :phase 3 :applied 64 :timeline 1 :promotion-child 2
                           :next-xid 50 :state-time 1793030000
                           :wal-namespace :timeline :promotion-source-namespace :timeline
                           :source-reservation (list :version 1 :through 64 :bound 1002 :complete t)))
              (plan (physical-promotion-checkpoint-plan state 20000 1003))
              (prefix (make-array 64 :element-type '(unsigned-byte 8) :initial-element 17))
              (files (physical-promotion-checkpoint-files plan prefix))
              (parent (wal-segment-path 0 1 :timeline))
              (orphan (wal-segment-path 1 2 :timeline))
              (events nil) (lifecycle nil))
         (with-open-file (stream parent :direction :output :element-type '(unsigned-byte 8))
           (write-sequence #(1 2 3) stream))
         (with-open-file (stream orphan :direction :output :element-type '(unsigned-byte 8))
           (declare (ignore stream)))
         (flet ((observe (event operation name fd &optional destination)
                  (declare (ignore name destination))
                  (when fd (rove:ok (sb-posix:s-isreg (third (physical-image-file-facts fd)))))
                  (push (list event operation) events))
                (check (stage)
                  (push stage lifecycle)
                  (when stage
                    (dolist (entry files)
                      (rove:ok (equalp (second entry)
                                       (read-bounded-regular-file
                                        (wal-segment-path (first entry) 2 :timeline)
                                        +wal-segment-size+)))))))
           (physical-promotion-stage-child-checkpoint
            state plan prefix files nil #'check (lambda () (push :data lifecycle))
            :context context :install-prefix t :observer #'observe)
           (dolist (event '((:created :prefix) (:installed :prefix) (:created :history)
                            (:installed :history) (:unlinked :history) (:opened-existing :frame)
                            (:written :frame) (:before-unlink :orphan) (:unlinked :orphan)))
             (rove:ok (member event events :test #'equal)))
           (rove:ok (equal '(:staged :data :staged) (subseq lifecycle 0 3)))
           (rove:ok (equalp #(1 2 3) (read-bounded-regular-file parent 64)))
           (let ((before (mapcar (lambda (entry)
                                   (physical-image-file-facts
                                    (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal))
                                    (wal-segment-filename (first entry) 2 :timeline)))
                                 files)))
             (setf events nil)
             (physical-promotion-stage-child-checkpoint
              state plan prefix files t #'check (lambda () (push :data lifecycle))
              :context context :install-prefix t :observer #'observe)
             (rove:ok (not (find :prefix events :key #'second)))
             (rove:ok (= (length (getf plan :frames))
                         (count '(:opened-existing :frame) events :test #'equal)))
             (rove:ok (equal before
                             (mapcar (lambda (entry)
                                       (physical-image-file-facts
                                        (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal))
                                        (wal-segment-filename (first entry) 2 :timeline)))
                                     files)))))))))

(rove:deftest staging-observer-postmutation-exits-preserve-retry-state
    (dolist (case '((:prefix :installed :error) (:prefix :installed :throw)
                    (:prefix :installed :interrupt) (:prefix :unlinked :error)
                    (:history :installed :error) (:history :installed :interrupt)
                    (:history :unlinked :throw)))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (declare (ignore root))
         (let* ((kind (first case)) (point (second case)) (exit (third case))
                                    (directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                                    (path (if (eq kind :prefix) (wal-segment-path 0 2 :timeline)
                                              (timeline-history-path 2 *pgdata*)))
                                    (bytes (if (eq kind :prefix)
                                               (make-array 8 :element-type '(unsigned-byte 8) :initial-element 17)
                                               (timeline-encode-history 2 1 8 "observer" nil)))
                                    (marker (make-condition 'simple-error :format-control "postmutation observer cut"))
                                    (witness nil) (fired nil)
                                    (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
           (when (and (eq kind :prefix) (eq point :unlinked))
             (with-open-file (output path :direction :output :element-type '(unsigned-byte 8))
               (write-sequence #(1 2 3) output)))
           (unwind-protect
                (flet ((observe (event operation name fd &optional destination)
                         (declare (ignore operation name destination))
                         (when (eq event :created) (setf witness (sb-posix:fcntl fd 1030 0)))
                         (when (eq point event)
                           (setf fired t)
                           (ecase exit
                             (:error (error marker))
                             (:throw (throw 'observer-cut :cut))
                             (:interrupt (sb-thread:interrupt-thread
                                          sb-thread:*current-thread*
                                          (lambda () (throw 'observer-cut :cut))))))))
                  (let ((outcome
                          (handler-case
                              (catch 'observer-cut
                                (if (eq kind :prefix)
                                    (wal-install-timeline-prefix 1 2 8 bytes :timeline directory #'observe)
                                    (timeline-publish-history-bytes path bytes
                                                                    :directory-fd directory :observer #'observe))
                                :returned)
                            (error (condition) condition))))
                    (rove:ok fired)
                    (rove:ok (eq outcome (if (eq exit :error) marker :cut)))
                    (rove:ok witness)
                    (rove:ok (= (1+ before)
                                (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
                    (if (and (eq kind :prefix) (eq point :unlinked))
                        (progn
                          (rove:ok (not (probe-file path)))
                          (rove:ok (zerop (fourth (physical-image-file-facts witness)))))
                        (progn
                          (rove:ok (equalp bytes (read-bounded-regular-file path 4096)))
                          (rove:ok (= 1 (fourth (physical-image-file-facts witness))))
                          (rove:ok (= (second (physical-image-file-facts witness))
                                      (second (physical-image-file-facts directory (file-namestring path)))))))
                    (rove:ok (notany (lambda (name)
                                       (or (uiop:string-prefix-p ".timeline-" name)
                                           (uiop:string-prefix-p ".history-" name)))
                                     (directory-entry-names (physical-image-fd-path directory)
                                                            :follow-symlinks t)))))
             (when witness (sb-posix:close witness)))
           (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))))))))

(rove:deftest staging-observer-sync-interrupts-follow-written-event
    (dolist (kind '(:prefix :history :frame :history-retry))
      (call-with-unit-promotion-directory
       (lambda (context root)
         (declare (ignore root))
         (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
                (state '(:timeline 1 :promotion-child 2 :applied 8))
                (history (physical-promotion-history-image state))
                (path (timeline-history-path 2 *pgdata*))
                (prefix (make-array 8 :element-type '(unsigned-byte 8) :initial-element 17))
                (original (symbol-function 'fsync-stream))
                (target nil) (fired nil) (written nil))
           (when (eq kind :history-retry)
             (timeline-publish-history-bytes path history :directory-fd directory))
           (let ((before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
             (unwind-protect
                  (flet ((observe (event operation name fd &optional destination)
                           (declare (ignore operation name destination))
                           (when (member event '(:created :opened-existing)) (setf target fd))
                           (when (eq event :written) (setf written t))))
                    (setf (symbol-function 'fsync-stream)
                          (lambda (stream)
                            (prog1 (funcall original stream)
                              (when (and (eql target (sb-sys:fd-stream-fd stream)) (not fired))
                                (setf fired t)
                                (sb-thread:interrupt-thread sb-thread:*current-thread*
                                                            (lambda () (throw 'sync-cut :cut)))))))
                    (rove:ok
                     (eq :cut
                         (catch 'sync-cut
                           (ecase kind
                             (:prefix (wal-install-timeline-prefix 1 2 8 prefix :timeline directory #'observe))
                             (:history (timeline-publish-history-bytes path history
                                                                       :directory-fd directory :observer #'observe))
                             (:frame (physical-promotion-stage-frame
                                      2 (list 0 (wal-encode-record +wal-record-batch-start+ 0 0 #())) #'observe))
                             (:history-retry (physical-promotion-sync-history state directory #'observe)))
                           :returned)))
                    (rove:ok fired)
                    (rove:ok written)
                    (rove:ok (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))))
               (setf (symbol-function 'fsync-stream) original))))))))

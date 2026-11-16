(in-package #:pg)

(defvar *native-writer-test-failure* nil)

(defun native-host-writer-open-fds ()
  (loop for path in (directory #p"/proc/self/fd/*" :resolve-symlinks nil)
        for name = (file-namestring (pathname (string-right-trim "/" (namestring path))))
        for fd = (parse-integer name :junk-allowed nil)
        when (ignore-errors (sb-posix:fstat fd))
          collect (pathname (format nil "/proc/self/fd/~d" fd))))

(defun native-host-writer-selected ()
  (let ((bytes (read-bounded-regular-file
                (merge-pathnames "pg_logical_continuation/selector" *pgdata*)
                (logical-slot-continuation-prepared-maximum-bytes))))
    (values (logical-slot-continuation-decode bytes) bytes)))

(defun native-host-writer-second-generation (slot beta-image beta-facts)
  (let ((previous *wal-durable-lsn*) (end nil)
                                     (group (gethash *wal-replication-slots* *logical-slot-continuation-protection-registries*)))
    (with-wal-critical
      (when (equal "happy-abandoned" (uiop:getenv "PG_NATIVE_WRITER_CASE"))
        (wal-append-batch-start))
      (wal-append-batch-start)
      (setf end (wal-append-record +wal-record-maintenance+ 0
                                   (make-array 0 :element-type '(unsigned-byte 8))))
      (wal-flush-core))
    (sb-thread:with-mutex (*wal-lock*)
      (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*))
    (assert (> end previous))
    (call-with-logical-replication-slot
     "alpha"
     (lambda (owned)
       (assert (eq owned slot))
       (assert (eq slot (logical-slot-continuation-native-acknowledge
                         "alpha" end *wal-logical-slot-owner*)))
       (assert (= 3 (getf (native-host-writer-selected) :generation)))
       (let ((selected (physical-image-file-facts
                        -100 (namestring (merge-pathnames "pg_logical_continuation/selector" *pgdata*)))))
         (assert (eq slot (logical-slot-continuation-native-acknowledge
                           "alpha" end *wal-logical-slot-owner*)))
         (assert (equal selected
                        (physical-image-file-facts
                         -100 (namestring (merge-pathnames "pg_logical_continuation/selector" *pgdata*))))))
       (assert (= end (wal-replication-slot-confirmed-flush-lsn slot)))))
    (assert (= end (wal-replication-slot-restart-lsn slot)))
    (assert (= 2 (wal-replication-slot-confirmed-timeline slot)))
    (assert (= 2 (wal-replication-slot-restart-timeline slot)))
    (assert (= 3 (getf (native-host-writer-selected) :generation)))
    (assert (eq group (gethash *wal-replication-slots*
                               *logical-slot-continuation-protection-registries*)))
    (assert (= 41268 (gethash group *wal-snapshot-retention-pins*)))
    (assert (equalp beta-image (logical-slot-state-encode (gethash "beta" *wal-replication-slots*))))
    (assert (equal beta-facts
                   (physical-image-file-facts -100 (namestring (wal-replication-slot-path "beta")))))
    (call-with-logical-replication-slot
     "alpha"
     (lambda (owned)
       (declare (ignore owned))
       (multiple-value-bind (transactions progress exhausted)
           (wal-logical-slot-read-transactions "alpha" :from-lsn end :upto-lsn end)
         (assert (null transactions))
         (assert (= end progress))
         (assert exhausted))))
    (native-host-with-runtime-scan
     "beta" (lambda (scan source) (declare (ignore source))
              (native-host-continuation-runtime-transactions scan)))
    (format t "~&NATIVE-WRITER-SECOND PASS generation=3 child-end=~d noop-preserves-inode beta-readable~%" end)))

(defun native-host-writer-forgery-probes (writer ready)
  (let ((calls 0) (original (symbol-function 'physical-image-file-facts))
                  (phase (logical-slot-continuation-writer-call writer :phase))
                  (group (gethash *wal-replication-slots* *logical-slot-continuation-protection-registries*)))
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-image-file-facts)
                 (lambda (&rest arguments) (incf calls) (apply original arguments)))
           (dolist (action
                    (if ready
                        (let ((receipt (logical-slot-continuation-writer-call writer :receipt)))
                          (list
                           (lambda () (logical-slot-continuation-writer-publish-under-lock
                                       writer (copy-logical-slot-continuation-writer-receipt receipt)))
                           (lambda () (logical-slot-continuation-writer-publish-under-lock
                                       (copy-logical-slot-continuation-writer writer) receipt))))
                        (list
                         (lambda () (logical-slot-continuation-writer-call
                                     (copy-logical-slot-continuation-writer writer) :run))
                         (lambda () (logical-slot-continuation-writer-guard
                                     (%make-logical-slot-continuation-writer-context :writer writer)))
                         (lambda () (logical-slot-continuation-writer-bind-cell-under-lock
                                     writer group (cons (make-array 0) writer))))))
             (assert (handler-case (progn (funcall action) nil) (error () t))))
           (assert (zerop calls))
           (assert (eq phase (logical-slot-continuation-writer-call writer :phase))))
      (setf (symbol-function 'physical-image-file-facts) original))))

(defun native-host-writer-happy ()
  (let* ((slot (gethash "alpha" *wal-replication-slots*))
         (beta (gethash "beta" *wal-replication-slots*))
         (beta-image (logical-slot-state-encode beta))
         (beta-path (wal-replication-slot-path "beta"))
         (beta-facts (physical-image-file-facts -100 (namestring beta-path)))
         (fd-names (native-host-writer-open-fds))
         (fds (length fd-names))
         (points nil)
         (*logical-slot-continuation-writer-test-hook*
           (lambda (point writer)
             (when (member point '(:registered :before-live-publication))
               (native-host-writer-forgery-probes writer (eq point :before-live-publication)))
             (push point points)
             (format t "~&NATIVE-WRITER-POINT ~s~%" point)
             (finish-output))))
    (assert (= 20430 (wal-replication-slot-confirmed-flush-lsn slot)))
    (assert (= 2 (hash-table-count (wal-replication-slot-pending-xids slot))))
    (assert (= 1 (getf (native-host-writer-selected) :generation)))
    (call-with-logical-replication-slot
     "alpha"
     (lambda (owned)
       (assert (eq slot owned))
       (assert (eq slot (logical-slot-continuation-native-acknowledge
                         "alpha" 37853 *wal-logical-slot-owner*)))))
    (assert (= 37853 (wal-replication-slot-confirmed-flush-lsn slot)))
    (assert (= 37853 (wal-replication-slot-restart-lsn slot)))
    (assert (zerop (hash-table-count (wal-replication-slot-pending-xids slot))))
    (assert (= 2 (getf (native-host-writer-selected) :generation)))
    (assert (equalp (logical-slot-state-encode slot)
                    (read-bounded-regular-file (wal-replication-slot-path "alpha")
                                               +slot-state-maximum-bytes+)))
    (assert (equalp beta-image (logical-slot-state-encode beta)))
    (assert (equal beta-facts (physical-image-file-facts -100 (namestring beta-path))))
    (assert (= fds (length (native-host-writer-open-fds))))
    (let ((group (gethash *wal-replication-slots* *logical-slot-continuation-protection-registries*)))
      (assert group)
      (sb-thread:with-mutex (*wal-lock*)
        (assert (logical-slot-continuation-protection-call group :check))
        (assert (not (logical-slot-continuation-protection-call group :uncertain)))
        (assert (equalp (nth-value 1 (native-host-writer-selected))
                        (logical-slot-continuation-protection-call group :image)))
        (assert (= 41268 (gethash group *wal-snapshot-retention-pins*)))))
    (assert (equal (reverse points)
                   '(:lifetime-acquired :registered :begun :before-intent
                     :prepared-temporary :prepared-selected :prepared-durable
                     :p7-temporary :p7-selected :before-slot-root-sync :p7-durable
                     :stable-temporary :stable-selected :stable-durable
                     :before-live-publication :published :after-proof-return)))
    (native-host-writer-second-generation slot beta-image beta-facts)
    (let* ((after (native-host-writer-open-fds))
           (opened (set-difference after fd-names :test #'equal))
           (output (pathname (format nil "/proc/self/fd/~d" (sb-sys:fd-stream-fd *wal-output-stream*)))))
      (assert (null (set-difference fd-names after :test #'equal)))
      (assert (equal opened (list output)))
      (format t "~&NATIVE-WRITER-FDS PASS sole-new-descriptor=~s target=~s~%"
              output (sb-posix:readlink (namestring output))))
    (format t "~&NATIVE-WRITER PASS case=~a generations=2,3 child-wal noop beta-preserved no-fd-leak~%"
            (uiop:getenv "PG_NATIVE_WRITER_CASE"))))

(defun native-host-writer-with-io-fault (case inject function)
  (let ((close (symbol-function 'sb-posix:close))
        (replacement-close (symbol-function 'logical-slot-continuation-replacement-close))
        (hook *logical-slot-continuation-native-proof-test-hook*)
        (closes 0)
        (nth-close (cond ((equal case "prepared-close") 1)
                         ((equal case "p7-close") 2)
                         ((equal case "stable-close") 3))))
    (labels ((arm-close (target)
               (setf (symbol-function 'sb-posix:close)
                     (lambda (fd)
                       (prog1 (funcall close fd)
                         (when (or (null target) (= fd target))
                           (setf (symbol-function 'sb-posix:close) close)
                           (funcall inject)))))))
      (unwind-protect
           (let ((*logical-slot-continuation-native-proof-test-hook*
                   (lambda (point value)
                     (when (and (equal case "proof-close") (eq point :after-callback))
                       (arm-close nil))
                     (when hook (funcall hook point value)))))
             (when nth-close
               (setf (symbol-function 'logical-slot-continuation-replacement-close)
                     (lambda (resource complete)
                       (when (and complete (= (incf closes) nth-close))
                         (arm-close (logical-slot-continuation-replacement-new-fd resource)))
                       (funcall replacement-close resource complete))))
             (funcall function))
        (setf (symbol-function 'sb-posix:close) close
              (symbol-function 'logical-slot-continuation-replacement-close) replacement-close)))))

(defun native-host-writer-cut (case &optional fault-case)
  (let* ((cut (intern (string-upcase case) :keyword))
         (before-intent (member cut '(:registered :begun :before-intent)))
         (published (member cut '(:published :after-callback :after-proof-return)))
         (stable (member cut '(:stable-selected :stable-durable :before-live-publication
                               :published :after-callback :after-proof-return)))
         (p7-new (member cut '(:p7-selected :before-slot-root-sync :p7-durable
                               :stable-temporary :stable-selected :stable-durable
                               :before-live-publication :published :after-callback :after-proof-return)))
         (slot (gethash "alpha" *wal-replication-slots*))
         (beta-path (wal-replication-slot-path "beta"))
         (beta-bytes (read-bounded-regular-file beta-path +slot-state-maximum-bytes+))
         (beta-facts (physical-image-file-facts -100 (namestring beta-path)))
         (old (logical-slot-state-encode slot))
         (fds (length (directory #p"/proc/self/fd/*")))
         (reached 0) (caught nil))
    (flet ((stop (point value)
             (declare (ignore value))
             (when (and (not fault-case) (eq point cut))
               (incf reached)
               (error "native writer injected cut ~a" case))))
      (let ((*logical-slot-continuation-writer-test-hook* #'stop)
            (*logical-slot-continuation-native-proof-test-hook* #'stop))
        (setf caught
              (handler-case
                  (progn
                    (native-host-writer-with-io-fault
                     fault-case
                     (lambda () (incf reached) (error "native writer injected cut ~a" fault-case))
                     (lambda ()
                       (call-with-logical-replication-slot
                        "alpha" (lambda (owned)
                                  (declare (ignore owned))
                                  (logical-slot-continuation-native-acknowledge
                                   "alpha" 37853 *wal-logical-slot-owner*)))))
                    nil)
                (error (condition) (princ-to-string condition))))))
    (assert (= reached 1))
    (assert (and caught (search "native writer injected cut" caught)))
    (assert (= fds (length (directory #p"/proc/self/fd/*"))))
    (assert (= (if published 37853 20430) (wal-replication-slot-confirmed-flush-lsn slot)))
    (assert (equalp beta-bytes (read-bounded-regular-file beta-path +slot-state-maximum-bytes+)))
    (assert (equal beta-facts (physical-image-file-facts -100 (namestring beta-path))))
    (let* ((bytes (read-bounded-regular-file (wal-replication-slot-path "alpha")
                                             +slot-state-maximum-bytes+))
           (saved (logical-slot-state-decode bytes "alpha"))
           (selected (read-bounded-regular-file
                      (merge-pathnames "pg_logical_continuation/selector" *pgdata*)
                      (logical-slot-continuation-prepared-maximum-bytes)))
           (group (gethash *wal-replication-slots* *logical-slot-continuation-protection-registries*)))
      (assert (= (if p7-new 37853 20430) (wal-replication-slot-confirmed-flush-lsn saved)))
      (cond
        (stable (assert (= 2 (getf (logical-slot-continuation-decode selected) :generation))))
        ((or before-intent (eq cut :prepared-temporary))
         (assert (= 1 (getf (logical-slot-continuation-decode selected) :generation))))
        (t
         (let ((record (logical-slot-continuation-prepared-decode selected)))
           (assert (= 1 (getf (getf record :old) :generation)))
           (assert (= 2 (getf (getf record :new) :generation))))))
      (assert group)
      (sb-thread:with-mutex (*wal-lock*)
        (assert (eq (not (null before-intent))
                    (not (logical-slot-continuation-protection-call group :uncertain))))
        (assert (= 41268 (gethash group *wal-snapshot-retention-pins*)))
        (dolist (name '("alpha" "beta"))
          (assert (eq (not (null before-intent))
                      (not (wal-replication-slot-state-uncertain-p
                            (gethash name *wal-replication-slots*))))))
        (when published
          (assert (equalp selected (logical-slot-continuation-protection-call group :image)))))
      (if before-intent
          (progn
            (assert (equalp old bytes))
            (call-with-logical-replication-slot
             "alpha" (lambda (owned)
                       (declare (ignore owned))
                       (logical-slot-continuation-native-acknowledge
                        "alpha" 37853 *wal-logical-slot-owner*)))
            (assert (= 2 (getf (native-host-writer-selected) :generation))))
          (dolist (name '("alpha" "beta"))
            (dolist (action (list (lambda () (wal-logical-slot-read-transactions name))
                                  (lambda () (wal-drop-replication-slot name))))
              (assert (handler-case (progn (funcall action) nil)
                        (error (condition) (search "uncertain" (princ-to-string condition)))))))))
    (format t "~&NATIVE-WRITER PASS case=~a attempted=~s published=~s fds-closed beta-preserved~%"
            (or fault-case case) (null before-intent) (not (null published)))))

(defun native-host-writer-parent-before-d ()
  (let* ((slot (gethash "alpha" *wal-replication-slots*))
         (beta (wal-replication-slot-path "beta"))
         (beta-facts (physical-image-file-facts -100 (namestring beta)))
         (beta-image (read-bounded-regular-file beta +slot-state-maximum-bytes+))
         (fds (native-host-writer-open-fds)))
    (call-with-logical-replication-slot
     "alpha" (lambda (owned)
               (assert (eq owned slot))
               (assert (eq slot (logical-slot-continuation-native-acknowledge
                                 "alpha" 29061 *wal-logical-slot-owner*)))))
    (assert (= 2 (getf (native-host-writer-selected) :generation)))
    (assert (= 29061 (wal-replication-slot-confirmed-flush-lsn slot)
               (wal-replication-slot-restart-lsn slot)))
    (assert (= 1 (wal-replication-slot-confirmed-timeline slot)
               (wal-replication-slot-restart-timeline slot)))
    (assert (= 1 (hash-table-count (wal-replication-slot-pending-xids slot))))
    (assert (equalp (logical-slot-state-encode slot)
                    (read-bounded-regular-file (wal-replication-slot-path "alpha") +slot-state-maximum-bytes+)))
    (assert (equal beta-facts (physical-image-file-facts -100 (namestring beta))))
    (assert (equalp beta-image (read-bounded-regular-file beta +slot-state-maximum-bytes+)))
    (assert (equal fds (native-host-writer-open-fds)))
    (format t "~&NATIVE-WRITER PASS case=parent-before-D generation=2 confirmed=restart=29061 timeline=1 beta-preserved before-D-reader-still-unqualified~%")))

(defun native-host-continuation-native-writer ()
  (handler-case
      (let ((case (uiop:getenv "PG_NATIVE_WRITER_CASE")))
        (cond
          ((member case '("happy" "happy-abandoned") :test #'equal) (native-host-writer-happy))
          ((equal case "parent-before-D") (native-host-writer-parent-before-d))
          ((assoc case '(("prepared-close" . "prepared-durable")
                         ("p7-close" . "p7-durable") ("stable-close" . "stable-durable")
                         ("proof-close" . "after-callback")) :test #'equal)
           (native-host-writer-cut
            (cdr (assoc case '(("prepared-close" . "prepared-durable")
                               ("p7-close" . "p7-durable") ("stable-close" . "stable-durable")
                               ("proof-close" . "after-callback")) :test #'equal))
            case))
          ((member case '("registered" "begun" "before-intent" "prepared-temporary"
                          "prepared-selected" "prepared-durable" "p7-temporary" "p7-selected"
                          "before-slot-root-sync" "p7-durable" "stable-temporary"
                          "stable-selected" "stable-durable" "before-live-publication"
                          "published" "after-callback" "after-proof-return") :test #'equal)
           (native-host-writer-cut case))
          (t (error "unknown native writer case ~s" case))))
    (error (condition)
      (setf *native-writer-test-failure* condition)
      (format t "~&NATIVE-WRITER FAILURE ~a~%" condition)
      (finish-output))))

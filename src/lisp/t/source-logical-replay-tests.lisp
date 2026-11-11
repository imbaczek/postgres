(in-package #:pg-physical-tests)

(defun call-with-source-logical-replay-fixture (mode function)
  (call-with-restartpoint-fixture
   (lambda (root store)
     (let ((pg::*physical-replay-restarting-p* t)
           (pg::*physical-resource-pause* nil))
       (setf pg::*catalog* (pg::load-catalog))
       (pg::clog-init)
       (unwind-protect
            (funcall function root store
                     (lambda ()
                       (ecase mode
                         (:full (pg::physical-replay-from pg::*last-replay-lsn*))
                         (:rows (pg::physical-row-replay-prefix 0)))))
         (close pg::*clog-stream*)
         (setf pg::*clog-stream* nil))))))

(defun append-source-logical-test-eor (store)
  (let* ((fork (pg::physical-store-flushed-lsn store))
         (eor (pg::wal-encode-end-of-recovery-record
               fork (pg::physical-store-system-id store) 2 1 1)))
    (pg::physical-store-select-history
     store 2 (format nil "1~c~a~%" #\Tab (pg::wal-format-lsn fork)) eor)
    (setf pg::*physical-recovery-state*
          (pg::physical-recovery-attach-selected-store pg::*physical-recovery-state* store))
    (pg::physical-store-input store fork eor)
    (pg::physical-store-flush store)
    (values fork (pg::physical-store-flushed-lsn store))))

(deftest source-logical-changes-publish-each-status-boundary-in-both-replay-paths
    (dolist (mode '(:full :rows))
      (call-with-source-logical-replay-fixture
       mode
       (lambda (root store replay)
         (let ((enable nil) (disable nil) (coverage nil)
                            (primary pg::*wal-logical-policy*)
                            (force pg::*logical-capture-force*)
                            (incarnation pg::*wal-storage-incarnation*))
           (dolist (enabled '(nil t t nil))
             (let* ((start (+ (pg::physical-store-flushed-lsn store) pg::+wal-header-size+))
                    (end (append-source-logical-test-status store enabled)))
               (unless coverage (setf coverage start))
               (if enabled (setf enable end) (setf disable end))
               (ok (= end (funcall replay)))
               (let ((fact (getf pg::*physical-recovery-state* :source-logical-fact))
                     (disk (getf (pg::physical-recovery-read-control root) :logical-fact)))
                 (ok (pg::physical-logical-fact-at-p fact end 1))
                 (ok (eql enabled (getf fact :enabled)))
                 (ok (= start (getf fact :witness-start)))
                 (ok (= coverage (getf fact :coverage-start)))
                 (ok (eql enable (getf fact :last-enable-lsn)))
                 (ok (eql disable (getf fact :last-disable-lsn)))
                 (ok (equal fact disk))
                 (ok (equal fact (getf pg::*physical-recovery-state* :required-logical-fact)))
                 (ok (not (eq fact (getf pg::*physical-recovery-state* :required-logical-fact)))))
               (ok (eq primary pg::*wal-logical-policy*))
               (ok (eq force pg::*logical-capture-force*))
               (ok (eq incarnation pg::*wal-storage-incarnation*))
               (ok (= 50 pg::*next-xid*)))))))))

(deftest source-logical-restartpoint-restores-known-off-and-replays-later-on
    (dolist (mode '(:full :rows))
      (call-with-source-logical-replay-fixture
       mode
       (lambda (root store replay)
         (let ((end (append-source-logical-test-status store nil))
               (primary pg::*wal-logical-policy*)
               (incarnation pg::*wal-storage-incarnation*))
           (ok (= end (funcall replay)))
           (let ((expected (copy-tree (getf pg::*physical-recovery-state* :source-logical-fact))))
             (ok expected)
             (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
             (dotimes (retry 2)
               (declare (ignorable retry))
               (setf (getf pg::*physical-recovery-state* :source-logical-fact) nil
                     pg::*last-replay-lsn* 32 pg::*wal-checkpoint-lsn* 32 pg::*next-xid* 50)
               (ok (= end (pg::physical-restartpoint-prepare-storage store)))
               (let ((restored (getf pg::*physical-recovery-state* :source-logical-fact)))
                 (ok (equal expected restored))
                 (ok (not (eq expected restored)))
                 (ok (null (getf restored :enabled)))
                 (ok (equal restored (getf (pg::physical-recovery-read-control root) :logical-fact)))))
             (let ((on (append-source-logical-test-status store t)))
               (ok (= on (funcall replay)))
               (let ((fact (getf pg::*physical-recovery-state* :source-logical-fact)))
                 (ok (getf fact :enabled))
                 (ok (= on (getf fact :last-enable-lsn)))
                 (ok (= end (getf fact :last-disable-lsn)))))
             (ok (eq primary pg::*wal-logical-policy*))
             (ok (eq incarnation pg::*wal-storage-incarnation*))))))))

(deftest source-logical-selected-eor-and-child-checkpoint-preserve-source-history
    (dolist (enabled '(nil t))
      (call-with-source-logical-replay-fixture
       :full
       (lambda (root store replay)
         (let ((first (append-source-logical-test-status store enabled))
               (primary pg::*wal-logical-policy*))
           (ok (= first (funcall replay)))
           (let ((expected (copy-tree (getf pg::*physical-recovery-state* :source-logical-fact))))
             (multiple-value-bind (fork end) (append-source-logical-test-eor store)
               (ok (= first fork))
               (ok (= end (funcall replay)))
               (setf (getf expected :through) end (getf expected :through-timeline) 2)
               (ok (equal expected (getf pg::*physical-recovery-state* :source-logical-fact)))
               (ok (equal expected (getf (pg::physical-recovery-read-control root) :logical-fact)))
               (let ((start (+ end pg::+wal-header-size+)))
                 (pg::physical-store-input store end (sample-physical-record end))
                 (pg::physical-store-input store start (selected-replay-test-child-checkpoint start))
                 (pg::physical-store-flush store))
               (let ((checkpoint-end (pg::physical-store-flushed-lsn store)))
                 (ok (= checkpoint-end (funcall replay)))
                 (setf (getf expected :through) checkpoint-end)
                 (ok (equal expected (getf pg::*physical-recovery-state* :source-logical-fact)))
                 (ok (= 2 (pg::physical-recovery-replay-timeline)))
                 (ok (= 2000 pg::*next-xid*)))
               (ok (eq primary pg::*wal-logical-policy*)))))))))

(deftest source-logical-selected-eor-publishes-only-after-control-succeeds
    (call-with-source-logical-replay-fixture
     :full
     (lambda (root store replay)
       (let ((fork (append-source-logical-test-status store t)))
         (ok (= fork (funcall replay)))
         (append-source-logical-test-eor store)
         (let ((before (copy-tree pg::*physical-recovery-state*))
               (disk (copy-tree (pg::physical-recovery-read-control root))))
           (let ((pg::*physical-replay-test-hook*
                   (lambda (point record)
                     (declare (ignore record))
                     (when (eq point :before-eor-progress) (throw 'logical-eor-cut :cut)))))
             (ok (eq :cut (catch 'logical-eor-cut (funcall replay)))))
           (ok (equal before pg::*physical-recovery-state*))
           (ok (= fork pg::*last-replay-lsn*))
           (ok (= 1 (pg::physical-recovery-replay-timeline)))
           (ok (equal disk (pg::physical-recovery-read-control root)))
           (ok (= (pg::physical-store-flushed-lsn store) (funcall replay)))
           (ok (= 2 (pg::physical-recovery-replay-timeline)))
           (ok (pg::physical-logical-fact-at-p
                (getf pg::*physical-recovery-state* :source-logical-fact)
                pg::*last-replay-lsn* 2)))))))

(defun call-with-source-logical-base-fixture (enabled function)
  (call-with-promotion-fixture
   (lambda (root store)
     (let* ((snapshot (copy-tree (getf pg::*physical-recovery-state* :source-checkpoint)))
            (name (first (first (getf pg::*physical-recovery-state* :files)))))
       (unless (eq enabled :unknown)
         (setf (getf snapshot :logical-decoding-enabled) enabled))
       (let* ((seed (pg::physical-logical-fact-from-checkpoint snapshot))
              (end (append-source-logical-test-status store (not enabled)))
              (required (nth-value 2 (scan-source-logical-test-prefix store snapshot end seed)))
              (base-bytes (concatenate '(vector (unsigned-byte 8)) (sample-physical-record 0)
                                       (pg::wal-encode-record
                                        pg::+wal-record-checkpoint+ 0 32
                                        (pg::wal-encode-checkpoint-snapshot snapshot))))
              (wal-control (pg::wal-encode-control-image 152 152 snapshot :namespace :legacy)))
         (pg::wal-atomic-write-bytes (merge-pathnames name root) base-bytes)
         (pg::wal-atomic-write-bytes (pg::wal-control-path) wal-control)
         (multiple-value-bind (manifest old-digest) (pg::recovery-read-manifest root)
           (declare (ignore old-digest))
           (dolist (entry (getf manifest :files))
             (let ((bytes (cond ((equal name (first entry)) base-bytes)
                                ((equal "global/pg_wal_control" (first entry)) wal-control))))
               (when bytes
                 (setf (second entry) (length bytes)
                       (third entry) (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))))
           (write-recovery-manifest-text root (pg::physical-bootstrap-manifest-text manifest))
           (let ((digest (nth-value 1 (pg::recovery-read-manifest root))))
             (pg::wal-atomic-write-bytes
              (pg::physical-recovery-control-path)
              (pg::physical-recovery-control-image
               2 (getf manifest :system-id) 1 digest 152 end 50 :logical-fact required))))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (funcall function root store snapshot seed required))))
   :applied 152))

(deftest source-logical-verified-base-keeps-future-required-evidence-separate
    (dolist (enabled '(:unknown nil t))
      (call-with-source-logical-base-fixture
       enabled
       (lambda (root store snapshot expected required)
         (declare (ignore store))
         (let ((runtime pg::*physical-recovery-state*)
               (before (lineage-test-bytes (pg::physical-recovery-control-path root))))
           (multiple-value-bind (base control) (pg::physical-recovery-verify-base-state)
             (let ((source (getf base :source-logical-fact))
                   (immutable (getf base :base-source-logical-fact))
                   (minimum (getf base :required-logical-fact)))
               (ok (= 152 (getf snapshot :end) (getf base :end)))
               (ok (equal expected source))
               (ok (equal expected immutable))
               (if expected
                   (progn
                     (ok (not (eq source immutable)))
                     (ok (pg::physical-logical-fact-at-p source 152 1))
                     (ok (null (getf source :last-enable-lsn))))
                   (ok (null source)))
               (ok (equal required minimum))
               (ok (equal required (getf control :logical-fact)))
               (ok (not (eq minimum (getf control :logical-fact))))
               (ok (> (getf minimum :through) (getf base :end)))))
           (ok (eq runtime pg::*physical-recovery-state*))
           (ok (equalp before (lineage-test-bytes (pg::physical-recovery-control-path root)))))))))

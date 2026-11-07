(in-package #:pg-physical-tests)

(defun call-with-reseed-follow-observation-test (rows history function &optional on-check)
  (let* ((symbols '(pg::physical-adoption-attempt-check-client
                    pg::physical-client-identity-row pg::pg-client-native-history))
         (originals (mapcar #'symbol-function symbols))
         (remaining (copy-list rows))
         (events nil))
    (unwind-protect
         (progn
           (setf (symbol-function (first symbols))
                 (lambda (client source)
                   (assert (eq :test-client client))
                   (assert (eq :test-source source))
                   (push :route events)
                   (when on-check (funcall on-check))
                   t)
                 (symbol-function (second symbols))
                 (lambda (client)
                   (assert (eq :test-client client))
                   (push :identify events)
                   (or (pop remaining) (error "unexpected identity read")))
                 (symbol-function (third symbols))
                 (lambda (client timeline)
                   (assert (eq :test-client client))
                   (push (list :history timeline) events)
                   (pg::string->bytes history)))
           (funcall function (lambda () (reverse events))))
      (loop for symbol in symbols for original in originals
            do (setf (symbol-function symbol) original)))))

(deftest reseed-follow-observation-accepts-the-saved-writer-and-monotone-flush
    (dolist (timeline '(1 3))
      (let* ((history (if (= timeline 1) "" (format nil "1 0/80 first promotion~%2 0/90 second promotion~%")))
             (generation (list :coordinates (list :system-id 7 :source-timeline timeline :receive-lsn 152)
                               :source-history history))
             (rows (list (vector "7" (write-to-string timeline) "0/A0" nil)
                         (vector "7" (write-to-string timeline) "0/A8" nil))))
        (call-with-reseed-follow-observation-test
         rows history
         (lambda (events)
           (ok (= 168 (pg::physical-reseed-follow-observe-source
                       :test-client :test-source generation)))
           (ok (equal (if (= timeline 1)
                          '(:route :identify :identify :route)
                          '(:route :identify (:history 3) :identify :route))
                      (funcall events))))))))

(deftest reseed-follow-observation-rejects-source-or-history-changes
    (let ((generation (list :coordinates (list :system-id 7 :source-timeline 3 :receive-lsn 152)
                            :source-history (format nil "1 0/80 promotion~%2 0/90 promotion~%"))))
      (dolist (kind '(:system :timeline :behind :regression :history :minimum))
        (let ((rows (list (vector "7" "3" "0/A0" nil) (vector "7" "3" "0/A8" nil)))
              (history (getf generation :source-history)))
          (ecase kind
            (:system (setf (aref (first rows) 0) "8"))
            (:timeline (setf (aref (second rows) 1) "4"))
            (:behind (setf (aref (first rows) 2) "0/90"))
            (:regression (setf (aref (second rows) 2) "0/9F"))
            (:history (setf history (format nil "1 0/80 different reason~%2 0/90 promotion~%")))
            (:minimum nil))
          (call-with-reseed-follow-observation-test
           rows history
           (lambda (events)
             (ok (intake-fails-p
                  (lambda ()
                    (pg::physical-reseed-follow-observe-source
                     :test-client :test-source generation :minimum (if (eq kind :minimum) 169 152))))
                 (format nil "Reject changed source observation ~A" kind))
             (ok (member :identify (funcall events)))))))))

(defun call-with-reseed-follow-prepare-source (owner root record function &optional (kind :same-timeline))
  (let* ((generation (first (getf record :generations)))
         (request (getf (getf record :attempt) :request))
         (coordinates (getf generation :coordinates))
         (source (getf (getf record :attempt) :source))
         (proved (reseed-follow-proof-test-record record root kind))
         (anchor (pg::physical-reseed-follow-generation-anchor (first (getf proved :generations))))
         (token (gensym "FOLLOW-PREPARE-CLIENT"))
         (state (list :connections 0 :closes 0 :prepares 0 :requests nil
                      :lose-reply nil :on-connect nil :on-prepare nil :on-close nil :returned-anchor nil))
         (symbols '(pg::physical-libpq-connect-frozen pg::physical-client-close
                    pg::physical-adoption-attempt-check-client pg::physical-client-identity-row
                    pg::pg-client-native-history pg::pg-client-native-backup-lease-follow-prepare))
         (originals (mapcar #'symbol-function symbols)))
    (unwind-protect
         (progn
           (setf (symbol-function (first symbols))
                 (lambda (helper frozen &key on-spawn)
                   (assert (string= helper "/unused/helper"))
                   (assert (equal (getf source :owner) (pg::physical-libpq-frozen-profile-datum frozen)))
                   (incf (getf state :connections))
                   (when on-spawn (funcall on-spawn token))
                   (when (getf state :on-connect) (funcall (getf state :on-connect)))
                   token)
                 (symbol-function (second symbols))
                 (lambda (client)
                   (assert (eq token client))
                   (incf (getf state :closes))
                   (when (getf state :on-close) (funcall (getf state :on-close))))
                 (symbol-function (third symbols))
                 (lambda (client saved)
                   (assert (eq token client))
                   (assert (equal source saved))
                   t)
                 (symbol-function (fourth symbols))
                 (lambda (client)
                   (assert (eq token client))
                   (vector (write-to-string (getf coordinates :system-id))
                           (write-to-string (getf coordinates :source-timeline))
                           (pg::wal-format-lsn (+ 128 (getf coordinates :receive-lsn))) nil))
                 (symbol-function (fifth symbols))
                 (lambda (client timeline)
                   (assert (eq token client))
                   (assert (= timeline (getf coordinates :source-timeline)))
                   (pg::string->bytes (getf generation :source-history)))
                 (symbol-function (sixth symbols))
                 (lambda (client supplied supplied-generation name supplied-coordinates)
                   (assert (eq token client))
                   (assert (equal request supplied))
                   (assert (= (getf generation :generation) supplied-generation))
                   (assert (equal (getf generation :name) name))
                   (assert (equal coordinates supplied-coordinates))
                   (let ((durable (pg::physical-reseed-follow-decode
                                   (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
                     (assert (equal record durable)))
                   (assert (pg::physical-reseed-follow-owner-busy-p owner))
                   (incf (getf state :prepares))
                   (push (list (pg::physical-backup-lease-request-copy supplied) supplied-generation
                               (copy-seq name) (copy-tree supplied-coordinates))
                         (getf state :requests))
                   (when (getf state :on-prepare) (funcall (getf state :on-prepare)))
                   (when (getf state :lose-reply)
                     (setf (getf state :lose-reply) nil)
                     (error "lost prepare response"))
                   (setf (getf state :returned-anchor) (pg::physical-follow-anchor-copy anchor))))
           (funcall function state))
      (loop for symbol in symbols for original in originals
            do (setf (symbol-function symbol) original)))))

(deftest reseed-follow-prepare-publishes-a-proof-and-retries-as-historical-state
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-publish-intent
                           owner (pg::physical-reseed-follow-initial-record owner)))
                 (record (pg::physical-reseed-follow-publish-intent
                          owner (reseed-follow-intent-test-record initial root) :previous initial))
                 (control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
            (call-with-reseed-follow-prepare-source
             owner root record
             (lambda (state)
               (let ((proved (pg::physical-reseed-follow-prepare owner :helper "/unused/helper")))
                 (ok (eq :proved (getf (first (getf proved :generations)) :phase)))
                 (ok (equal proved (pg::physical-reseed-follow-read owner)))
                 (ok (equal proved (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))))
               (ok (= 1 (getf state :connections) (getf state :prepares) (getf state :closes)))
               (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
               (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))
            (ok (equalp control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root)))))
          (ok (not (probe-file (merge-pathnames "physical-follow.control" root)))))))))

(deftest reseed-follow-prepare-lost-reply-retries-the-identical-durable-request
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-publish-intent
                           owner (pg::physical-reseed-follow-initial-record owner)))
                 (record (pg::physical-reseed-follow-publish-intent
                          owner (reseed-follow-intent-test-record initial root) :previous initial)))
            (call-with-reseed-follow-prepare-source
             owner root record
             (lambda (state)
               (setf (getf state :lose-reply) t)
               (ok (intake-fails-p
                    (lambda () (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))))
               (ok (= 1 (getf state :prepares) (getf state :closes)))
               (ok (equal record (pg::physical-reseed-follow-read owner)))
               (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
               (ok (eq :proved (getf (first (getf (pg::physical-reseed-follow-prepare
                                                   owner :helper "/unused/helper")
                                                  :generations)) :phase)))
               (ok (= 2 (getf state :connections) (getf state :prepares) (getf state :closes)))
               (ok (equal (first (getf state :requests)) (second (getf state :requests))))))))))))

(deftest reseed-follow-prepare-excludes-public-reentry-through-client-close
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-publish-intent
                           owner (pg::physical-reseed-follow-initial-record owner)))
                 (record (pg::physical-reseed-follow-publish-intent
                          owner (reseed-follow-intent-test-record initial root) :previous initial))
                 (checks 0) (proofs 0))
            (call-with-reseed-follow-prepare-source
             owner root record
             (lambda (state)
               (flet ((probe ()
                        (incf checks)
                        (ok (pg::physical-reseed-follow-owner-busy-p owner))
                        (dolist (operation
                                 (list (lambda () (pg::physical-reseed-follow-read owner))
                                       (lambda () (pg::physical-reseed-follow-initial-record owner))
                                       (lambda () (pg::physical-reseed-follow-publish-intent owner record :previous record))
                                       (lambda () (pg::physical-reseed-follow-recover-intent owner))
                                       (lambda () (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))))
                          (ok (handler-case (progn (funcall operation) nil)
                                (error (condition)
                                  (not (null (search "already in progress" (princ-to-string condition))))))))
                        (pg::call-with-physical-reseed-follow-image
                         owner (lambda (fd check)
                                 (declare (ignore fd))
                                 (funcall check)
                                 (incf proofs)))))
                 (setf (getf state :on-connect) #'probe
                       (getf state :on-prepare) #'probe
                       (getf state :on-close) #'probe)
                 (ok (eq :proved (getf (first (getf (pg::physical-reseed-follow-prepare
                                                     owner :helper "/unused/helper")
                                                    :generations)) :phase))))
               (ok (= 3 checks proofs))
               (ok (= 1 (getf state :connections) (getf state :prepares) (getf state :closes)))))))))))

(deftest reseed-follow-prepare-detaches-the-reply-before-closing-the-client
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-publish-intent
                           owner (pg::physical-reseed-follow-initial-record owner)))
                 (record (pg::physical-reseed-follow-publish-intent
                          owner (reseed-follow-intent-test-record initial root) :previous initial)))
            (call-with-reseed-follow-prepare-source
             owner root record
             (lambda (state)
               (setf (getf state :on-close)
                     (lambda () (setf (getf (getf state :returned-anchor) :receive-digest) 0)))
               (let* ((proved (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))
                      (anchor (pg::physical-reseed-follow-generation-anchor
                               (first (getf proved :generations)))))
                 (ok (= #xCBF29CE484222325 (getf anchor :receive-digest)))
                 (ok (zerop (getf (getf state :returned-anchor) :receive-digest)))
                 (ok (equal proved (pg::physical-reseed-follow-read owner))))))))))))

(deftest reseed-follow-proved-temporary-recovers-offline-from-its-preparing-predecessor
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (let ((saved nil) (temporary nil))
         (pg::call-with-physical-reseed-follow-owner
          root
          (lambda (owner)
            (let* ((initial (pg::physical-reseed-follow-publish-intent
                             owner (pg::physical-reseed-follow-initial-record owner)))
                   (record (pg::physical-reseed-follow-publish-intent
                            owner (reseed-follow-intent-test-record initial root) :previous initial)))
              (setf saved record)
              (call-with-reseed-follow-prepare-source
               owner root record
               (lambda (state)
                 (let ((pg::*physical-reseed-follow-test-hook*
                         (lambda (point path)
                           (declare (ignore path))
                           (when (eq point :before-follow-rename) (error "interrupt proved rename")))))
                   (ok (intake-fails-p
                        (lambda () (pg::physical-reseed-follow-prepare owner :helper "/unused/helper")))))
                 (ok (= 1 (getf state :prepares) (getf state :closes)))
                 (ok (pg::physical-reseed-follow-owner-uncertain-p owner))))
              (setf temporary (lineage-test-bytes (merge-pathnames "physical-reseed.follow.tmp" root)))
              (ok (eq :proved (getf (first (getf (pg::physical-reseed-follow-decode temporary)
                                                 :generations)) :phase)))
              (ok (equal saved (pg::physical-reseed-follow-decode
                                (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))
         (let ((original (symbol-function 'pg::physical-libpq-connect-frozen)) (connections 0))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (incf connections)
                          (error "offline recovery must not connect")))
                  (pg::call-with-physical-reseed-follow-owner
                   root
                   (lambda (owner)
                     (let ((proved (pg::physical-reseed-follow-recover-intent owner)))
                       (ok (eq :proved (getf (first (getf proved :generations)) :phase)))
                       (ok (equalp temporary (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                       (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                       (ok (equal proved (pg::physical-reseed-follow-recover-intent owner)))
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-follow-publish-intent owner proved :previous saved)))))))
                  (ok (zerop connections)))
             (setf (symbol-function 'pg::physical-libpq-connect-frozen) original)))))))

(defun reseed-follow-prepare-test-abort (root record)
  (let ((attempt (pg::physical-reseed-attempt-copy (getf record :attempt)))
        (temporary (merge-pathnames "replacement-attempt" root))
        (path (pg::physical-reseed-attempt-path root)))
    (setf (getf attempt :phase) :aborting)
    (reseed-follow-owner-test-write temporary (pg::physical-reseed-attempt-image attempt))
    (sb-posix:rename (namestring temporary) (namestring path))
    (pg::fsync-directory root)))

(deftest reseed-follow-prepare-abort-during-rpc-prevents-proof-publication
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-publish-intent
                           owner (pg::physical-reseed-follow-initial-record owner)))
                 (record (pg::physical-reseed-follow-publish-intent
                          owner (reseed-follow-intent-test-record initial root) :previous initial)))
            (call-with-reseed-follow-prepare-source
             owner root record
             (lambda (state)
               (setf (getf state :on-prepare) (lambda () (reseed-follow-prepare-test-abort root record)))
               (ok (intake-fails-p
                    (lambda () (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))))
               (ok (= 1 (getf state :prepares) (getf state :closes)))
               (ok (equal record (pg::physical-reseed-follow-read owner)))
               (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
               (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
               (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
               (ok (intake-fails-p
                    (lambda () (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))))
               (ok (= 1 (getf state :connections)))
               (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-intent owner))))))))))))

(deftest reseed-follow-proved-history-survives-abort-without-authorizing-a-temporary
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-publish-intent
                           owner (pg::physical-reseed-follow-initial-record owner)))
                 (record (pg::physical-reseed-follow-publish-intent
                          owner (reseed-follow-intent-test-record initial root) :previous initial)))
            (call-with-reseed-follow-prepare-source
             owner root record
             (lambda (state)
               (let* ((proved (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))
                      (bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                 (reseed-follow-prepare-test-abort root record)
                 (ok (equal proved (pg::physical-reseed-follow-prepare owner :helper "/unused/helper")))
                 (ok (equal proved (pg::physical-reseed-follow-recover-intent owner)))
                 (ok (equalp bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                 (ok (= 1 (getf state :connections) (getf state :closes)))
                 (let ((temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
                   (reseed-follow-owner-test-write temporary bytes)
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-intent owner))))
                   (ok (equalp bytes (lineage-test-bytes temporary))))
                 (ok (= 1 (getf state :connections))))))))))))

(deftest reseed-follow-prepare-refuses-an-equal-but-replaced-intent-before-rpc
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-publish-intent
                           owner (pg::physical-reseed-follow-initial-record owner)))
                 (record (pg::physical-reseed-follow-publish-intent
                          owner (reseed-follow-intent-test-record initial root) :previous initial))
                 (path (merge-pathnames "physical-reseed.follow" root))
                 (bytes (lineage-test-bytes path))
                 (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
            (call-with-reseed-follow-prepare-source
             owner root record
             (lambda (state)
               (setf (getf state :on-connect)
                     (lambda ()
                       (let ((temporary (merge-pathnames "replacement-follow" root)))
                         (reseed-follow-owner-test-write temporary bytes)
                         (sb-posix:rename (namestring temporary) (namestring path))
                         (pg::fsync-directory root))))
               (ok (intake-fails-p
                    (lambda () (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))))
               (ok (= 1 (getf state :connections) (getf state :closes)))
               (ok (zerop (getf state :prepares)))
               (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
               (ok (equalp bytes (lineage-test-bytes path)))
               (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))))))

(deftest reseed-follow-prepare-keeps-attempt-proof-through-the-image-postcheck
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-publish-intent
                           owner (pg::physical-reseed-follow-initial-record owner)))
                 (record (pg::physical-reseed-follow-publish-intent
                          owner (reseed-follow-intent-test-record initial root) :previous initial))
                 (original (symbol-function 'pg::physical-reseed-follow-write-intent))
                 (returned nil) (fired 0)
                 (pg::*physical-reseed-install-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (and returned (eq point :after-install-image-validation) (zerop fired))
                       (incf fired)
                       (reseed-follow-prepare-test-abort root record)))))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-reseed-follow-write-intent)
                         (lambda (&rest arguments)
                           (multiple-value-prog1 (apply original arguments) (setf returned t))))
                   (call-with-reseed-follow-prepare-source
                    owner root record
                    (lambda (state)
                      (ok (intake-fails-p
                           (lambda () (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))))
                      (ok (= 1 fired (getf state :prepares) (getf state :closes)))
                      (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                      (ok (pg::physical-reseed-follow-owner-uncertain-p owner)))))
              (setf (symbol-function 'pg::physical-reseed-follow-write-intent) original))))))))

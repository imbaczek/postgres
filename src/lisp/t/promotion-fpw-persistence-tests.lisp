(in-package #:pg-physical-tests)

(defun promotion-fpw-append-source-checkpoint (store enabled)
  (let* ((begin (pg::physical-store-flushed-lsn store))
         (start (+ begin pg::+wal-header-size+))
         (snapshot (copy-list (getf pg::*physical-recovery-state* :source-checkpoint))))
    (setf (getf snapshot :start) start (getf snapshot :redo) start
          (getf snapshot :full-page-writes) enabled)
    (let ((end (+ start (pg::wal-checkpoint-record-size snapshot))))
      (setf (getf snapshot :end) end)
      (pg::physical-store-input store begin (sample-physical-record begin))
      (pg::physical-store-input store start
                                (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                                       (pg::wal-encode-checkpoint-snapshot snapshot)))
      (pg::physical-store-flush store)
      (assert (= end (pg::physical-replay-from begin)))
      (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
      end)))

(deftest promotion-fpw-cold-read-keeps-frozen-intent-without-recapture
    (dolist (source '(nil t :unknown))
      (dolist (desired '(nil t))
        (dolist (namespace '(:legacy :timeline))
          (call-with-promotion-fixture
           (lambda (root store)
             (declare (ignore root))
             (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
             (pg::wal-atomic-write-bytes (pg::pgdata-path "PG_VERSION") (pg::string->bytes (format nil "16~%")))
             (unless (eq source :unknown) (promotion-fpw-append-source-checkpoint store source))
             (let ((capture (symbol-function 'pg::startup-promotion-policy-arguments))
                   (calls 0)
                   (current desired)
                   (pg::*startup-installed-configuration* nil))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::startup-promotion-policy-arguments)
                            (lambda ()
                              (incf calls)
                              (list :promotion-full-page-writes current)))
                      (let* ((pending (call-with-promotion-exclusion
                                       (lambda () (pg::physical-promotion-publish-intent 2))))
                             (before (pg::physical-promotion-checkpoint-plan pending 20000 1003)))
                        (ok (= 1 calls))
                        (ok (get-properties pending '(:promotion-full-page-writes)))
                        (ok (eql desired (getf pending :promotion-full-page-writes)))
                        (ok (eql (eq source :unknown) (null (getf pending :source-fpw-fact))))
                        (unless (eq source :unknown)
                          (ok (eql source (getf (getf pending :source-fpw-fact) :enabled))))
                        (ok (get-properties (getf before :snapshot) '(:full-page-writes)))
                        (ok (eql desired (getf (getf before :snapshot) :full-page-writes)))
                        (let ((markers (remove-if-not
                                        (lambda (frame)
                                          (= pg::+wal-record-full-page-writes-change+
                                             (pg::wal-record-type (pg::wal-decode-record (second frame) (first frame)))))
                                        (getf before :frames))))
                          (ok (= (if (or (eq source :unknown) (not (eql source desired))) 1 0)
                                 (length markers)))
                          (when markers
                            (let ((frame (first markers)))
                              (ok (eql desired
                                       (pg::wal-full-page-writes-change-record-info
                                        (pg::wal-decode-record (second frame) (first frame))))))))
                        (setf current (not desired)
                              (getf pending :promotion-full-page-writes) (not desired))
                        (multiple-value-bind (cold metadata prefix generation)
                            (handler-bind ((error (lambda (condition)
                                                    (format *error-output* "~&COLD-FPW-READ-ERROR ~a~%" condition))))
                              (pg::physical-promotion-read-pending-state))
                          (declare (ignore metadata prefix generation))
                          (ok (= 1 calls))
                          (ok (equal (getf pending :source-fpw-fact) (getf cold :source-fpw-fact)))
                          (ok (get-properties cold '(:promotion-full-page-writes)))
                          (ok (eql desired (getf cold :promotion-full-page-writes)))
                          (ok (eql (eq source :unknown) (null (getf cold :source-fpw-fact))))
                          (ok (eql (eq source :unknown) (null (getf cold :required-fpw-fact))))
                          (ok (equalp (getf before :frames)
                                      (getf (pg::physical-promotion-checkpoint-plan cold 20000 1003) :frames))))))
                 (setf (symbol-function 'pg::startup-promotion-policy-arguments) capture))))
           :namespace namespace
           :system-id (pg::physical-store-update-digest
                       #xCBF29CE484222325 (pg::string->bytes "history-source")))))))

(deftest promotion-fpw-pending-rejects-runtime-intent-drift
    (dolist (intent '(:legacy nil t))
      (with-promotion-fixture (root store)
        (declare (ignore root store))
        (let ((capture (symbol-function 'pg::startup-promotion-policy-arguments))
              (pg::*startup-installed-configuration* nil))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::startup-promotion-policy-arguments)
                       (lambda () (unless (eq intent :legacy)
                                    (list :promotion-full-page-writes intent))))
                 (let ((pending (call-with-promotion-exclusion
                                 (lambda () (pg::physical-promotion-publish-intent 2)))))
                   (multiple-value-bind (metadata catalog prefix generation)
                       (pg::physical-restartpoint-read pending)
                     (declare (ignore catalog))
                     (dolist (change '(:flip :presence :required-fact))
                       (let ((bad (copy-list pending)))
                         (ecase change
                           (:flip (setf (getf bad :promotion-full-page-writes)
                                        (not (getf pending :promotion-full-page-writes))))
                           (:required-fact
                            (setf (getf bad :required-fpw-fact)
                                  (list :enabled nil :witness-kind :change :witness-start 168 :witness-end 216
                                        :witness-timeline 1 :coverage-start 168 :coverage-timeline 1
                                        :through 216 :through-timeline 1 :last-disable-start 168
                                        :last-disable-timeline 1)))
                           (:presence
                            (if (eq intent :legacy)
                                (setf (getf bad :promotion-full-page-writes) nil)
                                (remf bad :promotion-full-page-writes))))
                         (ok (intake-fails-p
                              (lambda ()
                                (call-with-promotion-exclusion
                                 (lambda ()
                                   (pg::physical-promotion-check-pending bad metadata prefix generation)))))))))))
            (setf (symbol-function 'pg::startup-promotion-policy-arguments) capture))))))

(in-package #:pg-physical-tests)

(deftest reseed-follow-settlement-client-errors-close-and-preserve-pending
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let* ((pending (pg::physical-reseed-follow-begin-settlement owner))
              (before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (dolist (boundary '(pg::physical-libpq-connect-frozen pg::pg-client-native-backup-lease-status
                             pg::pg-client-native-backup-lease-handoff pg::physical-client-close))
           (call-with-reseed-settlement-source
            owner root pending
            (lambda (state)
              (let ((original (symbol-function boundary)) (caught nil))
                (unwind-protect
                     (progn
                       (setf (symbol-function boundary)
                             (lambda (&rest arguments)
                               (apply original arguments)
                               (error "injected client boundary failure")))
                       (setf caught
                             (handler-case
                                 (progn (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper") nil)
                               (error (condition) (search "injected client boundary" (princ-to-string condition))))))
                  (setf (symbol-function boundary) original))
                (ok caught)
                (ok (= 1 (getf state :closes)))
                (ok (= (if (eq boundary 'pg::physical-libpq-connect-frozen) 0 1)
                       (getf state :statuses)))
                (ok (= (if (member boundary '(pg::pg-client-native-backup-lease-handoff pg::physical-client-close)) 1 0)
                       (getf state :handoffs)))
                (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                (ok (equal pending (pg::physical-reseed-follow-read owner)))
                (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))))))

(deftest reseed-follow-settlement-status-mutations-prevent-handoff
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
         (dolist (mutation '(:follow :temporary :attempt))
           (call-with-reseed-settlement-source
            owner root pending
            (lambda (state)
              (let ((original (symbol-function 'pg::pg-client-native-backup-lease-status))
                    (injected nil) (message nil)
                    (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
                (unwind-protect
                     (progn
                       (setf (symbol-function 'pg::pg-client-native-backup-lease-status)
                             (lambda (&rest arguments)
                               (let ((reply (apply original arguments)))
                                 (ecase mutation
                                   (:follow (reseed-test-replace-follow-record root pending))
                                   (:temporary (reseed-follow-owner-test-write temporary #()))
                                   (:attempt (reseed-settlement-test-abort-attempt root pending :aborting)))
                                 (setf injected t)
                                 reply)))
                       (setf message
                             (handler-case
                                 (progn (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper") nil)
                               (error (condition) (princ-to-string condition)))))
                  (setf (symbol-function 'pg::pg-client-native-backup-lease-status) original))
                (ok injected)
                (ok (and message
                         (search (if (eq mutation :temporary) "requires local recovery" "changed") message)))
                (ok (= 0 (getf state :handoffs)))
                (ok (= 1 (getf state :statuses) (getf state :closes)))
                (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                (when (eq mutation :temporary)
                  (ok (= 0 (length (lineage-test-bytes temporary))))
                  (delete-file temporary))))))))))

(deftest reseed-follow-settlement-detaches-receipt-through-client-close
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
         (call-with-reseed-settlement-source
          owner root pending
          (lambda (state)
            (setf (getf state :phase) :closed
                  (getf state :on-close)
                  (lambda ()
                    (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-settle owner :helper "/unused/helper"))))
                    (let ((actual (getf state :returned-receipt)))
                      (setf (getf (getf actual :request) :token) (make-string 64 :initial-element #\b))
                      (incf (getf (getf actual :base) :manifest-digest))
                      (setf (getf actual :outcome) :aborted (getf actual :successor) nil))))
            (let ((receipt (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper")))
              (ok (eq :handed-off (getf receipt :outcome)))
              (ok (equal (getf (getf pending :attempt) :request) (getf receipt :request)))
              (ok (equal (pg::physical-reseed-follow-settlement-base pending) (getf receipt :base)))
              (ok (equal (getf (getf pending :settlement) :successor) (getf receipt :successor))))
            (ok (= 1 (getf state :statuses) (getf state :closes)))
            (ok (= 0 (getf state :handoffs)))
            (ok (equal pending (pg::physical-reseed-follow-read owner)))))))))

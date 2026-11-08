(in-package #:pg-physical-tests)

(defun adoption-follow-history-chain (source &optional unterminated)
  (let* ((bytes (concatenate '(vector (unsigned-byte 8))
                             (pg::string->bytes "# inherited comment

")
                             (pg::timeline-encode-history 2 1 64 "first split" nil)))
         (bytes (if unterminated (subseq bytes 0 (1- (length bytes)))
                    (concatenate '(vector (unsigned-byte 8)) bytes #(10))))
         (chain (list (cons 2 (pg::bytes->string bytes)))))
    (loop for child from 3 to source
          do (setf bytes (pg::timeline-encode-history child (1- child) (* 64 (1- child))
                                                      "later split" bytes))
             (push (cons child (pg::bytes->string bytes)) chain))
    chain))

(deftest adoption-follow-child-history-preserves-copied-bytes
    (dolist (unterminated '(nil t))
      (let ((chain (adoption-follow-history-chain 6 unterminated)))
        (loop for child from 2 to 6
              do (ok (string= (cdr (assoc child chain))
                              (pg::physical-adoption-follow-history-prefix
                               6 child (cdr (assoc 6 chain)))))))))

(deftest adoption-follow-child-history-rejects-foreign-or-broken-lineage
    (dolist (case (list (list 3 1 (cdr (assoc 3 (adoption-follow-history-chain 3))))
                        (list 3 4 (cdr (assoc 3 (adoption-follow-history-chain 3))))
                        (list 4 3 "1 0/00000040 reason
")
                        (list 3 2 "4 0/00000040 reason
")))
      (ok (intake-fails-p
           (lambda () (apply #'pg::physical-adoption-follow-history-prefix case))))))

(cl-spec:defproperty adoption-follow-child-history-is-a-byte-preserving-prefix
    ((source (range integer 2 8)) (index (range integer 0 20)) (flag (range integer 0 1)))
  (:about pg::physical-adoption-follow-history-prefix)
  (:trials (:normal 100))
  (let* ((chain (adoption-follow-history-chain source (plusp flag)))
         (child (+ 2 (mod index (1- source)))))
    (string= (cdr (assoc child chain))
             (pg::physical-adoption-follow-history-prefix source child (cdr (assoc source chain))))))

(deftest generated-adoption-follow-history-prefixes
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-follow-child-history-is-a-byte-preserving-prefix :seed 83))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun call-with-adoption-follow-planning-fixture (function &key (fork-distance 64) (generations 1))
  (call-with-adoption-handoff-fixture
   (lambda (root owner stage workspace installed handoff)
     (declare (ignore stage workspace installed))
     (let ((value (pg::physical-adoption-handoff-start owner handoff)))
       (multiple-value-bind (base control) (pg::physical-recovery-verify-base-state)
         (let ((store (pg::physical-store-open
                       (merge-pathnames "pg_physical_receive/" root)
                       (getf base :system-id) (getf base :timeline)
                       :start-lsn (getf base :end) :discard-tail nil :create-lock nil
                       :retained-identity (pg::physical-recovery-retained-identity base))))
           (unwind-protect
                (funcall function root owner value base control store)
             (pg::physical-store-close store))))))
   :fork-distance fork-distance :generations generations))

(deftest adoption-follow-plan-reuses-the-proved-pin-and-empty-frontier
    (call-with-adoption-follow-planning-fixture
     (lambda (root owner handoff base control store)
       (declare (ignore owner))
       (let* ((record (getf (getf handoff :installation) :attempt))
              (stage (getf record :stage))
              (before (pg::physical-adoption-install-snapshot root))
              (binding (pg::physical-adoption-follow-make-binding record base control store))
              (config (pg::physical-adoption-follow-config record binding)))
         (ok (pg::physical-follow-binding-check binding))
         (ok (equal binding (pg::physical-adoption-follow-planned-binding record)))
         (ok (pg::physical-follow-check-config config))
         (ok (equal (getf binding :applied) (list (getf stage :end) (getf stage :timeline))))
         (ok (equal (getf binding :required) (getf binding :applied)))
         (ok (= (nth 9 (getf binding :before)) (getf stage :manifest-digest)))
         (ok (equal (fourth config) (getf stage :source-pin)))
         (ok (equal (getf (sixth config) :source) (getf record :source)))
         (ok (getf (sixth config) :enabled))
         (ok (null (getf (sixth config) :cleanup)))
         (ok (null (getf (sixth config) :unowned-bootstrap)))
         (ok (equal before (pg::physical-adoption-install-snapshot root)))))))

(deftest adoption-follow-plans-old-history-before-selected-recovery
    (call-with-adoption-follow-planning-fixture
     (lambda (root owner handoff base control store)
       (declare (ignore owner))
       (let* ((record (getf (getf handoff :installation) :attempt))
              (before (pg::physical-adoption-install-snapshot root)))
         (multiple-value-bind (binding candidate)
             (pg::physical-adoption-follow-make-binding record base control store)
           (let* ((images (pg::physical-adoption-follow-images record binding 12345))
                  (initial (pg::physical-recovery-decode-control (cdr (assoc "recovery.initial" images :test #'equal))))
                  (active (pg::physical-follow-decode-config (cdr (assoc "follow.active" images :test #'equal)))))
             (ok (eq :before (pg::physical-follow-pending-recovery-position base binding initial)))
             (ok (= 12345 (getf initial :state-time)))
             (ok (= 2 (getf initial :phase)))
             (ok (null (getf initial :history-target)))
             (ok (eq :active (getf (fifth active) :phase)))
             (ok (equal (getf (getf record :stage) :source-pin) (fourth active)))
             (when (not (equal (getf binding :before) (getf binding :after)))
               (let ((post (pg::physical-recovery-decode-control
                            (cdr (assoc "recovery.selected" images :test #'equal)))))
                 (ok (eq :after (pg::physical-follow-pending-recovery-position base binding post)))
                 (ok (equalp (pg::physical-store-encode-control candidate (getf base :end))
                             (cdr (assoc "receive.selected" images :test #'equal))))))
             (ok (equal before (pg::physical-adoption-install-snapshot root)))))))))

(deftest adoption-follow-plan-rejects-a-rebound-stage-witness
    (call-with-adoption-follow-planning-fixture
     (lambda (root owner handoff base control store)
       (declare (ignore root owner))
       (let* ((record (getf (getf handoff :installation) :attempt))
              (binding (pg::physical-adoption-follow-make-binding record base control store)))
         (dolist (mode '(:xid :resource :history :cluster))
           (let* ((changed (copy-tree binding))
                  (witness (getf changed :recovery)))
             (ecase mode
               (:xid (incf (getf witness :observed-next-xid)))
               (:resource
                (if (getf witness :resource-fact)
                    (incf (first (getf (getf witness :resource-fact) :resource-parameters)))
                    (setf (getf witness :resource-fact)
                          (list :resource-parameters (list 100 8 10 0 128)
                                :resource-lsn 0 :resource-timeline (getf (getf record :stage) :timeline)))))
               (:history (setf (getf (getf changed :anchor) :history-text)
                               (concatenate 'string (getf (getf changed :anchor) :history-text)
                                            (string #\Newline))))
               (:cluster (setf (getf witness :cluster-identity) "other")))
             (setf (getf changed :recovery) witness)
             (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-config record changed)))
                 (symbol-name mode))))))))

(deftest adoption-follow-plans-equal-forks-and-multiple-source-promotions
    (dolist (distance '(0 64))
      (dolist (generations '(1 2 3))
        (call-with-adoption-follow-planning-fixture
         (lambda (root owner handoff base control store)
           (declare (ignore root owner))
           (let* ((record (getf (getf handoff :installation) :attempt))
                  (binding (pg::physical-adoption-follow-make-binding record base control store))
                  (images (pg::physical-adoption-follow-images record binding 12345)))
             (ok (eq (zerop distance)
                     (not (equal (getf binding :before) (getf binding :after)))))
             (ok (= (1+ generations) (getf (getf binding :anchor) :source-timeline)))
             (ok (= (if (zerop distance) 7 3) (length images)))
             (when (zerop distance)
               (ok (= 2 (eighth (getf binding :after))))
               (let ((child (pg::timeline-check-lineage
                             (pg::timeline-parse-history 2 (getf binding :child-history)))))
                 (ok (= 2 (pg::timeline-history-entry-timeline (first child))))))))
         :fork-distance distance :generations generations))))

(defun check-adoption-store-open-handoff (function expected calls-target root spool)
  (dolist (kind '(:normal :error :throw :interrupt))
    (testing (symbol-name kind)
             (let ((original (symbol-function 'pg::physical-store-open))
                   (opened nil) (calls 0)
                   (tag (gensym "ADOPTION-STORE-HANDOFF"))
                   (inventory (pg::physical-adoption-install-snapshot root))
                   (control (lineage-test-bytes (merge-pathnames "receive.control" spool))))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::physical-store-open)
                            (lambda (&rest arguments)
                              (let ((store (apply original arguments)))
                                (incf calls)
                                (when (= calls calls-target)
                                  (setf opened store)
                                  (case kind
                                    (:error (error "injected adoption store handoff"))
                                    (:throw (throw tag :cut))
                                    (:interrupt
                                     (sb-thread:interrupt-thread
                                      sb-thread:*current-thread*
                                      (lambda () (error "injected adoption store handoff"))))))
                                store)))
                      (ok (equal (if (eq kind :normal) expected :cut)
                                 (catch tag
                                   (handler-case
                                       (multiple-value-list (funcall function))
                                     (error (condition)
                                       (when (search "injected adoption store handoff"
                                                     (princ-to-string condition))
                                         :cut))))))
                      (setf (symbol-function 'pg::physical-store-open) original)
                      (ok (= calls calls-target))
                      (ok (and opened (pg::physical-store-closed-p opened)))
                      (ok (and opened (not (open-stream-p (pg::physical-store-lock-stream opened)))))
                      (ok (equal inventory (pg::physical-adoption-install-snapshot root)))
                      (ok (equalp control (lineage-test-bytes (merge-pathnames "receive.control" spool))))
                      (ok (and opened
                               (handler-case
                                   (let ((fresh
                                           (funcall original spool
                                                    (pg::physical-store-system-id opened)
                                                    (pg::physical-store-timeline opened)
                                                    :start-lsn (pg::physical-store-base-lsn opened)
                                                    :discard-tail nil :create-lock nil
                                                    :retained-identity (pg::physical-store-retained-identity opened))))
                                     (unwind-protect (pg::physical-store-check-held-lock fresh)
                                       (pg::physical-store-close fresh)))
                                 (error () nil)))))
                 (setf (symbol-function 'pg::physical-store-open) original)
                 (when opened (pg::physical-store-close opened)))))))

(deftest adoption-empty-store-scopes-close-interrupted-open-handoffs
    (call-with-adoption-handoff-fixture
     (lambda (root owner stage workspace installed handoff)
       (declare (ignore stage workspace installed))
       (multiple-value-bind (value location) (pg::physical-adoption-handoff-start owner handoff)
         (ok (eq location :ready))
         (let* ((spool (merge-pathnames "pg_physical_receive/" root))
                (record (getf (getf value :installation) :attempt)))
           (testing "handoff spool validation"
                    (check-adoption-store-open-handoff
                     (lambda () (pg::physical-adoption-handoff-check-spool owner value location))
                     '(:ready) 1 root spool))
           (testing "follow preflight"
                    (check-adoption-store-open-handoff
                     (lambda ()
                       (multiple-value-bind (binding candidate)
                           (pg::physical-adoption-follow-preflight owner value)
                         (values binding
                                 (and (pg::physical-store-p candidate)
                                      (null (pg::physical-store-lock-stream candidate))
                                      (progn (pg::physical-store-check-live candidate) t)
                                      (equal (pg::physical-store-control-tuple candidate)
                                             (getf binding :after))))))
                     (list (pg::physical-adoption-follow-planned-binding record) t) 2 root spool)))))))

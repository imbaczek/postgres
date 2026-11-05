(in-package #:pg-physical-tests)

(defun reseed-follow-settlement-fixture (&optional selected)
  (multiple-value-bind (configuration core)
      (reseed-follow-configuration-codec-fixture :inert selected)
    (let* ((record (append core (list :configuration configuration)))
           (generation (first (getf core :generations)))
           (control (pg::physical-recovery-decode-control
                     (pg::physical-libpq-datum-octets
                      (getf (getf core :base) :recovery-control) 196)))
           (successor (list :name (copy-seq (getf generation :name))
                            :anchor-digest (getf configuration :anchor-digest)))
           (receipt (list :version 1 :request (getf (getf core :attempt) :request)
                          :phase :closed
                          :base (list :checkpoint-start (getf (getf core :base) :checkpoint-start)
                                      :base (getf control :applied)
                                      :manifest-digest (getf (getf core :base) :manifest-digest))
                          :outcome :handed-off :successor successor)))
      (values record (list :phase :handoff-pending :successor successor :receipt nil) receipt))))

(deftest reseed-follow-settlement-validates-exact-terminal-receipts
    (dolist (selected '(nil t))
      (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture selected)
        (ok (equal pending (pg::physical-reseed-follow-settlement-copy pending record)))
        (dolist (outcome '(:handed-off :source-aborted))
          (let ((terminal (copy-tree pending)) (actual (pg::physical-backup-lease-copy receipt)))
            (when (eq outcome :source-aborted)
              (setf (getf actual :outcome) :aborted (getf actual :successor) nil))
            (setf (getf terminal :phase) outcome (getf terminal :receipt) actual)
            (let ((copy (pg::physical-reseed-follow-settlement-copy terminal record)))
              (ok (equal terminal copy))
              (setf (char (getf (getf actual :request) :name) 0) #\x)
              (ok (not (equal terminal copy)))))))))

(deftest reseed-follow-settlement-rejects-unproved-outcomes-and-rebinding
    (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture)
      (dolist (key '(:checkpoint-start :base :manifest-digest))
        (let ((terminal (copy-tree pending)) (actual (copy-tree receipt)))
          (incf (getf (getf actual :base) key))
          (setf (getf terminal :phase) :handed-off (getf terminal :receipt) actual)
          (ok (handler-case
                  (progn (pg::physical-reseed-follow-settlement-copy terminal record) nil)
                (error () t)))))
      (dolist (phase '(:ready :closing))
        (let ((terminal (copy-tree pending)) (actual (copy-tree receipt)))
          (setf (getf actual :phase) phase
                (getf terminal :phase) :handed-off (getf terminal :receipt) actual)
          (ok (handler-case
                  (progn (pg::physical-reseed-follow-settlement-copy terminal record) nil)
                (error () t)))))
      (let ((wrong (copy-tree pending)))
        (incf (getf (getf wrong :successor) :anchor-digest))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-settlement-copy wrong record) nil)
              (error () t))))))

(cl-spec:defproperty reseed-follow-settlement-copies-preserve-pending-authority
    ((selected boolean))
  (:about pg::physical-reseed-follow-settlement-copy)
  (:trials (:normal 80))
  (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture selected)
    (declare (ignore receipt))
    (let ((copy (pg::physical-reseed-follow-settlement-copy pending record))
          (before (copy-tree record)))
      (setf (char (getf (getf pending :successor) :name) 0) #\x)
      (and (eq :handoff-pending (getf copy :phase))
           (null (getf copy :receipt))
           (not (equal pending copy))
           (equal before record)))))

(deftest generated-reseed-follow-settlement-owned-copies
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-settlement-copies-preserve-pending-authority
                                       :seed 124 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-settlement-journal-preserves-optional-encoding
    (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture)
      (declare (ignore receipt))
      (let* ((old-bytes (pg::physical-backup-lease-encode-literal
                         "PGRESEEDFOLLOW1" record pg::+physical-reseed-follow-maximum-bytes+))
             (candidate (append record (list :settlement pending))))
        (ok (equal record (pg::physical-reseed-follow-decode old-bytes)))
        (ok (equalp old-bytes (pg::physical-reseed-follow-image
                               (pg::physical-reseed-follow-decode old-bytes))))
        (ok (not (member :settlement (pg::physical-reseed-follow-copy record))))
        (ok (equal candidate (pg::physical-reseed-follow-decode
                              (pg::physical-reseed-follow-image candidate))))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-copy
                        (append record (list :settlement nil))) nil)
              (error () t))))))

(deftest reseed-follow-settlement-journal-permits-only-pending-then-exact-outcome
    (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture)
      (let* ((candidate (append record (list :settlement pending)))
             (terminal (pg::physical-reseed-follow-copy candidate)))
        (setf (getf terminal :settlement)
              (list :phase :handed-off :successor (getf pending :successor) :receipt receipt))
        (ok (pg::physical-reseed-follow-check-transition record candidate))
        (ok (pg::physical-reseed-follow-check-transition candidate candidate))
        (ok (pg::physical-reseed-follow-check-transition candidate terminal))
        (ok (pg::physical-reseed-follow-check-transition terminal terminal))
        (dolist (pair (list (list record terminal) (list candidate record)
                            (list terminal candidate)))
          (ok (handler-case
                  (progn (apply #'pg::physical-reseed-follow-check-transition pair) nil)
                (error () t))))
        (let ((changed (pg::physical-reseed-follow-copy terminal)))
          (incf (getf (getf changed :configuration) :time))
          (ok (handler-case
                  (progn (pg::physical-reseed-follow-check-transition candidate changed) nil)
                (error () t)))))))

(deftest reseed-follow-settlement-reserves-both-terminal-encodings-at-the-boundary
    (dolist (selected '(nil t))
      (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture selected)
        (let* ((candidate (pg::physical-reseed-follow-copy
                           (append record (list :settlement pending))))
               (pending-size (length (pg::physical-backup-lease-encode-literal
                                      "PGRESEEDFOLLOW1" candidate
                                      pg::+physical-reseed-follow-maximum-bytes+)))
               (terminal-sizes
                 (loop for phase in '(:handed-off :source-aborted)
                       collect
                       (let ((terminal (pg::physical-reseed-follow-copy candidate))
                             (actual (pg::physical-backup-lease-copy receipt)))
                         (when (eq phase :source-aborted)
                           (setf (getf actual :outcome) :aborted (getf actual :successor) nil))
                         (setf (getf terminal :settlement)
                               (list :phase phase :successor (getf pending :successor) :receipt actual))
                         (length (pg::physical-reseed-follow-image terminal)))))
               (boundary (+ 32 (apply #'max terminal-sizes))))
          (ok (> boundary (+ pending-size 32)))
          (ok (pg::physical-reseed-follow-check-image-capacity candidate pending-size boundary))
          (ok (handler-case
                  (progn (pg::physical-reseed-follow-check-image-capacity
                          candidate pending-size (1- boundary)) nil)
                (error () t)))
          (ok (equal candidate (pg::physical-reseed-follow-decode
                                (pg::physical-reseed-follow-image candidate))))))))

(deftest reseed-follow-settlement-rejects-other-receipt-authority
    (dolist (mutation '(:request-token :successor-name :successor-digest :aborted-successor))
      (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture)
        (let ((actual (pg::physical-backup-lease-copy receipt)))
          (ecase mutation
            (:request-token (setf (getf (getf actual :request) :token) (make-string 64 :initial-element #\b)))
            (:successor-name (setf (getf (getf actual :successor) :name) "other_follow"))
            (:successor-digest (incf (getf (getf actual :successor) :anchor-digest)))
            (:aborted-successor (setf (getf actual :outcome) :aborted)))
          (ok (handler-case
                  (progn
                    (pg::physical-reseed-follow-settlement-copy
                     (list :phase :handed-off :successor (getf pending :successor) :receipt actual)
                     record)
                    nil)
                (error () t)))))))

(deftest reseed-follow-settlement-rejects-unready-local-evidence
    (dolist (mutation '(:staged :unproved :no-generation :missing-configuration))
      (multiple-value-bind (record pending) (reseed-follow-settlement-fixture)
        (ecase mutation
          (:staged (setf (getf (getf record :configuration) :phase) :staged
                         (getf (getf record :configuration) :step) 0))
          (:unproved (setf (getf (first (getf record :generations)) :phase) :preparing
                           (getf (first (getf record :generations)) :anchor) nil))
          (:no-generation (setf (getf record :generations) nil))
          (:missing-configuration (remf record :configuration)))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-settlement-copy pending record) nil)
              (error () t))))))

(deftest reseed-follow-settlement-source-abort-is-terminal-and-cannot-flip
    (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture)
      (let* ((before (append record (list :settlement pending)))
             (handed (pg::physical-reseed-follow-copy before))
             (aborted (pg::physical-reseed-follow-copy before))
             (abort-receipt (pg::physical-backup-lease-copy receipt)))
        (setf (getf abort-receipt :outcome) :aborted
              (getf abort-receipt :successor) nil
              (getf handed :settlement)
              (list :phase :handed-off :successor (getf pending :successor) :receipt receipt)
              (getf aborted :settlement)
              (list :phase :source-aborted :successor (getf pending :successor) :receipt abort-receipt))
        (ok (pg::physical-reseed-follow-check-transition before aborted))
        (ok (pg::physical-reseed-follow-check-transition aborted aborted))
        (dolist (pair (list (list handed aborted) (list aborted handed) (list aborted before)))
          (ok (handler-case
                  (progn (apply #'pg::physical-reseed-follow-check-transition pair) nil)
                (error () t)))))))

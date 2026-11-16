(in-package #:pg)

(cl-spec:defproperty continuation-parent-proof-uses-only-earlier-reserved-anchors
    ((delta (range integer -2 65)) (lost (range integer 0 1)))
  (:about logical-slot-continuation-parent-proof-start)
  (:trials (:normal 200))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
    (declare (ignore rows))
    (let* ((row (first (getf certificate :slots)))
           (slot (logical-slot-state-decode (string->bytes (fourth row)) (first row)))
           (restart (wal-replication-slot-restart-lsn slot))
           (target (+ restart delta))
           (history (logical-slot-continuation-history 2 (fourth host))))
      (when (= lost 1)
        (setf (getf certificate :slots) (rest (getf certificate :slots))))
      (let ((result (handler-case
                        (logical-slot-continuation-parent-proof-start
                         certificate history (list (list :confirmed target 1 :prefix-end)) 96)
                      (error () nil))))
        (if (and (= lost 0) (> delta 0) (< restart 96))
            (and result (= restart (first result)) (= 1 (second result)))
            (null result))))))

(rove:deftest continuation-parent-proof-anchor-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property
                    'continuation-parent-proof-uses-only-earlier-reserved-anchors
                    :seed 11603 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest continuation-parent-proof-rejects-an-open-lane-seam
    (let* ((start 80)
           (bytes (make-array start :element-type '(unsigned-byte 8) :initial-element 0))
           (history (logical-slot-continuation-history 1 (make-array 0 :element-type '(unsigned-byte 8)))))
      (dolist (type (list +wal-record-batch-start+ +wal-record-batch-start+ +wal-record-maintenance+))
        (let ((raw (wal-encode-record type 0 start (make-array 0 :element-type '(unsigned-byte 8)))))
          (setf bytes (concatenate '(vector (unsigned-byte 8)) bytes raw))
          (incf start (length raw))))
      (call-with-cursor-provider-file
       bytes
       (lambda (path)
         (call-with-cursor-provider-input
          path
          (lambda (stream size)
            (dolist (end '(112 176))
              (let ((accepted
                      (handler-case
                          (multiple-value-bind (observe finish)
                              (logical-slot-continuation-boundary-observer
                               nil :batch-policy :logical-reader)
                            (call-with-logical-wal-cursor
                             history :timeline 80 end 1 :record-start (lambda () t)
                             (lambda (cursor)
                               (logical-slot-continuation-observe-boundary-cursor cursor observe end)
                               (funcall finish))
                             :segment-reader
                             (lambda (segment carrier consume)
                               (declare (ignore segment carrier))
                               (funcall consume stream size))
                             :batch-policy :logical-reader))
                        (error () nil))))
                (rove:ok (eq (not (null accepted)) (= end 176))
                         "An open D seam cannot borrow completion from later archived WAL")))))))))

(defun unit-selected-prefix-records (observe &optional (start 80))
  (dolist (type (list +wal-record-batch-start+ +wal-record-batch-start+ +wal-record-maintenance+))
    (let* ((raw (wal-encode-record type 0 start (make-array 0 :element-type '(unsigned-byte 8))))
           (record (wal-decode-record raw start)))
      (funcall observe record raw 2 2 :ordinary start)
      (setf start (wal-record-end record))))
  start)

(rove:deftest continuation-selected-prefix-allows-abandonment-with-a-completed-seal
    (multiple-value-bind (observe finish)
        (logical-slot-continuation-boundary-observer '((:confirmed 176 2 :prefix-end)))
      (rove:ok (handler-case (progn (unit-selected-prefix-records observe) nil) (error () t)))
      (rove:ok (handler-case (progn (funcall finish) nil) (error () t))))
  (multiple-value-bind (observe finish)
      (logical-slot-continuation-boundary-observer
       '((:confirmed 176 2 :prefix-end)) :batch-policy :logical-reader)
    (rove:ok (= 176 (unit-selected-prefix-records observe)))
    (rove:ok (funcall finish))))

(rove:deftest continuation-selected-prefix-cannot-repair-an-open-or-crossed-seal
    (dolist (seal '(111 112 144 176))
      (let ((accepted
              (handler-case
                  (multiple-value-bind (observe finish)
                      (logical-slot-continuation-runtime-tail-observer 80 seal nil :logical-reader)
                    (unit-selected-prefix-records observe)
                    (let* ((raw (wal-encode-record +wal-record-batch-start+ 0 176
                                                   (make-array 0 :element-type '(unsigned-byte 8))))
                           (record (wal-decode-record raw 176)))
                      (funcall observe record raw 2 2 :ordinary 176))
                    (funcall finish))
                (error () nil))))
        (rove:ok (eq (not (null accepted)) (= seal 176))
                 (format nil "seal ~d must already be completed before the open suffix" seal)))))

(rove:deftest continuation-selected-prefix-keeps-checkpoint-and-eor-ack-roles
    (let* ((start 80)
           (marker (wal-encode-record +wal-record-batch-start+ 0 start
                                      (make-array 0 :element-type '(unsigned-byte 8))))
           (checkpoint (wal-encode-record +wal-record-checkpoint+ 0 (+ start +wal-header-size+)
                                          (make-array 0 :element-type '(unsigned-byte 8))))
           (end (+ start (* 2 +wal-header-size+))))
      (multiple-value-bind (observe finish)
          (logical-slot-continuation-boundary-observer
           (list (list :confirmed end 2 :prefix-end)) :batch-policy :logical-reader)
        (funcall observe (wal-decode-record marker start) marker 2 2 :ordinary start)
        (funcall observe (wal-decode-record checkpoint (+ start +wal-header-size+))
                 checkpoint 2 2 :ordinary (+ start +wal-header-size+))
        (rove:ok (funcall finish)))
      (multiple-value-bind (observe finish)
          (logical-slot-continuation-boundary-observer
           (list (list :confirmed end 2 :prefix-end))
           :checkpoint (list (+ start +wal-header-size+) checkpoint) :batch-policy :logical-reader)
        (funcall observe (wal-decode-record checkpoint (+ start +wal-header-size+))
                 checkpoint 2 2 :ordinary (+ start +wal-header-size+))
        (rove:ok (handler-case (progn (funcall finish) nil) (error () t)))))
  (multiple-value-bind (fork eor-end ignored ignored-end) (unit-continuation-boundary-positions)
    (declare (ignore ignored ignored-end))
    (multiple-value-bind (observe finish)
        (logical-slot-continuation-boundary-observer
         (list (list :confirmed eor-end 2 :prefix-end)) :fork fork :batch-policy :logical-reader)
      (unit-continuation-boundary-records observe)
      (rove:ok (handler-case (progn (funcall finish) nil) (error () t))))))

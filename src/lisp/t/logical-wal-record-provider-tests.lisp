(in-package #:pg-physical-tests)

(defun provider-test-call (history start end reader function &key (owner 1) (guard (lambda () t)) (batch-policy :strict))
  ;; These fixtures supply cursor mechanics, not storage or slot authority.
  (pg::call-with-logical-wal-record-provider
   reader guard
   (lambda (provider)
     (pg::call-with-logical-wal-cursor
      history :timeline start end owner :prefix-end guard function :record-reader provider :batch-policy batch-policy))))

(cl-spec:defproperty record-provider-preserves-rotation-geometry
    ((gap (range integer 1 128)) (length (range integer 33 128)))
  (:about pg::call-with-logical-wal-record-provider pg::logical-wal-cursor-next)
  (:trials (:normal 200))
  (let* ((start pg::+wal-segment-size+)
         (raw (lineage-test-record start length))
         (calls 0)
         (result
           (handler-case
               (provider-test-call
                (pg::timeline-parse-history 1 "") (- start gap) (+ start length)
                (lambda (position consume)
                  (assert (= position (- start gap)))
                  (assert (not (sb-thread:holding-mutex-p pg::*wal-lock*)))
                  (incf calls)
                  (funcall consume raw start 1))
                (lambda (cursor)
                  (let ((values (multiple-value-list (pg::logical-wal-cursor-next cursor))))
                    (and (equalp raw (second values)) (= 1 (third values) (fourth values))
                         (= (+ start length) (pg::logical-wal-cursor-position cursor))
                         (null (pg::logical-wal-cursor-next cursor))))))
             (error () nil))))
    (and (= 1 calls) (eq (not (null result)) (< gap length)))))

(deftest record-provider-generated-rotation-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'record-provider-preserves-rotation-geometry
                                         :seed 7523 :profile :normal))))
      (ok (eq :passed (getf result :status)) (format nil "~s" result))
      (ok (= 200 (getf result :trials)))))

(deftest record-provider-keeps-midsegment-carriers-separate-from-record-owners
    (let* ((fork 66)
           (eor (pg::wal-encode-end-of-recovery-record fork 0 2 1 2))
           (child (+ fork (length eor)))
           (records (list (list 0 (lineage-test-record 0) 1)
                          (list 33 (lineage-test-record 33) 1)
                          (list fork eor 2) (list child (lineage-test-record child) 2)))
           (calls nil))
      (provider-test-call
       (lineage-test-history fork) 0 (+ child 33)
       (lambda (position consume)
         (push position calls)
         (let ((entry (assoc position records)))
           (assert entry)
           (funcall consume (second entry) position (third entry))))
       (lambda (cursor)
         (let ((rows (cursor-test-collect cursor)))
           (ok (equal (mapcar (lambda (row) (subseq row 2 5)) rows)
                      '((1 1 :ordinary) (1 1 :ordinary) (2 2 :end-of-recovery) (2 2 :ordinary))))
           (ok (equal (reverse calls) (mapcar #'first records)))
           (ok (every #'equalp (mapcar #'sixth rows) (mapcar #'second records))))))))

(defun provider-test-rejects (reader)
  (let ((calls 0) (poison pg::*wal-poisoned*))
    (provider-test-call
     (pg::timeline-parse-history 1 "") 0 33
     (lambda (position consume)
       (incf calls)
       (funcall reader position consume))
     (lambda (cursor)
       (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
       (ok (zerop (pg::logical-wal-cursor-position cursor)))
       (ok (pg::logical-wal-cursor-failed cursor))
       (ok (not (pg::logical-wal-cursor-busy cursor)))
       (ok (not (pg::logical-wal-record-provider-busy
                 (pg::logical-wal-cursor-record-reader cursor))))
       (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
       (ok (= 1 calls))
       (ok (eq poison pg::*wal-poisoned*))))))

(deftest record-provider-rejects-missing-duplicate-and-malformed-delivery
    (provider-test-rejects (lambda (position consume) (declare (ignore position consume))))
  (provider-test-rejects
   (lambda (position consume)
     (funcall consume (lineage-test-record position) position 1)
     (handler-case (funcall consume (lineage-test-record position) position 1)
       (error () nil))))
  (dolist (kind '(:crc :short :extra :position :carrier :backward :horizon :segment))
    (provider-test-rejects
     (lambda (position consume)
       (let ((raw (lineage-test-record position)) (start position) (carrier 1))
         (case kind
           (:crc (setf (aref raw 0) 0))
           (:short (setf raw (subseq raw 0 31)))
           (:extra (setf raw (concatenate '(vector (unsigned-byte 8)) raw #(0))))
           (:position (setf raw (lineage-test-record 1)))
           (:carrier (setf carrier 99))
           (:backward (setf start -1))
           (:horizon (setf start 33))
           (:segment (setf start (- pg::+wal-segment-size+ 16))))
         (funcall consume raw start carrier))))))

(deftest record-provider-waits-for-postcheck-and-copies-private-bytes
    (let ((raw (lineage-test-record 0)) (allowed t) (calls 0))
      (provider-test-call
       (pg::timeline-parse-history 1 "") 0 33
       (lambda (position consume)
         (incf calls)
         (funcall consume raw position 1)
         (setf (aref raw 0) 0))
       (lambda (cursor)
         (ok (pg::logical-wal-cursor-next cursor))
         (ok (null (pg::logical-wal-cursor-next cursor)))
         (ok (= 1 calls)))
       :guard (lambda ()
                (assert (sb-thread:holding-mutex-p pg::*wal-lock*))
                allowed))
      (provider-test-call
       (pg::timeline-parse-history 1 "") 0 33
       (lambda (position consume)
         (funcall consume (lineage-test-record position) position 1)
         (setf allowed nil))
       (lambda (cursor)
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
         (ok (zerop (pg::logical-wal-cursor-position cursor)))
         (setf allowed t)
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error)))
       :guard (lambda () allowed)))
  (provider-test-rejects
   (lambda (position consume)
     (funcall consume (lineage-test-record position) position 1)
     (error "provider postcheck failed"))))

(deftest record-provider-invalidates-reentry-nonlocal-exit-and-escaped-consumer
    (let ((current nil) (saved nil))
      (provider-test-call
       (pg::timeline-parse-history 1 "") 0 33
       (lambda (position consume)
         (handler-case (pg::logical-wal-cursor-next current) (error () nil))
         (funcall consume (lineage-test-record position) position 1))
       (lambda (cursor)
         (setf current cursor)
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
         (ok (zerop (pg::logical-wal-cursor-position cursor)))
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))))
      (provider-test-call
       (pg::timeline-parse-history 1 "") 0 33
       (lambda (position consume)
         (setf saved consume)
         (funcall consume (lineage-test-record position) position 1))
       (lambda (cursor)
         (ok (pg::logical-wal-cursor-next cursor))
         (ok (signals (funcall saved (lineage-test-record 33) 33 1) 'error))
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))))
      (provider-test-call
       (pg::timeline-parse-history 1 "") 0 33
       (lambda (position consume) (declare (ignore position consume)) (throw :provider-cut :cut))
       (lambda (cursor)
         (ok (eq :cut (catch :provider-cut (pg::logical-wal-cursor-next cursor))))
         (ok (zerop (pg::logical-wal-cursor-position cursor)))
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))))))

(deftest record-provider-rejects-forged-expired-and-foreign-scopes
    (let ((saved nil) (calls 0)
                      (reader (lambda (position consume)
                                (funcall consume (lineage-test-record position) position 1))))
      (pg::call-with-logical-wal-record-provider
       reader (lambda () t)
       (lambda (provider)
         (setf saved provider)
         (let ((copy (pg::copy-logical-wal-record-provider provider)))
           (ok (signals
                (pg::call-with-logical-wal-cursor
                 (pg::timeline-parse-history 1 "") :timeline 0 33 1 :prefix-end
                 (lambda () t) (lambda (cursor) (pg::logical-wal-cursor-next cursor))
                 :record-reader copy) 'error)))
         (ok (sb-thread:join-thread
              (sb-thread:make-thread
               (lambda ()
                 (handler-case
                     (progn
                       (pg::call-with-logical-wal-cursor
                        (pg::timeline-parse-history 1 "") :timeline 0 33 1 :prefix-end
                        (lambda () t) (lambda (cursor) (pg::logical-wal-cursor-next cursor))
                        :record-reader provider)
                       nil)
                   (error () t))))))
         (pg::call-with-logical-wal-cursor
          (pg::timeline-parse-history 1 "") :timeline 0 33 1 :prefix-end
          (lambda () t) (lambda (cursor) (incf calls) (ok (pg::logical-wal-cursor-next cursor)))
          :record-reader provider)))
      (ok (= 1 calls))
      (ok (signals
           (pg::call-with-logical-wal-cursor
            (pg::timeline-parse-history 1 "") :timeline 0 33 1 :prefix-end
            (lambda () t) (lambda (cursor) (pg::logical-wal-cursor-next cursor))
            :record-reader saved) 'error))
      (ok (signals
           (pg::call-with-logical-wal-cursor
            (pg::timeline-parse-history 1 "") :timeline 0 33 1 :prefix-end
            (lambda () t) (lambda (cursor) (declare (ignore cursor)))
            :record-reader reader) 'error))))

(deftest record-provider-rejects-regressing-and-ancestor-carriers
    (let ((calls 0))
      (provider-test-call
       (lineage-test-history 100) 0 66
       (lambda (position consume)
         (incf calls)
         (funcall consume (lineage-test-record position) position (if (zerop position) 2 1)))
       (lambda (cursor)
         (ok (pg::logical-wal-cursor-next cursor))
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
         (ok (= 33 (pg::logical-wal-cursor-position cursor)))
         (ok (= 2 calls))
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
         (ok (= 2 calls)))))
  (provider-test-call
   (lineage-test-history 0) 0 pg::+wal-end-of-recovery-record-size+
   (lambda (position consume)
     (funcall consume (pg::wal-encode-end-of-recovery-record position 0 2 1 2) position 1))
   (lambda (cursor)
     (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
     (ok (zerop (pg::logical-wal-cursor-position cursor))))
   :owner 1))

(deftest record-provider-rejects-segment-crossing-before-decode
    (let ((start (- pg::+wal-segment-size+ 16)) (calls 0))
      (provider-test-call
       (pg::timeline-parse-history 1 "") start (+ pg::+wal-segment-size+ 33)
       (lambda (position consume)
         (incf calls)
         (funcall consume (lineage-test-record position) position 1))
       (lambda (cursor)
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
         (ok (= start (pg::logical-wal-cursor-position cursor)))
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
         (ok (= 1 calls))))))

(deftest record-provider-rejects-cross-cursor-reentry
    (let ((current nil) (inner nil) (calls 0))
      (provider-test-call
       (pg::timeline-parse-history 1 "") 0 33
       (lambda (position consume)
         (incf calls)
         (handler-case
             (pg::call-with-logical-wal-cursor
              (pg::timeline-parse-history 1 "") :timeline 0 33 1 :prefix-end
              (lambda () t)
              (lambda (cursor) (setf inner cursor) (pg::logical-wal-cursor-next cursor))
              :record-reader (pg::logical-wal-cursor-record-reader current))
           (error () nil))
         (funcall consume (lineage-test-record position) position 1))
       (lambda (cursor)
         (setf current cursor)
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
         (ok inner)
         (ok (pg::logical-wal-cursor-failed inner))
         (ok (pg::logical-wal-cursor-failed cursor))
         (ok (zerop (pg::logical-wal-cursor-position cursor)))
         (ok (not (pg::logical-wal-record-provider-busy
                   (pg::logical-wal-cursor-record-reader cursor))))
         (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
         (ok (= 1 calls))))))

(deftest record-provider-latches-interrupts-and-locked-or-foreign-consumers
    (provider-test-rejects
     (lambda (position consume)
       (funcall consume (lineage-test-record position) position 1)
       (sb-thread:interrupt-thread
        sb-thread:*current-thread* (lambda () (error "provider read interrupted")))))
  (provider-test-rejects
   (lambda (position consume)
     (sb-thread:with-mutex (pg::*wal-lock*)
       (funcall consume (lineage-test-record position) position 1))))
  (provider-test-rejects
   (lambda (position consume)
     (assert
      (sb-thread:join-thread
       (sb-thread:make-thread
        (lambda ()
          (handler-case
              (progn (funcall consume (lineage-test-record position) position 1) nil)
            (error () t))))))
     (handler-case (funcall consume (lineage-test-record position) position 1)
       (error () nil)))))

(deftest record-provider-keeps-batch-horizons-and-terminal-classification
    (let* ((batch (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 #()))
           (terminal (pg::wal-encode-record pg::+wal-record-maintenance+ 0 32 #()))
           (reader (lambda (position consume)
                     (funcall consume (if (zerop position) batch terminal) position 1))))
      (dolist (target '(33 64))
        (provider-test-call
         (pg::timeline-parse-history 1 "") 0 64 reader
         (lambda (cursor)
           (ok (eq (= target 64) (not (null (pg::logical-wal-cursor-terminal-p cursor target))))))))
      (let ((calls 0))
        (provider-test-call
         (pg::timeline-parse-history 1 "") 0 32
         (lambda (position consume) (incf calls) (funcall consume batch position 1))
         (lambda (cursor)
           (ok (pg::logical-wal-cursor-next cursor))
           (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
           (ok (= 1 calls))
           (ok (not (pg::logical-wal-cursor-busy cursor)))
           (ok (pg::logical-wal-cursor-failed cursor)))))
      (provider-test-call
       (pg::timeline-parse-history 1 "") 0 32 reader
       (lambda (cursor) (ok (= 1 (length (cursor-test-collect cursor)))))
       :batch-policy :logical-reader))
  (let ((raw (pg::wal-encode-end-of-recovery-record 0 0 2 1 2)))
    (provider-test-call
     (lineage-test-history 0) 0 (length raw)
     (lambda (position consume) (funcall consume raw position 2))
     (lambda (cursor)
       (ok (not (pg::logical-wal-cursor-terminal-p cursor (length raw))))))))

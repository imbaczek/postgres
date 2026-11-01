;;;; Test-only conservative feedback credit in an owned replica.
(in-package #:pg)

(defvar *syncrep-test-feedback-lock* (sb-thread:make-mutex :name "test feedback credit"))
(defvar *syncrep-test-feedback-actual* nil)
(defvar *syncrep-test-feedback-reported* nil)
(defvar *syncrep-test-feedback-limits* nil)

(defun syncrep-test-cap-feedback (payload)
  "Copy an owned replica's feedback and cap its three positions for a test.
The caller holds the private feedback lock; actual and reported credit stay ordered."
  (let* ((copy (copy-seq payload))
         (actual (mapcar (lambda (offset) (pg-client-u64 payload offset)) '(1 9 17)))
         (reported (loop for value in actual
                         for index from 0
                         for limit = (nth index *syncrep-test-feedback-limits*)
                         collect (if limit (min value limit) value))))
    (setf *syncrep-test-feedback-actual* actual)
    (assert (apply #'>= reported))
    (when *syncrep-test-feedback-reported*
      (assert (every #'<= *syncrep-test-feedback-reported* reported)))
    (loop for value in reported for offset in '(1 9 17)
          do (dotimes (index 8)
               (setf (aref copy (+ offset index))
                     (ldb (byte 8 (* 8 (- 7 index))) value))))
    (values copy reported)))

(defmethod physical-client-feedback :around ((client t) payload)
  "Cap an owned replica's reply and always forward a valid status frame.
The private lock serializes arming with transmission. Record reported positions
only after the underlying send succeeds; actual receive and replay keep running."
  (assert (and (= (length payload) 34) (= (aref payload 0) (char-code #\r))))
  (sb-thread:with-mutex (*syncrep-test-feedback-lock*)
    (multiple-value-bind (copy reported) (syncrep-test-cap-feedback payload)
      (multiple-value-prog1 (call-next-method client copy)
        (setf *syncrep-test-feedback-reported* reported)))))

(defun syncrep-test-arm-feedback ()
  "Freeze credit at the last reported positions while replies continue."
  (sb-thread:with-mutex (*syncrep-test-feedback-lock*)
    (assert *syncrep-test-feedback-reported*)
    (setf *syncrep-test-feedback-limits* (copy-list *syncrep-test-feedback-reported*))
    (copy-list *syncrep-test-feedback-limits*)))

(defun syncrep-test-release-feedback (level)
  "Release the first LEVEL write, flush and apply credit caps."
  (check-type level (integer 0 3))
  (sb-thread:with-mutex (*syncrep-test-feedback-lock*)
    (when *syncrep-test-feedback-limits*
      (dotimes (index level)
        (setf (nth index *syncrep-test-feedback-limits*) nil))))
  (values))

(defun syncrep-test-feedback-state ()
  "Return copies of sampled positions, the last successful reply and credit caps."
  (sb-thread:with-mutex (*syncrep-test-feedback-lock*)
    (list :actual (copy-list *syncrep-test-feedback-actual*)
          :reported (copy-list *syncrep-test-feedback-reported*)
          :limits (copy-list *syncrep-test-feedback-limits*))))

;;;; Additional C byte-preservation cases, including partial known history.
(in-package #:pg-physical-tests)

(deftest timeline-history-parent-presence-controls-newline-even-for-empty-files
    (dolist (text (list "" "1 0/40 reason" (format nil "1 0/40 reason~%")))
      (let* ((parent (pg::string->bytes text))
             (bytes (pg::timeline-encode-history 3 2 64 "equal fork" parent))
             (expected (concatenate 'string text
                                    (format nil "~%2~c0/00000040~cequal fork~%" #\Tab #\Tab))))
        (ok (equalp bytes (pg::string->bytes expected)))
        (let ((history (pg::timeline-parse-history 3 (pg::bytes->string bytes))))
          ;; An existing empty parent history proves only a timeline-2 root.
          (ok (= (if (zerop (length parent)) 2 1) (pg::timeline-at-point 0 history)))
          (ok (= 3 (pg::timeline-at-point 64 history)))))))

(deftest timeline-history-reason-bounds-preserve-readable-native-records
    (let ((nul (string (code-char 0))))
      (ok (intake-fails-p (lambda () (pg::timeline-encode-history 2 1 64 nul nil)))))
  (ok (intake-fails-p
       (lambda ()
         (pg::timeline-encode-history 2 1 64 (make-string 1024 :initial-element #\a) nil)))))

(deftest timeline-segment-candidates-follow-c-file-order-and-monotonic-floor
    (let* ((size pg::+wal-segment-size+)
           (history (list (pg::make-timeline-history-entry :timeline 7 :begin (* 2 size))
                          (pg::make-timeline-history-entry :timeline 4 :begin (+ size 64) :end (* 2 size))
                          (pg::make-timeline-history-entry :timeline 1 :begin 0 :end (+ size 64)))))
      ;; The child's copied prefix can contain records belonging to its parent.
      (ok (equal '(1) (pg::timeline-segment-candidates 0 history)))
      (ok (equal '(4 1) (pg::timeline-segment-candidates 1 history)))
      (ok (= 1 (pg::timeline-at-point size history)))
      (ok (equal '(7 4 1) (pg::timeline-segment-candidates 2 history)))
      ;; C does not filter ancestors by end segment: callers validate record TLI.
      (ok (equal '(7 4 1) (pg::timeline-segment-candidates 3 history)))
      (ok (equal '(7 4) (pg::timeline-segment-candidates 3 history 4)))
      (ok (null (pg::timeline-segment-candidates 1 history 7)))
      (dolist (bad '(-1 1099511627776))
        (ok (intake-fails-p (lambda () (pg::timeline-segment-candidates bad history)))))
      (ok (intake-fails-p (lambda () (pg::timeline-segment-candidates 0 history -1))))
      (ok (intake-fails-p (lambda () (pg::timeline-segment-candidates 0 history 2))))
      (ok (intake-fails-p (lambda () (pg::timeline-segment-candidates 0 nil))))))

(deftest timeline-segment-candidates-keep-equal-forks-and-partial-known-roots
    (let ((history (list (pg::make-timeline-history-entry :timeline 9 :begin 64)
                         (pg::make-timeline-history-entry :timeline 6 :begin 64 :end 64)
                         (pg::make-timeline-history-entry :timeline 3 :begin 0 :end 64))))
      (ok (equal '(9 6 3) (pg::timeline-segment-candidates 0 history)))
      (ok (= 9 (pg::timeline-at-point 64 history)))
      (ok (equal '(9 6) (pg::timeline-segment-candidates 0 history 6)))))

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

(in-package #:pg-physical-tests)

(defun adoption-retire-expected-marker (crc)
  (pg::string->bytes
   (format nil "RETIRE ~8,'0X~%"
           (pg::wal-crc32 (pg::string->bytes (format nil "~8,'0X RETIRE" crc))))))

(deftest adoption-retire-marker-follows-only-complete-follow-progress
    (dolist (distance '(0 64))
      (dolist (generations '(1 2 3))
        (let* ((plan (adoption-follow-codec-fixture :distance distance :generations generations))
               (last (length (pg::physical-adoption-follow-moves plan)))
               (image (pg::physical-adoption-follow-image plan last))
               (state (pg::physical-adoption-follow-decode image))
               (marker (adoption-retire-expected-marker (getf state :body-crc))))
          (ok (equalp marker (pg::physical-adoption-retire-marker-image (getf state :body-crc))))
          (loop for count from 1 to (length marker) do
            (let* ((bytes (concatenate '(vector (unsigned-byte 8)) image (subseq marker 0 count)))
                   (decoded (pg::physical-adoption-follow-decode bytes))
                   (complete (= count (length marker))))
              (ok (= last (getf decoded :step)))
              (ok (not (getf decoded :partial)))
              (ok (eq complete (getf decoded :retiring)))
              (ok (eq (not complete) (getf decoded :retirement-partial)))
              (ok (= (getf decoded :valid-length) (if complete (length bytes) (length image))))))
          (loop for step below last do
            (ok (intake-fails-p
                 (lambda ()
                   (pg::physical-adoption-follow-decode
                    (concatenate '(vector (unsigned-byte 8))
                                 (pg::physical-adoption-follow-image plan step) marker))))))))))

(deftest adoption-retire-marker-rejects-corruption-duplicates-and-extra-progress
    (let* ((plan (adoption-follow-codec-fixture :distance 0))
           (last (length (pg::physical-adoption-follow-moves plan)))
           (image (pg::physical-adoption-follow-image plan last))
           (crc (getf (pg::physical-adoption-follow-decode image) :body-crc))
           (marker (adoption-retire-expected-marker crc)))
      (dolist (suffix (list (adoption-retire-expected-marker (logxor 1 crc))
                            (concatenate '(vector (unsigned-byte 8)) marker marker)
                            (concatenate '(vector (unsigned-byte 8)) marker
                                         (pg::physical-adoption-install-step-image crc (1+ last)))
                            (concatenate '(vector (unsigned-byte 8)) marker (pg::string->bytes "x"))))
        (ok (intake-fails-p
             (lambda ()
               (pg::physical-adoption-follow-decode
                (concatenate '(vector (unsigned-byte 8)) image suffix))))))))

(cl-spec:defproperty adoption-retire-marker-preserves-the-completed-prefix
    ((distance (range integer 0 1)) (generation (range integer 1 3)) (cut (range integer 0 20)))
  (:about pg::physical-adoption-follow-decode)
  (:trials (:normal 100))
  (let* ((plan (adoption-follow-codec-fixture :distance (* 64 distance) :generations generation))
         (last (length (pg::physical-adoption-follow-moves plan)))
         (image (pg::physical-adoption-follow-image plan last))
         (crc (getf (pg::physical-adoption-follow-decode image) :body-crc))
         (marker (adoption-retire-expected-marker crc))
         (count (mod cut (1+ (length marker))))
         (state (pg::physical-adoption-follow-decode
                 (concatenate '(vector (unsigned-byte 8)) image (subseq marker 0 count)))))
    (and (= last (getf state :step))
         (not (getf state :partial))
         (eq (= count (length marker)) (getf state :retiring))
         (eq (< 0 count (length marker)) (getf state :retirement-partial)))))

(deftest generated-adoption-retire-marker-prefixes
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'adoption-retire-marker-preserves-the-completed-prefix
                                         :seed 85 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

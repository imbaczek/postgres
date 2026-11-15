(in-package #:pg)

(defun unit-call-with-lane-cursors (specifications function)
  (labels ((open-next (remaining cursors)
             (if (null remaining)
                 (funcall function (nreverse cursors))
                 (destructuring-bind (start end records) (first remaining)
                   (call-with-logical-wal-record-provider
                    (lambda (position consume)
                      (let ((raw (cdr (assoc position records))))
                        (unless raw (error "test lane has no record"))
                        (funcall consume raw position 1)))
                    (constantly t)
                    (lambda (provider)
                      (call-with-logical-wal-cursor
                       (timeline-parse-history 1 "") :timeline start end 1 :prefix-end (constantly t)
                       (lambda (cursor) (open-next (rest remaining) (cons cursor cursors)))
                       :record-reader provider)))))))
    (open-next specifications nil)))

(defun unit-composed-lane-fixture (start)
  (let ((specifications nil) (all nil) (position start))
    (dotimes (lane 3)
      (let* ((batch (wal-encode-record +wal-record-batch-start+ 0 position
                                       (make-array 0 :element-type '(unsigned-byte 8))))
             (terminal-start (+ position (length batch)))
             (terminal (wal-encode-record +wal-record-commit+ (1+ lane) terminal-start
                                          (wal-encode-commit-timestamp 0)))
             (end (+ terminal-start (length terminal)))
             (records (list (cons position batch) (cons terminal-start terminal))))
        (push (list position end records) specifications)
        (setf all (append all (mapcar #'cdr records)) position end)))
    (values (nreverse specifications) all position)))

(cl-spec:defproperty composed-lanes-preserve-all-records-at-both-seams
    ((start (range integer 1 4096)))
  (:about call-with-logical-wal-lane-provider)
  (:trials (:normal 200))
  (multiple-value-bind (specifications expected end) (unit-composed-lane-fixture start)
    (unit-call-with-lane-cursors
     specifications
     (lambda (cursors)
       (call-with-logical-wal-lane-provider
        cursors (constantly t)
        (lambda (provider)
          (call-with-logical-wal-cursor
           (timeline-parse-history 1 "") :timeline start end 1 :prefix-end (constantly t)
           (lambda (cursor)
             (let ((actual (loop
                             for values = (multiple-value-list (logical-wal-cursor-next cursor))
                             while (first values) collect (second values))))
               (and (equalp expected actual) (= end (logical-wal-cursor-position cursor))
                    (every (lambda (lane) (= (logical-wal-cursor-position lane)
                                             (logical-wal-cursor-end lane))) cursors))))
           :record-reader provider)))))))

(rove:deftest composed-lanes-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'composed-lanes-preserve-all-records-at-both-seams
                                         :seed 11501 :profile :normal))))
      (format t "~&COMPOSED-LANES status=~s trials=~s seed=11501 digest=~s~%"
              (getf result :status) (getf result :trials) (getf result :definition-digest))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest composed-lanes-reject-duplicate-gapped-and-overlapping-sources
    (multiple-value-bind (specifications expected end) (unit-composed-lane-fixture 64)
      (declare (ignore expected end))
      (unit-call-with-lane-cursors
       specifications
       (lambda (cursors)
         (rove:ok
          (handler-case
              (progn (call-with-logical-wal-lane-provider
                      (list (first cursors) (first cursors)) (constantly t) (lambda (provider) (declare (ignore provider))))
                     nil)
            (error () t))))))
  (dolist (shift '(-1 1))
    (multiple-value-bind (specifications expected end) (unit-composed-lane-fixture 64)
      (declare (ignore expected end))
      (incf (first (second specifications)) shift)
      (unit-call-with-lane-cursors
       specifications
       (lambda (cursors)
         (rove:ok
          (handler-case
              (progn (call-with-logical-wal-lane-provider
                      cursors (constantly t) (lambda (provider) (declare (ignore provider))))
                     nil)
            (error () t))))))))

(rove:deftest composed-lanes-reject-external-cursor-advance-and-expired-provider
    (let ((escaped nil) (rejected nil) (caught nil))
      (multiple-value-bind (specifications expected end) (unit-composed-lane-fixture 64)
        (declare (ignore expected))
        (setf rejected
              (handler-case
                  (progn
                    (unit-call-with-lane-cursors
                     specifications
                     (lambda (cursors)
                       (call-with-logical-wal-lane-provider
                        cursors (constantly t)
                        (lambda (provider)
                          (setf escaped provider)
                          (call-with-logical-wal-cursor
                           (timeline-parse-history 1 "") :timeline 64 end 1 :prefix-end (constantly t)
                           (lambda (cursor)
                             (logical-wal-cursor-next (second cursors))
                             (setf caught (handler-case (progn (logical-wal-cursor-next cursor) nil) (error () t)))
                             (rove:ok (= 64 (logical-wal-cursor-position cursor)))
                             (rove:ok (handler-case (progn (logical-wal-cursor-next cursor) nil) (error () t))))
                           :record-reader provider)))))
                    nil)
                (error () t))))
      (rove:ok caught)
      (rove:ok rejected)
      (rove:ok escaped)
      (sb-thread:with-mutex (*wal-lock*)
        (rove:ok (handler-case (progn (logical-wal-record-provider-check escaped) nil) (error () t))))))

(rove:deftest composed-lanes-do-not-splice-an-open-batch-across-a-seam
    (let ((caught nil) (rejected nil))
      (multiple-value-bind (specifications expected end) (unit-composed-lane-fixture 64)
        (declare (ignore expected))
        (let* ((first-lane (first specifications))
               (second-lane (second specifications))
               (commit (second (third first-lane)))
               (seam (car commit)))
          (setf (second first-lane) seam
                (third first-lane) (list (first (third first-lane)))
                (first second-lane) seam
                (third second-lane) (cons commit (third second-lane)))
          (setf rejected
                (handler-case
                    (progn
                      (unit-call-with-lane-cursors
                       specifications
                       (lambda (cursors)
                         (call-with-logical-wal-lane-provider
                          cursors (constantly t)
                          (lambda (provider)
                            (call-with-logical-wal-cursor
                             (timeline-parse-history 1 "") :timeline 64 end 1 :prefix-end (constantly t)
                             (lambda (cursor)
                               (logical-wal-cursor-next cursor)
                               (setf caught
                                     (handler-case (progn (logical-wal-cursor-next cursor) nil)
                                       (error (condition)
                                         (search "batch crosses a source lane seam" (princ-to-string condition)))))
                               (rove:ok (= seam (logical-wal-cursor-position cursor))))
                             :record-reader provider)))))
                      nil)
                  (error () t)))))
      (rove:ok caught)
      (rove:ok rejected "A caught seam error rejects the whole lane scope")))

(rove:deftest composed-lanes-cannot-return-success-after-a-caught-read-failure
    (let ((caught nil) (rejected nil))
      (multiple-value-bind (specifications expected end) (unit-composed-lane-fixture 64)
        (declare (ignore expected))
        (setf (cdr (first (third (first specifications)))) nil)
        (setf rejected
              (handler-case
                  (progn
                    (unit-call-with-lane-cursors
                     specifications
                     (lambda (cursors)
                       (call-with-logical-wal-lane-provider
                        cursors (constantly t)
                        (lambda (provider)
                          (call-with-logical-wal-cursor
                           (timeline-parse-history 1 "") :timeline 64 end 1 :prefix-end (constantly t)
                           (lambda (cursor)
                             (handler-case (logical-wal-cursor-next cursor)
                               (error () (setf caught t))))
                           :record-reader provider)))))
                    nil)
                (error () t))))
      (rove:ok caught)
      (rove:ok rejected "A caught lane read failure still rejects the enclosing source scope")))

(rove:deftest composed-lanes-reject-direct-reader-entry-before-consuming-a-lane
    (let ((caught nil) (rejected nil) (delivered nil) (unchanged nil))
      (multiple-value-bind (specifications expected end) (unit-composed-lane-fixture 64)
        (declare (ignore expected end))
        (setf rejected
              (handler-case
                  (progn
                    (unit-call-with-lane-cursors
                     specifications
                     (lambda (cursors)
                       (let ((positions (mapcar #'logical-wal-cursor-position cursors)))
                         (call-with-logical-wal-lane-provider
                          cursors (constantly t)
                          (lambda (provider)
                            (handler-case
                                (funcall (logical-wal-record-provider-reader provider) 64
                                         (lambda (&rest values) (declare (ignore values)) (setf delivered t)))
                              (error (condition)
                                (setf caught (search "requires its cursor invocation" (princ-to-string condition)))))
                            (setf unchanged (equal positions (mapcar #'logical-wal-cursor-position cursors))))))))
                    nil)
                (error () t))))
      (rove:ok caught)
      (rove:ok rejected)
      (rove:ok unchanged)
      (rove:ok (not delivered))))

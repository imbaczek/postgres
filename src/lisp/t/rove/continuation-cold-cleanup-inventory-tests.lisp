(in-package #:pg)

(declaim (ftype function logical-slot-continuation-cold-entry-base
                logical-slot-continuation-cold-read-raw-inventory))

(defun unit-continuation-cleanup-write-file (directory relative bytes)
  (let ((path (merge-pathnames relative (physical-image-fd-path directory))))
    (with-open-file (stream path :direction :output :if-exists :error
                                 :element-type '(unsigned-byte 8))
      (write-sequence bytes stream))
    (sb-posix:chmod (namestring path) #o600)
    path))

(cl-spec:defproperty continuation-cleanup-names-preserve-one-staging-suffix
    ((number (range integer 0 100000)) (staging (range integer 0 1)))
  (:about logical-slot-continuation-cold-entry-base)
  (:trials (:normal 80))
  (let* ((base (format nil "slot_~d" number))
         (name (if (zerop staging) base (concatenate 'string base ".tmp"))))
    (multiple-value-bind (actual temporary)
        (logical-slot-continuation-cold-entry-base name)
      (and (string= base actual)
           (eq (not (zerop staging)) temporary)
           (handler-case
               (progn (logical-slot-continuation-cold-entry-base
                       (concatenate 'string name ".tmp.tmp")) nil)
             (error () t))))))

(rove:deftest continuation-cleanup-raw-name-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-cleanup-names-preserve-one-staging-suffix
                                         :seed 11911 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 80 (getf result :trials)))))

(rove:deftest continuation-cleanup-raw-inventory-observes-scratch-without-eligibility
    (unit-continuation-inventory-fixture
     '(("ordinary" #(1 2 3)))
     (lambda (directory check)
       (let ((root (physical-image-fd-path directory)))
         (sb-posix:mkdir (namestring (merge-pathnames "empty.tmp/" root)) #o700)
         (sb-posix:mkdir (namestring (merge-pathnames "partial.tmp/" root)) #o700)
         (let ((scratch (unit-continuation-cleanup-write-file directory "ordinary/state.tmp" #(4 5)))
               (partial (unit-continuation-cleanup-write-file directory "partial.tmp/state.tmp" #(6))))
           (let ((before (list (physical-image-file-facts -100 (namestring scratch))
                               (physical-image-file-facts -100 (namestring partial))))
                 (fds (length (directory #p"/proc/self/fd/*"))))
             (let ((rows (logical-slot-continuation-cold-read-raw-inventory directory check)))
               (rove:ok (equal '("empty.tmp" "ordinary" "partial.tmp")
                               (mapcar (lambda (row) (getf row :name)) rows)))
               (rove:ok (null (getf (first rows) :files)))
               (rove:ok (equal '("state" "state.tmp")
                               (mapcar #'first (getf (second rows) :files))))
               (rove:ok (equalp #(1 2 3) (third (first (getf (second rows) :files)))))
               (rove:ok (equalp #(6) (third (first (getf (third rows) :files))))))
             (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
             (rove:ok (equal before
                             (list (physical-image-file-facts -100 (namestring scratch))
                                   (physical-image-file-facts -100 (namestring partial)))))
             (rove:ok (handler-case
                          (progn (logical-slot-continuation-read-slot-inventory directory check) nil)
                        (error () t)))))))))

(rove:deftest continuation-cleanup-raw-inventory-reserves-a-separate-staging-bound
    (dolist (staging '(nil t))
      (unit-continuation-inventory-fixture
       nil
       (lambda (directory check)
         (declare (ignore check))
         (let ((root (physical-image-fd-path directory))
               (suffix (if staging ".tmp/" "/")))
           (dotimes (index +logical-slot-continuation-obligation-limit+)
             (sb-posix:mkdir
              (namestring (merge-pathnames (format nil "slot_~d~a" index suffix) root)) #o700))
           (sb-posix:mkdir
            (namestring (merge-pathnames (if staging "live_slot/" "retired.tmp/") root)) #o700)
           (rove:ok (= (1+ +logical-slot-continuation-obligation-limit+)
                       (length (logical-slot-continuation-cold-raw-names directory))))
           (sb-posix:mkdir
            (namestring (merge-pathnames (concatenate 'string "overflow" suffix) root)) #o700)
           (rove:ok (handler-case
                        (progn (logical-slot-continuation-cold-raw-names directory) nil)
                      (error () t))))))))

(rove:deftest continuation-cleanup-raw-inventory-rejects-unexpected-and-linked-files
    (dolist (mode '(:unexpected :symlink :hardlink))
      (unit-continuation-inventory-fixture
       '(("ordinary" #(1 2 3)))
       (lambda (directory check)
         (let* ((root (physical-image-fd-path directory))
                (source (merge-pathnames "ordinary/state" root))
                (scratch (merge-pathnames "ordinary/state.tmp" root))
                (fds (length (directory #p"/proc/self/fd/*"))))
           (ecase mode
             (:unexpected
              (unit-continuation-cleanup-write-file directory "ordinary/extra" #(9)))
             (:symlink (sb-posix:symlink (namestring source) (namestring scratch)))
             (:hardlink (sb-posix:link (namestring source) (namestring scratch))))
           (rove:ok (handler-case
                        (progn (logical-slot-continuation-cold-read-raw-inventory directory check) nil)
                      (error () t)))
           (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
           (rove:ok (equalp #(1 2 3) (read-bounded-regular-file source 3))))))))

(rove:deftest continuation-cleanup-raw-inventory-detects-byte-identical-replacement
    (unit-continuation-inventory-fixture
     '(("ordinary" #(1 2 3)))
     (lambda (directory check)
       (let* ((scratch (unit-continuation-cleanup-write-file directory "ordinary/state.tmp" #(4 5)))
              (changed nil)
              (*logical-slot-continuation-inventory-test-hook*
                (lambda (point)
                  (when (and (eq point :between-passes) (not changed))
                    (let ((replacement
                            (unit-continuation-cleanup-write-file
                             directory "ordinary/replacement" #(4 5))))
                      (sb-posix:rename (namestring replacement) (namestring scratch))
                      (setf changed t))))))
         (rove:ok (handler-case
                      (progn (logical-slot-continuation-cold-read-raw-inventory directory check) nil)
                    (error () t)))
         (rove:ok changed)
         (rove:ok (equalp #(4 5) (read-bounded-regular-file scratch 2)))))))

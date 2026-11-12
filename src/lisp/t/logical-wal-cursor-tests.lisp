(in-package #:pg-physical-tests)

(cl-spec:defproperty logical-wal-carrier-follows-segment-last-byte
    ((segment (range integer 0 1099511627774))
     (offset (range integer 0 16777215)))
  (:about pg::logical-wal-segment-carrier)
  (:trials (:normal 100))
  (let* ((size pg::+wal-segment-size+) (fork (+ (* segment size) offset))
                                       (history (lineage-test-history fork)))
    (and (= 2 (pg::logical-wal-segment-carrier segment history))
         (or (zerop segment) (= 1 (pg::logical-wal-segment-carrier (1- segment) history)))
         (= 2 (pg::logical-wal-segment-carrier (1+ segment) history)))))

(deftest generated-logical-wal-carrier-selection
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-wal-carrier-follows-segment-last-byte :seed 745))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 100 (getf data :trials)))))

(cl-spec:defproperty logical-wal-record-length-preserves-native-bounds
    ((offset (range integer 0 16777215))
     (length (range integer 32 16777216))
     (margin (range integer -1 1)))
  (:about pg::logical-wal-check-record-length)
  (:trials (:normal 100))
  (let ((header (make-array pg::+wal-header-size+ :element-type '(unsigned-byte 8) :initial-element 0))
        (size (+ offset length)))
    (pg::wal-set-u32 header 8 length)
    (let ((result (handler-case
                      (pg::logical-wal-check-record-length header 0 offset size (+ size margin))
                    (error () nil))))
      (if (and (<= size pg::+wal-segment-size+) (not (minusp margin)))
          (eql length result)
          (null result)))))

(deftest generated-logical-wal-record-length-bounds
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-wal-record-length-preserves-native-bounds :seed 747))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 100 (getf data :trials)))))

(defun cursor-test-call (history start end function &key (owner 1) (role :prefix-end)
                                                      (guard (lambda () t)))
  (pg::call-with-logical-wal-cursor history :timeline start end owner role guard function))

(defun cursor-test-collect (cursor)
  (loop for values = (multiple-value-list (pg::logical-wal-cursor-next cursor))
        while (first values)
        collect (destructuring-bind (record bytes owner carrier kind) values
                  (list (pg::wal-record-start record) (pg::wal-record-end record)
                        owner carrier kind bytes))))

(deftest logical-wal-cursor-selects-newest-carrier-across-same-segment-forks
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (a (lineage-test-record 0))
             (b (pg::wal-encode-end-of-recovery-record 33 0 2 1 1))
             (fork (+ 33 (length b)))
             (c (pg::wal-encode-end-of-recovery-record fork 1 3 2 1))
             (end (+ fork (length c)))
             (history (list (pg::make-timeline-history-entry :timeline 3 :begin fork)
                            (pg::make-timeline-history-entry :timeline 2 :begin 33 :end fork)
                            (pg::make-timeline-history-entry :timeline 1 :begin 0 :end 33)))
             (path (lineage-test-file 3 0 a b c))
             (original (lineage-test-bytes path)))
        (lineage-test-file 1 0 a (lineage-test-record 33))
        (lineage-test-file 2 0 a b (lineage-test-record fork))
        (cursor-test-call
         history 0 end
         (lambda (cursor)
           (let ((seen (cursor-test-collect cursor)))
             (ok (equal '((1 3 :ordinary) (2 3 :end-of-recovery) (3 3 :end-of-recovery))
                        (mapcar (lambda (row) (subseq row 2 5)) seen)))
             (ok (every #'equalp (list a b c) (mapcar #'sixth seen)))
             (ok (= end (pg::logical-wal-cursor-position cursor))))))
        (ok (equalp original (lineage-test-bytes path))))))

(deftest logical-wal-cursor-never-falls-back-to-an-older-carrier
    (dolist (failure '(:missing :corrupt :short :symlink :directory :fifo))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (a (lineage-test-record 0))
               (b (pg::wal-encode-end-of-recovery-record 33 0 2 1 1))
               (end (+ 33 (length b)))
               (parent (lineage-test-file 1 0 a b))
               (child (pg::wal-segment-path 0 2 :timeline)))
          (ecase failure
            (:missing)
            (:corrupt
             (let ((bytes (concatenate '(vector (unsigned-byte 8)) a b)))
               (setf (aref bytes 32) (logxor 1 (aref bytes 32)))
               (pg::wal-atomic-write-bytes child bytes)))
            (:short (pg::wal-atomic-write-bytes child (subseq a 0 31)))
            (:symlink (sb-posix:symlink (namestring parent) (namestring child)))
            (:directory (ensure-directories-exist (uiop:ensure-directory-pathname child)))
            (:fifo (sb-posix:mkfifo (namestring child) #o600)))
          (ok (signals
               (cursor-test-call (lineage-test-history 33) 0 end
                                 (lambda (cursor) (pg::logical-wal-cursor-next cursor)))
               'error)
              (format nil "selected ~a carrier cannot fall back" failure))))))

(deftest logical-wal-cursor-preserves-prefix-and-record-start-roles-at-exact-fork
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (fork pg::+wal-segment-size+)
                                 (raw (pg::wal-encode-end-of-recovery-record fork 0 2 1 1))
                                 (history (lineage-test-history fork)))
        (lineage-test-file 2 1 raw)
        (dolist (pair '((1 :prefix-end) (2 :record-start)))
          (cursor-test-call
           history fork (+ fork (length raw))
           (lambda (cursor)
             (multiple-value-bind (record bytes owner carrier kind) (pg::logical-wal-cursor-next cursor)
               (ok (= fork (pg::wal-record-start record)))
               (ok (equalp raw bytes))
               (ok (= 2 owner carrier))
               (ok (eq :end-of-recovery kind))))
           :owner (first pair) :role (second pair)))
        (ok (signals (cursor-test-call history fork fork #'identity :owner 1 :role :record-start) 'error)))))

(deftest logical-wal-cursor-reads-ancestor-segments-and-checks-rotated-eor
    (dolist (rotated '(nil t))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (size pg::+wal-segment-size+)
                                   (first-size (- size 40))
                                   (a (lineage-test-record 0 first-size))
                                   (b (unless rotated (lineage-test-record size 96)))
                                   (fork (if rotated first-size (+ size 96)))
                                   (start (if rotated size fork))
                                   (eor (pg::wal-encode-end-of-recovery-record start 0 2 1 1))
                                   (history (lineage-test-history fork))
                                   (end (+ start (length eor))))
          (if rotated
              (progn (lineage-test-file 2 0 a) (lineage-test-file 2 1 eor))
              (progn (lineage-test-file 1 0 a) (lineage-test-file 2 1 b eor)))
          (cursor-test-call
           history 0 end
           (lambda (cursor)
             (let ((seen (cursor-test-collect cursor)))
               (ok (equal (if rotated '(2 2) '(1 2 2)) (mapcar #'fourth seen)))
               (ok (equal (if rotated '(1 2) '(1 1 2)) (mapcar #'third seen)))
               (ok (eq :end-of-recovery (fifth (car (last seen))))))))))))

(deftest logical-wal-cursor-rejects-illegal-gaps-and-empty-destinations
    (dolist (failure '(:fits :empty :fork :past-end))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (size pg::+wal-segment-size+)
                                   (a (lineage-test-record 0))
                                   (history (if (eq failure :fork) (lineage-test-history 100)
                                                (pg::timeline-parse-history 1 "")))
                                   (carrier (if (eq failure :fork) 2 1)))
          (lineage-test-file carrier 0 a)
          (lineage-test-file carrier 1
                             (if (eq failure :empty) (make-array 0 :element-type '(unsigned-byte 8))
                                 (lineage-test-record size)))
          (ok (signals
               (cursor-test-call history 33 (if (eq failure :past-end) size (+ size 33))
                                 (lambda (cursor) (pg::logical-wal-cursor-next cursor)))
               'error))))))

(deftest logical-wal-cursor-validates-eor-history-and-batch-isolation
    (dolist (failure '(:parent :child :late :batch :missing))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (a (if (eq failure :batch)
                      (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0
                                             (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0))
                      (lineage-test-record 0)))
               (fork (if (eq failure :late) 32 33))
               (child (if (eq failure :parent) 3 2))
               (raw (if (eq failure :missing) (lineage-test-record 33)
                        (pg::wal-encode-end-of-recovery-record
                         33 0 (if (member failure '(:child :parent)) 3 2)
                         (if (eq failure :parent) 2 1) 1))))
          (lineage-test-file child 0 a raw)
          (ok (signals
               (cursor-test-call (lineage-test-history fork child)
                                 (if (eq failure :late) 33 0) (+ 33 (length raw))
                                 (lambda (cursor) (cursor-test-collect cursor))
                                 :owner (if (eq failure :late) 2 1))
               'error))))))

(deftest logical-wal-cursor-enforces-frozen-horizon-and-scope-lifetime
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (a (lineage-test-record 0))
                                 (b (lineage-test-record 33)) (escaped nil))
        (lineage-test-file 1 0 a b)
        (cursor-test-call (pg::timeline-parse-history 1 "") 0 33
                          (lambda (cursor)
                            (setf escaped cursor)
                            (ok (pg::logical-wal-cursor-next cursor))
                            (ok (null (pg::logical-wal-cursor-next cursor)))))
        (ok (signals (pg::logical-wal-cursor-next escaped) 'error))
        (ok (signals
             (cursor-test-call (pg::timeline-parse-history 1 "") 0 34
                               (lambda (cursor) (cursor-test-collect cursor)))
             'error)))))

(deftest logical-wal-cursor-rechecks-guard-after-decode-before-progress
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (allowed t) (calls 0)
                                 (decode (symbol-function 'pg::wal-decode-record)))
        (lineage-test-file 1 0 (lineage-test-record 0))
        (cursor-test-call
         (pg::timeline-parse-history 1 "") 0 33
         (lambda (cursor)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::wal-decode-record)
                        (lambda (&rest arguments)
                          (assert (not (sb-thread:holding-mutex-p pg::*wal-lock*)))
                          (prog1 (apply decode arguments) (setf allowed nil))))
                  (ok (signals (pg::logical-wal-cursor-next cursor) 'error))
                  (ok (zerop (pg::logical-wal-cursor-position cursor))))
             (setf (symbol-function 'pg::wal-decode-record) decode)))
         :guard (lambda ()
                  (assert (sb-thread:holding-mutex-p pg::*wal-lock*))
                  (incf calls)
                  allowed))
        (ok (= 3 calls)))))

(deftest logical-wal-cursor-rejects-unproven-guards-and-cross-thread-use
    (let ((history (pg::timeline-parse-history 1 "")))
      (dolist (guard (list nil (lambda () nil)))
        (ok (signals (cursor-test-call history 0 0 #'identity :guard guard) 'error)))
      (dolist (segment '(-1 1099511627776 1.5 nil))
        (ok (signals (pg::logical-wal-segment-carrier segment history) 'error))))
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root))
      (lineage-test-file 1 0 (lineage-test-record 0))
      (cursor-test-call
       (pg::timeline-parse-history 1 "") 0 33
       (lambda (cursor)
         (let ((thread (sb-thread:make-thread
                        (lambda ()
                          (handler-case (pg::logical-wal-cursor-next cursor)
                            (error () :rejected))))))
           (ok (eq :rejected (sb-thread:join-thread thread)))
           (ok (signals
                (let ((pg::*wal-lock* (sb-thread:make-mutex)))
                  (pg::logical-wal-cursor-next cursor))
                'error))
           (ok (zerop (pg::logical-wal-cursor-position cursor)))
           (ok (pg::logical-wal-cursor-next cursor))))))))

(deftest logical-wal-cursor-rejects-nested-and-incomplete-batches
    (dolist (nested '(nil t))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (empty (make-array 0 :element-type '(unsigned-byte 8)))
               (a (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 empty))
               (b (if nested (pg::wal-encode-record pg::+wal-record-batch-start+ 0 32 empty) empty)))
          (lineage-test-file 1 0 a b)
          (ok (signals
               (cursor-test-call (pg::timeline-parse-history 1 "") 0 (+ (length a) (length b))
                                 (lambda (cursor) (cursor-test-collect cursor)))
               'error))))))

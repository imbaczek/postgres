(in-package #:pg-physical-tests)

(deftest btree-read-lease-early-release-preserves-nesting-and-values
    (call-with-parking-test-state
     (lambda ()
       (let* ((pg::*btree-relation-locks* (make-hash-table :test #'eql))
              (state (pg::btree-relation-lock 71)))
         (ok (equal '(10 20)
                    (multiple-value-list
                     (pg::call-with-btree-relation-read-lease
                      71
                      (lambda (outer)
                        (pg::call-with-btree-relation-read-lease
                         71
                         (lambda (inner)
                           (ok (= 2 (pg::btree-relation-lock-state-reader-count state)))
                           (ok (= 2 (gethash sb-thread:*current-thread*
                                             (pg::btree-relation-lock-state-reader-owners state))))
                           (ok (pg::release-btree-read-lease inner))
                           (ok (not (pg::release-btree-read-lease inner)))
                           (ok (= 1 (pg::btree-relation-lock-state-reader-count state)))))
                        (ok (= 1 (pg::btree-relation-lock-state-reader-count state)))
                        (ok (pg::release-btree-read-lease outer))
                        (pg::call-with-btree-relation-write-lock 71 (lambda () (values 10 20))))))))
         (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
         (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state))))))))

(deftest btree-read-lease-cannot-be-acquired-twice-or-reused-after-release
    (call-with-parking-test-state
     (lambda ()
       (let* ((pg::*btree-relation-locks* (make-hash-table :test #'eql))
              (state (pg::btree-relation-lock 75)))
         (pg::call-with-btree-relation-read-lease
          75
          (lambda (lease)
            (ok (intake-fails-p (lambda () (pg::acquire-btree-read-lease lease))))
            (ok (= 1 (pg::btree-relation-lock-state-reader-count state)))
            (pg::release-btree-read-lease lease)
            (ok (intake-fails-p (lambda () (pg::acquire-btree-read-lease lease))))))
         (ok (zerop (pg::btree-relation-lock-state-reader-count state)))))))

(deftest btree-read-lease-nonlocal-exit-releases-its-credit
    (call-with-parking-test-state
     (lambda ()
       (let* ((pg::*btree-relation-locks* (make-hash-table :test #'eql))
              (state (pg::btree-relation-lock 72)))
         (ok (eq :escaped
                 (catch 'leave
                   (pg::call-with-btree-relation-read-lease
                    72 (lambda (lease) (declare (ignore lease)) (throw 'leave :escaped))))))
         (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
         (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state))))))))

(deftest btree-read-lease-preserves-writer-reentrant-ownership
    (call-with-parking-test-state
     (lambda ()
       (let* ((pg::*btree-relation-locks* (make-hash-table :test #'eql))
              (state (pg::btree-relation-lock 73)))
         (pg::call-with-btree-relation-write-lock
          73
          (lambda ()
            (pg::call-with-btree-relation-read-lease
             73
             (lambda (lease)
               (ok (pg::btree-read-lease-writer-reentrant-p lease))
               (ok (pg::release-btree-read-lease lease))
               (ok (not (pg::release-btree-read-lease lease)))
               (ok (eq sb-thread:*current-thread*
                       (pg::btree-relation-lock-state-writer-owner state)))
               (ok (= 1 (pg::btree-relation-lock-state-writer-depth state)))
               (ok (zerop (pg::btree-relation-lock-state-reader-count state)))))))
         (ok (null (pg::btree-relation-lock-state-writer-owner state)))))))

(deftest btree-read-lease-last-release-wakes-a-writer-after-nested-reader-drains
    (call-with-parking-test-state
     (lambda ()
       (let* ((locks (make-hash-table :test #'eql))
              (pg::*btree-relation-locks* locks)
              (state (pg::btree-relation-lock 79))
              (entered (sb-thread:make-semaphore))
              (release-writer (sb-thread:make-semaphore))
              (writer nil))
         (unwind-protect
              (progn
                (pg::call-with-btree-relation-read-lease
                 79
                 (lambda (outer)
                   (pg::call-with-btree-relation-read-lease
                    79
                    (lambda (inner)
                      (setf writer
                            (parking-test-spawn
                             (lambda ()
                               (let ((pg::*btree-relation-locks* locks))
                                 (pg::call-with-btree-relation-write-lock
                                  79
                                  (lambda ()
                                    (sb-thread:signal-semaphore entered)
                                    (parking-test-wait release-writer)))))
                             "B-tree waiting writer"))
                      (ok (loop repeat 500
                                  thereis (sb-thread:with-mutex
                                              ((pg::btree-relation-lock-state-mutex state))
                                            (plusp (pg::btree-relation-lock-state-waiting-writers state)))
                                do (sleep 0.001)))
                      (pg::call-with-btree-relation-read-lease
                       79
                       (lambda (nested)
                         (declare (ignore nested))
                         (ok (= 3 (pg::btree-relation-lock-state-reader-count state)))))
                      (pg::release-btree-read-lease inner)
                      (ok (= 1 (pg::btree-relation-lock-state-reader-count state)))
                      (ok (zerop (sb-thread:semaphore-count entered)))))
                   (pg::release-btree-read-lease outer)
                   (parking-test-wait entered)
                   (ok (eq writer (pg::btree-relation-lock-state-writer-owner state)))
                   (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
                   (sb-thread:signal-semaphore release-writer)))
                (parking-test-join writer)
                (ok (null (pg::btree-relation-lock-state-writer-owner state)))
                (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state)))))
           (sb-thread:signal-semaphore release-writer))))))

(deftest btree-read-lease-rejects-release-from-another-thread
    (call-with-parking-test-state
     (lambda ()
       (let* ((pg::*btree-relation-locks* (make-hash-table :test #'eql))
              (state (pg::btree-relation-lock 74))
              (rejected nil))
         (pg::call-with-btree-relation-read-lease
          74
          (lambda (lease)
            (let ((thread (parking-test-spawn
                           (lambda ()
                             (setf rejected
                                   (intake-fails-p
                                    (lambda () (pg::release-btree-read-lease lease)))))
                           "foreign B-tree lease release")))
              (parking-test-join thread))
            (ok rejected)
            (ok (pg::btree-read-lease-active-p lease))
            (ok (= 1 (pg::btree-relation-lock-state-reader-count state)))))
         (ok (zerop (pg::btree-relation-lock-state-reader-count state)))))))

(deftest btree-read-lease-fatal-during-acquisition-does-not-charge
    (call-with-parking-test-state
     (lambda ()
       (let* ((locks (make-hash-table :test #'eql))
              (pg::*btree-relation-locks* locks)
              (state (pg::btree-relation-lock 82))
              (waiting (sb-thread:make-semaphore))
              (old-wait (symbol-function 'pg::btree-relation-condition-wait))
              (reader nil) (fatal nil) (entered nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::btree-relation-condition-wait)
                      (lambda (lock-state)
                        (sb-thread:signal-semaphore waiting)
                        (funcall old-wait lock-state)))
                (pg::call-with-btree-relation-write-lock
                 82
                 (lambda ()
                   (setf reader
                         (parking-test-spawn
                          (lambda ()
                            (let ((pg::*btree-relation-locks* locks))
                              (handler-case
                                  (pg::call-with-btree-relation-read-lease
                                   82 (lambda (lease) (declare (ignore lease)) (setf entered t)))
                                (pg::pg-error (condition)
                                  (setf fatal (pg::fatal-error-p condition))))))
                          "B-tree terminated acquiring reader"))
                   (parking-test-wait waiting)
                   (sb-thread:interrupt-thread
                    reader (lambda () (pg::ereport-fatal "57P01" "B-tree acquisition termination")))
                   (parking-test-join reader)
                   (ok fatal)
                   (ok (not entered))
                   (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
                   (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state))))
                   (ok (eq sb-thread:*current-thread*
                           (pg::btree-relation-lock-state-writer-owner state))))))
           (setf (symbol-function 'pg::btree-relation-condition-wait) old-wait))))))

(deftest btree-read-lease-deadline-during-acquisition-does-not-charge
    (call-with-parking-test-state
     (lambda ()
       (let* ((locks (make-hash-table :test #'eql))
              (pg::*btree-relation-locks* locks)
              (state (pg::btree-relation-lock 76))
              (done (sb-thread:make-semaphore))
              (timed-out nil) (entered nil))
         (pg::call-with-btree-relation-write-lock
          76
          (lambda ()
            (let ((reader
                    (parking-test-spawn
                     (lambda ()
                       (let ((pg::*btree-relation-locks* locks))
                         (handler-case
                             (sb-sys:with-deadline (:seconds 0.1)
                               (pg::call-with-btree-relation-read-lease
                                76 (lambda (lease) (declare (ignore lease)) (setf entered t))))
                           (sb-ext:timeout () (setf timed-out t))))
                       (sb-thread:signal-semaphore done))
                     "B-tree read acquisition deadline")))
              (parking-test-wait done)
              (parking-test-join reader))
            (ok timed-out)
            (ok (not entered))
            (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
            (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state))))
            (ok (eq sb-thread:*current-thread* (pg::btree-relation-lock-state-writer-owner state)))))
         (ok (null (pg::btree-relation-lock-state-writer-owner state)))))))

(deftest btree-read-lease-deadline-during-release-is-reported-after-discharge
    (call-with-parking-test-state
     (lambda ()
       (let* ((pg::*btree-relation-locks* (make-hash-table :test #'eql))
              (state (pg::btree-relation-lock 77))
              (holder-ready (sb-thread:make-semaphore))
              (release-holder (sb-thread:make-semaphore))
              (holder nil) (controller nil) (lease nil) (timed-out nil))
         (unwind-protect
              (progn
                (handler-case
                    (sb-sys:with-deadline (:seconds 0.15)
                      (pg::call-with-btree-relation-read-lease
                       77
                       (lambda (token)
                         (setf lease token
                               holder
                               (parking-test-spawn
                                (lambda ()
                                  (sb-thread:with-mutex ((pg::btree-relation-lock-state-mutex state))
                                    (sb-thread:signal-semaphore holder-ready)
                                    (parking-test-wait release-holder)))
                                "B-tree release mutex holder"))
                         (parking-test-wait holder-ready)
                         (setf controller
                               (parking-test-spawn
                                (lambda ()
                                  (sleep 0.3)
                                  (sb-thread:signal-semaphore release-holder))
                                "B-tree release deadline controller")))))
                  (sb-ext:timeout () (setf timed-out t)))
                (parking-test-join holder)
                (parking-test-join controller)
                (ok timed-out)
                (ok (not (pg::btree-read-lease-active-p lease)))
                (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
                (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state)))))
           (sb-thread:signal-semaphore release-holder))))))

(deftest btree-read-lease-fatal-body-exit-releases-before-thread-finishes
    (call-with-parking-test-state
     (lambda ()
       (let* ((locks (make-hash-table :test #'eql))
              (pg::*btree-relation-locks* locks)
              (state (pg::btree-relation-lock 78))
              (ready (sb-thread:make-semaphore))
              (release (sb-thread:make-semaphore))
              (fatal nil) (reader nil))
         (unwind-protect
              (progn
                (setf reader
                      (parking-test-spawn
                       (lambda ()
                         (let ((pg::*btree-relation-locks* locks))
                           (handler-case
                               (pg::call-with-btree-relation-read-lease
                                78
                                (lambda (lease)
                                  (declare (ignore lease))
                                  (sb-thread:signal-semaphore ready)
                                  (parking-test-wait release)))
                             (pg::pg-error (condition)
                               (setf fatal (pg::fatal-error-p condition))))))
                       "B-tree fatal reader"))
                (parking-test-wait ready)
                (sb-thread:interrupt-thread
                 reader (lambda () (pg::ereport-fatal "57P01" "B-tree lease test termination")))
                (parking-test-join reader)
                (ok fatal)
                (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
                (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state)))))
           (sb-thread:signal-semaphore release))))))

(deftest btree-read-lease-defers-fatal-while-release-waits-for-its-mutex
    (call-with-parking-test-state
     (lambda ()
       (let* ((locks (make-hash-table :test #'eql))
              (pg::*btree-relation-locks* locks)
              (state (pg::btree-relation-lock 80))
              (old-lock (symbol-function 'pg::call-with-btree-read-lease-lock))
              (start-holder (sb-thread:make-semaphore))
              (holder-ready (sb-thread:make-semaphore))
              (body-ready (sb-thread:make-semaphore))
              (start-cleanup (sb-thread:make-semaphore))
              (releasing (sb-thread:make-semaphore))
              (release-holder (sb-thread:make-semaphore))
              (finished (sb-thread:make-semaphore))
              (reader nil) (holder nil) (lease nil) (fatal nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::call-with-btree-read-lease-lock)
                      (lambda (lock-state function)
                        (when (and (eq reader sb-thread:*current-thread*) lease
                                   (pg::btree-read-lease-active-p lease))
                          (sb-thread:signal-semaphore releasing))
                        (funcall old-lock lock-state function)))
                (setf holder
                      (parking-test-spawn
                       (lambda ()
                         (parking-test-wait start-holder)
                         (sb-thread:with-mutex ((pg::btree-relation-lock-state-mutex state))
                           (sb-thread:signal-semaphore holder-ready)
                           (parking-test-wait release-holder)))
                       "B-tree fatal cleanup mutex holder"))
                (setf reader
                      (parking-test-spawn
                       (lambda ()
                         (let ((pg::*btree-relation-locks* locks))
                           (handler-case
                               (pg::call-with-btree-relation-read-lease
                                80
                                (lambda (token)
                                  (setf lease token)
                                  (sb-thread:signal-semaphore start-holder)
                                  (parking-test-wait holder-ready)
                                  (sb-thread:signal-semaphore body-ready)
                                  (parking-test-wait start-cleanup)))
                             (pg::pg-error (condition)
                               (setf fatal (pg::fatal-error-p condition)))))
                         (sb-thread:signal-semaphore finished))
                       "B-tree fatal cleanup reader"))
                (parking-test-wait body-ready)
                (sb-thread:signal-semaphore start-cleanup)
                (parking-test-wait releasing)
                (sb-thread:interrupt-thread
                 reader (lambda () (pg::ereport-fatal "57P01" "B-tree cleanup termination")))
                (ok (pg::btree-read-lease-active-p lease))
                (ok (= 1 (pg::btree-relation-lock-state-reader-count state)))
                (ok (zerop (sb-thread:semaphore-count finished)))
                (sb-thread:signal-semaphore release-holder)
                (parking-test-wait finished)
                (parking-test-join reader)
                (parking-test-join holder)
                (ok fatal)
                (ok (not (pg::btree-read-lease-active-p lease)))
                (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
                (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state)))))
           (setf (symbol-function 'pg::call-with-btree-read-lease-lock) old-lock)
           (sb-thread:signal-semaphore start-holder)
           (sb-thread:signal-semaphore start-cleanup)
           (sb-thread:signal-semaphore release-holder))))))

(deftest btree-read-lease-repeated-fatal-at-cleanup-entry-does-not-strand-credit
    (call-with-parking-test-state
     (lambda ()
       (let* ((locks (make-hash-table :test #'eql))
              (pg::*btree-relation-locks* locks)
              (state (pg::btree-relation-lock 81))
              (old-release (symbol-function 'pg::release-btree-read-lease))
              (body-ready (sb-thread:make-semaphore))
              (release-body (sb-thread:make-semaphore))
              (cleanup-ready (sb-thread:make-semaphore))
              (release-cleanup (sb-thread:make-semaphore))
              (reader nil) (fatal-count 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::release-btree-read-lease)
                      (lambda (lease)
                        (when (and (eq reader sb-thread:*current-thread*)
                                   (pg::btree-read-lease-active-p lease))
                          (sb-thread:signal-semaphore cleanup-ready)
                          (parking-test-wait release-cleanup))
                        (funcall old-release lease)))
                (setf reader
                      (parking-test-spawn
                       (lambda ()
                         (let ((pg::*btree-relation-locks* locks))
                           (sb-sys:without-interrupts
                             (handler-case
                                 (sb-sys:allow-with-interrupts
                                   (sb-sys:with-local-interrupts
                                     (pg::call-with-btree-relation-read-lease
                                      81
                                      (lambda (lease)
                                        (declare (ignore lease))
                                        (sb-thread:signal-semaphore body-ready)
                                        (parking-test-wait release-body)))))
                               (pg::pg-error (condition)
                                 (unless (pg::fatal-error-p condition) (error condition))))
                             (loop until (= fatal-count 2)
                                   do (handler-case
                                          (sb-sys:allow-with-interrupts
                                            (sb-sys:with-local-interrupts (sb-thread:thread-yield)))
                                        (pg::pg-error (condition)
                                          (unless (pg::fatal-error-p condition) (error condition))))))))
                       "B-tree repeatedly terminated reader"))
                (parking-test-wait body-ready)
                (sb-thread:interrupt-thread
                 reader (lambda () (incf fatal-count)
                          (pg::ereport-fatal "57P01" "B-tree first termination")))
                (parking-test-wait cleanup-ready)
                (sb-thread:interrupt-thread
                 reader (lambda () (incf fatal-count)
                          (pg::ereport-fatal "57P01" "B-tree cleanup termination")))
                (sb-thread:signal-semaphore release-cleanup)
                (parking-test-join reader)
                (ok (= 2 fatal-count))
                (ok (zerop (pg::btree-relation-lock-state-reader-count state)))
                (ok (zerop (hash-table-count (pg::btree-relation-lock-state-reader-owners state)))))
           (setf (symbol-function 'pg::release-btree-read-lease) old-release)
           (sb-thread:signal-semaphore release-body)
           (sb-thread:signal-semaphore release-cleanup))))))

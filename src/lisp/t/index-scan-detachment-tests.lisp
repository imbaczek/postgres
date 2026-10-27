(in-package #:pg-physical-tests)

(defun detachment-test-cursor (count &optional (node 901))
  (let ((key (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7)))
    (pg::make-btree-equal-cursor
     :relfilenode node :key key
     :stack (list (pg::make-btree-equal-cursor-frame
                   :level 0
                   :entries (loop for block from 1 to count collect (list key block 1))
                   :end (1- count))))))

(defun detachment-test-tail (cursor)
  (loop for tid = (pg::btree-cursor-next-tid cursor) while tid collect tid))

(defun call-with-multileaf-detachment-test-cursor (function)
  (let* ((key (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
         (pages (make-hash-table :test #'eql))
         (read (symbol-function 'pg::buf-read))
         (cursor
           (pg::make-btree-equal-cursor
            :relfilenode 901 :key key
            :stack (list
                    (pg::make-btree-equal-cursor-frame
                     :level 0 :entries (list (list key 1 1) (list key 2 1)) :end 1)
                    (pg::make-btree-equal-cursor-frame
                     :level 1 :next 1 :end 2
                     :entries (list (cons key 501) (cons key 502) (cons key 503)))))))
    (loop for block from 501 to 503
          for first from 1 by 2
          do (setf (gethash block pages)
                   (pg::btree-page-build
                    (list (list key first 1) (list key (1+ first) 1)) nil)))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::buf-read)
                 (lambda (node block)
                   (if (= node 901) (gethash block pages) (funcall read node block))))
           (funcall function cursor pages))
      (setf (symbol-function 'pg::buf-read) read))))

(defun call-with-detachment-test-state (function)
  (call-with-parking-test-state
   (lambda ()
     (with-physical-store-directory (root)
       (let ((pg::*btree-relation-locks* (make-hash-table :test #'eql))
             (pg::*query-temp-directory* root)
             (pg::*query-temp-file-scope-active-p* nil)
             (pg::*query-temp-quota* nil))
         (funcall function root))))))

(defun call-with-detachment-test-plan (nodes function &key compiler-hook)
  (let* ((pg::*query-plan-backends* (make-hash-table :test #'eq))
         (ensure (symbol-function 'pg::btree-ensure-init-before-read))
         (relations
           (loop for node in nodes
                 collect
                 (list :table (pg::make-pg-class :oid (+ 100000 node) :relfilenode (+ 300000 node)
                                                 :name (format nil "index~d" node) :columns #())
                       :path (pg::make-select-scan-path
                              :kind :index-scan
                              :index (pg::make-pg-index :oid (+ 200000 node) :relfilenode node
                                                        :table-oid (+ 100000 node) :columns '(1))))))
         (scans (loop for node in nodes for index from 0
                      collect (list :index-scan index (+ 100000 node) (+ 200000 node)
                                    '(:bool t :type :bool) #(7))))
         (tree (if scans
                   (reduce (lambda (left right)
                             (list :nested-loop-join (list :type :inner) left right))
                           scans)
                   '(:result nil)))
         (plan (pg::%make-query-plan :physical tree)))
    (pg::register-query-plan-backend
     :detachment-test
     :executor (unless compiler-hook
                 (lambda (query registered callback)
                   (declare (ignore query registered callback))
                   (funcall function)))
     :compiler (when compiler-hook
                 (lambda (query)
                   (funcall compiler-hook query)
                   (lambda (registered callback)
                     (declare (ignore registered callback))
                     (funcall function)))))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::btree-ensure-init-before-read)
                 (lambda (node) (declare (ignore node)) nil))
           (pg::execute-query-plan plan relations nil :backend :detachment-test))
      (setf (symbol-function 'pg::btree-ensure-init-before-read) ensure))))

(defun detachment-test-domain-scan (node count callback)
  (pg::call-with-current-select-index-domain
   node
   (lambda ()
     (pg::call-with-btree-relation-read-lease
      node
      (lambda (lease)
        (pg::call-with-select-index-cursor (detachment-test-cursor count node) lease callback))))))

(cl-spec:defproperty cloned-index-cursor-preserves-unread-position
    ((size (range integer 0 60)) (prefix (range integer 0 100)))
  (:about pg::clone-btree-equal-cursor)
  (:trials (:normal 200))
  (let* ((cursor (detachment-test-cursor size))
         (skip (mod prefix (1+ size))))
    (dotimes (index skip) (pg::btree-cursor-next-tid cursor))
    (let ((tail (detachment-test-tail (pg::clone-btree-equal-cursor cursor))))
      (and (equal tail (loop for block from (1+ skip) to size collect (cons block 1)))
           (equal tail (detachment-test-tail cursor))))))

(deftest generated-cloned-index-cursor-preserves-unread-position
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'cloned-index-cursor-preserves-unread-position :seed 73))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest multileaf-index-detachment-preserves-parent-continuations-on-success-and-failure
    (dolist (mode '(:detach :seal-failure))
      (call-with-detachment-test-state
       (lambda (root)
         (call-with-multileaf-detachment-test-cursor
          (lambda (cursor pages)
            (let ((seen nil) (seal (symbol-function 'pg::query-row-spool-seal)))
              (unwind-protect
                   (progn
                     (when (eq mode :seal-failure)
                       (setf (symbol-function 'pg::query-row-spool-seal)
                             (lambda (spool) (declare (ignore spool)) (error "multi-leaf seal failure"))))
                     (pg::with-live-operation
                       (pg::call-with-btree-relation-read-lease
                        901
                        (lambda (lease)
                          (pg::call-with-select-index-cursor
                           cursor lease
                           (lambda (tid)
                             (push tid seen)
                             (when (= 1 (car tid))
                               (if (eq mode :detach)
                                   (pg::call-with-live-operation-parked
                                    (lambda () (clrhash pages)))
                                   (ok (intake-fails-p
                                        (lambda ()
                                          (pg::call-with-live-operation-parked (lambda () :unexpected)))))))
                             nil))))))
                (setf (symbol-function 'pg::query-row-spool-seal) seal))
              (ok (equal (nreverse seen) (loop for block from 1 to 6 collect (cons block 1))))
              (ok (null (uiop:directory-files root))))))))))

(deftest attached-index-cursor-remains-lazy-without-parking
    (call-with-detachment-test-state
     (lambda (root)
       (let ((cursor (detachment-test-cursor 100)) (calls 0))
         (pg::call-with-btree-relation-read-lease
          901
          (lambda (lease)
            (ok (eq :stop
                    (pg::call-with-select-index-cursor
                     cursor lease (lambda (tid) (declare (ignore tid)) (incf calls) :stop))))))
         (ok (= 1 calls))
         (ok (= 1 (pg::btree-equal-cursor-frame-next (first (pg::btree-equal-cursor-stack cursor)))))
         (ok (null (uiop:directory-files root)))))))

(deftest detached-index-cursor-resumes-once-without-a-read-lease
    (call-with-detachment-test-state
     (lambda (root)
       (let ((seen nil) (cursor (detachment-test-cursor 5)) (path nil))
         (pg::with-live-operation
           (pg::call-with-btree-relation-read-lease
            901
            (lambda (lease)
              (pg::call-with-select-index-cursor
               cursor lease
               (lambda (tid)
                 (push tid seen)
                 (when (= 2 (car tid))
                   (pg::call-with-live-operation-parked
                    (lambda ()
                      (ok (not (pg::btree-read-lease-active-p lease)))
                      (setf path (pg::query-row-spool-pathname
                                  (pg::select-index-scan-spool (first pg::*select-index-scans*))))
                      (ok (probe-file path))
                      (pg::call-with-btree-relation-write-lock 901 (lambda () :writer))))
                   (ok (= 1 pg::*live-operation-count*)))
                 nil)))))
         (ok (equal (nreverse seen) (loop for block from 1 to 5 collect (cons block 1))))
         (ok (not (probe-file path)))
         (ok (null (uiop:directory-files root)))))))

(deftest real-plan-index-lease-is-released-with-its-detached-cursor
    (call-with-detachment-test-state
     (lambda (root)
       (let ((seen nil))
         (pg::with-live-operation
           (call-with-detachment-test-plan
            '(901)
            (lambda ()
              (pg::call-with-btree-relation-read-lease
               901
               (lambda (lease)
                 (pg::call-with-select-index-cursor
                  (detachment-test-cursor 3) lease
                  (lambda (tid)
                    (push tid seen)
                    (when (= 1 (car tid))
                      (pg::call-with-live-operation-parked
                       (lambda ()
                         (ok (notany #'pg::btree-read-lease-active-p pg::*btree-read-leases*)))))
                    nil)))))))
         (ok (equal (nreverse seen) '((1 . 1) (2 . 1) (3 . 1))))
         (ok (null (uiop:directory-files root)))))))

(deftest resumed-high-index-scans-reacquire-the-full-domain-before-low-index-joins
    (call-with-detachment-test-state
     (lambda (root)
       (let ((locks pg::*btree-relation-locks*)
             (writer nil) (parks 0) (seen nil)
             (writer-entered (sb-thread:make-semaphore))
             (release-writer (sb-thread:make-semaphore)))
         (unwind-protect
              (pg::with-live-operation
                (call-with-detachment-test-plan
                 '(902 901)
                 (lambda ()
                   (detachment-test-domain-scan
                    902 3
                    (lambda (left)
                      (detachment-test-domain-scan
                       902 1
                       (lambda (middle)
                         (declare (ignore middle))
                         (ok (plusp (pg::btree-relation-lock-state-reader-count
                                     (pg::btree-relation-lock 901))))
                         (detachment-test-domain-scan
                          901 2
                          (lambda (right)
                            (push (list (car left) (car right)) seen)
                            (when (= 1 (car right))
                              (when (zerop parks)
                                (setf writer
                                      (parking-test-spawn
                                       (lambda ()
                                         (let ((pg::*btree-relation-locks* locks))
                                           (pg::call-with-btree-relation-locks
                                            '(901 902)
                                            (lambda ()
                                              (sb-thread:signal-semaphore writer-entered)
                                              (parking-test-wait release-writer)))))
                                       "ordered writer during plan parking"))
                                (ok (loop repeat 500
                                            thereis (sb-thread:with-mutex
                                                        ((pg::btree-relation-lock-state-mutex
                                                          (pg::btree-relation-lock 901)))
                                                      (plusp (pg::btree-relation-lock-state-waiting-writers
                                                              (pg::btree-relation-lock 901))))
                                          do (sleep 0.001))))
                              (pg::call-with-live-operation-parked
                               (lambda ()
                                 (incf parks)
                                 (ok (notany #'pg::btree-read-lease-active-p pg::*btree-read-leases*))
                                 (when (= parks 1)
                                   (parking-test-wait writer-entered)
                                   (sb-thread:signal-semaphore release-writer)
                                   (parking-test-join writer)))))
                            nil))
                         nil))
                      nil)))))
           (sb-thread:signal-semaphore release-writer))
         (ok (= 3 parks))
         (ok (equal (nreverse seen) '((1 1) (1 2) (2 1) (2 2) (3 1) (3 2))))
         (ok (null (uiop:directory-files root)))))))

(deftest group-release-deadlines-discharge-the-full-domain-before-reporting
    (dolist (interrupted-node '(901 902))
      (call-with-detachment-test-state
       (lambda (root)
         (let ((release (symbol-function 'pg::release-btree-read-lease))
               (fired nil) (reported nil) (order nil))
           (unwind-protect
                (pg::with-live-operation
                  (call-with-detachment-test-plan
                   '(902 901)
                   (lambda ()
                     (setf (symbol-function 'pg::release-btree-read-lease)
                           (lambda (lease)
                             (let* ((pairs (pg::select-index-lease-group-leases
                                            (first pg::*select-index-lease-groups*)))
                                    (pair (find lease pairs :key #'cdr :test #'eq)))
                               (prog1 (funcall release lease)
                                 (when pair
                                   (push (car pair) order)
                                   (when (and (= interrupted-node (car pair)) (not fired))
                                     (setf fired t)
                                     (error 'sb-ext:timeout)))))))
                     (handler-case
                         (pg::call-with-live-operation-parked (lambda () :unexpected))
                       (sb-ext:timeout () (setf reported t)))
                     (setf (symbol-function 'pg::release-btree-read-lease) release)
                     (ok reported)
                     (ok (equal (nreverse order) '(902 901)))
                     (ok (notany #'pg::btree-read-lease-active-p pg::*btree-read-leases*))
                     (ok (= 1 pg::*live-operation-count*))
                     (ok (zerop (pg::live-operation-parked-count)))
                     (let ((old (mapcar #'cdr
                                        (pg::select-index-lease-group-leases
                                         (first pg::*select-index-lease-groups*)))))
                       (pg::call-with-current-select-index-domain
                        902
                        (lambda ()
                          (let ((new (mapcar #'cdr
                                             (pg::select-index-lease-group-leases
                                              (first pg::*select-index-lease-groups*)))))
                            (ok (every #'pg::btree-read-lease-active-p new))
                            (ok (notany (lambda (lease) (member lease old :test #'eq)) new)))))))))
             (setf (symbol-function 'pg::release-btree-read-lease) release))
           (ok (null (uiop:directory-files root))))))))

(deftest failed-replacement-group-acquisition-unwinds-before-retry
    (call-with-detachment-test-state
     (lambda (root)
       (let ((acquire (symbol-function 'pg::acquire-btree-read-lease))
             (entered nil))
         (unwind-protect
              (pg::with-live-operation
                (call-with-detachment-test-plan
                 '(902 901)
                 (lambda ()
                   (pg::call-with-live-operation-parked (lambda () :resumed))
                   (setf (symbol-function 'pg::acquire-btree-read-lease)
                         (lambda (lease)
                           (prog1 (funcall acquire lease)
                             (when (eq (pg::btree-read-lease-lock-state lease)
                                       (pg::btree-relation-lock 902))
                               (error "replacement group acquisition failed")))))
                   (ok (intake-fails-p
                        (lambda ()
                          (pg::call-with-current-select-index-domain 902 (lambda () (setf entered t))))))
                   (ok (not entered))
                   (ok (notany #'pg::btree-read-lease-active-p pg::*btree-read-leases*))
                   (ok (zerop (pg::btree-relation-lock-state-reader-count (pg::btree-relation-lock 901))))
                   (ok (zerop (pg::btree-relation-lock-state-reader-count (pg::btree-relation-lock 902))))
                   (setf (symbol-function 'pg::acquire-btree-read-lease) acquire)
                   (pg::call-with-current-select-index-domain
                    902
                    (lambda ()
                      (setf entered t)
                      (ok (pg::select-index-domain-covered-p pg::*select-index-lease-domain*)))))))
           (setf (symbol-function 'pg::acquire-btree-read-lease) acquire))
         (ok entered)
         (ok (null (uiop:directory-files root)))))))

(deftest nested-plan-domain-expansion-rejects-before-index-initialization
    (call-with-detachment-test-state
     (lambda (root)
       (pg::with-live-operation
         (call-with-detachment-test-plan
          '(902 901)
          (lambda ()
            (let ((ensure (symbol-function 'pg::btree-ensure-init-before-read))
                  (initialized nil) (entered nil) (domain pg::*select-index-lease-domain*))
              (unwind-protect
                   (progn
                     (setf (symbol-function 'pg::btree-ensure-init-before-read)
                           (lambda (node) (push node initialized)))
                     (ok (intake-fails-p
                          (lambda ()
                            (pg::call-with-select-index-lease-domain '(900 901) (lambda () :unexpected)))))
                     (ok (null initialized))
                     (pg::call-with-select-index-lease-domain
                      '(901)
                      (lambda ()
                        (setf entered t)
                        (ok (eq domain pg::*select-index-lease-domain*))))
                     (ok entered)
                     (ok (null initialized)))
                (setf (symbol-function 'pg::btree-ensure-init-before-read) ensure))))))
       (ok (null (uiop:directory-files root))))))

(deftest root-plan-domain-rejects-raw-leases-before-index-initialization
    (call-with-detachment-test-state
     (lambda (root)
       (declare (ignore root))
       (let ((ensure (symbol-function 'pg::btree-ensure-init-before-read))
             (initialized nil)
             (entered nil))
         (unwind-protect
              (pg::call-with-btree-relation-read-lease
               902
               (lambda (lease)
                 (declare (ignore lease))
                 (setf (symbol-function 'pg::btree-ensure-init-before-read)
                       (lambda (node) (push node initialized)))
                 (ok (intake-fails-p
                      (lambda ()
                        (pg::call-with-select-index-lease-domain
                         '(901) (lambda () (setf entered t))))))
                 (ok (null initialized))
                 (ok (not entered))))
           (setf (symbol-function 'pg::btree-ensure-init-before-read) ensure))))))

(deftest compiler-hook-can-park-before-opening-its-first-index-cursor
    (call-with-detachment-test-state
     (lambda (root)
       (let ((compiles 0) (seen nil))
         (pg::with-live-operation
           (call-with-detachment-test-plan
            '(901)
            (lambda ()
              (detachment-test-domain-scan
               901 3 (lambda (tid) (push tid seen) nil)))
            :compiler-hook
            (lambda (plan)
              (declare (ignore plan))
              (incf compiles)
              (ok (pg::select-index-domain-covered-p pg::*select-index-lease-domain*))
              (pg::call-with-live-operation-parked
               (lambda ()
                 (ok (notany #'pg::btree-read-lease-active-p pg::*btree-read-leases*)))))))
         (ok (= 1 compiles))
         (ok (equal (nreverse seen) '((1 . 1) (2 . 1) (3 . 1))))
         (ok (null (uiop:directory-files root)))))))

(deftest parameter-expression-parking-repairs-coverage-after-evaluating-the-key-once
    (call-with-detachment-test-state
     (lambda (root)
       (let ((key (symbol-function 'pg::select-scan-index-key))
             (cursor (symbol-function 'pg::call-with-btree-equal-cursor))
             (keys 0) (cursors 0))
         (unwind-protect
              (pg::with-live-operation
                (call-with-detachment-test-plan
                 '(902 901)
                 (lambda ()
                   (setf (symbol-function 'pg::select-scan-index-key)
                         (lambda (table path environment)
                           (declare (ignore table path environment))
                           (incf keys)
                           (pg::call-with-live-operation-parked
                            (lambda ()
                              (ok (notany #'pg::btree-read-lease-active-p pg::*btree-read-leases*))))
                           #(7))
                         (symbol-function 'pg::call-with-btree-equal-cursor)
                         (lambda (node value function)
                           (declare (ignore node value function))
                           (incf cursors)
                           (ok (pg::select-index-domain-covered-p pg::*select-index-lease-domain*))
                           (ok (plusp (pg::btree-relation-lock-state-reader-count
                                       (pg::btree-relation-lock 901))))
                           (ok (plusp (pg::btree-relation-lock-state-reader-count
                                       (pg::btree-relation-lock 902))))))
                   (pg::scan-select-index
                    (pg::make-pg-class :oid 100901 :relfilenode 300901 :columns #())
                    (pg::make-select-scan-path
                     :kind :index-scan
                     :index (pg::make-pg-index :oid 200901 :relfilenode 901
                                               :table-oid 100901 :columns '(1)))
                    nil (lambda (tuple) (declare (ignore tuple)) :unexpected)))))
           (setf (symbol-function 'pg::select-scan-index-key) key
                 (symbol-function 'pg::call-with-btree-equal-cursor) cursor))
         (ok (= 1 keys))
         (ok (= 1 cursors))
         (ok (null (uiop:directory-files root)))))))

(deftest stopping-after-parking-does-not-eagerly-reacquire-plan-leases
    (call-with-detachment-test-state
     (lambda (root)
       (let ((acquire (symbol-function 'pg::call-with-btree-relation-read-leases))
             (groups 0) (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::call-with-btree-relation-read-leases)
                      (lambda (nodes function)
                        (incf groups)
                        (funcall acquire nodes function)))
                (pg::with-live-operation
                  (call-with-detachment-test-plan
                   '(901)
                   (lambda ()
                     (detachment-test-domain-scan
                      901 6
                      (lambda (tid)
                        (declare (ignore tid))
                        (incf calls)
                        (pg::call-with-live-operation-parked (lambda () :resumed))
                        :stop))))))
           (setf (symbol-function 'pg::call-with-btree-relation-read-leases) acquire))
         (ok (= 1 groups))
         (ok (= 1 calls))
         (ok (null (uiop:directory-files root)))))))

(deftest partial-group-prefix-is-repaired-in-order-with-a-writer-waiting
    (call-with-detachment-test-state
     (lambda (root)
       (let ((locks pg::*btree-relation-locks*)
             (release (symbol-function 'pg::release-btree-read-lease))
             (writer nil) (entered (sb-thread:make-semaphore))
             (release-writer (sb-thread:make-semaphore)))
         (unwind-protect
              (pg::with-live-operation
                (call-with-detachment-test-plan
                 '(903 902 901)
                 (lambda ()
                   (setf (symbol-function 'pg::release-btree-read-lease)
                         (lambda (lease)
                           (prog1 (funcall release lease)
                             (when (eq (pg::btree-read-lease-lock-state lease)
                                       (pg::btree-relation-lock 903))
                               (error "group release interrupted after its highest node")))))
                   (ok (intake-fails-p
                        (lambda () (pg::call-with-live-operation-parked (lambda () :unexpected)))))
                   (setf (symbol-function 'pg::release-btree-read-lease) release)
                   (ok (equal
                        (loop for pair in (pg::select-index-lease-group-leases
                                           (first pg::*select-index-lease-groups*))
                              when (pg::btree-read-lease-active-p (cdr pair)) collect (car pair))
                        '(902 901)))
                   (setf writer
                         (parking-test-spawn
                          (lambda ()
                            (let ((pg::*btree-relation-locks* locks))
                              (pg::call-with-btree-relation-locks
                               '(901 902 903)
                               (lambda ()
                                 (sb-thread:signal-semaphore entered)
                                 (parking-test-wait release-writer)))))
                          "writer waiting on retained group prefix"))
                   (ok (loop repeat 500
                               thereis (sb-thread:with-mutex
                                           ((pg::btree-relation-lock-state-mutex
                                             (pg::btree-relation-lock 901)))
                                         (plusp (pg::btree-relation-lock-state-waiting-writers
                                                 (pg::btree-relation-lock 901))))
                             do (sleep 0.001)))
                   (pg::call-with-current-select-index-domain
                    903
                    (lambda ()
                      (ok (pg::select-index-domain-covered-p pg::*select-index-lease-domain*))
                      (pg::call-with-live-operation-parked
                       (lambda ()
                         (ok (notany #'pg::btree-read-lease-active-p pg::*btree-read-leases*))
                         (parking-test-wait entered)
                         (sb-thread:signal-semaphore release-writer)
                         (parking-test-join writer)))
                      (pg::call-with-live-operation-parked (lambda () :second))
                      (pg::call-with-current-select-index-domain
                       901
                       (lambda ()
                         (ok (pg::select-index-domain-covered-p pg::*select-index-lease-domain*)))))))))
           (setf (symbol-function 'pg::release-btree-read-lease) release)
           (sb-thread:signal-semaphore release-writer))
         (ok (null (uiop:directory-files root)))))))

(deftest an-empty-plan-domain-still-rejects-an-unselected-index
    (call-with-detachment-test-state
     (lambda (root)
       (let ((entered nil))
         (pg::with-live-operation
           (call-with-detachment-test-plan
            nil
            (lambda ()
              (ok pg::*select-index-lease-domain*)
              (ok (null (pg::select-index-lease-domain-nodes pg::*select-index-lease-domain*)))
              (ok (= 1 (length pg::*select-index-lease-groups*)))
              (ok (intake-fails-p
                   (lambda ()
                     (pg::call-with-current-select-index-domain 901 (lambda () (setf entered t))))))
              (pg::call-with-live-operation-parked (lambda () :resumed)))))
         (ok (not entered))
         (ok (null (uiop:directory-files root)))))))

(deftest plan-group-audit-rejects-writer-reentrancy-and-duplicate-owners
    (dolist (mode '(:writer :duplicate))
      (call-with-detachment-test-state
       (lambda (root)
         (let ((execute
                 (lambda ()
                   (pg::with-live-operation
                     (call-with-detachment-test-plan
                      '(901)
                      (lambda ()
                        (detachment-test-domain-scan
                         901 2
                         (lambda (tid)
                           (declare (ignore tid))
                           (let ((pg::*select-index-lease-groups*
                                   (if (eq mode :duplicate)
                                       (cons (first pg::*select-index-lease-groups*)
                                             pg::*select-index-lease-groups*)
                                       pg::*select-index-lease-groups*)))
                             (ok (intake-fails-p
                                  (lambda ()
                                    (pg::call-with-live-operation-parked (lambda () :unexpected)))))
                             (ok (every (lambda (scan) (null (pg::select-index-scan-spool scan)))
                                        pg::*select-index-scans*)))
                           :stop))))))))
           (if (eq mode :writer)
               (pg::call-with-btree-relation-write-lock 901 execute)
               (funcall execute)))
         (ok (zerop (pg::btree-relation-lock-state-reader-count (pg::btree-relation-lock 901))))
         (ok (null (uiop:directory-files root)))))))

(deftest a-nonprefix-partial-group-rejects-reacquisition-before-opening-a-cursor
    (call-with-detachment-test-state
     (lambda (root)
       (let ((entered nil))
         (pg::with-live-operation
           (call-with-detachment-test-plan
            '(902 901)
            (lambda ()
              (let* ((pairs (pg::select-index-lease-group-leases
                             (first pg::*select-index-lease-groups*)))
                     (low (cdr (assoc 901 pairs))))
                (pg::release-btree-read-lease low)
                (ok (intake-fails-p
                     (lambda ()
                       (pg::call-with-current-select-index-domain 902 (lambda () (setf entered t))))))))))
         (ok (not entered))
         (ok (zerop (pg::btree-relation-lock-state-reader-count (pg::btree-relation-lock 901))))
         (ok (zerop (pg::btree-relation-lock-state-reader-count (pg::btree-relation-lock 902))))
         (ok (null (uiop:directory-files root)))))))

(deftest sequential-parks-do-not-restart-an-index-spool
    (call-with-detachment-test-state
     (lambda (root)
       (let ((seen nil) (parks 0) (path nil))
         (pg::with-live-operation
           (pg::call-with-btree-relation-read-lease
            901
            (lambda (lease)
              (pg::call-with-select-index-cursor
               (detachment-test-cursor 6) lease
               (lambda (tid)
                 (push tid seen)
                 (when (member (car tid) '(1 3 5))
                   (pg::call-with-live-operation-parked
                    (lambda ()
                      (incf parks)
                      (let ((current (pg::query-row-spool-pathname
                                      (pg::select-index-scan-spool (first pg::*select-index-scans*)))))
                        (if path (ok (equal path current)) (setf path current))))))
                 nil)))))
         (ok (= 3 parks))
         (ok (equal (nreverse seen) (loop for block from 1 to 6 collect (cons block 1))))
         (ok (null (uiop:directory-files root)))))))

(deftest failed-index-detachment-leaves-the-original-cursor-and-lease-valid
    (call-with-detachment-test-state
     (lambda (root)
       (let ((pg::*query-temp-file-limit-bytes* 1) (seen nil)
                                                   (cursor (detachment-test-cursor 5)))
         (pg::with-live-operation
           (pg::call-with-btree-relation-read-lease
            901
            (lambda (lease)
              (pg::call-with-select-index-cursor
               cursor lease
               (lambda (tid)
                 (push tid seen)
                 (when (= 1 (car tid))
                   (ok (intake-fails-p
                        (lambda () (pg::call-with-live-operation-parked (lambda () :unexpected)))))
                   (ok (pg::btree-read-lease-active-p lease))
                   (ok (eq cursor (pg::select-index-scan-cursor (first pg::*select-index-scans*))))
                   (ok (null (pg::select-index-scan-spool (first pg::*select-index-scans*))))
                   (ok (= 1 pg::*live-operation-count*))
                   (ok (zerop (pg::live-operation-parked-count))))
                 nil)))))
         (ok (equal (nreverse seen) (loop for block from 1 to 5 collect (cons block 1))))
         (ok (null (uiop:directory-files root)))))))

(deftest nested-same-index-scans-detach-before-each-park-without-repeating-rows
    (call-with-detachment-test-state
     (lambda (root)
       (let ((seen nil) (parks 0))
         (labels ((scan (count callback)
                    (pg::call-with-btree-relation-read-lease
                     901
                     (lambda (lease)
                       (pg::call-with-select-index-cursor (detachment-test-cursor count) lease callback)))))
           (pg::with-live-operation
             (scan 3
                   (lambda (outer)
                     (scan 2
                           (lambda (inner)
                             (push (list (car outer) (car inner)) seen)
                             (when (= 1 (car inner))
                               (pg::call-with-live-operation-parked
                                (lambda ()
                                  (incf parks)
                                  (ok (notany #'pg::btree-read-lease-active-p pg::*btree-read-leases*))
                                  (ok (every #'pg::select-index-scan-spool pg::*select-index-scans*)))))
                             nil))
                     nil))))
         (ok (= 3 parks))
         (ok (equal (nreverse seen) '((1 1) (1 2) (2 1) (2 2) (3 1) (3 2))))
         (ok (null (uiop:directory-files root)))))))

(deftest inner-detachment-remains-usable-when-the-outer-spool-fails
    (call-with-detachment-test-state
     (lambda (root)
       (let ((seal (symbol-function 'pg::query-row-spool-seal))
             (seals 0) (seen nil) (outer (detachment-test-cursor 3)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::query-row-spool-seal)
                      (lambda (spool)
                        (incf seals)
                        (if (= 2 seals) (error "outer detachment failed") (funcall seal spool))))
                (pg::with-live-operation
                  (pg::call-with-btree-relation-read-lease
                   901
                   (lambda (outer-lease)
                     (labels
                         ((visit-left (left)
                            (pg::call-with-btree-relation-read-lease
                             901
                             (lambda (inner-lease)
                               (pg::call-with-select-index-cursor
                                (detachment-test-cursor 2) inner-lease
                                (lambda (right)
                                  (push (list (car left) (car right)) seen)
                                  (when (and (= 1 (car left)) (= 1 (car right)))
                                    (ok (intake-fails-p
                                         (lambda ()
                                           (pg::call-with-live-operation-parked (lambda () :unexpected)))))
                                    (ok (not (pg::btree-read-lease-active-p inner-lease)))
                                    (ok (pg::btree-read-lease-active-p outer-lease))
                                    (ok (pg::select-index-scan-spool (first pg::*select-index-scans*)))
                                    (ok (eq outer (pg::select-index-scan-cursor
                                                   (second pg::*select-index-scans*))))
                                    (ok (zerop (pg::live-operation-parked-count))))
                                  nil))))
                            nil))
                       (pg::call-with-select-index-cursor outer outer-lease #'visit-left))))))
           (setf (symbol-function 'pg::query-row-spool-seal) seal))
         (ok (= 2 seals))
         (ok (equal (nreverse seen) '((1 1) (1 2) (2 1) (2 2) (3 1) (3 2))))
         (ok (null (uiop:directory-files root)))))))

(deftest raw-and-writer-reentrant-read-leases-reject-parking-before-detachment
    (call-with-detachment-test-state
     (lambda (root)
       (pg::with-live-operation
         (pg::call-with-btree-relation-read-lease
          901
          (lambda (lease)
            (ok (intake-fails-p
                 (lambda () (pg::call-with-live-operation-parked (lambda () :unexpected)))))
            (ok (pg::btree-read-lease-active-p lease))
            (ok (zerop (pg::live-operation-parked-count)))))
         (pg::call-with-btree-relation-write-lock
          901
          (lambda ()
            (pg::call-with-btree-relation-read-lease
             901
             (lambda (lease)
               (pg::call-with-select-index-cursor
                (detachment-test-cursor 3) lease
                (lambda (tid)
                  (declare (ignore tid))
                  (ok (intake-fails-p
                       (lambda () (pg::call-with-live-operation-parked (lambda () :unexpected)))))
                  (ok (pg::btree-read-lease-active-p lease))
                  (ok (null (pg::select-index-scan-spool (first pg::*select-index-scans*))))
                  :stop)))))))
       (ok (null (uiop:directory-files root))))))

(deftest duplicate-executor-ownership-of-a-lease-rejects-before-detachment
    (call-with-detachment-test-state
     (lambda (root)
       (pg::with-live-operation
         (pg::call-with-btree-relation-read-lease
          901
          (lambda (lease)
            (pg::call-with-select-index-cursor
             (detachment-test-cursor 2) lease
             (lambda (outer)
               (declare (ignore outer))
               (pg::call-with-select-index-cursor
                (detachment-test-cursor 2) lease
                (lambda (inner)
                  (declare (ignore inner))
                  (ok (intake-fails-p
                       (lambda () (pg::call-with-live-operation-parked (lambda () :unexpected)))))
                  (ok (pg::btree-read-lease-active-p lease))
                  (ok (every (lambda (scan) (null (pg::select-index-scan-spool scan)))
                             pg::*select-index-scans*))
                  :stop))
               :stop)))))
       (ok (null (uiop:directory-files root))))))

(deftest index-stop-after-parking-does-not-evaluate-the-detached-tail
    (call-with-detachment-test-state
     (lambda (root)
       (let ((calls 0))
         (pg::with-live-operation
           (pg::call-with-btree-relation-read-lease
            901
            (lambda (lease)
              (ok (eq :stop
                      (pg::call-with-select-index-cursor
                       (detachment-test-cursor 8) lease
                       (lambda (tid)
                         (declare (ignore tid))
                         (incf calls)
                         (pg::call-with-live-operation-parked (lambda () :resumed))
                         :stop)))))))
         (ok (= 1 calls))
         (ok (null (uiop:directory-files root)))))))

(deftest index-detachment-with-an-empty-tail-needs-no-spill-file
    (call-with-detachment-test-state
     (lambda (root)
       (let ((calls 0))
         (pg::with-live-operation
           (pg::call-with-btree-relation-read-lease
            901
            (lambda (lease)
              (pg::call-with-select-index-cursor
               (detachment-test-cursor 1) lease
               (lambda (tid)
                 (declare (ignore tid))
                 (incf calls)
                 (pg::call-with-live-operation-parked
                  (lambda ()
                    (let ((spool (pg::select-index-scan-spool (first pg::*select-index-scans*))))
                      (ok (pg::query-row-spool-sealed-p spool))
                      (ok (null (pg::query-row-spool-pathname spool))))))
                 nil)))))
         (ok (= 1 calls))
         (ok (null (uiop:directory-files root)))))))

(deftest index-seal-failure-closes-the-partial-spool-without-moving-the-cursor
    (call-with-detachment-test-state
     (lambda (root)
       (let ((seal (symbol-function 'pg::query-row-spool-seal))
             (stream nil) (seen nil) (cursor (detachment-test-cursor 4)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::query-row-spool-seal)
                      (lambda (spool)
                        (setf stream (pg::query-row-spool-output-stream spool))
                        (error "injected index spool seal failure")))
                (pg::with-live-operation
                  (pg::call-with-btree-relation-read-lease
                   901
                   (lambda (lease)
                     (pg::call-with-select-index-cursor
                      cursor lease
                      (lambda (tid)
                        (push tid seen)
                        (when (= 1 (car tid))
                          (ok (intake-fails-p
                               (lambda () (pg::call-with-live-operation-parked (lambda () :unexpected)))))
                          (ok (pg::btree-read-lease-active-p lease))
                          (ok (not (open-stream-p stream)))
                          (ok (eq cursor (pg::select-index-scan-cursor (first pg::*select-index-scans*)))))
                        nil))))))
           (setf (symbol-function 'pg::query-row-spool-seal) seal))
         (ok (equal (nreverse seen) '((1 . 1) (2 . 1) (3 . 1) (4 . 1))))
         (ok (null (uiop:directory-files root)))))))

(deftest index-detachment-spool-open-interruption-retains-stream-cleanup-ownership
    (call-with-detachment-test-state
     (lambda (root)
       (let ((open (symbol-function 'pg::open-query-temp-output))
             (stream nil) (canceled nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::open-query-temp-output)
                      (lambda ()
                        (multiple-value-bind (path opened) (funcall open)
                          (setf stream opened)
                          (sb-thread:interrupt-thread
                           sb-thread:*current-thread*
                           (lambda () (pg::ereport "57014" "index spool acquisition canceled")))
                          (values path opened))))
                (let ((thread
                        (parking-test-spawn
                         (lambda ()
                           (let ((pg::*query-temp-directory* root)
                                 (pg::*query-temp-file-scope-active-p* nil)
                                 (pg::*query-temp-quota* nil))
                             (handler-case
                                 (pg::with-query-temp-files
                                   (pg::with-query-row-spool (spool 1)
                                     (pg::query-row-spool-append spool #(1 1))))
                               (pg::pg-error (condition)
                                 (setf canceled (string= "57014" (pg::pg-error-sqlstate condition)))))))
                         "index spool acquisition cancellation")))
                  (parking-test-join thread))
                (ok canceled)
                (ok (not (open-stream-p stream)))
                (ok (null (uiop:directory-files root))))
           (setf (symbol-function 'pg::open-query-temp-output) open)
           (when (and stream (open-stream-p stream)) (ignore-errors (close stream :abort t))))))))

(deftest deadline-reported-after-lease-discharge-keeps-the-published-index-tail
    (call-with-detachment-test-state
     (lambda (root)
       (let ((release (symbol-function 'pg::release-btree-read-lease))
             (seen nil) (timed-out nil))
         (pg::with-live-operation
           (pg::call-with-btree-relation-read-lease
            901
            (lambda (lease)
              (pg::call-with-select-index-cursor
               (detachment-test-cursor 4) lease
               (lambda (tid)
                 (push tid seen)
                 (when (= 1 (car tid))
                   (unwind-protect
                        (progn
                          (setf (symbol-function 'pg::release-btree-read-lease)
                                (lambda (token)
                                  (funcall release token)
                                  (error 'sb-ext:timeout)))
                          (handler-case
                              (pg::call-with-live-operation-parked (lambda () :unexpected))
                            (sb-ext:timeout () (setf timed-out t))))
                     (setf (symbol-function 'pg::release-btree-read-lease) release))
                   (ok timed-out)
                   (ok (not (pg::btree-read-lease-active-p lease)))
                   (ok (pg::select-index-scan-spool (first pg::*select-index-scans*)))
                   (ok (zerop (pg::live-operation-parked-count))))
                 nil)))))
         (ok (equal (nreverse seen) '((1 . 1) (2 . 1) (3 . 1) (4 . 1))))
         (ok (null (uiop:directory-files root)))))))

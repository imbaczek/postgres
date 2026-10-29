(in-package #:pg-physical-tests)

(defclass resource-test-stream
    (trivial-gray-streams:fundamental-character-input-stream
     trivial-gray-streams:fundamental-character-output-stream)
  ((underlying :initarg :underlying :reader resource-test-underlying)
   (failures :initarg :failures :accessor resource-test-failures)
   (kind :initarg :kind :initform :timeout :reader resource-test-kind)
   (after-close :initarg :after-close :initform nil :reader resource-test-after-close)
   (attempts :initform 0 :accessor resource-test-attempts)))

(defmethod open-stream-p ((stream resource-test-stream))
  (open-stream-p (resource-test-underlying stream)))

(defmethod close ((stream resource-test-stream) &key abort)
  (incf (resource-test-attempts stream))
  (let ((fail (or (eq :always (resource-test-failures stream))
                  (plusp (resource-test-failures stream)))))
    (when (and fail (integerp (resource-test-failures stream)))
      (decf (resource-test-failures stream)))
    (when (or (not fail) (resource-test-after-close stream))
      (close (resource-test-underlying stream) :abort abort))
    (when fail
      (if (eq :timeout (resource-test-kind stream))
          (error 'sb-ext:timeout)
          (error "injected temporary stream close failure"))))
  t)

(defmethod trivial-gray-streams:stream-read-char ((stream resource-test-stream))
  (read-char (resource-test-underlying stream) nil :eof))

(defmethod trivial-gray-streams:stream-unread-char ((stream resource-test-stream) character)
  (unread-char character (resource-test-underlying stream)))

(defmethod trivial-gray-streams:stream-write-char ((stream resource-test-stream) character)
  (write-char character (resource-test-underlying stream)))

(defun wrap-resource-test-stream (stream &rest arguments)
  (let* ((owner pg::*query-temp-cleanup-owner*)
         (entry (find stream (pg::query-temp-cleanup-owner-entries owner)
                      :key #'pg::query-temp-stream-entry-stream :test #'eq))
         (map (pg::query-temp-quota-stream-paths pg::*query-temp-quota*))
         (path (gethash stream map))
         (wrapper (apply #'make-instance 'resource-test-stream :underlying stream arguments)))
    (unless entry (error "test stream lacks its reserved owner entry"))
    (setf (pg::query-temp-stream-entry-stream entry) wrapper)
    (when path (remhash stream map) (setf (gethash wrapper map) path))
    wrapper))

(deftest native-stream-close-timeout-before-discharge-is-retried-without-losing-ownership
    (call-with-detachment-test-state
     (lambda (root)
       (let ((wrapper nil) (*error-output* (make-string-output-stream)))
         (pg::with-query-temp-files
           (setf wrapper (wrap-resource-test-stream
                          (nth-value 1 (pg::open-query-temp-output)) :failures 1)))
         (ok (= 2 (resource-test-attempts wrapper)))
         (ok (not (open-stream-p (resource-test-underlying wrapper))))
         (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records)))
         (ok (null (uiop:directory-files root)))))))

(deftest a-timeout-after-stream-discharge-does-not-close-it-again
    (call-with-detachment-test-state
     (lambda (root)
       (let ((wrapper nil) (*error-output* (make-string-output-stream)))
         (pg::with-query-temp-files
           (setf wrapper (wrap-resource-test-stream
                          (nth-value 1 (pg::open-query-temp-output))
                          :failures 1 :after-close t)))
         (ok (= 1 (resource-test-attempts wrapper)))
         (ok (not (open-stream-p wrapper)))
         (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records)))
         (ok (null (uiop:directory-files root)))))))

(deftest pending-cleanup-is-retried-only-by-its-owning-thread
    (call-with-detachment-test-state
     (lambda (root)
       (let ((ready (sb-thread:make-semaphore))
             (resume (sb-thread:make-semaphore))
             (wrapper nil) (thread nil))
         (unwind-protect
              (progn
                (setf thread
                      (parking-test-spawn
                       (lambda ()
                         (let ((pg::*query-temp-directory* root)
                               (pg::*query-temp-file-scope-active-p* nil)
                               (pg::*query-temp-quota* nil)
                               (*error-output* (make-string-output-stream)))
                           (pg::with-query-temp-files
                             (setf wrapper
                                   (wrap-resource-test-stream
                                    (nth-value 1 (pg::open-query-temp-output)) :failures :always)))
                           (sb-thread:signal-semaphore ready)
                           (unwind-protect (parking-test-wait resume)
                             (setf (resource-test-failures wrapper) 0)
                             (pg::with-query-temp-files :same-thread-retry))))
                       "retained temp cleanup owner"))
                (parking-test-wait ready)
                (let ((attempts (resource-test-attempts wrapper)))
                  (pg::with-query-temp-files :foreign-query)
                  (ok (= attempts (resource-test-attempts wrapper)))
                  (ok (open-stream-p wrapper)))
                (sb-thread:signal-semaphore resume)
                (parking-test-join thread)
                (ok (not (open-stream-p wrapper)))
                (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records)))
                (ok (null (uiop:directory-files root))))
           (sb-thread:signal-semaphore resume)
           (when thread (parking-test-join thread)))))))

(deftest a-quota-reference-alone-does-not-authorize-closing-a-foreign-live-stream
    (call-with-detachment-test-state
     (lambda (root)
       (pg::with-query-temp-files
         (multiple-value-bind (path stream) (pg::open-query-temp-output)
           (let ((pg::*query-temp-cleanup-owner* nil))
             (pg::delete-query-temp-file path))
           (ok (open-stream-p stream))
           (ok (probe-file path))
           (ok (member path pg::*query-temp-files* :test #'equal))))
       (ok (null (uiop:directory-files root))))))

(deftest failed-registration-retains-unmapped-stream-before-and-after-path-registration
    (dolist (phase '(:before :after))
      (call-with-detachment-test-state
       (lambda (root)
         (let ((register (symbol-function 'pg::register-query-temp-file))
               (wrapper nil) (path nil) (quota nil) (original-error nil)
               (*error-output* (make-string-output-stream)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::register-query-temp-file)
                        (lambda (pathname)
                          (setf path pathname quota pg::*query-temp-quota*)
                          (when (eq phase :after) (funcall register pathname))
                          (let ((entry (first (pg::query-temp-cleanup-owner-entries pg::*query-temp-cleanup-owner*))))
                            (setf wrapper (wrap-resource-test-stream
                                           (pg::query-temp-stream-entry-stream entry) :failures :always)))
                          (error "registration rejected")))
                  (handler-case (pg::with-query-temp-files (pg::open-query-temp-output))
                    (error (condition) (setf original-error (search "registration rejected" (princ-to-string condition)))))
                  (ok original-error)
                  (ok (open-stream-p wrapper))
                  (ok (probe-file path))
                  (ok (= 1 (getf (pg::query-temp-pending-cleanup-status) :open-streams)))
                  (ok (getf (pg::query-temp-pending-cleanup-status 0) :truncated))
                  (ok (null (getf (pg::query-temp-pending-cleanup-status 0) :paths)))
                  (ok (string= "58030"
                               (handler-case
                                   (progn (pg::with-query-temp-files (pg::open-query-temp-output)) nil)
                                 (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))
                  (setf (symbol-function 'pg::register-query-temp-file) register
                        (resource-test-failures wrapper) 0)
                  (pg::with-query-temp-files :retry)
                  (ok (not (open-stream-p wrapper)))
                  (ok (not (probe-file path)))
                  (ok (zerop (pg::query-temp-quota-used-bytes quota)))
                  (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records)))
                  (ok (null (uiop:directory-files root))))
             (setf (symbol-function 'pg::register-query-temp-file) register)
             (when wrapper (setf (resource-test-failures wrapper) 0))
             (pg::retry-pending-query-temp-cleanup)))))))

(deftest spill-reader-close-failure-retains-the-reader-and-original-nonlocal-return
    (call-with-detachment-test-state
     (lambda (root)
       (let ((open (symbol-function 'pg::open-query-temp-input))
             (wrapper nil) (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::open-query-temp-input)
                      (lambda (pathname)
                        (setf wrapper (wrap-resource-test-stream (funcall open pathname) :failures :always))))
                (ok (eq :original
                        (catch 'reader-return
                          (pg::with-query-temp-files
                            (pg::with-query-row-spool (spool 1)
                              (pg::query-row-spool-append spool #(1))
                              (pg::query-row-spool-seal spool)
                              (pg::map-query-row-spool spool
                                                       (lambda (row index)
                                                         (declare (ignore row index))
                                                         (throw 'reader-return :original))))))))
                (ok (open-stream-p wrapper))
                (ok (= 1 (getf (pg::query-temp-pending-cleanup-status) :open-streams)))
                (setf (resource-test-failures wrapper) 0
                      (symbol-function 'pg::open-query-temp-input) open)
                (pg::with-query-temp-files :retry)
                (ok (not (open-stream-p wrapper)))
                (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records)))
                (ok (null (uiop:directory-files root))))
           (setf (symbol-function 'pg::open-query-temp-input) open)
           (when wrapper (setf (resource-test-failures wrapper) 0))
           (pg::retry-pending-query-temp-cleanup))))))

(deftest unmapped-pending-reader-retains-another-scopes-file-and-shared-quota-charge
    (call-with-detachment-test-state
     (lambda (root)
       (let ((register (symbol-function 'pg::register-query-temp-stream-path))
             (wrapper nil) (path nil) (quota nil)
             (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (pg::with-query-temp-files
                  (multiple-value-bind (pathname output) (pg::open-query-temp-output)
                    (setf path pathname quota pg::*query-temp-quota*)
                    (pg::query-spill-write-record output #(1 2 3))
                    (close output))
                  (setf (symbol-function 'pg::register-query-temp-stream-path)
                        (lambda (stream pathname)
                          (declare (ignore pathname))
                          (setf wrapper (wrap-resource-test-stream stream :failures :always))
                          (error "reader stream mapping rejected")))
                  (let ((pg::*query-temp-file-scope-active-p* nil))
                    (ok (intake-fails-p
                         (lambda () (pg::with-query-temp-files (pg::open-query-temp-input path))))))
                  (setf (symbol-function 'pg::register-query-temp-stream-path) register))
                (ok (open-stream-p wrapper))
                (ok (probe-file path))
                (ok (plusp (pg::query-temp-quota-used-bytes quota)))
                (setf (resource-test-failures wrapper) 0)
                (pg::with-query-temp-files :first-retry)
                (pg::with-query-temp-files :second-retry)
                (ok (not (open-stream-p wrapper)))
                (ok (not (probe-file path)))
                (ok (zerop (pg::query-temp-quota-used-bytes quota)))
                (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records)))
                (ok (null (uiop:directory-files root))))
           (setf (symbol-function 'pg::register-query-temp-stream-path) register)
           (when wrapper (setf (resource-test-failures wrapper) 0))
           (pg::retry-pending-query-temp-cleanup)
           (pg::retry-pending-query-temp-cleanup))))))

(deftest normal-spill-close-errors-fail-the-query-and-still-release-the-stream
    (call-with-detachment-test-state
     (lambda (root)
       (let ((open (symbol-function 'pg::open-query-temp-output))
             (wrapper nil) (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::open-query-temp-output)
                      (lambda ()
                        (multiple-value-bind (path stream) (funcall open)
                          (setf wrapper (wrap-resource-test-stream stream :failures 1 :kind :error))
                          (values path wrapper))))
                (ok (intake-fails-p
                     (lambda ()
                       (pg::with-query-temp-files
                         (pg::with-query-temp-output (path output)
                           (write-char #\a output))))))
                (ok (not (open-stream-p wrapper)))
                (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records)))
                (ok (null (uiop:directory-files root))))
           (setf (symbol-function 'pg::open-query-temp-output) open))))))

(deftest original-error-and-fatal-survive-persistent-cleanup-failures
    (dolist (fatal '(nil t))
      (call-with-detachment-test-state
       (lambda (root)
         (declare (ignore root))
         (let ((wrapper nil) (caught nil) (*error-output* (make-string-output-stream)))
           (unwind-protect
                (progn
                  (handler-case
                      (pg::with-query-temp-files
                        (setf wrapper (wrap-resource-test-stream
                                       (nth-value 1 (pg::open-query-temp-output)) :failures :always))
                        (if fatal
                            (pg::ereport-fatal "57P01" "original body failure")
                            (pg::ereport "22000" "original body failure")))
                    (pg::pg-error (condition) (setf caught condition)))
                  (ok caught)
                  (ok (string= "original body failure" (pg::pg-error-message caught)))
                  (ok (string= (if fatal "FATAL" "ERROR") (pg::pg-error-severity caught)))
                  (ok (string= (if fatal "57P01" "22000") (pg::pg-error-sqlstate caught)))
                  (ok (open-stream-p wrapper))
                  (setf (resource-test-failures wrapper) 0)
                  (pg::with-query-temp-files :retry)
                  (ok (not (open-stream-p wrapper)))
                  (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records))))
             (when wrapper (setf (resource-test-failures wrapper) 0))
             (pg::retry-pending-query-temp-cleanup)))))))

(defun check-interrupted-spill-open-cleanup (opener function &optional (skip-opens 0))
  (call-with-detachment-test-state
   (lambda (root)
     (let ((original (symbol-function opener))
           (streams nil)
           (canceled nil))
       (unwind-protect
            (progn
              (setf (symbol-function opener)
                    (lambda ()
                      (multiple-value-bind (path stream) (funcall original)
                        (push stream streams)
                        (if (plusp skip-opens)
                            (decf skip-opens)
                            (sb-thread:interrupt-thread
                             sb-thread:*current-thread*
                             (lambda () (pg::ereport "57014" "spill ownership interrupted"))))
                        (values path stream))))
              (parking-test-join
               (parking-test-spawn
                (lambda ()
                  (let ((pg::*query-temp-directory* root)
                        (pg::*query-temp-file-scope-active-p* nil)
                        (pg::*query-temp-quota* nil)
                        (pg::*query-work-memory-bytes* 1))
                    (handler-case
                        (pg::with-query-temp-files (funcall function))
                      (pg::pg-error (condition)
                        (setf canceled (string= "57014" (pg::pg-error-sqlstate condition)))))))
                "spill acquisition cancellation"))
              (ok canceled)
              (ok streams)
              (ok (every (lambda (stream) (not (open-stream-p stream))) streams))
              (ok (null (uiop:directory-files root))))
         (setf (symbol-function opener) original)
         (dolist (stream streams)
           (when (open-stream-p stream) (ignore-errors (close stream :abort t)))))))))

(deftest query-temp-scope-closes-a-stream-interrupted-before-consumer-publication
    (check-interrupted-spill-open-cleanup
     'pg::open-query-temp-output (lambda () (pg::open-query-temp-output))))

(deftest join-match-map-open-interruption-retains-scope-cleanup-ownership
    (check-interrupted-spill-open-cleanup
     'pg::open-query-temp-io
     (lambda ()
       (pg::with-query-match-map (map 1)
         (pg::query-match-map-append map)
         (pg::query-match-map-append map)))))

(deftest aggregate-spill-open-interruption-retains-scope-cleanup-ownership
    (check-interrupted-spill-open-cleanup
     'pg::open-query-temp-output
     (lambda ()
       (pg::stream-hash-aggregate
        (lambda (callback) (funcall callback #(1)) (funcall callback #(2)))
        '((:col 1 :type :int4 :typmod nil)) 1
        (lambda (row) (declare (ignore row)) nil)))))

(deftest aggregate-partition-open-interruption-retains-scope-cleanup-ownership
    (check-interrupted-spill-open-cleanup
     'pg::open-query-temp-output
     (lambda ()
       (let ((path
               (pg::with-query-temp-output (path output)
                 (pg::query-spill-write-record output #(1))
                 path)))
         (pg::hash-aggregate-partition-spill path '((:col 1 :type :int4 :typmod nil)) 1)))
     1))

(deftest query-temp-scope-owns-unclaimed-output-and-io-streams-on-normal-exit
    (call-with-detachment-test-state
     (lambda (root)
       (let ((streams nil))
         (unwind-protect
              (progn
                (pg::with-query-temp-files
                  (dolist (opener '(pg::open-query-temp-output pg::open-query-temp-io))
                    (push (nth-value 1 (funcall opener)) streams)))
                (ok (every (lambda (stream) (not (open-stream-p stream))) streams))
                (ok (null (uiop:directory-files root))))
           (dolist (stream streams)
             (when (open-stream-p stream) (ignore-errors (close stream :abort t)))))))))

(deftest nested-and-independent-temp-scopes-preserve-foreign-stream-ownership
    (call-with-detachment-test-state
     (lambda (root)
       (let ((outer nil) (inner nil))
         (unwind-protect
              (pg::with-query-temp-files
                (setf outer (nth-value 1 (pg::open-query-temp-output)))
                (pg::with-query-temp-files (ok (open-stream-p outer)))
                (ok (open-stream-p outer))
                (let ((pg::*query-temp-file-scope-active-p* nil))
                  (pg::with-query-temp-files
                    (setf inner (nth-value 1 (pg::open-query-temp-io)))
                    (ok (open-stream-p outer))))
                (ok (open-stream-p outer))
                (ok (not (open-stream-p inner))))
           (when (and outer (open-stream-p outer)) (ignore-errors (close outer :abort t)))
           (when (and inner (open-stream-p inner)) (ignore-errors (close inner :abort t))))
         (ok (null (uiop:directory-files root)))))))

(deftest spilled-full-join-retains-match-map-and-row-position-across-sequential-parks
    (call-with-detachment-test-state
     (lambda (root)
       (let ((pg::*query-work-memory-bytes* 1)
             (join '(:type :full :qual (:const t :type :bool)
                     :left-width 1 :right-width 1))
             (seen nil) (parks 0))
         (pg::with-live-operation
           (pg::stream-select-join-environments
            (lambda (callback)
              (dolist (row '(#(1) #(2))) (funcall callback row)))
            (lambda (callback)
              (dolist (row '(#(10) #(20) #(30))) (funcall callback row)))
            join
            (lambda (row)
              (push (coerce row 'list) seen)
              (pg::call-with-live-operation-parked (lambda () (incf parks)))
              nil)))
         (ok (= 6 parks))
         (ok (equal (nreverse seen) '((1 10) (1 20) (1 30) (2 10) (2 20) (2 30))))
         (ok (null (uiop:directory-files root)))))))

(deftest spilled-aggregate-transitions-retain-values-and-order-across-parks
    (call-with-detachment-test-state
     (lambda (root)
       (let ((pg::*query-work-memory-bytes* 1)
             (visits 0) (parks 0) (result nil))
         (pg::with-live-operation
           (pg::stream-select-aggregates
            (lambda (callback)
              (dolist (row '(#(3) #(1) #(2)))
                (incf visits)
                (funcall callback row)
                (pg::call-with-live-operation-parked (lambda () (incf parks)))))
            nil
            '((:kind :count)
              (:kind :sum :input (:col 1 :type :int4) :type :int8)
              (:kind :array-agg :input (:col 1 :type :int4)))
            (lambda (row)
              (pg::call-with-live-operation-parked (lambda () (incf parks)))
              (setf result row))))
         (ok (= 3 visits))
         (ok (= 4 parks))
         (ok (= (length result) 3))
         (ok (equalp (subseq result 0 2) #(3 6)))
         (ok (pg::sql-array-p (aref result 2)))
         (ok (equalp (pg::sql-array-elements (aref result 2)) #(3 1 2)))
         (ok (null (uiop:directory-files root)))))))

(deftest sorted-output-retains-spill-reader-position-across-parks
    (dolist (budget '(1 100000))
      (call-with-detachment-test-state
       (lambda (root)
         (let ((pg::*query-work-memory-bytes* budget)
               (visits 0) (parks 0) (seen nil))
           (pg::with-live-operation
             (pg::stream-select-sort
              (lambda (callback)
                (dolist (row '(#(3) #(1) #(2)))
                  (incf visits)
                  (funcall callback (cons row row))
                  (pg::call-with-live-operation-parked (lambda () (incf parks)))))
              '(((:col 1 :type :int4) . nil))
              (lambda (row)
                (push (aref (car row) 0) seen)
                (pg::call-with-live-operation-parked (lambda () (incf parks)))
                nil)))
           (ok (= 3 visits))
           (ok (= 6 parks))
           (ok (equal (nreverse seen) '(1 2 3)))
           (ok (null (uiop:directory-files root))))))))

(deftest many-sort-runs-release-closed-stream-objects-and-completed-entries-promptly
    (call-with-detachment-test-state
     (lambda (root)
       (let ((pg::*query-work-memory-bytes* 1)
             (retained-after-input 0) (retained-during-output 0)
             (entries-during-output 0) (seen nil))
         (pg::with-query-temp-files
           (pg::stream-select-sort
            (lambda (callback)
              (loop for value downfrom 150 to 1
                    for row = (vector value)
                    do (funcall callback (cons row row))
                       (setf retained-after-input
                             (max retained-after-input
                                  (count-if #'pg::query-temp-stream-entry-stream
                                            (pg::query-temp-cleanup-owner-entries pg::*query-temp-cleanup-owner*))))))
            '(((:col 1 :type :int4) . nil))
            (lambda (row)
              (let ((entries (pg::query-temp-cleanup-owner-entries pg::*query-temp-cleanup-owner*)))
                (setf retained-during-output (max retained-during-output
                                                  (count-if #'pg::query-temp-stream-entry-stream entries))
                      entries-during-output (max entries-during-output (length entries))))
              (push (aref (car row) 0) seen)))
           (ok (zerop retained-after-input))
           (ok (<= retained-during-output 1))
           (ok (<= entries-during-output 2))
           (ok (equal (nreverse seen) (loop for value from 1 to 150 collect value))))
         (ok (null (uiop:directory-files root)))))))

(deftest scope-directory-creation-interruption-cannot-escape-cleanup-ownership
    (call-with-detachment-test-state
     (lambda (root)
       (let ((create (symbol-function 'pg::create-query-temp-scope-directory))
             (directory nil) (canceled nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::create-query-temp-scope-directory)
                      (lambda (path)
                        (prog1 (funcall create path)
                          (setf directory path)
                          (sb-thread:interrupt-thread
                           sb-thread:*current-thread*
                           (lambda () (pg::ereport "57014" "directory acquisition canceled"))))))
                (parking-test-join
                 (parking-test-spawn
                  (lambda ()
                    (let ((pg::*pgdata* root)
                          (pg::*query-temp-directory* nil)
                          (pg::*query-temp-instance-directory* nil)
                          (pg::*query-temp-root-directory* nil)
                          (pg::*query-temp-file-scope-active-p* nil)
                          (pg::*query-temp-quota* nil))
                      (handler-case (pg::with-query-temp-files (pg::open-query-temp-output))
                        (pg::pg-error (condition)
                          (setf canceled (string= "57014" (pg::pg-error-sqlstate condition)))))))
                  "scope directory acquisition cancellation"))
                (ok canceled)
                (ok directory)
                (ok (not (probe-file directory)))
                (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records))))
           (setf (symbol-function 'pg::create-query-temp-scope-directory) create))))))

(deftest a-scope-directory-collision-does-not-adopt-or-delete-existing-content
    (call-with-detachment-test-state
     (lambda (root)
       (let ((create (symbol-function 'pg::create-query-temp-scope-directory))
             (pg::*pgdata* root)
             (pg::*query-temp-directory* nil)
             (pg::*query-temp-instance-directory* nil)
             (pg::*query-temp-root-directory* nil)
             (marker nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::create-query-temp-scope-directory)
                      (lambda (path)
                        (pg::query-temp-private-directory path)
                        (setf marker (merge-pathnames "existing" path))
                        (with-open-file (output marker :direction :output :if-exists :error)
                          (write-string "retain me" output))
                        (funcall create path)))
                (ok (intake-fails-p (lambda () (pg::with-query-temp-files (pg::open-query-temp-output)))))
                (ok marker)
                (ok (probe-file marker))
                (with-open-file (input marker) (ok (string= "retain me" (read-line input))))
                (ok (zerop (getf (pg::query-temp-pending-cleanup-status) :records))))
           (setf (symbol-function 'pg::create-query-temp-scope-directory) create))))))

(deftest abort-close-and-expired-deadline-release-file-and-quota-together
    (call-with-detachment-test-state
     (lambda (root)
       (pg::with-query-temp-files
         (multiple-value-bind (path stream) (pg::open-query-temp-output)
           (pg::query-spill-write-record stream #(1 2 3))
           (ok (plusp (pg::query-temp-quota-used-bytes pg::*query-temp-quota*)))
           (sb-sys:with-deadline (:seconds 0)
             (pg::delete-query-temp-file path))
           (ok (not (open-stream-p stream)))
           (ok (not (probe-file path)))
           (ok (zerop (pg::query-temp-quota-used-bytes pg::*query-temp-quota*)))
           (ok (zerop (hash-table-count
                       (pg::query-temp-quota-stream-paths pg::*query-temp-quota*))))))
       (ok (null (uiop:directory-files root))))))

(deftest one-cleanup-deadline-does-not-replace-the-original-unwind-or-skip-other-files
    (call-with-detachment-test-state
     (lambda (root)
       (let ((original (symbol-function 'pg::close-query-temp-file-streams))
             (pg::*query-temp-quota* (pg::make-query-temp-quota nil))
             (streams nil) (failed-path nil) (fired nil)
             (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::close-query-temp-file-streams)
                      (lambda (path)
                        (prog1 (funcall original path)
                          (unless fired
                            (setf fired t failed-path path)
                            (error 'sb-ext:timeout)))))
                (ok (eq :original
                        (catch 'original-unwind
                          (pg::with-query-temp-files
                            (dotimes (index 3)
                              (declare (ignorable index))
                              (push (nth-value 1 (pg::open-query-temp-output)) streams))
                            (throw 'original-unwind :original)))))
                (ok fired)
                (ok (every (lambda (stream) (not (open-stream-p stream))) streams))
                (ok (zerop (hash-table-count (pg::query-temp-quota-stream-paths pg::*query-temp-quota*))))
                (setf (symbol-function 'pg::close-query-temp-file-streams) original)
                (pg::delete-query-temp-file failed-path)
                (ok (zerop (hash-table-count (pg::query-temp-quota-stream-paths pg::*query-temp-quota*))))
                (ok (null (uiop:directory-files root))))
           (setf (symbol-function 'pg::close-query-temp-file-streams) original)
           (dolist (stream streams)
             (when (open-stream-p stream) (ignore-errors (close stream :abort t)))))))))

(deftest a-transient-timeout-before-close-does-not-orphan-a-stream-at-scope-exit
    (call-with-detachment-test-state
     (lambda (root)
       (let ((original (symbol-function 'pg::close-query-temp-file-streams))
             (pg::*query-temp-quota* (pg::make-query-temp-quota nil))
             (streams nil) (fired nil)
             (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::close-query-temp-file-streams)
                      (lambda (path)
                        (if fired
                            (funcall original path)
                            (progn (setf fired t) (error 'sb-ext:timeout)))))
                (ok (eq :original
                        (catch 'original-unwind
                          (pg::with-query-temp-files
                            (dotimes (index 3)
                              (declare (ignorable index))
                              (push (nth-value 1 (pg::open-query-temp-output)) streams))
                            (throw 'original-unwind :original)))))
                (ok fired)
                (ok (every (lambda (stream) (not (open-stream-p stream))) streams))
                (ok (zerop (hash-table-count (pg::query-temp-quota-stream-paths pg::*query-temp-quota*))))
                (ok (null (uiop:directory-files root))))
           (setf (symbol-function 'pg::close-query-temp-file-streams) original)
           (dolist (stream streams)
             (when (open-stream-p stream) (ignore-errors (close stream :abort t)))))))))

(deftest failed-unlink-retains-quota-and-private-namespace-for-a-later-retry
    (call-with-detachment-test-state
     (lambda (root)
       (let ((unlink (symbol-function 'pg::unlink-query-temp-file))
             (pg::*pgdata* root)
             (pg::*query-temp-directory* nil)
             (pg::*query-temp-instance-directory* nil)
             (pg::*query-temp-root-directory* nil)
             (pg::*query-temp-quota* (pg::make-query-temp-quota nil))
             (path nil) (directory nil) (charge nil)
             (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::unlink-query-temp-file)
                      (lambda (pathname) (declare (ignore pathname)) (error "unlink denied")))
                (pg::with-query-temp-files
                  (multiple-value-bind (pathname stream) (pg::open-query-temp-output)
                    (setf path pathname directory pg::*query-temp-scope-directory*)
                    (pg::query-spill-write-record stream #(10 20 30))
                    (close stream)
                    (setf charge (pg::query-temp-quota-used-bytes pg::*query-temp-quota*))))
                (ok (probe-file path))
                (ok (probe-file directory))
                (ok (plusp charge))
                (ok (= charge (pg::query-temp-quota-used-bytes pg::*query-temp-quota*)))
                (setf (symbol-function 'pg::unlink-query-temp-file) unlink)
                (pg::delete-query-temp-file path)
                (ok (not (probe-file path)))
                (ok (zerop (pg::query-temp-quota-used-bytes pg::*query-temp-quota*))))
           (setf (symbol-function 'pg::unlink-query-temp-file) unlink))))))

(deftest a-new-scope-retries-failed-cleanup-without-an-externally-retained-quota
    (call-with-detachment-test-state
     (lambda (root)
       (let ((unlink (symbol-function 'pg::unlink-query-temp-file))
             (path nil) (quota nil)
             (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::unlink-query-temp-file)
                      (lambda (pathname) (declare (ignore pathname)) (error "unlink denied")))
                (pg::with-query-temp-files
                  (multiple-value-bind (pathname stream) (pg::open-query-temp-output)
                    (setf path pathname quota pg::*query-temp-quota*)
                    (pg::query-spill-write-record stream #(1 2 3))
                    (close stream)))
                (ok (probe-file path))
                (setf (symbol-function 'pg::unlink-query-temp-file) unlink)
                (pg::with-query-temp-files :next-query)
                (ok (not (probe-file path)))
                (ok (zerop (pg::query-temp-quota-used-bytes quota)))
                (ok (null (uiop:directory-files root))))
           (setf (symbol-function 'pg::unlink-query-temp-file) unlink))))))

(deftest a-new-scope-retries-an-open-stream-retained-after-persistent-close-failure
    (call-with-detachment-test-state
     (lambda (root)
       (let ((original (symbol-function 'pg::close-query-temp-file-streams))
             (path nil) (stream nil)
             (*error-output* (make-string-output-stream)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::close-query-temp-file-streams)
                      (lambda (pathname) (declare (ignore pathname)) (error 'sb-ext:timeout)))
                (pg::with-query-temp-files
                  (multiple-value-setq (path stream) (pg::open-query-temp-output)))
                (ok (open-stream-p stream))
                (setf (symbol-function 'pg::close-query-temp-file-streams) original)
                (pg::with-query-temp-files :next-query)
                (ok (not (open-stream-p stream)))
                (ok (not (probe-file path)))
                (ok (null (uiop:directory-files root))))
           (setf (symbol-function 'pg::close-query-temp-file-streams) original)
           (when (and stream (open-stream-p stream)) (ignore-errors (close stream :abort t))))))))

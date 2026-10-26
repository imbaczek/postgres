;;;; spill-tests.lisp -- executor row-spool and external-sort invariants.

(in-package #:pg)

(declaim (ftype function make-query-spill-test-directory
                query-spill-test-directory-empty-p))

(defun test-hash-aggregate-partitioned-spill ()
  (let* ((directory (make-query-spill-test-directory))
         (rows (loop for index below 120
                     collect (vector (if (zerop (mod index 17)) +null+
                                         (mod index 23)))))
         (expected (make-hash-table :test #'equal))
         (actual (make-hash-table :test #'equal))
         (group-key '(:col 1 :type :int4)))
    (dolist (row rows)
      (incf (gethash (sql-group-value-key (aref row 0) :int4) expected 0)))
    (unwind-protect
         (let ((*query-temp-directory* directory)
               (*query-work-memory-bytes* 180))
           (stream-hash-aggregate
            (lambda (callback) (dolist (row rows) (funcall callback row)))
            (list group-key) 1
            (lambda (row)
              (setf (gethash (sql-group-value-key (aref row 0) :int4) actual)
                    (aref row 1))))
           (check (and (= (hash-table-count actual) (hash-table-count expected))
                       (loop for key being the hash-keys of expected
                             always (= (gethash key actual -1)
                                       (gethash key expected))))
                  "partitioned hash aggregation preserves duplicate and NULL groups")
           (check (query-spill-test-directory-empty-p directory)
                  "partitioned hash aggregation removes all spill files"))
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-hash-aggregate-sort-fallback-and-scalar-empty-input ()
  (let* ((directory (make-query-spill-test-directory))
         (rows (loop for value below 24 collect (vector value)))
         (actual (make-hash-table :test #'equal))
         (group-key '(:col 1 :type :int4)))
    (unwind-protect
         (let ((*query-temp-directory* directory)
               (*query-work-memory-bytes* 1)
               (+hash-aggregate-partition-count+ 1))
           (stream-hash-aggregate
            (lambda (callback) (dolist (row rows) (funcall callback row)))
            (list group-key) 1
            (lambda (row)
              (setf (gethash (aref row 0) actual) (aref row 1))))
           (check (and (= (hash-table-count actual) (length rows))
                       (loop for value below (length rows)
                             always (= (gethash value actual -1) 1)))
                  "external-sort fallback emits each spilled group exactly once")
           (check (query-spill-test-directory-empty-p directory)
                  "sort fallback removes spill and merge files")
           (let ((scalar-rows nil))
             (stream-hash-aggregate (lambda (callback)
                                      (declare (ignore callback)))
                                    nil 1
                                    (lambda (row) (push (aref row 0) scalar-rows)))
             (check (equal scalar-rows '(0))
                    "empty scalar COUNT(*) emits one zero-count row")))
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun make-query-spill-test-directory ()
  (let ((directory
          (merge-pathnames
           (format nil "pg-query-spill-test-~a-~a/"
                   (sb-posix:getpid) (random 1000000))
           (uiop:temporary-directory))))
    (ensure-directories-exist (merge-pathnames "sentinel" directory))
    directory))

(defun query-spill-test-directory-empty-p (directory)
  (null (directory (merge-pathnames "*.*" directory))))

(defun test-query-temp-startup-removes-abandoned-namespaces ()
  (let* ((pid (sb-posix:getpid))
         (start-token (query-temp-process-start-token pid))
         (pid-namespace-token (query-temp-pid-namespace-token))
         (pgdata (make-query-spill-test-directory))
         (root (merge-pathnames "pgsql_tmp/" pgdata))
         (abandoned (merge-pathnames
                     (format nil "pgsql_tmp-99999999-1-~d/session-9-query-1/"
                             pid-namespace-token)
                     root))
         (live (merge-pathnames
                (format nil "pgsql_tmp-~d-~d-~d/session-9-query-1/"
                        pid start-token pid-namespace-token)
                root))
         (reused-pid (merge-pathnames
                      (format nil "pgsql_tmp-~d-~d-~d/session-9-query-2/"
                              pid (1+ start-token) pid-namespace-token)
                      root))
         (unknown-owner (merge-pathnames
                         (format nil "pgsql_tmp-~d-~d-0/session-9-query-3/"
                                 pid start-token)
                         root))
         (legacy (merge-pathnames
                  (format nil "pgsql_tmp-~d/session-9-query-4/" pid) root))
         (malformed-pid (merge-pathnames
                         (format nil
                                 "pgsql_tmp-999999999999999999999-1-~d/session-9-query-5/"
                                 pid-namespace-token)
                         root))
         (preserved (merge-pathnames "keep/" root))
         (instance nil)
         (pathname nil)
         (*query-temp-directory* nil)
         (*query-temp-root-directory* nil)
         (*query-temp-instance-directory* nil)
         (*query-temp-session-id* 41))
    (unwind-protect
         (progn
           (ensure-directories-exist (merge-pathnames "spill.tmp" abandoned))
           (ensure-directories-exist (merge-pathnames "spill.tmp" live))
           (ensure-directories-exist (merge-pathnames "spill.tmp" reused-pid))
           (ensure-directories-exist (merge-pathnames "spill.tmp" unknown-owner))
           (ensure-directories-exist (merge-pathnames "spill.tmp" legacy))
           (ensure-directories-exist (merge-pathnames "spill.tmp" malformed-pid))
           (ensure-directories-exist (merge-pathnames "keep.tmp" preserved))
           (remove-abandoned-query-temp-namespaces
            root (1+ pid))
           (check (probe-file live)
                  "startup cleanup preserves spill namespaces owned by live servers")
           (check (not (probe-file reused-pid))
                  "startup cleanup detects a reused PID from its process start token")
           (check (probe-file unknown-owner)
                  "startup cleanup preserves namespaces from an unknown PID namespace")
           (check (probe-file legacy)
                  "startup cleanup preserves legacy namespaces with unknown ownership")
           (check (probe-file malformed-pid)
                  "startup cleanup preserves namespaces with an out-of-range PID")
           (setf instance (initialize-query-temp-storage pgdata))
           (check (not (probe-file abandoned))
                  "startup cleanup removes abandoned server spill namespaces")
           (check (not (probe-file live))
                  "startup cleanup removes this process's prior spill namespace")
           (check (probe-file legacy)
                  "startup cleanup keeps legacy namespaces with unverifiable owners")
           (check (probe-file malformed-pid)
                  "startup cleanup keeps malformed owner identities without failing")
           (check (probe-file preserved)
                  "startup cleanup preserves directories outside its namespace")
           (let ((*query-temp-instance-directory* instance))
             (with-query-temp-files ()
                                    (multiple-value-bind (path stream)
                                        (open-query-temp-output)
                                      (setf pathname path)
                                      (query-spill-write-record stream "recovered")
                                      (close stream))
                                    (check (search "session-41-query-"
                                                   (namestring pathname))
                                           "spill paths include a session query namespace"))
             (check (not (probe-file pathname))
                    "completed spill scopes remove their namespaced directory")))
      (ignore-errors
       (uiop:delete-directory-tree pgdata :validate t
                                          :if-does-not-exist :ignore)))))

(defun test-query-temp-fd-scope-closes-after-error ()
  (let ((original-close (symbol-function 'query-temp-close-fd))
        (opened-fd nil)
        (closed-fd nil)
        (caught-error-p nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'query-temp-close-fd)
                 (lambda (fd)
                   (setf closed-fd fd)
                   (funcall original-close fd)))
           (handler-case
               (with-query-temp-fd (fd (sb-posix:open "/dev/null"
                                                      sb-posix:o-rdonly))
                 (setf opened-fd fd)
                 (error "intentional spill descriptor scope test error"))
             (error ()
               (setf caught-error-p t)))
           (check caught-error-p
                  "spill descriptor scope propagates a body error")
           (check (and opened-fd (eql opened-fd closed-fd))
                  "spill descriptor scope closes its descriptor after an error"))
      (unless closed-fd
        (when opened-fd
          (ignore-errors (funcall original-close opened-fd))))
      (setf (symbol-function 'query-temp-close-fd) original-close))))

(defun test-query-temp-fd-scope-closes-after-acquisition-interrupt ()
  (let ((original-close (symbol-function 'query-temp-close-fd))
        (opened-fd nil)
        (closed-fd nil)
        (interrupt-queued-p nil)
        (caught-error-p nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'query-temp-close-fd)
                 (lambda (fd)
                   (setf closed-fd fd)
                   (funcall original-close fd)))
           (handler-case
               (with-query-temp-fd
                   (fd (let ((opened (sb-posix:open "/dev/null"
                                                    sb-posix:o-rdonly)))
                         (setf opened-fd opened)
                         (sb-thread:interrupt-thread
                          sb-thread:*current-thread*
                          (lambda ()
                            (error "forced spill descriptor acquisition interrupt")))
                         (setf interrupt-queued-p t)
                         opened))
                 nil)
             (error ()
               (setf caught-error-p t)))
           (check interrupt-queued-p
                  "spill descriptor acquisition interrupt was injected")
           (check caught-error-p
                  "spill descriptor acquisition interrupt unwinds the scope")
           (check (eql closed-fd opened-fd)
                  "spill descriptor acquired before an interrupt is closed"))
      (unless closed-fd
        (when opened-fd
          (ignore-errors (funcall original-close opened-fd))))
      (setf (symbol-function 'query-temp-close-fd) original-close))))

(defun test-query-temp-fd-scope-closes-before-cleanup-interrupt ()
  (let ((original-close (symbol-function 'query-temp-close-fd))
        (opened-fd nil)
        (closed-fd nil)
        (interrupt-queued-p nil)
        (caught-error-p nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'query-temp-close-fd)
                 (lambda (fd)
                   (sb-thread:interrupt-thread
                    sb-thread:*current-thread*
                    (lambda ()
                      (error "forced spill descriptor cleanup interrupt")))
                   (setf interrupt-queued-p t)
                   (funcall original-close fd)
                   (setf closed-fd fd)))
           (handler-case
               (with-query-temp-fd (fd (sb-posix:open "/dev/null"
                                                      sb-posix:o-rdonly))
                 (setf opened-fd fd))
             (error ()
               (setf caught-error-p t)))
           (check interrupt-queued-p
                  "spill descriptor cleanup interrupt was injected")
           (check caught-error-p
                  "spill descriptor cleanup interrupt is delivered after cleanup")
           (check (eql closed-fd opened-fd)
                  "spill descriptor closes before a cleanup interrupt is delivered"))
      (unless closed-fd
        (when opened-fd
          (ignore-errors (funcall original-close opened-fd))))
      (setf (symbol-function 'query-temp-close-fd) original-close))))

(defun test-query-temp-retained-namespace-observability ()
  (let* ((pgdata (make-query-spill-test-directory))
         (root (merge-pathnames "pgsql_tmp/" pgdata))
         (namespace (merge-pathnames "pgsql_tmp-99999999-1-0/" root))
         (session (merge-pathnames "session-7-query-1/" namespace))
         (outside (merge-pathnames "outside/" pgdata))
         (text-file (merge-pathnames "spill-a.tmp" session))
         (binary-file (merge-pathnames "spill-b.tmp" session))
         (outside-file (merge-pathnames "keep.tmp" outside))
         (link (merge-pathnames "outside-link" namespace))
         (reports nil)
         (now nil)
         (instance nil)
         (startup-log nil))
    (unwind-protect
         (progn
           (ensure-directories-exist text-file)
           (ensure-directories-exist outside-file)
           (with-open-file (stream text-file :direction :output
                                             :if-exists :supersede)
             (write-string "abc" stream))
           (with-open-file (stream binary-file :direction :output
                                               :element-type '(unsigned-byte 8)
                                               :if-exists :supersede)
             (map nil (lambda (byte) (write-byte byte stream)) '(1 2 3 4 5)))
           (with-open-file (stream outside-file :direction :output
                                                :if-exists :supersede)
             (write-string "outside namespace" stream))
           (sb-posix:symlink (namestring outside) (namestring link))
           (let* ((text-mtime
                    (sb-posix:stat-mtime (sb-posix:lstat (namestring text-file))))
                  (binary-mtime
                    (sb-posix:stat-mtime (sb-posix:lstat (namestring binary-file))))
                  (oldest-mtime (min text-mtime binary-mtime)))
             (setf now (+ oldest-mtime 60)
                   reports
                   (collect-retained-query-temp-stats
                    root now (lambda (directory)
                               (declare (ignore directory))
                               :unknown)))
             (check (= (length reports) 1)
                    "spill diagnostics report retained server namespaces only")
             (let ((report (first reports)))
               (check (eq (query-temp-namespace-report-owner-state report)
                          :unknown)
                      "spill diagnostics preserve the unknown-owner classification")
               (check (and (= (query-temp-namespace-report-file-count report) 2)
                           (= (query-temp-namespace-report-size-bytes report) 8))
                      "spill diagnostics count regular files without following symlinks")
               (check (= (query-temp-namespace-report-oldest-file-age-seconds report)
                         (- now oldest-mtime))
                      "spill diagnostics report age from the oldest file modification")))
           (setf startup-log
                 (with-output-to-string (log)
                   (let ((*error-output* log))
                     (setf instance (initialize-query-temp-storage pgdata)))))
           (check (and instance (search "pgsql_tmp-" (namestring instance)))
                  "spill startup computes its new instance namespace")
           (check (and (probe-file namespace)
                       (probe-file outside-file)
                       (probe-file link))
                  "spill startup retains unknown namespaces and does not follow links")
           (check (and (search "owner=unknown" startup-log)
                       (search "files=2" startup-log)
                       (search "bytes=8" startup-log)
                       (search "oldest-file-age=" startup-log))
                  "startup logs retained spill namespace size and age"))
      (ignore-errors
       (uiop:delete-directory-tree pgdata :validate t
                                          :if-does-not-exist :ignore)))))

(defun test-query-temp-retained-scan-refuses-symlink-swap ()
  (let* ((pgdata (make-query-spill-test-directory))
         (root (merge-pathnames "pgsql_tmp/" pgdata))
         (namespace-link (merge-pathnames "pgsql_tmp-race" root))
         (namespace (uiop:ensure-directory-pathname namespace-link))
         (outside (merge-pathnames "outside/" pgdata))
         (outside-file (merge-pathnames "keep.tmp" outside))
         (original-open (symbol-function 'query-temp-open-child-directory))
         (swapped-p nil)
         (reports nil))
    (unwind-protect
         (progn
           (ensure-directories-exist (merge-pathnames "spill.tmp" namespace))
           (ensure-directories-exist outside-file)
           (with-open-file (stream outside-file :direction :output
                                                :if-exists :supersede)
             (write-string "outside namespace" stream))
           (setf (symbol-function 'query-temp-open-child-directory)
                 (lambda (fd name)
                   (when (and (not swapped-p)
                              (string= name "pgsql_tmp-race"))
                     (setf swapped-p t)
                     (uiop:delete-directory-tree namespace :validate t)
                     (sb-posix:symlink (namestring outside)
                                       (namestring namespace-link)))
                   (funcall original-open fd name)))
           (setf reports
                 (collect-retained-query-temp-stats
                  root (- (get-universal-time) 2208988800)
                  #'query-temp-directory-owner-state))
           (check swapped-p
                  "retained namespace scan test replaced the directory after enumeration")
           (check (and (= (length reports) 1)
                       (not (query-temp-namespace-report-scan-complete-p
                             (first reports)))
                       (zerop (query-temp-namespace-report-size-bytes
                               (first reports))))
                  "retained namespace scan rejects a directory swapped to a symlink")
           (check (and (probe-file namespace-link) (probe-file outside-file))
                  "retained namespace scan does not traverse the symlink target"))
      (setf (symbol-function 'query-temp-open-child-directory) original-open)
      (ignore-errors
       (uiop:delete-directory-tree pgdata :validate t
                                          :if-does-not-exist :ignore)))))

(defun test-query-temp-retained-scan-reports-file-type-race ()
  (let* ((pgdata (make-query-spill-test-directory))
         (root (merge-pathnames "pgsql_tmp/" pgdata))
         (namespace (merge-pathnames "pgsql_tmp-race-file/" root))
         (session (merge-pathnames "session-7-query-1/" namespace))
         (spill-file (merge-pathnames "spill.tmp" session))
         (original-open (symbol-function 'query-temp-open-child-file))
         (replaced-p nil)
         (reports nil))
    (unwind-protect
         (progn
           (ensure-directories-exist spill-file)
           (with-open-file (stream spill-file :direction :output
                                              :if-exists :supersede)
             (write-string "spill" stream))
           (setf (symbol-function 'query-temp-open-child-file)
                 (lambda (fd name)
                   (when (and (not replaced-p) (string= name "spill.tmp"))
                     (setf replaced-p t)
                     (delete-file spill-file)
                     (ensure-directories-exist
                      (merge-pathnames "marker"
                                       (uiop:ensure-directory-pathname spill-file))))
                   (funcall original-open fd name)))
           (setf reports
                 (collect-retained-query-temp-stats
                  root (- (get-universal-time) 2208988800)
                  (lambda (name)
                    (declare (ignore name))
                    :unknown)))
           (check replaced-p
                  "retained namespace scan test replaced a file after lstat")
           (check (and (= (length reports) 1)
                       (not (query-temp-namespace-report-scan-complete-p
                             (first reports)))
                       (zerop (query-temp-namespace-report-file-count
                               (first reports)))
                       (zerop (query-temp-namespace-report-size-bytes
                               (first reports))))
                  "retained namespace scan marks a raced file type as incomplete"))
      (setf (symbol-function 'query-temp-open-child-file) original-open)
      (ignore-errors
       (uiop:delete-directory-tree pgdata :validate t
                                          :if-does-not-exist :ignore)))))

(defun test-query-temp-file-limit-is-enforced ()
  (let* ((directory (make-query-spill-test-directory))
         (pathname nil)
         (sqlstate nil))
    (unwind-protect
         (let ((*query-temp-directory* directory)
               (*query-temp-file-limit-bytes* 0))
           (handler-case
               (with-query-temp-files ()
                                      (multiple-value-bind (path stream)
                                          (open-query-temp-output)
                                        (setf pathname path)
                                        (query-spill-write-record stream "over limit")
                                        (close stream)))
             (pg-error (condition)
               (setf sqlstate (pg-error-sqlstate condition))))
           (check (equal sqlstate "53400")
                  "temporary spill bytes honor the configured query disk limit")
           (check (and pathname (not (probe-file pathname)))
                  "temporary spill limit errors still clean up the partial file")
           (check (query-spill-test-directory-empty-p directory)
                  "temporary spill limit errors leave no artifacts"))
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-spill-resource-macros-clean-up-after-error ()
  (let ((original-row-closer (symbol-function 'close-query-row-spool))
        (original-match-closer (symbol-function 'close-query-match-map))
        (row-closed-p nil)
        (match-map-closed-p nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'close-query-row-spool)
                 (lambda (spool)
                   (declare (ignore spool))
                   (setf row-closed-p t))
                 (symbol-function 'close-query-match-map)
                 (lambda (match-map)
                   (declare (ignore match-map))
                   (setf match-map-closed-p t)))
           (handler-case
               (with-query-row-spool (spool)
                 (check (plusp (query-row-spool-memory-limit spool))
                        "row-spool body runs before an exceptional exit")
                 (error "forced row-spool body error"))
             (error () nil))
           (handler-case
               (with-query-match-map (match-map)
                 (check (plusp (query-match-map-memory-limit match-map))
                        "match-map body runs before an exceptional exit")
                 (error "forced match-map body error"))
             (error () nil))
           (check row-closed-p
                  "row-spool resource closes after an exceptional body exit")
           (check match-map-closed-p
                  "match-map resource closes after an exceptional body exit"))
      (setf (symbol-function 'close-query-row-spool) original-row-closer
            (symbol-function 'close-query-match-map) original-match-closer))))

(defun test-query-temp-file-macro-cleans-up-after-error ()
  (let* ((directory (make-query-spill-test-directory))
         (pathname nil))
    (unwind-protect
         (let ((*query-temp-directory* directory))
           (handler-case
               (with-query-temp-files ()
                                      (multiple-value-bind (path stream) (open-query-temp-output)
                                        (setf pathname path)
                                        (query-spill-write-record stream #(1))
                                        (close stream))
                                      (error "forced query spill cleanup"))
             (error () nil))
           (check (and pathname (not (probe-file pathname)))
                  "query temporary-file scope removes files after errors")
           (check (query-spill-test-directory-empty-p directory)
                  "query temporary-file scope leaves no spill artifacts"))
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-temp-open-registration-failure-cleans-up ()
  (let* ((directory (make-query-spill-test-directory))
         (pathname nil)
         (original-register (symbol-function 'register-query-temp-file)))
    (unwind-protect
         (let ((*query-temp-directory* directory))
           (with-query-temp-files ()
                                  (setf (symbol-function 'register-query-temp-file)
                                        (lambda (path)
                                          (setf pathname path)
                                          (error "forced spill registration failure")))
                                  (handler-case (open-query-temp-output)
                                    (error () nil))
                                  (setf (symbol-function 'register-query-temp-file)
                                        original-register)
                                  (check (and pathname (not (probe-file pathname)))
                                         "failed spill registration closes and removes the opened file")
                                  (check (query-spill-test-directory-empty-p directory)
                                         "failed spill registration leaves no untracked artifact")))
      (setf (symbol-function 'register-query-temp-file) original-register)
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-temp-delete-failure-retains-cleanup-registration ()
  (let* ((directory (make-query-spill-test-directory))
         (pathname nil)
         (original-unlink (symbol-function 'unlink-query-temp-file)))
    (unwind-protect
         (let ((*query-temp-directory* directory))
           (with-query-temp-files ()
                                  (multiple-value-bind (path stream) (open-query-temp-output)
                                    (setf pathname path)
                                    (close stream))
                                  (setf (symbol-function 'unlink-query-temp-file)
                                        (lambda (path)
                                          (declare (ignore path))
                                          (error "forced spill deletion failure")))
                                  (unwind-protect
                                       (delete-query-temp-file pathname)
                                    (setf (symbol-function 'unlink-query-temp-file)
                                          original-unlink))
                                  (check (member pathname *query-temp-files* :test #'equal)
                                         "failed early spill deletion retains scope cleanup tracking"))
           (check (and pathname (not (probe-file pathname)))
                  "scope cleanup retries a failed early spill deletion")
           (check (query-spill-test-directory-empty-p directory)
                  "failed early spill deletion leaves no spill artifact"))
      (setf (symbol-function 'unlink-query-temp-file) original-unlink)
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-full-join-spool-spills-typed-bytea-and-match-state ()
  (let* ((directory (make-query-spill-test-directory))
         (bytea-a (make-array 2 :element-type '(unsigned-byte 8)
                                :initial-contents '(10 11)))
         (bytea-b (make-array 3 :element-type '(unsigned-byte 8)
                                :initial-contents '(20 21 22)))
         (original-eval (symbol-function 'eval-expr))
         (emitted '()))
    (unwind-protect
         (let ((*query-work-memory-bytes* 1)
               (*query-temp-directory* directory))
           (setf (symbol-function 'eval-expr)
                 (lambda (expr environment)
                   (if (eq expr :spill-join-equal)
                       (= (aref environment 0) (aref environment 1))
                       (funcall original-eval expr environment))))
           (stream-select-join-environments
            (lambda (visit-left) (funcall visit-left #(1)))
            (lambda (visit-right)
              (dolist (row (list (vector 1 bytea-a)
                                 (vector 2 bytea-b)))
                (when (eq (funcall visit-right row) :stop)
                  (return :stop))))
            (list :type :full :qual :spill-join-equal
                  :left-width 1 :right-width 2)
            (lambda (row) (push row emitted)))
           (setf emitted (nreverse emitted))
           (check (= (length emitted) 2)
                  "spilled FULL JOIN emits one match and one unmatched inner row")
           (check (and (= (aref (first emitted) 0) 1)
                       (= (aref (first emitted) 1) 1)
                       (typep (aref (first emitted) 2)
                              '(vector (unsigned-byte 8)))
                       (equalp (aref (first emitted) 2) bytea-a))
                  "spilled FULL JOIN preserves matched BYTEA element type and bytes")
           (check (and (eq (aref (second emitted) 0) +null+)
                       (= (aref (second emitted) 1) 2)
                       (typep (aref (second emitted) 2)
                              '(vector (unsigned-byte 8)))
                       (equalp (aref (second emitted) 2) bytea-b))
                  "spilled match bitmap preserves the unmatched BYTEA row")
           (check (query-spill-test-directory-empty-p directory)
                  "FULL JOIN removes row-spool and match-map files on completion"))
      (setf (symbol-function 'eval-expr) original-eval)
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-row-spool-spills-bytea-without-expanded-list ()
  (let* ((directory (make-query-spill-test-directory))
         (byte-length 65536)
         (bytea (make-array byte-length :element-type '(unsigned-byte 8)
                                        :initial-element 255))
         (spilled-size nil)
         (restored nil))
    (unwind-protect
         (let ((*query-temp-directory* directory))
           (with-query-temp-files ()
                                  (with-query-row-spool (spool 1)
                                    (query-row-spool-append spool
                                                            (vector +null+ bytea))
                                    (query-row-spool-seal spool)
                                    (with-open-file (stream
                                                     (query-row-spool-pathname spool)
                                                     :direction :input
                                                     :external-format :utf-8)
                                      (setf spilled-size (file-length stream)))
                                    (map-query-row-spool
                                     spool
                                     (lambda (row index)
                                       (declare (ignore index))
                                       (setf restored row)))))
           (check (and spilled-size (< spilled-size (* 3 byte-length)))
                  "spilled BYTEA uses compact encoding without per-byte list expansion")
           (check (and (eq (aref restored 0) +null+)
                       (typep (aref restored 1) '(vector (unsigned-byte 8)))
                       (= (length (aref restored 1)) byte-length)
                       (equalp (aref restored 1) bytea))
                  "large spilled NULL and BYTEA retain their values and types")
           (check (query-spill-test-directory-empty-p directory)
                  "large BYTEA spool removes its file after reading"))
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-plan-external-sort-spills-stably ()
  (let* ((directory (make-query-spill-test-directory))
         (table (make-pg-class :oid 93001 :name "sort_input"
                               :relfilenode 93011 :columns #()))
         (path (make-select-scan-path :kind :seq-scan))
         (target (list :expr '(:col 2 :type :varchar :typmod -1)
                       :name "label" :type :varchar :typmod -1
                       :table-oid 93001 :attnum 2))
         (scan (list :seq-scan 0 93001))
         (order (list (cons '(:col 1 :type :int4 :typmod -1) nil)))
         (plan (%make-query-plan
                :physical (list :sort order
                                (list :project (list target) scan))))
         (registered (list (list :table table :path path)))
         (original-scan (symbol-function 'scan-select-path))
         (input (list #(2 "A") #(1 "B") #(2 "C") #(1 "D")))
         (labels '()))
    (unwind-protect
         (let ((*query-work-memory-bytes* 1)
               (*query-temp-directory* directory))
           (setf (symbol-function 'scan-select-path)
                 (lambda (received-table relfilenode received-path snapshot callback
                          &optional environment)
                   (declare (ignore relfilenode received-path snapshot environment))
                   (check (eq received-table table)
                          "external sort reads its registered scan relation")
                   (dolist (values input)
                     (when (eq (funcall callback (make-htup :values values)) :stop)
                       (return :stop)))))
           (interpret-query-plan
            plan registered
            (lambda (row) (push (aref (car row) 0) labels)))
           (setf labels (nreverse labels))
           (check (equal labels '("B" "D" "A" "C"))
                  "external merge sort preserves stable order across equal keys")
           (check (query-spill-test-directory-empty-p directory)
                  "external sort removes run files after completion")
           (setf labels nil)
           (let ((delivered 0))
             (interpret-query-plan
              plan registered
              (lambda (row)
                (incf delivered)
                (push (aref (car row) 0) labels)
                :stop))
             (check (= delivered 1)
                    "external sort stops delivering rows when its consumer stops")
             (check (equal (nreverse labels) '("B"))
                    "external sort stop returns the first stable sorted row"))
           (check (query-spill-test-directory-empty-p directory)
                  "external sort removes merged run files after early stop"))
      (setf (symbol-function 'scan-select-path) original-scan)
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

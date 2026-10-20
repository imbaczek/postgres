;;;; unit-tests.lisp -- Lisp-level unit tests for the pg server.
;;;;
;;;; Run via t/run-unit-tests.sh, which loads the "pg" system first.

(in-package #:pg)

(defvar *test-failures* 0)
(defvar *test-checks* 0)

(defmacro check (form &optional (label nil))
  `(if ,form
       (incf *test-checks*)
       (progn
         (incf *test-failures*)
         (format t "~&FAIL: ~a~%" (or ,label ',form)))))

(defun test-wait-until (predicate &optional (timeout 10.0))
  "Poll PREDICATE for up to TIMEOUT seconds (ten seconds by default)."
  (let ((deadline (+ (get-internal-real-time)
                     (* timeout internal-time-units-per-second))))
    (loop
      (when (funcall predicate)
        (return t))
      (when (>= (get-internal-real-time) deadline)
        (return nil))
      (sleep 0.01))))

(defun test-reset-live-operation-admission ()
  (sb-thread:with-mutex (*live-operation-lock*)
    (setf *live-operation-admission-open* t
          *live-operation-drain-open* nil
          *live-operation-quiescence-owner* nil
          *live-operation-drain-backends* nil
          *live-operation-cleanup-backends* nil
          *live-operation-pending-cancel-requests* nil)
    (clrhash *live-operation-backend-query-tokens*)
    (sb-thread:condition-broadcast *live-operation-cv*))
  (values))

(defun test-query-message (query)
  (let* ((query-bytes (string->bytes query))
         (payload (make-array (1+ (length query-bytes))
                              :element-type '(unsigned-byte 8)
                              :initial-element 0)))
    (replace payload query-bytes)
    (make-frontend-message #\Q payload)))

(defun test-read-frontend-message (stream)
  (multiple-value-bind (type-byte length-bytes payload)
      (read-message-frame stream)
    (when type-byte
      (decode-frontend-message type-byte length-bytes payload))))

(defun test-dispatch-query-frame (stream query)
  (let ((payload (frontend-message-payload (test-query-message query))))
    (dispatch-frontend-frame
     stream (char-code #\Q) (u32-to-bytes (+ 4 (length payload))) payload)))

(defun test-page-roundtrip ()
  (let ((p (page-init (make-page))))
    (check (= (page-lp-count p) 0) "empty page has no line pointers")
    (check (= (page-free-space p) (- +blcksz+ +size-of-page-header+))
           "empty page free space")
    (let ((item (make-array 8 :element-type '(unsigned-byte 8)
                              :initial-element 7)))
      (check (= (page-add-item p item) 1) "first item gets LP 1")
      (multiple-value-bind (off flags len) (page-get-lp p 1)
        (check (= flags +lp-normal+) "LP is normal")
        (check (= len 8) "LP length")
        (check (equalp (subseq p off (+ off 8)) item) "LP payload"))
      (check (= (page-add-item p item) 2) "second item gets LP 2")
      (check (= (page-lp-count p) 2) "two line pointers")
      (page-mark-dead p 1)
      (check (= (page-lp-count p) 2) "dead slot keeps the LP")
      (check (= (page-add-item p item :allow-reuse t) 3)
             "dead slot is not reused before index cleanup")
      (page-mark-unused p 1)
      (check (= (page-add-item p item :allow-reuse t) 1)
             "unused slot is reused when ALLOW-REUSE"))))

(defun test-atomic-output ()
  (let* ((directory (merge-pathnames "atomic-output/" *pgdata*))
         (target (merge-pathnames "control.dat" directory))
         (temporary (merge-pathnames "control.tmp" directory))
         (bytes #(0 1 127 255))
         (events '()))
    (with-atomic-output (stream target temporary directory
                                :element-type '(unsigned-byte 8)
                                :fsync-function
                                (lambda (fd)
                                  (push :file-fsync events)
                                  (sb-posix:fsync fd))
                                :rename-function
                                (lambda (from to)
                                  (push :rename events)
                                  (sb-posix:rename from to))
                                :fsync-directory-function
                                (lambda (dir)
                                  (push :directory-fsync events)
                                  (fsync-directory dir)))
      (write-sequence bytes stream))
    (check (equal (nreverse events)
                  '(:file-fsync :rename :directory-fsync))
           "atomic output fsyncs the file before rename and directory fsync")
    (with-open-file (stream target :direction :input
                                   :element-type '(unsigned-byte 8))
      (let ((written (make-array (file-length stream)
                                 :element-type '(unsigned-byte 8))))
        (read-sequence written stream)
        (check (equalp written bytes) "atomic output writes binary bytes")))
    (let ((failed-target (merge-pathnames "failed.dat" directory))
          (failed-temp (merge-pathnames "failed.tmp" directory)))
      (check (handler-case
                 (progn
                   (with-atomic-output (stream failed-target failed-temp directory
                                               :element-type '(unsigned-byte 8)
                                               :rename-function
                                               (lambda (from to)
                                                 (declare (ignore from to))
                                                 (error "injected rename failure")))
                     (write-sequence bytes stream))
                   nil)
               (error () t))
             "atomic output reports rename failures")
      (check (not (probe-file failed-temp))
             "atomic output removes its temporary file after failure"))))

(defun test-backend-wait-state ()
  (let ((backend (make-backend))
        (backend-evaluations 0)
        (value-evaluations 0))
    (multiple-value-bind (first second)
        (with-backend-wait-state
            ((progn (incf backend-evaluations) backend)
             backend-waiting-xid
             (progn (incf value-evaluations) 8123))
          (check (= (backend-waiting-xid backend) 8123)
                 "wait-state macro publishes the wait value during its body")
          (values :first :second))
      (check (and (eq first :first) (eq second :second))
             "wait-state macro preserves body values"))
    (check (and (= backend-evaluations 1) (= value-evaluations 1))
           "wait-state macro evaluates its arguments once")
    (check (null (backend-waiting-xid backend))
           "wait-state macro clears the field after normal return")
    (check (handler-case
               (progn
                 (with-backend-wait-state (backend backend-waiting-xid 8124)
                   (error "test unwind"))
                 nil)
             (error () t))
           "wait-state macro preserves errors")
    (check (null (backend-waiting-xid backend))
           "wait-state macro clears the field after an error")
    (let ((body-ran nil))
      (with-backend-wait-state (nil backend-waiting-xid 8125)
        (setf body-ran t))
      (check body-ran "wait-state macro supports a missing backend"))))

(defun test-heap-tuple-edit ()
  (let* ((relfilenode 987640)
         (block 4)
         (offnum 1)
         (current-page (page-init (make-page)))
         (tuple (make-array +size-of-heap-tuple-header+
                            :element-type '(unsigned-byte 8)
                            :initial-element 0))
         (read-count 0)
         (write-count 0)
         (dirty-count 0)
         (argument-evaluations 0))
    (page-add-item current-page tuple)
    (flet ((buf-read (rel block-number)
             (declare (ignore rel block-number))
             (incf read-count)
             current-page)
           (buf-write (rel block-number page)
             (declare (ignore rel block-number page))
             (incf write-count))
           (mark-dirty (rel)
             (declare (ignore rel))
             (incf dirty-count)))
      (let ((result
              (with-heap-tuple-edit
                  (page offset
                        (progn (incf argument-evaluations) relfilenode)
                        (progn (incf argument-evaluations) block)
                        (progn (incf argument-evaluations) offnum))
                (page-set-u32 page (+ offset +heap-tuple-xmax-off+) 4567)
                :edited)))
        (check (eq result :edited) "heap tuple edit preserves body values"))
      (check (= argument-evaluations 3)
             "heap tuple edit evaluates relation, block, and offset once")
      (check (= (page-u32 current-page
                          (+ (nth-value 0 (page-get-lp current-page offnum))
                             +heap-tuple-xmax-off+))
                4567)
             "heap tuple edit applies the tuple-header mutation")
      (check (and (= read-count 1) (= write-count 1) (= dirty-count 1))
             "heap tuple edit reads, writes, and dirties once")
      (setf current-page (page-init (make-page)))
      (page-add-item current-page
                     (make-array 5 :element-type '(unsigned-byte 8)
                                   :initial-element 0))
      (check (handler-case
                 (progn
                   (with-heap-tuple-edit (page offset relfilenode block offnum)
                     (values page offset))
                   nil)
               (error () t))
             "heap tuple edit rejects a truncated tuple header")
      (check (and (= write-count 1) (= dirty-count 1))
             "heap tuple edit does not publish a truncated tuple"))))

(defun test-with-registered-table ()
  (let* ((table (make-pg-class :oid 987641 :name "macro_table"
                               :relfilenode 987642 :columns #() :indexes nil))
         (table-evaluations 0)
         (xid-evaluations 0)
         (events '())
         (body-ran nil))
    (flet ((get-current-xid ()
             (incf xid-evaluations)
             987643)
           (register-relation-reader (relfilenode xid)
             (declare (ignore relfilenode xid))
             (push :reader events))
           (register-relation-writer (relfilenode xid)
             (declare (ignore relfilenode xid))
             (push :writer events))
           (ensure-current-table (current-table)
             (check (eq current-table table)
                    "registered-table revalidates the same table")
             (check (sb-thread:holding-mutex-p *write-lock*)
                    "registered-table revalidates under the write lock")
             (push :revalidated events)))
      (with-registered-table
          ((progn (incf table-evaluations) table) relfilenode xid :write)
        (setf body-ran t)
        (check (= relfilenode 987642) "registered-table binds its relfilenode")
        (check (= xid 987643) "registered-table binds its transaction ID"))
      (check (equal (reverse events) '(:writer :revalidated))
             "registered-table registers a writer before revalidation")
      (setf events '())
      (with-registered-table (table relfilenode xid :read)
        (declare (ignore relfilenode xid)))
      (check (equal (reverse events) '(:reader :revalidated))
             "registered-table selects the reader registration for :READ"))
    (check body-ran "registered-table executes its body")
    (check (and (= table-evaluations 1) (= xid-evaluations 2))
           "registered-table evaluates the table and XID once per use")))

(defun test-live-operation-gate ()
  (test-reset-live-operation-admission)
  (let ((test-lock (sb-thread:make-mutex :name "test live-operation lock"))
        (test-cv (sb-thread:make-waitqueue :name "test live-operation condition"))
        (backend *backend*)
        (old-state (backend-state *backend*))
        (worker-ready nil)
        (release-worker nil)
        (reload-body-ran nil)
        (reloader-done nil)
        (drain-entered nil)
        (ordinary-attempting nil)
        (ordinary-entered nil)
        (threads '()))
    (setf (backend-state backend) :in-progress)
    (labels ((wait-for-flag (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader)))))
             (start-thread (function name)
               (push (sb-thread:make-thread function :name name) threads)))
      (unwind-protect
           (progn
             (start-thread
              (lambda ()
                (with-live-operation
                  (sb-thread:with-mutex (test-lock)
                    (setf worker-ready t)
                    (sb-thread:condition-broadcast test-cv)
                    (loop until release-worker
                          do (sb-thread:condition-wait test-cv test-lock)))))
              "test live operation")
             (check (wait-for-flag (lambda () worker-ready))
                    "live operation worker starts")
             (start-thread
              (lambda ()
                (with-live-operation-quiescence
                  (sb-thread:with-mutex (test-lock)
                    (setf reload-body-ran t)))
                (sb-thread:with-mutex (test-lock)
                  (setf reloader-done t)))
              "test reload quiescence")
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "reload closes live-operation admission")
             (start-thread
              (lambda ()
                (let ((*backend* backend)
                      (*live-operation-drain-backends* (list backend)))
                  (with-live-operation-transaction-drain
                    (sb-thread:with-mutex (test-lock)
                      (setf drain-entered t)))))
              "test drain operation")
             (check (wait-for-flag (lambda () drain-entered))
                    "transaction-ending work can enter during drain")
             (start-thread
              (lambda ()
                (sb-thread:with-mutex (test-lock)
                  (setf ordinary-attempting t))
                (with-live-operation
                  (sb-thread:with-mutex (test-lock)
                    (setf ordinary-entered t))))
              "test waiting operation")
             (check (wait-for-flag (lambda () ordinary-attempting))
                    "ordinary operation attempts entry during reload")
             (sleep 0.05)
             (check (sb-thread:with-mutex (test-lock)
                      (not ordinary-entered))
                    "ordinary work waits while reload drains")
             (sb-thread:with-mutex (test-lock)
               (setf release-worker t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for-flag (lambda () reload-body-ran))
                    "reload starts after existing work leaves")
             (check (wait-for-flag (lambda () reloader-done))
                    "reload reopens live-operation admission")
             (check (wait-for-flag (lambda () ordinary-entered))
                    "waiting work enters after reload"))
        (sb-thread:with-mutex (test-lock)
          (setf release-worker t)
          (sb-thread:condition-broadcast test-cv))
        (dolist (thread threads)
          (ignore-errors (sb-thread:join-thread thread)))
        (setf (backend-state backend) old-state)
        (test-reset-live-operation-admission)))))

(defun test-live-operation-quiescence-ownership ()
  (let ((original-finish (symbol-function 'finish-live-operation-drain)))
    (unwind-protect
         (progn
           (setf (symbol-function 'finish-live-operation-drain)
                 (lambda () (error "injected drain failure")))
           (check (handler-case
                      (progn (with-live-operation-quiescence :unreachable) nil)
                    (error () t))
                  "a drain failure propagates from gate acquisition")
           (check (and *live-operation-admission-open*
                       (null *live-operation-quiescence-owner*))
                  "a failed owner releases its own gate"))
      (setf (symbol-function 'finish-live-operation-drain) original-finish))
    (unwind-protect
         (progn
           ;; SBCL condition-wait can unwind after releasing the mutex. Ensure
           ;; recovery reacquires it and only the current owner reopens the gate.
           (setf (symbol-function 'finish-live-operation-drain)
                 (lambda ()
                   (sb-thread:release-mutex *live-operation-lock*)
                   (error "injected condition-wait failure after unlock")))
           (check (handler-case
                      (progn (with-live-operation-quiescence :unreachable) nil)
                    (error () t))
                  "an unwind after releasing the gate mutex propagates")
           (check (and *live-operation-admission-open*
                       (null *live-operation-quiescence-owner*))
                  "interrupted gate acquisition restores admission safely"))
      (setf (symbol-function 'finish-live-operation-drain) original-finish)))
  (let ((test-lock (sb-thread:make-mutex :name "test quiescence owner lock"))
        (test-cv (sb-thread:make-waitqueue :name "test quiescence owner condition"))
        (owner-entered nil)
        (release-owner nil)
        (owner-done nil)
        (owner-thread nil))
    (test-reset-live-operation-admission)
    (unwind-protect
         (progn
           (setf owner-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (with-live-operation-quiescence
                      (sb-thread:with-mutex (test-lock)
                        (setf owner-entered t)
                        (sb-thread:condition-broadcast test-cv)
                        (loop until release-owner
                              do (sb-thread:condition-wait test-cv test-lock))))
                    (sb-thread:with-mutex (test-lock)
                      (setf owner-done t)))
                  :name "test quiescence gate owner"))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) owner-entered)))
                  "the first quiescence operation owns the closed gate")
           (let ((*live-operation-depth* 1))
             (check (handler-case
                        (progn (with-live-operation-quiescence :unreachable) nil)
                      (error () t))
                    "a nested quiescence attempt fails before acquisition"))
           (check (and (not *live-operation-admission-open*)
                       *live-operation-quiescence-owner*)
                  "a failed nested close cannot reopen another owner's gate")
           (sb-thread:with-mutex (test-lock)
             (setf release-owner t)
             (sb-thread:condition-broadcast test-cv))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) owner-done)))
                  "the original quiescence owner completes")
           (check (and *live-operation-admission-open*
                       (null *live-operation-quiescence-owner*))
                  "the original owner reopens admission on exit"))
      (sb-thread:with-mutex (test-lock)
        (setf release-owner t)
        (sb-thread:condition-broadcast test-cv))
      (when owner-thread
        (unless (sb-thread:join-thread owner-thread :timeout 2)
          (ignore-errors
           (sb-thread:interrupt-thread
            owner-thread (lambda () (error "quiescence owner test timed out"))))
          (ignore-errors (sb-thread:join-thread owner-thread :timeout 2))))
      (test-reset-live-operation-admission))))

(defun test-nested-control-repl-reload ()
  (let ((test-lock (sb-thread:make-mutex :name "test nested reload lock"))
        (test-cv (sb-thread:make-waitqueue :name "test nested reload condition"))
        (backend *backend*)
        (worker-ready nil)
        (allow-control-eval nil)
        (nested-done nil)
        (reload-done nil)
        (nested-output nil)
        (load-count 0)
        (threads '())
        (original-load-system (symbol-function 'asdf:load-system)))
    (setf *reload-requested* nil
          (symbol-function 'asdf:load-system)
          (lambda (system &rest options)
            (declare (ignore system options))
            (sb-thread:with-mutex (test-lock)
              (incf load-count))
            t))
    (labels ((start-thread (function name)
               (push (sb-thread:make-thread function :name name) threads))
             (wait-for-flag (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader))))))
      (unwind-protect
           (progn
             (start-thread
              (lambda ()
                (let ((*backend* backend))
                  (with-live-operation
                    (sb-thread:with-mutex (test-lock)
                      (setf worker-ready t)
                      (sb-thread:condition-broadcast test-cv)
                      (loop until allow-control-eval
                            do (sb-thread:condition-wait test-cv test-lock)))
                    (let ((stream (make-string-output-stream)))
                      (control-eval stream '(progn (reload)))
                      (sb-thread:with-mutex (test-lock)
                        (setf nested-output (get-output-stream-string stream)
                              nested-done t))))))
              "test indirect control reload")
             (check (wait-for-flag (lambda () worker-ready))
                    "the control expression enters a live operation")
             ;; Exercise the same request flag the SIGHUP coordinator consumes.
             (request-reload)
             (start-thread
              (lambda ()
                (loop until (sb-ext:compare-and-swap *reload-requested* t nil)
                      do (sleep 0.01))
                (reload)
                (sb-thread:with-mutex (test-lock)
                  (setf reload-done t)))
              "test SIGHUP reload coordinator")
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "the SIGHUP reload closes admission while control is active")
             (sb-thread:with-mutex (test-lock)
               (setf allow-control-eval t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for-flag (lambda () nested-done))
                    "an indirect REPL reload returns instead of deadlocking")
             (check (wait-for-flag (lambda () reload-done))
                    "the SIGHUP reload finishes after the REPL operation leaves")
             (check (and nested-output (search ":QUEUED" nested-output))
                    (format nil "an indirect REPL reload is queued (~s)"
                            nested-output))
             (check (= load-count 1)
                    "the SIGHUP coordinator performs one reload")
             (check *reload-requested*
                    (format nil "the nested reload request remains queued (~s, loads ~d)"
                            *reload-requested* load-count)))
        (sb-thread:with-mutex (test-lock)
          (setf allow-control-eval t)
          (sb-thread:condition-broadcast test-cv))
        (setf (symbol-function 'asdf:load-system) original-load-system
              *reload-requested* nil)
        (dolist (thread threads)
          (unless (sb-thread:join-thread thread :timeout 2)
            (ignore-errors
             (sb-thread:interrupt-thread
              thread (lambda () (error "nested reload test timed out"))))
            (ignore-errors (sb-thread:join-thread thread :timeout 2))))))))

(defun test-legacy-live-operation-gate-call-shapes ()
  (let ((*live-operation-admission-open* nil)
        (*live-operation-drain-open* t))
    (check (eq (live-operation-admitted-p t) :drain)
           "legacy boolean drain predicates remain supported")
    (check (null (live-operation-admitted-p nil))
           "legacy false drain predicates remain closed"))
  (test-reset-live-operation-admission)
  ;; Simulate the first reload from an image whose old close function knew
  ;; nothing about owner tokens. The reload mutex identifies that old caller.
  (let ((*live-operation-admission-open* nil)
        (*live-operation-quiescence-owner* nil)
        (*live-operation-quiescence-owner-thread* nil)
        (*live-operation-drain-open* nil))
    (sb-thread:with-mutex (*reload-run-lock*)
      (adopt-legacy-live-operation-quiescence)
      (check (eq *live-operation-quiescence-owner-thread*
                 sb-thread:*current-thread*)
             "the first reload adopts its ownerless closed gate")
      (open-live-operation-admission)
      (check *live-operation-admission-open*
             "the first reload's legacy open call restores admission"))))

(defun test-live-operation-entry-interrupt-cleanup ()
  (let ((count-before *live-operation-count*)
        (original-try-enter (symbol-function 'try-enter-live-operation))
        (interrupt-queued nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'try-enter-live-operation)
                 (lambda (drain-p &optional backend)
                   (let ((entered-p (funcall original-try-enter drain-p backend)))
                     (when (and entered-p (not interrupt-queued))
                       (setf interrupt-queued t)
                       (sb-thread:interrupt-thread
                        sb-thread:*current-thread*
                        (lambda () (error "forced operation-entry interrupt"))))
                     entered-p)))
           (check (handler-case
                      (progn
                        (with-live-operation :body-should-not-finish)
                        nil)
                    (error () t))
                  "an interrupt after admission unwinds the live operation")
           (check interrupt-queued
                  "the operation-entry interrupt was queued")
           (check (= count-before *live-operation-count*)
                  "operation-entry interruption releases the reload count"))
      (setf (symbol-function 'try-enter-live-operation) original-try-enter))))

(defun test-transaction-ending-statement-classifier ()
  (dolist (query '("COMMIT" "END" "ROLLBACK" "ABORT"))
    (check (some #'transaction-ending-statement-p (parse-statements query))
           (format nil "transaction-ending AST recognized: ~a" query)))
  (let* ((statements (parse-statements "; ; SELECT 1; COMMIT; SELECT 2"))
         (ending-position (position-if #'transaction-ending-statement-p
                                       statements)))
    (check (= ending-position 1)
           "AST classifier finds a terminal after a nonterminal prefix")
    (check (= (length statements) 3)
           "the full simple-query message is parsed before dispatch"))
  (check (simple-query-payload-starts-with-transaction-end-p
          (frontend-message-payload
           (test-query-message "COMMIT; CHECKPOINT")))
         "a transaction-ending first command may enter drain admission")
  (dolist (query (list "/* leading comment */ COMMIT"
                       (format nil "-- leading comment~%ROLLBACK")
                       "; ; COMMIT"))
    (check (simple-query-payload-starts-with-transaction-end-p
            (frontend-message-payload (test-query-message query)))
           (format nil "SQL trivia before a transaction end is ignored: ~a"
                   query)))
  (check (not (simple-query-payload-starts-with-transaction-end-p
               (frontend-message-payload
                (test-query-message "SELECT 1; COMMIT"))))
         "work before COMMIT cannot enter drain admission")
  (check (not (simple-query-payload-starts-with-transaction-end-p
               (frontend-message-payload
                (test-query-message "SELECT 'COMMIT'"))))
         "a transaction keyword inside a string cannot enter drain admission")
  (check (handler-case
             (progn (parse-statements "COMMIT; SELECT") nil)
           (pg-error () t)
           (error () t))
         "a syntax error after COMMIT is detected while parsing the full message"))

(defun test-simple-query-terminal-statement-drain ()
  (let* ((backend *backend*)
         (events '())
         (query "COMMIT")
         (statements '((:type :commit)))
         (old-state (backend-state backend))
         (original-query-string (symbol-function 'query-string))
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (unwind-protect
         (progn
           (set-backend-state backend :in-progress)
           (setf (symbol-function 'query-string)
                 (lambda (message)
                   (declare (ignore message))
                   query)
                 (symbol-function 'exec-simple-query)
                 (lambda (stream query &rest options)
                   (declare (ignore stream query))
                   (push (list :exec options *live-operation-current-lane*) events)
                   (if (getf options :prepare-only-p)
                       (values statements t)
                       (values :ok t)))
                 (symbol-function 'send-ready-for-query)
                 (lambda (stream status)
                   (declare (ignore stream status))
                   (push (list :ready *live-operation-current-lane*) events)))
           (let ((*backend* backend)
                 (*live-operation-admission-open* nil)
                 (*live-operation-drain-open* t)
                 (*live-operation-drain-backends* (list backend))
                 (*live-operation-count* 0)
                 (*live-operation-backend-counts* (make-hash-table :test #'eq)))
             (test-dispatch-query-frame nil query))
           (check (equal (nreverse events)
                         '((:exec (:prepare-only-p t :start-execution-p nil
                                   :finish-execution-p nil) :drain)
                           (:exec (:statements ((:type :commit))
                                   :start-execution-p nil
                                   :finish-execution-p nil) :drain)
                           (:ready :drain)))
                  "a standalone transaction end executes in drain admission")
           (setf events '()
                 query "COMMIT; CHECKPOINT"
                 statements '((:type :commit) (:type :checkpoint)))
           (set-backend-state backend :idle)
           (let ((*backend* backend))
             (test-dispatch-query-frame nil query))
           (check (equal (nreverse events)
                         '((:exec (:start-execution-p nil
                                   :finish-execution-p nil) :normal)
                           (:ready :normal)))
                  "an idle COMMIT cannot admit unrelated SQL to the drain lane"))
      (setf (symbol-function 'query-string) original-query-string
            (symbol-function 'exec-simple-query) original-exec-simple-query
            (symbol-function 'send-ready-for-query) original-send-ready)
      (set-backend-state backend old-state))))

(defun test-cancel-query-waiting-for-live-operation-admission ()
  (let* ((backend (make-backend))
         (old-state (backend-state backend))
         (test-lock (sb-thread:make-mutex :name "test queued cancel lock"))
         (test-cv (sb-thread:make-waitqueue :name "test queued cancel condition"))
         (query-waiting-p nil)
         (blocker-ready nil)
         (release-blocker nil)
         (reload-entered nil)
         (release-reload nil)
         (reopen-attempting nil)
         (cancel-admission-locked nil)
         (release-cancel-admission nil)
         (cancel-done nil)
         (query-done nil)
         (query-executed nil)
         (query-error nil)
         (error-sqlstate nil)
         (ready-sent nil)
         (startup-registration-called nil)
         (cancel-path (merge-pathnames
                       (format nil "cancel-request-~d.bin"
                               (get-internal-real-time))
                       *pgdata*))
         (cancel-pid (+ #x70000000 (mod (get-internal-real-time) #x0fffffff)))
         (cancel-key #x1234abcd)
         (blocker-thread nil)
         (reload-thread nil)
         (query-thread nil)
         (second-query-thread nil)
         (idle-reload-thread nil)
         (idle-query-thread nil)
         (cancel-thread nil)
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-error (symbol-function 'send-error-response))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (labels ((wait-for (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader)))))
             (start-thread (function name)
               (sb-thread:make-thread function :name name)))
      (unwind-protect
           (progn
             (setf (backend-state backend) :in-progress
                   (backend-pid backend) cancel-pid
                   (backend-cancel-key backend) cancel-key
                   (backend-executing backend) nil
                   (backend-cancel-pending backend) nil
                   (symbol-function 'exec-simple-query)
                   (lambda (stream query &rest options)
                     (declare (ignore stream query options))
                     (sb-thread:with-mutex (test-lock)
                       (setf query-executed t))
                     (values :ok t))
                   (symbol-function 'send-error-response)
                   (lambda (stream condition)
                     (declare (ignore stream))
                     (sb-thread:with-mutex (test-lock)
                       (setf error-sqlstate
                             (and (typep condition 'pg-error)
                                  (pg-error-sqlstate condition)))))
                   (symbol-function 'send-ready-for-query)
                   (lambda (stream status)
                     (declare (ignore stream status))
                     (sb-thread:with-mutex (test-lock)
                       (setf ready-sent t))))
             (with-open-file (stream cancel-path
                                     :direction :output
                                     :if-exists :supersede
                                     :if-does-not-exist :create
                                     :element-type '(unsigned-byte 8))
               (write-sequence
                (concatenate '(vector (unsigned-byte 8))
                             (u32-to-bytes 16)
                             (u32-to-bytes +cancel-request-code+)
                             (u32-to-bytes cancel-pid)
                             (u32-to-bytes cancel-key))
                stream))
             (register-backend backend)
             (test-reset-live-operation-admission)
             (setf blocker-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* nil))
                        (with-live-operation
                          (sb-thread:with-mutex (test-lock)
                            (setf blocker-ready t)
                            (sb-thread:condition-broadcast test-cv)
                            (loop until release-blocker
                                  do (sb-thread:condition-wait test-cv
                                                               test-lock))))))
                    "test live operation holding reload drain"))
             (check (wait-for (lambda () blocker-ready))
                    "an admitted operation holds reload in the drain lane")
             (setf reload-thread
                   (start-thread
                    (lambda ()
                      (let ((*live-operation-before-open-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf reopen-attempting t)
                                  (sb-thread:condition-broadcast test-cv)))))
                        (with-live-operation-quiescence
                          (sb-thread:with-mutex (test-lock)
                            (setf reload-entered t)
                            (sb-thread:condition-broadcast test-cv)
                            (loop until release-reload
                                  do (sb-thread:condition-wait test-cv test-lock))))))
                    "test queued query cancel reload"))
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "reload closes admission before the query arrives")
             (setf query-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* backend)
                            (*live-operation-admission-wait-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf query-waiting-p t)
                                  (sb-thread:condition-broadcast test-cv)))))
                        (handler-case
                            (handle-simple-query
                             nil (test-query-message "SELECT 1"))
                          (error (condition)
                            (sb-thread:with-mutex (test-lock)
                              (setf query-error condition))))
                        (sb-thread:with-mutex (test-lock)
                          (setf query-done t)
                          (sb-thread:condition-broadcast test-cv))))
                    "test query queued for reload admission"))
             (check (wait-for (lambda () query-waiting-p))
                    "the query reaches the reload admission condition wait")
             (with-open-file (stream cancel-path
                                     :direction :input
                                     :element-type '(unsigned-byte 8))
               (let ((*backend* nil))
                 (check (null
                         (read-startup-until-ready
                          stream
                          (lambda ()
                            (setf startup-registration-called t))))
                        "a CancelRequest completes without starting a session")))
             (check (not startup-registration-called)
                    "CancelRequest bypasses ordinary backend registration")
             (check (wait-for (lambda () query-done))
                    "a canceled transaction can return its response during drain")
             (check (and (equal error-sqlstate "57014")
                         ready-sent
                         (null query-error))
                    "drain-time cancellation returns SQLSTATE 57014 and ReadyForQuery")
             (check (not reload-entered)
                    "the active counted operation still holds reload in drain")
             (sb-thread:with-mutex (test-lock)
               (setf release-blocker t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () reload-entered))
                    "reload enters ASDF quiescence after the drain completes")
             (sb-thread:with-mutex (test-lock)
               (setf query-waiting-p nil
                     query-done nil
                     query-error nil
                     error-sqlstate nil
                     ready-sent nil
                     cancel-done nil))
             (setf second-query-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* backend)
                            (*live-operation-admission-wait-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf query-waiting-p t)
                                  (sb-thread:condition-broadcast test-cv)))))
                        (handler-case
                            (handle-simple-query
                             nil (test-query-message "SELECT 2"))
                          (error (condition)
                            (sb-thread:with-mutex (test-lock)
                              (setf query-error condition))))
                        (sb-thread:with-mutex (test-lock)
                          (setf query-done t)
                          (sb-thread:condition-broadcast test-cv))))
                    "test query queued during ASDF reload"))
             (check (wait-for (lambda () query-waiting-p))
                    "a new query waits while ASDF reload is active")
             (setf cancel-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* nil)
                            (*live-operation-cancel-request-admission-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf cancel-admission-locked t)
                                  (sb-thread:condition-broadcast test-cv)
                                  (loop until release-cancel-admission
                                        do (sb-thread:condition-wait test-cv
                                                                     test-lock))))))
                        (with-open-file (stream cancel-path
                                                :direction :input
                                                :element-type '(unsigned-byte 8))
                          (read-startup-until-ready stream
                                                    (lambda ()
                                                      (setf startup-registration-called t))))
                        (sb-thread:with-mutex (test-lock)
                          (setf cancel-done t)
                          (sb-thread:condition-broadcast test-cv))))
                    "test cancel request deferred during ASDF quiescence"))
             (check (wait-for (lambda () cancel-admission-locked))
                    "CancelRequest reaches the atomic admission decision")
             (check (not query-done)
                    "the queued query cannot race ahead of deferred cancellation")
             (sb-thread:with-mutex (test-lock)
               (setf release-reload t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () reopen-attempting))
                    "reload attempts to reopen while CancelRequest owns admission")
             (sb-thread:with-mutex (test-lock)
               (setf release-cancel-admission t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () cancel-done))
                    "CancelRequest is queued without entering reloadable code")
             (check (wait-for (lambda () query-done))
                    "the deferred cancellation is delivered when admission reopens")
             (check (and (equal error-sqlstate "57014")
                         ready-sent
                         (null query-error))
                    "deferred CancelRequest cancels before SQL dispatch resumes")
             (check (null *live-operation-pending-cancel-requests*)
                    "deferred CancelRequests drain before admission reopens")
             (check (not query-executed)
                    "a canceled queued query never reaches SQL execution")
             (check (and (not (backend-executing backend))
                         (not (backend-cancel-pending backend)))
                    "queued cancellation clears backend interrupt state")
             ;; An idle CancelRequest received during ASDF must not survive
             ;; until this backend starts its next query.
             (ignore-errors (sb-thread:join-thread reload-thread :timeout 2))
             (sb-thread:with-mutex (test-lock)
               (setf reload-entered nil
                     release-reload nil
                     query-waiting-p nil
                     query-done nil
                     query-error nil
                     error-sqlstate nil
                     ready-sent nil
                     cancel-done nil
                     startup-registration-called nil))
             (setf idle-reload-thread
                   (start-thread
                    (lambda ()
                      (with-live-operation-quiescence
                        (sb-thread:with-mutex (test-lock)
                          (setf reload-entered t)
                          (sb-thread:condition-broadcast test-cv)
                          (loop until release-reload
                                do (sb-thread:condition-wait test-cv test-lock)))))
                    "test idle cancel reload"))
             (check (wait-for
                     (lambda ()
                       (and reload-entered
                            (sb-thread:with-mutex (*live-operation-lock*)
                              (not *live-operation-admission-open*)))))
                    "a second reload holds admission closed")
             (with-open-file (stream cancel-path
                                     :direction :input
                                     :element-type '(unsigned-byte 8))
               (let ((*backend* nil))
                 (check (null
                         (read-startup-until-ready stream
                                                   (lambda () nil)))
                        "an idle CancelRequest is ignored during ASDF")))
             (check (null *live-operation-pending-cancel-requests*)
                    "an idle CancelRequest is not deferred")
             (setf idle-query-thread
                   (start-thread
                    (lambda ()
                      (let ((*backend* backend)
                            (*live-operation-admission-wait-hook*
                              (lambda ()
                                (sb-thread:with-mutex (test-lock)
                                  (setf query-waiting-p t)
                                  (sb-thread:condition-broadcast test-cv)))))
                        (handler-case
                            (handle-simple-query nil (test-query-message "SELECT 3"))
                          (error (condition)
                            (sb-thread:with-mutex (test-lock)
                              (setf query-error condition))))
                        (sb-thread:with-mutex (test-lock)
                          (setf query-done t)
                          (sb-thread:condition-broadcast test-cv))))
                    "test query after idle cancel"))
             (check (wait-for (lambda () query-waiting-p))
                    "the next query waits behind the reload gate")
             (check (and (backend-executing backend)
                         (not (backend-cancel-pending backend)))
                    "the next query starts without inheriting an idle cancel")
             (sb-thread:with-mutex (test-lock)
               (setf release-reload t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () query-done))
                    "the next query completes after reload admission reopens")
             (check (and (null query-error)
                         (null error-sqlstate)
                         ready-sent
                         query-executed
                         (not (backend-cancel-pending backend)))
                    "an idle deferred cancel cannot cancel a later query"))
        (sb-thread:with-mutex (test-lock)
          (setf release-blocker t
                release-reload t)
          (sb-thread:condition-broadcast test-cv))
        (dolist (thread (list blocker-thread reload-thread query-thread
                              second-query-thread cancel-thread
                              idle-reload-thread idle-query-thread))
          (when thread
            (unless (sb-thread:join-thread thread :timeout 2)
              (ignore-errors
               (sb-thread:interrupt-thread
                thread (lambda () (error "queued cancellation test timed out"))))
              (ignore-errors (sb-thread:join-thread thread :timeout 2)))))
        (unregister-backend backend)
        (when (probe-file cancel-path)
          (delete-file cancel-path))
        (setf (symbol-function 'exec-simple-query) original-exec-simple-query
              (symbol-function 'send-error-response) original-send-error
              (symbol-function 'send-ready-for-query) original-send-ready)
        (set-backend-state backend old-state)
        (test-reset-live-operation-admission)))))

(defun test-late-cancel-is-cleared-at-query-end ()
  (let* ((backend *backend*)
         (ready-sent nil)
         (query-error nil)
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (unwind-protect
         (progn
           (test-reset-live-operation-admission)
           (setf (symbol-function 'exec-simple-query)
                 (lambda (stream query &rest options)
                   (declare (ignore stream query options))
                   (values :ok t))
                 (symbol-function 'send-ready-for-query)
                 (lambda (stream status)
                   (declare (ignore stream status))
                   ;; A protocol CancelRequest racing with the final response
                   ;; must be cleared by query cleanup, not become a new error.
                   (let ((*backend* nil))
                     (cancel-backend backend))
                   (setf ready-sent t)))
           (handler-case
               (handle-simple-query nil (test-query-message "SELECT 1"))
             (error (condition)
               (setf query-error condition)))
           (check (null query-error)
                  "a CancelRequest after ReadyForQuery does not escape cleanup")
           (check ready-sent
                  "the late-cancel query reaches ReadyForQuery")
           (check (and (not (backend-executing backend))
                       (not (backend-cancel-pending backend)))
                  "query-end cleanup clears a cancellation that arrived late"))
      (setf (symbol-function 'exec-simple-query) original-exec-simple-query
            (symbol-function 'send-ready-for-query) original-send-ready)
      (test-reset-live-operation-admission))))

(defun test-query-token-cleans-up-after-publication-error ()
  (let* ((backend (make-backend))
         (token-published nil)
         (original-begin (symbol-function 'begin-backend-query-execution)))
    (unwind-protect
         (let ((*backend* backend)
               (*live-operation-backend-query-tokens*
                 (make-hash-table :test #'eq)))
           (test-reset-live-operation-admission)
           (setf (symbol-function 'begin-backend-query-execution)
                 (lambda (target)
                   (funcall original-begin target)
                   (setf token-published
                         (not (null (gethash target
                                             *live-operation-backend-query-tokens*))))
                   (error "injected interrupt after query token publication")))
           (check (handler-case
                      (progn
                        (handle-simple-query nil (test-query-message "SELECT 1"))
                        nil)
                    (error () t))
                  "query setup error propagates after token publication")
           (check token-published
                  "query setup published its token before the injected error")
           (check (and (not (backend-executing backend))
                       (not (gethash backend
                                     *live-operation-backend-query-tokens*)))
                  "the installed unwind cleanup removes a partially published query"))
      (setf (symbol-function 'begin-backend-query-execution) original-begin)
      (test-reset-live-operation-admission))))

(defun test-unregister-backend-clears-query-state ()
  (let* ((backend (make-backend))
         (*live-operation-backend-query-tokens*
           (make-hash-table :test #'eq)))
    (setf (backend-executing backend) t
          (backend-cancel-pending backend) t
          (gethash backend *live-operation-backend-query-tokens*) (cons nil nil))
    (unregister-backend backend)
    (check (and (not (backend-executing backend))
                (not (backend-cancel-pending backend))
                (not (gethash backend *live-operation-backend-query-tokens*)))
           "backend teardown clears query state even if query cleanup was interrupted")))

(defun test-idle-backend-teardown-enters-captured-cleanup ()
  (let* ((backend (make-backend :pid #x7ffffffe))
         (*live-operation-backend-query-tokens*
           (make-hash-table :test #'eq))
         (original-close (symbol-function 'close-connection))
         (original-finished (symbol-function 'connection-finished))
         (original-unregister (symbol-function 'unregister-backend))
         (cleanup-lane nil)
         (cleanup-backend nil)
         (finished-count 0))
    (unwind-protect
         (progn
           (setf (backend-executing backend) t
                 (backend-cancel-pending backend) t
                 (gethash backend *live-operation-backend-query-tokens*)
                 (cons nil nil)
                 (symbol-function 'close-connection)
                 (lambda (sock stream)
                   (declare (ignore sock stream))
                   (setf cleanup-lane *live-operation-current-lane*
                         cleanup-backend *backend*))
                 (symbol-function 'connection-finished)
                 (lambda () (incf finished-count))
                 (symbol-function 'unregister-backend)
                 (lambda (target)
                   (funcall original-unregister target)
                   (error "injected interruption after backend unregister")))
           (test-reset-live-operation-admission)
           ;; The backend is idle, so it is deliberately absent from the
           ;; transaction-drain set. Registering after gate closure must add it
           ;; to cleanup admission without making it eligible for SQL drain.
           (sb-thread:with-mutex (*live-operation-lock*)
             (setf *live-operation-admission-open* nil
                   *live-operation-drain-open* t
                   *live-operation-drain-backends* nil
                   *live-operation-cleanup-backends* nil))
           (let ((*backend* backend)
                 (*shutdown* +smart-shutdown+))
             (register-backend backend))
           (check (and (member backend *live-operation-cleanup-backends* :test #'eq)
                       (not (member backend *live-operation-drain-backends* :test #'eq)))
                  "late registration enters cleanup admission only")
           (let ((*backend* nil)
                 (*shutdown* +smart-shutdown+))
             (check (handler-case
                        (progn
                          (finish-backend-connection backend nil nil)
                          nil)
                      (error () t))
                    "an error after unregister still runs connection accounting"))
           (check (eq cleanup-lane :drain)
                  "idle backend teardown enters its captured cleanup lane")
           (check (eq cleanup-backend backend)
                  "outer teardown binds the captured backend for admission")
           (check (and (not (backend-executing backend))
                       (not (backend-cancel-pending backend))
                       (not (gethash backend
                                     *live-operation-backend-query-tokens*)))
                  "captured teardown clears the backend's query state")
           (check (= finished-count 1)
                  "connection accounting runs once after unregister interruption"))
      (setf (symbol-function 'close-connection) original-close
            (symbol-function 'connection-finished) original-finished
            (symbol-function 'unregister-backend) original-unregister)
      (test-reset-live-operation-admission))))

(defun test-shutdown-cleanup-respects-reload-admission ()
  (let* ((backend (make-backend))
         (old-state (backend-state backend))
         (original-list-backends (symbol-function 'list-backends)))
    (unwind-protect
         (progn
           (set-backend-state backend :in-progress)
           (setf (symbol-function 'list-backends) (lambda () (list backend)))
           (test-reset-live-operation-admission)
           (sb-thread:with-mutex (*live-operation-lock*)
             (setf *live-operation-admission-open* nil
                   *live-operation-drain-open* t
                   *live-operation-drain-backends* (list backend)
                   *live-operation-cleanup-backends* (list backend)))
           (let ((*backend* backend)
                 (*shutdown* +smart-shutdown+))
             (with-live-operation-query-state-cleanup
               (check (eq *live-operation-current-lane* :drain)
                      "smart-shutdown query cleanup stays in reload drain admission"))
             (with-live-operation-shutdown-cleanup
               (check (eq *live-operation-current-lane* :drain)
                      "smart-shutdown fatal cleanup stays in reload drain admission")))
           (let ((*backend* backend)
                 (*shutdown* +fast-shutdown+))
             (with-live-operation-query-state-cleanup
               (check (null *live-operation-current-lane*)
                      "fast-shutdown query cleanup bypasses the closed gate"))
             (with-live-operation-shutdown-cleanup
               (check (null *live-operation-current-lane*)
                      "fast-shutdown fatal cleanup bypasses the closed gate"))))
      (setf (symbol-function 'list-backends) original-list-backends)
      (set-backend-state backend old-state)
      (test-reset-live-operation-admission))))

(defun test-pre-admitted-idle-backend-can-drain-after-starting-transaction ()
  (let* ((backend (make-backend))
         (test-lock (sb-thread:make-mutex :name "test snapshot drain lock"))
         (test-cv (sb-thread:make-waitqueue :name "test snapshot drain condition"))
         (idle-operation-ready nil)
         (release-idle-operation nil)
         (blocker-ready nil)
         (release-blocker nil)
         (reload-entered nil)
         (query-lane nil)
         (query-done nil)
         (query-error nil)
         (threads '())
         (original-list-backends (symbol-function 'list-backends))
         (original-query-string (symbol-function 'query-string))
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (labels ((signal-flag (writer)
               (sb-thread:with-mutex (test-lock)
                 (funcall writer)
                 (sb-thread:condition-broadcast test-cv)))
             (wait-for (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader)))))
             (start-thread (function name)
               (let ((thread (sb-thread:make-thread function :name name)))
                 (push thread threads)
                 thread)))
      (unwind-protect
           (progn
             (setf (backend-state backend) :idle
                   (symbol-function 'list-backends)
                   (lambda () (list backend))
                   (symbol-function 'query-string)
                   (lambda (message)
                     (declare (ignore message))
                     "COMMIT")
                   (symbol-function 'exec-simple-query)
                   (lambda (stream query &rest options)
                     (declare (ignore stream query))
                     (if (getf options :prepare-only-p)
                         (values '((:type :commit)) t)
                         (progn
                           (setf query-lane *live-operation-current-lane*)
                           (set-backend-state backend :idle)
                           (values :ok t))))
                   (symbol-function 'send-ready-for-query)
                   (lambda (stream status)
                     (declare (ignore stream status))
                     (signal-flag (lambda () (setf query-done t)))))
             (test-reset-live-operation-admission)
             ;; Keep a separate operation counted so reload remains in its drain
             ;; while the captured idle backend starts and commits a transaction.
             (start-thread
              (lambda ()
                (let ((*backend* nil))
                  (with-live-operation
                    (signal-flag (lambda () (setf blocker-ready t)))
                    (sb-thread:with-mutex (test-lock)
                      (loop until release-blocker
                            do (sb-thread:condition-wait test-cv test-lock))))))
              "test snapshot drain blocker")
             (check (wait-for (lambda () blocker-ready))
                    "an unrelated operation holds the reload drain")
             (start-thread
              (lambda ()
                (let ((*backend* backend))
                  (with-live-operation
                    (signal-flag (lambda () (setf idle-operation-ready t)))
                    (sb-thread:with-mutex (test-lock)
                      (loop until release-idle-operation
                            do (sb-thread:condition-wait test-cv test-lock)))
                    (set-backend-state backend :in-progress))))
              "test idle backend pre-admitted operation")
             (check (wait-for (lambda () idle-operation-ready))
                    "the idle backend has an admitted operation before reload")
             (start-thread
              (lambda ()
                (with-live-operation-quiescence
                  (signal-flag (lambda () (setf reload-entered t)))))
              "test snapshot drain reload")
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "reload closes admission and snapshots the idle backend")
             (sb-thread:with-mutex (test-lock)
               (setf release-idle-operation t)
               (sb-thread:condition-broadcast test-cv))
             (check (test-wait-until
                     (lambda ()
                       (and (eq (backend-state backend) :in-progress)
                            (not (sb-thread:with-mutex (*live-operation-lock*)
                                   (plusp (gethash backend
                                                   *live-operation-backend-counts*
                                                   0)))))))
                    "the captured backend starts a transaction after its first operation")
             (start-thread
              (lambda ()
                (let ((*backend* backend))
                  (handler-case
                      (handle-simple-query nil (test-query-message "COMMIT"))
                    (error (condition)
                      (signal-flag
                       (lambda ()
                         (setf query-error condition
                               query-done t)))))))
              "test transaction commit from captured idle backend")
             (check (wait-for (lambda () query-done))
                    "the captured backend commits while another operation holds drain")
             (check (and (eq query-lane :drain)
                         (eq (backend-state backend) :idle)
                         (null query-error))
                    "snapshot membership admits the newly active transaction to drain")
             (sb-thread:with-mutex (test-lock)
               (setf release-blocker t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () reload-entered))
                    "reload enters after the counted blocker exits"))
        (sb-thread:with-mutex (test-lock)
          (setf release-idle-operation t
                release-blocker t)
          (sb-thread:condition-broadcast test-cv))
        (dolist (thread threads)
          (unless (sb-thread:join-thread thread :timeout 2)
            (ignore-errors
             (sb-thread:interrupt-thread
              thread (lambda () (error "snapshot drain test timed out"))))
            (ignore-errors (sb-thread:join-thread thread :timeout 2))))
        (setf (symbol-function 'list-backends) original-list-backends
              (symbol-function 'query-string) original-query-string
              (symbol-function 'exec-simple-query) original-exec-simple-query
              (symbol-function 'send-ready-for-query) original-send-ready)
        (setf (backend-state backend) :idle)
        (test-reset-live-operation-admission)))))

(defun test-active-transaction-suffix-waits-for-reload ()
  (let* ((backend *backend*)
         (old-state (backend-state backend))
         (query "COMMIT; CHECKPOINT")
         (statements '((:type :commit)
                       (:type :checkpoint)))
         (test-lock (sb-thread:make-mutex :name "test terminal suffix lock"))
         (test-cv (sb-thread:make-waitqueue :name "test terminal suffix condition"))
         (operation-ready nil)
         (release-operation nil)
         (reload-entered nil)
         (release-reload nil)
         (prefix-executed nil)
         (suffix-executed nil)
         (query-done nil)
         (query-error nil)
         (reload-error nil)
         (threads '())
         (original-list-backends (symbol-function 'list-backends))
         (original-query-string (symbol-function 'query-string))
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (labels ((signal-flag (writer)
               (sb-thread:with-mutex (test-lock)
                 (funcall writer)
                 (sb-thread:condition-broadcast test-cv)))
             (wait-for (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader)))))
             (start-thread (function name)
               (push (sb-thread:make-thread function :name name) threads)))
      (unwind-protect
           (progn
             (set-backend-state backend :in-progress)
             (setf (symbol-function 'list-backends) (lambda () (list backend))
                   (symbol-function 'query-string)
                   (lambda (message)
                     (declare (ignore message))
                     query)
                   (symbol-function 'exec-simple-query)
                   (lambda (stream query &rest options)
                     (declare (ignore stream query))
                     (cond
                       ((getf options :prepare-only-p)
                        (values statements t))
                       ((getf options :statements)
                        (let ((executed (getf options :statements)))
                          (if (some #'transaction-ending-statement-p executed)
                              (progn
                                (check (eq *live-operation-current-lane* :drain)
                                       "transaction prefix executes in drain admission")
                                (set-backend-state backend :idle)
                                (signal-flag (lambda ()
                                               (setf prefix-executed t))))
                              (progn
                                (check (eq *live-operation-current-lane* :normal)
                                       "post-commit suffix uses normal admission")
                                (signal-flag (lambda ()
                                               (setf suffix-executed t))))))
                        (values :ok t))
                       (t (values :ok t))))
                   (symbol-function 'send-ready-for-query)
                   (lambda (stream status)
                     (declare (ignore stream status))
                     (signal-flag (lambda () (setf query-done t)))))
             (test-reset-live-operation-admission)
             ;; This admitted operation puts the active backend in the reload
             ;; snapshot before ordinary admission closes.
             (start-thread
              (lambda ()
                (let ((*backend* backend))
                  (with-live-operation
                    (sb-thread:with-mutex (test-lock)
                      (setf operation-ready t)
                      (sb-thread:condition-broadcast test-cv)
                      (loop until release-operation
                            do (sb-thread:condition-wait test-cv test-lock))))))
              "test pre-admitted transaction operation")
             (check (wait-for (lambda () operation-ready))
                    "the active transaction has an admitted operation")
             (start-thread
              (lambda ()
                (handler-case
                    (with-live-operation-quiescence
                      (sb-thread:with-mutex (test-lock)
                        (setf reload-entered t)
                        (sb-thread:condition-broadcast test-cv)
                        (loop until release-reload
                              do (sb-thread:condition-wait test-cv test-lock))))
                  (error (condition)
                    (signal-flag (lambda () (setf reload-error condition))))))
              "test suffix reload coordinator")
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "reload closes admission while the transaction is active")
             (start-thread
              (lambda ()
                (let ((*backend* backend))
                  (handler-case
                      (handle-simple-query nil (test-query-message query))
                    (error (condition)
                      (signal-flag (lambda () (setf query-error condition)))))))
              "test terminal prefix and suffix")
             (check (wait-for (lambda () prefix-executed))
                    "the prefix through COMMIT drains the active transaction")
             (sb-thread:with-mutex (test-lock)
               (setf release-operation t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () reload-entered))
                    "reload waits for the transaction-ending prefix to finish")
             (sleep 0.05)
             (check (not (sb-thread:with-mutex (test-lock) suffix-executed))
                    "CHECKPOINT after COMMIT waits for ordinary admission")
             (sb-thread:with-mutex (test-lock)
               (setf release-reload t)
               (sb-thread:condition-broadcast test-cv))
             (check (wait-for (lambda () query-done))
                    (format nil "the suffix resumes after reload (~a)" query-error))
             (check suffix-executed
                    "the suffix runs only after reload reopens admission")
             (check (null reload-error) "reload quiescence completes without error"))
        (sb-thread:with-mutex (test-lock)
          (setf release-operation t release-reload t)
          (sb-thread:condition-broadcast test-cv))
        (dolist (thread threads)
          (unless (sb-thread:join-thread thread :timeout 2)
            (ignore-errors
             (sb-thread:interrupt-thread
              thread (lambda () (error "terminal suffix test timed out"))))
            (ignore-errors (sb-thread:join-thread thread :timeout 2))))
        (setf (symbol-function 'list-backends) original-list-backends
              (symbol-function 'query-string) original-query-string
              (symbol-function 'exec-simple-query) original-exec-simple-query
              (symbol-function 'send-ready-for-query) original-send-ready)
        (set-backend-state backend old-state)
        (test-reset-live-operation-admission)))))

(defun test-disconnect-cleanup-drains-active-transaction ()
  (let* ((backend-a *backend*)
         (backend-b (make-backend))
         (old-state-a (backend-state backend-a))
         (old-state-b (backend-state backend-b))
         (test-lock (sb-thread:make-mutex :name "test disconnect drain lock"))
         (test-cv (sb-thread:make-waitqueue :name "test disconnect drain condition"))
         (waiter-ready nil)
         (waiter-done nil)
         (cleanup-done nil)
         (reload-entered nil)
         (release-reload nil)
         (cleanup-lane nil)
         (threads '())
         (original-list-backends (symbol-function 'list-backends)))
    (labels ((wait-for (reader)
               (test-wait-until
                (lambda ()
                  (sb-thread:with-mutex (test-lock)
                    (funcall reader)))))
             (start-thread (function name)
               (push (sb-thread:make-thread function :name name) threads)))
      (unwind-protect
           (progn
             (set-backend-state backend-a :in-progress)
             (set-backend-state backend-b :in-progress)
             (setf (symbol-function 'list-backends)
                   (lambda () (list backend-a backend-b)))
             (test-reset-live-operation-admission)
             (start-thread
              (lambda ()
                (let ((*backend* backend-b))
                  (with-live-operation
                    (sb-thread:with-mutex (test-lock)
                      (setf waiter-ready t)
                      (sb-thread:condition-broadcast test-cv))
                    (loop until (eq (backend-state backend-a) :idle)
                          do (sleep 0.01))
                    (set-backend-state backend-b :idle)
                    (sb-thread:with-mutex (test-lock)
                      (setf waiter-done t)
                      (sb-thread:condition-broadcast test-cv)))))
              "test active transaction waiter")
             (check (wait-for (lambda () waiter-ready))
                    "a second active transaction is waiting on the first")
             (start-thread
              (lambda ()
                (with-live-operation-quiescence
                  (sb-thread:with-mutex (test-lock)
                    (setf reload-entered t)
                    (sb-thread:condition-broadcast test-cv)
                    (loop until release-reload
                          do (sb-thread:condition-wait test-cv test-lock)))))
              "test disconnect reload coordinator")
             (check (test-wait-until
                     (lambda ()
                       (sb-thread:with-mutex (*live-operation-lock*)
                         (not *live-operation-admission-open*))))
                    "reload closes admission while both transactions are active")
             (start-thread
              (lambda ()
                (let ((*backend* backend-a))
                  (with-live-operation-transaction-drain
                    (setf cleanup-lane *live-operation-current-lane*)
                    (end-backend-session)
                    (sb-thread:with-mutex (test-lock)
                      (setf cleanup-done t)
                      (sb-thread:condition-broadcast test-cv)))))
              "test disconnected backend cleanup")
             (check (wait-for (lambda () cleanup-done))
                    "disconnect cleanup aborts its transaction in the drain lane")
             (check (eq cleanup-lane :drain)
                    "disconnect cleanup receives drain admission")
             (check (wait-for (lambda () waiter-done))
                    "transaction cleanup releases the other session's wait")
             (check (wait-for (lambda () reload-entered))
                    "cleanup lets reload finish draining")
             (sb-thread:with-mutex (test-lock)
               (setf release-reload t)
               (sb-thread:condition-broadcast test-cv)))
        (sb-thread:with-mutex (test-lock)
          (setf release-reload t)
          (sb-thread:condition-broadcast test-cv))
        (dolist (thread threads)
          (unless (sb-thread:join-thread thread :timeout 2)
            (ignore-errors
             (sb-thread:interrupt-thread
              thread (lambda () (error "disconnect drain test timed out"))))
            (ignore-errors (sb-thread:join-thread thread :timeout 2))))
        (setf (symbol-function 'list-backends) original-list-backends)
        (set-backend-state backend-a old-state-a)
        (set-backend-state backend-b old-state-b)
        (test-reset-live-operation-admission)))))

(defun test-idle-commit-waits-for-reload ()
  (let ((test-lock (sb-thread:make-mutex :name "test idle commit lock"))
        (test-cv (sb-thread:make-waitqueue :name "test idle commit condition"))
        (backend *backend*)
        (reload-entered nil)
        (release-reload nil)
        (query-attempting nil)
        (query-executed nil)
        (query-done nil)
        (query-error nil)
        (lane nil)
        (reload-thread nil)
        (query-thread nil)
        (old-state (backend-state *backend*))
        (original-try-enter (symbol-function 'try-enter-live-operation))
        (original-query-string (symbol-function 'query-string))
        (original-exec-simple-query (symbol-function 'exec-simple-query))
        (original-send-ready (symbol-function 'send-ready-for-query)))
    (set-backend-state backend :idle)
    (unwind-protect
         (progn
           (setf (symbol-function 'try-enter-live-operation)
                 (lambda (drain-p &optional backend)
                   (let ((entered-p (funcall original-try-enter drain-p backend)))
                     (when entered-p (setf lane entered-p))
                     entered-p))
                 (symbol-function 'query-string)
                 (lambda (message)
                   (declare (ignore message))
                   "COMMIT; CHECKPOINT")
                 (symbol-function 'exec-simple-query)
                 (lambda (stream query &rest options)
                   (declare (ignore stream query options))
                   (sb-thread:with-mutex (test-lock)
                     (setf query-executed t))
                   (values :ok t))
                 (symbol-function 'send-ready-for-query)
                 (lambda (stream status)
                   (declare (ignore stream status))
                   (sb-thread:with-mutex (test-lock)
                     (setf query-done t))))
           (test-reset-live-operation-admission)
           (setf reload-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (with-live-operation-quiescence
                      (sb-thread:with-mutex (test-lock)
                        (setf reload-entered t)
                        (sb-thread:condition-broadcast test-cv)
                        (loop until release-reload
                              do (sb-thread:condition-wait test-cv test-lock))))
                    (values))
                  :name "test held reload quiescence"))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) reload-entered)))
                  "reload reaches its quiescent body")
           (setf query-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* backend))
                      (sb-thread:with-mutex (test-lock)
                        (setf query-attempting t))
                      (handler-case
                          (handle-simple-query
                           nil (test-query-message "COMMIT; CHECKPOINT"))
                        (error (condition)
                          (sb-thread:with-mutex (test-lock)
                            (setf query-error condition))))))
                  :name "test idle COMMIT query"))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) query-attempting)))
                  "idle COMMIT request reaches ordinary admission")
           (sleep 0.05)
           (check (not (sb-thread:with-mutex (test-lock) query-executed))
                  "idle COMMIT and CHECKPOINT cannot execute during reload")
           (sb-thread:with-mutex (test-lock)
             (setf release-reload t)
             (sb-thread:condition-broadcast test-cv))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) query-done)))
                  (format nil "idle query proceeds after normal admission reopens (~a)"
                          query-error))
           (check (eq lane :normal)
                  "idle COMMIT uses normal admission after reload"))
      (sb-thread:with-mutex (test-lock)
        (setf release-reload t)
        (sb-thread:condition-broadcast test-cv))
      (dolist (thread (list reload-thread query-thread))
        (when thread
          (unless (sb-thread:join-thread thread :timeout 2)
            (ignore-errors
             (sb-thread:interrupt-thread
              thread (lambda () (error "idle commit test timed out"))))
            (ignore-errors (sb-thread:join-thread thread :timeout 2))))))
    (setf (symbol-function 'try-enter-live-operation) original-try-enter
          (symbol-function 'query-string) original-query-string
          (symbol-function 'exec-simple-query) original-exec-simple-query
          (symbol-function 'send-ready-for-query) original-send-ready
          (backend-state backend) old-state)
    (test-reset-live-operation-admission)))

(defun test-idle-active-transaction-does-not-block-reload ()
  (let* ((backend *backend*)
         (old-state (backend-state backend))
         (test-lock (sb-thread:make-mutex :name "test idle transaction reload lock"))
         (test-cv (sb-thread:make-waitqueue :name "test idle transaction reload cv"))
         (reload-entered nil)
         (release-reload nil)
         (query-executed nil)
         (query-done nil)
         (query-lane nil)
         (reload-thread nil)
         (query-thread nil)
         (original-list-backends (symbol-function 'list-backends))
         (original-query-string (symbol-function 'query-string))
         (original-exec-simple-query (symbol-function 'exec-simple-query))
         (original-send-ready (symbol-function 'send-ready-for-query)))
    (unwind-protect
         (progn
           (set-backend-state backend :in-progress)
           (setf (symbol-function 'list-backends) (lambda () (list backend))
                 (symbol-function 'query-string)
                 (lambda (message)
                   (declare (ignore message))
                   "SELECT 1")
                 (symbol-function 'exec-simple-query)
                 (lambda (stream query &rest options)
                   (declare (ignore stream query options))
                   (sb-thread:with-mutex (test-lock)
                     (setf query-lane *live-operation-current-lane*
                           query-executed t))
                   (values :ok t))
                 (symbol-function 'send-ready-for-query)
                 (lambda (stream status)
                   (declare (ignore stream status))
                   (sb-thread:with-mutex (test-lock)
                     (setf query-done t)
                     (sb-thread:condition-broadcast test-cv))))
           (test-reset-live-operation-admission)
           (setf reload-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (with-live-operation-quiescence
                      (sb-thread:with-mutex (test-lock)
                        (setf reload-entered t)
                        (sb-thread:condition-broadcast test-cv)
                        (loop until release-reload
                              do (sb-thread:condition-wait test-cv test-lock)))))
                  :name "test reload with idle transaction"))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) reload-entered)))
                  "an idle open transaction does not block reload quiescence")
           (setf query-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* backend))
                      (handle-simple-query nil (test-query-message "SELECT 1"))))
                  :name "test query after idle transaction reload"))
           (sleep 0.05)
           (check (not (sb-thread:with-mutex (test-lock) query-executed))
                  "a nonterminal query waits while reload is quiescent")
           (sb-thread:with-mutex (test-lock)
             (setf release-reload t)
             (sb-thread:condition-broadcast test-cv))
           (check (test-wait-until
                   (lambda ()
                     (sb-thread:with-mutex (test-lock) query-done)))
                  "the open transaction continues after reload reopens admission")
           (check (eq query-lane :normal)
                  "post-reload transaction work uses ordinary admission"))
      (sb-thread:with-mutex (test-lock)
        (setf release-reload t)
        (sb-thread:condition-broadcast test-cv))
      (dolist (thread (list reload-thread query-thread))
        (when thread
          (unless (sb-thread:join-thread thread :timeout 2)
            (ignore-errors
             (sb-thread:interrupt-thread
              thread (lambda () (error "idle transaction reload test timed out"))))
            (ignore-errors (sb-thread:join-thread thread :timeout 2)))))
      (setf (symbol-function 'list-backends) original-list-backends
            (symbol-function 'query-string) original-query-string
            (symbol-function 'exec-simple-query) original-exec-simple-query
            (symbol-function 'send-ready-for-query) original-send-ready)
      (set-backend-state backend old-state)
      (test-reset-live-operation-admission))))

(defun test-wal-catalog-codec-and-page-checksums ()
  (let* ((catalog (make-array 17 :element-type '(unsigned-byte 8)
                                 :initial-element 65))
         (payload (wal-encode-catalog-payload catalog '(123 456))))
    (multiple-value-bind (deletes decoded)
        (wal-decode-catalog-payload payload)
      (check (equal deletes '(123 456)) "WAL catalog delete list round-trips")
      (check (equalp decoded catalog) "WAL catalog image round-trips"))
    (let ((corrupt (copy-seq payload)))
      (decf (aref corrupt 12))
      (check (handler-case
                 (progn (wal-decode-catalog-payload corrupt) nil)
               (error () t))
             "WAL catalog image length mismatch is rejected")))
  (let ((page (page-init (make-page))))
    (check (wal-verify-page page 123 4) "unstamped page is accepted for staging")
    (wal-stamp-page page 99 123 4)
    (check (wal-verify-page page 123 4) "WAL page checksum verifies at its location")
    (check (not (wal-verify-page page 123 5))
           "WAL page checksum binds the block number")
    (setf (aref page 100) (logxor (aref page 100) #xFF))
    (check (not (wal-verify-page page 123 4))
           "WAL page checksum detects page damage")))

(defun test-wal-record-codec ()
  (let* ((payload (make-array 0 :element-type '(unsigned-byte 8)))
         (record (wal-encode-record +wal-record-commit+ 42 64 payload))
         (decoded (wal-decode-record record 64)))
    (check (= (wal-record-type decoded) +wal-record-commit+)
           "WAL record type round-trips")
    (check (= (wal-record-xid decoded) 42) "WAL record XID round-trips")
    (check (= (wal-record-start decoded) 64) "WAL record LSN round-trips")
    (check (= (wal-record-end decoded) (+ 64 +wal-header-size+))
           "WAL record end LSN is computed")
    (check (equalp (wal-record-payload decoded) payload)
           "WAL record payload round-trips")
    (dolist (case
                (list
                 (cons "bad WAL record magic is rejected"
                       (lambda (bytes) (setf (aref bytes 0) 0)))
                 (cons "unsupported WAL record version is rejected"
                       (lambda (bytes) (wal-set-u16 bytes 4 1)))
                 (cons "unknown WAL record type is rejected"
                       (lambda (bytes) (wal-set-u16 bytes 6 #xFFFF)))
                 (cons "bad WAL record length is rejected"
                       (lambda (bytes) (wal-set-u32 bytes 8 31)))
                 (cons "WAL record at the wrong LSN is rejected"
                       (lambda (bytes) (wal-set-u64 bytes 12 65)))
                 (cons "bad WAL record checksum is rejected"
                       (lambda (bytes)
                         (setf (aref bytes 20) (logxor (aref bytes 20) #xFF))))))
      (let ((corrupt (copy-seq record)))
        (funcall (cdr case) corrupt)
        (check (handler-case
                   (progn (wal-decode-record corrupt 64) nil)
                 (error () t))
               (car case))))))

(defun test-wal-chunked-catalog-record ()
  (let* ((test-pgdata (merge-pathnames "wal-catalog-chunks-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-ready* t)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-pending-deletes* '())
         (*wal-pending-btree-split* nil)
         (*wal-test-pause-at* nil)
         (catalog-bytes
           (make-array (+ (- +wal-segment-size+ +wal-header-size+) 1)
                       :element-type '(unsigned-byte 8)
                       :initial-element 65))
         (catalog-written nil)
         (record-types '()))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (unwind-protect
         (progn
           (wal-commit-catalog
            catalog-bytes
            (lambda (bytes)
              (setf catalog-written (equalp bytes catalog-bytes))))
           (check catalog-written
                  "oversized catalog image is published after WAL flush")
           (wal-scan (lambda (record)
                       (push (wal-record-type record) record-types)))
           (setf record-types (nreverse record-types))
           (check (and (= (count +wal-record-catalog-chunk+ record-types) 2)
                       (= (car (last record-types))
                          +wal-record-catalog-commit+))
                  "oversized catalog image uses chunks and a terminal record")
           (let ((*wal-recovery-group-active* nil)
                 (*wal-recovery-page-images* '())
                 (*wal-recovery-catalog-chunks* '())
                 (*wal-recovery-catalog-chunk-count* nil)
                 (*wal-recovery-catalog-next-chunk* 0))
             (wal-scan #'wal-replay-record))
           (with-open-file (stream (merge-pathnames "global/pg_catalog.dat"
                                                    *pgdata*)
                                   :direction :input
                                   :element-type '(unsigned-byte 8))
             (let ((recovered (make-array (file-length stream)
                                          :element-type '(unsigned-byte 8))))
               (read-sequence recovered stream)
               (check (equalp recovered catalog-bytes)
                      "chunked catalog WAL reassembles during recovery")))
           (let ((*wal-recovery-catalog-chunks*
                   (list (make-array 1 :element-type '(unsigned-byte 8))))
                 (*wal-recovery-catalog-chunk-count* 1)
                 (*wal-recovery-catalog-next-chunk* 1))
             (check (handler-case
                        (progn
                          (wal-reassemble-catalog-chunks 1 #xffffffffffffffff)
                          nil)
                      (error () t))
                    "catalog chunk length mismatch fails before allocation")))
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*))))))

(defun test-wal-catalog-terminal-validation-precedes-page-redo ()
  (let* ((test-pgdata
           (merge-pathnames "wal-invalid-catalog-terminal-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*relation-streams* (make-hash-table :test #'equal))
         (*wal-ready* t)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (relfilenode 424242)
         (page-before (page-init (make-page)))
         (page-after (let ((page (page-init (make-page))))
                       (setf (aref page 100) #x5A)
                       page))
         (images (list (make-wal-page-image :relfilenode relfilenode
                                            :block 0 :page page-after)))
         (catalog-chunk
           (make-array 1 :element-type '(unsigned-byte 8) :initial-element 65))
         (batch-bytes
           (wal-encode-record +wal-record-batch-start+ 0 0 #()))
         (batch-record (wal-decode-record batch-bytes 0))
         (pages-start (wal-record-end batch-record))
         (pages-end (+ pages-start +wal-header-size+
                       (wal-page-payload-length images)))
         (pages-payload (wal-encode-page-images images pages-end))
         (pages-bytes
           (wal-encode-record +wal-record-pages+ 0 pages-start pages-payload))
         (pages-record (wal-decode-record pages-bytes pages-start))
         (chunk-start (wal-record-end pages-record))
         (chunk-payload (wal-encode-catalog-chunk-payload
                         catalog-chunk 0 1))
         (chunk-bytes
           (wal-encode-record +wal-record-catalog-chunk+ 0 chunk-start
                              chunk-payload))
         (chunk-record (wal-decode-record chunk-bytes chunk-start))
         (terminal-start (wal-record-end chunk-record))
         (terminal-payload
           (wal-encode-catalog-commit-payload
            (make-array 2 :element-type '(unsigned-byte 8) :initial-element 65)
            1))
         (terminal-bytes
           (wal-encode-record +wal-record-catalog-commit+ 0 terminal-start
                              terminal-payload))
         (terminal-record (wal-decode-record terminal-bytes terminal-start))
         (end-lsn (wal-record-end terminal-record)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (merge-pathnames "base/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (ensure-directories-exist (relation-file-path relfilenode))
    (smgr-write relfilenode 0 page-before)
    (with-open-file (stream (wal-segment-path 0)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (dolist (bytes (list batch-bytes pages-bytes chunk-bytes terminal-bytes))
        (write-sequence bytes stream)))
    (setf *wal-durable-lsn* end-lsn)
    (wal-save-control-fields 0 end-lsn)
    (unwind-protect
         (progn
           ;; Each record was decoded after encoding, so its CRC is valid. The
           ;; terminal declares two catalog bytes although its only chunk has
           ;; one; recovery must reject it before writing the staged page.
           (let ((error-message
                   (handler-case
                       (progn (wal-recover) nil)
                     (error (condition) (princ-to-string condition)))))
             (check (and error-message
                         (search "catalog chunks do not match their declared length"
                                 error-message))
                    "malformed chunked catalog terminal fails recovery")
             (check (equalp (smgr-read relfilenode 0) page-before)
                    "invalid catalog terminal leaves relation pages unchanged")))
      (smgr-close-all)
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-wal-segment-rotation ()
  (let* ((test-pgdata (merge-pathnames "wal-segment-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (directory-synced-on-rotation nil)
         (original-fsync-directory (symbol-function 'fsync-directory)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (wal-directory))
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-directory)
                 (lambda (directory)
                   (when (eql *wal-output-segment* 0)
                     (setf directory-synced-on-rotation t))
                   (funcall original-fsync-directory directory)))
           (wal-append-record
            +wal-record-commit+ 42
            (make-array (- +wal-segment-size+ +wal-header-size+)
                        :element-type '(unsigned-byte 8)
                        :initial-element 0))
           (wal-append-record +wal-record-checkpoint+ 0 #())
           (check directory-synced-on-rotation
                  "WAL rotation syncs segment names before opening the next segment")
           (wal-flush)
           (let ((records 0))
             (multiple-value-bind (end-lsn segments)
                 (wal-scan (lambda (record)
                             (declare (ignore record))
                             (incf records)))
               (check (= records 2) "WAL scanner reads records across a segment boundary")
               (check (= end-lsn *wal-end-lsn*) "WAL scan ends at the last segment LSN")
               (check (equal segments '(0 1)) "WAL scan discovers contiguous segments")
               (wal-save-checkpoint-control end-lsn)
               (wal-remove-checkpointed-segments end-lsn)
               (check (not (probe-file (wal-segment-path 0)))
                      "checkpoint recycling removes older WAL segments")
               (check (probe-file (wal-segment-path 1))
                      "checkpoint recycling retains the checkpoint segment")
               (multiple-value-bind (retained-end retained-segments)
                   (wal-scan (lambda (record) (declare (ignore record))))
                 (check (= retained-end end-lsn)
                        "WAL recovery scans from the retained checkpoint segment")
                 (check (equal retained-segments '(1))
                        "WAL recovery accepts a retained checkpoint suffix"))))
           (setf (symbol-function 'fsync-directory) original-fsync-directory)
           (when *wal-output-stream*
             (ignore-errors (close *wal-output-stream*)))))))

(defun test-wal-checkpoint-skips-obsolete-prefix ()
  (let* ((test-pgdata (merge-pathnames "wal-checkpoint-scan-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-checkpoint-lsn* (+ (* 2 +wal-segment-size+)
                                  (* 2 +wal-header-size+)))
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+)
                               (* 3 +wal-header-size+)))
         (prefix (wal-encode-record +wal-record-commit+ 1 0 #()))
         (same-segment-prefix
           (wal-encode-record +wal-record-commit+ 3
                              (* 2 +wal-segment-size+) #()))
         (checkpoint-start (+ (* 2 +wal-segment-size+) +wal-header-size+))
         (checkpoint (wal-encode-record +wal-record-checkpoint+ 0
                                        checkpoint-start #()))
         (post-checkpoint (wal-encode-record
                           +wal-record-commit+ 2
                           (+ checkpoint-start +wal-header-size+) #()))
         (obsolete-bytes (copy-seq prefix))
         (same-segment-obsolete-bytes (copy-seq same-segment-prefix)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    ;; The gap at segment 1 and damage in segment 0 are both before the durable
    ;; checkpoint, so neither should prevent scanning the retained suffix.
    (setf (aref obsolete-bytes 28) (logxor (aref obsolete-bytes 28) #xFF))
    (setf (aref same-segment-obsolete-bytes 28)
          (logxor (aref same-segment-obsolete-bytes 28) #xFF))
    (with-open-file (stream (wal-segment-path 0)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-sequence obsolete-bytes stream))
    (with-open-file (stream (wal-segment-path 2)
                            :direction :output
                            :element-type '(unsigned-byte 8)
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-sequence same-segment-obsolete-bytes stream)
      (write-sequence checkpoint stream)
      (write-sequence post-checkpoint stream))
    (multiple-value-bind (end-lsn segments)
        (wal-scan (lambda (record) (declare (ignore record))))
      (check (= end-lsn *wal-durable-lsn*)
             "checkpoint scan advances through the retained WAL suffix")
      (check (equal segments '(2))
             "checkpoint scan ignores obsolete segments and gaps"))
    ;; The checkpoint itself and all records after it remain subject to normal
    ;; integrity checks.
    (with-open-file (stream (wal-segment-path 2)
                            :direction :io
                            :element-type '(unsigned-byte 8)
                            :if-exists :overwrite)
      (file-position stream (+ +wal-header-size+ 28))
      (write-byte (logxor (aref checkpoint 28) #xFF) stream))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "WAL checksum mismatch" (princ-to-string condition))))
           "checkpoint scan validates the checkpoint record")
    (with-open-file (stream (wal-segment-path 2)
                            :direction :io
                            :element-type '(unsigned-byte 8)
                            :if-exists :overwrite)
      (file-position stream +wal-header-size+)
      (write-sequence checkpoint stream)
      (file-position stream (+ (* 2 +wal-header-size+) 28))
      (write-byte (logxor (aref post-checkpoint 28) #xFF) stream))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "WAL checksum mismatch" (princ-to-string condition))))
           "checkpoint scan validates records after the checkpoint")))

(defun test-wal-missing-interior-segment ()
  (let* ((test-pgdata (merge-pathnames "wal-gap-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (*wal-checkpoint-lsn* +wal-header-size+)
         (*wal-durable-lsn* (+ (* 2 +wal-segment-size+) +wal-header-size+)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (dolist (segment '(0 1 2))
      (let* ((start (* segment +wal-segment-size+))
             (type (if (zerop segment)
                       +wal-record-checkpoint+
                       +wal-record-commit+))
             (record (wal-encode-record type 0 start #())))
        (with-open-file (stream (wal-segment-path segment)
                                :direction :output
                                :element-type '(unsigned-byte 8)
                                :if-exists :supersede
                                :if-does-not-exist :create)
          (write-sequence record stream))))
    (multiple-value-bind (end-lsn segments)
        (wal-scan (lambda (record) (declare (ignore record))))
      (check (= end-lsn *wal-durable-lsn*)
             "WAL scan reads a complete retained segment chain")
      (check (equal segments '(0 1 2))
             "WAL scan sees all segments before a missing-segment check"))
    (delete-file (wal-segment-path 1))
    (check (handler-case
               (progn (wal-scan (lambda (record) (declare (ignore record)))) nil)
             (error (condition)
               (search "missing WAL segment" (princ-to-string condition))))
           "WAL recovery rejects a missing interior segment")))

(defun test-swank-arguments ()
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address)
      (parse-args '("--swank-port=4512" "--swank-bind-address=0.0.0.0"))
    (declare (ignore pgdata control-port control-socket))
    (check (string= host "127.0.0.1") "default SQL host is unchanged")
    (check (= port 5433) "default SQL port is unchanged")
    (check (= swank-port 4512) "Swank port accepts --option=value")
    (check (string= swank-bind-address "0.0.0.0")
           "Swank bind address accepts --option=value"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address)
      (parse-args '("--swank-bind-address" "0.0.0.0"))
    (declare (ignore host port pgdata control-port control-socket))
    (check (null swank-port) "Swank bind address alone does not enable listener")
    (check (string= swank-bind-address "0.0.0.0")
           "Swank bind address is preserved without a listener"))
  (check (handler-case
             (progn (parse-args '("--swank-port=")) nil)
           (error () t))
         "empty Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-port")) nil)
           (error () t))
         "missing Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-port" "65536")) nil)
           (error () t))
         "out-of-range Swank port is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-bind-address=")) nil)
           (error () t))
         "empty Swank bind address is rejected")
  (check (handler-case
             (progn (parse-args '("--swank-bind-address")) nil)
           (error () t))
         "missing Swank bind address is rejected"))

(defun test-malformed-packet-p (path bytes reader &optional expected-message)
  (with-open-file (stream path
                          :direction :output
                          :if-exists :supersede
                          :if-does-not-exist :create
                          :element-type '(unsigned-byte 8))
    (write-sequence bytes stream))
  (handler-case
      (with-open-file (stream path
                              :direction :input
                              :element-type '(unsigned-byte 8))
        (funcall reader stream)
        nil)
    (pg-error (condition)
      (and expected-message
           (string= (pg-error-sqlstate condition) "08P01")
           (string= (pg-error-message condition) expected-message)))
    (error () (null expected-message))))

(defun test-concat-octets (&rest parts)
  (let* ((size (reduce #'+ parts :key #'length :initial-value 0))
         (bytes (make-array size :element-type '(unsigned-byte 8)))
         (offset 0))
    (dolist (part parts)
      (replace bytes part :start1 offset)
      (incf offset (length part)))
    bytes))

(defun test-startup-packet-size (path packet-length)
  (with-open-file (stream path
                          :direction :output
                          :if-exists :supersede
                          :if-does-not-exist :create
                          :element-type '(unsigned-byte 8))
    (write-sequence (u32-to-bytes packet-length) stream)
    (write-sequence
     (make-array (- packet-length 4)
                 :element-type '(unsigned-byte 8)
                 :initial-element 0)
     stream))
  (with-open-file (stream path
                          :direction :input
                          :element-type '(unsigned-byte 8))
    (let ((body (read-startup-packet stream)))
      (and body (= (length body) +max-startup-packet-length+)))))

(defun test-protocol-packet-limits ()
  (let ((path (merge-pathnames "malformed-packet.out" *pgdata*)))
    (check (test-startup-packet-size path +max-startup-message-size+)
           "startup packets accept PostgreSQL's maximum body length")
    (check (test-malformed-packet-p
            path (u32-to-bytes 7) #'read-startup-packet
            "startup packet length is too short")
           "undersized startup packets are rejected")
    (check (test-malformed-packet-p
            path (u32-to-bytes (1+ +max-startup-message-size+))
            #'read-startup-packet "startup packet exceeds the size limit")
           "oversized startup packets are rejected before allocation")
    (check (test-malformed-packet-p
            path (u32-to-bytes 8) #'read-startup-packet
            "incomplete startup packet body")
           "truncated startup packet bodies are rejected")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes 3))
            #'test-read-frontend-message "frontend message length is too short")
           "undersized query packets are rejected")
    (check (test-malformed-packet-p
            path (vector (char-code #\Q))
            #'test-read-frontend-message "incomplete frontend message length")
           "truncated query packet lengths are rejected after admission")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes
                                      (1+ +max-query-message-size+)))
            #'test-read-frontend-message "frontend message exceeds the size limit")
           "oversized query packets are rejected before allocation")
    (check (test-malformed-packet-p
            path (test-concat-octets (vector (char-code #\Q))
                                     (u32-to-bytes 5))
            #'test-read-frontend-message "incomplete frontend message body")
           "truncated query packet bodies are rejected")))

(defun test-protocol-decoding-enters-live-operation ()
  (let* ((backend (make-backend))
         (test-lock (sb-thread:make-mutex :name "test protocol admission lock"))
         (admission-waited nil)
         (decoder-ran nil)
         (decoder-live-count 0)
         (decoder-lane nil)
         (handler-ran nil)
         (worker-error nil)
         (worker nil)
         (original-decoder (symbol-function 'decode-frontend-message))
         (original-handler (symbol-function 'handle-unsupported-message)))
    (unwind-protect
         (progn
           (setf (symbol-function 'decode-frontend-message)
                 (lambda (type-byte length-bytes payload)
                   (let ((message (funcall original-decoder
                                           type-byte length-bytes payload)))
                     (sb-thread:with-mutex (test-lock)
                       (setf decoder-ran t
                             decoder-live-count *live-operation-count*
                             decoder-lane *live-operation-current-lane*))
                     message))
                 (symbol-function 'handle-unsupported-message)
                 (lambda (stream message)
                   (declare (ignore stream message))
                   (sb-thread:with-mutex (test-lock)
                     (setf handler-ran t))))
           (let ((*backend* backend))
             (with-live-operation-quiescence
               (setf worker
                     (sb-thread:make-thread
                      (lambda ()
                        (let ((*backend* backend)
                              (*live-operation-admission-wait-hook*
                                (lambda ()
                                  (sb-thread:with-mutex (test-lock)
                                    (setf admission-waited t)))))
                          (handler-case
                              (dispatch-frontend-frame
                               nil (char-code #\D) (u32-to-bytes 5) #(0))
                            (error (condition)
                              (sb-thread:with-mutex (test-lock)
                                (setf worker-error condition))))))
                      :name "test admitted protocol decode"))
               (check (test-wait-until
                       (lambda ()
                         (sb-thread:with-mutex (test-lock) admission-waited)))
                      "protocol dispatch waits at closed reload admission")
               (check (not (sb-thread:with-mutex (test-lock) decoder-ran))
                      "protocol validation and object construction wait for admission")
               (check (not (sb-thread:with-mutex (test-lock) handler-ran))
                      "protocol handling waits for admission")))
           (check (and worker
                       (eq (sb-thread:join-thread worker :timeout 2) :continue))
                  "raw protocol dispatch resumes after reload admission opens")
           (check (and (sb-thread:with-mutex (test-lock) decoder-ran)
                       (plusp (sb-thread:with-mutex (test-lock)
                                decoder-live-count))
                       (eq (sb-thread:with-mutex (test-lock) decoder-lane)
                           :normal))
                  "frontend validation and construction run inside an admitted operation")
           (check (and (sb-thread:with-mutex (test-lock) handler-ran)
                       (null worker-error))
                  "the decoded message is dispatched after admission"))
      (when (and worker (sb-thread:thread-alive-p worker))
        (ignore-errors
         (sb-thread:interrupt-thread
          worker (lambda () (error "protocol admission test timed out"))))
        (ignore-errors (sb-thread:join-thread worker :timeout 2)))
      (setf (symbol-function 'decode-frontend-message) original-decoder
            (symbol-function 'handle-unsupported-message) original-handler)
      (test-reset-live-operation-admission))))

(defun test-malformed-protocol-frames-drain-to-fatal-error ()
  (let* ((backend (make-backend))
         (old-state (backend-state backend)))
    (labels ((expect-fatal-frame-error (type-byte length-bytes payload label)
               (let ((worker
                       (sb-thread:make-thread
                        (lambda ()
                          (let ((*backend* backend)
                                (*live-operation-admission-open* nil)
                                (*live-operation-drain-open* t)
                                (*live-operation-drain-backends* (list backend))
                                (*live-operation-cleanup-backends* (list backend))
                                (*live-operation-count* 0)
                                (*live-operation-backend-counts*
                                  (make-hash-table :test #'eq)))
                            (handler-case
                                (dispatch-frontend-frame
                                 nil type-byte length-bytes payload)
                              (error (condition) condition))))
                        :name "test malformed frame drain")))
                 (let ((result (sb-thread:join-thread worker :timeout 2)))
                   (check (and (typep result 'pg-error)
                               (fatal-error-p result)
                               (string= (pg-error-sqlstate result) "08P01"))
                          label)
                   (when (sb-thread:thread-alive-p worker)
                     (ignore-errors
                      (sb-thread:interrupt-thread
                       worker (lambda () (error "malformed frame drain timed out"))))
                     (ignore-errors (sb-thread:join-thread worker :timeout 2)))))))
      (set-backend-state backend :in-progress)
      (unwind-protect
           (progn
             (expect-fatal-frame-error
              (char-code #\Q) (u32-to-bytes 3) nil
              "a malformed query frame reaches fatal validation during drain")
             (expect-fatal-frame-error
              (char-code #\D) (u32-to-bytes 5) nil
              "a malformed non-query frame reaches fatal validation during drain")
             (expect-fatal-frame-error
              (char-code #\X) (u32-to-bytes 5) #(0)
              "a malformed Terminate frame reaches fatal validation during drain"))
        (set-backend-state backend old-state)
        (test-reset-live-operation-admission)))))

(defun test-terminate-frame-drains-active-backend ()
  (let* ((backend (make-backend))
         (old-state (backend-state backend)))
    (set-backend-state backend :in-progress)
    (unwind-protect
         (let ((worker
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* backend)
                          (*live-operation-admission-open* nil)
                          (*live-operation-drain-open* t)
                          (*live-operation-drain-backends* (list backend))
                          (*live-operation-cleanup-backends* (list backend))
                          (*live-operation-count* 0)
                          (*live-operation-backend-counts*
                            (make-hash-table :test #'eq)))
                      (dispatch-frontend-frame
                       nil (char-code #\X) (u32-to-bytes 4)
                       (make-array 0 :element-type '(unsigned-byte 8)))))
                  :name "test Terminate frame drain")))
           (let ((result (sb-thread:join-thread worker :timeout 2)))
             (check (eq result :terminate)
                    "an active backend can dispatch Terminate during reload drain")
             (when (sb-thread:thread-alive-p worker)
               (ignore-errors
                (sb-thread:interrupt-thread
                 worker (lambda () (error "Terminate frame drain timed out"))))
               (ignore-errors (sb-thread:join-thread worker :timeout 2)))))
      (set-backend-state backend old-state)
      (test-reset-live-operation-admission))))

(defun test-live-page-items-stop ()
  (let ((page (page-init (make-page)))
        (page-evaluations 0)
        (seen '()))
    (dotimes (i 3)
      (page-add-item page (make-array 8 :element-type '(unsigned-byte 8)
                                        :initial-element i)))
    (page-mark-dead page 2)
    (do-live-page-items ((progn (incf page-evaluations) page) offnum off len)
      (check (and (plusp off) (plusp len)) "live page item has storage")
      (push offnum seen))
    (check (equal (nreverse seen) '(1 3))
           "live page iteration skips dead slots")
    (check (= page-evaluations 1) "live page iterator evaluates PAGE once")
    (flet ((first-live-item ()
             (do-live-page-items (page offnum off len)
               (when (and (plusp off) (plusp len))
                 (return-from first-live-item offnum)))
             nil))
      (check (equal (first-live-item) 1)
             "live page iteration preserves nonlocal early return"))))

(defun test-live-page-item-guard ()
  (let ((page (page-init (make-page)))
        (page-evaluations 0)
        (offnum-evaluations 0))
    (page-add-item page #(1 2 3))
    (multiple-value-bind (off len)
        (with-live-page-item ((progn (incf page-evaluations) page)
                              (progn (incf offnum-evaluations) 1)
                              item-off item-len)
          (values item-off item-len))
      (check (and (plusp off) (plusp len))
             "single live page item binds its tuple location")
      (check (= page-evaluations 1)
             "single live page item evaluates PAGE once")
      (check (= offnum-evaluations 1)
             "single live page item evaluates OFFNUM once"))
    (page-mark-dead page 1)
    (let ((body-ran nil)
          (line-pointer-reads 0))
      (with-live-page-item (page 1 item-off item-len)
        (when (plusp item-off)
          (setf body-ran t)))
      (check (not body-ran)
             "single live page item skips dead line pointers")
      (flet ((page-get-lp (page offnum)
               (declare (ignore page offnum))
               (incf line-pointer-reads)
               (error "out-of-range line pointer was read")))
        (with-live-page-item (page 0 item-off item-len)
          (setf body-ran t))
        (with-live-page-item (page 2 item-off item-len)
          (setf body-ran t)))
      (check (and (not body-ran) (zerop line-pointer-reads))
             "single live page item skips offsets outside the page"))))

(defun test-page-compaction ()
  (let ((p (page-init (make-page))))
    (dotimes (i 64)
      (page-add-item p (make-array 32 :element-type '(unsigned-byte 8)
                                      :initial-element i)))
    (page-mark-dead p 10)
    (page-mark-dead p 20)
    (page-compact p)
    (check (= (page-lp-count p) 64) "compaction preserves LP count")
    (loop for i from 1 to 64
          unless (member i '(10 20))
            do (multiple-value-bind (off flags len) (page-get-lp p i)
                 (declare (ignore off len))
                 (check (= flags +lp-normal+) "compaction keeps live LPs")))))

(defun test-tuple-roundtrip ()
  (let* ((cols (coerce
                (list (make-pg-column :name "a" :type :int4 :typmod -1
                                      :attnum 1 :notnull nil)
                      (make-pg-column :name "b" :type :varchar :typmod 8
                                      :attnum 2 :notnull nil)
                      (make-pg-column :name "c" :type :int8 :typmod -1
                                      :attnum 3 :notnull nil))
                'vector))
         (vals (vector 42 "hello" 1234567890123))
         (bytes (form-heap-tuple vals cols 7 0 1 0 0)))
    (multiple-value-bind (got xmin xmax cid infomask)
        (deform-heap-tuple bytes cols)
      (declare (ignore infomask))
      (check (= xmin 7) "xmin round-trips")
      (check (= xmax 0) "xmax round-trips")
      (check (= cid 1) "cid round-trips")
      (check (= (aref got 0) 42) "int4 round-trips")
      (check (string= (aref got 1) "hello") "varchar round-trips")
      (check (= (aref got 2) 1234567890123) "int8 round-trips")))
  ;; NULLs must survive the round trip too.
  (let* ((cols (coerce
                (list (make-pg-column :name "a" :type :int4 :typmod -1
                                      :attnum 1 :notnull nil)
                      (make-pg-column :name "b" :type :varchar :typmod 8
                                      :attnum 2 :notnull nil))
                'vector))
         (vals (vector +null+ "x"))
         (bytes (form-heap-tuple vals cols 7 0 1 0 0)))
    (multiple-value-bind (got xmin xmax cid infomask)
        (deform-heap-tuple bytes cols)
      (declare (ignore xmin xmax cid infomask))
      (check (eq (aref got 0) +null+) "null int4 round-trips")
      (check (string= (aref got 1) "x") "varchar after null"))))

(defun test-full-xid-tuple-format ()
  (let* ((columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (full-xmin (+ +xid-cycle+ 41))
         (full-xmax (+ +xid-cycle+ 42))
         (bytes (form-heap-tuple (vector 7) columns full-xmin full-xmax 0 0 0)))
    (check (= +size-of-heap-tuple-header+ 23)
           "heap tuple header has PostgreSQL's 23-byte size")
    (check (equal (list +heap-tuple-xmin-off+ +heap-tuple-xmax-off+
                        +heap-tuple-cid-off+ +heap-tuple-ctid-block-off+
                        +heap-tuple-ctid-offnum-off+ +heap-tuple-natts-off+
                        +heap-tuple-infomask-off+ +heap-tuple-hoff-off+)
                  '(0 4 8 12 16 18 20 22))
           "heap tuple header offsets match HeapTupleHeaderData")
    (check (= (page-u32 bytes +heap-tuple-xmin-off+) 41)
           "tuple xmin stores the low 32-bit TransactionId")
    (check (= (page-u32 bytes +heap-tuple-xmax-off+) 42)
           "tuple xmax stores the low 32-bit TransactionId")
    (multiple-value-bind (values xmin xmax)
        (deform-heap-tuple bytes columns)
      (check (= (aref values 0) 7) "wide-XID tuple keeps its attributes")
      (check (= (full-xid-from-32 xmin (+ +xid-cycle+ 100)) full-xmin)
             "tuple xmin reconstructs in the current epoch")
      (check (= (full-xid-from-32 xmax (+ +xid-cycle+ 100)) full-xmax)
             "tuple xmax reconstructs in the current epoch")))
  (check (= (advance-full-xid (1- +xid-cycle+))
            (+ +xid-cycle+ +first-normal-xid+))
         "FullXID advance skips special low XIDs at wrap")
  (check (= (full-xid-from-32 #xFFFFFFFE (+ +xid-cycle+ 3))
            #xFFFFFFFE)
         "tuple XIDs below the new epoch resolve to the prior epoch"))

(defun test-xid-tuple-update-roundtrip ()
  ;; Exercise the heap-level xmin/xmax contract behind the update system-column
  ;; cases in src/test/regress/sql/update.sql, using a later 32-bit XID epoch.
  (let* ((*next-xid* (+ +xid-cycle+ 52))
         (relfilenode 918273646)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (insert-xid (+ +xid-cycle+ 50))
         (update-xid (+ +xid-cycle+ 51)))
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (multiple-value-bind (old-block old-offnum)
             (heapam-insert relfilenode columns (vector 10) insert-xid 0)
           (heapam-set-xmax relfilenode old-block old-offnum update-xid 1)
           (multiple-value-bind (new-block new-offnum)
               (heapam-insert relfilenode columns (vector 11) update-xid 1)
             (check (heapam-set-ctid relfilenode old-block old-offnum
                                     new-block new-offnum)
                    "heap update links the old tuple to its successor")
             (let ((old-tuple (heapam-get-tuple relfilenode old-block
                                                old-offnum columns))
                   (new-tuple (heapam-get-tuple relfilenode new-block
                                                new-offnum columns)))
               (check (and (= (htup-ctid-block old-tuple) new-block)
                           (= (htup-ctid-offnum old-tuple) new-offnum))
                      "heap read preserves the update-chain ctid")
               (check (= (htup-xmin old-tuple) insert-xid)
                      "heap read reconstructs the old row's FullXID xmin")
               (check (= (htup-xmax old-tuple) update-xid)
                      "heap read reconstructs the updating FullXID xmax")
               (check (= (htup-xmin new-tuple) update-xid)
                      "heap read reconstructs the new row's FullXID xmin")
               (check (zerop (htup-xmax new-tuple))
                      "new row has an invalid xmax"))))
      (smgr-forget relfilenode :delete t))))

(defun test-heap-update-chain-follow ()
  ;; PostgreSQL's heap_delete reports TM_Updated and ExecDelete runs
  ;; EvalPlanQual. Model the ctid walk and row-predicate recheck at heap level.
  (let* ((*next-xid* 7205)
         (*active-xids* '(7204))
         (*backend* (make-backend))
         (*current-snapshot* (make-snapshot-struct :xmin 7200 :xmax 7205
                                                   :xip '() :curcid 10))
         (relfilenode 918273647)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (tids '()))
    (dolist (xid '(7200 7201 7202 7203))
      (clog-set xid +clog-committed+))
    (clog-set 7204 +clog-in-progress+)
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (progn
           (let ((previous nil))
             (dolist (spec '((7200 10) (7201 20) (7202 30) (7203 40)))
               (destructuring-bind (xmin value) spec
                 (multiple-value-bind (block offnum)
                     (heapam-insert relfilenode columns (vector value) xmin 0)
                   (when previous
                     (heapam-set-xmax relfilenode (first previous)
                                      (second previous) xmin 0)
                     (heapam-set-ctid relfilenode (first previous)
                                      (second previous) block offnum))
                   (push (cons block offnum) tids)
                   (setf previous (list block offnum))))))
           (setf tids (nreverse tids))
           (multiple-value-bind (deleted tuple block offnum)
               (heapam-delete-tid relfilenode columns
                                  (caar tids) (cdar tids) 7204 1
                                  (lambda (candidate)
                                    (= (aref (htup-values candidate) 0) 40)))
             (check deleted "update chain reaches and matches latest tuple")
             (check (= (aref (htup-values tuple) 0) 40)
                    "update-chain recheck returns the latest row values")
             (check (equal (cons block offnum) (car (last tids)))
                    "update-chain delete marks the latest tuple TID")
             (check (= (htup-xmin tuple) 7203)
                    "update chain can span several committed XIDs")
             (check (= (htup-xmax (heapam-get-tuple
                                   relfilenode block offnum columns))
                       7204)
                    "only the latest tuple receives the new xmax")))
      (release-xid 7204)
      (smgr-forget relfilenode :delete t))))

(defun test-heap-free-space-map ()
  (let ((fsm (make-heap-fsm)))
    (heap-fsm-record-page fsm 4 300)
    (heap-fsm-record-page fsm 6 160)
    (check (= (heap-fsm-take-page fsm 150) 6)
           "heap FSM selects the smallest fitting space category")
    (heap-fsm-record-page fsm 6 64)
    (check (= (heap-fsm-take-page fsm 250) 4)
           "heap FSM refreshes a page's space category")
    (check (null (heap-fsm-take-page fsm 250))
           "heap FSM skips pages below the requested space")
    (check (not (gethash 4 (heap-fsm-page-space fsm)))
           "taking a page removes its old category entry")
    (heap-fsm-record-page fsm 8 200)
    (check (= (heap-fsm-take-page fsm 200) 8)
           "heap FSM preserves exact-fit pages at category boundaries"))
  ;; The heap path should reuse the current page rather than extending a page
  ;; for every INSERT.
  (let* ((relfilenode 918273648)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (*backend* (make-backend)))
    (ensure-directories-exist (relation-file-path relfilenode))
    (unwind-protect
         (progn
           (dotimes (i 100)
             (heapam-insert relfilenode columns (vector i) 7200 0))
           (check (= (buf-nblocks relfilenode) 1)
                  "heap inserts reuse a page found through the FSM")
           (heap-fsm-forget relfilenode)
           (heapam-insert relfilenode columns #(100) 7200 0)
           (check (= (buf-nblocks relfilenode) 1)
                  "first insert rebuilds the FSM from existing heap pages"))
      (smgr-forget relfilenode :delete t))))

(defun test-postgres-special-xid-statuses ()
  ;; Port the status cases from src/test/regress/sql/xid.sql. PostgreSQL's
  ;; TransactionLogFetch treats bootstrap/frozen as committed and invalid as
  ;; aborted. This Lisp check also uses the active-XID set to distinguish
  ;; in-progress XIDs from abandoned zero-status entries.
  (let* ((committed 7021)
         (aborted 7022)
         (active 7023)
         (abandoned 7024)
         (*active-xids* (list active)))
    (clog-set committed +clog-committed+)
    (clog-set aborted +clog-aborted+)
    (clog-set active +clog-in-progress+)
    (check (xid-committed-p committed) "committed XID status is preserved")
    (check (xid-aborted-p aborted) "aborted XID status is preserved")
    (check (xid-in-progress-p active) "active XID status is in progress")
    (check (xid-aborted-p abandoned)
           "inactive zero-status XID is treated as abandoned")
    (check (xid-committed-p +bootstrap-xid+)
           "bootstrap XID is always committed")
    (check (xid-committed-p +frozen-xid+)
           "frozen XID is always committed")
    (check (not (xid-aborted-p +bootstrap-xid+))
           "bootstrap XID is never aborted")
    (check (not (xid-aborted-p +frozen-xid+))
           "frozen XID is never aborted")
    (check (xid-aborted-p +invalid-xid+)
           "invalid XID is treated as aborted")))

(defun test-xid-control-state ()
  (let* ((*pgdata* (merge-pathnames "xid-state/" *pgdata*))
         (control (control-path))
         (temporary (pgdata-path "global/pg_control.tmp"))
         (clog (clog-path)))
    (check (= (load-next-xid) +first-normal-xid+)
           "missing XID control state initializes an empty cluster")
    (ensure-directories-exist clog)
    (with-open-file (stream clog
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (write-byte 0 stream))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "missing XID control state is rejected when persistent state exists")
    (ensure-directories-exist control)
    ;; An empty file models a crash that truncated pg_control in place.
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-string "" stream))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "truncated XID control state is rejected")
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (format stream "not-an-xid~%"))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "invalid XID control state is rejected")
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (format stream "~a~%" (+ +first-normal-xid+ 100)))
    (check (= (load-next-xid) (+ +first-normal-xid+ 100))
           "valid XID control state loads exactly")
    (let ((fsync-count 0)
          (*xid-batch-end* (+ +first-normal-xid+ 200)))
      (save-next-xid
       :fsync-function
       (lambda (fd)
         (incf fsync-count)
         (sb-posix:fsync fd))
       :fsync-directory-function
       (lambda (directory)
         (incf fsync-count)
         (fsync-directory directory)))
      (check (= (load-next-xid) *xid-batch-end*)
             "XID control save installs the new boundary")
      (check (= fsync-count 2)
             "XID control save fsyncs its file and parent directory")
      (check (not (probe-file temporary))
             "successful XID control save removes its temporary file")
      (let ((*xid-batch-end* (+ +first-normal-xid+ 300)))
        (check (handler-case
                   (progn
                     (save-next-xid
                      :rename-function
                      (lambda (from to)
                        (declare (ignore from to))
                        (error "injected XID control rename failure")))
                     nil)
                 (error () t))
               "XID control rename errors are reported")
        (check (= (load-next-xid) (+ +first-normal-xid+ 200))
               "failed XID control replacement preserves prior state")
        (check (not (probe-file temporary))
               "failed XID control save removes its temporary file")))))

(defun test-data-row-first-column (payload)
  (when (and (>= (length payload) 6)
             (plusp (bytes-to-u16 payload 0)))
    (let ((length (bytes-to-u32 payload 2)))
      (unless (= length #xFFFFFFFF)
        (when (<= (+ 6 length) (length payload))
          (bytes->string payload :start 6 :end (+ 6 length)))))))

(defun test-show-timezone-values (path)
  (with-open-file (stream path
                          :direction :input
                          :element-type '(unsigned-byte 8))
    (loop for message = (test-read-frontend-message stream)
          while message
          when (char= (frontend-message-type message) #\D)
            collect (test-data-row-first-column
                     (frontend-message-payload message)))))

(defun test-session-setting-rollback ()
  (let ((*backend* (make-backend))
        (path (merge-pathnames "session-settings.out" *pgdata*)))
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream
                         (concatenate 'string
                                      "SET TIME ZONE 'Europe/Warsaw'; "
                                      "SHOW timezone; "
                                      "BEGIN; "
                                      "SET TIME ZONE 'America/New_York'; "
                                      "SHOW timezone; "
                                      "ROLLBACK; "
                                      "SHOW timezone; "
                                      "RESET timezone; "
                                      "SHOW timezone;")))
    (check (equal (test-show-timezone-values path)
                  '("Europe/Warsaw" "America/New_York" "Europe/Warsaw" "UTC"))
           "SHOW reports SET values and the value restored by ROLLBACK")
    (check (not (nth-value 1 (gethash "timezone"
                                      (backend-settings *backend*))))
           "RESET TIME ZONE returns the backend to its default")
    (check (eq (backend-state *backend*) :idle)
           "ROLLBACK leaves the backend outside a transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :append
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "SET TIME ZONE 'Europe/Warsaw';"))
    (let ((caught nil))
      (handler-case
          (exec-set nil '(:name "timezone" :value "Invalid/NoSuchZone"))
        (pg-error (condition)
          (setf caught condition)))
      (check (and caught
                  (string= (pg-error-sqlstate caught) "22023"))
             "SET TIME ZONE rejects missing zoneinfo names")
      (check (string= (gethash "timezone" (backend-settings *backend*))
                      "Europe/Warsaw")
             "rejected time zone preserves the previous session setting"))))

(defun test-xid-wraparound-freezes-tuples ()
  ;; Deterministic one-cycle counterpart to xid_wraparound/t/003_wraparounds.pl.
  (let* ((*backend* (make-backend))
         (*catalog* (make-catalog))
         (*next-xid* (+ +xid-cycle+ +first-normal-xid+))
         (*xid-batch-end* *next-xid*)
         (*active-xids* '())
         (*active-snapshots* 0)
         (*xid-wrap-pending* nil)
         (*xid-wrap-completed-through* nil)
         (relfilenode 918273645)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (db (make-database :name "postgres" :oid 1))
         (table (make-pg-class :oid 2 :name "wrap_rows"
                               :relfilenode relfilenode
                               :columns columns :indexes nil))
         (page (page-init (make-page))))
    (setf (gethash "postgres" (catalog-databases *catalog*)) db
          (gethash "wrap_rows" (database-tables db)) table)
    (ensure-directories-exist (relation-file-path relfilenode))
    (dolist (spec '((7 0 :committed nil)
                    (7 8 :committed :committed)
                    (9 0 :aborted nil)
                    (7 10 :committed :aborted)))
      (destructuring-bind (xmin xmax xmin-status xmax-status) spec
        (when xmin-status
          (clog-set xmin (ecase xmin-status
                           (:committed +clog-committed+)
                           (:aborted +clog-aborted+))))
        (when xmax-status
          (clog-set xmax (ecase xmax-status
                           (:committed +clog-committed+)
                           (:aborted +clog-aborted+))))
        (page-add-item page
                       (form-heap-tuple (vector 1) columns xmin xmax 0 0 0))))
    (unwind-protect
         (progn
           (buf-extend relfilenode page)
           (complete-xid-wraparound)
           (let ((frozen-page (buf-read relfilenode 0)))
             (loop for expected-flags in (list +lp-normal+ +lp-unused+
                                               +lp-unused+ +lp-normal+)
                   for expected-xmin in '(2 nil nil 2)
                   for expected-xmax in '(0 nil nil 0)
                   for offnum from 1
                   do (multiple-value-bind (off flags len)
                          (page-get-lp frozen-page offnum)
                        (declare (ignore len))
                        (check (= flags expected-flags)
                               "wraparound prunes old dead tuples")
                        (when (= flags +lp-normal+)
                          (check (= (page-u32 frozen-page
                                              (+ off +heap-tuple-xmin-off+))
                                    expected-xmin)
                                 "wraparound freezes old xmin status")
                          (check (= (page-u32 frozen-page
                                              (+ off +heap-tuple-xmax-off+))
                                    expected-xmax)
                                 "wraparound clears old xmax status")))))
           (let ((snapshot (make-snapshot-struct :xmin +first-normal-xid+
                                                 :xmax *next-xid*
                                                 :xip '() :curcid 10)))
             (loop for expected-visible in '(t nil nil t)
                   for offnum from 1
                   for tuple = (heapam-get-tuple relfilenode 0 offnum columns)
                   do (if expected-visible
                          (check (and tuple
                                      (heap-tuple-visible-p
                                       (htup-xmin tuple) (htup-xmax tuple)
                                       (htup-cid tuple) snapshot nil))
                                 "pre-wrap tuple visibility survives freezing")
                          (check (null tuple)
                                 "wraparound removes no-longer-visible tuples"))))
           (check (= (clog-get 7) +clog-in-progress+)
                  "wraparound resets the old CLOG cycle")
           (check (xid-aborted-p 7)
                  "cleared CLOG IDs are abandoned when no longer active")
           (check (not (xid-aborted-p +frozen-xid+))
                  "frozen XID is not aborted after CLOG reset")
           (check (not (xid-aborted-p +bootstrap-xid+))
                  "bootstrap XID is not aborted after CLOG reset")
           (check (eq (unique-conflict-status
                       (make-htup :xmin +frozen-xid+ :xmax 0) nil)
                      :violation)
                  "frozen live tuples still block duplicate unique keys")
           (check (eq (unique-conflict-status
                       (make-htup :xmin +frozen-xid+
                                  :xmax +frozen-xid+)
                       nil)
                      :free)
                  "frozen committed deletes free unique keys")
           (check (not (probe-file (xid-wrap-marker-path)))
                  "completed wraparound removes its recovery marker")
           ;; Replay after a crash that left the durable marker behind even
           ;; though freezing and the CLOG reset had already completed.
           (save-xid-wrap-marker *next-xid*)
           (setf *xid-wrap-pending* t
                 *xid-wrap-completed-through* nil)
           (complete-xid-wraparound)
           (check (not (probe-file (xid-wrap-marker-path)))
                  "wraparound marker replay completes idempotently")
           (check (null *xid-wrap-pending*)
                  "completed wraparound releases the XID allocation gate")
           ;; A cancellation delivered by the marker unlink must be reported
           ;; only after the completed state has opened the XID gate.
           (save-xid-wrap-marker *next-xid*)
           (setf *xid-wrap-pending* t
                 *xid-wrap-completed-through* nil)
           (let ((cancel-error nil)
                 (shutdown-count 0))
             (handler-case
                 (complete-xid-wraparound
                  :delete-marker-function
                  (lambda ()
                    (delete-xid-wrap-marker)
                    (setf (backend-cancel-pending *backend*) t))
                  :shutdown-function
                  (lambda () (incf shutdown-count)))
               (pg-error (condition)
                 (setf cancel-error condition)))
             (check (and cancel-error
                         (string= (pg-error-sqlstate cancel-error) "57014"))
                    "post-marker cancellation is an ordinary query cancel")
             (check (zerop shutdown-count)
                    "post-marker cancellation does not request server shutdown")
             (check (and (not *xid-wrap-pending*)
                         (eql *xid-wrap-completed-through* *next-xid*))
                    "post-marker cancellation publishes the completed transition")
             (check (not (probe-file (xid-wrap-marker-path)))
                    "post-marker cancellation leaves no recovery marker"))
           (let ((expected-xid *next-xid*))
             (check (= (assign-xid) expected-xid)
                    "new-epoch XIDs can be allocated after freezing")))
      (smgr-forget relfilenode :delete t))))

(defun test-prepare-wraparound-fixture (relfilenode)
  (smgr-forget relfilenode :delete t)
  (ignore-errors (delete-file (xid-wrap-marker-path)))
  (ignore-errors (delete-file (pgdata-path "global/pg_xid_wrap_pending.tmp")))
  (let* ((catalog (make-catalog))
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (db (make-database :name "postgres" :oid 1))
         (table (make-pg-class :oid 2 :name "wrap_fault_rows"
                               :relfilenode relfilenode
                               :columns columns :indexes nil))
         (boundary (+ +xid-cycle+ +first-normal-xid+)))
    (setf (gethash "postgres" (catalog-databases catalog)) db
          (gethash "wrap_fault_rows" (database-tables db)) table
          *next-xid* boundary
          *xid-batch-end* boundary
          *active-xids* '()
          *active-snapshots* 0
          *xid-wrap-pending* nil
          *xid-wrap-failure* nil
          *xid-wrap-completed-through* nil)
    ;; XID allocation persisted this reserved batch boundary before wrap.
    (save-next-xid)
    (clog-reset)
    (clog-set 7 +clog-committed+)
    (ensure-directories-exist (relation-file-path relfilenode))
    (dotimes (block 2)
      (let ((page (page-init (make-page))))
        (page-add-item page (form-heap-tuple (vector (1+ block)) columns 7 0 0 0 0))
        (buf-extend relfilenode page)))
    catalog))

(defun test-restart-wraparound-fixture (relfilenode)
  ;; Reopen heap and CLOG files and rebuild XID state from durable storage.
  (smgr-forget relfilenode)
  (when *clog-stream*
    (close *clog-stream*)
    (setf *clog-stream* nil))
  (setf *active-xids* '()
        *active-snapshots* 0
        *xid-wrap-pending* nil
        *xid-wrap-failure* nil
        *xid-wrap-completed-through* nil)
  (xact-init)
  (complete-xid-wraparound))

(defun test-xid-wraparound-fault-recovery ()
  (let ((*backend* nil)
        (*catalog* nil)
        (*next-xid* *next-xid*)
        (*xid-batch-end* *xid-batch-end*)
        (*active-xids* *active-xids*)
        (*active-snapshots* *active-snapshots*)
        (*xid-wrap-pending* *xid-wrap-pending*)
        (*xid-wrap-failure* *xid-wrap-failure*)
        (*xid-wrap-completed-through* *xid-wrap-completed-through*)
        (relfilenode 918273646))
    (labels ((run-failure (name inject expected-marker-p
                           &optional (restart-marker-p expected-marker-p))
               (let ((catalog (test-prepare-wraparound-fixture relfilenode))
                     (boundary (+ +xid-cycle+ +first-normal-xid+))
                     (shutdown-count 0))
                 (setf *catalog* catalog)
                 (check (handler-case
                            (progn
                              (funcall inject
                                       (lambda () (incf shutdown-count)))
                              nil)
                          (error () t))
                        (format nil "~a failure is reported" name))
                 (check (and *xid-wrap-pending* *xid-wrap-failure*)
                        (format nil "~a keeps the XID gate closed" name))
                 (check (= shutdown-count 1)
                        (format nil "~a requests server shutdown" name))
                 (check (eql (not (null (probe-file (xid-wrap-marker-path))))
                             expected-marker-p)
                        (format nil "~a leaves the expected recovery marker state"
                                name))
                 (when (string= name "partial relation freeze")
                   (let* ((first-page (buf-read relfilenode 0))
                          (second-page (buf-read relfilenode 1))
                          (first-off (nth-value 0 (page-get-lp first-page 1)))
                          (second-off (nth-value 0 (page-get-lp second-page 1))))
                     (check (= (page-u32 first-page
                                         (+ first-off +heap-tuple-xmin-off+))
                               +frozen-xid+)
                            "freeze fault leaves the first page partially frozen")
                     (check (= (page-u32 second-page
                                         (+ second-off +heap-tuple-xmin-off+))
                               7)
                            "freeze fault leaves the next page untouched")))
                 (check (handler-case
                            (progn (wait-for-xid-wraparound (make-backend)) nil)
                          (pg-error (condition)
                            (string= (pg-error-severity condition) "FATAL")))
                        (format nil "~a rejects sessions at the failed gate" name))
                 ;; A directory-fsync failure can leave either rename
                 ;; outcome after a crash. Exercise both marker states using
                 ;; a startup reload of the durable XID boundary and CLOG.
                 (if restart-marker-p
                     (save-xid-wrap-marker boundary)
                     (ignore-errors (delete-file (xid-wrap-marker-path))))
                 (test-restart-wraparound-fixture relfilenode)
                 (check (= *next-xid* boundary)
                        (format nil "~a restart reloads the XID boundary" name))
                 (check (not *xid-wrap-pending*)
                        (format nil "~a recovery opens the gate" name))
                 (check (not (probe-file (xid-wrap-marker-path)))
                        (format nil "~a recovery removes the marker" name))
                 (dotimes (block 2)
                   (let* ((page (buf-read relfilenode block))
                          (off (nth-value 0 (page-get-lp page 1))))
                     (check (= (page-u32 page (+ off +heap-tuple-xmin-off+))
                               +frozen-xid+)
                            (format nil "~a recovery freezes page ~d" name block)))))))
      (run-failure
       "control file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :fsync-function (lambda (fd)
                               (declare (ignore fd))
                               (error "injected control file fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "control file rename"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :rename-function (lambda (from to)
                                (declare (ignore from to))
                                (error "injected control file rename failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "control file directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected control directory fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-function (lambda (fd)
                               (declare (ignore fd))
                               (error "injected marker file fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker rename"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :rename-function (lambda (from to)
                                (declare (ignore from to))
                                (error "injected marker rename failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker directory fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "marker directory fsync with rename lost"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker directory fsync failure"))))
          :shutdown-function shutdown))
       t nil)
      (run-failure
       "partial relation freeze"
       (lambda (shutdown)
         (complete-xid-wraparound
          :freeze-function
          (lambda (old-epoch)
            (freeze-all-heap-xids
             old-epoch
             :after-page-function
             (lambda (relation block)
               (declare (ignore relation))
               (when (zerop block)
                 (error "injected partial relation freeze failure")))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "relation fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :freeze-function
          (lambda (old-epoch)
            (freeze-all-heap-xids
             old-epoch
             :fsync-function (lambda (relation)
                               (declare (ignore relation))
                               (error "injected relation fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "CLOG file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :reset-clog-function
          (lambda (&key fsync-function fsync-directory-function)
            (declare (ignore fsync-function fsync-directory-function))
            (clog-reset
             :fsync-function (lambda (stream)
                               (declare (ignore stream))
                               (error "injected CLOG file fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "CLOG directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :reset-clog-function
          (lambda (&key fsync-function fsync-directory-function)
            (declare (ignore fsync-function fsync-directory-function))
            (clog-reset
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected CLOG directory fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "marker removal directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :delete-marker-function
          (lambda (&key fsync-directory-function)
            (declare (ignore fsync-directory-function))
            (delete-xid-wrap-marker
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker removal directory fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker removal directory fsync with unlink lost"
       (lambda (shutdown)
         (complete-xid-wraparound
          :delete-marker-function
          (lambda (&key fsync-directory-function)
            (declare (ignore fsync-directory-function))
            (delete-xid-wrap-marker
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker removal directory fsync failure"))))
          :shutdown-function shutdown))
       nil t))))

(defun test-xid-wraparound-coordinator-cancel ()
  (let* ((relfilenode 918273647)
         (*backend* nil)
         (*catalog* nil)
         (old-next-xid *next-xid*)
         (old-batch-end *xid-batch-end*)
         (old-active-xids *active-xids*)
         (old-active-snapshots *active-snapshots*)
         (old-wrap-pending *xid-wrap-pending*)
         (old-wrap-failure *xid-wrap-failure*)
         (old-completed-through *xid-wrap-completed-through*)
         (pgdata *pgdata*)
         (coordinator (make-backend))
         (waiter (make-backend))
         (coordinator-waiting nil)
         (coordinator-error nil)
         (waiter-error nil)
         (shutdown-count 0)
         (catalog nil)
         (coordinator-thread nil)
         (waiter-thread nil))
    (unwind-protect
         (progn
           (setf catalog (test-prepare-wraparound-fixture relfilenode))
           ;; Keep the active-XID state global so both SBCL threads observe it.
           (setf *active-xids* '(77))
           (setf coordinator-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* coordinator)
                          (*catalog* catalog)
                          (*pgdata* pgdata))
                      (handler-case
                          (complete-xid-wraparound
                           :wait-for-xids-function
                           (lambda ()
                             (setf coordinator-waiting t)
                             (wait-for-no-active-xids))
                           :shutdown-function
                           (lambda () (incf shutdown-count)))
                        (error (condition)
                          (setf coordinator-error condition)))))))
           (check (test-wait-until (lambda () coordinator-waiting))
                  "wraparound coordinator waits for active XIDs")
           (setf waiter-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* waiter)
                          (*catalog* catalog)
                          (*pgdata* pgdata))
                      (handler-case
                          (prepare-xid-wraparound-for-statement waiter)
                        (error (condition)
                          (setf waiter-error condition)))))))
           (check (test-wait-until
                   (lambda () (backend-wraparound-waiting waiter)))
                  "another backend waits on the wraparound gate")
           (setf (backend-cancel-pending coordinator) t)
           (check (test-wait-until
                   (lambda () (and coordinator-error waiter-error)))
                  "coordinator cancellation releases waiting backends")
           (check (and (typep coordinator-error 'pg-error)
                       (string= (pg-error-sqlstate coordinator-error) "XX001"))
                  "failed coordinator receives a fatal wraparound error")
           (check (and (typep waiter-error 'pg-error)
                       (string= (pg-error-sqlstate waiter-error) "XX001"))
                  "waiting backend receives a fatal wraparound error")
           (check (and (typep *xid-wrap-failure* 'pg-error)
                       (string= (pg-error-sqlstate *xid-wrap-failure*) "57014"))
                  "wraparound failure records the original cancellation")
           (check (= shutdown-count 1)
                  "wraparound cancellation requests server shutdown once"))
      (setf *active-xids* '())
      (broadcast-xact)
      (sb-thread:with-mutex (*xid-wrap-lock*)
        (sb-thread:condition-broadcast *xid-wrap-cv*))
      (dolist (thread (list coordinator-thread waiter-thread))
        (when (and thread (sb-thread:thread-alive-p thread))
          (ignore-errors (sb-thread:join-thread thread :timeout 2 :default nil))))
      (ignore-errors (delete-file (xid-wrap-marker-path)))
      (smgr-forget relfilenode :delete t)
      (setf *next-xid* old-next-xid
            *xid-batch-end* old-batch-end
            *active-xids* old-active-xids
            *active-snapshots* old-active-snapshots
            *xid-wrap-pending* old-wrap-pending
            *xid-wrap-failure* old-wrap-failure
            *xid-wrap-completed-through* old-completed-through))))

(defun test-key-order ()
  (flet ((k (v ty) (encode-index-key (list v) (list ty))))
    (check (minusp (key-compare (k -1 :int4) (k 0 :int4))) "int4 order")
    (check (minusp (key-compare (k -2147483648 :int4) (k 2147483647 :int4)))
           "int4 extremes")
    (check (minusp (key-compare (k -1 :int8) (k 0 :int8))) "int8 order")
    (check (minusp (key-compare (k +null+ :int4) (k -2147483648 :int4)))
           "null sorts first")
    (check (minusp (key-compare (k "abc" :varchar) (k "abd" :varchar)))
           "varchar order")
    ;; Index keys carry a length prefix, so ordering is length-first.  This
    ;; is internal only: SQL comparisons and ORDER BY use compare-3way.
    (check (minusp (key-compare (k "b" :varchar) (k "aa" :varchar)))
           "varchar key encoding is length-first")))

(defun test-index-catalog-roundtrip ()
  (let* ((idx (make-pg-index :oid 42 :name "nnd" :relfilenode 42
                             :table-oid 41 :columns '(1) :unique-p t
                             :constraint-p t
                             :nulls-not-distinct-p t))
         (sexp (index-to-sexp idx))
         (copy (index-from-sexp sexp)))
    (check (equal sexp
                  '(:oid 42 :name "nnd" :relfilenode 42 :table-oid 41
                    :columns (1) :unique t :primary nil :constraint t
                    :nulls-not-distinct t))
           "index catalog encoding is the compatible flat plist")
    (check (pg-index-nulls-not-distinct-p copy)
           "NULLS NOT DISTINCT survives catalog serialization")
    (check (pg-index-constraint-p copy)
           "constraint ownership survives catalog serialization"))
  (let* ((class (make-pg-class
                 :oid 50 :name "codec" :relfilenode 51
                 :columns (vector (make-pg-column :name "value" :type :int4
                                                  :typmod -1 :attnum 1
                                                  :notnull t))
                 :indexes '(52)))
         (sexp (class-to-sexp class))
         (copy (class-from-sexp sexp)))
    (check (equal sexp
                  `(:oid 50 :name "codec" :relfilenode 51
                    :relfrozenxid ,+first-normal-xid+
                    :columns ((:name "value" :type :int4 :typmod -1
                               :attnum 1 :notnull t))
                    :indexes (52)))
           "class and column codecs use compatible flat plists")
    (check (vectorp (pg-class-columns copy))
           "class codec restores columns as a vector")
    (check (string= (pg-column-name (aref (pg-class-columns copy) 0)) "value")
           "class codec round-trips column mappings"))
  (let ((legacy-pk (index-from-sexp '(:primary t))))
    (check (not (pg-index-constraint-p legacy-pk))
           "legacy catalog has no constraint ownership flag")
    (check (pg-index-constraint-owned-p legacy-pk)
           "legacy primary index remains constraint-owned")))

(defun test-sql-strict-let-evaluation ()
  (let ((evaluations '()))
    (check (eq +null+
               (sql-strict-let ((left (progn (push :left evaluations) +null+))
                                (right (progn (push :right evaluations) 7)))
                 :body-must-not-run))
           "strict let returns NULL when any binding is NULL")
    (check (equal (nreverse evaluations) '(:left :right))
           "strict let evaluates every binding once before NULL check"))
  (let ((evaluations 0))
    (check (= 9
              (sql-strict-let ((left (progn (incf evaluations) 4))
                               (right (progn (incf evaluations) 5)))
                (+ left right)))
           "strict let returns body result for non-NULL bindings")
    (check (= evaluations 2) "strict let bindings run once each")))

(defun test-error-cleanup-and-fatal-rethrow ()
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (cleanup-count 0)
         (caught nil))
    (handler-case
        (with-error-cleanup (condition (incf cleanup-count))
          (error fatal))
      (pg-error (condition)
        (setf caught condition)))
    (check (= cleanup-count 1) "error cleanup runs before rethrow")
    (check (eq caught fatal) "error cleanup re-signals the original condition"))
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (handler-ran nil)
         (caught nil))
    (handler-case
        (handler-case-nonfatal
         (error fatal)
         (condition
          (setf handler-ran (typep condition 'error))))
      (pg-error (condition)
        (setf caught condition)))
    (check (not handler-ran) "nonfatal handler skips FATAL errors")
    (check (eq caught fatal) "nonfatal handler re-signals the original FATAL"))
  (check (eq :handled
             (handler-case-nonfatal
              (error "test nonfatal")
              (condition
               (if (typep condition 'error) :handled :unexpected))))
         "nonfatal handler handles ordinary errors"))

(defun test-create-table-cleanup-after-fsync-error ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend))
         (db (make-database :name "postgres" :oid 1))
         (original-fsync-index (symbol-function 'fsync-index))
         (caught nil))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) db)
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-index)
                 (lambda (index)
                   (declare (ignore index))
                   (error "injected final index fsync failure")))
           (handler-case
               (exec-create-table
                nil
                '(:name "cleanup_test"
                  :columns ((:name "value" :type :int4 :typmod -1 :unique t))))
             (error (condition)
               (setf caught condition)))
           (check (typep caught 'error)
                  "CREATE TABLE re-signals final index fsync failure")
           (check (null (gethash "cleanup_test" (database-tables db)))
                  "CREATE TABLE cleanup removes table after final fsync failure"))
      (setf (symbol-function 'fsync-index) original-fsync-index))))

(defun test-relation-build-own-xid ()
  (let ((relfilenode 987653)
        (xid 987654))
    (setf (gethash relfilenode *relation-writers*) (list xid))
    (let ((token (acquire-relation-build-lock relfilenode xid)))
      (check (eq token (gethash relfilenode *relation-builders*))
             "relation build ignores its own writer XID")
      (release-relation-build-lock relfilenode token))
    (remhash relfilenode *relation-writers*)))

(defun test-visibility ()
  (let ((snap (make-snapshot-struct :xmin 10 :xmax 1000 :xip '() :curcid 5)))
    ;; Committed insert, never deleted -> visible.
    (clog-set 100 +clog-committed+)
    (check (heap-tuple-visible-p 100 0 0 snap nil) "committed insert visible")
    ;; Aborted insert -> invisible.
    (clog-set 101 +clog-aborted+)
    (check (not (heap-tuple-visible-p 101 0 0 snap nil)) "aborted insert hidden")
    ;; Committed insert, committed delete -> invisible.
    (clog-set 102 +clog-committed+)
    (clog-set 103 +clog-committed+)
    (check (not (heap-tuple-visible-p 102 103 0 snap nil))
           "committed delete hides tuple")
    ;; Committed insert, in-progress delete still in our snapshot -> visible.
    (clog-set 104 +clog-committed+)
    (clog-set 105 +clog-in-progress+)
    (let ((snap2 (make-snapshot-struct :xmin 10 :xmax 1000
                                       :xip '(105) :curcid 5)))
      (check (heap-tuple-visible-p 104 105 0 snap2 nil)
             "concurrent delete leaves tuple visible"))
    ;; Own-xid rule: visible only if inserted before the current command.
    (let ((snap3 (make-snapshot-struct :xmin 10 :xmax 1000 :xip '() :curcid 5)))
      (check (heap-tuple-visible-p 200 0 3 snap3 200)
             "own insert visible for later command")
      (check (not (heap-tuple-visible-p 200 0 5 snap3 200))
             "own insert invisible in same command"))))

(defun test-vacuum-snapshot-horizon ()
  ;; Port the permanent-table horizon case from
  ;; src/test/isolation/specs/horizons.spec. The Lisp executor currently keeps
  ;; snapshots per statement, so register the older snapshot directly here.
  (let* ((*backend* (make-backend))
         (*next-xid* 7303)
         (*active-xids* '())
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*wal-ready* nil)
         (heap-relfilenode 918273651)
         (index-relfilenode 918273652)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (key (encode-index-key '(1) '(:int4)))
         (snapshot nil)
         (page (page-init (make-page)))
         (offset (page-add-item
                  page (form-heap-tuple #(1) columns 7301 0 0 0 1)))
         (snapshot-active nil)
         (index-cleanup-before-heap-reuse nil))
    ;; The snapshot starts after the insert but before the committed delete.
    (smgr-forget heap-relfilenode :delete t)
    (smgr-forget index-relfilenode :delete t)
    (clog-set 7301 +clog-committed+)
    (unwind-protect
         (progn
           (ensure-directories-exist (relation-file-path heap-relfilenode))
           (check (= (buf-extend heap-relfilenode page) 0)
                  "vacuum horizon fixture creates heap block zero")
           (btree-init index-relfilenode)
           (btree-insert index-relfilenode key 0 offset)
           (note-snapshot-start)
           (setf snapshot-active t
                 snapshot (make-snapshot 1 :register t)
                 *next-xid* 7304)
           (with-write-lock
             (let ((heap-page (buf-read heap-relfilenode 0)))
               (multiple-value-bind (tuple-off flags len)
                   (page-get-lp heap-page offset)
                 (declare (ignore flags len))
                 (page-set-u32 heap-page
                               (+ tuple-off +heap-tuple-xmax-off+) 7303)
                 (buf-write heap-relfilenode 0 heap-page))))
           (clog-set 7303 +clog-committed+)
           (check (= (snapshot-xmax snapshot) 7303)
                  "snapshot treats the later delete XID as in the future")
           (check (= (oldest-xmin) 7303)
                  "registered old snapshot holds the vacuum removal horizon")
           (heap-vacuum-relation heap-relfilenode (list index-relfilenode)
                                 (oldest-xmin) 7300)
           (multiple-value-bind (off flags len)
               (page-get-lp (buf-read heap-relfilenode 0) offset)
             (declare (ignore off len))
             (check (= flags +lp-normal+)
                    "vacuum preserves a deleted tuple visible to the old snapshot"))
           (let ((tuple (heapam-get-tuple heap-relfilenode 0 offset columns)))
             (check (and tuple
                         (heap-tuple-visible-p
                          (htup-xmin tuple) (htup-xmax tuple) (htup-cid tuple)
                          snapshot nil))
                    "old snapshot can still see the tuple after vacuum")
             (check (equal (btree-search index-relfilenode key)
                           (list (cons 0 offset)))
                    "vacuum retains the tuple's index TID behind the old horizon"))
           (note-snapshot-end snapshot)
           (setf snapshot-active nil)
           (check (= (oldest-xmin) 7304)
                  "releasing the old snapshot advances the removal horizon")
           (let ((original-btree-vacuum-delete-tids
                   (symbol-function 'btree-vacuum-delete-tids)))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'btree-vacuum-delete-tids)
                          (lambda (index-relfilenode tids)
                            (funcall original-btree-vacuum-delete-tids
                                     index-relfilenode tids)
                            (multiple-value-bind (off flags len)
                                (page-get-lp (buf-read heap-relfilenode 0) offset)
                              (declare (ignore off len))
                              (setf index-cleanup-before-heap-reuse
                                    (and (null (btree-search
                                                index-relfilenode key))
                                         (= flags +lp-dead+))))))
                    (heap-vacuum-relation heap-relfilenode
                                          (list index-relfilenode)
                                          (oldest-xmin) 7300))
               (setf (symbol-function 'btree-vacuum-delete-tids)
                     original-btree-vacuum-delete-tids)))
           (check index-cleanup-before-heap-reuse
                  "vacuum removes the index TID while its heap slot is still dead")
           (multiple-value-bind (off flags len)
               (page-get-lp (buf-read heap-relfilenode 0) offset)
             (declare (ignore off len))
             (check (= flags +lp-unused+)
                    "vacuum makes the dead heap slot reusable after horizon advance"))
           (check (null (btree-search index-relfilenode key))
                  "vacuum removes dead index TIDs before heap-slot reuse")
           (let* ((fsm (gethash heap-relfilenode *heap-fsms*))
                  (recorded-space (and fsm
                                       (gethash 0 (heap-fsm-page-space fsm))))
                  (page-space
                    (heap-page-insert-capacity (buf-read heap-relfilenode 0))))
             (check (and recorded-space (= recorded-space page-space))
                    "vacuum records reclaimed heap space in the FSM")))
      (when snapshot-active
        (note-snapshot-end snapshot))
      (smgr-forget heap-relfilenode :delete t)
      (smgr-forget index-relfilenode :delete t))))

(defun test-btree-duplicate-keys ()
  ;; Insert many entries with one key so the leaves holding them must split.
  ;; Repeatedly updating an indexed value produces exactly this shape: a long
  ;; run of index entries with equal keys spread over several leaves.
  (let* ((rel 987654)
         (key (encode-index-key (list 42) (list :int4)))
         (n 2000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel key 0 i))
    (let ((found (btree-search rel key)))
      (check (= (length found) n)
             "duplicate keys are found across leaf splits")
      (check (= (length (remove-duplicates found :test #'equal)) n)
             "every duplicate-key TID is distinct")
      (check (null (btree-search rel (encode-index-key (list 43) (list :int4))))
             "an absent key is not found"))))

(defun test-btree-mixed-duplicate-keys ()
  ;; Interleave two keys so splits produce equal separators for both, which
  ;; stresses the key-range order that search assumes.
  (let* ((rel 987655)
         (ka (encode-index-key (list 1) (list :int4)))
         (kb (encode-index-key (list 2) (list :int4)))
         (n 3000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel (if (evenp i) ka kb) 0 i))
    (let ((fa (btree-search rel ka))
          (fb (btree-search rel kb)))
      (check (= (length fa) (/ n 2)) "even entries found (mixed keys)")
      (check (= (length fb) (/ n 2)) "odd entries found (mixed keys)")
      (check (every (lambda (tid) (evenp (cdr tid))) fa)
             "key A search returned only key-A TIDs")
      (check (every (lambda (tid) (oddp (cdr tid))) fb)
             "key B search returned only key-B TIDs"))))

(defun test-parse-error-aborts-transaction ()
  (let ((path (merge-pathnames "parse-error.out" *pgdata*)))
    (setf (backend-state *backend*) :in-progress
          (backend-explicit *backend*) t
          (backend-command-id *backend*) 0)
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select from;"))
    (check (eq (backend-state *backend*) :aborted)
           "a parse error aborts an explicit transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select 1;"))
    (check (eq (backend-state *backend*) :aborted)
           "aborted transaction stays aborted")
    (abort-transaction)
    (check (eq (backend-state *backend*) :idle)
           "abort clears the transaction state")))

(defun run-unit-tests ()
  (setf *test-failures* 0
        *test-checks* 0)
  (let ((dir (merge-pathnames
              (format nil "pg-unit-~a/" (random 1000000))
              #p"/tmp/opencode/")))
    (ensure-directories-exist dir)
    (let ((*pgdata* dir)
          (*backend* (make-backend)))
      (clog-init)
      (test-swank-arguments)
      (check (test-wait-until (lambda () t) 0.01)
             "test wait helper accepts an explicit timeout")
      (test-protocol-packet-limits)
      (test-protocol-decoding-enters-live-operation)
      (test-malformed-protocol-frames-drain-to-fatal-error)
      (test-terminate-frame-drains-active-backend)
      (test-page-roundtrip)
      (test-atomic-output)
      (test-backend-wait-state)
      (test-heap-tuple-edit)
      (test-with-registered-table)
      (test-live-operation-gate)
      (test-live-operation-quiescence-ownership)
      (test-nested-control-repl-reload)
      (test-legacy-live-operation-gate-call-shapes)
      (test-live-operation-entry-interrupt-cleanup)
      (test-transaction-ending-statement-classifier)
      (test-simple-query-terminal-statement-drain)
      (test-cancel-query-waiting-for-live-operation-admission)
      (test-late-cancel-is-cleared-at-query-end)
      (test-query-token-cleans-up-after-publication-error)
      (test-unregister-backend-clears-query-state)
      (test-idle-backend-teardown-enters-captured-cleanup)
      (test-shutdown-cleanup-respects-reload-admission)
      (test-pre-admitted-idle-backend-can-drain-after-starting-transaction)
      (test-active-transaction-suffix-waits-for-reload)
      (test-disconnect-cleanup-drains-active-transaction)
      (test-idle-commit-waits-for-reload)
      (test-idle-active-transaction-does-not-block-reload)
      (test-wal-catalog-codec-and-page-checksums)
      (test-wal-record-codec)
      (test-wal-chunked-catalog-record)
      (test-wal-catalog-terminal-validation-precedes-page-redo)
      (test-wal-segment-rotation)
      (test-wal-checkpoint-skips-obsolete-prefix)
      (test-wal-missing-interior-segment)
      (test-live-page-items-stop)
      (test-live-page-item-guard)
      (test-page-compaction)
      (test-tuple-roundtrip)
      (test-full-xid-tuple-format)
      (test-xid-tuple-update-roundtrip)
      (test-heap-update-chain-follow)
      (test-heap-free-space-map)
      (test-postgres-special-xid-statuses)
      (test-xid-control-state)
      (test-key-order)
      (test-index-catalog-roundtrip)
      (test-sql-strict-let-evaluation)
      (test-error-cleanup-and-fatal-rethrow)
      (test-create-table-cleanup-after-fsync-error)
      (test-relation-build-own-xid)
      (test-visibility)
      (test-vacuum-snapshot-horizon)
      (test-btree-duplicate-keys)
      (test-btree-mixed-duplicate-keys)
      (test-session-setting-rollback)
      (test-parse-error-aborts-transaction)
      (test-xid-wraparound-freezes-tuples)
      (test-xid-wraparound-fault-recovery)
      (test-xid-wraparound-coordinator-cancel))
    (ignore-errors
     (uiop:delete-directory-tree dir :validate t :if-does-not-exist :ignore)))
  (format t "~&unit-tests: ~a checks, ~a failures~%"
          *test-checks* *test-failures*)
  (when (plusp *test-failures*)
    (sb-ext:exit :code 1)))

(run-unit-tests)

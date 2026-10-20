;;;; concurrency-tests.lisp -- live-operation and query lifecycle tests.

(in-package #:pg)

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

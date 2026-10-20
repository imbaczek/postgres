;;;; backend-lifecycle-tests.lisp -- backend cleanup and reload coordination tests.

(in-package #:pg)

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

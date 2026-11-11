(in-package #:pg)

(defvar *ddl-publication-test-state* nil)

(defun ddl-publication-test-restore ()
  (when *ddl-publication-test-state*
    (sb-thread:signal-semaphore (getf *ddl-publication-test-state* :release))
    (dolist (entry (getf *ddl-publication-test-state* :originals))
      (setf (symbol-function (car entry)) (cdr entry)))
    (setf *ddl-publication-test-state* nil))
  :restored)

(defun ddl-publication-test-setup (table-name &optional subscriber-p)
  (when *ddl-publication-test-state*
    (error "DDL publication hooks are already installed"))
  (let* ((symbols '(build-index-entries condition-wait-with-timeout
                    logical-catalog-event-for-ast execute-in-transaction))
         (originals (mapcar (lambda (name) (cons name (symbol-function name)))
                            symbols))
         (entries (cdr (assoc 'build-index-entries originals)))
         (wait (cdr (assoc 'condition-wait-with-timeout originals)))
         (event (cdr (assoc 'logical-catalog-event-for-ast originals)))
         (execute (cdr (assoc 'execute-in-transaction originals)))
         (expected
           (when subscriber-p
             (let ((*backend* (make-backend :database "postgres")))
               (logical-current-schema-contract)))))
    (setf *ddl-publication-test-state*
          (list :originals originals :release (sb-thread:make-semaphore)
                :paused nil :waiting nil :events nil :expected expected))
    (let ((complete nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'build-index-entries)
                   (lambda (&rest args)
                     (when (and (string= table-name (pg-class-name (second args)))
                                (not (getf *ddl-publication-test-state* :paused)))
                       (setf (getf *ddl-publication-test-state* :paused) t)
                       (unless (sb-thread:wait-on-semaphore
                                (getf *ddl-publication-test-state* :release) :timeout 30)
                         (error "DDL publication test release timed out")))
                     (apply entries args)))
             (setf (symbol-function 'condition-wait-with-timeout)
                   (lambda (queue mutex)
                     (when (and *backend* (eq mutex *relation-lock-mutex*)
                                (string= "ddl-publication-waiter"
                                         (or (backend-application-name *backend*) "")))
                       (setf (getf *ddl-publication-test-state* :waiting) t))
                     (funcall wait queue mutex)))
             (setf (symbol-function 'logical-catalog-event-for-ast)
                   (lambda (ast)
                     (let ((bytes (funcall event ast)))
                       (when bytes
                         (push (logical-read-data-string
                                (logical-change-event-payload
                                 (logical-decode-catalog-event bytes)))
                               (getf *ddl-publication-test-state* :events)))
                       bytes)))
             (setf (symbol-function 'execute-in-transaction)
                   (lambda (stream ast)
                     (if (and expected *backend*
                              (string= "ddl-publication-replay"
                                       (or (backend-application-name *backend*) "")))
                         (let ((*logical-event-schema-contract*
                                 (list :schema-before expected :schema-after expected)))
                           (funcall execute stream ast))
                         (funcall execute stream ast))))
             (setf complete t))
        (unless complete (ddl-publication-test-restore)))))
  :installed)

(defun ddl-publication-test-release ()
  (sb-thread:signal-semaphore (getf *ddl-publication-test-state* :release))
  :released)

(defun ddl-publication-test-chain (names)
  (let ((events (reverse (getf *ddl-publication-test-state* :events))))
    (assert (= (length events) (length names)))
    (loop for event in events
          for expected-name in names
          do (assert (equal expected-name
                            (or (getf (getf event :ast) :name)
                                (first (getf (getf event :ast) :names))))))
    (loop for (previous next) on events
          while next
          do (assert (equal (getf previous :schema-after)
                            (getf next :schema-before))))
    (let ((*backend* (make-backend :database "postgres")))
      (assert (equal (getf (car (last events)) :schema-after)
                     (logical-current-schema-contract)))))
  :chain-valid)

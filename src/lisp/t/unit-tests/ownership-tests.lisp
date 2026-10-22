;;;; ownership-tests.lisp -- interruption at resource publication and handoff.

(in-package #:pg)

(defun call-test-relation-scope (kind relations function)
  (ecase kind
    (:build (with-relation-build-locks (relations t) (funcall function)))
    (:maintenance (with-relation-maintenance-locks (relations) (funcall function)))
    (:rewrite
     (call-with-revalidated-relation-locks
      (lambda () relations) #'identity
      (lambda (requested publish)
        (acquire-relation-build-locks requested nil t publish))
      #'release-relation-build-locks
      (lambda (captured current locked)
        (and (equal captured current) (equal current locked)))
      (lambda (current) (declare (ignore current)) (funcall function))))))

(defun test-relation-scope-clear-p (relations)
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (and (every (lambda (relation)
                  (and (null (gethash relation *relation-builders*))
                       (null (gethash relation *relation-reader-blockers*))
                       (null (gethash relation *relation-maintainers*))))
                relations)
         (loop for state being the hash-values of *relation-build-groups*
               never (intersection relations (first state) :test #'=)))))

(defun test-relation-scope-publication-and-return-interrupts ()
  (dolist (kind '(:build :maintenance :rewrite))
    (dolist (phase '(:publication :return :throw))
      (let* ((relations '(987901 987902))
             (name (if (eq kind :maintenance)
                       'acquire-relation-maintenance-locks
                       'acquire-relation-build-locks))
             (original (symbol-function name))
             (tag (gensym "OWNERSHIP-EXIT-"))
             (published nil)
             (queued nil)
             (body-ran nil)
             (result nil))
        (unwind-protect
             (progn
               (setf (symbol-function name)
                     (lambda (&rest arguments)
                       (let* ((arguments (copy-list arguments))
                              (publish-cell (last arguments))
                              (publish (car publish-cell)))
                         (setf (car publish-cell)
                               (lambda (locked token)
                                 (check (not sb-sys:*interrupts-enabled*)
                                        "relation ownership is published with interrupts masked")
                                 (check (every (lambda (relation)
                                                 (eq token
                                                     (gethash relation
                                                              (if (eq kind :maintenance)
                                                                  *relation-maintainers*
                                                                  *relation-builders*))))
                                               locked)
                                        "the complete relation set exists at ownership publication")
                                 (funcall publish locked token)
                                 (setf published t)
                                 (case phase
                                   (:throw (throw tag :thrown))
                                   (:publication
                                    (sb-thread:interrupt-thread
                                     sb-thread:*current-thread*
                                     (lambda () (ereport "57014" "publication canceled")))
                                    (setf queued t)))))
                         (multiple-value-prog1 (apply original arguments)
                           (when (eq phase :return)
                             (setf queued t)
                             (sb-thread:interrupt-thread
                              sb-thread:*current-thread*
                              (lambda () (ereport "57014" "acquisition return canceled"))))))))
               (setf result
                     (catch tag
                       (handler-case
                           (call-test-relation-scope kind relations
                                                     (lambda () (setf body-ran t)))
                         (pg-error (condition) (pg-error-sqlstate condition)))))
               (check (and published (not body-ran)
                           (if (eq phase :throw)
                               (eq result :thrown)
                               (and queued (equal result "57014"))))
                      "publication/return cancellation and non-error exits propagate before the body")
               (check (test-relation-scope-clear-p relations)
                      "interrupted acquisition leaves no group, builder, blocker, or maintainer"))
          (setf (symbol-function name) original))))))

(defun test-relation-scope-waits-and-cleanup ()
  (dolist (kind '(:build :maintenance :rewrite))
    (let* ((relations '(987903))
           (blocker (acquire-relation-build-locks relations))
           (original-wait (symbol-function 'condition-wait-with-timeout))
           (waited nil)
           (condition nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'condition-wait-with-timeout)
                   (lambda (waitqueue mutex)
                     (if (eq waitqueue *relation-lock-cv*)
                         (progn
                           (setf waited t)
                           (check sb-sys:*interrupts-enabled*
                                  "relation gate waits remain interruptible")
                           (sb-thread:interrupt-thread
                            sb-thread:*current-thread*
                            (lambda () (ereport "57014" "gate wait canceled")))
                           (error "gate wait deferred its interrupt"))
                         (funcall original-wait waitqueue mutex))))
             (handler-case
                 (call-test-relation-scope kind relations (lambda () :unexpected))
               (pg-error (caught) (setf condition caught)))
             (check (and waited condition
                         (string= (pg-error-sqlstate condition) "57014"))
                    "blocked acquisition can be canceled before taking ownership")
             (check (eq (gethash (first relations) *relation-builders*) blocker)
                    "canceling a waiter preserves the existing owner's gate"))
        (setf (symbol-function 'condition-wait-with-timeout) original-wait)
        (release-relation-build-locks relations blocker)))
    (let* ((relations '(987904))
           (name (if (eq kind :maintenance)
                     'release-relation-maintenance-locks
                     'release-relation-build-locks))
           (original (symbol-function name))
           (queued nil)
           (cleared-before-interrupt nil)
           (condition nil))
      (unwind-protect
           (progn
             (setf (symbol-function name)
                   (lambda (locked token)
                     (check (not sb-sys:*interrupts-enabled*)
                            "relation release masks asynchronous interruption")
                     (sb-thread:interrupt-thread
                      sb-thread:*current-thread*
                      (lambda () (ereport "57014" "gate cleanup canceled")))
                     (setf queued t)
                     (funcall original locked token)
                     (setf cleared-before-interrupt (test-relation-scope-clear-p locked))))
             (handler-case
                 (call-test-relation-scope kind relations (lambda () :done))
               (pg-error (caught) (setf condition caught)))
             (check (and queued cleared-before-interrupt condition
                         (test-relation-scope-clear-p relations))
                    "relation release completes before a cleanup interrupt is delivered"))
        (setf (symbol-function name) original)))))

(defun test-relation-scope-empty-and-unscoped-failure ()
  (dolist (kind '(:build :maintenance :rewrite))
    (check (equal (multiple-value-list
                   (call-test-relation-scope kind nil (lambda () (values :one :two))))
                  '(:one :two))
           "empty relation scopes preserve multiple values without ownership"))
  (dolist (kind '(:build :maintenance :single-build :single-maintenance))
    (let ((relations '(987905 987906))
          (tag (gensym "RAW-ACQUIRE-EXIT-")))
      (check (eq (catch tag
                   (flet ((interrupt (locked token)
                            (declare (ignore locked token))
                            (throw tag :interrupted)))
                     (ecase kind
                       (:build
                        (acquire-relation-build-locks relations nil t #'interrupt))
                       (:maintenance
                        (acquire-relation-maintenance-locks relations #'interrupt))
                       (:single-build
                        (acquire-relation-build-lock (first relations) nil t #'interrupt))
                       (:single-maintenance
                        (acquire-relation-maintenance-lock (first relations) #'interrupt)))))
                 :interrupted)
             "unscoped acquisition propagates a non-error exit during publication")
      (check (test-relation-scope-clear-p relations)
             "unscoped acquisition also releases published gates on failed return"))))

(defun test-query-temp-output-acquisition-interrupts ()
  (dolist (operation '(:run :merge))
    (let* ((directory (make-query-spill-test-directory))
           (*query-temp-directory* directory)
           (original (symbol-function 'open-query-temp-output))
           (stream nil)
           (quota nil)
           (queued nil)
           (condition nil))
      (unwind-protect
           (progn
             (handler-case
                 (with-query-temp-files ()
                                        (setf quota *query-temp-quota*)
                                        (let* ((item (list :query-sort-item 0 #(1) (cons #(1) #(1))))
                                               (left (when (eq operation :merge)
                                                       (write-query-sort-run (list item) nil)))
                                               (right (when (eq operation :merge)
                                                        (write-query-sort-run (list item) nil))))
                                          (setf (symbol-function 'open-query-temp-output)
                                                (lambda ()
                                                  (multiple-value-bind (path opened) (funcall original)
                                                    (setf stream opened)
                                                    (sb-thread:interrupt-thread
                                                     sb-thread:*current-thread*
                                                     (lambda () (ereport "57014" "output acquisition canceled")))
                                                    (setf queued t)
                                                    (values path opened))))
                                          (ecase operation
                                            (:run (write-query-sort-run (list item) nil))
                                            (:merge (merge-query-sort-runs left right nil)))))
               (pg-error (caught) (setf condition caught)))
             (check (and queued condition stream (not (open-stream-p stream))
                         (string= (pg-error-sqlstate condition) "57014"))
                    "sort/merge output closes after interruption during open handoff")
             (check (and (query-spill-test-directory-empty-p directory)
                         (zerop (query-temp-quota-used-bytes quota))
                         (zerop (hash-table-count (query-temp-quota-file-sizes quota)))
                         (zerop (hash-table-count (query-temp-quota-stream-paths quota))))
                    "interrupted output retains file and quota cleanup ownership"))
        (setf (symbol-function 'open-query-temp-output) original)
        (when (and stream (open-stream-p stream)) (close stream :abort t))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore)))))

(defun test-query-temp-output-values-and-quota-error ()
  (let* ((directory (make-query-spill-test-directory))
         (*query-temp-directory* directory)
         (stream nil)
         (path nil))
    (unwind-protect
         (progn
           (with-query-temp-files ()
                                  (check (equal (multiple-value-list
                                                 (with-query-temp-output (pathname output)
                                                   (setf path pathname stream output)
                                                   (values :first :second)))
                                                '(:first :second))
                                         "output scope preserves all body values")
                                  (check (and (probe-file path) (not (open-stream-p stream)))
                                         "closing output leaves its path owned by the file scope"))
           (let ((*query-temp-file-limit-bytes* 0))
             (check (handler-case
                        (with-query-temp-files ()
                                               (with-query-temp-output (pathname output)
                                                 (setf stream output)
                                                 (query-spill-write-record output "over quota")))
                      (pg-error (condition)
                        (string= (pg-error-sqlstate condition) "53400")))
                    "output scope preserves quota errors"))
           (check (and (not (open-stream-p stream))
                       (query-spill-test-directory-empty-p directory))
                  "quota errors close output and reclaim files"))
      (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(defun test-backend-query-scope-controls-and-values ()
  (dolist (start '(nil t))
    (dolist (finish '(nil t))
      (let* ((backend (make-backend))
             (*backend* backend)
             (*live-operation-backend-query-tokens* (make-hash-table :test #'eq))
             (original-begin (symbol-function 'begin-backend-query-execution))
             (original-end (symbol-function 'end-backend-query-execution))
             (begins 0) (ends 0) (backend-evals 0) (start-evals 0) (finish-evals 0))
        (unwind-protect
             (progn
               (setf (symbol-function 'begin-backend-query-execution)
                     (lambda (target) (incf begins) (funcall original-begin target))
                     (symbol-function 'end-backend-query-execution)
                     (lambda (target) (incf ends) (funcall original-end target)))
               (check (equal (multiple-value-list
                              (with-backend-query-execution
                                  (:backend (progn (incf backend-evals) backend)
                                   :start (progn (incf start-evals) start)
                                   :finish (progn (incf finish-evals) finish))
                                (check (eql (backend-executing backend) start)
                                       "query start is controlled independently")
                                (values :one :two)))
                             '(:one :two))
                      "query scope preserves body multiple values")
               (check (and (= begins (if start 1 0)) (= ends (if finish 1 0))
                           (= backend-evals start-evals finish-evals 1)
                           (eql (backend-executing backend) (and start (not finish))))
                      "query lifecycle flags are independent and evaluated once"))
          (setf (symbol-function 'begin-backend-query-execution) original-begin
                (symbol-function 'end-backend-query-execution) original-end)
          (funcall original-end backend))))))

(defun test-backend-query-scope-cleanup-lanes ()
  (dolist (mode '(:direct :drain :shutdown))
    (let* ((backend (make-backend))
           (*backend* backend)
           (*live-operation-backend-query-tokens* (make-hash-table :test #'eq))
           (*shutdown* (if (eq mode :shutdown) +fast-shutdown+ +smart-shutdown+))
           (original (symbol-function 'end-backend-query-execution))
           (lane :unset))
      (unwind-protect
           (progn
             (test-reset-live-operation-admission)
             (setf (symbol-function 'end-backend-query-execution)
                   (lambda (target)
                     (setf lane *live-operation-current-lane*)
                     (funcall original target)))
             (sb-thread:with-mutex (*live-operation-lock*)
               (setf *live-operation-admission-open* nil
                     *live-operation-drain-open* t
                     *live-operation-cleanup-backends* (list backend)))
             (check (eq (catch 'scope-exit
                          (if (eq mode :direct)
                              (with-backend-query-execution () (throw 'scope-exit :done))
                              (with-backend-query-execution (:cleanup :query-state)
                                (throw 'scope-exit :done))))
                        :done)
                    "query lifecycle cleanup preserves nonlocal exit")
             (check (and (eq lane (if (eq mode :drain) :drain nil))
                         (not (backend-executing backend))
                         (not (gethash backend *live-operation-backend-query-tokens*)))
                    "direct, reload-drain, and terminating-shutdown cleanup lanes remain distinct"))
        (setf (symbol-function 'end-backend-query-execution) original)
        (test-reset-live-operation-admission)))))

(defun test-backend-query-scope-captures-owner ()
  (let* ((owner (make-backend))
         (other (make-backend))
         (*backend* owner)
         (*live-operation-backend-query-tokens* (make-hash-table :test #'eq))
         (original (symbol-function 'end-backend-query-execution))
         (ended nil)
         (ended-lane nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'end-backend-query-execution)
                 (lambda (target)
                   (setf ended target ended-lane *live-operation-current-lane*)
                   (funcall original target)))
           (with-backend-query-execution () (setf *backend* other))
           (check (and (eq ended owner) (not (backend-executing owner))
                       (zerop (hash-table-count *live-operation-backend-query-tokens*)))
                  "query cleanup releases the captured backend after rebinding")
           (let ((*backend* other)
                 (*shutdown* +smart-shutdown+))
             (unwind-protect
                  (progn
                    (test-reset-live-operation-admission)
                    (sb-thread:with-mutex (*live-operation-lock*)
                      (setf *live-operation-admission-open* nil
                            *live-operation-drain-open* t
                            *live-operation-cleanup-backends* (list owner)))
                    (sb-ext:with-timeout 2
                      (with-backend-query-execution (:backend owner :cleanup :query-state)
                        (setf *backend* nil)))
                    (check (and (eq ended owner) (eq ended-lane :drain)
                                (not (backend-executing owner))
                                (zerop (hash-table-count *live-operation-backend-query-tokens*)))
                           "query-state cleanup admits the captured owner after rebinding"))
               (test-reset-live-operation-admission)))
           (setf ended :unset)
           (with-backend-query-execution (:backend nil) :no-backend)
           (check (eq ended :unset) "a nil backend has no query ownership to release"))
      (setf (symbol-function 'end-backend-query-execution) original))))

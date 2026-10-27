(in-package #:pg)

(defvar *sql-promotion-test-root* nil)
(defvar *sql-promotion-test-mode* nil)
(defvar *sql-promotion-test-backend* nil)
(defvar *sql-promotion-test-captured* nil)
(defvar *sql-promotion-test-owners* nil)
(defvar *sql-promotion-test-requests* 0)
(defvar *sql-promotion-test-parks* 0)
(defvar *sql-promotion-test-worker* nil)
(defvar *sql-promotion-test-wrapper* nil)
(defvar *sql-promotion-test-promotion-hook* nil)
(defvar *sql-promotion-test-park-hook* nil)
(defvar *sql-promotion-test-open-hook* nil)

(defun sql-promotion-test-marker (name)
  (let* ((path (merge-pathnames name *sql-promotion-test-root*))
         (temporary (pathname (concatenate 'string (namestring path) ".tmp"))))
    (with-open-file (stream temporary :direction :output :if-exists :error)
      (format stream "~d~%" (sb-posix:getpid)))
    (sb-posix:rename (namestring temporary) (namestring path))))

(defun sql-promotion-test-request (point)
  (when (and (eq point :after-sql-request-close-before-wake)
             (eq *backend* *sql-promotion-test-backend*))
    (incf *sql-promotion-test-requests*)
    (setf *sql-promotion-test-captured*
          (list (backend-transaction-snapshot *backend*)
                (backend-virtual-reader-xid *backend*) *catalog* *clog-stream* *relation-streams*))
    (sql-promotion-test-marker "sql-request")
    (when (eq *sql-promotion-test-mode* :handoff-cancel)
      (sb-thread:interrupt-thread
       sb-thread:*current-thread*
       (lambda () (ereport "57014" "SQL promotion handoff cancellation"))))))

(defun sql-promotion-test-park (point token)
  (when (and (eq point :after-park) (eq *backend* *sql-promotion-test-backend*))
    (incf *sql-promotion-test-parks*)
    (setf *sql-promotion-test-owners* (copy-list (live-operation-token-snapshot-owners token)))
    (sql-promotion-test-marker "sql-parked")))

(defun sql-promotion-test-before-open ()
  (when (and (string= (sb-thread:thread-name sb-thread:*current-thread*) "pg reloader")
             (plusp (live-operation-parked-count))
             (not (probe-file (merge-pathnames "sql-reload-backed-off" *sql-promotion-test-root*))))
    (sql-promotion-test-marker "sql-reload-backed-off")))

(defun sql-promotion-test-install (root backend-pid mode)
  (assert (null *sql-promotion-test-worker*))
  (setf *sql-promotion-test-root* root
        *sql-promotion-test-mode* mode
        *sql-promotion-test-backend* (lookup-backend backend-pid)
        *sql-promotion-test-captured* nil
        *sql-promotion-test-owners* nil
        *sql-promotion-test-requests* 0
        *sql-promotion-test-parks* 0
        *sql-promotion-test-worker* (symbol-function 'physical-promotion-worker-step)
        *sql-promotion-test-promotion-hook* *physical-promotion-test-hook*
        *sql-promotion-test-park-hook* *live-operation-parking-test-hook*
        *sql-promotion-test-open-hook* *live-operation-before-open-hook*)
  (assert *sql-promotion-test-backend*)
  (let ((worker *sql-promotion-test-worker*))
    (setf *sql-promotion-test-wrapper*
          (lambda ()
            (when (and *physical-promotion-requested-p*
                       (not (probe-file (merge-pathnames "sql-worker-release" root))))
              (assert (zerop *live-operation-depth*))
              (unless (probe-file (merge-pathnames "sql-worker-held" root))
                (sql-promotion-test-marker "sql-worker-held"))
              (loop until (probe-file (merge-pathnames "sql-worker-release" root))
                    do (sleep 0.025)))
            (funcall worker))
          (symbol-function 'physical-promotion-worker-step) *sql-promotion-test-wrapper*
          *physical-promotion-test-hook* #'sql-promotion-test-request
          *live-operation-parking-test-hook* #'sql-promotion-test-park
          *live-operation-before-open-hook* #'sql-promotion-test-before-open))
  t)

(defun sql-promotion-test-verify (expected-parks retained-p)
  (assert (= 1 *sql-promotion-test-requests*))
  (assert (= expected-parks *sql-promotion-test-parks*))
  (assert (zerop (live-operation-parked-count)))
  (assert (not (sb-thread:with-mutex (*live-operation-lock*)
                 (gethash *sql-promotion-test-backend* *live-operation-backend-counts*))))
  (destructuring-bind (snapshot virtual catalog clog streams) *sql-promotion-test-captured*
    (assert (every #'eq (list catalog clog streams) (list *catalog* *clog-stream* *relation-streams*)))
    (if retained-p
        (progn
          (assert snapshot)
          (assert (eq snapshot (backend-transaction-snapshot *sql-promotion-test-backend*)))
          (assert (eql virtual (backend-virtual-reader-xid *sql-promotion-test-backend*)))
          (assert (backend-read-only *sql-promotion-test-backend*)))
        (progn
          (assert (null (backend-transaction-snapshot *sql-promotion-test-backend*)))
          (assert (null (backend-virtual-reader-xid *sql-promotion-test-backend*)))
          (dolist (owner *sql-promotion-test-owners*)
            (assert (not (snapshot-registration-active-p (first owner))))))))
  t)

(defun sql-promotion-test-restore ()
  (when *sql-promotion-test-worker*
    (when (eq (symbol-function 'physical-promotion-worker-step) *sql-promotion-test-wrapper*)
      (setf (symbol-function 'physical-promotion-worker-step) *sql-promotion-test-worker*))
    (when (eq *physical-promotion-test-hook* #'sql-promotion-test-request)
      (setf *physical-promotion-test-hook* *sql-promotion-test-promotion-hook*))
    (when (eq *live-operation-parking-test-hook* #'sql-promotion-test-park)
      (setf *live-operation-parking-test-hook* *sql-promotion-test-park-hook*))
    (when (eq *live-operation-before-open-hook* #'sql-promotion-test-before-open)
      (setf *live-operation-before-open-hook* *sql-promotion-test-open-hook*))
    (setf *sql-promotion-test-worker* nil))
  t)

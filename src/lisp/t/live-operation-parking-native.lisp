(in-package #:pg)

(defvar *parking-native-wrapper* nil)
(defvar *parking-native-calls* 0)
(defvar *parking-native-captured* nil)
(defvar *parking-native-root* nil)

(defun parking-native-marker (name)
  "Publish a marker at the exact requested path.

Use POSIX rename because CL RENAME-FILE can merge the temporary .tmp type into
an extensionless destination."
  (let* ((path (merge-pathnames name *parking-native-root*))
         (temporary (pathname (concatenate 'string (namestring path) ".tmp"))))
    (unwind-protect
         (progn
           (with-open-file (out temporary :direction :output :if-exists :error)
             (format out "~d~%" (sb-posix:getpid)))
           (sb-posix:rename (namestring temporary) (namestring path)))
      (when (probe-file temporary) (delete-file temporary)))))

(defun parking-native-wait ()
  (incf *parking-native-calls*)
  (let ((snapshot *current-snapshot*)
        (retained (backend-transaction-snapshot *backend*))
        (backend *backend*)
        (virtual (backend-virtual-reader-xid *backend*))
        (query-token (gethash *backend* *live-operation-backend-query-tokens*))
        (runtime (list *catalog* *clog-stream* *relation-streams*)))
    (setf *parking-native-captured* (list backend retained virtual))
    (call-with-live-operation-parked
     (lambda ()
       (parking-native-marker "parked")
       (loop repeat 3000 until (probe-file (merge-pathnames "release" *parking-native-root*))
             do (check-for-interrupts) (sleep 0.01))
       (assert (probe-file (merge-pathnames "release" *parking-native-root*)))))
    (assert (eq snapshot *current-snapshot*))
    (assert (eq retained (backend-transaction-snapshot backend)))
    (assert (eql virtual (backend-virtual-reader-xid backend)))
    (assert (eq query-token (gethash backend *live-operation-backend-query-tokens*)))
    (assert (every #'eq runtime (list *catalog* *clog-stream* *relation-streams*)))
    (assert (eq *parking-native-wrapper* (symbol-function 'eval-setting-function)))
    (assert (eq :active (live-operation-token-state *live-operation-current-token*)))
    (assert (backend-read-only backend))
    (assert (not (recovery-in-progress-p))))
  "resumed")

(defun parking-native-install (root)
  (let ((original (symbol-function 'eval-setting-function)))
    (setf *parking-native-root* root
          *parking-native-calls* 0
          *parking-native-wrapper*
          (lambda (name arguments)
            (if (and (string= name "current_setting")
                     (equal (first arguments) "pg_lisp_test_park"))
                (parking-native-wait)
                (funcall original name arguments)))
          (symbol-function 'eval-setting-function) *parking-native-wrapper*
          *live-operation-before-open-hook*
          (lambda ()
            (when (and (string= (sb-thread:thread-name sb-thread:*current-thread*) "pg reloader")
                       (plusp (live-operation-parked-count))
                       (not (probe-file (merge-pathnames "reload-backed-off" root))))
              (parking-native-marker "reload-backed-off")))))
  t)

(defun parking-native-reloaded-p ()
  (not (eq *parking-native-wrapper* (symbol-function 'eval-setting-function))))

(defun parking-native-verify ()
  (assert (= 1 *parking-native-calls*))
  (assert (parking-native-reloaded-p))
  (assert (zerop (live-operation-parked-count)))
  (destructuring-bind (backend retained virtual) *parking-native-captured*
    (assert (eq retained (backend-transaction-snapshot backend)))
    (assert (eql virtual (backend-virtual-reader-xid backend))))
  (setf *live-operation-before-open-hook* nil)
  t)

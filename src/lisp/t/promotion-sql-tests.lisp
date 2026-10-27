(in-package #:pg-physical-tests)

(defun promotion-sql-expression (sql)
  (let* ((statement (first (pg::parse-statements sql)))
         (target (first (getf statement :targets))))
    (pg::analyze-expr (getf target :expr) nil)))

(defun promotion-sql-error-state (function)
  (handler-case (progn (funcall function) :no-error)
    (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))

(deftest sql-promotion-default-and-named-arguments-match-c
    (dolist (sql '("SELECT pg_promote()" "SELECT pg_promote(true)"
                   "SELECT pg_promote(wait=>true)"
                   "SELECT pg_catalog.pg_promote(wait_seconds=>60)"
                   "SELECT pg_promote(wait_seconds:=60,wait:=true)"))
      (let ((expression (promotion-sql-expression sql)))
        (ok (eq :bool (pg::expr-type expression)))
        (ok (equal '(t 60) (mapcar #'second (third expression)))))))

(deftest sql-promotion-signature-errors-match-c
    (dolist (case '(("SELECT pg_promote(VARIADIC wait_seconds=>1)" "42883")
                    ("SELECT pg_promote('bad',1::bigint)" "42883")
                    ("SELECT pg_promote('bad',wrong=>1)" "42883")
                    ("SELECT pg_promote(1)" "42883")
                    ("SELECT pg_promote(true,1::bigint)" "42883")
                    ("SELECT pg_promote('true'::text,1)" "42883")
                    ("SELECT pg_promote(wrong=>NULL)" "42883")
                    ("SELECT pg_promote(true,wait=>false)" "42883")
                    ("SELECT pg_promote(wait=>true,wait=>false)" "42601")
                    ("SELECT pg_promote(wait=>true,true)" "42601")
                    ("SELECT pg_promote(NULL,'bad')" "22P02")
                    ("SELECT pg_promote(true,'2147483648')" "22003")))
      (destructuring-bind (sql state) case
        (ok (equal state (promotion-sql-error-state
                          (lambda () (promotion-sql-expression sql)))) sql))))

(deftest sql-promotion-strict-null-and-primary-error-order-match-c
    (let ((pg::*physical-recovery-state* nil))
      (dolist (sql '("SELECT pg_promote(NULL)" "SELECT pg_promote(wait_seconds=>NULL)"
                     "SELECT pg_promote(false,NULL::integer)"))
        (ok (eq pg::+null+ (pg::eval-expr (promotion-sql-expression sql) nil)) sql))
      (dolist (sql '("SELECT pg_promote()" "SELECT pg_promote(false,0)"
                     "SELECT pg_promote(true,-1)"))
        (ok (equal "55000" (promotion-sql-error-state
                            (lambda () (pg::eval-expr (promotion-sql-expression sql) nil)))) sql))))

(deftest sql-promotion-named-volatiles-evaluate-in-parameter-order
    (let ((pg::*backend* (pg::make-backend))
          (pg::*physical-recovery-state* nil))
      (pg::session-set-application-name "trace")
      (ok (eq pg::+null+
              (pg::eval-expr
               (promotion-sql-expression
                "SELECT pg_promote(wait_seconds=>CASE WHEN set_config('application_name',current_setting('application_name')||':seconds',false) IS NOT NULL THEN NULL::integer END, wait=>CASE WHEN set_config('application_name',current_setting('application_name')||':wait',false) IS NOT NULL THEN true END)")
               nil)))
      (ok (equal "trace:wait:seconds" (pg::sql-setting-value "application_name")))))

(cl-spec:defproperty promotion-call-notation-preserves-argument-values
    ((wait (range integer 0 1)) (seconds (range integer -100 100)) (style (range integer 0 4)))
  (:about pg::analyze-promotion-arguments)
  (:trials (:normal 200))
  (let* ((boolean (if (zerop wait) "false" "true"))
         (arguments (case style
                      (0 (format nil "~a,~d" boolean seconds))
                      (1 (format nil "wait=>~a,wait_seconds=>~d" boolean seconds))
                      (2 (format nil "wait_seconds=>~d,wait=>~a" seconds boolean))
                      (3 (format nil "wait_seconds:=~d,wait:=~a" seconds boolean))
                      (4 (format nil "~a,wait_seconds=>~d" boolean seconds))))
         (expression (promotion-sql-expression (format nil "SELECT pg_promote(~a)" arguments))))
    (equal (list (not (zerop wait)) seconds)
           (mapcar (lambda (argument) (pg::eval-expr argument nil)) (third expression)))))

(deftest generated-promotion-call-notation-agrees
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'promotion-call-notation-preserves-argument-values :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest sql-promotion-request-handoff-survives-deferred-cancellation
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*physical-promotion-worker* sb-thread:*current-thread*)
             (pg::*physical-promotion-requested-p* nil)
             (check (symbol-function 'pg::physical-promotion-check-online))
             (observed nil)
             (pg::*physical-promotion-test-hook*
               (lambda (point)
                 (when (eq point :after-sql-request-close-before-wake)
                   (setf observed t)
                   (sb-thread:interrupt-thread
                    sb-thread:*current-thread*
                    (lambda () (pg::ereport "57014" "request handoff cancellation")))))))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-promotion-check-online) (lambda () t))
               (ok (equal "57014" (promotion-sql-error-state #'pg::physical-promotion-request-from-sql)))
               (ok observed)
               (ok pg::*physical-promotion-requested-p*)
               (with-open-file (stream (merge-pathnames "promote" root) :element-type '(unsigned-byte 8))
                 (ok (zerop (file-length stream)))))
          (setf (symbol-function 'pg::physical-promotion-check-online) check)))))

(deftest sql-promotion-request-file-error-does-not-wake-worker
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root)
            (pg::*physical-promotion-worker* sb-thread:*current-thread*)
            (pg::*physical-promotion-requested-p* nil)
            (check (symbol-function 'pg::physical-promotion-check-online)))
        (ensure-directories-exist (merge-pathnames "promote/placeholder" root))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-promotion-check-online) (lambda () t))
               (ok (equal "42809" (promotion-sql-error-state #'pg::physical-promotion-request-from-sql)))
               (ok (not pg::*physical-promotion-requested-p*)))
          (setf (symbol-function 'pg::physical-promotion-check-online) check)))))

(deftest sql-promotion-expired-wait-cannot-report-late-success
    (let ((recovery (symbol-function 'pg::recovery-in-progress-p))
          (checked nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::recovery-in-progress-p)
                   (lambda () (setf checked t) nil))
             (ok (null (pg::physical-promotion-wait-for-sql
                        (1- (get-internal-real-time)))))
             (ok (not checked)))
        (setf (symbol-function 'pg::recovery-in-progress-p) recovery))))

(deftest named-aggregate-arguments-do-not-raise-internal-errors
    (dolist (sql '("SELECT sum(x=>1)" "SELECT array_agg(x=>'a'::text)"))
      (ok (equal "0A000"
                 (promotion-sql-error-state
                  (lambda ()
                    (pg::analyze-aggregate-targets
                     (getf (first (pg::parse-statements sql)) :targets) nil nil)))) sql)))

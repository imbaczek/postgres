(require :asdf)

(let* ((source (uiop:ensure-directory-pathname
                (or (uiop:getenv "PG_LISP_FROZEN_SOURCE")
                    (error "startup observer requires a frozen source"))))
       (*compile-verbose* nil) (*load-verbose* nil))
  (pushnew source asdf:*central-registry* :test #'equal)
  (asdf:load-asd (merge-pathnames "pg.asd" source))
  (asdf:load-system "pg"))

(in-package #:pg)

(defvar *native-imported-main-slots-restored* nil)
(defvar *native-imported-main-empty-restored* nil)

(defvar *native-imported-main-slot-loads* 0)

(defvar *native-imported-main-catalogs* 0)

(defvar *native-imported-main-bootstraps* 0)

(defvar *native-imported-main-owned-checks* 0)

(defun native-imported-main-install-fault-observer ()
  (let ((fault (uiop:getenv "PG_NATIVE_MAIN_FAULT")))
    (unless (member fault '(nil "proof-loss") :test #'equal)
      (error "unknown imported main instrumentation fault"))
    (let ((original (symbol-function 'logical-slot-imported-startup-check-owned)))
      (setf (symbol-function 'logical-slot-imported-startup-check-owned)
            (lambda (expected)
              (incf *native-imported-main-owned-checks*)
              (multiple-value-prog1 (funcall original expected)
                (when (and fault (= 1 *native-imported-main-owned-checks*))
                  (let ((saved (merge-pathnames "injected-main-source-proof"
                                                (uiop:temporary-directory))))
                    (when (slot-state-lstat saved) (error "proof fault destination already exists"))
                    (sb-posix:rename (merge-pathnames "logical-slot-copy.source-proof" *pgdata*) saved)
                    (let ((container (merge-pathnames "pg_logical_continuation/" *pgdata*)))
                      (when (slot-state-lstat container)
                        (sb-posix:rename container
                                         (merge-pathnames "injected-main-continuation"
                                                          (uiop:temporary-directory)))))
                    (format t "~&IMPORTED-MAIN-PROOF-REMOVED~%")))))))
    (dolist (entry '((wal-load-replication-slots . *native-imported-main-slot-loads*)
                     (catalog-init . *native-imported-main-catalogs*)
                     (physical-bootstrap-check-startup . *native-imported-main-bootstraps*)))
      (let ((original (symbol-function (car entry))) (counter (cdr entry)))
        (setf (symbol-function (car entry))
              (lambda (&rest arguments)
                (incf (symbol-value counter))
                (apply original arguments)))))
    (push (lambda ()
            (format t "~&IMPORTED-MAIN-COUNTS slots=~d catalogs=~d bootstraps=~d owned=~d wal-ready=~a fence=~a~%"
                    *native-imported-main-slot-loads* *native-imported-main-catalogs*
                    *native-imported-main-bootstraps* *native-imported-main-owned-checks*
                    *wal-ready* (not (null (logical-slot-continuation-gc-fenced-p *pgdata*)))))
          sb-ext:*exit-hooks*)))

(native-imported-main-install-fault-observer)

(let ((original (symbol-function 'logical-slot-continuation-restore-empty-publication)))
  (setf (symbol-function 'logical-slot-continuation-restore-empty-publication)
        (lambda (&rest arguments)
          (multiple-value-prog1 (apply original arguments)
            (setf *native-imported-main-empty-restored* t)))))

(let ((original (symbol-function 'wal-load-replication-slots)))
  (setf (symbol-function 'wal-load-replication-slots)
        (lambda (&key gate-owner ownership)
          (unless (and gate-owner ownership)
            (error "main startup observer requires the real gated slot loader"))
          (multiple-value-prog1
              (funcall original :gate-owner gate-owner :ownership ownership)
            (setf *native-imported-main-slots-restored* t)))))

(let ((original (symbol-function 'catalog-init)))
  (setf (symbol-function 'catalog-init)
        (lambda ()
          (unless *native-imported-main-slots-restored*
            (error "catalog initialization preceded slot restoration"))
          (sb-thread:with-mutex (*wal-lock*)
            (let ((slots (hash-table-count *wal-replication-slots*))
                  (groups (hash-table-count *logical-slot-continuation-protections*)))
              (assert *wal-ready*)
              (assert (not (recovery-in-progress-p)))
              (if (plusp slots)
                  (progn
                    (assert (plusp groups))
                    (maphash (lambda (key group)
                               (declare (ignore key))
                               (assert (logical-slot-continuation-protection-p group)))
                             *logical-slot-continuation-protections*))
                  (assert (and (zerop groups) *native-imported-main-empty-restored*)))
              (format t "~&IMPORTED-MAIN-BEFORE-CATALOG slots=~d groups=~d outcome=~a~%"
                      slots groups (if (plusp slots) :c :zero-c))))
          (funcall original))))

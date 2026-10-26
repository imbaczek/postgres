;;;; transaction-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-repeatable-read-write-conflict-horizon
    (let* ((*backend* (make-backend))
           (backend *backend*))
      (setf (backend-isolation-level backend) :repeatable-read
            (backend-transaction-snapshot backend)
            (make-snapshot-struct :xmin 100 :xmax 120 :xip '(115) :curcid 1))
      (rove:ok (repeatable-read-conflict-p 120)
               "a committed XID at the snapshot xmax is a write conflict")
      (rove:ok (repeatable-read-conflict-p 115)
               "an XID active when the repeatable-read snapshot was taken is a write conflict")
      (rove:ok (not (repeatable-read-conflict-p 119))
               "a committed XID below xmax and outside xip predates the snapshot")
      (setf (backend-isolation-level backend) :read-committed)
      (rove:ok (not (repeatable-read-conflict-p 120))
               "READ COMMITTED does not use the retained-snapshot conflict rule")))

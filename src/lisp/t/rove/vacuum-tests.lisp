;;;; vacuum-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-revalidated-relation-locks-retry-and-release
    (let ((state-count 0)
          (acquisitions '())
          (releases '())
          (callback-count 0)
          (result nil))
      (setf result
            (call-with-revalidated-relation-locks
             (lambda ()
               (incf state-count)
               (if (= state-count 1) 10 20))
             #'list
             (lambda (requested publish)
               (let ((locked (copy-list requested))
                     (token (gensym "TEST-LOCK-")))
                 (push (list locked token) acquisitions)
                 (funcall publish locked token)
                 (values locked token)))
             (lambda (locked token)
               (push (list locked token) releases))
             (lambda (captured current locked)
               (and (= captured current)
                    (member current locked :test #'=)))
             (lambda (current)
               (incf callback-count)
               current)))
      (rove:ok (and (= result 20)
                    (= callback-count 1)
                    (= state-count 4))
               "relation-lock revalidation retries stale state and runs once")
      (rove:ok (and (equal (mapcar #'first (reverse acquisitions)) '((10) (20)))
                    (equal (reverse acquisitions) (reverse releases)))
               "relation-lock retry releases each acquired gate before retry/return")))

(rove:deftest test-revalidated-relation-locks-release-after-revalidation-error
    (let ((released nil)
          (provider-calls 0)
          (propagated nil))
      (setf propagated
            (handler-case
                (progn
                  (call-with-revalidated-relation-locks
                   (lambda ()
                     (incf provider-calls)
                     (if (= provider-calls 1)
                         10
                         (error "injected relation-state failure")))
                   #'list
                   (lambda (requested publish)
                     (funcall publish requested :test-token)
                     (values requested :test-token))
                   (lambda (locked token)
                     (setf released
                           (and (equal locked '(10)) (eq token :test-token))))
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     t)
                   (lambda (current)
                     (declare (ignore current))
                     :unexpected))
                  nil)
              (error (condition)
                (search "injected relation-state failure"
                        (princ-to-string condition)))))
      (rove:ok propagated
               "relation-state errors propagate from lock revalidation")
      (rove:ok released
               "relation-lock gates release when state revalidation signals")))

(rove:deftest test-vacuum-index-object-set-invariant
    (let ((first (make-pg-index :oid 987650 :name "first"))
          (second (make-pg-index :oid 987651 :name "second"))
          (replacement (make-pg-index :oid 987650 :name "first")))
      (rove:ok (same-index-object-set-p (list first second) (list second first))
               "VACUUM FULL accepts the same index objects in another order")
      (rove:ok (not (same-index-object-set-p (list first) (list replacement)))
               "VACUUM FULL rejects replacement metadata with the same OID")
      (rove:ok (not (same-index-object-set-p (list first first) (list first second)))
               "VACUUM FULL rejects duplicate entries in an index set")))

(rove:deftest test-vacuum-freeze-cutoff-invariant
    (let* ((next-xid 300000000)
           (oldest-xmin (- next-xid 10000000))
           (normal-old-xid
             (- next-xid (1- +vacuum-aggressive-freeze-age+)))
           (aggressive-old-xid
             (- next-xid +vacuum-aggressive-freeze-age+))
           (table (make-pg-class :oid 992021 :name "freeze_cutoff")))
      (setf (pg-class-relfrozenxid table) aggressive-old-xid)
      (rove:ok (= (vacuum-compute-freeze-cutoff
                   normal-old-xid next-xid next-xid)
                  (- next-xid +vacuum-freeze-min-age+))
               "VACUUM uses the normal minimum-age target below aggressive age")
      (rove:ok (= (vacuum-compute-freeze-cutoff
                   aggressive-old-xid oldest-xmin next-xid)
                  (- next-xid +vacuum-freeze-min-age+))
               "aggressive age does not change the minimum-age freeze target")
      (rove:ok (= (vacuum-full-freeze-cutoff table oldest-xmin next-xid)
                  (vacuum-compute-freeze-cutoff
                   aggressive-old-xid oldest-xmin next-xid))
               "plain and FULL VACUUM share freeze-target arithmetic")
      (rove:ok (= (vacuum-compute-freeze-cutoff
                   normal-old-xid oldest-xmin next-xid
                   :forced-freeze-cutoff (- oldest-xmin 500))
                  (- oldest-xmin 500))
               "forced wraparound cutoff remains below the snapshot horizon")
      (rove:ok (= (vacuum-compute-freeze-cutoff
                   normal-old-xid oldest-xmin next-xid
                   :forced-freeze-cutoff (+ oldest-xmin 500))
                  oldest-xmin)
               "forced wraparound cutoff is clamped by oldest xmin")))

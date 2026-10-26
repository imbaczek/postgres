;;;; snapshot-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-make-snapshot-rejects-unowned-registration
    (let ((*next-xid* 7303)
          (*active-xids* '())
          (*active-snapshots* 0)
          (*active-snapshot-set* (make-hash-table :test #'eq)))
      (rove:ok (handler-case
                   (progn (make-snapshot 1 :register t) nil)
                 (error () t))
               "make-snapshot rejects a truthy non-token registration")
      (rove:ok (and (zerop *active-snapshots*)
                    (null (oldest-active-snapshot-xmin)))
               "rejected snapshot registration publishes no horizon")))

(rove:deftest test-register-active-snapshot-requires-token
    (let ((*active-snapshots* 0)
          (*active-snapshot-set* (make-hash-table :test #'eq))
          (snapshot (make-snapshot-struct :xmin 7 :xmax 8 :xip '() :curcid 1)))
      (rove:ok (handler-case
                   (progn (register-active-snapshot snapshot nil) nil)
                 (error () t))
               "register-active-snapshot rejects an unowned registration")
      (rove:ok (and (zerop *active-snapshots*)
                    (null (oldest-active-snapshot-xmin)))
               "rejected active snapshot registration changes no horizon")))

(rove:deftest test-snapshot-registration-cannot-be-reused-while-active
    (let ((*active-snapshots* 0)
          (*active-snapshot-set* (make-hash-table :test #'eq))
          (registration (make-snapshot-registration))
          (first (make-snapshot-struct :xmin 7 :xmax 8 :xip '() :curcid 1))
          (second (make-snapshot-struct :xmin 3 :xmax 8 :xip '() :curcid 1))
          (reuse-rejected nil))
      (sb-thread:with-mutex (*xid-lock*)
        (register-active-snapshot first registration)
        (setf reuse-rejected
              (handler-case
                  (progn (register-active-snapshot second registration) nil)
                (error () t))))
      (rove:ok reuse-rejected
               "an active snapshot registration token cannot be reused")
      (rove:ok (and (= *active-snapshots* 1)
                    (= (oldest-active-snapshot-xmin) 7)
                    (eq (snapshot-registration-snapshot registration) first))
               "rejected token reuse preserves the original snapshot horizon")
      (release-snapshot-registration registration)
      (rove:ok (and (zerop *active-snapshots*)
                    (null (oldest-active-snapshot-xmin))
                    (not (gethash second *active-snapshot-set*)))
               "releasing the original token clears its only snapshot registration")))

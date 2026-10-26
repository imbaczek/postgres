;;;; Isolated backend lifecycle regression.
(in-package #:pg)

(rove:deftest test-unregister-backend-clears-query-state
    (let* ((backend (make-backend))
           (*live-operation-backend-query-tokens*
             (make-hash-table :test #'eq)))
      (setf (backend-executing backend) t
            (backend-cancel-pending backend) t
            (gethash backend *live-operation-backend-query-tokens*) (cons nil nil))
      (unregister-backend backend)
      (rove:ok (and (not (backend-executing backend))
                    (not (backend-cancel-pending backend))
                    (not (gethash backend *live-operation-backend-query-tokens*)))
               "backend teardown clears query state even if query cleanup was interrupted")))

(in-package #:pg)

(rove:deftest physical-slot-cleanup-refusal-requires-issued-condition
    (let ((ordinary (make-condition 'pg-error :sqlstate "55000"))
          (forged (make-condition 'physical-slot-cold-cleanup-refusal)))
      (rove:ok (typep forged 'pg-error))
      (rove:ok (string= "55000" (pg-error-sqlstate forged)))
      (sb-thread:with-mutex (*wal-lock*)
        (dolist (candidate (list ordinary forged))
          (rove:ok
           (handler-case
               (progn
                 (physical-slot-cold-cleanup-refusal-check-under-lock candidate nil nil)
                 nil)
             (error (condition)
               (search "unissued temporary physical cleanup refusal"
                       (princ-to-string condition)))))))))

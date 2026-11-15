(in-package #:pg)

(defun call-with-logical-slot-imported-owner-check (owner function)
  (etypecase owner
    (logical-slot-imported-scan-owner
     (call-with-logical-slot-imported-scan-check owner function))
    (logical-slot-imported-startup-owner
     (call-with-logical-slot-imported-startup-owner-check owner function))))

(defun logical-slot-imported-check-owner (owner)
  "Run the full owner check outside WAL exclusion."
  (etypecase owner
    (logical-slot-imported-scan-owner (logical-slot-imported-scan-check-owner owner))
    (logical-slot-imported-startup-owner (logical-slot-imported-startup-check-owner owner))))

(defun logical-slot-imported-owner-under-lock (owner)
  (etypecase owner
    (logical-slot-imported-scan-owner (logical-slot-imported-scan-check-under-lock owner))
    (logical-slot-imported-startup-owner (logical-slot-imported-startup-owner-under-lock owner))))

(defun logical-slot-imported-owner-candidate-under-lock (owner)
  "Return the checked candidate and its source facts under WAL exclusion."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (logical-slot-imported-owner-under-lock owner)
  (etypecase owner
    (logical-slot-imported-scan-owner
     (let ((source (logical-slot-imported-scan-owner-source owner)))
       (values (logical-wal-source-slot source) (logical-wal-source-system-id source)
               (logical-wal-source-history source)
               (wal-retained-slot-scan-end (logical-slot-imported-scan-owner-scan owner)))))
    (logical-slot-imported-startup-owner
     (let ((source (logical-slot-imported-startup-owner-source owner)))
       (values (logical-slot-startup-entry-slot (logical-slot-imported-startup-owner-entry owner))
               (logical-slot-startup-source-system-id source)
               (logical-slot-startup-source-history source)
               (logical-slot-startup-source-end source))))))

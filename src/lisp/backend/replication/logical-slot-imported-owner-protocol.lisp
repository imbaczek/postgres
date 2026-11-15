(in-package #:pg)

(defun call-with-logical-slot-imported-owner-check (owner function)
  (etypecase owner
    (logical-slot-imported-host-startup-owner
     (call-with-logical-slot-imported-host-startup-check owner function))
    (logical-slot-imported-scan-owner
     (call-with-logical-slot-imported-scan-check owner function))
    (logical-slot-imported-startup-owner
     (call-with-logical-slot-imported-startup-owner-check owner function))))

(defun logical-slot-imported-check-owner (owner)
  "Run the full owner check outside WAL exclusion."
  (etypecase owner
    (logical-slot-imported-host-startup-owner
     (logical-slot-imported-host-startup-check-owner owner))
    (logical-slot-imported-scan-owner (logical-slot-imported-scan-check-owner owner))
    (logical-slot-imported-startup-owner (logical-slot-imported-startup-check-owner owner))))

(defun logical-slot-imported-owner-under-lock (owner)
  (etypecase owner
    (logical-slot-imported-host-startup-owner
     (logical-slot-imported-host-startup-under-lock owner))
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

(defun logical-slot-imported-owner-source-under-lock (owner)
  ;; Only the typed host owner has no candidate slot.  The startup and scan
  ;; owners supply their checked per-slot candidate and source facts.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (etypecase owner
    (logical-slot-imported-host-startup-owner
     (multiple-value-bind (system-id history end)
         (logical-slot-imported-host-startup-facts-under-lock owner)
       (values nil system-id history end)))
    ((or logical-slot-imported-startup-owner logical-slot-imported-scan-owner)
     (logical-slot-imported-owner-candidate-under-lock owner))))

(in-package #:pg)

(defun native-patroni-reseed-activate (target workspace port helper)
  (let ((*pgdata* target))
    (call-with-physical-reseed-follow-configuration-owner
     target
     (lambda (owner)
       (physical-reseed-follow-begin-configuration owner workspace port)
       (physical-reseed-follow-stage-configuration owner)
       (loop repeat 8
             for record = (physical-reseed-follow-advance-configuration owner)
             until (eq :inert (getf (getf record :configuration) :phase))
             finally (assert (eq :inert (getf (getf record :configuration) :phase))))
       (physical-reseed-follow-settle owner :helper helper)
       (physical-reseed-follow-begin-activation owner)
       (physical-reseed-follow-stage-activation owner)
       (physical-reseed-follow-start-activation owner :helper helper)
       (loop repeat 4
             for record = (physical-reseed-follow-advance-activation owner)
             until (eq :active (getf (getf record :activation) :phase))
             finally (assert (eq :active (getf (getf record :activation) :phase))))))))

(defun native-patroni-reseed-retire (target port)
  (let ((*pgdata* target)
        (before nil)
        (after nil))
    (unwind-protect
         (postmaster-own-data-directory
          "127.0.0.1" port
          :startup-observer
          (lambda (lock phase)
            (if (eq phase :before-pid)
                (setf before (physical-reseed-follow-startup-observation lock phase))
                (setf after (physical-reseed-follow-startup-observation lock phase before))))
          :startup-reconcile
          (lambda ()
            (call-with-physical-reseed-follow-retirement-startup-owner
             after
             (lambda (owner)
               (physical-reseed-follow-retirement-publish-initial owner)
               (physical-reseed-follow-retirement-archive-install owner)
               (physical-reseed-follow-retirement-archive-attempt owner)
               (physical-reseed-follow-retirement-publish-terminal owner)))))
      (when *postmaster-pid-stream* (postmaster-remove-pid-file))))
  (values))

(defun native-patroni-reseed-finalize (target port)
  (let ((*pgdata* target))
    (unwind-protect
         (progn
           ;; Consume the receipt before creating ordinary configuration under the data lock.
           (physical-reseed-follow-own-startup "127.0.0.1" port)
           (physical-reseed-follow-check-ordinary-startup)
           (let ((path (merge-pathnames "postgresql.conf" target)))
             (assert (not (probe-file path)))
             (wal-atomic-write-bytes path (make-array 0 :element-type '(unsigned-byte 8)))
             (fsync-directory target)))
      (unwind-protect
           (when *postmaster-pid-stream* (postmaster-remove-pid-file))
        (when *postmaster-data-lock*
          (close *postmaster-data-lock*)
          (setf *postmaster-data-lock* nil)))))
  (values))

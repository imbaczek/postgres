(in-package #:pg)

(defun physical-reseed-follow-check-terminal-startup (observation)
  (case (getf observation :branch)
    (:ordinary observation)
    ((:retirement :retired)
     (let* ((evidence (getf observation :evidence))
            (record (getf (getf evidence :snapshot) :record))
            (activation (getf record :activation))
            (retirement (getf record :retirement)))
       (unless (and (eq :active (getf activation :phase))
                    (eql 2 (getf activation :step))
                    (eq :retired (getf retirement :phase))
                    (eql 2 (getf retirement :step))
                    (eql 2 (getf evidence :prefix))
                    (null (getf evidence :location))
                    (null (getf evidence :temporary))
                    (null (getf evidence :temporary-file)))
         (error "postmaster startup requires terminal reseed retirement without pending journals")))
     observation)
    (otherwise (error "postmaster startup requires a recognized reseed observation"))))

(defun physical-reseed-follow-check-ordinary-startup ()
  (physical-reseed-check-before-pid)
  (physical-adoption-check-startup))

(defun physical-reseed-follow-own-startup (host port)
  (let ((before nil) (after nil))
    (postmaster-own-data-directory
     host port
     :startup-observer
     (lambda (lock phase)
       (ecase phase
         (:before-pid
          (setf before
                (physical-reseed-follow-check-terminal-startup
                 (physical-reseed-follow-startup-observation lock phase))))
         (:owned-pid
          (setf after (physical-reseed-follow-startup-observation lock phase before)))))
     :startup-reconcile
     (lambda ()
       (ecase (getf after :branch)
         (:ordinary (physical-adoption-reconcile-startup))
         ((:retirement :retired)
          (call-with-physical-reseed-follow-retirement-startup-owner
           after
           (lambda (owner)
             (physical-reseed-follow-retirement-publish-receipt owner)
             (physical-reseed-follow-retirement-consume-receipt owner)))))
       (physical-reseed-follow-check-ordinary-startup)))))
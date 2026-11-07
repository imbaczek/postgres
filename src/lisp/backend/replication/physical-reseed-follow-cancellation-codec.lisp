(in-package #:pg)

(defun physical-reseed-follow-cancellation-copy (value)
  (physical-reseed-install-check-literal value)
  (physical-follow-check-plist value '(:phase))
  (unless (eq :pending (getf value :phase))
    (error "target follow cancellation requires pending intent"))
  (list :phase :pending))

(defun physical-reseed-follow-check-forward-record (record)
  (when (getf record :cancellation)
    (error "target cancellation fences ordinary follow operations"))
  record)

(defun physical-reseed-follow-cancellation-check-transition (before after)
  (let ((old (getf before :cancellation)) (new (getf after :cancellation)))
    (unless (and new
                 (every (lambda (key) (equal (getf before key) (getf after key)))
                        '(:version :attempt :install :base :generations :configuration
                          :settlement :activation :retirement))
                 (or (null old) (equal old new)))
      (error "target follow cancellation cannot disappear or change its frozen predecessors"))
    t))

(defun physical-reseed-follow-cancellation-begin-record (value)
  (let* ((before (physical-reseed-follow-copy value))
         (candidate (copy-list before)))
    (unless (getf candidate :cancellation)
      (setf candidate (append candidate (list :cancellation (list :phase :pending)))))
    (setf candidate (physical-reseed-follow-copy candidate))
    (physical-reseed-follow-check-transition before candidate)
    candidate))

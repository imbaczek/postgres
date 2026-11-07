;; Encode pending cancellation and preserve its predecessor across transitions.

(in-package #:pg)

(defun physical-reseed-follow-cancellation-copy (value)
  ;; The durable cancellation marker has one canonical pending form.
  (physical-reseed-install-check-literal value)
  (physical-follow-check-plist value '(:phase))
  (unless (eq :pending (getf value :phase))
    (error "target follow cancellation requires pending intent"))
  (list :phase :pending))

(defun physical-reseed-follow-check-forward-record (record)
  ;; A pending marker fences preparation, settlement, activation and other
  ;; forward follow operations.  Recovery has its own admission path.
  (when (getf record :cancellation)
    (error "target cancellation fences ordinary follow operations"))
  record)

(defun physical-reseed-follow-cancellation-check-transition (before after)
  ;; Pending cancellation preserves all predecessor fields and cannot be removed.
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

(in-package #:pg)

(defun logical-slot-continuation-require-empty-inventory ()
  (when (logical-slot-startup-read-inventory :local-workspace t)
    (error "continuation restoration requires saved-slot proof before enabling active obligations"))
  t)

(defun logical-slot-continuation-empty-startup-snapshot (storage facts)
  (logical-slot-continuation-require-empty-inventory)
  (let* ((host (logical-slot-continuation-storage-call storage :host))
         (certificate (getf facts :certificate)))
    (list :version 1 :binding (logical-slot-continuation-storage-call storage :binding)
          :generation 1 :system-id (first host)
          :origin (list (getf certificate :child-timeline) (getf certificate :fork))
          :history-target (fifth host)
          :history (or (fourth host) (make-array 0 :element-type '(unsigned-byte 8)))
          :sealed-end (list (sixth host) (fifth host) :prefix-end)
          :slots nil :transfers nil :roots nil :runs nil :tail nil)))

(defun logical-slot-continuation-check-empty-startup (storage facts snapshot)
  (logical-slot-continuation-check snapshot)
  (logical-slot-continuation-require-empty-inventory)
  (let* ((host (logical-slot-continuation-storage-call storage :host))
         (certificate (getf facts :certificate))
         (history (logical-slot-continuation-history
                   (fifth host) (or (fourth host) (make-array 0 :element-type '(unsigned-byte 8)))))
         (selected (logical-slot-continuation-history
                    (getf snapshot :history-target) (getf snapshot :history))))
    (unless (and (equalp (getf snapshot :binding)
                         (logical-slot-continuation-storage-call storage :binding))
                 (eql (getf snapshot :system-id) (first host))
                 (equal (getf snapshot :origin)
                        (list (getf certificate :child-timeline) (getf certificate :fork)))
                 (physical-store-history-extends-p selected history)
                 (every (lambda (key) (null (getf snapshot key)))
                        '(:slots :transfers :roots :runs :tail)))
      (error "stable empty continuation selection differs from its actual host"))
    (logical-slot-continuation-position (getf snapshot :sealed-end) history (sixth host)))
  (logical-slot-continuation-storage-check storage)
  t)

(defun logical-slot-continuation-restore-empty-startup (gate ownership)
  ;; Only an actually empty durable inventory can be restored here.  Active
  ;; P7 rows, transfers, roots, runs and tails need proof, retention and a
  ;; provider before they can be accepted.  No transition or control mutation
  ;; or admission is performed here.
  ;;
  ;; Keep the actual origin proof and root-reader lease across publication and
  ;; readback.  Check host system ID, history, sealed end and binding against
  ;; the selected claims before and after sync.  Empty restoration does not
  ;; open the GC fence.
  (declare (notinline sb-posix:fsync))
  (call-with-logical-slot-continuation-startup-storage
   gate ownership
   (lambda (storage facts)
     (logical-slot-continuation-storage-call
      storage :with-origin
      (lambda (archive)
        (declare (ignore archive))
        (logical-slot-continuation-require-empty-inventory)
        (let ((names (logical-slot-continuation-selector-names storage)))
          (unless (member "selector" names :test #'equal)
            (let* ((snapshot (logical-slot-continuation-empty-startup-snapshot storage facts))
                   (image (logical-slot-continuation-encode snapshot)))
              (logical-slot-continuation-check-empty-startup storage facts snapshot)
              (logical-slot-continuation-install-initial-selector storage image)))
          (unless (equal '("selector") (logical-slot-continuation-selector-names storage))
            (error "stable continuation selector has an unresolved temporary"))
          (call-with-logical-slot-continuation-selector
           storage
           (lambda (snapshot fd selected-facts)
             (declare (ignore selected-facts))
             (logical-slot-continuation-check-empty-startup storage facts snapshot)
             (sb-posix:fsync fd)
             (sb-posix:fsync (logical-slot-continuation-storage-call storage :directory))
             (logical-slot-continuation-selector-point storage :after-selector-restoration-sync)
             (logical-slot-continuation-check-empty-startup storage facts snapshot)
             snapshot))))))))

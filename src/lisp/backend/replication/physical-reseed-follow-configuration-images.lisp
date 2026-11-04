(in-package #:pg)

;;; Derive disabled pending configuration and control images from a historical
;;; proved base. Image lists are alists of (logical-name . octets).

;; Build a disabled pending configuration from the proved generation and its
;; frozen source.  It does not establish a live source lease.
(defun physical-reseed-follow-pending-config (record binding)
  (let* ((record (physical-reseed-follow-copy record))
         (generations (getf record :generations)))
    (physical-follow-binding-check binding)
    (unless (and (= 1 (length generations)) (eq :proved (getf (first generations) :phase))
                 (eq :pending (getf binding :phase))
                 (equal (physical-reseed-follow-generation-anchor (first generations)) (getf binding :anchor)))
      (error "inert follow configuration requires its proved generation and pending binding"))
    (let ((source (physical-follow-source-copy (getf (getf record :attempt) :source))))
      (multiple-value-bind (profile owner) (physical-follow-source-profile source)
        (declare (ignore profile))
        (multiple-value-bind (host port) (physical-libpq-frozen-display-endpoint owner)
          (let ((config (physical-follow-with-controller
                         (list 1 host port (getf (first generations) :name) binding)
                         (list :version 1 :enabled nil :preparing nil :cleanup nil
                               :known-timeline (getf (getf binding :anchor) :source-timeline)
                               :source source))))
            (physical-follow-check-config config)
            config))))))

;; Return named byte images from the checked empty model and TIME (Unix seconds).
;; Selected history images are included only when the binding requires them.
(defun physical-reseed-follow-configuration-images (record binding model time)
  (let ((plain (copy-physical-store model))
        (before (getf binding :before)) (after (getf binding :after)))
    (setf (physical-store-selection plain) nil)
    (unless (and (equal before (physical-store-control-tuple plain))
                 (equal after (physical-store-control-tuple model))
                 (null (physical-store-directory plain)) (null (physical-store-stream plain))
                 (null (physical-store-lock-stream plain)) (null (physical-store-retained-root plain))
                 (not (physical-store-closed-p plain)) (not (physical-store-failed-p plain)))
      (error "inert follow images differ from their proved empty model"))
    (append
     (list (cons "follow.pending"
                 (physical-follow-config-image (physical-reseed-follow-pending-config record binding)))
           (cons "receive.initial" (physical-store-encode-control plain (fourth before)))
           (cons "receive.lock" (make-array 0 :element-type '(unsigned-byte 8)))
           (cons "recovery.initial" (physical-follow-binding-recovery-image binding time)))
     (unless (equal before after) (physical-follow-binding-selected-images binding model time)))))

;; Reprove the installed historical base before deriving images and binding.
;; The original installed phase-zero controls remain unchanged.
(defun physical-reseed-follow-configuration-images-at (owner root-fd value time)
  (let ((record (physical-reseed-follow-copy value)))
    (unless (typep time '(signed-byte 64)) (error "invalid inert follow image time"))
    (multiple-value-bind (binding model) (physical-reseed-follow-proof-at owner root-fd record)
      (values (physical-reseed-follow-configuration-images record binding model time) binding))))

;; Turn named bytes into sorted (name size digest) specifications for the journal.
(defun physical-reseed-follow-configuration-image-specifications (images core)
  (let* ((core (physical-reseed-follow-core-copy core))
         (specifications
           (loop for entry in images
                 collect
                 (progn
                   (unless (and (consp entry) (stringp (car entry))
                                (typep (cdr entry) '(vector (unsigned-byte 8)))
                                (physical-reseed-follow-configuration-image-size-p (car entry) (length (cdr entry))))
                     (error "invalid derived configuration image"))
                   (list (copy-seq (car entry)) (length (cdr entry))
                         (physical-store-update-digest #xCBF29CE484222325 (cdr entry)))))))
    (physical-reseed-follow-configuration-images-copy
     (sort specifications #'string< :key #'first) (physical-reseed-follow-configuration-selected-p core))))

(in-package #:pg)

;; Make the selected state durable, retire observed temporaries, then stabilize
;; the selector.  File and directory syncs preserve that order across crashes.

(defun logical-slot-continuation-cold-reconciliation-sync-row (context row)
  (declare (notinline sb-posix:fsync))
  (let ((root (logical-slot-continuation-cold-reconciliation-slot-root context))
        (name (first row)))
    (call-with-logical-slot-continuation-target-directory
     root name
     (lambda () (logical-slot-continuation-cold-reconciliation-check context))
     (lambda (directory check-directory)
       (call-with-logical-slot-copy-source-proof-fd
        directory "state" nil
        (lambda (fd facts)
          (unless (and (equal facts (third row))
                       (equalp (fourth row)
                               (logical-slot-copy-import-control-read-fd fd +slot-state-maximum-bytes+)))
            (error "cold reconciliation selected slot state changed before sync"))
          (funcall check-directory)
          (logical-slot-continuation-cold-reconciliation-point context :before-state-sync)
          (sb-posix:fsync fd)
          (logical-slot-continuation-cold-reconciliation-point context :after-state-sync)
          (setf (logical-slot-continuation-cold-reconciliation-active-slot context) name
                (logical-slot-continuation-cold-reconciliation-slot-removed context) nil)
          (call-with-logical-slot-continuation-temporary-retirement
           directory :slot-state (fifth row)
           (lambda (removed)
             (setf (logical-slot-continuation-cold-reconciliation-slot-removed context) removed)
             (logical-slot-continuation-cold-reconciliation-check context))
           (lambda (operation)
             (funcall operation :retire)
             (funcall operation :sync)
             (funcall operation :receipt)))
          (sb-sys:without-interrupts
            (setf (second row) (physical-image-file-facts directory)
                  (fifth row) nil
                  (logical-slot-continuation-cold-reconciliation-active-slot context) nil
                  (logical-slot-continuation-cold-reconciliation-slot-removed context) nil))
          (funcall check-directory)))))))

(defun logical-slot-continuation-cold-reconciliation-retire-selector (context)
  (let* ((storage (logical-slot-continuation-cold-reconciliation-storage context))
         (directory (logical-slot-continuation-storage-call storage :directory))
         (temporary (logical-slot-continuation-cold-reconciliation-temporary context)))
    (call-with-logical-slot-continuation-temporary-retirement
     directory :selector (when temporary (cdr temporary))
     (lambda (removed)
       (setf (logical-slot-continuation-cold-reconciliation-selector-removed context) removed)
       (logical-slot-continuation-cold-reconciliation-check context))
     (lambda (operation)
       (funcall operation :retire)
       (funcall operation :sync)
       (funcall operation :receipt)))
    (sb-sys:without-interrupts
      (setf (logical-slot-continuation-cold-reconciliation-temporary context) nil
            (logical-slot-continuation-cold-reconciliation-selector-removed context) nil))
    (logical-slot-continuation-cold-reconciliation-point context :selector-temporary-durable)))

(defun logical-slot-continuation-cold-reconciliation-stabilize (context)
  (declare (notinline sb-posix:fsync))
  (let* ((storage (logical-slot-continuation-cold-reconciliation-storage context))
         (directory (logical-slot-continuation-storage-call storage :directory))
         (observation (logical-slot-continuation-cold-reconciliation-observation context))
         (snapshot (getf observation :snapshot))
         (selected (logical-slot-continuation-cold-reconciliation-selector context))
         (image (logical-slot-continuation-encode snapshot)))
    (ecase (getf observation :kind)
      (:stable
       (call-with-logical-slot-copy-source-proof-fd
        directory "selector" nil
        (lambda (fd facts)
          (unless (and (equal facts (second selected)) (equalp image (third selected)))
            (error "cold reconciliation stable selector changed before sync"))
          (logical-slot-continuation-cold-reconciliation-point context :before-stable-sync)
          (sb-posix:fsync fd)
          (sb-posix:fsync directory)
          (logical-slot-continuation-cold-reconciliation-point context :after-stable-sync))))
      (:prepared
       (setf (logical-slot-continuation-cold-reconciliation-selector-resource context) :replacement)
       (call-with-logical-slot-continuation-file-replacement
        directory :selector (third selected) image
        (lambda () (logical-slot-continuation-cold-reconciliation-check context))
        (lambda (operation)
          (unless (equal (second selected) (funcall operation :receipt))
            (error "cold reconciliation PREPARED selector was substituted"))
          (funcall operation :prepare)
          (logical-slot-continuation-cold-reconciliation-point context :stable-temporary)
          (funcall operation :replace)
          (logical-slot-continuation-cold-reconciliation-point context :stable-selected)
          (funcall operation :sync)
          (sb-sys:without-interrupts
            (setf (logical-slot-continuation-cold-reconciliation-selector context)
                  (list "selector" (funcall operation :receipt) image)))))))
    (setf (logical-slot-continuation-cold-reconciliation-selector-resource context) nil)
    (logical-slot-continuation-cold-reconciliation-point context :stable-durable)))

(defun logical-slot-continuation-cold-reconciliation-execute-io (context)
  ;; As in slot.c RestoreSlotFromDisk and SaveSlotToPath, durable state needs
  ;; file and directory syncs.  Here the selected generation and exact typed
  ;; temporaries also determine what may be retired before stabilizing it.
  (declare (notinline sb-posix:fsync))
  (logical-slot-continuation-cold-reconciliation-point context :before-effects)
  (dolist (row (logical-slot-continuation-cold-reconciliation-ledger context))
    (logical-slot-continuation-cold-reconciliation-sync-row context row))
  (logical-slot-continuation-cold-reconciliation-point context :before-slot-root-sync)
  (sb-posix:fsync (logical-slot-continuation-cold-reconciliation-slot-root context))
  (logical-slot-continuation-cold-reconciliation-point context :after-slot-root-sync)
  (logical-slot-continuation-cold-reconciliation-retire-selector context)
  (logical-slot-continuation-cold-reconciliation-stabilize context)
  (logical-slot-continuation-cold-reconciliation-check context))

(defun logical-slot-continuation-cold-reconciliation-execute (context)
  (with-logical-slot-continuation-cold-reconciliation-check context
    (unless (eq :ready (logical-slot-continuation-cold-reconciliation-phase context))
      (error "cold reconciliation was already started"))
    (setf (logical-slot-continuation-cold-reconciliation-phase context) :working)
    (logical-slot-continuation-cold-reconciliation-execute-io context)
    (setf (logical-slot-continuation-cold-reconciliation-phase context) :complete)))

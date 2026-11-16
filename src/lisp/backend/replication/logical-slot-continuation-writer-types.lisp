(in-package #:pg)

(declaim (ftype function logical-slot-continuation-protection-call))

(defstruct (logical-slot-continuation-writer
            (:constructor %make-logical-slot-continuation-writer)
            (:print-function physical-libpq-print-private-object))
  ;; Opaque, registered operation; its state checks thread and scope.
  (state nil :read-only t))

(defstruct (logical-slot-continuation-writer-receipt
            (:constructor %make-logical-slot-continuation-writer-receipt)
            (:print-function physical-libpq-print-private-object))
  ;; Private publication capability, valid only for its registered writer.
  (writer nil :read-only t))

(defstruct (logical-slot-continuation-writer-control
            (:constructor %make-logical-slot-continuation-writer-control))
  ;; Outer lifetime and failure capability, retained through source cleanup.
  (state nil :read-only t))

(defvar *logical-slot-continuation-writer-controls* nil)

(defvar *logical-slot-continuation-writer-control-identities*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defun logical-slot-continuation-writer-control-call (control operation &optional value)
  (unless (logical-slot-continuation-writer-control-p control)
    (error "continuation writer requires its outer lifetime control"))
  (let ((state (logical-slot-continuation-writer-control-state control)))
    (unless (and (functionp state)
                 (eq state (gethash control *logical-slot-continuation-writer-control-identities*))
                 (member control *logical-slot-continuation-writer-controls* :test #'eq))
      (error "continuation writer has no outer lifetime registration"))
    (funcall state operation control value)))

(defstruct (logical-slot-continuation-writer-context
            (:constructor %make-logical-slot-continuation-writer-context))
  ;; Private state shared by the checked phases; callers cannot substitute it.
  writer receipt proof group record candidate ledger target-row
  slot name directory slot-root target-directory old-slot-image new-slot-image
  old-image prepared-image new-image selected-image selected-facts
  registry before after mirrors flags cell old-selection
  selector-resource slot-resource (phase :registered) (failed nil))

(defvar *logical-slot-continuation-writers* nil)
(defvar *logical-slot-continuation-writer-identities*
  (make-hash-table :test #'eq :weakness :key :synchronized t))
(defvar *logical-slot-continuation-writer-receipts*
  (make-hash-table :test #'eq :weakness :key :synchronized t))
(defvar *logical-slot-continuation-writer-lock*
  (sb-thread:make-mutex :name "continuation writer lifetime"))
(defvar *logical-slot-continuation-writer-test-hook* nil)

(defun logical-slot-continuation-writer-authenticate (writer)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (logical-slot-continuation-writer-p writer)
    (error "continuation writer requires its registered operation"))
  (let ((state (logical-slot-continuation-writer-state writer)))
    (unless (and (functionp state)
                 (eq state (gethash writer *logical-slot-continuation-writer-identities*))
                 (member writer *logical-slot-continuation-writers* :test #'eq))
      (error "continuation writer has no registered identity"))
    (funcall state :identity writer)
    state))

(defun logical-slot-continuation-writer-call (writer operation &optional first second)
  (let ((state (logical-slot-continuation-writer-authenticate writer)))
    (if (member operation '(:context :cancel-ready))
        (funcall state operation writer first second)
        (sb-sys:without-interrupts
          (let ((complete nil))
            (unwind-protect
                 (multiple-value-prog1
                     (sb-sys:with-local-interrupts
                       (funcall state operation writer first second))
                   (setf complete t))
              (unless complete (funcall state :fail writer))))))))

(defun logical-slot-continuation-writer-context-check (context)
  (unless (logical-slot-continuation-writer-context-p context)
    (error "continuation writer requires its private context"))
  (logical-slot-continuation-writer-call
   (logical-slot-continuation-writer-context-writer context) :context context)
  context)

(defmacro with-continuation-writer-context ((context &rest fields) &body body)
  (let ((value (gensym "WRITER-CONTEXT-")))
    `(let ((,value (logical-slot-continuation-writer-context-check ,context)))
       (symbol-macrolet
           ,(mapcar (lambda (field)
                      `(,field (,(intern (concatenate 'string
                                                      "LOGICAL-SLOT-CONTINUATION-WRITER-CONTEXT-"
                                                      (symbol-name field))
                                         (find-package "PG"))
                                ,value)))
             fields)
         ,@body))))

(defun logical-slot-continuation-writer-point (context point)
  (with-continuation-writer-context (context writer)
    (when *logical-slot-continuation-writer-test-hook*
      (funcall *logical-slot-continuation-writer-test-hook* point writer))))

(defun logical-slot-continuation-writer-bind-check-under-lock (writer group image)
  (logical-slot-continuation-writer-call writer :bind-check group image))

(defun logical-slot-continuation-writer-bind-cell-under-lock (writer group cell)
  (logical-slot-continuation-writer-authenticate writer)
  (unless (logical-slot-continuation-protection-call group :owns-cell (cons writer cell))
    (error "continuation writer requires the group's actual cell"))
  (logical-slot-continuation-writer-call writer :bind-cell group cell))

(defun logical-slot-continuation-writer-publish-under-lock (writer receipt)
  (logical-slot-continuation-writer-authenticate writer)
  (unless (and (logical-slot-continuation-writer-receipt-p receipt)
               (eq writer (logical-slot-continuation-writer-receipt-writer receipt))
               (eq writer (gethash receipt *logical-slot-continuation-writer-receipts*)))
    (error "continuation writer requires its registered publication receipt"))
  (logical-slot-continuation-writer-call writer :publish receipt))

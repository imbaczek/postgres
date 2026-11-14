(in-package #:pg)

(defconstant +logical-slot-copy-install-exchange+ 2)

(defstruct (logical-slot-copy-install-exchange-writer
            (:include logical-slot-copy-install-publisher)
            (:constructor %make-logical-slot-copy-install-exchange-writer)
            (:print-function physical-libpq-print-private-object))
  (receipt nil :read-only t) expected-phase pending armed)

(defvar *logical-slot-copy-install-exchange-test-hook* nil)

(defun logical-slot-copy-install-exchange-facts-p (before after)
  (and (= 9 (logical-slot-copy-descriptor-list-count before 9))
       (= 9 (logical-slot-copy-descriptor-list-count after 9))
       (equal (subseq before 0 7) (subseq after 0 7))
       (equal (nthcdr 8 before) (nthcdr 8 after))))

(defmethod logical-slot-copy-install-publisher-check-role ((writer logical-slot-copy-install-exchange-writer))
  (logical-slot-copy-install-check-fixed-origin writer)
  (let* ((record (logical-slot-copy-install-publisher-record writer))
         (receipt (logical-slot-copy-install-exchange-writer-receipt writer))
         (phase (logical-slot-copy-install-exchange-writer-expected-phase writer))
         (armed (logical-slot-copy-install-exchange-writer-armed writer))
         (layout (logical-slot-copy-install-read-layout
                  (logical-slot-copy-install-publisher-operation writer)
                  (logical-slot-copy-install-publisher-image writer))))
    (logical-slot-copy-install-check-row-receipt
     (logical-slot-copy-install-publisher-owner writer) (logical-slot-copy-install-publisher-context writer)
     (logical-slot-copy-install-publisher-input writer) receipt :state (if armed :spent :available))
    (unless (and (member phase '(:pending :active)) (eq phase (getf layout :phase))
                 (= (getf layout :prefix) (logical-slot-copy-install-row-receipt-prefix receipt))
                 (= (getf layout :prefix)
                    (length (getf (logical-slot-copy-install-decode
                                   (logical-slot-copy-install-publisher-image writer)) :entries)))
                 (if armed (eq phase :active)
                     (and (eq phase (logical-slot-copy-install-row-receipt-phase receipt))
                          (or (eq phase :pending)
                              (eq :cold (logical-slot-copy-install-input-state-origin record))))))
      (error "logical installation exchange phase or complete prefix changed")))
  writer)

(defmethod logical-slot-copy-install-publisher-check-closed-role ((writer logical-slot-copy-install-exchange-writer))
  (logical-slot-copy-install-publisher-check-role writer))

(defun logical-slot-copy-install-check-exchange-writer (writer)
  (unless (logical-slot-copy-install-exchange-writer-p writer)
    (error "logical installation requires an exchange writer"))
  (logical-slot-copy-install-check-publisher writer))

(defun logical-slot-copy-install-exchange-point (writer point)
  (when *logical-slot-copy-install-exchange-test-hook*
    (funcall *logical-slot-copy-install-exchange-test-hook* point writer))
  (logical-slot-copy-install-check-exchange-writer writer))

(defun logical-slot-copy-install-exchange-bind (writer)
  (let* ((root (logical-slot-copy-install-publisher-root writer))
         (container (logical-slot-copy-install-publisher-open writer root "logical-slot-copy.install" :directory))
         (journal (logical-slot-copy-install-publisher-open writer container "journal" :file))
         (canonical (logical-slot-copy-install-publisher-open writer root "logical-slot-copy.import" :file))
         (other (logical-slot-copy-install-publisher-open writer container "active" :file)))
    (setf (logical-slot-copy-install-publisher-container writer) container
          (logical-slot-copy-install-publisher-journal writer) journal
          (logical-slot-copy-install-publisher-active writer)
          (if (eq :pending (logical-slot-copy-install-exchange-writer-expected-phase writer)) other canonical)
          (logical-slot-copy-install-exchange-writer-pending writer)
          (if (eq :pending (logical-slot-copy-install-exchange-writer-expected-phase writer)) canonical other))
    (logical-slot-copy-install-publisher-proof writer)))

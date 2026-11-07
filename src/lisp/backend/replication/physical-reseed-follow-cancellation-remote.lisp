;;; Drive source cancellation and selected cleanup from the saved local intent.
;;; Publish completion only after the required source operations are confirmed.

(in-package #:pg)

(defun physical-reseed-follow-observe-cancellation-source-held (operation expected &key helper)
  (let* ((record (getf (getf expected :follow) :record))
         (request (physical-backup-lease-request-copy (getf (getf record :attempt) :request))))
    (unless (eq :pending (getf (getf record :cancellation) :phase))
      (error "source cleanup observation requires pending cancellation"))
    (call-with-physical-reseed-follow-cancellation-source
     operation expected
     (lambda (client rpc)
       (multiple-value-bind (action observation)
           (physical-reseed-follow-cancellation-source-action
            record (funcall rpc
                            (lambda ()
                              (pg-client-native-backup-lease-status
                               client (physical-backup-lease-request-copy request)))))
         (let ((result
                 (case action
                   (:abort
                    (funcall rpc
                             (lambda ()
                               (pg-client-native-backup-lease-abort
                                client (physical-backup-lease-request-copy request)))))
                   (:cleanup-selected
                    (funcall rpc
                             (lambda ()
                               (pg-client-native-backup-lease-cleanup-selected
                                client (physical-backup-lease-request-copy request)
                                (physical-backup-lease-successor-copy
                                 (getf observation :successor) request)))))
                   (otherwise observation))))
           (multiple-value-bind (receipt classification)
               (physical-reseed-follow-cancellation-source-observation-copy record result)
             (when (and (not (member classification '(:unknown :unbound)))
                        (eq :closed (getf receipt :phase)))
               receipt)))))
     :helper helper)))

;; Observe or close the source, save a closed receipt, then obtain operation
;; evidence before recording CLEANED.  Unresolved and deferred work is returned.

(defun physical-reseed-follow-cleanup-cancellation (owner &key helper)
  (call-with-physical-reseed-follow-cancellation-operation
   owner
   (lambda (operation)
     (let ((snapshot (physical-reseed-follow-cancellation-operation-call operation :snapshot)))
       (when (eq :pending (getf (getf (getf (getf snapshot :follow) :record) :cancellation) :phase))
         (let ((receipt (physical-reseed-follow-observe-cancellation-source-held
                         operation snapshot :helper helper)))
           (when receipt
             (setf snapshot
                   (nth-value 1
                              (physical-reseed-follow-record-cancellation-source-closed-held
                               operation snapshot receipt))))))
       (let* ((record (getf (getf snapshot :follow) :record))
              (action (physical-reseed-follow-cancellation-source-action record nil)))
         (case action
           (:complete (values record :complete))
           (:unresolved (values record :unresolved))
           (:defer (values record :deferred))
           ((:abort :release)
            (call-with-physical-reseed-follow-cleanup-operation-evidence
             operation snapshot
             (lambda (evidence)
               (values (physical-reseed-follow-record-cancellation-cleaned-held
                        operation snapshot evidence) :complete))
             :helper helper))
           (otherwise (error "cancellation source action has no cleanup continuation"))))))))

(defun physical-reseed-follow-recover-cleaned-cancellation (owner &key helper)
  (%call-with-physical-reseed-follow-cancellation-operation
   owner
   (lambda (operation)
     (let* ((snapshot (physical-reseed-follow-cancellation-operation-call operation :snapshot))
            (record (getf (getf snapshot :follow) :record)))
       (cond
         ((getf snapshot :temporary)
          (call-with-physical-reseed-follow-cleanup-operation-evidence
           operation snapshot
           (lambda (evidence)
             (values (physical-reseed-follow-record-cancellation-cleaned-held
                      operation snapshot evidence) :complete))
           :helper helper))
         ((eq :cleaned (getf (getf record :cancellation) :phase))
          (values record :complete))
         (t (error "no complete cleanup acknowledgment temporary to recover")))))
   :cleaned))

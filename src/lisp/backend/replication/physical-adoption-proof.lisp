(in-package #:pg)

(defun physical-adoption-attempt-proof-check (owner record function &key remote)
  (handler-case (funcall function)
    (error (condition)
      (unless (and remote (physical-client-retryable-condition-p condition))
        (unless (equal record (physical-adoption-attempt-read owner))
          (error "adoption intent changed while rejecting its proof"))
        (physical-adoption-attempt-advance owner record :aborting))
      (error condition))))

(defun physical-adoption-attempt-check-proof-context (owner record)
  (physical-adoption-attempt-validate-context owner record)
  (unless (equal record (physical-adoption-attempt-read owner))
    (error "adoption intent changed during proof validation"))
  t)

(defun physical-adoption-attempt-reobserve-source (client record)
  (multiple-value-bind (timeline history)
      (physical-adoption-attempt-observe-source client (getf record :source) (getf record :local))
    (unless (and (= timeline (getf (getf record :request) :source-timeline))
                 (equal history (getf record :source-history)))
      (error "adoption source no longer matches its saved timeline and history")))
  t)

(defun physical-adoption-attempt-proved-copy (record anchor proof)
  (unless (and (equal (getf anchor :history-text) (getf record :source-history))
               (equal (getf proof :history-text) (getf record :source-history)))
    (error "adoption source returned different proof history"))
  (let ((candidate (copy-list record)))
    (setf (getf candidate :phase) :proved
          (getf candidate :anchor)
          (physical-adoption-attempt-fields-copy anchor (remove :history-text *physical-follow-anchor-keys*))
          (getf candidate :proof)
          (physical-adoption-attempt-fields-copy
           proof (remove :history-text (mapcar #'second *physical-origin-proof-result-fields*))))
    (setf candidate (physical-adoption-attempt-copy candidate))
    (when (and (eq :proved (getf record :phase)) (not (equal candidate record)))
      (error "adoption source changed the saved proof"))
    candidate))

(defun physical-adoption-attempt-resume-proof (owner &key client)
  (let ((record (physical-adoption-attempt-read owner)))
    (unless (member (getf record :phase) '(:preparing :proved))
      (error "cancelled adoption attempts cannot resume proof"))
    (flet ((check (function)
             (physical-adoption-attempt-proof-check owner record function))
           (remote (function)
             (physical-adoption-attempt-proof-check owner record function :remote t)))
      (check (lambda () (physical-adoption-attempt-check-proof-context owner record)))
      (let ((candidate
              (call-with-physical-adoption-attempt-client
               record client
               (lambda (channel)
                 (remote (lambda () (physical-adoption-attempt-reobserve-source channel record)))
                 (check (lambda () (physical-adoption-attempt-check-proof-context owner record)))
                 (let ((anchor (remote
                                (lambda ()
                                  (pg-client-native-prepare channel (getf (getf record :stage) :source-pin)
                                                            (getf record :request))))))
                   (check (lambda ()
                            (unless (equal (getf anchor :history-text) (getf record :source-history))
                              (error "adoption PREPARE returned different history"))))
                   (let* ((proof (remote
                                  (lambda ()
                                    (pg-client-native-prove-retained
                                     channel (getf (getf record :stage) :source-pin) anchor
                                     (physical-adoption-attempt-origin-request (getf record :origin-proof))))))
                          (proved (check (lambda () (physical-adoption-attempt-proved-copy record anchor proof)))))
                     (remote (lambda () (physical-adoption-attempt-reobserve-source channel record)))
                     proved))))))
        (check (lambda () (physical-adoption-attempt-check-proof-context owner record)))
        (if (eq :proved (getf record :phase))
            record
            (physical-adoption-attempt-publish owner candidate :previous record))))))

(defun physical-adoption-attempt-start-proof (state image client profile)
  (let* ((record (physical-adoption-attempt-create state image client profile))
         (owner (getf state :owner)))
    (physical-adoption-attempt-publish owner record)
    (physical-adoption-attempt-resume-proof owner :client client)))

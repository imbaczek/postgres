(in-package #:pg-physical-tests)

(defun call-with-reseed-settlement-source (owner root record function)
  (let* ((request (getf (getf record :attempt) :request))
         (source (getf (getf record :attempt) :source))
         (generation (first (getf record :generations)))
         (coordinates (getf generation :coordinates))
         (successor (getf (getf record :settlement) :successor))
         (base (pg::physical-reseed-follow-settlement-base record))
         (token (gensym "SETTLEMENT-CLIENT"))
         (state (list :phase :ready :outcome :handed-off :statuses 0 :handoffs 0 :closes 0 :identities 0
                      :lose-reply nil :identity-timeline nil :returned-receipt nil :on-close nil))
         (symbols '(pg::physical-libpq-connect-frozen pg::physical-client-close
                    pg::physical-adoption-attempt-check-client pg::physical-client-identity-row
                    pg::pg-client-native-history pg::pg-client-native-backup-lease-status
                    pg::pg-client-native-backup-lease-handoff))
         (originals (mapcar #'symbol-function symbols)))
    (labels ((network (client)
               (assert (eq token client))
               (assert (pg::physical-reseed-follow-owner-busy-p owner))
               (assert (null pg::*physical-reseed-follow-installation-scope*)))
             (status ()
               (let ((closed (eq :closed (getf state :phase))))
                 (pg::physical-backup-lease-copy
                  (list :version 1 :request request :phase (getf state :phase) :base base
                        :outcome (when closed (getf state :outcome))
                        :successor (when (and closed (eq :handed-off (getf state :outcome))) successor))))))
      (unwind-protect
           (progn
             (setf (symbol-function (first symbols))
                   (lambda (helper frozen &key on-spawn)
                     (assert (string= helper "/unused/helper"))
                     (assert (equal (getf source :owner) (pg::physical-libpq-frozen-profile-datum frozen)))
                     (network token)
                     (when on-spawn (funcall on-spawn token))
                     token)
                   (symbol-function (second symbols))
                   (lambda (client)
                     (network client)
                     (incf (getf state :closes))
                     (when (getf state :on-close) (funcall (getf state :on-close))))
                   (symbol-function (third symbols))
                   (lambda (client saved)
                     (network client) (assert (equal source saved)) t)
                   (symbol-function (fourth symbols))
                   (lambda (client)
                     (network client)
                     (incf (getf state :identities))
                     (vector (write-to-string (getf coordinates :system-id))
                             (write-to-string (or (getf state :identity-timeline)
                                                  (getf coordinates :source-timeline)))
                             (pg::wal-format-lsn (+ 128 (getf coordinates :receive-lsn))) nil))
                   (symbol-function (fifth symbols))
                   (lambda (client timeline)
                     (network client)
                     (assert (= timeline (getf coordinates :source-timeline)))
                     (pg::string->bytes (getf generation :source-history)))
                   (symbol-function (sixth symbols))
                   (lambda (client supplied)
                     (network client) (assert (equal request supplied))
                     (incf (getf state :statuses))
                     (setf (getf state :returned-receipt) (status)))
                   (symbol-function (seventh symbols))
                   (lambda (client supplied selected)
                     (network client)
                     (assert (equal request supplied))
                     (assert (equal successor selected))
                     (assert (equal record (pg::physical-reseed-follow-decode
                                            (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
                     (incf (getf state :handoffs))
                     (setf (getf state :phase) :closed)
                     (when (getf state :lose-reply)
                       (setf (getf state :lose-reply) nil)
                       (error "source committed handoff but its reply was lost"))
                     (setf (getf state :returned-receipt) (status))))
             (funcall function state))
        (loop for symbol in symbols for original in originals
              do (setf (symbol-function symbol) original))))))

(deftest reseed-follow-settlement-remote-handoff-preserves-pending-until-publication
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
         (call-with-reseed-settlement-source
          owner root pending
          (lambda (state)
            (multiple-value-bind (receipt snapshot)
                (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper")
              (ok (eq :closed (getf receipt :phase)))
              (ok (eq :handed-off (getf receipt :outcome)))
              (ok (equal pending (getf (getf snapshot :follow) :record)))
              (ok (equal pending (pg::physical-reseed-follow-read owner)))
              (ok (= 1 (getf state :statuses) (getf state :handoffs) (getf state :closes))))
            (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))))))

(deftest reseed-follow-settlement-remote-lost-reply-reconciles-historical-status
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
         (call-with-reseed-settlement-source
          owner root pending
          (lambda (state)
            (setf (getf state :lose-reply) t)
            (ok (intake-fails-p
                 (lambda () (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper"))))
            (ok (equal pending (pg::physical-reseed-follow-read owner)))
            (let ((identities (getf state :identities)))
              (setf (getf state :identity-timeline) 99)
              (let ((receipt (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper")))
                (ok (eq :closed (getf receipt :phase)))
                (ok (eq :handed-off (getf receipt :outcome))))
              (ok (= identities (getf state :identities))))
            (ok (= 1 (getf state :handoffs))))
          )))))

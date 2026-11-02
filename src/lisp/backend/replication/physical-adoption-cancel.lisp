;;; Resumable cancellation of an offline adoption attempt.
;;;
;;; Save cancellation intent before contacting the source. Keep the startup fence
;;; until the source acknowledges retirement and the local record can be removed
;;; durably. A damaged stage must not prevent cleanup of the saved remote intent.

(in-package #:pg)

(define-condition physical-adoption-retirement-uncertain (error)
  ((cause :initarg :cause :reader physical-adoption-retirement-cause))
  (:documentation "Remote retirement was acknowledged, but local completion is uncertain.")
  (:report (lambda (condition stream)
             (declare (ignore condition))
             (write-string "remote adoption pin retired; local cancellation completion is uncertain" stream))))

(defun physical-adoption-attempt-read-if-present (owner)
  "Read the owned attempt, returning NIL if it is absent."
  (physical-adoption-check-owner owner)
  (handler-case (physical-adoption-attempt-read owner)
    (sb-posix:syscall-error (condition)
      (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
        (error condition)))))

(defun physical-adoption-attempt-advance (owner record phase)
  "Durably publish the next phase without changing the saved intent."
  (let ((candidate (copy-list record)))
    (setf (getf candidate :phase) phase)
    (physical-adoption-attempt-publish owner candidate :previous record)))

(defun call-with-physical-adoption-attempt-client (record supplied function)
  "Call FUNCTION on the saved route, closing only a client opened here."
  (let ((source (getf record :source)))
    (if supplied
        (progn
          (physical-adoption-attempt-check-client supplied source)
          (funcall function supplied))
        (let ((client nil))
          (unwind-protect
               (multiple-value-bind (profile frozen) (physical-follow-source-profile source)
                 (declare (ignore profile))
                 (setf client
                       (physical-libpq-connect-frozen
                        (physical-libpq-helper-path :on-spawn (lambda (resolver) (setf client resolver)))
                        frozen :on-spawn (lambda (channel) (setf client channel))))
                 (physical-adoption-attempt-check-client client source)
                 (funcall function client))
            (when client (physical-client-close client)))))))

(defun physical-adoption-attempt-clear-aborted (owner record)
  "Remove a durably aborted attempt and sync its directory."
  (unless (eq :aborted (getf record :phase))
    (error "only a durably aborted attempt can be removed"))
  (handler-case
      (let* ((directory (physical-adoption-owner-directory
                         (physical-adoption-attempt-check-owner owner record)))
             (path (physical-adoption-attempt-path directory)))
        (unless (equal record (physical-adoption-attempt-read owner))
          (error "adoption attempt changed before local retirement"))
        (physical-adoption-check-no-install directory)
        (physical-adoption-attempt-test-point :before-unlink path)
        (handler-case (sb-posix:unlink (namestring path))
          (sb-posix:syscall-error (condition)
            (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
              (error condition))))
        (physical-adoption-attempt-test-point :after-unlink path)
        (fsync-directory directory)
        (physical-adoption-attempt-test-point :after-removal-directory-fsync path))
    (error (condition)
      (error 'physical-adoption-retirement-uncertain :cause condition)))
  :retired)

(defun physical-adoption-attempt-cancel (owner &key client)
  "Retire the saved remote intent and then remove its durable local record.
An absent record returns :ABSENT without making a claim about the remote source.
After an ABORT acknowledgment, failures signal retirement-uncertain."
  (physical-adoption-check-no-install (physical-adoption-owner-directory (physical-adoption-check-owner owner)))
  (let ((record (physical-adoption-attempt-read-if-present owner)))
    (unless record
      (fsync-directory (physical-adoption-owner-directory owner))
      (return-from physical-adoption-attempt-cancel :absent))
    (when (member (getf record :phase) '(:installing :installed))
      (error "installation authority cannot be cancelled as an ordinary attempt"))
    (unless (member (getf record :phase) '(:aborting :aborted))
      (setf record (physical-adoption-attempt-advance owner record :aborting)))
    (unless (eq :aborted (getf record :phase))
      (let ((acknowledged nil))
        (handler-case
            (progn
              (call-with-physical-adoption-attempt-client
               record client
               (lambda (channel)
                 (let ((identity (physical-client-identity-row channel)))
                   (unless (= (getf (getf record :local) :system-id)
                              (pg-client-native-u64 (aref identity 0)))
                     (error "adoption cancellation source has a different system identity")))
                 (physical-adoption-attempt-check-client channel (getf record :source))
                 (physical-adoption-attempt-check-owner owner record)
                 (unless (equal record (physical-adoption-attempt-read owner))
                   (error "adoption cancellation intent changed before remote retirement"))
                 (physical-adoption-check-no-install (physical-adoption-owner-directory owner))
                 (pg-client-native-abort channel (getf (getf record :stage) :source-pin)
                                         (getf record :request))
                 (setf acknowledged t)))
              (setf record (physical-adoption-attempt-advance owner record :aborted)))
          (error (condition)
            (if acknowledged
                (error 'physical-adoption-retirement-uncertain :cause condition)
                (error condition))))))
    (physical-adoption-attempt-clear-aborted owner record)))

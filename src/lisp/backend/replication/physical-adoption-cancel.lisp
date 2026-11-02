(in-package #:pg)

(define-condition physical-adoption-retirement-uncertain (error)
  ((cause :initarg :cause :reader physical-adoption-retirement-cause))
  (:report (lambda (condition stream)
             (declare (ignore condition))
             (write-string "remote adoption pin retired; local cancellation completion is uncertain" stream))))

(defun physical-adoption-attempt-read-if-present (owner)
  (physical-adoption-check-owner owner)
  (handler-case (physical-adoption-attempt-read owner)
    (sb-posix:syscall-error (condition)
      (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
        (error condition)))))

(defun physical-adoption-attempt-advance (owner record phase)
  (let ((candidate (copy-list record)))
    (setf (getf candidate :phase) phase)
    (physical-adoption-attempt-publish owner candidate :previous record)))

(defun call-with-physical-adoption-attempt-client (record supplied function)
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
  (unless (eq :aborted (getf record :phase))
    (error "only a durably aborted attempt can be removed"))
  (handler-case
      (let* ((directory (physical-adoption-owner-directory
                         (physical-adoption-attempt-check-owner owner record)))
             (path (physical-adoption-attempt-path directory)))
        (unless (equal record (physical-adoption-attempt-read owner))
          (error "adoption attempt changed before local retirement"))
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
  (let ((record (physical-adoption-attempt-read-if-present owner)))
    (unless record
      (fsync-directory (physical-adoption-owner-directory owner))
      (return-from physical-adoption-attempt-cancel :absent))
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
                 (pg-client-native-abort channel (getf (getf record :stage) :source-pin)
                                         (getf record :request))
                 (setf acknowledged t)))
              (setf record (physical-adoption-attempt-advance owner record :aborted)))
          (error (condition)
            (if acknowledged
                (error 'physical-adoption-retirement-uncertain :cause condition)
                (error condition))))))
    (physical-adoption-attempt-clear-aborted owner record)))

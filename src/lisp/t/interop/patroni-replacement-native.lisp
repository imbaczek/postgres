(in-package #:pg)

(defun native-replacement-request (old-directory index)
  (let ((request
          (call-with-physical-reseed-follow-cancellation-owner
           old-directory
           (lambda (owner)
             (physical-backup-lease-request-copy
              (getf (getf (physical-reseed-follow-read-configured-cancellation owner) :attempt)
                    :request))))))
    (setf (getf request :name) (string-downcase (format nil "reseed_~32,'0x" index))
          (getf request :token) (string-downcase (format nil "~64,'0x" (+ index 1000))))
    request))

(defun native-replacement-crash-after-journal (old-directory target stage index port helper)
  (let ((request (native-replacement-request old-directory index))
        (profile (make-physical-libpq-profile
                  (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres" port)
                  :mode :physical))
        (*physical-reseed-remote-test-hook*
          (lambda (event)
            (when (eq event :after-journal)
              (sb-ext:exit :code 93 :abort t)))))
    (physical-reseed-start-configured-replacement-remote
     old-directory target stage request profile :helper helper)
    (error "replacement crash point was not reached")))

(defun native-replacement-reopen-after-crash (old-directory target stage index port helper &optional aborted)
  (let ((request (native-replacement-request old-directory index))
        (profile (make-physical-libpq-profile
                  (format nil "host=127.0.0.1 port=~d user=postgres dbname=postgres" port)
                  :mode :physical)))
    (call-with-physical-reseed-owner
     target
     (lambda (owner)
       (let ((record (physical-reseed-attempt-read owner)))
         (assert (eq (if aborted :aborted :premanifest) (getf record :phase)))
         (assert (equal request (getf record :request))))))
    (handler-case
        (progn
          (physical-reseed-start-configured-replacement-remote
           old-directory target stage request profile :helper helper)
          (error "replacement unexpectedly repeated CAPTURE"))
      (error (condition)
        (assert (search "replacement target requires" (princ-to-string condition)))))
    (call-with-physical-reseed-owner
     target
     (lambda (owner)
       (let ((source (physical-reseed-status owner :helper helper)))
         (if aborted
             (assert (and (eq :closed (getf source :phase)) (eq :aborted (getf source :outcome))))
             (assert (null source))))
       (assert (eq :aborted (getf (physical-reseed-cancel owner :helper helper) :phase)))))
    (write-line "REPLACEMENT-CRASH-REOPEN-PASS")))

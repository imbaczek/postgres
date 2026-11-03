(in-package #:pg-physical-tests)

(defun reseed-remote-test-record ()
  (append (reseed-attempt-test-record)
          (list :stage (list :directory "/tmp/reseed-receive-stage/" :device 1 :inode 7))))

(cl-spec:defproperty reseed-stage-identity-survives-private-journal-roundtrip
    ((inode (range integer 1 18446744073709551615)) (phase (range integer 0 2)))
  (:about pg::physical-reseed-attempt-copy pg::physical-reseed-attempt-stage-copy)
  (:trials (:normal 100))
  (let ((record (reseed-remote-test-record)))
    (setf (getf record :phase) (nth phase '(:premanifest :aborting :aborted))
          (getf (getf record :stage) :inode) inode)
    (equal (pg::physical-reseed-attempt-copy record)
           (pg::physical-reseed-attempt-decode (pg::physical-reseed-attempt-image record)))))

(deftest generated-reseed-stage-journal-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-stage-identity-survives-private-journal-roundtrip
                                         :seed 95 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-stage-codec-freezes-a-separate-stage-without-requiring-live-files
    (let* ((record (reseed-remote-test-record))
           (copy (pg::physical-reseed-attempt-copy record)))
      (ok (equal record copy))
      (ok (not (eq (getf (getf record :stage) :directory) (getf (getf copy :stage) :directory))))
      (dolist (change '((:directory "/tmp/reseed-receive-replaced/") (:inode 8)))
        (let ((changed (copy-tree record)))
          (setf (getf (getf changed :stage) (first change)) (second change))
          (ok (intake-fails-p
               (lambda () (pg::physical-reseed-attempt-check-transition copy changed))))))
      (dolist (bad (list nil
                         (list :directory "/tmp/reseed-attempt/" :device 1 :inode 7)
                         (list :directory "/tmp/" :device 1 :inode 7)
                         (list :directory "/tmp/reseed-attempt/child/" :device 1 :inode 7)
                         (list :directory "/tmp/separate/" :device 2 :inode 7)
                         (list :directory "/tmp/reseed-attempt/../elsewhere/" :device 1 :inode 7)))
        (let ((changed (copy-tree record)))
          (setf (getf changed :stage) bad)
          (ok (intake-fails-p (lambda () (pg::physical-reseed-attempt-copy changed))))))
      (let ((legacy (reseed-attempt-test-record)))
        (ok (equal legacy (pg::physical-reseed-attempt-decode (pg::physical-reseed-attempt-image legacy)))))
      (ok (intake-fails-p
           (lambda () (pg::physical-reseed-attempt-check-transition (reseed-attempt-test-record) record))))))


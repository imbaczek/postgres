(in-package #:pg-physical-tests)

(defun adoption-handoff-codec-fixture ()
  (let* ((plan (adoption-install-codec-fixture))
         (stage (getf (getf plan :attempt) :stage))
         (store (pg::%make-physical-store :system-id (getf stage :system-id)
                                          :timeline (getf stage :timeline)
                                          :base-lsn (getf stage :end) :floor-lsn (getf stage :end)
                                          :written-lsn (getf stage :end) :flushed-lsn (getf stage :end)))
         (bytes (pg::physical-store-encode-control store (getf stage :end))))
    (copy-tree (list :version 1 :installation plan
                     :attempt-file '(:device 1 :inode 4000 :size 123 :digest 456)
                     :journal-file '(:device 1 :inode 8 :size 789 :digest 1011)
                     :workspace '(:directory "/tmp/adoption-handoff-codec/" :device 1 :inode 4001
                                  :lock-inode 4002 :fence-inode 4003)
                     :spool '(:device 1 :inode 4004)
                     :spool-entries
                     (list (list "receive.control"
                                 (list (list "receive.control" :file #o600 1 4005 68
                                             (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
                           (list "receive.lock"
                                 (list (list "receive.lock" :file #o600 1 4006 0 #xCBF29CE484222325))))
                     :intent-file '(:device 1 :inode 4007)))))

(deftest adoption-handoff-codec-roundtrips-a-frozen-empty-spool
    (let ((plan (adoption-handoff-codec-fixture)))
      (ok (equal plan (pg::physical-adoption-handoff-decode
                       (pg::physical-adoption-handoff-image plan))))))

(deftest adoption-handoff-codec-rejects-rebinding-aliases-and-nonempty-input
    (dolist (mode '(:version :key :alias :mount :nested-root :nested-workspace
                    :journal :spool-name :spool-mode :spool-size :spool-digest))
      (let ((plan (adoption-handoff-codec-fixture)))
        (ecase mode
          (:version (setf (getf plan :version) 2))
          (:key (setf plan (append plan '(:unexpected 1))))
          (:alias (setf (getf (getf plan :spool) :inode) 4000))
          (:mount (setf (getf (getf plan :spool) :mount) 123))
          (:nested-root (setf (getf (getf plan :workspace) :directory)
                              (concatenate 'string
                                           (getf (getf (getf (getf plan :installation) :attempt) :local) :directory)
                                           "child/")))
          (:nested-workspace (setf (getf (getf plan :workspace) :directory)
                                   "/tmp/adoption-codec-work/child/"))
          (:journal (incf (getf (getf plan :journal-file) :inode)))
          (:spool-name (setf (first (first (getf plan :spool-entries))) "other"))
          (:spool-mode (setf (third (first (second (first (getf plan :spool-entries))))) #o644))
          (:spool-size (incf (sixth (first (second (first (getf plan :spool-entries)))))))
          (:spool-digest (incf (seventh (first (second (first (getf plan :spool-entries))))))))
        (ok (intake-fails-p (lambda () (pg::physical-adoption-handoff-image plan)))
            (symbol-name mode)))))

(cl-spec:defproperty adoption-handoff-intent-rejects-truncated-envelopes
    ((index (range integer 0 100000)))
  (:about pg::physical-adoption-handoff-decode)
  (:trials (:normal 100))
  (let* ((plan (adoption-handoff-codec-fixture))
         (bytes (pg::physical-adoption-handoff-image plan))
         (cut (mod index (length bytes))))
    (and (equal plan (pg::physical-adoption-handoff-decode bytes))
         (intake-fails-p
          (lambda () (pg::physical-adoption-handoff-decode (subseq bytes 0 cut)))))))

(deftest generated-adoption-handoff-intent-truncation
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-handoff-intent-rejects-truncated-envelopes :seed 82))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest adoption-handoff-intent-rejects-corruption-extra-data-and-malformed-headers
    (let* ((bytes (pg::physical-adoption-handoff-image (adoption-handoff-codec-fixture)))
           (corrupt (copy-seq bytes)))
      (setf (aref corrupt (- (length corrupt) 3)) (logxor 1 (aref corrupt (- (length corrupt) 3))))
      (dolist (image (list corrupt
                           (concatenate '(vector (unsigned-byte 8)) bytes #(0))
                           (pg::string->bytes "PGADOPTIONHANDOFF1 0 00000000
")
                           (pg::string->bytes "PGADOPTIONHANDOFF1 999999999 00000000
")))
        (ok (intake-fails-p (lambda () (pg::physical-adoption-handoff-decode image)))))))

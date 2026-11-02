(in-package #:pg-physical-tests)

(defun adoption-follow-source-fixture (distance generations)
  (let* ((handoff (adoption-handoff-codec-fixture))
         (record (getf (getf handoff :installation) :attempt))
         (end (getf (getf record :stage) :end))
         (fork (+ end distance))
         (source (1+ generations))
         (history (pg::timeline-encode-history 2 1 fork "first planned split" nil))
         (anchor (getf record :anchor))
         (proof (getf record :proof)))
    (loop for child from 3 to source
          do (setf history (pg::timeline-encode-history
                            child (1- child) (+ fork (* 64 (- child 2))) "later planned split" history)))
    (setf (getf record :source-history) (pg::bytes->string history)
          (getf (getf record :request) :source-timeline) source
          (getf anchor :source-timeline) source
          (getf anchor :eor-octets) (coerce (pg::wal-encode-end-of-recovery-record fork 42 2 1 1) 'list)
          (getf proof :source-timeline) source
          (getf proof :fork) (+ fork (* 64 (1- generations))))
    (setf (getf proof :anchor-digest)
          (pg::physical-follow-anchor-digest (pg::physical-adoption-follow-record-anchor record)))
    (pg::physical-adoption-handoff-copy handoff)))

(defun adoption-follow-codec-fixture (&key (distance 64) (generations 1) reuse)
  (let* ((handoff (adoption-follow-source-fixture distance generations))
         (record (getf (getf handoff :installation) :attempt))
         (binding (pg::physical-adoption-follow-planned-binding record))
         (images (pg::physical-adoption-follow-images record binding 12345))
         (wal (assoc "history.wal" images :test #'equal)))
    (when reuse
      (assert wal)
      (let* ((name (concatenate 'string "pg_wal/" (pg::timeline-history-filename (eighth (getf binding :after)))))
             (bytes (cdr wal))
             (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
        (loop for role in '(:stage-tree :new) for inode from 4500
              do (let ((entry (assoc "pg_wal" (getf (getf handoff :installation) role) :test #'equal)))
                   (setf (second entry)
                         (append (second entry) (list (list name :file #o600 1 inode (length bytes) digest))))))))
    (copy-tree (list :version 1 :handoff handoff
                     :handoff-file '(:device 1 :inode 4007 :size 12345 :digest 6789)
                     :workspace '(:directory "/tmp/adoption-follow-codec/" :device 1 :inode 6000 :lock-inode 6001 :fence-inode 6002)
                     :staged '(:device 1 :inode 6003) :saved '(:device 1 :inode 6004)
                     :journal '(:device 1 :inode 6005) :time 12345
                     :entries
                     (sort (loop for (name . bytes) in images
                                 for inode from 6010
                                 unless (and reuse (equal name "history.wal"))
                                   collect (list name (list (list name :file #o600 1 inode (length bytes)
                                                                  (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))))
                           #'string< :key #'first)
                     :reuse-wal reuse))))

(deftest adoption-follow-codec-roundtrips-the-derived-publication-plan
    (dolist (distance '(0 64))
      (dolist (generations '(1 2 3))
        (let ((plan (adoption-follow-codec-fixture :distance distance :generations generations)))
          (dotimes (step (1+ (length (pg::physical-adoption-follow-moves plan))))
            (let ((decoded (pg::physical-adoption-follow-decode
                            (pg::physical-adoption-follow-image plan step))))
              (ok (equal plan (getf decoded :plan)))
              (ok (= step (getf decoded :step)))))))))

(deftest adoption-follow-codec-rejects-future-history-in-the-frozen-base
    (let ((plan (adoption-follow-codec-fixture :distance 0 :generations 3 :reuse t)))
      (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-copy plan))))
      (setf (getf plan :reuse-wal) nil)
      (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-copy plan)))))
  (let ((plan (adoption-follow-codec-fixture :distance 0)))
    (setf (getf plan :reuse-wal) t)
    (ok (intake-fails-p (lambda () (pg::physical-adoption-follow-copy plan))))))

(cl-spec:defproperty adoption-follow-journal-accepts-only-the-next-progress-prefix
    ((flag (range integer 0 1)) (index (range integer 0 50)) (cut (range integer 0 30)))
  (:about pg::physical-adoption-follow-decode)
  (:trials (:normal 100))
  (let* ((plan (adoption-follow-codec-fixture :distance (if (zerop flag) 0 64)))
         (step (mod index (length (pg::physical-adoption-follow-moves plan))))
         (before (pg::physical-adoption-follow-image plan step))
         (after (pg::physical-adoption-follow-image plan (1+ step)))
         (prefix (mod cut (- (length after) (length before))))
         (decoded (pg::physical-adoption-follow-decode (subseq after 0 (+ (length before) prefix)))))
    (and (= step (getf decoded :step))
         (= (length before) (getf decoded :valid-length))
         (eq (plusp prefix) (not (null (getf decoded :partial)))))))

(deftest generated-adoption-follow-progress-prefixes
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'adoption-follow-journal-accepts-only-the-next-progress-prefix :seed 84))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

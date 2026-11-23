(in-package #:pg)

(defun unit-source-logical-control (logical &key pending selected resources fpw retained
                                              (intent :absent) (logical-intent :absent) exhausted)
  (apply #'physical-recovery-control-image
         (append
          (list (if pending 3 2) 42 1 4 32 80
                (if exhausted 18446744073709551616 3) :state-time 0
                :applied-timeline 1 :history-target (when selected 3) :history-digest (when selected 7)
                :promotion-child (when pending 4) :promotion-generation (when pending 9)
                :promotion-source-namespace (when pending :timeline)
                :retained-reference (when retained '(:root-id 1 :base-timeline 1 :base 32
                                                     :manifest-digest 4 :end 80 :end-digest 7))
                :resource-fact (when resources
                                 '(:resource-parameters (100 8 10 0 128) :resource-lsn 16 :resource-timeline 1))
                :fpw-fact (when fpw (unit-source-fpw-fact))
                :logical-fact logical)
          (unless (eq intent :absent) (list :promotion-full-page-writes intent))
          (unless (eq logical-intent :absent) (list :promotion-logical-decoding-enabled logical-intent
                                                    :promotion-wal-level (if logical-intent :logical :replica))))))

(rove:deftest reseed-journal-accepts-recovery-control-with-source-facts
    (dolist (selected '(nil t))
      (dolist (interval '(nil t))
        (let* ((fact (unit-source-logical-fact :enabled t))
               (name (if selected "recovery.selected" "recovery.initial")))
          (setf (getf fact :through) 80)
          (when interval
            (setf fact (append fact '(:logical-on-start-lsn 65 :logical-on-start-timeline 1))))
          (let* ((bytes (unit-source-logical-control fact :selected selected :resources t :fpw t))
                 (size (length bytes)))
            (rove:ok (physical-reseed-follow-configuration-image-size-p name size))
            (rove:ok (not (physical-reseed-follow-configuration-image-size-p name (1+ size)))))))))

(rove:deftest reseed-literal-check-keeps-boolean-permission-local
    (let ((fact (unit-source-logical-fact :enabled t)))
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-reseed-install-check-literal fact))))
      (rove:ok (physical-reseed-install-check-literal fact :allow-true t))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-reseed-install-check-literal (list 'foo) :allow-true t))))))

(rove:deftest source-logical-control-roundtrips-all-existing-extension-combinations
    (dolist (pending '(nil t))
      (dolist (selected '(nil t))
        (dolist (resources '(nil t))
          (dolist (fpw '(nil t))
            (dolist (retained (if pending '(nil t) '(nil)))
              (dolist (intent (if pending '(:absent nil t) '(:absent)))
                (dolist (exhausted '(nil t))
                  (let ((old (unit-source-logical-control nil :pending pending :selected selected
                                                              :resources resources :fpw fpw :retained retained
                                                              :intent intent :exhausted exhausted)))
                    (rove:ok (not (logbitp 30 (wal-u32 old 8))))
                    (rove:ok (null (get-properties (physical-recovery-decode-control old) '(:logical-fact))))
                    (dolist (enabled '(nil t))
                      (let ((fact (unit-source-logical-fact :enabled enabled)))
                        (setf (getf fact :through) 80)
                        (let* ((bytes (unit-source-logical-control fact :pending pending :selected selected
                                                                        :resources resources :fpw fpw :retained retained
                                                                        :intent intent :exhausted exhausted))
                               (control (physical-recovery-decode-control bytes)))
                          (rove:ok (= (+ 88 (length old)) (length bytes)))
                          (rove:ok (logbitp 30 (wal-u32 bytes 8)))
                          (rove:ok (eql exhausted (logbitp 31 (wal-u32 bytes 8))))
                          (rove:ok (equal fact (getf control :logical-fact)))
                          (rove:ok (= 80 (getf control :applied)))
                          (rove:ok (= (if pending 3 2) (getf control :phase)))
                          (rove:ok (eql (not (eq intent :absent))
                                        (not (null (get-properties control '(:promotion-full-page-writes))))))
                          (unless (eq intent :absent)
                            (rove:ok (eq intent (getf control :promotion-full-page-writes))))))))))))))))

(rove:deftest source-logical-control-rejects-corruption-flags-and-stale-frontiers
    (dolist (known '(nil t))
      (let ((fact (unit-source-logical-fact :enabled t)))
        (when known
          (setf fact (append fact '(:logical-on-start-lsn 65 :logical-on-start-timeline 1))))
        (setf (getf fact :through) 80)
        (let* ((bytes (unit-source-logical-control fact :pending t :selected t :resources t
                                                        :fpw t :retained t :intent nil :logical-intent nil :exhausted t))
               (size (physical-logical-fact-image-size fact))
               (offset (- (length bytes) (+ 4 size))))
          (rove:ok (= (- +physical-recovery-control-maximum-bytes+ 8 (if known 0 16))
                      (length bytes)))
          (dotimes (i size)
            (let ((bad (copy-seq bytes)))
              (setf (aref bad (+ offset i)) (logxor 1 (aref bad (+ offset i))))
              (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bad))))))
          (dolist (item '((0 1) (8 16) (12 1) (48 81) (56 2) (84 1)))
            (let ((bad (copy-seq bytes)))
              (setf (aref bad (+ offset (first item))) (second item))
              (unit-promotion-fpw-refresh-crc bad)
              (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bad))))))
          (let ((bad (copy-seq bytes)))
            (wal-set-u32 bad 8 (logandc2 (wal-u32 bad 8) #x40000000))
            (unit-promotion-fpw-refresh-crc bad)
            (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bad)))))))
      )
  (dolist (frontier '(79 81))
    (let ((fact (unit-source-logical-fact)))
      (setf (getf fact :through) frontier)
      (rove:ok (unit-commit-ts-error-p (lambda () (unit-source-logical-control fact)))))))

(rove:deftest source-logical-control-presence-flag-requires-supported-layout-and-fact
    (let ((bytes (unit-source-fpw-control nil)))
      (wal-set-u32 bytes 8 (logior #x40000000 (wal-u32 bytes 8)))
      (unit-promotion-fpw-refresh-crc bytes)
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bytes)))))
  (dolist (size '(60 72))
    (let ((bytes (make-array size :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace bytes (unit-source-fpw-control nil) :end1 (- size 4))
      (replace bytes (if (= size 60) #(80 71 82 83 84 65 84 69) #(80 71 82 83 84 65 84 50)))
      (unit-promotion-fpw-refresh-crc bytes)
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bytes)))))))

(in-package #:pg)

(defun logical-slot-continuation-origin-inputs (binding certificate host)
  ;; Bound the certificate and host inputs and select the original child's
  ;; history before using either in a continuation proof.
  (logical-slot-copy-descriptor-list-count certificate 128)
  (logical-slot-continuation-list host 6)
  (logical-slot-copy-descriptor-blob-size binding 1 (logical-slot-copy-source-proof-maximum-bytes))
  (logical-slot-copy-descriptor-blob-size (fourth host) 0 +physical-store-history-maximum-bytes+)
  (logical-slot-copy-descriptor-list-count
   (getf certificate :slots) +logical-slot-continuation-obligation-limit+)
  (dolist (row (getf certificate :slots))
    (unless (<= 4 (logical-slot-copy-descriptor-list-count row 16))
      (error "invalid initial continuation certificate row"))
    (logical-slot-continuation-ascii-size (first row) 63)
    (when (fourth row) (logical-slot-continuation-ascii-size (fourth row) +slot-state-maximum-bytes+)))
  (unless (and (eql (first host) (getf certificate :system-id))
               (eql (fifth host) (getf certificate :child-timeline))
               (typep (sixth host) '(unsigned-byte 64)))
    (error "continuation requires its original child host"))
  (logical-slot-continuation-history (fifth host) (fourth host)))

(defun logical-slot-continuation-saved-origin-rows (certificate history end rows)
  ;; Check each saved image against its permanent certificate, returning the
  ;; ordered inventory and whether any reserved slot still demands WAL.
  (logical-slot-copy-descriptor-list-count rows +logical-slot-continuation-obligation-limit+)
  (unless rows (error "active continuation requires its saved inventory"))
  (dolist (row rows)
    (logical-slot-continuation-list row 2)
    (logical-slot-continuation-ascii-size (first row) 63)
    (logical-slot-copy-descriptor-blob-size (second row) 1 +slot-state-maximum-bytes+))
  (let ((saved (sort (logical-slot-continuation-copy rows) #'string< :key #'first))
        (prior nil) (demand nil))
    (dolist (row saved)
      (unless (or (null prior) (string< prior (first row)))
        (error "active continuation has repeated saved slots"))
      (setf prior (first row))
      (let ((current (logical-slot-state-decode (second row) (first row)))
            (original (find (first row) (getf certificate :slots) :key #'first :test #'string=)))
        (unless (and original (fourth original))
          (error "saved continuation slot is absent from its permanent certificate"))
        (logical-slot-imported-check-state
         current (logical-slot-state-decode (string->bytes (fourth original)) (first row))
         history end)
        (when (eq :reserved (wal-replication-slot-wal-status current)) (setf demand t))))
    (values saved demand)))

(defun logical-slot-continuation-active-snapshot (binding certificate host rows)
  ;; Construct a detached generation 1 bootstrap image.  Later selected
  ;; generations are checked separately against the saved inventory and host.
  (let ((history (logical-slot-continuation-origin-inputs binding certificate host)))
    (multiple-value-bind (saved demand)
        (logical-slot-continuation-saved-origin-rows certificate history (sixth host) rows)
      (let* ((fork (getf certificate :fork)) (child (getf certificate :child-timeline))
                                             (snapshot
                                               (list :version 1 :binding (copy-seq binding) :generation 1 :system-id (first host)
                                                     :origin (list child fork) :history-target (fifth host)
                                                     :history (copy-seq (fourth host)) :sealed-end (list (sixth host) (fifth host) :prefix-end)
                                                     :slots saved :transfers nil :roots nil :runs nil
                                                     :tail (when demand (list (list fork child :record-start) child)))))
        (logical-slot-continuation-check snapshot)
        snapshot))))

(defun logical-slot-continuation-check-selected-snapshot
    (snapshot binding certificate host rows horizon)
  ;; Support the original child and its local tail.  The selected historical
  ;; seal may precede the captured durable horizon; compare the exact saved
  ;; inventory and reject other roots, runs or transfers here.
  (logical-slot-continuation-check snapshot)
  (let* ((history (logical-slot-continuation-origin-inputs binding certificate host))
         (sealed (getf snapshot :sealed-end))
         (fork (getf certificate :fork)) (child (getf certificate :child-timeline)))
    (unless (and (typep horizon '(unsigned-byte 64))
                 (<= (first sealed) horizon (sixth host))
                 (equalp binding (getf snapshot :binding))
                 (eql (first host) (getf snapshot :system-id))
                 (equal (list child fork) (getf snapshot :origin))
                 (eql (fifth host) (getf snapshot :history-target))
                 (equalp (fourth host) (getf snapshot :history))
                 (null (getf snapshot :roots)) (null (getf snapshot :runs))
                 (null (getf snapshot :transfers)))
      (error "selected continuation differs from its original host or supported topology"))
    (multiple-value-bind (saved demand)
        (logical-slot-continuation-saved-origin-rows certificate history (first sealed) rows)
      (unless (and (logical-slot-continuation-slot-rows-equal-p saved (getf snapshot :slots))
                   (equal (when demand (list (list fork child :record-start) child)) (getf snapshot :tail)))
        (error "selected continuation differs from its exact saved inventory or original tail")))
    t))

(defun logical-slot-continuation-read-saved-rows (storage)
  ;; Runtime observes the held slot root; startup retains its existing inventory
  ;; path.  Both return saved bytes for the later continuation checks.
  (logical-slot-continuation-storage-check storage)
  (let ((rows
          (if (logical-slot-continuation-runtime-storage-p storage)
              (logical-slot-continuation-storage-call
               storage :with-slot-root
               (lambda (directory check)
                 (logical-slot-continuation-read-slot-inventory directory check)))
              (loop for entry in (logical-slot-startup-read-inventory :local-workspace t)
                    collect (list (wal-replication-slot-name (logical-slot-startup-entry-slot entry))
                                  (copy-seq (logical-slot-startup-entry-bytes entry)))))))
    (logical-slot-continuation-storage-check storage)
    (sort rows #'string< :key #'first)))

(defun logical-slot-continuation-active-startup-image (storage facts)
  (setf facts (logical-slot-continuation-storage-call storage :facts))
  (logical-slot-continuation-active-snapshot
   (logical-slot-continuation-storage-call storage :binding) (getf facts :certificate)
   (logical-slot-continuation-storage-call storage :host)
   (logical-slot-continuation-read-saved-rows storage)))

(defun logical-slot-continuation-check-active-startup (storage facts snapshot)
  (unless (equalp (logical-slot-continuation-encode snapshot)
                  (logical-slot-continuation-encode
                   (logical-slot-continuation-active-startup-image storage facts)))
    (error "initial active continuation differs from its durable host or saved inventory"))
  t)

(defun logical-slot-continuation-check-tail-files (directory held)
  (dolist (row held)
    (destructuring-bind (name stream facts size) row
      (declare (ignore size))
      (unless (and (equal facts (physical-image-file-facts (sb-sys:fd-stream-fd stream)))
                   (equal facts (physical-image-file-facts directory name)))
        (error "continuation local WAL changed its held or named segment"))))
  t)

(defun logical-slot-continuation-check-tail-ledger (directory ledger)
  (maphash (lambda (name row)
             (unless (equal (first row) (physical-image-file-facts directory name))
               (error "continuation WAL segment differs from its proof ledger")))
           ledger)
  t)

(defun logical-slot-continuation-scan-tail
    (history fork child end guard reader spool observe finish
     &key (batch-policy :strict) (require-closed t))
  (let ((verify (logical-spool-sealed-p spool)) (records 0)
                                                (*wal-recovery-reservation-batch-clean-p* nil))
    (call-with-logical-wal-cursor
     history :timeline fork end child :record-start guard
     (lambda (cursor)
       (labels ((next (expected)
                  (let ((original (logical-wal-cursor-position cursor)))
                    (multiple-value-bind (record raw owner carrier kind) (logical-wal-cursor-next cursor)
                      (unless record
                        (when expected (error "continuation WAL ended before its transcript"))
                        (return-from next nil))
                      (let ((frame (logical-slot-continuation-tail-frame
                                    record raw owner carrier kind original)))
                        (if verify
                            (unless (and expected (equalp frame expected))
                              (error "continuation WAL differs from its exact transcript"))
                            (logical-spool-append spool frame)))
                      (when observe (funcall observe record raw owner carrier kind original))
                      (incf records)
                      t))))
         (if verify
             (progn
               (logical-spool-map-events spool #'next)
               (when (next nil) (error "continuation WAL exceeds its transcript")))
             (loop while (next nil)))
         (unless (and (= end (logical-wal-cursor-position cursor))
                      (or (not require-closed) (not (logical-wal-cursor-batch-open cursor))))
           (error "continuation WAL did not reach its sealed completed end"))
         (when finish (funcall finish))
         (unless verify (logical-spool-seal spool))
         records))
     :segment-reader reader :batch-policy batch-policy)))

;; With :TRANSCRIPT T, prove and seal the tail first and pass a scoped mapper
;; as the fifth callback argument.  Live checks and reproof remain outside it.

(defun call-with-logical-slot-continuation-local-tail-source (storage snapshot function &key boundaries
                                                                                          (end (first (getf snapshot :sealed-end))) (batch-policy :strict) (require-closed t) transcript witness)
  ;; Prove locally present original-child WAL from F to END.  END defaults to
  ;; historical seal S and must satisfy S <= END <= captured horizon L.
  ;; Strict batch handling and a closed end remain the defaults.
  ;; The first pass records exact frames and a segment ledger; the second
  ;; compares frames, segment facts and ordered visits.  Hold one segment FD
  ;; at a time and keep the transcript in a private temporary directory.
  ;; Close any current segment before a fresh proof so ordered visits can be
  ;; compared with the first pass.
  (let* ((history (logical-slot-continuation-history
                   (getf snapshot :history-target) (getf snapshot :history)))
         (fork (second (getf snapshot :origin)))
         (child (first (getf snapshot :origin))))
    (unless (and (member batch-policy '(:strict :logical-reader))
                 (typep require-closed 'boolean) (typep transcript 'boolean) (typep witness 'boolean)
                 (typep end '(unsigned-byte 64))
                 (<= (first (getf snapshot :sealed-end)) end
                     (if (logical-slot-continuation-runtime-storage-p storage)
                         (logical-slot-continuation-storage-call storage :horizon)
                         (sixth (logical-slot-continuation-storage-call storage :host)))))
      (error "continuation local proof exceeds its captured horizon"))
    (logical-slot-continuation-storage-call
     storage :with-local-wal
     (lambda (directory check-directory)
       (call-with-logical-slot-continuation-transcript
        (lambda (spool)
          (let ((current nil) (active t) (failed nil) (visits nil) (expected-visits nil)
                              (ledger (make-hash-table :test #'equal)))
            (labels ((guard ()
                       (unless (and active (not failed)) (error "continuation local WAL proof ended or failed"))
                       (logical-slot-continuation-storage-call storage :check-under-lock)
                       (funcall check-directory)
                       (logical-slot-continuation-check-tail-files directory (when current (list current))))
                     (check ()
                       (logical-slot-continuation-storage-check storage)
                       (sb-thread:with-mutex (*wal-lock*) (guard))
                       (logical-slot-continuation-check-tail-ledger directory ledger)
                       (sb-thread:with-mutex (*wal-lock*) (guard)))
                     (close-current ()
                       (when current
                         (if (sb-thread:holding-mutex-p *wal-lock*) (guard)
                             (sb-thread:with-mutex (*wal-lock*) (guard)))
                         (sb-sys:without-interrupts
                           (let ((stream (second current)))
                             (setf current nil)
                             (close stream)))))
                     (read-segment (segment carrier consume)
                       (guard)
                       (unless (eql child carrier)
                         (error "initial continuation requires its original child's local carrier"))
                       (let ((name (wal-segment-filename segment carrier :timeline)))
                         (unless (and current (equal name (first current)))
                           (close-current)
                           (sb-sys:without-interrupts
                             (multiple-value-bind (stream size)
                                 (open-bounded-regular-file
                                  (slot-state-fd-path directory name) +wal-segment-size+)
                               (setf current (list name stream nil size))
                               (setf (third current) (physical-image-file-facts (sb-sys:fd-stream-fd stream)))))
                           (let ((prior (gethash name ledger))
                                 (row (list (third current) (fourth current))))
                             (if prior
                                 (unless (equal prior row) (error "continuation segment changed between passes"))
                                 (if (logical-spool-sealed-p spool)
                                     (error "continuation reproof visited an unrecorded segment")
                                     (setf (gethash name ledger) row))))
                           (push name visits))
                         (guard)
                         (multiple-value-prog1 (funcall consume (second current) (fourth current))
                           (guard))))
                     (prove ()
                       (check)
                       (close-current)
                       (setf visits nil)
                       (let ((verify (logical-spool-sealed-p spool)) (complete nil))
                         (unwind-protect
                              (multiple-value-prog1
                                  (multiple-value-bind (observe finish)
                                      (if boundaries (funcall boundaries) (values nil nil))
                                    (let ((records
                                            (unwind-protect
                                                 (logical-slot-continuation-scan-tail
                                                  history fork child end #'guard #'read-segment spool observe finish
                                                  :batch-policy batch-policy :require-closed require-closed)
                                              (close-current))))
                                      (if verify
                                          (unless (equal expected-visits (nreverse visits))
                                            (error "continuation reproof changed its ordered segment visits"))
                                          (setf expected-visits (nreverse visits)))
                                      (check)
                                      (if witness
                                          (values records (logical-slot-continuation-tail-witness
                                                           spool fork end ledger expected-visits))
                                          records)))
                                (setf complete t))
                           (unless complete (setf failed t))))))
              (unwind-protect
                   (progn
                     (check)
                     (flet ((invoke (&optional mapper)
                              (multiple-value-prog1
                                  (apply function #'prove #'check #'read-segment #'guard
                                         (when transcript (list mapper)))
                                (check))))
                       (if transcript
                           (progn (prove)
                                  (call-with-logical-slot-continuation-transcript-reader
                                   spool fork end #'invoke))
                           (invoke))))
                (sb-sys:without-interrupts
                  (setf active nil)
                  (when current
                    (let ((stream (second current)))
                      (setf current nil)
                      (close stream)))))))))))))

(defun call-with-logical-slot-continuation-local-tail (storage snapshot function &key boundaries)
  (call-with-logical-slot-continuation-local-tail-source
   storage snapshot
   (lambda (prove check reader guard)
     (declare (ignore reader guard))
     (funcall function prove check))
   :boundaries boundaries))

(defun logical-slot-continuation-restore-active-startup (gate ownership)
  ;; Hold the original origin proof and root lease through both passes,
  ;; selector installation and readback.  This does not install a provider,
  ;; admit consumers or open the GC fence.
  (declare (notinline sb-posix:fsync))
  (call-with-logical-slot-continuation-startup-storage
   gate ownership
   (lambda (storage facts)
     (logical-slot-continuation-storage-call
      storage :with-origin
      (lambda (archive)
        (let ((snapshot (logical-slot-continuation-active-startup-image storage facts)))
          (call-with-logical-slot-continuation-local-tail
           storage snapshot
           (lambda (prove check)
             (let ((records (funcall prove)))
               (logical-slot-continuation-selector-point storage :after-active-tail-proof)
               (logical-slot-continuation-check-active-startup storage facts snapshot)
               (unless (member "selector" (logical-slot-continuation-selector-names storage) :test #'equal)
                 (logical-slot-continuation-install-initial-selector
                  storage (logical-slot-continuation-encode snapshot)))
               (unless (equal '("selector") (logical-slot-continuation-selector-names storage))
                 (error "active continuation selector has an unresolved temporary"))
               (call-with-logical-slot-continuation-selector
                storage
                (lambda (selected fd selected-facts)
                  (declare (ignore selected-facts))
                  (logical-slot-continuation-check-active-startup storage facts selected)
                  (sb-posix:fsync fd)
                  (sb-posix:fsync (logical-slot-continuation-storage-call storage :directory))
                  (logical-slot-continuation-selector-point storage :after-selector-restoration-sync)
                  (funcall prove)
                  (logical-slot-continuation-check-active-startup storage facts selected)
                  (funcall check)
                  (values selected records)))))
           :boundaries (lambda ()
                         (logical-slot-continuation-active-boundaries storage facts snapshot archive)))))))))

(defun logical-slot-continuation-restore-startup (gate ownership)
  (if (logical-slot-startup-read-inventory :local-workspace t)
      (logical-slot-continuation-restore-active-startup gate ownership)
      (logical-slot-continuation-restore-empty-startup gate ownership)))

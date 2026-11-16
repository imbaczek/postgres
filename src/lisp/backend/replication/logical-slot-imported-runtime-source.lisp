(in-package #:pg)

;; An imported runtime scan borrows the registered scan owner and the held
;; permanent proof files.  Origin lookup only finds a previously restored
;; record; the archive operation obtains a fresh root-reader lease.  These
;; scopes are observational and cover the original child, generation 1 only.
;; Callbacks are trusted internal code, not a sandbox for raw Lisp access.

(defun call-with-logical-slot-imported-runtime-source (scan source function)
  (call-with-logical-slot-imported-scan-owner
   scan source
   (lambda (owner)
     (call-with-physical-directory-root
      owner
      (lambda (root path)
        (declare (ignore path))
        (call-with-physical-image-owned-context
         root t
         (lambda (context)
           (call-with-logical-slot-imported-runtime-proof-files
            owner context
            (lambda (facts check under-lock)
              (call-with-logical-slot-imported-archive-input
               context
               (lambda (directory input reader anchors)
                 (let* ((binding (funcall check t))
                        (record (logical-slot-imported-host-origin-lookup
                                 owner context directory binding)))
                   (call-with-logical-slot-imported-origin-archive
                    record facts check under-lock directory input reader anchors
                    (lambda (archive)
                      (funcall function owner context facts archive check under-lock)))))))))))))))

(defun call-with-logical-slot-continuation-runtime-storage (scan source function)
  (call-with-logical-slot-imported-runtime-source
   scan source
   (lambda (owner context facts archive check under-lock)
     (call-with-logical-slot-continuation-storage-context
      owner context facts check under-lock function archive))))

;; Check the selected historical seal S against the captured durable horizon L
;; and the live original-child host and saved inventory.  S remains independent
;; of L; storage checks bracket the validation.

(defun logical-slot-continuation-check-runtime-snapshot (storage facts snapshot)
  (unless (logical-slot-continuation-runtime-storage-p storage)
    (error "runtime selector proof requires runtime storage"))
  (logical-slot-continuation-storage-check storage)
  (logical-slot-continuation-check-selected-snapshot
   snapshot (logical-slot-continuation-storage-call storage :binding) (getf facts :certificate)
   (logical-slot-continuation-storage-call storage :host)
   (logical-slot-continuation-read-saved-rows storage)
   (logical-slot-continuation-storage-call storage :horizon))
  (logical-slot-continuation-storage-check storage))

;; Prove the original strict boundary rules through S, then use ordinary
;; reader batch rules for the suffix.  The suffix may end with an open or
;; abandoned batch; a record may not cross the historical seal.

(defun logical-slot-continuation-runtime-tail-observer (fork sealed &optional requests)
  (unless (and (typep fork '(unsigned-byte 64))
               (typep sealed '(unsigned-byte 64)) (<= fork sealed))
    (error "invalid historical continuation seal"))
  (multiple-value-bind (strict-observe strict-finish)
      (logical-slot-continuation-boundary-observer requests :fork fork)
    (let ((position fork) (sealed-p (= fork sealed)) (active t) (failed nil)
                          (progress (make-logical-slot-startup-progress nil)) (clean nil))
      (when sealed-p (funcall strict-finish))
      (labels ((observe (record raw owner carrier kind original)
                 (unless (and active (not failed))
                   (error "runtime tail observer ended or failed"))
                 (let ((complete nil))
                   (unwind-protect
                        (progn
                          (unless (= original position)
                            (error "runtime tail observer changed its ordered prefix"))
                          (if sealed-p
                              (let ((*wal-recovery-reservation-batch-clean-p* clean))
                                (logical-slot-startup-accept-record progress record kind)
                                (setf clean *wal-recovery-reservation-batch-clean-p*))
                              (progn
                                (unless (<= (wal-record-end record) sealed)
                                  (error "runtime record crosses the historical seal"))
                                (funcall strict-observe record raw owner carrier kind original)
                                (when (= (wal-record-end record) sealed)
                                  (funcall strict-finish)
                                  (setf sealed-p t))))
                          (setf position (wal-record-end record) complete t))
                     (unless complete (setf failed t)))))
               (finish ()
                 (unless (and active (not failed) sealed-p)
                   (setf failed t)
                   (error "runtime tail did not prove its historical seal"))
                 (setf active nil)
                 t))
        (values #'observe #'finish)))))

;; Hold an independent pin at fork F throughout capture, replay, drain and
;; reproof.  The consumer's own retention pin is not a substitute for it.

(defun call-with-logical-slot-continuation-runtime-pin (storage fork function)
  (let ((table nil) (pin (gensym "CONTINUATION-TAIL-")) (active nil)
                    (thread sb-thread:*current-thread*))
    (labels ((guard ()
               (unless (and active (eq thread sb-thread:*current-thread*)
                            (eq table *wal-snapshot-retention-pins*)
                            (eql fork (gethash pin table)))
                 (error "runtime continuation lost its original-child retention pin"))
               (logical-slot-continuation-storage-call storage :check-under-lock)))
      (sb-sys:without-interrupts
        (unwind-protect
             (progn
               (sb-thread:with-mutex (*wal-lock*)
                 (logical-slot-continuation-storage-call storage :check-under-lock)
                 (setf table *wal-snapshot-retention-pins*
                       (gethash pin table) fork
                       active t))
               (sb-sys:with-local-interrupts (funcall function #'guard)))
          (sb-thread:with-mutex (*wal-lock*)
            (setf active nil)
            (when table (remhash pin table))))))))

(defun logical-slot-continuation-runtime-segment-match-p (expected minimum observed)
  (and (equal expected (logical-slot-copy-source-proof-directory-identity observed))
       (= (logand (third observed) #o170000) #o100000)
       (= (fifth observed) (sb-posix:geteuid))
       (zerop (logand (third observed) #o077))
       (<= minimum (sixth observed) +wal-segment-size+)))

;; Track named segment identities and minimum captured prefixes while reading
;; the original child.  Each read uses the current descriptor size.  The
;; ordered visits are checked again after replay.  Failure revokes the scope;
;; release masks interrupts between detaching and closing the held stream.

(defun call-with-logical-slot-continuation-runtime-segments
    (storage fork child horizon function)
  (call-with-logical-slot-continuation-runtime-pin
   storage fork
   (lambda (pin-guard)
     (logical-slot-continuation-storage-call
      storage :with-local-wal
      (lambda (directory check-directory)
        (let ((current nil) (ledger (make-hash-table :test #'equal))
                            (visits nil) (active t) (failed nil) (thread sb-thread:*current-thread*))
          (labels ((check-row (name facts)
                     (let ((row (gethash name ledger)))
                       (unless (and row (logical-slot-continuation-runtime-segment-match-p
                                         (first row) (second row) facts))
                         (error "runtime continuation segment lost its named identity or prefix"))))
                   (guard ()
                     (unless (and active (not failed) (eq thread sb-thread:*current-thread*))
                       (error "runtime continuation segment scope ended or failed"))
                     (funcall pin-guard)
                     (funcall check-directory)
                     (when current
                       (check-row (first current)
                                  (physical-image-file-facts (sb-sys:fd-stream-fd (second current))))
                       (check-row (first current) (physical-image-file-facts directory (first current))))
                     t)
                   (release ()
                     (sb-sys:without-interrupts
                       (when current
                         (let ((stream (second current)))
                           (setf current nil)
                           (close stream)))))
                   (check ()
                     (logical-slot-continuation-storage-check storage)
                     (sb-thread:with-mutex (*wal-lock*) (guard))
                     (maphash (lambda (name row)
                                (declare (ignore row))
                                (check-row name (physical-image-file-facts directory name))) ledger)
                     (sb-thread:with-mutex (*wal-lock*) (guard)))
                   (read-checked (segment carrier consume)
                     (guard)
                     (unless (eql child carrier)
                       (error "runtime continuation requires its original child carrier"))
                     (let ((name (wal-segment-filename segment carrier :timeline)))
                       (unless (and current (equal name (first current)))
                         (release)
                         (sb-sys:without-interrupts
                           (multiple-value-bind (stream size)
                               (open-bounded-regular-file
                                (slot-state-fd-path directory name) +wal-segment-size+)
                             (setf current (list name stream size))
                             (let ((facts (physical-image-file-facts (sb-sys:fd-stream-fd stream))))
                               (unless (gethash name ledger)
                                 (setf (gethash name ledger)
                                       (list (logical-slot-copy-source-proof-directory-identity facts)
                                             (min size (max 0 (- horizon (* segment +wal-segment-size+))))))))
                             (push name visits))))
                       (guard)
                       (multiple-value-prog1 (funcall consume (second current) (sixth (physical-image-file-facts (sb-sys:fd-stream-fd (second current)))))
                         (guard))))
                   (read-segment (segment carrier consume)
                     (guard)
                     (let ((complete nil))
                       (unwind-protect
                            (multiple-value-prog1 (read-checked segment carrier consume)
                              (setf complete t))
                         (unless complete (setf failed t)))))
                   (finish-pass ()
                     (check)
                     (sb-sys:without-interrupts (release))
                     (prog1 (nreverse visits) (setf visits nil))))
            (let ((complete nil))
              (unwind-protect
                   (multiple-value-prog1
                       (progn (check) (funcall function #'read-segment #'guard #'check #'finish-pass))
                     (check)
                     (setf complete t))
                (sb-sys:without-interrupts
                  (unless complete (setf failed t))
                  (setf active nil)
                  (release)))))))))))

;; E is the registered consumer end, L the private captured and checked
;; durable proof horizon, and S the selected historical seal.  Prove through
;; H = max(E, S), requiring H <= L, even when the consumer stops before S.
;;
;; Capture into a private bounded spool, replay and drain it, then reprove the
;; same bytes and ordered segment visits.  Append after H is qualified here;
;; committed durable growth and cold historical reconciliation are not yet
;; qualified.  Without an independent digest this cannot detect consistent
;; corruption predating capture, or a transient rewrite that is restored.

(defun call-with-logical-slot-continuation-runtime-tail
    (storage snapshot end function)
  (unless (eql end (logical-slot-continuation-storage-call storage :end))
    (error "runtime tail endpoint differs from its registered scan"))
  (let* ((facts (logical-slot-continuation-storage-call storage :facts))
         (fork (second (getf snapshot :origin)))
         (child (first (getf snapshot :origin)))
         (sealed (first (getf snapshot :sealed-end)))
         (horizon (max end sealed))
         (history (logical-slot-continuation-history
                   (getf snapshot :history-target) (getf snapshot :history))))
    (logical-slot-continuation-check-runtime-snapshot storage facts snapshot)
    (unless (<= horizon (logical-slot-continuation-storage-call storage :horizon))
      (error "runtime tail exceeds its captured durable horizon"))
    (logical-slot-continuation-storage-call
     storage :with-origin
     (lambda (archive)
       (let ((requests (logical-slot-continuation-prove-parent-boundaries
                        storage facts archive
                        (logical-slot-continuation-saved-boundaries snapshot (getf facts :certificate)))))
         (call-with-logical-slot-continuation-runtime-segments
          storage fork child horizon
          (lambda (reader guard check finish-pass)
            (call-with-logical-slot-continuation-transcript
             (lambda (spool)
               (let ((expected-visits nil))
                 (labels ((prove ()
                            (funcall check)
                            (multiple-value-bind (observe finish)
                                (logical-slot-continuation-runtime-tail-observer fork sealed requests)
                              (let* ((verify (logical-spool-sealed-p spool))
                                     (records (logical-slot-continuation-scan-tail
                                               history fork child horizon guard reader spool observe finish
                                               :batch-policy :logical-reader :require-closed nil))
                                     (visits (funcall finish-pass)))
                                (if verify
                                    (unless (equal visits expected-visits)
                                      (error "runtime tail changed its ordered segment visits"))
                                    (setf expected-visits visits))
                                records))))
                   (prove)
                   (multiple-value-prog1
                       (call-with-logical-spool-reader
                        spool (+ 40 +wal-segment-size+)
                        (lambda (next)
                          (multiple-value-prog1 (funcall function next check guard)
                            (loop while (funcall next)))))
                     (prove)
                     (logical-slot-continuation-check-runtime-snapshot storage facts snapshot)))))))))))))

;; Supply the captured transcript sequentially from the registered start.
;; The first record may use the saved restart boundary's batch-start position.

(defun call-with-logical-slot-continuation-runtime-tail-provider
    (storage snapshot scan function)
  (let* ((fork (second (getf snapshot :origin)))
         (start (wal-retained-slot-scan-start scan))
         (next-position (max fork start))
         (initial t)
         (slot (wal-retained-slot-scan-slot scan)))
    (call-with-logical-slot-continuation-runtime-tail
     storage snapshot (wal-retained-slot-scan-end scan)
     (lambda (next check guard)
       (labels ((read-record (position consume)
                  (unless (= position next-position)
                    (error "runtime transcript request changed its sequential position"))
                  (funcall check)
                  (let ((frame (funcall next)))
                    (when initial
                      (loop while (and frame (<= (wal-u64 frame 16) position))
                            do (setf frame (funcall next))))
                    (unless (and frame
                                 (or (= position (wal-u64 frame 0))
                                     (and initial (= position start)
                                          (= start (wal-replication-slot-restart-lsn slot))
                                          (= position (wal-u64 frame 8))
                                          (= +wal-record-batch-start+
                                             (wal-record-type
                                              (wal-decode-record (subseq frame 40) position))))))
                      (error "runtime transcript has no proved next record at the requested start"))
                    (setf initial nil next-position (wal-u64 frame 16))
                    (funcall consume (subseq frame 40) (wal-u64 frame 8) (wal-u32 frame 28)))
                  (funcall check)))
         (call-with-logical-wal-record-provider
          #'read-record guard
          (lambda (provider)
            (multiple-value-prog1 (funcall function provider)
              (sb-thread:with-mutex (*wal-lock*)
                (logical-wal-record-provider-check provider))))))))))

;; Keep the exact selected file, sidecar, origin lease and local proof live
;; across the cursor callback.  Checks before and after use bind the named
;; selector to its held descriptor and bytes; an ambiguous selector fails.

(defun call-with-logical-slot-continuation-runtime-observation
    (storage facts scan source owner role function &key (batch-policy :logical-reader))
  (unless (and (logical-slot-continuation-runtime-storage-p storage)
               (member (logical-wal-source-purpose source) '(:stream :terminal-check)))
    (error "runtime observation requires its registered source purpose"))
  (multiple-value-bind (actual-scan actual-source)
      (logical-slot-continuation-storage-call storage :scan-source)
    (unless (and (eq scan actual-scan) (eq source actual-source))
      (error "runtime observation differs from its storage scan source")))
  (unless (equal '("selector") (logical-slot-continuation-selector-names storage))
    (error "runtime reader requires an unambiguous stable selector"))
  (call-with-logical-slot-continuation-selector
   storage
   (lambda (snapshot fd selector-facts)
     (let ((directory (logical-slot-continuation-storage-call storage :directory))
           (image (logical-slot-continuation-encode snapshot)))
       (logical-slot-continuation-check-runtime-snapshot storage facts snapshot)
       (call-with-logical-slot-continuation-runtime-tail-provider
        storage snapshot scan
        (lambda (local-provider)
          (logical-slot-continuation-storage-call
           storage :with-origin
           (lambda (archive)
             (logical-slot-continuation-storage-call
              storage :with-sidecar
              (lambda (sidecar-reader check-files)
                (labels ((guard ()
                           (logical-slot-continuation-storage-call storage :check-under-lock)
                           (logical-wal-record-provider-check local-provider)
                           (unless (and
                                    (equal selector-facts (physical-image-file-facts fd))
                                    (equal selector-facts (physical-image-file-facts directory "selector"))
                                    (equal '("selector")
                                           (logical-slot-copy-import-control-names directory 3)))
                             (error "runtime reader lost its stable selector"))
                           t)
                         (check ()
                           (logical-slot-continuation-check-runtime-snapshot storage facts snapshot)
                           (logical-slot-imported-check-held-file directory "selector" fd selector-facts image)
                           (funcall check-files)
                           (sb-thread:with-mutex (*wal-lock*) (guard))))
                  (check)
                  (multiple-value-prog1
                      (call-with-logical-slot-continuation-lanes
                       facts snapshot (wal-retained-slot-scan-slot scan) archive
                       sidecar-reader nil #'guard
                       (lambda (cursor bounds)
                         (declare (ignore bounds))
                         (multiple-value-prog1 (funcall function cursor source snapshot image)
                           (sb-thread:with-mutex (*wal-lock*)
                             (logical-wal-cursor-check-scope cursor))))
                       :end (wal-retained-slot-scan-end scan)
                       :position (list (wal-retained-slot-scan-start scan) owner role)
                       :local-provider local-provider :batch-policy batch-policy)
                    (check)))))))))))))

(defun call-with-logical-slot-imported-runtime-cursor
    (scan source owner role function &key (batch-policy :logical-reader))
  (unless (eq :stream (logical-wal-source-purpose source))
    (error "terminal evidence cannot open an imported stream cursor"))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-retained-slot-scan-validate-under-lock scan)
    (logical-wal-check-source source (wal-retained-slot-scan-end scan))
    (unless (logical-slot-wal-start-proven-p
             (wal-retained-slot-scan-slot scan) (wal-retained-slot-scan-start scan) owner role)
      (error "imported stream start lacks its saved slot boundary")))
  (call-with-logical-slot-continuation-runtime-storage
   scan source
   (lambda (storage facts)
     (call-with-logical-slot-continuation-runtime-observation
      storage facts scan source owner role
      (lambda (cursor actual-source snapshot image)
        (declare (ignore snapshot image))
        (funcall function cursor actual-source))
      :batch-policy batch-policy))))

;; Dispatch the imported observational scan within the caller's registered
;; scan scope.  Borrowed cursors and artifact capabilities stay within their
;; callbacks; all artifact scopes end before progress is published.

;; Stage progress privately until the cursor, drain and reproof all succeed;
;; publish it only after rechecking the registered scan and source under WAL.

(defun call-with-logical-slot-imported-runtime-scan
    (scan source owner role function batch-policy)
  (let ((progress *wal-logical-slot-read-progress*))
    (multiple-value-prog1
        (let ((*wal-logical-slot-read-progress* progress)
              (*wal-logical-slot-read-continuation* nil))
          (multiple-value-prog1
              (call-with-logical-slot-imported-runtime-cursor
               scan source owner role function :batch-policy batch-policy)
            (setf progress *wal-logical-slot-read-progress*)))
      (sb-thread:with-mutex (*wal-lock*)
        (wal-retained-slot-scan-validate-under-lock scan)
        (logical-wal-check-source source (wal-retained-slot-scan-end scan))
        (when (integerp progress)
          (setf *wal-logical-slot-read-progress*
                (max progress (or *wal-logical-slot-read-progress* progress))))))))

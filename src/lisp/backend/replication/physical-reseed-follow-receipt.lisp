;; Publish the terminal follow journal as a durable retirement receipt.
;; Its result records evidence at actor return.  Later consumption and main
;; startup remain separate operations.

(in-package #:pg)

(defstruct (physical-reseed-follow-receipt-witness (:conc-name receipt-witness-))
  ;; The witness borrows the owner, root, roles, store, PID and source stream.
  ;; Its lifetime and phase limit every use to the active operation and thread.
  owner root descriptors store base binding check pid stream bytes journal snapshot
  proof root-row active-p thread phase)

(defmethod print-object ((witness physical-reseed-follow-receipt-witness) stream)
  ;; Keep private witness contents out of printed representations.
  (print-unreadable-object (witness stream :type t :identity t)))

(defun physical-reseed-follow-receipt-check-scope (witness)
  ;; Check the thread, busy operation and borrowed ownership before observation.
  (unless (and (receipt-witness-active-p witness)
               (eq (receipt-witness-thread witness) sb-thread:*current-thread*)
               (retirement-startup-busy-p (receipt-witness-owner witness)))
    (error "receipt witness ended, changed thread or lost its operation"))
  (physical-reseed-follow-retirement-check-startup-owner (receipt-witness-owner witness))
  (funcall (receipt-witness-check witness))
  t)

(defun physical-reseed-follow-receipt-check-source (witness)
  ;; Check the held inode around a complete read of the private source bytes.
  (physical-reseed-follow-receipt-check-scope witness)
  (let* ((root (receipt-witness-root witness))
         (journal (receipt-witness-journal witness))
         (stream (receipt-witness-stream witness))
         (bytes (receipt-witness-bytes witness))
         (current (make-array (length bytes) :element-type '(unsigned-byte 8))))
    (physical-reseed-follow-retired-check-held
     root (sb-sys:fd-stream-fd stream) journal
     (lambda () (physical-reseed-follow-receipt-check-scope witness)))
    (file-position stream 0)
    (unless (and (= (length current) (read-sequence current stream))
                 (eq :eof (read-byte stream nil :eof))
                 (equalp bytes current))
      (error "receipt witness changed canonical source bytes"))
    (physical-reseed-follow-retired-check-held
     root (sb-sys:fd-stream-fd stream) journal
     (lambda () (physical-reseed-follow-receipt-check-scope witness))))
  t)

(defun physical-reseed-follow-receipt-extra (witness root)
  ;; Include copied journal and PID rows in the root layout proof.
  (unless (= root (receipt-witness-root witness))
    (error "receipt witness changed its root descriptor"))
  (physical-reseed-follow-receipt-check-source witness)
  (append
   (loop for key in '(:follow :receipt)
         for row = (getf (receipt-witness-journal witness) key)
         when row collect (cons (copy-seq (first row)) (copy-list row)))
   (list (cons "postmaster.pid" (copy-list (getf (receipt-witness-pid witness) :row))))))

(defun physical-reseed-follow-receipt-observe (witness)
  ;; Prove the layout on both sides of receive validation while resources live.
  (flet ((check () (physical-reseed-follow-receipt-check-scope witness))
         (source-check () (physical-reseed-follow-receipt-check-source witness))
         (extra (root) (physical-reseed-follow-receipt-extra witness root)))
    (let* ((entry (cons "postmaster.pid" (copy-list (getf (receipt-witness-pid witness) :row))))
           (pid-evidence (list :entry entry :verify #'check))
           (names (mapcar #'car (extra (receipt-witness-root witness)))))
      (flet ((layout ()
               (physical-reseed-follow-retirement-layout-state-core
                (receipt-witness-descriptors witness) (receipt-witness-snapshot witness)
                #'check #'extra nil pid-evidence #'source-check names)))
        (let ((proof (layout)))
          (physical-reseed-follow-retirement-check-receive
           (receipt-witness-descriptors witness) (receipt-witness-store witness)
           (getf (receipt-witness-snapshot witness) :record)
           (receipt-witness-base witness) (receipt-witness-binding witness) #'check)
          (unless (equal proof (layout))
            (error "receipt layout changed around receive validation"))
          (append proof
                  (list :journal (physical-adoption-attempt-value-copy (receipt-witness-journal witness)))))))))

(defun physical-reseed-follow-receipt-check (witness)
  ;; Require the complete observation to match the frozen proof.
  (unless (equal (receipt-witness-proof witness)
                 (physical-reseed-follow-receipt-observe witness))
    (error "receipt witness changed its frozen full proof"))
  t)

(defun physical-reseed-follow-receipt-stable-proof (proof)
  ;; Remove transition-owned fields from comparison while preserving independent
  ;; source identity and mtime and the other roles.  The raw seal checks the
  ;; authorized deltas before this comparison.
  (let* ((copy (physical-adoption-attempt-value-copy proof))
         (snapshot (getf copy :snapshot))
         (row (copy-list (getf snapshot :row)))
         (identity (copy-list (getf snapshot :identity)))
         (layout (getf copy :layout))
         (names (getf layout :names)))
    (setf (fourth identity) 1
          (first row) "physical-reseed.follow"
          (fifth row) 1
          (ninth row) nil
          (getf snapshot :row) row
          (getf snapshot :identity) (butlast identity)
          (getf copy :journal) (list :mount (getf (getf copy :journal) :mount))
          (getf names :root)
          (remove-if (lambda (name) (member name '("physical-reseed.follow" "physical-reseed.retired")
                                            :test #'equal))
                     (getf names :root))
          (getf layout :directories)
          (loop for (role . facts) in (getf layout :directories)
                collect (cons role (if (eq role :root) (subseq facts 0 6) facts))))
    copy))

(defun physical-reseed-follow-receipt-point (witness point)
  ;; Test callbacks run between checks of the complete frozen proof.
  (physical-reseed-follow-receipt-check witness)
  (physical-reseed-follow-test-point
   point (merge-pathnames "physical-reseed.retired"
                          (retirement-startup-directory (receipt-witness-owner witness))))
  (physical-reseed-follow-receipt-check witness))

(defun physical-reseed-follow-receipt-claim (witness expected claim)
  ;; Claim the phase before any callback or effect can reenter the operation.
  (sb-sys:without-interrupts
    (unless (and (receipt-witness-active-p witness)
                 (eq (receipt-witness-thread witness) sb-thread:*current-thread*)
                 (retirement-startup-busy-p (receipt-witness-owner witness))
                 (retirement-startup-uncertain-p (receipt-witness-owner witness))
                 (eq expected (receipt-witness-phase witness))
                 (eq (ecase expected
                       ((:follow :follow-durable) :follow)
                       ((:alias :alias-durable) :alias)
                       ((:receipt :receipt-durable) :receipt))
                     (getf (receipt-witness-journal witness) :geometry)))
      (error "receipt effect cannot claim this witness phase"))
    (setf (receipt-witness-phase witness) claim)))

(defun physical-reseed-follow-receipt-check-effect (witness phase)
  ;; Effects require both their claimed phase and the uncertainty latch.
  (physical-reseed-follow-receipt-check-scope witness)
  (unless (and (retirement-startup-uncertain-p (receipt-witness-owner witness))
               (eq phase (receipt-witness-phase witness)))
    (error "receipt effect requires its claimed phase and uncertainty"))
  t)

(defun physical-reseed-follow-receipt-sync (witness inode-point root-point after-inode after-root)
  ;; A fresh alias or receipt-only retry repeats both inode and root syncs.
  ;; Advance to a durable phase only after both barriers and the full proof.
  (declare (notinline sb-posix:fsync))
  (let ((geometry (getf (receipt-witness-journal witness) :geometry)))
    (unless (member geometry '(:follow :alias :receipt))
      (error "receipt sync requires a supported geometry"))
    (physical-reseed-follow-receipt-claim witness geometry :syncing)
    (physical-reseed-follow-receipt-point witness inode-point)
    (physical-reseed-follow-receipt-check-effect witness :syncing)
    (sb-posix:fsync (sb-sys:fd-stream-fd (receipt-witness-stream witness)))
    (physical-reseed-follow-receipt-point witness after-inode)
    (physical-reseed-follow-receipt-point witness root-point)
    (physical-reseed-follow-receipt-check-effect witness :syncing)
    (sb-posix:fsync (receipt-witness-root witness))
    (physical-reseed-follow-receipt-point witness after-root)
    (physical-reseed-follow-receipt-check witness)
    (physical-reseed-follow-receipt-check-effect witness :syncing)
    (setf (receipt-witness-phase witness)
          (ecase geometry (:follow :follow-durable) (:alias :alias-durable) (:receipt :receipt-durable)))))

(defun physical-reseed-follow-receipt-seal (witness operation)
  ;; Capture raw journal, held inode and root facts immediately after the effect
  ;; and before proof checks or test callbacks.
  (let* ((root (receipt-witness-root witness))
         (before (receipt-witness-journal witness))
         (old-row (or (getf before :follow) (getf before :receipt)))
         (journal (physical-reseed-follow-retired-rows root (constantly t)))
         (row (or (getf journal :follow) (getf journal :receipt)))
         (root-row (physical-reseed-install-old-observe root "." (getf journal :mount)))
         (held (physical-reseed-follow-retirement-held-file-facts
                (sb-sys:fd-stream-fd (receipt-witness-stream witness)) (getf journal :mount))))
    (unless (and (eq (getf before :geometry) (ecase operation (:link :follow) (:unlink :alias)))
                 (eq (getf journal :geometry) (ecase operation (:link :alias) (:unlink :receipt)))
                 (= (getf before :mount) (getf journal :mount))
                 (equal (subseq old-row 1 4) (subseq row 1 4))
                 (equal (subseq old-row 5 8) (subseq row 5 8))
                 (equal (rest row) held)
                 (equal (subseq (receipt-witness-root-row witness) 0 6) (subseq root-row 0 6)))
      (error "receipt transition changed facts beyond its owned namespace operation"))
    (list :journal journal :root root-row :held held)))

(defun physical-reseed-follow-receipt-transition (witness operation)
  ;; Seal the namespace effect without interruption, then compare a fresh full
  ;; proof with the seal and the stable parts of the preceding proof.
  (declare (notinline port-linkat port-unlinkat))
  (let ((root (receipt-witness-root witness))
        (before (receipt-witness-proof witness))
        (seal nil)
        (claim (ecase operation (:link :linking) (:unlink :unlinking))))
    (physical-reseed-follow-receipt-claim
     witness (ecase operation (:link :follow-durable) (:unlink :alias-durable)) claim)
    (physical-reseed-follow-receipt-point
     witness (ecase operation (:link :before-retirement-receipt-link)
                    (:unlink :before-retirement-receipt-unlink)))
    (sb-sys:without-interrupts
      (physical-reseed-follow-receipt-check-effect witness claim)
      (physical-reseed-follow-receipt-check-source witness)
      (physical-reseed-follow-receipt-check-effect witness claim)
      (unless (zerop (ecase operation
                       (:link (port-linkat root "physical-reseed.follow" root "physical-reseed.retired" 0))
                       (:unlink (port-unlinkat root "physical-reseed.follow" 0))))
        (error "receipt namespace operation failed: errno ~d" (sb-alien:get-errno)))
      (setf seal (physical-reseed-follow-receipt-seal witness operation)
            (receipt-witness-phase witness) :bridging))
    (let* ((journal (getf seal :journal))
           (row (or (getf journal :follow) (getf journal :receipt))))
      (setf (receipt-witness-journal witness) journal
            (receipt-witness-snapshot witness)
            (list :record (getf (receipt-witness-snapshot witness) :record)
                  :identity (physical-reseed-install-row-file-facts row) :row (copy-list row)))
      (physical-reseed-follow-receipt-check-source witness)
      (let ((proof (physical-reseed-follow-receipt-observe witness)))
        (unless (and (equal journal (getf proof :journal))
                     (equal (getf seal :root)
                            (cdr (assoc :root (getf (getf proof :layout) :directories))))
                     (equal (physical-reseed-follow-receipt-stable-proof before)
                            (physical-reseed-follow-receipt-stable-proof proof)))
          (error "receipt namespace transition changed frozen historical state"))
        (physical-reseed-follow-receipt-check-source witness)
        (unless (equal (getf seal :root)
                       (physical-reseed-install-old-observe root "." (getf journal :mount)))
          (error "receipt namespace transition changed its immediate root seal"))
        (sb-sys:without-interrupts
          (setf (receipt-witness-proof witness) proof
                (receipt-witness-root-row witness) (getf seal :root)
                (receipt-witness-phase witness) (getf journal :geometry)))))
    (physical-reseed-follow-receipt-point
     witness (ecase operation (:link :after-retirement-receipt-link)
                    (:unlink :after-retirement-receipt-unlink)))))

(defun physical-reseed-follow-receipt-run (witness)
  ;; Bind the initial proof to handover evidence and drive each durable phase.
  ;; Capture the cleanup stamp while the roles are still held.
  (unless (eq :ready (receipt-witness-phase witness))
    (error "receipt witness was already consumed"))
  (setf (receipt-witness-phase witness) :checking
        (receipt-witness-proof witness) (physical-reseed-follow-receipt-observe witness))
  (let* ((owner (receipt-witness-owner witness))
         (proof (receipt-witness-proof witness))
         (projection
           (if (getf (retirement-startup-evidence owner) :journal)
               (physical-reseed-follow-retired-startup-projection (receipt-witness-descriptors witness) proof)
               (physical-reseed-follow-startup-projection (receipt-witness-descriptors witness) proof))))
    (unless (equal projection (retirement-startup-evidence owner))
      (error "receipt witness differs from its startup handover"))
    (setf (receipt-witness-root-row witness)
          (cdr (assoc :root (getf (getf proof :layout) :directories))))
    (sb-sys:without-interrupts
      (setf (retirement-startup-uncertain-p owner) t
            (receipt-witness-phase witness) (getf (receipt-witness-journal witness) :geometry)))
    (when (eq :follow (receipt-witness-phase witness))
      (physical-reseed-follow-receipt-sync
       witness :before-retirement-receipt-source-fsync :before-retirement-receipt-source-root-fsync
       :after-retirement-receipt-source-fsync :after-retirement-receipt-source-root-fsync)
      (physical-reseed-follow-receipt-transition witness :link))
    (when (eq :alias (receipt-witness-phase witness))
      (physical-reseed-follow-receipt-sync
       witness :before-retirement-receipt-alias-fsync :before-retirement-receipt-alias-root-fsync
       :after-retirement-receipt-alias-fsync :after-retirement-receipt-alias-root-fsync)
      (physical-reseed-follow-receipt-transition witness :unlink))
    (unless (eq :receipt (receipt-witness-phase witness))
      (error "receipt witness did not reach receipt-only geometry"))
    (physical-reseed-follow-receipt-sync
     witness :before-retirement-receipt-final-fsync :before-retirement-receipt-final-root-fsync
     :after-retirement-receipt-final-fsync :after-retirement-receipt-final-root-fsync)
    (physical-reseed-follow-receipt-check witness)
    (let ((stamp (physical-reseed-follow-retirement-role-stamp
                  (receipt-witness-descriptors witness)
                  (getf (getf (receipt-witness-proof witness) :layout) :names))))
      (physical-reseed-follow-receipt-check witness)
      (values (physical-reseed-follow-retired-startup-projection
               (receipt-witness-descriptors witness) (receipt-witness-proof witness))
              stamp))))

(defun call-with-physical-reseed-follow-receipt-source
    (owner root descriptors store base binding check pid source function)
  ;; Own the source stream for the witness and keep private copies of source
  ;; bytes, descriptors and journal facts until the witness is revoked.
  (declare (notinline physical-image-openat))
  (let ((fd nil) (stream nil) (witness nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       root (first (getf (getf source :snapshot) :row))
                       (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                               +postmaster-o-cloexec+) 0))
             (when (minusp fd) (setf fd nil) (error "cannot open owned receipt source"))
             (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8))))
           (setf witness
                 (make-physical-reseed-follow-receipt-witness
                  :owner owner :root root :descriptors (copy-tree descriptors) :store store
                  :base base :binding binding :check check :pid pid :stream stream
                  :bytes (copy-seq (getf source :bytes))
                  :journal (loop for key in '(:geometry :follow :receipt :mount)
                                 append (list key (physical-adoption-attempt-value-copy (getf source key))))
                  :snapshot (physical-adoption-attempt-value-copy (getf source :snapshot))
                  :active-p t :thread sb-thread:*current-thread* :phase :ready))
           (physical-reseed-follow-receipt-check-source witness)
           (funcall function witness))
      (when witness (setf (receipt-witness-active-p witness) nil))
      (if stream (close stream) (when fd (sb-posix:close fd))))))

(defun physical-reseed-follow-retirement-publish-receipt (owner)
  ;; Hold source, store, roles, PID-reader and root scopes through the transition.
  ;; After closure, audit before adopting evidence and clearing uncertainty.
  ;; The final pure audit follows the owner guard.  Failure after effects begin
  ;; restores uncertainty before the operation releases its busy flag.
  (let ((stamp nil) (evidence nil) (success nil) (started nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-physical-reseed-follow-retirement-operation
              owner
              (lambda ()
                (call-with-physical-reseed-follow-retirement-startup-root
                 owner
                 (lambda (root root-check)
                   (let ((source (physical-reseed-follow-retired-journal-at root root-check)))
                     (call-with-physical-reseed-follow-retirement-owned-pid
                      root root-check
                      (lambda (pid pid-check)
                        (call-with-physical-reseed-follow-retirement-resources
                         root (getf (getf source :snapshot) :record) pid-check
                         (lambda (descriptors store base binding check)
                           (call-with-physical-reseed-follow-receipt-source
                            owner root descriptors store base binding check pid source
                            (lambda (witness)
                              (multiple-value-setq (evidence stamp)
                                (unwind-protect
                                     (physical-reseed-follow-receipt-run witness)
                                  (when (retirement-startup-uncertain-p owner)
                                    (setf started t)))))))))))))
                (physical-reseed-follow-retirement-audit-stamp
                 stamp (lambda () (physical-reseed-follow-retirement-check-startup-owner owner)))
                (sb-sys:without-interrupts
                  (setf (retirement-startup-evidence owner) evidence
                        (retirement-startup-uncertain-p owner) nil))
                (physical-adoption-attempt-value-copy evidence))
              (lambda () (physical-reseed-follow-retirement-check-stamp stamp))
              (lambda ()
                (when started (setf (retirement-startup-uncertain-p owner) t))))
           (setf success t))
      (when (and started (not success))
        (setf (retirement-startup-uncertain-p owner) t)))))

;; Consume a published retirement receipt after local standby admission.
;; This scoped operation leaves startup integration to its caller.

(in-package #:pg)

(defun physical-reseed-follow-startup-local-state-at (descriptors store check)
  ;; Check ordinary standby control, follow configuration and recovery state
  ;; under the held directory and store checks.
  (funcall check)
  (physical-store-check-live store)
  (call-with-physical-reseed-follow-held-base-directory
   (physical-reseed-follow-configuration-role-fd descriptors :root)
   (physical-reseed-follow-configuration-role-fd descriptors :base)
   (physical-reseed-follow-configuration-role-fd descriptors :global)
   (physical-reseed-follow-configuration-role-fd descriptors :wal)
   (lambda (directory)
     (let ((*pgdata* directory))
       (physical-adoption-check-startup)
       (multiple-value-bind (base control) (physical-recovery-verify-base-state)
         (unless (= 2 (getf control :phase))
           (error "reseed handover requires ordinary streaming standby control"))
         (let* ((*wal-segment-namespace* (getf base :wal-namespace))
                (*wal-insert-timeline* (getf base :timeline))
                (*wal-checkpoint-timeline* (getf base :timeline))
                (config (physical-follow-startup-owner-check)))
           (unless (and config (member (length config) '(5 6))
                        (eq :active (getf (fifth config) :phase)))
             (error "reseed handover requires an active local follow binding"))
           (physical-follow-check-unowned-bootstrap-base config base)
           (setf (getf base :follow-startup-config) config)
           (physical-follow-check-startup-admission base)
           (let* ((metadata (physical-restartpoint-read base))
                  (policy (physical-recovery-resolve-timeline-policy
                           base store metadata (startup-recovery-timeline-goal))))
             (physical-follow-check-active-state store base config :policy policy))
           (physical-follow-managed-marker-p base)
           (unless (and (equal control (physical-recovery-read-control directory))
                        (equal config (physical-follow-read-config)))
             (error "ordinary standby metadata changed during reseed handover"))))))
   check)
  (physical-store-check-live store)
  (funcall check)
  t)

(defstruct (physical-reseed-follow-receipt-consumer (:conc-name receipt-consumer-))
  ;; Borrow the owner, root, roles, store and PID context for one operation.
  ;; Keep private receipt bytes and facts with a thread-bound phase and stream.
  owner root descriptors store base binding check pid stream bytes snapshot journal
  row root-row proof phase removed-p active-p thread)

(defmethod print-object ((consumer physical-reseed-follow-receipt-consumer) stream)
  (print-unreadable-object (consumer stream :type t :identity t)))

(defun physical-reseed-follow-consumption-check-owner (consumer)
  ;; The consumer is usable only on its creating thread while the owner is busy.
  (unless (and (physical-reseed-follow-receipt-consumer-p consumer)
               (receipt-consumer-active-p consumer)
               (eq (receipt-consumer-thread consumer) sb-thread:*current-thread*)
               (retirement-startup-busy-p (receipt-consumer-owner consumer)))
    (error "receipt consumption ended, changed thread or lost its operation"))
  (physical-reseed-follow-retirement-check-startup-owner (receipt-consumer-owner consumer))
  t)

(defun physical-reseed-follow-consumption-check-source (consumer)
  ;; Check the held inode and private bytes around ownership checks.  After
  ;; the last owner check, repeat both checks without further callbacks.  After
  ;; unlink, require zero links and continued absence of the receipt name.
  (physical-reseed-follow-consumption-check-owner consumer)
  (let* ((owner (receipt-consumer-owner consumer))
         (directory (retirement-startup-directory owner))
         (mount (retirement-startup-mount owner))
         (row (receipt-consumer-row consumer))
         (stream (receipt-consumer-stream consumer))
         (fd (sb-sys:fd-stream-fd stream))
         (bytes (receipt-consumer-bytes consumer))
         (current (make-array (length bytes) :element-type '(unsigned-byte 8))))
    (flet ((facts ()
             (physical-directory-check-reseed-follow-journals directory)
             (unless (and (= (if (receipt-consumer-removed-p consumer) 0 1) (fifth row))
                          (equal (rest row)
                                 (physical-reseed-follow-retirement-held-file-facts fd mount)))
               (error "receipt consumption changed its held source facts"))
             (if (receipt-consumer-removed-p consumer)
                 (physical-adoption-require-absent
                  (merge-pathnames "physical-reseed.retired" directory)
                  "consumed receipt name reappeared")
                 (unless (equal row
                                (physical-reseed-follow-retirement-named-row
                                 (namestring directory) "physical-reseed.retired" mount))
                   (error "receipt consumption changed its named source"))))
           (check-bytes ()
             (file-position stream 0)
             (unless (and (= (length current) (read-sequence current stream))
                          (eq :eof (read-byte stream nil :eof))
                          (equalp bytes current))
               (error "receipt consumption changed its canonical bytes"))))
      (facts)
      (check-bytes)
      (facts)
      (physical-reseed-follow-consumption-check-owner consumer)
      (facts)
      (check-bytes)
      (facts)))
  t)

(defun physical-reseed-follow-consumption-check-scope (consumer)
  ;; Combine receipt and borrowed resource checks for the current scope.
  (physical-reseed-follow-consumption-check-source consumer)
  (funcall (receipt-consumer-check consumer))
  t)

(defun physical-reseed-follow-consumption-extra (consumer root)
  ;; Supply copied receipt and PID rows to the layout observation.
  (unless (= root (receipt-consumer-root consumer))
    (error "receipt consumption changed its root descriptor"))
  (physical-reseed-follow-consumption-check-scope consumer)
  (append
   (unless (receipt-consumer-removed-p consumer)
     (list (cons "physical-reseed.retired" (copy-list (receipt-consumer-row consumer)))))
   (list (cons "postmaster.pid" (copy-list (getf (receipt-consumer-pid consumer) :row))))))

(defun physical-reseed-follow-consumption-observe (consumer)
  ;; Match the full layout on both sides of receive validation.
  (flet ((check () (physical-reseed-follow-consumption-check-scope consumer))
         (extra (root) (physical-reseed-follow-consumption-extra consumer root)))
    (let* ((entry (cons "postmaster.pid" (copy-list (getf (receipt-consumer-pid consumer) :row))))
           (evidence (list :entry entry :verify #'check))
           (names (mapcar #'car (extra (receipt-consumer-root consumer)))))
      (flet ((layout ()
               (physical-reseed-follow-retirement-layout-state-core
                (receipt-consumer-descriptors consumer) (receipt-consumer-snapshot consumer)
                #'check #'extra nil evidence #'check names)))
        (let ((proof (layout)))
          (physical-reseed-follow-retirement-check-receive
           (receipt-consumer-descriptors consumer) (receipt-consumer-store consumer)
           (getf (receipt-consumer-snapshot consumer) :record)
           (receipt-consumer-base consumer) (receipt-consumer-binding consumer) #'check)
          (unless (equal proof (layout))
            (error "receipt consumption layout changed around receive validation"))
          (append proof
                  (list :journal
                        (physical-adoption-attempt-value-copy (receipt-consumer-journal consumer)))))))))

(defun physical-reseed-follow-consumption-check (consumer)
  ;; Require a fresh observation to match the frozen proof.
  (unless (equal (receipt-consumer-proof consumer)
                 (physical-reseed-follow-consumption-observe consumer))
    (error "receipt consumption changed its frozen full proof"))
  t)

(defun physical-reseed-follow-consumption-point (consumer point)
  ;; Check the frozen proof around each test callback.
  (physical-reseed-follow-consumption-check consumer)
  (physical-reseed-follow-test-point
   point (merge-pathnames "physical-reseed.retired"
                          (retirement-startup-directory (receipt-consumer-owner consumer))))
  (physical-reseed-follow-consumption-check consumer))

(defun physical-reseed-follow-consumption-claim (consumer expected next)
  ;; Claim the next phase before callbacks or effects can reenter it.
  (sb-sys:without-interrupts
    (unless (and (receipt-consumer-active-p consumer)
                 (eq (receipt-consumer-thread consumer) sb-thread:*current-thread*)
                 (retirement-startup-busy-p (receipt-consumer-owner consumer))
                 (retirement-startup-uncertain-p (receipt-consumer-owner consumer))
                 (eq expected (receipt-consumer-phase consumer)))
      (error "receipt consumption cannot claim this phase"))
    (setf (receipt-consumer-phase consumer) next)))

(defun physical-reseed-follow-consumption-check-effect (consumer phase)
  ;; Effects require the claimed phase and the owner's uncertainty latch.
  (physical-reseed-follow-consumption-check-scope consumer)
  (unless (and (eq phase (receipt-consumer-phase consumer))
               (retirement-startup-uncertain-p (receipt-consumer-owner consumer)))
    (error "receipt consumption effect lost its claim or uncertainty"))
  t)

(defun physical-reseed-follow-consumption-sync-receipt (consumer)
  ;; After admission, latch uncertainty and sync the receipt inode and root
  ;; before allowing the unlink.
  (declare (notinline sb-posix:fsync))
  (sb-sys:without-interrupts
    (unless (and (receipt-consumer-active-p consumer)
                 (eq (receipt-consumer-thread consumer) sb-thread:*current-thread*)
                 (retirement-startup-busy-p (receipt-consumer-owner consumer))
                 (eq :admitted (receipt-consumer-phase consumer))
                 (not (receipt-consumer-removed-p consumer)))
      (error "receipt consumption requires completed local admission"))
    (setf (retirement-startup-uncertain-p (receipt-consumer-owner consumer)) t
          (receipt-consumer-phase consumer) :syncing))
  (physical-reseed-follow-consumption-point consumer :before-receipt-consumption-inode-fsync)
  (physical-reseed-follow-consumption-check-effect consumer :syncing)
  (sb-posix:fsync (sb-sys:fd-stream-fd (receipt-consumer-stream consumer)))
  (physical-reseed-follow-consumption-point consumer :after-receipt-consumption-inode-fsync)
  (physical-reseed-follow-consumption-point consumer :before-receipt-consumption-baseline-root-fsync)
  (physical-reseed-follow-consumption-check-effect consumer :syncing)
  (sb-posix:fsync (receipt-consumer-root consumer))
  (physical-reseed-follow-consumption-point consumer :after-receipt-consumption-baseline-root-fsync)
  (physical-reseed-follow-consumption-check-effect consumer :syncing)
  (setf (receipt-consumer-phase consumer) :synced))

(defun physical-reseed-follow-consumption-unlink (consumer)
  ;; Seal the held inode and root immediately after unlink, before callbacks.
  ;; Compare the new full proof with the seal and stable historical facts.
  (declare (notinline port-unlinkat))
  (physical-reseed-follow-consumption-claim consumer :synced :unlinking)
  (physical-reseed-follow-consumption-point consumer :before-receipt-consumption-unlink)
  (let* ((root (receipt-consumer-root consumer))
         (mount (retirement-startup-mount (receipt-consumer-owner consumer)))
         (before (receipt-consumer-proof consumer))
         (old-row (receipt-consumer-row consumer))
         (row nil) (root-row nil))
    (sb-sys:without-interrupts
      (physical-reseed-follow-consumption-check-effect consumer :unlinking)
      (unless (and (not (receipt-consumer-removed-p consumer)) (= 1 (fifth old-row)))
        (error "receipt consumption requires its named single-link source"))
      (unless (zerop (port-unlinkat root "physical-reseed.retired" 0))
        (error "cannot consume retirement receipt with errno ~d" (sb-alien:get-errno)))
      (setf row (cons "physical-reseed.retired"
                      (physical-reseed-follow-retirement-held-file-facts
                       (sb-sys:fd-stream-fd (receipt-consumer-stream consumer)) mount))
            root-row (physical-reseed-install-old-observe root "." mount))
      (physical-adoption-require-absent
       (merge-pathnames "physical-reseed.retired" (physical-image-fd-path root))
       "consumed receipt name reappeared")
      (physical-directory-check-reseed-follow-journals (physical-image-fd-path root))
      (unless (and (= 0 (fifth row))
                   (equal (subseq old-row 1 4) (subseq row 1 4))
                   (equal (subseq old-row 5 8) (subseq row 5 8))
                   (equal (subseq (receipt-consumer-root-row consumer) 0 6) (subseq root-row 0 6)))
        (error "receipt consumption changed facts beyond its owned unlink"))
      (setf (receipt-consumer-removed-p consumer) t
            (receipt-consumer-row consumer) row
            (receipt-consumer-snapshot consumer)
            (list :record (getf (receipt-consumer-snapshot consumer) :record)
                  :identity (physical-reseed-install-row-file-facts row) :row (copy-list row))
            (receipt-consumer-journal consumer)
            (list :geometry :consumed :follow nil :receipt nil :mount mount)
            (receipt-consumer-phase consumer) :removed))
    (let ((proof (physical-reseed-follow-consumption-observe consumer)))
      (unless (and (equal root-row (cdr (assoc :root (getf (getf proof :layout) :directories))))
                   (equal (physical-reseed-follow-receipt-stable-proof before)
                          (physical-reseed-follow-receipt-stable-proof proof)))
        (error "receipt consumption changed frozen historical state"))
      (physical-reseed-follow-consumption-check-source consumer)
      (unless (equal root-row (physical-reseed-install-old-observe root "." mount))
        (error "receipt consumption changed its immediate root seal"))
      (setf (receipt-consumer-proof consumer) proof
            (receipt-consumer-root-row consumer) root-row)))
  (physical-reseed-follow-consumption-point consumer :after-receipt-consumption-unlink))

(defun physical-reseed-follow-consumption-sync-removed-root (consumer)
  ;; Sync the parent directory to make the removal durable.
  (declare (notinline sb-posix:fsync))
  (physical-reseed-follow-consumption-claim consumer :removed :syncing-removed)
  (physical-reseed-follow-consumption-point consumer :before-receipt-consumption-root-fsync)
  (physical-reseed-follow-consumption-check-effect consumer :syncing-removed)
  (sb-posix:fsync (receipt-consumer-root consumer))
  (physical-reseed-follow-consumption-point consumer :after-receipt-consumption-root-fsync)
  (physical-reseed-follow-consumption-check-effect consumer :syncing-removed)
  (setf (receipt-consumer-phase consumer) :durable))

(defun physical-reseed-follow-consumption-run (consumer)
  ;; Match handover evidence and admit local standby state before removal.
  ;; Recheck intent, local state and the full proof after the removal sync.
  (unless (eq :ready (receipt-consumer-phase consumer))
    (error "receipt consumption witness was already consumed"))
  (setf (receipt-consumer-phase consumer) :checking
        (receipt-consumer-proof consumer) (physical-reseed-follow-consumption-observe consumer))
  (unless (equal (retirement-startup-evidence (receipt-consumer-owner consumer))
                 (physical-reseed-follow-retired-startup-projection
                  (receipt-consumer-descriptors consumer) (receipt-consumer-proof consumer)))
    (error "receipt consumption differs from its startup handover"))
  (setf (receipt-consumer-root-row consumer)
        (cdr (assoc :root (getf (getf (receipt-consumer-proof consumer) :layout) :directories))))
  (physical-directory-check-reseed-install-attempt-intent
   (retirement-startup-directory (receipt-consumer-owner consumer)))
  (physical-reseed-follow-startup-local-state-at
   (receipt-consumer-descriptors consumer) (receipt-consumer-store consumer)
   (lambda () (physical-reseed-follow-consumption-check-scope consumer)))
  (physical-reseed-follow-consumption-check consumer)
  (setf (receipt-consumer-phase consumer) :admitted)
  (physical-reseed-follow-consumption-sync-receipt consumer)
  (physical-reseed-follow-consumption-unlink consumer)
  (physical-reseed-follow-consumption-sync-removed-root consumer)
  (physical-directory-check-reseed-intent (retirement-startup-directory (receipt-consumer-owner consumer)))
  (physical-reseed-follow-startup-local-state-at
   (receipt-consumer-descriptors consumer) (receipt-consumer-store consumer)
   (lambda () (physical-reseed-follow-consumption-check-scope consumer)))
  (physical-reseed-follow-consumption-check consumer)
  t)

(defun call-with-physical-reseed-follow-consumption-context (owner function)
  ;; Keep the receipt stream open while the store, roles, PID reader and root
  ;; close.  Capture the stamp while roles are held, then close the stream before
  ;; auditing the stamp.
  (declare (notinline physical-image-openat))
  (let ((fd nil) (stream nil) (consumer nil) (stamp nil) (result nil))
    (multiple-value-prog1
        (unwind-protect
             (progn
               (call-with-physical-reseed-follow-retirement-startup-root
                owner
                (lambda (root root-check)
                  (let* ((source (physical-reseed-follow-retired-journal-at root root-check))
                         (snapshot (getf source :snapshot)))
                    (unless (eq :receipt (getf source :geometry))
                      (error "receipt consumption requires receipt-only geometry"))
                    (sb-sys:without-interrupts
                      (setf fd (physical-image-openat
                                root "physical-reseed.retired"
                                (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                        +postmaster-o-cloexec+) 0))
                      (when (minusp fd) (setf fd nil) (error "cannot open receipt for consumption"))
                      (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8))))
                    (call-with-physical-reseed-follow-retirement-owned-pid
                     root root-check
                     (lambda (pid pid-check)
                       (call-with-physical-reseed-follow-retirement-resources
                        root (getf snapshot :record) pid-check
                        (lambda (descriptors store base binding check)
                          (setf consumer
                                (make-physical-reseed-follow-receipt-consumer
                                 :owner owner :root root :descriptors (copy-tree descriptors)
                                 :store store :base base :binding binding :check check :pid pid
                                 :stream stream :bytes (copy-seq (getf source :bytes))
                                 :snapshot (physical-adoption-attempt-value-copy snapshot)
                                 :journal (loop for key in '(:geometry :follow :receipt :mount)
                                                append (list key (physical-adoption-attempt-value-copy
                                                                  (getf source key))))
                                 :row (copy-list (getf snapshot :row))
                                 :phase :ready :active-p t :thread sb-thread:*current-thread*))
                          (setf result (funcall function consumer))
                          (unless (and (receipt-consumer-removed-p consumer)
                                       (eq :durable (receipt-consumer-phase consumer)))
                            (error "receipt consumption did not finish durable removal"))
                          (setf stamp
                                (physical-reseed-follow-retirement-role-stamp
                                 (receipt-consumer-descriptors consumer)
                                 (getf (getf (receipt-consumer-proof consumer) :layout) :names)))
                          (physical-reseed-follow-consumption-check consumer))))))))
               (physical-reseed-follow-consumption-check-source consumer)
               (values result stamp))
          (when consumer (setf (receipt-consumer-active-p consumer) nil))
          (if stream (close stream) (when fd (sb-posix:close fd))))
      (physical-reseed-follow-retirement-audit-stamp
       stamp (lambda () (physical-reseed-follow-retirement-check-startup-owner owner))))))

(defun physical-reseed-follow-retirement-consume-receipt (owner)
  ;; Clear evidence and uncertainty after the closed-resource audit succeeds.
  ;; The final pure audit follows the owner guard.  A failure after effects
  ;; begin restores uncertainty before the operation releases its busy flag.
  (let ((stamp nil) (started nil) (success nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-physical-reseed-follow-retirement-operation
              owner
              (lambda ()
                (unless (eq :receipt (getf (getf (retirement-startup-evidence owner) :journal) :geometry))
                  (error "receipt consumption requires receipt-only handover evidence"))
                (multiple-value-bind (result closed-stamp)
                    (unwind-protect
                         (call-with-physical-reseed-follow-consumption-context
                          owner #'physical-reseed-follow-consumption-run)
                      (when (retirement-startup-uncertain-p owner) (setf started t)))
                  (unless result (error "receipt consumption did not complete"))
                  (setf stamp closed-stamp))
                (sb-sys:without-interrupts
                  (setf (retirement-startup-evidence owner) nil
                        (retirement-startup-uncertain-p owner) nil))
                t)
              (lambda () (physical-reseed-follow-retirement-check-stamp stamp))
              (lambda () (when started (setf (retirement-startup-uncertain-p owner) t))))
           (setf success t))
      (when (and started (not success))
        (setf (retirement-startup-uncertain-p owner) t)))))

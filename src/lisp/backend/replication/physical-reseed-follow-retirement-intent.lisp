;;; Publish the initial retirement intent from Active step 2 under published
;;; postmaster ownership.  Recover an exact complete temporary for that intent.
(in-package #:pg)

(defun call-with-physical-reseed-follow-retirement-journal (root name expected function check)
  ;; Hold the named journal descriptor around the callback and bind it to the
  ;; expected snapshot.  Close the descriptor when the scope ends.
  (let ((expected (physical-adoption-attempt-value-copy expected))
        (fd nil) (stream nil))
    (unwind-protect
         (progn
           (funcall check)
           (physical-reseed-follow-entry root name)
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       root name
                       (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                               +postmaster-o-cloexec+) 0))
             (when (minusp fd)
               (setf fd nil)
               (error "cannot open retirement journal")))
           (sb-sys:without-interrupts
             (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8))))
           (physical-reseed-follow-test-point
            :after-retirement-journal-open
            (merge-pathnames name (physical-image-fd-path root)))
           (funcall check)
           (let ((state (physical-reseed-follow-retirement-journal-state-at root stream name)))
             (unless (equal expected state)
               (error "retirement journal differs from its expected held snapshot"))
             (multiple-value-prog1
                 (funcall function stream (physical-adoption-attempt-value-copy state))
               (funcall check))))
      (if stream (close stream) (when fd (sb-posix:close fd))))))

(defun physical-reseed-follow-retirement-intent-layout (descriptors layout)
  ;; Keep the nonjournal layout projection stable while the temporary name is
  ;; created and removed during publication.
  (let ((copy (copy-list layout))
        (names (copy-list (getf layout :names))))
    (setf (getf names :root)
          (remove "physical-reseed.follow.tmp" (getf names :root) :test #'equal)
          (getf copy :names) names)
    (getf (physical-reseed-follow-startup-projection descriptors (list :layout copy)) :layout)))

(defun physical-reseed-follow-retirement-prefix-proof-at (descriptors store value prefix check read-extra &optional alias)
  ;; Prove the requested prefix and optional pending alias around active
  ;; receive validation.  The initial intent wrapper uses strict prefix zero.
  (funcall check)
  (let* ((record (physical-reseed-follow-copy value))
         (root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (plan (physical-reseed-follow-retirement-provisional-plan-at root saved record check))
         (base-fds (loop for role in '(:base :global :wal)
                         collect (cons role (physical-reseed-follow-configuration-role-fd descriptors role))))
         (mount (physical-adoption-install-mount-id nil :directory-fd root)))
    (physical-reseed-follow-retirement-check-prefix record prefix)
    (flet ((observe ()
             (multiple-value-bind (base control binding model images row)
                 (physical-reseed-follow-configuration-base-proof
                  root saved base-fds plan record (getf (getf record :configuration) :step) check)
               (declare (ignore control model row))
               (let* ((extra (funcall read-extra root))
                      (positions (physical-reseed-follow-retirement-positions record binding prefix))
                      (expected (physical-reseed-follow-retirement-inventories-at plan record binding prefix extra alias))
                      (layout
                        (physical-reseed-follow-layout-proof-at
                         descriptors plan record positions images extra expected mount check check read-extra
                         (lambda ()
                           (physical-reseed-follow-activation-active-row
                            descriptors record binding positions mount)))))
                 (values
                  (multiple-value-bind (install attempt)
                      (physical-reseed-follow-retirement-journals-at descriptors record prefix alias check)
                    (append (when alias (list :location :alias))
                            (list :prefix prefix :layout layout :install install :attempt attempt
                                  :receive (physical-adoption-attempt-value-copy
                                            (physical-store-control-tuple store)))))
                  base binding)))))
      (multiple-value-bind (before base binding) (observe)
        (physical-reseed-follow-retirement-check-receive descriptors store record base binding check)
        (let ((after (observe)))
          (unless (equal before after)
            (error "retirement nonjournal state changed around active receive validation"))
          (funcall check)
          (setf (getf after :layout)
                (physical-reseed-follow-retirement-intent-layout descriptors (getf after :layout)))
          after)))))

(defun physical-reseed-follow-retirement-intent-proof-at (descriptors store value check read-extra)
  ;; Prove prefix zero and its files and check the active receive state between
  ;; two layout observations.  The result binds nonjournal state for publication.
  (physical-reseed-follow-retirement-prefix-proof-at descriptors store value 0 check read-extra))

(defun physical-reseed-follow-retirement-check-move-proof (before after step)
  ;; Compare two already-proved observations.  Equal prefixes require exact
  ;; equality.  The caller supplies full proofs and held guards around a move.
  (unless (and (typep step '(integer 0 1))
               (member (getf before :prefix) (list step (1+ step)))
               (eql (getf after :prefix) (1+ step)))
    (error "archive move proof has an invalid prefix transition"))
  (flet ((stable (proof)
           (let* ((copy (physical-adoption-attempt-value-copy proof))
                  (layout (getf copy :layout))
                  (names (getf layout :names))
                  (key (if (zerop step) :install :attempt))
                  (name (if (zerop step) "physical-reseed.install" "physical-reseed.attempt"))
                  (file (getf copy key)))
             (setf (getf copy :prefix) nil
                   (getf layout :directories)
                   (loop for (role . row) in (getf layout :directories)
                         collect (cons role (if (member role '(:root :saved)) (subseq row 0 6) row)))
                   (getf names :root) (remove name (getf names :root) :test #'equal)
                   (getf names :saved) (remove name (getf names :saved) :test #'equal)
                   (getf file :identity) (butlast (getf file :identity))
                   (getf file :row) (butlast (getf file :row)))
             copy)))
    (unless (if (= (getf before :prefix) (getf after :prefix))
                (equal before after)
                (equal (stable before) (stable after)))
      (error "archive move changed frozen nonjournal state")))
  t)

(defstruct (physical-reseed-follow-retirement-intent-witness (:conc-name retirement-intent-))
  ;; Private, thread-bound publication state ties the held resources and
  ;; predecessor to one candidate, proof and sequence of effects.
  owner thread active-p descriptors store parent-check pid snapshot old-stream old-bytes
  candidate bytes proof phase temporary-fd temporary-stream temporary-fixed temporary-state
  written effect-p publication-claimed-p)

(defmethod print-object ((witness physical-reseed-follow-retirement-intent-witness) stream)
  (print-unreadable-object (witness stream :type t :identity t)))

(defun physical-reseed-follow-retirement-intent-check-scope (witness)
  ;; Require the original owner, thread and active resources.  The busy and
  ;; uncertainty flags must agree with whether effects have begun.
  (unless (and (physical-reseed-follow-retirement-intent-witness-p witness)
               (retirement-intent-active-p witness)
               (eq (retirement-intent-thread witness) sb-thread:*current-thread*))
    (error "retirement intent witness ended or changed thread"))
  (let ((owner (retirement-intent-owner witness)))
    (physical-reseed-follow-retirement-check-startup-owner owner)
    (unless (and (retirement-startup-busy-p owner)
                 (eql (not (null (retirement-startup-uncertain-p owner)))
                      (not (null (retirement-intent-effect-p witness)))))
      (error "retirement intent lost its busy or uncertainty state")))
  (funcall (retirement-intent-parent-check witness))
  t)

(defun physical-reseed-follow-retirement-intent-root (witness)
  (physical-reseed-follow-configuration-role-fd (retirement-intent-descriptors witness) :root))

(defun physical-reseed-follow-retirement-intent-check-effect (witness &optional phases)
  ;; Require an effect-bearing operation and the allowed publication phase.
  (physical-reseed-follow-retirement-intent-check-scope witness)
  (unless (and (retirement-intent-effect-p witness)
               (retirement-startup-uncertain-p (retirement-intent-owner witness)))
    (error "retirement effect requires an uncertain operation"))
  (when (and phases (not (member (retirement-intent-phase witness) phases)))
    (error "retirement effect requires phase ~s" phases))
  t)

(defun physical-reseed-follow-retirement-intent-check-old (witness renamed)
  ;; Check the original held bytes and facts before and after replacement.
  ;; After rename the held predecessor has lost its directory link.
  (let* ((root (physical-reseed-follow-retirement-intent-root witness))
         (stream (retirement-intent-old-stream witness))
         (snapshot (retirement-intent-snapshot witness)))
    (if renamed
        (let* ((before (rest (getf snapshot :row)))
               (after (physical-reseed-follow-retirement-held-file-facts
                       (sb-sys:fd-stream-fd stream)
                       (retirement-startup-mount (retirement-intent-owner witness))))
               (bytes (make-array (sixth before) :element-type '(unsigned-byte 8))))
          (unless (and (zerop (fourth after))
                       (equal (subseq before 0 3) (subseq after 0 3))
                       (equal (subseq before 4 7) (subseq after 4 7)))
            (error "replaced retirement predecessor changed its held facts"))
          (file-position stream 0)
          (unless (and (= (length bytes) (read-sequence bytes stream))
                       (eq :eof (read-byte stream nil :eof))
                       (equalp bytes (retirement-intent-old-bytes witness))
                       (equal after
                              (physical-reseed-follow-retirement-held-file-facts
                               (sb-sys:fd-stream-fd stream)
                               (retirement-startup-mount (retirement-intent-owner witness)))))
            (error "replaced retirement predecessor changed its held bytes")))
        (multiple-value-bind (state bytes)
            (physical-reseed-follow-retirement-journal-state-at root stream "physical-reseed.follow")
          (unless (and (equal snapshot state) (equalp bytes (retirement-intent-old-bytes witness)))
            (error "retirement predecessor changed before replacement")))))
  t)

(defun physical-reseed-follow-retirement-intent-check-temporary (witness renamed)
  ;; Check the held temporary against its name, identity and expected bytes.
  ;; During writing, check the exact written prefix.
  (let* ((root (physical-reseed-follow-retirement-intent-root witness))
         (stream (retirement-intent-temporary-stream witness))
         (fd (retirement-intent-temporary-fd witness))
         (name (if renamed "physical-reseed.follow" "physical-reseed.follow.tmp"))
         (mount (retirement-startup-mount (retirement-intent-owner witness)))
         (fixed (retirement-intent-temporary-fixed witness)))
    (physical-reseed-follow-store-fd-check root fd name fixed)
    (physical-reseed-follow-store-fd-check root (sb-sys:fd-stream-fd stream) name fixed)
    (if (eq :writing (retirement-intent-phase witness))
        (multiple-value-bind (identity row) (physical-reseed-follow-entry root name t)
          (let* ((size (retirement-intent-written witness))
                 (bytes (make-array size :element-type '(unsigned-byte 8))))
            (unless (= size (sixth identity))
              (error "retirement temporary differs from its written prefix length"))
            (file-position stream 0)
            (unless (and (= size (read-sequence bytes stream))
                         (eq :eof (read-byte stream nil :eof))
                         (equalp bytes (subseq (retirement-intent-bytes witness) 0 size))
                         (equal (rest row) (physical-reseed-follow-retirement-held-file-facts fd mount))
                         (equal (rest row) (physical-reseed-follow-retirement-held-file-facts
                                            (sb-sys:fd-stream-fd stream) mount))
                         (equal (list identity row)
                                (multiple-value-list (physical-reseed-follow-entry root name t))))
              (error "retirement temporary differs from its held written prefix"))
            (list :identity identity :row row)))
        (multiple-value-bind (state bytes)
            (physical-reseed-follow-retirement-journal-state-at root stream name)
          (let ((expected (retirement-intent-temporary-state witness)))
            (unless (and (equal (getf state :record) (retirement-intent-candidate witness))
                         (equalp bytes (retirement-intent-bytes witness))
                         (if renamed
                             (and (equal (subseq (getf expected :identity) 0 7)
                                         (subseq (getf state :identity) 0 7))
                                  (equal (subseq (getf expected :row) 1 8)
                                         (subseq (getf state :row) 1 8)))
                             (equal expected state)))
              (error "retirement candidate changed its complete held image")))
          (unless (equal (rest (getf state :row))
                         (physical-reseed-follow-retirement-held-file-facts fd mount))
            (error "retirement writer differs from its held candidate reader"))
          state))))

(defun physical-reseed-follow-retirement-intent-extra (witness root)
  ;; Describe the journal and PID names admitted at the current phase.
  (physical-reseed-follow-retirement-intent-check-scope witness)
  (unless (= root (physical-reseed-follow-retirement-intent-root witness))
    (error "retirement intent proof changed its root descriptor"))
  (let* ((renamed (member (retirement-intent-phase witness) '(:renamed :durable)))
         (temporary (retirement-intent-temporary-stream witness))
         (state (when temporary
                  (physical-reseed-follow-retirement-intent-check-temporary witness renamed))))
    (physical-reseed-follow-retirement-intent-check-old witness renamed)
    (when (or renamed (not temporary))
      (when (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)
        (error "retirement intent has an unexpected temporary name")))
    (append
     (list (cons "physical-reseed.follow"
                 (physical-adoption-attempt-value-copy
                  (getf (if renamed state (retirement-intent-snapshot witness)) :row))))
     (when (and temporary (not renamed))
       (list (cons "physical-reseed.follow.tmp" (physical-adoption-attempt-value-copy (getf state :row)))))
     (list (cons "postmaster.pid"
                 (physical-adoption-attempt-value-copy (getf (retirement-intent-pid witness) :row)))))))

(defun physical-reseed-follow-retirement-intent-check (witness)
  ;; Reprove frozen nonjournal state and check a complete temporary against
  ;; the candidate and observed layout.
  (physical-reseed-follow-retirement-intent-check-scope witness)
  (let* ((renamed (member (retirement-intent-phase witness) '(:renamed :durable)))
         (record (if renamed (retirement-intent-candidate witness)
                     (getf (retirement-intent-snapshot witness) :record)))
         (prefix (getf (retirement-intent-proof witness) :prefix))
         (proof (physical-reseed-follow-retirement-prefix-proof-at
                 (retirement-intent-descriptors witness) (retirement-intent-store witness) record prefix
                 (lambda () (physical-reseed-follow-retirement-intent-check-scope witness))
                 (lambda (root) (physical-reseed-follow-retirement-intent-extra witness root)))))
    (unless (equal (retirement-intent-proof witness) proof)
      (error "retirement publication changed frozen nonjournal state"))
    (when (eq :complete (retirement-intent-phase witness))
      (physical-reseed-follow-retirement-check-temporary-layout
       (retirement-intent-descriptors witness) record (retirement-intent-candidate witness)
       prefix (getf proof :install) (getf proof :attempt))))
  (physical-reseed-follow-retirement-intent-check-scope witness)
  t)

(defun call-with-physical-reseed-follow-retirement-intent-existing (witness temporary function)
  ;; Hold an existing complete temporary through the callback.  Its witness is
  ;; revoked before the held descriptor closes.
  (if temporary
      (call-with-physical-reseed-follow-retirement-journal
       (physical-reseed-follow-retirement-intent-root witness) "physical-reseed.follow.tmp" temporary
       (lambda (stream state)
         (setf (retirement-intent-temporary-stream witness) stream
               (retirement-intent-temporary-fd witness) (sb-sys:fd-stream-fd stream)
               (retirement-intent-temporary-fixed witness) (subseq (getf state :identity) 0 5)
               (retirement-intent-temporary-state witness) state
               (retirement-intent-phase witness) :complete)
         (unwind-protect (funcall function witness)
           (setf (retirement-intent-active-p witness) nil)))
       (retirement-intent-parent-check witness))
      (funcall function witness)))

(defun call-with-physical-reseed-follow-retirement-intent-base-context
    (owner expected make-candidate function)
  (call-with-physical-reseed-follow-retirement-startup-root
   owner
   (lambda (root root-check)
     (call-with-physical-reseed-follow-retirement-owned-pid
      root root-check
      (lambda (pid pid-check)
        (call-with-physical-reseed-follow-retirement-resources
         root (getf (getf expected :snapshot) :record) pid-check
         (lambda (descriptors store base binding check)
           (declare (ignore base binding))
           (call-with-physical-reseed-follow-retirement-journal
            root "physical-reseed.follow" (getf expected :snapshot)
            (lambda (stream snapshot)
              (let* ((candidate (funcall make-candidate snapshot))
                     (witness
                       (make-physical-reseed-follow-retirement-intent-witness
                        :owner owner :thread sb-thread:*current-thread* :active-p t
                        :descriptors descriptors :store store :parent-check check :pid pid
                        :snapshot snapshot :old-stream stream
                        :old-bytes (nth-value 1 (physical-reseed-follow-retirement-journal-state-at
                                                 root stream "physical-reseed.follow"))
                        :candidate candidate :bytes (physical-reseed-follow-image candidate)
                        :phase :predecessor :written 0)))
                (unwind-protect
                     (call-with-physical-reseed-follow-retirement-intent-existing
                      witness (getf expected :temporary-file) function)
                  (setf (retirement-intent-active-p witness) nil))))
            check))))))))

(defun call-with-physical-reseed-follow-retirement-intent-context (owner expected function)
  (call-with-physical-reseed-follow-retirement-intent-base-context
   owner expected
   (lambda (snapshot)
     (let ((attempt (getf expected :attempt)))
       (physical-reseed-follow-retirement-next-record
        (getf snapshot :record)
        (list :identity (getf attempt :identity) :digest (getf attempt :digest)))))
   (lambda (current)
     (let ((proof (physical-reseed-follow-retirement-intent-proof-at
                   (retirement-intent-descriptors current) (retirement-intent-store current)
                   (getf (retirement-intent-snapshot current) :record)
                   (lambda () (physical-reseed-follow-retirement-intent-check-scope current))
                   (lambda (fd) (physical-reseed-follow-retirement-intent-extra current fd)))))
       (dolist (key '(:prefix :layout :install :attempt))
         (unless (equal (getf expected key) (getf proof key))
           (error "retirement predecessor changed after startup reproof")))
       (setf (retirement-intent-proof current) proof)
       (physical-reseed-follow-retirement-intent-check current)
       (funcall function current)))))

(defun physical-reseed-follow-retirement-intent-point (witness point)
  ;; Recheck the witness and effect authority around each fault point.
  (physical-reseed-follow-retirement-intent-check-effect witness)
  (physical-reseed-follow-retirement-intent-check witness)
  (physical-reseed-follow-test-point
   point (merge-pathnames "physical-reseed.follow"
                          (retirement-startup-directory (retirement-intent-owner witness))))
  (physical-reseed-follow-retirement-intent-check witness)
  (physical-reseed-follow-retirement-intent-check-effect witness))

(defun physical-reseed-follow-retirement-intent-sync (witness role before after)
  ;; Sync the held descriptor for the requested phase with checks around it.
  (declare (notinline sb-posix:fsync))
  (physical-reseed-follow-retirement-intent-check-effect witness)
  (multiple-value-bind (fd phases)
      (ecase role
        (:old-final
         (values (sb-sys:fd-stream-fd (retirement-intent-old-stream witness)) '(:predecessor :complete)))
        (:predecessor-root
         (values (physical-reseed-follow-retirement-intent-root witness) '(:predecessor :complete)))
        (:temporary (values (retirement-intent-temporary-fd witness) '(:complete)))
        (:candidate (values (retirement-intent-temporary-fd witness) '(:renamed)))
        (:root (values (physical-reseed-follow-retirement-intent-root witness) '(:renamed))))
    (physical-reseed-follow-retirement-intent-check-effect witness phases)
    (physical-reseed-follow-retirement-intent-point witness before)
    (physical-reseed-follow-retirement-intent-check-effect witness phases)
    (sb-posix:fsync fd)
    (physical-reseed-follow-retirement-intent-point witness after)
    (physical-reseed-follow-retirement-intent-check-effect witness phases)))

(defun call-with-physical-reseed-follow-retirement-intent-created (witness function)
  ;; Create the private temporary exclusively and hold writer and reader
  ;; descriptors through the callback.  Revoke the witness before cleanup.
  (physical-reseed-follow-retirement-intent-check-effect witness '(:predecessor))
  (when (or (retirement-intent-temporary-stream witness)
            (retirement-intent-temporary-fd witness))
    (error "retirement temporary is already held"))
  (let ((root (physical-reseed-follow-retirement-intent-root witness))
        (fd nil) (reader-fd nil) (stream nil))
    (unwind-protect
         (progn
           (physical-reseed-follow-retirement-intent-point witness :before-retirement-temporary-create)
           (sb-sys:without-interrupts
             (physical-reseed-follow-retirement-intent-check-effect witness '(:predecessor))
             (setf fd (physical-image-openat
                       root "physical-reseed.follow.tmp"
                       (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                               sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+) #o600))
             (when (minusp fd) (setf fd nil) (error "cannot create retirement intent temporary")))
           (sb-posix:fchmod fd #o600)
           (sb-sys:without-interrupts
             (setf reader-fd (physical-image-openat
                              root "physical-reseed.follow.tmp"
                              (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                      +postmaster-o-cloexec+) 0))
             (when (minusp reader-fd)
               (setf reader-fd nil)
               (error "cannot open retirement intent temporary reader")))
           (sb-sys:without-interrupts
             (setf stream (sb-sys:make-fd-stream reader-fd :input t :element-type '(unsigned-byte 8))))
           (setf (retirement-intent-temporary-fd witness) fd
                 (retirement-intent-temporary-stream witness) stream
                 (retirement-intent-temporary-fixed witness)
                 (subseq (physical-reseed-follow-file-identity (sb-posix:fstat fd) t) 0 5)
                 (retirement-intent-phase witness) :writing)
           (physical-reseed-follow-retirement-intent-point witness :after-retirement-temporary-create)
           (funcall function witness))
      (setf (retirement-intent-active-p witness) nil)
      (unwind-protect
           (if stream (close stream) (when reader-fd (sb-posix:close reader-fd)))
        (when fd (sb-posix:close fd))))))

(defun physical-reseed-follow-retirement-intent-write (witness)
  ;; Write the canonical candidate in checked chunks and validate the complete
  ;; image through the held reader.
  (physical-reseed-follow-retirement-intent-check-effect witness '(:writing))
  (let* ((bytes (retirement-intent-bytes witness))
         (limit (or *physical-reseed-follow-write-limit* (length bytes)))
         (path (merge-pathnames "physical-reseed.follow.tmp"
                                (physical-image-fd-path (physical-reseed-follow-retirement-intent-root witness)))))
    (unless (and (integerp limit) (plusp limit)) (error "invalid retirement intent write limit"))
    (loop while (< (retirement-intent-written witness) (length bytes))
          for start = (retirement-intent-written witness)
          for end = (min (length bytes) (+ start limit))
          do (physical-reseed-follow-retirement-intent-check witness)
             (physical-reseed-follow-retirement-intent-check-effect witness '(:writing))
             (let ((*physical-adoption-install-test-hook* nil))
               (physical-adoption-install-write-bytes
                (retirement-intent-temporary-fd witness) (subseq bytes start end) start path))
             (setf (retirement-intent-written witness) end)
             (physical-reseed-follow-retirement-intent-point witness :after-retirement-write-chunk))
    (setf (retirement-intent-temporary-state witness)
          (physical-reseed-follow-retirement-temporary-state-at
           (physical-reseed-follow-retirement-intent-root witness)
           (retirement-intent-temporary-stream witness))
          (retirement-intent-phase witness) :complete)
    (physical-reseed-follow-retirement-intent-check witness)))

(defun physical-reseed-follow-retirement-intent-adopt (witness)
  ;; Read back the promoted candidate, reprove its layout and update the
  ;; owner's handover evidence after the journal is durable.
  (physical-reseed-follow-retirement-intent-check-effect witness '(:renamed))
  (let* ((root (physical-reseed-follow-retirement-intent-root witness))
         (descriptors (retirement-intent-descriptors witness))
         (snapshot (physical-reseed-follow-retirement-journal-state-at
                    root (retirement-intent-temporary-stream witness) "physical-reseed.follow"))
         (pid (retirement-intent-pid witness))
         (check (retirement-intent-parent-check witness))
         (state (physical-reseed-follow-retirement-layout-state-at
                 descriptors snapshot check
                 (lambda (fd) (physical-reseed-follow-retirement-intent-extra witness fd))
                 nil (list :entry (cons "postmaster.pid" (getf pid :row)) :verify check))))
    (physical-reseed-follow-retirement-intent-point witness :after-retirement-readback)
    (physical-reseed-follow-check-snapshot root snapshot)
    (let ((evidence (physical-reseed-follow-startup-projection
                     descriptors (append state (list :pid-file pid :temporary-file nil)))))
      (sb-sys:without-interrupts
        (physical-reseed-follow-retirement-intent-check-effect witness '(:renamed))
        (setf (retirement-startup-evidence (retirement-intent-owner witness)) evidence
              (retirement-intent-phase witness) :durable)))
    (values (physical-reseed-follow-copy (retirement-intent-candidate witness))
            (physical-adoption-attempt-value-copy snapshot))))

(defun physical-reseed-follow-retirement-intent-promote (witness)
  ;; Sync the complete temporary, rename it over the final journal, then sync
  ;; the candidate and root directory before adopting the readback.
  (physical-reseed-follow-retirement-intent-check-effect witness '(:complete))
  (let ((root (physical-reseed-follow-retirement-intent-root witness)))
    (physical-reseed-follow-retirement-intent-sync
     witness :temporary :before-retirement-temporary-fsync :after-retirement-temporary-fsync)
    (physical-reseed-follow-retirement-intent-point witness :before-retirement-rename)
    (sb-sys:without-interrupts
      (physical-reseed-follow-retirement-intent-check-effect witness '(:complete))
      (unless (zerop (port-renameat root "physical-reseed.follow.tmp" root "physical-reseed.follow"))
        (error "cannot promote retirement intent: errno ~d" (sb-alien:get-errno)))
      (setf (retirement-intent-phase witness) :renamed))
    (physical-reseed-follow-retirement-intent-point witness :after-retirement-rename)
    (physical-reseed-follow-retirement-intent-sync
     witness :candidate :before-retirement-candidate-fsync :after-retirement-candidate-fsync)
    (physical-reseed-follow-retirement-intent-sync
     witness :root :before-retirement-directory-fsync :after-retirement-directory-fsync)
    (physical-reseed-follow-retirement-intent-adopt witness)))

(defun physical-reseed-follow-retirement-intent-claim-publication (witness)
  ;; Claim the witness once before publication callbacks can run.
  (sb-sys:without-interrupts
    (unless (and (physical-reseed-follow-retirement-intent-witness-p witness)
                 (retirement-intent-active-p witness)
                 (eq (retirement-intent-thread witness) sb-thread:*current-thread*))
      (error "retirement publication requires its live thread-bound witness"))
    (when (retirement-intent-publication-claimed-p witness)
      (error "retirement publication already claimed"))
    (setf (retirement-intent-publication-claimed-p witness) t))
  t)

(defgeneric physical-reseed-follow-retirement-authorize-progress (authority witness))

(defmethod physical-reseed-follow-retirement-authorize-progress (authority witness)
  (declare (ignore authority witness))
  (error "retirement progress requires a live archive authority"))

(defun physical-reseed-follow-retirement-intent-publish-held (witness &optional authority)
  ;; Mark the operation uncertain before effects.  Sync the old final and root,
  ;; then write or reuse the exact complete temporary and promote it.
  (physical-reseed-follow-retirement-intent-claim-publication witness)
  (unless (and (or authority (not (retirement-intent-effect-p witness)))
               (case (retirement-intent-phase witness)
                 (:predecessor (and (null (retirement-intent-temporary-stream witness))
                                    (null (retirement-intent-temporary-fd witness))))
                 (:complete (and (retirement-intent-temporary-stream witness)
                                 (retirement-intent-temporary-fd witness)))))
    (error "retirement publisher already started or changed phase"))
  (if authority
      (physical-reseed-follow-retirement-authorize-progress authority witness)
      (progn
        (physical-reseed-follow-retirement-intent-check-scope witness)
        (physical-reseed-follow-retirement-intent-check witness)
        (unless (and (eql 0 (getf (retirement-intent-proof witness) :prefix))
                     (null (getf (getf (retirement-intent-snapshot witness) :record) :retirement)))
          (error "initial retirement publisher requires its strict Active2 predecessor"))
        (sb-sys:without-interrupts
          (setf (retirement-intent-effect-p witness) t
                (retirement-startup-uncertain-p (retirement-intent-owner witness)) t))))
  (physical-reseed-follow-retirement-intent-sync
   witness :old-final
   :before-retirement-old-final-fsync :after-retirement-old-final-fsync)
  (physical-reseed-follow-retirement-intent-sync
   witness :predecessor-root
   :before-retirement-predecessor-directory-fsync :after-retirement-predecessor-directory-fsync)
  (if (retirement-intent-temporary-stream witness)
      (physical-reseed-follow-retirement-intent-promote witness)
      (call-with-physical-reseed-follow-retirement-intent-created
       witness
       (lambda (current)
         (physical-reseed-follow-retirement-intent-write current)
         (physical-reseed-follow-retirement-intent-promote current)))))

(defun physical-reseed-follow-retirement-publish-initial (owner)
  ;; Publish the first retirement intent from a freshly proved Active state.
  ;; Keep uncertainty through resource cleanup and clear it only after the
  ;; publication context returns normally.
  (call-with-physical-reseed-follow-retirement-operation
   owner
   (lambda ()
     (let ((expected
             (call-with-physical-reseed-follow-retirement-startup-observation
              owner
              (lambda (descriptors state verify)
                (funcall verify)
                (let ((copy (physical-adoption-attempt-value-copy state)))
                  (setf (getf copy :layout)
                        (physical-reseed-follow-retirement-intent-layout descriptors (getf state :layout)))
                  copy)))))
       (unless (and (zerop (getf expected :prefix))
                    (null (getf (getf (getf expected :snapshot) :record) :retirement)))
         (error "initial retirement publication requires Active2 without retirement"))
       (when (and *physical-reseed-follow-write-limit*
                  (not (and (integerp *physical-reseed-follow-write-limit*)
                            (plusp *physical-reseed-follow-write-limit*))))
         (error "invalid retirement intent write limit"))
       (multiple-value-prog1
           (call-with-physical-reseed-follow-retirement-intent-context
            owner expected #'physical-reseed-follow-retirement-intent-publish-held)
         (setf (retirement-startup-uncertain-p owner) nil))))))

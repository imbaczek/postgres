(in-package #:pg)

(defun call-with-physical-reseed-follow-retirement-journal (root name expected function check)
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
  (let ((copy (copy-list layout))
        (names (copy-list (getf layout :names))))
    (setf (getf names :root)
          (remove "physical-reseed.follow.tmp" (getf names :root) :test #'equal)
          (getf copy :names) names)
    (getf (physical-reseed-follow-startup-projection descriptors (list :layout copy)) :layout)))

(defun physical-reseed-follow-retirement-intent-proof-at (descriptors store value check read-extra)
  (funcall check)
  (let* ((record (physical-reseed-follow-copy value))
         (root (physical-reseed-follow-configuration-role-fd descriptors :root))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (plan (physical-reseed-follow-retirement-provisional-plan-at root saved record))
         (base-fds (loop for role in '(:base :global :wal)
                         collect (cons role (physical-reseed-follow-configuration-role-fd descriptors role))))
         (mount (physical-adoption-install-mount-id nil :directory-fd root)))
    (physical-reseed-follow-retirement-check-prefix record 0)
    (flet ((observe ()
             (multiple-value-bind (base control binding model images row)
                 (physical-reseed-follow-configuration-base-proof
                  root saved base-fds plan record (getf (getf record :configuration) :step) check)
               (declare (ignore control model row))
               (let* ((extra (funcall read-extra root))
                      (positions (physical-reseed-follow-retirement-positions record binding 0))
                      (expected (physical-reseed-follow-retirement-inventories plan record binding 0 extra))
                      (layout
                        (physical-reseed-follow-layout-proof-at
                         descriptors plan record positions images extra expected mount check check read-extra
                         (lambda ()
                           (physical-reseed-follow-activation-active-row
                            descriptors record binding positions mount)))))
                 (values
                  (list :prefix 0 :layout layout
                        :install (physical-reseed-follow-retirement-install-at descriptors record 0)
                        :attempt (physical-reseed-follow-retirement-attempt-at root root record 0)
                        :receive (physical-adoption-attempt-value-copy (physical-store-control-tuple store)))
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

(defstruct (physical-reseed-follow-retirement-intent-witness (:conc-name retirement-intent-))
  owner thread active-p descriptors store parent-check pid snapshot old-stream old-bytes
  candidate bytes proof phase temporary-fd temporary-stream temporary-fixed temporary-state
  written effect-p)

(defmethod print-object ((witness physical-reseed-follow-retirement-intent-witness) stream)
  (print-unreadable-object (witness stream :type t :identity t)))

(defun physical-reseed-follow-retirement-intent-check-scope (witness)
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
  (physical-reseed-follow-retirement-intent-check-scope witness)
  (unless (and (retirement-intent-effect-p witness)
               (retirement-startup-uncertain-p (retirement-intent-owner witness)))
    (error "retirement effect requires an uncertain operation"))
  (when (and phases (not (member (retirement-intent-phase witness) phases)))
    (error "retirement effect requires phase ~s" phases))
  t)

(defun physical-reseed-follow-retirement-intent-check-old (witness renamed)
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
  (physical-reseed-follow-retirement-intent-check-scope witness)
  (let* ((renamed (member (retirement-intent-phase witness) '(:renamed :durable)))
         (record (if renamed (retirement-intent-candidate witness)
                     (getf (retirement-intent-snapshot witness) :record)))
         (proof (physical-reseed-follow-retirement-intent-proof-at
                 (retirement-intent-descriptors witness) (retirement-intent-store witness) record
                 (lambda () (physical-reseed-follow-retirement-intent-check-scope witness))
                 (lambda (root) (physical-reseed-follow-retirement-intent-extra witness root)))))
    (unless (equal (retirement-intent-proof witness) proof)
      (error "retirement publication changed frozen nonjournal state"))
    (when (eq :complete (retirement-intent-phase witness))
      (physical-reseed-follow-retirement-check-temporary-layout
       (retirement-intent-descriptors witness) record (retirement-intent-candidate witness)
       0 (getf proof :install) (getf proof :attempt))))
  (physical-reseed-follow-retirement-intent-check-scope witness)
  t)

(defun call-with-physical-reseed-follow-retirement-intent-existing (witness temporary function)
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

(defun call-with-physical-reseed-follow-retirement-intent-context (owner expected function)
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
              (let* ((attempt (getf expected :attempt))
                     (candidate (physical-reseed-follow-retirement-next-record
                                 (getf snapshot :record)
                                 (list :identity (getf attempt :identity) :digest (getf attempt :digest))))
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
                      witness (getf expected :temporary-file)
                      (lambda (current)
                        (let ((proof (physical-reseed-follow-retirement-intent-proof-at
                                      descriptors store (getf snapshot :record)
                                      (lambda () (physical-reseed-follow-retirement-intent-check-scope current))
                                      (lambda (fd) (physical-reseed-follow-retirement-intent-extra current fd)))))
                          (dolist (key '(:prefix :layout :install :attempt))
                            (unless (equal (getf expected key) (getf proof key))
                              (error "retirement predecessor changed after startup reproof")))
                          (setf (retirement-intent-proof current) proof)
                          (physical-reseed-follow-retirement-intent-check current)
                          (funcall function current))))
                  (setf (retirement-intent-active-p witness) nil))))
            check))))))))

(defun physical-reseed-follow-retirement-intent-point (witness point)
  (physical-reseed-follow-retirement-intent-check-effect witness)
  (physical-reseed-follow-retirement-intent-check witness)
  (physical-reseed-follow-test-point
   point (merge-pathnames "physical-reseed.follow"
                          (retirement-startup-directory (retirement-intent-owner witness))))
  (physical-reseed-follow-retirement-intent-check witness)
  (physical-reseed-follow-retirement-intent-check-effect witness))

(defun physical-reseed-follow-retirement-intent-sync (witness role before after)
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

(defun physical-reseed-follow-retirement-intent-publish-held (witness)
  (physical-reseed-follow-retirement-intent-check-scope witness)
  (unless (and (not (retirement-intent-effect-p witness))
               (case (retirement-intent-phase witness)
                 (:predecessor (and (null (retirement-intent-temporary-stream witness))
                                    (null (retirement-intent-temporary-fd witness))))
                 (:complete (and (retirement-intent-temporary-stream witness)
                                 (retirement-intent-temporary-fd witness)))))
    (error "retirement publisher already started or changed phase"))
  (physical-reseed-follow-retirement-intent-check witness)
  (sb-sys:without-interrupts
    (setf (retirement-intent-effect-p witness) t
          (retirement-startup-uncertain-p (retirement-intent-owner witness)) t))
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

;; Bind cancellation publication to a checked, one-use local authority.

(in-package #:pg)

(defun physical-reseed-follow-cancellation-preconfiguration-record (value)
  ;; Publication is allowed only before configuration and later follow phases.
  (let* ((record (physical-reseed-follow-copy value))
         (generations (getf record :generations)))
    (unless (and (notany (lambda (key) (getf record key))
                         '(:configuration :settlement :activation :retirement))
                 (<= (length generations) 1)
                 (every (lambda (generation) (member (getf generation :phase) '(:preparing :proved)))
                        generations))
      (error "cancellation publication requires an unconfigured follow predecessor"))
    record))

(defstruct (physical-reseed-follow-cancellation-authority
            (:constructor %make-physical-reseed-follow-cancellation-authority)
            (:copier nil)
            (:print-function physical-libpq-print-private-object))
  ;; Bind publication to the owner, root, thread, predecessor and canonical
  ;; bytes for one active scope.  The phase limits each effect to one claim.
  (owner nil :read-only t) (root-fd nil :read-only t) (thread nil :read-only t)
  (candidate nil :read-only t) (previous nil :read-only t) (bytes nil :read-only t)
  (mode nil :read-only t) (scope nil :read-only t) (check nil :read-only t)
  fd published (phase :ready) active-p)

(defvar *physical-reseed-follow-cancellation-authority* nil)

(defun physical-reseed-follow-cancellation-claim (owner root-fd candidate previous operation
                                                  &key bytes fd)
  ;; Claim the write or promotion before callbacks can reenter the operation.
  (let ((authority *physical-reseed-follow-cancellation-authority*))
    (when (or authority (getf candidate :cancellation)
              (getf (getf previous :record) :cancellation))
      (unless (and (physical-reseed-follow-cancellation-authority-p authority)
                   (physical-reseed-follow-cancellation-authority-active-p authority)
                   (physical-reseed-follow-owner-busy-p owner)
                   (eq sb-thread:*current-thread*
                       (physical-reseed-follow-cancellation-authority-thread authority)))
        (error "cancellation publication requires its live typed authority"))
      (sb-sys:without-interrupts
        (funcall (physical-reseed-follow-cancellation-authority-scope authority)
                 authority owner root-fd candidate previous bytes)
        (ecase operation
          (:write
           (unless (and (eq :write (physical-reseed-follow-cancellation-authority-mode authority))
                        (eq :ready (physical-reseed-follow-cancellation-authority-phase authority))
                        bytes
                        (equalp bytes (physical-reseed-follow-cancellation-authority-bytes authority)))
             (error "cancellation write authority was reused or changed"))
           (setf (physical-reseed-follow-cancellation-authority-phase authority) :writing))
          (:promote
           (unless (and (eql fd (physical-reseed-follow-cancellation-authority-fd authority))
                        (case (physical-reseed-follow-cancellation-authority-mode authority)
                          (:write (eq :written (physical-reseed-follow-cancellation-authority-phase authority)))
                          (:recover (eq :ready (physical-reseed-follow-cancellation-authority-phase authority)))))
             (error "cancellation promotion authority was reused or changed"))
           (setf (physical-reseed-follow-cancellation-authority-phase authority) :promoting))))
      (funcall (physical-reseed-follow-cancellation-authority-check authority)))
    authority))

(defun physical-reseed-follow-cancellation-write-complete (owner root-fd fd fixed candidate previous)
  ;; Bind the completed temporary descriptor to the claimed write.
  (let ((authority *physical-reseed-follow-cancellation-authority*))
    (when authority
      (unless (and (physical-reseed-follow-cancellation-authority-active-p authority)
                   (eq sb-thread:*current-thread*
                       (physical-reseed-follow-cancellation-authority-thread authority))
                   (eq :write (physical-reseed-follow-cancellation-authority-mode authority))
                   (eq :writing (physical-reseed-follow-cancellation-authority-phase authority)))
        (error "cancellation temporary has no active write authority"))
      (funcall (physical-reseed-follow-cancellation-authority-scope authority)
               authority owner root-fd candidate previous nil)
      (unless (= (length (physical-reseed-follow-cancellation-authority-bytes authority))
                 (sixth (physical-reseed-follow-store-fd-check
                         root-fd fd "physical-reseed.follow.tmp" fixed)))
        (error "cancellation temporary differs from its frozen image"))
      (sb-sys:without-interrupts
        (unless (eq :writing (physical-reseed-follow-cancellation-authority-phase authority))
          (error "cancellation write authority changed before completion"))
        (setf (physical-reseed-follow-cancellation-authority-fd authority) fd
              (physical-reseed-follow-cancellation-authority-phase authority) :written)))))

(defun physical-reseed-follow-cancellation-check-publication-state (authority root-fd temporary-identity)
  (let ((published (physical-reseed-follow-cancellation-authority-published authority)))
    (cond
      (published
       (physical-reseed-follow-check-snapshot root-fd published)
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root-fd))
        "published cancellation has an unexpected temporary"))
      (temporary-identity
       (unless (equal temporary-identity
                      (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp"))
         (error "cancellation recovery temporary changed"))))))

(defun call-with-physical-reseed-follow-cancellation-authority
    (owner root-fd candidate previous bytes mode check function &optional fd temporary-identity)
  ;; Keep the authority in acquiring phase during its initial proof so a
  ;; reentrant call cannot use it before verification finishes.
  (when *physical-reseed-follow-cancellation-authority*
    (error "cancellation authority cannot reenter acquisition or publication"))
  (let* ((frozen (physical-reseed-follow-cancellation-preconfiguration-record candidate))
         (before (physical-adoption-attempt-value-copy previous))
         (image (copy-seq bytes))
         (thread sb-thread:*current-thread*)
         (authority nil))
    (unless (equalp image (physical-reseed-follow-image frozen))
      (error "cancellation authority requires its canonical pending image"))
    (unless (and (physical-reseed-follow-owner-busy-p owner) (member mode '(:write :recover))
                 (equal frozen (physical-reseed-follow-cancellation-begin-record (getf before :record))))
      (error "cancellation authority requires its exact pending successor"))
    (labels ((scope (value actual-owner actual-root actual-candidate actual-previous actual-bytes)
               (unless (and (eq value authority)
                            (eq owner (physical-reseed-follow-cancellation-authority-owner value))
                            (eql root-fd (physical-reseed-follow-cancellation-authority-root-fd value))
                            (eq actual-owner owner) (eql actual-root root-fd)
                            (eq thread sb-thread:*current-thread*)
                            (physical-reseed-follow-cancellation-authority-active-p value)
                            (equal frozen actual-candidate) (equal before actual-previous)
                            (or (null actual-bytes) (equalp image actual-bytes))
                            (equal frozen (physical-reseed-follow-cancellation-authority-candidate value))
                            (equal before (physical-reseed-follow-cancellation-authority-previous value))
                            (equalp image (physical-reseed-follow-cancellation-authority-bytes value)))
                 (error "cancellation authority changed candidate, predecessor or scope")))
             (verify ()
               (scope authority owner root-fd frozen before image)
               (physical-reseed-follow-check-active-attempt owner)
               (funcall check)
               (physical-reseed-follow-cancellation-check-publication-state
                authority root-fd temporary-identity)))
      (setf authority
            (%make-physical-reseed-follow-cancellation-authority
             :owner owner :root-fd root-fd :thread thread :mode mode :fd fd
             :candidate (physical-reseed-follow-copy frozen)
             :previous (physical-adoption-attempt-value-copy before) :bytes (copy-seq image)
             :scope #'scope :check #'verify :phase :acquiring :active-p t))
      (let ((*physical-reseed-follow-cancellation-authority* authority))
        (unwind-protect
             (progn
               (verify)
               (sb-sys:without-interrupts
                 (setf (physical-reseed-follow-cancellation-authority-phase authority) :ready))
               (funcall function #'verify))
          (setf (physical-reseed-follow-cancellation-authority-active-p authority) nil))))))

(defun physical-reseed-follow-cancellation-local-proof (owner root-fd value)
  ;; Check local intent and any proof already present in the predecessor.
  (let ((record (physical-reseed-follow-cancellation-preconfiguration-record value)))
    (physical-reseed-follow-check-local-intent owner root-fd record)
    (when (eq :proved (getf (first (getf record :generations)) :phase))
      (physical-reseed-follow-proof-at owner root-fd record))
    record))

(defmethod physical-reseed-follow-published-snapshot :around
    ((owner physical-reseed-follow-owner) root-fd candidate snapshot previous)
  ;; Adopt exactly one verified published snapshot into this authority.
  (let ((authority *physical-reseed-follow-cancellation-authority*))
    (when authority
      (unless (and (physical-reseed-follow-cancellation-authority-active-p authority)
                   (eq :promoting (physical-reseed-follow-cancellation-authority-phase authority))
                   (null (physical-reseed-follow-cancellation-authority-published authority)))
        (error "cancellation publication authority already adopted a journal"))
      (funcall (physical-reseed-follow-cancellation-authority-scope authority)
               authority owner root-fd candidate previous nil)
      (unless (equal candidate (getf snapshot :record))
        (error "cancellation publication differs from its pending candidate"))
      (physical-reseed-follow-check-snapshot root-fd snapshot)
      (physical-adoption-require-absent
       (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root-fd))
       "cancellation publication retained its temporary")
      (setf (physical-reseed-follow-cancellation-authority-published authority)
            (physical-adoption-attempt-value-copy snapshot))))
  (call-next-method))

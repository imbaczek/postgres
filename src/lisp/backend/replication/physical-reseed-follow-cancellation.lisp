;; Begin or recover pending cancellation under the follow owner.

(in-package #:pg)

(defun physical-reseed-follow-cancellation-stable (owner root-fd snapshot check)
  ;; A stable pending marker can be synchronized after an actual abort.
  (let ((record (getf snapshot :record)))
    (unless (getf record :cancellation) (error "no pending cancellation to recover"))
    (physical-reseed-follow-cancellation-local-proof owner root-fd record)
    (funcall check)
    (setf (physical-reseed-follow-owner-uncertain-p owner) t)
    (multiple-value-bind (actual identity)
        (physical-reseed-follow-read-at root-fd "physical-reseed.follow" :stabilize t)
      (unless (and (equal record actual) (equal identity (getf snapshot :identity)))
        (error "stable cancellation changed during synchronization")))
    (physical-reseed-follow-check-snapshot root-fd snapshot)
    (funcall check)
    (values (physical-reseed-follow-copy record) snapshot)))

(declaim (ftype function physical-reseed-follow-cancellation-recover-source-closed))

(defun physical-reseed-follow-cancellation-recover-temporary (owner root-fd previous temporary check)
  ;; Promoting a temporary still requires the active attempt and its exact
  ;; pending successor.  Its descriptor stays held through promotion.
  (declare (notinline sb-posix:close))
  (when (getf (getf previous :record) :cancellation)
    (return-from physical-reseed-follow-cancellation-recover-temporary
      (physical-reseed-follow-cancellation-recover-source-closed owner root-fd previous temporary check)))
  (physical-reseed-follow-check-active-attempt owner)
  (multiple-value-bind (candidate identity)
      (physical-reseed-follow-read-at root-fd "physical-reseed.follow.tmp")
    (unless (and (equal temporary identity)
                 (equal candidate (physical-reseed-follow-cancellation-begin-record (getf previous :record))))
      (error "cancellation temporary is not its exact pending successor"))
    (physical-reseed-follow-cancellation-local-proof owner root-fd candidate)
    (funcall check)
    (physical-reseed-follow-check-snapshot root-fd previous)
    (let ((fd nil) (bytes (physical-reseed-follow-image candidate)))
      (unwind-protect
           (progn
             (setf (physical-reseed-follow-owner-uncertain-p owner) t)
             (sb-sys:without-interrupts
               (setf fd (physical-image-openat
                         root-fd "physical-reseed.follow.tmp"
                         (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                 +postmaster-o-cloexec+) 0)))
             (when (minusp fd) (setf fd nil) (error "cannot open cancellation recovery temporary"))
             (unless (equal identity (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
               (error "cancellation recovery descriptor changed"))
             (call-with-physical-reseed-follow-cancellation-authority
              owner root-fd candidate previous bytes :recover check
              (lambda (verify)
                (physical-reseed-follow-promote-intent
                 owner root-fd fd (subseq identity 0 5) candidate previous verify))
              fd identity))
        (when fd (sb-posix:close fd))))))

(defun physical-reseed-follow-cancellation-write-pending (owner root-fd previous check)
  (physical-reseed-follow-check-active-attempt owner)
  (let* ((candidate (physical-reseed-follow-cancellation-begin-record (getf previous :record)))
         (bytes (physical-reseed-follow-image candidate)))
    (physical-reseed-follow-cancellation-local-proof owner root-fd candidate)
    (funcall check)
    (call-with-physical-reseed-follow-cancellation-authority
     owner root-fd candidate previous bytes :write check
     (lambda (verify)
       (physical-reseed-follow-write-intent owner root-fd candidate bytes previous verify)))))

(defun call-with-physical-reseed-follow-cancellation-store (owner operation)
  ;; This scope owns busy through finalization.  Read-only admission leaves
  ;; certainty intact.  Publication latches uncertainty before possible disk
  ;; mutation and retains it until finalization succeeds.
  (physical-reseed-follow-check-idle-state owner)
  (setf (physical-reseed-follow-owner-busy-p owner) t)
  (unwind-protect
       (let ((expected
               (call-with-physical-directory-root
                owner
                (lambda (root-fd directory)
                  (declare (ignore directory))
                  (let ((snapshot (physical-reseed-follow-snapshot-at root-fd)))
                    (unless snapshot (error "cancellation requires a durable follow predecessor"))
                    (physical-reseed-follow-cancellation-preconfiguration-record (getf snapshot :record))
                    snapshot)))))
         (call-with-physical-reseed-follow-cancellation-finalization
          owner
          (lambda (root-fd check)
            (physical-reseed-follow-check-snapshot root-fd expected)
            (let* ((record (getf expected :record))
                   (temporary (physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp" t)))
              (cond
                (temporary
                 (unless (eq operation :recover)
                   (error "cancellation temporary requires explicit recovery"))
                 (physical-reseed-follow-cancellation-recover-temporary owner root-fd expected temporary check))
                ((getf record :cancellation)
                 (physical-reseed-follow-cancellation-stable owner root-fd expected check))
                ((eq operation :recover) (error "no pending cancellation to recover"))
                (t (physical-reseed-follow-cancellation-write-pending owner root-fd expected check)))))))
    (sb-sys:without-interrupts
      (setf (physical-reseed-follow-owner-busy-p owner) nil))))

(defun physical-reseed-follow-begin-cancellation (owner)
  ;; Publish pending cancellation for an eligible predecessor.
  (call-with-physical-reseed-follow-cancellation-store owner :begin))

(defun physical-reseed-follow-recover-cancellation (owner)
  ;; Reconcile a stable pending marker or an eligible temporary.
  (call-with-physical-reseed-follow-cancellation-store owner :recover))

(in-package #:pg)

(defun physical-reseed-follow-installation-candidate (context value)
  (let* ((candidate (physical-reseed-follow-copy value))
         (before (physical-reseed-follow-installation-context-record context))
         (old (getf before :configuration)) (new (getf candidate :configuration))
         (prefix (physical-reseed-follow-installation-context-prefix context))
         (maximum (length (physical-reseed-follow-configuration-moves
                           before (physical-reseed-follow-installation-context-binding context)))))
    (physical-reseed-follow-check-transition before candidate)
    (unless (and (equal (physical-reseed-follow-core-projection before)
                        (physical-reseed-follow-core-projection candidate))
                 (case (getf old :phase)
                   (:staged (and (eq :installing (getf new :phase)) (zerop prefix) (zerop (getf new :step))))
                   (:installing
                    (if (< (getf old :step) maximum)
                        (and (eq :installing (getf new :phase))
                             (= (1+ (getf old :step)) (getf new :step) prefix))
                        (and (eq :inert (getf new :phase)) (= maximum (getf new :step) prefix))))))
      (error "configuration candidate is not the exact next transition at the proved prefix"))
    candidate))

(defmethod physical-reseed-follow-published-snapshot
    ((owner physical-reseed-follow-configuration-owner) root-fd candidate snapshot previous)
  (unless *physical-reseed-follow-installation-scope*
    (return-from physical-reseed-follow-published-snapshot (call-next-method)))
  (let ((context (physical-reseed-follow-installation-context owner)))
    (unless (and (equal candidate *physical-reseed-follow-installation-candidate*)
                 (= root-fd (physical-reseed-follow-configuration-role-fd
                             (physical-reseed-follow-installation-context-descriptors context) :root))
                 (equal previous (physical-reseed-follow-installation-context-snapshot context))
                 (equal (physical-reseed-follow-installation-context-record context) (getf previous :record))
                 (equal candidate (getf snapshot :record)))
      (error "published configuration differs from this operation's frozen candidate or predecessor"))
    (setf (physical-reseed-follow-installation-context-record context)
          (physical-reseed-follow-installation-candidate context candidate)
          (physical-reseed-follow-installation-context-snapshot context) snapshot))
  t)

(defun physical-reseed-follow-installation-point (context check event)
  (funcall check)
  (physical-reseed-follow-check-active-attempt (physical-reseed-follow-installation-context-owner context))
  (physical-reseed-follow-test-point
   event (physical-image-fd-path
          (physical-reseed-follow-configuration-role-fd
           (physical-reseed-follow-installation-context-descriptors context) :root)))
  (funcall check)
  (physical-reseed-follow-check-active-attempt (physical-reseed-follow-installation-context-owner context)))

(defun physical-reseed-follow-installation-next-move (context check)
  (declare (notinline sb-posix:fsync))
  (let* ((owner (physical-reseed-follow-installation-context-owner context))
         (record (physical-reseed-follow-installation-context-record context))
         (step (getf (getf record :configuration) :step))
         (moves (physical-reseed-follow-configuration-moves
                 record (physical-reseed-follow-installation-context-binding context)))
         (prefix (physical-reseed-follow-installation-context-prefix context))
         (descriptors (physical-reseed-follow-installation-context-descriptors context)))
    (unless (and (< step (length moves)) (member prefix (list step (1+ step))))
      (error "configuration move does not follow its recorded prefix"))
    (destructuring-bind (logical from-role from-leaf to-role to-leaf) (nth step moves)
      (declare (ignore logical))
      (let ((from (physical-reseed-follow-configuration-role-fd descriptors from-role))
            (to (physical-reseed-follow-configuration-role-fd descriptors to-role)))
        (setf (physical-reseed-follow-owner-uncertain-p owner) t)
        (when (= prefix step)
          (physical-reseed-follow-installation-point context check :before-configuration-install-move)
          (sb-sys:without-interrupts
            (unless (zerop (physical-adoption-renameat2
                            from from-leaf to to-leaf +physical-adoption-rename-noreplace+))
              (error "cannot move the next configuration entry: errno ~d" (sb-alien:get-errno)))
            (setf (physical-reseed-follow-installation-context-prefix context) (1+ step)))
          (physical-reseed-follow-installation-point context check :after-configuration-install-move))
        (physical-reseed-follow-installation-point context check :before-configuration-install-source-fsync)
        (sb-posix:fsync from)
        (physical-reseed-follow-installation-point context check :after-configuration-install-source-fsync)
        (physical-reseed-follow-installation-point context check :before-configuration-install-destination-fsync)
        (sb-posix:fsync to)
        (physical-reseed-follow-installation-point context check :after-configuration-install-destination-fsync)
        (1+ step)))))

(defun physical-reseed-follow-installation-publish (context candidate check)
  (let* ((owner (physical-reseed-follow-installation-context-owner context))
         (previous (physical-reseed-follow-installation-context-snapshot context))
         (candidate (physical-reseed-follow-installation-candidate context candidate))
         (*physical-reseed-follow-installation-candidate* candidate)
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-installation-context-descriptors context) :root)))
    (physical-reseed-follow-installation-point context check :before-configuration-install-progress)
    (setf (physical-reseed-follow-owner-uncertain-p owner) t)
    (multiple-value-bind (record snapshot)
        (physical-reseed-follow-write-intent
         owner root candidate (physical-reseed-follow-image candidate) previous check)
      (unless (and (equal record (physical-reseed-follow-installation-context-record context))
                   (equal snapshot (physical-reseed-follow-installation-context-snapshot context)))
        (error "configuration publication did not install its exact canonical readback"))
      (physical-reseed-follow-installation-point context check :after-configuration-install-progress)
      (values record snapshot))))

(defun call-with-physical-reseed-follow-installation-operation (owner function)
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "installation operation requires dedicated configuration ownership"))
  (physical-reseed-follow-check-idle owner)
  (let ((*physical-reseed-follow-installation-request* t) (completed nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-physical-reseed-follow-store owner function)
           (setf completed t))
      (unless completed (setf (physical-reseed-follow-owner-uncertain-p owner) t)))))

(defun physical-reseed-follow-advance-configuration (owner)
  (call-with-physical-reseed-follow-installation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-installation-context owner))
            (record (physical-reseed-follow-copy (physical-reseed-follow-installation-context-record context)))
            (configuration (getf record :configuration))
            (maximum (length (physical-reseed-follow-configuration-moves
                              record (physical-reseed-follow-installation-context-binding context)))))
       (funcall check)
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
        "configuration temporary requires explicit recovery")
       (physical-reseed-follow-check-active-attempt owner)
       (if (eq :inert (getf configuration :phase))
           (values record (physical-reseed-follow-installation-context-snapshot context))
           (progn
             (case (getf configuration :phase)
               (:staged (setf (getf configuration :phase) :installing))
               (:installing
                (if (< (getf configuration :step) maximum)
                    (setf (getf configuration :step)
                          (physical-reseed-follow-installation-next-move context check))
                    (setf (getf configuration :phase) :inert)))
               (otherwise (error "configuration is not staged for installation")))
             (physical-reseed-follow-installation-publish context record check)))))))

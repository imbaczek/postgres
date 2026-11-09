(in-package #:pg)

(defun physical-reseed-follow-retirement-predecessor (record)
  ;; Validate against the core and all groups through activation, excluding
  ;; retirement itself.
  (physical-reseed-follow-copy
   (append (physical-reseed-follow-core-projection record)
           (loop for key in '(:configuration :settlement :activation)
                 append (list key (getf record key))))))

(defun physical-reseed-follow-retirement-attempt-file-copy (value record)
  ;; The identity follows physical-reseed-attempt-file-identity: device,
  ;; inode, mode, UID, link count, size, and two timestamps.  The digest and
  ;; size bind this reference to the original canonical attempt image.
  ;; Bounding the UID here does not establish ownership.  A publisher must
  ;; check actual ownership using the held file descriptor.
  (physical-reseed-install-check-literal value)
  (physical-follow-check-plist value '(:identity :digest))
  (let* ((identity (getf value :identity))
         (device (getf (getf (getf record :attempt) :local) :device))
         (entry (getf (getf record :activation) :entry))
         (identities (physical-reseed-follow-activation-protected-identities record)))
    (unless (and (listp identity) (= 8 (length identity))
                 (every (lambda (item) (typep item '(unsigned-byte 64))) (subseq identity 0 6))
                 (every (lambda (item) (typep item '(signed-byte 64))) (subseq identity 6))
                 (= device (first identity)) (= #o100600 (third identity))
                 (= 1 (fifth identity))
                 (typep (getf value :digest) '(unsigned-byte 64)))
      (error "invalid retirement attempt file reference"))
    (let ((bytes (physical-reseed-attempt-image (getf record :attempt))))
      (unless (and (= (length bytes) (sixth identity))
                   (= (physical-store-update-digest #xCBF29CE484222325 bytes) (getf value :digest)))
        (error "retirement attempt reference differs from its original canonical bytes")))
    (physical-reseed-install-register (fourth entry) (fifth entry) device identities)
    (physical-reseed-install-register device (second identity) device identities)
    (list :identity (copy-list identity) :digest (getf value :digest))))

(defun %physical-reseed-follow-retirement-copy (value record)
  (physical-follow-check-plist value '(:phase :step :attempt-file))
  (let* ((activation (getf record :activation))
         (phase (getf value :phase)) (step (getf value :step)))
    (unless (and (eq :active (getf activation :phase)) (= 2 (getf activation :step))
                 (typep step '(integer 0 2))
                 (or (eq phase :retiring) (and (eq phase :retired) (= step 2))))
      (error "retirement requires completed activation and an exact archive prefix"))
    (list :phase phase :step step
          :attempt-file (physical-reseed-follow-retirement-attempt-file-copy
                         (getf value :attempt-file) record))))

(defun physical-reseed-follow-retirement-copy (value record)
  ;; Retirement records the attempt-file identity and digest, with an archive
  ;; prefix in steps 0 to 2.  It requires active step 2; retired is step 2.
  ;; Retired permits final follow-fence removal; it does not say the fence
  ;; is already absent.
  (physical-reseed-install-check-literal (list value record))
  (physical-follow-check-plist value '(:phase :step :attempt-file))
  (%physical-reseed-follow-retirement-copy value (physical-reseed-follow-retirement-predecessor record)))

(defun physical-reseed-follow-retirement-check-transition (before after)
  ;; Freeze activation and the attempt-file reference while advancing one
  ;; archive step at a time.  A completed prefix can then become retired.
  (let ((old (getf before :retirement)) (new (getf after :retirement)))
    (unless (every (lambda (key) (equal (getf before key) (getf after key)))
                   '(:version :attempt :install :base :generations :configuration :settlement :activation))
      (error "retirement freezes activation and its historical predecessors"))
    (unless
        (and new
             (cond
               ((null old) (and (eq :retiring (getf new :phase)) (zerop (getf new :step))))
               ((equal old new) t)
               ((equal (getf old :attempt-file) (getf new :attempt-file))
                (and (eq :retiring (getf old :phase))
                     (or (and (eq :retiring (getf new :phase))
                              (= (getf new :step) (1+ (getf old :step))))
                         (and (= (getf old :step) (getf new :step) 2)
                              (eq :retired (getf new :phase))))))))
      (error "retirement cannot regress, skip an archive, rebind or disappear"))
    t))

(defun physical-reseed-follow-retirement-next-record (value attempt-file)
  ;; Advance one retirement step, using the observed attempt reference for
  ;; the initial record and the frozen reference thereafter.  Canonical copy
  ;; and transition checks preserve the other groups.  File and layout proof
  ;; and publication authority belong to the caller.
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation))
         (retirement (getf record :retirement)))
    (unless (and (eq :active (getf activation :phase))
                 (= 2 (getf activation :step))
                 (not (eq :retired (getf retirement :phase))))
      (error "retirement progress requires Active/2 before Retired/2"))
    (setf (getf record :retirement)
          (list :phase (if (and retirement (= 2 (getf retirement :step))) :retired :retiring)
                :step (if retirement (min 2 (1+ (getf retirement :step))) 0)
                :attempt-file attempt-file))
    (let ((candidate (physical-reseed-follow-copy record)))
      (physical-reseed-follow-check-transition value candidate)
      candidate)))

(in-package #:pg)

;;; Internal driver for a leased native base image.  READY proves the source
;;; image and its retained source position; it does not prove delivery to a
;;; receiver.  Target installation and handoff are outside this driver.

(defun physical-backup-lease-captured-base (manifest digest)
  (list :checkpoint-start (physical-manifest-checkpoint-start manifest)
        :base (getf manifest :end) :manifest-digest digest))

(defun physical-rewind-stage-seal-copy (seal)
  (physical-follow-check-plist seal '(:request :base :metadata))
  (let* ((request (physical-backup-lease-request-copy (getf seal :request)))
         (base (physical-backup-lease-base-copy (getf seal :base)))
         (metadata (getf seal :metadata))
         (bounds (physical-backup-metadata-bounds)))
    (unless (and (= (length metadata) (length bounds))
                 (every (lambda (entry bound)
                          (destructuring-bind (name size digest) entry
                            (and (equal name (first bound))
                                 (typep size '(unsigned-byte 64))
                                 (<= size (second bound))
                                 (typep digest '(unsigned-byte 64)))))
                        metadata bounds)
                 (= (third (first metadata)) (getf base :manifest-digest)))
      (error "invalid rewind stage metadata"))
    (list :request request :base base :metadata (copy-tree metadata))))

(defun physical-rewind-read-stage-seal (path)
  (multiple-value-bind (stream size)
      (open-bounded-regular-file path +physical-backup-lease-maximum-bytes+)
    (unwind-protect
         (let* ((bytes (make-array size :element-type '(unsigned-byte 8)))
                (fd (sb-sys:fd-stream-fd stream))
                (identity (physical-backup-lease-file-identity (sb-posix:fstat fd))))
           (unless (and (= size (sixth identity) (read-sequence bytes stream))
                        (eq :eof (read-byte stream nil :eof))
                        (equal identity (physical-backup-lease-file-identity (sb-posix:fstat fd)))
                        (equal identity (physical-backup-lease-file-identity
                                         (sb-posix:lstat (namestring path)))))
             (error "rewind stage seal changed while reading"))
           (values (physical-backup-lease-decode-literal
                    "PGREWINDSTAGE1" bytes +physical-backup-lease-maximum-bytes+
                    #'physical-rewind-stage-seal-copy)
                   (physical-store-update-digest #xCBF29CE484222325 bytes)))
      (close stream))))

(defun physical-rewind-write-stage-seal (directory request base entries)
  (let* ((seal (physical-rewind-stage-seal-copy
                (list :request request :base base
                      :metadata (subseq entries 0 (length (physical-backup-metadata-bounds))))))
         (path (merge-pathnames "rewind.seal" directory))
         (bytes (physical-backup-lease-encode-literal
                 "PGREWINDSTAGE1" seal +physical-backup-lease-maximum-bytes+)))
    (physical-backup-write-private-bytes
     path (merge-pathnames "rewind.tmp" directory) bytes :directory directory
     :check-readback (lambda ()
                       (unless (equal seal (physical-rewind-read-stage-seal path))
                         (error "rewind stage seal differs from publication"))))
    (physical-store-update-digest #xCBF29CE484222325 bytes)))

(defun physical-rewind-stage-sealed-entries (directory request base manifest seal-digest)
  (multiple-value-bind (seal actual)
      (physical-rewind-read-stage-seal (merge-pathnames "rewind.seal" directory))
    (unless (and (eql seal-digest actual)
                 (equal request (getf seal :request)) (equal base (getf seal :base)))
      (error "rewind stage seal differs from the lease"))
    (append (getf seal :metadata) (getf manifest :files))))

(defun physical-backup-lease-capture-active (owner)
  (with-live-operation
    (with-write-lock
      (with-wal-critical
        (physical-backup-lease-check-capture-owner owner))))
  (values))

(defun physical-backup-lease-copy-capture (target owner reservation)
  (with-write-lock
    (with-wal-critical
      (physical-backup-lease-check-capture-owner owner)))
  (let ((request (physical-backup-lease-capture-owner-request owner)))
    (physical-export-origin-capture-backup
     target (physical-bootstrap-copy-under-quiescence
             target (getf request :name) :lease-request request)
     reservation)))

;; Recheck the owner, source context, and exact retained pin under write and WAL
;; exclusion.  Install the verified origin and publish READY under the same
;; WAL publication fence, so failure cannot expose an unsafe memory state.
(defun physical-backup-lease-publish-capture (owner reservation origin manifest digest seal-digest)
  (with-live-operation
    (with-write-lock
      (with-wal-publication-fence (begin-publication)
        (let* ((record (physical-backup-lease-check-capture-owner owner))
               (request (getf record :request))
               (next (copy-list record)))
          (setf (getf next :phase) :ready
                (getf next :base) (physical-backup-lease-captured-base manifest digest))
          (when seal-digest (setf (getf next :rewind-seal-digest) seal-digest))
          (setf next (physical-backup-lease-transition record next))
          (multiple-value-bind (pin disk) (physical-source-retirement-pins (getf request :name))
            (declare (ignore disk))
            (unless (and pin (not (physical-source-pin-lost-p pin)))
              (wal-invalid-input "backup lease capture lost its retained source pin"))
            (physical-backup-lease-check-pin-record pin next)
            (physical-source-pin-check-wal pin))
          (physical-export-origin-install-under-lock reservation origin #'begin-publication)
          (physical-backup-lease-test-point :after-capture-origin next)
          (physical-backup-lease-publish-checked-under-lock record next #'begin-publication))))))

;; Hold reload exclusion through copying, verification, READY publication and
;; owner cleanup.  Copying also requires operation quiescence.  The caller
;; owns DIRECTORY and any partial contents; this driver does not delete them.
;; ON-START runs only for a new claim, after CAPTURING is durable and before
;; the long copy, allowing a wire caller to begin COPY within its deadline.
(defun physical-backup-lease-capture (directory request &key on-start before-ready)
  (let ((request (physical-backup-lease-request-copy request)))
    (when (plusp *live-operation-depth*)
      (error "backup lease capture must start outside a live operation"))
    (call-with-invalidated-slot-cleanup
     (lambda ()
       (sb-thread:with-mutex (*reload-run-lock*)
         (call-with-physical-backup-lease-capture
          request
          (lambda (owner)
            (when on-start (funcall on-start))
            (call-with-physical-export-origin-reservation
             (lambda (reservation)
               (let* ((target (with-live-operation
                                (prevent-recovery-primary-operation "leased native backup")
                                (physical-bootstrap-target directory)))
                      (manifest (with-live-operation-quiescence
                                  (physical-backup-lease-copy-capture target owner reservation))))
                 (physical-backup-lease-test-point :after-capture-copy
                                                   (physical-backup-lease-status request))
                 (physical-backup-lease-capture-active owner)
                 (multiple-value-bind (origin actual digest)
                     (physical-export-origin-verify-backup target manifest reservation)
                   (physical-backup-lease-test-point :after-capture-verification
                                                     (physical-backup-lease-status request))
                   (let ((seal-digest (when before-ready (funcall before-ready target actual digest))))
                     (physical-backup-lease-publish-capture owner reservation origin actual digest
                                                            seal-digest)))
                 manifest))))))))))

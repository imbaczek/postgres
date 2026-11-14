(in-package #:pg)

;; A handle tied to one owner, input, context and thread.  IMAGE is a callback
;; view; the private registry keeps independent bytes and the failure latch.

(defstruct (logical-slot-copy-source-proof-preparation
            (:constructor %make-logical-slot-copy-source-proof-preparation)
            (:print-function physical-libpq-print-private-object))
  (owner nil :read-only t) (context nil :read-only t) (input nil :read-only t)
  (image nil :read-only t))

(defvar *logical-slot-copy-source-proof-preparations* nil)

(defun logical-slot-copy-source-proof-input-receipt (input)
  ;; Read actual R, including its full catalog, and verify the canonical
  ;; generation hash.  Record catalog observations, next OID and local XID floor.
  (let* ((proof (logical-slot-copy-install-input-proof input))
         (certificate (logical-slot-copy-install-input-certificate input)))
    (multiple-value-bind (metadata catalog prefix generation)
        (physical-restartpoint-read (logical-slot-copy-committed-proof-state proof))
      (unless (and metadata
                   (equal metadata (logical-slot-copy-committed-proof-metadata proof))
                   (equalp prefix (logical-slot-copy-committed-proof-prefix proof))
                   (eql generation (getf certificate :promotion-generation))
                   (eql generation (logical-slot-copy-committed-proof-generation proof)))
        (error "source proof preparation lost its actual restartpoint"))
      (let ((image (physical-restartpoint-encode metadata catalog prefix)))
        (unless (= generation (physical-store-update-digest #xCBF29CE484222325 image))
          (error "source proof preparation requires a canonical restartpoint generation"))
        (values
         (logical-slot-copy-source-receipt-image
          (list :metadata (copy-tree metadata) :generation-size (length image)
                :catalog-size (length catalog)
                :catalog-digest (logical-slot-copy-import-digest catalog)
                :next-oid (getf (physical-history-read-catalog catalog) :next-oid)
                :local-floor (load-next-xid)))
         prefix)))))

(defun logical-slot-copy-source-proof-build-image (owner context input)
  ;; Build proof bytes from the genuine active cold journal and its full slot
  ;; prefix.  Keep the original pending certificate embedded in that journal.
  (let* ((record (logical-slot-copy-install-input-record owner context input))
         (outer (logical-slot-copy-install-operation-record
                 (logical-slot-copy-install-input-state-operation record)))
         (proof (logical-slot-copy-install-input-proof input))
         (certificate (logical-slot-copy-install-input-certificate input)))
    (logical-slot-copy-install-check-input owner context input)
    (unless (and (eq :cold (logical-slot-copy-install-operation-state-mode outer))
                 (eq :cold (logical-slot-copy-install-input-state-origin record))
                 (eq :active (logical-slot-copy-install-input-state-phase record))
                 (= (logical-slot-copy-install-input-state-prefix record)
                    (length (getf certificate :slots))))
      (error "source proof preparation requires the full active cold installation"))
    (multiple-value-bind (receipt prefix)
        (logical-slot-copy-source-proof-input-receipt input)
      (let ((image
              (logical-slot-copy-source-proof-encode
               (list :installation (copy-seq (logical-slot-copy-install-input-image input))
                     :base-proof (logical-slot-copy-read-file
                                  context "logical-slot-copy.base-verified"
                                  +logical-slot-copy-base-proof-maximum-bytes+)
                     :descriptor (logical-slot-copy-descriptor-encode
                                  (logical-slot-copy-committed-proof-descriptor proof))
                     :evidence (logical-slot-copy-import-evidence-encode
                                (logical-slot-copy-committed-proof-evidence proof))
                     :fact (physical-logical-encode-fact
                            (logical-slot-copy-install-input-fact input))
                     :restartpoint receipt :prefix (copy-seq prefix)
                     :child-control (logical-slot-copy-read-file
                                     context "global/pg_wal_control"
                                     (1+ +wal-control-maximum-bytes+))))))
        (logical-slot-copy-check-source-proof image)
        (logical-slot-copy-install-check-input owner context input)
        image))))

(defun logical-slot-copy-source-proof-preparation-record (owner context input attempt)
  (let ((record (assoc attempt *logical-slot-copy-source-proof-preparations* :test #'eq)))
    (unless (and record (logical-slot-copy-source-proof-preparation-p attempt)
                 (eq owner (logical-slot-copy-source-proof-preparation-owner attempt))
                 (eq context (logical-slot-copy-source-proof-preparation-context attempt))
                 (eq input (logical-slot-copy-source-proof-preparation-input attempt))
                 (eq sb-thread:*current-thread* (third record)))
      (error "source proof publication requires its registered preparation"))
    (logical-slot-copy-install-input-record owner context input)
    record))

(defun logical-slot-copy-source-proof-check-preparation (owner context input attempt &key (source t))
  ;; Full source checks rebuild from actual R.  Without SOURCE, each write
  ;; chunk still checks the sealed image, input observations and layout.
  (let ((record (logical-slot-copy-source-proof-preparation-record owner context input attempt))
        (complete nil))
    (unwind-protect
         (progn
           (funcall (fourth record) :check)
           (logical-slot-copy-install-check-input owner context input :source nil)
           (unless (and (typep (logical-slot-copy-source-proof-preparation-image attempt)
                               '(simple-array (unsigned-byte 8) (*)))
                        (equalp (second record)
                                (logical-slot-copy-source-proof-preparation-image attempt))
                        (or (not source)
                            (equalp (second record)
                                    (logical-slot-copy-source-proof-build-image owner context input))))
             (error "source proof preparation image or actual source changed"))
           (setf complete t))
      (unless complete (funcall (fourth record) :fail)))))

(defun call-with-logical-slot-copy-source-proof-preparation (owner context input function)
  ;; Keep a private image and independent seal for this scope.  Any failed
  ;; check or nonlocal exit revokes the input for the rest of the attempt.
  (let ((input-record (logical-slot-copy-install-input-record owner context input)))
    (when *logical-slot-copy-source-proof-preparations*
      (error "source proof preparation is already active"))
    (let* ((image (logical-slot-copy-source-proof-build-image owner context input))
           (attempt (%make-logical-slot-copy-source-proof-preparation
                     :owner owner :context context :input input :image (copy-seq image)))
           (active t) (failed nil) (complete nil)
           (record
             (list attempt (copy-seq image) sb-thread:*current-thread*
                   (lambda (command)
                     (ecase command
                       (:check
                        (unless (and active (not failed))
                          (error "source proof preparation ended or failed")))
                       (:fail
                        (setf failed t)
                        (logical-slot-copy-install-revoke-input input-record))))))
           (*logical-slot-copy-source-proof-preparations* (list record)))
      (unwind-protect
           (multiple-value-prog1
               (progn
                 (logical-slot-copy-source-proof-check-preparation owner context input attempt)
                 (funcall function attempt))
             (logical-slot-copy-source-proof-check-preparation owner context input attempt)
             (setf complete t))
        (unless complete (funcall (fourth record) :fail))
        (setf active nil)))))

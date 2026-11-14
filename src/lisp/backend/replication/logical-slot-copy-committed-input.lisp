;;; D is the copied base checkpoint end; C3 is saved phase 3 recovery control.
;;; CF is committed child global control, R the selected restartpoint, and F
;;; the promotion fork.

;;; Bind preserved input to committed child control under read ownership.
(in-package #:pg)

(defstruct (logical-slot-copy-committed-owner
            (:include physical-adoption-owner)
            (:constructor %make-logical-slot-copy-committed-owner)
            (:print-function physical-libpq-print-private-object))
  "Sibling read owner with held root, quiescence and generation exclusion.
Live mode captures the caller\'s pending state and receive store; cold mode captures NIL."
  (ownership nil :read-only t)
  (mode nil :read-only t)
  (state nil :read-only t)
  (store nil :read-only t))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-copy-committed-owner))
  (logical-slot-copy-check-import-locks)
  (unless (sb-thread:holding-mutex-p *physical-follow-generation-lock*)
    (error "committed import input requires the generation fence"))
  (ecase (logical-slot-copy-committed-owner-mode owner)
    (:cold
     (physical-adoption-check-startup-quiescence)
     (unless (and (null (logical-slot-copy-committed-owner-state owner))
                  (null (logical-slot-copy-committed-owner-store owner))
                  (null *physical-recovery-state*) (null *catalog*)
                  (null *wal-output-stream*) (null *clog-stream*)
                  (zerop *n-backends*) (zerop (hash-table-count *relation-streams*)))
       (error "cold committed import input requires unopened storage")))
    (:live
     (unless (and *wal-ready*
                  (eq *physical-recovery-state* (logical-slot-copy-committed-owner-state owner))
                  (eq *physical-receive-store* (logical-slot-copy-committed-owner-store owner))
                  (physical-store-p *physical-receive-store*)
                  (eql 3 (getf *physical-recovery-state* :phase)))
       (error "live committed import input lost its pending recovery state or store"))
     (physical-store-check-live *physical-receive-store*)))
  (logical-slot-copy-check-postmaster-owner
   owner (logical-slot-copy-committed-owner-ownership owner))
  (physical-adoption-check-startup)
  owner)

(defmethod physical-adoption-owner-extra-root-names ((owner logical-slot-copy-committed-owner))
  (physical-directory-check-owner owner)
  (loop for name in '("postmaster.pid" "logical-slot-copy.base-verified"
                      "logical-slot-copy.import" "logical-slot-copy.import.tmp"
                      "logical-slot-copy.evidence" "logical-slot-copy.evidence.tmp")
        when (slot-state-lstat
              (merge-pathnames name (physical-adoption-owner-directory owner)))
          collect name))

(defun logical-slot-copy-committed-observations (owner context manifest descriptor)
  "Observe immutable base files and sidecar pieces under the held input root."
  (logical-slot-copy-check-input owner context)
  (logical-slot-copy-input-sidecar-inventory context descriptor)
  (let ((observations nil) (index (make-hash-table :test #'equal)))
    (dolist (row (getf manifest :files))
      (when (and (physical-recovery-immutable-base-file-p (first row))
                 (not (equal "global/pg_wal_control" (first row))))
        (let ((actual (logical-slot-copy-file-row context (first row))))
          (unless (equal row (subseq actual 0 3))
            (error "committed import immutable file differs from its base manifest"))
          (push actual observations))))
    (dolist (name '("physical-backup.manifest" "logical-slot-copy.descriptor"
                    "logical-slot-copy.base-verified" "recovery.required" "physical-recovery.control"
                    "pg_hba.conf" "pg_ident.conf" "authentication.initialized"))
      (push (logical-slot-copy-file-row context name) observations))
    (logical-slot-copy-observe-pieces context descriptor observations index)))

(defun logical-slot-copy-read-preserved-inputs (owner context proof-bytes certificate control-phase)
  "Check preserved inputs against the caller-bound original pending certificate.
This does not observe the canonical import file or grant authority.  A caller
using a journal must first prove its exact certificate pair.  The caller must
recheck the returned input observations across its callback."
  (unless (member control-phase '(:parent :child))
    (error "unknown preserved import control phase"))
  (unless (logical-slot-copy-committed-owner-p owner)
    (error "committed import binding requires its sibling input owner"))
  (logical-slot-copy-check-input owner context)
  (unless proof-bytes (error "committed import lost its base receipt"))
  (unless (eq :pending (getf certificate :phase))
    (error "committed import binding requires its original pending certificate"))
  (let ((bound-observations nil))
    (dolist (name '("logical-slot-copy.in-progress" "logical-slot-copy.base-verified.tmp"
                    "logical-slot-copy.import.tmp" "logical-slot-copy.evidence.tmp"
                    "global/pg_wal_control.tmp"))
      (when (slot-state-lstat (merge-pathnames name (physical-image-context-root-path context)))
        (error "committed import has an unresolved temporary or copy fence")))
    (flet ((read-input (name maximum)
             (multiple-value-bind (bytes facts) (logical-slot-copy-read-file context name maximum)
               (push (list name (length bytes) (logical-slot-copy-import-digest bytes) facts)
                     bound-observations)
               bytes)))
      (multiple-value-bind (evidence evidence-bytes evidence-facts)
          (logical-slot-copy-import-read-artifact context :evidence)
        (unless evidence (error "committed import lost its durable evidence"))
        (push (list "logical-slot-copy.evidence" (length evidence-bytes)
                    (logical-slot-copy-import-digest evidence-bytes) evidence-facts)
              bound-observations)
        (let ((descriptor
                (logical-slot-copy-descriptor-decode
                 (read-input "logical-slot-copy.descriptor"
                             +logical-slot-copy-descriptor-maximum-bytes+))))
          (multiple-value-bind (manifest control plan status)
              (logical-slot-copy-import-evidence-check-certificate
               evidence certificate descriptor proof-bytes)
            (unless (and (equalp proof-bytes
                                 (read-input "logical-slot-copy.base-verified"
                                             +logical-slot-copy-base-proof-maximum-bytes+))
                         (equal (logical-slot-copy-base-proof-decode proof-bytes)
                                (logical-slot-copy-base-proof-value context manifest descriptor)))
              (error "committed import receipt differs from its held root"))
            (unless (and (equalp (getf evidence :manifest)
                                 (read-input "physical-backup.manifest" (* 16 1024 1024)))
                         (equalp (getf evidence :control)
                                 (read-input "physical-recovery.control"
                                             +physical-recovery-control-maximum-bytes+))
                         (equalp (physical-recovery-marker-bytes)
                                 (read-input "recovery.required" 4096)))
              (error "committed import differs from preserved base or pending control"))
            (let ((expected
                    (ecase control-phase
                      (:parent (logical-slot-copy-descriptor-base-control descriptor))
                      (:child
                       (let ((*physical-recovery-state* control))
                         (physical-promotion-child-control-image
                          plan (getf (getf plan :snapshot) :time)))))))
              (unless (equalp expected
                              (read-input "global/pg_wal_control"
                                          (1+ +wal-control-maximum-bytes+)))
                (error (if (eq control-phase :child)
                           "committed import does not have its exact child control"
                           "preserved import does not have its exact parent control"))))
            (multiple-value-bind (observations index)
                (logical-slot-copy-committed-observations owner context manifest descriptor)
              (declare (ignore index))
              (setf observations (append bound-observations observations))
              (logical-slot-copy-input-checkpoint context manifest descriptor)
              (logical-slot-copy-input-check-observations owner context observations)
              (dolist (row observations)
                (unless (equal row (logical-slot-copy-file-row context (first row)))
                  (error "committed import binding changed during verification")))
              (values certificate evidence descriptor manifest control plan status observations))))))))

(defun logical-slot-copy-read-committed-inputs (owner context proof-bytes certificate)
  "Check preserved inputs against the caller-bound original pending certificate.
This does not observe the canonical import file or grant authority. A caller
using a journal must first prove its exact certificate pair. The caller must
recheck the returned input observations across its callback."
  (logical-slot-copy-read-preserved-inputs owner context proof-bytes certificate :child))

(defun logical-slot-copy-read-committed-binding (owner context proof-bytes)
  "Read and bind the canonical pending certificate to preserved input and child control.
Include its file observation in the returned observations for the caller's
callback checks."
  (unless (logical-slot-copy-committed-owner-p owner)
    (error "committed import binding requires its sibling input owner"))
  (logical-slot-copy-check-input owner context)
  (unless proof-bytes (error "committed import lost its base receipt"))
  (multiple-value-bind (certificate bytes facts)
      (logical-slot-copy-import-read-certificate context)
    (unless (eq :pending (getf certificate :phase))
      (error "committed import binding requires a pending certificate"))
    (let ((row (list "logical-slot-copy.import" (length bytes)
                     (logical-slot-copy-import-digest bytes) facts)))
      (multiple-value-bind (bound evidence descriptor manifest control plan status observations)
          (logical-slot-copy-read-committed-inputs owner context proof-bytes certificate)
        (logical-slot-copy-input-check-observations owner context (list row))
        (unless (equal row (logical-slot-copy-file-row context "logical-slot-copy.import"))
          (error "committed import binding changed during verification"))
        (values bound evidence descriptor manifest control plan status
                (cons row observations))))))

(defun logical-slot-copy-committed-image-equal-p (left right)
  "Compare finite input image trees, including vectors and timeline entries.
String case and scalar types must match exactly."
  (cond
    ((eql left right) t)
    ((stringp left) (and (stringp right) (string= left right)))
    ((consp left)
     (and (consp right)
          (logical-slot-copy-committed-image-equal-p (car left) (car right))
          (logical-slot-copy-committed-image-equal-p (cdr left) (cdr right))))
    ((vectorp left)
     (and (vectorp right) (not (stringp right))
          (equal (array-element-type left) (array-element-type right))
          (= (length left) (length right))
          (loop for a across left for b across right
                always (logical-slot-copy-committed-image-equal-p a b))))
    ((timeline-history-entry-p left)
     (and (timeline-history-entry-p right)
          (eql (timeline-history-entry-timeline left) (timeline-history-entry-timeline right))
          (eql (timeline-history-entry-begin left) (timeline-history-entry-begin right))
          (eql (timeline-history-entry-end left) (timeline-history-entry-end right))))
    (t nil)))

(defun logical-slot-copy-committed-input-image
    (certificate evidence descriptor manifest control plan)
  "Build independent encoded and typed images for callback mutation checks."
  (list (logical-slot-copy-import-image certificate)
        (logical-slot-copy-import-evidence-encode evidence)
        (logical-slot-copy-descriptor-encode descriptor)
        (logical-slot-copy-import-state-image (list manifest control plan))))

(defun call-with-logical-slot-copy-committed-input-kind (constructor function)
  "Call FUNCTION under the sibling read owner, then recheck input images and files.
Cold mode requires unopened globals; live mode borrows the caller's store.
This scope grants no installation or admission."
  (logical-slot-copy-check-import-locks)
  (let ((mode (if *wal-ready* :live :cold))
        (state *physical-recovery-state*)
        (store *physical-receive-store*))
    (call-with-logical-slot-copy-owned-input
     (lambda (&rest arguments)
       (apply constructor
              :mode mode :state state :store store arguments))
     (lambda (owner context proof)
       (multiple-value-bind (certificate evidence descriptor manifest control plan status observations)
           (logical-slot-copy-read-committed-binding owner context proof)
         (let ((image (logical-slot-copy-committed-input-image
                       certificate evidence descriptor manifest control plan)))
           (multiple-value-prog1
               (funcall function owner context certificate evidence descriptor manifest control plan status
                        (logical-slot-copy-import-state-image observations))
             (unless (logical-slot-copy-committed-image-equal-p image (logical-slot-copy-committed-input-image
                                                                       certificate evidence descriptor manifest control plan))
               (error "committed import callback modified its input binding"))
             (logical-slot-copy-input-check-observations owner context observations)
             (dolist (row observations)
               (unless (equal row (logical-slot-copy-file-row context (first row)))
                 (error "committed import input changed during its scope")))
             (logical-slot-copy-input-sidecar-inventory context descriptor))))))))

(defun call-with-logical-slot-copy-committed-input (function)
  "Call FUNCTION under committed read ownership and recheck the input afterward.
Cold mode requires unopened globals; live mode borrows the caller's store.
This scope grants no installation or admission."
  (call-with-logical-slot-copy-committed-input-kind
   #'%make-logical-slot-copy-committed-owner
   (lambda (owner context certificate evidence descriptor manifest control plan status observations)
     (declare (ignore observations))
     (funcall function owner context certificate evidence descriptor manifest control plan status))))

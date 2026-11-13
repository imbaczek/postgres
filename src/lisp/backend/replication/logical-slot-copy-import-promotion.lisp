(in-package #:pg)

(defstruct (logical-slot-copy-import-intent-owner
            (:include logical-slot-copy-import-owner)
            (:constructor %make-logical-slot-copy-import-intent-owner)
            (:print-function physical-libpq-print-private-object))
  "Live phase 2 owner for import intent; its role requires the generation fence.")

(defmethod physical-adoption-check-owner-role ((owner logical-slot-copy-import-intent-owner))
  (call-next-method)
  (unless (and (eq :live (logical-slot-copy-import-owner-mode owner))
               (= 2 (logical-slot-copy-import-owner-phase owner))
               (sb-thread:holding-mutex-p *physical-follow-generation-lock*))
    (error "logical import intent requires live phase 2"))
  owner)

(defun logical-slot-copy-import-add-interest (policy interest)
  "Add eligible logical interest while retaining configured WAL level and full-page writes."
  (let ((result (copy-list policy)))
    (when interest (setf (getf result :promotion-logical-decoding-enabled) t))
    result))

(defun logical-slot-copy-import-intent-parent (owner context)
  "Return the checked named root and held FD for promotion I/O in this scope."
  (unless (logical-slot-copy-import-intent-owner-p owner)
    (error "logical import parent requires its intent owner"))
  (logical-slot-copy-check-input owner context)
  (cons (physical-adoption-owner-directory owner) (physical-image-context-root-fd context)))

(defun logical-slot-copy-import-intent-policy (owner context configured)
  "Recheck source eligibility and combine it with the configured promotion policy."
  (unless (logical-slot-copy-import-intent-owner-p owner)
    (error "logical import policy requires its intent owner"))
  (logical-slot-copy-check-input owner context)
  (dolist (name '("logical-slot-copy.import" "logical-slot-copy.import.tmp"))
    (when (slot-state-lstat (merge-pathnames name (physical-image-context-root-path context)))
      (error "phase 2 import intent found an unexpected import certificate")))
  (let ((source (logical-slot-copy-prepare-import-source owner context)))
    (logical-slot-copy-import-check-capacity owner context source)
    (logical-slot-copy-check-input owner context)
    (logical-slot-copy-import-add-interest configured (getf source :eligible-interest))))

(defun logical-slot-copy-import-publish-intent ()
  "Publish live phase 2 intent under the generation fence and typed owner.
Copied slots remain fenced from ordinary promotion and admission."
  (physical-promotion-check-lifecycle-context
   2 :preserve-readers *physical-promotion-preserve-readers-p*)
  (unless (sb-thread:holding-mutex-p *physical-follow-generation-lock*)
    (error "logical import intent requires the generation fence"))
  (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
    (when *physical-restartpoint-worker*
      (error "logical import intent requires a joined restartpoint worker"))
    (sb-thread:with-mutex (*timeline-history-lock*)
      (call-with-physical-promotion-intent-publication
       (lambda (publication)
         (call-with-logical-slot-copy-import-input-kind
          #'%make-logical-slot-copy-import-intent-owner
          (lambda (owner context)
            (let ((*pgdata* (physical-image-context-root-path context))
                  (*physical-image-context* nil))
              (multiple-value-prog1
                  (physical-promotion-publish-intent-core publication nil owner context)
                (logical-slot-copy-verification-test-point :after-import-intent-control context))))))))))

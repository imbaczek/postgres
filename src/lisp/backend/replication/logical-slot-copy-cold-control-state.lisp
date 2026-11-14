(in-package #:pg)

;; Mutable observations for one attempt.  The binding is checked around
;; operations as directory entries, catalog, WAL, and control move through
;; their expected phases.  These fields do not install runtime server state.
(defstruct (logical-slot-copy-cold-control-runtime
            (:constructor %make-logical-slot-copy-cold-control-runtime)
            (:print-function physical-libpq-print-private-object))
  global directory-facts children phase catalog-bytes catalog-facts control-bytes control-facts
  temporary-kind temporary-bytes temporary-facts
  source-proof (source-active t) wal-checker wal-snapshot child-complete
  data-synced old-synced temp-synced file-synced directory-synced)

(declaim (ftype function logical-slot-copy-cold-control-check-data))

(defun logical-slot-copy-cold-control-runtime-record (attempt runtime)
  (logical-slot-copy-cold-control-record attempt)
  (funcall (logical-slot-copy-cold-control-attempt-operations attempt) :check-runtime attempt runtime)
  runtime)

(defun logical-slot-copy-cold-control-check-inputs (attempt)
  (let ((owner (logical-slot-copy-cold-control-attempt-owner attempt))
        (context (logical-slot-copy-cold-control-attempt-context attempt))
        (binding (logical-slot-copy-cold-control-attempt-binding attempt)))
    (logical-slot-copy-check-input owner context)
    (logical-slot-copy-import-control-fences context)
    (when (getf binding :data-observations) (logical-slot-copy-cold-control-check-data attempt))
    (logical-slot-copy-input-sidecar-inventory context (getf binding :descriptor))
    (unless (and (eql (getf binding :restartpoints)
                      (cdr (assoc "pg_physical_restartpoints" (physical-image-context-children context)
                                  :test #'equal)))
                 (equal (getf binding :restartpoint-directory-facts)
                        (physical-image-file-facts (getf binding :restartpoints)))
                 (equal (getf binding :restartpoint-directory-facts)
                        (physical-image-file-facts (physical-image-context-root-fd context)
                                                   "pg_physical_restartpoints")))
      (error "cold publication restartpoint directory changed"))
    (dolist (row (append (getf binding :inputs) (getf binding :restartpoint-observations)))
      (logical-slot-copy-input-check-observations owner context (list row))
      (unless (equal row (logical-slot-copy-file-row context (first row) (second row)))
        (error "cold publication immutable input changed"))))
  t)

(defun logical-slot-copy-cold-control-temporary-name (runtime)
  (case (logical-slot-copy-cold-control-runtime-temporary-kind runtime)
    (:catalog "pg_catalog.tmp")
    ((:control :writing :ready) "pg_wal_control.tmp")
    ((:absent :installed) nil)
    (otherwise (error "invalid cold control temporary phase"))))

(defun logical-slot-copy-cold-control-check-global (attempt runtime)
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (let* ((context (logical-slot-copy-cold-control-attempt-context attempt))
         (global (logical-slot-copy-cold-control-runtime-global runtime))
         (temporary (logical-slot-copy-cold-control-temporary-name runtime))
         (children (logical-slot-copy-cold-control-runtime-children runtime))
         (names (sort (append (mapcar #'first children) (when temporary (list temporary))) #'string<)))
    (unless (and (eql global (cdr (assoc "global" (physical-image-context-children context) :test #'equal)))
                 (equal (logical-slot-copy-cold-control-runtime-directory-facts runtime)
                        (physical-image-file-facts global))
                 (equal (physical-image-file-facts global)
                        (physical-image-file-facts (physical-image-context-root-fd context) "global"))
                 (equal names (logical-slot-copy-import-control-names global (1+ (length names)))))
      (error "cold publication global directory or namespace changed"))
    (dolist (row children)
      (let ((facts (cond ((equal (first row) "pg_catalog.dat")
                          (logical-slot-copy-cold-control-runtime-catalog-facts runtime))
                         ((equal (first row) "pg_wal_control")
                          (logical-slot-copy-cold-control-runtime-control-facts runtime))
                         (t (second row)))))
        (unless (equal facts (physical-image-file-facts global (first row)))
          (error "cold publication protected global inode changed"))))
    (unless (equalp (logical-slot-copy-cold-control-runtime-control-bytes runtime)
                    (logical-slot-copy-cold-wal-read
                     global "pg_wal_control" (logical-slot-copy-cold-control-runtime-control-facts runtime)
                     (length (logical-slot-copy-cold-control-runtime-control-bytes runtime))))
      (error "cold publication canonical control bytes changed"))
    (when temporary
      (unless (and (equal (logical-slot-copy-cold-control-runtime-temporary-facts runtime)
                          (physical-image-file-facts global temporary))
                   (equalp (logical-slot-copy-cold-control-runtime-temporary-bytes runtime)
                           (logical-slot-copy-cold-wal-read
                            global temporary (logical-slot-copy-cold-control-runtime-temporary-facts runtime)
                            (length (logical-slot-copy-cold-control-runtime-temporary-bytes runtime)))))
        (error "cold publication temporary binding changed"))))
  t)

(defun logical-slot-copy-cold-control-advance-directory (attempt runtime)
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (let ((before (logical-slot-copy-cold-control-runtime-directory-facts runtime))
        (after (physical-image-file-facts (logical-slot-copy-cold-control-runtime-global runtime))))
    (unless (logical-slot-copy-cold-facts-match-p before after '(5 6 7))
      (error "cold publication global directory identity changed"))
    (setf (logical-slot-copy-cold-control-runtime-directory-facts runtime) after))
  (logical-slot-copy-cold-control-check-global attempt runtime))

(defun logical-slot-copy-cold-control-check-metadata (attempt runtime)
  (logical-slot-copy-cold-control-runtime-record attempt runtime)
  (logical-slot-copy-cold-control-check-inputs attempt)
  (logical-slot-copy-cold-control-check-global attempt runtime))

;; Recheck the retained source and current artifacts while the source callback
;; is active.  The runtime phase determines which publication evidence the
;; artifact check must establish.
(defun logical-slot-copy-cold-control-check-source (attempt runtime store artifacts &optional (emit-hook t))
  (logical-slot-copy-cold-control-check-metadata attempt runtime)
  (unless (logical-slot-copy-cold-control-runtime-source-active runtime)
    (error "cold publication source checker expired"))
  (when emit-hook
    (logical-slot-copy-verification-test-point
     :before-cold-control-source (logical-slot-copy-cold-control-attempt-context attempt))
    (logical-slot-copy-cold-control-check-metadata attempt runtime))
  (let* ((b (logical-slot-copy-cold-control-attempt-binding attempt))
         (actual (let ((*pgdata* (physical-image-context-root-path
                                  (logical-slot-copy-cold-control-attempt-context attempt)))
                       (*physical-image-context* nil))
                   (multiple-value-list
                    (logical-slot-copy-check-retained-source
                     (getf b :certificate) (getf b :evidence) (getf b :descriptor)
                     (getf b :manifest) (getf b :control) (getf b :plan) (getf b :state)
                     (getf b :metadata) (getf b :prefix) (getf b :generation) store artifacts))))
         (stable (subseq actual 0 3)))
    (if (logical-slot-copy-cold-control-runtime-source-proof runtime)
        (unless (logical-slot-copy-committed-image-equal-p
                 (logical-slot-copy-cold-control-runtime-source-proof runtime) stable)
          (error "cold publication retained source changed"))
        (setf (logical-slot-copy-cold-control-runtime-source-proof runtime)
              (logical-slot-copy-import-state-image stable))))
  (logical-slot-copy-cold-control-check-metadata attempt runtime))

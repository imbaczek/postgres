(in-package #:pg)

;; Define these layouts before predicates and callers compile.

(defstruct (logical-slot-startup-entry (:constructor %make-logical-slot-startup-entry))
  ;; Keep both the decoded candidate and its disk image for later rechecks.
  (slot nil :read-only t) (bytes nil :read-only t) (image nil :read-only t)
  (profile nil :read-only t) (provenance nil :read-only t))

(defstruct (logical-slot-startup-inventory
            (:constructor %make-logical-slot-startup-inventory)
            (:print-function physical-libpq-print-private-object))
  (state nil :read-only t))

(defstruct (logical-slot-startup-source (:constructor %make-logical-slot-startup-source))
  ;; Capture post-replay facts before catalog initialization: durable
  ;; catalog system ID, control, history, durable end and slot candidates.
  ;; The gate and postmaster ownership are separate, borrowed authority.
  (gate-owner nil :read-only t) (ownership nil :read-only t)
  (entries nil :read-only t) (system-id nil :read-only t)
  (inventory nil :read-only t)
  (root nil :read-only t) (history nil :read-only t) (history-bytes nil :read-only t)
  (control nil :read-only t) (timeline nil :read-only t) (namespace nil :read-only t)
  (end nil :read-only t) (incarnation nil :read-only t) (wal-level nil :read-only t)
  (registry nil :read-only t) (maximum nil :read-only t) (policy nil :read-only t)
  (thread nil :read-only t) (mutex nil :read-only t) active-p)

(defstruct (logical-slot-imported-startup-capture
            (:constructor %make-logical-slot-imported-startup-capture)
            (:print-object
             (lambda (object stream)
               (print-unreadable-object (object stream :type t :identity t)))))
  ;; One capture binds one exact source and its private principal.
  (state nil :read-only t))

(defstruct (logical-slot-imported-startup-source
            (:include logical-slot-startup-source)
            (:constructor %make-logical-slot-imported-startup-source))
  (capture nil :read-only t))

(defstruct (logical-slot-imported-startup-principal
            (:constructor %make-logical-slot-imported-startup-principal)
            (:print-object
             (lambda (object stream)
               (print-unreadable-object (object stream :type t :identity t)))))
  ;; State closes over the independent snapshot and revocation status.
  (source nil :read-only t) (state nil :read-only t))

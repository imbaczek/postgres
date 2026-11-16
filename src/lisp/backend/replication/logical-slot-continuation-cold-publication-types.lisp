(in-package #:pg)

(defvar *logical-slot-continuation-protections* (make-hash-table :test #'equal :synchronized t))

(defvar *logical-slot-continuation-protection-registries* (make-hash-table :test #'eq :synchronized t))

(defvar *logical-slot-continuation-protection-identities*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defvar *logical-slot-continuation-cold-reservations* (make-hash-table :test #'equal :synchronized t))

(defvar *logical-slot-continuation-cold-registries* (make-hash-table :test #'eq :synchronized t))

(defvar *logical-slot-continuation-cold-publication-test-hook* nil)

(defstruct (logical-slot-continuation-cold-completion
            (:constructor %make-logical-slot-continuation-cold-completion)
            (:print-function physical-libpq-print-private-object))
  (state nil :read-only t))

(defstruct (logical-slot-continuation-cold-commit
            (:constructor %make-logical-slot-continuation-cold-commit)
            (:print-function physical-libpq-print-private-object))
  (state nil :read-only t))

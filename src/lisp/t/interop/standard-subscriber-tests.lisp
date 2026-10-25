(in-package #:pg-interop-tests)

(deftest standard-subscriber-explicit-mode
    (let ((options (pg::parse-logical-subscription-args
                    '("--mode" "standard" "--publication" "pub" "--slot" "subscriber" "--table" "rows"))))
      (setf (pg::logical-subscription-options-actual-slot options) "pl_00000000000000000000000000000000")
      (ok (eq :standard (pg::logical-subscription-options-mode options)))
      (ok (equal "pub" (pg::logical-subscription-options-publication options)))
      (ok (equal "START_REPLICATION SLOT pl_00000000000000000000000000000000 LOGICAL 0/00000010 (proto_version '1', publication_names 'pub', binary 'false', streaming 'off', messages 'false', origin 'any')"
                 (pg::logical-standard-start-command options 16))))
  (let ((options (pg::parse-logical-subscription-args '("--slot" "subscriber" "--table" "rows"))))
    (ok (eq :extension (pg::logical-subscription-options-mode options))))
  (dolist (args '(("--mode" "standard" "--slot" "subscriber" "--table" "rows")
                  ("--publication" "pub" "--slot" "subscriber" "--table" "rows")
                  ("--mode" "guess" "--slot" "subscriber" "--table" "rows")
                  ("--mode" "standard" "--publication" "pub,other" "--slot" "subscriber" "--table" "rows")))
    (ok (handler-case (progn (pg::parse-logical-subscription-args args) nil)
          (error () t)))))

(deftest standard-subscriber-rejects-private-progress
    (let* ((options (pg::make-logical-subscription-options :mode :standard))
           (runtime (pg::make-logical-subscription-runtime :options options)))
      (ok (handler-case
              (progn (pg::logical-pgoutput-dispatch runtime #(77)) nil)
            (error (condition)
              (search "does not accept extension Message" (princ-to-string condition)))))))

(deftest standard-ready-copy-cannot-be-reseeded
    (dolist (state '(nil (:state "ready" :actual-slot "pl_00000000000000000000000000000000")))
      (ok (handler-case (progn (pg::logical-standard-resume-copy nil nil state) nil)
            (error (condition)
              (search "only an owned incomplete standard copy" (princ-to-string condition)))))))

(deftest standard-slot-resume-respects-target-durability
    (let ((row (vector "pgoutput" "logical" "t" "f" "f" "0/10" "0/8" nil "f" "f")))
      (ok (pg::logical-standard-resume-slot-safe-p row 16))
      (ok (not (pg::logical-standard-resume-slot-safe-p row 15)))
      (dolist (change '((0 "pg_lisp") (1 "physical") (2 "f") (3 "t") (4 "t")
                        (5 nil) (6 nil) (6 "0/11") (7 "wal_removed") (8 "t") (9 "t")))
        (let ((changed (copy-seq row)))
          (setf (aref changed (first change)) (second change))
          (ok (not (pg::logical-standard-resume-slot-safe-p changed 16)))))))

(deftest standard-attempt-names-and-identity-keys
    (let ((name (pg::logical-standard-new-attempt-slot)))
      (ok (pg::logical-standard-attempt-slot-p name))
      (ok (not (pg::logical-standard-attempt-slot-p "interop_sub"))))
  (ok (equal '(2 4) (pg::logical-standard-key-attnums "4 2 1 3" 2)))
  (ok (equal '(1) (pg::logical-standard-key-attnums "1 3" 1)))
  (ok (handler-case (progn (pg::logical-standard-key-attnums "1" 2) nil)
        (error () t))))

(deftest standard-subscription-session-owner
    (let ((pg::*logical-subscription-owners* (make-hash-table :test #'equal))
          (one (pg::make-backend :database "postgres"))
          (two (pg::make-backend :database "postgres")))
      (let ((pg::*backend* one))
        (ok (pg::logical-acquire-subscription-owner "subscriber"))
        (ok (pg::logical-acquire-subscription-owner "subscriber")))
      (let ((pg::*backend* two))
        (ok (handler-case (progn (pg::logical-acquire-subscription-owner "subscriber") nil)
              (pg::pg-error (condition) (equal "55006" (pg::pg-error-sqlstate condition))))))
      (pg::logical-release-subscription-owner one)
      (let ((pg::*backend* two)) (ok (pg::logical-acquire-subscription-owner "subscriber")))
      (pg::logical-release-subscription-owner two)
      (ok (zerop (hash-table-count pg::*logical-subscription-owners*)))))

(deftest standard-state-rejects-config-and-owner-drift
    (let* ((options (pg::make-logical-subscription-options :mode :standard :slot-name "subscriber"
                                                           :publication "pub" :tables '("rows") :source-identity "cluster-db"))
           (state (list :config (pg::logical-standard-config-values options)
                        :actual-slot "pl_00000000000000000000000000000000" :state "copying")))
      (ok (null (pg::logical-standard-validate-state options state)))
      (dolist (index '(0 1 2 3 4 5 6))
        (let ((changed (copy-tree state)))
          (setf (nth index (getf changed :config)) "changed")
          (ok (handler-case (progn (pg::logical-standard-validate-state options changed) nil)
                (error () t)))))
      (setf (getf state :actual-slot) "unowned_slot")
      (ok (handler-case (progn (pg::logical-standard-validate-state options state) nil)
            (error () t)))))

(deftest standard-publication-rejects-unsupported-effective-relations
    (let ((row (vector "public" "rows" "r" "d" "1 2 3" "t" "3" "f")))
      (ok (pg::logical-standard-publication-row-supported-p row))
      ;; Namespace, partition root, nondefault identity, column list, filter,
      ;; incomplete projection, and a leaf expanded from a partitioned root.
      (dolist (change '((0 "other") (2 "p") (3 "f") (4 "1 2") (5 "f") (6 "4") (7 "t")))
        (let ((changed (copy-seq row)))
          (setf (aref changed (first change)) (second change))
          (ok (not (pg::logical-standard-publication-row-supported-p changed)))))))

(deftest standard-relation-rejects-identity-and-flag-drift
    ;; Independent Relation layout from C logicalrep_write_rel/logicalrep_write_attrs.
    (let ((payload (octets '(82 0 0 0 1 112 117 98 108 105 99 0
                             114 111 119 115 0 100 0 1 1 105 100 0
                             0 0 0 23 255 255 255 255))))
      (ok (equal "rows" (pg::pgoutput-relation-name
                         (pg::logical-pgoutput-read-relation payload t))))
      (dolist (identity '(102 105 110 120))
        (let* ((changed (copy-seq payload))
               (runtime (pg::make-logical-subscription-runtime
                         :options (pg::make-logical-subscription-options :mode :standard))))
          (setf (aref changed 17) identity)
          (ok (handler-case (progn (pg::logical-pgoutput-dispatch runtime changed) nil)
                (error (condition)
                  (search "default replica identity" (princ-to-string condition)))))))
      (dolist (flags '(2 3 128 255))
        (let ((changed (copy-seq payload)))
          (setf (aref changed 20) flags)
          (ok (handler-case (progn (pg::logical-pgoutput-read-relation changed t) nil)
                (error (condition)
                  (search "Relation column flags" (princ-to-string condition)))))))))

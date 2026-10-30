(in-package #:pg-physical-tests)

(defun follow-anchor-request (seed overlap)
  (list :version 1 :system-id 42 :base-timeline 1 :base 64 :manifest-digest 77
        :source-timeline 2 :receive-timeline 1 :receive-lsn 96 :receive-digest seed
        :overlap-start 0 :overlap-digest overlap
        :history-text (format nil "1~c0/00000060~cpromotion~%" #\Tab #\Tab)
        :eor-octets (coerce (pg::wal-encode-end-of-recovery-record 96 42 2 1 1) 'list)))

(defun same-timeline-follow-anchor (cursor carried overlap &optional (timeline 1))
  (list :version 1 :kind :same-timeline :system-id 42 :base-timeline 1 :base 0
        :manifest-digest 77 :source-timeline timeline :receive-timeline timeline
        :receive-lsn cursor :receive-digest carried
        :overlap-start (* (floor (1- cursor) pg::+wal-segment-size+) pg::+wal-segment-size+)
        :overlap-digest overlap
        :history-text (if (= timeline 1) "" (format nil "1~c0/00000040~cpromotion~%" #\Tab #\Tab))))

(defun retained-timeline-follow-anchor (cursor fork &optional (carried 123) (overlap 456))
  (list :version 1 :kind :retained-timeline :system-id 42 :base-timeline 1 :base 0
        :manifest-digest 77 :source-timeline 2 :receive-timeline 1
        :receive-lsn cursor :receive-digest carried
        :overlap-start (* (floor (1- cursor) pg::+wal-segment-size+) pg::+wal-segment-size+)
        :overlap-digest overlap
        :history-text (format nil "1~c~a~cretained parent~%" #\Tab (pg::wal-format-lsn fork) #\Tab)))

(deftest retained-timeline-anchor-proves-only-a-finite-parent-prefix
    (let ((anchor (retained-timeline-follow-anchor 64 96)))
      (multiple-value-bind (history fork eor) (pg::physical-follow-anchor-check anchor)
        (ok (= 2 (pg::timeline-history-entry-timeline (first history))))
        (ok (= 96 fork))
        (ok (null eor)))
      (ok (equal anchor (pg::physical-follow-anchor-copy anchor)))
      (ok (not (member :eor-octets anchor))))
  (dolist (change '((:receive-lsn 97) (:receive-lsn 0) (:receive-timeline 2)
                    (:source-timeline 1) (:base 65) (:base-timeline 2)
                    (:kind :unknown) (:eor-octets nil)))
    (let ((anchor (retained-timeline-follow-anchor 64 96)))
      (setf (getf anchor (first change)) (second change))
      (ok (signals (pg::physical-follow-anchor-check anchor) 'error)
          (format nil "~s" change)))))

(deftest retained-timeline-request-identity-cannot-match-a-descendant-request
    (let* ((anchor (retained-timeline-follow-anchor 64 96))
           (request (pg::physical-follow-anchor-request anchor))
           (legacy (copy-list request)))
      (remf legacy :kind)
      (ok (eq :retained-timeline (pg::physical-follow-request-kind request)))
      (ok (eq :descendant (pg::physical-follow-request-kind legacy)))
      (ok (equal request (pg::physical-follow-request-copy request)))
      (ok (pg::physical-follow-request-matches-anchor-p request anchor))
      (ok (not (pg::physical-follow-request-matches-anchor-p legacy anchor)))
      (let ((reordered (loop for (key value) on request by #'cddr
                             nconc (list (cons key value)))))
        (ok (equal request
                   (pg::physical-follow-request-copy
                    (loop for (key . value) in (reverse reordered) append (list key value))))))
      (dolist (kind '(nil :unknown :descendant :same-timeline))
        (setf (getf request :kind) kind)
        (ok (signals (pg::physical-follow-request-copy request) 'error))))
  (let* ((anchor (follow-anchor-request 123 456))
         (request (pg::physical-follow-anchor-request anchor)))
    (setf (getf request :kind) :retained-timeline)
    (ok (not (pg::physical-follow-request-matches-anchor-p request anchor))))
  (dolist (anchor (list (follow-anchor-request 123 456)
                        (same-timeline-follow-anchor 96 123 456)))
    (let ((request (pg::physical-follow-anchor-request anchor)))
      (ok (not (member :kind request)))
      (ok (pg::physical-follow-request-matches-anchor-p request anchor)))))

(cl-spec:defproperty retained-timeline-pin-roundtrips-bounded-confirmation
    ((fork (range integer 1 18446744073709551615))
     (carried (range integer 0 18446744073709551615))
     (overlap (range integer 0 18446744073709551615))
     (ready boolean))
  (:about pg::physical-follow-pin-decode)
  (:trials (:normal 200))
  (let* ((cursor (max 1 (floor fork 2)))
         (anchor (retained-timeline-follow-anchor cursor fork carried overlap))
         (pin (pg::make-physical-follow-pin
               :name "retained_parent" :system-id 42 :timeline 2 :manifest-digest 77
               :consistent-lsn cursor :restart-lsn (if ready fork (getf anchor :overlap-start))
               :confirmed-lsn (when ready fork)
               :prefix-digest (if (and ready (> fork cursor)) (logxor carried 1) carried)
               :stage (if ready :ready :retaining) :anchor anchor))
         (restored (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
    (and (equal anchor (pg::physical-follow-pin-anchor restored))
         (= (pg::physical-follow-anchor-digest anchor)
            (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor restored)))
         (eql (pg::physical-source-pin-confirmed-lsn pin)
              (pg::physical-source-pin-confirmed-lsn restored)))))

(deftest generated-retained-timeline-pin-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'retained-timeline-pin-roundtrips-bounded-confirmation :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest retained-timeline-pin-rejects-confirmation-beyond-its-fork
    (let* ((anchor (retained-timeline-follow-anchor 64 96))
           (pin (pg::make-physical-follow-pin
                 :name "retained_parent" :system-id 42 :timeline 2 :manifest-digest 77
                 :consistent-lsn 64 :restart-lsn 96 :confirmed-lsn 96
                 :prefix-digest 999 :stage :ready :anchor anchor)))
      (ok (pg::physical-follow-pin-check-fields pin))
      (setf (pg::physical-source-pin-confirmed-lsn pin) 97
            (pg::physical-source-pin-restart-lsn pin) 97)
      (ok (signals (pg::physical-follow-pin-check-fields pin) 'error))))

(deftest same-timeline-anchor-keeps-history-without-a-fork-or-eor
    (dolist (timeline '(1 2))
      (let ((anchor (same-timeline-follow-anchor 96 123 456 timeline)))
        (multiple-value-bind (history fork eor) (pg::physical-follow-anchor-check anchor)
          (ok (= timeline (pg::timeline-history-entry-timeline (first history))))
          (ok (null fork))
          (ok (null eor)))
        (ok (equal anchor (pg::physical-follow-anchor-copy anchor)))
        (ok (not (member :eor-octets anchor))))))

(deftest same-timeline-anchor-copy-owns-its-history-bytes
    (let* ((anchor (same-timeline-follow-anchor 96 123 456 2))
           (saved (copy-seq (getf anchor :history-text)))
           (copy (pg::physical-follow-anchor-copy anchor)))
      (setf (char (getf anchor :history-text) 0) #\9)
      (ok (equal saved (getf copy :history-text)))
      (setf (char (getf copy :history-text) 0) #\8)
      (ok (char= #\9 (char (getf anchor :history-text) 0)))))

(deftest same-timeline-anchor-rejects-foreign-coordinates-and-descendant-fields
    (dolist (change '((:kind :unknown) (:kind nil) (:source-timeline 2) (:base 97)
                      (:base-timeline 2) (:receive-timeline 2) (:receive-lsn 0) (:overlap-start 96)
                      (:history-text "# ignored") (:eor-octets nil)))
      (let ((anchor (same-timeline-follow-anchor 96 123 456)))
        (setf (getf anchor (first change)) (second change))
        (ok (signals (pg::physical-follow-anchor-check anchor) 'error)
            (format nil "~s" change))))
  (let ((anchor (same-timeline-follow-anchor 96 123 456)))
    (remf anchor :kind)
    (ok (signals (pg::physical-follow-anchor-check anchor) 'error))))

(deftest same-timeline-proof-extension-preserves-descendant-anchor-identity
    ;; Independent native-frame CRC32 and canonical UTF-8/FNV-1a fixtures.
    ;; Retain the stored v7 identity while the current encoder emits v8 bytes.
    (let ((legacy (follow-anchor-request 123 456)))
      (setf (getf legacy :eor-octets)
            '(80 71 87 76 7 0 13 0 56 0 0 0 96 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
              18 39 251 14 42 0 0 0 0 0 0 0 2 0 0 0 1 0 0 0 1 0 0 0 0 0 0 0))
      (ok (= 15144333420950785491 (pg::physical-follow-anchor-digest legacy))))
  (ok (= 16393646900456083562
         (pg::physical-follow-anchor-digest (follow-anchor-request 123 456))))
  (ok (= 15813853048420144424
         (pg::physical-follow-anchor-digest (same-timeline-follow-anchor 96 123 456)))))

(cl-spec:defproperty same-timeline-follow-pin-roundtrips-full-width-proof
    ((cursor (range integer 1 18446744073709551615))
     (carried (range integer 0 18446744073709551615))
     (overlap (range integer 0 18446744073709551615))
     (ready boolean))
  (:about pg::physical-follow-pin-decode)
  (:trials (:normal 200))
  (let* ((anchor (same-timeline-follow-anchor cursor carried overlap))
         (pin (pg::make-physical-follow-pin
               :name "same_timeline" :system-id 42 :timeline 1 :manifest-digest 77
               :consistent-lsn cursor :restart-lsn (if ready cursor (getf anchor :overlap-start))
               :confirmed-lsn (when ready cursor) :prefix-digest carried
               :stage (if ready :ready :retaining) :anchor anchor))
         (restored (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
    (and (equal anchor (pg::physical-follow-pin-anchor restored))
         (eql (pg::physical-follow-pin-stage pin) (pg::physical-follow-pin-stage restored))
         (= (pg::physical-follow-anchor-digest anchor)
            (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor restored))))))

(deftest generated-same-timeline-follow-pin-proof-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'same-timeline-follow-pin-roundtrips-full-width-proof :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(defun follow-anchor-pin (seed overlap &optional ready)
  (pg::make-physical-follow-pin
   :name "sibling" :system-id 42 :timeline 2 :manifest-digest 77
   :consistent-lsn 96 :restart-lsn (if ready 96 0)
   :confirmed-lsn (when ready 96) :prefix-digest seed
   :stage (if ready :ready :retaining) :anchor (follow-anchor-request seed overlap)))

(defun follow-binding-test-config (&optional padding)
  (let* ((anchor (follow-anchor-request 123 456))
         (history (getf anchor :history-text))
         (before (list 42 1 64 128 555 64 #xCBF29CE484222325 nil nil 77 nil))
         (store (pg::%make-physical-store :system-id 42 :timeline 1 :base-lsn 64)))
    (when padding
      (setf history (with-output-to-string (out)
                      (write-string history out)
                      (dotimes (index (ceiling padding 10))
                        (declare (ignore index))
                        (write-line "# padding" out)))
            (getf anchor :history-text) history))
    (let* ((selection (pg::physical-store-history-candidate
                       store 2 history (getf anchor :eor-octets) 123))
           (after (copy-list before)))
      (setf (fourth after) 96 (fifth after) 123
            (eighth after) 2 (ninth after) (getf selection :digest))
      (list 1 "127.0.0.1" 5433 "binding_probe"
            (list :version 1 :phase :pending :anchor anchor :child-history history
                  :before before :after after :applied '(96 1) :required '(96 1)
                  :recovery (list :phase 2 :cluster-identity "binding-probe" :checkpoint 64
                                  :observed-next-xid 9 :history-target nil :history-digest nil
                                  :restartpoint nil))))))

(defun same-timeline-follow-binding-config ()
  (let* ((config (follow-binding-test-config))
         (binding (fifth config)) (before (getf binding :before))
         (anchor (same-timeline-follow-anchor (fourth before) (fifth before) 456)))
    (setf (getf anchor :base) (third before)
          (getf binding :anchor) anchor
          (getf binding :child-history) nil
          (getf binding :after) (copy-list before))
    config))

(deftest same-timeline-binding-preserves-every-receive-control-field
    (let* ((config (same-timeline-follow-binding-config))
           (binding (fifth config))
           (runtime (pg::make-physical-follow-runtime :config config)))
      (ok (eq config (pg::physical-follow-check-config config)))
      (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
      (ok (equal (getf binding :before) (getf binding :after)))
      (multiple-value-bind (limit next) (pg::physical-follow-runtime-boundary runtime)
        (ok (null limit))
        (ok (null next))))
  (dotimes (index 11)
    (let* ((config (same-timeline-follow-binding-config))
           (after (getf (fifth config) :after)))
      (setf (nth index after) (if (integerp (nth index after)) (1+ (nth index after)) 1))
      (ok (signals (pg::physical-follow-check-config config) 'error)
          (format nil "changed receive field ~d" index))))
  (let ((config (same-timeline-follow-binding-config)))
    (setf (getf (fifth config) :child-history) "")
    (ok (signals (pg::physical-follow-check-config config) 'error))))

(deftest follow-binding-config-roundtrips-pending-proof-with-existing-envelope
    (dolist (padding '(nil 8192))
      (let* ((config (follow-binding-test-config padding))
             (bytes (pg::physical-follow-config-image config)))
        (ok (equal config (pg::physical-follow-decode-config bytes)))
        (ok (search "PGPHYSICALFOLLOW1 " (pg::bytes->string bytes)))
        (when padding (ok (> (length bytes) 4096))))))

(cl-spec:defproperty follow-binding-envelope-preserves-full-width-proof-and-root
    ((carried (range integer 0 18446744073709551615))
     (overlap (range integer 0 18446744073709551615))
     (root (range integer 0 18446744073709551615))
     (active (range integer 0 1)))
  (:about pg::physical-follow-decode-config)
  (:trials (:normal 200))
  (let* ((config (follow-binding-test-config)) (binding (fifth config))
                                               (anchor (getf binding :anchor)) (before (getf binding :before))
                                               (after (getf binding :after))
                                               (store (pg::%make-physical-store :system-id 42 :timeline 1 :base-lsn 64)))
    (setf (getf binding :phase) (if (zerop active) :pending :active)
          (getf anchor :receive-digest) carried (getf anchor :overlap-digest) overlap
          (nth 10 before) root (nth 10 after) root (fifth after) carried
          (ninth after) (getf (pg::physical-store-history-candidate
                               store 2 (getf binding :child-history) (getf anchor :eor-octets) carried)
                              :digest))
    (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config)))))

(deftest generated-follow-binding-full-width-proof-and-root
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-binding-envelope-preserves-full-width-proof-and-root :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest follow-binding-config-rejects-null-or-incomplete-fifth-field
    (dolist (binding '(nil (:version 1) (:phase :pending)))
      (ok (signals (pg::physical-follow-check-config
                    (list 1 "127.0.0.1" 5433 "binding_probe" binding)) 'error))))

(deftest follow-binding-config-preserves-local-proof-and-transition-fields
    (dolist (kind '(:phase :identity :seed :floor :root :selection :eor :history :required :applied :recovery))
      (let* ((config (follow-binding-test-config))
             (binding (fifth config)))
        (ecase kind
          (:phase (setf (getf binding :phase) :unknown))
          (:identity (incf (first (getf binding :before))))
          (:seed (incf (fifth (getf binding :after))))
          (:floor (incf (sixth (getf binding :after))))
          (:root (setf (nth 10 (getf binding :after)) 0))
          (:selection (incf (ninth (getf binding :after))))
          (:eor (setf (getf (getf binding :anchor) :eor-octets) '(0)))
          (:history (setf (getf binding :child-history) (store-history-text 1 95)))
          (:required (setf (getf binding :required) '(128 1)))
          (:applied (setf (getf binding :applied) '(96 2)))
          (:recovery (setf (getf binding :recovery) '(:phase 2 :cluster-identity "binding-probe" :checkpoint 64
                                                      :observed-next-xid 9 :history-target 2 :history-digest nil
                                                      :restartpoint nil))))
        (ok (signals (pg::physical-follow-check-config config) 'error)))))

(deftest follow-binding-config-is-preserved-by-stop-start-and-cold-storage-guards
    (dolist (phase '(:pending :active))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (pg::*physical-recovery-state* (list :phase 2))
               (pg::*physical-recovery-failed-p* nil)
               (pg::*physical-follow-runtime* nil)
               (config (follow-binding-test-config)))
          (setf (getf (fifth config) :phase) phase)
          (pg::physical-follow-save-config config)
          (let ((before (lineage-test-bytes (pg::physical-follow-config-path))))
            (ok (signals (pg::physical-follow-stop) 'error))
            (ok (equalp before (lineage-test-bytes (pg::physical-follow-config-path))))
            (ok (signals (pg::physical-follow-start "127.0.0.1" 5434 "replacement") 'error))
            (ok (equalp before (lineage-test-bytes (pg::physical-follow-config-path))))
            (ok (signals (pg::physical-recovery-open-storage) 'error))
            (ok (equalp before (lineage-test-bytes (pg::physical-follow-config-path))))
            (ok (null pg::*physical-follow-runtime*)))))))

(deftest follow-binding-config-supports-parent-catchup-and-zero-root-witnesses
    (let* ((config (follow-binding-test-config)) (binding (fifth config))
                                                 (anchor (getf binding :anchor)) (before (getf binding :before)))
      (setf (getf anchor :receive-lsn) 64 (getf anchor :receive-digest) #xCBF29CE484222325
            (fourth before) 64 (fifth before) #xCBF29CE484222325
            (getf binding :after) (copy-list before)
            (getf binding :applied) '(64 1) (getf binding :required) '(64 1))
      (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config)))))
  (let* ((config (follow-binding-test-config)) (binding (fifth config))
                                               (before (getf binding :before)) (after (getf binding :after)))
    (setf (sixth before) 80 (seventh before) 999 (nth 10 before) 0
          (sixth after) 80 (seventh after) 999 (nth 10 after) 0
          (getf binding :required) '(64 1)
          (getf binding :recovery) '(:phase 2 :cluster-identity "binding-probe" :checkpoint 64
                                     :observed-next-xid 9 :history-target nil :history-digest nil
                                     :restartpoint (80 4242)))
    (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))))

(deftest follow-binding-witness-preserves-exhausted-next-xid-sentinel
    (let* ((config (follow-binding-test-config))
           (witness (getf (fifth config) :recovery)))
      (setf (getf witness :observed-next-xid) 18446744073709551616)
      (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))))

(defun call-with-follow-replay-proof (function &key unfinished)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root)
           (identity '(:system-id 42 :base-timeline 1 :base 64 :manifest-digest 77))
           (store (pg::physical-store-open root 42 1 :create t :start-lsn 64 :retained-identity identity))
           (cursor 64) (first-terminal nil)
           (base (list :system-id 42 :timeline 1 :end 64 :manifest-digest 77 :checkpoint 64
                       :cluster-identity "history-source" :next-xid 3 :source-checkpoint nil
                       :source-reservation (list :version 1 :through 64 :bound 1000 :complete nil))))
      (unwind-protect
           (progn
             (dolist (xid '(10 20))
               (let ((raw (pg::wal-encode-record pg::+wal-record-batch-start+ 0 cursor (empty-octets))))
                 (pg::physical-store-input store cursor raw) (incf cursor (length raw)))
               (let ((raw (pg::wal-encode-record pg::+wal-record-commit+ xid cursor
                                                 (pg::wal-encode-commit-timestamp 42))))
                 (pg::physical-store-input store cursor raw) (incf cursor (length raw)))
               (unless first-terminal (setf first-terminal cursor)))
             (when unfinished
               (let ((raw (pg::wal-encode-record pg::+wal-record-batch-start+ 0 cursor (empty-octets))))
                 (pg::physical-store-input store cursor raw) (incf cursor (length raw))))
             (pg::physical-store-flush store)
             (let* ((anchor (follow-anchor-request (pg::physical-store-prefix-digest store cursor) 456))
                    (history (store-history-text 1 cursor))
                    (eor (store-history-eor cursor 2 1))
                    (before (pg::physical-store-control-tuple store))
                    (applied (list first-terminal 1))
                    (candidate (sb-thread:with-mutex ((pg::physical-store-mutex store))
                                 (pg::physical-store-follow-candidate store before applied applied 2 history eor))))
               (setf (getf anchor :receive-lsn) cursor (getf anchor :history-text) history
                     (getf anchor :eor-octets) (coerce eor 'list))
               (let ((binding (list :version 1 :phase :pending :anchor anchor :child-history history
                                    :before before :after (pg::physical-store-control-tuple candidate)
                                    :applied applied :required (copy-list applied)
                                    :recovery (list :phase 2 :cluster-identity "history-source" :checkpoint 64
                                                    :observed-next-xid 11 :history-target nil
                                                    :history-digest nil :restartpoint nil))))
                 (pg::physical-follow-binding-check binding)
                 (funcall function root store candidate base binding))))
        (pg::physical-store-close store)))))

(deftest follow-replay-proof-observes-required-position-and-preserves-live-state
    (call-with-follow-replay-proof
     (lambda (root store candidate base binding)
       (let* ((live (list :timeline 99 :source-checkpoint nil :observed-next-xid 999))
              (pg::*physical-recovery-state* live) (pg::*physical-receive-store* store)
              (pg::*next-xid* 999) (pg::*last-replay-lsn* 999)
              (pg::*wal-recovery-group-active* :live)
              (control-before (lineage-test-bytes (pg::physical-store-path store "receive.control")))
              (base-before (copy-tree base)))
         (pg::wal-ensure-directory-durable (merge-pathnames "global/" root))
         (pg::wal-atomic-write-bytes (pg::catalog-path) #(1 2 3))
         (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) #(4 5 6))
         (let ((result (pg::physical-follow-validate-candidate-replay candidate base binding)))
           (ok (= 21 (getf result :next-xid)))
           (ok (= 11 (getf result :required-next-xid)))
           (ok (= (getf (getf binding :anchor) :receive-lsn) (getf result :terminal)))
           (ok (= 1 (getf result :timeline))))
         (incf (getf (getf binding :recovery) :observed-next-xid))
         (ok (signals (pg::physical-follow-validate-candidate-replay candidate base binding) 'error))
         (ok (eq live pg::*physical-recovery-state*))
         (ok (equal live '(:timeline 99 :source-checkpoint nil :observed-next-xid 999)))
         (ok (eq store pg::*physical-receive-store*))
         (ok (= 999 pg::*next-xid* pg::*last-replay-lsn*))
         (ok (eq :live pg::*wal-recovery-group-active*))
         (ok (equal base-before base))
         (ok (equalp control-before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
         (ok (equalp #(1 2 3) (lineage-test-bytes (pg::catalog-path))))
         (ok (equalp #(4 5 6) (lineage-test-bytes (pg::physical-recovery-control-path))))))))

(deftest follow-replay-proof-rejects-forged-visibility-and-unclean-boundaries
    (dolist (kind '(:visibility :applied :unfinished))
      (call-with-follow-replay-proof
       (lambda (root store candidate base binding)
         (declare (ignore root store))
         (ecase kind
           (:visibility (incf (getf (getf binding :recovery) :observed-next-xid)))
           (:applied (setf (getf binding :applied) '(96 1)
                           (getf binding :required) '(64 1)
                           (getf (getf binding :recovery) :observed-next-xid) 3))
           (:unfinished nil))
         (ok (signals (pg::physical-follow-validate-candidate-replay candidate base binding) 'error)))
       :unfinished (eq kind :unfinished))))

(defun follow-replay-proof-write-restartpoint (store base lsn next &optional (version 3) (prefix-delta 0) overrides)
  (multiple-value-bind (deletes catalog) (pg::wal-decode-catalog-payload (history-catalog-payload nil nil))
    (declare (ignore deletes))
    (let* ((metadata (list :version version :system-id (getf base :system-id)
                           :cluster-identity (getf base :cluster-identity)
                           :timeline (getf base :timeline) :manifest-digest (getf base :manifest-digest) :base (getf base :end)
                           :lsn lsn :prefix-digest (+ prefix-delta (pg::physical-store-prefix-digest store lsn))
                           :next-xid next :forks nil :seen nil :source-checkpoint nil))
           (pointer (make-array 28 :element-type '(unsigned-byte 8) :initial-element 0)))
      (when (>= version 3)
        (setf (getf metadata :source-reservation)
              (list :version 1 :through lsn :bound 1000 :complete nil)))
      (when (= version 4) (setf (getf metadata :wal-prefix-digest) nil))
      (loop for (key value) on overrides by #'cddr do (setf (getf metadata key) value))
      (let* ((bytes (pg::physical-restartpoint-encode metadata catalog))
             (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
        (pg::wal-ensure-directory-durable (pg::physical-restartpoint-directory))
        (pg::wal-atomic-write-bytes (pg::physical-restartpoint-generation-path lsn digest) bytes)
        (replace pointer #(80 71 82 80 80 84 82 49))
        (pg::wal-set-u64 pointer 8 lsn) (pg::wal-set-u64 pointer 16 digest)
        (pg::wal-set-u32 pointer 24 (pg::wal-crc32 pointer))
        (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer)
        (list lsn digest)))))

(deftest follow-replay-proof-seeds-from-real-restartpoint-and-allows-older-required-pair
    (dolist (empty-scan '(nil t))
      (call-with-follow-replay-proof
       (lambda (root store candidate base binding)
         (declare (ignore root))
         (let* ((lsn (if empty-scan (pg::physical-store-flushed-lsn store) (first (getf binding :applied))))
                (next (if empty-scan 21 11))
                (witness (getf binding :recovery)))
           (setf (getf witness :restartpoint)
                 (follow-replay-proof-write-restartpoint store base lsn next)
                 (getf binding :applied) (list lsn 1)
                 (getf binding :required) (if empty-scan (list lsn 1) '(64 1))
                 (getf witness :observed-next-xid) (if empty-scan next 3))
           (let ((result (pg::physical-follow-validate-candidate-replay candidate base binding)))
             (ok (= lsn (getf result :start)))
             (ok (= 21 (getf result :next-xid)))
             (ok (= (if empty-scan 21 3) (getf result :required-next-xid)))))))))

(deftest follow-replay-proof-rejects-changed-restartpoint-and-visibility-regression
    (dolist (kind '(:pointer :visibility :prefix :legacy))
      (call-with-follow-replay-proof
       (lambda (root store candidate base binding)
         (declare (ignore root))
         (let* ((witness (getf binding :recovery))
                (lsn (first (getf binding :applied)))
                (token (follow-replay-proof-write-restartpoint
                        store base lsn 11 (if (eq kind :legacy) 2 3) (if (eq kind :prefix) 1 0))))
           (setf (getf witness :restartpoint) token)
           (ecase kind
             (:pointer (incf (second token)))
             (:visibility (setf (getf binding :required) '(64 1)
                                (getf witness :observed-next-xid) 12))
             (:prefix nil)
             (:legacy (setf (getf witness :observed-next-xid) 12)))
           (ok (signals (pg::physical-follow-validate-candidate-replay candidate base binding) 'error)))))))

(deftest follow-replay-proof-legacy-restartpoint-falls-back-to-retained-base
    (call-with-follow-replay-proof
     (lambda (root store candidate base binding)
       (declare (ignore root))
       (setf (getf (getf binding :recovery) :restartpoint)
             (follow-replay-proof-write-restartpoint store base (first (getf binding :applied)) 999 2))
       (let ((result (pg::physical-follow-validate-candidate-replay candidate base binding)))
         (ok (= 64 (getf result :start)))
         (ok (= 11 (getf result :required-next-xid)))
         (ok (= 21 (getf result :next-xid)))))))

(deftest follow-replay-proof-crosses-selected-eor-and-restores-child-seed
    (dolist (restartpoint '(nil t))
      (call-with-restartpoint-fixture
       (lambda (root store)
         (declare (ignore root))
         (let ((base (copy-tree pg::*physical-recovery-state*)))
           (setf (pg::physical-store-retained-identity store)
                 '(:system-id 42 :base-timeline 1 :base 32 :manifest-digest 77))
           (selected-replay-test-prepare store)
           (pg::physical-store-input store 152 (sample-physical-record 152))
           (pg::physical-store-input store 184 (selected-replay-test-child-checkpoint 184))
           (pg::physical-store-flush store)
           (let* ((fork (pg::physical-store-flushed-lsn store))
                  (before (pg::physical-store-control-tuple store))
                  (parent-history (getf (pg::physical-store-selection store) :history-text))
                  (history (store-history-text 2 fork parent-history))
                  (eor (store-history-eor fork 3 2))
                  (anchor (follow-anchor-request (pg::physical-store-prefix-digest store fork) 456))
                  (applied (list fork 2))
                  (candidate (sb-thread:with-mutex ((pg::physical-store-mutex store))
                               (pg::physical-store-follow-candidate store before applied '(152 2) 3 history eor)))
                  (binding (list :version 1 :phase :pending :anchor anchor :child-history history
                                 :before before :after (pg::physical-store-control-tuple candidate)
                                 :applied applied :required '(152 2)
                                 :recovery (list :phase 2 :cluster-identity "history-source" :checkpoint 32
                                                 :observed-next-xid 50 :history-target 2
                                                 :history-digest (ninth before) :restartpoint nil))))
             (setf (getf anchor :base) 32 (getf anchor :source-timeline) 3
                   (getf anchor :receive-timeline) 2 (getf anchor :receive-lsn) fork
                   (getf anchor :history-text) history (getf anchor :eor-octets) (coerce eor 'list))
             (when restartpoint
               (setf (getf (getf binding :recovery) :restartpoint)
                     (follow-replay-proof-write-restartpoint
                      store base 152 50 4 0
                      (list :replay-timeline 2 :history-target 2 :history-digest (ninth before)
                            :source-checkpoint (sample-checkpoint-snapshot :start 0 :end 120 :redo 0 :next-xid 50)))))
             (let ((result (pg::physical-follow-validate-candidate-replay candidate base binding)))
               (ok (= (if restartpoint 152 32) (getf result :start)))
               (ok (= 2 (getf result :timeline)))
               (ok (= 2000 (getf result :next-xid)))
               (ok (= 50 (getf result :required-next-xid)))))))
       :system-id 42)))

(deftest native-checkpoint-cannot-change-timeline-without-selected-eor
    (dolist (claimed '(1 2 9))
      (let* ((pg::*physical-recovery-state* (list :timeline 1 :replay-timeline 1 :source-checkpoint nil))
             (record (pg::wal-decode-record (selected-replay-test-child-checkpoint 64) 64))
             (snapshot (pg::wal-checkpoint-record-snapshot record)))
        (setf (getf snapshot :timeline) claimed)
        (setf record (pg::wal-decode-record
                      (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 64
                                             (pg::wal-encode-checkpoint-snapshot snapshot)) 64))
        (if (= claimed 1)
            (ok (null (pg::physical-replay-check-child-initialization record 3)))
            (ok (signals (pg::physical-replay-check-child-initialization record 3) 'error))))))

(defun follow-pending-proof-control (binding &key post (next 11) (required '(136 1)))
  (let ((tuple (getf binding :after)))
    (pg::physical-recovery-control-image
     2 (first tuple) (second tuple) (nth 9 tuple)
     (getf (getf binding :recovery) :checkpoint)
     (first required) next :applied-timeline (second required)
     :history-target (when post (eighth tuple))
     :history-digest (when post (ninth tuple)))))

(defun call-with-follow-pending-proof (function)
  (call-with-follow-replay-proof
   (lambda (root store candidate base binding)
     (let ((pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
                                               (config (list 1 "127.0.0.1" 5433 "pending_proof" binding)))
       (pg::wal-ensure-directory-durable (pg::wal-directory))
       (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                   (follow-pending-proof-control binding))
       (let ((cursor (pg::physical-store-flushed-lsn store)))
         (dolist (entry (list (cons pg::+wal-record-batch-start+ (empty-octets))
                              (cons pg::+wal-record-commit+ (pg::wal-encode-commit-timestamp 42))))
           (let ((raw (pg::wal-encode-record (car entry) (if (= (car entry) pg::+wal-record-commit+) 30 0)
                                             cursor (cdr entry))))
             (pg::physical-store-input store cursor raw) (incf cursor (length raw))))
         (pg::physical-store-flush store)
         (setf (getf binding :before) (pg::physical-store-control-tuple store)))
       (pg::physical-follow-save-config config)
       (funcall function root store candidate base config)))))

(deftest follow-pending-catchup-preserves-an-unfinished-parent-batch
    (call-with-follow-replay-proof
     (lambda (root store ignored base binding)
       (declare (ignore root ignored))
       (let* ((cursor (pg::physical-store-flushed-lsn store)) (fork (+ cursor 32))
                                                              (history (store-history-text 1 fork)) (anchor (getf binding :anchor))
                                                              (copy (pg::physical-follow-copy-replay-candidate store))
                                                              (config (list 1 "127.0.0.1" 5433 "pending_catchup" binding)))
         (setf (getf binding :after) (copy-list (getf binding :before))
               (getf binding :child-history) history
               (getf anchor :history-text) history
               (getf anchor :eor-octets) (coerce (store-history-eor fork 2 1) 'list))
         (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                     (follow-pending-proof-control binding))
         (pg::physical-follow-save-config config)
         (let ((result (pg::physical-follow-validate-candidate-replay copy base binding)))
           (ok (= 208 (getf result :terminal)))
           (ok (= 11 (getf result :required-next-xid))))
         (multiple-value-bind (candidate position) (pg::physical-follow-check-pending-state store base config)
           (ok (eq :after position))
           (ok (equal (getf binding :before) (pg::physical-store-control-tuple candidate))))
         (pg::wal-ensure-directory-durable (pg::wal-directory))
         (let ((active (pg::physical-follow-reconcile-pending-store store base config)))
           (ok (eq :active (getf (fifth active) :phase)))
           (ok (null (probe-file (pg::timeline-history-path 2 pg::*pgdata*))))
           (ok (= 136 (getf (pg::physical-recovery-read-control pg::*pgdata*) :applied))))
         (ok (= cursor (pg::physical-store-flushed-lsn store)))))
     :unfinished t))

(deftest follow-pending-catchup-rejects-conflicting-proposed-child-history
    (call-with-follow-replay-proof
     (lambda (root store ignored base binding)
       (declare (ignore root ignored))
       (let* ((fork (+ 32 (pg::physical-store-flushed-lsn store)))
              (history (store-history-text 1 fork)) (anchor (getf binding :anchor))
              (config (list 1 "127.0.0.1" 1 "pending_catchup" binding))
              (before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
         (setf (getf binding :after) (copy-list (getf binding :before))
               (getf binding :child-history) history (getf anchor :history-text) history
               (getf anchor :eor-octets) (coerce (store-history-eor fork 2 1) 'list))
         (pg::wal-ensure-directory-durable (pg::wal-directory))
         (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                     (follow-pending-proof-control binding))
         (pg::physical-follow-save-config config)
         (let ((recovery-before (lineage-test-bytes (pg::physical-recovery-control-path))))
           (pg::wal-atomic-write-bytes (pg::timeline-history-path 2 pg::*pgdata*)
                                       (pg::string->bytes (store-history-text 1 64)))
           (ok (signals (pg::physical-follow-reconcile-pending-store store base config) 'error))
           (ok (equalp recovery-before (lineage-test-bytes (pg::physical-recovery-control-path)))))
         (ok (equalp before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
         (ok (equal config (pg::physical-follow-read-config)))
         (ng (pg::physical-store-failed-p store))))
     :unfinished t))

(deftest follow-pending-preflight-accepts-only-valid-durable-publication-states
    (dolist (stage '(:old :receive :recovery))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore root candidate))
         (let* ((binding (fifth config)) (anchor (getf binding :anchor)))
           (unless (eq stage :old)
             (pg::physical-store-rewind-select-history
              store (getf binding :before) (getf binding :applied) (getf binding :required)
              2 (getf binding :child-history) (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8)))))
           (when (eq stage :recovery)
             (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                         (follow-pending-proof-control binding :post t)))
           (let ((receive (lineage-test-bytes (pg::physical-store-path store "receive.control")))
                 (recovery (lineage-test-bytes (pg::physical-recovery-control-path)))
                 (follow (lineage-test-bytes (pg::physical-follow-config-path))))
             (multiple-value-bind (validated position)
                 (pg::physical-follow-check-pending-state store base config)
               (ok (equal (getf binding :after) (pg::physical-store-control-tuple validated)))
               (ok (eq position (if (eq stage :old) :before :after))))
             (ok (equalp receive (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
             (ok (equalp recovery (lineage-test-bytes (pg::physical-recovery-control-path))))
             (ok (equalp follow (lineage-test-bytes (pg::physical-follow-config-path))))))))))

(deftest follow-pending-preflight-rejects-changed-progress-or-crossed-publication
    (dolist (kind '(:next :required :crossed :config :phase :identity :checkpoint :timeline :history :restartpoint))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore root candidate))
         (let ((binding (fifth config)))
           (ecase kind
             (:next (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                                (follow-pending-proof-control binding :next 12)))
             (:required (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                                    (follow-pending-proof-control binding :required '(64 1))))
             (:crossed (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                                   (follow-pending-proof-control binding :post t)))
             (:config (let ((other (copy-tree config)))
                        (setf (fourth other) "changed_pin") (pg::physical-follow-save-config other)))
             (:phase (setf (getf binding :phase) :active))
             (:identity (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                                    (pg::physical-recovery-control-image 2 43 1 77 64 136 11)))
             (:checkpoint (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                                      (pg::physical-recovery-control-image 2 42 1 77 32 136 11)))
             (:timeline (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                                    (pg::physical-recovery-control-image 2 42 2 77 64 136 11)))
             (:history (pg::wal-atomic-write-bytes
                        (pg::physical-recovery-control-path)
                        (pg::physical-recovery-control-image 2 42 1 77 64 136 11
                                                             :history-target 3 :history-digest 123)))
             (:restartpoint (follow-replay-proof-write-restartpoint store base 136 11)))
           (ok (signals (pg::physical-follow-check-pending-state store base config) 'error)))))))

(deftest follow-pending-reconciliation-commits-without-advancing-required-progress
    (dolist (stage '(:old :receive :recovery))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore root candidate))
         (let* ((binding (fifth config)) (anchor (getf binding :anchor))
                                         (runtime (list :untouched t)) (pg::*physical-recovery-state* runtime)
                                         (pg::*next-xid* 999) (pg::*last-replay-lsn* 999))
           (setf (getf binding :applied) (list (getf anchor :receive-lsn) 1))
           (pg::physical-follow-save-config config)
           (unless (eq stage :old)
             (pg::physical-store-rewind-select-history
              store (getf binding :before) (getf binding :applied) (getf binding :required)
              2 (getf binding :child-history) (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8)))))
           (when (eq stage :recovery)
             (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                         (follow-pending-proof-control binding :post t)))
           (let ((active (pg::physical-follow-reconcile-pending-store store base config))
                 (control (pg::physical-recovery-read-control pg::*pgdata*)))
             (ok (eq :active (getf (fifth active) :phase)))
             (ok (equal active (pg::physical-follow-read-config)))
             (ok (equal (getf binding :after) (pg::physical-store-control-tuple store)))
             (ok (= 136 (getf control :applied))))
           (ok (= 11 (getf (pg::physical-recovery-read-control pg::*pgdata*) :next-xid)))
           (ok (= 1 (getf (pg::physical-recovery-read-control pg::*pgdata*) :applied-timeline)))
           (ok (equalp (pg::string->bytes (getf binding :child-history))
                       (lineage-test-bytes (pg::timeline-history-path 2 pg::*pgdata*))))
           (ok (eq runtime pg::*physical-recovery-state*))
           (ok (= 999 pg::*next-xid* pg::*last-replay-lsn*))
           (ok (signals (pg::physical-follow-check-startup-config) 'error)))))))

(deftest follow-active-proof-allows-receipt-required-and-restartpoint-progress
    (dolist (progress '(:unchanged :receipt :required :restartpoint))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore root candidate))
         (let* ((active (pg::physical-follow-reconcile-pending-store store base config))
                (binding (fifth active))
                (fork (pg::physical-store-flushed-lsn store))
                (end fork))
           (unless (eq progress :unchanged)
             (let ((raw (coerce (getf (getf binding :anchor) :eor-octets)
                                '(vector (unsigned-byte 8)))))
               (pg::physical-store-input store fork raw)
               (incf end (length raw))
               (pg::physical-store-flush store)))
           (when (eq progress :required)
             (pg::wal-atomic-write-bytes
              (pg::physical-recovery-control-path)
              (follow-pending-proof-control binding :post t :next 21 :required (list end 2))))
           (when (eq progress :restartpoint)
             (follow-replay-proof-write-restartpoint
              store base end 21 4 0
              (list :replay-timeline 2 :history-target 2
                    :history-digest (ninth (getf binding :after))))
             )
           (let ((tuple (pg::physical-store-control-tuple store))
                 (control (pg::physical-recovery-read-control pg::*pgdata*)))
             (let ((result (pg::physical-follow-check-active-state store base active)))
               (ok (= end (getf result :terminal)))
               (ok (= (if (eq progress :unchanged) 1 2) (getf result :timeline))))
             (ok (equal active (pg::physical-follow-read-config)))
             (ok (equal tuple (pg::physical-store-control-tuple store)))
             (ok (equal control (pg::physical-recovery-read-control pg::*pgdata*)))
             (ok (not (pg::physical-store-failed-p store)))))))))

(deftest follow-active-proof-rejects-progress-and-provenance-regression
    (dolist (kind '(:required :visibility :timeline :manifest :selection :config))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore root candidate))
         (let* ((active (pg::physical-follow-reconcile-pending-store store base config))
                (binding (fifth active)))
           (case kind
             (:required
              (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                          (follow-pending-proof-control binding :post t :next 3 :required '(64 1))))
             (:visibility
              (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                          (follow-pending-proof-control binding :post t :next 10)))
             (:timeline
              (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                          (follow-pending-proof-control binding :post t :required '(136 2))))
             (:manifest (incf (getf base :manifest-digest)))
             (:selection (setf (pg::physical-store-selection store) nil))
             (:config (setf (second active) "changed.example")))
           (ok (signals (pg::physical-follow-check-active-state store base active) 'error)))))))

(deftest follow-active-proof-checks-compacted-anchor-through-the-current-root
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (config (follow-pending-proof-config-for-store store base)))
         (pg::physical-follow-save-config config)
         (let* ((active (pg::physical-follow-reconcile-pending-store store base config))
                (binding (fifth active))
                (fork (pg::physical-store-flushed-lsn store))
                (raw (coerce (getf (getf binding :anchor) :eor-octets)
                             '(vector (unsigned-byte 8))))
                (end (+ fork (length raw))))
           (pg::physical-store-input store fork raw)
           (pg::physical-store-flush store)
           (follow-replay-proof-write-restartpoint
            store base end 50 4 0
            (list :replay-timeline 2 :history-target 2
                  :history-digest (ninth (getf binding :after))))
           (pg::physical-store-compact store end (lambda (digest) (declare (ignore digest))))
           (let ((result (pg::physical-follow-check-active-state store base active)))
             (ok (= (getf base :end) (getf result :start)))
             (ok (= end (getf result :terminal)))
             (ok (= 2 (getf result :timeline))))
           (ok (> (pg::physical-store-floor-lsn store) fork))
           (let* ((index (pg::physical-retained-inventory-load
                          (pg::physical-store-retained-directory store)
                          (pg::physical-store-retained-root store)
                          (pg::physical-store-retained-identity store)
                          (pg::physical-store-retained-history store)))
                  (anchor (getf binding :anchor))
                  (expected (getf anchor :receive-digest)))
             (ok (= expected (pg::physical-follow-active-prefix-digest store fork index)))
             (incf (getf anchor :receive-digest))
             (ok (signals (pg::physical-follow-check-active-receive store base binding) 'error))
             (setf (getf anchor :receive-digest) expected)
             (ok (signals (pg::physical-follow-active-prefix-digest store fork nil) 'error)))
           (ok (not (pg::physical-store-failed-p store)))))) :system-id 42))

(deftest follow-active-proof-reconstructs-new-required-visibility-before-a-newer-restartpoint
    (dolist (next '(20 21 22))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore root candidate))
         (let* ((active (pg::physical-follow-reconcile-pending-store store base config))
                (binding (fifth active))
                (fork (pg::physical-store-flushed-lsn store))
                (raw (coerce (getf (getf binding :anchor) :eor-octets)
                             '(vector (unsigned-byte 8))))
                (end (+ fork (length raw))))
           (pg::physical-store-input store fork raw)
           (pg::physical-store-flush store)
           (follow-replay-proof-write-restartpoint
            store base end 21 4 0
            (list :replay-timeline 2 :history-target 2
                  :history-digest (ninth (getf binding :after))))
           (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                       (follow-pending-proof-control binding :post t :next next :required (list fork 1)))
           (if (= next 21)
               (let ((result (pg::physical-follow-check-active-state store base active)))
                 (ok (= 64 (getf result :start)))
                 (ok (= 21 (getf result :required-next-xid))))
               (ok (signals (pg::physical-follow-check-active-state store base active) 'error)))
           (ok (not (pg::physical-store-failed-p store))))))))

(deftest follow-active-proof-allows-an-open-receive-batch-with-clean-required-progress
    (call-with-follow-pending-proof
     (lambda (root store candidate base config)
       (declare (ignore root candidate))
       (let* ((active (pg::physical-follow-reconcile-pending-store store base config))
              (fork (pg::physical-store-flushed-lsn store))
              (raw (coerce (getf (getf (fifth active) :anchor) :eor-octets)
                           '(vector (unsigned-byte 8))))
              (end (+ fork (length raw))))
         (pg::physical-store-input store fork raw)
         (pg::physical-store-input store end
                                   (pg::wal-encode-record pg::+wal-record-batch-start+ 0 end (empty-octets)))
         (pg::physical-store-flush store)
         (let ((result (pg::physical-follow-check-active-state store base active)))
           (ok (= end (getf result :terminal)))
           (ok (< (getf result :terminal) (pg::physical-store-flushed-lsn store))))
         (ok (not (pg::physical-store-failed-p store)))))))

(deftest follow-active-proof-compares-historical-replay-with-the-newer-restartpoint-counter
    (dolist (next '(20 21 22))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore root candidate))
         (let* ((active (pg::physical-follow-reconcile-pending-store store base config))
                (binding (fifth active)) (fork (pg::physical-store-flushed-lsn store))
                (raw (coerce (getf (getf binding :anchor) :eor-octets)
                             '(vector (unsigned-byte 8))))
                (end (+ fork (length raw))))
           (pg::physical-store-input store fork raw)
           (pg::physical-store-flush store)
           (follow-replay-proof-write-restartpoint
            store base end next 4 0
            (list :replay-timeline 2 :history-target 2
                  :history-digest (ninth (getf binding :after))))
           (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                       (follow-pending-proof-control binding :post t :next 21 :required (list fork 1)))
           (if (= next 21)
               (ok (= 21 (getf (pg::physical-follow-check-active-state store base active) :next-xid)))
               (ok (signals (pg::physical-follow-check-active-state store base active) 'error)))
           (ok (not (pg::physical-store-failed-p store))))))))

(deftest follow-active-proof-reconstructs-required-visibility-through-compacted-xid-records
    (dolist (next '(70 71 72))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let ((cursor (pg::physical-store-flushed-lsn store)))
           (dolist (type (list pg::+wal-record-batch-start+ pg::+wal-record-commit+))
             (let ((raw (pg::wal-encode-record
                         type (if (= type pg::+wal-record-commit+) 70 0) cursor
                         (if (= type pg::+wal-record-commit+)
                             (pg::wal-encode-commit-timestamp 42) (empty-octets)))))
               (pg::physical-store-input store cursor raw) (incf cursor (length raw)))))
         (pg::physical-store-flush store)
         (let* ((base (pg::physical-recovery-verify-base-state))
                (config (follow-pending-proof-config-for-store store base)))
           (pg::physical-follow-save-config config)
           (let* ((active (pg::physical-follow-reconcile-pending-store store base config))
                  (binding (fifth active)) (fork (pg::physical-store-flushed-lsn store))
                  (raw (coerce (getf (getf binding :anchor) :eor-octets)
                               '(vector (unsigned-byte 8))))
                  (end (+ fork (length raw))))
             (pg::physical-store-input store fork raw)
             (pg::physical-store-flush store)
             (follow-replay-proof-write-restartpoint
              store base end 71 4 0
              (list :replay-timeline 2 :history-target 2
                    :history-digest (ninth (getf binding :after))))
             (pg::physical-store-compact store end (lambda (digest) (declare (ignore digest))))
             (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path)
                                         (follow-pending-proof-control binding :post t :next next :required (list fork 1)))
             (if (= next 71)
                 (let ((result (pg::physical-follow-check-active-state store base active)))
                   (ok (= (getf base :end) (getf result :start)))
                   (ok (= 71 (getf result :required-next-xid))))
                 (ok (signals (pg::physical-follow-check-active-state store base active) 'error)))
             (ok (> (pg::physical-store-floor-lsn store) fork))
             (ok (not (pg::physical-store-failed-p store))))))
       :system-id 42)))

(deftest follow-pending-history-conflict-is-rejected-before-receive-publication
    (call-with-follow-pending-proof
     (lambda (root store candidate base config)
       (declare (ignore root candidate))
       (let ((before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
         (pg::wal-atomic-write-bytes (pg::timeline-history-path 2 pg::*pgdata*)
                                     (pg::string->bytes (store-history-text 1 64)))
         (ok (signals (pg::physical-follow-reconcile-pending-store store base config) 'error))
         (ok (equalp before (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
         (ok (eq :pending (getf (fifth (pg::physical-follow-read-config)) :phase)))
         (ng (pg::physical-store-failed-p store))))))

(deftest follow-pending-offline-entry-rejects-live-storage
    (let ((pg::*wal-ready* t))
      (ok (signals (pg::physical-follow-reconcile-pending-offline) 'error))))

(defun follow-pending-proof-config-for-store (store base)
  (let* ((fork (pg::physical-store-flushed-lsn store))
         (parent (pg::physical-store-selected-timeline store)) (child (1+ parent))
         (history (store-history-text parent fork (getf (pg::physical-store-selection store) :history-text)))
         (eor (store-history-eor fork child parent))
         (before (pg::physical-store-control-tuple store))
         (applied (list fork parent))
         (control (pg::physical-recovery-read-control pg::*pgdata*))
         (required (list (getf control :applied) (getf control :applied-timeline (getf control :timeline))))
         (candidate (sb-thread:with-mutex ((pg::physical-store-mutex store))
                      (pg::physical-store-follow-candidate store before applied required child history eor)))
         (anchor (follow-anchor-request (pg::physical-store-prefix-digest store fork) 456))
         (restartpoint (multiple-value-bind (metadata catalog prefix digest) (pg::physical-restartpoint-read base)
                         (declare (ignore catalog prefix))
                         (when metadata (list (getf metadata :lsn) digest)))))
    (setf (getf anchor :system-id) (getf base :system-id)
          (getf anchor :base) (getf base :end) (getf anchor :base-timeline) (getf base :timeline)
          (getf anchor :manifest-digest) (getf base :manifest-digest)
          (getf anchor :receive-lsn) fork (getf anchor :receive-timeline) parent
          (getf anchor :source-timeline) child (getf anchor :history-text) history
          (getf anchor :eor-octets) (coerce eor 'list))
    (list 1 "127.0.0.1" 1 "offline_pending"
          (list :version 1 :phase :pending :anchor anchor :child-history history
                :before before :after (pg::physical-store-control-tuple candidate)
                :applied applied :required required
                :recovery (list :phase 2 :cluster-identity (getf base :cluster-identity)
                                :checkpoint (getf base :checkpoint) :observed-next-xid (getf control :next-xid)
                                :history-target (getf control :history-target)
                                :history-digest (getf control :history-digest) :restartpoint restartpoint)))))

(deftest follow-pending-offline-entry-owns-rooted-spool-without-starting-storage
    (call-with-promotion-fixture
     (lambda (root store)
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (config (follow-pending-proof-config-for-store store base))
              (pg::*postmaster-data-lock* nil) (pg::*postmaster-pid-stream* nil)
              (pg::*postmaster-pid-directory* nil) (pg::*postmaster-start-time* nil)
              (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
              (pg::*postmaster-pid-port* nil) (pg::*postmaster-pid-inode* nil)
              (saved-exit-hooks sb-ext:*exit-hooks*))
         (pg::physical-follow-save-config config)
         (pg::physical-store-close store)
         (sb-posix:rename (namestring (pg::physical-store-directory store))
                          (namestring (pg::physical-recovery-spool-directory)))
         (sb-posix:chmod (namestring root) #o700)
         (unwind-protect
              (let ((pg::*wal-ready* nil) (pg::*physical-receive-store* nil))
                (pg::postmaster-own-data-directory "127.0.0.1" 5439)
                (let ((active (pg::physical-follow-reconcile-pending-offline)))
                  (ok (eq :active (getf (fifth active) :phase)))
                  (ok (equal active (pg::physical-follow-read-config)))
                  (ok (null pg::*wal-ready*))
                  (ok (null pg::*physical-receive-store*))
                  (ok (null pg::*physical-follow-runtime*))
                  (ok (signals (pg::physical-follow-check-startup-config) 'error))))
           (pg::postmaster-remove-pid-file)
           (setf sb-ext:*exit-hooks* saved-exit-hooks)
           (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))))) :system-id 42))

(defun follow-pending-proof-run-crash (root store base point &optional rooted)
  (pg::wal-atomic-write-bytes
   (merge-pathnames "pending-base.fixture" root)
   (pg::string->bytes (pg::physical-bootstrap-manifest-text base)))
  (pg::physical-store-close store)
  (multiple-value-bind (output errors status)
      (uiop:run-program
       (append (list (or (uiop:getenv "SBCL") "sbcl") "--noinform" "--no-sysinit" "--no-userinit" "--script"
                     (namestring (asdf:system-relative-pathname "pg" "t/follow-pending-crash-child.lisp"))
                     (namestring root) (namestring (pg::physical-store-directory store)) (symbol-name point))
               (when rooted (list "rooted")))
       :output :string :error-output :string :ignore-error-status t)
    (ok (not (zerop status)))
    (ok (probe-file (merge-pathnames "pending-crash.marker" root))
        (format nil "~a: ~a ~a" point output errors))))

(defun same-timeline-pending-proof-config (store config)
  (let* ((result (copy-tree config))
         (binding (fifth result))
         (before (pg::physical-store-control-tuple store))
         (old (getf binding :anchor))
         (anchor (same-timeline-follow-anchor (fourth before) (fifth before) 456)))
    (setf (getf anchor :system-id) (first before)
          (getf anchor :base-timeline) (second before)
          (getf anchor :base) (third before)
          (getf anchor :manifest-digest) (getf old :manifest-digest)
          (getf binding :anchor) anchor
          (getf binding :child-history) nil
          (getf binding :before) before
          (getf binding :after) (copy-list before))
    (pg::physical-follow-check-config result)
    result))

(deftest same-timeline-pending-sigkill-preserves-the-existing-receive-selection
    (dolist (point '(:after-pending-preflight :after-pending-receive
                     :after-pending-history :after-pending-recovery
                     :before-follow-config-rename :after-follow-config-rename
                     :after-pending-active))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore candidate))
         (let* ((pending (same-timeline-pending-proof-config store config))
                (before (copy-list (getf (fifth pending) :before))))
           (pg::physical-follow-save-config pending)
           (follow-pending-proof-run-crash root store base point)
           (multiple-value-bind (output errors status)
               (uiop:run-program
                (list (or (uiop:getenv "SBCL") "sbcl") "--noinform" "--no-sysinit" "--no-userinit" "--script"
                      (namestring (asdf:system-relative-pathname "pg" "t/follow-pending-crash-child.lisp"))
                      (namestring root) (namestring (pg::physical-store-directory store)) "VERIFY-SAME")
                :output :string :error-output :string :ignore-error-status t)
             (ok (zerop status) (format nil "~a: ~a ~a" point output errors))
             (ok (search "same-timeline pending cold verification PASS" output)))
           (let* ((saved (pg::physical-follow-read-config))
                  (reopened
                    (pg::physical-store-open
                     root 42 1 :discard-tail nil :create-lock nil
                     :retained-identity (pg::physical-recovery-retained-identity base))))
             (unwind-protect
                  (progn
                    (when (eq :pending (getf (fifth saved) :phase))
                      (pg::physical-follow-reconcile-pending-store reopened base saved))
                    (let ((active (pg::physical-follow-read-config))
                          (control (pg::physical-recovery-read-control root)))
                      (ok (eq :active (getf (fifth active) :phase)))
                      (ok (equal before (pg::physical-store-control-tuple reopened)))
                      (ok (null (pg::physical-store-selection reopened)))
                      (ok (null (getf control :history-target)))
                      (ok (= (getf control :applied) (first (getf (fifth pending) :required))))
                      (ok (pg::physical-follow-check-active-state reopened base active))))
               (pg::physical-store-close reopened))))))))

(deftest follow-pending-sigkill-reconciles-receive-recovery-and-active-publication
    (dolist (point '(:after-pending-preflight :after-history-publication :before-control-rename :after-control-rename
                     :after-control-directory-fsync :after-follow-control-readback :before-follow-tail-cleanup
                     :after-pending-receive :after-pending-history
                     :after-rename :after-directory-fsync :after-pending-recovery
                     :before-follow-config-fsync :after-follow-config-fsync :before-follow-config-rename
                     :after-follow-config-rename :after-follow-config-directory-fsync :after-pending-active))
      (call-with-follow-pending-proof
       (lambda (root store candidate base config)
         (declare (ignore candidate))
         (follow-pending-proof-run-crash root store base point)
         (let* ((saved (pg::physical-follow-read-config))
                (active (eq :active (getf (fifth saved) :phase)))
                (reopened (pg::physical-store-open
                           root 42 1 :discard-tail nil :create-lock nil
                           :retained-identity (pg::physical-recovery-retained-identity base))))
           (unwind-protect
                (progn
                  (ok (eq active (not (null (member point '(:after-follow-config-rename :after-follow-config-directory-fsync :after-pending-active)))))))
             (unless active
               (pg::physical-follow-reconcile-pending-store reopened base config))
             (ok (eq :active (getf (fifth (pg::physical-follow-read-config)) :phase)))
             (ok (= 208 (pg::physical-store-flushed-lsn reopened)))
             (with-open-file (in (pg::physical-store-segment-path reopened 0 1)
                                 :element-type '(unsigned-byte 8))
               (ok (= 208 (file-length in))))
             (let ((control (pg::physical-recovery-read-control root)))
               (ok (= 136 (getf control :applied)))
               (ok (= 11 (getf control :next-xid)))
               (ok (= 2 (getf control :history-target))))
             (ok (signals (pg::physical-follow-check-startup-config) 'error)))
           (pg::physical-store-close reopened))))))

(deftest follow-pending-rooted-sigkill-restores-protection-in-a-fresh-image
    (dolist (point '(:after-control-rename :after-pending-receive :after-pending-recovery))
      (call-with-promotion-fixture
       (lambda (root store)
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((base (pg::physical-recovery-verify-base-state))
                (config (follow-pending-proof-config-for-store store base)))
           (pg::physical-follow-save-config config)
           (follow-pending-proof-run-crash root store base point t)
           (multiple-value-bind (output errors status)
               (uiop:run-program
                (list (or (uiop:getenv "SBCL") "sbcl") "--noinform" "--no-sysinit" "--no-userinit" "--script"
                      (namestring (asdf:system-relative-pathname "pg" "t/follow-pending-crash-child.lisp"))
                      (namestring root) (namestring (pg::physical-store-directory store)) "VERIFY" "rooted")
                :output :string :error-output :string :ignore-error-status t)
             (ok (zerop status) (format nil "~a ~a" output errors))
             (ok (search "pending rooted cold verification PASS" output)))
           (ok (eq :active (getf (fifth (pg::physical-follow-read-config)) :phase)))))) :system-id 42))

(cl-spec:defproperty follow-pending-recovery-witness-keeps-full-width-visibility
    ((next (range integer 3 18446744073709551616))
     (clean (range integer 0 1))
     (time (range integer -9223372036854775808 9223372036854775807)))
  (:about pg::physical-follow-pending-recovery-position)
  (:trials (:normal 200))
  (let* ((binding (fifth (follow-binding-test-config)))
         (base (list :system-id 42 :timeline 1 :end 64 :manifest-digest 77 :checkpoint 64
                     :cluster-identity "binding-probe"))
         (control (pg::physical-recovery-decode-control
                   (pg::physical-recovery-control-image
                    2 42 1 77 64 96 next :state-time time
                    :db-state (if (zerop clean) pg::+db-in-archive-recovery+
                                  pg::+db-shutdowned-in-recovery+)))))
    (setf (getf (getf binding :recovery) :observed-next-xid) next)
    (and (eq :before (pg::physical-follow-pending-recovery-position base binding control))
         (progn
           (setf (getf control :next-xid) (if (= next 18446744073709551616) 3 (1+ next)))
           (handler-case (progn (pg::physical-follow-pending-recovery-position base binding control) nil)
             (error () t))))))

(deftest generated-follow-pending-recovery-witness-keeps-full-width-visibility
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-pending-recovery-witness-keeps-full-width-visibility :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest follow-binding-config-projects-immediate-child-history-from-newer-source
    (let* ((config (follow-binding-test-config))
           (binding (fifth config)) (anchor (getf binding :anchor)))
      (setf (getf anchor :source-timeline) 3
            (getf anchor :history-text) (store-history-text 2 160 (getf binding :child-history)))
      (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
      (setf (getf binding :child-history) (getf anchor :history-text))
      (ok (signals (pg::physical-follow-check-config config) 'error))))

(deftest follow-binding-startup-guard-precedes-storage-or-recovery-control-work
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (pg::*physical-recovery-state* (list :phase 2))
                                (pg::*physical-recovery-failed-p* nil) (pg::*last-replay-lsn* 999)
                                (pg::*wal-ready* :untouched) (pg::*catalog* :untouched))
        (pg::physical-follow-save-config (follow-binding-test-config))
        (ok (signals (pg::physical-recovery-open-storage) 'error))
        (ok (= 999 pg::*last-replay-lsn*))
        (ok (eq :untouched pg::*wal-ready*))
        (ok (eq :untouched pg::*catalog*))
        (pg::wal-atomic-write-bytes
         (merge-pathnames "recovery.required" root) (pg::physical-recovery-marker-bytes))
        (let ((read-control (symbol-function 'pg::physical-recovery-read-control)) (called nil))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::physical-recovery-read-control)
                       (lambda (&rest args) (declare (ignore args))
                         (setf called t) (error "recovery control was reached")))
                 (ok (signals (pg::physical-recovery-prepare-with-namespace) 'error))
                 (ng called))
            (setf (symbol-function 'pg::physical-recovery-read-control) read-control))))))

(deftest follow-binding-config-decode-refuses-valid-crc-invalid-schema-and-extra-forms
    (dolist (bad (list (append '(1 "127.0.0.1" 5433 "binding_probe") '(nil))
                       (append '(1 "127.0.0.1" 5433 "binding_probe") '((:version 1 :phase :pending)))))
      (let* ((text (pg::physical-bootstrap-manifest-text bad))
             (crc (pg::wal-crc32 (pg::string->bytes text)))
             (bytes (pg::string->bytes (format nil "PGPHYSICALFOLLOW1 ~8,'0X~%~a~%" crc text))))
        (ok (signals (pg::physical-follow-decode-config bytes) 'error))))
  (let* ((text (concatenate 'string
                            (pg::physical-bootstrap-manifest-text (follow-binding-test-config)) " nil"))
         (bytes (pg::string->bytes
                 (format nil "PGPHYSICALFOLLOW1 ~8,'0X~%~a~%"
                         (pg::wal-crc32 (pg::string->bytes text)) text))))
    (ok (signals (pg::physical-follow-decode-config bytes) 'error))))

(deftest follow-binding-config-bounds-history-and-file-before-parsing
    (let* ((config (follow-binding-test-config))
           (binding (fifth config)))
      (setf (getf binding :child-history)
            (make-string (1+ pg::+physical-store-history-maximum-bytes+) :initial-element #\a))
      (ok (signals (pg::physical-follow-check-config config) 'error)))
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root))
      (with-open-file (out (pg::physical-follow-config-path) :direction :output
                                                             :element-type '(unsigned-byte 8))
        (file-position out pg::+physical-follow-config-maximum-bytes+)
        (write-byte 0 out) (pg::fsync-stream out))
      (ok (signals (pg::physical-follow-read-config) 'error)))))

(deftest follow-anchor-format-keeps-carried-seed-separate-from-overlap
    (let* ((pin (follow-anchor-pin 123 456))
           (loaded (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
      (ok (pg::physical-follow-pin-p loaded))
      (ok (eq :retaining (pg::physical-follow-pin-stage loaded)))
      (ok (null (pg::physical-source-pin-confirmed-lsn loaded)))
      (ok (= 123 (pg::physical-source-pin-prefix-digest loaded)))
      (ok (= 456 (getf (pg::physical-follow-pin-anchor loaded) :overlap-digest)))
      (ok (equal (pg::physical-follow-pin-anchor pin) (pg::physical-follow-pin-anchor loaded)))))

(deftest follow-anchor-ready-format-preserves-advanced-confirmation
    (let ((pin (follow-anchor-pin 123 456 t)))
      (setf (pg::physical-source-pin-confirmed-lsn pin) 512
            (pg::physical-source-pin-restart-lsn pin) 512
            (pg::physical-source-pin-prefix-digest pin) 999)
      (let ((loaded (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
        (ok (eq :ready (pg::physical-follow-pin-stage loaded)))
        (ok (= 512 (pg::physical-source-pin-confirmed-lsn loaded)))
        (ok (= 999 (pg::physical-source-pin-prefix-digest loaded)))
        (ok (= 123 (getf (pg::physical-follow-pin-anchor loaded) :receive-digest))))))

(deftest follow-anchor-format-rejects-inconsistent-phase-and-progress
    (dolist (change (list (lambda (pin) (setf (pg::physical-source-pin-confirmed-lsn pin) 96))
                          (lambda (pin) (setf (pg::physical-source-pin-prefix-digest pin) 0))
                          (lambda (pin) (setf (pg::physical-source-pin-timeline pin) 3))
                          (lambda (pin) (setf (pg::physical-source-pin-manifest-digest pin) 78))
                          (lambda (pin) (setf (pg::physical-follow-pin-stage pin) :unknown))))
      (let ((pin (follow-anchor-pin 123 456)))
        (funcall change pin)
        (ok (signals (pg::physical-follow-pin-encode pin) 'error)))))

(deftest follow-anchor-request-rejects-empty-or-foreign-overlap
    (dolist (change
             (list (lambda (request) (setf (getf request :overlap-start) 96))
                   (lambda (request) (setf (getf request :receive-lsn) 97))
                   (lambda (request) (setf (getf request :base) 97))
                   (lambda (request) (setf (getf request :receive-timeline) 2))
                   (lambda (request) (setf (getf request :source-timeline) 3))
                   (lambda (request) (setf (getf request :eor-octets) '(0)))
                   (lambda (request) (setf (getf request :version) 2))))
      (let ((request (follow-anchor-request 123 456)))
        (funcall change request)
        (ok (signals (pg::physical-follow-anchor-check request) 'error)))))

(cl-spec:defproperty follow-anchor-format-roundtrips-independent-proof-fields
    ((seed (range integer 0 18446744073709551615))
     (overlap (range integer 0 18446744073709551615))
     (ready boolean))
  (:about pg::physical-follow-pin-encode)
  (:trials (:normal 200))
  (let* ((pin (follow-anchor-pin seed overlap ready))
         (loaded (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
    (and (eql (pg::physical-follow-pin-stage pin) (pg::physical-follow-pin-stage loaded))
         (= seed (pg::physical-source-pin-prefix-digest loaded))
         (= overlap (getf (pg::physical-follow-pin-anchor loaded) :overlap-digest))
         (equal (pg::physical-follow-pin-anchor pin) (pg::physical-follow-pin-anchor loaded)))))

(deftest generated-follow-anchor-proof-fields
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-anchor-format-roundtrips-independent-proof-fields :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun call-with-follow-anchor-source (function)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root) (pg::*catalog* (pg::make-catalog))
                               (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
                               (pg::*physical-source-pins* (make-hash-table :test #'equal))
                               (pg::*physical-source-senders* (make-hash-table :test #'equal))
                               (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                               (pg::*wal-insert-timeline* 2) (pg::*wal-checkpoint-timeline* 2)
                               (pg::*wal-segment-namespace* :timeline) (pg::*wal-poisoned* nil)
                               (pg::*pm-requested-mode* pg::+no-shutdown+) (pg::*max-slot-wal-keep-bytes* nil)
                               (prefix (concatenate '(vector (unsigned-byte 8))
                                                    (legacy-checkpoint-record 0) (legacy-checkpoint-record 32)))
                               (eor (pg::wal-encode-end-of-recovery-record 64 42 2 1 1))
                               (pg::*wal-durable-lsn* (+ 64 (length eor)))
                               (seed (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 32)))
                               (request (follow-anchor-request seed (pg::physical-store-update-digest #xCBF29CE484222325 prefix))))
      ;; These are decoder/retention fixtures, not a complete recoverable cluster.
      (setf (getf request :system-id) (pg::catalog-system-identifier)
            (getf request :base) 32 (getf request :receive-lsn) 64
            (getf request :history-text) (format nil "1~c0/00000040~cpromotion~%" #\Tab #\Tab)
            (getf request :eor-octets) (coerce eor 'list))
      (pg::wal-ensure-directory-durable (pg::wal-directory))
      (pg::wal-atomic-write-bytes
       (pg::wal-segment-path 0) (concatenate '(vector (unsigned-byte 8)) prefix eor))
      (pg::timeline-write-history 2 1 64 "promotion")
      (funcall function root request eor))))

(deftest follow-attempt-retry-and-cleanup-preserve-the-explicit-proof-kind
    (dolist (retained '(nil t))
      (call-with-follow-anchor-source
       (lambda (root descendant eor)
         (declare (ignore root eor))
         (let* ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
                (anchor (copy-tree descendant)))
           (when retained
             (remf anchor :eor-octets)
             (setf (getf anchor :kind) :retained-timeline))
           (let* ((request (pg::physical-follow-anchor-request anchor))
                  (other (copy-list request))
                  (pin (pg::physical-source-follow-reserve "kind_exact" anchor)))
             (if retained (remf other :kind)
                 (setf (getf other :kind) :retained-timeline))
             (ok (signals (pg::physical-source-follow-existing-anchor "kind_exact" other) 'error))
             (ok (signals (pg::physical-source-follow-abort "kind_exact" other) 'error))
             (ok (eq pin (gethash "kind_exact" pg::*physical-source-pins*)))
             (ok (probe-file (pg::physical-source-pin-path "kind_exact")))
             (ok (not pg::*wal-poisoned*))
             (ok (equal (pg::physical-follow-anchor-copy anchor)
                        (pg::physical-source-follow-existing-anchor "kind_exact" request)))
             (ok (eq :released (pg::physical-source-follow-abort "kind_exact" request)))
             (ok (not (gethash "kind_exact" pg::*physical-source-pins*)))))))))

(deftest retained-source-proof-and-reopen-do-not-require-child-eor
    (call-with-follow-anchor-source
     (lambda (root descendant eor)
       (declare (ignore root eor))
       (let* ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
              (request (pg::physical-follow-anchor-request descendant))
              (path (pg::wal-segment-path 0))
              (prefix (with-open-file (input path :element-type '(unsigned-byte 8))
                        (let ((bytes (make-array 64 :element-type '(unsigned-byte 8))))
                          (read-sequence bytes input)
                          bytes))))
         (setf (getf request :kind) :retained-timeline
               (getf request :receive-lsn) 32
               (getf request :receive-digest) #xCBF29CE484222325
               (getf request :overlap-digest)
               (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 0 32)))
         (pg::wal-atomic-write-bytes path prefix)
         (let ((pin (pg::physical-source-follow-prepare "bounded_ready" request)))
           (ok (pg::physical-follow-pin-ready-p pin))
           (ok (= 64 (pg::physical-follow-pin-end-lsn pin)))
           (let* ((command (pg::parse-replication-command
                            (pg::pg-client-native-prepare-query "bounded_ready" request)))
                  (row (pg::physical-follow-prepare-row command)))
             (ok (equal "2" (nth 12 row)))
             (ok (equal "0/00000040" (nth 13 row)))
             (ok (null (nth 14 row))))
           (sb-thread:with-mutex (pg::*wal-lock*)
             (let ((record (pg::physical-follow-source-read-next pin 32)))
               (ok (= 64 (pg::wal-record-end record))))
             (multiple-value-bind (record bytes end) (pg::physical-follow-source-read-next pin 64)
               (ok (null record)) (ok (null bytes)) (ok (= 64 end)))
             (ok (signals (pg::physical-follow-source-read-next pin 65) 'error)))
           (clrhash pg::*physical-source-pins*)
           (clrhash pg::*wal-snapshot-retention-pins*)
           (pg::physical-source-pin-load)
           (ok (pg::physical-follow-pin-ready-p
                (gethash "bounded_ready" pg::*physical-source-pins*)))
           (remf request :kind)
           (ok (signals (pg::physical-source-follow-prepare "missing_child" request) 'error)))))))

(deftest retained-source-sender-cannot-authorize-the-child-or-acknowledge-past-the-fork
    (call-with-follow-anchor-source
     (lambda (root descendant eor)
       (declare (ignore root eor))
       (let* ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
              (request (pg::physical-follow-anchor-request descendant))
              (seed #xCBF29CE484222325))
         (setf (getf request :kind) :retained-timeline
               (getf request :receive-lsn) 32
               (getf request :receive-digest) seed
               (getf request :overlap-digest)
               (pg::physical-store-update-digest seed (legacy-checkpoint-record 0)))
         (let* ((pin (pg::physical-source-follow-prepare "sibling" request))
                (command
                  (pg::parse-replication-command
                   (format nil "START_REPLICATION PHYSICAL 0/00000020 (proof_pin 'sibling', native_format '~a', system_id '~d', timeline '1', base_lsn '0/00000020', prefix_digest '~d', manifest_digest '77', mode 'follow')"
                           pg::+physical-native-format+ (getf request :system-id) seed)))
                (owner (pg::physical-source-acquire-sender command)))
           (unwind-protect
                (progn
                  (ok (= 32 (pg::physical-source-validate-resume command owner)))
                  (ok (signals (pg::physical-source-prove-range "sibling" owner 32 65 seed) 'error))
                  (ok (signals (pg::physical-source-prove-range "sibling" owner 65 65 seed) 'error))
                  (sb-thread:with-mutex (pg::*wal-lock*)
                    (ok (signals (pg::physical-source-validate-receipt-under-lock
                                  "sibling" owner 65 seed) 'error))
                    (ok (= 32 (pg::physical-source-pin-confirmed-lsn pin))))
                  (setf (pg::logical-replication-command-requested-lsn command) 65)
                  (ok (signals (pg::physical-source-validate-resume command owner) 'error))
                  (let ((digest (pg::physical-store-update-digest seed (legacy-checkpoint-record 32))))
                    (ok (= digest (pg::physical-source-prove-range "sibling" owner 32 64 seed)))
                    (pg::physical-source-confirm-feedback command owner 64 digest)
                    (ok (= 64 (pg::physical-source-pin-confirmed-lsn pin)))))
             (pg::physical-source-release-sender "sibling" owner))
           (setf (pg::logical-replication-command-requested-lsn command) 64)
           (let ((child (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)))
             (setf (cdr child) "'2'")
             (ok (signals (pg::physical-source-acquire-sender command) 'error))
             (ok (zerop (hash-table-count pg::*physical-source-senders*))))
           (ok (not pg::*wal-poisoned*)))))))

(deftest retained-source-at-the-fork-completes-without-copy-or-public-flush
    (call-with-follow-anchor-source
     (lambda (root descendant eor)
       (declare (ignore root eor))
       (let* ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
              (pg::*wal-replication-slots* (make-hash-table :test #'equal))
              (pg::*backend* (pg::make-backend :pid 3499))
              (request (pg::physical-follow-anchor-request descendant))
              (public (physical-slot-test-object :name "bounded_public"
                                                 :system-id (pg::catalog-system-identifier)))
              (names '(pg::send-copy-both-response pg::physical-source-read-next
                       pg::physical-stream-switch-result))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (switched nil))
         (setf (getf request :kind) :retained-timeline
               (gethash "bounded_public" pg::*wal-replication-slots*) public)
         (let* ((pin (pg::physical-source-follow-prepare "sibling" request))
                (command (follow-anchor-start-command request "follow")))
           (setf (pg::logical-replication-command-slot-name command) "bounded_public"
                 (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) "1")
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::send-copy-both-response)
                        (lambda (stream) (declare (ignore stream)) (error "unexpected CopyBoth at fork"))
                        (symbol-function 'pg::physical-source-read-next)
                        (lambda (&rest arguments) (declare (ignore arguments)) (error "unexpected read at fork"))
                        (symbol-function 'pg::physical-stream-switch-result)
                        (lambda (stream next limit)
                          (declare (ignore stream))
                          (ok (null (pg::wal-replication-slot-active-owner public)))
                          (ok (null (gethash "sibling" pg::*physical-source-senders*)))
                          (setf switched (list next limit))))
                  (pg::start-native-physical-replication nil command)
                  (ok (equal '(2 64) switched))
                  (ok (= 64 (pg::physical-source-pin-confirmed-lsn pin)))
                  (ok (null (pg::wal-replication-slot-restart-lsn public)))
                  (ok (null (pg::wal-replication-slot-restart-timeline public))))
             (dolist (entry saved)
               (setf (symbol-function (car entry)) (cdr entry)))))))))

(defun call-with-same-timeline-follow-source (function timeline)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root) (pg::*catalog* (pg::make-catalog))
                               (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
                               (pg::*physical-source-pins* (make-hash-table :test #'equal))
                               (pg::*physical-source-senders* (make-hash-table :test #'equal))
                               (pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
                               (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                               (pg::*wal-insert-timeline* timeline) (pg::*wal-checkpoint-timeline* timeline)
                               (pg::*wal-segment-namespace* (if (= timeline 1) :legacy :timeline))
                               (pg::*wal-poisoned* nil) (pg::*pm-requested-mode* pg::+no-shutdown+)
                               (pg::*max-slot-wal-keep-bytes* nil)
                               (prefix (concatenate '(vector (unsigned-byte 8))
                                                    (legacy-checkpoint-record 0) (legacy-checkpoint-record 32)
                                                    (if (= timeline 1) #()
                                                        (pg::wal-encode-end-of-recovery-record 64 42 2 1 1))))
                               (cursor (length prefix))
                               (wal (concatenate '(vector (unsigned-byte 8)) prefix (legacy-checkpoint-record cursor)))
                               (pg::*wal-durable-lsn* (length wal))
                               (anchor (same-timeline-follow-anchor
                                        cursor (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 32))
                                        (pg::physical-store-update-digest #xCBF29CE484222325 prefix) timeline)))
      (setf (getf anchor :base) 32 (getf anchor :system-id) (pg::catalog-system-identifier))
      (pg::wal-ensure-directory-durable (pg::wal-directory))
      (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) wal)
      (when (= timeline 2) (pg::timeline-write-history 2 1 64 "promotion"))
      (funcall function root anchor wal))))

(deftest same-timeline-source-preparation-proves-real-wal-and-reopens-ready-pin
    (dolist (timeline '(1 2))
      (call-with-same-timeline-follow-source
       (lambda (root anchor wal)
         (declare (ignore root wal))
         (let* ((coordinates (follow-anchor-receiver-request anchor))
                (pin (pg::physical-source-follow-prepare "same_ready" coordinates)))
           (ok (pg::physical-follow-pin-ready-p pin))
           (ok (equal anchor (pg::physical-follow-pin-anchor pin)))
           (ok (= (getf anchor :receive-lsn) (pg::physical-source-pin-restart-lsn pin)))
           (ok (eq pin (pg::physical-source-follow-prepare "same_ready" coordinates)))
           (clrhash pg::*physical-source-pins*)
           (clrhash pg::*wal-snapshot-retention-pins*)
           (pg::physical-source-pin-load)
           (ok (pg::physical-follow-pin-ready-p (gethash "same_ready" pg::*physical-source-pins*)))
           (ok (equal anchor (pg::physical-follow-pin-anchor
                              (gethash "same_ready" pg::*physical-source-pins*))))))
       timeline)))

(deftest same-timeline-source-rejects-unproved-bytes-and-incomplete-records
    (dolist (kind '(:digest :boundary :future))
      (call-with-same-timeline-follow-source
       (lambda (root anchor wal)
         (declare (ignore root))
         (ecase kind
           (:digest (incf (getf anchor :overlap-digest)))
           (:boundary
            (let ((cursor (1- (getf anchor :receive-lsn))))
              (setf (getf anchor :receive-lsn) cursor
                    (getf anchor :overlap-digest)
                    (pg::physical-store-update-digest #xCBF29CE484222325 (subseq wal 0 cursor)))))
           (:future (setf (getf anchor :receive-lsn) (1+ pg::*wal-durable-lsn*))))
         (ok (intake-fails-p
              (lambda () (pg::physical-source-follow-prepare
                          "same_rejected" (follow-anchor-receiver-request anchor)))))
         (let ((pin (gethash "same_rejected" pg::*physical-source-pins*)))
           (if (eq kind :future)
               (ok (null pin))
               (progn (ok pin) (ok (not (pg::physical-follow-pin-ready-p pin))))))
         (ok (not pg::*wal-poisoned*)))
       1)))

(deftest legacy-source-namespace-is-limited-to-same-timeline-one
    (call-with-follow-anchor-source
     (lambda (root anchor eor)
       (declare (ignore root eor))
       (let ((pg::*wal-segment-namespace* :legacy))
         (ok (intake-fails-p
              (lambda () (pg::physical-source-follow-reserve "legacy_descendant" anchor))))
         (ok (zerop (hash-table-count pg::*physical-source-pins*))))))
  (call-with-same-timeline-follow-source
   (lambda (root anchor wal)
     (declare (ignore root wal))
     (let ((pg::*wal-segment-namespace* :legacy))
       (ok (intake-fails-p
            (lambda () (pg::physical-source-follow-prepare
                        "legacy_two" (follow-anchor-receiver-request anchor)))))
       (ok (zerop (hash-table-count pg::*physical-source-pins*)))))
   2))

(defun follow-anchor-receiver-request (request)
  (loop for (key value) on request by #'cddr
        unless (member key '(:kind :history-text :eor-octets))
          append (list key value)))

(defun follow-prepare-query (request &optional (name "wire_prepare"))
  (format nil "PREPARE_FOLLOW SLOT ~a (native_format '~a', system_id '~d', base_timeline '~d', base_lsn '~a', manifest_digest '~d', source_timeline '~d', receive_timeline '~d', receive_lsn '~a', prefix_digest '~d', overlap_start '~a', overlap_digest '~d')"
          name pg::+physical-native-format+ (getf request :system-id)
          (getf request :base-timeline) (pg::wal-format-lsn (getf request :base))
          (getf request :manifest-digest) (getf request :source-timeline)
          (getf request :receive-timeline) (pg::wal-format-lsn (getf request :receive-lsn))
          (getf request :receive-digest) (pg::wal-format-lsn (getf request :overlap-start))
          (getf request :overlap-digest)))

(deftest follow-prepare-wire-parser-preserves-receiver-coordinates
    (let* ((request (follow-anchor-request 18446744073709551615 456))
           (command (pg::parse-replication-command (follow-prepare-query request))))
      (ok (eq :physical-prepare (pg::logical-replication-command-type command)))
      (ok (equal "wire_prepare" (pg::logical-replication-command-slot-name command)))
      (ok (equal (follow-anchor-receiver-request request)
                 (pg::physical-follow-prepare-request command)))
      (ok (pg::replication-query-payload-p
           (pg::string->bytes (concatenate 'string (follow-prepare-query request) (string (code-char 0))))))))

(deftest follow-prepare-wire-obeys-transaction-and-recovery-context
    (let ((pg::*physical-recovery-state* nil)
          (pg::*physical-recovery-failed-p* nil)
          (pg::*backend* (pg::make-backend :explicit t :state :in-progress))
          (command (pg::parse-replication-command
                    (follow-prepare-query (follow-anchor-request 123 456)))))
      (ok (equal "25001" (recovery-state-of
                          (lambda () (pg::validate-replication-command-context command)))))
      (setf (pg::backend-explicit pg::*backend*) nil
            pg::*physical-recovery-state* (list :phase 2))
      (ok (equal "25006" (recovery-state-of
                          (lambda () (pg::validate-replication-command-context command)))))
      (setf (pg::backend-state pg::*backend*) :aborted)
      (ok (equal "25P02" (recovery-state-of
                          (lambda () (pg::validate-replication-command-context command)))))))

(deftest follow-prepare-wire-result-requires-durable-ready-and-returns-anchor
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((command (pg::parse-replication-command (follow-prepare-query request)))
              (row (pg::physical-follow-prepare-row command))
              (pin (gethash "wire_prepare" pg::*physical-source-pins*)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (pg::physical-follow-pin-ready-p
              (pg::physical-follow-pin-decode
               (lineage-test-bytes (pg::physical-source-pin-path "wire_prepare")))))
         (ok (equal (list "wire_prepare" (format nil "~d" (getf request :system-id))
                          "1" "0/00000020" "77" "2" "1" "0/00000040"
                          (format nil "~d" (getf request :receive-digest))
                          "0/00000000" (format nil "~d" (getf request :overlap-digest))
                          (getf request :history-text) "2" "0/00000040"
                          (format nil "~(~{~2,'0X~}~)" (coerce eor 'list)))
                    row))))))

(deftest follow-prepare-wire-parser-rejects-extra-missing-and-invalid-options
    (let ((query (follow-prepare-query (follow-anchor-request 123 456))))
      (dolist (case
                  (list (list "native_format '[^']+'" "native_format 'wrong'" "0A000")
                        (list ", system_id '[^']+'" "" "0A000")
                        (list "source_timeline '2'" "source_timeline '0'" "22023")
                        (list "receive_timeline '1'" "receive_timeline '4294967296'" "22023")
                        (list "prefix_digest '123'" "prefix_digest '-1'" "22023")
                        (list "prefix_digest '123'" "prefix_digest '18446744073709551616'" "22023")
                        (list "base_lsn '[^']+'" "base_lsn 'bad'" "22P02")
                        (list "\\)$" ", system_id '42')" "0A000")
                        (list "\\)$" ", history 'forged')" "0A000")
                        (list "\\)$" ", eor_hex '00')" "0A000")))
        (destructuring-bind (pattern replacement state) case
          (ok (equal state
                     (handler-case
                         (progn (pg::parse-replication-command
                                 (cl-ppcre:regex-replace pattern query replacement)) nil)
                       (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))))))

(deftest follow-prepare-wire-does-not-return-a-row-after-failed-proof
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (incf (getf request :overlap-digest))
       (let ((command (pg::parse-replication-command (follow-prepare-query request))))
         (ok (equal "55000"
                    (handler-case (progn (pg::physical-follow-prepare-row command) nil)
                      (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))
         (let ((pin (gethash "wire_prepare" pg::*physical-source-pins*)))
           (ok pin)
           (ng (pg::physical-follow-pin-ready-p pin))
           (ng (pg::physical-source-pin-confirmed-lsn pin)))))))

(deftest follow-prepare-wire-returns-no-row-on-ambiguous-ready-publication
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pg::*physical-source-test-hook*
               (lambda (point pin)
                 (when (and (eq point :after-save)
                            (pg::physical-follow-pin-ready-p pin))
                   (error "ambiguous ready publication")))))
         (ok (equal "XX000"
                    (recovery-state-of
                     (lambda () (pg::physical-follow-prepare-row
                                 (pg::parse-replication-command (follow-prepare-query request)))))))
         (ok pg::*wal-poisoned*)
         (ng (pg::physical-follow-pin-ready-p
              (gethash "wire_prepare" pg::*physical-source-pins*)))))))

(deftest follow-prepare-wire-reports-unavailable-source-eor-as-object-state
    (dolist (kind '(:missing :truncated :corrupt))
      (call-with-follow-anchor-source
       (lambda (root request eor)
         (declare (ignore root))
         (let ((path (pg::wal-segment-path 0)))
           (ecase kind
             (:missing (delete-file path))
             (:truncated (pg::wal-atomic-write-bytes
                          path (make-array (+ 64 (1- (length eor)))
                                           :element-type '(unsigned-byte 8) :initial-element 0)))
             (:corrupt (pg::wal-atomic-write-bytes
                        path (make-array (+ 64 (length eor))
                                         :element-type '(unsigned-byte 8) :initial-element 0)))))
         (ok (equal "55000"
                    (recovery-state-of
                     (lambda () (pg::physical-follow-prepare-row
                                 (pg::parse-replication-command (follow-prepare-query request)))))))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ng pg::*wal-poisoned*)))))

(deftest follow-preparation-builds-source-owned-history-and-eor
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((receiver (follow-anchor-receiver-request request))
              (pin (pg::physical-source-follow-prepare "prepared" receiver)))
         (ok (equal request (pg::physical-follow-pin-anchor pin)))
         (ok (equal (coerce eor 'list)
                    (getf (pg::physical-follow-pin-anchor pin) :eor-octets)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (eq pin (pg::physical-source-follow-prepare "prepared" receiver)))
         (ok (pg::physical-follow-pin-ready-p
              (pg::physical-follow-pin-decode
               (pg::physical-follow-pin-encode pin)))))
       (ok (= 1 (hash-table-count pg::*physical-source-pins*))))))

(deftest follow-preparation-selects-immediate-successor-with-fresh-pins
    (call-with-follow-anchor-source
     (lambda (root request first-eor)
       (declare (ignore root))
       (let* ((fork pg::*wal-durable-lsn*)
              (last-eor (pg::wal-encode-end-of-recovery-record fork 43 3 2 1))
              (prefix (lineage-test-bytes (pg::wal-segment-path 0)))
              (pg::*wal-insert-timeline* 3)
              (pg::*wal-checkpoint-timeline* 3)
              (pg::*wal-durable-lsn* (+ fork (length last-eor))))
         (pg::timeline-write-history 3 2 fork "second promotion")
         (pg::wal-atomic-write-bytes
          (pg::wal-segment-path 0)
          (concatenate '(vector (unsigned-byte 8)) prefix last-eor))
         (let ((receiver (follow-anchor-receiver-request request)))
           (setf (getf receiver :source-timeline) 3)
           (let ((first (pg::physical-source-follow-prepare "first_fork" receiver)))
             (ok (equal (coerce first-eor 'list)
                        (getf (pg::physical-follow-pin-anchor first) :eor-octets))))
           (setf (getf receiver :receive-timeline) 2
                 (getf receiver :receive-lsn) fork
                 (getf receiver :receive-digest)
                 (pg::physical-store-update-digest (getf request :receive-digest) first-eor)
                 (getf receiver :overlap-digest)
                 (pg::physical-store-update-digest #xCBF29CE484222325 prefix))
           (let ((second (pg::physical-source-follow-prepare "second_fork" receiver)))
             (ok (equal (coerce last-eor 'list)
                        (getf (pg::physical-follow-pin-anchor second) :eor-octets))))
           (ok (= 2 (hash-table-count pg::*physical-source-pins*))))))))

(deftest follow-preparation-rejects-client-descriptors-and-foreign-coordinates
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (dolist (change
                (list (lambda (r) (append r (list :history-text "")))
                      (lambda (r) (append r (list :eor-octets nil)))
                      (lambda (r) (setf (getf r :source-timeline) 3) r)
                      (lambda (r) (setf (getf r :receive-timeline) 3) r)
                      (lambda (r) (setf (getf r :receive-lsn) 65) r)
                      (lambda (r) (setf (getf r :system-id) 0) r)
                      (lambda (r) (setf (getf r :receive-lsn) "64") r)))
         (ok (signals
              (pg::physical-source-follow-prepare
               "rejected" (funcall change (follow-anchor-receiver-request request)))
              'error))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ng (probe-file (pg::physical-source-pin-path "rejected")))))))

(deftest follow-preparation-ready-retry-does-not-reread-released-anchor-bytes
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((receiver (follow-anchor-receiver-request request))
              (pin (pg::physical-source-follow-prepare "retry" receiver))
              (owner (gensym))
              (digest (pg::physical-store-update-digest (getf request :receive-digest) eor)))
         (setf (gethash "retry" pg::*physical-source-senders*) owner)
         (pg::physical-source-confirm-receipt "retry" owner pg::*wal-durable-lsn* digest)
         (remhash "retry" pg::*physical-source-senders*)
         (let ((read-segment (symbol-function 'pg::physical-follow-source-segment)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-source-segment)
                        (lambda (&rest args) (declare (ignore args))
                          (error "released original anchor bytes must not be reread")))
                  (ok (eq pin (pg::physical-source-follow-prepare "retry" receiver)))
                  (ok (= pg::*wal-durable-lsn* (pg::physical-source-pin-confirmed-lsn pin)))
                  (ok (= digest (pg::physical-source-pin-prefix-digest pin)))
                  (setf (getf receiver :receive-digest) (1+ (getf receiver :receive-digest)))
                  (ok (signals (pg::physical-source-follow-prepare "retry" receiver)
                               'pg::wal-input-validation-error)))
             (setf (symbol-function 'pg::physical-follow-source-segment) read-segment)))))))

(deftest follow-preparation-rechecks-concurrently-published-ready-pin
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((receiver (follow-anchor-receiver-request request))
             (construct (symbol-function 'pg::physical-source-follow-anchor))
             (published nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-source-follow-anchor)
                      (lambda (ignored)
                        (declare (ignore ignored))
                        (setf published (pg::physical-source-follow-ready "concurrent" request))
                        (error "original EOR was released before delayed descriptor read")))
                (ok (eq (pg::physical-source-follow-prepare "concurrent" receiver) published))
                (ok (pg::physical-follow-pin-ready-p published)))
           (setf (symbol-function 'pg::physical-source-follow-anchor) construct))))))

(deftest follow-preparation-rejects-missing-truncated-or-corrupt-source-eor
    (dolist (kind '(:missing :truncated :corrupt))
      (call-with-follow-anchor-source
       (lambda (root request eor)
         (declare (ignore root))
         (let ((path (pg::wal-segment-path 0)))
           (ecase kind
             (:missing (delete-file path))
             (:truncated (pg::wal-atomic-write-bytes
                          path (make-array (+ 64 (1- (length eor)))
                                           :element-type '(unsigned-byte 8) :initial-element 0)))
             (:corrupt (pg::wal-atomic-write-bytes
                        path (make-array (+ 64 (length eor))
                                         :element-type '(unsigned-byte 8) :initial-element 0)))))
         (ok (signals (pg::physical-source-follow-prepare
                       "bad_eor" (follow-anchor-receiver-request request)) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ng (probe-file (pg::physical-source-pin-path "bad_eor")))))))

(deftest follow-anchor-source-publishes-proof-before-ready-and-reloads
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pin (pg::physical-source-follow-reserve "sibling" request)))
         (ok (eq :retaining (pg::physical-follow-pin-stage pin)))
         (ng (pg::physical-source-pin-stream-ready-p pin))
         (ok (= 0 (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ok (eq pin (pg::physical-source-follow-ready "sibling" request)))
         (ok (pg::physical-source-pin-stream-ready-p pin))
         (ok (= 64 (gethash pin pg::*wal-snapshot-retention-pins*))))
       (clrhash pg::*physical-source-pins*)
       (clrhash pg::*wal-snapshot-retention-pins*)
       (pg::physical-source-pin-load)
       (let ((loaded (gethash "sibling" pg::*physical-source-pins*)))
         (ok (pg::physical-follow-pin-ready-p loaded))
         (ok (= 64 (pg::physical-source-pin-confirmed-lsn loaded)))
         (ok (equal request (pg::physical-follow-pin-anchor loaded)))))))

(deftest follow-anchor-bad-overlap-keeps-retention-without-granting-stream
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (setf (getf request :overlap-digest) (logxor 1 (getf request :overlap-digest)))
       (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
       (let ((pin (gethash "sibling" pg::*physical-source-pins*)))
         (ok (eq :retaining (pg::physical-follow-pin-stage pin)))
         (ng (pg::physical-source-pin-stream-ready-p pin))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ok (= 0 (gethash pin pg::*wal-snapshot-retention-pins*))))
       (ng pg::*wal-poisoned*))))

(deftest follow-anchor-ready-retry-preserves-confirmed-digest-and-rejects-rebinding
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((pin (pg::physical-source-follow-ready "sibling" request))
              (owner (gensym "FOLLOW-SENDER"))
              (next (pg::physical-store-update-digest (getf request :receive-digest) eor))
              (proof (symbol-function 'pg::physical-follow-pin-proof-digest)))
         (setf (gethash "sibling" pg::*physical-source-senders*) owner)
         (pg::physical-source-confirm-receipt "sibling" owner pg::*wal-durable-lsn* next)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-pin-proof-digest)
                      (lambda (ignored) (declare (ignore ignored)) (error "old overlap was reread")))
                (ok (eq pin (pg::physical-source-follow-ready "sibling" request)))
                (ok (= pg::*wal-durable-lsn* (pg::physical-source-pin-confirmed-lsn pin))))
           (setf (symbol-function 'pg::physical-follow-pin-proof-digest) proof))
         (ok (= next (pg::physical-source-pin-prefix-digest pin)))
         (incf (getf request :receive-digest))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
         (ok (= next (pg::physical-source-pin-prefix-digest pin)))))))

(deftest follow-anchor-reservation-owns-its-name-and-request
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((name (copy-seq "sibling"))
              (original (pg::physical-follow-anchor-copy request))
              (pin (pg::physical-source-follow-reserve name request)))
         (setf (char name 0) #\x
               (char (getf request :history-text) 0) #\9
               (first (getf request :eor-octets)) 0)
         (ok (eq pin (gethash "sibling" pg::*physical-source-pins*)))
         (ok (equal "sibling" (pg::physical-source-pin-name pin)))
         (ok (equal original (pg::physical-follow-pin-anchor pin)))))))

(deftest follow-anchor-lost-retention-cannot-be-readied-or-rebound
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((pin (pg::physical-source-follow-reserve "sibling" request))
              (pg::*max-slot-wal-keep-bytes* 1))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::physical-source-invalidate-lagging-pins-under-wal-lock))
         (ok (pg::physical-source-pin-lost-p pin))
         (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (pg::physical-source-pin-load)
         (ok (pg::physical-source-pin-lost-p (gethash "sibling" pg::*physical-source-pins*)))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))))))

(deftest follow-anchor-loss-after-proof-prevents-ready-publication
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pg::*max-slot-wal-keep-bytes* 1)
             (pg::*physical-source-test-hook*
               (lambda (point pin)
                 (declare (ignore pin))
                 (when (eq point :after-follow-proof)
                   (sb-thread:with-mutex (pg::*wal-lock*)
                     (pg::physical-source-invalidate-lagging-pins-under-wal-lock))))))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error)))
       (let ((pin (gethash "sibling" pg::*physical-source-pins*)))
         (ok (pg::physical-source-pin-lost-p pin))
         (ng (pg::physical-follow-pin-ready-p pin))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ng pg::*wal-poisoned*)))))

(deftest follow-anchor-cannot-use-bootstrap-stream-admission
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (pg::physical-source-follow-reserve "sibling" request)
       (let ((command
               (pg::parse-replication-command
                (format nil
                        "START_REPLICATION PHYSICAL 0/00000040 (proof_pin 'sibling', native_format '~a', system_id '~d', timeline '2', base_lsn '0/00000040', prefix_digest '~d', manifest_digest '77')"
                        pg::+physical-native-format+ (getf request :system-id)
                        (getf request :receive-digest)))))
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (pg::physical-source-follow-ready "sibling" request)
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))))))

(defun follow-anchor-start-command (request &optional mode)
  (pg::parse-replication-command
   (format nil
           "START_REPLICATION PHYSICAL ~a (proof_pin 'sibling', native_format '~a', system_id '~d', timeline '2', base_lsn '~a', prefix_digest '~d', manifest_digest '77'~a)"
           (pg::wal-format-lsn (getf request :receive-lsn))
           pg::+physical-native-format+ (getf request :system-id)
           (pg::wal-format-lsn (if (equal mode "follow") (getf request :base) 64))
           (getf request :receive-digest)
           (if mode (format nil ", mode '~a'" mode) ""))))

(deftest follow-anchor-stream-requires-explicit-ready-mode-and-exclusive-owner
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root))
       (let* ((pin (pg::physical-source-follow-reserve "sibling" request))
              (command (follow-anchor-start-command request "follow")))
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (pg::physical-source-follow-ready "sibling" request)
         (let ((owner (pg::physical-source-acquire-sender command)))
           (unwind-protect
                (progn
                  (ok (eq owner (gethash "sibling" pg::*physical-source-senders*)))
                  (ok (signals (pg::physical-source-acquire-sender command) 'error))
                  (ok (= 64 (pg::physical-source-validate-resume command owner)))
                  (ok (= 64 (pg::physical-source-pin-confirmed-lsn pin)))
                  (multiple-value-bind (record raw durable)
                      (pg::physical-source-read-next "sibling" owner 64)
                    (ok (= pg::+wal-record-end-of-recovery+ (pg::wal-record-type record)))
                    (ok (equalp raw eor))
                    (ok (= 120 durable))))
             (pg::physical-source-release-sender "sibling" owner)))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))))))

(deftest follow-stream-timeline-is-requested-ancestry-with-an-exact-fork-limit
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (pg::physical-source-follow-ready "sibling" request)
       (let ((command (follow-anchor-start-command request "follow")))
         (setf (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) "1")
         (multiple-value-bind (owner limit next) (pg::physical-source-acquire-sender command)
           (unwind-protect (progn (ok (= 64 limit)) (ok (= 2 next)))
             (pg::physical-source-release-sender "sibling" owner)))
         (setf (pg::logical-replication-command-requested-lsn command) 65)
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (setf (pg::logical-replication-command-requested-lsn command) 64
               (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) "3")
         (ok (signals (pg::physical-source-acquire-sender command) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))))))

(cl-spec:defproperty follow-stream-boundaries-preserve-c-parent-limit-and-child-prefix
    ((fork (range integer 16777216 18446744073709551614))
     (child (range integer 2 4294967295))
     (case-number (range integer 0 5)))
  (:about pg::physical-follow-stream-boundary)
  (:trials (:normal 200))
  (let* ((parent-request (< case-number 3))
         (origin (* (floor fork pg::+wal-segment-size+) pg::+wal-segment-size+))
         (cursor (ecase case-number
                   (0 (1- fork)) (1 fork) (2 (1+ fork))
                   (3 fork) (4 origin) (5 (1- origin))))
         (accepted (member case-number '(0 1 3 4)))
         (pin (pg::make-physical-follow-pin
               :timeline child :anchor (list :history-text (store-history-text 1 fork))))
         (command (pg::make-logical-replication-command
                   :requested-lsn cursor
                   :options (list (cons "timeline" (format nil "~d" (if parent-request 1 child)))))))
    (handler-case
        (multiple-value-bind (limit next) (pg::physical-follow-stream-boundary command pin)
          (and accepted (if parent-request (and (= fork limit) (= child next))
                            (and (null limit) (null next)))))
      (pg::pg-error (condition)
        (and (not accepted) (equal "55000" (pg::pg-error-sqlstate condition)))))))

(deftest generated-follow-stream-timeline-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property
                  'follow-stream-boundaries-preserve-c-parent-limit-and-child-prefix :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest historical-stream-stops-before-eor-and-reports-next-timeline-without-copy-at-fork
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (setf (getf request :receive-lsn) 32
             (getf request :receive-digest) #xCBF29CE484222325
             (getf request :overlap-digest)
             (pg::physical-store-update-digest #xCBF29CE484222325 (legacy-checkpoint-record 0)))
       (let* ((pin (pg::physical-source-follow-ready "sibling" request))
              (command (follow-anchor-start-command request "follow"))
              (names '(pg::physical-stream-send-data pg::physical-stream-feedback
                       pg::send-copy-both-response pg::physical-stream-switch-result
                       pg::physical-source-read-next))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (reads 0) (sent nil) (switched nil))
         (setf (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) "1")
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-stream-send-data)
                      (lambda (stream start end bytes)
                        (declare (ignore stream))
                        (push (list start end bytes) sent))
                      (symbol-function 'pg::physical-stream-feedback)
                      (lambda (stream) (declare (ignore stream))
                        (list :write 64 :flush 64 :apply 32))
                      (symbol-function 'pg::physical-source-read-next)
                      (lambda (name owner cursor)
                        (incf reads)
                        (when (>= cursor 64) (error "historical stream attempted child WAL"))
                        (funcall (cdr (assoc 'pg::physical-source-read-next saved)) name owner cursor)))
                (multiple-value-bind (owner limit next) (pg::physical-source-acquire-sender command)
                  (unwind-protect
                       (progn
                         (ok (= 64 limit)) (ok (= 2 next))
                         (ok (eq :switch (pg::physical-stream-send-loop nil command owner limit))))
                    (pg::physical-source-release-sender "sibling" owner)))
                (ok (= 2 reads))
                (ok (= 1 (length sent)))
                (ok (= 32 (first (first sent))))
                (ok (= 64 (second (first sent))))
                (ok (equalp (third (first sent)) (legacy-checkpoint-record 32)))
                (ok (= 64 (pg::physical-source-pin-confirmed-lsn pin)))
                (setf (pg::logical-replication-command-requested-lsn command) 64
                      (cdr (assoc "prefix_digest" (pg::logical-replication-command-options command)
                                  :test #'equal))
                      (format nil "~d" (pg::physical-source-pin-prefix-digest pin))
                      (symbol-function 'pg::send-copy-both-response)
                      (lambda (stream) (declare (ignore stream)) (error "unexpected CopyBoth at fork"))
                      (symbol-function 'pg::physical-stream-switch-result)
                      (lambda (stream next limit) (declare (ignore stream)) (setf switched (list next limit))))
                (pg::start-native-physical-replication nil command)
                (ok (equal '(2 64) switched))
                (ok (= 2 reads))
                (ok (zerop (hash-table-count pg::*physical-source-senders*))))
           (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))))

(deftest physical-follow-copy-done-during-proof-finishes-without-prefix-confirmation
    (dolist (timeline '("1" "2"))
      (call-with-follow-anchor-source
       (lambda (root request eor)
         (declare (ignore root eor))
         (setf (getf request :receive-lsn) 32
               (getf request :receive-digest) #xCBF29CE484222325
               (getf request :overlap-digest)
               (pg::physical-store-update-digest #xCBF29CE484222325 (legacy-checkpoint-record 0)))
         (let* ((pin (pg::physical-source-follow-ready "sibling" request))
                (command (follow-anchor-start-command request "follow"))
                (pg::*backend* (pg::make-backend :state :idle))
                (names '(pg::send-copy-both-response pg::physical-stream-feedback
                         pg::physical-stream-send-keepalive pg::physical-stream-send-data
                         pg::write-message pg::physical-stream-switch-result
                         pg::send-command-complete pg::send-ready-for-query))
                (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
                (events nil))
           (setf (cdr (assoc "timeline" (pg::logical-replication-command-options command) :test #'equal)) timeline
                 (cdr (assoc "prefix_digest" (pg::logical-replication-command-options command) :test #'equal)) "0")
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::send-copy-both-response)
                        (lambda (stream) (declare (ignore stream)) (push :copy events))
                        (symbol-function 'pg::physical-stream-feedback)
                        (lambda (stream) (declare (ignore stream)) (values nil t))
                        (symbol-function 'pg::physical-stream-send-keepalive)
                        (lambda (stream end) (declare (ignore stream end)))
                        (symbol-function 'pg::physical-stream-send-data)
                        (lambda (&rest arguments) (declare (ignore arguments)) (error "sent unproved WAL"))
                        (symbol-function 'pg::write-message)
                        (lambda (stream kind bytes)
                          (declare (ignore stream))
                          (ok (zerop (length bytes))) (push kind events))
                        (symbol-function 'pg::physical-stream-switch-result)
                        (lambda (stream next limit)
                          (declare (ignore stream))
                          (ok (zerop (hash-table-count pg::*physical-source-senders*)))
                          (push (list next limit) events))
                        (symbol-function 'pg::send-command-complete)
                        (lambda (stream tag) (declare (ignore stream)) (push tag events))
                        (symbol-function 'pg::send-ready-for-query)
                        (lambda (stream state) (declare (ignore stream))
                          (ok (zerop (hash-table-count pg::*physical-source-senders*)))
                          (push state events)))
                  (pg::start-native-physical-replication nil command)
                  (ok (equal (reverse events)
                             (if (equal timeline "1") '(:copy #\c (2 64))
                                 '(:copy #\c "START_STREAMING" #\I))))
                  (ok (= 32 (pg::physical-source-pin-confirmed-lsn pin)))
                  (ok (= #xCBF29CE484222325 (pg::physical-source-pin-prefix-digest pin))))
             (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))))))))

(deftest follow-stream-mode-cannot-acquire-a-bootstrap-pin
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pin (pg::make-physical-source-pin
                   :name "sibling" :system-id (getf request :system-id) :timeline 2
                   :manifest-digest 77 :consistent-lsn 64 :restart-lsn 32
                   :prefix-digest #xCBF29CE484222325)))
         (setf (gethash "sibling" pg::*physical-source-pins*) pin)
         (ok (signals (pg::physical-source-acquire-sender
                       (follow-anchor-start-command request "follow")) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-senders*)))
         (let ((owner (pg::physical-source-acquire-sender (follow-anchor-start-command request))))
           (pg::physical-source-release-sender "sibling" owner))))))

(deftest physical-stream-mode-rejects-unknown-and-duplicate-values
    (let ((request (follow-anchor-request 123 456)))
      (dolist (mode '("" "other" "FOLLOW" "bootstrap follow"))
        (ok (signals (follow-anchor-start-command request mode) 'error)))
      (let* ((query (format nil
                            "START_REPLICATION PHYSICAL 0/40 (proof_pin 'sibling', native_format '~a', system_id '42', timeline '2', base_lsn '0/40', prefix_digest '123', manifest_digest '77', mode 'follow', mode 'follow')"
                            pg::+physical-native-format+)))
        (ok (signals (pg::parse-replication-command query) 'error)))))

(deftest follow-anchor-unproved-retention-cannot-confirm-receipt
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pin (pg::physical-source-follow-reserve "sibling" request))
             (owner (gensym "UNPROVED-SENDER")))
         (setf (gethash "sibling" pg::*physical-source-senders*) owner)
         (ok (signals (pg::physical-source-confirm-receipt "sibling" owner 64
                                                           (getf request :receive-digest)) 'error))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ng (pg::physical-follow-pin-ready-p pin))
         (ng pg::*wal-poisoned*)))))

(deftest follow-anchor-ambiguous-ready-publication-recovers-from-disk
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pg::*physical-source-test-hook*
               (lambda (point pin)
                 (when (and (eq point :after-save)
                            (eq :ready (pg::physical-follow-pin-stage pin)))
                   (error "injected post-ready-save failure")))))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error)))
       (let ((pin (gethash "sibling" pg::*physical-source-pins*))
             (disk (sb-thread:with-mutex (pg::*wal-lock*)
                     (pg::physical-source-pin-read (pg::physical-source-pin-path "sibling")))))
         (ok pg::*wal-poisoned*)
         (ng (pg::physical-follow-pin-ready-p pin))
         (ok (pg::physical-follow-pin-ready-p disk))
         (ok (= 0 (gethash pin pg::*wal-snapshot-retention-pins*))))
       ;; A new owning process clears poison and reconstructs the durable registry.
       (clrhash pg::*physical-source-pins*)
       (clrhash pg::*wal-snapshot-retention-pins*)
       (setf pg::*wal-poisoned* nil pg::*pm-requested-mode* pg::+no-shutdown+)
       (pg::physical-source-pin-load)
       (let ((pin (gethash "sibling" pg::*physical-source-pins*)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (= 64 (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (eq pin (pg::physical-source-follow-ready "sibling" request)))))))

(deftest restored-ready-follow-pin-rejects-a-truncated-live-tail
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (pg::physical-source-follow-ready "sibling" request)
       (let* ((path (pg::wal-segment-path 0))
              (bytes (lineage-test-bytes path)))
         (unwind-protect
              (progn
                (pg::wal-atomic-write-bytes path (subseq bytes 0 (1- (length bytes))))
                (ok (signals
                     (sb-thread:with-mutex (pg::*wal-lock*)
                       (pg::physical-source-pin-read (pg::physical-source-pin-path "sibling")))
                     'error)))
           (pg::wal-atomic-write-bytes path bytes))))))

(deftest follow-anchor-envelope-rejects-corruption-and-extra-forms
    (let ((bytes (pg::physical-follow-pin-encode (follow-anchor-pin 123 456))))
      (setf (aref bytes (1- (length bytes))) (logxor 1 (aref bytes (1- (length bytes)))))
      (ok (signals (pg::physical-follow-pin-decode bytes) 'error)))
  (let* ((payload (pg::physical-follow-pin-payload (follow-anchor-pin 123 456)))
         (body (pg::string->bytes
                (concatenate 'string (pg::physical-bootstrap-manifest-text payload) " NIL")))
         (header (pg::string->bytes
                  (format nil "PGPHYSFOLLOW1 ~d ~8,'0X~%" (length body) (pg::wal-crc32 body)))))
    (ok (signals (pg::physical-follow-pin-decode
                  (concatenate '(vector (unsigned-byte 8)) header body)) 'error))))

(deftest follow-anchor-byte-digest-alone-does-not-prove-a-record-end
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((prefix (concatenate '(vector (unsigned-byte 8))
                                  (legacy-checkpoint-record 0) (legacy-checkpoint-record 32))))
         (setf (getf request :receive-lsn) 48
               (getf request :overlap-digest)
               (pg::physical-store-update-digest #xCBF29CE484222325 (subseq prefix 0 48)))
         (ok (signals (pg::physical-source-follow-ready "interior" request) 'error)))
       (let ((pin (gethash "interior" pg::*physical-source-pins*)))
         (ng (pg::physical-follow-pin-ready-p pin))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin))))
       (ng pg::*wal-poisoned*))))

(deftest follow-anchor-format-distinguishes-base-receipt-and-source-timelines
    (let* ((request (follow-anchor-request 123 456))
           (raw (pg::wal-encode-end-of-recovery-record 160 42 3 2 1)))
      (setf (getf request :source-timeline) 3
            (getf request :receive-timeline) 2 (getf request :receive-lsn) 152
            (getf request :history-text)
            (format nil "1~c0/00000060~cpromotion~%2~c0/000000A0~cpromotion~%"
                    #\Tab #\Tab #\Tab #\Tab)
            (getf request :eor-octets) (coerce raw 'list))
      (multiple-value-bind (history fork record) (pg::physical-follow-anchor-check request)
        (declare (ignore history record))
        (ok (= 160 fork)))
      (let* ((pin (pg::make-physical-follow-pin
                   :name "later" :system-id 42 :timeline 3 :manifest-digest 77
                   :restart-lsn 0 :consistent-lsn 152 :prefix-digest 123 :anchor request))
             (loaded (pg::physical-follow-pin-decode (pg::physical-follow-pin-encode pin))))
        (ok (= 1 (getf (pg::physical-follow-pin-anchor loaded) :base-timeline)))
        (ok (= 2 (getf (pg::physical-follow-pin-anchor loaded) :receive-timeline)))
        (ok (= 3 (pg::physical-source-pin-timeline loaded))))))

(deftest follow-anchor-ready-proof-does-not-override-terminal-retention-loss
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((pin (pg::physical-source-follow-ready "sibling" request))
              (pg::*max-slot-wal-keep-bytes* 1))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::physical-source-invalidate-lagging-pins-under-wal-lock))
         (ok (eq :ready (pg::physical-follow-pin-stage pin)))
         (ok (pg::physical-source-pin-lost-p pin))
         (ng (pg::physical-source-pin-stream-ready-p pin))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
         (let ((loaded (pg::physical-source-pin-read (pg::physical-source-pin-path "sibling"))))
           (ok (eq :ready (pg::physical-follow-pin-stage loaded)))
           (ng (pg::physical-source-pin-stream-ready-p loaded)))))))

(deftest follow-anchor-native-eor-placement-crosses-page-offset-and-rotates-whole-segments
    (dolist (fork (list (- 8192 16) (- pg::+wal-segment-size+ 16)))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (pg::*wal-insert-timeline* 2)
                                   (pg::*catalog* (pg::make-catalog))
                                   (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
                                   (system-id (pg::catalog-system-identifier))
                                   (pg::*wal-segment-namespace* :timeline)
                                   (start (pg::wal-record-start-after fork pg::+wal-end-of-recovery-record-size+))
                                   (raw (pg::wal-encode-end-of-recovery-record start 42 2 1 1))
                                   (pg::*wal-durable-lsn* (+ start (length raw)))
                                   (request (follow-anchor-request 123 456)))
          (setf (getf request :system-id) system-id
                (getf request :receive-lsn) fork
                (getf request :history-text)
                (format nil "1~c~X/~8,'0X~cpromotion~%" #\Tab
                        (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab)
                (getf request :eor-octets) (coerce raw 'list))
          (pg::wal-ensure-directory-durable (pg::wal-directory))
          (pg::timeline-write-history 2 1 fork "promotion")
          (with-open-file (out (pg::wal-segment-path (floor start pg::+wal-segment-size+))
                               :direction :output :element-type '(unsigned-byte 8))
            (file-position out (mod start pg::+wal-segment-size+))
            (write-sequence raw out) (pg::fsync-stream out))
          (ok (equal request (pg::physical-source-follow-anchor
                              (follow-anchor-receiver-request request))))
          (let ((pin (pg::make-physical-follow-pin
                      :name "boundary" :system-id system-id :timeline 2 :manifest-digest 77
                      :restart-lsn 0 :consistent-lsn fork :prefix-digest 123 :anchor request)))
            (pg::physical-follow-pin-check-fields pin)
            (ok (not (signals (sb-thread:with-mutex (pg::*wal-lock*)
                                (pg::physical-follow-pin-check-source-eor pin)) 'error))))))))

(deftest follow-anchor-replaced-reservation-cannot-inherit-an-older-proof
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((replacement (pg::physical-follow-anchor-copy request))
             (pg::*physical-source-test-hook*
               (lambda (point pin)
                 (declare (ignore pin))
                 (when (eq point :after-follow-proof)
                   (pg::physical-source-pin-release "sibling")
                   (let ((other (pg::physical-follow-anchor-copy request)))
                     (incf (getf other :manifest-digest))
                     (pg::physical-source-follow-reserve "sibling" other))))))
         (incf (getf replacement :manifest-digest))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error))
         (let ((pin (gethash "sibling" pg::*physical-source-pins*)))
           (ok (equal replacement (pg::physical-follow-pin-anchor pin)))
           (ng (pg::physical-follow-pin-ready-p pin))
           (ok (null (pg::physical-source-pin-confirmed-lsn pin))))
         (ng pg::*wal-poisoned*)))))

(deftest follow-anchor-source-entering-recovery-cannot-publish-ready
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let ((pg::*physical-source-test-hook*
               (lambda (point pin)
                 (declare (ignore pin))
                 (when (eq point :after-follow-proof)
                   (setf pg::*physical-recovery-state* (list :phase 2))))))
         (ok (signals (pg::physical-source-follow-ready "sibling" request) 'error)))
       (ng (pg::physical-follow-pin-ready-p (gethash "sibling" pg::*physical-source-pins*)))
       (ng pg::*wal-poisoned*))))

(deftest follow-anchor-source-preflight-rejections-do-not-create-a-reservation
    (dolist (change
             (list (lambda (request) (incf (getf request :system-id)))
                   (lambda (request)
                     (setf (getf request :eor-octets)
                           (coerce (pg::wal-encode-end-of-recovery-record 64 43 2 1 1) 'list)))
                   (lambda (request)
                     (setf (getf request :history-text)
                           (format nil "1~c0/00000040~canother reason~%" #\Tab #\Tab)))))
      (call-with-follow-anchor-source
       (lambda (root request eor)
         (declare (ignore root eor))
         (funcall change request)
         (ok (signals (pg::physical-source-follow-ready "foreign" request) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))
         (ok (not (probe-file (pg::physical-source-pin-path "foreign"))))
         (ng pg::*wal-poisoned*)))))

(deftest follow-anchor-partial-base-file-is-not-a-retained-historical-suffix
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((fork (- pg::+wal-segment-size+ 16))
              (start pg::+wal-segment-size+)
              (raw (pg::wal-encode-end-of-recovery-record start 42 2 1 1))
              (prefix (concatenate '(vector (unsigned-byte 8))
                                   (legacy-checkpoint-record 0) (legacy-checkpoint-record 32))))
         ;; A promoted source may retain only the old backup prefix under TLI 1.
         ;; Its complete TLI-2 copy of segment 0 has already been recycled.
         (delete-file (pg::wal-segment-path 0 2 :timeline))
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0 1 :timeline) prefix)
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 1 2 :timeline) raw)
         (setf pg::*wal-durable-lsn* (+ start (length raw))
               (getf request :history-text)
               (format nil "1~c~X/~8,'0X~cpromotion~%" #\Tab
                       (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab)
               (getf request :eor-octets) (coerce raw 'list))
         (pg::wal-atomic-write-bytes
          (pg::timeline-history-path 2 pg::*pgdata*)
          (pg::string->bytes (getf request :history-text)))
         (ok (signals (pg::physical-source-follow-ready "partial" request) 'error))
         (ok (zerop (hash-table-count pg::*physical-source-pins*)))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*))))
       (ng pg::*wal-poisoned*))))

(deftest follow-anchor-current-file-name-does-not-prove-a-complete-suffix
    (call-with-follow-anchor-source
     (lambda (root request eor)
       (declare (ignore root eor))
       (let* ((fork (- pg::+wal-segment-size+ 16))
              (start pg::+wal-segment-size+)
              (raw (pg::wal-encode-end-of-recovery-record start 42 2 1 1))
              (prefix (concatenate '(vector (unsigned-byte 8))
                                   (legacy-checkpoint-record 0) (legacy-checkpoint-record 32))))
         ;; Current naming alone cannot justify a gap from 64 to the fork.
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0 2 :timeline) prefix)
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 1 2 :timeline) raw)
         (setf pg::*wal-durable-lsn* (+ start (length raw))
               (getf request :history-text)
               (format nil "1~c~X/~8,'0X~cpromotion~%" #\Tab
                       (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab)
               (getf request :eor-octets) (coerce raw 'list))
         (pg::wal-atomic-write-bytes
          (pg::timeline-history-path 2 pg::*pgdata*)
          (pg::string->bytes (getf request :history-text)))
         (ok (signals (pg::physical-source-follow-ready "partial" request) 'error))
         (let ((pin (gethash "partial" pg::*physical-source-pins*)))
           (ok pin)
           (ng (pg::physical-follow-pin-ready-p pin))
           (ok (= 0 (gethash pin pg::*wal-snapshot-retention-pins*)))))
       (ng pg::*wal-poisoned*))))

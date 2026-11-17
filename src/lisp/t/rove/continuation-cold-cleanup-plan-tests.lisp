(in-package #:pg)

(declaim (ftype function logical-slot-continuation-cold-build-cleanup-plan))

(defun unit-continuation-cleanup-physical (name temporary &optional (sid 999) (timeline 77) (restart 99999))
  (physical-slot-encode
   (make-wal-replication-slot :name name :kind :physical :system-id sid
                              :database-oid nil :plugin nil :consistent-lsn nil
                              :restart-lsn restart :restart-timeline timeline
                              :inactive-since 0 :temporary-p temporary)))

(defun unit-continuation-cleanup-plan
    (directory certificate host &key (kind :absent) model temporary
                                  (binding (string->bytes "binding")))
  (unit-continuation-selector-observation-fixture
   (lambda (metadata)
     (unless (eq kind :absent)
       (unit-continuation-cleanup-write-file
        metadata "selector"
        (ecase kind
          (:stable (logical-slot-continuation-encode model))
          (:prepared (logical-slot-continuation-prepared-encode model)))))
     (when temporary
       (unit-continuation-cleanup-write-file metadata "selector.tmp" temporary))
     (call-with-logical-slot-continuation-cold-selector-files
      metadata (lambda () t)
      (lambda (observed selected selector scratch check)
        (funcall check)
        (logical-slot-continuation-cold-build-cleanup-plan
         observed selected selector scratch
         (logical-slot-continuation-cold-read-raw-inventory directory (lambda () t))
         (physical-image-file-facts directory) binding certificate host))))))

(cl-spec:defproperty continuation-cleanup-plan-ignores-temporary-local-eligibility
    ((sid (range integer 0 18446744073709551615))
     (restart (range integer 1 18446744073709551615)))
  (:about logical-slot-continuation-cold-build-cleanup-plan)
  (:trials (:normal 40))
  (multiple-value-bind (certificate host children) (unit-continuation-active-inputs)
    (unit-continuation-inventory-fixture
     (append children
             (list (list "temporary" (unit-continuation-cleanup-physical "temporary" t sid 77 restart))
                   (list "ordinary" (unit-continuation-cleanup-physical "ordinary" nil sid 77 restart))))
     (lambda (directory check)
       (let* ((before (logical-slot-continuation-cold-read-raw-inventory directory check))
              (plan (unit-continuation-cleanup-plan directory certificate host)))
         (and (equal '(:retire-temporary) (mapcar (lambda (row) (getf row :operation))
                                                  (getf plan :actions)))
              (equal '("alpha" "beta" "ordinary") (mapcar #'first (getf plan :retained)))
              (equalp before (logical-slot-continuation-cold-read-raw-inventory directory check))))))))

(rove:deftest continuation-cleanup-plan-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-cleanup-plan-ignores-temporary-local-eligibility
                                         :seed 11916 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 40 (getf result :trials)))))

(rove:deftest continuation-cleanup-plan-classifies-complete-mixed-inventory-without-effects
    (multiple-value-bind (certificate host children) (unit-continuation-active-inputs)
      (unit-continuation-inventory-fixture
       (append children
               (list (list "temporary" (unit-continuation-cleanup-physical "temporary" t))
                     (list "ordinary" (unit-continuation-cleanup-physical "ordinary" nil))))
       (lambda (directory check)
         (let ((*wal-replication-slots* (make-hash-table :test #'equal)))
           (sb-posix:mkdir (namestring (merge-pathnames "temporary.tmp/" (physical-image-fd-path directory))) #o700)
           (unit-continuation-cleanup-write-file directory "temporary.tmp/state.tmp" #(1 2))
           (unit-continuation-cleanup-write-file directory "ordinary/state.tmp" #(3))
           (let* ((before (logical-slot-continuation-cold-read-raw-inventory directory check))
                  (plan (unit-continuation-cleanup-plan directory certificate host)))
             (rove:ok (equal '(:remove-staging :remove-scratch :retire-temporary)
                             (mapcar (lambda (row) (getf row :operation)) (getf plan :actions))))
             (rove:ok (equal '("alpha" "beta") (getf plan :protected)))
             (rove:ok (equal '("alpha" "beta" "ordinary") (mapcar #'first (getf plan :retained))))
             (rove:ok (equalp before (logical-slot-continuation-cold-read-raw-inventory directory check)))
             (rove:ok (zerop (hash-table-count *wal-replication-slots*)))
             (rove:ok
              (handler-case
                  (progn
                    (logical-slot-continuation-check-ordinary-slot
                     (wal-decode-replication-slot-image
                      (second (assoc "ordinary" (getf plan :retained) :test #'string=)) "ordinary")
                     42 (logical-slot-continuation-history (fifth host) (fourth host)) (sixth host))
                    nil)
                (error () t)))))))))

(rove:deftest continuation-cleanup-plan-preserves-protected-and-aliased-child-images
    (dolist (mode '(:owner :provenance :staging-copy :scratch-copy :protected-staging))
      (multiple-value-bind (certificate host children) (unit-continuation-active-inputs)
        (let* ((original (logical-slot-state-decode (second (assoc "alpha" children :test #'string=)) "alpha"))
               (alias (unit-imported-state-with-origin original :owner (unless (eq mode :provenance) 104))))
          (setf (wal-replication-slot-name alias) "ordinary")
          (unit-continuation-inventory-fixture
           (append children
                   (list (list "ordinary"
                               (if (member mode '(:owner :provenance)) (logical-slot-state-encode alias)
                                   (unit-continuation-cleanup-physical "ordinary" nil 42 1 17)))))
           (lambda (directory check)
             (case mode
               (:staging-copy
                (sb-posix:mkdir (namestring (merge-pathnames "discard.tmp/" (physical-image-fd-path directory))) #o700)
                (unit-continuation-cleanup-write-file directory "discard.tmp/state"
                                                      (second (assoc "alpha" children :test #'string=))))
               (:scratch-copy
                (unit-continuation-cleanup-write-file directory "ordinary/state.tmp"
                                                      (second (assoc "alpha" children :test #'string=))))
               (:protected-staging
                (sb-posix:mkdir (namestring (merge-pathnames "alpha.tmp/" (physical-image-fd-path directory))) #o700)))
             (let ((before (logical-slot-continuation-cold-read-raw-inventory directory check)))
               (rove:ok (handler-case
                            (progn (unit-continuation-cleanup-plan directory certificate host) nil)
                          (error () t))
                        (format nil "reject alias ~s" mode))
               (rove:ok (equalp before (logical-slot-continuation-cold-read-raw-inventory directory check))))))))))

(rove:deftest continuation-cleanup-plan-checks-prepared-target-prefixes
    (multiple-value-bind (certificate host children) (unit-continuation-active-inputs)
      (declare (ignore children))
      (let* ((record (unit-continuation-prepared))
             (binding (getf (getf record :old) :binding))
             (image (second (assoc "alpha" (getf (getf record :new) :slots) :test #'string=))))
        (dolist (key '(:old :new))
          (unit-continuation-inventory-fixture
           (getf (getf record key) :slots)
           (lambda (directory check)
             (declare (ignore check))
             (unit-continuation-cleanup-write-file directory "alpha/state.tmp" (subseq image 0 17))
             (let ((plan (unit-continuation-cleanup-plan directory certificate host
                                                         :kind :prepared :model record :binding binding)))
               (rove:ok (eq key (getf plan :choice)))
               (rove:ok (null (getf plan :actions)))))))
        (unit-continuation-inventory-fixture
         (getf (getf record :old) :slots)
         (lambda (directory check)
           (declare (ignore check))
           (unit-continuation-cleanup-write-file directory "beta/state.tmp" (subseq image 0 17))
           (rove:ok (handler-case
                        (progn (unit-continuation-cleanup-plan directory certificate host
                                                               :kind :prepared :model record :binding binding) nil)
                      (error () t))))))))

(rove:deftest continuation-cleanup-plan-rejects-wrong-target-prefix-bytes
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (declare (ignore rows))
      (let ((record (unit-continuation-prepared)))
        (unit-continuation-inventory-fixture
         (getf (getf record :old) :slots)
         (lambda (directory check)
           (let ((path (unit-continuation-cleanup-write-file directory "alpha/state.tmp" #(0))))
             (declare (ignore path))
             (let ((before (logical-slot-continuation-cold-read-raw-inventory directory check)))
               (rove:ok (handler-case
                            (progn (unit-continuation-cleanup-plan
                                    directory certificate host :kind :prepared :model record
                                    :binding (getf (getf record :old) :binding)) nil)
                          (error () t)))
               (rove:ok (equalp before (logical-slot-continuation-cold-read-raw-inventory directory check))))))))))

(rove:deftest continuation-cleanup-plan-checks-absent-selector-bootstrap-prefix
    (multiple-value-bind (certificate host children) (unit-continuation-active-inputs)
      (let* ((binding (string->bytes "binding"))
             (snapshot (logical-slot-continuation-active-child-snapshot binding certificate host children))
             (image (logical-slot-continuation-encode snapshot)))
        (unit-continuation-inventory-fixture
         children
         (lambda (directory check)
           (declare (ignore check))
           (dolist (size (list 0 17 (length image)))
             (let ((plan (unit-continuation-cleanup-plan
                          directory certificate host :temporary (subseq image 0 size))))
               (rove:ok (eq :bootstrap (getf plan :choice)))
               (rove:ok (equalp snapshot (getf plan :snapshot)))))
           (let ((wrong (subseq image 0 17)))
             (setf (aref wrong 0) 0)
             (rove:ok (handler-case
                          (progn (unit-continuation-cleanup-plan directory certificate host :temporary wrong) nil)
                        (error () t)))))))))

(rove:deftest continuation-cleanup-plan-rejects-resource-aliases-and-incomplete-canonical-entries
    (multiple-value-bind (certificate host children) (unit-continuation-active-inputs)
      (unit-continuation-inventory-fixture
       children
       (lambda (directory check)
         (let ((original (logical-slot-continuation-cold-read-raw-inventory directory check))
               (facts (physical-image-file-facts directory))
               (binding (string->bytes "binding")))
           (dolist (mode '(:directory-alias :file-alias :mount :size :empty-canonical))
             (let* ((rows (logical-slot-continuation-copy original))
                    (first (first rows)) (second (second rows)))
               (ecase mode
                 (:directory-alias (setf (getf second :directory) (getf first :directory)))
                 (:file-alias
                  (setf (second (first (getf second :files))) (second (first (getf first :files)))))
                 (:mount (incf (ninth (getf first :directory))))
                 (:size (incf (sixth (second (first (getf first :files))))))
                 (:empty-canonical (setf (getf first :files) nil)))
               (rove:ok
                (handler-case
                    (progn (logical-slot-continuation-cold-build-cleanup-plan
                            :absent nil nil nil rows facts binding certificate host) nil)
                  (error () t))
                (format nil "reject raw resource ~s" mode)))))))))

(define-condition unit-cold-cleanup-entry-interrupt (error) ())

(rove:deftest continuation-cold-check-masks-entry-before-async-interruption
    (let* ((owner sb-thread:*current-thread*)
           (request (sb-thread:make-semaphore :count 0))
           (queued nil) (masked nil) (caught nil) (failed nil)
           (deadline (+ (get-internal-real-time) (* 5 internal-time-units-per-second)))
           (worker
             (sb-thread:make-thread
              (lambda ()
                (when (sb-thread:wait-on-semaphore request :timeout 5)
                  (sb-thread:interrupt-thread owner
                                              (lambda () (error 'unit-cold-cleanup-entry-interrupt)))
                  (setf queued t)
                  (sb-thread:barrier (:write)))))))
      (unwind-protect
           (handler-case
               (with-logical-slot-continuation-observation-check
                   ((progn
                      (setf masked (not sb-sys:*interrupts-enabled*))
                      (sb-thread:signal-semaphore request)
                      (loop until (progn (sb-thread:barrier (:read)) queued)
                            do (when (> (get-internal-real-time) deadline)
                                 (error "interrupt was not queued before the entry deadline")))
                      t)
                    failed owner "test guard")
                 (sleep 0.05)
                 (error "queued interrupt did not run inside guarded work"))
             (unit-cold-cleanup-entry-interrupt () (setf caught t)))
        (sb-thread:join-thread worker))
      (rove:ok masked)
      (rove:ok caught)
      (rove:ok failed)))

(rove:deftest continuation-cold-check-preserves-values-and-foreign-rejection
    (let ((active t) (failed nil) (owner sb-thread:*current-thread*))
      (rove:ok (equal '(1 nil 3)
                      (multiple-value-list
                       (with-logical-slot-continuation-observation-check
                           (active failed owner "test guard")
                         (values 1 nil 3)))))
      (rove:ok (not failed))
      (rove:ok
       (sb-thread:join-thread
        (sb-thread:make-thread
         (lambda ()
           (handler-case
               (progn (with-logical-slot-continuation-observation-check
                          (active failed owner "test guard")
                        (error "foreign thread reached guarded work"))
                      nil)
             (error () t))))))
      (rove:ok (not failed))
      (rove:ok (eq :valid (with-logical-slot-continuation-observation-check
                              (active failed owner "test guard")
                            :valid)))))

(rove:deftest continuation-cleanup-plan-binds-selector-and-certified-domain
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (let* ((binding (string->bytes "binding"))
             (snapshot (logical-slot-continuation-active-child-snapshot binding certificate host rows)))
        (unit-continuation-inventory-fixture
         rows
         (lambda (directory check)
           (let ((raw (logical-slot-continuation-cold-read-raw-inventory directory check))
                 (root (physical-image-file-facts directory)))
             (unit-continuation-selector-observation-fixture
              (lambda (metadata)
                (unit-continuation-cleanup-write-file metadata "selector"
                                                      (logical-slot-continuation-encode snapshot))
                (call-with-logical-slot-continuation-cold-selector-files
                 metadata (lambda () t)
                 (lambda (kind model selector temporary verify)
                   (let ((changed (logical-slot-continuation-copy model)))
                     (incf (getf changed :generation))
                     (rove:ok (handler-case
                                  (progn (logical-slot-continuation-cold-build-cleanup-plan
                                          kind changed selector temporary raw root binding certificate host) nil)
                                (error () t))))
                   (setf (fourth (first (getf certificate :slots))) nil)
                   (rove:ok (handler-case
                                (progn (logical-slot-continuation-cold-build-cleanup-plan
                                        kind model selector temporary raw root binding certificate host) nil)
                              (error () t)))
                   (rove:ok (funcall verify))))))))))))

(rove:deftest continuation-cleanup-plan-keeps-partial-staging-as-namespace-authority
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (declare (ignore rows))
      (unit-continuation-inventory-fixture
       nil
       (lambda (directory check)
         (dotimes (mode 4)
           (let ((name (format nil "stage_~d.tmp/" mode)))
             (sb-posix:mkdir (namestring (merge-pathnames name (physical-image-fd-path directory))) #o700)
             (when (logbitp 0 mode)
               (unit-continuation-cleanup-write-file directory (concatenate 'string name "state") #(1)))
             (when (logbitp 1 mode)
               (unit-continuation-cleanup-write-file directory (concatenate 'string name "state.tmp") #()))))
         (let* ((before (logical-slot-continuation-cold-read-raw-inventory directory check))
                (plan (unit-continuation-cleanup-plan directory certificate host)))
           (rove:ok (null (getf plan :retained)))
           (rove:ok (null (getf (getf plan :snapshot) :slots)))
           (rove:ok (equal '(:remove-staging :remove-staging :remove-staging :remove-staging)
                           (mapcar (lambda (action) (getf action :operation)) (getf plan :actions))))
           (rove:ok (equal '("alpha" "beta") (getf plan :protected)))
           (rove:ok (equalp before (logical-slot-continuation-cold-read-raw-inventory directory check))))))))

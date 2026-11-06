(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-retirement-intent-layout-preserves-nonjournal-facts
    ((offset (range integer 10 1000)) (delta (range integer 1 1000)))
  (:about pg::physical-reseed-follow-retirement-intent-layout)
  (:trials (:normal 100))
  (multiple-value-bind (before-fds before) (reseed-startup-projection-fixture offset)
    (multiple-value-bind (after-fds after) (reseed-startup-projection-fixture (+ offset delta))
      (let* ((layout (getf after :layout))
             (root (cdr (assoc :root (getf layout :directories)))))
        (incf (seventh root) delta)
        (incf (eighth root) delta)
        (incf (ninth root) delta)
        (push "physical-reseed.follow.tmp" (getf (getf layout :names) :root))
        (let ((expected (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                                 before-fds (getf before :layout))))
          (and
           (equal expected
                  (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                           after-fds layout))
           (progn
             (push "foreign" (getf (getf layout :names) :root))
             (not (equal expected
                         (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                                  after-fds layout))))))))))

(deftest generated-reseed-retirement-intent-layout-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retirement-intent-layout-preserves-nonjournal-facts
                                       :seed 150 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-retirement-intent-layout-preserves-case-roles-and-copies
    (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 20)
      (let* ((layout (getf state :layout))
             (expected (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                                descriptors layout)))
        (dolist (entry '((:root "Physical-reseed.follow.tmp")
                         (:saved "physical-reseed.follow.tmp")))
          (let ((copy (pg::physical-adoption-attempt-value-copy layout)))
            (push (second entry) (getf (getf copy :names) (first entry)))
            (ok (not (equal expected
                            (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                                     descriptors copy))))))
        (let ((copy (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                             descriptors layout)))
          (setf (char (first (getf (getf copy :names) :root)) 0) #\X)
          (ok (equal expected
                     (funcall (symbol-function 'pg::physical-reseed-follow-retirement-intent-layout)
                              descriptors layout)))))))

(deftest reseed-retirement-intent-proof-preserves-state-during-a-partial-temporary
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner workspace))
       (let ((stream nil) (fixed nil) (before nil)
                          (path (merge-pathnames "physical-reseed.follow.tmp" root))
                          (bytes (sb-ext:string-to-octets "partial" :external-format :utf-8)))
         (unwind-protect
              (pg::call-with-physical-reseed-follow-retirement-resources
               root-fd (getf snapshot :record) check
               (lambda (descriptors store base binding verify)
                 (declare (ignore base binding))
                 (flet ((extra (fd)
                          (funcall verify)
                          (append
                           (pg::physical-reseed-follow-retirement-final-extra-at fd snapshot)
                           (when stream
                             (pg::physical-reseed-follow-store-fd-check
                              fd (sb-sys:fd-stream-fd stream) "physical-reseed.follow.tmp" fixed)
                             (list (cons "physical-reseed.follow.tmp"
                                         (nth-value 1 (pg::physical-reseed-follow-entry
                                                       fd "physical-reseed.follow.tmp" t))))))))
                   (setf before
                         (pg::physical-reseed-follow-retirement-intent-proof-at
                          descriptors store (getf snapshot :record) verify #'extra))
                   (sb-sys:without-interrupts
                     (setf stream (open path :direction :io :element-type '(unsigned-byte 8)
                                             :if-exists :error :if-does-not-exist :create)))
                   (sb-posix:fchmod (sb-sys:fd-stream-fd stream) #o600)
                   (setf fixed (subseq (pg::physical-reseed-follow-file-identity
                                        (sb-posix:fstat (sb-sys:fd-stream-fd stream)) t) 0 5))
                   (ok (equal before
                              (pg::physical-reseed-follow-retirement-intent-proof-at
                               descriptors store (getf snapshot :record) verify #'extra)))
                   (write-sequence bytes stream)
                   (finish-output stream)
                   (ok (equal before
                              (pg::physical-reseed-follow-retirement-intent-proof-at
                               descriptors store (getf snapshot :record) verify #'extra)))
                   (ok (funcall verify)))))
           (when stream (close stream)))
         (ok (equalp bytes (lineage-test-bytes path)))
         (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))))))

(deftest reseed-retirement-intent-proof-checks-parent-before-descriptor-use
    (ok (eq :refused
            (catch 'retirement-intent-parent-refused
              (pg::physical-reseed-follow-retirement-intent-proof-at
               nil nil nil
               (lambda () (throw 'retirement-intent-parent-refused :refused))
               (lambda (root)
                 (declare (ignore root))
                 (error "extra reader must stay unreachable")))))))

(deftest reseed-retirement-prefix-proof-checks-parent-before-descriptor-use
    (ok (eq :refused
            (catch 'retirement-prefix-parent-refused
              (pg::physical-reseed-follow-retirement-prefix-proof-at
               nil nil nil 1
               (lambda () (throw 'retirement-prefix-parent-refused :refused))
               (lambda (root)
                 (declare (ignore root))
                 (error "extra reader must stay unreachable")))))))

(defun check-retirement-prefix-test-proof (root workspace root-fd record snapshot prefix check &optional alias)
  (pg::call-with-physical-reseed-follow-retirement-resources
   root-fd record check
   (lambda (descriptors store base binding verify)
     (declare (ignore base binding))
     (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
       (flet ((extra (fd) (pg::physical-reseed-follow-retirement-extra-at fd snapshot)))
         (let* ((observed
                  (pg::physical-reseed-follow-retirement-layout-state descriptors snapshot verify))
                (proof
                  (pg::physical-reseed-follow-retirement-prefix-proof-at
                   descriptors store record prefix verify #'extra alias)))
           (ok (= prefix (getf proof :prefix)))
           (ok (equal (getf proof :layout)
                      (pg::physical-reseed-follow-retirement-intent-layout
                       descriptors (getf observed :layout))))
           (ok (equal (getf proof :install) (getf observed :install)))
           (ok (equal (getf proof :attempt) (getf observed :attempt)))
           (ok (equal (getf proof :receive) (pg::physical-store-control-tuple store)))
           (if (and (zerop prefix) (not alias))
               (ok (equal proof
                          (pg::physical-reseed-follow-retirement-intent-proof-at
                           descriptors store record verify #'extra)))
               (ok (handler-case
                       (progn
                         (pg::physical-reseed-follow-retirement-intent-proof-at
                          descriptors store record verify #'extra)
                         nil)
                     (error () t))))
           (ok (funcall verify))
           (ok (reseed-activation-layout-facts-equal-p
                before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
           proof))))))

(deftest reseed-retirement-prefix-proof-binds-each-observed-archive-position
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner))
       (let* ((record (pg::physical-reseed-follow-copy (getf snapshot :record)))
              (attempt (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 0))
              (reference (list :identity (getf attempt :identity) :digest (getf attempt :digest)))
              (saved (pg::physical-backup-open-directory (merge-pathnames "saved/" workspace))))
         (unwind-protect
              (progn
                (check-retirement-prefix-test-proof root workspace root-fd record snapshot 0 check)
                (loop for prefix from 1 to 2
                      for name in '("physical-reseed.install" "physical-reseed.attempt")
                      do
                         (setf (getf record :retirement)
                               (list :phase :retiring :step (1- prefix) :attempt-file reference))
                         (reseed-retirement-layout-write root record)
                         (pg::call-with-physical-reseed-follow-retirement-journal
                          root-fd "physical-reseed.follow" (pg::physical-reseed-follow-snapshot-at root-fd)
                          (lambda (stream held)
                            (let ((bytes
                                    (nth-value 1
                                               (pg::physical-reseed-follow-retirement-journal-state-at
                                                root-fd stream "physical-reseed.follow")))
                                  (before
                                    (check-retirement-prefix-test-proof
                                     root workspace root-fd record held (1- prefix) check)))
                              (unless (zerop (pg::physical-adoption-renameat2
                                              root-fd name saved name pg::+physical-adoption-rename-noreplace+))
                                (error "retirement prefix proof fixture move failed"))
                              (multiple-value-bind (actual actual-bytes)
                                  (pg::physical-reseed-follow-retirement-journal-state-at
                                   root-fd stream "physical-reseed.follow")
                                (ok (equal held actual))
                                (ok (equalp bytes actual-bytes)))
                              (let ((after (check-retirement-prefix-test-proof
                                            root workspace root-fd record held prefix check)))
                                (ok (pg::physical-reseed-follow-retirement-check-move-proof
                                     before after (1- prefix)))
                                (ok (pg::physical-reseed-follow-retirement-check-move-proof
                                     after
                                     (check-retirement-prefix-test-proof
                                      root workspace root-fd record held prefix check)
                                     (1- prefix))))))
                          check)
                         (setf (getf (getf record :retirement) :step) prefix)
                         (reseed-retirement-layout-write root record)
                         (check-retirement-prefix-test-proof
                          root workspace root-fd record
                          (pg::physical-reseed-follow-snapshot-at root-fd) prefix check)))
           (sb-posix:close saved))))))

(defun retirement-move-proof-test-pair (step delta)
  (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 20)
    (let* ((name (if (zerop step) "physical-reseed.install" "physical-reseed.attempt"))
           (key (if (zerop step) :install :attempt))
           (before
             (list :prefix step
                   :layout (pg::physical-reseed-follow-retirement-intent-layout descriptors (getf state :layout))
                   :install (list :identity (list 1 2 #o100600 1 1000 40 50 60)
                                  :row (list "physical-reseed.install" 1 2 #o100600 1 1000 40 50 60)
                                  :digest 12)
                   :attempt (list :identity (list 1 3 #o100600 1 1000 41 51 61)
                                  :row (list "physical-reseed.attempt" 1 3 #o100600 1 1000 41 51 61)
                                  :digest 13)
                   :receive (list 1 2 3))))
      (push name (getf (getf (getf before :layout) :names) :root))
      (let* ((after (pg::physical-adoption-attempt-value-copy before))
             (layout (getf after :layout))
             (names (getf layout :names))
             (file (getf after key)))
        (setf (getf after :prefix) (1+ step)
              (getf names :root) (remove name (getf names :root) :test #'equal))
        (push name (getf names :saved))
        (dolist (role '(:root :saved))
          (let ((row (cdr (assoc role (getf layout :directories)))))
            (when (eq role :saved)
              (incf (seventh row) delta)
              (incf (eighth row) delta)
              (incf (ninth row) delta))))
        (incf (car (last (getf file :identity))) delta)
        (incf (car (last (getf file :row))) delta)
        (values before after)))))

(cl-spec:defproperty reseed-retirement-move-proof-preserves-every-unmoved-fact
    ((second-move boolean) (delta (range integer 1 1000)))
  (:about pg::physical-reseed-follow-retirement-check-move-proof)
  (:trials (:normal 100))
  (let ((step (if second-move 1 0)))
    (multiple-value-bind (before after) (retirement-move-proof-test-pair step delta)
      (let ((saved-before (pg::physical-adoption-attempt-value-copy before))
            (saved-after (pg::physical-adoption-attempt-value-copy after)))
        (and (funcall (symbol-function 'pg::physical-reseed-follow-retirement-check-move-proof)
                      before after step)
             (equal saved-before before)
             (equal saved-after after)
             (progn
               (push "foreign" (getf (getf (getf after :layout) :names) :saved))
               (handler-case
                   (progn
                     (funcall (symbol-function 'pg::physical-reseed-follow-retirement-check-move-proof)
                              before after step)
                     nil)
                 (error () t))))))))

(deftest generated-reseed-retirement-move-proof-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retirement-move-proof-preserves-every-unmoved-fact
                                       :seed 152 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(defun retirement-archive-test-proofs (step delta)
  (multiple-value-bind (source ignored) (retirement-move-proof-test-pair step delta)
    (declare (ignore ignored))
    (let* ((key (if (zerop step) :install :attempt))
           (name (if (zerop step) "physical-reseed.install" "physical-reseed.attempt"))
           (alias (pg::physical-adoption-attempt-value-copy source))
           (file (getf alias key))
           (row (getf file :row)))
      (when (= step 1)
        (setf (getf (getf source :attempt) :identity)
              (pg::physical-reseed-follow-retirement-attempt-row-identity
               (getf (getf source :attempt) :row))
              (getf file :identity) (copy-list (getf (getf source :attempt) :identity))))
      (setf (getf alias :location) :alias
            (getf file :location) :alias
            (fifth row) 2
            (nth (if (zerop step) 3 4) (getf file :identity)) 2)
      (incf (car (last row)) delta)
      (incf (car (last (getf file :identity))) delta)
      (setf (getf file :alias-row) (copy-list row)
            (getf alias key) file)
      (push name (getf (getf (getf alias :layout) :names) :saved))
      (let ((saved-directory (cdr (assoc :saved (getf (getf alias :layout) :directories)))))
        (incf (seventh saved-directory) delta)
        (incf (eighth saved-directory) delta)
        (incf (ninth saved-directory) delta))
      (let* ((saved (pg::physical-adoption-attempt-value-copy alias))
             (saved-file (getf saved key))
             (names (getf (getf saved :layout) :names)))
        (remf saved :location)
        (remf saved-file :location)
        (remf saved-file :alias-row)
        (setf (getf saved :prefix) (1+ step)
              (getf names :root) (remove name (getf names :root) :test #'equal)
              (fifth (getf saved-file :row)) 1
              (nth (if (zerop step) 3 4) (getf saved-file :identity)) 1
              (getf saved key) saved-file)
        (incf (car (last (getf saved-file :row))) delta)
        (incf (car (last (getf saved-file :identity))) delta)
        (values source alias saved)))))

(cl-spec:defproperty reseed-retirement-archive-bridges-preserve-unmoved-facts
    ((second-move boolean) (delta (range integer 1 1000)))
  (:about pg::physical-reseed-follow-retirement-check-archive-bridge)
  (:trials (:normal 100))
  (let ((step (if second-move 1 0)))
    (multiple-value-bind (source alias saved) (retirement-archive-test-proofs step delta)
      (let ((original (pg::physical-adoption-attempt-value-copy (list source alias saved))))
        (and (funcall (symbol-function 'pg::physical-reseed-follow-retirement-check-archive-bridge)
                      source alias step :link)
             (funcall (symbol-function 'pg::physical-reseed-follow-retirement-check-archive-bridge)
                      alias saved step :unlink)
             (equal original (list source alias saved))
             (progn
               (incf (getf (getf saved (if (zerop step) :attempt :install)) :digest))
               (handler-case
                   (progn
                     (funcall (symbol-function 'pg::physical-reseed-follow-retirement-check-archive-bridge)
                              alias saved step :unlink)
                     nil)
                 (error () t))))))))

(deftest generated-reseed-retirement-archive-bridge-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retirement-archive-bridges-preserve-unmoved-facts
                                       :seed 158 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-retirement-archive-bridge-rejects-nonowned-changes
    (dolist (step '(0 1))
      (dolist (operation '(:link :unlink))
        (dolist (change '(:uid :mtime :other-journal :inventory :root-uid :prefix :link-count :extra-field :saved-time))
          (unless (and (eq change :saved-time) (eq operation :link))
            (multiple-value-bind (source alias saved) (retirement-archive-test-proofs step 10)
              (let* ((before (if (eq operation :link) source alias))
                     (after (if (eq operation :link) alias saved))
                     (key (if (zerop step) :install :attempt))
                     (file (getf after key))
                     (layout (getf after :layout)))
                (ecase change
                  (:uid (incf (nth (if (zerop step) 4 3) (getf file :identity))))
                  (:mtime
                   (incf (eighth (getf file :row)))
                   (when (getf file :alias-row)
                     (setf (getf file :alias-row) (copy-list (getf file :row)))))
                  (:other-journal (incf (getf (getf after (if (zerop step) :attempt :install)) :digest)))
                  (:inventory (push "foreign" (getf (getf layout :names) :saved)))
                  (:root-uid (incf (sixth (cdr (assoc :root (getf layout :directories))))))
                  (:prefix (incf (getf after :prefix)))
                  (:link-count (incf (fifth (getf file :row))))
                  (:extra-field (setf (getf file :unexpected) t (getf after key) file))
                  (:saved-time (incf (ninth (cdr (assoc :saved (getf layout :directories)))))))
                (ok (handler-case
                        (progn
                          (pg::physical-reseed-follow-retirement-check-archive-bridge before after step operation)
                          nil)
                      (error (condition)
                        (search "archive bridge" (princ-to-string condition))))))))))))

(deftest reseed-retirement-move-proof-rejects-changes-beyond-the-single-rename
    (dolist (step '(0 1))
      (multiple-value-bind (before after) (retirement-move-proof-test-pair step 7)
        (ok (pg::physical-reseed-follow-retirement-check-move-proof before after step))
        (dolist (change
                 (list
                  (lambda (value) (incf (first (getf value :receive))))
                  (lambda (value) (incf (getf (getf value :install) :digest)))
                  (lambda (value) (incf (getf (getf value :attempt) :digest)))
                  (lambda (value)
                    (incf (third (cdr (assoc :saved (getf (getf value :layout) :directories))))))
                  (lambda (value)
                    (incf (third (cdr (assoc :root (getf (getf value :layout) :directories))))))
                  (lambda (value)
                    (incf (sixth (cdr (assoc :root (getf (getf value :layout) :directories))))))
                  (lambda (value)
                    (incf (sixth (cdr (assoc :saved (getf (getf value :layout) :directories))))))
                  (lambda (value)
                    (push "foreign" (getf (getf (getf value :layout) :names) :root)))
                  (lambda (value)
                    (incf (second (getf (getf value (if (zerop step) :install :attempt)) :identity))))
                  (lambda (value)
                    (incf (eighth (getf (getf value (if (zerop step) :install :attempt)) :row))))
                  (lambda (value)
                    (incf (car (last (getf (getf value (if (zerop step) :attempt :install)) :identity)))))))
          (let ((changed (pg::physical-adoption-attempt-value-copy after)))
            (funcall change changed)
            (ok (handler-case
                    (progn (pg::physical-reseed-follow-retirement-check-move-proof before changed step) nil)
                  (error (condition) (search "archive move changed frozen" (princ-to-string condition)))))))
        (let ((changed (pg::physical-adoption-attempt-value-copy after)))
          (incf (ninth (cdr (assoc :saved (getf (getf changed :layout) :directories)))))
          (ok (pg::physical-reseed-follow-retirement-check-move-proof after after step))
          (ok (handler-case
                  (progn (pg::physical-reseed-follow-retirement-check-move-proof after changed step) nil)
                (error (condition) (search "archive move changed frozen" (princ-to-string condition)))))))))

(defun check-retirement-test-ctime-change (parent name)
  (pg::call-with-physical-image-input-at
   parent name
   (lambda (stream)
     (let* ((fd (sb-sys:fd-stream-fd stream))
            (before (pg::physical-reseed-file-facts (sb-posix:fstat fd))))
       (sleep 1.1)
       (sb-posix:fchmod fd #o600)
       (let ((after (pg::physical-reseed-file-facts (sb-posix:fstat fd))))
         (ok (equal (butlast before) (butlast after)))
         (ok (/= (car (last before)) (car (last after)))))))
   :element-type '(unsigned-byte 8)))

(defun check-retirement-test-same-byte-write (parent name)
  (let* ((path (merge-pathnames name (pg::physical-image-fd-path parent)))
         (bytes (pg::recovery-read-bytes path))
         (before (pg::physical-reseed-file-facts (sb-posix:stat (namestring path)))))
    (sleep 1.1)
    (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8)
                                 :if-exists :overwrite :if-does-not-exist :error)
      (write-sequence bytes stream)
      (finish-output stream)
      (sb-posix:fsync (sb-sys:fd-stream-fd stream)))
    (let ((after (pg::physical-reseed-file-facts (sb-posix:stat (namestring path)))))
      (ok (equal (subseq before 0 6) (subseq after 0 6)))
      (ok (/= (seventh before) (seventh after))))
    (ok (equalp bytes (pg::recovery-read-bytes path)))))

(deftest reseed-retirement-admits-pending-source-ctime-only-after-its-final-intent
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd initial check)
       (declare (ignore owner))
       (let* ((record (pg::physical-reseed-follow-copy (getf initial :record)))
              (attempt (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 0))
              (reference (list :identity (getf attempt :identity) :digest (getf attempt :digest)))
              (saved (pg::physical-backup-open-directory (merge-pathnames "saved/" workspace)))
              (observed 0))
         (unwind-protect
              (flet ((observe ()
                       (handler-case
                           (pg::call-with-physical-reseed-follow-retirement-observation
                            root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                            (lambda (descriptors state verify)
                              (declare (ignore descriptors))
                              (incf observed)
                              (ok (equal record (getf (getf state :snapshot) :record)))
                              (ok (funcall verify))
                              :observed))
                         (error (condition)
                           (format t "~%RETIREMENT-CTIME-OBSERVATION-REFUSAL ~a~%" condition)
                           :refused))))
                (check-retirement-test-ctime-change root-fd "physical-reseed.install")
                (ok (handler-case
                        (progn
                          (pg::physical-reseed-follow-retirement-install-at
                           (list (cons :root root-fd) (cons :saved saved)) record 0)
                          nil)
                      (error (condition) (search "frozen reference" (princ-to-string condition)))))
                (setf record (pg::physical-reseed-follow-retirement-next-record record reference))
                (reseed-retirement-layout-write root record)
                (ok (eq :observed (observe)))
                (ok (= 1 observed))
                (unless (zerop (pg::physical-adoption-renameat2
                                root-fd "physical-reseed.install" saved "physical-reseed.install"
                                pg::+physical-adoption-rename-noreplace+))
                  (error "retirement ctime fixture move failed"))
                (check-retirement-test-ctime-change root-fd "physical-reseed.attempt")
                (ok (handler-case
                        (progn
                          (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 1)
                          nil)
                      (error (condition) (search "frozen attempt file reference" (princ-to-string condition)))))
                (setf record (pg::physical-reseed-follow-retirement-next-record record reference))
                (reseed-retirement-layout-write root record)
                (ok (eq :observed (observe)))
                (ok (= 2 observed))
                (ok (equal reference (getf (getf record :retirement) :attempt-file)))
                (check-retirement-test-same-byte-write root-fd "physical-reseed.attempt")
                (ok (handler-case
                        (progn
                          (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 1)
                          nil)
                      (error (condition) (search "frozen attempt file reference" (princ-to-string condition)))))
                (check-retirement-test-same-byte-write saved "physical-reseed.install")
                (ok (handler-case
                        (progn
                          (pg::physical-reseed-follow-retirement-install-at
                           (list (cons :root root-fd) (cons :saved saved)) record 1)
                          nil)
                      (error (condition) (search "frozen reference" (princ-to-string condition))))))
           (sb-posix:close saved))))))

(deftest reseed-retirement-pending-root-install-still-rejects-a-same-byte-write
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner workspace))
       (let* ((before (getf snapshot :record))
              (attempt (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd before 0))
              (reference (list :identity (getf attempt :identity) :digest (getf attempt :digest)))
              (record (pg::physical-reseed-follow-retirement-next-record before reference))
              (entered nil))
         (reseed-retirement-layout-write root record)
         (check-retirement-test-same-byte-write root-fd "physical-reseed.install")
         (ok (handler-case
                 (progn
                   (pg::call-with-physical-reseed-follow-retirement-observation
                    root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                    (lambda (descriptors state verify)
                      (declare (ignore descriptors state verify))
                      (setf entered t)))
                   nil)
               (error (condition) (search "frozen reference" (princ-to-string condition)))))
         (ok (not entered))))))

(deftest reseed-retirement-alias-holder-checks-parent-before-descriptor-use
    (ok (eq :refused
            (catch 'retirement-alias-parent-refused
              (pg::call-with-physical-reseed-follow-retirement-alias-file
               nil nil nil
               (lambda (&rest arguments)
                 (declare (ignore arguments))
                 (error "alias callback must stay unreachable"))
               (lambda () (throw 'retirement-alias-parent-refused :refused)))))))

(cl-spec:defproperty reseed-retirement-alias-reference-preserves-all-other-facts
    ((attempt boolean) (delta (range integer 1 1000)))
  (:about pg::physical-reseed-follow-retirement-alias-reference-p)
  (:trials (:normal 100))
  (let* ((index (if attempt 4 3))
         (expected (if attempt
                       (list 1 2 #o100600 1000 1 40 50 60)
                       (list 1 2 #o100600 1 1000 40 50 60)))
         (actual (copy-list expected)))
    (setf (nth index actual) 2)
    (incf (car (last actual)) delta)
    (let ((saved-expected (copy-list expected)) (saved-actual (copy-list actual)))
      (and (funcall (symbol-function 'pg::physical-reseed-follow-retirement-alias-reference-p)
                    expected actual index)
           (equal expected saved-expected)
           (equal actual saved-actual)
           (progn
             (incf (first actual) delta)
             (not (funcall (symbol-function 'pg::physical-reseed-follow-retirement-alias-reference-p)
                           expected actual index)))))))

(deftest generated-reseed-retirement-alias-reference-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-retirement-alias-reference-preserves-all-other-facts
                                       :seed 154 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(defun call-with-retirement-alias-test-states (function)
  (call-with-reseed-retirement-observation-fixture
   :same-timeline
   (lambda (owner root workspace root-fd snapshot check)
     (declare (ignore owner))
     (let* ((record (getf snapshot :record))
            (attempt (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 0))
            (reference (list :identity (getf attempt :identity) :digest (getf attempt :digest)))
            (saved (pg::physical-backup-open-directory (merge-pathnames "saved/" workspace))))
       (unwind-protect
            (dolist (kind '(:install :attempt))
              (setf record (pg::physical-reseed-follow-retirement-next-record record reference))
              (reseed-retirement-layout-write root record)
              (let* ((name (if (eq kind :install) "physical-reseed.install" "physical-reseed.attempt"))
                     (source (merge-pathnames name root))
                     (target (merge-pathnames name (pg::physical-image-fd-path saved))))
                (sb-posix:link (namestring source) (namestring target))
                (funcall function kind root workspace root-fd saved record name check)
                (unless (and (= 2 (sb-posix:stat-nlink (sb-posix:stat (namestring source))))
                             (= (sb-posix:stat-ino (sb-posix:stat (namestring source)))
                                (sb-posix:stat-ino (sb-posix:stat (namestring target)))))
                  (error "alias fixture callback changed its expected pair"))
                (delete-file source)))
         (sb-posix:close saved))))))

(defun check-retirement-alias-open-cuts (root saved record name check)
  (let ((original (symbol-function 'pg::physical-image-openat)))
    (dolist (mode '(:error :throw :interrupt))
      (let ((held nil) (injected nil) (entered nil))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-image-openat)
                     (lambda (&rest arguments)
                       (let ((fd (apply original arguments)))
                         (when (and (= root (first arguments)) (equal name (second arguments))
                                    (not (minusp fd)))
                           (setf held fd))
                         fd)))
               (let ((pg::*physical-reseed-follow-test-hook*
                       (lambda (point path)
                         (declare (ignore path))
                         (when (eq point :after-retirement-alias-open)
                           (setf injected t)
                           (ecase mode
                             (:error (error "retirement alias open cut"))
                             (:throw (throw 'retirement-alias-open-cut :cut))
                             (:interrupt
                              (sb-thread:interrupt-thread
                               sb-thread:*current-thread*
                               (lambda () (throw 'retirement-alias-open-cut :cut)))))))))
                 (ok (eq :cut
                         (catch 'retirement-alias-open-cut
                           (handler-case
                               (pg::call-with-physical-reseed-follow-retirement-alias-file
                                root saved record
                                (lambda (&rest arguments)
                                  (declare (ignore arguments))
                                  (setf entered t))
                                check)
                             (error (condition)
                               (if (search "retirement alias open cut" (princ-to-string condition))
                                   :cut (error condition)))))))))
          (setf (symbol-function 'pg::physical-image-openat) original))
        (ok injected)
        (ok (not entered))
        (ok (and held (handler-case (progn (sb-posix:fstat held) nil)
                        (sb-posix:syscall-error () t))))
        (ok (funcall check))))))

(deftest reseed-retirement-alias-holder-binds-only-the-exact-pending-pair
    (call-with-retirement-alias-test-states
     (lambda (kind root workspace root-fd saved record name check)
       (let ((captured nil) (escaped nil)
                            (before (pg::physical-adoption-attempt-value-copy record))
                            (reference (if (eq kind :install) (getf record :install)
                                           (getf (getf record :retirement) :attempt-file))))
         (ok (handler-case
                 (progn
                   (if (eq kind :install)
                       (pg::physical-reseed-install-read-at root-fd name)
                       (pg::physical-reseed-attempt-read-state-at root-fd name))
                   nil)
               (error (condition) (search "single-link regular file" (princ-to-string condition)))))
         (pg::call-with-physical-reseed-follow-retirement-alias-file
          root-fd saved record
          (lambda (stream state verify)
            (setf captured (sb-sys:fd-stream-fd stream) escaped verify)
            (ok (eq :alias (getf state :location)))
            (ok (= 2 (nth (if (eq kind :install) 3 4) (getf state :identity))))
            (ok (= (getf reference :digest) (getf state :digest)))
            (ok (logbitp 0 (sb-posix:fcntl captured sb-posix:f-getfd)))
            (ok (funcall verify))
            (let ((thread (sb-thread:make-thread
                           (lambda ()
                             (handler-case (progn (funcall verify) nil)
                               (error (condition) (princ-to-string condition)))))))
              (ok (search "changed thread" (sb-thread:join-thread thread))))
            (incf (third (getf state :row)))
            (ok (funcall verify)))
          check)
         (ok (equal before record))
         (ok (handler-case (progn (funcall escaped) nil)
               (error (condition) (search "scope ended" (princ-to-string condition)))))
         (ok (handler-case (progn (sb-posix:fstat captured) nil)
               (sb-posix:syscall-error () t)))
         (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat root-fd))))
         (check-retirement-alias-open-cuts root-fd saved record name check)
         (let ((third (merge-pathnames "third-alias" workspace))
               (source (merge-pathnames name root)))
           (unwind-protect
                (progn
                  (sb-posix:link (namestring source) (namestring third))
                  (ok (= 3 (sb-posix:stat-nlink (sb-posix:stat (namestring source)))))
                  (ok (handler-case
                          (progn (pg::physical-reseed-follow-retirement-alias-at root-fd saved record check) nil)
                        (error (condition) (search "exactly two private names" (princ-to-string condition))))))
             (when (probe-file third) (delete-file third)))
           (ok (eq :alias (getf (pg::physical-reseed-follow-retirement-alias-at root-fd saved record check)
                                :location))))
         (let ((without-intent (pg::physical-reseed-follow-copy record)) (entered nil))
           (remf without-intent :retirement)
           (ok (handler-case
                   (progn
                     (pg::call-with-physical-reseed-follow-retirement-alias-file
                      root-fd saved without-intent
                      (lambda (&rest arguments) (declare (ignore arguments)) (setf entered t))
                      check)
                     nil)
                 (error (condition) (search "pending retirement journal" (princ-to-string condition)))))
           (ok (not entered)))))))

(deftest reseed-retirement-layout-observes-the-exact-pending-alias-without-effects
    (call-with-retirement-alias-test-states
     (lambda (kind root workspace root-fd saved record name check)
       (declare (ignore saved name))
       (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace)))
             (entered 0) (escaped nil))
         (ok (eq :observed
                 (handler-case
                     (pg::call-with-physical-reseed-follow-retirement-observation
                      root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                      (lambda (descriptors state verify)
                        (declare (ignore descriptors))
                        (incf entered)
                        (setf escaped verify)
                        (ok (= (getf (getf record :retirement) :step) (getf state :prefix)))
                        (ok (eq :alias (getf state :location)))
                        (ok (eq :alias (getf (getf state kind) :location)))
                        (ok (= 2 (nth (if (eq kind :install) 3 4)
                                      (getf (getf state kind) :identity))))
                        (ok (equal record (getf (getf state :snapshot) :record)))
                        (ok (funcall verify))
                        :observed))
                   (error (condition)
                     (format t "~%RETIREMENT-ALIAS-LAYOUT-REFUSAL ~a~%" condition)
                     :refused))))
         (ok (= 1 entered))
         (when escaped
           (ok (handler-case (progn (funcall escaped) nil)
                 (error (condition) (search "scope" (princ-to-string condition))))))
         (ok (reseed-activation-layout-facts-equal-p
              before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))))))

(deftest reseed-retirement-alias-refuses-progress-temporaries-before-source-unlink
    (call-with-retirement-alias-test-states
     (lambda (kind root workspace root-fd saved record name check)
       (declare (ignore kind saved name))
       (let* ((candidate
                (pg::physical-reseed-follow-retirement-next-record
                 record (getf (getf record :retirement) :attempt-file)))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (entered nil))
         (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate))
         (unwind-protect
              (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                           root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                           (lambda (&rest arguments)
                             (declare (ignore arguments))
                             (setf entered t)))
                          nil)
                      (error (condition)
                        (search "retirement temporary requires its exact next observed archive prefix"
                                (princ-to-string condition)))))
                (ng entered)
                (ok (reseed-activation-layout-facts-equal-p
                     before (mapcar #'reseed-mapped-test-tree-facts (list root workspace)))))
           (delete-file temporary))))))

(deftest reseed-retirement-prefix-proof-binds-the-pending-alias-without-initial-authority
    (call-with-retirement-alias-test-states
     (lambda (kind root workspace root-fd saved record name check)
       (declare (ignore kind saved name))
       (let ((proof
               (check-retirement-prefix-test-proof
                root workspace root-fd record (pg::physical-reseed-follow-snapshot-at root-fd)
                (getf (getf record :retirement) :step) check t)))
         (ok (eq :alias (getf proof :location)))))))

(deftest reseed-retirement-alias-rejects-different-inodes-even-with-two-links-each
    (call-with-retirement-alias-test-states
     (lambda (kind root workspace root-fd saved record name check)
       (declare (ignore kind record))
       (let* ((source (merge-pathnames name root))
              (target (merge-pathnames name (pg::physical-image-fd-path saved)))
              (source-extra (merge-pathnames "alias-source-extra" workspace))
              (target-extra (merge-pathnames "alias-target-extra" workspace))
              (bytes (with-open-file (stream source :element-type '(unsigned-byte 8))
                       (let ((bytes (make-array (file-length stream) :element-type '(unsigned-byte 8))))
                         (read-sequence bytes stream)
                         bytes))))
         (delete-file target)
         (unwind-protect
              (progn
                (reseed-follow-owner-test-write target bytes)
                (sb-posix:link (namestring source) (namestring source-extra))
                (sb-posix:link (namestring target) (namestring target-extra))
                (ok (= 2 (sb-posix:stat-nlink (sb-posix:stat (namestring source)))))
                (ok (= 2 (sb-posix:stat-nlink (sb-posix:stat (namestring target)))))
                (ok (/= (sb-posix:stat-ino (sb-posix:stat (namestring source)))
                        (sb-posix:stat-ino (sb-posix:stat (namestring target)))))
                (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
                  (ok (handler-case
                          (progn
                            (pg::physical-reseed-follow-retirement-alias-row-at
                             root-fd saved name pg::+physical-reseed-install-maximum-bytes+ check)
                            nil)
                        (error (condition)
                          (search "exactly two private names of one inode" (princ-to-string condition)))))
                  (ok (reseed-activation-layout-facts-equal-p
                       before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))))
           (when (probe-file source-extra) (delete-file source-extra))
           (when (probe-file target-extra) (delete-file target-extra))
           (when (probe-file target) (delete-file target))
           (sb-posix:link (namestring source) (namestring target)))))))

(deftest reseed-retirement-layout-refuses-an-alias-of-the-nonpending-journal
    (call-with-retirement-alias-test-states
     (lambda (kind root workspace root-fd saved record name check)
       (declare (ignore record name))
       (let* ((other (if (eq kind :install) "physical-reseed.attempt" "physical-reseed.install"))
              (source (merge-pathnames other (if (eq kind :install) root (pg::physical-image-fd-path saved))))
              (target (merge-pathnames other (if (eq kind :install) (pg::physical-image-fd-path saved) root)))
              (entered nil))
         (sb-posix:link (namestring source) (namestring target))
         (unwind-protect
              (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retirement-observation
                           root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                           (lambda (&rest arguments)
                             (declare (ignore arguments))
                             (setf entered t)))
                          nil)
                      (error (condition)
                        (search (if (eq kind :install)
                                    "unique complete archive prefix"
                                    "install requires one root or saved location")
                                (princ-to-string condition)))))
                (ng entered)
                (ok (reseed-activation-layout-facts-equal-p
                     before (mapcar #'reseed-mapped-test-tree-facts (list root workspace)))))
           (delete-file target))))))

(deftest reseed-retirement-alias-holder-rechecks-callback-mutation-before-return
    (call-with-retirement-alias-test-states
     (lambda (kind root workspace root-fd saved record name check)
       (declare (ignore kind root workspace name))
       (let ((entered nil) (escaped nil) (held-fd nil))
         (ok (handler-case
                 (progn
                   (pg::call-with-physical-reseed-follow-retirement-alias-file
                    root-fd saved record
                    (lambda (stream state verify)
                      (declare (ignore state))
                      (setf entered t escaped verify held-fd (sb-sys:fd-stream-fd stream))
                      (let ((before (pg::physical-reseed-file-facts (sb-posix:fstat held-fd))))
                        (sleep 1.1)
                        (sb-posix:fchmod held-fd #o600)
                        (let ((after (pg::physical-reseed-file-facts (sb-posix:fstat held-fd))))
                          (ok (equal (butlast before) (butlast after)))
                          (ok (/= (car (last before)) (car (last after))))))
                      :changed)
                    check)
                   nil)
               (error (condition)
                 (search "retirement alias changed its held nanosecond facts"
                         (princ-to-string condition)))))
         (ok entered)
         (ok (handler-case (progn (sb-posix:fstat held-fd) nil)
               (sb-posix:syscall-error () t)))
         (ok (handler-case (progn (funcall escaped) nil)
               (error (condition) (search "scope ended" (princ-to-string condition)))))
         (ok (funcall check))))))

(deftest reseed-startup-projection-preserves-explicit-retirement-alias-location
    (multiple-value-bind (descriptors state) (reseed-startup-projection-fixture 30)
      (setf (getf state :location) :alias)
      (let ((projection (pg::physical-reseed-follow-startup-projection descriptors state)))
        (ok (eq :alias (getf projection :location)))
        (setf (getf state :location) :source)
        (ok (eq :alias (getf projection :location))))))

(deftest reseed-retirement-alias-survives-real-before-and-owned-pid-handover
    (dolist (step '(0 1))
      (call-with-retirement-intent-test-owner
       (lambda (owner root root-fd snapshot)
         (declare (ignore root root-fd))
         (let ((evidence (pg::retirement-startup-evidence owner)))
           (ok (pg::postmaster-pid-owned-p))
           (ok (eq :alias (getf evidence :location)))
           (ok (= step (getf evidence :prefix)))
           (ok (equal snapshot (getf evidence :snapshot)))
           (pg::call-with-physical-reseed-follow-retirement-startup-observation
            owner
            (lambda (descriptors state verify)
              (ok (equal evidence (pg::physical-reseed-follow-startup-projection descriptors state)))
              (ok (funcall verify))))))
       (lambda (root root-fd snapshot)
         (let* ((record (reseed-retirement-test-initial-candidate root-fd snapshot))
                (saved (merge-pathnames "saved/"
                                        (getf (getf (getf record :configuration) :workspace) :directory)))
                (reference (getf (getf record :retirement) :attempt-file)))
           (when (= step 1)
             (let ((fd (pg::physical-backup-open-directory saved)))
               (unwind-protect
                    (unless (zerop (pg::physical-adoption-renameat2
                                    root-fd "physical-reseed.install" fd "physical-reseed.install"
                                    pg::+physical-adoption-rename-noreplace+))
                      (error "alias startup fixture archive failed"))
                 (sb-posix:close fd)))
             (setf record (pg::physical-reseed-follow-retirement-next-record record reference)))
           (reseed-retirement-layout-write root record)
           (let ((name (if (zerop step) "physical-reseed.install" "physical-reseed.attempt")))
             (sb-posix:link (namestring (merge-pathnames name root))
                            (namestring (merge-pathnames name saved)))))))))

(deftest reseed-retirement-test-startup-scope-reopens-and-revokes-each-owner
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace fixture-fd original check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let ((owners nil))
         (dotimes (index 2)
           (declare (ignorable index))
           (let ((captured nil) (held nil))
             (call-with-retirement-intent-published-owner
              root
              (lambda (owner directory root-fd snapshot)
                (setf captured owner held root-fd)
                (push owner owners)
                (ok (equal root directory))
                (ok (equal original snapshot))
                (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat root-fd))))
                (ok (pg::postmaster-pid-owned-p))))
             (ok (not (pg::retirement-startup-active-p captured)))
             (ok (handler-case (progn (sb-posix:fstat held) nil)
                   (sb-posix:syscall-error () t)))
             (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat fixture-fd))))))
         (ok (not (eq (first owners) (second owners))))))))

(defun retirement-intent-test-nonjournal-facts (root)
  (let* ((excluded '("physical-reseed.follow" "physical-reseed.follow.tmp" "postmaster.pid"))
         (facts (reseed-mapped-test-tree-facts root))
         (directory (first facts)))
    (setf (second directory) (subseq (second directory) 0 7)
          (third directory) (sort (set-difference (third directory) excluded :test #'equal) #'string<))
    (dolist (entry (rest facts))
      (when (and (eq :file (first entry)) (equal ".pg_lisp.lock" (first (second entry))))
        (setf (second entry) (butlast (second entry)))))
    (remove-if (lambda (entry)
                 (and (eq :file (first entry))
                      (member (first (second entry)) excluded :test #'equal)))
               facts)))

(defun retirement-intent-test-reset (root record)
  (let ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
        (final (merge-pathnames "physical-reseed.follow" root)))
    (when (pg::physical-adoption-path-present-p temporary) (delete-file temporary))
    (unless (equalp (pg::physical-reseed-follow-image record) (pg::recovery-read-bytes final))
      (reseed-test-replace-follow-record root record))
    (pg::fsync-directory root)))

(deftest reseed-retirement-crash-child-cache-retains-source-directory-hierarchy
    (with-physical-store-directory (driver)
      (let* ((cache (merge-pathnames "cache/" driver))
             (arguments (retirement-intent-test-child-arguments
                         driver :after-retirement-rename cache (merge-pathnames "marker" driver)))
             (*package* (find-package :cl-user))
             (*read-eval* nil)
             (forms (loop for (flag value) on (nthcdr 4 arguments) by #'cddr
                          when (string= flag "--eval") collect (read-from-string value)))
             (form (find 'asdf:initialize-output-translations forms :key #'first))
             (configuration (second (second form)))
             (destination (second (second configuration))))
        (ok (stringp destination))
        (ok (equal (namestring cache) destination)))))

(deftest reseed-retirement-crash-baseline-allows-only-the-lock-ctime-change
    (with-physical-store-directory (root)
      (let ((path (merge-pathnames ".pg_lisp.lock" root)))
        (reseed-follow-owner-test-write path #(1))
        (let ((before (retirement-intent-test-nonjournal-facts root))
              (ctime (sb-posix:stat-ctime (sb-posix:stat path))))
          (sleep 1.1)
          (let ((fd (sb-posix:open (namestring path) sb-posix:o-rdonly)))
            (unwind-protect (sb-posix:fchmod fd #o600) (sb-posix:close fd)))
          (ok (/= ctime (sb-posix:stat-ctime (sb-posix:stat path))))
          (ok (reseed-activation-layout-facts-equal-p before (retirement-intent-test-nonjournal-facts root)))
          (sb-posix:chmod (namestring path) #o640)
          (ok (not (reseed-activation-layout-facts-equal-p before (retirement-intent-test-nonjournal-facts root))))
          (sb-posix:chmod (namestring path) #o600)
          (with-open-file (out path :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
            (write-byte 2 out))
          (ok (not (reseed-activation-layout-facts-equal-p before (retirement-intent-test-nonjournal-facts root))))))))

(defun retirement-intent-test-child-arguments (root point cache marker &optional (operation :initial))
  (let* ((directories
           (remove-duplicates
            (remove nil (mapcar #'asdf:system-source-directory (asdf:already-loaded-systems)))
            :test #'equal))
         (registry `(:source-registry
                     ,@(mapcar (lambda (directory) `(:directory ,directory)) directories)
                     :ignore-inherited-configuration))
         (forms
           (list
            '(require :asdf)
            `(asdf:initialize-source-registry ',registry)
            `(asdf:initialize-output-translations
              '(:output-translations (t ,(namestring cache)) :ignore-inherited-configuration))
            '(asdf:load-system "pg-physical-tests")
            `(let ((pg::*physical-reseed-follow-test-hook*
                     (lambda (event path)
                       (declare (ignore path))
                       (when (eq event ,point)
                         (with-open-file (out ,marker :direction :output :if-exists :error)
                           (write (list (sb-posix:getpid) ,point) :stream out :readably t))
                         (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                         (error "retirement child SIGKILL returned")))))
               (format t "~%RETIREMENT-CRASH-CHILD ~d ~s ~s~%" (sb-posix:getpid) ,root ,point)
               (finish-output)
               (call-with-retirement-intent-published-owner
                ,root
                (lambda (owner directory root-fd snapshot)
                  (declare (ignore directory))
                  (let* ((record (getf snapshot :record))
                         (candidate
                           (pg::physical-reseed-follow-retirement-next-record
                            record (pg::physical-reseed-follow-retirement-next-reference root-fd record)))
                         (pg::*physical-reseed-follow-write-limit*
                           (ceiling (length (pg::physical-reseed-follow-image candidate)) 2)))
                    ,(ecase operation
                       (:initial '(pg::physical-reseed-follow-retirement-publish-initial owner))
                       (:archive '(pg::physical-reseed-follow-retirement-archive-install owner))))))
               (error "retirement child did not reach its crash point")))))
    (let ((*package* (find-package :cl-user))
          (*print-length* nil) (*print-level* nil) (*print-readably* t))
      (append '("--noinform" "--no-sysinit" "--no-userinit" "--non-interactive")
              (loop for form in forms append (list "--eval" (write-to-string form)))))))

(defun run-retirement-intent-test-crash-child (root point cache marker &key (timeout 300) (operation :initial))
  (let ((process nil) (pid nil))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf process
                   (sb-ext:run-program
                    (namestring sb-ext:*runtime-pathname*)
                    (if (eq operation :initial)
                        (retirement-intent-test-child-arguments root point cache marker)
                        (retirement-intent-test-child-arguments root point cache marker operation))
                    :search t :wait nil :output t :error :output)
                   pid (sb-ext:process-pid process)))
           (format t "~%RETIREMENT-CRASH-OWNED-CHILD ~d ~s ~s~%" pid root point)
           (finish-output)
           (loop with deadline = (+ (get-internal-real-time) (* timeout internal-time-units-per-second))
                 while (member (sb-ext:process-status process) '(:running :stopped))
                 do (when (>= (get-internal-real-time) deadline)
                      (error "retirement crash child did not exit within ~a seconds" timeout))
                    (sleep 0.05))
           (sb-ext:process-wait process)
           (assert (eq :signaled (sb-ext:process-status process)))
           (assert (= sb-posix:sigkill (sb-ext:process-exit-code process)))
           (with-open-file (input marker)
             (let ((*read-eval* nil) (*package* (find-package :cl-user)))
               (assert (equal (list pid point) (read input)))
               (assert (eq :eof (read input nil :eof)))))
           pid)
      (when process
        (when (member (sb-ext:process-status process) '(:running :stopped))
          (sb-ext:process-kill process sb-posix:sigkill))
        (sb-ext:process-wait process)
        (sb-ext:process-close process)))))

(deftest reseed-retirement-crash-child-timeout-kills-reaps-and-closes-the-owned-process
    (with-physical-store-directory (driver)
      (let ((original-arguments (symbol-function 'retirement-intent-test-child-arguments))
            (original-run (symbol-function 'sb-ext:run-program))
            (original-close (symbol-function 'sb-ext:process-close))
            (captured nil) (closed 0) (started 0))
        (sb-ext:with-unlocked-packages (:sb-ext)
          (unwind-protect
               (progn
                 (setf (symbol-function 'retirement-intent-test-child-arguments)
                       (lambda (&rest values)
                         (declare (ignore values))
                         '("--noinform" "--no-sysinit" "--no-userinit" "--non-interactive" "--eval" "(sleep 60)"))
                       (symbol-function 'sb-ext:run-program)
                       (lambda (&rest values)
                         (incf started)
                         (setf captured (apply original-run values)))
                       (symbol-function 'sb-ext:process-close)
                       (lambda (process)
                         (when (eq process captured) (incf closed))
                         (funcall original-close process)))
                 (ok (handler-case
                         (progn
                           (run-retirement-intent-test-crash-child
                            driver :after-retirement-rename (merge-pathnames "cache/" driver)
                            (merge-pathnames "marker" driver) :timeout 0.05)
                           nil)
                       (error (condition) (search "did not exit within" (princ-to-string condition))))))
            (setf (symbol-function 'retirement-intent-test-child-arguments) original-arguments
                  (symbol-function 'sb-ext:run-program) original-run
                  (symbol-function 'sb-ext:process-close) original-close)))
        (ok (= 1 started))
        (ok (= 1 closed))
        (ok (eq :signaled (sb-ext:process-status captured)))
        (ok (= sb-posix:sigkill (sb-ext:process-exit-code captured))))))

(defun call-with-retirement-intent-published-owner (root function)
  (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                            (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                            (pg::*postmaster-pid-inode* nil) (pg::*postmaster-start-time* nil)
                            (pg::*postmaster-start-timestamp* nil) (pg::*postmaster-pid-host* nil)
                            (pg::*postmaster-pid-port* nil) (hooks sb-ext:*exit-hooks*)
                            (before nil) (after nil))
    (unwind-protect
         (pg::postmaster-own-data-directory
          "127.0.0.1" 5439
          :startup-observer
          (lambda (lock phase)
            (if (eq phase :before-pid)
                (setf before (pg::physical-reseed-follow-startup-observation lock phase))
                (setf after (pg::physical-reseed-follow-startup-observation lock phase before))))
          :startup-reconcile
          (lambda ()
            (pg::call-with-physical-reseed-follow-retirement-startup-owner
             after
             (lambda (owner)
               (pg::call-with-physical-reseed-follow-retirement-startup-root
                owner
                (lambda (root-fd check)
                  (funcall check)
                  (let ((snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))
                    (pg::physical-reseed-follow-check-snapshot
                     root-fd (getf (pg::retirement-startup-evidence owner) :snapshot))
                    (funcall function owner root root-fd snapshot))))))))
      (unwind-protect
           (when pg::*postmaster-pid-stream* (pg::postmaster-remove-pid-file))
        (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
        (setf sb-ext:*exit-hooks* hooks)))))

(defun call-with-retirement-intent-test-owner (function &optional before-startup (kind :same-timeline))
  (call-with-reseed-retirement-observation-fixture
   kind
   (lambda (offline root workspace root-fd snapshot check)
     (declare (ignore workspace check))
     (close (pg::physical-adoption-owner-lock offline))
     (setf (pg::physical-adoption-owner-active-p offline) nil)
     (when before-startup (funcall before-startup root root-fd snapshot))
     (call-with-retirement-intent-published-owner root function))))

(defun check-retirement-operation-busy-cuts (owner)
  (dolist (mode '(:error :throw :interrupt))
    (let ((entered nil) (fired nil))
      (unwind-protect
           (let ((pg::*physical-reseed-follow-test-hook*
                   (lambda (event path)
                     (declare (ignore path))
                     (when (eq event :after-retirement-operation-busy)
                       (setf fired t)
                       (ecase mode
                         (:error (error "retirement busy cut"))
                         (:throw (throw 'retirement-busy-cut :cut))
                         (:interrupt
                          (sb-thread:interrupt-thread
                           sb-thread:*current-thread*
                           (lambda () (throw 'retirement-busy-cut :cut)))))))))
             (ok (eq :cut
                     (catch 'retirement-busy-cut
                       (handler-case
                           (pg::call-with-physical-reseed-follow-retirement-operation
                            owner (lambda () (setf entered t)))
                         (error (condition)
                           (if (search "retirement busy cut" (princ-to-string condition))
                               :cut (error condition)))))))
             (ok fired)
             (ok (not entered))
             (let ((busy (pg::retirement-startup-busy-p owner)))
               (ok (null busy))))
        (setf (pg::retirement-startup-busy-p owner) nil)))))

(deftest reseed-retirement-operation-blocks-reentry-and-retains-uncertainty
    (call-with-retirement-intent-test-owner
     (lambda (owner root root-fd snapshot)
       (declare (ignore root root-fd snapshot))
       (check-retirement-operation-busy-cuts owner)
       (let ((entered nil))
         (ok (eq :finished
                 (pg::call-with-physical-reseed-follow-retirement-operation
                  owner
                  (lambda ()
                    (ok (funcall (symbol-function 'pg::retirement-startup-busy-p) owner))
                    (ok (handler-case
                            (progn
                              (pg::call-with-physical-reseed-follow-retirement-operation
                               owner (lambda () (setf entered t)))
                              nil)
                          (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                    :finished))))
         (ok (not entered))
         (ok (not (funcall (symbol-function 'pg::retirement-startup-busy-p) owner))))
       (dolist (mode '(:error :throw :interrupt))
         (ok
          (eq :cut
              (catch 'retirement-operation-cut
                (handler-case
                    (pg::call-with-physical-reseed-follow-retirement-operation
                     owner
                     (lambda ()
                       (ecase mode
                         (:error (error "retirement operation cut"))
                         (:throw (throw 'retirement-operation-cut :cut))
                         (:interrupt
                          (sb-thread:interrupt-thread
                           sb-thread:*current-thread*
                           (lambda () (throw 'retirement-operation-cut :cut)))))))
                  (error (condition)
                    (if (search "retirement operation cut" (princ-to-string condition))
                        :cut (error condition)))))))
         (ok (not (funcall (symbol-function 'pg::retirement-startup-busy-p) owner)))
         (ok (not (funcall (symbol-function 'pg::retirement-startup-uncertain-p) owner))))
       (let ((escaped nil))
         (ok (handler-case
                 (progn
                   (pg::call-with-physical-reseed-follow-retirement-startup-owner
                    (list :branch :retirement :evidence (pg::retirement-startup-evidence owner))
                    (lambda (inner)
                      (setf escaped inner)
                      (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) t inner)
                      :unfinished))
                   nil)
               (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
         (ok escaped)
         (ok (not (pg::retirement-startup-active-p escaped)))
         (ok (pg::physical-reseed-follow-retirement-check-startup-owner owner)))
       (unwind-protect
            (progn
              (ok (handler-case
                      (progn
                        (pg::call-with-physical-reseed-follow-retirement-operation
                         owner
                         (lambda ()
                           (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) t owner)
                           :unfinished))
                        nil)
                    (error (condition) (search "durable reconciliation" (princ-to-string condition)))))
              (ok (funcall (symbol-function 'pg::retirement-startup-uncertain-p) owner))
              (ok (not (funcall (symbol-function 'pg::retirement-startup-busy-p) owner))))
         (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) nil owner))
       (unwind-protect
            (progn
              (ok (handler-case
                      (progn
                        (pg::call-with-physical-reseed-follow-retirement-operation
                         owner
                         (lambda ()
                           (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) t owner)
                           (error "uncertain operation cut")))
                        nil)
                    (error (condition) (search "uncertain operation cut" (princ-to-string condition)))))
              (ok (not (funcall (symbol-function 'pg::retirement-startup-busy-p) owner)))
              (ok (funcall (symbol-function 'pg::retirement-startup-uncertain-p) owner))
              (let ((entered nil))
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retirement-operation
                           owner (lambda () (setf entered t)))
                          nil)
                      (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                (ok (not entered))))
         (funcall (fdefinition '(setf pg::retirement-startup-uncertain-p)) nil owner)))))

(deftest reseed-retirement-startup-owner-printing-keeps-handover-evidence-private
    (let* ((secret "retirement-owner-print-test-secret")
           (owner (pg::make-physical-reseed-follow-retirement-startup-owner
                   :evidence (list :snapshot (list :record (list :private-test-value secret)))))
           (*print-level* nil) (*print-length* nil) (*print-readably* nil)
           (text (write-to-string owner)))
      (ok (search "physical-reseed-follow-retirement-startup-owner" (string-downcase text)))
      (ok (not (search secret text)))))

(deftest reseed-retirement-held-file-facts-survive-unlink-and-require-the-mount
    (with-physical-store-directory (root)
      (let* ((path (merge-pathnames "retirement-held-test" root))
             (root-fd (pg::physical-backup-open-directory root)))
        (unwind-protect
             (with-open-file (stream path :direction :io :if-exists :error
                                          :element-type '(unsigned-byte 8))
               (write-sequence #(1 2 3) stream)
               (finish-output stream)
               (let* ((fd (sb-sys:fd-stream-fd stream))
                      (mount (pg::physical-adoption-install-mount-id nil :directory-fd root-fd))
                      (named (pg::physical-reseed-install-old-observe root-fd "retirement-held-test" mount))
                      (before (pg::physical-reseed-follow-retirement-held-file-facts fd mount)))
                 (ok (equal (rest named) before))
                 (ok (= 1 (fourth before)))
                 (ok
                  (handler-case
                      (progn (pg::physical-reseed-follow-retirement-held-file-facts fd (1+ mount)) nil)
                    (error (condition) (search "target mount" (princ-to-string condition)))))
                 (delete-file path)
                 (let ((after (pg::physical-reseed-follow-retirement-held-file-facts fd mount)))
                   (ok (zerop (fourth after)))
                   (ok (equal (subseq before 0 3) (subseq after 0 3)))
                   (ok (equal (subseq before 4 7) (subseq after 4 7)))
                   (ok (not (probe-file path))))))
          (sb-posix:close root-fd)))))

(deftest reseed-retirement-journal-checks-parent-before-descriptor-use
    (let ((entered nil))
      (ok
       (eq :parent
           (catch 'retirement-journal-parent
             (pg::call-with-physical-reseed-follow-retirement-journal
              nil "physical-reseed.follow" nil
              (lambda (stream state)
                (declare (ignore stream state))
                (setf entered t))
              (lambda () (throw 'retirement-journal-parent :parent))))))
      (ok (not entered))))

(defun check-retirement-journal-open-cuts (root-fd snapshot check)
  (let ((original (symbol-function 'pg::physical-image-openat)))
    (dolist (mode '(:error :throw :interrupt))
      (let ((held nil) (injected nil) (entered nil))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-image-openat)
                     (lambda (&rest arguments)
                       (let ((fd (apply original arguments)))
                         (when (and (equal (second arguments) "physical-reseed.follow")
                                    (not (minusp fd)))
                           (setf held fd))
                         fd)))
               (let ((pg::*physical-reseed-follow-test-hook*
                       (lambda (point path)
                         (declare (ignore path))
                         (when (eq point :after-retirement-journal-open)
                           (setf injected t)
                           (ecase mode
                             (:error (error "retirement journal open cut"))
                             (:throw (throw 'retirement-journal-open-cut :cut))
                             (:interrupt
                              (sb-thread:interrupt-thread
                               sb-thread:*current-thread*
                               (lambda () (throw 'retirement-journal-open-cut :cut)))))))))
                 (ok
                  (eq :cut
                      (catch 'retirement-journal-open-cut
                        (handler-case
                            (pg::call-with-physical-reseed-follow-retirement-journal
                             root-fd "physical-reseed.follow" snapshot
                             (lambda (stream state)
                               (declare (ignore stream state))
                               (setf entered t))
                             check)
                          (error (condition)
                            (if (search "retirement journal open cut" (princ-to-string condition))
                                :cut (error condition)))))))))
          (setf (symbol-function 'pg::physical-image-openat) original))
        (ok injected)
        (ok (not entered))
        (ok (and held (handler-case (progn (sb-posix:fstat held) nil)
                        (sb-posix:syscall-error () t))))
        (ok (funcall check))
        (ok (pg::postmaster-pid-owned-p))))))

(deftest reseed-retirement-journal-scope-binds-the-held-file-and-closes-it-on-exit
    (call-with-retirement-intent-test-owner
     (lambda (owner root fixture-fd snapshot)
       (declare (ignore root fixture-fd))
       (pg::call-with-physical-reseed-follow-retirement-operation
        owner
        (lambda ()
          (pg::call-with-physical-reseed-follow-retirement-startup-root
           owner
           (lambda (root-fd check)
             (check-retirement-journal-open-cuts root-fd snapshot check)
             (dolist (mode '(:normal :error :throw :interrupt))
               (let ((held nil) (stream nil))
                 (ok
                  (eq :finished
                      (catch 'retirement-journal-cut
                        (handler-case
                            (pg::call-with-physical-reseed-follow-retirement-journal
                             root-fd "physical-reseed.follow" snapshot
                             (lambda (opened state)
                               (setf stream opened held (sb-sys:fd-stream-fd opened))
                               (ok (equal snapshot state))
                               (ok (logbitp 0 (sb-posix:fcntl held sb-posix:f-getfd)))
                               (ecase mode
                                 (:normal :finished)
                                 (:error (error "retirement journal cut"))
                                 (:throw (throw 'retirement-journal-cut :finished))
                                 (:interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (throw 'retirement-journal-cut :finished))))))
                             check)
                          (error (condition)
                            (if (search "retirement journal cut" (princ-to-string condition))
                                :finished (error condition)))))))
                 (ok (not (open-stream-p stream)))
                 (ok (handler-case (progn (sb-posix:fstat held) nil)
                       (sb-posix:syscall-error () t)))
                 (ok (funcall check))
                 (ok (pg::postmaster-pid-owned-p)))))))))))

(deftest reseed-retirement-journal-rejects-same-record-name-replacement
    (call-with-retirement-intent-test-owner
     (lambda (owner root fixture-fd snapshot)
       (declare (ignore fixture-fd))
       (pg::call-with-physical-reseed-follow-retirement-operation
        owner
        (lambda ()
          (pg::call-with-physical-reseed-follow-retirement-startup-root
           owner
           (lambda (root-fd check)
             (let ((original-open (symbol-function 'pg::physical-image-openat))
                   (final (merge-pathnames "physical-reseed.follow" root))
                   (backup (merge-pathnames "retirement-test-held-original" root))
                   (bytes (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))
                   (held nil) (injected nil) (entered nil))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::physical-image-openat)
                            (lambda (&rest arguments)
                              (let ((fd (apply original-open arguments)))
                                (when (and (equal (second arguments) "physical-reseed.follow")
                                           (not (minusp fd)))
                                  (setf held fd))
                                fd)))
                      (let ((pg::*physical-reseed-follow-test-hook*
                              (lambda (point path)
                                (declare (ignore path))
                                (when (eq point :after-retirement-journal-open)
                                  (sb-posix:rename (namestring final) (namestring backup))
                                  (setf injected t)
                                  (reseed-follow-owner-test-write final bytes)))))
                        (ok
                         (handler-case
                             (progn
                               (pg::call-with-physical-reseed-follow-retirement-journal
                                root-fd "physical-reseed.follow" snapshot
                                (lambda (stream state)
                                  (declare (ignore stream state))
                                  (setf entered t))
                                check)
                               nil)
                           (error (condition)
                             (search "differs from its held descriptor" (princ-to-string condition))))))
                      (ok injected)
                      (ok (not entered))
                      (ok (and held (handler-case (progn (sb-posix:fstat held) nil)
                                      (sb-posix:syscall-error () t))))
                      (ok (equalp bytes (pg::recovery-read-bytes final)))
                      (ok (/= (sb-posix:stat-ino (sb-posix:stat (namestring final)))
                              (sb-posix:stat-ino (sb-posix:stat (namestring backup)))))
                      (ok (funcall check))
                      (ok (pg::postmaster-pid-owned-p)))
                 (setf (symbol-function 'pg::physical-image-openat) original-open)
                 (when injected
                   (when (probe-file final) (delete-file final))
                   (sb-posix:rename (namestring backup) (namestring final))))))))))))

(deftest reseed-retirement-install-archive-publishes-progress-under-owned-pid
    (call-with-retirement-intent-test-owner
     (lambda (owner root root-fd snapshot)
       (let* ((before (getf snapshot :record))
              (reference (pg::physical-adoption-attempt-value-copy
                          (getf (getf before :retirement) :attempt-file)))
              (points nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (point path)
                  (declare (ignore path))
                  (push point points)))
              (result
                (handler-case
                    (funcall (symbol-function 'pg::physical-reseed-follow-retirement-archive-install) owner)
                  (error (condition)
                    (format t "~%RETIREMENT-ARCHIVE-INSTALL-REFUSAL ~a~%" condition)
                    nil))))
         (ok result)
         (when result
           (ok (eq :retiring (getf (getf result :retirement) :phase)))
           (ok (= 1 (getf (getf result :retirement) :step)))
           (ok (equal reference (getf (getf result :retirement) :attempt-file)))
           (ng (probe-file (merge-pathnames "physical-reseed.install" root)))
           (let* ((workspace (getf (getf result :configuration) :workspace))
                  (saved (merge-pathnames "saved/physical-reseed.install" (getf workspace :directory))))
             (ok (= 1 (sb-posix:stat-nlink (sb-posix:lstat (namestring saved)))))))
         (ng (pg::retirement-startup-uncertain-p owner))
         (ng (pg::retirement-startup-busy-p owner))
         (ok (equal result (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record)))
         (when result
           (let ((ordered (reverse points)))
             (ok (< (position :after-retirement-archive-saved-fsync ordered)
                    (position :before-retirement-archive-unlink ordered)
                    (position :after-retirement-archive-root-fsync ordered)
                    (position :before-retirement-old-final-fsync ordered))))
           (pg::call-with-physical-reseed-follow-retirement-startup-observation
            owner
            (lambda (descriptors state verify)
              (declare (ignore descriptors))
              (ok (= 1 (getf state :prefix)))
              (ok (funcall verify)))))))
     (lambda (root root-fd snapshot)
       (reseed-retirement-layout-write
        root (reseed-retirement-test-initial-candidate root-fd snapshot)))))

(defun prepare-retirement-install-archive-test (root root-fd snapshot geometry)
  (let* ((record (reseed-retirement-test-initial-candidate root-fd snapshot))
         (workspace (getf (getf record :configuration) :workspace))
         (saved (merge-pathnames "saved/" (getf workspace :directory)))
         (source (merge-pathnames "physical-reseed.install" root))
         (target (merge-pathnames "physical-reseed.install" saved)))
    (reseed-retirement-layout-write root record)
    (ecase geometry
      (:source)
      ((:alias :saved :temporary)
       (sb-posix:link (namestring source) (namestring target))
       (unless (eq geometry :alias) (delete-file source))))
    (when (eq geometry :temporary)
      (reseed-follow-owner-test-write
       (merge-pathnames "physical-reseed.follow.tmp" root)
       (pg::physical-reseed-follow-image
        (pg::physical-reseed-follow-retirement-next-record
         record (getf (getf record :retirement) :attempt-file)))))
    record))

(deftest reseed-retirement-install-archive-recovers-alias-saved-and-complete-temporary
    (dolist (geometry '(:alias :saved :temporary))
      (call-with-retirement-intent-test-owner
       (lambda (owner root root-fd snapshot)
         (let* ((link (symbol-function 'pg::port-linkat))
                (unlink (symbol-function 'pg::port-unlinkat))
                (fsync (symbol-function 'sb-posix:fsync))
                (links 0) (unlinks 0) (syncs 0) (points nil)
                (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                (temporary-inode (when (probe-file temporary)
                                   (sb-posix:stat-ino (sb-posix:lstat (namestring temporary)))))
                (reference (pg::physical-adoption-attempt-value-copy
                            (getf (getf (getf snapshot :record) :retirement) :attempt-file))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::port-linkat)
                        (lambda (&rest arguments) (incf links) (apply link arguments))
                        (symbol-function 'pg::port-unlinkat)
                        (lambda (&rest arguments) (incf unlinks) (apply unlink arguments))
                        (symbol-function 'sb-posix:fsync)
                        (lambda (fd) (incf syncs) (funcall fsync fd)))
                  (let* ((pg::*physical-reseed-follow-test-hook*
                           (lambda (event path) (declare (ignore path)) (push event points)))
                         (values
                           (handler-case
                               (multiple-value-list (pg::physical-reseed-follow-retirement-archive-install owner))
                             (error (condition)
                               (format t "~%RETIREMENT-ARCHIVE-RECOVERY-REFUSAL ~s ~a~%" geometry condition)
                               nil)))
                         (record (first values))
                         (actual (second values))
                         (ordered (reverse points)))
                    (ok record)
                    (when record
                      (ok (= 1 (getf (getf record :retirement) :step)))
                      (ok (equal reference (getf (getf record :retirement) :attempt-file)))
                      (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                      (ok (zerop links))
                      (ok (= (if (eq geometry :alias) 1 0) unlinks))
                      (ok (< (position :after-retirement-archive-saved-fsync ordered)
                             (position :before-retirement-old-final-fsync ordered)))
                      (when (eq geometry :alias)
                        (ok (< (position :after-retirement-archive-saved-fsync ordered)
                               (position :before-retirement-archive-unlink ordered))))
                      (when temporary-inode
                        (ok (= temporary-inode (second (getf actual :identity))))
                        (ng (member :after-retirement-temporary-create ordered)))
                      (ng (probe-file temporary))
                      (ng (probe-file (merge-pathnames "physical-reseed.install" root)))
                      (ng (pg::retirement-startup-uncertain-p owner))
                      (setf links 0 unlinks 0 syncs 0)
                      (multiple-value-bind (retry retry-snapshot)
                          (pg::physical-reseed-follow-retirement-archive-install owner)
                        (ok (equal record retry))
                        (ok (equal actual retry-snapshot)))
                      (ok (zerop links))
                      (ok (zerop unlinks))
                      (ok (zerop syncs)))))
             (setf (symbol-function 'pg::port-linkat) link
                   (symbol-function 'pg::port-unlinkat) unlink
                   (symbol-function 'sb-posix:fsync) fsync))))
       (lambda (root root-fd snapshot)
         (prepare-retirement-install-archive-test root root-fd snapshot geometry)))))

(deftest reseed-retirement-install-archive-completes-every-activation-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-retirement-intent-test-owner
       (lambda (owner root root-fd snapshot)
         (let* ((original (getf snapshot :record))
                (reference (pg::physical-adoption-attempt-value-copy
                            (getf (getf original :retirement) :attempt-file))))
           (multiple-value-bind (record actual)
               (pg::physical-reseed-follow-retirement-archive-install owner)
             (ok (= 1 (getf (getf record :retirement) :step)))
             (ok (equal reference (getf (getf record :retirement) :attempt-file)))
             (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))
             (ng (probe-file (merge-pathnames "physical-reseed.install" root)))
             (let* ((workspace (getf (getf record :configuration) :workspace))
                    (saved (merge-pathnames "saved/physical-reseed.install" (getf workspace :directory)))
                    (stat (sb-posix:lstat (namestring saved))))
               (ok (= (second (getf (getf original :install) :identity)) (sb-posix:stat-ino stat)))
               (ok (= 1 (sb-posix:stat-nlink stat))))
             (ng (pg::retirement-startup-uncertain-p owner))
             (ng (pg::retirement-startup-busy-p owner))
             (pg::call-with-physical-reseed-follow-retirement-startup-observation
              owner
              (lambda (descriptors state verify)
                (declare (ignore descriptors))
                (ok (= 1 (getf state :prefix)))
                (ok (funcall verify)))))))
       (lambda (root root-fd snapshot)
         (prepare-retirement-install-archive-test root root-fd snapshot :source))
       kind)))

(defun retirement-archive-test-reset (root record)
  (let* ((workspace (getf (getf record :configuration) :workspace))
         (source (merge-pathnames "physical-reseed.install" root))
         (saved (merge-pathnames "saved/physical-reseed.install" (getf workspace :directory))))
    (unless (probe-file source)
      (sb-posix:link (namestring saved) (namestring source)))
    (when (probe-file saved) (delete-file saved))
    (retirement-intent-test-reset root record)))

(defun retirement-archive-test-invariants (root workspace auxiliaries)
  (loop for directory in (append (list root workspace) auxiliaries)
        collect
        (let ((facts (reseed-mapped-test-tree-facts directory)))
          (loop for entry in facts
                for file = (second entry)
                for name = (first file)
                for root-p = (equal directory root)
                for workspace-p = (equal directory workspace)
                for excluded = (cond (root-p '("physical-reseed.follow" "physical-reseed.follow.tmp"
                                               "postmaster.pid" "physical-reseed.install"))
                                     (workspace-p '("saved/physical-reseed.install")))
                unless (and (eq :file (first entry)) (member name excluded :test #'equal))
                  collect
                  (progn
                    (when (and (eq :directory (first entry))
                               (or (and root-p (eq entry (first facts)))
                                   (and workspace-p (equal name "saved/"))))
                      (setf (second entry) (subseq file 0 7)
                            (third entry)
                            (remove-if (lambda (name)
                                         (member name
                                                 (if root-p '("physical-reseed.follow" "physical-reseed.follow.tmp"
                                                              "postmaster.pid" "physical-reseed.install")
                                                     '("physical-reseed.install"))
                                                 :test #'equal))
                                       (third entry))))
                    (when (and root-p (eq :file (first entry)) (equal name ".pg_lisp.lock"))
                      (setf (second entry) (butlast (second entry))))
                    entry)))))

(defun check-retirement-archive-test-location (root-fd record geometry original-row bytes)
  (let* ((workspace (getf (getf record :configuration) :workspace))
         (saved (pg::physical-backup-open-directory
                 (merge-pathnames "saved/" (getf workspace :directory))))
         (mount (pg::physical-adoption-install-mount-id nil :directory-fd root-fd)))
    (unwind-protect
         (loop for role in '(:root :saved)
               for fd in (list root-fd saved)
               for expected = (or (eq geometry :alias) (eq role (if (eq geometry :source) :root :saved)))
               for path = (merge-pathnames "physical-reseed.install" (pg::physical-image-fd-path fd))
               for present = (not (null (pg::physical-adoption-path-present-p path)))
               do (ok (eql expected present))
                  (when present
                    (let ((row (pg::physical-reseed-install-old-observe fd "physical-reseed.install" mount)))
                      (ok (= (if (eq geometry :alias) 2 1) (fifth row)))
                      (ok (equal (subseq original-row 0 4) (subseq row 0 4)))
                      (ok (equal (subseq original-row 5 8) (subseq row 5 8)))
                      (ok (equalp bytes (lineage-test-bytes path))))))
      (sb-posix:close saved))))

(defun check-retirement-archive-native-cuts (points)
  (call-with-reseed-retirement-observation-fixture
   :same-timeline
   (lambda (offline root workspace root-fd initial check)
     (declare (ignore check))
     (close (pg::physical-adoption-owner-lock offline))
     (setf (pg::physical-adoption-owner-active-p offline) nil)
     (let* ((record (prepare-retirement-install-archive-test root root-fd initial :source))
            (candidate (pg::physical-reseed-follow-retirement-next-record
                        record (getf (getf record :retirement) :attempt-file)))
            (plan (getf (pg::physical-reseed-install-read-at root-fd "physical-reseed.install") :plan))
            (auxiliaries (mapcar (lambda (key) (pathname (getf (getf plan key) :directory)))
                                 '(:incoming :quarantine)))
            (original-row (pg::physical-reseed-install-old-observe
                           root-fd "physical-reseed.install"
                           (pg::physical-adoption-install-mount-id nil :directory-fd root-fd)))
            (original-bytes (lineage-test-bytes (merge-pathnames "physical-reseed.install" root)))
            (baseline (retirement-archive-test-invariants root workspace auxiliaries)))
       (with-physical-store-directory (driver)
         (let ((cache (merge-pathnames "cache/" driver)))
           (dolist (point points)
             (retirement-archive-test-reset root record)
             (ok (reseed-activation-layout-facts-equal-p
                  baseline (retirement-archive-test-invariants root workspace auxiliaries)))
             (let* ((marker (merge-pathnames (format nil "~(~a~).marker" point) driver))
                    (pid (run-retirement-intent-test-crash-child
                          root point cache marker :operation :archive :timeout 600))
                    (stale (pg::physical-reseed-read-pid root))
                    (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                    (partial (member point '(:after-retirement-temporary-create :after-retirement-write-chunk)))
                    (renamed (member point '(:after-retirement-rename :after-retirement-candidate-fsync
                                             :after-retirement-directory-fsync :after-retirement-readback))))
               (ok (= pid (getf stale :pid)))
               (check-retirement-archive-test-location
                root-fd record
                (cond ((member point '(:after-retirement-archive-old-final-fsync
                                       :after-retirement-archive-baseline-root-fsync
                                       :after-retirement-archive-source-fsync)) :source)
                      ((member point '(:after-retirement-archive-link
                                       :after-retirement-archive-alias-fsync
                                       :after-retirement-archive-saved-fsync)) :alias)
                      (t :saved))
                original-row original-bytes)
               (ok (reseed-activation-layout-facts-equal-p
                    baseline (retirement-archive-test-invariants root workspace auxiliaries)))
               (if partial
                   (let ((entered nil) (bytes (lineage-test-bytes temporary))
                                       (temporary-row (nth-value 1 (pg::physical-reseed-follow-entry
                                                                    root-fd "physical-reseed.follow.tmp" t))))
                     (ok (handler-case
                             (progn
                               (call-with-retirement-intent-published-owner
                                root (lambda (&rest arguments)
                                       (declare (ignore arguments))
                                       (setf entered t)))
                               nil)
                           (error (condition)
                             (search
                              (if (eq point :after-retirement-temporary-create)
                                  "target follow journal is not a bounded private single-link regular file"
                                  "physical backup lease checksum mismatch")
                              (princ-to-string condition)))))
                     (ng entered)
                     (let ((after (pg::physical-reseed-read-pid root)))
                       (ok (= (getf stale :pid) (getf after :pid)))
                       (ok (equal (getf stale :identity) (getf after :identity)))
                       (ok (equalp (getf stale :bytes) (getf after :bytes))))
                     (ok (equalp bytes (lineage-test-bytes temporary)))
                     (ok (equal temporary-row
                                (nth-value 1 (pg::physical-reseed-follow-entry
                                              root-fd "physical-reseed.follow.tmp" t))))
                     (ok (equal record (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record))))
                   (call-with-retirement-intent-published-owner
                    root
                    (lambda (owner directory fd snapshot)
                      (declare (ignore directory))
                      (when renamed (ok (equal candidate (getf snapshot :record))))
                      (multiple-value-bind (published actual)
                          (pg::physical-reseed-follow-retirement-archive-install owner)
                        (ok (equal candidate published))
                        (ok (equal actual (pg::physical-reseed-follow-snapshot-at fd))))
                      (ng (probe-file temporary))
                      (ng (probe-file (merge-pathnames "physical-reseed.install" root)))
                      (ok (pg::postmaster-pid-owned-p)))))
               (check-retirement-archive-test-location root-fd record :saved original-row original-bytes)
               (ok (reseed-activation-layout-facts-equal-p
                    baseline (retirement-archive-test-invariants root workspace auxiliaries)))))
           (retirement-archive-test-reset root record)))))))

(deftest reseed-retirement-install-archive-survives-native-process-crashes
    (check-retirement-archive-native-cuts '(:after-retirement-archive-old-final-fsync
                                            :after-retirement-archive-baseline-root-fsync
                                            :after-retirement-archive-source-fsync
                                            :after-retirement-archive-link
                                            :after-retirement-archive-alias-fsync
                                            :after-retirement-archive-saved-fsync
                                            :after-retirement-archive-unlink
                                            :after-retirement-archive-inode-fsync
                                            :after-retirement-archive-root-fsync
                                            :after-retirement-old-final-fsync
                                            :after-retirement-predecessor-directory-fsync
                                            :after-retirement-temporary-create
                                            :after-retirement-write-chunk
                                            :after-retirement-temporary-fsync
                                            :after-retirement-rename
                                            :after-retirement-candidate-fsync
                                            :after-retirement-directory-fsync
                                            :after-retirement-readback)))

(deftest reseed-retirement-install-archive-preserves-partial-temporary-metadata-before-pid
    (check-retirement-archive-native-cuts
     '(:after-retirement-temporary-create :after-retirement-write-chunk)))

(defun check-retirement-archive-test-cut (root record point mode)
  (retirement-archive-test-reset root record)
  (catch 'retirement-archive-cut-checked
    (call-with-retirement-intent-published-owner
     root
     (lambda (owner directory root-fd snapshot)
       (declare (ignore directory))
       (let* ((workspace (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
              (plan (getf (pg::physical-reseed-install-read-at root-fd "physical-reseed.install") :plan))
              (auxiliaries (mapcar (lambda (key) (pathname (getf (getf plan key) :directory)))
                                   '(:incoming :quarantine)))
              (baseline (retirement-archive-test-invariants root workspace auxiliaries))
              (original-row (pg::physical-reseed-install-old-observe
                             root-fd "physical-reseed.install"
                             (pg::physical-adoption-install-mount-id nil :directory-fd root-fd)))
              (original-bytes (lineage-test-bytes (merge-pathnames "physical-reseed.install" root)))
              (candidate (pg::physical-reseed-follow-retirement-next-record
                          record (getf (getf record :retirement) :attempt-file)))
              (bytes (pg::physical-reseed-follow-image candidate))
              (pg::*physical-reseed-follow-write-limit* (ceiling (length bytes) 2))
              (injected nil)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event path)
                  (declare (ignore path))
                  (when (and (eq event point) (not injected))
                    (setf injected t)
                    (ecase mode
                      (:error (error "injected retirement archive cut"))
                      (:throw (throw 'retirement-archive-injected-cut :cut))
                      (:interrupt
                       (sb-thread:interrupt-thread
                        sb-thread:*current-thread*
                        (lambda () (error "injected retirement archive cut")))))))))
         (let ((outcome
                 (catch 'retirement-archive-injected-cut
                   (handler-case
                       (progn (pg::physical-reseed-follow-retirement-archive-install owner) :returned)
                     (error (condition)
                       (if (search "injected retirement archive cut" (princ-to-string condition))
                           :error
                           (progn
                             (format t "~%RETIREMENT-ARCHIVE-UNEXPECTED-CUT-ERROR ~s ~s ~a~%"
                                     point mode condition)
                             :wrong-error)))))))
           (ok (eq (if (eq mode :throw) :cut :error) outcome)))
         (ok injected)
         (ng (pg::retirement-startup-busy-p owner))
         (ok (pg::retirement-startup-uncertain-p owner))
         (ok (handler-case
                 (progn (pg::physical-reseed-follow-retirement-archive-install owner) nil)
               (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
         (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
         (ok (pg::postmaster-pid-owned-p))
         (check-retirement-archive-test-location
          root-fd record
          (cond ((member point '(:after-retirement-archive-old-final-fsync
                                 :after-retirement-archive-baseline-root-fsync
                                 :after-retirement-archive-source-fsync)) :source)
                ((member point '(:after-retirement-archive-link
                                 :after-retirement-archive-alias-fsync
                                 :after-retirement-archive-saved-fsync)) :alias)
                (t :saved))
          original-row original-bytes)
         (let* ((actual (pg::physical-reseed-follow-snapshot-at root-fd))
                (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                (renamed (member point '(:after-retirement-rename :after-retirement-candidate-fsync
                                         :after-retirement-directory-fsync :after-retirement-readback))))
           (if renamed
               (progn (ok (equal candidate (getf actual :record))) (ng (probe-file temporary)))
               (progn
                 (ok (equal snapshot actual))
                 (cond
                   ((eq point :after-retirement-temporary-create)
                    (ok (zerop (length (pg::read-bounded-regular-file temporary 1 :minimum 0)))))
                   ((eq point :after-retirement-write-chunk)
                    (ok (equalp (subseq bytes 0 (ceiling (length bytes) 2)) (lineage-test-bytes temporary))))
                   ((eq point :after-retirement-temporary-fsync)
                    (ok (equalp bytes (lineage-test-bytes temporary))))
                   (t (ng (probe-file temporary)))))))
         (ok (reseed-activation-layout-facts-equal-p
              baseline (retirement-archive-test-invariants root workspace auxiliaries)))
         (throw 'retirement-archive-cut-checked :checked))))))

(deftest reseed-retirement-install-archive-fences-error-throw-and-self-interrupt-cuts
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace root-fd initial check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let ((record (prepare-retirement-install-archive-test root root-fd initial :source)))
         (dolist (point '(:after-retirement-archive-old-final-fsync
                          :after-retirement-archive-baseline-root-fsync
                          :after-retirement-archive-source-fsync
                          :after-retirement-archive-link
                          :after-retirement-archive-alias-fsync
                          :after-retirement-archive-saved-fsync
                          :after-retirement-archive-unlink
                          :after-retirement-archive-inode-fsync
                          :after-retirement-archive-root-fsync
                          :after-retirement-old-final-fsync
                          :after-retirement-predecessor-directory-fsync
                          :after-retirement-temporary-create
                          :after-retirement-write-chunk
                          :after-retirement-temporary-fsync
                          :after-retirement-rename
                          :after-retirement-candidate-fsync
                          :after-retirement-directory-fsync
                          :after-retirement-readback))
           (ok (eq :checked (check-retirement-archive-test-cut root record point :error))))
         (dolist (mode '(:throw :interrupt))
           (dolist (point '(:after-retirement-archive-old-final-fsync
                            :after-retirement-archive-link
                            :after-retirement-archive-saved-fsync
                            :after-retirement-archive-unlink
                            :after-retirement-temporary-create
                            :after-retirement-readback))
             (ok (eq :checked (check-retirement-archive-test-cut root record point mode)))))
         (retirement-archive-test-reset root record)))))

(defun check-retirement-archive-syscall-failure (root record case)
  (destructuring-bind (operation event occurrence geometry final-step temporary-state role after-real) case
    (retirement-archive-test-reset root record)
    (catch 'retirement-archive-syscall-checked
      (call-with-retirement-intent-published-owner
       root
       (lambda (owner directory root-fd snapshot)
         (declare (ignore directory snapshot))
         (let* ((syscall (symbol-function operation))
                (run (symbol-function 'pg::physical-reseed-follow-retirement-archive-run))
                (captured nil) (armed nil) (seen 0) (failures 0)
                (workspace (pathname (getf (getf (getf record :configuration) :workspace) :directory)))
                (plan (getf (pg::physical-reseed-install-read-at root-fd "physical-reseed.install") :plan))
                (auxiliaries (mapcar (lambda (key) (pathname (getf (getf plan key) :directory)))
                                     '(:incoming :quarantine)))
                (baseline (retirement-archive-test-invariants root workspace auxiliaries))
                (original-row (pg::physical-reseed-install-old-observe
                               root-fd "physical-reseed.install"
                               (pg::physical-adoption-install-mount-id nil :directory-fd root-fd)))
                (original-bytes (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-reseed-follow-retirement-archive-run)
                        (lambda (witness) (setf captured witness) (funcall run witness))
                        (symbol-function operation)
                        (lambda (&rest arguments)
                          (when armed
                            (setf armed nil)
                            (let* ((intent (pg::retirement-archive-intent captured))
                                   (root (pg::physical-reseed-follow-retirement-archive-directory captured :root))
                                   (saved (pg::physical-reseed-follow-retirement-archive-directory captured :saved))
                                   (expected
                                     (ecase role
                                       (:file (list (sb-sys:fd-stream-fd (pg::retirement-archive-stream captured))))
                                       (:old-final (list (sb-sys:fd-stream-fd (pg::retirement-intent-old-stream intent))))
                                       (:temporary (list (pg::retirement-intent-temporary-fd intent)))
                                       (:root (list root))
                                       (:saved (list saved))
                                       (:link (list root "physical-reseed.install" saved "physical-reseed.install" 0))
                                       (:unlink (list root "physical-reseed.install" 0)))))
                              (unless (equal arguments expected) (error "wrong archive syscall injection site"))
                              (incf failures)
                              (when after-real
                                (unless (zerop (apply syscall arguments))
                                  (error "real archive namespace syscall failed before injection")))
                              (error 'sb-posix:syscall-error :name operation :errno sb-posix:eio)))
                          (apply syscall arguments)))
                  (let ((pg::*physical-reseed-follow-test-hook*
                          (lambda (point path)
                            (declare (ignore path))
                            (when (eq point event)
                              (incf seen)
                              (when (= seen occurrence) (setf armed t))))))
                    (ok (handler-case
                            (progn (pg::physical-reseed-follow-retirement-archive-install owner) nil)
                          (sb-posix:syscall-error (condition) (= sb-posix:eio (sb-posix:syscall-errno condition))))))
                  (ok (= occurrence seen))
                  (ok (= 1 failures))
                  (ng (pg::retirement-startup-busy-p owner))
                  (ok (pg::retirement-startup-uncertain-p owner))
                  (ok (handler-case
                          (progn (pg::physical-reseed-follow-retirement-archive-install owner) nil)
                        (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                  (let* ((actual (pg::physical-reseed-follow-snapshot-at root-fd))
                         (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                         (candidate (pg::physical-reseed-follow-retirement-next-record
                                     record (getf (getf record :retirement) :attempt-file))))
                    (ok (equal (if (zerop final-step) record candidate) (getf actual :record)))
                    (ecase temporary-state
                      (:absent (ng (probe-file temporary)))
                      (:complete
                       (ok (equalp (pg::physical-reseed-follow-image candidate) (lineage-test-bytes temporary))))))
                  (check-retirement-archive-test-location
                   root-fd record geometry original-row original-bytes)
                  (ok (reseed-activation-layout-facts-equal-p
                       baseline (retirement-archive-test-invariants root workspace auxiliaries))))
             (setf (symbol-function operation) syscall
                   (symbol-function 'pg::physical-reseed-follow-retirement-archive-run) run))
           (throw 'retirement-archive-syscall-checked :checked)))))))

(deftest reseed-retirement-install-archive-fences-each-real-effect-syscall-failure
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace root-fd initial check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let ((record (prepare-retirement-install-archive-test root root-fd initial :source)))
         (dolist (case '((sb-posix:fsync :before-retirement-archive-old-final-fsync 1 :source 0 :absent :old-final nil)
                         (sb-posix:fsync :before-retirement-archive-baseline-root-fsync 1 :source 0 :absent :root nil)
                         (sb-posix:fsync :before-retirement-archive-source-fsync 1 :source 0 :absent :file nil)
                         (sb-posix:fsync :before-retirement-archive-alias-fsync 1 :alias 0 :absent :file nil)
                         (sb-posix:fsync :before-retirement-archive-saved-fsync 1 :alias 0 :absent :saved nil)
                         (sb-posix:fsync :before-retirement-archive-inode-fsync 1 :saved 0 :absent :file nil)
                         (sb-posix:fsync :before-retirement-archive-saved-fsync 2 :saved 0 :absent :saved nil)
                         (sb-posix:fsync :before-retirement-archive-root-fsync 1 :saved 0 :absent :root nil)
                         (sb-posix:fsync :before-retirement-old-final-fsync 1 :saved 0 :absent :old-final nil)
                         (sb-posix:fsync :before-retirement-predecessor-directory-fsync 1 :saved 0 :absent :root nil)
                         (sb-posix:fsync :before-retirement-temporary-fsync 1 :saved 0 :complete :temporary nil)
                         (sb-posix:fsync :before-retirement-candidate-fsync 1 :saved 1 :absent :temporary nil)
                         (sb-posix:fsync :before-retirement-directory-fsync 1 :saved 1 :absent :root nil)
                         (pg::port-linkat :before-retirement-archive-link 1 :source 0 :absent :link nil)
                         (pg::port-linkat :before-retirement-archive-link 1 :alias 0 :absent :link t)
                         (pg::port-unlinkat :before-retirement-archive-unlink 1 :alias 0 :absent :unlink nil)
                         (pg::port-unlinkat :before-retirement-archive-unlink 1 :saved 0 :absent :unlink t)))
           (ok (eq :checked (check-retirement-archive-syscall-failure root record case))))
         (retirement-archive-test-reset root record)))))

(deftest reseed-retirement-install-archive-fences-postcommit-store-cleanup-failure
    (check-retirement-archive-postcommit-cleanup :error))

(defun check-retirement-archive-postcommit-cleanup (mode)
  (ok
   (eq :checked
       (catch 'retirement-archive-cleanup-checked
         (call-with-retirement-intent-test-owner
          (lambda (owner root root-fd snapshot)
            (declare (ignore snapshot))
            (let ((close-store (symbol-function 'pg::physical-store-close))
                  (run (symbol-function 'pg::physical-reseed-follow-retirement-archive-run))
                  (captured-store nil) (captured-archive nil) (armed nil) (failures 0))
              (unwind-protect
                   (progn
                     (setf (symbol-function 'pg::physical-reseed-follow-retirement-archive-run)
                           (lambda (witness)
                             (setf captured-archive witness)
                             (funcall run witness))
                           (symbol-function 'pg::physical-store-close)
                           (lambda (store)
                             (when armed
                               (setf captured-store store)
                               (incf failures)
                               (ecase mode
                                 (:error (error "injected archive store cleanup failure"))
                                 (:throw (throw 'retirement-archive-cleanup-cut :cut))
                                 (:interrupt
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (error "injected archive store cleanup failure")))
                                  (error "archive cleanup interrupt did not run"))))
                             (funcall close-store store)))
                     (let ((pg::*physical-reseed-follow-test-hook*
                             (lambda (event path)
                               (declare (ignore path))
                               (when (eq event :after-retirement-readback) (setf armed t)))))
                       (ok (eq (if (eq mode :throw) :cut :error)
                               (catch 'retirement-archive-cleanup-cut
                                 (handler-case
                                     (progn (pg::physical-reseed-follow-retirement-archive-install owner) nil)
                                   (error (condition)
                                     (when (search "injected archive store cleanup failure"
                                                   (princ-to-string condition))
                                       :error)))))))
                     (ok (= 1 failures))
                     (ng (pg::physical-store-closed-p captured-store))
                     (ng (pg::retirement-archive-active-p captured-archive))
                     (ng (open-stream-p (pg::retirement-archive-stream captured-archive)))
                     (ng (pg::retirement-startup-busy-p owner))
                     (ok (pg::retirement-startup-uncertain-p owner))
                     (let ((actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                       (ok (= 1 (getf (getf (getf actual :record) :retirement) :step)))
                       (ok (equal actual (getf (pg::retirement-startup-evidence owner) :snapshot))))
                     (ng (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))
                     (ok (handler-case
                             (progn (pg::physical-reseed-follow-retirement-archive-install owner) nil)
                           (error (condition) (search "busy or uncertain" (princ-to-string condition))))))
                (setf (symbol-function 'pg::physical-store-close) close-store
                      (symbol-function 'pg::physical-reseed-follow-retirement-archive-run) run)
                (when captured-store (funcall close-store captured-store)))
              (ok (pg::physical-store-closed-p captured-store))
              (throw 'retirement-archive-cleanup-checked :checked)))
          (lambda (root root-fd snapshot)
            (prepare-retirement-install-archive-test root root-fd snapshot :source)))))))

(deftest reseed-retirement-install-archive-fences-postcommit-cleanup-nonlocal-exits
    (dolist (mode '(:throw :interrupt))
      (check-retirement-archive-postcommit-cleanup mode)))

(deftest reseed-retirement-install-archive-final-handover-rejects-a-replaced-published-journal
    (ok
     (eq :checked
         (catch 'retirement-archive-final-replacement-checked
           (call-with-retirement-intent-test-owner
            (lambda (owner root root-fd snapshot)
              (declare (ignore snapshot))
              (let ((publish (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held))
                    (replaced nil) (replacement-inode nil) (published-snapshot nil))
                (unwind-protect
                     (progn
                       (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held)
                             (lambda (witness &optional authority)
                               (multiple-value-bind (record actual) (funcall publish witness authority)
                                 (setf published-snapshot (pg::physical-adoption-attempt-value-copy actual))
                                 (reseed-test-replace-follow-record root record)
                                 (setf replaced t
                                       replacement-inode
                                       (sb-posix:stat-ino
                                        (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.follow" root)))))
                                 (values record actual))))
                       (ok (handler-case
                               (progn (pg::physical-reseed-follow-retirement-archive-install owner) nil)
                             (error (condition)
                               (search "target follow final generation changed" (princ-to-string condition)))))
                       (ok replaced)
                       (ng (pg::retirement-startup-busy-p owner))
                       (ok (pg::retirement-startup-uncertain-p owner))
                       (let ((actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                         (ok (= replacement-inode (second (getf actual :identity))))
                         (ok (/= replacement-inode (second (getf published-snapshot :identity))))
                         (ok (equal (getf actual :record) (getf published-snapshot :record)))
                         (ok (equal published-snapshot (getf (pg::retirement-startup-evidence owner) :snapshot))))
                       (ok (handler-case
                               (progn (pg::physical-reseed-follow-retirement-archive-install owner) nil)
                             (error (condition) (search "busy or uncertain" (princ-to-string condition))))))
                  (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held) publish))
                (throw 'retirement-archive-final-replacement-checked :checked)))
            (lambda (root root-fd snapshot)
              (prepare-retirement-install-archive-test root root-fd snapshot :source)))))))

(defun check-retirement-archive-late-bridge-mutation (root record operation target)
  (retirement-archive-test-reset root record)
  (catch 'retirement-archive-mutation-checked
    (call-with-retirement-intent-published-owner
     root
     (lambda (owner directory root-fd snapshot)
       (declare (ignore directory snapshot))
       (let* ((syscall-name (ecase operation (:link 'pg::port-linkat) (:unlink 'pg::port-unlinkat)))
              (syscall (symbol-function syscall-name))
              (proof (symbol-function 'pg::physical-reseed-follow-retirement-prefix-proof-at))
              (run (symbol-function 'pg::physical-reseed-follow-retirement-archive-run))
              (captured nil) (armed nil) (injected nil) (after-row nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-follow-retirement-archive-run)
                      (lambda (witness) (setf captured witness) (funcall run witness))
                      (symbol-function syscall-name)
                      (lambda (&rest arguments)
                        (let ((result (apply syscall arguments)))
                          (unless (zerop result) (error "late-mutation fixture syscall failed"))
                          (setf armed t)
                          result))
                      (symbol-function 'pg::physical-reseed-follow-retirement-prefix-proof-at)
                      (lambda (&rest arguments)
                        (when (and armed (not injected))
                          (setf injected t armed nil)
                          (let* ((intent (pg::retirement-archive-intent captured))
                                 (mount (pg::retirement-startup-mount (pg::retirement-intent-owner intent)))
                                 (fd (if (eq target :file)
                                         (sb-sys:fd-stream-fd (pg::retirement-archive-stream captured))
                                         (pg::physical-reseed-follow-retirement-archive-directory
                                          captured (if (eq operation :link) :saved :root))))
                                 (before (pg::physical-reseed-follow-retirement-held-file-facts fd mount)))
                            (sleep 1.1)
                            (sb-posix:fchmod fd (if (eq target :file) #o600 #o700))
                            (setf after-row (pg::physical-reseed-follow-retirement-held-file-facts fd mount))
                            (ok (equal (butlast before) (butlast after-row)))
                            (ok (/= (car (last before)) (car (last after-row))))))
                        (apply proof arguments)))
                (ok (handler-case
                        (progn (pg::physical-reseed-follow-retirement-archive-install owner) nil)
                      (error (condition)
                        (search "retirement archive bridge changed its post-syscall facts"
                                (princ-to-string condition)))))
                (ok injected)
                (ng (pg::retirement-startup-busy-p owner))
                (ok (pg::retirement-startup-uncertain-p owner))
                (ok (equal record (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record)))
                (let* ((workspace (getf (getf record :configuration) :workspace))
                       (saved (pg::physical-backup-open-directory
                               (merge-pathnames "saved/" (getf workspace :directory))))
                       (fd (if (and (eq target :directory) (eq operation :unlink)) root-fd saved))
                       (name (if (eq target :file) "physical-reseed.install" ".")))
                  (unwind-protect
                       (ok (equal after-row
                                  (rest (pg::physical-reseed-install-old-observe
                                         fd name (pg::physical-adoption-install-mount-id
                                                  nil :directory-fd root-fd)))))
                    (sb-posix:close saved))))
           (setf (symbol-function syscall-name) syscall
                 (symbol-function 'pg::physical-reseed-follow-retirement-prefix-proof-at) proof
                 (symbol-function 'pg::physical-reseed-follow-retirement-archive-run) run))
         (throw 'retirement-archive-mutation-checked :checked))))))

(deftest reseed-retirement-install-archive-seals-metadata-before-post-syscall-proof-callbacks
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace root-fd initial check)
       (declare (ignore workspace check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let ((record (prepare-retirement-install-archive-test root root-fd initial :source)))
         (dolist (operation '(:link :unlink))
           (dolist (target '(:directory :file))
             (ok (eq :checked (check-retirement-archive-late-bridge-mutation root record operation target)))))
         (retirement-archive-test-reset root record)))))

(deftest reseed-retirement-initial-publication-adopts-one-durable-next-record
    (call-with-retirement-intent-test-owner
     (lambda (owner root root-fd snapshot)
       (let* ((record (getf snapshot :record))
              (reference (pg::physical-reseed-follow-retirement-next-reference root-fd record))
              (expected (pg::physical-reseed-follow-retirement-next-record record reference))
              (before (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))
              (pg::*physical-reseed-follow-write-limit*
                (ceiling (length (pg::physical-reseed-follow-image expected)) 2))
              (original-sync (symbol-function 'sb-posix:fsync))
              (original-close (symbol-function 'pg::physical-store-close))
              (cleanup-fenced nil) (readback nil)
              (pending nil) (events nil) (chunks 0)
              (published nil) (actual nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'sb-posix:fsync)
                      (lambda (fd)
                        (let ((stat (sb-posix:fstat fd)))
                          (push (list pending (sb-posix:stat-ino stat)
                                      (sb-posix:s-isdir (sb-posix:stat-mode stat)))
                                events))
                        (funcall original-sync fd))
                      (symbol-function 'pg::physical-store-close)
                      (lambda (store)
                        (when readback
                          (setf cleanup-fenced (pg::retirement-startup-uncertain-p owner)))
                        (funcall original-close store)))
                (let ((pg::*physical-reseed-follow-test-hook*
                        (lambda (point path)
                          (declare (ignore path))
                          (when (member point '(:before-retirement-old-final-fsync
                                                :before-retirement-predecessor-directory-fsync
                                                :before-retirement-temporary-fsync
                                                :before-retirement-candidate-fsync
                                                :before-retirement-directory-fsync))
                            (setf pending point))
                          (when (eq point :after-retirement-write-chunk) (incf chunks))
                          (when (eq point :after-retirement-readback) (setf readback t)))))
                  (multiple-value-setq (published actual)
                    (pg::physical-reseed-follow-retirement-publish-initial owner))))
           (setf (symbol-function 'sb-posix:fsync) original-sync
                 (symbol-function 'pg::physical-store-close) original-close))
         (setf events (nreverse events))
         (ok (equal (mapcar #'first events)
                    '(:before-retirement-old-final-fsync :before-retirement-predecessor-directory-fsync
                      :before-retirement-temporary-fsync :before-retirement-candidate-fsync
                      :before-retirement-directory-fsync)))
         (ok (equal (mapcar #'third events) '(nil t nil nil t)))
         (ok (= (third (getf snapshot :row)) (second (first events))))
         (ok (= (second (second events)) (second (fifth events))))
         (ok (= (second (third events)) (second (fourth events))))
         (ok (= 2 chunks))
         (ok cleanup-fenced)
         (ok (equal expected published))
         (ok (equal expected (getf actual :record)))
         (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))
         (ok (not (equalp before (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (not (pg::retirement-startup-busy-p owner)))
         (ok (not (pg::retirement-startup-uncertain-p owner)))
         (ok (pg::postmaster-pid-owned-p))
         (ok
          (pg::call-with-physical-reseed-follow-retirement-startup-observation
           owner
           (lambda (descriptors state check)
             (declare (ignore descriptors))
             (ok (equal actual (getf state :snapshot)))
             (funcall check))))))))

(deftest reseed-retirement-initial-publication-recovers-the-exact-complete-temporary
    (let ((expected nil) (temporary-inode nil) (writes 0))
      (call-with-retirement-intent-test-owner
       (lambda (owner root root-fd snapshot)
         (declare (ignore snapshot))
         (let ((pg::*physical-reseed-follow-test-hook*
                 (lambda (point path)
                   (declare (ignore path))
                   (when (member point '(:before-retirement-temporary-create :after-retirement-write-chunk))
                     (incf writes)
                     (error "complete retirement temporary must be reused")))))
           (multiple-value-bind (published actual)
               (pg::physical-reseed-follow-retirement-publish-initial owner)
             (ok (equal expected published))
             (ok (equal expected (getf actual :record)))
             (ok (= temporary-inode (third (getf actual :row))))
             (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))
             (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
             (ok (not (pg::retirement-startup-busy-p owner)))
             (ok (not (pg::retirement-startup-uncertain-p owner)))
             (ok (pg::postmaster-pid-owned-p))
             (ok (zerop writes)))))
       (lambda (root root-fd snapshot)
         (setf expected
               (pg::physical-reseed-follow-retirement-next-record
                (getf snapshot :record)
                (pg::physical-reseed-follow-retirement-next-reference root-fd (getf snapshot :record))))
         (reseed-follow-owner-test-write
          (merge-pathnames "physical-reseed.follow.tmp" root)
          (pg::physical-reseed-follow-image expected))
         (setf temporary-inode
               (third (nth-value 1 (pg::physical-reseed-follow-entry root-fd "physical-reseed.follow.tmp"))))))))

(deftest reseed-retirement-publication-and-temporary-recovery-cover-every-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-retirement-observation-fixture
       kind
       (lambda (offline root workspace fixture-fd initial check)
         (declare (ignore check))
         (close (pg::physical-adoption-owner-lock offline))
         (setf (pg::physical-adoption-owner-active-p offline) nil)
         (let* ((record (getf initial :record))
                (candidate
                  (pg::physical-reseed-follow-retirement-next-record
                   record (pg::physical-reseed-follow-retirement-next-reference fixture-fd record)))
                (baseline (list (retirement-intent-test-nonjournal-facts root)
                                (reseed-mapped-test-tree-facts workspace)))
                (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
           (dolist (recover '(nil t))
             (retirement-intent-test-reset root record)
             (when recover
               (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate)))
             (let ((temporary-inode
                     (when recover
                       (third (nth-value 1 (pg::physical-reseed-follow-entry
                                            fixture-fd "physical-reseed.follow.tmp")))))
                   (writes 0) (captured-owner nil))
               (call-with-retirement-intent-published-owner
                root
                (lambda (owner directory root-fd snapshot)
                  (declare (ignore directory snapshot))
                  (setf captured-owner owner)
                  (let ((pg::*physical-reseed-follow-test-hook*
                          (lambda (point path)
                            (declare (ignore path))
                            (when (member point '(:before-retirement-temporary-create
                                                  :after-retirement-write-chunk))
                              (incf writes)
                              (when recover (error "complete temporary must be reused"))))))
                    (multiple-value-bind (published actual)
                        (pg::physical-reseed-follow-retirement-publish-initial owner)
                      (ok (equal candidate published))
                      (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                      (ok (equal actual (getf (pg::retirement-startup-evidence owner) :snapshot)))
                      (when recover (ok (= temporary-inode (third (getf actual :row)))))
                      (ok (not (probe-file temporary)))
                      (ok (not (pg::retirement-startup-busy-p owner)))
                      (ok (not (pg::retirement-startup-uncertain-p owner)))
                      (ok (pg::postmaster-pid-owned-p))))))
               (ok (if recover (zerop writes) (plusp writes)))
               (ok (not (pg::retirement-startup-active-p captured-owner)))
               (ok (reseed-activation-layout-facts-equal-p
                    baseline (list (retirement-intent-test-nonjournal-facts root)
                                   (reseed-mapped-test-tree-facts workspace))))))
           (retirement-intent-test-reset root record))))))

(deftest reseed-retirement-initial-publication-preserves-invalid-temporaries-before-sync
    (call-with-retirement-intent-test-owner
     (lambda (owner root root-fd snapshot)
       (let* ((record (getf snapshot :record))
              (reference (pg::physical-reseed-follow-retirement-next-reference root-fd record))
              (candidate (pg::physical-reseed-follow-retirement-next-record record reference))
              (wrong-next (pg::physical-reseed-follow-retirement-next-record candidate reference))
              (path (merge-pathnames "physical-reseed.follow.tmp" root))
              (original-sync (symbol-function 'sb-posix:fsync))
              (syncs 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'sb-posix:fsync)
                      (lambda (fd) (incf syncs) (funcall original-sync fd)))
                (dolist (bytes (list (make-array 0 :element-type '(unsigned-byte 8))
                                     (pg::string->bytes "partial")
                                     (pg::physical-reseed-follow-image wrong-next)))
                  (unwind-protect
                       (progn
                         (reseed-follow-owner-test-write path bytes)
                         (ok (handler-case
                                 (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                               (error () t)))
                         (ok (zerop syncs))
                         (ok (equalp bytes (pg::read-bounded-regular-file path (max 1 (length bytes)) :minimum 0)))
                         (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))
                         (ok (not (pg::retirement-startup-busy-p owner)))
                         (ok (not (pg::retirement-startup-uncertain-p owner)))
                         (ok (pg::postmaster-pid-owned-p)))
                    (when (probe-file path) (delete-file path)))))
           (setf (symbol-function 'sb-posix:fsync) original-sync))))))

(defun check-retirement-intent-effect-phases (witness original-held)
  (let ((owner (pg::retirement-intent-owner witness)))
    (unwind-protect
         (progn
           (setf (pg::retirement-intent-effect-p witness) t
                 (pg::retirement-startup-uncertain-p owner) t)
           (dolist
               (case
                   (list
                    (list :complete
                          (lambda () (pg::call-with-physical-reseed-follow-retirement-intent-created
                                      witness (lambda (current) (declare (ignore current))))))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-write witness)))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-promote witness)))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-adopt witness)))
                    (list :writing
                          (lambda () (pg::physical-reseed-follow-retirement-intent-sync
                                      witness :old-final :negative-before-sync :negative-after-sync)))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-sync
                                      witness :temporary :negative-before-sync :negative-after-sync)))
                    (list :predecessor
                          (lambda () (pg::physical-reseed-follow-retirement-intent-sync
                                      witness :candidate :negative-before-sync :negative-after-sync)))))
             (setf (pg::retirement-intent-phase witness) (first case))
             (ok (handler-case (progn (funcall (second case)) nil)
                   (error (condition) (search "requires phase" (princ-to-string condition))))))
           (setf (pg::retirement-intent-phase witness) :predecessor)
           (ok (handler-case
                   (progn (funcall original-held witness) nil)
                 (error (condition) (search "already started" (princ-to-string condition)))))
           (setf (pg::retirement-intent-temporary-stream witness) :already-held)
           (unwind-protect
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retirement-intent-created
                           witness (lambda (current) (declare (ignore current))))
                          nil)
                      (error (condition) (search "already held" (princ-to-string condition)))))
             (setf (pg::retirement-intent-temporary-stream witness) nil))
           (let* ((hit nil)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (point path)
                      (declare (ignore path))
                      (when (eq point :negative-before-sync)
                        (setf hit t
                              (pg::retirement-intent-effect-p witness) nil
                              (pg::retirement-startup-uncertain-p owner) nil)))))
             (ok (handler-case
                     (progn
                       (pg::physical-reseed-follow-retirement-intent-sync
                        witness :old-final :negative-before-sync :negative-after-sync)
                       nil)
                   (error (condition)
                     (search "requires an uncertain operation" (princ-to-string condition)))))
             (ok hit)))
      (setf (pg::retirement-intent-phase witness) :predecessor
            (pg::retirement-intent-effect-p witness) nil
            (pg::retirement-startup-uncertain-p owner) nil))))

(deftest reseed-retirement-publication-claims-its-witness-before-proof-callbacks
    (let ((witness (pg::make-physical-reseed-follow-retirement-intent-witness
                    :active-p t :thread sb-thread:*current-thread* :phase :predecessor))
          (scope (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope))
          (proof (symbol-function 'pg::physical-reseed-follow-retirement-intent-check))
          (proofs 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope)
                   (lambda (current) (declare (ignore current)) t)
                   (symbol-function 'pg::physical-reseed-follow-retirement-intent-check)
                   (lambda (current)
                     (declare (ignore current))
                     (incf proofs)
                     (throw 'retirement-publication-claim-cut :cut)))
             (ok (eq :cut
                     (catch 'retirement-publication-claim-cut
                       (pg::physical-reseed-follow-retirement-intent-publish-held witness))))
             (ok (eq :refused
                     (catch 'retirement-publication-claim-cut
                       (handler-case
                           (pg::physical-reseed-follow-retirement-intent-publish-held witness)
                         (error (condition)
                           (when (search "publication already claimed" (princ-to-string condition))
                             :refused))))))
             (ok (= 1 proofs)))
        (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope) scope
              (symbol-function 'pg::physical-reseed-follow-retirement-intent-check) proof))))

(deftest reseed-retirement-progress-publication-refuses-untyped-or-unmoved-authority
    (let ((scope (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope))
          (check (symbol-function 'pg::physical-reseed-follow-retirement-intent-check))
          (sync (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync))
          (effects 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope)
                   (lambda (witness) (declare (ignore witness)) t)
                   (symbol-function 'pg::physical-reseed-follow-retirement-intent-check)
                   (lambda (witness) (declare (ignore witness)) t)
                   (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (incf effects)
                     (error "unexpected progress publication effect")))
             (dolist (case '((1 nil nil "strict Active2 predecessor")
                             (0 (:phase :retiring :step 0) nil "strict Active2 predecessor")
                             (1 (:phase :retiring :step 0) t "live archive authority")))
               (destructuring-bind (prefix retirement authority message) case
                 (let* ((owner (pg::make-physical-reseed-follow-retirement-startup-owner))
                        (witness
                          (pg::make-physical-reseed-follow-retirement-intent-witness
                           :owner owner :thread sb-thread:*current-thread* :active-p t :phase :predecessor
                           :proof (list :prefix prefix)
                           :snapshot (list :record (list :retirement retirement)))))
                   (ok (handler-case
                           (progn
                             (pg::physical-reseed-follow-retirement-intent-publish-held witness authority)
                             nil)
                         (error (condition) (search message (princ-to-string condition)))))
                   (ok (pg::retirement-intent-publication-claimed-p witness))
                   (ng (pg::retirement-intent-effect-p witness))
                   (ng (pg::retirement-startup-uncertain-p owner)))))
             (let* ((publisher (pg::make-physical-reseed-follow-retirement-intent-witness))
                    (other (pg::make-physical-reseed-follow-retirement-intent-witness))
                    (authority (pg::make-physical-reseed-follow-retirement-archive-witness :intent other)))
               (ok (handler-case
                       (progn
                         (pg::physical-reseed-follow-retirement-authorize-progress authority publisher)
                         nil)
                     (error (condition) (search "belongs to another publisher" (princ-to-string condition))))))
             (ok (zerop effects)))
        (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope) scope
              (symbol-function 'pg::physical-reseed-follow-retirement-intent-check) check
              (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync) sync))))

(deftest reseed-retirement-archive-claims-its-witness-before-proof-callbacks
    (let ((witness (pg::make-physical-reseed-follow-retirement-archive-witness
                    :active-p t :thread sb-thread:*current-thread* :phase :ready))
          (check (symbol-function 'pg::physical-reseed-follow-retirement-archive-check))
          (checks 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-archive-check)
                   (lambda (current)
                     (declare (ignore current))
                     (incf checks)
                     (throw 'retirement-archive-claim-cut :cut)))
             (ok (eq :cut (catch 'retirement-archive-claim-cut
                            (pg::physical-reseed-follow-retirement-archive-run witness))))
             (ok (handler-case
                     (progn (pg::physical-reseed-follow-retirement-archive-run witness) nil)
                   (error (condition) (search "already claimed" (princ-to-string condition)))))
             (ok (= 1 checks)))
        (setf (symbol-function 'pg::physical-reseed-follow-retirement-archive-check) check))))

(deftest reseed-retirement-publication-rejects-inconsistent-phase-and-temporary-ownership
    (let ((scope (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope))
          (check (symbol-function 'pg::physical-reseed-follow-retirement-intent-check))
          (sync (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync))
          (effects 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope)
                   (lambda (witness) (declare (ignore witness)) t)
                   (symbol-function 'pg::physical-reseed-follow-retirement-intent-check)
                   (lambda (witness) (declare (ignore witness)) t)
                   (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (incf effects)
                     (error "unexpected retirement publication effect")))
             (dolist (phase '(:predecessor :complete))
               (let* ((owner (pg::make-physical-reseed-follow-retirement-startup-owner))
                      (witness
                        (pg::make-physical-reseed-follow-retirement-intent-witness
                         :owner owner :phase phase :active-p t :thread sb-thread:*current-thread*
                         :temporary-stream (when (eq phase :predecessor) :held)
                         :temporary-fd (when (eq phase :predecessor) 1))))
                 (ok (handler-case
                         (progn (pg::physical-reseed-follow-retirement-intent-publish-held witness) nil)
                       (error (condition) (search "changed phase" (princ-to-string condition)))))
                 (ok (zerop effects))
                 (ok (not (pg::retirement-intent-effect-p witness)))
                 (ok (not (pg::retirement-startup-uncertain-p owner))))))
        (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-check-scope) scope
              (symbol-function 'pg::physical-reseed-follow-retirement-intent-check) check
              (symbol-function 'pg::physical-reseed-follow-retirement-intent-sync) sync))))

(deftest reseed-retirement-effect-leaves-require-uncertainty-before-filesystem-effects
    (let ((original-held (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held))
          (captured nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held)
                   (lambda (witness)
                     (setf captured witness)
                     (let ((original-sync (symbol-function 'sb-posix:fsync))
                           (original-open (symbol-function 'pg::physical-image-openat))
                           (original-write (symbol-function 'pg::physical-adoption-install-write-bytes))
                           (effects 0))
                       (unwind-protect
                            (progn
                              (setf (symbol-function 'sb-posix:fsync)
                                    (lambda (&rest arguments)
                                      (declare (ignore arguments))
                                      (incf effects)
                                      (error "unexpected sync before uncertainty"))
                                    (symbol-function 'pg::physical-adoption-install-write-bytes)
                                    (lambda (&rest arguments)
                                      (declare (ignore arguments))
                                      (incf effects)
                                      (error "unexpected write before uncertainty"))
                                    (symbol-function 'pg::physical-image-openat)
                                    (lambda (&rest arguments)
                                      (when (logtest sb-posix:o-creat (third arguments))
                                        (incf effects)
                                        (error "unexpected create before uncertainty"))
                                      (apply original-open arguments)))
                              (dolist
                                  (effect
                                   (list
                                    (lambda () (pg::physical-reseed-follow-retirement-intent-sync
                                                witness :predecessor-root :negative-before-sync :negative-after-sync))
                                    (lambda () (pg::call-with-physical-reseed-follow-retirement-intent-created
                                                witness (lambda (current) (declare (ignore current)))))
                                    (lambda () (pg::physical-reseed-follow-retirement-intent-write witness))
                                    (lambda () (pg::physical-reseed-follow-retirement-intent-promote witness))
                                    (lambda () (pg::physical-reseed-follow-retirement-intent-adopt witness))))
                                (unwind-protect
                                     (ok (handler-case (progn (funcall effect) nil)
                                           (error (condition)
                                             (search "requires an uncertain operation" (princ-to-string condition)))))
                                  (setf (pg::retirement-intent-active-p witness) t))
                                (ok (zerop effects))
                                (ok (not (pg::retirement-intent-effect-p witness)))
                                (ok (not (pg::retirement-startup-uncertain-p
                                          (pg::retirement-intent-owner witness)))))
                              (check-retirement-intent-effect-phases witness original-held)
                              (ok (zerop effects)))
                         (setf (symbol-function 'sb-posix:fsync) original-sync
                               (symbol-function 'pg::physical-image-openat) original-open
                               (symbol-function 'pg::physical-adoption-install-write-bytes) original-write)))
                     :checked))
             (call-with-retirement-intent-test-owner
              (lambda (owner root root-fd snapshot)
                (declare (ignore root))
                (ok (eq :checked (pg::physical-reseed-follow-retirement-publish-initial owner)))
                (ok (equal snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))
                (ok (not (pg::retirement-startup-busy-p owner)))
                (ok (not (pg::retirement-startup-uncertain-p owner))))))
        (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-publish-held) original-held))
      (ok (not (pg::retirement-intent-active-p captured)))
      (ok (handler-case
              (progn (pg::physical-reseed-follow-retirement-intent-check-scope captured) nil)
            (error (condition) (search "ended or changed thread" (princ-to-string condition)))))))

(deftest reseed-retirement-initial-publication-fences-every-publication-cut
    (dolist (point '(:after-retirement-old-final-fsync
                     :after-retirement-predecessor-directory-fsync
                     :after-retirement-temporary-create
                     :after-retirement-write-chunk
                     :after-retirement-temporary-fsync
                     :after-retirement-rename
                     :after-retirement-candidate-fsync
                     :after-retirement-directory-fsync
                     :after-retirement-readback))
      (ok
       (eq :checked
           (catch 'retirement-publication-cut-checked
             (call-with-retirement-intent-test-owner
              (lambda (owner root root-fd snapshot)
                (let* ((record (getf snapshot :record))
                       (expected (pg::physical-reseed-follow-retirement-next-record
                                  record (pg::physical-reseed-follow-retirement-next-reference root-fd record)))
                       (bytes (pg::physical-reseed-follow-image expected))
                       (pg::*physical-reseed-follow-write-limit* (ceiling (length bytes) 2))
                       (injected nil)
                       (pg::*physical-reseed-follow-test-hook*
                         (lambda (event path)
                           (declare (ignore path))
                           (when (eq event point)
                             (setf injected t)
                             (error "injected retirement publication cut")))))
                  (ok
                   (handler-case
                       (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                     (error (condition)
                       (search "injected retirement publication cut" (princ-to-string condition)))))
                  (ok injected)
                  (ok (not (pg::retirement-startup-busy-p owner)))
                  (ok (pg::retirement-startup-uncertain-p owner))
                  (ok
                   (handler-case
                       (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                     (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                  (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                  (ok (pg::postmaster-pid-owned-p))
                  (let* ((actual (pg::physical-reseed-follow-snapshot-at root-fd))
                         (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                         (renamed (member point '(:after-retirement-rename
                                                  :after-retirement-candidate-fsync
                                                  :after-retirement-directory-fsync
                                                  :after-retirement-readback))))
                    (if renamed
                        (progn
                          (ok (equal expected (getf actual :record)))
                          (ok (not (probe-file temporary))))
                        (progn
                          (ok (equal snapshot actual))
                          (cond
                            ((member point '(:after-retirement-old-final-fsync
                                             :after-retirement-predecessor-directory-fsync))
                             (ok (not (probe-file temporary))))
                            ((eq point :after-retirement-temporary-create)
                             (ok (zerop (length (pg::read-bounded-regular-file temporary 1 :minimum 0)))))
                            ((eq point :after-retirement-write-chunk)
                             (ok (equalp (subseq bytes 0 (ceiling (length bytes) 2))
                                         (pg::recovery-read-bytes temporary))))
                            (t (ok (equalp bytes (pg::recovery-read-bytes temporary))))))))
                  (throw 'retirement-publication-cut-checked :checked)))))))))

(deftest reseed-retirement-publication-fences-throw-and-self-interrupt
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace fixture-fd initial check)
       (declare (ignore fixture-fd check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let ((record (getf initial :record))
             (baseline (list (retirement-intent-test-nonjournal-facts root)
                             (reseed-mapped-test-tree-facts workspace))))
         (dolist (mode '(:throw :interrupt))
           (dolist (point '(:after-retirement-old-final-fsync
                            :after-retirement-predecessor-directory-fsync
                            :after-retirement-temporary-create
                            :after-retirement-write-chunk
                            :after-retirement-temporary-fsync
                            :after-retirement-rename
                            :after-retirement-candidate-fsync
                            :after-retirement-directory-fsync
                            :after-retirement-readback))
             (retirement-intent-test-reset root record)
             (let ((captured-owner nil))
               (ok
                (eq :checked
                    (catch 'retirement-nonlocal-checked
                      (call-with-retirement-intent-published-owner
                       root
                       (lambda (owner directory root-fd snapshot)
                         (declare (ignore directory))
                         (setf captured-owner owner)
                         (let* ((candidate
                                  (pg::physical-reseed-follow-retirement-next-record
                                   record
                                   (pg::physical-reseed-follow-retirement-next-reference root-fd record)))
                                (bytes (pg::physical-reseed-follow-image candidate))
                                (pg::*physical-reseed-follow-write-limit* (ceiling (length bytes) 2))
                                (injected nil)
                                (pg::*physical-reseed-follow-test-hook*
                                  (lambda (event path)
                                    (declare (ignore path))
                                    (when (eq event point)
                                      (setf injected t)
                                      (ecase mode
                                        (:throw (throw 'retirement-nonlocal-cut :cut))
                                        (:interrupt
                                         (sb-thread:interrupt-thread
                                          sb-thread:*current-thread*
                                          (lambda () (throw 'retirement-nonlocal-cut :cut)))))))))
                           (ok (eq :cut
                                   (catch 'retirement-nonlocal-cut
                                     (pg::physical-reseed-follow-retirement-publish-initial owner)
                                     :returned)))
                           (ok injected)
                           (ok (not (pg::retirement-startup-busy-p owner)))
                           (ok (pg::retirement-startup-uncertain-p owner))
                           (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                           (ok (handler-case
                                   (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                                 (error (condition)
                                   (search "busy or uncertain" (princ-to-string condition)))))
                           (let ((actual (pg::physical-reseed-follow-snapshot-at root-fd))
                                 (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
                             (if (member point '(:after-retirement-rename :after-retirement-candidate-fsync
                                                 :after-retirement-directory-fsync :after-retirement-readback))
                                 (progn
                                   (ok (equal candidate (getf actual :record)))
                                   (ok (not (probe-file temporary))))
                                 (progn
                                   (ok (equal snapshot actual))
                                   (cond
                                     ((member point '(:after-retirement-old-final-fsync
                                                      :after-retirement-predecessor-directory-fsync))
                                      (ok (not (probe-file temporary))))
                                     ((eq point :after-retirement-temporary-create)
                                      (ok (zerop (length (pg::read-bounded-regular-file temporary 1 :minimum 0)))))
                                     ((eq point :after-retirement-write-chunk)
                                      (ok (equalp (subseq bytes 0 (ceiling (length bytes) 2))
                                                  (pg::recovery-read-bytes temporary))))
                                     (t (ok (equalp bytes (pg::recovery-read-bytes temporary))))))))
                           (ok (pg::postmaster-pid-owned-p))
                           (ok (reseed-activation-layout-facts-equal-p
                                baseline
                                (list (retirement-intent-test-nonjournal-facts root)
                                      (reseed-mapped-test-tree-facts workspace))))
                           (throw 'retirement-nonlocal-checked :checked)))))))
               (ok (not (pg::retirement-startup-active-p captured-owner))))))
         (retirement-intent-test-reset root record)))))

(deftest reseed-retirement-initial-publication-fences-each-fsync-failure
    (loop for fail-at from 1 to 5
          do
             (ok
              (eq :checked
                  (catch 'retirement-sync-failure-checked
                    (call-with-retirement-intent-test-owner
                     (lambda (owner root root-fd snapshot)
                       (let* ((original (symbol-function 'sb-posix:fsync))
                              (calls 0) (injected nil) (failed-fd nil) (failed-inode nil))
                         (unwind-protect
                              (progn
                                (setf (symbol-function 'sb-posix:fsync)
                                      (lambda (fd)
                                        (incf calls)
                                        (when (= calls fail-at)
                                          (setf injected t failed-fd fd
                                                failed-inode (sb-posix:stat-ino (sb-posix:fstat fd)))
                                          (error "injected retirement fsync failure"))
                                        (funcall original fd)))
                                (ok
                                 (handler-case
                                     (progn
                                       (pg::physical-reseed-follow-retirement-publish-initial owner)
                                       nil)
                                   (error (condition)
                                     (search "injected retirement fsync failure" (princ-to-string condition))))))
                           (setf (symbol-function 'sb-posix:fsync) original))
                         (ok injected)
                         (ok (= calls fail-at))
                         (ok (handler-case (progn (sb-posix:fstat failed-fd) nil)
                               (sb-posix:syscall-error () t)))
                         (ok (not (pg::retirement-startup-busy-p owner)))
                         (ok (pg::retirement-startup-uncertain-p owner))
                         (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                         (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                                (path (case fail-at
                                        ((1 4) (merge-pathnames "physical-reseed.follow" root))
                                        ((2 5) root)
                                        (3 temporary)))
                                (actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                           (ok (= failed-inode (sb-posix:stat-ino (sb-posix:stat (namestring path)))))
                           (if (< fail-at 4)
                               (progn
                                 (ok (equal snapshot actual))
                                 (if (= fail-at 3)
                                     (ok (equal
                                          (pg::physical-reseed-follow-retirement-next-record
                                           (getf snapshot :record)
                                           (pg::physical-reseed-follow-retirement-next-reference
                                            root-fd (getf snapshot :record)))
                                          (pg::physical-reseed-follow-read-at root-fd "physical-reseed.follow.tmp")))
                                     (ok (not (probe-file temporary)))))
                               (progn
                                 (ok (eq :retiring (getf (getf (getf actual :record) :retirement) :phase)))
                                 (ok (zerop (getf (getf (getf actual :record) :retirement) :step)))
                                 (ok (not (probe-file temporary))))))
                         (ok (pg::postmaster-pid-owned-p))
                         (throw 'retirement-sync-failure-checked :checked)))))))))

(deftest reseed-retirement-publication-keeps-the-owner-fenced-after-postcommit-cleanup-failure
    (let ((captured-owner nil))
      (ok
       (handler-case
           (progn
             (call-with-retirement-intent-test-owner
              (lambda (owner root root-fd snapshot)
                (declare (ignore snapshot))
                (setf captured-owner owner)
                (let ((original (symbol-function 'pg::physical-store-close))
                      (armed nil) (captured-store nil) (failures 0))
                  (unwind-protect
                       (progn
                         (setf (symbol-function 'pg::physical-store-close)
                               (lambda (store)
                                 (when armed
                                   (setf captured-store store)
                                   (incf failures)
                                   (error "injected retirement store cleanup failure"))
                                 (funcall original store)))
                         (let ((pg::*physical-reseed-follow-test-hook*
                                 (lambda (point path)
                                   (declare (ignore path))
                                   (when (eq point :after-retirement-readback) (setf armed t)))))
                           (ok (handler-case
                                   (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                                 (error (condition)
                                   (search "injected retirement store cleanup failure"
                                           (princ-to-string condition))))))
                         (ok (= 1 failures))
                         (ok (not (pg::physical-store-closed-p captured-store)))
                         (ok (not (pg::retirement-startup-busy-p owner)))
                         (ok (pg::retirement-startup-uncertain-p owner))
                         (let ((actual (pg::physical-reseed-follow-snapshot-at root-fd)))
                           (ok (eq :retiring (getf (getf (getf actual :record) :retirement) :phase)))
                           (ok (zerop (getf (getf (getf actual :record) :retirement) :step)))
                           (ok (equal actual (getf (pg::retirement-startup-evidence owner) :snapshot))))
                         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                         (let ((entered nil))
                           (ok (handler-case
                                   (progn
                                     (pg::call-with-physical-reseed-follow-retirement-operation
                                      owner (lambda () (setf entered t)))
                                     nil)
                                 (error (condition)
                                   (search "busy or uncertain" (princ-to-string condition)))))
                           (ok (not entered))))
                    (setf (symbol-function 'pg::physical-store-close) original)
                    (when captured-store (funcall original captured-store)))
                  (ok (pg::physical-store-closed-p captured-store))
                  :ignored)))
             nil)
         (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
      (ok (not (pg::retirement-startup-active-p captured-owner)))))

(deftest reseed-retirement-publication-refuses-same-byte-final-replacement-after-readback
    (ok
     (eq :checked
         (catch 'retirement-late-replacement-checked
           (call-with-retirement-intent-test-owner
            (lambda (owner root root-fd snapshot)
              (let* ((candidate
                       (pg::physical-reseed-follow-retirement-next-record
                        (getf snapshot :record)
                        (pg::physical-reseed-follow-retirement-next-reference root-fd (getf snapshot :record))))
                     (original (symbol-function 'pg::physical-reseed-follow-retirement-intent-adopt))
                     (completed-adoptions 0) (held-fd nil) (injected nil) (old-inode nil) (new-inode nil)
                     (baseline (retirement-intent-test-nonjournal-facts root)))
                (unwind-protect
                     (progn
                       (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-adopt)
                             (lambda (witness)
                               (setf held-fd (pg::retirement-intent-temporary-fd witness))
                               (multiple-value-prog1 (funcall original witness)
                                 (incf completed-adoptions))))
                       (let ((pg::*physical-reseed-follow-test-hook*
                               (lambda (point path)
                                 (declare (ignore path))
                                 (when (eq point :after-retirement-readback)
                                   (setf old-inode
                                         (third (getf (pg::physical-reseed-follow-snapshot-at root-fd) :row)))
                                   (reseed-test-replace-follow-record root candidate)
                                   (setf new-inode
                                         (third (getf (pg::physical-reseed-follow-snapshot-at root-fd) :row))
                                         injected t)))))
                         (ok
                          (handler-case
                              (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                            (error (condition)
                              (format t "~%RETIREMENT-LATE-REPLACEMENT-REFUSAL ~a~%" condition)
                              (search "target follow journal is not a bounded private single-link regular file"
                                      (princ-to-string condition)))))))
                  (setf (symbol-function 'pg::physical-reseed-follow-retirement-intent-adopt) original))
                (ok injected)
                (ok (and held-fd
                         (handler-case (progn (sb-posix:fstat held-fd) nil)
                           (sb-posix:syscall-error () t))))
                (ok (and old-inode new-inode (/= old-inode new-inode)))
                (ok (zerop completed-adoptions))
                (ok (equal candidate (getf (pg::physical-reseed-follow-snapshot-at root-fd) :record)))
                (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                (ok (equal snapshot (getf (pg::retirement-startup-evidence owner) :snapshot)))
                (ok (not (pg::retirement-startup-busy-p owner)))
                (ok (pg::retirement-startup-uncertain-p owner))
                (ok (handler-case
                        (progn (pg::physical-reseed-follow-retirement-publish-initial owner) nil)
                      (error (condition) (search "busy or uncertain" (princ-to-string condition)))))
                (ok (reseed-activation-layout-facts-equal-p
                     baseline (retirement-intent-test-nonjournal-facts root)))
                (ok (pg::postmaster-pid-owned-p))
                (throw 'retirement-late-replacement-checked :checked))))))))

(deftest reseed-retirement-publication-survives-process-crashes-under-fresh-startup
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (offline root workspace fixture-fd initial check)
       (declare (ignore fixture-fd check))
       (close (pg::physical-adoption-owner-lock offline))
       (setf (pg::physical-adoption-owner-active-p offline) nil)
       (let* ((record (getf initial :record))
              (baseline (list (retirement-intent-test-nonjournal-facts root)
                              (reseed-mapped-test-tree-facts workspace)))
              (candidate nil))
         (call-with-retirement-intent-published-owner
          root
          (lambda (owner directory root-fd snapshot)
            (declare (ignore owner directory))
            (setf candidate
                  (pg::physical-reseed-follow-retirement-next-record
                   (getf snapshot :record)
                   (pg::physical-reseed-follow-retirement-next-reference root-fd (getf snapshot :record))))))
         (with-physical-store-directory (driver)
           (let ((cache (merge-pathnames "cache/" driver)))
             (dolist (point '(:after-retirement-old-final-fsync
                              :after-retirement-predecessor-directory-fsync
                              :after-retirement-temporary-create
                              :after-retirement-write-chunk
                              :after-retirement-temporary-fsync
                              :after-retirement-rename
                              :after-retirement-candidate-fsync
                              :after-retirement-directory-fsync
                              :after-retirement-readback))
               (retirement-intent-test-reset root record)
               (ok (reseed-activation-layout-facts-equal-p
                    baseline (list (retirement-intent-test-nonjournal-facts root)
                                   (reseed-mapped-test-tree-facts workspace))))
               (let* ((marker (merge-pathnames (format nil "~(~a~).marker" point) driver))
                      (pid (run-retirement-intent-test-crash-child root point cache marker))
                      (stale (pg::physical-reseed-read-pid root))
                      (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                      (partial (member point '(:after-retirement-temporary-create :after-retirement-write-chunk)))
                      (renamed (member point '(:after-retirement-rename :after-retirement-candidate-fsync
                                               :after-retirement-directory-fsync :after-retirement-readback))))
                 (ok (= pid (getf stale :pid)))
                 (ok (reseed-activation-layout-facts-equal-p
                      baseline (list (retirement-intent-test-nonjournal-facts root)
                                     (reseed-mapped-test-tree-facts workspace))))
                 (if partial
                     (let ((entered nil)
                           (bytes (lineage-test-bytes temporary)))
                       (ok (handler-case
                               (progn
                                 (call-with-retirement-intent-published-owner
                                  root (lambda (&rest arguments)
                                         (declare (ignore arguments))
                                         (setf entered t)))
                                 nil)
                             (error (condition)
                               (search
                                (if (eq point :after-retirement-temporary-create)
                                    "target follow journal is not a bounded private single-link regular file"
                                    "physical backup lease checksum mismatch")
                                (princ-to-string condition)))))
                       (ok (not entered))
                       (let ((after (pg::physical-reseed-read-pid root)))
                         (ok (= (getf stale :pid) (getf after :pid)))
                         (ok (equal (getf stale :identity) (getf after :identity)))
                         (ok (equalp (getf stale :bytes) (getf after :bytes))))
                       (ok (equalp bytes (lineage-test-bytes temporary)))
                       (ok (equalp (pg::physical-reseed-follow-image record)
                                   (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))))
                     (call-with-retirement-intent-published-owner
                      root
                      (lambda (owner directory root-fd snapshot)
                        (declare (ignore directory))
                        (if renamed
                            (ok (equal candidate (getf snapshot :record)))
                            (multiple-value-bind (published actual)
                                (pg::physical-reseed-follow-retirement-publish-initial owner)
                              (ok (equal candidate published))
                              (ok (equal actual (pg::physical-reseed-follow-snapshot-at root-fd)))))
                        (ok (not (probe-file temporary)))
                        (ok (pg::postmaster-pid-owned-p)))))
                 (ok (reseed-activation-layout-facts-equal-p
                      baseline (list (retirement-intent-test-nonjournal-facts root)
                                     (reseed-mapped-test-tree-facts workspace))))))
             (retirement-intent-test-reset root record)))))))

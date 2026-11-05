(in-package #:pg-physical-tests)

(defun call-with-reseed-retirement-observation-fixture (kind function)
  (let ((tag (gensym "RETIREMENT-OBSERVATION")))
    (catch tag
      (call-with-reseed-activating-fixture
       kind
       (lambda (owner root workspace record)
         (declare (ignore record))
         (loop repeat 3 do (pg::physical-reseed-follow-advance-activation owner))
         (let ((fd (pg::physical-backup-open-directory root)))
           (unwind-protect
                (flet ((check () (pg::physical-directory-check-owner-common owner)))
                  (throw tag (multiple-value-list
                              (funcall function owner root workspace fd
                                       (pg::physical-reseed-follow-snapshot-at fd) #'check))))
             (sb-posix:close fd))))))))

(deftest reseed-retirement-observation-acquires-and-revokes-every-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-retirement-observation-fixture
       kind
       (lambda (owner root workspace root-fd snapshot check)
         (let* ((plan (pg::physical-reseed-install-owner-plan owner))
                (directories (list root workspace
                                   (pathname (getf (getf plan :incoming) :directory))
                                   (pathname (getf (getf plan :quarantine) :directory))))
                (before (mapcar #'reseed-mapped-test-tree-facts directories))
                (saved-check nil) (borrowed nil)
                (result
                  (multiple-value-list
                   (funcall (symbol-function 'pg::call-with-physical-reseed-follow-retirement-observation)
                            root-fd snapshot check
                            (lambda (descriptors state verify)
                              (setf saved-check verify borrowed (copy-tree descriptors))
                              (ok (= 10 (length descriptors)))
                              (ok (zerop (getf state :prefix)))
                              (multiple-value-bind (base control binding model images row)
                                  (pg::physical-reseed-follow-configuration-base-proof
                                   root-fd (cdr (assoc :saved descriptors))
                                   (loop for role in '(:base :global :wal)
                                         collect (cons role (cdr (assoc role descriptors))))
                                   plan (getf snapshot :record)
                                   (getf (getf (getf snapshot :record) :configuration) :step) check)
                                (declare (ignore base control images row))
                                (ok (null (pg::physical-store-retained-root model)))
                                (ok (null (nth 10 (getf binding :after))))
                                (setf (pg::physical-store-retained-root model) 1)
                                (ok (handler-case
                                        (progn
                                          (pg::physical-reseed-follow-configuration-images
                                           (getf snapshot :record) binding model
                                           (getf (getf (getf snapshot :record) :configuration) :time))
                                          nil)
                                      (error (condition)
                                        (search "proved empty model" (princ-to-string condition))))))
                              (dolist (role '(:workspace :incoming :quarantine))
                                (let ((fd (sb-posix:open
                                           (namestring (merge-pathnames ".pg_lisp.lock"
                                                                        (pg::physical-image-fd-path (cdr (assoc role descriptors)))))
                                           sb-posix:o-rdonly)))
                                  (unwind-protect
                                       (ok (not (zerop (pg::physical-store-flock fd 6))))
                                    (sb-posix:close fd))))
                              (ok (funcall verify))
                              (let ((thread (sb-thread:make-thread
                                             (lambda ()
                                               (handler-case (progn (funcall verify) nil)
                                                 (error (condition) (princ-to-string condition)))))))
                                (ok (search "thread" (sb-thread:join-thread thread))))
                              (let ((fd (sb-posix:open
                                         (namestring
                                          (merge-pathnames "receive.lock"
                                                           (pg::physical-image-fd-path (cdr (assoc :receive descriptors)))))
                                         sb-posix:o-rdonly)))
                                (unwind-protect
                                     (ok (not (zerop (pg::physical-store-flock fd 6))))
                                  (sb-posix:close fd)))
                              (values :first :second))))))
           (ok (equal '(:first :second) result))
           (ok (handler-case (progn (funcall saved-check) nil)
                 (error (condition) (search "scope" (princ-to-string condition)))))
           (dolist (entry borrowed)
             (if (eq :root (car entry))
                 (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat (cdr entry)))))
                 (ok (handler-case (progn (sb-posix:fstat (cdr entry)) nil)
                       (sb-posix:syscall-error () t)))))
           (ok (reseed-activation-layout-facts-equal-p
                before (mapcar #'reseed-mapped-test-tree-facts directories))))))))

(deftest reseed-retirement-observation-cleans-up-on-error-and-nonlocal-exit
    (dolist (kind '(:error :throw))
      (call-with-reseed-retirement-observation-fixture
       :same-timeline
       (lambda (owner root workspace root-fd snapshot check)
         (declare (ignore owner root workspace))
         (let ((saved-check nil) (called nil))
           (ok (eq :cut
                   (catch 'retirement-cut
                     (handler-case
                         (pg::call-with-physical-reseed-follow-retirement-observation
                          root-fd snapshot check
                          (lambda (descriptors state verify)
                            (declare (ignore descriptors state))
                            (setf saved-check verify called t)
                            (if (eq kind :throw) (throw 'retirement-cut :cut)
                                (error "retirement callback cut"))))
                       (error (condition)
                         (when (search "retirement callback cut" (princ-to-string condition)) :cut))))))
           (ok called)
           (ok (handler-case (progn (funcall saved-check) nil)
                 (error (condition) (search "scope" (princ-to-string condition)))))
           (ok (eq :reopened
                   (pg::call-with-physical-reseed-follow-retirement-observation
                    root-fd snapshot check
                    (lambda (descriptors state verify)
                      (declare (ignore descriptors state))
                      (funcall verify)
                      :reopened)))))))))

(deftest reseed-retirement-observation-fences-an-injected-abort
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore workspace))
       (let* ((original (getf (getf snapshot :record) :attempt))
              (aborting (pg::physical-reseed-attempt-copy original))
              (message nil))
         (setf (getf aborting :phase) :aborting)
         (ok (handler-case
                 (progn (pg::physical-reseed-attempt-publish owner aborting :previous original) nil)
               (error (condition)
                 (search "does not authorize legacy reseed attempt operations" (princ-to-string condition)))))
         (ok (equal original (pg::physical-reseed-attempt-read-at root-fd "physical-reseed.attempt")))
         (handler-case
             (pg::call-with-physical-reseed-follow-retirement-observation
              root-fd snapshot check
              (lambda (descriptors state verify)
                (declare (ignore descriptors state verify))
                ;; Inject an external journal change. Follow ownership rejects
                ;; the legacy publisher before it can write an abort.
                (let ((temporary (merge-pathnames "replacement-attempt" root)))
                  (reseed-follow-owner-test-write temporary (pg::physical-reseed-attempt-image aborting))
                  (sb-posix:rename (namestring temporary)
                                   (namestring (merge-pathnames "physical-reseed.attempt" root)))
                  (pg::fsync-directory root))))
           (error (condition) (setf message (princ-to-string condition))))
         (ok (and message (search "exact active original attempt" message)))
         (ok (equal aborting (pg::physical-reseed-attempt-read-at root-fd "physical-reseed.attempt")))
         (let ((before (reseed-mapped-test-tree-facts root)))
           (ok (handler-case
                   (progn
                     (pg::call-with-physical-reseed-follow-retirement-observation
                      root-fd snapshot check
                      (lambda (descriptors state verify)
                        (declare (ignore descriptors state verify))
                        (error "aborted callback must stay unreachable")))
                     nil)
                 (error (condition) (search "exact active original attempt" (princ-to-string condition)))))
           (ok (reseed-activation-layout-facts-equal-p before (reseed-mapped-test-tree-facts root))))))))

(deftest reseed-retirement-observation-refuses-held-guard-and-directory-replacement
    (dolist (kind '(:workspace-lock :incoming-lock :workspace-directory :incoming-directory))
      (call-with-reseed-retirement-observation-fixture
       :same-timeline
       (lambda (owner root workspace root-fd snapshot check)
         (declare (ignore root))
         (let* ((plan (pg::physical-reseed-install-owner-plan owner))
                (directory (if (member kind '(:workspace-lock :workspace-directory))
                               workspace (pathname (getf (getf plan :incoming) :directory))))
                (guard (member kind '(:workspace-lock :incoming-lock)))
                (path (if guard (merge-pathnames ".pg_lisp.lock" directory) directory))
                (backup (if guard (merge-pathnames "lock.original" directory)
                            (pathname (concatenate 'string (string-right-trim "/" (namestring directory)) ".original/"))))
                (moved nil) (created nil) (foreign-before nil) (message nil))
           (unwind-protect
                (progn
                  (handler-case
                      (pg::call-with-physical-reseed-follow-retirement-observation
                       root-fd snapshot check
                       (lambda (descriptors state verify)
                         (declare (ignore descriptors state verify))
                         (sb-sys:without-interrupts
                           (sb-posix:rename (namestring path) (namestring backup))
                           (setf moved t))
                         (if guard
                             (with-open-file (out path :direction :output :if-exists :error
                                                       :if-does-not-exist :create :element-type '(unsigned-byte 8))
                               (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                               (setf created t))
                             (progn
                               (sb-posix:mkdir (namestring path) #o700)
                               (setf created t)))
                         (setf foreign-before
                               (reseed-mapped-test-tree-facts (if guard directory path)))))
                    (error (condition) (setf message (princ-to-string condition))))
                  (ok moved)
                  (ok created)
                  (ok (and message (search (if guard "guard" "target path") message)))
                  (ok (reseed-activation-layout-facts-equal-p
                       foreign-before (reseed-mapped-test-tree-facts (if guard directory path)))))
             (when moved
               (when created
                 (if guard (delete-file path) (uiop:delete-directory-tree path :validate t)))
               (sb-posix:rename (namestring backup) (namestring path))))
           (ok (eq :reopened
                   (pg::call-with-physical-reseed-follow-retirement-observation
                    root-fd snapshot check
                    (lambda (descriptors state verify)
                      (declare (ignore descriptors state))
                      (funcall verify)
                      :reopened)))))))))

(deftest reseed-retirement-observation-rejects-a-retained-root-before-store-open
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner workspace))
       (let* ((path (merge-pathnames "pg_physical_receive/receive.control" root))
              (old (lineage-test-bytes path))
              (bytes (make-array 96 :element-type '(unsigned-byte 8) :initial-element 0))
              (opener (symbol-function 'pg::physical-store-open))
              (opens 0) (called nil) (message nil)
              (registries (list pg::*physical-store-retained-owners* pg::*physical-retained-root-references*
                                pg::*physical-retained-root-holds* pg::*physical-retained-root-updates*))
              (counts (mapcar #'hash-table-count registries)))
         (replace bytes old :end2 (- (length old) 4))
         (pg::wal-set-u64 bytes 76 1)
         (pg::wal-set-u64 bytes 84 (getf (getf (getf snapshot :record) :base) :manifest-digest))
         (pg::wal-set-u32 bytes 92 (pg::wal-crc32 bytes))
         (with-open-file (out path :direction :output :if-exists :overwrite :element-type '(unsigned-byte 8))
           (write-sequence bytes out)
           (finish-output out)
           (sb-posix:ftruncate (sb-sys:fd-stream-fd out) (length bytes)))
         (let ((before (reseed-mapped-test-tree-facts root)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-store-open)
                        (lambda (&rest arguments) (incf opens) (apply opener arguments)))
                  (handler-case
                      (pg::call-with-physical-reseed-follow-retirement-observation
                       root-fd snapshot check
                       (lambda (descriptors state verify)
                         (declare (ignore descriptors state verify))
                         (setf called t)))
                    (error (condition) (setf message (princ-to-string condition)))))
             (setf (symbol-function 'pg::physical-store-open) opener))
           (ok (and message (search "configuration file differs" message)))
           (ok (zerop opens))
           (ok (not called))
           (ok (equal counts (mapcar #'hash-table-count registries)))
           (ok (reseed-activation-layout-facts-equal-p before (reseed-mapped-test-tree-facts root))))))))

(deftest reseed-retirement-observation-reopens-archived-journal-prefixes
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
                (loop for prefix from 1 to 2
                      for name in '("physical-reseed.install" "physical-reseed.attempt")
                      do (setf (getf record :retirement)
                               (list :phase :retiring :step (1- prefix) :attempt-file reference))
                         (reseed-retirement-layout-write root record)
                         (unless (zerop (pg::physical-adoption-renameat2
                                         root-fd name saved name pg::+physical-adoption-rename-noreplace+))
                           (error "retirement observation fixture move failed"))
                         (let ((current (pg::physical-reseed-follow-snapshot-at root-fd))
                               (before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
                           (ok (eq :observed
                                   (pg::call-with-physical-reseed-follow-retirement-observation
                                    root-fd current check
                                    (lambda (descriptors state verify)
                                      (declare (ignore descriptors))
                                      (ok (= prefix (getf state :prefix)))
                                      (ok (funcall verify))
                                      :observed))))
                           (ok (reseed-activation-layout-facts-equal-p
                                before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))))
                (setf (getf (getf record :retirement) :step) 2)
                (reseed-retirement-layout-write root record)
                (setf (getf (getf record :retirement) :phase) :retired)
                (reseed-retirement-layout-write root record)
                (ok (eq :retired
                        (pg::call-with-physical-reseed-follow-retirement-observation
                         root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                         (lambda (descriptors state verify)
                           (declare (ignore descriptors))
                           (ok (= 2 (getf state :prefix)))
                           (ok (funcall verify))
                           :retired)))))
           (sb-posix:close saved))))))

(deftest reseed-retirement-observation-closes-an-interrupted-store-handoff
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner workspace))
       (let ((opener (symbol-function 'pg::physical-store-open))
             (opened nil) (called nil) (path-correct nil) (message nil)
             (before (reseed-mapped-test-tree-facts root)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-store-open)
                      (lambda (&rest arguments)
                        (let* ((options (copy-list (nthcdr 3 arguments)))
                               (handoff (getf options :on-open)))
                          (setf path-correct
                                (equal (getf options :retained-directory)
                                       (merge-pathnames "pg_physical_retained/" (pg::physical-image-fd-path root-fd))))
                          (setf (getf options :on-open)
                                (lambda (store)
                                  (setf opened store)
                                  (funcall handoff store)
                                  (sb-thread:interrupt-thread
                                   sb-thread:*current-thread*
                                   (lambda () (error "injected retirement store interruption")))))
                          (apply opener (append (subseq arguments 0 3) options)))))
                (handler-case
                    (pg::call-with-physical-reseed-follow-retirement-observation
                     root-fd snapshot check
                     (lambda (descriptors state verify)
                       (declare (ignore descriptors state verify))
                       (setf called t)))
                  (error (condition) (setf message (princ-to-string condition)))))
           (setf (symbol-function 'pg::physical-store-open) opener))
         (ok (and message (search "injected retirement store interruption" message)))
         (ok path-correct)
         (ok (not called))
         (ok (and opened (pg::physical-store-closed-p opened)))
         (ok (and opened (not (open-stream-p (pg::physical-store-lock-stream opened)))))
         (ok (reseed-activation-layout-facts-equal-p before (reseed-mapped-test-tree-facts root)))
         (ok (eq :reopened
                 (pg::call-with-physical-reseed-follow-retirement-observation
                  root-fd snapshot check
                  (lambda (descriptors state verify)
                    (declare (ignore descriptors state))
                    (funcall verify)
                    :reopened))))))))

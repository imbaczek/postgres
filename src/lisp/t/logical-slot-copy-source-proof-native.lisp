(in-package #:pg)

(defun logical-source-proof-test-parts (owner context input)
  (let* ((record (logical-slot-copy-install-input-record owner context input))
         (proof (logical-slot-copy-install-input-proof input))
         (certificate (logical-slot-copy-install-input-certificate input))
         (state (logical-slot-copy-committed-proof-state proof)))
    (assert (eq :active (logical-slot-copy-install-input-state-phase record)))
    (assert (= (logical-slot-copy-install-input-state-prefix record)
               (length (getf certificate :slots))))
    (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
      (assert (equal metadata (logical-slot-copy-committed-proof-metadata proof)))
      (assert (equalp prefix (logical-slot-copy-committed-proof-prefix proof)))
      (assert (eql generation (getf certificate :promotion-generation)))
      (let* ((path (physical-restartpoint-generation-path (getf metadata :lsn) generation))
             (receipt (list :metadata (copy-tree metadata)
                            :generation-size (sb-posix:stat-size (slot-state-lstat path))
                            :catalog-size (length catalog)
                            :catalog-digest (logical-slot-copy-import-digest catalog)
                            :next-oid (getf (physical-history-read-catalog catalog) :next-oid)
                            :local-floor (load-next-xid))))
        (logical-slot-copy-install-check-input owner context input)
        (list :installation (copy-seq (logical-slot-copy-install-input-image input))
              :base-proof (logical-slot-copy-read-file
                           context "logical-slot-copy.base-verified" +logical-slot-copy-base-proof-maximum-bytes+)
              :descriptor (logical-slot-copy-descriptor-encode
                           (logical-slot-copy-committed-proof-descriptor proof))
              :evidence (logical-slot-copy-import-evidence-encode
                         (logical-slot-copy-committed-proof-evidence proof))
              :fact (physical-logical-encode-fact (logical-slot-copy-install-input-fact input))
              :restartpoint (logical-slot-copy-source-receipt-image receipt)
              :prefix (copy-seq prefix)
              :child-control (logical-slot-copy-read-file
                              context "global/pg_wal_control" (1+ +wal-control-maximum-bytes+)))))))

(defun logical-source-proof-test-rejected-p (function)
  (handler-case (progn (funcall function) nil) (error () t)))

(defun logical-source-proof-test-check (owner context input)
  (let* ((parts (logical-source-proof-test-parts owner context input))
         (image (logical-slot-copy-source-proof-encode parts))
         (original-read (symbol-function 'physical-restartpoint-read))
         (facts nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-restartpoint-read)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (error "permanent pure checker attempted a forbidden R read")))
           (setf facts (logical-slot-copy-check-source-proof image))
           (let ((directory (physical-recovery-retained-directory)))
             (logical-slot-copy-source-proof-scan-archive
              facts directory #'physical-retained-object-read)
             (logical-slot-copy-source-proof-check-archive-prefix
              facts directory #'physical-retained-object-read)))
      (setf (symbol-function 'physical-restartpoint-read) original-read))
    (assert (equal (getf facts :fact) (logical-slot-copy-install-input-fact input)))
    (assert (eq :pending (getf (getf facts :certificate) :phase)))
    (assert (equalp (getf facts :plan) (logical-slot-copy-install-input-plan input)))
    (assert (not (eq (getf facts :prefix) (getf parts :prefix))))
    (dolist (mode '(:fact :journal :child-control :receipt))
      (let ((bad (logical-slot-copy-source-proof-decode image)))
        (ecase mode
          (:fact
           (let ((fact (physical-logical-decode-fact (getf bad :fact))))
             (incf (getf fact :through))
             (setf (getf bad :fact) (physical-logical-encode-fact fact))))
          (:journal
           (let ((journal (logical-slot-copy-install-decode (getf bad :installation))))
             (setf (getf journal :evidence-digest) (logxor 1 (getf journal :evidence-digest))
                   (getf bad :installation) (logical-slot-copy-install-encode journal))))
          (:child-control
           (setf (aref (getf bad :child-control) 0)
                 (logxor 1 (aref (getf bad :child-control) 0))))
          (:receipt
           (let ((receipt (logical-slot-copy-source-receipt-decode (getf bad :restartpoint))))
             (incf (getf receipt :generation-size))
             (setf (getf bad :restartpoint) (logical-slot-copy-source-receipt-image receipt)))))
        (assert (logical-source-proof-test-rejected-p
                 (lambda ()
                   (logical-slot-copy-check-source-proof
                    (logical-slot-copy-source-proof-encode bad)))))))
    (format t "~&SOURCE-PROOF-BINDING-PASS original-fact=~s bytes=~d rows=~d~%"
            (not (null (getf (getf facts :control) :logical-fact)))
            (length image) (length (getf (getf facts :certificate) :slots)))
    (finish-output)
    (values image facts)))

(defun logical-source-proof-test-fd-cleanup ()
  (declare (notinline sb-posix:close))
  (let* ((parent (sb-posix:open (namestring *pgdata*)
                                (logior sb-posix:o-rdonly sb-posix:o-directory) 0))
         (before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))
         (original-close (symbol-function 'sb-posix:close))
         (owned nil) (foreign nil) (attempts 0))
    (unwind-protect
         (progn
           (assert (eq :escaped
                       (catch 'source-fd-exit
                         (call-with-logical-slot-copy-source-proof-fd
                          parent "logical-slot-copy.source-proof" nil
                          (lambda (fd facts)
                            (declare (ignore facts))
                            (setf owned fd)
                            (throw 'source-fd-exit :escaped))))))
           (assert (handler-case (progn (sb-posix:fstat owned) nil)
                     (sb-posix:syscall-error () t)))
           (assert (logical-source-proof-test-rejected-p
                    (lambda ()
                      (call-with-logical-slot-copy-source-proof-fd
                       parent "logical-slot-copy.source-proof" nil
                       (lambda (fd facts)
                         (declare (ignore facts))
                         (setf owned fd)
                         (funcall original-close fd)
                         (setf foreign (sb-posix:open "/dev/null" sb-posix:o-rdonly 0))
                         (assert (= owned foreign))
                         :foreign-replacement)))))
           (assert (sb-posix:fstat foreign))
           (funcall original-close foreign)
           (setf foreign nil)
           (unwind-protect
                (progn
                  (setf (symbol-function 'sb-posix:close)
                        (lambda (fd)
                          (if (eql fd owned)
                              (progn (incf attempts) (funcall original-close fd)
                                     (error "injected source input close failure"))
                              (funcall original-close fd))))
                  (assert (logical-source-proof-test-rejected-p
                           (lambda ()
                             (call-with-logical-slot-copy-source-proof-fd
                              parent "logical-slot-copy.source-proof" nil
                              (lambda (fd facts)
                                (declare (ignore facts))
                                (setf owned fd)
                                :normal-return)))))
                  (assert (= 1 attempts)))
             (setf (symbol-function 'sb-posix:close) original-close))
           (assert (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
           (format t "~&SOURCE-PROOF-FD-CLEANUP-PASS throw=t reuse=t close-attempts=~d~%" attempts))
      (setf (symbol-function 'sb-posix:close) original-close)
      (when foreign (funcall original-close foreign))
      (funcall original-close parent))))

(defun logical-source-proof-test-reader-failures ()
  (let ((before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))
        (proof (merge-pathnames "logical-slot-copy.source-proof" *pgdata*))
        (archive (merge-pathnames "pg_physical_retained/" *pgdata*))
        (temporary (merge-pathnames "logical-slot-copy.source-proof.tmp" *pgdata*))
        (cases 0))
    (dolist (mode '(:error :throw :late-proof-mode :late-archive-mode :late-temporary))
      (let ((injected nil)
            (*logical-slot-copy-verification-test-hook* nil))
        (unwind-protect
             (progn
               (setf *logical-slot-copy-verification-test-hook*
                     (lambda (point context)
                       (declare (ignore context))
                       (when (eq point :after-source-proof-files-close)
                         (assert (= 1 (hash-table-count *physical-retained-root-references*)))
                         (setf injected t)
                         (ecase mode
                           (:error (error "injected source reader exit"))
                           (:throw (throw 'source-reader-exit :escaped))
                           (:late-proof-mode (sb-posix:chmod (namestring proof) #o644))
                           (:late-archive-mode (sb-posix:chmod (namestring archive) #o755))
                           (:late-temporary
                            (with-open-file (out temporary :direction :output :if-exists :error)
                              (write-line "untrusted temporary" out)))))))
               (if (eq mode :throw)
                   (assert (eq :escaped
                               (catch 'source-reader-exit
                                 (logical-slot-copy-read-source-proof)
                                 :unexpected-return)))
                   (assert (logical-source-proof-test-rejected-p
                            #'logical-slot-copy-read-source-proof)))
               (assert injected)
               (assert (zerop (hash-table-count *physical-retained-root-references*)))
               (assert (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
               (incf cases))
          (setf *logical-slot-copy-verification-test-hook* nil)
          (sb-posix:chmod (namestring proof) #o600)
          (sb-posix:chmod (namestring archive) #o700)
          (when (slot-state-lstat temporary) (delete-file temporary)))))
    (assert (getf (logical-slot-copy-read-source-proof) :fact))
    (assert (zerop (hash-table-count *physical-retained-root-references*)))
    (assert (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
    (format t "~&SOURCE-PROOF-READER-FAILURES-PASS cases=~d retry=t~%" cases)))

(defun logical-source-proof-test-read-independent ()
  (let* ((hidden (uiop:ensure-directory-pathname
                  (sb-posix:mkdtemp
                   (namestring (merge-pathnames "source-proof-hidden-XXXXXX"
                                                (uiop:temporary-directory))))))
         (moved nil) (forbidden 0) (seen nil)
         (saved (loop for name in '(physical-restartpoint-read physical-recovery-read-control
                                    recovery-read-manifest physical-store-open physical-store-load-control
                                    timeline-read-history timeline-history-bytes load-next-xid
                                    wal-read-checkpoint-record)
                      collect (cons name (symbol-function name)))))
    (unwind-protect
         (progn
           (dolist (name '("physical-restartpoint.control" "pg_physical_restartpoints"
                           "pg_physical_receive" "physical-recovery.control"
                           "physical-backup.manifest" "logical-slot-copy.base-verified"
                           "logical-slot-copy.descriptor" "logical-slot-copy.evidence"
                           "logical-slot-copy.import" "logical-slot-copy.install"
                           "logical-slot-copy.work" "global" "pg_wal"))
             (let ((path (merge-pathnames name *pgdata*)))
               (when (slot-state-lstat path)
                 (sb-sys:without-interrupts
                   (sb-posix:rename (namestring path) (namestring (merge-pathnames name hidden)))
                   (push name moved)))))
           (unwind-protect
                (progn
                  (dolist (row saved)
                    (let ((name (car row)))
                      (setf (symbol-function name)
                            (lambda (&rest arguments)
                              (declare (ignore arguments))
                              (incf forbidden)
                              (error "source proof reader attempted forbidden access through ~s" name)))))
                  (let* ((*logical-slot-copy-verification-test-hook*
                           (lambda (point context)
                             (declare (ignore context))
                             (when (eq point :after-source-proof-files-close)
                               (push point seen)
                               (assert (= 1 (hash-table-count *physical-retained-root-references*))))))
                         (facts (logical-slot-copy-read-source-proof)))
                    (assert (getf facts :fact))
                    (assert (getf facts :certificate))
                    (assert (= 1 (length seen)))
                    (assert (zerop forbidden))
                    (assert (zerop (hash-table-count *physical-retained-root-references*)))
                    (let ((*logical-slot-copy-verification-test-hook* nil))
                      (logical-source-proof-test-fd-cleanup)
                      (logical-source-proof-test-reader-failures))
                    (format t "~&SOURCE-PROOF-READER-PASS hidden=~d forbidden=~d~%"
                            (length moved) forbidden)
                    facts))
             (dolist (row saved) (setf (symbol-function (car row)) (cdr row)))))
      (dolist (name moved)
        (sb-posix:rename (namestring (merge-pathnames name hidden))
                         (namestring (merge-pathnames name *pgdata*))))
      (uiop:delete-directory-tree hidden :validate t :if-does-not-exist :ignore))))

(defun logical-source-proof-test-run ()
  (let ((before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t)))
        (path (merge-pathnames "logical-slot-copy.source-proof" *pgdata*)))
    (unless (slot-state-lstat path)
      (unless (slot-state-lstat (merge-pathnames "logical-slot-copy.install" *pgdata*))
        (assert (eq :active (logical-slot-copy-install-fresh-active))))
      (let ((image (call-with-logical-slot-copy-install-cold-input #'logical-source-proof-test-check)))
        (with-open-file (out path :direction :output :if-exists :error :element-type '(unsigned-byte 8))
          (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
          (write-sequence image out)
          (finish-output out)
          (sb-posix:fsync (sb-sys:fd-stream-fd out)))
        (fsync-directory *pgdata*)))
    (assert (zerop (hash-table-count *physical-retained-root-holds*)))
    (assert (zerop (hash-table-count *physical-retained-root-updates*)))
    (clrhash *physical-retained-root-references*)
    (logical-source-proof-test-read-independent)
    (let ((image (call-with-logical-slot-copy-install-cold-input
                  (lambda (owner context input)
                    (logical-source-proof-test-parts owner context input)))))
      (assert (equalp (logical-slot-copy-source-proof-encode image)
                      (recovery-read-bytes path
                                           :maximum (logical-slot-copy-source-proof-maximum-bytes))))
      (format t "~&SOURCE-PROOF-CANONICAL-INPUT-PASS~%"))
    (assert (logical-source-proof-test-rejected-p #'logical-slot-copy-check-promotion))
    (assert (= before (length (directory-entry-names "/proc/self/fd/" :follow-symlinks t))))
    (assert (and (null *physical-recovery-state*) (null *physical-receive-store*)
                 (null *catalog*) (null *clog-stream*) (null *wal-output-stream*)
                 (not *wal-ready*) (not *physical-recovery-failed-p*)
                 (zerop (hash-table-count *wal-replication-slots*))))))

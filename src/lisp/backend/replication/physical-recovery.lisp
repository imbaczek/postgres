;;;; Verified immutable base-image recovery. Streaming replay extends this state later.
(in-package #:pg)

(defun recovery-read-bytes (path &key (maximum (* 16 1024 1024)))
  "Read bounded control metadata without creating or changing any file."
  (with-open-file (stream path :element-type '(unsigned-byte 8))
    (let ((length (file-length stream)))
      (unless (<= 1 length maximum) (error "invalid recovery metadata length"))
      (let ((bytes (make-array length :element-type '(unsigned-byte 8))))
        (unless (= length (read-sequence bytes stream)) (error "short recovery metadata read"))
        bytes))))

(defun physical-recovery-control-path (&optional (directory *pgdata*))
  (merge-pathnames "physical-recovery.control" directory))

(defconstant +db-startup+ 0)

(defconstant +db-shutdowned-in-recovery+ 2)

(defconstant +db-in-archive-recovery+ 5)

(defun physical-recovery-control-valid-p (control &key legacy)
  "Validate role/progress and native recovery DBState as separate invariants."
  (destructuring-bind (&key phase system-id timeline manifest-digest checkpoint
                         applied next-xid db-state state-time &allow-other-keys)
      control
    (and (member phase '(0 1 2))
         (eql timeline 1)
         (every (lambda (value) (typep value '(unsigned-byte 64)))
                (list system-id manifest-digest checkpoint applied next-xid))
         (<= 32 checkpoint applied)
         (or (= phase 2) (= checkpoint applied))
         (<= 3 next-xid)
         (or (plusp phase) (zerop manifest-digest))
         (if legacy
             (and (null db-state) (null state-time))
             (and (typep state-time '(signed-byte 64))
                  (if (zerop phase)
                      (eql db-state +db-startup+)
                      (member db-state (list +db-in-archive-recovery+
                                             +db-shutdowned-in-recovery+))))))))

(defun physical-recovery-control-image (phase system-id timeline manifest-digest checkpoint applied next-xid
                                        &key (db-state (if (zerop phase) +db-startup+
                                                           +db-in-archive-recovery+))
                                          (state-time (- (get-universal-time) 2208988800)))
  "Encode v2 native recovery control; never persist cluster write authority."
  (unless (physical-recovery-control-valid-p
           (list :phase phase :system-id system-id :timeline timeline
                 :manifest-digest manifest-digest :checkpoint checkpoint :applied applied
                 :next-xid next-xid :db-state db-state :state-time state-time))
    (error "unrepresentable physical recovery control state"))
  (let ((bytes (make-array 72 :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes #(80 71 82 83 84 65 84 50))
    (wal-set-u32 bytes 8 phase)
    (wal-set-u32 bytes 12 timeline)
    (wal-set-u64 bytes 16 system-id)
    (wal-set-u64 bytes 24 manifest-digest)
    (wal-set-u64 bytes 32 checkpoint)
    (wal-set-u64 bytes 40 applied)
    (wal-set-u64 bytes 48 next-xid)
    (wal-set-u32 bytes 56 db-state)
    (wal-set-i64 bytes 60 state-time)
    (wal-set-u32 bytes 68 (wal-crc32 bytes))
    bytes))

(defun physical-recovery-decode-control (bytes)
  "Read legacy progress conservatively, or v2 state with exact length and CRC."
  (unless (member (length bytes) '(60 72))
    (error "invalid physical recovery control length"))
  (let* ((legacy (= (length bytes) 60))
         (crc-offset (if legacy 56 68))
         (copy (copy-seq bytes))
         (crc (wal-u32 copy crc-offset)))
    (wal-set-u32 copy crc-offset 0)
    (unless (and (equalp (subseq bytes 0 8)
                         (if legacy #(80 71 82 83 84 65 84 69) #(80 71 82 83 84 65 84 50)))
                 (= crc (wal-crc32 copy)))
      (error "corrupt or unsupported physical recovery control"))
    (let ((control
            (list :control-version (if legacy 1 2)
                  :phase (wal-u32 bytes 8) :timeline (wal-u32 bytes 12)
                  :system-id (wal-u64 bytes 16) :manifest-digest (wal-u64 bytes 24)
                  :checkpoint (wal-u64 bytes 32) :applied (wal-u64 bytes 40)
                  :next-xid (wal-u64 bytes 48)
                  :db-state (unless legacy (wal-u32 bytes 56))
                  :state-time (unless legacy (wal-i64 bytes 60)))))
      (unless (physical-recovery-control-valid-p control :legacy legacy)
        (error "corrupt or unsupported physical recovery control"))
      control)))

(defun physical-recovery-read-control (directory)
  (physical-recovery-decode-control
   (recovery-read-bytes (physical-recovery-control-path directory) :maximum 72)))

(defvar *physical-recovery-control-test-hook* nil
  "Optional deterministic hook for recovery control publication failures.")

(defun physical-recovery-control-test-point (point state)
  (when *physical-recovery-control-test-hook*
    (funcall *physical-recovery-control-test-hook* point state)))

(defun physical-recovery-write-control (state phase applied next-xid
                                        &key (db-state +db-in-archive-recovery+))
  "Publish role/progress/state atomically; never lower startup recovery requirements."
  (when (and (= db-state +db-shutdowned-in-recovery+)
             (< applied (or (getf state :required-applied) applied)))
    (error "clean recovery has not reached its previously applied boundary"))
  (let* ((state (copy-list state))
         (applied (max applied (or (getf state :required-applied) applied)))
         (next-xid (max next-xid (or (getf state :observed-next-xid) next-xid)))
         (time (- (get-universal-time) 2208988800))
         (bytes (physical-recovery-control-image
                 phase (getf state :system-id) (getf state :timeline)
                 (getf state :manifest-digest) (getf state :checkpoint) applied next-xid
                 :db-state db-state :state-time time))
         (path (physical-recovery-control-path)))
    (physical-recovery-control-test-point :before-write db-state)
    (with-atomic-output
        (stream path (make-pathname :type "tmp" :defaults path) *pgdata*
                :element-type '(unsigned-byte 8)
                :fsync-function
                (lambda (fd)
                  (physical-recovery-control-test-point :before-fsync db-state)
                  (sb-posix:fsync fd)
                  (physical-recovery-control-test-point :after-fsync db-state))
                :rename-function
                (lambda (from to)
                  (physical-recovery-control-test-point :before-rename db-state)
                  (sb-posix:rename from to)
                  (physical-recovery-control-test-point :after-rename db-state))
                :fsync-directory-function
                (lambda (directory)
                  (physical-recovery-control-test-point :before-directory-fsync db-state)
                  (fsync-directory directory)
                  (physical-recovery-control-test-point :after-directory-fsync db-state)))
      (write-sequence bytes stream))
    (setf (getf state :phase) phase
          (getf state :db-state) db-state
          (getf state :state-time) time
          (getf state :control-version) 2
          (getf state :required-applied) applied
          (getf state :observed-next-xid) next-xid)
    (physical-recovery-control-test-point :after-write db-state)
    state))

(defun physical-recovery-mark-target (directory)
  "Persist incomplete recovery role before any copied database files are created."
  (when (probe-file (physical-recovery-control-path directory))
    (error "physical recovery control already exists"))
  (wal-atomic-write-bytes
   (physical-recovery-control-path directory)
   (physical-recovery-control-image 0 (catalog-system-identifier) 1 0
                                    *wal-checkpoint-lsn* *wal-durable-lsn* (load-next-xid))))

(defun recovery-check-literal-text (text)
  "Accept only the literal list/string/number/keyword grammar emitted by our manifest."
  (let ((quoted nil) (escaped nil) (depth 0))
    (loop for char across text
          do (cond
               (quoted
                (cond (escaped (setf escaped nil))
                      ((char= char #\\) (setf escaped t))
                      ((char= char #\") (setf quoted nil))))
               ((char= char #\") (setf quoted t))
               ((char= char #\() (when (> (incf depth) 16) (error "recovery manifest too deep")))
               ((char= char #\)) (when (minusp (decf depth)) (error "invalid recovery manifest")))
               ((or (find char " :+-" :test #'char=)
                    (find char '(#\Tab #\Newline #\Return #\Page))
                    (and (char>= char #\A) (char<= char #\Z))
                    (and (char>= char #\0) (char<= char #\9))))
               (t (error "nonliteral syntax in recovery manifest"))))
    (when (or quoted escaped (not (zerop depth))) (error "incomplete recovery manifest"))))

(defun recovery-parse-manifest-text (text)
  "Read only the bounded literal grammar, with reader evaluation disabled."
  (recovery-check-literal-text text)
  (with-standard-io-syntax
    (let ((*read-eval* nil) (*package* (find-package "CL-USER")))
      (multiple-value-bind (manifest position) (read-from-string text)
        (unless (every (lambda (char) (find char '(#\Space #\Tab #\Newline #\Return)))
                       (subseq text position))
          (error "trailing recovery manifest data"))
        manifest))))

(defun recovery-read-manifest (directory)
  "Check the envelope CRC before safely reading its bounded literal payload."
  (let* ((bytes (recovery-read-bytes (merge-pathnames "physical-backup.manifest" directory)))
         (newline (position 10 bytes))
         (header (and newline (bytes->string bytes :end newline))))
    (unless (and header (= (length header) 26)
                 (uiop:string-prefix-p "PGPHYSICALBACKUP1 " header)
                 (= (aref bytes (1- (length bytes))) 10))
      (error "invalid physical recovery manifest envelope"))
    (let* ((text-start (1+ newline))
           (text-end (1- (length bytes)))
           (crc (ignore-errors (parse-integer header :start 18 :radix 16))))
      (unless (and crc (= crc (wal-crc32 bytes text-start text-end)))
        (error "physical recovery manifest checksum mismatch"))
      (values (recovery-parse-manifest-text
               (bytes->string bytes :start text-start :end text-end))
              (physical-store-update-digest #xCBF29CE484222325 bytes)))))

(defun recovery-manifest-path-p (name)
  "Whitelist the native producer's file grammar before opening manifest paths."
  (and (stringp name)
       (or (member name '("PG_VERSION" "pg_xact" "global/pg_control"
                          "global/pg_catalog.dat" "global/pg_wal_control") :test #'equal)
           (and (uiop:string-prefix-p "base/" name)
                (let* ((suffix (subseq name 5))
                       (digits (if (uiop:string-suffix-p suffix ".vm")
                                   (subseq suffix 0 (- (length suffix) 3)) suffix)))
                  (and (plusp (length digits)) (every #'digit-char-p digits))))
           (and (= (length name) 27) (uiop:string-prefix-p "pg_wal/" name)
                (string= name ".wal" :start1 23)
                (every (lambda (char) (digit-char-p char 16)) (subseq name 7 23))))))

(defun physical-manifest-checkpoint-start (manifest)
  (if (eql 2 (getf manifest :version)) (getf manifest :checkpoint-start)
      (- (getf manifest :checkpoint) +wal-header-size+)))

(defun recovery-check-manifest-schema (manifest)
  (unless (and (listp manifest) (member (getf manifest :version) '(1 2))
               (equal (loop for (key value) on manifest by #'cddr collect key)
                      (if (eql 2 (getf manifest :version))
                          '(:version :system-id :cluster-identity :timeline :checkpoint
                            :checkpoint-start :end :next-xid :source-pin :files)
                          '(:version :system-id :cluster-identity :timeline :checkpoint
                            :end :next-xid :source-pin :files))) (eql 1 (getf manifest :timeline))
               (typep (getf manifest :system-id) '(unsigned-byte 64))
               (stringp (getf manifest :cluster-identity))
               (plusp (length (getf manifest :cluster-identity)))
               (typep (getf manifest :checkpoint) '(integer 32 18446744073709551615))
               (= (getf manifest :checkpoint) (getf manifest :end))
               (typep (physical-manifest-checkpoint-start manifest) '(unsigned-byte 64))
               (member (- (getf manifest :checkpoint) (physical-manifest-checkpoint-start manifest))
                       (list +wal-header-size+ (wal-checkpoint-record-size)))
               (typep (getf manifest :next-xid)
                      '(integer 3 18446744073709551615))
               (wal-valid-replication-slot-name-p (getf manifest :source-pin))
               (listp (getf manifest :files)) (getf manifest :files))
    (error "unsupported or malformed physical recovery manifest"))
  (let ((names (make-hash-table :test #'equal)))
    (dolist (entry (getf manifest :files))
      (unless (and (listp entry) (= (length entry) 3)
                   (recovery-manifest-path-p (first entry))
                   (typep (second entry) '(unsigned-byte 64))
                   (typep (third entry) '(unsigned-byte 64))
                   (not (gethash (first entry) names)))
        (error "invalid or duplicate physical recovery file entry"))
      (setf (gethash (first entry) names) t)))
  manifest)

(defun recovery-check-manifest-files (manifest directory)
  "Verify every declared immutable base file before any storage initializer runs."
  (let ((root (truename directory))
        (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
    (dolist (entry (getf manifest :files))
      (destructuring-bind (name length digest) entry
        (let ((path (merge-pathnames name root))
              (actual #xCBF29CE484222325))
          (unless (equal path (truename path)) (error "symlink in physical recovery image"))
          (with-open-file (stream path :element-type '(unsigned-byte 8))
            (unless (= length (file-length stream)) (error "physical recovery file length mismatch: ~a" name))
            (loop for count = (read-sequence buffer stream)
                  while (plusp count)
                  do (setf actual (physical-store-update-digest actual (subseq buffer 0 count)))))
          (unless (= digest actual) (error "physical recovery file checksum mismatch: ~a" name)))))))

(defun recovery-check-image-metadata (manifest directory)
  "Bind catalog identity, exact file inventory, WAL checkpoint and XID reservation."
  (let* ((*pgdata* directory)
         (*catalog-identity-needs-persist-p* nil)
         (*catalog* (load-catalog))
         (*wal-checkpoint-lsn* (getf manifest :checkpoint))
         (*wal-durable-lsn* (getf manifest :end))
         (*wal-checkpoint-snapshot* (nth-value 2 (wal-read-control-fields)))
         (names (sort (mapcar #'first (getf manifest :files)) #'string<)))
    (unless (and (not *catalog-identity-needs-persist-p*)
                 (equal (catalog-cluster-identity *catalog*) (getf manifest :cluster-identity))
                 (= (catalog-system-identifier) (getf manifest :system-id))
                 (gethash "postgres" (catalog-databases *catalog*))
                 (equal names (physical-bootstrap-inventory)))
      (error "physical recovery catalog identity or inventory mismatch"))
    (dolist (subdirectory '("base/" "global/" "pg_wal/"))
      (let* ((path (merge-pathnames subdirectory directory))
             (actual (sort (mapcar (lambda (file)
                                     (concatenate 'string subdirectory (file-namestring file)))
                                   (uiop:directory-files path)) #'string<))
             (expected (remove-if-not
                        (lambda (name) (uiop:string-prefix-p subdirectory name)) names)))
        (unless (and (null (uiop:subdirectories path)) (equal expected actual))
          (error "unexpected files in physical recovery directory ~a" subdirectory))))
    (with-open-file (stream (merge-pathnames "PG_VERSION" directory))
      (unless (and (equal "16" (read-line stream nil nil)) (null (read-line stream nil nil)))
        (error "unsupported native physical recovery data version")))
    (multiple-value-bind (checkpoint durable) (wal-read-control-fields)
      (unless (and (eql checkpoint *wal-checkpoint-lsn*) (eql durable *wal-durable-lsn*)
                   (= (physical-manifest-checkpoint-start manifest) (wal-checkpoint-record-start)))
        (error "physical recovery WAL control differs from manifest")))
    (unless (and (not (probe-file (xid-wrap-marker-path)))
                 (<= (getf manifest :next-xid) (load-next-xid)))
      (error "physical recovery XID reservation mismatch"))
    (physical-source-pin-check-wal
     (make-physical-source-pin :name (getf manifest :source-pin)
                               :system-id (getf manifest :system-id)
                               :consistent-lsn *wal-checkpoint-lsn*
                               :restart-lsn (physical-manifest-checkpoint-start manifest)))
    *catalog*))

(defun physical-recovery-prepare ()
  "Validate provenance, then durably enter active recovery before any restoration."
  (unless (probe-file (merge-pathnames "recovery.required" *pgdata*))
    (error "physical recovery control or manifest has lost its required recovery marker"))
  (let ((marker (bytes->string (recovery-read-bytes
                                (merge-pathnames "recovery.required" *pgdata*) :maximum 128))))
    (unless (equal marker (format nil "PGPHYSICAL-RECOVERY-REQUIRED-1~%"))
      (error "corrupt physical recovery marker")))
  (let ((control (physical-recovery-read-control *pgdata*)))
    (multiple-value-bind (manifest digest) (recovery-read-manifest *pgdata*)
      (recovery-check-manifest-schema manifest)
      (unless (and (= (getf control :system-id) (getf manifest :system-id))
                   (= (getf control :timeline) (getf manifest :timeline))
                   (= (getf control :checkpoint) (getf manifest :checkpoint))
                   (or (zerop (getf control :phase))
                       (= (getf control :manifest-digest) digest)))
        (error "physical recovery control does not match its base provenance"))
      (cond
        ((= 2 (getf control :phase))
         (physical-recovery-check-streaming-base manifest control))
        (t
         (recovery-check-manifest-files manifest *pgdata*)
         (recovery-check-image-metadata manifest *pgdata*)
         (unless (and (= (getf control :applied) (getf manifest :end))
                      (= (getf control :next-xid) (load-next-xid)))
           (error "physical recovery control does not match its immutable base image"))))
      (multiple-value-bind (checkpoint durable snapshot) (wal-read-control-fields)
        (unless (and (eql checkpoint (getf manifest :checkpoint))
                     (eql durable (getf manifest :end))
                     (= (physical-manifest-checkpoint-start manifest)
                        (if snapshot (getf snapshot :start) (- checkpoint +wal-header-size+))))
          (error "base checkpoint control differs from manifest"))
        (setf (getf manifest :source-checkpoint) snapshot))
      (setf (getf manifest :manifest-digest) digest
            (getf manifest :phase) (if (= 2 (getf control :phase)) 2 1)
            (getf manifest :required-applied) (getf control :applied)
            (getf manifest :observed-next-xid) (getf control :next-xid)
            (getf manifest :previous-db-state) (getf control :db-state)
            (getf manifest :previous-state-time) (getf control :state-time))
      ;; Includes phase 0 -> 1 and legacy migration. A clean previous run never
      ;; allows catalog restoration or redo to start with a clean control file.
      (setf manifest
            (physical-recovery-write-control
             manifest (getf manifest :phase) (getf control :applied) (getf control :next-xid)))
      (setf *physical-recovery-state* manifest))))

(defun note-primary-startup-replay ()
  "Report the last applied terminal, excluding complete records in an unfinished batch."
  (setf *last-replay-lsn* nil)
  ;; WAL-RECOVER has already validated and applied terminal batches. A second
  ;; read-only pass records their boundary without mistaking durable receipt
  ;; of an incomplete batch for applied recovery progress.
  (wal-scan
   (lambda (record)
     (when (and (> (wal-record-end record) *wal-checkpoint-lsn*)
                (wal-terminal-record-type-p (wal-record-type record)))
       (setf *last-replay-lsn* (wal-record-end record)))))
  (values))

(defun physical-recovery-open-storage ()
  "Open verified recovery; phase 2 redoes its entire retained prefix before SQL."
  (unless (recovery-in-progress-p) (error "physical recovery image was not verified"))
  (let ((streaming (= 2 (getf *physical-recovery-state* :phase))))
    (setf *wal-checkpoint-snapshot* (copy-list (getf *physical-recovery-state* :source-checkpoint))
          *wal-checkpoint-lsn* (getf *physical-recovery-state* :checkpoint)
          *wal-durable-lsn* (getf *physical-recovery-state* :end)
          *wal-end-lsn* *wal-durable-lsn*
          *last-replay-lsn* *wal-durable-lsn*
          *wal-ready* t
          *wal-poisoned* nil
          *next-xid* (getf *physical-recovery-state* :observed-next-xid)
          *xid-batch-end* *next-xid*
          *active-xids* nil
          *catalog* (load-catalog))
    (when streaming
      (setf *physical-receive-store* (physical-recovery-open-receive-store))
      (unless (= (physical-store-base-lsn *physical-receive-store*)
                 (getf *physical-recovery-state* :end))
        (error "streaming receive base differs from verified backup"))
      (setf (getf *physical-recovery-state* :receive-lsn)
            (physical-store-flushed-lsn *physical-receive-store*))
      (unless (<= (getf *physical-recovery-state* :required-applied)
                  (physical-store-flushed-lsn *physical-receive-store*))
        (error "streaming receive spool lost previously applied WAL"))
      (physical-restartpoint-prepare-storage *physical-receive-store*)
      (setf *clog-stream* (open (clog-path) :element-type '(unsigned-byte 8)
                                            :direction :io :if-exists :overwrite
                                            :if-does-not-exist :error))
      (let ((*physical-replay-restarting-p* t))
        (physical-replay-from *last-replay-lsn*))
      (unless (<= (getf *physical-recovery-state* :required-applied) *last-replay-lsn*)
        (error "streaming redo did not reach its previously applied terminal"))
      (physical-recovery-check-live-files))
    (unless streaming
      (setf *clog-stream* (open (clog-path) :element-type '(unsigned-byte 8)
                                            :direction :input :if-does-not-exist :error)))
    (catalog-refresh-oldest-relfrozenxid))
  (values))

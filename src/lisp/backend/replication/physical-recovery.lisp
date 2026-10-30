;;;; Verified immutable base-image recovery. Streaming replay extends this state later.
(in-package #:pg)

(defun recovery-read-bytes (path &key (maximum (* 16 1024 1024)))
  "Read bounded control metadata without creating or changing any file."
  (read-bounded-regular-file path maximum :minimum 1))

(defun physical-recovery-check-required-marker ()
  "Validate the exact bounded guard before ordinary or pending reconstruction."
  (let* ((path (merge-pathnames "recovery.required" *pgdata*))
         (stat (sb-posix:lstat (namestring path))))
    (unless (= (logand (sb-posix:stat-mode stat) sb-posix:s-ifmt) sb-posix:s-ifreg)
      (error "physical recovery marker is not a regular file"))
    (unless (equalp (recovery-read-bytes path :maximum 128) (physical-recovery-marker-bytes))
      (error "corrupt physical recovery marker"))))

(defun physical-recovery-control-path (&optional (directory *pgdata*))
  (merge-pathnames "physical-recovery.control" directory))

(defun physical-recovery-selection-fields-valid-p (phase version base applied target digest)
  "Check selected-history fields without conflating base, applied and target timelines.
An absent selection requires the applied timeline to remain the base timeline."
  (if (or target digest)
      (and (member phase '(2 3)) (member version '(3 4))
           (typep target '(integer 1 4294967295)) (> target base)
           (typep digest '(unsigned-byte 64))
           (typep applied '(integer 1 4294967295)) (<= base applied target))
      (eql applied base)))

(defun physical-recovery-control-valid-p (control &key legacy)
  "Validate recovery progress and a pending child's local identity.

A retained reference is allowed only in pending control and must match its base
timeline, checkpoint, manifest digest and applied end. This checks fields, not
the referenced archive."
  (destructuring-bind (&key phase system-id timeline manifest-digest checkpoint
                         applied next-xid db-state state-time (control-version 3)
                         (applied-timeline timeline) history-target history-digest
                         promotion-child promotion-generation promotion-source-namespace retained-reference resource-fact
                       &allow-other-keys)
      control
    (and (member phase '(0 1 2 3))
         (typep timeline '(integer 1 4294967295))
         (or (member control-version '(3 4)) (eql timeline 1))
         (every (lambda (value) (typep value '(unsigned-byte 64)))
                (list system-id manifest-digest checkpoint applied))
         (member control-version '(1 2 3 4))
         (or (null resource-fact)
             (and (member control-version '(3 4))
                  (typep applied-timeline '(integer 1 4294967295))
                  (wal-resource-fact-valid-p resource-fact)
                  (<= (getf resource-fact :resource-lsn) applied)
                  (<= (getf resource-fact :resource-timeline) applied-timeline)))
         (or (null retained-reference)
             (and (= phase 3) (= control-version 4)
                  (wal-retained-reference-valid-p retained-reference)
                  (eql timeline (getf retained-reference :base-timeline))
                  (eql checkpoint (getf retained-reference :base))
                  (eql manifest-digest (getf retained-reference :manifest-digest))
                  (eql applied (getf retained-reference :end))))
         (physical-recovery-selection-fields-valid-p
          phase control-version timeline applied-timeline history-target history-digest)
         (integerp next-xid)
         (<= 3 next-xid (if (member control-version '(3 4)) 18446744073709551616
                            18446744073709551615))
         (<= 32 checkpoint applied)
         (or (member phase '(2 3)) (= checkpoint applied))
         (or (plusp phase) (zerop manifest-digest))
         (if (= phase 3)
             (and (= control-version 4) (not legacy)
                  (typep promotion-child '(integer 2 4294967295))
                  (> promotion-child applied-timeline)
                  (or (eql timeline 1) (eq promotion-source-namespace :timeline))
                  (typep promotion-generation '(unsigned-byte 64))
                  (member promotion-source-namespace '(:legacy :timeline))
                  (eql db-state +db-in-archive-recovery+))
             (and (/= control-version 4) (null promotion-child)
                  (null promotion-generation) (null promotion-source-namespace)))
         (if legacy
             (and (null db-state) (null state-time))
             (and (typep state-time '(signed-byte 64))
                  (if (zerop phase)
                      (eql db-state +db-startup+)
                      (member db-state (list +db-in-archive-recovery+
                                             +db-shutdowned-in-recovery+))))))))

(defun physical-recovery-write-retained-reference (bytes offset reference)
  "Write the 44-byte retained reference at OFFSET in BYTES."
  (wal-set-u64 bytes offset (getf reference :root-id))
  (wal-set-u32 bytes (+ offset 8) (getf reference :base-timeline))
  (loop for key in '(:base :manifest-digest :end :end-digest)
        for position from (+ offset 12) by 8
        do (wal-set-u64 bytes position (getf reference key))))

(defun physical-recovery-read-retained-reference (bytes offset)
  "Read a 44-byte retained reference from BYTES at OFFSET."
  (list :root-id (wal-u64 bytes offset) :base-timeline (wal-u32 bytes (+ offset 8))
        :base (wal-u64 bytes (+ offset 12)) :manifest-digest (wal-u64 bytes (+ offset 20))
        :end (wal-u64 bytes (+ offset 28)) :end-digest (wal-u64 bytes (+ offset 36))))

(defun physical-recovery-control-image (phase system-id timeline manifest-digest checkpoint applied next-xid
                                        &key (db-state (if (zerop phase) +db-startup+ +db-in-archive-recovery+))
                                          (state-time (- (get-universal-time) 2208988800))
                                          (applied-timeline timeline) history-target history-digest
                                          promotion-child promotion-generation promotion-source-namespace retained-reference resource-fact)
  "Encode recovery progress and its optional extensions under one CRC.
Ordinary and pending images may append a 48-byte positioned resource fact."
  (let* ((pending (= phase 3)) (version (if pending 4 3))
                               (extension-start (if pending 84 68))
                               (selected (not (null history-target)))
                               (reference-offset (+ extension-start (if selected 16 0)))
                               (resource-offset (+ reference-offset (if retained-reference 44 0)))
                               (crc-offset (+ resource-offset (if resource-fact +wal-resource-fact-size+ 0))))
    (unless (physical-recovery-control-valid-p
             (list :control-version version :phase phase :system-id system-id :timeline timeline
                   :manifest-digest manifest-digest :checkpoint checkpoint :applied applied
                   :next-xid next-xid :db-state db-state :state-time state-time
                   :applied-timeline applied-timeline :history-target history-target :history-digest history-digest
                   :promotion-child promotion-child :promotion-generation promotion-generation
                   :promotion-source-namespace promotion-source-namespace :retained-reference retained-reference
                   :resource-fact resource-fact))
      (error "unrepresentable physical recovery control state"))
    (let ((bytes (make-array (+ crc-offset 4) :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace bytes (if pending #(80 71 82 83 84 65 84 52) #(80 71 82 83 84 65 84 51)))
      (wal-set-u32 bytes 8 (logior phase (if (= next-xid 18446744073709551616) #x80000000 0)))
      (wal-set-u32 bytes 12 timeline)
      (wal-set-u64 bytes 16 system-id)
      (wal-set-u64 bytes 24 manifest-digest)
      (wal-set-u64 bytes 32 checkpoint)
      (wal-set-u64 bytes 40 applied)
      (wal-set-u64 bytes 48 (if (= next-xid 18446744073709551616) 0 next-xid))
      (wal-set-u32 bytes 56 db-state)
      (wal-set-i64 bytes 60 state-time)
      (when pending
        (wal-set-u32 bytes 68 promotion-child)
        (wal-set-u32 bytes 72 (ecase promotion-source-namespace (:legacy 0) (:timeline 1)))
        (wal-set-u64 bytes 76 promotion-generation))
      (when selected
        (wal-set-u32 bytes extension-start applied-timeline)
        (wal-set-u32 bytes (+ extension-start 4) history-target)
        (wal-set-u64 bytes (+ extension-start 8) history-digest))
      (when retained-reference
        (physical-recovery-write-retained-reference bytes reference-offset retained-reference))
      (when resource-fact
        (replace bytes (wal-encode-resource-fact resource-fact) :start1 resource-offset))
      (wal-set-u32 bytes crc-offset (wal-crc32 bytes))
      bytes)))

(defun physical-recovery-decode-control (bytes)
  "Decode recovery control by magic and exact length, then verify its CRC.
Legacy versions use 60 or 72 bytes. Ordinary v3 uses 72 or 88 bytes;
pending v4 uses 88 or 104 bytes, or 132 or 148 with a retained reference.
The v3 and v4 layouts can add a 48-byte resource fact before the CRC.
Magic distinguishes the overlapping 88-byte and 136-byte layouts."

  (unless (member (length bytes) '(60 72 88 104 120 132 136 148 152 180 196))
    (error "invalid physical recovery control length"))
  (let* ((size (length bytes)) (magic (subseq bytes 0 8))
                               (resources (or (and (equalp magic #(80 71 82 83 84 65 84 51))
                                                   (member size '(120 136)))
                                              (and (equalp magic #(80 71 82 83 84 65 84 52))
                                                   (member size '(136 152 180 196)))))
                               (layout-size (- size (if resources +wal-resource-fact-size+ 0)))
                               (version (cond
                                          ((and (= size 60) (equalp magic #(80 71 82 83 84 65 84 69))) 1)
                                          ((and (= size 72) (equalp magic #(80 71 82 83 84 65 84 50))) 2)
                                          ((and (member layout-size '(72 88)) (equalp magic #(80 71 82 83 84 65 84 51))) 3)
                                          ((and (member layout-size '(88 104 132 148)) (equalp magic #(80 71 82 83 84 65 84 52))) 4)))
                               (legacy (eql version 1)) (pending (eql version 4))
                               (extension-start (if pending 84 68))
                               (retained (and pending (member layout-size '(132 148))))
                               (selected (and (member version '(3 4))
                                              (= layout-size (+ extension-start 20 (if retained 44 0)))))
                               (crc-offset (- size 4))
                               (copy (copy-seq bytes)) (crc (wal-u32 copy crc-offset))
                               (raw-phase (wal-u32 bytes 8))
                               (exhausted (and (member version '(3 4)) (logbitp 31 raw-phase))))
    (wal-set-u32 copy crc-offset 0)
    (unless (and version (= crc (wal-crc32 copy))
                 (or (not exhausted) (zerop (wal-u64 bytes 48))))
      (error "corrupt or unsupported physical recovery control"))
    (let ((control
            (list :control-version version
                  :phase (if (member version '(3 4)) (logand raw-phase #x7fffffff) raw-phase)
                  :timeline (wal-u32 bytes 12)
                  :system-id (wal-u64 bytes 16) :manifest-digest (wal-u64 bytes 24)
                  :checkpoint (wal-u64 bytes 32) :applied (wal-u64 bytes 40)
                  :next-xid (if exhausted 18446744073709551616 (wal-u64 bytes 48))
                  :db-state (unless legacy (wal-u32 bytes 56))
                  :state-time (unless legacy (wal-i64 bytes 60)))))
      (when pending
        (setf (getf control :promotion-child) (wal-u32 bytes 68)
              (getf control :promotion-source-namespace)
              (case (wal-u32 bytes 72) (0 :legacy) (1 :timeline))
              (getf control :promotion-generation) (wal-u64 bytes 76)))
      (when selected
        (setf (getf control :applied-timeline) (wal-u32 bytes extension-start)
              (getf control :history-target) (wal-u32 bytes (+ extension-start 4))
              (getf control :history-digest) (wal-u64 bytes (+ extension-start 8))))
      (when retained
        (setf (getf control :retained-reference)
              (physical-recovery-read-retained-reference bytes (- layout-size 48))))
      (when resources
        (setf (getf control :resource-fact)
              (wal-decode-resource-fact (subseq bytes (- size 52) crc-offset))))
      (unless (physical-recovery-control-valid-p control :legacy legacy)
        (error "corrupt or unsupported physical recovery control"))
      control)))

(defun physical-recovery-read-control (directory)
  (physical-recovery-decode-control
   (recovery-read-bytes (physical-recovery-control-path directory) :maximum 196)))

(defvar *physical-recovery-control-test-hook* nil
  "Optional deterministic hook for recovery control publication failures.")

(defun physical-recovery-control-test-point (point state)
  (when *physical-recovery-control-test-hook*
    (funcall *physical-recovery-control-test-hook* point state)))

(defun physical-recovery-persist-control-bytes (bytes db-state)
  "Sync and atomically publish encoded bytes; the caller installs runtime state."
  (let ((path (physical-recovery-control-path)))
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
      (write-sequence bytes stream))))

(defun physical-recovery-control-frontier (state applied)
  "Check current and required facts at their respective replay positions.
Before required replay is reached, preserve its durable fact and position.
A known required fact must match at the boundary; later facts may have lower
resource values, but their reporting position cannot move backward."
  (let* ((base (getf state :timeline))
         (runtime (getf state :replay-timeline base))
         (required (getf state :required-applied))
         (required-timeline (getf state :required-timeline base))
         (source (getf state :source-resource-fact))
         (minimum (getf state :required-resource-fact)))
    (unless (or (null source)
                (and (wal-resource-fact-valid-p source)
                     (<= (getf source :resource-lsn) applied)
                     (<= (getf source :resource-timeline) runtime)))
      (error "resource fact is outside the current replay frontier"))
    (unless (or (null minimum)
                (and required (wal-resource-fact-valid-p minimum)
                     (<= (getf minimum :resource-lsn) required)
                     (<= (getf minimum :resource-timeline) required-timeline)))
      (error "resource fact is outside the required recovery frontier"))
    (when (and required (= applied required) (/= runtime required-timeline))
      (error "replay timeline differs at the required applied boundary"))
    (when (and required (>= applied required) minimum)
      (unless (and source
                   (>= (getf source :resource-lsn) (getf minimum :resource-lsn))
                   (or (> (getf source :resource-lsn) (getf minimum :resource-lsn))
                       (equal source minimum))
                   (or (> applied required) (equal source minimum)))
        (error "reconstructed resources differ at the required recovery frontier")))
    (if (and required (< applied required))
        (values required required-timeline (wal-copy-control-resource-fields minimum))
        (values applied runtime (wal-copy-control-resource-fields source)))))

(defun physical-recovery-write-control (state phase applied next-xid
                                        &key (db-state +db-in-archive-recovery+))
  "Publish ordinary progress; pending child state needs its dedicated publisher."
  (when *physical-recovery-failed-p*
    (error "failed recovery cannot publish ordinary progress"))
  (when (or (eql phase 3) (eql (getf state :phase) 3) (getf state :promotion-child)
            (eql 3 (getf *physical-recovery-state* :phase)))
    (error "pending native promotion requires a dedicated control publisher"))
  (when (and (= db-state +db-shutdowned-in-recovery+)
             (< applied (or (getf state :required-applied) applied)))
    (error "clean recovery has not reached its previously applied boundary"))
  (multiple-value-bind (required required-timeline resource-fact) (physical-recovery-control-frontier state applied)
    (let* ((state (copy-list state))
           (next-xid (max next-xid (or (getf state :observed-next-xid) next-xid)))
           (time (- (get-universal-time) 2208988800))
           (bytes (physical-recovery-control-image
                   phase (getf state :system-id) (getf state :timeline)
                   (getf state :manifest-digest) (getf state :checkpoint) required next-xid
                   :applied-timeline required-timeline :resource-fact resource-fact
                   :history-target (getf state :history-target) :history-digest (getf state :history-digest)
                   :db-state db-state :state-time time)))
      (when (getf state :source-resource-fact)
        (setf (getf state :source-resource-fact)
              (wal-copy-control-resource-fields (getf state :source-resource-fact))))
      (if resource-fact
          (setf (getf state :required-resource-fact) (wal-copy-control-resource-fields resource-fact))
          (remf state :required-resource-fact))
      (physical-recovery-persist-control-bytes bytes db-state)
      (setf (getf state :phase) phase
            (getf state :db-state) db-state
            (getf state :state-time) time
            (getf state :control-version) 3
            (getf state :required-applied) required
            (getf state :required-timeline) required-timeline
            (getf state :observed-next-xid) next-xid)
      (physical-recovery-control-test-point :after-write db-state)
      state)))

(defun physical-recovery-mark-target (directory)
  "Persist incomplete recovery role before any copied database files are created."
  (when (probe-file (physical-recovery-control-path directory))
    (error "physical recovery control already exists"))
  (wal-atomic-write-bytes
   (physical-recovery-control-path directory)
   (physical-recovery-control-image 0 (catalog-system-identifier) *wal-insert-timeline* 0
                                    *wal-checkpoint-lsn* *wal-durable-lsn* *next-xid*
                                    :resource-fact (wal-copy-control-resource-fields *wal-checkpoint-snapshot*))))

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

(defun recovery-history-file-timeline (name)
  "Return the timeline from a canonical child history path, or NIL if it is invalid."
  (when (and (stringp name) (= (length name) 23)
             (uiop:string-prefix-p "pg_wal/" name)
             (string= name ".history" :start1 15)
             (every (lambda (character) (find character "0123456789ABCDEF"))
                    (subseq name 7 15)))
    (let ((timeline (parse-integer name :start 7 :end 15 :radix 16)))
      (when (> timeline 1) timeline))))

(defun recovery-manifest-path-p (name &optional (timeline 1))
  "Check that NAME is a permitted base-image path for TIMELINE.
Child images may include history files no newer than their selected timeline."
  (and (stringp name)
       (or (member name '("PG_VERSION" "pg_xact" "global/pg_control"
                          "global/pg_catalog.dat" "global/pg_wal_control") :test #'equal)
           (and (uiop:string-prefix-p "base/" name)
                (let* ((suffix (subseq name 5))
                       (digits (if (uiop:string-suffix-p suffix ".vm")
                                   (subseq suffix 0 (- (length suffix) 3)) suffix)))
                  (and (plusp (length digits)) (every #'digit-char-p digits))))
           (and (> timeline 1) (recovery-history-file-timeline name)
                (<= (recovery-history-file-timeline name) timeline))
           (and (uiop:string-prefix-p "pg_wal/" name)
                (uiop:string-suffix-p name ".wal")
                (let ((stem (subseq name 7 (- (length name) 4))))
                  (or (and (= timeline 1) (eql 1 (wal-parse-segment-name stem :legacy)))
                      (eql timeline (wal-parse-segment-name stem :timeline))))))))

(defun physical-manifest-checkpoint-start (manifest)
  (if (member (getf manifest :version) '(2 3)) (getf manifest :checkpoint-start)
      (- (getf manifest :checkpoint) +wal-header-size+)))

(defun recovery-check-manifest-schema (manifest)
  (unless (and (listp manifest) (member (getf manifest :version) '(1 2 3))
               (equal (loop for (key value) on manifest by #'cddr collect key)
                      (if (member (getf manifest :version) '(2 3))
                          '(:version :system-id :cluster-identity :timeline :checkpoint
                            :checkpoint-start :end :next-xid :source-pin :files)
                          '(:version :system-id :cluster-identity :timeline :checkpoint
                            :end :next-xid :source-pin :files))) (typep (getf manifest :timeline) '(integer 1 4294967295))
               (or (eql 3 (getf manifest :version)) (eql 1 (getf manifest :timeline)))
               (typep (getf manifest :system-id) '(unsigned-byte 64))
               (stringp (getf manifest :cluster-identity))
               (plusp (length (getf manifest :cluster-identity)))
               (typep (getf manifest :checkpoint) '(integer 32 18446744073709551615))
               (= (getf manifest :checkpoint) (getf manifest :end))
               (typep (physical-manifest-checkpoint-start manifest) '(unsigned-byte 64))
               (wal-checkpoint-record-length-p
                (- (getf manifest :checkpoint) (physical-manifest-checkpoint-start manifest)))
               (typep (getf manifest :next-xid)
                      (if (eql 3 (getf manifest :version))
                          '(integer 3 18446744073709551616)
                          '(integer 3 18446744073709551615)))
               (wal-valid-replication-slot-name-p (getf manifest :source-pin))
               (listp (getf manifest :files)) (getf manifest :files))
    (error "unsupported or malformed physical recovery manifest"))
  (let ((names (make-hash-table :test #'equal)))
    (dolist (entry (getf manifest :files))
      (unless (and (listp entry) (= (length entry) 3)
                   (recovery-manifest-path-p (first entry) (getf manifest :timeline))
                   (typep (second entry) '(unsigned-byte 64))
                   (typep (third entry) '(unsigned-byte 64))
                   (not (gethash (first entry) names)))
        (error "invalid or duplicate physical recovery file entry"))
      (setf (gethash (first entry) names) t)))
  (when (and (> (getf manifest :timeline) 1)
             (not (find (concatenate 'string "pg_wal/" (timeline-history-filename (getf manifest :timeline)))
                        (getf manifest :files) :key #'first :test #'equal)))
    (error "child physical recovery manifest lacks its timeline history"))
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

(defun physical-recovery-check-checkpoint-resources (snapshot metadata)
  "Require checkpoint and coherent control resource facts to agree.
For known facts, check the claimed origin timeline against selected history."
  (unless (and (wal-control-resource-fields-valid-p snapshot)
               (wal-control-resource-fields-valid-p metadata)
               (equal (wal-copy-control-resource-fields snapshot)
                      (wal-copy-control-resource-fields metadata)))
    (error "base checkpoint resource facts differ from copied control"))
  (if (getf snapshot :resource-parameters)
      (timeline-check-resource-origin snapshot (timeline-read-history (getf snapshot :timeline)))
      (wal-copy-control-resource-fields snapshot)))

(defun recovery-check-image-metadata (manifest directory)
  "Bind catalog identity, exact file inventory, WAL checkpoint and XID reservation."
  (let* ((*pgdata* directory)
         (*catalog-identity-needs-persist-p* nil)
         (*catalog* (load-catalog))
         (*wal-checkpoint-lsn* (getf manifest :checkpoint))
         (*wal-durable-lsn* (getf manifest :end))
         (control-fields (multiple-value-list (wal-read-control-fields)))
         (*wal-checkpoint-snapshot* (third control-fields))
         (*wal-insert-timeline* (fourth control-fields))
         (*wal-checkpoint-timeline* (fifth control-fields))
         (*wal-segment-namespace* (sixth control-fields))
         (names (sort (mapcar #'first (getf manifest :files)) #'string<)))
    (unless (and (= *wal-insert-timeline* *wal-checkpoint-timeline* (getf manifest :timeline))
                 (or (= *wal-insert-timeline* 1) (eq *wal-segment-namespace* :timeline))
                 (not *catalog-identity-needs-persist-p*)
                 (equal (catalog-cluster-identity *catalog*) (getf manifest :cluster-identity))
                 (= (catalog-system-identifier) (getf manifest :system-id))
                 (gethash "postgres" (catalog-databases *catalog*))
                 (equal names (physical-bootstrap-inventory)))
      (error "physical recovery catalog identity or inventory mismatch"))
    (physical-recovery-check-checkpoint-resources *wal-checkpoint-snapshot* (seventh control-fields))
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
                               :system-id (getf manifest :system-id) :timeline (getf manifest :timeline)
                               :consistent-lsn *wal-checkpoint-lsn*
                               :restart-lsn (physical-manifest-checkpoint-start manifest)))
    *catalog*))

(declaim (ftype function physical-recovery-read-signal-mode))

(defun physical-recovery-prepare (&key (signal-mode (physical-recovery-read-signal-mode)) (timeline-goal :latest))
  "Select the base WAL namespace while verifying immutable provenance.
A nonpending standby cannot fall back to primary startup when validation fails."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace)
      (wal-read-control-fields)
    (declare (ignore checkpoint durable snapshot))
    (let ((*wal-segment-namespace* namespace)
          (*wal-insert-timeline* insertion)
          (*wal-checkpoint-timeline* checkpoint-timeline))
      (physical-recovery-prepare-with-namespace :signal-mode signal-mode :timeline-goal timeline-goal)
      (setf (getf *physical-recovery-state* :wal-namespace) namespace))))

(declaim (ftype function physical-follow-check-startup-config))

(defun physical-recovery-verify-base-with-namespace ()
  "Verify base provenance in the selected namespace without publishing control.
Return the reconstructed state and durable control; do not install runtime state.
The nested source checkpoint records base facts, not later applied resources;
the manifest does not add another authority for those facts."
  (physical-recovery-check-required-marker)
  (let ((control (physical-recovery-read-control *pgdata*)))
    ;; Pending control must never fall through the ordinary phase rewrite/redo path.
    ;; A later milestone will verify and reconcile this exact generation before SQL.
    (when (= 3 (getf control :phase))
      (error "pending native promotion requires reconciliation before startup"))
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
                      (= (getf control :next-xid)
                         (if (= 3 (getf control :control-version))
                             (getf manifest :next-xid) (load-next-xid))))
           (error "physical recovery control does not match its immutable base image"))))
      (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace metadata)
          (wal-read-control-fields)
        (declare (ignore namespace))
        (wal-require-supported-timelines insertion checkpoint-timeline)
        (unless (and (= insertion checkpoint-timeline (getf manifest :timeline))
                     (eql checkpoint (getf manifest :checkpoint))
                     (eql durable (getf manifest :end))
                     (= (physical-manifest-checkpoint-start manifest)
                        (if snapshot (getf snapshot :start) (- checkpoint +wal-header-size+))))
          (error "base checkpoint control differs from manifest"))
        (physical-recovery-check-checkpoint-resources snapshot metadata)
        (setf (getf manifest :source-checkpoint) snapshot))
      (when (getf (getf manifest :source-checkpoint) :resource-parameters)
        (setf (getf manifest :source-resource-fact)
              (wal-copy-control-resource-fields (getf manifest :source-checkpoint))
              (getf manifest :base-source-resource-fact)
              (wal-copy-control-resource-fields (getf manifest :source-checkpoint))))
      (when (getf control :resource-fact)
        (setf (getf manifest :required-resource-fact)
              (wal-copy-control-resource-fields (getf control :resource-fact))))
      (setf (getf manifest :source-reservation) (physical-recovery-initial-reservation manifest))
      (setf (getf manifest :manifest-digest) digest
            (getf manifest :phase) (if (= 2 (getf control :phase)) 2 1)
            (getf manifest :required-applied) (getf control :applied)
            (getf manifest :required-timeline) (getf control :applied-timeline (getf control :timeline))
            (getf manifest :replay-timeline) (getf manifest :timeline)
            (getf manifest :history-target) (getf control :history-target)
            (getf manifest :history-digest) (getf control :history-digest)
            (getf manifest :observed-next-xid)
            (if (= 3 (getf control :control-version))
                (getf control :next-xid) (getf manifest :next-xid))
            (getf manifest :previous-db-state) (getf control :db-state)
            (getf manifest :previous-state-time) (getf control :state-time))
      (values manifest control))))

(defun physical-recovery-verify-base-state ()
  "Read the WAL namespace and verify the base without changing recovery state."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace)
      (wal-read-control-fields)
    (declare (ignore checkpoint durable snapshot))
    (let ((*wal-segment-namespace* namespace)
          (*wal-insert-timeline* insertion)
          (*wal-checkpoint-timeline* checkpoint-timeline))
      (multiple-value-bind (state control) (physical-recovery-verify-base-with-namespace)
        (setf (getf state :wal-namespace) namespace)
        (values state control)))))

(declaim (ftype function physical-follow-startup-owner-check physical-follow-prepare-startup
                physical-follow-check-startup-admission))

(declaim (ftype function physical-recovery-prepare-timeline-policy))

(declaim (ftype function physical-follow-standard-management-p))

(defun physical-recovery-prepare-with-namespace (&key signal-mode (timeline-goal :latest))
  "Verify the base and reconcile saved follow state before ordinary startup.
Resolve policy before active or unbound admission, then re-resolve after pending
reconciliation and compare protected fields. Install the original policy object.
Publish dirty recovery control before installing state and allowing redo."
  (physical-recovery-check-required-marker)
  (physical-follow-startup-owner-check)
  (let ((manifest (physical-recovery-verify-base-with-namespace)))
    (when (eq signal-mode :archive)
      (error "archive recovery requested by recovery.signal is not implemented"))
    (let* ((managed (physical-follow-standard-management-p manifest signal-mode))
           (policy (physical-recovery-prepare-timeline-policy manifest timeline-goal))
           (config (physical-follow-prepare-startup manifest :policy policy)))
      (setf manifest (physical-recovery-verify-base-with-namespace))
      (let ((checked (physical-recovery-prepare-timeline-policy manifest timeline-goal)))
        (unless (every (lambda (key) (equalp (getf policy key) (getf checked key)))
                       '(:target :startup-anchor :original-required :original-observed-next-xid
                         :startup-seed :history))
          (error "follow startup reconciliation changed the recovery timeline policy")))
      (setf (getf manifest :follow-startup-config) config
            (getf manifest :standard-follow-managed-p) managed
            (getf manifest :timeline-policy) policy)
      (setf *physical-recovery-state*
            (physical-recovery-write-control
             manifest (getf manifest :phase) (getf manifest :end)
             (getf manifest :observed-next-xid))))))

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

(declaim (ftype function physical-recovery-attach-selected-store physical-recovery-check-required-pair))

(declaim (ftype function physical-recovery-publish-selected-history))

(defun physical-recovery-open-storage ()
  "Open verified recovery after follow startup admission. In phase 2, attach the
receive store and redo its retained prefix before loading slots and serving SQL.
A saved binding must have been reconciled and admitted before this point."
  (unless (recovery-in-progress-p) (error "physical recovery image was not verified"))
  (physical-follow-check-startup-admission *physical-recovery-state*)
  (let ((streaming (= 2 (getf *physical-recovery-state* :phase))))
    (setf (getf *physical-recovery-state* :replay-timeline) (getf *physical-recovery-state* :timeline)
          *wal-segment-namespace* (getf *physical-recovery-state* :wal-namespace :legacy)
          *wal-insert-timeline* (getf *physical-recovery-state* :timeline)
          *wal-checkpoint-timeline* (getf *physical-recovery-state* :timeline)
          *wal-checkpoint-snapshot* (copy-tree (getf *physical-recovery-state* :source-checkpoint))
          *wal-checkpoint-lsn* (getf *physical-recovery-state* :checkpoint)
          *wal-durable-lsn* (getf *physical-recovery-state* :end)
          *wal-end-lsn* *wal-durable-lsn*
          *last-replay-lsn* *wal-durable-lsn*
          *wal-ready* t
          *wal-poisoned* nil
          *next-xid* (getf *physical-recovery-state* :next-xid)
          *xid-batch-end* *next-xid*
          *active-xids* nil
          *catalog* (load-catalog))
    (when streaming
      (setf *physical-receive-store* (physical-recovery-open-receive-store))
      (unless (= (physical-store-base-lsn *physical-receive-store*)
                 (getf *physical-recovery-state* :end))
        (error "streaming receive base differs from verified backup"))
      (setf *physical-recovery-state*
            (physical-recovery-attach-selected-store *physical-recovery-state* *physical-receive-store*)
            (getf *physical-recovery-state* :receive-lsn)
            (physical-store-flushed-lsn *physical-receive-store*))
      (unless (<= (getf *physical-recovery-state* :required-applied)
                  (physical-store-flushed-lsn *physical-receive-store*))
        (error "streaming receive spool lost previously applied WAL"))
      (physical-recovery-publish-selected-history *physical-receive-store*)
      (physical-restartpoint-prepare-storage *physical-receive-store*)
      (setf *clog-stream* (open (clog-path) :element-type '(unsigned-byte 8)
                                            :direction :io :if-exists :overwrite
                                            :if-does-not-exist :error))
      (let ((*physical-replay-restarting-p* t))
        (physical-replay-from *last-replay-lsn*))
      (physical-recovery-check-required-pair *physical-receive-store*)
      (unless (<= (getf *physical-recovery-state* :observed-next-xid) *next-xid*)
        (error "streaming redo did not restore its observed XID visibility floor"))
      (physical-recovery-check-live-files)
      (physical-recovery-save-progress *last-replay-lsn* *next-xid*))
    (unless streaming
      (setf *clog-stream* (open (clog-path) :element-type '(unsigned-byte 8)
                                            :direction :input :if-does-not-exist :error)))
    (wal-load-replication-slots)
    (catalog-refresh-oldest-relfrozenxid))
  (values))

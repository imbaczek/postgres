;;;; C-compatible history text and read-only replication history access.
;;;; These helpers do not grant write authority or perform promotion.
(in-package #:pg)

(defstruct timeline-history-entry timeline begin end)

(sb-alien:define-alien-routine ("sscanf" timeline-scanf-history-line) sb-alien:int
  (line sb-alien:c-string) (format sb-alien:c-string)
  (timeline (* sb-alien:unsigned-int)) (high (* sb-alien:unsigned-int))
  (low (* sb-alien:unsigned-int)))

(defun timeline-history-fields (line)
  "Use the same scanf conversion and low-word width as timeline.c."
  (sb-alien:with-alien ((timeline sb-alien:unsigned-int 0)
                        (high sb-alien:unsigned-int 0) (low sb-alien:unsigned-int 0))
    (unless (= 3 (timeline-scanf-history-line
                  line (format nil "%u~c%X/%08X" #\Tab)
                  (sb-alien:addr timeline) (sb-alien:addr high) (sb-alien:addr low)))
      (error "syntax error in timeline history: ~s" line))
    (values timeline (logior (ash high 32) low))))

(defun timeline-parse-history (child text)
  "readTimeLineHistory's newest-first intervals; reasons are deliberately ignored."
  (unless (typep child '(integer 1 4294967295)) (error "invalid child timeline"))
  (let ((entries nil) (last-timeline 0) (previous-end 0))
    (with-input-from-string (stream text)
      (loop for line = (read-line stream nil nil) while line
            for start = (position-if-not #'sql-array-space-p line)
            when (and start (not (char= (char line start) #\#)))
              do (when (> (length (string->bytes line)) 1023)
                   (error "timeline history line exceeds MAXPGPATH"))
                 (multiple-value-bind (timeline switchpoint) (timeline-history-fields line)
                   (when (and entries (<= timeline last-timeline))
                     (error "timeline identifiers must increase in history"))
                   (push (make-timeline-history-entry :timeline timeline
                                                      :begin previous-end :end switchpoint)
                         entries)
                   (setf last-timeline timeline previous-end switchpoint))))
    (when (and entries (<= child last-timeline))
      (error "history ancestor timeline must precede child timeline"))
    (cons (make-timeline-history-entry :timeline child :begin previous-end :end nil) entries)))

(defun timeline-check-lineage (history)
  "Require forward native ancestry before using parsed C history for a safety check."
  (unless history (error "empty timeline lineage"))
  (let ((previous-end 0) (previous-timeline 0))
    (dolist (entry (reverse history))
      (let ((timeline (timeline-history-entry-timeline entry))
            (begin (timeline-history-entry-begin entry))
            (end (timeline-history-entry-end entry)))
        (unless (and (typep timeline '(integer 1 4294967295))
                     (> timeline previous-timeline)
                     (typep begin '(unsigned-byte 64)) (= begin previous-end)
                     (if end (and (typep end '(unsigned-byte 64)) (<= begin end))
                         (eq entry (first history))))
          (error "invalid forward timeline lineage"))
        (setf previous-timeline timeline previous-end end))))
  history)

(defun timeline-at-point (point history)
  "Follow tliOfPointInHistory: a switchpoint belongs to the child, not its parent."
  (unless (typep point '(unsigned-byte 64)) (error "invalid WAL point"))
  (timeline-check-lineage history)
  (let ((entry (find-if (lambda (entry)
                          (and (<= (timeline-history-entry-begin entry) point)
                               (or (null (timeline-history-entry-end entry))
                                   (< point (timeline-history-entry-end entry)))))
                        history)))
    (unless entry (error "WAL point is outside timeline history"))
    (timeline-history-entry-timeline entry)))

(defun timeline-check-resource-origin (facts history)
  (unless (wal-control-resource-fields-valid-p facts)
    (error "invalid resource origin facts"))
  (timeline-check-lineage history)
  (when (getf facts :resource-parameters)
    (unless (eql (getf facts :resource-timeline)
                 (timeline-at-point (1- (getf facts :resource-lsn)) history))
      (error "resource origin is outside its timeline history")))
  (wal-copy-control-resource-fields facts))

(defun timeline-segment-candidates (segment history &optional (previous-file-timeline 0))
  "Return C XLogFileReadAnyTLI's newest-first file candidates, not record owners.
A descendant file can contain an ancestor's copied prefix. The caller must
validate record identity separately, and retain the successful file TLI as its
monotonic floor. This function grants no recovery or insertion authority."
  (unless (typep segment '(integer 0 1099511627775))
    (error "invalid WAL segment number"))
  (unless (typep previous-file-timeline '(unsigned-byte 32))
    (error "invalid previous WAL file timeline"))
  (timeline-check-lineage history)
  (unless (or (zerop previous-file-timeline)
              (find previous-file-timeline history
                    :key #'timeline-history-entry-timeline))
    (error "previous WAL file timeline is outside selected history"))
  ;; xlogrecovery.c:4331-4361 deliberately checks only the begin segment.
  (loop for entry in history
        for timeline = (timeline-history-entry-timeline entry)
        while (>= timeline previous-file-timeline)
        when (>= segment (floor (timeline-history-entry-begin entry)
                                +wal-segment-size+))
          collect timeline))

(defun timeline-prefix-boundary-p (timeline boundary history)
  "Test ancestry position for a prefix ending at BOUNDARY, not equality of WAL bytes.
The parent's end is allowed here: a cursor at the fork has consumed no child WAL.
System identity and exact prefix digest must still be verified by the caller."
  (unless (typep boundary '(unsigned-byte 64)) (error "invalid WAL boundary"))
  (timeline-check-lineage history)
  (let ((entry (find timeline history :key #'timeline-history-entry-timeline)))
    (and entry (<= (timeline-history-entry-begin entry) boundary)
         (or (null (timeline-history-entry-end entry))
             (<= boundary (timeline-history-entry-end entry))))))

(defun timeline-follow-candidate-p (timeline applied received history)
  "Check whether a descendant history contains both standby prefix frontiers.
APPLIED and RECEIVED belong to TIMELINE and must be ordered. RECEIVED must
describe durable receipt. A frontier at the fork is allowed. Invalid inputs
or lineage raise an error. The caller must still verify record boundaries,
system identity, exact prefix and retention; this grants no stream or role authority."
  (unless (and (typep timeline '(integer 1 4294967295))
               (typep applied '(unsigned-byte 64))
               (typep received '(unsigned-byte 64))
               (<= applied received))
    (error "invalid standby timeline frontiers"))
  (timeline-check-lineage history)
  (and (> (timeline-history-entry-timeline (first history)) timeline)
       (timeline-prefix-boundary-p timeline applied history)
       (timeline-prefix-boundary-p timeline received history)))

(defun timeline-check-end-of-recovery (record replay-timeline history
                                       &key (min-recovery-point 0) (min-recovery-timeline 0))
  "Check C's timeline switch before redo; return the child without changing state.
The caller separately verifies cluster/prefix provenance and publishes replay TLI
before applying EOR. This check grants neither recovery completion nor writer admission."
  (unless (and (typep replay-timeline '(integer 1 4294967295))
               (typep min-recovery-point '(unsigned-byte 64))
               (typep min-recovery-timeline '(unsigned-byte 32))
               (or (zerop min-recovery-point) (plusp min-recovery-timeline)))
    (error "invalid end-of-recovery replay context"))
  (timeline-check-lineage history)
  (let* ((info (wal-end-of-recovery-record-info record))
         (child (getf info :timeline)) (previous (getf info :prev-timeline))
         (end (wal-record-end record)))
    (unless (and (= child (timeline-at-point (wal-record-start record) history))
                 (timeline-prefix-boundary-p child end history))
      (error "end-of-recovery record is outside expected timeline history"))
    ;; ApplyWalRecord calls checkTimeLineSwitch only when the TLI changes.
    (unless (= child replay-timeline)
      (unless (and (= previous replay-timeline) (>= child replay-timeline))
        (error "end-of-recovery record disagrees with current replay timeline"))
      (when (and (plusp min-recovery-point) (< end min-recovery-point)
                 (> child min-recovery-timeline))
        (error "timeline switch precedes required minimum recovery point")))
    child))

(defun timeline-history-filename (timeline)
  (unless (typep timeline '(integer 1 4294967295)) (error "invalid timeline identifier"))
  (format nil "~8,'0X.history" timeline))

(defun timeline-history-path (timeline directory)
  (merge-pathnames (concatenate 'string "pg_wal/" (timeline-history-filename timeline)) directory))

(defun timeline-encode-history (child parent switchpoint reason parent-bytes)
  "Copy parent bytes and append C writeTimeLineHistory's exact split-line layout.
Native ancestry requires the parent history and a forward fork; no role is changed."
  (unless (and (typep child '(integer 1 4294967295))
               (typep parent '(integer 1 4294967295)) (> child parent)
               (typep switchpoint '(unsigned-byte 64))
               (stringp reason)
               (notany (lambda (char) (member (char-code char) '(0 10 13))) reason)
               (if (= parent 1) (null parent-bytes)
                   (typep parent-bytes '(vector (unsigned-byte 8)))))
    (error "invalid native timeline split"))
  (let* ((history (timeline-check-lineage
                   (timeline-parse-history parent (if parent-bytes (bytes->string parent-bytes) ""))))
         (line (string->bytes
                (format nil "~d~c~X/~8,'0X~c~a~%" parent #\Tab
                        (ldb (byte 32 32) switchpoint) (ldb (byte 32 0) switchpoint)
                        #\Tab reason))))
    (unless (and (<= (timeline-history-entry-begin (first history)) switchpoint)
                 (<= (1- (length line)) 1023))
      (error "backward or oversized native timeline split"))
    (let ((bytes (concatenate '(vector (unsigned-byte 8))
                              (or parent-bytes #())
                              (if parent-bytes #(10) #()) line)))
      (unless (<= (length bytes) (* 16 1024 1024))
        (error "timeline history exceeds readable size"))
      bytes)))

(defvar *timeline-history-lock* (sb-thread:make-mutex :name "timeline history publication"))

(defvar *timeline-history-test-hook* nil)

(defun timeline-history-test-point (point)
  (when *timeline-history-test-hook* (funcall *timeline-history-test-hook* point)))

(defun timeline-history-bytes (timeline &optional (directory *pgdata*))
  "Read an immutable history file without hanging on special files or following growth."
  (let* ((path (timeline-history-path timeline directory))
         (fd (handler-case
                 (sb-posix:open (namestring path)
                                (logior sb-posix:o-rdonly sb-posix:o-nonblock +postmaster-o-cloexec+))
               (sb-posix:syscall-error (condition)
                 (ereport (if (= (sb-posix:syscall-errno condition) sb-posix:enoent) "58P01" "58030")
                          "could not open timeline history file ~s" (namestring path)))))
         (stream nil))
    (unwind-protect
         (let* ((stat (sb-posix:fstat fd)) (length (sb-posix:stat-size stat)))
           (unless (and (= sb-posix:s-ifreg (logand (sb-posix:stat-mode stat) sb-posix:s-ifmt))
                        (<= 0 length (* 16 1024 1024)))
             (ereport "58030" "invalid or oversized timeline history file"))
           (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8)))
           (let ((bytes (make-array length :element-type '(unsigned-byte 8))))
             (unless (= length (read-sequence bytes stream))
               (ereport "XX001" "short timeline history read"))
             bytes))
      (if stream (close stream) (sb-posix:close fd)))))

(defun timeline-read-history (timeline &optional (directory *pgdata*))
  (if (= timeline 1) (timeline-parse-history 1 "")
      (timeline-parse-history timeline (bytes->string (timeline-history-bytes timeline directory)))))

(defun timeline-publish-history-bytes (path bytes)
  "Exclusively install a synced inode; never replace an existing pathname."
  (let ((directory (uiop:pathname-directory-pathname path)))
    (wal-ensure-directory-durable directory)
    (multiple-value-bind (fd temporary)
        (sb-posix:mkstemp (namestring (merge-pathnames ".history-XXXXXX" directory)))
      (unwind-protect
           (progn
             (let ((stream (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8))))
               (setf fd nil)
               (unwind-protect
                    (progn
                      (write-sequence bytes stream)
                      (finish-output stream)
                      (timeline-history-test-point :before-file-fsync)
                      (fsync-stream stream)
                      (timeline-history-test-point :after-file-fsync)
                      (sb-posix:link temporary (namestring path))
                      (timeline-history-test-point :after-install)
                      ;; durable_rename fsyncs the installed inode, then its directory.
                      (fsync-stream stream))
                 (close stream)))
             (sb-posix:unlink temporary)
             (fsync-directory directory)
             (timeline-history-test-point :after-directory-fsync)
             path)
        (when fd (sb-posix:close fd))
        (ignore-errors (sb-posix:unlink temporary))))))

(defun timeline-write-history (child parent switchpoint reason &optional (directory *pgdata*))
  "Durably publish immutable history without changing runtime timeline or role.
The caller's future recovery transition must handle publication errors fail-closed."
  (sb-thread:with-mutex (*timeline-history-lock*)
    (let* ((parent-bytes (unless (= parent 1) (timeline-history-bytes parent directory)))
           (bytes (timeline-encode-history child parent switchpoint reason parent-bytes)))
      (timeline-publish-history-bytes (timeline-history-path child directory) bytes))))

(defun send-timeline-history (stream timeline)
  "Match SendTimeLineHistory's text columns and exact file bytes."
  (send-row-description
   stream (list (make-result-column :name "filename" :type-oid +textoid+ :typlen -1)
                (make-result-column :name "content" :type-oid +textoid+ :typlen -1)))
  (let* ((filename (string->bytes (timeline-history-filename timeline)))
         (content (timeline-history-bytes timeline)))
    (with-backend-message (stream #\D payload)
      (payload-push-u16 payload 2)
      (payload-push-u32 payload (length filename))
      (payload-push payload filename)
      (payload-push-u32 payload (length content))
      (payload-push payload content)))
  (send-command-complete stream "TIMELINE_HISTORY")
  (send-ready-for-query stream (transaction-status-char)))

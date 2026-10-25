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

(defun timeline-history-filename (timeline)
  (unless (typep timeline '(integer 1 4294967295)) (error "invalid timeline identifier"))
  (format nil "~8,'0X.history" timeline))

(defun timeline-history-bytes (timeline &optional (directory *pgdata*))
  "Read an immutable history file without hanging on special files or following growth."
  (let* ((path (merge-pathnames (concatenate 'string "pg_wal/" (timeline-history-filename timeline))
                                directory))
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

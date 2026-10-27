;;;; Durable native physical WAL receive spool, independently owned from live storage.
(in-package #:pg)

(sb-alien:define-alien-routine ("flock" physical-store-flock) sb-alien:int
  (fd sb-alien:int) (operation sb-alien:int))

(defstruct (physical-store (:constructor %make-physical-store))
  "A locally owned receive spool and its durable progress.

SYSTEM-ID, TIMELINE and BASE-LSN identify the original base. SELECTION changes
the receive history without changing that base or main WAL control."
  directory system-id timeline base-lsn floor-lsn (floor-digest #xCBF29CE484222325)
  written-lsn flushed-lsn decoder
  (written-digest #xCBF29CE484222325) (flushed-digest #xCBF29CE484222325)
  lock-stream stream segment segment-timeline selection retained-identity retained-root
  (failed-p nil) (closed-p nil)
  (mutex (sb-thread:make-mutex :name "physical WAL receive store")))

(declaim (ftype function physical-store-load-history-selection physical-store-validate-history-prefix
                physical-store-history-discard-tail physical-store-history-segment-entries
                physical-store-history-check-record physical-store-check-retained-root
                physical-store-check-retained-identity))

(defvar *physical-store-test-hook* nil
  "Disabled-by-default hook for deterministic receive durability failures.")

(defun physical-store-path (store name)
  (merge-pathnames name (physical-store-directory store)))

(defun physical-store-segment-path (store segment &optional (timeline (physical-store-timeline store)))
  "Return the receive segment path, keeping base and child timeline names distinct."
  (physical-store-path store
                       (if (= timeline (physical-store-timeline store))
                           (format nil "~16,'0X.wal" segment)
                           (format nil "~8,'0X-~16,'0X.wal" timeline segment))))

(defun physical-store-check-live (store)
  (when (or (physical-store-closed-p store) (physical-store-failed-p store))
    (error "physical receive store is closed or failed; reopen at durable progress")))

(defmacro with-physical-store ((store) &body body)
  "Serialize one receiver operation and latch any failure before releasing ownership."
  (let ((owner (gensym "STORE")))
    `(let ((,owner ,store))
       (sb-thread:with-mutex ((physical-store-mutex ,owner))
         (physical-store-check-live ,owner)
         (handler-case (progn ,@body)
           (error (condition)
             (setf (physical-store-failed-p ,owner) t)
             (error condition)))))))

(defun physical-store-test-point (point store)
  (when *physical-store-test-hook*
    (funcall *physical-store-test-hook* point store)))

(defun physical-store-update-digest (digest bytes)
  "Checksum the exact ordered prefix, including each record's embedded coordinates.
Per-record CRCs alone cannot detect a missing whole record before a rotation.
This FNV-1a-64 checksum detects accidental prefix damage, not malicious tampering."
  (loop for octet across bytes
        do (setf digest (logand #xffffffffffffffff
                                (* (logxor digest octet) #x100000001B3))))
  digest)

(defun physical-store-encode-control (store flush-lsn &key
                                                        (digest (physical-store-written-digest store))
                                                        (floor-lsn (physical-store-floor-lsn store))
                                                        (floor-digest (physical-store-floor-digest store))
                                                        (selection (physical-store-selection store))
                                                        (retained-root (physical-store-retained-root store)))
  (let* ((size (cond (retained-root 96) (selection 80) (t 68)))
         (bytes (make-array size :element-type '(unsigned-byte 8) :initial-element 0)))
    (when retained-root
      (unless (typep retained-root '(unsigned-byte 64))
        (error "invalid retained receive inventory ID"))
      (physical-store-check-retained-identity store))
    (replace bytes #(80 71 80 72 89 83 48 51))
    (wal-set-u64 bytes 8 (physical-store-system-id store))
    (wal-set-u32 bytes 16 (physical-store-timeline store))
    (wal-set-u64 bytes 24 (physical-store-base-lsn store))
    (wal-set-u64 bytes 32 flush-lsn)
    (wal-set-u64 bytes 40 digest)
    (wal-set-u64 bytes 48 floor-lsn)
    (wal-set-u64 bytes 56 floor-digest)
    (when selection
      (wal-set-u32 bytes 64 (getf selection :target))
      (wal-set-u64 bytes 68 (getf selection :digest)))
    (when retained-root
      (wal-set-u64 bytes 76 retained-root)
      (wal-set-u64 bytes 84 (getf (physical-store-retained-identity store) :manifest-digest)))
    (wal-set-u32 bytes (- size 4) (wal-crc32 bytes))
    bytes))

(defun physical-store-save-control (store flush-lsn &rest control-options)
  (physical-store-test-point :before-control-write store)
  (let ((path (physical-store-path store "receive.control")))
    (with-atomic-output
        (stream path (physical-store-path store "receive.tmp")
                (physical-store-directory store)
                :element-type '(unsigned-byte 8)
                :fsync-function
                (lambda (fd)
                  (physical-store-test-point :before-control-fsync store)
                  (sb-posix:fsync fd)
                  (physical-store-test-point :after-control-fsync store))
                :rename-function
                (lambda (from to)
                  (physical-store-test-point :before-control-rename store)
                  (sb-posix:rename from to)
                  (physical-store-test-point :after-control-rename store))
                :fsync-directory-function
                (lambda (directory)
                  (physical-store-test-point :before-control-directory-fsync store)
                  (fsync-directory directory)
                  (physical-store-test-point :after-control-directory-fsync store)))
      (write-sequence (apply #'physical-store-encode-control store flush-lsn control-options) stream)))
  (physical-store-test-point :after-control-write store))

(defun physical-store-load-control (store)
  (let* ((bytes (read-bounded-regular-file (physical-store-path store "receive.control") 96))
         (size (length bytes)))
    (unless (member size '(52 68 80 96))
      (error "invalid physical receive control length"))
    (let* ((v3 (>= size 68))
           (crc-offset (- size 4)) (crc (wal-u32 bytes crc-offset))
           (base (wal-u64 bytes 24)) (flush (wal-u64 bytes 32))
           (floor (if v3 (wal-u64 bytes 48) base))
           (seed (if v3 (wal-u64 bytes 56) #xCBF29CE484222325))
           (history-p (or (= size 80) (and (= size 96) (plusp (wal-u32 bytes 64))))))
      (wal-set-u32 bytes crc-offset 0)
      (unless (and (equalp (subseq bytes 0 8)
                           (if v3 #(80 71 80 72 89 83 48 51) #(80 71 80 72 89 83 48 50)))
                   (zerop (wal-u32 bytes 20)) (= crc (wal-crc32 bytes))
                   (= (wal-u64 bytes 8) (physical-store-system-id store))
                   (= (wal-u32 bytes 16) (physical-store-timeline store))
                   (<= base floor flush)
                   (or (/= base floor) (= seed #xCBF29CE484222325))
                   (or (/= size 96) history-p (zerop (wal-u64 bytes 68))))
        (error "corrupt or foreign physical receive control"))
      (setf (physical-store-base-lsn store) base
            (physical-store-floor-lsn store) floor
            (physical-store-floor-digest store) seed
            (physical-store-written-lsn store) flush
            (physical-store-flushed-lsn store) flush
            (physical-store-written-digest store) (wal-u64 bytes 40)
            (physical-store-flushed-digest store) (wal-u64 bytes 40)
            (physical-store-selection store)
            (when history-p (physical-store-load-history-selection store (wal-u32 bytes 64) (wal-u64 bytes 68)))
            (physical-store-retained-root store) (when (= size 96) (wal-u64 bytes 76)))
      (when (= size 96)
        (physical-store-check-retained-identity store)
        (unless (= (wal-u64 bytes 84) (getf (physical-store-retained-identity store) :manifest-digest))
          (error "receive inventory belongs to another immutable base"))))))

(defun physical-store-segments (store)
  "Return sorted segment numbers for the original base timeline.
The history inventory also includes child timelines and file paths."
  (sort (loop for (timeline segment path) in (physical-store-history-segment-entries store)
              when (= timeline (physical-store-timeline store)) collect segment)
        #'<))

(defun physical-store-validate-prefix (store &key on-record digest-cursor)
  "Validate the complete durable prefix before returning.

ON-RECORD is internal and runs during validation. The secondary value is the
cumulative digest at DIGEST-CURSOR, or NIL if it is not a base or record-end
boundary. A byte boundary alone does not establish a terminal batch."
  (when (physical-store-selection store)
    (return-from physical-store-validate-prefix
      (physical-store-validate-history-prefix store :on-record on-record :digest-cursor digest-cursor)))
  (let* ((base (physical-store-floor-lsn store))
         (flush (physical-store-flushed-lsn store))
         (decoder (make-physical-wal-decoder :start-lsn base))
         (digest (physical-store-floor-digest store))
         (cursor-digest (and (eql digest-cursor base) digest))
         (segments (physical-store-segments store))
         (first (floor base +wal-segment-size+))
         (last (and (< base flush) (floor (1- flush) +wal-segment-size+)))
         (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
    ;; A resume cursor near a segment end can rotate before writing any bytes
    ;; to its initial segment. The decoder checks that the first record really
    ;; cannot fit there; the prefix digest still detects omitted records.
    (when (and last (< first last) (plusp (mod base +wal-segment-size+))
               (not (member first segments)))
      (incf first))
    (when last
      (loop for segment from first to last
            do (unless (member segment segments)
                 (error "missing acknowledged physical WAL segment ~a" segment))
               (with-open-file (stream (physical-store-segment-path store segment)
                                       :element-type '(unsigned-byte 8))
                 (let* ((start (if (= segment (floor base +wal-segment-size+))
                                   (mod base +wal-segment-size+) 0))
                        (end (if (= segment last) (mod (1- flush) +wal-segment-size+)
                                 (1- (file-length stream))))
                        (limit (1+ end)))
                   (unless (<= start limit (file-length stream) +wal-segment-size+)
                     (error "truncated or oversized acknowledged physical WAL"))
                   (file-position stream start)
                   (loop for offset = start then (+ offset count)
                         while (< offset limit)
                         for count = (min (length buffer) (- limit offset))
                         do (unless (= (read-sequence buffer stream :end count) count)
                              (error "short acknowledged physical WAL read"))
                            (physical-wal-decode-input
                             decoder (+ (* segment +wal-segment-size+) offset)
                             (subseq buffer 0 count)
                             :on-record (lambda (record raw)
                                          (wal-require-ordinary-record-type (wal-record-type record))
                                          (setf digest
                                                (physical-store-update-digest digest raw))
                                          (when (eql digest-cursor (wal-record-end record))
                                            (setf cursor-digest digest))
                                          (when on-record
                                            (funcall on-record record raw)))))))))
    (unless (and (= (physical-wal-decoder-next-lsn decoder) flush)
                 (zerop (physical-wal-decoder-used decoder))
                 (= digest (physical-store-flushed-digest store)))
      (error "physical receive flush does not end at a complete WAL record"))
    (values nil cursor-digest)))

(defun physical-store-prefix-digest-under-lock (store cursor)
  "Derive the cumulative checksum at an exact durable boundary.
The caller owns the store mutex. Validate the entire acknowledged prefix,
including bytes after CURSOR, before returning an anchor for a restartpoint.
A record start after rotation padding is not an exclusive resume boundary."
  (unless (and (integerp cursor)
               (<= (physical-store-floor-lsn store) cursor
                   (physical-store-flushed-lsn store)))
    (error "physical replay cursor outside durable receive prefix"))
  (let ((digest (nth-value 1 (physical-store-validate-prefix
                              store :digest-cursor cursor))))
    (unless digest
      (error "physical replay cursor is not a record boundary"))
    digest))

(defun physical-store-prefix-digest (store cursor)
  "Return a validated cumulative checksum, never including unflushed input.
This does not certify a terminal batch or a durable data restartpoint."
  (with-physical-store (store)
    (physical-store-prefix-digest-under-lock store cursor)))

(defun physical-store-map-durable-records (store function &key
                                                            (start-lsn (physical-store-floor-lsn store)))
  "Call FUNCTION with each durable record and its raw octets after START-LSN.

Validate the whole prefix and exact resume boundary before the first call.
START-LSN must be the base or a record end, never a position in rotation padding.
Callbacks run under the store mutex and must not reenter receiver operations.
An error latches the store; earlier callbacks are not rolled back. Two bounded
scans avoid retaining the whole prefix in memory. The caller must establish any
required terminal batch boundary and replay authority, and own the files against
external modification."
  (with-physical-store (store)
    (let ((flush (physical-store-flushed-lsn store)))
      (physical-store-prefix-digest-under-lock store start-lsn)
      (physical-store-validate-prefix
       store :on-record (lambda (record raw)
                          (when (>= (wal-record-start record) start-lsn)
                            (funcall function record raw))))
      flush)))

(defun physical-store-discard-tail (store)
  "Discard only WAL outside the already-validated acknowledged prefix."
  (when (physical-store-selection store)
    (return-from physical-store-discard-tail (physical-store-history-discard-tail store)))
  (let* ((base (physical-store-floor-lsn store))
         (flush (physical-store-flushed-lsn store))
         (first (floor base +wal-segment-size+))
         (last (and (< base flush) (floor (1- flush) +wal-segment-size+))))
    (dolist (segment (physical-store-segments store))
      (let ((path (physical-store-segment-path store segment)))
        (cond
          ((or (null last) (< segment first) (> segment last))
           (delete-file path))
          ((= segment last)
           (with-open-file (stream path :direction :io :if-exists :overwrite
                                        :element-type '(unsigned-byte 8))
             (sb-posix:ftruncate (sb-sys:fd-stream-fd stream)
                                 (1+ (mod (1- flush) +wal-segment-size+)))
             (fsync-stream stream))))))
    (fsync-directory (physical-store-directory store))))

(defun physical-store-compact (store cursor publish-restartpoint)
  "Publish a durable data restartpoint before moving the receive floor.
PUBLISH-RESTARTPOINT runs under the store mutex with the validated cumulative
checksum at CURSOR. It must durably publish database state before returning.
Do not call from a mapper callback. Preserve unflushed writes and decoder state."
  (with-physical-store (store)
    (when (and (physical-store-retained-root store)
               (> cursor (physical-store-floor-lsn store)))
      (error "receive compaction requires a sealed replacement inventory"))
    (let ((digest (physical-store-prefix-digest-under-lock store cursor)))
      (funcall publish-restartpoint digest)
      (physical-store-test-point :after-restartpoint-publication store)
      (physical-store-save-control store (physical-store-flushed-lsn store)
                                   :digest (physical-store-flushed-digest store)
                                   :floor-lsn cursor :floor-digest digest)
      (setf (physical-store-floor-lsn store) cursor
            (physical-store-floor-digest store) digest)
      (physical-store-test-point :after-floor-publication store)
      (dolist (file (physical-store-history-segment-entries store))
        (when (< (second file) (floor cursor +wal-segment-size+))
          (delete-file (third file))))
      (physical-store-test-point :after-obsolete-segment-delete store)
      (fsync-directory (physical-store-directory store))
      cursor)))

(defun physical-store-close (store)
  "Release local ownership without converting unacknowledged writes into flush progress."
  (sb-thread:with-mutex ((physical-store-mutex store))
    (unless (physical-store-closed-p store)
      (setf (physical-store-closed-p store) t)
      (unwind-protect
           (when (physical-store-stream store)
             (close (physical-store-stream store) :abort t))
        (when (physical-store-lock-stream store)
          (close (physical-store-lock-stream store))))))
  (values))

(defun physical-store-open-lock (store create)
  "Keep a stable regular ownership inode; pending validation never creates it."
  (let ((fd (sb-posix:open (namestring (physical-store-path store "receive.lock"))
                           (logior sb-posix:o-rdwr sb-posix:o-nofollow +postmaster-o-cloexec+
                                   (if create sb-posix:o-creat 0))
                           #o600))
        (stream nil))
    (unwind-protect
         (progn
           (unless (= (logand (sb-posix:stat-mode (sb-posix:fstat fd)) sb-posix:s-ifmt)
                      sb-posix:s-ifreg)
             (error "physical receive ownership inode is not regular"))
           (setf stream (sb-sys:make-fd-stream fd :input t :output t))
           stream)
      (unless stream (sb-posix:close fd)))))

(defun physical-store-open (directory system-id timeline &key create (start-lsn 0) (discard-tail t) (create-lock t) retained-identity)
  "Own a native receive spool, creating only in an existing empty directory.
CREATE never overwrites an old incarnation. Opening an existing spool validates
its control, selected history and durable prefix before discarding tails.
Invalid selected metadata is an error, never a fallback to an older history.
The local file lock excludes another local receiver, not a distributed writer.
With DISCARD-TAIL and CREATE-LOCK both NIL, validate without changing spool bytes
or creating a missing lock. Do not resume input in that mode."
  (unless (and (typep system-id '(unsigned-byte 64))
               (typep timeline '(integer 1 4294967295))
               (typep start-lsn '(unsigned-byte 64))
               (uiop:directory-pathname-p directory)
               (uiop:directory-exists-p directory))
    (error "invalid physical receive store directory or identity"))
  (let ((store (%make-physical-store :directory directory :system-id system-id
                                     :timeline timeline :base-lsn start-lsn :floor-lsn start-lsn
                                     :written-lsn start-lsn :flushed-lsn start-lsn
                                     :retained-identity retained-identity))
        (success nil))
    (unwind-protect
         (progn
           (setf (physical-store-lock-stream store)
                 (physical-store-open-lock store create-lock))
           ;; POSIX flock LOCK_EX | LOCK_NB; per-open-description ownership also
           ;; excludes a second receiver in this Lisp process.
           (unless (zerop (physical-store-flock
                           (sb-sys:fd-stream-fd (physical-store-lock-stream store)) 6))
             (error "physical receive store already owned"))
           (if create
               (progn
                 (when (or (probe-file (physical-store-path store "receive.control"))
                           (physical-store-segments store)
                           (uiop:subdirectories directory)
                           (remove-if (lambda (path)
                                        (equal (file-namestring path) "receive.lock"))
                                      (uiop:directory-files directory)))
                   (error "physical receive store creation requires an empty directory"))
                 ;; Persist the spool directory entry in its existing parent.
                 (fsync-directory (uiop:pathname-parent-directory-pathname
                                   (truename directory)))
                 (physical-store-save-control store start-lsn))
               (progn
                 (physical-store-load-control store)
                 (physical-store-validate-prefix store)
                 (physical-store-check-retained-root store)
                 (when discard-tail (physical-store-discard-tail store))))
           (setf (physical-store-decoder store)
                 (make-physical-wal-decoder :start-lsn (physical-store-flushed-lsn store))
                 success t)
           store)
      (unless success (physical-store-close store)))))

(defun physical-store-open-segment (store segment &optional (timeline (physical-store-timeline store)))
  (unless (and (eql segment (physical-store-segment store))
               (eql timeline (physical-store-segment-timeline store)))
    (when (physical-store-stream store)
      ;; C walreceiver.c fsyncs a rotated segment before closing it. Persisting
      ;; bytes here alone never advances our acknowledged flush manifest.
      (fsync-stream (physical-store-stream store))
      (close (physical-store-stream store))
      (setf (physical-store-stream store) nil))
    (setf (physical-store-stream store)
          (open (physical-store-segment-path store segment timeline)
                :direction :io :element-type '(unsigned-byte 8)
                :if-exists :overwrite :if-does-not-exist :create)
          (physical-store-segment store) segment
          (physical-store-segment-timeline store) timeline))
  (physical-store-stream store))

(defun physical-store-write-record (store record bytes)
  "Write one validated record to its timeline segment and hash its raw bytes once."
  (let* ((timeline (physical-store-history-check-record
                    store record bytes (physical-store-written-lsn store)))
         (start (wal-record-start record))
         (stream (physical-store-open-segment store (floor start +wal-segment-size+) timeline)))
    (file-position stream (mod start +wal-segment-size+))
    (write-sequence bytes stream)
    (finish-output stream)
    (setf (physical-store-written-digest store)
          (physical-store-update-digest (physical-store-written-digest store) bytes)
          (physical-store-written-lsn store) (wal-record-end record))))

(defun physical-store-reset-unflushed (store)
  "Reset a healthy spool after network loss, preserving its authoritative flush.
Keep ownership and the object stable for concurrent replay. I/O/decoder failure
still requires process restart; reconnect alone never clears a failure latch."
  (with-physical-store (store)
    (physical-store-validate-prefix store)
    (when (physical-store-stream store)
      (close (physical-store-stream store) :abort t))
    (setf (physical-store-stream store) nil
          (physical-store-segment store) nil
          (physical-store-segment-timeline store) nil)
    (physical-store-discard-tail store)
    (setf (physical-store-written-lsn store) (physical-store-flushed-lsn store)
          (physical-store-written-digest store) (physical-store-flushed-digest store)
          (physical-store-decoder store)
          (make-physical-wal-decoder :start-lsn (physical-store-flushed-lsn store)))
    (physical-store-flushed-lsn store)))

(defun physical-store-input (store start-lsn bytes)
  "Write complete validated records; partial input never advances write or flush."
  (with-physical-store (store)
    (physical-wal-decode-input
     (physical-store-decoder store) start-lsn bytes
     :on-record (lambda (record raw) (physical-store-write-record store record raw)))
    (physical-store-written-lsn store)))

(defun physical-store-flush (store)
  "Fsync WAL and names, then persist the manifest, then publish acknowledged flush."
  (with-physical-store (store)
    (when (< (physical-store-flushed-lsn store) (physical-store-written-lsn store))
      (when (physical-store-stream store)
        (fsync-stream (physical-store-stream store)))
      (fsync-directory (physical-store-directory store))
      (physical-store-test-point :after-segment-fsync store)
      (physical-store-save-control store (physical-store-written-lsn store))
      (setf (physical-store-flushed-lsn store) (physical-store-written-lsn store)
            (physical-store-flushed-digest store) (physical-store-written-digest store)))
    (physical-store-flushed-lsn store)))

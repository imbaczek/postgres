;; Destructive test for a disposable prepared child only.
;; Drops canonical slot files, then checks that the permanent historical proof
;; can be observed without resurrecting slots.

;; Reselect and read back a real primary root while the original root remains
;; independently pinned.  Check replay failure retains that pin and reject a
;; wrong child carrier or fork.

(require :asdf)
(let ((source (truename (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*)))))
  (pushnew source asdf:*central-registry* :test #'equal)
  (asdf:load-asd (merge-pathnames "pg.asd" source))
  (asdf:load-system "pg"))
(in-package #:pg)

(defun native-imported-host-reject-short-control (owner context facts)
  (let ((original (symbol-function 'logical-slot-imported-owner-source-under-lock))
        (fork (getf (getf facts :certificate) :fork)))
    (assert (plusp fork))
    (unwind-protect
         (progn
           (setf (symbol-function 'logical-slot-imported-owner-source-under-lock)
                 (lambda (candidate)
                   (multiple-value-bind (slot system-id history end) (funcall original candidate)
                     (declare (ignore end))
                     (values slot system-id history (1- fork)))))
           (assert (handler-case
                       (progn (logical-slot-imported-check-certificate-source owner context facts) nil)
                     (error () t))))
      (setf (symbol-function 'logical-slot-imported-owner-source-under-lock) original))))

(defun native-host-with-root (gate ownership function)
  (call-with-logical-slot-imported-host-startup-owner
   gate ownership
   (lambda (owner)
     (call-with-physical-directory-root
      owner (lambda (root path)
              (declare (ignore path))
              (call-with-physical-image-owned-context
               root t (lambda (context) (funcall function owner context))))))))

(defun native-host-origin-cursor-bridge (archive)
  ;; Test the original parent archive through an archive-only cursor and a
  ;; record provider into a cursor using current child history.  The enclosing
  ;; archive scope holds its lease; guards recheck the source.  This test bridge
  ;; does not provide admission.
  (let* ((base (getf (logical-wal-archive-reference archive) :base))
         (fork (getf (logical-wal-archive-reference archive) :end))
         (history (timeline-check-lineage
                   (timeline-parse-history *wal-insert-timeline*
                                           (bytes->string (timeline-history-bytes *wal-insert-timeline*)))))
         (records 0))
    (flet ((guard () (logical-wal-archive-check archive)))
      (call-with-logical-wal-cursor
       (logical-wal-archive-history archive) :timeline base fork
       (timeline-at-point base (logical-wal-archive-history archive)) :prefix-end #'guard
       (lambda (inner)
         (call-with-logical-wal-record-provider
          (lambda (position consume)
            (sb-thread:with-mutex (*wal-lock*)
              (logical-wal-cursor-check-scope inner)
              (assert (= position (logical-wal-cursor-position inner))))
            (multiple-value-bind (record raw owner carrier kind) (logical-wal-cursor-next inner)
              (declare (ignore owner kind))
              (assert record)
              (funcall consume raw (wal-record-start record) carrier)
              (sb-thread:with-mutex (*wal-lock*)
                (logical-wal-cursor-check-scope inner)
                (assert (= (wal-record-end record) (logical-wal-cursor-position inner))))))
          #'guard
          (lambda (provider)
            (call-with-logical-wal-cursor
             history :timeline base fork (timeline-at-point base history) :prefix-end #'guard
             (lambda (outer)
               (loop for record = (logical-wal-cursor-next outer) while record
                     do (incf records)
                        (assert (<= (wal-record-end record) fork)))
               (assert (= fork (logical-wal-cursor-position outer)))
               (assert (= fork (logical-wal-cursor-position inner)))
               (assert (not (logical-wal-cursor-batch-open inner))))
             :record-reader provider :batch-policy :logical-reader))))
       :archive archive :archive-only t :batch-policy :logical-reader))
    (when (< base fork) (assert (plusp records)))
    (format t "~&NATIVE-ORIGIN archive-only parent cursor supplied ~d records to current-history provider cursor~%"
            records)))

(defun native-host-origin-read (gate ownership different-primary)
  (let ((saved nil) (record nil) (token nil) (origin-id nil)
                    (primary (physical-promotion-retained-owner :primary)))
    (native-host-with-root
     gate ownership
     (lambda (owner context)
       (call-with-logical-slot-imported-host-origin
        owner context
        (lambda (archive)
          (setf saved archive origin-id (getf (logical-wal-archive-reference archive) :root-id))
          (assert (= 1 (hash-table-count *logical-slot-imported-host-origins*)))
          (setf record (loop for value being the hash-values of *logical-slot-imported-host-origins* return value))
          (sb-thread:with-mutex (*wal-lock*)
            (setf token (logical-slot-imported-host-origin-call record :selection))
            (assert (logical-slot-imported-host-origin-call record :check-root))
            (assert (handler-case
                        (progn (logical-slot-imported-host-origin-call
                                (copy-logical-slot-imported-host-origin record) :check-root) nil)
                      (error () t)))
            (logical-wal-archive-check archive))
          (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
            (let* ((origin (gethash token *physical-retained-root-references*))
                   (current (gethash primary *physical-retained-root-references*))
                   (readers (loop for key being the hash-keys of *physical-retained-root-references*
                                    using (hash-value value)
                                  when (and (not (eq key token)) (eq value origin)) collect key)))
              (assert origin)
              (assert (not (eq origin current)))
              (assert (= 1 (length readers)))
              (when different-primary
                (assert (/= origin-id (physical-retained-root-reference-root-id current))))))
          (let* ((segment (floor (getf (logical-wal-archive-reference archive) :base) +wal-segment-size+))
                 (carrier (logical-wal-segment-carrier segment (logical-wal-archive-history archive))))
            (assert (/= carrier *wal-insert-timeline*))
            (assert (handler-case
                        (progn (logical-wal-archive-prepare archive segment *wal-insert-timeline*) nil)
                      (error () t)))
            (logical-wal-archive-prepare archive segment carrier)
            (sb-thread:with-mutex (*wal-lock*)
              (let ((base (getf (logical-wal-archive-reference archive) :base))
                    (fork (getf (logical-wal-archive-reference archive) :end)))
                (when (< base fork)
                  (multiple-value-bind (raw size found)
                      (logical-wal-archive-copy-at archive base fork carrier)
                    (declare (ignore size))
                    (assert found)
                    (assert raw)
                    (assert (<= (wal-record-end (wal-decode-record raw base)) fork))))
                (assert (handler-case
                            (progn (logical-wal-archive-copy-at archive fork *wal-durable-lsn* carrier) nil)
                          (error () t))))))
          (native-host-origin-cursor-bridge archive)
          (format t "~&NATIVE-ORIGIN read original root=~d different-primary=~s~%" origin-id different-primary)))))
    (assert (null (logical-wal-archive-index saved)))
    (assert (null (logical-wal-archive-cache-bytes saved)))
    (sb-thread:with-mutex (*wal-lock*)
      (assert (handler-case (progn (logical-wal-archive-check saved) nil) (error () t)))
      (assert (logical-slot-imported-host-origin-call record :check-root)))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (let ((origin (gethash token *physical-retained-root-references*)))
        (assert (= 1 (loop for value being the hash-values of *physical-retained-root-references*
                           count (eq value origin))))))
    origin-id))

(defun native-host-republish-primary-root ()
  (sb-thread:with-mutex (*wal-lock*)
    (let* ((fields (multiple-value-list (wal-read-control-fields)))
           (old (getf (seventh fields) :retained-reference))
           (old-id (getf old :root-id))
           (directory (physical-recovery-retained-directory))
           (identity (physical-promotion-retained-identity (getf (seventh fields) :system-id) old))
           (history (timeline-check-lineage
                     (timeline-parse-history *wal-insert-timeline*
                                             (bytes->string (timeline-history-bytes *wal-insert-timeline*)))))
           (owner (physical-promotion-retained-owner :primary))
           (expected nil))
      (multiple-value-bind (parent fork projected) (physical-promotion-retained-parent-history history)
        (declare (ignore fork))
        (let* ((index (physical-retained-inventory-load directory old-id identity projected))
               (text (format nil "# host origin test after ~d~%~a" old-id (getf index :history-text)))
               (new-id
                 (call-with-physical-retained-root-update
                  (lambda (id hold)
                    (declare (ignore hold))
                    (let ((reference (copy-tree old)))
                      (setf (getf reference :root-id) id
                            *wal-control-metadata* (copy-tree *wal-control-metadata*)
                            (getf *wal-control-metadata* :retained-reference) reference)
                      (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*)
                      (setf expected (multiple-value-list (wal-read-control-fields)))
                      id))
                  owner directory identity parent text nil
                  :previous old-id :expected-root old-id
                  :read-selected (lambda () (physical-promotion-primary-root-selection expected)))))
          (assert (/= old-id new-id))
          (physical-promotion-restore-primary-retained)
          (assert (eql new-id (physical-retained-root-current owner)))
          new-id)))))

(defun native-host-origin-failure-keeps-pin (gate ownership original)
  (let ((prior (symbol-function 'logical-slot-copy-source-proof-scan-archive))
        (calls 0) (entered nil) (count (hash-table-count *physical-retained-root-references*)))
    (unwind-protect
         (progn
           (setf (symbol-function 'logical-slot-copy-source-proof-scan-archive)
                 (lambda (facts directory reader)
                   (declare (ignore facts directory reader))
                   (incf calls)
                   (error "injected origin replay failure")))
           (assert (handler-case
                       (progn
                         (native-host-with-root
                          gate ownership
                          (lambda (owner context)
                            (call-with-logical-slot-imported-host-origin
                             owner context (lambda (archive) (declare (ignore archive)) (setf entered t)))))
                         nil)
                     (error () t))))
      (setf (symbol-function 'logical-slot-copy-source-proof-scan-archive) prior))
    (assert (= 1 calls))
    (assert (not entered))
    (assert (= count (hash-table-count *physical-retained-root-references*)))
    (assert (null (physical-retained-root-delete-unprotected
                   (physical-recovery-retained-directory) :inventory original)))
    (format t "~&NATIVE-ORIGIN replay failure kept the original root protected and released its reader~%")))

(defun native-host-origin-cycle (gate ownership)
  "Exercise rejection of the child carrier and reads at F."
  (let* ((original (native-host-origin-read gate ownership nil))
         (current (progn (native-host-origin-failure-keeps-pin gate ownership original)
                         (native-host-republish-primary-root))))
    (assert (/= original current))
    (assert (null (physical-retained-root-delete-unprotected
                   (physical-recovery-retained-directory) :inventory original)))
    (assert (eql original (native-host-origin-read gate ownership t)))
    (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
    (format t "~&NATIVE-ORIGIN PASS independent original root survives owner exit and current primary reselection; zero slot files~%")))

(defun native-host-continuation-check-unwind (storage &optional interrupt)
  (let* ((directory (logical-slot-continuation-storage-call storage :directory))
         (original (symbol-function 'physical-image-file-facts))
         (target sb-thread:*current-thread*) (actor nil)
         (blocked (sb-thread:make-semaphore :count 0))
         (injected nil) (result nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'physical-image-file-facts)
                 (lambda (descriptor &optional (name ""))
                   (when (and (eql descriptor directory) (equal name ""))
                     (if interrupt
                         (progn
                           (sb-sys:without-interrupts
                             (setf actor
                                   (sb-thread:make-thread
                                    (lambda ()
                                      (sb-thread:interrupt-thread
                                       target
                                       (lambda ()
                                         (setf injected t)
                                         (throw 'native-continuation-check :interrupted))))
                                    :name "native continuation check interrupt")))
                           (sb-thread:wait-on-semaphore blocked :timeout 5)
                           (error "continuation check interrupt was not delivered"))
                         (progn
                           (setf injected t)
                           (throw 'native-continuation-check :interrupted))))
                   (funcall original descriptor name)))
           (setf result
                 (catch 'native-continuation-check
                   (unwind-protect
                        (progn (logical-slot-continuation-storage-check storage) :unexpected-return)
                     (when actor (sb-thread:join-thread actor))))))
      (setf (symbol-function 'physical-image-file-facts) original))
    (and injected (eq result :interrupted))))

(defun native-host-continuation-storage (gate ownership)
  (let ((saved nil) (fds (length (directory #p"/proc/self/fd/*"))))
    (call-with-logical-slot-continuation-startup-storage
     gate ownership
     (lambda (storage facts)
       (setf saved storage)
       (assert (plusp (length (getf (getf facts :certificate) :slots))))
       (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
       (assert (handler-case
                   (progn (logical-slot-continuation-storage-check
                           (copy-logical-slot-continuation-startup-storage storage)) nil)
                 (error () t)))
       (assert
        (eq :rejected
            (sb-thread:join-thread
             (sb-thread:make-thread
              (lambda ()
                (handler-case (logical-slot-continuation-storage-check storage)
                  (error () :rejected)))))))
       (let* ((directory (logical-slot-continuation-storage-call storage :directory))
              (root-facts (physical-image-file-facts -100 (namestring *pgdata*)))
              (path (merge-pathnames "native-storage-probe" (physical-image-fd-path directory)))
              (binding (logical-slot-continuation-storage-call storage :binding)))
         (assert (plusp (length binding)))
         (setf (aref binding 0) (logxor 1 (aref binding 0)))
         (assert (not (equalp binding (logical-slot-continuation-storage-call storage :binding))))
         (unwind-protect
              (progn
                (with-open-file (stream path :direction :output :if-exists :error
                                             :element-type '(unsigned-byte 8))
                  (write-byte 42 stream)
                  (finish-output stream)
                  (sb-posix:fsync (sb-sys:fd-stream-fd stream)))
                (sb-posix:fsync directory)
                (assert (equal root-facts (physical-image-file-facts -100 (namestring *pgdata*))))
                (assert (logical-slot-continuation-storage-check storage)))
           (when (probe-file path) (delete-file path))
           (sb-posix:fsync directory)))))
    (assert (handler-case (progn (logical-slot-continuation-storage-check saved) nil) (error () t)))
    (dolist (kind '(:root-entry :directory-mode :registration :nonlocal :interrupt))
      (let ((entered nil) (rejected nil) (sticky nil) (scope-failed nil)
                          (unexpected (merge-pathnames "native-continuation-root-mutation" *pgdata*))
                          (child (merge-pathnames "pg_logical_continuation/" *pgdata*)))
        (handler-case
            (call-with-logical-slot-continuation-startup-storage
             gate ownership
             (lambda (storage facts)
               (declare (ignore facts))
               (setf entered t)
               (unwind-protect
                    (progn
                      (ecase kind
                        (:root-entry (sb-posix:mkdir (namestring unexpected) #o700))
                        (:directory-mode (sb-posix:chmod (namestring child) #o755))
                        ((:registration :nonlocal :interrupt) nil))
                      (setf rejected
                            (ecase kind
                              ((:root-entry :directory-mode)
                               (handler-case
                                   (progn (logical-slot-continuation-storage-check storage) nil)
                                 (error () t)))
                              (:registration
                               (let ((*logical-slot-continuation-startup-storages* nil))
                                 (handler-case
                                     (progn (logical-slot-continuation-storage-check storage) nil)
                                   (error () t))))
                              (:nonlocal (native-host-continuation-check-unwind storage))
                              (:interrupt (native-host-continuation-check-unwind storage t)))))
                 (ecase kind
                   (:root-entry (when (probe-file unexpected) (sb-posix:rmdir (namestring unexpected))))
                   (:directory-mode (sb-posix:chmod (namestring child) #o700))
                   ((:registration :nonlocal :interrupt) nil)))
               (setf sticky
                     (handler-case
                         (progn (logical-slot-continuation-storage-check storage) nil)
                       (error () t)))))
          (error () (setf scope-failed t)))
        (assert entered)
        (assert rejected)
        (assert sticky)
        (assert scope-failed)))
    (call-with-logical-slot-continuation-startup-storage
     gate ownership
     (lambda (storage facts)
       (declare (ignore facts))
       (assert (logical-slot-continuation-storage-check storage))))
    (assert (= fds (length (directory #p"/proc/self/fd/*"))))
    (format t "~&NATIVE-CONTINUATION-STORAGE PASS private child writes preserve full root proof; copied, foreign-thread, expired and mutated scopes rejected; registration, throw and external interrupt failures sticky; zero slot files~%")))

(defun native-host-continuation-early-gc-fence ()
  (let* ((directory (physical-recovery-retained-directory))
         (bytes (string->bytes "native continuation unknown-selection probe"))
         (id (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
               (physical-retained-object-publish directory :inventory bytes 128)))
         (path (physical-retained-object-path directory :inventory id)))
    (unwind-protect
         (progn
           (assert (not (physical-retained-root-protected-p directory :inventory id)))
           (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
           (assert (null (physical-retained-root-delete-unprotected directory :inventory id)))
           (assert (equalp bytes (physical-retained-object-read directory :inventory id 128))))
      (when (probe-file path)
        (assert (equalp bytes (physical-retained-object-read directory :inventory id 128)))
        (delete-file path)
        (fsync-directory directory)))))

(defun native-host-continuation-selector-reset (gate ownership)
  (call-with-logical-slot-continuation-startup-storage
   gate ownership
   (lambda (storage facts)
     (logical-slot-continuation-storage-call
      storage :with-origin
      (lambda (archive)
        (declare (ignore archive))
        (let ((names (logical-slot-continuation-selector-names storage)))
          (when names
            (assert (equal names '("selector")))
            (call-with-logical-slot-continuation-selector
             storage
             (lambda (snapshot fd selected-facts)
               (declare (ignore fd selected-facts))
               (logical-slot-continuation-check-empty-startup storage facts snapshot)))
            (let* ((directory (logical-slot-continuation-storage-call storage :directory))
                   (path (slot-state-fd-path directory "selector")))
              (delete-file path)
              (sb-posix:fsync directory)))))))))

(defun native-host-continuation-write-selector-image (path image)
  (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8)
                               :if-exists :overwrite :if-does-not-exist :create)
    (write-sequence image stream)
    (finish-output stream)
    (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) (length image))
    (sb-posix:fsync (sb-sys:fd-stream-fd stream)))
  (sb-posix:chmod (namestring path) #o600)
  (fsync-directory (uiop:pathname-directory-pathname path)))

(defun native-host-continuation-selector-adversarial (gate ownership)
  (let* ((directory (merge-pathnames "pg_logical_continuation/" *pgdata*))
         (path (merge-pathnames "selector" directory))
         (baseline (read-bounded-regular-file path (logical-slot-continuation-maximum-bytes)))
         (control (logical-slot-startup-control-image))
         (identity (logical-slot-copy-install-file-identity
                    (physical-image-file-facts -100 (namestring path))))
         (cases 0))
    (dolist (kind '(:crc :binding :system-id :future-end :transfer :ambiguous :unknown))
      (let ((image (copy-seq baseline))
            (extra (merge-pathnames (if (eq kind :ambiguous) "selector.tmp" "unclassified") directory))
            (registry *wal-replication-slots*) (rejected nil))
        (unless (member kind '(:crc :ambiguous :unknown))
          (let ((snapshot (logical-slot-continuation-decode baseline)))
            (ecase kind
              (:binding (setf (aref (getf snapshot :binding) 0)
                              (logxor 1 (aref (getf snapshot :binding) 0))))
              (:system-id (setf (getf snapshot :system-id)
                                (if (= (getf snapshot :system-id) 18446744073709551615)
                                    1 (1+ (getf snapshot :system-id)))))
              (:future-end (incf (first (getf snapshot :sealed-end))))
              (:transfer
               (let ((position (copy-list (getf snapshot :sealed-end))))
                 (setf (getf snapshot :transfers)
                       (list (list "00000000000000000000000000000001" position))
                       (getf snapshot :tail) (list (copy-list position) (getf snapshot :history-target))))))
            (setf image (logical-slot-continuation-encode snapshot))
            (logical-slot-continuation-decode image)))
        (when (eq kind :crc) (setf (aref image (1- (length image))) (logxor 1 (aref image (1- (length image))))))
        (unwind-protect
             (progn
               (if (member kind '(:ambiguous :unknown))
                   (native-host-continuation-write-selector-image extra image)
                   (native-host-continuation-write-selector-image path image))
               (setf rejected
                     (handler-case
                         (progn (wal-load-replication-slots :gate-owner gate :ownership ownership) nil)
                       (error () t)))
               (assert rejected)
               (assert (eq registry *wal-replication-slots*))
               (assert (equalp image
                               (read-bounded-regular-file
                                (if (member kind '(:ambiguous :unknown)) extra path)
                                (logical-slot-continuation-maximum-bytes))))
               (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
               (assert (equalp control (logical-slot-startup-control-image)))
               (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
               (incf cases))
          (when (probe-file extra) (delete-file extra))
          (native-host-continuation-write-selector-image path baseline)))
      (wal-load-replication-slots :gate-owner gate :ownership ownership)
      (assert (equal identity (logical-slot-copy-install-file-identity
                               (physical-image-file-facts -100 (namestring path))))))
    (format t "~&NATIVE-SELECTOR-ADVERSARIAL PASS cases=~d; rejected evidence preserved, no registry or control mutation, positive retry passed~%" cases)))

(defun native-host-continuation-selector (gate ownership)
  (let* ((cut (uiop:getenv "PG_NATIVE_SELECTOR_CUT"))
         (point (cdr (assoc cut
                            '(("after-temp-write" . :after-selector-temp-write)
                              ("after-temp-sync" . :after-selector-temp-sync)
                              ("before-install" . :before-selector-install)
                              ("after-install" . :after-selector-install)
                              ("after-directory-sync" . :after-selector-directory-sync)
                              ("after-restoration-sync" . :after-selector-restoration-sync))
                            :test #'equal)))
         (before (logical-slot-startup-control-image))
         (*logical-slot-continuation-selector-write-limit*
           (when (eq point :after-selector-temp-write) 31)))
    (when (and cut (not point)) (error "unknown native selector cut"))
    (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
    (native-host-continuation-early-gc-fence)
    (when cut (native-host-continuation-selector-reset gate ownership))
    (let ((*logical-slot-continuation-selector-test-hook*
            (when cut
              (lambda (at storage)
                (declare (ignore storage))
                (when (eq at point)
                  (assert (equalp before (logical-slot-startup-control-image)))
                  (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
                  (format t "~&NATIVE-SELECTOR-CUT ~a~%" cut)
                  (finish-output)
                  (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill))))))
      (wal-load-replication-slots :gate-owner gate :ownership ownership))
    (when cut (error "native selector cut did not execute"))
    (assert (equalp before (logical-slot-startup-control-image)))
    (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
    (assert (zerop (hash-table-count *wal-replication-slots*)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (when (uiop:getenv "PG_NATIVE_SELECTOR_ADVERSARIAL")
      (native-host-continuation-selector-adversarial gate ownership))
    (format t "~&NATIVE-SELECTOR-RECOVER PASS actual startup hook, origin proof, empty registry, unchanged control; GC and admission still fenced~%")))

(defun native-host-continuation-active-refusal (gate ownership)
  (let* ((entries (logical-slot-startup-read-inventory :local-workspace t))
         (registry *wal-replication-slots*)
         (before
           (loop for entry in entries
                 for slot = (logical-slot-startup-entry-slot entry)
                 for path = (wal-replication-slot-path (wal-replication-slot-name slot))
                 collect (list path (physical-image-file-facts -100 (namestring path))
                               (read-bounded-regular-file path +slot-state-maximum-bytes+))))
         (rejected nil))
    (assert entries)
    (assert (every (lambda (entry)
                     (wal-replication-slot-child-owner (logical-slot-startup-entry-slot entry))) entries))
    (assert (not (probe-file (merge-pathnames "pg_logical_continuation/selector" *pgdata*))))
    (setf rejected
          (handler-case
              (progn (wal-load-replication-slots :gate-owner gate :ownership ownership) nil)
            (error () t)))
    (assert rejected)
    (assert (eq registry *wal-replication-slots*))
    (assert (zerop (hash-table-count registry)))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (assert (not (probe-file (merge-pathnames "pg_logical_continuation/selector" *pgdata*))))
    (assert (not (probe-file (merge-pathnames "pg_logical_continuation/selector.tmp" *pgdata*))))
    (dolist (row before)
      (destructuring-bind (path facts bytes) row
        (assert (equal facts (physical-image-file-facts -100 (namestring path))))
        (assert (equalp bytes (read-bounded-regular-file path +slot-state-maximum-bytes+)))))
    (format t "~&NATIVE-SELECTOR-ACTIVE-REFUSAL PASS slots=~d; files and registry unchanged; no selector publication~%"
            (length entries))))

(defun native-imported-host-observation ()
  (unless (equal "1" (uiop:getenv "PG_NATIVE_HOST_DISPOSABLE"))
    (error "This test drops slot files; set PG_NATIVE_HOST_DISPOSABLE=1 only for its disposable fixture"))
  (let* ((*pgdata* (uiop:ensure-directory-pathname
                    (or (uiop:getenv "PG_NATIVE_PROOF_DATA") (error "Set PG_NATIVE_PROOF_DATA"))))
         (*print-circle* t) (*print-level* 4) (*print-length* 8)
         (*wal-level* :logical) (*startup-installed-configuration* nil)
         (exit-hooks sb-ext:*exit-hooks*) (saved-owner nil) (saved-check nil))
    (let ((settings (startup-default-settings (namestring *pgdata*))))
      (setf (getf (gethash "wal_level" settings) :value) "logical"
            *startup-installed-configuration*
            (startup-prepare-installation settings (make-hash-table :test #'equal)
                                          (make-hash-table :test #'equal) nil)))
    (unwind-protect
         (progn
           (postmaster-own-data-directory "127.0.0.1" 5439)
           (call-with-live-operation-startup-quiescence
            (lambda (gate)
              (call-with-postmaster-ownership
               (lambda (ownership)
                 (check-primary-storage-startup-context gate ownership)
                 (wal-mark-primary-startup)
                 (wal-init)
                 (xact-init)
                 (wal-recover)
                 (note-primary-startup-replay)
                 (physical-promotion-restore-primary-retained)
                 (when (uiop:getenv "PG_NATIVE_SELECTOR_REQUIRE_ACTIVE")
                   (native-host-continuation-active-refusal gate ownership)
                   (return-from native-imported-host-observation t))
                 (let ((registry *wal-replication-slots*)
                       (fds (length (directory #p"/proc/self/fd/*")))
                       (dropped 0))
                   (assert (zerop (hash-table-count registry)))
                   (assert (null *catalog*))
                   (when (or (uiop:getenv "PG_NATIVE_SELECTOR_CUT") (uiop:getenv "PG_NATIVE_SELECTOR_RECOVER"))
                     (assert (null (logical-slot-startup-read-inventory :local-workspace t))))
                   (dolist (entry (logical-slot-startup-read-inventory :local-workspace t))
                     (let* ((name (wal-replication-slot-name (logical-slot-startup-entry-slot entry)))
                            (root (wal-replication-slot-directory))
                            (claim (slot-state-claim-drop root name)))
                       (slot-state-drop root name claim)
                       (incf dropped)))
                   (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
                   (call-with-logical-slot-imported-host-startup-owner
                    gate ownership
                    (lambda (owner)
                      (setf saved-owner owner)
                      (call-with-physical-directory-root
                       owner
                       (lambda (root path)
                         (declare (ignore path))
                         (call-with-physical-image-owned-context
                          root t
                          (lambda (context)
                            (call-with-logical-slot-imported-proof-files
                             owner context
                             (lambda (facts check under-lock)
                               (setf saved-check check)
                               (assert (plusp (length (getf (getf facts :certificate) :slots))))
                               (native-imported-host-reject-short-control owner context facts)
                               (funcall check)
                               (sb-thread:with-mutex (*wal-lock*) (funcall under-lock))
                               (format t "~&NATIVE-HOST proof bound with zero slot files; dropped=~d historical-rows=~d~%"
                                       dropped (length (getf (getf facts :certificate) :slots)))))))))))
                   (assert (not (physical-adoption-owner-active-p saved-owner)))
                   (assert (handler-case (progn (funcall saved-check) nil) (error () t)))
                   (native-host-origin-cycle gate ownership)
                   (native-host-continuation-storage gate ownership)
                   (if (or (uiop:getenv "PG_NATIVE_SELECTOR_CUT") (uiop:getenv "PG_NATIVE_SELECTOR_RECOVER"))
                       (native-host-continuation-selector gate ownership)
                       (assert (eq registry *wal-replication-slots*)))
                   (assert (zerop (hash-table-count registry)))
                   (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
                   (assert (null *catalog*))
                   (assert (= fds (length (directory #p"/proc/self/fd/*"))))
                   (assert (null *primary-wal-writer*))
                   (format t "~&NATIVE-HOST PASS canonical permanent proof checked independently of current slots; no imported-slot admission~%")))))))
      (postmaster-remove-pid-file)
      (when *postmaster-data-lock* (close *postmaster-data-lock*))
      (setf sb-ext:*exit-hooks* exit-hooks))))

(native-imported-host-observation)

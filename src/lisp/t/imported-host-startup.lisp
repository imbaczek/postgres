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

(defun native-host-continuation-startup-reject-slot-resources (gate ownership)
  (let ((original (symbol-function 'call-with-logical-slot-continuation-owned-directory))
        (opens 0) (callbacks 0) (entered nil) (rejected nil) (sticky nil) (scope-failed nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'call-with-logical-slot-continuation-owned-directory)
                 (lambda (&rest arguments)
                   (incf opens)
                   (apply original arguments)))
           (handler-case
               (call-with-logical-slot-continuation-startup-storage
                gate ownership
                (lambda (storage facts)
                  (declare (ignore facts))
                  (setf entered t)
                  (setf rejected
                        (handler-case
                            (progn
                              (logical-slot-continuation-storage-call
                               storage :with-slot-root
                               (lambda (&rest ignored)
                                 (declare (ignore ignored))
                                 (incf callbacks)))
                              nil)
                          (error (condition)
                            (search "slot directory resources require runtime storage"
                                    (princ-to-string condition)))))
                  (setf sticky
                        (handler-case
                            (progn (logical-slot-continuation-storage-check storage) nil)
                          (error () t)))))
             (error () (setf scope-failed t))))
      (setf (symbol-function 'call-with-logical-slot-continuation-owned-directory) original))
    (assert (and entered rejected sticky scope-failed (zerop opens) (zerop callbacks)))
    (format t "~&NATIVE-STARTUP-SLOT-RESOURCES-REFUSED PASS no-open no-callback sticky-owner~%")))

(defun native-host-continuation-storage (gate ownership)
  (native-host-continuation-startup-reject-slot-resources gate ownership)
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

(defun native-host-continuation-selector-reset (gate ownership &optional (kind :empty))
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
               (ecase kind
                 (:empty (logical-slot-continuation-check-empty-startup storage facts snapshot))
                 (:active (logical-slot-continuation-check-active-startup storage facts snapshot)))))
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

(defun native-host-continuation-boundary-probes (gate ownership)
  (call-with-logical-slot-continuation-startup-storage
   gate ownership
   (lambda (storage facts)
     (logical-slot-continuation-storage-call
      storage :with-origin
      (lambda (archive)
        (let* ((reference (logical-wal-archive-reference archive))
               (history (logical-wal-archive-history archive))
               (base (getf reference :base)) (fork (getf reference :end))
               (first-start nil) (first-owner nil) (inside nil) (terminal nil) (terminal-owner nil))
          (flet ((guard ()
                   (logical-slot-continuation-storage-call storage :check-under-lock)
                   (logical-wal-archive-check archive)))
            (call-with-logical-wal-cursor
             history :timeline base fork (timeline-at-point base history) :record-start #'guard
             (lambda (cursor)
               (loop
                 (multiple-value-bind (record raw owner carrier kind) (logical-wal-cursor-next cursor)
                   (declare (ignore raw carrier))
                   (unless record (return))
                   (unless first-start
                     (setf first-start (wal-record-start record) first-owner owner
                           inside (1+ (wal-record-start record))))
                   (when (and (eq kind :ordinary) (wal-terminal-record-type-p (wal-record-type record)))
                     (setf terminal (wal-record-end record) terminal-owner owner)))))
             :archive archive :archive-only t :batch-policy :logical-reader))
          (assert (and first-start terminal))
          (assert
           (null
            (logical-slot-continuation-prove-parent-boundaries
             storage facts archive (list (list :restart first-start first-owner :record-start)
                                         (list :confirmed terminal terminal-owner :prefix-end)))))
          (assert
           (handler-case
               (progn
                 (logical-slot-continuation-prove-parent-boundaries
                  storage facts archive (list (list :restart inside first-owner :prefix-end)))
                 nil)
             (error (condition) (search "record-boundary proof" (princ-to-string condition)))))
          (logical-slot-continuation-storage-check storage)
          (format t "~&NATIVE-ACTIVE-BOUNDARIES parent-start=~d parent-terminal=~d inside-record=~d rejected~%"
                  first-start terminal inside)))))))

(defun native-host-continuation-reject-caller-baseline (gate ownership)
  (let ((rejected nil))
    (setf rejected
          (handler-case
              (progn
                (call-with-logical-slot-continuation-startup-storage
                 gate ownership
                 (lambda (storage facts)
                   (logical-slot-continuation-storage-call
                    storage :with-origin
                    (lambda (archive)
                      (let* ((snapshot (logical-slot-continuation-active-startup-image storage nil))
                             (row (find-if
                                   (lambda (candidate)
                                     (eq :reserved
                                         (wal-replication-slot-wal-status
                                          (logical-slot-state-decode (second candidate) (first candidate)))))
                                   (getf snapshot :slots)))
                             (slot (and row (logical-slot-state-decode (second row) (first row))))
                             (fake-facts (copy-list facts))
                             (fake-certificate (copy-list (getf facts :certificate)))
                             (fake-rows (mapcar #'copy-list (getf fake-certificate :slots))))
                        (assert slot)
                        (setf (wal-replication-slot-restart-lsn slot) (1+ (second (getf snapshot :origin)))
                              (wal-replication-slot-restart-timeline slot) (getf snapshot :history-target)
                              (wal-replication-slot-confirmed-flush-lsn slot) (first (getf snapshot :sealed-end))
                              (wal-replication-slot-confirmed-timeline slot) (getf snapshot :history-target)
                              (second row) (logical-slot-state-encode slot)
                              (fourth (find (first row) fake-rows :key #'first :test #'equal))
                              (bytes->string (second row))
                              (getf fake-certificate :slots) fake-rows
                              (getf fake-facts :certificate) fake-certificate)
                        (logical-slot-continuation-check snapshot)
                        (call-with-logical-slot-continuation-local-tail
                         storage snapshot
                         (lambda (prove check) (declare (ignore check)) (funcall prove))
                         :boundaries (lambda ()
                                       (logical-slot-continuation-active-boundaries
                                        storage fake-facts snapshot archive))))))))
                nil)
            (error (condition) (search "record-boundary proof" (princ-to-string condition)))))
    (assert rejected)
    (format t "~&NATIVE-ACTIVE-CANONICAL-BASELINE PASS caller-provided saved baseline cannot hide an inside-record restart~%")))

(defun native-host-continuation-composed-cursors (gate ownership)
  (let ((totals (list 0 0 0)) (slots 0))
    (dolist (row (slot-state-map (wal-replication-slot-directory)
                                 (lambda (bytes name) (list name bytes)) :local-workspace t))
      (let ((slot (logical-slot-state-decode (second row) (first row))))
        (when (eq :reserved (wal-replication-slot-wal-status slot))
          (incf slots)
          (call-with-logical-slot-continuation-startup-cursor
           gate ownership (first row)
           (lambda (cursor check seams)
             (destructuring-bind (base fork end) seams
               (let ((counts (list 0 0 0)))
                 (loop
                   (multiple-value-bind (record raw owner carrier kind) (logical-wal-cursor-next cursor)
                     (declare (ignore raw owner carrier kind))
                     (unless record (return))
                     (incf (nth (cond ((< (wal-record-start record) base) 0)
                                      ((< (wal-record-start record) fork) 1) (t 2)) counts))))
                 (funcall check)
                 (assert (= end (logical-wal-cursor-position cursor)))
                 (setf totals (mapcar #'+ totals counts))
                 (format t "~&NATIVE-COMPOSED-READER slot=~a sidecar=~d origin=~d child=~d seams=~s~%"
                         (first row) (first counts) (second counts) (third counts) seams))))))))
    (assert (equal '(:primary :secondary)
                   (multiple-value-list
                    (call-with-logical-slot-continuation-startup-readers
                     gate ownership
                     (lambda (read-slot check)
                       (declare (ignore read-slot))
                       (funcall check)
                       (values :primary :secondary))))))
    (assert (plusp slots))
    (assert (every #'plusp totals))
    (format t "~&NATIVE-COMPOSED-READER PASS slots=~d lane-records=~s~%" slots totals)))

(defun native-host-continuation-pending-candidates (gate ownership)
  (let ((entries (logical-slot-startup-read-inventory :local-workspace t))
        (total 0))
    (call-with-logical-slot-imported-startup-source
     gate ownership entries
     (lambda (source)
       (call-with-logical-slot-continuation-startup-readers
        gate ownership
        (lambda (read-slot check)
          (dolist (entry entries)
            (let ((slot (logical-slot-startup-entry-slot entry)))
              (when (eq :reserved (wal-replication-slot-wal-status slot))
                (funcall read-slot (wal-replication-slot-name slot)
                         (lambda (cursor seams)
                           (declare (ignore seams))
                           (let* ((pending (logical-slot-startup-derive-pending-with-cursor source slot cursor))
                                  (rows (loop for xid being the hash-keys of pending
                                                using (hash-value positions)
                                              collect (list xid (car positions) (cdr positions)))))
                             (incf total (hash-table-count pending))
                             (dolist (row rows)
                               (assert (<= (wal-replication-slot-restart-lsn slot)
                                           (second row) (third row) (logical-slot-startup-source-end source))))
                             (format t "~&NATIVE-COMPOSED-PENDING slot=~a pending=~s~%"
                                     (wal-replication-slot-name slot) (sort rows #'< :key #'first))))))))
          (funcall check)))))
    (assert (plusp total))
    (format t "~&NATIVE-COMPOSED-PENDING PASS candidates=~d pending-xids=~d registry-unpublished~%"
            (length entries) total)))

(defun native-host-continuation-caught-reader-failure (gate ownership)
  (let ((caught nil) (rejected nil)
                     (name (first (find-if
                                   (lambda (row)
                                     (eq :reserved (wal-replication-slot-wal-status
                                                    (logical-slot-state-decode (second row) (first row)))))
                                   (slot-state-map (wal-replication-slot-directory)
                                                   (lambda (bytes name) (list name bytes)) :local-workspace t)))))
    (assert name)
    (setf rejected
          (handler-case
              (progn
                (call-with-logical-slot-continuation-startup-readers
                 gate ownership
                 (lambda (read-slot check)
                   (declare (ignore check))
                   (handler-case
                       (funcall read-slot name
                                (lambda (cursor seams)
                                  (declare (ignore seams))
                                  (logical-wal-cursor-next cursor)
                                  (error "intentional composed slot callback failure")))
                     (error (condition)
                       (setf caught (search "intentional composed slot callback failure"
                                            (princ-to-string condition)))))))
                nil)
            (error () t)))
    (assert caught)
    (assert rejected)
    (format t "~&NATIVE-COMPOSED-CAUGHT-FAILURE PASS caught slot failure rejects entire reader scope~%")))

(defun native-host-continuation-observer-cannot-publish (gate ownership)
  (let ((registry *wal-replication-slots*)
        (count *logical-capture-live-slots*)
        (policy *wal-logical-policy*)
        (disable *wal-logical-info-disable-pending*)
        (rejected nil))
    (call-with-logical-slot-imported-startup-source
     gate ownership (logical-slot-startup-read-inventory :local-workspace t)
     (lambda (source)
       (handler-case
           (logical-slot-startup-publish source (make-hash-table :test #'equal))
         (error (condition)
           (setf rejected (search "imported startup publication requires"
                                  (princ-to-string condition)))))))
    (assert (eq registry *wal-replication-slots*))
    (assert (eql count *logical-capture-live-slots*))
    (assert (eq policy *wal-logical-policy*))
    (assert (eq disable *wal-logical-info-disable-pending*))
    (assert rejected)
    (format t "~&NATIVE-STARTUP-OBSERVATION-NO-PUBLISH PASS registry and mirrors unchanged~%")))

(defun native-host-continuation-publication-failures (gate ownership)
  (dolist (point '(:derived :prepared :before-commit :pending-tamper))
    (let* ((mirrors (logical-slot-publication-mirrors))
           (fds (length (directory #p"/proc/self/fd/*")))
           (sentinel (make-condition 'simple-error :format-control "native startup publication cut"))
           (injected nil) (caught nil)
           (*logical-slot-startup-publication-test-hook*
             (lambda (observed owner token restored)
               (declare (ignore owner token))
               (when (or (eq point observed)
                         (and (eq point :pending-tamper) (eq observed :before-commit)))
                 (setf injected t)
                 (if (eq point :pending-tamper)
                     (let* ((slot (loop for slot being the hash-values of restored return slot))
                            (pending (wal-replication-slot-pending-xids slot))
                            (xid (loop for xid being the hash-keys of pending return xid)))
                       (incf (cdr (gethash xid pending))))
                     (error sentinel))))))
      (handler-case (logical-slot-continuation-publish-startup gate ownership)
        (error (condition) (setf caught condition)))
      (assert injected)
      (assert (if (eq point :pending-tamper)
                  (and caught (search "derived pending state changed" (princ-to-string caught)))
                  (eq sentinel caught)))
      (assert (equal mirrors (logical-slot-publication-mirrors)))
      (assert (= fds (length (directory #p"/proc/self/fd/*"))))
      (format t "~&NATIVE-STARTUP-PUBLICATION-PRECOMMIT PASS cut=~s registry-and-mirrors-unchanged~%" point)))
  (let* ((registry *wal-replication-slots*)
         (fds (length (directory #p"/proc/self/fd/*")))
         (sentinel (make-condition 'simple-error :format-control "native startup publication postcommit cut"))
         (caught nil)
         (*logical-slot-startup-publication-test-hook*
           (lambda (point owner token restored)
             (declare (ignore owner token restored))
             (when (eq point :after-commit) (error sentinel)))))
    (handler-case (logical-slot-continuation-publish-startup gate ownership)
      (error (condition) (setf caught condition)))
    (assert (eq sentinel caught))
    (assert (not (eq registry *wal-replication-slots*)))
    (assert (= 2 (hash-table-count *wal-replication-slots*) *logical-capture-live-slots*))
    (maphash
     (lambda (name slot)
       (assert (wal-replication-slot-state-uncertain-p slot))
       (assert (handler-case (progn (wal-require-known-replication-slot name) nil) (error () t))))
     *wal-replication-slots*)
    (assert (= fds (length (directory #p"/proc/self/fd/*"))))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-STARTUP-PUBLICATION-POSTCOMMIT PASS registry-installed slots-uncertain no-rollback~%")))

(defun native-host-continuation-publication-notify-unwind (gate ownership)
  (dolist (site '(:context :notify-entry))
    (let* ((registry *wal-replication-slots*)
           (fds (length (directory #p"/proc/self/fd/*")))
           (saved-owner nil)
           (tag (gensym "PUBLICATION-NOTIFY-CUT-"))
           (target (ecase site
                     (:context 'check-primary-storage-startup-context)
                     (:notify-entry 'logical-slot-startup-publication-notify)))
           (original (symbol-function target))
           (result nil))
      (let ((*logical-slot-startup-publication-test-hook*
              (lambda (point owner token restored)
                (declare (ignore token restored))
                (when (eq point :after-commit) (setf saved-owner owner)))))
        (unwind-protect
             (progn
               (setf (symbol-function target)
                     (lambda (&rest arguments)
                       (ecase site
                         (:context
                          (multiple-value-prog1 (apply original arguments)
                            (when (and saved-owner (not (physical-adoption-owner-active-p saved-owner)))
                              (throw tag :notify-cut))))
                         (:notify-entry
                          (assert saved-owner)
                          (assert (not (physical-adoption-owner-active-p saved-owner)))
                          (throw tag :notify-cut)))))
               (setf result
                     (catch tag
                       (logical-slot-continuation-publish-startup gate ownership)
                       :unexpected-return)))
          (setf (symbol-function target) original)))
      (assert (eq :notify-cut result))
      (assert (not (eq registry *wal-replication-slots*)))
      (assert (not (physical-adoption-owner-active-p saved-owner)))
      (maphash (lambda (name slot)
                 (declare (ignore name))
                 (assert (wal-replication-slot-state-uncertain-p slot)))
               *wal-replication-slots*)
      (assert (= fds (length (directory #p"/proc/self/fd/*"))))
      (format t "~&NATIVE-STARTUP-PUBLICATION-NOTIFY-UNWIND PASS site=~s original-throw-preserved registry-fenced~%" site))))

(defun native-host-continuation-publication (gate ownership)
  (let* ((entries (logical-slot-startup-read-inventory :local-workspace t))
         (registry *wal-replication-slots*)
         (expected (make-hash-table :test #'equal)))
    (call-with-logical-slot-imported-startup-source
     gate ownership entries
     (lambda (source)
       (call-with-logical-slot-continuation-startup-readers
        gate ownership
        (lambda (read-slot check)
          (dolist (entry entries)
            (let ((slot (logical-slot-startup-entry-slot entry)))
              (when (eq :reserved (wal-replication-slot-wal-status slot))
                (funcall read-slot (wal-replication-slot-name slot)
                         (lambda (cursor seams)
                           (declare (ignore seams))
                           (setf (gethash (wal-replication-slot-name slot) expected)
                                 (logical-slot-startup-derive-pending-with-cursor source slot cursor)))))))
          (funcall check)))))
    (when (uiop:getenv "PG_NATIVE_STARTUP_PUBLICATION_FAULTS")
      (native-host-continuation-publication-failures gate ownership)
      (native-host-continuation-publication-notify-unwind gate ownership))
    (let* ((negative-checks 0) (saved-owner nil) (saved-token nil)
                               (*logical-slot-startup-publication-test-hook*
                                 (lambda (point owner token prepared)
                                   (declare (ignore prepared))
                                   (setf saved-owner owner saved-token token)
                                   (case point
                                     (:derived (assert (null *logical-slot-startup-sources*)))
                                     (:prepared
                                      (dolist (pair (list (list (copy-logical-slot-startup-publication-owner owner) token)
                                                          (list owner (copy-logical-slot-startup-publication-token token))))
                                        (assert (handler-case
                                                    (progn (logical-slot-startup-publication-call
                                                            (first pair) :prepare (second pair)) nil)
                                                  (error () t)))
                                        (incf negative-checks))
                                      (let* ((registered *logical-slot-startup-publication-owners*)
                                             (thread
                                               (sb-thread:make-thread
                                                (lambda ()
                                                  (let ((*logical-slot-startup-publication-owners* registered))
                                                    (handler-case
                                                        (progn (logical-slot-startup-publication-call owner :prepare token) nil)
                                                      (error () t)))))))
                                        (assert (sb-thread:join-thread thread))
                                        (incf negative-checks)))
                                     (:after-commit
                                      (assert (handler-case
                                                  (progn (logical-slot-startup-publication-call owner :commit token) nil)
                                                (error () t)))
                                      (incf negative-checks)))))
                               (restored (logical-slot-continuation-publish-startup gate ownership)))
      (assert (= 4 negative-checks))
      (assert (not (physical-adoption-owner-active-p saved-owner)))
      (assert (handler-case
                  (progn (logical-slot-startup-publication-call saved-owner :commit saved-token) nil)
                (error () t)))
      (assert (and (eq restored *wal-replication-slots*) (not (eq registry restored))))
      (assert (= (length entries) (hash-table-count restored) *logical-capture-live-slots*))
      (dolist (entry entries)
        (let* ((old (logical-slot-startup-entry-slot entry))
               (slot (gethash (wal-replication-slot-name old) restored))
               (pending (gethash (wal-replication-slot-name old) expected)))
          (assert (and slot (not (eq old slot))))
          (assert (equalp (logical-slot-startup-candidate-image old)
                          (logical-slot-startup-candidate-image slot)))
          (assert (and (= 2 (hash-table-count pending))
                       (equalp pending (wal-replication-slot-pending-xids slot))))
          (assert (not (wal-replication-slot-state-uncertain-p slot)))))
      (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
      (format t "~&NATIVE-STARTUP-PUBLICATION PASS slots=~d pending-per-slot=2 registry-and-mirrors-installed GC-fenced~%"
              (hash-table-count restored)))))

(defun native-host-runtime-root-metadata-change (context)
  (let* ((root (physical-image-context-root-fd context))
         (leaf "pg-runtime-source-probe.tmp")
         (path (slot-state-fd-path root leaf))
         (fd (physical-image-openat root leaf
                                    (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl
                                            sb-posix:o-nofollow +postmaster-o-cloexec+) #o600)))
    (when (minusp fd) (error "cannot create native runtime root metadata probe"))
    (unwind-protect
         (values)
      (sb-posix:close fd)
      (sb-posix:unlink path))))

(defun native-host-with-runtime-scan (name function &key upto)
  (call-with-logical-replication-slot
   name
   (lambda (slot)
     (call-with-wal-retained-slot-scan
      name (wal-replication-slot-restart-lsn slot) (or upto *wal-durable-lsn*)
      (lambda (scan)
        (let ((source (sb-thread:with-mutex (*wal-lock*) (logical-wal-capture-source scan))))
          (funcall function scan source)))))))

(defun native-host-continuation-runtime-source-negative ()
  (native-host-with-runtime-scan
   "alpha"
   (lambda (scan source)
     (let ((rejected nil))
       (handler-case
           (call-with-logical-slot-imported-scan-owner
            scan source
            (lambda (owner)
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
                       (declare (ignore facts under-lock))
                       (sb-thread:with-mutex (*wal-lock*)
                         (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*))
                       (native-host-runtime-root-metadata-change context)
                       (funcall check)))))))))
         (error (condition) (setf rejected (princ-to-string condition))))
       (assert (and rejected (search "root descriptor changed" rejected)) ()
               "strict proof control-replacement rejection was ~s" rejected))))
  (let ((registry *logical-slot-imported-host-origins*)
        (count (hash-table-count *logical-slot-imported-host-origins*)))
    (native-host-with-runtime-scan
     "alpha"
     (lambda (scan source)
       (let ((*logical-slot-imported-host-origins* (make-hash-table :test #'equal))
             (rejected nil))
         (handler-case
             (call-with-logical-slot-imported-runtime-source
              scan source (lambda (&rest arguments) (declare (ignore arguments))
                            (error "unrestored runtime origin reached its callback")))
           (error (condition) (setf rejected (princ-to-string condition))))
         (assert (and rejected (search "has not been restored" rejected)))
         (assert (zerop (hash-table-count *logical-slot-imported-host-origins*))))))
    (assert (eq registry *logical-slot-imported-host-origins*))
    (assert (= count (hash-table-count registry))))
  (format t "~&NATIVE-RUNTIME-SOURCE-NEGATIVE PASS strict-default-rejects-root-metadata-change missing-origin-not-restored~%"))

(defun native-host-with-runtime-tail-file (storage snapshot function)
  (logical-slot-continuation-storage-call
   storage :with-local-wal
   (lambda (directory check-directory)
     (let* ((fork (second (getf snapshot :origin)))
            (child (first (getf snapshot :origin)))
            (segment (floor fork +wal-segment-size+))
            (name (wal-segment-filename segment child :timeline)))
       (with-open-file (stream (slot-state-fd-path directory name)
                               :direction :io :if-exists :overwrite
                               :element-type '(unsigned-byte 8))
         (let* ((fd (sb-sys:fd-stream-fd stream))
                (size (sixth (physical-image-file-facts fd)))
                (original (make-array size :element-type '(unsigned-byte 8))))
           (assert (= size (read-sequence original stream)))
           (unwind-protect
                (funcall function stream original (logical-wal-eor-position fork))
             (file-position stream 0)
             (write-sequence original stream)
             (finish-output stream)
             (sb-posix:ftruncate fd size)
             (file-position stream 0)
             (let ((after (make-array size :element-type '(unsigned-byte 8))))
               (assert (= size (read-sequence after stream)))
               (assert (equalp original after)))
             (funcall check-directory))))))))

(defun native-host-runtime-tail-mutate (stream original start mode)
  (let ((offset (mod start +wal-segment-size+)))
    (ecase mode
      (:append
       (file-position stream (length original))
       (write-sequence
        (wal-encode-record +wal-record-batch-start+ 0
                           (+ (- start offset) (length original))
                           (make-array 0 :element-type '(unsigned-byte 8)))
        stream))
      (:rewrite
       (let* ((raw (subseq original offset (+ offset +wal-end-of-recovery-record-size+)))
              (record (wal-decode-record raw start))
              (info (wal-end-of-recovery-record-info record))
              (replacement
                (wal-encode-end-of-recovery-record
                 start (logxor 1 (getf info :end-time)) (getf info :timeline)
                 (getf info :prev-timeline) (getf info :wal-level))))
         (assert (not (equalp raw replacement)))
         (wal-end-of-recovery-record-info (wal-decode-record replacement start))
         (file-position stream offset)
         (write-sequence replacement stream))))
    (finish-output stream)))

(defun native-host-continuation-runtime-tail-mutations (scan source)
  (dolist (mode '(:append :rewrite))
    (let ((entered nil) (returned nil) (failure nil) (seen-eor nil)
                        (*wal-logical-slot-read-progress* 17) (*wal-logical-slot-read-continuation* nil))
      (handler-case
          (call-with-logical-slot-continuation-runtime-storage
           scan source
           (lambda (storage facts)
             (declare (ignore facts))
             (call-with-logical-slot-continuation-selector
              storage
              (lambda (snapshot fd selector-facts)
                (declare (ignore fd selector-facts))
                (native-host-with-runtime-tail-file
                 storage snapshot
                 (lambda (stream original start)
                   (multiple-value-bind (owner role)
                       (sb-thread:with-mutex (*wal-lock*) (logical-slot-wal-scan-start-position scan))
                     (setf returned
                           (call-with-logical-slot-wal-cursor-from-scan
                            scan owner role
                            (lambda (cursor selected-source)
                              (setf entered t)
                              (native-host-runtime-tail-mutate stream original start mode)
                              (loop
                                (multiple-value-bind (record raw record-owner carrier kind)
                                    (logical-wal-cursor-next cursor)
                                  (declare (ignore record-owner carrier))
                                  (unless record (return))
                                  (when (eq kind :end-of-recovery)
                                    (setf seen-eor t)
                                    (assert (equalp raw
                                                    (subseq original (mod start +wal-segment-size+)
                                                            (+ (mod start +wal-segment-size+) (length raw))))))))
                              (sb-thread:with-mutex (*wal-lock*)
                                (setf *wal-logical-slot-read-progress* (wal-retained-slot-scan-end scan)
                                      *wal-logical-slot-read-continuation*
                                      (logical-wal-certify-boundary
                                       selected-source (wal-retained-slot-scan-end scan))))
                              :returned))))))))))
        (error (condition) (setf failure condition)))
      (assert (and entered seen-eor))
      (assert (null *wal-logical-slot-read-continuation*))
      (if (eq mode :rewrite)
          (progn
            (assert (and failure (search "differs from its exact transcript" (princ-to-string failure))))
            (assert (not returned))
            (assert (= 17 *wal-logical-slot-read-progress*)))
          (progn
            (assert (null failure))
            (assert (eq :returned returned))
            (assert (= (wal-retained-slot-scan-end scan) *wal-logical-slot-read-progress*))))))
  (format t "~&NATIVE-RUNTIME-TAIL-MUTATIONS PASS append-after-horizon-accepted valid-crc-rewrite-rejected private-replay-unchanged progress-staged~%"))

(defun native-host-continuation-runtime-slot-resources (storage)
  (let ((escaped nil) (foreign-rejected nil))
    (multiple-value-bind (first second)
        (logical-slot-continuation-storage-call
         storage :with-slot-root
         (lambda (directory check)
           (setf escaped check)
           (assert (physical-image-file-facts directory))
           (funcall check)
           (sb-thread:join-thread
            (sb-thread:make-thread
             (lambda ()
               (handler-case (funcall check)
                 (error () (setf foreign-rejected t))))))
           (assert foreign-rejected)
           (funcall check)
           (logical-slot-continuation-storage-check storage)
           (values :slot-root :checked)))
      (assert (and (eq first :slot-root) (eq second :checked))))
    (assert (handler-case (progn (funcall escaped) nil) (error () t)))
    (logical-slot-continuation-storage-check storage)
    (format t "~&NATIVE-RUNTIME-SLOT-RESOURCES PASS thread expiry multiple-values owner-healthy~%")))

(defun native-host-continuation-runtime-storage (scan source)
  (let ((escaped nil))
    (call-with-logical-slot-continuation-runtime-storage
     scan source
     (lambda (storage facts)
       (setf escaped storage)
       (native-host-continuation-runtime-slot-resources storage)
       (call-with-logical-slot-continuation-selector
        storage
        (lambda (snapshot fd selector-facts)
          (declare (ignore fd selector-facts))
          (logical-slot-continuation-check-runtime-snapshot storage facts snapshot)
          (let ((escaped-next nil) (frames 0))
            (multiple-value-bind (first second)
                (call-with-logical-slot-continuation-runtime-tail
                 storage snapshot (wal-retained-slot-scan-end scan)
                 (lambda (next check guard)
                   (declare (ignore guard))
                   (setf escaped-next next)
                   (funcall check)
                   (loop for frame = (funcall next) while frame do
                     (assert (= (length frame) (+ 40 (wal-u32 frame 36))))
                     (incf frames))
                   (values :first :second)))
              (assert (and (eq first :first) (eq second :second))))
            (assert (plusp frames))
            (assert (handler-case (progn (funcall escaped-next) nil) (error () t))))))
       (assert (logical-slot-continuation-runtime-storage-p storage))
       (assert (eql (wal-retained-slot-scan-end scan)
                    (logical-slot-continuation-storage-call storage :end)))
       (let ((entered nil))
         (assert
          (handler-case
              (progn
                (call-with-logical-slot-continuation-selector
                 storage
                 (lambda (snapshot fd selector-facts)
                   (declare (ignore fd selector-facts))
                   (call-with-logical-slot-continuation-runtime-tail
                    storage snapshot (1+ (wal-retained-slot-scan-end scan))
                    (lambda (&rest ignored) (declare (ignore ignored)) (setf entered t)))))
                nil)
            (error () t)))
         (assert (not entered)))
       (assert (equalp facts (logical-slot-continuation-storage-call storage :facts)))
       (assert (<= (getf (getf facts :certificate) :fork)
                   (logical-slot-continuation-storage-call storage :horizon)))
       (logical-slot-continuation-storage-call
        storage :with-local-wal
        (lambda (directory check)
          (declare (ignore directory))
          (funcall check)
          (logical-slot-continuation-storage-check storage)))
       (logical-slot-continuation-storage-call
        storage :with-origin
        (lambda (archive)
          (sb-thread:with-mutex (*wal-lock*)
            (logical-wal-archive-check archive))))
       (let ((rejected nil))
         (sb-thread:join-thread
          (sb-thread:make-thread
           (lambda ()
             (handler-case (logical-slot-continuation-storage-check storage)
               (error () (setf rejected t))))))
         (assert rejected))
       (logical-slot-continuation-storage-check storage)))
    (assert (handler-case
                (progn (logical-slot-continuation-storage-check escaped) nil)
              (error () t)))))

(defun native-host-continuation-runtime-bounded-lanes (scan source)
  (let ((records 0) (escaped nil))
    (call-with-logical-slot-continuation-runtime-storage
     scan source
     (lambda (storage facts)
       (call-with-logical-slot-continuation-selector
        storage
        (lambda (snapshot fd selector-facts)
          (declare (ignore fd selector-facts))
          (logical-slot-continuation-check-runtime-snapshot storage facts snapshot)
          (logical-slot-continuation-storage-call
           storage :with-origin
           (lambda (archive)
             (logical-slot-continuation-storage-call
              storage :with-sidecar
              (lambda (reader check)
                (multiple-value-bind (owner role)
                    (sb-thread:with-mutex (*wal-lock*)
                      (logical-slot-wal-scan-start-position scan))
                  (call-with-logical-slot-continuation-lanes
                   facts snapshot (wal-retained-slot-scan-slot scan) archive reader nil
                   (lambda () (logical-slot-continuation-storage-call storage :check-under-lock))
                   (lambda (cursor bounds)
                     (setf escaped cursor)
                     (assert (= (wal-retained-slot-scan-end scan) (third bounds)))
                     (loop while (logical-wal-cursor-next cursor) do (incf records))
                     (assert (= (wal-retained-slot-scan-end scan)
                                (logical-wal-cursor-position cursor))))
                   :end (wal-retained-slot-scan-end scan)
                   :position (list (wal-retained-slot-scan-start scan) owner role)))
                (funcall check)))))))))
    (assert (handler-case (progn (logical-wal-cursor-next escaped) nil)
              (error () t)))
    (assert (if (= (wal-retained-slot-scan-start scan) (wal-retained-slot-scan-end scan))
                (zerop records) (plusp records)))
    (format t "~&NATIVE-RUNTIME-BOUNDED-LANES PASS start=~d end=~d records=~d~%"
            (wal-retained-slot-scan-start scan) (wal-retained-slot-scan-end scan) records)))

(declaim (ftype function native-host-continuation-runtime-cursor))

(defun native-host-continuation-runtime-bounded-source (ends)
  (dolist (end ends)
    (native-host-with-runtime-scan
     "alpha"
     (lambda (scan source)
       (assert (= end (wal-retained-slot-scan-end scan)))
       (native-host-continuation-runtime-storage scan source)
       (native-host-continuation-runtime-bounded-lanes scan source)
       (native-host-continuation-runtime-cursor scan source)
       (call-with-logical-slot-imported-runtime-source
        scan source
        (lambda (owner context facts archive check under-lock)
          (declare (ignore context archive))
          (assert (< end (getf (getf facts :certificate) :fork)))
          (funcall check)
          (sb-thread:with-mutex (*wal-lock*)
            (funcall under-lock)
            (assert (<= (getf (getf facts :certificate) :fork)
                        (logical-slot-imported-scan-proof-horizon-under-lock owner)))))))
     :upto end))
  (format t "~&NATIVE-RUNTIME-BOUNDED-SOURCE PASS pre-fork-and-empty-horizons=~s~%" ends))

(defun native-host-continuation-runtime-ack-fenced (scan)
  (let* ((slot (wal-retained-slot-scan-slot scan))
         (name (wal-retained-slot-scan-name scan))
         (confirmed (wal-replication-slot-confirmed-flush-lsn slot))
         (before (logical-slot-state-encode slot))
         (pending (wal-replication-slot-pending-xids slot))
         (prepared nil))
    (dolist (operation
             (list
              (lambda ()
                (sb-thread:with-mutex (*wal-lock*)
                  (wal-persist-replication-slot-ack-under-lock slot confirmed)))
              (lambda () (wal-acknowledge-standard-replication-slot name confirmed confirmed confirmed))
              (lambda () (wal-logical-slot-accept-resume-position name confirmed))
              (lambda ()
                (call-with-logical-slot-terminal-check
                 name confirmed
                 (lambda (candidate) (declare (ignore candidate)) (setf prepared t) (values t nil))
                 (lambda (&rest arguments) (declare (ignore arguments)) (error "unexpected finish"))))))
      (assert
       (handler-case (progn (funcall operation) nil)
         (error (condition)
           (search "prepared continuation publication" (princ-to-string condition))))))
    (assert (not prepared))
    (assert (eq pending (wal-replication-slot-pending-xids slot)))
    (assert (equalp before (logical-slot-state-encode slot)))
    (assert (not (wal-replication-slot-state-uncertain-p slot)))
    (format t "~&NATIVE-RUNTIME-ACK-FENCED PASS slot=~a persistence-standard-resume-prepare~%" name)))

(defun native-host-continuation-runtime-cursor (scan source)
  (native-host-continuation-runtime-ack-fenced scan)
  (let ((records 0) (escaped nil))
    (multiple-value-bind (owner role)
        (sb-thread:with-mutex (*wal-lock*) (logical-slot-wal-scan-start-position scan))
      (call-with-logical-slot-wal-cursor-from-scan
       scan owner role
       (lambda (cursor selected-source)
         (assert (and (eq (logical-wal-source-slot source) (logical-wal-source-slot selected-source))
                      (eq :stream (logical-wal-source-purpose selected-source))))
         (setf escaped cursor)
         (when (< (wal-retained-slot-scan-start scan) (wal-retained-slot-scan-end scan))
           (assert (logical-wal-record-provider-p (logical-wal-cursor-record-reader cursor))))
         (loop while (logical-wal-cursor-next cursor) do (incf records))
         (assert (= (wal-retained-slot-scan-end scan) (logical-wal-cursor-position cursor))))))
    (assert (handler-case (progn (logical-wal-cursor-next escaped) nil) (error () t)))
    (format t "~&NATIVE-RUNTIME-CURSOR PASS slot=~a start=~d end=~d records=~d~%"
            (wal-retained-slot-scan-name scan) (wal-retained-slot-scan-start scan)
            (wal-retained-slot-scan-end scan) records)))

(defun native-host-continuation-runtime-transactions (scan)
  (let ((position (wal-retained-slot-scan-start scan))
        (end (wal-retained-slot-scan-end scan))
        (xids nil) (commits nil) (changes 0) (frontiers nil) (finished nil))
    (call-with-logical-reader-spools
     (lambda ()
       (dotimes (attempt 16)
         (declare (ignorable attempt))
         (multiple-value-bind (transactions progress exhausted)
             (wal-logical-slot-read-transactions
              (wal-retained-slot-scan-name scan) :from-lsn position :upto-lsn end :max-transactions 1)
           (assert (<= position progress end))
           (assert (null *wal-logical-slot-read-continuation*))
           (dolist (transaction transactions)
             (assert (not (member (logical-transaction-commit-lsn transaction) commits)))
             (push (logical-transaction-commit-lsn transaction) commits)
             (when (plusp (logical-transaction-xid transaction))
               (assert (not (member (logical-transaction-xid transaction) xids)))
               (push (logical-transaction-xid transaction) xids))
             (logical-transaction-map-changes
              transaction (lambda (change) (declare (ignore change)) (incf changes))))
           (push progress frontiers)
           (setf position progress)
           (when exhausted (setf finished t) (return))))))
    (assert (and finished (= end position)))
    (assert (equal '(3 4) (nreverse xids)))
    (assert (<= 2 changes))
    (format t "~&NATIVE-RUNTIME-TRANSACTIONS PASS slot=~a xids=(3 4) changes=~d frontiers=~s~%"
            (wal-retained-slot-scan-name scan) changes (nreverse frontiers))))

(defun native-host-continuation-runtime-source ()
  (catalog-init)
  (let ((fds (length (directory #p"/proc/self/fd/*")))
        (records 0) (boundary nil))
    (dolist (name '("alpha" "beta"))
      (native-host-with-runtime-scan
       name
       (lambda (scan source)
         (when (string= name "alpha")
           (native-host-continuation-runtime-tail-mutations scan source))
         (native-host-continuation-runtime-cursor scan source)
         (native-host-continuation-runtime-transactions scan)
         (call-with-logical-slot-imported-runtime-source
          scan source
          (lambda (owner context facts archive check under-lock)
            (declare (ignore owner))
            (setf boundary (getf (getf facts :manifest) :end))
            (funcall check)
            (sb-thread:with-mutex (*wal-lock*)
              (funcall under-lock)
              (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*))
            (native-host-runtime-root-metadata-change context)
            (funcall check)
            (let* ((history (logical-slot-copy-source-proof-parent-history facts))
                   (start (getf (getf facts :manifest) :end))
                   (end (getf (getf facts :certificate) :fork)))
              (call-with-logical-wal-cursor
               history :timeline start end (timeline-at-point (1- start) history) :prefix-end
               under-lock
               (lambda (cursor)
                 (loop while (logical-wal-cursor-next cursor) do (incf records)))
               :archive archive :archive-only t :batch-policy :logical-reader))
            (funcall check))))))
    (native-host-continuation-runtime-bounded-source
     (list (wal-replication-slot-restart-lsn (gethash "alpha" *wal-replication-slots*)) boundary))
    (native-host-continuation-runtime-source-negative)
    (assert (plusp records))
    (assert (= fds (length (directory #p"/proc/self/fd/*"))))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (format t "~&NATIVE-RUNTIME-SOURCE PASS archive-records=~d control-replacement-accepted~%" records)))

(defun native-host-continuation-active-refusal (gate ownership)
  (let* ((entries (logical-slot-startup-read-inventory :local-workspace t))
         (registry *wal-replication-slots*)
         (*logical-capture-memory-bytes*
           (if (equal "1" (uiop:getenv "PG_NATIVE_CONTINUATION_FORCE_SPILL")) 0 *logical-capture-memory-bytes*))
         (fds (length (directory #p"/proc/self/fd/*")))
         (control (logical-slot-startup-control-image))
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
    (setf rejected
          (handler-case
              (progn (wal-load-replication-slots :gate-owner gate :ownership ownership) nil)
            (error (condition) (princ-to-string condition))))
    (assert (and rejected (search "active import reconciliation" rejected)))
    (multiple-value-bind (snapshot records)
        (logical-slot-continuation-restore-active-startup gate ownership)
      (let* ((plan (logical-slot-continuation-check snapshot))
             (selector (merge-pathnames "pg_logical_continuation/selector" *pgdata*))
             (identity (physical-image-file-facts -100 (namestring selector)))
             (rows (sort (loop for entry in entries
                               collect (list (wal-replication-slot-name (logical-slot-startup-entry-slot entry))
                                             (logical-slot-startup-entry-bytes entry)))
                         #'string< :key #'first)))
        (assert (equalp rows (getf snapshot :slots)))
        (assert (plusp records))
        (assert (= (second (getf snapshot :origin)) (logical-slot-continuation-plan-floor plan)))
        (assert (logical-slot-continuation-plan-tail-request plan))
        (assert (null (getf snapshot :roots)))
        (assert (null (getf snapshot :runs)))
        (assert (equalp snapshot (logical-slot-continuation-restore-active-startup gate ownership)))
        (assert (equal identity (physical-image-file-facts -100 (namestring selector))))
        (format t "~&NATIVE-SELECTOR-ACTIVE-REFUSAL PASS slots=~d records=~d tail=~s; stable active selector verified; loader still refuses: ~a~%"
                (length rows) records (logical-slot-continuation-plan-tail-request plan) rejected)))
    (native-host-continuation-startup-reject-slot-resources gate ownership)
    (native-host-continuation-boundary-probes gate ownership)
    (native-host-continuation-reject-caller-baseline gate ownership)
    (when (uiop:getenv "PG_NATIVE_COMPOSED_READER")
      (native-host-continuation-composed-cursors gate ownership)
      (native-host-continuation-pending-candidates gate ownership)
      (native-host-continuation-caught-reader-failure gate ownership)
      (native-host-continuation-observer-cannot-publish gate ownership))
    (assert (equalp control (logical-slot-startup-control-image)))
    (assert (eq registry *wal-replication-slots*))
    (assert (zerop (hash-table-count registry)))
    (assert (= fds (length (directory #p"/proc/self/fd/*"))))
    (assert (logical-slot-continuation-gc-fenced-p *pgdata*))
    (assert (not (probe-file (merge-pathnames "pg_logical_continuation/selector.tmp" *pgdata*))))
    (dolist (row before)
      (destructuring-bind (path facts bytes) row
        (assert (equal facts (physical-image-file-facts -100 (namestring path))))
        (assert (equalp bytes (read-bounded-regular-file path +slot-state-maximum-bytes+)))))
    (when (uiop:getenv "PG_NATIVE_STARTUP_PUBLICATION")
      (native-host-continuation-publication gate ownership)
      (assert (equalp control (logical-slot-startup-control-image)))
      (assert (= fds (length (directory #p"/proc/self/fd/*"))))
      (assert (null *catalog*)))
    (when (uiop:getenv "PG_NATIVE_RUNTIME_SOURCE")
      (native-host-continuation-runtime-source))
    t))

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
                   (when (equal "1" (uiop:getenv "PG_NATIVE_SELECTOR_ACTIVE_RESET"))
                     (logical-slot-continuation-restore-active-startup gate ownership)
                     (native-host-continuation-selector-reset gate ownership :active))
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

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
                 (let ((registry *wal-replication-slots*)
                       (fds (length (directory #p"/proc/self/fd/*")))
                       (dropped 0))
                   (assert (zerop (hash-table-count registry)))
                   (assert (null *catalog*))
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
                   (assert (eq registry *wal-replication-slots*))
                   (assert (zerop (hash-table-count registry)))
                   (assert (null (logical-slot-startup-read-inventory :local-workspace t)))
                   (assert (null *catalog*))
                   (assert (= fds (length (directory #p"/proc/self/fd/*"))))
                   (assert (null *primary-wal-writer*))
                   (format t "~&NATIVE-HOST PASS canonical permanent proof checked independently of current slots; no publication~%")))))))
      (postmaster-remove-pid-file)
      (when *postmaster-data-lock* (close *postmaster-data-lock*))
      (setf sb-ext:*exit-hooks* exit-hooks))))

(native-imported-host-observation)

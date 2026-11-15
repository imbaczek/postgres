(require :asdf)
(let ((source (truename (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*)))))
  (pushnew source asdf:*central-registry* :test #'equal)
  (asdf:load-asd (merge-pathnames "pg.asd" source))
  (asdf:load-system "pg"))
(in-package #:pg)

;; Run against the original stopped prepared child: its permanent pair is
;; inode-bound. Startup replay changes control and recovery state; this is not
;; a read-only file probe.

(defun native-imported-child-prefix (context facts)
  (let* ((certificate (getf facts :certificate))
         (fork (getf certificate :fork))
         (prefix (getf facts :prefix))
         (size (mod fork +wal-segment-size+)))
    (assert (= size (length prefix)))
    (when (plusp size)
      (call-with-logical-slot-copy-source-proof-fd
       (physical-image-context-root-fd context) "pg_wal" t
       (lambda (wal-fd wal-facts)
         (declare (ignore wal-facts))
         (call-with-logical-slot-copy-source-proof-fd
          wal-fd (wal-segment-filename (floor fork +wal-segment-size+)
                                       (getf certificate :child-timeline) :timeline) nil
          (lambda (fd observed)
            (declare (ignore observed))
            (let ((bytes (logical-slot-copy-import-control-read-fd fd +wal-segment-size+)))
              (assert (<= size (length bytes)))
              (assert (equalp prefix (subseq bytes 0 size)))))))))
    t))

(defun native-eq-table-image (table)
  (loop for key being the hash-keys of table using (hash-value value)
        collect (cons key value)))

(defun native-eq-table-image-p (image table)
  (and (= (length image) (hash-table-count table))
       (every (lambda (entry)
                (multiple-value-bind (value present) (gethash (car entry) table)
                  (and present (eq value (cdr entry)))))
              image)))

(defun native-imported-history-observation (owner context facts check under-lock references function)
  "Replay D through F and check archive and child prefixes under a real lease."
  (let* ((source (logical-slot-imported-startup-owner-source owner))
         (root (physical-image-context-root-fd context))
         (certificate (getf facts :certificate)))
    (call-with-logical-slot-copy-source-proof-fd
     root "pg_physical_retained" t
     (lambda (archive-fd archive-facts)
       (let ((input (physical-image-fd-path archive-fd)) (active t)
                                                         (thread sb-thread:*current-thread*) (mutex *wal-lock*))
         (labels ((anchors ()
                    (assert (and active (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*)))
                    (physical-image-check-context context)
                    (assert (equal archive-facts (physical-image-file-facts archive-fd)))
                    (assert (equal archive-facts (physical-image-file-facts root "pg_physical_retained")))
                    t)
                  ;; Historical replay binds simulated recovery globals. Live
                  ;; owner, pair and archive checks bracket it outside the scan;
                  ;; this reader checks only private scope, FDs and contents.
                  ;; Directory facts assume a quiescent fixture, not a runtime provider.
                  (read-object (selected kind id maximum)
                    (anchors)
                    (assert (equal selected input))
                    (assert (= maximum (ecase kind
                                         (:inventory +physical-retained-inventory-limit+)
                                         ((:extent :overlap) +physical-retained-image-limit+))))
                    (let ((bytes
                            (call-with-logical-slot-copy-source-proof-fd
                             archive-fd (file-namestring (physical-retained-object-path input kind id)) nil
                             (lambda (fd observed)
                               (declare (ignore observed))
                               (logical-slot-copy-import-control-read-fd fd maximum)))))
                      (assert (= id (logical-slot-copy-import-digest bytes)))
                      (anchors)
                      bytes)))
           (unwind-protect
                (progn
                  (funcall check)
                  (call-with-logical-wal-archive-core
                   (logical-slot-startup-source-system-id source)
                   (logical-slot-startup-source-history source)
                   (logical-slot-startup-source-namespace source)
                   (lambda () (funcall under-lock) (anchors))
                   #'physical-slot-startup-system-id
                   (lambda (archive)
                     (assert (equal (getf certificate :retained-reference)
                                    (logical-wal-archive-reference archive)))
                     (assert (equalp (logical-wal-archive-history archive)
                                     (timeline-parse-history
                                      (getf certificate :child-timeline)
                                      (bytes->string (getf (getf facts :evidence) :child-history)))))
                     (let* ((inside (native-eq-table-image *physical-retained-root-references*))
                            (readers (remove-if (lambda (entry) (assoc (car entry) references :test #'eq))
                                                inside)))
                       (assert (= (length inside) (1+ (length references))))
                       (assert (= 1 (length readers)))
                       (assert (every (lambda (entry) (eq (cdr entry) (gethash (car entry) *physical-retained-root-references*)))
                                      references))
                       (assert (eq (cdar readers)
                                   (gethash (physical-promotion-retained-owner :primary)
                                            *physical-retained-root-references*)))
                       (let ((base (getf certificate :base-end)) (fork (getf certificate :fork)))
                         (format t "~&NATIVE-HISTORY range D=~d F=~d bytes=~d~%" base fork (- fork base))
                         (assert (<= base fork))
                         (unless (equal "1" (uiop:getenv "PG_NATIVE_PROOF_ALLOW_EMPTY"))
                           (assert (< base fork))))
                       (logical-slot-copy-source-proof-scan-archive facts input #'read-object)
                       (logical-slot-copy-source-proof-check-archive-prefix facts input #'read-object)
                       (native-imported-child-prefix context facts)
                       (funcall check)
                       (sb-thread:with-mutex (*wal-lock*) (logical-wal-archive-check archive))
                       (assert (native-eq-table-image-p inside *physical-retained-root-references*))
                       (format t "~&NATIVE-HISTORY archive lease, D-through-F replay and both prefixes validated~%")
                       (funcall function archive)))))
             (setf active nil))))))))

(defun startup-proof-native-observation ()
  (let* ((*pgdata* (uiop:ensure-directory-pathname (or (uiop:getenv "PG_NATIVE_PROOF_DATA") (error "Set PG_NATIVE_PROOF_DATA to the prepared child fixture"))))
         (*print-circle* t) (*print-level* 4) (*print-length* 8)
         (*wal-level* :logical)
         (*startup-installed-configuration* nil)
         (saved-exit-hooks sb-ext:*exit-hooks*)
         (saved-owner nil) (saved-check nil) (saved-archive nil) (entered nil))
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
                 (check-primary-storage-startup-context gate ownership)
                 (physical-promotion-restore-primary-retained)
                 (format t "~&NATIVE-PROOF post-replay timeline=~d durable=~d~%"
                         *wal-insert-timeline* *wal-durable-lsn*)
                 (let ((registry *wal-replication-slots*)
                       (registry-count (hash-table-count *wal-replication-slots*))
                       (registry-image (native-eq-table-image *wal-replication-slots*))
                       (references (native-eq-table-image *physical-retained-root-references*))
                       (live-count *logical-capture-live-slots*)
                       (interests (hash-table-count *wal-logical-catalog-event-interests*))
                       (fds (length (directory #p"/proc/self/fd/*"))))
                   (assert (zerop registry-count))
                   (assert (zerop interests))
                   (assert (null *catalog*))
                   (assert
                    (eq :observed
                        (catch 'native-proof-observed
                          (call-with-logical-slot-imported-startup-source
                           gate ownership (logical-slot-startup-read-inventory :local-workspace t)
                           (lambda (source)
                             (let ((entry
                                     (find-if
                                      (lambda (entry)
                                        (wal-replication-slot-child-owner
                                         (logical-slot-startup-entry-slot entry)))
                                      (logical-slot-startup-source-entries source))))
                               (assert entry)
                               (assert (not (eq (logical-slot-startup-entry-slot entry)
                                                (gethash (wal-replication-slot-name (logical-slot-startup-entry-slot entry)) registry))))
                               (call-with-logical-slot-imported-startup-owner
                                source entry
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
                                           (native-imported-history-observation
                                            owner context facts check under-lock references
                                            (lambda (archive)
                                              (setf entered t saved-archive archive)
                                              (throw 'native-proof-observed :observed)))))))))))))))))
                   (assert entered)
                   (assert (null *catalog*))
                   (assert (native-eq-table-image-p references *physical-retained-root-references*))
                   (assert (null (logical-wal-archive-index saved-archive)))
                   (assert (null (logical-wal-archive-cache-bytes saved-archive)))
                   (assert (handler-case (progn (sb-thread:with-mutex (*wal-lock*) (logical-wal-archive-check saved-archive)) nil) (error () t)))
                   (assert (not (physical-adoption-owner-active-p saved-owner)))
                   (assert (handler-case (progn (funcall saved-check) nil) (error () t)))
                   (assert (eq registry *wal-replication-slots*))
                   (assert (= registry-count (hash-table-count registry)))
                   (assert (native-eq-table-image-p registry-image registry))
                   (assert (eql live-count *logical-capture-live-slots*))
                   (assert (= interests (hash-table-count *wal-logical-catalog-event-interests*)))
                   (assert (= fds (length (directory #p"/proc/self/fd/*"))))
                   (assert (null *primary-wal-writer*))
                   (format t "~&NATIVE-PROOF PASS observation only; registry/mirrors unchanged; scopes expired; descriptors released~%")))))))
      (postmaster-remove-pid-file)
      (when *postmaster-data-lock* (close *postmaster-data-lock*))
      (setf sb-ext:*exit-hooks* saved-exit-hooks))))

(startup-proof-native-observation)

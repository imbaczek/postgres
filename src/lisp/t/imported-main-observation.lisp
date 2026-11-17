(require :asdf)
(let ((source (truename (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*)))))
  (pushnew source asdf:*central-registry* :test #'equal)
  (asdf:load-asd (merge-pathnames "pg.asd" source))
  (asdf:load-system "pg")
  (asdf:load-system "rove"))

(in-package #:pg)

(defun native-imported-main-with-root (function)
  (call-with-physical-adoption-owner
   *pgdata*
   (lambda (owner)
     (call-with-physical-directory-root
      owner
      (lambda (root path)
        (declare (ignore path))
        (let ((mount (physical-adoption-install-mount-id nil :directory-fd root)))
          (flet ((check ()
                   (physical-directory-check-owner owner)
                   (physical-image-check-target *pgdata* root)
                   (assert (= mount
                              (physical-adoption-install-mount-id *pgdata*)
                              (physical-adoption-install-mount-id nil :directory-fd root)
                              (physical-adoption-install-mount-id
                               nil :directory-fd
                               (sb-sys:fd-stream-fd (physical-adoption-owner-lock owner)))))
                   t))
            (check)
            (multiple-value-prog1 (funcall function root #'check)
              (check)))))))))

(defun native-imported-main-with-history-read-fault (name enabled function)
  (let ((original (symbol-function 'logical-slot-copy-import-control-read-fd))
        (injected 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'logical-slot-copy-import-control-read-fd)
                 (lambda (fd maximum)
                   (let ((bytes (funcall original fd maximum)))
                     (if (and (funcall enabled)
                              (equal name
                                     (file-namestring
                                      (sb-posix:readlink (format nil "/proc/self/fd/~d" fd)))))
                         (let ((changed (copy-seq bytes)))
                           (incf injected)
                           (setf (aref changed 0) (logxor #xff (aref changed 0)))
                           changed)
                         bytes))))
           (funcall function (lambda () injected)))
      (setf (symbol-function 'logical-slot-copy-import-control-read-fd) original))))

(defun native-imported-main-with-checkpoint-read-fault (name offset function)
  (let ((original (symbol-function 'logical-slot-copy-import-control-read-fd))
        (injected 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'logical-slot-copy-import-control-read-fd)
                 (lambda (fd maximum)
                   (multiple-value-bind (bytes facts) (funcall original fd maximum)
                     (when (equal name
                                  (file-namestring
                                   (sb-posix:readlink (format nil "/proc/self/fd/~d" fd))))
                       (setf bytes (copy-seq bytes))
                       (incf injected)
                       (setf (aref bytes offset) (logxor #xff (aref bytes offset))))
                     (values bytes facts))))
           (funcall function (lambda () injected)))
      (setf (symbol-function 'logical-slot-copy-import-control-read-fd) original))))

(defun native-imported-main-checkpoint-contract (root root-check)
  (call-with-logical-slot-imported-startup-pair
   root root-check
   (lambda (image facts pair-check)
     (declare (ignore facts))
     (let* ((fields (multiple-value-list (wal-read-control-fields)))
            (fds (length (directory #p"/proc/self/fd/*")))
            (saved-check nil) (called nil))
       (rove:ok
        (equal '(:observed 17)
               (multiple-value-list
                (call-with-logical-slot-imported-startup-checkpoint
                 root image pair-check
                 (lambda (view check)
                   (setf saved-check check called t)
                   (rove:ok (= (getf (seventh fields) :system-id) (getf view :system-id)))
                   (rove:ok (= (fourth fields) (getf view :timeline)))
                   (rove:ok (= (second fields) (getf view :durable)))
                   (rove:ok (= 5 (length (getf view :inputs))))
                   (rove:ok (= (+ fds 5) (length (directory #p"/proc/self/fd/*"))))
                   (dolist (key '(:control :history :checkpoint))
                     (setf (aref (getf view key) 0)
                           (logxor #xff (aref (getf view key) 0))))
                   (setf (getf view :system-id) 0 (getf view :durable) 0)
                   (setf (char (first (first (getf view :inputs))) 0) #\x)
                   (incf (second (second (first (getf view :inputs)))))
                   (incf (second (getf view :root-facts)))
                   (rove:ok (funcall check))
                   (let ((rejected nil))
                     (sb-thread:join-thread
                      (sb-thread:make-thread
                       (lambda ()
                         (setf rejected
                               (handler-case (progn (funcall check) nil) (error () t))))))
                     (rove:ok rejected))
                   (rove:ok (funcall check))
                   (values :observed 17))))))
       (rove:ok called)
       (rove:ok (rove:signals (funcall saved-check) 'error))
       (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
       (rove:ok
        (rove:signals
         (call-with-logical-slot-imported-startup-checkpoint
          root image pair-check
          (lambda (view check)
            (declare (ignore view))
            (setf saved-check check)
            (error "deliberate checkpoint callback failure")))
         'error))
       (rove:ok (rove:signals (funcall saved-check) 'error))
       (let ((reject nil))
         (rove:ok
          (rove:signals
           (call-with-logical-slot-imported-startup-checkpoint
            root image
            (lambda ()
              (funcall pair-check)
              (when reject (error "deliberate checkpoint root-check failure")))
            (lambda (view check)
              (declare (ignore view))
              (setf reject t)
              (rove:ok (rove:signals (funcall check) 'error))
              (setf reject nil)
              (rove:ok (rove:signals (funcall check) 'error))))
           'error)))
       (let* ((names '(wal-read-control-fields physical-promotion-read-file physical-promotion-open-file))
              (originals (mapcar #'symbol-function names)))
         (unwind-protect
              (progn
                (dolist (name names)
                  (setf (symbol-function name)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (error "checkpoint observer attempted pathname-based I/O"))))
                (rove:ok
                 (eq :held
                     (call-with-logical-slot-imported-startup-checkpoint
                      root image pair-check
                      (lambda (view check)
                        (declare (ignore view))
                        (rove:ok (funcall check))
                        :held)))))
           (loop for name in names for original in originals
                 do (setf (symbol-function name) original))))
       (let ((history-calls 0) (record-calls 0))
         (rove:ok
          (rove:signals
           (%physical-promotion-check-child-checkpoint
            (fourth fields) (1+ (fifth fields)) (lambda () (values-list fields))
            (lambda (&rest arguments)
              (declare (ignore arguments)) (incf history-calls))
            (lambda (&rest arguments)
              (declare (ignore arguments)) (incf record-calls))
            #'values)
           'error))
         (rove:ok (zerop history-calls))
         (rove:ok (zerop record-calls))
         (rove:ok
          (rove:signals
           (%physical-promotion-check-child-checkpoint
            (fourth fields) (fifth fields) (lambda () (values-list fields))
            (lambda (path consume)
              (declare (ignore path))
              (incf history-calls)
              (funcall consume (make-array 0 :element-type '(unsigned-byte 8))))
            (lambda (&rest arguments)
              (declare (ignore arguments)) (incf record-calls))
            #'values)
           'error))
         (rove:ok (= history-calls 1))
         (rove:ok (zerop record-calls)))
       (let ((original (symbol-function 'physical-promotion-open-file)) (opened nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'physical-promotion-open-file)
                      (lambda (&rest arguments)
                        (multiple-value-bind (stream size) (apply original arguments)
                          (setf opened stream)
                          (values stream size))))
                (rove:ok
                 (rove:signals
                  (call-with-physical-promotion-child-checkpoint-record
                   (wal-segment-path (floor (getf (third fields) :start) +wal-segment-size+)
                                     (fourth fields) :timeline)
                   (third fields)
                   (lambda (bytes)
                     (rove:ok (open-stream-p opened))
                     (rove:ok (= (length bytes) (wal-checkpoint-record-size (third fields))))
                     (error "deliberate checkpoint consumer failure")))
                  'error))
                (rove:ok (not (open-stream-p opened))))
           (setf (symbol-function 'physical-promotion-open-file) original)))
       (dolist (case
                   (list (list "pg_wal_control" 0)
                         (list (timeline-history-filename (fourth fields)) 0)
                         (list (wal-segment-filename
                                (floor (getf (third fields) :start) +wal-segment-size+)
                                (fourth fields) :timeline)
                               (mod (getf (third fields) :start) +wal-segment-size+))))
         (native-imported-main-with-checkpoint-read-fault
          (first case) (second case)
          (lambda (injected)
            (let ((entered nil))
              (rove:ok
               (rove:signals
                (call-with-logical-slot-imported-startup-checkpoint
                 root image pair-check
                 (lambda (view check)
                   (declare (ignore view check)) (setf entered t)))
                'error))
              (rove:ok (not entered))
              (rove:ok (plusp (funcall injected)))))))
       (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))))))

(defun native-imported-main-checkpoint-replacement-contract (root root-check)
  ;; This explicit destructive mode restores inode, bytes and permissions.
  ;; Rename changes timestamps; the driver records that narrower preservation.
  (let* ((path (merge-pathnames "global/pg_wal_control" *pgdata*))
         (saved (merge-pathnames
                 (format nil "global/.checkpoint-probe-original-~d" (sb-posix:getpid)) *pgdata*))
         (copy (merge-pathnames
                (format nil "global/.checkpoint-probe-copy-~d" (sb-posix:getpid)) *pgdata*))
         (bytes (read-bounded-regular-file path (1+ +wal-control-maximum-bytes+)))
         (inode (sb-posix:stat-ino (sb-posix:lstat path)))
         (moved nil) (installed nil) (entered nil) (saved-check nil))
    (when (or (wal-namespace-file-stat saved t) (wal-namespace-file-stat copy t))
      (error "checkpoint replacement probe names already exist"))
    (unwind-protect
         (progn
           (with-open-file (out copy :direction :output :if-exists :error
                                     :element-type '(unsigned-byte 8))
             (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
             (write-sequence bytes out))
           (call-with-logical-slot-imported-startup-pair
            root root-check
            (lambda (image facts check)
              (declare (ignore facts))
              (rove:ok
               (rove:signals
                (call-with-logical-slot-imported-startup-checkpoint
                 root image check
                 (lambda (view current-check)
                   (declare (ignore view))
                   (setf entered t saved-check current-check)
                   (sb-sys:without-interrupts
                     (sb-posix:rename path saved)
                     (setf moved t)
                     (sb-posix:rename copy path)
                     (setf installed t))
                   (rove:ok (rove:signals (funcall current-check) 'error))))
                'error))))
           (rove:ok entered)
           (rove:ok (rove:signals (funcall saved-check) 'error)))
      (sb-sys:without-interrupts
        (when installed
          (sb-posix:rename path copy)
          (setf installed nil))
        (when moved
          (sb-posix:rename saved path)
          (setf moved nil))
        (when (wal-namespace-file-stat copy t) (delete-file copy))))
    (rove:ok (= inode (sb-posix:stat-ino (sb-posix:lstat path))))
    (rove:ok (equalp bytes (read-bounded-regular-file path (1+ +wal-control-maximum-bytes+))))))

(defun native-imported-main-with-pair-replacement (name function)
  ;; Own an exclusive scratch directory, restore originals before deleting copies.
  (let* ((path (merge-pathnames name *pgdata*))
         (directory-p (equal name "logical-slot-copy.install"))
         (temporary (merge-pathnames (format nil "pair-replacement-~d-~d/"
                                             (sb-posix:getpid) (random 1000000000))
                                     (uiop:temporary-directory)))
         (saved (merge-pathnames "original" temporary))
         (copy (merge-pathnames "replacement" temporary))
         (moved nil) (installed nil))
    (labels ((copy-file (source target)
               (with-open-file (out target :direction :output :if-exists :error
                                           :element-type '(unsigned-byte 8))
                 (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
                 (write-sequence (read-bounded-regular-file source
                                                            (logical-slot-copy-source-proof-maximum-bytes)) out)))
             (install ()
               (when (or moved installed) (error "replacement already installed"))
               (sb-sys:without-interrupts
                 (sb-posix:rename path saved) (setf moved t)
                 (sb-posix:rename copy path) (setf installed t))))
      (sb-posix:mkdir temporary #o700)
      (unwind-protect
           (progn
             (if directory-p
                 (progn
                   (sb-posix:mkdir copy #o700)
                   (dolist (leaf '("journal" "active"))
                     (copy-file (merge-pathnames leaf (uiop:ensure-directory-pathname path))
                                (merge-pathnames leaf (uiop:ensure-directory-pathname copy)))))
                 (copy-file path copy))
             (funcall function #'install))
        (sb-sys:without-interrupts
          (when installed (sb-posix:rename path copy) (setf installed nil))
          (when moved (sb-posix:rename saved path) (setf moved nil)))
        (uiop:delete-directory-tree temporary :validate t :if-does-not-exist :ignore)))))

(defun native-imported-main-pid-boundary-contract ()
  ;; Exercise real flock/PID publication and refuse changed bytes or identities.
  (rove:ok (null *postmaster-data-lock*))
  (rove:ok (null *postmaster-pid-stream*))
  (unless (null (slot-state-lstat (merge-pathnames "postmaster.pid" *pgdata*)))
    (error "PID-boundary fixture must initially have no PID file"))
  (let ((fds (length (directory #p"/proc/self/fd/*"))))
    (dolist (fault '(nil :proof :file :root
                     "logical-slot-copy.source-proof" "logical-slot-copy.import"
                     "logical-slot-copy.install/journal" "logical-slot-copy.install/active"
                     "logical-slot-copy.install"))
      (flet ((run (install)
               (let ((before nil) (after nil) (phases nil) (message nil))
                 (unwind-protect
                      (handler-case
                          (postmaster-own-data-directory
                           "127.0.0.1" 54321
                           :startup-observer
                           (lambda (lock phase)
                             (push phase phases)
                             (ecase phase
                               (:before-pid
                                (setf before (logical-slot-imported-startup-observation lock phase))
                                (let ((inputs (getf before :inputs)))
                                  (case fault
                                    (:proof (setf (aref (getf inputs :proof) 0)
                                                  (logxor #xff (aref (getf inputs :proof) 0))))
                                    (:file (setf (second (second (first (getf inputs :pair)))) -1))
                                    (:root (setf (second (getf inputs :root)) -1))))
                                (when install (funcall install)))
                               (:owned-pid
                                (setf after
                                      (logical-slot-imported-startup-observation lock phase before))))))
                        (error (condition) (setf message (princ-to-string condition))))
                   (unwind-protect
                        (when *postmaster-pid-stream* (postmaster-remove-pid-file))
                     (when *postmaster-data-lock* (close *postmaster-data-lock*))
                     (setf *postmaster-data-lock* nil *postmaster-start-timestamp* nil)))
                 (rove:ok (equal '(:owned-pid :before-pid) phases))
                 (if fault
                     (progn
                       (rove:ok (and message (search "changed across PID publication" message)))
                       (rove:ok (null after)))
                     (progn
                       (rove:ok (null message))
                       (rove:ok (and after (eq :owned-pid (getf after :phase))))
                       (rove:ok (logical-slot-copy-committed-image-equal-p
                                 (getf before :inputs) (getf after :inputs)))))
                 (rove:ok (null (slot-state-lstat (postmaster-data-path "postmaster.pid"))))
                 (rove:ok (null *postmaster-data-lock*))
                 (rove:ok (not *wal-ready*))
                 (rove:ok (= fds (length (directory #p"/proc/self/fd/*")))))))
        (if (stringp fault)
            (native-imported-main-with-pair-replacement fault #'run)
            (run nil))))))

(defun native-imported-main-pair-projection-error-contract (root root-check)
  (rove:ok
   (rove:signals
    (call-with-logical-slot-imported-startup-pair
     root root-check
     (lambda (image facts check)
       (declare (ignore image facts))
       (rove:ok (rove:signals (funcall check :unsupported-projection) 'error))
       (rove:ok (rove:signals (funcall check) 'error))
       (rove:ok (rove:signals (funcall check :file-facts) 'error))))
    'error)))

(defun native-imported-main-pair-replacement-contract (root root-check)
  ;; Both fresh scopes accept canonical bytes, but their inode evidence differs.
  (dolist (name '("logical-slot-copy.source-proof" "logical-slot-copy.import"
                  "logical-slot-copy.install/journal" "logical-slot-copy.install/active"
                  "logical-slot-copy.install"))
    (native-imported-main-with-pair-replacement
     name
     (lambda (install)
       (flet ((observe ()
                (call-with-logical-slot-imported-startup-pair
                 root root-check
                 (lambda (image facts check)
                   (declare (ignore facts))
                   (list image (funcall check :file-facts))))))
         (let ((before (observe)))
           (funcall install)
           (let* ((after (observe))
                  (old (assoc name (second before) :test #'equal))
                  (new (assoc name (second after) :test #'equal)))
             (rove:ok (equalp (first before) (first after)))
             (rove:ok (not (equal (second before) (second after))))
             (rove:ok (and old new (/= (second (second old)) (second (second new))))))))))))

(defun native-imported-main-history-contract (root root-check)
  (let ((saved-check nil)
        (expected (or (uiop:getenv "PG_NATIVE_IMPORTED_HISTORY_PROFILE") "original")))
    (unless (member expected '("original" "retired") :test #'equal)
      (error "unknown native imported history profile"))
    (call-with-logical-slot-imported-startup-pair
     root root-check
     (lambda (image facts pair-check)
       (declare (ignore facts))
       (let ((names
               (append '("logical-slot-copy.descriptor" "logical-slot-copy.base-verified"
                         "logical-slot-copy.evidence")
                       (when (equal expected "original")
                         '("physical-backup.manifest" "physical-recovery.control" "recovery.required")))))
         (rove:ok
          (eq :history
              (call-with-logical-slot-imported-startup-history
               root image pair-check
               (lambda (profile snapshot check)
                 (rove:ok (eq profile (if (equal expected "original") :original :retired)))
                 (rove:ok (equal names (mapcar #'first snapshot)))
                 (setf saved-check check)
                 (let* ((name (caar snapshot)) (initial (char name 0)))
                   (unwind-protect
                        (progn
                          (setf (char name 0) #\x)
                          (rove:ok (funcall check)))
                     (setf (char name 0) initial)))
                 (setf (first (second (first snapshot))) 0
                       (aref (third (first snapshot)) 0)
                       (logxor #xff (aref (third (first snapshot)) 0)))
                 (rove:ok (funcall check))
                 (let ((rejected nil))
                   (sb-thread:join-thread
                    (sb-thread:make-thread
                     (lambda ()
                       (setf rejected
                             (handler-case (progn (funcall check) nil) (error () t))))))
                   (rove:ok rejected))
                 :history))))
         (rove:ok (rove:signals (funcall saved-check) 'error))
         (rove:ok
          (rove:signals
           (call-with-logical-slot-imported-startup-history
            root image pair-check
            (lambda (profile snapshot check)
              (declare (ignore profile snapshot))
              (setf saved-check check)
              (error "deliberate historical observation callback failure")))
           'error))
         (rove:ok (rove:signals (funcall saved-check) 'error))
         (dolist (name names)
           (native-imported-main-with-history-read-fault
            name (constantly t)
            (lambda (injected)
              (let ((called nil))
                (rove:ok
                 (rove:signals
                  (call-with-logical-slot-imported-startup-history
                   root image pair-check
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (setf called t)))
                  'error))
                (rove:ok (not called))
                (rove:ok (= 1 (funcall injected)))))))
         (let ((enabled nil))
           (native-imported-main-with-history-read-fault
            (first names) (lambda () enabled)
            (lambda (injected)
              (rove:ok
               (rove:signals
                (call-with-logical-slot-imported-startup-history
                 root image pair-check
                 (lambda (profile snapshot check)
                   (declare (ignore profile snapshot))
                   (setf enabled t)
                   (rove:ok (rove:signals (funcall check) 'error))
                   (setf enabled nil)
                   (rove:ok (rove:signals (funcall check) 'error))))
                'error))
              (rove:ok (= 1 (funcall injected)))))))))))

(defun native-imported-main-sidecar-contract (root root-check)
  (call-with-logical-slot-copy-source-proof-fd
   root "logical_slot_wal" t
   (lambda (sidecar facts)
     (call-with-physical-image-owned-context
      root t
      (lambda (context)
        (rove:ok (/= sidecar (physical-image-duplicate-child context "logical_slot_wal" sidecar)))
        (let ((fds (length (directory #p"/proc/self/fd/*"))))
          (rove:ok (rove:signals
                    (physical-image-duplicate-child context "logical_slot_wal" sidecar)
                    'error))
          (rove:ok (equal facts (physical-image-file-facts sidecar)))
          (rove:ok (= fds (length (directory #p"/proc/self/fd/*")))))))))
  (call-with-logical-slot-imported-startup-pair
   root root-check
   (lambda (image facts pair-check)
     (let ((saved-check nil)
           (pieces (logical-slot-copy-descriptor-wal-pieces (getf facts :descriptor))))
       (call-with-logical-slot-imported-startup-history
        root image pair-check
        (lambda (profile history history-check)
          (declare (ignore profile history))
          (rove:ok
           (eq :sidecar
               (call-with-logical-slot-imported-startup-sidecar
                root image history-check
                (lambda (snapshot check)
                  (setf saved-check check)
                  (rove:ok (= (length pieces) (length snapshot)))
                  (rove:ok
                   (equal (mapcar (lambda (piece)
                                    (logical-slot-copy-piece-path (first piece) (second piece)))
                                  pieces)
                          (mapcar #'first snapshot)))
                  (let ((row (first snapshot)))
                    (rove:ok row)
                    (setf (char (first row) 0) #\x
                          (second row) 0 (third row) 0 (first (fourth row)) 0))
                  (rove:ok (funcall check))
                  (rove:ok (eq t (funcall check nil)))
                  (let* ((directory (funcall check :directory-facts))
                         (before (copy-list directory)))
                    (rove:ok (= 9 (length directory)))
                    (rove:ok (equal directory (physical-image-file-facts root "logical_slot_wal")))
                    (setf (first directory) -1)
                    (rove:ok (equal before (funcall check :directory-facts))))
                  (let ((rejected nil))
                    (sb-thread:join-thread
                     (sb-thread:make-thread
                      (lambda ()
                        (setf rejected
                              (handler-case (progn (funcall check :directory-facts) nil) (error () t))))))
                    (rove:ok rejected))
                  :sidecar))))
          (rove:ok (rove:signals (funcall saved-check :directory-facts) 'error))
          (rove:ok (rove:signals (funcall saved-check) 'error))
          (rove:ok
           (rove:signals
            (call-with-logical-slot-imported-startup-sidecar
             root image history-check
             (lambda (snapshot check)
               (declare (ignore snapshot))
               (setf saved-check check)
               (error "deliberate sidecar callback failure")))
            'error))
          (rove:ok (rove:signals (funcall saved-check) 'error))
          (rove:ok
           (rove:signals
            (call-with-logical-slot-imported-startup-sidecar
             root image history-check
             (lambda (snapshot check)
               (declare (ignore snapshot))
               (rove:ok (rove:signals (funcall check :unsupported) 'error))
               (rove:ok (rove:signals (funcall check) 'error))
               (rove:ok (rove:signals (funcall check :directory-facts) 'error))))
            'error))
          (let ((original (symbol-function 'physical-image-file-facts))
                (names (mapcar (lambda (piece)
                                 (wal-segment-filename (second piece) (first piece) :timeline))
                               pieces))
                (injected 0) (called nil))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'physical-image-file-facts)
                         (lambda (fd &optional (name ""))
                           (let* ((facts (funcall original fd name))
                                  (leaf (file-namestring
                                         (if (plusp (length name)) name
                                             (sb-posix:readlink (format nil "/proc/self/fd/~d" fd))))))
                             (when (member leaf names :test #'equal)
                               (incf injected)
                               (incf (ninth facts) 1000000))
                             facts)))
                   (rove:ok
                    (rove:signals
                     (call-with-logical-slot-imported-startup-sidecar
                      root image history-check
                      (lambda (&rest arguments)
                        (declare (ignore arguments))
                        (setf called t)))
                     'error))
                   (rove:ok (not called))
                   (rove:ok (plusp injected)))
              (setf (symbol-function 'physical-image-file-facts) original)))
          (dolist (kind '(:size :digest :identity :mount))
            (let ((original (symbol-function 'logical-slot-copy-file-row))
                  (injected 0) (called nil))
              (unwind-protect
                   (progn
                     (setf (symbol-function 'logical-slot-copy-file-row)
                           (lambda (context relative &optional limit)
                             (let ((row (funcall original context relative limit)))
                               (when (and (zerop injected) (eql 0 (search "logical_slot_wal/" relative)))
                                 (incf injected)
                                 (ecase kind
                                   (:size (incf (second row)))
                                   (:digest (setf (third row) (logxor 1 (third row))))
                                   (:identity (setf (first (fourth row)) 0))
                                   (:mount (setf (ninth (fourth row)) 0))))
                               row)))
                     (rove:ok
                      (rove:signals
                       (call-with-logical-slot-imported-startup-sidecar
                        root image history-check
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (setf called t)))
                       'error))
                     (rove:ok (not called))
                     (rove:ok (= 1 injected)))
                (setf (symbol-function 'logical-slot-copy-file-row) original))))))))))

(rove:deftest imported-main-pair-observer-is-read-only-and-scoped-before-wal
    (let* ((started (get-internal-real-time))
           (*pgdata* (uiop:ensure-directory-pathname
                      (or (uiop:getenv "PG_NATIVE_PROOF_DATA")
                          (error "Set PG_NATIVE_PROOF_DATA to the owned certified fixture"))))
           (fds (length (directory #p"/proc/self/fd/*")))
           (saved-check nil)
           (called 0))
      (when (boundp 'cl-user::*native-imported-main-bootstrap-start*)
        (format t "~&IMPORTED-MAIN-OBSERVATION-PHASE setup wall=~,6f~%"
                (/ (- started (symbol-value 'cl-user::*native-imported-main-bootstrap-start*))
                   internal-time-units-per-second)))
      (rove:ok (not *wal-ready*))
      (rove:ok (zerop (hash-table-count *wal-replication-slots*)))
      (native-imported-main-with-root
       (lambda (root root-check)
         (native-imported-main-history-contract root root-check)
         (native-imported-main-sidecar-contract root root-check)
         (native-imported-main-checkpoint-contract root root-check)
         (native-imported-main-pair-projection-error-contract root root-check)
         (when (equal (uiop:getenv "PG_NATIVE_IMPORTED_CHECKPOINT_REPLACE") "1")
           (native-imported-main-checkpoint-replacement-contract root root-check)
           (native-imported-main-pair-replacement-contract root root-check))
         (rove:ok
          (equal
           '(:observed 17)
           (multiple-value-list
            (call-with-logical-slot-imported-startup-pair
             root root-check
             (lambda (image facts check)
               (incf called)
               (setf saved-check check)
               (rove:ok (equalp image
                                (read-bounded-regular-file
                                 (merge-pathnames "logical-slot-copy.source-proof" *pgdata*)
                                 (logical-slot-copy-source-proof-maximum-bytes))))
               (rove:ok (eql (sb-posix:stat-ino (sb-posix:fstat root))
                             (getf (getf facts :certificate) :inode)))
               (setf (getf (getf facts :certificate) :inode) 0
                     (aref image 0) (logxor #xff (aref image 0)))
               (rove:ok (funcall check))
               (rove:ok (eq t (funcall check nil)))
               (let* ((projection (funcall check :file-facts))
                      (before (logical-slot-copy-clone-facts projection)))
                 (rove:ok (equal (mapcar #'first projection)
                                 '("logical-slot-copy.source-proof" "logical-slot-copy.import"
                                   "logical-slot-copy.install" "logical-slot-copy.install/journal"
                                   "logical-slot-copy.install/active")))
                 (rove:ok (every (lambda (row) (and (= 2 (length row))
                                                    (= 9 (length (second row))))) projection))
                 (setf (aref (first (first projection)) 0) #\X
                       (first (second (first projection))) -1)
                 (rove:ok (equal before (funcall check :file-facts))))
               (let ((rejected nil))
                 (sb-thread:join-thread
                  (sb-thread:make-thread
                   (lambda ()
                     (setf rejected
                           (handler-case (progn (funcall check :file-facts) nil) (error () t))))))
                 (rove:ok rejected))
               (values :observed 17))))))
         (rove:ok (rove:signals (funcall saved-check) 'error))
         (rove:ok (rove:signals (funcall saved-check :file-facts) 'error))
         (rove:ok
          (rove:signals
           (call-with-logical-slot-imported-startup-pair
            root root-check
            (lambda (image facts check)
              (declare (ignore image facts))
              (incf called)
              (setf saved-check check)
              (error "deliberate imported observation callback failure")))
           'error))
         (rove:ok (rove:signals (funcall saved-check) 'error))
         (let ((reject nil))
           (rove:ok
            (rove:signals
             (call-with-logical-slot-imported-startup-pair
              root
              (lambda ()
                (funcall root-check)
                (when reject (error "deliberate root-check failure")))
              (lambda (image facts check)
                (declare (ignore image facts))
                (incf called)
                (setf reject t)
                (rove:ok (rove:signals (funcall check :file-facts) 'error))
                (setf reject nil)
                (rove:ok (rove:signals (funcall check :file-facts) 'error))))
             'error)))
         (let ((directory *pgdata*))
           (rove:ok
            (rove:signals
             (call-with-logical-slot-imported-startup-pair
              root
              (lambda () (let ((*pgdata* directory)) (funcall root-check)))
              (lambda (image facts check)
                (declare (ignore image facts))
                (incf called)
                (let ((*pgdata* (merge-pathnames "missing-startup-root/" directory)))
                  (rove:ok (rove:signals (funcall check) 'error)))
                (rove:ok (rove:signals (funcall check) 'error))))
             'error)))))
      (when (equal (uiop:getenv "PG_NATIVE_IMPORTED_CHECKPOINT_REPLACE") "1")
        (handler-bind ((error (lambda (condition)
                                (format *error-output* "~&IMPORTED-PID-BOUNDARY-ERROR ~a~%" condition))))
          (native-imported-main-pid-boundary-contract)))
      (rove:ok (= called 4))
      (rove:ok (not *wal-ready*))
      (rove:ok (zerop (hash-table-count *wal-replication-slots*)))
      (rove:ok (= fds (length (directory #p"/proc/self/fd/*"))))
      (format t "~&IMPORTED-MAIN-OBSERVATION-PHASE execution wall=~,6f~%"
              (/ (- (get-internal-real-time) started) internal-time-units-per-second))))

(unless (rove:run-test 'imported-main-pair-observer-is-read-only-and-scoped-before-wal)
  (error "imported main observation tests failed"))
(format t "~&IMPORTED-MAIN-OBSERVATION PASS~%")

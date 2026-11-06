;;; Archive the install journal under the held retirement owner and publish
;;; its completed progress.  Resume from a checked intermediate layout.
(in-package #:pg)

;;; The shared archive step handles the install and attempt journals in order.
(defun physical-reseed-follow-retirement-archive-stable-proof (proof step operation)
  ;; Project the facts allowed to change for one link or unlink.  The caller
  ;; compares the remaining facts across complete observations.
  (let* ((copy (physical-adoption-attempt-value-copy proof))
         (layout (getf copy :layout))
         (names (getf layout :names))
         (key (if (zerop step) :install :attempt))
         (name (if (zerop step) "physical-reseed.install" "physical-reseed.attempt"))
         (file (getf copy key))
         (identity (copy-list (getf file :identity)))
         (row (copy-list (getf file :row))))
    (remf copy :location)
    (remf file :location)
    (remf file :alias-row)
    (setf (getf copy :prefix) nil
          (nth (if (zerop step) 3 4) identity) 1
          (fifth row) 1
          (getf file :identity) (butlast identity)
          (getf file :row) (butlast row)
          (getf names :root) (remove name (getf names :root) :test #'equal)
          (getf names :saved) (remove name (getf names :saved) :test #'equal)
          (getf layout :directories)
          (loop for (role . facts) in (getf layout :directories)
                collect (cons role (if (eq role (ecase operation (:link :saved) (:unlink :root)))
                                       (subseq facts 0 6) facts)))
          (getf copy key)
          (sort (loop for (field value) on file by #'cddr collect (cons field value))
                #'string< :key (lambda (pair) (symbol-name (car pair)))))
    copy))

(defun physical-reseed-follow-retirement-check-archive-bridge (before after step operation)
  ;; Check the expected source, alias and saved transitions and compare the
  ;; frozen facts across each namespace operation.
  (unless (typep step '(integer 0 1)) (error "invalid archive bridge step"))
  (let* ((key (if (zerop step) :install :attempt))
         (link (eq operation :link))
         (before-alias (not link))
         (after-alias link))
    (unless (and (member operation '(:link :unlink))
                 (eql step (getf before :prefix))
                 (eql (if link step (1+ step)) (getf after :prefix))
                 (eql before-alias (eq :alias (getf before :location)))
                 (eql after-alias (eq :alias (getf after :location)))
                 (loop for proof in (list before after)
                       for alias in (list before-alias after-alias)
                       for file = (getf proof key)
                       for row = (getf file :row)
                       always (and (= (if alias 2 1) (fifth row)
                                      (nth (if (zerop step) 3 4) (getf file :identity)))
                                   (eql alias (eq :alias (getf file :location)))
                                   (if alias (equal row (getf file :alias-row))
                                       (null (getf file :alias-row))))))
      (error "archive bridge has an invalid link or prefix transition"))
    (unless (equal (physical-reseed-follow-retirement-archive-stable-proof before step operation)
                   (physical-reseed-follow-retirement-archive-stable-proof after step operation))
      (error "archive bridge changed frozen nonjournal state")))
  t)

(defstruct (physical-reseed-follow-retirement-archive-witness (:conc-name retirement-archive-))
  ;; This private, thread-bound witness holds the file stream, proof and phase
  ;; for one archive operation and its progress publication.
  intent thread active-p binding step prefix alias phase proof root-row stream row bytes final-snapshot)

(defmethod print-object ((witness physical-reseed-follow-retirement-archive-witness) stream)
  (physical-libpq-print-private-object witness stream 0))

(defun physical-reseed-follow-retirement-archive-directory (witness role)
  (physical-reseed-follow-configuration-role-fd
   (retirement-intent-descriptors (retirement-archive-intent witness)) role))

(defun physical-reseed-follow-retirement-archive-check-outer (witness &optional phases)
  ;; Check the archive witness, thread, held stream and frozen intent binding
  ;; for the lifetime of the outer archive scope.
  (unless (and (physical-reseed-follow-retirement-archive-witness-p witness)
               (retirement-archive-active-p witness)
               (eq (retirement-archive-thread witness) sb-thread:*current-thread*)
               (streamp (retirement-archive-stream witness))
               (open-stream-p (retirement-archive-stream witness)))
    (error "retirement archive witness ended or changed thread"))
  (let ((intent (retirement-archive-intent witness)))
    (unless (and (equal (retirement-archive-binding witness)
                        (physical-reseed-follow-retirement-intent-binding intent))
                 (eql (retirement-archive-step witness)
                      (getf (getf (getf (retirement-intent-snapshot intent) :record) :retirement) :step)))
      (error "retirement archive changed its publication binding")))
  (when (and phases (not (member (retirement-archive-phase witness) phases)))
    (error "retirement archive requires phase ~s" phases))
  t)

(defun physical-reseed-follow-retirement-archive-check-scope (witness &optional phases)
  ;; Use the intent scope during publication and the durable scope after the
  ;; archive reaches done.  The outer check keeps the archive binding fixed.
  (physical-reseed-follow-retirement-archive-check-outer witness phases)
  (let ((intent (retirement-archive-intent witness)))
    (if (eq :done (retirement-archive-phase witness))
        (physical-reseed-follow-retirement-intent-durable-scope
         intent (retirement-archive-final-snapshot witness)
         (lambda () (physical-reseed-follow-retirement-archive-check-outer witness '(:done))))
        (physical-reseed-follow-retirement-intent-check-scope intent)))
  t)

(defun physical-reseed-follow-retirement-archive-extra (witness root)
  ;; Use the adopted final snapshot after completion and the current intent
  ;; extras during publication.
  (if (eq :done (retirement-archive-phase witness))
      (physical-reseed-follow-retirement-intent-durable-extra
       (retirement-archive-intent witness) (retirement-archive-final-snapshot witness) root
       (lambda () (physical-reseed-follow-retirement-archive-check-outer witness '(:done))))
      (physical-reseed-follow-retirement-intent-extra (retirement-archive-intent witness) root)))

(defun physical-reseed-follow-retirement-archive-observe (witness)
  (let* ((intent (retirement-archive-intent witness))
         (record (if (member (retirement-intent-phase intent) '(:renamed :durable))
                     (retirement-intent-candidate intent)
                     (getf (retirement-intent-snapshot intent) :record))))
    (physical-reseed-follow-retirement-prefix-proof-at
     (retirement-intent-descriptors intent) (retirement-intent-store intent) record
     (retirement-archive-prefix witness)
     (lambda () (physical-reseed-follow-retirement-archive-check-scope witness))
     (lambda (root) (physical-reseed-follow-retirement-archive-extra witness root))
     (retirement-archive-alias witness))))

(defun physical-reseed-follow-retirement-archive-check-file (witness row)
  (let* ((stream (retirement-archive-stream witness))
         (fd (sb-sys:fd-stream-fd stream))
         (mount (retirement-startup-mount
                 (retirement-intent-owner (retirement-archive-intent witness))))
         (bytes (retirement-archive-bytes witness))
         (current (make-array (length bytes) :element-type '(unsigned-byte 8))))
    (unless (equal (rest row) (physical-reseed-follow-retirement-held-file-facts fd mount))
      (error "retirement archive changed its held nanosecond facts"))
    (file-position stream 0)
    (unless (and (= (length current) (read-sequence current stream))
                 (eq :eof (read-byte stream nil :eof)) (equalp bytes current)
                 (equal (rest row) (physical-reseed-follow-retirement-held-file-facts fd mount)))
      (error "retirement archive changed its held bytes")))
  t)

(defun physical-reseed-follow-retirement-archive-check (witness)
  ;; Recheck held bytes and the full frozen proof under the cooperating locks.
  ;; Root size and times may change while the journal is published.
  (physical-reseed-follow-retirement-archive-check-scope witness)
  (let* ((intent (retirement-archive-intent witness))
         (root (physical-reseed-follow-retirement-archive-directory witness :root))
         (mount (retirement-startup-mount (retirement-intent-owner intent)))
         (row (physical-reseed-install-old-observe root "." mount))
         (publishing (member (retirement-archive-phase witness) '(:publishing :done))))
    (unless (equal (if publishing (subseq row 0 6) row)
                   (if publishing (subseq (retirement-archive-root-row witness) 0 6)
                       (retirement-archive-root-row witness)))
      (error "retirement archive changed its frozen root facts")))
  (physical-reseed-follow-retirement-archive-check-file witness (retirement-archive-row witness))
  (unless (equal (retirement-archive-proof witness)
                 (physical-reseed-follow-retirement-archive-observe witness))
    (error "retirement archive changed its complete frozen proof"))
  (physical-reseed-follow-retirement-archive-check-file witness (retirement-archive-row witness))
  (physical-reseed-follow-retirement-archive-check-scope witness)
  t)

(defun physical-reseed-follow-retirement-archive-check-effect (witness phases)
  (physical-reseed-follow-retirement-archive-check-scope witness phases)
  (let ((intent (retirement-archive-intent witness)))
    (unless (and (retirement-intent-effect-p intent)
                 (retirement-startup-uncertain-p (retirement-intent-owner intent)))
      (error "retirement archive effect requires an uncertain operation")))
  t)

(defun physical-reseed-follow-retirement-archive-point (witness point)
  (physical-reseed-follow-retirement-archive-check witness)
  (physical-reseed-follow-test-point
   point (merge-pathnames
          (if (zerop (retirement-archive-step witness))
              "physical-reseed.install" "physical-reseed.attempt")
          (retirement-startup-directory (retirement-intent-owner (retirement-archive-intent witness)))))
  (physical-reseed-follow-retirement-archive-check witness))

(defun physical-reseed-follow-retirement-archive-sync (witness role)
  (declare (notinline sb-posix:fsync))
  (physical-reseed-follow-retirement-archive-check-scope witness)
  (multiple-value-bind (fd phases before after)
      (ecase role
        (:old-final
         (values (sb-sys:fd-stream-fd (retirement-intent-old-stream (retirement-archive-intent witness)))
                 '(:source) :before-retirement-archive-old-final-fsync :after-retirement-archive-old-final-fsync))
        (:baseline-root
         (values (physical-reseed-follow-retirement-archive-directory witness :root)
                 '(:source) :before-retirement-archive-baseline-root-fsync :after-retirement-archive-baseline-root-fsync))
        (:source-inode
         (values (sb-sys:fd-stream-fd (retirement-archive-stream witness))
                 '(:source) :before-retirement-archive-source-fsync :after-retirement-archive-source-fsync))
        (:alias-inode
         (values (sb-sys:fd-stream-fd (retirement-archive-stream witness))
                 '(:alias) :before-retirement-archive-alias-fsync :after-retirement-archive-alias-fsync))
        (:saved-parent
         (values (physical-reseed-follow-retirement-archive-directory witness :saved)
                 '(:alias :saved) :before-retirement-archive-saved-fsync :after-retirement-archive-saved-fsync))
        (:saved-inode
         (values (sb-sys:fd-stream-fd (retirement-archive-stream witness))
                 '(:saved) :before-retirement-archive-inode-fsync :after-retirement-archive-inode-fsync))
        (:root-parent
         (values (physical-reseed-follow-retirement-archive-directory witness :root)
                 '(:saved) :before-retirement-archive-root-fsync :after-retirement-archive-root-fsync)))
    (physical-reseed-follow-retirement-archive-check-effect witness phases)
    (physical-reseed-follow-retirement-archive-point witness before)
    (physical-reseed-follow-retirement-archive-check-effect witness phases)
    (sb-posix:fsync fd)
    (physical-reseed-follow-retirement-archive-point witness after)
    (physical-reseed-follow-retirement-archive-check-effect witness phases)))

(defun physical-reseed-follow-retirement-archive-bridge-rows (witness)
  ;; Capture the immediate named and held facts after a link or unlink.  The
  ;; seal allows the subsequent full proof to check the same transition.
  (let* ((phase (retirement-archive-phase witness))
         (alias (ecase phase (:bridging-link t) (:bridging-unlink nil)))
         (root (physical-reseed-follow-retirement-archive-directory witness :root))
         (saved (physical-reseed-follow-retirement-archive-directory witness :saved))
         (mount (retirement-startup-mount (retirement-intent-owner (retirement-archive-intent witness))))
         (before (retirement-archive-row witness))
         (name (first before))
         (file-row (physical-reseed-install-old-observe (if alias root saved) name mount))
         (held-row (physical-reseed-follow-retirement-held-file-facts
                    (sb-sys:fd-stream-fd (retirement-archive-stream witness)) mount))
         (root-row (physical-reseed-install-old-observe root "." mount))
         (saved-row (physical-reseed-install-old-observe saved "." mount))
         (before-saved (cdr (assoc :saved (getf (getf (retirement-archive-proof witness) :layout) :directories)))))
    (unless (and (eql alias (retirement-archive-alias witness))
                 (= (if alias 1 2) (fifth before))
                 (= (if alias 2 1) (fifth file-row))
                 (equal (subseq before 0 4) (subseq file-row 0 4))
                 (equal (subseq before 5 8) (subseq file-row 5 8))
                 (equal (rest file-row) held-row)
                 (if alias
                     (equal file-row (physical-reseed-install-old-observe saved name mount))
                     (not (physical-adoption-path-present-p
                           (merge-pathnames name (physical-image-fd-path root)))))
                 (if alias
                     (and (equal (retirement-archive-root-row witness) root-row)
                          (equal (subseq before-saved 0 6) (subseq saved-row 0 6)))
                     (and (equal (subseq (retirement-archive-root-row witness) 0 6) (subseq root-row 0 6))
                          (equal before-saved saved-row))))
      (error "retirement archive bridge changed more than its owned metadata"))
    (list :file file-row :held held-row :root root-row :saved saved-row)))

(defun physical-reseed-follow-retirement-archive-bridge (witness operation seal)
  ;; Check the post-system-call seal around the full observation before
  ;; adopting the new phase and frozen proof.
  (physical-reseed-follow-retirement-archive-check-effect
   witness (ecase operation (:link '(:bridging-link)) (:unlink '(:bridging-unlink))))
  (unless (equal seal (physical-reseed-follow-retirement-archive-bridge-rows witness))
    (error "retirement archive bridge changed its post-syscall facts"))
  (let* ((step (retirement-archive-step witness))
         (row (getf seal :file))
         (root-row (getf seal :root)))
    (physical-reseed-follow-retirement-archive-check-file witness row)
    (let ((proof (physical-reseed-follow-retirement-archive-observe witness)))
      (unless (equal seal (physical-reseed-follow-retirement-archive-bridge-rows witness))
        (error "retirement archive bridge changed its post-syscall facts"))
      (physical-reseed-follow-retirement-archive-check-file witness row)
      (unless (and (equal row (getf (getf proof (if (zerop step) :install :attempt)) :row))
                   (equal (getf seal :saved)
                          (cdr (assoc :saved (getf (getf proof :layout) :directories)))))
        (error "retirement archive proof differs from its sealed rows"))
      (physical-reseed-follow-retirement-check-archive-bridge
       (retirement-archive-proof witness) proof step operation)
      (sb-sys:without-interrupts
        (setf (retirement-archive-proof witness) proof
              (retirement-archive-row witness) (copy-list row)
              (retirement-archive-root-row witness) (copy-list root-row)
              (retirement-archive-phase witness) (ecase operation (:link :alias) (:unlink :saved)))))
    (physical-reseed-follow-retirement-archive-check witness)))

(defun physical-reseed-follow-retirement-archive-link (witness)
  ;; Create the saved name and capture raw post-link facts before callbacks or
  ;; the full proof can run.
  (declare (notinline port-linkat))
  (physical-reseed-follow-retirement-archive-check-effect witness '(:source))
  (physical-reseed-follow-retirement-archive-point witness :before-retirement-archive-link)
  (let ((root (physical-reseed-follow-retirement-archive-directory witness :root))
        (saved (physical-reseed-follow-retirement-archive-directory witness :saved))
        (name (first (retirement-archive-row witness)))
        (seal nil))
    (sb-sys:without-interrupts
      (physical-reseed-follow-retirement-archive-check-effect witness '(:source))
      (unless (zerop (port-linkat root name saved name 0))
        (error "cannot link retirement archive: errno ~d" (sb-alien:get-errno)))
      (setf (retirement-archive-alias witness) t
            (retirement-archive-phase witness) :bridging-link
            seal (physical-reseed-follow-retirement-archive-bridge-rows witness)))
    (physical-reseed-follow-retirement-archive-bridge witness :link seal))
  (physical-reseed-follow-retirement-archive-point witness :after-retirement-archive-link))

(defun physical-reseed-follow-retirement-archive-unlink (witness)
  ;; Remove the root name and capture raw post-unlink facts before callbacks or
  ;; the full proof can run.
  (declare (notinline port-unlinkat))
  (physical-reseed-follow-retirement-archive-check-effect witness '(:alias-durable))
  (physical-reseed-follow-retirement-archive-point witness :before-retirement-archive-unlink)
  (let ((root (physical-reseed-follow-retirement-archive-directory witness :root))
        (name (first (retirement-archive-row witness)))
        (seal nil))
    (sb-sys:without-interrupts
      (physical-reseed-follow-retirement-archive-check-effect witness '(:alias-durable))
      (unless (zerop (port-unlinkat root name 0))
        (error "cannot unlink retirement archive source: errno ~d" (sb-alien:get-errno)))
      (setf (retirement-archive-alias witness) nil
            (retirement-archive-prefix witness) (1+ (retirement-archive-step witness))
            (retirement-archive-phase witness) :bridging-unlink
            seal (physical-reseed-follow-retirement-archive-bridge-rows witness)))
    (physical-reseed-follow-retirement-archive-bridge witness :unlink seal))
  (physical-reseed-follow-retirement-archive-point witness :after-retirement-archive-unlink))

(defmethod physical-reseed-follow-retirement-authorize-progress
    ((authority physical-reseed-follow-retirement-archive-witness) witness)
  ;; Only the archive witness bound to this intent can authorize publication
  ;; after the saved-only move has been synced and proved.
  (unless (eq witness (retirement-archive-intent authority))
    (error "retirement archive authority belongs to another publisher"))
  (physical-reseed-follow-retirement-archive-check-effect authority '(:moved-durable))
  (unless (and (not (retirement-archive-alias authority))
               (eql (1+ (retirement-archive-step authority)) (retirement-archive-prefix authority))
               (equal (retirement-archive-proof authority) (retirement-intent-proof witness)))
    (error "retirement progress requires its durable saved-only proof"))
  (sb-sys:without-interrupts
    (physical-reseed-follow-retirement-archive-check-effect authority '(:moved-durable))
    (setf (retirement-archive-phase authority) :publishing))
  (physical-reseed-follow-retirement-archive-check authority)
  (physical-reseed-follow-retirement-intent-check witness)
  t)

(defun physical-reseed-follow-retirement-archive-run (witness)
  ;; Resume from the proved source, two-name alias or saved-only state.  Link
  ;; the destination first, sync the inode and saved directory, then unlink
  ;; the source.  Sync the saved inode and both parents before publishing
  ;; progress with this witness.
  (unless (and (physical-reseed-follow-retirement-archive-witness-p witness)
               (retirement-archive-active-p witness)
               (eq (retirement-archive-thread witness) sb-thread:*current-thread*)
               (eq :ready (retirement-archive-phase witness)))
    (error "retirement archive witness already claimed or ended"))
  (sb-sys:without-interrupts
    (unless (eq :ready (retirement-archive-phase witness))
      (error "retirement archive witness already claimed or ended"))
    (setf (retirement-archive-phase witness) :checking))
  (physical-reseed-follow-retirement-archive-check witness)
  (let* ((intent (retirement-archive-intent witness))
         (owner (retirement-intent-owner intent)))
    (when (or (retirement-intent-effect-p intent) (retirement-startup-uncertain-p owner))
      (error "retirement archive requires a fresh operation"))
    (sb-sys:without-interrupts
      (setf (retirement-intent-effect-p intent) t
            (retirement-startup-uncertain-p owner) t
            (retirement-archive-phase witness)
            (cond ((retirement-archive-alias witness) :alias)
                  ((= (retirement-archive-prefix witness) (retirement-archive-step witness)) :source)
                  (t :saved))))
    (when (eq :source (retirement-archive-phase witness))
      (physical-reseed-follow-retirement-archive-sync witness :old-final)
      (physical-reseed-follow-retirement-archive-sync witness :baseline-root)
      (physical-reseed-follow-retirement-archive-sync witness :source-inode)
      (physical-reseed-follow-retirement-archive-link witness))
    (when (eq :alias (retirement-archive-phase witness))
      (physical-reseed-follow-retirement-archive-sync witness :alias-inode)
      (physical-reseed-follow-retirement-archive-sync witness :saved-parent)
      (physical-reseed-follow-retirement-archive-check witness)
      (setf (retirement-archive-phase witness) :alias-durable)
      (physical-reseed-follow-retirement-archive-unlink witness))
    (physical-reseed-follow-retirement-archive-sync witness :saved-inode)
    (physical-reseed-follow-retirement-archive-sync witness :saved-parent)
    (physical-reseed-follow-retirement-archive-sync witness :root-parent)
    (physical-reseed-follow-retirement-archive-check witness)
    (sb-sys:without-interrupts
      (setf (retirement-archive-phase witness) :moved-durable
            (retirement-intent-proof intent) (physical-adoption-attempt-value-copy (retirement-archive-proof witness))))
    (multiple-value-bind (record snapshot)
        (physical-reseed-follow-retirement-intent-publish-held intent witness)
      (unless (equal record (retirement-intent-candidate intent))
        (error "retirement archive publication returned another record"))
      (sb-sys:without-interrupts
        (setf (retirement-archive-final-snapshot witness)
              (physical-adoption-attempt-value-copy snapshot)
              (retirement-archive-phase witness) :done))
      (values record snapshot))))

(defun call-with-physical-reseed-follow-retirement-archive-file (intent expected function)
  ;; Hold the archive file and its private bytes through the thread-bound
  ;; witness scope.  Reprove the observed state before and after the callback.
  (declare (notinline physical-image-openat))
  (physical-reseed-follow-retirement-intent-check-scope intent)
  (let* ((record (getf (retirement-intent-snapshot intent) :record))
         (step (getf (getf record :retirement) :step))
         (prefix (getf expected :prefix))
         (alias (eq :alias (getf expected :location)))
         (descriptors (retirement-intent-descriptors intent))
         (root (physical-reseed-follow-retirement-intent-root intent))
         (saved (physical-reseed-follow-configuration-role-fd descriptors :saved))
         (root-row (physical-reseed-install-old-observe
                    root "." (retirement-startup-mount (retirement-intent-owner intent))))
         (key (if (zerop step) :install :attempt))
         (proof (physical-reseed-follow-retirement-prefix-proof-at
                 descriptors (retirement-intent-store intent) record prefix
                 (lambda () (physical-reseed-follow-retirement-intent-check-scope intent))
                 (lambda (fd) (physical-reseed-follow-retirement-intent-extra intent fd)) alias))
         (row (getf (getf proof key) :row))
         (name (first row))
         (maximum (if (zerop step) +physical-reseed-install-maximum-bytes+
                      +physical-reseed-attempt-maximum-bytes+))
         (parent (if (and (not alias) (= prefix (1+ step))) saved root))
         (fd nil) (stream nil) (witness nil))
    (dolist (field '(:prefix :location :layout :install :attempt))
      (unless (equal (getf expected field) (getf proof field))
        (error "retirement archive changed after its startup observation")))
    (unless (<= 1 (seventh row) maximum) (error "invalid retirement archive length"))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (setf fd (physical-image-openat
                       parent name (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                           +postmaster-o-cloexec+) 0))
             (when (minusp fd) (setf fd nil) (error "cannot open retirement archive"))
             (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8))))
           (physical-reseed-follow-test-point
            :after-retirement-archive-open
            (merge-pathnames name (retirement-startup-directory (retirement-intent-owner intent))))
           (let ((bytes (make-array (seventh row) :element-type '(unsigned-byte 8))))
             (unless (and (= (length bytes) (read-sequence bytes stream))
                          (eq :eof (read-byte stream nil :eof))
                          (= (getf (getf proof key) :digest)
                             (physical-store-update-digest #xCBF29CE484222325 bytes)))
               (error "retirement archive differs from its proved bytes"))
             (setf witness
                   (make-physical-reseed-follow-retirement-archive-witness
                    :intent intent :thread sb-thread:*current-thread* :active-p t
                    :binding (physical-adoption-attempt-value-copy
                              (physical-reseed-follow-retirement-intent-binding intent))
                    :step step :prefix prefix :alias alias :phase :ready :proof proof
                    :root-row root-row
                    :stream stream :row (copy-list row) :bytes bytes))
             (physical-reseed-follow-retirement-archive-check witness)
             (multiple-value-prog1 (funcall function witness)
               (physical-reseed-follow-retirement-archive-check witness))))
      (when witness (setf (retirement-archive-active-p witness) nil))
      (if stream (close stream) (when fd (sb-posix:close fd))))))

(defun call-with-physical-reseed-follow-retirement-archive-context (owner expected function)
  ;; Bind the archive witness and next candidate to one intent context.
  (call-with-physical-reseed-follow-retirement-intent-base-context
   owner expected
   (lambda (snapshot)
     (let* ((record (getf snapshot :record))
            (retirement (getf record :retirement))
            (candidate
              (physical-reseed-follow-retirement-next-record record (getf retirement :attempt-file))))
       (unless (and (eq :retiring (getf retirement :phase))
                    (typep (getf retirement :step) '(integer 0 1)))
         (error "retirement archive context requires a pending final move"))
       candidate))
   (lambda (current)
     (call-with-physical-reseed-follow-retirement-archive-file current expected function))))

(defun physical-reseed-follow-retirement-archive-step (owner step)
  ;; Archive the journal for STEP from Retiring at that step.  A Retiring
  ;; successor is an effect-free retry only at its completed prefix with no
  ;; alias or temporary.  The context keeps the frozen attempt reference.
  ;; Clear uncertainty only after the archive context finishes all cleanup.
  (unless (typep step '(integer 0 1)) (error "invalid retirement archive step"))
  (call-with-physical-reseed-follow-retirement-operation
   owner
   (lambda ()
     (let* ((expected
              (physical-reseed-follow-retirement-intent-startup-expected owner))
            (record (getf (getf expected :snapshot) :record))
            (retirement (getf record :retirement))
            (name (if (zerop step) "install" "attempt")))
       (unless (and (eq :retiring (getf retirement :phase))
                    (member (getf retirement :step) (list step (1+ step))))
         (error "~a archive requires Retiring~d or completed Retiring~d" name step (1+ step)))
       (when (and *physical-reseed-follow-write-limit*
                  (not (and (integerp *physical-reseed-follow-write-limit*)
                            (plusp *physical-reseed-follow-write-limit*))))
         (error "invalid retirement intent write limit"))
       (if (= (1+ step) (getf retirement :step))
           (progn
             (unless (and (= (1+ step) (getf expected :prefix)) (null (getf expected :location))
                          (null (getf expected :temporary-file)))
               (error "~a archive retry requires stable Retiring~d" name (1+ step)))
             (values (physical-reseed-follow-copy record)
                     (physical-adoption-attempt-value-copy (getf expected :snapshot))))
           (multiple-value-prog1
               (call-with-physical-reseed-follow-retirement-archive-context
                owner expected #'physical-reseed-follow-retirement-archive-run)
             (setf (retirement-startup-uncertain-p owner) nil)))))))

(defun physical-reseed-follow-retirement-archive-attempt (owner)
  ;; Archive the attempt journal at step 1.
  (physical-reseed-follow-retirement-archive-step owner 1))

(defun physical-reseed-follow-retirement-archive-install (owner)
  ;; Archive the install journal at step 0.
  (physical-reseed-follow-retirement-archive-step owner 0))

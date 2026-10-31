(in-package #:pg)

;; Parse synchronous standby names and select sender frontiers only.  The C
;; references are syncrep_scanner.l, syncrep_gram.y and syncrep.c.

(defun sync-standby-syntax-error ()
  (ereport "42601" "invalid value for parameter \"synchronous_standby_names\""))

(defun sync-standby-name-equal-p (left right)
  (labels ((fold (character)
             (let ((code (char-code character)))
               (if (<= 65 code 90) (+ code 32) code))))
    (and (= (length left) (length right))
         (loop for a across left for b across right always (= (fold a) (fold b))))))

(defun sync-standby-identifier-start-p (character)
  (let ((code (char-code character)))
    (or (<= 65 code 90) (<= 97 code 122) (= code 95) (>= code 128))))

(defun sync-standby-identifier-continue-p (character)
  (or (sync-standby-identifier-start-p character)
      (<= 48 (char-code character) 57)
      (char= character #\$)))

(defun sync-standby-quoted-token (text position)
  (let ((out (make-string-output-stream))
        (size (length text)))
    (loop while (< position size)
          for character = (char text position)
          do (incf position)
             (if (char= character #\")
                 (if (and (< position size) (char= (char text position) #\"))
                     (progn (write-char #\" out) (incf position))
                     (return-from sync-standby-quoted-token
                       (values (get-output-stream-string out) position)))
                 (write-char character out)))
    (sync-standby-syntax-error)))

(defun sync-standby-tokenize (text)
  ;; This is the configuration grammar, not the SQL identifier scanner; names
  ;; are retained without SQL identifier truncation.
  (let ((position 0)
        (size (length text))
        (tokens nil))
    (labels ((emit (kind value) (push (cons kind value) tokens))
             (digit-p (character) (<= 48 (char-code character) 57)))
      (loop while (< position size)
            for character = (char text position)
            do (cond
                 ((member (char-code character) '(32 9 10 13 12 11))
                  (incf position))
                 ((char= character #\")
                  (multiple-value-bind (value end)
                      (sync-standby-quoted-token text (1+ position))
                    (emit :name value)
                    (setf position end)))
                 ((digit-p character)
                  (let ((start position))
                    (loop while (and (< position size) (digit-p (char text position)))
                          do (incf position))
                    (emit :number (subseq text start position))))
                 ((sync-standby-identifier-start-p character)
                  (let* ((start position)
                         (end (loop while (and (< position size)
                                               (sync-standby-identifier-continue-p
                                                (char text position)))
                                    do (incf position)
                                    finally (return position)))
                         (value (subseq text start end)))
                    (emit (cond ((sync-standby-name-equal-p value "FIRST") :first)
                                ((sync-standby-name-equal-p value "ANY") :any)
                                (t :name))
                          value)))
                 (t
                  (emit (case character
                          (#\* :name) (#\, :comma) (#\( :left) (#\) :right)
                          (otherwise (sync-standby-syntax-error)))
                        (string character))
                  (incf position)))))
    (nreverse tokens)))

(defun parse-sync-standby-names (text)
  ;; Only an actually empty string disables selection.  The owner must treat
  ;; the returned :METHOD, :COUNT and :MEMBERS plist as immutable.
  (when (zerop (length text)) (return-from parse-sync-standby-names nil))
  (when (find (code-char 0) text) (sync-standby-syntax-error))
  (let ((tokens (sync-standby-tokenize text))
        (method :first)
        (count 1)
        (members nil)
        (parenthesized nil))
    (labels ((take (kind)
               (unless (eq kind (caar tokens)) (sync-standby-syntax-error))
               (cdr (pop tokens)))
             (member-list ()
               (loop
                 (unless (member (caar tokens) '(:name :number))
                   (sync-standby-syntax-error))
                 (push (cdr (pop tokens)) members)
                 (unless (eq :comma (caar tokens)) (return))
                 (pop tokens))))
      (when (or (member (caar tokens) '(:first :any))
                (and (eq :number (caar tokens)) (eq :left (caadr tokens))))
        (when (member (caar tokens) '(:first :any))
          (setf method (car (pop tokens))))
        (setf count (parse-integer (take :number))
              parenthesized t)
        (take :left))
      (member-list)
      (when parenthesized (take :right))
      (when tokens (sync-standby-syntax-error))
      (unless (<= 1 count 2147483647)
        (ereport "22023" "number of synchronous standbys is outside the valid range"))
      (list :method method :count count :members (nreverse members)))))

(defun sync-standby-priority (config application-name &optional cascading-p)
  ;; The first matching name or wildcard supplies priority; ANY uses priority 1.
  ;; Recovery senders are excluded, including cascading senders.
  (if (or (null config) cascading-p)
      0
      (loop for name in (getf config :members)
            for priority from 1
            when (or (string= name "*")
                     (sync-standby-name-equal-p name application-name))
              return (if (eq :any (getf config :method)) 1 priority)
            finally (return 0))))

(defun sync-standby-candidate-less-p (left right)
  (let ((a (getf left :sync-priority))
        (b (getf right :sync-priority)))
    (if (= a b)
        (< (getf left :index) (getf right :index))
        (< a b))))

(defun sync-standby-mode-frontier (selected field method count)
  ;; FIRST takes a strict minimum, including NIL normalized to zero.  Unlike
  ;; C's SyncRepGetOldestSyncRecPtr accumulator, a zero cannot be discarded
  ;; by a later nonzero value.
  ;; Thus an unknown selected peer blocks this mode; do not substitute another.
  ;; ANY takes the Nth highest position independently for each mode.
  (let ((positions (mapcar (lambda (row) (or (getf row field) 0)) selected)))
    (ecase method
      (:first (reduce #'min positions))
      (:any (nth (1- count) (sort positions #'>))))))

(defun sync-replication-select-standbys (config rows)
  ;; The caller supplies current sender rows; this selector does not validate
  ;; registry tokens.  Eligible senders are streaming or stopping with positive flush;
  ;; logical senders are eligible on a primary.  FIRST sorts by priority only
  ;; when a cutoff is needed; otherwise preserve slot order.  Too few candidates
  ;; still return the selected rows, but no frontiers.
  (unless config (return-from sync-replication-select-standbys (values nil nil)))
  (let* ((method (getf config :method))
         (count (getf config :count))
         (candidates
           (loop for row in rows
                 for priority = (sync-standby-priority
                                 config (getf row :application-name "")
                                 (getf row :cascading-p))
                 when (and (plusp priority)
                           (typep (getf row :pid) '(integer 1 *))
                           (member (getf row :state) '("streaming" "stopping") :test #'equal)
                           (typep (getf row :flush-lsn) '(integer 1 18446744073709551615)))
                   collect (list* :sync-priority priority row)))
         (ordered (if (and (eq method :first) (> (length candidates) count))
                      (sort candidates #'sync-standby-candidate-less-p)
                      (sort candidates #'< :key (lambda (row) (getf row :index)))))
         (selected (if (eq method :first)
                       (subseq ordered 0 (min count (length ordered)))
                       ordered)))
    (values selected
            (when (>= (length selected) count)
              (list :write (sync-standby-mode-frontier selected :write-lsn method count)
                    :flush (sync-standby-mode-frontier selected :flush-lsn method count)
                    :apply (sync-standby-mode-frontier selected :replay-lsn method count))))))

(defun sync-replication-select-current-under-lock (config)
  (let ((cascading-p (recovery-in-progress-p)))
    (sync-replication-select-standbys
     config
     (loop for sender being the hash-values of *wal-sender-registry*
           collect (list* :index (wal-sender-status-index sender)
                          :cascading-p cascading-p
                          (wal-sender-snapshot-under-lock sender))))))

(defun sync-replication-current-selection (config)
  ;; Read current registry membership and sender fields under the sender mutex.
  ;; This selection alone does not expose sync state or wait for acknowledgments.
  (sb-thread:with-mutex (*wal-sender-lock*)
    (sync-replication-select-current-under-lock config)))

;; Private publication and wait core.  Public settings, commit, startup and
;; native source events are not yet wired to it.

(defstruct sync-replication-waiter
  ;; Only :WAITING entries are queued; terminal outcomes stay fixed.  The
  ;; caller owns transaction cleanup after the wait.
  backend lsn mode source-id (state :waiting))

(defstruct sync-replication-state
  ;; One shared state is protected by the sender mutex.  Queues are sorted by
  ;; LSN per mode; frontiers retain monotonic proof until the source changes.
  config (names "") source-id (generation 0)
  (frontiers (list :write 0 :flush 0 :apply 0))
  (queues (list :write nil :flush nil :apply nil))
  (cv (sb-thread:make-waitqueue :name "synchronous replication")))

(defvar *sync-replication-state* (make-sync-replication-state))

(defun sync-replication-check-source-id (source-id)
  (unless (typep source-id '(or symbol integer))
    (ereport "22023" "replication source identity must be an immutable symbol or integer"))
  (values))

(defun sync-replication-configuration-state ()
  (sb-thread:with-mutex (*wal-sender-lock*)
    (values (sync-replication-state-source-id *sync-replication-state*) (sync-replication-state-generation *sync-replication-state*)
            (copy-seq (sync-replication-state-names *sync-replication-state*)))))

(defun sync-replication-check-registration-under-lock (source-id)
  (sync-replication-check-source-id source-id)
  (unless (eql source-id (sync-replication-state-source-id *sync-replication-state*))
    (ereport "55000" "replication source changed before sender registration"))
  (values))

(defun sync-replication-complete-all-under-lock (state)
  (dolist (mode '(:write :flush :apply))
    (dolist (waiter (getf (sync-replication-state-queues *sync-replication-state*) mode))
      (setf (sync-replication-waiter-state waiter) state))
    (setf (getf (sync-replication-state-queues *sync-replication-state*) mode) nil))
  (sb-thread:condition-broadcast (sync-replication-state-cv *sync-replication-state*))
  (values))

(defun sync-replication-release-under-lock ()
  (let ((released nil))
    (dolist (mode '(:write :flush :apply))
      (let ((queue (getf (sync-replication-state-queues *sync-replication-state*) mode))
            (frontier (getf (sync-replication-state-frontiers *sync-replication-state*) mode)))
        (loop while (and queue (<= (sync-replication-waiter-lsn (first queue)) frontier))
              do (setf (sync-replication-waiter-state (pop queue)) :acknowledged
                       released t))
        (setf (getf (sync-replication-state-queues *sync-replication-state*) mode) queue)))
    (when released (sb-thread:condition-broadcast (sync-replication-state-cv *sync-replication-state*))))
  (values))

(defun sync-replication-next-frontiers-under-lock (config)
  ;; Current candidates may fall behind or disappear; retained proof does not.
  (let ((current (when config
                   (nth-value 1 (sync-replication-select-current-under-lock config)))))
    (loop for mode in '(:write :flush :apply)
          append (list mode (max (getf (sync-replication-state-frontiers *sync-replication-state*) mode)
                                 (getf current mode 0))))))

(defun sync-replication-refresh-under-lock ()
  (when (sync-replication-state-config *sync-replication-state*)
    (setf (sync-replication-state-frontiers *sync-replication-state*)
          (sync-replication-next-frontiers-under-lock (sync-replication-state-config *sync-replication-state*)))
    (sync-replication-release-under-lock))
  (values))

(defun sync-replication-enqueue-under-lock (backend lsn mode source-id)
  ;; Hold the sender mutex and mask interrupts through the caller's waiter
  ;; assignment and unwind ownership, so a published entry cannot be orphaned.
  (sync-replication-check-source-id source-id)
  (assert (and (sb-thread:holding-mutex-p *wal-sender-lock*)
               (not sb-sys:*interrupts-enabled*)))
  (unless (and (backend-p backend)
               (typep lsn '(integer 1 18446744073709551615))
               (member mode '(:write :flush :apply)))
    (error "invalid synchronous replication wait"))
  (let* ((state (cond ((not (eql source-id (sync-replication-state-source-id *sync-replication-state*))) :source-changed)
                      ((null (sync-replication-state-config *sync-replication-state*)) :disabled)
                      ((<= lsn (getf (sync-replication-state-frontiers *sync-replication-state*) mode)) :acknowledged)
                      (t :waiting)))
         (waiter (make-sync-replication-waiter :backend backend :lsn lsn :mode mode
                                               :source-id source-id :state state)))
    (when (eq state :waiting)
      (setf (getf (sync-replication-state-queues *sync-replication-state*) mode)
            (merge 'list (list waiter) (getf (sync-replication-state-queues *sync-replication-state*) mode)
                   #'< :key #'sync-replication-waiter-lsn)))
    waiter))

(defun sync-replication-cancel-under-lock (waiter reason)
  (unless (member reason '(:cancelled :timeout :terminated))
    (error "invalid synchronous replication cancellation"))
  (when (eq :waiting (sync-replication-waiter-state waiter))
    (let ((mode (sync-replication-waiter-mode waiter)))
      (assert (member waiter (getf (sync-replication-state-queues *sync-replication-state*) mode) :test #'eq))
      (setf (getf (sync-replication-state-queues *sync-replication-state*) mode)
            (delete waiter (getf (sync-replication-state-queues *sync-replication-state*) mode) :test #'eq)
            (sync-replication-waiter-state waiter) reason)
      (sb-thread:condition-broadcast (sync-replication-state-cv *sync-replication-state*))))
  (values))

(defun sync-replication-install-configuration (text source-id expected-source-id expected-generation)
  ;; Compare both predecessor source and configuration generation under lock.
  ;; Source IDs are immutable symbols or integers; the owning lifecycle must
  ;; supply a fresh real generation for every source incarnation.
  (sync-replication-check-source-id source-id)
  (sync-replication-check-source-id expected-source-id)
  (let ((config (parse-sync-standby-names text))
        (names (copy-seq text))
        (obsolete nil)
        (installed-generation nil))
    (sb-sys:without-interrupts
      (sb-thread:with-mutex (*wal-sender-lock*)
        (unless (and (eql expected-generation (sync-replication-state-generation *sync-replication-state*))
                     (eql expected-source-id (sync-replication-state-source-id *sync-replication-state*)))
          (ereport "55000" "synchronous replication configuration generation changed"))
        (let* ((source-changed (not (eql source-id (sync-replication-state-source-id *sync-replication-state*))))
               (frontiers (if source-changed
                              (list :write 0 :flush 0 :apply 0)
                              (sync-replication-next-frontiers-under-lock config)))
               (generation (1+ (sync-replication-state-generation *sync-replication-state*))))
          (when source-changed
            (setf obsolete (loop for backend being the hash-keys of *wal-sender-registry*
                                 collect backend))
            (sync-replication-complete-all-under-lock :source-changed)
            (clrhash *wal-sender-registry*))
          (setf (sync-replication-state-config *sync-replication-state*) config
                (sync-replication-state-names *sync-replication-state*) names
                (sync-replication-state-source-id *sync-replication-state*) source-id
                (sync-replication-state-generation *sync-replication-state*) generation
                (sync-replication-state-frontiers *sync-replication-state*) frontiers
                installed-generation generation)
          (if config
              (sync-replication-release-under-lock)
              (sync-replication-complete-all-under-lock :disabled))))
      ;; OBSOLETE holds exact backend objects captured before registry clear.
      ;; Signal after unlock, with this backend last so it cannot interrupt the rest.
      (dolist (backend obsolete)
        (unless (eq backend *backend*) (terminate-backend backend)))
      (when (member *backend* obsolete :test #'eq)
        (terminate-backend *backend*))
      installed-generation)))

(defun sync-replication-interruption (backend)
  ;; Termination wins and stays pending for delivery after committed cleanup.
  ;; Timeout and cancel are returned as reasons, not aborting errors.
  (cond
    ((backend-termination-pending backend) :terminated)
    ((and (backend-statement-deadline backend)
          (>= (get-internal-real-time) (backend-statement-deadline backend)))
     (clear-statement-timeout backend)
     (setf (backend-cancel-pending backend) nil)
     :timeout)
    ((backend-cancel-pending backend)
     (setf (backend-cancel-pending backend) nil)
     :cancelled)))

(defun sync-replication-wait (backend lsn mode source-id)
  ;; The caller must keep interrupts masked through later committed cleanup and
  ;; must not hold WAL here.  Condition waits release the sender mutex; polling
  ;; checks interrupt flags every 0.1 seconds.  Source change is not success.
  ;; On any nonlocal exit, unlink the waiter without poisoning WAL.
  (assert (not sb-sys:*interrupts-enabled*))
  (sb-sys:without-interrupts
    (let ((waiter nil))
      (unwind-protect
           (sb-thread:with-mutex (*wal-sender-lock*)
             (setf waiter (sync-replication-enqueue-under-lock backend lsn mode source-id))
             (loop while (eq :waiting (sync-replication-waiter-state waiter))
                   do (let ((reason (sync-replication-interruption backend)))
                        (if reason
                            (sync-replication-cancel-under-lock waiter reason)
                            (sb-thread:condition-wait
                             (sync-replication-state-cv *sync-replication-state*)
                             *wal-sender-lock* :timeout 0.1))))
             (sync-replication-waiter-state waiter))
        (when waiter
          (sb-thread:with-mutex (*wal-sender-lock*)
            (sync-replication-cancel-under-lock waiter :cancelled)))))))

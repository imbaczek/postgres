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

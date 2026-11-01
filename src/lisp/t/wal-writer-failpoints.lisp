;;;; Barriers used by the task-owned native WAL writer tests.
(in-package #:pg)

(defun wal-writer-test-file-barrier (ready release)
  (with-open-file (out ready :direction :output :if-exists :error)
    (write-line "ready" out))
  (let ((deadline (+ (get-internal-real-time) (* 30 internal-time-units-per-second))))
    (loop until (probe-file release)
          do (when (>= (get-internal-real-time) deadline)
               (error "WAL writer test barrier release timed out"))
             (sleep 0.01))))

(defun wal-writer-test-pause-next-cycle (ready release done)
  (when *primary-wal-writer-test-hook* (error "WAL writer already has a test hook"))
  (setf *primary-wal-writer-test-hook*
        (lambda (point writer)
          (when (eq point :before-cycle)
            (assert (eq writer *primary-wal-writer*))
            (assert (eq sb-thread:*current-thread* (primary-wal-writer-thread writer)))
            (assert (null *backend*))
            (assert (zerop *live-operation-depth*))
            (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
            (assert (not (sb-thread:holding-mutex-p *reload-definition-lock*)))
            (setf *primary-wal-writer-test-hook* nil)
            (unwind-protect
                 (wal-writer-test-file-barrier ready release)
              (with-open-file (out done :direction :output :if-exists :error)
                (write-line "done" out))))))
  (values))

(defvar *writer-test-owner* nil)
(defvar *writer-test-backend* nil)
(defvar *writer-test-end* nil)

(defvar *writer-test-idle-checkpoint* nil)

(defvar *writer-test-idle-metric* nil)

(defun wal-writer-test-append-idle-batch ()
  (with-wal-critical
    (assert (null *wal-pending-page-publication*))
    (assert (null *wal-pending-page-publication-tail*))
    (assert (zerop (hash-table-count *clog-pending-commits*)))
    (setf *writer-test-idle-checkpoint*
          (list *wal-checkpoint-lsn* (copy-list *wal-checkpoint-snapshot*) *wal-checkpoint-timeline*)
          *writer-test-idle-metric* (primary-wal-writer-last-drained-end *primary-wal-writer*))
    (wal-append-batch-start)
    (setf *writer-test-end*
          (wal-append-record +wal-record-parameter-change+ 0
                             (wal-encode-resource-parameters
                              (resource-parameters-from-values
                               (getf *wal-control-metadata* :resource-parameters)))))
    (assert (> *writer-test-end* *wal-durable-lsn*)))
  (values))

(defun wal-writer-test-idle-durable-p ()
  (sb-thread:with-mutex (*wal-lock*)
    (let ((disk (multiple-value-list (wal-read-control-fields))))
      (and (>= *wal-durable-lsn* *writer-test-end*)
           (>= (second disk) *writer-test-end*)
           (null *wal-pending-page-publication*)
           (null *wal-pending-page-publication-tail*)
           (zerop (hash-table-count *clog-pending-commits*))
           (equal *writer-test-idle-checkpoint* (list (first disk) (third disk) (fifth disk)))
           (eql *writer-test-idle-metric* (primary-wal-writer-last-drained-end *primary-wal-writer*))))))

(defun wal-writer-test-install-idle-failure (evidence phase)
  (let ((flush (symbol-function 'wal-flush-core))
        (pause (symbol-function 'wal-test-pause))
        (fail-stop (symbol-function 'wal-fail-stop))
        (writer *primary-wal-writer*)
        (failure (make-condition 'simple-error :format-control "owned idle writer flush failure"))
        (reported nil))
    (flet ((writer-p () (eq sb-thread:*current-thread* (primary-wal-writer-thread writer))))
      (setf (symbol-function 'wal-flush-core)
            (lambda ()
              (when (writer-p)
                (case phase
                  (:before (error failure))
                  (:exit (sb-thread:return-from-thread :idle-flush-exit))))
              (funcall flush)
              (when (and (writer-p) (eq phase :after)) (error failure)))
            (symbol-function 'wal-test-pause)
            (lambda (point)
              (when (and (writer-p) (eq phase :middle) (eq point :after-wal-bytes-flush))
                (error failure))
              (funcall pause point))
            (symbol-function 'wal-fail-stop)
            (lambda (condition)
              (when (and (writer-p) (not reported))
                (assert (or (eq phase :exit) (eq condition failure)))
                (assert (eq condition (primary-wal-writer-failure writer)))
                (assert (eq :failed (primary-wal-writer-state writer)))
                (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                (assert (not (sb-thread:holding-mutex-p *reload-definition-lock*)))
                (sb-thread:with-mutex (*wal-lock*)
                  (assert *wal-poisoned*)
                  (assert (null *wal-pending-page-publication*))
                  (assert (null *wal-pending-page-publication-tail*))
                  (assert (eql (eq phase :after) (>= *wal-durable-lsn* *writer-test-end*))))
                (setf reported t)
                (with-open-file (out evidence :direction :output :if-exists :error)
                  (write-line "idle flush fenced outside locks" out)))
              (funcall fail-stop condition)))))
  (values))

(defun wal-writer-test-admit-backend (pid)
  (setf *writer-test-backend* (lookup-backend pid))
  (unless *writer-test-backend* (error "missing WAL writer test backend"))
  (let ((*backend* *writer-test-backend*))
    (with-wal-publication-fence (begin)
      (begin)
      (multiple-value-setq (*writer-test-owner* *writer-test-end*)
        (wal-admit-transaction-under-lock (backend-xid *backend*) nil))))
  (values))

(defun wal-writer-test-prefix-drained-p ()
  (sb-thread:with-mutex (*wal-lock*)
    (and *writer-test-owner*
         (eq :released (wal-page-publication-state *writer-test-owner*))
         (>= *wal-durable-lsn* *writer-test-end*)
         (null *wal-pending-page-publication*)
         (zerop (hash-table-count *clog-pending-commits*)))))

(defun wal-writer-test-finish-backend ()
  (unless (wal-writer-test-prefix-drained-p) (error "test WAL prefix has not drained"))
  (finish-committed-transaction *writer-test-backend* (backend-xid *writer-test-backend*))
  (values))

(defvar *writer-test-reset-worker* nil)
(defvar *writer-test-stop-worker* nil)
(defvar *writer-test-reset-error* nil)
(defvar *writer-test-stop-error* nil)

(defun wal-writer-test-start-reset (ready release)
  (setf *writer-test-reset-error* nil
        *writer-test-reset-worker*
        (sb-thread:make-thread
         (lambda ()
           (handler-case
               (with-live-operation
                 (clog-reset :fsync-function
                             (lambda (stream)
                               (fsync-stream stream)
                               (wal-writer-test-file-barrier ready release))))
             (error (condition) (setf *writer-test-reset-error* (princ-to-string condition)))))
         :name "owned writer epoch reset"))
  (values))

(defun wal-writer-test-start-stop ()
  (setf *writer-test-stop-error* nil
        *writer-test-stop-worker*
        (sb-thread:make-thread
         (lambda ()
           (handler-case (stop-primary-wal-writer)
             (error (condition) (setf *writer-test-stop-error* (princ-to-string condition)))))
         :name "owned writer stop and join"))
  (values))

(defun wal-writer-test-install-flush-failure (evidence after-flush-p)
  (let ((flush (symbol-function 'wal-flush-core))
        (fail-stop (symbol-function 'wal-fail-stop))
        (writer *primary-wal-writer*)
        (failure (make-condition 'simple-error :format-control "owned primary writer flush failure"))
        (reported nil))
    (setf (symbol-function 'wal-flush-core)
          (lambda ()
            (if (eq sb-thread:*current-thread* (primary-wal-writer-thread writer))
                (progn
                  (when after-flush-p (funcall flush))
                  (error failure))
                (funcall flush)))
          (symbol-function 'wal-fail-stop)
          (lambda (condition)
            (when (and (eq sb-thread:*current-thread* (primary-wal-writer-thread writer))
                       (not reported))
              (assert (eq condition failure))
              (assert (eq failure (primary-wal-writer-failure writer)))
              (assert (eq :failed (primary-wal-writer-state writer)))
              (assert (null *backend*))
              (assert (zerop *live-operation-depth*))
              (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
              (assert (not (sb-thread:holding-mutex-p *reload-definition-lock*)))
              (sb-thread:with-mutex (*wal-lock*)
                (assert *wal-poisoned*)
                (assert (eq *writer-test-owner* *wal-pending-page-publication*))
                (assert (eq :pending (wal-page-publication-state *writer-test-owner*)))
                (assert (eql after-flush-p (>= *wal-durable-lsn* *writer-test-end*))))
              (setf reported t)
              (with-open-file (out evidence :direction :output :if-exists :error)
                (write-line "fenced with retained debt" out)))
            (funcall fail-stop condition))))
  (values))

(defun wal-writer-test-prepare-wrap-boundary ()
  (with-wal-publication-fence (begin)
    (sb-thread:with-mutex (*xid-lock*)
      (unless (and (null *active-xids*) (zerop *active-snapshots*))
        (error "wrap test requires no active XIDs or snapshots"))
      (let ((boundary (xid-next-epoch-boundary *next-xid*)))
        (begin)
        (setf *next-xid* boundary *xid-batch-end* boundary)
        (xid-reserve-wal-batch-under-lock))))
  (values))

(defvar *writer-test-pressure-worker* nil)

(defvar *writer-test-pressure-error* nil)

(defun wal-pressure-test-start-incoming-at-page-boundary (pid ready release)
  (let ((pause (symbol-function 'wal-test-pause)))
    (setf (symbol-function 'wal-test-pause)
          (lambda (point)
            (when (and (eq point :after-page-images) *backend*
                       (= pid (backend-pid *backend*)))
              (wal-writer-test-file-barrier ready release))
            (funcall pause point))))
  (setf *writer-test-pressure-error* nil
        *writer-test-pressure-worker*
        (sb-thread:make-thread
         (lambda ()
           (handler-case
               (with-live-operation (wal-writer-test-admit-backend pid))
             (error (condition) (setf *writer-test-pressure-error* (princ-to-string condition)))))
         :name "owned incoming pressure batch"))
  (values))

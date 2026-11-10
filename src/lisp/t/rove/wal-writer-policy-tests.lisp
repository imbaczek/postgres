;;;; Background WAL write and flush scheduling.
(in-package #:pg)

(cl-spec:defgenerator writer-request-arguments ()
  (let* ((end (case (random 4)
                (0 0) (1 (1- (ash 1 64))) (t (random (* 8192 1024)))))
         (flushed (random (1+ end)))
         (async (random (1+ end))))
    (list end flushed async)))

(cl-spec:defspec-function wal-writer-write-request
    (:args (end (type (unsigned-byte 64)))
           (flushed (type (unsigned-byte 64)))
           (async (type (unsigned-byte 64))))
  (:args-generator writer-request-arguments)
  (:pre (and (<= flushed end) (<= async end)))
  (:returns (type (unsigned-byte 64)))
  (:post
   (let ((page (* 8192 (floor end 8192))))
     (cond ((> page flushed) (= result page))
           ((> async flushed) (= result async))
           (t (zerop result))))))

(cl-spec:defgenerator writer-flush-arguments ()
  (let* ((flushed (random (* 8192 256)))
         (request (+ flushed (random (* 8192 256))))
         (now (/ (random 400000) 4))
         (last (when (plusp (random 4)) (/ (random (1+ (* now 4))) 4))))
    (list request flushed last now (1+ (random 10000)) (random 257))))

(cl-spec:defspec-function wal-writer-flush-due-p
    (:args (request (type (unsigned-byte 64)))
           (flushed (type (unsigned-byte 64)))
           (last (type (or null (rational 0 *))))
           (now (type (rational 0 *)))
           (delay (range integer 1 10000))
           (blocks (range integer 0 2147483647)))
  (:args-generator writer-flush-arguments)
  (:pre (or (null last) (<= last now)))
  (:returns (type boolean))
  (:post
   (eq result
       (and (> request flushed)
            (or (null last) (zerop blocks) (>= (- now last) delay)
                (>= (- (floor request 8192) (floor flushed 8192)) blocks))))))

(cl-spec:defproperty writer-request-honors-partial-page-and-async-tail
    ((page (range integer 1 65536)) (tail (range integer 1 8191)))
  (:about wal-writer-write-request)
  (:trials (:normal 100))
  (let* ((boundary (* page 8192)) (end (+ boundary tail)))
    (and (zerop (wal-writer-write-request end boundary 0))
         (= end (wal-writer-write-request end boundary end))
         (= boundary (wal-writer-write-request end (- boundary 8192) end)))))

(cl-spec:defproperty writer-flush-honors-threshold-and-delay-equality
    ((blocks (range integer 1 1024)) (delay (range integer 1 10000)))
  (:about wal-writer-flush-due-p)
  (:trials (:normal 100))
  (let ((end (* blocks 8192)))
    (and (wal-writer-flush-due-p end 8191 0 0 delay blocks)
         (not (wal-writer-flush-due-p (1- end) 8191 0 0 delay blocks))
         (not (wal-writer-flush-due-p end 8191 0 (1- delay) delay (1+ blocks)))
         (wal-writer-flush-due-p end 8191 0 delay delay (1+ blocks))
         (wal-writer-flush-due-p end 8191 nil 0 delay (1+ blocks))
         (wal-writer-flush-due-p end 8191 0 0 delay 0)
         (not (wal-writer-flush-due-p end end nil delay delay 0)))))

(rove:deftest writer-request-selects-completed-pages-before-async-tail
    (dolist (case '((0 0 0 0) (4096 0 0 0) (8191 0 0 0)
                    (8192 0 0 8192) (8200 0 8200 8192)
                    (8200 8192 8200 8200) (8200 8192 8192 0)))
      (destructuring-bind (end flushed async expected) case
        (rove:ok (= expected (wal-writer-write-request end flushed async)))))
  (let ((maximum (1- (ash 1 64))))
    (rove:ok (= (- maximum 8191) (wal-writer-write-request maximum 0 maximum)))
    (rove:ok (zerop (wal-writer-write-request maximum maximum maximum)))))

(rove:deftest writer-flush-checks-first-call-time-and-block-count
    (rove:ok (wal-writer-flush-due-p 100 0 nil 0 200 128))
  (rove:ok (not (wal-writer-flush-due-p 100 0 0 199 200 128)))
  (rove:ok (wal-writer-flush-due-p 100 0 0 200 200 128))
  (rove:ok (wal-writer-flush-due-p 100 0 0 0 200 0))
  (rove:ok (wal-writer-flush-due-p 8192 8191 0 0 200 1))
  (rove:ok (not (wal-writer-flush-due-p 8191 0 0 0 200 1)))
  (rove:ok (not (wal-writer-flush-due-p 8192 8192 nil 1000 200 0))))

(rove:deftest writer-background-scheduling-generated-contracts
    (dolist (function '(wal-writer-write-request wal-writer-flush-due-p))
      (let ((data (cl-spec:result-data
                   (cl-spec:check-function function :trials 100 :seed 237))))
        (rove:ok (eq :passed (getf data :status)))))
  (unit-check-property 'writer-request-honors-partial-page-and-async-tail)
  (unit-check-property 'writer-flush-honors-threshold-and-delay-equality))

(defvar *writer-policy-test-now* 0)

(defun call-with-writer-policy-test (function &key (isolate t))
  (let ((*writer-policy-test-now* 0)
        (*wal-explicit-write-frontier* nil)
        (*wal-async-flush-frontier* nil)
        (clock (symbol-function 'wal-writer-now-ms)))
    (unwind-protect
         (progn
           (setf (symbol-function 'wal-writer-now-ms) (lambda () *writer-policy-test-now*))
           (call-with-primary-writer-authority-test function :isolate isolate))
      (setf (symbol-function 'wal-writer-now-ms) clock))))

(rove:deftest writer-background-separates-explicit-write-from-delayed-flush
    (call-with-writer-policy-test
     (lambda (writer)
       (let ((write (symbol-function 'wal-write-core))
             (flush (symbol-function 'wal-flush-core))
             (policy (make-wal-writer-policy 200 128))
             (events nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-write-core)
                      (lambda () (push :write events) (wal-note-explicit-write))
                      (symbol-function 'wal-flush-core)
                      (lambda ()
                        (push :flush events)
                        (wal-note-explicit-write)
                        (setf *wal-durable-lsn* *wal-end-lsn*))
                      *wal-end-lsn* 4096
                      (primary-wal-writer-last-flush-time writer) 0
                      (primary-wal-writer-last-drained-end writer) 77)
                (rove:ok (eq :continue (primary-wal-writer-background-step writer policy)))
                (rove:ok (null events))
                (setf *wal-end-lsn* 8200 *writer-policy-test-now* 1)
                (primary-wal-writer-background-step writer policy)
                (rove:ok (equal '(:write) events))
                (rove:ok (zerop *wal-durable-lsn*))
                (sb-thread:with-mutex (*wal-lock*)
                  (rove:ok (= 8200 (wal-explicit-written-lsn))))
                (setf *writer-policy-test-now* 199)
                (primary-wal-writer-background-step writer policy)
                (rove:ok (equal '(:write) events))
                (setf *writer-policy-test-now* 200)
                (primary-wal-writer-background-step writer policy)
                (rove:ok (equal '(:flush :write) events))
                (rove:ok (= 8200 *wal-durable-lsn*))
                (rove:ok (= 200 (primary-wal-writer-last-flush-time writer)))
                (rove:ok (= 77 (primary-wal-writer-last-drained-end writer)))
                (primary-wal-writer-background-step writer policy)
                (rove:ok (equal '(:flush :write) events))
                (rove:ok (= 5 (primary-wal-writer-cycles writer))))
           (setf (symbol-function 'wal-write-core) write
                 (symbol-function 'wal-flush-core) flush))))))

(rove:deftest writer-background-keeps-async-pages-and-clog-until-flush
    (call-with-queued-clog-test
     (lambda ()
       (call-with-writer-policy-test
        (lambda (writer)
          (let ((page (page-init (make-page)))
                (*wal-row-page-write-p* t)
                (policy (make-wal-writer-policy 200 128)))
            (setf (aref page 100) 71
                  (primary-wal-writer-policy writer) policy
                  (primary-wal-writer-last-flush-time writer) 0)
            (wal-buffer-write 7311 0 page)
            (multiple-value-bind (end receipt)
                (wal-commit-transaction 44 (lambda (end) (declare (ignore end))
                                             (error "unexpected synchronous callback"))
                                        :source-id :writer-policy :local-durability :async)
              (rove:ok (not (wal-commit-receipt-wal-durable-p receipt)))
              (rove:ok (= +clog-committed+ (clog-get 44)))
              (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44)))
              (primary-wal-writer-background-step writer policy)
              (rove:ok (< *wal-durable-lsn* end))
              (rove:ok *wal-pending-page-publication*)
              (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44)))
              (rove:ok (zerop (smgr-nblocks 7311)))
              (setf *writer-policy-test-now* 200)
              (primary-wal-writer-background-step writer policy)
              (rove:ok (= end *wal-durable-lsn* (primary-wal-writer-last-drained-end writer)))
              (rove:ok (null *wal-pending-page-publication*))
              (rove:ok (= +clog-committed+ (pending-clog-raw-status 44)))
              (rove:ok (= 71 (aref (smgr-read 7311 0) 100))))))
        :isolate nil))))

(rove:deftest writer-background-retires-an-already-durable-async-owner
    (call-with-queued-clog-test
     (lambda ()
       (call-with-writer-policy-test
        (lambda (writer)
          (let ((policy (make-wal-writer-policy 200 128))
                (flush (symbol-function 'wal-flush-core)))
            (setf (primary-wal-writer-policy writer) policy)
            (multiple-value-bind (end receipt)
                (wal-commit-transaction 44 (constantly nil)
                                        :source-id :writer-policy :local-durability :async)
              (declare (ignore receipt))
              (sb-thread:with-mutex (*wal-lock*) (wal-flush))
              (unwind-protect
                   (progn
                     (setf (symbol-function 'wal-flush-core)
                           (lambda () (error "duplicate background flush")))
                     (primary-wal-writer-background-step writer policy)
                     (rove:ok (= end (primary-wal-writer-last-drained-end writer)))
                     (rove:ok (null *wal-pending-page-publication*))
                     (rove:ok (= +clog-committed+ (pending-clog-raw-status 44))))
                (setf (symbol-function 'wal-flush-core) flush)))))
        :isolate nil))))

(rove:deftest writer-explicit-frontier-rejects-a-different-storage-epoch
    (call-with-writer-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (sb-thread:with-mutex (*wal-lock*)
         (setf *wal-end-lsn* 100 *wal-durable-lsn* 10)
         (wal-note-explicit-write)
         (rove:ok (= 100 (wal-explicit-written-lsn)))
         (let ((*wal-storage-incarnation* (gensym "ANOTHER-STORE-")))
           (rove:ok (= 10 (wal-explicit-written-lsn))))
         (let ((*wal-insert-timeline* (1+ *wal-insert-timeline*)))
           (rove:ok (= 10 (wal-explicit-written-lsn))))))))

(rove:deftest writer-async-wakeup-uses-the-captured-policy-and-sleeping-state
    (dolist (case '((128 nil 8191 nil) (1 nil 8192 t) (0 nil 1 t) (128 t 1 t)))
      (destructuring-bind (blocks sleeping end expected) case
        (call-with-writer-policy-test
         (lambda (writer)
           (setf (primary-wal-writer-policy writer) (make-wal-writer-policy 200 blocks)
                 (primary-wal-writer-sleeping-p writer) sleeping)
           (sb-thread:with-mutex (*wal-lock*)
             (notify-primary-wal-writer-async-under-lock end))
           (rove:ok (= (if expected 1 0)
                       (sb-thread:semaphore-count (primary-wal-writer-wake writer)))))))))

(rove:deftest writer-background-honors-an-explicit-empty-fifo-flush-request
    (call-with-queued-clog-test
     (lambda ()
       (call-with-writer-policy-test
        (lambda (writer)
          (let ((policy (make-wal-writer-policy 200 128)) (end nil))
            (setf (primary-wal-writer-policy writer) policy
                  (primary-wal-writer-last-flush-time writer) 0
                  (primary-wal-writer-last-drained-end writer) 77)
            (sb-thread:with-mutex (*wal-lock*)
              (wal-append-batch-start)
              (setf end (wal-append-record +wal-record-maintenance+ 0
                                           (make-array 0 :element-type '(unsigned-byte 8))))
              (wal-request-async-flush-under-lock end)
              (rove:ok (null *wal-pending-page-publication*))
              (rove:ok (= end (wal-async-flush-lsn))))
            (primary-wal-writer-background-step writer policy)
            (rove:ok (< *wal-durable-lsn* end))
            (setf *writer-policy-test-now* 200)
            (primary-wal-writer-background-step writer policy)
            (rove:ok (= end *wal-durable-lsn*))
            (rove:ok (= 77 (primary-wal-writer-last-drained-end writer)))
            (sb-thread:with-mutex (*wal-lock*)
              (let ((*wal-storage-incarnation* (gensym "ANOTHER-ASYNC-STORE-")))
                (rove:ok (zerop (wal-async-flush-lsn)))))))
        :isolate nil))))

(rove:deftest writer-settings-parse-c-millisecond-and-xblock-units
    (dolist (case '(("wal_writer_delay" "200ms" 200)
                    ("wal_writer_delay" "1s" 1000)
                    ("wal_writer_delay" "10s" 10000)
                    ("wal_writer_delay" "1000us" 1)
                    ("wal_writer_flush_after" "1MB" 128)
                    ("wal_writer_flush_after" "8kB" 1)
                    ("wal_writer_flush_after" "8192B" 1)
                    ("wal_writer_flush_after" "4kB" 0)
                    ("wal_writer_flush_after" "12kB" 2)
                    ("wal_writer_flush_after" "0" 0)))
      (destructuring-bind (name input expected) case
        (rove:ok (= expected (startup-normalize-setting name input)))))
  (dolist (case '(("wal_writer_delay" "0") ("wal_writer_delay" "10001")
                  ("wal_writer_delay" "1us") ("wal_writer_flush_after" "-1")
                  ("wal_writer_flush_after" "2147483648")
                  ("wal_writer_flush_after" "1mB")))
    (destructuring-bind (name input) case
      (rove:ok (handler-case (progn (startup-normalize-setting name input) nil)
                 (error () t))))))

(rove:deftest writer-settings-capture-one-root-and-expose-c-metadata
    (let* ((settings (startup-default-settings "/tmp/writer-policy-settings/"))
           (*startup-installed-configuration* (make-startup-configuration :settings settings))
           (*primary-wal-writer-delay* nil))
      (let ((policy (primary-wal-writer-capture-policy)))
        (rove:ok (= 200 (wal-writer-policy-delay policy)))
        (rove:ok (= 128 (wal-writer-policy-flush-after policy))))
      (dolist (case '(("wal_writer_delay" "200" "ms" "200ms")
                      ("wal_writer_flush_after" "128" "8kB" "1MB")))
        (destructuring-bind (name value unit show) case
          (let ((row (startup-settings-row name (gethash name settings))))
            (rove:ok (equal value (aref row 1)))
            (rove:ok (equal unit (aref row 2)))
            (rove:ok (equal "sighup" (aref row 3)))
            (rove:ok (equal "integer" (aref row 4)))
            (rove:ok (not (aref row 9))))
          (rove:ok (equal show (startup-setting-show-value name)))
          (rove:ok (equal "55P02"
                          (handler-case (startup-check-session-setting name)
                            (pg-error (condition) (pg-error-sqlstate condition)))))))))

(rove:deftest writer-background-idle-cycles-enter-hibernation-and-remain-wakeable
    (call-with-writer-policy-test
     (lambda (writer)
       (let ((policy (make-wal-writer-policy 200 128)))
         (dotimes (index 50) (primary-wal-writer-background-step writer policy))
         (rove:ok (= 50 (primary-wal-writer-idle-cycles writer)))
         (rove:ok (primary-wal-writer-sleeping-p writer))
         (notify-primary-wal-writer-configuration)
         (rove:ok (= 1 (sb-thread:semaphore-count (primary-wal-writer-wake writer))))
         (primary-wal-writer-wait writer policy)
         (rove:ok (not (primary-wal-writer-wake-pending-p writer)))
         (setf *wal-end-lsn* 1)
         (sb-thread:with-mutex (*wal-lock*) (wal-request-async-flush-under-lock 1))
         (rove:ok (= 1 (sb-thread:semaphore-count (primary-wal-writer-wake writer))))))))

(rove:deftest writer-stop-forces-an-unmarked-partial-page-without-a-fifo
    (call-with-queued-clog-test
     (lambda ()
       (let ((*primary-wal-writer* nil) (*pm-requested-mode* +no-shutdown+))
         (sb-thread:with-mutex (*wal-lock*)
           (wal-append-batch-start)
           (wal-append-record +wal-record-maintenance+ 0
                              (make-array 0 :element-type '(unsigned-byte 8))))
         (let ((end *wal-end-lsn*))
           (rove:ok (< end 8192))
           (rove:ok (null *wal-pending-page-publication*))
           (rove:ok (< *wal-durable-lsn* end))
           (stop-primary-wal-writer)
           (rove:ok (= end *wal-durable-lsn*)))))))

(rove:deftest writer-background-rejects-stopping-and-stale-authority-before-effects
    (dolist (scenario '(:stop :incarnation :timeline))
      (call-with-writer-policy-test
       (lambda (writer)
         (let ((policy (make-wal-writer-policy 200 0)))
           (setf *wal-end-lsn* 8192)
           (ecase scenario
             (:stop (setf (primary-wal-writer-stop-p writer) t))
             (:incarnation (setf *wal-storage-incarnation* (gensym "STALE-WRITER-")))
             (:timeline (incf *wal-insert-timeline*)))
           (let ((result
                   (handler-case (primary-wal-writer-background-step writer policy)
                     (error () :error))))
             (rove:ok (eq (if (eq scenario :stop) :stop :error) result)))
           (rove:ok (zerop *wal-durable-lsn*))
           (rove:ok (zerop (primary-wal-writer-cycles writer)))
           (rove:ok (null (primary-wal-writer-policy writer))))))))

(rove:deftest writer-write-only-errors-and-nonlocal-exits-poison-before-unlock
    (dolist (escape '(nil t))
      (call-with-writer-policy-test
       (lambda (writer)
         (let ((write (symbol-function 'wal-write-core))
               (observed nil)
               (policy (make-wal-writer-policy 200 128)))
           (setf *wal-end-lsn* 8192 (primary-wal-writer-last-flush-time writer) 0)
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-write-core)
                        (lambda ()
                          (if escape (throw 'writer-write-escape :escaped)
                              (error "injected background write failure"))))
                  (handler-case
                      (catch 'writer-write-escape
                        (primary-wal-writer-background-step writer policy))
                    (error (condition) (setf observed condition)))
                  (rove:ok observed)
                  (rove:ok *wal-poisoned*)
                  (rove:ok (zerop *wal-durable-lsn*))
                  (rove:ok (zerop (primary-wal-writer-cycles writer)))
                  (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*))))
             (setf (symbol-function 'wal-write-core) write)))))))

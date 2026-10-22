;;;; arena-tests.lisp -- sort ownership, capacity failures, and backend isolation.

(in-package #:pg)

(defun sort-arena-test-supported-p ()
  ;; Inspect SBCL itself, not the wrapper under test. A wrapper regression on
  ;; a supported runtime must fail the suite rather than silently skip tests.
  (let ((condition (find-symbol "ARENA-EXHAUSTED-ERROR" "SB-KERNEL"))
        (handler (find-symbol "*ARENA-EXHAUSTION-HANDLER*" "SB-VM")))
    (and condition (find-class condition nil) handler (boundp handler)
         (every (lambda (name)
                  (let ((symbol (find-symbol name "SB-VM")))
                    (and symbol (fboundp symbol))))
                '("NEW-ARENA" "ARENA-P" "SWITCH-TO-ARENA"
                  "REWIND-ARENA" "DESTROY-ARENA")))))

(defun sort-arena-test-stream (rows callback)
  (stream-select-sort
   (lambda (visit)
     (dolist (values rows)
       (funcall visit (cons values values))))
   '(((:col 1 :type :int4) . nil)) callback))

(defun test-sort-arena-option-parsing ()
  (check (= (nth-value 8 (parse-args '("--sort-arena-limit-bytes" "65536")))
            65536)
         "sort arena option accepts its minimum capacity")
  (check (null (nth-value 8 (parse-args '("--sort-arena-limit-bytes=-1"))))
         "sort arena option can disable arenas")
  (dolist (value '("0" "65535" "-2" "bad" "65536junk"))
    (check (handler-case (progn (parse-query-sort-arena-limit-bytes value) nil)
             (error () t))
           "sort arena option rejects invalid capacities"))
  (let ((*query-sort-arena-limit-bytes* nil)
        (original (symbol-function 'make-query-sort-arena))
        (rows nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'make-query-sort-arena)
                 (lambda (limit)
                   (declare (ignore limit))
                   (error "disabled sorting must not require SBCL arenas")))
           (sort-arena-test-stream '(#(2) #(1))
                                   (lambda (row) (push (aref (car row) 0) rows)))
           (check (equal (nreverse rows) '(1 2))
                  "disabled sort arenas do not consult the runtime arena API"))
      (setf (symbol-function 'make-query-sort-arena) original))))

(defun test-sort-arena-output-survives-release ()
  (dolist (work-memory '(1 1048576))
    (let* ((*query-sort-arena-limit-bytes* 65536)
           (*query-work-memory-bytes* work-memory)
           (directory (make-query-spill-test-directory))
           (*query-temp-directory* directory)
           (input (loop for key in '(2 1 2 1)
                        for label in '("A" "B" "C" "D")
                        collect (vector key (copy-seq label)
                                        (make-array 3 :element-type '(unsigned-byte 8)
                                                      :initial-contents '(0 128 255))
                                        (+ (expt 2 100) key) +null+)))
           (output nil))
      (unwind-protect
           (progn
             (sort-arena-test-stream
              input
              (lambda (row)
                (check (null (funcall (query-sort-arena-function "POINTS-TO-ARENA") row))
                       "sort consumers receive only heap-owned compound values")
                (push row output)
                ;; There are still arena-held items during the no-spill path.
                (sb-ext:gc :full t)))
             (setf output (nreverse output))
             (sb-ext:gc :full t)
             (check (equal (mapcar (lambda (row) (aref (car row) 1)) output)
                           '("B" "D" "A" "C"))
                    "released arenas preserve stable sort output after full GC")
             (check (every (lambda (row)
                             (and (typep (aref (car row) 2)
                                         '(vector (unsigned-byte 8)))
                                  (equalp (aref (car row) 2) #(0 128 255))
                                  (= (aref (car row) 3)
                                     (+ (expt 2 100) (aref (car row) 0)))
                                  (eq (aref (car row) 4) +null+)))
                           output)
                    "arena output preserves BYTEA, borrowed bignums, and NULL")
             (setf (char (aref (car (first output)) 1) 0) #\X)
             (check (string= (aref (second input) 1) "B")
                    "sort output does not alias input strings")
             (let ((count 0))
               (check (eq :stop
                          (sort-arena-test-stream
                           input (lambda (row) (declare (ignore row))
                                   (incf count) :stop)))
                      "arena sort propagates early stop")
               (check (= count 1) "arena sort stops after one consumer row"))
             (check (query-spill-test-directory-empty-p directory)
                    "arena sort and early stop reclaim spill files"))
        (uiop:delete-directory-tree directory :validate t
                                              :if-does-not-exist :ignore)))))

(defun test-sort-arena-exhaustion-cleans-spills ()
  (let* ((*query-sort-arena-limit-bytes* 65536)
         (*query-work-memory-bytes* 2048)
         (directory (make-query-spill-test-directory))
         (*query-temp-directory* directory)
         (saw-spill nil)
         (condition nil))
    (unwind-protect
         (progn
           (handler-case
               (stream-select-sort
                (lambda (visit)
                  (dotimes (index 32) (funcall visit (cons (vector index) #(0))))
                  (setf saw-spill (not (query-spill-test-directory-empty-p directory)))
                  (funcall visit (cons (vector (make-string 131072 :initial-element #\x))
                                       #(0))))
                '(((:col 1 :type :int4) . nil))
                (lambda (row) (declare (ignore row))))
             (pg-error (caught) (setf condition caught)))
           (check saw-spill "arena capacity test fails after a spill already exists")
           (check (and condition (string= (pg-error-sqlstate condition) "53200"))
                  "actual arena allocation failure becomes SQLSTATE 53200")
           (sb-ext:gc :full t)
           (check (search "65536" (pg-error-message condition))
                  "capacity diagnostic remains readable after arena destruction")
           (check (query-spill-test-directory-empty-p directory)
                  "arena exhaustion removes previously written runs")
           (let ((count 0))
             (sort-arena-test-stream '(#(2) #(1))
                                     (lambda (row) (declare (ignore row)) (incf count)))
             (check (= count 2) "the same thread can sort after arena exhaustion")))
      (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(defun test-sort-arena-is-only-scalar-root ()
  (let ((*query-sort-arena-limit-bytes* 65536)
        (*query-work-memory-bytes* 1048576)
        (output nil))
    (stream-select-sort
     (lambda (visit)
       ;; No input collection survives the streamer. After the batch is copied,
       ;; future rows' bignums have no live heap holder outside the arena.
       (loop for index from 32 downto 1
             do (let ((values (vector index (+ (expt 2 200) index))))
                  (funcall visit (cons values values)))))
     '(((:col 1 :type :int4) . nil))
     (lambda (row)
       (push (car row) output)
       (sb-ext:gc :full t)))
    (sb-ext:gc :full t)
    (check (and (= (length output) 32)
                (every (lambda (row)
                         (= (aref row 1) (+ (expt 2 200) (aref row 0))))
                       output))
           "visible arena roots preserve otherwise unreferenced heap scalars")))

(defun test-sort-arena-errors-own-diagnostics ()
  (let* ((*query-sort-arena-limit-bytes* 65536)
         (*query-work-memory-bytes* 1)
         (directory (make-query-spill-test-directory))
         (*query-temp-directory* directory)
         (original (symbol-function 'write-query-sort-run)))
    (unwind-protect
         (dolist (state '("57014" "53400"))
           (let ((condition nil))
             (setf (symbol-function 'write-query-sort-run)
                   (lambda (items order)
                     (declare (ignore order))
                     ;; Deliberately borrow diagnostic text from arena data.
                     (error (make-condition 'pg-error :sqlstate state
                                                      :message (aref (car (fourth (first items))) 1)))))
             (handler-case
                 (sort-arena-test-stream (list (vector 1 (copy-seq "arena diagnostic")))
                                         (lambda (row) (declare (ignore row))))
               (pg-error (caught) (setf condition caught)))
             (sb-ext:gc :full t)
             (check (and condition (string= (pg-error-sqlstate condition) state)
                         (string= (pg-error-message condition) "arena diagnostic")
                         (not (funcall (query-sort-arena-function "POINTS-TO-ARENA")
                                       condition)))
                    "cancellation/quota errors preserve heap-owned diagnostics")
             (check (query-spill-test-directory-empty-p directory)
                    "non-OOM errors release sort spill resources")))
      (setf (symbol-function 'write-query-sort-run) original)
      (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(defun test-sort-arena-concurrent-capacities ()
  (let* ((input (loop for index below 1000 collect (vector index)))
         (threads
           (loop for capacity in '(65536 2097152)
                 collect
                 (let ((limit capacity))
                   (sb-thread:make-thread
                    (lambda ()
                      (let ((*query-sort-arena-limit-bytes* limit)
                            (*query-work-memory-bytes* 1048576)
                            (count 0))
                        (handler-case
                            (progn
                              (sort-arena-test-stream
                               input (lambda (row) (declare (ignore row)) (incf count)))
                              count)
                          (pg-error (condition) (pg-error-sqlstate condition))))))))))
    (check (string= (sb-thread:join-thread (first threads)) "53200")
           "one backend independently exhausts its arena")
    (check (= (sb-thread:join-thread (second threads)) 1000)
           "another backend completes with its own capacity")))

(defun test-sort-arena-interrupt-after-switch ()
  (let ((*query-sort-arena-limit-bytes* 65536)
        (*query-work-memory-bytes* 1048576)
        (original (symbol-function 'query-sort-arena-function))
        (queued-p nil)
        (destroyed-p nil)
        (condition nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'query-sort-arena-function)
                 (lambda (name)
                   (let ((function (funcall original name)))
                     (cond
                       ((string= name "SWITCH-TO-ARENA")
                        (lambda (arena)
                          (prog1 (funcall function arena)
                            (when (and (not (eql arena 0)) (not queued-p))
                              (setf queued-p t)
                              ;; Queued while interrupts are disabled, then
                              ;; delivered only after the unwind guard exists.
                              (sb-thread:interrupt-thread
                               sb-thread:*current-thread*
                               (lambda () (ereport "57014" "arena switch canceled")))))))
                       ((string= name "DESTROY-ARENA")
                        (lambda (arena)
                          (prog1 (funcall function arena)
                            (setf destroyed-p t))))
                       (t function)))))
           (handler-case
               (sort-arena-test-stream '(#(1))
                                       (lambda (row) (declare (ignore row))))
             (pg-error (caught) (setf condition caught)))
           (check (and queued-p destroyed-p condition
                       (string= (pg-error-sqlstate condition) "57014"))
                  "interrupt after arena switch unwinds and destroys the arena")
           (sb-ext:gc :full t)
           (check (string= (pg-error-message condition) "arena switch canceled")
                  "interrupt diagnostic survives release and full GC"))
      (setf (symbol-function 'query-sort-arena-function) original))))

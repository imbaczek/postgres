;;;; spill-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-hash-aggregate-skew-does-not-spill-unnecessarily
    (let ((actual nil))
      (let ((*query-work-memory-bytes* 1))
        (stream-hash-aggregate
         (lambda (callback)
           (dotimes (index 100) (funcall callback #(7))))
         '((:col 1 :type :int4)) 1
         (lambda (row) (push (list (aref row 0) (aref row 1)) actual))))
      (rove:ok (equal actual '((7 100)))
               "a single heavily skewed group stays in one bounded hash state")))

(rove:deftest replay-readers-preserve-independent-row-presence
    (let ((*query-temp-directory* (uiop:temporary-directory)))
      (with-query-temp-files
        (dolist (limit '(1 100000))
          (let* ((count 0)
                 (source-presence (vector 11 12))
                 (producer (make-sql-iterator
                            (lambda ()
                              (when (< count 2)
                                (incf count)
                                (values (vector +null+) t nil nil
                                        (if (= count 1) source-presence #()))))))
                 (store (make-query-replay-store producer limit))
                 (first-reader (make-query-replay-reader store))
                 (second-reader (make-query-replay-reader store)))
            (unwind-protect
                 (progn
                   (multiple-value-bind (row present origins toast-context presence)
                       (sql-iterator-next first-reader)
                     (declare (ignore toast-context))
                     (rove:ok present)
                     (rove:ok (eq +null+ (aref row 0)))
                     (rove:ok (null origins))
                     (rove:ok (equalp #(11 12) presence))
                     (when (plusp (length presence)) (setf (aref presence 0) 99)))
                   (multiple-value-bind (row present origins toast-context presence)
                       (sql-iterator-next second-reader)
                     (declare (ignore row origins toast-context))
                     (rove:ok present)
                     (rove:ok (equalp #(11 12) presence))
                     (rove:ok (equalp #(11 12) source-presence)))
                   (multiple-value-bind (row present origins toast-context presence)
                       (sql-iterator-next first-reader)
                     (declare (ignore origins toast-context))
                     (rove:ok present)
                     (rove:ok (eq +null+ (aref row 0)))
                     (rove:ok (equalp #() presence))))
              (close-query-replay-store store)))))))

(rove:deftest iterator-mapping-preserves-toast-and-row-presence-separately
    (let* ((done nil)
           (context (list :toast-context))
           (presence (vector 21))
           (producer (make-sql-iterator
                      (lambda ()
                        (unless done
                          (setf done t)
                          (values #(1) t nil context presence)))))
           (mapped (make-mapping-sql-iterator
                    producer
                    (lambda (row)
                      (rove:ok (eq context *toast-slice-current*))
                      (rove:ok (equalp presence *query-row-presence*))
                      (values row t nil))
                    :preserve-toast-context t)))
      (unwind-protect
           (multiple-value-bind (row present origins toast output-presence)
               (sql-iterator-next mapped)
             (declare (ignore row origins))
             (rove:ok present)
             (rove:ok (eq context toast))
             (rove:ok (equalp presence output-presence)))
        (close-sql-iterator mapped))))

(rove:deftest query-source-identities-survive-namespace-copies
    (let* ((table (make-pg-class :oid 0 :name "empty" :columns #()))
           (source (list :name "r" :alias "r" :table table :offset 0 :width 0))
           (other (list :name "r" :alias "r" :table table :offset 0 :width 0))
           (first (select-source-scope-relation source))
           (second (select-source-scope-relation source))
           (third (select-source-scope-relation other)))
      (rove:ok (integerp (select-relation-identity first)))
      (rove:ok (eql (getf source :identity) (select-relation-identity first)))
      (rove:ok (eql (select-relation-identity first) (select-relation-identity second)))
      (rove:ok (eql (select-relation-identity first)
                    (select-relation-identity (copy-select-relation first))))
      (rove:ok (not (eql (select-relation-identity first) (select-relation-identity third))))))

(rove:deftest whole-row-resolution-retains-source-and-lexical-depth
    (let* ((table (make-pg-class :oid 0 :name "empty" :columns #()))
           (source (list :name "r" :alias "r" :table table :offset 0 :width 0))
           (relation (select-source-scope-relation source))
           (scope (make-select-scope :parent nil :relations (list relation)))
           (expression (analyze-expr '(:whole-row "r") scope))
           (outer (analyze-expr '(:whole-row "r")
                                (make-select-scope :parent scope))))
      (rove:ok (eq (first expression) :whole-row))
      (rove:ok (eql (second expression) (getf source :identity)))
      (rove:ok (null (third expression)))
      (rove:ok (eq (expr-type expression) :record))
      (rove:ok (equal outer (list :outer-ref 1 expression :type :record :typmod -1)))
      (flet ((state (relations)
               (handler-case
                   (progn (analyze-expr '(:whole-row "r")
                                        (make-select-scope :parent nil :relations relations))
                          nil)
                 (pg-error (condition) (pg-error-sqlstate condition)))))
        (rove:ok (equal (state nil) "42P01"))
        (rove:ok (equal (state (list relation (copy-select-relation relation))) "42P09"))
        (let ((forbidden (copy-select-relation relation)))
          (setf (select-relation-lateral-forbidden-p forbidden) t)
          (rove:ok (equal (state (list forbidden relation)) "42P10"))))))

(rove:deftest whole-row-evaluation-distinguishes-absence-from-null-fields
    (let ((expression '(:whole-row 37 ((:col 1 :type :int4 :typmod -1))
                        :type :record :typmod -1 :fields (("id" :int4)))))
      (let* ((*query-row-presence* #(37))
             (record (eval-expr expression (vector +null+))))
        (rove:ok (sql-record-p record))
        (rove:ok (equal (sql-record-fields record) '(("id" :int4))))
        (rove:ok (equalp (sql-record-values record) (vector +null+))))
      (let ((*query-row-presence* #()))
        (rove:ok (eq (eval-expr expression (vector +null+)) +null+)))
      (let ((*query-row-presence* nil))
        (rove:ok
         (handler-case (progn (eval-expr expression (vector +null+)) nil)
           (pg-error (condition) (equal (pg-error-sqlstate condition) "XX000"))))))
  (let ((*query-row-presence* #(38)))
    (rove:ok (sql-record-p
              (eval-expr '(:whole-row 38 nil :type :record :typmod -1 :fields nil)
                         #())))))

(rove:deftest scan-presence-retains-zero-column-source-identity
    (let* ((table (make-pg-class :oid 42 :name "empty" :columns #()))
           (source (list :name "r" :table table
                         :row-source (lambda () (list #()))))
           (identity (ensure-select-source-identity source))
           (iterator (make-select-scan-iterator '(:seq-scan 0 42) (list source))))
      (unwind-protect
           (multiple-value-bind (row present origins toast presence)
               (sql-iterator-next iterator)
             (rove:ok present)
             (rove:ok (equalp row #()))
             (rove:ok (null origins))
             (rove:ok (null toast))
             (rove:ok (equalp presence (vector identity)))
             (let ((*query-row-presence* presence))
               (rove:ok (sql-record-p
                         (eval-expr (list :whole-row identity nil :type :record
                                                                  :typmod -1 :fields nil) row))))
             (rove:ok (not (nth-value 1 (sql-iterator-next iterator)))))
        (close-sql-iterator iterator))))

(rove:deftest outer-whole-row-evaluation-restores-frame-presence
    (let* ((*query-row-presence* #(41))
           (environment (vector +null+))
           (frame (make-subquery-parameter-frame environment :copy-p t))
           (reference '(:outer-ref 1
                        (:whole-row 41 ((:col 1 :type :int4))
                         :type :record :fields (("id" :int4)))))
           (*subquery-outer-frames* (list frame))
           (*query-row-presence* #(99)))
      (setf (aref environment 0) 7)
      (let ((record (eval-expr reference #())))
        (rove:ok (sql-record-p record))
        (rove:ok (eq (aref (sql-record-values record) 0) +null+)))
      (rove:ok (equalp *query-row-presence* #(99)))
      (rove:ok (equalp (gethash +subquery-frame-presence-key+ (cdr frame)) #(41)))
      (let* ((*query-row-presence* #())
             (absent (make-subquery-parameter-frame environment))
             (*subquery-outer-frames* (list absent))
             (*query-row-presence* #(41)))
        (rove:ok (eq (eval-expr reference #()) +null+)))
      (let* ((*query-row-presence* #(42))
             (inner (make-subquery-parameter-frame #()))
             (*subquery-outer-frames* (list inner frame)))
        (rove:ok
         (sql-record-p
          (eval-expr (list :outer-ref 2 (third reference)) #()))))))

(rove:deftest composite-json-preserves-fields-and-normalizes-jsonb
    (let* ((child (make-sql-record :fields '(("x" :int4))
                                   :values (vector +null+)))
           (record (make-sql-record
                    :fields '(("z" :int4) ("a" :json) ("z" :text) ("child" :record))
                    :values (vector 1 "{\"raw\" : 2}" "last" child))))
      (rove:ok
       (equal (sql-to-json record :record)
              "{\"z\":1,\"a\":{\"raw\" : 2},\"z\":\"last\",\"child\":{\"x\":null}}"))
      (rove:ok
       (equal (sql-jsonb-output (sql-to-jsonb record :record))
              "{\"a\": {\"raw\": 2}, \"z\": \"last\", \"child\": {\"x\": null}}"))
      (rove:ok (equal (sql-to-json (make-sql-record :fields nil :values #()) :record) "{}"))
      (rove:ok (equal (sql-to-json +null+ :record) "null"))
      (rove:ok (eq (sql-to-jsonb +null+ :record) +null+))))

(cl-spec:defgenerator json-text-writer-arguments ()
  (let ((alphabet #(#\a #\" #\\ #\Newline #\Tab #\ą #\€ #\😀)))
    (list (coerce (loop repeat (random 5000)
                        collect (aref alphabet (random (length alphabet)))) 'string)
          :text)))

(cl-spec:defspec-function sql-to-json
  "Typed text becomes a JSON string with the same Unicode characters."
  (:args (value (type string)) (type (member :text)))
  (:args-generator json-text-writer-arguments)
  (:returns (type string))
  (:post (equal value (sql-jsonb-value (sql-jsonb-input result)))))

(cl-spec:defproperty json-text-rendering-retains-unicode
    ((size (range integer 0 5000)) (choice (range integer 0 7)))
  (:about sql-to-json)
  (:trials (:normal 100))
  (let* ((alphabet #(#\a #\" #\\ #\Newline #\Tab #\ą #\€ #\😀))
         (text (make-string size :initial-element (aref alphabet choice))))
    (equal text (sql-jsonb-value (sql-jsonb-input (sql-to-json text :text))))))

(rove:deftest json-text-rendering-contracts
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'sql-to-json :trials 100 :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data)))
  (unit-check-property 'json-text-rendering-retains-unicode))

(rove:deftest spool-prefix-reading-keeps-transition-appendable
    (let ((*query-temp-directory* (uiop:temporary-directory)))
      (dolist (memory '(1 100000))
        (with-query-temp-files ()
                               (with-query-row-spool (spool memory)
                                 (query-row-spool-append spool #(1))
                                 (let ((first nil))
                                   (map-query-row-spool-prefix spool (lambda (row index)
                                                                       (declare (ignore index))
                                                                       (push row first)))
                                   (rove:ok (equalp first '(#(1))))
                                   (rove:ok (not (query-row-spool-sealed-p spool))))
                                 (query-row-spool-append spool #(2))
                                 (let ((second nil))
                                   (map-query-row-spool-prefix spool (lambda (row index)
                                                                       (declare (ignore index))
                                                                       (push row second)))
                                   (rove:ok (equalp (nreverse second) '(#(1) #(2))))
                                   (rove:ok (not (query-row-spool-sealed-p spool)))))))))

(cl-spec:defproperty json-aggregate-byte-limit-counts-utf8-and-escaping
    ((size (range integer 0 128)) (choice (range integer 0 5))
                                  (delta (range integer -1 1)))
  (:about json-aggregate-result)
  (:trials (:normal 75))
  (let* ((alphabet #(#\a #\€ #\😀 #\" #\Newline #\\))
         (text (make-string size :initial-element (aref alphabet choice)))
         (expected (concatenate 'string "[" (sql-to-json text :text) "]"))
         (limit (+ (length (string->bytes expected)) delta))
         (*query-work-memory-bytes* 1)
         (*query-temp-directory* (uiop:temporary-directory)))
    (with-query-temp-files ()
                           (progv '(*json-aggregate-output-max-bytes*) (list limit)
                             (handler-case
                                 (let ((rows (subquery-test-rows (format nil "SELECT json_agg('~a'::text)" text))))
                                   (and (not (minusp delta)) (equal rows (list (list expected)))))
                               (pg-error (condition)
                                 (and (minusp delta) (equal (pg-error-sqlstate condition) "54000"))))))))

(rove:deftest json-aggregate-byte-limit-property
    (unit-check-property 'json-aggregate-byte-limit-counts-utf8-and-escaping))

(rove:deftest json-aggregate-coalesces-small-utf8-transitions
    (let ((*query-temp-directory* (uiop:temporary-directory))
          (*query-work-memory-bytes* 256))
      (with-query-temp-files ()
                             (let* ((descriptors '((:kind :json-agg :input (:col 1 :type :text) :type :json)))
                                    (states (make-aggregate-transition-states descriptors))
                                    (token (sql-to-json "€" :text)))
                               (unwind-protect
                                    (progn
                                      (dotimes (index 300)
                                        (step-aggregate-transition-states states descriptors (vector "€") 0))
                                      (rove:ok (< (query-row-spool-count
                                                   (json-aggregate-transition-spool (aref states 0))) 50))
                                      (rove:ok (equal (aref (window-transition-results states) 0)
                                                      (format nil "[~{~a~^, ~}]" (make-list 300 :initial-element token)))))
                                 (close-aggregate-transition-states states))))))

(rove:deftest json-aggregate-batches-around-large-utf8-fragments
    (let ((*query-temp-directory* (uiop:temporary-directory))
          (*query-work-memory-bytes* 256))
      (with-query-temp-files ()
                             (let* ((descriptors '((:kind :json-agg :input (:col 1 :type :text) :type :json)))
                                    (states (make-aggregate-transition-states descriptors))
                                    (large (make-string 5000 :initial-element #\😀))
                                    (values (list "start" large "end")))
                               (unwind-protect
                                    (progn
                                      (dolist (value values)
                                        (step-aggregate-transition-states states descriptors (vector value) 0))
                                      (rove:ok
                                       (equal (aref (window-transition-results states) 0)
                                              (format nil "[~{~a~^, ~}]"
                                                      (mapcar (lambda (value) (sql-to-json value :text)) values))))
                                      (let* ((state (aref states 0))
                                             (spool (json-aggregate-transition-spool state))
                                             (pending (json-aggregate-transition-pending state)))
                                        (rove:ok (query-row-spool-pathname spool))
                                        (rove:ok (<= (+ (query-row-spool-memory-limit spool)
                                                        (array-total-size pending))
                                                     *query-work-memory-bytes*))))
                                 (close-aggregate-transition-states states))))))

(rove:deftest json-aggregate-limit-stops-before-later-input
    (let ((*query-temp-directory* (uiop:temporary-directory))
          (*query-work-memory-bytes* 1)
          (*json-aggregate-output-max-bytes* 4)
          (visited 0))
      (rove:ok
       (handler-case
           (progn
             (stream-sorted-aggregates
              (lambda (callback)
                (incf visited)
                (funcall callback #("oversized"))
                (incf visited)
                (error "later input must not be evaluated"))
              nil '((:kind :json-agg :input (:col 1 :type :text) :type :json))
              (lambda (row) (declare (ignore row)) nil))
             nil)
         (pg-error (condition) (equal (pg-error-sqlstate condition) "54000"))))
      (rove:ok (= visited 1))))

(rove:deftest json-aggregate-freezes-session-formatting-at-transition
    (let ((*query-temp-directory* (uiop:temporary-directory))
          (*backend* (make-backend)))
      (dolist (memory '(1 100000))
        (setf (gethash "timezone" (backend-settings *backend*)) "UTC")
        (with-query-temp-files ()
                               (let* ((*query-work-memory-bytes* memory)
                                      (descriptors '((:kind :json-agg :input (:col 1 :type :timestamptz) :type :json)))
                                      (states (make-aggregate-transition-states descriptors))
                                      (value (sql-cast-value "2020-01-01 00:00:00+00" :text :timestamptz -1))
                                      (first (sql-to-json value :timestamptz)))
                                 (unwind-protect
                                      (progn
                                        (step-aggregate-transition-states states descriptors (vector value) 0)
                                        (setf (gethash "timezone" (backend-settings *backend*)) "Europe/Warsaw")
                                        (let ((second (sql-to-json value :timestamptz)))
                                          (rove:ok (not (equal first second)))
                                          (rove:ok (equal (aref (window-transition-results states) 0)
                                                          (concatenate 'string "[" first "]")))
                                          (step-aggregate-transition-states states descriptors (vector value) 0)
                                          (rove:ok (equal (aref (window-transition-results states) 0)
                                                          (concatenate 'string "[" first ", " second "]")))))
                                   (close-aggregate-transition-states states)))))))

(rove:deftest json-aggregate-owns-inputs-and-window-results
    (let ((*query-temp-directory* (uiop:temporary-directory)))
      (dolist (memory '(1 100000))
        (with-query-temp-files ()
                               (let* ((*query-work-memory-bytes* memory)
                                      (descriptors '((:kind :json-agg :input (:col 1 :type :record) :type :json)))
                                      (states (make-aggregate-transition-states descriptors))
                                      (input (make-sql-record :fields '(("label" :text))
                                                              :values (vector (copy-seq "before")))))
                                 (unwind-protect
                                      (progn
                                        (step-aggregate-transition-states states descriptors (vector input) 0)
                                        (setf (char (aref (sql-record-values input) 0) 0) #\X)
                                        (let ((first (aref (window-transition-results states) 0)))
                                          (rove:ok (equal first "[{\"label\":\"before\"}]"))
                                          (setf (char first 1) #\X))
                                        (rove:ok (not (query-row-spool-sealed-p
                                                       (json-aggregate-transition-spool (aref states 0)))))
                                        (step-aggregate-transition-states
                                         states descriptors
                                         (vector (make-sql-record :fields '(("label" :text)) :values (vector "after"))) 0)
                                        (let ((expected (format nil "[{\"label\":\"before\"}, ~% {\"label\":\"after\"}]")))
                                          (rove:ok (equal (aref (window-transition-results states) 0) expected))
                                          (rove:ok (equal (aref (finalize-aggregate-transition-states states) 0) expected))))
                                   (close-aggregate-transition-states states)))))))

(rove:deftest json-aggregate-cleans-spills-on-stop-consumer-error-and-limit
    (let* ((directory (merge-pathnames (format nil "pg-json-aggregate-~a/" (gensym))
                                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-work-memory-bytes* 1)
           (descriptors '((:kind :json-agg :input (:col 2 :type :text) :type :json)))
           (input '(#(1 "a") #(2 "b") #(1 "c") #(2 "d"))))
      (ensure-directories-exist directory)
      (unwind-protect
           (flet ((source (callback) (dolist (row input) (funcall callback row))))
             (let ((count 0))
               (stream-sorted-aggregates
                #'source '((:col 1 :type :int4)) descriptors
                (lambda (row) (declare (ignore row)) (incf count) :stop))
               (rove:ok (= count 1)))
             (rove:ok (null (uiop:directory-files directory)))
             (rove:ok
              (handler-case
                  (progn
                    (stream-sorted-aggregates
                     #'source '((:col 1 :type :int4)) descriptors
                     (lambda (row) (declare (ignore row)) (ereport "57014" "cancelled")))
                    nil)
                (pg-error (condition) (equal (pg-error-sqlstate condition) "57014"))))
             (rove:ok (null (uiop:directory-files directory)))
             (let ((*json-aggregate-output-max-bytes* 8))
               (rove:ok
                (handler-case
                    (progn
                      (stream-sorted-aggregates
                       #'source '((:col 1 :type :int4)) descriptors
                       (lambda (row) (declare (ignore row)) nil))
                      nil)
                  (pg-error (condition) (equal (pg-error-sqlstate condition) "54000")))))
             (rove:ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest whole-row-semantic-keys-ignore-runtime-source-identities
    (labels ((expressions ()
               (let* ((table (make-pg-class :oid 0 :name "empty" :columns #()))
                      (left (select-source-scope-relation
                             (list :name "a" :table table :offset 0 :width 0)))
                      (right (select-source-scope-relation
                              (list :name "b" :table table :offset 0 :width 0)))
                      (scope (make-select-scope :parent nil :relations (list left right))))
                 (list (resolve-select-whole-row scope "a")
                       (resolve-select-whole-row scope "b")))))
      (let* ((first (expressions))
             (second (expressions))
             (a (first first))
             (b (second first)))
        (rove:ok (not (eql (second a) (second (first second)))))
        (rove:ok (equal (subquery-semantic-key a) (subquery-semantic-key (first second))))
        (rove:ok (not (equal (subquery-semantic-key a) (subquery-semantic-key b))))
        (rove:ok (expression-contains-column-p a))
        (rove:ok (not (dml-one-time-qualification-p a)))
        (rove:ok (not (cte-reference-filter-p a '(:offset 0 :width 0)))))))

(rove:deftest top-n-retains-source-presence
    (let* ((collector (make-select-top-n-collector 1 '(((:col 1 :type :int4)))))
           (*query-row-presence* (vector 71)))
      (offer-select-top-n-row collector (cons #(2) #(2)))
      (setf (aref *query-row-presence* 0) 99)
      (let* ((stored (first (select-top-n-collector-rows collector nil t)))
             (*query-row-presence* (third stored)))
        (rove:ok (equalp *query-row-presence* #(71)))
        (rove:ok (equalp (car (first stored)) #(2))))))

(rove:deftest project-set-preserves-whole-row-source-presence
    (let* ((table (make-pg-class :oid 42 :name "r"
                                 :columns (vector (make-pg-column :name "id" :type :int4 :attnum 1))))
           (source (list :name "r" :table table :offset 0 :width 1
                         :row-source (lambda () (list (vector +null+)))))
           (identity (ensure-select-source-identity source))
           (expression (list :whole-row identity '((:col 1 :type :int4))
                             :type :record :typmod -1 :fields '(("id" :int4))))
           (targets (list (list :expr (list :func "to_json" (list expression) :type :json :typmod -1))
                          (list :expr '(:col 2 :type :int4 :typmod -1))))
           (node (list :project targets
                       (list :project-set '((:set-func "presence-test" nil)) '(:seq-scan 0 42) 1)))
           (original (symbol-function 'make-sql-set-function-iterator)))
      (unwind-protect
           (progn
             (setf (symbol-function 'make-sql-set-function-iterator)
                   (lambda (call environment)
                     (declare (ignore call environment))
                     (rove:ok (equalp *query-row-presence* (vector identity)))
                     (let ((index 0))
                       (lambda () (when (< index 2) (values (incf index) t))))))
             (dolist (iterator-p '(nil t))
               (let ((rows nil))
                 (flet ((emit (row) (push (copy-seq (car row)) rows) nil))
                   (if iterator-p
                       (let ((input (make-physical-plan-iterator node (list source))))
                         (unwind-protect (map-sql-iterator input #'emit)
                           (close-sql-iterator input)))
                       (interpret-query-plan (%make-query-plan :physical node)
                                             (list source) #'emit)))
                 (rove:ok (equalp (nreverse rows)
                                  '(#("{\"id\":null}" 1) #("{\"id\":null}" 2)))))))
        (setf (symbol-function 'make-sql-set-function-iterator) original))))

(rove:deftest test-query-temp-fd-scope-macro-expansion-protects-cleanup
    (let* ((expansion (macroexpand-1 '(with-query-temp-fd (fd nil) :body)))
           (scope-form (and (consp expansion) (third expansion)))
           (unwind-form
             (and (consp scope-form)
                  (eq (first scope-form) 'sb-sys:without-interrupts)
                  (second scope-form)))
           (protected-form
             (and (consp unwind-form)
                  (eq (first unwind-form) 'unwind-protect)
                  (second unwind-form)))
           (cleanup-form (and (consp unwind-form) (third unwind-form))))
      (rove:ok (and (consp expansion)
                    (eq (first expansion) 'let)
                    unwind-form)
               "spill fd scope installs the interrupt mask around unwind-protect")
      (rove:ok (and (consp protected-form)
                    (eq (first protected-form) 'progn)
                    (eq (first (second protected-form)) 'setf)
                    (eq (first (third protected-form)) 'sb-sys:with-local-interrupts))
               "spill fd scope binds acquisition before enabling body interrupts")
      (rove:ok (and (consp cleanup-form)
                    (eq (first cleanup-form) 'when)
                    (eq (first (third cleanup-form)) 'query-temp-close-fd))
               "spill fd cleanup stays inside the interrupt-masked unwind-protect")))

(rove:deftest test-query-spill-macros-use-default-memory-limit
    (let ((*query-work-memory-bytes* 37)
          (row-limit-evaluations 0)
          (match-limit-evaluations 0))
      (with-query-row-spool (spool)
        (rove:ok (= (query-row-spool-memory-limit spool) 37)
                 "row-spool macro defaults to the configured work-memory limit"))
      (with-query-match-map (match-map)
        (rove:ok (= (query-match-map-memory-limit match-map) 37)
                 "match-map macro defaults to the configured work-memory limit"))
      (with-query-row-spool (spool (incf row-limit-evaluations))
        (rove:ok (= (query-row-spool-memory-limit spool) 1)
                 "row-spool macro passes an explicit memory limit"))
      (with-query-match-map (match-map (incf match-limit-evaluations))
        (rove:ok (= (query-match-map-memory-limit match-map) 1)
                 "match-map macro passes an explicit memory limit"))
      (rove:ok (= row-limit-evaluations 1)
               "row-spool macro evaluates its memory limit once")
      (rove:ok (= match-limit-evaluations 1)
               "match-map macro evaluates its memory limit once")))

(rove:deftest test-query-temp-file-limit-option-parsing
    (multiple-value-bind (host port pgdata control-port control-socket
                          swank-port swank-address limit)
        (parse-args '("--temp-file-limit-bytes" "4096"))
      (declare (ignore host port pgdata control-port control-socket
                       swank-port swank-address))
      (rove:ok (= limit 4096)
               "server option parses a finite temporary file limit"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-address limit)
      (parse-args '("--temp-file-limit-bytes=-1"))
    (declare (ignore host port pgdata control-port control-socket
                     swank-port swank-address))
    (rove:ok (null limit)
             "server option accepts -1 for unlimited temporary disk space")))

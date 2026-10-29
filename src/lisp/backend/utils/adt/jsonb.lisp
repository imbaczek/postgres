;;;; jsonb.lisp -- parsed JSONB values and canonical rendering.

(in-package #:pg)

(defstruct (sql-jsonb (:constructor make-sql-jsonb (value)))
  "Parsed SQL JSONB value. Objects are (:OBJECT . sorted-pairs), arrays are vectors.
The constructor retains VALUE without copying; callers must treat its tree,
including strings, pairs and vectors, as immutable while the value is in use."
  value)

(defun sql-jsonb-key-less-p (left right)
  "Order object keys by UTF-8 byte length, then unsigned byte order."
  (let ((a (string->bytes left)) (b (string->bytes right)))
    (if (/= (length a) (length b))
        (< (length a) (length b))
        (loop for x across a for y across b
              when (/= x y) return (< x y)
                finally (return nil)))))

(defun sql-jsonb-input (text)
  "Parse JSON into normalized JSONB, retaining the last duplicate key.
A first pass validates the complete JSON syntax; only then does a second pass
check JSONB-specific escapes, numeric range, and nesting. Thus a syntax error
anywhere takes precedence over errors found during JSONB conversion. Values at
depth greater than 512 are rejected with SQLSTATE 54001; the root has depth zero."
  (sql-json-input text)
  (let ((position 0) (length (length text)))
    (labels ((peek () (and (< position length) (char text position)))
             (take () (prog1 (peek) (incf position)))
             (space-p (c) (and c (find c '(#\Space #\Tab #\Newline #\Return))))
             (skip-space () (loop while (space-p (peek)) do (incf position)))
             (hex-unit ()
               (let ((n 0))
                 (dotimes (i 4 n)
                   (setf n (+ (* n 16) (digit-char-p (take) 16))))))
             (unicode-character ()
               (let ((high (hex-unit)))
                 (cond
                   ((zerop high)
                    (ereport "22P05" "unsupported Unicode escape sequence"))
                   ((<= #xd800 high #xdbff)
                    (unless (and (eql (take) (code-char 92)) (eql (take) #\u))
                      (ereport "22P02" "invalid Unicode surrogate pair"))
                    (let ((low (hex-unit)))
                      (unless (<= #xdc00 low #xdfff)
                        (ereport "22P02" "invalid Unicode surrogate pair"))
                      (code-char (+ #x10000 (* (- high #xd800) 1024) (- low #xdc00)))))
                   ((<= #xdc00 high #xdfff)
                    (ereport "22P02" "invalid Unicode surrogate pair"))
                   (t (code-char high)))))
             (string-token ()
               (take)
               (with-output-to-string (out)
                 (loop for c = (take)
                       until (eql c #\")
                       do (if (eql c (code-char 92))
                              (let ((escaped (take)))
                                (write-char
                                 (case escaped
                                   (#\u (unicode-character))
                                   (#\b (code-char 8)) (#\f (code-char 12))
                                   (#\n (code-char 10)) (#\r (code-char 13))
                                   (#\t (code-char 9))
                                   (otherwise escaped))
                                 out))
                              (write-char c out)))))
             (object-token (depth)
               (take)
               (skip-space)
               (let ((items (make-hash-table :test #'equal)))
                 (if (eql (peek) #\})
                     (take)
                     (loop
                       (let ((key (string-token)))
                         (skip-space) (take) (skip-space)
                         (setf (gethash key items) (value-token (1+ depth))))
                       (skip-space)
                       (if (eql (take) #\}) (return) (skip-space))))
                 (let ((pairs nil))
                   (maphash (lambda (key value) (push (cons key value) pairs)) items)
                   (cons :object (sort pairs #'sql-jsonb-key-less-p :key #'car)))))
             (array-token (depth)
               (take)
               (skip-space)
               (let ((items nil))
                 (if (eql (peek) #\])
                     (take)
                     (loop
                       (push (value-token (1+ depth)) items)
                       (skip-space)
                       (if (eql (take) #\]) (return) (skip-space))))
                 (coerce (nreverse items) 'vector)))
             (value-token (depth)
               (when (> depth 512) (ereport "54001" "JSON nesting exceeds supported depth"))
               (skip-space)
               (case (peek)
                 (#\" (string-token))
                 (#\{ (object-token depth))
                 (#\[ (array-token depth))
                 (#\t (incf position 4) :json-true)
                 (#\f (incf position 5) :json-false)
                 (#\n (incf position 4) :json-null)
                 (otherwise
                  (let ((start position))
                    (loop while (and (peek)
                                     (not (space-p (peek)))
                                     (not (find (peek) '(#\, #\] #\}))))
                          do (incf position))
                    (sql-numeric-input (subseq text start position)))))))
      (make-sql-jsonb (value-token 0)))))

(defun sql-jsonb-write-string (value out)
  "Write one string in JSONB's canonical JSON output form."
  (write-char #\" out)
  (loop for c across value
        for code = (char-code c)
        do (cond ((or (char= c #\") (= code 92))
                  (write-char (code-char 92) out) (write-char c out))
                 ((= code 8) (write-string "\\b" out))
                 ((= code 12) (write-string "\\f" out))
                 ((= code 10) (write-string "\\n" out))
                 ((= code 13) (write-string "\\r" out))
                 ((= code 9) (write-string "\\t" out))
                 ((< code 32) (format out "\\u~a" (string-downcase (format nil "~4,'0x" code))))
                 (t (write-char c out))))
  (write-char #\" out))

(defun sql-jsonb-output (jsonb)
  "Render normalized JSONB in PostgreSQL's spaced, canonical text form."
  (labels ((emit (value out)
             (cond
               ((eq value :json-null) (write-string "null" out))
               ((eq value :json-true) (write-string "true" out))
               ((eq value :json-false) (write-string "false" out))
               ((stringp value) (sql-jsonb-write-string value out))
               ((and (consp value) (eq (car value) :object))
                (write-char #\{ out)
                (loop for (key . item) in (cdr value) for first = t then nil
                      do (unless first (write-string ", " out))
                         (sql-jsonb-write-string key out)
                         (write-string ": " out)
                         (emit item out))
                (write-char #\} out))
               ((vectorp value)
                (write-char #\[ out)
                (loop for item across value for first = t then nil
                      do (unless first (write-string ", " out)) (emit item out))
                (write-char #\] out))
               (t (write-string (sql-numeric-output value) out)))))
    (with-output-to-string (out) (emit (sql-jsonb-value jsonb) out))))

(defun sql-jsonb-equality-key (jsonb)
  "Return an EQUAL-comparable key that ignores JSONB numeric display scale.
Numeric values are compared by value recursively, so 1 and 1.0 are equal even
though their canonical output retains the display scale."
  (labels ((key (value)
             (cond ((sql-numeric-p value) (sql-numeric-rational value))
                   ((and (vectorp value) (not (stringp value)))
                    (map 'list #'key value))
                   ((consp value) (cons (key (car value)) (key (cdr value))))
                   (t value))))
    (key (sql-jsonb-value jsonb))))

(defun sql-jsonb-compare (left right)
  "Compare normalized JSONB values in PostgreSQL's B-tree type order.
String and key comparisons use character order, corresponding to C collation
for UTF-8; database-specific or locale-aware collations are not implemented."
  (labels ((rank (value)
             (cond ((eq value :json-null) 0)
                   ((stringp value) 1)
                   ((sql-numeric-p value) 2)
                   ((member value '(:json-false :json-true)) 3)
                   ((vectorp value) 4)
                   ((and (consp value) (eq (car value) :object)) 5)
                   (t (error "invalid JSONB value"))))
           (order (a b) (cond ((< a b) -1) ((> a b) 1) (t 0)))
           (string-order (a b) (cond ((string< a b) -1) ((string> a b) 1) (t 0)))
           (compare-values (a b)
             (let ((ra (rank a)) (rb (rank b)))
               (unless (= ra rb) (return-from compare-values (order ra rb)))
               (case ra
                 (0 0)
                 (1 (string-order a b))
                 (2 (sql-numeric-compare a b))
                 (3 (order (if (eq a :json-true) 1 0)
                           (if (eq b :json-true) 1 0)))
                 (4 (let ((length-order (order (length a) (length b))))
                      (if (not (zerop length-order)) length-order
                          (loop for x across a for y across b
                                for result = (compare-values x y)
                                unless (zerop result) return result
                                  finally (return 0)))))
                 (5 (let ((length-order (order (length (cdr a)) (length (cdr b)))))
                      (if (not (zerop length-order)) length-order
                          (loop for (ka . va) in (cdr a)
                                for (kb . vb) in (cdr b)
                                for key-order = (string-order ka kb)
                                for value-order = (if (zerop key-order)
                                                      (compare-values va vb) key-order)
                                unless (zerop value-order) return value-order
                                  finally (return 0)))))))))
    (let* ((a (sql-jsonb-value left)) (b (sql-jsonb-value right))
                                      (ra (rank a)) (rb (rank b)))
      ;; PostgreSQL's raw-scalar wrapper makes empty top-level arrays sort first.
      (cond ((and (= ra 4) (zerop (length a)) (< rb 4)) -1)
            ((and (= rb 4) (zerop (length b)) (< ra 4)) 1)
            (t (compare-values a b))))))

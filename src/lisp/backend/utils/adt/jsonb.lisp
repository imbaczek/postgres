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
Scalar conversion occurs during syntax validation, in parser callback order.
Values at depth greater than 512 are rejected with SQLSTATE 54001; the root has depth zero."
  (let ((position 0) (length (length text)) (scalar-cache (make-hash-table)))
    (sql-json-input text :jsonb-p t :scalar-cache scalar-cache)
    (labels ((peek () (and (< position length) (char text position)))
             (take () (prog1 (peek) (incf position)))
             (space-p (c) (and c (find c '(#\Space #\Tab #\Newline #\Return))))
             (skip-space () (loop while (space-p (peek)) do (incf position)))
             (string-token ()
               (let ((cached (gethash position scalar-cache)))
                 (unless cached (error "missing validated JSONB string"))
                 (setf position (car cached))
                 (cdr cached)))
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
                  (let ((cached (gethash position scalar-cache)))
                    (unless cached (error "missing validated JSONB number"))
                    (setf position (car cached))
                    (cdr cached))))))
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

(defun sql-jsonb-write (jsonb out)
  "Write normalized JSONB in PostgreSQL's spaced, canonical text form."
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
    (emit (sql-jsonb-value jsonb) out)))

(defun sql-jsonb-output (jsonb)
  "Render normalized JSONB in PostgreSQL's spaced, canonical text form."
  (with-output-to-string (out) (sql-jsonb-write jsonb out)))

(defun sql-jsonb-extract (jsonb key text-p)
  "Extract a JSONB object field or array element; return SQL NULL when absent.
KEY is a string field name or an integer index, including negative indexes.
TEXT-P requests the text form, for which JSON null becomes SQL NULL."
  (let* ((value (sql-jsonb-value jsonb))
         (found nil)
         (result nil))
    (cond
      ((and (stringp key) (consp value) (eq (car value) :object))
       (let ((pair (assoc key (cdr value) :test #'string=)))
         (when pair (setf found t result (cdr pair)))))
      ((and (integerp key) (vectorp value) (not (stringp value)))
       (let ((index (if (minusp key) (+ (length value) key) key)))
         (when (<= 0 index (1- (length value)))
           (setf found t result (aref value index)))))
      ((and (integerp key) (member key '(0 -1))
            (not (and (consp value) (eq (car value) :object))))
       ;; JSONB stores a scalar root as a one-element raw-scalar array.
       (setf found t result value)))
    (cond ((not found) +null+)
          ((not text-p) (make-sql-jsonb result))
          ((eq result :json-null) +null+)
          ((stringp result) result)
          (t (sql-jsonb-output (make-sql-jsonb result))))))

(defun jsonb-object-value-p (value)
  (and (consp value) (eq (car value) :object)))

(defun jsonb-array-value-p (value)
  (and (vectorp value) (not (stringp value))))

(defun sql-jsonb-set-index (text position)
  "Parse an array path index using PostgreSQL's signed int32 rules."
  (let ((i 0) (length (length text)) (negative nil) (value 0) (digits 0))
    (labels ((fail () (ereport "22P02" "path element at position ~d is not an integer: \"~a\"" position text)))
      (loop while (and (< i length) (find (char-code (char text i)) '(9 10 11 12 13 32)))
            do (incf i))
      (when (and (< i length) (find (char text i) "+-"))
        (setf negative (char= (char text i) #\-)) (incf i))
      (loop while (and (< i length) (char<= #\0 (char text i) #\9))
            do (incf digits)
               (setf value (+ (* value 10) (- (char-code (char text i)) (char-code #\0))))
               (when (> value (if negative 2147483648 2147483647)) (fail))
               (incf i))
      (unless (and (plusp digits) (= i length)) (fail))
      (if negative (- value) value))))

(declaim (notinline sql-array-p sql-array-dimensions sql-array-elements))

(defun jsonb-array-insert-at (array index value)
  "Copy ARRAY, inserting VALUE before INDEX."
  (let ((result (make-array (1+ (length array)))))
    (replace result array :end1 index)
    (setf (aref result index) value)
    (replace result array :start1 (1+ index) :start2 index)
    result))

(defun sql-jsonb-set (jsonb path replacement create-p &key insert-p after-p)
  "Return JSONB with PATH changed without mutating either input."
  (when (sql-array-p path)
    (when (> (length (sql-array-dimensions path)) 1)
      (ereport "2202E" "wrong number of array subscripts"))
    (setf path (sql-array-elements path)))
  (let ((root (sql-jsonb-value jsonb))
        (new-value (sql-jsonb-value replacement))
        (path-length (length path)))
    (unless (or (jsonb-object-value-p root) (jsonb-array-value-p root))
      (ereport "22023" "cannot set path in scalar"))
    (when (or (zerop path-length)
              (and (not create-p)
                   (if (jsonb-object-value-p root) (null (cdr root)) (zerop (length root)))))
      (return-from sql-jsonb-set jsonb))
    (labels ((visit (value level)
               (let ((key (aref path level))
                     (last-p (= level (1- path-length))))
                 (when (eq key +null+)
                   (ereport "22004" "path element at position ~d is null" (1+ level)))
                 (cond
                   ((jsonb-object-value-p value)
                    (let* ((pairs (cdr value))
                           (entry (assoc key pairs :test #'string=)))
                      (cond
                        (entry
                         (when (and insert-p last-p)
                           (ereport "22023" "cannot replace existing key"
                                    :hint "Try using the function jsonb_set to replace key value."))
                         (let ((changed (if last-p new-value (visit (cdr entry) (1+ level)))))
                           (if (eq changed (cdr entry)) value
                               (cons :object
                                     (mapcar (lambda (pair)
                                               (if (eq pair entry) (cons (car pair) changed) pair))
                                             pairs)))))
                        ((and last-p create-p)
                         (cons :object (stable-sort (cons (cons key new-value) (copy-list pairs))
                                                    #'sql-jsonb-key-less-p :key #'car)))
                        (t value))))
                   ((jsonb-array-value-p value)
                    (let* ((count (length value))
                           (index (sql-jsonb-set-index key (1+ level)))
                           (actual (if (minusp index) (+ count index) index)))
                      (cond
                        ((<= 0 actual (1- count))
                         (if (and insert-p last-p)
                             (jsonb-array-insert-at value (if after-p (1+ actual) actual) new-value)
                             (let ((changed (if last-p new-value (visit (aref value actual) (1+ level)))))
                               (if (eq changed (aref value actual)) value
                                   (let ((copy (copy-seq value)))
                                     (setf (aref copy actual) changed) copy)))))
                        ((and last-p create-p)
                         (if (minusp actual)
                             (concatenate 'vector (vector new-value) value)
                             (concatenate 'vector value (vector new-value))))
                        (t value))))
                   (t value)))))
      (let ((result (visit root 0)))
        (let ((pending (list (cons result 0))))
          (loop while pending for (item . depth) = (pop pending)
                do (when (> depth 512) (ereport "54001" "JSON nesting exceeds supported depth"))
                   (cond ((jsonb-object-value-p item)
                          (dolist (pair (cdr item)) (push (cons (cdr pair) (1+ depth)) pending)))
                         ((jsonb-array-value-p item)
                          (loop for child across item do (push (cons child (1+ depth)) pending))))))
        (make-sql-jsonb result)))))

(defun jsonb-array-without-index (array index)
  "Copy ARRAY without its INDEX, allocating only the result vector."
  (let ((result (make-array (1- (length array)))))
    (replace result array :end1 index)
    (replace result array :start1 index :start2 (1+ index))
    result))

(defun sql-jsonb-delete (jsonb operand kind)
  "Delete top-level keys, string elements, or one array index."
  (when (and (eq kind :keys) (sql-array-p operand))
    (when (> (length (sql-array-dimensions operand)) 1)
      (ereport "2202E" "wrong number of array subscripts"))
    (setf operand (sql-array-elements operand)))
  (let ((root (sql-jsonb-value jsonb)))
    (unless (or (jsonb-object-value-p root) (jsonb-array-value-p root))
      (ereport "22023" "cannot delete from scalar"))
    (when (and (eq kind :index) (jsonb-object-value-p root))
      (ereport "22023" "cannot delete from object using integer index"))
    (cond
      ((eq kind :index)
       (let* ((count (length root))
              (index (if (minusp operand) (+ count operand) operand)))
         (if (<= 0 index (1- count))
             (make-sql-jsonb (jsonb-array-without-index root index))
             jsonb)))
      (t
       (let ((keys (make-hash-table :test #'equal)))
         (if (eq kind :key)
             (setf (gethash operand keys) t)
             (loop for key across operand unless (eq key +null+)
                   do (setf (gethash key keys) t)))
         (if (jsonb-object-value-p root)
             (make-sql-jsonb
              (cons :object (remove-if (lambda (pair) (gethash (car pair) keys)) (cdr root))))
             (make-sql-jsonb
              (remove-if (lambda (item) (and (stringp item) (gethash item keys))) root))))))))

(defun sql-jsonb-delete-path (jsonb path)
  "Delete an existing JSONB path, preserving the original tree."
  (when (sql-array-p path)
    (when (> (length (sql-array-dimensions path)) 1)
      (ereport "2202E" "wrong number of array subscripts"))
    (setf path (sql-array-elements path)))
  (let ((root (sql-jsonb-value jsonb)) (path-length (length path)))
    (unless (or (jsonb-object-value-p root) (jsonb-array-value-p root))
      (ereport "22023" "cannot delete path in scalar"))
    (when (or (zerop path-length)
              (if (jsonb-object-value-p root) (null (cdr root)) (zerop (length root))))
      (return-from sql-jsonb-delete-path jsonb))
    (labels ((visit (value level)
               (let ((key (aref path level)) (last-p (= level (1- path-length))))
                 (when (eq key +null+)
                   (ereport "22004" "path element at position ~d is null" (1+ level)))
                 (cond
                   ((jsonb-object-value-p value)
                    (let ((entry (assoc key (cdr value) :test #'string=)))
                      (cond ((null entry) value)
                            (last-p (cons :object (remove entry (cdr value) :test #'eq)))
                            (t (let ((changed (visit (cdr entry) (1+ level))))
                                 (if (eq changed (cdr entry)) value
                                     (cons :object
                                           (mapcar (lambda (pair)
                                                     (if (eq pair entry) (cons (car pair) changed) pair))
                                                   (cdr value)))))))))
                   ((jsonb-array-value-p value)
                    (let* ((count (length value))
                           (index (sql-jsonb-set-index key (1+ level)))
                           (actual (if (minusp index) (+ count index) index)))
                      (if (<= 0 actual (1- count))
                          (if last-p
                              (jsonb-array-without-index value actual)
                              (let ((changed (visit (aref value actual) (1+ level))))
                                (if (eq changed (aref value actual)) value
                                    (let ((copy (copy-seq value)))
                                      (setf (aref copy actual) changed) copy))))
                          value)))
                   (t value)))))
      (make-sql-jsonb (visit root 0)))))

(defun sql-jsonb-concat (left right)
  "Concatenate JSONB roots as IteratorConcat does, preserving immutable inputs."
  (let* ((a (sql-jsonb-value left)) (b (sql-jsonb-value right))
                                    (value
                                      (if (and (jsonb-object-value-p a) (jsonb-object-value-p b))
                                          (let ((left-pairs (cdr a)) (right-pairs (cdr b)) (result nil))
                                            (loop while (and left-pairs right-pairs)
                                                  for left-key = (caar left-pairs)
                                                  for right-key = (caar right-pairs)
                                                  do (cond ((string= left-key right-key)
                                                            (push (pop right-pairs) result) (pop left-pairs))
                                                           ((sql-jsonb-key-less-p left-key right-key)
                                                            (push (pop left-pairs) result))
                                                           (t (push (pop right-pairs) result))))
                                            (dolist (pair left-pairs) (push pair result))
                                            (dolist (pair right-pairs) (push pair result))
                                            (cons :object (nreverse result)))
                                          (concatenate 'vector
                                                       (if (jsonb-array-value-p a) a (vector a))
                                                       (if (jsonb-array-value-p b) b (vector b))))))
    ;; A wrapped scalar/object can add a level. Keep generated values readable
    ;; by the same JSONB input and logical-WAL paths.
    (let ((pending (list (cons value 0))))
      (loop while pending
            for (item . depth) = (pop pending)
            do (when (> depth 512) (ereport "54001" "JSON nesting exceeds supported depth"))
               (cond ((jsonb-object-value-p item)
                      (dolist (pair (cdr item)) (push (cons (cdr pair) (1+ depth)) pending)))
                     ((jsonb-array-value-p item)
                      (loop for child across item do (push (cons child (1+ depth)) pending))))))
    (make-sql-jsonb value)))

(defun jsonb-scalar-equal-p (left right)
  "Compare JSONB scalar values, preserving type and ignoring numeric scale."
  (cond ((and (sql-numeric-p left) (sql-numeric-p right))
         (zerop (sql-numeric-compare left right)))
        ((and (stringp left) (stringp right)) (string= left right))
        (t (and (keywordp left) (eq left right)))))

(defun jsonb-value-contains-p (left right)
  "Test containment at one JSONB tree level, following JsonbDeepContains."
  (cond
    ((and (jsonb-object-value-p left) (jsonb-object-value-p right))
     (and (>= (length (cdr left)) (length (cdr right)))
          (let ((remaining (cdr left)))
            ;; JSONB object keys are unique and sorted in storage order.
            (every (lambda (pair)
                     (loop while (and remaining
                                      (sql-jsonb-key-less-p (caar remaining) (car pair)))
                           do (pop remaining))
                     (and remaining
                          (string= (caar remaining) (car pair))
                          (jsonb-value-contains-p (cdar remaining) (cdr pair))
                          (progn (pop remaining) t)))
                   (cdr right)))))
    ((and (jsonb-array-value-p left) (jsonb-array-value-p right))
     (loop for needle across right
           always (loop for candidate across left
                          thereis (jsonb-value-contains-p candidate needle))))
    ((or (jsonb-object-value-p left) (jsonb-object-value-p right)
         (jsonb-array-value-p left) (jsonb-array-value-p right))
     nil)
    (t (jsonb-scalar-equal-p left right))))

(defun sql-jsonb-contains-p (left right)
  "Return whether LEFT contains RIGHT, including the top-level scalar exception."
  (let ((container (sql-jsonb-value left)) (contained (sql-jsonb-value right)))
    (if (and (jsonb-array-value-p container)
             (not (jsonb-array-value-p contained))
             (not (jsonb-object-value-p contained)))
        (loop for candidate across container
                thereis (jsonb-scalar-equal-p candidate contained))
        (jsonb-value-contains-p container contained))))

(defun sql-jsonb-exists-p (jsonb key)
  "Match KEY against a top-level object key, string array element, or string scalar."
  (let ((value (sql-jsonb-value jsonb)))
    (cond ((jsonb-object-value-p value) (not (null (assoc key (cdr value) :test #'string=))))
          ((jsonb-array-value-p value)
           (loop for element across value
                   thereis (and (stringp element) (string= element key))))
          ((stringp value) (string= value key))
          (t nil))))

(defun sql-jsonb-exists-keys-p (jsonb keys all-p)
  "Check any or all non-NULL KEYS against object keys, array strings, or a string scalar."
  (if all-p
      (every (lambda (key) (or (eq key +null+) (sql-jsonb-exists-p jsonb key))) keys)
      (some (lambda (key) (and (not (eq key +null+)) (sql-jsonb-exists-p jsonb key))) keys)))

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

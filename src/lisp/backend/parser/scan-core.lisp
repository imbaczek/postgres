;;;; scan.lisp -- hand-written SQL lexer for the MVP subset.

(in-package #:pg)

(defstruct token
  quoted-p
  kind     ; :ident | :string | :integer | :op | :punct | :eof
  value
  pos)

(defun scan-error (message pos)
  (ereport "42601" message :position (1+ pos)))

(defun scan-quoted-text (sql start delimiter)
  "Return quoted contents and the next position, accepting doubled delimiters."
  (let ((position (1+ start)) (count 0) (end nil))
    ;; Count first so a large literal never needs a growable buffer and copy.
    (loop
      (when (>= position (length sql))
        (scan-error (if (char= delimiter #\') "unterminated quoted string"
                        "unterminated quoted identifier") start))
      (let ((character (char sql position)))
        (incf position)
        (cond ((char/= character delimiter) (incf count))
              ((and (< position (length sql)) (char= (char sql position) delimiter))
               (incf count) (incf position))
              (t (setf end position) (return)))))
    (let ((result (make-string count)) (input (1+ start)))
      (loop for output below count
            for character = (char sql input)
            do (setf (char result output) character)
               (incf input)
               (when (char= character delimiter) (incf input)))
      (values result end))))

(defun sql-number-digit-p (character radix)
  (and character (< (char-code character) 128) (digit-char-p character radix)))

(defun scan-sql-number (sql start)
  "Preserve C scan.l numeric spelling; no decimal conversion or Lisp reader is used."
  (let ((i start) (size (length sql)) (numeric-p nil))
    (labels ((peek (&optional (offset 0))
               (when (< (+ i offset) size) (char sql (+ i offset))))
             (invalid () (scan-error "invalid numeric literal" start))
             (digits (radix)
               (let ((before i))
                 (loop while (sql-number-digit-p (peek) radix)
                       do (incf i)
                          (when (and (eql (peek) #\_)
                                     (sql-number-digit-p (peek 1) radix))
                            (incf i)))
                 (when (= before i) (invalid))))
             (junk-p ()
               (let ((character (peek)))
                 (and character
                      (or (> (char-code character) 127) (alphanumericp character)
                          (find character "_$"))))))
      (let ((radix (and (eql (peek) #\0) (peek 1)
                        (case (char-downcase (peek 1)) (#\x 16) (#\o 8) (#\b 2)))))
        (when radix
          (incf i 2)
          (when (eql (peek) #\_) (incf i))
          (let ((begin i))
            (digits radix)
            (when (junk-p) (invalid))
            (return-from scan-sql-number
              (values :integer (parse-integer (remove #\_ (subseq sql begin i)) :radix radix) i)))))
      (if (eql (peek) #\.)
          (progn (setf numeric-p t) (incf i) (digits 10))
          (progn
            (digits 10)
            (when (and (eql (peek) #\.) (not (eql (peek 1) #\.)))
              (setf numeric-p t)
              (incf i)
              (when (sql-number-digit-p (peek) 10) (digits 10)))))
      (when (and (peek) (find (peek) "eE"))
        (setf numeric-p t)
        (incf i)
        (when (and (peek) (find (peek) "+-")) (incf i))
        (digits 10))
      (when (junk-p) (invalid))
      (values (if numeric-p :numeric :integer)
              (if numeric-p (subseq sql start i)
                  (parse-integer (remove #\_ (subseq sql start i))))
              i))))

(defun sql-dollar-tag-character-p (character &optional continuation-p)
  "Recognize a dollar tag character; digits are allowed only in continuation.
Like C's scanner, non-ASCII characters are allowed in tags."
  (and character
       (or (char= character #\_)
           (char<= #\a character #\z)
           (char<= #\A character #\Z)
           (> (char-code character) 127)
           (and continuation-p (sql-number-digit-p character 10)))))

(defun scan-dollar-quoted-text (sql start)
  "Return raw quoted text and the position after its matching delimiter.
START must point at a dollar sign. Tags match exactly, including case. A non-opener returns NIL, NIL for the
caller to handle; an unterminated valid opener reports an error at START."
  (let ((tag-end (1+ start)) (size (length sql)))
    (when (and (< tag-end size) (sql-dollar-tag-character-p (char sql tag-end)))
      (incf tag-end)
      (loop while (and (< tag-end size)
                       (sql-dollar-tag-character-p (char sql tag-end) t))
            do (incf tag-end)))
    (unless (and (< tag-end size) (char= (char sql tag-end) #\$))
      (return-from scan-dollar-quoted-text (values nil nil)))
    (let* ((begin (1+ tag-end))
           (delimiter (subseq sql start begin))
           (end (search delimiter sql :start2 begin)))
      (unless end (scan-error "unterminated dollar-quoted string" start))
      (values (subseq sql begin end) (+ end (length delimiter))))))

(defun scan-sql-parameter (sql start)
  "Scan the parameter digits and reject trailing identifier junk.
A dollar after the digits begins the next token."
  (let ((position (1+ start)) (index 0))
    (loop for digit = (and (< position (length sql))
                           (sql-number-digit-p (char sql position) 10))
          while digit
          do (setf index (min 65536 (+ (* index 10) digit)))
             (incf position))
    (when (and (< position (length sql))
               (sql-dollar-tag-character-p (char sql position)))
      (scan-error "trailing junk after parameter" start))
    (values index position)))

(defun tokenize (sql)
  (let ((i 0)
        (n (length sql))
        (out '()))
    (declare (type (integer 0 *) i n)
             (type list out))
    (labels ((peek (&optional (k 0))
               (let ((j (+ i k)))
                 (and (< j n) (char sql j))))
             (skip-space ()
               (loop
                 (cond
                   ((and (< i n)
                         (member (char sql i)
                                 '(#\Space #\Tab #\Newline #\Return #\Page)))
                    (incf i))
                   ((and (char= (or (peek) (code-char 0)) #\-)
                         (char= (or (peek 1) (code-char 0)) #\-))
                    (loop while (and (< i n) (not (char= (char sql i) #\Newline)))
                          do (incf i)))
                   ((and (char= (or (peek) (code-char 0)) #\/)
                         (char= (or (peek 1) (code-char 0)) #\*))
                    (incf i 2)
                    (loop until (or (>= i n)
                                    (and (char= (char sql i) #\*)
                                         (< (1+ i) n)
                                         (char= (char sql (1+ i)) #\/)))
                          do (incf i))
                    (when (<= (+ i 2) n) (incf i 2)))
                   (t (return))))))
      (loop
        (skip-space)
        (when (>= i n) (return))
        (let ((c (char sql i)))
          (cond
            ((and (find c "bBxX") (eql (peek 1) #\'))
             (let ((start i))
               (multiple-value-bind (value next) (scan-quoted-text sql (1+ i) #\')
                 (setf i next)
                 (push (make-token :kind :bit-string
                                   :value (concatenate 'string (string c) value) :pos start)
                       out))))
            ((and (char= c #\$) (sql-number-digit-p (peek 1) 10))
             (let ((start i))
               (multiple-value-bind (index next) (scan-sql-parameter sql start)
                 (setf i next)
                 (push (make-token :kind :parameter :value index :pos start) out))))
            ((char= c #\$)
             (let ((start i))
               (multiple-value-bind (value next) (scan-dollar-quoted-text sql start)
                 (unless next
                   (scan-error (format nil "syntax error at or near ~s" "$") start))
                 (setf i next)
                 (push (make-token :kind :string :value value :pos start) out))))
            ((or (alpha-char-p c) (char= c #\_))
             (let ((start i))
               (incf i)
               (loop while (and (< i n)
                                (or (alphanumericp (char sql i))
                                    (member (char sql i) '(#\_ #\$))))
                     do (incf i))
               (push (make-token :kind :ident
                                 :value (string-downcase (subseq sql start i))
                                 :pos start)
                     out)))
            ((or (sql-number-digit-p c 10)
                 (and (char= c #\.) (sql-number-digit-p (peek 1) 10)))
             (let ((start i))
               (multiple-value-bind (kind value next) (scan-sql-number sql i)
                 (setf i next)
                 (push (make-token :kind kind :value value :pos start) out))))
            ((member c '(#\' #\"))
             (let ((start i))
               (multiple-value-bind (value next) (scan-quoted-text sql start c)
                 (setf i next)
                 (push (make-token :kind (if (char= c #\') :string :ident)
                                   :quoted-p (char= c #\")
                                   :value value :pos start)
                       out))))
            ((member c '(#\( #\) #\[ #\] #\, #\; #\.))
             (push (make-token :kind :punct :value (string c) :pos i) out)
             (incf i))
            ((member c '(#\= #\< #\> #\! #\+ #\- #\* #\/ #\% #\: #\| #\^ #\& #\# #\@ #\~ #\? #\`))
             (let ((start i))
               (incf i)
               (cond
                 ((char= c #\:)
                  (when (and (peek) (member (peek) '(#\: #\=))) (incf i)))
                 (t
                  ;; scan.l truncates an operator before either comment opener.
                  (loop while (and (peek) (find (peek) "+-*/%<>=~!@#^&|?`")
                                   (not (or (and (eql (peek) #\-) (eql (peek 1) #\-))
                                            (and (eql (peek) #\/) (eql (peek 1) #\*)))))
                        do (incf i))
                  ;; SQL permits trailing +/- only with a non-SQL operator character.
                  (unless (find-if (lambda (ch) (find ch "~!@#^&|?`%"))
                                   (subseq sql start i))
                    (loop while (and (> (- i start) 1)
                                     (find (char sql (1- i)) "+-"))
                          do (decf i)))))
               (push (make-token :kind :op
                                 :value (subseq sql start i)
                                 :pos start)
                     out)))
            (t
             (scan-error
              (format nil "syntax error at or near \"~a\"" c)
              i)))))
      (push (make-token :kind :eof :value nil :pos n) out)
      (nreverse out))))

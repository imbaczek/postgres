;;;; scan.lisp -- hand-written SQL lexer for the MVP subset.

(in-package #:pg)

(defstruct token
  kind     ; :ident | :string | :integer | :op | :punct | :eof
  value
  pos)

(defun scan-error (message pos)
  (ereport "42601" message :position (1+ pos)))

(defun tokenize (sql)
  (let ((i 0)
        (n (length sql))
        (out '()))
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
                   ((and (char= (or (peek) #\Nul) #\-)
                         (char= (or (peek 1) #\Nul) #\-))
                    (loop while (and (< i n) (not (char= (char sql i) #\Newline)))
                          do (incf i)))
                   ((and (char= (or (peek) #\Nul) #\/)
                         (char= (or (peek 1) #\Nul) #\*))
                    (incf i 2)
                    (loop until (or (>= i n)
                                    (and (char= (char sql i) #\*)
                                         (< (1+ i) n)
                                         (char= (char sql (1+ i)) #\/)))
                          do (incf i))
                    (when (<= (+ i 2) n) (incf i 2)))
                   (t (return)))))
             (read-ident ()
               (let ((start i))
                 (loop while (and (< i n)
                                  (or (alphanumericp (char sql i))
                                      (member (char sql i) '(#\_ #\$))))
                       do (incf i))
                 (subseq sql start i))))
      (loop
        (skip-space)
        (when (>= i n) (return))
        (let ((c (char sql i)))
          (cond
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
            ((digit-char-p c)
             (let ((start i))
               (loop while (and (< i n) (digit-char-p (char sql i))) do (incf i))
               (when (and (< i n) (char= (char sql i) #\.)
                          (< (1+ i) n) (digit-char-p (char sql (1+ i))))
                 (ereport "0A000" "numeric types are not supported"))
               (push (make-token :kind :integer
                                 :value (parse-integer sql :start start :end i)
                                 :pos start)
                     out)))
            ((char= c #\')
             (let ((start i))
               (incf i)
               (let ((buf (make-array 0 :adjustable t :fill-pointer 0
                                        :element-type 'character)))
                 (loop
                   (when (>= i n)
                     (scan-error "unterminated quoted string" start))
                   (let ((ch (char sql i)))
                     (cond
                       ((char= ch #\')
                        (incf i)
                        (if (and (< i n) (char= (char sql i) #\'))
                            (progn (vector-push-extend #\' buf) (incf i))
                            (return)))
                       (t
                        (vector-push-extend ch buf)
                        (incf i)))))
                 (push (make-token :kind :string
                                   :value (coerce buf 'string)
                                   :pos start)
                       out))))
            ((char= c #\")
             (let ((start i))
               (incf i)
               (let ((buf (make-array 0 :adjustable t :fill-pointer 0
                                        :element-type 'character)))
                 (loop
                   (when (>= i n)
                     (scan-error "unterminated quoted identifier" start))
                   (let ((ch (char sql i)))
                     (cond
                       ((char= ch #\")
                        (incf i)
                        (if (and (< i n) (char= (char sql i) #\"))
                            (progn (vector-push-extend #\" buf) (incf i))
                            (return)))
                       (t
                        (vector-push-extend ch buf)
                        (incf i)))))
                 (push (make-token :kind :ident
                                   :value (coerce buf 'string)
                                   :pos start)
                       out))))
            ((member c '(#\( #\) #\, #\; #\.))
             (push (make-token :kind :punct :value (string c) :pos i) out)
             (incf i))
            ((member c '(#\= #\< #\> #\! #\+ #\- #\* #\/ #\% #\:))
             (let ((start i))
               (incf i)
               (cond
                 ((and (char= c #\<) (peek)
                       (member (peek) '(#\= #\>)))
                  (incf i))
                 ((and (char= c #\>) (peek) (char= (peek) #\=))
                  (incf i))
                 ((and (char= c #\!) (peek) (char= (peek) #\=))
                  (incf i))
                 ((and (char= c #\:) (peek) (char= (peek) #\:))
                  (incf i)))
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

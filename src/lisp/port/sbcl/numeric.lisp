;;;; numeric.lisp -- libc numeric conversions for PostgreSQL settings.

(in-package #:pg)

(sb-alien:define-alien-routine ("strtol" port-strtol) sb-alien:long
  (text (* sb-alien:char)) (end (* (* sb-alien:char))) (base sb-alien:int))

(sb-alien:define-alien-routine ("strtod" port-strtod) sb-alien:double
  (text (* sb-alien:char)) (end (* (* sb-alien:char))))

(sb-alien:define-alien-routine ("strtoull" port-strtoull) sb-alien:unsigned-long-long
  (text (* sb-alien:char)) (end (* (* sb-alien:char))) (base sb-alien:int))

(defmacro define-port-base-zero-converter (name result-type c23-name fallback)
  "Prefer the libc C23 base-zero entry point when available. Resolve the symbol
before clearing errno so lookup cannot change the conversion status."
  `(defun ,name (begin end)
     (let ((address (sb-sys:find-foreign-symbol-address ,c23-name)))
       (sb-alien:set-errno 0)
       (if address
           (sb-alien:alien-funcall
            (sb-alien:sap-alien
             (sb-sys:int-sap address)
             (function ,result-type (* sb-alien:char) (* (* sb-alien:char)) sb-alien:int))
            begin end 0)
           (,fallback begin end 0)))))

(define-port-base-zero-converter port-unsigned-base-zero sb-alien:unsigned-long-long
  "__isoc23_strtoull" port-strtoull)

(define-port-base-zero-converter port-signed-base-zero sb-alien:long
  "__isoc23_strtol" port-strtol)

(defmacro with-port-numeric-buffer ((bytes begin end) text &body body)
  "Pin a NUL-terminated buffer while libc reports the end of a conversion."
  (let ((address (gensym "END-ADDRESS-")))
    `(let ((,bytes (sb-ext:string-to-octets ,text :external-format :utf-8 :null-terminate t)))
       (sb-sys:with-pinned-objects (,bytes)
         (sb-alien:with-alien ((,address (* sb-alien:char)))
           (let ((,begin (sb-alien:sap-alien (sb-sys:vector-sap ,bytes) (* sb-alien:char)))
                 (,end (sb-alien:addr ,address)))
             ,@body))))))

(defun port-numeric-end-offset (bytes end)
  (- (sb-sys:sap-int (sb-alien:alien-sap (sb-alien:deref end)))
     (sb-sys:sap-int (sb-sys:vector-sap bytes))))

(defun port-parse-unsigned-base-zero (text)
  "Return the unsigned libc conversion, or NIL if conversion fails or leaves a suffix.
The caller rejects embedded NULs and checks the domain's numeric range."
  (with-port-numeric-buffer (bytes begin end) text
    (let* ((number (port-unsigned-base-zero begin end))
           (offset (port-numeric-end-offset bytes end)))
      (when (and (plusp offset) (zerop (sb-alien:get-errno)) (zerop (aref bytes offset)))
        number))))

(defun port-numeric-prefix (text)
  "Return a double-float prefix and the whitespace-trimmed suffix.
As in PostgreSQL parse_int, retry the signed base-zero conversion with strtod
on overflow or a decimal point or exponent marker. The caller applies unit
validation, scaling, rounding and range checks."
  (when (find (code-char 0) text) (error "NUL in configuration value"))
  (with-port-numeric-buffer (bytes begin end) text
    (let* ((number (port-signed-base-zero begin end))
           (offset (port-numeric-end-offset bytes end)))
      (when (or (= (sb-alien:get-errno) sb-posix:erange)
                (find (code-char (aref bytes offset)) ".eE"))
        (sb-alien:set-errno 0)
        (setf number (port-strtod begin end)
              offset (port-numeric-end-offset bytes end)))
      (unless (and (plusp offset) (zerop (sb-alien:get-errno)))
        (error "invalid numeric configuration value ~s" text))
      (setf number (coerce number 'double-float))
      (when (or (sb-ext:float-nan-p number) (sb-ext:float-infinity-p number))
        (error "nonfinite configuration value"))
      (values number (string-trim '(#\Space #\Tab #\Newline #\Return #\Page)
                                  (sb-ext:octets-to-string bytes :start offset :end (1- (length bytes))
                                                                 :external-format :utf-8))))))

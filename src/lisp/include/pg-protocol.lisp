;;;; pg-protocol.lisp -- wire-protocol constants and type OIDs.

(in-package #:pg)

;;; Protocol version and special request codes.
(defconstant +protocol-version-3-0+ 196608)      ; 3.0
(defconstant +ssl-request-code+ 80877103)          ; 1234,5679
(defconstant +gssenc-request-code+ 80877104)       ; 1234,5680
(defconstant +cancel-request-code+ 80877102)       ; 1234,5678

;;; Reported server version.
(defparameter *server-version* "20devel")

(defparameter *server-version-num* 200000)

;;; Type OIDs.
(defconstant +booloid+ 16)
(defconstant +int8oid+ 20)
(defconstant +int4oid+ 23)
(defconstant +textoid+ 25)
(defconstant +oidoid+ 26)
(defconstant +varcharoid+ 1043)
(defconstant +nameoid+ 19)

;;; PostgreSQL reports these typlen values in RowDescription.
(defun type-typlen (oid)
  (case oid
    (#.+booloid+ 1)
    (#.+int4oid+ 4)
    (#.+int8oid+ 8)
    (#.+nameoid+ 64)
    (#.+varcharoid+ -1)
    (#.+textoid+ -1)
    (t -1)))

(defun type-name (oid)
  (case oid
    (#.+booloid+ "bool")
    (#.+int4oid+ "int4")
    (#.+int8oid+ "int8")
    (#.+nameoid+ "name")
    (#.+varcharoid+ "varchar")
    (#.+textoid+ "text")
    (t "unknown")))

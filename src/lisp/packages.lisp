;;;; packages.lisp -- package definitions for the pg server.

(defpackage #:pg
  (:use #:cl)
  (:intern #:ereport #:+sql-expression-types+
           #:type-oid #:sql-type-from-oid #:sql-expression-type-description
           #:sql-expression-type-length #:sql-varchar-assignment-value
           #:sql-varchar-cast-value #:sql-varchar-typmod)
  (:export #:main))

(defpackage #:pg.sql-types
  (:use #:cl)
  ;; Keep the server-facing identities across reload. The OID index is private
  ;; to this package, and SQLSTATE reporting comes from the server's condition API.
  (:import-from #:pg #:ereport #:+sql-expression-types+
                #:type-oid #:sql-type-from-oid #:sql-expression-type-description
                #:sql-expression-type-length #:sql-varchar-assignment-value
                #:sql-varchar-cast-value #:sql-varchar-typmod)
  (:export #:type-oid #:sql-type-from-oid #:sql-expression-type-description
           #:sql-expression-type-length #:sql-varchar-assignment-value
           #:sql-varchar-cast-value #:sql-varchar-typmod))

;;;; packages.lisp -- package definitions for the pg server.

(defpackage #:pg
  (:use #:cl)
  (:intern #:ereport #:+sql-expression-types+
           #:type-oid #:sql-type-from-oid #:sql-expression-type-description
           #:sql-expression-type-length #:sql-varchar-assignment-value
           #:sql-varchar-cast-value #:sql-varchar-typmod #:sql-varchar-typmod-length
           #:sql-numeric-typmod #:sql-numeric-typmod-components
           #:sql-interval-typmod #:sql-interval-typmod-range #:sql-interval-typmod-components
           #:sql-interval-range-mask #:sql-interval-range-fields
           #:+sql-array-types+ #:sql-array-type #:sql-array-element-type
           #:sql-type-storage-code #:sql-type-catalog-name #:sql-type-alignment-code #:sql-type-display-name)
  (:export #:main))

(defpackage #:pg.sql-types
  (:use #:cl)
  ;; Keep the server-facing identities across reload. The OID index is private
  ;; to this package, and SQLSTATE reporting comes from the server's condition API.
  (:import-from #:pg #:ereport #:+sql-expression-types+
                #:type-oid #:sql-type-from-oid #:sql-expression-type-description
                #:sql-expression-type-length #:sql-varchar-assignment-value
                #:sql-varchar-cast-value #:sql-varchar-typmod #:sql-varchar-typmod-length
                #:sql-numeric-typmod #:sql-numeric-typmod-components
                #:sql-interval-typmod #:sql-interval-typmod-range #:sql-interval-typmod-components
                #:sql-interval-range-mask #:sql-interval-range-fields
                #:+sql-array-types+ #:sql-array-type #:sql-array-element-type
                #:sql-type-storage-code #:sql-type-catalog-name #:sql-type-alignment-code #:sql-type-display-name)
  (:export #:type-oid #:sql-type-from-oid #:sql-expression-type-description
           #:sql-expression-type-length #:sql-varchar-assignment-value
           #:sql-varchar-cast-value #:sql-varchar-typmod #:sql-varchar-typmod-length
           #:sql-numeric-typmod #:sql-numeric-typmod-components
           #:sql-interval-typmod #:sql-interval-typmod-range #:sql-interval-typmod-components
           #:sql-interval-range-mask #:sql-interval-range-fields
           #:sql-array-type #:sql-array-element-type
           #:sql-type-storage-code #:sql-type-catalog-name #:sql-type-alignment-code #:sql-type-display-name))

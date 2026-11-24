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
           #:sql-type-storage-code #:sql-type-catalog-name #:sql-type-alignment-code #:sql-type-display-name
           #:metadata-literal-envelope-image #:metadata-literal-envelope-read #:metadata-literal-envelope-decode
           #:metadata-sized-literal-image #:metadata-sized-literal-header #:metadata-sized-literal-span #:physical-bootstrap-manifest-text #:recovery-parse-manifest-text
           #:recovery-check-literal-text #:%metadata-literal-tree-p #:%metadata-literal-list-p #:string->bytes #:bytes->string #:wal-crc32
           #:*wal-recovery-group-active* #:*wal-recovery-reservation-batch-clean-p*
           #:*wal-recovery-page-images* #:*wal-recovery-relation-resets* #:*wal-recovery-page-records-seen-p*
           #:*wal-recovery-catalog-chunk-count* #:*wal-recovery-catalog-next-chunk*
           #:*wal-recovery-logical-xid* #:*wal-recovery-logical-chunk-count*
           #:*wal-recovery-logical-next-chunk* #:*wal-recovery-logical-bytes* #:*wal-recovery-logical-catalog-event-p*
           #:wal-recovery-stage-relation-reset #:wal-empty-metadata-terminal-type-p
           #:wal-note-recovery-batch-record #:wal-validate-empty-metadata-terminal
           #:wal-check-batch-logical-terminal #:wal-decode-batch-terminal #:wal-decode-next-catalog-chunk
           #:wal-decode-relation-reset #:wal-record-xid #:wal-record-type #:wal-record-payload #:wal-require-ordinary-record-type
           #:+wal-record-running-xacts+ #:+wal-record-xid-reservation+ #:+wal-record-parameter-change+
           #:+wal-record-full-page-writes-change+ #:+wal-record-logical-decoding-status-change+ #:+wal-record-batch-start+
           #:+wal-record-catalog-commit+ #:+wal-record-commit+ #:+wal-record-catalog+ #:+wal-record-clog-reset+
           #:+wal-record-checkpoint+ #:+wal-record-maintenance+
           #:wal-running-xacts-record-info #:wal-parameter-change-record-info #:wal-full-page-writes-change-record-info
           #:wal-logical-decoding-status-record-info #:wal-xid-reservation-record-bound #:wal-decode-commit-timestamp
           #:wal-decode-catalog-commit-payload #:wal-read-u64-payload #:wal-checkpoint-record-snapshot #:wal-decode-catalog-chunk-payload
           #:wal-check-batch-page-record #:wal-check-batch-logical-changes #:wal-check-batch-logical-catalog-event #:wal-u32)
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

(defpackage #:pg.metadata-literal
  (:use #:cl)
  ;; Import the established identities so running callers survive reload.
  (:import-from #:pg
                #:metadata-literal-envelope-image #:metadata-literal-envelope-read #:metadata-literal-envelope-decode
                #:metadata-sized-literal-header #:metadata-sized-literal-span #:physical-bootstrap-manifest-text #:recovery-parse-manifest-text
                #:recovery-check-literal-text #:%metadata-literal-tree-p #:%metadata-literal-list-p #:string->bytes #:bytes->string #:wal-crc32 #:metadata-sized-literal-image)
  (:export
   #:metadata-sized-literal-image #:metadata-literal-envelope-image #:metadata-literal-envelope-read #:metadata-literal-envelope-decode
   #:metadata-sized-literal-header #:metadata-sized-literal-span #:physical-bootstrap-manifest-text #:recovery-parse-manifest-text))

(defpackage #:pg.wal-batch-grammar
  (:use #:cl)
  ;; Import established identities so existing replay callers remain valid on reload.
  (:import-from #:pg
                #:*wal-recovery-group-active* #:*wal-recovery-reservation-batch-clean-p*
                #:*wal-recovery-page-images* #:*wal-recovery-relation-resets* #:*wal-recovery-page-records-seen-p*
                #:*wal-recovery-catalog-chunk-count* #:*wal-recovery-catalog-next-chunk*
                #:*wal-recovery-logical-xid* #:*wal-recovery-logical-chunk-count*
                #:*wal-recovery-logical-next-chunk* #:*wal-recovery-logical-bytes* #:*wal-recovery-logical-catalog-event-p*
                #:wal-recovery-stage-relation-reset #:wal-empty-metadata-terminal-type-p
                #:wal-note-recovery-batch-record #:wal-validate-empty-metadata-terminal
                #:wal-check-batch-logical-terminal #:wal-decode-batch-terminal #:wal-decode-next-catalog-chunk
                #:wal-decode-relation-reset #:wal-record-xid #:wal-record-type #:wal-record-payload #:wal-require-ordinary-record-type
                #:+wal-record-running-xacts+ #:+wal-record-xid-reservation+ #:+wal-record-parameter-change+
                #:+wal-record-full-page-writes-change+ #:+wal-record-logical-decoding-status-change+ #:+wal-record-batch-start+
                #:+wal-record-catalog-commit+ #:+wal-record-commit+ #:+wal-record-catalog+ #:+wal-record-clog-reset+
                #:+wal-record-checkpoint+ #:+wal-record-maintenance+
                #:wal-running-xacts-record-info #:wal-parameter-change-record-info #:wal-full-page-writes-change-record-info
                #:wal-logical-decoding-status-record-info #:wal-xid-reservation-record-bound #:wal-decode-commit-timestamp
                #:wal-decode-catalog-commit-payload #:wal-read-u64-payload #:wal-checkpoint-record-snapshot #:wal-decode-catalog-chunk-payload
                #:wal-check-batch-page-record #:wal-check-batch-logical-changes #:wal-check-batch-logical-catalog-event #:wal-u32)
  (:export #:wal-decode-batch-terminal #:wal-decode-next-catalog-chunk
           #:wal-check-batch-logical-terminal #:wal-validate-empty-metadata-terminal))

;;;; pg-limits.lisp -- PostgreSQL limits and this server's page format.

(in-package #:pg)

(defconstant +blcksz+ 8192)
(defconstant +maxalign+ 8)
(defconstant +size-of-page-header+ 24)
(defconstant +size-of-item-id+ 4)
;;; Version 5 uses 64-bit XIDs, so its heap tuple header differs from PostgreSQL's.
(defconstant +size-of-heap-tuple-header+ 31)
(defconstant +heap-tuple-xmin-off+ 0)
(defconstant +heap-tuple-xmax-off+ 8)
(defconstant +heap-tuple-cid-off+ 16)
(defconstant +heap-tuple-ctid-block-off+ 20)
(defconstant +heap-tuple-ctid-offnum-off+ 24)
(defconstant +heap-tuple-natts-off+ 26)
(defconstant +heap-tuple-infomask-off+ 28)
(defconstant +heap-tuple-hoff-off+ 30)
(defconstant +namedatalen+ 64)
(defconstant +pg-page-layout-version+ 5)

;;; Wire message sizes include their length field, as PostgreSQL specifies.
(defconstant +max-startup-message-size+ 10000)
(defconstant +max-query-message-size+ (* 16 1024 1024))

;;; Transaction IDs.
(defconstant +invalid-xid+ 0)
(defconstant +first-normal-xid+ 3)
(defconstant +max-xid+ #xFFFFFFFFFFFFFFFF)
(defconstant +max-xid-one-past+ #x10000000000000000)

;;; OIDs.
(defconstant +invalid-oid+ 0)
(defconstant +first-normal-object-id+ 16384)

;;; Heap tuple infomask bits (subset of htup_details.h).
(defconstant +heap-hasnull+ #x0001)
(defconstant +heap-hasvarwidth+ #x0002)
(defconstant +heap-xmin-committed+ #x0100)
(defconstant +heap-xmin-invalid+ #x0200)
(defconstant +heap-xmax-invalid+ #x0400)
(defconstant +heap-xmax-committed+ #x0800)
(defconstant +heap-xmax-is-multi+ #x1000)
(defconstant +heap-xmax-locked-only+ #x2000)

;;; Line pointer flags.
(defconstant +lp-unused+ 0)
(defconstant +lp-normal+ 1)
(defconstant +lp-redirect+ 2)
(defconstant +lp-dead+ 3)

;;; Page flags.
(defconstant +pd-has-free-space+ #x0001)
(defconstant +pd-all-visible+ #x0002)

;;; SQL NULL marker used by the executor and heap tuple values.
(defconstant +null+ :sql-null)

;;; Integer ranges.
(defconstant +int4-min+ -2147483648)
(defconstant +int4-max+ 2147483647)
(defconstant +int8-min+ -9223372036854775808)
(defconstant +int8-max+ 9223372036854775807)

;;; Data directory, set at startup.
(defvar *pgdata* nil)

(defun maxalign (n)
  (logand (+ n (1- +maxalign+)) (lognot (1- +maxalign+))))

(defun align-up (n align)
  (logand (+ n (1- align)) (lognot (1- align))))

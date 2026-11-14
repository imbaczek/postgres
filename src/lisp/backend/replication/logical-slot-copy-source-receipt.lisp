(in-package #:pg)

(defconstant +logical-slot-copy-source-receipt-maximum-bytes+ (+ (* 16 1024 1024) 4096))

(defun logical-slot-copy-source-receipt-image (value)
  ;; Store original R metadata and bounded historical catalog and generation
  ;; observations, not the catalog or a recomputable generation proof.  An
  ;; owned publisher must derive and seal these from fully validated R.
  (unless (and (= 12 (logical-slot-copy-descriptor-list-count value 12))
               (equal (loop for key in value by #'cddr collect key)
                      '(:metadata :generation-size :catalog-size :catalog-digest :next-oid :local-floor))
               (plusp (logical-slot-copy-descriptor-list-count (getf value :metadata) 64))
               (typep (getf value :generation-size) '(integer 34 1073741824))
               (typep (getf value :catalog-size) '(integer 1 1073741824))
               (< (getf value :catalog-size) (getf value :generation-size))
               (typep (getf value :catalog-digest) '(unsigned-byte 64))
               (typep (getf value :next-oid) '(unsigned-byte 64))
               (typep (getf value :local-floor) '(integer 3 18446744073709551616)))
    (error "invalid permanent source restartpoint receipt"))
  (logical-slot-copy-literal-image
   "PGLOGICALSOURCER1" +logical-slot-copy-source-receipt-maximum-bytes+
   value "permanent source restartpoint receipt"))

(defun logical-slot-copy-source-receipt-decode (bytes)
  ;; Decode the bounded receipt; this does not establish its provenance.
  (logical-slot-copy-literal-decode
   bytes "PGLOGICALSOURCER1" +logical-slot-copy-source-receipt-maximum-bytes+
   #'logical-slot-copy-source-receipt-image "permanent source restartpoint receipt"))

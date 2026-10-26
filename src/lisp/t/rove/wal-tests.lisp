;;;; wal-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-wal-catalog-codec-and-page-checksums
    (let* ((catalog (make-array 17 :element-type '(unsigned-byte 8)
                                   :initial-element 65))
           (payload (wal-encode-catalog-payload catalog '(123 456))))
      (multiple-value-bind (deletes decoded)
          (wal-decode-catalog-payload payload)
        (rove:ok (equal deletes '(123 456)) "WAL catalog delete list round-trips")
        (rove:ok (equalp decoded catalog) "WAL catalog image round-trips"))
      (let ((corrupt (copy-seq payload)))
        (decf (aref corrupt 12))
        (rove:ok (handler-case
                     (progn (wal-decode-catalog-payload corrupt) nil)
                   (error () t))
                 "WAL catalog image length mismatch is rejected")))
  (let ((page (page-init (make-page))))
    (rove:ok (wal-verify-page page 123 4) "unstamped page is accepted for staging")
    (wal-stamp-page page 99 123 4)
    (rove:ok (wal-verify-page page 123 4) "WAL page checksum verifies at its location")
    (rove:ok (not (wal-verify-page page 123 5))
             "WAL page checksum binds the block number")
    (setf (aref page 100) (logxor (aref page 100) #xFF))
    (rove:ok (not (wal-verify-page page 123 4))
             "WAL page checksum detects page damage")))

(rove:deftest test-wal-record-codec
    (let* ((payload (make-array 0 :element-type '(unsigned-byte 8)))
           (record (wal-encode-record +wal-record-commit+ 42 64 payload))
           (decoded (wal-decode-record record 64)))
      (rove:ok (= (wal-record-type decoded) +wal-record-commit+)
               "WAL record type round-trips")
      (rove:ok (= (wal-record-xid decoded) 42) "WAL record XID round-trips")
      (rove:ok (= (wal-record-start decoded) 64) "WAL record LSN round-trips")
      (rove:ok (= (wal-record-end decoded) (+ 64 +wal-header-size+))
               "WAL record end LSN is computed")
      (rove:ok (equalp (wal-record-payload decoded) payload)
               "WAL record payload round-trips")
      (dolist (case
                  (list
                   (cons "bad WAL record magic is rejected"
                         (lambda (bytes) (setf (aref bytes 0) 0)))
                   (cons "unsupported WAL record version is rejected"
                         (lambda (bytes) (wal-set-u16 bytes 4 1)))
                   (cons "unknown WAL record type is rejected"
                         (lambda (bytes) (wal-set-u16 bytes 6 #xFFFF)))
                   (cons "bad WAL record length is rejected"
                         (lambda (bytes) (wal-set-u32 bytes 8 31)))
                   (cons "WAL record at the wrong LSN is rejected"
                         (lambda (bytes) (wal-set-u64 bytes 12 65)))
                   (cons "bad WAL record checksum is rejected"
                         (lambda (bytes)
                           (setf (aref bytes 20) (logxor (aref bytes 20) #xFF))))))
        (let ((corrupt (copy-seq record)))
          (funcall (cdr case) corrupt)
          (rove:ok (handler-case
                       (progn (wal-decode-record corrupt 64) nil)
                     (error () t))
                   (car case))))))

(rove:deftest test-wal-page-image-xid-codec-compatibility
    (let* ((lsn 4096)
           (page (page-init (make-page)))
           (image (make-wal-page-image :relfilenode 7310 :block 3 :page page
                                       :xids '(7201 7202)))
           (payload (wal-encode-page-images (list image) lsn))
           (decoded (first (wal-decode-page-images payload lsn +wal-version+)))
           (no-xid-image (make-wal-page-image
                          :relfilenode 7311 :block 4 :page (page-init (make-page))))
           (one-xid-image (make-wal-page-image
                           :relfilenode 7312 :block 5 :page (page-init (make-page))
                           :xids '(7203)))
           (multi-payload
             (wal-encode-page-images (list image no-xid-image one-xid-image) lsn))
           (multi-decoded
             (wal-decode-page-images multi-payload lsn +wal-version+))
           (legacy-v3 (make-array (+ 4 12 +blcksz+)
                                  :element-type '(unsigned-byte 8)
                                  :initial-element 0))
           (legacy-v2 (make-array (+ 4 8 +blcksz+)
                                  :element-type '(unsigned-byte 8)
                                  :initial-element 0)))
      (rove:ok (equal (wal-page-image-xids decoded) '(7201 7202))
               "WAL v4 page images retain transaction contributors")
      (rove:ok (= (length payload) (+ 4 12 +blcksz+ 4 (* 8 2)))
               "WAL v4 page-image length includes variable contributor data")
      (rove:ok (equal (mapcar #'wal-page-image-xids multi-decoded)
                      '((7201 7202) nil (7203)))
               "WAL v4 multi-page records retain each image's contributor list")
      (wal-set-u32 legacy-v3 0 1)
      (wal-set-u32 legacy-v3 4 7310)
      (wal-set-u32 legacy-v3 8 0)
      (wal-set-u32 legacy-v3 12 3)
      (replace legacy-v3 page :start1 16)
      (wal-set-u32 legacy-v2 0 1)
      (wal-set-u32 legacy-v2 4 7310)
      (wal-set-u32 legacy-v2 8 3)
      (replace legacy-v2 page :start1 12)
      (rove:ok (null (wal-page-image-xids
                      (first (wal-decode-page-images legacy-v3 lsn +wal-version-v3+))))
               "WAL v3 page images remain readable without contributor metadata")
      (rove:ok (null (wal-page-image-xids
                      (first (wal-decode-page-images legacy-v2 lsn +wal-version-v2+))))
               "WAL v2 page images remain readable without fork metadata")))

(rove:deftest test-with-wal-critical-validation-errors-do-not-poison
    (let ((*wal-poisoned* nil))
      (rove:ok (handler-case
                   (progn
                     (with-wal-critical
                       (wal-invalid-input "invalid test input"))
                     nil)
                 (wal-input-validation-error () t))
               "WAL input validation errors propagate through the critical macro")
      (rove:ok (not *wal-poisoned*)
               "WAL input validation errors do not poison the critical section")
      (rove:ok (eq :healthy
                   (with-wal-critical :healthy))
               "the critical macro releases its lock after an input validation error")))

(rove:deftest test-wal-drop-missing-slot-does-not-poison-wal
    (let ((*wal-ready* t)
          (*wal-poisoned* nil)
          (*wal-replication-slots* (make-hash-table :test #'equal)))
      (rove:ok (handler-case
                   (progn (wal-drop-replication-slot "missing_slot") nil)
                 (error () t))
               "dropping a missing slot reports an error")
      (rove:ok (not *wal-poisoned*)
               "a missing slot does not poison WAL or request fail-stop shutdown")))

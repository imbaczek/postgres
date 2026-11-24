;;;; Explicit timeline and segment identity with legacy namespace refusal.
(in-package #:pg-physical-tests)

(deftest raw-directory-inventory-preserves-hidden-and-dangling-entries
    (with-physical-store-directory (root)
      (let ((directory (merge-pathnames "inventory/" root))
            (link (merge-pathnames "inventory-link" root))
            (expected '(".pending.tmp" "broken.wal" "valid.slot")))
        (pg::wal-ensure-directory-durable directory)
        (dolist (name '(".pending.tmp" "valid.slot"))
          (with-open-file (stream (merge-pathnames name directory)
                                  :direction :output :element-type '(unsigned-byte 8))
            (finish-output stream)))
        (sb-posix:symlink "missing-target" (namestring (merge-pathnames "broken.wal" directory)))
        (ok (equal expected (sort (pg::directory-entry-names directory) #'string<)))
        (sb-posix:symlink (namestring directory) (namestring link))
        (ok (intake-fails-p (lambda () (pg::directory-entry-names link))))
        (ok (equal expected (sort (pg::directory-entry-names link :follow-symlinks t) #'string<)))
        (ok (intake-fails-p
             (lambda () (pg::directory-entry-names (merge-pathnames "valid.slot" directory)))))
        (ok (null (pg::directory-entry-names (merge-pathnames "absent/" root)))))))

(deftest timeline-qualified-segment-names-use-c-log-and-segment-fields
    (ok (equal "000000030000000100000002.wal" (pg::wal-segment-filename 258 3 :timeline)))
  (ok (intake-fails-p (lambda () (pg::wal-segment-filename 258 1 :legacy))))
  (multiple-value-bind (timeline segment)
      (pg::wal-parse-segment-name "000000030000000100000002" :timeline)
    (ok (= timeline 3)) (ok (= segment 258)))
  (ok (null (pg::wal-parse-segment-name "0000000000000102" :legacy)))
  (ok (null (pg::wal-parse-segment-name "0000000000000102" :timeline)))
  (ok (pg::wal-retired-segment-name-p "0000000000000102.wal"))
  (ok (intake-fails-p (lambda () (pg::wal-segment-filename 258 3 :legacy))))
  (dolist (name '("000000000000000000000000" "000000010000000000000100"
                  "00000001000000000000000a" "../000000010000000000000001"))
    (ok (null (pg::wal-parse-segment-name name :timeline)))))

(cl-spec:defproperty wal-segment-identity-roundtrips-without-branch-aliasing
    ((timeline (range integer 1 4294967295)) (segment (range integer 0 1099511627775)))
  (:about pg::wal-segment-filename)
  (:trials (:normal 200))
  (let ((name (subseq (pg::wal-segment-filename segment timeline :timeline) 0 24)))
    (multiple-value-bind (decoded-timeline decoded-segment)
        (pg::wal-parse-segment-name name :timeline)
      (and (= timeline decoded-timeline) (= segment decoded-segment)
           (null (pg::wal-parse-segment-name name :legacy))))))

(deftest generated-timeline-qualified-segment-identities
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'wal-segment-identity-roundtrips-without-branch-aliasing :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest wal-discovery-selects-explicit-namespace-and-timeline
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (dolist (name '("0000000000000001.wal" "000000010000000000000002.wal"
                        "000000020000000000000002.wal" "000000020000000000000003.wal"))
          (pg::wal-atomic-write-bytes (merge-pathnames (concatenate 'string "pg_wal/" name) root)
                                      (make-array 0 :element-type '(unsigned-byte 8))))
        (ok (null (pg::wal-discover-segments 1 :legacy)))
        (ok (equal '(2) (pg::wal-discover-segments 1 :timeline)))
        (ok (equal '(2 3) (pg::wal-discover-segments 2 :timeline)))
        (ok (null (pg::wal-discover-segments 3 :timeline))))))

(deftest wal-init-refuses-legacy-segment-before-mutating-qualified-storage
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*wal-ready* nil)
             (incarnation pg::*wal-storage-incarnation*)
             (control (pg::wal-control-path))
             (bytes (pg::wal-encode-control-image 0 0 nil))
             (legacy (merge-pathnames "pg_wal/0000000000000063.wal" root)))
        (pg::wal-atomic-write-bytes control bytes)
        (pg::wal-atomic-write-bytes legacy #(1 2 3))
        (ok (handler-case (progn (pg::wal-init) nil)
              (error (condition) (search "requires timeline-qualified WAL" (princ-to-string condition)))))
        (ok (not pg::*wal-ready*))
        (ok (eq incarnation pg::*wal-storage-incarnation*))
        (ok (not (probe-file (merge-pathnames "base/" root))))
        (ok (equalp bytes (pg::recovery-read-bytes control)))
        (ok (equalp #(1 2 3) (pg::recovery-read-bytes legacy))))))

(deftest missing-control-never-ignores-a-dangling-wal-entry-in-either-namespace
    (dolist (namespace '(:legacy :timeline))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (pg::*wal-ready* nil) (pg::*wal-segment-namespace* :legacy)
                                   (path (if (eq namespace :legacy)
                                             (merge-pathnames "pg_wal/0000000000000063.wal" root)
                                             (pg::wal-segment-path 99 1 :timeline))))
          (ensure-directories-exist path)
          (sb-posix:symlink "missing-wal-target" (namestring path))
          (ok (intake-fails-p #'pg::wal-init))
          (ok (not pg::*wal-ready*))
          (ok (not (probe-file (pg::wal-control-path))))))))

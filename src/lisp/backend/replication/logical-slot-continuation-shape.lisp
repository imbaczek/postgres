(in-package #:pg)

(defconstant +logical-slot-continuation-root-limit+ 256)
(defconstant +logical-slot-continuation-run-limit+ 65536)
(defconstant +logical-slot-continuation-obligation-limit+ 1024)

(defconstant +logical-slot-continuation-work-limit+ 16777216)

(defstruct (logical-slot-continuation-plan
            (:constructor %make-logical-slot-continuation-plan)
            (:print-function physical-libpq-print-private-object))
  (snapshot nil :read-only t)
  (floor nil :read-only t) (root-requests nil :read-only t)
  (run-requests nil :read-only t) (seam-requests nil :read-only t)
  (tail-request nil :read-only t))

(defun logical-slot-continuation-maximum-bytes ()
  (+ (logical-slot-copy-source-proof-maximum-bytes) (* 64 1024 1024)))

(defun logical-slot-continuation-copy (value)
  (typecase value
    (cons (loop for item in value collect (logical-slot-continuation-copy item)))
    (vector (copy-seq value))
    (t value)))

(defun logical-slot-continuation-list (value count)
  (unless (= count (logical-slot-copy-descriptor-list-count value count))
    (error "invalid continuation tuple"))
  value)

(defun logical-slot-continuation-plist (value keys)
  (logical-slot-continuation-list value (* 2 (length keys)))
  (unless (equal keys (loop for key in value by #'cddr collect key))
    (error "invalid continuation field order"))
  value)

(defun logical-slot-continuation-position-shape (position)
  (logical-slot-continuation-list position 3)
  (unless (and (logical-slot-position-valid-p (first position) (second position))
               (member (third position) '(:record-start :prefix-end)))
    (error "invalid continuation position"))
  position)

(defun logical-slot-continuation-ascii-size (text maximum)
  (unless (and (stringp text) (<= 1 (length text) maximum)
               (every (lambda (character) (<= 1 (char-code character) 127)) text))
    (error "invalid continuation identifier"))
  (length text))

(defun logical-slot-continuation-root-shape (root)
  (logical-slot-continuation-list root 5)
  (logical-slot-continuation-plist
   (third root) '(:root-id :base-timeline :base :manifest-digest :end :end-digest))
  (unless (and (typep (first root) '(integer 1 18446744073709551615))
               (typep (second root) '(integer 1 18446744073709551615))
               (wal-retained-reference-valid-p (third root))
               (typep (fourth root) '(integer 1 4294967295)))
    (error "invalid continuation root"))
  (+ 68 (logical-slot-copy-descriptor-blob-size
         (fifth root) 0 +physical-store-history-maximum-bytes+)))

(defun logical-slot-continuation-run-shape (run)
  (logical-slot-continuation-list run 5)
  (logical-slot-continuation-position-shape (second run))
  (logical-slot-continuation-position-shape (third run))
  (unless (and (typep (first run) '(integer 1 18446744073709551615))
               (typep (fourth run) '(integer 1 4294967295))
               (typep (fifth run) '(integer 1 4294967295))
               (eq :record-start (third (second run)))
               (eq :prefix-end (third (third run))))
    (error "invalid continuation run"))
  42)

(defun logical-slot-continuation-shape (snapshot)
  (logical-slot-continuation-plist
   snapshot '(:version :binding :generation :system-id :origin :history-target :history
              :sealed-end :slots :transfers :roots :runs :tail))
  (logical-slot-continuation-list (getf snapshot :origin) 2)
  (logical-slot-continuation-position-shape (getf snapshot :sealed-end))
  (unless (and (eql 1 (getf snapshot :version))
               (typep (getf snapshot :generation) '(integer 1 18446744073709551615))
               (typep (getf snapshot :system-id) '(integer 1 18446744073709551615))
               (typep (first (getf snapshot :origin)) '(integer 2 4294967295))
               (typep (second (getf snapshot :origin)) '(unsigned-byte 64))
               (typep (getf snapshot :history-target) '(integer 1 4294967295)))
    (error "invalid continuation snapshot fields"))
  (loop for key in '(:slots :transfers :roots :runs)
        for maximum in (list +logical-slot-continuation-obligation-limit+
                             +logical-slot-continuation-obligation-limit+
                             +logical-slot-continuation-root-limit+ +logical-slot-continuation-run-limit+)
        do (logical-slot-copy-descriptor-list-count (getf snapshot key) maximum))
  (let ((size (+ 24 8 8 4 8 4 4 4 13 4 4 4 4 1
                 (logical-slot-copy-descriptor-blob-size
                  (getf snapshot :binding) 1 (logical-slot-copy-source-proof-maximum-bytes))
                 (logical-slot-copy-descriptor-blob-size
                  (getf snapshot :history) 0 +physical-store-history-maximum-bytes+))))
    (dolist (row (getf snapshot :slots))
      (logical-slot-continuation-list row 2)
      (incf size (+ 8 (logical-slot-continuation-ascii-size (first row) 63)
                    (logical-slot-copy-descriptor-blob-size (second row) 1 +slot-state-maximum-bytes+))))
    (dolist (row (getf snapshot :transfers))
      (logical-slot-continuation-list row 2)
      (logical-slot-continuation-position-shape (second row))
      (logical-slot-parse-generation (first row))
      (incf size 49))
    (dolist (root (getf snapshot :roots))
      (incf size (logical-slot-continuation-root-shape root)))
    (dolist (run (getf snapshot :runs))
      (incf size (logical-slot-continuation-run-shape run)))
    (when (getf snapshot :tail)
      (logical-slot-continuation-list (getf snapshot :tail) 2)
      (logical-slot-continuation-position-shape (first (getf snapshot :tail)))
      (unless (typep (second (getf snapshot :tail)) '(integer 1 4294967295))
        (error "invalid continuation tail carrier"))
      (incf size 17))
    (unless (<= size (logical-slot-continuation-maximum-bytes) (+ 24 #xffffffff))
      (error "continuation snapshot exceeds its byte profile"))
    size))

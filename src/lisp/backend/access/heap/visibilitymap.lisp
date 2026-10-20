;;;; visibilitymap.lisp -- persistent per-heap-page visibility flags.

(in-package #:pg)

(defconstant +vm-bits-per-heap-block+ 2)
(defconstant +vm-heap-blocks-per-byte+ 4)
(defconstant +vm-heap-blocks-per-page+
  (* +vm-heap-blocks-per-byte+
     (- +blcksz+ +size-of-page-header+)))
(defconstant +vm-all-visible-bit+ #b01)
(defconstant +vm-all-frozen-bit+ #b10)

(defun visibilitymap-location (heap-block)
  (let* ((vm-block (floor heap-block +vm-heap-blocks-per-page+))
         (index (mod heap-block +vm-heap-blocks-per-page+))
         (byte-offset (+ +size-of-page-header+
                         (floor index +vm-heap-blocks-per-byte+)))
         (bit-shift (* +vm-bits-per-heap-block+ (mod index 4))))
    (values vm-block byte-offset bit-shift)))

(defun visibilitymap-status (relfilenode heap-block)
  "Return the two-bit visibility status for HEAP-BLOCK, or zero if unset."
  (multiple-value-bind (vm-block byte-offset bit-shift)
      (visibilitymap-location heap-block)
    (let ((page (buf-read relfilenode vm-block :visibility)))
      (if page
          (logand (ash (aref page byte-offset) (- bit-shift)) #b11)
          0))))

(defun visibilitymap-clear (relfilenode heap-block)
  "Clear all visibility bits if HEAP-BLOCK already has a VM entry."
  (with-wal-page-batch
    (multiple-value-bind (vm-block byte-offset bit-shift)
        (visibilitymap-location heap-block)
      (when (< vm-block (buf-nblocks relfilenode :visibility))
        (let ((page (buf-read relfilenode vm-block :visibility)))
          (when page
            (let ((mask (ash #b11 bit-shift)))
              (setf (aref page byte-offset)
                    (logand (aref page byte-offset) (logxor #xFF mask)))
              (buf-write relfilenode vm-block page :visibility)))))))
  (values))

(defun visibilitymap-set-status (relfilenode heap-block status)
  "Set the visibility bits after the heap page has been checked and updated."
  (unless (and (<= 0 status #b11)
               (or (zerop (logand status +vm-all-frozen-bit+))
                   (not (zerop (logand status +vm-all-visible-bit+)))))
    (error "invalid visibility map status ~a" status))
  (with-wal-page-batch
    (multiple-value-bind (vm-block byte-offset bit-shift)
        (visibilitymap-location heap-block)
      (let* ((nblocks (buf-nblocks relfilenode :visibility))
             (page (if (< vm-block nblocks)
                       (buf-read relfilenode vm-block :visibility)
                       (page-init (make-page)))))
        (unless page
          (error "visibility map page ~a is missing" vm-block))
        (if (< vm-block nblocks)
            (let ((mask (ash #b11 bit-shift)))
              (setf (aref page byte-offset)
                    (logior (logand (aref page byte-offset)
                                    (logxor #xFF mask))
                            (ash status bit-shift)))
              (buf-write relfilenode vm-block page :visibility))
            (progn
              (unless (= vm-block nblocks)
                (error "visibility map pages must be extended in order"))
              (setf (aref page byte-offset) (ash status bit-shift))
              (unless (= (buf-extend relfilenode page :visibility) vm-block)
                (error "visibility map fork changed while extending")))))))
  (values))

(defun visibilitymap-mark-visible (relfilenode heap-block &optional all-frozen)
  (visibilitymap-set-status
   relfilenode heap-block
   (if all-frozen
       (logior +vm-all-visible-bit+ +vm-all-frozen-bit+)
       +vm-all-visible-bit+)))

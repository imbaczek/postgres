;;;; Native source allocation metadata; never a grant of writer authority.
(in-package #:pg)

(defun wal-xid-reservation-valid-bound-p (bound)
  "BOUND is inclusive; its successor is a normal FullXID or exhaustion sentinel."
  (and (typep bound '(integer 2 18446744073709551615))
       (= (1+ bound) (canonical-next-full-xid (1+ bound)))))

(defun wal-encode-xid-reservation (bound)
  (unless (wal-xid-reservation-valid-bound-p bound)
    (error "invalid inclusive XID reservation bound"))
  (let ((bytes (make-array 8 :element-type '(unsigned-byte 8))))
    (wal-set-u64 bytes 0 bound)
    bytes))

(defun wal-decode-xid-reservation (bytes)
  (unless (= (length bytes) 8) (error "invalid XID reservation payload length"))
  (let ((bound (wal-u64 bytes 0)))
    (unless (wal-xid-reservation-valid-bound-p bound)
      (error "XID reservation ends on a special or unrepresentable counter"))
    bound))

(defun wal-xid-reservation-record-bound (record)
  "Validate the schema introduced in v7; the WAL header decoder checks versions."
  (unless (and (= (wal-record-type record) +wal-record-xid-reservation+)
               (>= (wal-record-version record) 7)
               (zerop (wal-record-xid record))
               (typep (wal-record-start record) '(unsigned-byte 64))
               (typep (wal-record-end record) '(unsigned-byte 64))
               (= (wal-record-end record) (+ (wal-record-start record) +wal-header-size+ 8))
               (= (floor (wal-record-start record) +wal-segment-size+)
                  (floor (1- (wal-record-end record)) +wal-segment-size+)))
    (error "invalid native XID reservation record"))
  (wal-decode-xid-reservation (wal-record-payload record)))

(defvar *xid-reservation-test-hook* nil)

(defun xid-reservation-test-point (point end)
  (when *xid-reservation-test-hook* (funcall *xid-reservation-test-hook* point end)))

(defun xid-reserve-wal-batch-under-lock ()
  "Caller holds WAL then XID. No identifier may escape before both durable writes."
  (let ((end (xid-reservation-end *next-xid*)))
    (wal-append-batch-start)
    (wal-append-record +wal-record-xid-reservation+ 0
                       (wal-encode-xid-reservation (1- end)))
    (wal-flush)
    (xid-reservation-test-point :after-wal-flush end)
    (let ((*xid-batch-end* end)) (save-next-xid))
    (xid-reservation-test-point :after-control-save end)
    (setf *xid-batch-end* end
          *wal-xid-reserved-through* (max (or *wal-xid-reserved-through* 2) (1- end)))
    (xid-reservation-test-point :after-capacity-publication end)))

(defun xid-assign-with-wal-reservation (oldest)
  "Slow path rechecks capacity and ordinary refusals before entering critical I/O."
  (let ((result nil) (failure nil) (started nil) (completed nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (sb-sys:with-local-interrupts
             (sb-thread:with-mutex (*wal-lock*)
               (wal-check-healthy)
               (sb-thread:with-mutex (*xid-lock*)
                 (xid-check-allocation-under-lock oldest)
                 (when (>= *next-xid* *xid-batch-end*)
                   (sb-sys:without-interrupts
                     (setf started t)
                     (unwind-protect
                          (handler-case
                              (progn (xid-reserve-wal-batch-under-lock) (setf completed t))
                            (error (condition) (setf failure condition)))
                       (unless completed (setf *wal-poisoned* t)))))
                 (unless failure (setf result (xid-allocate-under-lock))))))
        (cond
          (failure (wal-fail-stop failure))
          ((and started (not completed))
           (when (fboundp 'request-fast-shutdown)
             (ignore-errors (funcall (symbol-function 'request-fast-shutdown))))))))
    result))

(defun xid-restore-reservation-floor (bound)
  "Primary startup only: persist a replayed allocation floor before listeners."
  (unless (wal-xid-reservation-valid-bound-p bound)
    (error "invalid replayed XID reservation floor"))
  (sb-thread:with-mutex (*xid-lock*)
    (let ((end (max *next-xid* *xid-batch-end* (1+ bound))))
      (when (> end *xid-batch-end*)
        (let ((*xid-batch-end* end)) (save-next-xid)))
      (setf *next-xid* end *xid-batch-end* end
            *wal-xid-reserved-through* (max (or *wal-xid-reserved-through* 2) (1- end))))))

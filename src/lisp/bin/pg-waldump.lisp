(require :asdf)

(in-package #:cl-user)

(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")))

(in-package #:pg)

(defun native-waldump-checkpoint (directory timeline start)
  (call-with-physical-adoption-owner
   directory
   (lambda (owner)
     (let ((state (physical-adoption-inspect-primary owner)))
       (unless (and (= timeline (getf state :timeline))
                    (= start (getf state :checkpoint-start)))
         (error "requested coordinates do not select the clean shutdown checkpoint"))
       (let* ((*pgdata* (physical-adoption-owner-directory owner))
              (end (getf state :end))
              (path (wal-segment-path (floor end +wal-segment-size+)
                                      timeline (getf state :namespace)))
              (offset (mod end +wal-segment-size+)))
         (when (or (plusp offset) (physical-adoption-path-present-p path))
           (multiple-value-bind (stream size) (open-bounded-regular-file path +wal-segment-size+)
             (unwind-protect
                  (progn
                    (unless (and (<= offset size) (file-position stream offset))
                      (error "could not seek to the shutdown checkpoint end"))
                    (loop repeat (min +wal-header-size+ (- size offset))
                          for byte = (read-byte stream)
                          unless (zerop byte)
                            do (error "unexpected WAL after the shutdown checkpoint")))
               (close stream))))
         (physical-adoption-check-owner owner)
         ;; Patroni uses this two-record probe to locate the end of the first
         ;; record. Native WAL has no previous-record pointer.
         (format t "checkpoint, lsn: ~a, prev unavailable~%" (wal-format-lsn start))
         (format *error-output*
                 "pg_waldump: error in WAL record at ~X/~X: invalid record length at ~X/~X: expected at least ~d, got 0~%"
                 (ldb (byte 32 32) start) (ldb (byte 32 0) start)
                 (ldb (byte 32 32) end) (ldb (byte 32 0) end) +wal-header-size+)
         1)))))

(defun native-waldump-main (arguments)
  (handler-case
      (destructuring-bind (timeline-option timeline start-option start limit-option limit) arguments
        (unless (and (string= timeline-option "-t")
                     (string= start-option "-s")
                     (string= limit-option "-n")
                     (string= limit "2"))
          (error "expected -t TIMELINE -s LSN -n 2"))
        (let ((directory (uiop:getenv "PGDATA"))
              (timeline (parse-integer timeline))
              (start (parse-replication-lsn start)))
          (unless (and directory (plusp (length directory))
                       (typep timeline '(integer 1 #xffffffff))
                       (typep start '(unsigned-byte 64)))
            (error "invalid PGDATA, timeline or record location"))
          (native-waldump-checkpoint directory timeline start)))
    (error (condition)
      (format *error-output* "pg_waldump: ~a~%" condition)
      1)))

(sb-ext:exit :code (native-waldump-main (uiop:command-line-arguments)))

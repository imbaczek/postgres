;;;; WAL output durability across segment rotation.
(in-package #:pg)

(defun call-with-writer-output-test (function)
  (call-with-queued-clog-test
   (lambda ()
     (let ((*pm-requested-mode* +no-shutdown+))
       (funcall function)))))

(defun writer-output-test-rotate ()
  (let ((old (wal-open-output 0)))
    (write-byte 17 old)
    (setf *wal-end-lsn* 1)
    (write-byte 23 (wal-open-output 1))
    (setf *wal-end-lsn* (1+ +wal-segment-size+))
    old))

(rove:deftest writer-output-rotation-syncs-before-opening-the-next-segment
    (call-with-writer-output-test
     (lambda ()
       (let ((sync-stream (symbol-function 'fsync-stream))
             (sync-directory (symbol-function 'fsync-directory))
             (syncs nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'fsync-stream)
                      (lambda (stream)
                        (push :file syncs)
                        (funcall sync-stream stream))
                      (symbol-function 'fsync-directory)
                      (lambda (directory)
                        (push :directory syncs)
                        (funcall sync-directory directory)))
                (let ((old (writer-output-test-rotate)))
                  (rove:ok (not (open-stream-p old))))
                (rove:ok (equal '(:file :directory) (reverse syncs)))
                (rove:ok (equal '(1) *wal-new-segments*))
                (rove:ok (zerop *wal-durable-lsn*)))
           (setf (symbol-function 'fsync-stream) sync-stream
                 (symbol-function 'fsync-directory) sync-directory))))))

(defun writer-output-test-flush (fault)
  (call-with-writer-output-test
   (lambda ()
     (let ((sync-stream (symbol-function 'fsync-stream))
           (sync-directory (symbol-function 'fsync-directory))
           (save (symbol-function 'wal-save-durable-lsn))
           (events nil)
           (failure nil))
       (unwind-protect
            (progn
              (setf (symbol-function 'fsync-stream)
                    (lambda (stream)
                      (let ((segment
                              (if (equal (pathname stream) (wal-segment-path 0))
                                  :old :current)))
                        (push segment events)
                        (when (eq segment fault) (error "injected segment sync failure"))
                        (funcall sync-stream stream)))
                    (symbol-function 'fsync-directory)
                    (lambda (directory)
                      (let ((phase (if (zerop *wal-output-segment*)
                                       :old-directory :current-directory)))
                        (push phase events)
                        (when (eq phase fault) (error "injected directory sync failure"))
                        (funcall sync-directory directory)))
                    (symbol-function 'wal-save-durable-lsn)
                    (lambda (end)
                      (push :control events)
                      (when (eq :control fault) (error "injected control publication failure"))
                      (setf *wal-durable-lsn* end)))
              (handler-case
                  (with-wal-critical (writer-output-test-rotate) (wal-flush))
                (error (condition) (setf failure condition)))
              (list :events (reverse events) :failure (not (null failure))
                    :poisoned *wal-poisoned* :durable *wal-durable-lsn*))
         (setf (symbol-function 'fsync-stream) sync-stream
               (symbol-function 'fsync-directory) sync-directory
               (symbol-function 'wal-save-durable-lsn) save))))))

(rove:deftest writer-output-flush-publishes-control-after-the-synced-prefix
    (let ((result (writer-output-test-flush nil)))
      (rove:ok (equal '(:old :old-directory :current :current-directory :control)
                      (getf result :events)))
      (rove:ok (not (getf result :failure)))
      (rove:ok (not (getf result :poisoned)))
      (rove:ok (= (1+ +wal-segment-size+) (getf result :durable)))))

(rove:deftest writer-output-failures-withhold-the-durable-control-claim
    (dolist (fault '(:old :old-directory :current :current-directory :control))
      (let ((result (writer-output-test-flush fault)))
        (rove:ok (getf result :failure))
        (rove:ok (getf result :poisoned))
        (rove:ok (zerop (getf result :durable)))
        (rove:ok (eq fault (car (last (getf result :events))))))))

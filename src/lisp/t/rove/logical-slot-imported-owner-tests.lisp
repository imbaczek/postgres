(in-package #:pg)

(defun call-with-imported-scan-postmaster-test (function)
  (call-with-imported-principal-test
   (lambda (scan)
     (let ((*postmaster-data-lock* nil) (*postmaster-pid-stream* nil)
                                        (*postmaster-pid-directory* nil) (*postmaster-start-time* nil)
                                        (*postmaster-start-timestamp* nil) (*postmaster-pid-host* nil)
                                        (*postmaster-pid-port* nil) (*postmaster-pid-inode* nil)
                                        (exit-hooks sb-ext:*exit-hooks*))
       (sb-posix:chmod (namestring *pgdata*) #o700)
       (unwind-protect
            (progn
              (postmaster-own-data-directory "127.0.0.1" 5439)
              (let ((source (sb-thread:with-mutex (*wal-lock*) (logical-wal-capture-source scan))))
                (funcall function scan source)))
         (postmaster-remove-pid-file)
         (setf sb-ext:*exit-hooks* exit-hooks)
         (when *postmaster-data-lock* (close *postmaster-data-lock*)))))))

(rove:deftest imported-scan-owner-borrows-real-postmaster-and-matching-principal
    (let ((escaped nil))
      (call-with-imported-scan-postmaster-test
       (lambda (scan source)
         (call-with-logical-slot-imported-scan-owner
          scan source
          (lambda (owner)
            (setf escaped owner)
            (rove:ok (physical-directory-check-owner owner))
            (rove:ok (rove:signals (physical-adoption-check-owner owner) 'error))
            (sb-thread:with-mutex (*wal-lock*)
              (rove:ok (logical-slot-imported-scan-check-under-lock owner))
              (rove:ok (rove:signals
                        (logical-slot-imported-scan-check-under-lock
                         (copy-logical-slot-imported-scan-owner owner)) 'error))
              (rove:ok (logical-slot-imported-scan-check-under-lock owner)))))
         (rove:ok (open-stream-p *postmaster-data-lock*))
         (rove:ok (open-stream-p *postmaster-pid-stream*))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok (rove:signals
                     (logical-slot-imported-scan-check-under-lock escaped) 'error)))))))

(rove:deftest imported-scan-owner-rejects-copied-scans-and-purpose-substitution
    (call-with-imported-scan-postmaster-test
     (lambda (scan source)
       (dolist (case '(:copy :purpose))
         (let ((entered nil)
               (selected-scan (if (eq case :copy) (copy-wal-retained-slot-scan scan) scan))
               (selected-source
                 (if (eq case :purpose)
                     (sb-thread:with-mutex (*wal-lock*)
                       (logical-wal-capture-source scan :purpose :terminal-check))
                     source)))
           (rove:ok (rove:signals
                     (call-with-logical-slot-imported-scan-owner
                      selected-scan selected-source (lambda (owner) (declare (ignore owner)) (setf entered t)))
                     'error))
           (rove:ok (not entered))))
       (rove:ok (open-stream-p *postmaster-data-lock*))
       (sb-thread:with-mutex (*wal-lock*)
         (rove:ok (wal-retained-slot-scan-check-scope-under-lock scan))))))

(rove:deftest imported-scan-owner-revokes-after-caught-source-loss
    (call-with-imported-scan-postmaster-test
     (lambda (scan source)
       (let ((entered nil) (escaped nil) (prior *wal-control-metadata*))
         (unwind-protect
              (rove:ok
               (rove:signals
                (call-with-logical-slot-imported-scan-owner
                 scan source
                 (lambda (owner)
                   (setf entered t escaped owner)
                   (sb-thread:with-mutex (*wal-lock*)
                     (let ((catalog *catalog*))
                       (unwind-protect
                            (progn
                              (setf *catalog* nil)
                              (rove:ok (rove:signals
                                        (logical-slot-imported-scan-check-under-lock owner) 'error)))
                         (setf *catalog* catalog)))
                     (setf (physical-adoption-owner-active-p owner) t)
                     (rove:ok (rove:signals
                               (logical-slot-imported-scan-check-under-lock owner) 'error)))))
                'error))
           (setf *wal-control-metadata* prior))
         (rove:ok entered)
         (rove:ok (not (physical-adoption-owner-active-p escaped)))
         (rove:ok (open-stream-p *postmaster-data-lock*))))))

(rove:deftest imported-scan-owner-unwind-preserves-postmaster-borrow
    (call-with-imported-scan-postmaster-test
     (lambda (scan source)
       (let ((escaped nil))
         (rove:ok
          (eq :escaped
              (catch 'owner-exit
                (call-with-logical-slot-imported-scan-owner
                 scan source (lambda (owner) (setf escaped owner) (throw 'owner-exit :escaped))))))
         (rove:ok (not (physical-adoption-owner-active-p escaped)))
         (rove:ok (open-stream-p *postmaster-data-lock*))
         (call-with-logical-slot-imported-scan-owner
          scan source (lambda (owner) (rove:ok (physical-directory-check-owner owner))))))))

(rove:deftest imported-scan-owner-latches-public-and-postmaster-check-failures
    (dolist (failure '(:active :registry :postmaster :root-mode))
      (call-with-imported-scan-postmaster-test
       (lambda (scan source)
         (let ((entered nil))
           (rove:ok
            (rove:signals
             (call-with-logical-slot-imported-scan-owner
              scan source
              (lambda (owner)
                (setf entered t)
                (let ((inode *postmaster-pid-inode*))
                  (unwind-protect
                       (case failure
                         (:active
                          (setf (physical-adoption-owner-active-p owner) nil)
                          (rove:ok (rove:signals (logical-slot-imported-scan-check-owner owner) 'error)))
                         (:registry
                          (let ((*logical-slot-imported-scan-owners* nil))
                            (rove:ok (rove:signals (logical-slot-imported-scan-check-owner owner) 'error))))
                         (:postmaster
                          (setf *postmaster-pid-inode* nil)
                          (rove:ok (rove:signals (logical-slot-imported-scan-check-owner owner) 'error)))
                         (:root-mode
                          (sb-posix:chmod (namestring *pgdata*) #o750)
                          (rove:ok (rove:signals (logical-slot-imported-scan-check-owner owner) 'error))))
                    (setf *postmaster-pid-inode* inode)
                    (sb-posix:chmod (namestring *pgdata*) #o700)))
                (setf (physical-adoption-owner-active-p owner) t)
                (sb-thread:with-mutex (*wal-lock*)
                  (rove:ok (rove:signals
                            (logical-slot-imported-scan-check-under-lock owner) 'error)))))
             'error))
           (rove:ok entered)
           (rove:ok (open-stream-p *postmaster-data-lock*)))))))

(rove:deftest imported-scan-owner-binds-private-history-to-the-source-bytes
    (dolist (when-change '(:before :inside))
      (dolist (change '(:advance :float :cycle :extra :entry :improper))
        (call-with-imported-scan-postmaster-test
         (lambda (scan source)
           (let* ((history (logical-wal-source-history source))
                  (entry (first history)) (tail (cdr history))
                  (begin (timeline-history-entry-begin entry)) (entered nil))
             (labels ((mutate ()
                        (ecase change
                          (:advance (setf (timeline-history-entry-begin entry) (1+ begin)))
                          (:float (setf (timeline-history-entry-begin entry) (float begin 0d0)))
                          (:cycle (setf (cdr history) history))
                          (:extra (setf (cdr history) (list (copy-timeline-history-entry entry))))
                          (:entry (setf (car history) nil))
                          (:improper (setf (cdr history) :invalid-tail))))
                      (restore ()
                        (setf (timeline-history-entry-begin entry) begin
                              (car history) entry (cdr history) tail)))
               (unwind-protect
                    (progn
                      (when (eq when-change :before) (mutate))
                      (rove:ok
                       (rove:signals
                        (call-with-logical-slot-imported-scan-owner
                         scan source
                         (lambda (owner)
                           (setf entered t)
                           (mutate)
                           (sb-thread:with-mutex (*wal-lock*)
                             (rove:ok (rove:signals
                                       (logical-slot-imported-scan-check-under-lock owner) 'error))
                             (restore)
                             (setf (physical-adoption-owner-active-p owner) t)
                             (rove:ok (rove:signals
                                       (logical-slot-imported-scan-check-under-lock owner) 'error)))))
                        'error))
                      (rove:ok (eq (eq when-change :inside) entered)))
                 (restore)))))))))

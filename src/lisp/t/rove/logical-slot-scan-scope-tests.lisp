(in-package #:pg)

(defun call-with-imported-principal-test (function)
  (call-with-owned-cursor-test
   (lambda (slot start middle end)
     (declare (ignore slot middle))
     (call-with-wal-retained-slot-scan "cursor_source" start end function))))

(rove:deftest imported-scan-principal-requires-the-exact-live-scan
    (let ((escaped nil) (frozen nil))
      (call-with-imported-principal-test
       (lambda (scan)
         (setf escaped scan frozen (copy-wal-retained-slot-scan scan))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok (eq scan (wal-retained-slot-scan-check-scope-under-lock scan)))
           (rove:ok (rove:signals
                     (wal-retained-slot-scan-check-scope-under-lock frozen) 'error))
           (incf (wal-retained-slot-scan-cursor scan))
           (rove:ok (eq scan (wal-retained-slot-scan-check-scope-under-lock scan))))))
      (sb-thread:with-mutex (*wal-lock*)
        (rove:ok (rove:signals
                  (wal-retained-slot-scan-check-scope-under-lock escaped) 'error))
        (rove:ok (rove:signals
                  (wal-retained-slot-scan-check-scope-under-lock frozen) 'error)))))

(rove:deftest imported-scan-principal-freezes-every-authority-field
    (dolist (accessor '(wal-retained-slot-scan-name wal-retained-slot-scan-slot
                        wal-retained-slot-scan-owner wal-retained-slot-scan-database
                        wal-retained-slot-scan-plugin wal-retained-slot-scan-timeline
                        wal-retained-slot-scan-namespace wal-retained-slot-scan-start
                        wal-retained-slot-scan-end wal-retained-slot-scan-pin
                        wal-retained-slot-scan-continuation))
      (call-with-imported-principal-test
       (lambda (scan)
         (let ((prior (funcall accessor scan))
               (setter (fdefinition (list 'setf accessor)))
               (other (gensym "OTHER-SCAN-FIELD")))
           (sb-thread:with-mutex (*wal-lock*)
             (funcall setter other scan)
             (rove:ok (rove:signals
                       (wal-retained-slot-scan-check-scope-under-lock scan) 'error)
                      (symbol-name accessor))
             (funcall setter prior scan)
             (rove:ok (rove:signals
                       (wal-retained-slot-scan-check-scope-under-lock scan) 'error)
                      "restoring a field does not revive the principal")))))))

(rove:deftest imported-scan-principal-rejects-in-place-string-changes
    (dolist (accessor '(wal-retained-slot-scan-name wal-retained-slot-scan-plugin))
      (call-with-imported-principal-test
       (lambda (scan)
         ;; Give the scan its own equal mutable string before testing its contents.
         (let* ((setter (fdefinition (list 'setf accessor)))
                (text (copy-seq (funcall accessor scan))))
           (funcall setter text scan)
           (sb-thread:with-mutex (*wal-lock*)
             (rove:ok (wal-retained-slot-scan-check-scope-under-lock scan))
             (setf (char text 0) #\!)
             (rove:ok (rove:signals
                       (wal-retained-slot-scan-check-scope-under-lock scan) 'error))))))))

(rove:deftest imported-scan-principal-cleanup-uses-its-owned-pin
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (declare (ignore slot middle))
       (let ((other (gensym "UNRELATED-PIN")) (owned nil))
         (setf (gethash other *wal-snapshot-retention-pins*) start)
         (unwind-protect
              (progn
                (call-with-wal-retained-slot-scan
                 "cursor_source" start end
                 (lambda (scan)
                   (setf owned (wal-retained-slot-scan-pin scan)
                         (wal-retained-slot-scan-pin scan) other)))
                (rove:ok (not (nth-value 1 (gethash owned *wal-snapshot-retention-pins*))))
                (rove:ok (eql start (gethash other *wal-snapshot-retention-pins*))))
           (remhash other *wal-snapshot-retention-pins*))))))

(rove:deftest imported-scan-principal-unwinds-and-keeps-nested-scopes-independent
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (declare (ignore slot middle))
       (let ((escaped nil))
         (call-with-wal-retained-slot-scan
          "cursor_source" start end
          (lambda (outer)
            (rove:ok
             (eq :escaped
                 (catch 'scan-exit
                   (call-with-wal-retained-slot-scan
                    "cursor_source" start end
                    (lambda (inner)
                      (setf escaped inner)
                      (throw 'scan-exit :escaped))))))
            (sb-thread:with-mutex (*wal-lock*)
              (rove:ok (wal-retained-slot-scan-check-scope-under-lock outer))
              (rove:ok (rove:signals
                        (wal-retained-slot-scan-check-scope-under-lock escaped) 'error)))
            (rove:ok (= 1 (hash-table-count *wal-snapshot-retention-pins*)))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))))))

(rove:deftest imported-scan-principal-does-not-cross-threads-or-wal-mutexes
    (call-with-imported-principal-test
     (lambda (scan)
       (let ((inherited *wal-retained-slot-scan-scopes*)
             (lock *wal-lock*) (rejected nil))
         (sb-thread:join-thread
          (sb-thread:make-thread
           (lambda ()
             (let ((*wal-retained-slot-scan-scopes* inherited) (*wal-lock* lock))
               (sb-thread:with-mutex (*wal-lock*)
                 (setf rejected
                       (handler-case
                           (progn (wal-retained-slot-scan-check-scope-under-lock scan) nil)
                         (error () t))))))))
         (rove:ok rejected)
         (let ((*wal-lock* (sb-thread:make-mutex :name "foreign scan mutex")))
           (sb-thread:with-mutex (*wal-lock*)
             (rove:ok (rove:signals
                       (wal-retained-slot-scan-check-scope-under-lock scan) 'error))))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok (wal-retained-slot-scan-check-scope-under-lock scan)))))))

(rove:deftest imported-terminal-scan-registers-and-releases-its-own-principal
    (dolist (failure '(nil :retag :throw))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore slot end))
         (let ((original (symbol-function 'logical-wal-capture-source))
               (captured nil) (owned-pin nil) (other (gensym "OTHER-TERMINAL-PIN"))
               (entered nil))
           (setf (gethash other *wal-snapshot-retention-pins*) start)
           (unwind-protect
                (progn
                  (setf (symbol-function 'logical-wal-capture-source)
                        (lambda (scan &key (purpose :stream))
                          (setf captured scan owned-pin (wal-retained-slot-scan-pin scan))
                          (rove:ok (eq :terminal-check purpose))
                          (rove:ok (wal-retained-slot-scan-check-scope-under-lock scan))
                          (let ((scope (find scan *wal-retained-slot-scan-scopes*
                                             :key #'wal-retained-slot-scan-scope-scan :test #'eq)))
                            (rove:ok (eq :terminal-check (wal-retained-slot-scan-scope-purpose scope))))
                          (case failure
                            (:retag (setf (wal-retained-slot-scan-pin scan) other))
                            (:throw (throw 'terminal-exit :escaped)))
                          (when failure
                            (wal-retained-slot-scan-check-scope-under-lock scan))
                          (funcall original scan :purpose purpose)))
                  (flet ((run ()
                           (call-with-logical-slot-terminal-check
                            "cursor_source" middle (lambda (current) (declare (ignore current)) (values nil nil))
                            (lambda (current found)
                              (declare (ignore current))
                              (setf entered t)
                              (rove:ok found)
                              (rove:ok (wal-retained-slot-scan-check-scope-under-lock captured))))))
                    (case failure
                      (:retag (rove:ok (rove:signals (run) 'error)))
                      (:throw (rove:ok (eq :escaped (catch 'terminal-exit (run)))))
                      (t (run))))
                  (rove:ok (eq (null failure) entered))
                  (rove:ok (not (nth-value 1 (gethash owned-pin *wal-snapshot-retention-pins*))))
                  (rove:ok (eql start (gethash other *wal-snapshot-retention-pins*)))
                  (sb-thread:with-mutex (*wal-lock*)
                    (rove:ok (rove:signals
                              (wal-retained-slot-scan-check-scope-under-lock captured) 'error))))
             (setf (symbol-function 'logical-wal-capture-source) original)
             (remhash other *wal-snapshot-retention-pins*)))))))

(rove:deftest imported-scan-principal-hides-frozen-state-and-revocation
    (call-with-imported-principal-test
     (lambda (scan)
       (let ((scope (find scan *wal-retained-slot-scan-scopes*
                          :key #'wal-retained-slot-scan-scope-scan :test #'eq))
             (end (wal-retained-slot-scan-end scan)))
         (rove:ok (rove:signals (slot-value scope 'snapshot) 'error))
         (rove:ok (rove:signals (setf (slot-value scope 'active-p) t) 'error))
         (cursor-unit-append-maintenance)
         (sb-thread:with-mutex (*wal-lock*)
           (setf (wal-retained-slot-scan-end scan) (1+ end))
           (rove:ok (rove:signals
                     (wal-retained-slot-scan-check-scope-under-lock scan) 'error))
           (setf (wal-retained-slot-scan-end scan) end)
           (rove:ok (rove:signals
                     (funcall (wal-retained-slot-scan-scope-dispatch scope) :reset) 'error))
           (rove:ok (rove:signals
                     (wal-retained-slot-scan-check-scope-under-lock scan) 'error)))))))

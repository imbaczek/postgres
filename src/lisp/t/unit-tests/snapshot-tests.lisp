;;;; snapshot-tests.lisp -- snapshot and visibility tests.

(in-package #:pg)

(defun test-visibility ()
  (let ((snap (make-snapshot-struct :xmin 10 :xmax 1000 :xip '() :curcid 5)))
    ;; Committed insert, never deleted -> visible.
    (clog-set 100 +clog-committed+)
    (check (heap-tuple-visible-p 100 0 0 snap nil) "committed insert visible")
    ;; Aborted insert -> invisible.
    (clog-set 101 +clog-aborted+)
    (check (not (heap-tuple-visible-p 101 0 0 snap nil)) "aborted insert hidden")
    ;; Committed insert, committed delete -> invisible.
    (clog-set 102 +clog-committed+)
    (clog-set 103 +clog-committed+)
    (check (not (heap-tuple-visible-p 102 103 0 snap nil))
           "committed delete hides tuple")
    ;; Committed insert, in-progress delete still in our snapshot -> visible.
    (clog-set 104 +clog-committed+)
    (clog-set 105 +clog-in-progress+)
    (let ((snap2 (make-snapshot-struct :xmin 10 :xmax 1000
                                       :xip '(105) :curcid 5)))
      (check (heap-tuple-visible-p 104 105 0 snap2 nil)
             "concurrent delete leaves tuple visible"))
    ;; Own-xid rule: visible only if inserted before the current command.
    (let ((snap3 (make-snapshot-struct :xmin 10 :xmax 1000 :xip '() :curcid 5)))
      (check (heap-tuple-visible-p 200 0 3 snap3 200)
             "own insert visible for later command")
      (check (not (heap-tuple-visible-p 200 0 5 snap3 200))
             "own insert invisible in same command"))))

(defun test-statement-snapshot-acquisition-interrupt-cleanup ()
  (let* ((*backend* (make-backend))
         (*next-xid* 7303)
         (*active-xids* '())
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (original-begin (symbol-function 'begin-statement-snapshot))
         (interrupt-queued nil))
    (unwind-protect
         (progn
           (setf (symbol-function 'begin-statement-snapshot)
                 (lambda (backend curcid registration)
                   (let ((snapshot
                           (funcall original-begin backend curcid registration)))
                     (setf interrupt-queued t)
                     (sb-thread:interrupt-thread
                      sb-thread:*current-thread*
                      (lambda () (error "forced snapshot-acquisition interrupt")))
                     snapshot)))
           (check (handler-case
                      (progn
                        (with-statement-snapshot
                            (snapshot
                             (lambda (registration)
                               (begin-statement-snapshot *backend* 1 registration)))
                          :body-should-not-run)
                        nil)
                    (error () t))
                  "an interrupt after snapshot publication unwinds acquisition")
           (check interrupt-queued
                  "snapshot-acquisition interrupt was injected")
           (check (zerop *active-snapshots*)
                  "snapshot-acquisition interruption releases the active count")
           (check (null (oldest-active-snapshot-xmin))
                  "snapshot-acquisition interruption removes the published xmin"))
      (setf (symbol-function 'begin-statement-snapshot) original-begin))))

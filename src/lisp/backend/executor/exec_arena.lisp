;;;; exec_arena.lisp -- optional, bounded arenas for owned sort-run data.

(in-package #:pg)

(defvar *query-sort-arena-limit-bytes* nil
  "Maximum arena capacity per sort run, or NIL to use the ordinary GC heap.
This does not bound scans, expression evaluation, merge I/O, or other operators.")

(defun query-sort-arena-symbol (name)
  (or (find-symbol name "SB-VM")
      (error "This SBCL does not support executor sort arenas (~a)" name)))

(defun query-sort-arena-function (name)
  (let ((symbol (query-sort-arena-symbol name)))
    (unless (fboundp symbol)
      (error "This SBCL does not support executor sort arenas (~a)" name))
    (symbol-function symbol)))

(defun query-sort-arena-condition-info ()
  "Reject runtimes whose arena exhaustion cannot be identified safely."
  (let ((type (find-symbol "ARENA-EXHAUSTED-ERROR" "SB-KERNEL"))
        (slot (find-symbol "ARENA" "SB-KERNEL")))
    (unless (and type slot (find-class type nil)
                 (let ((probe (make-condition type :arena :probe :request 0)))
                   (and (typep probe 'storage-condition)
                        (slot-exists-p probe slot)
                        (eq (slot-value probe slot) :probe))))
      (error "This SBCL does not expose a recoverable arena exhaustion condition"))
    (values type slot)))

(defun make-query-sort-arena (limit)
  ;; Keep the API behind runtime lookup: SBCL builds without arena support can
  ;; still load and run the server with the prototype disabled.
  (unless (and (integerp limit) (<= 65536 limit most-positive-fixnum))
    (error "sort arena limit must be at least 65536 bytes and fit a fixnum"))
  (query-sort-arena-condition-info)
  (let ((handler (query-sort-arena-symbol "*ARENA-EXHAUSTION-HANDLER*")))
    (unless (and (boundp handler)
                 (eq (symbol-value handler)
                     (query-sort-arena-function "DEFAULT-ARENA-EXTENSION-TEST")))
      (error "Sort arenas require SBCL's default arena extension predicate")))
  (dolist (name '("SWITCH-TO-ARENA" "REWIND-ARENA" "DESTROY-ARENA" "ARENA-P"))
    (query-sort-arena-function name))
  (let ((arena (funcall (query-sort-arena-function "NEW-ARENA")
                        limit 32768 0)))
    (unless (funcall (query-sort-arena-function "ARENA-P") arena)
      (error "This SBCL was built without executor sort arena support"))
    arena))

(defun copy-query-sort-error (condition)
  "Keep an error's diagnostic data valid after destroying its sort arena."
  (if (typep condition 'pg-error)
      (make-condition 'pg-error
                      :sqlstate (copy-query-sort-value (pg-error-sqlstate condition))
                      :severity (copy-query-sort-value (pg-error-severity condition))
                      :message (copy-query-sort-value (pg-error-message condition))
                      :detail (copy-query-sort-value (pg-error-detail condition))
                      :position (copy-query-sort-value (pg-error-position condition))
                      :hint (copy-query-sort-value (pg-error-hint condition)))
      (make-condition 'pg-error :sqlstate "XX000"
                                :message (princ-to-string condition))))

(defun call-with-query-sort-arena (function)
  "Call FUNCTION with an arena (or NIL). No arena object may escape FUNCTION.

Only explicit CALL-IN-QUERY-SORT-ARENA scopes redirect allocation. Like C's
tuplesort tuple context, the arena is reset after writing each run. An arena
budget failure unwinds and releases storage before allocating the SQL error."
  (let ((limit *query-sort-arena-limit-bytes*))
    (if (null limit)
        (funcall function nil)
        (multiple-value-bind (condition-type arena-slot)
            (query-sort-arena-condition-info)
          (let ((tag (gensym "SORT-ARENA-EXHAUSTED-"))
                (exhausted-p nil))
            (multiple-value-prog1
                (sb-sys:without-interrupts
                  (let ((arena (make-query-sort-arena limit)))
                    (unwind-protect
                         (sb-sys:with-local-interrupts
                           (catch tag
                             ;; The runtime hook is global, not dynamically
                             ;; bindable. Its default rejects all extensions of
                             ;; our fixed arena. Catch only this arena's condition;
                             ;; never retain the condition's soon-to-be-freed handle.
                             ;; This handler is removed before the outer destroy:
                             ;; neither its closure nor the condition escapes.
                             (handler-bind
                                 ((storage-condition
                                    (lambda (condition)
                                      (when (and (typep condition condition-type)
                                                 (eq (slot-value condition arena-slot)
                                                     arena))
                                        (setf exhausted-p t)
                                        (throw tag nil)))))
                               (handler-case (funcall function arena)
                                 (error (condition)
                                   (error (copy-query-sort-error condition)))))))
                      (funcall (query-sort-arena-function "DESTROY-ARENA") arena))))
              (when exhausted-p
                (ereport "53200" "sort arena memory limit exceeded (~d bytes)"
                         limit))))))))

(defun call-in-query-sort-arena (arena function)
  "Redirect only FUNCTION's owned allocations; callers must start on the heap."
  (if (null arena)
      (funcall function)
      (sb-sys:without-interrupts
        (funcall (query-sort-arena-function "SWITCH-TO-ARENA") arena)
        (unwind-protect
             (sb-sys:with-local-interrupts (funcall function))
          (funcall (query-sort-arena-function "SWITCH-TO-ARENA") 0)))))

(defun reset-query-sort-arena (arena)
  (when arena
    (sb-sys:without-interrupts
      (funcall (query-sort-arena-function "REWIND-ARENA") arena))))

(defun copy-query-sort-value (value)
  "Copy compound row data into the current allocation region.
Immutable scalar values are borrowed from the ordinary heap. This function
is used only for heap-produced SQL rows and copies of those same rows;
expression evaluation must never run in the arena. Arenas remain visible to
SBCL's GC so borrowed heap scalars remain live and their pointers are updated."
  (cond
    ((consp value)
     ;; Iterate over list spines: a run can contain many thousands of items.
     (let* ((head (cons (copy-query-sort-value (car value)) nil))
            (tail head))
       (loop for rest = (cdr value) then (cdr rest)
             while (consp rest)
             do (setf (cdr tail) (cons (copy-query-sort-value (car rest)) nil)
                      tail (cdr tail))
             finally (setf (cdr tail) (copy-query-sort-value rest)))
       head))
    ((stringp value) (copy-seq value))
    ((vectorp value)
     (let ((copy (make-array (length value)
                             :element-type (array-element-type value))))
       (dotimes (index (length value) copy)
         (setf (aref copy index) (copy-query-sort-value (aref value index))))))
    (t value)))

(in-package #:pg)

(declaim (ftype (function (t (integer 0 64)) boolean)
                %metadata-literal-tree-p %metadata-literal-list-p))

(defun %metadata-literal-tree-p (value depth)
  (declare (optimize (speed 3) (safety 1) (debug 0))
           (type (integer 0 64) depth))
  (and (< depth 64)
       (cond
         ((or (integerp value) (stringp value)) t)
         ((symbolp value) (or (null value) (eq value t) (keywordp value)))
         ((consp value) (%metadata-literal-list-p value (1+ depth)))
         (t nil))))

(defun %metadata-literal-list-p (value depth)
  (declare (optimize (speed 3) (safety 1) (debug 0))
           (type (integer 0 64) depth))
  (let ((slow value) (fast value))
    (loop
      (dotimes (step 2)
        (unless (consp fast)
          (return-from %metadata-literal-list-p (null fast)))
        (unless (%metadata-literal-tree-p (car fast) depth)
          (return-from %metadata-literal-list-p nil))
        (setf fast (cdr fast)))
      (setf slow (cdr slow))
      (when (eq slow fast) (return nil)))))

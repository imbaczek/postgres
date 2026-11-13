(in-package #:pg)

(declaim (optimize (speed 3) (safety 1) (debug 0)))

(defun %logical-sql-quote-into (value output)
  "Quote stable VALUE into preallocated OUTPUT without heap allocation.
OUTPUT is a simple CHARACTER array of length 2 plus VALUE's length and its
apostrophe count. Apostrophes are doubled; backslashes are copied unchanged."
  (declare (type string value)
           (type (simple-array character (*)) output))
  (let ((position 1))
    (declare (type fixnum position))
    (setf (schar output 0) #\')
    (loop for index fixnum below (length value)
          for character = (char value index)
          do (when (char= character #\')
               (setf (schar output position) #\')
               (incf position))
             (setf (schar output position) character)
             (incf position))
    (setf (schar output position) #\')
    output))

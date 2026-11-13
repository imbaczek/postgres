(in-package #:pg)

(declaim (optimize (speed 3) (safety 1) (debug 0)))

(defun %logical-sql-quote-into (value output)
  "Quote stable VALUE into OUTPUT without heap allocation.
The caller supplies a simple CHARACTER array of length 2 plus VALUE's active
length and apostrophe count.  Apostrophes are doubled and backslashes are copied
unchanged for standard-conforming strings."
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

(defun %logical-sql-quote-base-into (value output)
  "Quote stable VALUE into OUTPUT without heap allocation.
The caller supplies a simple base string of length 2 plus VALUE's active
length and apostrophe count.  Apostrophes are doubled and backslashes are copied
unchanged for standard-conforming strings."
  (declare (type base-string value)
           (type simple-base-string output))
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

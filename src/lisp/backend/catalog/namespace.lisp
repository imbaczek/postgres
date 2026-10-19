;;;; namespace.lisp -- name resolution helpers.

(in-package #:pg)

(defun resolve-table-or-error (name)
  (let ((table (lookup-table name)))
    (unless table
      (ereport "42P01" "relation \"~a\" does not exist" name))
    table))

(defun resolve-column-or-error (table name)
  (let ((col (find-column table name)))
    (unless col
      (ereport "42703" "column \"~a\" does not exist" name))
    col))

(defun current-schema-name () "public")
(defun current-user-name () "postgres")

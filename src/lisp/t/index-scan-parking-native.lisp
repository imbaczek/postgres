(in-package #:pg)

(defvar *index-parking-visits* 0)

(defvar *index-parking-paths* nil)

(defvar *index-parking-node* nil)

(defvar *index-parking-leaves* nil)

(defvar *index-parking-push-wrapper* nil)

(defvar *index-parking-detached-leaves* 0)

(defun index-parking-install (root expected)
  (parking-native-install root)
  (let ((original (symbol-function 'eval-setting-function))
        (push-node (symbol-function 'btree-cursor-push-node)))
    (setf *index-parking-visits* 0
          *index-parking-paths* nil
          *index-parking-leaves* nil
          *index-parking-detached-leaves* 0
          *index-parking-push-wrapper*
          (lambda (cursor level block)
            (when (and (zerop level) *backend*
                       (equal "index-parking" (backend-application-name *backend*)))
              (pushnew (cons (btree-equal-cursor-relfilenode cursor) block)
                       *index-parking-leaves* :test #'equal))
            (funcall push-node cursor level block))
          (symbol-function 'btree-cursor-push-node) *index-parking-push-wrapper*
          *parking-native-wrapper*
          (lambda (name arguments)
            (if (and (string= name "current_setting")
                     (equal (first arguments) "pg_lisp_test_index_park"))
                (progn
                  (incf *index-parking-visits*)
                  (if (= 1 *index-parking-visits*)
                      (progn
                        (assert (= 1 (length *select-index-scans*)))
                        (setf *index-parking-node*
                              (btree-equal-cursor-relfilenode
                               (select-index-scan-cursor (first *select-index-scans*))))
                        (parking-native-wait))
                      "visited"))
                (funcall original name arguments)))
          (symbol-function 'eval-setting-function) *parking-native-wrapper*
          *live-operation-parking-test-hook*
          (lambda (point token)
            (declare (ignore token))
            (when (eq point :after-park)
              (assert (notany #'btree-read-lease-active-p *btree-read-leases*))
              (setf *index-parking-detached-leaves*
                    (count *index-parking-node* *index-parking-leaves* :key #'car))
              (assert (>= *index-parking-detached-leaves* 2))
              (assert (= 1 (length *select-index-scans*)))
              (let* ((scan (first *select-index-scans*))
                     (spool (select-index-scan-spool scan))
                     (path (query-row-spool-pathname spool)))
                (assert (null (select-index-scan-cursor scan)))
                (assert (query-row-spool-sealed-p spool))
                (assert (= (1- expected) (query-row-spool-count spool)))
                (assert (probe-file path))
                (setf *index-parking-paths* (list path))
                (with-open-file (out (merge-pathnames "spools" root)
                                     :direction :output :if-exists :error)
                  (write-line (namestring path) out)))))))
  t)

(defun index-parking-index-blocks ()
  (buf-nblocks *index-parking-node*))

(defun index-parking-verify (expected)
  (parking-native-verify)
  (assert (= expected *index-parking-visits*))
  (assert (>= *index-parking-detached-leaves* 2))
  (assert (not (eq *index-parking-push-wrapper* (symbol-function 'btree-cursor-push-node))))
  (assert (every (lambda (path) (not (probe-file path))) *index-parking-paths*))
  (setf *live-operation-parking-test-hook* nil)
  t)

;;;; Test-only instrumentation for executor continuations across promotion.
;;;; The harness installs wrappers in its owned server, captures resources while
;;;; an operation is parked, and verifies cleanup after HUP reloads the functions.
;;;; These wrappers are not permanent production hooks.
(in-package #:pg)

(defvar *resource-parking-visits* 0)

(defvar *resource-parking-after* 0)

(defvar *resource-parking-paths* nil)

(defvar *resource-parking-pages* nil)

(defvar *resource-parking-page-prefix* 0)

(defvar *resource-parking-heap-nodes* nil)

(defvar *resource-parking-arenas* 0)

(defvar *resource-parking-plan-wrapper* nil)

(defvar *resource-parking-heap-wrapper* nil)

(defvar *resource-parking-read-wrapper* nil)

(defvar *resource-parking-arena-wrapper* nil)

(defvar *resource-parking-match-wrapper* nil)

(defvar *resource-parking-match-finished* nil)

(defvar *resource-parking-owner* nil)

(defvar *resource-parking-quota* nil)

(defvar *resource-parking-index-node* nil)

(defvar *resource-parking-lease-wrapper* nil)

(defvar *resource-parking-resumed* nil)

(defvar *resource-parking-repairs* 0)

(defun resource-parking-backend-p ()
  "Return true for the backend selected by the test harness."
  (and *backend* (equal "executor-parking" (backend-application-name *backend*))))

(defun resource-parking-capture (minima)
  "Capture the parked query's temporary resources and check the requested minima.
Verify that index scans are detached and record paths for later cleanup checks."
  (assert (notany #'btree-read-lease-active-p *btree-read-leases*))
  (let* ((entries (when *query-temp-cleanup-owner*
                    (query-temp-cleanup-owner-entries *query-temp-cleanup-owner*)))
         (streams (loop for entry in entries
                        when (query-temp-entry-open-p entry)
                          collect (query-temp-stream-entry-stream entry)))
         (readers (count-if (lambda (stream) (and (input-stream-p stream)
                                                  (not (output-stream-p stream)))) streams))
         (writers (count-if (lambda (stream) (and (output-stream-p stream)
                                                  (not (input-stream-p stream)))) streams))
         (io (count-if (lambda (stream) (and (input-stream-p stream)
                                             (output-stream-p stream))) streams)))
    (setf *resource-parking-owner* *query-temp-cleanup-owner*
          *resource-parking-quota* *query-temp-quota*
          *resource-parking-paths*
          (loop for entry in entries when (query-temp-stream-entry-delete-p entry)
                collect (query-temp-stream-entry-pathname entry))
          *resource-parking-page-prefix* (length *resource-parking-pages*))
    (destructuring-bind (files-needed readers-needed writers-needed io-needed match-phase indexes-needed) minima
      (assert (>= (length *resource-parking-paths*) files-needed))
      (assert (>= readers readers-needed))
      (assert (>= writers writers-needed))
      (assert (>= io io-needed))
      (assert (>= (length *select-index-scans*) indexes-needed))
      (dolist (scan *select-index-scans*)
        (assert (null (select-index-scan-cursor scan)))
        (assert (query-row-spool-sealed-p (select-index-scan-spool scan))))
      (ecase match-phase
        (:any nil)
        (:finished (assert *resource-parking-match-finished*))
        (:unfinished (assert (not *resource-parking-match-finished*)))))
    (assert (every #'probe-file *resource-parking-paths*))
    (with-open-file (output (merge-pathnames "resource-paths" *parking-native-root*)
                            :direction :output :if-exists :error)
      (dolist (path *resource-parking-paths*) (write-line (namestring path) output)))
    (with-open-file (output (merge-pathnames "resource-facts" *parking-native-root*)
                            :direction :output :if-exists :error)
      (format output "files=~d~%readers=~d~%writers=~d~%io=~d~%pages-before=~d~%"
              (length *resource-parking-paths*) readers writers io *resource-parking-page-prefix*))))

(defun resource-parking-install (root after budget arena &rest minima)
  "Install test-only wrappers for the selected backend and parking point.
Record executor resources and replace the setting call used to park the query.
A later HUP reload restores the production functions."
  (parking-native-install root)
  (let ((setting (symbol-function 'eval-setting-function))
        (plan (symbol-function 'execute-query-plan))
        (heap (symbol-function 'heapam-scan))
        (read-page (symbol-function 'buf-read))
        (sort-arena (symbol-function 'call-with-query-sort-arena))
        (finish-match (symbol-function 'query-match-map-finish))
        (leases (symbol-function 'call-with-btree-relation-read-leases)))
    (setf *resource-parking-visits* 0
          *resource-parking-index-node* nil
          *resource-parking-resumed* nil
          *resource-parking-repairs* 0
          *resource-parking-lease-wrapper*
          (lambda (nodes function)
            (when (and nodes *resource-parking-resumed* (resource-parking-backend-p))
              (incf *resource-parking-repairs*))
            (funcall leases nodes function))
          *resource-parking-owner* nil
          *resource-parking-quota* nil
          *resource-parking-after* after
          *resource-parking-paths* nil
          *resource-parking-pages* nil
          *resource-parking-heap-nodes* nil
          *resource-parking-arenas* 0
          *resource-parking-match-finished* nil
          *resource-parking-match-wrapper*
          (lambda (map)
            (prog1 (funcall finish-match map)
              (when (resource-parking-backend-p)
                (assert (query-match-map-finished-p map))
                (setf *resource-parking-match-finished* t))))
          *resource-parking-plan-wrapper*
          (lambda (query relations callback &rest options)
            (if (resource-parking-backend-p)
                (let ((*query-work-memory-bytes* budget)
                      (*query-sort-arena-limit-bytes* arena))
                  (when (and (plusp (sixth minima)) (null *resource-parking-index-node*))
                    (let* ((inner (find "resource_inner_indexed" relations
                                        :key (lambda (relation)
                                               (pg-class-name (getf relation :table)))
                                        :test #'equal))
                           (path (and inner (getf inner :path)))
                           (index (lookup-index "resource_inner_owner_idx"))
                           (parameter (and path (select-scan-path-parameter-expr path))))
                      (assert index)
                      (assert (eq index (select-scan-path-index path)))
                      (assert (eq :col (first parameter)))
                      (assert (= 1 (second parameter)))
                      (setf *resource-parking-index-node* (pg-index-relfilenode index))))
                  (apply plan query relations callback options))
                (apply plan query relations callback options)))
          *resource-parking-heap-wrapper*
          (lambda (node columns snapshot callback)
            (when (resource-parking-backend-p)
              (pushnew node *resource-parking-heap-nodes* :test #'eql))
            (funcall heap node columns snapshot callback))
          *resource-parking-read-wrapper*
          (lambda (node block &optional (fork :main))
            (prog1 (funcall read-page node block fork)
              (when (and (resource-parking-backend-p)
                         (eq fork :main) (member node *resource-parking-heap-nodes* :test #'eql))
                (pushnew (cons node block) *resource-parking-pages* :test #'equal))))
          *resource-parking-arena-wrapper*
          (lambda (function)
            (funcall sort-arena
                     (lambda (region)
                       (when (and region (resource-parking-backend-p))
                         (incf *resource-parking-arenas*))
                       (funcall function region))))
          *parking-native-wrapper*
          (lambda (name arguments)
            (if (and (string= name "current_setting")
                     (equal (first arguments) "pg_lisp_test_resource_park"))
                (progn
                  (incf *resource-parking-visits*)
                  (if (= *resource-parking-after* *resource-parking-visits*)
                      (progn
                        (prog1 (parking-native-wait) (setf *resource-parking-resumed* t)))
                      "visited"))
                (funcall setting name arguments)))
          (symbol-function 'eval-setting-function) *parking-native-wrapper*
          (symbol-function 'execute-query-plan) *resource-parking-plan-wrapper*
          (symbol-function 'heapam-scan) *resource-parking-heap-wrapper*
          (symbol-function 'buf-read) *resource-parking-read-wrapper*
          (symbol-function 'call-with-query-sort-arena) *resource-parking-arena-wrapper*
          (symbol-function 'query-match-map-finish) *resource-parking-match-wrapper*
          (symbol-function 'call-with-btree-relation-read-leases) *resource-parking-lease-wrapper*
          *live-operation-parking-test-hook*
          (lambda (point token)
            (declare (ignore token))
            (when (eq point :after-park) (resource-parking-capture minima)))))
  t)

(defun resource-parking-verify-cleanup ()
  "Check that reload replaced the wrappers and that owned temporary resources were released.
Clear the test hooks after verification."
  (assert (not (eq *resource-parking-plan-wrapper* (symbol-function 'execute-query-plan))))
  (assert (not (eq *resource-parking-heap-wrapper* (symbol-function 'heapam-scan))))
  (assert (not (eq *resource-parking-read-wrapper* (symbol-function 'buf-read))))
  (assert (not (eq *resource-parking-arena-wrapper* (symbol-function 'call-with-query-sort-arena))))
  (assert (not (eq *resource-parking-match-wrapper* (symbol-function 'query-match-map-finish))))
  (assert (not (eq *resource-parking-lease-wrapper* (symbol-function 'call-with-btree-relation-read-leases))))
  (assert (every (lambda (path) (not (probe-file path))) *resource-parking-paths*))
  (assert (zerop (getf (query-temp-pending-cleanup-status) :records)))
  (when *resource-parking-owner*
    (assert (eq :resolved (query-temp-cleanup-owner-state *resource-parking-owner*)))
    (assert (null (query-temp-cleanup-owner-entries *resource-parking-owner*))))
  (when *resource-parking-quota*
    (assert (zerop (query-temp-quota-used-bytes *resource-parking-quota*))))
  (setf *live-operation-parking-test-hook* nil *live-operation-before-open-hook* nil)
  t)

(defun resource-parking-exhaust-quota ()
  "Reduce the parked query's quota to its current usage to exercise failure on resume."
  (assert (= 1 (live-operation-parked-count)))
  (assert *resource-parking-quota*)
  (setf (query-temp-quota-limit *resource-parking-quota*)
        (query-temp-quota-used-bytes *resource-parking-quota*))
  t)

(defun resource-parking-abort-ready-p (disconnected)
  "Check that an aborted query has left admission and, for disconnect, the backend registry."
  (let ((backend (first *parking-native-captured*)))
    (and backend (parking-native-reloaded-p) (zerop (live-operation-parked-count))
         (not (sb-thread:with-mutex (*live-operation-lock*)
                (gethash backend *live-operation-backend-counts*)))
         (or (not disconnected) (null (lookup-backend (backend-pid backend)))))))

(defun resource-parking-verify-aborted (disconnected expected-visits)
  "Verify the abort path's exact park and visit counts and temporary resource cleanup."
  (assert (resource-parking-abort-ready-p disconnected))
  (assert (= 1 *parking-native-calls*))
  (assert (= *resource-parking-visits* expected-visits))
  (resource-parking-verify-cleanup))

(defun resource-parking-index-blocks ()
  "Return the selected inner index's block count for the page-growth check."
  (assert *resource-parking-index-node*)
  (buf-nblocks *resource-parking-index-node*))

(defun resource-parking-verify (visits minimum-pages minimum-arenas minimum-repairs)
  "Verify resumed execution, resource use, lease repairs and cleanup after reload."
  (parking-native-verify)
  (assert (= visits *resource-parking-visits*))
  (when (plusp minimum-pages)
    (assert (>= (length *resource-parking-pages*) minimum-pages))
    (assert (> (length *resource-parking-pages*) *resource-parking-page-prefix*)))
  (assert (>= *resource-parking-arenas* minimum-arenas))
  (assert (>= *resource-parking-repairs* minimum-repairs))
  (resource-parking-verify-cleanup))

(defun resource-parking-install-sql-promotion (root backend-pid)
  (let ((capture *live-operation-parking-test-hook*)
        (open *live-operation-before-open-hook*)
        (setting (symbol-function 'eval-setting-function))
        (wait (symbol-function 'physical-promotion-wait-for-sql)))
    (sql-promotion-test-install root backend-pid :normal)
    (let ((park *live-operation-parking-test-hook*))
      (setf *live-operation-parking-test-hook*
            (lambda (point token)
              (funcall park point token)
              (when (and (eq point :after-park) (resource-parking-backend-p))
                (funcall capture point token)
                (incf *parking-native-calls*)
                (setf *parking-native-captured*
                      (list *backend* (backend-transaction-snapshot *backend*)
                            (backend-virtual-reader-xid *backend*)))
                (parking-native-marker "parked")))
            *live-operation-before-open-hook* open))
    (setf *parking-native-wrapper*
          (lambda (name arguments)
            (if (and (string= name "current_setting")
                     (equal (first arguments) "pg_lisp_test_resource_park"))
                (progn
                  (incf *resource-parking-visits*)
                  (if (= *resource-parking-after* *resource-parking-visits*) "promote" "visited"))
                (funcall setting name arguments)))
          (symbol-function 'eval-setting-function) *parking-native-wrapper*
          (symbol-function 'physical-promotion-wait-for-sql)
          (lambda (deadline)
            (let ((result (funcall wait deadline)))
              (when (and result (resource-parking-backend-p))
                (parking-native-marker "sql-wait-completed")
                (loop repeat 3000
                      until (probe-file (merge-pathnames "release" root))
                      do (check-for-interrupts) (sleep 0.01))
                (assert (probe-file (merge-pathnames "release" root)))
                (setf *resource-parking-resumed* t))
              result))))
  t)

(in-package #:pg-interop-tests)

(defun history-test-row (kind selection &optional (id 1))
  (pg::make-logical-change
   :kind kind :database-oid 10 :relation-oid 201 :relation-name "alpha"
   :columns '((:attnum 1 :name "id" :type :int4 :typmod -1 :notnull t))
   :key-attnums '(1) :publication-selection selection
   :old-values (when (member kind '(:update :delete)) (vector id))
   :new-values (when (member kind '(:insert :update)) (vector id))))

(defun history-test-select (changes names)
  (pg::standard-transaction-changes (pg::make-logical-transaction :changes changes) names))

(deftest historic-publication-bytes-and-corruption
    (let* ((selection '((:relation-oid 7 :publications
                         ((:name "p" :oid 9 :generation 11 :unsupported t)))))
           (buffer (pg::make-payload))
           (expected (octets '(1 0 7 0 0 0 1 0 1 0 0 0 112 9 0 0 0 11 0 0 0 0 0 0 0 1))))
      (pg::logical-write-publication-selection buffer selection)
      (ok (equalp expected buffer))
      (ok (equal selection (pg::logical-read-publication-selection
                            (pg::make-logical-cursor :bytes expected))))
      (dolist (size '(0 1 7 12 24 25))
        (ok (handler-case
                (progn (pg::logical-read-publication-selection
                        (pg::make-logical-cursor :bytes (subseq expected 0 size))) nil)
              (error () t))))
      (setf (aref expected 25) 2)
      (ok (handler-case
              (progn (pg::logical-read-publication-selection
                      (pg::make-logical-cursor :bytes expected)) nil)
            (error () t)))))

(deftest historic-row-codec-preserves-keys-and-unknown-versus-empty
    (let* ((empty '((:relation-oid 201 :publications nil)))
           (change (history-test-row :insert empty))
           (encoded (pg::logical-encode-change change))
           (decoded (pg::logical-decode-change encoded)))
      (ok (= 3 (aref encoded 1)))
      (ok (equal empty (pg::logical-change-publication-selection decoded)))
      (ok (equal '(1) (pg::logical-change-key-attnums decoded)))
      (ok (null (history-test-select (list decoded) '("p"))))
      (setf (pg::logical-change-publication-selection change) :unknown)
      (let* ((legacy (pg::logical-encode-change change))
             (old (pg::logical-decode-change legacy)))
        (ok (= 1 (aref legacy 1)))
        (ok (eq :unknown (pg::logical-change-publication-selection old)))
        (ok (handler-case (progn (history-test-select (list old) '("p")) nil)
              (pg::pg-error (condition) (equal "0A000" (pg::pg-error-sqlstate condition))))))
      (setf (pg::logical-change-publication-selection change)
            '((:relation-oid 999 :publications nil)))
      (ok (handler-case (progn (pg::logical-encode-change change) nil) (error () t)))))

(deftest historic-selection-survives-catalog-edit-and-name-recreation
    (call-with-publication-srf-fixture
     (lambda (database)
       (let* ((table (gethash "alpha" (pg::database-tables database)))
              (publication (gethash "p" (pg::database-publications database)))
              (before (pg::with-write-lock (pg::logical-capture-publications (list table) :insert)))
              (row (history-test-row :insert before)))
         (setf (pg::pg-publication-publish-actions publication) '(:delete)
               (pg::pg-publication-generation publication) 2)
         (let* ((after (pg::with-write-lock (pg::logical-capture-publications (list table) :insert)))
                (later (history-test-row :insert after 2)))
           (ok (equal (list row) (history-test-select (list row later) '("p"))))
           (ok (= 2 (length (history-test-select (list row later) '("p" "q"))))))
         (remhash "p" (pg::database-publications database))
         (ok (equal (list row) (history-test-select (list row) '("p"))))
         (setf (gethash "p" (pg::database-publications database))
               (pg::make-pg-publication :oid 777 :name "p" :generation 1
                                        :tables '((:name "alpha" :table-oid 201))))
         (let ((new (history-test-row :insert
                                      (pg::with-write-lock (pg::logical-capture-publications (list table) :insert)) 3)))
           (ok (= 2 (length (history-test-select (list row new) '("p"))))))
         (ok (null (history-test-select (list row) '("created_later"))))))))

(deftest historic-selection-validates-duplicates-and-filter-eligibility
    (let ((duplicate '((:relation-oid 201 :publications
                        ((:name "p" :oid 9 :generation 1 :unsupported nil)
                         (:name "p" :oid 10 :generation 2 :unsupported nil))))))
      (ok (handler-case (progn (pg::logical-encode-change (history-test-row :insert duplicate)) nil)
            (error () t))))
  (let ((filtered (history-test-row :insert
                                    '((:relation-oid 201 :publications
                                       ((:name "filtered" :oid 9 :generation 1 :unsupported t)))))))
    (ok (null (history-test-select (list filtered) '("other"))))
    (ok (handler-case (progn (history-test-select (list filtered) '("filtered")) nil)
          (pg::pg-error (condition) (equal "0A000" (pg::pg-error-sqlstate condition)))))))

(deftest historic-truncate-selection-is-per-relation
    (call-with-publication-srf-fixture
     (lambda (database)
       (let* ((alpha (gethash "alpha" (pg::database-tables database)))
              (beta (gethash "beta" (pg::database-tables database)))
              (history (pg::with-write-lock (pg::logical-capture-publications (list alpha beta) :truncate)))
              (payload (list :tables '("alpha" "beta") :relation-oids '(201 202)
                             :relations (list (pg::logical-relation-description alpha)
                                              (pg::logical-relation-description beta))))
              (change (pg::make-logical-change
                       :kind :truncate :database-oid 10 :publication-selection history
                       :event-payload (pg::catalog-readable-data-string payload)))
              (encoded (pg::logical-encode-change change))
              (decoded (pg::logical-decode-change encoded)))
         (remhash "q" (pg::database-publications database))
         (ok (= 4 (aref encoded 1)))
         (ok (equal history (pg::logical-change-publication-selection decoded)))
         (ok (equal '(201)
                    (mapcar #'pg::logical-change-relation-oid
                            (cdar (history-test-select (list decoded) '("q"))))))
         (ok (equal '(201 202)
                    (mapcar #'pg::logical-change-relation-oid
                            (cdar (history-test-select (list decoded) '("p" "q"))))))))))

;;;; catalog-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest expression-index-uses-typed-parameter-after-independent-predicate-proof
    (let* ((*query-parameter-types* #(:text :int4))
           (table (make-pg-class :oid 101 :name "events"
                                 :statistics (make-pg-table-statistics :row-count 1200)
                                 :columns (vector
                                           (make-pg-column :name "active" :type :int4 :attnum 1)
                                           (make-pg-column :name "label" :type :text :attnum 2))))
           (definition (first (parse-statements
                               "CREATE UNIQUE INDEX idx ON events (active, lower(label)) WHERE active = 1")))
           (index (make-analyzed-index-object 102 "idx" table definition)))
      (flet ((path (sql)
               (let* ((where (getf (first (parse-statements sql)) :where))
                      (conjuncts (where-conjuncts (analyze-expr where table))))
                 (expression-index-equality-path table index conjuncts))))
        (let* ((path (path "SELECT label FROM events WHERE active = 1 AND lower(label) = $1"))
               (range (and path (select-scan-path-range path))))
          (rove:ok range)
          (when range
            (let ((*query-parameter-values* #("target")))
              (let* ((resolved (resolve-select-index-range table path range #()))
                     (prefix (btree-scan-bound-prefix (select-index-range-lower resolved))))
                (rove:ok (equalp prefix (encode-index-key '(1 "target") '(:int4 :text))))))
            (let ((*query-parameter-values* (vector +null+)))
              (rove:ok (null (resolve-select-index-range table path range #()))))))
        (rove:ok (null (path "SELECT label FROM events WHERE active = $2 AND lower(label) = $1"))))))

(rove:deftest terminated-index-components-retain-their-byte-layout
    (let* ((text (make-string 127 :initial-element #\x))
           (octets (make-array 3 :element-type '(unsigned-byte 8)
                                 :initial-contents '(7 0 8)))
           (key (encode-index-key (list text octets) '(:varchar :bytea)))
           (expected (concatenate 'vector #(3) (make-array 127 :initial-element 120)
                                  #(0 0 15 7 0 255 8 0 0))))
      (rove:ok (equalp key expected))
      (rove:ok (equalp (encode-index-key (list text) '(:varchar)) (subseq expected 0 130)))
      (rove:ok (key< (encode-index-key '("x") '(:varchar))
                     (encode-index-key (list text) '(:varchar)))))
  (let* ((uuid (sql-uuid-input "01234567-89ab-cdef-0123-456789abcdef"))
         (composite (encode-index-key (list uuid 10) '(:uuid :int4))))
    (rove:ok (equalp (encode-index-key (list uuid) '(:uuid)) (subseq composite 0 17)))))

(rove:deftest fixed-index-components-retain-their-byte-order
    (dolist (example '((:int2 -2 #(5 127 254))
                       (:int4 0 #(1 128 0 0 0))
                       (:int8 -1 #(2 127 255 255 255 255 255 255 255))
                       (:date 1 #(8 128 0 0 1))
                       (:bool nil #(19 0))
                       (:float4 1.0f0 #(6 191 128 0 0))))
      (destructuring-bind (type datum expected) example
        (let ((singleton (encode-index-key (list datum) (list type)))
              (composite (encode-index-key (list datum t) (list type :bool))))
          (rove:ok (equalp singleton expected))
          (rove:ok (equalp singleton (subseq composite 0 (length singleton))))))))

(rove:deftest test-expression-index-catalog-roundtrip
    (let* ((table (make-pg-class :oid 101 :name "events"
                                 :columns (vector
                                           (make-pg-column :name "id" :type :int4 :attnum 1)
                                           (make-pg-column :name "label" :type :text :attnum 2)
                                           (make-pg-column :name "active" :type :bool :attnum 3))))
           (ast (first (parse-statements
                        "CREATE UNIQUE INDEX idx ON events ((id::oid), lower(label), id) WHERE active")))
           (index (make-analyzed-index-object 102 "idx" table ast))
           (decoded (index-from-sexp (index-to-sexp index)))
           (snapshot (catalog-mutation-copy-index index))
           (legacy (make-index-object 103 "legacy" table '(1))))
      (rove:ok (equalp (pg-index-expression-keys decoded) (pg-index-expression-keys index)))
      (rove:ok (equalp (pg-index-predicate decoded) (pg-index-predicate index)))
      (rove:ok (equal '(1 2 3) (pg-index-referenced-columns decoded)))
      (rove:ok (pg-index-unique-p decoded))
      (let ((boolean-index
              (make-analyzed-index-object nil "bool_idx" table
                                          (first (parse-statements
                                                  "CREATE INDEX bool_idx ON events ((id > 0))")))))
        (rove:ok (equal '(:bool) (index-types boolean-index table)))
        (rove:ok (equal '(nil) (evaluate-index-key boolean-index #(-1 "name" nil)))))
      (rove:ok (equal '(:oid :text :int4) (index-types decoded table)))
      (rove:ok (not (eq (pg-index-predicate snapshot) (pg-index-predicate index))))
      (rove:ok (not (eq (first (pg-index-expression-keys snapshot))
                        (first (pg-index-expression-keys index)))))
      (rove:ok (catalog-index-owner-compatible-p decoded table))
      (rove:ok (not (getf (index-to-sexp legacy) :expression-keys)))
      (rove:ok (equal '(7 "name" 7) (evaluate-index-key decoded #(7 "NAME" t))))
      (rove:ok (index-tuple-qualifies-p decoded #(7 "NAME" t)))
      (let* ((row #(7 "NAME" t))
             (tuple (make-htup :values row))
             (row-keys (prepare-expression-index-row-keys (list decoded) row))
             (expected (encode-index-key '(7 "name" 7) '(:oid :text :int4))))
        (rove:ok (equal '(7 "name" 7) (index-row-key-values decoded row row-keys)))
        (rove:ok (equalp expected (index-build-key decoded table tuple nil))))
      (let ((row (vector 7 "NAME" +null+)))
        (rove:ok (null (prepare-expression-index-row-keys (list decoded) row)))
        (rove:ok (null (index-build-key decoded table (make-htup :values row) nil))))
      (rove:ok (not (index-tuple-qualifies-p decoded #(7 "NAME" nil))))
      (rove:ok (not (index-tuple-qualifies-p decoded (vector 7 "NAME" +null+))))))

(rove:deftest test-analyze-index-definition-immutability
    (let ((table (make-pg-class :oid 100 :name "events"
                                :columns (vector
                                          (make-pg-column :name "tenant" :type :int4 :attnum 1)
                                          (make-pg-column :name "created_at" :type :timestamp :attnum 2)
                                          (make-pg-column :name "instant" :type :timestamptz :attnum 3)
                                          (make-pg-column :name "label" :type :text :attnum 4)
                                          (make-pg-column :name "active" :type :bool :attnum 5)
                                          (make-pg-column :name "measurement" :type :float8 :attnum 6)))))
      (flet ((analyze (sql) (analyze-index-definition (first (parse-statements sql)) table)))
        (multiple-value-bind (keys predicate dependencies)
            (analyze "CREATE INDEX idx ON events (tenant, (created_at::date), lower(label)) WHERE active AND tenant > 1")
          (rove:ok (equal dependencies '(1 2 4 5)))
          (rove:ok (eql 1 (first keys)))
          (rove:ok (eq :date (expr-type (second keys))))
          (rove:ok (eq :text (expr-type (third keys))))
          (rove:ok (eq :bool (expr-type predicate))))
        (let ((keys (analyze "CREATE INDEX idx ON events ((tenant::oid), (tenant::bytea))")))
          (rove:ok (equal '(:oid :bytea) (mapcar #'expr-type keys))))
        (dolist (example '(("CREATE INDEX idx ON events ((instant::date))" . "42P17")
                           ("CREATE INDEX idx ON events (random())" . "42P17")
                           ("CREATE INDEX idx ON events ((measurement::text))" . "42P17")
                           ("CREATE INDEX idx ON events (tenant) WHERE current_database() = 'postgres'" . "42P17")
                           ("CREATE INDEX idx ON events (tenant) WHERE tenant" . "42804")
                           ("CREATE INDEX idx ON events ((other.tenant))" . "42P01")
                           ("CREATE INDEX idx ON events (((SELECT 1)))" . "0A000")))
          (rove:ok (equal (cdr example)
                          (handler-case (progn (analyze (car example)) nil)
                            (pg-error (error) (pg-error-sqlstate error)))))))))

(rove:deftest test-create-index-expression-and-predicate-syntax
    (let* ((ast (first (parse-statements
                        "CREATE INDEX idx ON events (tenant, (created_at::date), pg_catalog.lower(label)) WHERE active")))
           (keys (getf ast :index-keys)))
      (rove:ok (null (getf ast :columns)))
      (rove:ok (equal (first keys) "tenant"))
      (rove:ok (eq (first (second keys)) :cast))
      (rove:ok (consp (third keys)))
      (rove:ok (equal (getf ast :predicate) '(:column "active"))))
  (let ((ast (first (parse-statements "CREATE INDEX idx ON events (tenant, label)"))))
    (rove:ok (equal (getf ast :columns) '("tenant" "label")))
    (rove:ok (not (getf ast :index-keys))))
  (let ((ast (first (parse-statements "CREATE INDEX idx ON events (tenant) WHERE active"))))
    (rove:ok (equal (getf ast :columns) '("tenant")))
    (rove:ok (equal (getf ast :predicate) '(:column "active")))))

(rove:deftest test-staged-index-column-dependencies
    (let* ((database (make-database :oid 5 :name "dependency"))
           (transaction (make-catalog-transaction database))
           (table (make-pg-class :oid 11 :name "rel" :relfilenode 81
                                 :columns (vector (make-pg-column :name "a" :attnum 1)
                                                  (make-pg-column :name "b" :attnum 2))))
           (index (make-pg-index :oid 12 :name "rel_ix" :relfilenode 82
                                 :table-oid 11 :columns '(2))))
      (catalog-transaction-stage transaction :table "rel" table)
      (rove:ok (rove:signals
                (catalog-transaction-stage transaction :index "bad"
                                           (make-pg-index :table-oid 11 :columns '(3)))
                'pg-error))
      (catalog-transaction-stage transaction :index "rel_ix" index)
      (catalog-transaction-validate transaction database)
      (let ((replacement (copy-pg-class table)))
        (setf (pg-class-columns replacement) (subseq (pg-class-columns table) 0 1))
        (catalog-transaction-stage transaction :table "rel" replacement)
        (rove:ok (rove:signals (catalog-transaction-validate transaction database) 'pg-error)))))

(rove:deftest logical-schema-contract-keeps-explicit-database-view
    (let* ((database (make-database :oid 5 :name "overlay"))
           (column (make-pg-column :name "id" :type :int4))
           (table (make-pg-class :oid 11 :name "items" :relfilenode 81
                                 :columns (vector column) :indexes '(14)))
           (index (make-index-object 14 "old_ix" table '(1)))
           (transaction (make-catalog-transaction database))
           (*catalog* (make-catalog))
           (*backend* (make-backend :database "overlay" :catalog-transaction transaction)))
      (setf (gethash "overlay" (catalog-databases *catalog*)) database
            (gethash "items" (database-tables database)) table
            (gethash "old_ix" (database-indexes database)) index
            (pg-index-logical-visible-p index) t)
      (catalog-transaction-stage transaction :index "old_ix" nil)
      (with-write-catalog-locks
        (let* ((view (catalog-transaction-prepare-database transaction database))
               (committed (logical-current-schema-contract-under-write-lock database))
               (staged (logical-current-schema-contract-under-write-lock view)))
          (rove:ok (null (table-indexes table)))
          (rove:ok (equal '("old_ix") (mapcar (lambda (entry) (getf entry :name))
                                              (getf committed :indexes))))
          (rove:ok (null (getf staged :indexes)))
          (rove:ok (equal (getf committed :tables) (getf staged :tables)))))))

(rove:deftest test-catalog-transaction-private-names-and-conflict
    (let* ((database (make-database :oid 5 :name "overlay"))
           (old (make-pg-class :oid 11 :name "rel" :relfilenode 81 :columns #() :indexes '(14)))
           (new (make-pg-class :oid 12 :name "rel" :relfilenode 82 :columns #()))
           (unrelated (make-pg-class :oid 13 :name "other" :relfilenode 83 :columns #()))
           (writer (make-catalog-transaction database))
           (observer (make-catalog-transaction database))
           (live (database-tables database)))
      (setf (gethash "rel" live) old
            (gethash "old_ix" (database-indexes database))
            (make-pg-index :oid 14 :name "old_ix" :relfilenode 84 :table-oid 11))
      (catalog-transaction-stage writer :table "rel" nil)
      (rove:ok (null (catalog-transaction-lookup writer :table "rel")))
      (rove:ok (null (catalog-transaction-lookup writer :index "old_ix")))
      (rove:ok (catalog-transaction-lookup observer :index "old_ix"))
      (rove:ok (eq old (catalog-transaction-lookup observer :table "rel")))
      (catalog-transaction-stage writer :table "rel" new)
      (rove:ok (eq new (catalog-transaction-lookup writer :table "rel")))
      (let ((*catalog* (make-catalog))
            (*backend* (make-backend :database "overlay" :catalog-transaction writer)))
        (setf (gethash "overlay" (catalog-databases *catalog*)) database)
        (rove:ok (eq new (lookup-table "public.rel")))
        (rove:ok (eq new (find-table-by-oid 12)))
        (rove:ok (null (find-table-by-oid 11)))
        (let ((*backend* (make-backend :database "overlay" :catalog-transaction observer)))
          (rove:ok (eq old (lookup-table "rel")))
          (rove:ok (eq old (find-table-by-oid 11)))))
      (rove:ok (eq old (gethash "rel" live)))
      (rove:ok (= 11 (catalog-object-address-object-oid (catalog-relation-address database old))))
      (let* ((address (catalog-relation-address database new 2))
             (decoded (catalog-object-address-from-sexp (catalog-object-address-to-sexp address))))
        (rove:ok (= 12 (catalog-object-address-object-oid address)))
        (rove:ok (equalp address decoded)))
      (setf (gethash "other" live) unrelated)
      (catalog-transaction-validate writer database)
      (let ((view (catalog-transaction-prepare-database writer database)))
        (rove:ok (zerop (hash-table-count (database-indexes view))))
        (rove:ok (equal '(81 84) (catalog-transaction-deferred-deletes writer view))))
      (let ((merged (catalog-transaction-merged-name-map writer :table)))
        (rove:ok (eq unrelated (gethash "other" merged)))
        (rove:ok (eq new (gethash "rel" merged)))
        (rove:ok (eq old (gethash "rel" live))))
      (setf (pg-class-indexes old) (list 91))
      (rove:ok (equal "40001"
                      (handler-case (catalog-transaction-validate writer database)
                        (pg-error (condition) (pg-error-sqlstate condition)))))))

(rove:deftest varchar-point-index-accepts-typed-bindings
    (let* ((database (make-database :oid 5 :name "point_probe"))
           (column (make-pg-column :name "k" :type :varchar :typmod 131 :attnum 1))
           (table (make-pg-class :oid 11 :name "keys" :relfilenode 81 :columns (vector column)
                                 :statistics (make-pg-table-statistics :row-count 2000)))
           (index (make-pg-index :oid 12 :name "probe" :relfilenode 82
                                 :table-oid 11 :columns '(1) :unique-p t :valid-p t))
           (partial (make-pg-index :oid 13 :name "partial_probe" :relfilenode 83
                                   :table-oid 11 :columns '(1) :valid-p t :predicate '(:const nil :type :bool)))
           (transaction (make-catalog-transaction database))
           (*catalog* (make-catalog))
           (*backend* (make-backend :database "point_probe" :catalog-transaction transaction))
           (key (make-string 127 :initial-element #\a)))
      (setf (gethash "point_probe" (catalog-databases *catalog*)) database
            (gethash "keys" (database-tables database)) table)
      (catalog-transaction-stage transaction :index "probe" index)
      (catalog-transaction-stage transaction :index "partial_probe" partial)
      (dolist (type '(:varchar :text))
        (let ((*query-parameter-types* (vector type)))
          (dolist (sql '("SELECT k FROM keys WHERE k = $1" "SELECT k FROM keys WHERE $1 = k"))
            (let* ((where (analyze-expr (getf (first (parse-statements sql)) :where) table))
                   (paths (index-paths-for-equality table where))
                   (path (first paths)))
              (rove:ok (equal (mapcar #'select-scan-path-index paths) (list index)))
              (when path
                (let ((*query-parameter-values* (vector key)))
                  (rove:ok (equalp (select-scan-index-key table path #())
                                   (encode-index-key (list key) '(:varchar)))))
                (let ((*query-parameter-values* (vector +null+)))
                  (rove:ok (null (select-scan-index-key table path #())))))))))))

(rove:deftest test-catalog-transaction-validates-index-owner-and-namespace
    (let* ((database (make-database :oid 5 :name "overlay"))
           (table (make-pg-class :oid 11 :name "heap" :relfilenode 81
                                 :columns (vector (make-pg-column :name "key" :type :int4 :attnum 1))))
           (index (make-pg-index :oid 12 :name "probe" :relfilenode 82 :table-oid 11 :columns '(1)))
           (writer (make-catalog-transaction database))
           (observer (make-catalog-transaction database)))
      (setf (gethash "heap" (database-tables database)) table)
      (catalog-transaction-stage writer :index "probe" index)
      (let ((*catalog* (make-catalog))
            (*backend* (make-backend :database "overlay" :catalog-transaction writer)))
        (setf (gethash "overlay" (catalog-databases *catalog*)) database)
        (rove:ok (backend-own-wal-effects-p *backend*))
        (rove:ok (equal "0A000"
                        (handler-case (let ((*wal-ready* nil)) (commit-transaction))
                          (pg-error (condition) (pg-error-sqlstate condition))))
                 "catalog changes require native WAL")
        (rove:ok (eq index (lookup-index "public.probe")))
        (rove:ok (eq index (find-index-by-oid 12)))
        (rove:ok (equal (list index) (table-indexes table)))
        (rove:ok (null (table-indexes table database)))
        (rove:ok (find 12 (sql-class-rows) :key (lambda (row) (aref row 0))))
        (rove:ok (find 12 (sql-attribute-rows) :key (lambda (row) (aref row 0))))
        (rove:ok (find 12 (sql-index-rows) :key (lambda (row) (aref row 0))))
        (rove:ok (null (find-index-by-oid 12 database)))
        (let ((*backend* (make-backend :database "overlay" :catalog-transaction observer)))
          (rove:ok (null (lookup-index "probe")))
          (rove:ok (null (find-index-by-oid 12)))
          (rove:ok (null (table-indexes table)))
          (rove:ok (not (find 12 (sql-index-rows) :key (lambda (row) (aref row 0)))))))
      (let ((*catalog* (make-catalog :cluster-identity "overlay-test")))
        (setf (gethash "overlay" (catalog-databases *catalog*)) database)
        (let* ((payload (catalog-transaction-prepare-wal writer database))
               (bytes (nth-value 1 (wal-decode-catalog-payload payload)))
               (decoded (catalog-from-sexp (read-from-string (bytes->string bytes))))
               (decoded-db (gethash "overlay" (catalog-databases decoded)))
               (decoded-table (gethash "heap" (database-tables decoded-db))))
          (rove:ok (equal '(12) (pg-class-indexes decoded-table)))
          (rove:ok (= 12 (pg-index-oid (gethash "probe" (database-indexes decoded-db)))))
          (rove:ok (null (pg-class-indexes table))
                   "preparing disk metadata does not edit the live table descriptor")))
      (flet ((conflict-p ()
               (equal "40001"
                      (handler-case (catalog-transaction-validate writer database)
                        (pg-error (condition) (pg-error-sqlstate condition))))))
        (setf (gethash "probe" (database-sequences database)) (make-pg-sequence :name "probe"))
        (rove:ok (conflict-p) "a concurrent sequence can claim the same relation name")
        (remhash "probe" (database-sequences database))
        (setf (pg-class-relfilenode table) 99)
        (rove:ok (conflict-p) "index publication validates the owning storage generation"))))

(rove:deftest test-sql-prng-sequence
    (let ((*sql-prng-state* nil))
      (sql-setseed 0.5d0)
      (rove:ok (= (sql-random-float) 0.9851677175347999d0))
      (rove:ok (= (sql-random-integer -100 100) -67))
      (let ((before (copy-seq *sql-prng-state*)))
        (rove:ok (= (sql-prng-range 4 4) 4))
        (rove:ok (equalp before *sql-prng-state*)))
      (sql-setseed 0.5d0)
      (rove:ok (= (sql-random-float) 0.9851677175347999d0))))

(rove:deftest test-key-order
    (flet ((k (v ty) (encode-index-key (list v) (list ty))))
      (rove:ok (minusp (key-compare (k -1 :int4) (k 0 :int4))) "int4 order")
      (rove:ok (minusp (key-compare (k -2147483648 :int4) (k 2147483647 :int4)))
               "int4 extremes")
      (rove:ok (minusp (key-compare (k -1 :int8) (k 0 :int8))) "int8 order")
      (rove:ok (minusp (key-compare (k +null+ :int4) (k -2147483648 :int4)))
               "null sorts first")
      (rove:ok (minusp (key-compare (k "abc" :varchar) (k "abd" :varchar)))
               "varchar order")
      (rove:ok (plusp (key-compare (k "b" :varchar) (k "aa" :varchar)))
               "varchar keys follow lexical order")))

(rove:deftest heap-header-bitmap-depends-on-actual-nulls
    (let* ((columns (coerce (loop for i from 1 to 9
                                  collect (make-pg-column :type :int4 :typmod -1 :attnum i)) 'vector))
           (values (make-array 9 :initial-element 1))
           (without-null (form-heap-tuple values columns 1 0 0 0 1)))
      (rove:ok (= 24 (aref without-null +heap-tuple-hoff-off+)))
      (rove:ok (equalp values (nth-value 0 (deform-heap-tuple without-null columns))))
      (setf (aref values 0) +null+)
      (let ((with-null (form-heap-tuple values columns 1 0 0 0 1)))
        (rove:ok (= 32 (aref with-null +heap-tuple-hoff-off+)))
        (rove:ok (equalp values (nth-value 0 (deform-heap-tuple with-null columns))))
        (rove:ok (rove:signals
                  (deform-heap-tuple with-null columns :limit (1- (length with-null)))
                  'pg-error)
                 "a fixed-width attribute cannot read past the tuple bound"))))

(rove:deftest test-btree-split-balances-bytes
    (flet ((key (n) (make-array n :element-type '(unsigned-byte 8) :initial-element 1)))
      (dolist (internal-p '(nil t))
        (let* ((small (loop repeat 100 collect
                                       (if internal-p (cons (key 3) 1) (list (key 3) 1 1))))
               (large (loop repeat 3 collect
                                     (if internal-p (cons (key 2600) 1) (list (key 2600) 1 1))))
               (entries (append small large)))
          (multiple-value-bind (left right) (btree-split-entries entries internal-p)
            (rove:ok (and left right) "both split pages have entries")
            (rove:ok (btree-page-build left internal-p) "left page fits")
            (rove:ok (btree-page-build right internal-p) "right page fits")
            (rove:ok (= 103 (+ (length left) (length right))) "all entries retained"))))
      (rove:ok (handler-case
                   (progn (btree-split-entries (list (list (key 9000) 1 1)
                                                     (list (key 2) 1 2)) nil)
                          nil)
                 (pg-error (e) (string= (pg-error-sqlstate e) "54000")))
               "an unsplittable entry reports 54000")))

(rove:deftest btree-equality-children-preserve-the-duplicate-boundary
    (flet ((key (text) (encode-index-key (list text) '(:varchar))))
      (let* ((entries (loop for word in '("c" "b" "b" "b" "d" "d" "z")
                            for block from 40 collect (cons (key word) block)))
             (page (btree-page-build entries t)))
        ;; The first separator can be stale after an insert below the old minimum.
        (dolist (probe '("" "a" "b" "c" "d" "e" "z" "zz"))
          (multiple-value-bind (first last) (btree-equal-child-range entries (key probe))
            (let* ((expected (map 'vector #'cdr (subseq entries first (1+ last))))
                   (scalar-or-vector (btree-equal-page-children page (key probe) t)))
              (rove:ok (equalp expected (btree-equal-page-children page (key probe))))
              (rove:ok (equalp (if (= (length expected) 1) (aref expected 0) expected)
                               scalar-or-vector)))))
        (page-set-lp page 3 0 +lp-unused+ 0)
        ;; The unused slot forces a scan through an equal run with a greater suffix.
        (rove:ok (equalp #(40 41 43) (btree-equal-page-children page (key "b"))))
        (rove:ok (equalp #(40 41 43) (btree-equal-page-children page (key "b") t))))))

(rove:deftest btree-equality-leaf-boundaries
    (flet ((key (text) (encode-index-key (list text) '(:varchar))))
      (let* ((entries (loop for word in '("b" "b" "c" "d" "d" "d" "f" "z")
                            for slot from 1 collect (list (key word) 25 slot)))
             (page (btree-page-build entries nil))
             (sparse-page (copy-seq page)))
        (page-set-lp sparse-page 5 0 +lp-unused+ 0)
        (dolist (input (list page sparse-page))
          (dolist (probe '("" "b" "c" "d" "e" "f" "z" "zz"))
            (let ((key (key probe)))
              (rove:ok (equal (btree-equal-page-leaf-tids input key)
                              (loop for slot from 1 to (page-lp-count input)
                                    for tid = (btree-scan-leaf-tid input slot nil nil key)
                                    when tid collect tid)))))))))

(rove:deftest btree-long-key-order-uses-octets-and-length
    (let* ((key (make-array 65 :element-type '(unsigned-byte 8) :initial-element 128))
           (page (make-array 68 :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace page key :start1 3)
      (rove:ok (zerop (compare-octet-vector-to-span key page 3 65)))
      (rove:ok (= 1 (compare-octet-vector-to-span key page 3 64)))
      (rove:ok (= -1 (compare-octet-vector-to-span (subseq key 0 64) page 3 65)))
      (dolist (offset '(0 7 8 31 32 33 63 64))
        (dolist (other-byte '(127 129))
          (setf (aref page (+ offset 3)) other-byte)
          (rove:ok (= (signum (- 128 other-byte))
                      (compare-octet-vector-to-span key page 3 65)))
          (setf (aref page (+ offset 3)) 128)))))

(rove:deftest ordered-plain-btree-leaf-insert
    (flet ((key (&rest bytes)
             (make-array (length bytes) :element-type '(unsigned-byte 8)
                                        :initial-contents bytes)))
      (let* ((low (key 1))
             (middle (key 1 2))
             (high (key 2))
             (entries (list (list low 1 1) (list middle 3 4) (list high 5 1)))
             (page (btree-page-build entries nil)))
        (dolist (entry (list (list high 6 1) (list low 1 0) (list middle 2 5)
                             (list middle 3 2) (list (key 0) 1 1)))
          (rove:ok (apply #'btree-insert-plain-leaf page entry)))
        (rove:ok (equalp (btree-page-entries page nil)
                         (list (list (key 0) 1 1) (list low 1 0) (list low 1 1)
                               (list middle 2 5) (list middle 3 2) (list middle 3 4)
                               (list high 5 1) (list high 6 1)))
                 "line pointer order uses key bytes and both TID fields")
        (let ((before (copy-seq page))
              (oversize (make-array +blcksz+ :element-type '(unsigned-byte 8)
                                             :initial-element 1)))
          (rove:ok (not (btree-insert-plain-leaf page oversize 1 1)))
          (rove:ok (equalp page before) "the split fallback sees the original page"))
        (let* ((segmented (make-btree-storage-key :logical low :payload low))
               (segmented-page (btree-page-build (list (list segmented 1 1)) nil))
               (unused-page (copy-seq page))
               (bad-header (copy-seq page)))
          (page-set-lp unused-page 1 0 +lp-unused+ 0)
          (setf (page-lower bad-header) (1+ (page-lower bad-header)))
          (dolist (input (list segmented-page unused-page bad-header))
            (let ((before (copy-seq input)))
              (rove:ok (not (btree-insert-plain-leaf input low 1 5)))
              (rove:ok (equalp input before) "fallback leaves the source page intact")))))))

(rove:deftest compressed-reuse-requires-exact-claimed-bytes
    (let* ((column (make-pg-column :name "v" :type :text :typmod -1 :attnum 1))
           (compressed (make-heap-compressed-varlena :payload #(1 2)))
           (tuple (make-htup :values (vector "abc") :physical-compressed (vector compressed)))
           (assignment (cons column (list :assignment (list :col 1 :type :text :typmod -1) :type :text :typmod -1))))
      (rove:ok (eq compressed (aref (unchanged-tuple-compressed-values tuple (list assignment) (vector "abc")) 0)))
      (rove:ok (null (aref (unchanged-tuple-compressed-values tuple (list assignment) (vector "ABC")) 0)))
      (let ((conflict (cons column (list :expr (cdr assignment)))))
        (rove:ok (eq compressed (aref (unchanged-tuple-compressed-values tuple (list conflict) (vector "abc")) 0))))))

(rove:deftest query-origin-follows-selected-branch
    (let* ((origin (make-datum-origin :compressed (make-heap-compressed-varlena :payload #(1 2))))
           (origins (vector origin))
           (environment (vector "abc"))
           (column (list :col 1 :type :text :typmod -1))
           (empty (list :const "" :type :text :typmod -1))
           (null (list :const +null+ :type :text :typmod -1)))
      (multiple-value-bind (value selected)
          (eval-expr-with-origin (list :coalesce (list null column) :type :text :typmod -1)
                                 environment origins)
        (rove:ok (string= value "abc"))
        (rove:ok (eq selected origin)))
      (multiple-value-bind (value selected)
          (eval-expr-with-origin (list :case (list (list (list :bool t) column)) empty :type :text :typmod -1)
                                 environment origins)
        (rove:ok (string= value "abc"))
        (rove:ok (eq selected origin)))
      (multiple-value-bind (value selected)
          (eval-expr-with-origin (list :concat column empty :type :text :typmod -1)
                                 environment origins)
        (rove:ok (string= value "abc"))
        (rove:ok (null selected)))))

(rove:deftest query-origin-assignment-typmods
    (flet ((expression (type typmod) (list :col 1 :type type :typmod typmod))
           (column (type typmod) (make-pg-column :type type :typmod typmod)))
      (rove:ok (datum-origin-assignment-compatible-p (expression :text -1) (column :varchar -1)))
      (rove:ok (not (datum-origin-assignment-compatible-p (expression :text -1) (column :varchar 30004))))
      (rove:ok (datum-origin-assignment-compatible-p (expression :varchar 30004) (column :varchar 40004)))
      (rove:ok (not (datum-origin-assignment-compatible-p (expression :varchar 30004) (column :varchar 20004))))
      (let ((old (+ 4 (ash 20 16) 2))
            (wider (+ 4 (ash 30 16) 2))
            (other-scale (+ 4 (ash 30 16) 3)))
        (rove:ok (datum-origin-assignment-compatible-p (expression :numeric old) (column :numeric wider)))
        (rove:ok (not (datum-origin-assignment-compatible-p (expression :numeric old) (column :numeric other-scale)))))))

(rove:deftest sorted-origin-payload-is-owned
    (let* ((bytes (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (origin (make-datum-origin :external-p t :compressed (make-heap-compressed-varlena :payload bytes)))
           (packed (datum-origins-for-spill (vector origin nil)))
           (restored (datum-origins-from-spill packed))
           (copy (datum-origin-compressed (aref restored 0))))
      (setf (aref bytes 0) 9)
      (rove:ok (= 1 (aref (heap-compressed-varlena-payload copy) 0)))
      (rove:ok (datum-origin-external-p (aref restored 0)))
      (rove:ok (null (aref restored 1)))))

(rove:deftest external-origin-descriptor-is-owned
    (let* ((descriptor (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(4 5 6)))
           (origin (make-datum-origin :external-p t :descriptor descriptor))
           (restored (datum-origins-from-spill (datum-origins-for-spill (vector origin)))))
      (setf (aref descriptor 0) 9)
      (rove:ok (null (datum-origin-compressed (aref restored 0))))
      (rove:ok (datum-origin-external-p (aref restored 0)))
      (rove:ok (= 4 (aref (datum-origin-descriptor (aref restored 0)) 0)))))

(rove:deftest replay-origin-readers-own-payloads
    (let ((*query-temp-directory* (uiop:temporary-directory)))
      (with-query-temp-files
        (dolist (limit '(1 100000))
          (let* ((*datum-origin-capture* t)
                 (bytes (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
                 (origin (make-datum-origin :external-p t
                                            :compressed (make-heap-compressed-varlena :payload bytes)))
                 (raw-origin (make-datum-origin :external-p t :descriptor (copy-seq bytes)))
                 (done nil)
                 (producer (make-sql-iterator
                            (lambda ()
                              (unless done
                                (setf done t)
                                (values (vector "row") t (vector origin raw-origin))))))
                 (store (make-query-replay-store producer limit))
                 (first-reader (make-query-replay-reader store))
                 (second-reader (make-query-replay-reader store)))
            (unwind-protect
                 (let ((*datum-origin-capture* nil))
                   (multiple-value-bind (row present origins) (sql-iterator-next first-reader)
                     (rove:ok present)
                     (rove:ok (equal "row" (aref row 0)))
                     (setf (aref (heap-compressed-varlena-payload
                                  (datum-origin-compressed (aref origins 0))) 0) 9)
                     (setf (aref (datum-origin-descriptor (aref origins 1)) 0) 9))
                   (multiple-value-bind (row present origins) (sql-iterator-next second-reader)
                     (declare (ignore row))
                     (rove:ok present)
                     (rove:ok (datum-origin-external-p (aref origins 0)))
                     (rove:ok (= 1 (aref (heap-compressed-varlena-payload
                                          (datum-origin-compressed (aref origins 0))) 0)))
                     (rove:ok (= 1 (aref bytes 0)))
                     (rove:ok (null (datum-origin-compressed (aref origins 1))))
                     (rove:ok (= 1 (aref (datum-origin-descriptor (aref origins 1)) 0)))))
              (close-query-replay-store store)))))))

(rove:deftest replay-candidate-rejection-advances-without-copy
    (let* ((store (make-query-replay-store nil))
           (rejected (vector "rejected"))
           (accepted (vector (copy-seq "accepted")))
           (inspected nil))
      (unwind-protect
           (progn
             (append-query-replay-row store rejected)
             (append-query-replay-row store accepted)
             (let* ((records (query-replay-store-memory-rows store))
                    (borrowed-rejected (first (aref records 0)))
                    (borrowed-accepted (first (aref records 1)))
                    (screened (make-query-replay-reader
                               store :candidate-p (lambda (row)
                                                    (cond ((eq row borrowed-accepted)
                                                           (push :accepted inspected)
                                                           t)
                                                          ((eq row borrowed-rejected)
                                                           (push :rejected inspected)
                                                           nil)))))
                    (plain (make-query-replay-reader store)))
               (rove:ok (equal (multiple-value-list (sql-iterator-next screened)) '(nil t)))
               (rove:ok (equal inspected '(:rejected)))
               (multiple-value-bind (row present) (sql-iterator-next screened)
                 (rove:ok present)
                 (rove:ok (equal inspected '(:accepted :rejected)))
                 (rove:ok (equalp row accepted))
                 (setf (char (aref row 0) 0) #\X))
               (rove:ok (equalp (sql-iterator-next plain) rejected))
               (rove:ok (equalp (sql-iterator-next plain) accepted))))
        (close-query-replay-store store))))

(rove:deftest nested-loop-replay-keeps-unmatched-row-positions
    (let ((*query-temp-directory* (uiop:temporary-directory)))
      (with-query-temp-files
        (dolist (limit '(1 1000000))
          (let* ((*query-work-memory-bytes* limit)
                 (left-rows (list (vector 1 "left") (vector +null+ "null-left")))
                 (right-rows (list (vector 9 "first") (vector 1 "match")
                                   (vector +null+ "null-right") (vector 7 "last")))
                 (join (list :type :full :left-width 2 :right-width 2
                             :qual '(:binop "=" (:col 1 :type :int4) (:col 3 :type :int4))))
                 (left (make-sql-iterator (lambda ()
                                            (let ((present (not (null left-rows))))
                                              (values (pop left-rows) present)))))
                 (right (make-sql-iterator (lambda ()
                                             (let ((present (not (null right-rows))))
                                               (values (pop right-rows) present)))))
                 (iterator (make-nested-loop-iterator join left right))
                 (rows nil))
            (unwind-protect
                 (progn
                   (map-sql-iterator iterator (lambda (row) (push row rows)))
                   (rove:ok (equalp (nreverse rows)
                                    (list (vector 1 "left" 1 "match")
                                          (vector +null+ "null-left" +null+ +null+)
                                          (vector +null+ +null+ 9 "first")
                                          (vector +null+ +null+ +null+ "null-right")
                                          (vector +null+ +null+ 7 "last")))
                            "skipped candidates keep their positions across replay and spill"))
              (close-sql-iterator iterator)))))))

(rove:deftest test-toast-catalog-ownership-roundtrip
    (let* ((toast (make-pg-toast :oid 100 :relfilenode 101 :index-oid 102
                                 :index-relfilenode 103 :relfrozenxid 20))
           (class (make-pg-class :oid 50 :name "parent" :relfilenode 51
                                 :relfrozenxid 25 :columns #() :indexes nil
                                 :toast toast))
           (decoded (class-from-sexp (class-to-sexp class)))
           (snapshot (catalog-mutation-copy-class class)))
      (rove:ok (equalp toast (pg-class-toast decoded)) "TOAST metadata survives catalog encoding")
      (setf (pg-toast-relfilenode toast) 500)
      (catalog-mutation-restore-class class snapshot)
      (rove:ok (= 101 (pg-toast-relfilenode (pg-class-toast class)))
               "rollback restores independent TOAST ownership metadata")))

(rove:deftest concurrent-index-ddl-parses-supported-options
    (let ((create (first (parse-statements
                          "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx ON tbl (id) NULLS NOT DISTINCT")))
          (drop (first (parse-statements
                        "DROP INDEX CONCURRENTLY IF EXISTS idx RESTRICT")))
          (ordinary (first (parse-statements "DROP INDEX idx"))))
      (rove:ok (eq :create-index (getf create :type)))
      (rove:ok (and (getf create :concurrently) (getf create :unique)
                    (getf create :if-not-exists) (getf create :nulls-not-distinct)))
      (rove:ok (and (eq :drop-index (getf drop :type))
                    (getf drop :concurrently) (getf drop :if-exists)
                    (equal '("idx") (getf drop :names))))
      (rove:ok (not (getf ordinary :concurrently))))
  (dolist (sql '("DROP INDEX CONCURRENTLY a, b" "DROP INDEX CONCURRENTLY a CASCADE"))
    (rove:ok (equal "0A000"
                    (handler-case (parse-statements sql)
                      (pg-error (condition) (pg-error-sqlstate condition)))))))

(rove:deftest index-catalog-defaults-preserve-legacy-invalid-indexes
    (let ((index (index-from-sexp '(:oid 51 :name "legacy" :valid nil))))
      (rove:ok (not (pg-index-valid-p index)))
      (rove:ok (pg-index-ready-p index) "legacy invalid indexes are still maintained")
      (rove:ok (pg-index-live-p index))
      (rove:ok (pg-index-logical-visible-p index)
               "old catalogs included legacy indexes in logical bootstrap")))

(rove:deftest logical-bootstrap-excludes-local-index-residue
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (database (make-database :name "postgres" :oid 1))
           (columns (vector (make-pg-column :name "id" :type :int4 :attnum 1)))
           (table (make-pg-class :name "tbl" :oid 41 :columns columns
                                 :indexes (list 51 52)))
           (creating (index-from-sexp
                      '(:oid 51 :name "creating" :table-oid 41 :columns (1)
                        :valid nil :ready t :live t :logical-visible nil)))
           (legacy (index-from-sexp
                    '(:oid 52 :name "legacy" :table-oid 41 :columns (1)
                      :valid nil :ready t :live t :logical-visible t))))
      (setf (gethash "postgres" (catalog-databases *catalog*)) database
            (gethash "tbl" (database-tables database)) table
            (gethash "creating" (database-indexes database)) creating
            (gethash "legacy" (database-indexes database)) legacy)
      (rove:ok (equal '("legacy")
                      (mapcar (lambda (entry) (getf entry :name))
                              (getf (catalog-replication-bootstrap-data database) :indexes))))
      (rove:ok (eq nil (getf (index-to-sexp creating) :logical-visible :missing)))
      (rove:ok (pg-index-logical-visible-p (index-from-sexp (index-to-sexp legacy))))))

(cl-spec:defproperty concurrent-index-catalog-state-roundtrip
    ((valid (member nil t)) (ready (member nil t)) (live (member nil t))
                            (logical-visible (member nil t)))
  (:about index-from-sexp index-to-sexp)
  (:trials (:normal 100))
  (let* ((encoded (list :valid valid :ready ready :live live :logical-visible logical-visible))
         (result (index-from-sexp (index-to-sexp (index-from-sexp encoded)))))
    (and (eq valid (pg-index-valid-p result))
         (eq ready (pg-index-ready-p result))
         (eq live (pg-index-live-p result))
         (eq logical-visible (pg-index-logical-visible-p result)))))

(rove:deftest concurrent-index-catalog-generated-roundtrip
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'concurrent-index-catalog-state-roundtrip :seed 1050))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))

(rove:deftest test-catalog-mutation-rollback-preserves-object-identity
    (let* ((*catalog* (make-catalog :next-oid 20000))
           (*wal-pending-deletes* nil)
           (database (make-database))
           (table (make-pg-class :name "t" :columns
                                 (vector (make-pg-column :default-expression (list :const 7)))
                                 :indexes (list 51)))
           (index (make-pg-index :name "i" :columns (list 1)))
           (sequence (make-pg-sequence :name "s" :last-value 9))
           (publication (make-pg-publication :name "p" :tables (list (list "t")))))
      (setf (gethash "t" (database-tables database)) table
            (gethash "i" (database-indexes database)) index
            (gethash "s" (database-sequences database)) sequence
            (gethash "p" (database-publications database)) publication)
      (let ((snapshot (catalog-capture-mutation-snapshot database)))
        (clrhash (database-tables database))
        (setf (first (pg-class-indexes table)) 52
              (second (pg-column-default-expression (aref (pg-class-columns table) 0))) 8
              (first (pg-index-columns index)) 2
              (pg-sequence-last-value sequence) 10
              (caar (pg-publication-tables publication)) "other")
        (catalog-restore-mutation-snapshot snapshot))
      (rove:ok (eq table (gethash "t" (database-tables database))))
      (rove:ok (eq index (gethash "i" (database-indexes database))))
      (rove:ok (eq sequence (gethash "s" (database-sequences database))))
      (rove:ok (eq publication (gethash "p" (database-publications database))))
      (rove:ok (equal '(51) (pg-class-indexes table)))
      (rove:ok (equal '(:const 7) (pg-column-default-expression (aref (pg-class-columns table) 0))))
      (rove:ok (equal '(1) (pg-index-columns index)))
      (rove:ok (= 9 (pg-sequence-last-value sequence)))
      (rove:ok (equal '(("t")) (pg-publication-tables publication)))))

(rove:deftest type-modifier-widths-preserve-coercion-and-origin-rules
    (rove:ok (null (sql-varchar-typmod-length -1)))
  (rove:ok (null (sql-varchar-typmod-length 3)))
  (rove:ok (= 0 (sql-varchar-typmod-length 4)))
  (rove:ok (= 2 (sql-varchar-typmod-length (sql-varchar-typmod 2))))
  (rove:ok (string= "" (sql-varchar-cast-value "é" 4)))
  (let ((source (sql-numeric-typmod 4 -2))
        (widened (sql-numeric-typmod 6 -2))
        (changed-scale (sql-numeric-typmod 6 2)))
    (rove:ok (datum-origin-coercion-compatible-p :numeric source :numeric widened))
    (rove:ok (not (datum-origin-coercion-compatible-p :numeric widened :numeric source)))
    (rove:ok (not (datum-origin-coercion-compatible-p :numeric source :numeric changed-scale)))
    (rove:ok (datum-origin-coercion-compatible-p :numeric source :numeric -1))
    (rove:ok (not (datum-origin-coercion-compatible-p :numeric -1 :numeric source)))))

(rove:deftest catalog-replay-keeps-numeric-and-timestamp-type-modifiers
    (let* ((typmod (sql-numeric-typmod 9 -3))
           (numeric (make-pg-column :name "n" :type :numeric :typmod typmod))
           (timestamp (make-pg-column :name "t" :type :timestamp :typmod 2)))
      (rove:ok (= typmod #x90801))
      (rove:ok (equal '(9 -3) (multiple-value-list (sql-numeric-typmod-components typmod))))
      (rove:ok (string= "numeric(9,-3)" (catalog-expression-type-sql :numeric typmod)))
      (rove:ok (string= "\"n\" numeric(9,-3)" (catalog-column-definition-sql numeric)))
      (rove:ok (string= "timestamp(2)" (catalog-expression-type-sql :timestamp 2)))
      (rove:ok (string= "\"t\" timestamp(2) without time zone" (catalog-column-definition-sql timestamp)))))

(rove:deftest catalog-replay-keeps-interval-range-and-precision
    (let ((hour-minute #x0c000002)
          (day-second #x1c080002))
      (rove:ok (= 3072 (sql-interval-range-mask :hour :minute)))
      (rove:ok (= 7176 (sql-interval-range-mask :day :second)))
      (rove:ok (null (sql-interval-range-mask :hour :hour)))
      (rove:ok (equal '(3072 2) (multiple-value-list (sql-interval-typmod-components hour-minute))))
      (rove:ok (= day-second (sql-interval-typmod 7176 2)))
      (rove:ok (string= "\"interval\"(3072,2)" (catalog-expression-type-sql :interval hour-minute)))
      (rove:ok (string= "interval day to second(2)" (catalog-expression-type-sql :interval day-second)))))

(rove:deftest test-toast-relation-eligibility
    (flet ((column (type &optional (typmod -1))
             (make-pg-column :name "x" :type type :typmod typmod :attnum 1)))
      (rove:ok (not (toast-table-needed-p (vector (column :int4))))
               "fixed-width-only tables need no TOAST table")
      (rove:ok (not (toast-table-needed-p (vector (column :varchar 24))))
               "bounded small varchar fits without TOAST")
      (rove:ok (toast-table-needed-p (vector (column :varchar -1)))
               "unlimited varchar needs TOAST")
      (rove:ok (toast-table-needed-p (vector (column :numeric -1)))
               "unbounded numeric needs TOAST")
      (rove:ok (not (toast-table-needed-p (vector (column :numeric (+ 4 (ash 10 16))))))
               "bounded small numeric fits without TOAST")
      (rove:ok (toast-table-needed-p (make-array 5 :initial-element (column :varchar 130)))
               "combined maximum tuple width can require TOAST")))

(rove:deftest test-toast-relation-inventory
    (let* ((*catalog* (make-catalog))
           (db (make-database :name "db"))
           (table (make-pg-class :name "parent" :relfilenode 51
                                 :toast (make-pg-toast :oid 100 :relfilenode 101
                                                       :index-oid 102 :index-relfilenode 103))))
      (setf (gethash "db" (catalog-databases *catalog*)) db
            (gethash "parent" (database-tables db)) table)
      (let ((nodes (catalog-physical-relfilenodes)))
        (rove:ok (and (member 51 nodes) (member 101 nodes) (member 103 nodes))
                 "inventory retains parent, TOAST heap, and TOAST index"))))

(rove:deftest test-toast-catalog-type-metadata
    (let ((rows (sql-type-rows)))
      (dolist (case '((25 "text" -1 #\i #\x)
                      (17 "bytea" -1 #\i #\x)
                      (114 "json" -1 #\i #\x)
                      (3802 "jsonb" -1 #\i #\x)
                      (1700 "numeric" -1 #\i #\m)
                      (23 "int4" 4 #\i #\p)
                      (1016 "_int8" -1 #\d #\x)))
        (let ((row (find (first case) rows :key (lambda (row) (aref row 0)))))
          (rove:ok row)
          (when row
            (rove:ok (equal (coerce row 'list)
                            (list (first case) (second case) 11 (third case)
                                  (char-code (fourth case)) (char-code (fifth case)))))))))
  (rove:ok (= (sql-result-column-attnum 1259 7) 14))
  (rove:ok (= (sql-result-column-attnum 1249 9) 10))
  (rove:ok (= (sql-result-column-attnum 1247 6) 24)))

(rove:deftest test-toast-ownership-horizon-and-drop
    (let* ((*catalog* (make-catalog))
           (*wal-ready* t)
           (*wal-pending-deletes* nil)
           (db (make-database :name "db"))
           (toast (make-pg-toast :oid 100 :relfilenode 101 :index-oid 102
                                 :index-relfilenode 103 :relfrozenxid 10))
           (table (make-pg-class :name "parent" :relfilenode 51 :relfrozenxid 20 :toast toast)))
      (setf (gethash "db" (catalog-databases *catalog*)) db
            (gethash "parent" (database-tables db)) table)
      (rove:ok (= 10 (catalog-compute-oldest-relfrozenxid))
               "TOAST heap constrains the cluster freeze horizon")
      (drop-table* db "parent")
      (rove:ok (and (member 51 *wal-pending-deletes*)
                    (member 101 *wal-pending-deletes*)
                    (member 103 *wal-pending-deletes*))
               "parent drop defers deletion of both owned TOAST relations")))

(cl-spec:defproperty catalog-comparison-keeps-complete-reader-forms
    ((salt (range integer 0 255)) (width (range integer 0 4)))
  (:about physical-history-catalog-images-equal-p)
  (:trials (:normal 100))
  (let* ((matrix (make-array (list 2 width) :element-type '(unsigned-byte 8)))
         (image (list :name "Case" :number salt :matrix matrix
                      :vector #(1 "Case") :float 1.25d0 :character #\Q)))
    (dotimes (i (array-total-size matrix))
      (setf (row-major-aref matrix i) (mod (+ salt i) 256)))
    (let* ((text (with-standard-io-syntax (prin1-to-string image)))
           (bytes (string->bytes text))
           (spaced (string->bytes (format nil "; leading~%~a~%#| trailing |#  " text))))
      (and (physical-history-catalog-images-equal-p bytes spaced)
           (physical-history-catalog-images-equal-p spaced bytes)))))

(rove:deftest catalog-comparison-generated-reader-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'catalog-comparison-keeps-complete-reader-forms
                                       :seed 7619 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 100 (getf data :trials)))))

(rove:deftest catalog-comparison-rejects-changed-complete-forms
    (dolist (pair (list (list '(:name "Case") '(:name "case"))
                        (list '(:number 1) '(:number 1.0))
                        (list '(:number 1) '(:number 2))
                        (list '(:name "Case") '(:name "Case" :unknown nil))
                        (list '(:name "Case") '(:name "Case" :name "Case"))
                        (list '(:a 1 :b 2) '(:b 2 :a 1))
                        (list #2A((1 2) (3 4)) #2A((1 2 3 4)))
                        (list #2A((1 2) (3 4)) #2A((1 2) (3 5)))
                        (list (make-array 2 :element-type '(unsigned-byte 8)
                                            :initial-contents '(1 2)) #(1 2))
                        (list #\a #\A)))
      (let ((left (string->bytes (with-standard-io-syntax (prin1-to-string (first pair)))))
            (right (string->bytes (with-standard-io-syntax (prin1-to-string (second pair))))))
        (rove:ok (not (physical-history-catalog-images-equal-p left right)))))
  (let ((unsupported (make-hash-table)))
    (rove:ok (not (physical-history-catalog-form-equal-p unsupported unsupported)))))

(rove:deftest catalog-comparison-rejects-trailing-forms-and-reader-labels
    (dolist (text '("(:name \"Case\") (:extra t)"
                    "#1=(:name \"Case\")" "#1#"))
      (rove:ok
       (handler-case
           (progn (physical-history-catalog-images-equal-p
                   (string->bytes text) (string->bytes "(:name \"Case\")"))
                  nil)
         (error () t)))))

(rove:deftest test-toast-catalog-physical-history
    (let* ((toast (make-pg-toast :oid 100 :relfilenode 101 :index-oid 102
                                 :index-relfilenode 103 :relfrozenxid 20))
           (class (make-pg-class :oid 50 :name "parent" :relfilenode 51
                                 :relfrozenxid 25 :columns #() :toast toast))
           (image (list :cluster-identity "toast-test" :next-oid 104
                        :databases (list (list :name "db"
                                               :tables (list (class-to-sexp class))
                                               :indexes nil))))
           (bytes (string->bytes (catalog-readable-data-string image)))
           (live (physical-history-catalog-relfilenodes bytes "toast-test")))
      (rove:ok (and (gethash 51 live) (gethash 101 live) (gethash 103 live))
               "physical history retains both TOAST storage files")))

(rove:deftest test-toast-catalog-history-rejects-ambiguous-ownership
    (labels ((read-live (toast)
               (let* ((table (list :name "parent" :relfilenode 51 :toast toast))
                      (image (list :cluster-identity "toast-test" :next-oid 104
                                   :databases (list (list :name "db" :tables (list table)
                                                          :indexes nil)))))
                 (physical-history-catalog-relfilenodes
                  (string->bytes (catalog-readable-data-string image)) "toast-test"))))
      (dolist (toast (list '(:relfilenode 101 :index-relfilenode 0)
                           '(:relfilenode 101 :index-relfilenode 103 :relfilenode 102)
                           '(:relfilenode 101)))
        (rove:ok (handler-case (progn (read-live toast) nil) (error () t))
                 "malformed or ambiguous TOAST ownership is rejected"))))

(rove:deftest catalog-read-requires-a-stored-cluster-identity
    (let ((image (catalog-to-sexp (make-catalog))))
      (dolist (identity '(nil "" 42))
        (setf (getf image :cluster-identity) identity)
        (rove:ok (handler-case (progn (catalog-from-sexp image) nil)
                   (error (condition)
                     (search "invalid or missing cluster identity" (princ-to-string condition))))))
      (remf image :cluster-identity)
      (rove:ok (handler-case (progn (catalog-from-sexp image) nil)
                 (error (condition)
                   (search "invalid or missing cluster identity" (princ-to-string condition)))))))

(rove:deftest test-index-catalog-roundtrip
    (let* ((catalog (make-catalog))
           (identity (catalog-cluster-identity catalog))
           (copy (catalog-from-sexp (catalog-to-sexp catalog))))
      (rove:ok (and (stringp identity)
                    (string= identity (catalog-cluster-identity copy)))
               "catalog incarnation survives durable catalog serialization"))
  (let* ((idx (make-pg-index :oid 42 :name "nnd" :relfilenode 42
                             :table-oid 41 :columns '(1) :unique-p t
                             :constraint-p t
                             :nulls-not-distinct-p t :valid-p t))
         (sexp (index-to-sexp idx))
         (copy (index-from-sexp sexp)))
    (rove:ok (equal sexp
                    '(:oid 42 :name "nnd" :relfilenode 42 :table-oid 41
                      :columns (1) :unique t :primary nil :constraint t
                      :nulls-not-distinct t :valid t))
             "index catalog encoding is the compatible flat plist")
    (rove:ok (pg-index-nulls-not-distinct-p copy)
             "NULLS NOT DISTINCT survives catalog serialization")
    (rove:ok (pg-index-constraint-p copy)
             "constraint ownership survives catalog serialization")
    (rove:ok (pg-index-valid-p copy)
             "index validity survives catalog serialization"))
  (let* ((class (make-pg-class
                 :oid 50 :name "codec" :relfilenode 51
                 :columns (vector (make-pg-column :name "value" :type :int4
                                                  :typmod -1 :attnum 1
                                                  :notnull t))
                 :indexes '(52)))
         (sexp (class-to-sexp class))
         (copy (class-from-sexp sexp)))
    (rove:ok (equal sexp
                    `(:oid 50 :name "codec" :relfilenode 51
                      :relfrozenxid ,+first-normal-xid+
                      :columns ((:name "value" :type :int4 :typmod -1
                                 :attnum 1 :notnull t :default-sequence nil
                                 :default-expression nil :default-sql nil :storage nil))
                      :indexes (52) :toast nil :checks nil
                      :statistics nil))
             "class and column codecs use compatible flat plists")
    (rove:ok (vectorp (pg-class-columns copy))
             "class codec restores columns as a vector")
    (rove:ok (string= (pg-column-name (aref (pg-class-columns copy) 0)) "value")
             "class codec round-trips column mappings"))
  (let ((legacy-pk (index-from-sexp '(:primary t))))
    (rove:ok (not (pg-index-constraint-p legacy-pk))
             "legacy catalog has no constraint ownership flag")
    (rove:ok (not (pg-index-valid-p legacy-pk))
             "legacy indexes without a validity flag are not trusted")
    (rove:ok (pg-index-constraint-owned-p legacy-pk)
             "legacy primary index remains constraint-owned")))

(rove:deftest test-table-statistics-catalog-roundtrip
    (let* ((statistics
             (make-pg-table-statistics
              :row-count 6 :sample-count 3
              :columns
              (vector (make-pg-column-statistics
                       :null-fraction 0.25d0 :distinct-count 3
                       :most-common-values '((1 . 0.5d0))
                       :histogram-bounds '(-9223372036854775808 5 9223372036854775807)))))
           (table (make-pg-class :oid 55 :name "stats" :relfilenode 56
                                 :columns #() :statistics statistics))
           (copy (class-from-sexp (class-to-sexp table)))
           (restored (pg-class-statistics copy)))
      (rove:ok (= (pg-table-statistics-row-count restored) 6)
               "ANALYZE row count persists through the class catalog codec")
      (rove:ok (= (pg-table-statistics-sample-count restored) 3)
               "ANALYZE sample count persists without storing raw sample rows")
      (rove:ok (= (pg-column-statistics-distinct-count
                   (aref (pg-table-statistics-columns restored) 0))
                  3)
               "column distinct count persists through the class catalog codec")
      (rove:ok (equal (pg-column-statistics-histogram-bounds
                       (aref (pg-table-statistics-columns restored) 0))
                      '(-9223372036854775808 5 9223372036854775807)))
      (rove:ok (null (pg-column-statistics-histogram-bounds
                      (column-statistics-from-sexp '(:null-fraction 0.0d0 :distinct-count 1
                                                     :most-common-values ((1 . 1.0d0)))))))))

(rove:deftest range-index-cost-uses-nonnull-distribution
    (let* ((column (make-pg-column :name "k" :type :int8 :attnum 1))
           (distribution (make-pg-column-statistics :null-fraction 0.1d0
                                                    :most-common-values '((0 . 0.8d0))
                                                    :histogram-bounds '(1000 2000)))
           (stats (make-pg-table-statistics :row-count 1000 :columns (vector distribution)))
           (table (make-pg-class :columns (vector column) :statistics stats))
           (index (make-pg-index :columns '(1) :valid-p t))
           (qual '(:binop ">" (:col 1 :type :int8) (:const 1000 :type :int4)))
           (path (index-path-for-range table index (index-range-comparisons (list qual) 0))))
      (rove:ok (eq (select-scan-path-kind path) :index-scan))
      (rove:ok (= (round (select-scan-path-row-estimate table path)) 100))
      (rove:ok (= 900 (round (* 1000 (planner-scalar-range-selectivity
                                      table 1 "<" :numeric-positive-infinity :numeric)))))
      (rove:ok (zerop (planner-scalar-range-selectivity
                       table 1 ">" :numeric-positive-infinity :numeric)))
      (setf (pg-column-statistics-histogram-bounds distribution) nil)
      (rove:ok (= (round (* 1000 (planner-scalar-range-selectivity table 1 ">" 1000 :int4))) 50))))

(rove:deftest float-range-histogram-interpolates-across-extreme-values
    (let* ((limit (rational most-positive-double-float))
           (stats (make-pg-column-statistics :null-fraction 0.0d0
                                             :histogram-bounds (list (- limit) limit)))
           (table (make-pg-class :columns (vector (make-pg-column :type :float8 :attnum 1))
                                 :statistics (make-pg-table-statistics :columns (vector stats)))))
      (rove:ok (= 0.5d0 (planner-scalar-range-selectivity table 1 "<" 0.0d0 :float8)))
      (rove:ok (= 0.5d0 (planner-scalar-range-selectivity table 1 ">" 0.0d0 :float8)))))

(rove:deftest histogram-equality-correction-is-monotone-at-bucket-edges
    (let* ((stats (make-pg-column-statistics :null-fraction 0.0d0
                                             :distinct-count 3 :histogram-bounds '(0 1 2)))
           (table (make-pg-class :columns (vector (make-pg-column :type :float8 :attnum 1))
                                 :statistics (make-pg-table-statistics :columns (vector stats)))))
      (flet ((estimate (operator value) (planner-scalar-range-selectivity table 1 operator value :float8)))
        (rove:ok (= 0.0d0 (estimate "<" 0.0d0)))
        (rove:ok (< (estimate "<=" 0.0d0) (estimate "<=" 0.0001d0)))
        (rove:ok (< (estimate "<" 0.9999d0) (estimate "<" 1.0d0)))
        (rove:ok (< (estimate "<" 1.0d0) (estimate "<=" 1.0d0)))
        (rove:ok (< (estimate ">" 2.0d0) (estimate ">=" 2.0d0))))))

(rove:deftest join-cardinality-fallback-uses-input-size-and-known-null-mass
    (let* ((column (vector (make-pg-column :type :text :attnum 1)))
           (skew (make-pg-column-statistics :null-fraction 0.25d0 :distinct-count 2
                                            :most-common-values '(("common" . 0.5d0))))
           (left (make-pg-class :columns column))
           (right (make-pg-class :columns column
                                 :statistics (make-pg-table-statistics :columns (vector skew))))
           (relations (list (list :table left) (list :table right)))
           (join '(:left-width 1 :right-width 1 :qual (:binop "=" (:col 1 :type :text) (:col 2 :type :text)))))
      (rove:ok (= 0.005d0 (explain-equality-join-selectivity
                           join '(:seq-scan 0 1) '(:filter nil (:seq-scan 1 2)) relations 1000 40)))
      (rove:ok (null (explain-equality-join-selectivity
                      join '(:seq-scan 0 1) '(:seq-scan 1 2) relations 1000 40 t)))
      (rove:ok (= 0.1875d0 (explain-equality-join-selectivity
                            join '(:index-scan 0 1) '(:seq-scan 1 2) relations 4 1000)))))

(rove:deftest aggregate-toast-demand-proofs
    (let* ((table (make-pg-class :oid 9001 :columns (vector (make-pg-column :name "id" :type :int4)
                                                            (make-pg-column :name "v" :type :text))))
           (relations (list (list :table table)))
           (scan '(:seq-scan 0 9001))
           (project (lambda (child) (list :project (list (list :expr (list :col 1))) child))))
      (rove:ok (equal (aggregate-toast-demand
                       (funcall project (list :hash-aggregate nil 1 scan)) relations)
                      (cons table nil)))
      (rove:ok (equal (aggregate-toast-demand
                       (funcall project (list :hash-aggregate nil 1
                                              (list :filter '(:binop := (:col 1) (:const 7)) scan)))
                       relations)
                      (cons table '(1))))
      (rove:ok (equal (aggregate-toast-demand
                       (funcall project (list :sorted-aggregate nil (list '(:kind :count-expr :input (:col 2))) scan)) relations)
                      (cons table '(2))))
      (rove:ok (equal (aggregate-toast-demand
                       (funcall project (list :hash-aggregate (list '(:col 2)) 1 scan)) relations)
                      (cons table '(2))))
      (rove:ok (null (aggregate-toast-demand
                      (funcall project (list :hash-aggregate nil 1
                                             (list :filter '(:subplan x) scan))) relations)))
      (rove:ok (null (aggregate-toast-demand
                      (funcall project (list :sorted-aggregate nil (list '(:kind :sum :input (:subplan x))) scan))
                      relations)))))

(rove:deftest test-analyze-physical-column-width-and-distinct
    (let* ((column (make-pg-column :name "v" :type :text))
           (columns (vector column))
           (table (make-pg-class :columns columns))
           (narrow (make-string 1020 :initial-element #\a))
           (wide (make-string 1021 :initial-element #\b))
           (sample (map 'vector (lambda (value)
                                  (form-heap-tuple (vector value) columns 1 0 0 0 1))
                        (vector narrow narrow wide +null+)))
           (stats (analyze-physical-column-statistics table sample 0 4 nil (list +analyze-sample-memory-bytes+))))
      (rove:ok (= (pg-column-statistics-null-fraction stats) 0.25d0))
      (rove:ok (= (pg-column-statistics-distinct-count stats) 2))
      (rove:ok (= (cdar (pg-column-statistics-most-common-values stats)) 0.5d0))
      (rove:ok (string= (caar (pg-column-statistics-most-common-values stats)) narrow))))

(rove:deftest test-analyze-wide-external-keeps-other-column
    (let* ((columns (vector (make-pg-column :name "id" :type :int4)
                            (make-pg-column :name "v" :type :text)))
           (table (make-pg-class :columns columns))
           (pointer (make-heap-external-varlena :raw-size 1100004 :external-info 1100000
                                                :value-id 1 :relation-id 1 :id-width 4))
           (bytes (form-heap-tuple (vector 7 pointer) columns 1 0 0 0 1))
           (sample (vector bytes bytes bytes))
           (id-stats (analyze-physical-column-statistics table sample 0 3 nil (list +analyze-sample-memory-bytes+)))
           (wide-stats (analyze-physical-column-statistics table sample 1 300000 nil (list +analyze-sample-memory-bytes+))))
      (rove:ok (= (pg-column-statistics-distinct-count id-stats) 1))
      (rove:ok (= (cdar (pg-column-statistics-most-common-values id-stats)) 1.0d0))
      (rove:ok (= (pg-column-statistics-distinct-count wide-stats) 300000))
      (rove:ok (null (pg-column-statistics-most-common-values wide-stats)))))

(rove:deftest test-analyze-physical-large-array-late-majority
    (let* ((columns (vector (make-pg-column :name "v" :type :bytea-array)))
           (table (make-pg-class :columns columns))
           (sample (make-array 300)))
      (dotimes (i 300)
        (let* ((elements (make-array 7800 :initial-element +null+))
               (tag (if (< i 67) i 67)))
          (setf (aref elements 0) (make-array 1 :element-type '(unsigned-byte 8) :initial-element tag))
          (let ((array (make-sql-array :element-type-id :bytea :elements elements
                                       :dimensions '(7800) :lower-bounds '(1))))
            (setf (aref sample i) (form-heap-tuple (vector array) columns 1 0 0 0 1)))))
      (let ((stats (analyze-physical-column-statistics table sample 0 300 nil
                                                       (list +analyze-sample-memory-bytes+)
                                                       :value-hash (constantly 1))))
        (rove:ok (= (pg-column-statistics-distinct-count stats) 68))
        (rove:ok (= (cdar (pg-column-statistics-most-common-values stats)) (/ 233.0d0 300))))))

(rove:deftest test-analyze-byte-vector-statistics-and-planner
    (let* ((first (make-array 3 :element-type '(unsigned-byte 8)
                                :initial-contents '(0 255 1)))
           (first-copy (copy-seq first))
           (other (make-array 4 :element-type '(unsigned-byte 8)
                                :initial-contents '(0 255 1 2)))
           (other-copy (copy-seq other))
           (empty (make-array 0 :element-type '(unsigned-byte 8)))
           (empty-copy (copy-seq empty))
           (sample (vector (vector first) (vector first-copy) (vector (copy-seq first))
                           (vector other) (vector other-copy) (vector empty)
                           (vector empty-copy) (vector +null+)))
           (column (analyze-column-statistics sample 0 8))
           (table (make-pg-class
                   :oid 5703 :name "byte_statistics" :relfilenode 5703
                   :columns #()
                   :statistics
                   (make-pg-table-statistics
                    :row-count 8 :sample-count 8 :columns (vector column))))
           (restored (test-analyze-catalog-class-image-roundtrip table))
           (restored-column
             (aref (pg-table-statistics-columns
                    (pg-class-statistics restored))
                   0))
           (mcv (pg-column-statistics-most-common-values restored-column)))
      (rove:ok (= (pg-column-statistics-distinct-count column) 3)
               "ANALYZE counts separately allocated equal byte vectors once")
      (rove:ok (= (pg-column-statistics-null-fraction column) (/ 1.0d0 8))
               "ANALYZE keeps NULL separate from byte-vector values")
      (rove:ok (= (cdr (assoc first-copy mcv
                              :test #'analyze-statistics-value-equal-p))
                  (/ 3.0d0 8))
               "ANALYZE records the byte-vector MCV frequency over all sample rows")
      (rove:ok (= (cdr (assoc other-copy mcv
                              :test #'analyze-statistics-value-equal-p))
                  (/ 2.0d0 8))
               "ANALYZE distinguishes byte vectors with different lengths")
      (rove:ok (= (cdr (assoc empty-copy mcv
                              :test #'analyze-statistics-value-equal-p))
                  (/ 2.0d0 8))
               "ANALYZE matches separately allocated empty byte vectors")
      (rove:ok (typep (car (first mcv)) '(vector (unsigned-byte 8)))
               "catalog image restores byte-vector element type")
      (rove:ok (= (planner-equality-selectivity
                   restored 1 (copy-seq first))
                  (/ 3.0d0 8))
               "planner matches a fresh byte vector against the persisted MCV")
      (rove:ok (= (planner-equality-selectivity
                   restored 1 (copy-seq other))
                  (/ 2.0d0 8))
               "planner preserves byte-vector MCV frequency after catalog reload")))

(rove:deftest test-analyze-byte-vector-hash-collisions-and-text-equality
    (let* ((first (make-array 2 :element-type '(unsigned-byte 8)
                                :initial-contents '(1 2)))
           (sample (vector (vector first) (vector (copy-seq first))
                           (vector (make-array 2 :element-type '(unsigned-byte 8)
                                                 :initial-contents '(2 1)))
                           (vector (make-array 0 :element-type '(unsigned-byte 8)))
                           (vector +null+)))
           (collided
             (analyze-column-statistics
              sample 0 5 :value-hash (lambda (value) (declare (ignore value)) 0)))
           (text-column
             (analyze-column-statistics (vector #("A") #("a") #("A")) 0 3)))
      (rove:ok (= (pg-column-statistics-distinct-count collided) 3)
               "byte-vector collisions are resolved with content equality")
      (rove:ok (= (cdr (assoc first
                              (pg-column-statistics-most-common-values collided)
                              :test #'analyze-statistics-value-equal-p))
                  (/ 2.0d0 5))
               "colliding byte-vector buckets retain the correct repeated count")
      (rove:ok (= (pg-column-statistics-distinct-count text-column) 2)
               "ANALYZE preserves case-sensitive EQUAL semantics for strings")))

(rove:deftest test-analyze-value-estimate-counts-backing-storage
    (let* ((adjustable-bytes
             (make-array 1024 :element-type '(unsigned-byte 8)
                              :initial-element 0 :adjustable t :fill-pointer 4))
           (byte-backing
             (make-array 4096 :element-type '(unsigned-byte 8)
                              :initial-element 0))
           (displaced-bytes
             (make-array 8 :element-type '(unsigned-byte 8)
                           :displaced-to byte-backing :displaced-index-offset 512))
           (adjustable-string
             (make-array 1024 :element-type 'character :initial-element #\x
                              :adjustable t :fill-pointer 4))
           (string-backing (make-string 1000 :initial-element #\x))
           (displaced-string
             (make-array 10 :element-type 'character
                            :displaced-to string-backing :displaced-index-offset 20)))
      (rove:ok (>= (analyze-value-memory-estimate adjustable-bytes) (+ 64 1024))
               "ANALYZE charges adjustable byte-vector capacity beyond its fill pointer")
      (rove:ok (>= (analyze-value-memory-estimate adjustable-string) (+ 64 4096))
               "ANALYZE charges adjustable string capacity beyond its fill pointer")
      (rove:ok (>= (analyze-value-memory-estimate displaced-bytes) (+ 64 64 4096))
               "ANALYZE charges a displaced byte vector's full backing array")
      (rove:ok (>= (analyze-value-memory-estimate displaced-string) (+ 64 64 4000))
               "ANALYZE charges a displaced string's full backing array")))

(rove:deftest test-analyze-reservoir-shrink-clears-evicted-values
    (let* ((*analyze-random-function*
             (lambda (limit) (declare (ignore limit)) 0))
           (sample (make-array 4 :adjustable t :fill-pointer 4
                                 :initial-contents (vector :a :b :c :d))))
      (analyze-shrink-reservoir sample 2)
      (rove:ok (= (fill-pointer sample) 2)
               "ANALYZE shrink updates the reservoir fill pointer")
      (rove:ok (and (null (aref sample 2)) (null (aref sample 3)))
               "ANALYZE shrink clears references beyond the fill pointer")))

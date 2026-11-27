;;;; Referenced keys for foreign key constraints.

(in-package #:pg)

(rove:deftest foreign-key-target-uses-valid-plain-unique-keys
    (let* ((table (make-pg-class :name "parent" :columns
                                 (vector (make-pg-column :name "a" :attnum 1 :type :int4)
                                         (make-pg-column :name "b" :attnum 2 :type :int4))))
           (primary (make-pg-index :columns '(1 2) :unique-p t :primary-p t :valid-p t))
           (partial (make-pg-index :columns '(1 2) :unique-p t :valid-p t :predicate '(:bool t)))
           (invalid (make-pg-index :columns '(1 2) :unique-p t)))
      (multiple-value-bind (attnums index)
          (resolve-foreign-key-target table '("b" "a") (list partial invalid primary))
        (rove:ok (equal attnums '(2 1)))
        (rove:ok (eq index primary)))
      (rove:ok (equal (resolve-foreign-key-target table nil (list primary)) '(1 2)))
      (rove:ok (rove:signals (resolve-foreign-key-target table '("a" "a") (list primary)) 'pg-error))
      (rove:ok (rove:signals (resolve-foreign-key-target table '("a" "b") (list partial invalid))
                             'pg-error))))

(rove:deftest foreign-key-definition-binds-column-and-index-identities
    (let* ((child (make-pg-class :oid 100 :name "child" :columns
                                 (vector (make-pg-column :name "x" :attnum 1 :type :int8)
                                         (make-pg-column :name "y" :attnum 2 :type :int4))))
           (parent (make-pg-class :oid 101 :name "parent" :columns
                                  (vector (make-pg-column :name "a" :attnum 1 :type :int4)
                                          (make-pg-column :name "b" :attnum 2 :type :int8))))
           (index (make-pg-index :oid 102 :valid-p t :unique-p t :columns '(1 2)))
           (definition '(:columns ("x" "y") :referenced-columns ("b" "a") :not-valid t
                         :delete-action :cascade))
           (key (cook-foreign-key child parent definition 103 (list index))))
      (rove:ok (equal (list (pg-foreign-key-oid key)
                            (pg-foreign-key-name key)
                            (pg-foreign-key-columns key)
                            (pg-foreign-key-referenced-columns key)
                            (pg-foreign-key-referenced-table-oid key)
                            (pg-foreign-key-referenced-index-oid key)
                            (pg-foreign-key-delete-action key)
                            (pg-foreign-key-validated-p key))
                      '(103 "child_x_y_fkey" (1 2) (2 1) 101 102 :cascade nil)))
      (rove:ok (equal "42830"
                      (handler-case
                          (cook-foreign-key child parent '(:columns ("x") :referenced-columns ("b" "a"))
                                            104 (list index))
                        (pg-error (error) (pg-error-sqlstate error)))))))

(rove:deftest foreign-key-child-key-matches-parent-encoding
    (let* ((parent (make-pg-class :columns
                                  (vector (make-pg-column :type :int2)
                                          (make-pg-column :type :bool))))
           (index (make-pg-index :columns '(1 2)))
           (key (make-pg-foreign-key :columns '(1 3) :referenced-columns '(2 1) :match-type :full)))
      (rove:ok (equalp (encode-index-key '(7 nil) '(:int2 :bool))
                       (foreign-key-child-index-key key parent index (vector nil 99 7))))
      (rove:ok (eq :out-of-range (nth-value 1
                                            (foreign-key-child-index-key key parent index (vector nil 99 65543)))))
      (rove:ok (eq :mixed-null (nth-value 1
                                          (foreign-key-child-index-key key parent index (vector nil 99 +null+)))))
      (rove:ok (eq :null (nth-value 1
                                    (foreign-key-child-index-key key parent index (vector +null+ 99 +null+)))))))

(rove:deftest foreign-key-table-catalog-round-trip
    (let* ((key (make-pg-foreign-key :oid 12000 :name "child_parent"
                                     :columns '(1 3) :referenced-columns '(2 1)
                                     :referenced-table-oid 12001 :referenced-index-oid 12002
                                     :delete-action :cascade :deferrable-p t
                                     :initially-deferred-p t :validated-p t))
           (table (make-pg-class :oid 12003 :name "child" :columns #()
                                 :foreign-keys (list key)))
           (encoded (class-to-sexp table))
           (decoded (class-from-sexp encoded)))
      (rove:ok (equal encoded (class-to-sexp decoded)))
      (rove:ok (equal '(2 1) (pg-foreign-key-referenced-columns
                              (first (pg-class-foreign-keys decoded)))))
      (rove:ok (null (pg-class-foreign-keys
                      (class-from-sexp (class-to-sexp (make-pg-class :columns #()))))))))

(rove:deftest foreign-key-catalog-rejects-removed-dependencies
    (let* ((database (make-database))
           (child-column (make-pg-column :attnum 1 :type :int4))
           (parent-column (make-pg-column :attnum 1 :type :int4))
           (constraint (make-pg-foreign-key :name "fk" :columns '(1)
                                            :referenced-table-oid 201 :referenced-index-oid 202
                                            :referenced-columns '(1)))
           (child (make-pg-class :oid 200 :name "child" :columns (vector child-column)
                                 :foreign-keys (list constraint)))
           (parent (make-pg-class :oid 201 :name "parent" :columns (vector parent-column)))
           (index (make-pg-index :oid 202 :table-oid 201 :columns '(1) :valid-p t :unique-p t)))
      (setf (gethash "child" (database-tables database)) child
            (gethash "parent" (database-tables database)) parent
            (gethash "parent_key" (database-indexes database)) index)
      (rove:ok (not (rove:signals (validate-catalog-foreign-key-dependencies database) 'pg-error)))
      (setf (pg-column-dropped-p parent-column) t)
      (rove:ok (rove:signals (validate-catalog-foreign-key-dependencies database) 'pg-error))
      (setf (pg-column-dropped-p parent-column) nil)
      (remhash "parent_key" (database-indexes database))
      (rove:ok (rove:signals (validate-catalog-foreign-key-dependencies database) 'pg-error))))

(rove:deftest parse-foreign-key-definitions
    (let* ((create (first (parse-statements
                           "CREATE TABLE child (a int, b int REFERENCES parent NOT NULL, CONSTRAINT fk FOREIGN KEY (a,b) REFERENCES parent (y,x) MATCH FULL ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED)")))
           (keys (getf create :constraints))
           (inline (first keys))
           (named (second keys)))
      (rove:ok (equal '("b") (getf inline :columns)))
      (rove:ok (getf (second (getf create :columns)) :notnull))
      (rove:ok (equal "fk" (getf named :name)))
      (rove:ok (equal '("y" "x") (getf named :referenced-columns)))
      (rove:ok (equal '(:full :cascade :restrict t t)
                      (mapcar (lambda (key) (getf named key))
                              '(:match-type :delete-action :update-action :deferrable :initially-deferred)))))
  (let* ((alter (first (parse-statements
                        "ALTER TABLE child ADD FOREIGN KEY (a) REFERENCES parent ON DELETE SET NULL NOT VALID")))
         (key (getf alter :foreign-key)))
    (rove:ok (eq :add-foreign-key (getf alter :action)))
    (rove:ok (eq :set-null (getf key :delete-action)))
    (rove:ok (getf key :not-valid)))
  (rove:ok (rove:signals
            (parse-statements "CREATE TABLE child (a int REFERENCES parent ON DELETE NO ACTION ON DELETE CASCADE)")
            'pg-error)))

(rove:deftest parse-copy-stdin-with-explicit-columns
    (let* ((ast (first (parse-statements "COPY public.child (b, a) FROM STDIN")))
           (table (getf ast :table)))
      (rove:ok (eq :copy-from (getf ast :type)))
      (rove:ok (string= "public" (sql-relation-name-schema table)))
      (rove:ok (string= "child" (sql-relation-name-name table)))
      (rove:ok (equal '("b" "a") (getf ast :columns))))
  (rove:ok (eq :copy-to (getf (first (parse-statements "COPY (SELECT 1) TO STDOUT")) :type)))
  (rove:ok (rove:signals (parse-statements "COPY child (a,a) FROM STDIN") 'pg-error)))

(rove:deftest referenced-key-locks-share-wait-and-cancel
    (let* ((child (make-backend :xid 991001))
           (second-child (make-backend :xid 991002))
           (parent (make-backend :xid 991003 :executing t))
           (key (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(1 2)))
           (worker nil))
      (labels ((start-parent ()
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* parent))
                      (handler-case
                          (progn (acquire-referenced-key-lock 994001 key :exclusive) :granted)
                        (pg-error (condition) (pg-error-sqlstate condition)))))))
               (wait-for-parent ()
                 (loop repeat 200
                       when (sb-thread:with-mutex (*relation-lock-mutex*)
                              (equal '(991001 991002)
                                     (sort (mapcar #'referenced-key-wait-xid
                                                   (gethash parent *relation-wait-dependencies*)) #'<)))
                         return t
                       do (sleep 0.005))))
        (unwind-protect
             (progn
               (let ((*backend* child)) (acquire-referenced-key-lock 994001 key :shared))
               (let ((*backend* second-child)) (acquire-referenced-key-lock 994001 (copy-seq key) :shared))
               (setf worker (start-parent))
               (rove:ok (wait-for-parent))
               (cancel-backend parent)
               (rove:ok (equal "57014" (sb-thread:join-thread worker :timeout 5 :default :timeout)))
               (setf worker (start-parent))
               (rove:ok (wait-for-parent))
               (release-relation-writer-xid (backend-xid child))
               (rove:ok (sb-thread:thread-alive-p worker))
               (release-relation-writer-xid (backend-xid second-child))
               (rove:ok (eq :granted (sb-thread:join-thread worker :timeout 5 :default :timeout))))
          (dolist (backend (list child second-child parent))
            (release-relation-writer-xid (backend-xid backend)))
          (when (and worker (sb-thread:thread-alive-p worker))
            (cancel-backend parent)
            (sb-thread:join-thread worker :timeout 5 :default :timeout))))))

(rove:deftest referenced-key-lock-upgrade-detects-deadlock
    (let* ((left (make-backend :xid 991004 :executing t))
           (right (make-backend :xid 991005))
           (key (make-array 1 :element-type '(unsigned-byte 8) :initial-element 3))
           (worker nil))
      (unwind-protect
           (progn
             (sb-thread:with-mutex (*xid-lock*)
               (setf (gethash 991004 *active-xid-backends*) left
                     (gethash 991005 *active-xid-backends*) right))
             (dolist (backend (list left right))
               (let ((*backend* backend)) (acquire-referenced-key-lock 994001 key :shared)))
             (setf worker (sb-thread:make-thread
                           (lambda ()
                             (let ((*backend* left))
                               (acquire-referenced-key-lock 994001 key :exclusive)
                               :granted))))
             (rove:ok (loop repeat 200
                            when (sb-thread:with-mutex (*relation-lock-mutex*)
                                   (equal '(991005)
                                          (mapcar #'referenced-key-wait-xid
                                                  (gethash left *relation-wait-dependencies*))))
                              return t
                            do (sleep 0.005)))
             (let ((*backend* right)
                   (deadline (+ (get-internal-real-time) (* 2 internal-time-units-per-second))))
               (setf (backend-statement-deadline right) deadline)
               (rove:ok (equal "40P01"
                               (handler-case
                                   (acquire-referenced-key-lock 994001 key :exclusive)
                                 (pg-error (condition) (pg-error-sqlstate condition))))))
             (release-relation-writer-xid 991005)
             (rove:ok (eq :granted (sb-thread:join-thread worker :timeout 5 :default :timeout))))
        (release-relation-writer-xid 991005)
        (when worker (sb-thread:join-thread worker :timeout 5 :default :timeout))
        (release-relation-writer-xid 991004))))

(rove:deftest foreign-key-savepoint-restores-events-and-grants
    (let* ((*backend* (make-backend :xid 991006))
           (key (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
           (younger (make-array 1 :element-type '(unsigned-byte 8) :initial-element 8))
           (index 994001)
           (shared-wait (make-referenced-key-wait :identity (cons index key) :xid 991006))
           (exclusive-wait (make-referenced-key-wait :identity (cons index key) :xid 991006 :exclusive-p t)))
      (unwind-protect
           (progn
             (acquire-referenced-key-lock index key :shared)
             (setf (backend-deferred-foreign-key-checks *backend*) (list :ancestor)
                   (backend-foreign-key-constraint-modes *backend*) (list (cons :all :immediate)))
             (let ((checkpoint (capture-foreign-key-savepoint-state *backend*)))
               (acquire-referenced-key-lock index key :exclusive)
               (acquire-referenced-key-lock index younger :exclusive)
               (push :younger (backend-deferred-foreign-key-checks *backend*))
               (push (cons :all :deferred) (backend-foreign-key-constraint-modes *backend*))
               (rove:ok (= 991006 (sb-thread:with-mutex (*relation-lock-mutex*)
                                    (referenced-key-wait-owner-xid shared-wait))))
               (restore-foreign-key-savepoint-state *backend* checkpoint)
               (rove:ok (equal '(:ancestor) (backend-deferred-foreign-key-checks *backend*)))
               (rove:ok (equal '((:all . :immediate)) (backend-foreign-key-constraint-modes *backend*)))
               (sb-thread:with-mutex (*relation-lock-mutex*)
                 (rove:ok (null (referenced-key-wait-owner-xid shared-wait)))
                 (rove:ok (= 991006 (referenced-key-wait-owner-xid exclusive-wait)))
                 (rove:ok (null (gethash (cons index younger) *referenced-key-grants*))))))
        (release-relation-writer-xid 991006))))

(rove:deftest parse-subtransaction-command-names
    (let ((statements (parse-statements "SAVEPOINT p; ROLLBACK WORK TO SAVEPOINT p; RELEASE p")))
      (rove:ok (equal '(:savepoint :rollback-to-savepoint :release-savepoint)
                      (mapcar (lambda (statement) (getf statement :type)) statements)))
      (rove:ok (every (lambda (statement) (string= "p" (getf statement :name))) statements))))

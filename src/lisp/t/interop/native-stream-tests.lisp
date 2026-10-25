(defpackage #:pg-interop-native-tests (:use #:cl #:rove))
(in-package #:pg-interop-native-tests)

(defun wait-native-rows (client query expected)
  (loop repeat 150
        when (equalp expected (pg::pg-client-query client query)) return t
          do (sleep 0.1)))

(defun check-native-c-subscriber (copy-data)
  ;; These must be isolated task-owned peers. Full crash/concurrent-copy
  ;; acceptance remains the responsibility of t/logical-interop.sh.
  (let* ((publisher-port (parse-integer
                          (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                              (error "PG_INTEROP_TARGET_PORT is required"))))
         (subscriber-port (parse-integer
                           (or (uiop:getenv "PG_INTEROP_REFERENCE_PORT")
                               (error "PG_INTEROP_REFERENCE_PORT is required"))))
         (source (pg::pg-client-connect "127.0.0.1" publisher-port "postgres"))
         (target (pg::pg-client-connect
                  "127.0.0.1" subscriber-port "postgres"
                  :user (or (uiop:getenv "PG_INTEROP_REFERENCE_USER") "postgres"))))
    (unwind-protect
         (progn
           (dolist (client (list source target))
             (pg::pg-client-query
              client "CREATE TABLE native_stream_rows (id integer PRIMARY KEY, amount bigint, label varchar(100)); CREATE TABLE native_stream_other (id integer PRIMARY KEY, amount bigint, label varchar(100)); CREATE TABLE native_stream_excluded (id integer PRIMARY KEY)"))
           (pg::pg-client-query
            source "CREATE PUBLICATION native_stream_pub FOR TABLE native_stream_rows, native_stream_other WITH (publish = 'insert, update, delete, truncate')")
           (when copy-data
             (pg::pg-client-query
              source "INSERT INTO native_stream_rows VALUES (1, 10, 'first'), (2, 20, 'second')"))
           (pg::pg-client-query
            target (format nil "CREATE SUBSCRIPTION native_stream_sub CONNECTION 'host=127.0.0.1 port=~d dbname=postgres user=postgres application_name=native_stream_sub' PUBLICATION native_stream_pub WITH (copy_data = ~a, streaming = off, binary = false, two_phase = false, failover = false, origin = 'any')" publisher-port (if copy-data "true" "false")))
           (ok (equalp '(#("f" "f" "d" "f" "any"))
                       (pg::pg-client-query
                        target "SELECT substream, subbinary, subtwophasestate, subfailover, suborigin FROM pg_subscription WHERE subname = 'native_stream_sub'")))
           (ok (wait-native-rows
                target "SELECT srsubstate FROM pg_subscription_rel JOIN pg_subscription ON oid = srsubid WHERE subname = 'native_stream_sub' ORDER BY srrelid"
                '(#("r") #("r"))))
           (unless copy-data
             (pg::pg-client-query
              source "INSERT INTO native_stream_rows VALUES (1, 10, 'first'), (2, 20, 'second')"))
           (ok (wait-native-rows
                target "SELECT id, amount, label FROM native_stream_rows ORDER BY id"
                '(#("1" "10" "first") #("2" "20" "second"))))
           (pg::pg-client-query
            source "BEGIN; INSERT INTO native_stream_rows VALUES (3, 30, 'third'); UPDATE native_stream_rows SET amount = amount + 7 WHERE id = 1; UPDATE native_stream_rows SET id = 4 WHERE id = 2; DELETE FROM native_stream_rows WHERE id = 3; COMMIT; BEGIN; INSERT INTO native_stream_rows VALUES (99, 99, 'abort'); ROLLBACK; INSERT INTO native_stream_excluded VALUES (1); BEGIN; TRUNCATE TABLE native_stream_other; INSERT INTO native_stream_other VALUES (5, 50, 'after truncate'); COMMIT")
           (ok (wait-native-rows
                target "SELECT id, amount, label FROM native_stream_rows ORDER BY id"
                '(#("1" "17" "first") #("4" "20" "second"))))
           (ok (wait-native-rows
                target "SELECT id, amount, label FROM native_stream_other ORDER BY id"
                '(#("5" "50" "after truncate")))
               "a TRUNCATE-first relation is announced before its T frame")
           (ok (equalp '(#("0"))
                       (pg::pg-client-query target "SELECT count(*) FROM native_stream_excluded")))
           (pg::pg-client-query source "ALTER PUBLICATION native_stream_pub SET (publish = 'insert')")
           (pg::pg-client-query
            source "UPDATE native_stream_rows SET amount = 999 WHERE id = 1; INSERT INTO native_stream_rows VALUES (9, 90, 'insert-only')")
           (ok (wait-native-rows target "SELECT id FROM native_stream_rows WHERE id = 9"
                                 '(#("9"))))
           (ok (equalp '(#("17"))
                       (pg::pg-client-query target "SELECT amount FROM native_stream_rows WHERE id = 1"))
               "publication actions select the source stream")
           (pg::pg-client-query target "ALTER SUBSCRIPTION native_stream_sub DISABLE")
           (pg::pg-client-query source "INSERT INTO native_stream_rows VALUES (6, 60, 'reconnect')")
           (pg::pg-client-query target "ALTER SUBSCRIPTION native_stream_sub ENABLE")
           (ok (wait-native-rows target "SELECT id FROM native_stream_rows ORDER BY id"
                                 '(#("1") #("4") #("6") #("9"))))
           (pg::pg-client-query target "DROP SUBSCRIPTION native_stream_sub")
           (ok (equalp '(#("0"))
                       (pg::pg-client-query
                        source "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'native_stream_sub'"))))
      (ignore-errors (pg::pg-client-query target "DROP SUBSCRIPTION IF EXISTS native_stream_sub"))
      (ignore-errors (pg::pg-client-query source "DROP PUBLICATION IF EXISTS native_stream_pub"))
      (dolist (client (list source target))
        (dolist (name '("native_stream_rows" "native_stream_other" "native_stream_excluded"))
          (ignore-errors
           (pg::pg-client-query client (format nil "DROP TABLE IF EXISTS ~a" name)))))
      (pg::pg-client-close source)
      (pg::pg-client-close target))))

(deftest native-c-subscriber-row-stream
    (check-native-c-subscriber nil))

(deftest native-c-subscriber-initial-copy
    (check-native-c-subscriber t))

(deftest native-c-subscriber-historic-publication-selection
    (let* ((publisher-port (parse-integer (or (uiop:getenv "PG_INTEROP_TARGET_PORT") (error "target port required"))))
           (subscriber-port (parse-integer (or (uiop:getenv "PG_INTEROP_REFERENCE_PORT") (error "C port required"))))
           (source (pg::pg-client-connect "127.0.0.1" publisher-port "postgres"))
           (peer (pg::pg-client-connect "127.0.0.1" publisher-port "postgres"))
           (target (pg::pg-client-connect "127.0.0.1" subscriber-port "postgres"
                                          :user (or (uiop:getenv "PG_INTEROP_REFERENCE_USER") "postgres"))))
      (labels ((send (sql) (pg::pg-client-query source sql))
               (pause () (pg::pg-client-query target "ALTER SUBSCRIPTION native_history_sub DISABLE"))
               (resume () (pg::pg-client-query target "ALTER SUBSCRIPTION native_history_sub ENABLE"))
               (ids (table expected)
                 (wait-native-rows target (format nil "SELECT id FROM ~a ORDER BY id" table)
                                   (mapcar (lambda (id) (vector (write-to-string id))) expected))))
        (unwind-protect
             (progn
               (dolist (client (list source target))
                 (pg::pg-client-query client
                                      "CREATE TABLE native_history_rows (id integer PRIMARY KEY, amount bigint, label varchar(100)); CREATE TABLE native_history_other (id integer PRIMARY KEY, amount bigint, label varchar(100))"))
               (send "CREATE PUBLICATION native_history_p FOR TABLE native_history_rows, native_history_other")
               (send "CREATE PUBLICATION native_history_q FOR TABLE native_history_rows WITH (publish='delete')")
               (send "INSERT INTO native_history_rows VALUES (1,1,'initial'); INSERT INTO native_history_other VALUES (1,1,'initial')")
               (pg::pg-client-query target
                                    (format nil "CREATE SUBSCRIPTION native_history_sub CONNECTION 'host=127.0.0.1 port=~d dbname=postgres user=postgres' PUBLICATION native_history_p,native_history_q WITH (copy_data=true,streaming=off,binary=false,two_phase=false,failover=false,origin='any')" publisher-port))
               (ok (ids "native_history_rows" '(1)))
               (ok (ids "native_history_other" '(1)))
               (pause)
               (send "INSERT INTO native_history_rows VALUES (2,2,'published backlog')")
               (send "ALTER PUBLICATION native_history_p DROP TABLE native_history_rows")
               (send "INSERT INTO native_history_rows VALUES (3,3,'unpublished backlog')")
               (send "ALTER PUBLICATION native_history_p ADD TABLE native_history_rows")
               (send "INSERT INTO native_history_rows VALUES (4,4,'published again')")
               (resume)
               (ok (ids "native_history_rows" '(1 2 4))
                   "membership edits preserve old inclusion and exclusion")
               (pause)
               (pg::pg-client-query peer "BEGIN; INSERT INTO native_history_rows VALUES (10,10,'before action edit')")
               (send "ALTER PUBLICATION native_history_p SET (publish='delete')")
               (pg::pg-client-query peer "INSERT INTO native_history_rows VALUES (11,11,'after action edit'); COMMIT")
               (send "ALTER PUBLICATION native_history_p SET (publish='insert,update,delete,truncate')")
               (send "INSERT INTO native_history_rows VALUES (13,13,'action barrier')")
               (resume)
               (ok (ids "native_history_rows" '(1 2 4 10 13))
                   "publication actions can change between writes in one transaction")
               (pause)
               (send "ALTER PUBLICATION native_history_q SET (publish='insert,delete')")
               (send "ALTER PUBLICATION native_history_p SET (publish='delete')")
               (send "INSERT INTO native_history_rows VALUES (12,12,'second publication'); DELETE FROM native_history_rows WHERE id=4")
               (send "ALTER PUBLICATION native_history_p SET (publish='insert,update,delete,truncate')")
               (send "ALTER PUBLICATION native_history_q SET (publish='delete')")
               (send "INSERT INTO native_history_rows VALUES (14,14,'old publication object')")
               (send "DROP PUBLICATION native_history_p")
               (send "CREATE PUBLICATION native_history_p FOR TABLE native_history_other")
               (send "INSERT INTO native_history_rows VALUES (15,15,'missing from replacement')")
               (send "ALTER PUBLICATION native_history_p ADD TABLE native_history_rows")
               (send "INSERT INTO native_history_rows VALUES (16,16,'new publication object'),(17,17,'identity barrier')")
               (resume)
               (ok (ids "native_history_rows" '(1 2 10 12 13 14 16 17))
                   "requested publication names retain historic meaning across recreation")
               (pause)
               (send "TRUNCATE TABLE native_history_rows,native_history_other")
               (send "ALTER PUBLICATION native_history_p DROP TABLE native_history_other")
               (send "INSERT INTO native_history_other VALUES (30,30,'excluded after truncate')")
               (send "ALTER PUBLICATION native_history_p ADD TABLE native_history_other")
               (send "INSERT INTO native_history_other VALUES (31,31,'included after truncate'); INSERT INTO native_history_rows VALUES (32,32,'truncate barrier')")
               (resume)
               (ok (ids "native_history_rows" '(32)))
               (ok (ids "native_history_other" '(31))
                   "both old TRUNCATE relations retain their historic membership")
               (pause)
               (send "ALTER PUBLICATION native_history_p SET (publish='insert')")
               (send "TRUNCATE TABLE native_history_rows,native_history_other")
               (send "INSERT INTO native_history_rows VALUES (33,33,'after filtered truncate'); INSERT INTO native_history_other VALUES (34,34,'after filtered truncate')")
               (send "ALTER PUBLICATION native_history_p SET (publish='insert,update,delete,truncate')")
               (send "INSERT INTO native_history_rows VALUES (35,35,'final barrier')")
               (resume)
               (ok (ids "native_history_rows" '(32 33 35)))
               (ok (ids "native_history_other" '(31 34))
                   "a previously excluded TRUNCATE must stay excluded"))
          (ignore-errors (pg::pg-client-query peer "ROLLBACK"))
          (ignore-errors (pg::pg-client-query target "DROP SUBSCRIPTION IF EXISTS native_history_sub"))
          (dolist (name '("native_history_p" "native_history_q"))
            (ignore-errors (send (format nil "DROP PUBLICATION IF EXISTS ~a" name))))
          (dolist (client (list source target))
            (dolist (name '("native_history_rows" "native_history_other"))
              (ignore-errors (pg::pg-client-query client (format nil "DROP TABLE IF EXISTS ~a" name)))))
          (pg::pg-client-close peer)
          (pg::pg-client-close source)
          (pg::pg-client-close target)))))

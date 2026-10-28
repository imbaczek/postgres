"""Compare UPDATE FROM and DELETE USING with the owned C server."""
import sys
from pathlib import Path
from subqueries import QueryConnection
from joined_dml_concurrency import check as check_concurrency
from joined_dml_resources import check as check_resources

SETUP = (
    "CREATE TABLE jd_target(id int PRIMARY KEY,v int,label varchar(20))",
    "CREATE TABLE jd_source(k int,delta int,label varchar(20))",
    "INSERT INTO jd_target VALUES(1,10,'a'),(2,20,'b'),(3,30,'c')",
    "INSERT INTO jd_source VALUES(1,100,'x'),(2,200,'y'),(4,400,'z')",
    "CREATE SEQUENCE jd_seq",
)
CASES = (
    "UPDATE jd_target AS t SET v=t.v+s.delta FROM jd_source AS s WHERE t.id=s.k RETURNING t.id,t.v,s.delta,s.label,old.v,new.v",
    "UPDATE jd_target AS t SET label=s.label FROM jd_source s WHERE t.id=s.k RETURNING *",
    "UPDATE jd_target t SET v=(SELECT s.delta+t.id) FROM jd_source s WHERE t.id=s.k RETURNING t.*,s.*,(SELECT s.label)",
    "UPDATE jd_target t SET v=s.delta FROM (SELECT k,delta FROM jd_source WHERE k=1) s WHERE t.id=s.k RETURNING *",
    "WITH q AS(SELECT k,delta FROM jd_source) UPDATE jd_target t SET v=q.delta FROM q WHERE t.id=q.k RETURNING t.id,q.delta",
    "UPDATE jd_target t SET v=u.delta FROM jd_source s LEFT JOIN jd_source u ON u.k=s.k+1 WHERE t.id=s.k RETURNING t.id,s.k,u.k,t.v",
    "UPDATE jd_target t SET v=s.delta FROM jd_source s JOIN jd_source u USING(k) WHERE t.id=s.k RETURNING t.id,s.k,u.k",
    "UPDATE jd_target t SET v=s.delta FROM jd_source s CROSS JOIN LATERAL(SELECT s.k AS n) q WHERE t.id=q.n RETURNING t.id,q.n",
    "UPDATE jd_target t SET v=s.v FROM jd_target s WHERE t.id=s.id RETURNING t.id,s.v",
    "SELECT setval('jd_seq',1,false)",
    "UPDATE jd_target t SET v=0 FROM (SELECT nextval('jd_seq') AS n) s WHERE false RETURNING t.id",
    "SELECT currval('jd_seq')",
    "DELETE FROM jd_target t USING (SELECT nextval('jd_seq') AS n) s WHERE false RETURNING t.id",
    "SELECT currval('jd_seq')",
    "UPDATE jd_target t SET v=0 FROM (SELECT nextval('jd_seq') AS n) s WHERE (SELECT false) RETURNING t.id",
    "SELECT currval('jd_seq')",
    "DELETE FROM jd_target t USING (SELECT nextval('jd_seq') AS n) s WHERE current_database()='absent_db' RETURNING t.id",
    "SELECT currval('jd_seq')",
    "UPDATE jd_target t SET v=s.n FROM (SELECT nextval('jd_seq') AS n) s RETURNING t.id,t.v",
    "SELECT currval('jd_seq')",
    "UPDATE jd_target t SET v=delta FROM jd_source s FULL JOIN jd_source u USING(k) WHERE t.id=k RETURNING t.id,k",
    "UPDATE jd_target t SET v=s.delta FROM jd_source s RIGHT JOIN jd_source u USING(k) WHERE t.id=k RETURNING t.id,k",
    "UPDATE jd_target t SET v=s.delta FROM (SELECT k,sum(delta) AS delta FROM jd_source GROUP BY k) s WHERE t.id=s.k RETURNING t.id,s.delta",
    "WITH RECURSIVE q(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM q WHERE n<2) UPDATE jd_target t SET v=q.n FROM q WHERE t.id=q.n RETURNING t.id,q.n",
    "UPDATE jd_target SET v=s.delta FROM jd_source s WHERE false RETURNING *",
    "UPDATE jd_target t SET v=s.delta FROM jd_source s WHERE t.id=s.k RETURNING WITH(OLD AS o,NEW AS n) o.id,n.v,s.label",
    "UPDATE jd_target t SET v=(SELECT 1 UNION ALL SELECT 2) FROM jd_source s WHERE t.id=s.k RETURNING *",
    "SELECT * FROM jd_target ORDER BY id",
    "UPDATE jd_target t SET v=1 FROM jd_source t",
    "UPDATE jd_target t SET v=1 FROM jd_source t CROSS JOIN LATERAL(SELECT t.k) q",
    "UPDATE jd_target t SET v=id FROM jd_source s",
    "UPDATE jd_target t SET v=k FROM jd_source s RETURNING missing",
    "UPDATE jd_target t SET v=q.n FROM (SELECT t.id AS n) q",
    "UPDATE jd_target t SET v=q.n FROM LATERAL(SELECT t.id AS n) q",
    "UPDATE jd_target t SET v=q.n FROM LATERAL(SELECT id AS n) q",
    "UPDATE jd_target t SET v=q.id FROM LATERAL(SELECT t.*) q",
    "UPDATE jd_target t SET v=q.n FROM LATERAL(SELECT (SELECT t.id) AS n) q",
    "UPDATE jd_target t SET v=q.n FROM (SELECT x.n FROM LATERAL(SELECT t.id AS n) x) q",
    "UPDATE jd_target t SET v=q.id FROM (SELECT 1 AS id) s CROSS JOIN LATERAL(SELECT id) q WHERE t.id=q.id RETURNING t.id",
    "UPDATE jd_target t SET v=q.id FROM (SELECT 1 AS id) s CROSS JOIN LATERAL(SELECT s.id) q WHERE t.id=q.id RETURNING t.id",
    "UPDATE jd_target t SET v=jd_target.v FROM jd_source s",
    "CREATE TABLE jd_dup(k int,delta int)",
    "INSERT INTO jd_dup VALUES(1,5),(1,5),(2,5),(2,5)",
    "SELECT setval('jd_seq',1,false)",
    "UPDATE jd_target t SET v=nextval('jd_seq') FROM jd_dup s WHERE t.id=s.k RETURNING t.id",
    "SELECT currval('jd_seq')",
    "UPDATE jd_target SET v=id",
    "CREATE TABLE jd_identical(v int)",
    "INSERT INTO jd_identical VALUES(1),(1)",
    "UPDATE jd_identical t SET v=2 FROM jd_dup s WHERE s.k=1 RETURNING t.v",
    "SELECT * FROM jd_identical",
    "DELETE FROM jd_identical t USING jd_dup s WHERE s.k=1 RETURNING t.*,s.*",
    "DELETE FROM jd_target t USING jd_source s WHERE t.id=s.k RETURNING t.*,s.*,old.id,new.id,(SELECT s.delta)",
    "SELECT * FROM jd_target ORDER BY id",
    "INSERT INTO jd_target VALUES(1,10,'a'),(2,20,'b')",
    "DELETE FROM jd_target t USING jd_source s LEFT JOIN jd_source u ON u.k=s.k+1 WHERE t.id=s.k RETURNING *",
    "INSERT INTO jd_target VALUES(1,10,'a'),(2,20,'b')",
    "WITH q AS(SELECT k FROM jd_source) DELETE FROM jd_target t USING q WHERE t.id=q.k RETURNING t.id,q.k",
    "DELETE FROM jd_target t USING jd_source t",
    "DELETE FROM jd_target t USING LATERAL(SELECT t.id) q",
    "DELETE FROM jd_target t USING (SELECT t.id) q",
    "DELETE FROM jd_target t USING jd_source s WHERE false RETURNING *",
)

def check(reference, native):
    peers = [QueryConnection(port, "joined-dml") for port in (reference, native)]
    failures = []
    try:
        for sql in SETUP:
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
        for index, sql in enumerate(CASES, 1):
            outcomes = [peer.observe(sql) for peer in peers]
            # SQL does not promise an ordering for RETURNING or unordered SELECT.
            normalized = [(value[0], value[1], sorted(value[2], key=repr), value[3])
                          if value[0] == "rows" else value for value in outcomes]
            if normalized[0] != normalized[1]:
                failures.append((index, sql, outcomes))
                print("joined-dml: FAIL", failures[-1], flush=True)
        assert not failures, failures
        print(f"joined-dml: {len(CASES)} C comparisons PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()

if __name__ == "__main__":
    check(int(sys.argv[1]), int(sys.argv[2]))
    check_concurrency(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
    check_resources(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))

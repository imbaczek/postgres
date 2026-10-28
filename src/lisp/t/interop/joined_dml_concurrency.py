"""Recheck a changed target while retaining its original source row."""
from concurrent.futures import ThreadPoolExecutor
from subqueries import QueryConnection
from subqueries_concurrency import wait_for_lock
from dml_concurrency import cleanup, finish


def scenario(port, native, base, socket, action, kind):
    writer, mutation, admin = peers = [QueryConnection(port, "joined-dml-wait") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=1)
    pending = []
    try:
        for sql in ("DROP TABLE IF EXISTS jdc_target", "DROP TABLE IF EXISTS jdc_source",
                    "CREATE TABLE jdc_target(id int PRIMARY KEY,k int,v int)",
                    "CREATE TABLE jdc_source(k int,delta int)",
                    "INSERT INTO jdc_target VALUES(1,1,10)",
                    "INSERT INTO jdc_source VALUES(1,5),(2,7)"):
            admin.query(sql)
        mutation.query("BEGIN ISOLATION LEVEL REPEATABLE READ" if kind == "rr" else "BEGIN")
        mutation.query("SELECT count(*) FROM jdc_target")
        writer.query("BEGIN")
        writer.query("UPDATE jdc_target SET v=20" + (",k=2" if kind == "later-match" else ",k=3" if kind == "new-source" else ""))
        if kind == "source-update":
            writer.query("UPDATE jdc_source SET delta=50 WHERE k=1")
        if kind == "source-delete":
            writer.query("DELETE FROM jdc_source WHERE k=1")
        if kind == "new-source":
            writer.query("INSERT INTO jdc_source VALUES(3,9)")
        condition = "t.k<=s.k" if kind == "later-match" else "t.k=s.k"
        sql = (f"UPDATE jdc_target t SET v=t.v+s.delta FROM jdc_source s WHERE {condition} RETURNING t.id,old.v,new.v,s.delta"
               if action == "update" else
               f"DELETE FROM jdc_target t USING jdc_source s WHERE {condition} RETURNING t.id,t.v,s.delta")
        future = pool.submit(mutation.observe, sql)
        pending.append((future, mutation))
        wait_for_lock(admin, future, mutation.backend_pid, native, base, socket)
        writer.query("ROLLBACK" if kind == "rollback" else "COMMIT")
        outcome = future.result(timeout=20)
        if kind == "rr":
            assert outcome == ("error", "40001"), outcome
        elif kind == "new-source":
            assert outcome[0] == "rows" and outcome[2] == [] and outcome[3] == [action.upper()+" 0"], outcome
        else:
            delta = "7" if kind == "later-match" else "5"
            old = "10" if kind == "rollback" else "20"
            expected = [["1", old, str(int(old)+int(delta)), delta]] if action == "update" else [["1", old, delta]]
            assert outcome[0] == "rows" and outcome[2] == expected, (action, kind, outcome)
        finish(mutation, outcome)
        return outcome, admin.query("SELECT id,k,v FROM jdc_target"), admin.query("SELECT k,delta FROM jdc_source ORDER BY k")
    finally:
        cleanup(peers, admin, pool, pending)


def check(reference, native, base, socket):
    count = 0
    for action in ("update", "delete"):
        for kind in ("value", "source-update", "source-delete", "new-source", "later-match", "rr", "rollback"):
            outcomes = [scenario(port, native_p, base, socket, action, kind)
                        for port, native_p in ((reference, False), (native, True))]
            assert outcomes[0] == outcomes[1], (action, kind, outcomes)
            count += 1
            print(f"joined-dml: concurrency {action}/{kind} PASS", flush=True)
    print(f"joined-dml: {count} controlled concurrency comparisons PASS", flush=True)

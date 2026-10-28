"""CHECK validation barriers and final EvalPlanQual row enforcement."""
from concurrent.futures import ThreadPoolExecutor
from subqueries import QueryConnection
from subqueries_concurrency import wait_for_lock
from dml_concurrency import cleanup, finish


def scenario(port, native, base, socket, kind, release):
    writer, mutation, admin = peers = [QueryConnection(port, "check-barrier") for _ in range(3)]
    pool = ThreadPoolExecutor(max_workers=1)
    pending = []
    try:
        admin.query("DROP TABLE IF EXISTS dc_concurrent")
        admin.query("CREATE TABLE dc_concurrent(a int,b int)")
        if kind == "validate":
            admin.query("INSERT INTO dc_concurrent VALUES(-1,0)")
            admin.query("ALTER TABLE dc_concurrent ADD CONSTRAINT positive CHECK(a>0) NOT VALID")
        elif kind == "epq":
            admin.query("INSERT INTO dc_concurrent VALUES(1,1)")
            admin.query("ALTER TABLE dc_concurrent ADD CONSTRAINT total CHECK(a+b<10)")
        writer.query("BEGIN")
        if kind == "add":
            writer.query("INSERT INTO dc_concurrent VALUES(-1,0)")
            sql = "ALTER TABLE dc_concurrent ADD CONSTRAINT positive CHECK(a>0)"
        elif kind == "validate":
            writer.query("DELETE FROM dc_concurrent")
            sql = "ALTER TABLE dc_concurrent VALIDATE CONSTRAINT positive"
        else:
            writer.query("UPDATE dc_concurrent SET a=8")
            mutation.query("BEGIN")
            sql = "UPDATE dc_concurrent SET b=3 RETURNING a,b"
        future = pool.submit(mutation.observe, sql)
        pending.append((future, mutation))
        if kind == "validate":
            # C's VALIDATE lock permits DML. Its snapshot still sees the
            # uncommitted deletion's old invalid row, so this fails promptly.
            assert future.result(timeout=20) == ("error", "23514")
        else:
            wait_for_lock(admin, future, mutation.backend_pid, native, base, socket)
        writer.query(release)
        outcome = future.result(timeout=20)
        should_fail = (kind == "validate" or
                       (kind in ("add", "epq") and release == "COMMIT"))
        assert (outcome == ("error", "23514")) if should_fail else outcome[0] == "rows", (kind, release, outcome)
        if kind == "epq":
            finish(mutation, outcome)
        # A failed ADD must leave the table unconstrained. A failed VALIDATE
        # keeps its old NOT VALID check enforcing new rows.
        retry = (mutation.observe(sql) if kind == "validate" else None)
        follow = admin.observe("INSERT INTO dc_concurrent VALUES(-2,0)")
        return outcome, retry, follow, admin.query("SELECT a,b FROM dc_concurrent ORDER BY a,b")
    finally:
        cleanup(peers, admin, pool, pending)


def check(reference, native, base, socket):
    for kind in ("add", "validate", "epq"):
        for release in ("COMMIT", "ROLLBACK"):
            outcomes = [scenario(port, native_p, base, socket, kind, release)
                        for port, native_p in ((reference, False), (native, True))]
            assert outcomes[0] == outcomes[1], (kind, release, outcomes)
            print(f"defaults-checks: concurrency {kind}/{release} PASS", flush=True)

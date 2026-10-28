"""Decimal values retain scale through spill, replay and window buffers."""
import re
from subqueries import QueryConnection
from subqueries_concurrency import control, spill_counter, assert_no_spill_files


def scenario(port, native, base, socket):
    peer = QueryConnection(port, "aggregate-resources")
    old_memory = old_limit = None
    try:
        peer.observe("DROP TABLE IF EXISTS dc_resources")
        peer.query("CREATE TABLE dc_resources(g int,x int)")
        peer.query("INSERT INTO dc_resources VALUES " + ",".join(
            f"({i % 40},{'NULL' if i % 7 == 0 else i - 500})" for i in range(1000)))
        if native:
            settings = control(base, socket, '(format t "AG_MEMORY=~d~%AG_LIMIT=~d~%" pg::*query-work-memory-bytes* (or pg::*query-temp-file-limit-bytes* -1))')
            old_memory = int(re.search(r"AG_MEMORY=(\d+)", settings)[1])
            old_limit = int(re.search(r"AG_LIMIT=(-?\d+)", settings)[1])
            control(base, socket, '(setf pg::*query-work-memory-bytes* 8192)')
            before = spill_counter(base, socket)
        queries = (
            "WITH q AS MATERIALIZED(SELECT g,avg(x) AS a FROM dc_resources GROUP BY g) SELECT a,count(*),min(a),max(a),sum(a) FROM q GROUP BY a ORDER BY a",
            "SELECT g,avg(x) OVER(PARTITION BY g ORDER BY x) AS a,count(x) OVER(PARTITION BY g),min(x) OVER(PARTITION BY g),max(x) OVER(PARTITION BY g) FROM dc_resources ORDER BY a,g",
            "WITH q AS MATERIALIZED(SELECT g,avg(x) AS a FROM dc_resources GROUP BY g) SELECT DISTINCT a FROM q UNION SELECT avg(x) FROM dc_resources ORDER BY 1",
            "SELECT count(CASE WHEN x IS NULL THEN false END),min(x),max(x),avg(x) FROM dc_resources",
        )
        outcomes = [peer.observe(sql) for sql in queries]
        assert all(outcome[0] == "rows" for outcome in outcomes), outcomes
        if native:
            assert spill_counter(base, socket) > before
            assert_no_spill_files(base, socket)
            control(base, socket, '(setf pg::*query-temp-file-limit-bytes* 16384)')
            quota = peer.observe("WITH q AS MATERIALIZED(SELECT avg(x) OVER(ORDER BY x) AS a FROM dc_resources) SELECT a FROM q ORDER BY a")
            assert quota == ("error", "53400"), quota
            assert_no_spill_files(base, socket)
        assert peer.query("SELECT 42") == [["42"]]
        return outcomes
    finally:
        if old_memory is not None:
            control(base, socket, f'(setf pg::*query-work-memory-bytes* {old_memory} pg::*query-temp-file-limit-bytes* {"nil" if old_limit == -1 else old_limit})')
        peer.close()


def check(reference, native, base, socket):
    outcomes = [scenario(port, native_p, base, socket)
                for port, native_p in ((reference, False), (native, True))]
    assert outcomes[0] == outcomes[1], outcomes
    print("basic-aggregates: spill/replay/windows/decimal scale, quota cleanup PASS", flush=True)

#!/bin/sh
set -eu
BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5491}
TMP=$(mktemp -d /tmp/pg-toast-dml.XXXXXX)
LOG="$TMP/server.log"
SRV=
READER=
WRITER=
. "$BASE/t/test-support.sh"
test_debug_setup toast-dml "$PORT" "$TMP" "$LOG"
cleanup() {
    exec 3>&-
    [ -z "$WRITER" ] || kill "$WRITER" 2>/dev/null || true
    [ -z "$READER" ] || kill "$READER" 2>/dev/null || true
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$WRITER" ] || wait "$WRITER" 2>/dev/null || true
    [ -z "$READER" ] || wait "$READER" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup
run() {
    PGPASSWORD=x psql -X -q -A -t -h 127.0.0.1 -p "$PORT" \
        -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" --control-socket "$TMP/control.sock"
test_wait_owned_sql_server "$SRV" "$PORT" || { cat "$LOG" >&2; exit 1; }
# A count-based split puts all three large keys on one page even though a
# boundary exists where both pages fit. Keep the values distinct and indexable.
"${PYTHON:-python3}" - >"$TMP/uneven-index.sql" <<'PY'
import random
import string
rng = random.Random(42)
suffix = ''.join(rng.choice(string.ascii_letters + string.digits) for _ in range(2200))
print('CREATE TABLE uneven_index (v text UNIQUE);')
for i in range(100):
    print(f"INSERT INTO uneven_index VALUES ('a{i:03d}');")
for i in range(3):
    print(f"INSERT INTO uneven_index VALUES ('{'z' * 10000}{suffix}{i}');")
PY
run -f "$TMP/uneven-index.sql" >/dev/null
[ "$(run -c 'select count(*) from uneven_index')" = 103 ]
# Force compressed leaf entries and internal separators with long logical keys.
# Build the index after loading rows, then exercise its unique insert path.
index_suffix=$(awk 'BEGIN { for (i=0;i<20000;i++) printf "a" }')
run -c 'create table toast_inline_provenance (id int primary key, v text unique)' >/dev/null
run -c "insert into toast_inline_provenance values (1,'$index_suffix')" >/dev/null
run -c 'alter table toast_inline_provenance alter column v set storage external' >/dev/null
run -c 'update toast_inline_provenance set id=2 where id=1' >/dev/null
run -c 'update toast_inline_provenance set v=v where id=2' >/dev/null
run -c "insert into toast_inline_provenance values (2,'short') on conflict (id) do update set v=toast_inline_provenance.v" >/dev/null
if run -v VERBOSITY=verbose -c "update toast_inline_provenance set v=v || '' where id=2" >"$TMP/inline-computed.out" 2>&1; then
    echo 'toast-dml: computed value incorrectly retained compression' >&2; exit 1
fi
case $(cat "$TMP/inline-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed value did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
[ "$(run -c 'select id,length(v) from toast_inline_provenance')" = '2|20000' ]
run -c 'alter table toast_inline_provenance alter column v set storage plain' >/dev/null
run -c 'vacuum full toast_inline_provenance' >/dev/null
[ "$(run -c 'select id,length(v) from toast_inline_provenance')" = '2|20000' ]
run -c 'create table toast_inline_build (v text)' >/dev/null
run -c "insert into toast_inline_build values ('$index_suffix')" >/dev/null
run -c 'alter table toast_inline_build alter column v set storage external' >/dev/null
run -c 'create unique index toast_inline_build_v on toast_inline_build(v)' >/dev/null
run -c 'create table toast_select_plain (v text)' >/dev/null
run -c 'alter table toast_select_plain alter column v set storage plain' >/dev/null
run -c 'insert into toast_select_plain select v from toast_inline_build' >/dev/null
run -c "insert into toast_select_plain select coalesce(v, '') from toast_inline_build" >/dev/null
run -c "insert into toast_select_plain select case when true then v else '' end from toast_inline_build" >/dev/null
[ "$(run -c 'select count(*) from toast_select_plain')" = 3 ]
run -c "insert into toast_inline_build values ('short')" >/dev/null
run -c 'create table toast_select_mixed (a text, b text)' >/dev/null
run -c 'alter table toast_select_mixed alter column a set storage plain' >/dev/null
run -c 'alter table toast_select_mixed alter column b set storage plain' >/dev/null
run -c "insert into toast_select_mixed select 'fixed', v from toast_inline_build" >/dev/null
[ "$(run -c "select count(*) from toast_select_mixed where a = 'fixed' and length(b) = 20000")" = 1 ]
[ "$(run -c "select count(*) from toast_select_mixed where a = 'fixed' and b = 'short'")" = 1 ]
# The ordered path must retain each row's detached origin through disk spill.
printf '%s\n' '(setf pg::*query-work-memory-bytes* 1024)' '(quit)' | \
    "${SBCL:-sbcl}" --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/sort-work-mem.out"
run -c 'insert into toast_select_plain select v from toast_inline_build order by v' >/dev/null
run -c 'insert into toast_select_plain select v from toast_inline_build order by v limit 1' >/dev/null
[ "$(run -c 'select count(*) from toast_select_plain where length(v)=20000')" = 5 ]
run -c 'create table toast_join_keys (id int)' >/dev/null
run -c 'insert into toast_join_keys values (1),(2),(3)' >/dev/null
run -c 'create table toast_join_result (v text)' >/dev/null
run -c 'alter table toast_join_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_join_result select s.v from toast_join_keys k left join toast_inline_build s on k.id <> 2' >/dev/null
[ "$(run -c 'select count(*) from toast_join_result where length(v)=20000')" = 2 ]
[ "$(run -c "select count(*) from toast_join_result where v='short'")" = 2 ]
[ "$(run -c 'select count(*) from toast_join_result where v is null')" = 1 ]
run -c 'insert into toast_join_result select s.v from toast_inline_build s right join toast_join_keys k on k.id=1' >/dev/null
[ "$(run -c 'select count(*) from toast_join_result where length(v)=20000')" = 3 ]
[ "$(run -c 'select count(*) from toast_join_result where v is null')" = 3 ]
run -c 'insert into toast_join_result select s.v from toast_inline_build s full join toast_join_keys k on k.id=1' >/dev/null
[ "$(run -c 'select count(*) from toast_join_result where length(v)=20000')" = 4 ]
[ "$(run -c 'select count(*) from toast_join_result where v is null')" = 5 ]
run -c 'create table toast_join_empty (v text)' >/dev/null
run -c 'insert into toast_join_result select s.v from toast_join_empty s full join toast_join_keys k on k.id=1' >/dev/null
[ "$(run -c 'select count(*) from toast_join_result where v is null')" = 8 ]
run -c 'insert into toast_join_result select s.v from toast_join_empty e full join toast_inline_build s on false' >/dev/null
[ "$(run -c 'select count(*) from toast_join_result where length(v)=20000')" = 5 ]
run -c 'create table toast_derived_result (v text)' >/dev/null
run -c 'alter table toast_derived_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_derived_result select q.v from (select v from toast_inline_build) q' >/dev/null
[ "$(run -c 'select count(*) from toast_derived_result where length(v)=20000')" = 1 ]
run -c 'create table toast_cte_result (v text)' >/dev/null
run -c 'alter table toast_cte_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_cte_result with q as materialized (select v from toast_inline_build order by v) select v from q' >/dev/null
run -c 'insert into toast_cte_result with q as not materialized (select v from toast_inline_build order by v limit 1) select v from q' >/dev/null
run -c 'insert into toast_cte_result with q as materialized (select v from toast_inline_build) select a.v from q a join q b on a.v=b.v' >/dev/null
[ "$(run -c 'select count(*) from toast_cte_result where length(v)=20000')" = 3 ]
[ "$(run -c "select count(*) from toast_cte_result where v='short'")" = 2 ]
run -c 'create table toast_cte_chain (v text)' >/dev/null
run -c 'alter table toast_cte_chain alter column v set storage plain' >/dev/null
run -c 'insert into toast_cte_chain with a as materialized (select v from toast_inline_build), b as materialized (select v from a) select v from b' >/dev/null
run -c 'insert into toast_cte_chain with a as materialized (with b as materialized (select v from toast_inline_build) select v from b) select v from a' >/dev/null
run -c 'insert into toast_cte_chain with a as not materialized (select v from toast_inline_build), b as materialized (select v from a) select v from b' >/dev/null
run -c 'insert into toast_cte_chain with recursive a(v,n) as (select v,1 from toast_inline_build union all select v,n+1 from a where n<2), b as materialized (select v from a where n=2) select v from b' >/dev/null
[ "$(run -c 'select count(*) from toast_cte_chain where length(v)=20000')" = 4 ]
[ "$(run -c "select count(*) from toast_cte_chain where v='short'")" = 4 ]
if run -v VERBOSITY=verbose -c "insert into toast_cte_chain with a as materialized (select v from toast_inline_build), b as materialized (select v || '' as v from a) select v from b" >"$TMP/cte-chain-computed.out" 2>&1; then
    echo 'toast-dml: computed chained CTE retained compression' >&2; exit 1
fi
case $(cat "$TMP/cte-chain-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed chained CTE did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_recursive_result (v text)' >/dev/null
run -c 'alter table toast_recursive_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_recursive_result with recursive q(v,n) as (select v,1 from toast_inline_build union all select v,n+1 from q where n<3) select v from q' >/dev/null
[ "$(run -c 'select count(*) from toast_recursive_result where length(v)=20000')" = 3 ]
[ "$(run -c "select count(*) from toast_recursive_result where v='short'")" = 3 ]
run -c 'create table toast_scalar_result (v text)' >/dev/null
run -c 'alter table toast_scalar_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_scalar_result select (select v) from toast_inline_build' >/dev/null
run -c "insert into toast_scalar_result select (select v from toast_inline_build where v <> 'short') from toast_join_keys" >/dev/null
run -c 'insert into toast_scalar_result select (select (select v)) from toast_inline_build' >/dev/null
[ "$(run -c 'select count(*) from toast_scalar_result where length(v)=20000')" = 5 ]
[ "$(run -c "select count(*) from toast_scalar_result where v='short'")" = 2 ]
run -c 'create table toast_scalar_source (id int, v text)' >/dev/null
run -c "insert into toast_scalar_source values (1,'$index_suffix'),(2,'b$index_suffix')" >/dev/null
run -c 'create publication toast_project_set for table toast_scalar_source' >/dev/null
run -c 'create table toast_project_set_result (v text)' >/dev/null
run -c 'alter table toast_project_set_result alter column v set storage plain' >/dev/null
run -c "insert into toast_project_set_result select v from (select v,(pg_get_publication_tables('toast_project_set')).relid as r from toast_scalar_source) q" >/dev/null
run -c "insert into toast_project_set_result with q as materialized (select v,(pg_get_publication_tables('toast_project_set')).relid as r from toast_scalar_source) select v from q" >/dev/null
run -c "insert into toast_project_set_result select distinct v from (select v,(pg_get_publication_tables('toast_project_set')).relid as r from toast_scalar_source) q" >/dev/null
[ "$(run -c "select count(*) from toast_project_set_result where (length(v)=20000 and substr(v,1,1)='a') or (length(v)=20001 and substr(v,1,1)='b')")" = 6 ]
run -c 'create publication toast_project_set_two for table toast_scalar_source,toast_join_keys' >/dev/null
run -c "insert into toast_project_set_result with q as materialized (select v,(pg_get_publication_tables('toast_project_set')).relid as a,(pg_get_publication_tables('toast_project_set_two')).relid as b from toast_scalar_source) select v from q" >/dev/null
[ "$(run -c "select count(*) from toast_project_set_result where (length(v)=20000 and substr(v,1,1)='a') or (length(v)=20001 and substr(v,1,1)='b')")" = 10 ]
run -c 'create table toast_window_result (v text, n int)' >/dev/null
run -c 'alter table toast_window_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_window_result select v,row_number() over (order by id) from toast_scalar_source' >/dev/null
run -c 'insert into toast_window_result select v,rank() over (order by v) from toast_scalar_source' >/dev/null
run -c 'insert into toast_window_result with q as materialized (select v,dense_rank() over (order by id) as n from toast_scalar_source) select v,n from q' >/dev/null
[ "$(run -c "select count(*) from toast_window_result where (n=1 and length(v)=20000 and substr(v,1,1)='a') or (n=2 and length(v)=20001 and substr(v,1,1)='b')")" = 6 ]
run -c 'create table toast_window_aggregate (v text, n int)' >/dev/null
run -c 'alter table toast_window_aggregate alter column v set storage plain' >/dev/null
run -c 'insert into toast_window_aggregate select v,count(*) over (order by id rows between unbounded preceding and current row) from toast_scalar_source' >/dev/null
run -c 'insert into toast_window_aggregate select v,min(id) over () from toast_scalar_source' >/dev/null
run -c 'insert into toast_window_aggregate with q as materialized (select min(v) over (partition by id rows between unbounded preceding and unbounded following) as v,id from toast_scalar_source) select v,id from q' >/dev/null
run -c 'insert into toast_window_aggregate with q as materialized (select v,count(*) over (order by id) as n from toast_scalar_source) select v,n from q' >/dev/null
run -c 'insert into toast_window_aggregate with q as materialized (select min(v) over (partition by id order by id) as v,id from toast_scalar_source) select v,id from q' >/dev/null
[ "$(run -c 'select count(*) from toast_window_aggregate where length(v)=20000')" = 5 ]
[ "$(run -c 'select count(*) from toast_window_aggregate where length(v)=20001')" = 5 ]
run -c 'create table toast_window_stages (a text, b text)' >/dev/null
run -c 'alter table toast_window_stages alter column a set storage plain' >/dev/null
run -c 'alter table toast_window_stages alter column b set storage plain' >/dev/null
run -c 'insert into toast_window_stages select min(v) over (partition by id),max(v) over (partition by id+0) from toast_scalar_source' >/dev/null
[ "$(run -c 'select count(*) from toast_window_stages where length(a)=length(b) and length(a)>19999')" = 2 ]
if run -v VERBOSITY=verbose -c 'insert into toast_window_aggregate select min(v) over (),id from toast_scalar_source' >"$TMP/window-min-multiple.out" 2>&1; then
    echo 'toast-dml: compared window aggregate retained compression' >&2; exit 1
fi
case $(cat "$TMP/window-min-multiple.out") in
    *54000*) ;;
    *) echo 'toast-dml: compared window aggregate did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_conflict_origins (id int primary key, a text, b text)' >/dev/null
run -c 'alter table toast_conflict_origins alter column a set storage external' >/dev/null
run -c 'alter table toast_conflict_origins alter column b set storage plain' >/dev/null
run -c "insert into toast_conflict_origins values (1,'old','small')" >/dev/null
run -c "insert into toast_conflict_origins select 1,v,'ignored' from toast_scalar_source where id=1 on conflict (id) do update set b=excluded.a" >/dev/null
run -c "insert into toast_conflict_origins values (1,'ignored','ignored') on conflict (id) do update set b=case when true then toast_conflict_origins.b else '' end" >/dev/null
run -c "insert into toast_conflict_origins values (1,'ignored','ignored') on conflict (id) do update set b=(select toast_conflict_origins.b)" >/dev/null
[ "$(run -c 'select length(b) from toast_conflict_origins where id=1')" = 20000 ]
run -c 'create table toast_conflict_narrow (id int primary key, a varchar(30000), b text)' >/dev/null
run -c 'alter table toast_conflict_narrow alter column a set storage external' >/dev/null
run -c 'alter table toast_conflict_narrow alter column b set storage plain' >/dev/null
run -c "insert into toast_conflict_narrow values (1,'old','small')" >/dev/null
if run -v VERBOSITY=verbose -c "insert into toast_conflict_narrow select 1,v,'ignored' from toast_scalar_source where id=1 on conflict (id) do update set b=excluded.a" >"$TMP/conflict-narrow.out" 2>&1; then
    echo 'toast-dml: narrowed proposed datum retained origin' >&2; exit 1
fi
case $(cat "$TMP/conflict-narrow.out") in
    *54000*) ;;
    *) echo 'toast-dml: narrowed proposed datum did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c "insert into toast_conflict_origins select 1,v || '', 'ignored' from toast_scalar_source where id=1 on conflict (id) do update set b=excluded.a" >"$TMP/conflict-computed.out" 2>&1; then
    echo 'toast-dml: computed EXCLUDED datum retained compression' >&2; exit 1
fi
case $(cat "$TMP/conflict-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed EXCLUDED datum did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
mkfifo "$TMP/conflict-origin-epq.sql"
run < "$TMP/conflict-origin-epq.sql" > "$TMP/conflict-origin-epq.out" 2>&1 &
READER=$!
exec 3> "$TMP/conflict-origin-epq.sql"
printf '%s\n' "begin; update toast_conflict_origins set b='after' where id=1;" '\echo ready' >&3
i=0
until grep -Fxq ready "$TMP/conflict-origin-epq.out"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || { cat "$TMP/conflict-origin-epq.out" >&2; exit 1; }; sleep 0.1
done
run -c 'select pg_backend_pid()' -c "insert into toast_conflict_origins select 1,v,'ignored' from toast_scalar_source where id=2 on conflict (id) do update set b=case when toast_conflict_origins.b='after' then excluded.a else 'bad' end" >"$TMP/conflict-origin-waiter.out" 2>&1 &
WRITER=$!
i=0
until waiter_pid=$(awk '/^[0-9]+$/{print; exit}' "$TMP/conflict-origin-waiter.out") && [ -n "$waiter_pid" ]; do
    kill -0 "$WRITER" 2>/dev/null || { cat "$TMP/conflict-origin-waiter.out" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 100 ] || exit 1; sleep 0.1
done
printf '%s\n(quit)\n' "(loop repeat 100 for backend = (lookup-backend $waiter_pid) when (and backend (backend-waiting-xid backend)) return :waiting do (sleep 0.05) finally (return :timeout))" | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/conflict-origin-wait.out"
grep -Fq ':WAITING' "$TMP/conflict-origin-wait.out" || { cat "$TMP/conflict-origin-wait.out" >&2; exit 1; }
printf '%s\n' 'commit;' '\q' >&3
exec 3>&-
wait "$READER"; READER=
wait "$WRITER"; WRITER=
[ "$(run -c 'select length(b),substr(b,1,1) from toast_conflict_origins where id=1')" = '20001|b' ]
run -c 'create table toast_scalar_dml (id int primary key, v text)' >/dev/null
run -c 'alter table toast_scalar_dml alter column v set storage plain' >/dev/null
run -c 'insert into toast_scalar_dml values (1,(select v from toast_scalar_source where id=1)),(2,(select v from toast_scalar_source where id=2))' >/dev/null
run -c "insert into toast_scalar_dml values (3,case when true then (select v from toast_scalar_source where id=1) else '' end)" >/dev/null
run -c 'update toast_scalar_dml set v=(select v from toast_scalar_source where id=2) where id=1' >/dev/null
run -c 'update toast_scalar_dml set v=(select v) where id=3' >/dev/null
[ "$(run -c "select count(*) from toast_scalar_dml where (id=1 and length(v)=20001 and substr(v,1,1)='b') or (id=2 and length(v)=20001 and substr(v,1,1)='b') or (id=3 and length(v)=20000 and substr(v,1,1)='a')")" = 3 ]
run -c 'create table toast_scalar_narrow (v varchar(30000))' >/dev/null
run -c 'alter table toast_scalar_narrow alter column v set storage plain' >/dev/null
if run -v VERBOSITY=verbose -c 'insert into toast_scalar_narrow values ((select v from toast_scalar_source where id=1))' >"$TMP/dml-scalar-narrow.out" 2>&1; then
    echo 'toast-dml: narrowing scalar assignment retained compression' >&2; exit 1
fi
case $(cat "$TMP/dml-scalar-narrow.out") in
    *54000*) ;;
    *) echo 'toast-dml: narrowing scalar assignment did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c "update toast_scalar_dml set v=(select v || '' from toast_scalar_source where id=1) where id=1" >"$TMP/dml-scalar-computed.out" 2>&1; then
    echo 'toast-dml: computed scalar assignment retained compression' >&2; exit 1
fi
case $(cat "$TMP/dml-scalar-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed scalar assignment did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
# The waiting updater must recompute both its scalar value and its origin after
# following the committed row version through EvalPlanQual.
run -c 'create table toast_scalar_epq (id int primary key, v text)' >/dev/null
run -c 'alter table toast_scalar_epq alter column v set storage plain' >/dev/null
run -c "insert into toast_scalar_epq values (1,'short')" >/dev/null
mkfifo "$TMP/scalar-epq.sql"
run < "$TMP/scalar-epq.sql" > "$TMP/scalar-epq.out" 2>&1 &
READER=$!
exec 3> "$TMP/scalar-epq.sql"
printf '%s\n' 'begin; update toast_scalar_epq set id=2 where id=1;' '\echo ready' >&3
i=0
until grep -Fxq ready "$TMP/scalar-epq.out"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || { cat "$TMP/scalar-epq.out" >&2; exit 1; }; sleep 0.1
done
run -c 'select pg_backend_pid()' -c 'update toast_scalar_epq set v=(select v from toast_scalar_source s where s.id=toast_scalar_epq.id) where id<=2' >"$TMP/scalar-epq-waiter.out" 2>&1 &
WRITER=$!
i=0
until waiter_pid=$(awk '/^[0-9]+$/{print; exit}' "$TMP/scalar-epq-waiter.out") && [ -n "$waiter_pid" ]; do
    kill -0 "$WRITER" 2>/dev/null || { cat "$TMP/scalar-epq-waiter.out" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 100 ] || exit 1; sleep 0.1
done
printf '%s\n(quit)\n' "(loop repeat 100 for backend = (lookup-backend $waiter_pid) when (and backend (backend-waiting-xid backend)) return :waiting do (sleep 0.05) finally (return :timeout))" | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/scalar-epq-wait.out"
grep -Fq ':WAITING' "$TMP/scalar-epq-wait.out" || { cat "$TMP/scalar-epq-wait.out" >&2; exit 1; }
printf '%s\n' 'commit;' '\q' >&3
exec 3>&-
wait "$READER"; READER=
wait "$WRITER"; WRITER=
[ "$(run -c 'select id,length(v),substr(v,1,1) from toast_scalar_epq')" = '2|20001|b' ]
run -c 'create table toast_update_from (id int primary key, v text)' >/dev/null
run -c 'alter table toast_update_from alter column v set storage plain' >/dev/null
run -c "insert into toast_update_from values (1,'small'),(2,'small')" >/dev/null
run -c 'update toast_update_from t set v=s.v from toast_scalar_source s where t.id=s.id' >/dev/null
[ "$(run -c "select count(*) from toast_update_from where (id=1 and length(v)=20000 and substr(v,1,1)='a') or (id=2 and length(v)=20001 and substr(v,1,1)='b')")" = 2 ]
run -c 'update toast_update_from t set v=s.v from (select s.id,s.v from toast_scalar_source s join toast_join_keys k using(id)) s where t.id=s.id' >/dev/null
run -c 'update toast_update_from t set v=s.v from toast_scalar_source s join toast_join_keys k using(id) where t.id=s.id' >/dev/null
[ "$(run -c "select count(*) from toast_update_from where (id=1 and length(v)=20000 and substr(v,1,1)='a') or (id=2 and length(v)=20001 and substr(v,1,1)='b')")" = 2 ]
if run -v VERBOSITY=verbose -c "update toast_update_from t set v=s.v || '' from toast_scalar_source s where t.id=s.id" >"$TMP/update-from-computed.out" 2>&1; then
    echo 'toast-dml: computed UPDATE FROM retained compression' >&2; exit 1
fi
case $(cat "$TMP/update-from-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed UPDATE FROM did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_from_epq (id int primary key, v text)' >/dev/null
run -c 'alter table toast_from_epq alter column v set storage plain' >/dev/null
run -c "insert into toast_from_epq values (1,'short')" >/dev/null
run -c 'create table toast_from_pair (a text, b text)' >/dev/null
run -c "insert into toast_from_pair values ('$index_suffix','b$index_suffix')" >/dev/null
mkfifo "$TMP/from-epq.sql"
run < "$TMP/from-epq.sql" > "$TMP/from-epq.out" 2>&1 &
READER=$!
exec 3> "$TMP/from-epq.sql"
printf '%s\n' 'begin; update toast_from_epq set id=2 where id=1;' '\echo ready' >&3
i=0
until grep -Fxq ready "$TMP/from-epq.out"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || { cat "$TMP/from-epq.out" >&2; exit 1; }; sleep 0.1
done
run -c 'select pg_backend_pid()' -c 'update toast_from_epq t set v=case when t.id=1 then s.a else s.b end from toast_from_pair s where t.id<=2' >"$TMP/from-epq-waiter.out" 2>&1 &
WRITER=$!
i=0
until waiter_pid=$(awk '/^[0-9]+$/{print; exit}' "$TMP/from-epq-waiter.out") && [ -n "$waiter_pid" ]; do
    kill -0 "$WRITER" 2>/dev/null || { cat "$TMP/from-epq-waiter.out" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 100 ] || exit 1; sleep 0.1
done
printf '%s\n(quit)\n' "(loop repeat 100 for backend = (lookup-backend $waiter_pid) when (and backend (backend-waiting-xid backend)) return :waiting do (sleep 0.05) finally (return :timeout))" | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/from-epq-wait.out"
grep -Fq ':WAITING' "$TMP/from-epq-wait.out" || { cat "$TMP/from-epq-wait.out" >&2; exit 1; }
printf '%s\n' 'commit;' '\q' >&3
exec 3>&-
wait "$READER"; READER=
wait "$WRITER"; WRITER=
[ "$(run -c 'select id,length(v),substr(v,1,1) from toast_from_epq')" = '2|20001|b' ]
run -c 'create table toast_scalar_mixed (id int, v text)' >/dev/null
run -c 'alter table toast_scalar_mixed alter column v set storage plain' >/dev/null
run -c 'insert into toast_scalar_mixed select id,(select (select v)) from toast_scalar_source' >/dev/null
run -c 'insert into toast_scalar_mixed select s.id,(with a as materialized (select s.v), b as materialized (select v from a) select v from b) from toast_scalar_source s' >/dev/null
[ "$(run -c "select count(*) from toast_scalar_mixed where (id=1 and length(v)=20000 and substr(v,1,1)='a') or (id=2 and length(v)=20001 and substr(v,1,1)='b')")" = 4 ]
run -c 'create table toast_union_result (v text)' >/dev/null
run -c 'alter table toast_union_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_union_result select v from toast_scalar_source where id=1 union all select v from toast_scalar_source where id=2' >/dev/null
run -c 'insert into toast_union_result with q as materialized (select v from toast_scalar_source where id=1 union all select v from toast_scalar_source where id=2) select v from q' >/dev/null
[ "$(run -c "select count(*) from toast_union_result where (length(v)=20000 and substr(v,1,1)='a') or (length(v)=20001 and substr(v,1,1)='b')")" = 4 ]
run -c 'create table toast_union_varchar (v varchar(30000))' >/dev/null
run -c "insert into toast_union_varchar values ('$index_suffix')" >/dev/null
run -c 'insert into toast_union_result select v from toast_union_varchar union all select v from toast_scalar_source where id=2' >/dev/null
run -c 'insert into toast_union_result select v from toast_scalar_source where id=2 union all select v from toast_union_varchar' >/dev/null
[ "$(run -c "select count(*) from toast_union_result where (length(v)=20000 and substr(v,1,1)='a') or (length(v)=20001 and substr(v,1,1)='b')")" = 8 ]
run -c "insert into toast_union_result select v from toast_scalar_source where id=1 union select v || '' from toast_scalar_source where id=1" >/dev/null
if run -v VERBOSITY=verbose -c "insert into toast_union_result select v || '' from toast_scalar_source where id=1 union select v from toast_scalar_source where id=1" >"$TMP/union-first-plain.out" 2>&1; then
    echo 'toast-dml: union replaced the first representative origin' >&2; exit 1
fi
case $(cat "$TMP/union-first-plain.out") in
    *54000*) ;;
    *) echo 'toast-dml: union representative did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c "insert into toast_union_result select v from toast_scalar_source where id=1 union all select v || '' from toast_scalar_source where id=2" >"$TMP/union-computed.out" 2>&1; then
    echo 'toast-dml: computed union arm retained compression' >&2; exit 1
fi
case $(cat "$TMP/union-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed union arm did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_aggregate_result (v text)' >/dev/null
run -c 'alter table toast_aggregate_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_aggregate_result select min(v) from toast_scalar_source where id=1' >/dev/null
run -c 'insert into toast_aggregate_result select max(v) from toast_scalar_source where id=2' >/dev/null
run -c 'insert into toast_aggregate_result select v from toast_scalar_source group by v having min(id)>0' >/dev/null
[ "$(run -c 'select count(*) from toast_aggregate_result where length(v)=20000')" = 2 ]
[ "$(run -c 'select count(*) from toast_aggregate_result where length(v)=20001')" = 2 ]
run -c 'create table toast_aggregate_source (v text)' >/dev/null
run -c "insert into toast_aggregate_source values (null),('$index_suffix'),(null)" >/dev/null
run -c 'insert into toast_aggregate_result select min(v) from toast_aggregate_source' >/dev/null
run -c 'insert into toast_aggregate_result with q as materialized (select min(v) as v from toast_aggregate_source) select v from q' >/dev/null
[ "$(run -c 'select count(*) from toast_aggregate_result where length(v)=20000')" = 4 ]
run -c 'create table toast_hash_source (id int, v text)' >/dev/null
run -c "insert into toast_hash_source values (1,'$index_suffix'),(2,'$index_suffix')" >/dev/null
run -c "insert into toast_aggregate_result select distinct v from (select case when id=1 then v else v || '' end as v from toast_hash_source order by id) s" >/dev/null
run -c "insert into toast_aggregate_result select v from (select case when id=1 then v else v || '' end as v from toast_hash_source order by id) s group by v" >/dev/null
run -c 'insert into toast_aggregate_result with q as materialized (select distinct v from toast_hash_source) select v from q' >/dev/null
[ "$(run -c 'select count(*) from toast_aggregate_result where length(v)=20000')" = 7 ]
run -c 'create table toast_hash_spill_result (v text)' >/dev/null
run -c 'alter table toast_hash_spill_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_hash_spill_result select distinct v from toast_scalar_source' >/dev/null
run -c 'insert into toast_hash_spill_result select v from toast_scalar_source group by v' >/dev/null
run -c 'insert into toast_hash_spill_result with q as materialized (select distinct v from toast_scalar_source) select v from q' >/dev/null
[ "$(run -c 'select count(*) from toast_hash_spill_result where length(v)=20000')" = 3 ]
[ "$(run -c 'select count(*) from toast_hash_spill_result where length(v)=20001')" = 3 ]
if run -v VERBOSITY=verbose -c "insert into toast_aggregate_result select distinct v from (select case when id=1 then v || '' else v end as v from toast_hash_source order by id) s" >"$TMP/hash-first-plain.out" 2>&1; then
    echo 'toast-dml: hash distinct replaced the first representative origin' >&2; exit 1
fi
case $(cat "$TMP/hash-first-plain.out") in
    *54000*) ;;
    *) echo 'toast-dml: hash distinct did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c "insert into toast_aggregate_source values ('$index_suffix')" >/dev/null
if run -v VERBOSITY=verbose -c 'insert into toast_aggregate_result select max(v) from toast_aggregate_source' >"$TMP/aggregate-equal.out" 2>&1; then
    echo 'toast-dml: equal aggregate inputs retained compression' >&2; exit 1
fi
case $(cat "$TMP/aggregate-equal.out") in
    *54000*) ;;
    *) echo 'toast-dml: equal aggregate inputs did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c 'insert into toast_aggregate_result select min(v) from toast_scalar_source' >"$TMP/aggregate-computed.out" 2>&1; then
    echo 'toast-dml: compared aggregate datum retained compression' >&2; exit 1
fi
case $(cat "$TMP/aggregate-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: compared aggregate did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_lateral_result (id int, v text)' >/dev/null
run -c 'alter table toast_lateral_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_lateral_result select s.id,q.v from toast_scalar_source s cross join lateral (select s.v) q' >/dev/null
run -c 'insert into toast_lateral_result select s.id,q.v from toast_scalar_source s left join lateral (select s.v where s.id=1) q on true' >/dev/null
[ "$(run -c "select count(*) from toast_lateral_result where (id=1 and length(v)=20000 and substr(v,1,1)='a') or (id=2 and length(v)=20001 and substr(v,1,1)='b')")" = 3 ]
[ "$(run -c 'select count(*) from toast_lateral_result where id=2 and v is null')" = 1 ]
run -c 'create table toast_cte_join_result (id int, v text)' >/dev/null
run -c 'alter table toast_cte_join_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_cte_join_result with q as materialized (select s.id,s.v from toast_scalar_source s full join toast_join_keys k on s.id=k.id) select id,v from q' >/dev/null
run -c 'insert into toast_cte_join_result with q as materialized (select s.id,x.v from toast_scalar_source s cross join lateral (select s.v) x) select id,v from q' >/dev/null
[ "$(run -c "select count(*) from toast_cte_join_result where (id=1 and length(v)=20000 and substr(v,1,1)='a') or (id=2 and length(v)=20001 and substr(v,1,1)='b')")" = 4 ]
[ "$(run -c 'select count(*) from toast_cte_join_result where v is null')" = 1 ]
run -c 'insert into toast_cte_join_result with q as materialized (select s.id,s.v from toast_join_keys k cross join toast_scalar_source s) select id,v from q' >/dev/null
run -c 'insert into toast_cte_join_result with q as materialized (select s.id,s.v from toast_join_empty e full join toast_scalar_source s on false) select id,v from q' >/dev/null
run -c 'insert into toast_cte_join_result with q as materialized (select s.id,x.v from toast_scalar_source s left join lateral (select s.v where s.id=1) x on true) select id,v from q' >/dev/null
[ "$(run -c "select count(*) from toast_cte_join_result where (id=1 and length(v)=20000 and substr(v,1,1)='a') or (id=2 and length(v)=20001 and substr(v,1,1)='b')")" = 13 ]
[ "$(run -c 'select count(*) from toast_cte_join_result where v is null')" = 2 ]
run -c 'create table toast_recursive_join_result (v text)' >/dev/null
run -c 'alter table toast_recursive_join_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_recursive_join_result with recursive q(v,n) as (select v,1 from toast_scalar_source union all select q.v,q.n+1 from q join toast_join_keys k on k.id=1 where q.n<2) select v from q' >/dev/null
[ "$(run -c 'select count(*) from toast_recursive_join_result where length(v)=20000 or length(v)=20001')" = 4 ]
if run -v VERBOSITY=verbose -c "insert into toast_lateral_result select s.id,q.v from toast_scalar_source s cross join lateral (select s.v || '' as v) q" >"$TMP/lateral-computed.out" 2>&1; then
    echo 'toast-dml: computed lateral datum retained compression' >&2; exit 1
fi
case $(cat "$TMP/lateral-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed lateral datum did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'insert into toast_scalar_result select (select v from toast_inline_build where false)' >/dev/null
[ "$(run -c 'select count(*) from toast_scalar_result where v is null')" = 1 ]
if run -v VERBOSITY=verbose -c "insert into toast_scalar_result select (select v || '') from toast_inline_build" >"$TMP/scalar-computed.out" 2>&1; then
    echo 'toast-dml: computed scalar datum retained compression' >&2; exit 1
fi
case $(cat "$TMP/scalar-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed scalar datum did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'insert into toast_recursive_result with recursive q(v) as (select v from toast_inline_build union select v from q) select v from q' >/dev/null
[ "$(run -c 'select count(*) from toast_recursive_result where length(v)=20000')" = 4 ]
if run -v VERBOSITY=verbose -c "insert into toast_recursive_result with recursive q(v,n) as (select v,1 from toast_inline_build union all select v || '',n+1 from q where n<2) select v from q" >"$TMP/recursive-computed.out" 2>&1; then
    echo 'toast-dml: computed recursive datum retained compression' >&2; exit 1
fi
case $(cat "$TMP/recursive-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed recursive datum did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c "insert into toast_cte_result with q as materialized (select v || '' as v from toast_inline_build) select v from q" >"$TMP/cte-computed.out" 2>&1; then
    echo 'toast-dml: computed CTE datum retained compression' >&2; exit 1
fi
case $(cat "$TMP/cte-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed CTE did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
printf '%s\n' '(setf pg::*query-work-memory-bytes* (* 4 1024 1024))' '(quit)' | \
    "${SBCL:-sbcl}" --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/sort-work-mem-reset.out"
run -c 'create table toast_select_conflict (id int primary key, v text)' >/dev/null
run -c 'alter table toast_select_conflict alter column v set storage plain' >/dev/null
run -c "insert into toast_select_conflict values (1,'small')" >/dev/null
run -c "insert into toast_select_conflict select 1,v from toast_inline_build where v <> 'short' on conflict (id) do update set v=excluded.v" >/dev/null
[ "$(run -c 'select length(v) from toast_select_conflict')" = 20000 ]
run -c 'create table toast_select_varchar (v varchar)' >/dev/null
run -c 'alter table toast_select_varchar alter column v set storage plain' >/dev/null
run -c "insert into toast_select_varchar select v from toast_inline_build where v <> 'short'" >/dev/null
run -c 'create table toast_select_bounded (v varchar(30000))' >/dev/null
run -c 'alter table toast_select_bounded alter column v set storage plain' >/dev/null
if run -v VERBOSITY=verbose -c "insert into toast_select_bounded select v from toast_inline_build where v <> 'short'" >"$TMP/select-coercion.out" 2>&1; then
    echo 'toast-dml: narrowing coercion incorrectly retained compression' >&2; exit 1
fi
case $(cat "$TMP/select-coercion.out") in
    *54000*) ;;
    *) echo 'toast-dml: narrowing coercion did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c "insert into toast_select_plain select v || '' from toast_inline_build" >"$TMP/select-computed.out" 2>&1; then
    echo 'toast-dml: computed INSERT SELECT retained compression' >&2; exit 1
fi
case $(cat "$TMP/select-computed.out") in
    *54000*) ;;
    *) echo 'toast-dml: computed INSERT SELECT did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_inline_varchar (id int primary key, v varchar(30000) unique)' >/dev/null
run -c "insert into toast_inline_varchar values (1,'$index_suffix')" >/dev/null
run -c 'alter table toast_inline_varchar alter column v set storage external' >/dev/null
run -c 'update toast_inline_varchar set id=2, v=v where id=1' >/dev/null
[ "$(run -c 'select id,length(v) from toast_inline_varchar')" = '2|20000' ]
run -c 'create table toast_cast_result (v text)' >/dev/null
run -c 'alter table toast_cast_result alter column v set storage plain' >/dev/null
run -c 'insert into toast_cast_result select v::text from toast_inline_varchar' >/dev/null
run -c 'insert into toast_cast_result select v::varchar(40000) from toast_inline_varchar' >/dev/null
run -c "insert into toast_cast_result select case when true then v::varchar else '' end from toast_inline_varchar" >/dev/null
[ "$(run -c 'select count(*) from toast_cast_result where length(v)=20000')" = 3 ]
if run -v VERBOSITY=verbose -c 'insert into toast_cast_result select v::varchar(20000) from toast_inline_varchar' >"$TMP/cast-narrow.out" 2>&1; then
    echo 'toast-dml: narrowing cast retained compression' >&2; exit 1
fi
case $(cat "$TMP/cast-narrow.out") in
    *54000*) ;;
    *) echo 'toast-dml: narrowing cast did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c 'insert into toast_cast_result select v::varchar(30000) from toast_scalar_source where id=1' >"$TMP/cast-text-bounded.out" 2>&1; then
    echo 'toast-dml: text to bounded varchar retained compression' >&2; exit 1
fi
case $(cat "$TMP/cast-text-bounded.out") in
    *54000*) ;;
    *) echo 'toast-dml: text to bounded varchar did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_select_wider (v varchar(40000))' >/dev/null
run -c 'alter table toast_select_wider alter column v set storage plain' >/dev/null
run -c 'insert into toast_select_wider select v from toast_inline_varchar' >/dev/null
run -c 'insert into toast_select_wider select v from toast_inline_varchar where id=2' >/dev/null
[ "$(run -c 'select count(*) from toast_select_wider where length(v)=20000')" = 2 ]
run -c 'create table toast_join_wider (v varchar(40000))' >/dev/null
run -c "insert into toast_join_wider values ('$index_suffix')" >/dev/null
run -c 'insert into toast_select_plain select v from toast_inline_varchar join toast_join_wider using(v)' >/dev/null
[ "$(run -c 'select count(*) from toast_select_plain where length(v)=20000')" = 6 ]
run -c 'create index toast_join_keys_id on toast_join_keys(id)' >/dev/null
run -c 'insert into toast_join_result select s.v from toast_inline_varchar s join toast_join_keys k on s.id=k.id' >/dev/null
[ "$(run -c 'select count(*) from toast_join_result where length(v)=20000')" = 6 ]
run -c 'create table toast_external_compressed (v text unique)' >/dev/null
awk 'BEGIN { printf "insert into toast_external_compressed values (\047"; for (i=0;i<200000;i++) printf "a"; print "\047);" }' | run >/dev/null
run -c 'alter table toast_external_compressed alter column v set storage external' >/dev/null
run -c 'update toast_external_compressed set v=v' >/dev/null
run -c 'vacuum full toast_external_compressed' >/dev/null
[ "$(run -c 'select length(v) from toast_external_compressed')" = 200000 ]
run -c 'create table toast_external_self (id int primary key, v text)' >/dev/null
awk 'BEGIN { printf "insert into toast_external_self values (1,\047"; for (i=0;i<200000;i++) printf "a"; printf "\047),(2,\047"; for (i=0;i<200000;i++) printf "a"; print "\047);" }' | run >/dev/null
run -c 'alter table toast_external_self alter column v set storage plain' >/dev/null
run -c 'update toast_external_self set v=(select v) where id=1' >/dev/null
run -c "update toast_external_self set v=coalesce(v,'') where id=1" >/dev/null
run -c 'update toast_external_self set v=(v::varchar)::text where id=1' >/dev/null
run -c 'update toast_external_self t set v=s.v from toast_external_self s where t.id=1 and s.id=1' >/dev/null
[ "$(run -c 'select length(v) from toast_external_self where id=1')" = 200000 ]
run -c 'create table toast_conflict_external (id int primary key, a text, b text)' >/dev/null
awk 'BEGIN { printf "insert into toast_conflict_external values (1,\047small\047,\047"; for (i=0;i<200000;i++) printf "a"; print "\047);" }' | run >/dev/null
run -c 'alter table toast_conflict_external alter column a set storage plain' >/dev/null
run -c 'alter table toast_conflict_external alter column b set storage plain' >/dev/null
run -c "insert into toast_conflict_external select 1,b,'ignored' from toast_conflict_external where id=1 on conflict (id) do update set b=excluded.a" >/dev/null
[ "$(run -c 'select length(b) from toast_conflict_external where id=1')" = 200000 ]
run -c 'create table toast_conflict_cross (id int primary key, a text, b text unique)' >/dev/null
run -c 'alter table toast_conflict_cross alter column a set storage plain' >/dev/null
run -c 'alter table toast_conflict_cross alter column b set storage external' >/dev/null
run -c "insert into toast_conflict_cross values (1,'old','small')" >/dev/null
run -c "insert into toast_conflict_cross select 1,v,'ignored' from toast_external_self where id=1 on conflict (id) do update set b=excluded.a" >/dev/null
[ "$(run -c 'select length(b) from toast_conflict_cross where id=1')" = 200000 ]
if run -v VERBOSITY=verbose -c "insert into toast_conflict_external select 1,v,'ignored' from toast_external_self where id=1 on conflict (id) do update set b=excluded.a" >"$TMP/conflict-external-foreign.out" 2>&1; then
    echo 'toast-dml: foreign EXCLUDED descriptor reused under PLAIN' >&2; exit 1
fi
case $(cat "$TMP/conflict-external-foreign.out") in
    *54000*) ;;
    *) echo 'toast-dml: foreign EXCLUDED descriptor did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
# EXCLUDED retains its statement-snapshot datum across a conflict wait, but
# must match the newly claimed old tuple before any descriptor is reused.
mkfifo "$TMP/conflict-descriptor.sql"
run < "$TMP/conflict-descriptor.sql" > "$TMP/conflict-descriptor.out" 2>&1 &
READER=$!
exec 3> "$TMP/conflict-descriptor.sql"
printf '%s\n' "begin; update toast_conflict_external set b='short' where id=1;" '\echo ready' >&3
i=0
until grep -Fxq ready "$TMP/conflict-descriptor.out"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || { cat "$TMP/conflict-descriptor.out" >&2; exit 1; }; sleep 0.1
done
run -v VERBOSITY=verbose -c 'select pg_backend_pid()' -c "insert into toast_conflict_external select 1,b,'ignored' from toast_conflict_external where id=1 on conflict (id) do update set b=excluded.a" >"$TMP/conflict-descriptor-waiter.out" 2>&1 &
WRITER=$!
i=0
until waiter_pid=$(awk '/^[0-9]+$/{print; exit}' "$TMP/conflict-descriptor-waiter.out") && [ -n "$waiter_pid" ]; do
    kill -0 "$WRITER" 2>/dev/null || { cat "$TMP/conflict-descriptor-waiter.out" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 100 ] || exit 1; sleep 0.1
done
printf '%s\n(quit)\n' "(loop repeat 100 for backend = (lookup-backend $waiter_pid) when (and backend (backend-waiting-xid backend)) return :waiting do (sleep 0.05) finally (return :timeout))" | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/conflict-descriptor-wait.out"
grep -Fq ':WAITING' "$TMP/conflict-descriptor-wait.out" || { cat "$TMP/conflict-descriptor-wait.out" >&2; exit 1; }
printf '%s\n' 'commit;' '\q' >&3
exec 3>&-
wait "$READER"; READER=
if wait "$WRITER"; then echo 'toast-dml: stale EXCLUDED descriptor reused after conflict wait' >&2; exit 1; fi
WRITER=
case $(cat "$TMP/conflict-descriptor-waiter.out") in
    *54000*) ;;
    *) echo 'toast-dml: stale EXCLUDED descriptor did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
[ "$(run -c 'select b from toast_conflict_external where id=1')" = short ]
if run -v VERBOSITY=verbose -c 'update toast_external_self t set v=s.v from toast_external_self s where t.id=1 and s.id=2' >"$TMP/external-other-row.out" 2>&1; then
    echo 'toast-dml: equal external value from another row reused a descriptor' >&2; exit 1
fi
case $(cat "$TMP/external-other-row.out") in
    *54000*) ;;
    *) echo 'toast-dml: foreign external descriptor did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
raw_external=$(awk 'BEGIN { s=1; for (i=0;i<10000;i++) { s=(s*1664525+1013904223)%4294967296; printf "%c", 97+int(s/16777216)%26 } }')
run -c 'create table toast_raw_self (id int primary key, v text)' >/dev/null
run -c 'alter table toast_raw_self alter column v set storage external' >/dev/null
run -c "insert into toast_raw_self values (1,'$raw_external'),(2,'$raw_external')" >/dev/null
run -c 'alter table toast_raw_self alter column v set storage plain' >/dev/null
run -c 'update toast_raw_self set v=(select v) where id=1' >/dev/null
run -c "update toast_raw_self set v=case when true then v else '' end where id=1" >/dev/null
[ "$(run -c 'select length(v) from toast_raw_self where id=1')" = 10000 ]
if run -v VERBOSITY=verbose -c 'update toast_raw_self t set v=s.v from toast_raw_self s where t.id=1 and s.id=2' >"$TMP/raw-other-row.out" 2>&1; then
    echo 'toast-dml: equal raw external value from another row reused a descriptor' >&2; exit 1
fi
case $(cat "$TMP/raw-other-row.out") in
    *54000*) ;;
    *) echo 'toast-dml: foreign raw descriptor did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
# A cached source descriptor from the old snapshot must not be reused after
# the target is concurrently replaced with a different claimed tuple.
mkfifo "$TMP/descriptor-epq.sql"
run < "$TMP/descriptor-epq.sql" > "$TMP/descriptor-epq.out" 2>&1 &
READER=$!
exec 3> "$TMP/descriptor-epq.sql"
printf '%s\n' "begin; update toast_raw_self set v='short' where id=1;" '\echo ready' >&3
i=0
until grep -Fxq ready "$TMP/descriptor-epq.out"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || { cat "$TMP/descriptor-epq.out" >&2; exit 1; }; sleep 0.1
done
run -v VERBOSITY=verbose -c 'select pg_backend_pid()' -c 'update toast_raw_self set v=(select v from toast_raw_self where id=1) where id=1' >"$TMP/descriptor-epq-waiter.out" 2>&1 &
WRITER=$!
i=0
until waiter_pid=$(awk '/^[0-9]+$/{print; exit}' "$TMP/descriptor-epq-waiter.out") && [ -n "$waiter_pid" ]; do
    kill -0 "$WRITER" 2>/dev/null || { cat "$TMP/descriptor-epq-waiter.out" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 100 ] || exit 1; sleep 0.1
done
printf '%s\n(quit)\n' "(loop repeat 100 for backend = (lookup-backend $waiter_pid) when (and backend (backend-waiting-xid backend)) return :waiting do (sleep 0.05) finally (return :timeout))" | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/descriptor-epq-wait.out"
grep -Fq ':WAITING' "$TMP/descriptor-epq-wait.out" || { cat "$TMP/descriptor-epq-wait.out" >&2; exit 1; }
printf '%s\n' 'commit;' '\q' >&3
exec 3>&-
wait "$READER"; READER=
if wait "$WRITER"; then echo 'toast-dml: stale descriptor reused after EPQ' >&2; exit 1; fi
WRITER=
case $(cat "$TMP/descriptor-epq-waiter.out") in
    *54000*) ;;
    *) echo 'toast-dml: stale descriptor did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
[ "$(run -c 'select v from toast_raw_self where id=1')" = short ]
if run -v VERBOSITY=verbose -c 'insert into toast_scalar_dml values (4,(select v from toast_external_compressed))' >"$TMP/dml-scalar-external.out" 2>&1; then
    echo 'toast-dml: external scalar datum was retained under PLAIN' >&2; exit 1
fi
case $(cat "$TMP/dml-scalar-external.out") in
    *54000*) ;;
    *) echo 'toast-dml: external scalar datum did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c 'insert into toast_select_plain select v from toast_external_compressed' >"$TMP/select-external-plain.out" 2>&1; then
    echo 'toast-dml: foreign external datum was retained under PLAIN' >&2; exit 1
fi
case $(cat "$TMP/select-external-plain.out") in
    *54000*) ;;
    *) echo 'toast-dml: external datum under PLAIN did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_select_external (v text)' >/dev/null
run -c 'alter table toast_select_external alter column v set storage external' >/dev/null
run -c 'insert into toast_select_external select v from toast_external_compressed' >/dev/null
[ "$(run -c 'select length(v) from toast_select_external')" = 200000 ]
run -c 'drop table toast_external_compressed' >/dev/null
[ "$(run -c 'select length(v) from toast_select_external')" = 200000 ]
run -c 'create table toast_long_index (v text)' >/dev/null
i=0
while [ "$i" -lt 80 ]; do
    prefix=$(printf '%03d' "$i")
    run -c "insert into toast_long_index values ('$prefix$index_suffix')" >/dev/null
    i=$((i + 1))
done
run -c 'create unique index toast_long_index_v on toast_long_index(v)' >/dev/null
[ "$(run -c "select count(*) from toast_long_index where v = '039$index_suffix'")" = 1 ]
kill -INT "$SRV"
wait "$SRV"
SRV=
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" --control-socket "$TMP/control.sock"
test_wait_owned_sql_server "$SRV" "$PORT" || { cat "$LOG" >&2; exit 1; }
[ "$(run -c "select count(*) from toast_long_index where v = '039$index_suffix'")" = 1 ]
if run -v VERBOSITY=verbose -c "insert into toast_long_index values ('039$index_suffix')" >"$TMP/duplicate.out" 2>&1; then
    echo 'toast-dml: duplicate compressed key accepted' >&2; exit 1
fi
case $(cat "$TMP/duplicate.out") in
    *23505*) ;;
    *) echo 'toast-dml: duplicate compressed key did not report SQLSTATE 23505' >&2; exit 1 ;;
esac
run -c "insert into toast_long_index values ('080$index_suffix')" >/dev/null
[ "$(run -c "select count(*) from toast_long_index where v = '080$index_suffix'")" = 1 ]
run -c 'create table toast_external_index (v text)' >/dev/null
run -c 'alter table toast_external_index alter column v set storage external' >/dev/null
run -c 'create index toast_external_index_v on toast_external_index(v)' >/dev/null
if run -v VERBOSITY=verbose -c "insert into toast_external_index values ('$index_suffix')" >"$TMP/external-index.out" 2>&1; then
    echo 'toast-dml: oversized EXTERNAL index key accepted' >&2; exit 1
fi
case $(cat "$TMP/external-index.out") in
    *54000*) ;;
    *) echo 'toast-dml: oversized EXTERNAL key did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table toast_composite_index (a text, b text)' >/dev/null
run -c 'alter table toast_composite_index alter column b set storage external' >/dev/null
run -c 'create unique index toast_composite_index_ab on toast_composite_index(a,b)' >/dev/null
run -c "insert into toast_composite_index values ('$index_suffix','small')" >/dev/null
[ "$(run -c "select count(*) from toast_composite_index where a = '$index_suffix' and b = 'small'")" = 1 ]
long_index_suffix=$(awk 'BEGIN { for (i=0;i<70000;i++) printf "a" }')
run -c 'create table toast_large_composite (a text, b text, unique(a,b))' >/dev/null
printf "insert into toast_large_composite values ('%s','x'), ('%s','y');\n" "$long_index_suffix" "$long_index_suffix" | run >/dev/null
[ "$(run -c "select count(*) from toast_large_composite where a = '$long_index_suffix' and b = 'y'")" = 1 ]
run -c 'alter table toast_long_index alter column v set storage external' >/dev/null
index_blocks() {
    printf '%s\n' '(let* ((db (gethash "postgres" (catalog-databases *catalog*))) (idx (gethash "toast_long_index_v" (database-indexes db)))) (format t "BLOCKS=~D~%" (buf-nblocks (pg-index-relfilenode idx))))' '(quit)' | \
        "${SBCL:-sbcl}" --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock"
}
index_blocks >"$TMP/index-blocks-before"
i=100
while [ "$i" -lt 400 ]; do
    run -c "insert into toast_long_index values ('$i-abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz')" >/dev/null
    i=$((i + 1))
done
index_blocks >"$TMP/index-blocks-after"
"${PYTHON:-python3}" - "$TMP/index-blocks-before" "$TMP/index-blocks-after" <<'PY'
import re
import sys
counts = []
for path in sys.argv[1:]:
    with open(path, encoding="utf-8") as stream:
        match = re.search(r"BLOCKS=(\d+)", stream.read())
    assert match, f"missing index block count in {path}"
    counts.append(int(match.group(1)))
assert counts[1] > counts[0], f"index did not split after ALTER STORAGE: {counts}"
PY
[ "$(run -c "select count(*) from toast_long_index where v = '039$index_suffix'")" = 1 ]
# Keep the long text incompressible so snapshot and deletion checks exercise
# external TOAST chunks as well as the separate inline-compression cases below.
big=$(awk 'BEGIN { s=1; for (i=0;i<3000;i++) { s=(s*1664525+1013904223)%4294967296; printf "%c", 97+int(s/16777216)%26 } }')
other=$(awk 'BEGIN { s=2; for (i=0;i<3000;i++) { s=(s*1664525+1013904223)%4294967296; printf "%c", 97+int(s/16777216)%26 } }')
json_value=" { \"key\": \"$big\", \"key\": 2 } "
jsonb_value=" { \"key\": 2, \"long\": \"$big\", \"key\": 3 } "
run -c 'create table toast_jsonb (id int primary key, v jsonb)' >/dev/null
run -c "insert into toast_jsonb values (1, '$jsonb_value')" >/dev/null
[ "$(run -c 'select v from toast_jsonb where id = 1')" = "{\"key\": 3, \"long\": \"$big\"}" ]
[ "$(run -c "select v ->> 'long' from toast_jsonb where id = 1")" = "$big" ]
[ "$(run -c "select v #>> '{long}' from toast_jsonb where id = 1")" = "$big" ]
[ "$(run -c "select v @> '{\"key\":3}'::jsonb from toast_jsonb where id = 1")" = t ]
[ "$(run -c "select v ? 'long' from toast_jsonb where id = 1")" = t ]
[ "$(run -c "select v -> 'key' from toast_jsonb where id = 1")" = 3 ]
[ "$(run -c "select (v || '{\"key\":4}') ->> 'key' from toast_jsonb where id = 1")" = 4 ]
[ "$(run -c "select length((v || '{\"key\":4}') ->> 'long') from toast_jsonb where id = 1")" = 3000 ]
[ "$(run -c "select v = '{\"long\":\"$big\",\"key\":3}'::jsonb from toast_jsonb where id = 1")" = t ]
run -c 'create index toast_jsonb_v_idx on toast_jsonb(v)' >/dev/null
[ "$(run -c "select id from toast_jsonb where v = '{\"long\":\"$big\",\"key\":3}'::jsonb")" = 1 ]
[ "$(run -c 'select count(*) from (select distinct v from toast_jsonb) s')" = 1 ]
[ "$(run -c "select distinct '1'::jsonb order by '1'::jsonb")" = 1 ]
[ "$(run -c "select '1.5'::jsonb::int")" = 2 ]
[ "$(run -c "select 'true'::jsonb::boolean")" = t ]
run -c "insert into toast_jsonb values (2, '{\"n\":1.0}'), (3, '{\"n\":1.00}')" >/dev/null
[ "$(run -c 'select count(*) from (select distinct v from toast_jsonb where id > 1) s')" = 1 ]
run -c 'analyze toast_jsonb' >/dev/null
run -c 'vacuum full toast_jsonb' >/dev/null
kill "$SRV"
wait "$SRV" || true
SRV=
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" --control-socket "$TMP/control.sock"
test_wait_owned_sql_server "$SRV" "$PORT" || { cat "$LOG" >&2; exit 1; }
[ "$(run -c "select id from toast_jsonb where v = '{\"long\":\"$big\",\"key\":3}'::jsonb")" = 1 ]
run -c "update toast_jsonb set v = '{\"changed\":true}' where id = 1" >/dev/null
[ "$(run -c 'delete from toast_jsonb where id = 1 returning v')" = '{"changed": true}' ]
run -c 'create table toast_json (id int primary key, v json)' >/dev/null
run -c "insert into toast_json values (1, '$json_value')" >/dev/null
[ "$(run -c 'select v from toast_json where id = 1')" = "$json_value" ]
[ "$(run -c "select v -> 'key' from toast_json where id = 1")" = 2 ]
[ "$(run -c "select v ->> 'key' from toast_json where id = 1")" = 2 ]
[ "$(run -c "select v #>> '{key}' from toast_json where id = 1")" = 2 ]
run -c 'insert into toast_jsonb select 4, v from toast_json where id = 1' >/dev/null
[ "$(run -c 'select v from toast_jsonb where id = 4')" = '{"key": 2}' ]
run -c 'insert into toast_json select 2, v from toast_jsonb where id = 4' >/dev/null
[ "$(run -c 'select v from toast_json where id = 2')" = '{"key": 2}' ]
if run -c 'select v = v from toast_json' >/dev/null 2>&1; then
    echo 'toast-dml: JSON equality was accepted' >&2
    exit 1
fi
for query in 'select distinct v from toast_json' 'select v from toast_json order by v' \
             'select v, count(*) from toast_json group by v' \
             'select v from toast_json union select v from toast_json' \
             'select row_number() over (partition by v) from toast_json' \
             'select row_number() over (order by v) from toast_json' \
             'create index invalid_json_idx on toast_json(v)' \
             'create table invalid_json_pk (v json primary key)'; do
    if run -c "$query" >/dev/null 2>&1; then
        echo "toast-dml: unsupported JSON operation was accepted: $query" >&2
        exit 1
    fi
done
run -c "update toast_json set v = '{\"updated\":true}' where id = 1" >/dev/null
[ "$(run -c 'delete from toast_json where id = 1 returning v')" = '{"updated":true}' ]
if run -c "insert into toast_json values (2, '{invalid}')" >/dev/null 2>&1; then
    echo 'toast-dml: invalid JSON was accepted' >&2
    exit 1
fi
binary_hex=$(awk 'BEGIN { for (i=0;i<1500;i++) printf "00ff" }')
bytea_escape=$(printf '%s' '\000\\\177\200\377 A')
[ "$(run -c "set bytea_output = escape; select '\\x005c7f80ff2041'::bytea")" = "$bytea_escape" ]
[ "$(run -c "set bytea_output = escape; select '\\x005c7f80ff2041'::bytea::text")" = "$bytea_escape" ]
[ "$(run -c "set bytea_output = escape; reset bytea_output; show bytea_output")" = hex ]
[ "$(run -c "set bytea_output = escape; begin; set bytea_output = hex; rollback; show bytea_output")" = escape ]
[ "$(run -c "begin; set bytea_output = escape; commit; show bytea_output")" = escape ]
[ "$(PGOPTIONS='-c bytea_output=escape' run -c "set bytea_output = hex; reset bytea_output; show bytea_output")" = escape ]
if run -c 'set bytea_output = invalid' >/dev/null 2>&1; then
    echo 'toast-dml: invalid bytea_output was accepted' >&2
    exit 1
fi
run -c 'create table toast_bytes (id int primary key, v bytea)' >/dev/null
run -c 'alter table toast_bytes alter column v set storage external' >/dev/null
run -c "insert into toast_bytes values (1, '\\x$binary_hex')" >/dev/null
run -c 'create table toast_two_slices (id int, v bytea, w bytea)' >/dev/null
run -c 'alter table toast_two_slices alter column v set storage external' >/dev/null
run -c 'alter table toast_two_slices alter column w set storage external' >/dev/null
binary_hex_other="11$binary_hex"
run -c "insert into toast_two_slices values (1, '\\x$binary_hex', '\\x$binary_hex_other')" >/dev/null
[ "$(run -c 'select length(v) from toast_bytes where id = 1')" = 3000 ]
[ "$(run -c "select v = '\\x$binary_hex' from toast_bytes where id = 1")" = t ]
[ "$(run -c "select length(v || '\\x00'::bytea), length('\\xff'::bytea || v) from toast_bytes where id = 1")" = '3001|3001' ]
[ "$(run -c "select v || '\\x00'::bytea = '\\x${binary_hex}00'::bytea from toast_bytes where id = 1")" = t ]
# Count exact chunk lookups to distinguish a partial fetch from full detoasting.
printf '%s\n(quit)\n' '(let ((original (symbol-function (quote btree-search-unlocked))) (counter (intern "*SLICE-LOOKUPS*"))) (setf (symbol-value counter) 0 (symbol-value (intern "*SLICE-LOOKUP-ORIGINAL*")) original) (setf (symbol-function (quote btree-search-unlocked)) (lambda (&rest args) (incf (symbol-value counter)) (apply original args))))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/slice-install.out"
! grep -q ';; error:' "$TMP/slice-install.out"
[ "$(run -c 'select substr(v,100,20) from toast_bytes where id + 0 = 1')" = "\\x$(printf '%s' "$binary_hex" | cut -c 199-238)" ]
[ "$(run -c 'select substring(v,1995,20) from toast_bytes where id + 0 = 1')" = "\\x$(printf '%s' "$binary_hex" | cut -c 3989-4028)" ]
[ "$(run -c 'select id, substr(v,100,20), substring(v,200,20) from toast_bytes where id + 0 = 1')" = "1|\\x$(printf '%s' "$binary_hex" | cut -c 199-238)|\\x$(printf '%s' "$binary_hex" | cut -c 399-438)" ]
[ "$(run -c 'select substr(v,100,20), substr(w,200,20) from toast_two_slices')" = "\\x$(printf '%s' "$binary_hex" | cut -c 199-238)|\\x$(printf '%s' "$binary_hex_other" | cut -c 399-438)" ]
[ "$(run -c 'select substr(v,100,20), length(v), substr(w,200,20) from toast_two_slices')" = "\\x$(printf '%s' "$binary_hex" | cut -c 199-238)|3000|\\x$(printf '%s' "$binary_hex_other" | cut -c 399-438)" ]
[ "$(run -c 'select substr(v,100,20), substr(v,100,20) from toast_two_slices')" = "\\x$(printf '%s' "$binary_hex" | cut -c 199-238)|\\x$(printf '%s' "$binary_hex" | cut -c 199-238)" ]
[ "$(run -c "select substr(w,200,20) from toast_two_slices where v = '\\x$binary_hex'")" = "\\x$(printf '%s' "$binary_hex_other" | cut -c 399-438)" ]
[ "$(run -c 'select substr(v,NULL), substr(w,200,20) from toast_two_slices')" = "|\\x$(printf '%s' "$binary_hex_other" | cut -c 399-438)" ]
[ "$(run -c 'select substr(v,NULL) from toast_bytes where id + 0 = 1')" = '' ]
[ "$(run -c 'select substr(v,1,NULL) from toast_bytes where id + 0 = 1')" = '' ]
[ "$(run -c 'select substr(v,1,-1) from toast_bytes where id + 0 = 0')" = '' ]
[ "$(run -c 'select substr(v,1,-1) from toast_bytes limit 0')" = '' ]
printf '%s\n(quit)\n' '(format t "LOOKUPS=~d~%" (symbol-value (intern "*SLICE-LOOKUPS*"))) (setf (symbol-function (quote btree-search-unlocked)) (symbol-value (intern "*SLICE-LOOKUP-ORIGINAL*")))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/slice-restore.out"
! grep -q ';; error:' "$TMP/slice-restore.out"
[ "$(awk -F= '/LOOKUPS=/{print $2}' "$TMP/slice-restore.out")" = 13 ]
run -c "insert into toast_two_slices values (2, '\\x010203', NULL), (3, NULL, '\\x040506')" >/dev/null
[ "$(run -c 'select substr(v,2,2), substr(w,2,2) from toast_two_slices where id = 2')" = '\x0203|' ]
[ "$(run -c 'select substr(v,2,2), substr(w,2,2) from toast_two_slices where id = 3')" = '|\x0506' ]
if run -c 'select 1/0, substr(w,1,-1) from toast_two_slices where id = 1' >"$TMP/slice-order.out" 2>&1; then
    echo 'toast-dml: expected first projection error' >&2; exit 1
fi
grep -q 'division by zero' "$TMP/slice-order.out"
if run -c 'select substr(w,1,-1), 1/0 from toast_two_slices where id = 1' >"$TMP/slice-order.out" 2>&1; then
    echo 'toast-dml: expected first projection error' >&2; exit 1
fi
grep -q 'negative substring length not allowed' "$TMP/slice-order.out"
if run -c 'select substr(v,1,-1) from toast_bytes where id + 0 = 1' >"$TMP/slice-negative.out" 2>&1; then
    echo 'toast-dml: negative substring length was accepted' >&2
    exit 1
fi
grep -q 'negative substring length not allowed' "$TMP/slice-negative.out"
run -c "insert into toast_bytes values (3, '\\x010203'), (4, NULL)" >/dev/null
[ "$(run -c 'select substr(v,2,2) from toast_bytes where id + 0 = 3')" = '\x0203' ]
[ "$(run -c 'select substr(v,2,2) from toast_bytes where id + 0 = 4')" = '' ]
[ "$(run -c 'select substr(v,1,-1) from toast_bytes where id + 0 = 4')" = '' ]
[ "$(run -c 'select substr(v,1,2) from toast_bytes where v is null')" = '' ]
run -c 'delete from toast_bytes where id > 2' >/dev/null
[ "$(run -c 'select substr(v,1995,20) from toast_bytes where id = 1')" = "\\x$(printf '%s' "$binary_hex" | cut -c 3989-4028)" ]
[ "$(run -c "select substr(v,2,2), v = '\\x$binary_hex' from toast_bytes where id + 0 = 1")" = "\\x$(printf '%s' "$binary_hex" | cut -c 3-6)|t" ]
[ "$(run -c 'select substring(v,0,3) from toast_bytes where id = 1')" = "\\x$(printf '%s' "$binary_hex" | cut -c 1-4)" ]
mkfifo "$TMP/slice-reader.sql"
run < "$TMP/slice-reader.sql" > "$TMP/slice-reader.out" 2>&1 &
READER=$!
exec 3> "$TMP/slice-reader.sql"
printf '%s\n' 'begin isolation level repeatable read; select substr(v,100,20) from toast_bytes where id + 0 = 1;' '\echo slice-ready' >&3
slice_wait=0
while ! grep -q '^slice-ready$' "$TMP/slice-reader.out"; do
    kill -0 "$READER" 2>/dev/null || { cat "$TMP/slice-reader.out" >&2; exit 1; }
    slice_wait=$((slice_wait + 1))
    [ "$slice_wait" -lt 300 ] || { echo 'toast-dml: slice reader timed out' >&2; exit 1; }
    sleep 0.1
done
run -c "update toast_bytes set v = '\\x00' where id = 1" >/dev/null
run -c 'vacuum toast_bytes' >/dev/null
printf '%s\n' 'select substr(v,100,20) from toast_bytes where id + 0 = 1; commit;' '\q' >&3
exec 3>&-
wait "$READER"
READER=
[ "$(grep -c "^\\\\x$(printf '%s' "$binary_hex" | cut -c 199-238)$" "$TMP/slice-reader.out")" = 2 ]
run -c "update toast_bytes set v = '\\x$binary_hex' where id = 1" >/dev/null
run -c 'create index toast_bytes_v_idx on toast_bytes (v)' >/dev/null
[ "$(run -c "select id from toast_bytes where v = '\\x$binary_hex'")" = 1 ]
[ "$(run -c "delete from toast_bytes where id = 1 returning v")" = "\\x$binary_hex" ]
# The first run encourages PGLZ, while the pseudorandom block makes its
# compressed result span multiple TOAST chunks.
binary_block=$(awk 'BEGIN { s=1; for (i=0;i<2100;i++) { s=(s*1664525+1013904223)%4294967296; printf "%02x", (i<200 ? 42 : int(s/16777216)) } }')
binary_multi="$binary_block$binary_block$binary_block"
run -c 'alter table toast_bytes alter column v set storage extended' >/dev/null
run -c "insert into toast_bytes values (2, '\\x$binary_multi')" >/dev/null
[ "$(run -c 'select length(v) from toast_bytes where id = 2')" = 6300 ]
[ "$(run -c 'select substr(v,200,30) from toast_bytes where id = 2')" = "\\x$(printf '%s' "$binary_multi" | cut -c 399-458)" ]
[ "$(run -c "delete from toast_bytes where id = 2 returning v")" = "\\x$binary_multi" ]
run -c 'create table toast_text (id int primary key, v text)' >/dev/null
run -c "insert into toast_text values (1, '$big')" >/dev/null
run -c 'alter table toast_text alter column v set storage external' >/dev/null
[ "$(run -c "select a.attstorage from pg_attribute a join pg_class c on a.attrelid = c.oid where c.relname = 'toast_text' and a.attname = 'v'")" = e ]
[ "$(run -c "select v = '$big' from toast_text where id = 1")" = t ]
run -c "insert into toast_text values (2, '$big')" >/dev/null
run -c 'alter table toast_text alter column v set storage plain' >/dev/null
[ "$(run -c "select v = '$big' from toast_text where id = 2")" = t ]
run -c 'alter table toast_text alter column v set storage default' >/dev/null
[ "$(run -c "select a.attstorage from pg_attribute a join pg_class c on a.attrelid = c.oid where c.relname = 'toast_text' and a.attname = 'v'")" = x ]
run -c 'delete from toast_text where id = 2' >/dev/null
if run -c 'alter table toast_text alter column id set storage extended' >/dev/null 2>&1; then
    echo 'toast-dml: fixed-width storage override was accepted' >&2
    exit 1
fi
for query in 'alter table toast_text alter column v set storage nonsense' \
             'alter table toast_text alter column absent set storage plain' \
             'alter table toast_text alter column ctid set storage plain'; do
    if run -c "$query" >/dev/null 2>&1; then
        echo "toast-dml: invalid storage alteration was accepted: $query" >&2
        exit 1
    fi
done
[ "$(run -c 'select length(v) from toast_text where id = 1')" = 3000 ]
[ "$(run -c "select v = '$big' from toast_text where id = 1")" = t ]
run -c 'create index toast_text_v_idx on toast_text (v)' >/dev/null
[ "$(run -c "select id from toast_text where v = '$big'")" = 1 ]
[ "$(run -c "update toast_text set v = '$other' where id = 1 returning length(v)")" = 3000 ]
[ "$(run -c 'delete from toast_text where id = 1 returning length(v)')" = 3000 ]
run -c 'create table toast_dml (id int primary key, v varchar)' >/dev/null
run -c "insert into toast_dml values (1, '$big')" >/dev/null
run -c 'create index toast_dml_v_idx on toast_dml (v)' >/dev/null
run -c 'alter table toast_dml add constraint value_length check (length(v) = 3000)' >/dev/null
run -c 'analyze toast_dml' >/dev/null
run -c 'vacuum full toast_dml' >/dev/null
[ "$(run -c 'select length(v) from toast_dml where id = 1')" = 3000 ]
[ "$(run -c "update toast_dml set v = '$other' where id = 1 returning length(v)")" = 3000 ]
[ "$(run -c "select v = '$other' from toast_dml where id = 1")" = t ]
[ "$(run -c 'select length(v) from toast_dml where id = 1')" = 3000 ]
run -c "begin; update toast_dml set v = '$big' where id = 1; rollback" >/dev/null
[ "$(run -c "select v = '$other' from toast_dml where id = 1")" = t ]
[ "$(run -c "insert into toast_dml values (1, '$other') on conflict (id) do update set v = excluded.v returning length(v)")" = 3000 ]
[ "$(run -c "select v = '$other' from toast_dml where id = 1")" = t ]
run -c 'begin; delete from toast_dml where id = 1; rollback' >/dev/null
[ "$(run -c "select v = '$other' from toast_dml where id = 1")" = t ]
[ "$(run -c 'delete from toast_dml where id = 1 returning length(v)')" = 3000 ]
[ -z "$(run -c 'select id from toast_dml')" ]
run -c "insert into toast_dml values (2, '$big')" >/dev/null
mkfifo "$TMP/reader.sql"
run < "$TMP/reader.sql" > "$TMP/reader.out" 2>&1 &
READER=$!
exec 3> "$TMP/reader.sql"
printf 'begin isolation level repeatable read; select length(v) from toast_dml where id = 2;\n\\echo ready\n' >&3
i=0
until grep -q '^ready$' "$TMP/reader.out"; do
    i=$((i + 1))
    [ "$i" -lt 100 ] || { cat "$TMP/reader.out" >&2; exit 1; }
    sleep 0.1
done
run -c "update toast_dml set v = '$other' where id = 2" >/dev/null
run -c 'vacuum toast_dml' >/dev/null
printf "select v = '%s' from toast_dml where id = 2; commit;\n\\q\n" "$big" >&3
exec 3>&-
wait "$READER"
READER=
grep -q '^t$' "$TMP/reader.out"
run -c "update toast_dml set v = '$big' where id = 2" >/dev/null
run -c 'begin; truncate toast_dml; rollback' >/dev/null
[ "$(run -c "select v = '$big' from toast_dml where id = 2")" = t ]
run -c 'truncate toast_dml' >/dev/null
[ -z "$(run -c 'select id from toast_dml')" ]
run -c 'vacuum toast_dml' >/dev/null
numeric_big=$(awk 'BEGIN { s=1; printf "7"; for (i=1;i<20000;i++) { s=(s*1664525+1013904223)%4294967296; printf "%d", int(s/16777216)%10 } }')
numeric_repeat=$(awk 'BEGIN { for (i=0;i<20000;i++) printf "1" }')
numeric_cast_value=$(awk 'BEGIN { for (i=0;i<700;i++) printf "1" }')
numeric_cast_source_pad=$(awk 'BEGIN { for (i=0;i<1900;i++) printf "x" }')
numeric_cast_target_pad=$(awk 'BEGIN { for (i=0;i<7900;i++) printf "x" }')
run -c 'create table toast_numeric_cast_source (v numeric(900,0), pad text)' >/dev/null
run -c 'alter table toast_numeric_cast_source alter column pad set storage plain' >/dev/null
run -c "insert into toast_numeric_cast_source values ('$numeric_cast_value','$numeric_cast_source_pad')" >/dev/null
run -c 'create table toast_numeric_cast_target (v numeric, pad text)' >/dev/null
run -c 'alter table toast_numeric_cast_target alter column v set storage plain' >/dev/null
run -c 'alter table toast_numeric_cast_target alter column pad set storage plain' >/dev/null
run -c "insert into toast_numeric_cast_target select v::numeric(1000,0),'$numeric_cast_target_pad' from toast_numeric_cast_source" >/dev/null
if run -v VERBOSITY=verbose -c "insert into toast_numeric_cast_target select v::numeric(800,0),'$numeric_cast_target_pad' from toast_numeric_cast_source" >"$TMP/numeric-cast-narrow.out" 2>&1; then
    echo 'toast-dml: narrowing numeric cast retained compression' >&2; exit 1
fi
case $(cat "$TMP/numeric-cast-narrow.out") in
    *54000*) ;;
    *) echo 'toast-dml: narrowing numeric cast did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
if run -v VERBOSITY=verbose -c "insert into toast_numeric_cast_target select v::numeric(1000,1),'$numeric_cast_target_pad' from toast_numeric_cast_source" >"$TMP/numeric-cast-scale.out" 2>&1; then
    echo 'toast-dml: numeric scale cast retained compression' >&2; exit 1
fi
case $(cat "$TMP/numeric-cast-scale.out") in
    *54000*) ;;
    *) echo 'toast-dml: numeric scale cast did not report SQLSTATE 54000' >&2; exit 1 ;;
esac
run -c 'create table storage_restart (v text)' >/dev/null
run -c 'alter table storage_restart alter column v set storage external' >/dev/null
run -c "insert into storage_restart values ('$numeric_repeat')" >/dev/null
run -c 'vacuum full storage_restart' >/dev/null
[ "$(run -c 'select length(v) from storage_restart')" = 20000 ]
run -c 'create table rewrite_plain (v text)' >/dev/null
run -c 'alter table rewrite_plain alter column v set storage external' >/dev/null
run -c "insert into rewrite_plain values ('$big')" >/dev/null
run -c 'alter table rewrite_plain alter column v set storage plain' >/dev/null
run -c 'vacuum full rewrite_plain' >/dev/null
[ "$(run -c "select v = '$big' from rewrite_plain")" = t ]
run -c 'create table rewrite_snapshot (id int primary key, v text)' >/dev/null
run -c 'alter table rewrite_snapshot alter column v set storage external' >/dev/null
run -c "insert into rewrite_snapshot values (1, '$big')" >/dev/null
mkfifo "$TMP/rewrite-reader.sql"
run < "$TMP/rewrite-reader.sql" > "$TMP/rewrite-reader.out" 2>&1 &
READER=$!
exec 3> "$TMP/rewrite-reader.sql"
printf 'begin isolation level repeatable read; select 1;\n\\echo ready\n' >&3
i=0
until grep -q '^ready$' "$TMP/rewrite-reader.out"; do
    i=$((i + 1))
    [ "$i" -lt 100 ] || { cat "$TMP/rewrite-reader.out" >&2; exit 1; }
    sleep 0.1
done
run -c "update rewrite_snapshot set v = '$other' where id = 1" >/dev/null
run -c 'vacuum full rewrite_snapshot' >/dev/null
run -c 'vacuum rewrite_snapshot' >/dev/null
printf "select v = '%s' from rewrite_snapshot where id = 1; commit;\n\\q\n" "$big" >&3
exec 3>&-
wait "$READER"
READER=
grep -q '^t$' "$TMP/rewrite-reader.out"
run -c 'vacuum rewrite_snapshot' >/dev/null
[ "$(run -c "select v = '$other' from rewrite_snapshot where id = 1")" = t ]
run -c 'alter table storage_restart alter column v set storage plain' >/dev/null
[ "$(run -c 'select length(v) from storage_restart')" = 20000 ]
if run -c 'vacuum full storage_restart' >/dev/null 2>&1; then
    echo 'toast-dml: VACUUM FULL accepted an oversized PLAIN tuple' >&2
    exit 1
fi
[ "$(run -c 'select length(v) from storage_restart')" = 20000 ]
kill "$SRV"
wait "$SRV" || true
SRV=
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" --control-socket "$TMP/control.sock"
test_wait_owned_sql_server "$SRV" "$PORT" || { cat "$LOG" >&2; exit 1; }
[ "$(run -c 'select length(v) from storage_restart')" = 20000 ]
[ "$(run -c "select v = '$big' from rewrite_plain")" = t ]
if run -c "insert into storage_restart values ('$numeric_repeat')" >/dev/null 2>&1; then
    echo 'toast-dml: PLAIN accepted a tuple exceeding a heap page' >&2
    exit 1
fi
run -c 'alter table storage_restart alter column v set storage main' >/dev/null
run -c 'create table vacuum_inline_text (v text)' >/dev/null
run -c "insert into vacuum_inline_text values ('$numeric_repeat')" >/dev/null
run -c 'vacuum full vacuum_inline_text' >/dev/null
[ "$(run -c 'select length(v) from vacuum_inline_text')" = 20000 ]
# Keep an old snapshot across updates which retain the external value, then
# replace and roll back the value while VACUUM must preserve the old version.
run -c 'create table toast_reuse (id int primary key, marker int, v text)' >/dev/null
run -c 'alter table toast_reuse alter column v set storage external' >/dev/null
run -c "insert into toast_reuse values (1, 0, '$big')" >/dev/null
reuse_pointer() {
    printf '%s\n(quit)\n' '(let* ((db (gethash "postgres" (catalog-databases *catalog*))) (table (gethash "toast_reuse" (database-tables db))) (snapshot (make-snapshot 0)) (ids nil)) (heapam-scan (pg-class-relfilenode table) (pg-class-columns table) snapshot (lambda (tuple) (let ((value (aref (htup-values tuple) 2))) (unless (heap-external-varlena-p value) (error "value is not external")) (push (heap-external-varlena-value-id value) ids)))) (unless (= (length ids) 1) (error "unexpected visible row count")) (format t "VALUE-ID=~d~%" (first ids)))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/reuse-pointer.out"
    if grep -q ';; error:' "$TMP/reuse-pointer.out"; then cat "$TMP/reuse-pointer.out" >&2; exit 1; fi
    awk -F= '/VALUE-ID=/{print $2}' "$TMP/reuse-pointer.out"
}
original_pointer=$(reuse_pointer)
[ -n "$original_pointer" ]
toast_reuse_chunk_count() {
    printf '%s\n(quit)\n' "(let* ((db (gethash \"postgres\" (catalog-databases *catalog*))) (table (gethash \"toast_reuse\" (database-tables db))) (node (pg-toast-index-relfilenode (pg-class-toast table)))) (call-with-btree-relation-read-lock node (lambda () (format t \"CHUNKS=~d~%\" (length (btree-prefix-entries-unlocked node (encode-index-key (list $1) '(:oid))))))))" | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/reuse-chunks.out"
    if grep -q ';; error:' "$TMP/reuse-chunks.out"; then cat "$TMP/reuse-chunks.out" >&2; exit 1; fi
    awk -F= '/CHUNKS=/{print $2}' "$TMP/reuse-chunks.out"
}
original_chunks=$(toast_reuse_chunk_count "$original_pointer")
[ "$original_chunks" -gt 1 ]
# An optimized INSERT SELECT omits v from the source projection; conflict
# handling must still materialize v from the existing target row.
[ "$(run -c "insert into toast_reuse (id, marker) select id, marker from toast_reuse on conflict (id) do update set marker = excluded.marker + 1 returning v = '$big'")" = t ]
[ "$(run -c "select marker, v = '$big' from toast_reuse where id = 1")" = '1|t' ]
[ "$(reuse_pointer)" = "$original_pointer" ]
[ "$(run -c "insert into toast_reuse (id, marker) select id, marker from toast_reuse order by id on conflict (id) do update set marker = excluded.marker returning v = '$big'")" = t ]
[ "$(run -c "insert into toast_reuse (id, marker) select id, marker from toast_reuse order by id limit 1 on conflict (id) do update set marker = excluded.marker returning v = '$big'")" = t ]
[ "$(reuse_pointer)" = "$original_pointer" ]
run -c 'create table toast_index_demand (id int primary key, v text)' >/dev/null
run -c 'alter table toast_index_demand alter column v set storage external' >/dev/null
index_rows=$(awk 'BEGIN { for (i=2;i<=1001;i++) printf "%s(%d, NULL)", i==2 ? "" : ",", i }')
run -c "insert into toast_index_demand values $index_rows, (1, '$big')" >/dev/null
run -c 'analyze toast_index_demand' >/dev/null
run -c 'create table toast_sort_demand (id int, v text)' >/dev/null
run -c 'alter table toast_sort_demand alter column v set storage external' >/dev/null
run -c "insert into toast_sort_demand values (1, 'c$big'), (2, 'a$big'), (3, 'b$big')" >/dev/null
# Count actual TOAST fetches during live SQL execution. A query that needs only
# id must not touch v; fetching v and using it in a filter must still work.
printf '%s\n(quit)\n' '(let ((counter (intern "*TOAST-TEST-FETCH-COUNT*")) (original (symbol-function (quote toast-fetch-value)))) (setf (symbol-value counter) 0 (symbol-value (intern "*TOAST-TEST-FETCH-ORIGINAL*")) original) (setf (symbol-function (quote toast-fetch-value)) (lambda (&rest args) (incf (symbol-value counter)) (apply original args))))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/toast-fetch-install.out"
! grep -q ';; error:' "$TMP/toast-fetch-install.out"
toast_fetch_count() {
    printf '%s\n(quit)\n' '(format t "FETCH-COUNT=~d~%" (symbol-value (intern "*TOAST-TEST-FETCH-COUNT*")))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/toast-fetch-count.out"
    if grep -q ';; error:' "$TMP/toast-fetch-count.out"; then cat "$TMP/toast-fetch-count.out" >&2; exit 1; fi
    awk -F= '/FETCH-COUNT=/{print $2}' "$TMP/toast-fetch-count.out"
}
[ "$(run -c 'select id from toast_reuse')" = 1 ]
[ "$(toast_fetch_count)" = 0 ]
# The primary-key equality uses a nonparameterized index scan. Its recheck
# needs id, but should not fetch the unreferenced external value.
run -c 'explain select id from toast_index_demand where id = 1' | grep -q 'Index Scan using'
[ "$(run -c 'select id from toast_index_demand where id = 1')" = 1 ]
[ "$(toast_fetch_count)" = 0 ]
[ "$(run -c 'select id + 1 from toast_reuse where id + 0 = 1')" = 2 ]
[ "$(toast_fetch_count)" = 0 ]
[ "$(run -c 'select abs(id) from toast_reuse where id + 0 = 1')" = 1 ]
[ "$(toast_fetch_count)" = 0 ]
run -c 'explain select id from toast_reuse order by id' | grep -q 'Sort'
[ "$(run -c 'select id from toast_reuse order by id')" = 1 ]
[ "$(toast_fetch_count)" = 0 ]
run -c 'explain select id from toast_reuse order by id limit 1' | grep -q 'Top-N'
[ "$(run -c 'select id from toast_reuse order by id limit 1')" = 1 ]
[ "$(toast_fetch_count)" = 0 ]
# Sort and top-N must retain unprojected ordering values, while sorting by an
# alias or ordinal only needs the selected id. Force and count external runs.
printf '%s\n(quit)\n' '(let ((counter (intern "*TOAST-TEST-SORT-COUNT*")) (original (symbol-function (quote write-query-sort-run)))) (setf (symbol-value counter) 0 (symbol-value (intern "*TOAST-TEST-SORT-ORIGINAL*")) original (symbol-value (intern "*TOAST-TEST-WORK-ORIGINAL*")) *query-work-memory-bytes* *query-work-memory-bytes* 1) (setf (symbol-function (quote write-query-sort-run)) (lambda (&rest args) (incf (symbol-value counter)) (apply original args))))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/toast-sort-install.out"
! grep -q ';; error:' "$TMP/toast-sort-install.out"
[ "$(run -c 'select id as k from toast_sort_demand order by k')" = "$(printf '1\n2\n3')" ]
[ "$(run -c 'select id from toast_sort_demand order by 1 limit 2')" = "$(printf '1\n2')" ]
[ "$(run -c 'select id from toast_sort_demand order by id + 1 desc')" = "$(printf '3\n2\n1')" ]
[ "$(toast_fetch_count)" = 0 ]
[ "$(run -c 'select id from toast_sort_demand order by v')" = "$(printf '2\n3\n1')" ]
fetch_after_multi_sort=$(toast_fetch_count)
[ "$fetch_after_multi_sort" = 3 ]
[ "$(run -c 'select id from toast_sort_demand order by v limit 2')" = "$(printf '2\n3')" ]
[ "$(toast_fetch_count)" = 6 ]
printf '%s\n(quit)\n' '(format t "SORT-COUNT=~d~%" (symbol-value (intern "*TOAST-TEST-SORT-COUNT*"))) (setf (symbol-function (quote write-query-sort-run)) (symbol-value (intern "*TOAST-TEST-SORT-ORIGINAL*")) *query-work-memory-bytes* (symbol-value (intern "*TOAST-TEST-WORK-ORIGINAL*")))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/toast-sort-restore.out"
! grep -q ';; error:' "$TMP/toast-sort-restore.out"
[ "$(awk -F= '/SORT-COUNT=/{print $2}' "$TMP/toast-sort-restore.out")" -gt 0 ]
[ "$(run -c 'select id from toast_reuse order by v limit 1')" = 1 ]
fetch_after_order=$(toast_fetch_count)
[ "$fetch_after_order" -gt 6 ]
[ "$(run -c "select v = '$big' from toast_index_demand where id = 1")" = t ]
fetch_after_index=$(toast_fetch_count)
[ "$fetch_after_index" -gt "$fetch_after_order" ]
[ "$(run -c "select v = '$big' from toast_reuse where id = 1")" = t ]
fetch_after_projection=$(toast_fetch_count)
[ "$fetch_after_projection" -gt "$fetch_after_index" ]
[ "$(run -c "select id from toast_reuse where v = '$big'")" = 1 ]
fetch_after_filter=$(toast_fetch_count)
[ "$fetch_after_filter" -gt "$fetch_after_projection" ]
# A subquery expression is not analyzed for demand, so it must fall back to
# full materialization even though its result does not use the large value.
[ "$(run -c 'select (select 1) from toast_reuse')" = 1 ]
[ "$(toast_fetch_count)" -gt "$fetch_after_filter" ]
printf '%s\n(quit)\n' '(setf (symbol-function (quote toast-fetch-value)) (symbol-value (intern "*TOAST-TEST-FETCH-ORIGINAL*")))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/toast-fetch-restore.out"
! grep -q ';; error:' "$TMP/toast-fetch-restore.out"
mkfifo "$TMP/reuse-reader.sql"
run < "$TMP/reuse-reader.sql" > "$TMP/reuse-reader.out" 2>&1 &
READER=$!
exec 3> "$TMP/reuse-reader.sql"
printf "%s\n" "begin isolation level repeatable read; select v = '$big' from toast_reuse where id = 1;" '\echo reuse-ready' >&3
i=0
until grep -Fxq 'reuse-ready' "$TMP/reuse-reader.out"; do
    i=$((i + 1))
    [ "$i" -lt 100 ] || { cat "$TMP/reuse-reader.out" >&2; exit 1; }
    sleep 0.1
done
run -c 'update toast_reuse set marker = 1 where id = 1' >/dev/null
[ "$(reuse_pointer)" = "$original_pointer" ]
run -c "insert into toast_reuse values (1, 2, '$other') on conflict (id) do update set marker = excluded.marker" >/dev/null
[ "$(run -c "select marker, v = '$big' from toast_reuse where id = 1")" = '2|t' ]
[ "$(reuse_pointer)" = "$original_pointer" ]
run -c "begin; update toast_reuse set v = '$other', marker = 3 where id = 1; rollback" >/dev/null
[ "$(run -c "select marker, v = '$big' from toast_reuse where id = 1")" = '2|t' ]
[ "$(reuse_pointer)" = "$original_pointer" ]
run -c "update toast_reuse set v = '$other', marker = 4 where id = 1" >/dev/null
replacement_pointer=$(reuse_pointer)
[ -n "$replacement_pointer" ]
[ "$replacement_pointer" != "$original_pointer" ]
run -c 'vacuum toast_reuse' >/dev/null
[ "$(run -c "select marker, v = '$other' from toast_reuse where id = 1")" = '4|t' ]
[ "$(toast_reuse_chunk_count "$original_pointer")" = "$original_chunks" ]
printf "select v = '%s' from toast_reuse where id = 1; commit;\n\\q\n" "$big" >&3
exec 3>&-
wait "$READER"
READER=
[ "$(grep -Fxc 't' "$TMP/reuse-reader.out")" = 2 ]
run -c 'vacuum toast_reuse' >/dev/null
[ "$(toast_reuse_chunk_count "$original_pointer")" = 0 ]
[ "$(toast_reuse_chunk_count "$replacement_pointer")" -gt 1 ]
[ "$(run -c "select marker, v = '$other' from toast_reuse where id = 1")" = '4|t' ]
# A concurrent updater must follow the latest committed tuple before reusing
# the external value; its scan starts while the replacement is uncommitted.
mkfifo "$TMP/reuse-writer.sql"
run < "$TMP/reuse-writer.sql" > "$TMP/reuse-writer.out" 2>&1 &
READER=$!
exec 3> "$TMP/reuse-writer.sql"
printf '%s\n' "begin; update toast_reuse set v = '$big', marker = 5 where id = 1;" '\echo replacement-ready' >&3
i=0
until grep -Fxq 'replacement-ready' "$TMP/reuse-writer.out"; do
    i=$((i + 1))
    [ "$i" -lt 100 ] || { cat "$TMP/reuse-writer.out" >&2; exit 1; }
    sleep 0.1
done
toast_next_id() {
    printf '%s\n(quit)\n' '(format t "NEXT-ID=~d~%" *toast-next-value-id*)' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/reuse-next-id.out"
    if grep -q ';; error:' "$TMP/reuse-next-id.out"; then cat "$TMP/reuse-next-id.out" >&2; exit 1; fi
    awk -F= '/NEXT-ID=/{print $2}' "$TMP/reuse-next-id.out"
}
replacement_next_id=$(toast_next_id)
[ -n "$replacement_next_id" ]
run -c 'select pg_backend_pid()' -c 'update toast_reuse set marker = 6 where id = 1' >"$TMP/reuse-waiter.out" 2>&1 &
WRITER=$!
i=0
until waiter_pid=$(awk '/^[0-9]+$/{print; exit}' "$TMP/reuse-waiter.out") && [ -n "$waiter_pid" ]; do
    kill -0 "$WRITER" 2>/dev/null || { cat "$TMP/reuse-waiter.out" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 100 ] || exit 1; sleep 0.1
done
printf '%s\n(quit)\n' "(loop repeat 100 for backend = (lookup-backend $waiter_pid) when (and backend (backend-waiting-xid backend)) return :waiting do (sleep 0.05) finally (return :timeout))" | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/reuse-wait.out"
grep -Fq ':WAITING' "$TMP/reuse-wait.out" || { cat "$TMP/reuse-wait.out" >&2; exit 1; }
printf '%s\n' 'commit;' '\q' >&3
exec 3>&-
wait "$READER"
READER=
wait "$WRITER"
WRITER=
[ "$(run -c "select marker, v = '$big' from toast_reuse where id = 1")" = '6|t' ]
[ "$(toast_next_id)" = "$replacement_next_id" ]
latest_pointer=$(reuse_pointer)
[ -n "$latest_pointer" ]
[ "$latest_pointer" != "$replacement_pointer" ]
run -c 'create table toast_numeric (id int primary key, v numeric)' >/dev/null
numeric_toast_oid=$(run -c "select reltoastrelid from pg_class where relname = 'toast_numeric'")
[ "$numeric_toast_oid" -gt 0 ]
[ "$(run -c "select relkind from pg_class where oid = $numeric_toast_oid")" = t ]
[ "$(run -c "select count(*) from pg_index where indrelid = $numeric_toast_oid")" = 1 ]
[ "$(run -c "select count(*) from pg_attribute where attrelid = $numeric_toast_oid")" = 3 ]
[ "$(run -c "select a.attstorage from pg_attribute a join pg_class c on a.attrelid = c.oid where c.relname = 'toast_numeric' and a.attname = 'v'")" = m ]
[ "$(run -c "select count(*) from pg_type where typname in ('text','bytea','json','jsonb','numeric')")" = 5 ]
run -c "insert into toast_numeric values (1, '$numeric_big'::numeric), (2, '$numeric_repeat'::numeric), (3, 'NaN'::numeric)" >/dev/null
[ "$(run -c 'select length(v::text) from toast_numeric where id = 1')" = 20000 ]
[ "$(run -c "select v = '$numeric_big'::numeric from toast_numeric where id = 1")" = t ]
[ "$(run -c "select v = '$numeric_repeat'::numeric from toast_numeric where id = 2")" = t ]
[ "$(run -c 'select v from toast_numeric where id = 3')" = NaN ]
run -c "begin; update toast_numeric set v = '$numeric_repeat'::numeric where id = 1; rollback" >/dev/null
[ "$(run -c "select v = '$numeric_big'::numeric from toast_numeric where id = 1")" = t ]
run -c 'vacuum full toast_numeric' >/dev/null
[ "$(run -c 'select length(v::text) from toast_numeric where id = 2')" = 20000 ]
[ "$(run -c "select v = '$numeric_repeat'::numeric from toast_numeric where id = 2")" = t ]
kill "$SRV"
wait "$SRV" || true
SRV=
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" --control-socket "$TMP/control.sock"
test_wait_owned_sql_server "$SRV" "$PORT" || { cat "$LOG" >&2; exit 1; }
[ "$(run -c "select a.attstorage from pg_attribute a join pg_class c on a.attrelid = c.oid where c.relname = 'storage_restart' and a.attname = 'v'")" = m ]
[ "$(run -c 'select length(v) from storage_restart')" = 20000 ]
[ "$(run -c "select marker, v = '$big' from toast_reuse where id = 1")" = '6|t' ]
[ "$(reuse_pointer)" = "$latest_pointer" ]
# Multiple updates in one transaction must keep the new pointer after the
# replacement, and an aborted insert/reuse/replacement must leave no row.
transaction_next_id=$(toast_next_id)
run -c "begin; update toast_reuse set marker = 7 where id = 1; update toast_reuse set v = '$other', marker = 8 where id = 1; update toast_reuse set marker = 9 where id = 1; commit" >/dev/null
transaction_pointer=$(reuse_pointer)
[ "$transaction_pointer" != "$latest_pointer" ]
[ "$(toast_next_id)" -eq "$((transaction_next_id + 1))" ]
[ "$(run -c "select marker, v = '$other' from toast_reuse where id = 1")" = '9|t' ]
run -c "begin; insert into toast_reuse values (2, 0, '$big'); update toast_reuse set marker = 1 where id = 2; update toast_reuse set v = '$other' where id = 2; rollback" >/dev/null
[ "$(run -c 'select count(*) from toast_reuse where id = 2')" = 0 ]
[ "$(reuse_pointer)" = "$transaction_pointer" ]
run -c 'vacuum toast_reuse' >/dev/null
[ "$(run -c "select marker, v = '$other' from toast_reuse where id = 1")" = '9|t' ]
[ "$(reuse_pointer)" = "$transaction_pointer" ]
# PLAIN affects newly assigned values, while an omitted column keeps the
# already external datum even after its storage policy changes.
run -c 'alter table toast_reuse alter column v set storage plain' >/dev/null
run -c 'update toast_reuse set marker = 10 where id = 1' >/dev/null
[ "$(reuse_pointer)" = "$transaction_pointer" ]
run -c "update toast_reuse set v = '$other', marker = 11 where id = 1" >/dev/null
[ "$(run -c "select marker, v = '$other' from toast_reuse where id = 1")" = '11|t' ]
printf '%s\n(quit)\n' '(let* ((db (gethash "postgres" (catalog-databases *catalog*))) (table (gethash "toast_reuse" (database-tables db))) (snapshot (make-snapshot 0)) (count 0)) (heapam-scan (pg-class-relfilenode table) (pg-class-columns table) snapshot (lambda (tuple) (incf count) (when (heap-external-varlena-p (aref (htup-values tuple) 2)) (error "assigned PLAIN value is external")))) (unless (= count 1) (error "unexpected visible row count")) (format t "PLAIN-OK~%"))' | sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/reuse-plain.out"
grep -Fq 'PLAIN-OK' "$TMP/reuse-plain.out"
! grep -q ';; error:' "$TMP/reuse-plain.out"
run -c 'vacuum toast_reuse' >/dev/null
[ "$(run -c "select marker, v = '$other' from toast_reuse where id = 1")" = '11|t' ]
[ "$(run -c "select reltoastrelid from pg_class where relname = 'toast_numeric'")" = "$numeric_toast_oid" ]
[ "$(run -c "select v = '$numeric_big'::numeric from toast_numeric where id = 1")" = t ]
[ "$(run -c 'select length(v::text) from toast_numeric where id = 2')" = 20000 ]
[ "$(run -c "select v = '$numeric_repeat'::numeric from toast_numeric where id = 2")" = t ]
[ "$(run -c 'delete from toast_numeric where id = 1 returning length(v::text)')" = 20000 ]
run -c 'drop table toast_numeric' >/dev/null
[ "$(run -c "select count(*) from pg_class where oid = $numeric_toast_oid")" = 0 ]
[ "$(run -c "select count(*) from pg_attribute where attrelid = $numeric_toast_oid")" = 0 ]
[ "$(run -c "select count(*) from pg_index where indrelid = $numeric_toast_oid")" = 0 ]
echo 'toast-dml: PASS'

#!/bin/sh
set -eu
BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5491}
TMP=$(mktemp -d /tmp/pg-toast-dml.XXXXXX)
LOG="$TMP/server.log"
SRV=
READER=
. "$BASE/t/test-support.sh"
test_debug_setup toast-dml "$PORT" "$TMP" "$LOG"
cleanup() {
    [ -z "$READER" ] || kill "$READER" 2>/dev/null || true
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$READER" ] || wait "$READER" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup
run() {
    PGPASSWORD=x psql -X -q -A -t -h 127.0.0.1 -p "$PORT" \
        -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data"
test_wait_owned_sql_server "$SRV" "$PORT" || { cat "$LOG" >&2; exit 1; }
big=$(awk 'BEGIN { for (i=0;i<3000;i++) printf "a" }')
other=$(awk 'BEGIN { for (i=0;i<3000;i++) printf "b" }')
binary_hex=$(awk 'BEGIN { for (i=0;i<1500;i++) printf "00ff" }')
run -c 'create table toast_bytes (id int primary key, v bytea)' >/dev/null
run -c "insert into toast_bytes values (1, '\\x$binary_hex')" >/dev/null
[ "$(run -c 'select length(v) from toast_bytes where id = 1')" = 3000 ]
[ "$(run -c "select v = '\\x$binary_hex' from toast_bytes where id = 1")" = t ]
run -c 'create index toast_bytes_v_idx on toast_bytes (v)' >/dev/null
[ "$(run -c "select id from toast_bytes where v = '\\x$binary_hex'")" = 1 ]
[ "$(run -c "delete from toast_bytes where id = 1 returning v")" = "\\x$binary_hex" ]
run -c 'create table toast_text (id int primary key, v text)' >/dev/null
run -c "insert into toast_text values (1, '$big')" >/dev/null
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
echo 'toast-dml: PASS'

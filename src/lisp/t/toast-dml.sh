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
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data"
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
run -c 'create table toast_bytes (id int primary key, v bytea)' >/dev/null
run -c "insert into toast_bytes values (1, '\\x$binary_hex')" >/dev/null
[ "$(run -c 'select length(v) from toast_bytes where id = 1')" = 3000 ]
[ "$(run -c "select v = '\\x$binary_hex' from toast_bytes where id = 1")" = t ]
run -c 'create index toast_bytes_v_idx on toast_bytes (v)' >/dev/null
[ "$(run -c "select id from toast_bytes where v = '\\x$binary_hex'")" = 1 ]
[ "$(run -c "delete from toast_bytes where id = 1 returning v")" = "\\x$binary_hex" ]
# The first run encourages PGLZ, while the pseudorandom block makes its
# compressed result span multiple TOAST chunks.
binary_block=$(awk 'BEGIN { s=1; for (i=0;i<2100;i++) { s=(s*1664525+1013904223)%4294967296; printf "%02x", (i<200 ? 42 : int(s/16777216)) } }')
binary_multi="$binary_block$binary_block$binary_block"
run -c "insert into toast_bytes values (2, '\\x$binary_multi')" >/dev/null
[ "$(run -c 'select length(v) from toast_bytes where id = 2')" = 6300 ]
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
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data"
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
test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data"
test_wait_owned_sql_server "$SRV" "$PORT" || { cat "$LOG" >&2; exit 1; }
[ "$(run -c "select a.attstorage from pg_attribute a join pg_class c on a.attrelid = c.oid where c.relname = 'storage_restart' and a.attname = 'v'")" = m ]
[ "$(run -c 'select length(v) from storage_restart')" = 20000 ]
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

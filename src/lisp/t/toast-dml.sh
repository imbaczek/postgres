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
printf "select v = '%s' from toast_reuse where id = 1; commit;\n\\q\n" "$big" >&3
exec 3>&-
wait "$READER"
READER=
[ "$(grep -Fxc 't' "$TMP/reuse-reader.out")" = 2 ]
run -c 'vacuum toast_reuse' >/dev/null
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

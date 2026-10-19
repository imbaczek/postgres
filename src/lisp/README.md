# pg-lisp — a minimal PostgreSQL-wire server in Common Lisp

`src/lisp/` implements a small, self-contained server that speaks enough of the
PostgreSQL frontend/backend protocol (v3) for `psql` to connect and run a
deliberately small SQL subset. It mirrors PostgreSQL's directory shape and
function names (`tcop`, `libpq`, `access/heap`, `access/nbtree`, `access/transam`,
`storage`, `catalog`, `parser`, `executor`) but implements only the MVP paths.

It is an educational port, not a production database. See "Known gaps" below.

## Requirements

- SBCL at `/home/imbaczek/proj/sbcl/run-sbcl.sh` (any recent SBCL with threads
  and `sb-bsd-sockets` should work; the launcher assumes that path).
- `psql` 16.x for the acceptance test.
- x86_64 Linux. Wire integers are big-endian; on-disk page integers are
  little-endian (native x86).

## Running

```sh
cd src/lisp
./pg-lisp.sh -p 5439 -D /tmp/pgdata
```

Then, in another shell:

```sh
psql -h 127.0.0.1 -p 5439 -U postgres -d postgres
```

Options: `-p PORT` (default 5433), `-h HOST` (default 127.0.0.1), `-D PGDATA`
(default `./pgdata`). PGDATA is created on first boot with `PG_VERSION`,
`global/`, `base/`, `pg_xact`, and `pg_catalog.dat`.

Authentication and authorization are accepted and ignored: any user/database is
trusted, and `postgres` is always the effective role. Only the simple query
protocol (`Q`) is implemented, which is all `psql` uses for the supported
statements.

## Hot reload

Send `SIGHUP` to a running server and it recompiles and reloads the `pg` system
in place, like PostgreSQL reloading configuration:

```sh
kill -HUP "$(pgrep -f 'sbcl.*boot.lisp')"
```

Functions are redefined live, so existing connections and global state (xact
counters, catalog, relation files) survive. Two caveats: a change to a
`DEFSTRUCT` layout needs a real restart, and a reload whose compilation fails
partway can leave a mixed image, so restart if the reload log shows an error.
`t/hot-reload.sh` exercises this.

## Supported SQL

Types: `int`/`integer`/`int4`, `bigint`/`int8`, `varchar(n)` / `character
varying(n)` with `n <= 126`. `varchar` without a length is treated as 126.

DDL:

```sql
CREATE TABLE t (a int, b varchar(20), PRIMARY KEY (a));
CREATE TABLE t (a int UNIQUE, b varchar(20));
DROP TABLE [IF EXISTS] t;
CREATE UNIQUE INDEX name ON t (cols);
CREATE INDEX name ON t (cols);          -- accepted; no scan path uses it
DROP INDEX [IF EXISTS] name;
ALTER TABLE t ADD PRIMARY KEY (cols);
ALTER TABLE t ADD UNIQUE (cols);
```

DML:

```sql
SELECT expr [AS alias], ... | * FROM t [WHERE e] [ORDER BY col [ASC|DESC] [, ...]] [LIMIT n];
INSERT INTO t [(cols)] VALUES (...)[, (...)];
UPDATE t SET col = expr [, ...] [WHERE e];
DELETE FROM t [WHERE e];
```

Transactions (READ COMMITTED only):

```sql
BEGIN | START TRANSACTION;
COMMIT | END;
ROLLBACK | ABORT;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

Expressions: column references, integer and string literals, `+ - * / %`,
comparisons, `AND` / `OR` / `NOT`, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE`, and
parentheses. Scalar functions: `version()`, `current_database()`,
`current_schema()`, `current_catalog()`, `current_user`, `session_user`, `user`,
`length()`.

`SET` and `SHOW` are accepted broadly and mostly remembered per session.
Meta-commands that need `pg_catalog` and regular-expression operators (`\d`,
`\dt`, `\l`) return an error from the server; `psql` prints it and continues.

## Testing

```sh
cd src/lisp
./t/run-unit-tests.sh     # Lisp-level page/tuple/index/visibility tests
./t/psql-smoke.sh         # boots a server and diffs psql output against a golden file
./t/index-stress.sh       # repeated updates to one unique value (slow; UPDATES=n to tune)
./t/hot-reload.sh         # SIGHUP reloads code in a running server
```

`t/psql-smoke.sh` is the acceptance test: it launches the server on a throwaway
PGDATA, runs `t/psql-smoke.sql` through a real `psql`, and compares the
transcript with `t/psql-smoke.out`.

`t/index-stress.sh` updates a single unique value a thousand times, then checks
that a fresh transaction still sees the live tuple as a duplicate.  It exercises
the case where equal index keys straddle many leaf splits.

## Storage and concurrency

- 8 KB pages with PostgreSQL's `PageHeaderData` + `ItemIdData` layout, one file
  per relation under `$PGDATA/base/<relfilenode>`.
- Heap tuples use PostgreSQL's `HeapTupleHeaderData` field order (`t_xmin`,
  `t_xmax`, `t_cid`, `t_ctid`, `t_infomask2`, `t_infomask`, `t_hoff`) followed
  by a null bitmap and attribute data. Attribute lookup is a single
  left-to-right walk (no offset table) — see `afm` notes in the code.
- Real MVCC: 64-bit XIDs, a CLog (`pg_xact`) with 2 status bits per XID, and a
  per-statement READ COMMITTED snapshot with `curcid`. `XIDs` are handed out in
  batches of 1000, with `global/pg_control` fsynced before any XID in the batch
  is used.
- Commit order: fsync every dirty heap/index file, then write + fsync CLog
  COMMITTED, then let the tuple become visible.
- The catalog (`pg_class`/`pg_attribute`/`pg_index` as structs) is serialized to
  `$PGDATA/global/pg_catalog.dat` and fsynced at each DDL commit. It is replaced
  atomically (write a temp file, `rename`, then fsync the directory), so a crash
  never leaves the catalog missing.
- Unique b-trees are real file-backed B+trees. Because duplicate keys can
  straddle a leaf split, a lookup visits every leaf that can hold the key. A
  uniqueness probe that lands on an in-progress conflicting tuple blocks until
  that XID ends, then re-evaluates.
- `UNIQUE` and `PRIMARY KEY` follow PostgreSQL's default NULLS DISTINCT
  semantics: a key containing a NULL never conflicts, though the index entry is
  still written.
- A disconnecting backend aborts any transaction it left open, so an abandoned
  XID (and the in-progress tuple it wrote) cannot block other sessions.
- Writers take a single global write mutex; DDL also takes a catalog mutex;
  page I/O is serialized under an smgr mutex. Readers do not block.

## Known gaps (documented, not hidden)

- No WAL and no crash recovery. A crash between the data fsync and the CLog
  commit loses that transaction; it is treated as aborted on restart. A
  returned `COMMIT` is therefore not power-fail safe.
- DDL inside an explicit transaction block is rejected (`0A000`) because the
  catalog has no rollback log.
- Catalog changes are fsynced at DDL statement time, while heap/index pages are
  fsynced at commit time. A crash in between can leave a durable catalog entry
  for a relation whose heap pages did not make it to disk; this is bounded by
  the same no-WAL commit hole as above.
- No joins, aggregates, subqueries, CTEs, or `DISTINCT`.
- No `GROUP BY` / `HAVING`, no `OFFSET`/`FETCH`. `ORDER BY` accepts multiple
  keys with `ASC`/`DESC`.
- No `DEFAULT`, `CHECK`, foreign keys, or `NOT NULL`/`UNIQUE` beyond what is
  listed above.
- `SELECT` always uses a sequential heap scan; indexes exist to enforce
  uniqueness, not yet to accelerate reads.
- No VACUUM; only lazy in-page pruning of dead tuples. Freed heap slots are
  recycled only when the table has no indexes (to avoid dangling index TIDs).
- Byte-wise comparison only, i.e. C-like collation for ASCII. Index key
  encoding is length-first and is used internally only.
- Numeric/float, date/time, boolean `text`, and other types are unsupported.
- Extended query protocol (Parse/Bind/Describe/Execute/Sync) is not
  implemented; `psql` is unaffected, other drivers are.
- `\d`, `\dt`, `\l` fail because the catalog SQL uses constructs beyond this
  subset (regex operators, `pg_catalog` functions); `psql` prints an error and
  shows no listing.

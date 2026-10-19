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
`global/`, `base/`, `pg_xact`, and `pg_catalog.dat`. Ports are validated at
startup (1..65535) and a missing option value is a clean error, not a crash.

Authentication and authorization are accepted and ignored: any user/database is
trusted, and `postgres` is always the effective role. Only the simple query
protocol (`Q`) is implemented, which is all `psql` uses for the supported
statements.

## Hot reload and control REPL

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

For interactive work there is also an out-of-band control REPL. It is **off by
default** because it evaluates arbitrary Lisp; enable it explicitly:

```sh
./pg-lisp.sh -p 5439 --control-port 5490 --control-socket /tmp/pg.sock
```

`--control-port` binds a TCP listener on `127.0.0.1` only; `--control-socket`
binds an AF_UNIX socket and `chmod`s the node to `0700`. Either or both may be
given. A clean shutdown removes the Unix socket on the way out, and a stale
socket is deleted at startup, so restarts are clean.

Then connect with any line-oriented client. Over TCP:

```sh
nc 127.0.0.1 5490          # or: socat - TCP:127.0.0.1:5490
```

Over the Unix socket:

```sh
nc -U /tmp/pg.sock         # or: socat - UNIX-CONNECT:/tmp/pg.sock
```

The REPL starts in the `pg` package and prints the value of each form you type.
Two helpers are provided: `(reload)` recompiles and reloads in place, and
`(quit)` closes the connection. Any other Lisp can be evaluated too, e.g. to
inspect server state. Reader evaluation (`#.`) is disabled, so a form is never
executed merely by being read; `eval` then runs the parsed form. `t/control-repl.sh`
exercises both transports and a reload through the REPL. Note that `telnet` sends
terminal-negotiation bytes that the reader will parse as Lisp; use `nc`/`socat`
instead.

## Shutdown and backend signaling

Signals follow PostgreSQL's postmaster:

| Signal | Mode | Behavior |
| --- | --- | --- |
| `SIGTERM` | smart | stop accepting new clients, wait for connected ones to leave, then exit 0 |
| `SIGINT` (Ctrl-C) | fast | abort active transactions, disconnect clients, exit 0 |
| `SIGQUIT` | immediate | exit 1 at once, skipping cleanup |

A later signal may escalate (smart → fast → immediate), never de-escalate.
While shutting down, new connections are rejected with `FATAL 57P03`
("the database system is shutting down") before authentication, exactly like
PostgreSQL's `canAcceptConnections()`.

The SQL admin functions mirror `signalfuncs.c`:

```sql
SELECT pg_backend_pid();
SELECT pg_cancel_backend(pid);                 -- ERROR 57014 in the target
SELECT pg_terminate_backend(pid [, timeout]);  -- FATAL 57P01, connection ends
SELECT pg_reload_conf();                       -- same as SIGHUP
```

`pg_cancel_backend` cancels the running statement at a safe point; a cancel
aimed at an idle backend is ignored, as in PostgreSQL. `pg_terminate_backend`
delivers the FATAL even to an idle backend and returns true once the signal is
sent. With a positive millisecond `timeout` it waits for the backend to
disappear and returns false if it does not; a negative timeout raises `22003`.
An unknown PID produces a `WARNING` and returns false. `t/shutdown.sh`
exercises all of this, including smart/fast/immediate shutdown and escalation.

## Supported SQL

Types: `int`/`integer`/`int4`, `bigint`/`int8`, `varchar(n)` / `character
varying(n)` with `n <= 126`, and `text`. For now, `text` is an alias for
`varchar(126)`; distinct text/varchar type semantics and OIDs are a TODO.
`varchar` without a length is also treated as 126.

DDL:

```sql
CREATE TABLE t (a int, b varchar(20), PRIMARY KEY (a));
CREATE TABLE t (a int UNIQUE, b varchar(20));
DROP TABLE [IF EXISTS] t;
CREATE UNIQUE INDEX name ON t (cols);
CREATE UNIQUE INDEX name ON t (cols) NULLS NOT DISTINCT;
CREATE INDEX name ON t (cols);          -- accepted; no scan path uses it
DROP INDEX [IF EXISTS] name;
ALTER INDEX name RENAME TO new_name;
ALTER TABLE t ADD PRIMARY KEY (cols);
ALTER TABLE t ADD PRIMARY KEY USING INDEX name;
ALTER TABLE t ADD UNIQUE [NULLS NOT DISTINCT] (cols);
ALTER TABLE t ADD UNIQUE USING INDEX name;
ALTER TABLE t RENAME CONSTRAINT old_name TO new_name;
ALTER TABLE t DROP CONSTRAINT name;     -- UNIQUE and PRIMARY KEY constraints
ALTER TABLE t DROP CONSTRAINT old_name,
  ADD CONSTRAINT new_name PRIMARY KEY USING INDEX index_name;
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
`length()`, `pg_backend_pid()`, `pg_cancel_backend(pid)`,
`pg_terminate_backend(pid [, timeout])`, `pg_reload_conf()`.

`SET` and `SHOW` are accepted broadly and mostly remembered per session.
Meta-commands that need `pg_catalog` and regular-expression operators (`\d`,
`\dt`, `\l`) return an error from the server; `psql` prints it and continues.

## Testing

```sh
cd src/lisp
./t/run-unit-tests.sh     # Lisp-level page/tuple/index/visibility tests
./t/psql-smoke.sh         # boots a server and diffs psql output against a golden file
./t/unique-index-concurrency.sh # unique-index build/drop writer races
./t/index-stress.sh       # repeated updates to one unique value (slow; UPDATES=n to tune)
./t/hot-reload.sh         # SIGHUP reloads code in a running server
./t/control-repl.sh       # control REPL over TCP + Unix socket, reload, SQL intact
./t/shutdown.sh           # signals, shutdown modes, cancel/terminate admin functions
```

`t/psql-smoke.sh` is the acceptance test: it launches the server on a throwaway
PGDATA, runs `t/psql-smoke.sql` through a real `psql`, and compares the
transcript with `t/psql-smoke.out`.

`t/index-stress.sh` updates a single unique value a thousand times, then checks
that a fresh transaction still sees the live tuple as a duplicate.  It exercises
the case where equal index keys straddle many leaf splits.

`t/unique-index-concurrency.sh` checks that unique-index builds and `DROP INDEX`
wait for active writers, and that `DROP TABLE` serializes with builds, readers,
and writers.

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
- Numeric/float, date/time, boolean, and other types are unsupported. `text` is
  temporarily treated as `varchar(126)`; full text/varchar semantics remain a
  TODO.
- Extended query protocol (Parse/Bind/Describe/Execute/Sync) is not
  implemented; `psql` is unaffected, other drivers are.
- `pg_terminate_backend` (and fast/immediate shutdown) unwinds the target with
  an SBCL thread interrupt at an arbitrary point instead of PostgreSQL's
  deferred safe-point check. A terminate in the middle of a page write can
  leave a torn page; the no-WAL crash model already tolerates that, but it is a
  real divergence from the original signal handling.
- `\d`, `\dt`, `\l` fail because the catalog SQL uses constructs beyond this
  subset (regex operators, `pg_catalog` functions); `psql` prints an error and
  shows no listing.

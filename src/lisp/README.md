# pg-lisp — a minimal PostgreSQL-wire server in Common Lisp

`src/lisp/` implements a small, self-contained server that speaks enough of the
PostgreSQL frontend/backend protocol (v3) for `psql` to connect and run a
deliberately small SQL subset. It mirrors PostgreSQL's directory shape and
function names (`tcop`, `libpq`, `access/heap`, `access/nbtree`, `access/transam`,
`storage`, `catalog`, `parser`, `executor`) but implements only the MVP paths.

It is an educational port, not a production database. See "Known gaps" below.

## Requirements

- SBCL with threads and `sb-bsd-sockets`, available as `sbcl` on `PATH`; set
  `SBCL=/path/to/sbcl` to use another executable.
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
`--swank-port PORT` optionally opens the Swank listener on `127.0.0.1`; the
Swank code is loaded in the image by default, but the listener stays off unless
this option is supplied. `--swank-bind-address ADDR` selects another interface
when `--swank-port` is also set; its default is `127.0.0.1`, and the address
option by itself never starts a listener.

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
CONTROL_DIR=$(mktemp -d)
./pg-lisp.sh -p 5439 --control-port 5490 \
  --control-socket "$CONTROL_DIR/pg.sock"
```

`--control-port` binds a TCP listener on `127.0.0.1` only; `--control-socket`
binds an AF_UNIX socket and `chmod`s the node to `0700`. Its parent directory
must already be owned by the server user and have mode `0700`. Either or both
may be given. A clean shutdown removes the Unix socket. After an unclean exit,
remove the stale socket path manually before restarting.

Then connect with any line-oriented client. Over TCP:

```sh
nc 127.0.0.1 5490          # or: socat - TCP:127.0.0.1:5490
```

Over the Unix socket:

```sh
nc -U "$CONTROL_DIR/pg.sock"  # or: socat - UNIX-CONNECT:"$CONTROL_DIR/pg.sock"
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
parentheses. Scalar functions: `version()`, `pg_lisp_runtime()`,
`current_database()`, `current_schema()`, `current_catalog()`, `current_user`,
`session_user`, `user`, `length()`, `pg_backend_pid()`,
`pg_cancel_backend(pid)`,
`pg_terminate_backend(pid [, timeout])`, `pg_reload_conf()`.

`SET TIME ZONE` is stored per backend and returned by `SHOW timezone`;
`RESET TIME ZONE` and `RESET ALL` restore UTC. The server supports only
`READ COMMITTED`, UTF8 client encoding, and `standard_conforming_strings = on`.
`SET LOCAL`, unsupported setting names, and unsupported values are rejected.
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
./t/swank.sh              # Swank preload, bind options, invalid/occupied ports
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

For live debugging, server-backed tests accept `DEBUG_ON_FAILURE=1`. The test
starts Swank on `SQL port + 1000`; if an assertion fails while the server is
running, it prints the SQL and Swank ports, PGDATA, and log path, then waits for
Enter before cleaning up. Set `DEBUG_SWANK_PORT` to choose a different Swank
port, and run the test in a terminal or tmux pane to attach with
`M-x slime-connect`. If the server has already exited, the test reports that
there is no live listener and cleans up without pausing. The dedicated
`t/swank.sh` listener test controls its own listener states.

## Compilation and formatting

`./t/compile.sh` force-compiles the `pg` ASDF system and fails on compiler
warnings. It uses `sbcl` from `PATH` by default; set `SBCL=/path/to/sbcl` to
override.

The batch formatter requires GNU Emacs and SLIME. Follow the
[MELPA setup guide](https://melpa.org/#/getting-started), then install SLIME
using its [installation instructions](https://slime.common-lisp.dev/doc/html/Installation.html).
For a minimal Emacs setup, add MELPA to the package archives and install SLIME
once:

```elisp
(require 'package)
(add-to-list 'package-archives '("melpa" . "https://melpa.org/packages/") t)
(package-initialize)
```

Then run `M-x package-refresh-contents` and `M-x package-install RET slime RET`.
Format selected files or check them without writing:

```sh
./t/format.sh backend/executor/exec_simple.lisp
./t/format.sh --check backend/executor/exec_simple.lisp
```

The formatter starts a separate SLIME inferior process, loads `pg` so SLIME can
learn the system's macro indentation, and applies Emacs Lisp-mode indentation.
It only adjusts indentation; it does not reflow long lines. The inferior Lisp
defaults to `SBCL` if set, or `sbcl` from `PATH`; select another executable with
`--lisp /path/to/sbcl` or `PG_LISP_INFERIOR=/path/to/sbcl`.

Format project Common Lisp sources, including `pg.asd`, from the repository
root. The pinned Swank sources keep their upstream formatting:

```sh
git ls-files -z -- 'src/lisp/*.lisp' 'src/lisp/**/*.lisp' \
  'src/lisp/*.asd' 'src/lisp/**/*.asd' \
  ':(exclude)src/lisp/vendor/swank/**' |
  xargs -0 src/lisp/t/format.sh
```

The tracked pre-commit hook checks staged project `.lisp` and `.asd` files; it
skips the pinned Swank source to preserve upstream formatting. Enable it in
this clone from the repository root with:

```sh
git config core.hooksPath .githooks
```

The hook requires the same Emacs, SLIME, and inferior Lisp setup as the batch
formatter. It reports files that need formatting; run the formatter and stage
those files before committing.

## SLIME debugging

The launcher loads the vendored Swank server from SLIME v2.32 into the Lisp
image on every start, without opening a network port. Enable the listener
explicitly when you want to debug the running server:

```sh
./pg-lisp.sh -p 5439 -D /tmp/pgdata --swank-port 4005
```

Connect from Emacs with the matching SLIME v2.32 frontend using
`M-x slime-connect`, host `127.0.0.1`, and port `4005`. By default the listener
binds to `127.0.0.1`, even when the SQL server is exposed on another address
with `-h`. For remote debugging, keep that loopback binding and use an SSH local
forward, for example `ssh -L 4005:127.0.0.1:4005 db-host`; connect Emacs to
`127.0.0.1:4005`.

To bind another interface directly, pass `--swank-bind-address ADDR` with
`--swank-port` (use `0.0.0.0` to listen on all IPv4 interfaces). Direct Swank
connections can evaluate Lisp in the server process and use unencrypted TCP, so
expose them only on a trusted, access-controlled network. See the [SLIME manual
on setting up the Lisp image](https://slime.common-lisp.dev/doc/html/Setting-up-the-lisp-image.html)
for its SSH tunnelling guidance. The pinned server source and commit are
recorded in [`vendor/swank/README.md`](vendor/swank/README.md).

## Storage and concurrency

- 8 KB pages with PostgreSQL's `PageHeaderData` + `ItemIdData` layout, one file
  per relation under `$PGDATA/base/<relfilenode>`.
- Heap tuples use a versioned 23-byte `HeapTupleHeaderData` layout. The `xmin`,
  `xmax`, `cid`, `ctid`, attribute-count, infomask, and header-offset fields
  follow PostgreSQL's offsets; tuple XIDs are 32-bit `TransactionId`s. A null
  bitmap and attribute data follow the header. Attribute lookup is a single
  left-to-right walk (no offset table) — see `afm` notes in the code.
- Real MVCC: the allocator keeps 64-bit full XIDs and stores their low 32 bits
  in tuple headers and the CLog (`pg_xact`, with 2 status bits per XID). Before
  reusing a 32-bit XID cycle, the server blocks new statements, waits for active
  transactions and snapshots to drain, freezes old tuple XIDs, and resets the
  CLog. A per-statement READ COMMITTED snapshot tracks full XIDs and `curcid`.
  XIDs are handed out in
  batches of 1000, with `global/pg_control` atomically replaced and its
  directory fsynced before any XID in the batch is used.
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
- UPDATE and DELETE follow a committed update's `ctid` chain after waiting for
  its writer, refresh the row snapshot, and recheck the WHERE condition.
- Writers take a single global write mutex; DDL also takes a catalog mutex;
  page I/O is serialized under an smgr mutex. Readers do not block.

## Known gaps (documented, not hidden)

- No WAL and no crash recovery. A crash between the data fsync and the CLog
  commit loses that transaction; it is treated as aborted on restart. A
  returned `COMMIT` is therefore not power-fail safe.
- Heap page layout version 6 stores 32-bit tuple XIDs in the PostgreSQL header
  layout. Existing version 4 and 5 relation files are rejected and must be
  recreated before use.
- XID wrap handling uses a global stop-the-world freeze at each 32-bit cycle
  boundary. PostgreSQL instead freezes tuples incrementally during vacuum and
  protects XID age with wraparound limits; this server has no VACUUM yet. XID
  allocation stops after `2^64 - 1`.
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
- No VACUUM or heap tuple pruning; old row versions and their TIDs are retained
  so concurrent update-chain rechecks cannot follow a reused TID. Updates and
  deletes do not reclaim heap space, and inserts always allocate new heap TIDs.
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

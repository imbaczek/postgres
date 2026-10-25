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
`--temp-file-limit-bytes BYTES` caps the live spill files used by one query;
`-1` (the default) means unlimited. `PG_QUERY_TEMP_FILE_LIMIT_BYTES` sets the
same value when the command-line option is omitted. Spill files live in private
`PGDATA/pgsql_tmp` namespaces keyed by process identity and session/query; startup
removes namespaces it can verify are abandoned and preserves unknown owners.
`--sort-arena-limit-bytes BYTES` enables an experimental SBCL arena for each
`ORDER BY` sort run. The default `-1` disables it; enabled capacities must be at
least 65536 bytes. `PG_QUERY_SORT_ARENA_LIMIT_BYTES` supplies the value when the
option is omitted. This requires an SBCL build with recoverable arena exhaustion
and its default arena extension predicate; unsupported builds reject the option
at startup. Capacity exhaustion reports SQLSTATE `53200` and follows ordinary
statement/transaction error handling.
`--swank-port PORT` optionally opens the Swank listener on `127.0.0.1`; the
Swank code is loaded in the image by default, but the listener stays off unless
this option is supplied. `--swank-bind-address ADDR` selects another interface
when `--swank-port` is also set; its default is `127.0.0.1`, and the address
option by itself never starts a listener.

Authentication and authorization are accepted and ignored: any user/database is
trusted, and `postgres` is always the effective role. Only the simple query
protocol (`Q`) is implemented, which is all `psql` uses for the supported
statements.

## Write-ahead logging

The server keeps append-only WAL segments under `PGDATA/pg_wal`. Each transaction
flushes full-page after-images and a commit record before applying modified heap
and B-tree pages. Recovery replays only complete batches after the saved
checkpoint. Page checksums include the relation and block number. A short final
header, invalid-length record, or incomplete record can be truncated only when
it starts beyond the durable LSN in the control file. Damage at or before that
boundary stops startup. The WAL bytes are synced before the control file
advances the durable LSN. Run `CHECKPOINT` to flush staged pages and catalog/CLOG
state before advancing the recovery pointer. This is a small physical WAL
implementation for the server's supported operations.

## Hot reload and control REPL

Send `SIGHUP` to a running server, or call `pg_reload_code()`, to recompile and
reload the `pg` system in place:

```sh
kill -HUP "$(pgrep -f 'sbcl.*boot.lisp')"
```

Functions are redefined live, so existing connections and global state (xact
counters, catalog, relation files) survive. Two caveats: a change to a
`DEFSTRUCT` layout needs a real restart, and a reload whose compilation fails
partway can leave a mixed image, so restart if the reload log shows an error.
`t/hot-reload.sh` exercises the SIGHUP path.

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

The SQL admin functions follow `signalfuncs.c`; `pg_reload_code()` is a local
helper for this Lisp server:

```sql
SELECT pg_backend_pid();
SELECT pg_cancel_backend(pid);                 -- ERROR 57014 in the target
SELECT pg_terminate_backend(pid [, timeout]);  -- FATAL 57P01, connection ends
SELECT pg_reload_conf();                       -- config reload (currently a no-op)
SELECT pg_reload_code();                       -- recompile and reload the Lisp system
```

`pg_cancel_backend` cancels the running statement at a safe point; a cancel
aimed at an idle backend is ignored, as in PostgreSQL. `pg_terminate_backend`
delivers the FATAL even to an idle backend and returns true once the signal is
sent. With a positive millisecond `timeout` it waits for the backend to
disappear and returns false if it does not; a negative timeout raises `22003`.
An unknown PID produces a `WARNING` and returns false. `t/shutdown.sh`
exercises backend signaling and both reload functions through SQL and the raw
TCP CancelRequest packet, including rejection of invalid PID/key pairs, plus
smart/fast/immediate shutdown and escalation.

## Supported SQL

Types: `int`/`integer`/`int4`, `bigint`/`int8`, `varchar(n)` / `character
varying(n)` with `n <= 126`, and `text`. For now, `text` is an alias for
`varchar(126)`; distinct text/varchar type semantics and OIDs are a TODO.
`varchar` without a length is also treated as 126.

DDL:

```sql
CREATE TABLE t (a int, b varchar(20), PRIMARY KEY (a));
CREATE TABLE t (a int UNIQUE, b varchar(20));
CREATE TABLE t (id bigint DEFAULT nextval('t_id_seq'));
DROP TABLE [IF EXISTS] t;
TRUNCATE TABLE t [, ...];
CREATE UNIQUE INDEX name ON t (cols);
CREATE UNIQUE INDEX name ON t (cols) NULLS NOT DISTINCT;
CREATE INDEX name ON t (cols);
DROP INDEX [IF EXISTS] name;
ALTER INDEX name RENAME TO new_name;
CREATE [IF NOT EXISTS] SEQUENCE name [START [WITH] n] [INCREMENT [BY] n]
  [MINVALUE n] [MAXVALUE n];
ALTER SEQUENCE name INCREMENT [BY] n | RESTART [WITH] n;
DROP SEQUENCE [IF EXISTS] name [, ...];
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
SELECT expr [AS alias], ... | * FROM t [AS alias]
  [{[INNER] | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]} JOIN t [AS alias]
    {ON e | USING (cols)} | CROSS JOIN t [AS alias]] ...
  [WHERE e] [ORDER BY col [ASC|DESC] [, ...]] [LIMIT n];
EXPLAIN SELECT ...;                     -- reports scans or a Nested Loop join
INSERT INTO t [(cols)] VALUES (...)[, (...)];
UPDATE t SET col = expr [, ...] [WHERE e];
DELETE FROM t [WHERE e];
```

Sequence expressions support `nextval('sequence')`, `currval('sequence')`, and
`setval('sequence', value [, is_called])`. The only supported column default is
`DEFAULT nextval('sequence')`. Sequence values use the implemented signed `int8`
range and are persisted separately from transaction rollback, as in PostgreSQL.

Joins use a nested-loop executor over sequential scans. `ON` predicates can
reference both join operands; `USING` merges columns with a common type (equal
types and `int4`/`int8` pairs), with FULL JOIN coalescing the merged key.
Comma-separated FROM items and NATURAL JOIN are not supported.

Transactions (READ COMMITTED only):

```sql
BEGIN | START TRANSACTION;
COMMIT | END;
ROLLBACK | ABORT;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
CHECKPOINT;
```

Expressions: column references, integer and string literals, `+ - * / %`,
comparisons, `AND` / `OR` / `NOT`, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE`, and
parentheses. Scalar functions: `version()`, `pg_lisp_runtime()`,
`current_database()`, `current_schema()`, `current_catalog()`, `current_user`,
`session_user`, `user`, `length()`, `pg_backend_pid()`,
`pg_cancel_backend(pid)`,
`pg_terminate_backend(pid [, timeout])`, `pg_reload_conf()`,
`pg_reload_code()`.

`SET TIME ZONE` accepts installed named TZif zones, stores the value per backend,
and returns it from `SHOW timezone`; numeric offsets and `INTERVAL` forms are
unsupported. `RESET TIME ZONE` and `RESET ALL` restore UTC. The server supports
only `READ COMMITTED`, UTF8 client encoding, and `standard_conforming_strings = on`.
`SET LOCAL`, unsupported setting names, and unsupported values are rejected.
Meta-commands that need `pg_catalog` and regular-expression operators (`\d`,
`\dt`, `\l`) return an error from the server; `psql` prints it and continues.

## Logical replication

`./pg-lisp-subscribe` defaults to extension mode between two `pg-lisp` servers. It creates a
logical slot with an exported snapshot, initializes supported table/index/sequence
definitions on the target, copies rows from an explicit `--table` allowlist, then
applies committed row transactions and supported DDL/sequence events. Each target
stores a durable source-LSN marker in the same catalog commit as an applied event;
the worker sends replication feedback only after that commit. The worker explicitly
requests the `pg_lisp` output plugin for its snapshot lifecycle and private events.
`t/logical-replication.sh`
exercises initial copy, DDL and sequence changes, two independent replicas, and
restart after a lost feedback message.

Extension table definitions and supported DDL cover the SQL subset listed
above. Initial bootstrap mirrors supported schema objects in `public`; `--table`
selects which tables have rows copied and whose row changes are applied. DDL and
sequence events are database-wide and are applied independently of that table
allowlist. Publication row filters, cascading replication origins,
cascading-loop prevention, arbitrary DDL, and unsupported types or storage
features are not implemented. The wire stream uses pgoutput row messages plus
the v2 `Message` form for the worker's DDL and sequence events; those custom
events require the bundled subscriber. A single catalog event must fit within
one WAL segment.

Standard mode interoperates with the unmodified C `20devel` reference in both
directions: pg-lisp publishes through `pgoutput` to native `CREATE SUBSCRIPTION`,
and the bundled worker subscribes to a C publication:

```sh
./pg-lisp-subscribe --mode standard --publication interop_pub --slot receiver_key \
  --source-port 5432 --target-port 5433 --table interop_rows --table interop_other
```

Precreate matching ordinary `public` tables with non-null primary keys. The
supported types are `int4`, `int8`, and bounded `varchar`; supported operations
are INSERT, UPDATE including key changes, DELETE, and plain transactional
TRUNCATE. Initial copy and concurrent writes use a consistent snapshot/WAL
handoff. Target effects and source progress commit together before feedback.
`--slot` identifies the target subscription; standard mode records a random
owned source slot and durable creating/copying/ready state for crash recovery.
Ready subscriptions resume without resetting target data.

Standard mode uses explicit publication table lists and text tuples with
streaming, binary, two-phase, and failover options disabled. DDL/sequence
automation remains in extension mode. Row filters, column lists, partitions,
generated columns, TOAST, and replica identity FULL remain outside the subset.
See [C-peer acceptance and limits](REPLICATION-COMPATIBILITY.md) for the pinned
reference, test matrix, recovery evidence, and exact options.

## Testing

The `pg-interop-tests` ASDF system depends on Rove and `cl-spec/check-it` for
new executable contracts and property checks. These are test dependencies;
the `pg` server system does not load them. Install
[cl-spec](https://github.com/masatoi/cl-spec) in your Quicklisp `local-projects/`
directory (for Roswell, under `~/.roswell/lisp/quicklisp/`). The verified revision
is `147e3de5d0ab636cf8cc4c83253e03e5a7d7e639`. Quicklisp supplies Rove, check-it,
and their dependencies. From `src/lisp/` in a Quicklisp-enabled REPL:

```lisp
(ql:register-local-projects)
(asdf:load-asd (truename "pg.asd"))
(asdf:load-asd (truename "pg-interop-tests.asd"))
(ql:quickload "pg-interop-tests")
(asdf:test-system "pg-interop-tests")
```

See the [interop test inventory](t/interop/README.md) for refactor decisions,
generated-check domains, replay seeds and verification limits.

```sh
cd src/lisp
./t/run-unit-tests.sh     # page/tuple/index/visibility and VACUUM horizon tests
./t/psql-smoke.sh         # boots a server and diffs psql output against a golden file
./t/wal-crash-recovery.sh  # SIGKILL recovery at commit, page, update, split, and checkpoint boundaries
./t/unique-index-concurrency.sh # unique-index build/drop writer races
./t/update-concurrency.sh # concurrent UPDATE chain and WHERE rechecks
./t/vacuum-update-concurrency.sh # ordinary VACUUM with same-page concurrent writers
./t/index-scan-regression.sh # indexed/SeqScan differential and DROP INDEX handoff
./t/index-lock-contention.sh # unrelated INSERT progress during duplicate-key scans
./t/index-build-contention.sh # detached build, relation gate, publication name race
./t/join-streaming.sh    # outer-join results and unordered LIMIT early stop
./t/join-ddl-concurrency.sh # atomic JOIN admission against concurrent DROP TABLE
./t/spill-crash-recovery.sh # crash cleanup, restart recovery, and temp-file limit
./t/sort-arena.sh        # bounded sort arenas, transaction recovery, concurrent sorts
./t/logical-replication.sh # exported snapshot, row/DDL/sequence stream, restart recovery
./t/logical-private-metadata-ddl.sh # rejects replicated DDL targeting subscriber metadata
./t/index-stress.sh       # repeated updates to one unique value (slow; UPDATES=n to tune)
./t/hot-reload.sh         # SIGHUP reloads code in a running server
./t/control-repl.sh       # control REPL over TCP + Unix socket, reload, SQL intact
./t/swank.sh              # Swank preload, bind options, invalid/occupied ports
./t/shutdown.sh           # signals, shutdown modes, cancel/terminate admin functions
```

`t/psql-smoke.sh` is the acceptance test: it launches the server on a throwaway
PGDATA, runs `t/psql-smoke.sql` through a real `psql`, and compares the
transcript with `t/psql-smoke.out`.

`t/wal-crash-recovery.sh` reuses one PGDATA across forced exits. It checks commit
redo, incomplete batch and UPDATE visibility, unique-index recovery after a
B-tree split, incomplete WAL tail truncation, rejection of malformed and
checksummed record corruption, and crashes around checkpoints.

`t/index-stress.sh` updates a single unique value a thousand times, then checks
that a fresh transaction still sees the live tuple as a duplicate.  It exercises
the case where equal index keys straddle many leaf splits.

`t/unique-index-concurrency.sh` checks that unique-index builds and `DROP INDEX`
wait for active writers, and that `DROP TABLE` serializes with builds, readers,
and writers.

`t/update-concurrency.sh` holds one transaction open while another session
updates the same row. It confirms the waiter entered a transaction-ID wait,
then checks that the waiter follows the committed `ctid` chain, recalculates
`value + 1` against the latest version, and skips both UPDATE and DELETE when
the refreshed row no longer satisfies `WHERE`.

`t/vacuum-update-concurrency.sh` pauses ordinary VACUUM before publishing a
heap-page proposal while two writer sessions repeatedly update rows on that
page. It verifies VACUUM retries after the page changes, then compares indexed
results for old and current keys with SeqScan after VACUUM completes.

`t/index-scan-regression.sh` builds a non-unique index on populated data, applies
deterministic randomized updates and deletes with duplicate keys and NULLs,
runs VACUUM, and compares indexed results with SeqScan for equality, NULL, and
residual-filter queries. It also pauses an explicit-transaction SELECT before
atomic heap/index reader admission while DROP INDEX installs relation gates,
then checks for partial registrations and verifies the SELECT revalidates to
SeqScan after the index is dropped.

`t/index-lock-contention.sh` pauses equality searches over many duplicate keys
while unrelated INSERTs run. Each INSERT must commit before the search is
released; the test also reports search duration and INSERT completion time.

`t/index-build-contention.sh` pauses a populated non-unique index build. It checks
that the unfinished index is absent from the catalog, a writer on another table
commits, and a writer on the indexed table waits for the relation gate. It then
races two builds for one index name, checks failures before and after durable
catalog publication, and crashes during a detached build. Startup must discard
both unpublished index files and retain the durably published index.

`t/join-streaming.sh` checks INNER/LEFT/RIGHT/FULL results, WHERE evaluation above
the join, and that an unordered `LIMIT 1` evaluates only its first joined pair.
`t/join-ddl-concurrency.sh` coordinates JOINs that each have one admissible heap
and one gated heap with two `DROP TABLE` sessions. It confirms reader admission is
all-or-none, then checks the stale plans fail safely after the tables are dropped.

`t/spill-crash-recovery.sh` forces an external sort spill, SIGKILLs the server
after its first record reaches disk, then restarts on the same PGDATA and checks
that startup removes the abandoned query namespace while committed rows remain.
It also verifies that the configured temporary-file limit rejects a later spill,
cleans partial files, and leaves the server able to serve another query.

`t/sort-arena.sh` requires arena support and provokes actual arena exhaustion,
then checks SQLSTATE `53200`, an aborted transaction (`25P02`), rollback, and
continued use of the same connection. It also runs concurrent external sorts
whose many runs reuse their own arenas. Unit tests cover owned VARCHAR/BYTEA
output after arena release and full GC, scalar GC roots, cancellation during
the allocation-region switch, and cleanup after errors and exhaustion.

Sort arenas currently cover only owned copies of completed `ORDER BY` run
buffers. Input scanning, expression evaluation, spill/merge I/O, output
callbacks, top-N sorts, joins, and aggregation use the ordinary GC heap. A run
is staged under the existing approximate work-memory budget before a single
batch copy enters its arena; both copies briefly coexist. The arena is reset
after each spilled run, and in-memory output is copied back onto the heap before
delivery. Arena storage includes allocator overhead and is capped independently
of the spill threshold. PostgreSQL C's `tuplesort_begin_batch` likewise uses a
dedicated tuple memory context; this prototype imposes an additional capacity
error rather than replacing `work_mem` spilling. It does not provide independent
backend heaps, a global process memory limit, or recovery from a kernel OOM kill.

`t/spill-oom-cgroup.sh` repeats spill recovery after a real cgroup OOM kill.
On a systemd host with cgroup v2 memory delegation, run it in a private scope
(from `src/lisp`):

```sh
systemd-run --user --scope -p Delegate=memory ./t/spill-oom-cgroup.sh
```

From tmux or a noninteractive agent harness, use a transient service to give
the test its own delegated cgroup, stream its output, and wait for its exit:

```sh
cd src/lisp  # from the repository root
systemd-run --user --wait --pipe --collect -p Delegate=memory \
  --working-directory="$PWD" --setenv=SBCL="${SBCL:-$(command -v sbcl)}" \
  "$PWD/t/spill-oom-cgroup.sh"
```

This needs a running systemd user manager and access to its bus. If the harness
lacks the session environment, run `export XDG_RUNTIME_DIR=/run/user/$(id -u)`
then `export DBUS_SESSION_BUS_ADDRESS=unix:path=$XDG_RUNTIME_DIR/bus` (the bus
must already exist). Use an absolute path for a custom `SBCL`; pass other
overrides with `--setenv=NAME=value`. Look for `spill-oom-cgroup: PASS`: `SKIP` also exits zero
and means the OOM checks did not run.

The harness moves itself into a supervisor child, enables memory control on
the empty scope, and places the server in a sibling with a 768 MiB limit,
swap disabled, and whole-group OOM killing enabled. It verifies OOM counters,
spill cleanup, and committed rows after restart. It skips when delegation,
memory controls, or memory headroom are unavailable; a shared scope such as
tmux must be replaced with the private scope above. `MEMORY_MAX` and
`MEMORY_RESERVE` override the limit and required spare memory in bytes
(defaults: 805306368 and 268435456). The host and each visible limited ancestor
must have at least their sum available.

The Lisp-level `test-vacuum-snapshot-horizon` ports the permanent-table case
from PostgreSQL's `src/test/isolation/specs/horizons.spec`: an older registered
snapshot keeps a deleted row and its index TID until VACUUM can safely advance
its horizon.

`t/shutdown.sh` also confirms that a raw TCP CancelRequest interrupts a blocked
statement after the backend enters its transaction-ID wait, and that incorrect
PID or secret-key pairs do not cancel it. The test uses the local control REPL
on SQL port + 2000 as a synchronization point.

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
- Transaction commit is durable at the WAL flush: a commit batch writes any
  currently staged page after-images (potentially including changes from other
  active transactions) alongside the committing XID's commit record, then fsyncs
  WAL before CLog marks that XID committed and applies the page images to
  heap/index files. Heap and index files are fsynced at CHECKPOINT; the
  checkpoint pointer advances only after pages, CLog, and the relevant
  directories are synced.
- The catalog (`pg_class`/`pg_attribute`/`pg_index` as structs) is serialized to
  `$PGDATA/global/pg_catalog.dat` and fsynced at each DDL commit. It is replaced
  atomically (write a temp file, `rename`, then fsync the directory), so a crash
  never leaves the catalog missing.
- Executor spill files live below `$PGDATA/pgsql_tmp` in per-instance and
  per-session query namespaces. Normal query cleanup removes them, startup
  removes namespaces left by a crashed server, and `--temp-file-limit-bytes`
  can cap the live spill bytes for a query.
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
- Heap insertion consults a volatile 256-category free-space map per
  relfilenode. It is rebuilt from heap pages once on the first insert after a
  restart; successful inserts refresh the page's free-space entry.
- Heap/catalog writes still use a global write mutex, and page I/O is serialized
  under an smgr mutex. B-tree reads and mutations use relation-scoped leases;
  relation DDL waits for registered readers. A query acquires all selected index
  read leases in relfilenode order before execution and holds them through row
  callbacks so nested scans cannot deadlock with multi-index inserts. A slow
  scan or output consumer can therefore delay writes and VACUUM on each selected
  index. Populated index builds use detached files and publish under relation
  gates.

## Known gaps (documented, not hidden)

- No WAL archiving, complete PostgreSQL replication protocol, or point-in-time
  recovery. The `pg-lisp` logical subscriber supports only the documented SQL
  subset, source-side supported DDL and sequence state, and an explicit table
  allowlist; it does not implement publication selection or filters, replication
  origins, or conflict handling. The WAL layer supports persistent replication
  slots with restart/consistent LSNs, confirmed-flush acknowledgments, and
  `pg_catalog.pg_replication_slots` diagnostics. Slots that exceed the configured
  per-slot WAL-retention cap are durably marked lost at checkpoint so recycling
  can proceed. A complete WAL record with a bad checksum stops startup; this
  implementation detects that damage but does not reconstruct the record.
- Heap page layout version 6 stores 32-bit tuple XIDs in the PostgreSQL header
  layout. Existing version 4 and 5 relation files are rejected and must be
  recreated before use.
- XID wrap handling still uses a global stop-the-world freeze at each 32-bit
  cycle boundary. VACUUM now freezes eligible tuples incrementally, but
  PostgreSQL's wraparound limits are not implemented. XID allocation stops
  after `2^64 - 1`.
- DDL and `ANALYZE` inside an explicit transaction block are rejected (`0A000`)
  because catalog changes and statistics have no rollback log.
- `COUNT(*)` and `GROUP BY` are supported. Hash aggregation is bounded by query
  work memory and spills through recursive hash partitions, with external sort
  as the skew/depth fallback. Subqueries, CTEs, `DISTINCT`, and `HAVING` are not
  implemented. Joins use nested loops; INNER/CROSS join order uses a greedy
  statistics-based choice, while outer joins retain source order. NATURAL JOIN
  and comma-separated FROM items are not supported.
- No `OFFSET`/`FETCH`. `ORDER BY` accepts multiple keys with `ASC`/`DESC` and
  uses bounded in-memory runs with external merge when needed.
- No general `DEFAULT` expressions (only `DEFAULT nextval('sequence')`), `CHECK`,
  foreign keys, or `NOT NULL`/`UNIQUE` beyond what is listed above.
- `ANALYZE table` scans visible heap rows to count them exactly and keeps a
  transient reservoir capped at 300 rows and about 4 MiB. It persists row count,
  NULL fraction, distinct-count estimate, and up to ten most-common values per
  column; raw sample rows are not stored. These estimates feed a small cost
  model for SeqScan, single-column equality IndexScan, parameterized INNER/LEFT
  nested-loop inputs, and greedy
  INNER/CROSS join order. Statistics are explicit and can become stale between
  ANALYZE commands. Index scans stream duplicate-key TIDs, enforce MVCC
  visibility, and recheck the full predicate. Composite equality, range scans,
  bitmap scans, and index-only scans are not implemented.
- VACUUM is manual only; autovacuum and index-only scans are not implemented.
  Manual VACUUM removes dead index TIDs before making safe dead heap slots
  reusable, freezes eligible tuples, and updates the visibility map and volatile
  free-space map. Its first FSM rebuild after restart scans the existing heap
  once.
- Byte-wise comparison only, i.e. C-like collation for ASCII. Index key
  encoding is length-first and is safe for equality lookups; text range ordering
  and ordered index iteration are not implemented.
- Numeric/float, date/time, boolean, and other types are unsupported. `text` is
  temporarily treated as `varchar(126)`; full text/varchar semantics remain a
  TODO.
- Extended query protocol (Parse/Bind/Describe/Execute/Sync) is not
  implemented; `psql` is unaffected, other drivers are.
- `pg_terminate_backend` (and fast/immediate shutdown) unwinds the target with
  an SBCL thread interrupt at an arbitrary point instead of PostgreSQL's
  deferred safe-point check. An interrupt during WAL I/O forces server shutdown;
  this remains a divergence from PostgreSQL's signal handling.
- `\d`, `\dt`, `\l` fail because the catalog SQL uses constructs beyond this
  subset (regex operators, `pg_catalog` functions); `psql` prints an error and
  shows no listing.

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

Native WAL checkpoints (payload introduced in version 6) preserve source counters, checkpoint time
and explicit start/end/redo positions in the record and the CRC-checked WAL
control copy. The copy advances only after data/CLOG sync. Stored legacy WAL and
controls remain readable with unknown checkpoint facts until a new checkpoint.
Native streaming uses the experimental `pg_lisp_wal_7` format. End-of-recovery
syntax and read-only timeline checks use that same format; operational promotion
remains unfinished.
This does not provide C WAL or control-file disk compatibility.
`./t/checkpoint-snapshot.sh` covers legacy upgrade, both publication crash
boundaries, control-copy mismatch rejection and standby restartpoint preservation.
Version 7 additionally logs native XID reservations before allocation. Standby
restartpoint v3 preserves their source upper bounds separately from reader
visibility. Checkpoint v2, recovery control v3, bootstrap manifest v3 and
restartpoint v3 preserve exhausted FullXIDs exactly. Older restartpoint generations
require the full retained prefix to rebuild actual visibility; compacted legacy
images require reseeding. The regression `./t/xid-reservation.sh` checks both
control-file crash windows, reader-only allocation, checkpoint-only actual counter
progress, and crashes during migration of real v1/v2 restartpoints. This adds no
promotion authority, coordination or synchronous acknowledged-commit guarantee.

WAL control v6 records filename namespace separately from insertion/checkpoint
timelines. Fresh and legacy clusters retain 16-digit native names until an explicit
primary control call `(wal-migrate-timeline-namespace)` selects 24-digit
timeline-qualified native names. Startup reconciles interrupted migration before
writes or recycling. `./t/wal-namespace.sh` covers multi-segment migration/startup
crashes, continued follow and qualified replica bootstrap. Runtime timeline 1 and
native WAL7 framing remain in use; this is not promotion or C disk compatibility.

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

Table columns support `smallint`/`int2`, `int`/`integer`/`int4`, `bigint`/`int8`,
`numeric`/`decimal` (including precision and scale), `real`/`float4`,
`double precision`/`float8`, `date`, `time` and `timestamp` with or without time
zone, `interval`, and `varchar(n)` / `character varying(n)` with `n <= 126`.
For now, `text` is an alias for `varchar(126)`; distinct text/varchar type
semantics and OIDs are a TODO.
`varchar` without a length is also treated as 126.

Decimal literals and numeric casts support finite values; numeric casts also
accept NaN and infinities. Precision-constrained numeric values are rounded and range checked. Floating
types use IEEE values, including NaN and infinities. Date/time and interval
types support casts, comparisons, and the implemented arithmetic operators;
time and timestamp precision is at most six fractional digits. Interval field
and precision modifiers are supported. These are subsets of PostgreSQL's full
type and operator catalog, not a promise that every input spelling or function
is implemented.

Basic numeric functions include `abs`, `numeric_abs`, `sign`, `round`,
`trunc`, `ceil`/`ceiling`, `floor`, `sqrt`, `numeric_sqrt`, `exp`,
`numeric_exp`, `ln`, `numeric_ln`, `log`, `log10`, and `numeric_log`.
`round` and `trunc` accept an optional
integer scale for numeric values; negative scales act before the decimal point.
Numeric rounding breaks ties away from zero, while floating-point rounding uses
ties to even. Square root rejects negative inputs; `numeric_sqrt` uses numeric
arithmetic, while `sqrt` also supports floating-point inputs. `numeric_exp`
uses numeric arithmetic and `exp` also supports floating-point inputs. For both
exponential functions, NaN and positive infinity pass through and negative
infinity returns zero. Numeric exponential reports overflow for large positive
inputs and returns scaled zero for large negative inputs; floating exponential
reports overflow or underflow when a finite input yields infinity or zero.
`ln` and `numeric_ln` compute natural logarithms; one-argument `log` and
`log10` use base ten, and two-argument `log(base, value)` and
`numeric_log(base, value)` use the supplied numeric base. `ln`, one-argument
`log`, and `log10`
also accept floating-point inputs; the explicitly numeric forms use numeric
arithmetic. Logarithms reject zero and negative arguments, and a base of one,
with SQLSTATE `2201E`. These functions return NULL when an argument is NULL.

Exponentiation is available as `base ^ exponent`, `power(base, exponent)`,
`pow(base, exponent)`, `numeric_power(base, exponent)`, and `dpow(base, exponent)`.
`^` associates left to right and binds more tightly than multiplication and
less tightly than unary signs: `2 ^ 3 ^ 2` is `(2 ^ 3) ^ 2`, and `-2 ^ 2`
is `(-2) ^ 2`. Parentheses can change this grouping. `^`, `power`, and `pow`
return numeric when an argument is numeric and neither argument is floating
point; otherwise they return float8. Thus two integer arguments use float8.
`numeric_power` selects numeric and `dpow` selects float8, subject to implicit
argument coercions. A NULL argument yields NULL. Zero raised to a negative
power and a negative base raised to a non-integer power raise SQLSTATE `2201F`.
NaNs and infinities follow PostgreSQL's power rules, including `NaN ^ 0 = 1`
and `1 ^ NaN = 1`. Numeric powers use decimal result scales; finite float8
overflow and underflow raise SQLSTATE `22003`.

Constant target expressions are evaluated during execution rather than folded
during planning. Consequently, `WHERE false` or `LIMIT 0` can suppress errors
from constant SELECT targets that PostgreSQL would raise while planning.

DDL:

```sql
CREATE TABLE t (a int, b varchar(20), PRIMARY KEY (a));
CREATE TABLE t (a int UNIQUE, b varchar(20));
CREATE TABLE t (id bigint DEFAULT nextval('t_id_seq'));
CREATE TABLE t (a int DEFAULT 3, CONSTRAINT positive CHECK (a > 0));
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
ALTER TABLE t ALTER COLUMN a SET DEFAULT expression;
ALTER TABLE t ALTER COLUMN a DROP DEFAULT;
ALTER TABLE t ADD CONSTRAINT positive CHECK (a > 0) [NOT VALID];
ALTER TABLE t VALIDATE CONSTRAINT positive;
ALTER TABLE t RENAME CONSTRAINT old_name TO new_name;
ALTER TABLE t DROP CONSTRAINT name;     -- CHECK, UNIQUE and PRIMARY KEY constraints
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
INSERT INTO t [AS alias] [(cols)] {VALUES (...)[, (...)] | SELECT ... | DEFAULT VALUES}
  [ON CONFLICT [(cols) | ON CONSTRAINT name] DO NOTHING
   | ON CONFLICT {(cols) | ON CONSTRAINT name} DO UPDATE SET col = expr [, ...] [WHERE e]]
  [RETURNING expr, ... | *];
UPDATE t [AS alias] SET col = expr [, ...] [FROM source [, ...]]
  [WHERE e] [RETURNING expr, ... | *];
DELETE FROM t [AS alias] [USING source [, ...]] [WHERE e] [RETURNING expr, ... | *];
```

Expression subqueries support correlated and uncorrelated `EXISTS`, scalar
subqueries, `IN`/`NOT IN`, and comparisons with `ANY`/`SOME`/`ALL`.
`ARRAY(SELECT ...)` supports text elements. These forms use the existing
supported clauses and types, including their metadata and type checks. See
[the subquery design and tests](t/SUBQUERIES.md) for concurrency semantics and
known parity limits.

SELECT CTEs can prefix INSERT, UPDATE and DELETE. RETURNING supports scalar
expressions, subqueries, qualified stars, OLD/NEW fields and aliases declared
with `RETURNING WITH (OLD AS o, NEW AS n)`. Results are buffered in a spillable
store until the statement succeeds. See [DML scope and tests](t/DML.md) for
conflict arbitration, visibility and remaining limitations.

UPDATE FROM and DELETE USING accept the supported SELECT source forms,
including joins, derived queries, CTE references and LATERAL between sources.
WHERE, SET and RETURNING can reference source columns; bare RETURNING * includes
the target and visible source columns. See [joined DML](t/JOINED-DML.md) for
duplicate matches, concurrent rechecks and the target-reference boundary.

Nonrecursive `WITH` supports SELECT CTEs, sequential lexical bindings, nested
shadowing, column aliases, and default, `MATERIALIZED` and `NOT MATERIALIZED`
policies. Scalar SELECT subqueries with `WITH` also work in DML expressions.
Recursive SELECT CTEs support `UNION` and `UNION ALL`, including forward
dependencies and a single legal self-reference. Ordinary `UNION DISTINCT` is
also supported. See [nonrecursive CTEs](t/CTES.md) and [recursive CTEs](t/RECURSIVE-CTES.md)
for behavior and limits.

Window expressions support `row_number()`, `rank()`, `dense_rank()`, `count(*)`,
`count(expr)`, `min`, `max`, `sum`, `avg` and text `array_agg`, with multiple inline
or named windows and supported inheritance. The default is peer-aware
`RANGE UNBOUNDED PRECEDING` through
`CURRENT ROW`; explicit `ROWS` or `RANGE` supports that prefix or an unbounded
following end. See [window functions](t/WINDOWS.md) for boundaries and tests.

Sequence expressions support `nextval('sequence')`, `currval('sequence')`, and
`setval('sequence', value [, is_called])`. Supported scalar expressions can be
stored as column defaults and CHECK constraints. Sequence names in stored
expressions must be literal strings or NULL. Sequence values use the signed `int8`
range and are persisted separately from transaction rollback, as in PostgreSQL.
See [defaults, checks and aggregates](t/DEFAULTS-CHECKS-AGGREGATES.md) for scope,
validation and replication limits.

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

Expressions: column references, integer, decimal and string literals, `+ - * / % ^`,
comparisons, `AND` / `OR` / `NOT`, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE`, and
parentheses. Scalar functions: `version()`, `pg_lisp_runtime()`,
`current_database()`, `current_schema()`, `current_catalog()`, `current_user`,
`session_user`, `user`, `length()`, `pg_backend_pid()`,
`pg_cancel_backend(pid)`,
`pg_terminate_backend(pid [, timeout])`, `pg_reload_conf()`,
`pg_reload_code()`.

`SET TIME ZONE` accepts installed named TZif zones, stores the value per backend,
and returns it from `SHOW timezone`; numeric offsets and `INTERVAL` forms are
unsupported. `RESET TIME ZONE` and `RESET ALL` restore the startup baseline, UTC
when unspecified. `DateStyle` supports ISO, SQL, Postgres and German output and
MDY, DMY and YMD input order; `IntervalStyle` supports only `postgres`.
`extra_float_digits` controls floating-point output. Supported isolation levels
are `READ COMMITTED` and `REPEATABLE READ`; client encoding is UTF8 and
`standard_conforming_strings = on`.
Client startup and SQL `application_name` support SET, SHOW, RESET and
`current_setting`/`set_config`, including transaction-local values. Other LOCAL
settings remain unsupported. Parameter reports are coalesced before ReadyForQuery.
Unsupported setting names and values return errors.

Idle simple-query messages containing only supported ordinary settings and MVCC
statements commit or roll back together. DDL, maintenance and transaction-control
messages retain the prior per-statement behavior; see the staged boundary in
[the HA ledger](PHYSICAL-HA.md#application-name-and-supported-implicit-query-messages).
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
supported types are `int2`, `int4`, `int8`, `numeric`, `float4`, `float8`,
bounded `varchar`, `date`, `time`, `timetz`, `timestamp`, `timestamptz` and
`interval`. Supported operations are INSERT, UPDATE including key changes,
DELETE, and plain transactional
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

For a coordinator-free planned writer handoff experiment, see the
[manual logical switchover rehearsal](MANUAL-SWITCHOVER.md). Its script exercises
Lisp→Lisp, Lisp→C and C→Lisp with an owned writer, source shutdown as the fence,
durable target validation, sequence alignment and post-cutover writes.

## Testing

From `src/lisp`, `PG_C_BINDIR=/path/to/installed/postgres/bin ./t/subqueries.sh`
compares the native server with the repository's installed C PostgreSQL and owns
its peers and ports. It honors `SBCL` and `PYTHON` (default `python3`, standard
library only). See [subquery tests](t/SUBQUERIES.md) for scope and the known
concurrent semijoin boundary.

From `src/lisp`, `PG_C_BINDIR=/path/to/installed/postgres/bin ./t/ctes.sh`
compares CTE behavior with the repository's installed C PostgreSQL. See
[CTE tests](t/CTES.md) for scope and resource checks. Run
`PG_C_BINDIR=/path/to/installed/postgres/bin ./t/recursive-ctes.sh` for the
[recursive CTE differential and resource tests](t/RECURSIVE-CTES.md).

From `src/lisp`, `PG_C_BINDIR=/path/to/installed/postgres/bin ./t/windows.sh`
compares window cases with the repository's installed C PostgreSQL. The harness
honors `SBCL` and `PYTHON` (default `python3`, standard library only); see
[window tests](t/WINDOWS.md).

From `src/lisp`, `./t/child-bootstrap.sh` exercises a fresh child backup through
grandchild promotion. Set `CHILD_BOOTSTRAP_BACKUP_FAULTS=1`,
`CHILD_BOOTSTRAP_SOURCE_RESTART=1` and `CHILD_BOOTSTRAP_WRONG_MANIFEST=1` for
the combined acceptance cases. `CHILD_BOOTSTRAP_CRASH=1` exercises crash cases;
`CHILD_BOOTSTRAP_PROMOTION_CRASH=after-online-intent` or `after-child-control`
selects a promotion boundary. `CHILD_BOOTSTRAP_REMOVE_RETIRED=1` applies only
after child control. `CHILD_BOOTSTRAP_FORCE_TIMEOUT=1` is a private harness
regression that intentionally exits 137, not a normal feature test.

SQL promotion checks can be run from `src/lisp`:

```sh
./t/promotion-sql.sh
C_PG_BINDIR=... C_PG_REVISION=... LD_LIBRARY_PATH=... ./t/promotion-sql-c.sh
./t/promotion-sql-faults.sh
PROMOTION_SQL_CASE=timeout C_PG_BINDIR=... C_PG_REVISION=... ./t/promotion-sql-faults-c.sh
RESOURCE_SQL_PROMOTION=1 PG_C_BINDIR=... ./t/executor-parking.sh sort_output nested_indexes
```

The native fault script defaults to all nine cases. The C fault script accepts a
single case and uses a `recovery_end_command` barrier with matching C libraries;
it does not use SIGSTOP. The executor SQL mode adds test-only barriers around
promotion and callback resumption; the common SQL contract runs without them.
See [physical HA implementation and limits](PHYSICAL-HA.md) for validation scope.

The `pg-unit-tests`, `pg-interop-tests` and `pg-physical-tests` ASDF systems depend
on Rove and `cl-spec/check-it` for executable regressions and property checks.
These are test dependencies; the `pg` server system does not load them. Install
[cl-spec](https://github.com/masatoi/cl-spec) in your Quicklisp `local-projects/`
directory of the Quicklisp installation used by the test SBCL (for Roswell,
under `~/.roswell/lisp/quicklisp/`). The verified revision
is `147e3de5d0ab636cf8cc4c83253e03e5a7d7e639`. Quicklisp supplies Rove, check-it,
and their dependencies. From `src/lisp/` in a Quicklisp-enabled REPL:

```lisp
(ql:register-local-projects)
(asdf:load-asd (truename "pg.asd"))
(asdf:load-asd (truename "pg-unit-tests.asd"))
(ql:quickload "pg-unit-tests")
(asdf:test-system "pg-unit-tests")
(asdf:load-asd (truename "pg-interop-tests.asd"))
(ql:quickload "pg-interop-tests")
(asdf:test-system "pg-interop-tests")
(asdf:load-asd (truename "pg-physical-tests.asd"))
(ql:quickload "pg-physical-tests")
(asdf:test-system "pg-physical-tests")
```

The shell runner preserves SBCL's normal init configuration. Its
`t/test-dependencies.lisp` bootstrap first tries configured ASDF systems, then
uses the active Quicklisp installation. If Quicklisp is not loaded, it tries
`QUICKLISP_HOME/setup.lisp` or `~/quicklisp/setup.lisp`. It does not switch an
already loaded Quicklisp installation. A cl-spec checkout elsewhere can be
registered through standard ASDF configuration, for example:

```sh
CL_SOURCE_REGISTRY='(:source-registry (:directory "/path/to/cl-spec/") :inherit-configuration)' \
  ./t/run-unit-tests.sh
```

The unit suite contains 68 regressions migrated from the legacy runner, eight
Properties and one Function Spec, all explicitly executed through Rove. Loading
it alone does not run tests. `./t/run-unit-tests.sh` executes this ASDF suite and
then the 161 deferred legacy cases (arena cases remain conditional on runtime
support). See the [complete test inventory](t/TEST-INVENTORY.md) for every case,
shared helper, retained process harness, deferral reason and generated domain.

The physical suite checks native WAL intake, durable receive progress, source
retention, bootstrap, recovery write exclusion and streaming contracts. See
[physical HA implementation and limits](PHYSICAL-HA.md) for the task-owned native
server crash/restart scripts and remaining compatibility work. Streaming native
standbys support explicit local restartpoints through the private control REPL:
`(physical-recovery-restartpoint)` publishes a durable catalog/fork snapshot and
compacts the receive spool. A local worker also requests a restartpoint after
300 seconds or 1 GiB of applied WAL progress; received-but-unapplied WAL never
triggers one. SQL `CHECKPOINT` requests a standby restartpoint and succeeds as a
no-op on an unchanged static base. Smart/fast standby shutdown also publishes an
applied restartpoint and records clean recovery state in native control metadata.
The startup configuration subset below controls these restartpoint thresholds.

Recovery SQL supports `pg_wal_replay_pause()`, `pg_wal_replay_resume()`,
`pg_is_wal_replay_paused()` and `pg_get_wal_replay_pause_state()`. Pausing keeps
receipt and flush running while apply and visible data stay fixed. Native pause
waits for exclusive redo to finish and acknowledges `paused` synchronously;
C can return earlier with `pause requested`. Pause survives reload and clears
on process restart. These functions reject a primary with SQLSTATE `55000`.
`pg_wal_lsn_diff()` returns an exact integral `numeric`, including differences
outside signed int64; `::bigint` checks overflow. This does not add general
decimal numeric input, storage or arithmetic.
Text `||` and `substr(text, start [, length])` support the string operations used
by Patroni's status expression. Substring positions count Unicode characters,
including SQL's adjusted zero/negative starts. Array concatenation remains unsupported.
Fixed `bit(N)` expression casts and `B'...'`/`X'...'` literals support binary/hex
input, explicit padding/truncation, and C-compatible integer/bigint conversion.
Varying-bit types, bitwise operations, bit concatenation and bit table storage
remain unfinished.

See the [interop test inventory](t/interop/README.md) for refactor decisions,
generated-check domains, replay seeds and verification limits.

`t/logical-interior-ack.sh` checks publisher crash/restart with a flush inside
a commit record and subsequent end acknowledgment. `t/logical-mixed-mode.sh`
requires `C_PG_BINDIR` for the reference C build and checks subscriber metadata
protection while standard and extension subscriptions share a target. Both
scripts require Linux `ss` to verify listener ownership and create isolated
peers; `KEEP_ARTIFACTS=1` preserves their evidence.

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
- `COUNT(*)`, `COUNT(expr)`, `MIN`, `MAX` and `GROUP BY` are supported.
  `COUNT(expr)` counts non-NULL values, including false and zero; `MIN` and `MAX`
  ignore NULLs and support numeric, floating, text, date/time and interval inputs,
  plus implemented name, OID and LSN expression types. Hash aggregation is bounded
  by query work memory and spills through recursive hash partitions, with external sort
  as the skew/depth fallback. `SUM` supports smallint/integer inputs with bigint
  results, and bigint/finite numeric expressions with exact numeric
  results. `SUM` and `AVG` also support floating and interval inputs; integer
  and numeric averages return numeric. Empty or all-NULL sums and averages return
  NULL. SUM, AVG and text `array_agg` use sorted grouping with spill. Money remains
  unsupported.
  Derived SELECTs in `FROM` support `LATERAL` references. `HAVING`, ordinary
  `DISTINCT`, `UNION`/`UNION ALL`, and the expression subqueries described above are
  supported; see [HAVING and LATERAL](t/HAVING-LATERAL.md) for scope and tests.
  Row-valued and MULTIEXPR subqueries and `DISTINCT ON` remain unsupported.
  Hash subplans, full optimizer constant folding, and subquery decorrelation are
  not implemented. Joins use nested loops;
  INNER/CROSS join order uses a greedy statistics-based choice, while outer
  joins retain source order. NATURAL JOIN
  is not supported; comma-separated `FROM` items act as cross joins.
- `SEARCH`/`CYCLE`, data-modifying CTE bodies and
  mutual recursion are rejected (`0A000`). CTE producer scans
  use sequential scans and source order. Nonrecursive `NOT MATERIALIZED` uses
  per-reference plans with limited safe filter pushdown, not full joint optimization; remaining
  planning differences can affect error timing as well as performance.
- Window support is limited to the functions and frames listed above. `lead`,
  `lag`, value functions, `ntile`, floating-point rank functions, offset frame
  bounds, `GROUPS`, exclusions, `FILTER`, `DISTINCT` and `count(expr)` windows
  are deferred.
- No `OFFSET`/`FETCH`. `ORDER BY` accepts multiple keys with `ASC`/`DESC` and
  uses bounded in-memory runs with external merge when needed.
- Foreign keys and `NOT NULL`/`UNIQUE` beyond what is listed above are unsupported.
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
- Boolean and other unlisted table column types are unsupported. `text` is
  temporarily treated as `varchar(126)`; full text/varchar semantics remain a TODO.
- Extended query protocol (Parse/Bind/Describe/Execute/Sync) is not
  implemented; `psql` is unaffected, other drivers are.
- `pg_terminate_backend` (and fast/immediate shutdown) unwinds the target with
  an SBCL thread interrupt at an arbitrary point instead of PostgreSQL's
  deferred safe-point check. An interrupt during WAL I/O forces server shutdown;
  this remains a divergence from PostgreSQL's signal handling.
- `\d`, `\dt`, `\l` fail because the catalog SQL uses constructs beyond this
  subset (regex operators, `pg_catalog` functions); `psql` prints an error and
  shows no listing.


The Linux launcher owns PGDATA before storage initialization and writes the
standard eight-line `postmaster.pid` for process discovery. Existing PGDATA must
belong to the server user and use mode 0700 or 0750; missing directories are
created with mode 0700. A normal exit removes the owned PID file; SIGKILL leaves
it for stale-owner validation on restart. The persistent `.pg_lisp.lock` is a
local Lisp process lock and should not be removed while the server is running.

The pinned Patroni process/driver smoke test starts no HA controller or DCS:

```sh
uv venv /tmp/pg-patroni-4.1.5-venv
uv pip install --python /tmp/pg-patroni-4.1.5-venv/bin/python3 -r t/interop/patroni-requirements.txt
PATRONI_PYTHON=/tmp/pg-patroni-4.1.5-venv/bin/python3 ./t/postmaster-pid.sh
PATRONI_PYTHON=/tmp/pg-patroni-4.1.5-venv/bin/python3 ./t/patroni-start.sh
PATRONI_PYTHON=/tmp/pg-patroni-4.1.5-venv/bin/python3 ./t/physical-follow.sh
```

Physical follow also requires that pinned environment and checks Patroni's unchanged
five-column timeline/LSN fragment on primary, standby and paused standby, including
reload and resume. It also checks the unchanged ordinary 16-column cluster status
query with slot queries and synchronous mode disabled, using actual receiver
write/flush progress and message timestamps. Standby-leader control metadata,
slot-enabled and synchronous variants remain unfinished. Receiver timestamp
output currently supports UTC aliases only; other session time zones fail explicitly.
`./t/physical-exit-flush.sh` needs SBCL and psql without Patroni. It verifies
below-threshold receive flushing on disconnect and explicit stop, target crash/
restart, and receive-control fsync failure. Receiver session exit flushes complete
native records before clearing status; failed flush closes recovery until restart.
Smart/fast standby shutdown joins recovery workers outside backend admission and
publishes an applied restartpoint before successful process exit.
`./t/physical-shutdown-prepare.sh` tests the callable preparation primitive:
worker joins, a checkpoint at the applied terminal with received-ahead WAL
preserved, automatic following after restart, and failed publication. The private
control form `(physical-recovery-prepare-shutdown)` preserves follow configuration
and blocks further supported recovery mutations after preparation. It does not
exit the server. Automatic smart/fast integration is exercised by
`./t/physical-shutdown-signals.sh`, including received-ahead WAL, restart,
immediate escalation during preparation and failure exit status. Versioned native
control records active recovery before storage initialization and clean recovery
after durable shutdown preparation, including an unchanged restartpoint. Legacy
60-byte controls migrate to the 72-byte format; older binaries cannot read it.
An independent binary probe checks state/CRC, migration and startup/shutdown
fsync failure behavior. This does not add C `pg_control` disk compatibility.
Changed active main/accept-loop frames require a fresh process rather than reload.
`pg_postmaster_start_time()` reports the captured server-initialization timestamp
as timestamptz and remains unchanged across connections and reloads. The pinned
Patroni method is included in direct-start acceptance. An already-running image
from before this capture was added needs a one-time process restart. The full
20-field `pg_control_checkpoint()` interface remains unfinished; see the field
inventory in [PHYSICAL-HA.md](PHYSICAL-HA.md).
These tests check process discovery, start-time validation, direct startup, fast stop,
configuration precedence, SIGHUP and the pinned ConfigHandler's settings queries
through real psycopg2. The configuration subset supports `port`,
`listen_addresses`, `data_directory`, `checkpoint_timeout`, `max_wal_size`,
`max_slot_wal_keep_size` and the empty `restore_command` default, with includes,
auto.conf and CLI overrides. Nonempty restore commands are rejected until archive
recovery exists. SHOW,
current_setting and typed pg_settings rows expose the actual installed values.
Unknown settings fail visibly; empty or multiple listen addresses are unsupported.
Session startup `options` accept `application_name`, `statement_timeout` and dotted custom GUC
placeholders, including Patroni's defaults. Statement deadlines apply at safe
interrupt points and bounded lock waits, with independent budgets for statements
in one simple-query frame. RESET restores startup values; ordinary timeout
cancellation during blocked partial output is deferred as in C PostgreSQL.
Native defaults remain port 5433 and a 1 GiB slot cap. Full generated Patroni
configurations, recovery parameters, session GUC catalog coverage and control
interfaces remain unfinished, as does C physical data compatibility; see
[the HA ledger](PHYSICAL-HA.md).

Physical replication connections support inspection with `IDENTIFY_SYSTEM`,
`TIMELINE_HISTORY` and `SHOW`. History parsing includes native ancestry-position
checks; it does not perform promotion or change the current timeline. Timeline
aware writer storage, crash-safe promotion and rejoin remain unfinished.

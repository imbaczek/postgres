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
`--temp-file-limit-bytes BYTES` caps the live spill files used by one query and,
separately, each transaction's logical capture spool;
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
During startup recovery and physical standby replay, page records for a batch
are validated and staged in a spool that spills above 4 MiB. After the terminal
record is validated, replay validates the spool before applying its page records
in WAL order. Physical history validation also uses this staging mechanism. The
temporary file is cleaned up at batch completion or unwind and does not use the
SQL temporary-file quota. Each page record and catalog chunks are still decoded
in memory; standby replay preflight also retains metadata proportional to the
number of pages.
Primary WAL publication also stages copied page records in an internal spool,
building bounded record-sized chunks in sorted page-key order and validating the
spool before the terminal record. Its temporary file is independent of the SQL
temporary-file quota and is cleaned up on exit. The primary retains at most 512
dirty page vectors across its shared cache and private batch; further pages use
process-owned scratch slots with checksums bound to the page identity. Scratch
is transient, exempt from the SQL temporary-file quota, and released when its
last reference is gone. Page keys, contributor metadata and the sorted key list
remain in memory and grow with dirty pages; temporary record buffers also use
memory. This is not a total process memory bound and does not remove logical
transaction size limits. WAL is flushed before commit visibility and page
application.
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
zone, `interval`, `varchar` / `character varying`, `text`, `bytea`, `json`,
and `jsonb`. Unbounded variable-width values, including `numeric`, can use
inline compression or the table's TOAST storage when needed.
TOAST selection defaults to `MAIN` for `numeric` and `EXTENDED` for text,
varchar, bytea, JSON and JSONB. `ALTER TABLE ... ALTER COLUMN ... SET STORAGE`
accepts `PLAIN`, `EXTERNAL`, `EXTENDED`, `MAIN`, and `DEFAULT`. The policy
applies to subsequent writes; existing values remain readable after a change.
Full TOAST fetch walks the B-tree prefix under a read lease, validating and
copying one chunk at a time into a complete stored-payload buffer. It rejects
missing, misordered or malformed chunks and mismatched index entries. The full
value still occupies memory. Inline and external PGLZ decoding reads a validated
range of the existing source buffer without copying the compressed stream; the
external stored payload and full decompressed output still require allocations.
Malformed streams and trailing bytes in the selected range are rejected.
An UPDATE, including `ON CONFLICT DO UPDATE`, reuses external TOAST pointers
for columns omitted from its assignments. The pointer comes from the physical
row version actually claimed after any concurrent-update recheck; its chunks
are retained for the new version. Assigned columns are written using the
current storage policy, even when the assigned value is equal to the old one.
For a simple UPDATE, unassigned external columns that are neither referenced
nor indexed can defer detoasting; SET and WHERE inputs, assigned columns and
index keys remain materialized. Captured updates also materialize replica
identity columns, and updates without a usable identity fall back to full-row
materialization. CHECK constraints, RETURNING, joins, subqueries and unrecognized
expressions also fall back. A concurrent-update recheck uses the row version
actually claimed. Logical admission conservatively includes reused external
values in its full-row size bound even when an unchanged marker makes the event
small; a sufficiently large bound triggers full-row materialization and transport
validation.
For an interpreted SELECT plan with a projection over one base-table sequential
or non-parameterized index scan, optionally with filters, sorting or top-N,
and LIMIT, external values unused by the projection, filters, ordering, and
index qualification are not fetched. Demand analysis recognizes columns, constants, casts, Boolean and
arithmetic operators, and function calls with recognized arguments. Other plan
or expression shapes fall back to full tuple materialization.
An internal TOAST byte-slice API reads only intersecting chunks of an
uncompressed external value under an active snapshot protecting its parent.
It validates the pointer and requested chunks, but does not check unrequested
chunks. For a nonempty compressed slice it reads and validates every stored
chunk and the complete PGLZ stream, retaining the requested slice and a 4096-byte
history rather than the full value. Empty slices skip chunk checks. The limited
SQL bytea substring optimization described below uses this API.
Fixed-width types accept only `PLAIN` or `DEFAULT`. `VACUUM FULL` rewrites
existing values using the current policy; it fails if a `PLAIN` value cannot
fit on a heap page, leaving the original table intact.

`json_typeof(json)` and `jsonb_typeof(jsonb)` return the top-level type as
text: `object`, `array`, `string`, `number`, `boolean`, or `null`.
`json_array_length(json)` and `jsonb_array_length(jsonb)` return the number of
top-level array elements as an integer; objects and scalars raise SQLSTATE
`22023`. All four return SQL NULL for SQL NULL input. JSON null is a JSON
scalar, so its type is the text `null` and it is not an array.
JSONB `||` merges top-level objects with right-hand keys winning. Otherwise it
concatenates arrays, treating each non-array root as a single element. The merge
is not recursive. Generated values obey the 512-level nesting limit.

Decimal literals and numeric casts support finite values; numeric casts also
accept NaN, infinities, and signed hexadecimal, octal, and binary integer input
with PostgreSQL's underscore rules. Precision-constrained numeric values are rounded and range checked. Floating
types use IEEE values, including NaN and infinities. Date/time and interval
types support casts, comparisons, and the implemented arithmetic operators;
time and timestamp precision is at most six fractional digits. Interval field
and precision modifiers are supported. These are subsets of PostgreSQL's full
type and operator catalog, not a promise that every input spelling or function
is implemented.

SQL array expressions support text, varchar, name, internal `"char"`, bool,
int2, int4, int8, numeric, float4, float8, oid, date, time, timetz, timestamp,
timestamptz, interval, pg_lsn, bit and int2vector elements. `ARRAY[...]` chooses a common element type; an empty
constructor needs a type supplied by context or a cast. Nested constructors
must have matching dimensions and lower bounds, and support up to six
dimensions. `array_length`, comparisons, scalar `ANY`/`ALL`, and text
`array_agg` are supported in the implemented expression subset. These array
expression types do not imply support for array-valued table columns.

Typed array input accepts brace-delimited values, quoted and backslash-escaped
elements, unquoted `NULL`, rectangular nested arrays, and explicit bounds such
as `[0:1]={10,20}`. Elements go through the target type's input conversion.
Ordinary empty arrays have no dimensions; absent explicit bounds, lower bounds are 1.
The legacy untyped literal path for scalar `ANY`/`ALL` accepts only
one-dimensional input without explicit bounds.

`int2vector` text input accepts space-separated decimal smallints. Its input
representation uses one dimension with lower bound 0, even when empty. A direct
`ARRAY[...]::int2vector` constructor instead constructs an int2 array in context,
retaining the constructor's dimensions, bounds, and NULL elements. A general
cast from an existing array to `int2vector` is not supported. Casting an
`int2vector` to `int2[]` preserves its dimensions and bounds, including the
zero-length dimension; casting to a different supported array element type
converts the elements and produces a dimensionless empty array when empty.

Internally, `sql-array` stores an element type ID, a flat row-major element
vector (including SQL NULL markers), dimension lengths, and lower bounds.
The dimension and lower-bound lists have equal length, and the dimension
product matches the element count for nonempty arrays; ordinary empty arrays use an
empty vector and no dimensions or bounds, with the `int2vector` cast exception
described above. Sort-arena copying recursively copies
the array and its compound values, and query spill preserves all four fields
and restores them on replay. This is an executor representation, not the C
server's on-disk array format.

`width_bucket(value, thresholds)` returns the number of thresholds less than or
equal to the value, using a binary search. Thresholds must be one-dimensional
and contain no NULLs; an empty array returns zero. The caller must supply
ascending thresholds: their order is assumed, not checked. The supported
threshold element types are the array expression types listed above.
The two-argument overload resolves a common type from the value first and then
the threshold element type; unknown literals are resolved from that type. An
`int2vector` threshold supplies int2 elements, while `int2vector[]` supplies
int2vector elements. Ordinary array-valued operands require an array type over
that array type, which is unavailable: even with a NULL threshold, resolution
reports SQLSTATE `42704` before strict NULL evaluation. An int2vector operand
can instead resolve to `int2vector[]` thresholds. Incompatible argument types
fail function resolution before this array-type lookup.
For `timetz` thresholds, compatibility follows the tested little-endian 64-bit
C peer: its fixed-width search uses a 12-byte stride over 16-byte aligned
elements. Results for multiple thresholds can therefore differ from a logical
`timetz` comparison and from C builds with a different layout.
`width_bucket(value, bound1, bound2, count)` supports numeric and float8
arithmetic, ascending or descending bounds, and a positive int4 count. Values
outside the range return 0 or count + 1; the first bound is included and the
second excluded. Bounds must be finite, unequal and not NaN. Both forms return
NULL when an argument is NULL.

`random()` returns a float8 uniformly in [0, 1). `random(min, max)` supports
integer, bigint, and numeric bounds, inclusive at both ends; numeric results
use the greater input scale. Bounds must be ordered, and numeric bounds must
be finite. Named `min` and `max` arguments are supported. Calls are strict:
NULL arguments yield NULL. `setseed(float8)` accepts values from -1 through 1,
returns SQL void (an empty, non-NULL text field on the wire), and rejects NaN
and out-of-range values. Ambiguous overloads, such as two smallint arguments,
require explicit casts.

Each connection has its own shared random state, initialized on first use.
`setseed` resets that state, so the same seed and sequence of calls reproduce
the same results. State changes survive transaction rollback. The implementation
uses PostgreSQL's xoroshiro128** generator and splitmix64 seed expansion;
integer ranges use rejection sampling, and numeric ranges use scaled decimal
digits and rejection sampling. Equal bounds return the bound without advancing
the generator. This pseudorandom generator is not cryptographically secure.

Basic numeric functions include `abs`, `numeric_abs`, `sign`, `round`,
`trunc`, `ceil`/`ceiling`, `floor`, `sqrt`, `numeric_sqrt`, `exp`,
`numeric_exp`, `ln`, `numeric_ln`, `log`, `log10`, `numeric_log`, `scale`,
`min_scale`, and `trim_scale`.
`scale` returns the stored number of fractional digits; `min_scale` returns
the fewest fractional digits needed to preserve the value, and `trim_scale`
returns the value at that minimum scale. For example, `scale(1.2300)` is 4,
`min_scale(1.2300)` is 2, and `trim_scale(1.2300)` is 1.23. For NaN and
infinities, `scale` and `min_scale` return NULL and `trim_scale` returns the
input. All three return NULL for a NULL argument.
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

The catalog functions `float4abs(float4)` and `float8abs(float8)` share the
corresponding `abs` implementation and return their argument type. The
`float8` functions `dround`, `dtrunc`, `dsqrt`, `dexp`, `dlog1`, and
`dlog10` each take one `float8` and return `float8`; they share execution with
`round`, `trunc`, `sqrt`, `exp`, `ln`, and `log10`, respectively. These
catalog names remain distinct function identities during expression analysis.

The integer catalog functions `int2mod`, `int4mod`, and `int8mod` each take two
arguments of the named integer type (or arguments implicitly coercible to it)
and return that type. They are strict and immutable. Like integer `%`, they
return the remainder after division truncated toward zero; a nonzero remainder
has the dividend's sign. A zero divisor raises SQLSTATE `22012`, while the
minimum value divided by -1 has remainder zero. Their names remain distinct
function identities during expression analysis, including DISTINCT ordering.

The numeric catalog helpers `numeric_eq`, `numeric_ne`, `numeric_gt`,
`numeric_ge`, `numeric_lt`, and `numeric_le` compare two numeric arguments and
return boolean; `numeric_cmp` returns -1, 0, or 1. Their ordering places
negative infinity below finite values, positive infinity above them, and NaN
above positive infinity; NaN compares equal to NaN. `numeric_smaller` and
`numeric_larger` return the selected numeric argument. On equality they return
the second argument, preserving its display scale: for example,
`numeric_smaller(1.0, 1.000)` displays `1.000`. `numeric_inc` adds one,
`numeric_uminus` negates, and `numeric_uplus` returns its argument. These
helpers accept arguments implicitly coercible to numeric, are strict (a NULL
argument yields NULL), and are treated as immutable.

Floating-point math also includes `pi`, `degrees`, `radians`, `cbrt`/`dcbrt`,
`sin`, `cos`, `tan`, `cot`, `asin`, `acos`, `atan`, and `atan2`; the
degree-based variants are `sind`, `cosd`, `tand`, `cotd`, `asind`, `acosd`,
`atand`, and `atan2d`. Hyperbolic functions include `sinh`, `cosh`, `tanh`,
`asinh`, `acosh`, and `atanh`. These accept arguments coercible to float8 and
return float8; a NULL argument yields NULL. The trigonometric functions use
radians unless their names end in `d`. Degree-based functions reduce angles
and use PostgreSQL's special-angle formulas. NaN propagates, infinite inputs
to sine, cosine, tangent, and cotangent raise `22003`, and out-of-domain
inverse functions raise `22003`. Poles such as `cotd(0)` and `atanh(1)`
return infinity. Results depend on the host math library for general inputs.

`div(x, y)` and `numeric_div_trunc(x, y)` return a numeric quotient truncated
toward zero. `mod(x, y)` supports numeric and integer overloads; `numeric_mod`
uses numeric. The remainder has the dividend's sign, so `mod(-5, 2)` is -1.
`numeric_add`, `numeric_sub`, `numeric_mul`, and `numeric_div` expose the
corresponding numeric arithmetic operations; `numeric_div` uses the normal
numeric division scale and rounding, unlike `div`. Arguments must be implicitly
coercible to the selected overload; two unknown `mod` arguments are ambiguous.
These functions are strict: a NULL argument yields NULL. Division or remainder
by zero raises `22012`, except that NaN takes precedence. NaN propagates;
for division, infinity divided by infinity is NaN and a finite value divided
by infinity is zero. For remainder, infinity modulo a nonzero value is NaN,
and a finite value modulo infinity is the finite value.

`gcd(x, y)` and `lcm(x, y)` accept integer or numeric inputs and return a
nonnegative result. Integer results retain the resolved integer type and report
`22003` on overflow. Numeric results retain the larger input scale; a NaN or
infinity input produces NaN. Two unknown arguments, or two smallint arguments,
are ambiguous. `factorial(x)` accepts a bigint-coercible argument and returns
numeric; negative arguments and arguments above 32177 raise `22003`. These
functions return NULL when any argument is NULL.

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

Integer bitwise operators `&`, `|`, `#`, `~`, `<<` and `>>` support int2,
int4 and int8. Binary bitwise operands resolve to a common integer width;
shifts retain the left operand's width and take an int4 count. The named
`int2`, `int4` and `int8` helpers with `and`, `or`, `xor`, `not`, `shl`,
`shr` and `abs` suffixes are also available. These operations are strict.
Incompatible bitwise operand types raise `42883` during operator resolution;
unresolved unknown operands can instead raise the ambiguous-operator error
`42725`. Both errors include the operand type signature and explicit-cast advice.
Generic operators bind below arithmetic and associate left to right; prefix
generic operators take an arithmetic expression as their operand. Prefix `@`,
`|/` and `||/` provide absolute value, square root and cube root respectively.
They accept supported numeric operands; `|/` and `||/` return float8, while
`@` preserves the numeric overload. Unknown literals and NULL resolve through
the preferred float8 overload. An incompatible typed operand raises SQLSTATE
`42883` during analysis, after errors in analyzing the operand itself; invalid
unknown literal input raises its input conversion error after operator resolution.
Shift counts are masked as on the x86 C peer, with int2 promoted to 32 bits
before shifting. C does not define all signed shift cases portably, so results
for such cases may differ from a PostgreSQL server built for another host.

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

Function calls accept `ALL` before their arguments; it has the same effect as
omitting it, including for scalar functions. `DISTINCT` and an `ORDER BY`
inside the argument list are parsed as aggregate decorations. Function lookup
uses the ordinary arguments first, so an unknown signature fails lookup; after
a scalar function is selected, these decorations are rejected before argument
coercion. Aggregate `DISTINCT` and argument-level `ORDER BY` remain unsupported,
as do these modifiers on window calls.

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
`extra_float_digits` controls floating-point output. `bytea_output` selects
hexadecimal (the default) or traditional escape output. Supported isolation levels
are `READ COMMITTED` and `REPEATABLE READ`; client encoding is UTF8 and
`standard_conforming_strings = on`.
The extended query protocol supports Parse, Bind, Execute, Sync, Describe,
Close and Flush for a single INSERT, UPDATE or DELETE without RETURNING, and
the plain `SELECT * FROM table` scan used for initial copy.
Parse requires explicit supported built-in expression type OIDs for parameters;
string literals in these statements are rejected in favor of parameters.
Parameters use text format, except that bytea also supports binary format;
SELECT result columns may use binary format only for bytea; limited portal execution is unsupported; each portal
can execute only once. This is a restricted DML interface, not general
extended-query support. During reload drain, a Simple Query sent while an
extended implicit transaction is open is rejected if it has statements after a
transaction-ending statement. An old protocol handler also fails closed for
extended message suffixes during a mixed live reload.
Connections entering the new backend loop spool Bind messages whose length word
exceeds 1 MiB to an anonymous per-frame file using 64 KiB receive buffers. The
complete spool's CRC is checked before parsing and it is closed after dispatch,
including on error. Individual typed parameters and portal and statement names
still materialize. During reload drain, extended messages other than Sync are
rejected; an open implicit extended transaction is rolled back on error.
Client startup and SQL `application_name` support SET, SHOW, RESET and
`current_setting`/`set_config`, including transaction-local values. Other LOCAL
settings remain unsupported. Parameter reports are coalesced before ReadyForQuery.
Unsupported setting names and values return errors.

`pg_export_snapshot()` exports the snapshot of an explicit read-only REPEATABLE
READ transaction. Another unused explicit read-only REPEATABLE READ transaction
in the same database can import its token with `SET TRANSACTION SNAPSHOT` before
its first snapshot. The token expires when the exporter ends, while an existing
importer retains its own snapshot and schema lease until its transaction ends;
the lease can delay incompatible schema changes.

SELECT text DataRows preflight every field's encoded length and checksum before
writing the frame. Text, JSON, JSONB and bytea output use bounded UTF-8 blocks;
bytea honors `bytea_output`, and other types retain their session-dependent
formatting and are formatted once. Emission checks the lengths and checksums
again, withholds the last frame byte until validation succeeds, and closes the
connection if emission fails. Source SQL values, including detoasted values,
still occupy memory, as can values and frames at the receiving client.

Idle simple-query messages containing only supported ordinary settings and MVCC
statements commit or roll back together. DDL, maintenance and transaction-control
messages retain the prior per-statement behavior; see the staged boundary in
[the HA ledger](PHYSICAL-HA.md#application-name-and-supported-implicit-query-messages).
The read-only virtual catalogs expose a subset of `pg_type`, `pg_class`,
`pg_attribute`, and `pg_index`. TOAST ownership and its heap and index appear in
those rows, but the internal TOAST relations are not directly queryable as SQL
tables. `pg_type` describes the built-in expression types supported here; it is
not a complete PostgreSQL type catalog.
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
When no live logical slot or slot creation is active, newly assigned transactions
can skip logical row and transactional-event capture. Capture mode remains fixed
for a transaction that has already written. Slot creation first enables capture
for new transactions and waits for older uncaptured writers to finish before
establishing its boundary. It rejects creation by a transaction already admitted
as a writer or changed by row, TRUNCATE or sequence work, and rejects a wait
that would deadlock on its retained schema lease. Dropping or losing the last
slot permits new transactions to skip capture; already captured transactions
remain captured. Unknown capture state is handled conservatively.
In extension mode, combining `--binary-bytea` and `--binary-copy` creates a
durable binary-bytea-only slot profile and records the subscription configuration
for restart; reconnecting that slot requires binary bytea streaming. When a row
fits binary transport but not text transport, admission requires that profile.
Before publishing the transaction's WAL, a commit fence rejects it if an
incompatible live slot exists or slot creation is armed, conservatively avoiding
an incompatible concurrent subscriber. A later text-profile initial copy can
still fail on a large bytea already stored before that slot was created. The
128 MiB captured row event and 132 MiB frame limits remain in force.
Initial copy batches rows to an 8 MiB target; a larger row uses a separate
transaction and must still fit the source/target protocol and logical WAL limits.
For extension-mode initial copy, each target INSERT uses typed Bind parameters
emitted in bounded blocks. The source
DataRow is read through a bounded cursor without a complete frame copy. Text-format
fields above 1 MiB, or fields that would exceed the shared 4 MiB conservative
resident-field budget, use row-owned temporary slices, retained through their
target chunk commit and released on completion or error. This includes integer
fields. A chunk flushes after 32 rows with spilled owners; the 8 MiB batching
target counts the original UTF-8 field bytes. Row metadata and other allocations
remain in memory, and the target still materializes parameter values.
Frontend query message lengths are limited to 132 MiB, including the four-byte
length word. The lexer counts quoted string and identifier contents before
allocating their exact-sized decoded strings, avoiding a growable literal buffer;
the incoming frame and complete query string still materialize. When capture is enabled, each encoded row change is limited to
128 MiB, including encoding overhead; transactional utility events retain a
64 MiB limit. Row changes made by the `pg-lisp-subscribe-apply` application have an extra 1 MiB of headroom; WAL
recovery and slot readers accept individual events up to 129 MiB. Captured row
events above 64 MiB require a supported replica identity and schema and must
pass preflight checks for pgoutput, subscriber apply Bind, and full-new-row
initial-copy DataRow and Bind frames. This permits a 65 MiB quote-heavy text
value when the other limits and checks are met. Values written before a slot existed may
still be too large for a later initial copy. Individual decoded values and JSONB
trees can still require substantial memory. The aggregate
transaction size is controlled by the capture spool's temporary-file quota,
which is unlimited by default. Unscoped logical readers that materialize changes
retain a 65 MiB encoded-byte limit, checked after decoding the event or legacy
chunk that crosses it; peak memory can therefore include that additional decoded
data. Scoped slot readers use temporary spools instead. Large transaction
publication can hold the WAL lock for the full batch.
These event limits do not guarantee that a value of that size will fit.

Logical capture keeps encoded events in memory until their cumulative size
exceeds 4 MiB, then spills all events to a transaction-owned temporary file.
For each change, capture first counts encoded bytes and computes a checksum;
when the threshold would be exceeded, a second encoding pass writes directly
to the file. String encoding uses bounded UTF-8 blocks. JSONB capture counts
and emits its canonical text in bounded UTF-8 blocks without constructing a
complete output string; the normalized JSONB value itself remains in memory.
The spool has its own quota using the `--temp-file-limit-bytes` setting,
separate from query spill quotas; its length and checksum framing also counts
against that quota. It is cleaned up when the transaction ends. At commit the
spool is validated in bounded blocks and its WAL records planned before
publication. A second blockwise pass writes fragments or bounded chunks without
materializing a whole oversized event. For fragmented events, recovery and
logical slot decoding keep encoded events of at most 4 MiB in memory; larger
events use a temporary file and are read from its stream. These files do not
use the SQL temporary-file quota. Recovery validation avoids retaining decoded
text, varchar, JSON text, or bytea values, but still constructs JSONB and
numeric values. The scoped slot sender stages committed changes in a
transaction spool, spilling above 4 MiB without the SQL temporary-file quota.
For legacy WAL chunks, the scoped reader validates each event and stages its
original encoded bytes, avoiding re-encoding and retaining decoded text or
bytea values during staging. For fragmented events, the scoped reader validates
the complete event and copies its original bytes into the transaction spool in
bounded blocks. Validation avoids materializing text and bytea, but may still
construct JSONB and numeric values; later output decoding can materialize
individual values. The unscoped reader retains its materialization behavior.
Commit and terminal validation still precede output. A slot scan captures a fixed durable horizon and
pins its retained start under the WAL lock. It copies one WAL record at a time
under that lock, then validates and stages it outside the lock. The pin prevents
recycling during the scan, but does not prevent slot invalidation; slot identity,
ownership and retention are rechecked before returning progress. The pin is
released when the scan ends or unwinds.
It validates the staged changes before sending and iterates them again for
output; the spool is cleaned up when the reader scope ends. For spilled events,
each iteration checks the event CRC in bounded blocks before decoding through a
bounded stream cursor, without allocating the complete encoded event. Scoped
standard and private senders use event-lifetime slices for text, varchar, JSON
and bytea, checking slice CRCs and emitting bytes or bytea hex in bounded blocks.
Slices expire when the event callback returns. Recovery validation and scoped
senders also scan canonical JSONB values of at least 64 KiB without constructing
the value, including nested arrays and objects, strings, literals and decimal
numbers. Object keys must be in canonical order; their comparisons use bounded
buffers even for long keys. Nesting and decimal digit bounds apply to this fast path. Smaller
values, noncanonical forms and values outside those bounds fall back to the
materializing parser for validation and normalization; malformed values are
rejected. SQL numeric values and ordinary list-based reader values still
materialize; complete transaction validation precedes
output. The sender measures each INSERT, UPDATE, or DELETE row before writing one pgoutput
CopyData frame through a bounded buffer. The subscriber reads replication frames
directly through a length-bounded stream cursor and completes decoding before
applying an operation. Pgoutput row text-format fields above 1 MiB or beyond
the frame's conservative 4 MiB resident-field budget, including integer fields,
are validated as UTF-8 and stored in an unlinked, process-owned temporary file
for synchronous apply; their CRC is checked during SQL preflight and emission.
Extension Message contents and other allocations still materialize. INSERT, UPDATE and DELETE apply
uses typed Bind parameters, counted and emitted in bounded blocks; the target
still materializes parameter values. For bytea fields, canonical hex text over
262144 characters is decoded in bounded blocks to a binary Bind parameter;
other spellings and smaller fields use text Bind. Initial-copy source fields
default to text, and this conversion does not raise the source bytea,
event or frame limits. A malformed or incomplete
incoming frame closes the source connection; a partial row-frame or query write
closes its connection. Other message types use their existing encoders. The 4 MiB
threshold is not a bound on the size of an individual event. The 128 MiB capture
and 129 MiB WAL event limits and protocol frame length limit still apply. The
legacy unscoped reader that materializes a transaction remains limited to 65 MiB;
both replication senders use the scoped spool reader. The internal legacy
list-based WAL adapter also retains its aggregate limit; normal capture uses
the transaction spool.

When an encoded row event exceeds one WAL record, the transaction's events
are written as ordered fragments and reassembled by recovery and logical slot
reads. Smaller events retain the existing chunk format. Fragmentation does not
remove the per-event limit; the commit path checks the complete batch before
writing its logical records.
`t/logical-replication.sh`
exercises initial copy, DDL and sequence changes, two independent replicas, and
restart after a lost feedback message.

For a keyed UPDATE, logical capture marks a reused external non-key value as
unchanged instead of encoding it again. Key values remain in the identity
image; apply omits marked columns so the target retains their existing values.
Without a usable key, capture retains full row images and does not use this
marker. New private logical WAL row events use format version 5, and the current
reader rejects pre-version-5 row events; compatibility is not limited to rows
containing the new marker. This optimization applies to proven pointer reuse,
not every equal value.

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

Standard mode uses explicit publication table lists and the bundled subscriber
requests text tuples, with streaming, binary, two-phase, and failover options
disabled. A pg-lisp publisher also accepts standard `binary 'true'` and emits
binary tuple fields for bytea only, retaining text for other types. Extension
mode defaults to text; `--binary-bytea` opts the bundled subscriber into binary
bytea streaming from a supporting pg-lisp source. Older sources that do not
recognize the option fail rather than silently switching formats. Separately,
`--binary-copy` requests binary bytea and text for other columns through an
extended SELECT during initial copy. It works with the native private snapshot
and with C or native standard sources using an exported snapshot bridge on a
normal connection. Standard discovery uses catalog projections. Standard mode
requires source `pg_export_snapshot()`.
This is not binary COPY and does not request
arbitrary C global binary output or change admission and copy limits. DDL/sequence
automation remains in extension mode. UPDATE messages may omit unchanged
out-of-line values; the subscriber preserves those values on the target. Row
filters, column lists, partitions, generated columns, and replica identity FULL
remain outside the subset.
See [C-peer acceptance and limits](REPLICATION-COMPATIBILITY.md) for the pinned
reference, test matrix, recovery evidence, and exact options.

For a coordinator-free planned writer handoff experiment, see the
[manual logical switchover rehearsal](MANUAL-SWITCHOVER.md). Its script exercises
Lisp→Lisp, Lisp→C and C→Lisp with an owned writer, source shutdown as the fence,
durable target validation, sequence alignment and post-cutover writes.

## Testing

From `src/lisp`, `PG_C_BINDIR=/path/to/installed/postgres/bin ./t/random-sql.sh`
compares seeded sequences and mixed overloads with the repository's C PostgreSQL,
including session isolation, rollback, void wire output, NULLs and errors. It
starts both servers and honors `SBCL`, `PYTHON`, `PORT`, and `REFERENCE_PORT`.

From `src/lisp`, `PG_C_BINDIR=/path/to/installed/postgres/bin ./t/array-types.sh`
compares scalar-array and `width_bucket` cases, result metadata and errors with
an isolated C peer. It honors `PYTHON`, `PORT`, `REFERENCE_PORT`, `PG_C_LIBDIR`
and `PSQL`; its `timetz` comparisons cover the local C build's layout.

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
outside signed int64; `::bigint` checks overflow.
Text `||` and `substr(text, start [, length])` support the string operations used
by Patroni's status expression. Substring positions count Unicode characters,
including SQL's adjusted zero/negative starts. Direct text or varchar substring
projections with constant bounds can use the same narrow interpreted single-table
sequential-scan optimization described below when the column is not otherwise
needed in full. For external values it streams all chunks, validates the full
UTF-8 value (including for an empty result), and retains only selected
characters; compressed values also decode the full PGLZ stream. Other plan or
expression shapes use ordinary materialization. In the same plan scope, direct
`length` and `octet_length` projections on bytea and text/varchar, and
`char_length` or `character_length` on text/varchar, can avoid full
materialization when the column has no full-value use. Byte lengths use the
validated TOAST pointer size without reading chunks, so they do not detect chunk
corruption; character lengths stream and validate the complete UTF-8 value.
The `substr` and `substring`
function forms also accept `bytea`, where positions count bytes. Bytea `||`
concatenates octets; when one operand is bytea, an unknown literal on the
other side is read as bytea. The result allocation, including its four-byte
header, is limited to 1073741823 bytes. Interpreted
direct substring projections with constant bounds over one base-table sequential
scan can read only the needed uncompressed TOAST chunks when the column is not
otherwise needed in full. Multiple slices and other recognized projections,
filters, and LIMIT are supported. A column used in full by a projection or filter
is materialized, while eligible slices of other columns can still use partial
fetches. Unrecognized plan or expression shapes fall back to ordinary
materialization; compressed slices still decode the complete stored stream.
Array concatenation remains unsupported.
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
./t/logical-large-json.sh # fragmented external JSON/JSONB capture, apply, and restarts
./t/logical-large-bytea.sh # exact external bytea, fragmented replication, and restarts
./t/logical-large-transaction.sh # transactions above 65 MiB, replay and apply
./t/logical-unchanged-toast.sh # retained marker replay after source crash and subscriber resume
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
- Boolean and other unlisted table column types are unsupported. `text` and
  unbounded `varchar` are distinct supported types; `varchar(n)` accepts lengths
  from 1 through 10485760.
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
Native defaults remain port 5433 and unlimited standard slot retention
(`max_slot_wal_keep_size = -1`). An explicit `--max-slot-wal-keep-bytes` or
`PG_MAX_SLOT_WAL_KEEP_BYTES` selects the native exact-byte cap; the standard cap
uses whole segments. Public physical slot create, advance and drop, the
21-column SRF and the 22 C view columns plus native `retained_wal_bytes` (23
columns total) are
implemented. Named physical slot streaming remains open. Full generated Patroni
configurations, recovery parameters, session GUC catalog coverage and control
interfaces remain unfinished, as does C physical data compatibility; see
[the HA ledger](PHYSICAL-HA.md).

Physical replication connections support inspection with `IDENTIFY_SYSTEM`,
`TIMELINE_HISTORY` and `SHOW`. History parsing includes native ancestry-position
checks; it does not perform promotion or change the current timeline. Timeline
aware writer storage, crash-safe promotion and rejoin remain unfinished.

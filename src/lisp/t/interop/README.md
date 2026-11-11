# Interoperability test inventory

`python3 t/interop/record-comparison.py NATIVE_PORT REFERENCE_PORT` compares
whole-row equality, ordering, DISTINCT and JSON rendering against an isolated C
peer, using both Simple Query and parameterized Extended Query. Both supplied
ports must belong to trust-authenticated test servers. The test includes NULL
JSON fields, nested records, JSONB controls and absent outer-join records.
Record grouping covers singleton inputs and decisive field prefixes; DISTINCT
covers projected constants, inline and materialized CTEs, grouped output slots,
and volatile projection barriers.

Inventory and refactor review: 2026-10-25, based on commit `19df2752d7e`.

| Tests | Opportunity | Decision |
| --- | --- | --- |
| SQL/parser/catalog error cases in `protocol-tests.lisp`, plus standard/copy/history tests | Repeated `handler-case` scaffolding obscured the SQLSTATE under test. | Replaced 39 handlers with `sqlstate-of`; retained every expected state and operation. A helper regression checks one evaluation, successful return, PostgreSQL errors and propagation of host errors. Removed the narrower copy-only helper. |
| Seven C differential tests in `apply-tests.lisp` | Repeated port parsing, two connections and cleanup. | Added `with-reference-clients`, retaining all SQL cases and direct wire comparisons. Nested cleanup also closes the first connection if opening the second fails. |
| UTF8 name clipping | One explicit multibyte boundary example cannot explore varying character widths. | Kept that example; added a Function Spec for the maximal whole-character prefix within 63 bytes and a separate idempotence Property. Inputs mix one-, two-, three- and four-byte characters, with lengths 0–99. Reference: C `name.c:namein` and `pg_mbcliplen`. |
| Signed commit timestamps | Existing endpoint and endian vectors are strong oracles; generated signed values add breadth. | Kept those vectors; added an eight-octet Function Spec and a signed-int64 roundtrip Property. C `proto.c:logicalrep_write_commit` uses an int64 timestamp; pg-lisp's private WAL remains little endian. |
| Standard subscription resume guard | Fixed examples test metadata changes but few position orderings. | Kept them; added a Property requiring the target marker to cover both source confirmed and restart positions over the unsigned-int64 range. |
| Replica identity key prefix | Existing examples distinguish primary keys from INCLUDE columns. | Kept them; added a Property sampling rotations 0–5 and prefix lengths 1–6, checking sorted keys and invariance when INCLUDE attributes are appended. |
| Publication-selection codec | Generated multi-relation/UTF8 histories could add breadth. | Future candidate. Keep the independent 26-byte fixture, corruption cases and historic catalog mutation/recreation tests. |
| Publication action subsets | All 16 possibilities are already enumerated. | Keep exhaustive deterministic coverage; random sampling would weaken it. |
| C frame fixtures, native subscriber and COPY/apply crash tests | Their value comes from independent bytes, native workers, visibility and durable progress. | Keep explicit. A self-roundtrip property cannot replace these oracles. |
| Legacy `t/unit-tests/` and shell concurrency/crash suites | Many tests manage real files, threads, sockets, WAL and failpoints. | Retain their runners and fixtures; no bulk Rove/cl-spec migration. Existing setup helpers already centralize most shared state. |

The legacy-unit decision above describes the initial interop refactor. The later
[complete test inventory](../TEST-INVENTORY.md) records the reviewed migration of
68 independently runnable cases to `pg-unit-tests` and the 161 deferred cases;
process and concurrency harnesses retain their coverage.

The existing six test files lose 117 net lines, including the new SQLSTATE
helper regression. New generated checks live in `property-tests.lisp`.
That refactor changed no server implementation.

The subsequent metadata/partial-ACK hardening adds a fifth Property for replay
eligibility around a commit record, and a deterministic standard-state protection
test (135 primary Rove tests total). The generated checks still run through four
Rove cases. `../logical-interior-ack.sh` verifies the raw-wire replay contract
across real publisher crashes; it uses a scripted transport peer because the
bundled subscriber correctly acknowledges complete transactions and cannot
produce interior feedback. `../logical-mixed-mode.sh` verifies standard metadata
survives extension bootstrap and DDL on the same target. See the main
compatibility document for their commands, semantics and review evidence.

## Running

Run `python3 t/interop/ddl-publication.py SQL_PORT CONTROL_SOCKET` from
`src/lisp` against an explicitly owned SQL server and its private control
socket. It checks relation gate ordering, the interleaved schema event chain,
late subscriber drift and no-op guards.

See the main README's test dependency setup. The primary `pg-interop-tests`
ASDF/Rove suite executes the Function Specs and Properties through four ordinary
Rove tests, using seed 42 and 200 trials per declaration. Merely registering
declarations does not run them. For interactive inspection, load that system
and use cl-mcp `spec-check` with `function=` for a Function Spec, and `property=`
or `symbol=` for Properties; a function selection does not run its Properties.

The live `pg-interop-tests/apply` and `pg-interop-tests/native` systems require
isolated pg-lisp and unmodified C peers through `PG_INTEROP_TARGET_PORT` and
`PG_INTEROP_REFERENCE_PORT`; `PG_INTEROP_REFERENCE_USER` defaults to postgres.
The apply suite creates fixtures and must start against fresh databases.

Run the pinned Patroni 4.1.5 synchronous monitor oracle from `src/lisp` with
the pinned environment, C binaries and matching libpq development files:

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/patroni-sync-monitor.py --reference-bin /path/to/c/bin
```

It uses Patroni's connection pool, complete cluster info query and sync handler,
and starts owned C and native peers. C physical feedback clients and two native
replicas started through real `Postgresql.follow` exercise FIRST/ANY monitoring.
It does not run DCS, election or the HA loop.

The descendant oracle uses the same pinned environment and C binary directory:

```sh
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/patroni-descendant.py --reference-bin /path/to/c/bin
```

It runs isolated three-peer descendant and applied-divergence cases using real
Patroni follow and promotion. Reseeding is an explicit local native backup with
shared filesystem access, not the default or remote Patroni bootstrap clone. It
does not run DCS, election or the HA loop. All database peers are native; the C
binary directory supplies `pg_config` and `pg_isready`.

The checkpoint oracle uses the same pinned environment and C binary directory:

```sh
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/control-checkpoint.py --reference-bin /path/to/c/bin
```

It compares the 20-column descriptor, scalar oid8 values and casts, and native
persisted facts. It also exercises the unchanged Patroni standby-leader query on
a real native follower through restartpoint publication, source-offline cold
restart and corrupt-pointer rejection. Unpersisted native fields remain NULL.

Run the native source FPW cold-recovery oracle from `src/lisp` on Linux with
Patroni 4.1.5, psutil and psycopg2 in the pinned Python environment, SBCL and
matching libpq build dependencies. It uses a Linux child subreaper to verify
injected process exits. The RAM wrapper keeps data, caches and temporary files
on the project's private RAM mount:

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3
./t/test-ramdisk.sh setup 4G
PATH="/path/to/c/bin:$PATH" PG_CONFIG=/path/to/c/bin/pg_config \
  ./t/test-ramdisk.sh run "$PATRONI_PYTHON" t/interop/source-fpw-cold.py --point both
```

Both replay cuts check source-offline cold recovery of the exact OFF witness
and last-disable position, a clean standby shutdown and second cold restart,
interrupted promotion after the child EOR is synced, and another child restart
with local ON policy and the row intact. The last-disable check corresponds to
the boundary tracked in C `xlog.c:xlog_redo`. The default case publishes policy
changes manually through the control REPL. To exercise configuration and live
promotion, use the same pinned environment and private RAM mount:

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3
export PG_CONFIG=/path/to/c/bin/pg_config
export PATH="/path/to/c/bin:$PATH"
./t/test-ramdisk.sh setup 4G
./t/test-ramdisk.sh run "$PATRONI_PYTHON" t/interop/fpw-config.py
./t/test-ramdisk.sh run "$PATRONI_PYTHON" t/interop/source-fpw-cold.py --point both --configured
./t/test-ramdisk.sh run "$PATRONI_PYTHON" t/interop/source-fpw-cold.py --point after-progress --configured --live-promotion
```

The configuration oracle checks startup, reload, unchanged policy, session
context rejection and cold reconciliation. After SIGHUP the displayed setting
is desired policy; the WAL writer applies it asynchronously. The configured
promotion cases preserve the frozen child checkpoint policy and then publish a
marker if current configuration differs. Live promotion checks that the new
incarnation retains the exact checkpoint witness when policy is unchanged.
OFF still uses conservative full page images; these tests make no WAL volume
claim. All peers use native PGWL. This does not test C binary WAL interoperability
or run a coordinator, DCS or HA loop.

Run the primary OFF backup oracle on Linux from `src/lisp`, using the pinned
Patroni 4.1.5 environment with psutil and psycopg2, SBCL, C PostgreSQL 20 binaries
and matching libpq development files:

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3
./t/test-ramdisk.sh setup 4G
PG_CONFIG=/path/to/c/bin/pg_config ./t/test-ramdisk.sh run "$PATRONI_PYTHON" t/interop/primary-off-backup.py --reference-bin /path/to/c/bin
```

The C case updates a heap while `pg_basebackup` copies an OFF primary. WAL
inspection shows no full page image for that heap before or after the backup,
but an image during it: C forces images during primary backup even with FPW OFF.
The restored backup contains value 19 while the source advances to 20, and its
checkpoint policy remains OFF. The native case copies a quiescent OFF primary
and checks checkpoint and source-fact metadata, source-offline cold recovery,
local ON promotion and a same-timeline primary restart after SIGKILL. It does
not test a concurrent native copy. Standby backup interval checks are a separate
case from these primary backup checks. The native peers use PGWL; the test runs
no DCS and does not test C binary WAL interoperability.

The independent C idle-writer check can be run from `src/lisp` with:

```sh
"$PATRONI_PYTHON" t/interop/wal-writer-idle-c.py --reference-bin /path/to/c/bin
```

It checks an unassigned read-only transaction's `pg_log_standby_snapshot` WAL
flush before COMMIT. `python3 t/wal-writer-idle.py` separately exercises the
native writer's SIGKILL and fault recovery boundaries with a test-only valid
parameter batch. A synced tail beyond old durable control can be adopted at
startup; the test does not assume all unadvertised WAL is lost.

Using the same pinned environment and C binaries, run the independent feedback
and crash oracles from `src/lisp`:

```sh
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/synchronous-ack.py --reference-bin /path/to/c/bin
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/synchronous-crash.py --reference-bin /path/to/c/bin
```

The C feedback clients simulate apply credit; native followers replay WAL. The
native after-ACK crash case keeps the source down and cold-starts both credited
followers from their own PGDATA to check recovery and the committed row.

`patroni-follow-config.py` constructs the pinned unmodified Patroni 4.1.5
`Postgresql` and `ConfigHandler` and checks the complete attach, replace and
clear writer output with C `postgres -C`. Use the pinned requirements, the C 20
binary directory and a new artifact directory:

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3
"$PATRONI_PYTHON" t/interop/patroni-follow-config.py /path/to/c/bin /tmp/new-follow-config-oracle
```

Pass `--native /path/to/pg-lisp.sh` to compare native metadata as well; the full
unchanged writer comparison now passes. This oracle does not run
`Postgresql.follow`, a receiver, a coordinator or DCS. The separate
`t/patroni-follow.sh` test exercises the real follow method with owned native
peers, without a coordinator or DCS. Its optional `DEBUG_WAL_FAILURE` instrumentation
is scoped to the owned source. `t/libpq-helper.sh` runs the helper wire probe,
which flushes contiguous WAL payload before acknowledging its flush position,
with apply set to zero. The suite Python must provide Linux `os.pidfd_open` and
`psutil.Process.net_connections`; psutil 7.1.3 is suitable.

Run the journaled reseed oracle from `src/lisp` with the pinned Patroni 4.1.5
Python environment, SBCL and native libpq helper build dependencies. Choose a new
output directory.

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/patroni-reseed.py --output /tmp/new-patroni-reseed-oracle
```

The test performs native reseed, activation and retirement. A fresh native child
consumes the retirement receipt and prepares ordinary configuration under PGDATA
ownership before Patroni's constructor and `Postgresql.follow` run. It checks
streaming, standby write rejection, cold restart, source-offline restart and
reconnect. The harness creates the source physical slot through SQL and runs
Patroni's PostgreSQL control layer directly. The output directory retains logs,
process results, elapsed timings and inventories.

Run the configured cancellation replacement oracle with the same dependencies
and a new output directory. Select `pending`, `source-closed` or `cleaned`.

```sh
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/patroni-replacement.py --output /tmp/new-patroni-replacement-oracle --phase pending
```

The test uses native peers and Patroni 4.1.5 `Postgresql.follow`. It checks
cold restart, source-offline restart and reconnect, along with the retained old
history and its fence. The source-closed case retains deferred generation debt.
The pending case also exits a child abruptly after journal publication, then uses
two fresh children to check absent source status, durable cancellation and an
idempotent cancellation retry.
The output retains logs, process results and timings.

The `patroni-reseed.py` default case uses a synthetic target. Run its
former-primary case with another new output directory.

```sh
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/patroni-reseed.py --case former-primary --output /tmp/new-patroni-former-primary-oracle
```

This case builds shared timeline 1 history from an old writer. A local native
base backup constructs a replacement that follows and promotes through real
Patroni methods onto timeline 2. The isolated old writer then commits row 999
past the fork. The test stops it and explicitly performs a journaled remote
reseed of its actual PGDATA from the promoted source. It checks that the old
files and nested directory identities survive in quarantine along with the exact
auxiliary guard, and that the standby can cold restart, restart with the source
offline and reconnect. It also checks a table created only on the child timeline.

Run the Patroni bootstrap cancellation oracle from `src/lisp` with the same
pinned Patroni 4.1.5 environment and build dependencies. Choose a new output
directory. The test uses an initially empty disposable destination.

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3
PG_CONFIG=/path/to/c/bin/pg_config "$PATRONI_PYTHON" t/interop/patroni-bootstrap-cancel.py --output /tmp/new-patroni-bootstrap-cancel-oracle
```

The test pauses the actual `Bootstrap.clone` before streaming through a source
control hook, then calls Patroni's `cancellable.cancel` and checks wrapper exit
130. It checks that the destination remains unpublished and empty, the client
and source staging are cleaned up, and observed descendants are gone. Fresh
Patroni objects then check clone failure with the source offline and successful
clone and follow with it online. The test verifies replay of SQL commits, standby
write rejection with SQLSTATE `25006`, and a cold restart. It runs Patroni's
PostgreSQL control layer directly. The output directory retains logs, process
results and phase elapsed timings in `result.json`.

From `src/lisp`, compare `cluster_name` configuration and byte normalization
against a C executable using Python 3, SBCL and an existing C installation:

```sh
python3 t/interop/cluster-name.py "$PWD" /path/to/c/bin/postgres
```

For the live libpq follow checks, use the pinned Patroni Python environment,
the C binaries and libpq 17 or newer. `PG_CONFIG` selects the matching libpq
headers and library for the helper build:

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3 \
REFERENCE_POSTGRES=/path/to/c/bin/postgres PG_CONFIG=/path/to/c/bin/pg_config \
FOLLOW_CLUSTER_NAME=1 t/libpq-follow.sh
```

The flag adds cluster-name fallback, empty-cluster fallback, SQL context and
reload checks to the follow acceptance run. It observes actual source backend
names through the task-owned control REPL. The wrapper builds a temporary helper.

From `src/lisp`, run the native session quota, reload, restart and raw-wire checks:

```sh
PATRONI_PYTHON=/path/to/pinned-venv/bin/python3 ./t/backend-admission.sh
```

Set `REFERENCE_POSTGRES` to the pinned C executable to run the same driver
against C. The script owns and cleans up its peers; these checks do not establish
a pre-authentication socket or thread limit.

Run `PATRONI_PYTHON=/path/to/pinned-venv/bin/python3 ./t/wal-keep.sh` from
`src/lisp` for native `wal_keep_size` metadata and reload checks. Set
`REFERENCE_POSTGRES` for the C wire comparison. It checks metadata thresholds,
safe size and unlimited caps, not actual C file recycling; native file recycling
is covered by the physical Rove suite. `t/config-output.sh` also compares the
setting's `-C` base-unit output when a C reference is available.

Run `PATRONI_PYTHON=/path/to/pinned-venv/bin/python3 ./t/hot-standby.sh` for
recovery admission, reload, restart, replay and promotion checks. Set
`REFERENCE_POSTGRES` to include the pinned C wire oracle.

## Verification

- Primary Rove: 134 pass; live apply/SQL/wire: 21 pass; native C subscriber: 3 pass.
- ASDF `test-op` also passes, including the generated checks. Strict compilation,
  formatting and all 1,597 legacy checks pass. Local compile/unit logs are in
  `/tmp/pg-test-refactor.j9JIti/`; the task-owned live peers have been stopped.
- Two Function Specs and four Properties each pass 200 trials at seed 42 and
  another fresh seed. Existing declaration digests match on the seed-42 rerun.
- Fresh seeds: name contract `2478105007809290850`, timestamp contract
  `1115415083142307338`, name idempotence `1554083716275278292`, timestamp
  roundtrip `3023390340129460873`, resume floors `1369115907904721316`, identity
  prefix `954190651310676617`.
- Function calls report `properties-not-run, input-coverage-unmeasured`.
  Their separate Property calls cover the selected Properties. Explicit Property
  calls report `rejection-counts-unmeasured, input-coverage-unmeasured`.
  The original `symbol=` selections for name/timestamp reported
  `contract-not-run, rejection-counts-unmeasured, input-coverage-unmeasured`;
  the separate Function Spec calls cover those contracts. These tool-reported
  gaps remain limits of each call; these checks are not exhaustive proofs.
- Luna max reviewed the inventory, Sol high reviewed the complete refactor and
  assertion/query preservation, and Sol medium found no worthwhile further
  helper extraction. C golden bytes and wire comparisons remain independent.

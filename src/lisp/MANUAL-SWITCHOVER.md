# Manual logical writer switchover rehearsal

`t/logical-switchover.sh` exercises a planned writer handoff without a persistent
coordinator. It creates fresh, isolated peers for these directions:

| Direction | Replication path |
| --- | --- |
| Lisp → Lisp | Bundled extension-mode subscriber |
| Lisp → C | Unmodified C native `CREATE SUBSCRIPTION` |
| C → Lisp | Bundled standard-mode subscriber consuming C `pgoutput` |

This is an application-quiesced happy-path experiment. The script owns the only
application writer and both database processes; there is no supervisor that can
restart the retired source. The target accepts application writes only after
the old process has stopped and apply has been detached. It is already an
ordinary writable database: no physical-standby promotion command is involved.

The databases use the existing trusted local test setup. The script does not
establish database-wide authorization or a write fence against arbitrary clients.
It must not be pointed at existing clusters. The fixture has three ordinary
tables, matching precreated schemas, and one ascending sequence with increment
one and the default cache of one. No concurrent schema changes are allowed.

## Run it

From `src/lisp`, Lisp-only needs SBCL, psql and Linux `ss`:

```sh
./t/logical-switchover.sh lisp-to-lisp
```

For either C direction or the complete matrix, supply the same reference build
used for interoperability acceptance:

```sh
C_PG_BINDIR=/path/to/reference/install/bin \
C_PG_REVISION=9c0909b34939f2d5a37b82b9d1efc8b08e310252 \
  ./t/logical-switchover.sh all
```

If needed, set `LD_LIBRARY_PATH` to that installation's `lib` directory.
Directions can be selected individually with `lisp-to-c` and `c-to-lisp`.
`PORT_BASE` defaults to 5850; the target uses the next port. Listener ownership
is checked before SQL readiness probes. Each case uses its own private PGDATA
and artifact directory. Missing prerequisites and failed gates fail the test.

For manual inspection after the handoff, run one direction in a terminal with
`PAUSE_AFTER_SWITCHOVER=1`. The script prints the live target port and endpoint
file, waits for Enter, and then stops the target. `DEBUG_ON_FAILURE=1` similarly
pauses on failure in a terminal, before cleanup; Lisp peers have private control
sockets for inspecting the actual running image with `t/control-client.lisp`.

By default, all remaining peers are stopped when the rehearsal finishes.
Artifacts are preserved; `KEEP_ARTIFACTS=0` removes them after success. Failed
artifacts are always retained. A successful endpoint file therefore describes
the tested target and is not a promise that it is still running after cleanup.

The C build revision is supplied by the operator and printed with the executable
version; the script does not derive a source revision from the binary.

## Operator order and evidence

1. Start replication, finish initial copy and verify READY. A persistent source
   client opens a transaction that changes both data tables. The target must not
   see its uncommitted insert.
2. Stop submitting application work, commit the outstanding transaction, wait
   for its acknowledgement and close that application connection. This is the
   rehearsal's writer-quiescence assumption; source shutdown is the later hard
   fence.
3. Reserve the sequence high-water value and publish it in a final barrier
   transaction. An earlier rolled-back insert consumed an ID, so `MAX(id)`
   cannot stand in for sequence state. Observe the barrier on the target and
   require target progress to advance beyond its pre-barrier value. Lisp target
   rows and source markers commit atomically; C uses flushed replication-origin
   progress. A global publisher WAL head is not used as an apply barrier.
4. Compare canonical rows, column definitions and primary-key metadata for all
   three tables. Stop the source process using fast shutdown, wait for exit,
   and verify its SQL endpoint is unavailable. The routing file is not a fence.
5. Stop the external Lisp subscriber or durably disable the C subscription and
   wait for all its apply workers to stop. Sample settled progress. No apply
   process may write alongside the new application writer.
6. Restart the target while the source remains stopped. Require the same data,
   schema, keys and progress, and verify apply remains detached. This proves
   the final barrier survived a real process restart.
7. Set the target sequence to the recorded high-water value with `setval(...,
   true)`. Publish the target endpoint, allocate the next ID and execute a new
   application transaction with an insert and non-idempotent update.
8. Restart the target again. Verify the new data and next sequence value, the
   unchanged source marker, detached apply and the old source still stopped.

`phase.state` records the last completed step. Canonical snapshots, progress,
sequence floor, writer/server logs and `new-writer.env` remain in the printed
`/tmp/pg-switchover.*` directory. These are rehearsal evidence, not a restartable
authority-transfer protocol. Failure never restarts the old source or performs
automatic failback. After new target writes, the old data directory is retired;
rejoining it requires resynchronization, not simply restarting its publisher or
subscriber command.

The references for these semantics include C `logical/worker.c` (origin end LSN
and flush mapping), `pg_ctl.c` (fast shutdown and waiting), and `sequence.c`
(`setval`/`is_called`). The full C interoperability crash matrix remains a
separate regression.

## Verification

The complete matrix passed on 2026-10-25 against the unmodified `20devel` C
reference at the revision above: Lisp→Lisp, Lisp→C and C→Lisp each reached
`complete`, including both target restarts, and the parent reported
`logical-switchover: all PASS` with exit status zero. An occupied-port negative
check also failed closed without creating fixture tables in the sentinel server.
Shell syntax and whitespace checks pass. Luna max reviewed the checkpoint,
Sol high reviewed the handoff ordering and assertions, and Sol medium reviewed
refactoring and polish; their cleanup/progress and vacuous-assertion findings
were addressed before the final matrix run. No server implementation changed.

## Assessment and next milestones

Assessment date: 2026-10-25. The experiment establishes the supported handoff's
ordering and gives us reusable cross-engine acceptance oracles. It does not
establish physical standby recovery, promotion, synchronous durability, or an
unplanned failover policy. The next implementation track is the missing C-backed
HA primitives and the interfaces required by Patroni. Coordination is out of
scope: no election, DCS/leases, routing automation, distributed cutover journal,
or custom writer-generation service.

Track verified findings and remaining call-site audits in
[the physical HA implementation ledger](PHYSICAL-HA.md).

### 1. Implement C-compatible standby semantics

Start with continuous Lisp→Lisp physical WAL receive, durable flush and replay,
consistent bootstrap and shared cluster identity. Persist recovery state across
restart and reject application writes throughout recovery, including attempts to
turn transaction read-only off. Replay is an internal recovery capability, not a
SQL role-name bypass. Report receive, flush and replay positions independently;
never substitute received bytes for durable or replayed progress.

Implement promotion with timeline/history changes and restart-safe completion.
Reject incompatible identities and divergent history on follow/rejoin. Initially
rebuild/reseed a former primary; stage rewind explicitly. C↔Lisp physical WAL and
disk compatibility is a separate audited requirement from the already working
logical interop, and must not be implied by a Lisp→Lisp test.

C references: `access/transam/xlogrecovery.c`, `access/transam/xlog.c`,
`access/transam/xlogfuncs.c`, `replication/walreceiver.c`,
`replication/walsender.c`, and `bin/pg_basebackup/`.

**Gate:** bootstrap under concurrent writes, stream and replay, restart both
peers, crash abruptly before and after reported durable flush, delay replay
while receiving WAL and verify distinct truthful progress, promote manually,
restart the promoted peer,
reject stale/divergent follow attempts, and rebuild/rejoin. Verify SQL-visible
state and data against independent client evidence. Recovery write exclusion
must cover DML, DDL, TRUNCATE and sequence mutation through existing and new
sessions.

### 2. Implement the actual Patroni backend contract

Pin Patroni and its SQL driver, inventory real call sites and classify each
contract as implemented, partial, missing or explicitly deferred. Cover process
start/stop/restart/reload/status/promote, configuration and recovery settings,
PGDATA/version/PID/control metadata, system identity, timeline, recovery and WAL
SQL functions, replication statistics and slots. Verify native behavior with
interface smoke tests; plausible constants and cosmetic utility wrappers are
not sufficient. Bootstrap and replica-creation hooks can initially use a native
pg-lisp copy path while truthful steady-state recovery remains mandatory.

C PostgreSQL supplies recovery write exclusion and promotion primitives. It does
not supply a general SQL writer-authority generation or an automatic election
service. A logical subscriber is an ordinary writable database, so the rehearsal's
process-stop fence remains a manual prerequisite for that path. Configurable
transaction read-only defaults do not replace physical recovery write exclusion
or external fencing of an unreachable former primary. Custom logical cutover
admission/journaling designs are deferred, not C compatibility prerequisites.

**Gate:** a real pinned Patroni control layer can inspect and control task-owned
instances using truthful interfaces, without running DCS/election or automatic
failover. Keep logical switchover and both C logical-interop directions passing.

### 3. Rehearse unplanned manual failover with an explicit loss policy

A failed source cannot supply the planned final barrier or sequence high-water.
Keep an independent client ledger distinguishing acknowledged commits, rejected
operations and unknown outcomes such as a timeout during COMMIT. Crash the source
with apply deliberately behind, fence it, select a target by its durable complete
prefix and identity, detach apply and grant authority. Compare promoted data with
the client ledger and report acknowledged loss under the chosen asynchronous
policy; never hide it by recopying or relabeling unknown outcomes as rollbacks.

Choose a source-independent ID policy for that experiment, such as disjoint
reserved ranges or a new allocation epoch. A stale target sequence cannot reveal
all allocations made by an unavailable source. Test old-source return, attempted
stale writes, candidate lag and ambiguous commits. Start with one eligible target;
multi-candidate selection and automated promotion follow only after this passes.

Zero acknowledged-commit loss needs a synchronous durability and candidate-selection
contract. Waiting for arbitrary replication receipt is insufficient: commits must
wait for the promised remote durability, and only suitable replicas may take over.
C references are `replication/syncrep.c`, the commit path in `access/transam/xact.c`,
and the WAL receiver/replay paths. Availability when no eligible replica remains
is an explicit policy decision. The current asynchronous logical tests do not
implement this guarantee.

### 4. Future Patroni coordination integration

The current parser accepts logical START_REPLICATION, the replication directory
has logical send/apply workers, and local `wal-recover` replays custom after-image
WAL. `IDENTIFY_SYSTEM` reports timeline 1. These are not a continuous physical
standby, timeline history, or promotion/follow/rejoin implementation.

First build and test Lisp→Lisp receive/flush/replay with shared cluster identity,
recovery write exclusion, promotion to a new history and safe rebuild/follow.
This can initially use pg-lisp's own WAL format. C↔Lisp physical replication
additionally requires PostgreSQL WAL/disk compatibility and remains separate
from the working cross-engine logical migration path.

Pin a Patroni version and SQL driver and inventory their real connection/protocol,
process, configuration, SQL, version/PID/control-file and utility contracts. The
current reference is
[Patroni 4.1.5's PostgreSQL control layer](https://github.com/patroni/patroni/blob/v4.1.5/patroni/postgresql/__init__.py),
which reads recovery/receive/replay state, timeline information and control-data
output. Implement truthful role/progress reporting, start/stop/reload/status and
promotion/follow controls; compatible wrappers must reflect real backend state.
[Custom bootstrap and replica creation](https://patroni.readthedocs.io/en/latest/replica_bootstrap.html)
can defer C-compatible initdb/basebackup, and rebuilding can defer rewind. Those
hooks do not supply the missing steady-state standby behavior.

Pre-DCS interface smoke tests belong to the current backend work and may run
as their prerequisites become available. Future coordination work, outside this
implementation scope: after the backend gates, including manual physical
promotion/rejoin and explicit durability policy, pass, use Patroni/DCS for election and
membership and test loss of its authority path. Patroni normally stops/demotes a
primary when it cannot maintain its leader lock;
[watchdog support](https://patroni.readthedocs.io/en/latest/watchdog.html) addresses
cases where that process-level action cannot finish in time. Test partitions,
paused/crashed managers, delayed shutdown, old-primary return and stale routing.
[Replication-mode policy](https://patroni.readthedocs.io/en/latest/replication_modes.html)
governs durability/availability and eligible candidates; configuring those flags
cannot create synchronous replication in an engine that does not implement it.

**Recommended sequence:** inventory C and Patroni contracts → physical standby
and bootstrap → promotion/history/rejoin → synchronous durability and manual
failover/RPO evidence → Patroni interface smoke tests. These milestones may
advance together where dependencies permit. Patroni/DCS integration and automatic
elections remain future work after the backend acceptance gates pass.

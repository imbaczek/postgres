# Logical replication plan and implementation status

## Goal

Implement a usable logical replication path from one `pg-lisp` master to
multiple `pg-lisp` replicas, including an exported-snapshot copy, a
transactional stream over the PostgreSQL replication protocol, and a durable
subscriber apply worker. The completion regression boots one master and two
replicas, copies a table snapshot, runs a transaction workload on the master,
and verifies both replicas converge.

## C PostgreSQL interoperability

[REPLICATION-COMPATIBILITY.md](REPLICATION-COMPATIBILITY.md) records the
compatibility assessment, observed protocol mistakes, and the implementation
checkpoints. Its acceptance tests are pg-lisp publisher -> unmodified C native
subscriber and C publisher -> pg-lisp subscriber, each with initial copy,
ongoing replication, and restart/reconnect. These are separate one-way tests.
The reference is the C implementation in this checkout (`20devel`), revision
`9c0909b34939f2d5a37b82b9d1efc8b08e310252`. Pg-lisp now advertises that version;
the harness prints both revisions, versions, executable hashes, and observed
protocol versions.

The original milestones below describe the pg-lisp extension path. A separate
standard `pgoutput` sender and `--mode standard` subscriber now implement the
bounded C-peer subset. Ordinary Message framing, native TRUNCATE, N-only UPDATE,
historic publication selection, standard snapshot ownership, and native
discovery SQL/text COPY have independent C-reference coverage. Initial copy,
concurrent handoff, apply/copy worker recovery, lost feedback, and endpoint
restarts pass in both one-way topologies. See the compatibility document for
the final audit and evidence. Physical standby/Patroni support and blue-green
authority transfer remain distinct follow-on work.

## Current state: implemented subset

`backend/storage/wal-core.lisp` stores physical page after-images. Page records can
carry several contributing XIDs, and the commit record makes a page batch
durable before CLOG publishes the transaction. This is enough for redo and WAL
retention, but it cannot recover the SQL row operations that produced those
pages.

The executor still has the logical row values at the right points:
`backend/executor/exec_insert.lisp:exec-insert`,
`backend/executor/exec_delete.lisp:exec-update`, and
`backend/executor/exec_delete.lisp:exec-delete`. `wal-commit-transaction`
serializes transaction commit under `*wal-lock*`; `commit-transaction` only
publishes CLOG after the commit record is flushed. The existing exported
snapshot path captures a WAL boundary while writers are gated and persists a
slot before exposing the snapshot. Extension mode prevents acknowledgment until
its private copy-complete handshake and validates terminal batch positions.
Standard mode follows C snapshot ownership and feedback semantics independently.
Both persist a new restart horizon before publishing it in memory.

The implemented path captures row changes at the executor boundary because
this backend writes page after-images rather than PostgreSQL heap WAL records.
`backend/replication/logical.lisp` stores those changes with their transaction,
validates commit batches, and reads them in commit order under an owned slot.
While a live logical slot exists or a captured snapshot is being handed off to
its slot, catalog DDL is stored in its catalog commit record and
nontransactional sequence state is stored in a standalone catalog event.
Earlier catalog state comes from the slot's exported snapshot.
`backend/replication/protocol.lisp`
handles database-scoped replication startup, slot commands, CopyBoth, pgoutput
row messages, v2 `Message` events, keepalives, and standby status.
`backend/replication/worker.lisp` imports the exported snapshot, bootstraps
supported table/index/sequence definitions, copies allowlisted table rows,
applies committed row changes and supported DDL/sequence events, and persists
target markers with the corresponding durable catalog update.
`backend/replication/client.lisp` is the small wire frontend used by the worker.
Run `t/logical-replication.sh` to boot one source and two independent replicas
and exercise this path.

`backend/replication/standard.lisp` sends standard publication-selected rows
without private DDL, sequence, or progress messages. Selection metadata is
captured with each successful mutation and persists in logical WAL, so later
publication edits cannot reinterpret retained rows. Standard Commit positions
and timestamps come from the durable source transaction. Its bounded sender
tracks safely emitted frontiers separately from subscriber flush feedback.
`standard-subscriber.lisp` uses standard C identity/catalog discovery and a
read-only repeatable-read USE snapshot. It records owned attempt slots before
creation, safely retries interrupted copy, and resumes READY targets from their
durable source marker. Run `t/logical-interop.sh both` with the reference C build
to exercise both independent C-peer topologies.

PostgreSQL's C implementation remains the behavioral reference. In particular,
`decode.c` and `reorderbuffer.c` show how heap changes are decoded and grouped by
XID; `snapbuild.c` establishes the consistent snapshot point;
`logical.c:LogicalConfirmReceivedLocation` persists subscriber progress before
moving the restart horizon; `walsender.c`, `pgoutput.c`, and
`replication/logical/proto.c` define the command, feedback, and row-message
protocol; and `logical/tablesync.c` plus `logical/worker.c` show the initial-copy
and apply-worker lifecycles. The executor-boundary record format and this
worker's limits are deliberate adaptations, not PostgreSQL heap-WAL decoding.
Sequence setup and advancement follow `commands/sequence.c` (`init_params`,
`AlterSequence`, `nextval_internal`, and `SetSequence`). Schema/data bootstrap is
scoped like the schema and data phases in `bin/pg_dump/pg_dump.c`, but only
serializes objects represented by this server's catalog.

The three milestones below are complete for the stated subset. Their numbered
items record the design and its compatibility references; the final section
lists the remaining limits before this can support general-purpose blue-green
upgrades.

## First milestone: durable transaction changes (complete)

1. Add a transaction-local ordered queue to each backend. Capture an insert
   only after the heap/index operation succeeds, a delete only after the tuple
   is successfully marked deleted, and an update as one old/new pair after its
   replacement tuple is installed. An aborted transaction discards its queue.
2. Store each event's database and relation OIDs, relation name, an immutable
   column description (attnum, name, type, typmod, nullability), and full old
   and/or new row values. Complete old/new images preserve the executor values;
   the subscriber still requires a usable unique replica identity for applying
   UPDATE/DELETE. Schema is attached to each event so later catalog changes
   cannot reinterpret an earlier row image. Persist the slot's database OID as
   well, and reject a reader opened against another database; existing legacy
   physical slot files remain readable for physical retention, but a logical
   reader must reject a database-unbound slot.
3. Add a versioned logical-change WAL record to the existing commit batch:
   batch start, page images, logical changes for that XID, then the commit
   terminal. Split larger transactions into ordered change-record chunks, but
   expose none until the commit terminal validates the full chunk sequence.
   Bound ordinary in-memory transactions at 64 MiB and fail the statement
   cleanly if they exceed the limit. Subscriber apply transactions may use a
   further 1 MiB for the durable progress update; their encoded WAL/recovery
   limit is 65 MiB. Copy validation requires matching source and target
   columns, typmods, and replica-identity columns so replay does not expand
   transaction metadata beyond that reserve. Reject malformed records and
   mismatched XIDs during recovery and decoding. A batch without its terminal
   remains invisible, just like an incomplete physical page batch.
4. Add a slot reader that becomes available after initial-copy acknowledgment
   and scans from `confirmed_flush_lsn`, groups row changes by XID, and
   returns only transactions whose commit terminal is after that point.
   `restart_lsn` remains the retention horizon for physical WAL. Capture the
   durable WAL end while holding
   `*wal-lock*`, scan only to that bound, and use the same lock to exclude
   checkpoint recycling for the scan. A reader session claims the slot's
   runtime owner until its read/apply/ack callback exits, blocking a second
   reader or a concurrent drop. This first design writes row changes only in
   the commit batch, so its reader can begin at `confirmed_flush_lsn`;
   `restart_lsn` may remain earlier to retain physical page history for active
   XIDs. Bound each read to 128 transactions, 8 MiB of encoded row changes,
   and 16 MiB of WAL scanned by default, stopping only after a terminal batch.
   A first transaction may exceed the row-change budget up to the 64 MiB
   transaction cap, so callers can always make progress. Return a safe progress
   LSN alongside the transactions (the starting confirmed position if no scan
   was needed), even when a bounded scan finds no rows for this database; after
   making those row changes durable,
   callers acknowledge that LSN and request the next batch. The scan budget can
   extend through the terminal record of one in-progress batch before it stops.
   This bounds decoded output and scan time under `*wal-lock*` by the configured
   budgets plus at most one complete batch. Emit groups in commit-record order,
   not page order. Keep the existing terminal-LSN acknowledgment path; a
   logical reader may acknowledge only while it owns the slot, no farther than
   the highest progress LSN returned in that owner session, and only after the
   downstream transaction is durable. Add a dedicated initial-copy finalizer
   that requires the exact active slot owner and the consistent LSN; do not
   permit a general low-level acknowledgment of an unconfirmed slot after its
   owner has been released. Both the in-process exported-snapshot helper and
   the wire protocol use that owner-checked finalizer after the snapshot
   consumer finishes and before releasing the runtime slot owner. Keep the
   snapshot registration live until all imports and copy reads are done.
5. Use the existing exported snapshot contract for initial copy. The copy is
   read from its captured snapshot, then the slot is acknowledged at the
   consistent boundary, then the reader emits later commits. Tests must cover
   transactions active at the boundary and transactions that commit while the
   copy is in progress.

## Second milestone: replication protocol (complete)

1. Recognize the PostgreSQL startup parameter `replication=database` and keep
   replication connections out of ordinary SQL transaction dispatch. Add
   strict handlers for `CREATE_REPLICATION_SLOT ... LOGICAL pg_lisp
   EXPORT_SNAPSHOT`, `DROP_REPLICATION_SLOT`, and `START_REPLICATION SLOT ...
   LOGICAL ...`. Validate the database, plugin, slot state, and requested LSN
   before entering streaming mode. Follow `tcop/postgres.c`'s walsender query
   dispatch and `replication/walsender.c:exec_replication_command`, plus
   `src/test/subscription/t/004_sync.pl` slot setup behavior.
2. Make slot creation return the consistent LSN and an opaque exported-snapshot
   token. Persist an `initial-copy-pending` slot state and retain the registered
   snapshot with that state, but release the runtime slot owner before returning
   the token so normal source connections can import it. The persistent state,
   rather than the runtime owner, prevents ordinary acknowledgment or streaming
   from passing the copy boundary. Add `SET TRANSACTION SNAPSHOT` for a
   read-only REPEATABLE READ transaction by importing an independent registered
   copy, following `utils/time/snapmgr.c:ExportSnapshot` and `ImportSnapshot`.
   The token is process-local: if the source restarts before copy completion,
   reject the expired token and require the worker to discard its partial copy,
   drop the still-unconfirmed slot, and create a new slot and snapshot.
3. Enter PostgreSQL `CopyBoth` mode for `START_REPLICATION`. Encode row changes
   as pgoutput `Begin`, `Relation`, `Insert`, `Update`, `Delete`, and `Commit`
   messages carried by `XLogData`. Encode supported transactional DDL and
   standalone sequence-state changes as pgoutput v2 `Message` records. Relation
   metadata marks unique replica-identity columns; UPDATE/DELETE use a `K` key
   tuple when present and an `O` full old tuple otherwise. The worker requires
   `K` identity for its configured tables. The message payload is a private
   pg-lisp event envelope, so only the bundled subscriber can apply it. Current
   transactional `M` framing also prepends XID outside streamed blocks; this
   shared sender/receiver mistake must be corrected under the compatibility
   plan, not treated as a PostgreSQL v2 requirement. Send
   keepalives for safe progress
   across batches with no rows for this database.
   For the initial connection, wait for the client's first standby status update
   at exactly the consistent LSN. This is the explicit copy-complete handshake:
   only the active streaming owner may perform this one-time boundary
   acknowledgment, change the slot from `initial-copy-pending`, and release the
   exported snapshot registration. Implement this with the owner-checked
   initial-copy finalizer, not the existing low-level initial-copy exception.
   For a resumed connection, validate the requested LSN as a terminal WAL
   position between
   the slot's confirmed LSN and durable WAL end; the first status update may
   confirm exactly that requested position as a resumption exception to the
   normal sent-progress rule. Then read and acknowledge only progress sent to
   that owner. Keepalives carry progress across batches with no rows, including
   rows for relations excluded by the worker's table allowlist. Compare message
   framing and feedback with `walsender.c`, `pgoutput.c`,
   `replication/logical/proto.c:logicalrep_write_*`, and
   `src/test/subscription/t/015_stream.pl`.

## Third milestone: subscriber apply worker and multi-replica regression (complete)

1. Add a small SBCL PostgreSQL frontend client for startup, simple-query result
   frames, and replication `CopyBoth`/`CopyData` frames. Use it for the worker's
   source SQL, source replication, and target SQL connections without an
   external driver. Add a foreground `pg-lisp-subscribe` worker with an explicit
   table allowlist. It opens an ordinary source connection to import the
   exported snapshot and holds the imported transaction for the copy. A schema
   read lease keeps source DDL beyond the slot boundary out of the live catalog
   bootstrap; those DDL commits stream after the copy. The worker creates the
   supported source table, standalone btree index, and sequence definitions on
   the target, validates matching definitions, and copies selected table rows
    in chunks targeting 8 MiB of estimated row data. A larger row is copied in
    its own chunk; source and target protocol frames and individual logical WAL
    events still have separate size limits. Sequence state is captured
   alongside the slot boundary because it is not MVCC-versioned. Target chunks
   commit independently, while a `copying` state and pending snapshot token in
   the target progress table prevent partial copies from being treated as ready.
   After every selected table is copied, commit the consistent LSN and `ready`
   state. Persist the source endpoint, publisher incarnation, database, and
   canonical table allowlist beside progress, and reject a resume whose source
   or allowlist differs. On retry with a live token, reset selected tables with
   `TRUNCATE TABLE` and repeat the copy from the same snapshot. If the token
   expired after a source restart, reset selected tables, drop the unconfirmed
   slot, and start a new snapshot and copy.
2. Apply each `pgoutput` transaction to the target inside one target
   transaction. Persist its source commit LSN in the same target transaction
   as its row changes, and send feedback only after that commit succeeds. Also
   persist progress when a batch or transaction advances the source LSN but
   contains no rows for the allowlist, before sending feedback. Skip row effects
   for relations outside the allowlist while still recording their transaction
   and commit progress. On reconnect, `START_REPLICATION` uses the durable
   target marker: require it to be a terminal LSN at or after the slot's
   confirmed LSN and no later than durable WAL; the client reports it in its
   first CopyBoth status update, then the publisher confirms it and reads from
   there. Reserve target transaction capture headroom for the progress-row
   update in addition to the source row changes. The target uses the same
   schema and identity metadata, and the 1 MiB apply reserve covers its
   progress-row event. A source transaction within the 64 MiB source cap must
   still be persistable atomically with its target marker under the 65 MiB
   target WAL cap. This validated startup exception avoids replaying a
   transaction committed at the target before its previous feedback was lost.
   A target marker behind the source's confirmed LSN requires reseeding.
   Require each configured table to expose an unambiguous unique replica
   identity: a primary key, an all-non-null unique key, or a unique key whose
   index uses `NULLS NOT DISTINCT`. Ordinary nullable `UNIQUE` indexes are
   unsafe because multiple NULL keys can match. Set `pgoutput` key flags from
   the selected identity, require the worker to receive a `K` key tuple, and
   use those columns for UPDATE/DELETE so each target statement changes exactly
   one row.
  3. `t/logical-replication.sh` boots three isolated `pg-lisp` servers and starts
     an independent slot and worker for each replica. It waits until both
     exported snapshots have been imported, commits row and sequence changes
     during the paused copy, and launches source DDL while the schema lease is
     held. It checks the copied state plus later stream results. Its workload covers a
     multi-row insert, several changes in one transaction, nullable values,
     `NULLS NOT DISTINCT` key update/delete, ordinary update/delete, and an
     aborted transaction, DDL during initial copy, supported ALTER TABLE/index
     changes, and sequence bootstrap and stream state with direction changes.
     It checks that a transaction for an excluded table advances each target's
     durable marker and that supported source DDL is applied. DDL and sequence
     events are database-wide; the `--table` allowlist filters row copy and row
     changes only.
     It then disconnects one worker after the target commit but before feedback,
     writes more source data while that worker is stopped, restarts it from its
     target marker, and compares both replicas with the source. The other worker
     continues streaming throughout. The test uses committed table results as
     workload barriers, captures a source LSN barrier with
     `pg_current_wal_lsn()`, and asserts both durable target markers reach it,
     including after the restarted worker resumes. It also checks that changing
     the source endpoint or table allowlist is rejected. Its C regression
      references are `src/test/subscription/t/001_rep_changes.pl`, `004_sync.pl`,
      and `035_conflicts.pl`.

### Follow-up checks (complete)

`t/logical-replication.sh` injects SIGKILL after a committed initial-copy
chunk, after one target row change inside an uncommitted apply transaction, and
after target COMMIT but before publisher feedback. It verifies that copy remains
in `copying`, partial row changes and their marker roll back together, and a
durable target commit resumes without applying the transaction twice. Its
stress phase applies a 300-row transaction while one subscriber is offline,
runs two concurrent source writers, checks that the inactive slot retains WAL,
and exercises a second reconnect. It also runs concurrent DDL and verifies that
the database-wide schema contract converges after the offline subscriber
reconnects. These checks compare the target commit and feedback order in
`logical/worker.c`, and the schema ordering with `commands/tablecmds.c`.

`t/logical-replication-faults.sh` kills the publisher after copy-slot
finalization and on both sides of exported snapshot release, then restarts the
same PGDATA and resumes from the target marker. It also kills the publisher at
slot-file boundaries before rename, after rename, and after directory fsync;
restart must load a valid slot image or safely replace an abandoned temporary
file. These are process-crash tests, not power-loss tests. The ordering follows
`snapbuild.c`, `snapmgr.c`, `walsender.c`, and `slot.c:ReplicationSlotPersist`.

`t/logical-catalog-durability-faults.sh` injects an error after the catalog WAL
terminal for `CREATE TABLE`, `ALTER TABLE ... UNIQUE`, and `CREATE SEQUENCE`.
The SQL call reports the injected error, while the durable catalog mutation
remains visible. This keeps the publisher catalog aligned with the event stream
and exercises the distinction between pre-durable rollback and post-durable
failure in `commands/tablecmds.c`, `commands/sequence.c`, and the Lisp catalog
WAL terminal.

`t/logical-standalone-event-faults.sh` crashes sequence-state application after
the target marker commits but before feedback. Restart verifies that the marker
and sequence state are atomic and that the publisher is not acknowledged early,
following `logical/worker.c`'s apply-then-feedback order.

`t/logical-slot-backpressure.sh` keeps fast and slow subscribers at different
LSNs while bounded and oversized WAL intervals accumulate. It checks that
checkpoint retains required WAL below the cap, persists slot loss above the
cap, terminates an importer and a publisher blocked on a small socket buffer,
rolls back the blocked target transaction, and allows both reconnect and fresh
slot reseed. The workload includes a 1,000-row fast apply, concurrent writers,
and a 300-row blocked apply with eight supported `varchar(126)` payloads per
row. It also records RSS for both worker states and exercises repeated
connection attempts.

The schema contract is deliberately strict for this implemented catalog
subset: table, standalone-index, and sequence definitions must match exactly;
sequence values are excluded. A drifted subscriber rejects the DDL event before
mutating its catalog or advancing the durable source marker. No implicit type
conversion is attempted. This is narrower than PostgreSQL's schema-sync
compatibility rules in `src/test/subscription/t/008_diff_schema.pl`.

## Invariants and limits

- A subscriber never sees an uncommitted or aborted transaction.
- All row events from a committed transaction are returned together and in
  executor order; transactions are returned in durable commit order.
- Acknowledgment cannot move backward, pass durable WAL, or name a nonterminal
  record. A failed slot-file fsync leaves the old in-memory restart pin in
  force.
- The initial copy and stream share one boundary. If an XID is active at that
  boundary, its snapshot-invisible changes are emitted in the later commit
  batch, after the copy boundary.
- The supported row model is limited to complete rows with the current scalar
  types (`int4`, `int8`, and the implemented bounded `varchar`).
  Initial bootstrap creates and validates supported `public` schema tables,
  standalone btree indexes, and integer sequences. The explicit `--table`
  allowlist controls which table rows are copied and which row changes are
  applied; supported DDL and sequence-definition events are database-wide.
  The allowlist is persisted in subscription configuration, so changing it
  requires a reseed. Supported DDL events (`CREATE`/`DROP TABLE`, supported btree index
  and table-constraint changes, `CREATE`/`ALTER`/`DROP SEQUENCE`) and sequence
  state are applied database-wide, independent of that allowlist. `TRUNCATE`
  changes are carried transactionally for selected tables. Each catalog event
  must fit within one WAL segment. DDL in explicit
  transaction blocks is rejected by the SQL server. Sequence support covers
  int8 increment/start/min/max bounds, restart, and `nextval`/`currval`/`setval`;
  sequence values remain nontransactional as in `sequence.c`.
- The worker does not implement PostgreSQL publication selection or row filters;
  publication catalog objects do not control this stream. It does not track
  replication origins or prevent loops in cascaded replication. Arbitrary DDL,
  unsupported types/storage, large objects, TOAST values beyond the supported
  type and replication size limits, partition routing,
  two-phase transactions, and conflict resolution are outside the implemented
  subset. These remain follow-on work before claiming general-purpose blue-green
  upgrade support.
- DDL compatibility uses a versioned exact schema contract for the implemented
  table, index, and sequence definitions. Existing target objects are checked
  before bootstrap or row reset; each streamed DDL checks the expected schema
  before and after execution. Sequence values are not schema. Drift halts that
  worker until the target is repaired; no implicit casts or column-order
  changes are attempted.
- Initial-copy reset uses target `TRUNCATE TABLE`; source `TRUNCATE` is emitted
  transactionally for the selected tables. The source and target must be
  `pg-lisp` servers using this worker's private v2 `Message` payload for DDL and
  sequence events; PostgreSQL's stock subscriber cannot apply those messages.
- Each logical read returns a bounded batch rather than materializing all WAL
  retained by a lagging slot. PostgreSQL's `reorderbuffer.c` can serialize
  pending transactions to disk; this milestone instead reads already durable
  transaction records in bounded commit-order batches. A read's progress LSN
  may advance across terminal batches with no rows for the slot's database, so
  a quiet database can release global WAL retained by its slot.
- This milestone serializes row changes for every write transaction, even when
  no slot exists, because a transaction active at a later snapshot boundary
  must still be available when it commits. Ordinary DML transactions are
  limited to 64 MiB; subscriber apply transactions have a 1 MiB reserve for the
  target progress update, with recovery and WAL readers accepting up to 65 MiB.
  These encoded-byte caps do not bound total process RSS: decoded Lisp objects
  and full catalog images add overhead. A lagging subscriber can retain
  additional WAL until its durable feedback advances the slot.

## Follow-on priorities

### Fault injection

Keep the target `COMMIT` to publisher-acknowledgment interval as the primary
recovery boundary. The current regressions also interrupt row application,
initial-copy chunk commits, slot finalization, snapshot release, standalone
event application, and slot-file publication. Add a failpoint whenever a new
durable transition is introduced, and assert both durable progress and WAL
retention after restart. The snapshot finalizer unit test injects an error
after it claims ownership and verifies that the MVCC pin and schema lease are
still released exactly once. These boundaries correspond to `snapbuild.c`,
`snapmgr.c`, `slot.c`, and `logical/worker.c`. Cleanup regressions now also
interrupt DROP after its slot and export ownership transfer, retry a lost-slot
cleanup after its original collector is gone, and interrupt importer shutdown
between two backends. Each importer receives at most one termination request,
so retry cannot deliver a second asynchronous abort while the first cleanup is
unwinding.

### Schema compatibility

The implemented policy remains strict: relation definitions, standalone index
definitions, and sequence definitions must match exactly before and after each
DDL event. The subscriber stops on drift; it does not guess at type conversions,
column reordering, or missing-column defaults. A future compatibility policy
should be explicit and versioned, validate every event before mutation, and
define which changes are safe for the currently implemented scalar types and
catalog objects. Until that policy and its upgrade tests exist, repair the
target schema or reseed it.

The current v1 contract compares these catalog fields:

| Operation | Strict compatibility check | Replicated state |
| --- | --- | --- |
| `CREATE TABLE` / `DROP TABLE` | Name; ordered column names, type OIDs, typmods, and nullability; `int4`, `int8`, and implemented bounded `varchar` (typmod through 126); supported `nextval(sequence)` defaults | Table rows |
| `ALTER TABLE` constraint changes | Supported named PRIMARY KEY and UNIQUE definitions; ordered constrained columns and `NULLS NOT DISTINCT` | Constraint/index contents and replica identity |
| `CREATE INDEX` / `DROP INDEX` / `ALTER INDEX ... RENAME` | Name, table, ordered columns, btree method, uniqueness, and `NULLS NOT DISTINCT` | Index contents |
| pgoutput replica identity | Ordered source key attnums and names from Relation metadata; target identity signature must match; selected UPDATE/DELETE requires the key tuple | Key values in row changes |
| `TRUNCATE` | Transactional list of source relations; the `--table` allowlist limits which target tables are truncated; table shape follows the strict table contract | Selected table rows |
| `CREATE SEQUENCE` / `ALTER SEQUENCE` / `DROP SEQUENCE` | Name, int8 increment, minimum, maximum, and start value | Sequence definition |
| Sequence-state event | Source and target sequence definitions must match | Last value and `is_called`, committed with the applied-event marker |

Before bootstrap or a copy retry resets target rows, every existing target
object must have a matching publisher definition. Bootstrap then creates
missing publisher objects and requires full schema equality. For each streamed
DDL event, the worker checks the expected schema under the schema lock before
execution and checks the resulting contract before committing progress.
Sequence-state events validate the source sequence definition and update only
its value and `is_called` flag. Any mismatch leaves the target catalog, target
progress marker, and publisher acknowledgment at their prior durable boundary.

The integration matrix covers compatible bootstrap, mismatched target table
and sequence definitions before initial-copy reset, DDL drift after a ready
subscription, concurrent DDL during copy, and an offline subscriber catching
up after DDL. These cases use the supported scalar/catalog objects above; they
do not introduce conversions or behavior for unimplemented types or storage.

### Resource limits and backpressure

Continue stress coverage with large transactions, concurrent writers, a slow
subscriber, repeated reconnects, and checkpoints. Verify that each slot retains
all WAL needed to replay through its durable target marker, while a fast slot
can advance independently. The current tests exercise the configured retention
cap and a blocked publisher socket; transaction payloads remain bounded by the
64 MiB logical-change limit and the 1 MiB apply reserve. Decoded objects and
catalog images still add process-memory overhead, so RSS and latency should be
measured as stress sizes increase. The 300-row blocked-apply case uses eight
`varchar(126)` payloads per row to exceed the socket's buffered window and
samples the subscriber's RSS, failing above the configurable 256 MiB default
(`BLOCKED_WORKER_RSS_MAX_KB`); the fast-consumer workload continues to report
its high-water mark and concurrent-writer latency.

## Follow-on milestone: cutover after protocol compatibility

The [manual switchover rehearsal](MANUAL-SWITCHOVER.md) now exercises the happy
path with isolated peers, one controlled application writer and a process-stop
source fence. It is an experimental subset of the general requirements below;
it does not supply arbitrary-client fencing or restartable authority transfer.
Its [next-step assessment](MANUAL-SWITCHOVER.md#assessment-and-next-milestones)
prioritizes durable write authority, restartable cutover and a manual failover/RPO
matrix, with physical follower/Patroni integration as a parallel track.

After the [C-peer compatibility milestone](REPLICATION-COMPATIBILITY.md), add
blue-green cutover: verify complete data coverage and one apply owner, fence
old-primary writes, drain transactions, establish a final durable barrier,
wait for target durability, validate schema/data/sequences, then transfer write
authority and routing. Recover interrupted transfers without two writers.
Cross-build logical compatibility and forward-only post-cutover recovery must
be explicit. General-purpose upgrade safety also requires broader schema/type/
storage coverage, conflict handling, and loop prevention. Patroni-compatible
physical standby behavior is a separate implementation milestone.

## Historical validation of the pg-lisp subset

Previously reported validation (not rerun for the compatibility assessment):

- `./t/format.sh --check` for all changed Lisp files
- `./t/compile.sh`
- CL-MCP unit suite — 1,554 checks, 0 failures
- `./t/logical-replication.sh` — source plus two independent subscribers,
  copy-time DML and DDL, concurrent DDL, sequence state and bounds,
  lost-feedback restart, schema drift rejection, WAL retention, and repeated
  reconnect checks.
- `./t/logical-replication-faults.sh` — snapshot release, slot finalization,
  slot-file crash stages, same-PGDATA restart, and resumed LSN checks.
- `./t/logical-catalog-durability-faults.sh` — post-terminal injected errors
  preserve durable table, constraint, and sequence DDL.
- `./t/logical-standalone-event-faults.sh` — standalone sequence state and
  target-progress durability before publisher feedback.
- `./t/logical-slot-backpressure.sh` — retention caps, slow and fast consumers,
  blocked sender termination, and slot reseeding.


The tests exercise transaction visibility, snapshot handoff, and acknowledgment
ordering within the pg-lisp subset, with C source as a design reference. They
do not establish C-peer interoperability; some codec expectations share the
implementation's framing mistake. Independent C-backed fixtures and both live
replication directions are required by the next milestone.

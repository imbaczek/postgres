# Logical replication plan and implementation status

## Goal

Implement a usable logical replication path from one `pg-lisp` master to
multiple `pg-lisp` replicas, including an exported-snapshot copy, a
transactional stream over the PostgreSQL replication protocol, and a durable
subscriber apply worker. The completion regression boots one master and two
replicas, copies a table snapshot, runs a transaction workload on the master,
and verifies both replicas converge.

## Current state: implemented subset

`backend/storage/wal.lisp` stores physical page after-images. Page records can
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
snapshot path captures a WAL boundary while writers are gated, persists a slot
before exposing the snapshot, and prevents acknowledgment until the copy has
finished. Slot acknowledgment already validates a terminal batch and persists
the new restart horizon before publishing it in memory.

The implemented path captures row changes at the executor boundary because
this backend writes page after-images rather than PostgreSQL heap WAL records.
`backend/replication/logical.lisp` stores those changes with their transaction,
validates their commit batches, and reads them in commit order under an owned
slot. `backend/replication/protocol.lisp` handles database-scoped replication
startup, slot commands, CopyBoth, pgoutput v1, keepalives, and standby status.
`backend/replication/worker.lisp` imports the exported snapshot, copies
allowlisted tables, applies committed row changes, and persists each target
marker with its row transaction. `backend/replication/client.lisp` is the small
wire frontend used by the worker. Run `t/logical-replication.sh` to boot one
source and two independent replicas and exercise this path.

PostgreSQL's C implementation remains the behavioral reference. In particular,
`decode.c` and `reorderbuffer.c` show how heap changes are decoded and grouped by
XID; `snapbuild.c` establishes the consistent snapshot point;
`logical.c:LogicalConfirmReceivedLocation` persists subscriber progress before
moving the restart horizon; `walsender.c`, `pgoutput.c`, and
`replication/logical/proto.c` define the command, feedback, and row-message
protocol; and `logical/tablesync.c` plus `logical/worker.c` show the initial-copy
and apply-worker lifecycles. The executor-boundary record format and this
worker's limits are deliberate adaptations, not PostgreSQL heap-WAL decoding.

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
   strict handlers for `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput
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
3. Enter PostgreSQL `CopyBoth` mode for `START_REPLICATION`. Encode the
   supported logical subset as `pgoutput` v1 `Begin`, `Relation`, `Insert`,
   `Update`, `Delete`, and `Commit` messages carried by `XLogData`. Relation
   metadata marks unique replica-identity columns; UPDATE/DELETE use a `K` key
   tuple when present and an `O` full old tuple otherwise. The worker requires
   `K` identity for its configured tables. Send keepalives for safe progress
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
   exported snapshot,
   copies those compatible tables to an already-created target schema in
   chunks capped at 8 MiB of logical row changes. Keep the imported snapshot
   transaction open on the source for the entire copy, while committing bounded
   chunks independently on the target. Before copying, persist a `copying`
   state and the pending snapshot token in an internal target progress table
   keyed by source slot. Partial target chunks are never treated as complete.
   After every table is copied,
   commit the consistent LSN and `ready` state in the target progress table.
    Persist the source endpoint/database and canonical table allowlist beside
    the progress row, and reject a resumed worker whose configuration differs.
   On retry with a live token, reset the allowlisted tables using supported
   `TRUNCATE TABLE` and repeat the copy from the same snapshot. Add the needed
   parser and executor support for `TRUNCATE`; clearing with row-by-row DELETE
   can exceed the logical transaction size bound. If the worker crashes before
   its `copying` row is durable, it drops and recreates the unconfirmed slot.
   If the token expired after a source restart, it resets the target tables,
   drops the still-unconfirmed slot, and creates a new slot and copy. Keep
    source DDL and target table creation outside this milestone; require
    matching columns, types, typmods, and replica-identity columns, and validate
    relation metadata again before applying rows.
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
     exported snapshots have been imported, commits writes during the copy, and
     checks the copied state plus later stream results. Its workload covers a
     multi-row insert, several changes in one transaction, nullable values,
     `NULLS NOT DISTINCT` key update/delete, ordinary update/delete, and an
     aborted transaction. It checks a filtered-table transaction and a source
     DDL commit with no logical row event advance each target's durable marker.
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
- The supported row model is limited to the current scalar types (`int4`,
  `int8`, `varchar`) and complete rows. The end-to-end worker copies an
  explicit table allowlist from the `public` schema into a pre-created target
  schema with matching columns, types, typmods, and identity columns. It
  applies source changes from all relations and filters rows at the subscriber;
  it does not implement publications or row filters. Target `TRUNCATE` resets
  an initial copy; source `TRUNCATE` is not emitted as a logical change. It
  also does not track replication origins or prevent loops in cascaded
  replication. It does not replicate arbitrary DDL, sequence state, large
  objects/TOAST values, partition routing, two-phase transactions, or
  conflicts. These remain follow-on work before claiming general-purpose
  blue-green upgrade support.
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

## Follow-on milestone

Add blue-green cutover guidance: prepare matching tables/indexes, copy under
the exported snapshot, wait for replay to reach a source commit barrier, fence
old-primary writes, validate, then promote the subscriber. Sequence state and
DDL need explicit support before claiming general-purpose upgrade safety.

## Validation

Validation completed:

- `./t/format.sh --check` for the changed Lisp files
- `./t/compile.sh`
- `./t/run-unit-tests.sh` — 1,333 checks, 0 failures
- Server-backed shell regressions: 19 passed, including
  `./t/logical-replication.sh` — source plus two independent subscribers,
  lost-feedback restart, config mismatch rejection, and source-LSN barrier
  checks. `spill-oom-cgroup.sh` skipped because this environment uses a shared
  cgroup.

The tests compare transaction visibility, snapshot handoff, pgoutput framing,
and acknowledgment ordering with the PostgreSQL C references above.

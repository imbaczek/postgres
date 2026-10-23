# Logical change stream plan

## Goal

Build the transactional source stream needed for logical replication and
blue-green upgrades in `pg-lisp`. The first implementation milestone adds
durable, per-transaction row changes and a slot reader. A subscriber transport
and apply worker are later milestones; this document does not claim that the
first milestone is end-to-end replication.

## Current state

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

PostgreSQL's C implementation is the behavioral reference: `decode.c`
decodes heap WAL records into row changes, `reorderbuffer.c` queues changes by
XID and emits them from `ReorderBufferCommit`, `snapbuild.c` establishes the
consistent snapshot point, and `logical.c:LogicalConfirmReceivedLocation`
persists subscriber progress before moving the effective restart horizon.
`pgoutput.c` defines the relation and transaction framing a future wire
protocol can use. Because this backend currently writes page after-images
rather than PostgreSQL heap WAL records, the first milestone records row
changes at the executor boundary instead of attempting to reverse-engineer
them from physical pages.

## First milestone: durable transaction changes

1. Add a transaction-local ordered queue to each backend. Capture an insert
   only after the heap/index operation succeeds, a delete only after the tuple
   is successfully marked deleted, and an update as one old/new pair after its
   replacement tuple is installed. An aborted transaction discards its queue.
2. Store each event's database and relation OIDs, relation name, an immutable
   column description (attnum, name, type, typmod, nullability), and full old
   and/or new row values. Full old rows provide `REPLICA IDENTITY FULL`-style
   identity in this subset, including tables without a primary key. Schema is
   attached to each event so later catalog changes cannot reinterpret an
   earlier row image. Persist the slot's database OID as well, and reject a
   reader opened against another database; existing legacy physical slot files
   remain readable for physical retention, but a logical reader must reject a
   database-unbound slot.
3. Add a versioned logical-change WAL record to the existing commit batch:
   batch start, page images, logical changes for that XID, then the commit
   terminal. Split larger transactions into ordered change-record chunks, but
   expose none until the commit terminal validates the full chunk sequence.
   Bound the first implementation's in-memory per-transaction queue (64 MiB)
   and fail the statement cleanly if it exceeds the limit. Reject malformed
   records and mismatched XIDs during recovery and decoding. A batch without
   its terminal remains invisible, just like an incomplete physical page
   batch.
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
   downstream transaction is durable.
   The exported-snapshot cleanup's one-time initial-copy acknowledgment remains
   a separate exception, performed after it releases the snapshot owner.
5. Use the existing exported snapshot contract for initial copy. The copy is
   read from its captured snapshot, then the slot is acknowledged at the
   consistent boundary, then the reader emits later commits. Tests must cover
   transactions active at the boundary and transactions that commit while the
   copy is in progress.

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
- The first milestone supports only the currently implemented scalar types
  (`int4`, `int8`, `varchar`) and complete rows. It does not implement
  publication filtering, DDL replication, large-object/TOAST handling,
  two-phase transactions, replication protocol framing, or a remote apply
  worker. The schema image makes row changes self-describing; it does not make
  arbitrary source DDL safe on a subscriber.
- Each logical read returns a bounded batch rather than materializing all WAL
  retained by a lagging slot. PostgreSQL's `reorderbuffer.c` can serialize
  pending transactions to disk; this milestone instead reads already durable
  transaction records in bounded commit-order batches. A read's progress LSN
  may advance across terminal batches with no rows for the slot's database, so
  a quiet database can release global WAL retained by its slot.
- This milestone serializes row changes for every write transaction, even when
  no slot exists, because a transaction active at a later snapshot boundary
  must still be available when it commits. The 64 MiB in-memory logical-change
  limit therefore applies to ordinary DML transactions until capture can be
  narrowed without losing active-at-boundary transactions.

## Follow-on milestones

1. Expose the stream with PostgreSQL logical replication protocol commands and
   feedback, following `replication/walsender.c:StartLogicalReplication`,
   `XLogSendLogical`, and `ProcessStandbyReplyMessage`.
2. Add a subscriber apply worker that frames each source transaction as one
   destination transaction and sends feedback only after the destination commit
   is durable. Track the last applied source LSN durably so a reconnect between
   apply and feedback does not apply a transaction twice.
3. Add schema compatibility and blue-green cutover guidance: prepare matching
   tables/indexes, copy under the exported snapshot, wait for replay to reach a
   source commit barrier, fence old-primary writes, validate, then promote the
   subscriber. Sequence state and DDL need explicit support before claiming
   general-purpose upgrade safety.

## Validation

Add unit and crash-recovery coverage for change encoding, malformed payloads,
commit grouping/order, rollback, multi-statement transactions, update old/new
images, slot restart and acknowledgment after restart, incomplete WAL tails,
and snapshot/stream handoff with an XID active across the boundary. Compare
transaction visibility and acknowledgment behavior with the C references above.
Run the Lisp compile and test scripts documented in `AGENTS.md` and
`README.md` before committing.

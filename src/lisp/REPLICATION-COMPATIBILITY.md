# Logical replication compatibility with C PostgreSQL

Status: complete for the initial supported contract below. Both isolated C-peer
topologies pass initial copy, live replication, recovery and durable published
barriers. Final acceptance audit and complete-diff review are complete.
Assessment baseline: repository commit
`9c0909b34939f2d5a37b82b9d1efc8b08e310252` (2026-10-24).

## Milestone and reference version

Make the supported logical replication subset interoperate in both directions:

1. pg-lisp publisher -> unmodified C PostgreSQL native subscriber.
2. Unmodified C PostgreSQL publisher -> pg-lisp subscriber.

These are separate one-way test topologies, not bidirectional replication of
the same data. Neither test may translate or repair replication messages in an
intermediary. The C subscriber must use its built-in subscription/apply workers;
the pg-lisp subscriber may use our own worker, consuming standard `pgoutput`.

Use C PostgreSQL built from this repository as the first reference peer. The
current C tree identifies itself as `20devel` in `../../configure.ac`. Per the
project decision, change pg-lisp's version advertisement from `16.0` to
`20devel` as part of this milestone. Audit startup `server_version`, `version()`,
`SHOW server_version`, numeric version reporting, and any other advertised
version together. Record the exact C build revision and version in test output;
`20devel` alone is not a reproducible protocol baseline.

The advertisement is a research-project target, not a claim that all PostgreSQL
20 features exist. Support the resulting version-selected client behavior for
the chosen subset; reject unsupported options honestly. Do not retain or invent
an older version solely to steer a C peer around missing behavior. A released
PostgreSQL version matrix can follow after this checkout-to-checkout baseline.

Completion requires initial copy, ongoing changes, and restart/reconnect in
both directions. A stream-only demonstration is an intermediate checkpoint.
Existing pg-lisp-to-pg-lisp durability and extension tests must continue to pass.

## Original implementation baseline

The current [implementation history](LOGICAL-REPLICATION.md) describes a
pg-lisp-to-pg-lisp logical replication subset. Its useful foundations are:

- A snapshot/WAL boundary that includes transactions active at capture time.
- Committed row changes recorded in durable commit order.
- Target row changes and source progress committed together, with feedback
  after target durability, including the lost-feedback window.
- Durable slots, bounded reads, retention limits, and explicit reseeding.
- Schema drift checks and supported schema/sequence bootstrap and replay.

Most of these are correctness properties PostgreSQL already provides. They
should survive compatibility work. DDL/sequence automation and exact schema
contracts are additional pg-lisp capabilities, not requirements to impose on
ordinary PostgreSQL peers.

Current tests exercise meaningful crash and concurrency behavior, but mainly
connect our sender to our receiver. Their success establishes the documented
subset, not external interoperability. Code inspection found a shared protocol
mistake in the sender, receiver, and expected test bytes; this is evidence of
an insufficient compatibility oracle, not evidence about implementation motive.

## Original findings (resolved by this milestone)

Paths below are relative to this directory. Function names are the reference
anchors; line numbers will move during implementation.

| Finding | Evidence | Required treatment |
| --- | --- | --- |
| Transactional and streamed `Message` framing are conflated | `backend/replication/protocol.lisp:pgoutput-build-message` prepends XID whenever transactional; `worker.lisp:logical-pgoutput-read-message` expects it inside ordinary Begin/Commit; `t/unit-tests/logical-tests.lisp:test-logical-pgoutput-v2-codec` asserts that layout | Match C: the extra XID belongs only to streamed transaction blocks. `proto_version=2` alone does not introduce it. Correct both endpoints and the expected bytes. |
| Native TRUNCATE is replaced by a private message | `protocol.lisp:send-logical-transaction` sends `pg_lisp/truncate` as `M`; the worker has no native `T` dispatch | Implement standard `T` encoding/apply for supported TRUNCATE; reject unsupported flags rather than lose their meaning. |
| Ordinary C UPDATE can omit its old-key tuple | `worker.lisp:logical-apply-update` consumes the first marker as an old-tuple marker, then requires another N and later requires K; C can send a single N plus the new tuple when identity is unchanged | Handle N-only updates using the new tuple's identity; preserve old K for key-changing updates. Test both paths against C. |
| Startup accepts our worker's command shape | `protocol.lisp:parse-replication-command` accepts only its limited slot syntax and optional `proto_version '2'` | Parse the actual C-generated commands, identifier quoting, publication options, and supported slot snapshot actions. |
| Publication catalogs do not select the stream | `LOGICAL-REPLICATION.md` documents database-wide source events and target-side row allowlists | Implement explicit publication table selection and supported publish actions on the source. A target allowlist does not replace source selection. |
| Source bootstrap requires private SQL | `worker.lisp:logical-read-source-identity`, `logical-table-identity-signature`, and `logical-copy-from-exported-snapshot` call `pg_lisp_*` functions | Use standard identity/metadata for a C source; precreate target schema for standard mode. Keep private bootstrap in explicit extension mode. |
| Snapshot lifetime and completion are private | `logical-create-source-slot` closes the exporting connection before import; pg-lisp retains a token until its private copy-complete feedback handshake | Follow C exporter lifetime and slot snapshot semantics. Ordinary clients must not need the private first-feedback handshake to begin streaming. Preserve snapshot/WAL safety while changing the mechanism. |
| Physical WAL and logical capture are custom | `backend/storage/wal.lisp` has page after-images/private records; executor changes feed logical records | Preserve logical correctness while fixing the external protocol. This milestone does not claim C WAL, backup, recovery, or rewind compatibility. |

Also audit rather than assume compatible:

- `IDENTIFY_SYSTEM`, replication startup, result column names/types, database
  identity, server startup parameters, and supported connection setup commands.
- Commit record position versus transaction end position, timestamps, and
  feedback write/flush/apply positions. The current encoder puts the same LSN
  in both Commit fields and zero in timestamp fields; compare their semantics
  with the reference rather than merely matching field widths.
- Client-requested start positions, duplicate suppression, slot confirmation,
  and snapshot/table-sync catch-up. Do not impose our current terminal-position
  and first-feedback restrictions on a standard client without C justification.
- Relation identity flags, UPDATE with/without old keys, DELETE, relation-cache
  invalidation, namespace/name quoting, and unsupported tuple representations.
- Keepalive reply requests, idle progress, CopyBoth completion, malformed input,
  disconnect cleanup, and feedback bounds. A keepalive WAL end is not durable
  target apply progress; a private progress message must not be required by C.
- Unsupported protocol versions/options and requested streaming/two-phase modes.
  Version negotiation must reflect implemented semantics, not accept a mode
  whose extra fields we ignore.

## Initial supported interoperability contract

- One database, `public` ordinary tables, explicit publication table lists,
  and matching precreated schemas on both endpoints.
- `int4`, `int8`, and supported bounded `varchar`; explicit non-null primary
  keys, text tuples, UTF8, and values within pg-lisp's current limits.
- INSERT, UPDATE of a non-key column, UPDATE of the primary key, DELETE,
  multi-statement committed transactions, aborted transactions, and plain
  transactional TRUNCATE. Include multiple tables and an unpublished table.
- Finite transactions within the existing encoded-size cap; disable in-progress
  transaction streaming, binary tuples, two-phase replication, and optional
  advanced features using real supported C settings.
- Precreated schema is standard logical replication setup, not a test bypass.
  Initial row copy is mandatory; concurrent writes must cross its handoff.
- In standard mode, DDL and sequence synchronization are outside this row-data
  acceptance contract. The C development tree is evolving and contains newer
  sequence/publication code: audit it by revision rather than claiming that C
  PostgreSQL never supports sequence replication.
- Existing private DDL, sequence, strict-schema, and bootstrap behavior remains
  covered separately by the pg-lisp extension suite. No private event may
  silently replace a supported standard row operation.

Unsupported types, TOAST, partitions, row filters, column lists, generated
columns, REPLICA IDENTITY FULL, arbitrary schema transformations, and cascading
origins are subsequent scope. Where a peer can request an unsupported feature,
refuse safely before claiming progress for data that was not applied. This
bounded milestone does not establish full PostgreSQL protocol compatibility.

For the native C subscriber, explicitly request the baseline options rather
than depending on development-version defaults:

```sql
CREATE SUBSCRIPTION interop_sub
  CONNECTION '<test publisher connection>' PUBLICATION interop_pub
  WITH (copy_data = true, streaming = off, binary = false,
        two_phase = false, failover = false, origin = 'any');
```

Use isolated per-test names and assert the effective subscription settings in
the harness output. The source publication explicitly selects the test tables
and INSERT/UPDATE/DELETE/TRUNCATE actions; it excludes the negative-test table.
The pg-lisp subscriber must request the equivalent supported standard stream
from C. Record the negotiated protocol version separately from these options.

## Implementation checkpoints

### Final acceptance audit (2026-10-25)

The current implementation supersedes the unfinished status recorded in older
checkpoints below. The reference is unmodified C PostgreSQL `20devel` at
`9c0909b34939f2d5a37b82b9d1efc8b08e310252`; neither topology translates messages
or substitutes an apply worker for C's native subscription workers.

| Requirement | Implementation and acceptance evidence |
| --- | --- |
| Reproducible independent peer | Harness requires the C installation/revision, checks executable versions and pg_config, prints executable SHA256s and pg-lisp revision/version. Missing prerequisites fail. C-generated byte fixtures are in `t/interop/20devel-reference.hex`; codec tests also use independently constructed bytes. |
| Version-selected native setup | Startup/SHOW/version() use `20devel` and numeric `200000`. Native discovery executes real catalog queries through the SQL engine, including derived queries, DISTINCT, UNION ALL, arrays, SRFs and catalog origins. The native stream actually negotiates protocol 4; the standard worker requests protocol 1. |
| Explicit standard/extension boundary | Persisted `pgoutput` versus `pg_lisp` selects separate senders. Standard mode rejects private envelopes and unsupported streaming/binary/two-phase/origin options. Legacy slot formats retain extension semantics. Message framing, native T, N-only/K-bearing UPDATE, Relation flags, malformed input, feedback and CopyDone have focused tests. |
| Native C initial copy | Both tables use native table-sync workers and independent permanent sync slots/origins. Generation-one workers are terminated while COPY is paused; replacements cross newer snapshots with concurrent commits and rollback. Each reaches READY; transient sync slots/origins retire. Text COPY covers UTF8, quotes, tabs/newlines, NULL, literal backslash-N and signed int8 endpoints. |
| C-source initial copy | Standard identity/catalog metadata and a permanent pgoutput USE snapshot require no private source functions. Crashes after CREATE response and a committed copy chunk retire only the recorded attempt slot. Concurrent writes cross the replacement snapshot. An unrelated same-named source slot and a shadow search-path table remain unaffected. |
| Live transactions and publication selection | Both directions compare ordered canonical rows across multiple tables after INSERT, non-key and key UPDATE, DELETE, commit, rollback and transactional TRUNCATE. Unpublished rows remain absent. Native history tests cover membership/action changes between mutations, missing/recreated names, publication union and per-relation TRUNCATE; the identical case also passes C publisher -> C native subscriber. |
| Atomic failed apply | C row locking exposes an uncommitted INSERT before native apply termination; effects remain invisible and durable origin unchanged. Source confirmation stays below the exact failed transaction end. Standard apply is killed after its first row; effects and marker roll back. Both replay complete transactions, with non-idempotent updates detecting duplicates. |
| Lost feedback | Native durable origin is observed ahead of source slot confirmation, then the publisher is SIGKILLed before ACK persistence. Standard apply is killed after durable target commit before feedback. Both resume without duplicate effects; READY keeps its actual slot and data. |
| Endpoint restart and retention | Native publisher SIGKILL preserves exact disabled confirmed/restart floors. Native target immediate restart preserves durable origin, subscription identity and per-table READY frontiers. Standard target SIGKILL and C source immediate restart retain the durable marker and owned source slot. Writes while disconnected subsequently arrive. |
| Final durable published barriers | Each topology publishes row 90 after recovery and compares both complete tables plus unpublished exclusion. Native durable origin must reach that transaction's observed Commit end; standard durable source marker comes from received Commit and the C source confirms it. Global WAL head/received statistics are not used as apply evidence. |
| Fail-closed supported subset | Standard startup validates explicit public ordinary tables, complete projections/actions and actual non-null primary keys (excluding INCLUDE attributes). Tests reject publication drift, filters, partitions, generated/dropped columns, unsupported identities/tuple forms/options and unknown legacy publication history before claiming applied progress. |
| Extension and regression preservation | `t/logical-replication.sh`, UPDATE concurrency, unique-index concurrency, strict compilation, psql smoke and all 1,597 legacy checks pass. Existing extension crash/retention/slot-loss/reseed behavior remains covered by that regression; no old tests were ported. |
| Review and maintainability | Luna max checkpoints, Sol high full-diff review and follow-up, Sol xhigh protocol/oracle advice, Astra high historic-snapshot advice, and Sol medium refactor reviews completed. The review's historic publication and Relation-validation findings are fixed. Reviewed production functions are at most 120 lines. |

Final acceptance run: `/tmp/pg-interop-final-acceptance.log`, exit status 0 with
`logical-interop: both PASS`. Native artifacts are
`/tmp/pg-logical-interop.fgCF5p`, standard artifacts
`/tmp/pg-logical-interop.qKvFCf`. The final durable published barriers were
`0/000617E1` and `0/017BF850`, respectively. The run identifies the pg-lisp
working tree as dirty; the implementation is not committed. Sol high's final
exact-end/barrier review found no remaining implementation blocker. The
intentional audit-failure exit has been removed.

Additional verification: 129 focused Rove tests, three live native C subscriber
tests, and the selected C-to-C historic-publication oracle pass. Strict compile
is recorded in `/tmp/pg-history-barrier-compile.log`; legacy and smoke results
are `/tmp/pg-history-final-unit.log` and `/tmp/pg-history-final-smoke.log`.
`cl-spec/check-it` is unavailable in the configured ASDF environment, so no
generated-contract result is claimed. The preexisting JOIN/DDL concurrency
timeout reproduced on committed HEAD remains separate baseline test debt.

This accepts only the initial supported contract above. Physical WAL/disk
compatibility, general SQL/catalog coverage, DDL/sequence replication in standard
mode, advanced tuple/replication modes, HA and authority transfer remain outside
this milestone.

### Historic publication selection checkpoint (2026-10-25)

- Source INSERT/UPDATE/DELETE now copy eligible publication name, OID,
  generation, and unsupported-filter state inside the successful mutation's
  existing writer critical section. UPDATE completes CTID linking there using
  a guarded unlocked helper, preserving the B-tree-before-writer lock order.
  TRUNCATE captures all affected relation selections in one writer/catalog view.
- Logical row format 3 and TRUNCATE format 4 persist that immutable metadata.
  Legacy formats 0/1/2 still decode with unknown history; new empty selections
  remain distinct. Standard replay refuses an emitted record with unknown
  history before advancing its frontier. The existing encoded-byte limits
  include the new metadata. Extension decoding remains backward compatible.
- Standard filtering now resolves requested names solely against each change's
  history, including names dropped/recreated with different OIDs. It no longer
  reads current publication membership/actions or errors on currently missing
  requested names, matching C pgoutput LoadPublications. C snapbuild and
  reorderbuffer change historic catalog snapshots between individual changes,
  which is why transaction-commit-time selection would not be sufficient.
- All 129 focused tests and three native C subscriber tests pass. The new
  native case covers disconnected inclusion/exclusion, action edits between
  two writes in one open transaction, multiple publications, publication-name
  recreation, and per-relation TRUNCATE history. Independent byte/error tests
  cover the new codec and unknown versus empty history.
- /tmp/pg-historic-matrix.log passes both functional matrices (artifacts
  /tmp/pg-logical-interop.HyuRlv and /tmp/pg-logical-interop.mHL0Xb). Its source
  race pauses after the heap mutation but before logical encoding, commits a
  publication membership edit, then SIGKILLs the publisher before replay.
  The older published row still arrives before membership is restored.
- This addresses the final-audit publication-history finding in implementation;
  Luna max and Sol high follow-up found no blocker in the capture/codec path.
  Sol medium found no worthwhile additional production refactor. UPDATE and
  unique-index concurrency and extension logical replication pass, as do all
  1,597 legacy checks and strict compilation. Logs:
  /tmp/pg-history-update-concurrency.log, /tmp/pg-history-unique-concurrency.log,
  /tmp/pg-history-extension.log, and /tmp/pg-history-unit.log.
- The identical selected native publication-history test also passes with an
  unmodified C publisher on 5802 and unmodified C native subscriber on 5803.
  This independent producer/consumer oracle confirms the expected membership,
  action, name-recreation, union, and TRUNCATE results; evidence is in
  /tmp/pg-history-c-oracle.RvkmQR/result.txt. It supplements, rather than
  replaces, the required pg-lisp/C topologies.
- At this checkpoint final acceptance verification and documentation were
  pending, and the harness retained its intentional audit-failure exit.

### Final audit findings (2026-10-24; resolved 2026-10-25)

- Full-diff Sol high review identified a publication-history blocker: the
  standard sender filtered retained changes against the present
  publication catalog. A membership/action edit during subscriber downtime can
  therefore retroactively change which older rows were emitted. C pgoutput uses
  historic catalog snapshots. The checkpoint above implements immutable
  per-change selection; follow-up review found no remaining blocker.
- The receiver now rejects nondefault replica identity in standard Relation
  messages and unknown column flag bits before applying changes. Independent
  malformed/drift Relation tests pass; all 124 focused tests and strict
  compilation pass. The final legacy run passed 1,597 checks and psql smoke.
- Do not impose a global confirmed_flush_lsn <= native durable origin rule:
  C can acknowledge safely scanned filtered WAL without creating a new origin
  transaction. The exact disabled-restart slot-floor checks and the failed
  published-transaction confirmation bound test the relevant invariants.
- At that checkpoint full review and final regression evidence were still
  required; the final acceptance audit above records their resolution.

### Native worker and endpoint recovery checkpoint (2026-10-24)

- The native C subscriber harness pauses both table COPYs independently, records
  their separate slot/origin boundaries, and terminates both table-sync workers
  with pg_terminate_backend. Replacement COPYs reuse the sync slot names while
  old publisher COPY sessions remain paused. It then releases old sessions,
  commits writes across both replacement snapshots, and verifies independent
  READY frontiers, a published durable-origin barrier, and retirement of both
  sync slots and origins. A committed row between generations makes the
  replacement boundary strictly newer for each table.
- A C target row lock stops the main apply worker after an uncommitted INSERT.
  Terminating that worker leaves the row invisible and origin unchanged; source
  confirmation remains below the failed transaction end. Reconnect replays the
  complete multi-table transaction exactly once.
- Publisher SIGKILL/restart preserves exact confirmed/restart slot floors while
  the C subscription is disabled. C target immediate restart preserves exact
  durable origin and per-table READY frontiers before re-enable. A separate
  feedback pause observes native durable apply before publisher acknowledgment
  persistence; publisher SIGKILL at that point retains the old floor, and C
  resumes from its newer durable origin without duplicates.
- The reverse topology now also SIGKILLs the pg-lisp target and immediately
  stops/restarts the C publisher, retaining the same durable target marker and
  actual source slot. /tmp/pg-standard-endpoints.log records the first run.
- /tmp/pg-interop-utf8-matrix.log runs both isolated topologies through all these
  gates with UTF8, quotes, literal backslash-N, tabs/newlines, NULL amount/label,
  and an int8 boundary value in the initial copy. Artifacts are
  /tmp/pg-logical-interop.8dvl6z (native C subscriber) and
  /tmp/pg-logical-interop.m5F3vN (standard pg-lisp subscriber).
- Both directions still intentionally return failure pending the final
  acceptance-matrix/full-diff audit. Passing these functional gates is not a
  claim that the complete milestone or its final review has finished.
- /tmp/pg-interop-protocol-matrix.log repeats both functional matrices and
  records the actual native protocol 4 request before CopyBoth and the standard
  worker's protocol 1 request in the C publisher log. Artifacts are
  /tmp/pg-logical-interop.QCDj3c and /tmp/pg-logical-interop.cQCz54.
  Strict compilation and all 123 focused tests pass after the two disabled
  instrumentation hooks. Sol high found no further gap in the revised native
  origin/slot recovery oracles; final complete-diff review remains pending.

### Standard subscriber owned-copy recovery checkpoint (2026-10-24)

- Standard subscriptions now use a dedicated durable state record with explicit
  source endpoint/identity, publication, table allowlist, plugin, random actual
  slot name, state, and source LSN. The stable --slot argument keys the target
  subscription; it does not authorize deleting a same-named source slot.
  A target session owner serializes workers for that key and is released when
  its backend disconnects.
- A creating intent commits before native slot creation. The copy connection
  uses C tablesync.c's read-only repeatable-read transaction and permanent
  pgoutput slot with SNAPSHOT 'use'. On creating/copying retry, only the saved
  inactive compatible attempt slot is retired; its intent remains until DROP
  succeeds or absence is observed. A fresh random attempt then recopies.
  READY never enters reseed and keeps its actual slot across worker restart.
  Random names plus intent prevent accidental ownership confusion; C has no
  custom slot-owner tag that could authenticate deliberate external replacement.
- Copy reset, bounded copying, source snapshot COMMIT, and durable READY now
  share one helper with extension mode. Apply commits source LSN and rows in
  one target transaction before feedback. Standard startup revalidates complete
  publication membership/projection and all four required publish actions.
  Primary key metadata uses only the indnkeyatts prefix of indkey, excluding
  INCLUDE attributes, and requires a valid non-null primary index.
- /tmp/pg-standard-owned-apply.log (artifacts
  /tmp/pg-logical-interop.nRnk9w) passes a crash after native CREATE before copying
  progress, a crash after a committed copy chunk, concurrent writes across the
  replacement copy snapshot, live DML, lost-feedback recovery after target
  durability, and uncommitted apply rollback/replay. It preserves an unrelated
  source slot named interop_sub and keeps the READY attempt slot. C source PKs
  include a non-key INCLUDE attribute. All 122 focused tests and strict
  compilation pass. Sol high found no new blocker in this checkpoint.
- The full harness still exits with failure: native C subscriber table-sync,
  apply and lost-feedback recovery, remaining endpoint restart gates, and the
  final requirement-by-requirement audit remain incomplete. This checkpoint is
  not full milestone acceptance.
- Follow-up /tmp/pg-standard-owned-final.log (artifacts
  /tmp/pg-logical-interop.U0hCn2) passes those gates after the copy-helper
  refactor, plus READY startup rejection of removed publication membership and
  missing publish actions without changing target progress. Partition leaves
  expanded by C publication discovery are now explicitly rejected, alongside
  roots, filters, and partial column projections. All 123 focused tests pass;
  the extension logical-replication regression also passes
  (/tmp/pg-standard-owned-extension.log).
- /tmp/pg-standard-owned-namespace.log (artifacts
  /tmp/pg-logical-interop.snsZn1) repeats the gates with the C role's default
  search path pointing at a populated shadow table before public. Standard
  schema checks and copy SELECTs now explicitly read public, and final equality
  excludes the shadow row. Publication rejection tests require the expected
  validation error rather than accepting an arbitrary failed worker.

### C publisher standard subscriber checkpoint (2026-10-24)

- Added explicit subscriber --mode standard --publication NAME; extension mode
  remains the default. Standard mode reads IDENTIFY_SYSTEM/database OID,
  publication membership and supported schema/primary-key metadata from C
  catalogs. It requires matching precreated public ordinary tables and rejects
  row filters, partial column lists, generated/dropped columns, and unsupported
  replica identities before target reset. No pg_lisp_* source SQL is needed.
- A permanent pgoutput slot exports the initial snapshot. The exporting
  connection remains alive through import/copy. The existing bounded copier
  commits target chunks, then durably records READY before streaming standard
  nonstreamed protocol version 1 with the named publication. Private Message
  events are rejected in standard mode.
- Ready startup checks source slot plugin/database/options and rejects a
  confirmed or restart position ahead of the target's durable marker; C would
  otherwise advance START_REPLICATION to its confirmed position and skip data.
- /tmp/pg-reverse-standard-slotguard.log (artifacts
  /tmp/pg-logical-interop.aQ5QJE) copied both tables from unmodified C and passed
  the live DML/key-update/DELETE/abort/TRUNCATE/unpublished-table workload.
  It then failed intentionally at missing recovery gates. All 119 focused
  tests pass. A later run /tmp/pg-reverse-standard-primarykey.log (artifacts
  /tmp/pg-logical-interop.tzl9bZ) also passed after requiring actual non-null
  primary keys on both endpoints. Strict compilation and the extension
  logical-replication regression pass. No recovery acceptance is inferred.
- At this earlier checkpoint interrupted-copy retry failed closed. The owned
  recovery checkpoint above supersedes that limitation; endpoint restart and
  final full acceptance remain unfinished. Ready targets must never be reset
  to make recovery converge.

### Native initial-copy checkpoint (2026-10-24)

- Added the metadata and SQL needed by the reference C tablesync worker:
  searched CASE, text ARRAY construction, ANY over text arrays/int2vector,
  smallint, array_length, noncorrelated publication SRFs in FROM, supported
  pg_attribute/pg_index fields, and replica identity index lookup.
- COPY TO STDOUT uses the SELECT snapshot/admission path and emits text COPY
  frames, escaping control characters, backslashes, NULLs, and UTF8 values.
  Implicit transaction completion precedes the COPY command tag. References
  include C tablesync.c, pg_publication.c, and copyto.c.
- All 115 focused Rove tests pass. Both native C subscriber tests pass against
  the pinned reference: copy_data=false and copy_data=true with preexisting
  rows, followed by multi-table DML/TRUNCATE, aborted transactions, publication
  filtering, disable/enable reconnect, and slot cleanup. Both relations reach
  READY. These focused tests do not establish concurrent copy or crash recovery.
- The native harness now pauses COPY after its response under the imported
  snapshot, commits an INSERT/UPDATE and another table's INSERT, aborts a
  separate INSERT, then releases COPY. Both tables reach READY and converge.
  It also flushes the native origin, SIGKILLs the publisher, restarts the same
  PGDATA, and verifies a new row through the existing subscription OID without
  resetting target data. Run /tmp/pg-native-copy-recovery.log, artifacts
  /tmp/pg-logical-interop.jp8zCN, reached both gates and the live workload.
  This does not establish recovery during native apply or table sync, nor the
  lost-feedback window. The full harness still fails explicitly for remaining
  recovery gates. Strict compilation and all 115 focused tests pass.
- Publication fixtures now copy mutable table-entry lists on each invocation;
  row-filter tests must not alter later fixtures through quoted constants.
  System catalogs expose dense local attribute descriptors; this remains a
  bounded catalog model rather than complete canonical C catalog introspection.

### 1. Establish the independent reference and failing tests

- Add a proposed `t/logical-interop.sh` harness with independent direction
  selection and a `both` acceptance mode. Use an explicit `C_PG_BINDIR` for
  `postgres`, `initdb`, `pg_ctl`, and `psql`; verify they belong to the intended
  build. Use private data directories and ports and deterministic cleanup.
- Print C version/build revision, pg-lisp revision/version, negotiated protocol
  options, direction, and copy/stream phase. CI must provide the reference
  binaries; a missing peer is not a passing interoperability check.
- Capture the actual SQL and replication commands generated by the native C
  subscriber. Inventory required catalog queries, SQL syntax, and connection
  setup even for `copy_data=false`; do not assume that disables discovery.
- Add independent expected bytes/C-generated fixtures for ordinary versus
  streamed Message, native TRUNCATE, and N-only/K-bearing UPDATE. Use cl-spec
  and Rove for new contracts/tests where useful; retain existing test systems.

### 2. Standard row stream and explicit extension boundary

- Correct the observed Message bug and native TRUNCATE/UPDATE gaps using
  `proto.c`, `pgoutput.c`, and the C apply worker as references.
- Implement minimal publication selection and command/option negotiation for
  the native peer. Begin with nonstreamed transactions; protocol versions that
  permit streaming do not require it to be enabled.
- Introduce an explicit standard-versus-extension choice. Decide whether a
  distinct output plugin name or a negotiated capability is the cleanest
  boundary before changing persisted subscription configuration. Do not infer
  extension support just from a PostgreSQL-looking version string.
- A standard C peer must never have to interpret our Lisp event envelopes.
  Preserve fail-closed handling and durable markers in the extension path.
- Establish native stream-only replication in each direction as an intermediate
  checkpoint. No custom relay/apply process may stand in for the C subscriber.

### 3. Standard bootstrap, initial copy, and restart

- For C publisher -> pg-lisp, replace mandatory `pg_lisp_*` discovery with
  standard source identity and publication/relation metadata. Keep an exported
  snapshot's owning session alive until a correct import, or follow C's
  use-snapshot path. Never blindly drop an existing slot during retry; retain
  source identity, ownership, and durable resume checks.
- For pg-lisp publisher -> C, implement the catalog/SQL surface actually used
  by native subscription setup and table synchronization, including supported
  slot actions, snapshot transactions, and text `COPY ... TO STDOUT` framing
  and escaping. Return truthful catalog data rather than matching query text.
- Reconcile our one-boundary copy design with the native C table-sync/main-apply
  handoff. Trace slot creation, stream startup, feedback, catch-up, and cleanup
  before changing copy completion. Keep current snapshot and retention safety.
- Exercise restart while copying, during uncommitted apply, and after durable
  apply before feedback. Isolate source-position tracking from local WAL and
  never reset a ready target merely to make final row comparisons converge.
- In the C-subscriber direction, interrupt and recover individual table-sync
  workers as well as the main apply worker. Assert each table reaches its
  native ready state and crosses its own snapshot/catch-up frontier; a healthy
  main worker alone is not proof that every table finished copying. Preserve
  and inspect the separate table-sync slots/origins during those tests.

### 4. Complete the acceptance matrix and review

| Required topology | Initial copy | Live stream | Recovery |
| --- | --- | --- | --- |
| pg-lisp publisher -> C native `CREATE SUBSCRIPTION` | `copy_data=true`, matching precreated tables, multiple tables, writes during copy | Supported DML/TRUNCATE, rollback, publication exclusion | Reconnect/restart with native durable origin progress, including lost feedback |
| C publisher -> pg-lisp worker in standard mode | Standard snapshot, matching precreated tables, writes during copy | Same workload, including C's N-only UPDATE and key-changing UPDATE | Durable source marker with effects; restart and lost-feedback replay without duplicates |
| pg-lisp publisher -> pg-lisp worker in extension mode | Existing schema/sequence bootstrap | Existing supported DDL/sequence and row behavior | Existing crash, retention, slot-loss, and reseed regressions |

For both C-peer directions, assert transaction visibility and rollback effects,
not just eventual row counts. Compare canonical rows after source transactions
and a durable applied barrier; use non-idempotent updates to expose duplicate
apply. Establish that barrier with an observed published transaction and its
source end position, rather than assuming an arbitrary global WAL end has been
applied. C's received/latest-end statistics are not durable origin progress.
Stop the source or sender as needed to make partial application observable.
Require durable target progress across target restart and retained WAL while
disconnected. Failed apply must not durably confirm through the failed
transaction; receipt and previously applied positions may advance independently.
Target loss/reseed must be explicit; reseeding is not a successful resume.

Use C's normal process/SQL controls for fault injection; do not patch the C
server to accept our dialect. `pg_recvlogical` or an independent decoder can
help diagnose bytes but does not replace native subscriber acceptance. Tests
must not write expected workload effects directly to the subscriber, recopy
after a streaming failure, or compare only our own encoder and decoder.

Review checkpoints with luna max and the completed milestone/full diff with
sol high, following project guidance. Investigate difficult contract decisions
with sol xhigh (astra high for exceptionally difficult issues). After code
implementation, use sol medium to look for shared invariant-preserving helpers;
refactor production functions longer than 120 lines. The final acceptance audit
above records the completed reviews and verification.

## Relationship to Patroni, cutover, and physical compatibility

Logical interoperability is useful for migration and provides an independent
correctness oracle. It does not turn a logical subscriber into a physical
standby, including when both ends run C PostgreSQL.

The separate Patroni/physical-HA milestone still needs continuous physical
receive/replay, shared cluster identity and timeline history, recovery roles,
promotion/follow/rejoin, reliable stop/fencing behavior, and the process,
configuration, control-tool, and monitoring interfaces Patroni actually uses.
The inspected Patroni reference was the
[4.1.5 source](https://github.com/patroni/patroni/tree/v4.1.5), especially its
[PostgreSQL control layer](https://github.com/patroni/patroni/blob/v4.1.5/patroni/postgresql/__init__.py).
Custom bootstrap/clone hooks can
defer `initdb`/`pg_basebackup` compatibility, and full rebuilds can defer rewind;
those hooks do not replace ongoing recovery/history semantics. Compatible
control utilities need not imply C-compatible disk files, but our long-term
binary compatibility target remains separate debt.

Keep logical migration enhancements and add physical HA alongside them. Avoid
implementing a second election system merely to compensate for missing server
interfaces. A future managed cluster can delegate authority to Patroni, with
tested shutdown/watchdog/infrastructure fencing as appropriate.

Blue-green cutover has additional requirements: complete database coverage,
one target apply owner/generation, compatible old/new event and type semantics,
healthy slots/retention, a durable writer fence and final barrier, schema/data/
sequence validation, and a restartable authority transfer. A routing update
does not fence an old primary. After green accepts writes, failback requires
reconciling its new history; initial cutover should be forward-only. Preserving
all acknowledged commits during automatic failover additionally requires an
explicit synchronous durability and candidate-eligibility policy. None of these
properties follows merely from passing the two logical C-peer tests.

## C implementation and test references

- [Protocol codec](../backend/replication/logical/proto.c), especially
  `logicalrep_write_message`, `logicalrep_write_truncate`, and Commit fields.
- [pgoutput](../backend/replication/pgoutput/pgoutput.c), especially options,
  publications, `pgoutput_message`, and the streaming-context checks.
- [Replication transport/client](../backend/replication/libpqwalreceiver/libpqwalreceiver.c),
  especially identity, connection setup, slot syntax, and stream options.
- [Subscription setup](../backend/commands/subscriptioncmds.c) and
  [table synchronization](../backend/replication/logical/tablesync.c), including
  discovery queries, snapshot actions, COPY, and catch-up transitions.
- [Apply worker](../backend/replication/logical/worker.c),
  [logical slots](../backend/replication/logical/logical.c),
  [slot durability](../backend/replication/slot.c), and
  [snapshot ownership](../backend/utils/time/snapmgr.c).
- [Native subscription tests](../test/subscription/t/001_rep_changes.pl),
  [table synchronization](../test/subscription/t/004_sync.pl),
  [schema differences](../test/subscription/t/008_diff_schema.pl), and
  [streaming tests](../test/subscription/t/015_stream.pl).

## Validation status

### Native publisher stream checkpoint (2026-10-24)

- An unmodified C 20devel native subscriber now applies the supported row
  stream from pg-lisp with `copy_data=false`: INSERT, non-key UPDATE,
  primary-key UPDATE, DELETE, multi-statement commits, rollback exclusion,
  plain transactional TRUNCATE, and unpublished-table exclusion. Changing a
  publication to INSERT-only filters UPDATE at the source. This is the stream
  checkpoint, not completion of the mandatory initial-copy topology.
- `pg-interop-tests/native` is a separate repeatable Rove regression against
  task-owned pg-lisp/C peers. It asserts the native subscription's effective
  options and table ready states, exercises those row operations and publication
  actions, disables/re-enables the native subscription to verify reconnect,
  and checks native subscription/slot removal. Its native worker passes without
  message translation or a custom C-side apply process.
- The persisted plugin selects a separate standard sender. START accepts native
  quoted options and protocol versions 1-4 with in-progress streaming, binary,
  two-phase, and logical messages disabled, and origin `any`. The existing
  pg_lisp sender retains its private first-feedback and extension lifecycle.
  DROP WAIT waits interruptibly for an active sender without retaining reload
  admission or the lifecycle mutex across the wait.
- Standard decoding scans from an aligned retained cursor independently of
  feedback. It suppresses commits whose record start precedes
  `max(requested_start, confirmed_flush)`, including C's zero/stale/future start
  semantics. Nonfinal XLogData headers carry zero positions; only a completed
  Commit or completely filtered scan establishes an advertised safe frontier.
  Idle progress uses ordinary keepalives, with no private progress Message.
- Feedback uses flush independently of write/apply, ignores stale/zero flush,
  and handles a lost-feedback resume claim ahead of the current scan without
  confirming unsent data. Exact flush positions are persisted separately from
  aligned restart floors and pending-XID retention. A bounded 1,024-entry queue
  of completed frontiers limits memory when feedback lags.
- The sender consumes fragmented input and feedback during partial output,
  retaining only one outbound frame and a bounded retry chunk. An error during
  a partial raw frame closes the transport before any ErrorResponse can corrupt
  framing. CopyDone drains the current frame, releases the slot, sends both
  `COPY 0` and `START_REPLICATION` completions, and preserves buffered input for
  the next command. Replication timeout closes the connection.
- TRUNCATE records immutable OIDs and relation descriptors and sends Relation
  metadata before each native T frame, including a relation's first operation.
  Row and TRUNCATE capture share descriptor-copy helpers. Commit WAL now carries
  signed microseconds since PostgreSQL's epoch; old empty commit payloads remain
  readable as unknown timestamps. Standard Begin/Commit use the durable source
  commit time, and standard headers/keepalives use actual send time. Timestamp-
  less legacy transactions are not silently assigned fabricated commit times.
- The primary Rove suite passes 112 tests. All 21 live apply/SQL/wire suites pass
  on a fresh target, and the separate native subscriber regression passes.
  Added cases cover exact/interior confirmation, resume feedback, fragmented
  status packets, replay floors, first-TRUNCATE Relation metadata, persisted
  timestamp vectors, CopyDone during output, cancellation after a partial frame,
  timeout, pipelined SQL after CopyDone, and a query after CopyBoth ERROR.
  Strict compilation, the legacy 1,597-check runner, and the extension replication
  suite pass. Luna max and Sol high reviewed this stage; Sol medium's useful
  cleanup was to parse TRUNCATE metadata once. Changed production functions
  remain below 120 lines.
- A separate live check also retained native durable origin progress through an
  actual pg-lisp publisher SIGKILL/restart and applied later rows without
  duplicate-key failures. This does not yet establish the full crash/copy/lost-
  feedback acceptance matrix. The full `copy_data=true` harness now starts the
  native main and both table-sync workers, then fails while fetching the missing
  `pg_class.relreplident` metadata. Attribute/index catalogs, COPY, full table-sync
  handoff/recovery, and the C-publisher standard subscriber path remain unfinished.

### Native slot creation and identity checkpoint (2026-10-24)

- The native C subscriber now completes its discovery SQL and creates the main
  permanent `pgoutput` slot with `SNAPSHOT 'nothing'`, `TWO_PHASE false`, and
  `FAILOVER false`. `IDENTIFY_SYSTEM` then succeeds. The latest native run
  reaches `START_REPLICATION` and fails at the remaining protocol-version and
  publication-option negotiation. Initial copy and ongoing native replication
  still do not pass; the full milestone remains incomplete.
- CREATE accepts C's parenthesized options and legacy snapshot spellings,
  including quoted identifiers and doubled quotes. Duplicate/malformed options
  fail, and temporary slots, two-phase/failover slots, and standard snapshot
  export remain explicitly unsupported. Standard NOTHING and USE return NULL
  snapshot names. USE requires a read-only REPEATABLE READ transaction before
  its first query, matching C's walsender checks.
- Standard slots persist a database binding, plugin, and confirmed boundary in
  their first durable image. PGSLOT4 records the output plugin; PGSLOT1-3 load
  as `pg_lisp`, preserving the previous private lifecycle. New extension workers
  explicitly request `pg_lisp` and verify the result plugin. Standard pgoutput
  START currently fails closed before entering the private handshake or sending
  private DDL/sequence/progress envelopes.
- Snapshot capture and slot publication share the writer critical section, so
  every post-boundary writer sees the slot. Active pre-boundary XIDs retain
  their earlier WAL positions. USE transfers MVCC and schema registrations to
  the SQL transaction independently of the slot: DROP/recreation cannot release
  them. COMMIT, abort, and failed-block entry release them. Failed publication
  and uncertain rename/fsync preserve the durable slot's WAL pin without
  leaking a provisional registration or runtime slot owner. CREATE response
  transmission remains serialized against DROP.
- IDENTIFY_SYSTEM returns C's TEXT/INT8/TEXT/TEXT result layout, timeline 1,
  durable flush LSN, and connected database. Its unsigned decimal system ID is
  a fixed FNV-1a-64 projection of the existing durable UTF8 catalog identity.
  The private identity string is unchanged. Fixed ASCII/UTF8/legacy vectors,
  catalog round trips, and live graceful/crash server restarts verify stability;
  both restarts also reload a confirmed pgoutput slot with its plugin intact.
- The primary Rove system passes 101 tests. New checks cover parser validation,
  first durable images, legacy slot migration, USE ownership, writer exclusion,
  response serialization, and error cleanup. All 19 live interop suites pass on
  a fresh target, including native USE transaction behavior, private slot
  transaction state, replication error state, and IDENTIFY_SYSTEM metadata
  against the C reference. The C-frame apply regression also passes, including
  its final rollback; the client now ignores valid ParameterStatus frames.
- Strict compilation, the legacy 1,597-check unit runner, and
  `t/logical-replication.sh` pass. Luna max identified and rechecked the writer
  race and response/parser fixes. Sol high reviewed the implemented slot stage;
  Sol medium found no additional useful invariant/refactor extraction. Reviewed
  production functions remain below 120 lines. These results establish the
  slot/identity prerequisites, not the acceptance matrix above.

### Catalog discovery checkpoint (2026-10-24)

- Added read-only `pg_class`, `pg_namespace`, and
  `pg_publication_sequences` sources. Relation rows derive from persisted
  tables, indexes, sequences, and implemented system relations. Namespace rows
  reflect the two implemented namespaces, pg_catalog/public. Sequence rows
  reflect current publication selection, including sequences created or dropped
  after a native `CREATE PUBLICATION ... FOR ALL SEQUENCES`. Owned-column
  sequence bootstrap remains a separate extension behavior.
- A shared descriptor now defines virtual relation identities, field types,
  original attribute numbers, and row-source dispatch. `pg_publication` exposes
  all eleven C columns in C order, including the all-sequences flag and defaults
  for unsupported options that the parser rejects. The owner is bootstrap role
  10 under the existing single-role model. The supported replication-slot view
  fields use the pinned C view OID 12283 and original attributes; the local
  `retained_wal_bytes` extension follows them at attribute 23.
- Added numeric OID comparison/IN coercion and explicit OID casts, retaining
  int4 binary reinterpretation and int8 range checks. SQL LSNs now have numeric
  values, strict C-compatible input, and the pinned C `%X/%08X` output. The slot
  adapter preserves the raw WAL snapshot API while converting SQL values and
  ordering columns. `pg_current_wal_lsn()` now returns typed numeric `pg_lsn`.
  LSN arithmetic remains explicitly unsupported; comparison, IN, sorting, and
  text conversion are tested against C, including hex-width boundaries.
- All 91 Rove tests and seven selected live C comparison suites pass. The latter
  include the native discovery query with an explicit ORDER BY for deterministic
  raw-wire comparison; the actual unmodified subscriber also executes its
  original unordered query successfully. Strict compile, 1,597 unit checks,
  smoke, sort-arena, extension replication, and slot backpressure pass. Existing
  slot/LSN unit assertions were updated for typed raw values while retaining
  formatted-output checks; no old tests were moved to a new framework.
- Luna max and Sol high reviewed catalog identities, column ordering, numeric
  coercion, snapshot lifetime, and LSN integration. Findings were addressed and
  tested. Sol medium found no additional useful abstraction or touched production
  function over 120 lines.
- C references include `pg_class.h`, `pg_namespace.h`, `pg_publication.h`,
  `system_views.sql:429-437`, `pg_publication.c:1767-1810`, `pg_cast.dat`,
  `common/pg_parse_lsn.c`, `pg_lsn.c`, and `xlogfuncs.c`. Initdb-created view OIDs
  were verified against the pinned unmodified C reference.
- These are supported catalog projections, not a complete native catalog dump:
  pg_class, pg_namespace, and pg_replication_slots expose their implemented fields
  and namespaces. Explicit sequence-list publication remains an extension beyond
  the pinned native all-sequences grammar. The complete captured-frame apply
  suite still has the pending client ParameterStatus gap.
- Native CREATE SUBSCRIPTION now completes publication discovery and reaches slot
  creation, failing on the newer `SNAPSHOT 'nothing'` option syntax. Next work
  must preserve distinct standard `nothing`/`use`/`export` semantics: a standard
  slot is confirmed in its first durable file, and tablesync `use` installs a
  transaction-owned snapshot and schema lease. It must not alias these actions
  to the private unconfirmed exported-copy handshake. Standard streaming/COPY
  and bidirectional recovery acceptance remain unfinished.

### UNION ALL discovery checkpoint (2026-10-24)

- Added binary, left-associative UNION ALL query expressions, including nested
  derived queries and parenthesized arms. Each arm retains its DISTINCT,
  ORDER BY, and LIMIT; unparenthesized ORDER BY/LIMIT apply to the compound
  query. Other set operations fail explicitly. Global ordering accepts output
  names and ordinals, with C-compatible ambiguity and missing-name errors.
- Type analysis resolves each pair of arms in order, preserving the all-unknown
  to text boundary. Unknown constants are validated during analysis; supported
  implicit casts, typmods, leftmost output names, and zero output origins match
  C. OID coercion covers catalog output combined with integer or unknown string
  arms, including signed int4 reinterpretation and int8 range checks. OID text
  input follows this reference host's base-0 strtoul behavior (including binary
  prefixes), ASCII digits, uint64 overflow, and signed-extension compatibility.
  Explicit OID cast syntax remains outside the supported expression-cast subset.
- Both arms prepare under one catalog view. Their complete child plans are
  rebased into one relation registration and index lease domain. Append coerces
  each projected arm row into common output slots before global sorting. An
  arm-local LIMIT ends only that arm; downstream early stop prevents admission
  to the right arm. Shared helpers centralize final order/limit construction
  and relation/metadata installation for ordinary and compound SELECT plans.
- All 85 Rove tests and six selected live C comparison suites pass. Tests include
  nested union/JOIN plans with both index scans, raw RowDescription/DataRow/
  completion frames, pairwise type failures, volatile early-stop behavior, and
  forced spilling through DISTINCT, Append, and outer sorting. Strict compile,
  1,594 existing unit checks, smoke, sort-arena, and extension replication pass.
  These selected suites do not establish a passing complete captured-frame apply
  suite or native initial-copy/recovery acceptance.
- Luna max and Sol high reviewed coercion, lifetime, stop propagation, and
  rebasing; the OID text-dispatch, Unicode digit, binary-prefix, and bounded
  accumulation findings were fixed and tested. Sol medium found no required
  further refactor or reviewed production function exceeding 120 lines.
- C references include `analyze.c:1792-2040,2280-2396`, `parse_coerce.c`'s common
  type selection, `prepunion.c:1495-1605`, `nodeAppend.c`, `pg_cast.dat`,
  `numutils.c:uint32in_subr`, and `int8.c:i8tooid`.
- A fresh unmodified C subscription now parses past UNION ALL and fails because
  `pg_class` is absent. The next catalog slice must expose actual relation and
  namespace state plus publication sequence discovery; it must not substitute
  query-specific rows. The bidirectional native replication milestone remains
  incomplete.

### Publication SRF discovery checkpoint (2026-10-24)

- Added target-list `pg_get_publication_tables`, composite field selection and
  expansion, and a ProjectSet executor that zips unequal sets with NULL padding.
  The result descriptor follows C: `pubid oid`, `relid oid`, `attrs int2vector`,
  and `qual pg_node_tree`. Publication/table identities and attribute numbers
  come from catalog state; repeated publication arguments retain repeated rows.
  Row-filter expression trees remain unsupported and fail explicitly.
- Publication OIDs now use the durable catalog allocator and survive ALTER.
  Startup upgrades legacy publications in deterministic name order and saves
  their identities before admitting clients. `t/publication-oids.sh` verifies
  read-only observation followed by SIGKILL/recovery, restart, allocator
  advancement, and replacement identity after DROP/CREATE.
- C planner behavior matters after record expansion: identical SRFs share an
  intermediate ProjectSet slot even with volatile scalar arguments. Duplicate
  bare SRF targets remain separate when no extra projection is needed. Identical
  nonvolatile aggregates share descriptors; volatile aggregate inputs retain
  separate identities. Live tests compare sequence advancement against C for
  expanded, bare, mixed, and volatile-aggregate calls. ORDER BY aliases reuse
  the selected expression.
- Whole-record DISTINCT and ordering compare field values, with explicit record
  serialization for spill files and recursive arena copying. Forced spilling
  exposed a nested executor lifetime bug: consumer sort runs were deleted when
  an inner source scope returned. Spill ownership now belongs to the outermost
  operation, sharing cleanup and quota accounting across nested callbacks.
  This can retain inner spill files longer and increase peak temporary usage.
- C references include `pg_publication.c:1459-1764`,
  `parse_target.c:1454-1505`, `tlist.c:1027-1121,1283-1324`, `prepagg.c`,
  `nodeProjectSet.c:159-215`, `rowtypes.c`, and the binary-compatible
  `pg_node_tree` to `text` cast used by record operator lookup.
- All 81 Rove tests and both selected live SRF tests pass. Strict compilation,
  1,594 existing unit checks, psql smoke, sort-arena, extension replication, and
  catalog durability fault injection pass. The legacy smoke JOIN queries use ORDER BY ordinal
  1 where their repeated output names are ambiguous in C; expected rows remain
  unchanged. Luna max and Sol high reviewed this checkpoint; Sol medium reviewed
  invariant centralization and found no production function over 120 lines.
- Runtime SRF catalog enumeration takes a catalog snapshot; concurrent schema
  DDL semantics remain outside the bounded compatibility contract. General SRF
  nesting, FROM-function syntax, and arbitrary record-returning functions are
  unsupported. The complete captured-frame apply suite still has the known
  client ParameterStatus gap.
- The unmodified C subscriber now reaches `UNION ALL` in publication discovery;
  that syntax is the next failing native gate. `pg_class`, `pg_namespace`, and
  publication sequence discovery still need real catalog sources. Standard
  bootstrap, COPY, row streaming, and bidirectional restart/recovery acceptance
  remain unfinished. This checkpoint does not establish native replication.

### Array-aggregate discovery checkpoint (2026-10-24)

- Added array_agg(text) with text[] result OID 1009/typlen -1, PostgreSQL array
  output quoting, NULL elements, and SQL NULL for zero input rows. The existing
  COUNT(*) result remains zero on empty scalar input. Non-text array_agg inputs
  and array literal/input syntax remain explicitly unsupported.
- Aggregates are lifted recursively from selected scalar expressions and ORDER
  BY, with grouped input Vars kept separate from aggregate result slots.
  CAST(COUNT(*) AS text), wrapped array aggregates, multiple aggregates, and
  ORDER BY-only aggregates now execute. Nested aggregates and invalid scalar
  operations on arrays fail during analysis. DISTINCT ORDER BY reuses selected
  aggregates by semantic identity while rejecting genuinely hidden aggregates.
- COUNT-only plans retain the existing hash aggregate path. Queries containing
  array_agg use stable sorted grouping and one active group's spillable input
  spools, sharing a per-group transition memory allowance across descriptors.
  Inputs are evaluated once before spill/replay. Final result arrays necessarily
  require memory proportional to their output size, as in C PostgreSQL.
- Added a sorter completion hook inside its temporary-file scope so the final
  group's spools remain available through finalization. Completed spools are
  closed and deleted promptly, including downstream stop/error paths. Forced
  work_mem=1 tests cover sorted spills, the last group, NULLs, and cleanup.
- All 72 focused Rove tests pass. A selected live test compares 24 identical SQL
  queries with unmodified C, including complete successful wire frames, SQLSTATE
  errors, metadata, nested casts, hidden ORDER aggregates, DISTINCT, and sequence
  inputs that verify single evaluation and separate volatile aggregate calls.
  Strict compilation, 1,594 existing checks, sort-arena, psql smoke, and extension
  replication pass. Smoke caught a lost grouping-error column name, which was
  restored before its successful rerun.
- Native CREATE SUBSCRIPTION still stops at the record expansion suffix in
  (pg_get_publication_tables(...)).*. Latest run: /tmp/pg-logical-interop.znTJLk.
  Publication SRF/record expansion, remaining catalog queries, UNION ALL,
  standard initial copy, live streaming, and recovery remain unfinished.
- Luna max, Sol high, and Sol medium reviewed this slice. ORDER BY ordinal/alias
  rules now share one helper; aggregate signatures centralize identity across
  differing result slots. Production functions remain below 120 lines. Text
  array comparison follows the existing C-locale string comparison; non-C
  collation remains unimplemented. C references include array_userfuncs.c,
  arrayfuncs.c, nodeAgg.c, parse_agg.c, analyze.c, and parse_clause.c.

### Expression-cast discovery checkpoint (2026-10-24)

- Added postfix :: and CAST AS for supported expression types, independently
  of the existing DDL storage aliases: text, varchar, name, int4/int8, bool,
  internal "char", and int2vector. OID/typlen declarations are shared, and
  unknown expression types no longer silently report text. Expression varchar
  has an unlimited default and explicit character-count truncation.
- Cast analysis distinguishes unknown literals from explicitly typed constants,
  validates literal input before execution, preserves no-op column casts, and
  implements NULL propagation. Integer input follows C radix/underscore rules;
  internal char uses unsigned byte comparison and signed integer conversion;
  char octal I/O and space-delimited int2vector I/O follow their C routines.
  Shared input functions also serve IN coercion.
- Quoted tokens retain their identifier status, so quoted keywords no longer
  become literals. Shared string/identifier quote scanning keeps tokenizer
  functions below 120 lines. Cast column labels follow C's strong column/function
  versus weak type-name precedence. VARIADIC is parsed only on the final argument;
  exact nonvariadic matches clear the marker as C ParseFuncOrColumn does.
  Variadic publication-table execution is still unimplemented.
- New catalog types participate in comparisons and IN. When a common IN array
  type cannot be selected, analysis falls back to individual comparisons, as
  C transformAExprIn does with a NULL error context. Incompatible typed pairs
  fail before execution, including when WHERE would discard every row.
- All 64 focused Rove tests pass. A selected live Rove test sends 28 identical
  SQL queries to pg-lisp and the unmodified C reference and compares successful
  RowDescription/DataRow/CommandComplete/ReadyForQuery frames byte-for-byte;
  error cases compare SQLSTATE. Values, names, OIDs, lengths, typmods, quoted
  identifiers, mixed IN inputs, and range errors match. Final strict compilation,
  all 1,594 existing checks, and psql smoke pass. Extension replication also
  passed in this checkpoint, before the last comparison/naming review fixes.
  cl-spec/check-it remains unavailable in the configured ASDF environment.
- Native C discovery now parses VARIADIC and pubname::text and stops at the
  record expansion suffix in (pg_get_publication_tables(...)).*.
  Run: /tmp/pg-logical-interop.h3VrjN. Text arrays, array_agg and recursive
  aggregate lifting (including CAST(COUNT(*) AS ...)), SRF/record expansion,
  remaining catalog queries, and UNION ALL remain unfinished. The two-direction
  initial-copy/live/recovery milestone is still incomplete.
- Luna max and Sol high reviewed the slice; regressions address quoted names,
  cast label strength, numeric overflow, typed comparisons, and mixed IN lists.
  Sol medium confirmed the helper structure and function length limits.
  References include parse_expr.c, parse_func.c, parse_target.c, pg_cast.dat,
  pg_type.dat, char.c, int.c, numutils.c, and subscriptioncmds.c.

### Derived-table discovery checkpoint (2026-10-24)

- Added non-LATERAL SELECT relations in FROM and JOIN, including nested queries,
  unnamed derived relations, DISTINCT/aggregate children, and outer joins.
  Child output names/types/typmods form the parent namespace; direct column
  origins survive nesting and qualified duplicate column names remain ambiguous.
  Qualified stars and derived column alias lists remain unsupported.
- Recursive preparation resolves all nested relations under one catalog view.
  Planning then splices child trees into the root relation-registration domain,
  rebasing relation handles while preserving each SELECT's local expression
  slots and join environment width. All real child heaps and chosen indexes
  participate in admission/revalidation before execution, and all index read
  leases share one sorted acquisition. Execution retains the statement snapshot;
  it does not invoke nested registration or acquire nested index lease sets.
- Subquery Scan nodes participate in traversal, EXPLAIN/ANALYZE, instrumentation,
  and index fallback. Focused tests verify nested execution, namespace errors,
  origins, complete child admission, sorted index leases, and child index
  fallback without mutating the original child tree.
- All 55 focused Rove tests and the live indexed derived-query/origin test pass.
  Strict compilation, all 1,594 existing checks, psql smoke, JOIN streaming,
  and extension replication pass. JOIN/DDL concurrency times out before any SELECT executes, while waiting
  for two simultaneous DROP gates. An isolated committed HEAD run reproduces
  that scheduling failure; DDL event ordering serializes the two DROP commands.
  Logs: /tmp/pg-derived-ddl.log and /tmp/pg-derived-baseline-ddl.log.
- Unmodified C CREATE SUBSCRIPTION now parses the derived JOIN and reaches
  VARIADIC array_agg in the publication-table function argument, where parsing
  reports expected closing parenthesis. Run: /tmp/pg-logical-interop.oTCj3t.
  Arrays/casts/array_agg/VARIADIC, SRF/record expansion, remaining catalogs,
  UNION ALL, initial copy, live row streaming, and recovery remain incomplete.
- Luna max and Sol high found no defect in this supported slice. Sol medium's
  review led to shared initial-scope construction, centralized recognition of
  relation-handle nodes, and explicit origin-width validation. Production
  functions remain below 120 lines. C references: transformRangeSubselect,
  addRangeTableEntryForSubquery, markTargetListOrigin, and SubqueryNext.

### DISTINCT discovery checkpoint (2026-10-24)

- Added ordinary SELECT DISTINCT through parser, analyzer, logical/physical
  plans, execution, traversal, and EXPLAIN. Deduplication follows projection
  and precedes sorting and LIMIT. It reuses bounded hash aggregation, including
  partitioned spill and external-sort fallback, with no aggregate columns.
- DISTINCT ORDER BY resolves output aliases, positions, and selected expressions
  to projected slots. Hidden expressions fail with 42P10, conflicting aliases
  with 42702, and non-integer constants with 42601. Identical aggregate aliases
  retain semantic identity separately from their internal output slots.
  DISTINCT ON remains explicitly unsupported.
- Grouping keys now compare integer-vector contents rather than vector identity;
  integer-vector comparison also supports the spill-sort fallback. This prepares
  DISTINCT for publication attribute vectors; it does not yet implement the
  int2vector SQL type or the native publication table function.
- All 51 focused Rove tests pass, including NULL/duplicate/empty input, vector
  values, forced spill and sort fallback, ordering errors, and repeated aggregate
  aliases. A live Rove test passes for projection/order/LIMIT, DISTINCT *, grouped
  aggregates, and hidden ORDER BY rejection. Strict compilation and all existing
  1,594 checks pass after the review fixes. Psql smoke and extension replication
  also pass in this checkpoint (started before the final review fixes).
- The unmodified C CREATE SUBSCRIPTION now gets past DISTINCT and fails at the
  derived-table JOIN in fetch_relation_list, with "expected identifier". The
  captured run is /tmp/pg-logical-interop.VZiKa0. Derived SELECT relations, the
  publication-table SRF/record expansion, casts/arrays/array_agg/VARIADIC, required
  class/namespace catalogs, and UNION ALL remain prerequisites for this query.
  Initial-copy, streaming, and recovery acceptance remain incomplete.
- Luna max and Sol high reviewed the slice; regressions address their constant,
  aggregate-alias, and vector findings. Sol medium's shared projected-slot helper
  centralizes type metadata. References include C parse_clause.c SQL92 sorting
  and transformDistinctClause, planner.c DISTINCT paths, bytea.c byteacmp, and
  subscriptioncmds.c fetch_relation_list.

### Publication discovery checkpoint (2026-10-24)

- Added list IN/NOT IN predicates with SQL three-valued results. Analysis follows
  C `transformAExprIn`: more than one non-Var item forms a common-type array
  group, while row-dependent items remain short-circuit comparisons. Unknown
  string literals are coerced in their comparison context for supported types;
  name input clips at UTF8 boundaries as C `namein` does.
- Added read-only supported `pg_catalog.pg_publication` fields backed by actual
  persisted publication definitions. CREATE/ALTER/DROP affect query results;
  publish-action fields describe configuration, not completed sender filtering.
  Unsupported catalog fields still fail ordinary column lookup rather than
  receiving invented values.
- Publication names report name OID 19/length 64. Dense executor row positions
  remain separate from C RowDescription origins (table OID 6104 and attribute
  numbers 2,4,6,7,8,9). One shared field declaration now owns names, types, and
  original attribute numbers, following the Sol medium refactoring review.
- All 42 protocol tests pass. Seven selected live connection tests pass,
  including the native publication existence query, action changes, wire origin
  metadata, and row-dependent IN short-circuit behavior. The complete apply
  system still includes the separate pending client ParameterStatus failure.
  Forced compilation, all 1,594 existing checks, psql smoke, and the extension
  replication regression also pass at this checkpoint.
- Native unmodified C CREATE SUBSCRIPTION now passes publication existence
  lookup and reaches replicated-table discovery, which stops at unsupported
  SELECT DISTINCT. This is runtime progress through the actual native command
  sequence, not initial-copy or streaming acceptance.
- Luna max and Sol high reviewed this slice. Findings about eager row-item
  evaluation, unknown literal coercion, and catalog attribute numbers were
  corrected with focused regressions. References include C `parse_expr.c`,
  `execExprInterp.c`, `name.c`, `pg_publication.h`, generated
  `pg_publication_d.h`, and `subscriptioncmds.c:check_publications`.

### Search-path checkpoint (2026-10-24)

- Added session `search_path` and supported `set_config(text,text,bool)`,
  SET/SHOW/RESET, startup/change/rollback ParameterStatus reporting, and
  `current_schema()` empty-path values. Transaction-local settings are rejected.
  Explicit transactions retain settings on COMMIT and restore them on ROLLBACK;
  a failing individual implicit statement also restores its settings.
- SQL relation resolution now preserves explicit schema/identifier boundaries,
  keeps raw storage lookup separate, and follows pg_catalog/public search order.
  Empty paths hide unqualified public tables while permitting qualified SELECT;
  unqualified creation without a default schema fails before catalog mutation.
  Virtual catalogs are recognized by reserved identity, preventing a real public
  table with the same name from becoming an executor-backed catalog. DROP and
  ALTER use the SQL lookup and retain their existing raw identity revalidation.
- Search-path identifier lists follow `SplitIdentifierString` quoting and
  delimiter rules rather than stripping SQL comments. SET list elements retain
  embedded commas as identifier data. Non-SELECT relation grammars still lack
  full qualified-name support; unsupported dotted literal mutation identifiers
  now fail before side effects instead of being silently stripped to another
  table name. Qualified SELECT identifiers and aliases are supported.
- All 36 protocol tests and six selected live connection tests pass. The live
  tests include session isolation, clear-path data visibility, qualified table
  access, transaction and statement-error undo, catalog collisions, and initial
  ParameterStatus. The forced compile and existing 1,594 checks pass. These
  selected tests do not include the known client ParameterStatus failure in the
  complete captured-frame replay system.
- The extension replication, psql smoke, and hot-reload scripts also pass after
  this checkpoint; no native copy or recovery acceptance is inferred from them.
- Native C CREATE SUBSCRIPTION now completes secure-path setup and reaches its
  publication discovery query; it currently stops at unsupported IN grammar.
  Publication catalogs, later discovery syntax, slot actions, COPY, standard
  mode, and native initial-copy/recovery acceptance remain unfinished.
- Luna max and Sol high reviewed namespace semantics and found the SET-list,
  mutation lookup, dotted-identifier, and startup reporting issues above; those
  were addressed. Sol medium found no worthwhile further macro/helper rewrite
  and no production function exceeding 120 lines in this checkpoint.
  Broader preexisting simple-query message atomicity remains:
  several statements without explicit BEGIN are still committed individually,
  so the new statement-error undo result does not establish whole-message undo.
  Extension DDL events also need a final audit of public catalog-name collisions
  because their unqualified AST is replayed under the subscriber's own path.

### SQL connection checkpoint (2026-10-24)

- Logical replication connections now execute ordinary SQL through the normal
  query path. A shared byte-only first-word classifier preserves transaction
  drain admission and selects the replication parser for replication verbs.
  `SHOW` uses the existing SQL handler. This follows the SQL fallback in C
  `exec_replication_command` rather than treating every query as replication
  grammar.
- Function calls retain explicit schema qualification separately from the
  identifier name. Supported built-ins resolve in `pg_catalog`; a literal
  quoted function name containing a dot is not mistaken for qualification.
- Replication commands in failed SQL transactions are rejected with `25P02`
  before slot changes. `START_REPLICATION` in an explicit transaction fails
  with `25001`. Error handling marks the SQL transaction failed and reports the
  actual ReadyForQuery status instead of claiming idle. Successful slot commands
  also report the actual transaction status; an allowed DROP inside BEGIN must
  not report idle. The preexisting CREATE/export transaction restriction is now
  explicit in command validation, before slot dispatch.
- All 28 protocol tests pass. Three live connection tests pass independently of
  the still-failing captured-frame replay: SQL SELECT/SHOW/transactions on a
  logical connection, and transaction error/ReadyForQuery behavior followed by
  recovery with ROLLBACK, plus successful DROP and rejected CREATE/export in an
  explicit transaction. The native C subscriber now reaches the supported SQL
  parser and fails at the genuinely missing `set_config(text, text, boolean)`
  function. Search-path semantics, source catalog discovery, and native copy
  remain unfinished.
- C references for this slice include `repl_scanner.l:303`,
  `walsender.c:2184-2199` and `2230-2300`, and the secure search-path SQL in
  `common/connect.h`. Search-path planning also compared `guc_funcs.c`,
  `namespace.c`, and `guc_parameters.dat`; no no-op setting emulation has been
  added.
- Luna max and Sol high reviewed the SQL checkpoint; their transaction-status
  and function-qualification findings were addressed and tested. Actual C
  `pg_proc` and runtime queries confirmed that `pg_catalog.current_user()` and
  `session_user()` exist, while qualified `user()` and `current_catalog()` do
  not. The latter two are rejected without disabling the actual C functions.

The search-path implementation must preserve namespace semantics: an empty
explicit path still searches `pg_catalog` implicitly, hides unqualified public
tables, and has no default creation schema. Qualified `public.table` remains
accessible. Keep raw catalog storage lookup separate from SQL namespace
resolution so relation revalidation does not fail merely because public is
outside the path. Preserve schema and identifier boundaries in relation ASTs,
as for function ASTs, rather than splitting literal dotted identifiers.
Session settings must survive COMMIT and revert on ROLLBACK or statement
failure, and `current_schema()` must return NULL when appropriate. The C tree
marks `search_path` with GUC_REPORT, so its ParameterStatus messages also make
the pending client response fix a prerequisite for native setup. A setting
that returns success without these semantics is not a completed substitute.

### First implementation checkpoint (2026-10-24)

- Built and privately installed the unmodified C reference from
  `9c0909b34939f2d5a37b82b9d1efc8b08e310252`, advertising `20devel`.
  `t/interop/20devel-reference.hex` records raw C-generated pgoutput frames and
  the server executable checksum; `capture-reference.sql` regenerates them.
- Corrected ordinary transactional Message framing and relation-width old-key
  tuples. The worker now decodes N-only UPDATE, projects replica identity from
  the new tuple when needed, and accepts native plain TRUNCATE. A native
  TRUNCATE encoder exists; publisher dispatch remains to be wired as part of
  the explicit standard/extension boundary.
- Startup/SHOW version constants now advertise `20devel` / `200000`; `version()`
  derives its version from that same string. Runtime checks passed for both
  SHOW forms, `version()`, and libpq/psql's startup `SERVER_VERSION_NUM`.
- Explicit publication action options now parse, persist, and alter. The
  catalog codec distinguishes a missing legacy action list (all four defaults)
  from an explicitly empty list. Standard publication filtering is not yet
  wired into the sender; catalog configuration alone does not establish it.
- Added a separate Rove ASDF system, `pg-interop-tests`, without moving legacy
  tests. Its 23 tests pass, including decoding captured C frames, independent
  expected bytes, invalid flags, malformed messages, and unsupported tuple
  representations. `cl-spec/check-it` was not available in the session's ASDF
  registry; no generated-contract verification is claimed.
- The existing unit runner passed 1,594 checks after publication changes.
  `t/logical-replication.sh` passed again after grouped-TRUNCATE and version
  changes; `t/psql-smoke.sh` passed. The latest forced compilation also passed.
- Luna max and Sol high reviewed the codec checkpoint. Sol medium reviewed
  refactoring: shared TRUNCATE apply boundaries were centralized in a helper;
  changed production functions remain below 120 lines. Sol high requested a
  live dispatch/apply regression, now under `pg-interop-tests/apply`. That test
  verified six original C transactions through dispatch, including observer
  visibility before and after each Commit and matching durable progress.
   The final rollback check exposed rejection of valid ParameterStatus messages;
   the native slot checkpoint above records its fix and passing rerun. This
   fixture replay is a decoder/apply regression, not a replacement for either
   native topology.
- `t/logical-interop.sh` supports separate directions and independent fresh
  topologies for `both`, with explicit C installation/revision prerequisites
  and failure artifacts. It is a failing acceptance scaffold, not acceptance:
  recovery and concurrent-copy gates are deliberately not yet implemented.
  The native publisher-direction run now passes publication `WITH (publish=...)`
  setup and stops while clearing the source search path. The opposite run stops
  because the worker's standard-mode options are not implemented.
- A separate native `CREATE SUBSCRIPTION` attempt with an existing publication
  failed while clearing the source search path. C sends
  `SELECT pg_catalog.set_config('search_path', '', false);` on its logical
  replication connection (`libpqwalreceiver.c`, `common/connect.h`); pg-lisp
  currently routes that SQL to its replication-command parser. Catalog
  discovery, COPY, native slot snapshots, initial-copy handoff, and recovery
  in both directions remain unverified and incomplete.

Run the codec system through ASDF `test-op` with Rove available. The live apply
system additionally requires `PG_INTEROP_TARGET_PORT` pointing to an isolated,
fresh pg-lisp server; it must not target an existing database. Run native
acceptance with, for example:

```sh
C_PG_BINDIR=/path/to/reference/install/bin \
C_PG_REVISION=9c0909b34939f2d5a37b82b9d1efc8b08e310252 \
  ./t/logical-interop.sh both
```

If a staged installation needs a library search path, point `LD_LIBRARY_PATH`
at that same installation's `lib` directory. Missing peers and failed gates
fail; neither is a skipped passing interoperability test.

### Original planning assessment

This document records source inspection and a proposed implementation plan.
The historical unit counts in `LOGICAL-REPLICATION.md` are not independent C-peer
results. No runtime interoperability or HA test was run for this assessment;
`pg_config` was not available on the planning session's PATH. Supplying or
building the recorded C reference peer is the first harness prerequisite.

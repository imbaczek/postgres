# Completed development work

[Active TODO](TODO.md) · [Benchmarking](BENCHMARKING.md)

## Summary

- **Linux portability.** Extracted process identity, PGDATA ownership, common
  file opens and numeric parsing. macOS and Windows work remains in the TODO.
- **Native formats and execution.** Audited current producers, retired legacy
  formats, and consolidated reader, portal, WAL and metadata ownership paths.
- **P01–P02, observability.** Added statement fingerprints and counters,
  resource and wait diagnostics, and richer EXPLAIN ANALYZE and activity views.
- **P03–P04, indexes.** Added composite B-tree bounds and ordered scans, plus
  partial and expression indexes with DML, recovery and supported native
  logical replication coverage.
- **P05–P06, query execution and planning.** Added bounded spilling hash joins,
  including outer joins, and improved selectivity, width and scan/join costing.
- **P07, transactional catalogs.** Introduced transaction-local catalog
  overlays, atomic row/catalog publication and native transactional logical DDL
  for the documented subset.
- **P08–P09, types and column DDL.** Added stored Boolean, UUID and common
  arrays. Implemented stable column identities, transactional column changes
  and selected ALTER TYPE rewrites.
- **P10, foreign keys.** Added text COPY FROM STDIN, referenced-key locking,
  immediate and deferred checks, validation and referential actions. Qualified
  inserts and cascades on the supported performance matrix.
- **P11, views.** Added persistent read-only views with definition-time
  bindings, transactional replacement and drop, planner and portal integration,
  introspection and physical replay.
- **Storage, replication and HA.** Completed the recorded WAL publication,
  acknowledgment and shutdown work, retained-root ownership checks, and
  reseed preparation and handoff milestones. See the [HA roadmap](PHYSICAL-HA.md)
  for the remaining work.
- **Verification and compatibility.** Fixed fixture ownership and cold-load
  issues, qualified supported protocol and SQL behavior, and documented
  completed HA ownership boundaries.

## Implementation and verification records

### Linux portability

- [x] Extract process identity and PGDATA ownership on Linux first. Then route
  shared file opens and numeric parsing through the port. Preserve current
  behavior and add focused coverage for PID reuse, lock revocation, and
  descriptor inheritance where coverage is missing.

### Native format cutovers and implementation notes

The B18 live regression in `t/promotion-commit.sh` changes just the source
namespace of qualified pending-promotion control and recomputes its CRC.
Startup refused the cluster before PID repair and signal cleanup. The stale
PID and 30 storage files retained their original contents, and the promotion
signal remained in place. Starting from the unchanged qualified fixture
completed promotion and served SQL as a timeline 2 primary. Separate live
namespace coverage verified retired filename refusal and qualified restart. The archive is
`/home/imbaczek/proj/pg-ha-artifacts/architecture-closeout/b18/`.

For B15, `0dbafd57add` removes the ownerless CLOG staging, materialization,
and synchronous wrapper after migrating their last Rove callers. Owned
completions retain exact tag validation. The migrated tests cover stale
incarnations and timelines, FullXID aliases, orphaned queue owners, and
retry after a failed raw CLOG write. The accepted-publication property covers
logical visibility with raw bits still in progress, including neighboring
writes and fsync. The live `t/commit-receipts.py` and `t/async-commit.py`
scenarios passed, including failure cleanup and restart.
The strict CLOG selection passed seven test heads. The combined unit suite
also passed, including 1,172 legacy checks with zero failures.

For B1, native record decoding and physical receive use one header predicate
for record magic, version, type and source LSN. Both require WAL8, with a
regression covering checksummed WAL2–7 records in both readers. Physical
test streams formerly using empty WAL5 checkpoints now use current
checkpoint payloads and derive expected LSNs from their lengths. Source
retention, history proof, retained inventory overlap and the restartpoint suffix and seal
fixtures now construct WAL8 checkpoints. The source fixture binds the
matching in-memory checkpoint snapshot and keeps its record end at a
segment boundary. Retained extent and inventory fixtures derive their
start, fork, and end from the WAL8 checkpoint and end-of-recovery sizes.
Retained projection fixtures also use WAL8 for overlap, fork clipping,
multi-timeline carrier selection, and metadata budget checks.
The historical archive fixture uses WAL8 for the retained base prefix
and parent tip, including archive-only reads and startup slot checks.
The cold continuation proof changes a native commit timestamp and checks
that policy and pending state remain equal while the raw transcript changes.
The cold proof and ten targeted physical test heads passed after the cutover.
The page-image decoder now requires fork and contributor metadata. Retained
logical progress derives its first LSNs from explicit FullXID contributors.
Both page spool readers validate the WAL8 version before decoding images.
The native SQL promotion contract passed with fresh source startup, physical
base backup, streaming follow and standby promotion.
The full WAL crash suite passed after the fixture primed an XID reservation
with a rolled-back insert and checked the final checkpoint against its
control snapshot before injecting corruption.
Checkpoint WAL validation now requires a nonempty WAL8 record, and base
reservation seeding requires its exact source counters.
Source transaction replay now follows WAL8 transitions. End-of-recovery
keeps the proven reservation bound but clears completeness until the child
checkpoint. End-of-recovery and CLOG reset clear source membership.
Running-xacts, XID reservation, end-of-recovery, relation reset, and policy
terminal validators require WAL8. The follow-anchor test rejects a
checksummed WAL7 transition while preserving the current digest vectors.
Primary scan still recognizes retired transition headers so tail repair
cannot truncate them as ordinary records.
The checkpoint process suite rejects a checksummed WAL7 checkpoint before
exercising native crash, replay and restartpoint behavior on a separate copy.
The promotion-resume backlog fixture appends a complete WAL8 maintenance
batch. The live run rejected the unreplayed batch with the expected
diagnostic and preserved the storage snapshot. Its independent archive
oracle rejects checksummed WAL7 and accepts the current checkpoint extensions.
The resource inspector checks offsets across recovery policy trailers and
slot evidence. Twenty inspector tests and the full promotion-resume process
suite passed.

For B2, primary WAL control and the independent checkpoint reader require
PGWAL6. The checksummed PGWAL2–5 fixtures are rejected at decode, and the
live checkpoint suite rejects a stopped copy with PGWAL5 before exercising
PGWAL6 crash recovery and standby restartpoints. That fixture exercised both
segment namespaces before B18. The current lifecycle, retained, resource,
logical evidence, page checksum and CLOG extent fields remain covered.

For B3, the checkpoint codec and independent inspection tools require
PGCKPT02. The base snapshot can still omit optional facts. Exhausted
FullXIDs, resource facts, commit timestamp counters, Boolean policy facts
and logical ON intervals retain their existing encodings.
The live checkpoint suite independently constructs an 88-byte PGCKPT01
payload with no extension flags and matching record coordinates. A
checksummed PGWAL6 envelope carrying that payload fails at the nested
snapshot decoder, and the rejected control bytes remain unchanged.
The original current-format image still replays and reaches restartpoint.

For B4, buffer reads no longer move a historical B-tree type byte out of
the checksum field. The checksum reader rejects that layout without
rewriting the page. Indexes from that layout must be rebuilt from matching
data before using the current reader.
The live index regression manufactures an unstamped metapage in the
retired layout while the fixture is stopped. The cold server rejects the
indexed lookup with XX001 for the exact relfilenode and block zero. The
index file remains byte-identical across the failed read and shutdown.
Restoring the current file and restarting yields the expected indexed row.

For B5, catalog decode requires a stored publication OID. Startup rejects
missing OIDs without rewriting the catalog. The publication process test
checks that stored OIDs survive a crash and restart, ALTER preserves them,
and dropping and recreating a publication allocates a new OID.

For B6, catalog decode requires a stored, nonempty cluster identity.
Fresh cluster construction still generates and persists an identity,
and existing identities still use the same system identifier projection.
The read path no longer generates a replacement identity.
Replay, adoption and slot checks compare stored identities directly.
The live publication fixture checks startup rejection of missing and
empty identities, and compares every fixture file after each rejected
catalog. It verifies the stored identity through the SQL builtin before
and after crash recovery and a later clean restart.

For B7, logical slot decode rejects PGSLOT1–3. PGSLOT4/5 remain supported
because current writers emit them, alongside the provenance and child owner
formats. Continuation fixtures use noncanonical PGSLOT4 values to verify
that inventory and no-op proof paths preserve the original bytes.
The live logical capture fixture derives checksummed PGSLOT1–5 images
from a SQL-created PGSLOT6 slot's durable coordinates. Cold startup rejects
PGSLOT1–3 without changing their bytes, restores PGSLOT4 and PGSLOT5
without rewriting them, and then restores the original PGSLOT6 slot and
the recovered heap row.

For B8, the bootstrap pin reader requires PGPHYSPIN2 and rejects a
checksummed PGPHYSPIN1 pin without rewriting it. PGPHYSPIN2 keeps both
manifest-bound and unbound encodings. Follow pins use their own codec.
The live source fixture derives a checksummed PGPHYSPIN1 file from a
current pin's identity and WAL coordinates. Startup rejects that exact
file without rewriting it. Restoring PGPHYSPIN2 makes the pin available
again and preserves all 1,800 source rows.

For B9, reopening the receive store requires PGPHYS03 with a recorded floor
and digest seed. A checksummed PGPHYS02 image is rejected without rewriting
it. PGPHYS03 still supports control with no history selection, with history
selection, and with a retained-root identity.
The live physical replay fixture projects an unreclaimed receive store
into the checksummed 52-byte PGPHYS02 format. A cold standby rejects it
without rewriting it. Restoring PGPHYS03 permits an owned restart,
successful replay, and recovery across the restartpoint crash cuts.
The physical store unit tests cover rejected v2 bytes and writable v3 reopen.
The obsolete restartpoint test expecting in-place v2 migration was removed.

For B10, startup requires PGRSTAT3 for ordinary recovery or PGRSTAT4 for
pending promotion. The signal harness checks rejection of both retired
formats, preservation of their bytes and recovery data, and successful restart
with the restored current control.
The recovery-control unit test round-trips v3 across both ordinary phases
and rejects both checksummed historical layouts.

For B11, the backup and recovery readers require a version 3 manifest with
an explicit checkpoint start. The schema rejects versions 1 and 2, missing
checkpoint starts, and inconsistent checkpoint boundaries.
The live bootstrap test generates checksummed v1 and v2 manifests with
their historical field sets and a v3 manifest without its checkpoint
start. Startup rejects all three at manifest validation. The test compares
the supplied manifest and all other backup files byte for byte after
each rejection, restores v3, and verifies startup in recovery.

For B12, `64002cfb17e` requires PGRPGEN2 and metadata version 4. The
`t/xid-reservation.sh` fixture exercises startup rejection of retired
generations and checks that their control files are unchanged.

For B13, restart the Lisp server across the gate representation cutover in
`87b987c4be0`.
The admission gate now closes and reopens with the current quiescence owner
token. The load-time bridge for ownerless gates has been removed. Six current
strict physical tests passed, including borrowed admission, unwind, canceled
reload drain and the terminal shutdown fence.

For B14, restart the Lisp server across the backend layout change in
`509bc1c6992`. Commit preflights logical row changes through the transaction
spool. The live capture fixture passed spill cleanup on rollback and disconnect,
restart recovery, and rejection of corruption after the first durable fragment.

For B17, restart the Lisp server across the portal and statement layout
change in `0ebd131b2eb`. Portal result metadata and prepared result signatures
live on their owners. The SELECT and DML execution registries track executable
portal state for cleanup and suspension. The live portal lifecycle fixture
passed snapshot retention, reload exclusion and cleanup after suspension.
Bind now constructs the portal directly. Both aborted Describe regressions
passed, and the live C comparison matched Bind, Describe and DataRow frames
and the result-type drift diagnostic for Bind and statement Describe.

The first A1 slice shares physical row projection between the callback and
iterator drivers. It preserves deferred sort expressions, ProjectSet
pass-through rows, and origin capture through both entry points.
The WindowAgg drivers share partition resets, ranking, aggregate transitions,
and transition origins. The window differential covers one-row portal
resumption for ROWS prefixes, RANGE peer groups, and whole partitions.
The callback and iterator drivers also share the streaming DISTINCT and
aggregate node dispatcher. This keeps the projected-value input for hash
DISTINCT and the output row shape in one implementation.
The DML differential exercises ordinary SELECT and resumed DISTINCT
and aggregate portals, with the driver still owning source cleanup.
Both drivers use the same ProjectSet postponement rule for sort and
top-N. A sort key that depends on a generated value keeps expansion
before sorting. The live DML harness checks set-returning target values,
volatility, LIMIT, and portal resumption against PostgreSQL C.
Sort and top-N now use the same buffered node dispatch and child layout
in both drivers. Deferred projection still runs as each driver delivers
a sorted row, including after portal suspension.
Both join drivers share pair qualification, including the combined row,
datum origins and source presence visible to the ON expression. Replay,
match tracking and null extension remain in the driver state machines.
Both drivers identify LATERAL and parameterized index inner inputs from
the physical plan. Deciding whether to rescan no longer constructs a scan
path. Index handle validation still runs during admission and scan setup.
Unmatched rows share the null-extension routine. It preserves the
surviving side's source presence and builds the padded datum origins
directly, with no temporary padding vectors.
The live TOAST suite passed origin-sensitive inserts through LEFT,
RIGHT, FULL, and LATERAL joins. The DML differential passed one-row
portal resumption through outer joins.
Callback execution and portals derive full-value TOAST demand and
direct slice access through the same function. The callback clears
scan-local access before delivery, and each portal pull restores its
captured read context.
SELECT and UPDATE share the conservative TOAST demand expression walk.
Each caller retains its accepted expression forms and column-slot policy.
UPDATE still maps OLD and NEW in RETURNING to the underlying table
columns. The SELECT probe uses the physical IndexScan parameter accessor.

The first A3 slice registers the four JSON and JSONB inspection functions.
Analysis, runtime dispatch, aggregate and CTE volatility checks, and HAVING
folding read the same signature, strictness, and volatility descriptors.

For A7, UPDATE and ON CONFLICT retain their own claim and expression
evaluation paths. Once the old tuple is claimed and deleted, a shared
replacement operation handles TOAST reuse, successor linking, and logical
update publication. UPDATE still supplies its lazy TOAST demand and returns
the matched source row through its own RETURNING path.

The first A8 slice moves SQL type OIDs and lengths into
`utils/adt/sql-types.lisp`. The analyzer, catalog, extended query protocol,
and pgoutput paths share those identities. The pgoutput encoder and
subscriber share their supported type set while retaining their own codecs.
INSERT and UPDATE assignments share the varchar length rule, including
space-only excess and SQLSTATE 22001. Explicit casts keep their own truncation
behavior.

The first A9 slice shares the local plan-expression walk for sort volatility
and ProjectSet dependencies. Calls and whole rows expose executable inputs,
subplans expose their outer test, and constants are leaves. The ProjectSet
visitor separately resolves references to projected sort outputs.

The first A13 slice shares SELECT plan admission across simple execution,
PERFORM, COPY TO, EXPLAIN, and extended portal initialization. Admission
registers and revalidates relation readers before reconciling the physical
access paths. Portal initialization still owns its snapshot, table retention,
index leases, and resumable iterator.
Simple SELECT, portal initialization, and extended-protocol metadata now
derive their result columns through the same preparation entry point. Parse
and Bind still retain readers before returning, and statement Describe
recomputes and checks the prepared statement's result signature.
SELECT and RETURNING Bind share execution-owner bindings, registry publication,
and failure cleanup. SELECT keeps its recovery check and snapshot acquisition
before plan admission. RETURNING retains its transaction and reader setup.
Eighty-five strict tests and the live portal lifecycle suite passed.

The B-tree point search, resumable equality cursor, and insert routing share
one internal-page child range. Equality visits the preceding child and every
child in an equal separator run. Insert uses the right endpoint. The range
calculation examines the full page because the first separator can be stale.
Each caller keeps its existing relation lock or read lease.
The Rove separator regression, legacy duplicate-key scan, and all 12 focused
read-lease tests passed. The SQL index regression checks indexed results
against sequential results and verifies the DROP INDEX handoff using the
reader's canonical relation-lock owner.

The pgoutput subscriber decodes complete socket messages and in-memory replay
through the same grammar. Stream fields retain their bounded spill and cleanup
rules. In-memory text and binary fields own their values. The independent
INSERT fixture uses the C tuple field order and checks null, unchanged TOAST,
UTF-8 text, and binary data after overwriting the input bytes.
The test-only INSERT, UPDATE, and DELETE adapters and their duplicate tuple
framing code have been retired. Malformed packet and C wire fixtures now
exercise the shared operation decoder. Apply tests pass decoded operations
directly to the apply entry point.
Control tests also use the shared decoder or the production CopyData reader.
The malformed keepalive fixture verifies connection closure without advancing
feedback. The duplicate native TRUNCATE apply adapter has no callers and was
removed.
Independent Relation, Message, and native TRUNCATE fixtures now use the
operation decoder with the subscriber mode and transaction state appropriate
to each frame. Their dedicated read adapters have been removed.

The startup settings registry uses named, read-only descriptors for context,
kind, boot value, integer bounds, and units. Configuration ingestion, reload,
SQL scope checks, `pg_settings`, and `--describe-config` read that registry.
Session and startup integer settings share the same bounds and units parser.

Virtual catalog descriptors supply OIDs, relation kinds, row producers, and
field definitions to the executor and catalog views. They derive the
`pg_class.relnatts` value from the highest original attribute number.
Each relation lookup still builds fresh metadata with dense executor slots,
and RowDescription translates those slots to the original attribute numbers.

History and redo share batch discard and staging checks for pages, logical
changes, logical catalog events, and catalog chunks. History still certifies
facts from complete terminals. Redo applies resets, then page images, then
the terminal effect. A replacement batch marker discards the prior incomplete
batch, including its staged catalog chunks.
Both consumers also validate complete native terminals through one decoder.
Catalog chunks are reassembled before either consumer uses the payload.
Selected end-of-recovery records follow their separate timeline transition.
Catalog chunks use a recovery-owned spool that spills at the existing memory
threshold. The terminal checks the observed chunk count and byte total before
allocating the contiguous catalog image. Recovery releases scratch files on a
replacement marker, at the terminal, or when the scan exits.
History and redo borrow the image bytes from that payload while consuming it.
Recovery retries pending scratch cleanup if an earlier close was interrupted
by an unlink failure.

`storage/wal-recovery.lisp` owns staging, terminal application, and primary
recovery. ASDF loads it after the temporary spool implementation and before
physical recovery and startup. `storage/wal-record.lisp` owns native framing,
shared record types, integer fields, and CRC32C dispatch. WAL storage declares
the codec component as a dependency, and the standalone CRC qualification
compiles the complete codec directly.

For A18, the `pg-unit-tests/all` ASDF test operation runs the Rove and legacy
unit harnesses in order. The coverage-aware shell wrapper delegates to that
operation. Direct Rove head selection and its campaign receipts use the
existing Rove-only system. The
[test inventory](t/TEST-INVENTORY.md#running-and-failure-behavior) documents
both entry points. The combined shell run reached the final success line after
Rove passed and the legacy harness reported 1,172 checks with zero failures.
An isolated run with `rove:run` returning false exited with status 1 before
the legacy harness or combined success line ran.

For B18, new clusters and backups of those clusters use timeline-qualified
WAL files. The bootstrap harness checks the control namespace and filename
grammar on both source and copy. The live bootstrap and namespace suites
passed, including byte preservation through migration crashes and a
legacy-base standby following a migrated primary. The namespace suite and
restartpoint fixtures share a stopped, TLI-1 legacy conversion that checks
the control coordinates and WAL bytes. The restartpoint process suite passed
with each source namespace, including startup refusal of an unproven pending
promotion with its control extensions intact. Startup still reads the namespace in
existing controls. The physical slot API fixtures use timeline-qualified WAL,
with an explicit legacy namespace mutation for the recycling revalidation case.
The archive rotation and backlog fixtures seed and publish timeline-qualified WAL.
The shared physical source fixture now also seeds timeline-qualified WAL for
streaming, pin retention and archive tests. The stream revalidation test changes
the namespace in flight to check that stale reads and feedback are rejected.
The logical recovery base fixtures use timeline-qualified checkpoint files and
matching WAL controls, including the case with a separate witness segment.
Restartpoint and promotion fixtures use qualified base files by default.
The live restartpoint seed also defaults to qualified WAL. Live restartpoint
tests passed with the new default and explicit legacy selection. The default
seed also passed the online promotion crash suite.
The automatic promotion commit suite passed with both seed namespaces. Its
owned fixture installs startup resource capacity and verifies durable follow
cleanup transfer before restarting the crashed process. The oversized
checkpoint fixture appends one byte to the exact staged record and checks
startup refusal without file mutation.
The automatic promotion commit matrix now defaults to qualified WAL for the
full set of crash cuts. The live matrix passed using the qualified seed.
Retained overlap, inventory, and projection fixtures also use qualified WAL
for ordinary success. Fifty selected strict heads passed.
Promotion fixtures pass the requested namespace to base setup, including
explicit legacy cases, so the base is written under the selected name from
the start.
Clean-primary adoption fixtures write qualified base WAL and carry the verified
namespace through the durable attempt record and staged backup. Shared setup
creates the CLOG file required for an owned restart of the source.
The WAL control encoder defaults to the timeline namespace. Production callers
pass the current, source, or child namespace explicitly, and historical fixtures
request legacy control images explicitly where they create legacy-named WAL.
Fresh Lisp images also start with the timeline namespace. The cursor fencing
fixtures inject legacy as the mismatched namespace in memory and on disk.
Sixteen strict namespace heads passed. The combined unit suite also passed,
including all 1,172 legacy checks.
The export-origin fixtures bind the current timeline-qualified namespace for
the WAL and sender. All 17 strict heads passed, including stale timeline
rejection and retained progress across publication failure.
The retained-reference control and adoption-attempt fixtures likewise write
timeline-qualified controls. Twelve strict heads passed, including startup
refusal without rewriting the retained reference and adoption journal
round trips and publication cuts.
The same-timeline follow source and resource WAL fixtures use qualified
names. Forty-six strict heads passed, including real-WAL anchor proof and
reopen, rejection of unsupported legacy timelines, and XID reservation and
resource publication failure boundaries.
The archive transition fixture now supplies a timeline-qualified ancestor
partial and matching proof when end-of-recovery rotates into the next
segment. Three strict archive heads passed, covering transition evidence,
history discovery, and bounded partial restore.
Promotion now produces timeline-qualified ancestor partials and proofs even
when source WAL itself uses the legacy namespace. Four strict heads passed,
covering byte-identical retries, conflicting bytes and hard links, committed
child matching, and proof retry after child WAL recycling. Previously staged
legacy partials still use the existing reader and archiver validation.
The live standby archive scenario passed with Patroni 4.1.5 and the matching
C client library. It covered cold restart, archive retries, promoted-writer
handoff, and an exact timeline-qualified fork proof after a second promotion.
Archive discovery, proof matching, and restore now require timeline-qualified
promotion partials. Six strict heads passed. They include a valid legacy
partial and proof that cannot advance receipt, exclusion of legacy partials
from the archiver queue, and qualified restore only at the exact fork.
The live Patroni archive-descendant observer replayed the child-only row
from the archive and retained it across restart.
Primary startup now requires qualified WAL when archiving is enabled.
Legacy primaries can still migrate with archiving disabled. Both archive
modes refused legacy startup with unchanged control bytes and WAL inventory.
The full namespace migration suite passed, including crash retries,
continued follow, and qualified bootstrap. Five strict control and archive
heads passed.
The receiver requests only qualified complete segments and partials.
Its restart cutoff uses the timeline at the durable receive floor. Six strict
heads passed, including refusal to advance receipt from a proved legacy
complete segment and admission of the same bytes under the qualified name.
The restore helper has no legacy namespace argument. Five strict heads
passed, covering proof validation, qualified receipt, and migration of
retained legacy proofs to qualified names.
Standby storage requires the WAL namespace established by verified recovery
preparation. A cold process with that field missing refused before rotating
storage or writing control and succeeded after the verified field was
restored. Both strict cold recovery heads and live static standby passed.
Base and retained overlap capture also require that verified namespace.
The boundary fixture rejected missing namespace even with a valid legacy
file present, then captured the same bytes under explicit legacy provenance.
Base overlap capture and retained overlap metadata now require the qualified
namespace. An independently enveloped legacy overlap with a valid checksum
was rejected. Fifty-six strict overlap, inventory, projection, and prefix heads
and three archived segment and cursor heads passed.
The archive migration test selects legacy seed files explicitly, and promotion
tests still cover legacy parent proofs. A read-only inventory of the repository
tree and the documented
`../pg-ha-artifacts` directory found no native WAL segments, partial files,
proofs or archive status entries in either filename grammar. A read-only scan
of the wider project directory found no 16-hex or 24-hex native WAL files and
no primary WAL control files. All 47 physical backup manifests had valid
envelope CRC32C, version 3 payloads, and 16-hex WAL entries. They were all
under `retired-logs`, with no listed WAL files or accompanying
`global/pg_wal_control`. Preserve those manifests as historical evidence.
Data outside the project directory was outside the scan.

The C reference is `XLogFileName` and `IsXLogFileName` in
`src/include/access/xlog_internal.h`. Native WAL retains its `.wal` suffix.
The cutover covered startup control, recovery base admission, and promotion
source namespace fields. Positive fixtures use qualified names. Independent
legacy bytes exercise rejection before control writes, file renames, or
changes to the selected timeline.
The serving startup check runs under PGDATA ownership before PID repair or
startup reconciliation in ordinary, imported, and single-user startup. Live
ordinary and single-user refusal preserved the entire stopped fixture and
stale PID, including independent legacy control and dangling filename cases.
Qualified startup committed a row and recovered both rows after restart.
The previous migration crash matrix is pinned at `b91e9f92423`.
Backup manifests and bundle intake require qualified WAL file entries. A
checksummed version 3 manifest with a legacy entry reached file-entry
rejection. Eighty-three strict recovery and promotion heads, three cold
policy heads, and live qualified bootstrap passed.
Pending promotion control requires a timeline-qualified source in the server
codec and the standalone `pg_ctl` reader. The control layout stays at
`PGRSTAT4`. A CRC-valid legacy namespace is rejected in both readers.
All 117 focused strict physical heads and all 1,639 unit heads passed.
The primary control codec and `pg_ctl` also require the `timeline` namespace.
WAL initialization refuses legacy segment names before replacing the storage
incarnation. The serving build removes the migration command and startup alias
reconciliation. The live cutover check confirmed legacy refusal, unchanged
storage and stale PID, and successful qualified restart. The cutover in
`d43e943a27c` passed 1,640 unit heads, 136 strict physical heads, and the
live promotion crash and restart matrix. The
[legacy primary cutover procedure](PHYSICAL-HA.md#legacy-primary-cutover)
specifies the tested migration build, clean restart and standby reseeding.
Use a cold start for the cutover. Direct recovery open checks the prepared
namespace and legacy segment names before installing runtime state. Follow
reservation rejects legacy sources before allocating a pin, and adoption
journal admission requires qualified local facts. The focused strict suite
passed 41 heads across these boundaries.
Archive proof admission and publication require timeline-qualified identity.
Legacy archive status markers stay untouched when qualified status is reclaimed.
Five strict physical heads covered proof rejection, archived partials, the
restore cursor, and cold recovery. A separate recycling test verified the
legacy marker bytes. The live standby restored 2,006 rows from archive while
the primary was down, then resumed streaming on its existing follow slot.
The B18 milestone review traced the serving readers and writers for startup,
backup, follow, archive and promotion through their qualified namespace
checks. The [current format table](PHYSICAL-HA.md#supported-native-formats)
records the archive proof and filename contract alongside the receive spool.

For B16, the three `collect-select-*-rows` wrappers had no production callers.
The joined collector was the only caller of `stream-select-joined-rows`, which
was itself the only caller of `stream-select-joined-environments`. The
FROM-less collector had one legacy test caller. Its regression now executes a
live plan. Cursor execution, plan execution and DML still share
`make-select-result-row`, and live join operators retain their streaming
helpers.

## Query observability

### P01 — Statement statistics

Expose cumulative statistics for supported SELECT, INSERT, UPDATE, and
DELETE through a native `pg_stat_statements` virtual relation and built-in
functions. An operator should be able to rank query shapes by total
execution time and compare the same workload across revisions. Extension
packaging can follow in P22. First-release statistics are volatile across
restart.

- [x] Pin the SQL field and setting contract against C PostgreSQL. Build a
  versioned, literal-normalized fingerprint from the analyzed query, using
  resolved object, operator, function, and type identities. Test grouping
  and reject unhandled node kinds. Keep workload identity, physical plan
  identity, and implementation revision independent.
  The fingerprint visitor covers simple SELECT shapes and their analyzed
  constants, columns, casts, parameters, and binary operators. It omits
  literal values and physical access paths. Unhandled analyzed forms
  return no identifier pending traversal coverage.
  A Rove fixture injects an unhandled analyzed node into SELECT and
  a DML assignment and verifies that neither gets an identifier.
  The wire fixture separates different target and predicate attribute
  positions, groups literal and alias variants of the same native
  relation, and creates a new group when that table is dropped and
  recreated. Native keys use resolved relation OIDs. The pinned C
  peer jumbles the expanded range-table reference name instead,
  which distinguishes the alias variant and retains the group
  across a same-name recreation. Numeric query IDs are native to
  each implementation.
  DML identity now covers target relation and column OIDs or attribute
  numbers, typed assignments, predicates, and RETURNING for supported
  INSERT, UPDATE, and DELETE forms.
  Ordered and aggregate SELECT fingerprints use resolved sort keys and
  aggregate expressions. Aggregate argument identity is independent of
  whether the planner shares a transition. HAVING fingerprints preserve
  the analyzed WHERE and HAVING boundaries before predicate pushdown.
  Non-correlated FROM subqueries contribute their resolved query shape.
  Literals and aliases are normalized at both query levels.
  Joins contribute their analyzed types and predicates in source order,
  independently of the physical join plan. Correlated LATERAL sources
  preserve the referenced outer slot and its scope depth. The wire
  regression distinguishes an outer reference from a literal operand.
  Scalar builtin calls contribute their resolved names and typed inputs.
  CASE, COALESCE, Boolean tests, unary operators, array constructors,
  subscripts, slices, and quantified comparisons contribute their
  analyzed operands. The wire regression verifies that literal variants
  of a CASE group together and that omitted array slice bounds remain
  distinguishable from explicit bounds.
  IN lists include their resolved array or per-item comparisons and
  preserve NOT polarity. The wire regression checks literal grouping
  and separation by polarity and comparison type. As in the C query
  jumble, an array of two or more literal or external-parameter elements
  groups across lengths. Representative text still shows the first
  captured shape's placeholders individually.
  Window output fingerprints include the analyzed function, partition,
  ordering, and frame. Shared pre-window slots carry their analyzed
  expressions so slot assignment does not determine query identity.
  The wire regression checks literal grouping and window sort direction.
  Set-returning builtin calls and record fields use the analyzed call
  and field position, independent of generated projection slots. The
  wire regression groups two activity lookups with different PID literals
  and counts one output row across both calls.
  Set operations contribute their operation kind, both analyzed branches,
  and final ordering and limit shape. The wire regression verifies that
  UNION ALL groups across literal changes and has separate totals from UNION.
  VALUES queries retain their source kind through analysis, so literal
  variants group together without merging into SELECT.
  SELECT expression subqueries contribute their kind, analyzed child, and
  comparison test when present. SELECT identity is captured before physical
  planning can simplify a child. The wire regression checks that EXISTS
  groups literal variants and distinguishes an explicit child cast even
  when the planner removes the child projection.
  SELECT fingerprints include WITH definitions and resolve references
  by lexical scope and definition position. Recursive worktable references
  do not expand their own definitions. CTE names and explicit materialization
  directives follow the C query jumble contract. The wire regression
  covers literal grouping, MATERIALIZED, recursive execution, and the
  presence of both identifiers in recursive EXPLAIN JSON.
  DML shares the same lexical WITH traversal. The wire regression checks
  that INSERT ... SELECT from a CTE groups literal variants and records
  the exact RETURNING row count.
  DML captures identity before planning as well. A joined DML source
  contributes its analyzed query shape. The wire regression checks exact
  plans, calls, and affected rows for UPDATE FROM a derived source and
  DELETE with a scalar subquery.
  INSERT ... ON CONFLICT includes the analyzed conflict action, arbiter
  specification, assignments, and qualifications. Inferred physical index
  choices do not affect identity. The wire regression distinguishes
  DO NOTHING from DO UPDATE and checks both affected-row totals.
- [x] Wire one observation into the simple-query and extended-query execution
  owners. Record successful planning and execution independently and track
  errors and cancellations separately. Preserve top-level status and
  backend-local counters across portal fetches. Exclude suspended time and
  count streamed DML RETURNING once. Make completion idempotent through
  close, error, cancellation, and disconnect.
  The observation API records successful planning independently and
  accumulates only active execution slices. Tests cover reset during
  suspension, unwind, and repeated completion. Simple and extended SELECT
  share observation ownership through the portal. A wire test exercised a
  suspended and resumed SELECT alongside a simple SELECT in one group.
  Collection is opt-in at startup with `pg_stat_statements.enabled=on`.
  Inline DO classifies its PERFORM statements as nested independently
  of the outer command's observation. With tracking set to `top`, no
  PERFORM entry is published. With `all`, simple and extended DO together
  publish four nested plans, calls, and rows in one literal-normalized
  group. A direct SELECT of the same shape is a separate top-level group.
  The pinned C peer matched the wire fixture with utility tracking off.
  Ordinary DML shares the resolved preparation with the identity visitor.
  A wire test verified affected-row counts and a unique violation that
  contributed only a successful planning phase. The extended RETURNING
  portal publishes one execution when it materializes the result and
  counts all affected rows. [The SQL wire regression](t/statement-statistics.py)
  checks partial fetches and early close from a second session. It also
  checks representative query text from both frontend protocols.
  Closing DML RETURNING before Execute records only planning and leaves
  the table intact. A duplicate-key failure during Execute records one
  execution error, with no successful call or returned rows.
  Rolling back a suspended SELECT discards its partial execution. An
  ordinary Close publishes the rows fetched so far, and both paths retain
  a successful planning phase.
  A fully fetched extended SELECT publishes at successful Execute completion.
  Its counts are visible from another session before transaction end and
  survive a subsequent rollback.
  Disconnecting a suspended portal also retains only planning and removes
  the backend's activity row. The wire regression verifies that a clean
  restart clears prior entries and reset metadata while preserving the
  queried table.
  A wire case enters the executable-owner reload gate with a suspended
  portal. It verifies that admission reopens while reload waits for the
  owner and that the portal's activity row is still visible while waiting.
  Rollback lets the drain finish. A subsequent live code reload preserves
  the planning-only entry and the connected backend's idle activity row.
- [x] Bound query entries and representative text with configurable limits
  and a documented eviction policy. Use short sharded locks, detached read
  snapshots, and online timing summaries. Reacquire entries by key after
  eviction. Give reset a precise ordering with phase completion. Following
  the pinned C peer, a successful phase stored after reset contributes to
  the current entry even if the phase began earlier.
  The aggregation core now divides entry and text budgets across FIFO
  shards, copies detached snapshots under individual shard locks, and
  clears all shards under ordered locks. It charges four bytes per
  retained text character to the shard's budget. A snapshot can span a
  concurrent reset across shards. Planning and execution have separate online timing
  accumulators.
  Mutations to the shard hash, FIFO links, and byte accounting mask
  asynchronous backend interrupts. Terminal observation status and its
  cumulative update are published within the same interrupt mask.
  A one-entry wire fixture verifies FIFO eviction, dropping over-budget
  representative text while retaining counters, reacquiring an evicted
  identity, and resetting the eviction count. A wire case resets a
  suspended SELECT's planning entry, then four clients record 128 calls
  and 128 rows in one group while the portal remains suspended. Resuming
  the original portal publishes exactly one call and both of its rows.
- [x] Wire the supported relation, reset and information functions, and
  settings through the catalog and configuration registry. Document field
  types, unsupported counters, and reset and restart behavior. Keep optional
  plan, revision, and defensibly attributed allocation metrics in a native
  companion view. Defer persistence, utility tracking, and routine coverage.
  The native `pg_stat_statements` relation uses the pinned 1.13 column
  order and types. It exposes identity, plan and execution timing, calls,
  rows, and entry and extrema reset timestamps. Unsupported C-specific
  counters return SQL NULL. The native resource view exposes directly
  measured resources in their own units. Query text and ID are masked
  for other roles.
  A wire-level comparison with the pinned C peer matched every field
  name, position, type OID, and typmod for the relation and its Boolean
  function (54 fields each) and for the info relation (two fields).
  The wire regression now pins those descriptors.
  A test drives the catalog adapters with a non-superuser backend fixture
  and checks hidden peer query IDs and text and denied reset and tracking
  changes. Ordinary non-superuser SQL admission is outside native scope.
  The `pg_stat_statements(boolean)` function shares the view descriptor
  and supports counters-only reads. A false argument returns SQL NULL
  for query text and skips copying it into the detached snapshot.
  A polling benchmark seeded 512 entries with 256-character texts in
  one shard. An untracked SQL session ran 128
  `SELECT calls FROM pg_stat_statements(showtext)` polls per sample,
  with identical results and three fresh servers per argument. Switching
  from true to false reduced median allocation from 204301072 to
  136165712 bytes and sampled peak RSS from 308920320 to 239976448
  bytes. Median client latency moved from 3.28 to 3.14 ms and p95 from
  3.64 to 3.49 ms. Post-GC heap stayed near 48.1 MB. This isolates
  the text snapshot cost with no concurrent writers.
  Representative text is rendered from scanner tokens, with comments
  omitted and literals replaced by parameters. Rendering and retained text
  have separate bounds. Formatting is normalized to space-separated tokens.
  Numeric tokens in cast type syntax retain their values, including
  varchar length and numeric precision and scale.
  Positional ORDER BY keys and NULL or Boolean keywords following IS
  also retain their values. Literal operands still become parameters.
  Superusers can clear all entries with `pg_stat_statements_reset()`.
  The call returns the reset timestamp. With tracking enabled, its
  enclosing SELECT can publish a successful execution after the reset.
  The `pg_stat_statements_info`
  relation and function report evictions and the last full reset together.
  The timestamp starts at store initialization and advances after a reset
  that empties the store, following the pinned C implementation.
  Named or positional reset arguments can filter by user, database, and
  query ID. The optional `minmax_only` flag clears the selected extrema
  while retaining counts and cumulative timing.
  `pg_stat_statements.max` bounds entries (default 5000).
  `pg_stat_statements.max_text_bytes` bounds representative text (default
  1048576 bytes). Both are startup settings. Session
  `pg_stat_statements.track` accepts `none`, `top` (default), and
  `all`. While collection is enabled, only superusers can change it.
  Session `pg_stat_statements.track_planning` is off by default, as in C.
  Superusers can enable planning timing alongside execution tracking.
  Planning clocks are skipped when it is off.
  With collection disabled at startup, the native statement relation and
  functions return no rows. Process runtime counters remain available.
- [x] Compare grouping and exact phase counts with the C fixtures and an
  independent log of completed operations. Cover simple queries, repeated
  Bind/Execute, partial RETURNING, early portal close, failures, eviction,
  and reset races. Benchmark warmed short, mixed read/write, and long
  workloads at one and several clients with collection off and on. Record
  throughput, latency, CPU, allocation, retained memory, eviction, and
  lock contention. Set the overhead budget before optimization.
  A clean C build at `f110a918b0079` with extension 1.13 matched the
  native plan, call, and row totals for seven simple-protocol statements:
  two scalar SELECTs, two UNION ALL statements, one UNION, and two VALUES
  statements. Both produced four groups with (plans, calls, rows) of
  (1, 1, 1), (2, 2, 2), (2, 2, 2), and (2, 2, 4). Planning tracking
  was enabled on both servers, with C utility tracking disabled.
  The same pinned pair agreed on repeated parameterized INSERT Bind and
  Execute (2, 2, 2), early Close after fetching one SELECT row (1, 1, 1),
  and early Close after fetching one INSERT RETURNING row (1, 1, 3).
  The fixture checked CommandComplete messages, the number of DataRow
  messages received before suspension, and the committed table contents.
  A duplicate-key Execute returned SQLSTATE 23505, retained the original
  row, and recorded (1, 0, 0) on both servers. The C run disabled utility
  tracking and forced custom plans so each parameterized Bind replanned.
  Resetting from another connection after the first DataRow of a
  suspended two-row SELECT gave (0, 1, 2) on both peers after resume
  and commit. With a 100-entry setting, 120 simple queries of distinct
  projection widths all completed. Each surviving entry recorded one
  plan, call, and row. C retained 90 entries across three eviction
  cycles. Native retained 83 entries and counted 37 individual
  evictions under its sharded FIFO policy.

### P02 — Execution and activity diagnostics

Extend the existing EXPLAIN ANALYZE instrumentation and activity reporting
so operators can correlate a plan and its resource use with a live backend
and P01 query statistics. Use P01's observation API before adding new
instrumentation.

- [x] Connect existing EXPLAIN rows, loops, and timing to P01 scopes.
  Measure node deltas with execution-local counters. Preserve inclusive
  node totals and count each physical operation once in the query total.
  Verify nested scopes and rescans with a fake clock and known deltas.
  Timed node pulls use the statement observation clock. A fake-clock
  fixture drives nested iterator pulls with known time and I/O deltas,
  and a second fixture checks timing across an empty rescan.
- [x] Extend `pg_stat_activity` with bounded query text, query identity,
  state, and separate query and transaction timestamps. Publish coherent
  detached snapshots across execute, suspend, commit, error, and disconnect.
  Keep measurements on suspended portals and distinguish execution work,
  waits, and client backpressure.
  The native view snapshots admitted backends and exposes state, query ID
  when available, transaction and query timestamps, and up to 1024 query
  characters. Query details are visible to the same role and superusers.
  Simple-query batches publish bounded source text for each statement and
  clear the preceding statement's query ID at the boundary. A wire
  regression observes the blocked middle update and then the final SHOW
  without an inherited query ID.
  A wire check saw a blocked writer go from active to idle and disappear
  on disconnect. Relation, transaction-ID, and B-tree waits publish a
  single event without acquiring the lock being observed. The wire
  regression checks a blocked writer's transaction-ID wait and its
  clearance after progress and cancellation. The canceled update returns
  SQLSTATE 57014 and contributes one successful plan and no execution
  or affected rows. Extended wire checks cover identity publication at
  Bind, a refreshed query timestamp on Execute after suspension, a stable
  transaction timestamp across both fetches, and cleared transaction
  state after COMMIT and an implicit-transaction duplicate-key error.
  With the observer enabled at startup, a session can set
  `pg_stat_statements.track = none` and still publish analyzed query
  IDs. Both SELECT and DML portals retain their activity ID through
  Bind and Execute without creating an observation or statistics entry.
  The wire fixture checks matching simple and extended IDs and an empty
  statement store for SELECT and UPDATE RETURNING. The pinned C peer
  passed the same fixture.
  Activity rows capture query, state, timestamps, and waits under
  the publication mutex. Formatting and detached copies of the
  published query and application strings run after releasing it.
  Rove mutates the returned strings and verifies that later rows still
  see the published text.
- [x] Instrument buffer and storage operations, temporary files, spill,
  and WAL publication. Describe native cache and page counts in terms of
  the operations they measure. Register relation, XID, B-tree, WAL, and I/O
  waits at their owning boundaries. Restore previous nested waits on exit
  or cancellation. Keep status reads independent of the observed locks.
  An execution-local accumulator now follows statement slices. Native
  `EXPLAIN (BUFFERS)` reports relation reads, clean-cache hits, and
  relation writes in text and JSON. A read means a successful relation
  stream page read, which can be served by the host page cache. Counts
  cover execution with ANALYZE and planning otherwise. The iterator records
  inclusive resource deltas per node, including across pulls. Query totals
  are counted from the execution scope and are independent of node totals.
  Spill traffic counts bytes consumed from query temporary files and
  bytes written to them, including compact sort records and join match
  maps. A forced sort spill reported 270400 bytes in each direction at
  the sort node and query scope. The backend loop labels waits for the
  next frontend message as ClientRead. DataRow writes and their flushes
  report ClientWrite. The wire regression checks ClientRead during a
  partial input frame and sees sustained ClientWrite when a client stops
  reading a large result. The backend disappears after disconnect.
  Storage-manager page reads and writes expose DataFileRead
  and DataFileWrite. A controlled storage read held the storage mutex
  while a SQL observer read the event, then restored the prior wait.
  WAL appends and output flushes publish WALWrite. Synchronous file and
  directory syncs publish WALSync, and remote acknowledgment waits publish
  SyncRep. A controlled INSERT exposed WALWrite to a SQL poller and
  cleared it at commit. The Rove synchronous-replication test observes
  SyncRep until acknowledgment. The native
  `pg_stat_statements_resources` relation reports cumulative completed
  execution counters for relation I/O, clean-cache hits, spill traffic,
  and WAL records and bytes. WAL counts include record headers and cover
  successful appends inside an execution scope. Transaction-end WAL and
  background writes outside that scope are excluded.
  The wire regression joins a completed aggregate scan to EXPLAIN by
  query ID. Total relation reads plus cache hits agree across the two
  runs, with no writes, spill, or WAL records.
  A second wire case forces a terminal sort to spill with a 64 KiB
  work-memory budget. EXPLAIN uses the same projected-row sort as normal
  SELECT, and both report identical nonzero spill bytes read and written.
  The resource view also reports execution errors and SQLSTATE 57014
  cancellations separately. Failed execution does not add successful calls,
  affected rows, or resource totals. Planning-only rows remain visible in
  the compatible view when planning tracking is enabled. Wire checks cover
  a duplicate-key violation and canceling a blocked writer.
- [x] Extend EXPLAIN with explicit option validation, JSON, `TIMING OFF`,
  resource counters, and rows removed. Preserve existing default text
  output and keep row, loop, and resource counts with timing disabled.
  Enable node timing by default for explicit EXPLAIN ANALYZE. Report
  process-wide GC separately until it can be attributed to an execution.
  The wire regression checks default ANALYZE timing, default text
  layout, and text output with TIMING OFF and BUFFERS ON. That form
  keeps rows, loops, and native buffer fields without node times.
  `pg_lisp_runtime_stats` exposes one process-wide row. Its
  `gc_cpu_time_ms` and `allocated_bytes` are cumulative since process
  startup. `dynamic_heap_bytes` is current SBCL dynamic-space usage,
  including objects awaiting collection. Reads sample the counters
  without forcing a GC or attributing any of the measurements to a query.
  In the benchmark SBCL build, allocating a 2 MB vector on a worker
  increased the observing thread's `get-bytes-consed` delta by about
  2.1 MB. The internal per-thread allocation fields tested on that build
  stayed zero while the thread allocated about 98 KB. The native views
  expose allocation at process scope for this runtime.
  The runtime view also exposes `build_revision`, an optional label
  supplied using `-c pg_lisp.build_revision=<revision>` at startup.
  It is SQL NULL when unset. The label identifies the deployment and
  is independent of both the query ID and the physical plan ID.
  The text `TIMING OFF` path now retains row and loop counts while
  skipping per-node clock reads. The overall execution summary still
  reports elapsed time. Focused Rove and SQL wire checks cover the change.
  `FORMAT JSON` returns a JSON-typed plan tree with actual rows and loops
  for ANALYZE, optional node timings, and separate relation and index
  names on scan nodes. With the statement statistics store enabled,
  JSON includes the supported SELECT's native Query Identifier. Operators
  can match it to statement statistics or the activity of a running SELECT.
  A separate Plan Identifier hashes the admitted physical node structure,
  scan object OIDs, join kinds, and dependent plans. It changes when the
  same query switches from a sequential scan to an index scan.
  Plain EXPLAIN does not add a statement statistics call.
  Text and JSON share traversal for separately
  planned subqueries and CTEs. JSON includes their names and parent
  relationships. JSON ANALYZE reports planning time. Scan and nested-loop
  nodes expose their names or join type separately, and physical children
  report their relationship. JSON includes the planner's row estimate and
  startup and total costs for native sequential and index scans. Startup
  cost is zero for a sequential scan and includes the modeled B-tree
  descent for an index scan. Explicit `COSTS ON`
  includes them in text as well, while `COSTS OFF` suppresses them.
  Result, projection, sort, subquery scan, window stages, append, and
  limit nodes also report row estimates where their inputs provide
  enough information. Result has the default one-tuple CPU cost.
  Projection and subquery scan propagate modeled input costs, excluding
  unmodeled expression CPU. An unordered Append pays startup for its
  first arm and adds half a CPU tuple cost per row to the sum of its
  arms. A plain Limit interpolates the input run cost by the estimated
  consumed fraction. On the analyzed 2048-row fixture, the pinned C and
  native peers matched at 64.48 total for the scan and 32.24 for LIMIT
  1024. Both estimated 0.03 total and two rows for a two-arm UNION ALL
  of constants. Both peers also estimate one row and 0.03 total for
  LIMIT 0 over that scan. pg-lisp's wire regression observes zero actual
  rows and no input loops. Append still publishes the row sum when a
  child has no modeled cost, leaving the combined cost absent.
  The wire regression checks those cost rules and top-N rows.
  Sort and Top-N over a direct scan or column-only projection estimate
  comparison and output CPU using the pinned C model. The native
  disk-cost estimate uses average heap bytes plus a record-container
  allowance for memory, then charges sequential page transfers per
  estimated merge pass. It uses whole-row heap width as a proxy for
  the sort payload. The estimate can differ from actual spill behavior,
  particularly for narrow projections or compressed values.
  At 4 MiB work memory, both peers report startup and total costs of
  177.12 and 182.24 for the full sort, and 90.95 and 90.96 for Top-N 3
  over the same analyzed table. The wire regression also checks that
  lowering work memory raises the full sort's modeled costs.
  The bounded comparison estimate now also covers the case where
  retained Top-N rows fit but the full input would exceed work memory.
  A bare column-equals-literal Filter over Seq Scan uses the existing
  ANALYZE equality selectivity and one operator CPU charge per scanned
  row. Its estimate can reach the sort above it. The pinned comparison
  matched one estimated row and 69.61 startup and total costs for the
  filtered sort. Other filter shapes retain unknown estimates.
  Global aggregates estimate one output row without relying on input
  selectivity. Global COUNT with a known input cost adds the default
  operator CPU cost per input row and transition plus one output tuple.
  The pinned fixture matched at 69.60 startup and 69.61 total for
  COUNT(*) and SUM of a plain int2 or int4 column. Computed SUM
  arguments and COUNT over an unmodeled filter still report one row
  while leaving their costs absent.
  Single-column GROUP BY over an analyzed full scan estimates groups
  from the non-null distinct count, adds one when the sample contains
  NULL, and caps the estimate at the input cardinality. The native
  hash COUNT and sorted SUM paths both estimated and produced five
  groups on the nullable fixture. The pinned C peer estimated four
  and also produced five. Grouped integer COUNT adds transition and
  hash CPU plus per-group output cost when the estimated state fits in
  work memory. It uses the same entry-size charge as the executor.
  Startup matched C at 1.12 and total was 1.17 versus 1.16 because
  native includes the NULL group in the output estimate. The overflow
  model uses the executor's entry charge and partition fan-out, with
  uniform group frequencies and average heap width as proxies. It
  charges the initial overflow write before first output and the
  estimated partition passes afterward. On the 2048-group, 64 KiB
  fixture, native estimated 137.47 startup and 471.70 total and the
  wire regression observed spill writes. The pinned C peer estimated
  74.72 and 95.20 using its own hash-memory and batch model.
  Sorted SUM of a plain int2 or int4 column adds the internal sort
  and comparison costs. Startup includes processing the estimated
  first group and reading ahead to its boundary. The pinned sorted C
  peer reported 1.20 startup and 1.30 total. Native reported 1.21
  and 1.31, including the first group work and one more output group
  for NULL. The hash spill estimate covers a single integer key and
  does not price skew-driven fallback sorting.
  Streaming ranking windows add CPU for their functions, partition and
  order keys, and emitted rows. The internal sort uses the sort cost
  model for plain scan inputs with known widths. On the pinned fixture,
  ROW_NUMBER without ordering matched at 0 startup and 90.08 total.
  Ordered RANK matched at 212.96 total. Native startup is 177.12 and C
  startup is 177.14 because C also estimates the cost of reading ahead
  after sorting. Window aggregates with unmodeled buffering keep their
  row estimates and omit the cost.
  Cartesian nested loops estimate the product of independent input
  cardinalities. A direct inner scan uses the planner's existing replay
  and spill cost, plus tuple CPU for the joined rows. On the three-by-two
  fixture, both peers estimated and returned six rows. The native
  cost was 2.14 using its in-iterator replay model. C reported 2.13
  with a separate Materialize node and the opposite scan order.
  A bare integer equality between two independent analyzed sequential
  scans uses the C selectivity formula for inputs without two
  most-common-value lists. It combines their null fractions and
  divides by the larger distinct count. The cost includes replay and
  a comparison per candidate pair. With one null and one matching key
  in one input, both peers estimated and returned one row.
  Native cost was 2.16 and pinned C nested-loop cost was 2.15.
  When both most-common-value lists are available, pg-lisp matches
  common integer keys directly and applies C's residual estimate to
  the remaining frequencies. Both peers estimated and returned six
  rows when the common-value lists covered all non-null keys. Adding
  unmatched uncommon values raised both estimates to eleven while
  the joins still produced six rows. Other qualified joins,
  buffered-window costs, and additional selectivity and width modeling
  remain open.
  Explicit Filter nodes report rows rejected by their predicate, including
  SQL NULL results, in both formats. The count resets on each ANALYZE run
  and is available with `TIMING OFF`. Row averages include rescans that
  produced no rows. Nested-loop join counts include
  rejected replay candidates and lateral join predicates. Outer-join
  null extension does not add a rejection.
  The wire regression observes two loops at the inner LATERAL subquery
  and an average of 0.5 rows when its second invocation returns none.
  Native B-tree index predicates
  are exact, so JSON ANALYZE reports zero rows removed by index recheck
  on qualified index scans. Residual filter rejections remain separate.
  The wire regression verifies both counts with timing disabled.
- [x] Diagnose a spilling sort, rescanned nested loop, indexed lookup,
  blocked writer, suspended portal, and slow consumer through SQL. Reconcile
  P01 and P02 totals and compare supported EXPLAIN structure, counts, and
  option behavior with C. Exercise cancellation, disconnect, reload drain,
  and restart. Publish throughput, median and p95 latency, allocation, GC,
  polling cost, and contention for disabled instrumentation, counters only,
  and node timing on the same build and workload.
  On a 2048-row analyzed table, C and native JSON EXPLAIN agreed on the
  table and index names, one index scan loop, zero index recheck removals,
  one residual predicate rejection, and zero output rows. C reports the
  residual predicate on Index Scan. The native Index Scan produced one
  candidate and the explicit Filter rejected it. Both omitted node timing
  and estimates with TIMING OFF and COSTS OFF.
  With a 64 KiB work-memory limit, the pinned C peer and native server
  sorted the same 2048 rows in the same order, with one Sort loop and
  2048 actual rows at the Sort and sequential scan. C reported external
  merge with 79 temporary blocks read and 86 written. Native reported
  331776 spill bytes in each direction. The two counters measure their
  respective storage formats. Both reported zero spill traffic at the
  scan. The native wire regression also reconciles spill traffic with
  the statement resource view.
  EXPLAIN ANALYZE reports the native sort method and a Memory or Disk
  space type in text and JSON even with TIMING OFF. It distinguishes
  the stable in-memory sort, the bounded Top-N heap, concatenation
  of disjoint runs, and merge of overlapping runs. If rescans use
  different methods, the summary lists the methods seen, with Mixed
  space type when both memory and disk were used. With ANALYZE and
  BUFFERS, the query summary also reports Native Peak Temp Bytes, the
  high-water mark of charged spill files written by that execution.
  Removing a run deducts its live charge, and files from an earlier
  execution do not contribute to the sample. This measures logical
  bytes, while the spill read and write fields measure traffic.
  In-memory and Top-N cases report zero peak temp bytes. The disjoint
  sort peaked at 331776 bytes with the same number of bytes written.
  An 8 KiB overlapping-run case peaked at 449874 bytes while writing
  995328 bytes over its merge passes.
  The pinned comparison reports Disk for both peers. The native
  fixture with disjoint integer ranges reports external run
  concatenation, whereas the C peer reports external merge.
  The native wire suite forces overlapping runs as well and verifies
  the external merge label. It also exercises the sort that orders
  source rows before evaluating a target-list set-returning function.
  A lateral nested-loop fixture forced a sequential inner scan and
  disabled memoization on the C peer. Both returned one row from a
  2048-row input, ran the inner Limit twice, and reported 0.5 inner
  output rows per loop. The filter rejected 15 rows on its successful
  loop and 2048 on the other, reported as 1032 rows removed per loop
  after rounding on both peers. C puts the count on Seq Scan and native
  puts it on the explicit Filter node. Both suppressed node times and
  costs in JSON when TIMING OFF and COSTS OFF were set.

## P03 — Composite range and ordered B-tree scans

The range cursor preserves the existing index format and equality cursor. It
uses encoded leading-key prefixes with inclusive or exclusive endpoints and
traverses parent frames in either direction under the existing index lease.
SELECT applies the original filter after fetching visible heap tuples. An
ordered LIMIT can omit the sort when the leading equality constraints and
the physical key order prove the requested direction and NULL placement.

The wire test also checks results after an immediate stop and restart.
With `--reference-bin`, it compares 144 queries with C PostgreSQL, including
keys with 700 duplicates at each endpoint, crossed bounds, and both directions.
The physical detachment test confirms that a parked backward range releases
its lease, permits a write, and resumes its original candidate stream. A Rove
regression executes a stale ordered plan after replacing its index path and
checks the top-N results.

All 45 comparisons passed with 1,000,000 seeded rows. The planner
naturally selected the ordered composite index in all 15 composite
cases and preferred the sequential path to the tenant-only index.
The forced equality measurements therefore describe the old access
path as a control.

The range cursor now returns a TID directly from an accepted plain
leaf entry because the executor only needs its block and slot.
Decoded and packed keys share the bound comparison. With 100 warmed
LIMIT 10 probes over 16,384 rows, the median of three samples moved
from 7,101,296 to 7,035,232 allocated bytes, about 66 bytes saved per
returned candidate.

The writer probe pauses each reader at its first input row, aligns a
same-table insert, and releases the reader. It reports acquisition
wait and the portion after the reader is released separately. At
1,000,000 rows with 50% selectivity and LIMIT 10,000, the sequential
writer finished in 8.2 ms while the reader was still paused. The
equality writer waited 1,220 ms, with 1,200 ms after release, and
the bounded writer waited 44 ms, with 24 ms after release. The
insert is rolled back at the end of each comparison.

The restart fixture builds an index before inserting 3,500 rows,
updates indexed keys, deletes rows and runs VACUUM. Forward and
backward bounded scans agree with sorted heap scans and the fixture's
expected values before and after immediate restart.
The 16,000-row timestamp fixture verifies that the ordered composite
range uses a bounded index scan without a sort and returns the expected
100 rows.

## P04 — Partial and expression indexes

CREATE INDEX parsing preserves key order across column names, function
calls and parenthesized expressions. It keeps WHERE as a separate AST.
Ordinary CREATE INDEX can publish these definitions using the transactional
catalog. CREATE INDEX CONCURRENTLY uses the same evaluator for its build,
writer maintenance, and validation phases.

Index builds and DML maintenance use the same predicate and key evaluator.
DML computes expression keys before heap insertion and reuses them after
uniqueness waits. UPDATE includes predicate and expression inputs in its
TOAST demand. For 10,000 row preparations the new step allocated zero
bytes for plain indexes, 2.46 MB for qualifying expression rows and
0.07 MB for excluded rows.

Wire tests cover build, predicate transitions, uniqueness, rollback, and
restart, including a numeric-array predicate. Targetless ON CONFLICT
checks qualifying unique keys. Unconditional conflict inference excludes
partial and expression definitions. Single-table planning can use an
equality probe on all index keys, including mixed column/expression keys,
after proving the predicate from top-level query conjuncts. Proofs cover
exact matches, strict column comparisons implying IS NOT NULL, and
same-type integer or temporal bounds. Proven partial indexes over plain
columns can also supply bounded ranges and ordered LIMIT scans.

For 2,000 rows at 1%, 10% and 90% selectivity, build allocations were
2.6 MB, 11.8 MB and 151.2 MB. The indexes occupied 2, 4 and 21 blocks
after updating all keys, compared with 21 blocks for the full expression
index. For the full index, encoding a single computed key directly
reduced build allocation from 168.6 MB to 168.3 MB and update allocation
from 196.8 MB to 196.4 MB.

Concurrent expression and partial builds are qualified for native
physical storage. SQL tests pause at the unready and ready boundaries,
apply row changes from another session, and check final uniqueness.
Only builds with plain keys and no predicate mark their snapshots safe
for other concurrent builds to ignore. A live standby replayed the
unready, valid, and dropped catalog states of a partial expression
index. It also replayed a mixed transaction that created a table and an
expression index and then inserted and updated rows in the new table.

A native logical subscriber applies standalone ordinary and concurrent
partial expression index builds and drops. Replay uses the cooked index
definition shared with bootstrap. The replay test covers predicate
membership, uniqueness, an `oid8` cast, and a temporal cutoff resolved
under a different publisher TimeZone. Concurrent creation emits one logical catalog event when
interest starts during the build. A name collision with `IF NOT EXISTS`
emits no logical catalog event, including when another DDL command takes
the name after an ordinary build finishes. The native replication
suite also sends an INSERT, partial expression index creation, and
expression-changing UPDATE in one transaction. Both subscribers
enforce uniqueness for the new key and accept the retired key. A
second mixed transaction removes the index and its test row.

Ordinary build, concurrent build, and concurrent validation now
fetch external columns needed by the predicate first. Rows it
accepts fetch any remaining key columns and reuse values the
predicate already read. The build preserves the original
compression metadata for plain-column keys. The ordering follows
PostgreSQL C's predicate check before `FormIndexDatum`. The wire test inserts
a qualifying row and a rejected row between concurrent build and
validation, then checks TOAST reads and B-tree membership. It also
verifies that ordinary and concurrent builds store a compressed
plain key when the predicate reads that same external column.

The PostgreSQL C differential checks DDL SQLSTATEs and catalog
cleanup, NULL membership, predicate transitions, changed keys,
rollback, targetless and explicit ON CONFLICT, DELETE/VACUUM,
and a copied table with a rebuilt NULLS NOT DISTINCT index.
Concurrent qualifying inserts wait for a conflicting transaction,
then reject on commit or proceed on rollback. A predicate-false
insert completes while the contender waits. After immediate restart,
the rebuilt index retains its catalog flags and uniqueness, and an
aborted index is absent.
Immediate restarts also cover a built but unpublished partial expression
index, catalog WAL commit, and key maintenance on both sides of WAL flush.
The composite date-expression fixture checks its results against PostgreSQL
C. Using ANALYZE statistics for the plain tenant key reduced allocation in
20 warmed lookups from 5.83 MB to 1.37 MB and elapsed time from 28 ms to 12 ms
in a controlled comparison with the former estimate.

```sh
python3 src/lisp/t/expression-index-differential.py --reference-bin /path/to/postgresql/bin
```

## P05 — Spilling hash joins

The inner hash-join iterator builds and probes resumable inputs, retains
duplicate rows and their carriers, and rechecks keys and the residual ON
predicate. NULL keys never match. Integer, temporal, text and numeric
keys have matching hash and equality rules, including numeric scale
normalization and NaN equality.

The admitted resolved domains are `int2`, `int4`, `int8`, `date`,
`time`, `timestamp`, `timestamptz`, `text`, `varchar`, and
`numeric`. Integer and temporal keys use integral equality, text keys
use the executor's string equality, and numeric keys share the
ANALYZE value hash with the numeric comparator. Other equality domains
use the available nested-loop paths.

SQL planning can choose a two-table INNER hash join for large sequential
inputs with bare, same-type equality keys. Selective indexed joins keep
their nested-loop path. Both simple execution and one-row portal pulls
use the same iterator. EXPLAIN reports Hash Join and estimates build work.

EXPLAIN ANALYZE JSON reports hash candidates examined before key
rechecks, populated spill batches, nonempty build blocks, and the
largest charged build block. Build blocks count the in-memory build
or the replay blocks used after spill. Candidates, batches, and blocks
sum across executions of the node. Peak build bytes can exceed
work_mem when replay processes an oversized tuple on its own.
The wire test checks that the in-memory and spilled runs examine the
same candidates and that the ordinary spilled fixture stays within
its 64 KiB build budget.

Oversized build partitions with varying non-NULL hashes now split
before replay. Each split estimates how many build blocks it needs,
selects up to three unused varying hash bits, and routes both inputs
through the same selector. The worklist
processes children depth first and closes each parent side after
its children are sealed. The initial and later passes write one
side at a time, with up to eight output streams. The partition
bit budget bounds the depth. Unsplittable batches and hard temporary
file quotas use bounded block replay. A final batch keeps one
probe match map across its build blocks, with its byte allowance
reserved from the replay build budget. A sealed probe partition supplies
the count used to allocate the backing array within that allowance.
Replay marks each matched probe ordinal once per build block. NULL-key rows still reach
the unmatched pass of preserved inputs. Four full-join cases with
NULLs, disjoint keys and a residual predicate matched PostgreSQL C
in memory and under repartitioning, in both source orders.
The wire suite pauses and cancels during initial build, initial
spill, between the sides of a repartition pass, during batch load
and probe, and during both unmatched passes. Each case returns
SQLSTATE 57014, leaves no registered iterators or temporary files,
and runs the same query successfully on the canceled connection.
Forcing every non-NULL key to the same hash checks unequal-key
rejection across 1.44 million candidates in memory and in one
spill batch. A spilled LIMIT 1 stops after one valid match and
leaves no temporary files.
A correlated LATERAL query rescans its hash join for three outer rows,
returns the same counts at 4 MiB and 64 KiB, and releases the spill files.
The wire suite also corrupts sealed spill records before replay
opens them. The decode error releases every file and iterator and
the connection can rerun the query. A concurrent TRUNCATE times out
while a spilled portal is suspended. The portal returns its original
snapshot across concurrent UPDATE and DELETE, and TRUNCATE succeeds
after the portal closes. The immediate-restart case compares all
committed input rows, including the wide payload, before and after
recovery.

The optional PostgreSQL C differential covers 46 cases across all
admitted key types, with duplicate rows, NULL keys, empty input,
numeric scale and NaN, residual ON conditions, post-join WHERE, and
correlated LATERAL. It checks the planned hash join and verifies
spill at 64 KiB for each hash key type. The Rove suite separately
checks nonempty datum-origin payloads and source-presence vectors
for both orientations, in memory and through repartitioning.

```sh
python3 src/lisp/t/hash-join.py --reference-bin /path/to/postgresql/bin
```

The executor fuses ungrouped `count(*)` with a spilling inner equality
join of two single-column `int4` heaps. It skips NULL keys, partitions
four-byte keys, and counts probe matches using a 48 KiB build frequency
table. Reusing a caller-owned block on clean cache misses keeps each heap
scan from allocating a page per read.

## P06 — Planner statistics and costing

ANALYZE already records sampled row count, mean tuple width, NULL
fractions, distinct counts and up to ten most-common values per
column. The sample retains at most 300 physical rows and decoded
distribution values share a table-wide memory budget. The planner
uses the MCV list for equality, while the previous single-sided range
estimate assumed one third regardless of the distribution. Join
estimates use MCV overlap and distinct counts for a simple integer
equality between full scans. Some filter estimates are computed only
for EXPLAIN.

Integer-backed scalar columns now retain up to 33 histogram bounds
over the sample values outside the MCV list. Catalog images without
histograms remain readable. ANALYZE caches decoded integer samples
while processing one column and charges the retained bounds to the
existing metadata budget. A single literal inequality uses the
histogram for non-MCV, non-NULL mass and checks MCVs separately.
The planner applies this estimate to B-tree range costing and to
standalone sequential-filter row estimates. As in PostgreSQL C
`scalarineqsel`, the estimate uses half the residual mass when
statistics exist without a histogram. The no-statistics default
remains one third.

Run `python3 src/lisp/t/planner-statistics.py --reference-bin /path/to/pg/bin`
for the skewed integer differential. Lisp estimated 40 rows for the
rare tail and 240 for its complement. Both were exact on this fixture.
PostgreSQL C estimates were within one row of the actual counts.

Range costing now groups literal inequalities on one integer-backed
column, selects the tightest bound on each side, and combines the
two selectivities with the NULL correction from C's `clausesel.c`.
EXPLAIN carries the index row estimate through a filter when the
index condition covers the entire filter. The differential exercises
the interior of a histogram bucket, repeated one-sided predicates,
an empty range, and an actual Index Scan with a one-row estimate.

The equality-join estimator now uses the existing typed ANALYZE
equality semantics for MCV matches on full scans. The varchar and
numeric differential fixtures each estimated 6,800 rows, matching
actual output and PostgreSQL C. The numeric fixture uses different
decimal scales on each side.

Finite float4, float8, and bounded numeric samples now produce
histograms as well. Bounds use exact rational scalars so interpolation
works across very large finite floats and retains decimal ordering.
Numeric projection limits coefficient and scale work. NaN and both
infinities get priority within the existing ten MCV slots, even for
sample singletons. That preserves the finite histogram and uses SQL
ordering for non-finite comparisons. Interpolation applies the
residual equality correction continuously to keep the CDF monotone.
Histograms with flat buckets retain the observed point masses.
The differential checks signed zero, all three non-finite values,
and actual indexed float4, float8, and numeric ranges.

Simple equality joins use the greater of the two distinct counts and
known NULL fractions when paired MCV lists are unavailable. An
unanalyzed or narrowed base input uses a default of 200 distinct
values, capped by its estimated rows. Filtered base inputs leave
full-table MCV frequencies out of this fallback. Derived relations
and composite or expression keys still use the previous generic
estimates. The spill-sensitive LIMIT choice requires analyzed
distributions on both inputs. With ANALYZE only on the left of the
varchar holdout, Lisp estimated 280 rows and C estimated 1,456, against
6,800 actual. This fallback supplies a cost input even when its
independence assumption misses the actual overlap.

Both range holdouts moved from sequential scans to index scans. The
numeric holdout kept its hash join. Automatic refresh scheduling
belongs to P15.

## P07 — Transactional catalog foundation for P04

The catalog tracks relation names in a backend-owned overlay. The owning
backend can resolve staged table and index identities by name and OID.
Committed maps remain visible to other backends. The write set captures the
committed definitions it replaces and the owning heap definitions observed
by new indexes. Validation rejects conflicts before publication. The detached
merge starts from the latest committed map so an unrelated publication
survives a transaction that began earlier.

Staged table creation captures the OIDs of referenced sequences under the
same lock that resolves its expressions. Commit rejects a missing or
replaced sequence and allows a settings change on the same OID. A staged
drop or replacement also rechecks explicit publication membership before
WAL. A publication added after staging causes a serialization failure,
and a FOR ALL TABLES publication continues to follow the live table set.
PostgreSQL C records the corresponding dependencies in `pg_depend` from
`StoreAttrDefault` and `publication_add_relation`.

Native logical capture and apply retain the sequence identities of
each staged CREATE even if the same transaction drops its new table.
The sequence check runs before the mixed WAL terminal when the final
catalog is empty. The wire regression drops the sequence on another
connection after staging and verifies that the transient CREATE/DROP
transaction fails at commit.

Skipping the expression walker for absent defaults reduced allocation
from 23.0 MB to 4.1 MB over 20,000 dependency checks on a 24-column
table. The 20-cycle create/insert/index/update SQL workload allocated
85.4 MB with capture disabled, with about 48 MB of live heap after full
GC, consistent with the prior run.

The storage prototype uses transaction-private forks for a new heap or
B-tree. Dropping a provisional node discards only that node's references.
The owning backend sees its staged indexes through table index lookup and
the virtual catalogs. Catalog preparation serializes a detached image with
adjusted index membership and retains the committed table descriptors for
admitted operations.

Logical schema rendering resolves table constraints and standalone indexes
against its supplied database image. A Rove test stages an index drop and
checks that the committed schema contract retains the index while the
detached candidate contract omits it. This lets mixed logical apply
validate both catalog images even when an overlay is active.

The logical WAL codec can carry ordered transactional DDL fragments beside
row changes. Recovery and retained readers require a catalog-bearing
transaction terminal, even when the slot filters out the event. The native
sender uses a transactional Message with the commit LSN. Standalone
catalog records and legacy change chunks reject the new event.

The publisher can capture effective staged CREATE and DROP operations for
tables and indexes in the same spool as the rows. CREATE events use the
cooked definition. A transaction that creates, writes, and drops a table
still uses a catalog-bearing terminal. A SQL test reads the committed
event order from a retained slot and checks that rollback emits nothing.

The native subscriber stages those operations in the row apply transaction
and commits the catalog, rows and progress marker together. It requires the
event LSN to match Begin, rejects subscriber-owned metadata targets, and
requires local WAL flush before feedback. Wire tests crash the worker both
before target commit and after commit but before feedback. They verify
rollback and retry at the first cut and resume without reapplying a table
replacement at the second.

The publisher appends one schema contract after the ordered changes
while holding the catalog publication locks. It describes the latest
committed schema and the prepared catalog image. Recovery and slot
readers require exactly one final contract for a mixed batch, including
when the slot filters out the database. The subscriber compares both
shapes under its own commit locks. The wire tests cover an unrelated
publisher DDL that commits first, a missing contract, and subscriber
drift that prevents catalog, rows and progress from committing until
the drift is removed.

The native worker selects the transactional plugin for a new slot
when the target supports transactional apply and the source accepts
the plugin. Older endpoints use the legacy native path. Resume uses
the existing source slot and rejects an incompatible target before
starting the stream. Publishers admit mixed DDL only when the database's
logical interests are compatible. The WAL fence checks again before
publication. The wire fixture adds a standard slot after captured DDL
is staged and verifies that COMMIT fails. It also checks activation of
the first slot during an uncaptured transaction and legacy startup and
resume with an older endpoint. A transaction can fail at commit if its
last capable slot disappears while logical capture remains active.
Legacy native and standard slots can continue to receive ordinary
changes. The publisher no longer needs a private capture switch.

The fragment reader now validates event bytes after decoding and verifies
transferred bytes against the same checksum. Removing the duplicate
pre-decode checksum pass cut allocation from 27.9 MB to 19.7 MB and
elapsed time from 388 ms to 268 ms over 200 filtered 64 KiB events in a
warmed REPL. Post-GC live heap was about 80 MB in the test image.

The catalog-bearing WAL terminal records the XID and commit timestamp and
certifies a chunked catalog image with its deferred deletions. Redo applies
relation resets and page images before installing the catalog and removing
dropped files. The primary creates fresh forks only after WAL flush and
uses the same page-before-delete ordering. Logical readers can count
catalog chunks and bytes without keeping the image in memory.

Explicit transactions can stage CREATE TABLE, DROP TABLE, ordinary CREATE
INDEX and DROP INDEX alongside row writes. The primary installs the catalog
and removes the committed XID from new snapshots under the same lock. Heap
and index admission stays in force through commit or rollback.

## P08 — Common stored types

- [x] Store Boolean columns end to end. Preserve FALSE, TRUE and NULL across
  DML, defaults, checks, indexes, supported COPY paths, rollback and restart.
- [x] Add UUID scalar semantics and persistence. Match upstream input,
  canonical output, comparison and hashing. Cover heap, indexes, spill,
  text and 16-byte binary wire formats, COPY and supported logical replication.
  Verify indexed and sequential results agree after VACUUM and restart.
- [x] Generalize array storage while preserving existing stored array types.
  Add Boolean, integer, text/varchar and UUID arrays. Verify dimensions, lower
  bounds, NULL elements, TOAST and slice updates. Qualify comparison, hashing,
  containment, overlap and each supported transport path.

Common stored arrays use the existing ArrayType framing. The heap uses the
scalar type's alignment for its array elements and double alignment for
four-byte `bigint[]` heap datums. Existing bytea, JSON and JSONB array bytes
and logical type codes remain readable. The new types support text Bind,
text results, COPY TO and text pgoutput. Boolean and UUID also support
binary Bind and results. Binary array Bind and results return `0A000`.

The C differential covers mixed types, typed UUID input and hashes, Boolean
defaults, array bounds and assignments, TOAST, containment and overlap,
keyless logical capture, and indexed and sequential UUID lookups. It
checks binary parameter length errors and restarts the native server
after a process kill to verify the recovered UUID indexes, stored defaults
and COPY output. Live pgoutput carried the changed values while retaining
the unchanged TOAST marker for an external `text[]`.

## P09 — Column schema evolution

Build on P07 catalog transactions, dependency tracking and descriptor generations.

- [x] Preserve stable attribute identities and decode older tuples using
  physical attribute counts and dropped/missing attribute metadata.
- [x] Add transactional column rename, nullable ADD COLUMN, immutable constant
  missing defaults and dependency-aware DROP COLUMN. Keep stored missing values
  stable across later default changes. Verify rollback, prepared-plan invalidation
  and RESTRICT/CASCADE behavior.
- [x] Add SET/DROP NOT NULL and ordered multi-action validation. Prevent
  concurrent writes from introducing invalid rows during validation and publish
  the descriptor atomically.
- [x] Qualify a bounded ALTER TYPE conversion matrix with USING. Rewrite heap,
  indexes and TOAST atomically, and verify conversion failure, cancellation,
  disk quota, restart, physical replay and supported logical replication.

ALTER TYPE covers integer to bigint and text to integer with explicit USING.
Native transactional logical replication carries the rewrite in statement order.
The publisher rejects schema rewrites with standard slots and rejects USING
expressions whose evaluation can depend on session settings during logical
replay. Dependent defaults, CHECK expressions, expression indexes, and
publication filters still limit eligible conversions.

The transactional catalog and physical reader tests cover cancellation,
failed conversion and uniqueness checks, crash restart, and standby replay.
An isolated ext4 project quota forced an actual EDQUOT during private
scratch writes. The old heap remained readable and retry succeeded after
lifting the quota.

## P10 — Foreign keys

- [x] Add default text `COPY FROM STDIN` and enforce foreign keys on imported
  rows. Verify that an invalid reference or failed input rolls back the COPY
  statement over simple and extended query protocol.
- [x] Use P07 for transactional constraint metadata and dependencies. Validate
  referenced unique keys and expose the constraint state in `pg_constraint`.
- [x] Implement transaction-owned referenced-key locks with deadlock handling
  and cancellation. Prove that a child insert racing a parent delete or
  key-changing update cannot commit an orphan before enabling enforcement.
- [x] Enforce immediate checks for single and composite keys across DML paths,
  including parent-side NO ACTION and RESTRICT, multirow writes and self-references.
- [x] Add NOT VALID/VALIDATE, CASCADE, SET NULL and SET DEFAULT. Verify concurrent
  validation, cascading chains and cycles, rollback and checks of default values.
- [x] Add deferrable checks and SET CONSTRAINTS using transaction-local
  ownership. Verify commit-time checking, savepoint rollback, replay and the
  deferred cascading constraint used by upstream pg_partman.

  Savepoints support DML and constraint modes. Physical recovery preserves
  catalog and row changes, and logical decoding emits surviving DML after
  savepoint rollback. Native logical schema replication of foreign-key DDL
  remains unsupported, as do DROP CONSTRAINT and RENAME CONSTRAINT for foreign
  keys.

## P11 — Views

Start with persistent, read-only views over the supported SELECT subset.
Durable view DDL depends on P07 catalog transactions, object dependencies and
plan invalidation. View definitions require stable attribute identities.

- [x] Add a storage-free view relation kind. Persist a versioned analyzed
  query, its output descriptor, source text for diagnostics and dependency
  identities. Resolve references at definition time and verify catalog round
  trips preserve those bindings without creating a relation file. The
  [catalog codec test](t/rove/catalog-tests.lisp) confirms the version,
  source text, descriptor and bindings. The
  [wire crash fixture](t/transactional-catalog.py) checks recovered bindings
  after a base-column rename, dependency enforcement and absence of view files.
  Bare sequence-name literals retain the sequence OID and prevent DROP.
  Commit validates the OID if another session replaces the sequence while
  the view definition is staged. Explicit text casts keep late binding.
  Admitted calls also check the OID at execution before using a sequence.
  Bare sequence-function literals that resolve to tables or indexes
  also retain the original OID. They still report the wrong-kind error
  when called. RESTRICT, CASCADE, restart and concurrent index-drop
  fixtures cover these dependencies, including constraint-index removal.
  Deparsing resolves renamed objects by OID and retains numeric regclass
  arguments as numbers inside the SQL literal.
  The wire fixture verifies definition-time timestamp input for explicit
  casts, scalar comparisons, ANY and UNION. It checks PostgreSQL parity
  after a session time-zone change and verifies Lisp crash restart.
- [x] Add transactional CREATE VIEW and DROP VIEW. Expand ordinary and nested
  views through the existing derived-query planner with cycle detection.
  Support output aliases and explicitly reject writes to views. Verify query
  equivalence, rollback and visibility across sessions. The
  [wire fixture](t/transactional-catalog.py) verified transactional visibility
  and nested query results on both runtimes, and verified Lisp rejects cyclic
  definitions and writes while preserving the view and base table.
  Unqualified DROP VIEW uses the session search path. The wire fixture
  checks that DROP VIEW IF EXISTS leaves an out-of-path public view intact.
- [x] Implement CREATE OR REPLACE VIEW and DROP RESTRICT/CASCADE. Preserve the
  existing output prefix of names, types and typmods on replacement, and
  freeze SELECT * at definition time. Verify base-column changes and failed
  replacements against C PostgreSQL, including cached-plan invalidation.
  The [wire fixture](t/transactional-catalog.py) passed on both runtimes. It
  checks frozen output after base-column addition and rename, rejected
  replacements, reexecution of a named statement after committed replacement,
  and dependent views under both drop modes.
- [x] Preserve eligible predicate pushdown and hold descriptor leases for
  admitted plans. Verify joins, aggregates, LIMIT and prepared execution,
  including a suspended portal during concurrent view DDL. The
  [Rove tests](t/rove/backend-lifecycle-tests.lisp) verify the pushed
  predicate, LIMIT and outer join boundaries, and admitted descriptor leases.
  The [wire fixture](t/transactional-catalog.py) replaces an inner view
  during suspended aggregate and streaming executions. Both portals finish
  with the admitted definition, then fresh binds see the replacement.
- [x] Expose relation kind `v`, column metadata, `pg_views` and
  `pg_get_viewdef`. Verify restart and physical replay preserve definitions
  and dependencies, including crashes on either side of the DDL commit record.
  Qualify logical DDL end to end or reject unsupported cases before source
  publication. The [transactional catalog suite](t/transactional-catalog.py)
  checks both crash boundaries, recovered metadata and executable definitions,
  and rejection before logical publication. The
  [physical readers suite](t/physical-readers.sh) verifies standby replay of
  creation, replacement, and dependency drops.
  Whole-row expressions use live named row descriptors and deparse after
  field renames, additions and drops. Explicit field selections, including
  correlated ones, keep their attribute binding and protect that attribute
  against DROP RESTRICT. Derived sources with unique field names also
  round-trip. Duplicate derived field labels survive recreation,
  including a null-extended outer-join row. Recreating the emitted
  SQL after a second rename preserves those labels and bindings.
  Typed temporal arrays retain their bounds, NULL elements and
  definition-time values across session time zones.

## Other completed development tasks

- [x] Bring the legacy WAL slot retention fixture up to the current startup
  contract. In `6bcf82cf717`,
  [the durable retention test](t/unit-tests/wal-tests.lisp) writes replayable
  batches across three WAL segments and restores the slot under startup
  ownership. It releases the original XID and snapshot before reload. The
  reconstructed slot alone keeps the old segments until the slot is dropped.
  All 12 checks passed in isolation. In `b934b3bbb91` the test also acquired a
  fresh active-XID list for each run.
  The combined run at `404fa69ea5b` passed all 1,642 Rove heads and 1,172
  legacy checks, including this fixture.

- [x] Reconcile XID wraparound recovery with strict primary startup. Stage and
  sync the empty CLOG separately, atomically replace `pg_xact`, then sync
  its directory. File and directory fsync faults leave a status file for
  restart and hold the allocation gate closed. The combined unit suite
  passed with 1,174 legacy checks and zero failures.

- [x] Close out the PostgreSQL backend and pinned Patroni comparison using the
  [goal closeout check](PHYSICAL-HA.md#goal-closeout-check). Standby feedback,
  both logical-slot copy variants, and password-backed bootstrap passed at
  `4679be54534`. Archive-only and archive-to-stream recovery passed at
  `41774ee2b32`. Archive-backed rewind passed with the corrected fixture in
  `72cc65a1a5b`, and the SCRAM lifecycle passed rewind and read-only rejoin.

- [x] Bind all retained-root references and deletion decisions to checked archive
  and host identities before permitting GC after continuation restoration.
  In `3e17f23fb17`, deletion uses a held archive descriptor and rechecks its
  named binding before unlink. The active host's fence also covers bind aliases
  of its fixed archive child by device and inode, even across different mount
  IDs. A private mount namespace test verified the alias and an unrelated
  archive. In `ee384379ac9`, references and holds capture archive device,
  inode and mount identity. Owner operations validate the original binding,
  and cleanup recognizes references on a bind alias of the same archive.
  Thirty strict physical test heads and a real bind alias deletion test passed.
  Restart the Lisp server across the reference and hold layout change.
  In `88058fd9a4f`, registration holds the archive directory through inventory
  validation and rejects a changed pathname binding before installing the
  reference. Thirty-one strict physical test heads passed, including injected
  rebinding and cold rewind.
  In `a38f5b39b6d`, the fence reads continuation markers and opens the
  active host's archive child through the same held host directory.
  A private mount namespace check verified that the fence follows a host
  bind alias after marker removal and leaves an unrelated directory eligible.
  All 32 strict retained-root test heads passed, including symlinked PGDATA
  and cold rewind.
  In `3eaa74a20d5`, owned imported startup checks continuation evidence
  through the same host descriptor used for the source proof and ownership
  checks. The rebinding regression verifies that the held descriptor
  still sees the original host's marker after replacement of its pathname.
  All 33 strict retained-root test heads passed.
  In `5fa2400bbd3`, cold import reads the base reservation checkpoint
  using the WAL namespace and checkpoint timeline from its bound copy
  control. A fresh timeline-qualified source-proof workflow and three
  strict namespace and reservation test heads passed.
  An instrumented imported-main run restored both slots before catalog
  initialization on three boots and preserved SQL writes across restarts.
  Removing proof after PID ownership caused the pre-WAL recheck to refuse
  startup without initializing WAL, slots, or the catalog. The continuation
  fence remained closed and the checked storage inputs were preserved.
  In `aa31362cba3`, owned startup records its continuation archive by
  device and inode while holding the host directory descriptor. Deletion
  also records an archive observed beneath a fenced parent. A retained-root
  regression attempts deletion before and after moving that archive and
  confirms the object survives both attempts. All 34 strict retained-root
  test heads passed. The instrumented restart and proof-loss cases passed
  with startup recording enabled.
  In `b6f83fb0345`, imported origin reconciliation checks the actual
  reader's device, inode and mount against the registration descriptor
  before reading any inventory objects. Runtime origin lookup checks its
  fresh reader against the registered archive. All 35 strict retained-root
  test heads passed. The instrumented imported restart, proof-loss and
  physical streaming cases passed.
  In `036bab32386`, imported startup restores the current primary
  control's root after replay under the drained startup gate and typed
  host owner. Reconciliation checks the held archive identity before
  loading the selected root. Instrumented startup observed the current
  root registered under the primary owner before catalog initialization
  on all three boots. The proof-loss refusal still kept the fence closed.
  Five strict promotion and retained-root test heads passed.
  In `63f933fc32e`, imported startup opens any surviving receive control
  under the held host and its existing spool lock. It checks the base
  against the verified import proof, validates the durable prefix, and
  restores the selected root against the held archive before slot loading.
  It skips tail cleanup and lock creation. All 35 strict retained-root test
  heads passed. Instrumented startup observed the same receive registration
  before catalog initialization on three boots, and proof-loss refusal passed.
  In `02863b0ba8d`, the post-restoration check verifies the required
  primary, receive and historical owner selections together against the
  held archive's device, inode and mount. All 36 strict retained-root
  test heads passed, including missing-owner and mount mismatch cases.
  Both the two-slot and zero-slot layouts passed three instrumented
  boots and the proof-loss refusal case.
  In `7f623f09353`, the post-restoration check runs with the receive spool
  locked and verifies the full durable control before and after checking
  the archive registrations. All 36 strict retained-root tests passed.
  Native restart and proof-loss cases passed with both a surviving
  receive spool and a fixture with the spool absent.
  The explicit startup unlink path carries host and receive ownership
  through the archive mutex, required-root check, and descriptor-relative
  unlink. All 36 strict retained-root tests passed. With
  `PG_NATIVE_MAIN_CHECK_RETAINED_UNLINK=1`, the native observer verified
  that the protected root survives and an unreferenced candidate is
  removed on each of three boots. Proof-loss refusal passed.
  The unlink check verifies that the primary control on the held host
  still selects the registered primary root and that a surviving
  historical promotion control matches the permanent origin proof.
  The native observer replaced the historical control and verified that
  the candidate survived, then restored the control and verified unlink.
  Imported primary startup now collects canonical, regular retained
  objects after slot restoration and feedback validation. Collection
  skips any object protected by a registered reference or hold and
  rechecks the durable selectors before each unlink. All 36 strict
  retained-root tests passed. Three native boots each with two slots
  and with no slots removed the test candidate and preserved the
  selected root and unrelated file. Both proof-loss cases refused startup.

- [x] Allow detached index builds to overlap logical DDL on other tables.
  Capture schema-before and validate subscriber state at final catalog publication,
  after relation gates. Keep response collection separate from publication locks.
  The unchanged name race, durable failure/restart checks and an interleaved
  B-to-A-to-DROP schema chain pass. Code is in `c992a20a742`; evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/wal-writer-policy/ddl-publication-scope/`.

- [x] Audit legacy fixtures that leave virtual reader owners behind.
  A full Rove-plus-legacy run reached wraparound with no real XIDs or retained
  snapshots in its fixture, but still waited for readers from earlier fixtures.
  The isolated wraparound fixtures now bind their own virtual reader registry.
  Per-fixture tracing identified the replication-slot catalog view test.
  It now resets its own backend transaction on exit. The instrumented
  Rove-plus-legacy run found no surviving readers and passed all 1,172
  legacy checks.

- [x] Recover interrupted preserved-data reseed installation moves.
  Verify the recorded move prefix or one completed rename ahead across the
  target, incoming image and quarantine. Sync visible journal progress before
  another move, then sync both move parents before recording the next step.
  Validate the installed base before publishing the terminal step. Keep the old
  entries in quarantine and retain the startup fences through follow handoff.
  Validation passed 1,852 physical tests, strict compilation and eight SIGKILL
  cases with fresh-process recovery and independent file identity checks.
  Code commits are `7bfa3540dfd` and `a31d9f8e27b`. Documentation is in
  `8216360fe1f`. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-install-moves/namespace-progress.json`.

- [x] Reconcile partial cancellation-phase publications.
  Derive ABORTING from the trusted final attempt after checking a canonical
  prefix in the held temporary file. Sync its empty truncation before writing
  replacement bytes. A fresh CLOSED/ABORTED source receipt authorizes ABORTED.
  Generic recovery also handles complete initial temporaries without a final
  attempt and rejects partial records in that state. Source cleanup uses the
  saved route while staging and incoming directories are unavailable.
  Validation passed 1,861 physical tests, 86 focused tests and strict compilation.
  Native checks covered 16 repair cuts, three complete initial-record recoveries
  and one expected partial-record refusal through fresh processes. The prefix
  property passed 80 trials at seed 102 with digest
  `fnv1a64-v1:dee007a39777a2a8`. Input coverage and rejection counts are unmeasured.
  Code is in `34165f98d0a`, with mutation-test corrections in `be47bf354b9`.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-cancel-reconcile/progress.json`.

- [x] Add source authority for backup-lease handoff to a proved follow pin.
  Match the original base to an unused READY successor and durably publish
  CLOSING/HANDED-OFF before retiring bootstrap retention. Protect the successor
  until CLOSED, then allow normal retirement while keeping its historical lease
  assignment. Exact handoff retries settle the recorded outcome. Primary and
  standby startup validate successor ownership before persisting bootstrap loss.
  A completed backup can hand off after promotion through its INTERRUPTED state.
  Validation passed 1,872 physical tests and strict cold compilation. Fifteen
  actual SIGKILL cuts recovered in fresh processes with unchanged successor
  bytes and retained WAL. Tests also cover disk-only standby recovery and
  descendant receivers before and at the fork. Code and refactoring reviews
  passed. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-follow-handoff/progress.md`.

- [x] Expose backup-lease handoff through the replication protocol and client.
  BACKUP_LEASE_HANDOFF carries the original request and exact successor identity.
  It returns a checked CLOSED/HANDED-OFF receipt. The client snapshots ownership
  before I/O and rejects a different outcome or successor. Native checks passed
  both normal completion and reply loss after source commit, followed by saved
  route status, exact retries, successor retirement and historical retries.
  Source SQL remained writable, with no retained pins or helper owners.
  Validation passed 1,877 physical tests, strict cold compilation and 80 generated
  wire trials at seed 103, digest `fnv1a64-v1:702a786762318f65`. Input coverage
  and rejection counts are unmeasured. Code and refactoring reviews passed.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-follow-handoff/progress.md`.

- [x] Add durable source allocation and startup cleanup for lease follow pins.
  Persist generation and name ownership, validate the complete parent and pin
  graph at startup, and finish canceled or aborted cleanup before releasing
  retention. Selected handoffs retain their separate closing and closed rules.
  Code: `b463c32c5db` and `3b59a0a6ab8`.

- [x] Expose allocated follow preparation and cancellation through the source
  protocol and strict client. The client freezes the intent before I/O; cancel
  returns a parent receipt and submitted identity, not a child-state claim.
  The fresh full run executed 1,962 distinct test heads with positive assertions;
  three later tests passed separately. Strict cold compilation passed. Native
  reply-loss checks covered four modes, restart and writable SQL. The wire
  property passed 100 trials at seed 113, digest `fnv1a64-v1:fb2a5a7ba229d029`;
  input coverage and rejection counts were not measured. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-lease-wire/progress.md`.

- [x] Capture a configured cancellation replacement in a separate private root.
  Hold and recheck the old cancellation history through capture, require a fresh
  lease for the same cluster and system, and retain the old history and debt.
  The ordinary durable capture journal fences retries. Existing operations handle
  status, cancellation, recovery and later installation and handoff. The caller
  chooses explicit directories and coordinates competing replacements.
  Production strict cold compilation and 24 Rove tests passed, with 799 passing
  assertion reports.
  Native Patroni replacement runs passed for pending, source-closed and cleaned,
  including the pending journal crash and fresh-process recovery. The original
  default reseed regression also passed. Logs, results and timings are under
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/`, with remaining broad
  work in `full-goal-completion-ledger.md` there.

- [x] Add the target follow codec and read-only owner foundation. Validate
  bounded generation records and the installed predecessor and image under a
  scoped owner. The next item adds durable intent publication and recovery.

- [x] Publish and recover durable target follow intent. The local store accepts
  empty genesis and PREPARING intent only, with checked predecessor and image;
  incomplete publication fences the owner until reopen. Validation passed 200
  selected tests and two later tests, strict cold compilation, and eight SIGKILL
  cuts with 16 fresh images. A 100-trial property at seed 117 passed with digest
  `fnv1a64-v1:b04db31708e74ac1`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/progress.md`.

- [x] Prepare and record the target's historical source proof. Check the frozen
  route, writer identity, full history and flush around the exact prepare RPC,
  then prove the installed phase-zero base locally before publishing PROVED.
  A stable proof is historical; handoff must revalidate source authority. The
  public intent publisher still accepts only empty and PREPARING records.
  Validation accounted for 241 distinct selected tests (239 in a fresh image
  and two later tests), including 18 new tests, and a strict fresh compile
  of 204 files without warnings or errors. The empty-base property passed 100
  trials at seed 118, digest `fnv1a64-v1:f212ed2b3a0ae697`; input coverage and
  rejection counts were not measured, and there is no FunctionSpec. Native TLI 1
  and SQL-promoted TLI 2 sources each passed ten cases, including eight SIGKILL
  cuts, with ten target images and 19 fresh target processes per run. Native RPC
  tests cover same-timeline proofs; descendant and retained proofs used local
  installed fixtures and properties. See `prepare-notes.md`, `progress.md`, the prepare
  check JSON files and native results under
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/`.

- [x] Durably stage disabled target follow configuration. Save Building intent
  before creating workspace outputs, then publish Staged after checking and
  syncing the exact guard, directory and file identities and derived bytes.
  Reopen recognized complete prefixes; preserve torn or unexplained entries.
  Staged retries are read-only. The installed base remains at phase zero, and
  startup remains fenced. Code: `9fefd569231`, `0cd7cc2e9e5` and `321916a3d70`.
  Validation passed 266 distinct selected tests in fresh processes, strict
  compilation of 210 production files, and the 31 focused tests after splitting
  the new test modules. Each code commit also compiled as an independent staged
  snapshot. Native Lisp TLI 1 checks covered ten target images, 30 fresh target
  processes and nine SIGKILL cuts, with source-off construction and recovery.
  Native proofs were same-timeline; installed fixtures covered all five proof
  geometries. The copy property passed 80 trials at seed 120, digest
  `fnv1a64-v1:ee7af4059f73f009`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/progress.md`.

- [x] Define the inert configuration move plan and verification prerequisites.
  The pure plan accounts for every image across four or five moves, including
  the saved original control and unused staged images. Shared verification can
  check an actual decoded original control separately from the exact namespace;
  ordinary recovery retains both checks. A separate directory scope holds the
  install guards without granting namespace or startup authority. Code:
  `b1c7e895256`, `56d48cf6806` and `e302374b627`. Validation passed 79 distinct
  selected tests in fresh processes and strict fresh compilation. The move
  position property passed 80 trials at seed 121, digest
  `fnv1a64-v1:38048727ed1c3732`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/inert-directory-checkpoint.md`.

- [x] Install and recover disabled inert target configuration. Hold the original
  base and all ten directory roles while moving receive and control metadata.
  Publish Installing/0 separately, then perform at most one no-replace move per
  call, sync both parents and publish its exact progress. Reopen one-ahead
  layouts without repeating a move. Promote only a complete, exact next
  temporary; preserve partial or conflicting evidence. Inert publication is
  separate, checks the actual pending state and leaves startup fenced.
  Validation covered 111 distinct selected tests across fresh runs, including
  all five installed proof geometries, original-file corruption, guard and path
  replacement, actual advance preflight and hook failures, and strict pending
  witness rejection. Strict cold compilation passed all 217 source files with
  no warnings or errors. Native Lisp TLI 1 checks passed 13 captured images,
  52 fresh target processes and 12 SIGKILL cuts, with the source stopped during
  local construction and recovery. Descendant and retained installation cases
  used local installed fixtures. The inventory and prefix-signature properties
  each passed 80 trials at seeds 122 and 123, with digests
  `fnv1a64-v1:0ff7e43f10c22c03` and `fnv1a64-v1:97580d0206f8561c`;
  input coverage and rejection counts were not measured. Code: `ac52498ee2c`,
  `d8ce64749bb`, `666e6aa3ba6` and `e545abbe9f8`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/inert-operation-scope-progress.md`.

- [x] Persist and reconcile target source settlement. Publish exact handoff-pending
  intent before remote effects, observe STATUS first on the frozen source route,
  and record only the exact CLOSED parent outcome and successor. Separate pending
  and terminal publication authority; recheck local snapshots after network I/O.
  Recover complete next temporaries offline and preserve partial or conflicting
  evidence. Historical settlement after local abort grants no activation or cleanup
  authority. Configuration remains Inert and disabled, with startup fenced.
  Validation passed 165 distinct selected tests and 3,089 assertions in a fresh
  process, strict compilation of 223 production files, and independent commit
  snapshots. Native TLI 1 acceptance covered 21 cases, 119 fresh target processes,
  11 target SIGKILLs and one source SIGKILL, including lost replies and cold reopen.
  All five installed proof geometries passed with mocked source transport.
  Properties passed 80 trials each at seeds 124 and 125; input coverage and
  rejection counts were not measured. Code: `999d6d4e87e`, `7bdf47c9871`,
  `b1a38bab591` and `1fba3d45b1d`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/progress.md` and
  `settlement-related-regression-check.json` in that directory.

  Activation, startup fence retirement, cancellation and replacement remain open,
  as do logical drain, control metadata and WAL parity, audits and
  pinned Patroni acceptance. No coordination is provided. During full acceptance,
  audit older harnesses including `t/interop/child-bootstrap.py`: direct control
  calls to physical base backup and manual follow start now fail live admission.
  Run these operations in task-owned workers outside control after it returns.

- [x] Reserve follow-journal room for activation and retirement before new
  pending publication or HANDOFF. Compute a conservative completion bound from
  exact terminal receipts and compatible activation/retirement field widths,
  adding the existing reserve once. Keep size-only templates separate from
  publication authority. Preserve the old exact receipt-size contract, journal
  readability, historical STATUS reconciliation and recovery of a complete
  pending temporary. The shared terminal constructor keeps receipt invariants
  in one place. A fresh regression passed 61 tests and 738 assertions; strengthened
  property coverage passed 100 trials at seed 130, digest
  `fnv1a64-v1:20733aff3238b523`, with input coverage and rejection counts
  unmeasured. Additional focused tests cover all 11 legal completion phase
  shapes from each fixture prefix, printer bindings and the shared-constructor
  refactor. Strict cold compilation passed all 227 production files. Luna
  checkpoint, Sol full review and the focused refactor rereview passed. Source
  docs were reviewed separately.
  Evidence: `capacity-green-check.json`, `capacity-followup-check.json`,
  `capacity-contract-check.json` and `capacity-refactor-cold-check.json` under
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-activation/`.

- [x] Fence the reserved target follow intent names before startup or older
  reseed ownership. Both final and temporary names reject every entry type;
  specialized cleanup and install roles repeat the check. Validation passed 44
  related tests, strict cold compilation and 16 fresh startup cases. The property
  passed 80 trials at seed 114, digest `fnv1a64-v1:e93f14d6ca661cf2`; input
  coverage and rejection counts were not measured. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-target-follow/progress.md`.
  Handoff and activation remain open as described above.

- [x] Fence reserved reseed installation state across startup and owner roles.
  Both `physical-reseed.install` and `physical-reseed.install.tmp` refuse startup
  before PID inspection or creation, even without an attempt journal. Ordinary
  stopped reseed ownership refuses them before reading PID bytes and on later
  checks. Clean adoption refuses all four reseed intent names, including after
  PID removal. Its specialized startup owner and both startup entrypoints repeat
  the shared guard, so intent appearing between callbacks cannot pass through
  an inherited-method gap or retire pending adoption journals.
  Eight new Rove tests first failed against the previous behavior, then passed.
  Validation passed 21 selected tests, fresh strict compilation and 16 actual
  startup attempts covering both install names, four entry types and present or
  absent opaque PID data. All original names, bytes, inodes and modes survived.
  The refactor review found no useful macro; the shared guard keeps the policy
  in an early module without a later startup dependency. Code commit:
  `3bfca57247b`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-install/fence-evidence.json`.
  This reserves and enforces fences only. The install journal format, dedicated
  owner, quarantine moves, crash recovery and source cleanup after a PID move
  remain open; no install authority or coordination has been added.

- [x] Connect the stopped-target journal to remote capture and cancellation.
  A new start freezes a separate private stage and the accepted source route
  with its effective credential owner before sending CAPTURE once. Reopening
  uses the saved request and credentials for STATUS/ABORT only, including when
  no manifest arrived or the source reports UNKNOWN. Abort intent is durable
  before the RPC; only CLOSED/ABORTED permits the local ABORTED phase. Both
  journal names remain startup fences, and cancellation does not need the stage
  or a matching current source timeline.
  Receipt duplicates the held stage descriptor and keeps descriptors for its
  base, global and pg_wal children through verification. Exclusive nofollow
  creation prevents overwriting existing files; scoped readers and exact
  descriptor inventories reject unsafe leaves and unexpected names. A new
  child must be empty when opened; this does not prove it is the inode created
  by mkdirat. Final stage identity checks reject a renamed root or child.
  Validation passed 1,740 distinct physical tests, 22 focused tests and fresh
  strict compilation. Nine native cases covered normal capture, root and child
  replacement during receipt, pre-RPC and post-capture cuts, lost local ABORT
  acknowledgment, a nonlocal connection exit, source failure before streaming,
  and an unexpected stage entry. Each case preserved the old file bytes,
  inodes and inventory apart from the deliberate journal fence, reacquired
  stopped ownership for STATUS/ABORT, and rejected late CAPTURE after cancellation.
  Source SQL remained intact and writable, with no helper channels, capture
  owners or retained source pins left. The stage property passed 100 trials at
  seed 95, digest `e731cd00462a62f1`; input coverage was not collected.
  Code commits: `f74ee6c4a2b`, `cd9b2e172ff`, `00f929a4584`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-remote/evidence.json`.
  Preserved-data installation, actual follow handoff and startup reconciliation
  remain open. No coordinator is implemented.

- [x] Add the stopped-target reseed journal foundation.
  Freeze the original lease request, physical source settings and credential
  owner, and local directory/lock/PID evidence in a canonical private record.
  Publication uses exclusive temporary creation, file sync, rename, directory
  sync and exact readback. Reopen only under the same stopped ownership.
  Complete temporary records can finish a valid transition; partial or malformed
  entries remain fenced. Both names refuse startup before PID mutation.
  ABORTED publication requires a caller-supplied typed CLOSED/ABORTED receipt
  for the original request; live observation belongs to the future RPC driver.
  Recovery can finish an already receipt-checked complete temporary publication.
  ABORTED retains the startup fence because the old divergent tree remains.
  Validation passed a fresh 1,718-test physical run before the final receipt
  guard, followed by all 15 new tests with that guard, including 24 SIGKILL cuts
  and 24 fresh-image reopens, and fresh strict compilation. The codec property
  passed 100 trials at seed 94, digest `50e18d5e3bfcd5b3`; input coverage and
  rejection counts were not measured. An earlier interrupted full run is not
  counted as a pass. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-attempt/evidence.json`.
  Connecting this journal to remote capture/status/cancellation, binding the
  receive stage, preserved-data installation and follow handoff remain open.

- [x] Expose remote backup lease capture, status, cancellation and strict clients.
  CAPTURE_BACKUP_NATIVE, BACKUP_LEASE_STATUS and BACKUP_LEASE_ABORT now have
  source dispatch and strict clients. COPY completion follows stage cleanup;
  READY status rechecks the pin, origin and required WAL. Fresh closeout passed
  1,705 physical tests and strict compilation. Twelve focused tests passed;
  final STATUS cancellation assertions and formatting were followed by another
  three-test run. Earlier verification passed 903 unit tests, 201 wire tests
  and 16 native scenario checks. The wire property passed 100 trials at seed 93,
  digest `8aa4973e17643a0b`; input coverage and rejection counts were not measured.
  Terra wrote the source documentation; primary and Sol reviews checked it
  separately from the code review.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/backup-lease-wire/evidence.json`.
  Target journal persistence followed in the checkpoint above. Connecting it
  to the remote commands, follow handoff and preserved-data installation remain
  open; there is no coordination.

- [x] Capture verified native backups under durable lease ownership.
  One runtime owner claims a new request; retries return status without
  recapturing. Recoverable errors and nonlocal exits interrupt unfinished
  attempts. Poisoned-WAL cleanup leaves the durable record for cold startup;
  both paths keep any private retention pin. ABORT saves closing intent first
  and defers pin removal until the capture owner exits. Cleanup keeps its own
  request copy and cannot be called early through the callback's owner token.
  The driver holds reload exclusion through copying, verification, publication
  and owner cleanup. It checks the actual base, manifest, source and pin, then
  publishes the origin and READY under one WAL fence. READY proves source
  capture, not delivery to a receiver. STATUS/ABORT use ordinary admission and
  wait while copying holds quiescence.
  Validation passed 1,694 physical tests, 903 unit tests, 201 wire tests,
  82 selected tests and strict fresh compilation. Ten new tests cover ownership,
  retries, cancellation and cleanup. The ownership-context property passed 100
  trials at seed 92, digest `a137ea3f9125d99e`; input coverage and rejection
  counts were not measured. A real server captured SQL data, rejected recapture
  into the nonempty stage, and canceled after copying and after verification
  without publishing an origin. Three SIGKILL/restarts covered origin-before-
  READY, durable READY before memory publication, and later READY recovery;
  retained pins and checked aborts remained usable, and SQL writes resumed.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/backup-lease-capture/evidence.json`.
  These internal source APIs gained remote capture/STATUS/ABORT commands and
  clients in the checkpoint above. Target attempt persistence, follow handoff
  and preserved-data reseed installation remain open. No coordination is
  implemented.

- [x] Enforce backup lease pin ownership and reconcile leases at startup.
  Indexed names, including closed tombstones, reject ordinary pin creation,
  streaming, feedback and release. Checked retirement requires the exact durable
  closing request; a primary also requires the registered pin generation.
  Startup loads the lease index before source pins. READY requires a matching
  pin and durable origin. Explicit loss, a changed timeline or standby startup
  persists pin loss before removing its retention floor; unexplained missing
  READY pins or WAL remain fatal. Closing cleanup resumes without old WAL.
  Validation passed 1,683 physical tests and one additional fault test run
  separately, 903 unit tests, 201 wire tests, 72 selected tests, strict fresh
  compilation, four pin-creation and four startup-loss nonlocal cuts. Two real
  postmaster SIGKILL/restarts verified interruption and closing reconciliation.
  The ownership property passed 100 trials at seed 91, digest
  `03055e58a9eeb791`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/backup-lease-authority/evidence.json`.
  Internal capture and checked STATUS/ABORT APIs followed in the checkpoint
  above. Remote commands followed later; follow handoff and target reseed
  remain open.

- [x] Add checked source backup request records and private durable publication.
  Request identity is immutable, interrupted capture cannot restart under the
  same identity, and closed records count toward capacity without eviction.
  File publication and cold loading use the WAL publication fence; final bytes
  are synced and read back before the new memory index is visible. Loading
  interrupts unfinished records and preserves source pins.
  Validation passed 52 selected lease, source-pin and export-origin tests,
  including 24 new tests, strict fresh compilation, twelve error/nonlocal cuts,
  twelve real SIGKILL cuts and twelve fresh-image reopen probes. Two properties
  passed 100 trials at seeds 89 and 90, digests `f44aabb064b11e6d` and
  `e5ffed3f68a4677f`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/backup-lease-codec/evidence.json`.
  At this checkpoint the records were internal metadata only. Pin authority,
  READY-to-pin/origin validation and startup wiring were added in the next
  checkpoint above. Internal capture and remote commands followed later.

- [x] Add stopped-target ownership and a pre-PID fence for future full reseed.
  Only ESRCH proves that an existing PID is dead. Preserve its complete bytes
  and identity, reject changes or uncertain liveness, and keep the reseed owner
  out of clean-adoption APIs. Any `physical-reseed.attempt` entry refuses real
  startup before stale PID repair. A common-ancestor helper follows C pg_rewind's
  timeline/begin walk; it establishes history geometry only.
  Validation passed 1,640 physical, 903 unit and 201 wire tests and strict fresh
  compilation. Real crashed-server ownership and fenced startup preserved old
  file contents and inodes; normal startup and cold restart also passed.
  Two properties ran 100 trials at seeds 87 and 88, digests
  `f08e33b11713bba1` and `9f51140381ef0a7e`; input coverage and rejection counts
  were not measured. Code commits: `401d71c1195`, `a532100c533`. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-owner/evidence.json`.
  Source backup leases, preserved-data reseed installation and terminal startup
  reconciliation remain open. No reseed operation or coordination is provided.

- [x] Connect the target journal to one-shot remote capture and cancellation.
  Use the stopped-target owner, not the clean-adoption capability. Save the
  original lease request and the accepted source route with its credential
  owner in a private bounded record at `physical-reseed.attempt`. Reuse the
  checked frozen-source representation; do not resolve a new host on retry.
  Keep PID evidence and dirty data untouched. A missing manifest must still
  permit STATUS and checked ABORT with the original request. Cover durable
  publication cuts, reopen, changed local ownership, changed source route and
  delayed capture after cancellation. This record must precede source capture;
  the existing clean-adoption attempt is created too late and requires clean
  control facts. Installation and retention handoff follow separately.
  Use `:premanifest`, `:aborting` and `:aborted` as target intent phases; do not
  guess source progress from a disconnected client. Open resumed operations
  from the saved frozen credential owner, not merely a supplied connection at
  the same route. UNKNOWN never authorizes recapture under the original ID.
  Save abort intent before the source RPC; wait through CLOSING until the source
  confirms CLOSED/ABORTED. The target's ABORTED record remains a startup fence:
  canceled source retention does not make the old divergent primary safe to
  start writable. Do not add journal retirement in this milestone.
  Before any lease RPC, require exclusive temporary creation, file fsync,
  rename, directory fsync and exact readback under the stopped-target owner.
  Publication errors or nonlocal exits stop RPCs. Reopen the original record
  conservatively; malformed or ambiguous final/temporary state cannot authorize
  a new identity. Extend the pre-PID startup check to reject the reserved
  temporary pathname too. A temporary-only failure precedes remote effects,
  but it still marks an interrupted attempt and must not lift the local fence.
  Expose CAPTURE only through a new-start entrypoint that requires both journal
  names absent. Reopening or recovering any journal permits STATUS/ABORT only,
  even when source status is UNKNOWN. Bind a private empty receive stage outside
  PGDATA by canonical path, device and inode before the first lease RPC; missing
  or damaged stage contents must not prevent remote cancellation. Reconnect from
  the saved frozen credential owner and recheck owner, journal, route and source
  identity before dispatch. Initial capture requires the saved source timeline;
  cancellation after source promotion must still accept the same system ID.

- [x] Prepare an independent incoming image for full reseed.
  Reopen the frozen receive stage through held read-only directory descriptors,
  verify its exact inventory and complete base, and check the original source
  lease is READY before writing the separate empty incoming workspace. Copy
  files into independent inodes, checking length, checksum and stable source
  identity. Compare all six metadata file digests as well as manifest contents,
  then recheck the source image, lease, original journal and incoming image.
  Require ordinary stopped ownership and a common descriptor-verified mount.
  Leave partial incoming files in place and do not abort the source lease on
  failure; saved STATUS and checked ABORT remain available. The returned facts
  are observations, not a durable installation proof. No old data moves or
  startup retirement occur.
  Verification: 1,760 distinct physical tests, 28 focused tests, 40 generated
  copy trials (seed 96), fresh strict compilation and 13 native cases passed.
  The two successful native copies independently checked equal bytes and
  distinct inodes for every one of their 13 files. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-image/evidence.json`.

- [x] Add the opaque old-entry inventory for a full reseed install plan.
  Read only the old root's immediate entries through a paired directory
  descriptor. A single no-follow statx observation binds each entry's mount,
  inode and metadata; require two complete matching scans, including nanosecond
  modification/change times, and recheck the original owner and attempt.
  Preserve hardlinked names and unreadable files/directories without opening
  contents, traversing children or following symlink referents. Keep management
  names out of the move inventory and reject their inode aliases.
  Enforce entry and encoded-name byte limits during readdir, before allocating
  the complete list or inspecting every entry. Invalid UTF-8 filenames remain
  unsupported and fail before any future install-journal publication. Descendant
  mounts are not inspected; later unsupported rename interactions must leave
  startup fenced.
  These are observations for a future plan, with no moves or new ownership role.
  Verification: 1,773 distinct physical tests including 13 new tests, 49 focused
  checks, 100 generated trials (seed 97), and fresh strict compilation passed.
  The shared management-name refactor also passed focused checks and strict
  compilation after the full suite had started. Evidence and next-journal notes:
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-inventory/evidence.json`.

- [x] Anchor reseed attempt I/O and recover initial-install source cleanup.
  Hold a checked root descriptor across metadata reads, exclusive temporary
  creation, renameat promotion, parent sync and readback. Root replacement
  regressions reproduced temporary creation and rename into a substitute tree
  before this change. The descriptor paths preserve the owned tree through
  those cuts, and ownership rechecks stop remote commands when the name changes.
  A separate cleanup owner uses the valid final attempt under the held lock,
  original stopped PID evidence, and immutable request/source/local bindings.
  It permits saved STATUS and checked ABORT with an initial install temporary
  present and final install journal absent. Every temporary inode kind stays
  intact, and a missing receive stage permits source cleanup. Forward phases
  may replace the attempt inode while preserving its identity fields.
  Use opaque owner printing because the saved attempt contains credentials.
  Verification passed 1,784 distinct physical tests including 11 new tests,
  81 focused checks, fresh strict compilation and six native libpq cases.
  Native checks covered four install temporary kinds, reopened cancellation
  cuts after write/rename, preserved old bytes/inodes, late capture refusal,
  released source retention and writable SQL. The shared immutable-field-list
  refactor passed focused checks and strict compilation after broader runs began.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-cleanup/evidence.json`.

- [x] Encode the dedicated full-reseed installation plan and progress record.
  Keep the original active attempt, initial attempt-file identity, source manifest,
  guarded workspace identities and old/new inventories in a bounded canonical
  CRC-protected image. Check alias exclusion, consistent old hardlinks, private
  incoming objects and original PID facts. Decode consecutive progress lines and
  report only a prefix of the next line as partial. Cache the terminal step once
  while decoding so long progress histories do not repeatedly traverse the plan.
  Validation passed 1,790 distinct physical tests, 87 focused Rove tests and
  fresh strict compilation. The
  generated property passed 100 trials at seed 98, declaration digest
  `fnv1a64-v1:e15893845e333891`. Input coverage and rejection counts were not
  measured. Luna and Sol reviewed the code. Terra wrote the source comments,
  followed by primary and separate Sol documentation reviews.
  The first full run exposed an existing PID fixture mutating a quoted identity
  list across warm executions. A two-iteration regression reproduced the failure
  after a fresh test load. Allocate the list for each iteration and retain the
  original assertions. Focused checks and repeated warm executions then passed;
  Luna and Sol reviewed this separate fixture fix.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-install-codec/evidence.json`.
  Guarded publication and install ownership are the next milestone. Publish
  them together so a final install journal retains saved STATUS/ABORT access.
  Before creating the root temporary, sync workspace guards, copied files,
  directories and naming parents. Hold both startup lock-file and directory
  locks. Derive the journal identity from its retained exclusive descriptor.
  Persist device/inode identities and compare fresh mount IDs within each
  operation. The initial attempt-file identity excludes aliases at publication;
  checked cancellation may replace that inode later. Actual namespace proof
  must precede progress-tail repair, with full image verification before the
  terminal step. The codec supplies the record format for that implementation.

- [x] Hold guarded incoming and quarantine directories for reseed installation.
  A checked scope retains target and auxiliary descriptors, directory and startup
  file locks, private single-link guards, and exact guard bytes and identities.
  Reopening requires saved guard facts. Fresh descriptor mount IDs must match
  the held target mount. Preparation checks ordinary stopped ownership before
  guard creation and before the callback, then permits common ownership checks
  after the callback so future journal publication can supersede that role.
  The scope revokes its check closure on exit and returns independent copies of
  facts. Guard files remain after errors or nonlocal exits.
  Guarded image inventory admits only the two fixed root guard names under a
  descriptor scope. Default receipt and all child inventories remain strict.
  The future publisher must check guard identities and contents around image
  verification and sync copied image files before creating the root journal.
  All 1,800 distinct physical tests, ten new Rove tests, 95 focused checks and
  fresh strict compilation passed.
  Tests cover both locks, saved reopen, scope/thread revocation, mutable copies,
  three arriving-intent cuts, directory replacement, guard corruption and unsafe
  guard entry kinds. Luna and Sol reviewed the code. Sol found no useful macro
  refactor. Terra wrote the comments, followed by primary and separate Sol review.
  Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/reseed-install-publication/auxiliary-evidence.json`.
  Guarded initial journal publication and install ownership remain the next
  coupled milestone, followed by moves and progress recovery.

- [x] Retire terminal adoption fences during real postmaster startup. A durable
  terminal RETIRE marker permits checked removal of handoff, install and attempt
  in that order, with follow removed last. Nonterminal follow progress remains
  fenced. Retirement leaves the server a standby; old quarantine
  and the source pin remain, and no coordinator, DCS or election is provided.
  Validation passed 1,624 physical, 903 unit, 201 wire and 20 focused tests,
  strict fresh compilation, 30 error cuts, 12 real-postmaster SIGKILL cases,
  16 valid prefix cases, five mutations and two late ownership faults. Two
  properties ran 100 trials at seeds 85 and 86, digests `4e5b9b5a61762595` and
  `c3fb2f17a0456875`; input coverage and rejection counts were not measured.
  Native startup retired the fences; with the helper configured it caught up from
  E=33752368, applied a new source commit (`proof_progress=703001`), and rejected
  a standby write with 25006. It passed a target cold restart; before restart,
  receive and replay matched at `0/02046CCB`. Root and lock inodes and preserved
  workspaces, stage and reference files remained unchanged. The first native
  attempt lacked `pg_config`; the rerun used an explicit local `PG_CONFIG` without
  a production logic change. A helper build needs `pg_config` on PATH or explicit
  `PG_CONFIG`; `PG_LIBPQ_HELPER` can select an installed helper. Evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/adoption-startup/evidence.json`.
  Code commits: `eab9aa41730`, `bd7b43dabcd`, `09bca60b619`. Divergent rewind
  or explicit reseed, logical/pgoutput drain, checkpoint facts, WAL policy,
  audits and Patroni acceptance remain open.

- [x] Prepare offline follow metadata from a ready adoption handoff. A fixed
  derived plan in a private third workspace uses a durable no-replace rename
  journal, accepting its recorded prefix or one completed move ahead. The
  terminal state is active-fenced; original controls and pending configuration
  are saved, and A/J/H/F, quarantine and the source pin remain in place.
  Validation passed 1,604 physical, 903 unit and 201 wire tests, 16 focused
  tests, 66 move cuts, five prepublication and four authority cuts, four negative
  mutations, seven fresh-process SIGKILL cases and strict cold compilation.
  Native publication and a refused cold startup passed; the startup probe
  preserved protected files. Two properties ran
  100 trials each at seeds 83 and 84, digests `e49d091c22d1d20d` and
  `682b6cb65b8ddf0e`; input coverage and rejection counts were not measured.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/adoption-follow/evidence.json`.
  Code commits: `9fe2159fd5b`, `fb2e059d880`, `70eb4942613`. Real postmaster
  startup reconciliation and fence retirement, actual catch-up, and divergent
  rewind or reseed remain open. Coordination is not provided.

- [x] Encode and privately publish bounded adoption attempt records under the
  active offline owner. The CRC-protected record preserves clean control facts,
  one bounded source history and monotone phase/proof state. Publication syncs
  and reads back an owned copy. These are structural checks, not fresh source or
  stage validation or authority to prepare or install. The record contains private
  credentials; hex encoding does not encrypt them.
  Validation passed 1,509 physical, 903 unit and 201 wire tests, ten new Rove
  tests and strict cold compilation. The property passed 100 trials at seed 76,
  digest `fnv1a64-v1:6f8d577637fba897`; it has no Function Spec and rejection
  counts and input coverage were not measured. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/adoption-attempt-record/evidence.json`.
  Full context validation, route construction, network PREPARE/PROVE and ABORT
  retirement remain open.

- [x] Fence startup on any `physical-adoption.attempt` directory entry before
  backup-stage cleanup or authentication completion. Only ENOENT permits startup;
  the check does not parse or follow the entry. A future attempt writer must use
  the same cooperative PGDATA lock. Ten focused PID tests, including three new
  Rove tests, native corrupt-file, dangling-symlink and directory fence cases,
  normal restart, and strict cold compilation passed. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/adoption-attempt-fence/`.
  Full context validation, network retry and installation remain open.

- [x] Stage a verified canonical E/Mnew image from a cooperatively owned, clean
  former primary. The guarded, private target is verified with the ordinary base
  and authentication checks, while historical B/M/H_E remains a separate witness.
  This does not install or start the image or grant writer authority. Failure can
  leave a guarded partial target that must not be retried as an empty directory.
  Validation passed 1,496 physical, 903 unit and 201 wire tests, strict cold
  compilation and eight new Rove tests. The manifest property passed 100 trials
  at seed 75, digest `fnv1a64-v1:8e1df9be1a4e8493`; no Function Spec is registered
  and rejection counts and input coverage were not measured. Native staging,
  PREPARE and retained proof passed with independent H_E/root comparison; the old
  primary's files and inode identities remained unchanged. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/adoption-stage/`.

- [x] Prove retained origin history on the promoted source for an initial READY
  pin. The source hashes the selected root from historical B/M through E while
  holding a root-reader lease, and rechecks the pin and source context before
  replying. The typed result remains a claim until independently compared locally.
  This prerequisite does not install an image or authorize adoption or writes.
  Validation passed 1,488 physical, 903 unit and 201 wire tests and strict cold
  compilation. The query property passed 100 trials at seed 74, digest
  `fnv1a64-v1:f77e314e8f25d6cb`; no Function Spec is registered and rejection
  counts and input coverage were not measured. Native evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/retained-origin-proof/`.

- [x] Add read-only local checks for former-primary adoption. Keep the existing
  PGDATA lock and directory inodes, require exact clean checkpoint/catalog,
  resource and allocator facts, and extend a genuine durable export-origin
  commitment through the clean endpoint using original validated WAL bytes.
  Actual next-XID remains separate from reserved allocation capacity. An empty
  checkpoint active-XID horizon may equal next-XID rather than zero.
  The checkpoint passed 1,476 physical, 903 unit and 201 wire tests and strict
  cold compilation. The raw-byte property ran 100 trials at seed 73; rejection
  counts and input coverage were not measured. Read-only probes matched the
  stopped former primary's commitment to the promoted descendant's selected
  retained root, leaving all 17 and 34 files respectively byte-identical.
  That real source receipt had P=E, so its suffix was empty; fixtures checked
  nonempty suffixes, incomplete records and corruption.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/adoption-preflight/`.
  These helpers grant no writer or recovery-installation authority.
  Durable attempt recording and journaled installation remain open.

- [x] Preserve source-owned history commitments for verified remote exports.
  A durable origin binds the original system, timeline, base and manifest to a
  source-proved prefix. Confirmation saves that commitment before advancing the
  private retention cursor. Origins retain no WAL and are excluded from clones;
  completed receipts survive client cancellation. The bounded registry has private
  list/forget operations. Progress currently requires the original timeline and a
  retained cursor at or before the saved proof endpoint.
  The milestone passed 1,464 physical, 903 unit and 201 wire tests, strict cold
  compilation, native backup fault/capacity checks and six shutdown scenarios.
  An independent multisegment oracle proved recycling, source restart and promotion.
  Offline origin loading left source file contents unchanged. Evidence is
  in `/home/imbaczek/proj/pg-ha-artifacts/export-origins/`.
  This supplies historical evidence; it does not yet implement offline adoption.

- [x] Keep ready native physical senders through the primary shutdown checkpoint.
  Frozen stream capabilities drain to the validated flush target under a narrow
  shutdown and cleanup lane, then release all claims before clean state. An empty
  plan keeps the terminal fence path. Disconnect is abandonment, not an ACK;
  clean exit does not prove delivery of the shutdown checkpoint's WAL endpoint
  to a disconnected peer.
  SIGQUIT's zero exit timeout avoids thread-join waiting, not every possible exit
  delay. Six native scenarios, 1,447 physical, 903 unit and 201 wire tests, strict
  cold compilation and the existing shutdown test passed. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/shutdown-streaming/`. Logical sender drain
  is covered by the following milestone. Former-primary adoption, rewind,
  provenance and coordination remain outside this milestone.

- [x] Drain ready private logical and pgoutput senders during primary shutdown.
  Freeze the exact slot claim and source epoch, settle the WAL writer and
  abandoned transactions, then capture the flushed target before the checkpoint.
  Require scanned, sent and confirmed positions through that target before
  recording acknowledgement. Release the slot before sending one `COPY 0`
  completion and closing. Client CopyDone retains its existing command sequence.
  Fast and smart shutdown, partial feedback, output backpressure, filtered and
  asynchronous WAL tails, disconnects, stalled feedback, SIGQUIT and incomplete
  WAL publication are covered by ten native cases. All 246 selected unit tests,
  six physical shutdown cases and both C interoperability directions passed.
  Code and harness commits are `b1327c80ba6`, `d73385a61ac` and `fde17d44d16`.
  The interop startup wait now allows 120 seconds after large-WAL crash recovery
  exceeded its previous 20-second allowance. Evidence and measured phase timings
  are in `/home/imbaczek/proj/pg-ha-artifacts/logical-shutdown/`.

- [x] Resolve the admission and receiver join race during concurrent reload.
  Receiver quiescence holds reload exclusion with admission open through stop,
  join and exit flush. It releases exclusion it acquired before replay joins;
  caller-owned exclusion remains the caller's responsibility. An
  existing reload owner with closed or terminal admission is rejected before
  stop is published. The checkpoint passed 1,439 physical checks and strict
  cold compilation. The owned-server `configured-follow.sh` matrix exercised
  manual stop and configured clear while reload admission was closed, followed
  by resume, metadata, retarget, cold restart and promotion. Its fixture simulates
  the reload lock and quiescence; it does not compile ASDF inside the gate.
  Evidence is in `/home/imbaczek/proj/pg-ha-artifacts/receiver-reload/`.

- [x] Add a native TCP primary clone for Patroni's default backup method.
  Staging pauses mutations, then streaming permits SQL to proceed. The native
  command and image differ from C's online base backup; a permanent follower
  slot must retain WAL until attachment. The reviewed checkpoint passed 903 unit,
  1,430 physical, 201 wire and 1,161 legacy checks and strict cold compilation.
  The pinned Patroni 4.1.5 TCP test covers HBA, format and occupied destination
  rejection, wrapper cancellation, SQL during streaming, damaged streams, source
  crash cleanup, cold follow restart and cloning a promoted timeline 2 primary.
  Evidence: `/home/imbaczek/proj/pg-ha-artifacts/native-basebackup/pg-backup-export-hba-recheck.log`.
  Rewind, offline primary adoption, unsupported checkpoint fields, write/flush
  parity, the cold-load audit and broader documentation remain open.

- [x] Flush outstanding WAL on an eligible writer cycle with an empty publication
  FIFO (`cc9ea0202c3`, `68785ed66c4`, `07c9368c734`). The checkpoint passed 903
  unit, 1,416 physical, 201 wire and 1,161 legacy checks and strict cold
  compilation. Six native idle-writer cases, eight FIFO regressions and one C
  unassigned read-only WAL flush case passed. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/idle-writer/`. Properties ran 100 trials
  at seeds 4020 and 4021 with respective digests `08daeb35cb803c4f` and
  `e5f7a51c19219c1c`; the authority target has no Function Spec, and input
  coverage and rejection counts were not measured. C's page rounding and
  background flush timing policy remain different.

- [x] Expose the native checkpoint SQL record and scalar oid8 subset
  (`f6cc550e1f8`, `2b2236e4aed`). The 20-column descriptor uses selected durable
  facts; eleven unavailable fields and additional missing legacy facts are NULL.
  The checkpoint passed 901 unit, 1,416 physical, 201 wire and 1,161 legacy
  checks and strict cold compilation. An independent C/native oracle checked
  metadata, text and numeric input/casts, JSON, primary control facts and the
  unchanged Patroni standby-leader query, including source-offline cold recovery
  and corrupt-pointer rejection. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/control-checkpoint/`. Two properties ran
  100 trials each at seeds 4018 and 4019, with respective digests
  `b9ea210f07666dd1` and `0aa6a19aa62a5126`.

- [x] Support typed scalar and fixed-descriptor composite functions in FROM
  (`b984839c702`, `de6637b686e`). The checkpoint passed 891 unit, 1,413
  physical, 201 wire and 1,161 legacy checks and strict cold compilation. Native C
  comparison covered 36 SQL cases for values, metadata and errors and two binary TEXT
  Bind/rebind cases; the 12 dollar-quote cases also passed. Existing binary INTEGER
  formats remain unsupported. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/scalar-from/`: `cold.log`, `physical.log`,
  `wire.log`, `legacy.log`, `native-c-reviewed.log`, `dollar-c-reviewed.log` and
  `live-format-diagnosis.log`; unit checks ran through the tool. The property over
  `parse-from-relation` and `expand-select-targets` ran 100 trials at seed 4014
  (digest `fnv1a64-v1:996181b0f1e156ab`); neither target has a registered
  Function Spec, and input coverage and rejection counts were not measured.
  At that checkpoint DO, PERFORM, `txid_current()` and full Patroni acceptance
  remained open.

- [x] Scan dollar-quoted strings and parameter boundaries (`1a831afe4bc`).
  At that checkpoint DO, PERFORM, `txid_current()` and scalar FROM support remained
  open. The checkpoint passed 885 unit and 201 wire checks and
  strict cold compilation. The native C comparison passed 12 value and error
  position cases, including Unicode tags, overlapping delimiters, parameter
  boundaries and a one-million-character literal. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/dollar-quoting/`:
  `cold-final.log`, `wire.log` and `native-c-reviewed.log`; unit checks ran through the
  tool. The helper/tokenizer property ran 100 trials at seed 4013 (digest
  `fnv1a64-v1:9318d3c8888b3fa3`); neither target has a registered Function Spec,
  and rejection counts and input coverage were not measured.

- [x] Enable public `synchronous_commit=off` (`0ce95c4e409`, `24cebb6d345`).
  The checkpoint passed 882 unit, 1,413 physical, 201 wire and 1,161 legacy
  checks, strict cold compilation, six native public OFF crash cases and the
  settings and fallback checks. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/public-async/`: `cold.log`,
  `physical.log`, `wire.log`, `legacy.log`, `native.log`, and
  `settings-final-reviewed.log`; unit checks ran through the
  tool. The normalization and policy property ran 100 trials at seed 4012
  (digest `fnv1a64-v1:db34c9708e1d3c6f`); neither target has a registered
  Function Spec, and rejection counts and input coverage were not measured.

- [x] Integrate private asynchronous acceptance with receipts, CLOG and backend
  cleanup (`280dc7f3541`, `0ff4a132af7`). A queued owner retains its resources
  after backend cleanup; pressure, resets or writer unavailability force a flush.
  Final DDL and remote waiting also force local flush. At that checkpoint public
  OFF remained disabled.
  The checkpoint passed 881 unit checks through the tool, 1,413 physical checks,
  strict cold compilation, six native crash/acceptance, eight native writer and
  eight native receipt cases. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/async-acceptance/`: `physical-final.log`,
  `cold-reviewed.log`, `native-crash-final.log`, `native-writer.log` and
  `native-receipts.log`. No new cl-spec contract was added for this milestone.

- [x] Add the logical CLOG acceptance transition for a linked pending
  publication (`d4a0b22ab30`). At that checkpoint the helper had no production
  caller and receipt and backend cleanup integration remained open. The checkpoint
  passed 874 unit and 1,413 physical checks and strict cold compilation. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/accepted-clog/` (`physical.log` and
  `cold.log`); unit checks ran through the tool. The property
  `accepted-clog-neighbor-writes-preserve-wal-order` ran 100 trials at seed 4011
  (digest `fnv1a64-v1:dadd7089ff788acf`); its target has no registered Function Spec,
  and rejection counts and input coverage were not measured.

- [x] Add retained WAL publication pressure accounting and admission
  (`e8673faf91a`). Pressure can settle predecessors and require synchronous
  completion; the limits bound asynchronous acceptance, not a transaction's
  transient allocations. At that checkpoint OFF remained disabled and integration
  still needed to honor the admission helper's third return value. It passed 871
  unit, 1,413 physical, 1,161 legacy and 201 wire checks, strict cold compilation,
  eight native writer cases including pressure and SIGKILL, 21 native page and five
  native CLOG cases. Logs are in `/home/imbaczek/proj/pg-ha-artifacts/wal-pressure/`.
  Two cl-spec properties ran 100 trials each at seeds 4009 and 4010, with respective
  digests `491b955b8bd3e744` and `19c520ff03696461`. Neither pressure property
  has a Function Spec; coverage and rejection counts were not measured. Asynchronous acceptance, OFF,
  and unrelated WAL flushing with an empty FIFO remained open at that checkpoint.

- [x] Add the primary WAL writer authority and FIFO drain foundation. No writer
  loop or startup is enabled. The checkpoint passed 842 unit checks and strict
  cold compilation. The scoped property ran 100 trials at seed 4007 (digest
  `fnv1a64-v1:e5f7a51c19219c1c`); it has no Function Spec, and coverage and
  rejection counts were not measured. At that checkpoint, adoption guards and
  CLOG reset epoch preparation and publication remained open.

- [x] Guard storage adoption against live or unjoined writers and prepare and
  publish CLOG reset epochs (`cf1a4a95d8d`). The checkpoint passed 851 unit,
  1,413 physical, 1,161 legacy and five native CLOG checks, plus strict cold
  compilation. Evidence is in `/home/imbaczek/proj/pg-ha-artifacts/wal-writer-epoch/`:
  `physical-current.log`, `legacy-current.log`, `native-clog-current.log`, and
  `cold-final.log`. The property `stale-clog-reset-plans-have-no-storage-effects`
  ran 100 trials at seed 4008 (digest `fnv1a64-v1:c2552cb71de45bf9`); there is
  no Function Spec, and coverage and rejection counts were not measured. At that
  checkpoint the loop and lifecycle had not yet been activated.

- [x] Activate the primary WAL writer lifecycle (`09d10de9ca7`, `1ece86c5993`).
  At that checkpoint the auxiliary writer drained the publication FIFO; unrelated
  WAL flushes with an empty FIFO remained open. It passed 858 unit, 1,413 physical,
  1,161 legacy and 201 wire checks, strict cold compilation, six native writer
  cases including SQL XID wrap, 21 native page, five native CLOG and two snapshot
  cases, native promotion and shutdown. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/wal-writer-lifecycle/`: `physical.log`,
  `legacy.log`, `wire.log`, `cold.log`, `native-final.log`, `native-wrap.log`,
  `native-pages.log`, `native-clog-current.log`, `native-snapshot.log`,
  `native-promotion-current.log`, and `shutdown.log`. No new cl-spec contract
  was added. Bounded pressure, asynchronous acceptance proof, the logical CLOG
  overlay, OFF and full Patroni acceptance remain open; no coordination was added.

- [x] Add the generic transferable temporary cleanup domain foundation. At that
  checkpoint fresh private owners could be adopted before use, but no production
  caller adopted one; WAL integration and later work remained open. The checkpoint
  passed 829 unit, 1,413 physical, 201 wire,
  and 1,161 legacy tests, strict fresh compilation, native spill and live reload.
  Evidence is in `/home/imbaczek/proj/pg-ha-artifacts/cleanup-domain/`:
  `physical-suite.log`, `cold-final.log`, `native-spill-reviewed.log`,
  `reload.log`, `wire.log`, and `legacy.log`. The cl-spec property
  `cleanup-domain-adoption-preserves-private-owner-and-quota` ran 100 trials
  each at seeds 42 and 2707969488570409574 (digest
  `fnv1a64-v1:320a49a385e9fcd9`). This adoption contract has no Function Spec;
  input coverage and rejection counts were not measured.

- [x] Give primary WAL spools and shared dirty scratch store-owned cleanup
  authority (`ff63d497263`). Cleanup retries retain unresolved ownership across
  threads and storage boundaries reject remaining debt. Recovery page spools
  retain ordinary creator-thread cleanup ownership. This adds no tracked primary
  writer, asynchronous OFF, snapshot-prefix handling, or coordination. The
  checkpoint passed 834 unit, 1,413 physical, 201 wire and
  1,161 legacy checks, strict fresh compilation, 21 native page and five native
  CLOG cases (including production SQL FIFO completion on another thread), native
  spill and live reload. Evidence is in `/home/imbaczek/proj/pg-ha-artifacts/wal-cleanup/`:
  `physical-current.log`, `cold-final.log`, `native-pages.log`, `native-clog.log`,
  `native-spill.log`, `reload.log`, `wire-current.log`, and `legacy-current.log`.
  The 834 unit checks ran through the tool; there is no `unit-final.log` and no
  new cl-spec contract for this milestone.

- [x] Settle the pending primary WAL publication prefix before capturing a fresh
  snapshot (`034d03464c4`), keeping WAL locked through XID capture and selecting
  logical snapshot boundaries after settlement. Existing transaction and portal
  snapshot reuse remains unchanged. This adds no tracked primary WAL writer,
  OFF, or coordination.
  The checkpoint passed 837 unit checks through the tool, 1,413 physical, 1,161
  legacy, 201 wire, strict cold compilation, 21 native page and five native CLOG
  cases, and two native snapshot reuse cases. Evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/snapshot-prefix/`: `physical-initial.log`,
  `legacy.log`, `wire.log`, `cold-initial.log`, `native-pages-final.log`,
  `native-clog.log`, and `native.log`. There is no final unit log or new cl-spec
  contract for this milestone.

- [x] Implement the source sender-status foundation. The sender and activity
  SRFs can be joined by PID. Captured registration tokens enforce ownership;
  snapshots are detached copies with independent, nullable feedback positions.
  Connection cleanup retires the registration. Activity exposes only PID
  and application name for registered backends, including NULL enumeration.
  Lag is unknown and reported NULL; sync priority/state are 0/async. All SQL
  sessions currently use the postgres superuser; ordinary-role visibility
  filtering is not implemented. The source C oracle is
  `/tmp/pg-sender-status-c-oracle-fhw2qqsb/result.log`.

- [x] Support whole-row arguments and composite JSON conversion in the supported
  query subset. An unmatched outer row is SQL NULL, distinct from a present
  all-NULL record. Named composite types and whole-row RETURNING remain unsupported.
  C parity evidence is `/tmp/pg-sender-status-c-oracle-fhw2qqsb/whole-row-parity.log`.

- [x] Support `json_agg` in the supported aggregate and window subset. The
  sender-status JSON subquery is tested with real registry rows through both
  callback and iterator execution. Aggregate DISTINCT and ORDER BY remain
  unsupported. C parity evidence is
  `/tmp/pg-sender-status-c-oracle-fhw2qqsb/json-aggregate-parity.log`.

- [x] Establish the commit lifecycle prerequisite for later synchronous waiting:
  retain the XID through local apply and retire resources on an indeterminate
  poisoned WAL outcome. No remote wait or coordination was added. The existing
  no-WAL replacement and CLOG failure boundary is not claimed hardened. Final
  code passed 709 unit and 201 wire tests; 1,323 physical tests passed before a
  diagnostic-only first-error fix, covered by a new two-error unit test. Strict
  cold compilation passed (`/tmp/pg-commit-lifecycle-final-cold.log`), as did
  actual psql smoke (`/tmp/pg-commit-lifecycle-smoke.log`).

- [x] Establish a termination handoff for later synchronous waiting: publish
  pending termination before queuing the target interrupt, and consume it at
  delivery. No remote wait was added. The checkpoint passed 711 unit and 201
  wire tests, strict cold compilation, and the real shutdown test
  (`/tmp/pg-sync-termination-shutdown.log`).

- [x] Add the synchronous standby configuration parser and sender selection
  foundation. At this checkpoint no GUC, public synchronous state or commit wait
  was wired, and config, feedback and source fencing integration remained open.
  Restart the Lisp image for the sender structure layout change. The checkpoint
  passed 719 unit and 201 wire tests, plus seven existing sender physical
  regressions (not a full physical rerun); strict cold compilation passed
  (`/tmp/pg-syncrep-selection-final-cold.log`). The 31-case C parser oracle is
  `/tmp/pg-syncrep-parser-c-oracle.json`. Scoped cl-spec checks at seed 42 ran
  100 trials each: parser Function Spec (digest `2ff4d2ff15c97d0d`), quoted
  property (`a3f4bbb1a9500ce5`) and quorum property (`99cb3720a61bacdc`). Input
  coverage and rejection counts were not measured.

- [x] Add the private synchronous publication and wait core. Real worker
  threads modeled feedback, config, cancel and termination; SQL commit waiting
  was not tested or wired at that checkpoint. It passed 732 unit and 201 wire tests,
  seven existing sender physical tests (not a full physical rerun), and strict
  cold compilation (`/tmp/pg-sync-wait-reviewed-cold.log`). The scoped cl-spec
  queue-prefix property ran 100 trials at seed 42 (digest `9248d664822bed72`);
  input coverage and rejection counts were not measured. The user-XID wait is
  now wired; public settings, final DDL and maintenance policy, and asynchronous
  `synchronous_commit=off` remain open below.

- [x] Capture exact durable catalog terminal boundaries before local apply, with
  fail-stop publication and backend failure fencing (`a59b2256057`). Install a
  fresh source incarnation at startup, promotion and accepted follow or history
  changes; capture sender ownership during startup (`cdfa0829c6c`). Capture each
  transaction's own catalog and commit boundaries for future waiting
  (`809b80b6110`). The latest checkpoint passed 744 unit, 1,325 physical and 201
  wire tests, strict cold compilation (`/tmp/pg-transaction-obligation-cold.log`)
  and SQL smoke. This captures obligations only; it adds no remote wait.

- [x] Prepare and publish owned configuration roots under existing quiescence,
  with predecessor checks and follow generation publication (`cb3a1f3ee46`).
  The checkpoint passed 744 unit, 1,332 physical and 201 wire tests, strict cold
  compilation (`/tmp/pg-prepared-config-cold.log`), SQL smoke
  (`/tmp/pg-prepared-config-smoke.log`) and live valid/invalid reload checks.
  At that checkpoint production readers still used globals.

- [x] Capture one installed root for SHOW/settings rows, authentication backup
  and startup admission, and derive reload candidates from their captured base
  (`c907ba9d05b`). The checkpoint passed 744 unit, 1,337 physical and 201 wire
  tests, strict cold compilation (`/tmp/pg-config-readers-cold.log`), SQL smoke
  (`/tmp/pg-config-readers-smoke.log`) and live divergence/reload checks. No root
  means uninstalled; quota admission takes explicit captured limits.

- [x] Capture restartpoint settings before the store mutex (`99ade2667aa`) and
  pass one retention policy and keep size through each removal calculation and
  catalog snapshot. Owner claims and removal retries capture fresh
  settings while preserving the ticket's WAL endpoint. The checkpoint passed
  744 unit, 1,341 physical and 201 wire tests, plus 1,159 legacy checks, strict
  cold compilation and SQL smoke. No independent configuration service is added.

- [x] Fence receiver-local effects and desired generation publication
  (`074b04f0e48`). Admission precedes the receiver fence; network I/O and joins
  stay outside it. A superseded generation still flushes previously accepted
  bytes while its runtime owns the store; a retired runtime leaves it alone.
  Runtime publication and retirement use the same fence. The checkpoint passed
  749 unit, 1,350 physical and 201 wire tests, 1,159 legacy checks, strict cold
  compilation and SQL smoke. At that checkpoint controller durable publication
  and the independent configuration service remained open.

- [x] Fence controller intent and active publication and defer notification of
  captured obsolete senders until after fence release (`a50140c079c`,
  `54702573208`). The installer still requires quiescence. The checkpoint passed
  1,363 physical (`/tmp/controller-fence-final-physical.log`), 752 unit
  (`/tmp/controller-fence-final-unit.log`), 201 wire
  (`/tmp/controller-fence-final-wire.log`) and 1,159 legacy checks
  (`/tmp/controller-fence-legacy.log`), strict cold compilation
  (`/tmp/controller-fence-cold.log`) and SQL smoke
  (`/tmp/controller-fence-smoke.log`).

- [x] Close worker-start interruption gaps in `physical-follow-start-operation`
  and `physical-follow-launch-workers` (`8687a20236b`). Mask thread creation,
  owner publication and start-gate release together; failures before handoff
  completion release each child without storage work. The checkpoint passed 1,367 physical
  (`/tmp/worker-start-final-physical.log`), 752 unit
  (`/tmp/worker-start-final-unit.log`), 201 wire
  (`/tmp/worker-start-final-wire.log`) and 1,159 legacy checks
  (`/tmp/worker-start-legacy.log`), strict cold compilation
  (`/tmp/worker-start-cold.log`) and SQL smoke (`/tmp/worker-start-smoke.log`).

- [x] Fence private history selection and live promotion through source rotation,
  carrying sender retirement and completion through the live promotion
  (`88a00f72820`, `39bf35d5396`). The checkpoint passed 1,370 physical
  (`/tmp/source-lifecycle-final-physical.log`), 752 unit
  (`/tmp/source-lifecycle-final-unit.log`), 201 wire
  (`/tmp/source-lifecycle-final-wire.log`) and 1,159 legacy checks
  (`/tmp/source-lifecycle-final-legacy.log`), strict cold compilation
  (`/tmp/source-lifecycle-final-cold.log`) and SQL smoke
  (`/tmp/source-lifecycle-final-smoke.log`). Real HBA backup and promotion with
  `DEBUG_PROMOTION=1` passed (`/tmp/source-lifecycle-hba-debug.log`, artifacts
  `/tmp/pg-hba-policy.49dp84dn`).

- [x] Run configuration reload independently of code drain (`c1c7f634e8a`,
  `6476238b1fd`). Candidate preparation and publication are serialized with
  definition replacement without taking code reload admission. This supplies no
  failover coordinator.

- [x] Wait for a user XID's own locally committed WAL boundary using the private
  synchronous wait mode (`ad8194a65c6`). The checkpoint passed 1,407 physical
  tests plus three final focused cases, 754 unit, 201 wire and 1,159 legacy
  checks, strict cold compilation and an owned seven-case SQL script.

- [x] Capture explicit final DDL catalog boundaries and wait without an XID;
  XID-bearing DDL waits for its user commit using the prepared mode
  (`64877d34169`). The checkpoint passed 1,409 physical, 755 unit, 201 wire
  and 1,159 legacy checks, strict cold compilation, a focused no-WAL rerun and
  the native DDL scenario script. The scoped cl-spec property ran 100 trials at
  seed 42 (digest `942121ab116a97c6`); coverage and rejection counts were not
  measured.

- [x] Prepare synchronous replication configuration before publishing it under
  the sender lock (`a6eae66b69d`). The checkpoint passed 1,409 physical, 759
  unit, 201 wire and 1,159 legacy checks, strict cold compilation and native
  user and DDL scripts. The scoped cl-spec property ran 100 trials at seed 42
  (digest `3f289db7c7c4a86e`); input coverage and rejection counts were not
  measured. At that checkpoint root and generation wiring remained open.

- [x] Publish validated `synchronous_standby_names` with the configuration root
  (`6aad437aef5`). The SIGHUP string defaults to empty; SQL SET returns 55P02.
  The native rescue test preserves blocked commit and XID waits across three
  invalid reloads, releases both after clearing names, and shows SQL progress
  while a retained portal holds code reload until it closes. The checkpoint
  passed 1,413 physical, 760 unit, 201 wire and 1,159 legacy checks, strict cold
  compilation, native user and DDL scripts, and real configured follow. The
  scoped cl-spec property ran 100 trials at seed 42 (digest `5501aa7e8c09f1d4`);
  input coverage and rejection counts were not measured.

- [x] Support session and client `synchronous_commit` values `on`, `local`,
  `remote_write` and `remote_apply`, including SET LOCAL and RESET
  (`b57b9154ce1`). `on` waits for remote flush; `local` keeps local durability.
  OFF and false aliases return 0A000 pending true asynchronous commit. The
  checkpoint passed 1,413 physical, 772 unit, 201 wire and 1,159 legacy checks,
  strict cold compilation and native DDL, user SQL and default-change reload
  tests. The scoped property ran 100 trials at seed 42 (digest `76743fc438b0bd6f`);
  coverage and rejection counts were not measured. Configuration publication is
  independent, but SIGHUP also requests code reload and can pause new SQL while
  draining an active commit. The reload test uses a pre-admitted callback across
  SIGHUP; ordinary control evaluation also requires admission.

- [x] Report current FIRST/ANY sender priority and state from one sender-lock
  snapshot (`336ab343ffb`). The native status test uses an owned server, public
  reload and SQL with controlled sender registrations. Insufficient ANY peers
  still show quorum candidates; unmeasured lag is NULL. The checkpoint passed
  1,413 physical, 777 unit,
  201 wire and 1,159 legacy checks, strict cold compilation and the native status,
  user, DDL and reload tests. The physical run used a tracked private active-XID
  binding; the unit run followed clearing 247 FASLs. The property ran 100 trials
  each at seeds 42 and 98749024680794852 (digest `cdce92d88fc2e9ad`); input
  coverage and rejection counts were not measured.

- [x] Capture and validate immutable commit acceptance receipts (`44f402191f6`).
  For WAL-ready commits, the validator requires the observed durable callback and
  WAL proof. OFF remains rejected with 0A000. The checkpoint passed 1,413 physical, 786 unit,
  201 wire and 1,159 legacy checks, strict cold compilation, native receipt (8
  PASS) and user (7 PASS) tests, and DDL and reload tests. The property ran 100
  trials each at seeds 42 and 3286687836371262213 (digest
  `fnv1a64-v1:92a441ee7fa3a065`); rejection counts and input coverage were not
  measured.

- [x] Capture a commit policy once before publication (`e01e84fadb3`), keeping
  requested local durability and remote wait distinct from receipt facts. Final
  DDL retains that policy for both XID and no-XID completion; an XID after a
  published final catalog boundary forces effective local flush without changing
  the request or adding a remote wait. C likewise forces a flush for forced-sync
  commits and non-temporary relation deletion before files can be deleted; Lisp
  additionally waits on the final catalog boundary for no-XID DDL when requested.
  The structure layout change requires a fresh worker. Strict cold compilation,
  1,413 physical, 790 unit, 201 wire and 1,159 legacy checks passed. Native
  receipt (8), user (7), DDL and reload checks passed. The scoped property ran
  100 trials each at seeds 42 and 4538924396900646344, digest
  `fnv1a64-v1:11f68abafa1ea68c`; rejection counts and input coverage were not
  measured. True `synchronous_commit=off` remains pending CLOG/page overlay,
  common FIFO and WAL writer work; Patroni acceptance remains open.

- [x] Add the pending CLOG foundation (`8a2a92a720d`). Production commits
  still materialize immediately after WAL flush, so pending CLOG after return is
  not enabled. The checkpoint passed 1,413 physical, 797 unit, 201 wire and
  1,161 legacy checks, strict cold compilation, native receipt (8), user (7),
  DDL and reload checks. Four failure cases cover fail-stop and restart before
  and after the byte write, SIGKILL after staging, and SIGKILL after reset WAL
  flush before replacement; the last proves redo because a previously committed
  CLOG slot is zero after restart while a frozen row survives. Process kills do
  not simulate power loss. The
  fresh-process legacy fixture now resets poison and epoch, with two assertions;
  no old tests were ported. The property ran 100 trials each at seeds 42 and
  3263075368047784275, digest `fnv1a64-v1:0aa70a24580bcf9c`; rejection counts
  and input coverage were not measured.

- [x] Own pending page publication through durable materialization (`65409e21153`). A single
  owner retains the sealed spool and dirty maps at the exact terminal boundary;
  the current commit flushes and drains it immediately before acceptance.
  The checkpoint passed 801 unit, 1,413 physical, 201 wire and 1,161 legacy
  checks, strict cold compilation, nine native page, eight receipt and four
  CLOG failure cases. The ownership property ran 100 trials each at seeds 42 and
  3483828763443979182, digest `fnv1a64-v1:a52e06a995fdb660`; rejection counts
  and input coverage were not measured. A lifecycle fault fixture now isolates
  its recovery-failure flag. Native tests caught a pre-flush response leak and
  a guard outside WAL that rejected a healthy competing commit; both are fixed.

- [x] Order transaction and maintenance page publications in a common FIFO
  (`63c3684d70c`). Normal calls flush and drain immediately. Synchronous barriers
  settle predecessors; cold installation and identity changes reject debt. Resets
  force sync and settle predecessors before private publication. Catalog and drop
  closures are not queued. The native WAL format is unchanged.
  The checkpoint passed 807 unit, 1,413 physical, 201 wire and 1,161 legacy checks,
  strict cold compilation, 21 native page cases, eight receipt and seven user
  cases, DDL and reload checks, and four CLOG failure/crash cases. The prefix
  property ran 100 trials each at seeds 42 and 3282750418480750988, digest
  `fnv1a64-v1:81cff11f8481e2b2`; the ownership property ran 100 each at seeds
  42 and 145193323930899829, digest `fnv1a64-v1:a52e06a995fdb660`. Rejection
  counts and input coverage were not measured for either property.

- [x] Add the CLOG completion reservation prerequisite (`f87ede05484`). A
  reservation can precede the terminal append without exposing committed status;
  completion requires durable WAL. Production queue integration remains open,
  and OFF remains disabled. The checkpoint passed 811 unit tests, four native
  cases and strict cold compilation. The scoped property ran 100 trials each at
  seeds 42 and 265648549833292381, digest `fnv1a64-v1:93537ecfe22d4395`;
  rejection counts and input coverage were not measured.

- [x] Integrate CLOG completion with the publication FIFO (`ec9f21ebe3f`).
  Transaction owners reserve before terminal insertion, publish against the actual
  end, and materialize CLOG at the durable queue head before page retirement.
  Normal production commits still flush and drain synchronously; OFF remains
  disabled. The checkpoint passed 820 unit, 1,413 physical, 201 wire and 1,161
  legacy checks, strict cold compilation, five native CLOG cases (including
  cross-thread SQL and restart), 21 page, eight receipt and seven user cases,
  DDL and reload checks. The `queued-clog-and-pages-retire-the-same-prefix`
  property passed 100 trials each at seeds 42 and 4575550243879747699, digest
  `fnv1a64-v1:71d269dfe392cfa8`; rejection counts and input coverage were not
  measured, and there is no function spec. Logs are in
  `/home/imbaczek/proj/pg-ha-artifacts/queued-clog/`.

- [x] Implement asynchronous OFF using captured policy and acceptance receipts,
  a tracked primary writer, bounded retained-resource pressure and CLOG overlay.
  Preserve WAL-to-CLOG lock order and never put pending committed bits on disk
  before WAL. No coordination was added.

- [x] Implement `txid_current()` (`34044987044`) and the inline DO/PERFORM
  subset (`90fed2d98ff`) required by Patroni's unchanged sync-change method.
  The pinned monitor oracle (`f46d7c0ad0c`) passes its full query and unchanged
  sync handler on C and native primaries with an empty sender registry. C feedback
  clients check FIRST/ANY monitoring; real native Patroni followers also check
  one handler's FIRST-to-ANY transition. Evidence is
  `/home/imbaczek/proj/pg-ha-artifacts/patroni-sync/final.log`.

- [x] Exercise independent FIRST/ANY acknowledgment and crash boundaries against
  C and native peers (`b97973dab93`, `d36f2f45a0e`). The checkpoint passed 898
  unit, 1,416 physical, 201 wire and 1,161 legacy checks, strict cold compilation,
  50 C/native feedback cases and the Patroni regression. Native ANY 2 crash
  recovery cold-started both credited followers while the source stayed down.
  Evidence is `/home/imbaczek/proj/pg-ha-artifacts/sync-ack/`; the 100-trial
  reanchor property at seed 4017 had digest `fnv1a64-v1:a46ee7aac2e86822`, with no Function
  Spec for its target. The existing 200-trial property at seed 42 retained digest
  `fnv1a64-v1:23e4bc80c1e52a8e`.

- [x] Publish an allocated backend XID with its active registration under the XID
  lock and interrupt mask, following C's ownership-before-unlock ordering. Raw
  `assign-xid` without an owner remains caller-owned. Real SQL termination tests
  cover the reserved XID 3 and cached XID 4 paths, abort cleanup and clean fast
  shutdown (`/tmp/pg-xid-owner-sql.log`, artifacts `/tmp/pg-xid-owner-q296anu1`).
  The milestone passed 1,312 physical, 683 unit and 186 logical wire tests, a
  strict cold compile (`/tmp/pg-xid-owner-cold.log`) and Patroni reconnect and
  promotion (`/tmp/pg-xid-owner-patroni.log`).

- [x] Implement desired-generation reconciliation for `primary_conninfo` and
  `primary_slot_name` for native standard follow, including reload, durable
  management ownership, cold restart and promotion. `recovery_target_timeline`
  is active at cold startup and changes require restart. Archive-only recovery
  remains unsupported; coordination is outside this scope.

- [x] Guard direct WAL publication in `xid-assign-with-wal-reservation` and
  `wal-report-resource-parameters`. Their publication regions defer interrupts;
  preflight and lock waits remain cancellable. Controlled tests cover termination
  and storage failure. This targeted change does not establish WAL-wide coverage.
  One earlier source SIGINT poison log lacked a captured cause; controlled tests
  independently reproduced the asynchronous guard bug, and owned restart passed
  after the fix.

- [x] Fix signed integer GUC parsing for C23 binary prefixes independently of
  the unsigned recovery timeline parser. The signed converter uses C23 strtol
  when available and clears errno after symbol lookup. C/native metadata tests
  cover signs, units and malformed forms; 200 generated int32 cases pass.

- [x] Restore legacy unit-test runner fixture state after the suite. A warm worker
  retained `*active-xids* = (3)` after 1,598 passing checks, causing later physical
  promotion and shutdown checks to fail. Use the intended fresh legacy entrypoint
  for acceptance and inspect or isolate fixture state before later warm suites;
  do not restart an owned live SQL server to hide leaks. A later warm worker also
  had orphan fixture XIDs and cached streams pointing at deleted legacy PGDATA;
  the fresh intended legacy runner passed 1,159 checks.
  The harness now closes its own CLOG, WAL and relation streams before
  deleting its temporary data directory. Both VACUUM workers use that
  suite's CLOG and relation cache. The WAL retention fixture allocates
  a fresh active-XID list because release mutates it. Two runs in the
  same Lisp image each passed 1,172 checks and left no cached relation
  streams, CLOG or WAL stream, real XIDs, or virtual readers.

- [x] Preserve ownership revocation across live reloads. The stream and directory
  proof modules retain their weak revocation tables and mutexes for the lifetime
  of the image. The regression releases each flock, observes its loss, reacquires
  the lock on the same descriptor and reloads the ASDF-built module. The new
  definitions reject the old proofs, including a directory-owner copy made
  before revocation and the original owner after its active flag is restored.
  Deploy this change with a fresh image. The previous closures' private
  registries cannot be recovered by the first live load of these definitions.

- [x] Audit `wal-atomic-write-bytes` callers for temporary destination aliases.
  All 20 production call sites use fixed metadata names, generated names with
  fixed suffixes, or the standby backup writer's three selected WAL and control
  paths. The writer rejects the reserved `.tmp` destination suffix before I/O.
  The regression covers an existing destination and an absent destination.

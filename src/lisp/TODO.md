# Development TODO

## Native macOS and Windows portability

Put platform operations under `src/lisp/port` and select their implementations
through ASDF. Share POSIX code across Linux and Darwin where their semantics
match. Use native Windows handles where Windows has different semantics.
Reuse SBCL operations when they provide the required guarantees. Use a small
C binding compiled against native headers where constants, macros, or
structure layouts need one.

### Platform interfaces

- **Process identity and control.** Capture OS process creation time and keep
  `postmaster.pid` compatible. Retain Linux pidfds. Prototype process birth
  lookup and a private local endpoint on Darwin. On Windows, use a process
  handle and `GetProcessTimes` for identity and waiting, and evaluate an
  ACL-protected named pipe. Deliver shutdown, reload, and promotion as narrow
  requests bound to the verified owner so PID reuse or a rapid restart cannot
  redirect them. Keep these requests separate from the Lisp evaluator.
  See [C signal delivery](../port/kill.c) and the
  [Windows signal receiver](../backend/port/win32/signal.c).
- **Data-directory ownership.** Put lock acquisition, lifetime, filesystem
  identity, and revalidation behind an opaque owner. Keep stale PID
  reconciliation and startup ordering in the postmaster. Preserve Linux's
  descriptor proof. Prototype Darwin `flock` and Windows `LockFileEx` against
  lock loss and replacement. If revalidation cannot be reproduced, audit
  borrowing sites and make the lock handle private before changing the
  ownership contract. C PostgreSQL uses atomic PID-file creation and checks
  for a live process and surviving shared memory before removing stale
  evidence. See [C lock acquisition](../backend/utils/init/miscinit.c#L1231).
- **Files and publication.** Centralize binary opens, close-on-exec or
  noninheritance, private temporary creation, handle-relative lookup, and
  file and volume identity. Replace procfs-derived paths in physical images,
  WAL, and spill with operations anchored to the owned directory. Expose
  replacement, no-replace publication, and exchange as distinct operations.
  Preserve the caller's sync ordering and fail before mutation when a required
  filesystem guarantee is unavailable. A recoverable publication protocol
  needs its own crash tests. See [Windows open](../port/open.c),
  [Windows rename](../port/dirmod.c), and
  [C write-through and durable rename](../backend/storage/file/fd.c).
- **Libraries and data.** Centralize OpenSSL and libpq loading for SCRAM and
  password encryption. Preserve numeric settings parsing without a glibc
  symbol dependency. Resolve timezone data from the installation or a
  configured directory. See [C SCRAM primitives](../common/scram-common.c),
  [GUC parsing](../backend/utils/misc/guc.c), and
  [timezone lookup](../timezone/pgtz.c).

### Milestones

- [x] Extract process identity and PGDATA ownership on Linux first. Then route
  shared file opens and numeric parsing through the port. Preserve current
  behavior and add focused coverage for PID reuse, lock revocation, and
  descriptor inheritance where coverage is missing.
- [ ] Bring up native macOS with `initdb`, TCP/SCRAM, clean shutdown and restart
  against the same PGDATA. Reject a competing startup. Prototype Windows
  locking, file sharing, and process control early enough to inform the
  interfaces.
- [ ] Bring native Windows through the same checks. Exercise paths with spaces
  and non-ASCII characters, open-file rename, stale ownership after forced
  exit, and control across rapid restart. Prove local control before
  integrating service management.
- [ ] Verify spill recovery, WAL archival, physical replication, and slot and
  image publication on each supported filesystem. Inject faults around rename
  and sync, including a successful rename followed by a failed sync. Record
  which platform and filesystem combinations satisfy each feature's contract.

## Development and format notes

### Architecture decisions

- The [supported native formats](PHYSICAL-HA.md#supported-native-formats) table
  and [producer inventory](#milestone-0-producer-inventory) govern development
  upgrades. Retired formats need matching older code for export or migration.
  Native WAL and physical replication have their own on-disk and wire formats.
- Push and pull preserve their row lifetimes while sharing SQL operator
  semantics. Coercion follows the caller's SQL context, and expression
  traversal enters nested scopes only at their executable boundaries.
- Execution owners hold readers, snapshots and cursors through cleanup.
  Suspended portals retain their execution until retirement. Reload uses
  live-operation exclusion, and layout cutovers require a restart.
- WAL record framing, batch validation and replay fact derivation precede
  publication. Primary FIFO settlement and redo each retain their ordering,
  and selected EOR remains a separate validated transition.
- Shared metadata codecs preserve canonical bytes and checksums. Journal
  readers accept a proven complete prefix, while domain owners validate and
  publish the adoption or reseed steps.
- ASDF dependencies and package imports follow the verified interfaces.
  Fixture owners retain cleanup authority. The combined test runner reports
  both harnesses, and independent C and wire probes check protocol and SQL behavior.

Consult the corresponding PostgreSQL C implementation during planning and
implementation. Work REPL-first with SLIME or Sly and use cl-mcp for Lisp edits.
Use tilth or a code-search MCP for other code. Use cl-spec and Rove where useful
for new tests. Luna max reviews checkpoints and Sol high reviews major
milestones and full diffs. Consult Sol xhigh when uncertain and Astra high on
exceptionally difficult issues. After implementation, have Sol medium look for
macros that centralize invariants and reduce repetition. Refactor production
functions longer than 120 lines. In the polish pass, check naming, vacuous
assertions and error checks, unnecessary complexity, and duplication. Document
why existing code could not meet the need when a new implementation is required.

### Milestone 0 producer inventory

| Audit item | Current writer or owner | Compatibility and acceptance evidence |
|---|---|---|
| B1 | [Native WAL encoder](backend/storage/wal-record.lisp) | `wal-encode-record` stamps version 8. The maintenance replay, lineage byte preservation and receive byte preservation fixtures now use the current writer. |
| B2 | [Primary WAL control encoder](backend/storage/wal-checkpoint.lisp) | `wal-encode-control-image` writes PGWAL6, including variants for lifecycle metadata, retained references, resources, native checksums, and a local CLOG extent. |
| B3 | [Checkpoint codec](backend/storage/wal-checkpoint.lisp) | The current checkpoint payload is PGCKPT02. Its optional flags carry transaction and resource state used by current startup and recovery. |
| B4 | [B-tree page construction](backend/access/btree/btree.lisp) | New pages set their type via `+pd-flags-off+`. The current reader rejects the historical byte-8 layout. |
| B5 | [CREATE PUBLICATION](backend/executor/exec_simple.lisp) and the [catalog codec](backend/catalog/catalog.lisp) | Creation allocates the OID before catalog publication. Startup requires stored OIDs. |
| B6 | [Catalog codec](backend/catalog/catalog.lisp) | Fresh catalogs allocate a cluster identity. `catalog-to-sexp` persists it, and `catalog-from-sexp` requires it. |
| B7 | [Slot encoder](backend/storage/wal-slot-persistence.lisp) and [logical slot state codec](backend/replication/logical-slot-state.lisp) | The current producer can emit PGSLOT4 or PGSLOT5. Provenance and explicit loss causes use PGSLOT6, and child ownership uses PGSLOT7. |
| B8 | [Source pin encoder](backend/replication/physical-source.lisp) | Ordinary source pins use PGPHYSPIN2. Follow pins have a separate encoder. |
| B9 | [Receive control codec](backend/replication/physical-store.lisp) | The writer uses PGPHYS03 at 68, 80, or 96 bytes for base control, history selection, and retained-root identity. |
| B10 | [Recovery control codec](backend/replication/physical-recovery.lisp) | The writer uses version 3 for ordinary phases and version 4 for pending promotion. Current extensions, including retained identity, policy intents and source facts, stay within those versions. |
| B11 | [Physical backup writer](backend/replication/physical-bootstrap.lisp) | New backup manifests use version 3 with explicit checkpoint start, derived from the checkpoint record. |
| B12 | [Restartpoint generation codec](backend/replication/physical-restartpoint.lisp) | Standby publication and logical-copy source proof emit PGRPGEN2 with metadata version 4. The XID process fixture constructs retired generations independently to test startup rejection. |
| B13 | [Live-operation admission](backend/tcop/control.lisp) | In `87b987c4be0`, the gate closes and reopens with a quiescence owner token. Cold restart crosses the image layout boundary. Six strict checks covered borrowed admission, unwind, canceled drain, and terminal shutdown. |
| B14 | [Backend transaction spool](backend/access/transam/xact.lisp) | In `509bc1c6992`, the backend owns a spool for logical row changes. Cold restart crosses the layout boundary. Live capture verified rollback, disconnect, restart, and corrupt spool refusal. |
| B15 | [Owned CLOG completion](backend/access/transam/clog.lisp) | In `0dbafd57add`, legacy fixture calls moved to owned completion. Strict CLOG tests, live commit receipts, and asynchronous commit verified tag checks, failure cleanup, and restart. |
| B16 | [Physical SELECT plans](backend/executor/exec_plan.lisp) | In `05af2a42ac8c`, the uncalled collectors and streaming wrappers were removed. The former test-only caller executes a live plan. Live joins retain the scan and result-row helpers. |
| B17 | [Extended statements and portals](backend/tcop/extended-query.lisp) | In `0ebd131b2eb`, metadata moved to its statement or portal. Cold restart crosses the layout boundary. Live portal lifecycle and C protocol comparisons verified cleanup, suspension, and result-type checks. |
| B18 | [WAL initialization](backend/storage/wal-core.lisp) | Fresh and existing serving clusters require qualified filenames. In `96d4aa5d8ba`, filename encoding, lineage scanning, and logical reading accept only that namespace. The startup guard still recognizes retired filenames for refusal. A physical base copy preserves the source control and copies qualified WAL entries. |

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

## Shared page cache for pg-lisp workers

SQL workers run as SBCL threads in one process and can share immutable page
vectors. The owned buffer API copies visible WAL pages and reads clean pages
from storage. Use a bounded cache of page versions so read-only workers can
reuse those vectors. Keep owned reads for callers that edit pages.

### 1. Borrow resident WAL pages in heap scans

- Add scoped read-only access in [bufmgr.lisp](backend/storage/bufmgr.lisp)
  and an explicit handle for cursors that suspend mid-page. Keep the same
  page version across cursor pulls and keep returned tuple data and TOAST
  state independent of the handle. Register and release handles without an
  interrupt window. Release on exhaustion, close, cancellation, and error.
  Make release idempotent and effectively infallible, or give it an owner
  that can retry independently of iterator close.
- Preserve exact-fork lookup precedence. A transaction-private replacement
  shadows the whole fork, including missing blocks. Otherwise look in the
  current WAL batch, shared dirty map, and newest pending publication before
  falling back to storage. Borrow only resident WAL dirty references whose
  keys match. Keep the owned read path for scratch, compatibility entries,
  and other ineligible reads. Preserve layout and checksum checks, scratch
  validation, and WAL fail-stop behavior.
- Bound borrowed memory separately from WAL dirty-store ownership. Account
  for each distinct version still held after WAL retires it and fall back to
  an owned read under pressure. Reader handles must allow WAL to retire its
  references and close scratch normally. Capture the handle under the WAL
  page-state lock and release the lock before tuple visibility checks, TOAST
  reads, or scan callbacks.
- Existing handles keep their page and release against their original
  accounting owner through online promotion. New lookups use the current
  storage domain. Use the suspended cursor in
  [promotion-readers.py](t/interop/promotion-readers.py) to exercise this.

### 2. Cache validated clean pages

- Key entries by storage incarnation, relfilenode, fork, and block. Admit
  complete pages after layout and checksum validation. Use bounded eviction
  that accounts for versions held by readers and allow uncached reads when
  the cache is full of borrowed pages. Follow the existing WAL-before-smgr
  lock order.
- Serialize clean fills and invalidation at the physical mutation boundary.
  Cover direct and WAL page-image writes, extension, fork reset, relation
  drop, stream close or reopen, and storage domain changes. Invalidate before
  a write that could fail partway. A reader holding a retired version can
  finish with it, and later lookups must resolve the current version. Keep
  physical verification reads independent of the cache.
- Audit read-only B-tree, visibility-map, and maintenance callers for page
  mutation and escaping references before migrating them.

### 3. Transfer write ownership where callers permit it

Let audited writers edit a private version and give up its vector when they
call WAL staging. Keep the copy-taking API for callers that reuse their
input. Audit LSN and checksum stamping and page-image construction for
in-place writes to published vectors. Preserve atomic batch publication,
contributor tracking, dirty spill ownership, and WAL-before-data ordering.

### Verification

- For the first patch, hold a page in a suspended cursor while a writer
  publishes its replacement. Verify the cursor keeps its version, a later
  acquisition gets the new version, and close or cancellation releases the
  handle. Cover borrow-budget exhaustion, dirty spill, and online promotion.
- For the clean cache, exercise concurrent scans, tiny-cache eviction,
  partial write failure, TRUNCATE and DROP, checkpoint, and replay. Compare
  heap scan pinning and invalidation with
  [heapam.c](../backend/access/heap/heapam.c) and
  [bufmgr.c](../backend/storage/buffer/bufmgr.c).
- Benchmark resident dirty hits, spilled dirty reads, warm clean hits, and
  large streaming scans separately. Measure full-page copies, physical reads,
  allocation, retained versions, WAL-lock wait, and writer latency. Require
  zero full-page copies on eligible resident dirty hits in the first patch
  and on warm read-only clean-cache hits once the clean cache lands.

### Shared page benchmark

A local SBCL microbenchmark compared the existing owned buffer API with
scoped reads over the same relation. The warm working set was 32 pages and
the streaming working set was 2,048 pages. The clean cache held 512 pages.
The warm clean and resident dirty samples returned the shared vector on
every scoped read. The suspended scan test separately verifies retirement
and release of old page versions.

| Workload | Reads | Physical reads, owned → scoped | Bytes allocated per read, owned → scoped |
| --- | ---: | ---: | ---: |
| Warm clean | 64,000 | 64,000 → 0 | 16,495 → 96 |
| Streaming clean | 16,384 | 16,384 → 16,352 | 16,493 → 8,339 |
| Resident dirty | 16,000 | 0 → 0 | 8,256 → 49 |
| Spilled dirty | 16,000 | 0 → 0 | 24,767 → 16,560 |

The physical-read column counts relation reads. Each spilled dirty read
still reads its WAL scratch slot. In a separate run with two clean readers
and one staging writer, mean WAL mutex wait per acquisition fell from
6.88 to 0.90 microseconds for readers and from 7.71 to 2.13 microseconds for
the writer. Writer median staging latency was 2 microseconds in both
samples, and the 95th percentile fell from 53 to 23 microseconds.

## Query observability

The observability plans target the SBCL server at `f110a918b0079`.
P01 owns the query identity and measurement API. P02 can start as soon as
that API covers nested scopes, portal ownership, and counter snapshots.
Shared roadmap summary

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
  Initial memory comparison used SBCL 2.6.10.334-ab0880140 and
  `t/workload-memory.py --rows 2048 --repeat 3 --runs 3 --case point_reads --case scan --case sort --case mixed`,
  once with `--statement-statistics` and once without it. Medians across fresh
  servers are below. The short-query optimization target is under 5%
  throughput loss with collection enabled.

  | Workload | Tracked calls | Allocated bytes off → on | Peak RSS bytes off → on | Post-GC heap bytes off → on |
  | --- | ---: | ---: | ---: | ---: |
  | Indexed point reads | 192 | 12272576 → 12894688 | 114667520 → 115367936 | 47524656 → 47562016 |
  | Aggregate scan | 0 | 4230640 → 4231232 | 106733568 → 106688512 | 47496416 → 47533616 |
  | Sort | 0 | 6218112 → 6218032 | 108695552 → 108695552 | 47520560 → 47550320 |
  | Four-client mixed read/write | 384 | 375916656 → 381088256 | 479100928 → 484274176 | 47846768 → 47853024 |

  The initial aggregate scan and sort rows had no identity coverage.
  Point reads took
  68.5 → 75.8 ms per sample and mixed took 1.076 → 1.094 s.
  Fixed-width hashing preserved the version-one query ID. In the REPL,
  hashing a prebuilt point-read shape 10000 times reduced measured
  allocation from 21917424 bytes to zero for that fixnum-valued query ID.
  Five fresh-server point-read samples per mode measured 12272560 bytes
  off and 12471552 bytes on, with 192 calls tracked per enabled sample.
  The enabled allocation premium fell from 622112 to 198992 bytes.
  Median elapsed time was 69.9 → 73.1 ms, about 4.5% lower throughput.
  After adding ordered SELECT fingerprints, the three-sort sample
  tracked all three calls. Across three fresh servers per mode, elapsed
  time was 30.9 → 31.3 ms and allocated bytes were 6217952 → 6207408.
  Peak RSS and post-GC heap each differed by less than 5 KiB in this run.
  After aggregate fingerprints were added, the plain aggregate scan and
  grouped aggregate each tracked all three calls. Median allocation was
  4230480 → 4254784 bytes for the scan and 5938592 → 5939792 bytes for
  the grouped aggregate. Peak RSS differed by less than 80 KiB for each.
  The benchmark also reports process CPU time and client SQL throughput,
  median latency, and p95 latency. CPU time has Linux scheduler-tick
  resolution. Client latency spans each simple-query protocol round trip,
  including transaction commands in the mixed workload. Peak RSS is
  sampled every 5 ms. GC time covers the server workload window, with
  forced heap snapshots outside that window.

  A fresh off/on comparison after adding join, builtin, and set-operation
  identities used three fresh servers per mode and the same workload
  sizes as the initial run. Each cell shows the sample median, off → on.

  | Workload | Tracked calls | SQL queries/s | Median / p95 latency (ms) | Server CPU (s) |
  | --- | ---: | ---: | ---: | ---: |
  | Indexed point reads | 192 | 2558 → 2695 | 0.374 / 0.577 → 0.360 / 0.459 | 0.04 → 0.04 |
  | Aggregate scan | 3 | 458 → 461 | 2.006 / 2.555 → 2.042 / 2.518 | 0 → 0 |
  | Sort | 3 | 98.9 → 93.4 | 9.997 / 10.510 → 10.660 / 10.880 | 0.01 → 0.01 |
  | Four-client mixed read/write | 384 | 702 → 704 | 4.510 / 16.230 → 4.470 / 16.280 | 0.89 → 0.88 |

  | Workload | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
  | --- | ---: | ---: | ---: |
  | Indexed point reads | 12268192 → 12466560 | 114651136 → 115003392 | 47518976 → 47590160 |
  | Aggregate scan | 4230480 → 4254784 | 106704896 → 106647552 | 47514736 → 47522832 |
  | Sort | 6217952 → 6207424 | 108605440 → 108556288 | 47484640 → 47523424 |
  | Four-client mixed read/write | 380327456 → 378746672 | 483598336 → 482037760 | 47848176 → 47932000 |

  All samples reported 0 ms median process GC time. The point-read
  allocation premium was 198368 bytes (1.6%). Sort throughput moved
  in opposite directions across runs of similar size, so the three-query
  samples do not establish a steady-state throughput difference.
  A later warm point-read run after the portal and statistics-store
  fixes measured 46486176 → 47252944 allocated bytes, 149389312 →
  150274048 bytes peak RSS, and 47658752 → 47701600 bytes post-GC
  heap. It used three fresh servers per mode, 2048 rows, and 768
  tracked calls per enabled sample, with planning tracking off.
  The allocation premium was 1.65%. A concurrent unit-suite run
  makes the elapsed-time comparison unsuitable for a throughput claim.
  A separate warm run without the unit suite used four fresh servers
  per mode in alternating two-server groups, with 1024 lookups per
  sample. Median throughput was 2741 → 2597 queries/s (5.3% lower
  with tracking), median client latency 0.351 → 0.372 ms, and p95
  0.454 → 0.488 ms. Allocation was 61872456 → 62937392 bytes (1.7%),
  peak RSS 164784128 → 165941248 bytes, and post-GC heap
  47696136 → 47721968 bytes. This run is slightly above the 5%
  short-query throughput budget.
  In an isolated renderer check with prebuilt point-read tokens,
  writing single-digit placeholders directly reduced median CPU time
  per 100000 renders from 41.4 to 38.3 ms. Allocation stayed near
  32 MB. This measures only the renderer.
  A subsequent alternating off/on run after the renderer change used four
  fresh servers per mode and 1536 warmed lookups per sample. The medians
  were 2861 → 2580 queries/s (9.8% lower with tracking), 0.339 → 0.380 ms
  client latency, 0.428 → 0.461 ms p95, and 0.295 → 0.320 s server CPU.
  Allocation was 92796128 → 94402856 bytes (1.7%), peak RSS was
  195796992 → 197328896 bytes, and post-GC heap was 47693168 → 47689416
  bytes. Each enabled sample counted 1536 calls. Enabled throughput
  ranged from 2058 to 2862 queries/s. The rerun still exceeds the
  throughput budget and does not isolate the cost of the renderer change.
  A separate SB-SPROF CPU run sampled 8000 warmed lookups per mode at
  2 ms intervals. It collected 393 samples with tracking off and 416
  with tracking on. The enabled profile attributed three inclusive
  samples to query fingerprinting, two to representative text, and
  one to the statistics store update. The sample counts do not isolate
  a single hot tracking function. Server CPU time was 1.57 → 1.66 s.
  A follow-up ran four fresh servers per mode with 1536 warmed point
  reads per sample. In disposable servers only, the control REPL
  substituted either the template's representative string or a fixed
  query ID after setup. Both substitutions kept the statement store
  active, and every sample counted 1536 calls with zero evictions.
  Normal tracking, fixed text, and fixed fingerprint used median
  allocations of 94402856, 93930160, and 93746552 bytes. Median
  throughput was 2633, 2723, and 2778 queries/s, and server CPU was
  0.310, 0.315, and 0.305 s. Peak RSS differed by under 0.3 MB and
  post-GC heap by under 18 KB. These substitutions estimate the
  individual costs on a single query shape. They do not qualify a
  general-purpose replacement, and the elapsed-time effect is larger
  than the CPU-clock difference in these short samples.
  Short simple-query messages now defer normalization until a statistics
  entry is missing. The renderer runs outside the shard lock, then
  publication rechecks the key. Extended Parse and simple messages over
  4096 characters render eagerly, bounding how much scanner input a
  deferred renderer can retain. The store retains only bounded strings.
  A Rove fixture checks that a repeat hit skips rendering and that a
  reset during rendering permits publication into the current entry.
  In the first four-sample rerun with 1536 lookups per sample, enabled
  median allocation fell from 94402856 to 94003488 bytes after deferring
  text. Every enabled sample still recorded all calls with zero evictions.
  A second, balanced-order run used four fresh servers per mode and
  2048 lookups per sample. Tracking off → on, median throughput was
  2679 → 2691 queries/s, median latency 0.357 → 0.361 ms, p95
  0.447 → 0.454 ms, and server CPU 0.405 → 0.415 s. Measured
  allocation was 123634896 → 125209416 bytes (1.27%), peak RSS
  226750464 → 228210688 bytes, and post-GC heap 47734712 → 47707840
  bytes. The enabled samples all recorded 2048 calls and zero evictions.
  The median met the 5% throughput budget in this run. Dropping the
  first pair, which contained the slowest disabled sample, yields a
  3.3% throughput loss with tracking enabled.
  To exercise same-entry contention, `t/workload-memory.py --rows 2048
  --repeat 24 --runs 3 --case concurrent_point_reads --warmup` ran the
  same 1536 lookups across four connections. With tracking off → on,
  median throughput was 3747 → 3691 queries/s, client median latency
  0.877 → 0.907 ms, p95 2.049 → 2.039 ms, and server CPU 0.44 s in
  both modes. Allocation was 93177088 → 94836784 bytes, peak RSS
  196460544 → 198004736 bytes, and post-GC heap 47819376 → 47826496
  bytes. The enabled runs recorded all 1536 calls and zero evictions.
  In a separate probe run, a temporary nonblocking attempt counted 18
  failed immediate acquisitions of the statement-update shard mutex
  across 8000 measured and 64 warmup lookups (0.22%). The probe retried
  through the blocking lock path and was omitted from timing samples.
  A warmed 16384-row aggregate scan exercised 32 queries in three
  fresh servers per mode. The serial case used `--case scan --repeat 32`
  and the four-client case used `--case concurrent_scan --repeat 8`.
  Tracking counted 32 calls with zero evictions in every enabled sample.
  The medians below compare tracking off → on. Median process GC time
  was zero throughout.

  | Clients | Queries/s | Median / p95 (ms) | Server CPU (s) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
  | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
  | 1 | 81.4 → 81.2 | 12.234 / 12.791 → 12.142 / 12.955 | 0.39 → 0.39 | 297240416 → 297305136 | 405155840 → 405200896 | 49483520 → 49452720 |
  | 4 | 269.5 → 282.7 | 13.356 / 18.732 → 13.162 / 13.861 | 0.43 → 0.41 | 297670768 → 297683728 | 405913600 → 405893120 | 49574336 → 49669680 |

  Warmed mixed read/write samples used `--rows 4096 --repeat 6 --runs 3
  --warmup` with `--case mixed_serial` (one connection) or
  `--case mixed` (four). Each sample ran 384 transactions with a point
  SELECT and UPDATE in each, plus the transaction commands. Every enabled
  sample recorded 768 calls and zero evictions. The fixture checked the
  selected keys and verified the resulting group sum. The table reports
  medians, tracking off → on.

  | Clients | Queries/s | Median / p95 (ms) | Server CPU (s) | GC (ms) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
  | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
  | 1 | 393.7 → 390.1 | 2.372 / 5.083 → 2.403 / 5.076 | 2.10 → 2.09 | 2 → 2 | 1454584240 → 1455514736 | 982302720 → 982401024 | 47909936 → 47935728 |
  | 4 | 587.1 → 591.4 | 4.492 / 20.858 → 4.442 / 20.460 | 3.09 → 3.06 | 2 → 3 | 1388598128 → 1387877616 | 965054464 → 965181440 | 48210240 → 48212960 |

  A separate enabled serial mixed run hit the 20000-sample cap of
  SB-SPROF's allocation profiler. Among the captured replenishments,
  the direct caller of the allocator was tuple construction in 24.5%,
  heap tuple deformation in 23.9%, and byte-to-string decoding in
  25.0%. The UPDATE candidate heap scan accounted for 86.8% of the
  sampled stacks. The profile covers only part of the run. It identifies
  candidate scanning and tuple decoding as the main opportunities in
  the roughly 1.45 GB allocated by either serial mixed mode.
  Source-free UPDATE and DELETE with a sole integer column equality
  on a table with variable-length columns now screen visible tuples
  before full deformation. The screen uses
  the heap's existing single-column demand to skip unrelated varlena
  decoding. Matching rows still run the normal predicate, and a changed
  target version still runs the concurrent-update recheck. The narrow
  deformation still walks the row and allocates a values vector, so
  the benefit is greatest when many rows with variable-length data fail
  the predicate.
  Repeating the serial mixed benchmark on three fresh servers per mode
  gave the following medians before → after the change.

  | Tracking | Queries/s | Server CPU (s) | GC (ms) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: |
  | Off | 393.7 → 559.2 | 2.10 → 1.00 | 2 → 0 | 1454584240 → 297242624 | 982302720 → 416665600 | 47909936 → 48029008 |
  | On | 390.1 → 517.3 | 2.09 → 1.11 | 2 → 0 | 1455514736 → 298299824 | 982401024 → 417460224 | 47935728 → 47998304 |
  | Off, four clients | 587.1 → 638.0 | 3.09 → 2.04 | 2 → 0 | 1388598128 → 231306768 | 965054464 → 335204352 | 48210240 → 48195776 |
  | On, four clients | 591.4 → 643.5 | 3.06 → 2.01 | 3 → 0 | 1387877616 → 233101232 | 965181440 → 337272832 | 48212960 → 48201440 |

  The four-client rerun used the same fixture and sample count with
  the variable-length-column guard in place. Each enabled sample still
  recorded 768 calls with zero evictions. Rove checks visibility before
  screening and full values on matches. The SQL wire fixture changes
  a matching row's key in another transaction and verifies that the
  blocked UPDATE leaves that newer version alone.

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
  A point-read rerun after adding sort metadata used three fresh
  servers per tracking mode and 2048 warmed lookups per sample.
  Tracking off → on, median throughput was 2686 → 2658 queries/s,
  allocation 123649168 → 125196048 bytes, peak RSS 226631680 →
  228241408 bytes, and post-GC heap 47722912 → 47731936 bytes.
  Every enabled sample counted 2048 calls and zero evictions.
  A lateral nested-loop fixture forced a sequential inner scan and
  disabled memoization on the C peer. Both returned one row from a
  2048-row input, ran the inner Limit twice, and reported 0.5 inner
  output rows per loop. The filter rejected 15 rows on its successful
  loop and 2048 on the other, reported as 1032 rows removed per loop
  after rounding on both peers. C puts the count on Seq Scan and native
  puts it on the explicit Filter node. Both suppressed node times and
  costs in JSON when TIMING OFF and COSTS OFF were set.
  The memory benchmark's `--warmup` and `--explain-mode` options run the
  same read-only queries through plain SQL or JSON EXPLAIN ANALYZE. The
  EXPLAIN modes are `rows` (BUFFERS OFF, TIMING OFF), `counters`
  (BUFFERS ON, TIMING OFF), and `timing` (BUFFERS ON, TIMING ON).
  All use statement tracking off and suppress plan estimates. Compare EXPLAIN modes directly to
  see the incremental instrumentation and reporting costs. Plain SQL
  can send more data, particularly when returning the full sorted result.

  With `--rows 2048 --repeat 12 --runs 3 --warmup`, the medians below
  compare plain SQL, rows-only EXPLAIN, counters, and counters with timing
  in that order. Each point-read sample ran 768 queries. The other samples
  ran 12 queries each.

  | Workload | Queries/s | Client p95 (ms) | Server CPU (s) |
  | --- | --- | --- | --- |
  | Indexed point reads | 2837 / 2332 / 2391 / 2508 | 0.433 / 0.512 / 0.501 / 0.463 | 0.16 / 0.16 / 0.18 / 0.17 |
  | Aggregate scan | 535 / 458 / 490 / 471 | 2.143 / 2.631 / 2.367 / 2.424 | 0.02 / 0.02 / 0.02 / 0.02 |
  | Sort | 102 / 335 / 332 / 319 | 10.464 / 3.346 / 3.444 / 3.350 | 0.04 / 0.03 / 0.04 / 0.03 |
  | Join | 121 / 116 / 117 / 114 | 8.584 / 9.159 / 9.069 / 9.376 | 0.10 / 0.10 / 0.10 / 0.11 |

  | Workload | Allocated bytes | Peak RSS bytes |
  | --- | --- | --- |
  | Indexed point reads | 46572512 / 50843552 / 60022560 / 62583296 | 149426176 / 153718784 / 162930688 / 165478400 |
  | Aggregate scan | 14396864 / 14462464 / 14586832 / 14656192 | 117219328 / 117334016 / 117493760 / 117563392 |
  | Sort | 22321280 / 18374448 / 18526464 / 18562432 | 125173760 / 121114624 / 121225216 / 121303040 |
  | Join | 39373616 / 39542352 / 39750064 / 39812144 | 142282752 / 142462976 / 142684160 / 142761984 |

  Median post-GC heaps differed by less than 40 KiB across modes for each
  workload. Median process GC time was 0 ms. Counters added 9179008
  allocated bytes over rows-only EXPLAIN for the point-read sample.
  SB-SPROF attributed 16.8% of sampled allocation to JSON plan
  formatting. Writing the seven fixed resource labels as JSON syntax
  reduced the counters sample from 60022560 to 59907248 allocated
  bytes. This recovered 115312 bytes of the measured premium.
  Reserving capacity once for each encoded frontend field and then
  copying it in bulk reduced the same sample to 57464288 allocated
  bytes and 160432128 bytes peak RSS. This recovered another 2442960
  allocated bytes and 2412544 bytes peak RSS. Median elapsed time was
  331 → 330 ms for the two runs after the JSON label change.
  A later warmed 12-sort rerun on the projected-row path measured
  18390144 allocated bytes with rows only, 18488448 with counters, and
  18521024 with timing. Peak RSS ranged from 121262080 to 121372672
  bytes, and post-GC heap from 47592592 to 47656672 bytes. These samples
  used the default work-memory budget and overlapped the unit suite.

  The same four-client point-read workload compared the EXPLAIN modes
  with tracking off. Rows only, counters, and node timing respectively
  used 101799968, 116449200, and 121961824 allocated bytes. Peak RSS was
  205160448, 219783168, and 225198080 bytes, and post-GC heap ranged
  from 47755360 to 47797392 bytes with 0 ms median GC time. Throughput
  was 3947, 4118, and 4072 queries/s, median latency was 0.726, 0.669,
  and 0.702 ms, and p95 was 2.106, 2.064, and 2.050 ms. Server CPU
  time was 0.43, 0.42, and 0.44 s. These three samples per mode show
  stepped memory costs with throughput varying across the modes.

  The warmed 16384-row scans also ran all three EXPLAIN modes with
  tracking off. Each series is rows only / counters / counters with
  node timing, with 32 queries per sample and three fresh servers per mode.
  Median process GC time was zero in all six cases.

  | Clients | Queries/s | Median client latency (ms) | Client p95 (ms) | Server CPU (s) |
  | ---: | ---: | ---: | ---: | ---: |
  | 1 | 80.0 / 78.0 / 76.6 | 12.442 / 12.766 / 13.021 | 13.113 / 13.284 / 13.769 | 0.39 / 0.40 / 0.40 |
  | 4 | 281.9 / 268.6 / 269.9 | 13.312 / 13.564 / 13.875 | 14.395 / 14.690 / 15.744 | 0.41 / 0.43 / 0.44 |

  | Clients | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
  | ---: | ---: | ---: | ---: |
  | 1 | 297374576 / 297689728 / 297880528 | 405303296 / 405659648 / 405811200 | 49459920 / 49521088 / 49451760 |
  | 4 | 297751664 / 298189360 / 298330304 | 406110208 / 406462464 / 406593536 | 49629472 / 49729072 / 49760608 |

  The activity polling benchmark used 30 admitted connections on one
  server, with four clients doing 1024 warmed indexed reads each. The
  poller selected PID, state, wait type, wait name, query ID, and query
  text at a 10 ms interval. A control returned six comparable fields
  from a 30-row table. The regular Python wire client reads type, length,
  and body separately from the socket. With that client, both polling
  queries reduced measured throughput from about 3692 to about 1610
  lookups/s. The buffered rerun received up to 64 KiB at a time and
  parsed complete frames from those bytes. Both polls then ran at the
  target cadence without the large throughput drop.

  | Poll source | Lookups/s | Lookup median / p95 (ms) | Poll median / p95 (ms) | Server CPU (s) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
  | None | 6617 | 0.553 / 0.890 | — | 1.24 | 249727392 | 358100992 | 48655536 |
  | Activity | 6494 | 0.575 / 0.927 | 0.725 / 1.053 | 1.29 | 253619504 | 362053632 | 48712800 |
  | Control table | 6402 | 0.581 / 0.939 | 0.746 / 1.172 | 1.29 | 253367856 | 361701376 | 48679888 |

  Each mode used three fresh servers in rotated order. Median activity
  and control poll counts were 62 and 63. Every sample tracked exactly
  4096 lookups, and median process GC time was zero in all modes.

  The final cross-mode run used the same buffered 30-connection
  workload with activity polling at 100 Hz in every mode. Statement
  collection was disabled, all four workers checked 1024 indexed
  lookups each, and each mode ran on three fresh servers in rotated
  order. Mode medians follow. Rows only, counters, and timing used
  JSON EXPLAIN ANALYZE with COSTS OFF and progressively enabled
  BUFFERS and TIMING.

  | Mode | Lookups/s | Lookup median / p95 (ms) | Poll median / p95 (ms) | Server CPU (s) | Allocated bytes | Peak RSS bytes | Post-GC heap bytes |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
  | Plain SQL | 6613 | 0.556 / 0.905 | 0.665 / 1.040 | 1.24 | 250443568 | 358772736 | 48744112 |
  | Rows only | 6612 | 0.529 / 0.881 | 0.621 / 1.011 | 1.15 | 273504416 | 382078976 | 48740960 |
  | Counters | 6393 | 0.529 / 0.923 | 0.642 / 0.992 | 1.18 | 313242032 | 421699584 | 48674880 |
  | Counters and timing | 6397 | 0.541 / 0.932 | 0.645 / 0.989 | 1.20 | 327919232 | 436260864 | 48741168 |

  Median process GC time was zero in all modes. The three EXPLAIN
  variants all checked the same actual row count and presence of
  the requested fields. A separate disposable-server probe replaced
  the blocking activity mutex acquisition at frontend begin and
  end with one immediate try before falling back to the blocking
  path. With 4096 lookups and 59–62 polls per sample, misses in the
  four modes were 152, 133, 137, and 137, respectively, across
  roughly 8310 acquisitions per mode (1.6–1.8%). These counts
  cover the two frontend activity publication sites and exclude
  timing samples. They do not measure lock hold durations.

## P03 — Composite range and ordered B-tree scans

The range cursor preserves the existing index format and equality cursor. It
uses encoded leading-key prefixes with inclusive or exclusive endpoints and
traverses parent frames in either direction under the existing index lease.
SELECT applies the original filter after fetching visible heap tuples. An
ordered LIMIT can omit the sort when the leading equality constraints and
the physical key order prove the requested direction and NULL placement.

The localhost wire smoke test is `python3 src/lisp/t/btree-range.py`.
It covers composite bounds, empty results, ordered LIMIT, NULL placement,
portal suspension, and rebinding. The allocation comparison used three warm
runs with 16,384 rows and 100 ordered LIMIT queries per run.

```sh
python3 src/lisp/t/workload-memory.py --case composite_range --case composite_range_seq \
    --rows 16384 --repeat 100 --runs 3 --warmup
```

| Path | Allocated bytes | Peak RSS bytes | Median SQL latency |
| --- | ---: | ---: | ---: |
| Index with eager leaf decoding | 8,642,240 | 114,536,448 | 0.486 ms |
| Index with leaf images and on-demand decoding | 7,155,280 | 113,049,600 | 0.451 ms |
| Sequential reference | 799,807,840 | 909,606,912 | 12.577 ms |

Three warm runs at 131,072 rows with 20 ordered LIMIT queries each measured
1,841,696 bytes allocated and 0.500 ms median query latency with the index.
The sequential reference allocated 1,937,763,056 bytes at 89.358 ms median
latency. Peak RSS was 111,001,600 and 1,004,949,504 bytes respectively.

One warm run at 1,000,000 rows with three ordered LIMIT queries allocated
353,280 bytes with the index and 2,216,342,848 bytes for the sequential
reference. Median query latency was 0.680 ms and 676.746 ms, respectively.
Peak RSS was 110,612,480 bytes and 1,002,242,048 bytes, respectively.

The wire test also checks results after an immediate stop and restart.
With `--reference-bin`, it compares 144 queries with C PostgreSQL, including
keys with 700 duplicates at each endpoint, crossed bounds, and both directions.
The physical detachment test confirms that a parked backward range releases
its lease, permits a write, and resumes its original candidate stream. A Rove
regression executes a stale ordered plan after replacing its index path and
checks the top-N results.

The skewed range benchmark gives one tenant half the rows and spreads
the remainder over 63 smaller tenants. It checks the same sorted query
at 0.01%, 0.1%, 1%, 10%, and 50% selectivity and LIMIT 10, 100, and
10,000. The sequential, tenant-equality, and composite-index cases use
one data set. The benchmark records the unforced plan before measuring
each controlled path, so a cost-based sequential choice remains visible.
The diagnostic pass counts candidate TIDs and distinct heap blocks in
the index stream. The sequential case visits all heap blocks. The
warm-cache allocation and time samples follow the diagnostic pass
and run with diagnostic counting disabled.

```sh
python3 src/lisp/t/btree-range-memory.py
python3 src/lisp/t/btree-range-memory.py --writer-only
```

All 45 comparisons passed with 1,000,000 seeded rows. The planner
naturally selected the ordered composite index in all 15 composite
cases and preferred the sequential path to the tenant-only index.
The forced equality measurements therefore describe the old access
path as a control.

| Selectivity and limit | Path | Candidate TIDs | Heap blocks | Rows removed | Allocation MB | Time ms |
|---|---|---:|---:|---:|---:|---:|
| 0.01%, 10 | Sequential | — | 4,425 | 999,900 | 202.58 | 625 |
| 0.01%, 10 | Tenant equality | 500,000 | 4,425 | 499,900 | 358.72 | 1,010 |
| 0.01%, 10 | Composite | 10 | 1 | 0 | 0.16 | 0.8 |
| 0.01%, 10,000 | Composite | 100 | 2 | 0 | 0.23 | 1.4 |
| 50%, 10,000 | Sequential | — | 4,425 | 500,000 | 288.46 | 963 |
| 50%, 10,000 | Tenant equality | 500,000 | 4,425 | 0 | 444.58 | 1,548 |
| 50%, 10,000 | Composite | 10,000 | 89 | 0 | 6.17 | 38 |

The equality allocation check used three warm samples of 64,000
single-column point lookups over 16,384 rows. The pre-roadmap
baseline at `280ecd87618` allocated 4,325,832,832 bytes at the
median. The roadmap worktree allocated 4,340,205,632 bytes before
the planner skipped redundant range comparisons and 4,329,950,688
bytes afterward. The change saves about 160 bytes per lookup and
leaves the measured worktree allocation 0.10% above the baseline.
Range terms and composite equality prefixes still produce range
candidates.

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

In a warmed SBCL REPL, 10,000 parses allocated 20.9 MB for a two-column
definition and 34.0 MB for a two-key definition with a qualified function
and predicate. Neither workload increased live Lisp data after full GC.

The definition analyzer resolves result types and typmods, requires a
Boolean predicate, and collects key and predicate column dependencies.
The initial volatility checks reuse the scalar builtin registry.
PostgreSQL C and the analyzer agree on the mixed timestamp-to-date
definition and six invalid definitions, including a session-dependent
timestamptz-to-date cast. After keeping the per-expression visitor
stack-local, three warmed runs of 10,000 analyses each allocated 16.9 MB
with no increase in post-GC live Lisp data.

The catalog codec persists ordered expression keys, the predicate and
referenced attribute numbers. Ordinary definitions retain the legacy
encoding. The shared row evaluator admits only SQL TRUE and evaluates
keys in definition order. In 10,000 operations, plain and expression
catalog round trips allocated 5.4 MB and 22.2 MB respectively. Plain and
expression row evaluations allocated 0.29 MB and 0.75 MB respectively.

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

On a 1,200-row table, 100 composite expression lookups allocated 6.1 MB
and took 38 ms with the index, versus 34.1 MB and 121 ms after dropping it.
Planning the same statements allocated 2.6 MB and 2.2 MB respectively.

Typed parameters can supply probe values for composite expression
indexes once independent query conjuncts prove the partial predicate.
The wire test holds a scan open, verifies DROP INDEX waits, then
replaces the index with a different predicate and rebinds the same
prepared statement. Over 100 prepared lookups, the indexed path
allocated 5.6 MB and took 39 ms. Dropping the index raised this to
34.8 MB and 122 ms.

On a 1,200-row table with 1% predicate selectivity, 100 descending
LIMIT queries allocated 5.4 MB and took 38 ms through the partial
index. After dropping it, they allocated 19.3 MB and took 92 ms.
The wire suite also checks that an excluded predicate falls back and
returns the non-indexed rows.

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

Across three 2,000-row builds at 10% selectivity, ordinary and concurrent
creation allocated a median of 6.24 MB and 6.21 MB and took 27 ms and
39 ms, respectively. Peak RSS was about 109 MB for both and post-GC
Lisp heap was about 48 MB. Moving the standalone build onto the shared
catalog publication path reduced ordinary build allocation from 11.0 MB
to 6.24 MB. The concurrent index still enforced uniqueness after
immediate restart.

With a live native subscriber, three 200-row builds at 10% selectivity
allocated a median of 1.38 MB for ordinary creation and 1.89 MB for
concurrent creation on the source. The subscriber allocated 1.49 MB and
1.52 MB respectively. Source and subscriber peak RSS was about 105 MB
in both cases. Median time to observe the valid index on the subscriber
was 124 ms and 160 ms, including polling.

The 2,000-row logical benchmark initially waited over 90 seconds for
bootstrap. The native client now sets `TCP_NODELAY` as libpq does.
Bootstrap completed in 1.96 seconds and allocated 5.86 MB on the
source and 192.93 MB on the subscriber. Sampled peak RSS was 105 MB
and 295 MB, with about 48 MB of live Lisp heap on each server after GC.
In three runs after bootstrap, ordinary and concurrent index creation
allocated 7.28 MB and 7.57 MB on the source and 7.43 MB and 7.66 MB on
the subscriber. The logical wire fixture now copies all 2,000 rows
before exercising ordinary and concurrent index DDL.

The expression-index benchmark seeds 10,000 rows, verifies 20 warm
point lookups, then inserts 1,000 more rows. It uses the same
`abs(key)` expression and lookup SQL across sequential, full,
and partial builds. Build allocation and time include the ordinary
catalog and WAL publication. Insert WAL numbers are the change
in durable WAL position.

```sh
python3 src/lisp/t/expression-index-memory.py
```

| Index | Index KiB | Build ms | Build allocation MB | Insert WAL KiB | Insert allocation MB |
|---|---:|---:|---:|---:|---:|
| Sequential baseline | 0 | — | — | 56.6 | 16.1 |
| Full expression | 408 | 362 | 224.6 | 120.8 | 46.5 |
| Partial, 1% | 16 | 38 | 6.3 | 72.6 | 16.5 |
| Partial, 10% | 48 | 64 | 21.6 | 88.7 | 19.9 |
| Partial, 90% | 368 | 331 | 200.2 | 120.8 | 43.8 |
| Partial, 100% control | 408 | 363 | 224.7 | 120.8 | 46.5 |

The indexed lookup reported one native clean-cache hit and removed
no rows. The sequential plan reported 55 hits and removed 9,999
rows. Twenty lookups allocated 32.9 MB sequentially,
1.34 MB with the full index, and 0.95–1.34 MB with the partial
indexes. Times were 129–136 ms sequentially and 7–9 ms indexed.
The all-qualifying control holds key count and size fixed. Its build
allocated about 24 kB more than the corresponding full index build,
and its insert WAL and allocation matched the full index in this
sample. The integer predicate added little allocation at equal
coverage.

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

At 1% coverage over 1,000 rows with external 6,400-byte payloads,
TOAST fetches fell from 1,000 to 10 per build. Median ordinary build
allocation fell from 89.0 MB to 2.23 MB and time from 91 ms to
19 ms. Concurrent build allocation fell from 89.2 MB to 2.37 MB
and time from 104 ms to 32 ms. Peak RSS fell from about 202 MB to
112 MB in both modes. Each build produced a 16 KiB index. The
10,000-row inline control at full coverage still allocated about
225 MB and produced a 408 KiB index for both full and partial indexes.

```sh
python3 src/lisp/t/expression-index-memory.py --external
```

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

Three warmed 10,000-row inner joins allocated 4.7–4.9 MB and produced
10,000 matches each. Hashing reads key components directly from row
vectors, avoiding a temporary composite key per row. Both inputs must
present keys in the same resolved equality domain.

At the memory boundary the iterator partitions both inputs into eight
initial batches. An oversized batch that cannot split streams bounded
build blocks and replays its probe spool for each block. This retains
all duplicate matches under skew. An oversized build row gets its own
replay block.
One decoded row can coexist with a charged block, and a single row can
exceed the workspace target. Temporary files remain subject to the
query's temp-file quota.
Replay releases the initial hash table and sizes a new table for its
build blocks. The peak build counter covers retained rows and their
estimated table overhead. Hash spill now uses 512-byte executor buffers
for each input, writer, and on-disk match map. This replaces SBCL's
32 KiB per direction, plus its character input cache. The initial build
reserves space for eight writers. Repartitioning runs without a retained
build block and opens at most eight writers and one reader. Replay
reserves two readers and, for probe-preserving joins, a disk match map.
The reservations include an allowance for stream metadata. Tiny
internal test budgets exercise the requested row cap with a separate
fixed buffer allowance.
Sealed replay now reads records sequentially. In three 100,000-row-per-side
runs at 64 KiB, removing the redundant seeks reduced median runtime from
3.22 s to 2.15 s. Median allocation stayed near 428 MB and peak RSS near
538 MB. All runs returned 100,000 matches.
The first executor-buffer implementation took a median 5.76 s with
1 KiB buffers. Reducing these to 512 bytes preserved an existing
in-memory build case, and the ASCII fast path cut median spill time
to 4.46 s. The latter runs allocated 430 MB with peak RSS of 539 MB.
Using typed byte-buffer state reduced the median to 3.06 s. Writing
disk-only partitions without copying their input rows, and returning
the rows already owned by the decoder, reduced median allocation to
324 MB and peak RSS to 434 MB. Median runtime was 3.03 s. The native
streams reached 2.15 s with their buffers outside the workspace charge.
With skewed keys and external text, 100,000 probes against 96 build
rows returned 140,325 matches in every sample. The copy and buffer
changes reduced the 64 KiB median from 0.93 s to 0.64 s, allocation
from 142 MB to 114 MB, and peak RSS from 252 MB to 222 MB.
On a 10,000-by-10,000 integer join, the 4 MiB, 256 KiB and 64 KiB budgets
all produced 10,000 matches. Allocations were 5.0 MB, 24.6 MB and
21.1 MB. The two spill runs took 92 ms and 152 ms respectively.

SQL planning can choose a two-table INNER hash join for large sequential
inputs with bare, same-type equality keys. Selective indexed joins keep
their nested-loop path. Both simple execution and one-row portal pulls
use the same iterator. EXPLAIN reports Hash Join and estimates build work.

Ten 1,200-row-per-side SQL joins allocated 11.5 MB and took 41 ms in
memory, and allocated 28.3 MB and took 155 ms at a 64 KiB budget. Writing
one equality operand as `l.a + 0` kept the equivalent query on the
nested-loop path, where ten runs allocated 2.09 GB and took 7.93 s.

SQL work_mem now supplies executor workspace budgets and supports SET,
SET LOCAL, RESET, and pg_settings reporting. Unadorned values use
kilobytes. The wire tests verify session isolation and transaction restore.
At SQL settings of 4 MiB and 64 KiB, ten joins allocated 11.5 MB and
28.3 MB respectively. Peak temporary space was zero and 183,660 bytes.

With a 100,000-byte TOAST payload at a 64 KiB workspace target, the
join returns the same count and payload length as PostgreSQL C. Ten
runs allocated 57.4 MB after reusing owned replay rows, down from
58.5 MB. Peak temporary space was 293,253 bytes.

LEFT hash joins retain unmatched and NULL-key probe rows. A spilled
partition tracks probe matches by spool ordinal and emits each unmatched
row after the final build block. Match tracking can spill independently.
The fit and spill wire workloads both matched PostgreSQL C, including
an ON residual and a post-join WHERE filter. Ten runs allocated 15.3 MB
at 4 MiB and 37.0 MB at 64 KiB, with peak temporary space of zero and
195,744 bytes respectively.

RIGHT and FULL hash joins mark build rows only after the key and ON
checks pass. An unmatched build row is emitted after its block has
scanned the full probe partition. NULL keys and disjoint partitions
receive the same null-extension and source-presence treatment.
PostgreSQL C agreed on the RIGHT and FULL wire results in memory and
during spill. Ten FULL joins allocated 21.0 MB at 4 MiB and 42.9 MB at
64 KiB, with peak temporary space of zero and 196,004 bytes respectively.

The wire suite checks early close of a suspended spill portal, DDL
admission after close, quota failure cleanup, and startup cleanup after
an immediate stop with live spill files. The iterator now unlinks each
finished partition pair before opening the next one. At 64 KiB, a
1,200-row-per-side workload used 193,260 bytes of peak temporary space.
After the first one-row portal pull, 170,220 bytes remained on disk,
and closing the portal removed them. Ten runs allocated 29.9 MB.

The wire suite also sends CancelRequest after observing spill files,
checks SQLSTATE 57014 and verifies that rollback leaves the session
usable. Disconnecting a suspended portal releases its spill files.
A suspended join continues to return its captured rows after another
session updates and deletes build-side tuples. The initial hash-table
flush checks for interrupts between rows. Repeating the ten-run spill
workload preserved the 29.9 MB allocation and 193,260-byte temp peak.

EXPLAIN ANALYZE JSON reports hash candidates examined before key
rechecks, populated spill batches, nonempty build blocks, and the
largest charged build block. Build blocks count the in-memory build
or the replay blocks used after spill. Candidates, batches, and blocks
sum across executions of the node. Peak build bytes can exceed
work_mem when replay processes an oversized tuple on its own.
The wire test checks that the in-memory and spilled runs examine the
same candidates and that the ordinary spilled fixture stays within
its 64 KiB build budget.

With 10,000 probe rows, 512 build rows and 64 KiB of work_mem,
uniform keys used 8 batches and 8 blocks (65,454 peak build bytes,
765,644 temp bytes, 8.2 MB allocated, 112 MB peak RSS, 52 ms).
Skewed keys used 8 batches and 8 blocks (65,518 peak build bytes,
760,216 temp bytes, 13.5 MB allocated, 118 MB RSS, 72 ms). One key
used one batch and three blocks (65,455 peak build bytes, 746,352
temp bytes, 425.3 MB allocated, 530 MB RSS, 1.48 s). Candidate and
result counts agreed between the 64 KiB and 4 MiB runs at 512,
67,698 and 5,120,000 respectively. The single-key case also
allocated 411.2 MB without spill. SBCL allocation profiling attributed
99.6% of samples to vector concatenation in hash join row assembly.
Reusing the last immutable source-presence vector reduced single-key
allocation to 247.3 MB in memory and 261.4 MB during spill. Peak RSS
fell from 516 MB to 355 MB in memory and from 530 MB to 369 MB
during spill. The candidate count, peak charged build bytes and
temporary bytes agreed with the baseline. The join still allocates
an independently owned value vector for each emitted row.

At 100,000 probe rows and 512 build rows, uniform keys returned 512
matches and skewed keys returned 675,901 at both budgets. At 64 KiB
the respective spill peaks were 7.43 MB and 7.27 MB, allocation was
74.5 MB and 107.0 MB, and peak RSS was 183 MB and 216 MB. At 4 MiB
neither spilled and allocation was 13.2 MB and 45.7 MB. The uniform
case shows the cost of writing probe rows whose keys have no build
match when their partition contains at least one build row.

Spilling joins with a non-preserved probe side use a fixed 8,192-bit
build-hash filter during partitioning. Equal join keys must share a
hash, so a clear bit rules out a match. A set bit sends the row
through the existing batch and equality checks. The initial build
reserves space for the filter, and replay restores the full build
budget. Small budgets and joins that emit unmatched probe rows
skip the filter. The wire regression checks matched rows and
temporary writes for both INNER and LEFT joins.
In the same 100,000-probe-row benchmark at 64 KiB, the filter cut
uniform temp use from 7.43 MB to 0.53 MB, allocation from 74.5 MB
to 17.8 MB, peak RSS from 183 MB to 129 MB and time from 506 ms to
69 ms. Skewed temp use fell from 7.27 MB to 5.36 MB, allocation
from 107.0 MB to 91.0 MB, peak RSS from 216 MB to 203 MB and time
from 686 ms to 564 ms. Both distributions retained their candidate
and result counts.

Run the hash-join memory workload from the checkout root. It starts
a disposable pg-lisp instance, verifies query results against counts
computed independently of the executor, and prints JSON lines with
the plan counters, spill reads and writes, peak temp bytes, elapsed
time, allocations, and RSS. The external case checks that the payload
was stored out of line. The comparison option selects the loop
paths in the disposable instance so it can use the same SQL and
memory budgets; index creation is outside the measured window.

```sh
python3 src/lisp/t/hash-join-memory.py --probe-rows 1000000
python3 src/lisp/t/hash-join-memory.py --probe-rows 1000000 --build-rows 1000000 --work-mem 4MB
python3 src/lisp/t/hash-join-memory.py --distribution skew --payload external
python3 src/lisp/t/hash-join-memory.py --build-rows 256 --compare-loops
```

The one-million-row narrow uniform run used 512 build rows and
returned 512 matches at both budgets. At 4 MiB it took 384 ms and
allocated 202.8 MB, with 319 MB peak RSS and no spill. At 64 KiB it
took 667 ms and allocated 241.6 MB, with 356 MB peak RSS, 4.75 MB
of temporary data, eight batches and blocks, and a 64,394-byte
peak build block.

With 100,000 uniform rows on each side, the fixed-partition
executor returned 100,000 matches at both budgets and wrote
14.78 MB of temp. At 4 MiB it used eight build blocks and
allocated 177 MB in 1.07 s. At 64 KiB it used 408 blocks and
allocated 2.09 GB in 14.65 s, with 980 MB peak RSS. This isolates
the cost of repeatedly reading probe partitions for large
splittable build batches.
The balanced one-million-row baseline at 4 MiB returned a million
matches, with eight spill batches and 64 build blocks. It peaked at
4,194,303 charged build bytes, 149.8 MB of temp and 992 MB RSS,
and allocated 4.42 GB in 29.23 s.

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

At 100,000 rows per side and 64 KiB, repartitioning kept all
100,000 matches. Allocation fell from 2.09 GB to 428 MB, peak RSS
from 980 MB to 539 MB, and elapsed time from 14.65 s to 2.90 s.
Peak temp rose from 14.78 MB to 15.70 MB while a parent and its
children coexisted. Spill batch counts now include populated parent
and child partitions, while build blocks count leaf replay work.
In this run, the 584 partitions produced 512 blocks. The temporary
I/O counters recorded 44.33 MB of spill reads and writes each.
The balanced million-row run still returned one million matches.
At 4 MiB it allocated 2.95 GB in 19.70 s, versus 4.42 GB in
29.23 s before repartitioning. At 64 KiB it completed in 37.89 s
and allocated 5.48 GB, with 4,096 build blocks and a 65,399-byte
peak build block. Peak temp was 159.1 MB at both budgets. Spill
reads and writes were 299.6 MB each at 4 MiB and 599.1 MB each
at 64 KiB. Peak RSS was about 991 MB, with about 53 MB live
after forced collection.

With the serialization buffers included in the 64 KiB workspace,
the million-row run returned all one million matches in 43.06 s.
Sizing repartition fanout reduced build blocks from 32,768 to 8,192
and allocation from 5.56 GB to 5.12 GB compared with the fixed
eight-way split at the same budget. Peak charged build bytes were
59,094, with 986 MB peak RSS and 53 MB live after collection.
Peak temp was 159.1 MB, with 748.9 MB of spill reads and writes each.

The compact spill format stores a signed 32-bit key and one source identity
in nine bytes. Other rows use the general codec. At 64 KiB the million-row
join returned one million matches in a median 4.40 s across three runs,
with 2.68 GB allocated and 955 MiB peak RSS. Spill I/O fell to 90 MB in each
direction, and peak temp fell to 19.1 MB. In isolation, one million-row
`count(*)` heap scan took a median 0.41 s and allocated 219 MB.
Decoding single-column integer tuples directly into SQL rows cut the
scan to 0.26 s and 123 MB. Computing the page checksum without copying
the page cut scan allocation further to 86 MB, with a 0.25 s median
and 190 MiB peak RSS. At the same settings the full join still took
4.11 s and allocated 2.41 GB, with 934 MiB peak RSS.
Reading non-NULL keys into reusable 256-element integer batches took
0.088 s per million-row input and allocated 37 MB in an isolated
scan. The scan verified the key count and sum in four samples under a
registered snapshot.

The executor fuses ungrouped `count(*)` with a spilling inner equality
join of two single-column `int4` heaps. It skips NULL keys, partitions
four-byte keys, and counts probe matches using a 48 KiB build frequency
table. Reusing a caller-owned block on clean cache misses keeps each heap
scan from allocating a page per read.

On the same one-million-row-per-side query, five warmed samples at
`work_mem = '64kB'` verified one million matches each. PostgreSQL C
20devel ran serially with JIT disabled and its default
`hash_mem_multiplier = 2`. The median elapsed time and largest sampled
RSS were:

| Engine | Median elapsed | Peak RSS | Spill read | Spill written |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL C | 0.198 s | 96.2 MiB, query backend | 47.7 MB | 47.7 MB |
| pg-lisp | 0.273 s | 110.7 MiB, whole server | 16.0 MB | 16.0 MB |

The Lisp run allocated 12.2 MB per warmed query, used 8.1 MB peak
temporary space, and probed one million keys across 512 build blocks.
To reproduce the Lisp case, run `python3 src/lisp/t/hash-join-memory.py
--probe-rows 1000000 --build-rows 1000000 --work-mem 64kB --samples 5`.

### Performance holdouts

`python3 src/lisp/t/hash-join-holdout.py` verifies every result and reports
CPU time, cumulative allocations, and sampled whole-server RSS. It runs
each fixture in a fresh server, warms the query once, and collects three
samples. CPU time and allocations below are the sample medians divided by
the number of executions. Peak RSS is the largest sampled process RSS
across the three samples. Each scan sample contains seven executions and
each join sample contains three.

The controlled comparison runs the same roadmap checkout with the typed
scan and count path disabled, the copy-based page checksums restored,
and hash spill records restored to text serialization with a quota
writer per record. This keeps spilling hash joins available on both
sides of the comparison. The pre-roadmap commit `280ecd87618` is a
separate reference and runs nested loops for these joins.
Run the controlled baseline with
`--control-prelude src/lisp/t/hash-join-holdout-ablation.lisp`.
Pass `typed_skew typed_sparse` to select the two large typed holdouts
outside the six default cases.

| Holdout | Allocation, MB | Peak RSS, MiB | CPU, ms |
| --- | ---: | ---: | ---: |
| 100k nullable int4 scan | 20.97 → 11.37 | 244.4 → 179.9 | 45.7 → 31.4 |
| 40k mixed int/text filtered scan | 8.75 → 8.75 | 160.3 → 160.2 | 28.6 → 27.1 |
| int4 join with sum of projected keys | 2.20 → 1.45 | 106.0 → 103.4 | 16.7 → 3.3 |
| two-column int4 join with a residual | 1.94 → 1.80 | 105.1 → 104.5 | 13.3 → 13.3 |
| text join with length aggregation | 2.49 → 2.35 | 106.7 → 105.9 | 13.3 → 16.7 |
| LEFT JOIN with 6,400-byte external text | 23.85 → 23.79 | 172.0 → 171.8 | 43.3 → 40.0 |
| 5,200 by 4,400 nullable, skewed int4 count | 15.80 → 0.70 | 145.3 → 101.6 | 113.3 → 3.3 |
| 14k by 12k nullable, sparse signed int4 count | 38.00 → 0.72 | 209.6 → 102.6 | 336.7 → 6.7 |

Against the committed baseline, the text join dropped from 207 MB and
383 ms CPU per query to 2.35 MB and about 15 ms. The wide LEFT JOIN
dropped from 8.40 GB and 2.70 s CPU per query to 23.79 MB and 43 ms.
These larger differences include the change from nested loops to hash
joins. Pass `--engine-root /path/to/baseline/worktree` to rerun that
comparison. The large typed holdouts use the controlled comparison.

CPU samples for the smaller joins were coarse because Linux exposes
process CPU in ticks. A separate 30-execution comparison isolated a
planner change that skips a hash join's ON evaluation when the hash keys
cover every conjunct. It reduced median elapsed time from 4.15 to 3.50 ms
for the int4 projection and from 14.45 to 13.75 ms for the text join,
with allocations and peak RSS within one percent. Residual conditions
keep their ordinary evaluation path. The TOAST-heavy join stayed around
43 ms. A 30-execution rerun against the full controlled baseline also
put the text join at 13.7 ms CPU versus 14.3 ms. The residual join
stayed within one CPU tick per batch, and the TOAST-heavy join fell
from 44.7 to 43.0 ms. The exact million-row workload retained both
2× stretch thresholds on rerun.

### Fixed-width join keys

The heap layout registry has ten scalar types in the 4-, 8-, and
16-byte scope. The current key contracts are:

| Width | Type | Equality key requirements |
| --- | --- | --- |
| 4 | `int4`, `date` | Signed scalar. Generic hash equality is available. |
| 4 | `oid` | Unsigned catalog scalar. SQL table DDL rejects `oid`, so it has no user-table benchmark. |
| 4 | `float4` | Canonicalize signed zero and NaNs before hashing. |
| 8 | `int8`, `time`, `timestamp`, `timestamptz` | Signed scalar. Generic hash equality is available. |
| 8 | `float8` | Canonicalize signed zero and NaNs before hashing. |
| 16 | `interval` | Canonicalize the duration used for SQL comparison, including month/day equivalents. |

The heap stores `timetz` in 12 bytes, and UUID has no registered heap
layout. Both fall outside this inventory. The fused count path handles
the signed integer and temporal domains, including interval, plus both
float widths. Float keys normalize signed zero and NaN representations
before partitioning. Integer, date, time, and both timestamp types retain
their exact heap bits.

The generic spill codec now stores wide signed integer keys with a
single source identity in 13 bytes. The existing 9-byte frame continues
to cover values in signed int32 range. With 100,000 distinct `int8`
keys per side starting at 2^40 and 64 KiB of work memory, median time
fell from 2.862 s to 0.315 s, allocation from 279 MB to 171 MB, peak RSS
from 371 MiB to 268 MiB, and spill bytes from 49.8 MB to 7.8 MB in each
direction. The comparable PostgreSQL C result was 0.020 s. The generic
path still materializes rows, so this codec change alone falls short
of the runtime and RSS targets.

The fused eight-byte path reads each key as two signed 32-bit words,
preserving the full range without boxing a SQL value. It spills eight
bytes per key and uses the same partition lifecycle as the four-byte
path. Its frequency table has 4,096 slots at 10 bytes per slot. Mixed
leaves split before they reach 2,731 build rows, so a 16-bit frequency
is sufficient. Single-key leaves multiply their full input counts.

Interval comparison uses whole days and a nonnegative subday remainder.
This represents the exact PostgreSQL comparison span even when adding
the month, day, and time fields in microseconds would exceed signed
64 bits. The private spill record uses ten bytes. The 48 KiB frequency
table, up to sixteen 672-byte split writers, and a reusable 4 KiB reader
fit within the 64 KiB workspace target for the table and buffers.
Temporary file metadata is additional. If a split sends every build
key to the same child, its next split starts at the highest bit that
varies across the build keys. This bounds redundant rewrites for skewed
keys that share the mixed 64-bit hash.

Run `python3 src/lisp/t/hash-join-fixed-width.py --reference-bin /path/to/pg/bin`
to reproduce the one-million-row-per-side comparisons. Pass type names
to select cases. The harness warms each query, checks the input and
result counts, and emits timings, CPU, memory, and the hash plan as JSON.
The runs use 64 KiB work memory and serial execution. C uses its default
hash memory multiplier and JIT is disabled. The reference build was
PostgreSQL C 20devel. Times are medians of three
Lisp and five C executions. RSS is the largest whole-server sample for
Lisp and the largest query-backend sample for C.

| Key | Lisp s | C s | Time ratio | Lisp MiB | C MiB | RSS ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `int4` | 0.271 | 0.292 | 0.93× | 117.4 | 93.1 | 1.26× |
| `int8` starting at 2^62 | 0.338 | 0.202 | 1.67× | 111.8 | 93.2 | 1.20× |
| `float4` | 0.286 | 0.205 | 1.40× | 117.0 | 93.3 | 1.25× |
| `float8` | 0.380 | 0.207 | 1.84× | 118.0 | 93.5 | 1.26× |
| `date` | 0.268 | 0.194 | 1.38× | 110.7 | 93.4 | 1.19× |
| `time` | 0.343 | 0.204 | 1.68× | 118.0 | 93.4 | 1.26× |
| `timestamp` | 0.355 | 0.205 | 1.73× | 118.1 | 93.2 | 1.27× |
| `timestamptz` | 0.356 | 0.205 | 1.74× | 118.1 | 93.1 | 1.27× |
| `interval` | 0.420 | 0.221 | 1.90× | 112.2 | 109.1 | 1.03× |

The float fixtures vary the significand across both signs. The time
fixture spans almost 24 hours, and both timestamp fixtures start in
year 280000, beyond SBCL's signed fixnum range in epoch microseconds.
The interval fixture uses two billion base months and represents each
additional 32 days as one month plus two days on one side. All million
keys match despite the different stored fields. Packing the key and
reusing the reader reduced allocations from 24.2 MB to 13.6 MB and
spill traffic from 48 MB to 40 MB in each direction.

The holdout comparison used `a06eadacd44` as the baseline. Run
`python3 src/lisp/t/hash-join-holdout.py --repeats 20 int4_projection int4_residual text_join wide_left typed_skew typed_sparse int8_rows int8_residual`
and add `--engine-root` for a baseline checkout. Allocation and CPU are
medians per query across three batches. RSS is the highest sample over
the three batches and includes the whole Lisp server.

| Holdout | Allocation MB, before → after | Peak RSS MiB, before → after | CPU ms, before → after |
| --- | ---: | ---: | ---: |
| `int4_projection` | 1.43 → 1.43 | 126.45 → 126.48 | 3.5 → 3.5 |
| `int4_residual` | 1.77 → 1.77 | 133.04 → 133.27 | 14.5 → 14.0 |
| `text_join` | 2.33 → 2.33 | 143.79 → 143.84 | 14.0 → 14.0 |
| `wide_left` | 22.63 → 22.63 | 553.38 → 553.39 | 43.5 → 44.5 |
| `typed_skew` | 0.69 → 0.69 | 112.39 → 112.59 | 3.0 → 3.0 |
| `typed_sparse` | 0.70 → 0.70 | 113.63 → 113.83 | 4.5 → 4.5 |
| `int8_rows` | 3.13 → 2.74 | 159.10 → 151.52 | 17.5 → 7.0 |
| `int8_residual` | 1.98 → 1.98 | 137.13 → 137.36 | 16.0 → 16.0 |

The spill codec improvement carries over to row-producing joins on
single-column wide integer tables. The residual fixture spills wider
rows and continues to use the general frame. The other holdouts show
small variations and keep their prior execution paths.

### Random varchar primary-key reads

Run `python3 src/lisp/t/varchar-point-read.py --reference-bin /path/to/pg/bin`
for the idempotency-key workload. The fixture inserts one million unique
URL-safe ASCII keys, 60 to 127 characters long, into a `varchar(127)`
primary key. Each warmed batch checks 20,000 seeded random lookups over
one persistent connection. Hits fetch and verify an integer payload.
Misses use keys generated outside the inserted row domain.

The first allocation profile showed a heap scan for the bound equality.
The planner now uses the existing runtime probe for a compatible
single-column index. Bind values alone cannot prove a partial-index
predicate. In the next profile, decoding internal separator entries
accounted for 32% of sampled allocation. Plain separators are now
compared in place. The equality cursor still includes the child before
the first equal separator, where duplicates can survive a split.

| Prepared batch | Hit median s | Miss median s | Hit peak RSS MiB | Hit allocation per lookup |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL C 20devel | 2.946 | 2.701 | 153.6 | unavailable |
| Lisp with bind equality probe | 7.758 | 7.244 | 936.9 | 95.1 kB |
| Lisp with in-place separators | 7.176 | 6.972 | 940.1 | 63.1 kB |
| Lisp with one-plan Bind and startup GC cap | 7.662 | 7.082 | 253.8 | 51.0 kB |
| Lisp with binary equality searches | 6.354 | 5.829 | 254.1 | 51.1 kB |

RSS is the C query backend or the whole Lisp server. The startup GC
cap bounds SBCL's allocation interval to 128 MiB and preserves a lower
runtime-selected value. The uncapped interval on the measured build was
about 819 MiB. The one-plan Bind validates the Parse signature against
the execution plan and admits relation readers before capturing the
read-committed snapshot. This preserves the Bind-time view when a
concurrent TRUNCATE holds the relation. With planning tracking enabled,
a completed plan can be counted even if Bind subsequently rejects
the result descriptor or format. No execution is counted in that case.

The final equality cursor binary-searches ordered leaf keys and
internal separators. It includes the preceding child at an equal
internal separator and scans the equal run. A probe of an unused slot
falls back to linear traversal. Each million-row timing used five
samples after warming, with every result checked. The harness confirms
that the prepared query opens an equality index cursor before timing.

Using the faster C baseline above, Lisp hits were 2.16× C elapsed and
1.65× C peak RSS. Misses were 2.16× elapsed and 1.64× peak RSS. Median
server CPU was 3.25 s for Lisp hits and 2.91 s for misses, versus
0.50 s and 0.39 s for C. Lisp allocated 40.3 kB per miss. A second fresh
C run on the shared host measured 4.166 s for hits and 3.910 s for
misses with similar peak RSS. The faster denominator gives the
conservative target comparison.

For holdouts, use `--rows 50000 --reads 3000 --samples 3` with the
options below. Add `--engine lisp --engine-root /path/to/baseline` to
compare an earlier checkout. The unprepared baseline was `a06eadacd44`.
Each table cell reports median elapsed seconds, median server CPU seconds,
and peak RSS MiB. Allocation is Lisp kilobytes per lookup.

| Holdout | Lookups | C elapsed / CPU / RSS | Lisp elapsed / CPU / RSS | Lisp kB |
| --- | --- | ---: | ---: | ---: |
| `--no-prepared` | hits | 0.591 / 0.15 / 40.1 | 1.156 / 0.57 / 229.0 | 43.3 |
| `--no-prepared` | misses | 0.595 / 0.15 / 40.1 | 1.123 / 0.52 / 201.4 | 33.8 |
| `--min-length 60 --max-length 60` | hits | 0.507 / 0.09 / 36.0 | 1.086 / 0.54 / 211.7 | 37.3 |
| `--min-length 60 --max-length 60` | misses | 0.471 / 0.09 / 36.0 | 1.013 / 0.47 / 185.0 | 28.0 |
| `--min-length 127 --max-length 127 --key-type text` | hits | 0.498 / 0.07 / 43.6 | 1.080 / 0.56 / 232.8 | 44.6 |
| `--min-length 127 --max-length 127 --key-type text` | misses | 0.478 / 0.07 / 43.6 | 0.955 / 0.48 / 203.9 | 34.6 |

Unprepared hits went from 66.0 to 43.3 kB per lookup, 294.5 to
229.0 MiB peak RSS, and 0.61 to 0.57 s CPU per batch. The elapsed median
rose from 1.045 to 1.156 s on this shared host. The smaller fixtures
produce a much smaller C backend RSS, so their whole-server RSS ratios
are higher than the primary comparison.

### Other join paths

Re-run the loop cases with
`python3 src/lisp/t/hash-join-holdout.py nested_cross nested_full lateral_index parameterized_index`.
The reference checkout was `976722fac52`. Allocation and CPU are medians
per execution, and RSS is the largest whole-server sample across three
batches at 64 KiB.

| Holdout | Allocation, MB | Peak RSS, MiB | CPU, ms |
| --- | ---: | ---: | ---: |
| Cross join with 48k projected pairs | 23.01 → 21.48 | 213.3 → 201.6 | 84 → 84 |
| Full nested loop with inequality | 6.94 → 6.47 | 132.3 → 130.0 | 22 → 24 |
| Indexed LATERAL with 800 outer rows | 411.27 → 5.95 | 921.1 → 139.6 | 1,430 → 22.9 |
| Parameterized index join with 800 probes | 30.01 → 4.52 | 300.4 → 130.1 | 28.6 → 20.0 |

With 20 executions per sample, cross-join CPU moved from 87.5 to
89.5 ms, and inequality full-join CPU moved from 24.5 to 24.0 ms.
Their allocation reductions persisted.

The correlated subquery uses the bound outer column as a runtime index
equality. Forward full-key equality ranges use the point cursor, and
point cursors retain only matching TIDs from each leaf. Heap lookup
deforms tuple bytes directly from the borrowed page. Nested loops and
LATERAL reuse their last immutable source-presence vector across pairs.
The PostgreSQL C differential also checks both predicate orientations
for a correlated indexed probe with NULL outer keys and an inner filter.

The row-producing hash join holdouts covered integer projection, an
integer join with a residual, text equality, and LEFT, RIGHT and FULL
joins. In separate 20-execution samples, the narrow cases stayed within
1 MiB peak RSS and 1 ms CPU per execution of the baseline. The
external-text LEFT JOIN allocated 22.63 MB per execution versus 23.75 MB
and peaked at 553 MiB versus 572 MiB. Its median CPU was 44.0 ms versus
42.5 ms. The original million-row count still met the stretch thresholds
at 0.290 s and 111 MiB in five warmed samples.

In the same-SQL comparison with 10,000 probe rows and 256 build
rows, all three plans returned 256 rows. At 64 KiB, the hash join
processed 256 candidates in 7 ms and allocated 2.0 MB, with 37 kB
of temp and 106 MB peak RSS. The parameterized loop used 256
index probes in 9 ms and allocated 9.3 MB, with no temp and
113 MB peak RSS. The plain loop rejected 2,559,744 pairs in 6.98 s
and allocated 823.7 MB, with 739 kB of temp and 928 MB peak RSS.
At 4 MiB the three times were 4 ms, 10 ms and 6.87 s.

With 10,000 skewed probe rows and 96 build rows carrying external
6,400-byte text, ANALYZE reported a 46-byte mean physical build
tuple. Both budgets returned 14,069 matches and the expected
maximum payload length. At 4 MiB, the hash join used one build
block, took 18 ms and allocated 11.1 MB, with 117 MB peak RSS.
At 64 KiB it used eight batches and 51 build blocks, took 102 ms
and allocated 32.8 MB, with 139 MB peak RSS and 859 kB temp.
With the same 96 external build rows and 100,000 probes, skewed
keys returned 140,325 matches at both budgets. At 64 KiB they used
75 build blocks, 3.46 MB peak temp and 142 MB of allocation in
0.63 s. A single key returned 9.6 million matches at both budgets.
At 64 KiB it stayed in one spill batch with 48 build blocks,
51,810 peak build bytes, 7.62 MB peak temp and 3.29 GB allocated
in 17.50 s. At 4 MiB the same output allocated 1.41 GB in 4.25 s
without spill.

The planner chooses the build side using estimated retained bytes.
ANALYZE records the mean physical width of visible tuples during its
existing scan. The planner adds entry and row-container overhead and
uses TOAST heap pages as a proxy for external payloads. Older statistics
fall back to heap pages and estimated rows. The estimate guides
orientation. The iterator still charges each decoded row against
work_mem.

When the estimated build exceeds work_mem, EXPLAIN charges startup
for scanning and partitioning both inputs. Total cost also includes
reading the spilled data. The cost uses estimated input bytes as a
page proxy and models one write and read pass, following the initial
hash join cost model in PostgreSQL C. For an unordered
constant LIMIT over a bare integer inner equality, the planner can
choose a lazy nested loop when ANALYZE statistics predict that the
requested matches cost less than hash spill startup. ORDER BY,
post-join filtering and whole-input operators leave the existing
selection rule in place.

On a 10,000-by-10,000 uniform integer join with LIMIT 1 and 64 KiB,
the median five-execution sample allocated 0.29 MB and took 2.6 ms.
Forcing the same query onto a hash join allocated 45.1 MB and took
291 ms. The hash plan peaked at 1.57 MB of temp and 150 MB RSS.
The nested loop used no temp and 104 MB peak RSS. Each sampled
execution verified that the returned keys matched.

The iterator restores SQL column order and source presence before
evaluating residual ON conditions and emitting rows. Wire cases cover
all four join types with the left input built, including a spilled
FULL join. EXPLAIN JSON identifies the build side.

The width regression leaves dead pages in a narrow table, restarts
after ANALYZE, and verifies that the planner builds the narrow input
with both inline and external wide payloads on the probe side. Eight
INNER, LEFT, RIGHT and FULL cases matched PostgreSQL C. On ten joins
of 100 rows with 1,500-byte text against 400 integer rows at 128 KiB,
the row-count choice allocated 37.7 MB, took 65 ms and used 186,080
temporary bytes. Selecting by width allocated 8.4 MB, took 15 ms and
used no temporary space. Peak RSS fell from 144 MB to 112 MB.

For ten 200-by-1,201-row INNER joins at 64 KiB, the smaller build
allocated 4.4 MB and took 29 ms. Changing only the estimates to force
the larger build allocated 16.3 MB and took 107 ms. Peak temporary
space was zero and 119,010 bytes respectively.

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

The same harness accepts `--analyze-memory` and an optional
`--engine-root /path/to/baseline`. For batches of 15 ANALYZE calls
on 36,000 rows, allocation fell from 45.65 to 44.56 MB, median CPU
from 390 to 380 ms, and peak whole-server RSS from 148.7 to 143.6 MiB
relative to `153c1ed0f5d`.

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

Run the same harness with `--range-planning-memory` or
`--join-planning-memory` for batches of 600 EXPLAIN calls. Against
`153c1ed0f5d`, range allocation rose from 15.59 to 18.36 MB, peak
RSS from 114.2 to 116.8 MiB, and median CPU stayed at 90 ms. The
allocation profile was dominated by JSON rendering and plan
bookkeeping. Range interpolation counts full buckets as integers
and only computes a fraction for the bin containing the constant.
Typed join allocation rose from 26.09 to 26.41 MB, peak RSS from
124.3 to 124.7 MiB, and median CPU from 100 to 110 ms.

Standalone scan selection and EXPLAIN now charge filter CPU per input
row using the same function. EXPLAIN uses the fractional candidate
estimate for index recheck cost and caps residual filter output at
the index input estimate. Filter selectivity groups predicates by
column. NULL tests on a range column share the same non-NULL mass,
and predicates on separate columns assume independence. A sequential
filter can estimate a comparable literal outside the indexed key
width. The differential checks a three-conjunct mixed filter, an
out-of-range bigint bound on int4, and a displayed 0.75 cost increment
for rechecking 300 rows against one predicate.

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

For ANALYZE allocation holdouts, use `--analyze-memory --sample-kind float`
or `--analyze-memory --sample-kind numeric`. In the 15-call float
batch, allocation fell from 50.47 to 49.14 MB, CPU stayed at 390 ms,
and peak RSS rose from 149.8 to 152.0 MiB. The numeric batch went from
48.92 to 47.41 MB and 148.2 to 147.0 MiB, with CPU at 400 ms. The
current 600-call range-planning batch used 18.46 MB, 117.0 MiB peak
RSS, and 100 ms CPU.

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

Run the same harness with `--execution-memory` to reproduce the
end-to-end holdouts below. Use `--engine-root` for the baseline
`153c1ed0f5d`. Each batch checks every result, records selected plan
nodes, and reports elapsed time, CPU, allocation, and whole-server RSS.

| Holdout | Calls per batch | CPU ms, before → after | Allocation MB, before → after | Peak RSS MiB, before → after |
| --- | ---: | ---: | ---: | ---: |
| Narrow integer range | 200 | 550 → 50 | 110.01 → 10.34 | 204.27 → 109.49 |
| Integer range with residual expression | 200 | 560 → 50 | 110.39 → 10.73 | 204.52 → 109.29 |
| Numeric equality hash join | 30 | 50 → 50 | 16.21 → 16.21 | 114.46 → 114.56 |

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

Catalog object addresses include the database, relation OID and column
attribute number. Staged indexes validate their owner and key columns on
insertion and again before publication, without allocating an address per
key. The 100-cycle implicit catalog workload allocated 78.7 MB with these
checks versus 78.8 MB in the prior run.

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

Retaining those identities reuses the dependency records from staging
and allocates no history for tables without sequence references. The
20-cycle SQL benchmark stayed at 85.4 MB with capture disabled and
89.6 MB with capture enabled, with about 48 MB live after full GC.
In a separate 20-cycle workload that created and dropped a table with
a sequence default in each transaction, allocation was 0.79 MB locally
and 1.67 MB with logical capture, with about 48 MB live in both cases.

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

The native protocol can create a `pg_lisp_transactional` slot and
requires `transactional_ddl 'true'` before streaming it. The slot
retains its plugin and confirmed position across restart, using the
existing slot file format. Standard publication streams reject the
private option. The capability requires modern slot provenance.
The wire fixture confirms initial copy with the binary bytea profile,
rejects mismatched START options in both directions, and resumes the
confirmed slot after an immediate restart. In a 10,000-round-trip
PGSLOT6 codec benchmark, the legacy and transactional images allocated
95.7 MB and 90.3 MB and took 112 ms and 116 ms, respectively. The sample
slot image grew by 14 bytes.

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

Across three runs of 20 create/insert/index/update transactions, with
50 rows per transaction, ordinary publication allocated 85.4 MB and
opt-in capture allocated 89.2 MB. Peak RSS was 194 MB and 199 MB, with
about 48 MB of live heap after full GC in both cases. Median elapsed
time was 484 ms and 520 ms. The local-only path skips replay AST
construction and avoids an extra catalog lock acquisition.

For a separate 200-row subscriber workload with 20 alternating index
creates/drops and row updates, issuing DDL as standalone commands
allocated 77.6 MB on the source and 79.0 MB on the target. Combining
each pair into one transaction allocated 118.1 MB and 112.7 MB,
with median end-to-end time of 587 ms versus 1,050 ms. Post-GC live
heap was about 48 MB on both servers. Streaming the SQL frame for
large apply payloads reduced sender allocation from 171.7 MB to
25.6 MB over twelve 1 MiB payloads in a REPL sink benchmark. It
increased CPU time for those large messages, so smaller payloads
keep the inline query path.

With the commit-wide check enabled, the same mixed workload allocated
120.3 MB on the source and 113.6 MB on the subscriber and took a median
605 ms. The schema footer transmits unchanged sections once. In a
1,000-table serialization benchmark with one added index, this reduced
each footer from 294 KB to 147 KB and allocation over 30 encodes from
97.7 MB to 52.2 MB.

After enabling durable slot negotiation, the live mixed workload
allocated 120.3 MB on the publisher and 113.6 MB on the subscriber,
took 601 ms, and retained about 48 MB of Lisp heap on both after GC.

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

Wire tests cover cross-backend visibility, rollback, committed uniqueness,
and immediate restart. Crash tests cover the interval before the terminal
record and the interval after WAL flush but before primary fork creation.
On a disposable server, 100 create/insert/create-index transactions
allocated 12.1 MB when rolled back and 77.4 MB when committed and followed
by an autocommit drop. After full GC, live Lisp data changed by +124 KB and
−22 KB respectively. RSS grew by 12.2 MB and 82.5 MB before the final GC.
In a separate cold-server comparison with identical SQL operations, the
explicit transaction reduced allocation from 84.8 MB to 77.4 MB and elapsed
time from 4.30 s to 2.20 s versus committing each statement separately.

Transactional CREATE TABLE stages the empty indexes for column and table
PRIMARY KEY and UNIQUE constraints. They enforce writes in the owning
transaction and recover with the table. In 100 create/insert/drop cycles
with two constraints, the explicit transaction allocated 105.1 MB and
took 2.42 s, compared with 122.5 MB and 3.41 s for autocommit. Post-GC live
Lisp data changed by −77 KB and −4 KB respectively.

Eligible multi-statement Simple Query messages stage table and ordinary
index DDL alongside row writes and TRUNCATE. A later statement error rolls
back the block. The commit and rollback cases also pass against PostgreSQL C.
Messages containing utilities outside the implicit-block allowlist, such
as CREATE SEQUENCE, still use the existing per-statement path.
In 100 create/insert/index/drop cycles, the implicit
message allocated 78.8 MB and took 1.95 s, compared with 79.0 MB and
2.01 s for the corresponding explicit transaction. Post-GC live Lisp
data changed by −43 KB and −47 KB respectively.

Mixed logical DDL uses the transactional native slot and its schema
contract. Admission checks slots when staging a catalog command and
again under the WAL lock before commit. The commit check remembers
effective staging even when create and drop cancel out in the final
catalog. A wire regression starts a standard slot during such a
transaction and verifies that commit fails and the original table's
rows stay unchanged. Without slot interest, the same local sequence
commits on the existing row-only path. In three warmed samples of 20
such transactions, median allocation was 3.75 MB, peak RSS was
107 MB, and elapsed time was 132 ms. The workload verified all
durable row inserts and the same catalog generation after the run.
With a standard slot present, four conditional DDL no-ops can commit
alongside an INSERT. The corresponding effective commands still fail
admission. Across 20 warmed transactions, adding the four no-ops raised
allocation from 5.63 MB to 5.99 MB. Both workloads took about 99 ms,
peaked at about 109 MB RSS, and preserved the catalog generation.

## Open development tasks

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

- [ ] Qualify native CRC32C on physical ARM hardware.
  Native WAL and storage checksums now use Castagnoli CRC32C, without an IEEE
  legacy reader. Existing development data with IEEE checksums is incompatible;
  the native WAL layout version remains 8. Physical Graviton/Mac performance
  and the macOS runtime feature probe still need hardware qualification. Swap
  validation evidence is recorded in
  `/home/imbaczek/proj/pg-ha-artifacts/crc32c-swap-20261111/summary.json`.
  Earlier benchmark and emulated portability evidence:
  `/home/imbaczek/proj/pg-ha-artifacts/crc32c-comparison-20261111-111649/summary.json`
  and `/home/imbaczek/proj/pg-ha-artifacts/crc32-arm64-portability-20261111/summary.json`.

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

- [ ] Finish target reseed follow handoff and startup reconciliation.
  Persist the target's follow intent and inert configuration before source
  settlement. Reconcile the source outcome and activate follow only after the
  required durable checks; retire installation fences during startup reconciliation.

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

- [ ] Activate settled target follow and retire startup fences. The pure active
  configuration derivation and optional activation/retirement codecs are
  implemented. They preserve historical predecessors, reject inode aliases and
  advance only exact prefixes. Retirement uses the actual attempt identity order
  (device, inode, mode, uid, nlink, size, mtime, ctime). A producer-backed
  regression covers that boundary. Validation passed 53 selected tests with
  611 assertions, strict cold compilation of 226 production files, and a normal
  formatter check of all nine changed Lisp/ASDF files. Luna checkpoint and Sol
  full-diff reviews passed; the Sol refactoring review found no worthwhile
  extraction. Source documentation was reviewed separately.
  The pure activation overlay now checks the exact captured active image and
  describes the two configuration moves and all three inventory prefixes,
  preserving every historical receive/history/recovery object and unused
  image. A shared position-move helper centralizes unique-source and
  empty-destination checks. Validation passed 19 selected tests with 206
  assertions, strict cold compilation of 228 production files and formatting
  of five Lisp/ASDF files. The property passed 100 trials each at seeds 131
  and 654776489432024191, digest `fnv1a64-v1:ea8ff940de7008c7`; input
  coverage and rejection counts were not measured. Static prefix descriptions
  grant no filesystem or startup authority. Evidence: `plan-contract-check.json`,
  `plan-refactor-regression-check.json`, `plan-refactor-cold-check.json` and
  `plan-refactor-format-check.json` in the activation artifact directory.
  Read-only filesystem proofs now check the active image and exact mapped
  names, identities and bytes at all three move prefixes. Building output
  admits only absence or a complete canonical private image; partial and
  conflicting entries stay fenced. Both layout wrappers share the filesystem
  proof while keeping their separate plans and owner checks. Validation passed 10
  selected tests with 270 assertions, including all five fixture geometries,
  strict cold compilation of 229 production files and five-file formatting.
  The tests construct proposed layouts without publishing activation intent.
  Evidence: `layout-refactor-regression-check.json`,
  `layout-refactor-cold-check.json` and `layout-format-check.json` in the
  activation artifact directory.
  The activation context now binds the actual journal, install and attempt
  snapshots to held directories. It checks the observed move prefix, the
  original base and receive state, and the pending or active control at its
  prescribed location. Publication requires the exact original PREMANIFEST
  attempt. Beginning activation checks future journal capacity and publishes
  Building intent before creating active output. Repeating Building preserves
  the journal inode.
  Recovery promotes the exact next complete Building temporary and
  preserves partial or conflicting state. Installation and activation share
  predecessor acquisition while keeping their own context and admission rules.
  Fresh verification passed 30 selected tests with 641 assertions, strict cold
  compilation of 231 production files and eight-file formatting. This includes
  Building publication across all five geometries, semantic checks at all three
  actual move prefixes, attempt and snapshot changes, frozen-candidate checks,
  journal cuts and preservation of unrelated namespace entries. See
  `context-final-check.json` in the activation artifact directory.
  Local Staged publication now holds and rechecks the active file descriptor
  across file, staged-directory, workspace and ancestor syncs. A scoped witness
  binds the captured entry and statx row through publication. Complete Staged
  temporary recovery checks the proposed entry before syncing, resyncs that
  same file and rechecks the proof before promotion. Partial and conflicting
  files stay fenced. Staged describes durable local output.
  Fresh verification passed 42 selected tests with 878 assertions, strict cold
  compilation of 231 production files and five-file formatting. Coverage
  includes all five geometries, nine output/directory sync cuts, five journal
  cuts, partial and invalid output, replacement and same-byte mutation, abort,
  three injected fsync failures and the earlier regressions. The transition
  property passed 100 trials each at seeds 134 and 135 with definition digest
  `fnv1a64-v1:0d476da5846170de`. Input coverage and rejection counts were not
  measured. See `staging-final-check.json` in the activation artifact directory.
  A read-only Staged/0 source probe now reconnects through the frozen route,
  repeats writer/history/flush checks and retries the exact selected allocation.
  It compares the full canonical anchor and detached local journal, attempt,
  install, plan and active-file observations around network work and client
  close. The owner stays busy through cleanup and keeps its certainty on failure.
  Fresh verification passed 12 selected regressions with 236 assertions,
  including exact retry across all five geometries, lost replies, source changes,
  local same-byte writes and admission fencing. Strict cold compilation passed
  231 production files and three-file formatting preserved reader forms.
  A 13-action native matrix verified exact selected READY retry after a real
  CLOSED handoff and source restart, wrong generation/name rejection and
  rejection of unready, replaced, canceled and absent pins. Those pin faults
  were controlled runtime injections. See `source-probe-final-check.json`.
  Initial Activating/0 publication now consumes the fresh source observation
  in a private owner/context/thread-bound authority. It freezes the original
  snapshot, full anchor and exact candidate, then adopts one post-rename journal
  snapshot while continuing to check the same local facts. Complete initial
  intent temporaries are checked before network work and require a fresh source
  probe before promotion. Partial or conflicting temporaries stay fenced.
  Committed Activating/0 intent retries offline and preserves its journal inode.
  A shared read-only observation helper keeps the owner busy through client
  cleanup. Failed public recovery retains the earlier owner uncertainty fence.
  Verification passed 15 selected regressions with 333 assertions. The affected
  paths passed nine fresh-process reruns with 260 assertions after the helper
  extraction. Strict cold compilation passed 231 production files, and four-file
  formatting preserved reader forms. The pure transition property passed 100
  trials each at seeds 136 and 137 with digest `fnv1a64-v1:72f99318105004ca`.
  Its generator selects two fixture variants. Input coverage and rejection
  counts were not measured. A ten-action native run verified an actual SIGKILL
  after temporary fsync, source-offline recovery refusal with preserved files,
  fresh-source recovery through the selected CLOSED pin, and offline committed
  intent retry. See `intent-final-check.json` in the activation artifact directory.
  The two local control moves and Active/2 publication are complete. Each move
  holds the captured file through canonical byte and metadata checks, a
  no-replace rename, and file and parent-directory synchronization. A layout
  one move ahead of its journal is rechecked and resynced before progress is
  published. After durable Activating/0, local completion and recovery work
  offline. A private progress witness freezes the exact next transition and
  synced facts and permits one published snapshot adoption. It starts after
  the initial source authority scope ends. Complete progress temporaries stay
  open and are checked before and around resync and promotion.
  Verification passed 22 selected regressions with 992 assertions, including
  all five move geometries, every file and parent sync cut, all three progress
  journal transitions, late destination collisions, temporary substitution,
  same-byte writes, witness misuse and earlier recovery paths. Strict cold
  compilation passed 231 production files without warnings. Property seeds
  138 and 139 passed 100 trials each under `fnv1a64-v1:5528779e6984029d`.
  The generator selects two fixture variants and checks all three transitions
  per trial. Input coverage and rejection counts remain unmeasured.
  Fourteen native actions passed with three expected target SIGKILLs and
  source-offline local completion. They preserved the original quarantined
  bytes and unrelated file facts across the target, workspace and auxiliary trees.
  See `progress-final-check.json` and `progress-native-v3/acceptance-check.json`.
  The follow-up recovery audit now resyncs a one-ahead move before final
  journal stabilization, rejects an aborted attempt before syncing, and
  excludes nested moves during publication or another move. Fresh acceptance
  passed 27 selected heads with 1,046 assertions, a 231-file warning-free cold
  compile and a fresh rerun of the 14-action native crash sequence. Evidence is
  `stable-recovery-check.json`. Process crashes and injected sync failures
  leave filesystem power-loss qualification and writeback-error policy open.
  A read-only archived install reader now checks a separate container while
  retaining the PGDATA root binding and existing root-reader behavior. It
  requires a canonical terminal image and rejects a root/container alias.
  Five focused heads passed 54 assertions, including same-byte name replacement;
  strict cold compilation passed 231 files. The retirement scope must still
  prove the actual saved-directory role before acting on this observation.
  Pure retirement plans now derive the fixed install-then-attempt archive
  layouts from Active/2. Static layout descriptions remain separate from the
  recorded-progress check. For Retiring, that check permits the recorded prefix
  or one move ahead.
  Verification passed 17 selected heads with 298 assertions, 232-file cold
  compilation, and 100 property trials each at seeds 140 and 141. Input coverage
  was not collected. Actual archive observation and startup authority remain open.
  Attempt-file observation now binds the decoded record, byte digest and
  identity from one held descriptor. It checks the exact original PREMANIFEST record, mount,
  nanosecond file facts and observed prefix. Frozen root references remain exact;
  an archived reference permits only ctime to change. Verification passed 17
  selected heads with 332 assertions, including legacy real SIGKILL recovery,
  233-file cold compilation, and a separate cross-mount refusal experiment.
  The actual saved-directory role and complete namespace proof remain open.
  A borrowed-descriptor layout observer now binds the final follow snapshot,
  derives the unique archive prefix from all ten directory inventories, and
  checks saved-directory identity, frozen journals and reconstructed base/config
  images around the proof. It rejects follow temporaries and PID entries.
  Verification passed 14 unique heads with 357 assertions, 234-file cold
  compilation and live reload. Fixtures manually construct activation and archive
  layouts. Acquisition, guard ownership, active receive-store semantics and startup
  authorization remain open.
  Archive retirement and startup admission remain open. START must check
  current source state and retention floors again.

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

- [ ] Retain source WAL across remote reseed capture and follow handoff.
  Save the original request identity before creating its private bootstrap pin;
  bind the actual verified base and manifest, and support STATUS and checked
  ABORT even when the client never received a manifest. Persist cancellation
  before releasing retention so delayed requests cannot recreate a canceled
  attempt. Keep the source pin until a matching ready follow pin and durable
  target follow configuration own retention. Interrupted attempts must not
  silently recapture a different base under the same identity.

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

- [ ] Add explicit reconciliation for a partial cancellation attempt temporary.
  A valid final attempt can identify the original source, but existing recovery
  keeps a partial or malformed `physical-reseed.attempt.tmp` fenced before RPC.
  Design and review a cancellation-only recovery under the root lock, with exact
  private temporary inode checks and parent sync, followed by durable ABORTING
  before ABORT. Complete canonical forward temporaries keep existing recovery.
  A partial temporary without a valid final attempt lacks a trusted source route.
  Cover publication cuts under both initial-install cleanup and future moved-PID
  install ownership, preserving startup fences and the original request.

- [ ] Install a full reseed image while preserving the opaque old data tree.
  Introduce a separate `physical-reseed.install` plan and install owner. Select
  that owner whenever a valid install journal exists, including before the first
  move; do not grant clean-adoption authority or require clean control/WAL facts
  from the divergent target. Reverify the external stage and source READY, clone
  a separate incoming image, and verify its exact inventory before publishing
  the immutable move plan. Bind the original attempt, request, frozen source,
  root/lock identities and all workspace identities.
  Preserve the root lock and attempt fence in place. Move the other original
  top-level entries, including the original PID inode, into a private external
  quarantine on the same mount. Treat dirty contents and symlinks as opaque;
  do not parse or follow them. Use checked no-replace renames and sync both
  parents before recording each completed step. Recovery may accept the recorded
  namespace or exactly one proven planned rename ahead; both/neither locations,
  changed identity or ambiguous progress must fail closed.
  The install owner must check the original PID bytes, inode and stopped-process
  evidence at the location derived from the journal, with absence at the other
  location. An actual rename probe preserved bytes and identity fields 1–7 but
  changed field 8, ctime. After a journal-proven PID move, permit only that saved
  ctime difference; still stabilize the current complete lstat/descriptor/lstat
  identity and retain the attempt's original local facts unchanged. The ordinary
  stopped owner must keep its exact original root-PID check.
  STATUS/checked ABORT must remain usable under this owner even if the
  stage or incoming image is damaged. Advancing installation requires healthy
  image evidence; remote cleanup does not. Keep the attempt/install startup
  fences after installation or cancellation, retain the old tree, and do not
  roll back or permit writable startup automatically. Real follow-successor
  handoff and terminal startup reconciliation follow separately.
  Reuse adoption's no-replace rename, mount/sync and one-step-ahead mechanics,
  not its old-tree snapshot, clean proof, codec or ownership validator. Cover
  every move/publication cut, fresh-image reopening and cleanup after the PID
  moves. Sol xhigh design notes are in the reseed-remote evidence file above.
  Bind immutable attempt fields rather than the attempt file's inode: checked
  cancellation republishes that file. A partial initial install temporary must
  still permit cleanup-only ownership after proving the original attempt and
  root PID; it must never grant capture, preparation, moves or startup authority.
  Preserve old hardlinked names independently rather than rejecting shared
  inodes. A rename can change their shared ctime even through an unmoved alias.
  Use stable membership identities across steps and stabilize complete current
  stat observations within each read. Parent directory timestamps, size and link
  count also change during moves; check identity and the expected namespace.
  Keep the PID's stronger byte and identity rule described above.

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

- [ ] Extend former-primary rejoin beyond the verified native paths. A clean
  former primary has already passed prefix-proved offline adoption, real
  startup fence retirement, catch-up and cold restart (adoption startup evidence
  above). A divergent former primary now also passes journaled reseed after
  deliberate source SIGKILL and manual promotion (`1d19cf6a2d5`). That path
  preserves the old tree and rejoins through pinned Patroni's follow method.
  The clean-adoption interop test starts the installed former primary with
  pinned Patroni 4.1.5, switches it to Patroni-managed follow, and verifies a
  restart with the source offline and replay after reconnect. The fixture uses
  a separate Patroni config directory and explicit HBA and ident file paths.
  Pinned Patroni's policy check also detects a clean former primary past the
  fork when directory removal is enabled, despite native rewind being
  unavailable. The probe preserves the stopped target, then the test rejoins
  through journaled reseed (`e6c2000d8dd`).
  Native rewind now verifies common retained WAL and the fork against an exact
  READY source checkpoint. It reuses complete matching relation files and
  fetches changed files, then verifies the entire image before publication.
  The fixture reopens the persisted capture in a fresh process, installs the
  image, and rejoins through pinned Patroni follow and restart (`c7fc921a907`).
  It covers a clean stopped former primary and its immediate child timeline,
  with writes on both branches and local reuse of an unchanged large relation.
   Native `pg_rewind` now discovers this case over replication and completes
   installation and standby handoff. Direct tests cover config repair with
   `--finish`. Pinned Patroni 4.1.5 invokes the executable through an explicit
   `Rewind.execute` call and follows and restarts the installed standby.
   Stock eligibility and divergence detection now work on a clean stopped
   former primary when all catalog relation pages have verified native CRCs.
   The check scans those pages under the data directory lock. Patroni can
   arm its divergence check before demoting a running primary, so a
   stopped-only scan cannot qualify that path. Stock Patroni's
   `Rewind.ensure_clean_shutdown` can now recover a dirty former primary
   offline, publish a shutdown checkpoint, and preserve the common WAL for
   rewind. The native `pg_checksums --enable` command verifies a stopped
   primary and records its checksum format in durable control. The pinned
   Patroni test arms divergence checking on the running primary and schedules
   offline recovery after a crash with a leader present, then rewinds and
   rejoins. A copied recovery base drops the source's local qualification.
   Native `pg_rewind --resume` passes process crashes before capture, during
   receipt and repair of a READY image, and during journaled install, follow
   configuration, and retirement. The interrupted-receive test retains the
   original stage and file inodes across both kills. A killed repair can leave
   a private replay workspace. Stock Patroni selects `--restore-target-wal`
   from the stopped target's configuration. The live test retrieves missing
   native WAL into a private cache, rejects a truncated archive, and rejoins.
   Stock Patroni also passes its source checkpoint query with no leader hint
   and reads the disabled archive settings through `postgres -C`.
   C pg_rewind builds a changed-page map from target WAL. Native rewind
   reuses whole matching relation files.
  Coordination stays outside this backend work.

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

- [ ] Support schema-qualified INSERT targets such as `INSERT INTO public.t`;
  the current parser returns 42601 although an unqualified target parses.

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

- [ ] Match C background WAL write and flush timing and page-rounding policy;
  native eligible cycles currently sync the complete tail under WAL exclusion.

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

- [ ] Complete the broader synchronous-replication backend contract.
  Persist missing checkpoint policy, freeze, MultiXact, commit-timestamp and
  checksum facts before claiming the complete C checkpoint contract.
  Complete remaining bootstrap and rejoin work, and finish the cold-load audit
  and broader C-style documentation. No coordination is added.

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

- [ ] Complete native bootstrap and rejoin for the pinned Patroni path. Real
  `Postgresql.follow` and promotion now cover a descendant handoff, ancestry
  rejection and explicit local reseeding (`53b16b79f49`); the oracle has no DCS
  or coordination. Authenticated native export and installation now support the
  default remote Patroni bootstrap clone, with staged publication and cancellation
  cleanup. Prefix-proved offline adoption supplies recovery provenance for
   the clean former primary covered above. The native rewind executable passes
   Patroni eligibility, divergence detection and rejoin on a clean stopped
   former primary. Native `pg_waldump` handles Patroni's two-record checkpoint
   probe on that state. The offline primary recovery command supports
   Patroni's single-user invocation with empty input. The stock crash-recovery
   scheduler is exercised with a supplied leader and no DCS. Other Patroni
   WAL dump queries remain open.
   C physical tooling is incompatible with the native format.
  The complete unchanged Patroni writer oracle now passes, including `hba_file`
  and `ident_file`. These settings accept only disabled or unset values:
  `max_prepared_transactions`,
  `track_commit_timestamp`, `recovery_target`, `recovery_target_lsn`,
  `recovery_target_name`, `recovery_target_time` and `recovery_target_xid`.
  Positive prepared-transaction capacity, commit timestamp tracking and targeted
  recovery remain unimplemented. Keep the complete writer oracle unchanged.
  Ordered trust/reject HBA rules are enforced at startup, with validated reloads
  and active-policy preservation in physical backups. SQL role authorization,
  other methods and selectors, raw-byte names split at 63 UTF-8 bytes, and scoped
  IPv6 addresses remain unsupported. Numeric IPv4 includes C's legacy forms.
  `wal_level` accepts replica and logical; minimal WAL generation remains
  unsupported. Replica uses dynamic logical capture for live or arming slots,
  while logical captures even without slots. Each XID keeps its capture choice.
  `wal_log_hints` accepts on and off; supported native paths have no persistent
   hint-bit writers. Full C hint full-page-image behavior remains open.
  Source effective WAL level and activation status, including WAL/checkpoint
  propagation, remain unimplemented; logical decoding on a standby is unsupported.
  Internal checks now derive shared
  relation-object and owner counts from published registrations and waiting
  requests. Cold startup installs a finite shared object/owner pool together with
  the local resource tuple. `max_locks_per_transaction` now controls sizing and
  reporting, with default 128 and changes pending until restart.
  `max_worker_processes` installs and reports the managed-worker slot capacity;
  changes are pending until restart. The validated prepared-transaction setting
  supplies zero to the installed capacity; implementing two-phase commit remains
  open.
  C sizes a shared pool, not a per-transaction limit.
  Preserve atomic multi-relation admission, repeated
  acquisition, abort/cancellation/disconnect and virtual-reader cleanup.
  Persisted source resource requirements and standby validation now follow
  C's `XLogReportParameters` and `CheckRequiredParameterValues`.
  The shared five-field value, strict native codec, floor comparison, capacity
  arithmetic and optional positioned control metadata are implemented. Native
  utilities and independent control readers accept the extension. The explicit
  primary reporter now flushes an empty-batch parameter-change terminal before
  publishing and reading back control metadata. Primary redo repairs an interrupted
  publication from durable WAL. Logical/history readers and slot resume checks
  validate the terminal without publishing applied resource facts. Owned-server
  SIGKILL tests cover both publication boundaries. Ordinary primary startup and
  child-primary completion report installed capacity before production readiness.
  Ordinary checkpoints now carry an optional positioned tuple, and verified base
  images require exact agreement with copied control and a valid ancestor origin.
  Their manifest hashes bind the checkpoint facts without extra manifest fields.
  Exact facts now follow applied/required positions, restartpoint seeds, private
  follow scans and durable follow witnesses. Pending promotion freezes the parent
  fact, and its child checkpoint and primary control preserve it across restart.
  Legacy seeds reconstruct facts from available durable WAL before restoring
  storage when a known durable requirement needs proof. Missing legacy
  facts stay unknown; do not substitute current defaults or newer received facts
  for an older replay seed. Cold hot-standby admission now requires positive
  proof after reconstruction; OFF recovery permits unknown facts. Online
  insufficiency persists the report, pauses before later WAL, preserves readers,
  refuses promotion and shuts down on resume. Owned tests cover direct pauses,
  insufficiency discovered during promotion backlog replay, and adequate restart.

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

- [ ] After implementation, tests and code reviews are complete, document modules,
  functions and structures in the style of the original C source. Use simple,
  straightforward language; explain purpose, assumptions, invariants, ownership
  and failure behavior where they matter. A gpt-6.1-terra subagent should write
  this pass; the primary agent reviews its documentation separately.
  The child checkpoint/primary, online promotion and reader-safe row replay
  modules, source running-transaction metadata helpers, covered standby snapshots,
  virtual reader lifecycle helpers, private child adoption and the private
  promotion reader-enrollment prerequisite, native online reader-preserving
  promotion, relation reader identity selection, statement snapshot scope bookkeeping,
  temporary stream ownership, executor parking, client application-name settings,
  supported implicit query blocks, SQL promotion, fresh child backup/follow and
  durable receive-history selection, source-side follow anchors, the read-only
  extent layer, immutable inventory publication, local retained-root ownership and
  receive-control inventory validation, receive-compaction sealing and configured
  native follow, and managed-worker lifecycle and shutdown have completed this
  pass and its review. Keep the same order for the remaining HA work.

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

- [ ] Audit cold-image compilation and ASDF load-order dependencies across `pg`
  and its test systems. A warm REPL must not conceal missing declarations or
  compile-time dependencies.
  The physical-slot range validator exposed another instance: calling a timeline
  structure accessor before its `DEFSTRUCT` caused an inlining warning only in a
  cold compile. Moving the slot module after its dependencies fixed that case;
  the wider audit remains open.
  Include output-cache isolation and reuse across different initial images.
  Retirement crash tests exposed flattened `packages.fasl` paths when an ASDF
  output target was a pathname instead of a directory string. A later clean
  process could not load a reused Rove FASL referencing `QUICKLISP-CLIENT`.
  Exercise separate source directories with identical filenames and two fresh
  images sharing a cache, including images with and without Quicklisp loaded.
  A new two-image audit reproduced that failure with the current Rove dependency.
  Its `core/suite.lisp` and `reporter.lisp` select direct `ql:quickload` calls with
  `#+quicklisp`, leaving package references in the resulting FASLs. Production
  loading passed in both images, while the plain consumer failed during test
  loading. Keep cache reuse within one read-time feature context while the wider
  audit remains open.
  Ownership proof handlers now need `elog` before the port files; verify that
  dependency in fresh compilation rather than relying on a loaded macro.
    Fresh compilation of `pg` passed the strict compiler, and all 1,600
    unit heads passed in RAM. Both ASDF test systems loaded from an empty
    cache without Quicklisp, then loaded in reverse order from the same
    cache in a second fresh image without compiling. The cold test load
    still emitted style and dependency warnings. The shell runner now
    defaults an unset `TMPDIR` to its explicit run workspace. The 17
    heads that had failed while accidentally using `/tmp` passed with
    that fallback.
  Campaign test FASL paths now include the checkout's absolute directory
  components and their count under the compilation policy namespace. The
  ASDF translation regression covers two checkout roots with test paths that
  would otherwise collide after stripping their respective `t/` prefixes.
  The campaign cache also separates dependency and test FASLs by the
  Quicklisp reader feature. A fallback that initializes Quicklisp reselects
  the mapping before retrying ASDF. Four fresh images alternated between
  both feature contexts against one cache. The second Rove load in each
  context reused its FASLs without compilation. Production FASL paths are
  shared. Noncampaign ASDF cache configuration and the cold test style and
  dependency warnings remain open.
  The combined unit runner and its inventory now compile into per-process
  caches. An overlapping live portal run passed startup and reload, and
  the inventory listed 1,633 Rove heads and 166 legacy cases. A REPL
  fault check confirmed that the private cache is removed and the
  original ASDF cache binding is restored when execution unwinds.
  The current combined unit compile isolated five source-level style warnings
  to two test declarations. The staging observer's file binding and the
  continuation contract's loop index are ignorable in the source body and
  used by the macro expansions. Recompiling both files removed those warnings.
  Both test heads passed, including the 200-trial property. Dependency
  warnings still appear from the test toolchain.
  The cold physical load exposed the same macro declaration issue in six
  fixture files and one unused restartpoint parameter. Correcting those
  declarations removed the fixture warnings. All eight affected physical
  heads passed after recompilation.

- [x] Preserve ownership revocation across live reloads. The stream and directory
  proof modules retain their weak revocation tables and mutexes for the lifetime
  of the image. The regression releases each flock, observes its loss, reacquires
  the lock on the same descriptor and reloads the ASDF-built module. The new
  definitions reject the old proofs, including a directory-owner copy made
  before revocation and the original owner after its active flag is restored.
  Deploy this change with a fresh image. The previous closures' private
  registries cannot be recovered by the first live load of these definitions.

- [ ] Audit destructive sequence operations on quoted or shared lists, including
  `SORT`, `NREVERSE`, `NCONC` and the final argument to `APPEND`. Installation
  verification once changed a quoted `APPEND` tail, breaking the next clone in
  the same image. A handoff negative test also changed a quoted fixture plist
  reused by later checks. Those cases now use fresh lists; check for other cases
  and exercise repeated operations in the same Lisp image.

- [x] Audit `wal-atomic-write-bytes` callers for temporary destination aliases.
  All 20 production call sites use fixed metadata names, generated names with
  fixed suffixes, or the standby backup writer's three selected WAL and control
  paths. The writer rejects the reserved `.tmp` destination suffix before I/O.
  The regression covers an existing destination and an absent destination.

- [ ] Reproduce and report Dissect's SBCL frame-local inspection fault. While Rove
  reported a missing installed configuration in a retention fixture, Dissect's
  `FRAME-LOCALS` called `SB-DI::SUB-ACCESS-DEBUG-VAR-SLOT` on a mutex frame and
  raised a memory fault. A direct handler reported the original `SIMPLE-ERROR`
  correctly. Preserve the original condition before collecting locals; do not
  infer storage corruption from this secondary reporting failure. Observed on
  2026-10-31 in SBCL process 1984912 during the retention-reader milestone.
  Background test logs must catch errors inside `with-open-file` so abort cleanup
  keeps the log. A direct REPL reproduction without Rove later exposed an ordinary
  promotion error hidden by the same frame-local fault.

- [ ] Extend implicit simple-query transactions to the remaining statement and
  transaction-control cases, including transactional DDL and TRUNCATE, using C as
  the reference. Those messages currently retain the prior per-statement path.

- [ ] Audit server-file and command-line defaults for `application_name`; client
  startup and SQL settings are implemented.

- [ ] Implement and verify C-style pre-authentication connection pooling,
  dead-end handling and authentication timeout. The current independent session
  quotas apply after startup packet parsing and do not bound pending raw sockets.

- [ ] Fix cl-mcp character-name alias acceptance and custom-reader handling of
  unknown packages across server and worker images; add regressions for parinfer
  repairs that alter balanced but misindented forms. Keep reader evaluation disabled.

- [ ] Investigate cl-spec declaration digests changing between source loading and
  compiled loading. The replacement contract and property metadata compare equal
  except for their digests. Compiling and loading the same saved test file restores
  the earlier digests. Preserve both observations and use compiled loading for
  replay until the difference is understood. The reproduction is retained in
  `replacement-spec-compile-probe.fasl` and `replacement-specs-final.json` under
  the reseed-target-follow verification artifacts.

- [ ] Fix cold test declarations in `t/promotion-intent-tests.lisp`.
  `call-with-promotion-fixture` binds `pg::*shutdown-mode*` and
  `pg::*shutdown-requested*` lexically before their declarations, producing four
  style warnings. Production strict compilation passes; this remains part of the
  open cold-load audit.

- [ ] Reject target/temporary path aliasing in atomic file publication. The WAL
  byte helper derives a `.tmp` pathname; a target already ending in `.tmp` can
  name that same file. A fixture exposed successful return followed by deletion
  of the target. Audit callers and add a regression for rejection or a distinct
  temporary name before changing the cleanup path.

- [ ] Add a real process-kill and restart oracle at the durable slot-loss save
  and WAL unlink boundary. Current injected cuts exercise this in process, but
  do not establish crash recovery across an actual process death.

The cold-image audit should cover:

- Cross-file constants and specials referenced before their definitions or
  proclamations, especially `LET` bindings compiled before `DECLAIM SPECIAL`.
- Macros, compiler macros, symbol macros, classes and structures whose definitions
  must exist during compilation; check `EVAL-WHEN` and ASDF dependency ordering.
  The active-follow tests exposed fixture macros used before their test module
  loads. Those calls now use the fixture function directly; audit other test
  modules for the same dependency hidden by a warm image.
  Include forward calls to structure predicates: the follow-anchor work needed
  local `NOTINLINE` declarations in the earlier source-pin module. A warm image
  compiled it without exposing the missing subtype definition.
- Entry points that work only after another system or optional feature has
  populated the image, and stale FASLs or inlined constants that conceal defects.
- Each supported system built and loaded in a fresh SBCL process with an empty
  output cache. Check diagnostics as well as exit status, including warnings
  deferred until compilation-unit completion.

For each finding, preserve a small cold-image regression and fix the declaration
or dependency at its owning layer. Keep expected SBCL reload/redefinition notices
distinct from actual compiler warnings. Live reload remains the iteration model;
cold builds are an independent verification path.

The child-base work exposed another warm-image dependency: bootstrap history
inventory used `timeline-history-entry` accessors before that `DEFSTRUCT` was
compiled. Warm tests passed, but a fresh strict compile and the formatter's
fresh SLIME image failed. The ASDF order now loads timeline definitions before
bootstrap/recovery. Preserve this cold-build check when extending those modules;
the broader audit remains open.

This remains an **open audit**. It was prompted by the receive-store lock opener
using the close-on-exec constant before its defining file loaded: the live image
already knew the constant, while fresh compilation warned. The compile harness
also returned success for that deferred warning. It now rejects deferred
compilation-unit warnings; the broader dependency audit remains open.

The active HA/backend implementation roadmap is [PHYSICAL-HA.md](PHYSICAL-HA.md).

- [ ] Replace lock reacquisition in the two older startup ownership proofs.
  `call-with-physical-adoption-startup-owner` and
  `physical-reseed-follow-retirement-check-startup-owner` still call flock with
  `LOCK_EX | LOCK_NB` while checking an already borrowed lock. That can reacquire
  a released lock and conceal a gap in ownership. Give each role passive flock
  checks and permanent loss tests without changing its startup or journal
  authority. Include actual competing-descriptor acquisition after release,
  interruption identity, rearming, and the existing role-specific native tests.
  The new primary postmaster capability uses the passive checker independently;
  these older paths remain a separate safety fix.

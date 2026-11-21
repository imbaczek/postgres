# Development TODO

## SBCL Postgres architecture milestones

The North Star Architecture
defines the audit IDs and contracts. The original discussion
reviews the audit through `06cc90a`. Refresh its findings against the current
checkout before marking any item complete.

| Milestone | Implementation and exit criteria |
|---|---|
| 0. Refresh the audit and baseline verification (A17) | Inventory live callers, format writers and fixtures. Record row lifetime, snapshots, reload, commit and redo contracts. Reuse fixture setup and cleanup across the fault scenarios. Every removal needs evidence about its current callers or producers. |
| 1. Remove unused paths and share metadata mechanics (A5, A6, A14, B15, B16) | Remove obsolete SELECT wrappers and switch tests to owned CLOG completion. Extract metadata envelopes, adoption/reseed progress-prefix parsing and named receive-control snapshots. Verify identical current bytes and preserve partial tails and full readback comparisons. |
| 2. Cut over the development baseline (B1–B14, B17–B18) | Regenerate development data and fixtures by format family as retired readers are removed. Keep WAL8, PGSLOT4/5 and recovery-control 3/4 while current producers emit them. Change WAL filename producers before retiring legacy-named data, backups and ancestral WAL. Restart for obsolete image layouts. Verify fresh init, restart, backup, follow, promotion and explicit rejection of retired formats. Pair rollback code with matching data. |
| 3A. Share query semantics (A1, A3, A7–A9, A13) | Start with projection and WindowAgg, then consolidate preparation and admission, types and builtins, scope-aware S-expression visitors and claimed-tuple replacement. Verify push/pull results and errors, row lifetimes and origins, suspension and early stop, cleanup and hot-path allocation. |
| 3B. Share WAL grammar and replay facts (A2, A4) | Move history and redo to shared framing and batch transitions. Accumulate facts in record order and bound retained bytes. Preserve selected end-of-recovery as its own transition. Verify accepted prefixes, incomplete tails, distinct redo and primary settlement order, FIFO retirement and immutable receipts. |
| 4. Consolidate subsystems and enforce dependencies (A10–A12, A15–A16, A19) | Share the catalog snapshot and restore field policy, catalog and settings descriptors, B-tree child-range selection, and one production pgoutput decoder with independent expected results. Split WAL responsibilities after ownership is explicit. Add package and ASDF dependencies once both tracks exercise the interfaces. Verify strict compilation, catalog rollback identity, setting scope and B-tree lock ownership. |
| 5. Retire adapters and update verification and docs (A18, A20) | Remove adapters after their last callers move. Unify discovery and reporting for the existing legacy and Rove suites, keeping their tests in their current harnesses. Update runbooks and format support. Link every audit item to implementation and acceptance evidence. |

Start with the inventory, obsolete SELECT wrappers, envelopes and progress
prefixes, then receive-control snapshots. The query and WAL tracks can proceed
in parallel wherever their inputs are stable. Keep format cutovers, replay-order
changes and package reorganizations in separate PRs.

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

| Audit item | Current writer | Compatibility evidence |
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
| B18 | [WAL initialization](backend/storage/wal-core.lisp) | Fresh clusters select timeline-qualified filenames. A physical base copy inherits the source WAL control and namespace. |

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

For B2, primary WAL control and the independent checkpoint reader require
PGWAL6. The checksummed PGWAL2–5 fixtures are rejected at decode, and the
live checkpoint suite rejects a stopped copy with PGWAL5 before exercising
PGWAL6 crash recovery and standby restartpoints. Both segment namespaces
and the current lifecycle, retained, resource, logical evidence, page
checksum and CLOG extent fields retain their existing checks.

For B3, the checkpoint codec and independent inspection tools require
PGCKPT02. The base snapshot can still omit optional facts. Exhausted
FullXIDs, resource facts, commit timestamp counters, Boolean policy facts
and logical ON intervals retain their existing encodings.

For B4, buffer reads no longer move a historical B-tree type byte out of
the checksum field. The checksum reader rejects that layout without
rewriting the page. Indexes from that layout must be rebuilt from matching
data before using the current reader.

For B5, catalog decode requires a stored publication OID. Startup rejects
missing OIDs without rewriting the catalog. The publication process test
checks that stored OIDs survive a crash and restart, ALTER preserves them,
and dropping and recreating a publication allocates a new OID.

For B6, catalog decode requires a stored, nonempty cluster identity.
Fresh cluster construction still generates and persists an identity,
and existing identities still use the same system identifier projection.
The read path no longer generates a replacement identity.
Replay, adoption and slot checks compare stored identities directly.

For B7, logical slot decode rejects PGSLOT1–3. PGSLOT4/5 remain supported
because current writers emit them, alongside the provenance and child owner
formats. Continuation fixtures use noncanonical PGSLOT4 values to verify
that inventory and no-op proof paths preserve the original bytes.

For B8, the bootstrap pin reader requires PGPHYSPIN2 and rejects a
checksummed PGPHYSPIN1 pin without rewriting it. PGPHYSPIN2 keeps both
manifest-bound and unbound encodings. Follow pins use their own codec.

For B9, reopening the receive store requires PGPHYS03 with a recorded floor
and digest seed. A checksummed PGPHYS02 image is rejected without rewriting
it. PGPHYS03 still supports control with no history selection, with history
selection, and with a retained-root identity.

For B10, startup requires PGRSTAT3 for ordinary recovery or PGRSTAT4 for
pending promotion. The signal harness checks rejection of both retired
formats, preservation of their bytes and recovery data, and successful restart
with the restored current control.

For B11, the backup and recovery readers require a version 3 manifest with
an explicit checkpoint start. The schema rejects versions 1 and 2, missing
checkpoint starts, and inconsistent checkpoint boundaries.

For B12, `64002cfb17e` requires PGRPGEN2 and metadata version 4. The
`t/xid-reservation.sh` fixture exercises startup rejection of retired
generations and checks that their control files are unchanged.

For B13, restart the Lisp server across the gate representation cutover.
The admission gate now closes and reopens with the current quiescence owner
token. The load-time bridge for ownerless gates has been removed.

For B14, restart the Lisp server across the backend layout change. Commit
preflights logical row changes through the transaction spool, and the
snapshot-handoff and rollback tests check that transaction cleanup releases it.

For B17, restart the Lisp server across the portal and statement layout
change. Portal result metadata and prepared result signatures now live on
their owners. The SELECT and DML execution registries still track executable
portal state for cleanup and suspension.

The first A1 slice shares physical row projection between the callback and
iterator drivers. It preserves deferred sort expressions, ProjectSet
pass-through rows, and origin capture through both entry points.
The WindowAgg drivers share partition resets, ranking, aggregate transitions,
and transition origins. The window differential covers one-row portal
resumption for ROWS prefixes, RANGE peer groups, and whole partitions.

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
existing controls. Retiring the legacy grammar requires recreating or
retiring remaining development data, backups, and retained ancestral WAL.

For B16, the three `collect-select-*-rows` wrappers had no production callers.
The joined collector was the only caller of `stream-select-joined-rows`, which
was itself the only caller of `stream-select-joined-environments`. The
FROM-less collector had one legacy test caller. Its regression now executes a
live plan. Cursor execution, plan execution and DML still share
`make-select-result-row`, and live join operators retain their streaming
helpers.

### Milestone 1 verification

- A5 shares the magic, CRC and terminal newline codec across backup lease,
  reseed attempt, adoption attempt and native follow records. Backup and
  reseed still require canonical bytes. Adoption and follow still validate
  their parsed records without requiring a canonical re-encoding. Logical
  slot literals and the installation journals share the sized header and
  checksum parser, with their own record bounds and canonicality checks.
- A6 shares consecutive-step and partial-tail parsing for the adoption and
  reseed installation journals. Each caller still validates its own plan and
  defines its own final step. Adoption follow uses the same parser for its
  steps and checks the retirement marker only after the final complete step.
- The strict physical selection passed 22 test heads covering exact UTF-8
  bytes, checksum failures, canonicality, publication cuts, and journal
  prefix recovery.
- A14 introduces named receive-control snapshots for active follow and
  retained root restoration. Active follow checks selection identity,
  monotone progress and exact readback. Persisted witnesses still use the
  eleven-field list. Seven strict physical test heads passed, including a
  regression that changes each witness field independently. The strict unit
  run passed 1,590 Rove test heads. Pending follow now also compares named
  snapshots before and after receive publication. Rewind uses the complete
  snapshot for readback before publishing in-memory state or removing the
  discarded tail. Retained WAL sealing, archive readback, promotion source
  checks and standby slot validation also use complete snapshots. Follow
  replay, adoption planning and reseed installation, activation and
  retirement match current snapshots against the existing persisted
  witnesses. The follow controller holds named snapshots through planning
  and serializes them when building the binding. Logical slot copy carries
  snapshots through source scans, publication, installation, and cold
  restoration. Private proof images copy the snapshot and compare every
  field. Existing follow bindings and detached physical adoption and reseed
  observations retain their eleven-field value images.
- Logical slot copy verification passed 1,590 strict unit heads, including
  generated proof-copy and exact comparison checks. Native committed-source
  and cold catalog tests passed, along with 54 installation staging crash
  cuts and their retries.

### pgoutput decoder and adapter verification (A12)

- Complete pgoutput messages share `logical-wire-operation` across the
  in-memory and streaming receive paths in `backend/replication/worker.lisp`.
  The protocol fixtures decode complete frames and check independent expected
  values. Rove tuple fixtures use a test-local cursor helper to check field
  markers, offsets, and binary bytea.
- The unit and ASDF interop suites passed after the tuple-only production
  adapter was removed. `t/logical-interior-ack.sh` confirmed exact transaction
  replay after the publisher crashed with an interior acknowledgment, and
  confirmed that a durable acknowledgment at transaction end suppressed replay.
- After the test-only dispatch wrapper was removed, the combined unit suite,
  protocol interop suite, and live `pg-interop-tests/apply` suite passed.
  Live replay applied all six captured C transactions, checked the committed
  progress LSNs, and verified rollback of a multi-relation TRUNCATE. Observer
  queries on truncated relations run after transaction end releases the locks.

### Refactoring audit crosswalk

The audit names below follow the North Star Architecture. Commit IDs identify
implementation slices. A slice can have passing tests while its audit item
still has work outstanding.

| Audit item | Implementation evidence and remaining verification |
|---|---|
| A1. Push and pull query duplication | Projection and WindowAgg transitions are shared in `e13c164f35a` and `0529ff96c23`. The window differential exercises one-row portal resumption. Audit remaining SQL rules and verify row ownership and allocations across both drivers. |
| A2. WAL batch grammar | Shared history and redo staging and terminal decoding in `d63521560e9` and `dd31595f1ad`. Recovery catalog spooling in `b9e448a23bd`. In `d50cd425f32`, primary redo and physical history use the same nonterminal dispatcher. The combined unit suite verified that relation resets and replacement pages are visible before commit status in both page staging modes. The physical replay test held back the row terminal and verified that replay published the preceding complete prefix without exposing the row. The WAL crash recovery and physical receiver exit tests passed, including a multi-segment catalog tail without a terminal and receiver restart with an incomplete batch. |
| A3. Builtin dispatch and metadata | JSON and JSONB inspection descriptors in `17b2661ab5e`. Fixed bytea indexing signatures in `b0944034dd7` share arity, argument coercion, result types, volatility, and strict evaluation. The C differential passed 1,297 comparisons, including qualified calls, smallint widening, bigint rejection, wrong arity, and byte copy-on-write. In `2655494c060`, get_bit and set_bit use the same descriptors for bytea and fixed-bit signatures. Candidate matching follows the C exact-type ranking for these overloads, and execution uses the existing analyzed argument type to dispatch stored calls. The combined unit suite and 1,316 C comparisons passed, including ambiguity, widening, qualified calls, strict argument evaluation, and a fresh-process DEFAULT and CHECK replay. Audit the remaining builtin families. |
| A4. Physical replay fact scans | WAL8 source transaction transitions in `f393cca86ef`. In `58f016b0170`, resource and drop proofs share policy seed validation, and those proofs, follow, and promotion share FPW and logical-policy transitions at complete terminals. In `2b9f7102d03`, both standby redo paths share resource and policy preflight, and selected EOR uses the policy transition after child validation. Thirteen proof heads and ten replay heads passed under strict compilation. The native hot-standby process test passed replay, restart, and promotion. Audit remaining fact consumers for explicit coverage policies. |
| A5. Metadata envelopes | Shared codecs in `b0a0b5b0c2d`. The Milestone 1 strict physical selection exercises exact bytes, checksum failures, and canonicality. |
| A6. Progress journals | Shared installation prefix parsing in `b0a0b5b0c2d`. The Milestone 1 selection exercises partial tails and publication cuts. |
| A7. Claimed-tuple replacement | Shared physical replacement tail in `5c3796e612e`. The DML differential passed 106 cases and 24 controlled concurrency cases, plus resumable RETURNING and restart checks for committed heap and index updates. The logical unchanged-TOAST test passed UPDATE, ON CONFLICT, primary-key change, source crash recovery, and subscriber restart and resume with the exact 40 MiB value. In `880626a7106`, the DML test inspects actual heap tuples before and after restart. It verified the UPDATE and ON CONFLICT successor block and offset, matching predecessor xmax and successor xmin, and the current tuple's self-link. The expanded DML differential passed all 108 SQL cases and 24 controlled concurrency cases. |
| A8. Type and coercion duplication | Shared SQL identities and varchar assignment in `61c670dd78b` and `8bbad6498a0`. In `62e99598bc8`, DDL and casts share varchar typmod validation and the C diagnostic wording, and assignment and cast share character clipping with explicit truncation policy. The independent type system and 1,308 C differential cases passed, including multibyte casts, INSERT and UPDATE, and typmod bounds. In `6aa8c210b1e`, the shared type registry also supplies both directions of array type lookup. The standalone system verified all 24 existing pairs and their array OID round trips. The combined unit suite, 1,308 C differential cases, and ten JSON array diagnostic comparisons passed. In `eeaaeaf6fde`, array catalog alignment derives from element alignment and array storage classification uses the shared array mapping. The PostgreSQL C peer matched all 54 exposed `typalign` rows, including the corrected `oid8` row, and all 1,309 cases in the expanded differential. Audit other coercion contexts and preserve wire and heap representations. |
| A9. Expression and plan walkers | Scope-aware traversal in `5360608bb51` also serves analyzed column and aggregate-volatility checks in `2c77fccce5a`. The Rove regression checks executable IN operands, SubPlan tests, opaque literal payloads, and nested-query boundaries. The subquery and aggregate C differentials passed, with the existing semijoin EPQ difference reproduced. In `ddd2079c05e`, prepared-query and aggregate rebasing rewrites share local operand mapping. The Rove regression distinguishes executable outer references from identically shaped literal payloads and call metadata. Combined units and both C differential suites passed with the same known semijoin EPQ difference. In `e24abd8fca2`, correlation depth analysis shares the local expression visitor and follows only referenced CTE producers. The Rove regression checks SubPlan test and child depth, referenced versus unused CTEs, and opaque metadata. Combined units and 108 subquery C differential cases passed. In `12d763cbf66`, aggregate owner selection also uses the local visitor. Rove verified nearest-level ownership through a sublink, whole-row ownership, and immunity to opaque column-shaped data. Both C differential suites and combined units passed. Audit remaining scope-changing walkers. |
| A10. Catalog snapshot and restore | `715e6eb9f3a` uses the catalog field schema for serialization, owned snapshot copies, and in-place restore. Two strict Rove heads verify table, index, sequence, and publication identity and restoration of independent TOAST and catalog fields. The live catalog durability fault script passed. Virtual relation descriptors in `b6aa35de135` cover catalog view metadata. |
| A11. B-tree equality traversal | Shared child range in `f1439ac21cd`. Separator, lease, and DROP INDEX handoff tests cover equality routing and canonical reader ownership. |
| A12. pgoutput dual parsers | Shared decoder in `970941ce5cd` and final dispatch adapter removal in `265909bd238`. See the pgoutput verification above for independent wire fixtures and live replay. |
| A13. Query setup duplication | Admission and result preparation in `ed467ac1ffa` and `6e51b5345a1`. The portal-snapshot process test passed Parse and Bind visibility, VACUUM retention, and pin retirement on Close, replacement, error, transaction end, and disconnect. The portal lifecycle test passed one-row suspension, resume after concurrent DELETE and VACUUM, reload waiting for SELECT and RETURNING executors, and spill cleanup. The TRUNCATE test passed 15 query shapes across five portal phases and verified that a completed Simple Query releases executor use while a completed named portal retains it until Close. |
| A14. Receive-control snapshots | Named snapshots begin in `7fd971c29ed`. See Milestone 1 for exact-field witness, readback, and logical slot copy verification. |
| A15. WAL core concentration | Recovery and record framing moved in `f1a33ccac3d` and `42c52f3357f`. In `4a0fde3b869`, native archiving moved to its own ASDF component after WAL core. The move preserves archival ownership, rotation and checkpoint recycling calls. Strict compilation and the combined unit suite passed. Live standby archiving passed retry, cold restart, primary handoff and two promotions with byte and proof checks. Archive-to-stream recovery replayed 2,006 rows while the source was down, then resumed on the physical slot. In `3a5dd9adb52`, native slot persistence, acknowledgment and startup restore moved to their own ASDF component. The terminal predicate moved to WAL record framing. Strict compilation and the combined unit suite passed. The live publication fault test verified failed creation fences and exact slot state across cold restart. The logical TOAST test verified acknowledgment and resumption after source and subscriber restart. In `260d4377b50`, transaction and maintenance publication moved into their own ASDF component. Commit timestamp encoding and decoding moved to record framing, and status settlement stays with the WAL page FIFO. Strict compilation, combined units, and live receipt and asynchronous commit tests passed, including FIFO order, immutable receipts, callback failure cleanup, and restart. In `21b4f36837c`, catalog publication moved to its own ASDF component and catalog payload codecs moved to record framing. Strict compilation, combined units, live post-terminal DDL fault injection, and the WAL crash suite passed. The crash suite checks a complete multi-segment catalog and discards a chunk sequence without its terminal before replaying the next complete batch. In `6a024627e6c`, FIFO preparation, ownership handoff, and settlement moved to a shared ASDF component used by transaction and catalog publication. WAL core retains the dirty-page state and checkpoint callers. Strict compilation and combined units passed, including captured-prefix settlement and barrier failures that retain unfinished work. Live asynchronous commit passed acknowledgment, crash recovery, and invalid receipt fencing. |
| A16. Settings metadata | Read-only descriptors in `63085c92361`. The config-output test verified include files, CLI overrides, base units, C metadata and binary numeric parity, and no storage initialization. The numeric and rollback session tests passed, and the application-name C differential verified startup, SQL, wire status, transaction-local restoration, and live reload. The full_page_writes process test passed startup, session context rejection, unchanged and changed SIGHUP reloads, invalid reload rollback, and cold reconciliation. Its probe in `054535a1613` uses the reload timeout when SIGHUP forces code recompilation. |
| A17. Repeated fixtures and fault setup | The producer inventory records current fixture formats. WAL namespace and restartpoint process tests share a stopped-source fixture conversion. In `03c8c542470`, receiver exit, physical replay and retention share native launch and checked control calls while keeping their own cleanup and readiness policies. All three process tests passed. The retention fixture pauses replay explicitly and installs restartpoint thresholds and slot retention through configuration, then verifies local recycling, cold restart and durable pin loss. In `c10f5055bef`, online promotion and ordinary crash restarts use the same fixture helpers, and instrumented startups retain their crash hooks. The crash suite compares recovered rows with the current seed and uses one short control socket path. Online promotion and the expanded crash suite passed, including seven crash cuts, startup signal handling and an unterminated receive batch. In `c7ec1f1ebb9`, namespace migration and automatic restartpoint also share launch and control helpers while retaining the namespace control timeout. Both namespace layouts passed restartpoint recovery and rejected an unproven promotion without changing storage. The migration suite passed its crash and retry boundaries, byte preservation, continued follow and qualified bootstrap. Audit the remaining fault scenarios. |
| A18. Test discovery split | Combined ASDF operation in `455e5e741aa`. The shell run and injected Rove failure evidence above cover execution and reporting. `t/run-unit-tests.sh --list` uses Rove registration and the legacy execution declarations to list 1,596 Rove heads and 166 legacy cases without running them. The legacy listing matches the previous invocation order, including the named VACUUM failpoint variant. Normal execution still passed 1,172 legacy checks. |
| A19. Package and ASDF organization | ASDF explicitly orders the WAL record and recovery components. In `d62f2710c66`, it also orders replay fact derivation after the store history and policy codecs, and before proof scanners and redo. Eight strict physical heads and the timeline-qualified restartpoint process test passed. In `729d9e4e567`, `pg/sql-types` loads independently with SQLSTATE support. In `7bbff227c87`, package `PG.SQL-TYPES` exposes the type operations and keeps the reverse OID index private. A clean-image check verified OIDs and varchar assignment errors. A running server reloaded across the package change and verified that a compiled caller and the exported entry point still use the same symbol. Further package boundaries remain to audit. |
| A20. Documentation drift | The test runbook was updated in `02438307f77`. The [supported native formats](PHYSICAL-HA.md#supported-native-formats) table covers current startup, backup, follow, and restart requirements and distinguishes historical milestones. The producer inventory in `bfb3acb33bf` records current B-tree and catalog reader requirements and links to the extracted slot encoder. Update format support when the remaining cutovers land. |

## Open development tasks

- [x] Bring the legacy WAL slot retention fixture up to the current startup
  contract. The fixture writes replayable batches across three WAL segments
  and restores the durable slot under real startup ownership. Its 12 checks
  pass in isolation, and the combined unit run advances past the fixture.

- [x] Reconcile XID wraparound recovery with strict primary startup. Stage and
  sync the empty CLOG separately, atomically replace `pg_xact`, then sync
  its directory. File and directory fsync faults leave a status file for
  restart and hold the allocation gate closed. The combined unit suite
  passed with 1,174 legacy checks and zero failures.

- [ ] Close out the PostgreSQL backend and pinned Patroni comparison using the
  [goal closeout check](PHYSICAL-HA.md#goal-closeout-check). Standby feedback,
  both logical-slot copy variants, and password-backed bootstrap passed at
  `4679be54534`. Complete the remaining archive and rewind qualification.

- [ ] Bind retained-root deletion to checked archive and host identities before
  permitting deletion after continuation restoration. The fence assumes the
  fixed `PGDATA/pg_physical_retained/` parent. A separate bind-mount alias can
  have another parent and miss that path check; generic root references also
  use canonical directory paths. Qualify aliases with held descriptors and mount
  identities before opening GC after continuation restoration. Current typed
  archive inputs require the fixed child, and the imported-host fence stays closed.

- [x] Allow detached index builds to overlap logical DDL on other tables.
  Capture schema-before and validate subscriber state at final catalog publication,
  after relation gates. Keep response collection separate from publication locks.
  The unchanged name race, durable failure/restart checks and an interleaved
  B-to-A-to-DROP schema chain pass. Code is in `c992a20a742`; evidence is in
  `/home/imbaczek/proj/pg-ha-artifacts/wal-writer-policy/ddl-publication-scope/`.

- [ ] Audit legacy fixtures that leave virtual reader owners behind.
  A full Rove-plus-legacy run reached wraparound with no real XIDs or retained
  snapshots in its fixture, but still waited for readers from earlier fixtures.
  The isolated wraparound fixtures now bind their own virtual reader registry.
  Identify the producers at test boundaries and end their owned backend scopes;
  do not clear live owners globally to make the suite pass. The interrupted run
  and its wait-for-no-active-xids backtrace are archived with the DDL evidence.

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

- [ ] Restore legacy unit-test runner fixture state after the suite. A warm worker
  retained `*active-xids* = (3)` after 1,598 passing checks, causing later physical
  promotion and shutdown checks to fail. Use the intended fresh legacy entrypoint
  for acceptance and inspect or isolate fixture state before later warm suites;
  do not restart an owned live SQL server to hide leaks. A later warm worker also
  had orphan fixture XIDs and cached streams pointing at deleted legacy PGDATA;
  the fresh intended legacy runner passed 1,159 checks. The leaking test and
  suite-isolation fix are not yet known.

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

- [ ] Test ownership revocation across live ASDF reloads with the same streams
  and owner tokens. The stream and directory proof modules keep their weak
  revocation tables in top-level `LET` forms. Check that reloading cannot make
  an observed lock loss reusable after the lock is reacquired.

- [ ] Audit destructive sequence operations on quoted or shared lists, including
  `SORT`, `NREVERSE`, `NCONC` and the final argument to `APPEND`. Installation
  verification once changed a quoted `APPEND` tail, breaking the next clone in
  the same image. A handoff negative test also changed a quoted fixture plist
  reused by later checks. Those cases now use fresh lists; check for other cases
  and exercise repeated operations in the same Lisp image.

- [ ] Audit `wal-atomic-write-bytes` callers for temporary destination aliases.
  Its temporary pathname uses type `tmp`, so a destination already ending in
  `.tmp` can alias the temporary and cleanup can remove the destination. This
  was observed in a new fixture; production callers have not been fully audited.

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

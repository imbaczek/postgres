# Development TODO

See [completed work](COMPLETED.md) for shipped milestones and verification records, and [benchmarking](BENCHMARKING.md) for workload commands, runtime profiles and measurements.

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


See the [format cutover notes](COMPLETED.md#native-format-cutovers-and-implementation-notes)
for implementation and verification history.

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
- Qualify page-copy and writer-latency behavior with the
  [shared-page benchmarks](BENCHMARKING.md#shared-page-cache). Require zero
  full-page copies on eligible resident dirty hits and warm clean-cache hits.

## Performance and coverage follow-ups

- [ ] Extend P02/P06 estimates to more join shapes, buffered-window costs,
  and additional selectivity and row-width cases.
- [ ] Bring P08 indexed scalar probes and array containment scans and P09
  ALTER TYPE rewrites within their 3× C CPU and peak RSS targets.
- [ ] Repeat the P11 primary, ordered and shuffled holdout sweeps until all
  cases consistently meet the 3× C CPU and RSS targets. The latest recorded
  RSS maximum was 3.014× C in one of three sweeps. See the
  [view benchmark](BENCHMARKING.md#views) for the matching profile.

## P12 — SQL functions and PL/pgSQL routines

Persistent routine DDL uses P07 object transactions, dependencies and plan
invalidation. PL/pgSQL exception handlers also need P07 subtransactions.

- [ ] Unify native and stored callable resolution around persistent signatures,
  dependency generations and routine catalog metadata. Verify overload and
  replacement rules and expose the registry for P22.
- [ ] Add transactional CREATE OR REPLACE FUNCTION and DROP FUNCTION for
  scalar SQL routines. Verify STRICT/NULL handling, coercion, volatility,
  rollback and restart against C PostgreSQL.
  The transactional catalog wire fixture covers scalar SELECT bodies,
  positional and named arguments, defaults, STRICT, private replacement,
  crash restart and overload-specific DROP. The shared transaction scenario
  also passes on the C reference.
  CREATE validates the body by default and supports deferred validation
  with `check_function_bodies = off`. The wire fixture also checks failed
  replacement, overload ambiguity and rebind after committed replacement.
- [ ] Run multi-statement SQL bodies in the caller's transaction. Add named
  and default arguments, fixed RETURNS TABLE/SETOF results and resumable
  execution. Verify close, cancel, recursion limits and error cleanup.
  Scalar SQL bodies run SELECT, INSERT, UPDATE and DELETE in the caller's
  transaction, with a final SELECT or DML RETURNING result. Shared C and Lisp
  wire tests cover visibility between volatile statements, rollback after
  an error, and rejection of writes in read-only transactions.
  SQL routines with SETOF and fixed RETURNS TABLE now resume a final SELECT
  across bounded Execute. Wire checks cover target-list laziness, FROM
  materialization and spill, snapshot retention across a concurrent commit,
  Close, LIMIT, cancellation, and returning records. Final DML RETURNING for
  set results and PL/pgSQL set results remain open.
- [x] Share the existing DO runtime with a bounded PL/pgSQL subset covering
  declarations, assignment, flow control, RETURN, PERFORM, SELECT INTO,
  FOUND, RAISE and parameterized dynamic EXECUTE.
  Anonymous blocks and stored scalar functions share these paths. The
  wire fixture compares control flow, STRICT SELECT INTO, FOUND and EXECUTE
  USING with C PostgreSQL, and checks stored PL/pgSQL recovery after restart.
  It verifies that dynamic SELECT INTO finishes all rows, checks variable
  scopes and assignment coercion, and checks invalid statement targets
  during CREATE when body validation is enabled.
- [ ] Add EXCEPTION rollback, SQLSTATE matching and diagnostics using real
  subtransactions. Verify nested failures and routine-backed checks and defaults.
  Exception blocks use the heap, logical change and resource checkpoints
  behind SAVEPOINT. Shared wire tests cover rollback of writes before a
  caught violation, nested rethrow, SQLSTATE classes and stacked diagnostics.
  An injected rollback restoration failure preserves its original SQLSTATE
  and bypasses outer handlers after the transaction has aborted.
  Column defaults and CHECK constraints track stored routine references.
  The paired wire fixture verifies replacement, DROP RESTRICT, DROP CASCADE,
  rollback and crash restart. Logical schema export and dependencies from
  other persistent expressions remain open.
- [ ] Use P23's upstream workload to select record returns, RETURN QUERY,
  catalog helpers and bounded CALL support. Treat procedure transaction
  control as a separate capability.

## P13 — Client protocol and tooling compatibility

- [ ] Capture a versioned client capability matrix for psql, psycopg2 2.9.11
  and a pinned psycopg 3 release. Compare actual SQL and wire traces with C
  PostgreSQL and classify unsupported workflows by protocol, SQL, catalog or codec.
- [ ] Preserve analyzed literal coercions and contextual parameter types in
  prepared statements. Verify omitted or zero OIDs, ambiguous types, repeated
  Bind metadata and supported text and binary formats.
  Parse now allocates inferred slots for OIDs omitted from the wire frame,
  including a partially specified OID list. The binary-values wire fixture
  compares omitted, partial and zero OIDs in cast contexts and untyped
  parameters in an IS NULL test.
- [ ] Qualify named and unnamed statements and portals, bounded Execute,
  Describe, Close, Flush and Sync. Check ReadyForQuery status, aborted
  pipelines, cancel, disconnect and resource cleanup.
- [ ] Support version-specific psql table and database inspection with real
  catalog rows and functions. Add binary codecs required by the admitted types.
- [ ] Run continuous client scenarios for parameterized SQL, DML RETURNING,
  transactions, error recovery, streaming and reconnection after restart.
  Add transactional migration scenarios after the required P07/P09 support
  and use P08 for newly admitted stored types.

## P14 — Autovacuum

- [ ] Deliver the shared scheduler for P15 and P18. Start with one worker,
  bounded dispatch, cooperative cancellation, owned database/backend contexts
  and one active job per relation incarnation. Track transaction-outcome
  changes and verify rescheduling, drop/recreate, shutdown and cleanup.
- [ ] Select vacuum work using base and scale thresholds, insert activity,
  supported table overrides, retry backoff and XID age. Preserve mandatory
  freeze scheduling even when ordinary autovacuum is disabled.
- [ ] Run the existing lazy vacuum operation in the owned scheduler context.
  Compare manual and automatic heap, index, visibility and freeze results
  under the same snapshot and standby feedback horizons.
- [ ] Bound work at safe page or batch boundaries, retry busy relations, and
  expose progress, reclaimed tuples, blocking horizons and the last outcome.
  Verify unrelated writers make progress and canceled jobs release ownership.
- [ ] Qualify near-wraparound freeze, long snapshots, standby feedback, TOAST,
  restart and promotion. Recover from interrupted jobs, start scheduling once
  on promotion and reserve primary heap cleanup for primaries.

## Open development tasks

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

- [ ] Finish target reseed follow handoff and startup reconciliation.
  Persist the target's follow intent and inert configuration before source
  settlement. Reconcile the source outcome and activate follow only after the
  required durable checks; retire installation fences during startup reconciliation.

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

- [ ] Retain source WAL across remote reseed capture and follow handoff.
  Save the original request identity before creating its private bootstrap pin;
  bind the actual verified base and manifest, and support STATUS and checked
  ABORT even when the client never received a manifest. Persist cancellation
  before releasing retention so delayed requests cannot recreate a canceled
  attempt. Keep the source pin until a matching ready follow pin and durable
  target follow configuration own retention. Interrupted attempts must not
  silently recapture a different base under the same identity.

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

- [ ] Support schema-qualified INSERT targets such as `INSERT INTO public.t`;
  the current parser returns 42601 although an unqualified target parses.

- [ ] Match C background WAL write and flush timing and page-rounding policy;
  native eligible cycles currently sync the complete tail under WAL exclusion.

- [ ] Complete the broader synchronous-replication backend contract.
  Persist missing checkpoint policy, freeze, MultiXact, commit-timestamp and
  checksum facts before claiming the complete C checkpoint contract.
  Complete remaining bootstrap and rejoin work, and finish the cold-load audit
  and broader C-style documentation. No coordination is added.

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

- [ ] Audit destructive sequence operations on quoted or shared lists, including
  `SORT`, `NREVERSE`, `NCONC` and the final argument to `APPEND`. Installation
  verification once changed a quoted `APPEND` tail, breaking the next clone in
  the same image. A handoff negative test also changed a quoted fixture plist
  reused by later checks. Those cases now use fresh lists; check for other cases
  and exercise repeated operations in the same Lisp image.

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

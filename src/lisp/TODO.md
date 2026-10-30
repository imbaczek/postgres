# Development TODO

- [x] Implement desired-generation reconciliation for `primary_conninfo` and
  `primary_slot_name` for native standard follow, including reload, durable
  management ownership, cold restart and promotion. `recovery_target_timeline`
  is active at cold startup and changes require restart. Archive-only recovery
  remains unsupported; coordination is outside this scope.

- [ ] Complete the pinned Patroni writer and real `Postgresql.follow` acceptance.
  Its current complete writer snapshots contain 13 unsupported settings:
  `max_locks_per_transaction`, `max_prepared_transactions`,
  `max_worker_processes`, `track_commit_timestamp`, `wal_level`, `wal_log_hints`,
  `hba_file`, `ident_file`, `recovery_target`, `recovery_target_lsn`,
  `recovery_target_name`, `recovery_target_time` and `recovery_target_xid`.
  Keep the complete writer oracle unchanged. Internal checks now derive shared
  relation-object and owner counts from published registrations and waiting
  requests. Cold startup installs a finite shared object/owner pool together with
  the local resource tuple. Workers, prepared transactions and locks still use
  internal values 8, 0 and 64; their public GUCs remain unregistered. Next expose
  those settings with supported semantics and preserve pending-restart behavior.
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

- [ ] Fix signed integer GUC parsing for C23 binary prefixes independently of
  the unsigned recovery timeline parser. C reports `100` for
  `max_connections=0b1100100`; the Lisp normalizer reports an invalid unit.

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
  native follow have completed this pass and its review. Keep the same order for
  the remaining HA work.

- [ ] Restore legacy unit-test runner fixture state after the suite. A warm worker
  retained `*active-xids* = (3)` after 1,598 passing checks, causing later physical
  promotion and shutdown checks to fail. Until fixed, run physical and interop
  suites in a fresh worker before the legacy suite. This is a test isolation issue.

- [ ] Audit cold-image compilation and ASDF load-order dependencies across `pg`
  and its test systems. A warm REPL must not conceal missing declarations or
  compile-time dependencies.
  The physical-slot range validator exposed another instance: calling a timeline
  structure accessor before its `DEFSTRUCT` caused an inlining warning only in a
  cold compile. Moving the slot module after its dependencies fixed that case;
  the wider audit remains open.

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

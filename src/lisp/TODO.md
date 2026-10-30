# Development TODO

- [ ] Implement desired-generation reconciliation for `primary_conninfo`,
  `primary_slot_name` and `recovery_target_timeline` for standard follow.
  Signal-managed startup currently prepares a verified native standby for replay
  only. Archive-only recovery remains unsupported future work; coordination is
  outside this scope. This does not provide full Patroni follow support.

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
  receive-control inventory validation and receive-compaction sealing have completed
  this pass and its review. Keep the same order for the remaining HA work.

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

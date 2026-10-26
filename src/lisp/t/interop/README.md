# Interoperability test inventory

Inventory and refactor review: 2026-10-25, based on commit `19df2752d7e`.

| Tests | Opportunity | Decision |
| --- | --- | --- |
| SQL/parser/catalog error cases in `protocol-tests.lisp`, plus standard/copy/history tests | Repeated `handler-case` scaffolding obscured the SQLSTATE under test. | Replaced 39 handlers with `sqlstate-of`; retained every expected state and operation. A helper regression checks one evaluation, successful return, PostgreSQL errors and propagation of host errors. Removed the narrower copy-only helper. |
| Seven C differential tests in `apply-tests.lisp` | Repeated port parsing, two connections and cleanup. | Added `with-reference-clients`, retaining all SQL cases and direct wire comparisons. Nested cleanup also closes the first connection if opening the second fails. |
| UTF8 name clipping | One explicit multibyte boundary example cannot explore varying character widths. | Kept that example; added a Function Spec for the maximal whole-character prefix within 63 bytes and a separate idempotence Property. Inputs mix one-, two-, three- and four-byte characters, with lengths 0–99. Reference: C `name.c:namein` and `pg_mbcliplen`. |
| Signed commit timestamps | Existing endpoint and endian vectors are strong oracles; generated signed values add breadth. | Kept those vectors; added an eight-octet Function Spec and a signed-int64 roundtrip Property. C `proto.c:logicalrep_write_commit` uses an int64 timestamp; pg-lisp's private WAL remains little endian. |
| Standard subscription resume guard | Fixed examples test metadata changes but few position orderings. | Kept them; added a Property requiring the target marker to cover both source confirmed and restart positions over the unsigned-int64 range. |
| Replica identity key prefix | Existing examples distinguish primary keys from INCLUDE columns. | Kept them; added a Property sampling rotations 0–5 and prefix lengths 1–6, checking sorted keys and invariance when INCLUDE attributes are appended. |
| Publication-selection codec | Generated multi-relation/UTF8 histories could add breadth. | Future candidate. Keep the independent 26-byte fixture, corruption cases and historic catalog mutation/recreation tests. |
| Publication action subsets | All 16 possibilities are already enumerated. | Keep exhaustive deterministic coverage; random sampling would weaken it. |
| C frame fixtures, native subscriber and COPY/apply crash tests | Their value comes from independent bytes, native workers, visibility and durable progress. | Keep explicit. A self-roundtrip property cannot replace these oracles. |
| Legacy `t/unit-tests/` and shell concurrency/crash suites | Many tests manage real files, threads, sockets, WAL and failpoints. | Retain their runners and fixtures; no bulk Rove/cl-spec migration. Existing setup helpers already centralize most shared state. |

The legacy-unit decision above describes the initial interop refactor. The later
[complete test inventory](../TEST-INVENTORY.md) records the reviewed migration of
68 independently runnable cases to `pg-unit-tests` and the 161 deferred cases;
process and concurrency harnesses retain their coverage.

The existing six test files lose 117 net lines, including the new SQLSTATE
helper regression. New generated checks live in `property-tests.lisp`.
That refactor changed no server implementation.

The subsequent metadata/partial-ACK hardening adds a fifth Property for replay
eligibility around a commit record, and a deterministic standard-state protection
test (135 primary Rove tests total). The generated checks still run through four
Rove cases. `../logical-interior-ack.sh` verifies the raw-wire replay contract
across real publisher crashes; it uses a scripted transport peer because the
bundled subscriber correctly acknowledges complete transactions and cannot
produce interior feedback. `../logical-mixed-mode.sh` verifies standard metadata
survives extension bootstrap and DDL on the same target. See the main
compatibility document for their commands, semantics and review evidence.

## Running

See the main README's test dependency setup. The primary `pg-interop-tests`
ASDF/Rove suite executes the Function Specs and Properties through four ordinary
Rove tests, using seed 42 and 200 trials per declaration. Merely registering
declarations does not run them. For interactive inspection, load that system
and use cl-mcp `spec-check` with `function=` for a Function Spec, and `property=`
or `symbol=` for Properties; a function selection does not run its Properties.

The live `pg-interop-tests/apply` and `pg-interop-tests/native` systems require
isolated pg-lisp and unmodified C peers through `PG_INTEROP_TARGET_PORT` and
`PG_INTEROP_REFERENCE_PORT`; `PG_INTEROP_REFERENCE_USER` defaults to postgres.
The apply suite creates fixtures and must start against fresh databases.

## Verification

- Primary Rove: 134 pass; live apply/SQL/wire: 21 pass; native C subscriber: 3 pass.
- ASDF `test-op` also passes, including the generated checks. Strict compilation,
  formatting and all 1,597 legacy checks pass. Local compile/unit logs are in
  `/tmp/pg-test-refactor.j9JIti/`; the task-owned live peers have been stopped.
- Two Function Specs and four Properties each pass 200 trials at seed 42 and
  another fresh seed. Existing declaration digests match on the seed-42 rerun.
- Fresh seeds: name contract `2478105007809290850`, timestamp contract
  `1115415083142307338`, name idempotence `1554083716275278292`, timestamp
  roundtrip `3023390340129460873`, resume floors `1369115907904721316`, identity
  prefix `954190651310676617`.
- Function calls report `properties-not-run, input-coverage-unmeasured`.
  Their separate Property calls cover the selected Properties. Explicit Property
  calls report `rejection-counts-unmeasured, input-coverage-unmeasured`.
  The original `symbol=` selections for name/timestamp reported
  `contract-not-run, rejection-counts-unmeasured, input-coverage-unmeasured`;
  the separate Function Spec calls cover those contracts. These tool-reported
  gaps remain limits of each call; these checks are not exhaustive proofs.
- Luna max reviewed the inventory, Sol high reviewed the complete refactor and
  assertion/query preservation, and Sol medium found no worthwhile further
  helper extraction. C golden bytes and wire comparisons remain independent.

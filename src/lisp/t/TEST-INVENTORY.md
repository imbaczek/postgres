# Legacy test inventory

Reviewed against `af553010d76` (2026-10-26). Scope: all of `src/lisp/t/`, including
legacy unit tests, process tests, fixtures and existing ASDF suites. Tests are
classified individually; a module name does not imply isolation.

## Result

- Original legacy runner: **229 named cases**, **49 helper functions**, and one inline
  assertion checking the explicit timeout argument of `test-wait-until`.
- **68 named cases moved** to [pg-unit-tests](../pg-unit-tests.asd) with real Rove assertions.
- **161 named cases remain** in the legacy runner, with reasons below. Six arena
  cases retain their runtime support guard; counts describe registered runner calls,
  not a promise that an unsupported host executes the guarded bodies.
- New generated coverage: **eight cl-spec Properties and one Function Spec**, explicitly
  executed by seven Rove tests. Three additional Rove cases cover integer endpoints,
  low-XID freeze policy and varchar byte boundaries. Total new suite: **78 Rove tests**.
- No process test was replaced by a unit-level property. Existing physical/interop
  ASDF suites were already Rove/cl-spec and retain their own integration entry points.

## First rebase onto the primary branch

Rebased onto the committed `sbcl` tip `988fd1a7e15` (2026-10-26). The original
`af553010d76` inventory counts remain the baseline for the migration below.
Upstream adds one legacy helper (50 original-plus-upstream helpers total) and
two process-test files; the 229 named-case split remains 68 moved / 161 deferred.

| Upstream addition | Disposition |
| --- | --- |
| `wal-test-legacy-checkpoint-record` in [unit-tests/wal-tests.lisp](unit-tests/wal-tests.lisp) | Retain helper: constructs a checksummed empty v5 checkpoint for compatibility/recovery fixtures. |
| [checkpoint-snapshot.sh](checkpoint-snapshot.sh) | Retain process test: native checkpoint durability and source/standby metadata across crash/restart boundaries. |
| [interop/checkpoint-snapshot.py](interop/checkpoint-snapshot.py) | Retain independent native control/WAL decoder and stopped-server fixture conversion used by the shell harness. |

Post-rebase validation passes: **78 unit Rove tests**, **1,131 deferred legacy
checks**, **169 interop tests**, **173 physical tests**, and strict fresh
`./t/compile.sh`. The combined shell runner exits 0 with the cl-spec checkout
registered through `CL_SOURCE_REGISTRY`, as in the original verification.
The Rove unit run explicitly executes all generated checks with seed 42.
Sol 6.1/high reviewed the checkpoint/WAL overlap and found no code blocker;
the migrated envelope checks remain valid with the current WAL version.
The added process harness is inventoried here; this rebase did not rerun it.

## Second rebase and new-test review

Rebased again onto committed `sbcl` tip `23703870da0` (2026-10-26), reviewing
all six commits after `988fd1a7e15`. No changes were added to
`t/unit-tests.lisp` or `t/unit-tests/`: **no new legacy cases need porting**.
The original 68 moved / 161 deferred named-case split and original-plus-upstream
50 legacy helpers remain unchanged.

Upstream adds **35 Rove cases**, all already wired into `pg-physical-tests`:
five in `xid-reservation-tests.lisp`, 19 in `wal-xid-reservation-tests.lisp`,
two in `timeline-publication-tests.lisp`, five in `timeline-tests.lisp`, one
each in the physical control/recovery modules, and two in restartpoint tests.
The four new cl-spec Properties have explicit Rove runners. Moving these cases
again would add no framework coverage; retain their existing suite/fixtures.

Every file added by this upstream range is accounted for below:

| New file | Disposition |
| --- | --- |
| [xid-reservation-tests.lisp](xid-reservation-tests.lisp) | Already Rove/cl-spec: reservation boundaries and canonical FullXIDs, including two generated Properties. |
| [wal-xid-reservation-tests.lisp](wal-xid-reservation-tests.lisp) | Already Rove/cl-spec: inclusive reservation codec Property, allocator durability/failure fixtures, visibility/capacity separation and legacy recovery. |
| [timeline-publication-tests.lisp](timeline-publication-tests.lisp) | Already Rove: exact parent-byte/newline handling and invalid/oversized reasons. |
| [timeline-publication-worker.lisp](timeline-publication-worker.lisp) | Retain process-owned publication/crash barrier helper; not an independent unit case. |
| [xid-legacy-migration.lisp](xid-legacy-migration.lisp) | Retain stopped-target old-generation fixture conversion and startup-crash helper; process ownership is essential. |
| [xid-reservation.sh](xid-reservation.sh) | Retain process acceptance: allocation WAL durability/crashes, standby visibility and legacy restartpoint migration. |
| [timeline-publication.sh](timeline-publication.sh) | Retain process acceptance: immutable publication, races, crashes and replication-wire output. |
| [interop/timeline-publication.py](interop/timeline-publication.py) | Retain independent pinned Patroni driver and replication-wire byte oracle. |

The modified physical control/recovery/restartpoint/stream modules and timeline
module already use Rove/cl-spec. Existing `interop/checkpoint-snapshot.py`,
`interop/recovery-control-state.py` and `wal-crash-recovery.sh` remain external
inspectors/harnesses. The added timeline encoding Property checks the exact fork;
fixed byte layout and process oracles remain complementary evidence.

Validation after this rebase: **78 unit Rove tests**, **1,131 deferred legacy
checks**, **169 interop tests**, **208 physical tests**, and strict fresh
`./t/compile.sh` all pass. The fresh combined shell runner exits 0 using the
previously documented ASDF dependency configuration. All generated Rove wrappers
execute at seed 42. Direct fresh-seed checks of the four new Properties also
pass 200 executed trials each:

| Property (package PG-PHYSICAL-TESTS) | Additional seed | Declaration digest |
| --- | --- | --- |
| `canonical-reservation-end-preserves-normal-full-xids` | `3374323980364780765` | `fnv1a64-v1:8bc7ced5240fd0ef` |
| `canonical-special-reservation-end-skips-to-first-normal` | `4425964474268112090` | `fnv1a64-v1:1dead604fc4cc1d3` |
| `wal-xid-reservation-preserves-canonical-inclusive-bound` | `2518200698590122520` | `fnv1a64-v1:c3baa8de42545025` |
| `timeline-encoded-child-starts-at-exact-fork` | `528944732803907763` | `fnv1a64-v1:d21b5f8e46f5379e` |

Each direct call reports `rejection-counts-unmeasured, input-coverage-unmeasured`;
these are supplemental bounded checks. The new process harnesses were inventoried,
not rerun here. Sol 6.1/high reviewed the new-test dispositions and rebase overlap:
WAL v7 retains the 32-byte record envelope; fixture risks concern reservation
flush timing and keeping actual visibility separate from allocation capacity.
No migrated assertion or upstream test required modification.

## Third rebase: timeline-qualified WAL namespace

Rebased onto committed `sbcl` tip `d01f1e0e60f` (2026-10-26), including all
seven commits after `23703870da0`. No legacy runner or legacy unit module changed.
The migration assertions and 68 moved / 161 deferred split remain intact.

Upstream adds **22 Rove cases**, already wired into `pg-physical-tests`:
15 in `wal-namespace-tests.lisp` (including its generated-property runner),
five in `checkpoint-tests.lisp`, and two in `timeline-publication-tests.lisp`.
The two new Properties, `wal-segment-identity-roundtrips-without-branch-aliasing`
and `wal-control-preserves-independent-insertion-identity`, have explicit Rove
runners. No additional framework conversion is needed.

| New file | Disposition |
| --- | --- |
| [wal-namespace-tests.lisp](wal-namespace-tests.lisp) | Already Rove/cl-spec: timeline-qualified identities, namespace validation, migration/reconciliation and recycling fixtures. |
| [wal-namespace-failpoints.lisp](wal-namespace-failpoints.lisp) | Retain process-owned namespace migration fault hooks. |
| [wal-namespace-startup-crash.lisp](wal-namespace-startup-crash.lisp) | Retain process-owned startup/crash helper. |
| [wal-namespace.sh](wal-namespace.sh) | Retain migration/crash/native-peer process acceptance. |
| [interop/wal-namespace-inspect.py](interop/wal-namespace-inspect.py) | Retain independent filename, control and digest inspector. |

Rebase validation passes: **78 unit Rove tests**, **1,131 deferred legacy checks**,
**169 interop tests**, **230 physical tests**, and strict fresh `./t/compile.sh`.
The fresh combined shell runner exits 0 with the documented ASDF dependency
configuration. Generated Rove checks execute at seed 42. The new process harness
is inventoried here, not rerun. Sol 6.1/high reviewed the semantic overlap; no
migrated assertion required adjustment for timeline-qualified filenames.

## Fourth rebase: promotion, lineage and native control

Rebased onto committed `sbcl` tip `fd33a1ac343` (2026-10-26), reviewing all
31 commits after `d01f1e0e60f`. The legacy runner and legacy unit modules have
no changes in this range. The migration still moves 68 original cases and
retains 161; **no new legacy cases need porting**.

The 41 added files comprise 11 Lisp test modules already using Rove/cl-spec,
seven Lisp process helpers, ten shell harnesses, eleven Python clients/oracles,
one C layout oracle and one generated-artifact ignore file. All new Lisp test
modules are registered in `pg-physical-tests`; all 11 new Properties have
explicit Rove runners, and there are no new Function Specs.

| New physical-suite module | Rove cases (including generated runners) | Properties |
| --- | ---: | ---: |
| [control-data-tests.lisp](control-data-tests.lisp) | 8 | 1 |
| [physical-prefix-tests.lisp](physical-prefix-tests.lisp) | 6 | 1 |
| [promotion-commit-tests.lisp](promotion-commit-tests.lisp) | 5 | 1 |
| [promotion-control-tests.lisp](promotion-control-tests.lisp) | 8 | 1 |
| [promotion-intent-tests.lisp](promotion-intent-tests.lisp) | 14 | 2 |
| [promotion-online-tests.lisp](promotion-online-tests.lisp) | 3 | 0 |
| [promotion-resume-tests.lisp](promotion-resume-tests.lisp) | 5 | 1 |
| [promotion-wal-tests.lisp](promotion-wal-tests.lisp) | 6 | 1 |
| [wal-eor-tests.lisp](wal-eor-tests.lisp) | 15 | 1 |
| [wal-lineage-tests.lisp](wal-lineage-tests.lisp) | 8 | 1 |
| [wal-timeline-tests.lisp](wal-timeline-tests.lisp) | 7 | 1 |
| **Total** | **85** | **11** |

Existing [interop/wal-filename-tests.lisp](interop/wal-filename-tests.lisp)
adds one Rove case for the insertion timeline used in SQL WAL filenames.
Modified restartpoint and reservation tests retain their existing Rove fixtures.
The remaining added files retain their current roles:

| New file | Retained role / reason |
| --- | --- |
| [control-data-native.lisp](control-data-native.lisp) | Process-owned native control lifecycle server fixture. |
| [control-data-promote.sh](control-data-promote.sh) | Process acceptance for control state across promotion. |
| [control-data.sh](control-data.sh) | Native control utility and lifecycle process acceptance. |
| [interop/.gitignore](interop/.gitignore) | Generated test artifact housekeeping. |
| [interop/control-data-clients.py](interop/control-data-clients.py) | External SQL/replication clients for native control lifecycle tests. |
| [interop/control-data-inspect.py](interop/control-data-inspect.py) | Independent native control and durable-state inspector. |
| [interop/control-data-reject.py](interop/control-data-reject.py) | External rejection/unchanged-storage oracle. |
| [interop/eor-layout.c](interop/eor-layout.c) | Independent pinned PostgreSQL C end-of-recovery layout oracle. |
| [interop/patroni-promote-request.py](interop/patroni-promote-request.py) | Pinned Patroni promotion request client. |
| [interop/patroni-promote.py](interop/patroni-promote.py) | Pinned Patroni promotion completion oracle. |
| [interop/patroni_native_node.py](interop/patroni_native_node.py) | Native node fixture for the pinned Patroni driver. |
| [interop/pg-ctl-promotion-reject.py](interop/pg-ctl-promotion-reject.py) | External pg_ctl promotion rejection oracle. |
| [interop/promotion-idle-client.py](interop/promotion-idle-client.py) | Connection owner for idle-reader/promotion process tests. |
| [interop/promotion-primary-inspect.py](interop/promotion-primary-inspect.py) | Independent promoted-primary durable-state inspector. |
| [interop/promotion-sql-contract.py](interop/promotion-sql-contract.py) | Independent C SQL/retained-reader promotion contract oracle. |
| [interop/restartpoint-prefix-inspect.py](interop/restartpoint-prefix-inspect.py) | Independent restartpoint prefix/record/digest inspector. |
| [promotion-commit-native.lisp](promotion-commit-native.lisp) | Process-owned promotion completion and publication/crash fixture. |
| [promotion-commit.sh](promotion-commit.sh) | Promotion completion publication/crash process acceptance. |
| [promotion-fill-wal.lisp](promotion-fill-wal.lisp) | Utility that pads real native WAL through its insertion path. |
| [promotion-intent-native.lisp](promotion-intent-native.lisp) | Process-owned intent publication and crash fixture. |
| [promotion-intent.sh](promotion-intent.sh) | Promotion intent publication/crash process acceptance. |
| [promotion-online-crash.sh](promotion-online-crash.sh) | Live promotion crash/restart process acceptance. |
| [promotion-online-native.lisp](promotion-online-native.lisp) | Server launch and online-promotion hook fixture. |
| [promotion-online.sh](promotion-online.sh) | Native pg_ctl/SIGUSR1/Patroni live promotion acceptance. |
| [promotion-prefix-native.lisp](promotion-prefix-native.lisp) | Process-owned child prefix publication/crash fixture. |
| [promotion-prefix.sh](promotion-prefix.sh) | Authenticated child WAL installation process acceptance. |
| [promotion-resume-native.lisp](promotion-resume-native.lisp) | Fresh-process pending-promotion reconstruction fixture. |
| [promotion-resume.sh](promotion-resume.sh) | Cold pending-promotion reconstruction process acceptance. |
| [promotion-sql-c.sh](promotion-sql-c.sh) | Independent C SQL promotion contract with retained readers; does not claim native SQL promotion support. |
| [promotion-wal.sh](promotion-wal.sh) | Process harness for history/end-of-recovery publication. |

Native-helper assertions depend on owned processes, stopped clones, immutable
on-disk bytes, PID ownership, SIGKILL barriers or fresh-process reconstruction.
Keep them beside those actions; a pure unit assertion cannot replace that
coverage. The C promotion SQL/retained-reader oracle remains independent of
native promotion prerequisites.

Validation after this rebase passes: **78 unit Rove tests**, **1,131 deferred
legacy checks**, **170 interop tests**, **315 physical tests**, and strict fresh
`./t/compile.sh` including its new deferred-warning guard. The fresh combined
shell runner exits 0 using the documented ASDF dependency configuration.
Generated Rove checks execute at seed 42. The added shell/C acceptance harnesses
were inventoried, not rerun as standalone harnesses here.

Sol 6.1/high reviewed case counts, wiring and the full file delta. Astra/medium
reviewed the process-helper boundaries and semantic overlap. No migrated
assertion needs adjustment: the current WAL still has a 32-byte envelope, and
its EOR payload validation does not change the raw commit-envelope contract.
Promotion admission, actual XID visibility versus reserved capacity, retained
readers and cold reconstruction remain responsibilities of their own fixtures.

## Latest rebase: reader acquisition, process fixtures and native SQL promotion

Rebased onto committed `sbcl` tip `41e28bdf6cf` (2026-10-27), covering
53 commits after `fd33a1ac343`. The original baseline remains 229 cases and
49 helpers, with 68 cases moved and 161 deferred; no new legacy defuns need
porting. The four modified legacy modules retain their deferred changes. The
upstream reader getter stubs were carried into the two migrated registration
cases; their assertions remain unchanged. Sol 6.1/high reviewed the overlap:
the other 66 migrated bodies and seven shared helpers are unchanged, with no
missing incoming migrated updates.

The 44 added files include 13 Rove suite modules, nine Lisp process helpers,
ten shell harnesses and twelve Python clients/oracles. The new modules have
172 cases and 11 Properties:

| New suite module | Rove cases | Properties |
| --- | ---: | ---: |
| [btree-read-lease-tests.lisp](btree-read-lease-tests.lisp) | 12 | 0 |
| [child-bootstrap-tests.lisp](child-bootstrap-tests.lisp) | 19 | 2 |
| [executor-resource-tests.lisp](executor-resource-tests.lisp) | 27 | 0 |
| [fork-follow-tests.lisp](fork-follow-tests.lisp) | 4 | 1 |
| [index-scan-detachment-tests.lisp](index-scan-detachment-tests.lisp) | 28 | 1 |
| [live-operation-parking-tests.lisp](live-operation-parking-tests.lisp) | 16 | 0 |
| [physical-row-replay-tests.lisp](physical-row-replay-tests.lisp) | 9 | 2 |
| [physical-store-history-tests.lisp](physical-store-history-tests.lisp) | 16 | 1 |
| [promotion-sql-tests.lisp](promotion-sql-tests.lisp) | 9 | 1 |
| [running-xacts-tests.lisp](running-xacts-tests.lisp) | 14 | 1 |
| [source-xids-replay-tests.lisp](source-xids-replay-tests.lisp) | 9 | 1 |
| [source-xids-snapshot-tests.lisp](source-xids-snapshot-tests.lisp) | 7 | 1 |
| [statement-snapshot-scope-tests.lisp](statement-snapshot-scope-tests.lisp) | 2 | 0 |
| **Total** | **172** | **11** |

Existing [physical-config-tests.lisp](physical-config-tests.lisp) adds eight
cases and one Property, [physical-recovery-tests.lisp](physical-recovery-tests.lisp)
ten cases, [promotion-commit-tests.lisp](promotion-commit-tests.lisp) 25 cases
and one Property, and [wal-namespace-tests.lisp](wal-namespace-tests.lisp) one
case. The total delta is 216 Rove cases and 13 Properties. All Properties have
explicit Rove runners, with no new Function Specs; these suites are already
wired into the framework and need no further port.

| Added files | Disposition |
| --- | --- |
| [application-name-native.lisp](application-name-native.lisp), [child-bootstrap-native.lisp](child-bootstrap-native.lisp), [executor-parking-native.lisp](executor-parking-native.lisp), [index-scan-parking-native.lisp](index-scan-parking-native.lisp), [live-operation-parking-native.lisp](live-operation-parking-native.lisp) | Retain process-owned native fixtures for their corresponding clients and harnesses. |
| [physical-row-replay-native.lisp](physical-row-replay-native.lisp), [physical-store-history-crash-child.lisp](physical-store-history-crash-child.lisp), [promotion-readers-native.lisp](promotion-readers-native.lisp), [promotion-sql-native.lisp](promotion-sql-native.lisp) | Retain process fixtures for replay, crash history, readers and SQL promotion. |
| [application-name.sh](application-name.sh), [child-bootstrap.sh](child-bootstrap.sh), [executor-parking.sh](executor-parking.sh), [index-scan-parking.sh](index-scan-parking.sh), [live-operation-parking.sh](live-operation-parking.sh) | Retain process harnesses for native clients, bootstrap and parking. |
| [physical-readers.sh](physical-readers.sh), [promotion-readers.sh](promotion-readers.sh), [promotion-sql-faults-c.sh](promotion-sql-faults-c.sh), [promotion-sql-faults.sh](promotion-sql-faults.sh), [promotion-sql.sh](promotion-sql.sh) | Retain reader and promotion process harnesses, including independent C fault coverage. |
| [interop/application-name.py](interop/application-name.py), [interop/child-bootstrap.py](interop/child-bootstrap.py), [interop/executor-parking.py](interop/executor-parking.py), [interop/index-scan-parking.py](interop/index-scan-parking.py), [interop/live-operation-parking.py](interop/live-operation-parking.py) | Retain external clients and oracles for the corresponding process harnesses. |
| [interop/owned-peer.py](interop/owned-peer.py), [interop/parking_wire.py](interop/parking_wire.py), [interop/physical-readers.py](interop/physical-readers.py), [interop/promotion-readers.py](interop/promotion-readers.py) | Retain owned-peer and wire support and independent reader clients. |
| [interop/promotion-sql-faults.py](interop/promotion-sql-faults.py), [interop/promotion-sql-native.py](interop/promotion-sql-native.py), [interop/signal-owned-server.py](interop/signal-owned-server.py) | Retain promotion clients and owned-server signaling support. |

These process fixtures remain necessary for crashes, replay and owned resources.
Native SQL `pg_promote` is now implemented; the earlier C-only notes describe
historical evidence and are superseded by this section for current support.

After conflict resolution, a fresh cl-mcp worker rebuilt `pg` with cleared FASLs:
**78 unit**, **170 interop**, and **531 physical** Rove tests passed. A fresh
combined `./t/run-unit-tests.sh` exited 0 with **1,131 legacy checks and zero
failures**, using the documented `CL_SOURCE_REGISTRY` for installed cl-spec.
Strict cold `./t/compile.sh`, including deferred-warning checks, passed. The
added shell and C acceptance harnesses were inventoried, not run standalone.

## Expression subquery coverage

[subquery-tests.lisp](rove/subquery-tests.lisp) adds 21 Rove cases (78 to 99
unit cases), one Function Spec and two Properties. The original 161 deferred
legacy cases and 1,131 checks retain their disposition. These contracts cover
the requested feature; no existing contracts were weakened. The existing
[join-ddl-concurrency.sh](join-ddl-concurrency.sh) baseline fixture uses one
DROP gate and opposite-order JOINs because DDL events are globally serialized;
its wait ID fields are read-only.

Final verification passed: **99 unit**, **170 interop**, and **531 physical**
Rove cases; **1,131 legacy checks**; strict compilation; and smoke, sort,
index and JOIN checks. Native/C acceptance passed 108 SQL differential cases,
seven controlled concurrent SubPlan rechecks, spill/cancel cleanup and indexed
self-read with concurrent DDL. The separate simple-EXISTS semijoin EPQ divergence
remains documented in [SUBQUERIES.md](SUBQUERIES.md).

Final direct cl-spec verification executed 1,500 trials, in addition to the Rove
wrappers. Each contract ran at seed 42 and the additional seed shown:

| Contract | Trials per seed | Additional seed | Digest | Measurement gaps |
| --- | ---: | ---: | --- | --- |
| `subquery-quantified-step` Function Spec | 300 | 1959987157382440046 | `fnv1a64-v1:a8675611be5b6d63` | properties-not-run, input-coverage-unmeasured |
| `subquery-quantified-fold-truth` Property | 300 | 1553238518679453301 | `fnv1a64-v1:f04501586e6b165d` | rejection-counts-unmeasured, input-coverage-unmeasured |
| `subquery-generated-scalar-cardinality` Property | 150 | 2279626208607443807 | `fnv1a64-v1:270189609c6b43bd` | rejection-counts-unmeasured, input-coverage-unmeasured |

## Window coverage

[window-tests.lisp](rove/window-tests.lisp) adds nine Rove cases, increasing the
unit suite from 99 to 108, and one Function Spec and two Properties.
Final verification passed 108 unit,
170 interop and 531 physical Rove cases, 1,131 legacy checks, strict compile,
smoke, JOIN streaming, sort arena and subquery regressions. The native/C harness
passed 168 SQL cases, including 30 deterministic generated cases with ties,
plus spill, cancellation and recovery checks.

Final direct cl-spec verification ran 1,500 trials, in addition to Rove wrappers:

| Contract | Trials per seed | Seeds | Digest | Measurement gaps |
| --- | ---: | --- | --- | --- |
| window-rank-step Function Spec | 300 | 42, 1959987157382440046 | `fnv1a64-v1:7094ded7ea5ec5c3` | properties-not-run, input-coverage-unmeasured |
| window-ranking-peer-sequence Property | 300 | 42, 1553238518679453301 | `fnv1a64-v1:529175196b310703` | rejection-counts-unmeasured, input-coverage-unmeasured |
| window-generated-prefix-totals Property | 150 | 42, 2279626208607443807 | `fnv1a64-v1:037354165c5b9e0c` | rejection-counts-unmeasured, input-coverage-unmeasured |

## Nonrecursive CTE coverage

[cte-tests.lisp](rove/cte-tests.lisp) adds six Rove cases, bringing the unit
suite to 114. The supplied validation passed 114 unit, 170 interop and 531
physical Rove cases. [ctes.sh](ctes.sh) passed 114 C differential cases and
spill, error and cancellation paths for SELECT and INSERT. See [CTES.md](CTES.md)
for the materialization and optimizer boundaries. The 168-case window regression
and the 114-unit and 170-interop suites also passed again after the final fixes.

Direct cl-spec checks of `cte-replay-readers-share-prefix` passed 300 trials
each with seeds `42` and `20261027`, definition digest
`fnv1a64-v1:ed029f74af8a5388`. Reported verification gaps were
`rejection-counts-unmeasured` and `input-coverage-unmeasured`; this property
checks generated in-memory reader interleavings, while the C harness checks
spill and SQL behavior separately. The fresh legacy runner passed 1,131 checks.

## Recursive CTE coverage

[recursive-ctes.sh](recursive-ctes.sh) and its independent C differential
client [recursive_ctes.py](interop/recursive_ctes.py) remain process tests:
they passed 60 SQL comparisons and forced frontier, result and seen-set spill,
cycle, cancellation (`57014`), temporary quota (`53400`), error and recovery
checks. The Rove unit suite passed 126 cases, interop 170 and physical 531;
the legacy runner passed 1,131 checks.
Existing CTE (114), HAVING (118), window (168), subquery, join, index and
smoke regressions, strict fresh compilation and formatting checks passed.
See [recursive CTEs](RECURSIVE-CTES.md) for scope and
performance limits.

## Running and failure behavior

From `src/lisp/`, `./t/run-unit-tests.sh` loads [test-dependencies.lisp](test-dependencies.lisp),
which tries configured ASDF systems before the active Quicklisp installation;
when Quicklisp is absent it tries `QUICKLISP_HOME/setup.lisp` or `~/quicklisp/setup.lisp`.
The runner preserves SBCL init configuration. cl-spec must be registered with
ASDF or installed under the active Quicklisp local-projects directory; see the
[dependency setup](../README.md#testing), including `CL_SOURCE_REGISTRY` for an
external checkout. The runner then runs the new ASDF suite first,
then all deferred cases and the inline wait-helper assertion. Rove failure causes
an ASDF error; legacy failures signal an error instead of exiting the Lisp image.
Both propagate a nonzero status from the shell runner. Loading `pg-unit-tests`
alone defines tests without running them or loading the legacy runner.

With the test dependencies installed, in a REPL with this directory registered:

```lisp
(asdf:test-system "pg-unit-tests")
;; Individual cases remain selectable through Rove/cl-mcp, e.g.:
;; run-tests system=pg-unit-tests test=pg::test-tuple-roundtrip
```

Rove and `cl-spec/check-it` are test-only dependencies; `pg` does not load them.
The tests intentionally remain in package `PG`: several macros expand into PG
symbols which the original lexical `flet` fixtures intercept. Helpers shared with
legacy cases load from [rove/support.lisp](rove/support.lisp) before legacy modules.
The heap-edit macro fixture now also intercepts visibility-map clearing and checks
it happens once, removing an implicit relation-file dependency.

The converted cases require no shared PGDATA/CLOG fixture. Remaining tests still
use the original shared fixture and execution order. Do not parallelize them with
tests that replace global functions or mutate admission/ownership state.

## Generated domains and limits

The migrated declarations below use `:normal 200` for Properties and 200 Function
Spec trials. Subquery declarations use the budgets in their coverage section above.
Rove runs these declarations with seed `42`. Direct cl-spec runs can use another seed; record the actual seed,
declaration digest and implementation revision when reporting a failure. Custom
CL:RANDOM generators use cl-spec's dynamically seeded random state.

| Check | Domain and oracle |
| --- | --- |
| `key-compare` Function Spec | Fresh byte vectors up to 64 bytes; independent pairs, identical vectors, strict prefixes, and shared prefixes ending in 0/255; compared to CL string lexicographic order. Correlated extension cases can reach 65 bytes. |
| `unit-int4-index-order`, `unit-int8-index-order` | Full signed integer ranges; encoded byte order versus arithmetic sign. Fixed Rove cross-products cover min/max, adjacent endpoints, -1/0/1 and equality. |
| `unit-tuples-preserve-values-and-header` | Three int4/varchar/int8 columns, all NULL masks, bounded Unicode text (at most 120 UTF8 bytes), xmin 3–2^64−1 and xmax 0–2^64−1, command ID and successor TID fields. Assert values, low 32-bit XIDs, command ID, successor TID and NULL/varwidth flags. Fixed examples retain exact header offsets and varchar 126/127-byte boundaries. |
| `unit-wal-records-preserve-fields` | Raw record envelope with 0–64 payload bytes, unsigned 64-bit XID and start bounded to avoid end overflow. Assert type/version/fields/payload and 32-byte header arithmetic. Arbitrary commit payloads here are envelope inputs, not semantically valid commits for replay. |
| `unit-page-compaction-preserves-live-items` | 1–64 items, 1–48 bytes each, repeated eight-bit dead-slot patterns; preserve live payloads/line pointers and never reduce free space. This does not establish optimal space reclamation. |
| `unit-freeze-cutoffs-respect-policy-and-horizon` | Normal/aggressive threshold bands including age 0/1 and threshold ±1, snapshot lag and forced cutoff; check policy arithmetic, horizon clamp and FULL delegation. Fixed examples cover low-XID clamping, NIL/future relfrozenxid and exact horizon/target equality. |
| `unit-logical-row-images-roundtrip` | Insert/update/delete over one fixed three-column schema and key shape, signed integers, bounded Unicode and nullable non-key values; preserve row images and metadata. |
| `unit-top-n-matches-stable-sort` | 0–64 rows, 0–64 LIMIT, ascending/descending, repeated integer keys; compare row identities and tie order against CL stable-sort and check retained-row bound after each input. |

Round trips can share encoder/decoder bugs. Fixed corruption checks, known bytes,
header offsets, wire field ordering and external PostgreSQL peer tests remain.
Generated input coverage and rejection counts are not measured by Property runs;
these checks do not prove full compatibility, durability or scheduling behavior.

## Named unit cases

Rove = fixed regressions retained. Both = those regressions plus a generated law
about part of the behavior. Legacy = retained runnable, with the fixture work
needed before later migration. Reason codes: D storage/durability; F filesystem;
H global hooks; M shared mutable ownership/registry state; T threads/interrupts;
A implementation-specific arenas. Deferral is about fixture readiness, not a
limitation of Rove: concurrent and stateful tests can use Rove once isolated.

| Original module | Named cases | Moved | Deferred |
| --- | ---: | ---: | ---: |
| `primitives-tests.lisp` | 6 | 5 | 1 |
| `storage-tests.lisp` | 8 | 5 | 3 |
| `sql-tests.lisp` | 19 | 8 | 11 |
| `transaction-tests.lisp` | 10 | 1 | 9 |
| `catalog-tests.lisp` | 16 | 7 | 9 |
| `protocol-tests.lisp` | 7 | 2 | 5 |
| `logical-tests.lisp` | 48 | 21 | 27 |
| `vacuum-tests.lisp` | 19 | 4 | 15 |
| `join-tests.lisp` | 5 | 1 | 4 |
| `arena-tests.lisp` | 7 | 0 | 7 |
| `ownership-tests.lisp` | 8 | 0 | 8 |
| `backend-lifecycle-tests.lisp` | 8 | 1 | 7 |
| `concurrency-tests.lisp` | 18 | 1 | 17 |
| `snapshot-tests.lisp` | 5 | 3 | 2 |
| `spill-tests.lisp` | 21 | 4 | 17 |
| `wal-tests.lisp` | 24 | 5 | 19 |

### primitives

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-page-roundtrip` | [Both](rove/primitives-tests.lisp) | page compaction |
| `test-atomic-output` | [Legacy](unit-tests/primitives-tests.lisp) | F/D: atomic control-file writes, fsync ordering and rename failure |
| `test-backend-wait-state` | [Rove](rove/primitives-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-heap-tuple-edit` | [Rove](rove/primitives-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-with-registered-table` | [Rove](rove/primitives-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-with-registered-relation-access-set` | [Rove](rove/primitives-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |

### storage

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-live-page-items-stop` | [Rove](rove/storage-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-live-page-item-guard` | [Rove](rove/storage-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-page-compaction` | [Both](rove/storage-tests.lisp) | page compaction |
| `test-tuple-roundtrip` | [Both](rove/storage-tests.lisp) | tuple values and header |
| `test-full-xid-tuple-format` | [Both](rove/storage-tests.lisp) | tuple low-32-bit XIDs and header |
| `test-xid-tuple-update-roundtrip` | [Legacy](unit-tests/storage-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-heap-update-chain-follow` | [Legacy](unit-tests/storage-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-heap-free-space-map` | [Legacy](unit-tests/storage-tests.lisp) | D: relation files, stream/FSM caches or CLOG |

### sql

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-truncate-parser` | [Rove](rove/sql-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-replication-slots-catalog-view` | [Legacy](unit-tests/sql-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-sql-strict-let-evaluation` | [Rove](rove/sql-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-sql-current-wal-lsn` | [Rove](rove/sql-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-query-plan-ir-and-count-aggregate` | [Rove](rove/sql-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-analyze-parser-and-costed-join-order` | [Legacy](unit-tests/sql-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-parameterized-join-streaming-and-reordered-slots` | [Rove](rove/sql-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-query-plan-index-lock-order` | [Legacy](unit-tests/sql-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-index-scan-limit-stops-duplicate-candidates` | [Legacy](unit-tests/sql-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-query-plan-compiler-backend-hook` | [Legacy](unit-tests/sql-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-query-plan-compiler-dispatch-uses-registry-snapshot` | [Legacy](unit-tests/sql-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-query-plan-backend-registry-synchronization` | [Legacy](unit-tests/sql-tests.lisp) | T: threads/interrupts at shared registry, snapshot or locking boundaries |
| `test-insert-index-catalog-snapshot-lock` | [Legacy](unit-tests/sql-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-select-top-n-collector` | [Both](rove/sql-tests.lisp) | stable-sort differential |
| `test-using-join-output-metadata` | [Rove](rove/sql-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-error-cleanup-and-fatal-rethrow` | [Rove](rove/sql-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-create-table-cleanup-after-fsync-error` | [Legacy](unit-tests/sql-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-relation-build-own-xid` | [Legacy](unit-tests/sql-tests.lisp) | M: global relation writer/builder ownership maps |
| `test-parse-error-aborts-transaction` | [Legacy](unit-tests/sql-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |

### transaction

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-postgres-special-xid-statuses` | [Legacy](unit-tests/transaction-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-xid-control-state` | [Legacy](unit-tests/transaction-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-session-setting-rollback` | [Legacy](unit-tests/transaction-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-transaction-snapshot-lifecycle` | [Legacy](unit-tests/transaction-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-wal-enabled-repeatable-read-commit-releases-unpinned-snapshot` | [Legacy](unit-tests/transaction-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-transaction-isolation-controls` | [Legacy](unit-tests/transaction-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-repeatable-read-write-conflict-horizon` | [Rove](rove/transaction-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-xid-wraparound-freezes-tuples` | [Legacy](unit-tests/transaction-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-xid-wraparound-fault-recovery` | [Legacy](unit-tests/transaction-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-xid-wraparound-coordinator-cancel` | [Legacy](unit-tests/transaction-tests.lisp) | T: threads/interrupts at shared registry, snapshot or locking boundaries |

### catalog

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-key-order` | [Both](rove/catalog-tests.lisp) | integer order + byte-comparator Function Spec |
| `test-index-catalog-roundtrip` | [Rove](rove/catalog-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-with-table-ddl-evaluates-lock-inputs-once` | [Legacy](unit-tests/catalog-tests.lisp) | M: relation/B-tree lock maps and ownership require isolation |
| `test-catalog-mutation-rollback-macro` | [Legacy](unit-tests/catalog-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-table-statistics-catalog-roundtrip` | [Rove](rove/catalog-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-analyze-column-statistics-and-costing` | [Legacy](unit-tests/catalog-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-analyze-byte-vector-statistics-and-planner` | [Rove](rove/catalog-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-analyze-byte-vector-hash-collisions-and-text-equality` | [Rove](rove/catalog-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-analyze-value-estimate-counts-backing-storage` | [Rove](rove/catalog-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-analyze-byte-vector-sample-retention` | [Legacy](unit-tests/catalog-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-analyze-byte-vector-reservoir-shrink-and-oversize` | [Legacy](unit-tests/catalog-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-analyze-reservoir-shrink-clears-evicted-values` | [Rove](rove/catalog-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-analyze-reservoir-sample-is-bounded` | [Legacy](unit-tests/catalog-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-analyze-publish-restores-on-save-failure` | [Legacy](unit-tests/catalog-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-analyze-publish-keeps-durable-stats-on-late-failure` | [Legacy](unit-tests/catalog-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-nested-loop-seq-cost-models-replayable-inner` | [Legacy](unit-tests/catalog-tests.lisp) | D: costing calls buf-nblocks and can open/cache relation files |

### protocol

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-swank-arguments` | [Rove](rove/protocol-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-replication-slot-retention-option-parsing` | [Rove](rove/protocol-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-ddl-response-deferral` | [Legacy](unit-tests/protocol-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-protocol-packet-limits` | [Legacy](unit-tests/protocol-tests.lisp) | F: binary packet fixture files under the shared PGDATA |
| `test-protocol-decoding-enters-live-operation` | [Legacy](unit-tests/protocol-tests.lisp) | T/M: real threads and shared admission/drain state |
| `test-malformed-protocol-frames-drain-to-fatal-error` | [Legacy](unit-tests/protocol-tests.lisp) | T/M: real threads and shared admission/drain state |
| `test-terminate-frame-drains-active-backend` | [Legacy](unit-tests/protocol-tests.lisp) | T/M: real threads and shared admission/drain state |

### logical

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-logical-row-change-codec` | [Both](rove/logical-tests.lisp) | logical row image round trips |
| `test-logical-pgoutput-v2-codec` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-keepalive-does-not-advance-progress` | [Legacy](unit-tests/logical-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-logical-catalog-event-codec-and-parser` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-catalog-terminal-validation` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-replication-bootstrap-definitions` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-bootstrap-private-index-projection` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-bootstrap-constraint-order-stable` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-schema-contract-strictness` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-private-metadata-ddl-targets` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-schema-contract-column-shape-drift` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-slot-file-failpoint-atomicity` | [Legacy](unit-tests/logical-tests.lisp) | F: slot/failpoint files and guard-file lifecycle |
| `test-logical-slot-owner-publication-failure-cleans-owner` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-retention-reload-preserves-checkpoint-delegate` | [Legacy](unit-tests/logical-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-logical-test-failpoint-lsn-threshold` | [Legacy](unit-tests/logical-tests.lisp) | F: slot/failpoint files and guard-file lifecycle |
| `test-logical-scan-budget-override` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-wal-logical-catalog-event-slot-gating` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-catalog-event-size-boundaries` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-snapshot-capture-to-slot-publication-interest` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-token-publication-failure-preserves-other-schema-lease` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-invalidated-backend-cleanup-is-graceful` | [Legacy](unit-tests/logical-tests.lisp) | T: concurrent actors and shared snapshot/WAL ownership |
| `test-logical-importer-termination-retry-is-per-backend` | [Legacy](unit-tests/logical-tests.lisp) | T: concurrent actors and shared snapshot/WAL ownership |
| `test-create-sequence-parser-bounds` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-change-chunk-validation` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-subscription-argument-parser` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-subscription-resume-config` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-capture-application-headroom` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-truncate-capture-headroom` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-wal-reader-commit-order-and-slot-ack` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-safe-progress-reaches-ddl-after-scan-budget` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-copy-chunk-limit-override` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-copy-chunk-byte-boundaries` | [Legacy](unit-tests/logical-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-logical-reader-transaction-count-boundaries` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-reader-change-byte-boundaries` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-reader-scan-byte-boundaries` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-wal-recovery-byte-boundaries` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-logical-wal-apply-headroom-recovery` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-snapshot-handoff-active-xid` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-export-cleanup-releases-after-claim-error` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-export-cleanup-retries-wal-pin-release` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-unpinned-snapshot-release-preserves-wal-pin-map` | [Rove](rove/logical-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-unpinned-snapshot-release-ignores-stale-reuse-pin` | [Legacy](unit-tests/logical-tests.lisp) | T: concurrent actors and shared snapshot/WAL ownership |
| `test-logical-snapshot-import-locks-token-through-pin` | [Legacy](unit-tests/logical-tests.lisp) | T: concurrent actors and shared snapshot/WAL ownership |
| `test-logical-lost-slot-cleans-importers-by-generation` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-drop-adopts-snapshot-before-cleanup` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-maintenance-rediscovers-lost-slot-owners` | [Legacy](unit-tests/logical-tests.lisp) | D/M: WAL fixture, durable slots, snapshot publication or injected cleanup failure |
| `test-logical-owner-termination-revalidates-captured-slot` | [Legacy](unit-tests/logical-tests.lisp) | T: concurrent actors and shared snapshot/WAL ownership |
| `test-wal-pgslot3-database-field-validation` | [Legacy](unit-tests/logical-tests.lisp) | F: slot/failpoint files and guard-file lifecycle |

### vacuum

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-vacuum-page-publication-is-atomic` | [Legacy](unit-tests/vacuum-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-revalidated-relation-locks-retry-and-release` | [Rove](rove/vacuum-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-revalidated-relation-locks-release-after-revalidation-error` | [Rove](rove/vacuum-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-vacuum-index-object-set-invariant` | [Rove](rove/vacuum-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-vacuum-current-table-index-set-lock-guard` | [Legacy](unit-tests/vacuum-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-vacuum-xid-normalization-invariant` | [Legacy](unit-tests/vacuum-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-vacuum-freeze-cutoff-invariant` | [Both](rove/vacuum-tests.lisp) | freeze policy and horizon |
| `test-vacuum-full-publish-failure-boundaries` | [Legacy](unit-tests/vacuum-tests.lisp) | D: catalog/WAL persistence or transaction/XID lifecycle |
| `test-vacuum-page-retry-rebuilds-proposal` | [Legacy](unit-tests/vacuum-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-vacuum-page-read-releases-global-write-lock` | [Legacy](unit-tests/vacuum-tests.lisp) | T: threads/interrupts at shared registry, snapshot or locking boundaries |
| `test-vacuum-reuses-tid-clears-index-and-vm` | [Legacy](unit-tests/vacuum-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-vacuum-snapshot-horizon` | [Legacy](unit-tests/vacuum-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-btree-relation-lock-macro` | [Legacy](unit-tests/vacuum-tests.lisp) | M: relation/B-tree lock maps and ownership require isolation |
| `test-btree-relation-lock-set-macro` | [Legacy](unit-tests/vacuum-tests.lisp) | M: relation/B-tree lock maps and ownership require isolation |
| `test-btree-writer-interrupt-cleans-waiter` | [Legacy](unit-tests/vacuum-tests.lisp) | T: threads/interrupts at shared registry, snapshot or locking boundaries |
| `test-btree-read-lock-set-macro` | [Legacy](unit-tests/vacuum-tests.lisp) | M: relation/B-tree lock maps and ownership require isolation |
| `test-btree-duplicate-keys` | [Legacy](unit-tests/vacuum-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-btree-mixed-duplicate-keys` | [Legacy](unit-tests/vacuum-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-btree-vacuum-bounded-split-retries` | [Legacy](unit-tests/vacuum-tests.lisp) | D: relation files, stream/FSM caches or CLOG |

### join

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-streamed-join-stop-and-outer-matches` | [Legacy](unit-tests/join-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-lazy-join-skips-inner-for-empty-outer` | [Rove](rove/join-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-lazy-join-interpreter-stops-inner-scan` | [Legacy](unit-tests/join-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-streamed-join-on-matches-survive-where-and-stop` | [Legacy](unit-tests/join-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-select-row-qualification-before-projection` | [Legacy](unit-tests/join-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |

### arena

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-sort-arena-option-parsing` | [Legacy](unit-tests/arena-tests.lisp) | H: global make-query-sort-arena replacement; option and disabled-runtime behavior (unguarded) |
| `test-sort-arena-output-survives-release` | [Legacy](unit-tests/arena-tests.lisp) | A: runtime arena/GC ownership, hooks, spill files or threads; retain support guard |
| `test-sort-arena-is-only-scalar-root` | [Legacy](unit-tests/arena-tests.lisp) | A: runtime arena/GC ownership, hooks, spill files or threads; retain support guard |
| `test-sort-arena-exhaustion-cleans-spills` | [Legacy](unit-tests/arena-tests.lisp) | A: runtime arena/GC ownership, hooks, spill files or threads; retain support guard |
| `test-sort-arena-errors-own-diagnostics` | [Legacy](unit-tests/arena-tests.lisp) | A: runtime arena/GC ownership, hooks, spill files or threads; retain support guard |
| `test-sort-arena-concurrent-capacities` | [Legacy](unit-tests/arena-tests.lisp) | A: runtime arena/GC ownership, hooks, spill files or threads; retain support guard |
| `test-sort-arena-interrupt-after-switch` | [Legacy](unit-tests/arena-tests.lisp) | A: runtime arena/GC ownership, hooks, spill files or threads; retain support guard |

### ownership

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-relation-scope-publication-and-return-interrupts` | [Legacy](unit-tests/ownership-tests.lisp) | M/H/F: ownership/admission maps, replaced functions, interrupts or temp-file quotas |
| `test-relation-scope-waits-and-cleanup` | [Legacy](unit-tests/ownership-tests.lisp) | M/H/F: ownership/admission maps, replaced functions, interrupts or temp-file quotas |
| `test-relation-scope-empty-and-unscoped-failure` | [Legacy](unit-tests/ownership-tests.lisp) | M/H/F: ownership/admission maps, replaced functions, interrupts or temp-file quotas |
| `test-backend-query-scope-controls-and-values` | [Legacy](unit-tests/ownership-tests.lisp) | M/H/F: ownership/admission maps, replaced functions, interrupts or temp-file quotas |
| `test-backend-query-scope-captures-owner` | [Legacy](unit-tests/ownership-tests.lisp) | M/H/F: ownership/admission maps, replaced functions, interrupts or temp-file quotas |
| `test-backend-query-scope-cleanup-lanes` | [Legacy](unit-tests/ownership-tests.lisp) | M/H/F: ownership/admission maps, replaced functions, interrupts or temp-file quotas |
| `test-query-temp-output-acquisition-interrupts` | [Legacy](unit-tests/ownership-tests.lisp) | M/H/F: ownership/admission maps, replaced functions, interrupts or temp-file quotas |
| `test-query-temp-output-values-and-quota-error` | [Legacy](unit-tests/ownership-tests.lisp) | M/H/F: ownership/admission maps, replaced functions, interrupts or temp-file quotas |

### backend-lifecycle

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-unregister-backend-clears-query-state` | [Rove](rove/backend-lifecycle-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-idle-backend-teardown-enters-captured-cleanup` | [Legacy](unit-tests/backend-lifecycle-tests.lisp) | T/M: backend admission, reload/shutdown actors and protocol hooks |
| `test-shutdown-cleanup-respects-reload-admission` | [Legacy](unit-tests/backend-lifecycle-tests.lisp) | T/M: backend admission, reload/shutdown actors and protocol hooks |
| `test-pre-admitted-idle-backend-can-drain-after-starting-transaction` | [Legacy](unit-tests/backend-lifecycle-tests.lisp) | T/M: backend admission, reload/shutdown actors and protocol hooks |
| `test-active-transaction-suffix-waits-for-reload` | [Legacy](unit-tests/backend-lifecycle-tests.lisp) | T/M: backend admission, reload/shutdown actors and protocol hooks |
| `test-disconnect-cleanup-drains-active-transaction` | [Legacy](unit-tests/backend-lifecycle-tests.lisp) | T/M: backend admission, reload/shutdown actors and protocol hooks |
| `test-idle-commit-waits-for-reload` | [Legacy](unit-tests/backend-lifecycle-tests.lisp) | T/M: backend admission, reload/shutdown actors and protocol hooks |
| `test-idle-active-transaction-does-not-block-reload` | [Legacy](unit-tests/backend-lifecycle-tests.lisp) | T/M: backend admission, reload/shutdown actors and protocol hooks |

### concurrency

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-exported-snapshot-schema-lock-coordination` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-schema-gate-allows-independent-writers` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-schema-gate-queued-writers-enter-together` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-schema-lock-nesting-and-read-upgrade-rejection` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-schema-lock-acquisition-interruption` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-atomic-relation-reader-registration` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-single-relation-lock-macros-share-set-guards` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-select-index-reader-blocks-drop-after-registration` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-live-operation-gate` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-live-operation-quiescence-ownership` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-nested-control-repl-reload` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-legacy-live-operation-gate-call-shapes` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-live-operation-entry-interrupt-cleanup` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-transaction-ending-statement-classifier` | [Rove](rove/concurrency-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-simple-query-terminal-statement-drain` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-cancel-query-waiting-for-live-operation-admission` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-late-cancel-is-cleared-at-query-end` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |
| `test-query-token-cleans-up-after-publication-error` | [Legacy](unit-tests/concurrency-tests.lisp) | T/M: shared admission/schema/relation state, hooks, threads or interruption |

### snapshot

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-visibility` | [Legacy](unit-tests/snapshot-tests.lisp) | D: relation files, stream/FSM caches or CLOG |
| `test-statement-snapshot-acquisition-interrupt-cleanup` | [Legacy](unit-tests/snapshot-tests.lisp) | T: threads/interrupts at shared registry, snapshot or locking boundaries |
| `test-make-snapshot-rejects-unowned-registration` | [Rove](rove/snapshot-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-register-active-snapshot-requires-token` | [Rove](rove/snapshot-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-snapshot-registration-cannot-be-reused-while-active` | [Rove](rove/snapshot-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |

### spill

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-query-temp-startup-removes-abandoned-namespaces` | [Legacy](unit-tests/spill-tests.lisp) | F: temporary files, ownership, cleanup or quota fixture |
| `test-query-temp-fd-scope-closes-after-error` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-query-temp-fd-scope-closes-after-acquisition-interrupt` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-query-temp-fd-scope-closes-before-cleanup-interrupt` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-query-temp-fd-scope-macro-expansion-protects-cleanup` | [Rove](rove/spill-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-query-temp-retained-namespace-observability` | [Legacy](unit-tests/spill-tests.lisp) | F: temporary files, ownership, cleanup or quota fixture |
| `test-query-temp-retained-scan-refuses-symlink-swap` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-query-temp-retained-scan-reports-file-type-race` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-query-temp-file-limit-is-enforced` | [Legacy](unit-tests/spill-tests.lisp) | F: temporary files, ownership, cleanup or quota fixture |
| `test-query-spill-macros-use-default-memory-limit` | [Rove](rove/spill-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-query-spill-resource-macros-clean-up-after-error` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-query-temp-file-limit-option-parsing` | [Rove](rove/spill-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-query-temp-file-macro-cleans-up-after-error` | [Legacy](unit-tests/spill-tests.lisp) | F: temporary files, ownership, cleanup or quota fixture |
| `test-query-temp-open-registration-failure-cleans-up` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-query-temp-delete-failure-retains-cleanup-registration` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-full-join-spool-spills-typed-bytea-and-match-state` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-query-row-spool-spills-bytea-without-expanded-list` | [Legacy](unit-tests/spill-tests.lisp) | F: temporary files, ownership, cleanup or quota fixture |
| `test-query-plan-external-sort-spills-stably` | [Legacy](unit-tests/spill-tests.lisp) | H/F: global hooks, filesystem races or interruption during resource cleanup |
| `test-hash-aggregate-partitioned-spill` | [Legacy](unit-tests/spill-tests.lisp) | F: temporary files, ownership, cleanup or quota fixture |
| `test-hash-aggregate-sort-fallback-and-scalar-empty-input` | [Legacy](unit-tests/spill-tests.lisp) | F: temporary files, ownership, cleanup or quota fixture |
| `test-hash-aggregate-skew-does-not-spill-unnecessarily` | [Rove](rove/spill-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |

### wal

| Original case | Disposition and location | Property or deferral reason |
| --- | --- | --- |
| `test-wal-catalog-codec-and-page-checksums` | [Rove](rove/wal-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-wal-record-codec` | [Both](rove/wal-tests.lisp) | raw WAL record envelope |
| `test-wal-v3-maintenance-record-replay` | [Legacy](unit-tests/wal-tests.lisp) | M: replay resets recovery state beyond the currently bound fields |
| `test-wal-page-image-xid-codec-compatibility` | [Rove](rove/wal-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-wal-snapshot-boundary-retains-active-xid-pages` | [Legacy](unit-tests/wal-tests.lisp) | M: snapshot/XID release reaches global relation-ownership cleanup |
| `test-wal-snapshot-release-race-before-pin-publication` | [Legacy](unit-tests/wal-tests.lisp) | T: concurrent actors and shared snapshot/WAL ownership |
| `test-wal-snapshot-release-race-before-reuse` | [Legacy](unit-tests/wal-tests.lisp) | T: concurrent actors and shared snapshot/WAL ownership |
| `test-wal-chunked-catalog-record` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-catalog-terminal-validation-precedes-page-redo` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-segment-rotation` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-checkpoint-retains-active-xid-segments` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-replication-slot-durable-retention` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-replication-slot-acknowledgment-progress` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-replication-slot-retention-cap-invalidates-pin` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-with-exported-snapshot-scope` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-exported-snapshot-wraparound-admission` | [Legacy](unit-tests/wal-tests.lisp) | H: global scan/executor/compiler hook or publication-failure injection |
| `test-exported-snapshot-wraparound-race` | [Legacy](unit-tests/wal-tests.lisp) | T: concurrent actors and shared snapshot/WAL ownership |
| `test-wal-replication-slot-fsync-failure-keeps-pin` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-drop-missing-slot-does-not-poison-wal` | [Rove](rove/wal-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-with-wal-critical-validation-errors-do-not-poison` | [Rove](rove/wal-tests.lisp) | Fixed examples; retain specific results/errors/evaluation order |
| `test-wal-drop-fsync-retry-keeps-pin` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-replication-slot-load-rejects-missing-wal` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-checkpoint-skips-obsolete-prefix` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |
| `test-wal-missing-interior-segment` | [Legacy](unit-tests/wal-tests.lisp) | D: WAL/slot files, durability, recovery or restart fixture |

## Helper and runner inventory

These 49 original functions are helpers, not independent test cases. Seven moved
to shared support. The legacy `check` macro and its counters stay in
`unit-tests/support.lisp`; converted tests use `rove:ok` directly. The four
`run-*-unit-tests` grouping functions, `run-unit-tests`, the module loader and
the inline wait assertion remain in [unit-tests.lisp](unit-tests.lisp).

| Helper | Location after migration |
| --- | --- |
| `call-test-relation-scope` | [Legacy](unit-tests/ownership-tests.lisp) |
| `call-with-vacuum-full-publish-test-fixture` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `call-with-wal-snapshot-release-race-state` | [Legacy](unit-tests/wal-tests.lisp) |
| `check-vacuum-full-publish-durable-state` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `check-vacuum-full-publish-fault-result` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `install-vacuum-full-publish-fault` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `logical-test-abort-insert` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-append-batch` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-append-catalog-event` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-assert-owned-reader-progress` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-capture-row-change` | [Shared](rove/support.lisp) |
| `logical-test-capture-truncate-event` | [Shared](rove/support.lisp) |
| `logical-test-change` | [Shared](rove/support.lisp) |
| `logical-test-chunk-record` | [Shared](rove/support.lisp) |
| `logical-test-columns` | [Shared](rove/support.lisp) |
| `logical-test-commit-boundary-batch` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-commit-insert` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-large-change` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-large-change-of-size` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-reader-scan-boundary` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-sized-catalog-event` | [Legacy](unit-tests/logical-tests.lisp) |
| `logical-test-with-wal-storage` | [Legacy](unit-tests/logical-tests.lisp) |
| `make-query-spill-test-directory` | [Legacy](unit-tests/spill-tests.lisp) |
| `query-spill-test-directory-empty-p` | [Legacy](unit-tests/spill-tests.lisp) |
| `run-vacuum-full-publish-fault-test` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `sort-arena-test-stream` | [Legacy](unit-tests/arena-tests.lisp) |
| `sort-arena-test-supported-p` | [Legacy](unit-tests/arena-tests.lisp) |
| `test-analyze-catalog-class-image-roundtrip` | [Shared](rove/support.lisp) |
| `test-analyze-retained-byte-vector-payload` | [Legacy](unit-tests/catalog-tests.lisp) |
| `test-concat-octets` | [Legacy](unit-tests/protocol-tests.lisp) |
| `test-data-row-first-column` | [Legacy](unit-tests/transaction-tests.lisp) |
| `test-dispatch-query-frame` | [Legacy](unit-tests/support.lisp) |
| `test-malformed-packet-p` | [Legacy](unit-tests/protocol-tests.lisp) |
| `test-prepare-wraparound-fixture` | [Legacy](unit-tests/transaction-tests.lisp) |
| `test-query-message` | [Shared](rove/support.lisp) |
| `test-read-frontend-message` | [Legacy](unit-tests/support.lisp) |
| `test-relation-scope-clear-p` | [Legacy](unit-tests/ownership-tests.lisp) |
| `test-reset-live-operation-admission` | [Legacy](unit-tests/support.lisp) |
| `test-restart-wraparound-fixture` | [Legacy](unit-tests/transaction-tests.lisp) |
| `test-show-timezone-values` | [Legacy](unit-tests/transaction-tests.lisp) |
| `test-startup-packet-size` | [Legacy](unit-tests/protocol-tests.lisp) |
| `test-wait-until` | [Legacy](unit-tests/support.lisp) |
| `vacuum-full-publish-fault-message` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `vacuum-full-test-read-file` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `vacuum-full-test-recover-appended-catalog-record` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `vacuum-full-test-recover-unappended-catalog-record` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `vacuum-full-test-write-relation-marker` | [Legacy](unit-tests/vacuum-tests.lisp) |
| `wal-test-append-committed-xid-batch` | [Legacy](unit-tests/wal-tests.lisp) |
| `wal-test-encode-versioned-record` | [Legacy](unit-tests/wal-tests.lisp) |

## Process tests and tooling retained

These are runnable harnesses rather than legacy unit assertions. Their process,
wire, crash/restart, signal or OS behavior needs an external oracle/lifecycle.
Keep their entry points; moving assertions into Rove would not replace the
process-level coverage. Prerequisites (C peers, Patroni, arena-enabled SBCL,
cgroups, ports) still apply; listing a harness here does not claim it was run.

| File | Disposition / coverage |
| --- | --- |
| [`compile.sh`](compile.sh) | Tool: strict fresh compilation |
| [`control-repl.sh`](control-repl.sh) | Retain process test: the out-of-band control REPL over TCP and Unix sockets. |
| [`format.sh`](format.sh) | Tool: Emacs/SLIME formatting |
| [`hot-reload.sh`](hot-reload.sh) | Retain process test: SIGHUP recompiles and reloads code in the running server. |
| [`index-build-contention.sh`](index-build-contention.sh) | Retain process test: relation-gated bulk build and atomic publish. |
| [`index-lock-contention.sh`](index-lock-contention.sh) | Retain process test: measure unrelated INSERTs during B-tree search. |
| [`index-scan-regression.sh`](index-scan-regression.sh) | Retain process test: differential results and reader/DDL handoff. |
| [`index-stress.sh`](index-stress.sh) | Retain process test: repeated updates to one unique value. |
| [`join-ddl-concurrency.sh`](join-ddl-concurrency.sh) | Retain process test: atomic JOIN admission against concurrent DROP TABLE. |
| [`join-streaming.sh`](join-streaming.sh) | Retain process test: outer-join semantics and bounded LIMIT execution. |
| [`logical-catalog-durability-faults.sh`](logical-catalog-durability-faults.sh) | Retain process test: DDL remains published when an injected error arrives after its WAL terminal. |
| [`logical-interior-ack.sh`](logical-interior-ack.sh) | Retain process test: Real publisher crash/restart after a flush inside its commit record. |
| [`logical-interop.sh`](logical-interop.sh) | Retain process test: Native C PostgreSQL peer acceptance. Missing prerequisites are failures. |
| [`logical-mixed-mode.sh`](logical-mixed-mode.sh) | Retain process test: A standard C-source subscription must survive extension bootstrap and DDL. |
| [`logical-private-metadata-ddl.sh`](logical-private-metadata-ddl.sh) | Retain process test: Replicated DDL must not alter subscriber-owned progress or config tables. |
| [`logical-replication-faults.sh`](logical-replication-faults.sh) | Retain process test: Crash/restart checks for logical-copy handoff state transitions. |
| [`logical-replication.sh`](logical-replication.sh) | Retain process test: Three-server logical replication regression: one source and two subscribers. |
| [`logical-slot-backpressure.sh`](logical-slot-backpressure.sh) | Retain process test: Exercise slot loss, importer cleanup, and reseeding with a stalled consumer. |
| [`logical-standalone-event-faults.sh`](logical-standalone-event-faults.sh) | Retain process test: Crash windows around durable standalone DDL/sequence apply and feedback. |
| [`logical-switchover.sh`](logical-switchover.sh) | Retain process test: Isolated, operator-ordered logical writer switchover rehearsal; no coordinator. |
| [`patroni-start.sh`](patroni-start.sh) | Retain process test: Pinned Patroni direct launch/config smoke; no HA loop or DCS is started. |
| [`physical-bootstrap.sh`](physical-bootstrap.sh) | Retain process test: A quiesced base image starts in recovery; it is never promoted by this test. |
| [`physical-control-c.sh`](physical-control-c.sh) | Retain process test: Pinned C-to-C state oracle; never feeds C physical WAL to pg-lisp. |
| [`physical-exit-flush.sh`](physical-exit-flush.sh) | Retain process test: Complete native WAL records survive receiver disconnect/stop below batch size. |
| [`physical-follow.sh`](physical-follow.sh) | Retain process test: Continuous native physical WAL over TCP, with independently delayed replay. |
| [`physical-replay.sh`](physical-replay.sh) | Retain process test: Actual standby redo using manually transported native frames, not network streaming. |
| [`physical-restartpoint-auto.sh`](physical-restartpoint-auto.sh) | Retain process test: Native standby maintenance without a follower, then with fresh-process recovery. |
| [`physical-retention.sh`](physical-retention.sh) | Retain process test: Flush advances source retention even while standby replay is delayed. |
| [`physical-shutdown-prepare.sh`](physical-shutdown-prepare.sh) | Retain process test: Explicit standby preparation, also used by automatic smart/fast shutdown. |
| [`physical-shutdown-signals.sh`](physical-shutdown-signals.sh) | Retain process test: Native recovery shutdown from actual process signals, without coordination. |
| [`physical-source.sh`](physical-source.sh) | Retain process test: Native source pin survives checkpoints and SIGKILL while SQL remains correct. |
| [`physical-standby.sh`](physical-standby.sh) | Retain process test: Actual native static standby: role/progress, immutable data and SQL write exclusion. |
| [`postmaster-pid.sh`](postmaster-pid.sh) | Retain process test: Exclusive local PGDATA ownership plus real Patroni process/driver smoke tests. |
| [`psql-smoke.sh`](psql-smoke.sh) | Retain process test: acceptance test for the Lisp PostgreSQL server. |
| [`publication-oids.sh`](publication-oids.sh) | Retain process test: Legacy publication identities are persisted before SQL admission and recovery. |
| [`run-unit-tests.sh`](run-unit-tests.sh) | Runner: migrated + deferred unit cases |
| [`shutdown.sh`](shutdown.sh) | Retain process test: PostgreSQL-compatible shutdown and backend signaling. |
| [`sort-arena.sh`](sort-arena.sh) | Retain process test: Verify SQL recovery from actual SBCL sort-arena capacity exhaustion. |
| [`spill-crash-recovery.sh`](spill-crash-recovery.sh) | Retain process test: reclaim abandoned executor spill files on startup. |
| [`spill-oom-cgroup.sh`](spill-oom-cgroup.sh) | Retain process test: verify spill recovery after an isolated server OOM. |
| [`swank.sh`](swank.sh) | Retain process test: Swank is preloaded, opt-in, and bound to the requested address. |
| [`test-support.sh`](test-support.sh) | Shared process-fixture helpers |
| [`unique-index-concurrency.sh`](unique-index-concurrency.sh) | Retain process test: unique-index builds wait for table writers. |
| [`update-concurrency.sh`](update-concurrency.sh) | Retain process test: READ COMMITTED rechecks and REPEATABLE READ conflicts. |
| [`vacuum-full-regression.sh`](vacuum-full-regression.sh) | Retain process test: VACUUM FULL rewrite, indexes and recovery. |
| [`vacuum-update-concurrency.sh`](vacuum-update-concurrency.sh) | Retain process test: ordinary VACUUM with same-page writers. |
| [`wal-crash-recovery.sh`](wal-crash-recovery.sh) | Retain process test: Crash/restart across WAL batch, page staging, B-tree split and checkpoint boundaries. |

## Other Lisp files, data and existing suites

New [test-dependencies.lisp](test-dependencies.lisp) is bootstrap code rather
than a test case. It discovers installed test-only systems for the shell runner
without changing the server system's dependencies.

The following accounts for every original file directly under `t/` and
`t/interop/` not covered by the module/case inventory or shell table. Scripted
clients, injected hooks and crash children intentionally run in the process
that owns the relevant state; they are not independent unit cases.

| File | Role / disposition |
| --- | --- |
| [`physical-tests.lisp`](physical-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-store-tests.lisp`](physical-store-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-pid-tests.lisp`](physical-pid-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-config-tests.lisp`](physical-config-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`coalesce-tests.lisp`](coalesce-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-source-tests.lisp`](physical-source-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-bootstrap-tests.lisp`](physical-bootstrap-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-recovery-tests.lisp`](physical-recovery-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-control-tests.lisp`](physical-control-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`checkpoint-tests.lisp`](checkpoint-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-history-tests.lisp`](physical-history-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`timeline-tests.lisp`](timeline-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-restartpoint-tests.lisp`](physical-restartpoint-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-stream-tests.lisp`](physical-stream-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-follow-exit-tests.lisp`](physical-follow-exit-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`physical-shutdown-tests.lisp`](physical-shutdown-tests.lisp) | Already Rove/cl-spec in `pg-physical-tests`; retain. |
| [`cancel-request-client.lisp`](cancel-request-client.lisp) | Retain process helper: Wire client used by shutdown.sh; real CancelRequest and backend I/O. |
| [`control-client.lisp`](control-client.lisp) | Retain process helper: Task-owned control-REPL transport used by process harnesses. |
| [`physical-bootstrap-native.lisp`](physical-bootstrap-native.lisp) | Retain process helper: Pause/crash hooks installed in the bootstrap server image. |
| [`physical-bootstrap-rows.lisp`](physical-bootstrap-rows.lisp) | Retain process helper: Extracts bootstrap rows through a live/native fixture. |
| [`physical-bootstrap-verify.lisp`](physical-bootstrap-verify.lisp) | Retain process helper: Standalone verification of a recovered bootstrap image. |
| [`physical-control-startup-failure.lisp`](physical-control-startup-failure.lisp) | Retain process helper: Startup-failure injection child for shutdown-signals coverage. |
| [`physical-replay-native.lisp`](physical-replay-native.lisp) | Retain process helper: WAL export/receive and redo-failure hooks installed in native peers. |
| [`physical-store-crash-child.lisp`](physical-store-crash-child.lisp) | Retain process helper: Intentional process-crash child of the physical store Rove suite. |
| [`stalled-startup-client.lisp`](stalled-startup-client.lisp) | Retain process helper: Socket client holding startup open for process shutdown tests. |
| [`format.el`](format.el) | Formatter implementation; retain tool. |
| [`psql-smoke-persist.sql`](psql-smoke-persist.sql) | psql smoke input/expected-output fixture; retain independent SQL oracle. |
| [`psql-smoke.out`](psql-smoke.out) | psql smoke input/expected-output fixture; retain independent SQL oracle. |
| [`psql-smoke.sql`](psql-smoke.sql) | psql smoke input/expected-output fixture; retain independent SQL oracle. |
| [`interop/20devel-reference.hex`](interop/20devel-reference.hex) | Independent C wire bytes; retain. |
| [`interop/README.md`](interop/README.md) | Existing interop inventory, commands and verification history. |
| [`interop/apply-tests.lisp`](interop/apply-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/bit-sql.py`](interop/bit-sql.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/bit-tests.lisp`](interop/bit-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/boolean-sql.py`](interop/boolean-sql.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/boolean-tests.lisp`](interop/boolean-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/capture-reference.sql`](interop/capture-reference.sql) | SQL used to capture independent C fixtures; retain. |
| [`interop/copy-tests.lisp`](interop/copy-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/interior-ack-client.lisp`](interop/interior-ack-client.lisp) | Scripted wire peer for interior-ACK crash/restart coverage; retain. |
| [`interop/native-failpoints.lisp`](interop/native-failpoints.lisp) | Hooks installed in the native interoperability publisher; retain. |
| [`interop/native-stream-tests.lisp`](interop/native-stream-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/patroni-history.py`](interop/patroni-history.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/patroni-process.py`](interop/patroni-process.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/patroni-requirements.txt`](interop/patroni-requirements.txt) | Pinned external Patroni dependencies; retain. |
| [`interop/patroni-start.py`](interop/patroni-start.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/patroni-tl-lsn.py`](interop/patroni-tl-lsn.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/postmaster-time-sql.py`](interop/postmaster-time-sql.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/postmaster-time-tests.lisp`](interop/postmaster-time-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/property-tests.lisp`](interop/property-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/protocol-tests.lisp`](interop/protocol-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/publication-history-tests.lisp`](interop/publication-history-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/receiver-sql.py`](interop/receiver-sql.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/receiver-tests.lisp`](interop/receiver-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/recovery-control-state.py`](interop/recovery-control-state.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/standard-subscriber-tests.lisp`](interop/standard-subscriber-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/standard-tests.lisp`](interop/standard-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/standard-wire-tests.lisp`](interop/standard-wire-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/string-sql.py`](interop/string-sql.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/string-tests.lisp`](interop/string-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/sum-tests.lisp`](interop/sum-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/wal-filename-sql.py`](interop/wal-filename-sql.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |
| [`interop/wal-filename-tests.lisp`](interop/wal-filename-tests.lisp) | Already Rove/cl-spec in the primary/apply/native interop ASDF systems; retain. |
| [`interop/wal-sql.py`](interop/wal-sql.py) | External SQL/C-peer or Patroni process oracle; retain its harness. |

## Review and verification

Sol 6.1/high reviewed all 17 original legacy modules and then checked assertion,
helper and runner preservation. Astra/medium advised on fixture boundaries and
generated-domain/oracle limits. All 68 moved bodies retain their assertions;
shared helpers are preserved and deferred runner order is unchanged.


Pre-rebase migration results (2026-10-26, saved source on base `af553010d76` plus this migration):

- Legacy baseline before conversion: **1,598 checks, zero failures**.
- `pg-unit-tests`: **78 passed, zero failed**, including all generated checks.
  The documented individual selection `pg::test-tuple-roundtrip` is independently runnable.
- Fresh `./t/run-unit-tests.sh` process: **exit 0**, new ASDF suite passed and
  **1,131 deferred checks, zero failures**. On this host cl-spec is installed in
  a different Quicklisp tree from plain SBCL's active one, so the run registered
  that checkout through standard `CL_SOURCE_REGISTRY`, as documented above.
  No global Quicklisp/ASDF or user init configuration was changed.
- Existing primary suites: **169 interop** and **165 physical** tests passed.
- Fresh strict `./t/compile.sh`, strict compilation of `pg-unit-tests`, formatter
  checks for all new/changed Lisp, shell syntax and `git diff --check` passed.
  Dependency loading emits the existing external check-it ASDF naming/deprecation
  warnings; the project sources compile with compiler warnings treated as errors.
- Direct cl-spec: **18 passing runs, 200 executed trials each** (3,600 total),
  one seed-42 and one fresh-seed run for each declaration below. Source was
  reloaded after clearing the primary system's cached FASLs before acceptance.

| Declaration (package PG) | Kind | Seed-42 digest | Additional seed |
| --- | --- | --- | --- |
| `key-compare` | function | `fnv1a64-v1:26499e4f5e869d81` | `4083318777664617531` |
| `unit-int4-index-order` | property | `fnv1a64-v1:b8f2f07eb323dff9` | `659705953632097041` |
| `unit-int8-index-order` | property | `fnv1a64-v1:2bfc2ef617d7cc6e` | `3244310399318937850` |
| `unit-tuples-preserve-values-and-header` | property | `fnv1a64-v1:db0ecdc51b22280e` | `4090713659768179624` |
| `unit-wal-records-preserve-fields` | property | `fnv1a64-v1:f74553c4d2e2f527` | `4122588990659590974` |
| `unit-page-compaction-preserves-live-items` | property | `fnv1a64-v1:5c6bd9c5eb21169f` | `2817208883017020917` |
| `unit-freeze-cutoffs-respect-policy-and-horizon` | property | `fnv1a64-v1:94d8b6557c0014f3` | `1680952277505416654` |
| `unit-logical-row-images-roundtrip` | property | `fnv1a64-v1:e22913d72b023698` | `1980981386493831707` |
| `unit-top-n-matches-stable-sort` | property | `fnv1a64-v1:18e65c5d69c78986` | `3537075612637790890` |

Tool-reported verification gaps are preserved: Function Spec calls report
`properties-not-run, input-coverage-unmeasured`. Each explicit Property call
reports `rejection-counts-unmeasured, input-coverage-unmeasured`. Separate calls
cover the comparator's Function Spec and its two Properties; no single call is
claimed to cover both. Declaration digests do not identify implementation bodies
or external process state. The retained shell/process harnesses and live C-peer
apply/native systems were inventoried, not rerun as part of this unit migration.

## HAVING and LATERAL coverage

The feature coverage in [having-lateral.sh](having-lateral.sh) and
[interop/having_lateral.py](interop/having_lateral.py) is retained as a process
and C differential test, not replaced by unit tests. It covers 118 C comparison
cases and spill, late-error, cancellation and recovery paths. The unit suite
includes [having-tests.lisp](rove/having-tests.lisp) and
[lateral-tests.lisp](rove/lateral-tests.lisp); see
[HAVING-LATERAL.md](HAVING-LATERAL.md) for scope and related suites.

## DML source, RETURNING and conflict coverage

The unit suite adds 15 Rove tests in [insert-select-tests.lisp](rove/insert-select-tests.lisp),
[returning-tests.lisp](rove/returning-tests.lisp) and
[on-conflict-tests.lisp](rove/on-conflict-tests.lisp). They cover parser and
analysis rules, assignment coercion, ComboCID visibility and cleanup, arbiter
inference and post-admission index refresh. That milestone brought the unit suite
to 141 tests.

[dml.sh](dml.sh) and its three Python modules under `interop/` remain process
tests: 102 C comparisons, 24 controlled concurrency scenarios, spill/quota,
buffered output, cancellation/rollback and restart persistence. They need real
transactions, catalog/index state and frontend protocol observation and cannot
be replaced by pure Rove assertions. The logical interoperability workload also
exercises INSERT SELECT, conflict update/skip and DELETE RETURNING in committed
and rolled-back transactions. See [DML.md](DML.md) for scope and references.

## UPDATE FROM and DELETE USING coverage

Four Rove tests in [joined-dml-tests.lisp](rove/joined-dml-tests.lisp) cover
parsing, target/source/OLD/NEW namespace slots, merged-column rebasing,
ambiguous names and forbidden target references. The unit suite now has
145 tests.

[joined-dml.sh](joined-dml.sh) and its three Python modules under `interop/`
remain process tests: 62 C SQL comparisons and 14 controlled concurrency
comparisons, plus bounded source replay spill, late-error/temporary-quota
rollback, cancellation, indexed self-sources and resource cleanup. They need
real tuple identities, concurrent sessions and frontend protocol observations.
The logical interoperability workload observes joined UPDATE and DELETE
separately on the subscriber. See [JOINED-DML.md](JOINED-DML.md) for scope.
The task-owned [joined-dml-native.lisp](interop/joined-dml-native.lisp) fixture
pauses after planning so the process test can drop a selected source index
before admission and verify execution-path reconciliation.

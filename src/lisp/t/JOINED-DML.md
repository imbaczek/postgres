# UPDATE FROM and DELETE USING

UPDATE accepts FROM after SET, and DELETE accepts USING after its target.
Sources use the existing SELECT source forms: tables, aliases, supported joins,
derived queries, CTEs and LATERAL references between source items. The mutation
target is unavailable inside those FROM/USING subqueries, including LATERAL.
WHERE, assignment expressions and RETURNING can reference target and sources.
An explicit target alias hides its original name; self-joins require a separate
source binding. Existing restrictions on mixed comma and explicit joins apply.

RETURNING supports source columns, qualified stars and scalar subqueries.
Bare * expands the current target and visible source columns, including merged
JOIN USING columns. OLD/NEW remain qualified-only target namespaces. Results
use the existing bounded RETURNING spool, so late errors publish no result rows.

Each physical target tuple is scanned separately from source values. A lazy,
spillable replay store owns copies of source environments under the original
statement snapshot. Source joins retain their own planner and iterator paths.
Target writes and every source/subquery read enter relation admission together.
B-tree scan leases are detached before SQL evaluation or target mutation.
Resumable index inputs buffer matching heap values under the index lease,
release leases, then yield values to SQL filters and projections. Parameterized
index inputs get a separate iterator and copied key environment per outer row.

At READ COMMITTED, a concurrent target change replaces only the target portion
of the candidate. WHERE and SET are reevaluated using that refreshed target and
the original matched source values. Concurrent source changes do not substitute
a new source version. REPEATABLE READ retains its serialization checks. This
matches C's nonlocking source rowmarks and EvalPlanQual behavior.

Multiple source matches modify a target at most once. Assignment expressions
still run for each originally qualifying match, as they do before C's
TM_SelfModified handling. A failed concurrent recheck leaves later source
candidates eligible. A per-target success flag provides bounded duplicate
tracking without confusing distinct target tuples that have identical values.

Immutable false gates, uncorrelated InitPlan predicates and supported STABLE
row-independent predicates run before fetching sources. Volatile or correlated
qualifications stay with candidate evaluation.

## References and tests

C references: parser/analyze.c transformUpdateStmt/transformDeleteStmt,
parser/parse_relation.c lateral checks, executor/nodeModifyTable.c self-modified
handling, executor/execMain.c EvalPlanQualFetchRowMark, and optimizer/util/clauses.c
is_pseudo_constant_clause. Relevant C regression sources include update.sql,
delete.sql, returning.sql and join.sql.

The Rove tests in joined-dml-tests.lisp cover parsing, namespace offsets,
merged columns, ambiguity and hidden-target errors. `./t/joined-dml.sh`
compares SQLSTATEs, metadata, rows and command tags with an owned C server.
Controlled concurrent sessions cover target changes, original source retention,
later candidates after failed rechecks, rollback and isolation conflicts.
Resource tests force source replay spilling, quota errors, cancellation and
late-error rollback, and check temporary-file cleanup and subsequent recovery.
The logical interoperability workload observes committed joined UPDATE and
DELETE phases independently on the subscriber.

Validation passes 145 unit, 170 interop and 714 physical tests, plus 1,131
legacy checks and strict compilation. The joined-DML harness passes 62 SQL
comparisons, 14 controlled concurrency comparisons and the resource checks.
The previous DML, SELECT/CTE/window/HAVING/LATERAL, index/UPDATE/VACUUM/JOIN
concurrency and smoke harnesses pass, as does logical interoperability in both
directions.

Additional promotion/parking checks expose inherited failures also reproduced
at pre-milestone commit `3100b12b5b3`: the pg_ctl promotion wait rejects a
17-field native WAL control, and the SQL-promotion nested-index case times out
at its wait-completion barrier. The SQL-promotion sequential case passes.

## Remaining limits

The target-outer execution strategy does not jointly optimize target and source
joins. It can perform more work than C's optimizer and may choose a different
source winner when several match. SQL does not promise that winner or RETURNING
row order; avoid relying on either. Source predicates inside derived queries
still use the existing source planner.
An index input buffers its full equality-key match before an upper LIMIT
consumes it, so that buffer can spill even when the source returns few rows.

MERGE, data-modifying CTE bodies, named relation composite values and the
existing broader type/optimizer restrictions are not added here.

# Defaults, CHECK constraints and basic aggregates

Column defaults and CHECK constraints accept the supported scalar expression
subset. For example:

```sql
CREATE TABLE items (n int DEFAULT 2 + 3, CONSTRAINT positive CHECK (n > 0));
ALTER TABLE items ADD CONSTRAINT small CHECK (n < 100) NOT VALID;
ALTER TABLE items VALIDATE CONSTRAINT small;
SELECT count(n), min(n), max(n), sum(n), avg(n) FROM items;
```

Defaults are analyzed when installed and folded on demand for a statement.
CHECK evaluates after NOT NULL, in constraint-name order; NULL passes. A
`NOT VALID` check skips the initial scan but enforces new row versions. VALIDATE
scans a fresh snapshot while allowing DML; schema changes remain serialized.
ADD, default changes, DROP and RENAME wait for table writers. DDL in explicit
transactions and additional multi-action ALTER forms remain unsupported.

COUNT(expr) counts non-NULL values, including false and zero. MIN and MAX ignore
NULLs and return NULL for empty or all-NULL inputs. They support numeric, float,
text, date/time and interval inputs, plus implemented name, OID and LSN types.

Numeric casts, arithmetic and aggregates support finite values and special
values, including NaN and infinities. Numeric columns and precision/scale
modifiers are supported. SUM and AVG also accept floating and interval inputs,
including in window aggregates; floating results are approximate.
Stored expressions use the existing supported function set. Sequence arguments
in persisted expressions must be literal names or NULL; dynamic names are
rejected with `0A000` so dependencies can be tracked.

Schema export preserves `NOT VALID` checks with a separate ALTER. Initial SQL
replication copy does not bypass checks: pre-existing violating rows or volatile
checks can make the copy fail with `23514`.

From `src/lisp`, run the differential and fresh-restart workload with:

```sh
PG_C_BINDIR=/path/to/postgres/bin ./t/defaults-checks-aggregates.sh
```

The validation run covered 155 unit, 170 interop, 714 physical and 1,131 legacy
tests; 155 C comparisons, six concurrency cases, spill quota and fresh restart;
native bootstrap and DDL replay; and C/native logical replication in both
directions. The physical suite ran before the final folding adjustments; focused
spill checks passed afterward. Numeric property checks ran 250 trials with seed
42; coverage and rejection rates were not measured, and no Function Spec ran.

C references include `catalog/heap.c` (`cookDefault`, `cookConstraint`),
`optimizer/util/clauses.c`, `executor/execExpr.c` (`ExecCheck`),
`commands/tablecmds.c`, `utils/adt/numeric.c` (`select_div_scale` and aggregate
finalizers), and `include/catalog/pg_aggregate.dat`.

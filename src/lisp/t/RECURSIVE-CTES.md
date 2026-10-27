# Recursive SELECT CTEs

`WITH RECURSIVE` supports SELECT definitions with forward dependencies and a
single legal self-reference in a recursive `UNION` or `UNION ALL`. The seed arm
fixes worktable column types and type modifiers before the recursive arm is
analyzed. Recursive producers are shared even when marked `NOT MATERIALIZED`. Ordinary `UNION`
with duplicate elimination is supported as well.

```sql
WITH RECURSIVE t(n) AS (
  SELECT 1
  UNION ALL
  SELECT n+1 FROM t WHERE n<5
)
SELECT n FROM t;
```

Each iteration reads a sealed current frontier and writes a separate next
frontier. Rows are produced lazily; the implementation does not sort all
levels. `UNION` removes duplicates across all generations using SQL grouping
keys for supported integral numeric values, NULL, arrays and records, with
case-sensitive character comparison. The seen set is bounded by work memory;
once spilled it uses 64 disk buckets and scans the relevant bucket linearly per
lookup. This can be slow for large distinct recursions. Frontier and result
stores can also spill to temporary files.

`SEARCH` and `CYCLE`, data-modifying CTE bodies and top-level `WITH` on DML
are deferred (`0A000`). Mutual recursion is unsupported (`0A000`), as in C
PostgreSQL. Existing restrictions on `VALUES`, `INTERSECT`, `EXCEPT` and
locking syntax remain. Full optimizer constant folding remains unavailable.
See [nonrecursive CTEs](CTES.md) for materialization and planning boundaries.

From `src/lisp`, with an installed C PostgreSQL peer, run:

```sh
PG_C_BINDIR=/path/to/installed/postgres/bin ./t/recursive-ctes.sh
```

The harness compares 60 SQL cases with C and exercises forced frontier, result
and seen-set spill, cycles, cancellation, temporary quota errors and recovery.
See [test inventory](TEST-INVENTORY.md#recursive-cte-coverage) for the recorded
verification and disposition.

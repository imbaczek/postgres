# Nonrecursive SELECT CTEs

`WITH` binds SELECT definitions in lexical order: later definitions can use
earlier ones, and nested `WITH` clauses can shadow outer names. Column alias
lists and default, `MATERIALIZED`, and `NOT MATERIALIZED` policies are
supported. Scalar SELECT subqueries containing `WITH` can appear in DML
expressions. Data-modifying CTE bodies and top-level `WITH` on DML are rejected with
`0A000`. See [recursive CTEs](RECURSIVE-CTES.md) for recursive SELECTs and
[HAVING and LATERAL](HAVING-LATERAL.md)
for their supported scope.

A shared CTE has a lazy producer and an appendable replay store that can spill;
each reader has its own cursor. A correlated producer's cache belongs to its
ancestor dependency frame, so a new parameter epoch gets its own state. The
default policy shares multiply referenced or volatile producers; explicit
`MATERIALIZED` requests sharing, while `NOT MATERIALIZED` uses per-reference
plans when safe. Volatile producers remain shared.

CTE producers force sequential scans and source order. Per-reference plans are
not full AST flattening or joint optimization: only safe filters in simple
inner/cross join shapes are pushed into a producer. Other optimization differences
can change error timing, not just performance.
Producers with named-window key projection also retain this boundary, even when
the named window has no active function: consumer filters are not pushed below
that projection.

From `src/lisp`, run:

```sh
PG_C_BINDIR=/path/to/repository/installed/bin ./t/ctes.sh
```

The C differential suite passed 114 cases, including stable `current_setting`,
a set-returning publication function, and nested unused named-window keys with
volatile and quantified expressions. Tests cover retained SQL99 key sharing,
grouped keys, and evaluation cadence for active and unused keys and for whole
selected expressions versus subexpressions. The harness
exercises spill, error and cancellation paths for SELECT and INSERT. The
unit suite also covers CTE-specific behavior in [cte-tests.lisp](rove/cte-tests.lisp).

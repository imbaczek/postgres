# Expression subqueries: design and tests

The supported forms are `EXISTS`, scalar subqueries, `IN` and `NOT IN`,
comparisons with `ANY`, `SOME` and `ALL`, and `ARRAY(SELECT ...)` for text
elements. Correlation uses nested lexical scopes. Only the server's existing
clauses and types are supported; analysis checks output arity, types and result
metadata. Scalar row cardinality is checked during execution. Row-valued and
MULTIEXPR subqueries, hashed subplans and decorrelation are deferred.
See [CTEs](CTES.md) and [HAVING and LATERAL](HAVING-LATERAL.md) for
their supported scope.

Analysis assigns aggregate ownership to the semantic query level and resolves
outer references by scope. Execution uses dependency-scoped caching where safe,
and scans correlated subplans against their outer frames. Quantified comparisons
preserve SQL three-valued logic and scalar expressions reject multiple rows.
DML subqueries see the statement snapshot; a concurrent target update can cause
an EPQ-style recheck against the refreshed target tuple while subqueries retain
that statement snapshot. Mixed relation registration includes a writer on the
target, excluding index DDL. Index leases are released both before and after DML
expression evaluation, including error exits. Streaming candidates preserve
per-row volatile-expression cadence.

PostgreSQL C reference points in this repository:

- [SubLink](../../include/nodes/primnodes.h) represents the parsed expression;
  [transformSubLink](../../backend/parser/parse_expr.c),
  [parse_relation.c](../../backend/parser/parse_relation.c) and
  [parse_agg.c](../../backend/parser/parse_agg.c) guide transformation, scope
  and semantic aggregate ownership.
- [subselect.c](../../backend/optimizer/plan/subselect.c) contains
  `simplify_EXISTS_query` and `build_subplan`; [paramassign.c](../../backend/optimizer/util/paramassign.c)
  assigns parameters. [nodeSubplan.c](../../backend/executor/nodeSubplan.c)
  implements scan subplans and initplans; [execMain.c](../../backend/executor/execMain.c)
  contains EPQ machinery. [pg_cast.dat](../../include/catalog/pg_cast.dat)
  supplies cast relationships for supported types.

From `src/lisp`, run:

```sh
PG_C_BINDIR=/path/to/repository/installed/bin ./t/subqueries.sh
```

The harness owns its peers and ports, honors `SBCL`, and uses `PYTHON` (default
`python3`) with only the standard library. The acceptance harness compares 108 SQL
differential cases, seven controlled concurrent SubPlan rechecks, and spill and
cancel cleanup plus indexed self-read with concurrent DDL. A separate fixture
in [subqueries_concurrency.py](interop/subqueries_concurrency.py) reproduces a
known C simple-EXISTS semijoin EPQ divergence. The scoped executor does not
implement the C decorrelation or hash optimizer, so these results do not imply
unrestricted C parity.

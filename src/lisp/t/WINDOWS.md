# Window functions: scope and tests

Supported functions are `row_number`, `rank`, `dense_rank`, `count(*)`, `sum`,
and text `array_agg`. Multiple inline and named windows and supported named
window inheritance are available. The default frame is peer-aware `RANGE`
from unbounded preceding through current row. Explicit `ROWS` and `RANGE`
accept that prefix or the whole partition through unbounded following.

`lead`, `lag`, value functions, `ntile`, floating-point rank functions, offset
bounds, `GROUPS`, exclusions, `FILTER`, `DISTINCT` and `count(expr)` windows are
deferred. Constant folding is limited to operator and cast window keys; it does
not imply general optimizer parity.

Specifications are resolved after targets, allowing SQL99 expression matching;
executable identity remains separate so distinct volatile transitions are not
merged. The planner evaluates shared partition/order keys before window stages,
which append function slots before final projection. A stage can omit sorting
when its keys are invariant. Ranking functions stream independently of frames.
Aggregate `ROWS` prefixes emit per row, while peer `RANGE` prefixes and whole
partitions buffer until their results are known. Array
transition snapshots are nondestructive; owned spill files and buffers are
cleaned on completion, stop, error and cancellation.

C reference points: [parse_agg.c](../../backend/parser/parse_agg.c) handles
window calls and ownership; [parse_clause.c](../../backend/parser/parse_clause.c)
resolves window definitions and SQL99 targets. The planner's
[make_window_input_target](../../backend/optimizer/plan/planner.c) and
[fix_upper_expr](../../backend/optimizer/plan/setrefs.c) guide input projection
and slot references. [nodeWindowAgg.c](../../backend/executor/nodeWindowAgg.c)
and [windowfuncs.c](../../backend/utils/adt/windowfuncs.c) describe execution
and ranking behavior.

From `src/lisp`, run:

```sh
PG_C_BINDIR=/path/to/repository/installed/bin ./t/windows.sh
```

`PG_C_BINDIR` names the repository's installed C PostgreSQL bin directory. The
harness honors `SBCL` and `PYTHON` (default `python3`, standard library only).
The 168 C differential cases include 30 deterministic generated SQL cases with
ties. Generated `ROWS` cases use an extra ordering key to avoid unspecified
ordering within peers. Spill, cancellation and recovery are also exercised.

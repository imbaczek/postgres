# HAVING and LATERAL

`HAVING` filters groups before window functions. Without `GROUP BY` it creates
an implicit global group, including for empty input. Safe clauses can move to
the input, and a constant gate can avoid scanning it. Analysis does not fold
constants in an unused CTE; folding occurs when the query is planned. The
implementation does not provide the C optimizer's general constant folding or
decorrelation. Constant expressions inside aggregate inputs can therefore raise
errors at a different time; for example, C folds `sum(1/0)` even over empty input.
`GROUP BY` remains limited to columns, and aggregates to the supported
`count`, `sum` and `array_agg` forms.

A `LATERAL` derived SELECT can refer to preceding FROM items. INNER, CROSS and
LEFT joins execute a dependent right input for each left row; LEFT emits a
null-extended row when there is no match. RIGHT and FULL joins permit a lateral
input only when it does not reference that join's left input. Join legality
considers references even in an unused CTE, while execution dependencies track
expressions that can actually run. Derived-table column aliases are supported. General FROM functions such
as `generate_series` and `unnest` are not supported; existing FROM composite
functions remain available. Mixed comma and explicit JOIN syntax is unsupported.

From `src/lisp`, run:

```sh
PG_C_BINDIR=/path/to/repository/installed/bin ./t/having-lateral.sh
```

The harness uses [interop/having_lateral.py](interop/having_lateral.py) for 118
C differential cases and also exercises spill, late errors, cancellation and
recovery. [having-tests.lisp](rove/having-tests.lisp) covers grouping, validation,
folding and query nesting; [lateral-tests.lisp](rove/lateral-tests.lisp) covers
iterator and parameter behavior. Related coverage includes [ctes.sh](ctes.sh),
[windows.sh](windows.sh), and the subquery and join suites.

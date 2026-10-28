# INSERT sources, RETURNING and conflict handling

INSERT accepts VALUES, DEFAULT VALUES and SELECT sources, including supported
set operations, subqueries and SELECT CTEs. Root SELECT CTEs can also prefix
INSERT, UPDATE and DELETE. Source plans stream under one statement snapshot;
rows inserted by the statement do not become new source rows.

INSERT, UPDATE and DELETE support RETURNING scalar expressions, subqueries,
bare/qualified stars and OLD/NEW scalar fields. RETURNING WITH can rename the
OLD and NEW namespaces. Projection uses actual modified rows, including a
concurrent row version accepted by the existing recheck path. Projected results
are buffered using the bounded query spool before RowDescription or DataRows
are sent, matching C's PORTAL_ONE_RETURNING/FillPortalStore behavior. A late
statement error rolls back writes and publishes no result rows.

ON CONFLICT supports DO NOTHING with or without an arbiter, and DO UPDATE
with inferred unique columns or a named unique/primary-key constraint. SET and
WHERE can reference the target and EXCLUDED. Inference uses existing plain
unique indexes, including NULLS NOT DISTINCT. Conflict updates lock before
evaluating WHERE, retain that lock through the transaction, retry changed rows
at READ COMMITTED and enforce snapshot visibility at REPEATABLE READ. A command
cannot update the same target row twice (21000).

Tuple headers preserve both insertion and deletion command IDs for a row
changed twice in one transaction, using the existing C ComboCID flag. This
keeps DML expression subqueries on the original command's snapshot. UPDATE and
single-row VALUES expressions evaluate in physical attribute order. Multirow
VALUES evaluates supplied expressions/defaults in source order before omitted
defaults, as C's separate VALUES range-table path does.

## References and coverage

C references include parser/analyze.c, rewrite/rewriteHandler.c,
executor/nodeModifyTable.c, tcop/pquery.c, utils/time/combocid.c and
include/access/htup_details.h; regression counterparts include insert.sql,
insert_conflict.sql, returning.sql and with.sql.

The three Rove modules returning-tests, insert-select-tests and
on-conflict-tests cover parsing, preparation, rejection rules, assignment
coercion, arbiter resolution and ComboCID boundaries/cleanup. `./t/dml.sh`
compares metadata, rows, command tags and SQLSTATEs against an owned C server.
Its companion concurrency and resource tests exercise transaction waits,
commit/rollback, isolation, row rechecks, false-WHERE locks, cancellation,
spill/quota cleanup and late-error rollback. The harness restarts the native
server to verify committed heap/index/command-ID behavior.

Validation for this change passes 141 unit, 170 interop and 531 physical Rove
tests, plus 1,131 legacy checks and strict fresh compilation. The DML harness
passes 102 SQL comparisons and 24 controlled concurrency scenarios, followed
by resource and restart checks. Existing SELECT/CTE/window/HAVING/LATERAL,
unique-index/UPDATE/VACUUM concurrency, index-scan and JOIN harnesses pass.
Logical interoperability passes in both directions; subscriber barriers prove
delivery of INSERT SELECT, conflict UPDATE and DELETE independently, with
skipped and rolled-back effects absent.

## Remaining limits

UPDATE FROM and DELETE USING are covered by the subsequent
[joined DML milestone](JOINED-DML.md). Data-modifying CTE bodies and MERGE remain
outside this implementation. Whole-row RETURNING values require named composite types,
which the current catalog does not provide; they fail explicitly with 0A000.
General DEFAULT expressions, expression/partial unique indexes and the existing
broader type/optimizer gaps are not added here. There is no general deadlock
detector; cancellation and statement timeout can release conflicting waits.

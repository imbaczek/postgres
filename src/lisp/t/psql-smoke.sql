-- t/psql-smoke.sql -- fixed acceptance script driven by t/psql-smoke.sh.

create table smoke (id int, name varchar(20));
insert into smoke values (3, 'c'), (1, 'a'), (2, 'b');
select id, name from smoke where id >= 2 order by id;
update smoke set name = 'bb' where id = 2;
select name from smoke where id = 2;
create unique index smoke_id on smoke (id);
insert into smoke values (1, 'dup');
delete from smoke where id = 3;
select id from smoke order by id;
begin;
insert into smoke values (10, 'x');
rollback;
select id from smoke order by id;
begin;
insert into smoke values (20, 'y');
commit;
select id from smoke order by id;
create table nn (a int not null);
insert into nn values (null);
create table vc (s varchar(3));
insert into vc values ('abcd');
select s from vc;
alter table nn add primary key (a);
insert into nn values (5);
insert into nn values (5);
select a from nn order by a;
create table un (a int unique);
insert into un values (null);
insert into un values (null);
insert into un values (1);
insert into un values (1);
select a from un where a is not null order by a;
drop table un;

-- PostgreSQL unique-index cases over int, bigint, and varchar-compatible keys.
-- Text is temporarily an alias for varchar(126).
create table unique_mix (
  i int,
  b bigint,
  v text,
  constraint unique_mix_key unique (i, b, v)
);
insert into unique_mix values (1, 9223372036854775807, 'alpha');
insert into unique_mix values (1, -9223372036854775808, 'alpha');
insert into unique_mix values (1, 9223372036854775807, 'alpha');
insert into unique_mix values (null, 9223372036854775807, 'alpha');
insert into unique_mix values (null, 9223372036854775807, 'alpha');
select i, b, v from unique_mix order by b;
drop table unique_mix;

create table unique_nnd_index (i int);
create unique index unique_nnd_i_idx on unique_nnd_index (i) nulls not distinct;
insert into unique_nnd_index values (null);
insert into unique_nnd_index values (null);
drop table unique_nnd_index;

create table unique_nnd_build (id int, i int);
insert into unique_nnd_build values (1, null), (2, null);
create unique index unique_null_build_idx on unique_nnd_build (i);
create unique index unique_nnd_build_idx on unique_nnd_build (i) nulls not distinct;
delete from unique_nnd_build where id = 2;
create unique index unique_nnd_build_idx on unique_nnd_build (i) nulls not distinct;
insert into unique_nnd_build values (3, null);
drop table unique_nnd_build;

create table unique_nnd_constraint (i int unique nulls not distinct);
insert into unique_nnd_constraint values (null);
insert into unique_nnd_constraint values (null);
drop table unique_nnd_constraint;

-- Failed CREATE TABLE constraint work must roll back the table and indexes.
create table unique_create_missing (i int, unique (missing));
create table unique_create_missing (i int);
drop table unique_create_missing;

create table unique_create_duplicate_name (
  i int,
  j int,
  constraint unique_create_duplicate_key unique (i),
  constraint unique_create_duplicate_key unique (j)
);
create table unique_create_duplicate_name (i int, j int);
drop table unique_create_duplicate_name;

create table unique_nnd_table (
  i int,
  b bigint,
  constraint unique_nnd_table_key unique nulls not distinct (i, b)
);
insert into unique_nnd_table values (null, 1);
insert into unique_nnd_table values (null, 1);
insert into unique_nnd_table values (null, 2);
insert into unique_nnd_table values (1, null);
insert into unique_nnd_table values (1, null);
select i, b from unique_nnd_table order by i, b;
drop table unique_nnd_table;

create table unique_alter (i int);
insert into unique_alter values (null);
alter table unique_alter add constraint unique_alter_key unique nulls not distinct (i);
insert into unique_alter values (null);
drop table unique_alter;

-- Adapted from alter_table.sql's int-only UNIQUE constraint cases.
create table unique_alter_add (test int);
alter table unique_alter_add add constraint unique_alter_add_key unique (test);
insert into unique_alter_add values (2);
insert into unique_alter_add values (2);
insert into unique_alter_add values (4);
drop table unique_alter_add;

create table unique_alter_build (test int);
insert into unique_alter_build values (2), (2);
alter table unique_alter_build add constraint unique_alter_build_key unique (test);
insert into unique_alter_build values (3);
drop table unique_alter_build;

create table primary_alter_build (i int);
insert into primary_alter_build values (2), (2);
alter table primary_alter_build add primary key (i);
insert into primary_alter_build values (null);
drop table primary_alter_build;

create table unique_alter_missing (test int);
alter table unique_alter_missing add constraint unique_alter_missing_key unique (test1);
drop table unique_alter_missing;

create table unique_alter_composite (test int, test2 int);
alter table unique_alter_composite add constraint unique_alter_composite_key unique (test, test2);
insert into unique_alter_composite values (4, 4);
insert into unique_alter_composite values (4, 4);
insert into unique_alter_composite values (4, 5), (5, 4), (5, 5);
select test, test2 from unique_alter_composite order by test, test2;
drop table unique_alter_composite;

create table atacc1 (test int, test2 int, unique(test));
alter table atacc1 add unique (test2);
insert into atacc1 (test2, test) values (3, 3);
insert into atacc1 (test2, test) values (2, 3);
drop table atacc1;

create table unique_using_pk (i int, b bigint);
insert into unique_using_pk values (1, 9223372036854775807), (2, -9223372036854775808);
create unique index unique_using_pk_idx on unique_using_pk (i, b);
alter table unique_using_pk add primary key using index unique_using_pk_idx;
drop index unique_using_pk_idx;
insert into unique_using_pk values (1, 9223372036854775807);
insert into unique_using_pk values (null, 3);
select i, b from unique_using_pk order by i;
drop table unique_using_pk;

create table unique_using_named (v varchar(20));
create unique index unique_using_named_idx on unique_using_named (v);
alter table unique_using_named add unique nulls not distinct using index unique_using_named_idx;
alter table unique_using_named add constraint unique_using_named_key unique using index unique_using_named_idx;
alter table unique_using_named add constraint unique_using_named_again unique using index unique_using_named_key;
drop index unique_using_named_key;
insert into unique_using_named values ('value');
insert into unique_using_named values ('value');
drop table unique_using_named;

create table unique_using_nnd_pk (i int);
create unique index unique_using_nnd_pk_idx on unique_using_nnd_pk (i) nulls not distinct;
alter table unique_using_nnd_pk add primary key using index unique_using_nnd_pk_idx;
drop table unique_using_nnd_pk;

-- Direct non-partition PRIMARY KEY regression cases from constraints.sql and
-- alter_table.sql, restricted to the supported integer types.
create table primary_inline (i int primary key, v text);
insert into primary_inline values (1, 'one');
insert into primary_inline values (1, 'duplicate');
insert into primary_inline values (null, 'null');
drop table primary_inline;

create table primary_composite_inline (
  i int,
  b bigint,
  v text,
  primary key (i, b)
);
insert into primary_composite_inline values (1, 10, 'one');
insert into primary_composite_inline values (1, 10, 'duplicate');
insert into primary_composite_inline values (1, 11, 'same first key');
insert into primary_composite_inline values (null, 12, 'null first key');
insert into primary_composite_inline values (2, null, 'null second key');
drop table primary_composite_inline;

create table primary_alter_success (i int);
alter table primary_alter_success add primary key (i);
insert into primary_alter_success values (1);
insert into primary_alter_success values (null);
drop table primary_alter_success;

create table primary_alter_existing_null (i int);
insert into primary_alter_existing_null values (null);
alter table primary_alter_existing_null add primary key (i);
insert into primary_alter_existing_null values (2);
drop table primary_alter_existing_null;

create table primary_alter_missing (i int);
alter table primary_alter_missing add primary key (missing);
drop table primary_alter_missing;

create table primary_alter_composite (i int, b bigint);
alter table primary_alter_composite add constraint primary_alter_composite_pk primary key (i, b);
alter table primary_alter_composite add primary key (i);
insert into primary_alter_composite values (4, 4);
insert into primary_alter_composite values (4, 4);
insert into primary_alter_composite values (4, 5), (5, 4), (5, 5);
insert into primary_alter_composite values (null, 3);
insert into primary_alter_composite values (3, null);
drop table primary_alter_composite;

create table primary_alter_replace (id int, test int);
alter table primary_alter_replace add constraint primary_alter_test_pk primary key (test);
insert into primary_alter_replace values (1, 1);
insert into primary_alter_replace values (2, null);
alter table primary_alter_replace drop constraint primary_alter_test_pk restrict;
insert into primary_alter_replace values (2, null);
alter table primary_alter_replace add constraint primary_alter_id_pk primary key (id);
insert into primary_alter_replace values (2, 3);
drop table primary_alter_replace;

create table primary_explicit_notnull (i int not null, primary key (i));
alter table primary_explicit_notnull drop constraint primary_explicit_notnull_pkey;
insert into primary_explicit_notnull values (null);
drop table primary_explicit_notnull;

-- Adapted from create_index.sql's DROP/ADD PRIMARY KEY USING INDEX replacement.
create table primary_replace_using_index (a int, b bigint);
alter table primary_replace_using_index add primary key (a);
create unique index primary_replace_using_idx on primary_replace_using_index (b, a);
insert into primary_replace_using_index values (1, 10);
alter table primary_replace_using_index
  drop constraint primary_replace_using_index_pkey,
  add constraint primary_replace_using_pkey primary key using index primary_replace_using_idx;
insert into primary_replace_using_index values (2, 20);
insert into primary_replace_using_index values (1, 10);
insert into primary_replace_using_index values (null, 30);
drop table primary_replace_using_index;

create table primary_replace_missing (a int);
create unique index primary_replace_absent on primary_replace_missing (a);
create unique index primary_replace_missing_idx on primary_replace_missing (a);
alter table primary_replace_missing
  drop constraint if exists primary_replace_absent,
  add constraint primary_replace_missing_pk primary key using index primary_replace_missing_idx;
insert into primary_replace_missing values (1);
drop index primary_replace_absent;
drop table primary_replace_missing;

-- Match upstream's NULLS DISTINCT/NND pair and constraint-owned index rename
-- and drop cases using supported key types.
create table unique_null_modes (i int);
create unique index unique_null_modes_distinct on unique_null_modes (i) nulls distinct;
create unique index unique_null_modes_nnd on unique_null_modes (i) nulls not distinct;
insert into unique_null_modes values (1);
insert into unique_null_modes values (null);
insert into unique_null_modes values (null);
drop index unique_null_modes_nnd;
insert into unique_null_modes values (null);
drop table unique_null_modes;

create table unique_constraint_rename (i int);
alter table unique_constraint_rename add constraint unique_constraint_old unique (i);
alter index unique_constraint_old rename to unique_constraint_index_renamed;
alter table unique_constraint_rename rename constraint unique_constraint_index_renamed to unique_constraint_renamed;
drop index unique_constraint_renamed;
alter table unique_constraint_rename drop constraint unique_constraint_renamed restrict;
insert into unique_constraint_rename values (1), (1);
drop table unique_constraint_rename;

create table unique_name_suffix (i int);
alter table unique_name_suffix add unique (i);
alter table unique_name_suffix add unique (i);
alter table unique_name_suffix rename constraint unique_name_suffix_i_key1 to unique_name_suffix_second_key;
alter table unique_name_suffix drop constraint unique_name_suffix_second_key;
drop table unique_name_suffix;

create table unique_pk_using_fail (i int);
insert into unique_pk_using_fail values (null);
create unique index unique_pk_using_fail_idx on unique_pk_using_fail (i);
alter table unique_pk_using_fail add primary key using index unique_pk_using_fail_idx;
insert into unique_pk_using_fail values (null);
drop index unique_pk_using_fail_idx;
drop table unique_pk_using_fail;

create table unique_drop_atomic (i int, j int, k int);
create unique index unique_drop_raw_first_idx on unique_drop_atomic (i);
create unique index unique_drop_raw_last_idx on unique_drop_atomic (j);
create unique index unique_drop_backed_idx on unique_drop_atomic (k);
alter table unique_drop_atomic add unique using index unique_drop_backed_idx;
drop index unique_drop_raw_first_idx, unique_drop_backed_idx;
insert into unique_drop_atomic values (1, 1, 1);
insert into unique_drop_atomic values (1, 2, 2);
drop index unique_drop_backed_idx, unique_drop_raw_last_idx;
insert into unique_drop_atomic values (2, 1, 2);
drop index unique_drop_raw_first_idx, unique_drop_raw_last_idx;
drop table unique_drop_atomic;

-- Keep catalog and index state across the server restart in psql-smoke.sh.
create table unique_nnd_restart (i int);
create unique index unique_nnd_restart_idx on unique_nnd_restart (i) nulls not distinct;
insert into unique_nnd_restart values (null);
create table unique_using_restart (i int);
create unique index unique_using_restart_idx on unique_using_restart (i);
alter table unique_using_restart add unique using index unique_using_restart_idx;

create table text_unique (v text);
create unique index text_unique_idx on text_unique (v);
insert into text_unique values ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
insert into text_unique values ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
insert into text_unique values ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
select length(v) from text_unique;
drop table text_unique;

create table unique_update (i int unique, v text);
insert into unique_update values (1, 'one'), (2, 'two');
update unique_update set i = 1 where i = 2;
delete from unique_update where i = 2;
insert into unique_update values (2, 'two again');
select i, v from unique_update order by i;
drop table unique_update;

create table unique_build (i int, v text);
insert into unique_build values (1, 'same'), (2, 'same');
create unique index unique_build_idx on unique_build (v);
delete from unique_build where i = 2;
create unique index unique_build_idx on unique_build (v);
insert into unique_build values (3, 'same');
drop table unique_build;

drop index smoke_id;
drop table smoke;
drop table nn;
drop table vc;
create table primary_restart (i int);
alter table primary_restart add constraint primary_restart_pk primary key (i);
select 1 as ok;

-- Ported from the filled-table equality case in btree_index.sql and the
-- populated-index builds in create_index.sql.
create table index_scan_events (id int, category int);
insert into index_scan_events values
  (1, 10), (2, 10), (3, 20), (4, null), (5, 10);
explain select id from index_scan_events where category = 10;
explain analyze select id from index_scan_events where category = 10;
select id from index_scan_events where category = 10 order by id;
create index index_scan_events_category_idx on index_scan_events (category);
explain select id from index_scan_events where category = 10;
explain (analyze) select id from index_scan_events where category = 10;
select id from index_scan_events where category = 10 order by id;
select id from index_scan_events
  where category = 10 and id >= 2 order by id desc limit 1;
explain select id from index_scan_events where category = 10 or id = 3;

-- The index keeps old versions as candidates; visibility and the complete
-- predicate still determine which rows survive UPDATE, DELETE, and VACUUM.
update index_scan_events set category = 30 where id = 2;
delete from index_scan_events where id = 5;
vacuum index_scan_events;
select id from index_scan_events where category = 10 order by id;
select id from index_scan_events where category = 30 order by id;
drop index index_scan_events_category_idx;
explain select id from index_scan_events where category = 30;
select id from index_scan_events where category = 30 order by id;
drop table index_scan_events;

-- Keep a populated non-unique index through the clean restart below.
create table index_scan_restart (id int, category int);
insert into index_scan_restart values (8, 7), (9, 7);
create index index_scan_restart_category_idx on index_scan_restart (category);

-- Ported from src/test/regress/sql/join.sql: inner and outer joins over
-- duplicate and NULL keys, including USING's merged output column.
create table join_left (id int, label text, note text);
create table join_right (id int, label text);
create table join_third (id int, label text);
insert into join_left values
  (1, 'left-one', 'l1'), (2, 'left-only', 'l2'),
  (3, 'left-three', 'l3'), (null, 'left-null', 'ln');
insert into join_right values
  (1, 'right-one'), (1, 'right-one-dup'), (3, 'right-three'),
  (4, 'right-only'), (null, 'right-null');
insert into join_third values
  (1, 'third-one'), (4, 'third-four'), (5, 'third-only');
explain select l.id, r.label from join_left as l
  inner join join_right as r on l.id = r.id;
select l.id, r.label from join_left as l
  inner join join_right as r on l.id = r.id order by l.id, r.label;
select l.id, l.label, r.label from join_left l
  left outer join join_right r on l.id = r.id
  order by l.id, l.label, r.label;
select l.id, l.label, r.id, r.label from join_left l
  right join join_right r on l.id = r.id
  order by r.id, r.label, l.id;
select l.id, l.label, r.id, r.label from join_left l
  full outer join join_right r on l.id = r.id
  order by l.id, r.id, l.label, r.label;
-- Exercise COUNT(*) over outer-join rows, including duplicate and NULL keys.
explain analyze select count(*) from join_left l
  left join join_right r on l.id = r.id;
select count(*) from join_left l left join join_right r on l.id = r.id;
select count(*) from join_left l full join join_right r using (id);

-- Restore the grouped COUNT(*) join case from PostgreSQL join.sql with a
-- compact tenk1 fixture.
create table tenk1 (thousand int, twothousand int, hundred int, fivethous int);
insert into tenk1 values
  (10, 100, 10, 3), (10, 200, 10, 5),
  (20, 100, 20, 7), (20, 200, 20, 10);
explain analyze select x.thousand, x.twothousand, count(*)
  from tenk1 x inner join tenk1 y on x.thousand = y.thousand
  group by x.thousand, x.twothousand
  order by x.thousand desc, x.twothousand;
select x.thousand, x.twothousand, count(*)
  from tenk1 x inner join tenk1 y on x.thousand = y.thousand
  group by x.thousand, x.twothousand
  order by x.thousand desc, x.twothousand;
-- Port join.sql's COUNT(*) and filter from its skew-join test. The current
-- planner uses nested loops here, so this checks SQL semantics, not hash join.
explain analyze select count(*) from tenk1 a cross join tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;
select count(*) from tenk1 a cross join tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;
drop table tenk1;

-- ON decides whether a row matched; WHERE filters after null extension.
select l.id, r.label from join_left l left join join_right r
  on l.id = r.id and r.label = 'right-three'
  order by l.id, l.label;
select l.id, r.label from join_left l left join join_right r
  on l.id = r.id where r.label = 'right-three';

-- USING emits one merged key. FULL joins coalesce both sides, including
-- through a second join in the left-deep join tree.
select * from join_left l full join join_right r using (id)
  order by id, l.label, r.label;
select id, l.id, r.id from join_left l full join join_right r using (id)
  order by 1, l.id, r.id;
select * from join_left l inner join join_right r using (id)
  order by id, r.label;
select * from join_left l left join join_right r using (id)
  order by id, l.label, r.label;
select * from join_left l right join join_right r using (id)
  order by id, l.label, r.label;
select id, l.id, r.id, c.id, l.label, r.label, c.label
  from join_left l full join join_right r using (id)
  full join join_third c using (id)
  order by 1, l.id, r.id, c.id;
select l.id, c.id from join_left l cross join join_third c
  where l.id = 1 order by c.id;

-- Multi-column USING resolves common integer types and keeps both NULL keys
-- unmatched, as in ordinary SQL equality joins.
create table join_pair_left (a int, b int, label text);
create table join_pair_right (a bigint, b int, label text);
insert into join_pair_left values (1, 2, 'left-match'), (3, 4, 'left-only'),
  (null, 1, 'left-null');
insert into join_pair_right values (1, 2, 'right-match'), (5, 6, 'right-only'),
  (null, 1, 'right-null');
select a, b, l.label, r.label from join_pair_left l
  full join join_pair_right r using (a, b)
  order by a, b, l.label, r.label;

-- Common varchar typmod falls back to unconstrained when inputs differ.
create table join_narrow (k varchar(5));
create table join_wide (k varchar(20));
insert into join_narrow values ('short');
insert into join_wide values ('long-right-only');
select k from join_narrow n full join join_wide w using (k) order by k;

select id from join_left l inner join join_right r on l.id = r.id;
select l.id from join_left as l where join_left.id = 1;
drop table join_narrow;
drop table join_wide;
drop table join_pair_left;
drop table join_pair_right;
drop table join_left;
drop table join_right;
drop table join_third;

-- Adapted from PostgreSQL aggregates.sql: simple grouped and plain counts.
create table aggregate_events (category int, label text);
insert into aggregate_events values
  (10, 'first'), (10, null), (20, 'other'), (null, 'null-a'), (null, 'null-b');
select category, count(*) from aggregate_events group by category order by category;
select count(*) from aggregate_events;
select 7, count(*) from aggregate_events;
select true, count(*) from aggregate_events;
select count(*) as n from aggregate_events where category = 10;
select count(*) from aggregate_events where category = 99;
select category, count(*) as n from aggregate_events
  group by category order by n desc, category limit 2;
select category from aggregate_events where category = 10 group by category;
select category, category, count(*) from aggregate_events
  group by category order by category;
create index aggregate_events_category_idx on aggregate_events (category);
explain select category from aggregate_events where category = 10;
explain select category, count(*) from aggregate_events group by category;
explain analyze select 1;
select category from aggregate_events where category = 10 limit 1;
select category, count(*) as n from aggregate_events
  where category = 10 group by category order by n;
select *, count(*) from aggregate_events group by category, label;
select category as label, count(*) from aggregate_events
  group by category, label order by aggregate_events.label;
select label, count(*) from aggregate_events group by category;
select category, count(*) from aggregate_events group by category having count(*) > 1;
drop table aggregate_events;

-- Run with the unmodified reference C server, never with pg-lisp.
-- The output is pgoutput's raw bytes, including source OIDs/LSNs/timestamps.
CREATE TABLE fixture_rows (id integer PRIMARY KEY, amount integer);
CREATE PUBLICATION fixture_pub FOR TABLE fixture_rows
  WITH (publish = 'insert, update, delete, truncate');
SELECT slot_name FROM pg_create_logical_replication_slot('fixture_slot', 'pgoutput');
INSERT INTO fixture_rows VALUES (1, 2);
UPDATE fixture_rows SET amount = 3 WHERE id = 1;
UPDATE fixture_rows SET id = 4 WHERE id = 1;
DELETE FROM fixture_rows WHERE id = 4;
INSERT INTO fixture_rows VALUES (5, 6);
TRUNCATE TABLE fixture_rows;
BEGIN;
SELECT pg_logical_emit_message(true, 'pg_lisp/ddl', 'x');
COMMIT;
SELECT pg_logical_emit_message(false, 'pg_lisp/ddl', 'x', true);
SELECT encode(data, 'hex') FROM pg_logical_slot_get_binary_changes(
  'fixture_slot', NULL, NULL, 'proto_version', '2',
  'publication_names', 'fixture_pub', 'messages', 'true', 'streaming', 'off');
SELECT pg_drop_replication_slot('fixture_slot');
DROP PUBLICATION fixture_pub;
DROP TABLE fixture_rows;

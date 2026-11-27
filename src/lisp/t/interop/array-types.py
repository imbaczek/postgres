"""Compare width_bucket threshold arrays and their wire types with PostgreSQL."""
import random
import struct
import sys

from subqueries import QueryConnection
from parking_wire import WireError


NAME_PREFIX = "a" * 63


class ArrayConnection(QueryConnection):
    def receive(self):
        kind, body = super().receive()
        if kind == b"E":
            fields = self.error_fields(body)
            self.diagnostics = tuple(fields.get(key) for key in ("C", "M", "D", "H"))
        return kind, body

    def observe_binary(self, sql, oid, parameter):
        self.columns = []
        bind = b"\0\0" + struct.pack("!HHH", 1, 1, 1)
        bind += (struct.pack("!i", -1) if parameter is None else
                 struct.pack("!i", len(parameter)) + parameter)
        bind += struct.pack("!HH", 1, 1)
        messages = (
            (b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!HI", 1, oid)),
            (b"B", bind),
            (b"D", b"P\0"),
            (b"E", b"\0" * 5),
            (b"S", b""),
        )
        self.socket.sendall(b"".join(kind + struct.pack("!I", len(body) + 4) + body
                                     for kind, body in messages))
        rows, error = [], None
        while True:
            kind, body = self.receive()
            if kind == b"E":
                error = self.error_fields(body)["C"]
            elif kind == b"D":
                rows.append(body)
            elif kind == b"Z":
                return ("error", error) if error else ("rows", list(self.columns), rows)


CASES = (
    "SELECT width_bucket(true, ARRAY[false,true])",
    "SELECT width_bucket('b'::varchar, '{a,b,c}'::varchar[])",
    "SELECT width_bucket('b'::name, ARRAY['a'::name,'b'::name])",
    *(f"SELECT width_bucket('{NAME_PREFIX}'::{operand_type}, "
      f"ARRAY['{NAME_PREFIX}b'::{threshold_type}])"
      for operand_type, threshold_type in (("name", "text"), ("name", "varchar"),
                                           ("text", "name"), ("varchar", "name"),
                                           ("text", "text"), ("varchar", "text"))),
    *(f"SELECT width_bucket('{NAME_PREFIX}'::{operand_type}, '{{{NAME_PREFIX}b}}')"
      for operand_type in ("name", "text")),
    "SELECT width_bucket('b'::\"char\", ARRAY['a'::\"char\",'b'::\"char\"])",
    "SELECT width_bucket('2020-01-02'::date, ARRAY['2020-01-01'::date,'2020-01-02'::date])",
    "SELECT width_bucket('12:00'::time, ARRAY['09:00'::time,'12:00'::time])",
    "SELECT width_bucket('12:00+00'::timetz, ARRAY['12:00+00'::timetz])",
    "SELECT width_bucket('12:00'::time, ARRAY['12:00+00'::timetz])",
    "SELECT width_bucket('12:00+00'::timetz, ARRAY['12:00'::time])",
    "SELECT width_bucket('12:00'::time, ARRAY['12:00+00'::timetz]), width_bucket('12:00+00'::timetz, ARRAY['12:00'::time])",
    "SELECT width_bucket('12:00+00'::timetz, ARRAY['09:00+00'::timetz,'12:00+00'::timetz])",
    "SELECT width_bucket('23:00+00'::timetz, ARRAY['01:00+01'::timetz,'05:00-02'::timetz,'12:00+00'::timetz,'18:00+03'::timetz])",
    "SELECT width_bucket('05:00-02'::timetz, '{01:00+01,05:00-02,12:00+00,18:00+03}'::timetz[])",
    "SELECT width_bucket('2020-01-02'::timestamp, ARRAY['2020-01-01'::timestamp,'2020-01-02'::timestamp])",
    "SELECT width_bucket('2020-01-02 00:00+00'::timestamptz, ARRAY['2020-01-01 00:00+00'::timestamptz,'2020-01-02 00:00+00'::timestamptz])",
    "SELECT width_bucket('2 days'::interval, ARRAY['1 day'::interval,'2 days'::interval])",
    "SELECT width_bucket(2::oid, ARRAY[1,2,3])",
    "SELECT width_bucket('0/10'::pg_lsn, ARRAY['0/01'::pg_lsn,'0/10'::pg_lsn])",
    "SELECT width_bucket(B'10', ARRAY[B'01',B'10'])",
    "SELECT width_bucket(2, '1 2 3'::int2vector)",
    "SELECT width_bucket(NULL::int2vector,NULL), width_bucket('1 2'::int2vector,NULL)",
    "SELECT width_bucket('1 2'::int2vector,ARRAY['1 2'::int2vector,'3 4'::int2vector])",
    "SELECT width_bucket('1 2'::int2vector,'{\"1 2\",\"3 4\"}')",
    "SELECT width_bucket('1 2'::int2vector,ARRAY[]::int2vector[])",
    "SELECT ARRAY['1 2'::int2vector,'3 4'::int2vector], '{\"1 2\",\"3 4\"}'::int2vector[]",
    "SELECT ARRAY['1 2','3 4']::int2vector[], ARRAY[NULL::int2vector]",
    "SELECT ARRAY[ARRAY[1,2]::int2vector,'1 2'::int2vector]",
    "SELECT width_bucket(ARRAY[1,2]::int2vector,ARRAY['1 2'::int2vector,ARRAY[1,2]::int2vector])",
    "SELECT width_bucket('1 2'::int2vector,ARRAY[NULL::int2vector])",
    "SELECT width_bucket('1 2'::int2vector,ARRAY[['1 2'::int2vector],['3 4'::int2vector]])",
    "SELECT '1 2'::int2vector::int2[], '1 2'::int2vector::int4[]",
    "SELECT ARRAY[1,2]::int2vector::text, ARRAY[1,2]::int2vector::int2[]",
    "SELECT ARRAY[1,2]::int2vector = '1 2'::int2vector, ARRAY[1,2]::int2vector > '1 2'::int2vector",
    "SELECT ARRAY[]::int2vector = ''::int2vector, ''::int2vector::int2[]",
    "SELECT array_length(''::int2vector::int2[],1), array_length(''::int2vector::int4[],1), array_length(''::int2vector::int2[]::int4[],1)",
    "SELECT ARRAY[]::int2vector::text",
    "SELECT ARRAY[1,NULL]::int2vector::text",
    "SELECT ARRAY[[1,2],[3,4]]::int2vector::text",
    "SELECT DISTINCT x FROM (SELECT ARRAY[1,2]::int2vector AS x UNION ALL SELECT '1 2'::int2vector) q ORDER BY x",
    "SELECT ARRAY[1,2]::int2vector IN ('1 2'::int2vector, ARRAY[1,2]::int2vector)",
    "SELECT x FROM (SELECT '1 2'::int2vector AS x UNION ALL SELECT ARRAY[1,2]::int2[]) q ORDER BY x",
    "SELECT '{1,2}'::int2[]::int2vector",
    "SELECT width_bucket(NULL::name, ARRAY['a'::name])",
    "SELECT width_bucket(true, NULL::bool[])",
    "SELECT width_bucket(true, ARRAY[false,NULL])",
    "SELECT width_bucket(true, ARRAY[[true,false],[true,false]])",
    "SELECT width_bucket('x'::date, ARRAY['2020-01-01'::date])",
    "SELECT width_bucket(true, ARRAY[1,2])",
    "SELECT width_bucket(1::oid, ARRAY[1.0::numeric])",
    "SELECT width_bucket('12:00'::time, ARRAY['2020-01-01'::date])",
    "SELECT width_bucket('not a date', ARRAY['2020-01-01'::date])",
    "SELECT ARRAY[false,true], ARRAY['a'::name], ARRAY['a'::varchar], ARRAY['a'::\"char\"]",
    "SELECT ARRAY['2020-01-01'::date], ARRAY['12:00'::time], ARRAY['12:00+00'::timetz]",
    "SELECT ARRAY['2020-01-01'::timestamp], ARRAY['2020-01-01 00:00+00'::timestamptz], ARRAY['1 day'::interval]",
    "SELECT ARRAY[1::oid], ARRAY['0/01'::pg_lsn], ARRAY[B'10']",
    "SELECT '{1 day,2 days}'::interval[], '{12:00+00,13:00+02}'::timetz[]",
    "SELECT x FROM (SELECT ARRAY['2 days'::interval] AS x UNION ALL SELECT ARRAY['1 day'::interval]) q ORDER BY x",
    "SELECT x FROM (SELECT ARRAY['13:00+00'::timetz] AS x UNION ALL SELECT ARRAY['12:00+00'::timetz]) q ORDER BY x",
    "SELECT x,count(*) FROM (SELECT ARRAY['1 day'::interval] AS x UNION ALL SELECT ARRAY['24 hours'::interval]) q GROUP BY x",
    "SELECT x,count(*) FROM (SELECT ARRAY['12:00+00'::timetz] AS x UNION ALL SELECT ARRAY['12:00+00'::timetz]) q GROUP BY x",
    "SET TIME ZONE 'America/New_York'",
    "SELECT width_bucket('12:00'::time, ARRAY['12:00'::timetz]), width_bucket('12:00'::timetz, ARRAY['12:00'::time])",
    "SELECT '12:00'::time::timetz, '12:00-04'::timetz::time",
)

UUID = "00112233-4455-6677-8899-aabbccddeeff"
TOAST_TEXT = "".join(random.Random(810).choices("0123456789abcdefghijklmnopqrstuvwxyz", k=60000))
STORED_CASES = (
    f"SELECT uuid '{UUID}'",
    f"SELECT uuid_hash('{UUID}'), uuid_hash_extended('{UUID}', -17), uuid_hash_extended('{UUID}', 0)",
    f"SELECT '{UUID}'::uuid::bytea::uuid",
    f"CREATE TABLE stored_defaults (id uuid DEFAULT '{UUID}', flags bool[] DEFAULT '{{f,NULL,t}}', "
    f"ids uuid[] DEFAULT '{{{UUID},NULL}}')",
    "INSERT INTO stored_defaults DEFAULT VALUES",
    "CREATE TABLE keyless_array_admission (payload text, a integer[])",
    f"INSERT INTO keyless_array_admission VALUES ('{TOAST_TEXT * 30}', ARRAY[1])",
    "SELECT length(payload), a FROM keyless_array_admission",
    "CREATE TABLE stored_common (id uuid PRIMARY KEY, enabled boolean DEFAULT false, "
    "b bool[], s smallint[], i integer[], l bigint[], v varchar(9)[], t text[], u uuid[])",
    "INSERT INTO stored_common (id,b,s,i,l,v,t,u) VALUES "
    "('{00112233445566778899AABBCCDDEEFF}', '[0:1][-1:0]={{f,NULL},{t,f}}', "
    "'{-32768,NULL,32767}', '{}', '{-9223372036854775808,NULL,9223372036854775807}', "
    f"'{{abc,NULL}}', ARRAY['{TOAST_TEXT}'::text], "
    f"'{{{UUID},NULL}}')",
    f"SELECT id,enabled,b,s,i,l,v,length(t[1]),u FROM stored_common WHERE id='{UUID}'",
    "BEGIN",
    "UPDATE stored_common SET enabled=true, b[0][-1]=true, i[1:2]=ARRAY[1,2], s=ARRAY[42], v[2:2]=ARRAY['abc ']",
    "SELECT enabled,b,i,s,v,b && '{f}',b @> '{NULL}',i <@ '{1,2,3}', i @> '{2,2}' FROM stored_common",
    "ROLLBACK",
    "SELECT enabled,b,i,length(t[1]) FROM stored_common",
    "UPDATE stored_common SET s=''::int2vector::smallint[]",
    "SELECT array_length(s,1), array_lower(s,1), array_upper(s,1) FROM stored_common",
    "VACUUM stored_common",
    f"SELECT id FROM stored_common WHERE id='{UUID}'",
    f"SELECT id FROM stored_common WHERE id='{UUID}' OR enabled IS TRUE",
    "CREATE TABLE stored_uuid_paths (id uuid PRIMARY KEY, marker integer)",
    "INSERT INTO stored_uuid_paths VALUES " + ",".join(f"('{i:032x}',{i})" for i in range(512)),
    "VACUUM stored_uuid_paths",
    "ANALYZE stored_uuid_paths",
    "SELECT marker FROM stored_uuid_paths WHERE id='000000000000000000000000000000ff'",
    "SELECT marker FROM stored_uuid_paths WHERE id='000000000000000000000000000000ff' OR marker=-1",
)
STORED_ERRORS = (
    ("SELECT ''::bytea::uuid", "22P03"),
    ("SELECT '{1}' && '{1}'", "42725"),
    ("INSERT INTO stored_common (id,v) VALUES ('00000000000000000000000000000000','{abcdefghij}')", "22001"),
    ("UPDATE stored_common SET v[2:2]=ARRAY['abcdefghij']", "22001"),
    ("UPDATE stored_common SET s=ARRAY[32768]", "22003"),
)

BINARY_CASES = (
    ("boolean", 16, 1, b"\0", b"\0"),
    ("boolean", 16, 1, b"\xff", b"\x01"),
    ("boolean", 16, 1, None, None),
    ("boolean", 16, 1, b"", "08P01"),
    ("boolean", 16, 1, b"\0\0", "22P03"),
    ("uuid", 2950, 16, bytes.fromhex(UUID.replace("-", "")), bytes.fromhex(UUID.replace("-", ""))),
    ("uuid", 2950, 16, None, None),
    ("uuid", 2950, 16, bytes(15), "08P01"),
    ("uuid", 2950, 16, bytes(17), "22P03"),
)


def check(reference, native):
    peers = [ArrayConnection(port, "array-types") for port in (reference, native)]
    failures = []
    try:
        for peer in peers:
            peer.observe("SET TIME ZONE 'UTC'")
        for sql in CASES:
            outcomes = [peer.observe(sql) for peer in peers]
            if outcomes[0] != outcomes[1]:
                failures.append((sql, outcomes))
                print("array-types: FAIL", failures[-1], flush=True)
        for position, sql in enumerate(STORED_CASES, 1):
            outcomes = [peer.observe(sql) for peer in peers]
            if outcomes[0] != outcomes[1] or outcomes[0][0] != "rows":
                failures.append((f"stored case {position}", outcomes))
                print("array-types: FAIL", failures[-1], flush=True)
        for sql, state in STORED_ERRORS:
            outcomes = [peer.observe(sql) for peer in peers]
            if outcomes != [("error", state)] * len(peers):
                failures.append((sql, outcomes))
                print("array-types: FAIL", failures[-1], flush=True)
        for type_name, oid, width, parameter, expected in BINARY_CASES:
            outcomes = [peer.observe_binary(f"SELECT $1::{type_name} AS datum", oid, parameter)
                        for peer in peers]
            if isinstance(expected, str):
                expected = ("error", expected)
            else:
                row = (struct.pack("!Hi", 1, -1) if expected is None else
                       struct.pack("!Hi", 1, len(expected)) + expected)
                expected = ("rows", [("datum", False, 0, oid, width, -1, 1)], [row])
            if outcomes != [expected] * len(peers):
                failures.append((type_name, parameter, expected, outcomes))
                print("array-types: FAIL", failures[-1], flush=True)
        for sql, oid, parameter in (
            ("SELECT $1::boolean[]", 1000, bytes(12)),
            ("SELECT ARRAY[$1::boolean]", 16, b"\0"),
        ):
            assert peers[1].observe_binary(sql, oid, parameter) == ("error", "0A000")
        indexed = "SELECT marker FROM stored_uuid_paths WHERE id='000000000000000000000000000000ff'"
        assert "Index Scan" in peers[1].query("EXPLAIN " + indexed)[0][0]
        assert "Seq Scan" in peers[1].query("EXPLAIN " + indexed + " OR marker=-1")[0][0]
        for sql in ("SELECT width_bucket(true, ARRAY['12:00'::time])",
                    "SELECT width_bucket(true, ARRAY['2020-01-01'::date])",
                    "SELECT width_bucket(true, ARRAY['a'::varchar])",
                    *(f"SELECT width_bucket(NULL::{type_name}[],NULL)"
                      for type_name in ("int2", "int4", "int8", "numeric", "float4", "float8",
                                        "text", "varchar", "name", '"char"', "oid", "bool",
                                        "date", "time", "timetz", "timestamp", "timestamptz",
                                        "interval", "pg_lsn", "bit", "int2vector"))):
            outcomes = []
            for peer in peers:
                try:
                    peer.query(sql)
                    outcomes.append("unexpected success")
                except WireError as error:
                    outcomes.append(peer.diagnostics)
            if outcomes[0] != outcomes[1]:
                failures.append((sql, outcomes))
                print("array-types: FAIL", failures[-1], flush=True)
        assert not failures, failures
        print(f"array-types: {len(CASES) + len(STORED_CASES)} C comparisons PASS", flush=True)
        print(f"array-types: {len(BINARY_CASES)} binary C comparisons PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


if __name__ == "__main__":
    check(int(sys.argv[1]), int(sys.argv[2]))

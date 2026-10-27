"""Compare expression subqueries with the repository's C PostgreSQL server."""
import struct
import sys

from parking_wire import RawConnection, WireError


class QueryConnection(RawConnection):
    def __init__(self, port, application_name="subqueries"):
        self.commands = []
        self.columns = []
        self.backend_pid = None
        super().__init__(port, application_name)

    def receive(self):
        kind, body = super().receive()
        if kind == b"K":
            self.backend_pid = struct.unpack("!II", body)[0]
        if kind == b"C":
            self.commands.append(body.rstrip(b"\0").decode())
        if kind == b"T":
            self.columns = []
            count = struct.unpack_from("!H", body)[0]
            offset = 2
            for _ in range(count):
                end = body.index(0, offset)
                name = body[offset:end].decode()
                origin, attnum, oid, size, typmod, form = struct.unpack_from("!IhIhih", body, end + 1)
                self.columns.append((name, bool(origin), attnum, oid, size, typmod, form))
                offset = end + 19
            assert offset == len(body)
        return kind, body

    def observe(self, sql):
        self.columns = []
        self.commands = []
        try:
            rows = self.query(sql)
            return ("rows", list(self.columns), list(rows), list(self.commands))
        except WireError as error:
            return ("error", error.sqlstate)

    def query(self, sql):
        self.columns = []
        self.commands = []
        return super().query(sql)


SETUP = (
    "CREATE TABLE subq_outer (id int, label varchar(20))",
    "CREATE TABLE subq_inner (owner int, value int, label varchar(20))",
    "INSERT INTO subq_outer VALUES (1,'a'), (2,'b'), (3,'c')",
    "INSERT INTO subq_inner VALUES (1,10,'x'), (1,20,'y'), (2,NULL,NULL)",
    "CREATE INDEX subq_inner_owner ON subq_inner(owner)",
    "CREATE INDEX subq_outer_id ON subq_outer(id)",
    "CREATE SEQUENCE subq_seq",
)

CASES = (
    "SELECT (SELECT 7)",
    "SELECT (SELECT 7 AS child_name)::text",
    "SELECT (SELECT label FROM subq_inner WHERE owner=2) AS child",
    "SELECT (SELECT 1 WHERE false)",
    "SELECT (SELECT 1 UNION ALL SELECT 2)",
    "SELECT (SELECT 1,2)",
    "SELECT EXISTS(SELECT 1/0), EXISTS(SELECT 1,2)",
    "SELECT EXISTS(SELECT sum(value) FROM subq_inner WHERE false)",
    "SELECT EXISTS(SELECT nextval('subq_seq') LIMIT 0)",
    "SELECT EXISTS(SELECT 1 FROM subq_inner LIMIT 10)",
    "SELECT 1 IN (SELECT 1 UNION ALL SELECT 2)",
    "SELECT 3 NOT IN (SELECT 1 UNION ALL SELECT NULL)",
    "SELECT NULL = ANY(SELECT 1 WHERE false), NULL = ALL(SELECT 1 WHERE false)",
    "SELECT 1 IN ((SELECT 1),2), 2 IN ((SELECT 1)+1)",
    "SELECT 1 = SOME(SELECT 1), 2 <> ALL(SELECT 1 UNION ALL SELECT NULL)",
    "SELECT 1 IN (SELECT '1')",
    "SELECT ARRAY(SELECT 'x' UNION ALL SELECT NULL)",
    "SELECT ARRAY(SELECT 'x' WHERE false)",
    "SELECT o.id FROM subq_outer o WHERE EXISTS(SELECT 1 FROM subq_inner i WHERE i.owner=o.id) ORDER BY o.id",
    "SELECT o.id FROM subq_outer o WHERE NOT EXISTS(SELECT 1 FROM subq_inner i WHERE i.owner=o.id) ORDER BY o.id",
    "SELECT o.id,(SELECT sum(value) FROM subq_inner i WHERE i.owner=o.id) FROM subq_outer o ORDER BY o.id",
    "SELECT o.id,(SELECT (SELECT o.id)) FROM subq_outer o ORDER BY o.id",
    "SELECT (SELECT s.x FROM (SELECT o.id AS x) s) FROM subq_outer o ORDER BY o.id",
    "SELECT (SELECT o.id FROM (SELECT 9 AS id) o) FROM subq_outer o ORDER BY o.id",
    "SELECT o.id,(SELECT o.id) FROM subq_outer o GROUP BY o.id ORDER BY o.id",
    "SELECT count(*),(SELECT o.id) FROM subq_outer o",
    "SELECT sum((SELECT sum(value) FROM subq_inner)) FROM subq_outer",
    "SELECT sum(o.id) = ANY(SELECT 6) FROM subq_outer o",
    "SELECT o.id,i.owner FROM subq_outer o LEFT JOIN subq_inner i ON i.owner=o.id AND EXISTS(SELECT 1 WHERE i.value>10) ORDER BY o.id,i.owner",
    "SELECT CASE WHEN false THEN (SELECT 1 UNION ALL SELECT 2) ELSE 7 END",
    "SELECT (SELECT nextval('subq_seq')) FROM subq_outer ORDER BY id",
    "SELECT (SELECT nextval('subq_seq')+o.id) FROM subq_outer o ORDER BY id",
    "SELECT (SELECT (SELECT nextval('subq_seq')+o.id) FROM subq_inner LIMIT 1) FROM subq_outer o ORDER BY id",
    "SELECT nextval('subq_seq') > ALL(SELECT value FROM subq_inner WHERE value IS NOT NULL)",
    "SELECT (SELECT sum(o.id)) FROM subq_outer o",
    "SELECT (SELECT sum(o.id)+1) FROM subq_outer o",
    "SELECT (SELECT (SELECT sum(o.id))) FROM subq_outer o",
    "SELECT (SELECT sum(o.id+i.value) FROM subq_inner i) FROM subq_outer o ORDER BY o.id",
    "SELECT sum((SELECT sum(o.id))) FROM subq_outer o",
    "SELECT o.id FROM subq_outer o WHERE (SELECT sum(o.id))>0",
    "SELECT count(*),(SELECT o.label) FROM subq_outer o",
    "SELECT o.label,(SELECT o.label) FROM subq_outer o GROUP BY o.label ORDER BY o.label",
    "SELECT o.id,(SELECT sum(o.id)) FROM subq_outer o GROUP BY o.id ORDER BY o.id",
    "SELECT 1 FROM subq_outer o ORDER BY (SELECT sum(o.id))",
    "SELECT (SELECT sum(o.id) FROM subq_inner) FROM subq_outer o",
    "SELECT sum((SELECT o.id)) FROM subq_outer o",
    "SELECT EXISTS(SELECT 1/0 FROM subq_outer GROUP BY id)",
    "SELECT ARRAY(SELECT (SELECT (nextval('subq_seq')+o.id)::text) FROM subq_inner) FROM subq_outer o ORDER BY o.id",
    "SELECT (SELECT nextval('subq_seq') FROM subq_outer LIMIT 1) FROM subq_inner",
    "SELECT CASE WHEN false THEN (SELECT nextval('subq_seq')) ELSE 7 END FROM subq_outer",
    "SELECT currval('subq_seq')",
    "SELECT (SELECT nextval('subq_seq') + CASE WHEN EXISTS(SELECT o.id) THEN 0 ELSE 0 END) FROM subq_outer o ORDER BY id",
    "SELECT (SELECT sum(i.value + sum(o.id)) FROM subq_inner i) FROM subq_outer o",
    "INSERT INTO subq_outer VALUES ((SELECT 4),'d')",
    "UPDATE subq_outer SET label=(SELECT 'v') WHERE id=4",
    "UPDATE subq_outer SET id=(SELECT id+10) WHERE id=4",
    "SELECT id,label FROM subq_outer ORDER BY id,label",
    "UPDATE subq_outer SET label=(SELECT label FROM subq_inner WHERE owner=subq_outer.id LIMIT 1) WHERE EXISTS(SELECT 1 FROM subq_inner WHERE owner=subq_outer.id)",
    "UPDATE subq_outer SET id=id+1 WHERE id IN(SELECT id FROM subq_outer WHERE id=1)",
    "SELECT id,label FROM subq_outer ORDER BY id,label",
    "UPDATE subq_outer SET id=(SELECT 1 UNION ALL SELECT 2) WHERE id=14",
    "INSERT INTO subq_outer VALUES (5,'ok'),((SELECT 1 UNION ALL SELECT 2),'bad')",
    "SELECT id,label FROM subq_outer ORDER BY id,label",
    "DELETE FROM subq_outer WHERE EXISTS(SELECT 1 FROM subq_inner WHERE owner=subq_outer.id)",
    "DELETE FROM subq_outer WHERE id IN(SELECT id FROM subq_outer WHERE id=14)",
    "SELECT id,label FROM subq_outer ORDER BY id,label",
    "INSERT INTO subq_outer VALUES ((SELECT '1'),'bad')",
    "UPDATE subq_outer SET id=(SELECT 'bad') WHERE false",
    "UPDATE subq_outer SET id=(SELECT true) WHERE false",
    "UPDATE subq_outer SET label=(SELECT true) WHERE id=3",
    "SELECT id,label FROM subq_outer ORDER BY id,label",
    "SELECT setval('subq_seq',1,false)",
    "UPDATE subq_outer SET id=(SELECT nextval('subq_seq')+id*0) WHERE (SELECT nextval('subq_seq')+id*0)>0",
    "SELECT id,label FROM subq_outer ORDER BY id,label",
    "SELECT 1 WHERE (SELECT 1)",
    "SELECT 1 FROM subq_outer o JOIN subq_inner i ON (SELECT 1)",
    "UPDATE subq_outer SET id=(SELECT 9) WHERE (SELECT 0)",
    "DELETE FROM subq_outer WHERE (SELECT 1)",
    "SELECT (SELECT count(*) FROM subq_inner GROUP BY o.id) FROM subq_outer o ORDER BY o.id",
    "SELECT (SELECT o.id FROM subq_inner GROUP BY o.id) FROM subq_outer o ORDER BY o.id",
    "SELECT EXISTS(SELECT 1/0 FROM subq_inner GROUP BY o.id) FROM subq_outer o ORDER BY o.id",
    "SELECT DISTINCT count(*) ORDER BY count(*)",
    "SELECT DISTINCT o.id,(SELECT o.id) AS related FROM subq_outer o GROUP BY o.id ORDER BY related",
    "SELECT DISTINCT count(*) AS k,count(*) AS k ORDER BY k",
    "SELECT DISTINCT (SELECT o.id) FROM subq_outer o ORDER BY (SELECT o.id)",
    "SELECT DISTINCT (SELECT o.id)+1 FROM subq_outer o ORDER BY (SELECT o.id)+1",
    "SELECT DISTINCT (SELECT d.x FROM (SELECT 1 AS x) d) ORDER BY (SELECT d.x FROM (SELECT 1 AS x) d)",
    "SELECT DISTINCT (SELECT 'A') ORDER BY (SELECT 'a')",
    "SELECT DISTINCT (SELECT o.id) FROM subq_outer o ORDER BY (SELECT id)",
    "SELECT DISTINCT (SELECT sum(o.id+nextval('subq_seq'))) FROM subq_outer o ORDER BY (SELECT sum(o.id+nextval('subq_seq')))",
    "SELECT setval('subq_seq',1,false)",
    "SELECT (SELECT sum(o.id+nextval('subq_seq')::int4)) FROM subq_outer o ORDER BY (SELECT sum(o.id+nextval('subq_seq')::int4))+1",
    "SELECT currval('subq_seq')",
    "SELECT setval('subq_seq',1,false)",
    "SELECT (SELECT nextval('subq_seq')+o.id) AS v FROM subq_outer o ORDER BY v",
    "SELECT currval('subq_seq')",
    "SELECT setval('subq_seq',1,false)",
    "SELECT (SELECT nextval('subq_seq')+o.id) FROM subq_outer o ORDER BY (SELECT nextval('subq_seq')+o.id) LIMIT 1",
    "SELECT currval('subq_seq')",
    "SELECT setval('subq_seq',1,false)",
    "SELECT 1 FROM subq_outer o ORDER BY (SELECT nextval('subq_seq')+o.id),(SELECT nextval('subq_seq')+o.id) DESC",
    "SELECT currval('subq_seq')",
    "SELECT setval('subq_seq',1,false)",
    "SELECT 1 FROM subq_outer o ORDER BY (SELECT sum(o.id+nextval('subq_seq')::int4)),(SELECT sum(o.id+nextval('subq_seq')::int4)) DESC",
    "SELECT currval('subq_seq')",
    "SELECT setval('subq_seq',1,false)",
    "SELECT o.id,(SELECT nextval('subq_seq')/3+o.id*0) AS a,(SELECT nextval('subq_seq')/3+o.id*0) AS b FROM subq_outer o ORDER BY a,b DESC",
    "SELECT currval('subq_seq')",
)


def check(reference_port, native_port):
    connections = [QueryConnection(port, "subqueries") for port in (reference_port, native_port)]
    try:
        for sql in SETUP:
            for connection in connections:
                result = connection.observe(sql)
                assert result[0] == "rows", (sql, result)
        for position, sql in enumerate(CASES, 1):
            observations = [connection.observe(sql) for connection in connections]
            assert observations[0] == observations[1], (position, sql, observations)
            print(f"subqueries: {position}: PASS", flush=True)
    finally:
        for connection in connections:
            connection.close()
    print(f"subqueries: {len(CASES)} C differential cases PASS", flush=True)


if __name__ == "__main__":
    check(*map(int, sys.argv[1:3]))

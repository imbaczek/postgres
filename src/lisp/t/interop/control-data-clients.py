import pathlib
import sys
import time

import psycopg2

port, directory = int(sys.argv[1]), pathlib.Path(sys.argv[2])
writer = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
reader = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
with writer.cursor() as cursor:
    cursor.execute("INSERT INTO lifecycle VALUES (2)")
reader.set_session(isolation_level="REPEATABLE READ", readonly=True)
with reader.cursor() as cursor:
    cursor.execute("SELECT count(*) FROM lifecycle")
    assert cursor.fetchone() == (2,)
(directory / "clients-ready").touch()
while True:
    time.sleep(0.1)

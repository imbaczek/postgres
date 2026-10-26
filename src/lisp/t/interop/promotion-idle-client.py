import pathlib
import sys
import time

import psycopg2


def wait_until(predicate):
    deadline = time.monotonic() + 90
    while not predicate():
        assert time.monotonic() < deadline, "promotion reader barrier timed out"
        time.sleep(0.05)


port, directory = int(sys.argv[1]), pathlib.Path(sys.argv[2])
reader = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
idle = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
idle.autocommit = True
try:
    with reader.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM data")
        assert cursor.fetchone() == (5,)
    with idle.cursor() as cursor:
        cursor.execute("SHOW transaction_read_only")
        assert cursor.fetchone() == ("on",)
    assert reader.get_parameter_status("in_hot_standby") == "on"
    assert idle.get_parameter_status("in_hot_standby") == "on"
    (directory / "reader-ready").touch()
    wait_until(lambda: (directory / "reader-release").exists())
    reader.commit()
    reader.autocommit = True

    def promoted():
        with idle.cursor() as cursor:
            cursor.execute("SELECT pg_is_in_recovery()")
            return cursor.fetchone() == (False,)

    wait_until(promoted)
    for connection, value in ((reader, 11), (idle, 12)):
        with connection.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            assert cursor.fetchone() == ("off",)
            assert connection.get_parameter_status("in_hot_standby") == "off"
            cursor.execute(f"INSERT INTO data VALUES ({value})")
            cursor.execute("BEGIN READ ONLY")
            try:
                cursor.execute("INSERT INTO data VALUES (999)")
            except psycopg2.Error as error:
                assert error.pgcode == "25006", error
            else:
                raise AssertionError("explicit READ ONLY allowed a write")
            cursor.execute("ROLLBACK")
    print("promotion-idle-client: existing readers and idle sessions write after promotion; wire status off PASS")
finally:
    reader.close()
    idle.close()

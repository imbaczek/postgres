"""Real pinned Patroni process discovery; this module starts no coordinator."""
import importlib.metadata
import os
import pathlib
import sys

import psutil
import psycopg2
from patroni.postgresql.postmaster import PostmasterProcess


def main():
    directory, expected_pid, port, action = sys.argv[1:]
    expected_pid, port = int(expected_pid), int(port)
    assert importlib.metadata.version("patroni") == "4.1.5"
    assert importlib.metadata.version("psutil") == "7.1.3"
    assert importlib.metadata.version("psycopg2-binary") == "2.9.11"
    if action == "reject-start-time":
        assert PostmasterProcess.from_pidfile(directory) is None
        print("patroni-process: reject-start-time PASS")
        return
    process = PostmasterProcess.from_pidfile(directory)
    assert process is not None and process.pid == expected_pid
    fields = PostmasterProcess._read_postmaster_pidfile(directory)
    assert pathlib.Path(fields["data_dir"]) == pathlib.Path(directory).resolve()
    assert fields["port"] == str(port)
    assert fields["socket_dir"] == "" and fields["shmem_key"] == ""
    assert fields["listen_addr"] == "127.0.0.1"
    assert abs(psutil.Process(expected_pid).create_time() - int(fields["start_time"])) < 2
    assert os.stat(pathlib.Path(directory) / "postmaster.pid").st_mode & 0o777 == 0o600
    lines = (pathlib.Path(directory) / "postmaster.pid").read_text().splitlines()
    assert len(lines) == 8 and lines[7] == "ready   "
    if action == "stop":
        assert process.signal_stop("fast") is None
    elif action == "check":
        with psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                              dbname="postgres", connect_timeout=2) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT %s::integer", (42,))
                assert cursor.fetchone() == (42,)
                cursor.execute("SELECT pg_is_in_recovery()")
                assert cursor.fetchone() == (False,)
            assert connection.server_version > 0
            print("patroni-process: driver server_version=" + str(connection.server_version))
    else:
        raise AssertionError(action)
    print("patroni-process: " + action + " PASS")


if __name__ == "__main__":
    main()

"""COPY TO preserves configured text octets in raw protocol frames."""
import importlib.util
from pathlib import Path
import struct

spec = importlib.util.spec_from_file_location(
    "command_complete", Path(__file__).with_name("command-complete.py"))
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)

VALUE = b"host=127.0.0.1 user=postgres password=public-fixture\xff\x80"


def check(port, sql_ascii=False):
    connection = wire.Connection(port)
    try:
        if sql_ascii:
            connection.query("SET client_encoding TO SQL_ASCII")
        connection.send(b"Q", b"SHOW primary_conninfo\0")
        rows = []
        while True:
            kind, body = connection.receive()
            assert kind != b"E", body
            if kind == b"D":
                assert body[:6] == struct.pack("!Hi", 1, len(VALUE)), body
                rows.append(body[6:])
            elif kind == b"Z":
                break
        assert rows == [VALUE], rows
        connection.send(b"Q", b"COPY (SELECT setting FROM pg_settings "
                        b"WHERE name='primary_conninfo') TO STDOUT\0")
        frames = []
        while True:
            kind, body = connection.receive()
            assert kind != b"E", body
            frames.append((kind, body))
            if kind == b"Z":
                break
        assert frames == [(b"H", b"\x00\x00\x01\x00\x00"),
                          (b"d", VALUE + b"\n"), (b"c", b""),
                          (b"C", b"COPY 1\0"), (b"Z", b"I")], frames
        assert connection.query("SELECT 1") == [["1"]]
    finally:
        connection.close()
    print("copy-octets: raw SHOW and COPY bytes, completion and connection recovery PASS")


def run():
    server = wire.OwnedLisp()
    server.data.mkdir(mode=0o700)
    (server.data / "postgresql.conf").write_bytes(b"primary_conninfo='" + VALUE + b"'\n")
    try:
        server.start()
        check(server.port)
    finally:
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()

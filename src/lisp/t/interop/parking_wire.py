"""Small trust-authenticated simple-query client for observing streamed rows."""
import socket
import struct


class WireError(RuntimeError):
    def __init__(self, fields):
        self.sqlstate = fields.get("C")
        super().__init__(f"{self.sqlstate}: {fields.get('M')}")


class RawConnection:
    def __init__(self, port, application_name="executor-parking"):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=120)
        self.rows_seen = []
        values = ("user", "postgres", "database", "postgres",
                  "application_name", application_name, "client_encoding", "UTF8")
        body = struct.pack("!I", 196608) + "\0".join(values).encode() + b"\0\0"
        self.socket.sendall(struct.pack("!I", len(body) + 4) + body)
        while True:
            kind, body = self.receive()
            if kind == b"R":
                assert body == struct.pack("!I", 0), "test server must use trust authentication"
            elif kind == b"E":
                raise WireError(self.error_fields(body))
            elif kind == b"Z":
                break

    def read_exact(self, size):
        data = bytearray()
        while len(data) < size:
            part = self.socket.recv(size - len(data))
            if not part:
                raise EOFError("server closed the test connection")
            data.extend(part)
        return bytes(data)

    def receive(self):
        kind = self.read_exact(1)
        length = struct.unpack("!I", self.read_exact(4))[0]
        assert 4 <= length <= 128 * 1024 * 1024, length
        return kind, self.read_exact(length - 4)

    @staticmethod
    def error_fields(body):
        fields = {}
        offset = 0
        while body[offset]:
            name = chr(body[offset])
            end = body.index(0, offset + 1)
            fields[name] = body[offset + 1:end].decode()
            offset = end + 1
        return fields

    @staticmethod
    def data_row(body):
        count = struct.unpack_from("!H", body)[0]
        offset, values = 2, []
        for _ in range(count):
            size = struct.unpack_from("!i", body, offset)[0]
            offset += 4
            assert size >= -1
            if size == -1:
                values.append(None)
            else:
                values.append(body[offset:offset + size].decode())
                offset += size
        assert offset == len(body)
        return values

    def query(self, sql):
        self.rows_seen = []
        body = sql.encode() + b"\0"
        self.socket.sendall(b"Q" + struct.pack("!I", len(body) + 4) + body)
        failure = None
        while True:
            kind, body = self.receive()
            if kind == b"D":
                self.rows_seen.append(self.data_row(body))
            elif kind == b"E":
                failure = WireError(self.error_fields(body))
            elif kind == b"Z":
                if failure:
                    raise failure
                return self.rows_seen

    def close(self):
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.socket.close()

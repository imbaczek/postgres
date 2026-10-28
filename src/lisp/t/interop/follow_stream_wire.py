"""Observe native follow admission and C-style timeline switches on owned peers."""
import struct

from parking_wire import RawConnection, WireError


def lsn(value):
    return f"{value >> 32:X}/{value & 0xffffffff:08X}"


def start_query(slot, system_id, base, cursor, timeline, manifest, seed):
    return (f"START_REPLICATION SLOT {slot} PHYSICAL {lsn(cursor)} "
            f"(native_format 'pg_lisp_wal_7', mode 'follow', system_id '{system_id}', "
            f"timeline '{timeline}', base_lsn '{lsn(base)}', prefix_digest '{seed}', "
            f"manifest_digest '{manifest}')")


def send_query(client, query):
    body = query.encode() + b"\0"
    client.socket.sendall(b"Q" + struct.pack("!I", len(body) + 4) + body)


def status(client, written, flushed, applied):
    body = b"r" + struct.pack("!QQQqB", written, flushed, applied, 0, 0)
    client.socket.sendall(b"d" + struct.pack("!I", len(body) + 4) + body)


def description(body):
    assert struct.unpack_from("!H", body)[0] == 2
    offset, columns = 2, []
    for _ in range(2):
        end = body.index(0, offset)
        name = body[offset:end].decode()
        _, _, oid, width, _, _ = struct.unpack_from("!IhIhih", body, end + 1)
        columns.append((name, oid, width))
        offset = end + 19
    assert offset == len(body)
    assert columns == [("next_tli", 20, 8), ("next_tli_startpos", 25, -1)], columns


def switch_result(client, fork):
    kinds, rows = [], []
    while True:
        kind, body = client.receive()
        kinds.append(kind)
        if kind == b"T":
            description(body)
        elif kind == b"D":
            rows.append(client.data_row(body))
        elif kind == b"C":
            assert body == b"START_STREAMING\0", body
        elif kind == b"Z":
            assert body == b"I"
            break
        elif kind == b"E":
            raise WireError(client.error_fields(body))
        else:
            raise AssertionError(("unexpected switch response", kind, body[:40]))
    assert kinds == [b"T", b"D", b"C", b"Z"], kinds
    assert rows == [["2", lsn(fork)]], rows


def at_fork(port, system_id, base, fork, manifest, seed, slot="selected_follow"):
    client = RawConnection(port, "follow-at-fork", replication=True)
    client.socket.settimeout(15)
    try:
        send_query(client, start_query(slot, system_id, base, fork, 1, manifest, seed))
        switch_result(client, fork)  # In particular, there must be no CopyBoth.
        assert client.query("IDENTIFY_SYSTEM")[0][:2] == [str(system_id), "2"]
    finally:
        client.close()


def rejects_unbound_requests(port, system_id, base, fork, manifest, seed):
    cases = [dict(system_id=system_id ^ 1), dict(manifest=manifest ^ 1),
             dict(cursor=fork + 1), dict(timeline=3), dict(seed=seed ^ 1)]
    for changes in cases:
        arguments = dict(slot="selected_follow", system_id=system_id, base=base,
                         cursor=fork, timeline=1, manifest=manifest, seed=seed)
        arguments.update(changes)
        client = RawConnection(port, "follow-rejected-identity", replication=True)
        client.socket.settimeout(15)
        try:
            send_query(client, start_query(**arguments))
            kind, body = client.receive()
            assert kind == b"E", (changes, kind, body[:40])
            assert client.error_fields(body)["C"] == "55000", changes
            assert client.receive() == (b"Z", b"I")
        finally:
            client.close()


def early_done(port, system_id, base, cursor, fork, manifest, seed, timeline,
               slot, after_data=False):
    client = RawConnection(port, "follow-early-copy-done", replication=True)
    client.socket.settimeout(15)
    sent_done = False
    try:
        send_query(client, start_query(slot, system_id, base, cursor, timeline, manifest, seed))
        assert client.receive() == (b"W", b"\0\0\0")
        if not after_data:
            client.socket.sendall(b"c" + struct.pack("!I", 4))
            sent_done = True
        while True:
            kind, body = client.receive()
            if kind == b"E":
                raise WireError(client.error_fields(body))
            if kind == b"c":
                assert sent_done and not body
                break
            assert kind == b"d"
            if body[:1] == b"k":
                if not sent_done:
                    status(client, cursor, cursor, base)
            else:
                assert body[:1] == b"w" and after_data and not sent_done
                client.socket.sendall(b"c" + struct.pack("!I", 4))
                sent_done = True  # No durable receipt is claimed for that frame.
        if timeline == 1:
            switch_result(client, fork)
        else:
            assert client.receive() == (b"C", b"START_STREAMING\0")
            assert client.receive() == (b"Z", b"I")
        assert client.query("IDENTIFY_SYSTEM")[0][:2] == [str(system_id), "2"]
    finally:
        client.close()


def malformed_copy_done(port, system_id, base, fork, manifest, seed):
    client = RawConnection(port, "follow-malformed-copy-done", replication=True)
    client.socket.settimeout(15)
    try:
        send_query(client, start_query("selected_follow", system_id, base, fork, 2, manifest, seed))
        assert client.receive() == (b"W", b"\0\0\0")
        client.socket.sendall(b"c" + struct.pack("!I", 5) + b"x")
        while True:
            kind, body = client.receive()
            if kind == b"E":
                assert client.error_fields(body)["C"] == "08P01"
                return
            assert kind == b"d" and body[:1] == b"k", (kind, body[:40])
    finally:
        client.close()


def parent_suffix(port, system_id, base, fork, manifest, prefix):
    client = RawConnection(port, "follow-parent-prefix", replication=True)
    client.socket.settimeout(15)
    written = flushed = base
    pending = bytearray()
    digest = 14695981039346656037
    try:
        send_query(client, start_query("selected_behind", system_id, base, base, 1,
                                       manifest, digest))
        assert client.receive() == (b"W", b"\0\0\0")
        while True:
            kind, body = client.receive()
            if kind == b"E":
                raise WireError(client.error_fields(body))
            if kind == b"c":
                assert not body and written == flushed == fork and not pending
                status(client, fork, fork, base)
                client.socket.sendall(b"c" + struct.pack("!I", 4))
                switch_result(client, fork)
                break
            assert kind == b"d" and body[:1] in (b"k", b"w"), (kind, body[:40])
            if body[:1] == b"w":
                start, durable, _ = struct.unpack_from("!QQq", body, 1)
                raw = body[25:]
                assert start == written and start + len(raw) <= durable <= fork
                assert raw == prefix[start:start + len(raw)]
                written += len(raw)
                pending.extend(raw)
                while len(pending) >= 32:
                    size = struct.unpack_from("<I", pending, 8)[0]
                    assert size >= 32
                    if len(pending) < size:
                        break
                    assert struct.unpack_from("<H", pending, 6)[0] != 13, "sent child EOR on parent"
                    for byte in pending[:size]:
                        digest = ((digest ^ byte) * 1099511628211) & ((1 << 64) - 1)
                    del pending[:size]
                    flushed += size
            else:
                assert len(body) == 18 and struct.unpack_from("!Q", body, 1)[0] <= fork
            status(client, written, flushed, base)  # Apply deliberately trails the anchor.
        return digest
    finally:
        client.close()


def child_eor(port, system_id, base, fork, manifest, seed, expected):
    client = RawConnection(port, "follow-child-eor", replication=True)
    client.socket.settimeout(15)
    try:
        send_query(client, start_query("selected_follow", system_id, base, fork, 2, manifest, seed))
        assert client.receive() == (b"W", b"\0\0\0")
        while True:
            kind, body = client.receive()
            if kind == b"E":
                raise WireError(client.error_fields(body))
            assert kind == b"d", (kind, body[:40])
            if body[:1] == b"k":
                status(client, fork, fork, base)
                continue
            assert body[:1] == b"w"
            start, durable, _ = struct.unpack_from("!QQq", body, 1)
            assert start == fork and durable >= fork + len(expected)
            assert body[25:] == expected
            # Read the frame without claiming its durability or applying it.
            status(client, fork + len(expected), fork, base)
            return
    finally:
        client.close()

"""Independent optional-field checks for native control inspection oracles."""
import zlib


def resource_values(data):
    assert len(data) == 32 and data[:8] == b"PGRES001" and not any(data[28:])
    values = tuple(int.from_bytes(data[index:index + 4], "little") for index in range(8, 28, 4))
    assert 1 <= values[0] <= 262143
    assert all(0 <= value <= 262143 for value in values[1:4])
    assert 10 <= values[4] <= 2147483647 and sum(values[:3]) + 3 <= 262143
    return values


def checkpoint_resource_fact(data):
    assert len(data) in (88, 136) and data[:8] in (b"PGCKPT01", b"PGCKPT02")
    if len(data) == 88:
        return None
    assert data[:8] == b"PGCKPT02" and not any(data[132:136])
    values = resource_values(data[88:120])
    lsn = int.from_bytes(data[120:128], "little")
    timeline = int.from_bytes(data[128:132], "little")
    assert 1 <= lsn <= int.from_bytes(data[8:16], "little")
    assert 1 <= timeline <= int.from_bytes(data[64:68], "little")
    return dict(parameters=list(values), lsn=lsn, timeline=timeline)


def recovery_control_layout(data):
    ordinary, pending = data[:8] == b"PGRSTAT3", data[:8] == b"PGRSTAT4"
    known = ((ordinary and len(data) in (120, 136))
             or (pending and len(data) in (136, 152, 180, 196)))
    size = len(data) - (48 if known else 0)
    assert (ordinary and size in (72, 88)) or (pending and size in (88, 104, 132, 148))
    assert zlib.crc32(data[:-4] + bytes(4)) == int.from_bytes(data[-4:], "little")
    if not known:
        return size, None
    fact = data[-52:-4]
    values = resource_values(fact[:32])
    lsn, timeline = int.from_bytes(fact[32:40], "little"), int.from_bytes(fact[40:44], "little")
    selected = size in ((104, 148) if pending else (88,))
    offset = (84 if pending else 68) if selected else 12
    assert not any(fact[44:]) and 1 <= lsn <= int.from_bytes(data[40:48], "little")
    assert 1 <= timeline <= int.from_bytes(data[offset:offset + 4], "little")
    return size, dict(parameters=list(values), lsn=lsn, timeline=timeline)


def check_optional_fields(fields):
    fields = [field.decode("ascii") if isinstance(field, bytes) else field for field in fields]
    assert fields[0] == "PGWAL6" and len(fields) in (7, 10, 14, 16, 20)
    if len(fields) == 7:
        return None
    assert 0 <= int(fields[7]) < 2**64
    assert 0 <= int(fields[8]) <= 6
    assert -(2**63) <= int(fields[9]) < 2**63
    if len(fields) in (16, 20):
        root, timeline, base, manifest, end, digest = map(int, fields[10:16])
        assert all(0 <= value < 2**64 for value in (root, base, manifest, end, digest))
        assert 1 <= timeline < 2**32 and 32 <= base <= end
    if len(fields) not in (14, 20):
        return None
    offset = 16 if len(fields) == 20 else 10
    assert fields[offset] == "R" and len(fields[offset + 1]) == 64
    data = bytes.fromhex(fields[offset + 1])
    values = resource_values(data)
    positions = fields[offset + 2:offset + 4]
    assert all(token.removeprefix("+").isascii() and token.removeprefix("+").isdigit()
               for token in positions)
    lsn, timeline = map(int, positions)
    assert 1 <= lsn <= min(int(fields[2]), 2**64 - 1)
    assert 1 <= timeline <= min(int(fields[3]), 2**32 - 1)
    return dict(parameters=values, lsn=lsn, timeline=timeline)

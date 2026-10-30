"""Independent optional-field checks for native control inspection oracles."""


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
    assert len(data) == 32 and data[:8] == b"PGRES001" and not any(data[28:])
    values = tuple(int.from_bytes(data[index:index + 4], "little") for index in range(8, 28, 4))
    assert 1 <= values[0] <= 262143
    assert all(0 <= value <= 262143 for value in values[1:4])
    assert 10 <= values[4] <= 2147483647 and sum(values[:3]) + 3 <= 262143
    positions = fields[offset + 2:offset + 4]
    assert all(token.removeprefix("+").isascii() and token.removeprefix("+").isdigit()
               for token in positions)
    lsn, timeline = map(int, positions)
    assert 1 <= lsn <= min(int(fields[2]), 2**64 - 1)
    assert 1 <= timeline <= min(int(fields[3]), 2**32 - 1)
    return dict(parameters=values, lsn=lsn, timeline=timeline)

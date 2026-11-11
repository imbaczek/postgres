"""Independent optional-field checks for native control inspection oracles."""
from crc32c_reference import crc32c


def resource_values(data):
    assert len(data) == 32 and data[:8] == b"PGRES001" and not any(data[28:])
    values = tuple(int.from_bytes(data[index:index + 4], "little") for index in range(8, 28, 4))
    assert 1 <= values[0] <= 262143
    assert all(0 <= value <= 262143 for value in values[1:4])
    assert 10 <= values[4] <= 2147483647 and sum(values[:3]) + 3 <= 262143
    return values


def checkpoint_commit_ts_fact(data):
    assert len(data) in (88, 96, 136, 144)
    assert data[:8] in (b"PGCKPT01", b"PGCKPT02")
    flags = int.from_bytes(data[84:88], "little")
    known = bool(flags & 4)
    assert flags <= 31 and known == (len(data) in (96, 144))
    assert not flags & 16 or flags & 8
    assert data[:8] == b"PGCKPT02" or (len(data) == 88 and flags == 0)
    if not known:
        return None
    pair = tuple(int.from_bytes(data[index:index + 4], "little")
                 for index in range(len(data) - 8, len(data), 4))
    assert pair == (0, 0)
    return pair


def checkpoint_full_page_writes_fact(data):
    checkpoint_commit_ts_fact(data)
    flags = int.from_bytes(data[84:88], "little")
    return bool(flags & 16) if flags & 8 else None


def checkpoint_resource_fact(data):
    checkpoint_commit_ts_fact(data)
    if len(data) in (88, 96):
        return None
    assert data[:8] == b"PGCKPT02" and not any(data[132:136])
    values = resource_values(data[88:120])
    lsn = int.from_bytes(data[120:128], "little")
    timeline = int.from_bytes(data[128:132], "little")
    assert 1 <= lsn <= int.from_bytes(data[8:16], "little")
    assert 1 <= timeline <= int.from_bytes(data[64:68], "little")
    return dict(parameters=list(values), lsn=lsn, timeline=timeline)


def source_fpw_fact(data, applied, applied_timeline):
    assert len(data) == 72 and data[:8] == b"PGFPWFC1"
    number = lambda offset, size: int.from_bytes(data[offset:offset + size], "little")
    flags = number(8, 4)
    assert flags <= 7 and number(12, 4) == 0
    checkpoint, disabled = bool(flags & 2), bool(flags & 4)
    fact = dict(enabled=bool(flags & 1), witness_kind="checkpoint" if checkpoint else "change",
                witness_start=number(16, 8), witness_end=number(24, 8),
                witness_timeline=number(32, 4), coverage_start=number(40, 8),
                coverage_timeline=number(36, 4), through=number(48, 8),
                through_timeline=number(56, 4),
                last_disable_start=number(64, 8) if disabled else None,
                last_disable_timeline=number(60, 4) if disabled else None)
    start, end = fact["witness_start"], fact["witness_end"]
    assert fact["coverage_start"] <= start < end <= fact["through"]
    assert 1 <= fact["coverage_timeline"] <= fact["witness_timeline"] <= fact["through_timeline"]
    assert (fact["through"], fact["through_timeline"]) == (applied, applied_timeline)
    assert start // (16 * 1024 * 1024) == (end - 1) // (16 * 1024 * 1024)
    assert (end - start - 32 in (88, 96, 136, 144)) if checkpoint else (end - start == 48)
    if disabled:
        position, timeline = fact["last_disable_start"], fact["last_disable_timeline"]
        assert fact["coverage_start"] <= position <= start
        assert fact["coverage_timeline"] <= timeline <= fact["witness_timeline"]
        assert position // (16 * 1024 * 1024) == (position + 47) // (16 * 1024 * 1024)
        if not checkpoint and not fact["enabled"]:
            assert (position, timeline) == (start, fact["witness_timeline"])
        else:
            assert position + 48 <= start
    else:
        assert number(60, 4) == number(64, 8) == 0
    assert checkpoint or fact["enabled"] or disabled
    return fact


def recovery_promotion_full_page_writes(data):
    """Read the pending promotion intent, or return None for the legacy layout.

    False means a present intent to turn FPW off. The desired policy is
    independent of the source FPW fact. This helper checks the intent and CRC
    when present; callers still need to validate the rest of the control image.
    """
    if len(data) % 4 == 0:
        return None
    assert len(data) % 4 == 1 and data[:8] == b"PGRSTAT4"
    assert len(data) - 17 in (88, 104, 132, 136, 148, 152, 160, 176,
                             180, 196, 204, 208, 220, 224, 252, 268)
    assert int.from_bytes(data[8:12], "little") & 0x7FFFFFFF == 3
    intent = data[-21:-4]
    assert intent[:8] == b"PGFPWPI1" and intent[8] in (0, 1) and not any(intent[9:])
    assert crc32c(data[:-4] + bytes(4)) == int.from_bytes(data[-4:], "little")
    return bool(intent[8])


def recovery_control_fields(data):
    """Check the control layout and return its resource and source FPW facts.

    A length of one modulo four marks the 17-byte intent after the optional
    source FPW fact and before the outer CRC. The intent is checked separately.
    """
    assert len(data) % 4 in (0, 1)
    intent = len(data) % 4 == 1
    if intent:
        recovery_promotion_full_page_writes(data)
    core_size = len(data) - (17 if intent else 0)
    ordinary, pending = data[:8] == b"PGRSTAT3", data[:8] == b"PGRSTAT4"
    fpw = ((ordinary and core_size in (144, 160, 192, 208))
           or (pending and core_size in (160, 176, 204, 208, 220, 224, 252, 268)))
    resource_end = core_size - 4 - (72 if fpw else 0)
    known = ((ordinary and resource_end + 4 in (120, 136))
             or (pending and resource_end + 4 in (136, 152, 180, 196)))
    size = resource_end + 4 - (48 if known else 0)
    assert (ordinary and size in (72, 88)) or (pending and size in (88, 104, 132, 148))
    assert crc32c(data[:-4] + bytes(4)) == int.from_bytes(data[-4:], "little")
    selected = size in ((104, 148) if pending else (88,))
    offset = (84 if pending else 68) if selected else 12
    applied, applied_timeline = int.from_bytes(data[40:48], "little"), int.from_bytes(data[offset:offset + 4], "little")
    resources = None
    if known:
        fact = data[resource_end - 48:resource_end]
        values = resource_values(fact[:32])
        lsn, timeline = int.from_bytes(fact[32:40], "little"), int.from_bytes(fact[40:44], "little")
        assert not any(fact[44:]) and 1 <= lsn <= applied
        assert 1 <= timeline <= applied_timeline
        resources = dict(parameters=list(values), lsn=lsn, timeline=timeline)
    policy = source_fpw_fact(data[core_size - 76:core_size - 4], applied, applied_timeline) if fpw else None
    return size, resources, policy


def recovery_control_layout(data):
    size, resources, _ = recovery_control_fields(data)
    return size, resources


def recovery_full_page_writes_fact(data):
    return recovery_control_fields(data)[2]


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

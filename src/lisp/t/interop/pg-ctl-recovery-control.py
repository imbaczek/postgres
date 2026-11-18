"""Exercise the pg_ctl reader with independently assembled control sections."""

from pathlib import Path
import runpy
import struct
import tempfile


utility = runpy.run_path(str(Path(__file__).resolve().parents[2] / "bin/pg_ctl"))


def image(pending=False, fact=b"", intents=b"", full=False):
    magic, phase, size = (b"PGRSTAT4", 3, 88) if pending else (b"PGRSTAT3", 2, 72)
    if full:
        assert pending
        size = 148
    core = bytearray(size - 4)
    struct.pack_into("<8sII", core, 0, magic, phase | ((1 << 30) if fact else 0), 1)
    struct.pack_into("<5QI", core, 16, 11, 17, 32, 160, 3, 5)
    if pending:
        struct.pack_into("<IIQ", core, 68, 3, 1, 1)
    if full:
        struct.pack_into("<IIQ", core, 84, 1, 2, 19)
        struct.pack_into("<QI4Q", core, 100, 1, 1, 32, 17, 160, 23)
        core += struct.pack("<8s6IQII", b"PGRES001", 100, 8, 10, 0, 128, 0, 32, 1, 0)
        core += struct.pack("<8sIIQQIIQQIIQ", b"PGFPWFC1", 3, 0, 32, 152, 1, 1, 32, 160, 1, 0, 0)
    data = core + intents + fact + bytes(4)
    struct.pack_into("<I", data, len(data) - 4, utility["crc32c"](data))
    return data


def fact(interval=False, through=160, interval_start=60):
    data = bytearray(104 if interval else 88)
    flags = 2 | (17 if interval else 0)
    struct.pack_into("<8sIIQQIIQQI", data, 0, b"PGLOGFC1", flags, 0, 70, 100, 1, 1, 32, through, 1)
    if interval:
        struct.pack_into("<QII", data, 88, interval_start, 1, 16)
    return data


def main():
    fpw = b"PGFPWPI1" + bytes(9)
    logical = b"PGLOGPI1" + bytes([1, 2]) + bytes(8)
    with tempfile.TemporaryDirectory(prefix="pg-ctl-control-") as directory:
        root = Path(directory)
        control = root / "physical-recovery.control"
        for pending in (False, True):
            for section in (b"", fact(), fact(interval=True)):
                for intents in ((b"", fpw, logical, fpw + logical) if pending else (b"",)):
                    control.write_bytes(image(pending, section, intents))
                    assert utility["recovery_phase"](root) == (3 if pending else 2)
        maximum = image(True, fact(interval=True), fpw + logical, full=True)
        assert len(maximum) == 407
        control.write_bytes(maximum)
        assert utility["recovery_phase"](root) == 3
        for payload_size in (104, 112, 152, 160):
            end = 32 + 32 + payload_size
            fpw_fact = struct.pack("<8sIIQQIIQQIIQ", b"PGFPWFC1", 3, 0,
                                   32, end, 1, 1, 32, end, 1, 0, 0)
            utility["validate_fpw_fact"](fpw_fact, end, 1)
        invalid = [
            image(fact=fact(through=159)),
            image(fact=fact(interval=True, interval_start=1)),
            image(fact=b"BADMAGIC" + fact()[8:]),
            image(fact=fact(), intents=logical),
            image(pending=True, fact=fact(), intents=b"PGLOGPI1" + bytes([0, 2]) + bytes(8)),
        ]
        for data in invalid:
            control.write_bytes(data)
            try:
                utility["recovery_phase"](root)
            except ValueError:
                pass
            else:
                raise AssertionError("pg_ctl accepted invalid recovery control")
    print("pg-ctl-recovery-control: logical fact, interval, and pending policy layouts PASS")


if __name__ == "__main__":
    main()

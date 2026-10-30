"""Check utility and independent readers against synthetic native control images."""
from pathlib import Path
import runpy
import struct
import tempfile
import unittest
import zlib

HERE = Path(__file__).resolve().parent
CTL = runpy.run_path(str(HERE.parents[1] / "bin/pg_ctl"))
CHECK = runpy.run_path(str(HERE / "resource_control_fields.py"))["check_optional_fields"]
PROMOTION = runpy.run_path(str(HERE / "promotion-primary-inspect.py"))["control"]
CHECKPOINT = runpy.run_path(str(HERE / "checkpoint-snapshot.py"))["read_copy"]
NAMESPACE = runpy.run_path(str(HERE / "wal-namespace-inspect.py"))["inspect"]


class ResourceControlTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="pg-resource-control-readers.")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "global").mkdir()
        (self.root / "pg_wal").mkdir()

    def fields(self, metadata=True, retained=False, resources=False, timeline=2):
        snapshot = (b"PGCKPT02" + struct.pack("<6QqIIQII", 0, 120, 0, 3, 2, 0, 0,
                                           timeline, 1, 0, 0, 0))
        record = bytearray(struct.pack("<4sHHIQQI", b"PGWL", 8, 4, 120, 0, 0, 0) + snapshot)
        struct.pack_into("<I", record, 28, zlib.crc32(record))
        namespace = "legacy" if timeline == 1 else "timeline"
        filename = "0000000000000000.wal" if timeline == 1 else f"{timeline:08X}0000000000000000.wal"
        (self.root / "pg_wal" / filename).write_bytes(record)
        fields = ["PGWAL6", "120", "120", str(timeline), str(timeline), namespace, snapshot.hex()]
        if metadata:
            fields += ["42", "6", "0"]
        if retained:
            fields += ["1", "1", "32", "99", "120", "7"]
        if resources:
            values = b"PGRES001" + struct.pack("<5I", 100, 8, 10, 0, 128) + bytes(4)
            fields += ["R", values.hex(), "120", str(timeline)]
        return fields

    def write(self, fields, corrupt_crc=False):
        prefix = " ".join(fields).encode("ascii")
        crc = zlib.crc32(prefix) ^ bool(corrupt_crc)
        (self.root / "global/pg_wal_control").write_bytes(prefix + f" {crc:08X}\n".encode("ascii"))

    def test_utility_accepts_legacy_and_extended_shapes(self):
        for metadata, retained, resources in ((False, False, False), (True, False, False),
                                               (True, True, False), (True, False, True),
                                               (True, True, True)):
            with self.subTest(metadata=metadata, retained=retained, resources=resources):
                fields = self.fields(metadata, retained, resources)
                self.write(fields)
                self.assertEqual(metadata, CTL["primary_committed"](self.root))
                self.assertEqual(resources, CHECK(fields) is not None)

    def test_independent_readers_accept_extended_shapes(self):
        for retained in (False, True):
            fields = self.fields(retained=retained, resources=True, timeline=1)
            self.write(fields)
            self.assertEqual((120, 120, 1, 1), PROMOTION(self.root)[:4])
            self.assertEqual(120, CHECKPOINT(self.root)[0]["end"])
            self.assertEqual(120, NAMESPACE(self.root)[0]["durable"])
            self.assertEqual((100, 8, 10, 0, 128), CHECK(fields)["parameters"])

    def test_malformed_extensions_fail_even_with_valid_crc(self):
        for retained in (False, True):
            original = self.fields(retained=retained, resources=True)
            offset = 16 if retained else 10
            bad_values = []
            for index, value in ((offset, "X"), (offset + 1, original[offset + 1][2:]),
                                 (offset + 2, "121"), (offset + 2, "0"),
                                 (offset + 3, "3"), (offset + 3, "0"),
                                 (offset + 2, "1_20"), (offset + 3, "0_2")):
                fields = original.copy()
                fields[index] = value
                bad_values.append(fields)
            for index, value in ((0, 0), (28, 1), (8, 0), (8, 262144),
                                 (8, 262143), (12, 262144), (16, 262144),
                                 (20, 262144), (24, 9), (24, 2147483648)):
                fields = original.copy()
                data = bytearray.fromhex(fields[offset + 1])
                struct.pack_into("<I", data, index, value)
                fields[offset + 1] = data.hex()
                bad_values.append(fields)
            bad_values += [original[:-1], original + ["0"]]
            for fields in bad_values:
                with self.subTest(retained=retained, fields=fields):
                    self.write(fields)
                    with self.assertRaises((ValueError, UnicodeError)):
                        CTL["primary_committed"](self.root)
                    with self.assertRaises((AssertionError, ValueError)):
                        CHECK(fields)

    def test_checksum_still_covers_the_extension(self):
        fields = self.fields(resources=True, timeline=1)
        self.write(fields, corrupt_crc=True)
        with self.assertRaises(ValueError):
            CTL["primary_committed"](self.root)
        for inspect in (PROMOTION, CHECKPOINT, NAMESPACE):
            with self.assertRaises(AssertionError):
                inspect(self.root)


if __name__ == "__main__":
    unittest.main()

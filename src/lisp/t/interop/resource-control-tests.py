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
DECODE_CHECKPOINT = runpy.run_path(str(HERE / "checkpoint-snapshot.py"))["checkpoint"]
NAMESPACE = runpy.run_path(str(HERE / "wal-namespace-inspect.py"))["inspect"]
RECOVERY_LAYOUT = runpy.run_path(str(HERE / "resource_control_fields.py"))["recovery_control_layout"]
DECODE_RECOVERY = runpy.run_path(str(HERE / "recovery-control-state.py"))["decode"]
PENDING = runpy.run_path(str(HERE / "restartpoint-prefix-inspect.py"))
PROMOTION_FIXTURE = runpy.run_path(str(HERE / "promotion-primary-inspect.py"))["checkpoint_bytes"]


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

    def known_checkpoint(self, timeline=1):
        fields = self.fields(resources=True, timeline=timeline)
        snapshot = bytearray.fromhex(fields[6])
        struct.pack_into("<3Q", snapshot, 8, 128, 296, 128)
        snapshot += bytes.fromhex(fields[11]) + struct.pack("<QII", 96, 1, 0)
        fields[1:3] = ["296", "296"]
        fields[6] = snapshot.hex()
        fields[12:14] = ["96", "1"]
        record = bytearray(struct.pack("<4sHHIQQI", b"PGWL", 8, 4, 168, 128, 0, 0) + snapshot)
        struct.pack_into("<I", record, 28, zlib.crc32(record))
        filename = "0000000000000000.wal" if timeline == 1 else f"{timeline:08X}0000000000000000.wal"
        (self.root / "pg_wal" / filename).write_bytes(bytes(128) + record)
        self.write(fields)
        return fields, snapshot

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

    def test_checkpoint_readers_accept_positioned_facts_and_parent_origins(self):
        for timeline in (1, 2):
            with self.subTest(timeline=timeline):
                fields, snapshot = self.known_checkpoint(timeline)
                self.assertEqual(timeline > 1, CTL["primary_committed"](self.root))
                facts, _ = CHECKPOINT(self.root)
                self.assertEqual(296, facts["end"])
                self.assertEqual(dict(parameters=[100, 8, 10, 0, 128], lsn=96, timeline=1),
                                 facts["resource_fact"])
                self.assertEqual(snapshot, PROMOTION(self.root)[5])
                if timeline == 1:
                    self.assertEqual(296, NAMESPACE(self.root)[0]["durable"])
                else:
                    with self.assertRaises(AssertionError):
                        NAMESPACE(self.root)  # This migration oracle selects timeline 1.
                self.assertEqual(96, CHECK(fields)["lsn"])

    def test_checkpoint_extension_rejects_malformed_and_future_facts(self):
        _, snapshot = self.known_checkpoint()
        for offset, value in ((88, 0), (116, 1), (120, 0), (120, 129), (128, 0), (128, 2), (132, 1)):
            bad = snapshot.copy()
            struct.pack_into("<I", bad, offset, value)
            with self.subTest(offset=offset, value=value), self.assertRaises(AssertionError):
                DECODE_CHECKPOINT(bad)
        bad = snapshot.copy()
        bad[:8] = b"PGCKPT01"
        for data in (bad, snapshot[:-1], snapshot + b"\0"):
            with self.assertRaises(AssertionError):
                DECODE_CHECKPOINT(data)

    def test_promotion_utility_reads_all_recovery_resource_layouts(self):
        for pending, layouts in ((False, (72, 88)), (True, (88, 104, 132, 148))):
            for layout in layouts:
                for known in (False, True):
                    with self.subTest(pending=pending, layout=layout, known=known):
                        data = bytearray(layout + (48 if known else 0))
                        data[:8] = b"PGRSTAT4" if pending else b"PGRSTAT3"
                        struct.pack_into("<IIQQQQQ", data, 8, 3 if pending else 2, 1,
                                         123, 456, 32, 256, 1003)
                        struct.pack_into("<Iq", data, 56, 5, 0)
                        selected = layout in ((104, 148) if pending else (88,))
                        if pending:
                            struct.pack_into("<IIQ", data, 68, 3, 0, 7)
                        offset = 84 if pending else 68
                        if selected:
                            struct.pack_into("<IIQ", data, offset, 2, 2, 99)
                            offset += 16
                        if pending and layout in (132, 148):
                            struct.pack_into("<QIQQQQ", data, offset, 9, 1, 32, 456, 256, 8)
                        if known:
                            fact = (b"PGRES001" + struct.pack("<5I", 100, 8, 10, 0, 128)
                                    + bytes(4) + struct.pack("<QII", 64, 1, 0))
                            data[-52:-4] = fact
                        struct.pack_into("<I", data, len(data) - 4, zlib.crc32(data))
                        path = self.root / "physical-recovery.control"
                        path.write_bytes(data)
                        self.assertEqual(3 if pending else 2, CTL["recovery_phase"](self.root))
                        decoded_size, decoded_fact = RECOVERY_LAYOUT(data)
                        self.assertEqual(layout, decoded_size)
                        self.assertEqual(known, decoded_fact is not None)
                        if pending:
                            self.assertEqual(data, PENDING["checked_pending_control"](self.root))
                            if layout in (132, 148):
                                self.assertEqual((9, 1, 32, 456, 256, 8), PENDING["retained_reference"](data))
                        else:
                            self.assertEqual(256, DECODE_RECOVERY(data)["applied"])
                            self.assertEqual(decoded_fact, DECODE_RECOVERY(data).get("resource_fact"))
                        if known:
                            for relative, value in ((0, 0), (28, 1), (32, 257), (40, 3), (44, 1)):
                                bad = data.copy()
                                struct.pack_into("<I", bad, len(bad) - 52 + relative, value)
                                struct.pack_into("<I", bad, len(bad) - 4, 0)
                                struct.pack_into("<I", bad, len(bad) - 4, zlib.crc32(bad))
                                path.write_bytes(bad)
                                with self.assertRaises(ValueError):
                                    CTL["recovery_phase"](self.root)
                                with self.assertRaises(AssertionError):
                                    RECOVERY_LAYOUT(bad)

    def test_promotion_fixture_preserves_frozen_resources_in_checkpoint_bytes(self):
        expected = dict(fork=256, floor=1003, oid=16384, time=0, child=3, parent=2)
        for known in (False, True):
            if known:
                expected["resource_fact"] = dict(parameters=[50, 4, 5, 0, 64], lsn=192, timeline=1)
            start, record = PROMOTION_FIXTURE(expected)
            self.assertEqual(168 if known else 120, len(record))
            self.assertEqual(start, struct.unpack_from("<Q", record, 12)[0])
            self.assertEqual(expected.get("resource_fact"), DECODE_CHECKPOINT(record[32:]).get("resource_fact"))


if __name__ == "__main__":
    unittest.main()

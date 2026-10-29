"""Exercise pg_ctl's production-state wait across native control shapes."""
from pathlib import Path
import runpy
import tempfile
from types import SimpleNamespace
import unittest
import zlib

module = SimpleNamespace(**runpy.run_path(str(Path(__file__).resolve().parents[2] / 'bin/pg_ctl')))


class PromotionControl(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='pg-ctl-control-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / 'global').mkdir()

    def write_control(self, fields):
        prefix = ' '.join(fields).encode()
        (self.root / 'global/pg_wal_control').write_bytes(
            prefix + f' {zlib.crc32(prefix):08X}\n'.encode())

    def test_production_state_with_and_without_retained_reference(self):
        # Checkpoint payload is opaque to pg_ctl; only lifecycle state is read.
        base = ['PGWAL6', '32', '64', '2', '2', 'timeline', '-', '42', '6', '1']
        for reference in ([], ['99', '1', '32', '123', '64', '456']):
            with self.subTest(reference=reference):
                self.write_control(base + reference)
                self.assertTrue(module.primary_committed(self.root))
                (self.root / 'physical-recovery.control').touch()
                self.assertFalse(module.primary_committed(self.root))
                (self.root / 'physical-recovery.control').unlink()
                for index, value in ((8, '5'), (3, '1'), (4, '1'), (5, 'legacy')):
                    fields = base.copy()
                    fields[index] = value
                    self.write_control(fields + reference)
                    self.assertFalse(module.primary_committed(self.root))

    def test_legacy_control_without_lifecycle_is_not_committed(self):
        self.write_control(['PGWAL6', '32', '64', '2', '2', 'timeline', '-'])
        self.assertFalse(module.primary_committed(self.root))

    def test_bad_shape_or_checksum_is_rejected(self):
        self.write_control(['PGWAL6', '32', '64', '2', '2', 'timeline', '-', '42', '6', '1', 'extra'])
        with self.assertRaises(ValueError):
            module.primary_committed(self.root)
        path = self.root / 'global/pg_wal_control'
        path.write_bytes(b'PGWAL6 32 64 2 2 timeline - 42 6 1 00000000\n')
        with self.assertRaises(ValueError):
            module.primary_committed(self.root)


if __name__ == '__main__':
    unittest.main()

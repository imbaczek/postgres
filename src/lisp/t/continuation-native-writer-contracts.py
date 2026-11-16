"""Preserve certified fixture data when a native case writes heap files."""

import importlib.util
import os
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'native_writer', Path(__file__).with_name('continuation-native-writer.py'))
writer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(writer)


class MutableBaseContracts(unittest.TestCase):
    def exercise(self, fail):
        with tempfile.TemporaryDirectory(prefix='writer-base-', dir=os.environ['TMPDIR']) as temporary:
            root = Path(temporary)
            data, source = root / 'data', root / 'source'
            data.mkdir()
            source.mkdir()
            (data / '.pg_lisp.lock').touch()
            for name in writer.NAMES:
                for parent, value in ((data, b'original'), (source, b'variant')):
                    if name == 'pg_xact':
                        (parent / name).write_bytes(value)
                    else:
                        (parent / name).mkdir()
                        (parent / name / 'state').write_bytes(value)
            (data / 'base').mkdir()
            (data / 'base' / 'heap').write_bytes(b'certified heap')
            before = writer.tree_image(data)
            destination = root / 'job'
            try:
                with writer.working_namespaces(data, destination, source=source, include_base=True):
                    self.assertEqual(b'variant', (data / 'global' / 'state').read_bytes())
                    self.assertEqual(b'certified heap', (data / 'base' / 'heap').read_bytes())
                    (data / 'base' / 'heap').write_bytes(b'changed by SQL')
                    (data / 'base' / 'new-table').write_bytes(b'new heap')
                    if fail:
                        raise RuntimeError('child failed after heap mutation')
            except RuntimeError:
                if not fail:
                    raise
            self.assertEqual(before, writer.tree_image(data))
            self.assertEqual(b'changed by SQL', (destination / 'variant/base/heap').read_bytes())
            self.assertTrue((destination / 'variant/base/new-table').exists())
            self.assertFalse((data.parent / 'continuation-writer-owner.json').exists())

    def test_heap_writes_restore_original_bytes_and_inodes(self):
        self.exercise(False)

    def test_failed_child_retains_working_heap_and_restores_original(self):
        self.exercise(True)


if __name__ == '__main__':
    unittest.main()

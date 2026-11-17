"""Check completeness and restoration of disposable launcher fixtures."""

import importlib.util
from pathlib import Path
import shutil
import tempfile
import time
import unittest


spec = importlib.util.spec_from_file_location('fixture_writer', Path(__file__).with_name('continuation-native-writer.py'))
writer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(writer)


def fixture(root, extras=()):
    root.mkdir(mode=0o700)
    (root / '.pg_lisp.lock').touch(mode=0o600)
    for name in writer.NAMES:
        if name == 'pg_xact':
            (root / name).write_bytes(b'clog')
        else:
            (root / name).mkdir(mode=0o700)
    for name in extras:
        (root / name).write_bytes(b'preserved metadata')


class CompleteVariantTests(unittest.TestCase):
    def test_unselected_intent_is_rejected_before_mutation(self):
        with tempfile.TemporaryDirectory(prefix='main-variant-') as temporary:
            root = Path(temporary)
            data, source = root / 'data', root / 'source'
            fixture(data)
            shutil.copytree(data, source)
            (source / 'physical-follow-managed.control').write_bytes(b'intent')
            before, source_before = writer.tree_image(data), writer.tree_image(source)
            with self.assertRaisesRegex(ValueError, 'unselected entries'):
                with writer.working_namespaces(data, root / 'work', source=source, full_source=True):
                    self.fail('A complete variant silently omitted managed intent')
            self.assertEqual(before, writer.tree_image(data))
            self.assertEqual(source_before, writer.tree_image(source))
            self.assertFalse((root / 'continuation-writer-owner.json').exists())

    def test_retired_profile_absence_is_copied_and_originals_are_restored(self):
        with tempfile.TemporaryDirectory(prefix='main-retired-') as temporary:
            root = Path(temporary)
            data, source = root / 'data', root / 'source'
            names = ('physical-recovery.control', 'physical-backup.manifest', 'recovery.required')
            fixture(data, names)
            shutil.copytree(data, source)
            for name in names:
                (source / name).unlink()
            before, source_before = writer.tree_image(data), writer.tree_image(source)
            with writer.working_namespaces(data, root / 'work', source=source,
                                           full_source=True, extra_names=names):
                self.assertTrue(all(not (data / name).exists() for name in names))
            self.assertEqual(before, writer.tree_image(data))
            self.assertEqual(source_before, writer.tree_image(source))


if __name__ == '__main__':
    started = time.monotonic()
    result = unittest.main(exit=False).result
    print('MAIN-HARNESS-CONTRACTS wall=%.6f tests=%d success=%s'
          % (time.monotonic() - started, result.testsRun, result.wasSuccessful()), flush=True)
    raise SystemExit(0 if result.wasSuccessful() else 1)

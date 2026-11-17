import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location('compile_cache', Path(__file__).with_name('test_compile_cache.py'))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)


class CompileCacheContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='compile-cache-', dir=os.environ['TMPDIR'])
        self.root = Path(self.temporary.name)
        self.campaign = self.root / 'campaign'
        self.job = self.root / 'producer'
        for path in [self.campaign / 'manifests', self.campaign / 'source/src/lisp',
                     self.job / 'tmp/plain-cache', self.job / 'logs']:
            path.mkdir(mode=0o700, parents=True)
        self.plain = self.job / 'tmp/plain-cache'
        self.fasl = self.plain / 'pg.fasl'
        self.fasl.write_bytes(b'fresh compiled code')
        self.log = self.job / 'tmp/strict.log'
        self.log.write_text('actual compiler output\n' + cache.PASS + '\n')
        self.image = self.job / 'logs/image.log'
        self.image.write_text('pinned runtime/core/ASDF/features\n')
        (self.campaign / 'manifests/image.log').write_bytes(self.image.read_bytes())
        self.checkpoint = os.memfd_create('test-checkpoint', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        self.checkpoint_path = '/proc/' + str(os.getpid()) + '/fd/' + str(self.checkpoint)
        self.report = self.job / 'logs/strict.json'
        self.data = dict(format=1, state='open', source=str(self.campaign / 'source/src/lisp'),
                         image=dict(sha256='source', files=1), checkout_state=dict(revision='head'),
                         environment=dict(values={'PG_LISP_TEST_COMPILE_POLICY': None}, executable='pinned'))
        cache.write_json(self.campaign / 'campaign.json', self.data)

    def tearDown(self):
        os.close(self.checkpoint)
        for path in self.root.rglob('*'):
            if path.is_dir() and not path.is_symlink():
                path.chmod(0o700)
        self.temporary.cleanup()

    def capture(self):
        return cache.capture(self.campaign, self.job, self.plain, self.log,
                             self.image, 'default', self.report, self.checkpoint_path)

    def publish(self):
        self.capture()
        return cache.publish(self.campaign, self.job, self.report, self.checkpoint)

    def consumer(self):
        job = self.root / 'consumer'
        (job / 'logs').mkdir(mode=0o700, parents=True)
        image = job / 'logs/image.log'
        image.write_bytes(self.image.read_bytes())
        plain, receipt = cache.prepare(self.campaign, job, 'default')
        return job, plain, receipt, image

    def test_capture_precedes_execution_and_only_compiler_listed_files_are_published(self):
        self.capture()
        (self.plain / 'test-created.fasl').write_bytes(b'not part of strict compilation')
        self.assertEqual(1, cache.publish(self.campaign, self.job, self.report, self.checkpoint))
        self.assertEqual(['pg.fasl'], [p.name for p in (self.campaign / 'plain-strict-cache').iterdir()])
        job, plain, receipt, image = self.consumer()
        self.assertEqual('reused', cache.verify_job(self.campaign, job, receipt, plain, 'default', image)['kind'])
        self.assertEqual(self.fasl.read_bytes(), (plain / 'pg.fasl').read_bytes())
        self.assertNotEqual(self.fasl.stat().st_ino, (plain / 'pg.fasl').stat().st_ino)

    def test_changed_compiler_artifact_is_not_published_after_tests(self):
        self.capture()
        self.fasl.write_bytes(b'test changed the compiler output')
        with self.assertRaises(ValueError):
            cache.publish(self.campaign, self.job, self.report, self.checkpoint)
        self.assertFalse((self.campaign / 'plain-strict-cache').exists())
        self.assertFalse((self.campaign / 'manifests/strict-compile.json').exists())

    def test_producer_receipt_cannot_requalify_test_modified_fasl(self):
        self.capture()
        self.fasl.write_bytes(b'changed after strict compilation')
        value = json.loads(self.report.read_text())
        value['files'] = cache.inventory(self.plain)
        self.report.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'pre-test checkpoint'):
            cache.publish(self.campaign, self.job, self.report, self.checkpoint)
        self.assertFalse((self.campaign / 'manifests/strict-compile.json').exists())

    def test_checkpoint_is_sealed_and_cannot_be_replaced_after_capture(self):
        original = self.capture()
        self.assertEqual(original, cache.read_checkpoint(self.checkpoint))
        with self.assertRaises(OSError):
            os.pwrite(self.checkpoint, b'changed', 0)
        with self.assertRaises(ValueError):
            cache.seal_checkpoint(self.checkpoint_path, original)

    def test_missing_fresh_marker_or_linked_artifact_cannot_qualify(self):
        self.log.write_text('compile did not finish\n')
        with self.assertRaises(ValueError):
            self.capture()
        self.log.write_text(cache.PASS + '\n')
        os.link(self.fasl, self.plain / 'alias.fasl')
        with self.assertRaises(ValueError):
            self.capture()
        self.assertFalse(self.report.exists())

    def test_missing_qualification_is_not_a_compile_fallback(self):
        job = self.root / 'consumer'
        (job / 'logs').mkdir(mode=0o700, parents=True)
        with self.assertRaises(FileNotFoundError):
            cache.prepare(self.campaign, job, 'default')
        self.assertFalse((job / 'plain-strict-cache').exists())

    def test_publication_refuses_existing_log_symlink_without_touching_target(self):
        self.capture()
        target = self.root / 'unrelated.log'
        target.write_bytes(b'preserve this file')
        (self.campaign / 'manifests/strict-compile.log').symlink_to(target)
        with self.assertRaises(FileExistsError):
            cache.publish(self.campaign, self.job, self.report, self.checkpoint)
        self.assertEqual(b'preserve this file', target.read_bytes())
        self.assertFalse((self.campaign / 'manifests/strict-compile.json').exists())

    def test_publication_and_readback_refuse_redirected_manifest_directory(self):
        self.capture()
        manifest_dir = self.campaign / 'manifests'
        original = self.campaign / 'original-manifests'
        manifest_dir.rename(original)
        unrelated = self.root / 'unrelated'
        unrelated.mkdir(mode=0o700)
        manifest_dir.symlink_to(unrelated, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'directory is not private'):
            cache.publish(self.campaign, self.job, self.report, self.checkpoint)
        with self.assertRaisesRegex(ValueError, 'directory is not private'):
            cache.read_published(self.campaign, 'default', self.image)
        self.assertEqual([], list(unrelated.iterdir()))

    def test_source_profile_and_actual_runtime_image_remain_bound(self):
        self.publish()
        job, plain, receipt, image = self.consumer()
        image.write_text('different actual core\n')
        with self.assertRaises(ValueError):
            cache.verify_job(self.campaign, job, receipt, plain, 'default', image)
        image.write_bytes(self.image.read_bytes())
        with self.assertRaises(ValueError):
            cache.verify_job(self.campaign, job, receipt, plain, 'fast', image)
        self.data['image']['sha256'] = 'changed source'
        (self.campaign / 'campaign.json').write_text(json.dumps(self.data))
        with self.assertRaises(ValueError):
            cache.verify_job(self.campaign, job, receipt, plain, 'default', image)

    def test_private_receipt_cannot_invent_a_different_qualified_fasl(self):
        self.publish()
        job, plain, receipt, image = self.consumer()
        (plain / 'pg.fasl').write_bytes(b'different code')
        value = json.loads(receipt.read_text())
        value['files'] = cache.inventory(plain)
        receipt.write_text(json.dumps(value))
        with self.assertRaises(ValueError):
            cache.verify_job(self.campaign, job, receipt, plain, 'default', image)

    def test_published_extra_files_symlinks_and_byte_changes_are_refused(self):
        self.publish()
        shared = self.campaign / 'plain-strict-cache'
        shared.chmod(0o700)
        extra = shared / 'extra.fasl'
        extra.write_bytes(b'extra')
        job = self.root / 'consumer'
        (job / 'logs').mkdir(mode=0o700, parents=True)
        with self.assertRaises(ValueError):
            cache.prepare(self.campaign, job, 'default')
        extra.unlink()
        extra.symlink_to(self.fasl)
        with self.assertRaises(ValueError):
            cache.prepare(self.campaign, job, 'default')
        extra.unlink()
        (shared / 'pg.fasl').chmod(0o600)
        (shared / 'pg.fasl').write_bytes(b'changed')
        with self.assertRaises(ValueError):
            cache.prepare(self.campaign, job, 'default')
        self.assertFalse((job / 'plain-strict-cache').exists())


if __name__ == '__main__':
    unittest.main()

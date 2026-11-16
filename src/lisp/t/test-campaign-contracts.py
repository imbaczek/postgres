"""Filesystem, cache and inherited-lease contracts for the coordinator."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('campaign', Path(__file__).with_name('test-campaign.py'))
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


class CampaignContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='campaign-contract-', dir=os.environ['TMPDIR'])
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_checkout_records_paths_from_repository_root(self):
        source = self.root / 'src/lisp'
        source.mkdir(parents=True)
        tracked = source / 'input.txt'
        tracked.write_text('original\n')
        (self.root / '.gitignore').write_text('src/lisp/ignored.txt\n')
        def git(*args):
            return subprocess.check_output(['git', '-C', str(self.root), *args], stderr=subprocess.STDOUT)
        git('init', '-q')
        git('config', 'core.hooksPath', '/dev/null')
        git('add', '.')
        git('-c', 'user.name=Campaign Test', '-c', 'user.email=campaign@example.invalid', 'commit', '-qm', 'fixture')
        tracked.write_text('changed\n')
        (source / 'added.txt').write_text('untracked\n')
        (source / 'ignored.txt').write_text('ignored but included input\n')
        state, patch = campaign.checkout_state(source)
        self.assertIn(b'+changed', patch)
        self.assertIn(b'src/lisp/input.txt', patch)
        self.assertEqual(['src/lisp/added.txt'], state['untracked'])
        self.assertEqual(['src/lisp/added.txt', 'src/lisp/ignored.txt'], state['nontracked_inputs'])

    def test_source_key_preserves_execution_bits_and_ignores_generated_files(self):
        source = self.root / 'source'
        source.mkdir()
        script = source / 'script.sh'
        script.write_text('#!/bin/sh\nexit 0\n')
        script.chmod(0o700)
        before = campaign.fingerprint(source)
        script.chmod(0o500)
        self.assertEqual(before, campaign.fingerprint(source))
        (source / 'generated.fasl').write_bytes(b'ignored')
        (source / '__pycache__').mkdir()
        (source / '__pycache__/ignored.pyc').write_bytes(b'ignored')
        self.assertEqual(before, campaign.fingerprint(source))
        script.chmod(0o400)
        self.assertNotEqual(before, campaign.fingerprint(source))

    def test_source_symlink_is_rejected(self):
        (self.root / 'link').symlink_to('/dev/null')
        with self.assertRaises(ValueError):
            campaign.fingerprint(self.root)

    def test_cache_collision_is_rejected_before_any_publication(self):
        private, shared = self.root / 'private', self.root / 'shared'
        private.mkdir()
        shared.mkdir()
        (private / 'a.fasl').write_bytes(b'new file')
        (private / 'z.fasl').write_bytes(b'new version')
        (shared / 'z.fasl').write_bytes(b'old version')
        with self.assertRaises(ValueError):
            campaign.publish_cache(private, shared)
        self.assertFalse((shared / 'a.fasl').exists())
        self.assertEqual(b'old version', (shared / 'z.fasl').read_bytes())

    def test_only_complete_asdf_fasls_are_published(self):
        private, shared = self.root / 'private', self.root / 'shared'
        private.mkdir()
        shared.mkdir()
        (private / 'good.fasl').write_bytes(b'complete')
        (private / 'good-tmpABC123.fasl').write_bytes(b'in progress')
        (private / 'other.log').write_text('not reusable')
        self.assertEqual(1, campaign.publish_cache(private, shared))
        self.assertEqual(['good.fasl'], [path.name for path in shared.iterdir()])
        self.assertEqual(0, campaign.publish_cache(private, shared))

    def test_cold_option_preserves_head_order(self):
        args = campaign.parse_args(['run', '/campaign', 'unit', '--cold', 'first', 'second'])
        self.assertTrue(args.cold)
        self.assertEqual(['first', 'second'], args.heads)
        args = campaign.parse_args(['run', '/campaign', 'unit', 'first', '--cold', 'second'])
        self.assertEqual(['first', 'second'], args.heads)

    def test_parallel_option_preserves_head_order(self):
        args = campaign.parse_args(['run', '/campaign', 'unit', 'first', '--parallel', 'second'])
        self.assertTrue(args.parallel)
        self.assertEqual(['first', 'second'], args.heads)

    def test_job_root_is_claimed_once_even_without_a_cache(self):
        campaign.claim_job(self.root)
        self.assertFalse((self.root / 'cache').exists())
        with self.assertRaises(FileExistsError):
            campaign.claim_job(self.root)

    def test_parent_cache_must_belong_to_the_job(self):
        cache = self.root / 'cache'
        cache.mkdir()
        with mock.patch.dict(os.environ, XDG_CACHE_HOME=str(cache)):
            self.assertEqual(cache / 'common-lisp', campaign.job_cache(self.root))
        with mock.patch.dict(os.environ, XDG_CACHE_HOME=str(self.root.parent / 'shared-cache')):
            with self.assertRaises(ValueError):
                campaign.job_cache(self.root)
        cache.rmdir()
        cache.symlink_to(self.root.parent, target_is_directory=True)
        with mock.patch.dict(os.environ, XDG_CACHE_HOME=str(cache)):
            with self.assertRaises(ValueError):
                campaign.job_cache(self.root)

    def test_wrapper_preserves_inherited_descriptors_seven_and_nine(self):
        script = """
import importlib.util, os, pathlib, subprocess, sys
spec = importlib.util.spec_from_file_location('campaign', sys.argv[1])
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
root = pathlib.Path(sys.argv[2])
with c.workspace_lease() as workspace, c.lock(root, 'run.lock', exclusive=True) as run:
    with c.lock(root, 'lease.lock') as lease:
        os.dup2(lease, 7)
        os.dup2(run, 9)
        wrapper = root / 'wrapper'
        wrapper.write_text(c.campaign_wrapper(root, sys.executable, (7, 9, workspace)))
        wrapper.chmod(0o700)
        command = [str(wrapper), '-c',
                   'import os,sys; assert os.fstat(7).st_ino == int(sys.argv[1]); assert os.fstat(9).st_ino == int(sys.argv[2])',
                   str(os.fstat(7).st_ino), str(os.fstat(9).st_ino)]
        status = subprocess.call(command, pass_fds=(7, 9, workspace))
        raise SystemExit(status)
"""
        result = subprocess.run([sys.executable, '-c', script, campaign.__file__, str(self.root)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10)
        self.assertEqual(0, result.returncode, result.stderr)

    def test_parallel_requires_existing_identity_and_target_inventory(self):
        manifests = self.root / 'manifests'
        manifests.mkdir()
        for name in ('image.log', 'unit.log'):
            with self.assertRaises(ValueError):
                campaign.require_parallel_manifests(self.root, 'unit')
            (manifests / name).write_text('checked by the child reader')
        campaign.require_parallel_manifests(self.root, 'unit')
        with self.assertRaises(ValueError):
            campaign.require_parallel_manifests(self.root, 'physical')
        (manifests / 'unit.log').unlink()
        (manifests / 'unit.log').symlink_to(manifests / 'image.log')
        with self.assertRaises(ValueError):
            campaign.require_parallel_manifests(self.root, 'unit')

    def test_parallel_run_can_enter_while_another_job_holds_shared_leases(self):
        script = """
import importlib.util, pathlib, sys
from unittest import mock
spec = importlib.util.spec_from_file_location('campaign', sys.argv[1])
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
root = pathlib.Path(sys.argv[2])
def admitted(_):
    print('admitted', flush=True)
    raise SystemExit(0)
with mock.patch.object(c, 'job_root', side_effect=[root, root / 'job']), \
     mock.patch.object(c, 'read_campaign', side_effect=admitted):
    c.run(root, 'unit', [], False, -1, parallel=True)
"""
        (self.root / 'job').mkdir()
        with campaign.lock(self.root, 'run.lock'), campaign.lock(self.root, 'lease.lock'):
            process = subprocess.Popen([sys.executable, '-c', script, campaign.__file__, str(self.root)],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                output, error = process.communicate(timeout=5)
            except BaseException:
                process.kill()
                process.communicate()
                raise
        self.assertEqual(0, process.returncode, error)
        self.assertEqual('admitted\n', output)

    def test_run_mount_redirection_is_rejected(self):
        root = os.environ['PG_LISP_TEST_RUN_DIR']
        with mock.patch.object(campaign.subprocess, 'check_output',
                               side_effect=['tmpfs rw,noswap\n', root + '\n']):
            with self.assertRaises(ValueError):
                campaign.job_root()

    def test_next_run_waits_for_surviving_compiler_lease(self):
        (self.root / 'job').mkdir()
        script = """
import importlib.util, pathlib, sys
from unittest import mock
spec = importlib.util.spec_from_file_location('campaign', sys.argv[1])
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
root = pathlib.Path(sys.argv[2])
def admitted(_):
    print('admitted', flush=True)
    raise SystemExit(0)
with mock.patch.object(c, 'job_root', side_effect=[root, root / 'job']), \
     mock.patch.object(c, 'read_campaign', side_effect=admitted):
    print('ready', flush=True)
    c.run(root, 'unit', [], False, -1)
"""
        with campaign.lock(self.root, 'lease.lock'):
            process = subprocess.Popen([sys.executable, '-c', script, campaign.__file__, str(self.root)],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.assertEqual('ready\n', process.stdout.readline())
                with self.assertRaises(subprocess.TimeoutExpired):
                    process.wait(timeout=0.2)
            except BaseException:
                process.kill()
                process.communicate()
                raise
        output, error = process.communicate(timeout=5)
        self.assertEqual(0, process.returncode, error)
        self.assertEqual('admitted\n', output)

    def test_exec_child_inherits_both_ownership_locks(self):
        with campaign.workspace_lease() as workspace, campaign.lock(self.root, 'run.lock', exclusive=True) as run_lock:
            with campaign.lock(self.root, 'lease.lock') as lease:
                command = [sys.executable, '-c',
                           'import os,sys; [os.fstat(int(value)) for value in sys.argv[1:]]',
                           str(lease), str(run_lock), str(workspace)]
                env = dict(os.environ, PG_LISP_FROZEN_SOURCE=str(self.root))
                self.assertEqual(0, campaign.wait_child(command, env, self.root / 'child-output.log', lease, run_lock, workspace))


if __name__ == '__main__':
    unittest.main()

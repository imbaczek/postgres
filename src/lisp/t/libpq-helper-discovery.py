"""Exercise helper discovery and atomic developer builds in isolated checkouts."""
import concurrent.futures
import os
from pathlib import Path
import shutil
import shlex
import stat
import subprocess
import tempfile
import time
import unittest


BASE = Path(__file__).resolve().parents[1]


class HelperDiscovery(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='pg-helper-discovery-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.project = self.root / 'source checkout'
        for name in ('bin', 'port', 't'):
            (self.project / name).mkdir(parents=True)
        self.resolver = self.project / 'bin/pg-libpq-helper-path'
        shutil.copy2(BASE / 'bin/pg-libpq-helper-path', self.resolver)
        shutil.copy2(BASE / 'port/libpq-helper.c', self.project / 'port/libpq-helper.c')
        builder = (BASE / 't/build-libpq-helper.sh').read_text()
        builder = builder.replace('CC=${CC:-cc}',
                                  'printf "build\\n" >> "$BASE/build-count"\nCC=${CC:-cc}')
        path = self.project / 't/build-libpq-helper.sh'
        path.write_text(builder)
        path.chmod(0o700)
        self.cache = self.root / 'private cache'
        self.env = dict(os.environ, XDG_CACHE_HOME=str(self.cache))
        self.env.pop('PG_LIBPQ_HELPER', None)

    def resolve(self, **overrides):
        return subprocess.run([str(self.resolver)], env=dict(self.env, **overrides),
                              text=True, capture_output=True, timeout=60)

    def helper(self, **overrides):
        result = self.resolve(**overrides)
        self.assertEqual(result.returncode, 0, result.stderr)
        path = Path(result.stdout.rstrip('\n'))
        self.assertTrue(path.is_file() and os.access(path, os.X_OK))
        return path

    def executable(self, path):
        path.write_text('#!/bin/sh\nexit 0\n')
        path.chmod(0o700)
        return path

    def test_explicit_override_precedes_installed_helper_and_build_tools(self):
        installed = self.executable(self.project / 'bin/pg-libpq-helper')
        override = self.executable(self.root / 'explicit helper')
        self.assertEqual(self.helper(PG_LIBPQ_HELPER=str(override), PG_CONFIG='/missing'), override)
        self.assertEqual(self.helper(PG_CONFIG='/missing'), installed)
        self.assertFalse(self.cache.exists())

    def test_cached_build_is_private_reused_and_invalidated_by_source_change(self):
        first = self.helper()
        self.assertTrue(first.is_relative_to(self.cache))
        self.assertEqual(stat.S_IMODE(first.parent.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(first.stat().st_mode) & 0o077, 0)
        self.assertEqual(self.helper(), first)
        self.assertEqual((self.project / 'build-count').read_text().splitlines(), ['build'])
        with (self.project / 'port/libpq-helper.c').open('a') as source:
            source.write('\n/* discovery invalidation fixture */\n')
        self.assertNotEqual(self.helper(), first)
        self.assertTrue(first.is_file())

    def test_concurrent_resolvers_publish_one_usable_cache_path(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(lambda _: self.helper(), range(4)))
        self.assertEqual(len(set(results)), 1)
        self.assertTrue(results[0].is_file())

    def test_failed_compilation_publishes_no_executable(self):
        result = self.resolve(CC='/bin/false')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(path.is_file() and os.access(path, os.X_OK)
                             for path in self.cache.rglob('*')))

    def test_daemon_without_home_variables_uses_account_home(self):
        tools = self.root / 'tools'
        tools.mkdir()
        account_home = self.root / 'account home'
        account_home.mkdir(mode=0o700)
        getent = tools / 'getent'
        record = f'fixture:x:{os.geteuid()}:{os.getegid()}::{account_home}:/bin/sh'
        getent.write_text('#!/bin/sh\nprintf "%s\\n" ' + shlex.quote(record) + '\n')
        getent.chmod(0o700)
        env = dict(self.env, PATH=str(tools) + os.pathsep + self.env['PATH'])
        env.pop('HOME', None)
        env.pop('XDG_CACHE_HOME', None)
        result = subprocess.run([str(self.resolver)], env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        helper = Path(result.stdout.rstrip('\n'))
        self.assertTrue(helper.is_relative_to(account_home / '.cache'))
        self.assertTrue(os.access(helper, os.X_OK))

    def test_cancellation_stops_compiler_and_discards_unpublished_build(self):
        compiler = self.root / 'slow compiler'
        compiler.write_text('#!/bin/sh\n'
                            'if [ "$1" = --version ]; then echo fixture-cc; exit 0; fi\n'
                            "trap 'trap \"\" HUP INT TERM; wait \"$child\"; exit 130' HUP INT TERM\n"
                            'sleep 60 &\nchild=$!\n'
                            'printf "%s %s\\n" "$$" "$child" > "$READY.tmp"\n'
                            'mv "$READY.tmp" "$READY"\nwait "$child"\n')
        compiler.chmod(0o700)
        ready = self.root / 'compiler.ready'
        process = subprocess.Popen([str(self.resolver)],
                                   env=dict(self.env, CC=str(compiler), READY=str(ready)),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 10
            while not ready.exists():
                self.assertIsNone(process.poll(), 'resolver stopped before compiler barrier')
                self.assertLess(time.monotonic(), deadline, 'compiler barrier timed out')
                time.sleep(0.01)
            compiler_pid, descendant_pid = map(int, ready.read_text().split())
            process.terminate()
            process.communicate(timeout=5)
            self.assertNotEqual(process.returncode, 0)
            self.assertFalse(Path(f'/proc/{compiler_pid}').exists())
            self.assertFalse(Path(f'/proc/{descendant_pid}').exists())
            self.assertFalse(any(path.is_file() for path in self.cache.rglob('*')))
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)


if __name__ == '__main__':
    unittest.main()

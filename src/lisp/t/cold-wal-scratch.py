#!/usr/bin/env python3
"""Kill real scratch writers, then normalize their residue in fresh Lisp images."""

import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import tempfile
import time


MODES = (
    'prefix-before-install', 'prefix-after-install',
    'history-before-install', 'history-after-install',
    'cleanup-after-unlink', 'cleanup-after-directory-sync',
)


def canonical_image(directory):
    result = {}
    for path in directory.iterdir():
        if path.name.startswith(('.timeline-', '.history-')):
            continue
        stat = path.stat()
        result[path.name] = (path.read_bytes(), stat.st_dev, stat.st_ino, stat.st_mtime_ns)
    return result


def main():
    lisp = Path(__file__).resolve().parents[1]
    logs = Path(os.environ.get('PG_LISP_TEST_LOG_DIR') or tempfile.mkdtemp(prefix='scratch-logs-'))
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    forms = [
        '(require :asdf)',
        f'(push #p{json.dumps(str(lisp) + "/")} asdf:*central-registry*)',
        '(asdf:load-system "pg")',
        f'(load {json.dumps(str(lisp / "t/cold-wal-scratch-native.lisp"))})',
        '(pg::cold-scratch-native-main)',
    ]
    for form in forms:
        command += ['--eval', form]
    start = time.monotonic()
    for mode in MODES:
        root = Path(tempfile.mkdtemp(prefix='cold-wal-scratch-'))
        for name in ('base', 'global', 'pg_wal'):
            (root / name).mkdir(mode=0o700)
        environment = dict(os.environ, PG_LISP_SCRATCH_TEST_ROOT=str(root))
        stage_log = logs / f'{mode}-stage.log'
        before_stage = time.monotonic()
        with stage_log.open('w') as output:
            stage = subprocess.run(command, env=dict(environment, PG_LISP_SCRATCH_TEST_MODE=mode),
                                   stdout=output, stderr=subprocess.STDOUT, timeout=120)
        assert stage.returncode == -signal.SIGKILL, (mode, stage.returncode, root, stage_log)
        assert f'COLD-SCRATCH-SIGKILL {mode}' in stage_log.read_text(), (mode, stage_log)
        before = canonical_image(root / 'pg_wal')
        if mode.endswith('before-install'):
            assert not before, (mode, before)
        else:
            assert len(before) == 1, (mode, before)
        if mode == 'history-after-install':
            assert (root / 'pg_wal/00000002.history').stat().st_nlink == 2
        recovery_log = logs / f'{mode}-recover.log'
        with recovery_log.open('w') as output:
            recovery = subprocess.run(command, env=dict(environment, PG_LISP_SCRATCH_TEST_MODE='recover'),
                                      stdout=output, stderr=subprocess.STDOUT, timeout=120)
        assert recovery.returncode == 0, (mode, recovery.returncode, root, recovery_log)
        assert 'COLD-SCRATCH-RECOVER-PASS' in recovery_log.read_text(), (mode, recovery_log)
        assert before == canonical_image(root / 'pg_wal'), (mode, 'canonical inode/bytes changed', root)
        assert all(not path.name.startswith(('.timeline-', '.history-'))
                   and path.stat().st_nlink == 1 for path in (root / 'pg_wal').iterdir()), (mode, root)
        print(f'COLD-SCRATCH-CASE-PASS {mode} seconds={time.monotonic()-before_stage:.6f}', flush=True)
        shutil.rmtree(root)
    print(f'COLD-SCRATCH-CASES {len(MODES)} wall_seconds={time.monotonic()-start:.6f}', flush=True)


if __name__ == '__main__':
    main()

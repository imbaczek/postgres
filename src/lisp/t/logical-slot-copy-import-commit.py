"""Commit a copied standby's child under real ownership, without admitting slots."""
import os
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import time

api = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-import-input.py')))
OwnedLisp = api['OwnedLisp']
standby = api['standby']
lisp_string = api['lisp_string']

CUTS = ('after-child-history-sync', 'after-child-eor-flush',
        'after-child-checkpoint-flush', 'after-child-data-sync',
        'before-child-control-file-fsync', 'after-child-control-file-fsync',
        'before-child-control-rename', 'after-child-control-rename',
        'after-child-control-directory-fsync')
FAULTS = {'rewrite-certificate': 'pending certificate changed',
          'hardlink-child': 'physical image input is not a private regular file',
          'symlink-control-temp': 'cannot create exclusive child control temporary',
          'corrupt-child': 'child WAL differs from its exact plan',
          'replace-child-control': 'child control inode changed after rename',
          'extra-child': 'child segment inventory differs from its exact plan',
          'extra-child-after-rename': 'child segment inventory differs from its exact plan',
          'corrupt-source-extent': 'retained WAL object differs from its immutable reference',
          'corrupt-control-temp': 'child control temporary bytes differ before rename',
          'owner-exit': 'child commit owner exit cut'}


def run_case(mode):
    started = time.monotonic()
    source = OwnedLisp(parameters=(('wal_level', 'logical'),))
    child = OwnedLisp(parameters=(('wal_level', 'replica'),))
    try:
        source.start()
        connection = standby['RawConnection'](source.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            for name in ('alpha', 'beta'):
                connection.query(f"SELECT * FROM pg_create_logical_replication_slot('{name}', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (1, 'healthy base')")
        finally:
            connection.close()
        child.data.mkdir(mode=0o700)
        source.control(f'(logical-slot-base-copy {lisp_string(child.data)} "healthy_import_pin")', timeout=90)
        child.start()
        standby['wait_rows'](child, [['1']])
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "healthy_import_pin")')
        standby['insert'](source, 2)
        standby['wait_rows'](child, [['1'], ['2']])
        child.control('(physical-follow-stop)')
        child.control('(physical-recovery-restartpoint)')
        api['probe'](child, pending=True, disposition='eligible')
        before = api['tree_image'](child.data)
        hook = Path(__file__).with_name('logical-slot-copy-import-commit-native.lisp')
        child.control(f'(handler-bind ((warning (lambda (c) (error "native load warning: ~a" c)))) (load {lisp_string(hook)}))')
        child.control(f'(setf *logical-import-commit-test-cut* :{mode})')
        child.control('(logical-import-commit-test-start)')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            status = child.process.poll()
            if status is not None:
                assert mode in CUTS and status == -signal.SIGKILL, (mode, status, child.root)
                assert f'LOGICAL-IMPORT-CHILD-SIGKILL :{mode.upper()}' in (child.root / 'server.log').read_text()
                break
            try:
                result = child.control('*logical-import-commit-test-result*')
            except (OSError, EOFError, AssertionError, subprocess.CalledProcessError):
                if mode in CUTS and child.process.poll() is not None:
                    continue
                raise
            if ':PASSED' in result:
                assert mode == 'delay', (mode, result)
                child.control('(assert *logical-import-commit-test-injected*)')
                break
            if ':ERROR' in result:
                assert mode in FAULTS and FAULTS[mode] in result, (mode, result, child.root / 'server.log')
                child.control('(assert (and *logical-import-commit-test-injected* *physical-recovery-failed-p*))')
                break
            time.sleep(0.05)
        else:
            raise TimeoutError((mode, child.root))
        committed = mode in ('delay', 'owner-exit', 'replace-child-control',
                             'extra-child-after-rename', 'after-child-control-rename',
                             'after-child-control-directory-fsync')
        after = api['tree_image'](child.data)
        control = 'global/pg_wal_control'
        assert (after[control] != before[control]) == committed, (mode, 'control commit boundary')
        source_mutations = []
        for name, image in before.items():
            if mode == 'corrupt-source-extent' and Path(name).name.startswith('extent-') and after[name] != image:
                source_mutations.append(name)
            elif name != control:
                assert after[name] == image, (mode, 'source changed', name)
        assert len(source_mutations) == (1 if mode == 'corrupt-source-extent' else 0), (mode, source_mutations)
        assert not list((child.data / 'pg_replslot').glob('*/state')), (mode, 'slots were admitted')
        temporary = child.data / 'global/pg_wal_control.tmp'
        quarantined = mode in ('before-child-control-file-fsync', 'after-child-control-file-fsync',
                               'before-child-control-rename', 'symlink-control-temp',
                               'corrupt-source-extent', 'corrupt-control-temp')
        assert os.path.lexists(temporary) == quarantined, (mode, 'temporary quarantine')
        if mode == 'symlink-control-temp':
            assert (child.data / 'child-control-witness').read_text() == 'unchanged'
            assert temporary.is_symlink()
        child.stop(immediate=True)
        stable = api['tree_image'](child.data)
        standby['startup_refused'](child, 'logical copy requires child-owner import before promotion')
        assert stable == api['tree_image'](child.data), (mode, 'fenced startup changed durable files')
        print(f'LOGICAL-IMPORT-CHILD-CASE-PASS {mode} committed={committed} seconds={time.monotonic()-started:.6f}', flush=True)
    finally:
        child.stop(immediate=True)
        source.stop(immediate=True)


if __name__ == '__main__':
    cases = sys.argv[1:] or ['delay', *FAULTS, *CUTS]
    assert all(mode == 'delay' or mode in CUTS or mode in FAULTS for mode in cases)
    for selected in cases:
        run_case(selected)

"""Run native writer cases while preserving the original certified fixture."""

from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import time


NAMES = ('pg_wal', 'pg_replslot', 'pg_logical_continuation', 'global', 'pg_xact')


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def tree_image(path):
    """Include identities without following workspace links."""
    result = {}

    def visit(item):
        info = item.lstat()
        row = [info.st_dev, info.st_ino, info.st_mode]
        if stat.S_ISREG(info.st_mode):
            row += [info.st_size, hashlib.sha256(item.read_bytes()).hexdigest()]
        elif stat.S_ISLNK(info.st_mode):
            row += [os.readlink(item)]
        elif stat.S_ISDIR(info.st_mode):
            for child in sorted(item.iterdir()):
                visit(child)
        else:
            raise ValueError('Unexpected special fixture file: ' + str(item))
        result[str(item.relative_to(path))] = row

    visit(path)
    return result


@contextmanager
def owned_fixture(data, identity):
    if (data.stat().st_dev, data.stat().st_ino) != identity:
        raise ValueError('Certified fixture root changed')
    fd = os.open(data / '.pg_lisp.lock', os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid():
            raise ValueError('Invalid fixture lock')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        named = (data / '.pg_lisp.lock').lstat()
        if (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino):
            raise ValueError('Fixture lock was replaced')
        yield
    finally:
        os.close(fd)


@contextmanager
def working_namespaces(data, destination, source=None):
    """Retain failed working images; restore original inodes after the child exits."""
    identity = (data.stat().st_dev, data.stat().st_ino)
    backup = destination / 'original'
    variant = destination / 'variant'
    backup.mkdir(parents=True)
    variant.mkdir()
    marker = data.parent / 'continuation-writer-owner.json'
    moved = []
    before = {}
    with owned_fixture(data, identity):
        with marker.open('x') as stream:
            json.dump(dict(pid=os.getpid(), root=str(data), identity=identity,
                           backup=str(backup), variant=str(variant)), stream)
        try:
            for name in NAMES:
                before[name] = tree_image(data / name)
                os.rename(data / name, backup / name)
                moved.append(name)
                original = (source / name) if source is not None else (backup / name)
                if original.is_symlink():
                    raise ValueError('Mutable fixture namespace is a symlink: ' + name)
                if original.is_dir():
                    shutil.copytree(original, data / name, symlinks=True)
                else:
                    shutil.copy2(original, data / name, follow_symlinks=False)
        except BaseException:
            for name in reversed(moved):
                if (data / name).exists():
                    os.rename(data / name, variant / name)
                os.rename(backup / name, data / name)
            marker.unlink()
            raise
    try:
        yield variant
    finally:
        # wait_child returns only after its process group is reaped on interruption.
        # A foreign owner prevents restoration and leaves marker/backups for recovery.
        with owned_fixture(data, identity):
            for name in NAMES:
                os.rename(data / name, variant / name)
                os.rename(backup / name, data / name)
            after = {name: tree_image(data / name) for name in NAMES}
            if before != after:
                raise ValueError('Original fixture was not restored exactly')
            (destination / 'preservation.json').write_text(json.dumps(
                dict(root=str(data), identity=identity, restored=True, images=after), indent=2))
            marker.unlink()


def main():
    directory = Path(__file__).resolve().parent
    campaign = module('campaign', directory / 'test-campaign.py')
    root = campaign.job_root(os.environ['PG_LISP_TEST_CAMPAIGN'])
    job = campaign.job_root()
    logs = Path(os.environ['PG_LISP_TEST_LOG_DIR'])
    data = Path(os.environ['PG_NATIVE_ACTIVE_PROOF_DATA']).resolve(strict=True)
    campaign.job_root(data.parents[2])
    with campaign.workspace_lease() as workspace, \
         campaign.lock(root, 'run.lock', exclusive=True) as run_lock, \
         campaign.lock(root, 'lease.lock') as lease:
        frozen = campaign.read_campaign(root)
        source = Path(frozen['source'])
        checkout, _ = campaign.checkout_state(Path(frozen['checkout']))
        if (frozen['state'] != 'open' or frozen['environment'] != campaign.environment()
                or frozen['checkout_state'] != checkout
                or frozen['image'] != campaign.fingerprint(source)
                or frozen['image'] != campaign.fingerprint(Path(frozen['checkout']))):
            raise ValueError('Native writer source or runtime changed')
        env = dict(os.environ, PG_NATIVE_HOST_DISPOSABLE='1', PG_NATIVE_PROOF_DATA=str(data),
                   PG_NATIVE_SELECTOR_REQUIRE_ACTIVE='1', PG_NATIVE_SELECTOR_ACTIVE_RESET='1',
                   PG_NATIVE_STARTUP_PUBLICATION='1', PG_NATIVE_RUNTIME_SOURCE='1',
                   PG_LISP_FROZEN_SOURCE=str(source))
        for key in ('PG_NATIVE_STARTUP_PUBLICATION_FAULTS', 'PG_NATIVE_COMPOSED_READER',
                    'PG_NATIVE_SELECTOR_CUT', 'PG_NATIVE_SELECTOR_RECOVER',
                    'PG_NATIVE_SELECTOR_ADVERSARIAL', 'PG_NATIVE_CONTINUATION_FORCE_SPILL'):
            env.pop(key, None)
        cases = os.environ.get('PG_NATIVE_WRITER_CASES', 'happy').split(',')
        if len(set(cases)) != len(cases) or any(not case.replace('-', '').isalnum() for case in cases):
            raise ValueError('Invalid or duplicate writer case')
        script = str(source / 't/imported-host-startup.lisp')
        quoted = '"' + script.replace('\\', '\\\\').replace('"', '\\"') + '"'
        load = '(handler-bind ((warning (lambda (c) (unless (typep c \'sb-kernel:redefinition-warning) (error c))))) (load ' + quoted + '))'
        results = []
        for case in cases:
            log = logs / ('writer-' + case + '.log')
            start = time.monotonic()
            with working_namespaces(data, job / ('writer-' + case)) as variant:
                status = campaign.wait_child(
                    [env.get('SBCL', 'sbcl'), '--noinform', '--no-sysinit', '--no-userinit',
                     '--non-interactive', '--eval', load],
                    dict(env, PG_NATIVE_WRITER_CASE=case), log, lease, run_lock, workspace)
            results.append(dict(case=case, exit_code=status, wall_seconds=time.monotonic() - start,
                                log=str(log), variant=str(variant), source=frozen['image']))
            campaign.write_json(logs / 'native-writer-results.json', results)
            if status or 'NATIVE-WRITER PASS case=' + case not in log.read_text(errors='replace'):
                raise RuntimeError('Writer case failed: ' + str(log))
        print('CONTINUATION-NATIVE-WRITER PASS ' + json.dumps(results), flush=True)


if __name__ == '__main__':
    main()

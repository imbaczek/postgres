"""Build a real descendant timeline in an isolated view of a certified root."""

import json
import os
from pathlib import Path
import re
import shutil
import time

from importlib.util import module_from_spec, spec_from_file_location


def module(name, path):
    spec = spec_from_file_location(name, path)
    result = module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def merge_certified_archive(original, working):
    """Restore immutable old objects without changing a promoted archive object."""
    for item in original.iterdir():
        if (item.is_symlink() or not item.is_file() or not re.fullmatch(
                r'(?:(?:extent|overlap)-[0-9A-F]{16}\.wal|inventory-[0-9A-F]{16}\.index)', item.name)):
            raise ValueError('Unexpected certified archive object')
        target = working / item.name
        if os.path.lexists(target):
            if target.is_symlink() or not target.is_file() or target.read_bytes() != item.read_bytes():
                raise ValueError('Certified archive object conflicts with promoted archive')
        else:
            shutil.copy2(item, target)


def retire_new_root_entries(data, initial_names, output, writer, identity):
    output.mkdir()
    with writer.owned_fixture(data, identity):
        for name in set(os.listdir(data)) - initial_names:
            os.rename(data / name, output / name)


def main():
    directory = Path(__file__).resolve().parent
    campaign = module('descendant_campaign', directory / 'test-campaign.py')
    writer = module('descendant_writer', directory / 'continuation-native-writer.py')
    root = campaign.job_root(os.environ['PG_LISP_TEST_CAMPAIGN'])
    job = campaign.job_root()
    logs = Path(os.environ['PG_LISP_TEST_LOG_DIR'])
    data = Path(os.environ['PG_NATIVE_ACTIVE_PROOF_DATA']).resolve(strict=True)
    campaign.job_root(data.parents[2])
    variant_root = campaign.job_root(os.environ['PG_NATIVE_COLD_VARIANT_ROOT'])
    variant = variant_root / 'writer-prepared-durable/variant'
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
            raise ValueError('Descendant source or runtime changed')
        env = dict(os.environ, PG_NATIVE_HOST_DISPOSABLE='1', PG_NATIVE_PROOF_DATA=str(data),
                   PG_LISP_FROZEN_SOURCE=str(source), PG_NATIVE_COLD_CASE='prepared-durable')
        for key in tuple(env):
            if key.startswith(('PG_NATIVE_SELECTOR_', 'PG_NATIVE_WRITER_',
                               'PG_NATIVE_COLD_', 'PG_NATIVE_DESCENDANT_')):
                env.pop(key)
        env['PG_NATIVE_COLD_CASE'] = 'prepared-durable'
        root_info = data.stat()
        identity = (root_info.st_dev, root_info.st_ino)
        before = writer.tree_image(data)
        variant_before = writer.tree_image(variant)
        initial_names = set(os.listdir(data))
        extras = tuple(sorted(initial_names - set(writer.NAMES) - {'base', '.pg_lisp.lock'}))
        backup = job / 'descendant-backup'
        backup.mkdir()
        env['PG_NATIVE_DESCENDANT_BACKUP'] = str(backup)
        results = []

        def child(phase, script, environment, marker):
            log = logs / ('descendant-' + phase + '.log')
            quoted = json.dumps(str(source / 't' / script))
            load = ('(handler-bind ((warning (lambda (c) (unless '
                    '(typep c \'sb-kernel:redefinition-warning) (error c))))) (load ' + quoted + '))')
            start = time.monotonic()
            status = campaign.wait_child(
                [env.get('SBCL', 'sbcl'), '--noinform', '--no-sysinit', '--no-userinit',
                 '--non-interactive', '--eval', load], environment, log, lease, run_lock, workspace)
            passed = status == 0 and marker in log.read_text(errors='replace')
            results.append(dict(phase=phase, exit_code=status, passed=passed,
                                wall_seconds=time.monotonic() - start, log=str(log)))
            campaign.write_json(logs / 'native-descendant-results.json', results)
            if not passed:
                raise RuntimeError('Descendant phase failed: ' + str(log))

        destination = job / 'descendant-working'
        try:
            with writer.working_namespaces(data, destination, source=variant,
                                           include_base=True, extra_names=extras):
                try:
                    child('backup', 'continuation-native-descendant.lisp',
                          dict(env, PG_NATIVE_DESCENDANT_PHASE='backup'),
                          'NATIVE-DESCENDANT BACKUP PASS')
                    overlay = job / 'pre-backup-overlay'
                    overlay.mkdir()
                    for item in backup.iterdir():
                        target = data / item.name
                        if os.path.lexists(target):
                            os.rename(target, overlay / item.name)
                        if item.is_dir():
                            shutil.copytree(item, target)
                        else:
                            shutil.copy2(item, target)
                    for name in ('pg_physical_receive', 'pg_physical_restartpoints',
                                 'pg_physical_pins', 'physical-follow.control',
                                 'physical-restartpoint.control'):
                        target = data / name
                        if os.path.lexists(target):
                            os.rename(target, overlay / name)
                    for phase in ('intent', 'complete'):
                        stash = job / ('descendant-stash-' + phase)
                        stash.mkdir()
                        child(phase, 'continuation-native-descendant.lisp',
                              dict(env, PG_NATIVE_DESCENDANT_PHASE=phase,
                                   PG_NATIVE_DESCENDANT_STASH=str(stash)),
                              'NATIVE-DESCENDANT ' + phase.upper() + ' PASS')
                    promoted = {name: (data / name).read_bytes()
                                for name in ('global/pg_wal_control', 'pg_wal/00000003.history')}
                    promoted_archive = writer.tree_image(data / 'pg_physical_retained')
                    merge_certified_archive(destination / 'original/pg_physical_retained',
                                            data / 'pg_physical_retained')
                    after_archive = writer.tree_image(data / 'pg_physical_retained')
                    if any(after_archive.get(name) != image for name, image in promoted_archive.items()):
                        raise ValueError('Archive union changed a promoted retained object')
                    if any((data / name).read_bytes() != image for name, image in promoted.items()):
                        raise ValueError('Archive union changed genuine descendant control or history')
                    for item in (job / 'descendant-stash-complete').iterdir():
                        target = data / item.name
                        if os.path.lexists(target):
                            raise ValueError('Promotion replaced a stashed proof name')
                        os.rename(item, target)
                    for phase in ('stable-empty', 'absent-empty'):
                        if phase == 'absent-empty':
                            (data / 'pg_logical_continuation/selector').unlink()
                        child(phase, 'imported-host-startup.lisp',
                              dict(env, PG_NATIVE_COLD_PROOF='load-descendant-empty'),
                              'NATIVE-COLD-DESCENDANT PASS')
                    selector = data / 'pg_logical_continuation/selector'
                    empty_selector = selector.read_bytes()
                    stable = variant_root / 'writer-stable-durable/variant'
                    for phase in ('prepared', 'child', 'timeline'):
                        try:
                            if phase == 'prepared':
                                shutil.copy2(variant / 'pg_logical_continuation/selector', selector)
                            elif phase == 'child':
                                shutil.copy2(stable / 'pg_logical_continuation/selector', selector)
                                for name in ('alpha', 'beta'):
                                    shutil.copytree(stable / 'pg_replslot' / name,
                                                    data / 'pg_replslot' / name)
                            child('refuse-' + phase, 'imported-host-startup.lisp',
                                  dict(env, PG_NATIVE_COLD_PROOF='load-descendant-' + phase),
                                  'NATIVE-COLD-DESCENDANT-REFUSAL PASS')
                        finally:
                            selector.write_bytes(empty_selector)
                            if phase == 'child':
                                saved = job / 'descendant-rejected-children'
                                saved.mkdir()
                                for name in ('alpha', 'beta'):
                                    candidate = data / 'pg_replslot' / name
                                    if os.path.lexists(candidate):
                                        os.rename(candidate, saved / name)
                finally:
                    # Preserve new root entries for inspection without leaving them
                    # beside the restored original namespaces.
                    extra_output = job / 'descendant-new-root-entries'
                    retire_new_root_entries(data, initial_names, extra_output, writer, identity)
        finally:
            restored = writer.tree_image(data) == before
            campaign.write_json(logs / 'descendant-original-preservation.json',
                                dict(root=str(data), restored=restored))
            if not restored:
                raise ValueError('Descendant experiment did not restore the original fixture')
            if writer.tree_image(variant) != variant_before:
                raise ValueError('Preserved writer variant changed')
        print('CONTINUATION-NATIVE-DESCENDANT PASS ' + json.dumps(results), flush=True)


if __name__ == '__main__':
    main()

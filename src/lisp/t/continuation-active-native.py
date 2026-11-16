import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import time


def file_image(path):
    info = path.lstat()
    if path.is_symlink() or not path.is_file():
        raise ValueError('Expected an ordinary fixture file: ' + str(path))
    return dict(device=info.st_dev, inode=info.st_ino, mode=info.st_mode,
                bytes=info.st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def fixture_image(data):
    result = {}
    for name in ('logical-slot-copy.source-proof', 'logical-slot-copy.import',
                 'logical-slot-copy.install/journal', 'logical-slot-copy.install/active'):
        result[name] = file_image(data / name)
    root = data / 'pg_replslot'
    for slot in sorted(root.iterdir()):
        if slot.name == '.logical-slot-copy.work':
            continue
        if slot.is_symlink() or not slot.is_dir():
            raise ValueError('Expected a canonical slot directory')
        result[str(slot.relative_to(data))] = [slot.stat().st_dev, slot.stat().st_ino]
        for path in sorted(slot.iterdir()):
            result[str(path.relative_to(data))] = file_image(path)
    for path in sorted((data / 'pg_wal').iterdir()):
        if path.is_file():
            result[str(path.relative_to(data))] = file_image(path)
    return result


def main():
    directory = Path(__file__).resolve().parent
    spec = importlib.util.spec_from_file_location('campaign', directory / 'test-campaign.py')
    campaign = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(campaign)
    root = campaign.job_root(os.environ['PG_LISP_TEST_CAMPAIGN'])
    campaign.job_root()
    logs = Path(os.environ['PG_LISP_TEST_LOG_DIR'])
    data = Path(os.environ['PG_NATIVE_ACTIVE_PROOF_DATA']).resolve(strict=True)
    campaign.job_root(data.parents[2])
    before = fixture_image(data)
    identity = (data.stat().st_dev, data.stat().st_ino)
    selected = data / 'pg_logical_continuation/selector'
    results = []
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
            raise ValueError('Native campaign source or runtime changed')
        env = dict(os.environ, PG_NATIVE_HOST_DISPOSABLE='1',
                   PG_NATIVE_PROOF_DATA=str(data), PG_NATIVE_SELECTOR_REQUIRE_ACTIVE='1',
                   PG_LISP_FROZEN_SOURCE=str(source))
        for key in ('PG_NATIVE_SELECTOR_CUT', 'PG_NATIVE_SELECTOR_RECOVER',
                    'PG_NATIVE_SELECTOR_ADVERSARIAL', 'PG_NATIVE_CONTINUATION_FORCE_SPILL',
                    'PG_NATIVE_SELECTOR_ACTIVE_RESET'):
            env.pop(key, None)
        stable = None
        script = str(source / 't/imported-host-startup.lisp')
        quoted_script = '"' + script.replace('\\', '\\\\').replace('"', '\\"') + '"'
        load_script = (
            '(handler-bind ((warning (lambda (condition) '
            '(unless (typep condition \'sb-kernel:redefinition-warning) '
            '(error condition))))) (load ' + quoted_script + '))'
        )
        for label in ('initial', 'cold-readback'):
            log = logs / ('active-' + label + '.log')
            start = time.monotonic()
            used = resource.getrusage(resource.RUSAGE_CHILDREN)
            child_env = dict(env)
            if label == 'initial':
                child_env['PG_NATIVE_SELECTOR_ACTIVE_RESET'] = '1'
            if label == 'cold-readback':
                child_env['PG_NATIVE_CONTINUATION_FORCE_SPILL'] = '1'
            status = campaign.wait_child([env.get('SBCL', 'sbcl'), '--noinform',
                                          '--no-sysinit', '--no-userinit',
                                          '--non-interactive', '--eval', load_script],
                                         child_env, log, lease, run_lock, workspace)
            final = resource.getrusage(resource.RUSAGE_CHILDREN)
            results.append(dict(label=label, exit_code=status, log=str(log),
                                forced_spill=label == 'cold-readback',
                                wall_seconds=time.monotonic() - start,
                                cpu_seconds=final.ru_utime + final.ru_stime - used.ru_utime - used.ru_stime,
                                child=json.loads((logs / 'child.log').read_text())))
            campaign.write_json(logs / 'active-native-results.json', results)
            assert status == 0, str(log)
            contents = log.read_text(errors='replace')
            assert 'NATIVE-SELECTOR-ACTIVE-REFUSAL PASS' in contents
            if os.environ.get('PG_NATIVE_STARTUP_PUBLICATION'):
                assert 'NATIVE-STARTUP-PUBLICATION PASS' in contents
            if os.environ.get('PG_NATIVE_STARTUP_PUBLICATION_FAULTS'):
                assert 'NATIVE-STARTUP-PUBLICATION-POSTCOMMIT PASS' in contents
            if os.environ.get('PG_NATIVE_RUNTIME_SOURCE'):
                for marker in ('NATIVE-RUNTIME-SOURCE PASS',
                               'NATIVE-RUNTIME-BOUNDED-SOURCE PASS',
                               'NATIVE-RUNTIME-BOUNDED-LANES PASS',
                               'NATIVE-RUNTIME-SLOT-RESOURCES PASS',
                               'NATIVE-STARTUP-SLOT-RESOURCES-REFUSED PASS',
                               'NATIVE-RUNTIME-TAIL-MUTATIONS PASS',
                               'NATIVE-RUNTIME-ACK-FENCED PASS',
                               'NATIVE-RUNTIME-TRANSACTIONS PASS slot=alpha',
                               'NATIVE-RUNTIME-TRANSACTIONS PASS slot=beta',
                               'NATIVE-RUNTIME-CURSOR PASS slot=alpha',
                               'NATIVE-RUNTIME-CURSOR PASS slot=beta'):
                    assert marker in contents, (marker, str(log))
            assert identity == (data.stat().st_dev, data.stat().st_ino)
            assert before == fixture_image(data)
            image = file_image(selected)
            if stable is not None:
                assert stable == image
            stable = image
        summary = dict(exit_code=0, source=frozen['image'], fixture=str(data),
                       fixture_identity=identity, preserved_files=before, selector=stable,
                       runs=results,
                       limits=('Imported runtime source and composed cursor library; automatic loading, ACK publication, admission and GC remain closed'
                               if os.environ.get('PG_NATIVE_RUNTIME_SOURCE') else
                               'Startup registry publication under the owned gate; admission and GC remain closed'
                               if os.environ.get('PG_NATIVE_STARTUP_PUBLICATION') else
                               'Active initial selector proof; registry, admission and GC remain closed'))
        campaign.write_json(logs / 'active-native-summary.json', summary)
        print('CONTINUATION-ACTIVE-NATIVE PASS ' + json.dumps(summary), flush=True)


if __name__ == '__main__':
    main()

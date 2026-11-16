"""Classify preserved native writer cuts under a fresh, owned startup."""

import json
import os
from pathlib import Path
import time

import importlib.util


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def main():
    directory = Path(__file__).resolve().parent
    campaign = module('campaign', directory / 'test-campaign.py')
    writer = module('writer', directory / 'continuation-native-writer.py')
    root = campaign.job_root(os.environ['PG_LISP_TEST_CAMPAIGN'])
    job = campaign.job_root()
    logs = Path(os.environ['PG_LISP_TEST_LOG_DIR'])
    data = Path(os.environ['PG_NATIVE_ACTIVE_PROOF_DATA']).resolve(strict=True)
    variants = campaign.job_root(os.environ['PG_NATIVE_COLD_VARIANT_ROOT'])
    campaign.job_root(data.parents[2])
    definitions = {
        'happy': ('happy', 3, 'STABLE', 'STABLE', False),
        'happy-extended': ('happy', 3, 'STABLE', 'STABLE', True),
        'prepared-temporary': ('prepared-temporary', 1, 'STABLE', 'STABLE', False),
        'prepared-durable': ('prepared-durable', 1, 'PREPARED', 'OLD', False),
        'p7-temporary': ('p7-temporary', 1, 'PREPARED', 'OLD', False),
        'p7-durable': ('p7-durable', 2, 'PREPARED', 'NEW', False),
        'stable-temporary': ('stable-temporary', 2, 'PREPARED', 'NEW', False),
        'stable-durable': ('stable-durable', 2, 'STABLE', 'STABLE', False),
        'stable-durable-extended': ('stable-durable', 2, 'STABLE', 'STABLE', True),
    }
    cases = os.environ.get('PG_NATIVE_COLD_CASES', ','.join(definitions)).split(',')
    definitions['parent-before-D'] = ('parent-before-D', 2, 'STABLE', 'STABLE', False)
    definitions['happy-abandoned'] = ('happy-abandoned', 3, 'STABLE', 'STABLE', False)
    faults = {'corrupt-selector': 'stable-durable', 'changed-beta': 'stable-durable',
              'stable-state-temp': 'stable-durable', 'unrelated-state-temp': 'prepared-durable',
              'wrong-prefix': 'p7-temporary', 'protected': 'stable-durable',
              'callback': 'stable-durable', 'after-observation': 'stable-durable',
              'after-proof-return': 'stable-durable', 'source-close': 'stable-durable',
              'reservation-returned': 'stable-durable', 'caught-recheck-failure': 'stable-durable'}
    for fault, variant in faults.items():
        definitions[fault] = definitions[variant]
    if len(set(cases)) != len(cases) or any(case not in definitions for case in cases):
        raise ValueError('Invalid cold case inventory')
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
            raise ValueError('Native cold source or runtime changed')
        env = dict(os.environ, PG_NATIVE_HOST_DISPOSABLE='1', PG_NATIVE_PROOF_DATA=str(data),
                   PG_LISP_FROZEN_SOURCE=str(source))
        for key in tuple(env):
            if key.startswith(('PG_NATIVE_SELECTOR_', 'PG_NATIVE_WRITER_',
                               'PG_NATIVE_STARTUP_PUBLICATION', 'PG_NATIVE_RUNTIME_SOURCE',
                               'PG_NATIVE_COMPOSED_READER', 'PG_NATIVE_CONTINUATION_FORCE_SPILL')):
                env.pop(key)
        script = str(source / 't/imported-host-startup.lisp')
        quoted = '"' + script.replace('\\', '\\\\').replace('"', '\\"') + '"'
        load = '(handler-bind ((warning (lambda (c) (unless (typep c \'sb-kernel:redefinition-warning) (error c))))) (load ' + quoted + '))'
        results = []
        for case in cases:
            variant_case, generation, kind, choice, append = definitions[case]
            variant = variants / ('writer-' + variant_case) / 'variant'
            before = writer.tree_image(variant)
            log = logs / ('cold-' + case + '.log')
            child_env = dict(env, PG_NATIVE_COLD_CASE=case,
                             PG_NATIVE_COLD_GENERATION=str(generation),
                             PG_NATIVE_COLD_KIND=kind, PG_NATIVE_COLD_CHOICE=choice)
            child_env.pop('PG_NATIVE_COLD_FAULT', None)
            if case in faults:
                child_env['PG_NATIVE_COLD_FAULT'] = case
            if append:
                child_env['PG_NATIVE_COLD_APPEND'] = '1'
            else:
                child_env.pop('PG_NATIVE_COLD_APPEND', None)
            start = time.monotonic()
            with writer.working_namespaces(data, job / ('cold-' + case), source=variant) as observed:
                status = campaign.wait_child(
                    [env.get('SBCL', 'sbcl'), '--noinform', '--no-sysinit', '--no-userinit',
                     '--non-interactive', '--eval', load],
                    child_env, log, lease, run_lock, workspace)
            assert before == writer.tree_image(variant), 'Preserved writer variant changed'
            results.append(dict(case=case, exit_code=status, wall_seconds=time.monotonic() - start,
                                log=str(log), input_variant=str(variant), observed=str(observed),
                                source=frozen['image'], proof_mode=env.get('PG_NATIVE_COLD_PROOF')))
            campaign.write_json(logs / 'native-cold-results.json', results)
            marker = {'parent-boundaries': 'NATIVE-COLD-PARENT-BOUNDARIES',
                      'selected-boundaries': 'NATIVE-COLD-SELECTED-BOUNDARIES',
                      None: 'NATIVE-COLD-CLASSIFICATION'}[env.get('PG_NATIVE_COLD_PROOF')] + ' PASS case=' + case
            if status or marker not in log.read_text(errors='replace'):
                raise RuntimeError('Cold case failed: ' + str(log))
        print('CONTINUATION-NATIVE-COLD PASS ' + json.dumps(results), flush=True)


if __name__ == '__main__':
    main()

"""Classify preserved native writer cuts under a fresh, owned startup."""

import importlib.util
import json
import os
from pathlib import Path
import re
import time


DEFAULT_CASES = {
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
FAULTS = {'corrupt-selector': 'stable-durable', 'changed-beta': 'stable-durable',
          'stable-state-temp': 'stable-durable', 'unrelated-state-temp': 'prepared-durable',
          'wrong-prefix': 'p7-temporary', 'protected': 'stable-durable',
          'callback': 'stable-durable', 'after-observation': 'stable-durable',
          'after-proof-return': 'stable-durable', 'source-close': 'stable-durable',
          'reservation-returned': 'stable-durable', 'caught-recheck-failure': 'stable-durable'}
CASE_DEFINITIONS = {
    **DEFAULT_CASES,
    'parent-before-D': ('parent-before-D', 2, 'STABLE', 'STABLE', False),
    'happy-abandoned': ('happy-abandoned', 3, 'STABLE', 'STABLE', False),
}
CASE_DEFINITIONS.update({fault: DEFAULT_CASES[variant] for fault, variant in FAULTS.items()})
PROOF_MARKERS = {
    'parent-boundaries': 'NATIVE-COLD-PARENT-BOUNDARIES',
    'selected-boundaries': 'NATIVE-COLD-SELECTED-BOUNDARIES',
    'pending': 'NATIVE-COLD-PENDING',
    'policy': 'NATIVE-COLD-PENDING',
    'prepare': 'NATIVE-COLD-PREPARATION',
    'prepare-wal': 'NATIVE-COLD-PREPARATION-WAL-REFUSAL',
    'prepare-registration': 'NATIVE-COLD-PREPARATION-REFUSAL',
    'prepare-pending': 'NATIVE-COLD-PREPARATION-REFUSAL',
    'prepare-pending-swap': 'NATIVE-COLD-PREPARATION-REFUSAL',
    'prepare-reuse': 'NATIVE-COLD-PREPARATION-REFUSAL',
    'prepare-reenter': 'NATIVE-COLD-PREPARATION-REFUSAL',
    'prepare-capacity': 'NATIVE-COLD-PREPARATION-REFUSAL',
    'prepare-after-close': 'NATIVE-COLD-PREPARATION-REFUSAL',
    'policy-off': 'NATIVE-COLD-POLICY-REFUSAL',
    'policy-off-on': 'NATIVE-COLD-POLICY-REFUSAL',
    'pending-registration': 'NATIVE-COLD-PENDING-REFUSAL',
    'pending-candidate': 'NATIVE-COLD-PENDING-REFUSAL',
    'pending-temp-swap': 'NATIVE-COLD-PENDING-REFUSAL',
    None: 'NATIVE-COLD-CLASSIFICATION',
}


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def cold_plan(environment, campaign):
    path = environment.get('PG_NATIVE_COLD_PLAN')
    if path is not None:
        with open(path, 'rb') as stream:
            raw = stream.read(262145)
        if len(raw) > 262144:
            raise ValueError('Cold plan exceeds its size limit')
        entries = json.loads(raw)
    else:
        entries = [{'case': case, 'label': case}
                   for case in environment.get('PG_NATIVE_COLD_CASES', ','.join(DEFAULT_CASES)).split(',')]
    if not isinstance(entries, list) or not 1 <= len(entries) <= 128:
        raise ValueError('Expected between one and 128 cold plan entries')
    result, labels, roots = [], set(), {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) - {'case', 'proof', 'label', 'variant_root', 'spill'}:
            raise ValueError('Invalid cold plan entry fields')
        case = entry.get('case')
        proof = entry.get('proof', environment.get('PG_NATIVE_COLD_PROOF'))
        spill = entry.get('spill', 'PG_NATIVE_COLD_SPILL' in environment)
        root = entry.get('variant_root', environment.get('PG_NATIVE_COLD_VARIANT_ROOT'))
        if not isinstance(case, str) or case not in CASE_DEFINITIONS:
            raise ValueError('Unknown cold case')
        if (proof is not None and not isinstance(proof, str)) or proof not in PROOF_MARKERS:
            raise ValueError('Unknown cold proof mode')
        if case in FAULTS and proof is not None:
            raise ValueError('Cold classification faults require the classification proof mode')
        if not isinstance(spill, bool) or not isinstance(root, str) or not root:
            raise ValueError('Invalid cold spill flag or variant root')
        label = entry.get('label', (proof or 'classification') + '-' + case + ('-spill' if spill else ''))
        if not isinstance(label, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]{0,79}', label) or label in labels:
            raise ValueError('Invalid or duplicate cold output label')
        labels.add(label)
        if root not in roots:
            roots[root] = str(campaign.job_root(root))
        result.append(dict(case=case, proof=proof, label=label, variant_root=roots[root], spill=spill))
    return result


def child_environment(base, row):
    _, generation, kind, choice, append = CASE_DEFINITIONS[row['case']]
    env = dict(base, PG_NATIVE_COLD_CASE=row['case'], PG_NATIVE_COLD_GENERATION=str(generation),
               PG_NATIVE_COLD_KIND=kind, PG_NATIVE_COLD_CHOICE=choice,
               PG_NATIVE_COLD_VARIANT_ROOT=row['variant_root'])
    env.pop('PG_NATIVE_COLD_PLAN', None)
    for key, value in (('PG_NATIVE_COLD_PROOF', row['proof']),
                       ('PG_NATIVE_COLD_SPILL', '1' if row['spill'] else None),
                       ('PG_NATIVE_COLD_APPEND', '1' if append else None),
                       ('PG_NATIVE_COLD_FAULT', row['case'] if row['case'] in FAULTS else None)):
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


def main():
    directory = Path(__file__).resolve().parent
    campaign = module('campaign', directory / 'test-campaign.py')
    writer = module('writer', directory / 'continuation-native-writer.py')
    root = campaign.job_root(os.environ['PG_LISP_TEST_CAMPAIGN'])
    job = campaign.job_root()
    logs = Path(os.environ['PG_LISP_TEST_LOG_DIR'])
    data = Path(os.environ['PG_NATIVE_ACTIVE_PROOF_DATA']).resolve(strict=True)
    campaign.job_root(data.parents[2])
    plan = cold_plan(os.environ, campaign)
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
        campaign.write_json(logs / 'native-cold-plan.json', plan)
        results = []
        for row in plan:
            case, label = row['case'], row['label']
            variant_case = CASE_DEFINITIONS[case][0]
            variant = Path(row['variant_root']) / ('writer-' + variant_case) / 'variant'
            before = writer.tree_image(variant)
            log = logs / ('cold-' + label + '.log')
            child_env = child_environment(env, row)
            start = time.monotonic()
            with writer.working_namespaces(data, job / ('cold-' + label), source=variant) as observed:
                status = campaign.wait_child(
                    [env.get('SBCL', 'sbcl'), '--noinform', '--no-sysinit', '--no-userinit',
                     '--non-interactive', '--eval', load],
                    child_env, log, lease, run_lock, workspace)
            assert before == writer.tree_image(variant), 'Preserved writer variant changed'
            marker = PROOF_MARKERS[row['proof']] + ' PASS case=' + case
            passed = status == 0 and re.search(
                '^' + re.escape(marker) + r'(?:\s|$)', log.read_text(errors='replace'), re.MULTILINE) is not None
            results.append(dict(case=case, exit_code=status, wall_seconds=time.monotonic() - start,
                                log=str(log), input_variant=str(variant), observed=str(observed),
                                source=frozen['image'], proof_mode=row['proof'], label=label,
                                spill=row['spill'], passed=passed))
            campaign.write_json(logs / 'native-cold-results.json', results)
            if not passed:
                raise RuntimeError('Cold case failed: ' + str(log))
        print('CONTINUATION-NATIVE-COLD PASS ' + json.dumps(results), flush=True)


if __name__ == '__main__':
    main()

"""Classify preserved native writer cuts under a fresh, owned startup."""

import importlib.util
import json
import os
from pathlib import Path
import re
import signal
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
    'load': 'NATIVE-COLD-PUBLICATION',
    'load-bootstrap': 'NATIVE-COLD-PUBLICATION',
    'load-bootstrap-prefix': 'NATIVE-COLD-PUBLICATION',
    'load-empty': 'NATIVE-COLD-EMPTY-LOADER',
    'load-empty-late': 'NATIVE-COLD-EMPTY-LOADER-REFUSAL',
    'load-mixed-refusal': 'NATIVE-COLD-LOADER-REFUSAL',
    'load-mixed-staging-refusal': 'NATIVE-COLD-LOADER-REFUSAL',
    'publish': 'NATIVE-COLD-PUBLICATION',
    'publish-after-group-pin': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-maps-ready': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-derived': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-prepared': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-before-commit': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-after-commit': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-owner-return': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-close': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-notify': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-before-retirement': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'publish-lost-registration': 'NATIVE-COLD-PUBLICATION-REFUSAL',
    'reconcile': 'NATIVE-COLD-RECONCILIATION',
    'reconcile-reenter': 'NATIVE-COLD-RECONCILIATION-REFUSAL',
    'reconcile-after-unlink': 'NATIVE-COLD-RECONCILIATION-REFUSAL',
    'reconcile-after-rename': 'NATIVE-COLD-RECONCILIATION-REFUSAL',
    'reconcile-after-close': 'NATIVE-COLD-RECONCILIATION-REFUSAL',
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
PROOF_MARKERS.update({'load-' + proof.removeprefix('publish-'): marker
                      for proof, marker in tuple(PROOF_MARKERS.items())
                      if isinstance(proof, str) and proof.startswith('publish-')})
COLD_RECONCILIATION_CUTS = {
    'before-state-sync', 'after-state-sync', 'slot-state-after-unlink',
    'slot-state-after-directory-sync', 'after-slot-root-sync', 'selector-after-unlink',
    'selector-after-directory-sync', 'selector-temporary-durable', 'stable-temporary',
    'stable-selected', 'replacement-after-selected-sync',
    'replacement-after-directory-sync', 'stable-durable',
}
COLD_PUBLICATION_CUTS = {
    'completion-ready', 'before-group-pin', 'after-group-pin', 'maps-ready',
    'derived', 'prepared', 'before-commit', 'after-commit', 'before-owner-return',
    'before-old-pin-retirement', 'completed',
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
    # Validate every row and owned variant root before starting any child.
    result, labels, roots = [], set(), {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) - {'case', 'proof', 'label', 'variant_root', 'spill', 'cut'}:
            raise ValueError('Invalid cold plan entry fields')
        case = entry.get('case')
        proof = entry.get('proof', environment.get('PG_NATIVE_COLD_PROOF'))
        spill = entry.get('spill', 'PG_NATIVE_COLD_SPILL' in environment)
        root = entry.get('variant_root', environment.get('PG_NATIVE_COLD_VARIANT_ROOT'))
        cut = entry.get('cut')
        if not isinstance(case, str) or case not in CASE_DEFINITIONS:
            raise ValueError('Unknown cold case')
        if (proof is not None and not isinstance(proof, str)) or proof not in PROOF_MARKERS:
            raise ValueError('Unknown cold proof mode')
        if case in FAULTS and proof is not None:
            raise ValueError('Cold classification faults require the classification proof mode')
        cuts = {'reconcile': COLD_RECONCILIATION_CUTS, 'publish': COLD_PUBLICATION_CUTS,
                'load': COLD_PUBLICATION_CUTS}.get(proof, set())
        if cut is not None and (not isinstance(cut, str) or cut not in cuts):
            raise ValueError('Invalid cold crash cut')
        if not isinstance(spill, bool) or not isinstance(root, str) or not root:
            raise ValueError('Invalid cold spill flag or variant root')
        label = entry.get('label', (proof or 'classification') + '-' + case
                          + ('-spill' if spill else '') + ('-' + cut if cut else ''))
        if not isinstance(label, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]{0,79}', label) or label in labels:
            raise ValueError('Invalid or duplicate cold output label')
        labels.add(label)
        if root not in roots:
            roots[root] = str(campaign.job_root(root))
        result.append(dict(case=case, proof=proof, label=label, variant_root=roots[root], spill=spill, cut=cut))
    return result


def child_environment(base, row):
    _, generation, kind, choice, append = CASE_DEFINITIONS[row['case']]
    env = dict(base, PG_NATIVE_COLD_CASE=row['case'], PG_NATIVE_COLD_GENERATION=str(generation),
               PG_NATIVE_COLD_KIND=kind, PG_NATIVE_COLD_CHOICE=choice,
               PG_NATIVE_COLD_VARIANT_ROOT=row['variant_root'])
    env.pop('PG_NATIVE_COLD_PLAN', None)
    # Do not let a previous or inherited mode leak into this row.
    for key, value in (('PG_NATIVE_COLD_PROOF', row['proof']),
                       ('PG_NATIVE_COLD_RECONCILE_CUT', row.get('cut')),
                       ('PG_NATIVE_COLD_SPILL', '1' if row['spill'] else None),
                       ('PG_NATIVE_COLD_APPEND', '1' if append else None),
                       ('PG_NATIVE_COLD_FAULT', row['case'] if row['case'] in FAULTS else None)):
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


def cold_spill_required(row):
    proof = row['proof'] or ''
    return row.get('spill', False) and (proof in {'prepare', 'pending', 'policy', 'reconcile', 'publish', 'load'}
                                      or proof.startswith(('publish-', 'reconcile-'))
                                      or (proof.startswith('load-') and proof not in
                                          {'load-empty', 'load-empty-late'}))


def cold_spill_marker(text):
    return re.search(r'^NATIVE-COLD-SPILL PASS transcripts=[1-9][0-9]* threshold=0 cleanup-complete$',
                     text, re.MULTILINE) is not None


def cold_pass_marker(row, text):
    marker = PROOF_MARKERS[row['proof']] + ' PASS case=' + row['case']
    return (re.search('^' + re.escape(marker) + r'(?:\s|$)', text, re.MULTILINE) is not None
            and (not cold_spill_required(row) or cold_spill_marker(text)))


def run_cold_children(campaign, command, env, row, log, lease, run_lock, workspace):
    status = campaign.wait_child(command, env, log, lease, run_lock, workspace)
    if not row.get('cut'):
        return status, log, None
    text = log.read_text(errors='replace')
    prefix = PROOF_MARKERS[row['proof']]
    expected_pattern = '^' + re.escape(prefix) + r'-EXPECTED ([0-9A-Fa-f]+)$'
    expected = re.findall(expected_pattern, text, re.MULTILINE)
    cut_marker = prefix + '-SIGKILL ' + row['cut']
    crash = dict(exit_code=status, log=str(log), restarts=[], passed=False)
    if (status != -signal.SIGKILL or cut_marker not in text.splitlines() or len(expected) != 1
            or (cold_spill_required(row) and 'NATIVE-COLD-SPILL-USED transcript threshold=0' not in text.splitlines())):
        return 1, log, crash
    recovery_env = dict(env)
    recovery_env.pop('PG_NATIVE_COLD_RECONCILE_CUT', None)
    recovery_env.pop('PG_NATIVE_COLD_APPEND', None)
    for attempt in range(1, 3):
        recovered_log = log.with_name(log.stem + '-restart-' + str(attempt) + log.suffix)
        status = campaign.wait_child(command, recovery_env, recovered_log, lease, run_lock, workspace)
        recovered_text = recovered_log.read_text(errors='replace')
        actual = re.findall(expected_pattern, recovered_text, re.MULTILINE)
        passed = status == 0 and actual == expected and cold_pass_marker(row, recovered_text)
        crash['restarts'].append(dict(exit_code=status, log=str(recovered_log), passed=passed))
        if not passed:
            return status or 1, recovered_log, crash
    crash['passed'] = True
    return status, recovered_log, crash


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
        # Each row gets a fresh image and working namespace; the job cache persists.
        for row in plan:
            case, label = row['case'], row['label']
            variant_case = CASE_DEFINITIONS[case][0]
            variant = Path(row['variant_root']) / ('writer-' + variant_case) / 'variant'
            before = writer.tree_image(variant)
            log = logs / ('cold-' + label + '.log')
            child_env = child_environment(env, row)
            start = time.monotonic()
            with writer.working_namespaces(data, job / ('cold-' + label), source=variant) as observed:
                status, log, crash = run_cold_children(campaign,
                    [env.get('SBCL', 'sbcl'), '--noinform', '--no-sysinit', '--no-userinit',
                     '--non-interactive', '--eval', load],
                    child_env, row, log, lease, run_lock, workspace)
            assert before == writer.tree_image(variant), 'Preserved writer variant changed'
            log_text = log.read_text(errors='replace')
            passed = status == 0 and cold_pass_marker(row, log_text)
            results.append(dict(case=case, exit_code=status, wall_seconds=time.monotonic() - start,
                                log=str(log), input_variant=str(variant), observed=str(observed),
                                source=frozen['image'], proof_mode=row['proof'], label=label,
                                spill=row['spill'], spill_exercised=cold_spill_marker(log_text),
                                cut=row.get('cut'), crash=crash, passed=passed))
            campaign.write_json(logs / 'native-cold-results.json', results)
            if not passed:
                raise RuntimeError('Cold case failed: ' + str(log))
        print('CONTINUATION-NATIVE-COLD PASS ' + json.dumps(results), flush=True)


if __name__ == '__main__':
    main()

"""Case-plan isolation and failure contracts for native cold campaigns."""

import contextlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    'native_cold', Path(__file__).with_name('continuation-native-cold.py'))
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)


class ColdPlanContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='cold-plan-', dir=os.environ['TMPDIR'])
        self.root = Path(self.temporary.name)
        self.variants = self.root / 'variants'
        self.plan_path = self.root / 'plan.json'
        self.environment = {'PG_NATIVE_COLD_VARIANT_ROOT': str(self.variants)}
        self.validated = []

    def tearDown(self):
        self.temporary.cleanup()

    def campaign(self):
        def owned(path):
            self.validated.append(path)
            return Path(path)
        return SimpleNamespace(job_root=owned)

    def plan(self, rows, **environment):
        self.plan_path.write_text(json.dumps(rows))
        env = dict(self.environment, PG_NATIVE_COLD_PLAN=str(self.plan_path), **environment)
        return native.cold_plan(env, self.campaign())

    def test_legacy_case_order_labels_and_presence_based_spill_are_preserved(self):
        env = dict(self.environment, PG_NATIVE_COLD_CASES='happy,parent-before-D',
                   PG_NATIVE_COLD_PROOF='prepare', PG_NATIVE_COLD_SPILL='0')
        rows = native.cold_plan(env, self.campaign())
        self.assertEqual(['happy', 'parent-before-D'], [row['label'] for row in rows])
        self.assertEqual(['prepare', 'prepare'], [row['proof'] for row in rows])
        self.assertTrue(all(row['spill'] for row in rows))

    def test_cleanup_plan_modes_do_not_claim_crash_coverage(self):
        for proof in ('plan', 'plan-bootstrap-prefix', 'plan-empty',
                      'plan-replaced', 'plan-caught', 'plan-close'):
            with self.subTest(proof=proof):
                row = self.plan([dict(case='stable-durable', proof=proof)])[0]
                self.assertIsNone(row['cut'])
                self.assertFalse(native.cold_spill_required(row))
                self.assertEqual('NATIVE-COLD-CLEANUP-PLAN', native.PROOF_MARKERS[proof])
                with self.assertRaises(ValueError):
                    self.plan([dict(case='stable-durable', proof=proof, cut='after-state-sync')])

    def test_same_case_in_different_proof_modes_has_distinct_outputs(self):
        other = self.root / 'other-variants'
        rows = self.plan([{'case': 'happy', 'proof': 'prepare'},
                          {'case': 'happy', 'proof': 'prepare-pending',
                           'variant_root': str(other), 'spill': True}])
        self.assertEqual(['prepare-happy', 'prepare-pending-happy-spill'],
                         [row['label'] for row in rows])
        self.assertEqual([str(self.variants), str(other)], self.validated)

    def test_complete_plan_rejects_invalid_or_colliding_entries(self):
        bad_rows = [[], {}, ['happy'], [{'case': 'missing'}],
                    [{'case': 'happy', 'proof': 'unknown'}],
                    [{'case': 'happy', 'proof': False}],
                    [{'case': 'corrupt-selector', 'proof': 'prepare'}],
                    [{'case': 'happy', 'spill': 'yes'}],
                    [{'case': 'happy', 'proof': 'prepare', 'cut': 'after-state-sync'}],
                    [{'case': 'happy', 'proof': 'reconcile', 'cut': 'unknown'}],
                    [{'case': 'happy', 'proof': 'reconcile', 'cut': False}],
                    [{'case': 'happy', 'proof': 'publish', 'cut': 'after-state-sync'}],
                    [{'case': 'happy', 'proof': 'reconcile', 'cut': 'after-commit'}],
                    [{'case': 'happy', 'label': '../outside'}],
                    [{'case': 'happy', 'label': ''}],
                    [{'case': 'happy', 'variant_root': 7}],
                    [{'case': 'happy', 'expected_kind': 'NEW'}],
                    [{'case': 'happy'}, {'case': 'happy'}],
                    [{'case': 'happy', 'label': str(index)} for index in range(129)]]
        for rows in bad_rows:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.plan(rows)

    def test_plan_file_size_is_bounded_before_parsing(self):
        self.plan_path.write_text(' ' * 262145)
        env = dict(self.environment, PG_NATIVE_COLD_PLAN=str(self.plan_path))
        with self.assertRaises(ValueError):
            native.cold_plan(env, self.campaign())

    def test_loader_modes_keep_crash_families_and_spill_requirements(self):
        rows = self.plan([{'case': 'happy', 'proof': 'load', 'cut': 'after-commit'},
                          {'case': 'happy', 'proof': 'load-close'},
                          {'case': 'happy', 'proof': 'load-bootstrap-prefix'},
                          {'case': 'happy', 'proof': 'load-empty'},
                          {'case': 'happy', 'proof': 'load-empty-late'}])
        self.assertEqual(5, len(rows))
        self.assertTrue(native.cold_spill_required(dict(rows[0], spill=True)))
        self.assertTrue(native.cold_spill_required(dict(rows[1], spill=True)))
        self.assertTrue(native.cold_spill_required(dict(rows[2], spill=True)))
        self.assertFalse(native.cold_spill_required(dict(rows[3], spill=True)))
        self.assertFalse(native.cold_spill_required(dict(rows[4], spill=True)))
        for proof, cut in [('load', 'after-state-sync'), ('load-close', 'after-commit'),
                           ('load-empty', 'after-commit')]:
            with self.subTest(proof=proof), self.assertRaises(ValueError):
                self.plan([{'case': 'happy', 'proof': proof, 'cut': cut}])

    def test_optional_child_flags_do_not_leak_between_rows(self):
        rows = self.plan([{'case': 'prepared-durable', 'proof': 'prepare-pending', 'spill': True},
                          {'case': 'happy', 'proof': None, 'spill': False},
                          {'case': 'happy-extended', 'proof': 'prepare'}])
        base = {'PG_NATIVE_COLD_APPEND': 'old', 'PG_NATIVE_COLD_FAULT': 'old',
                'PG_NATIVE_COLD_MIXED_RESTART': 'old',
                'PG_NATIVE_COLD_LOGICAL_PHASE': 'old',
                'PG_NATIVE_COLD_LOGICAL_SEED_IMAGE': 'old',
                'PG_NATIVE_COLD_LOGICAL_SEED_PENDING': 'old',
                'PG_NATIVE_COLD_PLAN': 'input-only', 'UNCHANGED': 'yes'}
        first = native.child_environment(base, rows[0])
        second = native.child_environment(first, rows[1])
        third = native.child_environment(second, rows[2])
        self.assertEqual('PREPARED', first['PG_NATIVE_COLD_KIND'])
        self.assertEqual('OLD', first['PG_NATIVE_COLD_CHOICE'])
        self.assertEqual('1', first['PG_NATIVE_COLD_GENERATION'])
        self.assertEqual('1', first['PG_NATIVE_COLD_SPILL'])
        for key in ('PG_NATIVE_COLD_PROOF', 'PG_NATIVE_COLD_SPILL', 'PG_NATIVE_COLD_APPEND',
                      'PG_NATIVE_COLD_FAULT', 'PG_NATIVE_COLD_PLAN',
                      'PG_NATIVE_COLD_MIXED_RESTART', 'PG_NATIVE_COLD_LOGICAL_PHASE',
                      'PG_NATIVE_COLD_LOGICAL_SEED_IMAGE', 'PG_NATIVE_COLD_LOGICAL_SEED_PENDING'):
            self.assertNotIn(key, second)
        self.assertNotIn('PG_NATIVE_COLD_MIXED_RESTART', first)
        self.assertNotIn('PG_NATIVE_COLD_LOGICAL_PHASE', first)
        self.assertEqual('1', third['PG_NATIVE_COLD_APPEND'])
        self.assertEqual('yes', third['UNCHANGED'])
        self.assertEqual('old', base['PG_NATIVE_COLD_FAULT'])

    def run_driver(self, *, failure=False):
        job = self.root / 'job'
        logs = job / 'logs'
        logs.mkdir(parents=True)
        data = self.root / 'fixture/tmp/session/data'
        data.mkdir(parents=True)
        self.plan_path.write_text(json.dumps([
            {'case': 'happy', 'proof': 'prepare'},
            {'case': 'happy', 'proof': 'prepare-pending', 'spill': True}]))
        environment = dict(self.environment, PG_LISP_TEST_CAMPAIGN=str(self.root / 'campaign'),
                           PG_LISP_TEST_LOG_DIR=str(logs), PG_NATIVE_ACTIVE_PROOF_DATA=str(data),
                           PG_NATIVE_COLD_PLAN=str(self.plan_path), XDG_CACHE_HOME=str(job / 'cache'))
        events, children = [], []
        image = {'sha256': 'source', 'files': 1}
        frozen = dict(state='open', source=str(self.root / 'source'),
                      checkout=str(self.root / 'checkout'), checkout_state={},
                      image=image, environment={})

        @contextlib.contextmanager
        def namespaces(actual, destination, source):
            self.assertEqual(data, actual)
            events.append(('enter', destination))
            try:
                yield destination / 'variant'
            finally:
                events.append(('leave', destination))

        def child(command, env, log, lease, run_lock, workspace):
            self.assertEqual((12, 11, 10), (lease, run_lock, workspace))
            self.assertIn('--non-interactive', command)
            self.assertEqual('1', env['PG_NATIVE_HOST_DISPOSABLE'])
            self.assertEqual(frozen['source'], env['PG_LISP_FROZEN_SOURCE'])
            self.assertEqual(environment['XDG_CACHE_HOME'], env['XDG_CACHE_HOME'])
            children.append((command, dict(env), log))
            marker = native.PROOF_MARKERS[env['PG_NATIVE_COLD_PROOF']]
            reported_case = 'happy-extended' if failure == 'prefix' else 'wrong' if failure is True else 'happy'
            log.write_text(marker + ' PASS case=' + reported_case + '\n')
            return 7 if failure == 'exit' else 0

        fake = SimpleNamespace(
            job_root=lambda path=None: Path(path) if path else job,
            workspace_lease=lambda: contextlib.nullcontext(10),
            lock=lambda root, name, **kw: contextlib.nullcontext(11 if name == 'run.lock' else 12),
            read_campaign=lambda root: frozen, checkout_state=lambda root: ({}, b''),
            environment=lambda: {}, fingerprint=lambda source: image, wait_child=child,
            write_json=lambda path, value: path.write_text(json.dumps(value)))
        writer = SimpleNamespace(tree_image=lambda path: ['unchanged'], working_namespaces=namespaces)
        with mock.patch.dict(os.environ, environment, clear=True), \
             mock.patch.object(native, 'module', side_effect=[fake, writer]):
            if failure:
                with self.assertRaises(RuntimeError):
                    native.main()
            else:
                native.main()
        return events, children, json.loads((logs / 'native-cold-results.json').read_text())

    def test_each_row_has_a_fresh_child_and_restored_distinct_namespace(self):
        events, children, results = self.run_driver()
        self.assertEqual(['enter', 'leave', 'enter', 'leave'], [event[0] for event in events])
        self.assertNotEqual(events[0][1], events[2][1])
        self.assertEqual(2, len(children))
        self.assertNotEqual(children[0][2], children[1][2])
        self.assertNotIn('PG_NATIVE_COLD_SPILL', children[0][1])
        self.assertEqual('1', children[1][1]['PG_NATIVE_COLD_SPILL'])
        self.assertTrue(all(row['passed'] for row in results))

    def test_missing_marker_restores_namespace_and_stops_the_plan(self):
        events, children, results = self.run_driver(failure=True)
        self.assertEqual(['enter', 'leave'], [event[0] for event in events])
        self.assertEqual(1, len(children))
        self.assertFalse(results[0]['passed'])

    def test_case_name_prefix_cannot_satisfy_another_cases_marker(self):
        _, children, results = self.run_driver(failure='prefix')
        self.assertEqual(1, len(children))
        self.assertFalse(results[0]['passed'])

    def test_success_marker_does_not_override_failed_child_exit(self):
        events, children, results = self.run_driver(failure='exit')
        self.assertEqual(['enter', 'leave'], [event[0] for event in events])
        self.assertEqual(1, len(children))
        self.assertEqual(7, results[0]['exit_code'])
        self.assertFalse(results[0]['passed'])

    def crash_children(self, *, first_exit=-9, changed_seal=False, proof='reconcile',
                       spill=False, missing_spill=False):
        row = dict(case='happy', proof=proof, spill=spill,
                   cut='after-commit' if proof in ('publish', 'load') else 'slot-state-after-unlink')
        prefix = native.PROOF_MARKERS[proof]
        env = dict(PG_NATIVE_COLD_RECONCILE_CUT=row['cut'], PG_NATIVE_COLD_APPEND='1')
        calls = []

        def child(command, actual, log, lease, run_lock, workspace):
            self.assertEqual((12, 11, 10), (lease, run_lock, workspace))
            calls.append((command, dict(actual), log))
            seal = 'CCDD' if changed_seal and len(calls) == 2 else 'AABB'
            text = prefix + '-EXPECTED ' + seal + '\n'
            if len(calls) == 1:
                if spill and not missing_spill:
                    text += 'NATIVE-COLD-SPILL-USED transcript threshold=0\n'
                text += prefix + '-SIGKILL ' + row['cut'] + '\n'
            else:
                text += prefix + ' PASS case=happy\n'
                if spill and not missing_spill:
                    text += 'NATIVE-COLD-SPILL PASS transcripts=2 threshold=0 cleanup-complete\n'
            log.write_text(text)
            return first_exit if len(calls) == 1 else 0

        result = native.run_cold_children(SimpleNamespace(wait_child=child), ['sbcl'], env,
                                          row, self.root / 'crash.log', 12, 11, 10)
        return calls, result

    def mixed_children(self, *, changed_seal=False, failed_restart=False,
                       temporary=False, missing_temporary=False):
        calls = []
        row = dict(case='happy', proof='load-physical', spill=False)

        def child(command, env, log, lease, run_lock, workspace):
            calls.append((dict(env), log))
            if len(calls) > 1:
                self.assertEqual('1', env['PG_NATIVE_COLD_MIXED_RESTART'])
                self.assertNotIn('PG_NATIVE_COLD_APPEND', env)
            seal = 'CCDD' if changed_seal and len(calls) == 2 else 'AABB'
            log.write_text('NATIVE-COLD-MIXED-PUBLICATION-EXPECTED 1122:' + seal + '\n'
                           'NATIVE-COLD-MIXED-PUBLICATION PASS case=happy\n'
                           + ('NATIVE-COLD-TEMPORARY PASS issued-lifetime\n'
                              if temporary and not missing_temporary and len(calls) == 1 else ''))
            return 1 if failed_restart and len(calls) == 2 else 0

        environment = dict(PG_NATIVE_COLD_APPEND='1')
        if temporary:
            environment['PG_NATIVE_COLD_TEMPORARY'] = '1'
        result = native.run_cold_children(SimpleNamespace(wait_child=child), ['sbcl'],
                                         environment, row,
                                         self.root / 'mixed.log', 12, 11, 10)
        return calls, result

    def test_mixed_inventory_requires_two_clean_restarts_with_exact_physical_and_selector_bytes(self):
        calls, (status, _, recovery) = self.mixed_children()
        self.assertEqual(3, len(calls))
        self.assertEqual(3, len({call[1] for call in calls}))
        self.assertEqual(0, status)
        self.assertEqual('clean', recovery['kind'])
        self.assertTrue(recovery['passed'])

    def test_mixed_restart_rejects_changed_physical_bytes_or_failed_child(self):
        for options in (dict(changed_seal=True), dict(failed_restart=True)):
            with self.subTest(options=options):
                calls, (status, _, recovery) = self.mixed_children(**options)
                self.assertEqual(2, len(calls))
                self.assertNotEqual(0, status)
                self.assertFalse(recovery['passed'])

    def test_temporary_qualification_requires_its_initial_child_marker(self):
        calls, (status, _, recovery) = self.mixed_children(temporary=True)
        self.assertEqual(3, len(calls))
        self.assertEqual(0, status)
        self.assertTrue(recovery['passed'])
        calls, (status, _, recovery) = self.mixed_children(temporary=True, missing_temporary=True)
        self.assertEqual(1, len(calls))
        self.assertNotEqual(0, status)
        self.assertFalse(recovery['passed'])

    def logical_children(self, *, changed_pending=False, failed_seed=False):
        calls = []
        row = dict(case='happy', proof='load-logical', spill=False)

        def child(command, env, log, lease, run_lock, workspace):
            calls.append(dict(env))
            if len(calls) == 1:
                log.write_text('NATIVE-COLD-LOGICAL-SEED-EXPECTED AABB 7/101/121;\n'
                               'NATIVE-COLD-LOGICAL-SEED PASS case=happy\n')
                return 1 if failed_seed else 0
            self.assertNotIn('PG_NATIVE_COLD_APPEND', env)
            self.assertEqual('AABB', env['PG_NATIVE_COLD_LOGICAL_SEED_IMAGE'])
            self.assertEqual('7/101/121;', env['PG_NATIVE_COLD_LOGICAL_SEED_PENDING'])
            pending = '7/101/122;' if changed_pending and len(calls) == 3 else '7/101/121;'
            log.write_text('NATIVE-COLD-LOGICAL-EXPECTED 1122:AABB:' + pending + ':\n'
                           'NATIVE-COLD-LOGICAL PASS case=happy\n')
            return 0

        result = native.run_cold_children(SimpleNamespace(wait_child=child), ['sbcl'],
                                         dict(PG_NATIVE_COLD_APPEND='1'), row,
                                         self.root / 'logical.log', 12, 11, 10)
        return calls, result

    def test_logical_seed_precedes_real_ack_and_two_exact_pending_restarts(self):
        calls, (status, _, recovery) = self.logical_children()
        self.assertEqual(['seed', 'advance', 'restart-1', 'restart-2'],
                         [env['PG_NATIVE_COLD_LOGICAL_PHASE'] for env in calls])
        self.assertEqual(0, status)
        self.assertTrue(recovery['passed'])

    def test_logical_seed_failure_and_changed_reconstructed_pending_are_rejected(self):
        for options, count in ((dict(failed_seed=True), 1), (dict(changed_pending=True), 3)):
            with self.subTest(options=options):
                calls, (status, _, recovery) = self.logical_children(**options)
                self.assertEqual(count, len(calls))
                self.assertNotEqual(0, status)
                self.assertFalse(recovery['passed'])

    def test_crash_requires_sigkill_then_two_fresh_restarts_with_the_same_seal(self):
        calls, (status, log, crash) = self.crash_children()
        self.assertEqual(3, len(calls))
        self.assertEqual(3, len({call[2] for call in calls}))
        self.assertEqual(0, status)
        self.assertEqual(calls[-1][2], log)
        self.assertTrue(crash['passed'])
        self.assertEqual(-9, crash['exit_code'])
        self.assertEqual(2, len(crash['restarts']))
        for _, env, _ in calls[1:]:
            self.assertNotIn('PG_NATIVE_COLD_RECONCILE_CUT', env)
            self.assertNotIn('PG_NATIVE_COLD_APPEND', env)

    def test_a_claimed_cut_without_sigkill_cannot_count_as_crash_coverage(self):
        calls, (status, _, crash) = self.crash_children(first_exit=0)
        self.assertEqual(1, len(calls))
        self.assertNotEqual(0, status)
        self.assertFalse(crash['passed'])

    def test_changed_selected_bytes_on_restart_stop_before_a_second_restart(self):
        calls, (status, _, crash) = self.crash_children(changed_seal=True)
        self.assertEqual(2, len(calls))
        self.assertNotEqual(0, status)
        self.assertFalse(crash['passed'])
        self.assertFalse(crash['restarts'][0]['passed'])

    def test_publication_crashes_use_the_publication_seal_and_restart_markers(self):
        calls, (status, _, crash) = self.crash_children(proof='publish', spill=True)
        self.assertEqual(3, len(calls))
        self.assertEqual(0, status)
        self.assertTrue(crash['passed'])

    def test_loader_crashes_require_two_fresh_restarts_and_real_spilling(self):
        calls, (status, _, crash) = self.crash_children(proof='load', spill=True)
        self.assertEqual(3, len(calls))
        self.assertEqual(0, status)
        self.assertTrue(crash['passed'])
        calls, (status, _, crash) = self.crash_children(proof='load', spill=True,
                                                      missing_spill=True)
        self.assertEqual(1, len(calls))
        self.assertNotEqual(0, status)
        self.assertFalse(crash['passed'])

    def test_spill_crash_cannot_pass_without_actual_spill_use_before_death(self):
        calls, (status, _, crash) = self.crash_children(proof='publish', spill=True,
                                                      missing_spill=True)
        self.assertEqual(1, len(calls))
        self.assertNotEqual(0, status)
        self.assertFalse(crash['passed'])

    def test_spill_flag_requires_a_real_nonempty_spill_and_cleanup_marker(self):
        row = dict(proof='publish', case='happy', spill=True)
        passed = 'NATIVE-COLD-PUBLICATION PASS case=happy\n'
        self.assertFalse(native.cold_pass_marker(row, passed))
        self.assertFalse(native.cold_pass_marker(
            row, passed + 'NATIVE-COLD-SPILL PASS transcripts=0 threshold=0 cleanup-complete\n'))
        self.assertTrue(native.cold_pass_marker(
            row, passed + 'NATIVE-COLD-SPILL PASS transcripts=2 threshold=0 cleanup-complete\n'))


if __name__ == '__main__':
    unittest.main()

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

    def test_optional_child_flags_do_not_leak_between_rows(self):
        rows = self.plan([{'case': 'prepared-durable', 'proof': 'prepare-pending', 'spill': True},
                          {'case': 'happy', 'proof': None, 'spill': False},
                          {'case': 'happy-extended', 'proof': 'prepare'}])
        base = {'PG_NATIVE_COLD_APPEND': 'old', 'PG_NATIVE_COLD_FAULT': 'old',
                'PG_NATIVE_COLD_PLAN': 'input-only', 'UNCHANGED': 'yes'}
        first = native.child_environment(base, rows[0])
        second = native.child_environment(first, rows[1])
        third = native.child_environment(second, rows[2])
        self.assertEqual('PREPARED', first['PG_NATIVE_COLD_KIND'])
        self.assertEqual('OLD', first['PG_NATIVE_COLD_CHOICE'])
        self.assertEqual('1', first['PG_NATIVE_COLD_GENERATION'])
        self.assertEqual('1', first['PG_NATIVE_COLD_SPILL'])
        for key in ('PG_NATIVE_COLD_PROOF', 'PG_NATIVE_COLD_SPILL', 'PG_NATIVE_COLD_APPEND',
                    'PG_NATIVE_COLD_FAULT', 'PG_NATIVE_COLD_PLAN'):
            self.assertNotIn(key, second)
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


if __name__ == '__main__':
    unittest.main()

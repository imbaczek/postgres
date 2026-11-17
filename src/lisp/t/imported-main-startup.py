"""Start the real launcher in a disposable view of an inode-bound import.

PG_NATIVE_MAIN_RESTART=1 runs three fresh servers in the same disposable view,
with clean SIGTERM shutdowns. Each boot checks its own control socket and SQL;
instrumented runs also check typed C or ZERO-C restoration before catalog entry.
Witness rows 41 and 42 persist across the clean restarts. This does not test
crash or commit-ack durability, or advancement of slot feedback.
"""

import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import shutil
import shlex
import socket
import struct
import subprocess
import time


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def inspect_control(path, pid, form=None, marker=':IMPORTED-MAIN-CONTROL-OK'):
    """Inspect this server's typed protection and absent follow workers."""
    form = form or """(let ((*print-circle* t) (*print-level* 4) (*print-length* 12))
      (sb-thread:with-mutex (pg::*wal-lock*)
        (assert (not (pg::recovery-in-progress-p)))
        (assert (not pg::*physical-follow-runtime*))
        (dolist (worker (list pg::*physical-follow-controller-worker* pg::*physical-follow-desired-worker*))
          (assert (not (and worker (sb-thread:thread-alive-p worker)))))
        (when (plusp (hash-table-count pg::*wal-replication-slots*))
          (assert (plusp (hash-table-count pg::*logical-slot-continuation-protections*))))
        (maphash (lambda (key group)
                   (declare (ignore key))
                   (assert (pg::logical-slot-continuation-protection-call group :check)))
                 pg::*logical-slot-continuation-protections*))
      :imported-main-control-ok)
      (quit)
    """
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(10)
        sock.connect(str(path))
        peer, uid, _ = struct.unpack('3i', sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if peer != pid or uid != os.geteuid():
            raise ValueError('Control socket is not owned by the expected server')
        sock.sendall(form.encode())
        chunks = []
        while chunk := sock.recv(65536):
            chunks.append(chunk)
            if sum(map(len, chunks)) > 65536:
                raise ValueError('Unexpectedly large control response')
    response = b''.join(chunks).decode()
    if marker not in response or 'error:' in response:
        raise RuntimeError('Imported main control check failed: ' + response)
    return response


def main():
    directory = Path(__file__).resolve().parent
    campaign = module('main_campaign', directory / 'test-campaign.py')
    writer = module('main_writer', directory / 'continuation-native-writer.py')
    descendant = module('main_descendant', directory / 'continuation-native-descendant.py')
    observation = module('main_observation', directory / 'imported-main-observation.py')
    root = campaign.job_root(os.environ['PG_LISP_TEST_CAMPAIGN'])
    job = campaign.job_root()
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
            raise ValueError('Main startup source or runtime changed')
        identity = (data.stat().st_dev, data.stat().st_ino)
        with writer.owned_fixture(data, identity):
            before = writer.tree_image(data)
            initial_names = set(os.listdir(data))
        extras = tuple(sorted(initial_names - set(writer.NAMES) - {'base', '.pg_lisp.lock'}))
        variant = os.environ.get('PG_NATIVE_MAIN_VARIANT_ROOT')
        variant = (campaign.job_root(variant) / 'descendant-working/variant') if variant else None
        variant_before = writer.tree_image(variant) if variant else None
        profile = os.environ.get('PG_NATIVE_MAIN_PROFILE', 'original')
        if profile not in ('original', 'retired'):
            raise ValueError('Unknown main startup history profile')
        case = os.environ.get('PG_NATIVE_MAIN_CASE', 'happy')
        if case not in ('happy', 'managed', 'promote', 'malformed-proof', 'proof-loss', 'cancel-spawn'):
            raise ValueError('Unknown main startup case')
        wire = os.environ.get('PG_NATIVE_MAIN_WIRE')
        if wire not in (None, '1') or (wire and case != 'happy'):
            raise ValueError('Wire qualification requires happy case and explicit value 1')
        restart = os.environ.get('PG_NATIVE_MAIN_RESTART')
        if restart not in (None, '1') or (restart and case != 'happy'):
            raise ValueError('Restart qualification requires happy case and explicit value 1')
        control = job / 'tmp/main-control'
        control.mkdir(mode=0o700)
        port = free_port()
        log = job / 'logs/imported-main-startup.log'
        result = dict(source=frozen['image'], port=port, ready=False, profile=profile, case=case,
                      variant=str(variant) if variant else None)
        result['restart_qualification'] = bool(restart)
        result['boots'] = []
        started = time.monotonic()
        try:
            with writer.working_namespaces(data, job / 'main-working',
                                           source=variant, full_source=bool(variant),
                                           include_base=True, extra_names=extras):
                try:
                    if profile == 'retired':
                        names = ['physical-recovery.control', 'physical-backup.manifest', 'recovery.required']
                        present = [name for name in names if os.path.lexists(data / name)]
                        if len(present) not in (0, 3):
                            raise ValueError('Cannot retire a partial historical profile')
                        stash = job / 'tmp/historical-profile'
                        stash.mkdir()
                        for name in present:
                            os.rename(data / name, stash / name)
                    if case not in ('happy', 'proof-loss', 'cancel-spawn'):
                        pid_file = data / 'postmaster.pid'
                        if os.path.lexists(pid_file):
                            raise ValueError('Refusal fixture must not replace an existing PID')
                        with pid_file.open('x') as stream:
                            stream.write('2147483647\n' + str(data) + '\n1234\n54321\n\n127.0.0.1\n\nstarting\n')
                        pid_file.chmod(0o600)
                        if case == 'malformed-proof':
                            path = data / 'logical-slot-copy.source-proof'
                            image = bytearray(path.read_bytes())
                            image[0] ^= 0xff
                            path.write_bytes(image)
                        else:
                            path = data / ('physical-follow-managed.control' if case == 'managed' else 'promote')
                            with path.open('x') as stream:
                                stream.write('must remain unchanged\n')
                            path.chmod(0o600)
                    refusal_before = observation.fixture_image(data, writer) if case not in ('happy', 'proof-loss', 'cancel-spawn') else None
                    storage_names = ('global', 'base', 'pg_wal', 'pg_xact', 'pg_replslot')
                    storage_before = {name: observation.fixture_image(data / name, writer)
                                      for name in storage_names} if case == 'proof-loss' else None
                    follow = data / 'physical-follow.control'
                    follow_before = writer.tree_image(follow) if follow.exists() else None
                    expected_names = sorted(path.name for path in (data / 'pg_replslot').iterdir()
                                            if path.is_dir() and not path.name.startswith('.'))
                    result['expected_slots'] = expected_names
                    process = None
                    previous = {}
                    child_transition = False
                    pending_signal = None

                    def interrupted(signum, frame):
                        nonlocal pending_signal
                        if child_transition:
                            pending_signal = signum
                            return
                        raise InterruptedError('Main startup interrupted by signal ' + str(signum))

                    try:
                        previous = {sig: signal.signal(sig, interrupted)
                                    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                        environment = dict(os.environ, PG_LISP_FROZEN_SOURCE=str(source))
                        instrumented = os.environ.get('PG_NATIVE_MAIN_INSTRUMENT')
                        if instrumented not in (None, '1'):
                            raise ValueError('Startup instrumentation requires explicit value 1')
                        result['instrumented'] = bool(instrumented)
                        if case == 'proof-loss':
                            if not instrumented:
                                raise ValueError('Proof-loss case requires startup instrumentation')
                            environment['PG_NATIVE_MAIN_FAULT'] = 'proof-loss'
                        if instrumented:
                            wrapper = job / 'tmp/main-sbcl'
                            wrapper.write_text('#!/bin/sh\nexec ' + shlex.quote(environment['SBCL'])
                                               + ' --noinform --no-sysinit --no-userinit --load '
                                               + shlex.quote(str(source / 't/imported-main-startup-observer.lisp'))
                                               + ' "$@"\n')
                            wrapper.chmod(0o700)
                            environment['SBCL'] = str(wrapper)
                        command = [str(source / 'pg-lisp.sh'), '-D', str(data), '-p', str(port),
                                   '-c', 'wal_level=logical', '--control-socket', str(control / 'pg.sock')]
                        result['command'] = command
                        for boot in range(3 if restart else 1):
                            result['ready'] = False
                            boot_started = time.monotonic()
                            log = job / ('logs/imported-main-startup-' + str(boot) + '.log'
                                         if restart else 'logs/imported-main-startup.log')
                            with log.open('w') as output:
                                def spawn():
                                    child = subprocess.Popen(command, cwd=source, env=environment,
                                                             stdout=output, stderr=subprocess.STDOUT,
                                                             pass_fds=(lease, run_lock, workspace),
                                                             start_new_session=True)
                                    if case == 'cancel-spawn':
                                        os.kill(os.getpid(), signal.SIGTERM)
                                    return child

                                child_transition = True
                                try:
                                    process = spawn()
                                finally:
                                    child_transition = False
                                if pending_signal is not None:
                                    deferred = pending_signal
                                    pending_signal = None
                                    result['deferred_spawn_signal'] = deferred
                                    raise InterruptedError('Deferred startup signal ' + str(deferred))
                                ticks = Path('/proc', str(process.pid), 'stat').read_text().rpartition(')')[2].split()[19]
                                result.update(pid=process.pid, start_ticks=ticks)
                                campaign.write_json(job / 'logs/main-child.json', result)
                                deadline = time.monotonic() + 180
                                marker = 'pg-lisp: listening on '
                                while process.poll() is None and time.monotonic() < deadline:
                                    if marker in log.read_text(errors='replace'):
                                        result['ready'] = True
                                        break
                                    time.sleep(0.05)
                                if case != 'happy':
                                    if result['ready']:
                                        raise RuntimeError('Conflicting startup unexpectedly became ready')
                                    result['exit_code'] = process.wait(timeout=30)
                                    text = log.read_text()
                                    expected_error = {'managed': 'request physical-follow-managed.control',
                                                      'promote': 'request promote',
                                                      'malformed-proof': 'source proof',
                                                      'proof-loss': 'requires the complete committed input names'}[case]
                                    if case == 'proof-loss':
                                        counts = re.findall(r'^IMPORTED-MAIN-COUNTS slots=(\d+) catalogs=(\d+) bootstraps=(\d+) owned=(\d+) wal-ready=(NIL|T) fence=(NIL|T)$',
                                                            text, re.MULTILINE)
                                        if counts != [('0', '0', '0', '2', 'NIL', 'T')]:
                                            raise RuntimeError('Proof loss bypassed the guarded initializer: ' + str(counts))
                                        result['proof_loss_counts'] = counts[0]
                                        observed_after = {name: observation.fixture_image(data / name, writer)
                                                          for name in storage_names}
                                        observed_before = storage_before
                                    else:
                                        observed_before = refusal_before
                                        observed_after = observation.fixture_image(data, writer)
                                    result['refusal_inputs_preserved'] = observed_before == observed_after
                                    if not result['refusal_inputs_preserved']:
                                        campaign.write_json(job / 'logs/refusal-before.json', observed_before)
                                        campaign.write_json(job / 'logs/refusal-after.json', observed_after)
                                    if (result['ready'] or result['exit_code'] == 0
                                            or expected_error not in text
                                            or not result['refusal_inputs_preserved']
                                            or (control / 'pg.sock').exists()):
                                        raise RuntimeError('Main refusal did not preserve pre-startup inputs: ' + str(log))
                                    result['refused'] = True
                                else:
                                    if not result['ready']:
                                        raise RuntimeError('Real imported main did not become ready: ' + str(log))
                                    pid = int((data / 'postmaster.pid').read_text().splitlines()[0])
                                    if pid != process.pid:
                                        raise ValueError('Ready server does not own the expected PID')
                                    result['startup_seconds'] = time.monotonic() - boot_started
                                    if instrumented:
                                        observations = re.findall(r'^IMPORTED-MAIN-BEFORE-CATALOG slots=(\d+) groups=(\d+) outcome=(C|ZERO-C)$',
                                                                  log.read_text(), re.MULTILINE)
                                        if len(observations) != 1:
                                            raise RuntimeError('Missing unique before-catalog observation')
                                        slots, groups, outcome = observations[0]
                                        if (int(slots) != len(expected_names)
                                                or (expected_names and (int(groups) < 1 or outcome != 'C'))
                                                or (not expected_names and (int(groups) or outcome != 'ZERO-C'))):
                                            raise RuntimeError('Wrong typed restoration at catalog entry')
                                        result['before_catalog'] = dict(slots=int(slots), groups=int(groups), outcome=outcome)
                                    result['control_response'] = inspect_control(control / 'pg.sock', process.pid)
                                    psql = os.environ.get('PSQL') or shutil.which('psql')
                                    if not psql:
                                        raise RuntimeError('Set PSQL to an installed PostgreSQL client')
                                    result['psql_version'] = subprocess.check_output([psql, '--version'], text=True).strip()
                                    sql = 'SELECT pg_is_in_recovery(); SELECT slot_name FROM pg_replication_slots ORDER BY slot_name;'
                                    expected_sql = ['f', *expected_names]
                                    if restart:
                                        if boot == 0:
                                            sql += ' CREATE TABLE imported_restart_witness (n integer); INSERT INTO imported_restart_witness VALUES (41);'
                                        sql += ' SELECT n FROM imported_restart_witness ORDER BY n;'
                                        expected_sql += ['41'] + (['42'] if boot == 2 else [])
                                        if boot == 1:
                                            sql += ' INSERT INTO imported_restart_witness VALUES (42);'
                                    query = subprocess.run([str(psql), '-X', '-q', '-A', '-t', '-h', '127.0.0.1',
                                                            '-p', str(port), '-U', 'postgres', '-d', 'postgres',
                                                            '-v', 'ON_ERROR_STOP=1', '-c', sql],
                                                           env=dict(environment, PGCONNECT_TIMEOUT='5'),
                                                           text=True, capture_output=True, timeout=30)
                                    result.update(query_exit=query.returncode, sql_stdout=query.stdout,
                                                  sql_stderr=query.stderr)
                                    if query.returncode or query.stdout.splitlines() != expected_sql:
                                        raise RuntimeError('Imported main SQL check failed: ' + repr(query))
                                    if wire and boot == 0:
                                        wire_tests = module('imported_main_wire', directory / 'imported-main-wire.py')
                                        result['wire'] = wire_tests.exercise(
                                            port, data, job, expected_names,
                                            lambda form, marker: inspect_control(
                                                control / 'pg.sock', process.pid, form, marker))
                                    process.send_signal(signal.SIGTERM)
                                    result['exit_code'] = process.wait(timeout=30)
                                    if result['exit_code']:
                                        raise RuntimeError('Imported main shutdown failed')
                                    result['follow_preserved'] = follow_before == (writer.tree_image(follow)
                                                                                  if follow.exists() else None)
                                    if not result['follow_preserved']:
                                        raise ValueError('Inert follow configuration changed')
                            result['boots'].append({key: result[key] for key in
                                ('pid', 'start_ticks', 'ready', 'exit_code', 'sql_stdout', 'before_catalog',
                                 'startup_seconds', 'control_response', 'follow_preserved')
                                if key in result})
                    finally:
                        child_transition = True
                        try:
                            if process and process.poll() is None:
                                try:
                                    os.killpg(process.pid, signal.SIGTERM)
                                except ProcessLookupError:
                                    pass
                                try:
                                    process.wait(timeout=10)
                                except subprocess.TimeoutExpired:
                                    os.killpg(process.pid, signal.SIGKILL)
                                    process.wait()
                            if process:
                                result['exit_code'] = process.returncode
                                result['pid'] = process.pid
                                result['child_reaped'] = process.returncode is not None
                        finally:
                            child_transition = False
                            for sig, handler in previous.items():
                                signal.signal(sig, handler)
                        if pending_signal is not None:
                            raise InterruptedError('Deferred cleanup signal ' + str(pending_signal))
                finally:
                    descendant.retire_new_root_entries(data, initial_names,
                                                       job / 'main-new-root-entries', writer, identity)
        except InterruptedError:
            if case != 'cancel-spawn':
                raise
            if not result.get('child_reaped'):
                raise RuntimeError('Spawn cancellation left an unreaped child')
            if result.get('deferred_spawn_signal') != signal.SIGTERM:
                raise RuntimeError('Spawn cancellation did not reach the publication boundary')
            result['cancelled_after_spawn'] = True
        finally:
            with writer.owned_fixture(data, identity):
                result['original_preserved'] = before == writer.tree_image(data)
            result['source_preserved'] = frozen['image'] == campaign.fingerprint(source)
            result['variant_preserved'] = variant is None or variant_before == writer.tree_image(variant)
            result['wall_seconds'] = time.monotonic() - started
            campaign.write_json(job / 'logs/imported-main-startup-result.json', result)
            if not result['original_preserved'] or not result['source_preserved'] or not result['variant_preserved']:
                raise RuntimeError('Main startup experiment changed preserved inputs')
        result['harness_passed'] = True
        campaign.write_json(job / 'logs/imported-main-startup-result.json', result)
        marker = ('IMPORTED-MAIN-CANCEL' if case == 'cancel-spawn'
                  else 'IMPORTED-MAIN-REFUSAL' if case != 'happy' else 'IMPORTED-MAIN-STARTUP')
        print(marker
              + ' PASS ' + json.dumps(result), flush=True)


if __name__ == '__main__':
    began = time.monotonic()
    try:
        main()
    except BaseException as error:
        path = Path(os.environ['PG_LISP_TEST_LOG_DIR']) / 'imported-main-startup-result.json'
        result = json.loads(path.read_text()) if path.exists() else {}
        result.update(harness_passed=False, error_type=type(error).__name__)
        result.setdefault('wall_seconds', time.monotonic() - began)
        path.write_text(json.dumps(result, indent=2) + '\n')
        raise

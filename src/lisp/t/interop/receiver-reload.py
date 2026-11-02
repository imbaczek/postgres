"""Stop real receipt while an owned reload gate keeps admission closed."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
from pathlib import Path
import time


@contextmanager
def installed_gate(flow, directory):
    flow.wait(lambda: ':IDLE T' in flow.control('sibling.sock',
        '(list :idle (and (null (sb-thread:mutex-owner *reload-run-lock*)) (not (reload-request-p +reload-request-code+))))').upper())
    flow.control('sibling.sock', f'(receiver-reload-native-install {json.dumps(str(directory) + "/")})')
    try:
        yield
    finally:
        (directory / 'release').touch()
        flow.control('sibling.sock', '(receiver-reload-native-uninstall)')


def exercise(flow):
    base = Path(__file__).resolve().parents[2]
    root = flow.data.parent / 'receiver-reload'
    root.mkdir(mode=0o700)
    flow.control('sibling.sock', f'(load {json.dumps(str(base / "t/receiver-reload-native.lisp"))})')
    conninfo = flow.conninfo(flow.api['source_port'])
    for mode in ('manual', 'configured'):
        directory = root / mode
        directory.mkdir(mode=0o700)
        with installed_gate(flow, directory), ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(flow.control, 'sibling.sock', '(physical-follow-stop)') if mode == 'manual' else None
            try:
                if mode == 'configured':
                    (flow.data / 'postgresql.auto.conf').write_text("primary_conninfo=''\nprimary_slot_name=''\n")
                    flow.control('sibling.sock', '(progn (request-configuration-reload) (values))')
                deadline = time.monotonic() + 20
                while not (directory / 'closed').exists():
                    if pending is not None and pending.done():
                        pending.result()
                    assert not (directory / 'error').exists(), (directory / 'error').read_text()
                    assert time.monotonic() < deadline, f'reload barrier timed out: {directory}'
                    time.sleep(0.02)
                assert not (directory / 'stopped').exists()
            finally:
                (directory / 'release').touch()
            if pending is not None:
                assert ':STOPPED' in pending.result(timeout=30).upper()
        flow.wait(lambda: (directory / 'opened').exists() and (directory / 'stopped').exists())
        assert not (directory / 'error').exists(), (directory / 'error').read_text()
        flow.control('sibling.sock', '(progn (sb-thread:join-thread *receiver-reload-native-thread* :timeout 2 :default :pending) (assert (not *physical-recovery-failed-p*)) (assert (not (sb-thread:thread-alive-p *receiver-reload-native-thread*))))')
        if mode == 'configured':
            flow.stopped()
            flow.control('sibling.sock', '(assert (and *physical-follow-runtime* (not (physical-follow-runtime-stopped-p *physical-follow-runtime*)) (sb-thread:thread-alive-p (physical-follow-runtime-replay-thread *physical-follow-runtime*))))')
        else:
            flow.control('sibling.sock', '(assert (null *physical-follow-runtime*))')
        # A changed desired generation resumes receipt after manual suspension.
        resumed = conninfo + b' application_name=reload_resume' if mode == 'manual' else conninfo
        flow.configure(resumed, 'configured_a')
        flow.streaming('configured_a')
        flow.count(1)
        print(f'receiver-reload: {mode} stop waits for closed reload gate, then resumes PASS', flush=True)

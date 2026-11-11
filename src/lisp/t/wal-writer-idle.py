"""Verify background durability of complete native WAL with no publication owner."""
from pathlib import Path
import re
import runpy
import signal
from interop.crc32c_reference import crc32c


api = runpy.run_path(str(Path(__file__).with_name('wal-writer.py')))
OwnedLisp = api['OwnedLisp']
pause_writer, resume_writer = api['pause_writer'], api['resume_writer']
wait_control = api['wait_control']
wait_file = api['wait_file']
optional_fields = runpy.run_path(
    str(Path(__file__).with_name('interop') / 'resource_control_fields.py'))['check_optional_fields']


def control_facts(server):
    raw = (server.data / 'global/pg_wal_control').read_text(encoding='ascii')
    assert raw.endswith('\n') and raw.count('\n') == 1
    prefix, crc = raw.rstrip('\n').rsplit(' ', 1)
    assert crc32c(prefix.encode('ascii')) == int(crc, 16)
    fields = prefix.split(' ')
    assert fields[0] == 'PGWAL6'
    return dict(durable=int(fields[2]), resource=optional_fields(fields)['lsn'],
                timeline=int(fields[3]), namespace=fields[5])


def require_buffered_terminal_absent(server, terminal):
    facts = control_facts(server)
    segment = (terminal - 1) // 16777216
    name = (f'{segment:016X}' if facts['namespace'] == 'legacy'
            else f"{facts['timeline']:08X}{segment // 256:08X}{segment % 256:08X}")
    path = server.data / 'pg_wal' / (name + '.wal')
    assert not path.exists() or path.stat().st_size < terminal - segment * 16777216


def number(server, form):
    output = server.control(f'(list :idle-number {form})')
    return int(re.search(r':IDLE-NUMBER\s+(\d+)', output).group(1))


def check_crash(after_flush):
    server = OwnedLisp()
    paused = None
    try:
        server.start()
        paused = pause_writer(server, 'empty-fifo')
        before = number(server, '(getf *wal-control-metadata* :resource-lsn)')
        server.control('(wal-writer-test-append-idle-batch)')
        terminal = number(server, '*writer-test-end*')
        assert terminal > before
        if after_flush:
            resume_writer(server, paused)
            paused = None
            # Do not run SQL: statement preparation can flush WAL itself.
            wait_control(server, '(list :idle-durable (wal-writer-test-idle-durable-p))',
                         ':IDLE-DURABLE T')
        process = server.process
        server.stop(immediate=True)
        assert process.returncode == -signal.SIGKILL
        if not after_flush:
            require_buffered_terminal_absent(server, terminal)
        resume_writer(server, paused)
        paused = None
        server.start()
        recovered = number(server, '(getf *wal-control-metadata* :resource-lsn)')
        assert recovered == (terminal if after_flush else before), (before, terminal, recovered)
        print(f'wal-writer-idle: empty FIFO, after_flush={after_flush}, '
              'SIGKILL recovered exact resource terminal PASS', flush=True)
    finally:
        resume_writer(server, paused)
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


def check_failure(phase):
    server = OwnedLisp()
    paused = None
    evidence = server.root / 'idle-failure-fenced'
    try:
        server.start()
        paused = pause_writer(server, 'idle-failure')
        before = number(server, '(getf *wal-control-metadata* :resource-lsn)')
        durable_before = control_facts(server)['durable']
        server.control('(wal-writer-test-append-idle-batch)')
        terminal = number(server, '*writer-test-end*')
        server.control(f'(wal-writer-test-install-idle-failure #p"{evidence}" :{phase})')
        process = server.process
        resume_writer(server, paused)
        paused = None
        wait_file(server, evidence)
        process.wait(timeout=15)
        assert process.returncode is not None
        facts = control_facts(server)
        assert facts['durable'] == (terminal if phase == 'after' else durable_before)
        assert facts['resource'] == before, 'flush must not publish resource replay state'
        if phase in ('before', 'exit'):
            # Exact loss is specific to this small buffered fixture. A complete
            # unadvertised tail may otherwise survive and be adopted at startup.
            require_buffered_terminal_absent(server, terminal)
        server.stop()
        server.start()
        recovered = number(server, '(getf *wal-control-metadata* :resource-lsn)')
        # Startup may adopt a longer complete WAL tail than old durable control.
        # The middle fault already synced bytes, so that terminal must be replayed.
        assert recovered == (terminal if phase in ('middle', 'after') else before), (
            phase, before, terminal, recovered)
        print(f'wal-writer-idle: {phase} failure fenced outside locks, '
              'automatic shutdown and exact recovery PASS', flush=True)
    finally:
        resume_writer(server, paused)
        server.stop(immediate=True)
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    for after_flush in (False, True):
        check_crash(after_flush)
    for phase in ('before', 'middle', 'after', 'exit'):
        check_failure(phase)

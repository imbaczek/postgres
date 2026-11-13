"""Exercise import ownership in an actual compacted copied standby."""
from pathlib import Path
import runpy
import os
import subprocess
import time

standby = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-standby.py')))
OwnedLisp = standby['api']['OwnedLisp']
lisp_string = standby['lisp_string']


def probe(peer, pending=False, disposition='wal-level-insufficient'):
    peer.control(f'(load {lisp_string(Path(__file__).with_name("logical-slot-copy-import-input-native.lisp"))})')
    peer.control(f'(setf *logical-import-native-expected-disposition* :{disposition})')
    names = '("alpha" "beta")' if disposition == 'eligible' else '("alpha")'
    peer.control(f"(setf *logical-import-native-expected-names* '{names})")
    peer.control('(logical-import-native-start t)' if pending else '(logical-import-native-start)')
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        assert peer.process.poll() is None, peer.root / 'server.log'
        state = peer.control('*logical-import-native-result*')
        if ':PASSED' in state:
            peer.control('(assert (eq :passed (first *logical-import-native-result*)))')
            peer.control('(assert (not (eq :timeout (sb-thread:join-thread *logical-import-native-thread* :timeout 5 :default :timeout))))')
            peer.control('(assert (not (sb-thread:thread-alive-p *logical-import-native-thread*)))')
            return
        if ':ERROR' in state:
            raise AssertionError((state, peer.root / 'server.log'))
        time.sleep(0.05)
    raise TimeoutError(('import owner probe did not finish', peer.root))


def run_live(pending=False):
    started = time.monotonic()
    original = OwnedLisp.control
    debug = os.environ.get('PG_LISP_COPY_DEBUG')
    fired = False

    def control(peer, expression, *args, **kwargs):
        nonlocal fired
        result = original(peer, expression, *args, **kwargs)
        if expression == '(physical-recovery-restartpoint)' and not fired:
            fired = True
            before = {str(p.relative_to(peer.data)): p.read_bytes()
                      for p in peer.data.rglob('*') if p.is_file()
                      and (p.parent == peer.data / 'global' or 'pg_wal' in p.relative_to(peer.data).parts)}
            probe(peer, pending)
            if pending:
                after = {str(p.relative_to(peer.data)): p.read_bytes()
                         for p in peer.data.rglob('*') if p.is_file()
                         and (p.parent == peer.data / 'global' or 'pg_wal' in p.relative_to(peer.data).parts)}
                assert before == after, 'pending fixture published primary control or WAL'
                os.environ.pop('PG_LISP_COPY_DEBUG', None)
                raise PendingReady(peer)
        return result

    OwnedLisp.control = control
    try:
        standby['run']()
        assert fired
        assert not pending, 'pending hook did not stop before ordinary restart'
    except PendingReady as ready:
        return ready.peer
    finally:
        OwnedLisp.control = original
        if debug is not None:
            os.environ['PG_LISP_COPY_DEBUG'] = debug
    print(f'LOGICAL-IMPORT-INPUT-PASS seconds={time.monotonic() - started:.6f}', flush=True)


class PendingReady(Exception):
    def __init__(self, peer):
        self.peer = peer


def tree_image(data):
    return {str(p.relative_to(data)): p.read_bytes() for p in data.rglob('*')
            if p.is_file() and p.name != 'postmaster.pid'}


def cold_probe(peer, label, disposition='wal-level-insufficient'):
    hook = Path(__file__).with_name('logical-slot-copy-import-input-native.lisp')
    form = f'''(let ((*pgdata* (directory-pathname {lisp_string(peer.data)})) (done nil))
      (unwind-protect
           (postmaster-own-data-directory "127.0.0.1" 6543
             :startup-reconcile (lambda () (logical-import-native-cold-probe) (setf done t)))
        (when *postmaster-pid-stream* (postmaster-remove-pid-file))
        (when *postmaster-data-lock* (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil)))
      (assert done))'''
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    names = '("alpha" "beta")' if disposition == 'eligible' else '("alpha")'
    for code in ['(require :asdf)',
                 f'(push (uiop:ensure-directory-pathname {lisp_string(standby["api"]["LISP"])}) asdf:*central-registry*)',
                 '(asdf:load-system "pg")', '(in-package :pg)', f'(load {lisp_string(hook)})',
                 f'(setf *logical-import-native-expected-disposition* :{disposition})',
                 f"(setf *logical-import-native-expected-names* '{names})", form]:
        command.extend(['--eval', code])
    log = peer.root / f'import-cold-{label}.log'
    with log.open('w') as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=90)
    assert result.returncode == 0 and 'LOGICAL-IMPORT-COLD-SCOPE-PASS 3' in log.read_text(), log
    assert not (peer.data / 'postmaster.pid').exists()


def run_healthy():
    started = time.monotonic()
    source = OwnedLisp(parameters=(('wal_level', 'logical'),))
    child = OwnedLisp(parameters=(('wal_level', 'replica'),))
    try:
        source.start()
        connection = standby['RawConnection'](source.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            connection.query("SELECT * FROM pg_create_logical_replication_slot('alpha', 'pgoutput')")
            connection.query("SELECT * FROM pg_create_logical_replication_slot('beta', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (1, 'healthy base')")
        finally:
            connection.close()
        child.data.mkdir(mode=0o700)
        source.control(f'(logical-slot-base-copy {lisp_string(child.data)} "healthy_import_pin")', timeout=90)
        child.start()
        standby['wait_rows'](child, [['1']])
        child.control(f'(physical-follow-start "127.0.0.1" {source.port} "healthy_import_pin")')
        standby['insert'](source, 2)
        standby['wait_rows'](child, [['1'], ['2']])
        child.control('(physical-follow-stop)')
        child.control('(physical-recovery-restartpoint)')
        probe(child, pending=True, disposition='eligible')
        child.control('''(assert (and (eq :replica (getf *physical-recovery-state* :promotion-wal-level))
                                      (getf *physical-recovery-state* :promotion-logical-decoding-enabled)))''')
        child.control('(assert (zerop (loop for slot being the hash-values of *wal-replication-slots* count (wal-logical-slot-p slot))))')
        child.stop(immediate=True)
        before = tree_image(child.data)
        cold_probe(child, 'healthy-first', 'eligible')
        assert before == tree_image(child.data), 'healthy cold preparation changed pending inputs'
        cold_probe(child, 'healthy-retry', 'eligible')
        assert before == tree_image(child.data), 'healthy cold retry changed pending inputs'
        print(f'LOGICAL-IMPORT-HEALTHY-REPLICA-PASS seconds={time.monotonic() - started:.6f}', flush=True)
    finally:
        child.stop(immediate=True)
        source.stop(immediate=True)


def run():
    started = time.monotonic()
    run_healthy()
    run_live()
    peer = run_live(pending=True)
    before = tree_image(peer.data)
    cold_probe(peer, 'first')
    assert before == tree_image(peer.data), 'cold validation changed durable pending inputs'
    cold_probe(peer, 'retry')
    assert before == tree_image(peer.data), 'cold retry changed durable pending inputs'
    standby['startup_refused'](peer, 'logical copy requires child-owner import before promotion')
    assert before == tree_image(peer.data), 'fenced startup changed durable pending inputs'
    print(f'LOGICAL-IMPORT-LIVE-COLD-PASS seconds={time.monotonic() - started:.6f}', flush=True)


if __name__ == '__main__':
    run()

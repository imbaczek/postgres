"""Exercise import ownership in an actual compacted copied standby."""
from pathlib import Path
import runpy
import os
import signal
import subprocess
import time

standby = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-standby.py')))
OwnedLisp = standby['api']['OwnedLisp']
lisp_string = standby['lisp_string']


def probe(peer, pending=False, disposition='wal-level-insufficient', *, intent_failure=False):
    peer.control(f'(load {lisp_string(Path(__file__).with_name("logical-slot-copy-import-input-native.lisp"))})')
    peer.control(f'(setf *logical-import-native-expected-disposition* :{disposition})')
    peer.control(f'(setf *logical-import-native-intent-failure* {"t" if intent_failure else "nil"})')
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


def cold_probe(peer, label, disposition='wal-level-insufficient', *, cut=None, failure=None,
               create=False):
    hook = Path(__file__).with_name('logical-slot-copy-import-input-native.lisp')
    operation = (f'(logical-import-native-crash-pending :{cut or "never"})' if cut or failure else
                 f'(logical-import-native-cold-probe {"t" if create else "nil"})')
    form = f'''(let ((*pgdata* (directory-pathname {lisp_string(peer.data)})) (done nil))
      (unwind-protect
           (postmaster-own-data-directory "127.0.0.1" 6543
             :startup-reconcile (lambda () {operation} (setf done t)))
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
    text = log.read_text()
    if cut:
        assert result.returncode == -signal.SIGKILL, (result.returncode, log)
        assert f'LOGICAL-IMPORT-SIGKILL-CUT :{cut.upper()}' in text, log
        return
    if failure:
        assert result.returncode == 1 and failure in text, (result.returncode, log)
        return
    assert result.returncode == 0 and 'LOGICAL-IMPORT-COLD-SCOPE-PASS 3' in text, log
    assert not (peer.data / 'postmaster.pid').exists()


def pending_crash_matrix(peer, disposition):
    certificate = peer.data / 'logical-slot-copy.import'
    temporary = peer.data / 'logical-slot-copy.import.tmp'
    saved = certificate.read_bytes()
    before = tree_image(peer.data)
    cuts = [('before-import-pending-file-fsync', False),
            ('after-import-pending-file-fsync', False),
            ('before-import-pending-rename', False),
            ('after-import-pending-rename', True),
            ('before-import-pending-root-fsync', True),
            ('after-import-pending-root-fsync', True)]
    for cut, published in cuts:
        certificate.unlink()
        cold_probe(peer, f'kill-{cut}', disposition, cut=cut)
        assert certificate.exists() == published, cut
        assert temporary.exists() != published, cut
        if published:
            image = certificate.read_bytes()
            inode = certificate.stat().st_ino
            for retry in range(2):
                cold_probe(peer, f'kill-{cut}-retry-{retry}', disposition)
                assert certificate.read_bytes() == image, cut
                assert certificate.stat().st_ino == inode, cut
        else:
            cold_probe(peer, f'kill-{cut}-quarantine', disposition,
                       failure='unexpected pending import temporary file')
            assert temporary.exists() and not certificate.exists(), cut
            temporary.unlink()  # Explicit fixture repair; production never removes quarantine.
            certificate.write_bytes(saved)
            certificate.chmod(0o600)
        after = tree_image(peer.data)
        assert {k: v for k, v in after.items() if k != certificate.name} == {
            k: v for k, v in before.items() if k != certificate.name}, cut
        print(f'LOGICAL-IMPORT-SIGKILL-PASS {cut}', flush=True)
    certificate.write_bytes(saved)
    certificate.chmod(0o600)
    assert before == tree_image(peer.data), 'crash matrix changed pending source inputs'


def run_healthy(*, intent_failure=False):
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
        probe(child, pending=True, disposition='eligible', intent_failure=intent_failure)
        if not intent_failure:
            child.control('''(assert (and (eq :replica (getf *physical-recovery-state* :promotion-wal-level))
                                          (getf *physical-recovery-state* :promotion-logical-decoding-enabled)))''')
        child.control('(assert (zerop (loop for slot being the hash-values of *wal-replication-slots* count (wal-logical-slot-p slot))))')
        child.stop(immediate=True)
        if intent_failure:
            assert 'LOGICAL-IMPORT-INTENT-OWNER-EXIT-FENCED' in (child.root / 'server.log').read_text()
            before = tree_image(child.data)
            assert 'logical-slot-copy.import' not in before
            cold_probe(child, 'intent-owner-exit-create', 'eligible', create=True)
            after = tree_image(child.data)
            assert {k: v for k, v in after.items() if k != 'logical-slot-copy.import'} == before
            print('LOGICAL-IMPORT-INTENT-OWNER-EXIT-COLD-PASS', flush=True)
        else:
            pending_crash_matrix(child, 'eligible')
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
    run_healthy(intent_failure=True)
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

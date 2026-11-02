"""An adoption attempt must stop startup before cleanup or authentication writes."""
import json
from pathlib import Path
import runpy
import stat
import subprocess


BASE = Path(__file__).resolve().parents[1]
peers = runpy.run_path(str(BASE / 't/command-complete.py'))


def snapshot(directory):
    result = {}
    for path in directory.rglob('*'):
        info = path.lstat()
        identity = (info.st_dev, info.st_ino, info.st_mode)
        if stat.S_ISREG(info.st_mode):
            try:
                value = path.read_bytes()
            except PermissionError:
                value = ('unreadable', info.st_size)
        elif stat.S_ISLNK(info.st_mode):
            value = str(path.readlink())
        else:
            value = None
        result[str(path.relative_to(directory))] = (identity, value)
    return result


def check():
    server = peers['OwnedLisp']()
    print(f'artifacts: {server.root}', flush=True)
    try:
        server.start()
        client = peers['Connection'](server.port)
        try:
            client.query('CREATE TABLE fence_items (id integer)')
            client.query('INSERT INTO fence_items VALUES (42)')
        finally:
            client.close()
        stage_path = server.root / 'stage-path'
        server.control(f'''(with-open-file (out {json.dumps(str(stage_path))}
                            :direction :output :if-exists :supersede)
                          (princ (namestring (physical-backup-create-stage)) out))''')
        stage = Path(stage_path.read_text())
        sentinel = stage / 'must-survive-refusal'
        sentinel.write_text('pending stage')
        process = server.process
        server.stop()
        assert process.returncode == 0
        # An ordinary startup would recreate these defaults. The fence must run first.
        auth = [server.data / name for name in
                ('authentication.initialized', 'pg_hba.conf', 'pg_ident.conf')]
        for path in auth:
            path.unlink()
        attempt = server.data / 'physical-adoption.attempt'
        for kind in ('corrupt', 'dangling', 'directory'):
            if kind == 'corrupt':
                attempt.write_bytes(b'\xff\x00not an adoption record')
                attempt.chmod(0o000)
            elif kind == 'dangling':
                attempt.symlink_to('missing-adoption-record')
            else:
                attempt.mkdir(mode=0o700)
            before = snapshot(server.data)
            control = server.root / 'control' / f'{kind}.sock'
            log_path = server.root / f'{kind}.log'
            with log_path.open('wb') as log:
                server.process = subprocess.Popen(
                    [str(BASE / 'pg-lisp.sh'), '-p', str(server.port), '-D', str(server.data),
                     '--control-socket', str(control)], cwd=BASE, stdout=log,
                    stderr=subprocess.STDOUT)
            assert server.process.wait(timeout=60) != 0
            server.process = None
            assert 'pending offline adoption attempt requires reconciliation' in log_path.read_text()
            assert not (server.data / 'postmaster.pid').exists()
            assert not control.exists()
            assert all(not path.exists() for path in auth)
            assert sentinel.read_text() == 'pending stage'
            assert before == snapshot(server.data), 'refused startup changed existing PGDATA files'
            print(f'adoption-fence: {kind} entry refuses before cleanup/auth and retires PID PASS', flush=True)
            if kind == 'directory':
                attempt.rmdir()
            else:
                attempt.unlink()
        server.start()
        assert not stage.exists(), 'ordinary startup did not retire the abandoned stage'
        assert all(path.exists() for path in auth)
        client = peers['Connection'](server.port)
        try:
            assert client.query('SELECT id FROM fence_items') == [['42']]
        finally:
            client.close()
        print('adoption-fence: ordinary startup succeeds after fixture attempt removal PASS', flush=True)
    finally:
        server.stop()


if __name__ == '__main__':
    check()

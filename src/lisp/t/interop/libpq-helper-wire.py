"""Exercise the helper's real libpq authentication, TLS and option semantics."""
import argparse
import importlib
import os
from pathlib import Path
import struct
import subprocess
import sys

peer = importlib.import_module('backend-admission')
ipc = importlib.import_module('libpq-helper-tests')


def rows(body):
    position = 0

    def take(size):
        nonlocal position
        assert size <= len(body) - position
        value = body[position:position + size]
        position += size
        return value

    def number():
        return struct.unpack('!I', take(4))[0]

    def text():
        size = number()
        return None if size == 0xFFFFFFFF else take(size).decode('utf-8')

    assert take(1) == b'\0', 'query did not finish idle'
    tag = text()
    columns, count = number(), number()
    assert 0 <= columns <= 64 and count == 1
    for _ in range(columns):
        text()
        metadata = struct.unpack('!IiIiii', take(24))
        assert metadata[-1] == 0, 'query did not return text format'
    result = [[text() for _ in range(columns)] for _ in range(count)]
    assert position == len(body)
    return tag, result


def query(helper, statement):
    kind, body = helper.call(b'Q', struct.pack('!I', 4096) + ipc.string(statement))
    assert kind == b'T', (kind, body)
    return rows(body)[1]


def exercise(observer, conninfo, expected_name, env, *, success=True):
    helper = ipc.Helper(env=env)
    try:
        assert helper.version >= 170000, 'TLS negotiation oracle needs libpq 17 or newer'
        kind, body = helper.call(b'O', ipc.string(conninfo) + ipc.string('cluster-fallback'))
        if not success:
            assert kind == b'!', (kind, body)
            assert body[:5] == b'08001'
            return
        assert kind == b'O', (kind, body)
        values = query(helper, "SELECT current_setting('application_name'), "
                       "current_setting('statement_timeout'), pg_backend_pid()")
        assert values[0][:2] == [expected_name, '1234ms'], values
        pid = int(values[0][2])
        assert observer.query(f'SELECT ssl FROM pg_stat_ssl WHERE pid={pid}') == [['t']]
        assert helper.call(b'X') == (b'A', b'')
        assert helper.process.wait(timeout=2) == 0
    finally:
        helper.close()


def run(base, root, executable, port, processes):
    directory = root / 'data'
    with (root / 'initdb.log').open('w') as output:
        subprocess.run([str(executable.parent / 'initdb'), '-D', str(directory),
                        '-A', 'trust', '-U', 'postgres', '--no-locale'],
                       stdout=output, stderr=output, check=True)
    with (root / 'certificate.log').open('w') as output:
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(root / 'server.key'), '-out', str(root / 'server.crt'),
                        '-days', '1', '-subj', '/CN=localhost',
                        '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'],
                       stdout=output, stderr=output, check=True, timeout=20)
    (root / 'server.key').chmod(0o600)
    (directory / 'postgresql.conf').write_text(
        f"ssl=on\nssl_cert_file='{root / 'server.crt'}'\nssl_key_file='{root / 'server.key'}'\n")
    (directory / 'pg_hba.conf').write_text(
        'host all postgres 127.0.0.1/32 trust\n'
        'host all transport_receiver 127.0.0.1/32 scram-sha-256\n')
    process = peer.start(base, root, executable, port, 'server', False, processes)
    observer = peer.connect(port)
    try:
        observer.query("CREATE ROLE transport_receiver LOGIN REPLICATION PASSWORD 'public-fixture-password'")
        password = root / 'passfile'
        password.write_text(f'localhost:{port}:postgres:transport_receiver:public-fixture-password\n'
                            f'other.invalid:{port}:postgres:transport_receiver:public-fixture-password\n')
        password.chmod(0o600)
        env = {key: value for key, value in os.environ.items() if not key.startswith('PG')}
        common = (f'host=localhost hostaddr=127.0.0.1 port={port} dbname=postgres '
                  f'user=transport_receiver passfile={password} sslmode=verify-full '
                  f"sslrootcert={root / 'server.crt'} options='-c statement_timeout=1234'")
        exercise(observer, common + ' application_name=explicit', 'explicit', env)
        exercise(observer, common, 'cluster-fallback', env)
        exercise(observer, common, 'environment-name', dict(env, PGAPPNAME='environment-name'))
        exercise(observer, common + ' sslnegotiation=direct', 'cluster-fallback', env)
        service = root / 'service.conf'
        service.write_text('[source]\n'
                           f'host=localhost\nhostaddr=127.0.0.1\nport={port}\ndbname=postgres\n'
                           f'user=transport_receiver\npassfile={password}\nsslmode=verify-full\n'
                           f'sslrootcert={root / "server.crt"}\noptions=-c statement_timeout=1234\n'
                           'application_name=service-name\n')
        service.chmod(0o600)
        service_env = dict(env, PGSERVICEFILE=str(service))
        exercise(observer, 'service=source', 'service-name', service_env)
        exercise(observer, 'service=source application_name=override', 'override', service_env)
        exercise(observer, common.replace('host=localhost ', 'host=other.invalid '), None, env, success=False)
        wrong = root / 'wrong-passfile'
        wrong.write_text(f'localhost:{port}:postgres:transport_receiver:wrong-fixture-password\n')
        wrong.chmod(0o600)
        exercise(observer, common.replace(str(password), str(wrong)), None, env, success=False)
        print('libpq-helper: SCRAM/passfile, verified TLS/direct TLS, services, options and appname precedence PASS',
              flush=True)
    finally:
        observer.close()
    peer.stop(base, root, process)


def main():
    if not __debug__:
        raise RuntimeError('Run the assertion-based oracle without Python -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('base', type=Path)
    parser.add_argument('root', type=Path)
    parser.add_argument('executable', type=Path)
    parser.add_argument('port', type=int)
    args = parser.parse_args()
    base, root = args.base.resolve(), args.root.resolve()
    root.mkdir(mode=0o700)
    processes = []
    try:
        run(base, root, args.executable.resolve(), args.port, processes)
    finally:
        if (root / 'owned').exists():
            subprocess.run([sys.executable, str(base / 't/interop/owned-peer.py'),
                            'cleanup', str(root / 'owned')], check=True)
        for process in processes:
            process.wait(timeout=10)


if __name__ == '__main__':
    main()

"""Capture the unmodified Patroni 4.1.5 follow writer's complete configuration.

This oracle writes to a fresh C cluster and checks its settings with postgres -C.
An optional native executable checks the same files through its metadata path;
neither mode starts a server, coordinator, or DCS.
"""
import argparse
import importlib.metadata
import json
from pathlib import Path
import subprocess

from patroni.dcs import Member
from patroni.postgresql import Postgresql
from patroni.postgresql.config import ConfigHandler
from patroni.postgresql.mpp import Null
from psycopg2.extensions import parse_dsn


def setting(executable, directory, name):
    result = subprocess.run(
        [str(executable), '-D', str(directory), '-C', name], text=True,
        capture_output=True, timeout=30)
    assert result.returncode == 0, (str(executable), name, result.returncode, result.stderr)
    return result.stdout.rstrip('\n')


def writer(directory, binary, target_port):
    parameters = {name: option[0] for name, option in ConfigHandler.CMDLINE_OPTIONS.items()
                  if option[0] is not None and option[2] <= 200000}
    configuration = dict(
        name='follow_one', scope='pg-lisp-follow', data_dir=str(directory),
        bin_dir=str(binary), listen=f'127.0.0.1:{target_port}',
        connect_address=f'127.0.0.1:{target_port}', retry_timeout=10,
        pgpass=str(directory.parent / 'replication passfile'), parameters=parameters,
        authentication={'superuser': {'username': 'postgres'},
                        'replication': {'username': 'replicator',
                                        'password': 'test:password\\with space'}},
        pg_hba=['host all all 127.0.0.1/32 trust'])
    return Postgresql(configuration, Null())


def write_follow(node, member):
    node.config.write_recovery_conf(node.config.build_recovery_params(member))
    node.config.write_postgresql_conf()


def capture(node, executable, label):
    # Include every server parameter selected by the real ConfigHandler, rather
    # than reducing the writer contract to just the three follow settings.
    names = set(node.config._server_parameters)
    names.update(('hba_file', 'ident_file', 'primary_conninfo', 'primary_slot_name',
                  'recovery_target_timeline', 'recovery_target', 'recovery_target_lsn',
                  'recovery_target_name', 'recovery_target_time', 'recovery_target_xid'))
    values = {name: setting(executable, node.data_dir, name) for name in sorted(names)}
    Path(node.data_dir, f'writer-{label}.conf').write_bytes(Path(node.config.postgresql_conf).read_bytes())
    return values


def check_follow(values, directory, port, target_port):
    conninfo = parse_dsn(values['primary_conninfo'])
    assert conninfo['host'] == '127.0.0.1' and conninfo['port'] == str(port), conninfo
    assert conninfo['user'] == 'replicator' and conninfo['application_name'] == 'follow_one', conninfo
    assert conninfo['passfile'] == str(directory.parent / 'replication passfile'), conninfo
    assert 'password' not in conninfo
    assert conninfo['sslmode'] == 'prefer' and conninfo['channel_binding'] == 'prefer', conninfo
    assert conninfo['gssencmode'] == 'prefer' and conninfo['sslnegotiation'] == 'postgres', conninfo
    assert values['primary_slot_name'] == 'follow_one'
    assert values['recovery_target_timeline'] == 'latest'
    assert all(values[key] == '' for key in (
        'recovery_target', 'recovery_target_lsn', 'recovery_target_name',
        'recovery_target_time', 'recovery_target_xid'))
    assert values['cluster_name'] == 'pg-lisp-follow' and values['wal_level'] == 'replica'
    assert values['port'] == str(target_port) and values['listen_addresses'] == '127.0.0.1'
    assert values['hba_file'] == str(directory / 'pg_hba.conf')
    assert values['ident_file'] == str(directory / 'pg_ident.conf')
    assert values['max_wal_senders'] == '10' and values['max_replication_slots'] == '10'
    assert values['wal_keep_size'] == '128'
    assert (directory / 'standby.signal').is_file() and not (directory / 'recovery.signal').exists()
    passfile = directory.parent / 'replication passfile'
    assert passfile.stat().st_mode & 0o777 == 0o600
    assert passfile.read_text() == f'127.0.0.1:{port}:*:replicator:test\\:password\\\\with space\n'


def main():
    if not __debug__:
        raise RuntimeError('This assertion-based oracle requires Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reference_bin', type=Path)
    parser.add_argument('root', type=Path, help='new directory for this oracle')
    parser.add_argument('--native', type=Path, help='also require native -C to accept all emitted settings')
    parser.add_argument('--source-port', type=int, default=6996)
    parser.add_argument('--target-port', type=int, default=6997)
    args = parser.parse_args()
    assert importlib.metadata.version('patroni') == '4.1.5'
    assert importlib.metadata.version('psycopg2-binary') == '2.9.11'
    root = args.root.resolve()
    root.mkdir(mode=0o700)
    directory = root / "data 'one'"
    binary = args.reference_bin.resolve()
    with (root / 'initdb.log').open('w') as output:
        subprocess.run([str(binary / 'initdb'), '-D', str(directory), '-U', 'postgres',
                        '-A', 'trust', '--no-locale'], check=True, stdout=output, stderr=output)
    # Native bases do not inherit initdb's stock C tuning template. Keep the
    # included base empty so this contract covers what Patroni itself emits.
    (directory / 'postgresql.conf').write_text('# Empty base configuration for the writer oracle.\n')
    node = writer(directory, binary, args.target_port)
    assert node.major_version == 200000
    snapshots = {}
    for label, port in (('attach', args.source_port), ('replace', args.source_port + 2)):
        member = Member(0, 'upstream', None, {'conn_url': f'postgres://127.0.0.1:{port}/postgres'})
        write_follow(node, member)
        values = capture(node, binary / 'postgres', label)
        check_follow(values, directory, port, args.target_port)
        snapshots[label] = values
        if args.native:
            for name, expected in values.items():
                actual = setting(args.native.resolve(), directory, name)
                assert actual == expected, (label, name, actual, expected)
    write_follow(node, None)
    snapshots['clear'] = capture(node, binary / 'postgres', 'clear')
    assert snapshots['clear']['primary_conninfo'] == ''
    assert snapshots['clear']['primary_slot_name'] == ''
    assert (directory / 'standby.signal').is_file()
    if args.native:
        for name, expected in snapshots['clear'].items():
            actual = setting(args.native.resolve(), directory, name)
            assert actual == expected, ('clear', name, actual, expected)
    (root / 'settings.json').write_text(json.dumps(snapshots, indent=2, sort_keys=True) + '\n')
    print(f'patroni-follow-config: complete attach/replace/clear writer output and C metadata PASS; artifacts={root}')


if __name__ == '__main__':
    main()

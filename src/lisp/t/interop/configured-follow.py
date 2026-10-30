"""Configure an owned standby through files and reload, without retarget calls."""
import os
import signal
import struct
import subprocess
from pathlib import Path

from parking_wire import RawConnection


def raw_setting(port, statement, result_format=None):
    connection = RawConnection(port, 'configured-setting')
    rows = []

    def send(kind, body):
        connection.socket.sendall(kind + struct.pack('!I', len(body) + 4) + body)

    try:
        if result_format is None:
            send(b'Q', statement.encode() + b'\0')
        else:
            parameter = b'primary_conninfo'
            assert "'primary_conninfo'" in statement
            prepared = statement.replace("'primary_conninfo'", '$1')
            send(b'P', b'\0' + prepared.encode() + b'\0' + struct.pack('!HI', 1, 25))
            send(b'B', b'\0\0' + struct.pack('!HHI', 0, 1, len(parameter))
                 + parameter + struct.pack('!HH', 1, result_format))
            send(b'E', b'\0' + struct.pack('!I', 0))
            send(b'S', b'')
        while True:
            kind, body = connection.receive()
            assert kind != b'E', body
            if kind == b'D':
                assert struct.unpack_from('!H', body)[0] == 1
                size = struct.unpack_from('!i', body, 2)[0]
                assert size >= 0 and len(body) == size + 6
                rows.append(body[6:])
            elif kind == b'Z':
                assert body == b'I'
                return rows
    finally:
        connection.close()


class ConfiguredFollow:
    def __init__(self, api, peer, identity, connection):
        self.api, self.peer, self.identity, self.connection = api, peer, identity, connection
        self.data, self.port = api['data'], api['replica_port']
        self.control, self.query, self.wait = api['control'], api['query'], api['wait']
        (self.data / 'postgresql.conf').write_text('')

    def conninfo(self, port, raw=True):
        value = (f'host=127.0.0.1 port={port} user=postgres dbname=unused '
                 'sslmode=disable gssencmode=disable password=public-fixture').encode()
        return value + (b'\xff\x80' if raw else b'')

    def configure(self, conninfo, slot='', comment=False):
        quoted = conninfo.replace(b'\\', b'\\\\').replace(b"'", b"\\'")
        body = (b'# provenance only\n' if comment else b'')
        body += b"primary_conninfo='" + quoted + b"'\nprimary_slot_name='" + slot.encode() + b"'\n"
        (self.data / 'postgresql.auto.conf').write_bytes(body)
        self.reload()

    def reload(self):
        assert self.query(self.connection, 'SELECT pg_reload_conf()') == [(True,)]

    def streaming(self, slot):
        self.wait(lambda: self.query(self.connection,
                  'SELECT status,slot_name FROM pg_catalog.pg_stat_get_wal_receiver()') == [('streaming', slot)])

    def stopped(self):
        self.wait(lambda: self.query(self.connection,
                  'SELECT pid FROM pg_catalog.pg_stat_get_wal_receiver()') == [(None,)])
        self.wait(lambda: ':DISABLED T' in self.control('sibling.sock',
                  '(list :disabled (not (physical-follow-controller-enabled-p (physical-follow-read-config))))'))
        self.control('sibling.sock', '(assert (not *physical-recovery-failed-p*))')

    def count(self, count):
        self.wait(lambda: self.query(self.connection,
                  'SELECT count(*) FROM promotion_sql_items') == [(count,)])

    def restart(self):
        self.connection.close()
        assert self.api['stop_peer'](self.peer, self.identity, crash=True) == -signal.SIGKILL
        self.api['peer'] = None
        self.peer, self.identity = self.api['start_peer']()
        self.api['peer'], self.api['socket_identity'] = self.peer, self.identity
        self.connection = self.api['connect'](self.port)
        self.api['connections'].append(self.connection)

    def raw_metadata(self, expected):
        for statement in ('SHOW primary_conninfo', "SELECT current_setting('primary_conninfo')",
                          "SELECT setting FROM pg_settings WHERE name='primary_conninfo'"):
            assert raw_setting(self.port, statement) == [expected]
            if statement.startswith('SELECT'):
                for result_format in (0, 1):
                    assert raw_setting(self.port, statement, result_format) == [expected]
        metadata_env = dict(os.environ, XDG_CACHE_HOME=str(self.api['root'] / 'metadata-cache'))
        commands = ([str(self.api['base'] / 'pg-lisp.sh')],
                    [str(Path(os.environ['PG_CONFIG']).with_name('postgres'))])
        for command in commands:
            result = subprocess.run([*command, '-D', str(self.data), '-C', 'primary_conninfo'],
                                    capture_output=True, env=metadata_env, timeout=60, check=True)
            assert result.stdout == expected + b'\n'


def exercise(api, peer, identity, sibling, source, promoted):
    if not __debug__:
        raise RuntimeError('configured follow assertions require Python without -O')
    flow = ConfiguredFollow(api, peer, identity, sibling)
    query, control = flow.query, flow.control
    for slot in ('configured_a', 'configured_b'):
        query(source, f"SELECT pg_create_physical_replication_slot('{slot}',true)")
    conninfo = flow.conninfo(api['source_port'])
    flow.configure(conninfo, 'configured_a')
    flow.streaming('configured_a')
    flow.raw_metadata(conninfo)
    query(source, "INSERT INTO promotion_sql_items VALUES (2,'configured-first')")
    flow.count(2)
    control('sibling.sock', '(defparameter *configured-before* (list *physical-follow-desired* *physical-follow-runtime* (physical-follow-runtime-receiver-thread *physical-follow-runtime*) (physical-follow-runtime-replay-thread *physical-follow-runtime*)))')
    flow.configure(conninfo, 'configured_a', comment=True)
    flow.wait(lambda: query(flow.connection, "SELECT sourceline FROM pg_settings WHERE name='primary_slot_name'") == [(3,)])
    control('sibling.sock', '(assert (and (eq (first *configured-before*) *physical-follow-desired*) (eq (second *configured-before*) *physical-follow-runtime*) (eq (third *configured-before*) (physical-follow-runtime-receiver-thread *physical-follow-runtime*)) (eq (fourth *configured-before*) (physical-follow-runtime-replay-thread *physical-follow-runtime*))))')
    flow.configure(conninfo, 'configured_b')
    flow.streaming('configured_b')
    assert query(source, "SELECT active FROM pg_replication_slots WHERE slot_name='configured_a'") == [(False,)]
    query(flow.connection, 'SELECT pg_wal_replay_pause()')
    query(source, "INSERT INTO promotion_sql_items VALUES (3,'buffered-before-clear')")
    end = query(source, 'SELECT pg_current_wal_flush_lsn()::text')[0][0]
    flow.wait(lambda: query(flow.connection, f"SELECT pg_last_wal_receive_lsn()>='{end}'::pg_lsn") == [(True,)])
    control('sibling.sock', '(defparameter *configured-clear-runtime* *physical-follow-runtime*) (defparameter *configured-clear-replay* (physical-follow-runtime-replay-thread *physical-follow-runtime*))')
    flow.configure(b'')
    flow.stopped()
    control('sibling.sock', '(assert (and (eq *configured-clear-runtime* *physical-follow-runtime*) (eq *configured-clear-replay* (physical-follow-runtime-replay-thread *physical-follow-runtime*)) *physical-replay-paused-p* (not (physical-follow-controller-enabled-p (physical-follow-read-config)))))')
    query(flow.connection, 'SELECT pg_wal_replay_resume()')
    flow.count(3)
    unavailable = flow.conninfo(1, raw=False)
    flow.configure(unavailable, 'configured_a')
    flow.wait(lambda: query(flow.connection, 'SHOW primary_conninfo') == [(unavailable.decode(),)])
    flow.stopped()
    query(source, "INSERT INTO promotion_sql_items VALUES (4,'old-source-must-stay-disabled')")
    flow.configure(b'not valid libpq syntax', 'configured_a')
    flow.wait(lambda: query(flow.connection, "SHOW primary_conninfo") == [('not valid libpq syntax',)])
    flow.stopped()
    assert query(flow.connection, 'SELECT count(*) FROM promotion_sql_items') == [(3,)]
    flow.configure(conninfo, 'configured_a')
    flow.streaming('configured_a')
    flow.count(4)
    flow.restart()
    flow.streaming('configured_a')
    flow.count(4)
    (flow.data / 'postgresql.auto.conf').write_text('')
    flow.reload()
    flow.stopped()
    assert (flow.data / 'physical-follow-managed.control').is_file()
    flow.restart()
    flow.stopped()
    query(source, "INSERT INTO promotion_sql_items VALUES (5,'removed-config-stays-replay-only')")
    assert query(flow.connection, 'SELECT count(*) FROM promotion_sql_items') == [(4,)]
    flow.wait(lambda: query(promoted, 'SELECT count(*) FROM promotion_sql_items') == [(5,)])
    assert query(promoted, 'SELECT pg_promote(true,30)') == [(True,)]
    query(promoted, "SELECT pg_create_physical_replication_slot('configured_child',true)")
    query(promoted, "INSERT INTO promotion_sql_items VALUES (6,'configured-child-source')")
    flow.configure(flow.conninfo(api['promoted_port']), 'configured_child')
    flow.streaming('configured_child')
    flow.count(6)
    assert query(flow.connection, 'SELECT pg_promote(true,30)') == [(True,)]
    assert not (flow.data / 'physical-follow-managed.control').exists()
    query(flow.connection, "INSERT INTO promotion_sql_items VALUES (7,'configured-promoted-writer')")
    flow.restart()
    assert query(flow.connection, 'SELECT pg_is_in_recovery(),count(*) FROM promotion_sql_items') == [(False, 7)]
    print('configured-follow: raw metadata, first attach, unchanged reload and slot replacement PASS', flush=True)
    print('configured-follow: clear preserves paused replay, failed replacement and retry, cold removal ownership PASS', flush=True)
    print('configured-follow: descendant source, promotion stops reconciler and retires claim, primary restart PASS', flush=True)
    return flow.peer, flow.identity, flow.connection

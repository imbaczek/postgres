"""Exercise logical shutdown through owned SQL, replication and control sockets."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import sys
import tempfile
import time
import traceback

from binary_values import frame
from parking_wire import RawConnection, WireError


BASE = Path(__file__).resolve().parents[2]
CASES = {
    'fast-pgoutput': ('pgoutput', 'INT', 'backpressure'),
    'smart-pgoutput': ('pgoutput', 'TERM', 'filtered'),
    'fast-private': ('pg_lisp', 'INT', 'partial-abort'),
    'smart-private': ('pg_lisp', 'TERM', 'async'),
    'copydone': ('pgoutput', 'INT', 'copydone'),
    'disconnect-pgoutput': ('pgoutput', 'INT', 'disconnect'),
    'disconnect-private': ('pg_lisp', 'INT', 'disconnect'),
    'quit-pgoutput': ('pgoutput', 'INT', 'stalled'),
    'quit-private': ('pg_lisp', 'INT', 'stalled'),
    'incomplete-batch': ('pgoutput', 'INT', 'failure'),
}
HEADER = ('db_state', 'end', 'durable', 'checkpoint', 'queued', 'connections',
          'backends', 'senders', 'streams', 'active', 'stage')
SLOT = ('name', 'kind', 'state', 'stopped', 'limit', 'target', 'outcome',
        'confirmed', 'active', 'disk_confirmed')
LARGE_LABEL = 'x' * 262144


def lisp_string(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def position(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) + int(low, 16)


def feedback(value):
    return frame(b'd', struct.pack('!cQQQqB', b'r', value, value, value, 0, 0))


def insert_values(payload):
    assert payload[:1] == b'I' and payload[5:6] == b'N'
    count = struct.unpack_from('!H', payload, 6)[0]
    offset, values = 8, []
    for _ in range(count):
        tag = payload[offset:offset + 1]
        offset += 1
        assert tag == b't', tag
        length = struct.unpack_from('!I', payload, offset)[0]
        offset += 4
        values.append(payload[offset:offset + length])
        offset += length
    assert offset == len(payload)
    return values


class Case:
    def __init__(self, root, name, cache):
        self.root, self.name, self.cache = root, name, cache
        self.plugin, self.mode, self.scenario = CASES[name]
        self.directory = root / name
        self.directory.mkdir(mode=0o700)
        self.data = self.directory / 'data'
        self.server = self.sql = self.client = self.pidfd = None
        self.environment = dict(os.environ, XDG_CACHE_HOME=str(cache))
        self.result = dict(name=name, plugin=self.plugin, mode=self.mode, complete=False,
                           frames=[], rows=[], commits=[], last_ack=0, timings={}, snapshots={})

    def alive(self):
        return self.server.poll() is None and not select.select([self.pidfd], [], [], 0)[0]

    def wait_until(self, predicate, message, timeout=30):
        deadline = time.monotonic() + timeout
        while not predicate():
            assert self.alive(), f'{message}, server exited {self.server.returncode}'
            assert time.monotonic() < deadline, message
            time.sleep(0.01)

    def wait_marker(self, name):
        path = self.directory / name
        self.wait_until(path.exists, f'missing native marker {name}')
        return path.read_text()

    def control(self, form, name):
        path = self.directory / 'control.sock'
        assert path.stat().st_uid == os.getuid() and path.stat().st_mode & 0o777 == 0o700
        with socket.socket(socket.AF_UNIX) as peer:
            peer.settimeout(30)
            peer.connect(str(path))
            request = f'(assert (= (sb-posix:getpid) {self.server.pid}))\n{form}\n(quit)\n'
            peer.sendall(request.encode())
            output = bytearray()
            while chunk := peer.recv(65536):
                output.extend(chunk)
        (self.directory / f'{name}.log').write_bytes(output)
        assert b';; error:' not in output and b';; read error:' not in output, output.decode()

    def call(self, function, name):
        self.control(f'({function} #p{lisp_string(str(self.directory) + "/")})', name)

    def signal(self, mode):
        subprocess.run([sys.executable, str(BASE / 't/interop/signal-owned-server.py'),
                        str(self.server.pid), str(self.data), mode], check=True, timeout=10)

    def start(self):
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            self.port = reservation.getsockname()[1]
        with (self.directory / 'server.log').open('xb') as log:
            self.server = subprocess.Popen(
                [sys.executable, str(BASE / 't/interop/owned-peer.py'), 'start',
                 str(self.root / 'owned'), str(self.data), str(BASE / 'pg-lisp.sh'),
                 '-p', str(self.port), '-D', str(self.data), '--control-socket',
                 str(self.directory / 'control.sock')], env=self.environment,
                stdout=log, stderr=subprocess.STDOUT)
        self.pidfd = os.pidfd_open(self.server.pid)

        def owned_listener():
            listeners = subprocess.check_output(
                ['ss', '-H', '-ltnp', f'sport = :{self.port}'], text=True)
            return f'pid={self.server.pid},' in listeners

        self.wait_until(owned_listener, 'server did not own its SQL listener', timeout=120)
        assert int((self.data / 'postmaster.pid').read_text().splitlines()[0]) == self.server.pid
        self.sql = RawConnection(self.port, 'logical-shutdown-sql')
        source = BASE / 't/logical-shutdown-native.lisp'
        self.control(f'(progn (load #p{lisp_string(source)}) '
                     f'(logical-shutdown-native-install #p{lisp_string(str(self.directory) + "/")}))',
                     'install')

    def receive(self, acknowledge=True):
        kind, body = self.client.receive()
        entry = dict(type=kind.decode(), length=len(body), sha256=hashlib.sha256(body).hexdigest())
        if len(body) <= 4096:
            entry['body_hex'] = body.hex()
        self.result['frames'].append(entry)
        assert kind != b'E', self.client.error_fields(body) if kind == b'E' else ''
        point = None
        if kind == b'd' and body[:1] == b'k':
            point = struct.unpack_from('!Q', body, 1)[0]
            entry['keepalive'] = point
        elif kind == b'd' and body[:1] == b'w':
            payload = body[25:]
            if payload[:1] == b'C':
                point = struct.unpack_from('!Q', payload, 10)[0]
                self.result['commits'].append(point)
            elif payload[:1] == b'I':
                values = insert_values(payload)
                identifier = int(values[0])
                assert values[1] == (LARGE_LABEL.encode() if identifier == 44 else b'plain')
                self.result['rows'].append(identifier)
            elif self.plugin == 'pg_lisp' and payload[:1] == b'M':
                prefix = payload[10:payload.index(0, 10)]
                if prefix == b'pg_lisp/progress':
                    point = struct.unpack_from('!Q', payload, 2)[0]
        if point is not None and (acknowledge or point <= self.result['last_ack']):
            self.client.socket.sendall(feedback(point))
            self.result['last_ack'] = max(point, self.result['last_ack'])
        return kind, body

    def read_commit(self, acknowledge=True):
        previous = len(self.result['commits'])
        while len(self.result['commits']) == previous:
            self.receive(acknowledge)
        return self.result['commits'][-1]

    def setup(self):
        self.sql.query('CREATE TABLE shutdown_probe(id integer,label text)')
        self.sql.query('CREATE TABLE shutdown_filtered(id integer)')
        self.sql.query('CREATE PUBLICATION shutdown_pub FOR TABLE shutdown_probe')
        self.client = RawConnection(self.port, 'logical-shutdown-reader', replication='database')
        self.client.socket.settimeout(60)
        option = ' NOEXPORT_SNAPSHOT' if self.plugin == 'pgoutput' else ''
        row = self.client.query(f'CREATE_REPLICATION_SLOT shutdown_slot LOGICAL {self.plugin}{option}')[0]
        self.result['start'] = position(row[1])
        options = " (proto_version '1', publication_names 'shutdown_pub')" if self.plugin == 'pgoutput' else ''
        self.client.socket.sendall(frame(b'Q',
            f'START_REPLICATION SLOT shutdown_slot LOGICAL {row[1]}{options}'.encode() + b'\0'))
        kind, _ = self.receive()
        assert kind == b'W'
        if self.plugin == 'pg_lisp':
            self.client.socket.sendall(feedback(self.result['start']))
            self.result['last_ack'] = self.result['start']
        self.sql.query("INSERT INTO shutdown_probe VALUES(42,'plain')")
        self.read_commit()

    def snapshot(self, name):
        lines = self.wait_marker(name).splitlines()
        values = lines[0].split()
        assert len(values) == len(HEADER), values
        result = {key: value if key == 'stage' else int(value) for key, value in zip(HEADER, values)}
        result['slots'] = []
        for line in lines[1:]:
            values = line.split()
            assert len(values) == len(SLOT), values
            result['slots'].append({key: value if key in ('name', 'kind', 'state', 'outcome') else int(value)
                                    for key, value in zip(SLOT, values)})
        self.result['snapshots'][name] = result
        return result

    def checkpoint(self, unconfirmed=False):
        snapshot = self.snapshot('checkpoint')
        assert snapshot['queued'] == 0 and snapshot['checkpoint'] == snapshot['durable']
        assert snapshot['stage'] == 'STOPPING' and len(snapshot['slots']) == 1
        slot = snapshot['slots'][0]
        assert slot['target'] == -1 and slot['stopped'] == 1
        assert slot['limit'] >= self.result['commits'][-1]
        assert slot['limit'] < snapshot['checkpoint']
        if unconfirmed:
            assert slot['confirmed'] < slot['limit']
        return slot

    def finish(self, outcome='ACKNOWLEDGED', wire=True):
        if wire:
            try:
                while True:
                    self.receive()
            except (EOFError, ConnectionResetError) as error:
                self.result['transport_close'] = type(error).__name__
            terminal = [item for item in self.result['frames'] if item['type'] not in ('d', 'W')]
            assert len(terminal) == 1 and terminal[0]['type'] == 'C'
            assert terminal[0]['body_hex'] == b'COPY 0\0'.hex(), terminal
        self.server.wait(timeout=30)
        assert self.server.returncode == 0
        clean = self.snapshot('clean')
        assert all(clean[key] == 0 for key in ('queued', 'connections', 'backends', 'senders', 'streams', 'active'))
        assert clean['stage'] == 'FINISHED' and len(clean['slots']) == 1
        slot = clean['slots'][0]
        assert slot['state'] == 'RELEASED' and slot['outcome'] == outcome and slot['active'] == 0
        assert slot['confirmed'] == slot['disk_confirmed']
        if outcome == 'ACKNOWLEDGED':
            assert slot['confirmed'] >= slot['target'] == slot['limit']
        else:
            assert slot['confirmed'] < slot['target'] == slot['limit']
        assert 99 not in self.result['rows']

    def smart(self):
        if self.scenario == 'async':
            self.call('logical-shutdown-native-pause-writer', 'pause-writer')
            self.wait_marker('writer-ready')
            self.sql.query('SET synchronous_commit=off')
        self.signal('TERM')
        self.wait_until(lambda: 'received smart shutdown request' in (self.directory / 'server.log').read_text(),
                        'smart shutdown request not observed')
        assert not (self.directory / 'dirty').exists()
        self.sql.query("INSERT INTO shutdown_probe VALUES(43,'plain')")
        if self.scenario == 'async':
            self.call('logical-shutdown-native-record-async', 'async')
            accepted, flushed = map(int, self.wait_marker('async').split())
            assert accepted > flushed
            self.result['async_end'] = accepted
        else:
            self.sql.query('INSERT INTO shutdown_filtered VALUES(91)')
        assert not (self.directory / 'dirty').exists()
        self.sql.close()
        self.sql = None
        if self.scenario == 'async':
            self.wait_marker('writer-stopping')
            (self.directory / 'release-writer').touch()
        slot = self.checkpoint(unconfirmed=True)
        if self.scenario == 'async':
            assert slot['limit'] >= self.result['async_end']
        self.finish()
        assert 43 in self.result['rows']

    def fast(self):
        if self.scenario == 'backpressure':
            self.control('(logical-shutdown-native-small-output)', 'small-output')
            self.client.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            (self.directory / 'arm-output').touch()
            self.sql.query(f"INSERT INTO shutdown_probe VALUES(44,'{LARGE_LABEL}')")
            self.wait_marker('backpressure')
            self.sql.query('INSERT INTO shutdown_filtered VALUES(91)')
        else:
            self.sql.query("INSERT INTO shutdown_probe VALUES(43,'plain')")
            held = self.read_commit(acknowledge=False)
            partial = feedback(held)
            (self.directory / 'arm-partial').touch()
            self.client.socket.sendall(partial[:7])
            assert 0 < int(self.wait_marker('partial')) <= 7
        self.sql.query('BEGIN')
        self.sql.query("INSERT INTO shutdown_probe VALUES(99,'plain')")
        self.signal('INT')
        self.checkpoint(unconfirmed=True)
        if self.scenario == 'backpressure':
            self.client.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)
            (self.directory / 'release-output').touch()
        else:
            self.client.socket.sendall(partial[7:])
        self.finish()
        assert (44 if self.scenario == 'backpressure' else 43) in self.result['rows']

    def departure(self):
        self.sql.query("INSERT INTO shutdown_probe VALUES(43,'plain')")
        self.read_commit(acknowledge=False)
        self.signal('INT')
        self.checkpoint(unconfirmed=True)
        draining = self.snapshot('draining')
        assert draining['slots'][0]['confirmed'] < draining['slots'][0]['target']
        assert not (self.directory / 'clean').exists() and self.alive()
        if self.scenario == 'disconnect':
            self.client.close()
            self.client = None
            self.finish(outcome='ABANDONED', wire=False)
        else:
            self.signal('QUIT')
            self.server.wait(timeout=10)
            assert self.server.returncode == 0 and not (self.directory / 'clean').exists()

    def copydone(self):
        self.client.socket.sendall(frame(b'c', b''))
        messages = []
        while not messages or messages[-1][0] != b'Z':
            kind, body = self.client.receive()
            if kind == b'd':
                continue
            assert kind != b'E'
            messages.append((kind, body))
        self.result['copydone_frames'] = [dict(type=kind.decode(), body_hex=body.hex())
                                           for kind, body in messages]
        assert messages == [(b'c', b''), (b'C', b'COPY 0\0'),
                            (b'C', b'START_REPLICATION\0'), (b'Z', b'I')], messages
        self.result['post_copydone_query'] = self.client.query('SELECT 1')
        assert self.result['post_copydone_query'] == [['1']]
        self.client.close()
        self.client = None
        self.sql.close()
        self.sql = None
        self.signal('INT')
        self.server.wait(timeout=10)
        assert self.server.returncode == 0 and not self.snapshot('clean')['slots']

    def failure(self):
        self.call('logical-shutdown-native-install-failure', 'install-failure')
        try:
            self.sql.query("INSERT INTO shutdown_probe VALUES(99,'plain')")
        except (WireError, EOFError, ConnectionResetError):
            pass
        else:
            raise AssertionError('incomplete WAL batch unexpectedly committed')
        assert 'owned incomplete logical batch' in self.wait_marker('failed-batch')
        self.server.wait(timeout=30)
        assert self.server.returncode != 0 and not (self.directory / 'clean').exists()

    def exercise(self):
        if self.scenario == 'copydone':
            self.copydone()
        elif self.scenario == 'failure':
            self.failure()
        elif self.scenario in ('disconnect', 'stalled'):
            self.departure()
        elif self.mode == 'TERM':
            self.smart()
        else:
            self.fast()
        self.inspect_control()

    def inspect_control(self):
        inspection = subprocess.run([str(BASE / 'bin/pg_controldata'), '-D', str(self.data)],
                                    env=self.environment, text=True, capture_output=True, timeout=60)
        (self.directory / 'controldata.log').write_text(inspection.stdout + inspection.stderr)
        assert inspection.returncode == 0
        fields = dict(line.split(':', 1) for line in inspection.stdout.splitlines() if ':' in line)
        state = fields['Database cluster state'].strip()
        self.result['control_state'] = state
        if self.scenario in ('stalled', 'failure'):
            assert not state.startswith('shut down'), state
        else:
            assert state == 'shut down', state

    def cleanup(self):
        (self.directory / 'release-writer').touch()
        (self.directory / 'release-output').touch()
        for client in (self.sql, self.client):
            if client is not None:
                client.close()
        if self.server is not None:
            if self.server.poll() is None:
                subprocess.run([sys.executable, str(BASE / 't/interop/signal-owned-server.py'),
                                str(self.server.pid), str(self.data), 'QUIT', '5'], timeout=15, check=True)
            self.server.wait(timeout=10)
            self.result['process'] = dict(pid=self.server.pid, exit=self.server.returncode,
                                          absent=not Path('/proc', str(self.server.pid)).exists())
            assert self.result['process']['absent']
        if self.pidfd is not None:
            os.close(self.pidfd)

    def run(self):
        started = time.monotonic()
        try:
            for name, action in (('startup', self.start), ('setup', self.setup), ('exercise', self.exercise)):
                phase = time.monotonic()
                try:
                    action()
                finally:
                    self.result['timings'][name] = time.monotonic() - phase
            self.result['complete'] = True
        except BaseException as error:
            self.result['error'] = repr(error)
            raise
        finally:
            try:
                self.cleanup()
            except BaseException as error:
                self.result['complete'] = False
                self.result['cleanup_error'] = repr(error)
                raise
            finally:
                self.result['elapsed_seconds'] = time.monotonic() - started
                (self.directory / 'result.json').write_text(json.dumps(self.result, indent=2) + '\n')
        print(json.dumps({key: self.result[key] for key in ('name', 'complete', 'elapsed_seconds', 'process')}), flush=True)
        return self.result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--case', action='append', choices=CASES)
    arguments = parser.parse_args()
    root = arguments.output
    if root is None:
        root = Path(tempfile.mkdtemp(prefix='pg-logical-shutdown.'))
    else:
        root.mkdir(mode=0o700)
    root = root.resolve()
    cache = root / 'cache'
    cases = arguments.case or list(CASES)
    results = []
    started = time.monotonic()
    try:
        for name in cases:
            results.append(Case(root, name, cache).run())
    finally:
        registry = root / 'owned'
        if registry.exists():
            subprocess.run([sys.executable, str(BASE / 't/interop/owned-peer.py'), 'cleanup',
                            str(registry)], check=True, timeout=30)
        summary = dict(cases=cases, passed=[result['name'] for result in results],
                       elapsed_seconds=time.monotonic() - started, base=str(BASE))
        (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(f'logical-shutdown artifacts={root}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        traceback.print_exc()
        sys.exit(1)

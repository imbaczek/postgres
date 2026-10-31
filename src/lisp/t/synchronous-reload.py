"""Clear public standby names while commits, row locks and code reload are pending."""
from pathlib import Path
import runpy
import select
import signal
import struct
import time


api = runpy.run_path(str(Path(__file__).with_name('synchronous-ddl.py')))
OwnedLisp, Connection = api['OwnedLisp'], api['Connection']
receive_ready = api['receive_ready']
wire = api['api']['fixture']


def wait(server, predicate, label, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.process.poll() is not None:
            raise RuntimeError(f'owned server exited while waiting for {label}')
        if predicate():
            return
        time.sleep(0.02)
    raise TimeoutError(label)


def run():
    server = OwnedLisp()
    clients = []
    try:
        server.start()
        witness, writer, contender, portal = [Connection(server.port) for _ in range(4)]
        clients.extend((witness, writer, contender, portal))
        for connection in clients:
            connection.socket.settimeout(40)
        witness.query('CREATE TABLE reload_rows (id int, n int)')
        witness.query('INSERT INTO reload_rows VALUES (1, 0)')
        witness.query('CREATE TABLE portal_rows (id int)')
        witness.query('INSERT INTO portal_rows VALUES (1), (2), (3)')
        config = server.data / 'postgresql.conf'
        log = server.root / 'server.log'
        config.write_text("synchronous_standby_names='FIRST 1 (peer)'\n")
        assert witness.query('SELECT pg_reload_conf()') == [['t']]
        wait(server, lambda: witness.query('SHOW synchronous_standby_names')
             == [['FIRST 1 (peer)']], 'public standby names')
        wait(server, lambda: 'pg-lisp: reload complete' in log.read_text(errors='replace'),
             'initial configuration and code reload', timeout=60)
        assert witness.query("SELECT context,vartype,source,pending_restart FROM pg_settings "
                             "WHERE name='synchronous_standby_names'") == [
                                 ['sighup', 'string', 'configuration file', 'f']]
        witness.send(b'Q', b"SET synchronous_standby_names=''\0")
        rejected = receive_ready(witness)
        assert [kind for kind, _ in rejected] == [b'E', b'Z'], rejected
        assert witness.error_fields(rejected[0][1])['C'] == '55P02', rejected
        portal.socket.sendall(wire['parse']('SELECT id FROM portal_rows')
                              + wire['bind']() + wire['frame'](b'D', b'P\0')
                              + wire['frame'](b'E', b'\0' + struct.pack('!I', 1))
                              + wire['frame'](b'H', b''))
        frames = []
        while not frames or frames[-1][0] != b's':
            frames.append(portal.receive())
            assert frames[-1][0] != b'E', frames
        assert [kind for kind, _ in frames] == [b'1', b'2', b'T', b'D', b's'], frames
        server.control('(sb-thread:with-mutex (*query-execution-owner-lock*) '
                       '(assert (plusp (hash-table-count *query-execution-owners*))))')
        baseline = log.read_text(errors='replace').count('pg-lisp: reload complete')
        writer.send(b'Q', b'UPDATE reload_rows SET n=1 WHERE id=1\0')
        api['api']['wait_for_commit'](server)
        contender.send(b'Q', b'UPDATE reload_rows SET n=2 WHERE id=1\0')
        wait(server, lambda: ':BLOCKED T' in server.control(
             '(list :blocked (not (null (some #\'backend-waiting-xid (list-backends)))))'),
             'second admitted transaction waiting for the committing XID')
        failures = log.read_text(errors='replace').count('configuration reload failed')
        config.write_text("synchronous_standby_names='ANY 0 (peer)'\n")
        for number in range(1, 4):
            server.process.send_signal(signal.SIGHUP)
            wait(server, lambda: log.read_text(errors='replace').count(
                 'configuration reload failed') >= failures + number,
                 'invalid reload rejection')
            assert not select.select([writer.socket, contender.socket], [], [], 0)[0]
        config.write_text("synchronous_standby_names=''\n")
        server.process.send_signal(signal.SIGHUP)
        for connection in (writer, contender):
            frames = receive_ready(connection)
            assert [kind for kind, _ in frames] == [b'C', b'Z'], frames
        # The retained portal keeps code replacement pending, while admission
        # must reopen so ordinary SQL can use the newly installed configuration.
        assert witness.query('SELECT n FROM reload_rows WHERE id=1') == [['2']]
        assert witness.query('SHOW synchronous_standby_names') == [['']]
        text = log.read_text(errors='replace')
        assert text.count('pg-lisp: reload complete') == baseline, text
        assert text.count('pg-lisp: reloading code...') > baseline, text
        portal.socket.sendall(wire['frame'](b'C', b'P\0') + wire['frame'](b'S', b''))
        assert [kind for kind, _ in receive_ready(portal)] == [b'3', b'Z']
        wait(server, lambda: log.read_text(errors='replace').count('pg-lisp: reload complete') > baseline,
             'code reload after portal retirement', timeout=60)
        assert witness.query('SELECT n FROM reload_rows WHERE id=1') == [['2']]
        assert witness.query('SHOW synchronous_standby_names') == [['']]
        print('synchronous-reload: invalid reloads preserve waits; clearing names '
              'releases commits; retained portal preserves SQL service; code reload PASS', flush=True)
    finally:
        for connection in clients:
            connection.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run()

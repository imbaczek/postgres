"""Check public FIRST/ANY sender status through an owned server's SQL protocol."""
from pathlib import Path
import runpy


api = runpy.run_path(str(Path(__file__).with_name('synchronous-reload.py')))
OwnedLisp, Connection, wait = api['OwnedLisp'], api['Connection'], api['wait']


def configure(server, connection, names):
    log = server.root / 'server.log'
    baseline = log.read_text(errors='replace').count('pg-lisp: reload complete')
    (server.data / 'postgresql.conf').write_text(f"synchronous_standby_names='{names}'\n")
    assert connection.query('SELECT pg_reload_conf()') == [['t']]
    wait(server, lambda: connection.query('SHOW synchronous_standby_names') == [[names]],
         'public synchronous standby configuration')
    wait(server, lambda: log.read_text(errors='replace').count('pg-lisp: reload complete') > baseline,
         'code reload before sender observations', timeout=60)


def rows(connection):
    return connection.query('SELECT s.pid, s.sync_priority, s.sync_state '
                            'FROM pg_catalog.pg_stat_get_wal_senders() AS s ORDER BY s.pid')


def run():
    server = OwnedLisp()
    connection = None
    try:
        server.start()
        connection = Connection(server.port)
        connection.socket.settimeout(40)
        configure(server, connection, 'FIRST 1 (a,b,late,unknown)')
        server.control('(defvar *monitoring-senders* nil)')
        server.control(
            '(progn (setf *monitoring-senders* '
            '(loop for (name pid mode state flush) in '
            '\'(("a" 40001 :physical "streaming" 80) '
            '("b" 40002 :database "stopping" 70) '
            '("late" 40003 :physical "catchup" 60) '
            '("unknown" 40004 :physical "streaming" 0)) '
            'collect (let ((sender (wal-sender-register '
            '(make-backend :pid pid :replication-mode mode :application-name name)))) '
            '(wal-sender-set-state sender state) '
            '(wal-sender-note-reply sender 100 flush 20 1) sender))) t)')
        assert rows(connection) == [['40001', '1', 'sync'], ['40002', '2', 'potential'],
                                    ['40003', '3', 'potential'], ['40004', '0', 'async']]
        configure(server, connection, 'ANY 3 (a,b,late,unknown)')
        assert rows(connection) == [['40001', '1', 'quorum'], ['40002', '1', 'quorum'],
                                    ['40003', '1', 'potential'], ['40004', '0', 'async']]
        configure(server, connection, 'FIRST 2 (a,b,late,unknown)')
        server.control('(let ((sender (pop *monitoring-senders*))) '
                       '(wal-sender-unregister (wal-sender-status-backend sender) sender))')
        assert rows(connection) == [['40002', '2', 'sync'], ['40003', '3', 'potential'],
                                    ['40004', '0', 'async']]
        configure(server, connection, '')
        assert rows(connection) == [['40002', '0', 'async'], ['40003', '0', 'async'],
                                    ['40004', '0', 'async']]
        print('synchronous-status: FIRST/ANY selection, insufficient peers, catchup, '
              'unknown flush, disconnect and disable PASS', flush=True)
    finally:
        if connection is not None:
            connection.close()
        server.stop()
        print(f'artifacts: {server.root}', flush=True)


if __name__ == '__main__':
    run()

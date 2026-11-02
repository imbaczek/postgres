"""Exercise a finite proof while its real source backend owns a root lease."""
import json
import os
import socket
import struct
import subprocess
import threading
import time

import psycopg2


def connection(port, application):
    result = psycopg2.connect(host='127.0.0.1', port=port, user='postgres',
                              dbname='postgres', replication='true',
                              application_name=application, connect_timeout=3,
                              sslmode='disable')
    result.autocommit = True
    return result


def terminated_proof(client, command):
    """Preserve FATAL before PQexec can replace its result with the later EOF."""
    with socket.socket(fileno=os.dup(client.fileno())) as wire:
        wire.settimeout(90)

        def exact(length):
            result = bytearray()
            while len(result) < length:
                chunk = wire.recv(length - len(result))
                assert chunk, 'truncated termination response'
                result.extend(chunk)
            return bytes(result)

        payload = command.encode('utf-8') + b'\0'
        wire.sendall(b'Q' + struct.pack('!I', len(payload) + 4) + payload)
        assert exact(1) == b'E', 'termination must emit an ErrorResponse'
        length, = struct.unpack('!I', exact(4))
        assert 4 < length <= 65536, length
        payload = exact(length - 4)
        assert payload.endswith(b'\0\0'), payload
        fields = {field[:1]: field[1:] for field in payload[:-1].split(b'\0') if field}
        assert fields[b'S'] == fields[b'V'] == b'FATAL', fields
        assert fields[b'C'] == b'57P01', fields
        assert wire.recv(1) == b'', 'FATAL must close without a success response'
        return fields


def check(base, source, node, replica, port, identity, endpoint, digest, root_id, follow):
    for scenario in ('progress', 'cancel', 'terminate'):
        commands = source.root / f'proof-{scenario}.commands'
        name = f'origin_cut_{scenario}'
        prepared = subprocess.run(
            [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--script',
             str(base / 't/retained-proof-native.lisp'), str(source.data), str(port),
             *map(str, (*identity, endpoint, digest, root_id)), name, str(commands)],
            capture_output=True, text=True, timeout=240)
        (source.root / f'proof-{scenario}-prepare.log').write_text(prepared.stdout + prepared.stderr)
        assert prepared.returncode == 0, prepared.stdout + prepared.stderr
        prove, release = commands.read_text().splitlines()
        entered = source.root / f'proof-{scenario}.entered'
        resume = source.root / f'proof-{scenario}.resume'
        entered.unlink(missing_ok=True)
        resume.unlink(missing_ok=True)
        follow['control'](node.is_running(), replica / 'target.sock', f'''
          (defparameter *proof-cut-root-count*
            (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
              (hash-table-count *physical-retained-root-references*)))
          (let ((fired nil))
            (setf *physical-origin-proof-test-hook*
              (lambda (point value)
                (declare (ignore value))
                (when (and (not fired) (eq point :before-record))
                  (setf fired t)
                  (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                    (assert (> (hash-table-count *physical-retained-root-references*)
                               *proof-cut-root-count*)))
                  (with-open-file (output {json.dumps(str(entered))}
                                          :direction :output :if-exists :supersede)
                    (format output "~d ~d ~d ~d" (backend-pid *backend*)
                            *live-operation-depth*
                            (if (sb-thread:holding-mutex-p *wal-lock*) 1 0)
                            (hash-table-count *wal-sender-streams*)))
                  (loop until (probe-file {json.dumps(str(resume))})
                        do (check-for-interrupts) (sleep 0.01))))))''')
        client = connection(port, f'retained-proof-{scenario}')
        outcome = {}

        def query():
            try:
                if scenario == 'terminate':
                    outcome['fatal'] = terminated_proof(client, prove)
                else:
                    with client.cursor() as cursor:
                        cursor.execute(prove)
                        outcome['rows'] = cursor.fetchall()
            except Exception as error:
                outcome['error'] = error

        worker = threading.Thread(target=query, daemon=True)
        try:
            worker.start()
            deadline = time.monotonic() + 90
            while not entered.exists():
                assert worker.is_alive(), outcome
                assert node.is_running(), 'proof source exited before its barrier'
                assert time.monotonic() < deadline, 'proof did not enter the record callback'
                time.sleep(0.02)
            # The file can exist briefly before its writer closes it.
            while len(entered.read_text().split()) != 4:
                assert time.monotonic() < deadline
                time.sleep(0.01)
            pid, admitted, wal_locked, stream_count = map(int, entered.read_text().split())
            assert admitted > 0 and wal_locked == 0 and stream_count == 0
            if scenario == 'progress':
                before = follow['query'](port, 'SELECT pg_current_wal_flush_lsn()::text')[0][0]
                follow['query'](port, 'CREATE TABLE proof_progress (id integer PRIMARY KEY)')
                follow['query'](port, 'INSERT INTO proof_progress VALUES (1)')
                follow['query'](port, 'CHECKPOINT')
                assert follow['query'](port, 'SELECT id FROM proof_progress') == [(1,)]
                assert follow['query'](port, 'SELECT pg_current_wal_flush_lsn()::text')[0][0] != before
                resume.touch()
            else:
                function = 'pg_cancel_backend' if scenario == 'cancel' else 'pg_terminate_backend'
                assert follow['query'](port, f'SELECT {function}({pid})') == [(True,)]
            worker.join(timeout=90)
            assert not worker.is_alive(), 'proof query did not settle'
            if scenario == 'progress':
                assert 'error' not in outcome, outcome
                row, = outcome['rows']
                assert int(row[7]) == digest and int(row[8]) == root_id, row
            elif scenario == 'terminate':
                assert 'fatal' in outcome and 'error' not in outcome, outcome
            else:
                error = outcome.get('error')
                assert error is not None and error.pgcode == '57014', repr(error)
                if scenario == 'cancel':
                    with client.cursor() as cursor:
                        cursor.execute('IDENTIFY_SYSTEM')
                        assert int(cursor.fetchone()[0]) == identity[0]
            client.close()
            follow['wait_for'](lambda: ':PROOF-CLIENTS-RETIRED' in follow['control'](
                node.is_running(), replica / 'target.sock',
                '(when (sb-thread:with-mutex (*wal-sender-lock*) '
                '(zerop (hash-table-count *wal-sender-registry*))) :proof-clients-retired)').upper())
            follow['control'](node.is_running(), replica / 'target.sock', f'''
              (sb-thread:with-mutex (*wal-lock*)
                (assert (zerop (hash-table-count *physical-source-senders*)))
                (assert (physical-follow-pin-ready-p (gethash "{name}" *physical-source-pins*))))
              (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
                (assert (= *proof-cut-root-count* (hash-table-count *physical-retained-root-references*))))
              (wal-sender-check-shutdown-released)''')
            print(f'retained-proof: real {scenario}, lease cleanup and READY pin preservation PASS', flush=True)
        finally:
            resume.touch()
            worker.join(timeout=15)
            client.close()
            try:
                follow['control'](node.is_running(), replica / 'target.sock',
                                  '(setf *physical-origin-proof-test-hook* nil)')
            finally:
                cleanup = connection(port, 'retained-proof-cleanup')
                try:
                    with cleanup.cursor() as cursor:
                        cursor.execute(release)
                finally:
                    cleanup.close()

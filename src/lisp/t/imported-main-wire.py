"""Exercise native physical slot lifetimes beside imported logical protection.

The server creates a local checkpoint and private source proof through a real
base backup. A separate public temporary physical slot belongs to its creator;
neither it nor the private proof replaces the imported logical protections.
"""

import os
import re
import random
import struct
import time
import threading
from concurrent.futures import ThreadPoolExecutor

from interop.parking_wire import RawConnection, WireError


SIZE = 16 * 1024 * 1024


def lsn(value):
    return f'{value >> 32:X}/{value & 0xffffffff:08X}'


def integer(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) | int(low, 16)


def send(client, kind, body=b''):
    client.socket.sendall(kind + struct.pack('!I', len(body) + 4) + body)


def query(client, text):
    send(client, b'Q', text.encode() + b'\0')


def feedback(client, written, flushed, applied):
    """Report separate write, flush and apply positions to the sender."""
    send(client, b'd', b'r' + struct.pack('!QQQqB', written, flushed, applied, 0, 0))


def wait(predicate):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise TimeoutError('Native slot observation did not converge')


def rejected(client, sql, state):
    try:
        client.query(sql)
    except WireError as error:
        assert error.sqlstate == state, (state, error.sqlstate)
    else:
        raise AssertionError('Conflicting native slot request succeeded')


def pin_facts(control):
    response = control('(sb-thread:with-mutex (pg::*wal-lock*)\n      (let ((pin (gethash "wire_proof" pg::*physical-source-pins*)))\n        (assert pin)\n        (format nil "WIRE-PIN ~d ~d ~d ~d ~d ~d"\n          (pg::physical-source-pin-system-id pin)\n          (pg::physical-source-pin-timeline pin)\n          (pg::physical-source-pin-consistent-lsn pin)\n          (or (pg::physical-source-pin-confirmed-lsn pin)\n              (pg::physical-source-pin-consistent-lsn pin))\n          (pg::physical-source-pin-prefix-digest pin)\n          (pg::physical-source-pin-manifest-digest pin))))\n      (quit)\n', 'WIRE-PIN')
    matches = re.findall(r'WIRE-PIN (\d+) (\d+) (\d+) (\d+) (\d+) (\d+)', response)
    assert len(matches) == 1, response
    return tuple(map(int, matches[0]))


def start(client, facts, name):
    sid, timeline, base, cursor, digest, manifest = facts
    sql = (f"START_REPLICATION SLOT {name} PHYSICAL {lsn(cursor)} "
           f"(native_format 'pg_lisp_wal_8', proof_pin 'wire_proof', "
           f"system_id '{sid}', timeline '{timeline}', base_lsn '{lsn(base)}', "
           f"prefix_digest '{digest}', manifest_digest '{manifest}')")
    query(client, sql)
    kind, body = client.receive()
    if kind == b'E':
        raise WireError(client.error_fields(body))
    assert (kind, body) == (b'W', b'\0\0\0'), (kind, body)
    return sql


def frame(client):
    kind, body = client.receive()
    if kind == b'E':
        raise WireError(client.error_fields(body))
    assert kind == b'd' and body[:1] in (b'w', b'k'), (kind, body[:40])
    return body


def finish(client):
    """End COPY and consume the sender's completion through ReadyForQuery."""
    send(client, b'c')
    kinds = []
    while True:
        kind, body = client.receive()
        if kind == b'd':
            continue
        if kind == b'E':
            raise WireError(client.error_fields(body))
        kinds.append(kind)
        if kind == b'c':
            assert not body
        elif kind == b'C':
            assert body == b'START_STREAMING\0'
        elif kind == b'Z':
            assert body == b'I'
            break
        else:
            raise AssertionError((kind, body))
    assert kinds == [b'c', b'C', b'Z'], kinds


def check_protections(control):
    """Check that the saved imported protections remain installed and valid."""
    control('(sb-thread:with-mutex (pg::*wal-lock*) '
            '(assert (= (length pg::*imported-wire-protections*) '
            '(hash-table-count pg::*logical-slot-continuation-protections*))) '
            '(dolist (entry pg::*imported-wire-protections*) '
            '(assert (eq (cdr entry) (gethash (car entry) pg::*logical-slot-continuation-protections*))) '
            '(assert (pg::logical-slot-continuation-protection-call (cdr entry) :check))) '
            ':wire-protection-ok)\n(quit)\n', ':WIRE-PROTECTION-OK')


def exercise(port, data, job, imported, control, source=None):
    """Check streaming, feedback and temporary-slot cleanup on this server."""
    began = time.monotonic()
    control('(progn (defparameter pg::*imported-wire-protections* nil) '
            '(sb-thread:with-mutex (pg::*wal-lock*) '
            '(maphash (lambda (key group) (push (cons key group) pg::*imported-wire-protections*)) '
            'pg::*logical-slot-continuation-protections*)) :wire-protection-saved)\n(quit)\n',
            ':WIRE-PROTECTION-SAVED')
    check_protections(control)
    backup = job / 'tmp/wire-backup'
    backup.mkdir(mode=0o700)
    # Direct literal control dispatch owns quiescence; no fabricated source pin.
    control(f'(pg::physical-base-backup "{backup}/" "wire_proof")\n(quit)\n', ':SOURCE-PIN')
    facts = pin_facts(control)
    witness = RawConnection(port, 'imported-wire-observer', replication='database')
    owner = None
    events = []

    def slot(name):
        return witness.query("SELECT temporary, active, active_pid, restart_lsn FROM pg_replication_slots "
                             f"WHERE slot_name='{name}'")

    def create(name):
        nonlocal owner
        owner = RawConnection(port, 'imported-wire-owner', replication='database')
        owner.socket.settimeout(15)
        pid = owner.query('SELECT pg_backend_pid()')[0][0]
        assert owner.query(f"SELECT slot_name FROM pg_create_physical_replication_slot('{name}',true,true)") == [[name]]
        row = slot(name)
        assert len(row) == 1 and row[0][:3] == ['t', 't', pid] and row[0][3] is not None, row
        return pid

    try:
        witness.socket.settimeout(15)
        pid = create('wire_temp')
        sql = start(owner, facts, 'wire_temp')
        sid, timeline, base, cursor, _, _ = facts
        assert frame(owner)[:1] == b'k'
        feedback(owner, cursor, cursor, base)
        assert frame(owner)[:1] == b'k'
        assert slot('wire_temp') == [['t', 't', pid, lsn(cursor)]]
        rejected(witness, sql, '55006')
        rejected(witness, "SELECT pg_drop_replication_slot('wire_temp')", '55006')
        release = (f"RELEASE_BOOTSTRAP SLOT wire_proof (native_format 'pg_lisp_wal_8', "
                   f"system_id '{sid}', source_timeline '{timeline}', "
                   f"base_lsn '{lsn(base)}', manifest_digest '{facts[5]}')")
        rejected(witness, release, '55006')
        check_protections(control)
        events.append('second backend cannot claim or drop creator-owned slot or release active private proof')
        witness.query('CREATE TABLE imported_wire_rows (n integer, payload text)')
        payload = ''.join(random.Random(418).choices('abcdefghijklmnopqrstuvwxyz0123456789', k=200000))
        witness.query("INSERT INTO imported_wire_rows VALUES (1,'" + payload + "')")
        target = integer(witness.query('SELECT pg_current_wal_flush_lsn()')[0][0])
        feedback(owner, cursor, cursor, base)
        # Write feedback deliberately leads flush. Compare every fragment with
        # local WAL and report flush only at complete record boundaries after fsync.
        pending = bytearray()
        received = completed = cursor
        first_end = None
        phase = 0
        fragments = records = large_records = 0
        largest_record = max_frame_bytes = 0
        write_only_checked = flush_checked = False
        last_flush_sent = cursor
        with (job / 'tmp/received-wire-wal').open('wb') as sink:
            while True:
                body = frame(owner)
                if phase == 1:
                    row = slot('wire_temp')
                    assert row == [['t', 't', pid, lsn(cursor)]], row
                    write_only_checked = True
                elif phase == 2:
                    row = slot('wire_temp')
                    assert row == [['t', 't', pid, lsn(first_end)]], row
                    flush_checked = True
                    events.append('write-only report retains old floor; fsynced complete-record flush advances it')
                if body[:1] == b'w':
                    start_lsn, durable, _ = struct.unpack_from('!QQq', body, 1)
                    raw = body[25:]
                    max_frame_bytes = max(max_frame_bytes, len(raw))
                    assert start_lsn == received and received + len(raw) <= durable
                    segment, offset = divmod(received, SIZE)
                    path = data / 'pg_wal' / f'{timeline:08X}{segment // 256:08X}{segment % 256:08X}.wal'
                    with path.open('rb') as source_stream:
                        source_stream.seek(offset)
                        assert source_stream.read(len(raw)) == raw
                    assert raw and len(raw) <= 65536
                    sink.write(raw)
                    pending.extend(raw)
                    received += len(raw)
                    while len(pending) >= 32:
                        length = struct.unpack_from('<I', pending, 8)[0]
                        assert 32 <= length <= 128 * 1024 * 1024
                        if len(pending) < length:
                            break
                        largest_record = max(largest_record, length)
                        if length > 65536:
                            large_records += 1
                        del pending[:length]
                        completed += length
                        records += 1
                    if pending:
                        fragments += 1
                else:
                    assert len(body) == 18
                if phase in (1, 2):
                    check_protections(control)
                if phase == 0:
                    sink.flush()
                    feedback(owner, received, cursor, base)
                    if completed > cursor:
                        first_end = completed
                        phase = 1
                elif phase == 1:
                    sink.flush()
                    os.fsync(sink.fileno())
                    feedback(owner, received, first_end, base)
                    phase = 2
                else:
                    sink.flush()
                    os.fsync(sink.fileno())
                    if phase >= 3 and body[:1] == b'k' and completed >= target:
                        assert not pending and fragments > 0 and records > 1 and large_records > 0
                        assert last_flush_sent == completed
                        assert largest_record > max_frame_bytes
                        assert write_only_checked and flush_checked
                        break  # Wait for another frame, then verify the applied floor below.
                    feedback(owner, received, completed, base)
                    last_flush_sent = completed
                    phase = 3
            check_protections(control)
            final_row = slot('wire_temp')
            assert final_row == [['t', 't', pid, lsn(completed)]], final_row
            events.append('fragmented committed transaction synced and acknowledged at complete-record boundaries')
        child_ack = None
        if os.environ.get('PG_NATIVE_MAIN_CHILD_ACK') == '1':
            if imported != ['alpha', 'beta']:
                raise ValueError('Child ACK composition requires the actual two-slot fixture')
            import importlib.util
            path = source / 't/imported-main-ack.py'
            spec = importlib.util.spec_from_file_location('imported_main_ack', path)
            helper = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(helper)
            stopped = threading.Event()
            peer_started = threading.Event()
            peer_lock = threading.Lock()
            packet_count = 0
            def peer_progress():
                with peer_lock:
                    return packet_count
            def keep_peer_alive():
                nonlocal packet_count
                packets = 0
                while True:
                    # The preceding frame is waiting for this report. New WAL
                    # receives no additional durability credit.
                    feedback(owner, completed, completed, base)
                    frame(owner)
                    packets += 1
                    with peer_lock:
                        packet_count = packets
                    peer_started.set()
                    if stopped.is_set():
                        return packets
            with ThreadPoolExecutor(max_workers=1) as peers:
                peer = peers.submit(keep_peer_alive)
                try:
                    if not peer_started.wait(15):
                        raise TimeoutError('Physical peer did not produce its first frame')
                    child_ack = helper.exercise(witness, control, job, source, peer_progress)
                finally:
                    stopped.set()
                    child_packets = peer.result(timeout=15)
            assert child_packets > 1
            child_ack['physical_peer_packets'] = child_packets

        finish(owner)
        check_protections(control)
        assert owner.query('SELECT 1') == [['1']]
        assert slot('wire_temp') == [['t', 't', pid, lsn(completed)]]
        rejected(witness, "SELECT pg_drop_replication_slot('wire_temp')", '55006')
        owner.close()
        owner = None
        wait(lambda: not slot('wire_temp'))
        events.append('CopyDone releases sender but keeps creator claim; disconnect removes temporary slot')
        for fault in ('malformed-copydone', 'future-feedback', 'disconnect'):
            name = 'wire_' + fault.replace('-', '_')
            create(name)
            current = pin_facts(control)
            start(owner, current, name)
            body = frame(owner)
            if fault == 'disconnect':
                owner.close()
                owner = None
            else:
                if fault == 'malformed-copydone':
                    send(owner, b'c', b'x')
                else:
                    # Beyond even the advertised durable end, never valid sent data.
                    durable = struct.unpack_from('!Q', body, 9 if body[:1] == b'w' else 1)[0]
                    feedback(owner, durable + 1, durable + 1, current[2])
                kind, body = owner.receive()
                assert kind == b'E', (fault, kind, body[:40])
                assert owner.error_fields(body)['C'] == '08P01'
                owner.close()
                owner = None
            wait(lambda: not slot(name))
            check_protections(control)
            events.append(fault + ' removes temporary slot without leaving sender claims')
        assert witness.query(release) == []
        check_protections(control)
        events.append('checked bootstrap release succeeds after all sender claims end')
        assert witness.query("SELECT slot_name FROM pg_replication_slots WHERE slot_type='logical' ORDER BY slot_name") == [[name] for name in imported]
        control('(sb-thread:with-mutex (pg::*wal-lock*)\n          (assert (zerop (hash-table-count pg::*physical-source-senders*)))\n          (assert (not (gethash \"wire_proof\" pg::*physical-source-pins*)))\n          (assert (not (pg::slot-state-lstat (pg::physical-source-pin-path \"wire_proof\"))))\n          (maphash (lambda (key group) (declare (ignore key))\n                     (assert (pg::logical-slot-continuation-protection-call group :check)))\n                   pg::*logical-slot-continuation-protections*)\n          :wire-owners-clean)\n          (quit)\n', ':WIRE-OWNERS-CLEAN')
        return dict(child_ack=child_ack,events=events,wall_seconds=time.monotonic()-began,
                    received_bytes=received-cursor,flushed_lsn=lsn(completed),
                    target_lsn=lsn(target),records=records,fragmented_frames=fragments,large_records=large_records,
                    largest_record_bytes=largest_record,max_frame_bytes=max_frame_bytes,
                    write_only_checked=write_only_checked,flush_checked=flush_checked,
                    initial_floor=lsn(cursor),first_flush_floor=lsn(first_end))
    finally:
        if owner is not None:
            owner.close()
        witness.close()

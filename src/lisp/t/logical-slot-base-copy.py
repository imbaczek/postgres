"""Check explicit logical-slot copy through an owned source and private control REPL."""
from pathlib import Path
import os
import runpy
import shutil
import struct
import time

api = runpy.run_path(str(Path(__file__).with_name('command-complete.py')))
from parking_wire import RawConnection


def lisp_string(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def crc32c(data):
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return crc ^ 0xffffffff


def fnv64(data):
    digest = 0xcbf29ce484222325
    for byte in data:
        digest = ((digest ^ byte) * 0x100000001b3) & 0xffffffffffffffff
    return digest


def decode_descriptor(path):
    data = path.read_bytes()
    assert data[:16] == b'PGLOGICALCOPY1\0\0'
    assert struct.unpack_from('<I', data, 16)[0] == len(data) - 24
    assert crc32c(data[:-4] + b'\0\0\0\0') == struct.unpack_from('<I', data, len(data) - 4)[0]
    position = 20

    def take(count):
        nonlocal position
        assert 0 <= count <= len(data) - 4 - position
        old = position
        position += count
        return data[old:position]

    def u32():
        return struct.unpack('<I', take(4))[0]

    def blob():
        return take(u32())

    copy_id = take(32).decode('ascii')
    manifest_digest = struct.unpack('<Q', take(8))[0]
    controls = [blob() for _ in range(4)]
    slots = [(take(take(1)[0]).decode('ascii'), blob()) for _ in range(u32())]
    pieces = [struct.unpack('<IQIQQQ', take(40)) for _ in range(u32())]
    assert position == len(data) - 4
    assert slots == sorted(slots)
    assert [piece[:2] for piece in pieces] == sorted(set(piece[:2] for piece in pieces))
    return copy_id, manifest_digest, controls, slots, pieces


def wait_result(server, path):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        text = path.read_text() if path.exists() else ''
        if text:
            assert text.startswith('PASS '), (text, server.root)
            return text
        assert server.process.poll() is None, server.root / 'server.log'
        time.sleep(0.05)
    raise TimeoutError(f'copy did not finish; inspect {server.root}')


def lsn_number(value):
    high, low = value.split('/')
    return (int(high, 16) << 32) | int(low, 16)


def fill_past_rotation(connection):
    body = 'x' * 512
    for batch in range(160):
        if lsn_number(connection.query('SELECT pg_current_wal_lsn()')[0][0]) > 17 * 1024 * 1024:
            return
        rows = ','.join(f"({100 + batch * 250 + row}, '{body}')" for row in range(250))
        connection.query('INSERT INTO copy_rows VALUES ' + rows)
    raise AssertionError('real SQL workload did not rotate WAL within its bound')


def check_preflight_refusal(server, target, pin, expected=None):
    before = list(target.iterdir()) if target.exists() else None
    try:
        server.control(f'(logical-slot-base-copy {lisp_string(target)} {lisp_string(pin)})')
    except AssertionError as error:
        assert ';; error:' in str(error)
        if expected:
            assert expected in str(error), error
    else:
        raise AssertionError('invalid source/target entered logical copy')
    assert not (server.data / f'pg_physical_pins/{pin}.pin').exists()
    assert (list(target.iterdir()) if target.exists() else None) == before


def check_active_slot_refusal(server):
    target = server.root / 'active_slot'
    target.mkdir(mode=0o700)
    result = server.root / 'active-slot-result'
    server.control(f'''(progn
      (sb-thread:make-thread
       (lambda ()
         (with-open-file (out {lisp_string(result)} :direction :output :if-exists :error)
           (handler-case
               (progn
                 (call-with-logical-replication-slot
                  "copy_slot"
                  (lambda (slot)
                    (declare (ignore slot))
                    (let ((caught nil))
                      (handler-case (logical-slot-base-copy {lisp_string(target)} "active_pin")
                        (error (condition) (setf caught condition)))
                      (assert (and caught (search "exact inactive" (princ-to-string caught)))))))
                 (sb-thread:with-mutex (*wal-lock*)
                   (assert (null (wal-replication-slot-active-owner
                                  (gethash "copy_slot" *wal-replication-slots*))))
                   (assert (null (gethash "active_pin" *physical-source-pins*))))
                 (format out "PASS active slot refused before target or pin~%"))
             (error (condition) (format out "FAIL ~a~%" condition))))))
      (values))''')
    wait_result(server, result)
    assert not list(target.iterdir())
    assert not (server.data / 'pg_physical_pins/active_pin.pin').exists()


def check_stale_pin_refusal(server):
    target = server.root / 'stale_pin_target'
    target.mkdir(mode=0o700)
    directory = server.data / 'pg_physical_pins'
    directory.mkdir(mode=0o700, exist_ok=True)
    temporary = directory / 'copy_pin.pin.tmp'
    temporary.write_bytes(b'owned by an earlier interrupted attempt')
    try:
        server.control(f'(logical-slot-base-copy {lisp_string(target)} "copy_pin")')
    except AssertionError as error:
        assert ';; error:' in str(error) and 'stale temporary file' in str(error), error
    else:
        raise AssertionError('stale pin temporary was accepted')
    assert temporary.read_bytes() == b'owned by an earlier interrupted attempt'
    assert not (directory / 'copy_pin.pin').exists()
    assert (target / 'logical-slot-copy.in-progress').exists()
    server.control('(progn (assert (not *wal-poisoned*)) '
                   '(assert *live-operation-admission-open*) '
                   '(sb-thread:with-mutex (*wal-lock*) '
                   '(assert (primary-wal-writer-available-under-lock-p))) (values))')
    temporary.unlink()
    shutil.rmtree(target)


def check_startup_refusal(data, expected):
    peer = api['OwnedLisp'](parameters=(('wal_level', 'logical'),))
    peer.data = data
    before = {str(p.relative_to(data)): p.read_bytes()
              for p in data.rglob('*') if p.is_file()}
    try:
        try:
            peer.start()
        except RuntimeError:
            assert expected in (peer.root / 'server.log').read_text(), peer.root
            assert peer.process.poll() is not None
        else:
            raise AssertionError('invalid copied logical image started')
        after = {str(p.relative_to(data)): p.read_bytes()
                 for p in data.rglob('*') if p.is_file()}
        assert before == after, peer.root
    finally:
        peer.stop(immediate=True)



def check_startup_standby(data):
    peer = api['OwnedLisp'](parameters=(('wal_level', 'logical'),))
    peer.data = data
    descriptor = (data / 'logical-slot-copy.descriptor').read_bytes()
    carriers = {p.name: p.read_bytes() for p in (data / 'logical_slot_wal').iterdir()}
    try:
        peer.start()
        connection = RawConnection(peer.port)
        try:
            assert connection.query('SELECT pg_is_in_recovery()') == [['t']]
            assert connection.query('SELECT id FROM copy_rows WHERE id < 3 ORDER BY id') == [['1'], ['2']]
        finally:
            connection.close()
        peer.control('(assert (zerop (loop for slot being the hash-values of *wal-replication-slots* '
                     'count (wal-logical-slot-p slot))))')
        assert (data / 'logical-slot-copy.base-verified').is_file()
        assert descriptor == (data / 'logical-slot-copy.descriptor').read_bytes()
        assert carriers == {p.name: p.read_bytes() for p in (data / 'logical_slot_wal').iterdir()}
    finally:
        peer.stop()


def check_publication_fault(server, point, label, mutate=False):
    target = server.root / label
    target.mkdir(mode=0o700)
    result = server.root / (label + '-result')
    server.control(f'''(progn
      (sb-thread:make-thread
       (lambda ()
         (with-open-file (out {lisp_string(result)} :direction :output :if-exists :error)
           (handler-case
               (let ((fired nil) (caught nil))
                 (let ((*logical-slot-copy-test-hook*
                         (lambda (actual context)
                           (when (eq actual {point})
                             (setf fired t)
                             (if {'t' if mutate else 'nil'}
                                 (with-open-file
                                     (stream (merge-pathnames "PG_VERSION"
                                                             (physical-image-context-root-path context))
                                             :direction :io :element-type '(unsigned-byte 8)
                                             :if-exists :overwrite)
                                   (write-byte (char-code #\\X) stream)
                                   (fsync-stream stream))
                                 (error "copy publication test fault"))))))
                   (handler-case (logical-slot-base-copy {lisp_string(target)} {lisp_string(label)})
                     (error (condition) (setf caught condition))))
                 (assert (and fired caught
                              (search {lisp_string('verified target changed' if mutate else 'copy publication test fault')}
                                      (princ-to-string caught))))
                 (assert *live-operation-admission-open*)
                 (assert (not *wal-poisoned*))
                 (sb-thread:with-mutex (*wal-lock*)
                   (assert (primary-wal-writer-available-under-lock-p)))
                 (format out "PASS publication refusal retains its marker~%"))
             (error (condition) (format out "FAIL ~a~%" condition))))))
      (values))''')
    wait_result(server, result)
    assert (target / 'logical-slot-copy.in-progress').is_file()
    assert (server.data / f'pg_physical_pins/{label}.pin').is_file()
    check_startup_refusal(target, 'unfinished logical slot copy cannot start')
    shutil.rmtree(target)


def descriptor_piece_offset(data):
    position = 60
    for _ in range(4):
        size = struct.unpack_from('<I', data, position)[0]
        position += 4 + size
    count = struct.unpack_from('<I', data, position)[0]
    position += 4
    for _ in range(count):
        position += 1 + data[position]
        size = struct.unpack_from('<I', data, position)[0]
        position += 4 + size
    assert struct.unpack_from('<I', data, position)[0] >= 1
    return position + 4


def rewrite_descriptor_crc(data):
    struct.pack_into('<I', data, len(data) - 4, 0)
    struct.pack_into('<I', data, len(data) - 4, crc32c(data))
    return data


def verification_refused(server, target, scratch, expected):
    before = {str(p.relative_to(target)): p.read_bytes()
              for p in target.rglob('*') if p.is_file()}
    try:
        server.control(f'(logical-slot-copy-verify {lisp_string(target)} {lisp_string(scratch)})', timeout=90)
    except AssertionError as error:
        assert ';; error:' in str(error) and expected in str(error), error
    else:
        raise AssertionError('changed logical copy was accepted')
    assert before == {str(p.relative_to(target)): p.read_bytes()
                      for p in target.rglob('*') if p.is_file()}
    server.control('(progn (assert (not *wal-poisoned*)) (assert *live-operation-admission-open*) (values))')


def check_verification_mutations(server, target, scratch, pieces):
    for path in [target / 'unexpected', target / 'logical_slot_wal/unexpected.wal',
                 target / 'logical-slot-copy.in-progress']:
        path.write_bytes(b'injected verification fixture')
        try:
            verification_refused(server, target, scratch,
                                 'unfinished logical slot copy' if path.name.endswith('in-progress')
                                 else 'inventory differs')
        finally:
            path.unlink()
    leftover = scratch / 'unclaimed.tmp'
    before = {str(p.relative_to(target)): p.read_bytes()
              for p in target.rglob('*') if p.is_file()}
    try:
        server.control(f'''(let ((fired nil) (caught nil))
          (let ((*logical-slot-copy-verification-test-hook*
                  (lambda (point context)
                    (declare (ignore context))
                    (when (eq point :after-scratch-lock)
                      (setf fired t)
                      (with-open-file (out {lisp_string(leftover)} :direction :output :if-exists :error)
                        (write-string "unclaimed scratch evidence" out))))))
            (handler-case (logical-slot-copy-verify {lisp_string(target)} {lisp_string(scratch)})
              (error (condition) (setf caught condition))))
          (assert (and fired caught (search "scratch changed before initialization"
                                           (princ-to-string caught)))) (values))''', timeout=90)
        assert leftover.read_text() == 'unclaimed scratch evidence'
        assert before == {str(p.relative_to(target)): p.read_bytes()
                          for p in target.rglob('*') if p.is_file()}
    finally:
        leftover.unlink(missing_ok=True)
    displaced = scratch.with_name(scratch.name + '_held')
    try:
        server.control(f'''(let ((fired nil) (caught nil) (attempts 0)
                                 (thread sb-thread:*current-thread*)
                                 (retry (symbol-function 'retry-pending-query-temp-cleanup)))
          (unwind-protect
              (progn
                (setf (symbol-function 'retry-pending-query-temp-cleanup)
                      (lambda (&optional domain)
                        (when (and fired (eq thread sb-thread:*current-thread*)) (incf attempts))
                        (funcall retry domain)))
                (let ((*logical-slot-copy-verification-test-hook*
                        (lambda (point context)
                          (declare (ignore context))
                          (when (and (eq point :before-scratch-cleanup) (not fired))
                            (setf fired t)
                            (sb-posix:rename {lisp_string(scratch)} {lisp_string(displaced)})
                            (sb-posix:mkdir {lisp_string(scratch)} #o700)
                            (with-open-file (out {lisp_string(scratch / 'sentinel')}
                                                 :direction :output :if-exists :error)
                              (write-string "replacement scratch is untouched" out))))))
                  (handler-case (logical-slot-copy-verify {lisp_string(target)} {lisp_string(scratch)})
                    (error (condition) (setf caught condition)))))
            (setf (symbol-function 'retry-pending-query-temp-cleanup) retry))
          (assert (and fired caught)) (assert (zerop attempts))
          (assert (not *wal-poisoned*)) (values))''', timeout=90)
        assert (scratch / 'sentinel').read_text() == 'replacement scratch is untouched'
        assert before == {str(p.relative_to(target)): p.read_bytes()
                          for p in target.rglob('*') if p.is_file()}
    finally:
        if displaced.exists():
            (scratch / 'sentinel').unlink(missing_ok=True)
            scratch.rmdir()
            displaced.rename(scratch)
        # The rejected cleanup left only the operation's known empty namespace
        # directories. Remove empty directories, never recursively delete files.
        for directory in sorted((p for p in scratch.rglob('*') if p.is_dir()),
                                key=lambda p: len(p.parts), reverse=True):
            directory.rmdir()
    descriptor = target / 'logical-slot-copy.descriptor'
    original = descriptor.read_bytes()
    position = descriptor_piece_offset(original)
    changed = bytearray(original)
    struct.pack_into('<Q', changed, position + 24, pieces[0][4] + 1)
    descriptor.write_bytes(rewrite_descriptor_crc(changed))
    try:
        verification_refused(server, target, scratch, 'declared piece coverage')
    finally:
        descriptor.write_bytes(original)
    timeline, segment, size, checksum, first, end = pieces[0]
    leaf = f'{timeline:08X}{segment // 256:08X}{segment % 256:08X}.wal'
    path = target / 'logical_slot_wal' / leaf
    raw = path.read_bytes()
    assert first < end and first % (16 * 1024 * 1024) + 32 < len(raw)
    corrupt = bytearray(raw)
    corrupt[first % (16 * 1024 * 1024)] ^= 1
    changed = bytearray(original)
    struct.pack_into('<Q', changed, position + 16, fnv64(corrupt))
    path.write_bytes(corrupt)
    descriptor.write_bytes(rewrite_descriptor_crc(changed))
    try:
        verification_refused(server, target, scratch, 'WAL')
    finally:
        path.write_bytes(raw)
        descriptor.write_bytes(original)
    # Touch an already validated piece without changing its bytes. The next
    # held-FD observation must reject the changed file facts.
    before = {str(p.relative_to(target)): p.read_bytes()
              for p in target.rglob('*') if p.is_file()}
    server.control(f'''(let ((fired nil) (caught nil))
      (let ((*logical-slot-copy-verification-test-hook*
              (lambda (point context)
                (declare (ignore context))
                (when (and (eq point :after-record) (not fired))
                  (setf fired t)
                  (with-open-file (stream {lisp_string(path)} :direction :io
                                          :element-type '(unsigned-byte 8) :if-exists :overwrite)
                    (let ((byte (read-byte stream)))
                      (file-position stream 0) (write-byte byte stream) (fsync-stream stream)))))))
        (handler-case (logical-slot-copy-verify {lisp_string(target)} {lisp_string(scratch)})
          (error (condition) (setf caught condition))))
      (assert (and fired caught (search "selected WAL piece changed" (princ-to-string caught))))
      (assert (not *wal-poisoned*)) (values))''', timeout=90)
    assert before == {str(p.relative_to(target)): p.read_bytes()
                      for p in target.rglob('*') if p.is_file()}
    assert sorted(p.name for p in scratch.iterdir()) == ['.pg_lisp.lock']


def run():
    start = time.monotonic()
    server = api['OwnedLisp'](parameters=(('wal_level', 'logical'),))
    try:
        server.start()
        connection = RawConnection(server.port)
        try:
            connection.query('CREATE TABLE copy_rows (id integer, body text)')
            connection.query("INSERT INTO copy_rows VALUES (1, 'before')")
            empty = server.root / 'empty_inventory'
            empty.mkdir(mode=0o700)
            check_preflight_refusal(server, empty, 'empty_pin', 'nonempty bounded logical slot inventory')
            connection.query("SELECT * FROM pg_create_logical_replication_slot('copy_slot', 'pgoutput')")
            connection.query("INSERT INTO copy_rows VALUES (2, 'after')")
            connection.query('CREATE TABLE copy_ddl (id integer)')
            check_preflight_refusal(server, server.root / 'missing', 'missing_pin')
            nonempty = server.root / 'nonempty'
            nonempty.mkdir(mode=0o700)
            (nonempty / 'sentinel').write_bytes(b'unchanged')
            check_preflight_refusal(server, nonempty, 'nonempty_pin', 'empty and separate')
            assert (nonempty / 'sentinel').read_bytes() == b'unchanged'
            nested = server.data / 'nested_target'
            nested.mkdir(mode=0o700)
            check_preflight_refusal(server, nested, 'nested_pin', 'empty and separate')
            not_private = server.root / 'not_private'
            not_private.mkdir(mode=0o755)
            not_private.chmod(0o755)
            check_preflight_refusal(server, not_private, 'private_pin', 'private and owned')
            check_active_slot_refusal(server)
            check_stale_pin_refusal(server)
            connection.query("INSERT INTO copy_rows VALUES (900000, '" + ('v' * (5 * 1024 * 1024)) + "')")
            fill_past_rotation(connection)
        finally:
            connection.close()
        target = server.root / 'copy'
        target.mkdir(mode=0o700)
        slot_before = (server.data / 'pg_replslot/copy_slot/state').read_bytes()
        result = server.root / 'base-copy-result'
        server.control(f'''(progn
          (sb-thread:make-thread
           (lambda ()
             (with-open-file (out {lisp_string(result)} :direction :output :if-exists :error)
               (handler-case
                   (handler-bind ((error (lambda (condition)
                                           (format out "CONDITION ~a~%" condition)
                                           (sb-debug:print-backtrace :stream out :count 24))))
                     (multiple-value-bind (manifest descriptor)
                         (logical-slot-base-copy {lisp_string(target)} "copy_pin")
                       (assert (= 1 (length (logical-slot-copy-descriptor-slots descriptor))))
                       (assert (logical-slot-copy-descriptor-wal-pieces descriptor))
                       (assert *live-operation-admission-open*)
                       (assert (not *wal-poisoned*))
                       (sb-thread:with-mutex (*wal-lock*)
                         (assert (primary-wal-writer-available-under-lock-p)))
                       (format out "PASS checkpoint ~d slots ~d pieces ~d~%"
                               (getf manifest :end)
                               (length (logical-slot-copy-descriptor-slots descriptor))
                               (length (logical-slot-copy-descriptor-wal-pieces descriptor)))))
                 (error (condition) (format out "FAIL ~a~%" condition))))))
          (values))''')
        outcome = wait_result(server, result)
        assert not (target / 'logical-slot-copy.in-progress').exists()
        assert not (target / 'pg_replslot').exists()
        copy_id, digest, controls, slots, pieces = decode_descriptor(target / 'logical-slot-copy.descriptor')
        manifest = (target / 'physical-backup.manifest').read_bytes()
        assert digest == fnv64(manifest)
        assert f':LOGICAL-SLOT-COPY-ID "{copy_id}"'.encode() in manifest
        assert slots == [('copy_slot', slot_before)]
        assert slot_before == (server.data / 'pg_replslot/copy_slot/state').read_bytes()
        assert controls[1] == (target / 'global/pg_wal_control').read_bytes()
        assert controls[2] == (target / 'physical-recovery.control').read_bytes()
        assert controls[3] == b''
        assert (server.data / 'pg_physical_pins/copy_pin.pin').is_file()
        assert len(pieces) >= 2
        assert len(list((target / 'logical_slot_wal').iterdir())) == len(pieces)
        assert len(list((target / 'pg_wal').glob('*.wal'))) < len(pieces)
        for timeline, segment, size, checksum, first, end in pieces:
            per_id = 256
            name = f'{timeline:08X}{segment // per_id:08X}{segment % per_id:08X}.wal'
            raw = (target / 'logical_slot_wal' / name).read_bytes()
            assert len(raw) == size and fnv64(raw) == checksum
            assert segment * 16 * 1024 * 1024 <= first <= end <= segment * 16 * 1024 * 1024 + size
        scratch = server.root / 'verification_scratch'
        scratch.mkdir(mode=0o700)
        before_verify = {str(p.relative_to(target)): p.read_bytes()
                         for p in target.rglob('*') if p.is_file()}
        server.control(f'''(let ((opened nil) (thread sb-thread:*current-thread*)
                                    (old (symbol-function 'open-query-temp-io))
                                    (catalog *catalog*) (slots *wal-replication-slots*)
                                    (incarnation *wal-storage-incarnation*)
                                    (temporary *query-temp-instance-directory*)
                                    (expected (make-hash-table :test #'eql)) (verified 0))
          (sb-thread:with-mutex (*wal-lock*)
            (maphash (lambda (xid value) (setf (gethash xid expected) (copy-tree value)))
                     (wal-replication-slot-pending-xids (gethash "copy_slot" *wal-replication-slots*))))
          (let ((*logical-slot-copy-verification-test-hook*
                  (lambda (point context)
                    (when (eq point :slot-verified)
                      (incf verified)
                      (assert (equalp expected (logical-slot-startup-progress-pending (second context))))))))
          (unwind-protect
              (progn
                (setf (symbol-function 'open-query-temp-io)
                      (lambda ()
                        (multiple-value-bind (path stream) (funcall old)
                          (when (eq thread sb-thread:*current-thread*)
                            (assert (uiop:subpathp path (pathname {lisp_string(str(scratch) + '/')})))
                            (push path opened))
                          (values path stream))))
                (let ((summary (logical-slot-copy-verify {lisp_string(target)} {lisp_string(scratch)})))
                  (assert (= 1 (getf summary :slots)))
                  (assert (plusp (getf summary :records)))
                  (assert (= (hash-table-count expected) (getf summary :pending-xids))))
                (assert (= 1 verified)) (assert opened)
                (assert (every #'query-temp-file-absent-p opened))
                (assert (eq catalog *catalog*)) (assert (eq slots *wal-replication-slots*))
                (assert (eq incarnation *wal-storage-incarnation*))
                (assert (equal temporary *query-temp-instance-directory*)))
            (setf (symbol-function 'open-query-temp-io) old)))
          (values))''', timeout=90)
        assert before_verify == {str(p.relative_to(target)): p.read_bytes()
                                 for p in target.rglob('*') if p.is_file()}
        assert sorted(p.name for p in scratch.iterdir()) == ['.pg_lisp.lock']
        check_verification_mutations(server, target, scratch, pieces)
        admitted = server.root / 'admitted_standby'
        shutil.copytree(target, admitted)
        check_startup_standby(admitted)
        shutil.rmtree(admitted)
        missing = server.root / 'missing_descriptor'
        shutil.copytree(target, missing)
        (missing / 'logical-slot-copy.descriptor').unlink()
        check_startup_refusal(missing, 'manifest and descriptor must both be present')
        shutil.rmtree(missing)
        check_publication_fault(server, ':after-base-copy', 'partial_base')
        check_publication_fault(server, ':after-descriptor', 'partial_descriptor')
        check_publication_fault(server, ':before-marker-retirement', 'changed_target', mutate=True)
        literal = server.root / 'literal_control'
        literal.mkdir(mode=0o700)
        server.control(f'(logical-slot-base-copy {lisp_string(literal)} "literal_pin")', timeout=90)
        assert not (literal / 'logical-slot-copy.in-progress').exists()
        assert decode_descriptor(literal / 'logical-slot-copy.descriptor')[3] == slots
        shutil.rmtree(literal)
        connection = RawConnection(server.port)
        try:
            assert connection.query('SELECT id FROM copy_rows WHERE id < 3 ORDER BY id') == [['1'], ['2']]
            connection.query("INSERT INTO copy_rows VALUES (3, 'still live')")
        finally:
            connection.close()
        print(f'logical-slot-base-copy: PASS {time.monotonic() - start:.6f}s; '
              f'{outcome.strip()}; artifacts: {server.root}', flush=True)
    except BaseException:
        if os.environ.get('PG_LISP_COPY_DEBUG') and server.process and server.process.poll() is None:
            print(f'COPY-DEBUG PID={server.process.pid} ROOT={server.root} '
                  f'CONTROL={server.control_socket}', flush=True)
            input()
        raise
    finally:
        server.stop(immediate=True)


if __name__ == '__main__':
    run()

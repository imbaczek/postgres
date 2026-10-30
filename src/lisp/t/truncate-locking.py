"""TRUNCATE gate waiters must not become users awaited by the gate owner."""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "transaction_fixture", Path(__file__).with_name("command-complete.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def complete(connection, tag):
    frames = []
    while True:
        kind, body = connection.receive()
        frames.append((kind, body))
        if kind == b"Z":
            break
    assert frames == [(b"C", tag.encode() + b"\0"), (b"Z", b"I")], frames


def run():
    server = fixture.OwnedLisp()
    connections = []
    try:
        server.start()
        a, b = [fixture.Connection(server.port) for _ in range(2)]
        connections.extend([a, b])
        for connection in connections:
            connection.query("SET statement_timeout='2500ms'")
        a.query("CREATE TABLE truncate_locking(id int)")
        a.query("INSERT INTO truncate_locking VALUES(1)")
        server.control("(defvar pg::*truncate-lock-original* "
                       "(symbol-function 'pg::acquire-relation-build-locks))")
        server.control("(defvar pg::*truncate-lock-resume* (sb-thread:make-semaphore))")
        server.control("(defvar pg::*truncate-lock-mutex* (sb-thread:make-mutex))")
        server.control("(defvar pg::*truncate-lock-arrivals* 0)")
        marker = server.root / "both-at-gate"
        server.control(
            "(setf (symbol-function 'pg::acquire-relation-build-locks) "
            "(lambda (&rest args) "
            "(sb-thread:with-mutex (pg::*truncate-lock-mutex*) "
            "(when (= 2 (incf pg::*truncate-lock-arrivals*)) "
            f'(with-open-file (out "{marker}" :direction :output) '
            '(write-line "both at gate" out)))) '
            "(sb-thread:wait-on-semaphore pg::*truncate-lock-resume*) "
            "(apply pg::*truncate-lock-original* args)))")
        try:
            a.send(b"Q", b"TRUNCATE truncate_locking\0")
            b.send(b"Q", b"TRUNCATE truncate_locking\0")
            fixture.wait_for_file(marker, server.process)
            server.control("(sb-thread:signal-semaphore pg::*truncate-lock-resume* 2)")
            complete(a, "TRUNCATE TABLE")
            complete(b, "TRUNCATE TABLE")
        finally:
            server.control("(setf (symbol-function 'pg::acquire-relation-build-locks) "
                           "pg::*truncate-lock-original*)")
            server.control("(sb-thread:signal-semaphore pg::*truncate-lock-resume* 2)")
        assert a.query("SELECT id FROM truncate_locking") == []

        # Hold TRUNCATE at the same pre-gate boundary while the index builder
        # publishes its gate first. The builder must not wait for that waiter.
        server.control("(setf pg::*truncate-lock-resume* (sb-thread:make-semaphore))")
        marker = server.root / "truncate-at-gate"
        gate = server.root / "index-has-gate"
        server.control(
            "(setf (symbol-function 'pg::acquire-relation-build-locks) "
            "(let ((paused nil)) (lambda (nodes &optional xid readers acquired) "
            "(when (and readers (not paused)) (setf paused t) "
            f'(progn (with-open-file (out "{marker}" :direction :output) '
            '(write-line "truncate waiting" out)) '
            "(sb-thread:wait-on-semaphore pg::*truncate-lock-resume*))) "
            "(funcall pg::*truncate-lock-original* nodes xid readers "
            "(if readers acquired (lambda (relations token) "
            "(when acquired (funcall acquired relations token)) "
            f'(with-open-file (out "{gate}" :direction :output) '
            '(write-line "index owns gate" out))))))))')
        try:
            a.send(b"Q", b"TRUNCATE truncate_locking\0")
            fixture.wait_for_file(marker, server.process)
            b.send(b"Q", b"CREATE INDEX truncate_locking_idx ON truncate_locking(id)\0")
            fixture.wait_for_file(gate, server.process)
            server.control("(sb-thread:signal-semaphore pg::*truncate-lock-resume*)")
            complete(a, "TRUNCATE TABLE")
            complete(b, "CREATE INDEX")
        finally:
            server.control("(setf (symbol-function 'pg::acquire-relation-build-locks) "
                           "pg::*truncate-lock-original*)")
            server.control("(sb-thread:signal-semaphore pg::*truncate-lock-resume*)")
        a.query("INSERT INTO truncate_locking VALUES(2)")
        assert a.query("SELECT id FROM truncate_locking WHERE id=2") == [["2"]]
        print("truncate-locking: PASS (parallel TRUNCATE and index build)")
    finally:
        for connection in connections:
            connection.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()

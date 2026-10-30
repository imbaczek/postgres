"""TX-01 regression: simple-query success follows durable autocommit.

Run from any directory with python3 src/lisp/t/command-complete.py.
Uses a disposable server, the existing WAL barrier, and the wire test client.
"""
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import sys
import tempfile
import time

LISP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LISP / "t/interop"))
from subqueries import QueryConnection
from binary_values import parse, bind, frame

class Connection(QueryConnection):
    def __init__(self, port):
        super().__init__(port, "command-complete-regression")
        self.socket.settimeout(10)

    def send(self, kind, body):
        self.socket.sendall(kind + struct.pack("!I", len(body) + 4) + body)


class OwnedLisp:
    """Own a disposable server; retain its data and log for inspection after stop."""

    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="pg-transaction-response-"))
        self.data = self.root / "data"
        control_dir = self.root / "control"
        control_dir.mkdir(mode=0o700)
        self.control_socket = control_dir / "pg.sock"
        self.process = None
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]

    def start(self):
        assert self.process is None, "test server is already owned"
        # An immediate stop leaves this fixture's Unix socket pathname behind.
        self.control_socket.unlink(missing_ok=True)
        with (self.root / "server.log").open("ab") as log:
            self.process = subprocess.Popen(
                [str(LISP / "pg-lisp.sh"), "-p", str(self.port), "-D", str(self.data),
                 "--control-socket", str(self.control_socket)],
                cwd=LISP, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"test server exited; see {self.root / 'server.log'}")
            listener = subprocess.check_output(
                ["ss", "-H", "-ltnp", f"sport = :{self.port}"], text=True)
            if f"pid={self.process.pid}," in listener:
                try:
                    connection = Connection(self.port)
                    try:
                        assert connection.query("SELECT 1") == [["1"]]
                    finally:
                        connection.close()
                    return
                except (OSError, EOFError):
                    pass
            time.sleep(0.05)
        raise TimeoutError(f"test server not ready; see {self.root / 'server.log'}")

    def control(self, expression):
        response = subprocess.run(
            [os.environ.get("SBCL", "sbcl"), "--script", str(LISP / "t/control-client.lisp"),
             "unix", str(self.control_socket)], input=expression + "\n(quit)\n",
            text=True, capture_output=True, check=True, timeout=15)
        output = response.stdout + response.stderr
        assert ";; error:" not in output, output
        return output

    def stop(self, immediate=False):
        if self.process is None:
            return
        if self.process.poll() is None:
            if immediate:
                self.process.kill()
            else:
                self.process.send_signal(2)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process = None


def wait_for_file(path, process):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            raise RuntimeError("test server exited before reaching the test barrier")
        time.sleep(0.02)
    raise TimeoutError(f"test barrier was not reached: {path}")


def run():
    server = OwnedLisp()
    connection = None
    resume = server.data / "pg_wal/test-resume-after-page-images"
    marker = server.data / "pg_wal/test-paused-after-page-images"
    try:
        server.start()
        connection = Connection(server.port)
        connection.query("CREATE TABLE completion_rows(id int)")
        connection.query("INSERT INTO completion_rows VALUES(0)")
        connection.query("DELETE FROM completion_rows")
        connection.query("CHECKPOINT")
        server.control('(setf pg::*wal-test-pause-at* "after-page-images")')
        connection.send(b"Q", b"INSERT INTO completion_rows VALUES(1)\0")
        wait_for_file(marker, server.process)
        # INSERT has no preceding row frames: any response here is premature.
        assert not select.select([connection.socket], [], [], 0.5)[0], (
            "autocommit sent a response before its commit record")
        resume.touch()
        frames = []
        while True:
            kind, body = connection.receive()
            frames.append((kind, body))
            if kind == b"Z":
                break
        assert frames == [(b"C", b"INSERT 0 1\0"), (b"Z", b"I")], frames
        server.stop(immediate=True)
        connection.close()
        connection = None
        server.start()
        connection = Connection(server.port)
        assert connection.query("SELECT id FROM completion_rows") == [["1"]]
        # Explicit transaction statements must report completion before COMMIT.
        connection.query("BEGIN")
        connection.query("INSERT INTO completion_rows VALUES(2)")
        connection.query("ROLLBACK")
        assert connection.query("SELECT id FROM completion_rows") == [["1"]]
        # Extended Execute completes before Sync commits its implicit block.
        connection.socket.sendall(
            parse("INSERT INTO completion_rows VALUES(3)") + bind()
            + frame(b"E", b"\0" + struct.pack("!I", 0)) + frame(b"H", b""))
        assert connection.receive() == (b"1", b"")
        assert connection.receive() == (b"2", b"")
        assert connection.receive() == (b"C", b"INSERT 0 1\0")
        # End that block and execute another transaction control command in
        # the same simple-query message: its outer deferred tag must survive.
        connection.send(b"Q", b"ROLLBACK; COMMIT\0")
        frames = []
        while True:
            kind, body = connection.receive()
            if kind in (b"C", b"Z", b"E"):
                frames.append((kind, body))
            if kind == b"Z":
                break
        assert frames == [(b"C", b"ROLLBACK\0"), (b"C", b"COMMIT\0"),
                          (b"Z", b"I")], frames
        assert connection.query("SELECT id FROM completion_rows") == [["1"]]
        # Inject an ordinary commit-time error before any WAL commit work.
        # Restore the function before issuing another query on this server.
        server.control("(defvar pg::*completion-test-commit* (symbol-function 'pg::commit-transaction))")
        try:
            server.control("(setf (symbol-function 'pg::commit-transaction) "
                           "(lambda () (pg::ereport \"40001\" \"injected commit failure\")))")
            connection.send(b"Q", b"INSERT INTO completion_rows VALUES(4)\0")
            frames = []
            while True:
                kind, body = connection.receive()
                frames.append((kind, body))
                if kind == b"Z":
                    break
            assert [kind for kind, _ in frames] == [b"E", b"Z"], frames
            assert connection.error_fields(frames[0][1])["C"] == "40001", frames
            assert frames[-1][1] == b"I", frames
        finally:
            server.control("(setf (symbol-function 'pg::commit-transaction) pg::*completion-test-commit*)")
        assert connection.query("SELECT id FROM completion_rows") == [["1"]]
        print("command-complete: passed (WAL barrier, recovery, commit error, explicit and extended boundaries)")
    finally:
        if resume.parent.exists():
            resume.touch()
        if connection is not None:
            connection.close()
        server.stop()
        print(f"artifacts: {server.root}")


if __name__ == "__main__":
    run()

#!/usr/bin/env python3
"""Exercise startup and storage reads against isolated, damaged cluster copies."""

import contextlib
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time


BASE = Path(__file__).resolve().parents[1]
SERVER = BASE / "pg-lisp.sh"
CATALOG = Path("global/pg_catalog.dat")


def check_same_second_identities(work):
    # Load the real generator in separate fresh images. Once both images are
    # ready, give them a shared future second so compile/load time is irrelevant.
    script = work / "identity.lisp"
    script.write_text('''(require :asdf)
(push (uiop:ensure-directory-pathname (first (uiop:command-line-arguments)))
      asdf:*central-registry*)
(let ((*compile-verbose* nil) (*load-verbose* nil))
  (asdf:load-system "pg"))
(with-open-file (ready (second (uiop:command-line-arguments)) :direction :output)
  (write-line "ready" ready))
(let ((target (read)))
  (loop while (< (get-universal-time) target) do (sleep 0.001))
  (format t "~&~d ~a ~d~%" (get-universal-time)
          (pg::catalog-cluster-identity (setf pg::*catalog* (pg::make-catalog)))
          (pg::catalog-system-identifier)))
''')
    with contextlib.ExitStack() as stack:
        images = []
        for number in range(2):
            ready = work / f"ready-{number}"
            image = subprocess.Popen(
                [os.environ.get("SBCL", "sbcl"), "--script", str(script), str(BASE), str(ready)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            )
            stack.callback(stop, image)
            images.append((image, ready))
        deadline = time.monotonic() + 60
        while not all(ready.exists() for _, ready in images):
            assert all(image.poll() is None for image, _ in images), "identity worker exited"
            assert time.monotonic() < deadline, "identity workers did not start"
            time.sleep(0.02)
        second = int(time.time()) + 2208988800 + 2
        for image, _ in images:
            image.stdin.write(f"{second}\n")
            image.stdin.flush()
        results = []
        for image, _ in images:
            output, _ = image.communicate(timeout=10)
            assert image.returncode == 0, output
            results.append(output.splitlines()[-1].split())
        assert all(int(row[0]) == second for row in results), results
        assert all(int(row[1].split("-")[0], 36) == second for row in results), results
        assert results[0][1] != results[1][1] and results[0][2] != results[1][2], results


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise


def sql(port, statement, *, replication=False):
    conninfo = (f"host=127.0.0.1 port={port} user=postgres dbname=postgres "
                "connect_timeout=1 password=x")
    if replication:
        conninfo += " replication=database"
    return subprocess.run(
        ["psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-d", conninfo, "-c", statement],
        capture_output=True, text=True, timeout=10,
    )


def query(port, statement, **kwargs):
    result = sql(port, statement, **kwargs)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


@contextlib.contextmanager
def start(work, data, *, control=False):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    log = work / f"{data.name}.log"
    command = [str(SERVER), "-D", str(data), "-p", str(port)]
    if control:
        path = work / f"{data.name}.sock"
        path.unlink(missing_ok=True)
        command += ["--control-socket", str(path)]
    with log.open("w") as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        try:
            yield process, port, log
        finally:
            stop(process)


def wait_ready(process, port, log):
    deadline = time.monotonic() + 90
    while process.poll() is None and time.monotonic() < deadline:
        if sql(port, "SELECT 1").returncode == 0:
            return
        time.sleep(0.05)
    raise AssertionError(log.read_text())


def relations(data):
    return {path.name: path.read_bytes() for path in (data / "base").iterdir()}


def check_storage(work):
    seed = work / "seed"
    with start(work, seed) as server:
        wait_ready(*server)
        port = server[1]
        system_id = query(port, "IDENTIFY_SYSTEM", replication=True).split("|")[0]
        query(port, "CREATE TABLE storage_review (id integer PRIMARY KEY, value text)")
        query(port, "INSERT INTO storage_review VALUES (1, 'survives-checkpoint')")
        # Fresh relations use their OID as their initial relfilenode.
        heap, index = [
            Path("base") / query(
                port, f"SELECT oid FROM pg_catalog.pg_class WHERE relname = '{name}'"
            )
            for name in ("storage_review", "storage_review_pkey")
        ]
        query(port, "CHECKPOINT")
    with start(work, seed) as server:
        wait_ready(*server)
        assert query(server[1], "IDENTIFY_SYSTEM", replication=True).split("|")[0] == system_id

    for missing in (CATALOG, Path("pg_xact")):
        data = work / f"missing-{missing.name}"
        shutil.copytree(seed, data)
        (data / missing).unlink()
        before = relations(data)
        with start(work, data) as (process, _, log):
            assert process.wait(timeout=30) == 1, log.read_text()
            assert missing.name in log.read_text(), log.read_text()
        assert not (data / missing).exists()
        assert relations(data) == before
        if missing != CATALOG:
            assert (data / CATALOG).read_bytes() == (seed / CATALOG).read_bytes()

    data = work / "truncated-clog"
    shutil.copytree(seed, data)
    clog = data / "pg_xact"
    assert clog.stat().st_size > 0
    clog.write_bytes(b"")
    with start(work, data) as (process, _, log):
        assert process.wait(timeout=30) == 1, log.read_text()
        assert "transaction status file pg_xact is truncated" in log.read_text()
    assert clog.stat().st_size == 0

    data = work / "foreign-catalog"
    shutil.copytree(seed, data)
    foreign = work / "foreign"
    with start(work, foreign) as server:
        wait_ready(*server)
        assert query(server[1], "IDENTIFY_SYSTEM", replication=True).split("|")[0] != system_id
    shutil.copyfile(foreign / CATALOG, data / CATALOG)
    before = relations(data)
    catalog_bytes = (data / CATALOG).read_bytes()
    with start(work, data) as (process, _, log):
        assert process.wait(timeout=30) == 1, log.read_text()
        assert "primary control and catalog system identifiers differ" in log.read_text()
    assert relations(data) == before
    assert (data / CATALOG).read_bytes() == catalog_bytes

    for name, damaged, truncate, statement in (
        ("missing-heap", heap, False, "SELECT * FROM storage_review"),
        ("missing-index", index, False, "INSERT INTO storage_review VALUES (1, 'duplicate')"),
        ("empty-index", index, True, "INSERT INTO storage_review VALUES (1, 'duplicate')"),
    ):
        data = work / name
        shutil.copytree(seed, data)
        if truncate:
            (data / damaged).write_bytes(b"")
        else:
            (data / damaged).unlink()
        before = relations(data)
        with start(work, data) as server:
            wait_ready(*server)
            port = server[1]
            result = sql(port, statement)
            assert result.returncode != 0, result.stdout
            expected = "index storage is incomplete" if truncate else str(data / damaged)
            assert expected in result.stderr, result.stderr
            if damaged == index:
                assert query(port, "SELECT * FROM storage_review") == "1|survives-checkpoint"
        assert relations(data) == before


def control(work, data, expression):
    path = work / f"{data.name}.sock"
    result = subprocess.run(
        [os.environ.get("SBCL", "sbcl"), "--script", str(BASE / "t/control-client.lisp"),
         "unix", str(path)],
        input=expression + "\n(quit)\n", capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0 and ";; error:" not in result.stdout, result.stderr + result.stdout


def check_standby_clog(work):
    source = work / "clog-source"
    base = work / "clog-base"
    base.mkdir(mode=0o700)
    with start(work, source, control=True) as server:
        wait_ready(*server)
        query(server[1], "CREATE TABLE standby_status_rows(id int)")
        query(server[1], "INSERT INTO standby_status_rows VALUES (42)")
        control(work, source, f'(physical-base-backup "{base}/" "clog_extent_test")')

    for mode in ("static", "transitioned", "streaming-restart"):
        data = work / mode
        shutil.copytree(base, data)
        if mode == "streaming-restart":
            with start(work, data, control=True) as server:
                wait_ready(*server)
                control(work, data, "(physical-recovery-enable-streaming)")
        with start(work, data, control=True) as server:
            wait_ready(*server)
            port = server[1]
            assert query(port, "SELECT pg_is_in_recovery()") == "t"
            if mode == "transitioned":
                control(work, data, "(physical-recovery-enable-streaming)")
            writable = "nil" if mode == "static" else "t"
            control(work, data, f"(assert (eq (output-stream-p *clog-stream*) {writable}))")
            statement = "SELECT id FROM standby_status_rows"
            assert query(port, statement) == "42"
            clog = data / "pg_xact"
            assert clog.stat().st_size > 0
            clog.write_bytes(b"")
            result = sql(port, statement)
            assert result.returncode != 0 and "XX001" in result.stderr, (mode, result)
            assert "transaction status file" in result.stderr, (mode, result)


    for checkpoint in (False, True):
        data = work / ("truncated-restartpoint-clog" if checkpoint else "truncated-base-clog")
        shutil.copytree(base, data)
        with start(work, data, control=True) as server:
            wait_ready(*server)
            control(work, data, "(physical-recovery-enable-streaming)")
            assert query(server[1], "SELECT id FROM standby_status_rows") == "42"
            if checkpoint:
                query(server[1], "CHECKPOINT")
                generations = list((data / "pg_physical_restartpoints").glob("*.rp"))
                assert any(b":CLOG-BYTES" in path.read_bytes() for path in generations)
            else:
                # A clean shutdown publishes a restartpoint. Preserve the
                # base-only state for this branch of the cold-start check.
                server[0].kill()
                server[0].wait(timeout=15)
        with start(work, data) as server:
            wait_ready(*server)
            assert query(server[1], "SELECT id FROM standby_status_rows") == "42"
            if not checkpoint:
                server[0].kill()
                server[0].wait(timeout=15)
        assert (data / "physical-restartpoint.control").exists() == checkpoint
        clog = data / "pg_xact"
        assert clog.stat().st_size > 0
        clog.write_bytes(b"")
        with start(work, data) as (process, _, log):
            assert process.wait(timeout=30) == 1, log.read_text()
            assert "transaction status file pg_xact is truncated" in log.read_text()
        assert clog.stat().st_size == 0


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="pg-storage-integrity-") as directory:
        work = Path(directory)
        check_storage(work)
        check_standby_clog(work)
        check_same_second_identities(work)
    print("storage-integrity: PASS")

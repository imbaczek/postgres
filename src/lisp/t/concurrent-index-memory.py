"""Measure pg-lisp concurrent index allocation and peak RSS on Linux."""

import argparse
import json
import re
import runpy
import statistics
import threading
import time
from pathlib import Path

fixture = runpy.run_path(str(Path(__file__).with_name("command-complete.py")))
OwnedLisp = fixture["OwnedLisp"]
Connection = fixture["Connection"]


def resident_bytes(pid):
    with Path(f"/proc/{pid}/status").open() as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("server did not report resident memory")


def bytes_consed(server, *, collect=False):
    gc = "(sb-ext:gc :full t)" if collect else ""
    response = server.control(
        f'(progn {gc} (format t "~&INDEX_BYTES=~d~%" (sb-ext:get-bytes-consed)) (values))'
    )
    return int(re.search(r"INDEX_BYTES=(\d+)", response)[1])


def measure(server, operation):
    pid = server.process.pid
    before = bytes_consed(server, collect=True)
    resident_before = resident_bytes(pid)
    stopped = threading.Event()
    samples = [resident_before]

    def sample():
        while not stopped.wait(0.01):
            samples.append(resident_bytes(pid))

    poller = threading.Thread(target=sample)
    poller.start()
    start = time.monotonic()
    try:
        operation()
    finally:
        elapsed = time.monotonic() - start
        samples.append(resident_bytes(pid))
        stopped.set()
        poller.join()
    return {
        "seconds": elapsed,
        "allocated_bytes": bytes_consed(server) - before,
        "before_rss_bytes": resident_before,
        "peak_rss_bytes": max(samples),
        "peak_rss_growth_bytes": max(samples) - resident_before,
    }


def run_case(rows, distinct):
    server = OwnedLisp()
    connection = None
    try:
        server.start()
        connection = Connection(server.port)
        connection.socket.settimeout(180)
        connection.query("CREATE TABLE index_memory(k int, payload varchar(100))")
        for start in range(0, rows, 250):
            values = ",".join(
                f"({value if distinct else value % 4},'{96 * 'x'}')"
                for value in range(start, min(start + 250, rows))
            )
            connection.query(f"INSERT INTO index_memory VALUES {values}")
        unique = "UNIQUE " if distinct else ""
        create = measure(server, lambda: connection.query(
            f"CREATE {unique}INDEX CONCURRENTLY memory_idx ON index_memory(k)"
        ))
        assert connection.query("SELECT count(*) FROM index_memory WHERE k=0") == [
            [str(1 if distinct else (rows + 3) // 4)]
        ]
        drop = measure(server, lambda: connection.query("DROP INDEX CONCURRENTLY memory_idx"))
        return {"create": create, "drop": drop}
    finally:
        if connection is not None:
            connection.close()
        server.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=4000)
    parser.add_argument("--runs", type=int, default=3)
    arguments = parser.parse_args()
    if arguments.rows < 1 or arguments.runs < 1:
        parser.error("--rows and --runs must be positive")

    result = {}
    for label, distinct in (("distinct_unique", True), ("four_equal_key_groups", False)):
        runs = [run_case(arguments.rows, distinct) for _ in range(arguments.runs)]
        result[label] = {
            "samples": runs,
            "median": {
                phase: {metric: statistics.median(run[phase][metric] for run in runs)
                        for metric in runs[0][phase]}
                for phase in ("create", "drop")
            },
        }
    print(json.dumps({"rows": arguments.rows, "results": result}, indent=2))


if __name__ == "__main__":
    main()

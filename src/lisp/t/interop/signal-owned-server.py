"""Check PGDATA and use a pidfd so PID reuse cannot redirect the signal."""
import os
import select
import signal
import sys
from pathlib import Path


def main(pid, pgdata, signal_name, wait_seconds=None):
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        return 0
    try:
        try:
            arguments = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        except FileNotFoundError:
            return 0
        expected = Path(pgdata).resolve()
        actual = None
        if b"-D" in arguments:
            position = arguments.index(b"-D") + 1
            if position < len(arguments):
                actual = Path(os.fsdecode(arguments[position])).resolve()
        if actual != expected:
            print(f"refusing to signal PID {pid}: PGDATA ownership does not match", file=sys.stderr)
            return 1
        try:
            signal.pidfd_send_signal(descriptor, getattr(signal, "SIG" + signal_name))
        except ProcessLookupError:
            pass
        if wait_seconds is not None and not select.select([descriptor], [], [], wait_seconds)[0]:
            try:
                signal.pidfd_send_signal(descriptor, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if not select.select([descriptor], [], [], 5)[0]:
                print(f"PID {pid} did not exit after owned SIGKILL", file=sys.stderr)
                return 125
            print(f"PID {pid} exceeded its shutdown deadline", file=sys.stderr)
            return 124
        return 0
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    wait_seconds = float(sys.argv[4]) if len(sys.argv) > 4 else None
    sys.exit(main(int(sys.argv[1]), sys.argv[2], sys.argv[3], wait_seconds))

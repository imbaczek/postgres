"""Pause an owned server's writer outside admission and storage locks."""
from pathlib import Path
import time


def wait_file(server, path):
    deadline = time.monotonic() + 15
    while not path.exists():
        assert server.process is not None and server.process.poll() is None, (
            f'owned server exited before {path}')
        assert time.monotonic() < deadline, f'writer barrier was not reached: {path}'
        time.sleep(0.02)


def pause_writer(server, label):
    paths = tuple(server.root / f'writer-{label}-{suffix}'
                  for suffix in ('ready', 'release', 'done'))
    for path in paths:
        assert not path.exists(), f'writer barrier already used: {path}'
    source = Path(__file__).with_name('wal-writer-failpoints.lisp').resolve()
    server.control(
        f'(load #p"{source}") '
        f'(wal-writer-test-pause-next-cycle #p"{paths[0]}" #p"{paths[1]}" #p"{paths[2]}")')
    try:
        wait_file(server, paths[0])
    except BaseException:
        paths[1].touch()
        raise
    return paths


def resume_writer(server, paths):
    if paths is None:
        return
    paths[1].touch()
    if server.process is not None and server.process.poll() is None:
        wait_file(server, paths[2])


def wait_control(server, expression, marker, timeout=15, call_timeout=None):
    deadline = time.monotonic() + timeout
    while True:
        assert server.process is not None and server.process.poll() is None
        assert time.monotonic() < deadline, f'writer state did not reach {marker}'
        if call_timeout is None:
            output = server.control(expression)
        else:
            output = server.control(expression, timeout=min(call_timeout, deadline - time.monotonic()))
        assert time.monotonic() < deadline, f'writer state did not reach {marker}'
        if marker in output:
            return
        time.sleep(0.02)


def paused_writer_server(base):
    """Keep manual settlement and snapshot oracles in control of their queue."""
    class PausedWriterLisp(base):
        def start(self):
            self.writer_barrier = None
            super().start()
            self.writer_pause_index = getattr(self, 'writer_pause_index', 0) + 1
            self.writer_barrier = pause_writer(self, f'inspection-{self.writer_pause_index}')

        def stop(self, immediate=False):
            barrier = getattr(self, 'writer_barrier', None)
            self.writer_barrier = None
            if immediate:
                try:
                    super().stop(immediate=True)
                finally:
                    resume_writer(self, barrier)
            else:
                try:
                    resume_writer(self, barrier)
                finally:
                    super().stop()

    return PausedWriterLisp

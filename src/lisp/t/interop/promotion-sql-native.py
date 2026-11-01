"""Run the unchanged shared SQL promotion contract on native physical peers."""
import runpy
import os
import subprocess
import sys
from pathlib import Path


def check_writer(base, root, name, expected):
    expression = ('(sb-thread:with-mutex (*wal-lock*) '
                  '(sb-thread:with-mutex (*primary-wal-writer-lock*) ')
    if expected:
        expression += ('(assert *primary-wal-writer*) '
                       '(assert (sb-thread:thread-alive-p (primary-wal-writer-thread *primary-wal-writer*))) '
                       '(assert (null (primary-wal-writer-failure *primary-wal-writer*))) '
                       '(assert (eq *wal-storage-incarnation* '
                       '(wal-writer-epoch-incarnation (primary-wal-writer-epoch *primary-wal-writer*))))))')
    else:
        expression += '(assert (null *primary-wal-writer*))))'
    result = subprocess.run(
        [os.environ.get('SBCL', 'sbcl'), '--script', str(base / 't/control-client.lisp'),
         'unix', str(root / f'{name}.sock')], input=expression + '\n(quit)\n',
        text=True, capture_output=True, check=True, timeout=15)
    output = result.stdout + result.stderr
    assert ';; error:' not in output and ';; read error:' not in output, output


if __name__ == "__main__":
    base = Path(sys.argv[1])
    root = Path(sys.argv[2])
    contract = runpy.run_path(str(base / "t/interop/promotion-sql-contract.py"))
    source, target = map(int, sys.argv[3:5])
    connections = [contract["connect"](port) for port in (source, target, target)]
    try:
        check_writer(base, root, 'source', True)
        check_writer(base, root, 'target', False)
        contract["check"](*connections)
        check_writer(base, root, 'target', True)
    finally:
        for connection in connections:
            connection.close()
    print("promotion-sql-native: shared C SQL contract PASS", flush=True)

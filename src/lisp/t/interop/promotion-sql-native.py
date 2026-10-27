"""Run the unchanged shared SQL promotion contract on native physical peers."""
import runpy
import sys
from pathlib import Path


if __name__ == "__main__":
    base = Path(sys.argv[1])
    contract = runpy.run_path(str(base / "t/interop/promotion-sql-contract.py"))
    source, target = map(int, sys.argv[3:5])
    connections = [contract["connect"](port) for port in (source, target, target)]
    try:
        contract["check"](*connections)
    finally:
        for connection in connections:
            connection.close()
    print("promotion-sql-native: shared C SQL contract PASS", flush=True)

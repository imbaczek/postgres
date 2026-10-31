#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
exec "$PATRONI_PYTHON" "$BASE/t/interop/patroni-follow.py"

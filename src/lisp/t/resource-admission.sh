#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
exec "${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}" "$BASE/t/interop/resource-admission.py"

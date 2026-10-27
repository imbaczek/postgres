#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PARKING_DRIVER="$BASE/t/interop/index-scan-parking.py"
export PARKING_SETUP_DRIVER="$PARKING_DRIVER"
export PORT="${PORT:-6670}"
exec "$BASE/t/live-operation-parking.sh"

#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PORT=${PORT:-6800}
export PARKING_DRIVER="$BASE/t/interop/selected-replay.py"
exec "$BASE/t/live-operation-parking.sh"

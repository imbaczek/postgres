#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PORT=${PORT:-6714}
export PARKING_DRIVER="$BASE/t/interop/promotion-sql-native.py"
exec "$BASE/t/live-operation-parking.sh"

#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PORT=${PORT:-6760}
export CHILD_BOOTSTRAP_PROMOTE=${CHILD_BOOTSTRAP_PROMOTE:-1}
export PARKING_DRIVER="$BASE/t/interop/child-bootstrap.py"
exec "$BASE/t/live-operation-parking.sh"

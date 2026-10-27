#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PORT=${PORT:-6730}
export PARKING_DRIVER="$BASE/t/interop/promotion-sql-faults.py"
if [ "$#" -eq 0 ]; then
    set -- async_accept rollback statement_error timeout cancel disconnect request_file handoff_cancel unsafe_batch
fi
for PROMOTION_SQL_CASE do
    export PROMOTION_SQL_CASE
    "$BASE/t/live-operation-parking.sh"
done
echo 'promotion-sql-faults: native requested cases PASS'

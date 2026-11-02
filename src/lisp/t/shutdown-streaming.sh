#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PORT=${PORT:-8410}
export PARKING_DRIVER="$BASE/t/interop/shutdown-streaming.py"
for SHUTDOWN_STREAMING_SIGNAL in INT TERM; do
    export SHUTDOWN_STREAMING_SIGNAL
    "$BASE/t/live-operation-parking.sh"
done
for SHUTDOWN_STREAMING_SCENARIO in no-stream quit checkpoint-cut second-disconnect; do
    export SHUTDOWN_STREAMING_SCENARIO
    SHUTDOWN_STREAMING_SIGNAL=INT "$BASE/t/live-operation-parking.sh"
done

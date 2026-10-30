#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PORT=${PORT:-8310}
export FOLLOW_CONFIGURED=1
exec "$BASE/t/libpq-follow.sh"

#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-libpq-follow.XXXXXX")
trap 'echo "libpq-follow: helper artifacts=$TMP"' EXIT
if [ -n "${REFERENCE_POSTGRES:-}" ] && [ -z "${PG_CONFIG:-}" ]; then
    PG_CONFIG="$(dirname -- "$REFERENCE_POSTGRES")/pg_config"
    export PG_CONFIG
fi
"$BASE/t/build-libpq-helper.sh" "$TMP/helper"
export PG_LIBPQ_HELPER="$TMP/helper"
export FOLLOW_LIBPQ=1 SELECTED_REPLAY_CONTROLLER=1
"$BASE/t/selected-replay.sh"

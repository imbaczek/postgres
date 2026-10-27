#!/bin/sh
# Owned-server source-anchor proof and crash recovery; no network onboarding yet.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export SELECTED_REPLAY_ANCHOR=1
exec "$BASE/t/selected-replay.sh"

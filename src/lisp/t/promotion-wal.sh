#!/bin/sh
# Reuse owned native fixture and storage checks for the history/EOR boundaries.
set -eu
PROMOTION_WAL=1
export PROMOTION_WAL
exec "$(dirname -- "$0")/promotion-prefix.sh" "$@"

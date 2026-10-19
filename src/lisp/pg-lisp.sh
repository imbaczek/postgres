#!/bin/sh
# Launcher for the Lisp PostgreSQL-compatible server.
set -e

BASE=$(cd "$(dirname "$0")" && pwd)
SBCL="$BASE/../../../sbcl/run-sbcl.sh"

if [ ! -x "$SBCL" ]; then
    echo "pg-lisp: cannot find SBCL at $SBCL" >&2
    exit 1
fi

exec "$SBCL" --script "$BASE/boot.lisp" "$@"

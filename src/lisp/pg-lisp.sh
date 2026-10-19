#!/bin/sh
# Launcher for the Lisp PostgreSQL-compatible server.
set -e

BASE=$(cd "$(dirname "$0")" && pwd)
SBCL=${SBCL:-sbcl}

if ! command -v "$SBCL" >/dev/null 2>&1; then
    echo "pg-lisp: cannot find SBCL executable '$SBCL' (set SBCL to override)" >&2
    exit 127
fi

exec "$SBCL" --script "$BASE/boot.lisp" "$@"

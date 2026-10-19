#!/bin/sh
# Force-compile the pg ASDF system and fail on compiler warnings.
set -eu

BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
REPO_ROOT=$(CDPATH= cd -- "$BASE/../.." && pwd)
DEFAULT_SBCL="$REPO_ROOT/../sbcl/run-sbcl.sh"

LISP=${SBCL:-$DEFAULT_SBCL}

if ! command -v "$LISP" >/dev/null 2>&1; then
    echo "compile: cannot find SBCL launcher '$LISP' (set SBCL to override)" >&2
    exit 127
fi

exec "$LISP" --noinform --disable-debugger --non-interactive \
    --eval '(require :asdf)' \
    --eval "(push #p\"$BASE/\" asdf:*central-registry*)" \
    --eval '(setf uiop:*compile-file-warnings-behaviour* :error
                 uiop:*compile-file-failure-behaviour* :error)' \
    --eval '(asdf:compile-system "pg" :force t)'

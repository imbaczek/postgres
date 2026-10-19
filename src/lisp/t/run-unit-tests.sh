#!/bin/sh
# t/run-unit-tests.sh -- load the pg system and run t/unit-tests.lisp.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL="$BASE/../../../sbcl/run-sbcl.sh"

exec "$SBCL" --non-interactive \
    --eval '(require :asdf)' \
    --eval "(push #p\"$BASE/\" asdf:*central-registry*)" \
    --eval '(asdf:load-system "pg")' \
    --load "$BASE/t/unit-tests.lisp"

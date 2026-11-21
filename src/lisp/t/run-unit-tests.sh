#!/bin/sh
# Run both unit harnesses through ASDF.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}

if [ "$#" -eq 1 ] && [ "$1" = "--list" ]; then
    exec "$SBCL" --non-interactive \
        --eval '(require :asdf)' \
        --eval "(push #p\"$BASE/\" asdf:*central-registry*)" \
        --load "$BASE/t/test-dependencies.lisp" \
        --load "$BASE/t/system-test-runner.lisp" \
        --eval '(cl-user::pg-print-unit-test-inventory)'
fi
if [ "$#" -ne 0 ]; then
    echo "Usage: $0 [--list]" >&2
    exit 2
fi

exec "$SBCL" --non-interactive \
    --eval '(require :asdf)' \
    --eval "(push #p\"$BASE/\" asdf:*central-registry*)" \
    --load "$BASE/t/test-coverage.lisp" \
    --eval "(cl-user::call-with-pg-test-coverage
              (lambda ()
                (asdf:load-system \"pg\")
                (load \"$BASE/t/test-dependencies.lisp\")
                (asdf:test-system \"pg-unit-tests/all\")))"

#!/bin/sh
# Force-compile the pg ASDF system and fail on compiler warnings.
# Catch deferred compilation-unit warnings too; SBCL macro reload notices are exempt.
set -eu

BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

LISP=${SBCL:-sbcl}

if ! command -v "$LISP" >/dev/null 2>&1; then
    echo "compile: cannot find SBCL executable '$LISP' (set SBCL to override)" >&2
    exit 127
fi

exec "$LISP" --noinform --disable-debugger --non-interactive \
    --eval '(require :asdf)' \
    --eval "(push #p\"$BASE/\" asdf:*central-registry*)" \
    --eval '(setf uiop:*compile-file-warnings-behaviour* :error
                 uiop:*compile-file-failure-behaviour* :error)' \
    --eval '(handler-bind ((warning (lambda (condition)
                                     (if (typep condition (quote sb-kernel:redefinition-warning))
                                         (muffle-warning condition)
                                         (error "Compiler warning: ~a" condition)))))
              (asdf:compile-system "pg" :force t))'

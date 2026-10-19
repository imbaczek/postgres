#!/bin/sh
# Format Common Lisp files using Emacs + SLIME's live macro indentation.
set -eu

BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
EMACS=${EMACS:-emacs}
CHECK=0

LISP=${PG_LISP_INFERIOR:-${SBCL:-sbcl}}

usage() {
    echo "Usage: $0 [--check] [--lisp PATH] [--] FILE..." >&2
    echo "Set PG_LISP_INFERIOR to choose the inferior Lisp without --lisp." >&2
}

while [ "$#" -gt 0 ]; do
    case $1 in
        --check)
            CHECK=1
            shift
            ;;
        --lisp)
            if [ "$#" -lt 2 ]; then
                usage
                exit 2
            fi
            LISP=$2
            shift 2
            ;;
        --)
            shift
            break
            ;;
        --help)
            usage
            exit 0
            ;;
        -*)
            usage
            exit 2
            ;;
        *)
            break
            ;;
    esac
done

if [ "$#" -eq 0 ]; then
    usage
    exit 2
fi

for input_file
do
    if [ ! -f "$input_file" ]; then
        echo "format: not a regular file '$input_file'" >&2
        exit 2
    fi
done

if ! command -v "$EMACS" >/dev/null 2>&1; then
    echo "format: cannot find Emacs executable '$EMACS' (set EMACS to override)" >&2
    exit 127
fi

if ! command -v "$LISP" >/dev/null 2>&1; then
    echo "format: cannot find inferior Lisp '$LISP' (use --lisp or PG_LISP_INFERIOR)" >&2
    exit 127
fi

export PG_LISP_ROOT=$BASE
export PG_LISP_INFERIOR=$LISP
export PG_LISP_FORMAT_CHECK=$CHECK

exec "$EMACS" --batch -Q -l "$BASE/t/format.el" -- "$@"

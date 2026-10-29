#!/bin/sh
set -eu
if [ "$#" -ne 1 ]; then
    echo 'usage: build-libpq-helper.sh OUTPUT' >&2
    exit 2
fi
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CC=${CC:-cc}
PG_CONFIG=${PG_CONFIG:-pg_config}
INCLUDE=$("$PG_CONFIG" --includedir)
LIBRARY=$("$PG_CONFIG" --libdir)
"$CC" -std=c11 -Wall -Wextra -Werror -O2 -I"$INCLUDE" \
    "$BASE/port/libpq-helper.c" -L"$LIBRARY" -Wl,-rpath,"$LIBRARY" -lpq -o "$1"

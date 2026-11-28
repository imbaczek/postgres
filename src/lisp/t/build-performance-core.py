#!/usr/bin/env python3
"""Build a preloaded pg-lisp core and an SBCL launcher for performance tests."""

import argparse
import os
from pathlib import Path
import shlex
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="New directory for the core, FASLs, and launcher")
    parser.add_argument("--sbcl", default=os.environ.get("SBCL", "sbcl"),
                        help="SBCL executable, preferably with a compact dynamic-space build")
    parser.add_argument("--heap-mib", type=int, default=72,
                        help="Dynamic-space limit in MiB for the launcher (default: 72)")
    parser.add_argument("--gc-threshold-kib", type=int, default=7 * 1024,
                        help="Allocation threshold between collections in KiB (default: 7168)")
    args = parser.parse_args()
    if args.heap_mib < 1 or args.gc_threshold_kib < 1:
        parser.error("heap size and GC threshold must be positive")
    executable = shutil.which(args.sbcl)
    if not executable:
        parser.error(f"SBCL executable {args.sbcl!r} was not found")
    executable = str(Path(executable).absolute())
    output = args.output.resolve()
    try:
        output.mkdir()
    except OSError as error:
        parser.error(str(error))

    source = Path(__file__).resolve().parent.parent
    environment = {
        **os.environ,
        "PG_PERF_SOURCE": str(source) + "/",
        "PG_PERF_CORE": str(output / "pg.core"),
        "PG_PERF_FASLS": str(output / "fasl") + "/",
    }
    with (output / "build.log").open("w") as log:
        subprocess.run([
            executable, "--noinform", "--no-sysinit", "--no-userinit", "--non-interactive",
            "--eval", "(require :asdf)",
            "--eval", "(asdf:initialize-output-translations "
            "(list :output-translations (list t (sb-ext:posix-getenv \"PG_PERF_FASLS\")) "
            ":ignore-inherited-configuration))",
            "--eval", "(proclaim '(optimize (debug 0)))",
            "--eval", "(push (pathname (sb-ext:posix-getenv \"PG_PERF_SOURCE\")) "
            "asdf:*central-registry*)",
            "--eval", "(asdf:load-system \"pg\")",
            "--eval", "(sb-ext:save-lisp-and-die (sb-ext:posix-getenv \"PG_PERF_CORE\"))",
        ], env=environment, stdout=log, stderr=subprocess.STDOUT, check=True)

    launcher = output / "sbcl"
    home = os.environ.get("SBCL_HOME")
    launcher.write_text(
        "#!/bin/sh\n"
        + (f"export SBCL_HOME={shlex.quote(home)}\n" if home else "")
        + f"runtime={shlex.quote(executable)}\n"
        + f"core={shlex.quote(str(output / 'pg.core'))}\n"
        + 'if [ "${1-}" = --script ] && [ "${2##*/}" = boot.lisp ]; then\n'
        + "  shift 2\n"
        + f'  exec "$runtime" --core "$core" --dynamic-space-size {args.heap_mib} --noinform \\\n'
        + "    --no-sysinit --no-userinit --non-interactive \\\n"
        + f"    --eval '(setf (sb-ext:bytes-consed-between-gcs) {args.gc_threshold_kib * 1024})' \\\n"
        + "    --eval '(pg:main)' --end-toplevel-options \"$@\"\n"
        + "fi\n"
        + 'exec "$runtime" "$@"\n'
    )
    launcher.chmod(0o755)
    print(launcher)


if __name__ == "__main__":
    main()

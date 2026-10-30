"""Compare startup cluster_name normalization with the pinned C server."""
import subprocess
import sys
import tempfile
from pathlib import Path


def setting(executable, directory, assignments):
    command = [str(executable), "-D", str(directory), "-C", "cluster_name"]
    for assignment in assignments:
        command.extend(("-c", assignment))
    result = subprocess.run(command, capture_output=True, text=True,
                            check=True, timeout=30)
    return result.stdout.removesuffix("\n")


def main():
    base, reference = map(Path, sys.argv[1:])
    native = base / "pg-lisp.sh"
    cases = ["", "research", "quote' and \\ slash", "line\n\tend", "é中",
             "a" * 70, "a" * 62 + "é", "a" * 61 + "é", "é" * 40]
    with tempfile.TemporaryDirectory(prefix="pg-cluster-name-") as temporary:
        directory = Path(temporary)
        configuration = directory / "postgresql.conf"
        configuration.write_text("")
        assert setting(native, directory, []) == setting(reference, directory, []) == ""
        for value in cases:
            assignments = ["cluster_name=" + value]
            expected = setting(reference, directory, assignments)
            actual = setting(native, directory, assignments)
            assert actual == expected, (value, expected, actual)
        for value in (r"\303\251", r"\377", "a" * 62 + r"\303\251",
                      r"\7779", r"\000", r"a\000b", r"\4009"):
            configuration.write_text("cluster_name = '" + value + "'\n")
            expected = setting(reference, directory, [])
            actual = setting(native, directory, [])
            assert actual == expected, ("configuration octets", value, expected, actual)
        included = directory / "included.conf"
        included.write_text("cluster_name = 'included'\n")
        configuration.write_text("cluster_name = 'base'\ninclude = 'included.conf'\n")
        assert setting(native, directory, []) == setting(reference, directory, []) == "included"
        (directory / "postgresql.auto.conf").write_text("cluster_name = 'automatic'\n")
        assert setting(native, directory, []) == setting(reference, directory, []) == "automatic"
        assignments = ["cluster_name=command"]
        assert setting(native, directory, assignments) == setting(reference, directory, assignments) == "command"
    print("cluster-name: C differential normalization and configuration precedence PASS")


if __name__ == "__main__":
    main()

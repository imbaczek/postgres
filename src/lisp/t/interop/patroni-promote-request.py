import importlib.metadata
import pathlib
import sys

from patroni.postgresql import Postgresql

assert importlib.metadata.version("patroni") == "4.1.5"
postgresql = Postgresql.__new__(Postgresql)
postgresql._bin_dir = str(pathlib.Path(__file__).resolve().parents[2] / "bin")
postgresql._data_dir = sys.argv[1]
postgresql.config = {}
assert postgresql.pg_ctl("promote", "-W")
print("patroni-promote-request: unchanged pgcommand/pg_ctl promote -W PASS")

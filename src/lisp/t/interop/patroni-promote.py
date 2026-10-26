import sys

from patroni_native_node import native_node

from patroni.async_executor import CriticalTask
from patroni.postgresql.misc import PostgresqlRole

node = native_node(sys.argv[1])
before = node.controldata()
assert before["Database cluster state"] == "in archive recovery", before
system_id = node.sysid
assert int(system_id) >= 0
assert node.promote(30, CriticalTask()) is True
assert node.role == PostgresqlRole.PRIMARY
after = node.controldata()
assert after["Database cluster state"] == "in production", after
assert after["Database system identifier"] == system_id
assert node.pg_control_timeline() == 2
print("patroni-promote: unchanged promote, pg_ctl, controldata, _wait_promote, sysid and timeline PASS")

import importlib.metadata
import pathlib
from threading import Lock

from patroni.postgresql import Postgresql
from patroni.postgresql.cancellable import CancellableSubprocess
from patroni.postgresql.misc import PostgresqlRole, PostgresqlState
from patroni.postgresql.mpp import Null
from patroni.postgresql.slots import SlotsHandler


def native_node(directory):
    assert importlib.metadata.version("patroni") == "4.1.5"
    root = pathlib.Path(directory).resolve()
    node = Postgresql.__new__(Postgresql)
    node._data_dir = str(root)
    node._bin_dir = str(pathlib.Path(__file__).resolve().parents[2] / "bin")
    node._version_file = str(root / "PG_VERSION")
    node._pg_control = str(root / "global/pg_control")
    node._major_version = node.get_major_version()
    node._state_lock, node._role_lock = Lock(), Lock()
    node._state, node._role = PostgresqlState.RUNNING, PostgresqlRole.REPLICA
    node._sysid = ""
    node.bootstrapping = False
    node.config = {}
    node.cancellable = CancellableSubprocess()
    node.mpp_handler = Null().get_handler_impl(node)
    node.slots_handler = SlotsHandler(node)
    return node

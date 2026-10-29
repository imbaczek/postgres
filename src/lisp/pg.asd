(asdf:defsystem "pg"
  :version "0.1"
  :description "Minimal PostgreSQL-compatible server in Common Lisp"
  :license "PostgreSQL"
  :serial t
  :components
  ((:file "packages")
   (:module "include"
    :serial t
    :components ((:file "pg-limits")
                 (:file "pg-protocol")))
   (:module "port"
    :serial t
    :components ((:module "sbcl"
                  :serial t
                  :components ((:file "sockets")
                               (:file "files")))))
   (:module "backend"
    :serial t
    :components
    ((:file "utils/elog")
     (:file "utils/recovery")
     (:file "utils/lwlock")
     (:module "libpq"
      :serial t
      :components ((:file "pqcomm")
                   (:file "pqformat")))
     (:module "storage"
      :serial t
      :components ((:file "bufpage")
                   (:file "smgr")
                   (:file "wal")
                   (:file "wal-eor")
                   (:file "wal-checkpoint")
                   (:file "bufmgr")))
     (:file "utils/adt/pglz")
     (:file "utils/adt/json")
     (:file "utils/adt/numeric")
     (:file "utils/adt/numeric-storage")
     (:file "utils/adt/jsonb")
     (:file "utils/adt/jsonb-storage")
     (:file "utils/adt/json-extract")
     (:file "utils/adt/float")
     (:file "utils/adt/timezone")
     (:file "utils/adt/datetime")
     (:module "access"
      :serial t
      :components
      ((:module "transam"
        :serial t
        :components ((:file "clog")
                     (:file "snapshot")
                     (:file "xact")
                     (:file "xid-reservation")
                     (:file "running-xacts")))
       (:module "heap"
        :serial t
        :components ((:file "visibilitymap")
                     (:file "heapam")))
       (:module "btree"
        :serial t
        :components ((:file "btree")))
       (:module "vacuum"
        :serial t
        :components ((:file "vacuumlazy")))))
     (:module "postmaster"
      :serial t
      :components ((:file "postmaster")))
     (:module "catalog"
      :serial t
      :components ((:file "catalog")
                   (:file "namespace")))
     (:module "replication"
      :serial t
      :components ((:file "logical" :pathname "logical-stream")
                   (:file "logical-fragments")
                   (:file "schema-contracts")
                   (:file "client")
                   (:file "physical")
                   (:file "physical-store")
                   (:file "physical-source")
                   (:file "../postmaster/pidfile")))
     (:module "commands"
      :serial t
      :components ((:file "vacuum")
                   (:file "analyze")))
     (:module "parser"
      :serial t
      :components ((:file "scan")
                   (:file "../utils/adt/varbit")
                   (:file "gram")
                   (:file "cte")
                   (:file "subselect")
                   (:file "window")
                   (:file "analyze")))
     (:module "executor"
      :serial t
      :components ((:file "exec_arena")
                   (:file "exec_spill")
                   (:file "../replication/logical-spool")
                   (:file "exec_scan")
                   (:file "exec_plan")
                   (:file "exec_window")
                   (:file "exec_iterator")
                   (:file "exec_cursor_window")
                   (:file "exec_cursor")
                   (:file "exec_cte")
                   (:file "exec_subplan")
                   (:file "exec_analyze")
                   (:file "exec_insert")
                   (:file "exec_delete")
                   (:file "exec_subplan_dml")
                   (:file "exec_joined_dml")
                   (:file "exec_on_conflict")
                   (:file "exec_constraints")
                   (:file "exec_vacuum")
                   (:file "exec_simple")))
     (:module "tcop"
      :serial t
      :components ((:file "dest")
                   (:file "control")
                   (:file "../utils/startup-config")
                   (:file "../utils/session-config")
                   (:file "../access/transam/timeline")
                   (:file "../replication/physical-bootstrap")
                   (:file "../replication/physical-recovery")
                   (:file "../replication/physical-store-history")
                   (:file "../storage/wal-namespace")
                   (:file "../storage/wal-lineage")
                   (:file "../storage/wal-timeline")
                   (:file "../replication/physical-history")
                   (:file "../replication/physical-replay")
                   (:file "../replication/physical-replay-history")
                   (:file "../replication/physical-row-replay")
                   (:file "../replication/physical-prefix")
                   (:file "../replication/physical-follow-pin")
                   (:file "../replication/physical-follow-anchor")
                   (:file "../replication/physical-retained-extent")
                   (:file "../replication/physical-retained-overlap")
                   (:file "../replication/physical-retained-inventory")
                   (:file "../replication/physical-retained-roots")
                   (:file "../replication/physical-store-retained")
                   (:file "../replication/physical-retained-seal")
                   (:file "../replication/physical-retained-project")
                   (:file "../replication/physical-restartpoint")
                   (:file "../replication/protocol")
                   (:file "../replication/physical-stream")
                   (:file "../replication/physical-follow")
                   (:file "../replication/physical-promotion")
                   (:file "../replication/physical-promotion-wal")
                   (:file "../replication/physical-promotion-resume")
                   (:file "../replication/physical-promotion-commit")
                   (:file "../replication/physical-promotion-retained")
                   (:file "../replication/physical-promotion-primary")
                   (:file "../utils/control-data")
                   (:file "../replication/physical-promotion-online")
                   (:file "../replication/standard")
                   (:file "../replication/worker")
                   (:file "../replication/standard-subscriber")
                   (:file "postgres")))
     (:file "replication/logical-retention")))))

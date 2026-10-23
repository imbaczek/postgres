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
                   (:file "bufmgr")))
     (:module "access"
      :serial t
      :components
      ((:module "transam"
        :serial t
        :components ((:file "clog")
                     (:file "snapshot")
                     (:file "xact")))
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
     (:module "commands"
      :serial t
      :components ((:file "vacuum")
                   (:file "analyze")))
     (:module "parser"
      :serial t
      :components ((:file "scan")
                   (:file "gram")
                   (:file "analyze")))
     (:module "executor"
      :serial t
      :components ((:file "exec_arena")
                   (:file "exec_spill")
                   (:file "exec_scan")
                   (:file "exec_plan")
                   (:file "exec_analyze")
                   (:file "exec_insert")
                   (:file "exec_delete")
                   (:file "exec_vacuum")
                   (:file "exec_simple")))
     (:module "tcop"
      :serial t
      :components ((:file "dest")
                   (:file "control")
                   (:file "postgres")))))))

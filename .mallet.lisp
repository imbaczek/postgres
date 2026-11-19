(:mallet-config
 (:extends :default)
 (:ignore "**/.mallet.lisp" "src/lisp/vendor/**")

 ;; Sequential bindings are idiomatic here, and the reader alone cannot
 ;; determine whether a cleanup or fallback intentionally uses IGNORE-ERRORS.
 (:disable :needless-let*)
 (:disable :no-ignore-errors)

 ;; These tests call local functions through project macros.
 (:for-paths ("src/lisp/t/rove/primitives-tests.lisp"
              "src/lisp/t/rove/storage-tests.lisp"
              "src/lisp/t/unit-tests/concurrency-tests.lisp"
              "src/lisp/t/unit-tests/vacuum-tests.lisp")
   (:enable :unused-local-functions :severity :info))

 ;; Observation macros and dynamically bound test settings hide references.
 (:for-paths ("src/lisp/backend/replication/logical-slot-continuation-*.lisp"
              "src/lisp/t/interop/protocol-tests.lisp"
              "src/lisp/t/unit-tests/spill-tests.lisp")
   (:enable :unused-variables :severity :info))

 ;; The control REPL and CRC portability test deliberately evaluate forms.
 (:for-paths ("src/lisp/backend/tcop/control.lisp"
              "src/lisp/t/crc32-portability.lisp")
   (:enable :no-eval :severity :info)))

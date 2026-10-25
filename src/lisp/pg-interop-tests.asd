(asdf:defsystem "pg-interop-tests"
  :description "Independent PostgreSQL logical replication wire contracts"
  :depends-on ("pg" "rove")
  :serial t
  :components ((:file "t/interop/protocol-tests")
               (:file "t/interop/standard-tests")
               (:file "t/interop/copy-tests")
               (:file "t/interop/standard-subscriber-tests")
               (:file "t/interop/publication-history-tests"))
  :perform (asdf:test-op (operation component)
                         (declare (ignore operation component))
                         (unless (uiop:symbol-call :rove :run :pg-interop-tests)
                           (error "Logical interoperability contracts failed"))))

(asdf:defsystem "pg-interop-tests/apply"
  :description "C frame replay against an isolated running pg-lisp target"
  :depends-on ("pg-interop-tests")
  :serial t
  :components ((:file "t/interop/apply-tests")
               (:file "t/interop/standard-wire-tests"))
  :perform (asdf:test-op (operation component)
                         (declare (ignore operation component))
                         (unless (uiop:symbol-call :rove :run :pg-interop-tests/apply)
                           (error "Logical apply interoperability tests failed"))))

(asdf:defsystem "pg-interop-tests/native"
  :description "Unmodified C PostgreSQL subscriber stream checkpoint"
  :depends-on ("pg-interop-tests")
  :serial t
  :components ((:file "t/interop/native-stream-tests"))
  :perform (asdf:test-op (operation component)
                         (declare (ignore operation component))
                         (unless (uiop:symbol-call :rove :run :pg-interop-tests/native)
                           (error "Native stream interoperability tests failed"))))

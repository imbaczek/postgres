(asdf:defsystem "pg-physical-tests"
  :description "Native physical WAL intake and recovery contracts"
  :depends-on ("pg" "rove" "cl-spec/check-it")
  :serial t
  :components ((:file "t/physical-tests")
               (:file "t/physical-store-tests")
               (:file "t/physical-source-tests"))
  :perform (asdf:test-op (operation component)
                         (declare (ignore operation component))
                         (unless (uiop:symbol-call :rove :run :pg-physical-tests)
                           (error "Physical recovery contracts failed"))))

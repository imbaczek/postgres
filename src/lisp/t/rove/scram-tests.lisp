(in-package #:pg)

(rove:deftest scram-keys-and-proof-match-independent-vector
    (let ((message (string->bytes
                    (concatenate 'string
                                 "n=postgres,r=clientnonce,r=clientnonceservernonce,"
                                 "s=Zml4ZWQtdGVzdC1zYWx0IQ==,i=4096,c=biws,r=clientnonceservernonce")))
          (proof (sql-bytea-input "\\x48300f65bf2fd8a95e0e52818ba671974b13168b70aa42c2c3199cb9b3193067")))
      (multiple-value-bind (stored server)
          (scram-derive-keys (string->bytes "pencil") (string->bytes "fixed-test-salt!") 4096)
        (rove:ok (equalp stored (sql-bytea-input "\\x409b67ab9c44b0e38a3d0ce5a3340d48e3b0070118af6b2a78d2205608926622")))
        (rove:ok (equalp server (sql-bytea-input "\\xdb9acf25693c9794440235643c5c6f5eaea2e7f0e31b35f368ffca6c00f1dcc4")))
        (rove:ok (equalp (scram-verify-client-proof stored server message proof)
                         (sql-bytea-input "\\xabf262ce7ad5bc72ce5fb01d4545b160b6be38a71fbdc49289b6ec2b3e4d437b")))
        (rove:ok (null (scram-verify-client-proof stored server message (subseq proof 1))))
        (setf (aref proof 0) (logxor (aref proof 0) 1))
        (rove:ok (null (scram-verify-client-proof stored server message proof))))))

(rove:deftest role-scram-verifiers-use-random-salt-and-postgresql-password-normalization
    (let* ((password (format nil "I~cX" (code-char #xad)))
           (verifier (scram-build-role-verifier password))
           (second-verifier (scram-build-role-verifier password)))
      (destructuring-bind (salt iterations stored server) verifier
        (rove:ok (not (equalp salt (first second-verifier))))
        (multiple-value-bind (expected-stored expected-server)
            (scram-derive-keys (string->bytes "IX") salt iterations)
          (rove:ok (equalp stored expected-stored))
          (rove:ok (equalp server expected-server)))))
  (rove:ok (null (scram-build-role-verifier "")))
  (rove:ok (rove:signals (scram-build-role-verifier (string (code-char 0))) 'pg-error)))

(rove:deftest scram-wire-rejects-invalid-input
    (rove:ok (null (scram-decode-proof (make-string 44 :initial-element (code-char 233)))))
  (multiple-value-bind (bare nonce binding) (scram-parse-client-first "y,,n=,r=test")
    (rove:ok (equal (list bare nonce binding) '("n=,r=test" "test" "eSws"))))
  (rove:ok (rove:signals (scram-parse-client-first "n,,n=,r=") 'pg-error))
  (rove:ok (rove:signals (scram-parse-client-first "n,,n=,r=test,xyz=bad") 'pg-error))
  (let ((proof (scram-encode64 (scram-key-buffer))))
    (multiple-value-bind (bare decoded)
        (scram-parse-client-final (concatenate 'string "c=biws,r=test,x=value,p=" proof)
                                  "biws" "test")
      (rove:ok (string= bare "c=biws,r=test,x=value"))
      (rove:ok (equalp decoded (scram-key-buffer))))
    (rove:ok (rove:signals (scram-parse-client-final
                            (concatenate 'string "c=biws,r=test,p=" proof ",p=" proof)
                            "biws" "test")
                           'pg-error)))
  (rove:ok (rove:signals (scram-parse-client-final "c=biws,r=wrong,p=bad" "biws" "expected")
                         'pg-error)))

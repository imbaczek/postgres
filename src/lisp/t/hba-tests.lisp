(in-package #:pg-physical-tests)

(defun hba-test-token-view (line)
  (mapcar (lambda (field)
            (mapcar (lambda (token)
                      (list (pg::hba-token-text token) (pg::hba-token-quoted-p token)))
                    field))
          (pg::hba-tokenize-line line)))

(deftest hba-tokenization-preserves-fields-quotes-and-c-comments
    (ok (equal '(("host") ("all") ("postgres") ("127.0.0.1/32") ("trust"))
               (mapcar (lambda (field) (mapcar #'first field))
                       (hba-test-token-view "host all postgres 127.0.0.1/32 trust # ignored"))))
  (ok (equal '((("host" nil)) (("db1" nil) ("db 2" t))
               (("alice" nil) ("A,B" t)) (("all" nil)) (("reject" nil)))
             (hba-test-token-view "host db1, \"db 2\" alice,\"A,B\" all reject")))
  (ok (equal '((("all" t)) (("a\"b,# c" t)))
             (hba-test-token-view "\"all\" \"a\"\"b,# c\"")))
  (ok (equal '((("unfinished" t)))
             (hba-test-token-view "\"unfinished")))
  (ok (null (pg::hba-tokenize-line "  # comment only"))))

(cl-spec:defproperty hba-quoted-token-roundtrip
    ((number (range integer 0 100000)))
  (:about pg::hba-tokenize-line)
  (:trials (:normal 200))
  (let* ((text (format nil "name ~d,#\"suffix" number))
         (quoted (with-output-to-string (out)
                   (write-char #\" out)
                   (loop for character across text
                         do (write-char character out)
                            (when (char= character #\") (write-char character out)))
                   (write-char #\" out))))
    (equal (list (list (list text t))) (hba-test-token-view quoted))))

(deftest generated-hba-quoted-token-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'hba-quoted-token-roundtrip :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest hba-tokenization-corner-cases-and-input-boundary
    (ok (equal '((("" t))) (hba-test-token-view "\"\"")))
  (ok (equal '((("a" nil) ("b" nil))) (hba-test-token-view ",,a,,b,")))
  (ok (equal '((("a" nil)) (("b" nil))) (hba-test-token-view "a , b")))
  (ok (equal '((("all" nil)) (("all" t))) (hba-test-token-view "a\"\"ll \"\"all")))
  (ok (signals (pg::hba-tokenize-line (format nil "a~%b")) 'error))
  (ok (signals (pg::hba-tokenize-line (string (code-char 0))) 'error)))

(defun hba-test-decision (text user database address &key replication ssl-p)
  (pg::hba-match-rules (pg::hba-parse-text text)
                       user database (pg::hba-parse-ip-address address)
                       :replication replication :ssl-p ssl-p))

(deftest hba-rule-order-and-startup-replication-mode-match-c
    (let ((rules (format nil "host all alice 127.0.0.0/8 reject~%host all all 127.0.0.0/8 trust~%host replication repl 127.0.0.0/8 trust~%")))
      (ok (eq :reject (hba-test-decision rules "alice" "postgres" "127.0.0.1")))
      (ok (eq :trust (hba-test-decision rules "bob" "postgres" "127.255.255.255")))
      (ok (eq :implicit-reject (hba-test-decision rules "bob" "postgres" "128.0.0.1")))
      (ok (eq :implicit-reject
              (hba-test-decision rules "bob" "postgres" "127.0.0.1" :replication :physical)))
      (ok (eq :trust
              (hba-test-decision rules "repl" "ignored" "127.0.0.1" :replication :physical)))
      (ok (eq :trust
              (hba-test-decision rules "bob" "postgres" "127.0.0.1" :replication :database)))))

(deftest hba-quoted-selectors-lists-and-plaintext-transport
    (let ((rules (format nil "hostssl all all all reject~%hostnossl \"all\",sameuser alice,\"A,B\" all trust~%host \"replication\" all all trust~%")))
      (ok (eq :trust (hba-test-decision rules "alice" "all" "127.0.0.1")))
      (ok (eq :trust (hba-test-decision rules "alice" "alice" "127.0.0.1")))
      (ok (eq :implicit-reject (hba-test-decision rules "alice" "postgres" "127.0.0.1")))
      (ok (eq :trust (hba-test-decision rules "A,B" "all" "127.0.0.1")))
      (ok (eq :implicit-reject (hba-test-decision rules "ALICE" "all" "127.0.0.1")))
      (ok (eq :trust (hba-test-decision rules "bob" "replication" "127.0.0.1")))
      (ok (eq :implicit-reject
              (hba-test-decision rules "bob" "replication" "127.0.0.1" :replication :physical)))
      (ok (eq :reject (hba-test-decision rules "alice" "all" "::1" :ssl-p t)))))

(deftest hba-numeric-networks-and-continuations
    (let ((rules (format nil "host all all 192.0.2.129 255.255.255.128 trust~%host all all 2001:db8::/32 reject~%")))
      (ok (eq :implicit-reject (hba-test-decision rules "u" "d" "192.0.2.127")))
      (ok (eq :trust (hba-test-decision rules "u" "d" "192.0.2.128")))
      (ok (eq :trust (hba-test-decision rules "u" "d" "192.0.2.255")))
      (ok (eq :reject (hba-test-decision rules "u" "d" "2001:db8:ffff::1")))
      (ok (eq :implicit-reject (hba-test-decision rules "u" "d" "2001:db9::1")))
      (ok (eq :implicit-reject (hba-test-decision rules "u" "d" "::ffff:192.0.2.128"))))
  (ok (eq :trust
          (hba-test-decision (format nil "host same\\~%user alice 127.0.0.1/32 trust")
                             "alice" "alice" "127.0.0.1"))))

(deftest hba-unsupported-or-empty-policy-is-not-silently-accepted
    (dolist (text '("" "# no rules" "host all all all md5"
                    "host all all all trust map=anything" "host all +group all trust"
                    "host samerole all all trust" "host all all localhost trust"
                    "host all all samehost trust" "host all all 127.0.0.1/33 trust"
                    "host all all ::1/129 trust" "host all all 127.0.0.1 ::1 trust"
                    "host /regex all all trust" "host \"/regex\" all all trust"
                    "host @included all all trust" "include other.conf"))
      (ok (signals (pg::hba-parse-text text) 'error))))

(deftest hba-plus-database-and-quoted-role-are-literal-names
    (ok (eq :trust
            (hba-test-decision "host +db \"+role\" all trust"
                               "+role" "+db" "127.0.0.1")))
  (ok (eq :implicit-reject
          (hba-test-decision "host +db \"+role\" all trust"
                             "role" "+db" "127.0.0.1"))))

(deftest hba-c-numeric-address-compatibility
    (dolist (text '("127.0.0.1" "127.1" "0177.0.0.1" "0x7f000001" "2130706433"))
      (ok (equalp #(127 0 0 1) (pg::hba-parse-ip-address text))))
  (dolist (text '("" "127.0.0.1 " "127.0.0.1 x" "08.0.0.1"
                  "256.0.0.1" "4294967296" "127..1" "+127.1"))
    (ok (signals (pg::hba-parse-ip-address text))))
  (dolist (prefix '("+24" "00024" " 24" "-0"))
    (ok (pg::hba-parse-text
         (format nil "host all all ~s trust" (concatenate 'string "127.1/" prefix)))))
  (dolist (prefix '("" "+" "24 " "0x18" "33" "-1"))
    (ok (signals
         (pg::hba-parse-text
          (format nil "host all all ~s trust" (concatenate 'string "127.1/" prefix)))))))

(deftest hba-bare-at-is-a-literal-selector
    (ok (eq :trust (hba-test-decision "host @ @ all trust" "@" "@" "127.1")))
  (ok (eq :implicit-reject
          (hba-test-decision "host @ @ all trust" "other" "@" "127.1"))))

(deftest hba-continuation-errors-report-source-and-start-line
    (dolist (text (list (format nil "# comment~%host all all \\~%all md5")
                        (format nil "# comment~%host all all \\~%~a"
                                (make-string 65537 :initial-element #\a))))
      (let ((message (handler-case
                         (progn (pg::hba-parse-text text :source "policy.conf") nil)
                       (error (condition) (princ-to-string condition)))))
        (ok (and message (search "policy.conf:2:" message))))))

(deftest hba-tcp-policy-requires-a-valid-peer-address
    (let ((rules (pg::hba-parse-text "host all all all trust")))
      (dolist (peer (list nil #() #(127 0 0) #(127 0 0 256) #(127 0 0 -1)
                          #(127 0 0 nil) '(127 0 0 1) "abcd"))
        (ok (signals (pg::hba-match-rules rules "u" "d" peer))))
      (ok (eq :trust (pg::hba-match-rules rules "u" "d" #(127 0 0 1)))))
  (ok (eq :trust
          (pg::hba-match-rules (pg::hba-parse-text "local all all trust")
                               "u" "d" nil :transport :local))))

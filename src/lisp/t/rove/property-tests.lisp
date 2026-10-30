;;;; Generated checks extend fixed regressions; they do not replace wire/byte oracles.
(in-package #:pg)

(defun unit-random-octets ()
  "Make fresh bounded bytes using cl-spec's dynamically seeded random state."
  (let ((size (random 65)))
    (make-array size :element-type '(unsigned-byte 8)
                     :initial-contents (loop repeat size collect (random 256)))))

(cl-spec:defgenerator unit-octets ()
  (unit-random-octets))

(cl-spec:defspec unit-octet-vector
    (type (simple-array (unsigned-byte 8) (*)))
  (:generator unit-octets))

(cl-spec:defgenerator unit-key-arguments ()
  ;; Include independent pairs as well as equality and prefix/last-byte cases.
  ;; Fresh arrays per trial keep generated arguments independent of prior calls.
  (let* ((prefix (unit-random-octets))
         (extended
           (concatenate '(vector (unsigned-byte 8)) prefix
                        (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0)))
         (high (copy-seq extended)))
    (setf (aref high (length prefix)) 255)
    (ecase (random 6)
      (0 (list prefix (unit-random-octets)))
      (1 (list prefix (copy-seq prefix)))
      (2 (list prefix extended))
      (3 (list extended prefix))
      (4 (list extended high))
      (5 (list high extended)))))

;; CL string ordering is an independent lexicographic oracle over byte characters.
(cl-spec:defspec-function key-compare
    (:args (a unit-octet-vector) (b unit-octet-vector))
  (:args-generator unit-key-arguments)
  (:returns (member -1 0 1))
  (:post
   (= result
      (let ((left (map 'string #'code-char a))
            (right (map 'string #'code-char b)))
        (cond ((string= left right) 0)
              ((string< left right) -1)
              (t 1))))))

(cl-spec:defproperty unit-int4-index-order
    ((a (range integer -2147483648 2147483647))
     (b (range integer -2147483648 2147483647)))
  (:about encode-index-key key-compare)
  (:trials (:normal 200))
  (= (signum (- a b))
     (key-compare (encode-index-key (list a) '(:int4))
                  (encode-index-key (list b) '(:int4)))))

(cl-spec:defproperty unit-int8-index-order
    ((a (range integer -9223372036854775808 9223372036854775807))
     (b (range integer -9223372036854775808 9223372036854775807)))
  (:about encode-index-key key-compare)
  (:trials (:normal 200))
  (= (signum (- a b))
     (key-compare (encode-index-key (list a) '(:int8))
                  (encode-index-key (list b) '(:int8)))))

(cl-spec:defgenerator unit-short-text ()
  ;; At most 120 UTF8 bytes, inside the tuple codec's 126-byte short-varlena limit.
  (let ((alphabet #(#\a #\ą #\€ #\😀)))
    (coerce (loop repeat (random 31)
                  collect (aref alphabet (random (length alphabet))))
            'string)))

(cl-spec:defspec unit-tuple-text (type string) (:generator unit-short-text))

(cl-spec:defproperty unit-tuples-preserve-values-and-header
    ((a (range integer -2147483648 2147483647))
     (b (range integer -9223372036854775808 9223372036854775807))
     (text unit-tuple-text)
     (null-mask (range integer 0 7))
     (xmin (range integer 3 18446744073709551615))
     (xmax (range integer 0 18446744073709551615))
     (cid (range integer 0 4294967295))
     (block (range integer 0 4294967295))
     (offnum (range integer 0 65535)))
  (:about form-heap-tuple deform-heap-tuple)
  (:trials (:normal 200))
  (let* ((columns
           (vector (make-pg-column :name "a" :type :int4 :typmod -1 :attnum 1)
                   (make-pg-column :name "b" :type :varchar :typmod -1 :attnum 2)
                   (make-pg-column :name "c" :type :int8 :typmod -1 :attnum 3)))
         (expected (vector a text b)))
    (dotimes (index 3)
      (when (logbitp index null-mask) (setf (aref expected index) +null+)))
    (multiple-value-bind (actual got-xmin got-xmax got-cid infomask got-block got-offnum)
        (deform-heap-tuple
            (form-heap-tuple expected columns xmin xmax cid block offnum) columns)
      (and (equalp actual expected)
           (= got-xmin (mod xmin 4294967296))
           (= got-xmax (mod xmax 4294967296))
           (= got-cid cid)
           (= got-block block)
           (= got-offnum offnum)
           (eql (logtest infomask +heap-hasnull+) (not (zerop null-mask)))
           (logtest infomask +heap-hasvarwidth+)))))

(cl-spec:defproperty unit-wal-records-preserve-fields
    ((payload unit-octet-vector)
     (xid (range integer 0 18446744073709551615))
     (start (range integer 0 18446744073709551519)))
  (:about wal-encode-record wal-decode-record)
  (:trials (:normal 200))
  ;; This is the raw record envelope contract, not semantic replay of a commit.
  (let* ((bytes (wal-encode-record +wal-record-commit+ xid start payload))
         (decoded (wal-decode-record bytes start)))
    (and (= (wal-record-version decoded) +wal-version+)
         (= (wal-record-type decoded) +wal-record-commit+)
         (= (wal-record-xid decoded) xid)
         (= (wal-record-start decoded) start)
         (= (wal-record-end decoded) (+ start 32 (length payload)))
         (= (length bytes) (+ 32 (length payload)))
         (equalp payload (wal-record-payload decoded)))))

(cl-spec:defproperty unit-page-compaction-preserves-live-items
    ((count (range integer 1 64)) (size (range integer 1 48))
                                  (dead-mask (range integer 0 255)))
  (:about page-add-item page-compact)
  (:trials (:normal 200))
  (let ((page (page-init (make-page)))
        (expected nil))
    (dotimes (index count)
      (let ((payload (make-array size :element-type '(unsigned-byte 8)
                                      :initial-element index)))
        (page-add-item page payload)
        (if (logbitp (mod index 8) dead-mask)
            (page-mark-dead page (1+ index))
            (push (cons (1+ index) payload) expected))))
    (let ((free-before (page-free-space page)))
      (page-compact page)
      (and (= count (page-lp-count page))
           (>= (page-free-space page) free-before)
           (every (lambda (entry)
                    (multiple-value-bind (offset flags length)
                        (page-get-lp page (car entry))
                      (and (= flags +lp-normal+)
                           (= length size)
                           (equalp (cdr entry) (subseq page offset (+ offset length))))))
                  expected)))))

(cl-spec:defproperty unit-freeze-cutoffs-respect-policy-and-horizon
    ((next (range integer 600000000 1000000000000))
     (lag (range integer 0 500000000))
     (band (range integer 0 5))
     (forced (range integer 3 1000000000000)))
  (:about vacuum-compute-freeze-cutoff vacuum-full-freeze-cutoff)
  (:trials (:normal 200))
  ;; Sample both sides of the aggressive threshold explicitly, as well as ages 0/1.
  (let* ((age (ecase band
                (0 0) (1 1)
                (2 (1- +vacuum-aggressive-freeze-age+))
                (3 +vacuum-aggressive-freeze-age+)
                (4 (1+ +vacuum-aggressive-freeze-age+))
                (5 500000000)))
         (old (- next age))
         (horizon (- next lag))
         (table (make-pg-class :relfrozenxid old))
         (actual (vacuum-compute-freeze-cutoff old horizon next))
         (minimum-age-target (- next +vacuum-freeze-min-age+)))
    (and (<= actual horizon)
         (if (>= age +vacuum-aggressive-freeze-age+)
             (= actual horizon)
             (= actual (min horizon minimum-age-target)))
         (= actual (vacuum-full-freeze-cutoff table horizon next))
         (= (vacuum-compute-freeze-cutoff old horizon next :forced-freeze-cutoff forced)
            (min horizon forced)))))

(cl-spec:defproperty unit-logical-row-images-roundtrip
    ((id (range integer -2147483648 2147483647))
     (amount (range integer -9223372036854775808 9223372036854775807))
     (text unit-tuple-text)
     (null-mask (range integer 0 3)))
  (:about logical-encode-change logical-decode-change)
  (:trials (:normal 200))
  (let ((old-row (vector id amount text))
        (new-row (vector id
                         (if (logbitp 0 null-mask) +null+ amount)
                         (if (logbitp 1 null-mask) +null+ text))))
    (every
     (lambda (event)
       (let ((decoded (logical-decode-change (logical-encode-change event))))
         (and (eq (logical-change-kind event) (logical-change-kind decoded))
              (= (logical-change-database-oid decoded) 7)
              (= (logical-change-relation-oid decoded) 701)
              (equal (logical-change-relation-name event) (logical-change-relation-name decoded))
              (equal (logical-change-columns event) (logical-change-columns decoded))
              (equal (logical-change-key-attnums event) (logical-change-key-attnums decoded))
              (equalp (logical-change-old-values event) (logical-change-old-values decoded))
              (equalp (logical-change-new-values event) (logical-change-new-values decoded)))))
     (list (logical-test-change :insert 7 701 :new-values new-row)
           (logical-test-change :update 7 701 :old-values old-row :new-values new-row)
           (logical-test-change :delete 7 701 :old-values old-row)))))

(cl-spec:defproperty unit-top-n-matches-stable-sort
    ((bytes unit-octet-vector) (limit (range integer 0 64))
                               (descending (range integer 0 1)))
  (:about make-select-top-n-collector offer-select-top-n-row select-top-n-collector-rows)
  (:trials (:normal 200))
  (let* ((entries (loop for byte across bytes for index from 0
                        collect (cons (mod byte 8) index)))
         (collector (make-select-top-n-collector
                     limit (list (cons '(:col 1) (= descending 1)))))
         (bounded t))
    (dolist (entry entries)
      (offer-select-top-n-row collector (cons (vector (cdr entry)) (vector (car entry))))
      (unless (<= (length (select-top-n-collector-heap collector)) limit)
        (setf bounded nil)))
    (let* ((sorted (stable-sort (copy-list entries)
                                (if (= descending 1) #'> #'<) :key #'car))
           (expected (mapcar #'cdr (subseq sorted 0 (min limit (length sorted)))))
           (actual (mapcar (lambda (row) (aref (car row) 0))
                           (select-top-n-collector-rows collector))))
      (and bounded (equal actual expected)))))

(defun unit-check-property (name)
  "Run a registered property and retain its seed/counterexample in failure output."
  (let* ((data (cl-spec:result-data (cl-spec:run-property name :seed 42)))
         (passed (eq :passed (getf data :status))))
    (rove:ok passed (if passed (format nil "~s passed" name) (format nil "~s" data)))))

(rove:deftest generated-unit-key-comparison
    (let* ((data (cl-spec:result-data (cl-spec:check-function 'key-compare :trials 200 :seed 42)))
           (passed (eq :passed (getf data :status))))
      (rove:ok passed (if passed "key-compare Function Spec passed" (format nil "~s" data))))
  (unit-check-property 'unit-int4-index-order)
  (unit-check-property 'unit-int8-index-order))

(rove:deftest generated-unit-tuple-codec
    (unit-check-property 'unit-tuples-preserve-values-and-header))

(rove:deftest generated-unit-wal-codec
    (unit-check-property 'unit-wal-records-preserve-fields))

(rove:deftest generated-unit-page-compaction
    (unit-check-property 'unit-page-compaction-preserves-live-items))

(rove:deftest generated-unit-freeze-cutoffs
    (unit-check-property 'unit-freeze-cutoffs-respect-policy-and-horizon))

(rove:deftest generated-unit-logical-codec
    (unit-check-property 'unit-logical-row-images-roundtrip))

(rove:deftest generated-unit-top-n
    (unit-check-property 'unit-top-n-matches-stable-sort))

(rove:deftest unit-comparison-boundaries
    (dolist (type '(:int4 :int8))
      (let* ((minimum (if (eq type :int4) -2147483648 -9223372036854775808))
             (maximum (if (eq type :int4) 2147483647 9223372036854775807))
             (values (list minimum (1+ minimum) -1 0 1 (1- maximum) maximum)))
        (dolist (a values)
          (dolist (b values)
            (rove:ok (= (signum (- a b))
                        (key-compare (encode-index-key (list a) (list type))
                                     (encode-index-key (list b) (list type))))))))))

(rove:deftest unit-freeze-cutoff-special-boundaries
    (rove:ok (= +first-normal-xid+ (vacuum-compute-freeze-cutoff nil 10 10))
             "low-XID freeze targets clamp to the first normal XID")
  (rove:ok (= +first-normal-xid+ (vacuum-compute-freeze-cutoff 20 10 10))
           "future relfrozenxid stays on the normal-age policy")
  (let* ((next 300000000)
         (target (- next +vacuum-freeze-min-age+)))
    (rove:ok (= target (vacuum-compute-freeze-cutoff next target next))
             "a horizon exactly at the minimum-age target is inclusive")))

(cl-spec:defproperty unit-toast-chunks-roundtrip
    ((payload unit-octet-vector)
     (chunk-size (range integer 1 24)))
  (:about toast-assemble-chunks)
  (:trials (:normal 200))
  (let ((chunks
          (loop for start from 0 below (length payload) by chunk-size
                for sequence from 0
                collect (cons sequence (subseq payload start (min (length payload)
                                                                  (+ start chunk-size)))))))
    (equalp payload (toast-assemble-chunks chunks (length payload) chunk-size))))

(rove:deftest generated-unit-toast-chunks
    (unit-check-property 'unit-toast-chunks-roundtrip))

(cl-spec:defproperty unit-toast-planner-preserves-policy-and-payload
    ((size-a (range integer 0 7000))
     (size-b (range integer 0 7000))
     (policy-a (range integer 0 3))
     (policy-b (range integer 0 3))
     (seed (range integer 0 65535))
     (external (member nil t)))
  (:about toast-plan-tuple heap-tuple-layout pglz-decompress)
  (:trials (:normal 200))
  (let* ((policies #(#\p #\e #\x #\m))
         (columns (vector (make-pg-column :type :bytea :storage (aref policies policy-a))
                          (make-pg-column :type :bytea :storage (aref policies policy-b))))
         (a (make-array size-a :element-type '(unsigned-byte 8)))
         (b (make-array size-b :element-type '(unsigned-byte 8)))
         (state seed))
    (dotimes (i size-a)
      (setf (aref a i) (mod (+ seed (mod i 4)) 256)))
    (dotimes (i size-b)
      (setf state (logand #xffffffff (+ (* state 1664525) 1013904223))
            (aref b i) (ldb (byte 8 24) state)))
    (let ((values (vector a b)) (before-a (copy-seq a)) (before-b (copy-seq b)))
      (multiple-value-bind (physical selected compressed-selected)
          (toast-plan-tuple values columns :external-p external)
        (and (equalp a before-a) (equalp b before-b)
             (= (length selected) (length (remove-duplicates selected)))
             (= (length compressed-selected) (length (remove-duplicates compressed-selected)))
             (every (lambda (i) (and (integerp i) (<= 0 i 1))) selected)
             (every (lambda (i) (member i selected)) compressed-selected)
             (or (not external)
                 (some (lambda (column) (char= (pg-column-effective-storage column) #\p)) columns)
                 (<= (heap-tuple-layout physical columns)
                     (if (every (lambda (column) (find (pg-column-effective-storage column) "xe")) columns)
                         +toast-tuple-target+ +toast-tuple-target-main+)))
             (loop for i below 2
                   for input = (aref values i) for value = (aref physical i)
                   for policy = (pg-column-effective-storage (aref columns i))
                   always
                   (cond ((heap-external-varlena-p value)
                          (and external (member i selected) (not (char= policy #\p))
                               (or (not (member i compressed-selected)) (find policy "xm"))))
                         ((heap-compressed-varlena-p value)
                          (let* ((payload (heap-compressed-varlena-payload value))
                                 (raw-size (page-u32 payload 0)))
                            (and (not (member i selected)) (find policy "xm")
                                 (= raw-size (length input))
                                 (equalp input (pglz-decompress payload raw-size :start 4)))))
                         (t (and (not (member i selected)) (equalp input value))))))))))

(rove:deftest generated-unit-toast-planner-policy-and-payload
    (unit-check-property 'unit-toast-planner-preserves-policy-and-payload))

(cl-spec:defproperty unit-toast-oid-preserves-value
    ((value (range integer 0 4294967295)))
  (:about form-heap-tuple deform-heap-tuple encode-index-key)
  (:trials (:normal 200))
  (let* ((columns (vector (make-pg-column :name "id" :type :oid :typmod -1 :attnum 1)))
         (bytes (form-heap-tuple (vector value) columns 3 0 0 0 0)))
    (= value (aref (deform-heap-tuple bytes columns) 0))))

(rove:deftest generated-unit-toast-oid
    (unit-check-property 'unit-toast-oid-preserves-value))

(cl-spec:defproperty unit-bytea-tuple-roundtrip
    ((payload unit-octet-vector))
  (:about form-heap-tuple deform-heap-tuple)
  (:trials (:normal 200))
  (let* ((columns (vector (make-pg-column :name "a" :type :int2 :typmod -1 :attnum 1)
                          (make-pg-column :name "b" :type :bytea :typmod -1 :attnum 2)
                          (make-pg-column :name "c" :type :int4 :typmod -1 :attnum 3)))
         (values (vector 7 payload 42))
         (bytes (form-heap-tuple values columns 3 0 0 0 0)))
    (equalp values (deform-heap-tuple bytes columns))))

(rove:deftest generated-unit-bytea-tuple
    (unit-check-property 'unit-bytea-tuple-roundtrip))

(cl-spec:defproperty unit-varlena-packing-boundaries
    ((n (range integer 0 2048)))
  (:about form-heap-tuple deform-heap-tuple)
  (:trials (:normal 200))
  (let* ((columns (vector (make-pg-column :name "n" :type :int2 :typmod -1 :attnum 1)
                          (make-pg-column :name "s" :type :varchar :typmod -1 :attnum 2)))
         (value (make-string n :initial-element #\x))
         (bytes (form-heap-tuple (vector 7 value) columns 3 0 0 0 0))
         (start (+ (aref bytes +heap-tuple-hoff-off+) 2))
         (datum (if (<= n 126) start (align-up start 4))))
    (and (equalp (vector 7 value) (deform-heap-tuple bytes columns))
         (if (<= n 126)
             (= (aref bytes datum) (logior 1 (ash (1+ n) 1)))
             (and (loop for i from start below datum always (zerop (aref bytes i)))
                  (= (page-u32 bytes datum) (ash (+ n 4) 2)))))))

(rove:deftest generated-unit-varlena-packing
    (unit-check-property 'unit-varlena-packing-boundaries))

(rove:deftest unit-tuple-varchar-byte-boundaries
    (let ((columns (vector (make-pg-column :name "n" :type :int2 :typmod -1 :attnum 1)
                           (make-pg-column :name "text" :type :varchar :typmod -1 :attnum 2)
                           (make-pg-column :name "tail" :type :int4 :typmod -1 :attnum 3))))
      (dolist (text (list "" (make-string 126 :initial-element #\a)
                          (make-string 127 :initial-element #\a)
                          (make-string 64 :initial-element #\ą)
                          (make-string 1024 :initial-element #\z)))
        (let ((values (vector 5 text 42)))
          (rove:ok (equalp values
                           (deform-heap-tuple
                               (form-heap-tuple values columns 7 0 1 0 0) columns))
                   "short and four-byte varlena preserve UTF-8 and alignment")))))

(rove:deftest unit-tuple-rejects-invalid-varlena
    (let* ((columns (vector (make-pg-column :name "s" :type :varchar :typmod -1 :attnum 1)))
           (tuple (form-heap-tuple (vector "hello") columns 3 0 0 0 0))
           (start (aref tuple +heap-tuple-hoff-off+)))
      (dolist (header '(1 2 255))
        (let ((invalid (copy-seq tuple)))
          (setf (aref invalid start) header)
          (rove:ok (handler-case (progn (deform-heap-tuple invalid columns) nil)
                     (error () t))
                   "external, compressed and truncated datums must not decode as text")))))

(rove:deftest unit-inline-varlena-invalid-lengths
    (dolist (bytes (list #(0) #(0 0 0 0) #(8 0 0 0) #(40 0 0 0 1)))
      (rove:ok (handler-case
                   (progn (heap-inline-varlena-range bytes 0) nil)
                 (error () t))
               "truncated and invalid four-byte varlena lengths are rejected")))

(cl-spec:defproperty unit-external-varlena-roundtrip
    ((raw-size (range integer 4 1073741823))
     (external-info (range integer 0 4294967295))
     (value-id (range integer 0 4294967295))
     (relation-id (range integer 0 4294967295)))
  (:about heap-encode-external-varlena heap-decode-external-varlena)
  (:trials (:normal 200))
  (loop for width in '(4 8)
        for value = (make-heap-external-varlena :raw-size raw-size
                                                :external-info external-info
                                                :value-id value-id
                                                :relation-id relation-id
                                                :id-width width)
        for bytes = (heap-encode-external-varlena value)
        always (multiple-value-bind (decoded end)
                   (heap-decode-external-varlena bytes 0)
                 (and (= end (length bytes)) (equalp value decoded)))))

(rove:deftest generated-unit-external-varlena
    (unit-check-property 'unit-external-varlena-roundtrip))

(cl-spec:defproperty unit-toast-catalog-metadata-roundtrip
    ((oid (range integer 1 4294967295))
     (heap-node (range integer 1 4294967295))
     (index-oid (range integer 1 4294967295))
     (index-node (range integer 1 4294967295))
     (frozen-xid (range integer 3 4294967295)))
  (:about toast-to-sexp toast-from-sexp)
  (:trials (:normal 200))
  (let ((toast (make-pg-toast :oid oid :relfilenode heap-node
                              :index-oid index-oid :index-relfilenode index-node
                              :relfrozenxid frozen-xid)))
    (equalp toast (toast-from-sexp (toast-to-sexp toast)))))

(rove:deftest generated-unit-toast-catalog-metadata
    (unit-check-property 'unit-toast-catalog-metadata-roundtrip))

(rove:deftest unit-external-varlena-reference-layout
    (let* ((bytes (make-array 23 :element-type '(unsigned-byte 8) :initial-element 0))
           (oid8 (make-heap-external-varlena :raw-size 2048 :external-info 2000
                                             :value-id #x1122334455667788
                                             :relation-id #x12345678 :id-width 8)))
      (replace bytes (heap-encode-external-varlena oid8) :start1 1)
      (rove:ok (equalp (subseq bytes 1)
                       #(1 4 0 8 0 0 208 7 0 0 136 119 102 85 68 51 34 17 120 86 52 18))
               "OID8 external descriptor has PostgreSQL byte layout even unaligned")
      (multiple-value-bind (decoded end) (heap-decode-external-varlena bytes 1)
        (rove:ok (and (= end 23) (equalp oid8 decoded)) "unaligned OID8 descriptor decodes")))
  (let ((oid (make-heap-external-varlena :raw-size 5 :external-info 1 :value-id 9
                                         :relation-id 10 :id-width 4)))
    (rove:ok (equalp (heap-encode-external-varlena oid)
                     #(1 18 5 0 0 0 1 0 0 0 9 0 0 0 10 0 0 0))
             "OID external descriptor has PostgreSQL byte layout"))
  (dolist (bytes (list #(1) #(1 18) #(1 4 0 0 0 0) #(1 1) #(1 2) #(1 3) #(1 99) #(0 18)))
    (rove:ok (handler-case (progn (heap-decode-external-varlena bytes 0) nil)
               (error () t))
             "invalid external header is rejected")))

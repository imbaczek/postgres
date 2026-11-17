(in-package #:pg)

(cl-spec:defproperty committed-input-images-preserve-exact-values
    ((number (range integer 0 18446744073709551615))
     (letter (range integer 65 90)) (size (range integer 0 512)))
  (:about logical-slot-copy-committed-image-equal-p)
  (:trials (:normal 200))
  (let* ((value (list :number number :path (format nil "~c/Path" (code-char letter))
                      :bytes (make-array size :element-type '(unsigned-byte 8)
                                              :initial-element (mod number 256))
                      :history (make-timeline-history-entry :timeline 1 :begin 0 :end number)))
         (image (logical-slot-copy-import-state-image value)))
    (and (logical-slot-copy-committed-image-equal-p image value)
         (progn (setf (char (getf value :path) 0) (char-downcase (code-char letter)))
                (not (logical-slot-copy-committed-image-equal-p image value)))
         (progn (setf (char (getf value :path) 0) (code-char letter)
                      (getf value :number) (float number 1d0))
                (not (logical-slot-copy-committed-image-equal-p image value))))))

(rove:deftest committed-input-exact-images-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'committed-input-images-preserve-exact-values
                                       :seed 7522 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest committed-input-images-distinguish-types-case-and-shape
    (dolist (pair (list (list "Root" "root") (list 1 1d0) (list #\A #\a)
                        (list #(1 2) #(1 2 3)) (list '(:a 1) '(:a 1.0))
                        (list (make-array 2 :element-type '(unsigned-byte 8) :initial-element 0)
                              #(0 0))
                        (list (make-timeline-history-entry :timeline 1 :begin 0 :end 5)
                              (make-timeline-history-entry :timeline 1 :begin 0 :end 6))))
      (rove:ok (not (logical-slot-copy-committed-image-equal-p (first pair) (second pair))))
      (rove:ok (not (logical-slot-copy-committed-image-equal-p (second pair) (first pair)))))
  (rove:ok (logical-slot-copy-committed-image-equal-p
            (list "Same" #(1 2) '(3 . 4))
            (list (copy-seq "Same") (vector 1 2) '(3 . 4)))))

(defun unit-logical-import-selection-chain ()
  (let* ((store (%make-physical-store :system-id 42 :timeline 1 :base-lsn 64))
         (parent (timeline-encode-history 2 1 100 "first fork" nil))
         (selected (timeline-encode-history 3 2 200 "received future" parent)))
    (multiple-value-bind (first first-bytes)
        (physical-store-history-candidate
         store 2 (bytes->string parent) (wal-encode-end-of-recovery-record 100 0 2 1 2) 123)
      (setf (physical-store-selection store) first)
      (multiple-value-bind (second second-bytes)
          (physical-store-history-candidate
           store 3 (bytes->string selected) (wal-encode-end-of-recovery-record 200 0 3 2 2) 456)
        (declare (ignore second))
        (values (list first-bytes second-bytes) parent selected)))))

(defun unit-logical-import-evidence (fork salt)
  (let* ((state (list :phase 3 :applied fork :timeline 1 :promotion-child 2
                      :next-xid 50 :state-time salt
                      :source-reservation (list :version 1 :through fork :bound 1002 :complete t)))
         (plan (progn
                 (when (plusp (mod salt 3))
                   (setf (getf state :promotion-full-page-writes) (oddp salt)))
                 (when (= 2 (mod salt 3))
                   (setf (getf state :promotion-logical-decoding-enabled) (oddp salt)
                         (getf state :promotion-wal-level) (if (and (oddp salt) (logbitp 3 salt)) :logical :replica)))
                 (physical-promotion-checkpoint-plan state 20000 1003))))
    (list :manifest (make-array 3 :element-type '(unsigned-byte 8)
                                  :initial-contents (list 0 255 (mod salt 256)))
          :checkpoint (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 0 255)) :control (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(2 0 255))
          :selected-history (make-array 0 :element-type '(unsigned-byte 8))
          :child-history (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(255 0 3)) :plan plan)))

(cl-spec:defproperty logical-import-evidence-keeps-exact-bytes
    ((fork (range integer 256 50000000)) (salt (range integer 0 2147483647)))
  (:about logical-slot-copy-import-evidence-encode logical-slot-copy-import-evidence-decode)
  (:trials (:normal 200))
  (let* ((value (append (unit-logical-import-evidence fork salt)
                        (when (oddp salt)
                          (list :selection-chain (unit-logical-import-selection-chain)))))
         (image (logical-slot-copy-import-evidence-encode value))
         (before (copy-seq image))
         (decoded (logical-slot-copy-import-evidence-decode image)))
    (and (equalp value decoded)
         (equalp before image)
         (equalp image (logical-slot-copy-import-evidence-encode decoded))
         (not (eq (getf value :manifest) (getf decoded :manifest))))))

(rove:deftest logical-import-evidence-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-import-evidence-keeps-exact-bytes
                                       :seed 7516 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest logical-import-evidence-covers-policy-and-rotation-geometries
    (dolist (offset '(0 1 31 55 56 57 119 120 121 191 192 193))
      (dotimes (salt 6)
        (let* ((value (unit-logical-import-evidence (- +wal-segment-size+ offset) salt))
               (image (logical-slot-copy-import-evidence-encode value)))
          (rove:ok (equalp value (logical-slot-copy-import-evidence-decode image)))))))

(cl-spec:defproperty recovery-manifest-byte-decoder-preserves-envelope
    ((number (range integer 0 18446744073709551615)))
  (:about recovery-decode-manifest)
  (:trials (:normal 100))
  (let* ((value (list :number number :name "λ\\\"🙂"))
         (image (physical-bootstrap-manifest-image value)))
    (multiple-value-bind (decoded digest) (recovery-decode-manifest image)
      (and (equal value decoded) (= digest (physical-store-update-digest #xCBF29CE484222325 image))))))

(rove:deftest recovery-manifest-byte-decoder-generated-contract
    (unit-check-property 'recovery-manifest-byte-decoder-preserves-envelope))

(rove:deftest imported-startup-candidate-requires-canonical-complete-names
    (let* ((root (uiop:ensure-directory-pathname
                  (sb-posix:mkdtemp
                   (namestring (merge-pathnames "import-candidate-XXXXXX"
                                                (uiop:temporary-directory))))))
           (*pgdata* root)
           (names '("logical-slot-copy.source-proof" "logical-slot-copy.import"
                    "logical-slot-copy.install")))
      (labels ((put (name &optional directory-p)
                 (let ((path (merge-pathnames name root)))
                   (if directory-p
                       (sb-posix:mkdir (namestring path) #o700)
                       (with-open-file (out path :direction :output :if-exists :error)
                         (write-char #\x out)))))
               (remove-name (name &optional directory-p)
                 (if directory-p
                     (sb-posix:rmdir (namestring (merge-pathnames name root)))
                     (delete-file (merge-pathnames name root)))))
        (unwind-protect
             (progn
               (dotimes (mask 8)
                 (loop for name in names for bit from 0
                       when (logbitp bit mask) do (put name (= bit 2)))
                 (rove:ok (eql (= mask 7)
                               (logical-slot-copy-imported-startup-candidate-p)))
                 (loop for name in names for bit from 0
                       when (logbitp bit mask) do (remove-name name (= bit 2))))
               (dolist (name names) (put name (equal name "logical-slot-copy.install")))
               (dolist (name '("logical-slot-copy.import.tmp"
                               "logical-slot-copy.source-proof.tmp"
                               "logical-slot-copy.evidence.tmp"
                               "logical-slot-copy.base-verified.tmp"
                               "logical-slot-copy.in-progress"))
                 (put name)
                 (rove:ok (null (logical-slot-copy-imported-startup-candidate-p)))
                 (remove-name name))
               (put "logical-slot-copy.descriptor")
               (put "logical-slot-copy.base-verified")
               (put "logical-slot-copy.evidence")
               (put "logical_slot_wal" t)
               (rove:ok (logical-slot-copy-imported-startup-candidate-p))
               (remove-name "logical-slot-copy.source-proof")
               (sb-posix:symlink "logical-slot-copy.import"
                                 (namestring (merge-pathnames "logical-slot-copy.source-proof" root)))
               (rove:ok (null (logical-slot-copy-imported-startup-candidate-p)))
               (remove-name "logical-slot-copy.source-proof")
               (put "logical-slot-copy.source-proof" t)
               (rove:ok (null (logical-slot-copy-imported-startup-candidate-p))))
          (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore)))))

(rove:deftest imported-startup-candidate-rejects-wrong-kinds-and-dangling-markers
    (let* ((root (uiop:ensure-directory-pathname
                  (sb-posix:mkdtemp
                   (namestring (merge-pathnames "import-candidate-kinds-XXXXXX"
                                                (uiop:temporary-directory))))))
           (*pgdata* root)
           (names '("logical-slot-copy.source-proof" "logical-slot-copy.import"
                    "logical-slot-copy.install")))
      (labels ((put (name directory-p)
                 (let ((path (merge-pathnames name root)))
                   (if directory-p
                       (sb-posix:mkdir (namestring path) #o700)
                       (with-open-file (out path :direction :output :if-exists :error)
                         (write-char #\x out)))))
               (remove-name (name directory-p)
                 (if directory-p
                     (sb-posix:rmdir (namestring (merge-pathnames name root)))
                     (delete-file (merge-pathnames name root)))))
        (unwind-protect
             (progn
               (dolist (name names) (put name (equal name "logical-slot-copy.install")))
               (dolist (name names)
                 (let ((directory-p (equal name "logical-slot-copy.install")))
                   (remove-name name directory-p)
                   (put name (not directory-p))
                   (rove:ok (null (logical-slot-copy-imported-startup-candidate-p)))
                   (remove-name name (not directory-p))
                   (sb-posix:symlink "missing-candidate-target"
                                     (namestring (merge-pathnames name root)))
                   (rove:ok (null (logical-slot-copy-imported-startup-candidate-p)))
                   (delete-file (merge-pathnames name root))
                   (put name directory-p)))
               (dolist (name '("logical-slot-copy.import.tmp"
                               "logical-slot-copy.source-proof.tmp"
                               "logical-slot-copy.evidence.tmp"
                               "logical-slot-copy.base-verified.tmp"
                               "logical-slot-copy.in-progress"))
                 (sb-posix:symlink "missing-candidate-target"
                                   (namestring (merge-pathnames name root)))
                 (rove:ok (null (logical-slot-copy-imported-startup-candidate-p)))
                 (delete-file (merge-pathnames name root)))
               (rove:ok (logical-slot-copy-imported-startup-candidate-p)))
          (uiop:delete-directory-tree root :validate t :if-does-not-exist :ignore)))))

(rove:deftest imported-startup-recovery-profile-rejects-partial-trios
    (let* ((directory (uiop:ensure-directory-pathname
                       (sb-posix:mkdtemp
                        (namestring (merge-pathnames "import-recovery-profile-XXXXXX"
                                                     (uiop:temporary-directory))))))
           (root nil)
           (names '("physical-recovery.control" "physical-backup.manifest" "recovery.required")))
      (unwind-protect
           (progn
             (setf root (physical-backup-open-directory directory))
             (dotimes (mask 8)
               (loop for name in names for bit from 0 when (logbitp bit mask)
                     do (with-open-file (out (merge-pathnames name directory)
                                             :direction :output :if-exists :error)
                          (write-char #\x out)))
               (case mask
                 (0 (rove:ok (eq :retired (logical-slot-imported-startup-recovery-profile root))))
                 (7 (rove:ok (eq :original (logical-slot-imported-startup-recovery-profile root))))
                 (otherwise
                  (rove:ok (rove:signals (logical-slot-imported-startup-recovery-profile root) 'error))))
               (loop for name in names for bit from 0 when (logbitp bit mask)
                     do (delete-file (merge-pathnames name directory))))
             (dolist (name names)
               (with-open-file (out (merge-pathnames name directory) :direction :output :if-exists :error)
                 (write-char #\x out)))
             (dolist (name names)
               (let ((path (merge-pathnames name directory)))
                 (delete-file path)
                 (sb-posix:symlink "missing-recovery-input" (namestring path))
                 (rove:ok (rove:signals (logical-slot-imported-startup-recovery-profile root) 'error))
                 (delete-file path)
                 (sb-posix:mkdir (namestring path) #o700)
                 (rove:ok (rove:signals (logical-slot-imported-startup-recovery-profile root) 'error))
                 (sb-posix:rmdir (namestring path))
                 (with-open-file (out path :direction :output :if-exists :error)
                   (write-char #\x out)))))
        (when root (sb-posix:close root))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(cl-spec:defproperty imported-primary-follow-policy-keeps-metadata-inert
    ((unattached boolean) (enabled boolean) (preparing boolean)
                          (cleanup boolean) (source boolean) (public-slot boolean))
  (:about logical-slot-imported-startup-inert-follow-p)
  (:trials (:normal 100))
  (let* ((config (list 1 nil nil nil nil
                       (list :mode (when unattached :standby-unattached) :enabled enabled
                             :preparing preparing :cleanup cleanup :source source
                             :public-slot public-slot)))
         (before (logical-slot-copy-clone-facts config)))
    (and (eql (not (null (logical-slot-imported-startup-inert-follow-p config)))
              (and (or (not unattached) (not enabled))
                   (not preparing) (not cleanup) (not source) (not public-slot)))
         (equal before config))))

(rove:deftest imported-primary-follow-policy-generated-inertness
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'imported-primary-follow-policy-keeps-metadata-inert
                                       :seed 418 :profile :normal))))
      (rove:ok (eq :passed (getf data :status))))
  (rove:ok (not (logical-slot-imported-startup-inert-follow-p
                 '(1 "endpoint" "slot" nil))))
  (rove:ok (not (logical-slot-imported-startup-inert-follow-p
                 '(1 nil nil nil (:binding) (:enabled nil))))))

(cl-spec:defproperty imported-startup-child-binding-keeps-identity-and-floor
    ((same-id boolean) (delta (range integer -2 2)))
  (:about logical-slot-imported-startup-check-child-binding)
  (:trials (:normal 100))
  (let* ((original (timeline-encode-history 2 1 200 "binding test" nil))
         (facts (list :certificate
                      (list :system-id 42 :child-timeline 2 :parent-timeline 1
                            :fork 200 :child-end 400)
                      :evidence (list :child-history original)))
         (fields (list nil (+ 400 delta) nil nil nil nil
                       (list :system-id (if same-id 42 43))))
         (history (timeline-parse-history 2 (bytes->string original)))
         (before (logical-slot-copy-clone-facts (list facts fields)))
         (accepted (handler-case
                       (logical-slot-imported-startup-check-child-binding facts fields history)
                     (error () nil))))
    (and (eql (not (null accepted)) (and same-id (not (minusp delta))))
         (equalp before (list facts fields)))))

(rove:deftest imported-startup-child-binding-generated-identity-and-floor
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property
                  'imported-startup-child-binding-keeps-identity-and-floor
                  :seed 417 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)))))

(rove:deftest imported-startup-child-binding-preserves-descendant-origin
    (let* ((original (timeline-encode-history 2 1 200 "binding test" nil))
           (facts (list :certificate
                        (list :system-id 42 :child-timeline 2 :parent-timeline 1
                              :fork 200 :child-end 400)
                        :evidence (list :child-history original)))
           (fields (list nil 700 nil nil nil nil (list :system-id 42)))
           (descendant (timeline-encode-history 3 2 600 "later child" original)))
      (rove:ok (logical-slot-imported-startup-check-child-binding
                facts fields (timeline-parse-history 3 (bytes->string descendant))))
      (dolist (fork '(300 399 400 401))
        (let ((history (timeline-parse-history
                        3 (bytes->string (timeline-encode-history 3 2 fork "boundary child" original)))))
          (if (< fork 400)
              (rove:ok (rove:signals
                        (logical-slot-imported-startup-check-child-binding facts fields history)
                        'error))
              (rove:ok (logical-slot-imported-startup-check-child-binding facts fields history)))))
      (rove:ok
       (rove:signals
        (logical-slot-imported-startup-check-child-binding
         facts fields
         (timeline-parse-history
          3 (bytes->string (timeline-encode-history 3 1 600 "unrelated child" nil))))
        'error))))

(rove:deftest logical-import-artifacts-retain-startup-and-promotion-fences
    (let* ((base (uiop:ensure-directory-pathname
                  (sb-posix:mkdtemp
                   (namestring (merge-pathnames "import-artifact-XXXXXX" (uiop:temporary-directory))))))
           (*pgdata* base) (*physical-recovery-state* nil))
      (unwind-protect
           (dolist (name '("logical-slot-copy.import" "logical-slot-copy.import.tmp"
                           "logical-slot-copy.evidence" "logical-slot-copy.evidence.tmp"
                           "logical-slot-copy.source-proof" "logical-slot-copy.source-proof.tmp"))
             (let ((path (merge-pathnames name base)))
               (with-open-file (out path :direction :output :if-exists :error)
                 (write-char #\x out))
               (rove:ok (rove:signals (logical-slot-copy-preflight-startup) 'error))
               (rove:ok (rove:signals (logical-slot-copy-check-promotion) 'error))
               (delete-file path)))
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore)))
  (rove:ok (rove:signals (logical-slot-copy-import-file-spec :active) 'error)))

(rove:deftest logical-import-copy-id-alone-retains-promotion-fence
    (let* ((base (uiop:ensure-directory-pathname
                  (sb-posix:mkdtemp
                   (namestring (merge-pathnames "import-identity-XXXXXX"
                                                (uiop:temporary-directory))))))
           (*pgdata* base) (*physical-recovery-state* nil))
      (unwind-protect
           (progn
             (rove:ok (null (logical-slot-copy-check-promotion)))
             (dolist (id '(nil "0123456789abcdef0123456789abcdef"))
               (wal-atomic-write-bytes
                (merge-pathnames "physical-backup.manifest" base)
                (physical-bootstrap-manifest-image (list :logical-slot-copy-id id)))
               (rove:ok (rove:signals (logical-slot-copy-check-promotion) 'error)))
             (delete-file (merge-pathnames "physical-backup.manifest" base))
             (let ((*physical-recovery-state* (list :logical-slot-copy-id "copy")))
               (rove:ok (rove:signals (logical-slot-copy-check-promotion) 'error))))
        (uiop:delete-directory-tree base :validate t :if-does-not-exist :ignore))))

(rove:deftest logical-import-evidence-history-keeps-the-applied-parent-before-a-newer-selection
    (multiple-value-bind (chain parent selected) (unit-logical-import-selection-chain)
      (let* ((descriptor (make-logical-slot-copy-descriptor
                          :history (make-array 0 :element-type '(unsigned-byte 8))))
             (control (list :timeline 1 :history-target 3
                            :history-digest (logical-slot-copy-import-digest (second chain)))))
        (dolist (fork '(100 150 200))
          (let* ((certificate (list :system-id 42 :base-end 64 :base-timeline 1
                                    :parent-timeline 2 :child-timeline 4 :fork fork))
                 (evidence (list :selected-history selected :selection-chain chain
                                 :child-history (timeline-encode-history 4 2 fork
                                                                         "pg-lisp promotion" parent)))
                 (history (logical-slot-copy-evidence-check-history
                           evidence certificate descriptor control)))
            (rove:ok (= 3 (timeline-history-entry-timeline (first history))))
            (rove:ok (timeline-prefix-boundary-p 2 fork history))
            (setf (getf evidence :child-history)
                  (timeline-encode-history 4 2 (1+ fork) "pg-lisp promotion" parent))
            (rove:ok (rove:signals
                      (logical-slot-copy-evidence-check-history
                       evidence certificate descriptor control) 'error))))
        (let* ((certificate (list :system-id 42 :base-end 64 :base-timeline 1
                                  :parent-timeline 2 :child-timeline 4 :fork 150))
               (changed-parent (timeline-encode-history 2 1 99 "changed ancestry" nil))
               (evidence (list :selected-history selected :selection-chain chain
                               :child-history (timeline-encode-history 4 2 150
                                                                       "pg-lisp promotion" changed-parent))))
          (rove:ok (rove:signals
                    (logical-slot-copy-evidence-check-history evidence certificate descriptor control)
                    'error))))))

(rove:deftest logical-import-evidence-selection-chain-rejects-substitution
    (multiple-value-bind (chain parent selected) (unit-logical-import-selection-chain)
      (declare (ignore parent))
      (let* ((certificate (list :system-id 42 :base-timeline 1 :base-end 64))
             (control (list :history-target 3
                            :history-digest (logical-slot-copy-import-digest (second chain))))
             (evidence (list :selected-history selected :selection-chain chain)))
        (rove:ok (logical-slot-copy-evidence-check-selection-chain evidence certificate control))
        (flet ((reject (changed-evidence changed-certificate changed-control)
                 (rove:ok (rove:signals
                           (logical-slot-copy-evidence-check-selection-chain
                            changed-evidence changed-certificate changed-control) 'error))))
          (reject (list :selected-history selected) certificate control)
          (reject evidence certificate nil)
          (reject evidence certificate (list :history-target 3))
          (reject evidence certificate (list :history-digest (getf control :history-digest)))
          (reject evidence (list :system-id 43 :base-timeline 1 :base-end 64) control)
          (reject evidence (list :system-id 42 :base-timeline 1 :base-end 65) control)
          (reject (list :selected-history selected :selection-chain (reverse chain)) certificate control)
          (reject (list :selected-history selected :selection-chain (list (second chain)))
                  certificate control)
          (reject (list :selected-history (string->bytes "changed") :selection-chain chain)
                  certificate control)
          (reject evidence certificate
                  (list :history-target 3 :history-digest (logical-slot-copy-import-digest selected)))
          (let ((bad (copy-seq (second chain))))
            (setf (aref bad 12) (logxor 1 (aref bad 12)))
            (reject (list :selected-history selected :selection-chain (list (first chain) bad))
                    certificate control))
          (dolist (key '(:previous-digest :fork-digest :eor-octets))
            (let ((payload (physical-store-history-payload (second chain))))
              (setf (getf payload key)
                    (if (eq key :eor-octets)
                        (coerce (wal-encode-end-of-recovery-record 200 0 3 1 2) 'list)
                        (1+ (getf payload key))))
              (let ((bad (physical-store-history-image payload)))
                (reject (list :selected-history selected :selection-chain (list (first chain) bad))
                        certificate control))))))
      (rove:ok (logical-slot-copy-evidence-check-selection-chain
                (list :selected-history selected)
                (list :system-id 42 :base-timeline 3 :base-end 256) (list :timeline 3)))
      (let* ((value (unit-logical-import-evidence 512 2))
             (legacy (logical-slot-copy-import-evidence-encode value)))
        (rove:ok (null (getf (logical-slot-copy-import-evidence-decode legacy) :selection-chain)))
        (rove:ok (equalp legacy (logical-slot-copy-import-evidence-encode
                                 (logical-slot-copy-import-evidence-decode legacy)))))
      (rove:ok (rove:signals (logical-slot-copy-evidence-chain-size
                              (make-list (1+ +physical-store-history-chain-limit+)
                                         :initial-element (first chain))) 'error))
      (rove:ok (rove:signals (logical-slot-copy-evidence-chain-size
                              (list (make-array +physical-store-history-maximum-bytes+
                                                :element-type '(unsigned-byte 8)))) 'error))))

(rove:deftest logical-import-evidence-rejects-selection-trailer-forgeries
    (let* ((value (append (unit-logical-import-evidence 512 2)
                          (list :selection-chain (unit-logical-import-selection-chain))))
           (image (logical-slot-copy-import-evidence-encode value))
           (start (- (length image) 4
                     (logical-slot-copy-evidence-chain-size (getf value :selection-chain)))))
      (rove:ok (equalp value (logical-slot-copy-import-evidence-decode image)))
      (flet ((reject (bytes)
               (wal-set-u32 bytes (- (length bytes) 4)
                            (wal-crc32 bytes 0 (- (length bytes) 4)))
               (rove:ok (rove:signals (logical-slot-copy-import-evidence-decode bytes) 'error))))
        (dolist (count '(0 3 257 4294967295))
          (let ((bad (copy-seq image)))
            (wal-set-u32 bad start count)
            (reject bad)))
        (dolist (size '(0 15 16777216 4294967295))
          (let ((bad (copy-seq image)))
            (wal-set-u32 bad (+ start 4) size)
            (reject bad)))
        (let ((bad (subseq image 0 (1- (length image)))))
          (wal-set-u32 bad 16 (- (length bad) 24))
          (reject bad)))))

(rove:deftest logical-import-evidence-retains-all-three-large-raw-blobs
    (let ((value (unit-logical-import-evidence 256 0))
          (size (* 16 1024 1024)))
      (dolist (key '(:manifest :selected-history :child-history))
        (setf (getf value key) (make-array size :element-type '(unsigned-byte 8)
                                                :initial-element 255)))
      (let* ((image (logical-slot-copy-import-evidence-encode value))
             (decoded (logical-slot-copy-import-evidence-decode image)))
        (rove:ok (> (length image) (* 3 size)))
        (rove:ok (<= (length image) +logical-slot-copy-evidence-maximum-bytes+))
        (dolist (key '(:manifest :selected-history :child-history))
          (rove:ok (= size (length (getf decoded key))))
          (rove:ok (equalp (getf value key) (getf decoded key)))))))

(rove:deftest logical-import-evidence-rejects-envelope-and-bounds
    (let ((image (logical-slot-copy-import-evidence-encode
                  (unit-logical-import-evidence 256 1))))
      (dotimes (index (length image))
        (let ((bad (copy-seq image)))
          (setf (aref bad index) (logxor 1 (aref bad index)))
          (rove:ok (rove:signals (logical-slot-copy-import-evidence-decode bad) 'error))))
      (rove:ok (rove:signals
                (logical-slot-copy-import-evidence-decode (subseq image 0 (1- (length image))))
                'error))
      (rove:ok (rove:signals
                (logical-slot-copy-import-evidence-decode
                 (concatenate '(vector (unsigned-byte 8)) image #(0))) 'error))
      (let ((bad (copy-seq image)))
        (wal-set-u32 bad 20 #xffffffff)
        (wal-set-u32 bad (- (length bad) 4) (wal-crc32 bad 0 (- (length bad) 4)))
        (rove:ok (rove:signals (logical-slot-copy-import-evidence-decode bad) 'error))))
  (dolist (change '((:extra 1) (:manifest nil) (:plan nil)))
    (let ((value (unit-logical-import-evidence 256 1)))
      (setf (getf value (first change)) (second change))
      (rove:ok (rove:signals (logical-slot-copy-import-evidence-encode value) 'error)))))

(rove:deftest logical-import-evidence-rejects-valid-crc-length-and-count-forgeries
    (let* ((image (logical-slot-copy-import-evidence-encode (unit-logical-import-evidence 256 0)))
           (position 20))
      (labels ((reject-at (offset number)
                 (let ((bad (copy-seq image)))
                   (wal-set-u32 bad offset number)
                   (wal-set-u32 bad (- (length bad) 4) (wal-crc32 bad 0 (- (length bad) 4)))
                   (rove:ok (rove:signals (logical-slot-copy-import-evidence-decode bad) 'error)))))
        (dotimes (index 5)
          (declare (ignorable index))
          (reject-at position #xffffffff)
          (incf position (+ 4 (wal-u32 image position))))
        (incf position 20)
        (reject-at position 16385)
        (incf position (+ 4 (wal-u32 image position)))
        (dolist (count '(0 1 2 4 6 8 4294967295))
          (reject-at position count)))))

(rove:deftest logical-import-evidence-rejects-child-plan-substitution
    (dolist (mode '(:fork :child :end :frame-order :frame-coordinate :snapshot-extra))
      (let* ((value (unit-logical-import-evidence 256 1))
             (plan (getf value :plan)))
        (ecase mode
          (:fork (incf (getf plan :fork)))
          (:child (incf (getf plan :child)))
          (:end (incf (getf plan :end)))
          (:frame-order (rotatef (first (getf plan :frames)) (second (getf plan :frames))))
          (:frame-coordinate (incf (first (first (getf plan :frames)))))
          (:snapshot-extra (setf (getf (getf plan :snapshot) :extra) 1)))
        (rove:ok (rove:signals (logical-slot-copy-import-evidence-encode value) 'error)
                 (format nil "~s" mode)))))

(rove:deftest logical-import-evidence-missing-control-fact-requires-source-replay
    (let* ((certificate (unit-logical-import-value))
           (source (unit-logical-provenance-slot :generation 0 :end 80 :plugin "pg_lisp" :profile :text))
           (bytes (logical-slot-state-encode source))
           (descriptor (make-logical-slot-copy-descriptor :slots (list (list "fixture" bytes))))
           (history (timeline-parse-history 1 "")))
      (rove:ok (eq :requires-source-replay
                   (logical-slot-copy-evidence-check-rows certificate descriptor nil history)))
      (rove:ok (rove:signals
                (logical-slot-copy-evidence-check-rows certificate descriptor nil nil) 'error))
      (let ((bad (copy-tree certificate)))
        (setf (second (first (getf bad :slots))) (logical-slot-generation-token 3))
        (rove:ok (rove:signals
                  (logical-slot-copy-evidence-check-rows bad descriptor nil history) 'error)))
      (setf source (unit-logical-provenance-slot
                    :generation 0 :end 80 :plugin "pg_lisp" :profile :text :child-owner 2)
            bytes (logical-slot-state-encode source)
            (logical-slot-copy-descriptor-slots descriptor) (list (list "fixture" bytes)))
      (let* ((bad (copy-tree certificate))
             (row (first (getf bad :slots)))
             (child (unit-logical-provenance-slot
                     :generation 0 :end 80 :plugin "pg_lisp" :profile :text :child-owner 2)))
        (setf (getf bad :owner) (logical-slot-generation-token 2)
              (third row) (logical-slot-copy-import-digest bytes)
              (fourth row) (bytes->string (logical-slot-state-encode child)))
        (rove:ok (rove:signals
                  (logical-slot-copy-evidence-check-rows bad descriptor nil history) 'error)))))

(cl-spec:defproperty resolved-import-fact-preserves-frozen-evidence
    ((cause (member :none :wal-removed :wal-level-insufficient))
     (offset (range integer 0 100000))
     (generation (range integer 0 18446744073709551615))
     (profile (member :text :binary-bytea))
     (persisted (member nil t)))
  (:about logical-slot-copy-evidence-check-resolved-fact)
  (:trials (:normal 150))
  (let* ((history (timeline-parse-history 1 ""))
         (source (unit-logical-provenance-slot
                  :cause cause :generation generation :end (+ 80 offset)
                  :plugin "pg_lisp" :profile profile))
         (certificate (unit-logical-import-value
                       :generation generation :end (+ 80 offset) :profile profile :cause cause))
         (descriptor (make-logical-slot-copy-descriptor
                      :slots (list (list "fixture" (logical-slot-state-encode source)))))
         (fact (physical-logical-fact-next
                nil (unit-logical-status-record t :start (+ 32 offset)) 1 history))
         (control (list :applied (+ 289 offset) :timeline 1
                        :promotion-logical-decoding-enabled t)))
    (setf fact (physical-logical-fact-next
                fact (unit-logical-status-record t :start (+ 256 offset)) 1 history)
          (getf certificate :fork) (+ 289 offset)
          (getf certificate :child-end) (+ 290 offset)
          (getf certificate :copy-id) (logical-slot-generation-token (+ generation 2))
          (third (first (getf certificate :slots)))
          (logical-slot-copy-import-digest (logical-slot-state-encode source))
          (fourth (first (getf certificate :slots)))
          (bytes->string (logical-slot-copy-import-child-image source (1+ generation) cause)))
    (when persisted (setf (getf control :logical-fact) (copy-list fact)))
    (let ((before (copy-tree control)) (fact-before (copy-list fact)))
      (multiple-value-bind (checked marker)
          (logical-slot-copy-evidence-check-resolved-fact
           certificate descriptor control history fact)
        (and (eq marker :control-fact-checked)
             (equal checked fact) (not (eq checked fact))
             (equal fact fact-before) (equal control before))))))

(rove:deftest resolved-import-fact-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'resolved-import-fact-preserves-frozen-evidence
                                       :seed 7523 :profile :normal))))
      (rove:ok (eq :passed (getf data :status)) (let ((*print-length* nil) (*print-level* nil)) (format nil "~s" data)))
      (rove:ok (= 150 (getf data :trials)))))

(rove:deftest resolved-import-fact-rejects-unproved-boundaries-and-policy
    (let* ((history (timeline-parse-history 1 ""))
           (source (unit-logical-provenance-slot :plugin "pg_lisp"))
           (descriptor (make-logical-slot-copy-descriptor
                        :slots (list (list "fixture" (logical-slot-state-encode source)))))
           (baseline (physical-logical-fact-next
                      nil (unit-logical-status-record t :start 32) 1 history)))
      (setf baseline (physical-logical-fact-next
                      baseline (unit-logical-status-record t :start 256) 1 history))
      (dolist (mode '(:missing :through :timeline :control-boundary :control-parent
                      :frozen-fact :disabled :disposition))
        (let ((certificate (unit-logical-import-value))
              (fact (copy-list baseline))
              (control (list :applied 289 :timeline 1 :promotion-logical-decoding-enabled t)))
          (setf (getf certificate :fork) 289 (getf certificate :child-end) 290)
          (ecase mode
            (:missing (setf fact nil))
            (:through (incf (getf fact :through)))
            (:timeline (setf (getf fact :through-timeline) 2))
            (:control-boundary (incf (getf control :applied)))
            (:control-parent (setf (getf control :applied-timeline) 2))
            (:frozen-fact
             (setf (getf control :logical-fact) (copy-list baseline))
             (incf (getf (getf control :logical-fact) :coverage-start)))
            (:disabled (setf (getf control :promotion-logical-decoding-enabled) nil))
            (:disposition (setf (fifth (first (getf certificate :slots))) :wal-removed)))
          (let ((before (copy-tree control)) (fact-before (copy-tree fact)))
            (rove:ok (rove:signals
                      (logical-slot-copy-evidence-check-resolved-fact
                       certificate descriptor control history fact) 'error)
                     (format nil "~s" mode))
            (rove:ok (equal control before))
            (rove:ok (equal fact fact-before)))))))

(defun unit-source-proof-cursor-result (gap length)
  (let* ((start +wal-segment-size+)
         (raw (wal-encode-record
               +wal-record-maintenance+ 0 start
               (make-array (- length +wal-header-size+)
                           :element-type '(unsigned-byte 8) :initial-element 0)))
         (history (list (make-timeline-history-entry :timeline 1 :begin 0)))
         (seen nil))
    (handler-case
        (multiple-value-bind (visit finish)
            (logical-slot-copy-source-proof-archive-cursor
             (%make-physical-store)
             (lambda (record bytes)
               (assert (equalp bytes raw))
               (push (wal-record-start record) seen))
             (- start gap) (+ start length) history)
          (funcall visit (wal-decode-record raw start) raw)
          (funcall finish)
          (list :passed seen))
      (error () (list :rejected seen)))))

(cl-spec:defproperty permanent-source-cursor-requires-necessary-rotation
    ((gap (range integer 1 256)) (length (range integer 32 256)))
  (:about logical-slot-copy-source-proof-archive-cursor)
  (:trials (:normal 200))
  (equal (unit-source-proof-cursor-result gap length)
         (if (< gap length)
             (list :passed (list +wal-segment-size+))
             (list :rejected nil))))

(rove:deftest permanent-source-cursor-rotation-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'permanent-source-cursor-requires-necessary-rotation
                                         :seed 7522 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 200 (getf result :trials)))))

(rove:deftest permanent-source-cursor-rejects-inexact-f-and-impossible-gaps
    (let* ((boundary +wal-segment-size+)
           (start (- boundary 48))
           (first-raw (wal-encode-record +wal-record-maintenance+ 0 start #()))
           (next-raw (wal-encode-record +wal-record-batch-start+ 0 boundary #()))
           (first (wal-decode-record first-raw start))
           (next (wal-decode-record next-raw boundary))
           (history (list (make-timeline-history-entry :timeline 1 :begin 0))))
      (dolist (fork (list (- boundary 8) boundary (1+ boundary)))
        (let ((seen nil))
          (multiple-value-bind (visit finish)
              (logical-slot-copy-source-proof-archive-cursor
               (%make-physical-store)
               (lambda (record raw) (declare (ignore raw)) (push (wal-record-start record) seen))
               start fork history)
            (funcall visit first first-raw)
            (rove:ok
             (handler-case
                 (progn (funcall visit next next-raw) (funcall finish) nil)
               (error (condition)
                 (search (if (> fork boundary) "crosses F" "did not reach exact F")
                         (princ-to-string condition)))))
            (rove:ok (equal seen (list start)) "rejected successor never reaches the replay callback"))))
      (multiple-value-bind (visit finish)
          (logical-slot-copy-source-proof-archive-cursor
           (%make-physical-store) (lambda (&rest args) (declare (ignore args)))
           (1+ start) (+ boundary 32) history)
        (declare (ignore finish))
        (rove:ok (handler-case (progn (funcall visit first first-raw) nil)
                   (error (condition) (search "crosses its replay cursor" (princ-to-string condition))))))
      (rove:ok (equal (unit-source-proof-cursor-result 16 32)
                      (list :passed (list boundary))))
      (rove:ok (equal (unit-source-proof-cursor-result 32 32) (list :rejected nil)))
      (rove:ok (equal (unit-source-proof-cursor-result 100 32) (list :rejected nil)))))

(rove:deftest permanent-source-cursor-checks-eor-before-callback
    (let* ((start +wal-segment-size+)
           (fork (- start 16))
           (raw (wal-encode-end-of-recovery-record start 0 2 1 2))
           (record (wal-decode-record raw start))
           (history (timeline-check-lineage
                     (timeline-parse-history
                      2 (bytes->string (timeline-encode-history 2 1 fork "rotation fork" nil)))))
           (previous (list :saved-selection t))
           (candidate (%make-physical-store :selection previous))
           (seen 0))
      (multiple-value-bind (visit finish)
          (logical-slot-copy-source-proof-archive-cursor
           candidate
           (lambda (item bytes)
             (rove:ok (eq record item))
             (rove:ok (eq raw bytes))
             (let ((selection (physical-store-selection candidate)))
               (rove:ok (= 2 (getf selection :target)))
               (rove:ok (= 1 (getf selection :parent)))
               (rove:ok (= fork (getf selection :fork)))
               (rove:ok (eq previous (getf selection :previous)))
               (rove:ok (eq raw (getf selection :eor-bytes))))
             (incf seen))
           fork (wal-record-end record) history)
        (funcall visit record raw)
        (funcall finish)
        (rove:ok (= 1 seen)))
      (dolist (bad-history
               (list (list (make-timeline-history-entry :timeline 1 :begin 0))
                     (timeline-check-lineage
                      (timeline-parse-history
                       2 (bytes->string (timeline-encode-history
                                         2 1 (1- fork) "wrong fork" nil))))))
        (let ((called nil) (candidate (%make-physical-store :selection previous)))
          (multiple-value-bind (visit finish)
              (logical-slot-copy-source-proof-archive-cursor
               candidate (lambda (&rest args) (declare (ignore args)) (setf called t))
               fork (wal-record-end record) bad-history)
            (declare (ignore finish))
            (rove:ok (handler-case (progn (funcall visit record raw) nil)
                       (error (condition)
                         (search "EOR differs from embedded ancestry" (princ-to-string condition)))))
            (rove:ok (null called))
            (rove:ok (eq previous (physical-store-selection candidate))))))))

(defun unit-source-proof-envelope (salt size)
  (loop for (key minimum maximum) in (logical-slot-copy-source-proof-sections)
        collect key
        collect (make-array (+ minimum (mod size (1+ (min 16 (- maximum minimum)))))
                            :element-type '(unsigned-byte 8) :initial-element salt)))

(cl-spec:defproperty source-proof-envelope-preserves-bounded-sections
    ((salt (range integer 0 255)) (size (range integer 0 256)))
  (:about logical-slot-copy-source-proof-encode logical-slot-copy-source-proof-decode)
  (:trials (:normal 150))
  (let* ((value (unit-source-proof-envelope salt size))
         (bytes (logical-slot-copy-source-proof-encode value))
         (before (copy-seq bytes))
         (decoded (logical-slot-copy-source-proof-decode bytes)))
    (and (equalp value decoded)
         (equalp bytes before)
         (equalp bytes (logical-slot-copy-source-proof-encode decoded))
         (every (lambda (key) (not (eq (getf value key) (getf decoded key))))
                (loop for key in value by #'cddr collect key)))))

(rove:deftest source-proof-envelope-generated-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'source-proof-envelope-preserves-bounded-sections
                                       :seed 7524 :profile :normal))))
      (rove:ok (eq :passed (getf data :status))
               (let ((*print-length* nil) (*print-level* nil)) (format nil "~s" data)))
      (rove:ok (= 150 (getf data :trials)))))

(rove:deftest source-proof-envelope-rejects-corruption-and-length-forgeries
    (let* ((value (unit-source-proof-envelope 7 4))
           (image (logical-slot-copy-source-proof-encode value))
           (position 20))
      (dotimes (index (length image))
        (let ((bad (copy-seq image)))
          (setf (aref bad index) (logxor 1 (aref bad index)))
          (rove:ok (rove:signals (logical-slot-copy-source-proof-decode bad) 'error))))
      (loop for key in value by #'cddr
            do (let ((bad (copy-seq image)))
                 (wal-set-u32 bad position #xffffffff)
                 (wal-set-u32 bad (- (length bad) 4) (wal-crc32 bad 0 (- (length bad) 4)))
                 (rove:ok (rove:signals (logical-slot-copy-source-proof-decode bad) 'error)))
               (incf position (+ 4 (length (getf value key)))))
      (let ((bad (concatenate '(vector (unsigned-byte 8)) image #(0))))
        (wal-set-u32 bad 16 (- (length bad) 24))
        (wal-set-u32 bad (- (length bad) 4) (wal-crc32 bad 0 (- (length bad) 4)))
        (rove:ok (rove:signals (logical-slot-copy-source-proof-decode bad) 'error)))
      (rove:ok (rove:signals
                (logical-slot-copy-source-proof-decode (subseq image 0 23)) 'error))
      (let ((bad (copy-list value)))
        (setf (getf bad :installation) #(1))
        (rove:ok (rove:signals (logical-slot-copy-source-proof-encode bad) 'error)))
      (rove:ok (rove:signals
                (logical-slot-copy-source-proof-encode (append value '(:extra 1))) 'error))
      (rove:ok (rove:signals
                (logical-slot-copy-source-proof-encode (cons :extra value)) 'error))))

(cl-spec:defproperty source-receipt-preserves-historical-observations
    ((number (range integer 0 18446744073709551615))
     (floor (range integer 3 18446744073709551616)))
  (:about logical-slot-copy-source-receipt-image logical-slot-copy-source-receipt-decode)
  (:trials (:normal 150))
  (let* ((value (list :metadata (list :version 4 :lsn number)
                      :generation-size 1024 :catalog-size 128 :catalog-digest number
                      :next-oid number :local-floor floor))
         (image (logical-slot-copy-source-receipt-image value))
         (decoded (logical-slot-copy-source-receipt-decode image)))
    (and (equal value decoded)
         (not (eq (getf value :metadata) (getf decoded :metadata)))
         (equalp image (logical-slot-copy-source-receipt-image decoded)))))

(rove:deftest source-receipt-generated-contract-and-schema-rejections
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'source-receipt-preserves-historical-observations
                                       :seed 7525 :profile :normal))))
      (rove:ok (eq :passed (getf data :status))
               (let ((*print-length* nil) (*print-level* nil)) (format nil "~s" data)))
      (rove:ok (= 150 (getf data :trials))))
  (dolist (change '((:metadata nil) (:catalog-size 0) (:catalog-size 1024)
                    (:generation-size 1073741825) (:catalog-digest -1)
                    (:next-oid 1.0) (:local-floor 2) (:local-floor 18446744073709551617)
                    (:extra 1)))
    (let ((value (list :metadata '(:version 4 :lsn 80)
                       :generation-size 1024 :catalog-size 128 :catalog-digest 0
                       :next-oid 10000 :local-floor 3)))
      (setf (getf value (first change)) (second change))
      (rove:ok (rove:signals (logical-slot-copy-source-receipt-image value) 'error)
               (format nil "~s" change)))))

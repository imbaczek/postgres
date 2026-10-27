(in-package #:pg-physical-tests)

(defun call-with-retained-inventory (function)
  (with-physical-store-directory (root)
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (multiple-value-bind (metadata bytes) (pg::physical-retained-extent-decode (second images))
        (declare (ignore bytes))
        (funcall function root images identity history (getf metadata :history-text))))))

(deftest retained-inventory-publishes-and-reopens-exact-references
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (let* ((id (pg::physical-retained-inventory-publish root identity 2 text images))
              (loaded (pg::physical-retained-inventory-load root id identity history)))
         (ok (= 2 (length (getf loaded :entries))))
         (ok (= 2 (length (pg::physical-retained-inventory-plan root id identity history 32 120))))
         (ok (= id (pg::physical-retained-inventory-publish root identity 2 text images)))
         (ok (= 3 (length (pg::directory-entry-names root))))))))

(deftest retained-inventory-append-preserves-old-generation
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (let* ((old (pg::physical-retained-inventory-publish root identity 2 text (list (first images))))
              (new (pg::physical-retained-inventory-publish root identity 2 text (rest images) :previous old)))
         (ok (/= old new))
         (ok (= 1 (length (getf (pg::physical-retained-inventory-load root old identity history) :entries))))
         (ok (= 2 (length (getf (pg::physical-retained-inventory-load root new identity history) :entries))))))))

(deftest retained-inventory-rejects-missing-or-changed-referenced-image
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (let* ((id (pg::physical-retained-inventory-publish root identity 2 text images))
              (loaded (pg::physical-retained-inventory-load root id identity history))
              (entry (first (getf loaded :entries)))
              (path (pg::physical-retained-object-path root :extent (first entry))))
         (pg::wal-atomic-write-bytes path #(1 2 3))
         (ok (signals (pg::physical-retained-inventory-load root id identity history) 'error))
         (ok (equalp #(1 2 3) (pg::read-bounded-regular-file path 100)))
         (delete-file path)
         (ok (signals (pg::physical-retained-inventory-load root id identity history) 'error))))))

(deftest retained-inventory-before-root-failure-leaves-only-unreferenced-images
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (declare (ignore history))
       (let ((pg::*physical-retained-inventory-test-hook*
               (lambda (point id)
                 (declare (ignore id))
                 (when (eq point :after-extent-publication) (error "cut before inventory")))))
         (ok (signals (pg::physical-retained-inventory-publish root identity 2 text images) 'error)))
       (ok (= 1 (length (pg::directory-entry-names root))))
       (ok (integerp (pg::physical-retained-inventory-publish root identity 2 text images))))))

(deftest retained-inventory-root-publication-failure-can-be-reopened-by-reference
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (let ((published nil))
         (let ((pg::*physical-retained-inventory-test-hook*
                 (lambda (point id)
                   (when (eq point :after-inventory-publication)
                     (setf published id)
                     (error "cut after inventory")))))
           (ok (signals (pg::physical-retained-inventory-publish root identity 2 text images) 'error)))
         (ok published)
         (ok (= 2 (length (getf (pg::physical-retained-inventory-load root published identity history) :entries))))))))

(deftest retained-inventory-publication-owns-caller-images
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (let ((pg::*physical-retained-inventory-test-hook*
               (lambda (point id)
                 (declare (ignore id))
                 (when (eq point :after-extent-publication) (fill (second images) 0)))))
         (let ((id (pg::physical-retained-inventory-publish root identity 2 text images)))
           (ok (= 2 (length (getf (pg::physical-retained-inventory-load root id identity history) :entries)))))))))

(deftest retained-inventory-rejects-bad-input-before-creating-files
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (declare (ignore history))
       (setf (aref (second images) 0) 0)
       (ok (signals (pg::physical-retained-inventory-publish root identity 2 text images) 'error))
       (ok (null (pg::directory-entry-names root))))))

(deftest retained-inventory-never-overwrites-a-conflicting-object
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (declare (ignore history))
       (let* ((id (pg::physical-store-update-digest #xCBF29CE484222325 (first images)))
              (path (pg::physical-retained-object-path root :extent id)))
         (pg::wal-atomic-write-bytes path #(7 8 9))
         (ok (signals (pg::physical-retained-inventory-publish root identity 2 text images) 'error))
         (ok (equalp #(7 8 9) (pg::read-bounded-regular-file path 100)))))))

(defun retained-inventory-payload-fixture (seed manifest)
  (multiple-value-bind (metadata bytes) (retained-extent-fixture seed manifest)
    (let ((image (pg::physical-retained-extent-encode metadata bytes)))
      (list :version 1
            :identity (list :system-id 42 :base-timeline 1 :base 0 :manifest-digest manifest)
            :history-target 2 :history-text (getf metadata :history-text)
            :entries (list (list (pg::physical-store-update-digest #xCBF29CE484222325 image)
                                 (length image) metadata))))))

(cl-spec:defproperty retained-inventory-codec-preserves-full-width-references
    ((seed (range integer 0 18446744073709551615))
     (manifest (range integer 0 18446744073709551615)))
  (:about pg::physical-retained-inventory-image)
  (:trials (:normal 200))
  (let* ((payload (retained-inventory-payload-fixture seed manifest))
         (loaded (pg::physical-retained-inventory-decode (pg::physical-retained-inventory-image payload))))
    (and (equal payload loaded)
         (= manifest (getf (getf loaded :identity) :manifest-digest))
         (= seed (getf (third (first (getf loaded :entries))) :start-digest)))))

(deftest generated-retained-inventory-reference-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'retained-inventory-codec-preserves-full-width-references :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest retained-inventory-checks-text-budget-before-serialization
    (let* ((payload (retained-inventory-payload-fixture 123 77))
           (size (length (pg::string->bytes (pg::physical-bootstrap-manifest-text payload)))))
      (ok (= size (pg::physical-retained-inventory-check-text-budget payload size)))
      (ok (signals (pg::physical-retained-inventory-check-text-budget payload (1- size)) 'error))))

(deftest retained-inventory-text-budget-counts-utf8-and-literal-escapes
    (dolist (text (list "" (format nil "~c~c~c" #\" #\\ #\Newline) "zażółć 😀"))
      (let* ((payload (list :text text :empty nil :values (list 0 18446744073709551615)))
             (size (length (pg::string->bytes (pg::physical-bootstrap-manifest-text payload)))))
        (ok (= size (pg::physical-retained-inventory-check-text-budget payload size)))
        (ok (signals (pg::physical-retained-inventory-check-text-budget payload (1- size)) 'error)))))

(deftest retained-inventory-does-not-follow-a-referenced-image-symlink
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (let* ((id (pg::physical-retained-inventory-publish root identity 2 text images))
              (loaded (pg::physical-retained-inventory-load root id identity history))
              (entry (first (getf loaded :entries)))
              (path (pg::physical-retained-object-path root :extent (first entry)))
              (backup (merge-pathnames "saved-image.wal" root)))
         (rename-file path backup)
         (sb-posix:symlink (namestring backup) (namestring path))
         (ok (signals (pg::physical-retained-inventory-load root id identity history) 'error))
         (ok (probe-file backup))))))

(deftest retained-inventory-missing-new-root-does-not-select-an-older-generation
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (let* ((old (pg::physical-retained-inventory-publish root identity 2 text (list (first images))))
              (new (pg::physical-retained-inventory-publish root identity 2 text (rest images) :previous old)))
         (delete-file (pg::physical-retained-object-path root :inventory new))
         (ok (signals (pg::physical-retained-inventory-load root new identity history) 'error))
         (ok (= 1 (length (getf (pg::physical-retained-inventory-load root old identity history) :entries))))))))

(deftest retained-inventory-query-loads-only-selected-extent-images
    (call-with-retained-inventory
     (lambda (root images identity history text)
       (let ((id (pg::physical-retained-inventory-publish root identity 2 text images))
             (original (symbol-function 'pg::physical-retained-object-read))
             (reads nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-retained-object-read)
                      (lambda (directory kind reference maximum)
                        (push kind reads)
                        (funcall original directory kind reference maximum)))
                (ok (= 1 (length (pg::physical-retained-inventory-plan root id identity history 64 120))))
                (ok (= 1 (count :extent reads)))
                (ok (= 1 (count :inventory reads))))
           (setf (symbol-function 'pg::physical-retained-object-read) original))))))

(defun retained-inventory-crash-at (root images token)
  (loop for image in images for number from 0
        do (pg::wal-atomic-write-bytes
            (merge-pathnames (format nil "input-~d.image" number) root) image))
  (multiple-value-bind (output errors status)
      (uiop:run-program
       (list (or (uiop:getenv "SBCL") "sbcl") "--noinform" "--no-sysinit" "--no-userinit" "--script"
             (namestring (asdf:system-relative-pathname "pg" "t/retained-inventory-crash-child.lisp"))
             (namestring root) token)
       :output :string :error-output :string :ignore-error-status t)
    (ok (= status 137) (format nil "~a: status ~a ~a ~a" token status output errors)))
  (let ((fields (pg::wal-split-fields
                 (pg::bytes->string
                  (pg::read-bounded-regular-file (merge-pathnames "inventory-crash.marker" root) 128)))))
    (ok (= 3 (length fields)))
    (ok (plusp (parse-integer (first fields))))
    (ok (string= (string-right-trim '(#\Newline #\Return) (third fields)) token))
    (parse-integer (second fields))))

(deftest retained-inventory-sigkill-publication-and-reopen
    (dolist (point '(:after-extent-publication :after-inventory-publication))
      (call-with-retained-inventory
       (lambda (root images identity history text)
         (let* ((id (retained-inventory-crash-at root images (symbol-name point)))
                (archive (merge-pathnames "archive/" root)))
           (if (eq point :after-inventory-publication)
               (ok (= 2 (length (getf (pg::physical-retained-inventory-load archive id identity history) :entries))))
               (progn
                 (ok (= 1 (length (pg::directory-entry-names archive))))
                 (ok (not (signals (pg::physical-retained-extent-decode
                                    (pg::physical-retained-object-read
                                     archive :extent id pg::+physical-retained-image-limit+)) 'error)))
                 (ok (signals (pg::physical-retained-inventory-load archive id identity history) 'error))))
           (let ((complete (pg::physical-retained-inventory-publish archive identity 2 text images)))
             (ok (= 2 (length (pg::physical-retained-inventory-plan archive complete identity history 32 120))))))))))

(deftest retained-inventory-sigkill-file-publication-cuts
    (dolist (kind '(:extent :inventory))
      (dolist (phase '(:before-file-fsync :after-file-fsync :before-rename
                       :after-rename :before-directory-fsync :after-directory-fsync))
        (call-with-retained-inventory
         (lambda (root images identity history text)
           (let* ((id (retained-inventory-crash-at root images (format nil "~a-~a" kind phase)))
                  (archive (merge-pathnames "archive/" root))
                  (path (pg::physical-retained-object-path archive kind id))
                  (published (member phase '(:after-rename :before-directory-fsync :after-directory-fsync))))
             (if published
                 (if (eq kind :inventory)
                     (ok (= 2 (length (getf (pg::physical-retained-inventory-load archive id identity history) :entries))))
                     (ok (not (signals (pg::physical-retained-extent-decode
                                        (pg::physical-retained-object-read
                                         archive :extent id pg::+physical-retained-image-limit+)) 'error))))
                 (ng (pg::wal-namespace-file-stat path t)))
             (let ((complete (pg::physical-retained-inventory-publish archive identity 2 text images)))
               (ok (= 2 (length (pg::physical-retained-inventory-plan archive complete identity history 32 120)))))))))))

(deftest retained-inventory-candidate-budget-precedes-extent-file-open
    (multiple-value-bind (metadata ignored) (retained-extent-fixture)
      (declare (ignore ignored))
      (with-physical-store-directory (root)
        (let* ((identity (list :system-id 42 :base-timeline 1 :base 0 :manifest-digest 77))
               (text (getf metadata :history-text))
               (entries (loop for number below 5
                              collect (let ((item (copy-list metadata)))
                                        (setf (getf item :start) (+ 128 (* number 32))
                                              (getf item :end) (+ 160 (* number 32))
                                              (getf item :start-digest) 123 (getf item :end-digest) 123)
                                        (list number pg::+physical-retained-image-limit+ item))))
               (payload (list :version 1 :identity identity :history-target 2 :history-text text :entries entries))
               (id (pg::physical-retained-object-publish
                    root :inventory (pg::physical-retained-inventory-image payload) pg::+physical-retained-inventory-limit+)))
          ;; There are deliberately no extent files: opening one would fail for ENOENT.
          (ok (handler-case
                  (progn (pg::physical-retained-inventory-plan
                          root id identity (pg::timeline-parse-history 2 text) 128 288) nil)
                (error (condition) (search "budget" (princ-to-string condition)))))))))

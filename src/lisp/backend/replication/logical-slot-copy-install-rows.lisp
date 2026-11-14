(in-package #:pg)

(defun logical-slot-copy-install-row-open (writer row expected installed)
  (logical-slot-copy-install-row-proof writer)
  (let* ((slot-root (logical-slot-copy-install-row-writer-slot-root writer))
         (parent (if installed slot-root (logical-slot-copy-install-row-writer-slots writer)))
         (directory (logical-slot-copy-install-publisher-open writer parent (first row) :directory :group slot-root))
         (state (logical-slot-copy-install-publisher-open writer directory "state" :file :group slot-root)))
    (unless (and (equal (subseq (second expected) 0 2)
                        (subseq (logical-slot-copy-install-node-identity directory) 0 2))
                 (equal (subseq (third expected) 0 2)
                        (subseq (logical-slot-copy-install-node-identity state) 0 2)))
      (error "logical installation row inodes differ from their journal"))
    (logical-slot-copy-install-check-row-writer writer)
    (values directory state)))

(defun logical-slot-copy-install-row-rebind (writer directory state)
  (let* ((next-directory (%make-logical-slot-copy-install-node
                          :fd (logical-slot-copy-install-node-fd directory)
                          :parent (logical-slot-copy-install-row-writer-slot-root writer)
                          :name (logical-slot-copy-install-node-name directory) :kind :directory
                          :identity (logical-slot-copy-install-node-identity directory)))
         (next-state (%make-logical-slot-copy-install-node
                      :fd (logical-slot-copy-install-node-fd state) :parent next-directory
                      :name "state" :kind :file :identity (logical-slot-copy-install-node-identity state))))
    (setf (logical-slot-copy-install-publisher-nodes writer)
          (mapcar (lambda (node) (cond ((eq node directory) next-directory)
                                       ((eq node state) next-state) (t node)))
                  (logical-slot-copy-install-publisher-nodes writer)))
    (values next-directory next-state)))

(defun logical-slot-copy-install-row-rename (writer directory state)
  (declare (notinline physical-adoption-renameat2))
  (let ((name (logical-slot-copy-install-node-name directory)))
    (logical-slot-copy-install-row-point writer :before-row-rename name)
    (logical-slot-copy-install-row-proof writer)
    (when *logical-slot-copy-install-inspection*
      (error "logical installation row rename cannot span readonly inspection"))
    (sb-sys:without-interrupts
      (unless (zerop (physical-adoption-renameat2
                      (logical-slot-copy-install-node-fd (logical-slot-copy-install-row-writer-slots writer)) name
                      (logical-slot-copy-install-node-fd (logical-slot-copy-install-row-writer-slot-root writer)) name
                      +physical-adoption-rename-noreplace+))
        (error "cannot exclusively install logical slot row: errno ~d" (sb-alien:get-errno)))
      (multiple-value-setq (directory state) (logical-slot-copy-install-row-rebind writer directory state))
      (incf (logical-slot-copy-install-row-writer-prefix writer)))
    (logical-slot-copy-install-row-point writer :after-row-rename name)
    (values directory state)))

(defun logical-slot-copy-install-row-sync (writer node point name)
  (logical-slot-copy-install-check-row-writer writer)
  (logical-slot-copy-install-publisher-sync writer node :row-sync)
  (logical-slot-copy-install-row-point writer point name))

(defun logical-slot-copy-install-row-finish (writer directory state)
  (let ((name (logical-slot-copy-install-node-name directory)))
    (logical-slot-copy-install-row-sync writer state :after-row-state-sync name)
    (logical-slot-copy-install-row-sync writer directory :after-row-directory-sync name)
    (logical-slot-copy-install-row-sync writer (logical-slot-copy-install-row-writer-slot-root writer)
                                        :after-row-target-parent-sync name)
    (logical-slot-copy-install-row-sync writer (logical-slot-copy-install-row-writer-slots writer)
                                        :after-row-source-parent-sync name)
    (logical-slot-copy-install-row-proof writer)
    (push (list (copy-seq name)
                (physical-image-file-facts (logical-slot-copy-install-node-fd directory))
                (physical-image-file-facts (logical-slot-copy-install-node-fd state)))
          (logical-slot-copy-install-row-writer-closed writer))
    (logical-slot-copy-install-publisher-close writer state)
    (logical-slot-copy-install-publisher-close writer directory)
    (logical-slot-copy-install-row-point writer :after-row-close name)))

(defun logical-slot-copy-install-rows (owner context input)
  (let* ((record (logical-slot-copy-install-input-record owner context input))
         (image (logical-slot-copy-install-input-state-image record)))
    (call-with-logical-slot-copy-install-publisher
     owner context input
     (lambda (writer)
       (logical-slot-copy-install-row-bind writer)
       (let* ((value (logical-slot-copy-install-row-writer-value writer))
              (rows (getf (logical-slot-copy-import-decode (getf value :certificate)) :slots))
              (initial (logical-slot-copy-install-row-writer-prefix writer)))
         (loop for row in rows for expected in (getf value :entries) for index from 0
               do (multiple-value-bind (directory state)
                      (logical-slot-copy-install-row-open writer row expected (< index initial))
                    (logical-slot-copy-install-row-point writer :row-open (first row))
                    (unless (< index initial)
                      (multiple-value-setq (directory state)
                        (logical-slot-copy-install-row-rename writer directory state)))
                    (logical-slot-copy-install-row-finish writer directory state)))
         (logical-slot-copy-install-row-proof writer)
         (unless (= (length rows) (logical-slot-copy-install-row-writer-prefix writer))
           (error "logical installation row prefix did not finish"))
         (length rows)))
     :constructor (lambda (&rest arguments)
                    (apply #'%make-logical-slot-copy-install-row-writer
                           :value (logical-slot-copy-install-decode image)
                           :prefix (logical-slot-copy-install-input-state-prefix record) arguments)))))

(defun logical-slot-copy-install-fresh-rows ()
  (call-with-logical-slot-copy-install-fresh-input
   (lambda (owner context input)
     (logical-slot-copy-install-publish-journal owner context input)
     (logical-slot-copy-install-rows owner context input))))

(defun logical-slot-copy-install-resume-rows ()
  (call-with-logical-slot-copy-install-cold-input
   (lambda (owner context input)
     (logical-slot-copy-install-publish-journal owner context input)
     (logical-slot-copy-install-rows owner context input))))

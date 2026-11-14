(in-package #:pg)

;;; This bound covers the writer. Validation opens separate temporary read descriptors.

;;; Resync installed rows and move the remaining pending rows in order.
;;; Only one row and its state file are held open at a time.
(defun logical-slot-copy-install-row-open (writer row expected installed)
  "Reprove the source and open one row and state file by recorded identities."
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
  "Bind the held row and state identities to the canonical parent after rename."
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
  "Reprove the source, then move one row with NOREPLACE and advance the prefix."
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
  "Sync one held inode with the row role checked around the primitive."
  (logical-slot-copy-install-check-row-writer writer)
  (logical-slot-copy-install-publisher-sync writer node :row-sync)
  (logical-slot-copy-install-row-point writer point name))

(defun logical-slot-copy-install-row-finish (writer directory state)
  "Sync state, row, target parent, then source parent and reprove the source.
Save row facts and close its descriptors before processing the next row."
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

(defun logical-slot-copy-install-rows (owner context input &key (phase :pending))
  "Resync the installed prefix, install remaining pending rows, and return the
count and a scoped receipt.
Pending is the default; cold active with a complete prefix only resyncs rows.
Mint the receipt after publisher cleanup, a fresh input and source check, and
rechecking full row and parent facts while the source lease is held."
  (let* ((record (logical-slot-copy-install-input-record owner context input))
         (image (logical-slot-copy-install-input-state-image record))
         (facts nil)
         (count
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
                (setf facts (logical-slot-copy-import-state-image
                             (logical-slot-copy-install-row-durable-facts
                              (logical-slot-copy-install-input-state-operation record) image)))
                (length rows)))
            :constructor (lambda (&rest arguments)
                           (apply #'%make-logical-slot-copy-install-row-writer
                                  :value (logical-slot-copy-install-decode image) :expected-phase phase
                                  :prefix (logical-slot-copy-install-input-state-prefix record) arguments)))))
    (logical-slot-copy-install-check-input owner context input)
    (unless (and (eq phase (logical-slot-copy-install-input-state-phase record))
                 (= count (logical-slot-copy-install-input-state-prefix record))
                 (logical-slot-copy-committed-image-equal-p
                  facts (logical-slot-copy-install-row-durable-facts
                         (logical-slot-copy-install-input-state-operation record) image)))
      (logical-slot-copy-install-revoke-input record)
      (error "logical installation rows changed after completion"))
    (let ((receipt (%make-logical-slot-copy-install-row-receipt
                    :owner owner :context context :input input
                    :operation (logical-slot-copy-install-input-state-operation record)
                    :image (copy-seq image) :phase phase :prefix count :facts facts)))
      (sb-sys:without-interrupts
        (setf (logical-slot-copy-install-input-state-row-receipt record) receipt
              (logical-slot-copy-install-input-state-row-receipt-seal record)
              (logical-slot-copy-import-state-image (logical-slot-copy-install-row-receipt-view receipt))
              (logical-slot-copy-install-input-state-row-receipt-state record) :available))
      (values count receipt))))

(defun logical-slot-copy-install-fresh-rows ()
  "Publish a fresh journal and install its pending rows."
  (call-with-logical-slot-copy-install-fresh-input
   (lambda (owner context input)
     (logical-slot-copy-install-publish-journal owner context input)
     (logical-slot-copy-install-rows owner context input))))

(defun logical-slot-copy-install-resume-rows ()
  "Resync a fixed cold journal and resume its pending rows."
  (call-with-logical-slot-copy-install-cold-input
   (lambda (owner context input)
     (logical-slot-copy-install-publish-journal owner context input)
     (logical-slot-copy-install-rows owner context input))))

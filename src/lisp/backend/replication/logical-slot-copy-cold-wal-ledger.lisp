(in-package #:pg)

(defstruct (logical-slot-copy-cold-wal-ledger
            (:constructor %make-logical-slot-copy-cold-wal-ledger)
            (:print-function physical-libpq-print-private-object))
  context directory directory-facts rows schedule operation expected acquire maximum)

(defun logical-slot-copy-cold-facts-match-p (before after ignored-fields)
  (and (= (length before) (length after))
       (loop for old in before for actual in after for index from 0
             always (or (member index ignored-fields) (eql old actual)))))

(defun logical-slot-copy-cold-wal-read (directory name facts maximum)
  (declare (notinline physical-image-openat sb-posix:close))
  (let ((fd nil) (complete nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-image-openat
                       directory name (logior sb-posix:o-rdonly sb-posix:o-nofollow
                                              sb-posix:o-nonblock +postmaster-o-cloexec+) 0))
             (when (minusp fd) (setf fd nil) (error "cannot open witnessed WAL bytes"))
             (sb-sys:with-local-interrupts
               (unless (and (sb-posix:s-isreg (third facts))
                            (equal facts (physical-image-file-facts fd))
                            (equal facts (physical-image-file-facts directory name)))
                 (error "WAL byte reader differs from its witnessed inode"))
               (multiple-value-prog1
                   (logical-slot-copy-import-control-read-fd fd maximum)
                 (unless (and (equal facts (physical-image-file-facts fd))
                              (equal facts (physical-image-file-facts directory name)))
                   (error "witnessed WAL bytes changed during read"))
                 (setf complete t))))
        (when fd
          (let ((closing fd) (failure nil))
            (setf fd nil)
            (handler-case (sb-posix:close closing) (error (condition) (setf failure condition)))
            (when (and complete failure) (error failure))))))))

(defun logical-slot-copy-cold-wal-row (ledger name)
  (assoc name (logical-slot-copy-cold-wal-ledger-rows ledger) :test #'equal))

(defun logical-slot-copy-cold-wal-check-rows (ledger &optional directory-changed check-bytes)
  (let* ((context (logical-slot-copy-cold-wal-ledger-context ledger))
         (directory (logical-slot-copy-cold-wal-ledger-directory ledger))
         (before (logical-slot-copy-cold-wal-ledger-directory-facts ledger))
         (rows (logical-slot-copy-cold-wal-ledger-rows ledger))
         (now (physical-image-file-facts directory)))
    (physical-image-check-children context)
    (unless (and (equal now (physical-image-file-facts (physical-image-context-root-fd context) "pg_wal"))
                 (logical-slot-copy-cold-facts-match-p before now (when directory-changed '(5 6 7)))
                 (equal (sort (mapcar #'first rows) #'string<)
                        (logical-slot-copy-import-control-names
                         directory (logical-slot-copy-cold-wal-ledger-maximum ledger))))
      (error "cold WAL ledger directory or names changed"))
    (dolist (row rows)
      (destructuring-bind (name facts bytes witness) row
        (unless (and (equal facts (physical-image-file-facts directory name))
                     (or (null witness) (equal facts (physical-image-file-facts witness)))
                     (or (null check-bytes) (null bytes)
                         (equalp bytes (logical-slot-copy-cold-wal-read directory name facts (length bytes)))))
          (error "cold WAL ledger inode or bytes changed"))))
    (when directory-changed
      (setf (logical-slot-copy-cold-wal-ledger-directory-facts ledger) now)))
  t)

(defun logical-slot-copy-cold-wal-frame-postimage (before offset record)
  (unless (<= offset (length before)) (error "cold WAL frame would create a hole"))
  (let ((after (make-array (max (length before) (+ offset (length record)))
                           :element-type '(unsigned-byte 8))))
    (replace after before)
    (replace after record :start1 offset)
    after))

(defun logical-slot-copy-cold-wal-schedule (state plan prefix files history-p install-prefix history)
  (let* ((child (getf state :promotion-child))
         (orphans (remove-if (lambda (segment) (assoc segment files))
                             (wal-discover-segments child :timeline)))
         (history-name (timeline-history-filename child)))
    (when (and history-p orphans) (error "published child history has unexplained WAL segments"))
    (append
     (mapcar (lambda (segment) (list :orphan (wal-segment-filename segment child :timeline) nil nil)) orphans)
     (unless history-p
       (append (when install-prefix
                 (list (list :prefix (wal-segment-filename (floor (getf plan :fork) +wal-segment-size+)
                                                           child :timeline) prefix nil)))
               (mapcar (lambda (entry)
                         (list :prefix (wal-segment-filename (first entry) child :timeline)
                               (make-array 0 :element-type '(unsigned-byte 8)) nil)) (rest files))))
     (unless history-p (list (list :history-create history-name history nil)))
     (list (list :history-sync history-name history nil))
     (mapcar (lambda (frame)
               (list :frame (wal-segment-filename (floor (first frame) +wal-segment-size+) child :timeline)
                     (second frame) (mod (first frame) +wal-segment-size+)))
             (getf plan :frames)))))

(defun logical-slot-copy-cold-wal-initialize (context state plan prefix history-p install-prefix maximum acquire maximum-bytes)
  (physical-image-check-children context)
  (let* ((directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal)))
         (facts (physical-image-file-facts directory))
         (history (physical-promotion-history-image state))
         (files (progn
                  (when (> (+ (length history) (length prefix)
                              (loop for frame in (getf plan :frames) sum (length (second frame))))
                           maximum-bytes)
                    (error "cold WAL planned payload and history exceed their byte bound"))
                  (physical-promotion-checkpoint-files plan prefix)))
         (schedule (logical-slot-copy-cold-wal-schedule
                    state plan prefix files history-p install-prefix history))
         (affected (mapcar #'second schedule))
         (mutable (append (loop for step in schedule unless (eq :orphan (first step)) collect (second step))
                          (mapcar (lambda (entry)
                                    (wal-segment-filename (first entry) (getf plan :child) :timeline)) files)))
         (byte-total (+ (length history) (loop for entry in files sum (length (second entry)))))
         (expected (append (mapcar (lambda (entry)
                                     (list (wal-segment-filename (first entry) (getf plan :child) :timeline)
                                           (second entry))) files)
                           (list (list (timeline-history-filename (getf plan :child)) history))))
         (rows
           (mapcar
            (lambda (name)
              (when (logical-slot-copy-cold-scratch-kind name)
                (error "cold WAL ledger requires normalized scratch"))
              (let* ((row-facts (physical-image-file-facts directory name))
                     (changeable (member name mutable :test #'equal)))
                (when (and (or changeable (member name affected :test #'equal))
                           (not (logical-slot-copy-import-control-private-facts-p row-facts facts)))
                  (error "cold WAL mutable file is not private"))
                (list name row-facts
                      (when changeable
                        (when (> (incf byte-total (sixth row-facts)) maximum-bytes)
                          (error "cold WAL byte images exceed their total bound"))
                        (logical-slot-copy-cold-wal-read directory name row-facts +wal-segment-size+))
                      nil)))
            (logical-slot-copy-import-control-names directory maximum))))
    (when (> byte-total maximum-bytes)
      (error "cold WAL planned and existing byte images exceed their total bound"))
    (let ((ledger (%make-logical-slot-copy-cold-wal-ledger
                   :context context :directory directory :directory-facts facts :rows rows
                   :schedule (logical-slot-copy-import-state-image schedule)
                   :expected (logical-slot-copy-import-state-image expected)
                   :acquire acquire :maximum maximum)))
      (logical-slot-copy-cold-wal-check-rows ledger)
      (dolist (entry expected)
        (unless (find (first entry) schedule :key #'second :test #'equal)
          (let ((row (logical-slot-copy-cold-wal-row ledger (first entry))))
            (unless (and row (equalp (third row) (second entry)))
              (error "cold WAL static prefix differs from its complete expected image")))))
      (let ((row (logical-slot-copy-cold-wal-row ledger (timeline-history-filename (getf plan :child)))))
        (unless (and (eql (not (null row)) (not (null history-p)))
                     (or (null row) (equalp (third row) history)))
          (error "cold WAL history phase differs from its initial binding")))
      ledger)))

(declaim (ftype function logical-slot-copy-cold-wal-observe))

(defun logical-slot-copy-cold-wal-complete (ledger)
  (when (or (logical-slot-copy-cold-wal-ledger-operation ledger)
            (logical-slot-copy-cold-wal-ledger-schedule ledger))
    (error "cold WAL staging schedule is incomplete"))
  (dolist (expected (logical-slot-copy-cold-wal-ledger-expected ledger))
    (let ((row (logical-slot-copy-cold-wal-row ledger (first expected))))
      (unless (and row (= 1 (fourth (second row))) (equalp (third row) (second expected)))
        (error "cold WAL complete child differs from planned bytes"))))
  t)

(defun logical-slot-copy-cold-wal-stage-checkpoint
    (state plan prefix files history-p check sync-data &key context install-prefix observer)
  (unless (and context (eql (getf state :promotion-child) (getf plan :child))
               (logical-slot-copy-committed-image-equal-p
                files (physical-promotion-checkpoint-files plan prefix)))
    (error "cold WAL staging inputs differ from their sealed plan"))
  (funcall check nil)
  (physical-promotion-stage-child-checkpoint
   state plan prefix (physical-promotion-checkpoint-files plan prefix) history-p check sync-data
   :context context :install-prefix install-prefix :observer observer))

(defun logical-slot-copy-cold-wal-snapshot (ledger)
  (logical-slot-copy-cold-wal-complete ledger)
  (list :directory-facts (copy-list (logical-slot-copy-cold-wal-ledger-directory-facts ledger))
        :rows (mapcar (lambda (row) (logical-slot-copy-import-state-image (subseq row 0 3)))
                      (logical-slot-copy-cold-wal-ledger-rows ledger))))

(defun logical-slot-copy-cold-wal-check-snapshot (context snapshot maximum)
  (physical-image-check-context context)
  (let ((ledger (%make-logical-slot-copy-cold-wal-ledger
                 :context context
                 :directory (cdr (assoc "pg_wal" (physical-image-context-children context) :test #'equal))
                 :directory-facts (getf snapshot :directory-facts)
                 :rows (mapcar (lambda (row) (append row (list nil))) (getf snapshot :rows))
                 :maximum maximum)))
    (logical-slot-copy-cold-wal-check-rows ledger nil t)))

(defun call-with-logical-slot-copy-cold-wal-ledger
    (context state plan prefix guard function &key history-p install-prefix (maximum 4096)
                                                (maximum-bytes (* 6 +wal-segment-size+)))
  (declare (notinline sb-posix:fcntl physical-image-openat))
  (unless (and (functionp guard) (functionp function) (typep maximum '(integer 1 *))
               (typep maximum-bytes '(integer 0 268435456))
               (eql (getf state :promotion-child) (getf plan :child)))
    (error "invalid cold WAL ledger scope"))
  (when (> (+ (length prefix) (loop for frame in (getf plan :frames) sum (length (second frame))))
           maximum-bytes)
    (error "cold WAL planned payload exceeds its byte bound"))
  (funcall guard)
  (let* ((owned nil) (ledger nil) (active t) (failed nil) (complete nil)
                     (image (logical-slot-copy-import-state-image (list state plan prefix history-p install-prefix))))
    (labels ((check-scope ()
               (unless (and active (not failed)) (error "cold WAL ledger ended or failed"))
               (physical-image-check-context context)
               (unless (logical-slot-copy-committed-image-equal-p
                        image (list state plan prefix history-p install-prefix))
                 (error "cold WAL ledger input binding changed"))
               (funcall guard))
             (acquire (operation argument)
               (let* ((fd nil) (entry (cons nil nil)) (link (list entry)))
                 (sb-sys:without-interrupts
                   (setf fd
                         (ecase operation
                           (:dup (sb-posix:fcntl argument #x406 0))
                           (:open
                            (physical-image-openat
                             (logical-slot-copy-cold-wal-ledger-directory ledger) argument
                             (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                     +postmaster-o-cloexec+) 0))))
                   (when (minusp fd) (error "cannot retain cold WAL inode witness"))
                   (setf (cdr entry) fd (cdr link) owned owned link))
                 fd))
             (invoke (operation &rest arguments)
               (let ((done nil))
                 (unwind-protect
                      (multiple-value-prog1
                          (progn
                            (check-scope)
                            (ecase operation
                              (:verify
                               (unless (member (first arguments) '(nil :staged :snapshot))
                                 (error "invalid cold WAL boundary phase"))
                               (when (logical-slot-copy-cold-wal-ledger-operation ledger)
                                 (error "cold WAL boundary has an unfinished operation"))
                               (logical-slot-copy-cold-wal-check-rows ledger nil t)
                               (when (first arguments) (logical-slot-copy-cold-wal-complete ledger))
                               (when (eq :snapshot (first arguments))
                                 (logical-slot-copy-cold-wal-snapshot ledger)))
                              (:observe
                               (apply #'logical-slot-copy-cold-wal-observe ledger arguments))))
                        (setf done t))
                   (unless done (setf failed t))))))
      (sb-sys:without-interrupts
        (unwind-protect
             (sb-sys:with-local-interrupts
               (setf ledger (logical-slot-copy-cold-wal-initialize
                             context state plan prefix history-p install-prefix maximum #'acquire maximum-bytes))
               (multiple-value-prog1
                   (funcall function
                            (lambda (event operation name fd &optional destination)
                              (invoke :observe event operation name fd destination))
                            (lambda (stage) (invoke :verify stage)))
                 (invoke :verify :staged)
                 (setf complete t)))
          (setf active nil)
          (let ((failure (physical-image-drain-descriptors owned nil)))
            (setf owned nil)
            (when (and complete failure) (error failure))))))))

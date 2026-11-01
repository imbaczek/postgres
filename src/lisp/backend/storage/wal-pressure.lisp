;;;; Admission pressure for retained primary WAL publication resources.
(in-package #:pg)

(declaim (special *logical-capture-memory-bytes* *query-temp-directory*))

(defstruct wal-pressure-ticket
  (page-entries 0 :read-only t)
  (contributor-entries 0 :read-only t)
  (page-capacity 0 :read-only t)
  (contributor-capacity 0 :read-only t)
  (contributor-cells 0 :read-only t)
  (resets 0 :read-only t)
  (spool-bytes 0 :read-only t)
  (spool-records 0 :read-only t)
  (disk-references 0 :read-only t))

(defparameter *wal-publication-pressure-limits*
  (list :owners 16 :page-map-entries 4096 :contributor-map-entries 4096
        :page-map-capacity 8192 :contributor-map-capacity 8192
        :contributor-cells 16384 :resets 256
        :spool-bytes (* 32 1024 1024) :spool-records 512
        :scratch-disk-references 4096 :scratch-bytes (* 32 1024 1024)
        :cleanup-owners 24 :cleanup-entries 64 :temporary-paths 24
        :open-streams 24 :private-directories 24 :temporary-bytes (* 64 1024 1024)))

(defparameter *wal-publication-pressure-keys*
  '(:owners :page-map-entries :contributor-map-entries :page-map-capacity
    :contributor-map-capacity :contributor-cells :resets :spool-bytes :spool-records
    :scratch-disk-references :scratch-bytes :cleanup-owners :cleanup-entries
    :temporary-paths :open-streams :private-directories :temporary-bytes))

(defun wal-pressure-vector-valid-p (vector complete-p)
  (let ((length (ignore-errors (list-length vector))) (seen nil))
    (and length (evenp length)
         (loop for (key value) on vector by #'cddr
               always (and (member key *wal-publication-pressure-keys*)
                           (not (member key seen))
                           (push key seen)
                           (typep value '(integer 0))))
         (or (not complete-p) (= (length seen) (length *wal-publication-pressure-keys*))))))

(defun wal-publication-pressure-exceeded-p (footprint &optional (limits *wal-publication-pressure-limits*))
  (unless (and (wal-pressure-vector-valid-p footprint nil)
               (wal-pressure-vector-valid-p limits t))
    (wal-invalid-input "invalid WAL publication pressure vector"))
  (some (lambda (key) (> (getf footprint key 0) (getf limits key)))
        *wal-publication-pressure-keys*))

(defun wal-pressure-ticket-footprint (ticket)
  (unless (wal-pressure-ticket-p ticket) (wal-invalid-input "missing WAL publication pressure ticket"))
  (list :owners 1 :page-map-entries (wal-pressure-ticket-page-entries ticket)
        :contributor-map-entries (wal-pressure-ticket-contributor-entries ticket)
        :page-map-capacity (wal-pressure-ticket-page-capacity ticket)
        :contributor-map-capacity (wal-pressure-ticket-contributor-capacity ticket)
        :contributor-cells (wal-pressure-ticket-contributor-cells ticket)
        :resets (wal-pressure-ticket-resets ticket)
        :spool-bytes (wal-pressure-ticket-spool-bytes ticket)
        :spool-records (wal-pressure-ticket-spool-records ticket)
        :scratch-disk-references (wal-pressure-ticket-disk-references ticket)
        :scratch-bytes (* +blcksz+ (wal-pressure-ticket-disk-references ticket))))

(defun wal-pressure-ticket-from-maps (pages contributors resets bytes records)
  (make-wal-pressure-ticket
   :page-entries (hash-table-count pages) :contributor-entries (hash-table-count contributors)
   :page-capacity (hash-table-size pages) :contributor-capacity (hash-table-size contributors)
   :contributor-cells (loop for xids being the hash-values of contributors sum (length xids))
   :resets (length resets) :spool-bytes bytes :spool-records records
   :disk-references (loop for reference being the hash-values of pages
                          count (and (wal-dirty-reference-p reference)
                                     (null (wal-dirty-reference-page reference))))))

(defun wal-project-publication-pressure (keys resets)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((bytes 0) (records 0))
    (map-wal-primary-page-key-chunks
     keys
     (lambda (chunk image-bytes)
       (declare (ignore chunk))
       (incf bytes (+ 16 image-bytes))
       (incf records)))
    (wal-pressure-ticket-from-maps *wal-page-cache* *wal-page-cache-xids* resets bytes records)))

(defun wal-actual-publication-pressure (owner)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (multiple-value-bind (bytes records)
      (logical-spool-resource-size (wal-page-publication-spool owner))
    (wal-pressure-ticket-from-maps
     (wal-page-publication-pages owner) (wal-page-publication-contributors owner)
     (wal-page-publication-resets owner) bytes records)))

(defun wal-add-pressure-footprint (target source)
  (loop for (key value) on source by #'cddr do (incf (getf target key) value))
  target)

(defun wal-publication-pressure-footprint (&optional incoming projected-p)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((cost (loop for key in *wal-publication-pressure-keys* append (list key 0))))
    (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
          while owner do
            (wal-add-pressure-footprint
             cost (wal-pressure-ticket-footprint (wal-page-publication-pressure-ticket owner))))
    (when incoming (wal-add-pressure-footprint cost (wal-pressure-ticket-footprint incoming)))
    (when *wal-temp-cleanup-domain*
      (wal-add-pressure-footprint
       cost (query-temp-cleanup-domain-resource-counts (wal-temp-cleanup-domain-under-lock))))
    (when (and incoming projected-p)
      (incf (getf cost :cleanup-owners))
      (when (> (wal-pressure-ticket-spool-bytes incoming) *logical-capture-memory-bytes*)
        (incf (getf cost :cleanup-entries))
        (incf (getf cost :temporary-paths))
        (incf (getf cost :open-streams))
        (unless *query-temp-directory* (incf (getf cost :private-directories)))
        (incf (getf cost :temporary-bytes)
              (+ (wal-pressure-ticket-spool-bytes incoming)
                 (* 8 (wal-pressure-ticket-spool-records incoming))))))
    cost))

(defun wal-preflight-publication-pressure (keys resets)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((ticket (wal-project-publication-pressure keys resets)))
    (when (and *wal-pending-page-publication*
               (wal-publication-pressure-exceeded-p
                (wal-publication-pressure-footprint ticket t)))
      (wal-settle-page-publications-under-lock)
      (setf ticket (wal-project-publication-pressure keys resets)))
    (values ticket
            (not (null (wal-publication-pressure-exceeded-p
                        (wal-publication-pressure-footprint ticket t)))))))

(defun wal-publication-pressure-requires-flush-p (owner projection forced-p)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (and (eq :stack (wal-page-publication-state owner))
               (wal-page-publication-prepared-p owner))
    (wal-invalid-input "pressure recheck requires an unpublished prepared owner"))
  (let* ((actual (wal-pressure-ticket-footprint (wal-page-publication-pressure-ticket owner)))
         (projected (wal-pressure-ticket-footprint projection)))
    (or forced-p
        (loop for (key value) on actual by #'cddr thereis (> value (getf projected key 0)))
        (not (null (wal-publication-pressure-exceeded-p
                    (wal-publication-pressure-footprint
                     (wal-page-publication-pressure-ticket owner))))))))
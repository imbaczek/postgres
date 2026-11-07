(in-package #:pg)

(defun physical-reseed-follow-source-position (client coordinates minimum)
  (let* ((row (physical-client-identity-row client))
         (system (pg-client-native-u64 (aref row 0)))
         (timeline (pg-client-native-u64 (aref row 1) t))
         (flush (pg-client-native-lsn (aref row 2))))
    (unless (and (= system (getf coordinates :system-id))
                 (= timeline (getf coordinates :source-timeline))
                 (>= flush (max (getf coordinates :receive-lsn) minimum)))
      (error "target follow source differs from the saved writer or receive frontier"))
    flush))

;; Check the selected writer and exact saved full history between identity
;; observations, with sufficient monotone flush.  The original request's capture
;; timeline need not be the selected writer timeline.
(defun physical-reseed-follow-observe-source (client source generation &key (minimum 0))
  (let* ((coordinates (copy-list (getf generation :coordinates)))
         (history (copy-seq (getf generation :source-history)))
         (timeline (getf coordinates :source-timeline)))
    (physical-adoption-attempt-check-client client source)
    (let* ((first (physical-reseed-follow-source-position client coordinates minimum))
           (actual (if (= timeline 1) "" (bytes->string (pg-client-native-history client timeline)))))
      (unless (equal history actual)
        (error "target follow source history differs from its durable intent"))
      (let ((position (physical-reseed-follow-source-position client coordinates first)))
        (physical-adoption-attempt-check-client client source)
        position))))

(defun physical-reseed-follow-check-network-snapshot-at (root-fd expected)
  (physical-reseed-follow-check-snapshot root-fd (getf expected :follow))
  (physical-reseed-follow-check-attempt-snapshot-at root-fd (getf expected :attempt))
  expected)

;; Freeze the follow record and actual attempt around network work, including
;; the installed image and final metadata checks.  A change fences publication.
(defun physical-reseed-follow-network-snapshot (owner &optional expected)
  (unless (physical-reseed-follow-owner-busy-p owner)
    (error "target follow source proof requires an active operation"))
  (call-with-physical-directory-root
   owner
   (lambda (root-fd directory)
     (declare (ignore directory))
     (let ((snapshot
             (call-with-physical-reseed-follow-image
              owner
              (lambda (fd check)
                (physical-adoption-require-absent
                 (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path fd))
                 "target follow temporary requires reconciliation")
                (let* ((follow (physical-reseed-follow-snapshot-at fd))
                       (record (getf follow :record))
                       (generations (getf record :generations))
                       (current (list :follow follow
                                      :attempt (physical-reseed-follow-attempt-snapshot-at fd))))
                  (physical-reseed-follow-check-forward-record record)
                  (physical-reseed-follow-read-at fd "physical-reseed.follow" :stabilize t)
                  (physical-reseed-follow-check-network-snapshot-at fd current)
                  (unless (and (= 1 (length generations))
                               (member (getf (first generations) :phase) '(:preparing :proved)))
                    (error "target follow source preparation requires one preparing or proved generation"))
                  (if (eq :proved (getf (first generations) :phase))
                      (physical-reseed-follow-proof-at owner fd record)
                      (progn
                        (physical-reseed-follow-check-active-attempt owner)
                        (physical-reseed-follow-check-local-intent owner fd record)))
                  (when (and expected (not (equal expected current)))
                    (error "target follow intent or original attempt changed during source preparation"))
                  (funcall check)
                  (physical-reseed-follow-check-network-snapshot-at fd current))))))
       (physical-reseed-follow-check-final-metadata root-fd (getf snapshot :follow))
       (physical-reseed-follow-check-network-snapshot-at root-fd snapshot)))))

(defun physical-reseed-follow-proved-record (previous anchor)
  (let* ((record (physical-reseed-follow-intent-copy previous))
         (generations (getf record :generations))
         (anchor (physical-follow-anchor-copy anchor)))
    (unless (= 1 (length generations))
      (error "target follow proof requires one durable preparing generation"))
    (let ((generation (first generations)))
      (unless (and (equal (getf anchor :history-text) (getf generation :source-history))
                   (physical-follow-request-matches-anchor-p (getf generation :coordinates) anchor))
        (error "target follow source proof differs from its durable intent"))
      (setf (getf generation :phase) :proved
            (getf generation :anchor)
            (loop for (key value) on anchor by #'cddr
                  unless (eq key :history-text) append (list key value))))
    (setf record (physical-reseed-follow-copy record))
    (physical-reseed-follow-check-transition previous record)
    record))

;; Use only the original frozen route and exact saved request.  Observe the
;; source before and after the RPC, detach its reply before closing the client,
;; and close owned handles on every exit.  Failed network work keeps the durable
;; preparation intent for retry; it does not abort or bind a different source.
(defun physical-reseed-follow-prepare-remote (owner helper)
  (physical-reseed-follow-check-idle owner)
  (setf (physical-reseed-follow-owner-busy-p owner) t)
  (unwind-protect
       (let* ((snapshot (physical-reseed-follow-network-snapshot owner))
              (record (getf (getf snapshot :follow) :record))
              (generation (first (getf record :generations)))
              (attempt (getf (getf snapshot :attempt) :record))
              (source (physical-follow-source-copy (getf attempt :source)))
              (client nil) (anchor nil))
         (when (eq :proved (getf generation :phase))
           (return-from physical-reseed-follow-prepare-remote
             (values (physical-reseed-follow-copy record) snapshot nil)))
         (unwind-protect
              (let ((frozen (nth-value 1 (physical-follow-source-profile source))))
                (setf client
                      (physical-libpq-connect-frozen
                       (or helper (physical-libpq-helper-path :on-spawn (lambda (actor) (setf client actor))))
                       frozen :on-spawn (lambda (actor) (setf client actor))))
                (let ((position (physical-reseed-follow-observe-source client source generation)))
                  (physical-reseed-follow-network-snapshot owner snapshot)
                  (setf anchor
                        (physical-follow-anchor-copy
                         (pg-client-native-backup-lease-follow-prepare
                          client (physical-backup-lease-request-copy (getf attempt :request))
                          (getf generation :generation) (copy-seq (getf generation :name))
                          (physical-follow-request-copy (getf generation :coordinates)))))
                  (physical-reseed-follow-network-snapshot owner snapshot)
                  (physical-reseed-follow-observe-source client source generation :minimum position)))
           (when client (physical-client-close client)))
         (physical-reseed-follow-network-snapshot owner snapshot)
         (values (physical-reseed-follow-proved-record record anchor) snapshot t))
    (setf (physical-reseed-follow-owner-busy-p owner) nil)))

;; Finish network work before entering the store scope.  Recheck the snapshot
;; and local proof there before publishing the proved record.
(defun physical-reseed-follow-prepare (owner &key helper)
  (multiple-value-bind (candidate snapshot new-proof)
      (physical-reseed-follow-prepare-remote owner helper)
    (unless new-proof (return-from physical-reseed-follow-prepare candidate))
    (let ((bytes (physical-reseed-follow-image candidate))
          (previous (getf snapshot :follow)))
      (call-with-physical-reseed-follow-store-proof
       owner
       (lambda (root-fd check)
         (physical-reseed-follow-check-network-snapshot-at root-fd snapshot)
         (physical-reseed-follow-proof-at owner root-fd candidate)
         (funcall check)
         (physical-reseed-follow-check-network-snapshot-at root-fd snapshot)
         (physical-reseed-follow-write-intent owner root-fd candidate bytes previous check))))))

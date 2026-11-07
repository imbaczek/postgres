(in-package #:pg)

;;; Settlement journal validation and capacity checks for the target follow
;;; record.  This is a private format, not a PostgreSQL wire or disk format.

;; Derive the successor from the only proved generation and its anchor digest.
(defun physical-reseed-follow-settlement-successor (record)
  (let* ((core (physical-reseed-follow-core-copy
                (physical-reseed-follow-core-projection record)))
         (generation (car (last (getf core :generations)))))
    (unless (and (= 1 (length (getf core :generations)))
                 (eq :proved (getf generation :phase)))
      (error "settlement requires one proved generation"))
    (physical-backup-lease-successor-copy
     (list :name (getf generation :name)
           :anchor-digest (physical-follow-anchor-digest
                           (physical-reseed-follow-generation-anchor generation)))
     (getf (getf core :attempt) :request))))

;; Reconstruct the base bound to a closed source receipt.
(defun physical-reseed-follow-settlement-base (record)
  (let* ((base (getf record :base))
         (control (physical-recovery-decode-control
                   (physical-libpq-datum-octets (getf base :recovery-control) 196))))
    (list :checkpoint-start (getf base :checkpoint-start)
          :base (getf control :applied)
          :manifest-digest (getf base :manifest-digest))))

;; Validate settlement against the inert configuration, proved successor, and
;; exact closed outcome.  The receipt request is the client's binding to the
;; submitted request; it is not a full-request echo from the wire.
(defun physical-reseed-follow-settlement-copy (value record)
  (physical-reseed-install-check-literal (list value record))
  (physical-follow-check-plist value '(:phase :successor :receipt))
  (let* ((core (physical-reseed-follow-core-copy
                (physical-reseed-follow-core-projection record)))
         (configuration (physical-reseed-follow-configuration-copy
                         (getf record :configuration) core))
         (phase (getf value :phase))
         (successor (physical-reseed-follow-settlement-successor core))
         (submitted (physical-backup-lease-successor-copy
                     (getf value :successor) (getf (getf core :attempt) :request)))
         (receipt (when (getf value :receipt)
                    (physical-backup-lease-copy (getf value :receipt)))))
    (unless (and (eq :inert (getf configuration :phase))
                 (equal successor submitted)
                 (member phase '(:handoff-pending :handed-off :source-aborted)))
      (error "settlement differs from its inert proved successor"))
    (if (eq :handoff-pending phase)
        (when receipt (error "pending settlement cannot claim a source receipt"))
        (unless (and receipt (eq :closed (getf receipt :phase))
                     (equal (getf receipt :request) (getf (getf core :attempt) :request))
                     (equal (getf receipt :base) (physical-reseed-follow-settlement-base core))
                     (if (eq :handed-off phase)
                         (and (eq :handed-off (getf receipt :outcome))
                              (equal successor (getf receipt :successor)))
                         (and (eq :aborted (getf receipt :outcome))
                              (null (getf receipt :successor)))))
          (error "settlement lacks the exact closed source outcome")))
    (list :phase phase :successor successor :receipt receipt)))

;; Freeze the installed predecessors while settlement advances through pending
;; to one terminal outcome.  A terminal outcome cannot be changed.
(defun physical-reseed-follow-settlement-check-transition (before after)
  (let ((old (getf before :settlement)) (new (getf after :settlement)))
    (unless (every (lambda (key) (equal (getf before key) (getf after key)))
                   '(:version :attempt :install :base :generations :configuration))
      (error "settlement freezes the installed configuration and its predecessors"))
    (unless (and new
                 (cond
                   ((null old) (eq :handoff-pending (getf new :phase)))
                   ((equal old new) t)
                   ((eq :handoff-pending (getf old :phase))
                    (and (member (getf new :phase) '(:handed-off :source-aborted))
                         (equal (getf old :successor) (getf new :successor))))
                   (t nil)))
      (error "settlement cannot skip pending intent, regress or change its terminal outcome"))
    t))

(defun physical-reseed-follow-completion-terminal (record phase)
  ;; Construct the same canonical terminal outcome for settlement sizing and
  ;; future completion sizing, including the closed receipt and its successor.
  (let* ((candidate (copy-list record))
         (settlement (getf record :settlement))
         (successor (getf settlement :successor))
         (receipt (physical-backup-lease-copy
                   (list :version 1 :request (getf (getf record :attempt) :request)
                         :phase :closed :base (physical-reseed-follow-settlement-base record)
                         :outcome (ecase phase (:handed-off :handed-off) (:source-aborted :aborted))
                         :successor (when (eq phase :handed-off) successor)))))
    (setf (getf candidate :settlement)
          (physical-reseed-follow-settlement-copy
           (list :phase phase :successor successor :receipt receipt) record))
    candidate))

;; Size both canonical terminal outcomes before publishing pending intent.
(defun physical-reseed-follow-settlement-terminal-size (record)
  (unless (eq :handoff-pending (getf (getf record :settlement) :phase))
    (error "terminal sizing requires canonical pending intent"))
  (loop for phase in '(:handed-off :source-aborted)
        maximize (length (physical-backup-lease-encode-literal
                          "PGRESEEDFOLLOW1"
                          (physical-reseed-follow-completion-terminal record phase)
                          +physical-reseed-follow-maximum-bytes+))))

(defun physical-reseed-follow-capacity-reserve (record)
  ;; Pending cancellation releases the reserve for ordinary follow completion.
  (if (getf record :cancellation)
      0
      (+ (case (getf (car (last (getf record :generations))) :phase)
           (:preparing +physical-reseed-follow-proof-reserve+)
           ((:proved :canceling) 32)
           (otherwise 0))
         (if (eq :building (getf (getf record :configuration) :phase))
             +physical-reseed-follow-configuration-reserve+ 0))))

;; Reserve space for the larger terminal receipt as well as existing margins.
(defun physical-reseed-follow-check-image-capacity (record encoded-length maximum)
  (let* ((reserve (physical-reseed-follow-capacity-reserve record))
         (required (if (and (null (getf record :cancellation))
                            (eq :handoff-pending (getf (getf record :settlement) :phase)))
                       (max encoded-length (physical-reseed-follow-settlement-terminal-size record))
                       encoded-length)))
    (unless (<= (+ required reserve) maximum)
      (error "target follow journal lacks room for its next durable outcome"))
    t))

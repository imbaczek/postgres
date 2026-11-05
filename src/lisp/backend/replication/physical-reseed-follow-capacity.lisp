(in-package #:pg)

(defun physical-reseed-follow-literal-size (value)
  ;; The encoded image has the tag and 11 framing bytes in addition to the
  ;; UTF-8 literal.  Count bytes rather than characters.
  (+ (length "PGRESEEDFOLLOW1") 11
     (length (string->bytes (physical-bootstrap-manifest-text value)))))

(defun physical-reseed-follow-group-size (key value)
  ;; Adding a key/value pair to a nonempty plist replaces its closing
  ;; parenthesis with a space, the pair, and a closing parenthesis.
  (1- (length (string->bytes (physical-bootstrap-manifest-text (list key value))))))

(defun physical-reseed-follow-completion-growth (record)
  ;; These are sizing templates, not records to normalize or publish.  Maximum
  ;; unsigned 64-bit facts and the most negative signed 64-bit timestamps
  ;; give the widest decimal representations allowed for those fields.
  ;; Only active step 2 can coexist with retirement; maximize over the
  ;; compatible activation and retirement phases rather than combining
  ;; independently chosen phases.
  (let* ((wide (1- (ash 1 64)))
         (device (getf (getf (getf record :attempt) :local) :device))
         (size +physical-follow-config-maximum-bytes+)
         (image (list size wide))
         (entry (list "follow.active" :file #o600 device wide size wide))
         (bytes (physical-reseed-attempt-image (getf record :attempt)))
         (reference (list :identity (list device wide #o100600 wide 1 (length bytes)
                                          (- (ash 1 63)) (- (ash 1 63)))
                          :digest (physical-store-update-digest #xCBF29CE484222325 bytes))))
    (loop for (phase step) in '((:building 0) (:staged 0) (:activating 0)
                                (:activating 1) (:activating 2) (:active 2))
          for activation = (physical-reseed-follow-group-size
                            :activation (list :phase phase :step step :image image
                                              :entry (unless (eq phase :building) entry)))
          maximize
          (+ activation
             (if (eq phase :active)
                 (loop for (retirement prefix) in '((:retiring 0) (:retiring 1)
                                                    (:retiring 2) (:retired 2))
                       maximize (physical-reseed-follow-group-size
                                 :retirement (list :phase retirement :step prefix
                                                   :attempt-file reference)))
                 0)))))

(defun physical-reseed-follow-future-completion-size-bound (value)
  ;; Include the current record and each possible terminal outcome.  Only a
  ;; handed-off outcome can grow into activation and retirement.  Add the
  ;; existing capacity reserve once, after taking the maximum.
  (let* ((record (physical-reseed-follow-copy value))
         (base (physical-reseed-follow-activation-predecessor record))
         (phase (getf (getf base :settlement) :phase)))
    (unless (and (eq :inert (getf (getf base :configuration) :phase))
                 (member phase '(:handoff-pending :handed-off :source-aborted)))
      (error "future capacity requires inert settlement"))
    (+ (physical-reseed-follow-capacity-reserve record)
       (max (physical-reseed-follow-literal-size record)
            (loop for outcome in (if (eq phase :handoff-pending)
                                     '(:handed-off :source-aborted) (list phase))
                  for terminal = (if (eq phase outcome) base
                                     (physical-reseed-follow-completion-terminal base outcome))
                  maximize (+ (physical-reseed-follow-literal-size terminal)
                              (if (eq outcome :handed-off)
                                  (physical-reseed-follow-completion-growth terminal) 0)))))))

(defun physical-reseed-follow-check-future-capacity
    (record &optional (maximum +physical-reseed-follow-maximum-bytes+))
  (let ((required (physical-reseed-follow-future-completion-size-bound record)))
    (unless (<= required maximum)
      (error "target follow completion requires ~d bytes; capacity ~d is short by ~d"
             required maximum (- required maximum)))
    t))

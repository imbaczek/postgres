(in-package #:pg)

(defun physical-reseed-install-publish-prepared
    (owner root-fd record client image source-context source-root incoming-context incoming-root
     incoming-facts quarantine-facts quarantine-fd stage-check incoming-check quarantine-check)
  (let* ((mount (physical-adoption-install-mount-id nil :directory-fd root-fd))
         (device (physical-adoption-owner-device owner))
         (attempt-row (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount))
         (old (physical-reseed-install-old-snapshot owner record))
         (new (physical-reseed-install-image-rows incoming-context image :synchronize t))
         (forbidden (list (list device (physical-adoption-owner-inode owner))
                          (list (physical-adoption-owner-lock-device owner) (physical-adoption-owner-lock-inode owner))
                          (subseq attempt-row 1 3)))
         (seed (list :version 1 :attempt record
                     :attempt-file (list :device (second attempt-row) :inode (third attempt-row))
                     :incoming incoming-facts :quarantine quarantine-facts :journal nil
                     :manifest (getf image :manifest) :manifest-digest (getf image :manifest-digest)
                     :old old :new new)))
    (flet ((check (install-entry)
             (physical-directory-check-owner-common owner)
             (physical-reseed-check-original-pid owner)
             (unless (and (equal record (physical-reseed-attempt-read-at root-fd "physical-reseed.attempt"))
                          (equal attempt-row (physical-reseed-install-old-observe root-fd "physical-reseed.attempt" mount)))
               (error "reseed original attempt generation changed during installation publication"))
             (unless install-entry (physical-reseed-require-journal owner record))
             (funcall stage-check)
             (funcall incoming-check)
             (funcall quarantine-check)
             (physical-reseed-install-check-empty-quarantine quarantine-fd)
             (let ((*physical-image-context* source-context))
               (physical-reseed-image-check source-root image))
             (let ((*physical-image-context* incoming-context))
               (physical-reseed-image-check incoming-root image t))
             (unless (equal new (physical-reseed-install-image-rows incoming-context image))
               (error "reseed incoming inventory changed during publication"))
             (unless (equal old (physical-reseed-install-old-scan
                                 root-fd mount device forbidden :install-entry install-entry))
               (error "reseed old namespace changed during publication"))
             (physical-reseed-image-check-source client record image)
             (physical-directory-check-owner-common owner)))
      (physical-reseed-install-publish-journal root-fd seed #'check))))

(defun physical-reseed-install-publish-workspaces
    (owner root-fd record client image source-context source-root stage-check incoming quarantine)
  (let ((expected (physical-reseed-install-preflight-incoming owner incoming image)))
    (physical-reseed-image-check-source client record image)
    (call-with-physical-reseed-install-auxiliary
     owner incoming
     (lambda (incoming-directory incoming-facts incoming-fd incoming-check)
       (declare (ignore incoming-directory))
       (call-with-physical-reseed-install-auxiliary
        owner quarantine
        (lambda (quarantine-directory quarantine-facts quarantine-fd quarantine-check)
          (declare (ignore quarantine-directory))
          (call-with-physical-backup-image-directory
           incoming-fd
           (lambda (incoming-root)
             (physical-reseed-image-check incoming-root image t)
             (physical-reseed-install-publish-prepared
              owner root-fd record client image source-context source-root
              *physical-image-context* incoming-root incoming-facts quarantine-facts quarantine-fd
              stage-check incoming-check quarantine-check))))
        :prepare t :empty t))
     :prepare t :expected expected)))

(defun physical-reseed-publish-install (owner incoming quarantine &key helper)
  (unless (eq (type-of owner) 'physical-reseed-owner)
    (error "reseed installation publication requires ordinary stopped ownership"))
  (let ((record (physical-reseed-attempt-recover owner)))
    (unless (and (eq :premanifest (getf record :phase)) (getf record :stage))
      (error "reseed installation requires the original active attempt and frozen stage"))
    (let ((incoming (truename (uiop:ensure-directory-pathname incoming)))
          (quarantine (truename (uiop:ensure-directory-pathname quarantine))))
      (physical-reseed-install-check-directories
       (list (namestring (physical-adoption-owner-directory owner))
             (getf (getf record :stage) :directory) (namestring incoming) (namestring quarantine)))
      (call-with-physical-directory-root
       owner
       (lambda (root-fd root)
         (declare (ignore root))
         (call-with-physical-reseed-install-stage
          owner root-fd record
          (lambda (stage stage-fd stage-check)
            (declare (ignore stage))
            (call-with-physical-reseed-client
             owner record
             (lambda (client)
               (call-with-physical-backup-image-directory
                stage-fd
                (lambda (source-root)
                  (let ((image (physical-reseed-image-check source-root)))
                    (physical-reseed-image-check-source client record image)
                    (physical-reseed-install-publish-workspaces
                     owner root-fd record client image *physical-image-context* source-root stage-check
                     incoming quarantine)))))
             :helper helper))))))))

;;;; ARM64 checksum feature discovery. Unknown or failed probes mean scalar.
(in-package #:pg)

(defvar *native-arm-crc32-available-p* nil)
(defvar *native-arm-pmull-available-p* nil)

(defun native-arm-read-auxv-hwcap (path)
  "Read Linux ARM64 AT_HWCAP from a little-endian auxiliary-vector file.
Search at most 128 entries, returning immediately at AT_HWCAP. Return NIL if
it is absent, the search is truncated, or opening or reading fails. Trailing
entries are not validated. No instruction is probed by executing it."
  (handler-case
      (with-open-file (input path :element-type '(unsigned-byte 8))
        (flet ((word ()
                 (let ((value 0))
                   (dotimes (index 8 value)
                     (let ((byte (read-byte input nil nil)))
                       (unless byte (return-from word nil))
                       (setf value (logior value (ash byte (* index 8)))))))))
          (loop repeat 128
                for key = (word)
                for value = (word)
                do (when (or (null key) (null value) (zerop key)) (return nil))
                   (when (= key 16) (return value)))))
    (error () nil)))

(defun native-arm-sysctl-feature-p (name)
  "Query one macOS instruction-set flag, failing closed if it is unavailable."
  (declare (ignorable name))
  #+darwin
  (handler-case
      (sb-alien:with-alien ((value sb-alien:int)
                            (size sb-alien:unsigned-long))
        (setf value 0 size (sb-alien:alien-size sb-alien:int :bytes))
        (and (zerop
              (sb-alien:alien-funcall
               (sb-alien:extern-alien
                "sysctlbyname"
                (function sb-alien:int sb-alien:c-string (* sb-alien:int)
                          (* sb-alien:unsigned-long) sb-alien:system-area-pointer
                          sb-alien:unsigned-long))
               name (sb-alien:addr value) (sb-alien:addr size)
               (sb-sys:int-sap 0) 0))
             (= size (sb-alien:alien-size sb-alien:int :bytes))
             (= value 1)))
    (error () nil))
  #-darwin
  nil)

(defun native-arm-crc32-os-features ()
  "Return CRC32 and PMULL availability from the operating system.
Linux uses HWCAP_CRC32 bit 7 and HWCAP_PMULL bit 4. macOS uses sysctl flags.
The PMULL result is recorded for future folding kernels, not used by CRC32."
  #+linux
  (let ((hwcap (or (native-arm-read-auxv-hwcap #p"/proc/self/auxv") 0)))
    (values (logbitp 7 hwcap) (logbitp 4 hwcap)))
  #+darwin
  (values (native-arm-sysctl-feature-p "hw.optional.armv8_crc32")
          (native-arm-sysctl-feature-p "hw.optional.arm.FEAT_PMULL"))
  #-(or linux darwin)
  (values nil nil))

(defun refresh-native-arm-crc32-support ()
  "Refresh feature caches on load and saved-image startup.
Only little-endian ARM64 can select these kernels. Clear old state before a
probe so failed discovery cannot retain support from the image's build host."
  (setf *native-arm-crc32-available-p* nil
        *native-arm-pmull-available-p* nil)
  #+(and arm64 little-endian)
  (multiple-value-setq (*native-arm-crc32-available-p* *native-arm-pmull-available-p*)
    (native-arm-crc32-os-features))
  (values *native-arm-crc32-available-p* *native-arm-pmull-available-p*))

(eval-when (:load-toplevel :execute)
  (refresh-native-arm-crc32-support)
  (pushnew 'refresh-native-arm-crc32-support sb-ext:*init-hooks*))

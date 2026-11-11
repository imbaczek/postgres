;;;; ARM64 checksum kernels. Callers validate bounds and CPU support.
(in-package #:pg)

#+arm64
(defun %wal-crc32-ieee-arm-octets (bytes start end table constants seed)
  "Return final IEEE CRC32 using ARM64 CRC32 instructions.
BYTES is a simple octet vector with 0 <= START <= END <= its length. SEED is
an uncomplemented 32-bit state. The caller establishes little-endian ARM64
and CRC32 support. TABLE and CONSTANTS are unused. Valid calls do not allocate."
  (declare (optimize (speed 3) (safety 0))
           (ignore table constants)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  (sb-sys:with-pinned-objects (bytes)
    (let ((sap (sb-sys:vector-sap bytes))
          (index start)
          (crc seed))
      (declare (type fixnum index) (type (unsigned-byte 32) crc))
      (loop for cursor fixnum from index below (- end 7) by 8
            do (setf crc (%native-arm-crc32-word crc (sb-sys:sap-ref-64 sap cursor) 3))
            finally (setf index cursor))
      (when (< index (- end 3))
        (setf crc (%native-arm-crc32-word crc (sb-sys:sap-ref-32 sap index) 2))
        (incf index 4))
      (when (< index (- end 1))
        (setf crc (%native-arm-crc32-word crc (sb-sys:sap-ref-16 sap index) 1))
        (incf index 2))
      (when (< index end)
        (setf crc (%native-arm-crc32-word crc (sb-sys:sap-ref-8 sap index) 0)))
      (logxor crc #xffffffff))))

#+arm64
(defun %wal-crc32c-arm-octets (bytes start end table constants seed)
  "Return final Castagnoli CRC32C under the IEEE ARM kernel's input contract.
The production WAL dispatcher uses this kernel when ARM CRC32 is available."
  (declare (optimize (speed 3) (safety 0))
           (ignore table constants)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  (sb-sys:with-pinned-objects (bytes)
    (let ((sap (sb-sys:vector-sap bytes))
          (index start)
          (crc seed))
      (declare (type fixnum index) (type (unsigned-byte 32) crc))
      (loop for cursor fixnum from index below (- end 7) by 8
            do (setf crc (%native-arm-crc32-word crc (sb-sys:sap-ref-64 sap cursor) 7))
            finally (setf index cursor))
      (when (< index (- end 3))
        (setf crc (%native-arm-crc32-word crc (sb-sys:sap-ref-32 sap index) 6))
        (incf index 4))
      (when (< index (- end 1))
        (setf crc (%native-arm-crc32-word crc (sb-sys:sap-ref-16 sap index) 5))
        (incf index 2))
      (when (< index end)
        (setf crc (%native-arm-crc32-word crc (sb-sys:sap-ref-8 sap index) 4)))
      (logxor crc #xffffffff))))

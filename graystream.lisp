(in-package :cl-rados)

(defparameter *default-buffer-size* 20000)

(defclass ceph-stream ()
  ((file-pos :initarg :file-pos
             :initform 0
             :accessor file-pos)
   (buffer :initform (make-array *default-buffer-size*
                                 :element-type '(unsigned-byte 8)
                                 :fill-pointer 0)
           :accessor buffer)
   (ceph-id :initarg :ceph-id
            :initform (error "please specify ceph-id to read from")
            :accessor ceph-id)
   (ioctx :initarg :ioctx
          :initform (error "please specify io-context")
          :accessor ioctx)))

(defclass ceph-input-stream (ceph-stream)
  ())

(defclass ceph-binary-input-stream (fundamental-binary-input-stream ceph-input-stream)
  ())

(defmethod stream-element-type ((stream ceph-binary-input-stream))
  'octet)

(defcvar "errno" :int)
(defun strerror ()
   (foreign-funcall "strerror" :int *errno* :string))

(define-condition librados-error (error)
  ((text :initarg :text :accessor text
         :initform (strerror))))

(defmethod stream-fill-buffer ((stream ceph-input-stream))
  (let* ((buffer (buffer stream))
         (chunk-size *default-buffer-size*))
    (with-foreign-object (*res :uchar chunk-size)
      (let ((bytes-read (rados_read (ioctx stream)
                                        (ceph-id stream)
                                        *res
                                        chunk-size
                                        (file-pos stream))))
        (if (< bytes-read 0)
            (error 'librados-error :stream stream))
        (setf (fill-pointer buffer) bytes-read)
        (loop for i below bytes-read
           do (setf (aref (buffer stream) (- (- bytes-read 1) i))
                    (mem-aref *res :uchar i)))))))

(defmethod stream-read-byte ((stream ceph-input-stream))
  (let ((buffer (buffer stream)))
    (if (= 0 (fill-pointer buffer))
        (stream-fill-buffer stream))
    (handler-case
        (let ((byte (vector-pop buffer)))
          (incf (file-pos stream))
          byte)
      (error ()
        :eof))))

(defclass ceph-output-stream (ceph-stream)
  ())

(defclass ceph-binary-output-stream (fundamental-binary-output-stream ceph-output-stream)
  ())

(defmethod truncate-cephobj ((stream ceph-output-stream) &optional (size 0))
  (rados_trunc (ioctx stream) (ceph-id stream) size))

(defmethod stream-drain-buffer ((stream ceph-output-stream))
  (let* ((buffer (buffer stream))
         (buflen (length buffer)))
    (with-foreign-object (*buf :uchar buflen)
      (loop for i below buflen
         do (setf (mem-aref *buf :uchar i)
                  (aref buffer i)))
      (assert (= 0 (rados_write (ioctx stream) (ceph-id stream)
                                *buf buflen (file-pos stream))))
      (incf (file-pos stream) buflen)
      (setf (fill-pointer buffer) 0)
      buflen)))

(defmethod stream-write-byte ((stream ceph-output-stream) integer)
  (let ((buffer (buffer stream)))
    (if (= *default-buffer-size* (length buffer))
        (stream-drain-buffer stream))
    (vector-push integer buffer)))

(defmethod stream-write-sequence ((stream ceph-binary-output-stream) sequence start end &rest rest)
  (apply #'call-next-method `(,stream ,(make-array (length sequence)
                                                   :element-type '(unsigned-byte 8)
                                                   :initial-contents sequence)
                                      ,start ,end ,@rest)))
      
(defmethod stream-force-output ((stream ceph-binary-output-stream))
  (stream-drain-buffer stream))

(defmethod close ((stream ceph-binary-output-stream) &key abort)
  (if (not abort)
      (stream-force-output stream))
  (call-next-method stream))

(in-package :cl-rados)

(defclass ceph-input-stream (fundamental-binary-input-stream fundamental-character-input-stream)
  ((file-pos :initarg :file-pos
             :initform 0
             :accessor file-pos)
   (ceph-id :initarg :ceph-id
            :initform (error "please specify ceph-id to read from")
            :accessor ceph-id)
   (ioctx :initarg :ioctx
           :initform (error "please specify io-context")
           :accessor ioctx)
   (external-format :initarg :external-format
                    :initform :latin1
                    :accessor external-format)))

(defcvar "errno" :int)
(defun strerror ()
   (foreign-funcall "strerror" :int *errno* :string))

(define-condition librados-error (error)
  ((text :initarg :text :accessor text
         :initform (strerror))))

(defmethod stream-read-byte ((stream ceph-input-stream))
  (with-foreign-object (*buf :uchar)
    (let ((bytes-read (rados_read (ioctx stream) (ceph-id stream)
                                  *buf 1 (file-pos stream))))
      (if (< bytes-read 0)
          (error 'librados-error :stream stream))
      (if (= bytes-read 0)
          (error 'end-of-file :text "hit end of file"
                 :stream stream))
      (incf (file-pos stream))
      (mem-ref *buf :uchar))))

(defmethod stream-read-sequence ((sequence simple-vector)
                                                      (stream ceph-input-stream)
                                 start end &key &allow-other-keys)
  (loop for i below (length sequence)
     do (setf (aref sequence i) (stream-read-byte stream))))

(define-condition rados-external-format-encoding-error (simple-condition)
   ((message :initarg :message :accessor rados-external-format-encoding-error)))

;; (defmethod print-object (object rados-external-format-encoding-error)
;;   "monkey!")

(defmethod stream-read-char ((stream ceph-input-stream))
  (let ((buf (make-array 0 :adjustable t :fill-pointer t)))
    (loop for i below 4
       do
         (return-from stream-read-char
           (handler-case
               (progn
                 (vector-push-extend (stream-read-byte stream) buf)
                 (coerce (octets-to-string buf :external-format (external-format stream)) 'character))
             (external-format-encoding-error (err)
               (print stream)
               (error 'rados-external-format-encoding-error
                      :format-control "bad data encountered in stream whilst trying to decode as ~A because:~%~A"
                      :format-arguments (list (external-format stream) err)))
             (end-of-file ()
               (return-from stream-read-char :eof)))))))

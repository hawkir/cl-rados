(in-package :cl-rados)

(defun make-completion ()
  (with-foreign-object
      (*completion :pointer)
    (let ((completion (foreign-alloc
                       :pointer)))
      (setf (mem-ref *completion :pointer) completion)
      (assert (>= 0 (rados_aio_create_completion (null-pointer)
                                                (null-pointer)
                                                (null-pointer)
                                                *completion)))
      completion)))

(defvar *default-buffer-size* 20000)

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

(defclass ceph-character-stream (ceph-stream)
  ((external-format :initarg :external-format
                    :initform :latin1
                    :accessor external-format)))

(defclass ceph-character-input-stream (fundamental-character-input-stream ceph-character-stream ceph-input-stream)
  ())

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
                    (mem-aref *res :uchar i)))
        bytes-read))))

(defmethod stream-read-byte ((stream ceph-input-stream))
  (let ((byte (or (ignore-errors (vector-pop (buffer stream)))
                  (progn
                    (stream-fill-buffer stream)
                    (vector-pop (buffer stream))))))
    (if byte
        (incf (file-pos stream))
        (error 'end-of-file :text "hit end of file"
               :stream stream))
    byte))

;; (defmethod stream-read-sequence ((sequence simple-vector)
;;                                                       (stream ceph-input-stream)
;;                                  start end &key &allow-other-keys)
;;   (loop for i from start to end
;;      do (setf (aref sequence i) (stream-read-byte stream))))

(define-condition rados-external-format-encoding-error (simple-condition)
   ((message :initarg :message :accessor rados-external-format-encoding-error)))

;; (defmethod print-object (object rados-external-format-encoding-error)
;;   "monkey!")

(defmethod stream-read-char ((stream ceph-character-input-stream))
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

(defclass ceph-output-stream (ceph-stream)
  ())

(defclass ceph-binary-output-stream (ceph-output-stream fundamental-binary-output-stream)
  ())

(defclass ceph-character-output-stream (ceph-output-stream ceph-character-stream fundamental-character-output-stream)
  ())

(defmethod stream-write-byte ((stream ceph-output-stream) integer)
  (with-foreign-object (foreign-integer :uchar)
    (setf (mem-ref foreign-integer :uchar) integer)
    (assert (>= (rados_write (ioctx stream) (ceph-id stream) foreign-integer 1 (file-pos stream))
                0))
    (incf (slot-value stream 'file-pos ))))

(defmethod stream-write-char ((stream ceph-character-output-stream) char)
  (let ((octets (string-to-octets (string char)
                                  :external-format (external-format stream)
                                  :start 0 :end 1)))
    (loop for i below (length octets)
       do (stream-write-byte stream (aref octets i)))))

;; (defmethod stream-finish-output ((stream ceph-output-stream))
;;   (rados_aio_wait_for_complete (completion stream))
;;   (rados_aio_wait_for_safe (completion stream))
;;   (rados_aio_release (completion stream)))


(defmethod stream-write-sequence ((stream ceph-output-stream) octets start end &rest rest)
  (declare (ignore rest))
  (if (null end)
      (setf end (length octets)))
  (with-pointer-to-vector-data (foreign-octets octets)
    (print foreign-octets)
    (incf-pointer foreign-octets start)
    (print (mem-ref foreign-octets :uchar ))
    (assert (= (rados_write (ioctx stream) (ceph-id stream) foreign-octets
                            (- end start) (+ start (file-pos stream)))
               0))
    (incf (slot-value stream 'file-pos) (- end start)))
  octets)

(defmethod stream-write-sequence ((stream ceph-binary-output-stream) sequence start end &rest rest)
  (apply #'call-next-method `(,stream ,(make-array (length sequence)
                                                   :element-type '(unsigned-byte 8)
                                                   :initial-contents sequence)
                                      ,start ,end ,@rest)))
      
(defmethod stream-write-sequence ((stream ceph-character-output-stream) sequence start end &rest rest)
  (apply #'call-next-method `(,stream ,(string-to-octets sequence) ,start ,end ,@rest)))

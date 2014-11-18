(in-package :cl-rados)


;; (defun string-to-octets (string &key (external-format :latin1) (start 0) (end (length string)))
;;   (babel:string-to-octets string :encoding external-format
;;                           :start start
;;                           :end end))

;; (defun octets-to-string (sequence &key (external-format :latin1) (start 0) (end (length sequence)))
;;   (babel:octets-to-string sequence
;;                                   :encoding external-format
;;                                   :start start
;;                                   :end end))

;; (defun make-completion ()
;;   (with-foreign-object
;;       (*completion :pointer)
;;     (let ((completion (foreign-alloc
;;                        :pointer)))
;;       (setf (mem-ref *completion :pointer) completion)
;;       (assert (>= 0 (rados_aio_create_completion (null-pointer)
;;                                                 (null-pointer)
;;                                                 (null-pointer)
;;                                                 *completion)))
;;       completion)))

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

(defclass ceph-character-stream (ceph-stream)
  ((external-format :initarg :external-format
                    :initform :latin1
                    :accessor external-format)
   (charbuf :initform (make-array *default-buffer-size*
                                  :element-type 'character
                                  :fill-pointer 0)
            :accessor charbuf)))

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
        (error 'end-of-file :text "hit end of file"
               :stream stream)))))

;; (defmethod stream-read-sequence ((sequence simple-vector)
;;                                                       (stream ceph-input-stream)
;;                                  start end &key &allow-other-keys)
;;   (loop for i from start to end
;;      do (setf (aref sequence i) (stream-read-byte stream))))

(defun fill-charbuf (buffer new-contents)
  (let ((count (length new-contents)))
    (setf (fill-pointer buffer) count)
    (loop for i below count
       do
         (setf (aref buffer i)
               (aref new-contents i)))
    count))

(defmethod stream-fill-charbuf ((stream ceph-character-stream))
  (let ((byte-buf (make-array *default-buffer-size*
                              :element-type '(unsigned-byte 8)
                              :fill-pointer 0)))
    (loop repeat (- *default-buffer-size* 4)
         do (vector-push (stream-read-byte stream) byte-buf))
    (loop repeat 4
       do
         (return-from stream-fill-charbuf
           (fill-charbuf (charbuf stream)
                     (handler-case
                         (progn
                           (vector-push (stream-read-byte stream) byte-buf)
                           (coerce (octets-to-string byte-buf :external-format (external-format stream)) 'character))
                       (external-format-encoding-error (err)
                         (print stream)
                         (error 'rados-external-format-encoding-error
                                :format-control "bad data encountered in stream whilst trying to decode as ~a because:~%~a"
                                :format-arguments (list (external-format stream) err)))
                       (end-of-file ()
                         (return-from stream-fill-charbuf :eof))))))))

(define-condition rados-external-format-encoding-error (simple-condition)
   ((message :initarg :message :accessor rados-external-format-encoding-error)))

;; (defmethod print-object (object rados-external-format-encoding-error)
;;   "monkey!")

(defmethod stream-read-char ((stream ceph-character-input-stream))
  (let ((buf (make-array 0 :adjustable t :fill-pointer t)))
    (loop repeat 4
       do
         (return-from stream-read-char
           (handler-case
               (progn
                 (vector-push-extend (stream-read-byte stream) buf)
                 (coerce (octets-to-string buf :external-format (external-format stream)) 'character))
             (external-format-encoding-error (err)
               (print stream)
               (error 'rados-external-format-encoding-error
                      :format-control "bad data encountered in stream whilst trying to decode as ~a because:~%~a"
                      :format-arguments (list (external-format stream) err)))
             (end-of-file ()
               (return-from stream-read-char :eof)))))))

(defclass ceph-output-stream (ceph-stream)
  ())

(defclass ceph-binary-output-stream (ceph-output-stream fundamental-binary-output-stream)
  ())

(defclass ceph-character-output-stream (ceph-output-stream ceph-character-stream fundamental-character-output-stream)
  ())

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

(defmethod stream-drain-charbuf ((stream ceph-character-output-stream))
  (let ((buffer (charbuf stream)))
    (let ((octets (string-to-octets buffer
                                    :external-format (external-format stream))))
      (loop for i below (length octets)
         do (stream-write-byte stream (aref octets i)))
      (setf (fill-pointer buffer) 0))))

(defmethod stream-write-char ((stream ceph-character-output-stream) char)
  (let ((buffer (charbuf stream)))
    (if (= *default-buffer-size* (fill-pointer buffer))
        (stream-drain-charbuf stream))
    (vector-push char buffer)))

;; (defmethod stream-finish-output ((stream ceph-output-stream))
;;   (rados_aio_wait_for_complete (completion stream))
;;   (rados_aio_wait_for_safe (completion stream))
;;   (rados_aio_release (completion stream)))


;; (defmethod stream-write-sequence ((stream ceph-output-stream) octets start end &rest rest)
;;   (declare (ignore rest))
;;   (if (null end)
;;       (setf end (length octets)))
;;   (with-pointer-to-vector-data (foreign-octets octets)
;;     (print foreign-octets)
;;     (incf-pointer foreign-octets start)
;;     (print (mem-ref foreign-octets :uchar ))
;;     (assert (= (rados_write (ioctx stream) (ceph-id stream) foreign-octets
;;                             (- end start) (+ start (file-pos stream)))
;;                0))
;;     (incf (slot-value stream 'file-pos) (- end start)))
;;   octets)

(defmethod stream-write-sequence ((stream ceph-binary-output-stream) sequence start end &rest rest)
  (apply #'call-next-method `(,stream ,(make-array (length sequence)
                                                   :element-type '(unsigned-byte 8)
                                                   :initial-contents sequence)
                                      ,start ,end ,@rest)))
      
(defmethod stream-write-sequence ((stream ceph-character-output-stream) sequence start end &rest rest)
  (apply #'call-next-method `(,stream ,(string-to-octets sequence) ,start ,end ,@rest)))
